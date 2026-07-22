# pyright: basic

from datetime import timedelta
from itertools import chain, repeat

import pytest
from pylon_client import artanis
from pylon_client.artanis import v1 as artanis_v1
from utils import CollectorActor, MockPylonClientProvider, dummy_epoch_beat, wait_until

from nexus.v1 import BlockCount, BlockNumber, EpochBeat, EpochBeatNode, Flow, SubnetBuilder

# Netuid 1 epochs for reference:
# -3 -> 357 (yes, it goes negative)
# 358 -> 718
# 719 -> 1079
# 1080 -> 1440
# 1441 -> 1801


@pytest.mark.parametrize(
    "blocks, beats, delay",
    [
        pytest.param(
            [500, 800, 1200],
            [358, 719, 1080],  # Identify an epoch by its first block
            BlockCount(0),
            id="emits-epochs",
        ),
        pytest.param(
            [500, 501, 502, 800, 805, 810],
            [358, 719],
            BlockCount(0),
            id="emits-only-once",
        ),
        pytest.param(
            [368, 718, 719, 720, 728],
            [358],  # With delay 10, should not emit epoch 719 until block 729
            BlockCount(10),
            id="respects-delay",
        ),
    ],
)
def test_epoch_beat(blocks: list[BlockNumber], beats: list[BlockNumber], delay: BlockCount, default_test_netuid):
    block_infos = [_dummy_block_info_response(block_number) for block_number in blocks]
    expected_beats = [dummy_epoch_beat(block_number, default_test_netuid) for block_number in beats]

    # Repeat the last block forever so the producer doesn't crash on exhaustion; dedup prevents extra emissions
    provider = MockPylonClientProvider()
    with provider.prepare_mock_client() as client:
        client.open_access.get_latest_block_info.side_effect = chain(block_infos, repeat(block_infos[-1]))

    node = EpochBeatNode(
        "test",
        netuid=default_test_netuid,
        delay=delay,
        polling_interval=timedelta(seconds=0.01),
        pylon_client_provider=provider,
    )
    builder = SubnetBuilder(nodes=[node])
    collector = CollectorActor[EpochBeat](
        pipe_to_bus=builder.pipe_to_bus,
        context_store=builder.context_store,
    )

    runtime = (
        builder.add_flows(Flow.from_connectable(node.source).then(taps=[collector.sink])).add_actors(collector).build()
    )

    with runtime.running(shutdown_timeout_seconds=1.0):
        wait_until(lambda: len(collector.received_events) >= len(expected_beats))

    assert [event.payload for event in collector.received_events] == expected_beats


def test_epoch_beat_retries_after_transient_pylon_failure(
    caplog: pytest.LogCaptureFixture, default_test_netuid
) -> None:
    block_infos = [_dummy_block_info_response(block_number) for block_number in [500, 800]]
    expected_beats = [dummy_epoch_beat(500, default_test_netuid), dummy_epoch_beat(800, default_test_netuid)]

    provider = MockPylonClientProvider()
    with provider.prepare_mock_client() as client:
        client.open_access.get_latest_block_info.side_effect = chain(
            [
                artanis.PylonRequestException("temporarily unavailable"),
                *block_infos,
            ],
            repeat(block_infos[-1]),
        )

    node = EpochBeatNode(
        "test",
        netuid=default_test_netuid,
        polling_interval=timedelta(seconds=0.01),
        pylon_client_provider=provider,
    )
    builder = SubnetBuilder(nodes=[node])
    collector = CollectorActor[EpochBeat](
        pipe_to_bus=builder.pipe_to_bus,
        context_store=builder.context_store,
    )
    runtime = (
        builder.add_flows(Flow.from_connectable(node.source).then(taps=[collector.sink])).add_actors(collector).build()
    )

    with caplog.at_level("WARNING", logger="nexus._internal.actors.chain_beat.epoch_beat"):
        with runtime.running(shutdown_timeout_seconds=1.0):
            wait_until(lambda: len(collector.received_events) >= len(expected_beats))
            wait_until(
                lambda: any("Transient Pylon poll failure; will retry." in record.message for record in caplog.records)
            )

    assert [event.payload for event in collector.received_events] == expected_beats
    assert any("error_type=PylonRequestException" in record.message for record in caplog.records)


def _dummy_block_info_response(block_number: BlockNumber | int) -> artanis_v1.GetLatestBlockInfoResponse:
    return artanis_v1.GetLatestBlockInfoResponse(
        number=artanis.BlockNumber(block_number),
        timestamp=artanis.Timestamp(block_number * 1000),
        hash=artanis.BlockHash(f"0x{block_number:064x}"),
    )
