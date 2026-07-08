# pyright: basic

from datetime import timedelta
from itertools import chain, repeat
from threading import Barrier
from typing import Any, override

import pytest
from pylon_client import artanis
from pylon_client.artanis import v1 as artanis_v1
from utils import CollectorActor, MockPylonClientProvider, dummy_block_beat, wait_until

from nexus.v1 import (
    Actor,
    BlockBeat,
    BlockBeatNode,
    BlockCount,
    BlockNumber,
    Context,
    ContextStore,
    EventHandler,
    Flow,
    MessagesToSend,
    PipeToBus,
    ReceiveEvent,
    Sink,
    SubnetBuilder,
)


class BarrierBlockBeatActor(Actor):
    """Test actor that waits at a barrier in its normal message handler."""

    def __init__(
        self,
        *,
        name: str,
        pipe_to_bus: PipeToBus,
        context_store: ContextStore,
        barrier: Barrier,
    ) -> None:
        super().__init__(name=name, pipe_to_bus=pipe_to_bus, context_store=context_store)
        self.sink = Sink[BlockBeat](f"{name}-sink")
        self.barrier = barrier
        self.received_events: list[ReceiveEvent[BlockBeat]] = []

    @override
    def handlers(self) -> dict[Sink[Any], EventHandler]:
        return {self.sink: self._handle}

    def _handle(self, _: Context, event: ReceiveEvent[BlockBeat]) -> MessagesToSend:
        if event.target != self.sink:
            raise RuntimeError(f"Unexpected target sink: {event.target.id}")
        self.barrier.wait(timeout=1.0)
        self.received_events.append(event)
        return ()


@pytest.mark.parametrize(
    "blocks, beats, nth",
    [
        pytest.param(
            [0, 1, 2],
            [0, 1, 2],
            BlockCount(1),
            id="all-consecutive-blocks-emitted",
        ),
        pytest.param(
            [0, 0, 0, 1, 1, 1],
            [0, 1],
            BlockCount(1),
            id="no-duplicates",
        ),
        pytest.param(
            [0, 1, 5, 7],
            [0, 1, 5, 7],
            BlockCount(1),
            id="no-hole-filling",
        ),
        pytest.param(
            [*range(15, 55)],
            [20, 30, 40, 50],
            BlockCount(10),
            id="every-nth-block",
        ),
    ],
)
def test_block_beat(blocks: list[BlockNumber], beats: list[BlockNumber], nth: BlockCount):
    block_infos = [_dummy_block_info_response(block_number) for block_number in blocks]
    expected_beats = [dummy_block_beat(block_number) for block_number in beats]

    # Repeat the last block forever so the producer doesn't crash on exhaustion; dedup prevents extra emissions
    provider = MockPylonClientProvider()
    with provider.prepare_mock_client() as client:
        client.open_access.get_latest_block_info.side_effect = chain(block_infos, repeat(block_infos[-1]))

    beat = BlockBeatNode(
        "test",
        pylon_client_provider=provider,
        polling_interval=timedelta(seconds=0.01),
        every_nth=nth,
    )
    builder = SubnetBuilder(nodes=[beat])
    collector = CollectorActor[BlockBeat](
        pipe_to_bus=builder.pipe_to_bus,
        context_store=builder.context_store,
    )

    runtime = builder.add_flows(Flow.from_connectable(beat.source).then(collector.sink)).add_actors(collector).build()

    with runtime.running(shutdown_timeout_seconds=1.0):
        wait_until(lambda: len(collector.received_events) >= len(expected_beats))

    assert [event.payload for event in collector.received_events] == expected_beats


def test_block_beat_retries_after_transient_pylon_failure(caplog: pytest.LogCaptureFixture):
    block_infos = [_dummy_block_info_response(block_number) for block_number in [10, 11]]
    expected_beats = [dummy_block_beat(10), dummy_block_beat(11)]

    provider = MockPylonClientProvider()
    with provider.prepare_mock_client() as client:
        client.open_access.get_latest_block_info.side_effect = chain(
            [
                artanis.PylonRequestException("temporarily unavailable"),
                *block_infos,
            ],
            repeat(block_infos[-1]),
        )

    beat = BlockBeatNode(
        "test",
        pylon_client_provider=provider,
        polling_interval=timedelta(seconds=0.01),
    )
    builder = SubnetBuilder(nodes=[beat])
    collector = CollectorActor[BlockBeat](
        pipe_to_bus=builder.pipe_to_bus,
        context_store=builder.context_store,
    )
    runtime = builder.add_flows(Flow.from_connectable(beat.source).then(collector.sink)).add_actors(collector).build()

    with caplog.at_level("WARNING", logger="nexus._internal.actors.chain_beat.block_beat"):
        with runtime.running(shutdown_timeout_seconds=1.0):
            wait_until(lambda: len(collector.received_events) >= len(expected_beats))
            wait_until(
                lambda: any("Transient Pylon poll failure; will retry." in record.message for record in caplog.records)
            )

    assert [event.payload for event in collector.received_events] == expected_beats
    assert any("error_type=PylonRequestException" in record.message for record in caplog.records)


def test_block_beat_fans_out_to_concurrent_actors():
    block_number = BlockNumber(12)
    block_info = _dummy_block_info_response(block_number)
    expected_beat = dummy_block_beat(block_number)

    provider = MockPylonClientProvider()
    with provider.prepare_mock_client() as client:
        client.open_access.get_latest_block_info.side_effect = repeat(block_info)

    beat = BlockBeatNode(
        "test",
        pylon_client_provider=provider,
        polling_interval=timedelta(seconds=0.01),
    )
    builder = SubnetBuilder(nodes=[beat])
    barrier = Barrier(2)
    worker_a = BarrierBlockBeatActor(
        name="block-worker-a",
        pipe_to_bus=builder.pipe_to_bus,
        context_store=builder.context_store,
        barrier=barrier,
    )
    worker_b = BarrierBlockBeatActor(
        name="block-worker-b",
        pipe_to_bus=builder.pipe_to_bus,
        context_store=builder.context_store,
        barrier=barrier,
    )

    runtime = (
        builder.add_flows(Flow.from_connectable(beat.source).then(worker_a.sink, worker_b.sink))
        .add_actors(worker_a, worker_b)
        .build()
    )

    with runtime.running(shutdown_timeout_seconds=1.0):
        wait_until(lambda: len(worker_a.received_events) == 1)
        wait_until(lambda: len(worker_b.received_events) == 1)

    received_a = worker_a.received_events[0]
    received_b = worker_b.received_events[0]

    assert received_a.payload == expected_beat
    assert received_b.payload == expected_beat
    assert received_a.ctx_id != received_b.ctx_id


def _dummy_block_info_response(block_number: BlockNumber | int) -> artanis_v1.GetLatestBlockInfoResponse:
    return artanis_v1.GetLatestBlockInfoResponse(
        number=artanis.BlockNumber(block_number),
        timestamp=artanis.Timestamp(block_number * 1000),
        hash=artanis.BlockHash(f"0x{block_number:064x}"),
    )
