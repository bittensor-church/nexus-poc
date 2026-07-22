# pyright: basic

from unittest.mock import call

import pytest
from pylon_client import artanis
from pylon_client.artanis.unstable import GetWeightsStatusResponse
from utils import CollectorActor, MockPylonClientProvider, dummy_block_beat, wait_until

from nexus.v1 import (
    BlockBeat,
    BlockCount,
    BlockNumber,
    Flow,
    NetUid,
    SendEvent,
    SetWeightsBeat,
    SetWeightsBeatNode,
    Source,
    SubnetBuilder,
    get_epoch_containing_block,
)

# Netuid 1 epochs (tempo=360):
# -3 -> 357
# 358 -> 718
# 719 -> 1079


def _weights_status_response(*, weights_submitted: bool) -> GetWeightsStatusResponse:
    return GetWeightsStatusResponse(weights_submitted=weights_submitted)


def _send_block_beat(*, builder: SubnetBuilder, source: Source[BlockBeat], block_number: int) -> None:
    with builder.context_store.create_context() as ctx:
        pass
    builder.pipe_to_bus.put(
        SendEvent(
            ctx_id=ctx.id,
            source=source,
            payload=dummy_block_beat(block_number),
        )
    )


def _build_runtime(
    *,
    node: SetWeightsBeatNode,
) -> tuple[SubnetBuilder, CollectorActor[SetWeightsBeat], Source[BlockBeat]]:
    block_beat_trigger = Source[BlockBeat]("test-block-beat-trigger")

    builder = SubnetBuilder(nodes=[node])
    collector = CollectorActor[SetWeightsBeat](
        pipe_to_bus=builder.pipe_to_bus,
        context_store=builder.context_store,
    )

    flow_in = Flow.from_connectable(block_beat_trigger).then(taps=[node.block_beat])
    flow_out = Flow.from_connectable(node.source).then(collector.sink)

    builder.add_flows(flow_in, flow_out).add_actors(collector)
    return builder, collector, block_beat_trigger


def test_emits_when_all_conditions_met(default_test_netuid: NetUid):
    provider = MockPylonClientProvider()
    with provider.prepare_mock_client() as client:
        client.unstable.identity.get_weights_status.return_value = _weights_status_response(weights_submitted=False)

    node = SetWeightsBeatNode(
        "test",
        netuid=default_test_netuid,
        epoch_start_offset=BlockCount(0),
        pylon_client_provider=provider,
    )
    builder, collector, block_beat_trigger = _build_runtime(node=node)
    runtime = builder.build()

    epoch_500 = get_epoch_containing_block(BlockNumber(500), netuid=default_test_netuid)

    with runtime.running(shutdown_timeout_seconds=1.0):
        _send_block_beat(builder=builder, source=block_beat_trigger, block_number=500)
        wait_until(lambda: len(collector.received_events) >= 1)

    assert [event.payload for event in collector.received_events] == [
        SetWeightsBeat(epoch=epoch_500, block_number=BlockNumber(500)),
    ]


def test_skips_when_too_early_in_epoch(default_test_netuid: NetUid):
    provider = MockPylonClientProvider()
    with provider.prepare_mock_client() as client:
        client.unstable.identity.get_weights_status.return_value = _weights_status_response(weights_submitted=False)

    # Epoch 358..718; offset 20 means first eligible block is 378. 360 must be skipped, 380 must emit.
    node = SetWeightsBeatNode(
        "test",
        netuid=default_test_netuid,
        epoch_start_offset=BlockCount(20),
        attempts_cooldown=BlockCount(1),
        pylon_client_provider=provider,
    )
    builder, collector, block_beat_trigger = _build_runtime(node=node)
    runtime = builder.build()

    epoch_380 = get_epoch_containing_block(BlockNumber(380), netuid=default_test_netuid)

    with runtime.running(shutdown_timeout_seconds=1.0):
        _send_block_beat(builder=builder, source=block_beat_trigger, block_number=360)
        _send_block_beat(builder=builder, source=block_beat_trigger, block_number=380)
        wait_until(lambda: len(collector.received_events) >= 1)

    assert [event.payload for event in collector.received_events] == [
        SetWeightsBeat(epoch=epoch_380, block_number=BlockNumber(380)),
    ]
    assert client.unstable.identity.get_weights_status.call_args_list == [
        call(block_number=BlockNumber(380)),
    ]


def test_resets_on_new_epoch_via_pylon(default_test_netuid: NetUid):
    provider = MockPylonClientProvider()
    with provider.prepare_mock_client() as client:
        # Epoch A (block 500) -> pylon reports weights already set -> no emission, flag cached.
        # Same epoch (block 600) -> gated by the cached flag without consulting pylon.
        # Epoch B (block 719) -> pylon reports weights not set -> emission expected.
        client.unstable.identity.get_weights_status.side_effect = [
            _weights_status_response(weights_submitted=True),
            _weights_status_response(weights_submitted=False),
        ]

    node = SetWeightsBeatNode(
        "test",
        netuid=default_test_netuid,
        epoch_start_offset=BlockCount(0),
        attempts_cooldown=BlockCount(1),
        pylon_client_provider=provider,
    )
    builder, collector, block_beat_trigger = _build_runtime(node=node)
    runtime = builder.build()

    epoch_500 = get_epoch_containing_block(BlockNumber(500), netuid=default_test_netuid)
    epoch_719 = get_epoch_containing_block(BlockNumber(719), netuid=default_test_netuid)
    assert epoch_500 != epoch_719

    with runtime.running(shutdown_timeout_seconds=1.0):
        _send_block_beat(builder=builder, source=block_beat_trigger, block_number=500)
        with pytest.raises(AssertionError):
            wait_until(lambda: len(collector.received_events) >= 1, timeout=0.3)
        _send_block_beat(builder=builder, source=block_beat_trigger, block_number=600)
        with pytest.raises(AssertionError):
            wait_until(lambda: len(collector.received_events) >= 1, timeout=0.3)
        _send_block_beat(builder=builder, source=block_beat_trigger, block_number=719)
        wait_until(lambda: len(collector.received_events) >= 1)

    assert [event.payload for event in collector.received_events] == [
        SetWeightsBeat(epoch=epoch_719, block_number=BlockNumber(719)),
    ]
    assert client.unstable.identity.get_weights_status.call_args_list == [
        call(block_number=BlockNumber(500)),
        call(block_number=BlockNumber(719)),
    ]


def test_respects_attempts_cooldown(default_test_netuid: NetUid):
    provider = MockPylonClientProvider()
    with provider.prepare_mock_client() as client:
        client.unstable.identity.get_weights_status.return_value = _weights_status_response(weights_submitted=False)

    node = SetWeightsBeatNode(
        "test",
        netuid=default_test_netuid,
        epoch_start_offset=BlockCount(0),
        attempts_cooldown=BlockCount(4),
        pylon_client_provider=provider,
    )
    builder, collector, block_beat_trigger = _build_runtime(node=node)
    runtime = builder.build()

    epoch_500 = get_epoch_containing_block(BlockNumber(500), netuid=default_test_netuid)

    with runtime.running(shutdown_timeout_seconds=1.0):
        # 500 -> emit; 502 -> skip (cooldown=4, gap=2); 504 -> emit (gap=4).
        _send_block_beat(builder=builder, source=block_beat_trigger, block_number=500)
        wait_until(lambda: len(collector.received_events) >= 1)
        _send_block_beat(builder=builder, source=block_beat_trigger, block_number=502)
        with pytest.raises(AssertionError):
            wait_until(lambda: len(collector.received_events) >= 2, timeout=0.3)
        _send_block_beat(builder=builder, source=block_beat_trigger, block_number=504)
        wait_until(lambda: len(collector.received_events) >= 2)

    assert [event.payload for event in collector.received_events] == [
        SetWeightsBeat(epoch=epoch_500, block_number=BlockNumber(500)),
        SetWeightsBeat(epoch=epoch_500, block_number=BlockNumber(504)),
    ]
    assert client.unstable.identity.get_weights_status.call_args_list == [
        call(block_number=BlockNumber(500)),
        call(block_number=BlockNumber(504)),
    ]


def test_skips_when_pylon_says_weights_submitted(default_test_netuid: NetUid):
    provider = MockPylonClientProvider()
    with provider.prepare_mock_client() as client:
        client.unstable.identity.get_weights_status.return_value = _weights_status_response(weights_submitted=True)

    node = SetWeightsBeatNode(
        "test",
        netuid=default_test_netuid,
        epoch_start_offset=BlockCount(0),
        attempts_cooldown=BlockCount(1),
        pylon_client_provider=provider,
    )
    builder, collector, block_beat_trigger = _build_runtime(node=node)
    runtime = builder.build()

    with runtime.running(shutdown_timeout_seconds=1.0):
        _send_block_beat(builder=builder, source=block_beat_trigger, block_number=500)
        with pytest.raises(AssertionError):
            wait_until(lambda: len(collector.received_events) >= 1, timeout=0.5)
        # Second beat in the same epoch must be gated by the cached _last_success_epoch
        # (set when pylon reported weights_submitted=True) without an additional pylon query.
        _send_block_beat(builder=builder, source=block_beat_trigger, block_number=502)
        with pytest.raises(AssertionError):
            wait_until(lambda: len(collector.received_events) >= 1, timeout=0.3)

    assert collector.received_events == []
    assert client.unstable.identity.get_weights_status.call_args_list == [
        call(block_number=BlockNumber(500)),
    ]


def test_pylon_failure(default_test_netuid: NetUid):
    provider = MockPylonClientProvider()
    with provider.prepare_mock_client() as client:
        client.unstable.identity.get_weights_status.side_effect = [
            artanis.PylonRequestException("temporarily unavailable"),
            _weights_status_response(weights_submitted=False),
        ]

    # High cooldown verifies that a Pylon failure does NOT count as an attempt:
    # if it did, the beat on block 501 (gap=1 < cooldown=100) would be suppressed.
    node = SetWeightsBeatNode(
        "test",
        netuid=default_test_netuid,
        epoch_start_offset=BlockCount(0),
        attempts_cooldown=BlockCount(100),
        pylon_client_provider=provider,
    )
    builder, collector, block_beat_trigger = _build_runtime(node=node)
    runtime = builder.build()

    epoch_500 = get_epoch_containing_block(BlockNumber(500), netuid=default_test_netuid)

    with runtime.running(shutdown_timeout_seconds=1.0):
        _send_block_beat(builder=builder, source=block_beat_trigger, block_number=500)
        # First call raised; no emission yet.
        with pytest.raises(AssertionError):
            wait_until(lambda: len(collector.received_events) >= 1, timeout=0.3)
        _send_block_beat(builder=builder, source=block_beat_trigger, block_number=501)
        wait_until(lambda: len(collector.received_events) >= 1)

    assert [event.payload for event in collector.received_events] == [
        SetWeightsBeat(epoch=epoch_500, block_number=BlockNumber(501)),
    ]
