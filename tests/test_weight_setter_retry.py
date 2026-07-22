# pyright: basic
"""
Integration tests for the weight setter + retry strategy loop.
"""

from datetime import timedelta
from typing import Any
from unittest.mock import ANY, MagicMock, create_autospec, seal

import pytest
from pylon_client.artanis import PylonResponseException
from pylon_client.artanis.v1 import SetWeightsResponse
from utils import CollectorActor, InMemoryTestTaskResultStoreProvider, wait_until

from nexus.v1 import (
    BlockNumber,
    Flow,
    Hotkey,
    IdentityPylonApiLike,
    NetUid,
    NexusException,
    PylonClientProvider,
    RetryStrategy,
    SendEvent,
    SetWeightsBeat,
    Source,
    SubnetBuilder,
    SyncPylonClientLike,
    WeighingFunc,
    Weight,
    WeightSetterNode,
    WeightSettingSuccess,
    get_epoch_containing_block,
)

NETUID = NetUid(1)
EPOCH = get_epoch_containing_block(BlockNumber(500), netuid=NETUID)
WEIGHTS = {Hotkey("hk1"): Weight(0.5), Hotkey("hk2"): Weight(1.0)}
MAX_ATTEMPTS = 3
RETRY_DELAY = timedelta(seconds=0)  # no delay in tests


type _PipelineResult = tuple[
    list[WeightSettingSuccess],  # pipeline ok
    list[NexusException],  # retries-exhausted errors
    list[NexusException],  # intermediate weight setter errors
]


@pytest.fixture
def mock_pylon_client():
    client = create_autospec(spec=SyncPylonClientLike, instance=True)
    client.identity = create_autospec(spec=IdentityPylonApiLike, instance=True)
    client.__enter__.return_value = client
    client.__exit__.return_value = None
    seal(client)
    return client


def _build_and_run(weighing_func: WeighingFunc, pylon_client: SyncPylonClientLike) -> _PipelineResult:
    provider = create_autospec(spec=PylonClientProvider, instance=True)
    provider.get_client.return_value = pylon_client
    seal(provider)

    trigger = Source[SetWeightsBeat]("test-trigger")
    retry = RetryStrategy[SetWeightsBeat]("retry", max_attempts=MAX_ATTEMPTS, delay=RETRY_DELAY)
    weight_setter = WeightSetterNode(
        "weight-setter",
        weighing_func=weighing_func,
        pylon_client_provider=provider,
        task_result_store_provider=InMemoryTestTaskResultStoreProvider[Any, Any](),
    )

    builder = SubnetBuilder(nodes=[retry, weight_setter])
    ok_collector = CollectorActor[WeightSettingSuccess](
        pipe_to_bus=builder.pipe_to_bus, context_store=builder.context_store, name="ok-collector"
    )
    error_collector = CollectorActor[NexusException](
        pipe_to_bus=builder.pipe_to_bus, context_store=builder.context_store, name="error-collector"
    )
    ws_error_collector = CollectorActor[NexusException](
        pipe_to_bus=builder.pipe_to_bus, context_store=builder.context_store, name="ws-error-collector"
    )

    runtime = (
        builder.add_flows(
            Flow.from_connectable(trigger).then(retry.input),
            Flow.from_connectable(retry.next_attempt).then(weight_setter.sink),
            Flow.from_connectable(weight_setter.ok).then(ok_collector.sink),
            Flow.from_connectable(weight_setter.error).then(
                retry.failed_attempt,
                taps=[ws_error_collector.sink],
            ),
            Flow.from_connectable(retry.error).then(error_collector.sink),
        )
        .add_actors(ok_collector, error_collector, ws_error_collector)
        .build()
    )

    # A single trigger always produces exactly 1 terminal event (pipeline ok or retries exhausted)
    with runtime.running(shutdown_timeout_seconds=1.0):
        with builder.context_store.create_context() as ctx:
            pass
        builder.pipe_to_bus.put(
            SendEvent(
                ctx_id=ctx.id,
                source=trigger,
                payload=SetWeightsBeat(epoch=EPOCH, block_number=BlockNumber(500)),
            )
        )
        wait_until(lambda: len(ok_collector.received_events) + len(error_collector.received_events) >= 1)

    return (
        [e.payload for e in ok_collector.received_events],
        [e.payload for e in error_collector.received_events],
        [e.payload for e in ws_error_collector.received_events],
    )


def test_happy_path_no_retries_needed(mock_pylon_client):
    ok, errors, weight_setter_errors = _build_and_run(weighing_func=lambda _: WEIGHTS, pylon_client=mock_pylon_client)

    assert ok == [WeightSettingSuccess()]
    assert errors == []
    assert weight_setter_errors == []


def test_succeeds_after_pylon_errors(mock_pylon_client):
    mock_pylon_client.identity.put_weights.side_effect = [
        PylonResponseException("nope"),
        PylonResponseException("still nope"),
        SetWeightsResponse(),
    ]

    ok, errors, weight_setter_errors = _build_and_run(weighing_func=lambda _: WEIGHTS, pylon_client=mock_pylon_client)

    assert ok == [WeightSettingSuccess()]
    assert errors == []
    assert weight_setter_errors == [ANY, ANY]


def test_succeeds_after_weighing_errors(mock_pylon_client):
    flaky_weighing = MagicMock(
        side_effect=[
            Exception("whoopsie"),
            Exception("that's also a whoopsie"),
            WEIGHTS,  # Success
        ],
    )
    seal(flaky_weighing)

    ok, errors, weight_setter_errors = _build_and_run(weighing_func=flaky_weighing, pylon_client=mock_pylon_client)

    assert weight_setter_errors == [ANY, ANY]
    assert errors == []
    assert ok == [WeightSettingSuccess()]


def test_succeeds_after_mixed_errors(mock_pylon_client):
    flaky_weighing = MagicMock(
        side_effect=[
            Exception("whoopsie"),
            WEIGHTS,  # Success
            WEIGHTS,  # Success - after pylon error
        ],
    )
    seal(flaky_weighing)

    mock_pylon_client.identity.put_weights.side_effect = [
        PylonResponseException("nope"),
        SetWeightsResponse(),
    ]

    ok, errors, weight_setter_errors = _build_and_run(weighing_func=flaky_weighing, pylon_client=mock_pylon_client)

    assert weight_setter_errors == [ANY, ANY]
    assert errors == []
    assert ok == [WeightSettingSuccess()]


def test_fails_after_retries_exhausted(mock_pylon_client):
    mock_pylon_client.identity.put_weights.side_effect = PylonResponseException("nope forever")

    ok, errors, weight_setter_errors = _build_and_run(weighing_func=lambda _: WEIGHTS, pylon_client=mock_pylon_client)

    assert weight_setter_errors == [ANY, ANY, ANY]
    assert errors == [ANY]
    assert ok == []
