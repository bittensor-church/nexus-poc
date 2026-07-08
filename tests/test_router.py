# pyright: basic

from collections import Counter
from collections.abc import Callable, Sequence

import pytest
from fake_pylon_client import FakePylonClientProvider
from pylon_client.artanis.v1 import Neuron
from transform_test_utils import TransformActorTestSetupFactory
from utils import build_neuron, wait_until

from nexus.v1 import (
    NoRoutableNeuronsException,
    RoundRobinNeuronRouter,
    miners_only,
    validators_only,
)


def test_round_robin_router_cycles_through_neurons_per_context(
    transform_actor_test_setup_factory: TransformActorTestSetupFactory,
) -> None:
    pylon_provider = FakePylonClientProvider(
        neurons=[
            build_neuron(uid=1, hotkey="alpha", validator_permit=False),
            build_neuron(uid=2, hotkey="beta", validator_permit=True),
            build_neuron(uid=3, hotkey="gamma", validator_permit=False),
        ]
    )
    router = RoundRobinNeuronRouter[str](
        "router",
        netuid=42,
        pylon_client_provider=pylon_provider,
    )
    setup = transform_actor_test_setup_factory(router)

    payloads = ["first", "second", "third", "fourth"]
    with setup.running(shutdown_timeout_seconds=1.0):
        ctx_id = setup.send(input_payload=payloads[0])
        for payload in payloads[1:]:
            setup.send(input_payload=payload, ctx_id=ctx_id)

        wait_until(lambda: len(setup.processed_collector.received_events) == len(payloads))

    routed_payloads = [event.payload for event in setup.processed_collector.received_events]
    hotkeys = [payload.target.hotkey for payload in routed_payloads]
    assert set(hotkeys[:3]) == {"alpha", "beta", "gamma"}
    assert hotkeys[3] == hotkeys[0]
    assert [payload.input for payload in routed_payloads] == payloads
    assert len(setup.error_collector.received_events) == 0


@pytest.mark.parametrize(
    ("neuron_filter", "expected_hotkeys", "expected_validator_permit"),
    [
        (miners_only, {"miner-a", "miner-b"}, False),
        (validators_only, {"validator-a", "validator-b"}, True),
    ],
)
def test_round_robin_router_filters_miners_or_validators(
    neuron_filter: Callable[[Sequence[Neuron]], Sequence[Neuron]],
    expected_hotkeys: set[str],
    expected_validator_permit: bool,
    transform_actor_test_setup_factory: TransformActorTestSetupFactory,
) -> None:
    pylon_provider = FakePylonClientProvider(
        neurons=[
            build_neuron(uid=1, hotkey="miner-a", validator_permit=False),
            build_neuron(uid=2, hotkey="validator-a", validator_permit=True),
            build_neuron(uid=3, hotkey="miner-b", validator_permit=False),
            build_neuron(uid=4, hotkey="validator-b", validator_permit=True),
        ]
    )
    router = RoundRobinNeuronRouter[str](
        "router-filtered",
        netuid=7,
        pylon_client_provider=pylon_provider,
        neuron_filter=neuron_filter,
    )
    setup = transform_actor_test_setup_factory(router)

    payloads = ["a", "b", "c"]
    with setup.running(shutdown_timeout_seconds=1.0):
        ctx_id = setup.send(input_payload=payloads[0])
        for payload in payloads[1:]:
            setup.send(input_payload=payload, ctx_id=ctx_id)

        wait_until(lambda: len(setup.processed_collector.received_events) == len(payloads))

    routed_payloads = [event.payload for event in setup.processed_collector.received_events]
    routed_hotkeys = [payload.target.hotkey for payload in routed_payloads]
    assert set(routed_hotkeys).issubset(expected_hotkeys)
    assert set(routed_hotkeys[:2]) == expected_hotkeys
    assert all(payload.target.validator_permit is expected_validator_permit for payload in routed_payloads)
    assert len(setup.error_collector.received_events) == 0


def test_round_robin_router_accepts_custom_neuron_filter_function(
    transform_actor_test_setup_factory: TransformActorTestSetupFactory,
) -> None:
    pylon_provider = FakePylonClientProvider(
        neurons=[
            build_neuron(uid=1, hotkey="alpha", validator_permit=False),
            build_neuron(uid=2, hotkey="beta", validator_permit=True),
            build_neuron(uid=3, hotkey="gamma", validator_permit=False),
        ]
    )

    def only_beta(neurons: Sequence[Neuron]) -> Sequence[Neuron]:
        return [neuron for neuron in neurons if neuron.hotkey == "beta"]

    router = RoundRobinNeuronRouter[str](
        "router-custom-filter",
        netuid=8,
        pylon_client_provider=pylon_provider,
        neuron_filter=only_beta,
    )
    setup = transform_actor_test_setup_factory(router)

    payloads = ["x", "y"]
    with setup.running(shutdown_timeout_seconds=1.0):
        ctx_id = setup.send(input_payload=payloads[0])
        for payload in payloads[1:]:
            setup.send(input_payload=payload, ctx_id=ctx_id)

        wait_until(lambda: len(setup.processed_collector.received_events) == len(payloads))

    routed_hotkeys = [event.payload.target.hotkey for event in setup.processed_collector.received_events]
    assert routed_hotkeys == ["beta", "beta"]
    assert len(setup.error_collector.received_events) == 0


def test_round_robin_router_balances_across_many_contexts(
    transform_actor_test_setup_factory: TransformActorTestSetupFactory,
) -> None:
    neurons = [
        build_neuron(uid=1, hotkey="alpha", validator_permit=False),
        build_neuron(uid=2, hotkey="beta", validator_permit=True),
        build_neuron(uid=3, hotkey="gamma", validator_permit=False),
        build_neuron(uid=4, hotkey="delta", validator_permit=True),
    ]
    pylon_provider = FakePylonClientProvider(neurons=neurons)
    router = RoundRobinNeuronRouter[str](
        "router-context-order",
        netuid=9,
        pylon_client_provider=pylon_provider,
    )
    setup = transform_actor_test_setup_factory(router)
    contexts_count = 2000
    payloads = [f"p-{idx}" for idx in range(contexts_count)]
    with setup.running(shutdown_timeout_seconds=1.0):
        for payload in payloads:
            setup.send(input_payload=payload)

        wait_until(lambda: len(setup.processed_collector.received_events) == len(payloads), timeout=10.0)

    routed_hotkeys = [str(event.payload.target.hotkey) for event in setup.processed_collector.received_events]
    counts = Counter(routed_hotkeys)
    expected_per_neuron = contexts_count / len(neurons)
    tolerance = expected_per_neuron * 0.30

    for neuron in neurons:
        hotkey = str(neuron.hotkey)
        assert hotkey in counts
        assert abs(counts[hotkey] - expected_per_neuron) <= tolerance
    assert len(setup.error_collector.received_events) == 0


def test_round_robin_router_emits_error_when_no_neurons_in_pylon(
    transform_actor_test_setup_factory: TransformActorTestSetupFactory,
) -> None:
    pylon_provider = FakePylonClientProvider(neurons=[])
    router = RoundRobinNeuronRouter[str](
        "router-no-neurons",
        netuid=10,
        pylon_client_provider=pylon_provider,
    )
    setup = transform_actor_test_setup_factory(router)

    with setup.running(shutdown_timeout_seconds=1.0):
        ctx_id = setup.send(input_payload="payload")
        wait_until(lambda: len(setup.error_collector.received_events) == 1)

    assert len(setup.processed_collector.received_events) == 0
    assert pylon_provider.netuid_calls == [10]

    error_event = setup.error_collector.received_events[0]
    assert error_event.ctx_id == ctx_id
    assert isinstance(error_event.payload, NoRoutableNeuronsException)


def test_round_robin_router_emits_error_when_all_neurons_filtered_out(
    transform_actor_test_setup_factory: TransformActorTestSetupFactory,
) -> None:
    pylon_provider = FakePylonClientProvider(
        neurons=[
            build_neuron(uid=1, hotkey="validator-a", validator_permit=True),
            build_neuron(uid=2, hotkey="validator-b", validator_permit=True),
        ]
    )
    router = RoundRobinNeuronRouter[str](
        "router-all-filtered-out",
        netuid=11,
        pylon_client_provider=pylon_provider,
        neuron_filter=miners_only,
    )
    setup = transform_actor_test_setup_factory(router)

    with setup.running(shutdown_timeout_seconds=1.0):
        ctx_id = setup.send(input_payload="payload")
        wait_until(lambda: len(setup.error_collector.received_events) == 1)

    assert len(setup.processed_collector.received_events) == 0
    assert pylon_provider.netuid_calls == [11]

    error_event = setup.error_collector.received_events[0]
    assert error_event.ctx_id == ctx_id
    assert isinstance(error_event.payload, NoRoutableNeuronsException)
