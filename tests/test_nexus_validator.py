# pyright: basic

import time
from contextlib import contextmanager

import pytest
from nexus_task_test_setup import (
    DummyTaskInput,
    build_nexus_task_test_setup,
)
from pydantic_settings import BaseSettings

from nexus.v1 import (
    FlowMisconfiguredException,
    NexusValidator,
    NoopPayloadCreator,
    Sink,
    Source,
    SubnetMisconfiguredException,
    get_subnet_settings_as,
)


class _TestSettings(BaseSettings):
    pass


def test_connect_declares_primary_and_tap_targets() -> None:
    validator = NexusValidator(_TestSettings())
    source = Source[str]("source")
    primary = Sink[str]("primary")
    tap_a = Sink[str]("tap-a")
    tap_b = Sink[str]("tap-b")

    validator.connect(source, primary, taps=(tap for tap in (tap_a, tap_b, tap_a)))

    targets = validator.subnet_flow.pipes[source]
    assert targets.primary is primary
    assert targets.taps == frozenset((tap_a, tap_b))


def test_connect_rejects_an_empty_target_declaration() -> None:
    validator = NexusValidator(_TestSettings())

    with pytest.raises(FlowMisconfiguredException, match="at least one target"):
        validator.connect(Source[str]("source"))


def test_connect_rejects_a_sink_in_both_roles() -> None:
    validator = NexusValidator(_TestSettings())
    source = Source[str]("source")
    sink = Sink[str]("sink")

    with pytest.raises(FlowMisconfiguredException, match="both primary and tap"):
        validator.connect(source, sink, taps=[sink])


def test_validator_construction_does_not_register_passed_settings_as_subnet_settings() -> None:
    settings = _TestSettings()

    NexusValidator(settings)

    with pytest.raises(SubnetMisconfiguredException):
        get_subnet_settings_as(_TestSettings)


def test_start_runtime_scopes_subnet_settings_to_runtime() -> None:
    observed_settings: list[_TestSettings | None] = []

    class _FakeRuntime:
        @contextmanager
        def running(self, shutdown_timeout_seconds: float = 30.0):
            del shutdown_timeout_seconds
            try:
                observed_settings.append(get_subnet_settings_as(_TestSettings))
            except SubnetMisconfiguredException:
                observed_settings.append(None)
            yield object()

    class _RuntimeScopedValidator(NexusValidator):
        def _build_runtime(self):
            return _FakeRuntime()

    validator = _RuntimeScopedValidator(_TestSettings())

    with validator.start_runtime():
        assert isinstance(get_subnet_settings_as(_TestSettings), _TestSettings)
        assert validator.runtime is not None

    assert validator.runtime is None
    assert len(observed_settings) == 1
    assert isinstance(observed_settings[0], _TestSettings)
    with pytest.raises(SubnetMisconfiguredException):
        get_subnet_settings_as(_TestSettings)


def test_run_can_be_called_twice_in_process(monkeypatch: pytest.MonkeyPatch) -> None:
    observed_settings: list[_TestSettings] = []

    class _FakeRuntime:
        @contextmanager
        def running(self, shutdown_timeout_seconds: float = 30.0):
            del shutdown_timeout_seconds
            observed_settings.append(get_subnet_settings_as(_TestSettings))
            yield object()

    class _RunValidator(NexusValidator):
        def _build_runtime(self):
            return _FakeRuntime()

    def _stop_immediately(_seconds: float) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(time, "sleep", _stop_immediately)

    _RunValidator.run(settings_class=_TestSettings)
    _RunValidator.run(settings_class=_TestSettings)

    assert len(observed_settings) == 2
    assert all(isinstance(settings, _TestSettings) for settings in observed_settings)


def test_connect_discovers_node_without_validator_field() -> None:
    class _LocalNodeValidator(NexusValidator):
        def __init__(self) -> None:
            super().__init__(_TestSettings())
            local_node = NoopPayloadCreator[str]("validator-local-node")
            self.connect(Source[str]("external-upstream"), local_node.input)
            self.connect(local_node.created_payload, Sink[str]("external-downstream"))

    runtime = _LocalNodeValidator()._build_runtime()

    assert len(runtime.actors) == 1
    assert any("validator-local-node" in actor.actor_id for actor in runtime.actors)


def test_connect_discovers_nodes_owned_by_tap_sinks() -> None:
    class _TapNodeValidator(NexusValidator):
        def __init__(self) -> None:
            super().__init__(_TestSettings())
            tap_node = NoopPayloadCreator[str]("validator-tap-node")
            self.connect(Source[str]("external-upstream"), taps=[tap_node.input])
            self.connect(tap_node.created_payload, Sink[str]("external-downstream"))

    runtime = _TapNodeValidator()._build_runtime()

    assert len(runtime.actors) == 1
    assert any("validator-tap-node" in actor.actor_id for actor in runtime.actors)


def test_build_runtime_includes_only_connected_nodes() -> None:
    class _OnlyConnectedValidator(NexusValidator):
        def __init__(self) -> None:
            super().__init__(_TestSettings())
            connected = NoopPayloadCreator[str]("validator-connected-node")
            NoopPayloadCreator[str]("validator-unused-node")
            self.connect(Source[str]("external-upstream"), connected.input)
            self.connect(connected.created_payload, Sink[str]("external-downstream"))

    runtime = _OnlyConnectedValidator()._build_runtime()

    assert len(runtime.actors) == 1
    assert any("validator-connected-node" in actor.actor_id for actor in runtime.actors)
    assert not any("validator-unused-node" in actor.actor_id for actor in runtime.actors)


def test_connect_discovers_task_from_task_endpoints() -> None:
    task_setup = build_nexus_task_test_setup()
    task = task_setup.task

    class _TaskValidator(NexusValidator):
        def __init__(self) -> None:
            super().__init__(_TestSettings())
            self.connect(Source[DummyTaskInput]("task-upstream"), task.input)
            self.connect(task.successful_task_result, Sink("task-downstream"))

    validator = _TaskValidator()
    runtime = validator._build_runtime()

    expected_actor_count = len(task.internal_nodes()) + 1  # + internal subnet clock
    assert len(runtime.actors) == expected_actor_count
    assert any("internal-subnet-clock" in actor.actor_id for actor in runtime.actors)
    clock_targets = validator.subnet_flow.pipes[validator.subnet_clock.source]
    assert clock_targets.primary is None
    assert clock_targets.taps == {task.block_beat}


def test_build_runtime_allows_ownerless_external_endpoints() -> None:
    class _OwnerlessEndpointsValidator(NexusValidator):
        def __init__(self) -> None:
            super().__init__(_TestSettings())
            self.connect(Source[str]("ownerless-upstream"), Sink[str]("ownerless-downstream"))

    runtime = _OwnerlessEndpointsValidator()._build_runtime()

    assert runtime.actors == ()
