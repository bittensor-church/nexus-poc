# pyright: basic

from collections.abc import Iterable
from typing import Any, cast, override

import pytest

from nexus.v1 import (
    DoubleTransform,
    Flow,
    FlowMisconfiguredException,
    Fork,
    Node,
    NodeSinks,
    NodeSources,
    Sink,
    SinkName,
    Source,
    Targets,
    Transform,
)


class DualSinkPreferred(Node):
    def __init__(self) -> None:
        super().__init__("dual-sink-preferred")
        self.primary = Sink("primary")
        self.secondary = Sink("secondary")

    @override
    def sinks(self) -> NodeSinks:
        sinks = NodeSinks(
            sinks={
                SinkName("primary"): self.primary,
                SinkName("secondary"): self.secondary,
            }
        )
        sinks.default_sink = self.secondary
        return sinks

    @override
    def sources(self) -> NodeSources:
        return NodeSources(sources={})


def test_then_requires_targets() -> None:
    flow = Flow.from_connectable(Source("start"))

    with pytest.raises(FlowMisconfiguredException, match="expected continuation"):
        flow.then()


def test_then_rejects_multiple_positional_targets() -> None:
    flow = Flow.from_connectable(Source("start"))

    with pytest.raises(FlowMisconfiguredException, match="at most one positional primary"):
        flow.then(Sink("a"), Sink("b"))


@pytest.mark.parametrize(
    "primary,taps",
    [
        (Sink("primary"), ()),
        (None, (Sink("tap"),)),
    ],
)
def test_then_rejects_default_source_targets_mixed_with_named_routes(
    primary: Sink[Any] | None,
    taps: tuple[Sink[Any], ...],
) -> None:
    flow = Flow.from_connectable(Source("start"))

    with pytest.raises(FlowMisconfiguredException, match="default source or named routes"):
        if primary is not None:
            flow.then(primary, ok=Sink("named"))
        else:
            flow.then(taps=taps, ok=Sink("named"))


def test_single_source_connects_to_primary() -> None:
    start = Source("start")
    end = Sink("end")

    flow = Flow.from_connectable(start).then(end)

    assert flow.pipes[start] == Targets(primary=end)
    assert flow.exit_sources.sources == {}


def test_single_source_connects_to_primary_and_taps() -> None:
    start = Source("start")
    main = Transform[str, str]("main")
    audit = Sink[str]("audit")

    flow = Flow.from_connectable(start).then(main, taps=[audit])

    assert flow.pipes[start] == Targets(primary=main.sink, taps=[audit])
    assert flow.exit_sources == main.sources()


def test_taps_only_clears_continuation_and_absorbs_all_target_flows() -> None:
    start = Source("start")
    left = Transform[str, str]("left")
    right = Transform[str, str]("right")

    flow = Flow.from_connectable(start).then(taps=[left, right])

    assert flow.pipes[start] == Targets(taps=[left.sink, right.sink])
    assert flow.exit_sources.sources == {}
    assert left in flow.nodes
    assert right in flow.nodes


def test_single_explicit_tap_remains_a_tap() -> None:
    start = Source("start")
    observer = Sink("observer")

    flow = Flow.from_connectable(start).then(taps=(target for target in [observer]))

    assert flow.pipes[start] == Targets(taps=[observer])


def test_duplicate_taps_are_deduplicated() -> None:
    start = Source("start")
    observer = Sink("observer")

    flow = Flow.from_connectable(start).then(taps=[observer, observer])

    assert flow.pipes[start] == Targets(taps=[observer])


def test_positional_then_uses_default_exit_source_when_available() -> None:
    transform = Transform[str, str]("transform")
    ok_sink = Sink("ok-sink")

    flow = Flow.from_connectable(transform).then(ok_sink)

    assert flow.pipes[transform.ok] == Targets(primary=ok_sink)
    assert transform.error not in flow.pipes


def test_positional_then_uses_preferred_entry_sink_when_available() -> None:
    start = Source("start")
    preferred = DualSinkPreferred()

    flow = Flow.from_connectable(start).then(preferred)

    assert flow.pipes[start] == Targets(primary=preferred.secondary)
    assert preferred.primary not in flow.pipes[start].taps


def test_named_routes_use_plain_values_as_primaries() -> None:
    start = Source("start")
    fork = Fork[str, str, str]("fork")
    left_sink = Sink("left")
    right_sink = Sink("right")

    flow = Flow.from_connectable(start).then(fork).then(left=left_sink, right=right_sink)

    assert flow.pipes[start] == Targets(primary=fork.sink)
    assert flow.pipes[fork.left] == Targets(primary=left_sink)
    assert flow.pipes[fork.right] == Targets(primary=right_sink)
    assert flow.exit_sources.sources == {}


def test_named_routes_support_explicit_primary_and_taps() -> None:
    fork = Fork[str, str, str]("fork")
    left_main = Sink("left-main")
    left_audit = Sink("left-audit")
    right_a = Sink("right-a")
    right_b = Sink("right-b")

    flow = Flow.from_connectable(fork).then(
        left=Targets(primary=left_main, taps=[left_audit]),
        right=Targets(taps=[right_a, right_b]),
    )

    assert flow.pipes[fork.left] == Targets(primary=left_main, taps=[left_audit])
    assert flow.pipes[fork.right] == Targets(taps=[right_a, right_b])
    assert flow.exit_sources.sources == {}


@pytest.mark.parametrize("targets", [[Sink("a"), Sink("b")], (Sink("a"), Sink("b"))])
def test_named_routes_reject_legacy_iterable_values(targets: Iterable[Sink[Any]]) -> None:
    fork = Fork[str, str, str]("fork")

    with pytest.raises(FlowMisconfiguredException, match="iterable named-route values are invalid"):
        Flow.from_connectable(fork).then(left=cast(Any, targets))


def test_named_route_flow_targets_are_absorbed() -> None:
    fork = Fork[str, str, str]("fork")
    transform = Transform[str, str]("transform")
    ok = Sink("ok")
    error = Sink("error")
    branch = Flow.from_connectable(transform).then(ok=ok, error=error)
    audit = Sink("audit")

    flow = Flow.from_connectable(fork).then(
        left=Targets(primary=branch, taps=[audit]),
        right=Sink("right"),
    )

    assert flow.pipes[fork.left] == Targets(primary=transform.sink, taps=[audit])
    assert flow.pipes[transform.ok] == Targets(primary=ok)
    assert flow.pipes[transform.error] == Targets(primary=error)


def test_only_primary_flow_supplies_continuation() -> None:
    start = Source("start")
    main = Transform[str, str]("main")
    observer = Transform[str, str]("observer")
    end = Sink("end")

    flow = Flow.from_connectable(start).then(main, taps=[observer]).then(ok=end)

    assert flow.pipes[start] == Targets(primary=main.sink, taps=[observer.sink])
    assert flow.pipes[main.ok] == Targets(primary=end)
    assert observer.ok not in flow.pipes


def test_error_when_source_cannot_be_implied() -> None:
    fork = Fork[str, str, str]("fork")

    with pytest.raises(FlowMisconfiguredException, match="No default exit source"):
        Flow.from_connectable(fork).then(Sink("target"))


def test_error_when_sink_cannot_be_implied() -> None:
    start = Source("start")
    ambiguous = DoubleTransform[str, str, str, str]("double")

    with pytest.raises(FlowMisconfiguredException, match="No default entry sink"):
        Flow.from_connectable(start).then(ambiguous)


def test_error_if_fork_branch_is_misnamed() -> None:
    fork = Fork[str, str, str]("fork")

    with pytest.raises(FlowMisconfiguredException, match="Unexpected connection"):
        Flow.from_connectable(fork).then(ok=Sink("sink"))


def test_error_if_keyword_route_uses_unknown_source_name() -> None:
    start = Source("start")

    with pytest.raises(FlowMisconfiguredException, match="Unexpected connection"):
        Flow.from_connectable(start).then(ok=Sink("sink"))


def test_error_when_keyword_routes_used_after_flow_has_no_exit_sources() -> None:
    flow = Flow.from_connectable(Source("start")).then(Sink("end"))

    with pytest.raises(FlowMisconfiguredException, match="Unexpected connection"):
        flow.then(ok=Sink("another"))


def test_empty_named_targets_are_rejected() -> None:
    fork = Fork[str, str, str]("fork")

    with pytest.raises(FlowMisconfiguredException, match="at least one target"):
        Flow.from_connectable(fork).then(left=Targets())


def test_sink_cannot_be_both_primary_and_tap() -> None:
    start = Source("start")
    sink = Sink("sink")

    with pytest.raises(FlowMisconfiguredException, match="both primary and tap"):
        Flow.from_connectable(start).then(sink, taps=[sink])


def test_positional_then_absorbs_target_flow_pipes() -> None:
    start = Source("start")
    a = Transform[str, str]("a")
    b = Transform[str, str]("b")
    end = Sink("end")

    subflow = Flow.from_connectable(a).then(b)
    flow = Flow.from_connectable(start).then(subflow).then(ok=end)

    assert flow.pipes[start] == Targets(primary=a.sink)
    assert flow.pipes[a.ok] == Targets(primary=b.sink)
    assert flow.pipes[b.ok] == Targets(primary=end)


def test_self_loops_are_allowed() -> None:
    start = Source("start")
    transform = Transform[str, str]("transform")

    flow = Flow.from_connectable(start).then(transform).then(transform)

    assert flow.pipes[start] == Targets(primary=transform.sink)
    assert flow.pipes[transform.ok] == Targets(primary=transform.sink)


def test_routes_to_flow_without_entry_sink_error_cleanly() -> None:
    start = Source("start")
    subflow = Flow.from_connectable(Source("sub-start"))

    with pytest.raises(FlowMisconfiguredException, match="No default entry sink"):
        Flow.from_connectable(start).then(subflow)
