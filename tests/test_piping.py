# pyright: basic

from dataclasses import FrozenInstanceError

import pytest

from nexus.v1 import Flow, FlowMisconfiguredException, Pipes, Piping, Sink, Source, Targets


def test_targets_are_immutable_and_accept_any_tap_iterable() -> None:
    primary = Sink("primary")
    tap_a = Sink("tap-a")
    tap_b = Sink("tap-b")

    targets = Targets(primary=primary, taps=(tap for tap in [tap_a, tap_b, tap_a]))

    assert targets.primary is primary
    assert targets.taps == frozenset({tap_a, tap_b})
    with pytest.raises(FrozenInstanceError):
        targets.primary = tap_a  # pyright: ignore[reportAttributeAccessIssue]


def test_missing_source_has_empty_targets_without_becoming_a_connection() -> None:
    pipes = Pipes()
    source = Source("source")

    assert pipes[source] == Targets()
    assert source not in pipes


def test_pipes_reject_empty_connections() -> None:
    pipes = Pipes()

    with pytest.raises(FlowMisconfiguredException, match="at least one target"):
        pipes.connect(Source("source"))


def test_pipes_merge_primary_and_deduplicated_taps_idempotently() -> None:
    pipes = Pipes()
    source = Source("source")
    primary = Sink("primary")
    tap_a = Sink("tap-a")
    tap_b = Sink("tap-b")

    pipes.connect(source, primary, taps=[tap_a, tap_a])
    pipes.connect(source, primary, taps=[tap_a, tap_b])

    assert pipes[source] == Targets(primary=primary, taps=[tap_a, tap_b])


def test_pipes_reject_distinct_primaries() -> None:
    pipes = Pipes()
    source = Source("source")
    pipes.connect(source, Sink("first"))

    with pytest.raises(FlowMisconfiguredException, match="multiple primary targets"):
        pipes.connect(source, Sink("second"))


@pytest.mark.parametrize("primary_first", [True, False])
def test_pipes_reject_role_overlap_across_declarations(primary_first: bool) -> None:
    pipes = Pipes()
    source = Source("source")
    sink = Sink("sink")

    if primary_first:
        pipes.connect(source, sink)
    else:
        pipes.connect(source, taps=[sink])

    with pytest.raises(FlowMisconfiguredException, match="both primary and tap"):
        if primary_first:
            pipes.connect(source, taps=[sink])
        else:
            pipes.connect(source, sink)


def test_pipes_reject_role_overlap_in_one_declaration() -> None:
    pipes = Pipes()
    source = Source("source")
    sink = Sink("sink")

    with pytest.raises(FlowMisconfiguredException, match="both primary and tap"):
        pipes.connect(source, sink, taps=[sink])


def test_piping_aggregates_primary_and_taps_from_multiple_flows() -> None:
    scattering_source = Source("scattering-source")
    primary = Sink("primary")
    tap = Sink("tap")
    other_source = Source("other-source")
    other_primary = Sink("other-primary")

    primary_flow = Flow.from_connectable(scattering_source).then(primary)
    tap_flow = Flow.from_connectable(scattering_source).then(taps=[tap])
    other_flow = Flow.from_connectable(other_source).then(other_primary)

    piping = Piping()
    piping.add_flow(primary_flow)
    piping.add_flow(tap_flow)
    piping.add_flow(other_flow)

    assert piping.sources == {scattering_source, other_source}
    assert piping.sinks == {primary, tap, other_primary}
    assert piping.pipes[scattering_source] == Targets(primary=primary, taps=[tap])
    assert piping.pipes[other_source] == Targets(primary=other_primary)


def test_piping_rejects_conflicting_flow_primaries() -> None:
    source = Source("source")
    piping = Piping()
    piping.add_flow(Flow.from_connectable(source).then(Sink("first")))

    with pytest.raises(FlowMisconfiguredException, match="multiple primary targets"):
        piping.add_flow(Flow.from_connectable(source).then(Sink("second")))
