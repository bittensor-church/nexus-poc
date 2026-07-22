from __future__ import annotations

from collections.abc import Iterable
from typing import Any, NewType, cast

from nexus._internal.utils.exceptions import FlowMisconfiguredException

from .nodes import Node, NodeSinks, NodeSources, Pipes, Sink, SinkNode, Source, SourceName, SourceNode, Targets

SinkPath = NewType("SinkPath", str)
SourcePath = NewType("SourcePath", str)

type Connectable = Node | Sink[Any] | Source[Any]


class Flow:
    """
    A lightweight graph representation builder, constructed with the DSL used in validator examples.

    The flow keeps track of:
    - entry_sinks: the sinks that need to be fed from upstream
    - exit_sources: the sources that can be connected further downstream
    - pipes: connections from sources to sinks
    - sources/sinks/nodes: all endpoints and components encountered in the flow

    ``then(primary)`` connects the default source to one primary target, which
    receives the existing context and supplies the next continuation. Use
    ``then(primary, taps=[...])`` or ``then(taps=[...])`` for independent tap
    branches, each of which receives a child context. Named routes accept a
    plain primary or an explicit ``Targets`` value.
    """

    entry_sinks: NodeSinks
    exit_sources: NodeSources
    pipes: Pipes
    nodes: set[Node]
    sinks: set[Sink[Any]]
    sources: set[Source[Any]]

    def __init__(
        self,
        *,
        entry_sinks: NodeSinks,
        exit_sources: NodeSources,
        pipes: Pipes | None = None,
        nodes: set[Node] | None = None,
        sinks: set[Sink[Any]] | None = None,
        sources: set[Source[Any]] | None = None,
    ) -> None:
        self.entry_sinks = entry_sinks
        self.exit_sources = exit_sources
        self.pipes = pipes if pipes is not None else Pipes()
        self.nodes = nodes or set()
        self.sinks = sinks or set()
        self.sources = sources or set()

    @classmethod
    def from_connectable(cls, connectable: Connectable) -> Flow:
        node: Node
        match connectable:
            case Sink() as sink:
                node = SinkNode(sink)
            case Source() as source:
                node = SourceNode(source)
            case Node() as node:
                pass
        sinks = node.sinks()
        sources = node.sources()

        flow_object = cls(
            entry_sinks=sinks,
            exit_sources=sources,
            pipes=Pipes(),
            nodes={node},
            sinks=set(sinks.sinks.values()),
            sources=set(sources.sources.values()),
        )
        return flow_object

    def then(
        self,
        *targets: Connectable | Flow,
        taps: Iterable[Connectable | Flow] = (),
        **routes: Connectable | Flow | Targets[Connectable | Flow],
    ) -> Flow:
        """
        Connect the current exits to explicit primary and tap targets.

        At most one positional target is accepted and is the primary. Named
        routes use plain values as primaries or ``Targets`` for explicit roles.
        Iterable named-route values are invalid; use ``Targets(taps=...)``.
        """
        if len(targets) > 1:
            raise FlowMisconfiguredException(
                "expected at most one positional primary target; declare additional targets with taps=[...]"
            )

        default_targets = Targets(primary=targets[0] if targets else None, taps=taps)
        if routes and (default_targets.primary is not None or default_targets.taps):
            raise FlowMisconfiguredException("expected targets for either the default source or named routes, not both")

        if routes:
            for source_str, route in routes.items():
                source_name = SourceName(source_str)
                if source_name not in self.exit_sources.sources:
                    raise FlowMisconfiguredException(
                        f"Unexpected connection from {source_name}; available sources: {self.exit_sources.sources}"
                    )
                self._connect_targets(
                    self.exit_sources.sources[source_name],
                    self._as_route_targets(route),
                )

            self.exit_sources = NodeSources(sources={})
            return self
        else:
            if default_targets.primary is None and not default_targets.taps:
                raise FlowMisconfiguredException("expected continuation of the flow as either a primary or tap target")

            source = self.exit_sources.default_source
            if source is None:
                raise FlowMisconfiguredException(
                    "No default exit source to connect the continuation to the provided sinks: "
                    f"exit_sources={self.exit_sources}"
                )

            self.exit_sources = self._connect_targets(source, default_targets)
            return self

    def _connect[T](
        self,
        source: Source[T],
        primary: Sink[T] | None = None,
        *,
        taps: Iterable[Sink[T]] = (),
    ) -> None:
        tap_targets = frozenset(taps)
        self.pipes.connect(source, primary, taps=tap_targets)
        self.sources.add(source)
        if primary is not None:
            self.sinks.add(primary)
        self.sinks.update(tap_targets)

    def _connect_targets(
        self,
        source: Source[Any],
        targets: Targets[Connectable | Flow],
    ) -> NodeSources:
        primary_flow = Flow._as_flow(targets.primary) if targets.primary is not None else None
        tap_flows = tuple(Flow._as_flow(target) for target in targets.taps)

        primary_sink = self._default_sink(primary_flow) if primary_flow is not None else None
        tap_sinks = frozenset(self._default_sink(target_flow) for target_flow in tap_flows)
        self._connect(source, primary_sink, taps=tap_sinks)

        if primary_flow is not None:
            self._absorb(primary_flow)
        for tap_flow in tap_flows:
            self._absorb(tap_flow)

        return primary_flow.exit_sources if primary_flow is not None else NodeSources(sources={})

    def _absorb(self, target_flow: Flow) -> None:
        self.pipes.merge(target_flow.pipes)
        self.nodes |= target_flow.nodes
        self.sinks |= target_flow.sinks
        self.sources |= target_flow.sources

    @staticmethod
    def _default_sink(target_flow: Flow) -> Sink[Any]:
        sink = target_flow.entry_sinks.default_sink
        if sink is None:
            raise FlowMisconfiguredException(
                f"No default entry sink to connect the source to the provided flow: target_flow={target_flow}"
            )
        return sink

    @staticmethod
    def _as_route_targets(
        target: object,
    ) -> Targets[Connectable | Flow]:
        if isinstance(target, Targets):
            return cast(Targets[Connectable | Flow], target)
        if isinstance(target, (Node, Sink, Source, Flow)):
            return Targets(primary=cast(Connectable | Flow, target))
        if isinstance(target, Iterable):
            raise FlowMisconfiguredException(
                "iterable named-route values are invalid; use Targets(primary=..., taps=[...])"
            )
        raise FlowMisconfiguredException(f"Expected a flow target, got {target!r}.")

    @staticmethod
    def _as_flow(component: Connectable | Flow) -> Flow:
        if isinstance(component, Flow):
            return component
        else:
            return Flow.from_connectable(component)
