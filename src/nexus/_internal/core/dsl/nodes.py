from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, NewType, TypeVar, override

from nexus._internal.utils.exceptions import FlowMisconfiguredException, InternalFrameworkException, NexusException

if TYPE_CHECKING:
    from nexus._internal.core.runtime.nexus_task import NexusTask

T = TypeVar("T")

# globally unique IDs for Sources, Sinks and Actors, to be used to identify them in the system
NodeId = NewType("NodeId", str)
SourceId = NewType("SourceId", str)
SinkId = NewType("SinkId", str)

# locally unique IDs for Sources and Sinks, to be used within a context of a specific
# contraption that owns them;
SourceName = NewType("SourceName", str)
SinkName = NewType("SinkName", str)


@dataclass
class NodeSources:
    sources: dict[SourceName, Source[Any]]
    default_source: Source[Any] | None = None

    def __post_init__(self) -> None:
        if not (self.default_source is None or self.default_source in self.sources.values()):
            raise InternalFrameworkException(
                f"primary_source {self.default_source!r} must be None or a key in sources; "
                f"available: {list(self.sources)}"
            )
        if len(self.sources) == 1:
            self.default_source = next(iter(self.sources.values()))

    def empty(self) -> bool:
        return not bool(self.sources)


@dataclass
class NodeSinks:
    sinks: dict[SinkName, Sink[Any]]
    default_sink: Sink[Any] | None = None

    def __post_init__(self) -> None:
        if not (self.default_sink is None or self.default_sink in self.sinks):
            raise InternalFrameworkException(
                f"primary_sink {self.default_sink!r} must be None or a key in sinks; "
                f"available: {list(self.sinks.keys())}"
            )
        if len(self.sinks) == 1:
            self.default_sink = next(iter(self.sinks.values()))


class Node(ABC):
    """
    any element of the processing graph is a Node
    """

    id: NodeId

    def __init__(self, _id: str):
        self.id = NodeId(_id)

    @abstractmethod
    def sinks(self) -> NodeSinks:
        pass

    @abstractmethod
    def sources(self) -> NodeSources:
        pass


class Source[T]:
    """
    A logical endpoint for data production.
    """

    id: SourceId
    owner_node: Node | None
    owner_task: NexusTask[Any, Any, Any, Any] | None

    def __init__(
        self,
        _id: str,
        *,
        owner_node: Node | None = None,
        owner_task: NexusTask[Any, Any, Any, Any] | None = None,
    ):
        self.id = SourceId(_id)
        self.owner_node = owner_node
        self.owner_task = owner_task


class SourceNode[T](Node):
    """
    A Node wrapper for a Source
    """

    source: Source[T]

    def __init__(self, source: Source[T]):
        super().__init__(source.id)
        self.source = source

    @override
    def sources(self) -> NodeSources:
        return NodeSources(sources={SourceName("self"): self.source})

    @override
    def sinks(self) -> NodeSinks:
        return NodeSinks(sinks={})


class Sink[T]:
    """
    A logical endpoint for data consumption.
    """

    id: SinkId
    owner_node: Node | None
    owner_task: NexusTask[Any, Any, Any, Any] | None

    def __init__(
        self,
        _id: str,
        *,
        owner_node: Node | None = None,
        owner_task: NexusTask[Any, Any, Any, Any] | None = None,
    ):
        self.id = SinkId(_id)
        self.owner_node = owner_node
        self.owner_task = owner_task


class SinkNode[T](Node):
    """
    A Node wrapper for a Sink
    """

    def __init__(self, sink: Sink[T]):
        super().__init__(sink.id)
        self.sink = sink

    @override
    def sources(self) -> NodeSources:
        return NodeSources(sources={})

    @override
    def sinks(self) -> NodeSinks:
        return NodeSinks({SinkName("self"): self.sink})


class Producer[T](Node):
    """
    A source-only node with a control sink for lifecycle signals (e.g. shutdown).
    """

    source: Source[T]
    sink: Sink[None]

    def __init__(self, _id: str):
        super().__init__(_id)
        self.source = Source[T](_id, owner_node=self)
        self.sink = Sink(f"{_id}-sink", owner_node=self)

    @override
    def sinks(self) -> NodeSinks:
        return NodeSinks({SinkName("sink"): self.sink})

    @override
    def sources(self) -> NodeSources:
        return NodeSources({SourceName("source"): self.source})


class Fork[From, ToLeft, ToRight](Node):
    """
    A logical data processing unit that forks data from a Sink to two Sources.
    """

    sink: Sink[From]
    left: Source[ToLeft]
    right: Source[ToRight]

    def __init__(self, _id: str):
        super().__init__(_id)
        self.sink = Sink[From](f"{_id}-sink", owner_node=self)
        self.left = Source[ToLeft](f"{_id}-left-source", owner_node=self)
        self.right = Source[ToRight](f"{_id}-right-source", owner_node=self)

    @override
    def sinks(self) -> NodeSinks:
        return NodeSinks(
            sinks={
                SinkName("sink"): self.sink,
            }
        )

    @override
    def sources(self) -> NodeSources:
        return NodeSources(
            sources={
                SourceName("left"): self.left,
                SourceName("right"): self.right,
            }
        )


class Transform[From, To](Fork[From, To, NexusException]):
    # convenient aliases
    ok: Source[To]
    error: Source[NexusException]
    """
        A logical data processing unit that consumes data from a Sink and produces data to the ok Source or
        reports errors to the error Source.
        Transform is a Fork really, but for the time being it's clearer to have it as a separate concept.
    """

    def __init__(self, _id: str):
        super().__init__(_id)

        self.ok = self.left
        self.error = self.right

    @override
    def sources(self) -> NodeSources:
        return NodeSources(
            sources={
                SourceName("ok"): self.ok,
                SourceName("error"): self.error,
            },
            default_source=self.ok,
        )


class DoubleTransform[InputFrom, InputTo, OutputFrom, OutputTo](Node):
    """
    A logical data processing unit that is a two-way Transform
    - the first Transform converts InputFrom to InputTo
    - the second Transform converts InputTo to OutputTo
    """

    input_transform: Transform[InputFrom, InputTo]
    output_transform: Transform[OutputFrom, OutputTo]

    # convenient aliases
    input_sink: Sink[InputFrom]
    input_ok: Source[InputTo]
    input_error: Source[NexusException]

    output_sink: Sink[OutputFrom]
    output_ok: Source[OutputTo]
    output_error: Source[NexusException]

    def __init__(self, _id: str):
        super().__init__(_id)
        self.input_transform = Transform[InputFrom, InputTo](f"{_id}-input-transform")
        self.output_transform = Transform[OutputFrom, OutputTo](f"{_id}-output-transform")

        self.input_sink = self.input_transform.sink
        self.input_ok = self.input_transform.ok
        self.input_error = self.input_transform.error

        self.output_sink = self.output_transform.sink
        self.output_ok = self.output_transform.ok
        self.output_error = self.output_transform.error

    @override
    def sources(self) -> NodeSources:
        return NodeSources(
            sources={
                SourceName("input_ok"): self.input_ok,
                SourceName("input_error"): self.input_error,
                SourceName("output_ok"): self.output_ok,
                SourceName("output_error"): self.output_error,
            }
        )

    @override
    def sinks(self) -> NodeSinks:
        return NodeSinks(
            sinks={
                SinkName("input_sink"): self.input_sink,
                SinkName("output_sink"): self.output_sink,
            }
        )


Sources = set[Source[Any]]
Sinks = set[Sink[Any]]


@dataclass(frozen=True, init=False)
class Targets[T]:
    """
    The primary and tap targets connected to one source.

    The optional primary continues the source context. Each tap receives a new
    child context. Any tap iterable is accepted and deduplicated into an
    immutable set; tap ordering is intentionally unspecified.
    """

    primary: T | None
    taps: frozenset[T]

    def __init__(self, primary: T | None = None, taps: Iterable[T] = ()) -> None:
        object.__setattr__(self, "primary", primary)
        object.__setattr__(self, "taps", frozenset(taps))


class Pipes(Mapping[Source[Any], Targets[Sink[Any]]]):
    """
    Read-only mapping view over invariant-preserving source connections.

    Build the graph through ``connect`` and ``merge``. A source can have at
    most one primary, and no sink can be both primary and tap for that source.
    """

    _targets: dict[Source[Any], Targets[Sink[Any]]]

    def __init__(self) -> None:
        self._targets = {}

    def __getitem__(self, source: Source[Any]) -> Targets[Sink[Any]]:
        return self._targets.get(source, Targets())

    def __iter__(self) -> Iterator[Source[Any]]:
        return iter(self._targets)

    def __len__(self) -> int:
        return len(self._targets)

    def __contains__(self, source: object) -> bool:
        return source in self._targets

    def connect[T](
        self,
        source: Source[T],
        primary: Sink[T] | None = None,
        *,
        taps: Iterable[Sink[T]] = (),
    ) -> None:
        """Add targets for ``source``, merging repeated declarations safely."""
        targets = Targets[Sink[T]](primary=primary, taps=taps)
        if targets.primary is None and not targets.taps:
            raise FlowMisconfiguredException(f"Expected at least one target for source {source.id!r}.")

        existing = self._targets.get(source)
        if existing is not None:
            targets = self._merge_targets(source, existing, targets)

        self._validate_roles(source, targets)
        self._targets[source] = targets

    def merge(self, other: Pipes) -> None:
        """Merge another connection graph while preserving target roles."""
        for source, targets in other.items():
            self.connect(source, targets.primary, taps=targets.taps)

    @staticmethod
    def _merge_targets[T](
        source: Source[T], existing: Targets[Sink[T]], incoming: Targets[Sink[T]]
    ) -> Targets[Sink[T]]:
        if existing.primary is not None and incoming.primary is not None and existing.primary != incoming.primary:
            raise FlowMisconfiguredException(
                f"Source {source.id!r} cannot have multiple primary targets: "
                f"{existing.primary.id!r} and {incoming.primary.id!r}."
            )

        return Targets(
            primary=existing.primary if existing.primary is not None else incoming.primary,
            taps=existing.taps | incoming.taps,
        )

    @staticmethod
    def _validate_roles(source: Source[Any], targets: Targets[Sink[Any]]) -> None:
        if targets.primary is not None and targets.primary in targets.taps:
            raise FlowMisconfiguredException(
                f"Sink {targets.primary.id!r} cannot be both primary and tap for source {source.id!r}."
            )
