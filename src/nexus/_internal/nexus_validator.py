# pyright: basic
import logging
import sys
import time
from collections.abc import Generator, Iterable
from contextlib import contextmanager
from typing import Any

from pydantic import ValidationError
from pydantic_settings import BaseSettings

from nexus._internal.actors import BlockBeatNode
from nexus._internal.core.dsl.flow import Flow
from nexus._internal.core.dsl.nodes import Node, NodeSinks, NodeSources, Sink, Source, SourceName
from nexus._internal.core.runtime.nexus_task import NexusTask
from nexus._internal.core.runtime.subnet_runtime import SubnetBuilder, SubnetRuntime
from nexus._internal.utils.subnet_settings import subnet_settings

log = logging.getLogger("validator")


class NexusValidator:
    """
    Base class for validator graphs built from explicit `connect(...)` wiring.

    Each source can have one primary sink, which continues on the source context,
    and any number of tap sinks, which each receive a child context. Calls of the
    form `connect(source, sink)` declare a primary connection; use
    `connect(source, taps=(observer,))` for isolated secondary consumers.

    Runtime components are discovered only from endpoints used in `connect(...)`.
    There is no separate node/task registration API.

    The validator instance keeps its settings on `self.settings`. When the runtime starts,
    `start_runtime()` temporarily scopes those settings into the subnet-settings registry
    so actors that resolve settings indirectly (for example OpenRouter-backed actors)
    see the current validator configuration only for the lifetime of that runtime.
    """

    subnet_clock: BlockBeatNode

    _connected_nodes: dict[int, Node]
    _connected_tasks: dict[int, NexusTask[Any, Any, Any, Any]]
    settings: BaseSettings
    subnet_flow: Flow
    runtime: SubnetRuntime | None = None

    def __init__(self, settings: BaseSettings) -> None:
        self.settings = settings
        self.subnet_clock = BlockBeatNode("internal-subnet-clock")
        self._connected_nodes = {}
        self._connected_tasks = {}

        self.subnet_flow = Flow(
            entry_sinks=NodeSinks(sinks={}),
            exit_sources=NodeSources(sources={SourceName("internal-subnet-clock"): self.subnet_clock.source}),
        )

    @classmethod
    def run[SettingsModel: BaseSettings](
        cls,
        *,
        settings_class: type[SettingsModel],
    ) -> None:
        shutdown_timeout_seconds = 30.0
        startup_message = "Validator running. Press Ctrl+C to stop."
        idle_sleep_seconds = 1.0

        logging.getLogger("httpx").setLevel(logging.WARN)

        settings = cls._load_settings_or_exit(settings_class)
        validator = cls(settings)

        with validator.start_runtime(shutdown_timeout_seconds=shutdown_timeout_seconds):
            print(startup_message)
            try:
                while True:
                    time.sleep(idle_sleep_seconds)
            except KeyboardInterrupt:
                pass

    def connect[T](
        self,
        source: Source[T],
        primary: Sink[T] | None = None,
        *,
        taps: Iterable[Sink[T]] = (),
    ) -> None:
        """Connect a source to an optional primary and isolated tap sinks."""
        tap_sinks = frozenset(taps)
        self.subnet_flow.pipes.connect(source, primary, taps=tap_sinks)
        self.subnet_flow.sources.add(source)
        if primary is not None:
            self.subnet_flow.sinks.add(primary)
        self.subnet_flow.sinks.update(tap_sinks)
        self._register_endpoint_owner(source)
        if primary is not None:
            self._register_endpoint_owner(primary)
        for tap in tap_sinks:
            self._register_endpoint_owner(tap)

    def _register_endpoint_owner(self, endpoint: Source[Any] | Sink[Any]) -> None:
        owner_node = endpoint.owner_node
        if owner_node is not None:
            self._connected_nodes[id(owner_node)] = owner_node

        owner_task = endpoint.owner_task
        if owner_task is not None:
            self._connected_tasks[id(owner_task)] = owner_task

    def _build_runtime(self) -> SubnetRuntime:
        connected_tasks = tuple(self._connected_tasks.values())
        for task in connected_tasks:
            self.connect(self.subnet_clock.source, taps=(task.block_beat,))

        task_flows = [task.internal_flow for task in connected_tasks]

        all_nodes = [
            *self._connected_nodes.values(),
            *(node for task in connected_tasks for node in task.internal_nodes()),
        ]
        deduped_nodes = tuple({id(node): node for node in all_nodes}.values())

        return SubnetBuilder(nodes=deduped_nodes).add_flows(self.subnet_flow).add_flows(*task_flows).build()

    @contextmanager
    def start_runtime(self, shutdown_timeout_seconds: float = 30.0) -> Generator[SubnetRuntime]:
        """Build and run the validator while scoping this instance's settings to the runtime."""
        if self.runtime is not None:
            raise RuntimeError("Runtime already started")
        with subnet_settings(self.settings):
            runtime = self._build_runtime()
            self.runtime = runtime
            try:
                with runtime.running(shutdown_timeout_seconds=shutdown_timeout_seconds) as running_runtime:
                    yield running_runtime
            finally:
                self.runtime = None

    @staticmethod
    def _load_settings_or_exit[SettingsModel: BaseSettings](settings_class: type[SettingsModel]) -> SettingsModel:
        try:
            return settings_class()  # type: ignore[call-arg]
        except ValidationError as e:
            fields = ", ".join(str(err["loc"][-1]) for err in e.errors() if err.get("loc"))
            log.error(f"Configuration error: missing or invalid fields: {fields}")
            log.error("Check your .env file or environment variables.")
            sys.exit(1)
