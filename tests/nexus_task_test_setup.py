# pyright: basic

"""
Reusable NexusTask wiring setup for internal flow tests.

The setup intentionally uses distinct payload types across retry/payload/router/
communicator stages so wiring mistakes are caught by static type checking.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Protocol, override

from fake_pylon_client import FakePylonClientProvider
from pylon_client.artanis.v1 import Neuron
from utils import CollectorActor, InMemoryTestTaskResultStoreProvider, build_neuron, dummy_block_beat

from nexus.v1 import (
    Actor,
    ActorBuilder,
    BlockBeat,
    CommunicatorActor,
    Context,
    ContextId,
    ContextStore,
    ExecutorCommunicator,
    ExecutorFailureTaskResult,
    Flow,
    MessagesToSend,
    NeuronRouter,
    NexusException,
    NexusTaskName,
    NoopPayloadCreator,
    PayloadCreator,
    PipeToBus,
    ProcessedInput,
    ReceiveEvent,
    RetryStrategy,
    Routed,
    SendEvent,
    Source,
    SubnetBuilder,
    SubnetRuntime,
    SuccessfulTaskResult,
    TaskResultStore,
    TransformActor,
)

if TYPE_CHECKING:
    from nexus.v1 import NexusTask

DEFAULT_RUNTIME_SHUTDOWN_TIMEOUT_SECONDS = 1.5


@dataclass(frozen=True)
class DummyTaskInput:
    request_id: str
    payload_text: str


@dataclass(frozen=True)
class DummyExecutorPayload:
    request_id: str
    transformed_text: str


@dataclass(frozen=True)
class DummyExecutorOutput:
    result_text: str


class DummyBlockBeatSource:
    """Manual source used by tests to emit deterministic `BlockBeat` events."""

    source: Source[BlockBeat]

    def __init__(self, _id: str = "test-block-beat-source") -> None:
        self.source = Source[BlockBeat](_id)

    def beat(self, block_number: int) -> BlockBeat:
        return dummy_block_beat(block_number)


class DummyPayloadCreator(PayloadCreator[DummyTaskInput, DummyExecutorPayload], ActorBuilder):
    """Payload creator used in NexusTask tests, with optional fail-first behavior."""

    fail_first_n_attempts: int
    attempts_by_ctx: dict[ContextId, int]

    def __init__(self, _id: str, *, fail_first_n_attempts: int = 0) -> None:
        super().__init__(_id)
        if fail_first_n_attempts < 0:
            raise ValueError("fail_first_n_attempts must be >= 0")
        self.fail_first_n_attempts = fail_first_n_attempts
        self.attempts_by_ctx = {}

    def to_executor_payload(self, payload: DummyTaskInput) -> DummyExecutorPayload:
        return DummyExecutorPayload(
            request_id=payload.request_id,
            transformed_text=payload.payload_text.upper(),
        )

    @override
    def build_actor(self, *, pipe_to_bus: PipeToBus, context_store: ContextStore) -> Actor:
        return DummyPayloadCreatorActor(spec=self, pipe_to_bus=pipe_to_bus, context_store=context_store)


class DummyPayloadCreatorActor(TransformActor[DummyTaskInput, DummyExecutorPayload]):
    creator_spec: DummyPayloadCreator

    def __init__(
        self,
        *,
        spec: DummyPayloadCreator,
        pipe_to_bus: PipeToBus,
        context_store: ContextStore,
    ) -> None:
        super().__init__(spec=spec, pipe_to_bus=pipe_to_bus, context_store=context_store)
        self.creator_spec = spec

    @override
    def _transform(self, ctx: Context, payload: DummyTaskInput) -> DummyExecutorPayload:
        next_attempt = self.creator_spec.attempts_by_ctx.get(ctx.id, 0) + 1
        self.creator_spec.attempts_by_ctx[ctx.id] = next_attempt
        if next_attempt <= self.creator_spec.fail_first_n_attempts:
            raise NexusException("forced failure in payload creator")
        return self.creator_spec.to_executor_payload(payload)


class NoopRouter(NeuronRouter[DummyExecutorPayload], ActorBuilder):
    """Router that always routes to the same neuron and does no external calls."""

    target: Neuron
    fail_first_n_attempts: int
    attempts_by_ctx: dict[ContextId, int]

    def __init__(
        self,
        _id: str,
        *,
        target: Neuron,
        fail_first_n_attempts: int = 0,
    ) -> None:
        super().__init__(
            _id,
            netuid=1,
            pylon_client_provider=FakePylonClientProvider(neurons=[target]),
        )
        if fail_first_n_attempts < 0:
            raise ValueError("fail_first_n_attempts must be >= 0")
        self.target = target
        self.fail_first_n_attempts = fail_first_n_attempts
        self.attempts_by_ctx = {}

    @override
    def build_actor(self, *, pipe_to_bus: PipeToBus, context_store: ContextStore) -> Actor:
        return NoopRouterActor(spec=self, pipe_to_bus=pipe_to_bus, context_store=context_store)


class NoopRouterActor(TransformActor[DummyExecutorPayload, Routed[DummyExecutorPayload]]):
    router_spec: NoopRouter

    def __init__(
        self,
        *,
        spec: NoopRouter,
        pipe_to_bus: PipeToBus,
        context_store: ContextStore,
    ) -> None:
        super().__init__(spec=spec, pipe_to_bus=pipe_to_bus, context_store=context_store)
        self.router_spec = spec

    @override
    def _transform(self, ctx: Context, payload: DummyExecutorPayload) -> Routed[DummyExecutorPayload]:
        next_attempt = self.router_spec.attempts_by_ctx.get(ctx.id, 0) + 1
        self.router_spec.attempts_by_ctx[ctx.id] = next_attempt
        if next_attempt <= self.router_spec.fail_first_n_attempts:
            raise NexusException("forced failure in router")
        return Routed(input=payload, target=self.router_spec.target)


class DummyExecutorCommunicator(ExecutorCommunicator[DummyExecutorPayload, DummyExecutorOutput], ActorBuilder):
    """Communicator that transforms payload into output without network communication."""

    fail_first_n_internal_errors: int
    fail_first_n_executor_failures: int
    attempts_by_ctx: dict[ContextId, int]

    def __init__(
        self,
        _id: str,
        *,
        fail_first_n_internal_errors: int = 0,
        fail_first_n_executor_failures: int = 0,
    ) -> None:
        super().__init__(
            _id,
            input_model=DummyExecutorPayload,
            output_model=DummyExecutorOutput,
        )
        if fail_first_n_internal_errors < 0:
            raise ValueError("fail_first_n_internal_errors must be >= 0")
        if fail_first_n_executor_failures < 0:
            raise ValueError("fail_first_n_executor_failures must be >= 0")
        self.fail_first_n_internal_errors = fail_first_n_internal_errors
        self.fail_first_n_executor_failures = fail_first_n_executor_failures
        self.attempts_by_ctx = {}

    def to_executor_output(self, payload: DummyExecutorPayload) -> DummyExecutorOutput:
        return DummyExecutorOutput(result_text=f"executor::{payload.transformed_text}")

    @override
    def build_actor(self, *, pipe_to_bus: PipeToBus, context_store: ContextStore) -> Actor:
        return TrivialExecutorCommunicatorActor(spec=self, pipe_to_bus=pipe_to_bus, context_store=context_store)


class TrivialExecutorCommunicatorActor(CommunicatorActor[DummyExecutorPayload, DummyExecutorOutput]):
    communicator_spec: DummyExecutorCommunicator

    def __init__(
        self,
        *,
        spec: DummyExecutorCommunicator,
        pipe_to_bus: PipeToBus,
        context_store: ContextStore,
    ) -> None:
        super().__init__(spec=spec, pipe_to_bus=pipe_to_bus, context_store=context_store)
        self.communicator_spec = spec

    @override
    def handle_input(self, _: Context, event: ReceiveEvent[Routed[DummyExecutorPayload]]) -> MessagesToSend:
        next_attempt = self.communicator_spec.attempts_by_ctx.get(event.ctx_id, 0) + 1
        self.communicator_spec.attempts_by_ctx[event.ctx_id] = next_attempt
        if next_attempt <= self.communicator_spec.fail_first_n_internal_errors:
            self._emit_internal_error(event.ctx_id, NexusException("forced internal error in communicator"))
            return ()
        if next_attempt <= (
            self.communicator_spec.fail_first_n_internal_errors + self.communicator_spec.fail_first_n_executor_failures
        ):
            self._emit_executor_error(event.ctx_id, NexusException("forced executor failure in communicator"))
            return ()
        self._emit_processed(
            event.ctx_id,
            self.communicator_spec.to_executor_output(event.payload.input),
        )
        return ()


type DummyProcessedInput = ProcessedInput[Routed[DummyExecutorPayload], DummyExecutorOutput]


@dataclass(frozen=True)
class NexusTaskTestSetup:
    """Runtime and endpoints needed to drive NexusTask wiring tests."""

    task: NexusTask[DummyTaskInput, DummyExecutorPayload, DummyExecutorOutput]
    payload_creator: DummyPayloadCreator
    executor_communicator: DummyExecutorCommunicator
    task_result_store: TaskResultStore[DummyExecutorPayload, DummyExecutorOutput, DummyExecutorOutput]
    runtime: SubnetRuntime
    successful_task_result_collector: CollectorActor[
        SuccessfulTaskResult[DummyExecutorPayload, DummyExecutorOutput, DummyExecutorOutput]
    ]
    executor_failure_collector: CollectorActor[ExecutorFailureTaskResult[DummyExecutorPayload]]
    executor_output_collector: CollectorActor[DummyExecutorOutput]
    error_collector: CollectorActor[NexusException]
    input_source: Source[DummyTaskInput]
    block_beat_source: DummyBlockBeatSource

    @contextmanager
    def running(
        self,
        *,
        shutdown_timeout_seconds: float = DEFAULT_RUNTIME_SHUTDOWN_TIMEOUT_SECONDS,
    ) -> Iterator[None]:
        with self.runtime.running(shutdown_timeout_seconds=shutdown_timeout_seconds):
            yield

    def send_input(
        self,
        *,
        input_payload: DummyTaskInput,
        ctx_id: ContextId | None = None,
    ) -> ContextId:
        resolved_ctx_id = self._resolve_ctx_id(ctx_id)
        self.runtime.pipe_to_bus.put(
            SendEvent(
                ctx_id=resolved_ctx_id,
                source=self.input_source,
                payload=input_payload,
            )
        )
        return resolved_ctx_id

    def send_block_beat(
        self,
        *,
        block_number: int,
        ctx_id: ContextId | None = None,
    ) -> ContextId:
        resolved_ctx_id = self._resolve_ctx_id(ctx_id)
        self.runtime.pipe_to_bus.put(
            SendEvent(
                ctx_id=resolved_ctx_id,
                source=self.block_beat_source.source,
                payload=self.block_beat_source.beat(block_number),
            )
        )
        return resolved_ctx_id

    def _resolve_ctx_id(self, ctx_id: ContextId | None) -> ContextId:
        if ctx_id is not None:
            return ctx_id
        with self.runtime.context_store.create_context() as context:
            return context.id


class NexusTaskTestSetupFactory(Protocol):
    def __call__(
        self,
        *,
        retry: RetryStrategy[DummyTaskInput] | None = None,
        payload_creator: DummyPayloadCreator | None = None,
        router: NoopRouter | None = None,
        executor_communicator: DummyExecutorCommunicator | None = None,
    ) -> NexusTaskTestSetup: ...


def build_nexus_task_test_setup(
    *,
    retry: RetryStrategy[DummyTaskInput] | None = None,
    payload_creator: DummyPayloadCreator | None = None,
    router: NoopRouter | None = None,
    executor_communicator: DummyExecutorCommunicator | None = None,
) -> NexusTaskTestSetup:
    """Construct a full NexusTask runtime using local, deterministic test actors."""

    from nexus.v1 import NexusTask

    task_result_store_provider = InMemoryTestTaskResultStoreProvider[
        DummyExecutorPayload,
        DummyExecutorOutput,
        DummyExecutorOutput,
    ]()
    resolved_retry = retry or RetryStrategy[DummyTaskInput](
        "nexus-task-test-retry",
        max_attempts=3,
        delay=timedelta(milliseconds=5),
    )
    resolved_payload_creator = payload_creator or DummyPayloadCreator("nexus-task-test-payload-creator")
    resolved_router = router or NoopRouter(
        "nexus-task-test-router",
        target=build_neuron(uid=1, hotkey="nexus-task-test-neuron", validator_permit=False),
    )
    resolved_executor_communicator = executor_communicator or DummyExecutorCommunicator("nexus-task-test-communicator")

    task = NexusTask[DummyTaskInput, DummyExecutorPayload, DummyExecutorOutput](
        name=NexusTaskName("test-nexus-task"),
        retry=resolved_retry,
        payload_creator=resolved_payload_creator,
        router=resolved_router,
        executor_communicator=resolved_executor_communicator,
        task_result_store_provider=task_result_store_provider,
        executor_result_converter=NoopPayloadCreator("nexus-task-test-executor-result-converter"),
    )

    builder = SubnetBuilder(nodes=task.internal_nodes())
    successful_task_result_collector = CollectorActor[
        SuccessfulTaskResult[DummyExecutorPayload, DummyExecutorOutput, DummyExecutorOutput]
    ](
        pipe_to_bus=builder.pipe_to_bus,
        context_store=builder.context_store,
        name="nexus-task-successful-task-result-collector",
    )
    executor_failure_collector = CollectorActor[ExecutorFailureTaskResult[DummyExecutorPayload]](
        pipe_to_bus=builder.pipe_to_bus,
        context_store=builder.context_store,
        name="nexus-task-executor-failure-collector",
    )
    executor_output_collector = CollectorActor[DummyExecutorOutput](
        pipe_to_bus=builder.pipe_to_bus,
        context_store=builder.context_store,
        name="nexus-task-executor-output-collector",
    )
    error_collector = CollectorActor[NexusException](
        pipe_to_bus=builder.pipe_to_bus,
        context_store=builder.context_store,
        name="nexus-task-error-collector",
    )

    input_source = Source[DummyTaskInput]("nexus-task-test-input-source")
    block_beat_source = DummyBlockBeatSource("nexus-task-test-block-beat-source")

    runtime = (
        builder.add_flows(
            task.internal_flow,
            Flow.from_connectable(input_source).then(task.input),
            Flow.from_connectable(block_beat_source.source).then(taps=[task.block_beat]),
            Flow.from_connectable(task.successful_task_result).then(successful_task_result_collector.sink),
            Flow.from_connectable(task.executor_failure).then(executor_failure_collector.sink),
            Flow.from_connectable(task.executor_output).then(executor_output_collector.sink),
            Flow.from_connectable(task.error).then(error_collector.sink),
        )
        .add_actors(
            successful_task_result_collector,
            executor_failure_collector,
            executor_output_collector,
            error_collector,
        )
        .build()
    )

    return NexusTaskTestSetup(
        task=task,
        payload_creator=resolved_payload_creator,
        executor_communicator=resolved_executor_communicator,
        task_result_store=task_result_store_provider.get_task_result_store(),
        runtime=runtime,
        successful_task_result_collector=successful_task_result_collector,
        executor_failure_collector=executor_failure_collector,
        executor_output_collector=executor_output_collector,
        error_collector=error_collector,
        input_source=input_source,
        block_beat_source=block_beat_source,
    )
