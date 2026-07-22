# pyright: basic

from datetime import UTC, datetime, timedelta
from typing import override

import pytest
from nexus_task_test_setup import (
    DummyBlockBeatSource,
    DummyExecutorCommunicator,
    DummyExecutorOutput,
    DummyExecutorPayload,
    DummyPayloadCreator,
    DummyTaskInput,
    NexusTaskTestSetup,
    NexusTaskTestSetupFactory,
    NoopRouter,
)
from utils import (
    CollectorActor,
    InMemoryTestTaskResultStoreProvider,
    build_neuron,
    get_executor_failure_results_for_block,
    get_successful_results_for_block,
    wait_until,
)

from nexus.v1 import (
    Actor,
    ActorBuilder,
    BlockNumber,
    Context,
    ContextId,
    ContextStore,
    ExecutorFailureException,
    ExecutorFailureTaskResult,
    Flow,
    MessagesToSend,
    NexusException,
    NexusTask,
    NexusTaskName,
    PayloadCreator,
    PipeToBus,
    ReceiveEvent,
    RetriesExhaustedException,
    RetryStrategy,
    SendEvent,
    Source,
    SubnetBuilder,
    SuccessfulTaskResult,
    Timestamped,
    TransformActor,
)


class PrefixingExecutorResultConverter(PayloadCreator[DummyExecutorOutput, str], ActorBuilder):
    @override
    def build_actor(self, *, pipe_to_bus: PipeToBus, context_store: ContextStore) -> Actor:
        return PrefixingExecutorResultConverterActor(
            spec=self,
            pipe_to_bus=pipe_to_bus,
            context_store=context_store,
        )


class PrefixingExecutorResultConverterActor(TransformActor[DummyExecutorOutput, str]):
    @override
    def _transform(self, ctx: Context, payload: DummyExecutorOutput) -> str:
        return f"public::{payload.result_text}"


class FailingExecutorResultConverter(PayloadCreator[DummyExecutorOutput, str], ActorBuilder):
    user_data_before_failure: list[dict[str, object]]

    def __init__(self, _id: str) -> None:
        super().__init__(_id)
        self.user_data_before_failure = []

    @override
    def build_actor(self, *, pipe_to_bus: PipeToBus, context_store: ContextStore) -> Actor:
        return FailingExecutorResultConverterActor(
            spec=self,
            pipe_to_bus=pipe_to_bus,
            context_store=context_store,
            user_data_before_failure=self.user_data_before_failure,
        )


class FailingExecutorResultConverterActor(TransformActor[DummyExecutorOutput, str]):
    user_data_before_failure: list[dict[str, object]]

    def __init__(
        self,
        *,
        spec: FailingExecutorResultConverter,
        pipe_to_bus: PipeToBus,
        context_store: ContextStore,
        user_data_before_failure: list[dict[str, object]],
    ) -> None:
        super().__init__(spec=spec, pipe_to_bus=pipe_to_bus, context_store=context_store)
        self.user_data_before_failure = user_data_before_failure

    @override
    def _transform(self, ctx: Context, payload: DummyExecutorOutput) -> str:
        self.user_data_before_failure.append(ctx.copy_user_data())
        raise NexusException("forced converter failure")


class UserDataErrorCollector(CollectorActor[NexusException]):
    user_data_on_error: list[dict[str, object]]

    def __init__(
        self,
        *,
        pipe_to_bus: PipeToBus,
        context_store: ContextStore,
        name: str = "user-data-error-collector",
    ) -> None:
        super().__init__(pipe_to_bus=pipe_to_bus, context_store=context_store, name=name)
        self.user_data_on_error = []

    @override
    def _handle(self, ctx: Context, event: ReceiveEvent[NexusException]) -> MessagesToSend:
        self.user_data_on_error.append(ctx.copy_user_data())
        return super()._handle(ctx, event)


def assert_stored_task_result(
    *,
    setup: NexusTaskTestSetup,
    stored_result: SuccessfulTaskResult[DummyExecutorPayload, DummyExecutorOutput, DummyExecutorOutput]
    | ExecutorFailureTaskResult[DummyExecutorPayload],
    input_payload: DummyTaskInput,
    expected_failure: bool = False,
    block_number: int,
) -> None:
    """Verify one stored task result entry for success output or executor failure output."""

    # Expected transformation contract comes from the dummy actors used in setup, not hardcoded test literals.
    expected_executor_payload = setup.payload_creator.to_executor_payload(input_payload)
    assert stored_result.executor_payload == expected_executor_payload

    # Stored executor output should match expected success output or represent executor failure.
    if expected_failure:
        assert isinstance(stored_result, ExecutorFailureTaskResult)
        assert isinstance(stored_result.executor_failure, ExecutorFailureException)
    else:
        assert isinstance(stored_result, SuccessfulTaskResult)
        expected_task_output = setup.executor_communicator.to_executor_output(expected_executor_payload)
        assert stored_result.executor_output == expected_task_output
        assert stored_result.executor_public_output == expected_task_output

    # Routing metadata should target the deterministic dummy neuron configured in test setup.
    assert stored_result.target.hotkey == "nexus-task-test-neuron"

    # Chain-time linkage and temporal ordering must be valid for persisted timestamps.
    assert stored_result.block_at_finish.block_number == BlockNumber(block_number)
    assert stored_result.processing_finished >= stored_result.processing_started

    now = datetime.now(tz=UTC)
    timestamp_tolerance = timedelta(seconds=10)
    # Persisted timestamps should be recent relative to test execution time.
    assert now - stored_result.processing_started <= timestamp_tolerance
    assert now - stored_result.processing_finished <= timestamp_tolerance


def assert_successful_task_result(
    *,
    setup: NexusTaskTestSetup,
    input_ctx_id: ContextId,
    block_number: int,
    expected_successful_results_for_epoch: int = 1,
    expected_executor_failure_events: int = 0,
) -> SuccessfulTaskResult[DummyExecutorPayload, DummyExecutorOutput, DummyExecutorOutput]:
    """Validate split outputs and return the matched stored + emitted task result."""
    # Runtime outcome: no terminal retries-exhausted error and one event on the success branches.
    assert len(setup.error_collector.received_events) == 0
    assert len(setup.successful_task_result_collector.received_events) == 1
    assert len(setup.executor_output_collector.received_events) == 1
    assert len(setup.executor_failure_collector.received_events) == expected_executor_failure_events

    task_result_event = setup.successful_task_result_collector.received_events[0]
    emitted_result = task_result_event.payload
    assert isinstance(emitted_result, SuccessfulTaskResult)
    # Task-result branch must use child context, while executor-output branch stays on original context.
    assert task_result_event.ctx_id != input_ctx_id

    executor_output_event = setup.executor_output_collector.received_events[0]
    assert executor_output_event.ctx_id == input_ctx_id
    assert executor_output_event.payload == emitted_result.executor_public_output

    # Store lookup is scoped to expected epoch and task identity.
    stored_results = get_successful_results_for_block(
        store=setup.task_result_store,
        task_name=setup.task.name,
        block_number=block_number,
    )
    # Expected number of persisted entries may vary by scenario (e.g. failure + retry success).
    assert len(stored_results) == expected_successful_results_for_epoch

    # Emitted result id must map to one stored entry and that entry must match emitted payload.
    stored_result = setup.task_result_store.get_task_result(
        task_name=setup.task.name,
        task_result_id=emitted_result.id,
    )
    assert isinstance(stored_result, SuccessfulTaskResult)
    assert stored_result == emitted_result
    return stored_result


def assert_executor_failure_result(
    *,
    setup: NexusTaskTestSetup,
    input_ctx_id: ContextId,
    block_number: int,
    expected_executor_failures_for_epoch: int = 1,
    expected_successful_task_results: int = 0,
    expected_executor_outputs: int = 0,
) -> ExecutorFailureTaskResult[DummyExecutorPayload]:
    """Validate executor-failure branch dispatch and return the stored failure record."""
    assert len(setup.executor_failure_collector.received_events) == 1
    assert len(setup.successful_task_result_collector.received_events) == expected_successful_task_results
    assert len(setup.executor_output_collector.received_events) == expected_executor_outputs
    assert len(setup.error_collector.received_events) == 0

    executor_failure_event = setup.executor_failure_collector.received_events[0]
    emitted_result = executor_failure_event.payload
    assert isinstance(emitted_result, ExecutorFailureTaskResult)
    assert executor_failure_event.ctx_id != input_ctx_id

    stored_results = get_executor_failure_results_for_block(
        store=setup.task_result_store,
        task_name=setup.task.name,
        block_number=block_number,
    )
    assert len(stored_results) == expected_executor_failures_for_epoch

    stored_result = setup.task_result_store.get_task_result(
        task_name=setup.task.name,
        task_result_id=emitted_result.id,
    )
    assert isinstance(stored_result, ExecutorFailureTaskResult)
    assert stored_result == emitted_result
    return emitted_result


def test_nexus_task_happy_path_routes_input_to_task_result(
    nexus_task_test_setup_factory: NexusTaskTestSetupFactory,
) -> None:
    # Scenario: with a BlockBeat available, an input should flow through retry -> payload creator -> router ->
    # communicator -> timestamper -> task-result pipeline and emit exactly one successful task result.
    setup = nexus_task_test_setup_factory()
    block_number = 123
    input_payload = DummyTaskInput(
        request_id="req-1",
        payload_text="hello",
    )

    with setup.running():
        setup.send_block_beat(block_number=block_number)
        input_ctx_id = setup.send_input(input_payload=input_payload)
        wait_until(
            lambda: (
                len(setup.successful_task_result_collector.received_events) == 1
                and len(setup.executor_output_collector.received_events) == 1
            ),
            timeout=2.0,
        )

    stored_success_result = assert_successful_task_result(
        setup=setup,
        input_ctx_id=input_ctx_id,
        block_number=block_number,
    )
    assert_stored_task_result(
        setup=setup,
        stored_result=stored_success_result,
        input_payload=input_payload,
        block_number=block_number,
    )


def test_nexus_task_applies_non_trivial_executor_result_converter() -> None:
    retry = RetryStrategy[DummyTaskInput](
        "nexus-task-test-retry",
        max_attempts=3,
        delay=timedelta(milliseconds=5),
    )
    payload_creator = DummyPayloadCreator("nexus-task-test-payload-creator")
    router = NoopRouter(
        "nexus-task-test-router",
        target=build_neuron(uid=1, hotkey="nexus-task-test-neuron", validator_permit=False),
    )
    executor_communicator = DummyExecutorCommunicator("nexus-task-test-communicator")
    task_result_store_provider = InMemoryTestTaskResultStoreProvider[
        DummyExecutorPayload,
        DummyExecutorOutput,
        str,
    ]()
    executor_result_converter = PrefixingExecutorResultConverter("nexus-task-test-executor-result-converter")

    task = NexusTask[DummyTaskInput, DummyExecutorPayload, DummyExecutorOutput, str](
        name=NexusTaskName("test-nexus-task-with-converter"),
        retry=retry,
        payload_creator=payload_creator,
        router=router,
        executor_communicator=executor_communicator,
        task_result_store_provider=task_result_store_provider,
        executor_result_converter=executor_result_converter,
    )

    builder = SubnetBuilder(nodes=task.internal_nodes())
    successful_task_result_collector = CollectorActor[
        SuccessfulTaskResult[DummyExecutorPayload, DummyExecutorOutput, str]
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
    executor_output_collector = CollectorActor[str](
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

    input_payload = DummyTaskInput(request_id="req-converted-output", payload_text="hello")
    block_number = 123

    with runtime.running():
        with runtime.context_store.create_context() as context:
            input_ctx_id = context.id
        runtime.pipe_to_bus.put(
            SendEvent(
                ctx_id=input_ctx_id,
                source=block_beat_source.source,
                payload=block_beat_source.beat(block_number),
            )
        )
        runtime.pipe_to_bus.put(
            SendEvent(
                ctx_id=input_ctx_id,
                source=input_source,
                payload=input_payload,
            )
        )
        wait_until(
            lambda: (
                len(successful_task_result_collector.received_events) == 1
                and len(executor_output_collector.received_events) == 1
            ),
            timeout=2.0,
        )

    assert len(error_collector.received_events) == 0
    assert len(executor_failure_collector.received_events) == 0
    task_result_event = successful_task_result_collector.received_events[0]
    executor_output_event = executor_output_collector.received_events[0]

    assert task_result_event.ctx_id != input_ctx_id
    assert executor_output_event.ctx_id == input_ctx_id

    emitted_result = task_result_event.payload
    expected_payload = payload_creator.to_executor_payload(input_payload)
    expected_output = executor_communicator.to_executor_output(expected_payload)

    assert emitted_result.executor_output == expected_output
    assert emitted_result.executor_public_output == f"public::{expected_output.result_text}"
    assert executor_output_event.payload == f"public::{expected_output.result_text}"

    stored_results = get_successful_results_for_block(
        store=task_result_store_provider.get_task_result_store(),
        task_name=task.name,
        block_number=block_number,
    )
    assert len(stored_results) == 1
    assert stored_results[0] == emitted_result


def test_nexus_task_emits_error_when_executor_result_converter_fails_without_retrying() -> None:
    retry = RetryStrategy[DummyTaskInput](
        "nexus-task-test-retry",
        max_attempts=3,
        delay=timedelta(milliseconds=5),
    )
    payload_creator = DummyPayloadCreator("nexus-task-test-payload-creator")
    router = NoopRouter(
        "nexus-task-test-router",
        target=build_neuron(uid=1, hotkey="nexus-task-test-neuron", validator_permit=False),
    )
    executor_communicator = DummyExecutorCommunicator("nexus-task-test-communicator")
    task_result_store_provider = InMemoryTestTaskResultStoreProvider[
        DummyExecutorPayload,
        DummyExecutorOutput,
        str,
    ]()
    executor_result_converter = FailingExecutorResultConverter("nexus-task-test-failing-executor-result-converter")

    task = NexusTask[DummyTaskInput, DummyExecutorPayload, DummyExecutorOutput, str](
        name=NexusTaskName("test-nexus-task-with-failing-converter"),
        retry=retry,
        payload_creator=payload_creator,
        router=router,
        executor_communicator=executor_communicator,
        task_result_store_provider=task_result_store_provider,
        executor_result_converter=executor_result_converter,
    )

    builder = SubnetBuilder(nodes=task.internal_nodes())
    successful_task_result_collector = CollectorActor[
        SuccessfulTaskResult[DummyExecutorPayload, DummyExecutorOutput, str]
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
    executor_output_collector = CollectorActor[str](
        pipe_to_bus=builder.pipe_to_bus,
        context_store=builder.context_store,
        name="nexus-task-executor-output-collector",
    )
    error_collector = UserDataErrorCollector(
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

    input_payload = DummyTaskInput(request_id="req-failing-converter", payload_text="hello")
    block_number = 123

    with runtime.running():
        with runtime.context_store.create_context() as context:
            input_ctx_id = context.id
        runtime.pipe_to_bus.put(
            SendEvent(
                ctx_id=input_ctx_id,
                source=block_beat_source.source,
                payload=block_beat_source.beat(block_number),
            )
        )
        runtime.pipe_to_bus.put(
            SendEvent(
                ctx_id=input_ctx_id,
                source=input_source,
                payload=input_payload,
            )
        )
        wait_until(
            lambda: len(error_collector.received_events) == 1,
            timeout=2.0,
        )

    assert len(successful_task_result_collector.received_events) == 0
    assert len(executor_failure_collector.received_events) == 0
    assert len(executor_output_collector.received_events) == 0
    assert len(error_collector.received_events) == 1
    assert len(executor_result_converter.user_data_before_failure) == 1
    user_data_before_failure = executor_result_converter.user_data_before_failure[0]
    timestamped_keys = [key for key, value in user_data_before_failure.items() if isinstance(value, Timestamped)]
    assert len(timestamped_keys) == 1
    pending_result_key = timestamped_keys[0]

    assert len(error_collector.user_data_on_error) == 1
    user_data_on_error = error_collector.user_data_on_error[0]
    assert pending_result_key in user_data_on_error
    assert user_data_on_error[pending_result_key] is None
    assert payload_creator.attempts_by_ctx[input_ctx_id] == 1
    assert executor_communicator.attempts_by_ctx[input_ctx_id] == 1

    converter_error = error_collector.received_events[0]
    assert converter_error.ctx_id == input_ctx_id
    assert "forced converter failure" in str(converter_error.payload)
    assert not isinstance(converter_error.payload, RetriesExhaustedException)

    stored_results = get_successful_results_for_block(
        store=task_result_store_provider.get_task_result_store(),
        task_name=task.name,
        block_number=block_number,
    )
    assert len(stored_results) == 0


def test_nexus_task_waits_for_block_beat_before_emitting_result(
    nexus_task_test_setup_factory: NexusTaskTestSetupFactory,
) -> None:
    # Scenario: communicator output is produced first, but no successful-task-result branch should be emitted
    # until a BlockBeat arrives
    # at the NexusTask block_beat sink.
    setup = nexus_task_test_setup_factory()
    input_payload = DummyTaskInput(
        request_id="req-waits-for-block-beat",
        payload_text="hello",
    )
    block_number = 222

    with setup.running():
        input_ctx_id = setup.send_input(input_payload=input_payload)
        with pytest.raises(AssertionError):
            wait_until(
                lambda: (
                    len(setup.successful_task_result_collector.received_events) == 1
                    or len(setup.executor_output_collector.received_events) == 1
                ),
                timeout=0.2,
                interval=0.05,
            )
        assert len(setup.successful_task_result_collector.received_events) == 0
        assert len(setup.executor_failure_collector.received_events) == 0
        assert len(setup.executor_output_collector.received_events) == 0

        setup.send_block_beat(block_number=block_number)
        wait_until(
            lambda: (
                len(setup.successful_task_result_collector.received_events) == 1
                and len(setup.executor_output_collector.received_events) == 1
            ),
            timeout=2.0,
        )

    stored_success_result = assert_successful_task_result(
        setup=setup,
        input_ctx_id=input_ctx_id,
        block_number=block_number,
    )
    assert_stored_task_result(
        setup=setup,
        stored_result=stored_success_result,
        input_payload=input_payload,
        block_number=block_number,
    )


def test_nexus_task_retries_after_payload_creator_failure(
    nexus_task_test_setup_factory: NexusTaskTestSetupFactory,
) -> None:
    # Scenario: payload creator emits an error and NexusTask forwards it to retry.failed_attempt, causing the next
    # attempt to be re-issued via retry.next_attempt.
    payload_creator = DummyPayloadCreator(
        "nexus-task-test-fails-first-payload-creator",
        fail_first_n_attempts=1,
    )
    setup = nexus_task_test_setup_factory(payload_creator=payload_creator)
    input_payload = DummyTaskInput(
        request_id="req-retry-on-payload-failure",
        payload_text="hello",
    )
    block_number = 321

    with setup.running():
        setup.send_block_beat(block_number=block_number)
        input_ctx_id = setup.send_input(input_payload=input_payload)
        wait_until(
            lambda: (
                len(setup.successful_task_result_collector.received_events) == 1
                and len(setup.executor_output_collector.received_events) == 1
            ),
            timeout=2.0,
        )

    stored_success_result = assert_successful_task_result(
        setup=setup,
        input_ctx_id=input_ctx_id,
        block_number=block_number,
    )
    assert_stored_task_result(
        setup=setup,
        stored_result=stored_success_result,
        input_payload=input_payload,
        block_number=block_number,
    )
    assert payload_creator.attempts_by_ctx[input_ctx_id] == 2


def test_nexus_task_retries_after_router_failure(
    nexus_task_test_setup_factory: NexusTaskTestSetupFactory,
) -> None:
    # Scenario: router emits an error and NexusTask forwards it to retry.failed_attempt so processing can continue
    # from the retry strategy instead of terminating immediately.
    router = NoopRouter(
        "nexus-task-test-fails-first-router",
        target=build_neuron(uid=1, hotkey="nexus-task-test-neuron", validator_permit=False),
        fail_first_n_attempts=1,
    )
    setup = nexus_task_test_setup_factory(router=router)
    input_payload = DummyTaskInput(
        request_id="req-retry-on-router-failure",
        payload_text="hello",
    )
    block_number = 654

    with setup.running():
        setup.send_block_beat(block_number=block_number)
        input_ctx_id = setup.send_input(input_payload=input_payload)
        wait_until(
            lambda: (
                len(setup.successful_task_result_collector.received_events) == 1
                and len(setup.executor_output_collector.received_events) == 1
            ),
            timeout=2.0,
        )

    stored_success_result = assert_successful_task_result(
        setup=setup,
        input_ctx_id=input_ctx_id,
        block_number=block_number,
    )
    assert_stored_task_result(
        setup=setup,
        stored_result=stored_success_result,
        input_payload=input_payload,
        block_number=block_number,
    )
    assert router.attempts_by_ctx[input_ctx_id] == 2


def test_nexus_task_retries_after_communicator_internal_error(
    nexus_task_test_setup_factory: NexusTaskTestSetupFactory,
) -> None:
    # Scenario: executor communicator emits a framework/internal error and NexusTask routes it into retry.failed_attempt
    # to trigger retry scheduling.
    communicator = DummyExecutorCommunicator(
        "nexus-task-test-fails-first-communicator",
        fail_first_n_internal_errors=1,
    )
    setup = nexus_task_test_setup_factory(executor_communicator=communicator)
    input_payload = DummyTaskInput(
        request_id="req-retry-on-communicator-internal-error",
        payload_text="hello",
    )
    block_number = 987

    with setup.running():
        setup.send_block_beat(block_number=block_number)
        input_ctx_id = setup.send_input(input_payload=input_payload)
        wait_until(
            lambda: (
                len(setup.successful_task_result_collector.received_events) == 1
                and len(setup.executor_output_collector.received_events) == 1
            ),
            timeout=2.0,
        )

    stored_success_result = assert_successful_task_result(
        setup=setup,
        input_ctx_id=input_ctx_id,
        block_number=block_number,
    )
    assert_stored_task_result(
        setup=setup,
        stored_result=stored_success_result,
        input_payload=input_payload,
        block_number=block_number,
    )
    assert communicator.attempts_by_ctx[input_ctx_id] == 2


def test_nexus_task_emits_executor_failure_on_child_context_persists_it_and_retries(
    nexus_task_test_setup_factory: NexusTaskTestSetupFactory,
) -> None:
    # Scenario: communicator returns an executor-side failure result, TaskResultStorer persists it and raises
    # RetryTaskAfterExecutorFailureException, and NexusTask wiring sends that error back into retry.failed_attempt.
    communicator = DummyExecutorCommunicator(
        "nexus-task-test-fails-first-with-executor-failure",
        fail_first_n_executor_failures=1,
    )
    setup = nexus_task_test_setup_factory(executor_communicator=communicator)
    input_payload = DummyTaskInput(
        request_id="req-retry-after-executor-failure-result",
        payload_text="hello",
    )
    block_number = 741

    with setup.running():
        input_ctx_id = setup.send_input(input_payload=input_payload)
        setup.send_block_beat(block_number=block_number)
        wait_until(lambda: len(setup.executor_failure_collector.received_events) == 1, timeout=2.0)
        wait_until(
            lambda: (
                len(setup.successful_task_result_collector.received_events) == 1
                and len(setup.executor_output_collector.received_events) == 1
            ),
            timeout=2.0,
        )

    failed_stored_result = assert_executor_failure_result(
        setup=setup,
        input_ctx_id=input_ctx_id,
        block_number=block_number,
        expected_successful_task_results=1,
        expected_executor_outputs=1,
    )
    assert_stored_task_result(
        setup=setup,
        stored_result=failed_stored_result,
        input_payload=input_payload,
        expected_failure=True,
        block_number=block_number,
    )

    stored_success_result = assert_successful_task_result(
        setup=setup,
        input_ctx_id=input_ctx_id,
        block_number=block_number,
        expected_successful_results_for_epoch=1,
        expected_executor_failure_events=1,
    )
    assert_stored_task_result(
        setup=setup,
        stored_result=stored_success_result,
        input_payload=input_payload,
        block_number=block_number,
    )
    assert communicator.attempts_by_ctx[input_ctx_id] == 2

    stored_results = get_executor_failure_results_for_block(
        store=setup.task_result_store,
        task_name=setup.task.name,
        block_number=block_number,
    )
    assert stored_results == (failed_stored_result,)


def test_nexus_task_emits_retries_exhausted_when_max_attempts_are_hit(
    nexus_task_test_setup_factory: NexusTaskTestSetupFactory,
) -> None:
    # Scenario: repeated failures from internal stages reach retry.max_attempts and NexusTask exposes the terminal
    # RetriesExhaustedException on task.error.
    max_attempts = 2
    retry = RetryStrategy[DummyTaskInput](
        "nexus-task-test-retry-exhausted",
        max_attempts=max_attempts,
        delay=timedelta(milliseconds=5),
    )
    payload_creator = DummyPayloadCreator(
        "nexus-task-test-always-failing-payload-creator",
        fail_first_n_attempts=max_attempts,
    )
    setup = nexus_task_test_setup_factory(retry=retry, payload_creator=payload_creator)
    input_payload = DummyTaskInput(
        request_id="req-retries-exhausted",
        payload_text="hello",
    )

    with setup.running():
        input_ctx_id = setup.send_input(input_payload=input_payload)
        wait_until(lambda: len(setup.error_collector.received_events) == 1, timeout=2.0)

    assert len(setup.successful_task_result_collector.received_events) == 0
    assert len(setup.executor_failure_collector.received_events) == 0
    assert len(setup.executor_output_collector.received_events) == 0
    assert len(setup.error_collector.received_events) == 1

    exhausted_event = setup.error_collector.received_events[0]
    assert exhausted_event.ctx_id == input_ctx_id
    assert isinstance(exhausted_event.payload, RetriesExhaustedException)
    assert f"All {max_attempts} retry attempts exhausted" in str(exhausted_event.payload)
    assert payload_creator.attempts_by_ctx[input_ctx_id] == max_attempts
