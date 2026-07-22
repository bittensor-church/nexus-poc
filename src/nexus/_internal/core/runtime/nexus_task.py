from nexus._internal.actors import ExecutorCommunicator, NeuronRouter, Routed, TimestamperNode
from nexus._internal.actors.chain_beat.block_beat import BlockBeat
from nexus._internal.actors.executor_communicator import ProcessedInput
from nexus._internal.actors.mux import Mux2
from nexus._internal.actors.payload_creator import PayloadCreator
from nexus._internal.actors.retry_strategy import RetriesExhaustedException, RetryStrategy
from nexus._internal.actors.task_result_dispatcher import TaskResultDispatcher
from nexus._internal.actors.task_result_preparer import TaskResultPreparer
from nexus._internal.actors.task_result_store_provider import (
    DEFAULT_TASK_RESULT_STORE_PROVIDER,
    TaskResultStoreProvider,
)
from nexus._internal.actors.task_result_storer import ExecutorFailureTaskResultStorer, SuccessfulTaskResultStorer
from nexus._internal.core.dsl.flow import Flow
from nexus._internal.core.dsl.nodes import Node, NodeId, NodeSinks, NodeSources, Sink, SinkName, Source, SourceName
from nexus._internal.core.runtime.nexus_task_types import NexusTaskName
from nexus._internal.core.runtime.task_result_store import ExecutorFailureTaskResult, SuccessfulTaskResult
from nexus._internal.utils.exceptions import NexusException


class NexusTask[Input, ExecutorPayload, ExecutorOutput, ExecutorPublicOutput = ExecutorOutput]:
    """
    Reusable task pipeline with typed task-result branches and success-only executor output.

    Runs `Input` through payload creation, routing, executor communication, and result
    conversion, with timing and retry. Every retry re-emits the original input, so the router
    picks freshly each attempt. Successful runs and reported executor failures are persisted
    under `name`; framework-side failures are not. Once retries are exhausted, `error` fires
    and none of the result sources emit.

    sink input: the unit of work to execute
    sink block_beat: timestamping clock
    source successful_task_result: persisted success branch, emitted with a new child context
    source executor_failure: persisted executor-failure branch, emitted with a new child context
    source executor_output: converted executor result for downstream consumers; success-only
    source error: framework-side failures — retries exhausted, or task result preparation failed
    """

    name: NexusTaskName
    input: Sink[Input]
    block_beat: Sink[BlockBeat]
    successful_task_result: Source[SuccessfulTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]]
    executor_failure: Source[ExecutorFailureTaskResult[ExecutorPayload]]
    executor_output: Source[ExecutorPublicOutput]
    error: Source[NexusException]
    internal_flow: Flow

    timestamper: TimestamperNode[
        Routed[ExecutorPayload],
        ProcessedInput[Routed[ExecutorPayload], ExecutorOutput],
    ]
    retry: RetryStrategy[Input]
    payload_creator: PayloadCreator[Input, ExecutorPayload]
    router: NeuronRouter[ExecutorPayload]
    executor_communicator: ExecutorCommunicator[ExecutorPayload, ExecutorOutput]
    task_result_preparer: TaskResultPreparer[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]
    successful_task_result_storer: SuccessfulTaskResultStorer[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]
    executor_failure_task_result_storer: ExecutorFailureTaskResultStorer[
        ExecutorPayload,
        ExecutorOutput,
        ExecutorPublicOutput,
    ]
    task_result_dispatcher: TaskResultDispatcher[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]
    executor_result_converter: PayloadCreator[ExecutorOutput, ExecutorPublicOutput]
    error_mux: Mux2[NexusException, RetriesExhaustedException, NexusException]

    def __init__(
        self,
        *,
        name: NexusTaskName,
        retry: RetryStrategy[Input],
        payload_creator: PayloadCreator[Input, ExecutorPayload],
        router: NeuronRouter[ExecutorPayload],
        executor_communicator: ExecutorCommunicator[ExecutorPayload, ExecutorOutput],
        executor_result_converter: PayloadCreator[ExecutorOutput, ExecutorPublicOutput],
        task_result_store_provider: TaskResultStoreProvider[
            ExecutorPayload,
            ExecutorOutput,
            ExecutorPublicOutput,
        ]
        | None = None,
    ) -> None:
        self.name = name
        self.timestamper = TimestamperNode[
            Routed[ExecutorPayload],
            ProcessedInput[Routed[ExecutorPayload], ExecutorOutput],
        ](f"{name}-timestamper")
        self.retry = retry
        self.payload_creator = payload_creator
        self.router = router
        self.executor_communicator = executor_communicator
        self.executor_result_converter = executor_result_converter
        self.task_result_preparer = TaskResultPreparer[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput](
            _id=NodeId(f"{name}-task-result-preparer")
        )
        if task_result_store_provider is None:
            task_result_store_provider = DEFAULT_TASK_RESULT_STORE_PROVIDER
        self.successful_task_result_storer = SuccessfulTaskResultStorer[
            ExecutorPayload,
            ExecutorOutput,
            ExecutorPublicOutput,
        ](
            _id=NodeId(f"{name}-successful-task-result-storer"),
            name=name,
            task_result_store_provider=task_result_store_provider,
        )
        self.executor_failure_task_result_storer = ExecutorFailureTaskResultStorer[
            ExecutorPayload,
            ExecutorOutput,
            ExecutorPublicOutput,
        ](
            _id=NodeId(f"{name}-executor-failure-task-result-storer"),
            name=name,
            task_result_store_provider=task_result_store_provider,
        )
        self.task_result_dispatcher = TaskResultDispatcher[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput](
            _id=NodeId(f"{name}-task-result-dispatcher")
        )
        self.error_mux = Mux2[NexusException, RetriesExhaustedException, NexusException](
            _id=NodeId(f"{name}-error-mux")
        )

        self.block_beat = self.timestamper.block_beat
        self.input = self.retry.input
        self.successful_task_result = self.task_result_dispatcher.successful_task_result
        self.executor_failure = self.task_result_dispatcher.executor_failure
        self.executor_output = self.task_result_dispatcher.executor_output
        self.error = self.error_mux.out
        for endpoint in (
            self.block_beat,
            self.input,
            self.successful_task_result,
            self.executor_failure,
            self.executor_output,
            self.error,
        ):
            endpoint.owner_task = self

        self.internal_flow = Flow(
            entry_sinks=NodeSinks(
                sinks={SinkName("input"): self.input},
            ),
            exit_sources=NodeSources(
                sources={
                    SourceName("successful_task_result"): self.successful_task_result,
                    SourceName("executor_failure"): self.executor_failure,
                    SourceName("executor_output"): self.executor_output,
                },
            ),
        )
        self.internal_flow.sinks.add(self.input)
        self.internal_flow.sources.add(self.successful_task_result)
        self.internal_flow.sources.add(self.executor_failure)
        self.internal_flow.sources.add(self.executor_output)

        def connect[T](source: Source[T], sink: Sink[T]) -> None:
            self.internal_flow.pipes.connect(source, sink)
            self.internal_flow.sources.add(source)
            self.internal_flow.sinks.add(sink)

        connect(self.retry.next_attempt, self.payload_creator.input)
        connect(self.payload_creator.created_payload, self.router.input)
        connect(self.router.routed, self.timestamper.input)
        connect(self.timestamper.forwarded_input, self.executor_communicator.input)
        connect(self.executor_communicator.processed, self.timestamper.executor_output)
        connect(self.timestamper.timestamped_output, self.task_result_preparer.timestamped_result)
        connect(self.task_result_preparer.executor_output_for_conversion, self.executor_result_converter.input)
        connect(self.executor_result_converter.created_payload, self.task_result_preparer.converted_public_output)
        connect(
            self.task_result_preparer.prepared_successful_task_result,
            self.successful_task_result_storer.sink,
        )
        connect(
            self.task_result_preparer.prepared_executor_failure,
            self.executor_failure_task_result_storer.sink,
        )
        connect(
            self.successful_task_result_storer.successful_task_result,
            self.task_result_dispatcher.successful_task_result_input,
        )
        connect(
            self.executor_failure_task_result_storer.executor_failure,
            self.task_result_dispatcher.executor_failure_input,
        )
        connect(self.executor_communicator.error, self.retry.failed_attempt)
        connect(self.successful_task_result_storer.error, self.retry.failed_attempt)
        connect(self.executor_failure_task_result_storer.error, self.retry.failed_attempt)
        connect(self.payload_creator.error, self.retry.failed_attempt)
        connect(self.router.error, self.retry.failed_attempt)
        connect(self.retry.error, self.error_mux.left)
        connect(self.task_result_preparer.error, self.error_mux.right)
        connect(self.executor_result_converter.error, self.task_result_preparer.conversion_failed)

    def internal_nodes(self) -> tuple[Node, ...]:
        """Return all internal nodes in build order for `SubnetBuilder(nodes=...)`."""
        return (
            self.retry,
            self.payload_creator,
            self.router,
            self.timestamper,
            self.executor_communicator,
            self.task_result_preparer,
            self.executor_result_converter,
            self.successful_task_result_storer,
            self.executor_failure_task_result_storer,
            self.task_result_dispatcher,
            self.error_mux,
        )
