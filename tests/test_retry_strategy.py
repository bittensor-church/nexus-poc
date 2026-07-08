# pyright: basic

from datetime import timedelta
from time import monotonic
from typing import Any, override

from pydantic import BaseModel
from utils import CollectorActor, wait_until

from nexus.v1 import (
    Actor,
    Context,
    ContextId,
    ContextStore,
    EventHandler,
    Flow,
    MessagesToSend,
    NexusException,
    PipeToBus,
    ReceiveEvent,
    RetriesExhaustedException,
    RetryStrategy,
    SendEvent,
    Sink,
    Source,
    SubnetBuilder,
)


class RetryInput(BaseModel):
    value: str


def attempts_in_context(context_store: ContextStore, ctx_id: ContextId, retry_strategy_id: str) -> int:
    with context_store.get_context(ctx_id) as context:
        retry_state = context.copy_user_data()[retry_strategy_id]
        return int(retry_state.attempts)


class FailFirstKAttemptsForInputValueActor(Actor):
    def __init__(
        self,
        *,
        pipe_to_bus: PipeToBus,
        context_store: ContextStore,
        failing_input_value: str,
        fail_first_k: int,
        name: str = "fail-first-k-for-input-value",
    ) -> None:
        super().__init__(name=name, pipe_to_bus=pipe_to_bus, context_store=context_store)
        self.failing_input_value = failing_input_value
        self.fail_first_k = fail_first_k
        self.attempt_sink = Sink[RetryInput](f"{name}-attempt-sink")
        self.failed_source = Source[NexusException](f"{name}-failed-source")
        self.success_source = Source[RetryInput](f"{name}-success-source")
        self.received_attempt_numbers_by_ctx: dict[ContextId, list[int]] = {}
        self.attempt_received_at: dict[tuple[ContextId, int], float] = {}
        self.success_emitted_at_by_ctx: dict[ContextId, float] = {}

    @override
    def handlers(self) -> dict[Sink[Any], EventHandler]:
        return {self.attempt_sink: self._handle_attempt}

    def _handle_attempt(self, _: Context, event: ReceiveEvent[RetryInput]) -> MessagesToSend:
        self.received_attempt_numbers_by_ctx.setdefault(event.ctx_id, [])
        attempt_number = len(self.received_attempt_numbers_by_ctx[event.ctx_id]) + 1
        self.received_attempt_numbers_by_ctx[event.ctx_id].append(attempt_number)
        self.attempt_received_at[(event.ctx_id, attempt_number)] = monotonic()

        should_fail = event.payload.value == self.failing_input_value and attempt_number <= self.fail_first_k
        if should_fail:
            return SendEvent(
                ctx_id=event.ctx_id,
                source=self.failed_source,
                payload=NexusException(f"failed attempt #{attempt_number}"),
            )
        else:
            self.success_emitted_at_by_ctx[event.ctx_id] = monotonic()
            return SendEvent(
                ctx_id=event.ctx_id,
                source=self.success_source,
                payload=event.payload,
            )


def test_retry_strategy_actor_sends_first_attempt_downstream_on_input() -> None:
    retry_strategy = RetryStrategy[RetryInput]("retry-strategy", max_attempts=3, delay=timedelta(milliseconds=10))

    builder = SubnetBuilder(nodes=[retry_strategy])
    collector = CollectorActor[RetryInput](
        pipe_to_bus=builder.pipe_to_bus,
        context_store=builder.context_store,
        name="input-collector",
    )

    upstream_source = Source[RetryInput]("retry-input-source")

    runtime = (
        builder.add_flows(
            Flow.from_connectable(upstream_source).then(retry_strategy.input),
            Flow.from_connectable(retry_strategy.next_attempt).then(collector.sink),
        )
        .add_actors(collector)
        .build()
    )

    with runtime.context_store.create_context() as context:
        ctx_id = context.id

    payload = RetryInput(value="ping")
    with runtime.running(shutdown_timeout_seconds=1.0):
        runtime.pipe_to_bus.put(SendEvent(ctx_id=ctx_id, source=upstream_source, payload=payload))

        wait_until(lambda: len(collector.received_events) == 1)

        received = collector.received_events[0]
        assert received.ctx_id == ctx_id
        assert received.payload == payload
        wait_until(lambda: attempts_in_context(runtime.context_store, ctx_id, retry_strategy.id) == 1)
        assert len(collector.received_events) == 1


def test_retry_strategy_actor_retries_first_k_failures() -> None:
    retry_strategy = RetryStrategy[RetryInput]("retry-strategy", max_attempts=5, delay=timedelta(milliseconds=10))
    builder = SubnetBuilder(nodes=[retry_strategy])
    flaky = FailFirstKAttemptsForInputValueActor(
        pipe_to_bus=builder.pipe_to_bus,
        context_store=builder.context_store,
        failing_input_value="needs-retries",
        fail_first_k=2,
    )

    upstream_source = Source[RetryInput]("retry-input-source")
    runtime = (
        builder.add_flows(
            Flow.from_connectable(upstream_source).then(retry_strategy.input),
            Flow.from_connectable(retry_strategy.next_attempt).then(flaky.attempt_sink),
            Flow.from_connectable(flaky.failed_source).then(retry_strategy.failed_attempt),
        )
        .add_actors(flaky)
        .build()
    )

    with runtime.context_store.create_context() as context:
        ctx_id = context.id

    payload = RetryInput(value="needs-retries")
    with runtime.running(shutdown_timeout_seconds=1.0):
        runtime.pipe_to_bus.put(SendEvent(ctx_id=ctx_id, source=upstream_source, payload=payload))

        wait_until(lambda: len(flaky.received_attempt_numbers_by_ctx.get(ctx_id, [])) >= 3)

        assert flaky.received_attempt_numbers_by_ctx[ctx_id] == [1, 2, 3]

    assert attempts_in_context(runtime.context_store, ctx_id, retry_strategy.id) == 3


def test_retry_strategy_actor_emits_retries_exhausted_after_too_many_failures() -> None:
    retry_strategy = RetryStrategy[RetryInput]("retry-strategy", max_attempts=3, delay=timedelta(milliseconds=10))
    builder = SubnetBuilder(nodes=[retry_strategy])
    flaky = FailFirstKAttemptsForInputValueActor(
        pipe_to_bus=builder.pipe_to_bus,
        context_store=builder.context_store,
        failing_input_value="always-fails",
        fail_first_k=10,
    )

    exhausted_collector = CollectorActor[RetriesExhaustedException](
        pipe_to_bus=builder.pipe_to_bus,
        context_store=builder.context_store,
        name="retries-exhausted-collector",
    )

    upstream_source = Source[RetryInput]("retry-input-source")
    runtime = (
        builder.add_flows(
            Flow.from_connectable(upstream_source).then(retry_strategy.input),
            Flow.from_connectable(retry_strategy.next_attempt).then(flaky.attempt_sink),
            Flow.from_connectable(flaky.failed_source).then(retry_strategy.failed_attempt),
            Flow.from_connectable(retry_strategy.error).then(exhausted_collector.sink),
        )
        .add_actors(flaky, exhausted_collector)
        .build()
    )

    with runtime.context_store.create_context() as context:
        ctx_id = context.id

    payload = RetryInput(value="always-fails")
    with runtime.running(shutdown_timeout_seconds=1.0):
        runtime.pipe_to_bus.put(SendEvent(ctx_id=ctx_id, source=upstream_source, payload=payload))

        wait_until(lambda: len(exhausted_collector.received_events) == 1)

        assert flaky.received_attempt_numbers_by_ctx[ctx_id] == [1, 2, 3]
        assert attempts_in_context(runtime.context_store, ctx_id, retry_strategy.id) == 3
        exhausted_event = exhausted_collector.received_events[0]
        assert exhausted_event.ctx_id == ctx_id
        assert isinstance(exhausted_event.payload, RetriesExhaustedException)


def test_retry_strategy_retry_wait_in_one_context_does_not_block_other_context() -> None:
    retry_strategy = RetryStrategy[RetryInput]("retry-strategy", max_attempts=5, delay=timedelta(milliseconds=200))

    builder = SubnetBuilder(nodes=[retry_strategy])
    flaky = FailFirstKAttemptsForInputValueActor(
        pipe_to_bus=builder.pipe_to_bus,
        context_store=builder.context_store,
        failing_input_value="needs-retries",
        fail_first_k=2,
    )
    success_collector = CollectorActor[RetryInput](
        pipe_to_bus=builder.pipe_to_bus,
        context_store=builder.context_store,
        name="attempt-collector-success",
    )

    upstream_source = Source[RetryInput]("retry-input-source-multi-context")
    runtime = (
        builder.add_flows(
            Flow.from_connectable(upstream_source).then(retry_strategy.input),
            Flow.from_connectable(retry_strategy.next_attempt).then(flaky.attempt_sink),
            Flow.from_connectable(flaky.failed_source).then(retry_strategy.failed_attempt),
            Flow.from_connectable(flaky.success_source).then(success_collector.sink),
        )
        .add_actors(flaky, success_collector)
        .build()
    )

    with runtime.context_store.create_context() as context:
        ctx_needs_retries = context.id
    with runtime.context_store.create_context() as context:
        ctx_always_success = context.id

    payload_needs_retries = RetryInput(value="needs-retries")
    payload_always_success = RetryInput(value="always-success")

    with runtime.running(shutdown_timeout_seconds=1.0):
        runtime.pipe_to_bus.put(
            SendEvent(ctx_id=ctx_needs_retries, source=upstream_source, payload=payload_needs_retries)
        )
        wait_until(lambda: flaky.received_attempt_numbers_by_ctx.get(ctx_needs_retries) == [1])

        runtime.pipe_to_bus.put(
            SendEvent(ctx_id=ctx_always_success, source=upstream_source, payload=payload_always_success)
        )
        wait_until(lambda: ctx_always_success in flaky.success_emitted_at_by_ctx)
        wait_until(lambda: (ctx_needs_retries, 2) in flaky.attempt_received_at)

        assert flaky.success_emitted_at_by_ctx[ctx_always_success] < flaky.attempt_received_at[(ctx_needs_retries, 2)]

        wait_until(
            lambda: (
                {event.ctx_id for event in success_collector.received_events} >= {ctx_needs_retries, ctx_always_success}
            )
        )

        assert flaky.received_attempt_numbers_by_ctx[ctx_needs_retries] == [1, 2, 3]
        assert flaky.received_attempt_numbers_by_ctx[ctx_always_success] == [1]
        assert attempts_in_context(runtime.context_store, ctx_always_success, retry_strategy.id) == 1
        assert attempts_in_context(runtime.context_store, ctx_needs_retries, retry_strategy.id) == 3

        successes_by_ctx = {event.ctx_id: event.payload for event in success_collector.received_events}

        assert successes_by_ctx[ctx_always_success] == payload_always_success
        assert successes_by_ctx[ctx_needs_retries] == payload_needs_retries
