import threading
from datetime import timedelta
from threading import Timer
from typing import Any, NewType, cast, override

from nexus._internal.core.dsl.nodes import Node, NodeSinks, NodeSources, Sink, SinkName, Source, SourceName
from nexus._internal.core.runtime.actor import Actor, ActorBuilder, EventHandler
from nexus._internal.core.runtime.context_store import Context, ContextStore
from nexus._internal.core.runtime.events import MessagesToSend, PipeToBus, ReceiveEvent, SendEvent
from nexus._internal.logging_utils import get_logger
from nexus._internal.utils.exceptions import (
    InternalFrameworkException,
    InternalStateCorruptionException,
    NexusException,
)

# 1-based index of the attempt: 1 for the first attempt, 2 for the second, etc.
AttemptNumber = NewType("AttemptNumber", int)

logger = get_logger(__name__)


class RetryState[T]:
    original_input: T
    attempts: AttemptNumber

    def __init__(self, original_input: T) -> None:
        self.original_input = original_input
        self.attempts = AttemptNumber(0)

    def next_attempt(self) -> None:
        self.attempts = AttemptNumber(int(self.attempts) + 1)


class RetriesExhaustedException(NexusException):
    """Raised when all retry attempts have been exhausted."""

    pass


class RetryStrategy[T](Node, ActorBuilder):
    """
    Re-emits the original input on each signal to `failed_attempt`, up to a configured number of
    attempts with a delay between each. Emits the first attempt immediately on receiving the input.

    sink input: original payload, starts the retry cycle
    sink failed_attempt: accepts any NexusException, issuing the next attempt when received
    source next_attempt: original payload, emitted for every attempt including the first
    source error: RetriesExhaustedException when all attempts are exhausted
    """

    max_attempts: int
    delay: timedelta

    input: Sink[T]  # consumes the original input and triggers the first attempt
    failed_attempt: Sink[NexusException]  # receives failures from the attempt execution

    next_attempt: Source[T]  # emits the next attempt to execute, with the original input
    error: Source[RetriesExhaustedException]  # emits an error when all retry attempts have been exhausted

    def __init__(self, _id: str, max_attempts: int, delay: timedelta) -> None:
        super().__init__(_id)
        self.max_attempts = max_attempts
        self.delay = delay
        self.input = Sink(f"{self.id}-input", owner_node=self)
        self.next_attempt = Source(f"{self.id}-next-attempt", owner_node=self)
        self.error = Source(f"{self.id}-error", owner_node=self)
        self.failed_attempt = Sink(f"{self.id}-failed-attempt", owner_node=self)

    @override
    def sinks(self) -> NodeSinks:
        # no default sink as we expect explicit wiring of both sinks
        return NodeSinks(
            sinks={
                SinkName("input"): self.input,
                SinkName("failed-attempt"): self.failed_attempt,
            }
        )

    @override
    def sources(self) -> NodeSources:
        return NodeSources(
            sources={
                SourceName("retries-exhausted"): self.error,
                SourceName("next-attempt"): self.next_attempt,
            }
        )

    @override
    def build_actor(self, *, pipe_to_bus: PipeToBus, context_store: ContextStore) -> Actor:
        return RetryStrategyActor[T](spec=self, pipe_to_bus=pipe_to_bus, context_store=context_store)


class RetryStrategyActor[T](Actor):
    spec: RetryStrategy[T]
    timers: set[threading.Thread]

    def __init__(self, *, spec: RetryStrategy[T], pipe_to_bus: PipeToBus, context_store: ContextStore) -> None:
        super().__init__(name=spec.id, pipe_to_bus=pipe_to_bus, context_store=context_store)
        self.spec = spec
        self.timers = set()

    def handle_input(self, ctx: Context, event: ReceiveEvent[T]) -> MessagesToSend:
        retry_state = self._retry_state_from_context(ctx)
        if retry_state is None:
            retry_state = RetryState(event.payload)
        return self._next_attempt_message(ctx, retry_state)

    def handle_failed_attempt(self, ctx: Context, event: ReceiveEvent[NexusException]) -> MessagesToSend:
        retry_state = self._retry_state_from_context(ctx)
        if retry_state is None:
            raise InternalStateCorruptionException(
                f"Received failed attempt without existing retry state? ctx_id: {event.ctx_id}"
            )
        if retry_state.attempts >= self.spec.max_attempts:
            exhausted_error = RetriesExhaustedException(
                f"All {self.spec.max_attempts} retry attempts exhausted when trying to process "
                f"{retry_state.original_input}."
            )
            return SendEvent(ctx_id=event.ctx_id, payload=exhausted_error, source=self.spec.error)
        else:
            self._schedule_next_attempt(ctx, retry_state)
            return ()

    def _schedule_next_attempt(self, ctx: Context, _: RetryState[T]) -> None:
        ctx_id = ctx.id  # we can't reference the whole Context in the inner function as we lose its ownership

        # when we leave the scope of the handler
        def trigger_next_attempt():
            logger.info("Time for next attempt for ctx_id: %s", ctx_id)
            try:
                self.timers.remove(threading.current_thread())
            except KeyError:
                # this should never happen, but since it's not in our hands and we
                # seem safe to just move on, let's only log it and not raise an exception
                logger.error(
                    "Timer thread not found in timers set when trying to remove it? current_thread: %s, timers: %s.",
                    threading.current_thread(),
                    self.timers,
                )
            with self.context_store.get_context(ctx_id) as context:
                retry_state = self._retry_state_from_context(context)
                if retry_state is None:
                    # this should also never happen. Retry state should be set in a
                    # previous handler, but we only log and continue.
                    logger.error(
                        "No retry state found in context user data when trying to trigger next attempt? "
                        "ctx_id: %s, context user_data: %s.",
                        ctx_id,
                        context.copy_user_data(),
                    )
                    return
                self._pipe_to_bus.put(self._next_attempt_message(context, retry_state))

        # I don't care about threads really... We can have lots of them and be fine;
        # that said, me might want one day to just have a separate thread and run an AIO loop there...
        timer = Timer(self.spec.delay.total_seconds(), trigger_next_attempt)
        timer.name = f"RetryTimer-{ctx_id}"
        self.timers.add(timer)
        timer.start()

    def _next_attempt_message(self, ctx: Context, retry_state: RetryState[T]) -> SendEvent[T]:
        retry_state.next_attempt()
        ctx.set_user_data(self.spec.id, retry_state)
        logger.info("Issuing attempt %d. ctx_id: %s", retry_state.attempts, ctx.id)
        return SendEvent(ctx_id=ctx.id, payload=retry_state.original_input, source=self.spec.next_attempt)

    def handlers(self) -> dict[Sink[Any], EventHandler]:
        return {
            self.spec.input: self.handle_input,
            self.spec.failed_attempt: self.handle_failed_attempt,
        }

    def _retry_state_from_context(self, ctx: Context) -> RetryState[T] | None:
        retry_state = ctx.copy_user_data().get(self.spec.id)
        if retry_state is None:
            return None
        if not isinstance(retry_state, RetryState):
            raise InternalFrameworkException(
                f"Unexpected retry state type for key {self.spec.id}: {type(retry_state)!r}"
            )
        return cast(RetryState[T], retry_state)
