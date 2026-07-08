"""
Timestamper node and actor.

This component tracks processing time boundaries around two logical phases:
- input phase: records the latest processing start timestamp in context user data
- output phase: emits a `Timestamped` payload enriched with start/end times and block beat

It also supports backpressure while waiting for chain time:
- outputs are queued until at least one `BlockBeat` is observed
- queue head age is monitored and logged
- when queue head age exceeds the error threshold, all queued entries older than that threshold are dropped
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, override

from nexus._internal.actors.chain_beat.block_beat import BlockBeat
from nexus._internal.core.dsl.nodes import Node, NodeSinks, NodeSources, Sink, SinkName, Source, SourceName
from nexus._internal.core.runtime.actor import Actor, ActorBuilder, EventHandler
from nexus._internal.core.runtime.context_store import Context, ContextStore
from nexus._internal.core.runtime.context_store_types import ContextId
from nexus._internal.core.runtime.events import MessagesToSend, PipeToBus, ReceiveEvent, SendEvent
from nexus._internal.logging_utils import get_logger
from nexus._internal.utils.exceptions import InternalStateCorruptionException

logger: logging.Logger = get_logger(__name__)

# Queue head age threshold for warning-level staleness logs.
QUEUE_AGE_WARNING_THRESHOLD = timedelta(minutes=1)
# Queue head age threshold for error-level staleness logs and stale entry dropping.
QUEUE_AGE_ERROR_THRESHOLD = timedelta(minutes=5)


@dataclass(frozen=True)
class Timestamped[Output]:
    """Output enriched with processing timing and the most recently seen block beat."""

    executor_output: Output
    processing_started: datetime
    processing_finished: datetime
    block_at_finish: BlockBeat


@dataclass(frozen=True)
class QueuedOutput[Output]:
    """Buffered output event waiting for a `BlockBeat` before timestamp finalization."""

    ctx_id: ContextId
    output: Output


class TimestamperNode[Input, Output](Node, ActorBuilder):
    """
    Wraps a processing step with timing metadata. Records a start timestamp on input,
    then enriches the output with start/end times and the latest observed block beat.
    Connect the processing step from `forwarded_input` back to `executor_output`.
    Outputs are queued until at least one BlockBeat has been received.

    sink input: payload to forward and start timing
    sink executor_output: result from the wrapped processing step
    sink block_beat: chain block beats for timestamp finalization
    source forwarded_input: input forwarded unchanged to the wrapped step
    source timestamped_output: Timestamped output with processing times and block beat
    """

    input: Sink[Input]
    executor_output: Sink[Output]
    block_beat: Sink[BlockBeat]

    forwarded_input: Source[Input]
    timestamped_output: Source[Timestamped[Output]]

    processing_started_at_user_data_key: str

    def __init__(self, _id: str) -> None:
        super().__init__(_id)

        self.input = Sink[Input](f"{self.id}-input-sink", owner_node=self)
        self.executor_output = Sink[Output](f"{self.id}-executor-output-sink", owner_node=self)
        self.block_beat = Sink[BlockBeat](f"{self.id}-block-beat-sink", owner_node=self)

        self.forwarded_input = Source[Input](f"{self.id}-input-source", owner_node=self)
        self.timestamped_output = Source[Timestamped[Output]](f"{self.id}-output-source", owner_node=self)

        self.processing_started_at_user_data_key = f"{self.id}-processing-started-at-utc"

    @override
    def sinks(self) -> NodeSinks:
        return NodeSinks(
            sinks={
                SinkName("input"): self.input,
                SinkName("output"): self.executor_output,
                SinkName("block_beat"): self.block_beat,
            }
        )

    @override
    def sources(self) -> NodeSources:
        return NodeSources(
            sources={
                SourceName("forwarded_input"): self.forwarded_input,
                SourceName("timestamped_output"): self.timestamped_output,
            }
        )

    @override
    def build_actor(self, *, pipe_to_bus: PipeToBus, context_store: ContextStore) -> Actor:
        return TimestamperActor[Input, Output](spec=self, pipe_to_bus=pipe_to_bus, context_store=context_store)


class TimestamperActor[Input, Output](Actor):
    """
    Runtime behavior for `TimestamperNode`.

    Notes:
    - Input events overwrite the stored start timestamp (latest input wins).
    - Output events require a `BlockBeat`; otherwise they are queued.
    - Warning logs are emitted only when crossing the warning threshold for the current queue head.

    """

    spec: TimestamperNode[Input, Output]
    latest_block_beat: BlockBeat | None
    queued_outputs: list[QueuedOutput[Output]]
    _warning_logged_head: tuple[ContextId, datetime] | None

    def __init__(
        self,
        *,
        spec: TimestamperNode[Input, Output],
        pipe_to_bus: PipeToBus,
        context_store: ContextStore,
    ) -> None:
        super().__init__(name=spec.id, pipe_to_bus=pipe_to_bus, context_store=context_store)
        self.spec = spec
        self.latest_block_beat = None
        self.queued_outputs = []
        self._warning_logged_head = None

    @override
    def handlers(self) -> dict[Sink[Any], EventHandler]:
        return {
            self.spec.input: self.handle_input,
            self.spec.executor_output: self.handle_output,
            self.spec.block_beat: self.handle_block_beat,
        }

    def handle_input(self, ctx: Context, event: ReceiveEvent[Input]) -> MessagesToSend:
        """Persist the latest processing start timestamp and forward input unchanged."""
        started_at = datetime.now(tz=UTC)
        ctx.set_user_data(self.spec.processing_started_at_user_data_key, started_at)
        return SendEvent(ctx_id=event.ctx_id, source=self.spec.forwarded_input, payload=event.payload)

    def handle_output(self, ctx: Context, event: ReceiveEvent[Output]) -> MessagesToSend:
        """Emit timestamped output if ready; otherwise enqueue while waiting for `BlockBeat`."""
        if self.latest_block_beat is None:
            self.queued_outputs.append(QueuedOutput(ctx_id=event.ctx_id, output=event.payload))
            self._log_if_first_queued_output_too_old(current_ctx=ctx)
            return ()

        timestamped_output = self._timestamp_output(ctx=ctx, output=event.payload)
        return SendEvent(ctx_id=event.ctx_id, source=self.spec.timestamped_output, payload=timestamped_output)

    def handle_block_beat(self, _: Context, event: ReceiveEvent[BlockBeat]) -> MessagesToSend:
        """Update latest beat and flush all currently queued outputs."""
        self.latest_block_beat = event.payload

        if len(self.queued_outputs) == 0:
            self._warning_logged_head = None
            return ()

        events_to_send: list[SendEvent[Timestamped[Output]]] = []
        for queued_output in self.queued_outputs:
            # this is safe, because if the context is queued,
            # there should be no one else using it
            with self.context_store.get_context(queued_output.ctx_id) as output_context:
                timestamped_output = self._timestamp_output(ctx=output_context, output=queued_output.output)
            events_to_send.append(
                SendEvent(
                    ctx_id=queued_output.ctx_id,
                    source=self.spec.timestamped_output,
                    payload=timestamped_output,
                )
            )

        self.queued_outputs.clear()
        self._warning_logged_head = None
        return tuple(events_to_send)

    def _log_if_first_queued_output_too_old(self, *, current_ctx: Context) -> None:
        """Log queue staleness based on head age and drop very old queued entries on error threshold."""
        if len(self.queued_outputs) == 0:
            self._warning_logged_head = None
            return

        first_queued = self.queued_outputs[0]
        first_started_at = self._started_at(first_queued.ctx_id)
        head_key = (first_queued.ctx_id, first_started_at)

        now = datetime.now(tz=UTC)
        first_age = now - first_started_at
        if first_age > QUEUE_AGE_ERROR_THRESHOLD:
            dropped_count = self._drop_entries_older_than_error_threshold(current_ctx=current_ctx, now=now)
            self._warning_logged_head = None
            logger.error(
                "Timestamper queue head is too old: age=%s threshold=%s head_ctx_id=%s queue_len=%d "
                "dropped_old_entries=%d",
                first_age,
                QUEUE_AGE_ERROR_THRESHOLD,
                first_queued.ctx_id,
                len(self.queued_outputs),
                dropped_count,
            )
        elif first_age > QUEUE_AGE_WARNING_THRESHOLD and self._warning_logged_head != head_key:
            logger.warning(
                "Timestamper queue head is old: age=%s threshold=%s head_ctx_id=%s queue_len=%d",
                first_age,
                QUEUE_AGE_WARNING_THRESHOLD,
                first_queued.ctx_id,
                len(self.queued_outputs),
            )
            self._warning_logged_head = head_key

    def _drop_entries_older_than_error_threshold(self, *, current_ctx: Context, now: datetime) -> int:
        """Drop all queued entries older than the error threshold and return dropped count."""
        retained_entries: list[QueuedOutput[Output]] = []
        dropped_count = 0
        for queued_output in self.queued_outputs:
            queued_started_at = self._started_at(queued_output.ctx_id)

            queued_age = now - queued_started_at
            if queued_age > QUEUE_AGE_ERROR_THRESHOLD:
                dropped_count += 1
            else:
                retained_entries.append(queued_output)

        self.queued_outputs = retained_entries
        return dropped_count

    def _timestamp_output(self, *, ctx: Context, output: Output) -> Timestamped[Output]:
        """
        Build `Timestamped` using context start time, current UTC end time, and latest block beat.

        Raises:
            InternalStateCorruptionException: if no block beat was observed yet.

        """
        started_at = self._started_at(ctx.id)
        block_beat = self.latest_block_beat
        if block_beat is None:
            raise InternalStateCorruptionException("Block beat is required to timestamp output but is missing.")

        return Timestamped(
            executor_output=output,
            processing_started=started_at.astimezone(UTC),
            processing_finished=datetime.now(tz=UTC),
            block_at_finish=block_beat,
        )

    def _started_at(self, ctx_id: ContextId) -> datetime:
        """
        Load and validate processing start timestamp from context user data.

        Raises:
            InternalStateCorruptionException: if the stored timestamp is missing, invalid, or timezone-naive.

        """
        with self.context_store.get_context(ctx_id) as ctx:
            started_at = ctx.copy_user_data().get(self.spec.processing_started_at_user_data_key)
            if not isinstance(started_at, datetime):
                raise InternalStateCorruptionException(
                    "Processing start timestamp missing or invalid in context user_data for "
                    f"ctx={ctx.id}, key={self.spec.processing_started_at_user_data_key!r}, "
                    f"value={started_at!r}."
                )
            if started_at.tzinfo is None:
                raise InternalStateCorruptionException(
                    f"Processing start timestamp is timezone-naive for ctx={ctx.id}: {started_at!r}"
                )
            return started_at.astimezone(UTC)
