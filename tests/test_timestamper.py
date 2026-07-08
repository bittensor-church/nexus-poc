# pyright: basic

from datetime import UTC, datetime, timedelta

import pytest
from utils import CollectorActor, dummy_block_beat, wait_until

from nexus.v1 import Flow, SendEvent, Source, SubnetBuilder, Timestamped, TimestamperNode


def test_timestamper_input_sets_start_timestamp_and_forwards_input() -> None:
    timestamper = TimestamperNode[str, str]("timestamper")
    builder = SubnetBuilder(nodes=[timestamper])

    upstream_input = Source[str]("upstream-input")
    input_collector = CollectorActor[str](
        pipe_to_bus=builder.pipe_to_bus,
        context_store=builder.context_store,
        name="input-collector",
    )

    runtime = (
        builder.add_flows(
            Flow.from_connectable(upstream_input).then(timestamper.input),
            Flow.from_connectable(timestamper.forwarded_input).then(input_collector.sink),
        )
        .add_actors(input_collector)
        .build()
    )

    with runtime.context_store.create_context() as context:
        ctx_id = context.id

    payload = "hello"
    with runtime.running(shutdown_timeout_seconds=1.0):
        runtime.pipe_to_bus.put(SendEvent(ctx_id=ctx_id, source=upstream_input, payload=payload))
        wait_until(lambda: len(input_collector.received_events) == 1)

    assert input_collector.received_events[0].payload == payload
    with runtime.context_store.get_context(ctx_id) as context:
        saved_start_time = context.copy_user_data()[timestamper.processing_started_at_user_data_key]
    assert isinstance(saved_start_time, datetime)
    assert saved_start_time.tzinfo == UTC
    assert datetime.now(tz=UTC) - saved_start_time <= timedelta(minutes=1)


def test_timestamper_queues_output_until_first_block_beat() -> None:
    timestamper = TimestamperNode[str, str]("timestamper")
    builder = SubnetBuilder(nodes=[timestamper])

    upstream_input = Source[str]("upstream-input")
    upstream_output = Source[str]("upstream-output")
    upstream_block_beat = Source("upstream-block-beat")

    input_collector = CollectorActor[str](
        pipe_to_bus=builder.pipe_to_bus,
        context_store=builder.context_store,
        name="input-collector",
    )
    output_collector = CollectorActor[Timestamped[str]](
        pipe_to_bus=builder.pipe_to_bus,
        context_store=builder.context_store,
        name="output-collector",
    )

    runtime = (
        builder.add_flows(
            Flow.from_connectable(upstream_input).then(timestamper.input),
            Flow.from_connectable(upstream_output).then(timestamper.executor_output),
            Flow.from_connectable(upstream_block_beat).then(timestamper.block_beat),
            Flow.from_connectable(timestamper.forwarded_input).then(input_collector.sink),
            Flow.from_connectable(timestamper.timestamped_output).then(output_collector.sink),
        )
        .add_actors(input_collector, output_collector)
        .build()
    )

    with runtime.context_store.create_context() as context:
        ctx_id = context.id

    output_payload = "result"
    block_beat = dummy_block_beat(123)

    with runtime.running(shutdown_timeout_seconds=1.0):
        runtime.pipe_to_bus.put(SendEvent(ctx_id=ctx_id, source=upstream_input, payload="request"))
        wait_until(lambda: len(input_collector.received_events) == 1)

        runtime.pipe_to_bus.put(SendEvent(ctx_id=ctx_id, source=upstream_output, payload=output_payload))
        with pytest.raises(AssertionError):
            wait_until(lambda: len(output_collector.received_events) > 0, timeout=0.2, interval=0.05)
        assert len(output_collector.received_events) == 0

        runtime.pipe_to_bus.put(SendEvent(ctx_id=ctx_id, source=upstream_block_beat, payload=block_beat))
        wait_until(lambda: len(output_collector.received_events) == 1)

    timestamped = output_collector.received_events[0].payload
    with runtime.context_store.get_context(ctx_id) as context:
        saved_start_time = context.copy_user_data()[timestamper.processing_started_at_user_data_key]

    assert timestamped.executor_output == output_payload
    assert timestamped.block_at_finish == block_beat
    assert timestamped.processing_started == saved_start_time
    assert timestamped.processing_finished >= timestamped.processing_started
    assert timestamped.processing_finished.tzinfo == UTC
    assert datetime.now(tz=UTC) - timestamped.processing_started <= timedelta(minutes=1)
    assert datetime.now(tz=UTC) - timestamped.processing_finished <= timedelta(minutes=1)


def test_timestamper_uses_most_recent_block_beat_for_output() -> None:
    timestamper = TimestamperNode[str, str]("timestamper")
    builder = SubnetBuilder(nodes=[timestamper])

    upstream_input = Source[str]("upstream-input")
    upstream_output = Source[str]("upstream-output")
    upstream_block_beat = Source("upstream-block-beat")

    output_collector = CollectorActor[Timestamped[str]](
        pipe_to_bus=builder.pipe_to_bus,
        context_store=builder.context_store,
        name="output-collector",
    )

    runtime = (
        builder.add_flows(
            Flow.from_connectable(upstream_input).then(timestamper.input),
            Flow.from_connectable(upstream_output).then(timestamper.executor_output),
            Flow.from_connectable(upstream_block_beat).then(timestamper.block_beat),
            Flow.from_connectable(timestamper.timestamped_output).then(output_collector.sink),
        )
        .add_actors(output_collector)
        .build()
    )

    with runtime.context_store.create_context() as context:
        ctx_id = context.id
    with runtime.context_store.create_context() as context:
        first_beat_ctx = context.id
    with runtime.context_store.create_context() as context:
        second_beat_ctx = context.id

    older_beat = dummy_block_beat(9)
    newer_beat = dummy_block_beat(10)

    with runtime.running(shutdown_timeout_seconds=1.0):
        runtime.pipe_to_bus.put(SendEvent(ctx_id=ctx_id, source=upstream_input, payload="request"))
        runtime.pipe_to_bus.put(SendEvent(ctx_id=first_beat_ctx, source=upstream_block_beat, payload=older_beat))
        runtime.pipe_to_bus.put(SendEvent(ctx_id=second_beat_ctx, source=upstream_block_beat, payload=newer_beat))
        runtime.pipe_to_bus.put(SendEvent(ctx_id=ctx_id, source=upstream_output, payload="result"))
        wait_until(lambda: len(output_collector.received_events) == 1)

    assert output_collector.received_events[0].payload.block_at_finish == newer_beat


def test_timestamper_drops_entries_older_than_five_minutes_when_logging_error(caplog: pytest.LogCaptureFixture) -> None:
    timestamper = TimestamperNode[str, str]("timestamper")
    builder = SubnetBuilder(nodes=[timestamper])

    upstream_output = Source[str]("upstream-output")
    upstream_block_beat = Source("upstream-block-beat")

    output_collector = CollectorActor[Timestamped[str]](
        pipe_to_bus=builder.pipe_to_bus,
        context_store=builder.context_store,
        name="output-collector",
    )

    runtime = (
        builder.add_flows(
            Flow.from_connectable(upstream_output).then(timestamper.executor_output),
            Flow.from_connectable(upstream_block_beat).then(timestamper.block_beat),
            Flow.from_connectable(timestamper.timestamped_output).then(output_collector.sink),
        )
        .add_actors(output_collector)
        .build()
    )

    with runtime.context_store.create_context() as context:
        old_ctx_id = context.id
    with runtime.context_store.create_context() as context:
        beat_ctx_id = context.id

    with runtime.context_store.get_context(old_ctx_id) as old_context:
        old_context.set_user_data(
            timestamper.processing_started_at_user_data_key,
            datetime.now(tz=UTC) - timedelta(minutes=6),
        )

    beat = dummy_block_beat(5)
    with runtime.running(shutdown_timeout_seconds=1.0):
        with caplog.at_level("ERROR", logger="nexus._internal.actors.timestamper"):
            runtime.pipe_to_bus.put(SendEvent(ctx_id=old_ctx_id, source=upstream_output, payload="too-old"))
            wait_until(
                lambda: any("dropped_old_entries=1" in record.message for record in caplog.records),
            )

        runtime.pipe_to_bus.put(SendEvent(ctx_id=beat_ctx_id, source=upstream_block_beat, payload=beat))
        with pytest.raises(AssertionError):
            wait_until(lambda: len(output_collector.received_events) > 0, timeout=0.2, interval=0.05)

    assert len(output_collector.received_events) == 0


def test_timestamper_logs_warning_only_once_after_threshold_crossing(caplog: pytest.LogCaptureFixture) -> None:
    timestamper = TimestamperNode[str, str]("timestamper")
    builder = SubnetBuilder(nodes=[timestamper])

    upstream_output = Source[str]("upstream-output")

    runtime = builder.add_flows(
        Flow.from_connectable(upstream_output).then(timestamper.executor_output),
    ).build()

    with runtime.context_store.create_context() as context:
        old_ctx_id = context.id

    with runtime.context_store.get_context(old_ctx_id) as old_context:
        old_context.set_user_data(
            timestamper.processing_started_at_user_data_key,
            datetime.now(tz=UTC) - timedelta(minutes=2),
        )

    with runtime.running(shutdown_timeout_seconds=1.0):
        with caplog.at_level("WARNING", logger="nexus._internal.actors.timestamper"):
            runtime.pipe_to_bus.put(SendEvent(ctx_id=old_ctx_id, source=upstream_output, payload="first"))
            wait_until(lambda: sum("Timestamper queue head is old" in record.message for record in caplog.records) == 1)

            runtime.pipe_to_bus.put(SendEvent(ctx_id=old_ctx_id, source=upstream_output, payload="second"))
            with pytest.raises(AssertionError):
                wait_until(
                    lambda: sum("Timestamper queue head is old" in record.message for record in caplog.records) > 1,
                    timeout=0.2,
                    interval=0.05,
                )
