# pyright: basic

import logging
import queue
from typing import Any, override

from utils import CollectorActor, Jobs, empty_context_store, wait_until

from nexus.v1 import (
    Actor,
    Context,
    ContextId,
    ContextStore,
    DoubleTransform,
    DoubleTransformActor,
    EvenSucks,
    Event,
    EventBus,
    EventHandler,
    Flow,
    Fork,
    ForkActor,
    MessagesToSend,
    PipeToBus,
    Piping,
    ReceiveEvent,
    SafeInvokeWrappedException,
    SendEvent,
    Sink,
    Source,
    StopActorEvent,
    StopBusEvent,
    Stringify,
    StringifyActor,
    Transform,
    TransformActor,
    UppercaseOrError,
    UppercaseOrErrorActor,
)


class DualSinkActor(Actor):
    def __init__(self, context_store: ContextStore, name: str = "dual-sink") -> None:
        super().__init__(name=name, pipe_to_bus=queue.Queue(), context_store=context_store)
        self.sink_left = Sink("left")
        self.sink_right = Sink("right")
        self.handled_left: list[Event] = []
        self.handled_right: list[Event] = []

    def handlers(self) -> dict[Sink, EventHandler]:
        return {
            self.sink_left: self.handle_left,
            self.sink_right: self.handle_right,
        }

    def handle_left(self, context: Context, receive_event: ReceiveEvent) -> MessagesToSend:
        self.handled_left.append(receive_event.payload)
        return ()

    def handle_right(self, context: Context, receive_event: ReceiveEvent) -> MessagesToSend:
        self.handled_right.append(receive_event.payload)
        return ()


class FaultyTransformActor(TransformActor):
    def __init__(self, *, name="faulty", pipe_to_bus: PipeToBus, context_store: ContextStore) -> None:
        ForkActor.__init__(self, spec=Transform(name), pipe_to_bus=pipe_to_bus, context_store=context_store)

    @override
    def _transform(self, ctx: Context, payload: Any):
        raise ValueError("boom")


class FlakyActor(Actor):
    """Actor that raises on specific payloads but forwards others."""

    def __init__(self, *, pipe_to_bus: PipeToBus, context_store: ContextStore) -> None:
        super().__init__(name="flaky", pipe_to_bus=pipe_to_bus, context_store=context_store)
        self.sink = Sink("flaky-sink")
        self.source = Source("flaky-source")

    def handlers(self) -> dict[Sink[Any], EventHandler]:
        return {self.sink: self.handle}

    def handle(self, context: Context, event: ReceiveEvent[Any]) -> MessagesToSend:
        if event.payload == "boom":
            raise ValueError("flaky failure")
        else:
            return (SendEvent(ctx_id=event.ctx_id, source=self.source, payload=f"ok-{event.payload}"),)


class BranchingForkActor(ForkActor[str, str, str]):
    def __init__(self, *, pipe_to_bus: PipeToBus, context_store: ContextStore) -> None:
        super().__init__(
            spec=Fork[str, str, str]("branching-fork"), pipe_to_bus=pipe_to_bus, context_store=context_store
        )

    @override
    def _process(self, ctx: ContextId, payload: str) -> tuple[str, None] | tuple[None, str]:
        if payload.startswith("left"):
            return payload, None
        else:
            return None, payload


class SomeDoubleTransformActor(DoubleTransformActor[str, str, str, str]):
    def __init__(self, *, pipe_to_bus: PipeToBus, context_store: ContextStore) -> None:
        spec = DoubleTransform[str, str, str, str]("instrumented-double")
        super().__init__(
            name=spec.id,
            input_spec=spec.input_transform,
            output_spec=spec.output_transform,
            pipe_to_bus=pipe_to_bus,
            context_store=context_store,
        )

    @override
    def _transform_input(self, ctx: Context, payload: str) -> str:
        if payload.startswith("fail"):
            raise ValueError("input-failed")
        return payload.upper()

    @override
    def _transform_output(self, ctx: Context, payload: str) -> str:
        if payload.startswith("fail"):
            raise ValueError("output-failed")
        return f"{payload}-out"


def _create_context(context_store: ContextStore) -> Context:
    with context_store.create_context() as context:
        return context


def test_actor_dispatches_events_to_handlers():
    context_store = empty_context_store()
    ctx_left_1 = _create_context(context_store)
    ctx_left_2 = _create_context(context_store)
    ctx_right = _create_context(context_store)
    actor = DualSinkActor(context_store)

    events = [
        ReceiveEvent(ctx_id=ctx_left_1.id, target=actor.sink_left, payload="payload-left-1"),
        ReceiveEvent(ctx_id=ctx_right.id, target=actor.sink_right, payload="payload-right"),
        ReceiveEvent(ctx_id=ctx_left_2.id, target=actor.sink_left, payload="payload-left-2"),
        StopActorEvent(),
    ]

    for event in events:
        actor.pipe_from_bus.put(event)
    actor_job = actor.run_loop()

    actor_job.join(1.0)
    assert not actor_job.is_alive()

    assert actor.handled_left == ["payload-left-1", "payload-left-2"]
    assert actor.handled_right == ["payload-right"]


def test_fork_actor_routes_left_and_right_sources():
    context_store = empty_context_store()
    ctx_left = _create_context(context_store)
    ctx_right = _create_context(context_store)

    pipe_to_bus: PipeToBus = queue.Queue()
    actor = BranchingForkActor(pipe_to_bus=pipe_to_bus, context_store=context_store)

    actor.pipe_from_bus.put(ReceiveEvent(ctx_id=ctx_left.id, target=actor.spec.sink, payload="left-payload"))
    actor.pipe_from_bus.put(ReceiveEvent(ctx_id=ctx_right.id, target=actor.spec.sink, payload="right-payload"))
    actor.pipe_from_bus.put(StopActorEvent())

    actor_job = actor.run_loop()
    actor_job.join(1.0)
    assert not actor_job.is_alive()

    left_event = pipe_to_bus.get_nowait()
    right_event = pipe_to_bus.get_nowait()

    assert left_event.source == actor.spec.left
    assert left_event.payload == "left-payload"
    assert left_event.ctx_id == ctx_left.id

    assert right_event.source == actor.spec.right
    assert right_event.payload == "right-payload"
    assert right_event.ctx_id == ctx_right.id

    assert pipe_to_bus.empty()


def test_fork_actor_preserves_context_id():
    context_store = empty_context_store()
    pipe_to_bus: PipeToBus = queue.Queue()
    actor = BranchingForkActor(pipe_to_bus=pipe_to_bus, context_store=context_store)
    ctx_to_preserve = _create_context(context_store)

    actor.pipe_from_bus.put(ReceiveEvent(ctx_id=ctx_to_preserve.id, target=actor.spec.sink, payload="left-ctx"))
    actor.pipe_from_bus.put(StopActorEvent())

    job = actor.run_loop()
    job.join(1.0)
    assert not job.is_alive()

    emitted = pipe_to_bus.get_nowait()
    assert emitted.ctx_id == ctx_to_preserve.id
    assert emitted.source == actor.spec.left
    assert emitted.payload == "left-ctx"
    assert pipe_to_bus.empty()


def test_transform_actor_emits_transformed_event():
    context_store = empty_context_store()
    stringify = Stringify("stringify")  # stringify is a test transform anyway, so let's use it here
    pipe_to_bus: PipeToBus = queue.Queue()
    actor = StringifyActor(spec=stringify, pipe_to_bus=pipe_to_bus, context_store=context_store)

    context = _create_context(context_store)

    actor.pipe_from_bus.put(ReceiveEvent(ctx_id=context.id, target=stringify.sink, payload=123))
    actor.pipe_from_bus.put(StopActorEvent())

    actor_job = actor.run_loop()
    actor_job.join(1.0)
    assert not actor_job.is_alive()

    send_event = pipe_to_bus.get_nowait()
    assert send_event.payload == "123"
    assert send_event.source == stringify.ok
    assert send_event.ctx_id == context.id
    assert pipe_to_bus.empty()


def test_transform_actor_routes_ok_and_error_sources():
    context_store = empty_context_store()
    transform = UppercaseOrError("uppercase-or-error")
    pipe_to_bus: PipeToBus = queue.Queue()
    actor = UppercaseOrErrorActor(spec=transform, pipe_to_bus=pipe_to_bus, context_store=context_store)

    ctx_ok = _create_context(context_store)
    ctx_error = _create_context(context_store)

    actor.pipe_from_bus.put(ReceiveEvent(ctx_id=ctx_ok.id, target=transform.sink, payload="odd"))
    actor.pipe_from_bus.put(ReceiveEvent(ctx_id=ctx_error.id, target=transform.sink, payload="boom"))
    actor.pipe_from_bus.put(StopActorEvent())

    job = actor.run_loop()
    job.join(1.0)
    assert not job.is_alive()

    events = [pipe_to_bus.get_nowait(), pipe_to_bus.get_nowait()]
    ok_event = next(event for event in events if event.ctx_id == ctx_ok.id)
    error_event = next(event for event in events if event.ctx_id == ctx_error.id)

    assert ok_event.source == transform.ok
    assert ok_event.payload == "ODD"
    assert ok_event.ctx_id == ctx_ok.id

    assert error_event.source == transform.error
    assert isinstance(error_event.payload, EvenSucks)
    assert error_event.ctx_id == ctx_error.id
    assert pipe_to_bus.empty()


def test_transform_actor_preserves_context_id():
    context_store = empty_context_store()
    stringify = Stringify("stringify")
    pipe_to_bus: PipeToBus = queue.Queue()
    actor = StringifyActor(spec=stringify, pipe_to_bus=pipe_to_bus, context_store=context_store)

    ctx = _create_context(context_store)
    actor.pipe_from_bus.put(ReceiveEvent(ctx_id=ctx.id, target=stringify.sink, payload=7))
    actor.pipe_from_bus.put(StopActorEvent())

    job = actor.run_loop()
    job.join(1.0)
    assert not job.is_alive()

    sent = pipe_to_bus.get_nowait()
    assert sent.ctx_id == ctx.id
    assert sent.payload == "7"
    assert sent.source == stringify.ok
    assert pipe_to_bus.empty()


def test_event_bus_preserves_context_id():
    context_store = empty_context_store()
    pipe_to_bus: PipeToBus = queue.Queue()

    source = Source("context-source")
    collector = CollectorActor(pipe_to_bus=pipe_to_bus, context_store=context_store)

    piping = Piping()
    piping.add_flow(Flow.from_connectable(source).then(collector.sink))

    event_bus = EventBus(
        connections=piping.pipes,
        input_pipe=pipe_to_bus,
        actors=[collector],
        context_store=context_store,
    )

    ctx = _create_context(context_store)
    pipe_to_bus.put(SendEvent(ctx_id=ctx.id, source=source, payload="bus-payload"))

    jobs = Jobs(event_bus.run_loop(), collector.run_loop())
    wait_until(lambda: len(collector.received_events) == 1)

    received = collector.received_events[0]
    assert received.ctx_id == ctx.id
    assert received.payload == "bus-payload"

    event_bus.request_stop()
    jobs.join()


def test_event_bus_routes_events_to_configured_sinks():
    context_store = empty_context_store()
    pipe_to_bus: PipeToBus = queue.Queue()

    broadcast = Source("broadcast")
    collector_a = CollectorActor(name="collector-a", pipe_to_bus=pipe_to_bus, context_store=context_store)
    collector_b = CollectorActor(name="collector-b", pipe_to_bus=pipe_to_bus, context_store=context_store)

    piping = Piping()
    piping.add_flow(Flow.from_connectable(broadcast).then(collector_a.sink, collector_b.sink))

    event_bus = EventBus(
        connections=piping.pipes,
        input_pipe=pipe_to_bus,
        actors=[collector_a, collector_b],
        context_store=context_store,
    )

    ctx = _create_context(context_store)
    with context_store.get_context(ctx.id) as context:
        context.set_user_data("request", {"id": 42})

    pipe_to_bus.put(SendEvent(ctx_id=ctx.id, source=broadcast, payload={"message": "hello"}))
    pipe_to_bus.put(StopBusEvent())

    jobs = Jobs(event_bus.run_loop(), collector_a.run_loop(), collector_b.run_loop())

    wait_until(lambda: len(collector_a.received_events) == 1)
    wait_until(lambda: len(collector_b.received_events) == 1)

    received_a = collector_a.received_events[0]
    received_b = collector_b.received_events[0]

    assert received_a.payload == {"message": "hello"}
    assert received_b.payload == {"message": "hello"}
    assert received_a.ctx_id != ctx.id
    assert received_b.ctx_id != ctx.id
    assert received_a.ctx_id != received_b.ctx_id

    with context_store.get_context(received_a.ctx_id) as context_a:
        assert context_a.payload == {"message": "hello"}
        assert context_a.user_data == {"request": {"id": 42}}
        assert len(context_a.parent_contexts) == 1
        assert context_a.parent_contexts[0].ctx_id == ctx.id

    with context_store.get_context(received_b.ctx_id) as context_b:
        assert context_b.payload == {"message": "hello"}
        assert context_b.user_data == {"request": {"id": 42}}
        assert len(context_b.parent_contexts) == 1
        assert context_b.parent_contexts[0].ctx_id == ctx.id

    event_bus.request_stop()
    jobs.join()


def test_event_bus_appends_sent_messages_to_context_store():
    context_store = empty_context_store()
    pipe_to_bus: PipeToBus = queue.Queue()

    source = Source("context-store-source")
    collector = CollectorActor(pipe_to_bus=pipe_to_bus, context_store=context_store)

    piping = Piping()
    piping.add_flow(Flow.from_connectable(source).then(collector.sink))

    event_bus = EventBus(
        connections=piping.pipes,
        input_pipe=pipe_to_bus,
        actors=[collector],
        context_store=context_store,
    )

    ctx_one = _create_context(context_store)
    ctx_two = _create_context(context_store)
    event_one = SendEvent(ctx_id=ctx_one.id, source=source, payload="one")
    event_two = SendEvent(ctx_id=ctx_two.id, source=source, payload="two")
    pipe_to_bus.put(event_one)
    pipe_to_bus.put(event_two)
    pipe_to_bus.put(StopBusEvent())

    jobs = Jobs(event_bus.run_loop(), collector.run_loop())
    wait_until(lambda: len(collector.received_events) == 2)

    event_bus.request_stop()
    jobs.join()

    with context_store.get_context(ctx_one.id) as context_one:
        assert context_one.payload == "one"
    with context_store.get_context(ctx_two.id) as context_two:
        assert context_two.payload == "two"


def test_event_bus_logs_when_no_connections(caplog: Any):
    context_store = empty_context_store()
    pipe_to_bus: PipeToBus = queue.Queue()

    event_bus = EventBus(
        connections=Piping().pipes,
        input_pipe=pipe_to_bus,
        actors=[],
        context_store=context_store,
    )
    source = Source("orphan")
    ctx = _create_context(context_store)

    with caplog.at_level(logging.WARNING):
        pipe_to_bus.put(SendEvent(ctx_id=ctx.id, source=source, payload="payload"))
        event_loop = event_bus.run_loop()

        wait_until(lambda: any("No connections found" in record.message for record in caplog.records))

    event_bus.request_stop()
    event_loop.join(1.0)
    assert not event_loop.is_alive()


def test_actor_error_does_not_stop_event_bus(caplog: Any):
    context_store = empty_context_store()
    pipe_to_bus: PipeToBus = queue.Queue()
    source = Source("source")
    flaky = FlakyActor(pipe_to_bus=pipe_to_bus, context_store=context_store)

    collector = CollectorActor(pipe_to_bus=pipe_to_bus, context_store=context_store)

    piping = Piping()
    piping.add_flow(Flow.from_connectable(source).then(flaky.sink))
    piping.add_flow(Flow.from_connectable(flaky.source).then(collector.sink))

    event_bus = EventBus(
        connections=piping.pipes,
        input_pipe=pipe_to_bus,
        actors=[flaky, collector],
        context_store=context_store,
    )

    ctx_flaky_1 = _create_context(context_store)
    ctx_flaky_2 = _create_context(context_store)
    ctx_flaky_3 = _create_context(context_store)
    pipe_to_bus.put(SendEvent(ctx_id=ctx_flaky_1.id, source=source, payload="good-1"))
    pipe_to_bus.put(SendEvent(ctx_id=ctx_flaky_2.id, source=source, payload="boom"))
    pipe_to_bus.put(SendEvent(ctx_id=ctx_flaky_3.id, source=source, payload="good-2"))

    with caplog.at_level(logging.ERROR):
        jobs = Jobs(event_bus.run_loop(), flaky.run_loop(), collector.run_loop())
        wait_until(lambda: len(collector.received_events) == 2)

    event_bus.request_stop()
    jobs.join()

    assert [(event.payload, event.ctx_id) for event in collector.received_events] == [
        ("ok-good-1", ctx_flaky_1.id),
        ("ok-good-2", ctx_flaky_3.id),
    ]
    error_records = [record for record in caplog.records if "Error while handling event" in record.message]
    assert len(error_records) == 1
    assert any(
        record.exc_info and isinstance(record.exc_info[1], ValueError) and str(record.exc_info[1]) == "flaky failure"
        for record in error_records
    )


def test_double_transform_actor_routes_input_output_and_errors():
    context_store = empty_context_store()
    pipe_to_bus: PipeToBus = queue.Queue()
    actor = SomeDoubleTransformActor(pipe_to_bus=pipe_to_bus, context_store=context_store)

    ctx_input_ok = _create_context(context_store)
    ctx_input_error = _create_context(context_store)
    ctx_output_ok = _create_context(context_store)
    ctx_output_error = _create_context(context_store)

    actor.pipe_from_bus.put(ReceiveEvent(ctx_id=ctx_input_ok.id, target=actor.input_spec.sink, payload="success"))
    actor.pipe_from_bus.put(ReceiveEvent(ctx_id=ctx_input_error.id, target=actor.input_spec.sink, payload="fail-input"))
    actor.pipe_from_bus.put(ReceiveEvent(ctx_id=ctx_output_ok.id, target=actor.output_spec.sink, payload="payload"))
    actor.pipe_from_bus.put(
        ReceiveEvent(ctx_id=ctx_output_error.id, target=actor.output_spec.sink, payload="fail-output")
    )
    actor.pipe_from_bus.put(StopActorEvent())

    actor_job = actor.run_loop()
    actor_job.join(1.0)
    assert not actor_job.is_alive()

    events = [pipe_to_bus.get_nowait() for _ in range(4)]

    def _event_by_ctx(ctx: ContextId) -> SendEvent[Any]:
        return next(event for event in events if event.ctx_id == ctx)

    input_ok = _event_by_ctx(ctx_input_ok.id)
    input_error = _event_by_ctx(ctx_input_error.id)
    output_ok = _event_by_ctx(ctx_output_ok.id)
    output_error = _event_by_ctx(ctx_output_error.id)

    assert input_ok.source == actor.input_spec.ok
    assert input_ok.payload == "SUCCESS"
    assert input_ok.ctx_id == ctx_input_ok.id

    assert input_error.source == actor.input_spec.error
    assert isinstance(input_error.payload, SafeInvokeWrappedException)
    assert input_error.payload.__cause__ is not None
    assert isinstance(input_error.payload.__cause__, ValueError)
    assert str(input_error.payload.__cause__) == "input-failed"
    assert input_error.ctx_id == ctx_input_error.id

    assert output_ok.source == actor.output_spec.ok
    assert output_ok.payload == "payload-out"
    assert output_ok.ctx_id == ctx_output_ok.id

    assert output_error.source == actor.output_spec.error
    assert isinstance(output_error.payload, SafeInvokeWrappedException)
    assert output_error.payload.__cause__ is not None
    assert isinstance(output_error.payload.__cause__, ValueError)
    assert str(output_error.payload.__cause__) == "output-failed"
    assert output_error.ctx_id == ctx_output_error.id

    assert pipe_to_bus.empty()


def test_double_transform_actor_preserves_context_id():
    context_store = empty_context_store()
    pipe_to_bus: PipeToBus = queue.Queue()
    actor = SomeDoubleTransformActor(pipe_to_bus=pipe_to_bus, context_store=context_store)

    ctx_input = _create_context(context_store)
    ctx_output = _create_context(context_store)

    actor.pipe_from_bus.put(ReceiveEvent(ctx_id=ctx_input.id, target=actor.input_spec.sink, payload="in"))
    actor.pipe_from_bus.put(ReceiveEvent(ctx_id=ctx_output.id, target=actor.output_spec.sink, payload="out"))
    actor.pipe_from_bus.put(StopActorEvent())

    job = actor.run_loop()
    job.join(1.0)
    assert not job.is_alive()

    events = [pipe_to_bus.get_nowait(), pipe_to_bus.get_nowait()]
    input_event = next(event for event in events if event.ctx_id == ctx_input.id)
    output_event = next(event for event in events if event.ctx_id == ctx_output.id)

    assert input_event.ctx_id == ctx_input.id
    assert input_event.payload == "IN"
    assert input_event.source == actor.input_spec.ok

    assert output_event.ctx_id == ctx_output.id
    assert output_event.payload == "out-out"
    assert output_event.source == actor.output_spec.ok

    assert pipe_to_bus.empty()
