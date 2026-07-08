# pyright: basic

import copy
import random
import threading

import pytest

from nexus.v1 import (
    ContextCompleted,
    ContextCompletedException,
    ContextId,
    ContextStore,
    InMemoryContextStorePersistence,
    InternalStateCorruptionException,
    InvalidContextIdException,
    Source,
)


def _random_payloads(rng: random.Random, count: int) -> list[dict[str, int]]:
    # FIXME: make the random payloads more complex and realistic, e.g. nested structures, lists, etc.
    payload = {}
    keys = []
    extra_idx = 0
    payloads = []
    for _ in range(count):
        action = rng.choice(["update", "add", "remove"])
        if action == "add" or not keys:
            key = f"extra_{extra_idx}"
            extra_idx += 1
            payload[key] = rng.randint(0, 10)
            keys.append(key)
        elif action == "update":
            key = rng.choice(keys)
            if key not in keys:
                keys.append(key)
            payload[key] = payload.get(key, 0) + rng.randint(1, 5)
        else:
            key = rng.choice(keys)
            keys.remove(key)
            payload.pop(key, None)
        payloads.append(copy.deepcopy(payload))
    return payloads


def _random_user_data(rng: random.Random, count: int) -> list[tuple[str, int]]:
    # FIXME: make the random user data more complex and realistic, e.g. nested structures, lists, etc.
    user_data: list[tuple[str, int]] = []
    for _ in range(count):
        key = f"key_{rng.randint(0, 10)}"
        value = rng.randint(0, 100)
        user_data.append((key, value))
    return user_data


def _create_context(context_store: ContextStore, *, parents: tuple[ContextId, ...] = ()) -> ContextId:
    with context_store.create_context(parents=parents) as context:
        return context.id


def test_append_message_persists_and_recovers_payload():
    persistence = InMemoryContextStorePersistence()
    original_context_store = ContextStore.recover_from(persistence).context_store

    context_id = _create_context(original_context_store)

    source = Source("payload-source")

    payloads = [
        {"count": 1, "items": [0, 1]},
        {"count": 2, "items": [0, 1, 2]},
        {"items": [2, 3]},
    ]
    with original_context_store.get_context(context_id) as context:
        for payload in payloads:
            context.append_message(source=source, payload=payload)

    entries = persistence.log_entries()
    assert len(entries) == len(payloads) + 1  # +1 for the initial ContextCreated entry

    another_context_store = ContextStore.recover_from(persistence).context_store
    with another_context_store.get_context(context_id) as recovered_context:
        with original_context_store.get_context(context_id) as original_context:
            assert recovered_context.payload == original_context.payload


def test_set_user_data_persists_and_recovers():
    persistence = InMemoryContextStorePersistence()
    original_context_store = ContextStore.recover_from(persistence).context_store

    context_id = _create_context(original_context_store)

    with original_context_store.get_context(context_id) as context:
        context.set_user_data("count", 2)
        context.set_user_data("string", "asd")
        context.set_user_data("map", {"nested": True})

    entries = persistence.log_entries()
    assert len(entries) == 3 + 1  # +1 for the initial ContextCreated entry

    another_context_store = ContextStore.recover_from(persistence).context_store
    with another_context_store.get_context(context_id) as recovered_context:
        with original_context_store.get_context(context_id) as original_context:
            assert recovered_context.user_data == original_context.user_data


def test_get_context_returns_or_raises():
    persistence = InMemoryContextStorePersistence()
    context_store = ContextStore.recover_from(persistence).context_store

    context_id = _create_context(context_store)

    with context_store.get_context(context_id) as context:
        assert context.id == context_id

    with pytest.raises(InvalidContextIdException):
        with context_store.get_context(ContextId("ctx-missing")):
            pass


def test_get_context_provides_mutual_exclusion():
    persistence = InMemoryContextStorePersistence()
    context_store = ContextStore.recover_from(persistence).context_store
    context_id = _create_context(context_store)

    first_has_lock = threading.Event()
    release_first = threading.Event()
    second_acquired = threading.Event()

    def first_worker() -> None:
        with context_store.get_context(context_id):
            first_has_lock.set()
            release_first.wait(timeout=1.0)

    def second_worker() -> None:
        first_has_lock.wait(timeout=1.0)
        with context_store.get_context(context_id):
            second_acquired.set()

    t1 = threading.Thread(target=first_worker, daemon=True)
    t2 = threading.Thread(target=second_worker, daemon=True)
    t1.start()
    t2.start()

    assert first_has_lock.wait(timeout=1.0)
    assert not second_acquired.wait(timeout=0.1)

    release_first.set()
    t1.join(timeout=1.0)
    t2.join(timeout=1.0)

    assert not t1.is_alive()
    assert not t2.is_alive()
    assert second_acquired.is_set()


def test_get_user_data_returns_typed_value():
    persistence = InMemoryContextStorePersistence()
    context_store = ContextStore.recover_from(persistence).context_store
    context_id = _create_context(context_store)

    with context_store.get_context(context_id) as context:
        context.set_user_data("count", 2)

    count = context_store.get_user_data(context_id, "count", expected_type=int)
    assert count == 2


def test_get_user_data_raises_when_value_is_missing():
    persistence = InMemoryContextStorePersistence()
    context_store = ContextStore.recover_from(persistence).context_store
    context_id = _create_context(context_store)

    with pytest.raises(InternalStateCorruptionException, match="Missing context user_data"):
        context_store.get_user_data(context_id, "missing", expected_type=int)


def test_get_user_data_raises_when_value_type_is_invalid():
    persistence = InMemoryContextStorePersistence()
    context_store = ContextStore.recover_from(persistence).context_store
    context_id = _create_context(context_store)

    with context_store.get_context(context_id) as context:
        context.set_user_data("count", "2")

    with pytest.raises(InternalStateCorruptionException, match="Unexpected context user_data type"):
        context_store.get_user_data(context_id, "count", expected_type=int)


def test_complete_context_appends_terminal_log_entry_and_blocks_mutation():
    persistence = InMemoryContextStorePersistence()
    context_store = ContextStore.recover_from(persistence).context_store
    context_id = _create_context(context_store)
    source = Source("completion-source")

    with context_store.get_context(context_id) as context:
        context.set_user_data("k", "v")
        context.complete()

        with pytest.raises(ContextCompletedException):
            context.set_user_data("k", "new")
        with pytest.raises(ContextCompletedException):
            context.append_message(source=source, payload={"hello": "world"})

    entries = persistence.log_entries()
    assert isinstance(entries[-1].data, ContextCompleted)


def test_create_child_from_completed_parent_raises():
    persistence = InMemoryContextStorePersistence()
    context_store = ContextStore.recover_from(persistence).context_store
    parent_context_id = _create_context(context_store)

    with context_store.get_context(parent_context_id) as parent_context:
        parent_context.complete()

    with pytest.raises(ContextCompletedException):
        with context_store.create_context(parents=(parent_context_id,)):
            pass


def test_single_parent_child_inherits_context_data_and_records_parent_snapshot():
    persistence = InMemoryContextStorePersistence()
    context_store = ContextStore.recover_from(persistence).context_store
    parent_context_id = _create_context(context_store)
    source = Source("single-parent-source")

    with context_store.get_context(parent_context_id) as parent_context:
        parent_context.append_message(source=source, payload={"items": ["parent"]})
        parent_context.set_user_data("request", {"id": 7})

    child_context_id = _create_context(context_store, parents=(parent_context_id,))

    with context_store.get_context(parent_context_id) as parent_context:
        parent_context.append_message(source=source, payload={"items": ["updated-parent"]})
        parent_context.set_user_data("request", {"id": 8})

    with context_store.get_context(child_context_id) as child_context:
        assert child_context.payload == {"items": ["parent"]}
        assert child_context.user_data == {"request": {"id": 7}}
        assert len(child_context.parent_contexts) == 1

        parent_snapshot = child_context.parent_contexts[0]
        assert parent_snapshot.ctx_id == parent_context_id
        assert parent_snapshot.payload == {"items": ["parent"]}
        assert parent_snapshot.user_data == {"request": {"id": 7}}


def test_multi_parent_child_keeps_empty_state_and_records_parent_snapshots():
    persistence = InMemoryContextStorePersistence()
    context_store = ContextStore.recover_from(persistence).context_store
    parent_a_id = _create_context(context_store)
    parent_b_id = _create_context(context_store)
    source_a = Source("parent-a-source")
    source_b = Source("parent-b-source")

    with context_store.get_context(parent_a_id) as parent_a:
        parent_a.append_message(source=source_a, payload={"parent": "a"})
        parent_a.set_user_data("request", {"id": "a"})
    with context_store.get_context(parent_b_id) as parent_b:
        parent_b.append_message(source=source_b, payload={"parent": "b"})
        parent_b.set_user_data("request", {"id": "b"})

    child_context_id = _create_context(context_store, parents=(parent_a_id, parent_b_id, parent_a_id))

    with context_store.get_context(child_context_id) as child_context:
        assert child_context.payload is None
        assert child_context.user_data == {}
        assert tuple(snapshot.ctx_id for snapshot in child_context.parent_contexts) == (parent_a_id, parent_b_id)
        assert child_context.parent_contexts[0].payload == {"parent": "a"}
        assert child_context.parent_contexts[0].user_data == {"request": {"id": "a"}}
        assert child_context.parent_contexts[1].payload == {"parent": "b"}
        assert child_context.parent_contexts[1].user_data == {"request": {"id": "b"}}


def test_child_context_initial_data_and_parent_snapshots_are_recoverable():
    persistence = InMemoryContextStorePersistence()
    original_context_store = ContextStore.recover_from(persistence).context_store
    parent_context_id = _create_context(original_context_store)
    source = Source("recover-child-source")

    with original_context_store.get_context(parent_context_id) as parent_context:
        parent_context.append_message(source=source, payload={"recoverable": True})
        parent_context.set_user_data("request", {"id": 12})

    child_context_id = _create_context(original_context_store, parents=(parent_context_id,))

    recovered_context_store = ContextStore.recover_from(persistence).context_store
    with recovered_context_store.get_context(child_context_id) as recovered_child:
        assert recovered_child.payload == {"recoverable": True}
        assert recovered_child.user_data == {"request": {"id": 12}}
        assert len(recovered_child.parent_contexts) == 1
        assert recovered_child.parent_contexts[0].ctx_id == parent_context_id
        assert recovered_child.parent_contexts[0].payload == {"recoverable": True}
        assert recovered_child.parent_contexts[0].user_data == {"request": {"id": 12}}


def test_recovery_ignores_completed_contexts_and_replay_messages():
    persistence = InMemoryContextStorePersistence()
    original_context_store = ContextStore.recover_from(persistence).context_store
    active_context_id = _create_context(original_context_store)
    completed_context_id = _create_context(original_context_store)
    source = Source("recover-source")

    with original_context_store.get_context(active_context_id) as active_context:
        active_context.append_message(source=source, payload="active")

    with original_context_store.get_context(completed_context_id) as completed_context:
        completed_context.append_message(source=source, payload="completed")
        completed_context.complete()

    recovered = ContextStore.recover_from(persistence)

    with recovered.context_store.get_context(active_context_id) as recovered_active_context:
        assert recovered_active_context.payload == "active"
    assert active_context_id in recovered.last_messages

    with pytest.raises(InvalidContextIdException):
        with recovered.context_store.get_context(completed_context_id):
            pass
    assert completed_context_id not in recovered.last_messages


def test_randomized_payloads_are_recoverable():
    # FIXME: fix randomness so that seed is random but test is deterministic
    rng = random.Random(1337)
    persistence = InMemoryContextStorePersistence()
    original_context_store = ContextStore.recover_from(persistence).context_store

    ctx_id = _create_context(original_context_store)
    source = Source("random-source")

    payloads = _random_payloads(rng, 25)
    with original_context_store.get_context(ctx_id) as context:
        for payload in payloads:
            context.append_message(source=source, payload=payload)

    recovered = ContextStore.recover_from(persistence).context_store
    with recovered.get_context(ctx_id) as recovered_context:
        with original_context_store.get_context(ctx_id) as original_context:
            assert recovered_context.payload == original_context.payload


def test_randomized_user_data_is_recoverable():
    # FIXME: fix randomness so that seed is random but test is deterministic
    rng = random.Random(2024)
    persistence = InMemoryContextStorePersistence()
    original_context_store = ContextStore.recover_from(persistence).context_store

    ctx_id = _create_context(original_context_store)

    random_user_data = _random_user_data(rng, 30)

    with original_context_store.get_context(ctx_id) as context:
        for set_user_data_event in random_user_data:
            context.set_user_data(set_user_data_event[0], set_user_data_event[1])

    recovered = ContextStore.recover_from(persistence).context_store
    with recovered.get_context(ctx_id) as recovered_context:
        with original_context_store.get_context(ctx_id) as original_context:
            assert recovered_context.user_data == original_context.user_data


def test_recover_rebuilds_messages():
    persistence = InMemoryContextStorePersistence()
    original_context_store = ContextStore.recover_from(persistence).context_store

    context_1 = _create_context(original_context_store)
    _create_context(original_context_store, parents=(context_1,))

    # FIXME: add testcases for last message handling when the children context consumed the last messages
    # so we should replay them


def test_recover_multiple_contexts():
    persistence = InMemoryContextStorePersistence()
    original_context_store = ContextStore.recover_from(persistence).context_store

    ctx_a = _create_context(original_context_store)
    ctx_b = _create_context(original_context_store)

    source_a = Source("source-a")
    source_b = Source("source-b")

    with original_context_store.get_context(ctx_a) as context_a:
        context_a.append_message(source=source_a, payload=1)
        context_a.set_user_data("count", 2)
    with original_context_store.get_context(ctx_b) as context_b:
        context_b.append_message(source=source_b, payload=12)
        context_b.set_user_data("pound", 7)

    another_context_store = ContextStore.recover_from(persistence).context_store
    with another_context_store.get_context(ctx_a) as recovered_ctx_a:
        with original_context_store.get_context(ctx_a) as original_ctx_a:
            assert recovered_ctx_a.payload == original_ctx_a.payload
            assert recovered_ctx_a.user_data == original_ctx_a.user_data

    with another_context_store.get_context(ctx_b) as recovered_ctx_b:
        with original_context_store.get_context(ctx_b) as original_ctx_b:
            assert recovered_ctx_b.payload == original_ctx_b.payload
            assert recovered_ctx_b.user_data == original_ctx_b.user_data
