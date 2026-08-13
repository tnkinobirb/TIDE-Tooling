import threading
from types import SimpleNamespace

import pytest

from tide_tooling.tooling_core.engine.classes import Event
from tide_tooling.tooling_core.engine.classes import Node, Pipeline
from tide_tooling.tooling_core.engine.events import OceanEvents, RouterEvents
from tide_tooling.tooling_core.engine.ocean import Ocean
from tide_tooling.tooling_core.engine.registries import StoreRegistry
from tide_tooling.tooling_core.engine.routers import EventRouter, StoreRouter
from tide_tooling.tooling_core.engine.stores import MemoryKVStore


def _join(thread):
    thread.join(timeout=2)
    assert not thread.is_alive()


def test_memory_store_methods_are_safe_for_concurrent_callers():
    store = MemoryKVStore()
    start = threading.Barrier(9)

    def write_partition(partition):
        start.wait(timeout=2)
        for offset in range(100):
            key = f"{partition}:{offset}"
            store.set(key, offset)
            assert store.exists(key)
            assert store.get(key) == offset

    workers = [
        threading.Thread(target=write_partition, args=(partition,))
        for partition in range(8)
    ]
    for worker in workers:
        worker.start()
    start.wait(timeout=2)
    for worker in workers:
        _join(worker)

    assert len(store.keys()) == 800


def test_memory_store_dump_is_a_mapping_snapshot():
    store = MemoryKVStore(store_template={"before": 1})

    snapshot = store.dump()
    store.set("after", 2)

    assert snapshot == {"before": 1}
    assert store.dump() == {"before": 1, "after": 2}


def test_store_router_never_handles_two_queued_events_at_once():
    router = StoreRouter(
        StoreRegistry(),
        event_interface=SimpleNamespace(emit_event_to_ocean=lambda event: None),
        subscribe_events=False,
    )
    first_entered = threading.Event()
    second_entered = threading.Event()
    release_first = threading.Event()
    state_lock = threading.Lock()
    active = 0
    maximum_active = 0
    handled = []

    def handle(event):
        nonlocal active, maximum_active
        with state_lock:
            active += 1
            maximum_active = max(maximum_active, active)
            position = len(handled)
            handled.append(event.id)
        if position == 0:
            first_entered.set()
            assert release_first.wait(timeout=2)
        else:
            second_entered.set()
        with state_lock:
            active -= 1

    router.event_handlers[RouterEvents.STORE_WRITE] = handle
    router.consume_event(Event(event_type=RouterEvents.STORE_WRITE))
    router.consume_event(Event(event_type=RouterEvents.STORE_WRITE))

    first_tick = threading.Thread(target=router.tick)
    second_tick = threading.Thread(target=router.tick)
    first_tick.start()
    assert first_entered.wait(timeout=2)
    second_tick.start()

    assert not second_entered.wait(timeout=0.1)
    release_first.set()
    _join(first_tick)
    _join(second_tick)

    assert len(handled) == 2
    assert maximum_active == 1


def test_ocean_never_executes_two_scheduling_turns_at_once():
    ocean = Ocean()
    first_entered = threading.Event()
    second_entered = threading.Event()
    release_first = threading.Event()
    state_lock = threading.Lock()
    active = 0
    maximum_active = 0
    routed = []

    def route(event):
        nonlocal active, maximum_active
        with state_lock:
            active += 1
            maximum_active = max(maximum_active, active)
            position = len(routed)
            routed.append(event.id)
        if position == 0:
            first_entered.set()
            assert release_first.wait(timeout=2)
        else:
            second_entered.set()
        with state_lock:
            active -= 1

    ocean.event_router.route = route
    ocean.consume_event(Event(event_type=OceanEvents.SET))
    ocean.consume_event(Event(event_type=OceanEvents.SET))

    first_tick = threading.Thread(target=ocean.tick, kwargs={"external": True})
    second_tick = threading.Thread(target=ocean.tick, kwargs={"external": True})
    first_tick.start()
    assert first_entered.wait(timeout=2)
    second_tick.start()

    assert not second_entered.wait(timeout=0.1)
    release_first.set()
    _join(first_tick)
    _join(second_tick)
    ocean.close(wait=False)

    assert routed == sorted(routed)
    assert maximum_active == 1


def test_ocean_can_kill_a_runner_while_its_thread_worker_is_still_running():
    worker_started = threading.Event()
    release_worker = threading.Event()
    caller_finished = threading.Event()
    outcome = {}

    def long_running_worker():
        worker_started.set()
        assert release_worker.wait(timeout=2)
        return "finished"

    ocean = Ocean(timeout=2)
    pipeline = Pipeline(
        stages=[Node(long_running_worker, targets=["@result"])],
        name="kill-responsive",
    )

    def run():
        try:
            outcome["trace"] = ocean.run_pipeline(pipeline, mode="thread")
        except Exception as error:
            outcome["error"] = error
        finally:
            caller_finished.set()

    caller = threading.Thread(target=run)
    caller.start()
    assert worker_started.wait(timeout=2)

    runner_id = next(iter(ocean.running))
    ocean.consume_event(Event(
        source="test",
        target_runner_id=runner_id,
        event_type=OceanEvents.KILL_PIPELINE,
    ))

    finished_while_worker_blocked = caller_finished.wait(timeout=0.5)
    release_worker.set()
    _join(caller)
    ocean.close()

    assert finished_while_worker_blocked
    assert "error" not in outcome
    assert outcome["trace"] is ocean.runner_to_trace[runner_id]


def test_ocean_rejects_a_second_public_run_while_one_is_active():
    worker_started = threading.Event()
    release_worker = threading.Event()

    def long_running_worker():
        worker_started.set()
        assert release_worker.wait(timeout=2)
        return "first"

    ocean = Ocean(timeout=2)
    first_pipeline = Pipeline(
        stages=[Node(long_running_worker, targets=["@first"])],
        name="first-run",
    )
    second_pipeline = Pipeline(
        stages=[Node(lambda: "second", targets=["@second"])],
        name="second-run",
    )
    first_outcome = {}

    def run_first():
        first_outcome["trace"] = ocean.run_pipeline(
            first_pipeline,
            mode="thread",
        )

    first_caller = threading.Thread(target=run_first)
    first_caller.start()
    assert worker_started.wait(timeout=2)

    with pytest.raises(
        RuntimeError,
        match="does not support concurrent public calls",
    ):
        ocean.run_pipeline(second_pipeline, mode="sequential")

    runner_id = next(iter(ocean.running))
    ocean.consume_event(Event(
        source="test",
        target_runner_id=runner_id,
        event_type=OceanEvents.KILL_PIPELINE,
    ))
    release_worker.set()
    _join(first_caller)
    ocean.close()

    assert second_pipeline.id is None
    assert "trace" in first_outcome


def test_closed_ocean_rejects_all_start_boundaries():
    ocean = Ocean()
    ocean.close(wait=False)

    with pytest.raises(RuntimeError, match="Ocean is closed"):
        ocean.start()
    with pytest.raises(RuntimeError, match="Ocean is closed"):
        ocean.start_threaded()


def test_unknown_execution_mode_is_rejected_with_the_valid_modes():
    ocean = Ocean(timeout=1)
    pipeline = Pipeline(
        stages=[Node(lambda: "value", targets=["global:result"])],
        name="invalid-mode",
    )

    with pytest.raises(ValueError, match="execution mode must be one of"):
        ocean.run_pipeline(pipeline, mode="bogus")

    ocean.close()


def test_event_router_rejects_an_event_type_without_a_subscriber():
    router = EventRouter(SimpleNamespace(log_event=lambda event: None))

    with pytest.raises(ValueError, match="No event subscriber"):
        router.route(Event(event_type="unknown"))


@pytest.mark.parametrize(
    ("event_type", "message"),
    [
        (OceanEvents.SET, "RouterEvents.STORE_WRITE"),
        (OceanEvents.DELETE, "RouterEvents.DELETE"),
        (OceanEvents.REGISTER_PIPELINE, "Ocean.register_pipeline"),
    ],
)
def test_unimplemented_ocean_events_fail_at_the_exact_boundary(
    event_type,
    message,
):
    ocean = Ocean()

    with pytest.raises(NotImplementedError, match=message):
        ocean._handle_ocean_event(Event(event_type=event_type))

    ocean.close(wait=False)


def test_close_unblocks_an_active_public_run_without_waiting_for_worker():
    worker_started = threading.Event()
    release_worker = threading.Event()
    caller_finished = threading.Event()
    outcome = {}

    def long_running_worker():
        worker_started.set()
        assert release_worker.wait(timeout=2)
        return "finished"

    ocean = Ocean()
    pipeline = Pipeline(
        stages=[Node(long_running_worker, targets=["global:result"])],
        name="close-responsive",
    )

    def run():
        try:
            ocean.run_pipeline(pipeline, mode="thread")
        except Exception as error:
            outcome["error"] = error
        finally:
            caller_finished.set()

    caller = threading.Thread(target=run)
    caller.start()
    assert worker_started.wait(timeout=2)

    ocean.close(wait=False)
    assert caller_finished.wait(timeout=0.5)
    release_worker.set()
    _join(caller)

    assert isinstance(outcome.get("error"), RuntimeError)
    assert "closed before the active run produced evidence" in str(
        outcome["error"]
    )
