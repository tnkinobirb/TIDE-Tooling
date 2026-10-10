import threading
from types import SimpleNamespace

import pytest

from tide_tooling import TIDE, WRITE_NOW, node, pipeline
from tide_tooling.tooling_core.engine.classes import DataRef, Node, Pipeline
from tide_tooling.tooling_core.engine.events import (
    Cause,
    Event,
    LogEvents,
    OceanEvents,
    PipelineEvents,
    RouterEvents,
)
from tide_tooling.tooling_core.engine.event_occurrences import EventLogger
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


def test_competing_now_writes_share_one_authoritative_order():
    start = threading.Barrier(4)

    def make_writer(label):
        @node(targets=[f"global:finished_{label}"])
        def writer():
            start.wait(timeout=2)
            assert WRITE_NOW("global:race", label) is True
            return label

        return writer

    writers = [make_writer(label) for label in ("a", "b", "c", "d")]

    @pipeline
    def race():
        return [writer() for writer in writers]

    tide = TIDE()
    trace = tide.ocean.run_pipeline(race, mode="thread")
    state_events = [
        event
        for event in trace.events
        if event.type in {
            PipelineEvents.DEP_AVAILABLE,
            PipelineEvents.DEP_UPDATED,
        }
        and DataRef.to_string(event.payload.ref, prefix=False) == "global:race"
    ]
    event_positions = {
        event.event_id: position
        for position, event in enumerate(trace.events)
    }

    assert len(state_events) == 4
    assert tide.ocean.store_router.get("global", "race") == (
        state_events[-1].payload.value
    )
    assert trace.value_of("global:race")[-1] == state_events[-1].payload.value
    for state_event in state_events:
        effect_event = next(
            event
            for event in trace.events
            if event.event_id == state_event.cause.event_id
        )
        assert effect_event.type == LogEvents.EFFECT
        assert event_positions[effect_event.event_id] < event_positions[
            state_event.event_id
        ]


def test_memory_store_dump_is_a_mapping_snapshot():
    store = MemoryKVStore(store_template={"before": 1})

    snapshot = store.dump()
    store.set("after", 2)

    assert snapshot == {"before": 1}
    assert store.dump() == {"before": 1, "after": 2}


def test_store_router_never_handles_two_queued_events_at_once():
    router = StoreRouter(
        StoreRegistry(),
        event_interface=SimpleNamespace(
            emit_event_to_ocean=lambda event: None,
            event_subscriber=lambda *args: None,
        ),
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
            handled.append(event.event_id)
        if position == 0:
            first_entered.set()
            assert release_first.wait(timeout=2)
        else:
            second_entered.set()
        with state_lock:
            active -= 1

    router.event_handlers[RouterEvents.STORE_WRITE] = handle
    router.consume_event(Event(source="test", source_id="test", type=RouterEvents.STORE_WRITE, cause=Cause(description="test"), ocean_id=0))
    router.consume_event(Event(source="test", source_id="test", type=RouterEvents.STORE_WRITE, cause=Cause(description="test"), ocean_id=0))

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
            routed.append(event.event_id)
        if position == 0:
            first_entered.set()
            assert release_first.wait(timeout=2)
        else:
            second_entered.set()
        with state_lock:
            active -= 1

    ocean.event_router.route = route
    first = Event(source="test", source_id="test", type=OceanEvents.QUIT, cause=Cause(description="test"), ocean_id=ocean.id)
    second = Event(source="test", source_id="test", type=OceanEvents.QUIT, cause=Cause(description="test"), ocean_id=ocean.id)
    ocean.consume_event(first)
    ocean.consume_event(second)

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

    assert routed == [first.event_id, second.event_id]
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
        nodes=[Node(long_running_worker, targets=["@result"])],
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
    ocean.consume_event(Event(ocean_id=ocean.id, 
        source="test",
        source_id="test",
        target_runner_id=runner_id,
        type=OceanEvents.KILL_RUNNER,
        cause=Cause(description="test"),
    ))

    finished_while_worker_blocked = caller_finished.wait(timeout=0.5)
    release_worker.set()
    _join(caller)
    ocean.close()

    assert finished_while_worker_blocked
    assert "error" not in outcome
    assert outcome["trace"] is ocean.last_trace


def test_ocean_rejects_a_second_public_run_while_one_is_active():
    worker_started = threading.Event()
    release_worker = threading.Event()

    def long_running_worker():
        worker_started.set()
        assert release_worker.wait(timeout=2)
        return "first"

    ocean = Ocean(timeout=2)
    first_pipeline = Pipeline(
        nodes=[Node(long_running_worker, targets=["@first"])],
        name="first-run",
    )
    second_pipeline = Pipeline(
        nodes=[Node(lambda: "second", targets=["@second"])],
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
        match="does not support concurrent calls",
    ):
        ocean.run_pipeline(second_pipeline, mode="sequential")

    runner_id = next(iter(ocean.running))
    ocean.consume_event(Event(ocean_id=ocean.id, 
        source="test",
        source_id="test",
        target_runner_id=runner_id,
        type=OceanEvents.KILL_RUNNER,
        cause=Cause(description="test"),
    ))
    first_caller.join(timeout=0.5)
    assert not first_caller.is_alive()
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
        nodes=[Node(lambda: "value", targets=["global:result"])],
        name="invalid-mode",
    )

    with pytest.raises(ValueError, match="execution mode must be one of"):
        ocean.run_pipeline(pipeline, mode="bogus")

    ocean.close()


def test_event_router_rejects_an_event_type_without_a_subscriber():
    router = EventRouter(EventLogger())

    with pytest.raises(TypeError, match="Unsupported Event type"):
        router.route(Event(source="test", source_id="test", type="unknown", cause=Cause(description="test"), ocean_id=0))


def test_register_pipeline_records_the_definition():
    ocean = Ocean()
    pipeline = Pipeline(nodes=[])
    registered = ocean.register_pipeline(pipeline)

    assert registered is pipeline
    assert ocean.pipelines[pipeline.id][0] is pipeline
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
        nodes=[Node(long_running_worker, targets=["global:result"])],
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
