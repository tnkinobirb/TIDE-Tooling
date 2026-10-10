import pytest

from tide_tooling import TIDE, WRITE, node, pipeline
from tide_tooling.tooling_core.engine.event_occurrences import (
    TIDEEventDeserialiser,
)
from testing.replay_helpers import serialise_events
from tide_tooling.tooling_core.engine.classes import DataRef
from tide_tooling.tooling_core.engine.event_payloads import (
    PipelineRun,
    Reference,
    RunnerPlan,
    State,
)
from tide_tooling.tooling_core.engine.event_readers import EventLogReader
from tide_tooling.tooling_core.engine.events import (
    Cause,
    Event,
    LogEvents,
    OceanEvents,
    PipelineEvents,
    RouterEvents,
)
from tide_tooling.tooling_core.engine.tide_logging import Trace
from tide_tooling.tooling_core.engine.tide_records import (
    Actor,
    AmbiguousActorOccurrence,
    ImmediateEffectRecord,
    ImmediateExecutionReference,
    ReadyAttemptRecord,
)
from tide_tooling.tooling_core.engine.utils import tide_snapshot_value


def test_logged_state_and_effect_values_are_frozen_at_publication():
    state_value = {"items": [1]}
    effect_output = {"items": [2]}
    tide = TIDE()

    tide.declare("global:data", state_value)
    tide.event(
        "writer",
        effects=WRITE("global:effect", 3, output=effect_output),
    )
    state_value["items"].append(9)
    effect_output["items"].append(9)

    trace = tide.trace()
    state_event = next(
        event
        for event in trace.events
        if event.type == PipelineEvents.DEP_AVAILABLE
        and event.payload.ref == DataRef("data", "global")
    )
    effect_event = next(
        event
        for event in trace.events
        if event.type == LogEvents.EFFECT
    )

    assert state_event.payload.value == {"items": [1]}
    assert state_event.payload.snapshot.restore() == {"items": [1]}
    assert effect_event.payload.output == {"items": [2]}


def test_event_state_retains_every_published_field_without_reconciliation():
    value = [1]
    event = Event(
        ocean_id=0,
        source="test",
        source_id="test",
        type=PipelineEvents.DEP_AVAILABLE,
        cause=Cause(description="state"),
        payload=State(
            DataRef("value", "global"),
            value,
            tide_snapshot_value(value, boundary="write"),
        ),
    )
    graph = serialise_events((event,))
    state_record = next(
        record
        for record in graph["records"].values()
        if record.get("type", "").endswith(":State")
    )
    assert set(state_record["fields"]) == {
        "ref", "value", "snapshot", "state_occurrence",
        "value_type",
    }
    restored = TIDEEventDeserialiser().deserialise(graph)[0]
    assert restored.payload.value == [1]
    assert restored.payload.snapshot.value_hash == event.payload.snapshot.value_hash


def test_store_write_snapshot_is_taken_at_the_store_linearization_turn():
    tide = TIDE()
    value = [1]
    request = Event(
        ocean_id=tide.ocean.id,
        source="test",
        source_id="test",
        type=RouterEvents.STORE_WRITE,
        cause=Cause(description="queued write"),
        target_runner_id="*",
        payload=State(
            DataRef("value", "global"),
            value,
            tide_snapshot_value(value, boundary="queued write"),
        ),
    )

    with tide.ocean._scheduler_context():
        tide.ocean.event_router.route(request)
        value.append(2)
        tide.ocean.store_router.tick()

    events = tide.ocean.event_logger.events
    requested = next(event for event in events if event.event_id == request.event_id)
    consequence = next(
        event
        for event in events
        if event.cause.event_id == request.event_id
        and event.type == PipelineEvents.DEP_AVAILABLE
    )

    assert requested.payload.value == [1]
    assert consequence.payload.value == [1, 2]
    assert consequence.payload.snapshot.restore() == [1, 2]
    assert tide.ocean.store_router.get("global", "value") == [1, 2]


def test_delete_ends_current_availability_without_erasing_history():
    ref = DataRef("value", "global")
    available = Event(
        ocean_id=0,
        source="test",
        source_id="test",
        type=PipelineEvents.DEP_AVAILABLE,
        cause=Cause(description="available"),
        payload=State(ref, 1, tide_snapshot_value(1, boundary="write")),
    )
    deleted = Event(
        ocean_id=0,
        source="test",
        source_id="test",
        type=PipelineEvents.DEP_DELETED,
        cause=Cause(description="deleted"),
        payload=Reference(ref),
    )
    reader = EventLogReader(0)
    state = reader._construct_log((available, deleted))
    reference = DataRef.to_string(ref, prefix=False)

    assert reference not in state.ref_to_last_value
    assert reference not in state.ref_to_last_snapshot
    assert reference not in state.ref_to_last_producer
    assert state.versioned_references[reference] == 0
    assert (reference, 0) in state.producer_to_exec_id


def test_unresolved_execution_projection_is_sealed():
    actor = Actor(0, 0, "worker")
    exec_id = "TIDE_EXEC:unresolved"
    occurrence = AmbiguousActorOccurrence(actor, 0, exec_id)
    reference = ImmediateExecutionReference(
        exec_id=exec_id,
        actor=actor,
        node_definition_id="node-definition",
        ambiguous_occurrence=occurrence,
    )
    immediate = ImmediateEffectRecord(
        seq_id=0,
        event_id="effect-event",
        event_seq_id=1,
        exec_id=exec_id,
        execution_reference=reference,
    )
    trace = Trace(log=(immediate,), ocean_id=0)

    unresolved = trace.execs[exec_id]
    with pytest.raises(AttributeError):
        unresolved.status = "succeeded"


def test_store_owns_seed_mapping_but_preserves_contained_runtime_values():
    shared = []
    seed = {"existing": shared}
    tide = TIDE()
    tide.register_store("owned", store_template=seed)
    seed["invented"] = 2

    store = tide.ocean.store_router.get_store("owned")
    assert not store.exists("invented")
    assert store.get("existing") is shared


def test_causal_child_pending_attempt_prevents_complete_root_scope():
    actor = Actor(0, 1, "child")
    attempt = ReadyAttemptRecord(
        event_id="ready-event",
        event_seq_id=3,
        exec_id="TIDE_EXEC:child",
        actor=actor,
        node_definition_id="child-node",
        runner_id=1,
        pipeline_id=1,
    )
    trace = Trace(
        log=(),
        runner=0,
        ocean_id=0,
        ready_attempts=(attempt,),
        started_runner_ids=(0, 1),
        causal_runner_ids=(0, 1),
        ended_runner_ids=(0, 1),
    )

    assert trace.pipeline_ended is True
    assert trace.scope == "ambiguous"
    assert trace.unresolved_ready_attempts == (trace.ready_attempts[0],)
    assert trace.corruption["incomplete_executions"] == {
        "TIDE_EXEC:child": "ready-event"
    }


def test_forward_replay_derives_root_causal_runner_tree():
    root = Event(
        ocean_id=0,
        source="ocean",
        source_id=0,
        type=OceanEvents.RUN_PIPELINE,
        cause=Cause(description="root run"),
        payload=PipelineRun("root-pipeline", None, "auto", {}, False, None, None),
    )
    root_plan = Event(
        ocean_id=0,
        source="runner",
        source_id=0,
        target_runner_id=0,
        type=LogEvents.RUNNER_PLAN,
        cause=Cause.from_event(root),
        payload=RunnerPlan({}),
    )
    child = Event(
        ocean_id=0,
        source="runner",
        source_id=0,
        target_runner_id=0,
        type=OceanEvents.RUN_PIPELINE,
        cause=Cause.from_event(root_plan),
        payload=PipelineRun("child-pipeline", None, "auto", {}, False, None, None),
    )
    child_plan = Event(
        ocean_id=0,
        source="runner",
        source_id=1,
        target_runner_id=1,
        type=LogEvents.RUNNER_PLAN,
        cause=Cause.from_event(child),
        payload=RunnerPlan({}),
    )

    state = EventLogReader(0)._construct_log(
        (root, root_plan, child, child_plan)
    )

    assert state.root_run_event_id == root.event_id
    assert state.causal_runner_ids == {0, 1}


def test_unexpected_scheduler_failure_permanently_closes_ocean():
    @node
    def work():
        return 1

    @pipeline
    def flow():
        return work()

    tide = TIDE()
    original = tide.ocean.handlers[OceanEvents.RUN_PIPELINE]

    def fail_after_start(event):
        original(event)
        raise RuntimeError("scheduler broke")

    tide.ocean.handlers[OceanEvents.RUN_PIPELINE] = fail_after_start

    with pytest.raises(RuntimeError, match="scheduler broke"):
        tide.ocean.run_pipeline(flow, mode="sequential")

    assert tide.ocean.closed is True
    assert tide.ocean.running == {}
    assert tide.ocean.event_router.subscribed_runners == {}
    assert tide.ocean.executor._closed is True
    with pytest.raises(RuntimeError, match="Ocean is closed"):
        tide.ocean.run_pipeline(flow, mode="sequential")


def test_trace_show_marks_explicit_pipeline_start():
    @node
    def work():
        return 1

    @pipeline
    def flow():
        return work()

    trace = TIDE().ocean.run_pipeline(flow, mode="sequential")
    rendered = trace.show(color=False)

    assert "---- PIPELINE START" in rendered
    assert "---- PIPELINE END" in rendered
