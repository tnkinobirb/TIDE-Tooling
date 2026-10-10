import json
import copy
from types import SimpleNamespace
from pathlib import Path

import pytest

from tide_tooling import TIDE, WRITE, WRITE_NOW, node, pipeline
from tide_tooling.tooling_core.engine.event_occurrences import (
    EventOccurrence,
    FrozenEventLog,
    FrozenEventLogBundle,
    TIDEEventSerialiser,
)
from tide_tooling.tooling_core.bundling.TIDEBundle import TIDEBundle
from tide_tooling.tooling_core.bundling.TIDEBundler import TIDEBundler
from tide_tooling.tooling_core.engine.classes import DataRef, __fallback__
from tide_tooling.tooling_core.engine.event_payloads import (
    EmittedNodes,
    EffectOccurrence,
    PipelineRun,
    Reference,
    State,
    NodeBlocked,
)
from tide_tooling.tooling_core.engine.events import (
    Cause,
    Effects,
    Event,
    ExecutorEvents,
    LogEvents,
    OceanEvents,
    PipelineEvents,
    RouterEvents,
)
from tide_tooling.tooling_core.engine.tide_runtime_logging import (
    trace_from_event_log,
)
from testing.replay_helpers import (
    deserialise_events,
    replay_events,
    serialise_events,
)
from tide_tooling.tooling_core.engine.ocean import Ocean
from tide_tooling.tooling_core.engine.tide_records import ImmediateEffectRecord
from tide_tooling.tooling_core.engine.utils import (
    tide_snapshot_value,
    tide_value_hasher,
)


class _SelfDeepcopyMutable:
    def __init__(self, items):
        self.items = items

    def __deepcopy__(self, memo):
        return self


def _replay_log(events, ocean_id):
    return replay_events(
        events,
        expected_ocean_id=ocean_id,
    ).exec_log


def test_child_runner_cannot_become_an_invented_event_trace_scope():
    before = Event(source="ocean", source_id=0, type=LogEvents.RUNNER_DEADLOCK, cause=Cause(description="before"), ocean_id=0, payload={})
    root = Event(source="ocean", source_id=0, type=LogEvents.RUNNER_DEADLOCK, cause=Cause(description="root"), ocean_id=0, payload={})
    child = Event(source="runner", source_id=1, type=LogEvents.RUNNER_DEADLOCK, cause=Cause.from_event(root), ocean_id=0, payload={})
    leaf = Event(source="runner", source_id=2, type=LogEvents.RUNNER_DEADLOCK, cause=Cause.from_event(child), ocean_id=0, payload={})
    sibling = Event(source="ocean", source_id=0, type=LogEvents.RUNNER_DEADLOCK, cause=Cause(description="sibling"), ocean_id=0, payload={})
    runner = SimpleNamespace(
        id=2,
        pipeline=None,
        index=None,
        trace_event_start_id=root.event_id,
        runner_start_event_id=child.event_id,
        _tide_plan=None,
    )
    ocean = SimpleNamespace(
        id=0,
        event_logger=SimpleNamespace(
            freeze=lambda: (before, root, child, leaf, sibling),
        ),
        pipelines={},
    )
    with pytest.raises(ValueError, match="root Runner"):
        Ocean._trace_from_runner(ocean, runner)


def test_immediate_effect_has_its_stream_position_and_resolves_only_on_result():
    @node(targets=["global:done"])
    def writer():
        WRITE_NOW("global:now", 1)
        return True

    tide = TIDE()
    tide.exec(writer)
    trace = tide.trace()
    immediate = next(
        record
        for record in trace.log
        if isinstance(record, ImmediateEffectRecord)
    )
    execution = trace.execs[immediate.exec_id]

    assert immediate.seq_id < execution.seq_id
    assert immediate.execution_reference.resolved is True
    assert immediate.execution_reference.resolved_seq_id == execution.seq_id
    assert (
        immediate.execution_reference.resolved_actor_occurrence
        == execution.producer_occurrence_id
    )
    assert immediate.effects[0] in execution.effects
    assert immediate.writes[0] in execution.writes
    inspected = trace.inspect(immediate.seq_id)
    assert inspected.type == "immediate_effect_record"
    assert inspected.object.object is immediate
    rendered = trace.show(color=False, full_trace=True)
    assert f"{immediate.seq_id}: IMMEDIATE EFFECT" in rendered
    assert "AMBIGUOUS SCOPE" in rendered


def test_immediate_effect_prefix_is_retained_without_inventing_completion():
    @node(targets=["global:done"])
    def writer():
        WRITE_NOW("global:now", 1)
        return True

    tide = TIDE()
    tide.exec(writer)
    trace = tide.trace()
    result_index = next(
        index
        for index, event in enumerate(trace.events)
        if event.type == PipelineEvents.RESULT_READY
    )
    prefix = trace.events[:result_index]
    rebuilt = _replay_log(prefix, trace.ocean_id)
    immediate = next(
        record
        for record in rebuilt.record_log
        if isinstance(record, ImmediateEffectRecord)
    )

    assert not rebuilt.exec_log
    assert immediate.execution_reference.resolved is False
    assert immediate.execution_reference.ambiguous_occurrence is not None
    assert immediate.effects
    assert immediate.writes
    assert rebuilt.ready_attempts[0].outcome == "ready"


def test_replaying_a_prefix_is_unchanged_by_its_later_suffix():
    @node(targets=["global:done"])
    def writer():
        WRITE_NOW("global:now", 1)
        return True

    tide = TIDE()
    tide.exec(writer)
    events = tide.trace().events
    result_index = next(
        index
        for index, event in enumerate(events)
        if event.type == PipelineEvents.RESULT_READY
    )
    prefix = events[:result_index]

    before = trace_from_event_log(
        prefix,
        ocean_id=tide.ocean.id,
    ).to_model(enrich=True)
    replay_events(events, expected_ocean_id=tide.ocean.id)
    after = trace_from_event_log(
        prefix,
        ocean_id=tide.ocean.id,
    ).to_model(enrich=True)

    assert after == before


def test_trace_never_consumes_a_separate_supplied_execution_log():
    tide = TIDE()
    tide.event("writer", writes={"global:value": 1})
    events = tide.trace().events
    replayed = _replay_log(events, tide.ocean.id)
    object.__setattr__(replayed.record_log[0], "status", "invented")

    rebuilt = trace_from_event_log(events, ocean_id=tide.ocean.id)
    assert rebuilt.log[0].status != "invented"


def test_trace_rejects_an_ocean_identity_not_carried_by_its_events():
    tide = TIDE()
    tide.event("writer", writes={"global:value": 1})
    source = tide.trace()

    with pytest.raises(
        ValueError,
        match="ocean identity disagrees",
    ):
        trace_from_event_log(
            source.events,
            ocean_id="invented-ocean",
        )




def test_unresolved_immediate_activity_projects_as_one_ambiguous_execution():
    @node(targets=["global:done"])
    def writer():
        WRITE_NOW("global:first", 1)
        WRITE_NOW("global:second", 2)
        return True

    tide = TIDE()
    tide.exec(writer)
    source = tide.trace()
    result_index = next(
        index
        for index, event in enumerate(source.events)
        if event.type == PipelineEvents.RESULT_READY
    )
    events = source.events[:result_index]
    rebuilt = _replay_log(events, source.ocean_id)
    trace = trace_from_event_log(
        events,
        ocean_id=source.ocean_id,
    )

    model = trace.to_model()

    assert len(model["E"]) == 1
    ambiguous_exec_id = model["E"][0]
    assert "[!" in ambiguous_exec_id
    assert model["executions"][ambiguous_exec_id]["ambiguous"] is True
    assert model["executions"][ambiguous_exec_id]["status"] == "ambiguous"
    assert {
        entry["state_id"]
        for entry in model["O"]
    } == {
        "global:first[v0]",
        "global:second[v0]",
    }


def test_bare_unresolved_readiness_does_not_invent_a_model_execution():
    @node(targets=["global:done"])
    def writer():
        return True

    tide = TIDE()
    tide.exec(writer)
    source = tide.trace()
    ready_index = next(
        index
        for index, event in enumerate(source.events)
        if event.type == ExecutorEvents.NODE_READY
    )
    events = source.events[:ready_index + 1]
    rebuilt = _replay_log(events, source.ocean_id)
    trace = trace_from_event_log(
        events,
        ocean_id=source.ocean_id,
    )

    assert trace.to_model()["E"] == []


def test_unresolved_immediate_bundle_replays_and_reaches_a_fixed_point():
    @node(targets=["global:done"])
    def writer():
        WRITE_NOW("global:now", 1)
        return True

    tide = TIDE()
    tide.exec(writer)
    source = tide.trace()
    result_index = next(
        index
        for index, event in enumerate(source.events)
        if event.type == PipelineEvents.RESULT_READY
    )
    events = source.events[:result_index]
    rebuilt = _replay_log(events, source.ocean_id)
    trace = trace_from_event_log(
        events,
        ocean_id=source.ocean_id,
    )

    first = trace.to_bundle().export(type="dict", kind="json")
    restored = TIDE.from_bundle(first)
    second = restored.to_bundle().export(type="dict", kind="json")

    assert second == first
    assert restored.scope == "ambiguous"
    assert len(restored.to_model()["E"]) == 1
    assert isinstance(restored.log[0], ImmediateEffectRecord)
    assert restored.log[0].execution_reference.resolved is False


def test_native_bundle_reexport_is_a_fixed_point():
    tide = TIDE()
    tide.declare("path", Path("portable/value"))
    trace = tide.trace()

    first = trace.to_bundle().export(type="dict", kind="json")
    restored = TIDE.from_bundle(first)

    assert restored.to_bundle().export(type="dict", kind="json") == first


def test_bare_node_ready_prefix_remains_readiness_not_execution():
    @node(targets=["global:done"])
    def writer():
        WRITE_NOW("global:now", 1)
        return True

    tide = TIDE()
    tide.exec(writer)
    trace = tide.trace()
    ready_index = next(
        index
        for index, event in enumerate(trace.events)
        if event.type == ExecutorEvents.NODE_READY
    )
    rebuilt = _replay_log(
        trace.events[:ready_index + 1],
        trace.ocean_id,
    )

    assert not rebuilt.record_log
    assert not rebuilt.exec_log
    assert len(rebuilt.ready_attempts) == 1
    assert rebuilt.ready_attempts[0].outcome == "ready"


def test_pipeline_scope_is_complete_only_with_end_and_no_unresolved_attempts():
    @node(targets=["global:value"])
    def writer():
        return 1

    @pipeline
    def flow():
        return writer()

    trace = TIDE().ocean.run_pipeline(flow, mode="sequential")

    assert trace.pipeline_ended is True
    assert trace.unresolved_ready_attempts == ()
    assert trace.scope == "complete"
    assert "---- PIPELINE END ----" in trace.show(color=False)


def test_immediate_event_bundle_is_an_exact_fixed_point_after_first_export():
    @node(targets=["global:done"])
    def writer():
        WRITE_NOW("global:now", 1)
        return True

    tide = TIDE()
    tide.exec(writer)
    first = tide.trace().to_bundle().export(type="dict", kind="json")
    restored = TIDE.from_bundle(first)
    first_model = restored.to_model()
    second = restored.to_bundle().export(type="dict", kind="json")
    restored_again = TIDE.from_bundle(second)

    assert second == first
    assert restored_again.to_bundle().export(
        type="dict",
        kind="json",
    ) == first
    assert restored_again.to_model() == first_model
    assert [type(record).__name__ for record in restored.log] == [
        "ImmediateEffectRecord",
        "ExecRecord",
    ]


def test_native_bundle_contains_tiderun_and_no_event_sidecar_by_default():
    @node(targets=["global:value"])
    def writer():
        return 1

    tide = TIDE()
    tide.exec(writer)
    manifest = tide.trace().to_bundle().manifest

    assert set(manifest) == {
        "kind",
        "codec_version",
        "content",
        "run",
        "values",
        "events",
        "metadata",
    }
    assert manifest["content"] == "trace"
    assert manifest["run"]["kind"] == "tide_run"
    assert manifest["events"] is None
    assert "authority" not in manifest



def test_trace_events_detach_custom_hash_keys_after_population():
    from tide_tooling.tooling_core.engine.tide_records import Actor

    actor = Actor("ocean", "runner", "worker")
    event = Event(
        source="ocean",
        source_id=0,
        type=LogEvents.RUNNER_DEADLOCK,
        cause=Cause(description="hash key"),
        ocean_id="ocean",
        payload={actor: "recorded"},
    )

    trace = trace_from_event_log([event], ocean_id="ocean")
    event.payload[actor] = "changed"
    detached = trace.events[0]
    restored_actor = next(iter(detached.payload))

    assert restored_actor == actor
    assert restored_actor is not actor
    assert detached.payload[restored_actor] == "recorded"


def test_bundle_restores_trace_event_boundaries():
    @node(targets=["global:value"])
    def make_value():
        return 1

    @pipeline
    def flow():
        return make_value()

    source = TIDE().ocean.run_pipeline(flow, mode="sequential")
    root = source.trace_starting_event

    restored = TIDE.from_bundle(
        source.to_bundle(serialise_events=True)
    )

    assert restored.events_verified is True
    assert restored.trace_starting_event.event_id == root.event_id
    assert restored.pipeline_starting_event.event_id == root.event_id














def test_tide_event_effect_structure_is_observed_once_by_the_scheduler(
    monkeypatch,
):
    tide = TIDE()
    effect = WRITE("global:before", 1)
    original_coordinate = tide.ocean._coordinate_tide_event

    def mutate_before_scheduler_ownership(apply):
        effect.targets = ((DataRef("after", "global"), 2),)
        return original_coordinate(apply)

    monkeypatch.setattr(
        tide.ocean,
        "_coordinate_tide_event",
        mutate_before_scheduler_ownership,
    )
    tide.event("writer", effects=effect)
    trace = tide.trace()

    assert trace.value_of("global:after", 0) == 2
    assert trace.find("global:before").values == []
    assert [
        DataRef.to_string(target[0], prefix=False)
        for target in trace.log[0].declared_effects[0].targets
    ] == ["global:after"]


def test_bundle_events_independently_reconstruct_a_pipeline_log():
    @node(targets=["global:a"])
    def make_a():
        return 1

    @node(targets=["global:b"])
    def make_b(value):
        return value + 1

    @pipeline
    def chain():
        return make_b(make_a())

    source = TIDE().ocean.run_pipeline(chain, mode="sequential")
    restored = TIDE.from_bundle(
        source.to_bundle(
            serialise_events=True,
        ).export(type="dict", kind="json")
    )
    reconstructed = _replay_log(restored.events, restored.ocean_id)

    assert restored.events_verified is True
    assert [record.func_name for record in reconstructed.exec_log] == [
        "make_a",
        "make_b",
    ]


def test_runner_plan_event_captures_tide_plan_before_live_mutation(monkeypatch):
    @node(targets=["global:value"])
    def produce():
        return 1

    @pipeline
    def flow():
        return produce()

    tide = TIDE()
    original_create_trace = tide.ocean._trace_from_runner
    recorded = {}

    def mutate_live_plan_before_trace(runner):
        plan_event = next(
            event
            for event in tide.ocean.event_logger.events
            if event.type == LogEvents.RUNNER_PLAN
            and event.source_id == runner.id
        )
        recorded["plan"] = copy.deepcopy(plan_event.payload.tide_plan)
        runner._tide_plan["E"].add("not-recorded")
        return original_create_trace(runner)

    monkeypatch.setattr(
        tide.ocean,
        "_trace_from_runner",
        mutate_live_plan_before_trace,
    )

    trace = tide.ocean.run_pipeline(flow, mode="sequential")

    assert trace._tide_plan == recorded["plan"]
    assert "not-recorded" not in trace._tide_plan["E"]


def test_blocked_attempt_is_indexed_without_inventing_an_execution():
    blocked_id = "TIDE_EXEC:blocked"
    cause = Event(
        source="store",
        source_id=0,
        type=PipelineEvents.DEP_DELETED,
        cause=Cause(description="deleted"),
        ocean_id="ocean",
        payload=Reference(DataRef("missing", "global")),
    )
    blocked = Event(
        source="runner",
        source_id=0,
        type=LogEvents.NODE_BLOCKED,
        cause=Cause.from_event(cause),
        ocean_id="ocean",
        payload=NodeBlocked(
            DataRef("missing", "global"),
            ("args",),
            {},
        ),
        exec_id=blocked_id,
    )

    reconstructed = _replay_log((cause, blocked), "ocean")

    assert not reconstructed.exec_log
    assert reconstructed.blocked_exec_ids == {blocked_id}
    assert reconstructed.ready_exec_ids == set()


def test_bundle_event_values_preserve_recursive_containers():
    value = []
    value.append(value)
    tide = TIDE()
    tide.event("writer", writes={"cycle": value})

    restored = TIDE.from_bundle(
        tide.trace().to_bundle(serialise_events=True)
    )
    state = next(
        event.payload
        for event in restored.events
        if event.type == PipelineEvents.DEP_AVAILABLE
    )
    rebuilt = _replay_log(restored.events, restored.ocean_id)

    assert state.value[0] is state.value
    assert rebuilt.exec_log


def test_bundle_event_values_preserve_indirect_immutable_cycles():
    items = []
    value = (items,)
    items.append(value)
    tide = TIDE()
    tide.event("writer", writes={"cycle": value})

    restored = TIDE.from_bundle(
        tide.trace().to_bundle(serialise_events=True)
    )
    state = next(
        event.payload
        for event in restored.events
        if event.type == PipelineEvents.DEP_AVAILABLE
    )

    assert state.value[0][0] is state.value


def test_bundle_event_values_preserve_aliases():
    shared = []
    value = [shared, shared]
    tide = TIDE()
    tide.event("writer", writes={"alias": value})

    restored = TIDE.from_bundle(
        tide.trace().to_bundle(serialise_events=True)
    )
    state = next(
        event.payload
        for event in restored.events
        if event.type == PipelineEvents.DEP_AVAILABLE
    )

    assert state.value[0] is state.value[1]


def test_frozen_event_log_captures_each_event_occurrence_independently():
    shared = []
    first_ref = DataRef("first", "global")
    second_ref = DataRef("second", "global")
    events = (
        Event(
            source="store_router",
            source_id=0,
            type=PipelineEvents.DEP_AVAILABLE,
            cause=Cause(description="first"),
            ocean_id=0,
            payload=State(
                first_ref,
                shared,
                tide_snapshot_value(shared, boundary="write"),
            ),
        ),
        Event(
            source="store_router",
            source_id=0,
            type=PipelineEvents.DEP_AVAILABLE,
            cause=Cause(description="second"),
            ocean_id=0,
            payload=State(
                second_ref,
                shared,
                tide_snapshot_value(shared, boundary="write"),
            ),
        ),
    )

    restored = FrozenEventLog.capture(events).events

    assert restored[0].payload.value == restored[1].payload.value == []
    assert restored[0].payload.value is not restored[1].payload.value
    assert (
        restored[0].payload.snapshot.value_hash
        == restored[1].payload.snapshot.value_hash
    )


def test_frozen_event_log_owns_the_complete_event_graph():
    value = ["recorded"]
    event = Event(
        source="store_router",
        source_id=0,
        type=PipelineEvents.DEP_AVAILABLE,
        cause=Cause(description="write"),
        ocean_id=0,
        payload=State(
            DataRef("value", "global"),
            value,
            tide_snapshot_value(value, boundary="write"),
        ),
    )
    frozen = FrozenEventLog.capture((event,))
    value.append("later")
    captured = frozen.events[0]

    # EventOccurrence owns the publication-time history. Mutation of the
    # caller-owned runtime value after capture cannot rewrite that history.
    assert captured.payload.value == ["recorded"]
    assert captured.payload.snapshot.restore() == ["recorded"]


def test_trace_events_never_expose_retained_mutable_event_evidence():
    tide = TIDE()
    tide.event("writer", writes={"global:value": {"items": [1]}})
    trace = tide.trace()
    hash_before = trace.event_log.to_event_bundle().to_dict()["event_log_hash"]

    exposed = next(
        event
        for event in trace.events
        if event.type == PipelineEvents.DEP_AVAILABLE
        and event.payload.ref == DataRef("value", "global")
    )
    exposed.payload.value["items"].append(2)

    retained = next(
        event
        for event in trace.events
        if event.type == PipelineEvents.DEP_AVAILABLE
        and event.payload.ref == DataRef("value", "global")
    )
    hash_after = trace.event_log.to_event_bundle().to_dict()["event_log_hash"]

    assert retained.payload.value == {"items": [1]}
    assert trace.value_of("global:value", 0) == {"items": [1]}
    assert hash_after == hash_before


def test_frozen_event_log_rejects_self_deepcopy_mutable_aliases():
    value = _SelfDeepcopyMutable([1])
    event = Event(
        source="store_router",
        source_id=0,
        type=PipelineEvents.DEP_AVAILABLE,
        cause=Cause(description="write"),
        ocean_id=0,
        payload=State(
            DataRef("value", "global"),
            value,
            tide_snapshot_value(value, boundary="write"),
        ),
    )
    frozen = FrozenEventLog.capture((event,))
    hash_before = frozen.to_event_bundle().to_dict()["event_log_hash"]

    value.items.append(2)

    captured = frozen.events[0]
    hash_after = frozen.to_event_bundle().to_dict()["event_log_hash"]
    assert captured.payload.value is not value
    assert hash_after == hash_before


def test_frozen_event_log_detaches_manually_constructed_occurrences():
    value = {"items": [1]}
    event = Event(
        source="store_router",
        source_id=0,
        type=PipelineEvents.DEP_AVAILABLE,
        cause=Cause(description="write"),
        ocean_id=0,
        payload=State(
            DataRef("value", "global"),
            value,
            tide_snapshot_value(value, boundary="write"),
        ),
    )
    frozen = FrozenEventLog((EventOccurrence(event),))

    value["items"].append(2)

    assert frozen.events[0].payload.value == {"items": [1]}


def test_event_identity_memo_rejects_a_different_object():
    original = []
    replacement = []
    serialiser = TIDEEventSerialiser()
    serialiser.records = {}
    serialiser.next_object_index = 0
    serialiser.object_ids = {
        id(replacement): (original, "event_object_0"),
    }

    with pytest.raises(RuntimeError, match="identity collision"):
        serialiser._add(replacement)


def test_event_graph_preserves_fallback_semantics():
    event = Event(
        source="runner",
        source_id=0,
        type=LogEvents.EFFECT,
        cause=Cause(description="effect"),
        ocean_id=0,
        payload=EffectOccurrence(
            Effects.WRITE,
            ((DataRef("value", "global"), 1),),
            __fallback__(),
            "declared",
        ),
    )

    restored = deserialise_events(serialise_events((event,)))[0]

    assert isinstance(restored.payload.output, __fallback__)


def test_bundle_event_log_is_optional_sidecar_not_trace_authority():
    tide = TIDE()
    tide.event("writer", writes={"result": [1, 2, 3]})

    bundle = tide.trace().to_bundle(serialise_events=True)
    manifest = bundle.manifest
    events = FrozenEventLog.from_event_bundle(
        FrozenEventLogBundle.from_dict(manifest["events"]),
    ).events

    assert manifest["content"] == "trace"
    assert manifest["run"]["kind"] == "tide_run"
    assert manifest["events"] is not None
    assert "authority" not in manifest
    assert any(event.type == LogEvents.EVENT_EXEC for event in events)
    assert _replay_log(events, tide.ocean.id).exec_log

    restored = TIDE.from_bundle(bundle)
    assert restored.value_of("global:result", 0) == [1, 2, 3]


def test_event_deserialisation_rejects_a_mismatched_payload():
    ref = DataRef("value", "global")
    value = "value"
    event = Event(
        source="store_router",
        source_id=0,
        type=PipelineEvents.DEP_DELETED,
        cause=Cause(description="write"),
        ocean_id=0,
        payload=State(
            ref,
            value,
            tide_snapshot_value(value, boundary="write"),
        ),
    )
    record = serialise_events((event,))

    with pytest.raises(TypeError, match="requires Reference"):
        deserialise_events(record)


def test_event_json_rejects_duplicate_equivalent_set_values():
    event = Event(
        source="store_router",
        source_id=0,
        type=PipelineEvents.DEP_DELETED,
        cause=Cause(description="delete"),
        ocean_id=0,
        payload=DataRef("value", "global"),
    )
    record = serialise_events((event,))
    first_record = next(iter(record["records"].values()))
    first_record["hostile"] = {
        "$tide_event_json_type": "set",
        "items": [True, 1],
    }

    with pytest.raises(ValueError, match="duplicate-equivalent"):
        deserialise_events(record)


def test_event_json_enforces_depth_and_byte_limits():
    event = Event(
        source="store_router",
        source_id=0,
        type=PipelineEvents.DEP_DELETED,
        cause=Cause(description="delete"),
        ocean_id=0,
        payload=DataRef("value", "global"),
    )

    deep_record = serialise_events((event,))
    first_record = next(iter(deep_record["records"].values()))
    first_record["hostile"] = [[[[["too deep"]]]]]
    with pytest.raises(ValueError, match="maximum depth"):
        deserialise_events(deep_record, max_depth=4)

    bytes_record = serialise_events((event,))
    first_record = next(iter(bytes_record["records"].values()))
    first_record["hostile"] = {
        "$tide_event_json_type": "bytes",
        "base64": "YWJj",
    }
    with pytest.raises(ValueError, match="maximum size"):
        deserialise_events(bytes_record, max_bytes=2)

    aggregate_record = serialise_events((event,))
    first_record = next(iter(aggregate_record["records"].values()))
    first_record["hostile"] = [
        {
            "$tide_event_json_type": "bytes",
            "base64": "YWI=",
        },
        {
            "$tide_event_json_type": "bytes",
            "base64": "Y2Q=",
        },
    ]
    with pytest.raises(ValueError, match="maximum size"):
        deserialise_events(aggregate_record, max_bytes=3)
