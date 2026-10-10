from types import SimpleNamespace

import pytest

from tide_tooling.tooling_core.engine.classes import DataRef
from tide_tooling.tooling_core.engine.event_payloads import (
    PipelineRun,
    Reference,
    State,
    validate_event_payload,
)
from tide_tooling.tooling_core.engine.event_readers import (
    EventLogReader,
    read_node_ready,
)
from tide_tooling.tooling_core.engine.event_occurrences import validate_event
from tide_tooling.tooling_core.engine.event_occurrences import FrozenEventLog
from tide_tooling.tooling_core.engine.events import Cause, Event, OceanEvents, PipelineEvents, RouterEvents
from tide_tooling.tooling_core.engine.tide_logging import Trace
from tide_tooling.tooling_core.engine.tide_records import DependencyUseRecord, InputUseRecord, Inputs
from tide_tooling.tooling_core.engine.tide_runtime_logging import ConstructionState, ExecutionLog, snapshot_lineage, trace_from_event_log
from tide_tooling.tooling_core.engine.utils import (
    TIDEValueSnapshot,
    tide_snapshot_value,
    tide_value_hash_from_bundle,
    tide_value_hasher,
)


class StatefulSnapshotValue:
    observations = 0

    def __init__(self, observation=None):
        self.observation = observation

    def __getstate__(self):
        type(self).observations += 1
        return {"observation": type(self).observations}

    def __setstate__(self, state):
        self.observation = state["observation"]


class StatefulUnorderedValue(StatefulSnapshotValue):
    pass


class TransformingStateValue:
    def __getstate__(self):
        return {"value": 1}

    def __setstate__(self, state):
        self.value = 999


def _roundtrip_snapshot(snapshot, value):
    event = Event(
        source="store_router",
        source_id=0,
        type=PipelineEvents.DEP_AVAILABLE,
        cause=Cause(description="snapshot"),
        ocean_id=0,
        payload=State(DataRef("value", "global"), value, snapshot),
    )
    frozen = FrozenEventLog.capture((event,))
    restored = FrozenEventLog.from_event_bundle(frozen.to_event_bundle())
    return restored.events[0].payload.snapshot


def test_snapshot_restore_isolates_exact_historical_value():
    snapshot = tide_snapshot_value({"items": [1]}, boundary="write")

    restored = snapshot.restore()
    restored["items"].append(2)

    assert snapshot.restore() == {"items": [1]}


def test_snapshot_deepcopy_fallback_never_exposes_retained_value():
    value = {"items": [1]}
    snapshot = TIDEValueSnapshot(
        value_hash=tide_value_hasher(value),
        value=value,
        capture_method="deepcopy",
        type_name="dict",
        exact=True,
    )

    exposed = snapshot.value
    exposed["items"].append(2)

    assert snapshot.value == {"items": [1]}
    assert snapshot.restore() == {"items": [1]}


def test_persisted_deepcopy_snapshot_retains_provenance_not_exact_value():
    value = {"items": [1]}
    value_hash = tide_value_hasher(value)
    snapshot = TIDEValueSnapshot(
        value_hash=value_hash,
        value=value,
        capture_method="deepcopy",
        type_name="dict",
        exact=True,
    )

    restored = _roundtrip_snapshot(snapshot, value)

    assert restored.value_hash == value_hash
    assert restored.capture_method == "deepcopy"
    assert restored.exact is False
    assert restored.value is None
    assert restored.restore().value_hash == value_hash


def test_persisted_deepcopy_snapshot_cannot_claim_missing_exact_value():
    with pytest.raises(
        ValueError,
        match="exactness must match retained evidence",
    ):
        TIDEValueSnapshot(
            value_hash=tide_value_hasher({"items": [1]}),
            capture_method="deepcopy",
            type_name="dict",
            exact=True,
        )


def test_serialised_exact_snapshot_remains_exact_after_roundtrip():
    value = {"items": [1]}
    snapshot = tide_snapshot_value(value, boundary="write")

    restored = _roundtrip_snapshot(snapshot, value)

    assert restored.capture_method == "serialised"
    assert restored.exact is True
    assert restored.value_hash == snapshot.value_hash
    assert restored.restore() == value


def test_snapshot_hash_and_restored_value_share_one_serialised_observation():
    StatefulSnapshotValue.observations = 0

    snapshot = tide_snapshot_value(
        StatefulSnapshotValue(),
        boundary="write",
    )

    assert StatefulSnapshotValue.observations == 1
    assert snapshot.value_hash == tide_value_hash_from_bundle(
        snapshot.value_bundle
    )
    assert snapshot.restore().observation == 1


def test_unordered_object_state_is_observed_once_for_order_and_storage():
    StatefulUnorderedValue.observations = 0

    snapshot = tide_snapshot_value(
        {StatefulUnorderedValue()},
        boundary="write",
    )

    assert StatefulUnorderedValue.observations == 1
    restored = snapshot.restore()
    assert next(iter(restored)).observation == 1


def test_snapshot_does_not_claim_transforming_reconstruction_is_exact():
    snapshot = tide_snapshot_value(
        TransformingStateValue(),
        boundary="write",
    )

    assert snapshot.exact is False
    assert snapshot.capture_method == "hash"
    assert snapshot.restore().value_hash == snapshot.value_hash


def test_snapshot_lineage_keeps_only_snapshots_in_order():
    first = tide_snapshot_value("first", boundary="result")
    second = tide_snapshot_value("second", boundary="write")

    assert snapshot_lineage(first, None, second) == [first.to_dict(), second.to_dict()]


def test_construction_state_owns_independent_reconstruction_maps():
    first = ConstructionState(exec_log=object())
    second = ConstructionState(exec_log=object())

    first.events_by_id["event"] = object()
    first.ready_inputs_by_event_id["ready"] = (
        "execution",
        object(),
        [],
    )
    first.ready_exec_ids.add("execution")

    assert second.events_by_id == {}
    assert second.ready_inputs_by_event_id == {}
    assert second.ready_exec_ids == set()
    assert first.exec_log is not second.exec_log


def test_hash_only_snapshot_restore_remains_explicitly_opaque():
    value_hash = tide_value_hasher("value")
    snapshot = TIDEValueSnapshot(
        value_hash=value_hash,
        representation="value",
    )

    restored = snapshot.restore()

    assert restored.value_hash == value_hash
    assert restored.representation == "value"


def test_restore_execution_evidence_does_not_share_mutable_values():
    evidence = {"items": [1]}

    restored = ExecutionLog._restore_execution_evidence(evidence)
    restored["items"].append(2)

    assert evidence == {"items": [1]}


def test_append_runtime_effect_reads_attaches_one_shared_dependency():
    execution_log = ExecutionLog("ocean")
    consumer = SimpleNamespace(inputs=Inputs({}), deps=[])
    producer = SimpleNamespace(runner_id="runner", producer_id="actor")
    execution_log.exec_id_to_record = {"consumer": consumer, "producer": producer}

    execution_log.append_runtime_effect_reads("consumer", "global:key", "value", 2, "producer")

    dependency = consumer.inputs.runtime_read.dependencies[0]
    assert consumer.deps == [dependency]
    assert dependency.producer_exec_id == "producer"
    assert dependency.resolved_version == 2


def test_append_runtime_reads_applies_exact_observed_state():
    execution_log = ExecutionLog("ocean")
    producer = SimpleNamespace(runner_id="runner", producer_id="actor")
    execution_log.exec_id_to_record = {"producer": producer}
    input_record = InputUseRecord("args", (), (), (), (), [])
    dependency = DependencyUseRecord(
        associated_input=("args", input_record),
        dep=DataRef(key="key", store="global"),
        type="static",
        unscoped_ref="global:key",
    )
    input_record.dependencies = [dependency]
    inputs = Inputs({"args": input_record})
    dependencies = [dependency]

    execution_log.append_runtime_reads(inputs, dependencies, [{
        "input_name": "args",
        "reference": "global:key",
        "value": "observed",
        "version": 3,
        "producer_exec_id": "producer",
        "input_path": (0,),
    }])

    assert dependency.resolved_value == "observed"
    assert dependency.resolved_version == 3
    assert dependency.producer_exec_id == "producer"
    assert dependency.input_path == (0,)


def test_assign_external_identity_sets_one_actor_identity():
    execution_log = ExecutionLog("ocean")
    record = SimpleNamespace()

    execution_log._assign_external_identity(record, "external", "component")

    assert record.runner_id == "external"
    assert record.node_id == "component"
    assert record.producer_id.id == ("ocean", "external", "component")


def test_node_ready_reader_holds_inputs_until_result():
    inputs = object()
    dependencies = []
    execution_log = SimpleNamespace(
        ocean_id="ocean",
        create_input_log=lambda snapshot: (inputs, dependencies),
        append_runtime_reads=lambda inputs, dependencies, observations: None,
    )
    state = ConstructionState(exec_log=execution_log)
    event = SimpleNamespace(
        event_id="ready-event",
        exec_id="execution",
        payload=SimpleNamespace(node_snapshot={
            "reads": [],
            "runner_id": "runner",
            "pipeline_id": "pipeline",
            "func_name": "worker",
            "base_node_id": "definition",
        }),
    )

    read_node_ready(event, state)

    assert state.ready_inputs_by_event_id["ready-event"] == (
        "execution",
        inputs,
        dependencies,
    )
    assert state.ready_exec_ids == {"execution"}
    attempt = state.ready_attempt_by_exec_id["execution"]
    assert attempt.actor.id == ("ocean", "runner", "worker")
    assert attempt.node_definition_id == "definition"


def test_event_log_reader_validates_the_frozen_payload_type():
    valid = SimpleNamespace(
        type=PipelineEvents.BUILD,
        payload=Reference(DataRef(key="key", store="mem")),
    )
    invalid = SimpleNamespace(type=PipelineEvents.BUILD, payload=("mem", "key"))

    validate_event_payload(valid.type, valid.payload)

    try:
        validate_event_payload(invalid.type, invalid.payload)
    except TypeError as error:
        assert "requires Reference" in str(error)
    else:
        raise AssertionError("mismatched payload was accepted")


def test_event_log_reader_rejects_duplicate_occurrence_identity():
    event = Event(
        ocean_id="ocean",
        source="test",
        source_id="test",
        type=PipelineEvents.BUILD,
        cause=Cause(description="duplicate occurrence probe"),
        payload=Reference(DataRef(key="key", store="mem")),
    )

    try:
        EventLogReader("ocean")._construct_log([event, event])
    except ValueError as error:
        assert "Duplicate Event occurrence" in str(error)
    else:
        raise AssertionError("duplicate Event occurrence was accepted")


def test_missing_causal_event_is_reported_without_changing_construction(capsys):
    event = Event(
        source="business",
        source_id="test",
        type=RouterEvents.STORE_WRITE,
        cause=Cause("missing-event", "external write"),
        ocean_id="ocean",
        payload=State(
            DataRef(key="key", store="global"),
            "value",
            tide_snapshot_value("value", boundary="write"),
        ),
    )
    reader = EventLogReader("ocean")

    state = reader._construct_log([event])

    assert state.missing_referenced_events == {
        "missing-event": [event.event_id],
    }
    assert state.exec_log.exec_log == []

    trace = trace_from_event_log(
        [event],
        ocean_id="ocean",
    )
    shown = trace.show(color=False)
    capsys.readouterr()

    assert "CORRUPTED: 1 missing causal Events" in shown
    assert trace.corruption == {
        "missing_causal_events": {"missing-event": (event.event_id,)},
        "causal_order_violations": {},
        "evidence_violations": {},
    }


def test_later_store_write_cause_is_reported_as_causal_order_violation(capsys):
    ref = DataRef(key="key", store="global")
    snapshot = tide_snapshot_value("value", boundary="write")
    store_write = Event(
        source="business",
        source_id="writer",
        type=RouterEvents.STORE_WRITE,
        cause=Cause(description="write command"),
        ocean_id="ocean",
        payload=State(ref, "value", snapshot),
    )
    dep_available = Event(
        source="runner",
        source_id="reader",
        type=PipelineEvents.DEP_AVAILABLE,
        cause=Cause(store_write.event_id, str(RouterEvents.STORE_WRITE)),
        ocean_id="ocean",
        payload=State(ref, "value", snapshot),
    )
    reader = EventLogReader("ocean")

    state = reader._construct_log([dep_available, store_write])

    assert state.missing_referenced_events == {}
    assert state.causal_order_violations == {
        store_write.event_id: [dep_available.event_id],
    }
    # The forward fold cannot borrow identity from a causal Event that has
    # not happened yet. The availability occurrence remains the external
    # State authority even though corruption analysis later reports the
    # out-of-order cause.
    assert len(state.exec_log.exec_log) == 1
    assert state.exec_log.exec_log[0].exec_id == (
        f"TIDE_EXEC:{dep_available.event_id}"
    )

    trace = trace_from_event_log(
        [dep_available, store_write],
        ocean_id="ocean",
    )
    shown = trace.show(color=False)
    capsys.readouterr()

    assert trace.corruption == {
        "missing_causal_events": {},
        "causal_order_violations": {
            store_write.event_id: (dep_available.event_id,),
        },
        "evidence_violations": {},
    }
    assert "CORRUPTED: 1 causal ordering violations" in shown


def test_trace_header_reports_each_missing_causal_event_in_red(capsys):
    missing = {
        "missing-one": ("event-one", "event-two"),
        "missing-two": ("event-three",),
    }
    trace = Trace(
        log=[],
        ocean_id="ocean",
        missing_referenced_events=missing,
    )

    shown = trace.show(color=True)
    capsys.readouterr()

    assert "\x1b[31mCORRUPTED: 2 missing causal Events\x1b[0m" in shown


def test_unchanged_polling_reuses_the_recorded_state():
    ref = DataRef(key="key", store="global")
    snapshot = tide_snapshot_value("value", boundary="write")
    write = Event(source="business", source_id="test", type=RouterEvents.STORE_WRITE, cause=Cause(description="external write"), ocean_id="ocean", payload=State(ref, "value", snapshot))
    start = Event(source="ocean", source_id=0, type=OceanEvents.RUN_PIPELINE, cause=Cause(description="trace start"), ocean_id="ocean", payload=PipelineRun(0, None, "sequential", {}, False, None, None))
    poll = Event(source="runner", source_id=0, type=PipelineEvents.DEP_AVAILABLE, cause=Cause.from_event(start), ocean_id="ocean", payload=State(ref, "value", snapshot))

    reader = EventLogReader("ocean")
    state = reader._construct_log([write, start, poll])

    assert state.versioned_references == {"global:key": 0}
    assert len(state.exec_log.exec_log) == 1


def test_store_write_command_alone_does_not_claim_success():
    ref = DataRef(key="key", store="global")
    snapshot = tide_snapshot_value("value", boundary="write")
    write = Event(source="business", source_id="test", type=RouterEvents.STORE_WRITE, cause=Cause(description="external write"), ocean_id="ocean", payload=State(ref, "value", snapshot))

    reader = EventLogReader("ocean")
    state = reader._construct_log([write])

    assert state.versioned_references == {}
    assert state.exec_log.exec_log == []


def test_store_write_consequence_confirms_one_write():
    ref = DataRef(key="key", store="global")
    snapshot = tide_snapshot_value("value", boundary="write")
    write = Event(source="business", source_id="test", type=RouterEvents.STORE_WRITE, cause=Cause(description="external write"), ocean_id="ocean", payload=State(ref, "value", snapshot))
    available = Event(source="store_router", source_id=0, type=PipelineEvents.DEP_AVAILABLE, cause=Cause.from_event(write), ocean_id="ocean", target_runner_id="*", payload=write.payload)

    reader = EventLogReader("ocean")
    state = reader._construct_log([write, available])

    assert state.versioned_references == {"global:key": 0}
    assert len(state.exec_log.exec_log) == 1
    assert len(state.exec_log.exec_log[0].writes) == 1
    assert state.exec_log.exec_log[0].writes[0].cause == write.cause
