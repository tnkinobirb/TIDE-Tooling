from types import SimpleNamespace

import pytest

from tide_tooling import TIDE, node, pipeline
from tide_tooling.tooling_core.engine.classes import DataRef, Node
from tide_tooling.tooling_core.engine.index import (
    PipelineIndex,
    _project_pipeline_index_to_tide_plan,
)
from tide_tooling.tooling_core.engine.tide_logging import Trace
from tide_tooling.tooling_core.engine.tide_records import (
    Actor,
    ActorOccurrence,
    ExecRecord,
)


def _record(seq_id, definition_id, actor, occurrence_id, *, deps=()):
    record = ExecRecord(
        seq_id=seq_id,
        exec_id=f"exec-{seq_id}",
        ocean_id=actor.source_ocean,
        runner_id=actor.source_runner,
        pipeline_id=None,
        node_id=seq_id,
        func=None,
        func_name=actor.source_actor,
        inputs={},
        deps=list(deps),
        effects=[],
        declared_effects=[],
        declared_targets=[],
        runtime_effects=[],
        runtime_targets=[],
        declared_deps=[],
        unscoped_store_refs={},
        status="success",
        started_at=None,
        finished_at=None,
        duration_ms=None,
        error=None,
        node_options={"preserve_exec_id": True},
        writes=[],
        emitted_nodes=[],
        base_node_id=definition_id,
    )
    record.producer_occurrence_id = ActorOccurrence(actor, occurrence_id)
    return record


def _plan_for(definitions, runner_id="runner"):
    index = PipelineIndex()
    for definition in definitions:
        index.node_index.index_node(definition)
    return _project_pipeline_index_to_tide_plan(
        index,
        runner_id=runner_id,
    )


def test_trace_plan_uses_exact_definition_occurrences_and_seq_order():
    def produce():
        return 1

    first = Node(produce, targets="global:value")
    second = Node(produce, targets="global:value")
    assert first.content_id == second.content_id

    actor = Actor("ocean", "runner", "produce")
    records = [
        _record(0, first.id, actor, 0),
        _record(1, first.id, actor, 1),
        _record(2, second.id, actor, 2),
    ]
    trace = Trace(
        log=records,
        ocean_id="ocean",
        tide_plan=_plan_for([first, second]),
    )

    assert list(trace.index.exec_ids_by_definition_id[first.id]) == [
        "exec-0",
        "exec-1",
    ]
    assert list(trace.index.exec_ids_by_definition_id[second.id]) == [
        "exec-2",
    ]
    assert trace.plan == {
        "E": ["produce[0]", "produce[1]", "produce[2]"],
        "S": [
            "global:value[v0]",
            "global:value[v1]",
            "global:value[v2]",
        ],
        "I": [],
        "O": [
            {"exec_id": "produce[0]", "state_id": "global:value[v0]"},
            {"exec_id": "produce[1]", "state_id": "global:value[v1]"},
            {"exec_id": "produce[2]", "state_id": "global:value[v2]"},
        ],
    }


def test_trace_plan_one_shot_keeps_only_first_runtime_occurrence():
    definition = Node(lambda: 1, targets="global:value", one_shot=True)
    actor = Actor("ocean", "runner", "<lambda>")
    trace = Trace(
        log=[
            _record(0, definition.id, actor, 0),
            _record(1, definition.id, actor, 1),
        ],
        ocean_id="ocean",
        tide_plan=_plan_for([definition]),
    )

    assert trace.plan["E"] == ["<lambda>[0]"]
    assert trace.plan["S"] == ["global:value[v0]"]


def test_trace_plan_appends_missing_occurrence_with_next_actor_id():
    def shared():
        return 1

    observed = Node(shared)
    missing = Node(shared, targets="global:expected")
    actor = Actor("ocean", "runner", "shared")
    trace = Trace(
        log=[_record(0, observed.id, actor, 4)],
        ocean_id="ocean",
        tide_plan=_plan_for([observed, missing]),
    )

    assert trace.plan["E"] == ["shared[4]", "shared[5]"]
    assert trace.plan["O"] == [{
        "exec_id": "shared[5]",
        "state_id": "global:expected[v0]",
    }]


def test_trace_plan_producerless_input_starts_at_v0():
    definition = Node(
        lambda value: value,
        args=("@global:value",),
        targets="global:value",
    )
    index = PipelineIndex()
    runtime_node = index.node_index.index_node(definition)
    runtime_node.deps.append(DataRef("value", "global"))
    plan = _project_pipeline_index_to_tide_plan(index, runner_id="runner")
    actor = Actor("ocean", "runner", "<lambda>")
    trace = Trace(
        log=[_record(0, definition.id, actor, 0)],
        ocean_id="ocean",
        tide_plan=plan,
    )

    assert trace.plan["S"] == ["global:value[v0]", "global:value[v1]"]
    assert trace.plan["I"] == [{
        "exec_id": "<lambda>[0]",
        "state_id": "global:value[v0]",
    }]
    assert trace.plan["O"] == [
        {
            "exec_id": "TIDE: External[0]",
            "state_id": "global:value[v0]",
        },
        {
            "exec_id": "<lambda>[0]",
            "state_id": "global:value[v1]",
        },
    ]
    assert TIDE.report_model_validity(trace.plan).semantically_valid


def test_trace_plan_replaces_dynamic_selector_per_occurrence():
    selector = DataRef("item*", "global")
    definition = Node(lambda values: values)
    index = PipelineIndex()
    runtime_node = index.node_index.index_node(definition)
    runtime_node.deps.append(selector)
    plan = _project_pipeline_index_to_tide_plan(index, runner_id="runner")
    actor = Actor("ocean", "runner", "<lambda>")
    dynamic = SimpleNamespace(
        type="dynamic",
        dep=selector,
        expanded_to=[
            SimpleNamespace(
                unscoped_reference=DataRef("item_a", "global"),
                resolved_version=0,
                resolved_value="a",
                producer_exec_id=None,
            ),
            SimpleNamespace(
                unscoped_reference=DataRef("item_b", "global"),
                resolved_version=0,
                resolved_value="b",
                producer_exec_id=None,
            ),
        ],
    )
    trace = Trace(
        log=[_record(0, definition.id, actor, 0, deps=[dynamic])],
        ocean_id="ocean",
        tide_plan=plan,
    )

    assert trace.plan["S"] == [
        "global:item_a[v0]",
        "global:item_b[v0]",
    ]
    assert all("item*" not in state for state in trace.plan["S"])


def test_missing_dynamic_occurrence_has_an_empty_expansion():
    selector = DataRef("item*", "global")
    definition = Node(lambda values: values)
    index = PipelineIndex()
    runtime_node = index.node_index.index_node(definition)
    runtime_node.deps.append(selector)
    trace = Trace(
        log=[],
        ocean_id="ocean",
        tide_plan=_project_pipeline_index_to_tide_plan(
            index,
            runner_id="runner",
        ),
    )

    assert trace.plan["E"] == ["<lambda>[0]"]
    assert trace.plan["S"] == []
    assert trace.plan["I"] == []


def test_undeclared_runtime_occurrence_is_not_added_to_the_plan():
    expected = Node(lambda: 1, targets="global:expected")
    actor = Actor("ocean", "runner", "external")
    trace = Trace(
        log=[_record(0, "not-declared", actor, 0)],
        ocean_id="ocean",
        tide_plan=_plan_for([expected]),
    )

    assert trace.to_model(enrich=False)["E"] == ["external[0]"]
    assert trace.plan["E"] == ["<lambda>[0]"]


def test_undeclared_runtime_occurrence_does_not_shift_the_plan_numbering():
    def shared():
        return 1

    expected = Node(shared, targets="global:expected")
    actor = Actor("ocean", "runner", "shared")
    trace = Trace(
        log=[_record(0, "not-declared", actor, 0)],
        ocean_id="ocean",
        tide_plan=_plan_for([expected]),
    )

    assert trace.to_model(enrich=False)["E"] == ["shared[0]"]
    assert trace.plan["E"] == ["shared[0]"]
    assert trace.plan["O"] == [{
        "exec_id": "shared[0]",
        "state_id": "global:expected[v0]",
    }]


def test_trace_plan_is_derived_and_bundle_round_trips_blueprint():
    @node(targets="global:value")
    def produce():
        return 1

    @pipeline
    def flow():
        produce()

    trace = TIDE().ocean.run_pipeline(flow, mode="sequential")
    original_plan = trace.plan
    returned = trace.plan
    returned["E"].append("mutated[0]")

    assert trace.plan == original_plan

    bundle = trace.to_bundle()
    run_trace = bundle.manifest["run"]["trace"]
    assert "tide_plan" in run_trace
    assert "plan" not in run_trace

    restored = TIDE.from_bundle(bundle)

    assert restored._tide_plan == trace._tide_plan
    assert restored.plan == original_plan
    with pytest.raises(AttributeError, match="sealed Trace evidence"):
        restored.log[0].base_node_id = "changed-after-load"
    assert restored.plan == original_plan


def test_bundle_round_trips_exact_dynamic_and_literal_plan_blueprint():
    @node(targets=DataRef("csv", "mem", literal=True))
    def produce():
        return 1

    @node(args=("@mem:csv:*",))
    def consume(values):
        return values

    @pipeline
    def flow():
        produce()
        consume()

    trace = TIDE().ocean.run_pipeline(flow, mode="sequential")
    restored = TIDE.from_bundle(trace.to_bundle())

    assert restored._tide_plan == trace._tide_plan
    assert restored.plan == trace.plan


