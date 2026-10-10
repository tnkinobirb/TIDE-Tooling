"""The declared TIDE plan is projected from the pipeline index, not Trace."""

from types import SimpleNamespace

from tide_tooling.tooling_core.engine.classes import (
    DataRef,
    Node,
    __fallback__,
)
from tide_tooling.tooling_core.engine.index import (
    PipelineIndex,
    _project_pipeline_index_to_tide_plan,
)
from tide_tooling.tooling_core.engine.tide_logging import Trace
from tide_tooling.tooling_core.engine.tide_records import (
    Actor,
    ActorOccurrence,
)


def test_plan_projects_declared_executions_states_and_incidence():
    index = PipelineIndex()
    load = index.node_index.index_node(Node(lambda: "raw", targets="global:raw"))
    report = index.node_index.index_node(Node(lambda raw: raw.upper(), targets="global:report"))
    report.deps.append(DataRef("raw", "global"))

    plan = _project_pipeline_index_to_tide_plan(index)

    assert {name: plan[name] for name in ("E", "S", "I", "O", "D")} == {
        "E": {load.node.id, report.node.id},
        "S": {DataRef("raw", "global"), DataRef("report", "global")},
        "I": {(report.node.id, DataRef("raw", "global"))},
        "O": {
            (load.node.id, DataRef("raw", "global")),
            (report.node.id, DataRef("report", "global")),
        },
        "D": {},
    }
    assert plan["nodes"] == [
        {
            "definition_id": load.node.id,
            "runner_id": None,
            "actor_name": "<lambda>",
            "one_shot": False,
            "dependencies": [],
            "targets": [DataRef("raw", "global")],
        },
        {
            "definition_id": report.node.id,
            "runner_id": None,
            "actor_name": "<lambda>",
            "one_shot": False,
            "dependencies": [{
                "declared": DataRef("raw", "global"),
                "reference": DataRef("raw", "global"),
                "dynamic": False,
            }],
            "targets": [DataRef("report", "global")],
        },
    ]


def test_dynamic_plan_distinguishes_no_plan_from_explicit_empty_expansion():
    index = PipelineIndex()
    node = index.node_index.index_node(Node(lambda rows: rows))
    selector = DataRef("csv_*", "mem")
    node.deps.append(selector)

    absent = _project_pipeline_index_to_tide_plan(index)
    empty = _project_pipeline_index_to_tide_plan(index, {selector: ()})

    assert absent["D"] == {selector: __fallback__()}
    assert empty["D"] == {selector: ()}
    assert absent["I"] == empty["I"] == {(node.node.id, selector)}


def test_dynamic_plan_retains_the_supplied_static_expansion():
    index = PipelineIndex()
    node = index.node_index.index_node(Node(lambda rows: rows))
    selector = DataRef("csv_*", "mem")
    expansion = (DataRef("csv_a", "mem"), DataRef("csv_b", "mem"))
    node.deps.append(selector)

    plan = _project_pipeline_index_to_tide_plan(index, {selector: expansion})

    assert plan["D"] == {selector: expansion}
    assert plan["S"] == {selector}


def test_materialised_plan_counts_only_projected_actor_occurrences():
    external = Actor(
        "ocean",
        "TIDE: External",
        "TIDE: External",
    )
    planned = Actor("ocean", "runner", "consume")

    def record(seq_id, actor, occurrence_id, definition_id=None, deps=()):
        return SimpleNamespace(
            seq_id=seq_id,
            exec_id=f"exec-{seq_id}",
            producer_id=actor,
            source_ocean_id=actor.source_ocean,
            producer_occurrence_id=ActorOccurrence(
                actor,
                occurrence_id,
            ),
            runner_id=actor.source_runner,
            node_id=seq_id,
            func_name=actor.source_actor,
            base_node_id=definition_id,
            deps=list(deps),
            writes=[],
            effects=[],
        )

    reference = DataRef("value", "global")
    trace = Trace(
        log=[
            record(0, external, 0),
            record(1, external, 1),
            record(2, planned, 0, "consume-definition"),
        ],
        ocean_id="ocean",
        tide_plan={
            "nodes": [{
                "definition_id": "consume-definition",
                "runner_id": "runner",
                "actor_name": "consume",
                "one_shot": False,
                "dependencies": [{
                    "declared": reference,
                    "reference": reference,
                    "dynamic": False,
                }],
                "targets": [],
            }],
        },
    )

    assert trace.plan["E"] == ["consume[0]", "TIDE: External[0]"]
