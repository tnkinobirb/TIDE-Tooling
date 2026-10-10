"""Power-user investigations over one deliberately difficult real workload.

These tests complement the narrow query unit tests.  Their purpose is to prove
that a professional can ask several independent public APIs about the same
execution and receive one coherent account, before and after repeated Bundle
round-trips.
"""

from __future__ import annotations

import copy
import json

import pytest

from tide_tooling import DELETE_NOW, READ, Effects, Ocean, TIDE, node, pipeline

from testing.test_real_world_trust_test import (
    DYNAMIC_SELECTOR,
    _TruthLedger,
    _build_workload,
)


def _execution_ids(trace):
    return {
        record.func_name: record.exec_id
        for record in trace.execs.values()
    }


def _execution_names(trace, pathed):
    names = {
        record.exec_id: record.func_name
        for record in trace.execs.values()
    }
    return {
        tuple(
            names[exec_id]
            for exec_id in path["executions"]
        )
        for paths in pathed.paths.values()
        for path in paths
    }


def _pair(entry):
    if isinstance(entry, dict):
        return entry["exec_id"], entry["state_id"]
    return tuple(entry)


def _model_execution_id(model, name):
    matches = [
        identity
        for identity, metadata in model["executions"].items()
        if metadata["actor"]["name"] == name
    ]
    assert len(matches) == 1
    return matches[0]


def _model_state_id(model, reference, version):
    expected = f"{reference}[v{version}]"
    matches = [identity for identity in model["S"] if identity == expected]
    assert len(matches) == 1
    return matches[0]


@pytest.fixture(scope="module")
def heavy_power_user_real_world(tmp_path_factory):
    root = tmp_path_factory.mktemp("heavy_power_user_real_world")
    ledger = _TruthLedger()
    workload, _ = _build_workload(root, ledger)
    ocean = Ocean()
    try:
        live = ocean.run_pipeline(
            workload,
            mode="sequential",
            dynamic_ref_plan={
                DYNAMIC_SELECTOR: ["global:batch_a", "global:batch_b"],
            },
        )
    finally:
        ocean.close()

    restored = live
    for _ in range(3):
        restored = TIDE.from_bundle(restored.to_bundle().to_dict())

    return live, restored


def _query_surface(trace):
    ids = _execution_ids(trace)
    merged_v0 = trace.inspect(
        "global:merged",
        "reference",
        version=0,
        grouped=False,
    )
    return {
        "find": trace.find("merged").to_dict(),
        "inspect": merged_v0.to_dict(),
        "investigate": trace.investigate(merged_v0).to_dict(),
        "produced_by": trace.produced_by(merged_v0, group=False).to_dict(),
        "consumed_by": trace.consumed_by(merged_v0, group=False).to_dict(),
        "downstream": trace.downstream(merged_v0).to_dict(),
        "upstream": trace.upstream(
            ids["export"],
            "exec_record",
            grouped=False,
        ).to_dict(),
        "path": trace.path(
            ids["load_a"],
            ids["export"],
            "exec_record",
            "exec_record",
        ).to_dict(),
        "inputs_of": trace.inputs_of(ids["aggregate"]).to_dict(),
        "output_of": trace.output_of(ids["aggregate"]).to_dict(),
        "effects": trace.investigate(Effects.DELETE, "effect").to_dict(),
    }


def test_heavy_power_user_all_query_surfaces_survive_three_bundle_generations(
    heavy_power_user_real_world,
):
    live, restored = heavy_power_user_real_world

    live_answers = _query_surface(live)
    restored_answers = _query_surface(restored)

    assert restored_answers == live_answers
    # The complete public result surface is suitable for an API/reporting
    # boundary rather than only for in-process object inspection.
    json.dumps(restored_answers, sort_keys=True)


@pytest.mark.parametrize("which", [0, 1])
def test_heavy_power_user_equal_values_keep_exact_versioned_lineage(
    heavy_power_user_real_world,
    which,
):
    live, restored = heavy_power_user_real_world
    trace = (live, restored)[which]
    ids = _execution_ids(trace)

    first = trace.inspect(
        "global:merged", "reference", version=0, grouped=False
    )
    second = trace.inspect(
        "global:merged", "reference", version=1, grouped=False
    )

    assert trace.value_of(first) == trace.value_of(second)
    assert trace.value_of(first) is not trace.value_of(second)
    assert trace.produced_by(first, group=False).execs == [ids["merge"]]
    assert trace.consumed_by(first, group=False).execs == [
        ids["repeat_equal"]
    ]
    assert trace.produced_by(second, group=False).execs == [
        ids["repeat_equal"]
    ]
    assert trace.consumed_by(second, group=False).execs == [
        ids["aggregate"]
    ]
    assert trace.diff(
        "global:merged",
        "global:merged",
        "reference",
        "reference",
        0,
        1,
    ).same is True


@pytest.mark.parametrize("which", [0, 1])
def test_heavy_power_user_dynamic_fanin_and_branches_are_complete_and_exact(
    heavy_power_user_real_world,
    which,
):
    live, restored = heavy_power_user_real_world
    trace = (live, restored)[which]
    ids = _execution_ids(trace)

    for loader in ("load_a", "load_b"):
        route = trace.path(
            ids[loader],
            ids["export"],
            "exec_record",
            "exec_record",
        )
        assert _execution_names(trace, route) == {
            (loader, "merge", "repeat_equal", "aggregate", "export")
        }

    merged_v0 = trace.inspect(
        "global:merged", "reference", version=0, grouped=False
    )
    branched = trace.downstream(merged_v0)
    assert branched.count() == 2
    assert _execution_names(trace, branched) == {
        ("merge", "repeat_equal", "aggregate", "export"),
        ("merge", "repeat_equal", "aggregate", "cleanup"),
    }

    # Once the branch has split, an exact State occurrence must not pull in
    # its sibling branch merely because both share the same producer.
    summary = trace.inspect(
        "global:summary", "reference", version=0, grouped=False
    )
    temporary = trace.inspect(
        "global:temporary", "reference", version=0, grouped=False
    )
    assert _execution_names(trace, trace.downstream(summary)) == {
        ("aggregate", "export")
    }
    assert _execution_names(trace, trace.downstream(temporary)) == {
        ("aggregate", "cleanup")
    }

    aggregate = next(
        record
        for record in trace.execs.values()
        if record.exec_id == ids["aggregate"]
    )
    exact_inputs = [
        (
            dependency.unscoped_reference,
            dependency.resolved_version,
            dependency.input_index,
        )
        for dependency in aggregate.deps
    ]
    assert exact_inputs == [
        ("global:merged", 1, 0),
        ("global:repeat_done", 0, 1),
    ]


def test_heavy_power_user_effect_history_explains_success_and_noop_deletes(
    heavy_power_user_real_world,
):
    live, restored = heavy_power_user_real_world

    for trace in (live, restored):
        investigated = trace.investigate(Effects.DELETE, "effect")
        facts = {
            (effect.targets[0], effect.target_existed, effect.origin)
            for record in trace.execs.values()
            for effect in record.effects
            if effect.effect == Effects.DELETE
        }
        assert facts == {
            ("global:temporary", True, "runtime"),
            ("global:never", False, "runtime"),
        }
        data = investigated.to_dict()
        assert data["summary"]["effect_count"] == 2
        assert data["summary"]["target_count"] == 2
        json.dumps(data, sort_keys=True)


def test_heavy_power_user_model_comparison_reports_exact_truth_breaks(
    heavy_power_user_real_world,
):
    _, trace = heavy_power_user_real_world
    source = trace.to_model(enrich=True, incidence_format="pairs")
    assert TIDE.report_model_validity(source)["semantically_valid"] is True

    execution_map = {identity: identity for identity in source["E"]}
    state_map = {identity: identity for identity in source["S"]}
    self_report = TIDE.from_model(source).compare(
        source,
        mapping="supplied",
        execution_map=execution_map,
        state_map=state_map,
    )
    assert self_report.conformant is True
    assert self_report.complete_agreement is True

    # A plausible but false second producer must remain represented and make
    # the model invalid; TIDE must not silently overwrite either producer.
    corrupted = copy.deepcopy(source)
    merged_v1 = _model_state_id(corrupted, "global:merged", 1)
    false_producer = _model_execution_id(corrupted, "load_a")
    corrupted["O"].append([false_producer, merged_v1])

    validity = TIDE.report_model_validity(corrupted)
    assert validity["semantically_valid"] is False
    assert validity["unique_creation"] is False
    reconstructed = TIDE.from_model(corrupted).to_model(
        enrich=True,
        incidence_format="pairs",
    )
    producers = {
        execution
        for execution, state in map(_pair, reconstructed["O"])
        if state == merged_v1
    }
    assert producers == {
        false_producer,
        _model_execution_id(corrupted, "repeat_equal"),
    }

    report = TIDE.from_model(source).compare(
        corrupted,
        mapping="supplied",
        execution_map=execution_map,
        state_map=state_map,
    )
    assert report.conformant is False
    assert report["models"]["target"]["semantically_valid"] is False
    assert len(report["deltas"]["O_plus"]) == 1


def test_heavy_power_user_failed_execution_is_inspectable_without_invented_output():
    @node(targets="global:should_not_exist")
    def fail_after_dispatch():
        raise RuntimeError("professional failure witness")

    @pipeline
    def failed_flow():
        fail_after_dispatch()

    ocean = Ocean()
    try:
        with pytest.raises(
            RuntimeError,
            match="professional failure witness",
        ) as raised:
            ocean.run_pipeline(failed_flow, mode="sequential")
    finally:
        ocean.close()

    live = raised.value.tide_trace
    restored = TIDE.from_bundle(live.to_bundle().to_dict())
    for trace in (live, restored):
        failed = next(
            record
            for record in trace.execs.values()
            if record.func_name == "fail_after_dispatch"
        )
        assert failed.status == "failed"
        assert failed.writes == []
        assert trace.find("fail_after_dispatch").exists("exec_record") == (
            False,
            True,
        )
        assert not trace.inspect(
            "global:should_not_exist",
            "reference",
        ).exists
        model = trace.to_model(enrich=True, incidence_format="pairs")
        assert not any(
            metadata.get("reference") == "global:should_not_exist"
                for metadata in model.get("states", {}).values()
        )


def test_heavy_power_user_effect_relationship_keeps_repeated_actor_occurrences():
    tide = TIDE()
    tide.declare("source", "present")

    def remove():
        DELETE_NOW("global:source")

    def observe_missing():
        return READ("global:source", fallback="missing")

    tide.exec(remove)
    tide.exec(observe_missing)
    tide.exec(observe_missing)
    live = tide.trace()
    restored = TIDE.from_bundle(live.to_bundle().to_dict())

    for trace in (live, restored):
        observers = [
            record
            for record in trace.execs.values()
            if record.func_name == "observe_missing"
        ]
        assert len(observers) == 2
        assert len({record.producer_occurrence_id for record in observers}) == 2

        deletion = next(
            effect
            for record in trace.execs.values()
            for effect in record.effects
            if effect.effect == Effects.DELETE
        )
        assert set(deletion.affected) == {
            record.exec_id for record in observers
        }
        assert {
            affected.affected_exec_id
            for affected in deletion.affected.values()
        } == {record.exec_id for record in observers}
        investigation = trace.investigate(Effects.DELETE, "effect").to_dict()
        assert investigation["summary"]["affected_execution_count"] == 2
