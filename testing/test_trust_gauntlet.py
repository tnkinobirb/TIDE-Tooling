"""One inspectable trust chain from declared work to mutation diagnosis."""

from __future__ import annotations

import copy
from collections import Counter

import pytest

from tide_tooling.tooling_core.engine.events import (
    Effects,
    ExecutorEvents,
    PipelineEvents,
    RouterEvents,
)
from tide_tooling import DELETE_NOW, Ocean, TIDE, comparator, node, pipeline


DYNAMIC_SELECTOR = "mem:csv_*:2:2"
DYNAMIC_STATES = (("mem:csv_a", 0), ("mem:csv_b", 0))


def _pair(entry):
    if isinstance(entry, dict):
        return entry.get("exec_id", entry.get("e")), entry.get(
            "state_id", entry.get("s")
        )
    return tuple(entry)


def _relation(model, name):
    return {_pair(entry) for entry in model[name]}


def _carriers(model):
    incidence = _relation(model, "I") | _relation(model, "O")
    return (
        {execution for execution, _ in incidence},
        {state for _, state in incidence},
    )


def _model(trace):
    return trace.to_model(
        include_metadata=True,
        include_values=True,
        include_hashes=False,
        incidence_format="pairs",
        assign_unknown=True,
    )


def _identity_maps(model):
    executions, states = _carriers(model)
    return (
        {identity: identity for identity in executions},
        {identity: identity for identity in states},
    )


def _exec_id(model, actor):
    matches = [
        execution_id
        for execution_id, metadata in model["executions"].items()
        if metadata.get("actor", {}).get("name") == actor
    ]
    assert len(matches) == 1, (actor, matches)
    return matches[0]


def _state_id(model, reference, version=0):
    def public_name(value):
        store, key = value.rsplit(":", 1)
        return f"mem:{key}" if store.startswith("mem_") else value

    matches = [
        state_id
        for state_id, metadata in model["states"].items()
        if public_name(metadata.get("reference")) == reference
        and metadata.get("version") == version
    ]
    assert len(matches) == 1, ((reference, version), matches)
    return matches[0]


def _replace_identity(model, old, new, *, carrier):
    position = 0 if carrier == "execution" else 1
    for relation in ("I", "O"):
        replaced = []
        for entry in model[relation]:
            pair = list(_pair(entry))
            if pair[position] == old:
                pair[position] = new
            replaced.append(pair)
        model[relation] = replaced


def _gauntlet_pipeline(observed=None):
    observed = observed if observed is not None else {}

    @node(targets="mem:csv_a")
    def seed_a():
        shared = ["a", 1]
        return {"left": shared, "right": shared}

    @node(targets="mem:csv_b")
    def seed_b():
        return {"left": ["b", 2], "right": ["b", 2]}

    @node(args=(f"@{DYNAMIC_SELECTOR}",), targets="mem:merged")
    def merge(rows):
        observed["dynamic_values"] = rows
        return {"rows": rows, "kind": "merged"}

    @node(
        args=("@mem:merged",),
        targets=["mem:merged", "mem:repeat_done"],
        unpack=True,
    )
    def repeat_equal(merged):
        return merged, True

    @node(
        args=("@mem:merged", "@mem:repeat_done"),
        targets="mem:analysis",
    )
    def analyse(merged, _repeat_done):
        return len(merged["rows"])

    @node(
        args=("@mem:merged", "@mem:analysis"),
        targets="mem:archive",
    )
    def archive(merged, analysed):
        return {"copy": merged, "analysed": analysed}

    @node(
        args=("@mem:analysis", "@mem:archive"),
        targets=["mem:result", "mem:temporary"],
        unpack=True,
    )
    def finalise(count, archived):
        return ({"count": count, "archive": archived}, "remove-me")

    @node(args=("@mem:temporary",), targets="mem:cleanup")
    def cleanup(temporary):
        removed = DELETE_NOW("mem:temporary")
        missing = DELETE_NOW("mem:never")
        return {
            "temporary": temporary,
            "removed": removed,
            "missing": missing,
        }

    @pipeline
    def trust_flow():
        seed_a()
        seed_b()
        merge()
        repeat_equal()
        analyse()
        archive()
        finalise()
        cleanup()

    return trust_flow


def _run_gauntlet(mode="sequential", *, ocean=None):
    observed = {}
    ocean = ocean or Ocean()
    trace = ocean.run_pipeline(
        _gauntlet_pipeline(observed),
        mode=mode,
        intent_dynamic_refs={
            DYNAMIC_SELECTOR: ["mem:csv_a", "mem:csv_b"],
        },
    )
    return ocean, trace, observed


@pytest.fixture(scope="module")
def gauntlet_run():
    ocean, trace, observed = _run_gauntlet()
    yield ocean, trace, observed
    ocean.close()


def test_gauntlet_declared_store_event_trace_and_model_agree(gauntlet_run):
    ocean, trace, observed = gauntlet_run
    model = _model(trace)
    executions, states = _carriers(model)

    # Intent and realised dynamic parentage agree.
    assert trace.intent.concretisations == {
        DYNAMIC_SELECTOR: DYNAMIC_STATES,
    }
    assert trace.concretisations == {
        DYNAMIC_SELECTOR: DYNAMIC_STATES,
    }
    assert len(observed["dynamic_values"]) == 2
    assert trace.intent.map.plan_only == {"executions": (), "states": ()}
    assert trace.intent.map.actual_only == {"executions": (), "states": ()}
    assert trace.intent.map.compare()["intent_equal"] is True

    # Every State has one producer and ancestry is acyclic.
    producers = Counter(state for _, state in _relation(model, "O"))
    assert set(producers) == states
    assert set(producers.values()) == {1}
    validation = TIDE.validate_model(model)
    assert validation["conforms"] is True

    execution_map, state_map = _identity_maps(model)
    self_report = comparator(
        model,
        model,
        execution_map=execution_map,
        state_map=state_map,
    )
    assert self_report["validity"]["K1"]["valid"] is True
    assert self_report["judgments"]["K1"]["reachability"]
    assert not any(
        source == target
        for source, target in self_report["judgments"]["K1"]["reachability"]
    )
    assert self_report["validity"]["K1"]["counts"] == {
        "executions": 8,
        "states": 10,
        "inputs": 10,
        "outputs": 10,
        "direct_handoffs": 10,
        "reachability": 27,
    }

    # Reads name the exact version and dynamic reads name only expansions.
    merge_record = next(record for record in trace.exec_log if record.func_name == "merge")
    dynamic = merge_record.deps[0]
    assert dynamic.type == "dynamic"
    assert {
        (expanded.unscoped_reference.rsplit(":", 1)[-1], expanded.resolved_version)
        for expanded in dynamic.expanded_to
    } == {("csv_a", 0), ("csv_b", 0)}
    repeat_record = next(
        record for record in trace.exec_log if record.func_name == "repeat_equal"
    )
    assert [
        (dependency.unscoped_reference.rsplit(":", 1)[-1], dependency.resolved_version)
        for dependency in repeat_record.deps
    ] == [("merged", 0)]

    # Equal internal writes still create distinct State versions.
    merged_writes = sorted(
        (
            write.stored_version,
            write.stored_result,
        )
        for record in trace.exec_log
        for write in record.writes
        if write.stored_target.endswith(":merged")
    )
    assert [version for version, _ in merged_writes] == [0, 1]
    assert merged_writes[0][1] == merged_writes[1][1]
    assert _state_id(model, "mem:merged", 0) != _state_id(
        model, "mem:merged", 1
    )

    # Successful writes occur once in both evidence layers.
    writes = [
        (record.exec_id, write.stored_target, write.stored_version)
        for record in trace.exec_log
        for write in record.writes
    ]
    assert len(writes) == len(set(writes)) == 10
    assert len(_relation(model, "O")) == 10
    event_counts = Counter(entry["event_type"] for entry in trace.events)
    assert event_counts[ExecutorEvents.NODE_READY] == 8
    assert event_counts[PipelineEvents.RESULT_READY] == 8
    assert event_counts[RouterEvents.STORE_WRITE] == 10

    # Deletion changes availability without inventing a State.
    runner_store = trace.runner.store_router
    assert runner_store.exists("mem", "temporary") is False
    assert runner_store.exists("mem", "never") is False
    assert not any(
        metadata.get("reference") == "mem:never"
        for metadata in model["states"].values()
    )
    delete_effects = [
        effect
        for record in trace.exec_log
        for effect in record.effects.values()
        if effect.effect == Effects.DELETE
    ]
    assert len(delete_effects) == 2
    cleanup_value = trace.value_of("cleanup")[0]
    assert cleanup_value == {
        "temporary": "remove-me",
        "removed": True,
        "missing": False,
    }

    # Every model carrier is represented in the total alignment.
    assert len(executions) == len(trace.intent.map.execution_map)
    assert len(states) == len(trace.intent.map.state_map)
    assert trace.diff(trace).same is True
    assert ocean.runner_to_trace[trace.run] is trace


def test_gauntlet_model_and_bundle_round_trips_preserve_answers(gauntlet_run):
    _, trace, _ = gauntlet_run
    model = _model(trace)

    # Trace -> model -> Trace keeps the literal kernel.
    from_model = TIDE.from_model(model)
    rebuilt_model = _model(from_model)
    execution_map, state_map = _identity_maps(model)
    model_report = comparator(
        model,
        rebuilt_model,
        execution_map=execution_map,
        state_map=state_map,
    )
    assert model_report.whole_kernel_equal is True
    assert model_report["judgments"]["mapped_correspond"] is True

    # Bundle keeps values, aliases, metadata, and structure.
    restored = TIDE.from_bundle(trace.to_bundle(), trusted=True)
    restored_model = _model(restored)
    bundle_report = comparator(
        model,
        restored_model,
        execution_map=execution_map,
        state_map=state_map,
    )
    assert bundle_report.whole_kernel_equal is True
    seed = restored.value_of("csv_a")[0]
    assert seed["left"] is seed["right"]
    assert [record.status for record in restored.exec_log] == [
        record.status for record in trace.exec_log
    ]
    assert [record.node_options for record in restored.exec_log] == [
        record.node_options for record in trace.exec_log
    ]


def test_gauntlet_reconstructing_the_same_events_is_idempotent(gauntlet_run):
    ocean, trace, _ = gauntlet_run

    first = ocean.log_tools.create_trace(trace.runner, trace.events)
    second = ocean.log_tools.create_trace(trace.runner, trace.events)

    first_model = _model(first)
    second_model = _model(second)
    execution_map, state_map = _identity_maps(first_model)
    report = comparator(
        first_model,
        second_model,
        execution_map=execution_map,
        state_map=state_map,
    )

    assert report.whole_kernel_equal is True
    assert [record.exec_id for record in first.exec_log] == [
        record.exec_id for record in second.exec_log
    ]
    assert first.concretisations == second.concretisations


def test_gauntlet_repeated_runs_do_not_reimport_prior_occurrences():
    ocean = Ocean()
    try:
        _, first, _ = _run_gauntlet(ocean=ocean)
        _, second, _ = _run_gauntlet(ocean=ocean)

        assert len(first.exec_log) == 8
        assert len(second.exec_log) == 8
        assert {record.exec_id for record in first.exec_log}.isdisjoint(
            record.exec_id for record in second.exec_log
        )
        assert first.intent.map.compare()["intent_equal"] is True
        assert second.intent.map.compare()["intent_equal"] is True
    finally:
        ocean.close()


@pytest.mark.parametrize("mode", ["sequential", "thread"])
def test_gauntlet_executor_metadata_reports_the_mode_that_ran(mode):
    ocean, trace, _ = _run_gauntlet(mode=mode)
    try:
        completed = [
            entry["payload"][2]
            for entry in trace.events
            if entry["event_type"] == PipelineEvents.RESULT_READY
        ]
        assert len(completed) == 8
        assert {metadata.execution_mode for metadata in completed} == {mode}
        assert trace.intent.map.compare()["intent_equal"] is True
    finally:
        ocean.close()


def test_gauntlet_failed_write_creates_no_actual_state():
    ocean = Ocean()

    @node(targets="mem:never_written")
    def fail():
        raise RuntimeError("gauntlet fault")

    @pipeline
    def fault_flow():
        fail()

    try:
        with pytest.raises(RuntimeError, match="gauntlet fault") as caught:
            ocean.run_pipeline(fault_flow, mode="sequential")
        trace = caught.value.tide_trace
        model = _model(trace)

        assert not any(record.writes for record in trace.exec_log)
        assert _relation(model, "O") == set()
        assert trace.intent.map.states.plan_only == (("mem:never_written", 0),)
        assert trace.intent.map.states.actual_only == ()
        assert trace.intent.map.compare()["intent_equal"] is False
        assert not any(
            entry["event_type"] == RouterEvents.STORE_WRITE
            for entry in trace.events
        )
    finally:
        ocean.close()


def _mutation_fixture(trace):
    source = _model(trace)
    target = copy.deepcopy(source)
    execution_map, state_map = _identity_maps(source)
    return source, target, execution_map, state_map


def _mutation_report(trace, mutate):
    source, target, execution_map, state_map = _mutation_fixture(trace)
    mutate(target, execution_map, state_map)
    return comparator(
        source,
        target,
        execution_map=execution_map,
        state_map=state_map,
    )


def test_gauntlet_mutation_remove_input_is_one_preservation_failure(gauntlet_run):
    _, trace, _ = gauntlet_run

    def mutate(target, _, __):
        target["I"].pop(0)

    report = _mutation_report(trace, mutate)
    assert report["validity"]["K2"]["valid"] is True
    assert report["preservation"] == {"I": False, "O": True}
    assert len(report["deltas"]["I_minus"]) == 1
    assert report.conformant is False


def test_gauntlet_mutation_add_input_is_one_reflection_failure(gauntlet_run):
    _, trace, _ = gauntlet_run

    def mutate(target, _, __):
        target["I"].append([
            _exec_id(target, "cleanup"),
            _state_id(target, "mem:result"),
        ])

    report = _mutation_report(trace, mutate)
    assert report["validity"]["K2"]["valid"] is True
    assert report["reflection"] == {"I": False, "O": True}
    assert len(report["deltas"]["I_plus"]) == 1
    assert report.conformant is False


def test_gauntlet_mutation_redirect_producer_is_exact_output_delta(gauntlet_run):
    _, trace, _ = gauntlet_run

    def mutate(target, _, __):
        state = _state_id(target, "mem:analysis")
        target["O"] = [
            [
                _exec_id(target, "repeat_equal")
                if state_id == state
                else execution,
                state_id,
            ]
            for execution, state_id in map(_pair, target["O"])
        ]

    report = _mutation_report(trace, mutate)
    assert report["validity"]["K2"]["valid"] is True
    assert len(report["deltas"]["O_minus"]) == 1
    assert len(report["deltas"]["O_plus"]) == 1
    assert report.conformant is False


def test_gauntlet_mutation_collapse_equal_states_is_invalid(gauntlet_run):
    _, trace, _ = gauntlet_run

    def mutate(target, _, __):
        first = _state_id(target, "mem:merged", 0)
        second = _state_id(target, "mem:merged", 1)
        _replace_identity(target, second, first, carrier="state")
        target["states"].pop(second)

    report = _mutation_report(trace, mutate)
    assert report["validity"]["K2"]["valid"] is False
    assert report["validity"]["K2"]["unique_creation"] is False
    assert report.conformant is False


def test_gauntlet_mutation_merge_executions_reports_absent_identity(gauntlet_run):
    _, trace, _ = gauntlet_run

    def mutate(target, _, __):
        removed = _exec_id(target, "seed_a")
        retained = _exec_id(target, "seed_b")
        _replace_identity(target, removed, retained, carrier="execution")
        target["executions"].pop(removed)

    report = _mutation_report(trace, mutate)
    assert report.assessable is True
    assert report["validity"]["K2"]["valid"] is True
    assert len(report["mapped_identities_absent"]["K2"]["Executions"]) == 1
    assert report.whole_kernel_equal is False


def test_gauntlet_mutation_reuse_state_id_breaks_unique_creation(gauntlet_run):
    _, trace, _ = gauntlet_run

    def mutate(target, _, __):
        target["O"].append([
            _exec_id(target, "seed_b"),
            _state_id(target, "mem:csv_a"),
        ])

    report = _mutation_report(trace, mutate)
    assert report["validity"]["K2"]["valid"] is False
    assert report["validity"]["K2"]["unique_creation"] is False
    assert len(report["deltas"]["O_plus"]) == 1


def test_gauntlet_mutation_cycle_breaks_acyclicity(gauntlet_run):
    _, trace, _ = gauntlet_run

    def mutate(target, _, __):
        target["I"].append([
            _exec_id(target, "seed_a"),
            _state_id(target, "mem:cleanup"),
        ])

    report = _mutation_report(trace, mutate)
    assert report["validity"]["K2"]["valid"] is False
    assert report["validity"]["K2"]["acyclic_ancestry"] is False
    assert len(report["deltas"]["I_plus"]) == 1


def test_gauntlet_mutation_remove_producer_is_invalid_and_missing(gauntlet_run):
    _, trace, _ = gauntlet_run

    def mutate(target, _, __):
        state = _state_id(target, "mem:temporary")
        target["O"] = [
            list(pair)
            for pair in map(_pair, target["O"])
            if pair[1] != state
        ]

    report = _mutation_report(trace, mutate)
    assert report["validity"]["K2"]["valid"] is False
    assert len(report["validity"]["K2"]["producer_count_errors"]) == 1
    assert len(report["deltas"]["O_minus"]) == 1


def test_gauntlet_mutation_swap_versions_is_valid_but_nonconforming(gauntlet_run):
    _, trace, _ = gauntlet_run

    def mutate(target, _, state_map):
        first = _state_id(target, "mem:merged", 0)
        second = _state_id(target, "mem:merged", 1)
        state_map[first], state_map[second] = second, first

    report = _mutation_report(trace, mutate)
    assert report.assessable is True
    assert report["validity"]["K2"]["valid"] is True
    assert report.conformant is False
    assert report["deltas"]["I_minus"]
    assert report["deltas"]["I_plus"]


def test_gauntlet_mutation_duplicate_incidence_is_setwise_equal(gauntlet_run):
    _, trace, _ = gauntlet_run

    def mutate(target, _, __):
        target["I"].append(copy.deepcopy(target["I"][0]))

    report = _mutation_report(trace, mutate)
    assert report["validity"]["K2"]["valid"] is True
    assert report.conformant is True
    assert report.whole_kernel_equal is True


def test_gauntlet_mutation_metadata_is_outside_the_kernel(gauntlet_run):
    _, trace, _ = gauntlet_run

    def mutate(target, _, __):
        target["host"] = "mutated-host"
        target["host_record"] = {"irrelevant": True}
        next(iter(target["executions"].values()))["actor"]["name"] = "renamed"
        next(iter(target["states"].values()))["value"] = "changed"

    report = _mutation_report(trace, mutate)
    assert report["validity"]["K2"]["valid"] is True
    assert report.conformant is True
    assert report.whole_kernel_equal is True


def test_gauntlet_mutation_unmapped_path_is_visible_outside_scope(gauntlet_run):
    _, trace, _ = gauntlet_run

    def mutate(target, _, __):
        target["O"].append(["extra-producer", "extra-state"])
        target["I"].append(["extra-consumer", "extra-state"])

    report = _mutation_report(trace, mutate)
    assert report["validity"]["K2"]["valid"] is True
    assert report.conformant is True
    assert report["carrier_coverage"] == {"K1": True, "K2": False}
    assert report.whole_kernel_equal is False
    assert report["outside"]["K2"] == {
        "I": (("extra-consumer", "extra-state"),),
        "O": (("extra-producer", "extra-state"),),
    }


def test_gauntlet_mutation_absent_mapped_target_is_reported(gauntlet_run):
    _, trace, _ = gauntlet_run

    def mutate(_, __, state_map):
        source_state = next(iter(state_map))
        state_map[source_state] = "absent-target-state"

    report = _mutation_report(trace, mutate)
    assert report.assessable is True
    assert report.conformant is False
    assert report["mapped_identities_absent"]["K2"]["States"] == (
        "absent-target-state",
    )
    assert report["deltas"]["I_minus"] or report["deltas"]["O_minus"]


def test_gauntlet_mutation_noninjective_map_is_unassessable(gauntlet_run):
    _, trace, _ = gauntlet_run

    def mutate(_, execution_map, __):
        first, second = list(execution_map)[:2]
        execution_map[first] = execution_map[second]

    report = _mutation_report(trace, mutate)
    assert report.assessable is False
    assert report.conformant is None
    assert report.whole_kernel_equal is None
    assert report["mapping_errors"] == (
        "Execution map must be one-to-one",
    ) or report["mapping_errors"] == [
        "Execution map must be one-to-one",
    ]
