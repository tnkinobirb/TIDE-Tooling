"""Execution-relationship torture tests built from the worst provenance cases.

This is deliberately not a collection of happy-path API examples.  It joins
the difficult cases described by W3C PROV constraints, OpenTelemetry's trace
and sampling contracts, OpenLineage's run/dataset model, and TIDE's own bug
history into one auditable suite.

Some hazards cannot truthfully coexist in one execution.  A valid runtime DAG,
a deliberately cyclic Model, a causally corrupted Event stream, and a hostile
Bundle are therefore separate chambers in this one file.  ``CASE_COVERAGE``
makes that division explicit and prevents a named hazard from becoming merely
decorative documentation.

Primary references:
* https://www.w3.org/TR/prov-constraints/
* https://opentelemetry.io/docs/specs/otel/trace/api/
* https://opentelemetry.io/docs/specs/otel/trace/sdk/
* https://openlineage.io/docs/spec/object-model/
* https://openlineage.io/docs/spec/facets/job-facets/lineage/
"""

from __future__ import annotations

import copy
import json
import threading

import pytest

from tide_tooling import (
    DELETE_NOW,
    EMIT,
    READ,
    RAISE,
    RUN,
    Effects,
    Ocean,
    OpenLineageAdapter,
    TIDE,
    node,
    pipeline,
)
from tide_tooling.tooling_core.bundling.TIDEBundler import TIDEBundler
from tide_tooling.tooling_core.engine.classes import DataRef
from tide_tooling.tooling_core.engine.event_payloads import State
from tide_tooling.tooling_core.engine.event_readers import EventLogReader
from tide_tooling.tooling_core.engine.events import (
    Cause,
    Event,
    PipelineEvents,
    RouterEvents,
)
from tide_tooling.tooling_core.engine.tide_logging import Trace
from tide_tooling.tooling_core.engine.tide_runtime_logging import trace_from_event_log
from tide_tooling.tooling_core.engine.utils import (
    tide_snapshot_value,
    tide_value_hasher,
)

from testing.test_real_world_trust_test import (
    DYNAMIC_SELECTOR,
    _TruthLedger,
    _build_workload,
)


HARD_CASES = {
    "equal_value_distinct_state": "equal values at different State versions",
    "mutation_after_recording": "external mutation and observed replacement State",
    "dynamic_fanin_fanout": "runtime-discovered fan-in followed by fan-out",
    "repeated_actor_occurrence": "same Actor definition, distinct executions",
    "partial_external_evidence": "boundary producers and missing observations",
    "failure_retry_speculation": "failed, handled, retried, and competing work",
    "concurrency_without_clock_order": "causality under concurrent scheduling",
    "deletion_absence_invalidation": "never existed versus deleted versus unknown",
    "aliases_and_cycles": "shared and recursively self-referential values",
    "multiple_contradictory_producers": "more than one producer for one State",
    "cross_system_identity": "independent host IDs and TIDE identities",
    "granularity_mismatch": "one coarse job versus several fine executions",
    "nested_emitted_delegated_work": "nested, emitted, and child-pipeline work",
    "effects_not_dataflow": "Effects with causal meaning beyond I/O incidence",
    "nondeterministic_environment": "same Actor producing different observations",
    "persisted_representations_disagree": "Bundle indexes/run/value graph conflict",
    "duplicate_input_occurrences": "same State used at multiple input positions",
    "false_cartesian_lineage": "independent inputs/outputs must not cross-connect",
    "cyclic_or_self_dependent_model": "invalid ancestry cycles and self-dependence",
    "disconnected_or_orphan_evidence": "idle work and States with no producer",
    "mapping_cardinality_and_absence": "non-bijections and absent mapped identity",
    "typed_value_identity": "cross-type and hash-looking value collisions",
    "deep_or_resource_hostile_graph": "deep traversal and import resource ceilings",
    "sampling_or_dropped_evidence": "known-incomplete telemetry must stay incomplete",
    "late_or_out_of_order_events": "cause appears missing or after consequence",
    "run_lifecycle_and_retry_identity": "one Job definition across distinct Runs",
    "speculative_winner_and_loser": "competing results without collapsed identity",
    "scoped_reference_collision": "same key in distinct stores/scopes",
    "field_level_lineage": "precise edges rather than dataset Cartesian products",
}


CASE_COVERAGE = {
    "test_torture_live_graph_queries_and_bundle_generations": {
        "equal_value_distinct_state",
        "dynamic_fanin_fanout",
        "deletion_absence_invalidation",
        "aliases_and_cycles",
        "effects_not_dataflow",
        "persisted_representations_disagree",
        "false_cartesian_lineage",
    },
    "test_torture_mutation_external_inputs_duplicates_scopes_and_value_identity": {
        "mutation_after_recording",
        "partial_external_evidence",
        "aliases_and_cycles",
        "duplicate_input_occurrences",
        "nondeterministic_environment",
        "typed_value_identity",
        "scoped_reference_collision",
    },
    "test_torture_concurrency_retries_emission_nesting_and_delegation": {
        "repeated_actor_occurrence",
        "failure_retry_speculation",
        "concurrency_without_clock_order",
        "nested_emitted_delegated_work",
        "effects_not_dataflow",
        "run_lifecycle_and_retry_identity",
        "speculative_winner_and_loser",
    },
    "test_torture_corruption_partial_order_and_resource_boundaries": {
        "partial_external_evidence",
        "sampling_or_dropped_evidence",
        "late_or_out_of_order_events",
        "deep_or_resource_hostile_graph",
    },
    "test_torture_models_retain_invalid_and_unmapped_truth": {
        "multiple_contradictory_producers",
        "granularity_mismatch",
        "cyclic_or_self_dependent_model",
        "disconnected_or_orphan_evidence",
        "mapping_cardinality_and_absence",
        "speculative_winner_and_loser",
    },
    "test_torture_cross_system_and_field_level_lineage_does_not_invent_edges": {
        "cross_system_identity",
        "granularity_mismatch",
        "false_cartesian_lineage",
        "field_level_lineage",
    },
    "test_torture_bundle_rejects_plausible_but_contradictory_relationships": {
        "persisted_representations_disagree",
        "multiple_contradictory_producers",
    },
    "test_torture_path_traversal_survives_graph_deeper_than_python_recursion": {
        "deep_or_resource_hostile_graph",
    },
}


def _ids(trace):
    return {
        record.func_name: record.exec_id
        for record in trace.execs.values()
    }


def _pair(entry):
    if isinstance(entry, dict):
        return entry.get("exec_id", entry.get("e")), entry.get(
            "state_id", entry.get("s")
        )
    return tuple(entry)


def _query_contract(trace):
    ids = _ids(trace)
    exact = trace.inspect(
        "global:merged", "reference", version=0, grouped=False
    )
    return {
        "find": trace.find("merged").to_dict(),
        "inspect": exact.to_dict(),
        "investigate": trace.investigate(exact).to_dict(),
        "producer": trace.produced_by(exact, group=False).to_dict(),
        "consumer": trace.consumed_by(exact, group=False).to_dict(),
        "upstream": trace.upstream(
            ids["export"], "exec_record", grouped=False
        ).to_dict(),
        "downstream": trace.downstream(exact).to_dict(),
        "path": trace.path(
            ids["load_a"],
            ids["export"],
            "exec_record",
            "exec_record",
        ).to_dict(),
        "inputs": trace.inputs_of(ids["aggregate"]).to_dict(),
        "output": trace.output_of(ids["aggregate"]).to_dict(),
        "definition": trace.definition_of(ids["aggregate"]).to_dict(),
        "effects": trace.investigate(Effects.DELETE, "effect").to_dict(),
    }


def test_torture_manifest_covers_every_named_hard_case():
    covered = set().union(*CASE_COVERAGE.values())
    assert covered == set(HARD_CASES)


def test_torture_live_graph_queries_and_bundle_generations(tmp_path):
    ledger = _TruthLedger()
    workload, _ = _build_workload(tmp_path, ledger)
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

    generations = [live]
    for _ in range(3):
        generations.append(
            TIDE.from_bundle(generations[-1].to_bundle().to_dict())
        )

    expected_queries = _query_contract(live)
    for trace in generations:
        assert _query_contract(trace) == expected_queries
        json.dumps(_query_contract(trace), sort_keys=True)

        first = trace.inspect(
            "global:merged", "reference", version=0, grouped=False
        )
        second = trace.inspect(
            "global:merged", "reference", version=1, grouped=False
        )
        assert trace.value_of(first) == trace.value_of(second)
        assert trace.produced_by(first, group=False).execs != (
            trace.produced_by(second, group=False).execs
        )

        merged = trace.value_of(second)
        assert merged["left"] is merged["right"]
        deletes = {
            (effect.targets[0], effect.target_existed)
            for record in trace.log
            for effect in record.effects
            if effect.effect == Effects.DELETE
        }
        assert deletes == {
            ("global:temporary", True),
            ("global:never", False),
        }


def test_torture_mutation_external_inputs_duplicates_scopes_and_value_identity():
    shared = []
    recursive = {"shared_left": shared, "shared_right": shared}
    recursive["self"] = recursive

    tide = TIDE()
    tide.ocean.register_store("secondary")
    tide.declare("mutable", recursive)
    shared.append("mutated-after-write")

    tide.exec(
        lambda value: value["shared_left"] is value["shared_right"],
        reads="mutable",
        writes="alias_preserved",
    )
    tide.event("global scoped", writes={"global:same_key": "global"})
    tide.event(
        "secondary scoped",
        writes={"secondary:same_key": "secondary"},
    )
    tide.exec(
        lambda first, second: first == second,
        args=["@mutable", "@mutable"],
        writes="duplicate_use",
    )
    live = tide.trace()
    restored = TIDE.from_bundle(live.to_bundle().to_dict())

    for trace in (live, restored):
        assert trace.value_of("global:mutable", 0)["shared_left"] == []
        uses = trace.used_as("global:mutable[v1]")
        assert len(uses) == 3
        assert all(
            not use["observed"]["differs_from_stored"]
            for use in uses.uses
        )
        observed = uses.values[0]
        assert observed["shared_left"] == ["mutated-after-write"]
        assert observed["shared_left"] is observed["shared_right"]
        assert observed["self"] is observed
        assert trace.value_of("global:same_key", 0) == "global"
        assert trace.value_of("secondary:same_key", 0) == "secondary"

        duplicate = next(
            record for record in trace.log if record.func_name == "<lambda>"
            and len(record.deps) == 2
        )
        assert [dependency.input_index for dependency in duplicate.deps] == [
            0,
            1,
        ]
        assert duplicate.deps[0] is not duplicate.deps[1]
        assert duplicate.deps[0] != duplicate.deps[1]

    identities = [
        tide_value_hasher(None),
        tide_value_hasher(b"null"),
        tide_value_hasher(1),
        tide_value_hasher(b"1"),
        tide_value_hasher(bytearray(b"1")),
        tide_value_hasher(memoryview(b"1")),
    ]
    assert len(set(identities)) == len(identities)
    ordinary_hash = tide_value_hasher(42)
    assert tide_value_hasher(ordinary_hash) != ordinary_hash
    assert tide_value_hasher({1, 2, 3}) == tide_value_hasher({3, 2, 1})
    assert tide_value_hasher({"first": 1, "second": 2}) != tide_value_hasher(
        {"second": 2, "first": 1}
    )


def test_torture_concurrency_retries_emission_nesting_and_delegation():
    barrier = threading.Barrier(2)

    @node(targets="global:left")
    def left():
        barrier.wait(timeout=3)
        return "left"

    @node(targets="global:right")
    def right():
        barrier.wait(timeout=3)
        return "right"

    @node(targets="global:joined")
    def join(left_value, right_value):
        return [left_value, right_value]

    @pipeline
    def concurrent_diamond():
        join(left(), right())

    ocean = Ocean(timeout=5)
    try:
        concurrent = ocean.run_pipeline(concurrent_diamond, mode="thread")
    finally:
        ocean.close()
    ids = _ids(concurrent)
    for source in ("left", "right"):
        assert concurrent.path(
            ids[source], ids["join"], "exec_record", "exec_record"
        ).exists()

    tide = TIDE()
    attempts = []

    def retryable():
        attempts.append(len(attempts))
        if len(attempts) == 1:
            raise RAISE(ValueError("transient"), output="fallback")
        return "winner"

    tide.exec(retryable, writes="attempt")
    tide.exec(retryable, writes="attempt")
    retry_trace = tide.trace()
    retries = [record for record in retry_trace.log if record.func_name == "retryable"]
    assert [record.status for record in retries] == ["handled_raise", "succeeded"]
    assert len({record.producer_occurrence_id for record in retries}) == 2
    assert [retry_trace.value_of("global:attempt", version) for version in (0, 1)] == [
        "fallback",
        "winner",
    ]

    @node(targets="global:emitted")
    def emitted_child():
        return "emitted"

    @node
    def emitter():
        return EMIT(emitted_child.as_node())

    @pipeline
    def emission_flow():
        emitter()

    emission_tide = TIDE()
    emitted = emission_tide.ocean.run_pipeline(
        emission_flow,
        mode="sequential",
    )
    assert emitted.log[1].emitted_by_exec_id == emitted.log[0].exec_id
    assert emitted.path("emitter", "emitted_child", "actor", "actor").exists()
    emitted_restored = TIDE.from_bundle(emitted.to_bundle().to_dict())
    assert emitted_restored.path(
        "emitter", "emitted_child", "actor", "actor"
    ).exists()

    @node(targets="global:nested_source")
    def nested_source():
        return {"nested": "value"}

    @node(targets="global:nested_result")
    def nested_consumer(value):
        return value["nested"]

    @pipeline
    def nested_flow():
        nested_consumer(nested_source())

    nested_tide = TIDE()
    nested = nested_tide.ocean.run_pipeline(nested_flow, mode="sequential")
    nested_source_ref = nested.log[0].writes[0].stored_target
    nested_target_ref = nested.log[1].writes[0].stored_target
    assert nested.path(
        nested_source_ref,
        nested_target_ref,
        "reference",
        "reference",
    ).exists()

    @node(targets="global:impossible")
    def terminal_failure():
        raise RuntimeError("terminal torture failure")

    @pipeline
    def terminal_flow():
        terminal_failure()

    failed_tide = TIDE()
    with pytest.raises(RuntimeError, match="terminal torture failure") as raised:
        failed_tide.ocean.run_pipeline(terminal_flow, mode="sequential")
    failed = raised.value.tide_trace
    failed_record = next(
        record for record in failed.log if record.func_name == "terminal_failure"
    )
    assert failed_record.status == "failed"
    assert failed_record.writes == []
    assert not failed.inspect("global:impossible", "reference").exists
    failed_restored = TIDE.from_bundle(failed.to_bundle().to_dict())
    assert failed_restored.log[-1].status == "failed"
    assert failed_restored.log[-1].writes == []

    @node(targets="global:child_result")
    def child_work():
        return "child"

    @pipeline
    def child_pipeline():
        child_work()

    delegated_tide = TIDE()
    child = delegated_tide.ocean.register_pipeline(child_pipeline())

    @node(effects=RUN(child.id, mode="sequential"))
    def launch_child():
        return "launched"

    @pipeline
    def parent_pipeline():
        launch_child()

    delegated_tide.ocean.run_pipeline(parent_pipeline, mode="sequential")
    combined = delegated_tide.ocean._trace_from_runner(
        delegated_tide.ocean.runners[0],
    )
    assert {record.func_name for record in combined.log} == {
        "launch_child",
        "child_work",
    }
    assert combined.find(Effects.RUN).exists("effect") == (True, False)
    emission_tide.ocean.close()
    nested_tide.ocean.close()
    failed_tide.ocean.close()
    delegated_tide.ocean.close()


def test_torture_corruption_partial_order_and_resource_boundaries():
    reference = DataRef(key="evidence", store="global")
    snapshot = tide_snapshot_value("value", boundary="write")
    write = Event(
        source="business",
        source_id="writer",
        type=RouterEvents.STORE_WRITE,
        cause=Cause(description="external write"),
        ocean_id="ocean",
        payload=State(reference, "value", snapshot),
    )
    consequence = Event(
        source="runner",
        source_id="reader",
        type=PipelineEvents.DEP_AVAILABLE,
        cause=Cause(write.event_id, str(RouterEvents.STORE_WRITE)),
        ocean_id="ocean",
        payload=State(reference, "value", snapshot),
    )

    out_of_order = EventLogReader("ocean")
    out_of_order_state = out_of_order._construct_log([consequence, write])
    assert out_of_order_state.causal_order_violations == {
        write.event_id: [consequence.event_id]
    }

    missing = Event(
        source="business",
        source_id="writer",
        type=RouterEvents.STORE_WRITE,
        cause=Cause("dropped-event", "sampled away"),
        ocean_id="ocean",
        payload=State(reference, "value", snapshot),
    )
    partial = EventLogReader("ocean")
    partial._construct_log([missing])
    corrupted = trace_from_event_log(
        [missing],
        ocean_id="ocean",
    )
    restored = TIDE.from_bundle(corrupted.to_bundle().to_dict())
    assert restored.corrupted is True
    assert restored.corruption == corrupted.corruption
    assert "CORRUPTED" in restored.show(color=False)

    ordinary = TIDE()
    ordinary.event("bounded Event")
    complete = ordinary.trace().to_bundle(serialise_events=True).to_dict()
    with pytest.raises(ValueError, match="max_event_records"):
        TIDEBundler(
            resource_limits={"max_event_records": 1}
        ).load_trace_bundle(complete)


def test_torture_models_retain_invalid_and_unmapped_truth():
    multi_producer = {
        "E": ["winner", "loser", "consumer"],
        "S": ["candidate"],
        "I": [["consumer", "candidate"]],
        "O": [["winner", "candidate"], ["loser", "candidate"]],
    }
    multi_report = TIDE.report_model_validity(multi_producer)
    assert multi_report["semantically_valid"] is False
    assert multi_report["unique_creation"] is False
    assert set(map(_pair, TIDE.from_model(multi_producer).to_model()["O"])) == {
        ("winner", "candidate"),
        ("loser", "candidate"),
    }

    cycle = {
        "E": ["a", "b"],
        "S": ["a_to_b", "b_to_a"],
        "I": [["a", "b_to_a"], ["b", "a_to_b"]],
        "O": [["a", "a_to_b"], ["b", "b_to_a"]],
    }
    cycle_report = TIDE.report_model_validity(cycle)
    assert cycle_report["semantically_valid"] is False
    assert cycle_report["acyclic_ancestry"] is False

    orphan = {
        "E": ["idle", "consumer"],
        "S": ["boundary"],
        "I": [["consumer", "boundary"]],
        "O": [],
    }
    orphan_report = TIDE.report_model_validity(orphan)
    assert orphan_report["semantically_valid"] is False
    from tide_tooling.tooling_core.engine.tide_model import TIDEModel

    assert TIDE.from_model(orphan).to_model(enrich=False) == TIDEModel.normalise(
        orphan
    )

    source = {
        "E": ["producer", "consumer", "outside"],
        "S": ["state", "outside_state"],
        "I": [["consumer", "state"]],
        "O": [["producer", "state"], ["outside", "outside_state"]],
    }
    target = {
        "E": ["P", "C"],
        "S": ["S"],
        "I": [["C", "S"]],
        "O": [["P", "S"]],
    }
    partial = TIDE.from_model(source).compare(
        target,
        mapping="supplied",
        execution_map={"producer": "P", "consumer": "C"},
        state_map={"state": "S"},
    )
    assert partial.conformant is True
    assert partial.complete_agreement is False
    assert partial["outside"]["source"]["executions"] == ["outside"]

    non_bijection = TIDE.from_model(source).compare(
        target,
        mapping="supplied",
        execution_map={"producer": "P", "consumer": "P"},
        state_map={"state": "S"},
    )
    assert non_bijection.assessable is False
    assert non_bijection.conformant is None


def test_torture_cross_system_and_field_level_lineage_does_not_invent_edges():
    events = [{
        "eventType": "COMPLETE",
        "eventTime": "2026-09-29T00:00:00Z",
        "run": {"runId": "coarse-run"},
        "job": {"namespace": "host", "name": "coarse-job"},
        "inputs": [
            {"namespace": "warehouse", "name": "customers"},
            {"namespace": "warehouse", "name": "orders"},
        ],
        "outputs": [
            {"namespace": "warehouse", "name": "customer_summary"},
            {"namespace": "warehouse", "name": "order_summary"},
        ],
        "producer": "https://example.test/lineage",
    }]
    coarse = OpenLineageAdapter(events).to_model()
    assert coarse["host_record"] == events

    fine = {
        "E": ["customer_job", "order_job"],
        "S": [
            "customers",
            "orders",
            "customer_summary",
            "order_summary",
        ],
        "I": [
            ["customer_job", "customers"],
            ["order_job", "orders"],
        ],
        "O": [
            ["customer_job", "customer_summary"],
            ["order_job", "order_summary"],
        ],
    }
    fine_validity = TIDE.report_model_validity(fine)
    # Boundary inputs receive explicit external producers in a Trace, while
    # the two precise internal jobs remain distinct.
    assert fine_validity["semantically_valid"] is False
    projected = TIDE.from_model(fine)
    exported = projected.to_model(enrich=False)
    assert set(map(_pair, exported["I"])) >= set(map(tuple, fine["I"]))
    assert set(map(_pair, exported["O"])) >= set(map(tuple, fine["O"]))

    comparison = projected.compare(coarse, mapping="structural")
    assert comparison.complete_agreement is False
    assert comparison.conformant is False
    # Crucially, the fine model contains only the two true field/dataset
    # relationships, never the four-edge Cartesian product.
    assert len(fine["I"]) == len(fine["O"]) == 2


def test_torture_path_traversal_survives_graph_deeper_than_python_recursion():
    tide = TIDE()
    length = 1050
    tide.event("deep_0", writes={"deep_state_0": 0})
    for index in range(1, length):
        tide.event(
            f"deep_{index}",
            reads={f"deep_state_{index - 1}": index - 1},
            writes={f"deep_state_{index}": index},
        )

    trace = tide.trace()
    path = trace.path("deep_0", f"deep_{length - 1}", "actor", "actor")
    assert path.count() == 1
    assert len(path.get_path()["executions"]) == length
