"""Third Provenance Challenge inspection contracts.

The source tutorial mixed notebook cells, proposed probes, source answers, and
explicit evidence gaps.  This module keeps only executable contracts:

* relation questions supported by the supplied file/table template;
* public inspection/query behavior exercised through one non-trivial Trace;
* explicit checks that absent rows, values, statuses, time, control flow, and
  domain joins are not presented as answers.

The fixture is an interpretation of one representative file/table history. It
is not the full PC3 run and the tests do not claim that it is.
"""

import copy
import json
from pathlib import Path

import pytest

from tide_tooling import TIDE


FIXTURE_PATH = (
    Path(__file__).parent
    / "fixtures"
    / "third_provenance_challenge_tide_io_template.json"
)


def scoped(state_id):
    return f"__model__:{state_id}"


def edge_identity(edge):
    return (
        edge.producer_exec_id,
        edge.consumer_exec_id,
        edge.produced_ref,
        edge.produced_ver,
    )


@pytest.fixture(scope="module")
def challenge_source():
    with FIXTURE_PATH.open(encoding="utf-8") as source:
        return json.load(source)


@pytest.fixture(scope="module")
def challenge_model(challenge_source):
    # Explicit and inferred outputs stay reviewable in the fixture.  The TIDE
    # import boundary receives their union as its one O relation.
    return {
        "host": challenge_source["name"],
        "I": copy.deepcopy(challenge_source["I"]),
        "O": copy.deepcopy(
            challenge_source["O_explicit"]
            + challenge_source["O_inferred"]
        ),
    }


@pytest.fixture(scope="module")
def challenge_trace(challenge_model):
    with pytest.warns(
        RuntimeWarning,
        match="does not establish host provenance",
    ):
        return TIDE.from_model(
            copy.deepcopy(challenge_model),
            assign_unknown=True,
        )


def test_fixture_keeps_claimed_and_inferred_outputs_separate(
    challenge_source,
    challenge_model,
):
    assert "O" not in challenge_source
    assert len(challenge_source["I"]) == 16
    assert len(challenge_source["O_explicit"]) == 11
    assert len(challenge_source["O_inferred"]) == 3
    assert len(challenge_model["O"]) == 14
    assert not (
        {tuple(edge) for edge in challenge_source["O_explicit"]}
        & {tuple(edge) for edge in challenge_source["O_inferred"]}
    )


def test_boundary_states_remain_an_explicit_import_decision(
    challenge_model,
    challenge_trace,
):
    original_report = TIDE.validate_model(challenge_model)
    producer_errors = {
        error
        for error in original_report["errors"]
        if error.startswith("every_state_has_producer:")
    }

    assert original_report["conforms"] is False
    # Validation stops at the first failed validity condition.  The repair
    # boundary below derives and records the complete missing-State set.
    assert len(producer_errors) == 1

    with pytest.raises(ValueError, match="assign_unknown=True"):
        TIDE.from_model(copy.deepcopy(challenge_model))

    assert challenge_trace.model_context["assigned_unknown_states"] == [
        "s:csv_root_path",
        "s:job_id",
    ]

    with pytest.raises(ValueError, match="assign_unknown=True"):
        challenge_trace.to_model(include_hashes=False)

    with pytest.warns(
        RuntimeWarning,
        match="does not establish host provenance",
    ):
        exported = challenge_trace.to_model(
            include_hashes=False,
            assign_unknown=True,
        )

    assert TIDE.validate_model(exported)["conforms"] is True


def test_trace_literal_and_structural_views_keep_the_same_history(
    challenge_trace,
):
    literal = challenge_trace.show(color=False)
    structural = challenge_trace.show(
        structural=True,
        color=False,
    )
    data = challenge_trace.to_dict()

    assert "TRACE | 13 executions found" in literal
    assert "e:08:load_csv_file_into_table" in literal
    assert "TRACE | 13 executions found" in structural
    assert data["host"] == "Third Provenance Challenge"
    assert data["model_context"]["identity_mode"] == "host"
    json.dumps(data)


def test_find_distinguishes_exact_identity_from_nearby_discovery(
    challenge_trace,
):
    exact_ref = challenge_trace.find(scoped("s:database_v2_computed"))
    nearby_ref = challenge_trace.find("database_v2_computed")
    nearby_actor = challenge_trace.find("load_csv_file_into_table")

    assert exact_ref.exists("reference") == (True, False)
    assert nearby_ref.exists("reference") == (False, True)
    assert nearby_actor.exists("actor") == (False, True)
    assert nearby_ref.first("reference").id == scoped(
        "s:database_v2_computed"
    )


def test_inspect_preserves_strict_and_grouped_identity_semantics(
    challenge_trace,
):
    execution = challenge_trace.inspect(
        "e:08:load_csv_file_into_table",
        "exec_record",
    )
    actor_group = challenge_trace.inspect(
        "load_csv_file_into_table",
        "actor",
    )
    reference_group = challenge_trace.inspect(
        "database_v2_computed",
        "reference",
    )
    wrong_kind = challenge_trace.inspect(
        "e:08:load_csv_file_into_table",
        "reference",
        grouped=False,
    )

    assert execution.exists and not execution.grouped
    assert execution.id == "e:08:load_csv_file_into_table"
    assert actor_group.exists and actor_group.grouped
    assert actor_group.id == "load_csv_file_into_table"
    assert reference_group.exists and reference_group.grouped
    assert reference_group.id == "database_v2_computed"
    assert wrong_kind.exists is False


def test_investigate_exposes_exact_load_and_database_relations(
    challenge_trace,
):
    load = challenge_trace.investigate(
        "load_csv_file_into_table",
        "actor",
    ).to_dict()
    database = challenge_trace.investigate(
        "database_v2_computed",
        "reference",
    ).to_dict()

    load_history = load["history"][0]
    assert {
        read["ref"]
        for read in load_history["reads"]
    } == {
        scoped("s:csv_file_entry_v1"),
        scoped("s:database_v0_empty"),
    }
    assert {
        write["ref"]
        for write in load_history["writes"]
    } == {
        scoped("s:database_v1_loaded"),
        scoped("s:load_result"),
    }

    database_version = database["history"][0]
    assert database_version["producer"]["id"] == (
        "e:09:update_computed_columns"
    )
    assert {
        consumer["id"]
        for consumer in database_version["consumers"]
    } == {
        "e:10:is_match_table_row_count",
        "e:11:is_match_table_column_ranges",
        "e:12:compact_database",
    }


def test_produced_and_consumed_helpers_agree_for_refs_actors_and_execs(
    challenge_trace,
):
    database_producers = challenge_trace.produced_by(
        "database_v2_computed",
        "reference",
    )
    database_consumers = challenge_trace.consumed_by(
        "database_v2_computed",
        "reference",
    )
    load_actor_outputs = challenge_trace.produced_by(
        "load_csv_file_into_table",
        "actor",
    )
    load_actor_inputs = challenge_trace.consumed_by(
        "load_csv_file_into_table",
        "actor",
    )
    load_exec = challenge_trace.inspect(
        "e:08:load_csv_file_into_table",
        "exec_record",
    )

    assert database_producers.execs == [
        "e:09:update_computed_columns"
    ]
    assert set(database_consumers.execs) == {
        "e:10:is_match_table_row_count",
        "e:11:is_match_table_column_ranges",
        "e:12:compact_database",
    }
    assert set(load_actor_outputs.refs) == {
        (scoped("s:database_v1_loaded"), 0),
        (scoped("s:load_result"), 0),
    }
    assert set(load_actor_inputs.refs) == {
        (scoped("s:csv_file_entry_v1"), 0),
        (scoped("s:database_v0_empty"), 0),
    }
    assert (
        challenge_trace.produced_by(load_exec).refs
        == load_actor_outputs.refs
    )
    assert (
        challenge_trace.consumed_by(load_exec).refs
        == load_actor_inputs.refs
    )


def test_core_query_2_range_check_has_the_selected_database_input(
    challenge_trace,
):
    path = challenge_trace.path(
        "database_v2_computed",
        "is_match_table_column_ranges",
        "reference",
        "actor",
    )

    assert path.exists()
    assert path.count() == 1
    assert path.execution_route_count() == 1
    assert "s:database_v2_computed" in path.show(color=False)


def test_core_query_3_reports_ancestry_candidates_not_strict_necessity(
    challenge_trace,
):
    read_to_loaded = challenge_trace.path(
        "read_csv_ready_file",
        "database_v1_loaded",
        "actor",
        "reference",
    )
    create_to_loaded = challenge_trace.path(
        "create_empty_load_db",
        "database_v1_loaded",
        "actor",
        "reference",
    )

    assert read_to_loaded.count() == 1
    assert create_to_loaded.count() == 1

    # Parallel checks consume common States but have no represented handoff.
    # That absence is not a proof that exchanging them preserves all values.
    parallel_checks = challenge_trace.path(
        "is_match_table_row_count",
        "is_match_table_column_ranges",
        "actor",
        "actor",
    )
    assert not parallel_checks.exists()


def test_core_queries_1_5_and_13_expose_file_to_database_structure(
    challenge_trace,
):
    entry_to_loaded = challenge_trace.path(
        "csv_file_entry",
        "database_v1_loaded",
        "reference",
        "reference",
    )
    root_to_computed = challenge_trace.path(
        "csv_root_path",
        "database_v2_computed",
        "reference",
        "reference",
    )

    assert entry_to_loaded.count() == 2
    assert root_to_computed.count() == 2
    assert root_to_computed.execution_route_count() == 2


def test_downstream_starts_from_the_selected_state_not_sibling_outputs(
    challenge_trace,
):
    stream = challenge_trace.downstream(
        "csv_root_path",
        "reference",
    )
    root_edges = stream.stream["unknown"]["root"]

    assert root_edges
    assert {
        edge.produced_ref
        for edge in root_edges
    } == {scoped("s:csv_root_path")}

    all_edges = [
        edge
        for layers in stream.stream.values()
        for edges in layers.values()
        for edge in edges
    ]
    assert not any(
        edge.produced_ref == scoped("s:job_id")
        for edge in all_edges
    )
    assert len({edge_identity(edge) for edge in all_edges}) == len(
        all_edges
    )


def test_upstream_preserves_both_real_fan_in_branches(challenge_trace):
    stream = challenge_trace.upstream(
        "database_v2_computed",
        "reference",
    )
    edge_refs = {
        edge.produced_ref
        for layers in stream.stream.values()
        for edges in layers.values()
        for edge in edges
    }

    assert scoped("s:csv_root_path") in edge_refs
    assert scoped("s:job_id") in edge_refs
    assert stream.truncated is False


def test_value_lookup_exposes_only_the_template_opaque_state(
    challenge_trace,
    capsys,
):
    reference = scoped("s:database_v2_computed")
    value = challenge_trace.value_of(reference, show=True)
    shown = capsys.readouterr().out

    assert isinstance(value, list) and len(value) == 1
    assert "TIDEModelState" in repr(value[0])
    assert "VALUE" in shown
    assert reference in shown


def test_all_public_inspection_results_render_and_return_json(
    challenge_trace,
):
    exact_actor = challenge_trace.find(
        "load_csv_file_into_table"
    ).first("actor")
    value_hash = challenge_trace.index.value_hash_by_ref_version[
        (scoped("s:database_v2_computed"), 0)
    ]
    results = {
        "find": challenge_trace.find("database_v2_computed"),
        "find_actor_identity": challenge_trace.find(
            "e:08:load_csv_file_into_table"
        ),
        "find_value": challenge_trace.find(value_hash),
        "inspect": challenge_trace.inspect(
            "database_v2_computed",
            "reference",
        ),
        "inspect_exact_actor": exact_actor,
        "investigate": challenge_trace.investigate(
            "database_v2_computed",
            "reference",
        ),
        "investigate_actor": challenge_trace.investigate(
            "load_csv_file_into_table",
            "actor",
        ),
        "produced_by": challenge_trace.produced_by(
            "database_v2_computed",
            "reference",
        ),
        "consumed_by": challenge_trace.consumed_by(
            "database_v2_computed",
            "reference",
        ),
        "produced_by_actor": challenge_trace.produced_by(exact_actor),
        "consumed_by_actor": challenge_trace.consumed_by(exact_actor),
        "upstream": challenge_trace.upstream(
            "database_v2_computed",
            "reference",
        ),
        "downstream": challenge_trace.downstream(
            "csv_root_path",
            "reference",
        ),
        "path": challenge_trace.path(
            "database_v2_computed",
            "is_match_table_column_ranges",
            "reference",
            "actor",
        ),
    }

    for name, result in results.items():
        payload = result.to_dict()
        rendered = result.show(color=False)

        assert isinstance(payload, dict), name
        assert isinstance(rendered, str) and rendered.strip(), name
        json.dumps(payload)

        compared = result.diff(result)
        assert compared.same, name
        json.dumps(compared.to_dict())


def test_assign_unknown_context_survives_diff_and_bundle_round_trip(
    challenge_trace,
):
    restored = TIDE.from_bundle(challenge_trace.to_bundle())

    assert challenge_trace.diff(challenge_trace).same
    assert restored.diff(challenge_trace).same
    assert restored.model_context["assigned_unknown_states"] == [
        "s:csv_root_path",
        "s:job_id",
    ]
    assert restored.path(
        "database_v2_computed",
        "is_match_table_column_ranges",
        "reference",
        "actor",
    ).count() == 1


def test_absent_pc3_evidence_is_not_silently_presented_as_an_answer(
    challenge_source,
    challenge_trace,
):
    # These are the evidence families required by the tutorial questions but
    # absent from this file/table relation template.  Their absence protects
    # the boundary between a structural candidate and a full PC3 answer.
    absent_evidence = {
        "P2Detection",
        "ImageMeta",
        "FrameMeta",
        "zoneGroup 1108",
        "workflow_execution_id",
        "input_data_set_name",
        "execution_status",
        "execution_timestamp",
        "data_granularity",
        "DisplayPlot",
    }

    assert set(challenge_source).isdisjoint({
        "values",
        "statuses",
        "timestamps",
        "control_flow",
        "workflow_runs",
        "domain_joins",
        "plot_results",
        "granularities",
    })
    assert all(
        not challenge_trace.find(item).any()
        for item in absent_evidence
    )
