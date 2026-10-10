"""Integer and string identities must remain distinct at model boundaries."""

import json

import pytest

from tide_tooling import TIDE
from tide_tooling.tooling_core.engine.tide_model import TIDEModel


MODEL = {
    "E": [42, "42", 0],
    "S": [42, "42"],
    "I": [[0, 42], [0, "42"]],
    "O": [[42, 42], ["42", "42"]],
}


def assert_kernel(model):
    assert set(model["E"]) == {42, "42", 0}
    assert set(model["S"]) == {42, "42"}
    for relation in ("I", "O"):
        assert {(edge["exec_id"], edge["state_id"]) for edge in model[relation]} == {
            tuple(edge) for edge in MODEL[relation]
        }


def test_mixed_ids_validate_and_round_trip_through_trace():
    model = json.loads(json.dumps(MODEL))
    assert TIDE.report_model_validity(model)["semantically_valid"]
    trace = TIDE.from_model(model)
    assert {record.exec_id for record in trace.log} == {42, "42", 0}
    stored_targets = {
        write.stored_target
        for record in trace.log
        for write in record.writes
    }
    assert stored_targets == {"__model_int__:42", "__model__:42"}
    assert_kernel(trace.to_model(enrich=False))


def test_carriers_accept_supported_containers_and_sort_by_type_then_value():
    expected = [2, 10, "10", "2"]
    for carrier in (["2", 10, "10", 2], ("2", 10, "10", 2), {"2", 10, "10", 2}, frozenset({"2", 10, "10", 2})):
        assert TIDEModel.normalise_carrier(carrier, "E") == expected


def test_mixed_ids_canonicalise_without_merging_and_keep_original_types():
    canonical = TIDEModel.canonicalise(MODEL)
    maps = {
        field: {
            enrichment["original_id"]: canonical_id
            for canonical_id, enrichment in canonical[field].items()
        }
        for field in ("executions", "states")
    }
    for field, carrier in (("executions", "E"), ("states", "S")):
        assert len(set(maps[field].values())) == len(MODEL[carrier])
        for original, alias in maps[field].items():
            restored = canonical[field][alias]["original_id"]
            assert type(restored) is type(original)
            assert restored == original
    for relation in ("I", "O"):
        assert {tuple(edge) for edge in canonical[relation]} == {
            (maps["executions"][execution], maps["states"][state])
            for execution, state in MODEL[relation]
        }


def test_mixed_ids_round_trip_through_bundle():
    trace = TIDE.from_model(MODEL)
    restored = TIDE.from_bundle(trace.to_bundle().to_dict())
    assert_kernel(restored.to_model(enrich=False))


def test_find_integer_collects_exact_id_and_sequence_id():
    trace = TIDE.from_model({
        "E": [1, 2],
        "S": [],
        "I": [],
        "O": [],
    })

    assert {match.id for match in trace.find(1).execs} == {1, 2}


def test_exact_integer_execution_id_wins_over_sequence_locator():
    trace = TIDE.from_model({
        "E": [1, 2],
        "S": [],
        "I": [],
        "O": [],
    })

    assert trace.inputs_outputs_of(1).execution.exec_id == 1


def test_model_validity_show_quotes_string_ids(capsys):
    rendered = TIDE.report_model_validity(MODEL).show(color=False)
    capsys.readouterr()

    assert "42 --42--> 0" in rendered
    assert '"42" --"42"--> 0' in rendered


def test_mixed_ids_are_accepted_in_comparison_maps():
    result = TIDE.from_model(MODEL).compare(
        MODEL,
        mapping="supplied",
        execution_map={identity: identity for identity in MODEL["E"]},
        state_map={identity: identity for identity in MODEL["S"]},
    )
    assert result.complete_agreement


@pytest.mark.parametrize("identity", [True, False, 1.0, None, ""])
def test_non_id_types_remain_structurally_invalid(identity):
    report = TIDE.report_model_validity(
        {"E": [identity], "S": [], "I": [], "O": []}
    )
    assert not report["structurally_valid"]
    assert report["errors"]
