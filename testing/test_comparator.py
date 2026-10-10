import json

import pytest

from tide_tooling import TIDE, TIDEModelComparisonReport, Trace


K1 = {"E": ["p", "c"], "S": ["s"], "I": [["c", "s"]], "O": [["p", "s"]]}
K2 = {"E": ["P", "C"], "S": ["S"], "I": [["C", "S"]], "O": [["P", "S"]]}
E_MAP = {"p": "P", "c": "C"}
S_MAP = {"s": "S"}


def compare(source=K1, target=K2, execution_map=E_MAP, state_map=S_MAP):
    trace = source if isinstance(source, Trace) else TIDE.from_model(source)
    return trace.compare(
        target,
        mapping="supplied",
        execution_map=execution_map,
        state_map=state_map,
    )


def test_trace_compare_matches_mapped_models():
    result = compare()

    assert isinstance(result, TIDEModelComparisonReport)
    assert result.assessable is True
    assert result.conformant is True
    assert result.complete_agreement is True
    assert result.judgments_correspond is True
    assert result["models"]["source"]["semantically_valid"] is True
    assert result["models"]["source"]["judgments"]["direct_handoffs"] == [{
        "producer_exec_id": "p",
        "state_id": "s",
        "consumer_exec_id": "c",
    }]


def test_compare_uses_set_semantics_and_accepts_all_incidence_forms():
    source = {
        "host": "source-host", "E": ["p", "c", "p"], "S": ["s", "s"],
        "I": [["c", "s"], ["c", "s"]],
        "O": [{"exec_id": "p", "state_id": "s"}],
        "states": {"s": {"value": "not part of K"}},
    }
    target = {
        "host": "target-host", "E": ["P", "C"], "S": ["S"],
        "I": [{"e": "C", "s": "S"}], "O": [["P", "S"]],
        "host_record": {"native": "ignored enrichment"},
    }
    result = compare(source, target)

    assert result.complete_agreement is True
    assert result["models"]["source"]["counts"]["inputs"] == 1
    assert result["models"]["source"]["host"] == "source-host"
    assert result["models"]["target"]["host"] == "target-host"


def test_compare_normalizes_trace_dict_json_text_bytes_and_path(tmp_path):
    source_trace = TIDE.from_model(K1)
    target_path = tmp_path / "target.json"
    target_path.write_text(json.dumps(K2), encoding="utf-8")

    assert compare(source_trace, target_path).complete_agreement is True
    assert compare(K1, json.dumps(K2)).complete_agreement is True
    assert compare(json.dumps(K1), json.dumps(K2).encode()).complete_agreement is True


def test_partial_mapping_reports_combined_outside_scope():
    source = {
        "E": K1["E"] + ["q", "x"], "S": K1["S"] + ["u"],
        "I": K1["I"] + [["x", "u"]], "O": K1["O"] + [["q", "u"]],
    }
    target = {
        "E": K2["E"] + ["Q", "X"], "S": K2["S"] + ["U"],
        "I": K2["I"] + [["X", "U"]], "O": K2["O"] + [["Q", "U"]],
    }
    result = compare(source, target)

    assert result.conformant is True
    assert result.complete_agreement is False
    assert result["identity_coverage"] == {"source": False, "target": False}
    assert result["outside"]["source"] == {
        "executions": ["q", "x"], "states": ["u"],
        "I": [["x", "u"]], "O": [["q", "u"]],
    }


def test_preservation_and_reflection_failures_are_directional_deltas():
    missing = compare(target={"E": ["C"], "S": ["S"], "I": [["C", "S"]], "O": []})
    unsupported = compare(
        target={
            "E": ["P", "C", "Q"], "S": ["S", "T"],
            "I": [["C", "S"]], "O": [["P", "S"], ["Q", "T"]],
        },
        execution_map={**E_MAP, "q": "Q"}, state_map={**S_MAP, "t": "T"},
    )

    assert missing.conformant is False
    assert missing["deltas"]["O_minus"] == [["P", "S"]]
    assert missing["mapped_identities_absent"]["target"]["executions"] == ["P"]
    assert unsupported.conformant is False
    assert unsupported["deltas"]["O_plus"] == [["Q", "T"]]


def test_validity_is_independent_of_mapped_conformance():
    cycle = {
        "E": ["a", "b"], "S": ["s0", "s1"],
        "I": [["a", "s1"], ["b", "s0"]],
        "O": [["a", "s0"], ["b", "s1"]],
    }
    result = TIDE.from_model(cycle).compare(cycle, mapping="literal")

    assert result["models"]["source"]["semantically_valid"] is False
    assert result["models"]["source"]["unique_creation"] is True
    assert result["models"]["source"]["acyclic_ancestry"] is False
    assert result.conformant is True
    assert result.judgments_correspond is True


def test_structural_and_literal_mapping_modes_are_explicit():
    structural = TIDE.from_model(K1).compare(K2, mapping="structural")
    literal = TIDE.from_model(K1).compare(K2, mapping="literal")

    assert structural.complete_agreement is True
    assert literal.conformant is False
    assert literal.complete_agreement is False
    assert literal["identity_coverage"] == {"source": False, "target": False}


def test_bad_supplied_maps_are_unassessable_and_maps_require_supplied_mode():
    with pytest.raises(TypeError, match="Explicit maps require mapping='supplied'"):
        TIDE.from_model(K1).compare(K2, execution_map=E_MAP, state_map=S_MAP)

    report = compare(execution_map={"p": "P", "c": "P"})
    assert report.assessable is False
    assert report.conformant is None
    assert report.complete_agreement is None
    assert "Execution map must be one-to-one" in report["mapping_errors"]


@pytest.mark.parametrize(
    "candidate, message",
    [
        ({"I": []}, "missing E, S, O"),
        ({"E": [], "S": [], "I": {}, "O": []}, "I must be a list"),
        ({"E": [True], "S": [], "I": [], "O": []}, "E\\[0\\] must be a non-empty string or integer"),
        ("not-json-or-a-file", "Expecting value"),
    ],
)
def test_malformed_source_models_fail_at_trace_boundary(candidate, message):
    with pytest.raises(ValueError, match=message):
        compare(source=candidate)


def test_report_show_uses_model_compare_style(capsys):
    report = compare(target={"E": ["C"], "S": ["S"], "I": [["C", "S"]], "O": []})
    summary = report.show(color=False)

    assert "TIDE MODEL COMPARE" in summary
    assert "MAPPED IDENTITIES ABSENT:" not in summary
    assert "MAPPED INCIDENCE:" not in summary

    shown = report.show(color=False, simple=True, full=True)

    assert shown in capsys.readouterr().out
    assert "TIDE MODEL COMPARE" in shown
    assert "NON-CONFORMANT" in shown
    assert "MAPPED INCIDENCE:" in shown
    assert "O_minus:" in shown
    assert "- P → S" in shown
    assert "mapping: supplied" in shown
    assert "MAPPED IDENTITIES ABSENT:" in shown
    assert "! P" in shown
    assert shown.index("MAPPED IDENTITIES ABSENT:") < shown.index("MAPPED INCIDENCE:")
    complete = report.show(color=False, simple=False, full=True)
    assert complete.index("I_minus:") < complete.index("O_minus:") < complete.index("I_plus:")
    assert "\x1b[" not in shown


def test_declared_carriers_control_outside_and_complete_agreement():
    target = {
        "E": ["P", "C", "isolated"], "S": ["S"],
        "I": [["C", "S"]], "O": [["P", "S"]],
    }
    result = compare(target=target)

    assert result.conformant is True
    assert result.complete_agreement is False
    assert result["outside"]["target"]["executions"] == ["isolated"]
    assert result["models"]["target"]["counts"]["executions"] == 3
    assert "OUTSIDE MAPPING SCOPE:" in result.show(
        color=False,
        simple=True,
        full=True,
    )


def test_incidence_endpoints_must_belong_to_declared_carriers():
    with pytest.raises(ValueError, match="outside E"):
        TIDE.from_model({"E": ["p"], "S": ["s"], "I": [["c", "s"]], "O": [["p", "s"]]})
