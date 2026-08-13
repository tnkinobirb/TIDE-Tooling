import json

import pytest

import tide_tooling as tide_package
from tide_tooling import TIDE, TIDEComparatorReport, comparator


K1 = {"I": [["c", "s"]], "O": [["p", "s"]]}
K2 = {"I": [["C", "S"]], "O": [["P", "S"]]}
E_MAP = {"p": "P", "c": "C"}
S_MAP = {"s": "S"}


def compare(source=K1, target=K2, execution_map=E_MAP, state_map=S_MAP):
    return comparator(
        source,
        target,
        execution_map=execution_map,
        state_map=state_map,
    )


def test_package_comparator_matches_reference_positive_case():
    result = compare()

    assert tide_package.comparator is comparator
    assert isinstance(result, TIDEComparatorReport)
    assert result.assessable is True
    assert result.conformant is True
    assert result.whole_kernel_equal is True
    assert result.mapped_judgments_correspond is True
    assert result["validity"]["K1"]["valid"] is True
    assert result["judgments"]["K1"]["direct_handoffs"] == (
        ("p", "s", "c"),
    )
    assert result.to_dict()["judgments"]["K1"]["direct_handoffs"] == [
        ["p", "s", "c"],
    ]


def test_comparator_uses_set_semantics_and_accepts_all_incidence_forms():
    source = {
        "host": "source-host",
        "I": [
            ["c", "s"],
            ["c", "s"],
        ],
        "O": [{"exec_id": "p", "state_id": "s"}],
        "states": {"s": {"value": "not part of K"}},
    }
    target = {
        "host": "target-host",
        "I": [{"e": "C", "s": "S"}],
        "O": [["P", "S"]],
        "host_record": {"native": "ignored enrichment"},
    }

    result = compare(source, target)

    assert result.whole_kernel_equal is True
    assert result["validity"]["K1"]["counts"]["inputs"] == 1
    assert result["sources"] == {
        "K1": {"host": "source-host"},
        "K2": {"host": "target-host"},
    }


def test_comparator_normalizes_trace_dict_json_text_bytes_and_path(tmp_path):
    source_trace = TIDE.from_model(K1)
    target_path = tmp_path / "target.json"
    target_path.write_text(json.dumps(K2), encoding="utf-8")

    trace_path = compare(source_trace, target_path)
    dict_text = compare(K1, json.dumps(K2))
    text_bytes = compare(json.dumps(K1), json.dumps(K2).encode("utf-8"))

    assert trace_path.whole_kernel_equal is True
    assert dict_text.whole_kernel_equal is True
    assert text_bytes.whole_kernel_equal is True
    assert source_trace.comparator(
        K2,
        execution_map=E_MAP,
        state_map=S_MAP,
    ).whole_kernel_equal is True


def test_tide_instance_has_thin_comparator_convenience():
    tide = TIDE()
    try:
        report = tide.comparator(
            {"I": [], "O": []},
            execution_map={},
            state_map={},
        )
    finally:
        tide.close()

    assert report.assessable is True
    assert report.whole_kernel_equal is True


def test_partial_mapping_reports_outside_scope_without_breaking_conformance():
    source = {
        "I": K1["I"] + [["x", "u"]],
        "O": K1["O"] + [["q", "u"]],
    }
    target = {
        "I": K2["I"] + [["X", "U"]],
        "O": K2["O"] + [["Q", "U"]],
    }

    result = compare(source, target)

    assert result.conformant is True
    assert result.whole_kernel_equal is False
    assert result["carrier_coverage"] == {"K1": False, "K2": False}
    assert result["outside"]["K1"] == {
        "I": (("x", "u"),),
        "O": (("q", "u"),),
    }


def test_missing_and_unsupported_facts_are_directional_deltas():
    missing = compare(target={"I": [["C", "S"]], "O": []})
    unsupported = compare(
        target={
            "I": [["C", "S"]],
            "O": [["P", "S"], ["Q", "T"]],
        },
        execution_map={**E_MAP, "q": "Q"},
        state_map={**S_MAP, "t": "T"},
    )

    assert missing.assessable is True
    assert missing.conformant is False
    assert missing["deltas"]["O_minus"] == (("P", "S"),)
    assert missing["mapped_identities_absent"]["K2"]["Executions"] == (
        "P",
    )
    assert unsupported.conformant is False
    assert unsupported["deltas"]["O_plus"] == (("Q", "T"),)


def test_validity_is_reported_independently_of_mapped_conformance():
    cycle = {
        "I": [["a", "s1"], ["b", "s0"]],
        "O": [["a", "s0"], ["b", "s1"]],
    }
    result = comparator(
        cycle,
        cycle,
        execution_map={"a": "a", "b": "b"},
        state_map={"s0": "s0", "s1": "s1"},
    )

    assert result["validity"]["K1"]["valid"] is False
    assert result["validity"]["K1"]["unique_creation"] is True
    assert result["validity"]["K1"]["acyclic_ancestry"] is False
    assert result.conformant is True


def test_outside_path_does_not_enter_scoped_reachability():
    source = {
        "I": [["middle", "path_0"], ["end", "path_1"]],
        "O": [
            ["start", "anchor_start"],
            ["end", "anchor_end"],
            ["start", "path_0"],
            ["middle", "path_1"],
        ],
    }
    target = {"I": [], "O": [["a", "x"], ["b", "y"]]}
    result = comparator(
        source,
        target,
        execution_map={"start": "a", "end": "b"},
        state_map={"anchor_start": "x", "anchor_end": "y"},
    )

    assert ["start", "end"] in result.to_dict()["judgments"]["K1"][
        "reachability"
    ]
    assert result.conformant is True
    assert result["judgments"]["mapped_differences"][
        "reachability_missing"
    ] == ()


def test_mapping_is_mandatory_never_inferred_and_bad_maps_are_unassessable():
    with pytest.raises(TypeError):
        comparator(K1, K2)

    report = compare(execution_map={"p": "P", "c": "P"})

    assert report.assessable is False
    assert report.conformant is None
    assert report.whole_kernel_equal is None
    assert "Execution map must be one-to-one" in report["mapping_errors"]


@pytest.mark.parametrize(
    "candidate, message",
    [
        ({"I": []}, "must contain I and O"),
        ({"I": {}, "O": []}, "K1.I must be a JSON list"),
        ({"I": [["e", ""]], "O": []}, "State must be a non-empty string"),
        ("not-json-or-a-file", "Could not load K1 JSON"),
    ],
)
def test_malformed_model_inputs_fail_at_normalization_boundary(candidate, message):
    with pytest.raises(ValueError, match=message):
        compare(source=candidate)


def test_report_show_matches_tide_diff_style(capsys):
    report = compare(target={"I": [["C", "S"]], "O": []})

    shown = report.show(color=False, simple=True)
    captured = capsys.readouterr().out

    assert shown in captured
    assert "TIDE COMPARATOR" in shown
    assert "NON-CONFORMANT" in shown
    assert "DIRECT HANDOFFS MISSING:" in shown
    assert "MAPPED KERNEL DIFFERENCES:" in shown
    assert "O_minus:" in shown
    assert "- P -> S" in shown
    assert "explicit supplied Execution and State maps" in shown
    assert "\x1b[" not in shown
