import re
from contextlib import redirect_stdout
from io import StringIO

import pytest

from tide_tooling import (
    Actor,
    Compared,
    StackLink,
    TIDE,
    TIDEModelCompared,
    Trace,
)
from tide_tooling.tooling_core.engine.tide_logging import Investigate, Inspected, Object


ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def strip_ansi(value):
    return ANSI_RE.sub("", value)


def test_actor_diff_reports_semantic_field_changes():
    first = Actor("ocean", "run_1", "writer")
    same = first.diff(Actor("ocean", "run_1", "writer"))
    changed = first.diff(Actor("ocean", "run_2", "writer"))

    assert isinstance(same, Compared)
    assert same.same is True
    assert changed.same is False
    assert changed.changed["source"] == {
        "before": "run_1",
        "after": "run_2",
    }
    assert changed.to_dict()["object"] == "Actor"


def test_compared_show_uses_tide_style_and_supports_plain_output():
    compared = Actor("ocean", "run_1", "writer").diff(
        Actor("ocean", "run_2", "writer")
    )

    output = StringIO()
    with redirect_stdout(output):
        coloured = compared.show()
    plain = compared.show(color=False, simple=True)

    assert "DIFF" in coloured
    assert "CHANGED" in coloured
    assert "run_1" in plain
    assert "run_2" in plain
    assert "\x1b[" not in plain
    assert "DIFF" in strip_ansi(output.getvalue())


def test_to_dict_objects_have_tide_style_show_methods():
    actor = Actor("ocean", "run_1", "writer")
    link = StackLink({
        "affected_input": ("args", object()),
        "affected_input_index": 0,
        "affected_actor": actor,
        "affected_exec_id": "TIDE_EXEC:consumer",
        "producer_actor": actor,
        "producer_exec_id": "TIDE_EXEC:producer",
        "produced_ref": "global:@message",
        "produced_ref_version": 0,
    })

    actor_output = actor.show(color=False)
    link_output = link.show(color=False, simple=True)
    alias_output = actor.show(colour=False, simple=True)

    assert "ACTOR" in actor_output
    assert "STACK LINK" in link_output
    assert "\x1b[" not in actor_output
    assert "\x1b[" not in link_output
    assert "\x1b[" not in alias_output


def test_trace_diff_compares_execution_evidence_and_view_objects():
    first_tide = TIDE()
    first_tide.event("writer", writes={"@message": "first"})
    second_tide = TIDE()
    second_tide.event("writer", writes={"@message": "second"})

    first = first_tide.trace()
    second = second_tide.trace()
    compared = first.diff(second)
    evidence_compared = first.diff(second, TIDE=False)

    assert isinstance(first, Trace)
    assert isinstance(compared, TIDEModelCompared)
    assert compared.same is False
    assert "run_evidence" in first.to_dict()
    assert any(
        "value_hash" in path
        for path in evidence_compared.changed
    )

    first_bundle = first.to_bundle()
    assert first_bundle.diff(first_bundle).same is True
    assert "BUNDLE" in first_bundle.show(color=False, simple=True)

    inspected = first.inspect("global:@message", "reference", version=0)
    assert inspected.diff(inspected).same is True
    assert first.investigate(inspected).diff(
        first.investigate(inspected)
    ).same is True
    assert first.find("writer").diff(first.find("writer")).same is True


def test_bundle_round_trip_does_not_turn_none_content_ids_into_strings():
    tide = TIDE()
    tide.event("writer", writes={"@message": "stable"})
    original = tide.trace()
    restored = TIDE.from_bundle(original.to_bundle())

    assert original.name == "TIDE: .trace()"
    assert restored.name == "TIDE: .trace()"
    assert all(record.content_id is None for record in restored.exec_log)

    changed_paths = restored.diff(original).changed
    assert not any("content_id" in path for path in changed_paths)


def test_diff_show_groups_execution_reads_and_writes_by_record():
    first_tide = TIDE()
    first_tide.event("writer", writes={"@a": "A"})
    second_tide = TIDE()
    second_tide.event("writer", writes={"@a": "A"})
    second_tide.event(
        "consumer",
        reads=["@a"],
        writes={"@b": "B"},
    )

    output = StringIO()
    with redirect_stdout(output):
        shown = first_tide.trace().diff(
            second_tide.trace(),
            TIDE=False,
        ).show(color=False)

    assert "run_evidence.exec_records[1]:" in shown
    assert "    reads:" in shown
    assert "    writes:" in shown
    assert "      reference: 'global:@b'" in shown
    assert "references[1]: 'global:@b'" in shown


def test_trace_diff_defaults_to_tide_model_and_judgment_sets():
    before = TIDE.from_model({
        "I": [["e1", "s0"], ["e2", "s1"]],
        "O": [
            ["unknown", "s0"],
            ["e1", "s1"],
            ["e2", "s2"],
        ],
    })
    after = TIDE.from_model({
        "I": [
            ["e1", "s0"],
            ["e2", "s1"],
            ["e3", "s2"],
        ],
        "O": [
            ["unknown", "s0"],
            ["e1", "s1"],
            ["e2", "s2"],
            ["e3", "s3"],
        ],
    })

    compared = before.diff(after)
    data = compared.to_dict()

    assert isinstance(compared, TIDEModelCompared)
    assert isinstance(compared, Compared)
    assert compared.same is False
    assert compared.same_kernel is False
    assert compared.same_judgments is False
    assert set(data["kernel"]) == {"I", "O"}
    assert data["kernel"]["I"]["added"] == [{
        "exec_id": "e3",
        "state_id": "s2",
    }]
    assert data["judgments"]["direct_handoffs"]["added"] == [{
        "producer_exec_id": "e2",
        "state_id": "s2",
        "consumer_exec_id": "e3",
    }]
    assert data["judgments"]["reachability"]["added"] == [
        {
            "source_exec_id": "e1",
            "target_exec_id": "e3",
        },
        {
            "source_exec_id": "e2",
            "target_exec_id": "e3",
        },
        {
            "source_exec_id": "unknown",
            "target_exec_id": "e3",
        },
    ]


def test_tide_model_diff_accepts_compact_incidence_dictionaries():
    before = TIDE.from_model({
        "I": [{"e": "e2", "s": "s1"}],
        "O": [{"e": "e1", "s": "s1"}],
    })
    after = {
        "I": [
            {"e": "e2", "s": "s1"},
            {"e": "e3", "s": "s2"},
        ],
        "O": [
            {"e": "e1", "s": "s1"},
            {"e": "e2", "s": "s2"},
        ],
    }

    compared = before.diff(after)

    assert compared.to_dict()["kernel"]["I"]["added"] == [{
        "exec_id": "e3",
        "state_id": "s2",
    }]
    assert compared.to_dict()["kernel"]["O"]["added"] == [{
        "exec_id": "e2",
        "state_id": "s2",
    }]
    assert compared.same_judgments is False


def test_tide_model_diff_can_change_kernel_without_changing_judgments():
    before = TIDE.from_model({
        "I": [],
        "O": [["e1", "s0"]],
    })
    after = TIDE.from_model({
        "I": [],
        "O": [["e1", "s0"], ["unknown", "boundary"]],
    })

    compared = before.diff(after)

    assert compared.same is False
    assert compared.same_kernel is False
    assert compared.same_judgments is True
    assert compared.to_dict()["kernel"]["O"]["added"] == [{
        "exec_id": "unknown",
        "state_id": "boundary",
    }]


def test_tide_model_diff_treats_host_labels_as_context_only():
    model = {
        "E": ["e1", "e2"],
        "S": ["s1"],
        "I": [["e2", "s1"]],
        "O": [["e1", "s1"]],
    }
    before = TIDE.from_model({**model, "host": "host-a"})
    after = TIDE.from_model({**model, "host": "host-b"})

    compared = before.diff(after)

    assert compared.same is True
    assert compared.same_kernel is True
    assert compared.same_judgments is True
    assert compared.to_dict()["before"]["host"] == "host-a"
    assert compared.to_dict()["after"]["host"] == "host-b"


def test_tide_model_diff_defaults_to_literal_host_identity():
    before = TIDE.from_model({
        "I": [["left-consume", "left-state"]],
        "O": [["left-produce", "left-state"]],
    })
    after = TIDE.from_model({
        "I": [["right-consume", "right-state"]],
        "O": [["right-produce", "right-state"]],
    })

    compared = before.diff(after)

    assert compared.same is False
    assert compared.same_kernel is False
    assert ("left-produce", "left-state") in compared.kernel["O"]["removed"]
    assert ("right-produce", "right-state") in compared.kernel["O"]["added"]


def test_tide_model_diff_structural_view_is_an_explicit_toggle():
    before = TIDE.from_model({
        "I": [["left-consume", "left-state"]],
        "O": [["left-produce", "left-state"]],
        "states": {"left-state": {"value": "left-value"}},
    })
    after = {
        "I": [["right-consume", "right-state"]],
        "O": [["right-produce", "right-state"]],
        "states": {"right-state": {"value": "different-value"}},
        "host_record": {"native": "ignored evidence"},
    }

    compared = before.diff(after, identity_mode="structural")

    assert compared.same is True
    assert compared.same_kernel is True
    assert compared.same_judgments is True
    assert compared.before["I"] == [{
        "exec_id": "exec_1",
        "state_id": "state_0",
    }]
    assert compared.after["O"] == [{
        "exec_id": "exec_0",
        "state_id": "state_0",
    }]
    assert compared.identity_basis == (
        "canonical I/O incidence structure "
        "(values and enrichment excluded)"
    )
    assert "states" not in compared.before
    assert "host_record" not in compared.after


def test_trace_diff_rejects_structural_identity_for_object_diff():
    trace = TIDE().trace()

    with pytest.raises(
        ValueError,
        match="structural identity requires TIDE=True",
    ):
        trace.diff(trace, TIDE=False, identity_mode="structural")


def test_tide_model_diff_show_is_relationship_first_and_tide_styled():
    before = TIDE.from_model({
        "host": "before-host",
        "I": [["e2", "s1"]],
        "O": [["e1", "s1"]],
    })
    after = TIDE.from_model({
        "host": "after-host",
        "I": [["e2", "s2"]],
        "O": [["e1", "s1"], ["unknown", "s2"]],
    })

    compared = before.diff(after)
    coloured = compared.show()
    plain = compared.show(color=False, simple=True)

    assert "TIDE DIFF" in coloured
    assert "\x1b[" in coloured
    assert "\x1b[" not in plain
    assert "DIRECT HANDOFFS:" in plain
    assert "REACHABILITY:" in plain
    assert "KERNEL:" in plain
    assert plain.index("DIRECT HANDOFFS:") < plain.index("KERNEL:")
    assert "- e1 --s1--> e2" in plain
    assert "literal Execution and State identities in I/O" in plain
    assert "host: before-host → after-host" in plain


def test_trace_diff_tide_false_preserves_full_object_diff():
    first = TIDE()
    first.event("writer", writes={"@message": "first"})
    second = TIDE()
    second.event("writer", writes={"@message": "second"})

    compared = first.trace().diff(second.trace(), TIDE=False)

    assert type(compared) is Compared
    assert compared.object_type == "Trace"
    assert any("value_hash" in path for path in compared.changed)


def test_trace_diff_two_value_form_ignores_tide_model_default():
    trace = TIDE().trace()

    compared = trace.diff({"value": 1}, {"value": 2})

    assert type(compared) is Compared
    assert compared.changed["value"] == {
        "before": 1,
        "after": 2,
    }


def test_trace_diff_rejects_non_boolean_tide_toggle():
    trace = TIDE().trace()

    try:
        trace.diff(trace, TIDE="yes")
    except TypeError as error:
        assert "TIDE must be a bool" in str(error)
    else:
        raise AssertionError("non-boolean TIDE toggle was accepted")


def test_diff_show_keeps_scalar_bundle_context_values_on_their_field_line():
    first = {
        "bundle_context": {
            "codec_version": 1,
            "kind": "tide_trace_values",
            "opaque_value_hashes": [],
            "value_count": 4,
        }
    }
    second = {}

    from tide_tooling.tooling_core.engine.tide_logging import _build_compared

    output = _build_compared(first, second).show(color=False)

    assert "bundle_context:\n    codec_version: 1" in output
    assert "bundle_context:\n    codec_version:\n1" not in output
    assert "    value_count: 4" in output


def test_diff_show_identifies_container_type_changes():
    from tide_tooling.tooling_core.engine.tide_logging import _build_compared

    output = _build_compared(
        [[{"value": "10"}]],
        [{"count": 1, "total": 10}],
    ).show(color=False)

    assert "[0]:" in output
    assert "type: list -> dict" in output
    assert "before:" in output
    assert "after:" in output


def test_trace_diff_two_value_form_identifies_value_shape_change():
    trace = TIDE().trace()

    output = trace.diff(
        [[{"value": "10"}]],
        [{"count": 1, "total": 10}],
    ).show(color=False)

    assert "DIFF | list | 1 changes" in output
    assert "[0]:" in output
    assert "type: list -> dict" in output


def test_trace_diff_resolves_loose_typed_references_to_values():
    tide = TIDE()
    tide.event("load", writes={"raw_data": [{"value": "10"}]})
    tide.event(
        "process",
        reads=["raw_data"],
        writes={"processed_data": {"count": 1, "total": 10}},
    )
    trace = tide.trace()

    compared = trace.diff(
        "raw_data",
        "processed_data",
        "reference",
        "reference",
    )

    expected = trace.diff(
        trace.value_of("global:raw_data"),
        trace.value_of("global:processed_data"),
    )
    assert compared.to_dict() == expected.to_dict()
    assert compared.same is False
    assert compared.changed["[0]"] == {
        "before": [{"value": "10"}],
        "after": {"count": 1, "total": 10},
    }


def test_trace_diff_auto_detects_scoped_references_and_hashes():
    tide = TIDE()
    tide.event("first", writes={"value_a": "A"})
    tide.event("second", writes={"value_b": "B"})
    trace = tide.trace()

    by_reference = trace.diff("global:value_a", "global:value_b")
    by_hash = trace.diff(
        trace.index.value_hash_by_ref_version[("global:value_a", 0)],
        trace.index.value_hash_by_ref_version[("global:value_b", 0)],
    )

    assert by_reference.same is False
    assert by_hash.same is False
    assert by_reference.to_dict() == trace.diff(["A"], ["B"]).to_dict()
    assert by_hash.to_dict() == trace.diff("A", "B").to_dict()


def test_trace_diff_reference_versions_select_exact_values():
    tide = TIDE()
    tide.event("first", writes={"counter": 1})
    tide.event("second", writes={"counter": 2})
    trace = tide.trace()

    compared = trace.diff(
        "counter",
        "counter",
        "reference",
        "reference",
        0,
        1,
    )

    assert compared.to_dict() == trace.diff(1, 2).to_dict()


def test_trace_diff_loose_reference_uses_find_first_match():
    tide = TIDE()
    tide.event("global", writes={"global:item": "global"})
    tide.event("memory", writes={"mem:item": "memory"})
    trace = tide.trace()

    assert trace.diff("item", "item", "reference", "reference").same is True


def test_trace_diff_loose_actor_uses_find_first_match():
    tide = TIDE()
    tide.event("load data", writes={"raw": "data"})
    tide.event("process data", reads=["raw"])
    trace = tide.trace()

    compared = trace.diff(
        "load",
        "process",
        "actor",
        "actor",
    )

    assert compared.same is False
    assert compared.object_type == "Actor"


def test_inspected_diff_defaults_to_underlying_object_with_wrapper_opt_in():
    left_object = Object(Actor(0, "run_1", "writer"), "actor", "writer")
    right_object = Object(Actor(0, "run_2", "writer"), "actor", "writer")
    left = Inspected(left_object, history=[0])
    right = Inspected(right_object, history=[1])

    underlying = left.diff(right)
    wrapper = left.diff(right, diff_object=False)

    assert underlying.object_type == "Actor"
    assert underlying.changed["source"] == {
        "before": "run_1",
        "after": "run_2",
    }
    assert wrapper.object_type == "Inspected"
    assert any(path.startswith("history") for path in wrapper.changed)

    trace = TIDE().trace()
    left_investigation = Investigate(trace, trace.inspect, left)
    right_investigation = Investigate(trace, trace.inspect, right)

    assert left_investigation.diff(right_investigation).object_type == "Actor"
    assert (
        left_investigation.diff(
            right_investigation,
            diff_object=False,
        ).object_type
        == "Investigate"
    )
