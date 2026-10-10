import re
from contextlib import redirect_stdout
from io import StringIO

from tide_tooling import Actor, Compared, TIDE, Trace
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
    assert changed.changed["source"] == {"before": "run_1", "after": "run_2"}
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


def test_actor_has_tide_style_show_method():
    actor = Actor("ocean", "run_1", "writer")

    actor_output = actor.show(color=False)
    alias_output = actor.show(colour=False, simple=True)

    assert "ACTOR" in actor_output
    assert "\x1b[" not in actor_output
    assert "\x1b[" not in alias_output


def test_trace_diff_is_an_ordinary_type_aware_object_diff():
    first_tide = TIDE()
    first_tide.event("writer", writes={"@message": "first"})
    second_tide = TIDE()
    second_tide.event("writer", writes={"@message": "second"})

    first = first_tide.trace()
    second = second_tide.trace()
    compared = first.diff(second)

    assert isinstance(first, Trace)
    assert type(compared) is Compared
    assert compared.object_type == "Trace"
    assert compared.same is False
    assert "run_evidence" not in first.to_dict()
    assert any(
        "records" in path
        for path in (*compared.added, *compared.removed)
    )

    first_bundle = first.to_bundle()
    assert first_bundle.diff(first_bundle).same is True
    assert "BUNDLE" in first_bundle.show(color=False, simple=True)

    inspected = first.inspect("global:@message", "reference", version=0)
    assert inspected.diff(inspected).same is True
    assert first.investigate(inspected).diff(first.investigate(inspected)).same is True
    assert first.find("writer").diff(first.find("writer")).same is True


def test_bundle_round_trip_does_not_turn_none_content_ids_into_strings():
    tide = TIDE()
    tide.event("writer", writes={"@message": "stable"})
    original = tide.trace()
    restored = TIDE.from_bundle(original.to_bundle())

    assert original.name == "TIDE: .trace()"
    assert restored.name == "TIDE: .trace()"
    assert all(record.content_id is None for record in restored.log)
    assert restored.diff(original).same is True


def test_diff_show_reports_trace_state_projection():
    first_tide = TIDE()
    first_tide.event("writer", writes={"@a": "A"})
    second_tide = TIDE()
    second_tide.event("writer", writes={"@a": "A"})
    second_tide.event("consumer", reads=["@a"], writes={"@b": "B"})

    output = StringIO()
    with redirect_stdout(output):
        shown = first_tide.trace().diff(second_tide.trace()).show(color=False)

    assert "records:" in shown
    assert "global:@b" in shown


def test_trace_diff_one_and_two_value_forms():
    trace = TIDE().trace()

    trace_comparison = trace.diff(TIDE().trace())
    value_comparison = trace.diff({"value": 1}, {"value": 2})

    assert type(trace_comparison) is Compared
    assert trace_comparison.object_type == "Trace"
    assert type(value_comparison) is Compared
    assert value_comparison.changed["value"] == {"before": 1, "after": 2}


def test_diff_show_keeps_scalar_bundle_context_values_on_their_field_line():
    first = {
        "bundle_context": {
            "codec_version": 1,
            "kind": "tide_trace_values",
            "opaque_value_hashes": [],
            "value_count": 4,
        }
    }
    from tide_tooling.tooling_core.engine.tide_logging import _build_compared

    output = _build_compared(first, {}).show(color=False)
    assert "bundle_context:\n    codec_version: 1" in output
    assert "bundle_context:\n    codec_version:\n1" not in output
    assert "    value_count: 4" in output


def test_diff_show_identifies_container_type_changes():
    from tide_tooling.tooling_core.engine.tide_logging import _build_compared

    output = _build_compared(
        [[{"value": "10"}]], [{"count": 1, "total": 10}]
    ).show(color=False)

    assert "[0]:" in output
    assert "type: list -> dict" in output
    assert "before:" in output
    assert "after:" in output


def test_trace_diff_two_value_form_identifies_value_shape_change():
    output = TIDE().trace().diff(
        [[{"value": "10"}]], [{"count": 1, "total": 10}]
    ).show(color=False)

    assert "DIFF | list | 1 difference" in output
    assert "[0]:" in output
    assert "type: list -> dict" in output


def test_trace_diff_resolves_loose_typed_references_to_values():
    tide = TIDE()
    tide.event("load", writes={"raw_data": [{"value": "10"}]})
    tide.event(
        "process", reads=["raw_data"],
        writes={"processed_data": {"count": 1, "total": 10}},
    )
    trace = tide.trace()
    compared = trace.diff("raw_data", "processed_data", "reference", "reference")
    expected = trace.diff(
        trace.value_of("global:raw_data"), trace.value_of("global:processed_data")
    )

    assert compared.to_dict() == expected.to_dict()
    assert compared.same is False
    assert compared.changed["[0]"] == {
        "before": [{"value": "10"}], "after": {"count": 1, "total": 10},
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

    assert by_reference.to_dict() == trace.diff(["A"], ["B"]).to_dict()
    assert by_hash.to_dict() == trace.diff("A", "B").to_dict()


def test_trace_diff_reference_versions_select_exact_values():
    tide = TIDE()
    tide.event("first", writes={"counter": 1})
    tide.event("second", writes={"counter": 2})
    trace = tide.trace()

    compared = trace.diff("counter", "counter", "reference", "reference", 0, 1)
    assert compared.to_dict() == trace.diff(1, 2).to_dict()


def test_trace_diff_loose_reference_and_actor_use_first_match():
    tide = TIDE()
    tide.event("load data", writes={"item": "global"})
    tide.event("process data", reads=["item"])
    trace = tide.trace()

    assert trace.diff("item", "item", "reference", "reference").same is True
    actors = trace.diff("load", "process", "actor", "actor")
    assert actors.same is False
    assert actors.object_type == "Actor"


def test_inspected_diff_defaults_to_underlying_object_with_wrapper_opt_in():
    left_object = Object(Actor(0, "run_1", "writer"), "actor", "writer")
    right_object = Object(Actor(0, "run_2", "writer"), "actor", "writer")
    left = Inspected(left_object, history=[0])
    right = Inspected(right_object, history=[1])

    underlying = left.diff(right)
    wrapper = left.diff(right, diff_object=False)
    assert underlying.object_type == "Actor"
    assert underlying.changed["source"] == {"before": "run_1", "after": "run_2"}
    assert wrapper.object_type == "Inspected"
    assert any(path.startswith("history") for path in wrapper.changed)

    trace = TIDE().trace()
    left_investigation = Investigate(trace, trace.inspect, left)
    right_investigation = Investigate(trace, trace.inspect, right)
    assert left_investigation.diff(right_investigation).object_type == "Actor"
