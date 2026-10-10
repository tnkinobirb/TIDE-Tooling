import pytest

from tide_tooling import TIDE
from tide_tooling.core import tide_value_hasher


# ================================================================
# VALUE / REF IDENTITY TESTS
# ================================================================
#
# Focus:
#   inspect/find/value-helper behavior.
#
# Static/store tests already cover most path/store-scope basics.
# Version tests already cover exact version routing.
#
# This file should lock the value-level invariant:
#   same value can appear at multiple ref/store/version identities
#   without collapsing those identities.
# ================================================================


def value(trace, raw_value):
    return trace.inspect(raw_value, "value")


def refs_as_set(by_result):
    return set(by_result.refs)


@pytest.fixture
def same_value_two_store_trace():
    tide = TIDE()
    tide.ocean.register_store("testing")

    tide.event("WriteGlobalA", writes={"@A": "same"})
    tide.event("WriteTestingA", writes={"testing:@A": "same"})
    tide.event("UseGlobalA", reads={"@A": "same"})
    tide.event("UseTestingA", reads={"testing:@A": "same"})

    return tide.trace()


def test_value_inspect_history_preserves_same_value_across_store_refs(same_value_two_store_trace):
    inspected_value = value(same_value_two_store_trace, "same")

    # NOTE:
    # Value history should be value appearances as ref/version pairs.
    # If the container type changes, keep the semantic check:
    # both scoped refs must remain separately represented.
    assert ("global:@A", 0) in inspected_value.history
    assert ("testing:@A", 0) in inspected_value.history


def test_produced_by_value_preserves_same_value_across_store_refs(same_value_two_store_trace):
    inspected_value = value(same_value_two_store_trace, "same")
    produced = same_value_two_store_trace.produced_by(inspected_value)

    assert refs_as_set(produced) == {
        ("global:@A", 0),
        ("testing:@A", 0),
    }


def test_consumed_by_value_preserves_same_value_across_store_refs(same_value_two_store_trace):
    inspected_value = value(same_value_two_store_trace, "same")
    consumed = same_value_two_store_trace.consumed_by(inspected_value)

    assert refs_as_set(consumed) == {
        ("global:@A", 0),
        ("testing:@A", 0),
    }


def test_find_value_hash_returns_value_with_all_ref_appearances(same_value_two_store_trace):
    value_hash = tide_value_hasher("same")
    searched = same_value_two_store_trace.find(value_hash)

    assert searched.exists("hash") == (True, False)

    found = searched.first("value")

    assert found.id == value_hash
    assert ("global:@A", 0) in found.history
    assert ("testing:@A", 0) in found.history


def test_raw_string_find_does_not_become_exact_value_lookup(same_value_two_store_trace):
    searched = same_value_two_store_trace.find("same")

    # NOTE:
    # Current find contract: raw string lookup is for ref/actor-ish search.
    # Exact value search is by hash; inspect(raw_value, "value") is explicit.
    assert searched.values == []


def test_trace_show_can_include_recorded_values(same_value_two_store_trace):
    shown = same_value_two_store_trace.show(values=True, color=False)

    assert "str 'same' as global:@A[v0]" in shown


def test_investigate_show_can_include_recorded_values(same_value_two_store_trace):
    shown = same_value_two_store_trace.investigate(
        "global:@A",
        "reference",
    ).show(values=True, color=False)

    assert "global:@A[v0] = str 'same'" in shown


def test_same_value_written_to_two_global_refs_preserves_two_value_appearances():
    tide = TIDE()

    tide.event("WriteA", writes={"@A": "same"})
    tide.event("WriteB", writes={"@B": "same"})

    trace = tide.trace()
    inspected_value = value(trace, "same")

    assert ("global:@A", 0) in inspected_value.history
    assert ("global:@B", 0) in inspected_value.history

    produced = trace.produced_by(inspected_value)

    assert refs_as_set(produced) == {
        ("global:@A", 0),
        ("global:@B", 0),
    }


def test_same_ref_different_versions_preserves_value_version_appearances():
    tide = TIDE()

    tide.event("WriteA0", writes={"@A": "v0"})
    tide.event("WriteA1", writes={"@A": "v1"})

    trace = tide.trace()

    v0 = value(trace, "v0")
    v1 = value(trace, "v1")

    assert ("global:@A", 0) in v0.history
    assert ("global:@A", 1) in v1.history

    assert refs_as_set(trace.produced_by(v0)) == {
        ("global:@A", 0),
    }
    assert refs_as_set(trace.produced_by(v1)) == {
        ("global:@A", 1),
    }
