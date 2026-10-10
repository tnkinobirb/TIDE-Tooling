import pytest

from tide_tooling import TIDE
from tide_tooling.tooling_core.engine.errors import InvalidObjectKindError


# ================================================================
# API NEGATIVE / EMPTY RESULT TESTS
# ================================================================
#
# Gap area #9 from tide_next_gaps_1_3_5_7_9.txt.
#
# Revised contract:
#   Query APIs should mostly return honest empty/nonexistent state, not raise.
#
# That gives users a clean pattern:
#   result = trace.find(...)
#   result.exists(...)
#
# Or:
#   inspected = trace.inspect(...)
#   inspected.exists == False
#
# Only truly malformed API usage should raise, such as unknown object kind.
# ================================================================


def actor(trace, name, grouped=False):
    return trace.inspect(name, "actor", grouped=grouped)


def ref(trace, name, version=None):
    if version is None:
        return trace.inspect(name, "reference")

    return trace.inspect(name, "reference", version=version)

def assert_empty_search(searched, kind, nearby:bool=False):

    if nearby:
        assert searched.exists(kind) == (False, True)
    else:
        assert searched.exists(kind) == (False, False)


def assert_nonexistent_inspected(inspected, expected_type=None, expected_id=None):
    assert hasattr(inspected, "exists")
    assert inspected.exists is False

    if expected_type is not None:
        assert inspected.type == expected_type

    if expected_id is not None:
        assert inspected.id == expected_id


@pytest.fixture
def basic_trace():
    tide = TIDE()

    tide.event("A", writes={"@A": "A"})
    tide.event("UseA", reads={"@A": "A"}, writes={"@out": "out"})

    return tide.trace()


def test_find_missing_store_ref_returns_empty_not_global_fallback(basic_trace):
    searched = basic_trace.find("missing_store:@A", version=0)

    assert searched.refs == []
    assert searched.nearby.refs == []
    assert_empty_search(searched, "reference")


def test_find_missing_version_returns_empty_not_latest_fallback(basic_trace):
    searched = basic_trace.find("global:@A", version=999)

    assert searched.refs == []
    for inspected_ref in searched.nearby.refs: 
        assert inspected_ref == basic_trace.inspect("global:@A", "reference")
    assert_empty_search(searched, "reference")


def test_find_malformed_ref_string_returns_empty_not_fake_reference(basic_trace):
    searched = basic_trace.find("global::::@A::::bad")

    assert searched.refs == []
    assert_empty_search(searched, "reference")


def test_find_unknown_actor_returns_empty_not_nearby_success(basic_trace):
    searched = basic_trace.find("NotAnActor")

    assert searched.actors == []
    assert_empty_search(searched, "actor")


def test_find_missing_exec_id_returns_empty(basic_trace):
    searched = basic_trace.find("not-an-exec-id")
    
    assert searched.execs == []
    assert_empty_search(searched, "exec_record")


def test_find_missing_value_hash_returns_empty(basic_trace):
    searched = basic_trace.find(999999999999999999999999999999)

    assert searched.values == []
    assert_empty_search(searched, "value")


def test_inspect_missing_exact_ref_returns_nonexistent_object(basic_trace):
    inspected = basic_trace.inspect("global:@missing", "reference", version=0)
    print(inspected)
    assert_nonexistent_inspected(
        inspected,
        expected_type="reference",
        expected_id="global:@missing",
    )


def test_inspect_missing_ref_version_returns_nonexistent_not_latest(basic_trace):
    inspected = basic_trace.inspect("global:@A", "reference", version=999)

    assert_nonexistent_inspected(
        inspected,
        expected_type="reference",
        expected_id="global:@A",
    )


def test_inspect_missing_actor_returns_nonexistent_object(basic_trace):
    inspected = basic_trace.inspect("NotAnActor", "actor", grouped=False)

    assert_nonexistent_inspected(
        inspected,
        expected_type="actor",
    )


def test_inspect_missing_seq_id_returns_nonexistent_object(basic_trace):
    inspected = basic_trace.inspect(999, "seq_id")

    assert_nonexistent_inspected(
        inspected,
        expected_type="seq_id",
        expected_id=999,
    )


def test_inspect_missing_exec_record_returns_nonexistent_object(basic_trace):
    inspected = basic_trace.inspect("not-an-exec-id", "exec_record")

    assert_nonexistent_inspected(
        inspected,
        expected_type="exec_record",
        expected_id="not-an-exec-id",
    )


def test_investigate_missing_ref_returns_nonexistent_object(basic_trace):
    investigated = basic_trace.investigate("global:@missing", "reference", version=0)

    assert_nonexistent_inspected(
        investigated.inspected,
        expected_type="reference",
        expected_id="global:@missing",
    )


def test_investigate_missing_actor_returns_nonexistent_object(basic_trace):
    investigated = basic_trace.investigate("NotAnActor", "actor", grouped=False)

    assert_nonexistent_inspected(
        investigated.inspected,
        expected_type="actor",
    )


def test_investigate_missing_exec_record_is_an_empty_result(basic_trace):
    investigated = basic_trace.investigate(
        "not-an-exec-id",
        "exec_record",
    )

    assert investigated.to_dict()["history"] == []
    assert "not-an-exec-id" in investigated.show(color=False)


def test_path_from_nonexistent_ref_to_actor_returns_no_path(basic_trace):
    missing = basic_trace.inspect("global:@missing", "reference", version=0)
    use_a = actor(basic_trace, "UseA", grouped=False)

    path = basic_trace.path(missing, use_a)

    assert not path.exists()


def test_path_from_ref_to_nonexistent_actor_returns_no_path(basic_trace):
    a_ref = ref(basic_trace, "global:@A", version=0)
    missing_actor = basic_trace.inspect("NotAnActor", "actor", grouped=False)

    path = basic_trace.path(a_ref, missing_actor)

    assert not path.exists()


def test_consumed_by_nonexistent_ref_returns_empty_bybase(basic_trace):
    missing = basic_trace.inspect("global:@missing", "reference", version=0)

    consumed = basic_trace.consumed_by(missing)

    assert consumed.refs == []
    assert consumed.actors == []
    assert consumed.execs == []


def test_produced_by_nonexistent_ref_returns_empty_bybase(basic_trace):
    missing = basic_trace.inspect("global:@missing", "reference", version=0)

    produced = basic_trace.produced_by(missing)

    assert produced.refs == []
    assert produced.actors == []
    assert produced.execs == []


def test_consumed_by_nonexistent_actor_returns_empty_bybase(basic_trace):
    missing_actor = basic_trace.inspect("NotAnActor", "actor", grouped=False)

    consumed = basic_trace.consumed_by(missing_actor)

    assert consumed.refs == []
    assert consumed.actors == []
    assert consumed.execs == []


def test_produced_by_nonexistent_actor_returns_empty_bybase(basic_trace):
    missing_actor = basic_trace.inspect("NotAnActor", "actor", grouped=False)

    produced = basic_trace.produced_by(missing_actor)

    assert produced.refs == []
    assert produced.actors == []
    assert produced.execs == []


def test_to_dict_on_nonexistent_ref_is_structured_empty_state(basic_trace):
    inspected = basic_trace.inspect("global:@missing", "reference", version=0)
    data = inspected.to_dict()

    assert data["exists"] is False
    assert data["type"] == "reference"
    assert data["id"] == "global:@missing"


def test_show_on_nonexistent_ref_reports_empty_state(basic_trace):
    inspected = basic_trace.inspect("global:@missing", "reference", version=0)
    output = basic_trace.investigate(inspected).show()

    assert "global:@missing" in output
    assert "exist" in output.lower() or "missing" in output.lower() or "none" in output.lower()


def test_inspect_unknown_kind_raises_honestly(basic_trace):
    # NOTE:
    # Unknown kind is API misuse, not a missing object within a valid kind.
    with pytest.raises(InvalidObjectKindError):
        basic_trace.inspect("@A", "not_a_real_kind")


def test_investigate_unknown_kind_raises_honestly(basic_trace):
    # NOTE:
    # Unknown kind is API misuse, not a missing object within a valid kind.
    with pytest.raises(InvalidObjectKindError):
        basic_trace.investigate("@A", "not_a_real_kind")


def test_path_none_source_is_handled_correctly(basic_trace):
    # NOTE:
    # None is not a structured nonexistent object. It is caller misuse.
    use_a = actor(basic_trace, "UseA", grouped=False)

    
    path = basic_trace.path(None, use_a)
    assert not path.exists()


def test_path_none_target_is_handled_correctly(basic_trace):
    # NOTE:
    # None is not a structured nonexistent object. It is caller misuse.
    a_ref = ref(basic_trace, "global:@A", version=0)

    path = basic_trace.path(a_ref, None)
    assert not path.exists()
