import pytest

from tide_tooling import TIDE
from tide_tooling.core import Actor, tide_value_hasher



def B(a, b, separate_b):
    return "!B"


def X(a, ab, abc):
    return None


@pytest.fixture(scope="module")
def trace():
    tide = TIDE()
    tide.ocean.register_store("testing")

    tide.event("A", writes={"@A": "!A"})
    tide.event("B", reads=["@A"], writes={"@B": "!B", "testing:@B": "!B"})
    tide.exec(B, args=["@@A", "@@B", "@testing:@B"], writes=["@B"])
    tide.event(B, reads={"@A": "!A", "@B": "!B"}, writes={"@B": "!B"})
    tide.event("C", reads={"@B": "!B"}, writes={"@C": "!C"})
    tide.event("X", writes={"@C": "!C"})
    tide.exec(X, args=["@@C", "@testing:@B", "@testing:@B"], writes=["@B"])

    return tide.trace()


def ids(matches):
    return [match.id for match in matches]


def test_find_exact_scoped_reference(trace):
    searched = trace.find("global:@B")

    assert ids(searched.refs) == ["global:@B"]
    assert searched.exists("reference")[0] is True
    assert searched.first("reference") is searched.refs[0]


def test_find_exact_scoped_reference_version(trace):
    searched = trace.find("global:@B", version=1)

    assert ids(searched.refs) == ["global:@B"]
    assert searched.refs[0].version == 1
    assert searched.exists("ref")[0] is True


def test_find_accepts_exact_state_identity(trace):
    searched = trace.find("global:@B[v1]")

    assert ids(searched.refs) == ["global:@B"]
    assert searched.refs[0].version == 1


def test_find_rejects_conflicting_state_versions(trace):
    with pytest.raises(ValueError, match="conflicts"):
        trace.find("global:@B[v1]", version=2)


def test_find_missing_reference_version_does_not_fall_back(trace):
    searched = trace.find("global:@B", version=999)

    assert searched.refs == []
    assert searched.exists("reference")[0] is False


def test_find_unscoped_reference_is_nearby_only(trace):
    searched = trace.find("@B")

    assert searched.refs == []
    assert set(ids(searched.nearby.refs)) == {"global:@B", "testing:@B"}
    assert searched.exists("reference") == (False, True)


def test_find_plain_key_matches_nearby_references_not_exact(trace):
    searched = trace.find("B")

    assert searched.refs == []
    assert set(ids(searched.nearby.refs)) == {"global:@B", "testing:@B"}


def test_find_actor_name_is_nearby_only_and_deduped(trace):
    searched = trace.find("B")

    assert searched.actors == []
    assert set(ids(searched.nearby.actors)) == {
        Actor(trace.ocean_id, "TIDE: .event()", "B"),
        Actor(trace.ocean_id, 0, "B"),
    }
    assert len(searched.nearby.actors) == 2
    assert searched.exists("actor") == (False, True)


def test_find_accepts_actor_occurrence_identity(trace):
    searched = trace.find("B[0]")

    assert len(searched.nearby.actors) == 1
    assert searched.nearby.actors[0].version == 0
    assert len(searched.nearby.actors[0].history) == 1


def test_actor_occurrences_are_trace_wide_across_runners(trace):
    b_executions = [record for record in trace.log if record.func_name == "B"]

    assert [repr(record.producer_occurrence_id) for record in b_executions] == [
        "B[0]",
        "B[1]",
        "B[2]",
    ]
    assert [identity for identity in trace.to_model(enrich=False)["E"] if identity.startswith("B[")] == [
        "B[0]",
        "B[1]",
        "B[2]",
    ]


def test_inspect_accepts_actor_occurrence_identity(trace):
    inspected = trace.inspect("B[0]", "actor")

    assert inspected.exists
    assert inspected.version == 0
    assert len(inspected.history) == 1


def test_why_shows_the_investigated_object(trace):
    shown = trace.why("B[0]", "actor", color=False)

    assert "INVESTIGATE" in shown
    assert "B[0]" in shown


def test_explicit_reference_occurrence_uses_exact_then_nearby(trace):
    exact = trace.inspect("global:@B[v1]", "reference")
    nearby = trace.inspect("@B[v1]", "reference")

    assert exact.exists
    assert exact.id == "global:@B"
    assert nearby.exists
    assert nearby.id == "global:@B"


def test_find_function_name_retains_nearby_execution_occurrences(trace):
    searched = trace.find("B")

    assert ids(searched.nearby.execs) == [
        execution.exec_id
        for execution in trace.log
        if "B" in execution.func_name
    ]
    assert searched.exists("exec_record") == (False, True)


def test_find_exact_actor_object(trace):
    actor = Actor(trace.ocean_id, "TIDE: .event()", "B")
    searched = trace.find(actor)

    assert ids(searched.actors) == [actor]
    assert searched.nearby.actors == []
    assert searched.exists("actor") == (True, False)
    assert searched.first("actor") is searched.actors[0]


def test_find_exact_actor_tuple(trace):
    actor_tuple = (trace.ocean_id, 0, "B")
    searched = trace.find(actor_tuple)

    assert ids(searched.actors) == [Actor(*actor_tuple)]
    assert searched.nearby.actors == []


def test_find_exec_by_sequence_id(trace):
    searched = trace.find(2)

    assert len(searched.execs) == 1
    assert searched.execs[0].object.object.seq_id == 2
    assert searched.exists("seq_id") == (True, False)


def test_find_exec_by_canonical_exec_id(trace):
    exec_id = trace.log[2].exec_id
    searched = trace.find(exec_id)

    assert ids(searched.execs) == [exec_id]
    assert searched.exists("exec_record") == (True, False)


def test_find_raw_value_is_not_exact_value(trace):
    searched = trace.find("!B")

    assert searched.values == []
    assert searched.exists("value") == (False, False)


def test_find_value_hash_is_exact(trace):
    value_hash = tide_value_hasher("!B")
    searched = trace.find(value_hash)

    assert ids(searched.values) == [value_hash]
    assert searched.exists("hash") == (True, False)
    assert searched.first("value") is searched.values[0]


def test_find_plain_key_does_not_match_value(trace):
    assert trace.find("B").values == []


def test_find_nearby_first_returns_first_nearby_actor(trace, capsys):
    searched = trace.find("B")
    first = searched.first("actor")
    output = capsys.readouterr().out

    assert first is searched.nearby.actors[0]
    assert output == ""


def test_find_nearby_first_returns_first_nearby_reference(trace, capsys):
    searched = trace.find("@B")
    first = searched.first("reference")
    output = capsys.readouterr().out

    assert first is searched.nearby.refs[0]
    assert output == ""


def test_find_first_without_type_uses_first_available_exact_match(trace):
    searched = trace.find("global:@B")

    assert searched.first() is searched.refs[0]


def test_find_first_without_type_falls_back_in_actor_ref_exec_value_order(
    trace,
    capsys,
):
    searched = trace.find("B")
    first = searched.first()
    output = capsys.readouterr().out

    assert first is searched.nearby.actors[0]
    assert output == ""


def test_find_first_missing_type_returns_none(trace, capsys):
    searched = trace.find("doesnt_exist")
    first = searched.first("exec")
    output = capsys.readouterr().out

    assert first is None
    assert output == ""


def test_find_first_without_type_returns_none_when_search_is_empty(
    trace,
    capsys,
):
    searched = trace.find("doesnt_exist")

    assert searched.first() is None
    assert capsys.readouterr().out == ""


def test_find_any_true_for_nearby_search(trace):
    searched = trace.find("B")

    assert searched.any() is True
    assert searched.exists("actor") == (False, True)
    assert searched.exists("reference") == (False, True)


def test_find_any_true_for_exact_search(trace):
    assert trace.find("global:@B").any() is True
    assert trace.find(2).any() is True
    assert trace.find(tide_value_hasher("!B")).any() is True


def test_find_any_false_for_missing_search(trace):
    assert trace.find("doesnt_exist").any() is False


@pytest.mark.parametrize(
    ("query_type", "attribute", "nearby_attribute"),
    [
        ("actor", "actors", "actors"),
        ("actors", "actors", "actors"),
        ("ref", "refs", "refs"),
        ("refs", "refs", "refs"),
        ("reference", "refs", "refs"),
        ("exec", "execs", "execs"),
        ("execs", "execs", "execs"),
        ("exec_record", "execs", "execs"),
        ("value", "values", None),
        ("values", "values", None),
        ("hash", "values", None),
        ("effect", "effects", None),
        ("effects", "effects", None),
    ],
)
def test_find_get_type_aliases(trace, query_type, attribute, nearby_attribute):
    searched = trace.find("B")
    result = searched.get_type(query_type)

    assert result["exact"] is getattr(searched, attribute)

    if nearby_attribute is not None:
        assert result["nearby"] is getattr(searched.nearby, nearby_attribute)
    else:
        assert result["nearby"] == []


def test_find_get_exact_returns_a_copy_of_exact_matches(trace):
    searched = trace.find("global:@B")

    exact = searched.get_exact("reference")

    assert exact == searched.refs
    assert exact is not searched.refs


def test_find_get_defaults_to_exact_scope(trace):
    searched = trace.find("global:@B")

    assert searched.get("reference") == searched.refs
    assert searched.get("reference") is not searched.refs


def test_find_get_nearby_returns_nearby_matches(trace):
    searched = trace.find("@B")

    nearby = searched.get_nearby("reference")

    assert nearby == searched.nearby.refs
    assert nearby is not searched.nearby.refs
    assert {match.id for match in nearby} == {
        "global:@B",
        "testing:@B",
    }


def test_find_get_selects_nearby_scope(trace):
    searched = trace.find("@B")

    assert searched.get("reference", scope="exact") == []
    assert {
        match.id
        for match in searched.get("reference", scope="nearby")
    } == {
        "global:@B",
        "testing:@B",
    }


def test_find_get_nearby_is_empty_for_types_without_nearby_matches(trace):
    searched = trace.find(2)

    assert searched.get_nearby("exec_record") == []
    assert searched.get("value", scope="nearby") == []


def test_find_to_dict_preserves_current_object_structure(trace):
    searched = trace.find("B")
    data = searched.to_dict()

    assert set(data) == {"actors", "refs", "values", "execs", "effects"}
    assert data["actors"] == {
        "exact": [match.to_dict() for match in searched.actors],
        "nearby": [
            match.to_dict()
            for match in searched.nearby.actors
        ],
    }
    assert data["refs"] == {
        "exact": [match.to_dict() for match in searched.refs],
        "nearby": [
            match.to_dict()
            for match in searched.nearby.refs
        ],
    }
    assert data["values"] == {
        "exact": [match.to_dict() for match in searched.values],
    }
    assert data["execs"] == {
        "exact": [match.to_dict() for match in searched.execs],
        "nearby": [
            match.to_dict()
            for match in searched.nearby.execs
        ],
    }
    assert data["effects"] == {
        "exact": [match.to_dict() for match in searched.effects],
    }


def test_find_exact_reference_version_is_not_duplicated_in_nearby(trace):
    searched = trace.find("global:@B", version=1)

    assert ids(searched.refs) == ["global:@B"]
    assert searched.refs[0].version == 1
    assert "global:@B" not in ids(searched.nearby.refs)


def test_find_show_can_render_only_nearby_results_for_one_type(trace):
    shown = trace.find("@B").show(
        kind="reference",
        exact=False,
        nearby=True,
    )

    assert "FIND" in shown
    assert "NEARBY" in shown
    assert "global:@B" in shown
    assert "testing:@B" in shown
    assert "EXACT" not in shown


def test_find_show_can_render_all_exact_result_types(trace):
    shown = trace.find("global:@B").show(
        kind="all",
        exact=True,
        nearby=False,
    )

    assert "EXACT" in shown
    assert "REFERENCE" in shown
    assert "global:@B" in shown
    assert "NEARBY" not in shown


def test_find_actor_nearby_order_is_stable_for_fixture(trace):
    searched = trace.find("B")

    assert ids(searched.nearby.actors) == [
        Actor(trace.ocean_id, "TIDE: .event()", "B"),
        Actor(trace.ocean_id, 0, "B"),
    ]
