import re

import pytest

from tide_tooling import TIDE


# ================================================================
# GROUPED SEMANTICS TESTS
# ================================================================
#
# Gap area #5 from tide_next_gaps_1_3_5_7_9.txt.
#
# Core distinction:
#   id      = semantic/query identifier
#   display = human-formatted label
#
# Examples:
#   grouped actor id      -> "UseTwice"
#   grouped actor display -> "GROUPED[UseTwice]"
#
#   grouped ref id        -> "@A"
#   scoped exact ref id   -> "global:@A" or "testing:@A"
# ================================================================


ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def strip_ansi(text):
    return ANSI_RE.sub("", text)


def actor(trace, name, grouped=True):
    return trace.inspect(name, "actor", grouped=grouped)


def ref(trace, name, version=None):
    if version is None:
        return trace.inspect(name, "reference")

    return trace.inspect(name, "reference", version=version)


def value(trace, raw_value):
    return trace.inspect(raw_value, "value")


def object_id_from_dict(data):
    return data["object"]["id"]


def object_display_from_dict(data):
    return str(data["object"]["display"])


@pytest.fixture
def grouped_actor_trace():
    def UseTwice(a):
        return a

    tide = TIDE()

    tide.event("SeedA", writes={"@A": "A"})
    tide.exec(UseTwice, args=["@@A"], writes=["@exec_out"])
    tide.event("UseTwice", reads={"@A": "A"}, writes={"@event_out": "event"})

    return tide.trace()


@pytest.fixture
def grouped_ref_trace():
    tide = TIDE()
    tide.ocean.register_store("testing")

    tide.event("GlobalA0", writes={"@A": "global-v0"})
    tide.event("GlobalA1", writes={"@A": "global-v1"})
    tide.event("TestingA0", writes={"testing:@A": "testing-v0"})

    return tide.trace()


def test_grouped_actor_id_is_raw_actor_name_not_display_label(grouped_actor_trace):
    grouped = actor(grouped_actor_trace, "UseTwice", grouped=True)

    assert grouped.grouped
    assert grouped.id == "UseTwice"
    for actor_obj in grouped.object:
        assert str(actor_obj) == "ACTOR[UseTwice]"


def test_exact_actor_id_is_full_actor_identity_not_grouped_name(grouped_actor_trace):
    exact = actor(grouped_actor_trace, "UseTwice", grouped=False)

    assert not exact.grouped
    assert exact.id != "UseTwice"

    # NOTE:
    # Exact actor id should be the Actor identity object/string,
    # while source_actor remains the human function/event name.
    assert getattr(exact.id, "source_actor", None) == "UseTwice"


def test_grouped_actor_history_preserves_multiple_actor_instances(grouped_actor_trace):
    grouped = actor(grouped_actor_trace, "UseTwice", grouped=True)

    assert grouped.grouped
    assert len(grouped.history) == 2


def test_grouped_actor_to_dict_separates_raw_id_from_display(grouped_actor_trace):
    investigated = grouped_actor_trace.investigate("UseTwice", "actor")
    data = investigated.to_dict()

    assert object_id_from_dict(data) == "UseTwice"
    assert object_display_from_dict(data) == "GROUPED[UseTwice]"


def test_grouped_actor_show_uses_display_without_changing_id(grouped_actor_trace):
    investigated = grouped_actor_trace.investigate("UseTwice", "actor")
    output = strip_ansi(investigated.show())

    assert investigated.inspected.id == "UseTwice"
    assert "ACTOR GROUPED[UseTwice]" in output


def test_grouped_actor_path_show_uses_kind_and_grouped_identity(
    grouped_actor_trace,
):
    source = ref(grouped_actor_trace, "global:@A", version=0)
    target = actor(grouped_actor_trace, "UseTwice", grouped=True)

    output = strip_ansi(grouped_actor_trace.path(source, target).show())

    assert "ACTOR GROUPED[UseTwice]" in output


def test_grouped_ref_id_is_key_ref_not_scoped_ref(grouped_ref_trace):
    grouped = ref(grouped_ref_trace, "@A")

    assert grouped.grouped
    assert grouped.id == "@A"
    assert grouped.id != "global:@A"
    assert grouped.id != "testing:@A"


def test_grouped_ref_history_contains_scoped_ref_version_pairs(grouped_ref_trace):
    grouped = ref(grouped_ref_trace, "@A")

    assert grouped.history == [
        ("global:@A", 0),
        ("global:@A", 1),
        ("testing:@A", 0),
    ]


def test_scoped_ref_inspect_is_exact_identity_not_grouped(grouped_ref_trace):
    global_ref = ref(grouped_ref_trace, "global:@A")
    testing_ref = ref(grouped_ref_trace, "testing:@A")

    assert not global_ref.grouped
    assert not testing_ref.grouped

    assert global_ref.id == "global:@A"
    assert testing_ref.id == "testing:@A"

    assert global_ref.history == [
        0,
        1,
    ]
    assert testing_ref.history == [
        0,
    ]


def test_exact_scoped_ref_version_keeps_complete_reference_history(grouped_ref_trace):
    global_v0 = ref(grouped_ref_trace, "global:@A", version=0)
    global_v1 = ref(grouped_ref_trace, "global:@A", version=1)

    assert global_v0.id == "global:@A"
    assert global_v0.version == 0
    assert global_v0.history == [
        0,
        1,
    ]

    assert global_v1.id == "global:@A"
    assert global_v1.version == 1
    assert global_v1.history == [
        0,
        1,
    ]


def test_grouped_ref_to_dict_uses_key_ref_id_and_display(grouped_ref_trace):
    investigated = grouped_ref_trace.investigate("@A", "reference")
    data = investigated.to_dict()

    assert object_id_from_dict(data) == "@A"

    # NOTE:
    # Display may be REFERENCE[@A] or similar, but it should not replace the id.
    assert "@A" in object_display_from_dict(data)


def test_grouped_ref_show_keeps_grouped_query_and_scoped_history_visible(grouped_ref_trace):
    investigated = grouped_ref_trace.investigate("@A", "reference")
    output = strip_ansi(investigated.show())

    assert investigated.inspected.id == "@A"
    assert "@A" in output
    assert "global:@A" in output
    assert "testing:@A" in output


def test_grouped_value_collects_ref_version_appearances_without_changing_value_identity():
    tide = TIDE()
    tide.ocean.register_store("testing")

    tide.event("GlobalSame", writes={"@A": "same"})
    tide.event("TestingSame", writes={"testing:@A": "same"})

    trace = tide.trace()
    same_value = value(trace, "same")

    assert ("global:@A", 0) in same_value.history
    assert ("testing:@A", 0) in same_value.history


def test_grouped_actor_path_query_reaches_matching_actor_instances(grouped_actor_trace):
    a_ref = ref(grouped_actor_trace, "global:@A", version=0)
    grouped = actor(grouped_actor_trace, "UseTwice", grouped=True)

    path = grouped_actor_trace.path(a_ref, grouped)

    assert path.exists()
    assert path.count() >= 1

    # NOTE:
    # If grouped path intentionally dedupes branches, do not force exact count.
    # The semantic lock is that grouped query reaches the grouped actor target.
    assert grouped.id == "UseTwice"


def test_grouped_ref_path_query_can_reach_consumer_without_erasing_scoped_ref():
    tide = TIDE()
    tide.ocean.register_store("testing")

    tide.event("GlobalA", writes={"@A": "global"})
    tide.event("TestingA", writes={"testing:@A": "testing"})
    tide.event("UseTestingA", reads={"testing:@A": "testing"})

    trace = tide.trace()

    grouped = ref(trace, "@A")
    consumer = actor(trace, "UseTestingA", grouped=False)
    testing_exact = ref(trace, "testing:@A", version=0)
    global_exact = ref(trace, "global:@A", version=0)

    assert trace.path(grouped, consumer).exists()
    assert trace.path(testing_exact, consumer).exists()
    assert not trace.path(global_exact, consumer).exists()
