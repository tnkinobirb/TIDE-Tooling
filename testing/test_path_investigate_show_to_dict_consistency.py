import json
import re

import pytest

from tide_tooling import TIDE


# ================================================================
# SHOW / TO_DICT CONSISTENCY TESTS
# ================================================================
#
# Fifth implementation slice.
#
# Core invariant:
#   .to_dict() is structured truth.
#   .show() is a human projection of that truth.
#
# .show() cannot invent refs, versions, path counts, or fake paths.
#
# Keep these tests semantic.
# Avoid brittle full multiline snapshots and colour-code assertions.
# ================================================================


ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def strip_ansi(text):
    return ANSI_RE.sub("", text)


def actor(trace, name, grouped=False):
    return trace.inspect(name, "actor", grouped=grouped)


def ref(trace, name, version=None):
    if version is None:
        return trace.inspect(name, "reference")

    return trace.inspect(name, "reference", version=version)


def shown(obj, **kwargs):
    return strip_ansi(obj.show(**kwargs))


def object_display(data):
    display = data["object"]["display"]

    return str(display)


@pytest.fixture
def show_dict_trace():
    def UseTwice(first, second):
        return "out"

    tide = TIDE()

    tide.event("A", writes={"@A": "!A"})
    tide.exec(UseTwice, args=["@@A", "@@A"], writes=["@out"])
    tide.event("Unused", writes={"@unused": "unused"})

    return tide.trace()


def test_inspect_exec_record_to_dict_preserves_reads_and_writes(show_dict_trace):
    inspected = show_dict_trace.inspect(1, "seq_id")
    data = inspected.to_dict()

    assert set(data) == {
        "type",
        "id",
        "version",
        "source_oceans",
        "source_runners",
        "source_pipelines",
        "history",
        "upstream_seeds",
        "downstream_seeds",
        "grouped",
        "exists",
        "divergence",
        "parents",
        "children",
    }
    assert data["type"] == "exec_record"
    assert data["id"] == inspected.id
    assert data["grouped"] is False
    assert data["exists"] is True
    assert "reads" not in data
    assert "writes" not in data


def test_inspect_exec_record_to_dict_preserves_duplicate_input_occurrences(show_dict_trace):
    inspected = show_dict_trace.inspect(1, "seq_id")
    reads = inspected.object.reads

    assert [
        (
            read.unscoped_reference,
            read.resolved_version,
            read.associated_input[0],
            read.input_index,
        )
        for read in reads
    ] == [
        ("global:@A", 0, "args", 0),
        ("global:@A", 0, "args", 1),
    ]


def test_investigate_exec_record_to_dict_preserves_reads_and_writes(show_dict_trace):
    investigated = show_dict_trace.investigate(1, "seq_id")
    data = investigated.to_dict()

    assert object_display(data) == "EXEC_RECORD[1] UseTwice"
    assert set(data) == {
        "type",
        "kind",
        "object",
        "summary",
        "appearances",
        "history",
        "inputs",
        "output",
        "reads",
        "writes",
        "upstream",
        "downstream",
        "navigation",
        "divergence",
        "parents",
        "children",
    }
    assert [read["id"] for read in data["reads"]] == [
        "global:@A[v0]",
        "global:@A[v0]",
    ]
    assert [read["input_use"]["display"] for read in data["reads"]] == [
        "args[0]",
        "args[1]",
    ]
    assert [write["id"] for write in data["writes"]] == [
        "global:@out[v0]",
    ]
    assert [read["id"] for read in data["history"][0]["reads"]] == [
        "global:@A[v0]",
        "global:@A[v0]",
    ]


def test_investigate_actor_to_dict_preserves_execution_history(show_dict_trace):
    investigated = show_dict_trace.investigate("UseTwice", "actor", grouped=False)
    data = investigated.to_dict()
    assert object_display(data) == "ACTOR[UseTwice]"
    assert len(data["history"]) == 1
    assert data["history"][0]["execution"]["display"] == "EXEC_RECORD[1] UseTwice"
    assert data["summary"]["execution_count"] == 1


def test_path_to_dict_no_path_has_no_fake_edges(show_dict_trace):
    unused_ref = ref(show_dict_trace, "global:@unused", version=0)
    use_actor = actor(show_dict_trace, "UseTwice", grouped=False)

    path = show_dict_trace.path(unused_ref, use_actor)
    data = path.to_dict()

    assert not path.exists()
    assert data["exists"] is False

    assert data["paths"] == []


def test_path_to_dict_is_json_compatible(show_dict_trace):
    source = ref(show_dict_trace, "global:@A", version=0)
    target = actor(show_dict_trace, "UseTwice", grouped=False)

    json.dumps(show_dict_trace.path(source, target).to_dict())


def test_path_show_reports_correct_path_count(show_dict_trace):
    a_ref = ref(show_dict_trace, "global:@A", version=0)
    use_actor = actor(show_dict_trace, "UseTwice", grouped=False)

    path = show_dict_trace.path(a_ref, use_actor)

    output = shown(path, show_input_positions=True)

    assert path.exists()
    assert path.count() == 1
    assert "1 path found" in output
    assert "args[0]" in output
    assert "args[1]" in output


def test_path_show_output_refs_are_subset_of_path_to_dict_or_path_object(show_dict_trace):
    a_ref = ref(show_dict_trace, "global:@A", version=0)
    use_actor = actor(show_dict_trace, "UseTwice", grouped=False)

    path = show_dict_trace.path(a_ref, use_actor)
    data = path.to_dict()
    output = shown(path, show_input_positions=True)

    assert data["exists"] is True

    # This is intentionally not a full snapshot assertion.
    # It locks that show mentions the real queried ref, not an unrelated ref.
    assert "global:@A" in output
    assert "global:@unused" not in output


def test_path_show_output_versions_are_subset_of_path_to_dict_or_path_object(show_dict_trace):
    a_ref = ref(show_dict_trace, "global:@A", version=0)
    use_actor = actor(show_dict_trace, "UseTwice", grouped=False)

    path = show_dict_trace.path(a_ref, use_actor)
    data = path.to_dict()
    output = shown(path, show_input_positions=True)

    assert path.exists()
    edge_dicts = [
        edge
        for path in data["paths"]
        for handoffs in path["handoffs"]
        for edge in handoffs
        if edge["type"] == "read_edge"
    ]
    assert any(
        edge["state"]["ref"] == "global:@A"
        and edge["state"]["version"] == 0
        for edge in edge_dicts
    )
    assert not any(
        edge["state"]["ref"] == "global:@A"
        and edge["state"]["version"] == 1
        for edge in edge_dicts
    )
    assert "global:@A[v0]" in output
    assert "global:@A[v1]" not in output


# ================================================================
# GROUPED ACTOR / GROUPED REFERENCE ID + DISPLAY SEMANTICS
# ================================================================
#
#
# Contract:
#   Grouped actor id is the raw string identifier, e.g. "UseTwice".
#   Grouped actor display is "GROUPED[UseTwice]"; show headers add the kind.
#
#   Grouped reference id is the key ref, e.g. "@A".
#   It is not the scoped identity "global:@A" or "testing:@A".
#
# This protects the distinction:
#   id = query/semantic identifier
#   display = human-formatted label
# ================================================================


def test_grouped_actor_id_is_raw_actor_name_not_display_label():
    def UseTwice(a):
        return a

    tide = TIDE()

    tide.event("SeedA", writes={"@A": "A"})
    tide.exec(UseTwice, args=["@@A"], writes=["@out"])
    tide.event("UseTwice", reads={"@A": "A"}, writes={"@event_out": "event"})

    trace = tide.trace()

    grouped_actor = trace.inspect("UseTwice", "actor")

    assert grouped_actor.grouped
    assert grouped_actor.id == "UseTwice"
    for actor in grouped_actor.object:
        assert str(actor) == "ACTOR[UseTwice]"


def test_grouped_actor_to_dict_uses_raw_id_and_display_separately():
    def UseTwice(a):
        return a

    tide = TIDE()

    tide.event("SeedA", writes={"@A": "A"})
    tide.exec(UseTwice, args=["@@A"], writes=["@out"])
    tide.event("UseTwice", reads={"@A": "A"}, writes={"@event_out": "event"})

    trace = tide.trace()

    investigated = trace.investigate("UseTwice", "actor")
    data = investigated.to_dict()

    assert data["object"]["id"] == "UseTwice"
    assert str(data["object"]["display"]) == "GROUPED[UseTwice]"


def test_grouped_actor_show_uses_display_without_changing_id_semantics(capsys):
    def UseTwice(a):
        return a

    tide = TIDE()

    tide.event("SeedA", writes={"@A": "A"})
    tide.exec(UseTwice, args=["@@A"], writes=["@out"])
    tide.event("UseTwice", reads={"@A": "A"}, writes={"@event_out": "event"})

    trace = tide.trace()

    investigated = trace.investigate("UseTwice", "actor")
    output = strip_ansi(investigated.show())
    capsys.readouterr()

    assert investigated.inspected.id == "UseTwice"
    assert "INVESTIGATE ACTOR GROUPED[UseTwice]" in output
    assert "UseTwice" in output


def test_grouped_reference_id_is_key_ref_not_scoped_ref():
    tide = TIDE()
    tide.ocean.register_store("testing")

    tide.event("GlobalA", writes={"@A": "global"})
    tide.event("TestingA", writes={"testing:@A": "testing"})

    trace = tide.trace()

    grouped_ref = trace.inspect("@A", "reference")

    assert grouped_ref.grouped
    assert grouped_ref.id == "@A"
    assert grouped_ref.id != "global:@A"
    assert grouped_ref.id != "testing:@A"


def test_grouped_reference_to_dict_uses_key_ref_id_and_preserves_scoped_history():
    tide = TIDE()
    tide.ocean.register_store("testing")

    tide.event("GlobalA", writes={"@A": "global"})
    tide.event("TestingA", writes={"testing:@A": "testing"})

    trace = tide.trace()

    investigated = trace.investigate("@A", "reference")
    data = investigated.to_dict()

    assert data["object"]["id"] == "@A"
    assert ("global:@A", 0) in investigated.inspected.history
    assert ("testing:@A", 0) in investigated.inspected.history


def test_grouped_reference_show_uses_key_ref_query_without_erasing_store_scopes():
    tide = TIDE()
    tide.ocean.register_store("testing")

    tide.event("GlobalA", writes={"@A": "global"})
    tide.event("TestingA", writes={"testing:@A": "testing"})

    trace = tide.trace()

    investigated = trace.investigate("@A", "reference")
    output = strip_ansi(investigated.show())

    assert investigated.inspected.id == "@A"
    assert "INVESTIGATE" in output
    assert "@A" in output
    assert "global:@A" in output
    assert "testing:@A" in output
