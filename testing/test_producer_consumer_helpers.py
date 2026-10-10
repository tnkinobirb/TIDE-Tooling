import pytest

from tide_tooling import TIDE
from tide_tooling.core import Actor
import re 
ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def strip_ansi(text):
    return ANSI_RE.sub("", text)



# ================================================================
# PRODUCER / CONSUMER HELPER TESTS
# ================================================================
#
# Fourth implementation slice from tide_all_generated_test_notes_mega.txt.
#
# Tiny vocabulary core only.
#
# Core questions:
#   What did this actor/exec use?
#   What did this actor/exec produce?
#   Who produced this ref/version?
#   Who consumed this ref/version?
#
# Current generic helpers:
#   trace.produced_by(source)
#   trace.consumed_by(source)
#
# These tests intentionally avoid alias helpers like inputs_of / outputs_of /
# producers_of / consumers_of unless those APIs already exist.
# ================================================================


def first_exec(trace, seq_id):
    return trace.inspect(seq_id, "seq_id").object


def actor(trace, name, grouped=False):
    return trace.inspect(name, "actor", grouped=grouped)


def ref(trace, name, version=None):
    if version is None:
        return trace.inspect(name, "reference")

    return trace.inspect(name, "reference", version=version)


def value(trace, raw_value):
    return trace.inspect(raw_value, "value")


def ref_set(by_result):
    return set(by_result.refs)


def actor_set(by_result):
    return set(by_result.actors)


@pytest.fixture
def producer_consumer_trace():
    def C(a, b):
        return "!C"

    tide = TIDE()

    tide.event("A", writes={"@A": "!A"})
    tide.event("B", reads={"@A": "!A"}, writes={"@B": "!B"})
    tide.exec(C, args=["@@A", "@@B"], writes=["@C"])
    tide.event("D", reads={"@C": "!C"})

    return tide.trace()


def test_consumed_by_actor_returns_refs_read_by_actor(producer_consumer_trace):
    b_actor = actor(producer_consumer_trace, "B", grouped=False)
    consumed = producer_consumer_trace.consumed_by(b_actor)

    assert consumed.refs == [
        ("global:@A", 0)
    ]


def test_produced_by_actor_returns_refs_written_by_actor(producer_consumer_trace):
    b_actor = actor(producer_consumer_trace, "B", grouped=False)
    produced = producer_consumer_trace.produced_by(b_actor)

    assert produced.refs == [
        ("global:@B", 0)
    ]


def test_consumed_by_exec_record_returns_refs_read_by_exec(producer_consumer_trace):
    b_exec = first_exec(producer_consumer_trace, 1)
    consumed = producer_consumer_trace.consumed_by(
        producer_consumer_trace.inspect(b_exec.id, "exec_record")
    )

    assert consumed.refs == [
        ("global:@A", 0)
    ]


def test_produced_by_exec_record_returns_refs_written_by_exec(producer_consumer_trace):
    b_exec = first_exec(producer_consumer_trace, 1)
    produced = producer_consumer_trace.produced_by(
        producer_consumer_trace.inspect(b_exec.id, "exec_record")
    )

    assert produced.refs == [
        ("global:@B", 0)
    ]


def test_actor_with_multiple_reads_returns_all_consumed_refs(producer_consumer_trace):
    c_actor = actor(producer_consumer_trace, "C", grouped=False)
    consumed = producer_consumer_trace.consumed_by(c_actor)

    assert ref_set(consumed) == {
        ("global:@A", 0),
        ("global:@B", 0),
    }


def test_actor_output_does_not_appear_in_consumed_by_actor(producer_consumer_trace):
    c_actor = actor(producer_consumer_trace, "C", grouped=False)
    consumed = producer_consumer_trace.consumed_by(c_actor)

    assert ("global:@C", 0) not in consumed.refs


def test_actor_input_does_not_appear_in_produced_by_actor(producer_consumer_trace):
    c_actor = actor(producer_consumer_trace, "C", grouped=False)
    produced = producer_consumer_trace.produced_by(c_actor)

    assert produced.refs == [
        ("global:@C", 0)
    ]
    assert ("global:@A", 0) not in produced.refs
    assert ("global:@B", 0) not in produced.refs


def test_produced_by_reference_returns_actor_that_wrote_ref_version(producer_consumer_trace):
    b_ref = ref(producer_consumer_trace, "global:@B", version=0)
    produced = producer_consumer_trace.produced_by(b_ref)

    assert actor_set(produced) == {
        Actor(producer_consumer_trace.ocean_id, "TIDE: .event()", "B")
    }


def test_consumed_by_reference_returns_actors_that_read_ref_version(producer_consumer_trace):
    a_ref = ref(producer_consumer_trace, "global:@A", version=0)
    consumed = producer_consumer_trace.consumed_by(a_ref)

    assert actor_set(consumed) == {
        Actor(producer_consumer_trace.ocean_id, "TIDE: .event()", "B"),
        Actor(producer_consumer_trace.ocean_id, 0, "C"),
    }


def test_reference_with_single_producer_and_single_consumer_stays_directional(producer_consumer_trace):
    c_ref = ref(producer_consumer_trace, "global:@C", version=0)

    produced = producer_consumer_trace.produced_by(c_ref)
    consumed = producer_consumer_trace.consumed_by(c_ref)

    assert actor_set(produced) == {
        Actor(producer_consumer_trace.ocean_id, 0, "C")
    }
    assert actor_set(consumed) == {
        Actor(producer_consumer_trace.ocean_id, "TIDE: .event()", "D")
    }


def test_consumed_by_reference_version_zero_does_not_include_later_version_consumer():
    tide = TIDE()

    tide.event("A0", writes={"@A": "A-v0"})
    tide.event("UseA0", reads={"@A": "A-v0"})
    tide.event("A1", writes={"@A": "A-v1"})
    tide.event("UseA1", reads={"@A": "A-v1"})

    trace = tide.trace()

    a_v0 = ref(trace, "global:@A", version=0)
    a_v1 = ref(trace, "global:@A", version=1)

    consumed_v0 = trace.consumed_by(a_v0)
    consumed_v1 = trace.consumed_by(a_v1)

    assert actor_set(consumed_v0) == {
        Actor(trace.ocean_id, "TIDE: .event()", "UseA0")
    }
    assert actor_set(consumed_v1) == {
        Actor(trace.ocean_id, "TIDE: .event()", "UseA1")
    }


def test_produced_by_reference_version_one_does_not_return_version_zero_producer():
    tide = TIDE()

    tide.event("A0", writes={"@A": "A-v0"})
    tide.event("A1", writes={"@A": "A-v1"})

    trace = tide.trace()

    a_v0 = ref(trace, "global:@A", version=0)
    a_v1 = ref(trace, "global:@A", version=1)

    produced_v0 = trace.produced_by(a_v0)
    produced_v1 = trace.produced_by(a_v1)

    assert actor_set(produced_v0) == {
        Actor(trace.ocean_id, "TIDE: .event()", "A0")
    }
    assert actor_set(produced_v1) == {
        Actor(trace.ocean_id, "TIDE: .event()", "A1")
    }


def test_exact_unversioned_reference_uses_every_state_occurrence():
    tide = TIDE()

    tide.event("A0", writes={"@A": "A-v0"})
    tide.event("UseA0", reads={"@A": "A-v0"})
    tide.event("A1", writes={"@A": "A-v1"})
    tide.event("UseA1", reads={"@A": "A-v1"})

    trace = tide.trace()
    exact = trace.inspect("global:@A", "reference")

    assert trace.produced_by(exact).refs == [
        ("global:@A", 0),
        ("global:@A", 1),
    ]
    assert trace.consumed_by(exact).refs == [
        ("global:@A", 0),
        ("global:@A", 1),
    ]


def test_exact_versioned_reference_uses_only_selected_state_occurrence():
    tide = TIDE()

    tide.event("A0", writes={"@A": "A-v0"})
    tide.event("UseA0", reads={"@A": "A-v0"})
    tide.event("A1", writes={"@A": "A-v1"})
    tide.event("UseA1", reads={"@A": "A-v1"})

    trace = tide.trace()
    exact = trace.inspect("global:@A", "reference", version=1)

    assert trace.produced_by(exact).refs == [("global:@A", 1)]
    assert trace.consumed_by(exact).refs == [("global:@A", 1)]


def test_produced_by_value_preserves_multiple_producer_refs():
    tide = TIDE()

    tide.event("A", writes={"@A": "same"})
    tide.event("B", writes={"@B": "same"})

    trace = tide.trace()

    same_value = value(trace, "same")
    produced = trace.produced_by(same_value)

    assert ref_set(produced) == {
        ("global:@A", 0),
        ("global:@B", 0),
    }


def test_consumed_by_value_preserves_multiple_consumer_refs():
    tide = TIDE()

    tide.event("A", writes={"@A": "same"})
    tide.event("B", writes={"@B": "same"})
    tide.event("UseA", reads={"@A": "same"})
    tide.event("UseB", reads={"@B": "same"})

    trace = tide.trace()

    same_value = value(trace, "same")
    consumed = trace.consumed_by(same_value)

    assert ref_set(consumed) == {
        ("global:@A", 0),
        ("global:@B", 0),
    }


def test_bybase_to_dict_preserves_source_id_execs_and_ref_paths(producer_consumer_trace):
    c_actor = actor(producer_consumer_trace, "C", grouped=False)
    produced = producer_consumer_trace.produced_by(c_actor)

    data = produced.to_dict()

    assert data["id"] == c_actor.id.to_dict()
    assert data["execs"] == produced.execs
    assert data["paths"] == {
        exec_id: [list(ref) for ref in refs]
        for exec_id, refs in produced.by_exec.items()
    }


def test_bybase_show_uses_shared_header_and_returns_printed_output(
    producer_consumer_trace,
    capsys,
):
    c_actor = actor(producer_consumer_trace, "C", grouped=False)
    produced = producer_consumer_trace.produced_by(c_actor)

    shown = strip_ansi(produced.show())
    printed = strip_ansi(capsys.readouterr().out)

    assert "PRODUCED BY ACTOR[C]" in shown
    assert "RELATIONS:" in shown
    assert "produced: global:@C[v0]" in shown
    assert shown in printed


def test_grouped_bybase_show_uses_kind_and_grouped_identity(
    producer_consumer_trace,
    capsys,
):
    c_actor = actor(producer_consumer_trace, "C", grouped=True)
    produced = producer_consumer_trace.produced_by(c_actor)

    shown = produced.show()
    capsys.readouterr()

    assert "PRODUCED BY ACTOR GROUPED[C]" in shown
