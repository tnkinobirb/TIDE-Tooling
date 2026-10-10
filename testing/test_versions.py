import pytest

from tide_tooling import TIDE


# ================================================================
# VERSION TESTS
# ================================================================
#
# First implementation slice from tide_all_generated_test_notes_mega.txt.
#
# These tests intentionally stay small:
#   - exact ref version progression
#   - consumers read the correct version
#   - wrong-version paths do not exist
#   - grouped reference history preserves all versions
#   - exact inspect does not silently fallback to latest
#
# Avoid broad seed/import/idempotence coverage here.
# Keep those tests near test_store_access.py while that behavior is still debt.
# ================================================================


def refs_from_reads(exec_record):
    return [
        (read.unscoped_reference, read.resolved_version)
        for read in exec_record.reads
    ]


def refs_from_writes(exec_record):
    return [
        (write.stored_target, write.stored_version)
        for write in exec_record.writes
    ]


def first_exec(trace, seq_id):
    return trace.inspect(seq_id, "seq_id").object


def actor(trace, name, grouped=False):
    return trace.inspect(name, "actor", grouped=grouped)


def ref(trace, name, version=None):
    if version is None:
        return trace.inspect(name, "reference")
    return trace.inspect(name, "reference", version=version)


@pytest.fixture
def versioned_trace():
    tide = TIDE()

    tide.event("A0", writes={"@A": "A-v0"})
    tide.event("B0", reads={"@A": "A-v0"}, writes={"@B0": "B-from-A-v0"})
    tide.event("A1", writes={"@A": "A-v1"})
    tide.event("B1", reads={"@A": "A-v1"}, writes={"@B1": "B-from-A-v1"})

    return tide.trace()


def test_same_ref_written_twice_produces_v0_then_v1(versioned_trace):
    a0_exec = first_exec(versioned_trace, 0)
    a1_exec = first_exec(versioned_trace, 2)

    assert refs_from_writes(a0_exec) == [
        ("global:@A", 0)
    ]
    assert refs_from_writes(a1_exec) == [
        ("global:@A", 1)
    ]


def test_consumer_between_writes_reads_v0(versioned_trace):
    b0_exec = first_exec(versioned_trace, 1)

    assert refs_from_reads(b0_exec) == [
        ("global:@A", 0)
    ]
    assert refs_from_writes(b0_exec) == [
        ("global:@B0", 0)
    ]


def test_consumer_after_second_write_reads_v1(versioned_trace):
    b1_exec = first_exec(versioned_trace, 3)

    assert refs_from_reads(b1_exec) == [
        ("global:@A", 1)
    ]
    assert refs_from_writes(b1_exec) == [
        ("global:@B1", 0)
    ]


def test_wrong_version_path_to_consumer_does_not_exist(versioned_trace):
    a_v0 = ref(versioned_trace, "global:@A", version=0)
    a_v1 = ref(versioned_trace, "global:@A", version=1)

    b0 = actor(versioned_trace, "B0", grouped=False)
    b1 = actor(versioned_trace, "B1", grouped=False)

    assert versioned_trace.path(a_v0, b0).exists()
    assert versioned_trace.path(a_v1, b1).exists()

    assert not versioned_trace.path(a_v0, b1).exists()
    assert not versioned_trace.path(a_v1, b0).exists()


def test_grouped_reference_history_preserves_v0_and_v1(versioned_trace):
    grouped = ref(versioned_trace, "@A")

    assert grouped.grouped
    assert grouped.history == [
        ("global:@A", 0),
        ("global:@A", 1),
    ]


def test_exact_version_inspect_does_not_fallback_to_latest(versioned_trace):
    a_v0 = ref(versioned_trace, "global:@A", version=0)
    a_v1 = ref(versioned_trace, "global:@A", version=1)

    assert a_v0.id == "global:@A"
    assert a_v0.version == 0

    assert a_v1.id == "global:@A"
    assert a_v1.version == 1

    # NOTE:
    # Explicit scoped inspect, e.g. inspect("global:@A", version=1),
    # returns the exact inspected ref/version while history remains the
    # complete version history for that ref identity. The selected version
    # constrains relations, not the history projection.
    assert a_v0.history == [
        0,
        1,
    ]
    assert a_v1.history == [
        0,
        1,
    ]

    b0 = actor(versioned_trace, "B0", grouped=False)
    b1 = actor(versioned_trace, "B1", grouped=False)

    assert versioned_trace.path(a_v0, b0).exists()
    assert not versioned_trace.path(a_v0, b1).exists()

    assert versioned_trace.path(a_v1, b1).exists()
    assert not versioned_trace.path(a_v1, b0).exists()


def test_different_refs_start_versions_independently():
    tide = TIDE()

    tide.event("WriteA0", writes={"@A": "A-v0"})
    tide.event("WriteB0", writes={"@B": "B-v0"})
    tide.event("WriteA1", writes={"@A": "A-v1"})

    trace = tide.trace()

    write_a0 = first_exec(trace, 0)
    write_b0 = first_exec(trace, 1)
    write_a1 = first_exec(trace, 2)

    assert refs_from_writes(write_a0) == [
        ("global:@A", 0)
    ]
    assert refs_from_writes(write_b0) == [
        ("global:@B", 0)
    ]
    assert refs_from_writes(write_a1) == [
        ("global:@A", 1)
    ]


def test_same_key_different_stores_start_versions_independently():
    tide = TIDE()
    tide.ocean.register_store("testing")

    tide.event("WriteGlobalA0", writes={"global:@A": "global-A-v0"})
    tide.event("WriteTestingA0", writes={"testing:@A": "testing-A-v0"})
    tide.event("WriteGlobalA1", writes={"global:@A": "global-A-v1"})

    trace = tide.trace()

    global_a0 = first_exec(trace, 0)
    testing_a0 = first_exec(trace, 1)
    global_a1 = first_exec(trace, 2)

    assert refs_from_writes(global_a0) == [
        ("global:@A", 0)
    ]
    assert refs_from_writes(testing_a0) == [
        ("testing:@A", 0)
    ]
    assert refs_from_writes(global_a1) == [
        ("global:@A", 1)
    ]

    exact_global = ref(trace, "global:@A")
    exact_testing = ref(trace, "testing:@A")

    # NOTE:
    # Once a store identifier is included, inspect("store:@ref") resolves to
    # that exact ref identity rather than the unscoped grouped-reference view.
    # Exact scoped reference history is version-only.
    assert exact_global.id == "global:@A"
    assert exact_global.history == [
        0,
        1,
    ]

    assert exact_testing.id == "testing:@A"
    assert exact_testing.history == [
        0,
    ]

    # The unscoped grouped view is still the one that carries ref/version pairs.
    grouped = ref(trace, "@A")
    assert grouped.grouped
    assert ("global:@A", 0) in grouped.history
    assert ("global:@A", 1) in grouped.history
    assert ("testing:@A", 0) in grouped.history


def test_consumer_of_v0_and_consumer_of_v1_are_separate_in_exec_reads(versioned_trace):
    b0_exec = first_exec(versioned_trace, 1)
    b1_exec = first_exec(versioned_trace, 3)

    assert refs_from_reads(b0_exec) == [
        ("global:@A", 0)
    ]
    assert refs_from_reads(b1_exec) == [
        ("global:@A", 1)
    ]


def test_exact_version_downstream_does_not_include_other_version_consumer(versioned_trace):
    a_v0 = ref(versioned_trace, "global:@A", version=0)
    a_v1 = ref(versioned_trace, "global:@A", version=1)

    # These assertions intentionally stay shape-light because inspected
    # downstream storage may be seq ids or exec ids depending on the object.
    # The semantic check is enforced through path existence/non-existence.
    b0 = actor(versioned_trace, "B0", grouped=False)
    b1 = actor(versioned_trace, "B1", grouped=False)

    assert versioned_trace.path(a_v0, b0).exists()
    assert not versioned_trace.path(a_v0, b1).exists()

    assert versioned_trace.path(a_v1, b1).exists()
    assert not versioned_trace.path(a_v1, b0).exists()
