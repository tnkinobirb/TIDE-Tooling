import pytest

from tide_tooling import TIDE
from tide_tooling.core import Actor


# ================================================================
# STATIC REF TESTS
# ================================================================
#
# These tests lock the idea that:
#   ref identity = store/scope + ref key + version
#
# They should not silently blur:
#   @A
#   global:@A
#   testing:@A
#
# Assumptions these tests intentionally make:
#   - Unscoped @A writes resolve to global:@A.
#   - Explicit global:@A stays global:@A.
#   - Explicit testing:@A stays testing:@A after registering testing store.
#   - Static reads link to the exact ref/version actually read.
#   - Same raw value in two stores does not collapse reference identity.
#   - Exact inspect of a scoped ref should preserve that scope.
#   - Inspected reference identity is ref id + separate version field.
#   - by/consumer helpers remain scope-exact for references, values, actors, and exec_records.
#   - Seeded stores are distinct from the live/global environment.
#
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


def input_name(read):
    associated_input = read.associated_input

    if isinstance(associated_input, tuple):
        return associated_input[0]

    return associated_input


def input_uses_from_reads(exec_record):
    return [
        (
            read.unscoped_reference,
            read.resolved_version,
            input_name(read),
            read.input_index,
        )
        for read in exec_record.reads
    ]


def first_exec(trace, seq_id):
    return trace.inspect(seq_id, "seq_id").object


def exec_for_actor(trace, actor_name, grouped=False):
    inspected_actor = trace.inspect(actor_name, "actor", grouped=grouped)
    exec_id = inspected_actor.history[0]

    return trace.inspect(exec_id, "exec_record").object


def test_unscoped_static_write_defaults_to_global_ref():
    tide = TIDE()
    tide.event("A", writes={"@A": "!A"})
    trace = tide.trace()

    a_exec = first_exec(trace, 0)

    assert refs_from_writes(a_exec) == [
        ("global:@A", 0)
    ]

    inspected = trace.inspect("global:@A", "reference", version=0)

    assert inspected.id == "global:@A"
    assert inspected.version == 0
    assert inspected.type == "reference"


def test_explicit_global_static_write_stays_global_ref():
    tide = TIDE()
    tide.event("A", writes={"global:@A": "!A"})
    trace = tide.trace()

    a_exec = first_exec(trace, 0)

    assert refs_from_writes(a_exec) == [
        ("global:@A", 0)
    ]

    inspected = trace.inspect("global:@A", "reference", version=0)

    assert inspected.id == "global:@A"
    assert inspected.version == 0
    assert inspected.type == "reference"


def test_explicit_registered_store_static_write_stays_scoped_ref():
    tide = TIDE()
    tide.ocean.register_store("testing")

    tide.event("A", writes={"testing:@A": "!A"})
    trace = tide.trace()

    a_exec = first_exec(trace, 0)

    assert refs_from_writes(a_exec) == [
        ("testing:@A", 0)
    ]

    inspected = trace.inspect("testing:@A", "reference", version=0)

    assert inspected.id == "testing:@A"
    assert inspected.version == 0
    assert inspected.type == "reference"


def test_exact_static_ref_inspect_preserves_version_separately_from_id():
    tide = TIDE()

    tide.event("A0", writes={"@A": "v0"})
    tide.event("A1", writes={"@A": "v1"})

    trace = tide.trace()

    a_v0 = trace.inspect("global:@A", "reference", version=0)
    a_v1 = trace.inspect("global:@A", "reference", version=1)

    assert a_v0.id == "global:@A"
    assert a_v0.version == 0

    assert a_v1.id == "global:@A"
    assert a_v1.version == 1

    assert a_v0.id == a_v1.id
    assert a_v0.version != a_v1.version


def test_static_read_of_unscoped_ref_reads_latest_global_version():
    tide = TIDE()

    tide.event("A0", writes={"@A": "v0"})
    tide.event("A1", writes={"@A": "v1"})
    tide.event("B", reads={"@A": "v1"}, writes={"@B": "from-v1"})

    trace = tide.trace()

    b_exec = first_exec(trace, 2)

    assert refs_from_reads(b_exec) == [
        ("global:@A", 1)
    ]

    a_v1 = trace.inspect("global:@A", "reference", version=1)
    b_actor = trace.inspect("B", "actor", grouped=False)
    wrong_a_v0 = trace.inspect("global:@A", "reference", version=0)

    assert trace.path(a_v1, b_actor).exists()
    assert not trace.path(wrong_a_v0, b_actor).exists()


def test_static_reads_are_version_exact_across_multiple_versions():
    tide = TIDE()

    tide.event("A0", writes={"@A": "v0"})
    tide.event("UseV0", reads={"@A": "v0"})
    tide.event("A1", writes={"@A": "v1"})
    tide.event("UseV1", reads={"@A": "v1"})

    trace = tide.trace()

    use_v0_exec = first_exec(trace, 1)
    use_v1_exec = first_exec(trace, 3)

    assert refs_from_reads(use_v0_exec) == [
        ("global:@A", 0)
    ]
    assert refs_from_reads(use_v1_exec) == [
        ("global:@A", 1)
    ]

    a_v0 = trace.inspect("global:@A", "reference", version=0)
    a_v1 = trace.inspect("global:@A", "reference", version=1)
    use_v0 = trace.inspect("UseV0", "actor", grouped=False)
    use_v1 = trace.inspect("UseV1", "actor", grouped=False)

    assert trace.path(a_v0, use_v0).exists()
    assert not trace.path(a_v0, use_v1).exists()

    assert trace.path(a_v1, use_v1).exists()
    assert not trace.path(a_v1, use_v0).exists()


def test_static_scoped_read_does_not_fall_back_to_global_ref():
    tide = TIDE()
    tide.ocean.register_store("testing")

    tide.event("GlobalA", writes={"@A": "global"})
    tide.event("TestingA", writes={"testing:@A": "testing"})
    tide.event("UseTesting", reads={"testing:@A": "testing"}, writes={"@out": "ok"})

    trace = tide.trace()

    use_exec = first_exec(trace, 2)

    assert refs_from_reads(use_exec) == [
        ("testing:@A", 0)
    ]

    global_ref = trace.inspect("global:@A", "reference", version=0)
    testing_ref = trace.inspect("testing:@A", "reference", version=0)
    use_actor = trace.inspect("UseTesting", "actor", grouped=False)

    assert not trace.path(global_ref, use_actor).exists()
    assert trace.path(testing_ref, use_actor).exists()


def test_seeded_store_static_exec_read_resolves_to_seeded_store_ref():
    def UseSeeded(a):
        return f"seeded-read-{a}"

    tide = TIDE()
    tide.ocean.register_store(
        "testing",
        store_template={
            "@seeded": "seeded-value",
        },
    )

    tide.exec(UseSeeded, args=["@testing:@seeded"], writes=["@out"])

    trace = tide.trace()

    use_exec = exec_for_actor(trace, "UseSeeded", grouped=False)

    assert refs_from_reads(use_exec) == [
        ("testing:@seeded", 0)
    ]

    seeded_ref = trace.inspect("testing:@seeded", "reference", version=0)
    use_seeded = trace.inspect("UseSeeded", "actor", grouped=False)

    consumed = trace.consumed_by(seeded_ref)

    assert Actor(trace.ocean_id, "UseSeeded", "UseSeeded") not in consumed.actors
    assert use_seeded.id in consumed.actors
    assert trace.path(seeded_ref, use_seeded).exists()


def test_seeded_store_ref_does_not_shadow_live_global_ref():
    def UseSeeded(a):
        return f"seeded-read-{a}"

    def UseLive(a):
        return f"live-read-{a}"

    tide = TIDE()
    tide.ocean.register_store(
        "testing",
        store_template={
            "@A": "seeded",
        },
    )

    tide.event("LiveGlobalA", writes={"@A": "live"})
    tide.exec(UseSeeded, args=["@testing:@A"], writes=["@seeded_out"])
    tide.exec(UseLive, args=["@@A"], writes=["@live_out"])

    trace = tide.trace()

    use_seeded_exec = exec_for_actor(trace, "UseSeeded", grouped=False)
    use_live_exec = exec_for_actor(trace, "UseLive", grouped=False)

    assert refs_from_reads(use_seeded_exec) == [
        ("testing:@A", 0)
    ]
    assert refs_from_reads(use_live_exec) == [
        ("global:@A", 0)
    ]

    seeded_ref = trace.inspect("testing:@A", "reference", version=0)
    live_ref = trace.inspect("global:@A", "reference", version=0)
    use_seeded = trace.inspect("UseSeeded", "actor", grouped=False)
    use_live = trace.inspect("UseLive", "actor", grouped=False)

    assert trace.path(seeded_ref, use_seeded).exists()
    assert not trace.path(seeded_ref, use_live).exists()

    assert trace.path(live_ref, use_live).exists()
    assert not trace.path(live_ref, use_seeded).exists()


def test_first_traced_testing_write_after_unread_seeded_value_is_version_zero():
    def UseSeeded(a):
        return f"seeded-read-{a}"

    def UseLive(a):
        return f"live-read-{a}"

    tide = TIDE()
    tide.ocean.register_store(
        "testing",
        store_template={
            "@A": "seeded-v0",
        },
    )

    # The seeded value is live store state, but in this test it has not been
    # observed/read inside the current trace before the explicit event write.
    # Therefore the first traced testing:@A write is version 0.
    tide.event("LiveGlobalA0", writes={"@A": "live-v0"})
    tide.event("SeededTestingA1", writes={"testing:@A": "seeded-v1"})
    tide.exec(UseSeeded, args=["@testing:@A"], writes=["@seeded_out"])
    tide.exec(UseLive, args=["@@A"], writes=["@live_out"])

    trace = tide.trace()

    use_seeded_exec = exec_for_actor(trace, "UseSeeded", grouped=False)
    use_live_exec = exec_for_actor(trace, "UseLive", grouped=False)

    assert refs_from_reads(use_seeded_exec) == [
        ("testing:@A", 0)
    ]
    assert refs_from_reads(use_live_exec) == [
        ("global:@A", 0)
    ]

    seeded_v0 = trace.inspect("testing:@A", "reference", version=0)
    live_v0 = trace.inspect("global:@A", "reference", version=0)
    use_seeded = trace.inspect("UseSeeded", "actor", grouped=False)
    use_live = trace.inspect("UseLive", "actor", grouped=False)

    assert trace.path(seeded_v0, use_seeded).exists()
    assert not trace.path(seeded_v0, use_live).exists()

    assert trace.path(live_v0, use_live).exists()
    assert not trace.path(live_v0, use_seeded).exists()


def test_same_ref_name_in_different_stores_has_distinct_reference_identity():
    tide = TIDE()
    tide.ocean.register_store("testing")

    tide.event("GlobalA", writes={"@A": "same"})
    tide.event("TestingA", writes={"testing:@A": "same"})

    trace = tide.trace()

    global_ref = trace.inspect("global:@A", "reference", version=0)
    testing_ref = trace.inspect("testing:@A", "reference", version=0)

    assert global_ref.id == "global:@A"
    assert testing_ref.id == "testing:@A"
    assert global_ref.id != testing_ref.id

    global_writer = trace.produced_by(trace.inspect("GlobalA", "actor", grouped=False)).refs
    testing_writer = trace.produced_by(trace.inspect("TestingA", "actor", grouped=False)).refs

    assert global_writer == [
        ("global:@A", 0)
    ]
    assert testing_writer == [
        ("testing:@A", 0)
    ]


def test_same_value_in_different_stores_does_not_collapse_ref_identity():
    tide = TIDE()
    tide.ocean.register_store("testing")

    tide.event("GlobalA", writes={"@A": "same"})
    tide.event("TestingA", writes={"testing:@A": "same"})
    tide.event("UseGlobal", reads={"@A": "same"})
    tide.event("UseTesting", reads={"testing:@A": "same"})

    trace = tide.trace()

    same_value = trace.inspect("same", "value")
    global_ref = trace.inspect("global:@A", "reference", version=0)
    testing_ref = trace.inspect("testing:@A", "reference", version=0)
    use_global = trace.inspect("UseGlobal", "actor", grouped=False)
    use_testing = trace.inspect("UseTesting", "actor", grouped=False)

    assert trace.path(same_value, global_ref).exists()
    assert trace.path(same_value, testing_ref).exists()

    assert trace.path(global_ref, use_global).exists()
    assert not trace.path(global_ref, use_testing).exists()

    assert trace.path(testing_ref, use_testing).exists()
    assert not trace.path(testing_ref, use_global).exists()


def test_unscoped_reference_query_can_group_scoped_history_without_becoming_exact_identity():
    tide = TIDE()
    tide.ocean.register_store("testing")

    tide.event("GlobalA", writes={"@A": "global"})
    tide.event("TestingA", writes={"testing:@A": "testing"})

    trace = tide.trace()

    grouped = trace.inspect("@A", "reference")
    exact_global = trace.inspect("global:@A", "reference", version=0)
    exact_testing = trace.inspect("testing:@A", "reference", version=0)

    assert grouped.grouped
    assert ("global:@A", 0) in grouped.history
    assert ("testing:@A", 0) in grouped.history

    assert exact_global.id == "global:@A"
    assert exact_testing.id == "testing:@A"
    assert exact_global.id != exact_testing.id


def test_produced_by_static_ref_is_scope_exact():
    tide = TIDE()
    tide.ocean.register_store("testing")

    tide.event("GlobalA", writes={"@A": "global"})
    tide.event("TestingA", writes={"testing:@A": "testing"})

    trace = tide.trace()

    global_ref = trace.inspect("global:@A", "reference", version=0)
    testing_ref = trace.inspect("testing:@A", "reference", version=0)

    global_produced_by = trace.produced_by(global_ref)
    testing_produced_by = trace.produced_by(testing_ref)

    assert global_produced_by.refs == [
        ("global:@A", 0)
    ]
    assert testing_produced_by.refs == [
        ("testing:@A", 0)
    ]

    assert Actor(trace.ocean_id, "TIDE: .event()", "GlobalA") in global_produced_by.actors
    assert Actor(trace.ocean_id, "TIDE: .event()", "TestingA") not in global_produced_by.actors

    assert Actor(trace.ocean_id, "TIDE: .event()", "TestingA") in testing_produced_by.actors
    assert Actor(trace.ocean_id, "TIDE: .event()", "GlobalA") not in testing_produced_by.actors


def test_produced_by_static_ref_is_version_exact():
    tide = TIDE()

    tide.event("A0", writes={"@A": "v0"})
    tide.event("A1", writes={"@A": "v1"})

    trace = tide.trace()

    a_v0 = trace.inspect("global:@A", "reference", version=0)
    a_v1 = trace.inspect("global:@A", "reference", version=1)

    produced_v0 = trace.produced_by(a_v0)
    produced_v1 = trace.produced_by(a_v1)

    assert produced_v0.refs == [
        ("global:@A", 0)
    ]
    assert produced_v1.refs == [
        ("global:@A", 1)
    ]

    assert Actor(trace.ocean_id, "TIDE: .event()", "A0") in produced_v0.actors
    assert Actor(trace.ocean_id, "TIDE: .event()", "A1") not in produced_v0.actors

    assert Actor(trace.ocean_id, "TIDE: .event()", "A1") in produced_v1.actors
    assert Actor(trace.ocean_id, "TIDE: .event()", "A0") not in produced_v1.actors


def test_consumed_by_static_ref_is_scope_exact():
    tide = TIDE()
    tide.ocean.register_store("testing")

    tide.event("GlobalA", writes={"@A": "global"})
    tide.event("TestingA", writes={"testing:@A": "testing"})
    tide.event("UseGlobal", reads={"@A": "global"})
    tide.event("UseTesting", reads={"testing:@A": "testing"})

    trace = tide.trace()

    use_global = trace.inspect("UseGlobal", "actor", grouped=False)
    use_testing = trace.inspect("UseTesting", "actor", grouped=False)

    assert trace.consumed_by(use_global).refs == [
        ("global:@A", 0)
    ]
    assert trace.consumed_by(use_testing).refs == [
        ("testing:@A", 0)
    ]

    global_ref = trace.inspect("global:@A", "reference", version=0)
    testing_ref = trace.inspect("testing:@A", "reference", version=0)

    global_ref_consumed_by = trace.consumed_by(global_ref)
    testing_ref_consumed_by = trace.consumed_by(testing_ref)

    assert global_ref_consumed_by.refs == [
        ("global:@A", 0)
    ]
    assert testing_ref_consumed_by.refs == [
        ("testing:@A", 0)
    ]

    assert Actor(trace.ocean_id, "TIDE: .event()", "UseGlobal") in global_ref_consumed_by.actors
    assert Actor(trace.ocean_id, "TIDE: .event()", "UseTesting") not in global_ref_consumed_by.actors

    assert Actor(trace.ocean_id, "TIDE: .event()", "UseTesting") in testing_ref_consumed_by.actors
    assert Actor(trace.ocean_id, "TIDE: .event()", "UseGlobal") not in testing_ref_consumed_by.actors


def test_consumed_by_static_ref_is_version_exact():
    tide = TIDE()

    tide.event("A0", writes={"@A": "v0"})
    tide.event("UseV0", reads={"@A": "v0"})
    tide.event("A1", writes={"@A": "v1"})
    tide.event("UseV1", reads={"@A": "v1"})

    trace = tide.trace()

    a_v0 = trace.inspect("global:@A", "reference", version=0)
    a_v1 = trace.inspect("global:@A", "reference", version=1)

    consumed_v0 = trace.consumed_by(a_v0)
    consumed_v1 = trace.consumed_by(a_v1)

    assert consumed_v0.refs == [
        ("global:@A", 0)
    ]
    assert consumed_v1.refs == [
        ("global:@A", 1)
    ]

    assert Actor(trace.ocean_id, "TIDE: .event()", "UseV0") in consumed_v0.actors
    assert Actor(trace.ocean_id, "TIDE: .event()", "UseV1") not in consumed_v0.actors

    assert Actor(trace.ocean_id, "TIDE: .event()", "UseV1") in consumed_v1.actors
    assert Actor(trace.ocean_id, "TIDE: .event()", "UseV0") not in consumed_v1.actors


def test_produced_by_value_tracks_all_static_ref_appearances():
    tide = TIDE()
    tide.ocean.register_store("testing")

    tide.event("GlobalA", writes={"@A": "same"})
    tide.event("TestingA", writes={"testing:@A": "same"})

    trace = tide.trace()

    same_value = trace.inspect("same", "value")
    produced = trace.produced_by(same_value)

    assert ("global:@A", 0) in produced.refs
    assert ("testing:@A", 0) in produced.refs

    assert Actor(trace.ocean_id, "TIDE: .event()", "GlobalA") in produced.actors
    assert Actor(trace.ocean_id, "TIDE: .event()", "TestingA") in produced.actors


def test_consumed_by_value_tracks_all_static_ref_consumers():
    tide = TIDE()
    tide.ocean.register_store("testing")

    tide.event("GlobalA", writes={"@A": "same"})
    tide.event("TestingA", writes={"testing:@A": "same"})
    tide.event("UseGlobal", reads={"@A": "same"})
    tide.event("UseTesting", reads={"testing:@A": "same"})

    trace = tide.trace()

    same_value = trace.inspect("same", "value")
    consumed = trace.consumed_by(same_value)

    assert ("global:@A", 0) in consumed.refs
    assert ("testing:@A", 0) in consumed.refs

    assert Actor(trace.ocean_id, "TIDE: .event()", "UseGlobal") in consumed.actors
    assert Actor(trace.ocean_id, "TIDE: .event()", "UseTesting") in consumed.actors


def test_consumed_by_state_deduplicates_top_level_refs():
    tide = TIDE()
    tide.event("A", writes={"@A": "same"})
    tide.event("UseA1", reads="@A")
    tide.event("UseA2", reads="@A")
    trace = tide.trace()

    consumed = trace.consumed_by(
        trace.inspect("global:@A", "reference", version=0)
    )

    assert consumed.refs == [("global:@A", 0)]


def test_produced_by_exec_record_returns_only_that_exec_writes():
    tide = TIDE()

    tide.event("A", writes={"@A": "!A"})
    tide.event("B", writes={"@B": "!B"})

    trace = tide.trace()

    a_exec = trace.inspect(0, "seq_id")
    b_exec = trace.inspect(1, "seq_id")

    assert trace.produced_by(a_exec).refs == [
        ("global:@A", 0)
    ]
    assert trace.produced_by(b_exec).refs == [
        ("global:@B", 0)
    ]


def test_consumed_by_exec_record_returns_only_that_exec_reads():
    tide = TIDE()

    tide.event("A", writes={"@A": "!A"})
    tide.event("B", writes={"@B": "!B"})
    tide.event("UseA", reads={"@A": "!A"})
    tide.event("UseB", reads={"@B": "!B"})

    trace = tide.trace()

    use_a_exec = trace.inspect(2, "seq_id")
    use_b_exec = trace.inspect(3, "seq_id")

    assert trace.consumed_by(use_a_exec).refs == [
        ("global:@A", 0)
    ]
    assert trace.consumed_by(use_b_exec).refs == [
        ("global:@B", 0)
    ]


def test_produced_by_grouped_actor_collects_all_static_writes():
    tide = TIDE()

    tide.event("A", writes={"@A": "v0"})
    tide.event("A", writes={"@A": "v1"})

    trace = tide.trace()

    a_actor = trace.inspect("A", "actor")
    produced = trace.produced_by(a_actor)

    assert produced.refs == [
        ("global:@A", 0),
        ("global:@A", 1)
    ]


def test_consumed_by_grouped_actor_collects_all_static_reads():
    tide = TIDE()

    tide.event("A0", writes={"@A": "v0"})
    tide.event("UseA", reads={"@A": "v0"})
    tide.event("A1", writes={"@A": "v1"})
    tide.event("UseA", reads={"@A": "v1"})

    trace = tide.trace()

    use_actor = trace.inspect("UseA", "actor")
    consumed = trace.consumed_by(use_actor)

    assert consumed.refs == [
        ("global:@A", 0),
        ("global:@A", 1)
    ]


def test_static_duplicate_input_occurrences_are_preserved_for_exec_without_ui_assertions():
    def UseTwice(a, also_a):
        return "done"

    tide = TIDE()

    tide.event("A", writes={"@A": "value"})
    tide.exec(UseTwice, args=["@@A", "@@A"], writes=["@out"])

    trace = tide.trace()

    use_exec = first_exec(trace, 1)

    assert refs_from_reads(use_exec) == [
        ("global:@A", 0),
        ("global:@A", 0),
    ]

    assert input_uses_from_reads(use_exec) == [
        ("global:@A", 0, "args", 0),
        ("global:@A", 0, "args", 1),
    ]

    ref = trace.inspect("global:@A", "reference", version=0)
    use_actor = trace.inspect("UseTwice", "actor", grouped=False)

    assert trace.path(ref, use_actor).exists()


def test_event_write_can_be_consumed_by_exec_static_read():
    def UseA(a):
        return f"exec-read-{a}"

    tide = TIDE()

    tide.event("SeedEvent", writes={"@A": "!A"})
    tide.exec(UseA, args=["@@A"], writes=["@B"])

    trace = tide.trace()

    seed_exec = first_exec(trace, 0)
    use_exec = first_exec(trace, 1)

    assert refs_from_writes(seed_exec) == [
        ("global:@A", 0)
    ]
    assert refs_from_reads(use_exec) == [
        ("global:@A", 0)
    ]
    assert refs_from_writes(use_exec) == [
        ("global:@B", 0)
    ]

    a_ref = trace.inspect("global:@A", "reference", version=0)
    use_actor = trace.inspect("UseA", "actor", grouped=False)

    assert trace.path(a_ref, use_actor).exists()


def test_exec_write_can_be_consumed_by_event_static_read():
    def SeedExec():
        return "!A"

    tide = TIDE()

    tide.exec(SeedExec, writes=["@A"])
    tide.event("UseEvent", reads={"@A": "!A"}, writes={"@B": "!B"})

    trace = tide.trace()

    seed_exec = first_exec(trace, 0)
    use_exec = first_exec(trace, 1)

    assert refs_from_writes(seed_exec) == [
        ("global:@A", 0)
    ]
    assert refs_from_reads(use_exec) == [
        ("global:@A", 0)
    ]
    assert refs_from_writes(use_exec) == [
        ("global:@B", 0)
    ]

    a_ref = trace.inspect("global:@A", "reference", version=0)
    use_actor = trace.inspect("UseEvent", "actor", grouped=False)

    assert trace.path(a_ref, use_actor).exists()


def test_exec_static_scoped_read_is_scope_exact():
    def UseA(a):
        return f"used-{a}"

    tide = TIDE()
    tide.ocean.register_store("testing")

    tide.event("GlobalA", writes={"@A": "global"})
    tide.event("TestingA", writes={"testing:@A": "testing"})
    tide.exec(UseA, args=["@testing:@A"], writes=["@out"])

    trace = tide.trace()

    use_exec = first_exec(trace, 2)

    assert refs_from_reads(use_exec) == [
        ("testing:@A", 0)
    ]

    global_ref = trace.inspect("global:@A", "reference", version=0)
    testing_ref = trace.inspect("testing:@A", "reference", version=0)
    use_actor = trace.inspect("UseA", "actor", grouped=False)

    assert not trace.path(global_ref, use_actor).exists()
    assert trace.path(testing_ref, use_actor).exists()


def test_missing_static_ref_does_not_create_successful_consumer_path():
    tide = TIDE()

    tide.event("Producer", writes={"@A": "present"})
    tide.event("Consumer", reads={"@missing": "missing"}, writes={"@out": "out"})

    trace = tide.trace()

    present_ref = trace.inspect("global:@A", "reference", version=0)
    consumer = trace.inspect("Consumer", "actor", grouped=False)

    assert not trace.path(present_ref, consumer).exists()

    consumed = trace.consumed_by(consumer).refs

    assert ("global:@A", 0) not in consumed
