import pytest

from tide_tooling import TIDE
from tide_tooling.core import Actor,  Ocean, Node, Pipeline
from tide_tooling.tooling_core.engine.errors import MissingStoreError, StoreAlreadyRegisteredError
from tide_tooling.tooling_core.engine.stores import StoreTemplate




# ================================================================
# STORE / ACCESS TESTS
# ================================================================
#
# These tests are lower-level than path/show UI.
#
# They lock store/access semantics:
#   - stores are part of ref identity
#   - scoped store reads/writes preserve store scope
#   - store_template seeds live store state
#   - seeded values become versioned only when observed/read/used in the current trace
#   - first traced write to an unread seeded ref is version 0
#   - if a seeded value was observed first, a later traced write becomes the next version
#
# No assertions depend on rendered .show() output.
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


def actor_id(trace, actor_name, grouped=False):
    return trace.inspect(actor_name, "actor", grouped=grouped).id


def assert_actor_consumed_ref(trace, actor_name, ref, version):
    actor = trace.inspect(actor_name, "actor", grouped=False)
    consumed = trace.consumed_by(actor)

    assert (ref, version) in consumed.refs


def assert_ref_consumed_by_actor(trace, ref, version, actor_name):
    inspected_ref = trace.inspect(ref, "reference", version=version)
    consumed = trace.consumed_by(inspected_ref)

    assert actor_id(trace, actor_name, grouped=False) in consumed.actors


def assert_ref_produced_by_actor(trace, ref, version, actor_name):
    inspected_ref = trace.inspect(ref, "reference", version=version)
    produced = trace.produced_by(inspected_ref)

    assert actor_id(trace, actor_name, grouped=False) in produced.actors


# ================================================================
# REGISTERED STORE READ / WRITE
# ================================================================


def test_registered_store_event_write_records_scoped_target_and_version():
    tide = TIDE()
    tide.ocean.register_store("testing")

    tide.event("WriteTesting", writes={"testing:@A": "testing-value"})

    trace = tide.trace()
    write_exec = exec_for_actor(trace, "WriteTesting", grouped=False)

    assert refs_from_writes(write_exec) == [
        ("testing:@A", 0)
    ]

    inspected = trace.inspect("testing:@A", "reference", version=0)

    assert inspected.id == "testing:@A"
    assert inspected.version == 0


def test_registered_store_event_read_records_scoped_ref_and_version():
    tide = TIDE()
    tide.ocean.register_store("testing")

    tide.event("WriteTesting", writes={"testing:@A": "testing-value"})
    tide.event("ReadTesting", reads={"testing:@A": "testing-value"})

    trace = tide.trace()
    read_exec = exec_for_actor(trace, "ReadTesting", grouped=False)

    assert refs_from_reads(read_exec) == [
        ("testing:@A", 0)
    ]

    assert_actor_consumed_ref(trace, "ReadTesting", "testing:@A", 0)
    assert_ref_consumed_by_actor(trace, "testing:@A", 0, "ReadTesting")


def test_registered_store_exec_read_records_scoped_ref_and_version():
    def ReadTesting(a):
        return f"read-{a}"

    tide = TIDE()
    tide.ocean.register_store("testing")

    tide.event("WriteTesting", writes={"testing:@A": "testing-value"})
    tide.exec(ReadTesting, args=["@testing:@A"], writes=["@out"])

    trace = tide.trace()
    read_exec = exec_for_actor(trace, "ReadTesting", grouped=False)

    assert refs_from_reads(read_exec) == [
        ("testing:@A", 0)
    ]

    assert_actor_consumed_ref(trace, "ReadTesting", "testing:@A", 0)
    assert_ref_consumed_by_actor(trace, "testing:@A", 0, "ReadTesting")


def test_registered_store_exec_write_records_scoped_target_and_version():
    def WriteTesting():
        return "testing-value"

    tide = TIDE()
    tide.ocean.register_store("testing")

    tide.exec(WriteTesting, writes=["testing:@A"])

    trace = tide.trace()
    write_exec = exec_for_actor(trace, "WriteTesting", grouped=False)

    assert refs_from_writes(write_exec) == [
        ("testing:@A", 0)
    ]

    assert_ref_produced_by_actor(trace, "testing:@A", 0, "WriteTesting")


# ================================================================
# STORE ISOLATION
# ================================================================


def test_same_ref_name_in_global_and_registered_store_stays_distinct():
    tide = TIDE()
    tide.ocean.register_store("testing")

    tide.event("WriteGlobal", writes={"@A": "global-value"})
    tide.event("WriteTesting", writes={"testing:@A": "testing-value"})

    trace = tide.trace()

    global_ref = trace.inspect("global:@A", "reference", version=0)
    testing_ref = trace.inspect("testing:@A", "reference", version=0)

    assert global_ref.id == "global:@A"
    assert testing_ref.id == "testing:@A"
    assert global_ref.id != testing_ref.id

    assert_ref_produced_by_actor(trace, "global:@A", 0, "WriteGlobal")
    assert_ref_produced_by_actor(trace, "testing:@A", 0, "WriteTesting")


def test_same_value_in_global_and_registered_store_stays_distinct_by_ref_identity():
    tide = TIDE()
    tide.ocean.register_store("testing")

    tide.event("WriteGlobal", writes={"@A": "same-value"})
    tide.event("WriteTesting", writes={"testing:@A": "same-value"})

    trace = tide.trace()

    same_value = trace.inspect("same-value", "value")
    produced = trace.produced_by(same_value)

    assert ("global:@A", 0) in produced.refs
    assert ("testing:@A", 0) in produced.refs

    assert_ref_produced_by_actor(trace, "global:@A", 0, "WriteGlobal")
    assert_ref_produced_by_actor(trace, "testing:@A", 0, "WriteTesting")


def test_scoped_read_does_not_fall_back_to_global_when_same_ref_name_exists():
    tide = TIDE()
    tide.ocean.register_store("testing")

    tide.event("WriteGlobal", writes={"@A": "global-value"})
    tide.event("WriteTesting", writes={"testing:@A": "testing-value"})
    tide.event("ReadTesting", reads={"testing:@A": "testing-value"})

    trace = tide.trace()
    read_exec = exec_for_actor(trace, "ReadTesting", grouped=False)

    assert refs_from_reads(read_exec) == [
        ("testing:@A", 0)
    ]

    global_ref = trace.inspect("global:@A", "reference", version=0)
    testing_ref = trace.inspect("testing:@A", "reference", version=0)
    read_actor = trace.inspect("ReadTesting", "actor", grouped=False)

    assert not trace.path(global_ref, read_actor).exists()
    assert trace.path(testing_ref, read_actor).exists()


def test_unscoped_read_uses_global_even_when_registered_store_has_same_ref_name():
    tide = TIDE()
    tide.ocean.register_store("testing")

    tide.event("WriteTesting", writes={"testing:@A": "testing-value"})
    tide.event("WriteGlobal", writes={"@A": "global-value"})
    tide.event("ReadGlobal", reads={"@A": "global-value"})

    trace = tide.trace()
    read_exec = exec_for_actor(trace, "ReadGlobal", grouped=False)

    assert refs_from_reads(read_exec) == [
        ("global:@A", 0)
    ]

    global_ref = trace.inspect("global:@A", "reference", version=0)
    testing_ref = trace.inspect("testing:@A", "reference", version=0)
    read_actor = trace.inspect("ReadGlobal", "actor", grouped=False)

    assert trace.path(global_ref, read_actor).exists()
    assert not trace.path(testing_ref, read_actor).exists()


# ================================================================
# STORE TEMPLATE / SEEDING
# ================================================================


def test_store_template_seed_can_be_read_by_exec_as_version_zero():
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

    assert_actor_consumed_ref(trace, "UseSeeded", "testing:@seeded", 0)
    assert_ref_consumed_by_actor(trace, "testing:@seeded", 0, "UseSeeded")


def test_store_template_seed_can_be_read_by_event_as_version_zero():
    tide = TIDE()
    tide.ocean.register_store(
        "testing",
        store_template={
            "@seeded": "seeded-value",
        },
    )

    tide.event("UseSeededEvent", reads={"testing:@seeded": "seeded-value"})

    trace = tide.trace()
    use_exec = exec_for_actor(trace, "UseSeededEvent", grouped=False)

    assert refs_from_reads(use_exec) == [
        ("testing:@seeded", 0)
    ]

    assert_actor_consumed_ref(trace, "UseSeededEvent", "testing:@seeded", 0)
    assert_ref_consumed_by_actor(trace, "testing:@seeded", 0, "UseSeededEvent")


def test_store_template_seed_does_not_shadow_live_global_ref_for_exec_reads():
    def UseSeeded(a):
        return f"seeded-read-{a}"

    def UseGlobal(a):
        return f"global-read-{a}"

    tide = TIDE()
    tide.ocean.register_store(
        "testing",
        store_template={
            "@A": "seeded-value",
        },
    )

    tide.event("WriteGlobal", writes={"@A": "global-value"})
    tide.exec(UseSeeded, args=["@testing:@A"], writes=["@seeded_out"])
    tide.exec(UseGlobal, args=["@@A"], writes=["@global_out"])

    trace = tide.trace()

    seeded_exec = exec_for_actor(trace, "UseSeeded", grouped=False)
    global_exec = exec_for_actor(trace, "UseGlobal", grouped=False)

    assert refs_from_reads(seeded_exec) == [
        ("testing:@A", 0)
    ]
    assert refs_from_reads(global_exec) == [
        ("global:@A", 0)
    ]

    seeded_ref = trace.inspect("testing:@A", "reference", version=0)
    global_ref = trace.inspect("global:@A", "reference", version=0)
    seeded_actor = trace.inspect("UseSeeded", "actor", grouped=False)
    global_actor = trace.inspect("UseGlobal", "actor", grouped=False)

    trace.investigate(1, "seq_id").show()
    trace.investigate(3, "seq_id").show()

    assert trace.path(seeded_ref, seeded_actor).exists()
    assert not trace.path(seeded_ref, global_actor).exists()

    assert trace.path(global_ref, global_actor).exists()
    assert not trace.path(global_ref, seeded_actor).exists()


def test_first_traced_write_after_unread_seeded_store_value_is_version_zero():
    tide = TIDE()
    tide.ocean.register_store(
        "testing",
        store_template={
            "@A": "seeded-but-unobserved",
        },
    )

    tide.event("WriteTesting", writes={"testing:@A": "first-traced-write"})

    trace = tide.trace()
    write_exec = exec_for_actor(trace, "WriteTesting", grouped=False)

    assert refs_from_writes(write_exec) == [
        ("testing:@A", 0)
    ]

    assert_ref_produced_by_actor(trace, "testing:@A", 0, "WriteTesting")

# @pytest.mark.xfail(reason="Need to fix proper handling of this case", strict=False)
def test_exec_observed_seed_then_event_write_advances_to_next_version():
    def UseSeeded(a):
        return f"seeded-read-{a}"

    tide = TIDE()
    tide.ocean.register_store(
        "testing",
        store_template={
            "@A": "seeded-v0",
        },
    )

    tide.exec(UseSeeded, args=["@testing:@A"], writes=["@seeded_out"])
    tide.event("WriteTestingV1", writes={"testing:@A": "event-v1"})

    trace = tide.trace()

    read_exec = exec_for_actor(trace, "UseSeeded", grouped=False)
    write_exec = exec_for_actor(trace, "WriteTestingV1", grouped=False)

    assert refs_from_reads(read_exec) == [
        ("testing:@A", 0)
    ]
    assert refs_from_writes(write_exec) == [
        ("testing:@A", 1)
    ]

    assert_ref_consumed_by_actor(trace, "testing:@A", 0, "UseSeeded")
    assert_ref_produced_by_actor(trace, "testing:@A", 1, "WriteTestingV1")


def test_event_observed_seed_then_exec_write_advances_to_next_version():
    def WriteTestingV1():
        return "exec-v1"

    tide = TIDE()
    tide.ocean.register_store(
        "testing",
        store_template={
            "@A": "seeded-v0",
        },
    )

    tide.event("UseSeededEvent", reads={"testing:@A": "seeded-v0"})
    tide.exec(WriteTestingV1, writes=["testing:@A"])

    trace = tide.trace()

    read_exec = exec_for_actor(trace, "UseSeededEvent", grouped=False)
    write_exec = exec_for_actor(trace, "WriteTestingV1", grouped=False)

    assert refs_from_reads(read_exec) == [
        ("testing:@A", 0)
    ]
    assert refs_from_writes(write_exec) == [
        ("testing:@A", 1)
    ]

    assert_ref_consumed_by_actor(trace, "testing:@A", 0, "UseSeededEvent")
    assert_ref_produced_by_actor(trace, "testing:@A", 1, "WriteTestingV1")


def test_seeded_same_value_as_global_still_keeps_store_identity():
    def UseSeeded(a):
        return f"seeded-read-{a}"

    def UseGlobal(a):
        return f"global-read-{a}"

    tide = TIDE()
    tide.ocean.register_store(
        "testing",
        store_template={
            "@A": "same-value",
        },
    )

    tide.event("WriteGlobal", writes={"@A": "same-value"})
    tide.exec(UseSeeded, args=["@testing:@A"], writes=["@seeded_out"])
    tide.exec(UseGlobal, args=["@@A"], writes=["@global_out"])

    trace = tide.trace()

    same_value = trace.inspect("same-value", "value")
    produced = trace.produced_by(same_value)
    consumed = trace.consumed_by(same_value)

    assert ("testing:@A", 0) in produced.refs
    assert ("global:@A", 0) in produced.refs

    assert ("testing:@A", 0) in consumed.refs
    assert ("global:@A", 0) in consumed.refs

    assert_ref_consumed_by_actor(trace, "testing:@A", 0, "UseSeeded")
    assert_ref_consumed_by_actor(trace, "global:@A", 0, "UseGlobal")


# ================================================================
# EVENT / EXEC ACCESS SYMMETRY
# ================================================================


def test_event_write_can_feed_exec_read_across_registered_store():
    def UseTesting(a):
        return f"exec-read-{a}"

    tide = TIDE()
    tide.ocean.register_store("testing")

    tide.event("WriteTesting", writes={"testing:@A": "testing-value"})
    tide.exec(UseTesting, args=["@testing:@A"], writes=["@out"])

    trace = tide.trace()

    write_exec = exec_for_actor(trace, "WriteTesting", grouped=False)
    read_exec = exec_for_actor(trace, "UseTesting", grouped=False)

    assert refs_from_writes(write_exec) == [
        ("testing:@A", 0)
    ]
    assert refs_from_reads(read_exec) == [
        ("testing:@A", 0)
    ]

    source = trace.inspect("testing:@A", "reference", version=0)
    target = trace.inspect("UseTesting", "actor", grouped=False)

    assert trace.path(source, target).exists()


def test_exec_write_can_feed_event_read_across_registered_store():
    def WriteTesting():
        return "testing-value"

    tide = TIDE()
    tide.ocean.register_store("testing")

    tide.exec(WriteTesting, writes=["testing:@A"])
    tide.event("UseTestingEvent", reads={"testing:@A": "testing-value"})

    trace = tide.trace()

    write_exec = exec_for_actor(trace, "WriteTesting", grouped=False)
    read_exec = exec_for_actor(trace, "UseTestingEvent", grouped=False)

    assert refs_from_writes(write_exec) == [
        ("testing:@A", 0)
    ]
    assert refs_from_reads(read_exec) == [
        ("testing:@A", 0)
    ]

    source = trace.inspect("testing:@A", "reference", version=0)
    target = trace.inspect("UseTestingEvent", "actor", grouped=False)

    assert trace.path(source, target).exists()


def test_exec_duplicate_scoped_input_occurrences_preserve_input_positions_without_ui():
    def UseTwice(a, also_a):
        return "done"

    tide = TIDE()
    tide.ocean.register_store("testing")

    tide.event("WriteTesting", writes={"testing:@A": "testing-value"})
    tide.exec(UseTwice, args=["@testing:@A", "@testing:@A"], writes=["@out"])

    trace = tide.trace()
    use_exec = exec_for_actor(trace, "UseTwice", grouped=False)

    assert refs_from_reads(use_exec) == [
        ("testing:@A", 0),
        ("testing:@A", 0),
    ]

    assert input_uses_from_reads(use_exec) == [
        ("testing:@A", 0, "args", 0),
        ("testing:@A", 0, "args", 1),
    ]

# @pytest.mark.xfail(reason="Unregistered store behavior is not finalized yet.", strict=False)
def test_unregistered_store_read_fails_honestly_or_gets_formally_defined():
    def UseMissingStore(a):
        return f"read-{a}"

    tide = TIDE()
    with pytest.raises(MissingStoreError):
        tide.exec(UseMissingStore, args=["@missing_store:@A"], writes=["@out"])

def test_seeded_store_read_twice_by_two_exec_consumers_both_consume_version_zero():
    def UseSeededOne(a):
        return f"one-{a}"

    def UseSeededTwo(a):
        return f"two-{a}"

    tide = TIDE()
    tide.ocean.register_store(
        "testing",
        store_template={
            "@A": "seeded-v0",
        },
    )

    tide.exec(UseSeededOne, args=["@testing:@A"], writes=["@one_out"])
    tide.exec(UseSeededTwo, args=["@testing:@A"], writes=["@two_out"])

    trace = tide.trace()

    one_exec = exec_for_actor(trace, "UseSeededOne", grouped=False)
    two_exec = exec_for_actor(trace, "UseSeededTwo", grouped=False)

    assert refs_from_reads(one_exec) == [
        ("testing:@A", 0)
    ]
    assert refs_from_reads(two_exec) == [
        ("testing:@A", 0)
    ]

    assert_ref_consumed_by_actor(trace, "testing:@A", 0, "UseSeededOne")
    assert_ref_consumed_by_actor(trace, "testing:@A", 0, "UseSeededTwo")


def test_seeded_store_read_twice_by_event_and_exec_consumers_both_consume_version_zero():
    def UseSeededExec(a):
        return f"exec-{a}"

    tide = TIDE()
    tide.ocean.register_store(
        "testing",
        store_template={
            "@A": "seeded-v0",
        },
    )

    tide.event("UseSeededEvent", reads={"testing:@A": "seeded-v0"})
    tide.exec(UseSeededExec, args=["@testing:@A"], writes=["@exec_out"])

    trace = tide.trace()

    event_exec = exec_for_actor(trace, "UseSeededEvent", grouped=False)
    exec_exec = exec_for_actor(trace, "UseSeededExec", grouped=False)

    assert refs_from_reads(event_exec) == [
        ("testing:@A", 0)
    ]
    assert refs_from_reads(exec_exec) == [
        ("testing:@A", 0)
    ]

    assert_ref_consumed_by_actor(trace, "testing:@A", 0, "UseSeededEvent")
    assert_ref_consumed_by_actor(trace, "testing:@A", 0, "UseSeededExec")


def test_event_observed_seed_then_event_write_advances_to_next_version():
    tide = TIDE()
    tide.ocean.register_store(
        "testing",
        store_template={
            "@A": "seeded-v0",
        },
    )

    tide.event("ObserveSeed", reads={"testing:@A": "seeded-v0"})
    tide.event("WriteTestingV1", writes={"testing:@A": "event-v1"})

    trace = tide.trace()

    observe_exec = exec_for_actor(trace, "ObserveSeed", grouped=False)
    write_exec = exec_for_actor(trace, "WriteTestingV1", grouped=False)

    assert refs_from_reads(observe_exec) == [
        ("testing:@A", 0)
    ]
    assert refs_from_writes(write_exec) == [
        ("testing:@A", 1)
    ]

    assert_ref_consumed_by_actor(trace, "testing:@A", 0, "ObserveSeed")
    assert_ref_produced_by_actor(trace, "testing:@A", 1, "WriteTestingV1")

# @pytest.mark.xfail(reason="seeding.import behaviour not fully solidified", strict=False)
def test_exec_observed_seed_then_exec_write_advances_to_next_version():
    def ObserveSeed(a):
        return f"observed-{a}"

    def WriteTestingV1():
        return "exec-v1"

    tide = TIDE()
    tide.ocean.register_store(
        "testing",
        store_template={
            "@A": "seeded-v0",
        },
    )

    tide.exec(ObserveSeed, args=["@testing:@A"], writes=["@observe_out"])
    tide.exec(WriteTestingV1, writes=["testing:@A"])

    trace = tide.trace()

    observe_exec = exec_for_actor(trace, "ObserveSeed", grouped=False)
    write_exec = exec_for_actor(trace, "WriteTestingV1", grouped=False)

    assert refs_from_reads(observe_exec) == [
        ("testing:@A", 0)
    ]
    assert refs_from_writes(write_exec) == [
        ("testing:@A", 1)
    ]

    assert_ref_consumed_by_actor(trace, "testing:@A", 0, "ObserveSeed")
    assert_ref_produced_by_actor(trace, "testing:@A", 1, "WriteTestingV1")

# @pytest.mark.xfail(reason="Need to make seeding/import proper handling of this case",strict=False)
def test_multiple_seeded_keys_in_same_store_stay_version_independent():
    def UseSeededA(a):
        return f"A-{a}"

    def UseSeededB(b):
        return f"B-{b}"

    tide = TIDE()
    tide.ocean.register_store(
        "testing",
        store_template={
            "@A": "seeded-A-v0",
            "@B": "seeded-B-v0",
        },
    )

    tide.exec(UseSeededA, args=["@testing:@A"], writes=["@A_out"])
    tide.exec(UseSeededB, args=["@testing:@B"], writes=["@B_out"])
    tide.event("WriteTestingA1", writes={"testing:@A": "A-v1"})

    trace = tide.trace()

    use_a_exec = exec_for_actor(trace, "UseSeededA", grouped=False)
    use_b_exec = exec_for_actor(trace, "UseSeededB", grouped=False)
    write_a_exec = exec_for_actor(trace, "WriteTestingA1", grouped=False)

    assert refs_from_reads(use_a_exec) == [
        ("testing:@A", 0)
    ]
    assert refs_from_reads(use_b_exec) == [
        ("testing:@B", 0)
    ]
    assert refs_from_writes(write_a_exec) == [
        ("testing:@A", 1)
    ]

    b_ref = trace.inspect("testing:@B", "reference", version=0)
    use_b = trace.inspect("UseSeededB", "actor", grouped=False)

    assert trace.path(b_ref, use_b).exists()


def test_registering_same_store_twice_does_not_silently_replace_existing_store_state():
    def ReadTestingA(a):
        return a

    tide = TIDE()
    tide.ocean.register_store(
        "testing",
        store_template={
            "@A": "first-value",
        },
    )

    with pytest.raises(StoreAlreadyRegisteredError):
        tide.ocean.register_store(
            "testing",
            store_template={
                "@A": "second-value",
                "@B": "new-value",
            },
        )


    # tide.exec(ReadTestingA, args=["@testing:@A"], writes=["@observed"])

    # trace = tide.trace()

    # read_exec = exec_for_actor(trace, "ReadTestingA", grouped=False)

    # assert refs_from_reads(read_exec) == [
    #     ("testing:@A", 0)
    # ]

    # first_value = trace.inspect("first-value", "value")
    # observed_ref = trace.inspect("global:@observed", "reference", version=0)

    # assert trace.path(first_value, observed_ref).exists()


def test_store_template_local_key_format_is_valid_for_registered_store_scope():
    def ReadTestingA(a):
        return a

    tide = TIDE()
    tide.ocean.register_store(
        "testing",
        store_template={
            "@A": "local-key-value",
        },
    )

    tide.exec(ReadTestingA, args=["@testing:@A"], writes=["@observed"])

    trace = tide.trace()

    read_exec = exec_for_actor(trace, "ReadTestingA", grouped=False)

    assert refs_from_reads(read_exec) == [
        ("testing:@A", 0)
    ]

    assert_ref_consumed_by_actor(trace, "testing:@A", 0, "ReadTestingA")

# @pytest.mark.xfail(reason="Need to fail this case", strict=False)
def test_store_template_rejects_fully_scoped_keys_because_store_scope_is_already_known():
    tide = TIDE()

    tide.ocean.register_store(
            "testing",
            store_template={
                "testing:@A": "scoped-key-value",
            },
        )

    with pytest.raises(StoreAlreadyRegisteredError):
        tide.ocean.register_store(
            "testing",
            store_template={
                "testing:@A": "scoped-key-value",
            },
        )




# Raw Ocean/Node defaults to runner-local mem.
# TIDE.event/TIDE.exec wrap Ocean with context that defaults user-facing refs to global.


def is_local_mem_ref(ref):
    return isinstance(ref, str) and ref.startswith("mem_") and ":" in ref


def assert_local_mem_ref(ref, key):
    assert is_local_mem_ref(ref)
    assert ref.endswith(f":{key}") or ref.endswith(f":@{key}")


def assert_single_local_mem_read(exec_record, key, version=0):
    reads = refs_from_reads(exec_record)

    assert len(reads) == 1

    ref, ver = reads[0]

    assert ver == version
    assert_local_mem_ref(ref, key)

    return ref


def assert_single_local_mem_write(exec_record, key, version=0):
    writes = refs_from_writes(exec_record)

    assert len(writes) == 1

    ref, ver = writes[0]

    assert ver == version
    assert_local_mem_ref(ref, key)

    return ref


def run_ocean_pipeline(nodes, name, mem_store_templates=None):
    ocean = Ocean()
    pipeline = Pipeline(stages=nodes, name=name)

    return ocean.run_pipeline(
        pipeline,
        mode="sequential",
        mem_store_templates=mem_store_templates or {},
    )


def test_ocean_mem_store_templates_seed_named_local_memory_store_for_one_run():
    def ReadSeed(value):
        return f"read-{value}"

    ocean = Ocean()
    ocean.register_store(name="data_store", scope="local")

    trace = ocean.run_pipeline(
        Pipeline(
            stages=[
                Node(
                    ReadSeed,
                    args=["@data_store:stored_data"],
                    targets=["@result"],
                ),
            ],
            name="named-local-template",
        ),
        mode="sequential",
        mem_store_templates={
            "data_store": {
                "stored_data": "DATA",
            },
        },
    )

    read_exec = exec_for_actor(trace, "ReadSeed", grouped=False)
    assert read_exec.object.output_value == "read-DATA"
    assert refs_from_reads(read_exec) == [("data_store_0:stored_data", 0)]


def test_ocean_mem_store_templates_reject_global_store_without_overwriting_it():
    ocean = Ocean()
    ocean.register_store(
        name="data_store",
        scope="global",
        store_template={"stored_data": "INITIAL"},
    )

    pipeline = Pipeline(
        stages=[
            Node(
                lambda value: value,
                args=["@data_store:stored_data"],
                targets=["@result"],
            ),
        ],
        name="global-template-is-invalid",
    )

    trace = ocean.run_pipeline(pipeline, mode="sequential")
    read_exec = exec_for_actor(trace, "<lambda>", grouped=False)
    assert read_exec.object.output_value == "INITIAL"

    with pytest.raises(
        ValueError,
        match="mem_store_templates cannot seed or overwrite global store",
    ):
        ocean.run_pipeline(
            pipeline,
            mode="sequential",
            mem_store_templates={
                "data_store": {
                    "stored_data": "DATA",
                },
            },
        )

    assert ocean.store_registry.get("data_store").dump() == {
        "stored_data": "INITIAL",
    }


def test_ocean_mem_store_templates_reject_non_memory_local_store():
    class NonMemoryStore(StoreTemplate):
        reference = "non_memory"

        def __init__(
            self,
            name=None,
            store_template=None,
            scope="global",
            parent_runner_id=None,
        ):
            super().__init__(parent_runner_id=parent_runner_id)
            self.name = name or self.reference
            self.scope = scope

    ocean = Ocean()
    ocean.register_store(
        name="custom_store",
        store_class=NonMemoryStore,
        scope="local",
    )

    with pytest.raises(
        ValueError,
        match="mem_store_templates can only seed registered local MemoryKVStore",
    ):
        ocean.run_pipeline(
            Pipeline(
                stages=[Node(lambda: None)],
                name="non-memory-template-is-invalid",
            ),
            mode="sequential",
            mem_store_templates={"custom_store": {"key": "value"}},
        )


def test_ocean_default_unscoped_node_write_is_local_mem_not_global():
    def WriteDefault():
        return "global-value"

    trace = run_ocean_pipeline(
        [
            Node(
                WriteDefault,
                targets=["@A"],
            ),
        ],
        name="default-global-write",
    )

    write_exec = exec_for_actor(trace, "WriteDefault", grouped=False)

    assert_single_local_mem_write(
        write_exec,
        "A",
        version=0,
    )


def test_ocean_local_mem_write_records_physical_scoped_store():
    def WriteLocal():
        return "local-value"

    trace = run_ocean_pipeline(
        [
            Node(
                WriteLocal,
                targets=["mem:local_value"],
            ),
        ],
        name="local-mem-write",
    )

    write_exec = exec_for_actor(trace, "WriteLocal", grouped=False)

    assert_single_local_mem_write(
        write_exec,
        "local_value",
        version=0,
    )


def test_ocean_local_mem_write_can_feed_local_mem_read_in_same_pipeline():
    def ProduceLocal():
        return "local-produced"

    def ConsumeLocal(value):
        return f"consumed-{value}"

    trace = run_ocean_pipeline(
        [
            Node(
                ProduceLocal,
                targets=["mem:local_value"],
            ),
            Node(
                ConsumeLocal,
                args=["@mem:local_value"],
                targets=["@result"],
            ),
        ],
        name="local-mem-write-read",
    )

    produce_exec = exec_for_actor(trace, "ProduceLocal", grouped=False)
    consume_exec = exec_for_actor(trace, "ConsumeLocal", grouped=False)

    written_ref = assert_single_local_mem_write(
        produce_exec,
        "local_value",
        version=0,
    )

    read_ref = assert_single_local_mem_read(
        consume_exec,
        "local_value",
        version=0,
    )

    result_ref = assert_single_local_mem_write(
        consume_exec,
        "result",
        version=0,
    )

    assert read_ref == written_ref
    assert result_ref != written_ref


def test_ocean_mem_store_template_read_routes_to_physical_scoped_runner_store():
    def ReadLocalSeed(value):
        return f"read-{value}"

    trace = run_ocean_pipeline(
        [
            Node(
                ReadLocalSeed,
                args=["@mem:local_seed"],
                targets=["@out"],
            ),
        ],
        name="local-mem-template-read",
        mem_store_templates={
            "mem": {
                "local_seed": "local-template-value",
            },
        },
    )

    read_exec = exec_for_actor(trace, "ReadLocalSeed", grouped=False)

    assert_single_local_mem_read(
        read_exec,
        "local_seed",
        version=0,
    )

    assert_single_local_mem_write(
        read_exec,
        "out",
        version=0,
    )


def test_ocean_global_read_and_local_mem_read_with_same_key_route_to_different_stores():
    def WriteGlobal():
        return "global-value"

    def ReadGlobal(value):
        return f"global-{value}"

    def ReadLocal(value):
        return f"local-{value}"

    trace = run_ocean_pipeline(
        [
            Node(
                WriteGlobal,
                targets=["global:@shared"],
            ),
            Node(
                ReadGlobal,
                args=["@global:@shared"],
                targets=["@global_out"],
            ),
            Node(
                ReadLocal,
                args=["@mem:shared"],
                targets=["@local_out"],
            ),
        ],
        name="global-vs-local-same-key",
        mem_store_templates={
            "mem": {
                "shared": "local-value",
            },
        },
    )

    global_exec = exec_for_actor(trace, "ReadGlobal", grouped=False)
    local_exec = exec_for_actor(trace, "ReadLocal", grouped=False)

    assert refs_from_reads(global_exec) == [
        ("global:@shared", 0)
    ]

    local_ref = assert_single_local_mem_read(
        local_exec,
        "shared",
        version=0,
    )

    assert local_ref != "global:@shared"


def test_ocean_local_mem_store_isolation_between_pipeline_runs():
    def ReadLocalOne(value):
        return f"one-{value}"

    def ReadLocalTwo(value):
        return f"two-{value}"

    ocean = Ocean()

    first_trace = ocean.run_pipeline(
        Pipeline(
            stages=[
                Node(
                    ReadLocalOne,
                    args=["@mem:shared"],
                    targets=["@first_out"],
                ),
            ],
            name="local-mem-run-one",
        ),
        mode="sequential",
        mem_store_templates={
            "mem": {
                "shared": "first-local-value",
            },
        },
    )

    second_trace = ocean.run_pipeline(
        Pipeline(
            stages=[
                Node(
                    ReadLocalTwo,
                    args=["@mem:shared"],
                    targets=["@second_out"],
                ),
            ],
            name="local-mem-run-two",
        ),
        mode="sequential",
        mem_store_templates={
            "mem": {
                "shared": "second-local-value",
            },
        },
    )

    first_exec = exec_for_actor(first_trace, "ReadLocalOne", grouped=False)
    second_exec = exec_for_actor(second_trace, "ReadLocalTwo", grouped=False)

    first_ref = assert_single_local_mem_read(
        first_exec,
        "shared",
        version=0,
    )
    second_ref = assert_single_local_mem_read(
        second_exec,
        "shared",
        version=0,
    )

    assert first_ref != second_ref
    assert first_exec.object.output_value == "one-first-local-value"
    assert second_exec.object.output_value == "two-second-local-value"


def test_ocean_explicit_physical_mem_store_ref_does_not_alias_to_current_runner_local_mem():
    def ProduceLocal():
        return "local-produced"

    def ConsumeLocal(value):
        return f"consumed-{value}"

    first_trace = run_ocean_pipeline(
        [
            Node(
                ProduceLocal,
                targets=["mem:leak_check"],
            ),
        ],
        name="local-mem-physical-source",
    )

    produced_ref = assert_single_local_mem_write(
        exec_for_actor(first_trace, "ProduceLocal", grouped=False),
        "leak_check",
        version=0,
    )

    # A later pipeline should use @mem:leak_check for its own local mem alias.
    # It should not need, nor accidentally resolve through, a previous physical
    # store name such as mem_0:leak_check.
    second_trace = run_ocean_pipeline(
        [
            Node(
                ConsumeLocal,
                args=["@mem:leak_check"],
                targets=["@local_out"],
            ),
        ],
        name="local-mem-physical-isolation",
        mem_store_templates={
            "mem": {
                "leak_check": "second-run-local",
            },
        },
    )

    consume_exec = exec_for_actor(second_trace, "ConsumeLocal", grouped=False)
    consumed_ref = assert_single_local_mem_read(
        consume_exec,
        "leak_check",
        version=0,
    )
    assert consume_exec.object.output_value == "consumed-second-run-local"
    assert is_local_mem_ref(consumed_ref)



# ================================================================
# TIDE DEFAULT STORE SCOPE
# ================================================================
#
# Raw Ocean/Node defaults unscoped refs to runner-local mem.
# TIDE.event / TIDE.exec are user-facing wrappers and should default
# unscoped refs to global instead.
# ================================================================


def test_tide_event_default_unscoped_write_is_global_not_mem():
    tide = TIDE()

    tide.event("WriteDefault", writes={"@A": "global-value"})

    trace = tide.trace()
    write_exec = exec_for_actor(trace, "WriteDefault", grouped=False)

    assert refs_from_writes(write_exec) == [
        ("global:@A", 0)
    ]


def test_tide_event_default_unscoped_read_is_global_not_mem():
    tide = TIDE()

    tide.event("WriteDefault", writes={"@A": "global-value"})
    tide.event("ReadDefault", reads={"@A": "global-value"})

    trace = tide.trace()
    read_exec = exec_for_actor(trace, "ReadDefault", grouped=False)

    assert refs_from_reads(read_exec) == [
        ("global:@A", 0)
    ]


def test_tide_exec_default_unscoped_write_is_global_not_mem():
    def WriteDefault():
        return "global-value"

    tide = TIDE()

    tide.exec(WriteDefault, writes=["@A"])

    trace = tide.trace()
    write_exec = exec_for_actor(trace, "WriteDefault", grouped=False)

    assert refs_from_writes(write_exec) == [
        ("global:@A", 0)
    ]


def test_tide_exec_default_unscoped_read_is_global_not_mem():
    def ReadDefault(a):
        return f"read-{a}"

    tide = TIDE()

    tide.event("WriteDefault", writes={"@A": "global-value"})
    tide.exec(ReadDefault, args=["@@A"], writes=["@out"])

    trace = tide.trace()
    read_exec = exec_for_actor(trace, "ReadDefault", grouped=False)

    assert refs_from_reads(read_exec) == [
        ("global:@A", 0)
    ]

    assert refs_from_writes(read_exec) == [
        ("global:@out", 0)
    ]
