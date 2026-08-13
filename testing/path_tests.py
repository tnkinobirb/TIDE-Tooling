from tide_tooling import TIDE
from tide_tooling.core import Actor, StackLink


# --------------------------------------- #
# TEST HELPERS
# --------------------------------------- #

class ExpectedHop:
    def __init__(
        self,
        pos: tuple,
        expected_type: type,
        string_match=None,
        assert_attrs: list[tuple] = None,
    ):
        self.pos = pos
        self.expected_type = expected_type
        self.string_match = string_match
        self.assert_attrs = assert_attrs or []


# hop_asserts is a list of ExpectedHop objects.
# Each ExpectedHop points to:
#   paths[source][path_n][hop_n]
#
# Example:
#   ExpectedHop(
#       pos=(0, 0, 0),
#       expected_type=StackLink,
#       assert_attrs=[
#           ("producer_actor", Actor(...)),
#           ("consumer_actor", Actor(...)),
#           ("produced_ref", "global:@A"),
#           ("produced_ver", 0),
#       ],
#   )
#
# Or for terminal strings:
#   ExpectedHop(
#       pos=(0, 0, 1),
#       expected_type=str,
#       string_match="read global:@A[v0] from A",
#   )
def assert_path(path, hop_asserts: list, expected_source_reprs: list, should_exist: bool = True):
    path.show()

    paths_source_reprs = [
        repr(trace.inspect(key, "exec_record").object)
        for key in path.paths.keys()
    ]

    paths = list(path.paths.values())

    assert path.exists() == should_exist
    assert paths_source_reprs == expected_source_reprs

    if should_exist is False:
        return

    for hop in hop_asserts:
        hop: ExpectedHop

        source, path_n, hop_n = hop.pos
        to_assert = paths[source][path_n][hop_n]

        assert isinstance(to_assert, hop.expected_type)

        if hop.expected_type == str:
            assert to_assert == hop.string_match
        else:
            for attr, expected in hop.assert_attrs:
                assert getattr(to_assert, attr) == expected


# --------------------------------------- #
# PIPELINE FIXTURE
# --------------------------------------- #
def B():
    pass
def create_pathing_fixture():
    tide = TIDE()
    tide.event("A", writes={"global:@A": "!A"})
    tide.event(B, reads={"global:@A": "!A"}, writes={"global:@B": "!B"})
    tide.event("C", reads={"global:@B": "!B"}, writes={"@C": "!C"})
    tide.event("X", writes={"@C": "!C"})
    return tide.trace()


# --------------------------------------- #
# TRACE
# --------------------------------------- #

trace = create_pathing_fixture()


# --------------------------------------- #
# inspect TESTS
# --------------------------------------- #

a_inspected = trace.inspect("A", "actor", grouped=False)
b_inspected = trace.inspect("B", "actor", grouped=False)
c_inspected = trace.inspect("C", "actor", grouped=False)

assert a_inspected.id == Actor(0, "TIDE: .event()", "A")
assert b_inspected.id == Actor(0, "TIDE: .event()", "B")
assert c_inspected.id == Actor(0, "TIDE: .event()", "C")


# --------------------------------------- #
# PRODUCED / CONSUMED TESTS
# --------------------------------------- #

reads = trace.consumed_by(b_inspected).refs
assert reads == [("global:@A", 0)]

writes = trace.produced_by(b_inspected).refs
assert writes == [("global:@B", 0)]


# --------------------------------------- #
# SAME ACTOR / SAME EXEC PATH TESTS
# --------------------------------------- #

same_exec_pathed_down = trace.path(b_inspected, b_inspected)
same_exec_pathed_up = trace.path(b_inspected, b_inspected, "upstream")

assert_path(
    same_exec_pathed_down,
    [
        ExpectedHop(
            pos=(0, 0, 0),
            expected_type=str,
            string_match="wrote global:@B[v0]",
        )
    ],
    ["EXEC_RECORD[1] B"],
)

assert_path(
    same_exec_pathed_up,
    [
        ExpectedHop(
            pos=(0, 0, 0),
            expected_type=str,
            string_match="read global:@A[v0] from A",
        )
    ],
    ["EXEC_RECORD[1] B"],
)


# --------------------------------------- #
# NEGATIVE PATH TESTS
# --------------------------------------- #

x_inspected = trace.find("X").first("actor")

no_path_down = trace.path(c_inspected, x_inspected)
no_path_up = trace.path(c_inspected, x_inspected, direction="upstream")

assert_path(no_path_down, [], [], should_exist=False)
assert_path(no_path_up, [], [], should_exist=False)


# --------------------------------------- #
# ONE-HOP PATH TESTS
# B -> C
# C <- B
# --------------------------------------- #

one_hop_down = trace.path(b_inspected, c_inspected)
one_hop_up = trace.path(c_inspected, b_inspected, direction="upstream")

assert_path(
    one_hop_down,
    [
        ExpectedHop(
            pos=(0, 0, 0),
            expected_type=StackLink,
            assert_attrs=[
                ("producer_actor", Actor(0, "TIDE: .event()", "B")),
                ("consumer_actor", Actor(0, "TIDE: .event()", "C")),
                ("produced_ref", "global:@B"),
                ("produced_ver", 0),
            ],
        ),
        ExpectedHop(
            pos=(0, 0, 1),
            expected_type=str,
            string_match="read global:@B[v0] from B",
        ),
    ],
    ["EXEC_RECORD[1] B"],
)

assert_path(
    one_hop_up,
    [
        ExpectedHop(
            pos=(0, 0, 0),
            expected_type=StackLink,
            assert_attrs=[
                ("producer_actor", Actor(0, "TIDE: .event()", "B")),
                ("consumer_actor", Actor(0, "TIDE: .event()", "C")),
                ("produced_ref", "global:@B"),
                ("produced_ver", 0),
            ],
        ),
        ExpectedHop(
            pos=(0, 0, 1),
            expected_type=str,
            string_match="wrote global:@B[v0] used by C",
        ),
    ],
    ["EXEC_RECORD[2] C"],
)


# --------------------------------------- #
# MULTI-HOP PATH TESTS
# A -> C
# C <- A
# --------------------------------------- #

multi_hop_down_path = trace.path(a_inspected, c_inspected)

assert_path(
    multi_hop_down_path,
    [
        ExpectedHop(
            pos=(0, 0, 0),
            expected_type=StackLink,
            assert_attrs=[
                ("producer_actor", Actor(0, "TIDE: .event()", "A")),
                ("consumer_actor", Actor(0, "TIDE: .event()", "B")),
                ("produced_ref", "global:@A"),
                ("produced_ver", 0),
            ],
        ),
        ExpectedHop(
            pos=(0, 0, 1),
            expected_type=str,
            string_match="read global:@A[v0] from A",
        ),
        ExpectedHop(
            pos=(0, 0, 2),
            expected_type=StackLink,
            assert_attrs=[
                ("producer_actor", Actor(0, "TIDE: .event()", "B")),
                ("consumer_actor", Actor(0, "TIDE: .event()", "C")),
                ("produced_ref", "global:@B"),
                ("produced_ver", 0),
            ],
        ),
        ExpectedHop(
            pos=(0, 0, 3),
            expected_type=str,
            string_match="read global:@B[v0] from B",
        ),
    ],
    ["EXEC_RECORD[0] A"],
)

multi_hop_up_path = trace.path(c_inspected, a_inspected, direction="upstream")

assert_path(
    multi_hop_up_path,
    [
        ExpectedHop(
            pos=(0, 0, 0),
            expected_type=StackLink,
            assert_attrs=[
                ("producer_actor", Actor(0, "TIDE: .event()", "B")),
                ("consumer_actor", Actor(0, "TIDE: .event()", "C")),
                ("produced_ref", "global:@B"),
                ("produced_ver", 0),
            ],
        ),
        ExpectedHop(
            pos=(0, 0, 1),
            expected_type=str,
            string_match="wrote global:@B[v0] used by C",
        ),
        ExpectedHop(
            pos=(0, 0, 2),
            expected_type=StackLink,
            assert_attrs=[
                ("producer_actor", Actor(0, "TIDE: .event()", "A")),
                ("consumer_actor", Actor(0, "TIDE: .event()", "B")),
                ("produced_ref", "global:@A"),
                ("produced_ver", 0),
            ],
        ),
        ExpectedHop(
            pos=(0, 0, 3),
            expected_type=str,
            string_match="wrote global:@A[v0] used by B",
        ),
    ],
    ["EXEC_RECORD[2] C"],
)


# --------------------------------------- #
# WHY OUTPUT CHECKS / MANUAL VISUAL CHECKS
# --------------------------------------- #

trace.investigate("B", "actor").show()
trace.investigate(1, "seq_id").show()
trace.investigate("global:@B", "reference").show()
trace.investigate("!B", "value").show()


# --------------------------------------- #
# TODO: NEXT SEMANTIC TESTS
# --------------------------------------- #

# reference -> actor/exec
a_ref_inspected = trace.inspect("global:@A", "reference")
b_ref_inspected = trace.inspect("global:@B", "reference")
b_exec_inspected = trace.inspect(1, "seq_id")
b_val_inspected = trace.inspect("!B", "value")

# --------------------------------------- #
# REF-TO PATH TESTS
# REF -> CONSUMER_ACTOR
# REF -> CONSUMER_EXEC
# --------------------------------------- #


ref_to_actor_down_path = trace.path(a_ref_inspected, b_inspected)
assert_path(
    ref_to_actor_down_path,
    [
        ExpectedHop(
            pos=(0, 0, 0),
            expected_type=str,
            string_match="read global:@A[v0] from A",
        )
    ],
    ["EXEC_RECORD[1] B"],
)

ref_to_exec_down_path = trace.path(a_ref_inspected, b_exec_inspected)

assert_path(
    ref_to_exec_down_path,
    [
        ExpectedHop(
            pos=(0, 0, 0),
            expected_type=str,
            string_match="read global:@A[v0] from A",
        )
    ],
    ["EXEC_RECORD[1] B"],
)

# --------------------------------------- #
# ACTOR-TO-REF PATH TESTS
# PRODUCER_ACTOR -> PRODUCED_REF
# CONSUMER_ACTOR <- CONSUMED_REF
# --------------------------------------- #


# actor/exec -> reference
actor_to_ref_down_path = trace.path(a_inspected, a_ref_inspected)

assert_path(
    actor_to_ref_down_path,
    [
        ExpectedHop(
            pos=(0, 0, 0),
            expected_type=str, # TODO: Change str into ReadWriteLink(type='read'|'write', ref, ver , producer) 
            string_match="wrote global:@A[v0]",
        )
    ],
    ["EXEC_RECORD[0] A"],
)


exec_to_ref_up_path = trace.path(b_exec_inspected, a_ref_inspected, direction="upstream")

assert_path(
    exec_to_ref_up_path,
    [
        ExpectedHop(
            pos=(0, 0, 0),
            expected_type=str, # TODO: Change str into ReadWriteLink(type='read'|'write', ref, ver , producer) 
            string_match="read global:@A[v0] from A",
        )
    ],
    ["EXEC_RECORD[1] B"],
)



# value <- actor/exec
# --------------------------------------- #
# VAL-TO-ACTOR PATH TESTS
# CONSUMED_VAL -> CONSUMER_ACTOR
# PRODUCED_VAL <- PRODUCER_ACTOR
# --------------------------------------- #


value_to_actor_down_path = trace.path(b_val_inspected, c_inspected)

assert_path(
    value_to_actor_down_path,
    [
        ExpectedHop(
            pos=(0, 0, 0),
            expected_type=str, # TODO: Change str into ReadWriteLink(type='read'|'write', ref, ver , producer) 
            string_match="read as global:@B[v0] from B",
        )
    ],
    ["EXEC_RECORD[2] C"],
)

value_to_actor_up_path = trace.path(b_val_inspected, b_inspected, direction="upstream")


assert_path(
    value_to_actor_up_path,
    [
        ExpectedHop(
            pos=(0, 0, 0),
            expected_type=str, # TODO: Change str into ReadWriteLink(type='read'|'write', ref, ver , producer) 
            string_match="written as global:@B[v0]",
        )
    ],
    ["EXEC_RECORD[1] B"],
)


# --------------------------------------- #
# VAL-TO-ACTOR PATH TESTS
# CONSUMED_VAL -> CONSUMER_ACTOR
# PRODUCED_VAL <- PRODUCER_ACTOR
# --------------------------------------- #

value_to_exec_down_path = trace.path(b_val_inspected, c_inspected)

assert_path(
    value_to_exec_down_path,
    [
        ExpectedHop(
            pos=(0, 0, 0),
            expected_type=str, # TODO: Change str into ReadWriteLink(type='read'|'write', ref, ver , producer) 
            string_match="read as global:@B[v0] from B",
        )
    ],
    ["EXEC_RECORD[2] C"],
)



value_to_exec_up_path = trace.path(b_val_inspected, b_inspected, direction="upstream")

assert_path(
    value_to_exec_up_path,
    [
        ExpectedHop(
            pos=(0, 0, 0),
            expected_type=str, # TODO: Change str into ReadWriteLink(type='read'|'write', ref, ver , producer) 
            string_match="written as global:@B[v0]",
        )
    ],
    ["EXEC_RECORD[1] B"],
)


# --------------------------------------- #
# VAL-TO-ACTOR REF TESTS
# CONSUMED_VAL -> CONSUMED_REF
# PRODUCED_VAL <- PRODUCED_REF
# --------------------------------------- #

value_to_ref_down_path = trace.path(b_val_inspected, b_ref_inspected)

assert_path(
    value_to_ref_down_path,
    [
        ExpectedHop(
            pos=(0, 0, 0),
            expected_type=str, # TODO: Change str into ReadWriteLink(type='read'|'write', ref, ver , producer) 
            string_match="read as global:@B[v0] from B",
        )
    ],
    ["EXEC_RECORD[2] C"],
)


value_to_ref_up_path = trace.path(b_val_inspected, b_ref_inspected, direction="upstream")


assert_path(
    value_to_ref_up_path,
    [
        ExpectedHop(
            pos=(0, 0, 0),
            expected_type=str, # TODO: Change str into ReadWriteLink(type='read'|'write', ref, ver , producer) 
            string_match="written as global:@B[v0]",
        )
    ],
    ["EXEC_RECORD[1] B"],
)



print("Tests Passed :)")
