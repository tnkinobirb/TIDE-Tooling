import pytest
import re

ANSI_ESCAPE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")

def strip_ansi(value: str) -> str:
    return ANSI_ESCAPE.sub("", value)

from tide_tooling import TIDE, node, pipeline
from tide_tooling.core import Actor, ReadWriteLink, StackLink


class ExpectedHop:
    def __init__(
        self,
        pos: tuple,
        expected_type: type,
        string_match=None,
        assert_attrs: list[tuple] = None,
    ):
        self.pos = pos
        self.legacy_text = expected_type == str
        self.expected_type = ReadWriteLink if self.legacy_text else expected_type
        self.string_match = string_match
        self.assert_attrs = assert_attrs or []


def assert_path(
    path,
    hop_asserts: list,
    expected_source_reprs: list,
    should_exist: bool = True,
    source_trace=None,
    expected_source_count: int = None,
    expected_path_counts: list[int] = None,
    expected_hop_counts: list[list[int]] = None,
):
    if source_trace is None:
        raise ValueError("source_trace is required for deterministic path assertions")

    paths = list(path.paths.values())
    paths_source_reprs = [
        (
            f"EXEC_RECORD[{source_trace.index.exec_by_exec_id[key].seq_id}] "
            f"{source_trace.index.exec_by_exec_id[key].producer_id.source_actor}"
        )
        for key in path.paths
    ]
    path.show()
    assert path.exists() == should_exist
    assert bool(path.paths) == should_exist
    assert bool(path) == should_exist
    assert paths_source_reprs == expected_source_reprs

    if expected_source_count is not None:
        assert len(paths) == expected_source_count

    if expected_path_counts is not None:
        actual_path_counts = [
            len(source_paths)
            for source_paths in paths
        ]

        assert actual_path_counts == expected_path_counts
        assert path.count() == sum(expected_path_counts)

    if expected_hop_counts is not None:
        actual_hop_counts = [
            [
                len(path_hops)
                for path_hops in source_paths
            ]
            for source_paths in paths
        ]
        assert actual_hop_counts == expected_hop_counts

    if should_exist is False:
        assert not hop_asserts
        return

    asserted_positions = {
        hop.pos
        for hop in hop_asserts
    }
    actual_positions = {
        (source_n, path_n, hop_n)
        for source_n, source_paths in enumerate(paths)
        for path_n, path_hops in enumerate(source_paths)
        for hop_n, _ in enumerate(path_hops)
    }
    assert asserted_positions == actual_positions

    for hop in hop_asserts:
        source_n, path_n, hop_n = hop.pos
        to_assert = paths[source_n][path_n][hop_n]

        if hop.legacy_text:
            # Legacy expectations are retained as display contracts, but
            # terminal hops are now typed ReadWriteLink objects.
            assert isinstance(to_assert, ReadWriteLink)
            assert str(to_assert) == hop.string_match
            continue

        assert isinstance(to_assert, hop.expected_type)

        for attr, expected in hop.assert_attrs:
            actual = getattr(to_assert, attr)

            if isinstance(expected, Actor):
                assert isinstance(actual, Actor)
                assert actual.id == expected.id
            else:
                assert actual == expected


def B():
    pass


@pytest.fixture(scope="module")
def path_context():
    tide = TIDE()
    tide.event("A", writes={"global:@A": "!A"})
    tide.event(
        B,
        reads={"global:@A": "!A"},
        writes={"global:@B": "!B"},
    )
    tide.event(
        "C",
        reads={"global:@B": "!B"},
        writes={"global:@C": "!C"},
    )
    tide.event(
        "X",
        writes={
            "global:@B": "!B",
            "global:@C": "!C",
        },
    )

    trace = tide.trace()
    main_ocean_id = trace.ocean_id

    a_inspected = trace.inspect("A", "actor", grouped=False)
    b_inspected = trace.inspect("B", "actor", grouped=False)
    c_inspected = trace.inspect("C", "actor", grouped=False)

    a_exec_inspected = trace.inspect(0, "seq_id")
    b_exec_inspected = trace.inspect(1, "seq_id")
    c_exec_inspected = trace.inspect(2, "seq_id")

    a_ref_inspected = trace.inspect(
        "global:@A",
        "reference",
        grouped=False,
    )
    b_ref_inspected = trace.inspect(
        "global:@B",
        "reference",
        grouped=False,
    )
    b_ref_grouped = trace.inspect("@B", "reference")
    b_val_inspected = trace.inspect("!B", "value")

    return locals()


def unpack_base(path_context):
    return (
        path_context["trace"],
        path_context["main_ocean_id"],
        path_context["a_inspected"],
        path_context["b_inspected"],
        path_context["c_inspected"],
        path_context["a_exec_inspected"],
        path_context["b_exec_inspected"],
        path_context["c_exec_inspected"],
        path_context["a_ref_inspected"],
        path_context["b_ref_inspected"],
        path_context["b_ref_grouped"],
        path_context["b_val_inspected"],
    )


def test_path_fixture_and_ref_queries(path_context):
    (
        trace,
        main_ocean_id,
        a_inspected,
        b_inspected,
        c_inspected,
        a_exec_inspected,
        b_exec_inspected,
        c_exec_inspected,
        a_ref_inspected,
        b_ref_inspected,
        b_ref_grouped,
        b_val_inspected,
    ) = unpack_base(path_context)

    assert a_inspected.id == Actor(
        main_ocean_id,
        "TIDE: .event()",
        "A",
    )
    assert b_inspected.id == Actor(
        main_ocean_id,
        "TIDE: .event()",
        "B",
    )
    assert c_inspected.id == Actor(
        main_ocean_id,
        "TIDE: .event()",
        "C",
    )

    assert b_ref_grouped.grouped
    assert b_ref_grouped.history == [
        ("global:@B", 0),
        ("global:@B", 1),
    ]

    assert trace.consumed_by(b_inspected).refs == [
        ("global:@A", 0)
    ]
    assert trace.produced_by(b_inspected).refs == [
        ("global:@B", 0)
    ]

def test_actor_source_paths(path_context):
    (
        trace,
        main_ocean_id,
        a_inspected,
        b_inspected,
        c_inspected,
        a_exec_inspected,
        b_exec_inspected,
        c_exec_inspected,
        a_ref_inspected,
        b_ref_inspected,
        b_ref_grouped,
        b_val_inspected,
    ) = unpack_base(path_context)

    def check_path(*args, **kwargs):
        kwargs.setdefault("source_trace", trace)
        return assert_path(*args, **kwargs)

    # SAME ACTOR / SAME EXEC PATH TESTS
        # --------------------------------------- #

    same_exec_pathed_down = trace.path(b_inspected, b_inspected)
    same_exec_pathed_up = trace.path(
        b_inspected,
        b_inspected,
        direction="upstream",
        deep_link=False,
    )

    check_path(
        same_exec_pathed_down,
        [
            ExpectedHop(
                pos=(0, 0, 0),
                expected_type=str,
                string_match="wrote global:@B[v0]",
            )
        ],
        ["EXEC_RECORD[1] B"],
        expected_source_count=1,
        expected_path_counts=[1],
        expected_hop_counts=[[1]],
    )

    check_path(
        same_exec_pathed_up,
        [
            ExpectedHop(
                pos=(0, 0, 0),
                expected_type=str,
                string_match="read global:@A[v0] from A",
            )
        ],
        ["EXEC_RECORD[1] B"],
        expected_source_count=1,
        expected_path_counts=[1],
        expected_hop_counts=[[1]],
    )


    # --------------------------------------- #
    # NEGATIVE PATH TESTS
    # --------------------------------------- #

    x_inspected = trace.find("X").first("actor")

    no_path_down = trace.path(c_inspected, x_inspected)
    no_path_up = trace.path(c_inspected, x_inspected, direction="upstream")

    check_path(no_path_down, [], [], should_exist=False)
    check_path(no_path_up, [], [], should_exist=False)


    # --------------------------------------- #
    # ONE-HOP PATH TESTS
    # B -> C
    # C <- B
    # --------------------------------------- #

    one_hop_down = trace.path(b_inspected, c_inspected)
    one_hop_up = trace.path(c_inspected, b_inspected, direction="upstream")

    check_path(
        one_hop_down,
        [
            ExpectedHop(
                pos=(0, 0, 0),
                expected_type=StackLink,
                assert_attrs=[
                    ("producer_actor", Actor(main_ocean_id, "TIDE: .event()", "B")),
                    ("consumer_actor", Actor(main_ocean_id, "TIDE: .event()", "C")),
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
        expected_source_count=1,
        expected_path_counts=[1],
        expected_hop_counts=[[2]],
    )

    check_path(
        one_hop_up,
        [
            ExpectedHop(
                pos=(0, 0, 0),
                expected_type=StackLink,
                assert_attrs=[
                    ("producer_actor", Actor(main_ocean_id, "TIDE: .event()", "B")),
                    ("consumer_actor", Actor(main_ocean_id, "TIDE: .event()", "C")),
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
        expected_source_count=1,
        expected_path_counts=[1],
        expected_hop_counts=[[2]],
    )


    # --------------------------------------- #
    # MULTI-HOP PATH TESTS
    # A -> C
    # C <- A
    # --------------------------------------- #

    multi_hop_down_path = trace.path(a_inspected, c_inspected, deep_link=True)

    check_path(
        multi_hop_down_path,
        [
            ExpectedHop(
                pos=(0, 0, 0),
                expected_type=StackLink,
                assert_attrs=[
                    ("producer_actor", Actor(main_ocean_id, "TIDE: .event()", "A")),
                    ("consumer_actor", Actor(main_ocean_id, "TIDE: .event()", "B")),
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
                    ("producer_actor", Actor(main_ocean_id, "TIDE: .event()", "B")),
                    ("consumer_actor", Actor(main_ocean_id, "TIDE: .event()", "C")),
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
        expected_source_count=1,
        expected_path_counts=[1],
        expected_hop_counts=[[4]],
    )

    multi_hop_up_path = trace.path(c_inspected, a_inspected, direction="upstream", deep_link=True)

    check_path(
        multi_hop_up_path,
        [
            ExpectedHop(
                pos=(0, 0, 0),
                expected_type=StackLink,
                assert_attrs=[
                    ("producer_actor", Actor(main_ocean_id, "TIDE: .event()", "B")),
                    ("consumer_actor", Actor(main_ocean_id, "TIDE: .event()", "C")),
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
                    ("producer_actor", Actor(main_ocean_id, "TIDE: .event()", "A")),
                    ("consumer_actor", Actor(main_ocean_id, "TIDE: .event()", "B")),
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
        expected_source_count=1,
        expected_path_counts=[1],
        expected_hop_counts=[[4]],
    )

    # --------------------------------------- #
    # ACTOR -> EXEC
    # EXACT EXECUTION AGAINST ACTOR IDENTITY
    # --------------------------------------- #

    actor_to_exec_down_path = trace.path(a_inspected, b_exec_inspected)

    check_path(
        actor_to_exec_down_path,
        [
            ExpectedHop(
                pos=(0, 0, 0),
                expected_type=StackLink,
                assert_attrs=[
                    ("producer_actor", Actor(main_ocean_id, "TIDE: .event()", "A")),
                    ("consumer_actor", Actor(main_ocean_id, "TIDE: .event()", "B")),
                    ("produced_ref", "global:@A"),
                    ("produced_ver", 0),
                ],
            ),
            ExpectedHop(
                pos=(0, 0, 1),
                expected_type=str,
                string_match="read global:@A[v0] from A",
            ),
        ],
        ["EXEC_RECORD[0] A"],
        expected_source_count=1,
        expected_path_counts=[1],
        expected_hop_counts=[[2]],
    )

    actor_to_exec_up_path = trace.path(
        b_inspected,
        a_exec_inspected,
        direction="upstream",
    )

    check_path(
        actor_to_exec_up_path,
        [
            ExpectedHop(
                pos=(0, 0, 0),
                expected_type=StackLink,
                assert_attrs=[
                    ("producer_actor", Actor(main_ocean_id, "TIDE: .event()", "A")),
                    ("consumer_actor", Actor(main_ocean_id, "TIDE: .event()", "B")),
                    ("produced_ref", "global:@A"),
                    ("produced_ver", 0),
                ],
            ),
            ExpectedHop(
                pos=(0, 0, 1),
                expected_type=str,
                string_match="wrote global:@A[v0] used by B",
            ),
        ],
        ["EXEC_RECORD[1] B"],
        expected_source_count=1,
        expected_path_counts=[1],
        expected_hop_counts=[[2]],
    )

    # --------------------------------------- #
    # ACTOR -> REF
    # PRODUCER_ACTOR -> PRODUCED_REF
    # --------------------------------------- #

    actor_to_ref_down_path = trace.path(a_inspected, a_ref_inspected)

    check_path(
        actor_to_ref_down_path,
        [
            ExpectedHop(
                pos=(0, 0, 0),
                expected_type=str,
                string_match="wrote global:@A[v0]",
            ),
            ExpectedHop(
                pos=(0, 1, 0),
                expected_type=StackLink,
                assert_attrs=[
                    ("producer_actor", Actor(main_ocean_id, "TIDE: .event()", "A")),
                    ("consumer_actor", Actor(main_ocean_id, "TIDE: .event()", "B")),
                    ("produced_ref", "global:@A"),
                    ("produced_ver", 0),
                ],
            ),
            ExpectedHop(
                pos=(0, 1, 1),
                expected_type=str,
                string_match="read global:@A[v0] from A",
            ),
        ],
        ["EXEC_RECORD[0] A"],
        expected_source_count=1,
        expected_path_counts=[2],
        expected_hop_counts=[[1, 2]],
    )

    # --------------------------------------- #
    # ACTOR -> VALUE
    # PRODUCER_ACTOR -> PRODUCED_VALUE
    # --------------------------------------- #

    actor_to_value_down_path = trace.path(b_inspected, b_val_inspected)

    check_path(
        actor_to_value_down_path,
        [
            ExpectedHop(
                pos=(0, 0, 0),
                expected_type=str,
                string_match="written as global:@B[v0]",
            ),
            ExpectedHop(
                pos=(0, 1, 0),
                expected_type=StackLink,
                assert_attrs=[
                    ("producer_actor", Actor(main_ocean_id, "TIDE: .event()", "B")),
                    ("consumer_actor", Actor(main_ocean_id, "TIDE: .event()", "C")),
                    ("produced_ref", "global:@B"),
                    ("produced_ver", 0),
                ],
            ),
            ExpectedHop(
                pos=(0, 1, 1),
                expected_type=str,
                string_match="read as global:@B[v0] from B",
            ),
        ],
        ["EXEC_RECORD[1] B"],
        expected_source_count=1,
        expected_path_counts=[2],
        expected_hop_counts=[[1, 2]],
    )

    # ================================================================ #

def test_exec_source_paths(path_context):
    (
        trace,
        main_ocean_id,
        a_inspected,
        b_inspected,
        c_inspected,
        a_exec_inspected,
        b_exec_inspected,
        c_exec_inspected,
        a_ref_inspected,
        b_ref_inspected,
        b_ref_grouped,
        b_val_inspected,
    ) = unpack_base(path_context)

    def check_path(*args, **kwargs):
        kwargs.setdefault("source_trace", trace)
        return assert_path(*args, **kwargs)

    # EXEC SOURCE PATH TESTS
    # ================================================================ #

    exec_to_actor_down_path = trace.path(a_exec_inspected, b_inspected)

    check_path(
        exec_to_actor_down_path,
        [
            ExpectedHop(
                pos=(0, 0, 0),
                expected_type=StackLink,
                assert_attrs=[
                    ("producer_actor", Actor(main_ocean_id, "TIDE: .event()", "A")),
                    ("consumer_actor", Actor(main_ocean_id, "TIDE: .event()", "B")),
                    ("produced_ref", "global:@A"),
                    ("produced_ver", 0),
                ],
            ),
            ExpectedHop(
                pos=(0, 0, 1),
                expected_type=str,
                string_match="read global:@A[v0] from A",
            ),
        ],
        ["EXEC_RECORD[0] A"],
        expected_source_count=1,
        expected_path_counts=[1],
        expected_hop_counts=[[2]],
    )

    exec_to_actor_up_path = trace.path(
        b_exec_inspected,
        a_inspected,
        direction="upstream",
    )

    check_path(
        exec_to_actor_up_path,
        [
            ExpectedHop(
                pos=(0, 0, 0),
                expected_type=StackLink,
                assert_attrs=[
                    ("producer_actor", Actor(main_ocean_id, "TIDE: .event()", "A")),
                    ("consumer_actor", Actor(main_ocean_id, "TIDE: .event()", "B")),
                    ("produced_ref", "global:@A"),
                    ("produced_ver", 0),
                ],
            ),
            ExpectedHop(
                pos=(0, 0, 1),
                expected_type=str,
                string_match="wrote global:@A[v0] used by B",
            ),
        ],
        ["EXEC_RECORD[1] B"],
        expected_source_count=1,
        expected_path_counts=[1],
        expected_hop_counts=[[2]],
    )

    # --------------------------------------- #
    # EXEC-TO-EXEC PATH TESTS
    # EXACT HISTORICAL EXECUTIONS
    # --------------------------------------- #

    exec_to_exec_one_hop_down = trace.path(a_exec_inspected, b_exec_inspected)

    check_path(
        exec_to_exec_one_hop_down,
        [
            ExpectedHop(
                pos=(0, 0, 0),
                expected_type=StackLink,
                assert_attrs=[
                    ("producer_actor", Actor(main_ocean_id, "TIDE: .event()", "A")),
                    ("consumer_actor", Actor(main_ocean_id, "TIDE: .event()", "B")),
                    ("produced_ref", "global:@A"),
                    ("produced_ver", 0),
                ],
            ),
            ExpectedHop(
                pos=(0, 0, 1),
                expected_type=str,
                string_match="read global:@A[v0] from A",
            ),
        ],
        ["EXEC_RECORD[0] A"],
        expected_source_count=1,
        expected_path_counts=[1],
        expected_hop_counts=[[2]],
    )

    exec_to_exec_one_hop_up = trace.path(
        b_exec_inspected,
        a_exec_inspected,
        direction="upstream",
    )

    check_path(
        exec_to_exec_one_hop_up,
        [
            ExpectedHop(
                pos=(0, 0, 0),
                expected_type=StackLink,
                assert_attrs=[
                    ("producer_actor", Actor(main_ocean_id, "TIDE: .event()", "A")),
                    ("consumer_actor", Actor(main_ocean_id, "TIDE: .event()", "B")),
                    ("produced_ref", "global:@A"),
                    ("produced_ver", 0),
                ],
            ),
            ExpectedHop(
                pos=(0, 0, 1),
                expected_type=str,
                string_match="wrote global:@A[v0] used by B",
            ),
        ],
        ["EXEC_RECORD[1] B"],
        expected_source_count=1,
        expected_path_counts=[1],
        expected_hop_counts=[[2]],
    )

    exec_to_exec_multi_hop_down = trace.path(a_exec_inspected, c_exec_inspected, deep_link=True)

    check_path(
        exec_to_exec_multi_hop_down,
        [
            ExpectedHop(
                pos=(0, 0, 0),
                expected_type=StackLink,
                assert_attrs=[
                    ("producer_actor", Actor(main_ocean_id, "TIDE: .event()", "A")),
                    ("consumer_actor", Actor(main_ocean_id, "TIDE: .event()", "B")),
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
                    ("producer_actor", Actor(main_ocean_id, "TIDE: .event()", "B")),
                    ("consumer_actor", Actor(main_ocean_id, "TIDE: .event()", "C")),
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
        expected_source_count=1,
        expected_path_counts=[1],
        expected_hop_counts=[[4]],
    )

    exec_to_exec_multi_hop_up = trace.path(
        c_exec_inspected,
        a_exec_inspected,
        direction="upstream",
        deep_link=True
    )

    check_path(
        exec_to_exec_multi_hop_up,
        [
            ExpectedHop(
                pos=(0, 0, 0),
                expected_type=StackLink,
                assert_attrs=[
                    ("producer_actor", Actor(main_ocean_id, "TIDE: .event()", "B")),
                    ("consumer_actor", Actor(main_ocean_id, "TIDE: .event()", "C")),
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
                    ("producer_actor", Actor(main_ocean_id, "TIDE: .event()", "A")),
                    ("consumer_actor", Actor(main_ocean_id, "TIDE: .event()", "B")),
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
        expected_source_count=1,
        expected_path_counts=[1],
        expected_hop_counts=[[4]],
    )

    # --------------------------------------- #
    # EXEC -> REF
    # CONSUMER_EXEC <- CONSUMED_REF
    # --------------------------------------- #

    exec_to_ref_up_path = trace.path(b_exec_inspected, a_ref_inspected, direction="upstream")

    check_path(
        exec_to_ref_up_path,
        [
            ExpectedHop(
                pos=(0, 0, 0),
                expected_type=StackLink,
                assert_attrs=[
                    ("producer_actor", Actor(main_ocean_id, "TIDE: .event()", "A")),
                    ("consumer_actor", Actor(main_ocean_id, "TIDE: .event()", "B")),
                    ("produced_ref", "global:@A"),
                    ("produced_ver", 0),
                ],
            ),
            ExpectedHop(
                pos=(0, 0, 1),
                expected_type=str,
                string_match="wrote global:@A[v0] used by B",
            ),
        ],
        ["EXEC_RECORD[1] B"],
        expected_source_count=1,
        expected_path_counts=[1],
        expected_hop_counts=[[2]],
    )

    # --------------------------------------- #
    # EXEC -> VALUE
    # PRODUCER_EXEC -> PRODUCED_VALUE
    # --------------------------------------- #

    exec_to_value_down_path = trace.path(b_exec_inspected, b_val_inspected)

    check_path(
        exec_to_value_down_path,
        [
            ExpectedHop(
                pos=(0, 0, 0),
                expected_type=str,
                string_match="written as global:@B[v0]",
            ),
            ExpectedHop(
                pos=(0, 1, 0),
                expected_type=StackLink,
                assert_attrs=[
                    ("producer_actor", Actor(main_ocean_id, "TIDE: .event()", "B")),
                    ("consumer_actor", Actor(main_ocean_id, "TIDE: .event()", "C")),
                    ("produced_ref", "global:@B"),
                    ("produced_ver", 0),
                ],
            ),
            ExpectedHop(
                pos=(0, 1, 1),
                expected_type=str,
                string_match="read as global:@B[v0] from B",
            ),
        ],
        ["EXEC_RECORD[1] B"],
        expected_source_count=1,
        expected_path_counts=[2],
        expected_hop_counts=[[1, 2]],
    )

    # ================================================================ #

def test_reference_source_paths(path_context):
    (
        trace,
        main_ocean_id,
        a_inspected,
        b_inspected,
        c_inspected,
        a_exec_inspected,
        b_exec_inspected,
        c_exec_inspected,
        a_ref_inspected,
        b_ref_inspected,
        b_ref_grouped,
        b_val_inspected,
    ) = unpack_base(path_context)

    def check_path(*args, **kwargs):
        kwargs.setdefault("source_trace", trace)
        return assert_path(*args, **kwargs)

    # REF SOURCE PATH TESTS
    # ================================================================ #



    # --------------------------------------- #
    # REF -> ACTOR
    # CONSUMED_REF -> CONSUMER_ACTOR
    # --------------------------------------- #

    ref_to_actor_down_path = trace.path(a_ref_inspected, b_inspected)
    check_path(
        ref_to_actor_down_path,
        [
            ExpectedHop(
                pos=(0, 0, 0),
                expected_type=StackLink,
                assert_attrs=[
                    ("producer_actor", Actor(main_ocean_id, "TIDE: .event()", "A")),
                    ("consumer_actor", Actor(main_ocean_id, "TIDE: .event()", "B")),
                    ("produced_ref", "global:@A"),
                    ("produced_ver", 0),
                ],
            ),
            ExpectedHop(
                pos=(0, 0, 1),
                expected_type=str,
                string_match="read global:@A[v0] from A",
            )
        ],
        ["EXEC_RECORD[0] A"],
        expected_source_count=1,
        expected_path_counts=[1],
        expected_hop_counts=[[2]],
    )

    # --------------------------------------- #
    # REF -> EXEC
    # CONSUMED_REF -> CONSUMER_EXEC
    # --------------------------------------- #

    ref_to_exec_down_path = trace.path(a_ref_inspected, b_exec_inspected)

    check_path(
        ref_to_exec_down_path,
        [
            ExpectedHop(
                pos=(0, 0, 0),
                expected_type=StackLink,
                assert_attrs=[
                    ("producer_actor", Actor(main_ocean_id, "TIDE: .event()", "A")),
                    ("consumer_actor", Actor(main_ocean_id, "TIDE: .event()", "B")),
                    ("produced_ref", "global:@A"),
                    ("produced_ver", 0),
                ],
            ),
            ExpectedHop(
                pos=(0, 0, 1),
                expected_type=str,
                string_match="read global:@A[v0] from A",
            )
        ],
        ["EXEC_RECORD[0] A"],
        expected_source_count=1,
        expected_path_counts=[1],
        expected_hop_counts=[[2]],
    )

    # --------------------------------------- #
    # REF -> REF PATH TESTS
    # GROUPED / UNVERSIONED AND EXACT VERSION BEHAVIOUR
    # --------------------------------------- #

    grouped_b_ref_down = trace.path(b_ref_grouped, b_ref_grouped)

    check_path(
        grouped_b_ref_down,
        [
            ExpectedHop(
                pos=(0, 0, 0),
                expected_type=StackLink,
                assert_attrs=[
                    ("producer_actor", Actor(trace.ocean_id, "TIDE: .event()", "B")),
                    ("consumer_actor", Actor(trace.ocean_id, "TIDE: .event()", "C")),
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
        expected_source_count=1,
        expected_path_counts=[1],
        expected_hop_counts=[[2]],
    )

    grouped_b_ref_up = trace.path(
        b_ref_grouped,
        b_ref_grouped,
        direction="upstream",
    )

    check_path(
        grouped_b_ref_up,
        [
            ExpectedHop(
                pos=(0, 0, 0),
                expected_type=str,
                string_match="wrote global:@B[v0]",
            ),
            ExpectedHop(
                pos=(1, 0, 0),
                expected_type=str,
                string_match="wrote global:@B[v1]",
            ),
        ],
        ["EXEC_RECORD[1] B", "EXEC_RECORD[3] X"],
        expected_source_count=2,
        expected_path_counts=[1, 1],
        expected_hop_counts=[[1], [1]],
    )

    b_ref_inspect_ver_0 = trace.inspect("global:@B", "reference", 0)
    b_ref_inspect_ver_1 = trace.inspect("global:@B", "reference", 1)


    ref_to_same_ref_ver_down = trace.path(b_ref_inspected, b_ref_inspect_ver_0)

    check_path(
        ref_to_same_ref_ver_down,
        [  
            ExpectedHop(
                pos=(0, 0, 0),
                expected_type=StackLink,
                assert_attrs=[
                    ("producer_actor", Actor(main_ocean_id, "TIDE: .event()", "B")),
                    ("consumer_actor", Actor(main_ocean_id, "TIDE: .event()", "C")),
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
        expected_source_count=1,
        expected_path_counts=[1],
        expected_hop_counts=[[2]],
    )

    ref_to_same_ref_ver_up = trace.path(
        b_ref_inspected,
        b_ref_inspect_ver_1,
        direction="upstream",
        deep_link=False,
    )
    check_path(
        ref_to_same_ref_ver_up,
        [
            ExpectedHop(
                pos=(0, 0, 0),
                expected_type=str, # TODO: Change str into ReadWriteLink(type='read'|'write', ref, ver , producer) 
                string_match="wrote global:@B[v1]",
            )
        ],
        ["EXEC_RECORD[3] X"],
        expected_source_count=1,
        expected_path_counts=[1],
        expected_hop_counts=[[1]],
    )
    # 


    ref_to_ref_down_path = trace.path(
        a_ref_inspected,
        b_ref_inspected,
        deep_link=False,
    )
    check_path(
        ref_to_ref_down_path,
        [],
        [],
        should_exist=False,
    )

    ref_to_ref_up_path = trace.path(
        b_ref_inspected,
        a_ref_inspected,
        direction="upstream",
        deep_link=False,
    )
    check_path(
        ref_to_ref_up_path,
        [],
        [],
        should_exist=False,
    )

    # Deep links between references follow the execution chain between
    # the producer of the source version and the producer of the target version.
    c_ref_inspected = trace.inspect(
        "global:@C",
        "reference",
        grouped=False,
    )

    ref_to_ref_deep_down = trace.path(
        a_ref_inspected,
        c_ref_inspected,
        deep_link=True,
    )

    check_path(
        ref_to_ref_deep_down,
        [
            ExpectedHop(
                pos=(0, 0, 0),
                expected_type=StackLink,
                assert_attrs=[
                    ("producer_actor", Actor(main_ocean_id, "TIDE: .event()", "A")),
                    ("consumer_actor", Actor(main_ocean_id, "TIDE: .event()", "B")),
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
                    ("producer_actor", Actor(main_ocean_id, "TIDE: .event()", "B")),
                    ("consumer_actor", Actor(main_ocean_id, "TIDE: .event()", "C")),
                    ("produced_ref", "global:@B"),
                    ("produced_ver", 0),
                ],
            ),
            ExpectedHop(
                pos=(0, 0, 3),
                expected_type=str,
                string_match="read global:@B[v0] from B",
            ),
            ExpectedHop(
                pos=(0, 0, 4),
                expected_type=str,
                string_match="wrote global:@C[v0]",
            ),
        ],
        ["EXEC_RECORD[0] A"],
        expected_source_count=1,
        expected_path_counts=[1],
        expected_hop_counts=[[5]],
    )

    ref_to_ref_deep_up = trace.path(
        c_ref_inspected,
        a_ref_inspected,
        direction="upstream",
        deep_link=True,
    )

    check_path(
        ref_to_ref_deep_up,
        [
            ExpectedHop(
                pos=(0, 0, 0),
                expected_type=StackLink,
                assert_attrs=[
                    ("producer_actor", Actor(main_ocean_id, "TIDE: .event()", "B")),
                    ("consumer_actor", Actor(main_ocean_id, "TIDE: .event()", "C")),
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
                    ("producer_actor", Actor(main_ocean_id, "TIDE: .event()", "A")),
                    ("consumer_actor", Actor(main_ocean_id, "TIDE: .event()", "B")),
                    ("produced_ref", "global:@A"),
                    ("produced_ver", 0),
                ],
            ),
            ExpectedHop(
                pos=(0, 0, 3),
                expected_type=str,
                string_match="wrote global:@A[v0]",
            ),
        ],
        ["EXEC_RECORD[2] C"],
        expected_source_count=1,
        expected_path_counts=[1],
        expected_hop_counts=[[4]],
    )

    output = ref_to_ref_deep_down.show(explore_tool_tips=False)
    plain_output = strip_ansi(output)

    assert "EXEC_RECORD[2] C | read global:@B[v0] from B" in plain_output
    assert "        └── wrote global:@C[v0]" in plain_output
    assert "\033[31mglobal:@C[v0]" in output

    # --------------------------------------- #
    # REF -> VALUE
    # REF -> STORED_VALUE
    # --------------------------------------- #

    ref_to_value_down_path = trace.path(b_ref_inspected, b_val_inspected)

    check_path(
        ref_to_value_down_path,
        [
            ExpectedHop(
                pos=(0, 0, 0),
                expected_type=StackLink,
                assert_attrs=[
                    ("producer_actor", Actor(main_ocean_id, "TIDE: .event()", "B")),
                    ("consumer_actor", Actor(main_ocean_id, "TIDE: .event()", "C")),
                    ("produced_ref", "global:@B"),
                    ("produced_ver", 0),
                ],
            ),
            ExpectedHop(
                pos=(0, 0, 1),
                expected_type=str,
                string_match="read as global:@B[v0] from B",
            )
        ],
        ["EXEC_RECORD[1] B"],
        expected_source_count=1,
        expected_path_counts=[1],
        expected_hop_counts=[[2]],
    )

    # ================================================================ #

def test_value_source_paths(path_context):
    (
        trace,
        main_ocean_id,
        a_inspected,
        b_inspected,
        c_inspected,
        a_exec_inspected,
        b_exec_inspected,
        c_exec_inspected,
        a_ref_inspected,
        b_ref_inspected,
        b_ref_grouped,
        b_val_inspected,
    ) = unpack_base(path_context)

    def check_path(*args, **kwargs):
        kwargs.setdefault("source_trace", trace)
        return assert_path(*args, **kwargs)

    # VALUE SOURCE PATH TESTS
    # ================================================================ #

    # --------------------------------------- #
    # VAL-TO-ACTOR PATH TESTS
    # CONSUMED_VAL -> CONSUMER_ACTOR
    # PRODUCED_VAL <- PRODUCER_ACTOR
    # --------------------------------------- #


    value_to_actor_down_path = trace.path(b_val_inspected, c_inspected)

    check_path(
        value_to_actor_down_path,
        [
            ExpectedHop(
                pos=(0, 0, 0),
                expected_type=StackLink,
                assert_attrs=[
                    ("producer_actor", Actor(main_ocean_id, "TIDE: .event()", "B")),
                    ("consumer_actor", Actor(main_ocean_id, "TIDE: .event()", "C")),
                    ("produced_ref", "global:@B"),
                    ("produced_ver", 0),
                ],
            ),
            ExpectedHop(
                pos=(0, 0, 1),
                expected_type=str,
                string_match="read as global:@B[v0] from B",
            )
        ],
        ["EXEC_RECORD[1] B"],
        expected_source_count=1,
        expected_path_counts=[1],
        expected_hop_counts=[[2]],
    )

    value_to_actor_up_path = trace.path(b_val_inspected, b_inspected, direction="upstream")


    check_path(
        value_to_actor_up_path,
        [
            ExpectedHop(
                pos=(0, 0, 0),
                expected_type=str, # TODO: Change str into ReadWriteLink(type='read'|'write', ref, ver , producer) 
                string_match="written as global:@B[v0]",
            )
        ],
        ["EXEC_RECORD[1] B"],
        expected_source_count=1,
        expected_path_counts=[1],
        expected_hop_counts=[[1]],
    )


    # --------------------------------------- #
    # VAL-TO-EXEC PATH TESTS
    # CONSUMED_VAL -> CONSUMER_EXEC
    # PRODUCED_VAL <- PRODUCER_EXEC
    # --------------------------------------- #

    value_to_exec_down_path = trace.path(b_val_inspected, c_exec_inspected)

    check_path(
        value_to_exec_down_path,
        [
            ExpectedHop(
                pos=(0, 0, 0),
                expected_type=StackLink,
                assert_attrs=[
                    ("producer_actor", Actor(main_ocean_id, "TIDE: .event()", "B")),
                    ("consumer_actor", Actor(main_ocean_id, "TIDE: .event()", "C")),
                    ("produced_ref", "global:@B"),
                    ("produced_ver", 0),
                ],
            ),
            ExpectedHop(
                pos=(0, 0, 1),
                expected_type=str,
                string_match="read as global:@B[v0] from B",
            )
        ],
        ["EXEC_RECORD[1] B"],
        expected_source_count=1,
        expected_path_counts=[1],
        expected_hop_counts=[[2]],
    )



    value_to_exec_up_path = trace.path(b_val_inspected, b_exec_inspected, direction="upstream")

    check_path(
        value_to_exec_up_path,
        [
            ExpectedHop(
                pos=(0, 0, 0),
                expected_type=str, # TODO: Change str into ReadWriteLink(type='read'|'write', ref, ver , producer) 
                string_match="written as global:@B[v0]",
            )
        ],
        ["EXEC_RECORD[1] B"],
        expected_source_count=1,
        expected_path_counts=[1],
        expected_hop_counts=[[1]],
    )


    # --------------------------------------- #
    # VAL-TO-ACTOR REF TESTS
    # CONSUMED_VAL -> CONSUMED_REF
    # PRODUCED_VAL <- PRODUCED_REF
    # --------------------------------------- #

    value_to_ref_down_path = trace.path(b_val_inspected, b_ref_inspected)

    check_path(
        value_to_ref_down_path,
        [
            ExpectedHop(
                pos=(0, 0, 0),
                expected_type=StackLink,
                assert_attrs=[
                    ("producer_actor", Actor(main_ocean_id, "TIDE: .event()", "B")),
                    ("consumer_actor", Actor(main_ocean_id, "TIDE: .event()", "C")),
                    ("produced_ref", "global:@B"),
                    ("produced_ver", 0),
                ],
            ),
            ExpectedHop(
                pos=(0, 0, 1),
                expected_type=str,
                string_match="read as global:@B[v0] from B",
            )
        ],
        ["EXEC_RECORD[1] B"],
        expected_source_count=1,
        expected_path_counts=[1],
        expected_hop_counts=[[2]],
    )


    value_to_ref_up_path = trace.path(b_val_inspected, b_ref_inspected, direction="upstream")


    check_path(
        value_to_ref_up_path,
        [
            ExpectedHop(
                pos=(0, 0, 0),
                expected_type=str, # TODO: Change str into ReadWriteLink(type='read'|'write', ref, ver, producer)
                string_match="written as global:@B[v0]",
            ),
            ExpectedHop(
                pos=(1, 0, 0),
                expected_type=str, # TODO: Change str into ReadWriteLink(type='read'|'write', ref, ver, producer)
                string_match="written as global:@B[v1]",
            ),
        ],
        ["EXEC_RECORD[1] B", "EXEC_RECORD[3] X"],
        expected_source_count=2,
        expected_path_counts=[1, 1],
        expected_hop_counts=[[1], [1]],
    )

    # --------------------------------------- #
    # VALUE -> VALUE
    # N/A / DEFINE SELF-PATH SEMANTICS LATER
    # --------------------------------------- #

    # TODO: Decide whether this should be direct, expanded through appearances,
    # or intentionally unsupported.

    # ================================================================ #

def test_repeated_actor_paths():
    def check_path(*args, **kwargs):
        return assert_path(*args, **kwargs)

    # REPEATED ACTOR FIXTURE
    # SAME ACTOR NAME, DIFFERENT EXECUTIONS/VERSIONS
    # --------------------------------------- #

    def repeated_actor():
        pass


    def create_repeated_actor_fixture():
        tide = TIDE()
        tide.event("Seed", writes={"@x": 1})
        tide.event(repeated_actor, reads={"@x": 1}, writes={"@y": 10})
        tide.event("Update", writes={"@x": 2})
        tide.event(repeated_actor, reads={"@x": 2}, writes={"@y": 20})
        return tide.trace()


    repeated_trace = create_repeated_actor_fixture()
    repeated_trace.show()
    repeated_group = repeated_trace.inspect("repeated_actor", "actor")

    assert repeated_group.grouped
    assert len(repeated_group.history) == 2

    repeated_trace.investigate("repeated_actor", "actor").show()

    seed_actor = repeated_trace.inspect("Seed", "actor", grouped=False)
    update_actor = repeated_trace.inspect("Update", "actor", grouped=False)

    seed_to_repeated = repeated_trace.path(seed_actor, repeated_group)
    update_to_repeated = repeated_trace.path(update_actor, repeated_group)

    # 
    check_path(
        seed_to_repeated,
        [
            ExpectedHop(
                pos=(0, 0, 0),
                expected_type=StackLink, # TODO: Change str into ReadWriteLink(type='read'|'write', ref, ver , producer) 
                assert_attrs=[
                    ("producer_actor", Actor(repeated_trace.ocean_id, "TIDE: .event()", "Seed")), # is ocean two as is a new instance of TIDE()
                    ("consumer_actor", Actor(repeated_trace.ocean_id, "TIDE: .event()", "repeated_actor")),
                    ("produced_ref", "global:@x"),
                    ("produced_ver", 0),
                ],
            ),
            ExpectedHop(
                pos=(0, 0, 1),
                expected_type=str, # TODO: Change str into ReadWriteLink(type='read'|'write', ref, ver , producer) 
                string_match="read global:@x[v0] from Seed",
            )
        ],
        ["EXEC_RECORD[0] Seed"],
        source_trace=repeated_trace,
        expected_source_count=1,
        expected_path_counts=[1],
        expected_hop_counts=[[2]],
    )


    check_path(
        update_to_repeated,
        [
            ExpectedHop(
                pos=(0, 0, 0),
                expected_type=StackLink, # TODO: Change str into ReadWriteLink(type='read'|'write', ref, ver , producer) 
                assert_attrs=[
                    ("producer_actor", Actor(repeated_trace.ocean_id, "TIDE: .event()", "Update")), # is ocean two as is a new instance of TIDE()
                    ("consumer_actor", Actor(repeated_trace.ocean_id, "TIDE: .event()", "repeated_actor")),
                    ("produced_ref", "global:@x"),
                    ("produced_ver", 1),
                ],
            ), 
            ExpectedHop(
                pos=(0, 0, 1),
                expected_type=str, # TODO: Change str into ReadWriteLink(type='read'|'write', ref, ver , producer) 
                string_match="read global:@x[v1] from Update",
            ), 
        ],
        ["EXEC_RECORD[2] Update"],
        source_trace=repeated_trace,
        expected_source_count=1,
        expected_path_counts=[1],
        expected_hop_counts=[[2]],
    )

    # # --------------------------------------- #

# REPEATED ACTOR EXACT EXECUTION CHECKS
    # SAME ACTOR IDENTITY MUST NOT COLLAPSE EXECUTIONS
    # --------------------------------------- #

    first_repeated_exec = repeated_trace.inspect(1, "seq_id")
    second_repeated_exec = repeated_trace.inspect(3, "seq_id")

    seed_to_first_repeated = repeated_trace.path(seed_actor, first_repeated_exec)
    seed_to_second_repeated = repeated_trace.path(seed_actor, second_repeated_exec)

    update_to_first_repeated = repeated_trace.path(update_actor, first_repeated_exec)
    update_to_second_repeated = repeated_trace.path(update_actor, second_repeated_exec)

    check_path(
        seed_to_first_repeated,
        [
            ExpectedHop(
                pos=(0, 0, 0),
                expected_type=StackLink,
                assert_attrs=[
                    ("producer_actor", Actor(repeated_trace.ocean_id, "TIDE: .event()", "Seed")),
                    ("consumer_actor", Actor(repeated_trace.ocean_id, "TIDE: .event()", "repeated_actor")),
                    ("produced_ref", "global:@x"),
                    ("produced_ver", 0),
                ],
            ),
            ExpectedHop(
                pos=(0, 0, 1),
                expected_type=str,
                string_match="read global:@x[v0] from Seed",
            ),
        ],
        ["EXEC_RECORD[0] Seed"],
        source_trace=repeated_trace,
        expected_source_count=1,
        expected_path_counts=[1],
        expected_hop_counts=[[2]],
    )
    check_path(
        seed_to_second_repeated,
        [],
        [],
        should_exist=False,
        source_trace=repeated_trace,
    )
    check_path(
        update_to_first_repeated,
        [],
        [],
        should_exist=False,
        source_trace=repeated_trace,
    )
    check_path(
        update_to_second_repeated,
        [
            ExpectedHop(
                pos=(0, 0, 0),
                expected_type=StackLink,
                assert_attrs=[
                    ("producer_actor", Actor(repeated_trace.ocean_id, "TIDE: .event()", "Update")),
                    ("consumer_actor", Actor(repeated_trace.ocean_id, "TIDE: .event()", "repeated_actor")),
                    ("produced_ref", "global:@x"),
                    ("produced_ver", 1),
                ],
            ),
            ExpectedHop(
                pos=(0, 0, 1),
                expected_type=str,
                string_match="read global:@x[v1] from Update",
            ),
        ],
        ["EXEC_RECORD[2] Update"],
        source_trace=repeated_trace,
        expected_source_count=1,
        expected_path_counts=[1],
        expected_hop_counts=[[2]],
    )


    # --------------------------------------- #

def test_multiple_consumers_keep_distinct_paths():
    def check_path(*args, **kwargs):
        return assert_path(*args, **kwargs)

    # # ONE WRITE / MULTIPLE CONSUMERS
    # # SAME REF-VERSION MUST KEEP BOTH READ USAGES
    # # --------------------------------------- #

    def create_multi_consumer_fixture():
        tide = TIDE()
        tide.event("A", writes={"@shared": "value"})
        tide.event("B", reads={"@shared": "value"})
        tide.event("C", reads={"@shared": "value"})
        return tide.trace()


    multi_consumer_trace = create_multi_consumer_fixture()
    multi_consumer_trace.show()
    shared_ref = multi_consumer_trace.inspect("global:@shared", "reference", version=0)
    consumer_b = multi_consumer_trace.inspect("B", "actor", grouped=False)
    consumer_c = multi_consumer_trace.inspect("C", "actor", grouped=False)

    shared_to_b = multi_consumer_trace.path(shared_ref, consumer_b)
    shared_to_c = multi_consumer_trace.path(shared_ref, consumer_c)

    # Both consumers must remain independently visible.
    assert multi_consumer_trace.consumed_by(consumer_b).refs == [("global:@shared", 0)]
    check_path(
        shared_to_b,
        [
            ExpectedHop(
                pos=(0, 0, 0),
                expected_type=StackLink,
                assert_attrs=[
                    ("producer_actor", Actor(multi_consumer_trace.ocean_id, "TIDE: .event()", "A")),
                    ("consumer_actor", Actor(multi_consumer_trace.ocean_id, "TIDE: .event()", "B")),
                    ("produced_ref", "global:@shared"),
                    ("produced_ver", 0),
                ],
            ),
            ExpectedHop(
                pos=(0, 0, 1),
                expected_type=str,
                string_match="read global:@shared[v0] from A",
            ), 
        ],
        ["EXEC_RECORD[0] A"],
        source_trace=multi_consumer_trace,
        expected_source_count=1,
        expected_path_counts=[1],
        expected_hop_counts=[[2]],
    )


    assert multi_consumer_trace.consumed_by(consumer_c).refs == [("global:@shared", 0)]
    check_path(
        shared_to_c,
        [
            ExpectedHop(
                pos=(0, 0, 0),
                expected_type=StackLink,
                assert_attrs=[
                    ("producer_actor", Actor(multi_consumer_trace.ocean_id, "TIDE: .event()", "A")),
                    ("consumer_actor", Actor(multi_consumer_trace.ocean_id, "TIDE: .event()", "C")),
                    ("produced_ref", "global:@shared"),
                    ("produced_ver", 0),
                ],
            ),
            ExpectedHop(
                pos=(0, 0, 1),
                expected_type=str,
                string_match="read global:@shared[v0] from A",
            ), 
        ],
        ["EXEC_RECORD[0] A"],
        source_trace=multi_consumer_trace,
        expected_source_count=1,
        expected_path_counts=[1],
        expected_hop_counts=[[2]],
    )



    # --------------------------------------- #

def test_branching_paths():
    def check_path(*args, **kwargs):
        return assert_path(*args, **kwargs)

    # BRANCHING / MULTI-PATH FIXTURE
    # A -> B -> D
    # A -> C -> D
    # --------------------------------------- #

    def create_branching_path_fixture():
        tide = TIDE()
        tide.event("A", writes={"@A": "!A"})
        tide.event("B", reads={"@A": "!A"}, writes={"@B": "!B"})
        tide.event("C", reads={"@A": "!A"}, writes={"@C": "!C"})
        tide.event("D", reads={"@B": "!B", "@C": "!C"}, writes={"@D": "!D"})
        return tide.trace()


    branch_trace = create_branching_path_fixture()
    branch_trace.show()
    branch_a = branch_trace.inspect("A", "actor", grouped=False)
    branch_c = branch_trace.inspect("C", "actor", grouped=False)
    branch_d = branch_trace.inspect("D", "actor", grouped=False)
    branch_ocean_id = branch_trace.ocean_id


    no_diamond_path = branch_trace.path(branch_a, branch_d, deep_link=False)
    assert not no_diamond_path.exists()


    direct_diamond_path = branch_trace.path(branch_a, branch_c)
    check_path(
        direct_diamond_path,
        [   ExpectedHop(
                pos=(0, 0, 0),
                expected_type=StackLink, # TODO: Change str into ReadWriteLink(type='read'|'write', ref, ver , producer) 
                assert_attrs=[
                    ("producer_actor", Actor(branch_ocean_id, "TIDE: .event()", "A")), # is ocean two as is a new instance of TIDE()
                    ("consumer_actor", Actor(branch_ocean_id, "TIDE: .event()", "C")),
                    ("produced_ref", "global:@A"),
                    ("produced_ver", 0)
                ],
            ), 
            ExpectedHop(
                pos=(0, 0, 1),
                expected_type=str, # TODO: Change str into ReadWriteLink(type='read'|'write', ref, ver , producer) 
                string_match="read global:@A[v0] from A",
            ), 
        ],
        ["EXEC_RECORD[0] A"],
        source_trace=branch_trace,
        expected_source_count=1,
        expected_path_counts=[1],
        expected_hop_counts=[[2]],
    )


    deep_diamond_path = branch_trace.path(branch_a, branch_d, deep_link=True)
    check_path(
        deep_diamond_path,
        [   ExpectedHop(
                pos=(0, 0, 0),
                expected_type=StackLink, # TODO: Change str into ReadWriteLink(type='read'|'write', ref, ver , producer) 
                assert_attrs=[
                    ("producer_actor", Actor(branch_ocean_id, "TIDE: .event()", "A")), # is ocean two as is a new instance of TIDE()
                    ("consumer_actor", Actor(branch_ocean_id, "TIDE: .event()", "B")),
                    ("produced_ref", "global:@A"),
                    ("produced_ver", 0)
                ],
            ), 
            ExpectedHop(
                pos=(0, 0, 1),
                expected_type=str, # TODO: Change str into ReadWriteLink(type='read'|'write', ref, ver , producer) 
                string_match="read global:@A[v0] from A",
            ), 
            ExpectedHop(
                pos=(0, 0, 2),
                expected_type=StackLink,
                assert_attrs=[
                    ("producer_actor", Actor(branch_ocean_id, "TIDE: .event()", "B")),
                    ("consumer_actor", Actor(branch_ocean_id, "TIDE: .event()", "D")),
                    ("produced_ref", "global:@B"),
                    ("produced_ver", 0),
                ],
            ),
            ExpectedHop(
                pos=(0, 0, 3),
                expected_type=str,
                string_match="read global:@B[v0] from B",
            ),

            ExpectedHop(
                pos=(0, 1, 0),
                expected_type=StackLink, # TODO: Change str into ReadWriteLink(type='read'|'write', ref, ver , producer) 
                assert_attrs=[
                    ("producer_actor", Actor(branch_ocean_id, "TIDE: .event()", "A")), # is ocean two as is a new instance of TIDE()
                    ("consumer_actor", Actor(branch_ocean_id, "TIDE: .event()", "C")),
                    ("produced_ref", "global:@A"),
                    ("produced_ver", 0)
                ],
            ), 
            ExpectedHop(
                pos=(0, 1, 1),
                expected_type=str, # TODO: Change str into ReadWriteLink(type='read'|'write', ref, ver , producer) 
                string_match="read global:@A[v0] from A",
            ), 

            ExpectedHop(
                pos=(0, 1, 2),
                expected_type=StackLink,
                assert_attrs=[
                    ("producer_actor", Actor(branch_ocean_id, "TIDE: .event()", "C")),
                    ("consumer_actor", Actor(branch_ocean_id, "TIDE: .event()", "D")),
                    ("produced_ref", "global:@C"),
                    ("produced_ver", 0),
                ],
            ),
            ExpectedHop(
                pos=(0, 1, 3),
                expected_type=str,
                string_match="read global:@C[v0] from C",
        ),
        ],
        ["EXEC_RECORD[0] A"],
        expected_source_count=1,
        expected_path_counts=[2],
        expected_hop_counts=[[4, 4]],
        source_trace=branch_trace,
    )

    # --------------------------------------- #

def test_exact_version_paths():
    def check_path(*args, **kwargs):
        return assert_path(*args, **kwargs)

    # EXACT VERSION CAUSALITY FIXTURE
    # EACH CONSUMER MUST LINK TO THE VERSION IT ACTUALLY READ
    # --------------------------------------- #

    def create_versioned_path_fixture():
        tide = TIDE()
        tide.event("A0", writes={"@A": "A-v0"})
        tide.event("B0", reads={"@A": "A-v0"}, writes={"@B": "B-from-A-v0"})
        tide.event("A1", writes={"@A": "A-v1"})
        tide.event("B1", reads={"@A": "A-v1"}, writes={"@B": "B-from-A-v1"})
        return tide.trace()


    versioned_trace = create_versioned_path_fixture()
    versioned_trace.show()

    a_v0 = versioned_trace.inspect("global:@A", "reference", version=0)
    a_v1 = versioned_trace.inspect("global:@A", "reference", version=1)

    b0_actor = versioned_trace.inspect("B0", "actor", grouped=False)
    b1_actor = versioned_trace.inspect("B1", "actor", grouped=False)

    a_v0_to_b0 = versioned_trace.path(a_v0, b0_actor)
    a_v1_to_b1 = versioned_trace.path(a_v1, b1_actor)

    check_path(
        a_v0_to_b0,
        [
            ExpectedHop(
                pos=(0, 0, 0),
                expected_type=StackLink,
                assert_attrs=[
                    ("producer_actor", Actor(versioned_trace.ocean_id, "TIDE: .event()", "A0")),
                    ("consumer_actor", Actor(versioned_trace.ocean_id, "TIDE: .event()", "B0")),
                    ("produced_ref", "global:@A"),
                    ("produced_ver", 0),
                ],
            ),
            ExpectedHop(
                pos=(0, 0, 1),
                expected_type=str,
                string_match="read global:@A[v0] from A0",
            )
        ],
        ["EXEC_RECORD[0] A0"],
        source_trace=versioned_trace,
        expected_source_count=1,
        expected_path_counts=[1],
        expected_hop_counts=[[2]],
    )

    check_path(
        a_v1_to_b1,
        [
            ExpectedHop(
                pos=(0, 0, 0),
                expected_type=StackLink,
                assert_attrs=[
                    ("producer_actor", Actor(versioned_trace.ocean_id, "TIDE: .event()", "A1")),
                    ("consumer_actor", Actor(versioned_trace.ocean_id, "TIDE: .event()", "B1")),
                    ("produced_ref", "global:@A"),
                    ("produced_ver", 1),
                ],
            ),
            ExpectedHop(
                pos=(0, 0, 1),
                expected_type=str,
                string_match="read global:@A[v1] from A1",
            )
        ],
        ["EXEC_RECORD[2] A1"],
        source_trace=versioned_trace,
        expected_source_count=1,
        expected_path_counts=[1],
        expected_hop_counts=[[2]],
    )

    wrong_a_v0_to_b1 = versioned_trace.path(a_v0, b1_actor)
    wrong_a_v1_to_b0 = versioned_trace.path(a_v1, b0_actor)

    check_path(
        wrong_a_v0_to_b1,
        [],
        [],
        should_exist=False,
        source_trace=versioned_trace,
    )

    check_path(
        wrong_a_v1_to_b0,
        [],
        [],
        should_exist=False,
        source_trace=versioned_trace,
    )


    # --------------------------------------- #

def test_same_value_multiple_reference_paths():
    def check_path(*args, **kwargs):
        return assert_path(*args, **kwargs)

    # SAME VALUE / MULTIPLE REFERENCES
    # VALUE IDENTITY MUST NOT COLLAPSE REF IDENTITY
    # --------------------------------------- #

    def create_same_value_multi_ref_fixture():
        tide = TIDE()
        tide.event("A", writes={"@left": "same", "@right": "same"})
        tide.event("B", reads={"@left": "same"})
        tide.event("C", reads={"@right": "same"})
        return tide.trace()


    same_value_trace = create_same_value_multi_ref_fixture()
    same_value_trace.show()
    same_value = same_value_trace.inspect("same", "value")
    left_ref = same_value_trace.inspect("global:@left", "reference")
    right_ref = same_value_trace.inspect("global:@right", "reference")
    left_consumer = same_value_trace.inspect("B", "actor", grouped=False)
    right_consumer = same_value_trace.inspect("C", "actor", grouped=False)

    same_to_left = same_value_trace.path(same_value, left_ref)
    same_to_right = same_value_trace.path(same_value, right_ref)
    same_to_b = same_value_trace.path(same_value, left_consumer)
    same_to_c = same_value_trace.path(same_value, right_consumer)


    check_path(
        same_to_left,
        [
            ExpectedHop(
                pos=(0, 0, 0),
                expected_type=StackLink,
                assert_attrs=[
                    ("producer_actor", Actor(same_value_trace.ocean_id, "TIDE: .event()", "A")),
                    ("consumer_actor", Actor(same_value_trace.ocean_id, "TIDE: .event()", "B")),
                    ("produced_ref", "global:@left"),
                    ("produced_ver", 0),
                ],
            ),
            ExpectedHop(
                pos=(0, 0, 1),
                expected_type=str,
                string_match="read as global:@left[v0] from A",
            ), 
        ],
        ["EXEC_RECORD[0] A"],
        source_trace=same_value_trace,
        expected_source_count=1,
        expected_path_counts=[1],
        expected_hop_counts=[[2]],
    )


    check_path(
        same_to_right,
        [
            ExpectedHop(
                pos=(0, 0, 0),
                expected_type=StackLink,
                assert_attrs=[
                    ("producer_actor", Actor(same_value_trace.ocean_id, "TIDE: .event()", "A")),
                    ("consumer_actor", Actor(same_value_trace.ocean_id, "TIDE: .event()", "C")),
                    ("produced_ref", "global:@right"),
                    ("produced_ver", 0),
                ],
            ),
            ExpectedHop(
                pos=(0, 0, 1),
                expected_type=str,
                string_match="read as global:@right[v0] from A",
            ), 
        ],
        ["EXEC_RECORD[0] A"],
        source_trace=same_value_trace,
        expected_source_count=1,
        expected_path_counts=[1],
        expected_hop_counts=[[2]],
    )


    check_path(
        same_to_b,
        [
            ExpectedHop(
                pos=(0, 0, 0),
                expected_type=StackLink,
                assert_attrs=[
                    ("producer_actor", Actor(same_value_trace.ocean_id, "TIDE: .event()", "A")),
                    ("consumer_actor", Actor(same_value_trace.ocean_id, "TIDE: .event()", "B")),
                    ("produced_ref", "global:@left"),
                    ("produced_ver", 0),
                ],
            ),
            ExpectedHop(
                pos=(0, 0, 1),
                expected_type=str,
                string_match="read as global:@left[v0] from A",
            ), 
        ],
        ["EXEC_RECORD[0] A"],
        source_trace=same_value_trace,
        expected_source_count=1,
        expected_path_counts=[1],
        expected_hop_counts=[[2]],
    )


    check_path(
        same_to_c,
        [
            ExpectedHop(
                pos=(0, 0, 0),
                expected_type=StackLink,
                assert_attrs=[
                    ("producer_actor", Actor(same_value_trace.ocean_id, "TIDE: .event()", "A")),
                    ("consumer_actor", Actor(same_value_trace.ocean_id, "TIDE: .event()", "C")),
                    ("produced_ref", "global:@right"),
                    ("produced_ver", 0),
                ],
            ),
            ExpectedHop(
                pos=(0, 0, 1),
                expected_type=str,
                string_match="read as global:@right[v0] from A",
            ), 
        ],
        ["EXEC_RECORD[0] A"],
        source_trace=same_value_trace,
        expected_source_count=1,
        expected_path_counts=[1],
        expected_hop_counts=[[2]],
    )

from tide_tooling.core import StackLink


def duplicate_B(a, b, separate_b):
    return "!B"


duplicate_B.__name__ = "B"


def X(a, ab, abc):
    return None


@pytest.fixture(scope="module")
def duplicate_input_trace():
    tide = TIDE()
    tide.ocean.register_store("testing")

    tide.event("A", writes={"@A": "!A"})
    tide.event("B", reads=["@A"], writes={"@B": "!B", "testing:@B": "!B"})
    tide.exec(duplicate_B, args=["@@A", "@@B", "@testing:@B"], writes=["@B"])
    tide.event(duplicate_B, reads={"@A": "!A", "@B": "!B"}, writes={"@B": "!B"})
    tide.event("C", reads={"@B": "!B"}, writes={"@C": "!C"})
    tide.event("X", writes={"@C": "!C"})
    tide.exec(X, args=["@@C", "@testing:@B", "@testing:@B"], writes=["@B"])

    return tide.trace()


def path_edges(path):
    return [
        edge
        for paths in path.paths.values()
        for one_path in paths
        for edge in one_path
        if isinstance(edge, StackLink)
    ]


def path_edge_dicts(path):
    data = path.to_dict()

    return [
        edge
        for paths in data["path_by_root"].values()
        for one_path in paths
        for edge in one_path
        if isinstance(edge, dict) and "type" not in edge
    ]


# Path rendering contract:
# StackLink is traversal structure + input slot.
# terminal_link() strings are the explanation authority.
# .show() should combine them, not reinterpret StackLink refs as causal wording.

def test_path_edges_preserve_duplicate_input_positions(duplicate_input_trace):
    trace = duplicate_input_trace
    source = trace.inspect("testing:@B", "reference", version=0)
    target = trace.inspect(6, "seq_id")
    pathed = trace.path(source, target)

    matching_edges = [
        edge
        for edge in path_edges(pathed)
        if edge.produced_ref == "testing:@B" and edge.produced_ver == 0
    ]

    assert [edge.input_use()["display"] for edge in matching_edges] == ["args[1]", "args[2]"]
    assert [edge.consumer_input_index for edge in matching_edges] == [1, 2]


def test_path_to_dict_includes_input_use_and_input_uses(duplicate_input_trace):
    trace = duplicate_input_trace
    source = trace.inspect("testing:@B", "reference", version=0)
    target = trace.inspect(6, "seq_id")
    pathed = trace.path(source, target)

    matching_edges = [
        edge
        for edge in path_edge_dicts(pathed)
        if edge["ref"] == "testing:@B" and edge["version"] == 0
    ]

    assert [edge["input_use"]["display"] for edge in matching_edges] == ["args[1]", "args[2]"]
    assert [edge["input_uses"] for edge in matching_edges] == [
        [{"name": "args", "index": 1, "display": "args[1]"}],
        [{"name": "args", "index": 2, "display": "args[2]"}],
    ]


def test_path_show_input_positions_toggle(duplicate_input_trace):
    trace = duplicate_input_trace
    source = trace.inspect("testing:@B", "reference", version=0)
    target = trace.inspect(6, "seq_id")
    pathed = trace.path(source, target)

    plain = pathed.show(show_input_positions=False)
    positioned = strip_ansi(pathed.show(show_input_positions=True))

    assert "via args" not in plain
    assert "testing:@B[v0] via args[1] from B" in positioned
    assert "testing:@B[v0] via args[2] from B" in positioned




def test_path_show_uses_investigate_style_header(duplicate_input_trace):
    trace = duplicate_input_trace
    source = trace.inspect("testing:@B", "reference", version=0)
    target = trace.inspect(6, "seq_id")
    pathed = trace.path(source, target)

    output = strip_ansi(pathed.show(show_input_positions=False, explore_tool_tips=False))

    assert "======================================================================" in output
    assert "PATH REFERENCE[testing:@B[v0]] -> EXEC_RECORD[6] X" in output
    assert "| 2 paths found" in output
    assert "PATHS:" in output
    assert "EXEC_RECORD[1] B | 2 paths" in output
    assert "├── path 0" in output
    assert "└── path 1" in output


def test_pathed_repr_reports_paths_without_missing_depth(path_context):
    trace = path_context["trace"]
    pathed = trace.path(
        path_context["a_inspected"],
        path_context["b_inspected"],
    )

    output = repr(pathed)

    assert output.startswith("Pathed[")
    assert "1 path" in output


def test_path_show_hides_input_positions_by_default(duplicate_input_trace):
    trace = duplicate_input_trace
    source = trace.inspect("testing:@B", "reference", version=0)
    target = trace.inspect(6, "seq_id")
    pathed = trace.path(source, target)

    output = strip_ansi(pathed.show())

    assert "via args" not in output
    assert "EXEC_RECORD[6] X | read testing:@B[v0] from B" in output
    assert "because" not in output


def test_path_show_can_display_input_positions(duplicate_input_trace):
    trace = duplicate_input_trace
    source = trace.inspect("testing:@B", "reference", version=0)
    target = trace.inspect(6, "seq_id")
    pathed = trace.path(source, target)

    output = strip_ansi(pathed.show(show_input_positions=True, explore_tool_tips=False))

    assert "EXEC_RECORD[6] X | read testing:@B[v0] via args[1] from B" in output
    assert "EXEC_RECORD[6] X | read testing:@B[v0] via args[2] from B" in output
    assert "because" not in output


def test_path_show_does_not_duplicate_terminal_read_lines(duplicate_input_trace):
    trace = duplicate_input_trace
    source = trace.inspect("testing:@B", "reference", version=0)
    target = trace.inspect(6, "seq_id")
    pathed = trace.path(source, target)

    output = strip_ansi(pathed.show(show_input_positions=True))

    assert output.count("testing:@B[v0] via args[1] from B") == 1
    assert output.count("testing:@B[v0] via args[2] from B") == 1


def test_path_show_no_path_uses_investigate_style_empty_state(path_context):
    (
        trace,
        main_ocean_id,
        a_inspected,
        b_inspected,
        c_inspected,
        a_exec_inspected,
        b_exec_inspected,
        c_exec_inspected,
        a_ref_inspected,
        b_ref_inspected,
        b_ref_grouped,
        b_val_inspected,
    ) = unpack_base(path_context)

    x_inspected = trace.find("X").first("actor")
    pathed = trace.path(c_inspected, x_inspected)

    output = strip_ansi(pathed.show())

    assert "PATH ACTOR[C] -> ACTOR[X]" in output
    assert "| no paths found" in output
    assert "PATHS:" in output
    assert "└── No paths!" in output


def test_path_show_downstream_reads_not_because(duplicate_input_trace):
    trace = duplicate_input_trace
    source = trace.inspect("testing:@B", "reference", version=0)
    target = trace.inspect(6, "seq_id")
    pathed = trace.path(source, target)

    output = strip_ansi(pathed.show(show_input_positions=True))

    assert "because" not in output
    assert "EXEC_RECORD[6] X | read testing:@B[v0] via args[1] from B" in output
    assert "EXEC_RECORD[6] X | read testing:@B[v0] via args[2] from B" in output


def test_path_show_upstream_writes_not_reads(path_context):
    (
        trace,
        main_ocean_id,
        a_inspected,
        b_inspected,
        c_inspected,
        a_exec_inspected,
        b_exec_inspected,
        c_exec_inspected,
        a_ref_inspected,
        b_ref_inspected,
        b_ref_grouped,
        b_val_inspected,
    ) = unpack_base(path_context)

    pathed = trace.path(b_exec_inspected, a_ref_inspected, direction="upstream")
    output = strip_ansi(pathed.show(show_input_positions=False))

    assert "PATH EXEC_RECORD[1] B <- REFERENCE[global:@A]" in output
    assert "EXEC_RECORD[0] A | wrote global:@A[v0] used by B" in output
    assert "EXEC_RECORD[0] A | read global:@A[v0] used by B" not in output
    assert "because" not in output


def test_path_show_upstream_can_display_consumer_input_position(path_context):
    (
        trace,
        main_ocean_id,
        a_inspected,
        b_inspected,
        c_inspected,
        a_exec_inspected,
        b_exec_inspected,
        c_exec_inspected,
        a_ref_inspected,
        b_ref_inspected,
        b_ref_grouped,
        b_val_inspected,
    ) = unpack_base(path_context)

    pathed = trace.path(b_exec_inspected, a_ref_inspected, direction="upstream")
    output = strip_ansi(pathed.show(show_input_positions=True))

    assert "EXEC_RECORD[0] A | wrote global:@A[v0] used by B via args[0]" in output
    assert "read global:@A[v0] used by B" not in output


def test_path_show_upstream_multi_hop_uses_wrote_for_each_producer(path_context):
    (
        trace,
        main_ocean_id,
        a_inspected,
        b_inspected,
        c_inspected,
        a_exec_inspected,
        b_exec_inspected,
        c_exec_inspected,
        a_ref_inspected,
        b_ref_inspected,
        b_ref_grouped,
        b_val_inspected,
    ) = unpack_base(path_context)

    pathed = trace.path(c_exec_inspected, a_exec_inspected, direction="upstream", deep_link=True)
    output = strip_ansi(pathed.show(show_input_positions=True))

    assert "EXEC_RECORD[1] B | wrote global:@B[v0] used by C via args[0]" in output
    assert "EXEC_RECORD[0] A | wrote global:@A[v0] used by B via args[0]" in output
    assert "read global:@B[v0] used by C" not in output
    assert "read global:@A[v0] used by B" not in output


def test_path_show_upstream_duplicate_input_positions_are_consumer_uses(duplicate_input_trace):
    trace = duplicate_input_trace
    source = trace.inspect(6, "seq_id")
    target = trace.inspect("testing:@B", "reference", version=0)
    pathed = trace.path(source, target, direction="upstream")

    output = strip_ansi(pathed.show(show_input_positions=True))

    assert "PATH EXEC_RECORD[6] X <- REFERENCE[testing:@B[v0]]" in output
    assert "EXEC_RECORD[1] B | wrote testing:@B[v0] used by X via args[1]" in output
    assert "EXEC_RECORD[1] B | wrote testing:@B[v0] used by X via args[2]" in output
    assert "EXEC_RECORD[1] B | read testing:@B[v0] used by X" not in output


def test_path_show_upstream_hides_input_positions_by_default(duplicate_input_trace):
    trace = duplicate_input_trace
    source = trace.inspect(6, "seq_id")
    target = trace.inspect("testing:@B", "reference", version=0)
    pathed = trace.path(source, target, direction="upstream")

    output = strip_ansi(pathed.show(show_input_positions=False))

    assert "via args" not in output
    assert "EXEC_RECORD[1] B | wrote testing:@B[v0] used by X" in output
    assert "read testing:@B[v0] used by X" not in output


def test_path_show_does_not_duplicate_terminal_read_or_write_lines(duplicate_input_trace):
    trace = duplicate_input_trace
    source = trace.inspect("testing:@B", "reference", version=0)
    target = trace.inspect(6, "seq_id")
    downstream = trace.path(source, target)

    source_up = trace.inspect(6, "seq_id")
    target_up = trace.inspect("testing:@B", "reference", version=0)
    upstream = trace.path(source_up, target_up, direction="upstream")

    downstream_output = strip_ansi(downstream.show(show_input_positions=True))
    upstream_output = strip_ansi(upstream.show(show_input_positions=True))

    assert downstream_output.count("testing:@B[v0] via args[1] from B") == 1
    assert downstream_output.count("testing:@B[v0] via args[2] from B") == 1
    assert upstream_output.count("wrote testing:@B[v0] used by X via args[1]") == 1
    assert upstream_output.count("wrote testing:@B[v0] used by X via args[2]") == 1


def test_path_show_multiple_writes_keep_refs_distinct():
    def UseBoth(left, right):
        return "done"

    tide = TIDE()
    tide.event("Seed", writes={"@left": "L", "@right": "R"})
    tide.exec(UseBoth, args=["@@left", "@@right"], writes=["@done"])
    trace = tide.trace()

    left = trace.inspect("global:@left", "reference", version=0)
    right = trace.inspect("global:@right", "reference", version=0)
    consumer = trace.find("UseBoth").first("actor")

    left_path = trace.path(left, consumer)
    right_path = trace.path(right, consumer)

    left_output = strip_ansi(left_path.show(show_input_positions=True))
    right_output = strip_ansi(right_path.show(show_input_positions=True))

    assert "read global:@left[v0] via args[0] from Seed" in left_output
    assert "read global:@right[v0] via args[1] from Seed" in right_output
    assert "global:@right[v0] via args[1]" not in left_output
    assert "global:@left[v0] via args[0]" not in right_output



def test_path_show_renders_terminal_strings_not_raw_stacklink_refs():
    def UseBoth(left, right):
        return "done"

    tide = TIDE()
    tide.event("Seed", writes={"@left": "L", "@right": "R"})
    tide.exec(UseBoth, args=["@@left", "@@right"], writes=["@done"])
    trace = tide.trace()

    left = trace.inspect("global:@left", "reference", version=0)
    consumer = trace.find("UseBoth").first("actor")
    pathed = trace.path(left, consumer)

    output = strip_ansi(pathed.show(show_input_positions=True))

    assert "read global:@left[v0] via args[0] from Seed" in output
    assert "read global:@right[v0]" not in output

def test_path_edges_multiple_writes_keep_refs_and_inputs_distinct():
    def UseBoth(left, right):
        return "done"

    tide = TIDE()
    tide.event("Seed", writes={"@left": "L", "@right": "R"})
    tide.exec(UseBoth, args=["@@left", "@@right"], writes=["@done"])
    trace = tide.trace()

    left = trace.inspect("global:@left", "reference", version=0)
    right = trace.inspect("global:@right", "reference", version=0)
    consumer = trace.find("UseBoth").first("actor")

    left_edges = path_edges(trace.path(left, consumer))
    right_edges = path_edges(trace.path(right, consumer))

    assert [(edge.produced_ref, edge.produced_ver, edge.input_use()["display"]) for edge in left_edges] == [
        ("global:@left", 0, "args[0]")
    ]
    assert [(edge.produced_ref, edge.produced_ver, edge.input_use()["display"]) for edge in right_edges] == [
        ("global:@right", 0, "args[1]")
    ]


def test_path_multiple_writes_downstream_filters_to_source_ref():
    def UseBoth(left, right):
        return "done"

    tide = TIDE()
    tide.event("Seed", writes={"@left": "L", "@right": "R"})
    tide.exec(UseBoth, args=["@@left", "@@right"], writes=["@done"])
    trace = tide.trace()

    left = trace.inspect("global:@left", "reference", version=0)
    right = trace.inspect("global:@right", "reference", version=0)
    consumer = trace.find("UseBoth").first("actor")

    left_path = trace.path(left, consumer)
    right_path = trace.path(right, consumer)

    left_output = strip_ansi(left_path.show(show_input_positions=True))
    right_output = strip_ansi(right_path.show(show_input_positions=True))

    assert "read global:@left[v0] via args[0] from Seed" in left_output
    assert "global:@right[v0]" not in left_output

    assert "read global:@right[v0] via args[1] from Seed" in right_output
    assert "global:@left[v0]" not in right_output


def test_path_multiple_writes_upstream_filters_to_target_ref():
    def UseBoth(left, right):
        return "done"

    tide = TIDE()
    tide.event("Seed", writes={"@left": "L", "@right": "R"})
    tide.exec(UseBoth, args=["@@left", "@@right"], writes=["@done"])
    trace = tide.trace()

    consumer_exec = trace.inspect(1, "seq_id")
    left = trace.inspect("global:@left", "reference", version=0)
    right = trace.inspect("global:@right", "reference", version=0)

    left_path = trace.path(
        consumer_exec,
        left,
        direction="upstream",
        deep_link=False,
    )
    right_path = trace.path(
        consumer_exec,
        right,
        direction="upstream",
        deep_link=False,
    )

    left_output = strip_ansi(left_path.show(show_input_positions=True))
    right_output = strip_ansi(right_path.show(show_input_positions=True))

    assert "wrote global:@left[v0] used by UseBoth via args[0]" in left_output
    assert "global:@right[v0]" not in left_output

    assert "wrote global:@right[v0] used by UseBoth via args[1]" in right_output
    assert "global:@left[v0]" not in right_output



def test_path_to_dict_multiple_writes_filters_to_requested_ref():
    def UseBoth(left, right):
        return "done"

    tide = TIDE()
    tide.event("Seed", writes={"@left": "L", "@right": "R"})
    tide.exec(UseBoth, args=["@@left", "@@right"], writes=["@done"])
    trace = tide.trace()

    left = trace.inspect("global:@left", "reference", version=0)
    right = trace.inspect("global:@right", "reference", version=0)
    consumer = trace.find("UseBoth").first("actor")

    left_edges = path_edge_dicts(trace.path(left, consumer))
    right_edges = path_edge_dicts(trace.path(right, consumer))

    assert [(edge["ref"], edge["version"], edge["input_use"]["display"]) for edge in left_edges] == [
        ("global:@left", 0, "args[0]")
    ]

    assert [(edge["ref"], edge["version"], edge["input_use"]["display"]) for edge in right_edges] == [
        ("global:@right", 0, "args[1]")
    ]

def test_path_multiple_writes_edge_data_filters_to_requested_ref():
    def UseBoth(left, right):
        return "done"

    tide = TIDE()
    tide.event("Seed", writes={"@left": "L", "@right": "R"})
    tide.exec(UseBoth, args=["@@left", "@@right"], writes=["@done"])
    trace = tide.trace()

    left = trace.inspect("global:@left", "reference", version=0)
    right = trace.inspect("global:@right", "reference", version=0)
    consumer = trace.find("UseBoth").first("actor")

    left_edges = path_edges(trace.path(left, consumer))
    right_edges = path_edges(trace.path(right, consumer))

    assert [(edge.produced_ref, edge.produced_ver, edge.input_use()["display"]) for edge in left_edges] == [
        ("global:@left", 0, "args[0]")
    ]

    assert [(edge.produced_ref, edge.produced_ver, edge.input_use()["display"]) for edge in right_edges] == [
        ("global:@right", 0, "args[1]")
    ]


PATH_LABEL = re.compile(r"[├└]── path \d+$")


def rendered_path_blocks(output):
    lines = strip_ansi(output).splitlines()
    blocks = []
    current = None

    for line in lines:
        stripped = line.strip()

        if PATH_LABEL.search(stripped):
            if current is not None:
                blocks.append(current)

            current = [line]
            continue

        if current is not None:
            if stripped.startswith("EXPLORE:") or stripped.startswith("======================================================================"):
                blocks.append(current)
                current = None
                continue

            current.append(line)

    if current is not None:
        blocks.append(current)

    return blocks


def assert_no_empty_rendered_path_blocks(output):
    blocks = rendered_path_blocks(output)

    assert blocks

    for block in blocks:
        content_lines = [
            line
            for line in block[1:]
            if "|" in strip_ansi(line)
            or "wrote " in strip_ansi(line)
            or "read " in strip_ansi(line)
            or "written as " in strip_ansi(line)
            or "read as " in strip_ansi(line)
        ]

        assert content_lines, "\n".join(block)


def test_path_show_direct_write_path_is_not_empty(path_context):
    (
        trace,
        main_ocean_id,
        a_inspected,
        b_inspected,
        c_inspected,
        a_exec_inspected,
        b_exec_inspected,
        c_exec_inspected,
        a_ref_inspected,
        b_ref_inspected,
        b_ref_grouped,
        b_val_inspected,
    ) = unpack_base(path_context)

    pathed = trace.path(a_inspected, a_ref_inspected)
    output = pathed.show(show_input_positions=True)

    assert "wrote global:@A[v0]" in strip_ansi(output)
    assert_no_empty_rendered_path_blocks(output)


def test_path_show_direct_value_write_path_is_not_empty(path_context):
    (
        trace,
        main_ocean_id,
        a_inspected,
        b_inspected,
        c_inspected,
        a_exec_inspected,
        b_exec_inspected,
        c_exec_inspected,
        a_ref_inspected,
        b_ref_inspected,
        b_ref_grouped,
        b_val_inspected,
    ) = unpack_base(path_context)

    pathed = trace.path(b_exec_inspected, b_val_inspected)
    output = pathed.show(show_input_positions=True)

    assert "written as global:@B[v0]" in strip_ansi(output)
    assert_no_empty_rendered_path_blocks(output)


def test_path_show_branching_paths_each_have_rendered_content():
    tide = TIDE()
    tide.event("A", writes={"@A": "!A"})
    tide.event("B", reads={"@A": "!A"}, writes={"@B": "!B"})
    tide.event("C", reads={"@A": "!A"}, writes={"@C": "!C"})
    tide.event("D", reads={"@B": "!B", "@C": "!C"}, writes={"@D": "!D"})
    trace = tide.trace()

    branch_a = trace.inspect("A", "actor", grouped=False)
    branch_d = trace.inspect("D", "actor", grouped=False)

    pathed = trace.path(branch_a, branch_d, deep_link=True)
    output = pathed.show(show_input_positions=True)

    assert "path 0" in strip_ansi(output)
    assert "path 1" in strip_ansi(output)
    assert_no_empty_rendered_path_blocks(output)


def test_nested_node_path_uses_producer_name_instead_of_internal_node_id():
    @node
    def create_data():
        return {number: None for number in range(3)}

    @node
    def process_data(raw_data):
        return dict(raw_data)

    @node(targets=["trust_flag"])
    def trust_check_data(processed_data):
        return all(isinstance(value, int) for value in processed_data.values())

    @pipeline
    def nested_pipeline():
        return trust_check_data(process_data(create_data()))

    trace = TIDE().ocean.run_pipeline(nested_pipeline)
    pathed = trace.path(
        "create_data",
        "process_data",
        "reference",
        "reference",
    )
    output = strip_ansi(pathed.show(color=False))
    stack_links = [edge for edge in pathed.edges() if isinstance(edge, StackLink)]

    assert stack_links
    assert stack_links[0].producer_actor.source_actor == "create_data"
    assert "read __tide___0:@tmp_node_link.create_data[v0] from create_data" in output
    assert re.search(r" from \d+(?:\n|$)", output) is None
