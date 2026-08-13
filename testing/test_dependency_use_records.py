import pytest

from types import SimpleNamespace

from tide_tooling import TIDE
from tide_tooling.tooling_core.engine.tide_logging import DependencyUseRecord, ExpandedDepRecord
from tide_tooling.tooling_core.engine.classes import DataRef
# logging_module = pytest.importorskip("tide_tooling.tooling_core.engine.tide_logging")
# references_module = pytest.importorskip("tide_tooling.tooling_core.engine.classes")



# ================================================================
# DEPENDENCY USE RECORD TESTS
# ================================================================
#
# Core invariant:
#   dependency requirement != input-use occurrence
#
# Runtime/scheduling may dedupe the requirement:
#   "global:@A must be available"
#
# Provenance must preserve each actual use:
#   "global:@A[v0] via args[0]"
#   "global:@A[v0] via args[1]"
#
# This file intentionally starts with static runtime behavior plus direct
# record-equality tests for DependencyUseRecord / ExpandedDepRecord.
# Dynamic runtime fixture tests can be added after pulling in the existing
# dynamic-ref fixture shape.
# ================================================================


def refs_from_reads(exec_record):
    return [
        (read.unscoped_reference, read.resolved_version)
        for read in exec_record.reads
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
            read.type,
        )
        for read in exec_record.reads
    ]


def exec_for_actor(trace, actor_name, grouped=False):
    inspected_actor = trace.inspect(actor_name, "actor", grouped=grouped)
    exec_id = inspected_actor.history[0]

    return trace.inspect(exec_id, "exec_record").object # NOTE: returns OBJECT(exec) not exec; for actual exec, use object.object (ik, confusion) 


def args_input_record(exec_record):
    return exec_record.object.inputs.args


def associated_input(name="args", index_count=2):
    values = tuple(f"raw-{index}" for index in range(index_count))

    return (
        name,
        SimpleNamespace(
            raw=values,
            evaluated=values,
            normalised=values,
            resolved=values,
        ),
    )


def data_ref(name="@A", store="global"):
    return DataRef(name, store)


def expanded_record(input_index=0, ref_name="@A", store="global", value="A-v0", version=0):
    return ExpandedDepRecord(
        associated_input=associated_input(index_count=2),
        dep=data_ref(ref_name, store),
        type="static",
        unscoped_ref=f"{store}:{ref_name}",
        resolved_val=value,
        resolved_ver=version,
        producer_source="source",
        producer_actor="actor",
        producer_exec_id="producer-exec",
        expanded_from=[],
        input_index=input_index,
    )


@pytest.fixture
def duplicate_static_input_trace():
    def UseTwice(first, second):
        return f"{first}-{second}"

    tide = TIDE()

    tide.event("ProducerA", writes={"@A": "A-v0"})
    tide.exec(UseTwice, args=["@@A", "@@A"], writes=["@out"])

    return tide.trace()


def test_duplicate_static_args_preserve_two_exec_read_records(duplicate_static_input_trace):
    use_exec = exec_for_actor(duplicate_static_input_trace, "UseTwice", grouped=False)

    assert refs_from_reads(use_exec) == [
        ("global:@A", 0),
        ("global:@A", 0),
    ]


def test_duplicate_static_args_preserve_distinct_input_indexes(duplicate_static_input_trace):
    use_exec = exec_for_actor(duplicate_static_input_trace, "UseTwice", grouped=False)

    assert input_uses_from_reads(use_exec) == [
        ("global:@A", 0, "args", 0, "static"),
        ("global:@A", 0, "args", 1, "static"),
    ]


def test_duplicate_static_args_input_record_dependencies_preserve_occurrences(duplicate_static_input_trace):
    use_exec = exec_for_actor(duplicate_static_input_trace, "UseTwice", grouped=False)
    input_record = args_input_record(use_exec)

    assert [
        (
            dep.unscoped_reference,
            dep.resolved_version,
            dep.input_index,
            dep.type,
        )
        for dep in input_record.dependencies
    ] == [
        ("global:@A", 0, 0, "static"),
        ("global:@A", 0, 1, "static"),
    ]


def test_duplicate_static_args_dependency_records_are_not_equal_by_occurrence(duplicate_static_input_trace):
    use_exec = exec_for_actor(duplicate_static_input_trace, "UseTwice", grouped=False)

    first, second = use_exec.reads

    assert first.input_index == 0
    assert second.input_index == 1
    assert first != second


def test_duplicate_static_args_share_same_ref_version_and_producer_but_not_occurrence(duplicate_static_input_trace):
    use_exec = exec_for_actor(duplicate_static_input_trace, "UseTwice", grouped=False)

    first, second = use_exec.reads

    assert first.unscoped_reference == second.unscoped_reference == "global:@A"
    assert first.resolved_version == second.resolved_version == 0
    assert first.producer_exec_id == second.producer_exec_id
    assert first.input_index != second.input_index


def test_consumed_by_actor_may_dedupe_refs_but_exec_reads_preserve_occurrences(duplicate_static_input_trace):
    use_actor = duplicate_static_input_trace.inspect("UseTwice", "actor", grouped=False)
    consumed = duplicate_static_input_trace.consumed_by(use_actor)
    use_exec = exec_for_actor(duplicate_static_input_trace, "UseTwice", grouped=False)

    assert consumed.refs == [
        ("global:@A", 0)
    ]
    assert refs_from_reads(use_exec) == [
        ("global:@A", 0),
        ("global:@A", 0),
    ]


def test_static_dependency_use_record_equality_includes_input_index():
    left = DependencyUseRecord(
        associated_input=associated_input(index_count=2),
        dep=data_ref("@A", "global"),
        type="static",
        unscoped_ref="global:@A",
        resolved_val="A-v0",
        resolved_ver=0,
        producer_source="source",
        producer_actor="actor",
        producer_exec_id="producer-exec",
        input_index=0,
    )

    right = DependencyUseRecord(
        associated_input=associated_input(index_count=2),
        dep=data_ref("@A", "global"),
        type="static",
        unscoped_ref="global:@A",
        resolved_val="A-v0",
        resolved_ver=0,
        producer_source="source",
        producer_actor="actor",
        producer_exec_id="producer-exec",
        input_index=1,
    )

    assert left != right


def test_expanded_dep_record_equality_includes_input_index():
    left = expanded_record(input_index=0)
    right = expanded_record(input_index=1)

    assert left != right


def test_dynamic_dependency_use_record_equality_should_include_input_index():
    left = DependencyUseRecord(
        associated_input=associated_input(index_count=2),
        dep=data_ref("@A*", "global"),
        type="dynamic",
        unscoped_ref="global:@A*",
        expanded_to=[
            expanded_record(input_index=0),
        ],
        input_index=0,
    )

    right = DependencyUseRecord(
        associated_input=associated_input(index_count=2),
        dep=data_ref("@A*", "global"),
        type="dynamic",
        unscoped_ref="global:@A*",
        expanded_to=[
            expanded_record(input_index=0),
        ],
        input_index=1,
    )

    # NOTE:
    # Desired semantic contract:
    # two dynamic reads from args[0] and args[1] are distinct input-use
    # occurrences even if they expand to the same concrete ref.
    assert left != right



def actual_exec_record(exec_like):
    # trace.inspect(exec_id, "exec_record").object may be a display/object wrapper.
    # In current Tide, the actual ExecRecord is often one level deeper.
    if hasattr(exec_like, "inputs"):
        return exec_like

    if hasattr(exec_like, "object") and hasattr(exec_like.object, "inputs"):
        return exec_like.object

    raise TypeError(f"Could not find ExecRecord inputs on {exec_like!r}")


def input_record_for(exec_record, name):
    inputs = exec_record.inputs

    if isinstance(inputs, dict):
        return inputs[name]

    return getattr(inputs, name)


def dep_summary(input_record):
    return [
        (
            dep.unscoped_reference,
            dep.resolved_version,
            dep.associated_input[0] if isinstance(dep.associated_input, tuple) else dep.associated_input,
            dep.input_index,
            dep.type,
        )
        for dep in input_record.dependencies
    ]


def test_all_exec_input_buckets_preserve_dependency_use_records():
    def UseAllInputs(*args, **kwargs):
        return "done"

    tide = TIDE()

    tide.event("SeedA", writes={"@A": "A"})
    tide.event("SeedB", writes={"@B": "B"})
    tide.event("SeedC", writes={"@C": "C"})
    tide.event("SeedD", writes={"@D": "D"})
    tide.event("SeedE", writes={"@E": "E"})
    tide.event("SeedF", writes={"@F": "F"})

    tide.exec(
        UseAllInputs,
        args=["@@A", "@@A"],
        kwargs={
            "kw_a": "@@B",
            "kw_b": "@@B",
        },
        p_args=["@@C", "@@C"],
        p_kwargs={
            "p_kw_a": "@@D",
            "p_kw_b": "@@D",
        },
        k_args=["@@E", "@@E"],
        k_kwargs={
            "k_kw_a": "@@F",
            "k_kw_b": "@@F",
        },
        writes=["@out"],
    )

    trace = tide.trace()
    use_exec = actual_exec_record(exec_for_actor(trace, "UseAllInputs", grouped=False))

    expected = {
        "args": [
            ("global:@A", 0, "args", 0, "static"),
            ("global:@A", 0, "args", 1, "static"),
        ],
        "kwargs": [
            ("global:@B", 0, "kwargs", 0, "static"),
            ("global:@B", 0, "kwargs", 1, "static"),
        ],
        "p_args": [
            ("global:@C", 0, "p_args", 0, "static"),
            ("global:@C", 0, "p_args", 1, "static"),
        ],
        "p_kwargs": [
            ("global:@D", 0, "p_kwargs", 0, "static"),
            ("global:@D", 0, "p_kwargs", 1, "static"),
        ],
        "k_args": [
            ("global:@E", 0, "k_args", 0, "static"),
            ("global:@E", 0, "k_args", 1, "static"),
        ],
        "k_kwargs": [
            ("global:@F", 0, "k_kwargs", 0, "static"),
            ("global:@F", 0, "k_kwargs", 1, "static"),
        ],
    }

    for input_name, expected_deps in expected.items():
        input_record = input_record_for(use_exec, input_name)

        assert input_record.name == input_name
        assert dep_summary(input_record) == expected_deps


# ================================================================
# DYNAMIC EXPANDED_TO / EXPANDED_FROM SEMANTICS
# ================================================================
#
# Contract:
#   Dynamic DependencyUseRecord keeps the declared dynamic dep.
#   Each ExpandedDepRecord keeps the concrete static dep it resolved to.
#   Each ExpandedDepRecord also links back to the dynamic dep it concretised from.
#
# This protects:
#   dynamic pattern evidence != concrete static read evidence
#
# These tests are deliberately record-level tests, not runtime dynamic-ref tests.
# Runtime dynamic expansion belongs in test_dynamic_refs.py.
# ================================================================


def expanded_from_of(expanded_record):
    if hasattr(expanded_record, "expanded_from"):
        return expanded_record.expanded_from

    raise AssertionError("ExpandedDepRecord has no expanded_from back-link")


def dynamic_dep():
    return data_ref("@alpha_*", "global")


def concrete_expanded_record(dynamic_dep, input_index=0, ref_name="@alpha_one", value="one", version=0):
    return ExpandedDepRecord(
        associated_input=associated_input(index_count=2),
        dep=data_ref(ref_name, "global"),
        type="static",
        unscoped_ref=f"global:{ref_name}",
        resolved_val=value,
        resolved_ver=version,
        producer_source="producer-source",
        producer_actor="producer-actor",
        producer_exec_id=f"producer-exec-{input_index}",
        expanded_from=dynamic_dep,
        input_index=input_index,
    )


@pytest.fixture
def dynamic_use_record_with_expansions():
    dep = dynamic_dep()

    expanded_one = concrete_expanded_record(
        dep,
        input_index=0,
        ref_name="@alpha_one",
        value="one",
        version=0,
    )
    expanded_two = concrete_expanded_record(
        dep,
        input_index=1,
        ref_name="@alpha_two",
        value="two",
        version=0,
    )

    dynamic_read = DependencyUseRecord(
        associated_input=associated_input(index_count=2),
        dep=dep,
        type="dynamic",
        unscoped_ref="global:@alpha_*",
        expanded_to=[
            expanded_one,
            expanded_two,
        ],
        input_index=0,
    )

    return dynamic_read


def test_dynamic_expanded_records_link_back_to_declared_dynamic_dep(dynamic_use_record_with_expansions):
    dynamic_read = dynamic_use_record_with_expansions

    assert dynamic_read.type == "dynamic"
    assert dynamic_read.expanded_to

    for expanded in dynamic_read.expanded_to:
        assert expanded_from_of(expanded) == dynamic_read.dep


def test_dynamic_expanded_records_keep_concrete_static_dep_separate_from_dynamic_dep(dynamic_use_record_with_expansions):
    dynamic_read = dynamic_use_record_with_expansions

    for expanded in dynamic_read.expanded_to:
        assert expanded.type == "static"
        assert expanded.dep != dynamic_read.dep
        assert expanded_from_of(expanded) == dynamic_read.dep


def test_dynamic_expanded_records_preserve_concrete_ref_version_and_input_index(dynamic_use_record_with_expansions):
    expanded_records = dynamic_use_record_with_expansions.expanded_to

    assert [
        (
            expanded.unscoped_reference,
            expanded.resolved_version,
            expanded.input_index,
            expanded.type,
        )
        for expanded in expanded_records
    ] == [
        ("global:@alpha_one", 0, 0, "static"),
        ("global:@alpha_two", 0, 1, "static"),
    ]


def test_dynamic_expanded_records_preserve_associated_input_name(dynamic_use_record_with_expansions):
    dynamic_read = dynamic_use_record_with_expansions

    for expanded in dynamic_read.expanded_to:
        assert expanded.associated_input[0] == dynamic_read.associated_input[0]


def test_dynamic_expanded_back_link_is_not_the_concrete_static_dep(dynamic_use_record_with_expansions):
    dynamic_read = dynamic_use_record_with_expansions

    for expanded in dynamic_read.expanded_to:
        assert expanded_from_of(expanded) != expanded.dep
        assert expanded_from_of(expanded) == dynamic_read.dep


def test_dynamic_expanded_records_preserve_distinct_concrete_deps(dynamic_use_record_with_expansions):
    expanded_one, expanded_two = dynamic_use_record_with_expansions.expanded_to

    assert expanded_one.dep != expanded_two.dep
    assert expanded_one.unscoped_reference == "global:@alpha_one"
    assert expanded_two.unscoped_reference == "global:@alpha_two"
