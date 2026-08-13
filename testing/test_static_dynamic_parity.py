import pytest

from types import SimpleNamespace

from tide_tooling import TIDE
from tide_tooling.core import DataRef
from tide_tooling.tooling_core.engine.tide_logging import DependencyUseRecord, ExpandedDepRecord


# ================================================================
# STATIC / DYNAMIC PARITY TESTS
# ================================================================
#
# Core question:
#   Once static and dynamic dependencies resolve to concrete refs, do they expose
#   the same useful provenance fields?
#
# This file intentionally does NOT retest dynamic runtime matching.
# That belongs in test_dynamic_refs.py.
#
# Here we compare:
#   - runtime static exec reads
#   - event/static reads
#   - seeded exec reads
#   - seeded event reads
#   - direct dynamic DependencyUseRecord + ExpandedDepRecord shape
#
# Contract:
#   Static concrete read and dynamic expanded concrete read both preserve:
#       unscoped_reference
#       resolved_version
#       associated_input
#       input_index
#       producer_exec_id/source/actor where applicable
#
# Runtime/event/seeded examples prove the static side in real traces.
# Direct dynamic records prove the dynamic-expanded side without guessing the
# current wildcard fixture syntax.
# ================================================================


def register_seeded_store(ocean, name, values):
    # NOTE:
    # Current project notes say register_store can receive a kv dict.
    # This helper only handles call-shape differences; semantic failures still fail.
    try:
        return ocean.register_store(name, values)
    except TypeError:
        return ocean.register_store(name, store_template=values)


def first_exec(trace, seq_id):
    return trace.inspect(seq_id, "seq_id").object


def associated_input(name="args", raw=("@@A",)):
    return (
        name,
        SimpleNamespace(
            raw=raw,
            evaluated=raw,
            normalised=raw,
            resolved=raw,
        ),
    )


def static_dep(ref_name="@A", store="global"):
    return DataRef(ref_name, store)


def dynamic_dep(pattern="@A*", store="global"):
    return DataRef(pattern, store)


def dynamic_expanded_record(
    declared_dynamic_dep=None,
    associated=None,
    concrete_ref="@A",
    store="global",
    value="A",
    version=0,
    input_index=0,
):
    declared_dynamic_dep = declared_dynamic_dep or dynamic_dep(store=store)
    associated = associated or associated_input()

    return ExpandedDepRecord(
        associated_input=associated,
        dep=static_dep(concrete_ref, store),
        type="static",
        unscoped_ref=f"{store}:{concrete_ref}",
        resolved_val=value,
        resolved_ver=version,
        producer_source="dynamic-producer-source",
        producer_actor="dynamic-producer-actor",
        producer_exec_id="dynamic-producer-exec",
        expanded_from=declared_dynamic_dep,
        input_index=input_index,
    )


def dynamic_use_record(expanded, associated=None, input_index=0, store="global"):
    associated = associated or associated_input()

    return DependencyUseRecord(
        associated_input=associated,
        dep=dynamic_dep(store=store),
        type="dynamic",
        unscoped_ref=f"{store}:@A*",
        expanded_to=expanded,
        input_index=input_index,
    )


def concrete_fields_from_static_read(read):
    return {
        "unscoped_reference": read.unscoped_reference,
        "resolved_version": read.resolved_version,
        "associated_input_name": read.associated_input[0],
        "input_index": read.input_index,
    }


def concrete_fields_from_expanded(expanded):
    return {
        "unscoped_reference": expanded.unscoped_reference,
        "resolved_version": expanded.resolved_version,
        "associated_input_name": expanded.associated_input[0],
        "input_index": expanded.input_index,
    }


def test_runtime_static_exec_read_and_dynamic_expansion_share_concrete_field_shape():
    def UseStatic(a):
        return a

    tide = TIDE()

    tide.event("SeedA", writes={"@A": "A"})
    tide.exec(UseStatic, args=["@@A"], writes=["@out"])

    trace = tide.trace()
    use_exec = first_exec(trace, 1)

    static_read = use_exec.reads[0]
    expanded = dynamic_expanded_record(
        concrete_ref="@A",
        store="global",
        value="A",
        version=0,
        input_index=0,
    )

    assert concrete_fields_from_static_read(static_read) == concrete_fields_from_expanded(expanded)


def test_event_static_read_and_dynamic_expansion_share_concrete_ref_and_version_shape():
    tide = TIDE()

    tide.event("SeedA", writes={"@A": "A"})
    tide.event("UseEventA", reads={"@A": "A"}, writes={"@out": "out"})

    trace = tide.trace()
    event_exec = first_exec(trace, 1)

    static_read = event_exec.reads[0]
    expanded = dynamic_expanded_record(
        associated=static_read.associated_input,
        concrete_ref="@A",
        store="global",
        value="A",
        version=0,
        input_index=static_read.input_index,
    )

    assert static_read.unscoped_reference == expanded.unscoped_reference
    assert static_read.resolved_version == expanded.resolved_version
    assert static_read.associated_input[0] == expanded.associated_input[0]
    assert static_read.input_index == expanded.input_index


def test_seeded_exec_read_and_dynamic_expansion_share_concrete_ref_and_version_shape():
    def UseSeed(seed):
        return seed

    tide = TIDE()
    register_seeded_store(
        tide.ocean,
        "testing",
        {
            "@seed": "seed-value",
        },
    )

    tide.exec(UseSeed, args=["@testing:@seed"], writes=["@out"])

    trace = tide.trace()
    use_exec = first_exec(trace, 1) if len(trace.exec_log) > 1 else first_exec(trace, 0)

    static_read = use_exec.reads[0]
    expanded = dynamic_expanded_record(
        associated=static_read.associated_input,
        concrete_ref="@seed",
        store="testing",
        value="seed-value",
        version=0,
        input_index=static_read.input_index,
    )

    assert static_read.unscoped_reference == expanded.unscoped_reference
    assert static_read.resolved_version == expanded.resolved_version
    assert static_read.associated_input[0] == expanded.associated_input[0]
    assert static_read.input_index == expanded.input_index


def test_seeded_event_read_and_dynamic_expansion_share_concrete_ref_and_version_shape():
    tide = TIDE()
    register_seeded_store(
        tide.ocean,
        "testing",
        {
            "@seed": "seed-value",
        },
    )

    tide.event("UseSeedEvent", reads={"testing:@seed": "seed-value"}, writes={"@out": "out"})

    trace = tide.trace()
    use_exec = first_exec(trace, 1) if len(trace.exec_log) > 1 else first_exec(trace, 0)

    static_read = use_exec.reads[0]
    expanded = dynamic_expanded_record(
        associated=static_read.associated_input,
        concrete_ref="@seed",
        store="testing",
        value="seed-value",
        version=0,
        input_index=static_read.input_index,
    )

    assert static_read.unscoped_reference == expanded.unscoped_reference
    assert static_read.resolved_version == expanded.resolved_version
    assert static_read.associated_input[0] == expanded.associated_input[0]
    assert static_read.input_index == expanded.input_index


def test_static_and_dynamic_use_records_both_preserve_associated_input_and_input_index():
    associated = associated_input(raw=("@@A", "@@A"))

    static_read = DependencyUseRecord(
        associated_input=associated,
        dep=static_dep("@A", "global"),
        type="static",
        unscoped_ref="global:@A",
        resolved_val="A",
        resolved_ver=0,
        producer_source="producer-source",
        producer_actor="producer-actor",
        producer_exec_id="producer-exec",
        input_index=1,
    )
    expanded = dynamic_expanded_record(
        declared_dynamic_dep=dynamic_dep("@A*", "global"),
        associated=associated,
        concrete_ref="@A",
        store="global",
        value="A",
        version=0,
        input_index=1,
    )
    dynamic_read = dynamic_use_record(
        expanded,
        associated=associated,
        input_index=1,
        store="global",
    )

    assert static_read.associated_input[0] == dynamic_read.associated_input[0] == "args"
    assert static_read.input_index == dynamic_read.input_index == 1
    assert expanded.associated_input[0] == dynamic_read.associated_input[0]
    assert expanded.input_index == dynamic_read.input_index


def test_static_and_dynamic_same_concrete_ref_do_not_collapse_read_types_or_occurrences():
    associated = associated_input(raw=("@@A", "@@A"))

    static_read = DependencyUseRecord(
        associated_input=associated,
        dep=static_dep("@A", "global"),
        type="static",
        unscoped_ref="global:@A",
        resolved_val="A",
        resolved_ver=0,
        producer_source="producer-source",
        producer_actor="producer-actor",
        producer_exec_id="producer-exec",
        input_index=0,
    )
    expanded = dynamic_expanded_record(
        declared_dynamic_dep=dynamic_dep("@A*", "global"),
        associated=associated,
        concrete_ref="@A",
        store="global",
        value="A",
        version=0,
        input_index=1,
    )
    dynamic_read = dynamic_use_record(
        expanded,
        associated=associated,
        input_index=1,
        store="global",
    )

    assert static_read.type == "static"
    assert dynamic_read.type == "dynamic"
    assert expanded.type == "static"

    assert static_read.unscoped_reference == expanded.unscoped_reference == "global:@A"
    assert static_read.input_index != dynamic_read.input_index
    assert static_read != dynamic_read
