import io
import sys
import types
from contextlib import redirect_stdout

import pytest

from tide_tooling.tooling_core.bundling import TIDEBundle
from tide_tooling.tooling_core.bundling.TIDEBundler import TIDEBundler
from tide_tooling.tooling_core.bundling.TIDEOpaqueValue import TIDEOpaqueValue
from tide_tooling.tooling_core.engine.events import Cause
from tide_tooling.tooling_core.engine.tide_logging import (
    DependencyUseRecord,
    ExecRecord,
    InputUseRecord,
    Inputs,
    Trace,
    WriteRecord,
)
from tide_tooling.tooling_core.engine.utils import tide_value_hasher


def make_trace(values, ref_versions, exec_log):
    trace = Trace(
        name="TIDE: .trace()",
        ocean_id="ocean",
        log=exec_log,
    )
    # Synthetic fixture evidence: public Trace indexes are deliberately sealed.
    object.__setattr__(trace._index, "value_by_hash", dict(values))
    object.__setattr__(
        trace._index,
        "value_hash_by_ref_version",
        dict(ref_versions),
    )
    return trace


def unsupported_value():
    return "not portable"


def make_exec_record(seq_id, exec_id, name, deps=(), writes=()):
    return ExecRecord(
        seq_id=seq_id,
        exec_id=exec_id,
        ocean_id="ocean",
        runner_id="run_1",
        pipeline_id="pipeline_1",
        node_id=name,
        func=None,
        func_name=name,
        inputs=Inputs({}),
        deps=list(deps),
        effects=[],
        declared_effects=[],
        declared_targets=[],
        runtime_effects=[],
        runtime_targets=[],
        declared_deps=[],
        unscoped_store_refs=[],
        content_id=b"content",
        status="succeeded",
        started_at=1,
        finished_at=2,
        duration_ms=1,
        error=None,
        node_options={"preserve_exec_id": True},
        writes=list(writes),
        emitted_nodes=[],
    )


def make_write(target, version, value, cause):
    return WriteRecord(
        cause=Cause(description=cause),
        stored_result=value,
        stored_version=version,
        stored_target=target,
    )


def make_read(reference, version, value, producer):
    input_record = InputUseRecord(
        "args",
        None,
        None,
        None,
        None,
        [],
    )
    dependency = DependencyUseRecord(
        associated_input=("args", input_record),
        dep=None,
        type="static",
        unscoped_ref=reference,
        resolved_ver=version,
        resolved_val=value,
        producer_source="run_1",
        producer_actor=producer.producer_id,
        producer_exec_id=producer.exec_id,
        input_index=0,
    )
    input_record.dependencies.append(dependency)
    return dependency


def restored_trace():
    sys.modules.setdefault("graphviz", types.ModuleType("graphviz"))
    sys.modules.setdefault("jsonpickle", types.ModuleType("jsonpickle"))

    seed_0 = {"seed": [b"tide", 0]}
    result = {"result": "from-seed"}
    seed_1 = {"seed": [b"tide", 1]}
    writer = make_exec_record(
        0,
        "TIDE_EXEC:writer",
        "A",
        writes=[make_write("global:@seed", 0, seed_0, "TIDE: .event()")],
    )
    consumer = make_exec_record(
        1,
        "TIDE_EXEC:consumer",
        "B",
        deps=[make_read("global:@seed", 0, seed_0, writer)],
        writes=[make_write("global:@result", 0, result, "TIDE: .exec(B)")],
    )
    updater = make_exec_record(
        2,
        "TIDE_EXEC:updater",
        "C",
        writes=[make_write("global:@seed", 1, seed_1, "TIDE: .event()")],
    )
    values = [seed_0, result, seed_1]
    source = make_trace(
        {tide_value_hasher(value): value for value in values},
        {
            ("global:@seed", 0): tide_value_hasher(seed_0),
            ("global:@result", 0): tide_value_hasher(result),
            ("global:@seed", 1): tide_value_hasher(seed_1),
        },
        [writer, consumer, updater],
    )

    return TIDEBundle.from_dict(
        TIDEBundle.from_trace(source).to_dict()
    ).trace()


def test_trace_from_bundle_preserves_execs_refs_values_and_provenance():
    trace = restored_trace()

    assert {record.func_name for record in trace.log} == {"A", "B", "C"}
    assert len(trace.reads) == 1
    assert len(trace.writes) == 3
    assert {(write.stored_target, write.stored_version) for write in trace.writes} == {
        ("global:@seed", 0),
        ("global:@result", 0),
        ("global:@seed", 1),
    }

    read = trace.reads[0]
    assert read.unscoped_reference == "global:@seed"
    assert read.resolved_version == 0
    assert read.producer_exec_id == "TIDE_EXEC:writer"
    assert read.resolved_value == {"seed": [b"tide", 0]}
    assert list(trace.index.producer_exec_ids_by_ref_version[
        ("global:@result", 0)
    ]) == ["TIDE_EXEC:consumer"]
    assert "TIDE_EXEC:consumer" in trace.index.consumer_exec_ids_by_ref_version[
        ("global:@seed", 0)
    ]


def test_trace_from_bundle_keeps_the_trace_query_surface_inspectable():
    trace = restored_trace()
    seed_hash = tide_value_hasher({"seed": [b"tide", 0]})

    inspected_value = trace.inspect(seed_hash, "hash")
    inspected_ref = trace.inspect("global:@seed", "reference", version=0)
    shown = trace.show(color=False)

    assert inspected_value.object.object == {"seed": [b"tide", 0]}
    assert inspected_ref.object.version == 0
    assert trace.value_of(seed_hash) == {"seed": [b"tide", 0]}
    assert trace.value_of("global:@seed", 0) == {"seed": [b"tide", 0]}
    assert trace.value_of("global:@seed") == [
        {"seed": [b"tide", 0]},
        {"seed": [b"tide", 1]},
    ]
    assert "3 executions shown | 3 writes | 1 read" in shown


def test_value_of_can_show_single_and_versioned_values_without_wrapping_them():
    trace = restored_trace()
    single_output = io.StringIO()
    versions_output = io.StringIO()

    with redirect_stdout(single_output):
        single = trace.value_of("global:@seed", 0, show=True)

    with redirect_stdout(versions_output):
        versions = trace.show_value_of("global:@seed")

    assert single == {"seed": [b"tide", 0]}
    assert versions == [
        {"seed": [b"tide", 0]},
        {"seed": [b"tide", 1]},
    ]
    assert "VALUE" in single_output.getvalue()
    assert "Reference: global:@seed[v0]" in single_output.getvalue()
    assert "type:" in single_output.getvalue()
    assert "VERSIONS" in versions_output.getvalue()
    assert "[v0]" in versions_output.getvalue()
    assert "[v1]" in versions_output.getvalue()


def test_trace_from_bundle_restores_and_displays_opaque_values():
    sys.modules.setdefault("graphviz", types.ModuleType("graphviz"))
    sys.modules.setdefault("jsonpickle", types.ModuleType("jsonpickle"))

    value_hash = tide_value_hasher(unsupported_value)
    writer = make_exec_record(
        0,
        "TIDE_EXEC:opaque",
        "OpaqueWriter",
        writes=[
            make_write(
                "global:@opaque",
                0,
                unsupported_value,
                "TIDE: .event()",
            )
        ],
    )
    source = make_trace(
        {value_hash: unsupported_value},
        {("global:@opaque", 0): value_hash},
        [writer],
    )
    bundle = TIDEBundle.from_dict(TIDEBundle.from_trace(source).to_dict())
    value = bundle.value_at("global:@opaque", 0)
    trace = bundle.trace()
    shown = trace.show(color=False)
    shown_with_values = trace.show(values=True, color=False)

    assert isinstance(value, TIDEOpaqueValue)
    assert value.value_hash == value_hash
    assert value.type_name == "function"
    assert trace.from_cas is True
    assert "codec_version" not in trace.bundle_context
    assert trace.writes[0].stored_result is not value
    assert trace.writes[0].stored_result.value_hash == value.value_hash
    assert "wrote: function as global:@opaque[v0]" in shown
    assert "function opaque<function>[sha3_512_tide:" in shown_with_values


def test_trace_bundle_preserves_multiple_exec_source_names():
    from tide_tooling import TIDE

    def first():
        return 1

    def second(value):
        return value + 1

    tide = TIDE()
    tide.exec(first, writes="first")
    tide.exec(second, reads="first", writes="second")

    rebuilt = TIDE.from_bundle(tide.trace().to_bundle())

    assert rebuilt.runs == (0, 1)
    assert rebuilt._sources() == (
        (0, 0),
        (1, 1),
    )
    assert [
            pipeline for pipeline in rebuilt.pipelines.values()
    ] == [
        "TIDE: .run_node(first)",
        "TIDE: .run_node(second)",
    ]


def test_nested_node_bundle_restores_runtime_scoped_references():
    from tide_tooling import TIDE, node

    @node
    def create_data():
        return {"value": 1}

    @node(targets=["result"])
    def process_data(raw_data):
        return raw_data["value"] + 1

    tide = TIDE()
    tide.exec(process_data.as_node(create_data.as_node()))
    source = tide.trace()
    rebuilt = TIDE.from_bundle(source.to_bundle())

    assert [record.func_name for record in rebuilt.log] == [
        "create_data",
        "process_data",
    ]
    assert rebuilt.log[1].deps[0].dep.store == "__tide__"
    assert rebuilt.log[1].deps[0].unscoped_reference.startswith(
        "__tide___0:"
    )
    expected_key = source.log[1].deps[0].dep.key
    assert rebuilt.log[1].deps[0].dep.key == expected_key
    assert rebuilt.value_of("mem_0:result", 0) == 2


def test_effect_evidence_survives_json_bundle_round_trip(capsys):
    from tide_tooling import DELETE, READ, READ_EFFECT, Effects, TIDE, WRITE, node
    from tide_tooling.tooling_core.engine.classes import __fallback__

    prepared_value = {"status": "prepared", "count": 3}
    audit_value = {"event": "prepare", "accepted": True}

    @node(effects=[
        WRITE("global:prepared", prepared_value),
        WRITE("global:audit", audit_value),
    ])
    def prepare(source):
        return {"received": source}

    def consume():
        return READ("global:prepared")

    tide = TIDE()
    tide.declare("source", [1, 2, 3])
    tide.exec(prepare, reads="source")
    tide.exec(consume, writes="consumed")
    tide.event("cleanup", effects=DELETE("global:audit"))
    source = tide.trace()

    exported = source.to_bundle().to_dict()
    rebuilt = TIDE.from_bundle(TIDEBundle.from_dict(exported))

    source_write = source.investigate(Effects.WRITE, "effect").to_dict()
    rebuilt_write = rebuilt.investigate(Effects.WRITE, "effect").to_dict()
    assert rebuilt_write["summary"] == source_write["summary"]
    assert rebuilt_write["history"] == source_write["history"]

    prepare_record = next(
        record for record in rebuilt.execs.values()
        if record.func_name == "prepare"
    )
    consume_record = next(
        record for record in rebuilt.execs.values()
        if record.func_name == "consume"
    )

    assert [type(effect).__name__ for effect in prepare_record.declared_effects] == [
        "WRITE",
        "WRITE",
    ]
    assert isinstance(prepare_record.declared_effects[0].output, __fallback__)
    assert isinstance(consume_record.runtime_effects[0], READ_EFFECT)
    runtime_read = next(effect for effect in consume_record.effects if effect.effect == Effects.READ)
    assert runtime_read.output == prepared_value

    prepared_effect = rebuilt.inspect(
        (Effects.WRITE, ("global:prepared", 0)),
        "effect",
    )
    effect_record = prepared_effect.object.effects[0]
    assert isinstance(effect_record.output, __fallback__)

    path = rebuilt.path(
        (Effects.WRITE, ("global:prepared", 0)),
        consume_record.exec_id,
        "effect",
        "exec_record",
    )
    assert path.exists()
    path.show(color=False)
    assert "WRITE at global:prepared[v0]" in capsys.readouterr().out

    compared = rebuilt.diff(
        (Effects.WRITE, ("global:prepared", 0)),
        (Effects.WRITE, ("global:audit", 0)),
    )
    assert not compared.same


def test_declared_write_value_uses_cas_and_preserves_cycles_and_aliases():
    from tide_tooling import TIDE, WRITE, node

    value = []
    value.append(value)

    @node(effects=WRITE("global:cycle", value))
    def write_cycle():
        return "done"

    tide = TIDE()
    tide.exec(write_cycle)
    bundle = tide.trace().to_bundle()

    rebuilt_record = TIDE.from_bundle(bundle).log[0]
    rebuilt_declared = rebuilt_record.declared_effects[0].targets[0][1]
    rebuilt_written = rebuilt_record.writes[0].stored_result

    assert rebuilt_declared[0] is rebuilt_declared
    assert rebuilt_written[0] is rebuilt_written
    assert tide_value_hasher(rebuilt_declared) == tide_value_hasher(rebuilt_written)


def test_runtime_read_fallback_uses_cas_and_preserves_cycles():
    from tide_tooling import READ, TIDE

    fallback = []
    fallback.append(fallback)

    def read_missing():
        READ("global:missing", fallback=fallback)
        return "done"

    tide = TIDE()
    tide.exec(read_missing)
    rebuilt = TIDE.from_bundle(tide.trace().to_bundle())
    execution = next(iter(rebuilt.execs.values()))
    rebuilt_fallback = execution.runtime_effects[0].fallback

    assert rebuilt_fallback[0] is rebuilt_fallback


def test_deleted_reference_read_remains_effect_evidence_without_fake_dependency():
    from tide_tooling import DELETE, READ, Effects, TIDE
    from tide_tooling.tooling_core.engine.classes import __fallback__

    tide = TIDE()
    tide.declare("temporary", {"available": True})
    tide.event("delete temporary", effects=DELETE("global:temporary"))

    def read_deleted():
        return READ("global:temporary")

    result = tide.exec(read_deleted, writes="fallback")
    source = tide.trace()
    read_record = source.log[-1]

    assert isinstance(result, __fallback__)
    assert not any(
        dependency.associated_input[0] == "runtime_read"
        for dependency in read_record.deps
    )
    assert read_record.runtime_effects[0].kind == Effects.READ
    runtime_read = next(effect for effect in read_record.effects if effect.effect == Effects.READ)
    assert runtime_read.target_existed is False
    source_delete = source.investigate(Effects.DELETE, "effect").to_dict()
    assert source_delete["summary"]["effect_count"] == 1
    assert source_delete["summary"]["affected_execution_count"] == 0
    assert source.investigate(Effects.READ, "effect").to_dict()[
        "summary"
    ]["effect_count"] == 1

    rebuilt = TIDE.from_bundle(source.to_bundle().to_dict())
    rebuilt_read = rebuilt.log[-1]
    rebuilt_effect = rebuilt_read.effects[0]

    assert rebuilt_read.runtime_effects[0].kind == Effects.READ
    assert rebuilt_effect.target_existed is False
    assert not rebuilt_read.deps
    assert len(rebuilt_read.writes) == 1
    assert isinstance(
        rebuilt_read.writes[0].stored_result,
        __fallback__,
    )
    assert rebuilt_effect.effect == Effects.READ
    assert isinstance(rebuilt_effect.output, __fallback__)
    assert rebuilt.investigate(Effects.DELETE, "effect").to_dict()[
        "summary"
    ]["effect_count"] == 1
    assert rebuilt.investigate(Effects.DELETE, "effect").to_dict()[
        "summary"
    ]["affected_execution_count"] == 0
    assert not rebuilt.path(
        (Effects.DELETE, ("global:temporary", 0)),
        rebuilt_read.exec_id,
        "effect",
        "exec_record",
    ).exists()
    assert not rebuilt.path(
        (Effects.WRITE, ("global:temporary", 0)),
        rebuilt_read.exec_id,
        "effect",
        "exec_record",
    ).exists()


def test_trace_show_colours_effect_references_by_availability_and_usage():
    from tide_tooling import DELETE, READ, Effects, TIDE, WRITE, node

    @node(effects=[
        WRITE("global:prepared", {"status": "prepared"}),
        WRITE("global:audit", {"status": "live"}),
        DELETE("global:prepared"),
    ])
    def prepare():
        return "done"

    def consume_deleted():
        return READ("global:prepared")

    tide = TIDE()
    tide.exec(prepare)
    tide.exec(consume_deleted)
    source = tide.trace()
    rebuilt = TIDE.from_bundle(source.to_bundle().to_dict())

    red_prepared = "\x1b[31mglobal:prepared[v0]\x1b[0m"
    red_missing_prepared = "\x1b[31mglobal:prepared\x1b[0m"
    green_prepared = "\x1b[32mglobal:prepared[v0]\x1b[0m"
    red_audit = "\x1b[31mglobal:audit[v0]\x1b[0m"
    green_audit = "\x1b[32mglobal:audit[v0]\x1b[0m"

    for trace in (source, rebuilt):
        shown = trace.show(color=True)

        # prepared: write line, WRITE effect, DELETE effect, failed READ.
        assert shown.count(red_prepared) == 3
        assert shown.count(red_missing_prepared) == 1
        assert green_prepared not in shown

        # audit: write line and WRITE effect; available but unused is red.
        assert shown.count(red_audit) == 2
        assert green_audit not in shown

        write_evidence = trace.investigate(
            Effects.WRITE,
            "effect",
        ).show(color=True)
        delete_evidence = trace.investigate(
            Effects.DELETE,
            "effect",
        ).show(color=True)
        consumer = next(
            record
            for record in trace.execs.values()
            if record.func_name == "consume_deleted"
        )
        delete_path = trace.path(
            (Effects.DELETE, ("global:prepared", 0)),
            consumer.exec_id,
            "effect",
            "exec_record",
        ).show(color=True)

        assert red_prepared in write_evidence
        assert red_audit in write_evidence
        assert red_prepared in delete_evidence
        assert "No paths found" in delete_path
