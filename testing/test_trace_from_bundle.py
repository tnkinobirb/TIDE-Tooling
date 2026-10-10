import io
import sys
from types import SimpleNamespace
import types
from contextlib import redirect_stdout

from tide_tooling.tooling_core.bundling import TIDEBundle
from tide_tooling.tooling_core.bundling.TIDEBundler import TIDEBundler
from tide_tooling.tooling_core.bundling.TIDEOpaqueValue import TIDEOpaqueValue
from tide_tooling.tooling_core.engine.utils import tide_value_hasher


def make_trace(values, ref_versions, exec_log):
    return SimpleNamespace(
        name="TIDE: .trace()",
        ocean_id="ocean",
        run="run_1",
        pipeline="TIDE: UNKNOWN",
        index=SimpleNamespace(
            value_by_hash=values,
            value_hash_by_ref_version=ref_versions,
        ),
        exec_log=exec_log,
    )


def unsupported_value():
    return "not portable"


def make_actor(name):
    return SimpleNamespace(
        source_ocean="ocean",
        source_runner="run_1",
        source_actor=name,
    )


def make_exec_record(seq_id, exec_id, name, deps=(), writes=()):
    return SimpleNamespace(
        seq_id=seq_id,
        exec_id=exec_id,
        source_ocean_id="ocean",
        runner_id="run_1",
        pipeline_id="pipeline_1",
        node_id=name,
        func_name=name,
        content_id=b"content",
        status="succeeded",
        started_at=1,
        finished_at=2,
        duration_ms=1,
        error=None,
        node_options={"resolve_output": False},
        deps=list(deps),
        writes=list(writes),
        effects=[],
        emitted_nodes=[],
        producer_id=make_actor(name),
    )


def make_write(target, version, value, cause):
    return SimpleNamespace(
        cause=cause,
        stored_result=value,
        stored_version=version,
        stored_target=target,
    )


def make_read(reference, version, value, producer):
    return SimpleNamespace(
        type="static",
        unscoped_reference=reference,
        resolved_version=version,
        resolved_value=value,
        producer_source="run_1",
        producer_actor=producer.producer_id,
        producer_exec_id=producer.exec_id,
        associated_input=("args", SimpleNamespace()),
        input_index=0,
    )


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

    assert [record.func_name for record in trace.exec_log] == ["A", "B", "C"]
    assert len(trace.reads) == 1
    assert len(trace.writes) == 3
    assert [(write.stored_target, write.stored_version) for write in trace.writes] == [
        ("global:@seed", 0),
        ("global:@result", 0),
        ("global:@seed", 1),
    ]

    read = trace.reads[0]
    assert read.unscoped_reference == "global:@seed"
    assert read.resolved_version == 0
    assert read.producer_exec_id == "TIDE_EXEC:writer"
    assert read.resolved_value == {"seed": [b"tide", 0]}
    assert trace.index.ref_version_to_producer_exec_id[
        ("global:@result", 0)
    ] == "TIDE_EXEC:consumer"
    assert "TIDE_EXEC:consumer" in trace.index.ref_version_to_consumer_exec_ids[
        ("global:@seed", 0)
    ]


def test_trace_from_bundle_keeps_the_trace_query_surface_inspectable():
    trace = restored_trace()
    seed_hash = tide_value_hasher({"seed": [b"tide", 0]})

    inspected_value = trace.inspect(seed_hash, "hash")
    inspected_ref = trace.inspect("global:@seed", "reference", version=0)
    shown = trace.show()

    assert inspected_value.object.object == {"seed": [b"tide", 0]}
    assert inspected_ref.object.version == 0
    assert trace.value_of(seed_hash) == {"seed": [b"tide", 0]}
    assert trace.value_of("global:@seed", 0) == {"seed": [b"tide", 0]}
    assert trace.value_of("global:@seed") == [
        {"seed": [b"tide", 0]},
        {"seed": [b"tide", 1]},
    ]
    assert "3 executions found | 3 writes | 1 read" in shown


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


def test_bundle_rekeys_a_pre_upgrade_trace_index_from_serialised_values():
    value = {"value": [b"tide"]}
    complete = TIDEBundler().bundle_trace(
        make_trace(
            {"sha3_512_tide:legacy": value},
            {("global:@A", 0): "sha3_512_tide:legacy"},
            [],
        )
    )
    value_hash = tide_value_hasher(value)

    assert complete["manifest"]["value_hash_codec"] == (
        "tide_serialised_bytes_v1"
    )
    assert complete["manifest"]["ref_index"]["global:@A"]["versions"]["0"][
        "value_hash"
    ] == value_hash
    assert complete["manifest"]["values"][value_hash]["root_cas_ref"] in complete[
        "cas"
    ]


def test_load_trace_bundle_migrates_legacy_repr_value_hashes():
    value = {"value": [b"tide"]}
    complete = TIDEBundler().bundle_trace(
        make_trace(
            {tide_value_hasher(value): value},
            {("global:@A", 0): tide_value_hasher(value)},
            [],
        )
    )
    manifest = complete["manifest"]
    value_hash = tide_value_hasher(value)
    legacy_hash = "sha3_512_tide:legacy"
    manifest["values"][legacy_hash] = manifest["values"].pop(value_hash)
    manifest["value_index"][legacy_hash] = manifest["value_index"].pop(
        value_hash
    )
    manifest["cas_index"] = {
        cas_ref: [legacy_hash if item == value_hash else item for item in items]
        for cas_ref, items in manifest["cas_index"].items()
    }
    manifest["ref_index"]["global:@A"]["versions"]["0"][
        "value_hash"
    ] = legacy_hash
    manifest.pop("value_hash_codec")

    loaded = TIDEBundler().load_trace_bundle(complete)

    assert loaded["manifest"]["value_hash_codec"] == (
        "tide_serialised_bytes_v1"
    )
    assert loaded["ref_index"]["global:@A"]["versions"]["0"][
        "value_hash"
    ] == value_hash
    assert loaded["values"] == {value_hash: value}


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
    shown = trace.show()

    assert isinstance(value, TIDEOpaqueValue)
    assert value.value_hash == value_hash
    assert value.type_name == "function"
    assert trace.from_cas is True
    assert trace.bundle_context["value_count"] == 1
    assert trace.bundle_context["opaque_value_hashes"] == [value_hash]
    assert trace.writes[0].stored_result is value
    assert "opaque<function>[sha3_512_tide:" in shown


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

    assert rebuilt.run == "MULTIPLE (2)"
    assert rebuilt.pipeline == "MULTIPLE (2)"
    assert [
        source["pipeline"] for source in rebuilt.execution_sources
    ] == [
        "TIDE: .exec(first[0])",
        "TIDE: .exec(second[1])",
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

    assert [record.func_name for record in rebuilt.exec_log] == [
        "create_data",
        "process_data",
    ]
    assert rebuilt.exec_log[1].deps[0].dep.store == "__tide___TIDE: .exec(0)"
    assert rebuilt.exec_log[1].deps[0].dep.key == (
        "@tmp_node_link.create_data"
    )
    assert rebuilt.value_of("mem_TIDE: .exec(0):result", 0) == 2


def test_effect_evidence_survives_json_bundle_round_trip(capsys):
    from tide_tooling import DELETE, READ, Effects, TIDE, WRITE, node

    prepared_value = {"status": "prepared", "count": 3}
    audit_value = {"event": "prepare", "accepted": True}

    @node(effects=[
        WRITE("global:prepared", preserve_output=prepared_value),
        WRITE("global:audit", preserve_output=audit_value),
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
        record for record in rebuilt.exec_log
        if record.func_name == "prepare"
    )
    consume_record = next(
        record for record in rebuilt.exec_log
        if record.func_name == "consume"
    )

    assert [type(effect).__name__ for effect in prepare_record.declared_effects] == [
        "WRITE",
        "WRITE",
    ]
    assert prepare_record.declared_effects[0].preserve_output == prepared_value
    assert consume_record.runtime_effects[0]["effect"] == Effects.READ
    assert consume_record.runtime_effects[0]["value"] == prepared_value

    prepared_effect = rebuilt.inspect(
        (Effects.WRITE, ("global:prepared", 0)),
        "effect",
    )
    effect_record = prepared_effect.object.effects[0]
    assert effect_record.output == prepared_value

    path = rebuilt.path(
        (Effects.WRITE, ("global:prepared", 0)),
        consume_record.exec_id,
        "effect",
        "exec_record",
    )
    assert path.exists()
    path.show(color=False)
    assert "affected by WRITE effect" in capsys.readouterr().out

    compared = rebuilt.diff(
        (Effects.WRITE, ("global:prepared", 0)),
        (Effects.WRITE, ("global:audit", 0)),
    )
    assert not compared.same


def test_deleted_reference_read_remains_effect_evidence_without_fake_dependency():
    from tide_tooling import DELETE, READ, Effects, TIDE
    from tide_tooling.tooling_core.engine.classes import __TIDE_TEMPLATE_VALUE__

    tide = TIDE()
    tide.declare("temporary", {"available": True})
    tide.event("delete temporary", effects=DELETE("global:temporary"))

    def read_deleted():
        return READ("global:temporary")

    result = tide.exec(read_deleted, writes="fallback")
    source = tide.trace()
    read_record = source.exec_log[-1]

    assert isinstance(result, __TIDE_TEMPLATE_VALUE__)
    assert not any(
        dependency.associated_input[0] == "runtime_read"
        for dependency in read_record.deps
    )
    assert read_record.runtime_effects[0]["found"] is False
    source_delete = source.investigate(Effects.DELETE, "effect").to_dict()
    assert source_delete["summary"]["effect_count"] == 1
    assert source_delete["summary"]["affected_execution_count"] == 1
    assert source.investigate(Effects.READ, "effect").to_dict()[
        "summary"
    ]["effect_count"] == 1

    rebuilt = TIDE.from_bundle(source.to_bundle().to_dict())
    rebuilt_read = rebuilt.exec_log[-1]
    rebuilt_effect = rebuilt_read.effects[0]

    assert rebuilt_read.runtime_effects[0]["found"] is False
    assert not rebuilt_read.deps
    assert len(rebuilt_read.writes) == 1
    assert isinstance(
        rebuilt_read.writes[0].stored_result,
        __TIDE_TEMPLATE_VALUE__,
    )
    assert rebuilt_effect.effect == Effects.READ
    assert isinstance(rebuilt_effect.output, __TIDE_TEMPLATE_VALUE__)
    assert rebuilt.investigate(Effects.DELETE, "effect").to_dict()[
        "summary"
    ]["effect_count"] == 1
    assert rebuilt.investigate(Effects.DELETE, "effect").to_dict()[
        "summary"
    ]["affected_execution_count"] == 1
    assert rebuilt.path(
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
        WRITE("global:prepared", preserve_output={"status": "prepared"}),
        WRITE("global:audit", preserve_output={"status": "live"}),
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
    green_prepared = "\x1b[32mglobal:prepared[v0]\x1b[0m"
    red_audit = "\x1b[31mglobal:audit[v0]\x1b[0m"
    green_audit = "\x1b[32mglobal:audit[v0]\x1b[0m"

    for trace in (source, rebuilt):
        shown = trace.show(color=True)

        # prepared: write line, WRITE effect, DELETE effect, failed READ.
        assert shown.count(red_prepared) == 4
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
            for record in trace.exec_log
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
        assert red_prepared in delete_path
