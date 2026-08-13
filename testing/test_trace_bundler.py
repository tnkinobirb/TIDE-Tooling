import sys
from types import SimpleNamespace
import types

import pytest

from tide_tooling.tooling_core.bundling.TIDEBundler import TIDEBundler
from tide_tooling.tooling_core.bundling.TIDECas import TIDECas
from tide_tooling.tooling_core.engine.utils import tide_value_hasher


def make_trace(values, ref_versions):
    return SimpleNamespace(
        name="TIDE: .trace()",
        ocean_id="ocean",
        run="run_1",
        pipeline="TIDE: UNKNOWN",
        index=SimpleNamespace(
            value_by_hash=values,
            value_hash_by_ref_version=ref_versions,
        ),
    )


def make_exec_record(
    seq_id,
    exec_id,
    actor_name,
    deps=(),
    writes=(),
):
    actor = SimpleNamespace(
        source_ocean="ocean",
        source_runner="run_1",
        source_actor=actor_name,
    )

    return SimpleNamespace(
        seq_id=seq_id,
        exec_id=exec_id,
        source_ocean_id="ocean",
        runner_id="run_1",
        pipeline_id="pipeline_1",
        node_id=actor_name,
        func_name=actor_name,
        content_id=b"content",
        status="succeeded",
        started_at=1,
        finished_at=2,
        duration_ms=1,
        error=None,
        node_options={"resolve_output": False},
        deps=list(deps),
        writes=list(writes),
        effects={},
        emitted_by=None,
        emitted_nodes=[],
        producer_id=actor,
    )


def make_static_read(
    reference,
    version,
    value,
    producer_actor,
    producer_exec_id,
    input_name,
    input_index,
):
    return SimpleNamespace(
        type="static",
        unscoped_reference=reference,
        resolved_version=version,
        resolved_value=value,
        producer_source="run_1",
        producer_actor=producer_actor,
        producer_exec_id=producer_exec_id,
        associated_input=(input_name, SimpleNamespace()),
        input_index=input_index,
    )


def test_bundle_trace_indexes_raw_refs_versions_and_value_hashes():
    first = {"value": [b"first"]}
    second = ["second", 2]
    first_hash = tide_value_hasher(first)
    second_hash = tide_value_hasher(second)
    trace = make_trace(
        {
            first_hash: first,
            second_hash: second,
        },
        {
            ("global:@A", 0): first_hash,
            ("global:@A", 1): second_hash,
            ("testing:@B", 0): first_hash,
        },
    )
    complete = TIDEBundler().bundle_trace(
        trace,
        handle={"metadata": {"bundle": "trace-values"}},
    )
    manifest = complete["manifest"]
    first_root = manifest["values"][first_hash]["root_cas_ref"]

    assert all(type(content) is bytes for content in complete["cas"].values())
    assert manifest["metadata"] == {"bundle": "trace-values"}
    assert manifest["trace"] == {
        "name": "TIDE: .trace()",
        "ocean_id": "ocean",
        "run": "run_1",
        "pipeline": "TIDE: UNKNOWN",
    }
    assert manifest["ref_index"]["global:@A"]["versions"]["0"] == {
        "value_hash": first_hash,
        "root_cas_ref": first_root,
    }
    assert manifest["ref_index"]["global:@A"]["versions"]["1"][
        "value_hash"
    ] == second_hash
    assert manifest["value_index"][first_hash] == [
        {"ref": "global:@A", "version": 0},
        {"ref": "testing:@B", "version": 0},
    ]
    assert manifest["cas_index"][first_root] == [first_hash]


def test_bundle_trace_rebuilds_values_from_a_fresh_cas():
    value = {"shared": [b"tide"]}
    value_hash = tide_value_hasher(value)
    trace = make_trace(
        {value_hash: value},
        {("global:@A", 3): value_hash},
    )
    complete = TIDEBundler().bundle_trace(trace)
    manifest = complete["manifest"]
    fresh_cas = TIDECas().import_bytes(complete["cas"])
    value_bundle = manifest["values"][value_hash]["bundle"]
    restored = TIDEBundler().deserialise(
        fresh_cas.load_bundle(value_bundle)
    )

    assert restored == value
    assert manifest["value_index"][value_hash] == [
        {"ref": "global:@A", "version": 3}
    ]


def test_load_trace_bundle_imports_bytes_and_rebuilds_known_values():
    first = {"first": [b"tide"]}
    second = ["second", 2]
    first_hash = tide_value_hasher(first)
    second_hash = tide_value_hasher(second)
    complete = TIDEBundler().bundle_trace(
        make_trace(
            {first_hash: first, second_hash: second},
            {
                ("global:@A", 0): first_hash,
                ("global:@A", 1): second_hash,
            },
        )
    )
    loaded = TIDEBundler().load_trace_bundle(complete)

    assert loaded["values"] == {
        first_hash: first,
        second_hash: second,
    }
    assert loaded["cas"].export_bytes() == complete["cas"]
    assert loaded["ref_index"] == complete["manifest"]["ref_index"]
    assert loaded["value_index"] == complete["manifest"]["value_index"]


def test_load_trace_bundle_rejects_an_invalid_manifest_kind():
    with pytest.raises(ValueError):
        TIDEBundler().load_trace_bundle(
            {"manifest": {"kind": "wrong"}, "cas": {}}
        )


def test_load_trace_bundle_rejects_cas_over_configured_record_budget():
    complete = TIDEBundler().bundle_trace(
        [b"first", b"second", {"third": 3}]
    )
    restrictive = TIDEBundler(
        resource_limits={"max_cas_records": 1}
    )

    with pytest.raises(ValueError, match="max_cas_records"):
        restrictive.load_trace_bundle(complete)


def test_load_trace_bundle_rejects_execution_record_budget():
    from tide_tooling import TIDE

    tide = TIDE()
    tide.event("first", writes={"first": 1})
    tide.event("second", reads="first", writes={"second": 2})
    complete = tide.trace().to_bundle().to_dict()

    with pytest.raises(ValueError, match="max_exec_records"):
        TIDEBundler(
            resource_limits={"max_exec_records": 1}
        ).load_trace_bundle(complete)


def test_load_trace_bundle_rejects_reference_version_budget():
    from tide_tooling import TIDE

    tide = TIDE()
    tide.event("first", writes={"value": 1})
    tide.event("second", writes={"value": 2})
    complete = tide.trace().to_bundle().to_dict()

    with pytest.raises(ValueError, match="max_ref_versions"):
        TIDEBundler(
            resource_limits={"max_ref_versions": 1}
        ).load_trace_bundle(complete)


def test_bundle_resource_limits_reject_unknown_or_invalid_options():
    with pytest.raises(ValueError, match="Unknown TIDE bundle"):
        TIDEBundler(resource_limits={"max_unknown": 1})

    with pytest.raises(ValueError, match="positive integer"):
        TIDEBundler(resource_limits={"max_values": 0})


def test_bundle_trace_rebuilds_portable_static_and_dynamic_exec_evidence():
    seed = {"seed": [b"tide"]}
    result = ["result", 2]
    write_a = SimpleNamespace(
        cause="TIDE: .event()",
        stored_result=seed,
        stored_version=0,
        stored_target="global:@A",
    )
    write_b = SimpleNamespace(
        cause="TIDE: .exec(UseA)",
        stored_result=result,
        stored_version=0,
        stored_target="global:@B",
    )
    writer = make_exec_record(
        0,
        "TIDE_EXEC:writer",
        "WriteA",
        writes=[write_a],
    )
    static_read = make_static_read(
        "global:@A",
        0,
        seed,
        writer.producer_id,
        writer.exec_id,
        "args",
        0,
    )
    expanded_read = make_static_read(
        "global:@B",
        0,
        result,
        writer.producer_id,
        "TIDE_EXEC:consumer",
        "kwargs",
        1,
    )
    dynamic_read = SimpleNamespace(
        type="dynamic",
        unscoped_reference="global:*",
        associated_input=("kwargs", SimpleNamespace()),
        input_index=1,
        expanded_to=[expanded_read],
    )
    consumer = make_exec_record(
        1,
        "TIDE_EXEC:consumer",
        "UseA",
        deps=[static_read, dynamic_read],
        writes=[write_b],
    )
    seed_hash = tide_value_hasher(seed)
    result_hash = tide_value_hasher(result)
    trace = make_trace(
        {seed_hash: seed, result_hash: result},
        {
            ("global:@A", 0): seed_hash,
            ("global:@B", 0): result_hash,
        },
    )
    trace.exec_log = [writer, consumer]
    complete = TIDEBundler().bundle_trace(trace)
    run = complete["manifest"]["run"]
    loaded = TIDEBundler().load_trace_bundle(complete)
    restored_consumer = loaded["exec_logs"][1]

    assert run["kind"] == "tide_run"
    assert run["codec_version"] == 2
    assert run["exec_records"][1]["reads"][0]["value_hash"] == seed_hash
    assert "resolved_value" not in run["exec_records"][1]["reads"][0]
    assert run["exec_records"][1]["reads"][1]["expanded"][0][
        "value_hash"
    ] == result_hash
    assert restored_consumer.deps[0].resolved_value == seed
    assert restored_consumer.deps[0].resolved_version == 0
    assert restored_consumer.deps[0].producer_exec_id == writer.exec_id
    assert restored_consumer.deps[1].type == "dynamic"
    assert restored_consumer.deps[1].expanded_to[0].resolved_value == result
    assert restored_consumer.writes[0].stored_result == result


def test_tide_run_v1_is_rejected_instead_of_silently_degrading():
    complete = TIDEBundler().bundle_trace(
        make_trace({}, {})
    )
    complete["manifest"]["run"]["codec_version"] = 1

    with pytest.raises(ValueError, match="codec version"):
        TIDEBundler().load_trace_bundle(complete)


def test_loaded_exec_logs_rebuild_real_trace_index():
    sys.modules.setdefault("graphviz", types.ModuleType("graphviz"))
    sys.modules.setdefault("jsonpickle", types.ModuleType("jsonpickle"))
    from tide_tooling.tooling_core.engine.classes import DataRef
    from tide_tooling.tooling_core.engine.tide_logging import (
        Actor,
        DependencyUseRecord,
        ExecRecord,
        InputUseRecord,
        WriteRecord,
    )

    seed = {"seed": [b"tide"]}
    result = ["result", 2]
    writer = make_exec_record(
        0,
        "TIDE_EXEC:writer",
        "WriteA",
        writes=[
            SimpleNamespace(
                cause="TIDE: .event()",
                stored_result=seed,
                stored_version=0,
                stored_target="global:@A",
            )
        ],
    )
    consumer = make_exec_record(
        1,
        "TIDE_EXEC:consumer",
        "UseA",
        deps=[
            make_static_read(
                "global:@A",
                0,
                seed,
                writer.producer_id,
                writer.exec_id,
                "args",
                0,
            )
        ],
        writes=[
            SimpleNamespace(
                cause="TIDE: .exec(UseA)",
                stored_result=result,
                stored_version=0,
                stored_target="global:@B",
            )
        ],
    )
    seed_hash = tide_value_hasher(seed)
    result_hash = tide_value_hasher(result)
    source = make_trace(
        {seed_hash: seed, result_hash: result},
        {
            ("global:@A", 0): seed_hash,
            ("global:@B", 0): result_hash,
        },
    )
    source.exec_log = [writer, consumer]
    loaded = TIDEBundler().load_trace_bundle(
        TIDEBundler().bundle_trace(source)
    )
    trace = TIDEBundler().rebuild_trace(loaded)
    restored_writer, restored_consumer = loaded["exec_logs"]

    assert isinstance(restored_writer, ExecRecord)
    assert isinstance(restored_writer.producer_id, Actor)
    assert isinstance(restored_consumer.deps[0], DependencyUseRecord)
    assert isinstance(
        restored_consumer.deps[0].associated_input[1],
        InputUseRecord,
    )
    assert isinstance(restored_consumer.deps[0].dep, DataRef)
    assert isinstance(restored_consumer.writes[0], WriteRecord)
    assert trace.inspect(restored_writer.producer_id, "actor").exists
    assert trace.index.value_hash_by_ref_version[("global:@A", 0)] == seed_hash
    assert trace.index.ref_version_to_producer_exec_id[
        ("global:@B", 0)
    ] == "TIDE_EXEC:consumer"
    assert "TIDE_EXEC:consumer" in trace.index.ref_version_to_consumer_exec_ids[
        ("global:@A", 0)
    ]


def test_bundle_trace_accepts_a_list_of_known_values():
    values = [b"tide", {"count": 2}]
    complete = TIDEBundler().bundle_trace(values)
    manifest = complete["manifest"]

    assert manifest["trace"] == {}
    assert manifest["ref_index"] == {}
    assert set(manifest["values"]) == {
        tide_value_hasher(value)
        for value in values
    }


def test_bundle_trace_rejects_a_ref_version_without_a_known_value():
    trace = make_trace(
        {},
        {("global:@missing", 0): "sha3_512_tide:missing"},
    )

    with pytest.raises(ValueError):
        TIDEBundler().bundle_trace(trace)


def test_large_trace_paths_and_tide_run_v2_survive_round_trip():
    from tide_tooling import TIDE

    final_step = 250
    tide = TIDE()
    tide.event("step_0", writes={"step_0": 0})
    for index in range(1, final_step + 1):
        tide.event(
            f"step_{index}",
            reads=[f"step_{index - 1}"],
            writes={f"step_{index}": index},
        )

    trace = tide.trace()
    path = trace.path(
        "global:step_0",
        f"global:step_{final_step}",
        "reference",
        "reference",
    )
    bundle = trace.to_bundle()
    imported = TIDE.from_bundle(bundle)

    assert len(trace.exec_log) == final_step + 1
    assert path.exists()
    assert bundle.manifest["run"]["codec_version"] == 2
    assert len(bundle.manifest["run"]["exec_records"]) == final_step + 1
    assert imported.value_of(f"global:step_{final_step}", 0) == final_step
    assert imported.path(
        "global:step_0",
        f"global:step_{final_step}",
        "reference",
        "reference",
    ).exists()


def test_repeated_large_values_are_deduplicated_in_cas():
    from tide_tooling import TIDE

    payload = b"x" * (64 * 1024)
    versions = 96
    tide = TIDE()
    for version in range(versions):
        tide.event(
            f"write_{version}",
            writes={"payload": payload},
        )

    bundle = tide.trace().to_bundle()
    manifest = bundle.manifest
    value_hash = tide_value_hasher(payload)
    value_entry = manifest["values"][value_hash]
    root_ref = value_entry["root_cas_ref"]

    assert len(
        manifest["ref_index"]["global:payload"]["versions"]
    ) == versions
    assert manifest["cas_index"][root_ref] == [value_hash]
    assert sum(len(content) for content in bundle.cas_bytes.values()) < (
        len(payload) * 2
    )
    assert sum(len(content) for content in bundle.cas_bytes.values()) < (
        len(payload) * versions
    )
