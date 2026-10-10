import copy
import json
import inspect
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import get_args, get_type_hints

import pytest

from tide_tooling.tooling_core.engine.tide_model import TIDEModel

from tide_tooling import (
    AffectedRecord,
    EffectEdge,
    Actor,
    ConsumedBy,
    DataRef,
    Downstream,
    EMIT,
    END,
    EffectRecord,
    EmittedNodeRecord,
    EmissionEdge,
    ExecRecord,
    ExpandedDepRecord,
    InspectedGroup,
    MemoryKVStore,
    Nearby,
    Node,
    Ocean,
    Pathed,
    Pipeline,
    ProducedBy,
    ReadEdge,
    RAISE,
    RUN,
    Searched,
    TIDE,
    TIDEBundle,
    TIDEModelComparisonReport,
    TIDEModelValidityReport,
    TIDEOpaqueValue,
    Trace,
    Upstream,
    node,
    pipeline,
)
from tide_tooling.tooling_core.engine.events import LogEvents
from testing.replay_helpers import replay_events


class PublicStateValue:
    def __init__(self, value):
        self.value = value


def _incidence_pairs(entries):
    pairs = set()
    for entry in entries:
        if isinstance(entry, dict):
            pairs.add((
                entry.get("exec_id", entry.get("e")),
                entry.get("state_id", entry.get("s")),
            ))
        else:
            pairs.add(tuple(entry))
    return pairs


def _model_carriers(model):
    incidence = _incidence_pairs(model["I"]) | _incidence_pairs(model["O"])
    return (
        {exec_id for exec_id, _ in incidence},
        {state_id for _, state_id in incidence},
    )


def test_public_facade_exposes_stable_engine_names():
    assert TIDE is not None
    assert TIDEModelValidityReport is not None
    assert TIDEModelComparisonReport is not None
    assert Trace is not None
    assert Actor is not None
    assert DataRef is not None
    assert Node is not None
    assert Pipeline is not None
    assert ExecRecord is not None
    assert Ocean is not None


def test_public_facade_exposes_query_and_nested_evidence_result_types():
    public_types = (
        Searched,
        Nearby,
        Pathed,
        Upstream,
        Downstream,
        ProducedBy,
        ConsumedBy,
        InspectedGroup,
        EmittedNodeRecord,
        EmissionEdge,
        ExpandedDepRecord,
        EffectRecord,
        AffectedRecord,
        EffectEdge,
        ReadEdge,
        TIDEBundle,
        TIDEOpaqueValue,
    )

    assert all(isinstance(result_type, type) for result_type in public_types)


def test_public_query_result_types_survive_trace_bundle_round_trip():
    tide = TIDE()
    tide.event("producer", writes={"seed": 1})
    tide.event(
        "consumer",
        reads=["seed"],
        writes={"result": 2},
    )
    source = tide.trace()
    restored = TIDE.from_bundle(source.to_bundle().to_dict())

    for trace in (source, restored):
        producer = trace.inspect("producer", "actor", grouped=False)
        consumer = trace.inspect("consumer", "actor", grouped=False)
        grouped = trace.inspect("producer", "actor", grouped=True)
        searched = trace.find("producer")
        path = trace.path(producer, consumer)
        downstream = trace.downstream(producer)
        upstream = trace.upstream(consumer)
        produced = trace.produced_by(producer)
        consumed = trace.consumed_by(consumer)

        assert isinstance(grouped, InspectedGroup)
        assert isinstance(searched, Searched)
        assert isinstance(searched.nearby, Nearby)
        assert isinstance(path, Pathed)
        assert isinstance(downstream, Downstream)
        assert isinstance(upstream, Upstream)
        assert isinstance(produced, ProducedBy)
        assert isinstance(consumed, ConsumedBy)
        assert searched.first("actor").id == producer.id
        assert path.exists()
        assert produced.refs == [("global:seed", 0)]
        assert consumed.refs == [("global:seed", 0)]

    source_producer = source.inspect("producer", "actor", grouped=False)
    restored_producer = restored.inspect(
        "producer",
        "actor",
        grouped=False,
    )
    source_consumer = source.inspect("consumer", "actor", grouped=False)
    restored_consumer = restored.inspect(
        "consumer",
        "actor",
        grouped=False,
    )

    assert source.path(source_producer, source_consumer).to_dict() == (
        restored.path(restored_producer, restored_consumer).to_dict()
    )
    assert source.downstream(source_producer).to_dict() == (
        restored.downstream(restored_producer).to_dict()
    )
    assert source.upstream(source_consumer).to_dict() == (
        restored.upstream(restored_consumer).to_dict()
    )
    assert source.produced_by(source_producer).to_dict() == (
        restored.produced_by(restored_producer).to_dict()
    )
    assert source.consumed_by(source_consumer).to_dict() == (
        restored.consumed_by(restored_consumer).to_dict()
    )


def test_public_write_annotations_match_the_implemented_contracts():
    exec_writes = get_args(get_type_hints(TIDE.exec)["writes"])
    event_writes = get_args(get_type_hints(TIDE.event)["writes"])

    assert dict not in exec_writes
    assert dict in event_writes
    assert list not in event_writes


def test_exec_context_facade_preserves_the_callable_signature_and_docs():
    tide = TIDE()
    signature = inspect.signature(tide.exec)

    assert list(signature.parameters) == [
        "func",
        "args",
        "kwargs",
        "writes",
        "default_store",
        "reads",
        "p_args",
        "p_kwargs",
        "k_args",
        "k_kwargs",
        "effects",
        "return_id",
        "return_trace",
    ]
    assert get_type_hints(tide.exec)["writes"] == (
        str | list[str] | tuple[str, ...] | None
    )
    assert "names the reference" in inspect.getdoc(tide.exec)
    assert "TIDE.event(..., writes={...})" in inspect.getdoc(tide.exec)


def test_exec_rejects_dictionary_writes_and_explains_the_two_write_apis():
    tide = TIDE()

    with pytest.raises(
        TypeError,
        match=r"TIDE\.event\(writes=\{\.\.\.\}\).*WRITE effect",
    ):
        tide.exec(lambda: 1, writes={"result": 1})


def test_event_rejects_target_lists_without_written_values():
    tide = TIDE()

    with pytest.raises(
        TypeError,
        match="must be a dict mapping references to values",
    ):
        tide.event("writer", writes=["result"])


def test_event_preserves_explicit_empty_metadata_without_inventing_status():
    tide = TIDE()
    tide.event("observer", source="", cause="")
    record = tide.trace().log[-1]

    assert record.runner_id == ""
    assert record.status is None
    assert next(
        event for event in tide.ocean.event_logger.events
        if event.exec_id == record.exec_id and event.type == LogEvents.EVENT_EXEC
    ).cause.description == ""
    assert MemoryKVStore is not None
    assert callable(node)
    assert callable(pipeline)
    assert END is not None
    assert RUN is not None
    assert EMIT is not None
    assert RAISE is not None


def test_tide_facade_round_trips_a_trace_and_metadata():
    tide = TIDE()
    tide.event("writer", writes={"@message": "hello from TIDE"})
    source = tide.trace()

    bundle = source.to_bundle(
        handle={"metadata": {"source": "public-api-test"}},
    )

    assert isinstance(bundle, TIDEBundle)
    assert bundle.manifest["metadata"] == {"source": "public-api-test"}

    restored = TIDE().from_bundle(bundle)

    assert isinstance(restored, Trace)
    assert restored.value_of("global:@message", 0) == "hello from TIDE"

    # Native Trace persistence is the default. Raw Events are optional sidecar
    # evidence and are absent unless serialise_events=True.
    assert source.events
    assert restored.events == ()
    assert restored.event_log is None

def test_trace_to_bundle_is_the_reverse_trace_entry_point():
    tide = TIDE()
    tide.event("writer", writes={"@message": "trace-owned bundle"})
    trace = tide.trace()

    bundle = trace.to_bundle()
    restored = TIDE.from_bundle(bundle)

    assert isinstance(bundle, TIDEBundle)
    assert isinstance(restored, Trace)
    assert restored.value_of("global:@message", 0) == "trace-owned bundle"


def test_trace_to_bundle_can_export_directly_to_json_file():
    tide = TIDE()
    tide.event("writer", writes={"@message": "direct trace export"})
    trace = tide.trace()

    with TemporaryDirectory() as directory:
        path = Path(directory) / "trace.json"
        result = trace.to_bundle(type="file", kind="json", path=path)

        assert result == path
        assert path.exists()
        restored = TIDE.from_bundle(path)

    assert restored.value_of("global:@message", 0) == "direct trace export"


def test_tide_to_bundle_can_infer_json_file_export_from_path():
    tide = TIDE()
    tide.event("writer", writes={"@message": "direct tide export"})

    with TemporaryDirectory() as directory:
        path = Path(directory) / "trace.json"
        result = tide.to_bundle(path=path)

        assert result == path
        restored = TIDE.from_bundle(path)

    assert restored.value_of("global:@message", 0) == "direct tide export"


def test_from_bundle_accepts_relative_string_and_path_objects(monkeypatch):
    tide = TIDE()
    tide.event("writer", writes={"@message": "path inputs"})

    with TemporaryDirectory() as directory:
        directory = Path(directory)
        bundle_path = directory / "trace.json"
        tide.to_bundle(path=bundle_path)

        # Restore cwd before TemporaryDirectory cleanup. Windows cannot remove
        # the directory that the current process is using as its cwd.
        with monkeypatch.context() as scoped_monkeypatch:
            scoped_monkeypatch.chdir(directory)
            from_string = TIDE.from_bundle("trace.json")
            from_path = TIDE.from_bundle(Path("trace.json"))

    assert from_string.value_of("global:@message", 0) == "path inputs"
    assert from_path.value_of("global:@message", 0) == "path inputs"


def test_bundle_json_dict_and_file_exports_are_importable():
    tide = TIDE()
    tide.event("writer", writes={"@payload": {"bytes": b"tide"}})
    bundle = tide.to_bundle()

    json_payload = bundle.export(type="dict", kind="json")
    json.dumps(json_payload)
    restored_from_dict = TIDE.from_bundle(json_payload)
    assert restored_from_dict.value_of("global:@payload", 0) == {
        "bytes": b"tide"
    }

    with TemporaryDirectory() as directory:
        path = Path(directory) / "trace-bundle.json"
        result = bundle.export(type="file", kind="json", path=path)

        assert result == path
        assert path.exists()
        restored_from_file = TIDE().from_bundle(path)

    assert restored_from_file.value_of("global:@payload", 0) == {
        "bytes": b"tide"
    }


def test_bundle_keeps_unsupported_values_as_typed_opaque_evidence():
    tide = TIDE()
    tide.event(
        "writer",
        writes={"@payload": PublicStateValue("trusted state")},
    )
    bundle = tide.to_bundle().export(type="dict", kind="json")

    safe_value = TIDE.from_bundle(bundle).value_of(
        "global:@payload",
        0,
    )
    trusted_value = TIDE.from_bundle(
        bundle,
        trusted=True,
    ).value_of("global:@payload", 0)

    assert isinstance(safe_value, TIDEOpaqueValue)
    assert safe_value.type_name == "PublicStateValue"
    assert isinstance(trusted_value, PublicStateValue)
    assert trusted_value.value == "trusted state"


def test_from_bundle_rejects_files_over_the_configured_byte_budget():
    tide = TIDE()
    tide.event("writer", writes={"@payload": "bounded"})

    with TemporaryDirectory() as directory:
        path = Path(directory) / "trace.json"
        tide.to_bundle(path=path)

        with pytest.raises(ValueError, match="file bytes"):
            TIDE.from_bundle(
                path,
                resource_limits={"max_bundle_file_bytes": 32},
            )


def test_trusted_loading_does_not_disable_resource_limits():
    tide = TIDE()
    tide.event(
        "writer",
        writes={"@payload": PublicStateValue("still bounded")},
    )
    bundle = tide.to_bundle(serialise_events=True)

    with pytest.raises(ValueError, match="events"):
        TIDE.from_bundle(
            bundle,
            trusted=True,
            resource_limits={"max_events": 1},
        )


def test_from_model_builds_real_records_from_the_io_kernel():
    model = {
        "E": ["load", "clean"],
        "S": ["raw", "clean"],
        "I": [["clean", "raw"]],
        "O": [["load", "raw"], ["clean", "clean"]],
    }

    trace = TIDE.from_model(model)

    assert all(isinstance(record, ExecRecord) for record in trace.log)
    assert [record.func_name for record in trace.log] == [
        "clean",
        "load",
    ]
    assert [record.exec_id for record in trace.log] == [
        "clean",
        "load",
    ]
    assert trace.log[0].deps[0].producer_exec_id == "load"
    assert trace.path(
        "load",
        "clean",
        "exec_record",
        "exec_record",
    ).exists()
    assert {
        write.stored_target
        for record in trace.log
        for write in record.writes
    } == {
        "__model__:raw",
        "__model__:clean",
    }
    assert isinstance(
        trace.value_of("__model__:raw", 0),
        TIDEOpaqueValue,
    )


def test_from_model_structural_identity_is_explicit_and_reversible():
    model = {
        "E": ["host-load-id", "host-clean-id"],
        "S": ["raw-host-id", "clean-host-id"],
        "I": [["host-clean-id", "raw-host-id"]],
        "O": [
            ["host-load-id", "raw-host-id"],
            ["host-clean-id", "clean-host-id"],
        ],
    }

    literal = TIDE.from_model(model)
    structural = TIDE.from_model(model, identity_mode="structural")

    assert [record.exec_id for record in literal.log] == [
        "host-clean-id",
        "host-load-id",
    ]
    assert [record.exec_id for record in structural.log] == [
        "exec_0",
        "exec_1",
    ]
    assert [record.func_name for record in structural.log] == [
        "exec_0",
        "exec_1",
    ]
    assert set(literal.to_model()["E"]) == set(model["E"])
    assert set(literal.to_model()["S"]) == set(model["S"])
    ordinary_model = structural.to_model()
    canonical_model = structural.to_model(identity_mode="structural")
    assert ordinary_model == TIDEModel.normalise(model)
    assert _model_carriers(canonical_model) == (
        {"exec_0", "exec_1"},
        {"state_0", "state_1"},
    )


def test_from_model_builds_exec_records_in_normalised_carrier_order():
    model = {
        "E": ["consumer", "producer"],
        "S": ["input", "result"],
        "I": [["consumer", "input"]],
        "O": [
            ["consumer", "result"],
            ["producer", "input"],
        ],
    }

    trace = TIDE.from_model(model)

    assert [record.exec_id for record in trace.log] == [
        "consumer",
        "producer",
    ]
    assert [record.seq_id for record in trace.log] == [0, 1]
    assert trace.log[0].deps[0].producer_exec_id == "producer"
    assert trace.inspect(0, "seq_id").id == "consumer"
    assert trace.inspect(1, "seq_id").id == "producer"
    assert _model_carriers(trace.to_model(enrich=False))[0] == {
        "producer",
        "consumer",
    }

    restored = TIDE.from_bundle(trace.to_bundle())
    assert [record.exec_id for record in restored.log] == [
        "consumer",
        "producer",
    ]
    assert [record.seq_id for record in restored.log] == [0, 1]


def test_trace_show_structural_reuses_trace_layout_with_canonical_identities():
    model = {
        "host": "Example",
        "E": ["unknown", "host-prepare-id", "host-analyse-id"],
        "S": ["host-source-id", "host-ready-id", "host-result-id"],
        "I": [
            ["host-prepare-id", "host-source-id"],
            ["host-analyse-id", "host-ready-id"],
        ],
        "O": [
            ["unknown", "host-source-id"],
            ["host-prepare-id", "host-ready-id"],
            ["host-analyse-id", "host-result-id"],
        ],
        "executions": {
            "host-prepare-id": {"actor": {"name": "prepare_left"}},
            "host-analyse-id": {"actor": {"name": "analyse_right"}},
        },
    }
    trace = TIDE.from_model(model)

    ordinary = trace.show(color=False)
    structural = trace.show(color=False, structural=True)
    structural_coloured = trace.show(structural=True)
    structural_alias_plain = trace.show(
        colour=False,
        structural=True,
    )

    assert "TRACE | 3 executions shown | 3 writes | 2 reads" in ordinary
    assert "TRACE | 3 executions shown | 3 writes | 2 reads" in structural
    assert "host: Example" in structural
    assert "Ocean: structural" in structural
    assert "RUN:" in structural
    assert "EXECUTIONS:" in structural
    assert "prepare_left" in ordinary
    assert "analyse_right" in ordinary
    assert "prepare_left" not in structural
    assert "analyse_right" not in structural
    assert "exec_0" in structural
    assert "exec_1" in structural
    assert "__model__:state_" in structural
    assert "host-prepare-id" not in structural
    assert "host-analyse-id" not in structural
    assert "host-source-id" not in structural
    assert "\033[" not in structural
    assert "\033[" in structural_coloured
    assert "\033[" not in structural_alias_plain


def test_trace_show_structural_requires_bool():
    trace = TIDE.from_model({"E": [], "S": [], "I": [], "O": []})

    with pytest.raises(TypeError, match="show structural must be a bool"):
        trace.show(structural="yes")


def test_structural_aliases_ignore_supplied_array_order():
    model = {
        "host": "OpenLineage",
        "E": [
            "unknown",
            "prepare-left",
            "prepare-right",
            "merge",
            "analyse",
            "archive",
        ],
        # Deliberately ordered like the artifact's alphabetical host IDs.
        "S": [
            "analysis",
            "archive-record",
            "merged",
            "ready-left",
            "ready-right",
            "source-left",
            "source-right",
        ],
        "I": [
            ["prepare-left", "source-left"],
            ["prepare-right", "source-right"],
            ["merge", "ready-left"],
            ["merge", "ready-right"],
            ["analyse", "merged"],
            ["archive", "merged"],
        ],
        "O": [
            ["unknown", "source-left"],
            ["unknown", "source-right"],
            ["prepare-left", "ready-left"],
            ["prepare-right", "ready-right"],
            ["merge", "merged"],
            ["analyse", "analysis"],
            ["archive", "archive-record"],
        ],
    }
    trace = TIDE.from_model(model)

    structural = trace.to_model(
        enrich=False,
        identity_mode="structural",
    )
    shown = trace.show(color=False, structural=True)

    assert structural["I"] == [
        {"exec_id": "exec_1", "state_id": "state_0"},
        {"exec_id": "exec_1", "state_id": "state_1"},
        {"exec_id": "exec_2", "state_id": "state_4"},
        {"exec_id": "exec_3", "state_id": "state_5"},
        {"exec_id": "exec_4", "state_id": "state_4"},
        {"exec_id": "exec_5", "state_id": "state_6"},
    ]
    assert structural["O"] == [
        {"exec_id": "exec_0", "state_id": "state_5"},
        {"exec_id": "exec_0", "state_id": "state_6"},
        {"exec_id": "exec_1", "state_id": "state_4"},
        {"exec_id": "exec_2", "state_id": "state_3"},
        {"exec_id": "exec_3", "state_id": "state_0"},
        {"exec_id": "exec_4", "state_id": "state_2"},
        {"exec_id": "exec_5", "state_id": "state_1"},
    ]
    assert "wrote: opaque<TIDEUnknownValue>" in shown
    assert "as __model__:state_5[v0]" in shown
    assert "as __model__:state_6[v0]" in shown
    assert "updated __model__:state_0[v0]" not in shown
    assert "updated __model__:state_1[v0]" not in shown
    assert "exec_3" in shown
    assert "used: __model__:state_" in shown
    assert "as __model__:state_0[v0]" in shown
    assert "as __model__:state_4[v0]" in shown

    reordered = dict(model)
    reordered["E"] = list(reversed(model["E"]))
    reordered["S"] = list(reversed(model["S"]))
    assert TIDE.from_model(reordered).to_model(
        enrich=False,
        identity_mode="structural",
    ) == structural


def test_from_model_display_uses_seq_locator_and_supplied_actor_name():
    exec_id = "018f0f00-0000-7000-8000-000000000001"
    model = {
        "E": [exec_id],
        "S": ["ready"],
        "I": [],
        "O": [[exec_id, "ready"]],
        "executions": {
            exec_id: {"actor": {"name": "prepare_left"}},
        },
    }

    trace = TIDE.from_model(model)
    rendered = repr(trace.inspect(exec_id, "exec_record").object)

    assert trace.log[0].exec_id == exec_id
    assert trace.inspect(0, "seq_id").id == exec_id
    assert rendered == "EXEC_RECORD[0] prepare_left"
    assert "model_0" not in rendered


def test_from_model_without_actor_shortens_only_the_displayed_id_fallback():
    exec_id = "018f0f00-0000-7000-8000-000000000001"
    trace = TIDE.from_model({
        "E": [exec_id],
        "S": ["ready"],
        "I": [],
        "O": [[exec_id, "ready"]],
    })

    assert repr(trace.inspect(exec_id, "exec_record").object) == (
        "EXEC_RECORD[0] 018f0f00-0000-7000-80\u202600000001"
    )
    assert repr(trace.inspect(exec_id, "exec_record")).startswith(
        "Inspected(exec_record: "
        "EXEC_RECORD[0] 018f0f00-0000-7000-80\u202600000001,"
    )
    assert trace.log[0].exec_id == exec_id


def test_from_model_shortens_only_a_long_actor_display_name():
    actor = "actor-with-a-very-long-host-supplied-display-name"
    trace = TIDE.from_model({
        "E": ["host-execution-id"],
        "S": ["ready"],
        "I": [],
        "O": [["host-execution-id", "ready"]],
        "executions": {
            "host-execution-id": {"actor": {"name": actor}},
        },
    })

    assert trace.log[0].func_name == actor
    assert repr(trace.inspect(0, "seq_id").object) == (
        "EXEC_RECORD[0] actor-with-a-very-lon\u2026lay-name"
    )


def test_from_model_rejects_unknown_identity_mode():
    with pytest.raises(
        ValueError,
        match="identity_mode must be 'literal' or 'structural'",
    ):
        TIDE.from_model(
            {"E": [], "S": [], "I": [], "O": []},
            identity_mode="invented",
        )


def test_from_model_attaches_schema_host_and_host_record_to_trace_and_model(
    capsys,
):
    host_record = {
        "native_schema": "example-v1",
        "record": {"id": 17, "tags": ["realised"]},
    }
    model = {
        "host": "Example",
        "host_record": host_record,
        "E": ["exec"],
        "S": ["state"],
        "I": [],
        "O": [["exec", "state"]],
    }

    trace = TIDE.from_model(model)
    host_record["record"]["id"] = 99

    assert trace.host == "Example"
    assert trace.host_record == {
        "native_schema": "example-v1",
        "record": {"id": 17, "tags": ["realised"]},
    }
    assert trace.to_model()["host_record"] == trace.host_record
    shown = trace.show(color=False)
    capsys.readouterr()
    assert "1 execution shown | 1 write | 0 reads" in shown
    assert "host: Example" in shown
    structural = trace.to_model(identity_mode="structural")
    assert "host_record" not in structural


def test_model_host_record_survives_bundle_round_trip():
    model = {
        "host": "Example",
        "host_record": {
            "kind": "native-event",
            "payload": [1, {"two": 2}],
        },
        "E": ["exec"],
        "S": ["state"],
        "I": [],
        "O": [["exec", "state"]],
    }

    bundle = TIDE.from_model(model).to_bundle()
    json_payload = bundle.export(type="dict", kind="json")
    json.dumps(json_payload)
    restored = TIDE.from_bundle(json_payload)

    assert restored.host == "Example"
    assert restored.host_record == model["host_record"]
    assert restored.to_model()["host_record"] == model["host_record"]
    assert restored.to_dict()["host_record"] == model["host_record"]
    json.dumps(restored.to_dict()["host_record"], allow_nan=False)


@pytest.mark.parametrize(
    "host_record",
    [
        ["not", "a", "dict"],
        {"tuple": (1, 2)},
        {1: "non-string key"},
        {"number": float("nan")},
    ],
)
def test_model_validity_ignores_non_kernel_host_record(
    host_record,
):
    model = {
        "host_record": host_record,
        "E": ["exec"],
        "S": ["state"],
        "I": [],
        "O": [["exec", "state"]],
    }

    report = TIDE.report_model_validity(model)

    assert report["semantically_valid"] is True
    assert report["errors"] == []
    trace = TIDE.from_model(model)
    assert repr(trace.host_record) == repr(host_record)
    assert trace.to_model()["host_record"] == host_record


def test_trace_host_record_retains_recursive_containers():
    host_record = {}
    host_record["self"] = host_record

    trace = Trace(
        log=[],
        runner="test",
        ocean_id="test",
        host_record=host_record,
    )
    assert trace.host_record is not host_record
    detached = trace.host_record
    assert detached["self"] is detached


def test_trace_without_schema_host_record_defaults_to_none():
    trace = TIDE.from_model({
        "E": [],
        "S": [],
        "I": [],
        "O": [],
    })

    assert trace.host is None
    assert trace.host_record is None
    assert "host_record" not in trace.to_model()
    assert trace.to_dict()["host_record"] is None


@pytest.mark.parametrize(
    "incidence",
    [
        (
            {"exec_id": "consume", "state_id": "shared"},
            {"exec_id": "produce", "state_id": "shared"},
        ),
        (
            {"e": "consume", "s": "shared"},
            {"e": "produce", "s": "shared"},
        ),
        (
            ["consume", "shared"],
            ["produce", "shared"],
        ),
        (
            ("consume", "shared"),
            ("produce", "shared"),
        ),
    ],
)
def test_model_incidence_accepts_explicit_compact_and_pair_forms(
    incidence,
):
    input_edge, output_edge = incidence
    model = {
        "E": ["produce", "consume"],
        "S": ["shared"],
        "I": [input_edge],
        "O": [output_edge],
    }

    report = TIDE.report_model_validity(model)
    trace = TIDE.from_model(model)

    assert report["semantically_valid"] is True
    assert set(report.direct_handoffs) == {
        ("produce", "shared", "consume"),
    }
    assert trace.to_model()["I"] == [{
        "exec_id": "consume",
        "state_id": "shared",
    }]


@pytest.mark.parametrize(
    ("incidence_format", "input_edge", "output_edge"),
    [
        (
            "explicit",
            {"exec_id": "consume", "state_id": "shared"},
            {"exec_id": "produce", "state_id": "shared"},
        ),
        (
            "compact",
            {"e": "consume", "s": "shared"},
            {"e": "produce", "s": "shared"},
        ),
        (
            "pairs",
            ["consume", "shared"],
            ["produce", "shared"],
        ),
    ],
)
def test_to_model_emits_each_supported_incidence_format(
    incidence_format,
    input_edge,
    output_edge,
):
    trace = TIDE.from_model({
        "E": ["produce", "consume"],
        "S": ["shared"],
        "I": [{"e": "consume", "s": "shared"}],
        "O": [{"e": "produce", "s": "shared"}],
    })

    exported = trace.to_model(incidence_format=incidence_format)

    assert exported["I"] == [input_edge]
    assert exported["O"] == [output_edge]
    assert TIDE.report_model_validity(exported)["semantically_valid"] is True


def test_tide_to_model_forwards_compact_incidence_format():
    tide = TIDE()
    tide.event("produce", writes={"shared": 1})
    tide.event("consume", reads=["shared"])

    exported = tide.to_model(
        enrich=False,
        incidence_format="compact",
    )

    assert exported["I"][0]["s"] == "global:shared[v0]"
    assert exported["O"][0]["s"] == "global:shared[v0]"
    assert exported["I"][0]["e"] != exported["O"][0]["e"]
    assert _model_carriers(exported)[0] == {
        exported["I"][0]["e"],
        exported["O"][0]["e"],
    }
    assert TIDE.report_model_validity(exported)["semantically_valid"] is True


@pytest.mark.parametrize(
    "edge",
    [
        {"e": "consume", "state_id": "shared"},
        {
            "e": "consume",
            "s": "shared",
            "exec_id": "consume",
            "state_id": "shared",
        },
    ],
)
def test_model_incidence_rejects_mixed_compact_and_explicit_keys(edge):
    model = {
        "E": ["consume"],
        "S": ["shared"],
        "I": [edge],
        "O": [],
    }

    report = TIDE.report_model_validity(model)

    assert report["semantically_valid"] is None
    assert any("cannot mix compact" in error for error in report["errors"])
    with pytest.raises(ValueError, match="cannot mix compact"):
        TIDE.from_model(model)


@pytest.mark.parametrize("incidence_format", [None, "tuple", "short"])
def test_to_model_rejects_unknown_incidence_format(incidence_format):
    trace = TIDE.from_model({
        "E": [],
        "S": [],
        "I": [],
        "O": [],
    })

    with pytest.raises(ValueError, match="incidence_format"):
        trace.to_model(incidence_format=incidence_format)


def test_from_model_preserves_explicit_unknown_producer():
    model = {
        "host": "TIDE MODEL: DEMO",
        "E": ["unknown", "exec0"],
        "S": ["state0", "state1"],
        "I": [["exec0", "state0"]],
        "O": [["unknown", "state0"], ["exec0", "state1"]],
        "states": {
            "state0": {"value": "hi I am state zero"},
            "state1": {"value": "hi I am state one"},
        },
    }

    trace = TIDE.from_model(model)
    boundary_ref = "__model__:state0"
    boundary_version = (boundary_ref, 0)
    consumer_record = trace.log[0]
    boundary_record = trace.log[1]
    consumer_exec_id = consumer_record.exec_id

    assert trace.value_of(boundary_ref) == ["hi I am state zero"]
    assert trace.value_of(boundary_ref, 0) == "hi I am state zero"
    assert boundary_record.exec_id == "unknown"
    assert boundary_record.producer_id.source_actor == "unknown"
    assert boundary_record.writes[0].cause.description == "TIDE: model output"
    assert boundary_record.writes[0].stored_target == boundary_ref
    assert boundary_record.node_options == {
        "source": "tide_model",
        "preserve_exec_id": True,
    }
    assert (
        list(trace.index.producer_exec_ids_by_ref_version[boundary_version])
        == [boundary_record.exec_id]
    )
    assert consumer_record.deps[0].producer_exec_id == boundary_record.exec_id
    assert list(
        trace.index.consumer_exec_ids_by_ref_version[boundary_version]
    ) == [consumer_exec_id]

    found = trace.find("state0").first()
    assert found.id == boundary_ref
    assert found.exists
    assert trace.value_of(found) == ["hi I am state zero"]

    investigated = trace.investigate(found).to_dict()
    assert investigated["summary"]["version_count"] == 1
    assert investigated["summary"]["used_version_count"] == 1
    assert (
        investigated["history"][0]["producer"]["id"]
        == boundary_record.exec_id
    )
    assert [
        consumer["id"]
        for consumer in investigated["history"][0]["consumers"]
    ] == [consumer_exec_id]
    rendered = trace.show(color=False)
    assert "wrote: opaque<TIDEUnknownType> as __model__:state0[v0]" in rendered
    assert "updated @__model__:state0[v0]" not in rendered
    exported = trace.to_model(
        enrich=True,
    )
    assert exported["host"] == model["host"]
    assert set(exported["E"]) == set(model["E"])
    assert set(exported["S"]) == set(model["S"])
    assert exported["I"] == [
        {"exec_id": "exec0", "state_id": "state0"},
    ]
    assert _incidence_pairs(exported["O"]) == {
        ("unknown", "state0"),
        ("exec0", "state1"),
    }
    assert exported["states"] == model["states"]


def test_from_model_paths_boundary_state_to_output_through_execution():
    model = {
        "E": ["unknown", "exec0", "exec1"],
        "S": ["state0", "state1", "state2"],
        "I": [["exec0", "state0"], ["exec1", "state1"]],
        "O": [
            ["unknown", "state0"],
            ["exec0", "state1"],
            ["exec1", "state2"],
        ],
    }

    trace = TIDE.from_model(model)

    exact = trace.path(
        "__model__:state0",
        "__model__:state1",
        "reference",
        "reference",
    )
    nearby = trace.path(
        "state0",
        "state1",
        "reference",
        "reference",
        group=False,
    )
    inspected_source = trace.find("state0").first("reference")
    mixed = trace.path(
        inspected_source,
        "state1",
        target_type="reference",
        group=False,
    )
    misspelled = trace.path(
        inspected_source,
        "state_1",
        target_type="reference",
        group=False,
    )

    assert exact.exists()
    assert exact.count() == 1
    assert list(exact.paths) == ["unknown"]
    assert nearby.exists()
    assert nearby.count() == 1
    assert mixed.exists()
    assert mixed.count() == 1
    assert not misspelled.exists()
    assert trace.path(
        "__model__:state0",
        "__model__:state2",
        "reference",
        "reference",
    ).exists()


def test_from_model_every_state_reference_has_a_tool_producer():
    model = {
        "E": ["unknown", "exec0"],
        "S": ["boundary", "produced", "isolated"],
        "I": [["exec0", "boundary"]],
        "O": [
            ["unknown", "boundary"],
            ["unknown", "isolated"],
            ["exec0", "produced"],
        ],
    }

    trace = TIDE.from_model(model)

    unknown_record = trace.inspect("unknown", "exec_record").object.object
    assert {
        write.stored_target
        for write in unknown_record.writes
    } == {"__model__:boundary", "__model__:isolated"}

    for reference in trace.index.seen_refs:
        for version in trace.index.versions_by_ref[reference]:
            assert (
                reference,
                version,
            ) in trace.index.producer_exec_ids_by_ref_version

    exported = trace.to_model()
    assert set(exported["E"]) == set(model["E"])
    assert set(exported["S"]) == set(model["S"])
    assert exported["I"] == [
        {"exec_id": "exec0", "state_id": "boundary"},
    ]
    assert _incidence_pairs(exported["O"]) == {
        ("unknown", "boundary"),
        ("unknown", "isolated"),
        ("exec0", "produced"),
    }


def test_from_model_preserves_literal_host_state_ids_through_bundle():
    model = {
        "host": "MLMD",
        "E": ["unknown", "op:run/124132"],
        "S": ["op:124132", "result*final:7"],
        "I": [["op:run/124132", "op:124132"]],
        "O": [
            ["unknown", "op:124132"],
            ["op:run/124132", "result*final:7"],
        ],
        "states": {
            "op:124132": {"value": {"raw": True}},
            "result*final:7": {"value": {"done": True}},
        },
    }

    trace = TIDE.from_model(model)
    restored = TIDE.from_bundle(trace.to_bundle())

    assert trace.value_of("__model__:op:124132", 0) == {"raw": True}
    restored_value = restored.value_of("__model__:op:124132", 0)
    assert isinstance(restored_value, TIDEOpaqueValue)
    assert restored_value.type_name == "TIDEUnknownType"

    for candidate in (trace, restored):
        assert set(candidate.index.seen_refs) == {
            "__model__:op:124132",
            "__model__:result*final:7",
        }
        assert candidate.path(
            "__model__:op:124132",
            "__model__:result*final:7",
            "reference",
            "reference",
        ).exists()

    assert _model_carriers(restored.to_model())[1] == set(model["S"])


def test_from_model_preserves_integer_occurrence_ids():
    model = {
        "E": ["unknown"],
        "S": [1],
        "I": [],
        "O": [["unknown", 1]],
    }

    trace = TIDE.from_model(model)
    assert trace.to_model(enrich=False)["S"] == [1]


def test_model_trace_indexes_every_represented_producer():
    trace = TIDE.from_model({
        "E": ["A", "B"],
        "S": ["shared"],
        "I": [],
        "O": [["A", "shared"], ["B", "shared"]],
    })
    reference = "__model__:shared"

    assert list(trace.index.producer_exec_ids_by_ref_version[
        (reference, 0)
    ]) == ["A", "B"]
    assert [
        exec_id
        for exec_id in trace.produced_by(reference).execs
    ] == ["A", "B"]


def test_trace_model_round_trip_restores_actor_occurrence_identity():
    tide = TIDE()
    tide.event("writer", writes={"value": 1})
    source = tide.trace()
    occurrence = source.log[0].producer_occurrence_id

    restored = TIDE.from_model(source.to_model())

    assert restored.log[0].producer_occurrence_id == occurrence
    assert restored.index.actor_occurrence_to_exec_id[occurrence] == (
        restored.log[0].exec_id
    )


def test_from_model_rejects_duplicate_actor_occurrence_identity():
    model = {
        "E": ["A", "B"],
        "S": [],
        "I": [],
        "O": [],
        "executions": {
            "A": {"actor": {"name": "worker"}, "occurrence": 0},
            "B": {"actor": {"name": "worker"}, "occurrence": 0},
        },
    }

    with pytest.raises(ValueError, match="duplicate ActorOccurrence"):
        TIDE.from_model(model)


def test_value_queries_return_detached_historical_values():
    tide = TIDE()
    tide.declare("value", {"items": []})
    trace = tide.trace()
    bundle = trace.to_bundle()

    trace_value = trace.value_of("global:value", 0)
    trace_value["items"].append("changed")
    bundle_value = bundle.value_at("global:value", 0)
    bundle_value["items"].append("changed")

    assert trace.value_of("global:value", 0) == {"items": []}
    assert bundle.value_at("global:value", 0) == {"items": []}


def test_from_model_hashes_supplied_values_and_generated_opaque_values():
    raw_value = {"rows": [1, 2, 3]}
    result_hash = "sha3_512_tide:" + ("a" * 128)
    model = {
        "schema": "TIDE-Model",
        "schema_version": 1,
        "host": "OPM",
        "E": ["E1", "E2"],
        "S": ["raw-state", "7"],
        "I": [{"exec_id": "E2", "state_id": "raw-state"}],
        "O": [
            {"exec_id": "E1", "state_id": "raw-state"},
            {"exec_id": "E2", "state_id": "7"},
        ],
        "executions": {
            "E1": {
                "actor": {
                    "name": "load",
                    "runner": "external-import",
                    "ocean": "model-ocean",
                },
                "start": 10,
                "end": 11,
                "duration": 1000,
            },
            "E2": {"actor": {"name": "process"}},
        },
        "states": {
            "raw-state": {
                "value": raw_value,
            },
            "7": {
                "value_hash": result_hash,
            },
        },
    }

    trace = TIDE.from_model(model)
    load, process = trace.log

    assert load.producer_id == Actor(
        "model-ocean",
        "external-import",
        "load",
    )
    assert load.started_at == 10
    assert load.finished_at == 11
    assert load.duration_ms == 1000
    assert process.deps[0].producer_exec_id == "E1"
    assert trace.value_of("__model__:raw-state", 0) == raw_value
    result = trace.value_of("__model__:7", 0)
    assert isinstance(result, TIDEOpaqueValue)
    assert result.value_hash != result_hash
    assert result.value_hash.startswith("sha3_512_tide:")

    exported = trace.to_model(enrich=True)
    assert exported["host"] == "OPM"
    assert _model_carriers(exported)[1] == {"raw-state", "7"}
    assert exported["states"]["raw-state"]["value"] == raw_value
    assert exported["executions"]["E1"]["actor"]["name"] == "load"


def test_from_model_value_type_can_describe_unavailable_raw_value():
    recorded_hash = "sha3_512_tide:" + ("b" * 128)
    trace = TIDE.from_model({
        "E": ["producer"],
        "S": ["state"],
        "I": [],
        "O": [["producer", "state"]],
        "states": {
            "state": {
                "value_hash": recorded_hash,
                "value_type": int,
            },
        },
    })

    value = trace.value_of("__model__:state", 0)
    assert isinstance(value, TIDEOpaqueValue)
    assert value.value_hash != recorded_hash
    assert value.value_hash.startswith("sha3_512_tide:")
    assert value.type_name == "int"
    assert trace.divergent is False
    shown = trace.show(color=False, values=True)
    assert "wrote: int opaque<int>" in shown
    assert "opaque<int> opaque<int>" not in shown
    investigation = trace.investigate("__model__:state").to_dict()
    assert investigation["history"][0]["value_type"] == "int"


def test_from_model_available_value_preserves_recorded_value_type_live():
    raw_value = {"rows": [1, 2, 3]}
    supplied_hash = "sha3_512_tide:" + ("c" * 128)
    trace = TIDE.from_model({
        "E": ["producer"],
        "S": ["state"],
        "I": [],
        "O": [["producer", "state"]],
        "states": {
            "state": {
                "value": raw_value,
                "value_type": "RecordedThing",
                "value_hash": supplied_hash,
            },
        },
    })

    assert trace.value_of("__model__:state", 0) == raw_value
    generated_hash = trace.index.value_hash_by_ref_version[
        ("__model__:state", 0)
    ]
    assert generated_hash != supplied_hash
    assert trace.index.value_by_hash[generated_hash] == raw_value
    assert trace.divergent is False
    assert "wrote: RecordedThing as __model__:state[v0]" in trace.show(
        color=False,
    )
    assert trace.to_model()["states"]["state"]["value_type"] == (
        "RecordedThing"
    )


def test_from_model_state_enrichment_cannot_redefine_state_identity():
    model = {
        "E": ["one", "two"],
        "S": ["state-one", "state-two"],
        "I": [],
        "O": [["one", "state-one"], ["two", "state-two"]],
        "states": {
            "state-one": {"reference": "same:value", "version": 7},
            "state-two": {"reference": "same:value", "version": 7},
        },
    }

    trace = TIDE.from_model(model)

    assert {write.stored_target for write in trace.writes} == {
        "__model__:state-one",
        "__model__:state-two",
    }
    assert {write.stored_version for write in trace.writes} == {0}
    exported = trace.to_model()
    assert set(exported["S"]) == {"state-one", "state-two"}
    assert all(
        "reference" not in enrichment and "version" not in enrichment
        for enrichment in exported["states"].values()
    )


def test_minimal_model_round_trip_does_not_fabricate_enrichment():
    model = {
        "E": ["produce", "consume"],
        "S": ["state"],
        "I": [["consume", "state"]],
        "O": [["produce", "state"]],
    }

    exported = TIDE.from_model(model).to_model(
        enrich=True,
    )

    assert set(exported["E"]) == set(model["E"])
    assert set(exported["S"]) == set(model["S"])
    assert "executions" not in exported
    assert "states" not in exported


@pytest.mark.parametrize(
    ("section", "identifier", "message"),
    [
        ("executions", "missing-exec", "unknown exec_id"),
        ("states", "missing-state", "unknown state_id"),
    ],
)
def test_model_validity_ignores_enrichment_outside_the_carriers(
    section,
    identifier,
    message,
):
    model = {
        "E": ["exec"],
        "S": ["state"],
        "I": [],
        "O": [["exec", "state"]],
        section: {identifier: {}},
    }

    report = TIDE.report_model_validity(model)

    assert report["semantically_valid"] is True
    assert report["errors"] == []
    assert TIDE.from_model(model).to_model()[section] == {
        identifier: {},
    }


def test_model_ids_survive_bundle_round_trip_without_exposing_trace_refs():
    model = {
        "host": "MLMD",
        "E": ["1", "2"],
        "S": ["artifact-A", "99"],
        "I": [["2", "artifact-A"]],
        "O": [["1", "artifact-A"], ["2", "99"]],
    }

    restored = TIDE.from_bundle(TIDE.from_model(model).to_bundle())
    exported = restored.to_model()
    imported_records = [
        record
        for record in restored.log
        if record.node_options.get("source") == "tide_model"
    ]

    assert exported["host"] == "MLMD"
    assert _model_carriers(exported)[0] == {"1", "2"}
    assert [record.exec_id for record in imported_records] == ["1", "2"]
    assert _model_carriers(exported)[1] == {"artifact-A", "99"}
    assert all(
        "__model__" not in repr(section)
        for section in (
            exported["I"],
            exported["O"],
        )
    )


def test_validate_model_reports_semantic_validity_and_judgments():
    model = {
        "E": ["produce", "consume"],
        "S": ["state"],
        "I": [["consume", "state"]],
        "O": [["produce", "state"]],
    }

    report = TIDE.report_model_validity(model, host="OPM")

    assert report["semantically_valid"] is True
    assert report["host"] == "OPM"
    assert report["structurally_valid"] is True
    assert report["unique_creation"] is True
    assert report["acyclic_ancestry"] is True
    assert report["counts"]["direct_handoffs"] == 1


@pytest.mark.parametrize(
    ("case", "model"),
    [
        (
            "simple chain",
            {
                "E": ["e1", "e2", "e3"],
                "S": ["s1", "s2"],
                "I": [["e2", "s1"], ["e3", "s2"]],
                "O": [["e1", "s1"], ["e2", "s2"]],
            },
        ),
        (
            "branching consumers",
            {
                "E": ["producer", "left", "right"],
                "S": ["shared"],
                "I": [["left", "shared"], ["right", "shared"]],
                "O": [["producer", "shared"]],
            },
        ),
        (
            "boundary State with unknown represented producer",
            {
                "E": ["unknown", "consumer"],
                "S": ["boundary"],
                "I": [["consumer", "boundary"]],
                "O": [["unknown", "boundary"]],
            },
        ),
        (
            "terminal State without consumer",
            {
                "E": ["producer"],
                "S": ["terminal"],
                "I": [],
                "O": [["producer", "terminal"]],
            },
        ),
        (
            "multiple outputs from one Execution",
            {
                "E": ["producer"],
                "S": ["left", "right"],
                "I": [],
                "O": [
                    ["producer", "left"],
                    ["producer", "right"],
                ],
            },
        ),
    ],
)
def test_tide_model_explicit_validity_cases_pass(case, model):
    report = TIDE.report_model_validity(model)

    assert report["semantically_valid"] is True, case
    assert report["errors"] == []
    assert report["structurally_valid"] is True
    assert report["unique_creation"] is True
    assert report["acyclic_ancestry"] is True
    assert isinstance(TIDE.from_model(model), Trace)


@pytest.mark.parametrize(
    ("case", "model", "message"),
    [
        (
            "State without a represented producer",
            {
                "E": ["consumer"],
                "S": ["boundary"],
                "I": [["consumer", "boundary"]],
                "O": [],
            },
            "unique_creation",
        ),
        (
            "two distinct creators for one State",
            {
                "E": ["left", "right"],
                "S": ["shared"],
                "I": [],
                "O": [["left", "shared"], ["right", "shared"]],
            },
            "unique_creation",
        ),
        (
            "direct self-handoff",
            {
                "E": ["self"],
                "S": ["loop"],
                "I": [["self", "loop"]],
                "O": [["self", "loop"]],
            },
            "acyclic_ancestry",
        ),
        (
            "multi-Execution cycle",
            {
                "E": ["e1", "e2", "e3"],
                "S": ["s12", "s23", "s31"],
                "I": [
                    ["e2", "s12"],
                    ["e3", "s23"],
                    ["e1", "s31"],
                ],
                "O": [
                    ["e1", "s12"],
                    ["e2", "s23"],
                    ["e3", "s31"],
                ],
            },
            "acyclic_ancestry",
        ),
        (
            "read-modify-write with one State identity",
            {
                "E": ["update"],
                "S": ["same-state"],
                "I": [["update", "same-state"]],
                "O": [["update", "same-state"]],
            },
            "acyclic_ancestry",
        ),
    ],
)
def test_tide_model_explicit_invalidity_cases_fail(
    case,
    model,
    message,
):
    report = TIDE.report_model_validity(model)

    assert report["semantically_valid"] is False, case
    assert any(message in error for error in report["errors"])
    trace = TIDE.from_model(model)
    assert trace.to_model(enrich=False) == TIDEModel.normalise(model)


def test_tide_model_exact_judgments_match_hard_coded_oracle():
    model = {
        "E": ["e1", "e2", "e3"],
        "S": ["s1", "s2"],
        "I": [["e2", "s1"], ["e3", "s2"]],
        "O": [["e1", "s1"], ["e2", "s2"]],
    }
    expected_direct = {
        ("e1", "s1", "e2"),
        ("e2", "s2", "e3"),
    }
    expected_reachability = {
        ("e1", "e2"),
        ("e2", "e3"),
        ("e1", "e3"),
    }

    report = TIDE.report_model_validity(model)

    assert set(report.direct_handoffs) == expected_direct
    assert set(report.reachability) == expected_reachability
    assert report["counts"]["direct_handoffs"] == len(expected_direct)
    assert report["counts"]["reachability"] == len(
        expected_reachability
    )


def test_tide_model_report_show_uses_tide_style(capsys):
    model = {
        "E": ["e1", "e2", "e3"],
        "S": ["s1", "s2"],
        "I": [["e2", "s1"], ["e3", "s2"]],
        "O": [["e1", "s1"], ["e2", "s2"]],
    }
    report = TIDE.report_model_validity(model, host="OPM")

    rendered = report.show(color=False)
    captured = capsys.readouterr().out

    assert isinstance(report, TIDEModelValidityReport)
    assert rendered in captured
    assert "TIDE MODEL | VALID | 3 executions | 2 states" in rendered
    assert "Host: OPM" in rendered
    assert '"e1" --"s1"--> "e2"' in rendered
    assert '"e1" \u2192 "e3"' in rendered
    assert "ERRORS:\n  none" in rendered
    assert "\033[" not in rendered


def test_tide_model_invalid_report_show_explains_failure(capsys):
    report = TIDE.report_model_validity({
        "E": ["left", "right"],
        "S": ["shared"],
        "I": [],
        "O": [["left", "shared"], ["right", "shared"]],
    })

    rendered = report.show(color=False)
    capsys.readouterr()

    assert "TIDE MODEL | INVALID" in rendered
    assert "unique_creation:" in rendered
    assert "not established" in rendered


def test_tide_model_and_bundle_round_trips_preserve_kernel_and_judgments():
    model = {
        "schema": "TIDE-Model",
        "schema_version": 1,
        "host": "OPM",
        "E": ["e1", "e2", "e3"],
        "S": ["s1", "s2", "terminal"],
        "I": [["e2", "s1"], ["e3", "s2"]],
        "O": [
            ["e1", "s1"],
            ["e2", "s2"],
            ["e3", "terminal"],
        ],
    }
    expected_kernel = {
        "I": {tuple(edge) for edge in model["I"]},
        "O": {tuple(edge) for edge in model["O"]},
    }
    expected_direct = {
        ("e1", "s1", "e2"),
        ("e2", "s2", "e3"),
    }
    expected_reachability = {
        ("e1", "e2"),
        ("e2", "e3"),
        ("e1", "e3"),
    }

    imported = TIDE.from_model(model)
    model_round_trip = imported.to_model()
    restored = TIDE.from_bundle(imported.to_bundle())
    bundle_round_trip = restored.to_model()

    for candidate in (model_round_trip, bundle_round_trip):
        candidate_kernel = {
            "I": {
                (edge["exec_id"], edge["state_id"])
                for edge in candidate["I"]
            },
            "O": {
                (edge["exec_id"], edge["state_id"])
                for edge in candidate["O"]
            },
        }
        report = TIDE.report_model_validity(candidate)

        assert candidate_kernel == expected_kernel
        assert set(report.direct_handoffs) == expected_direct
        assert set(report.reachability) == expected_reachability


def test_tide_and_trace_to_model_export_native_execution_evidence():
    tide = TIDE()
    tide.event("writer", writes={"payload": {"ok": True}})
    tide.event("reader", reads=["payload"], writes={"result": 1})

    from_instance = tide.to_model(enrich=True)
    from_trace = tide.trace().to_model(enrich=True)

    assert from_instance == from_trace
    assert TIDE.report_model_validity(from_instance)["semantically_valid"] is True
    assert _model_carriers(from_instance)[1] == {
        "global:payload[v0]",
        "global:result[v0]",
    }
    assert from_instance["states"]["global:payload[v0]"]["value"] == {
        "ok": True,
    }
    assert from_instance["I"][0]["state_id"] == "global:payload[v0]"
    assert any(
        edge["state_id"] == "global:payload[v0]"
        for edge in from_instance["O"]
    )


def test_to_model_retains_recorded_external_observation_producers():
    tide = TIDE()
    tide.event(
        "consume",
        reads={"left": 1, "right": 2},
        writes={"result": 3},
    )

    exported = tide.to_model(enrich=False)
    report = TIDE.report_model_validity(exported)

    produced_states = {
        edge["state_id"]
        for edge in exported["O"]
    }
    consumed_states = {
        edge["state_id"]
        for edge in exported["I"]
    }
    assert consumed_states <= produced_states
    assert report["semantically_valid"] is True
    assert report["unique_creation"] is True


def test_model_import_retains_state_without_represented_producer():
    model = {
        "E": ["consumer"],
        "S": ["boundary"],
        "I": [["consumer", "boundary"]],
        "O": [],
    }

    report = TIDE.report_model_validity(model)

    assert report["semantically_valid"] is False
    assert report["unique_creation"] is False
    assert any(
        "has no represented producer" in error
        for error in report["errors"]
    )
    assert TIDE.from_model(model).to_model(enrich=False) == TIDEModel.normalise(model)


def test_declared_execution_without_incidence_remains_represented():
    model = {"E": ["idle"], "S": [], "I": [], "O": []}

    report = TIDE.report_model_validity(model)
    trace = TIDE.from_model(model)
    exported = trace.to_model(enrich=False)

    assert report["structurally_valid"] is True
    assert report["semantically_valid"] is True
    assert report["counts"]["executions"] == 1
    assert [record.exec_id for record in trace.log] == ["idle"]
    assert exported["E"] == ["idle"]
    assert exported["I"] == exported["O"] == []


def test_semantic_invalidity_does_not_erase_determined_judgments():
    model = {
        "E": ["a", "b"],
        "S": ["a-to-b", "b-to-a"],
        "I": [["a", "b-to-a"], ["b", "a-to-b"]],
        "O": [["a", "a-to-b"], ["b", "b-to-a"]],
    }

    report = TIDE.report_model_validity(model)

    assert report["structurally_valid"] is True
    assert report["semantically_valid"] is False
    assert set(report.direct_handoffs) == {
        ("a", "a-to-b", "b"),
        ("b", "b-to-a", "a"),
    }
    assert set(report.reachability) == {
        ("a", "a"), ("a", "b"), ("b", "a"), ("b", "b"),
    }


def test_from_model_external_producer_is_explicit_and_not_host_evidence():
    model = {
        "E": ["consumer"],
        "S": ["boundary"],
        "I": [["consumer", "boundary"]],
        "O": [],
    }
    original = copy.deepcopy(model)

    trace = TIDE.from_model(model)

    assert model == original

    assert [record.exec_id for record in trace.log] == [
        "TIDE: External",
        "consumer",
    ]
    assert trace.model_context["external_producer"] == {
        "exec_id": "TIDE: External",
        "state_ids": ["boundary"],
    }

    exported = trace.to_model(enrich=False)
    assert exported["O"] == []
    assert TIDE.report_model_validity(model)["semantically_valid"] is False
    assert TIDE.report_model_validity(exported)["semantically_valid"] is False

    restored = TIDE.from_bundle(trace.to_bundle())
    assert restored.model_context["external_producer"] == {
        "exec_id": "TIDE: External",
        "state_ids": ["boundary"],
    }
    assert restored.to_model(enrich=False) == exported


def test_model_carriers_and_relations_use_set_semantics():
    model = {
        "E": ["produce", "consume", "produce"],
        "S": ["shared", "shared"],
        "I": [
            ["consume", "shared"],
            {"e": "consume", "s": "shared"},
        ],
        "O": [
            ["produce", "shared"],
            {"exec_id": "produce", "state_id": "shared"},
        ],
    }

    report = TIDE.report_model_validity(model)
    exported = TIDE.from_model(model).to_model(enrich=False)

    assert report["semantically_valid"] is True
    assert report["counts"]["inputs"] == 1
    assert report["counts"]["outputs"] == 1
    assert exported["I"] == [{
        "exec_id": "consume",
        "state_id": "shared",
    }]
    assert exported["O"] == [{
        "exec_id": "produce",
        "state_id": "shared",
    }]
    assert set(exported["E"]) == {"produce", "consume"}
    assert exported["S"] == ["shared"]


@pytest.mark.parametrize(
    ("model", "message"),
    [
        (
            {"E": [], "S": [], "I": []},
            "missing O",
        ),
        (
            {
                "E": ["consume"],
                "S": ["state"],
                "I": [["missing", "state"]],
                "O": [],
            },
            "references Execution",
        ),
        (
            {
                "E": ["a", "b"],
                "S": ["state"],
                "I": [],
                "O": [["a", "state"], ["b", "state"]],
            },
            "unique_creation",
        ),
        (
            {
                "E": ["a", "b"],
                "S": ["a-to-b", "b-to-a"],
                "I": [["a", "b-to-a"], ["b", "a-to-b"]],
                "O": [["a", "a-to-b"], ["b", "b-to-a"]],
            },
            "acyclic_ancestry",
        ),
    ],
)
def test_from_model_rejects_only_structurally_invalid_documents(model, message):
    report = TIDE.report_model_validity(model)

    if message in {"missing O", "references Execution"}:
        assert report["semantically_valid"] is None
        assert any(message in error for error in report["errors"])
        with pytest.raises(ValueError, match=message):
            TIDE.from_model(model)
        return

    trace = TIDE.from_model(model)
    assert trace.to_model() == TIDEModel.normalise(model)
    assert report["semantically_valid"] is False
    assert any(message in error for error in report["errors"])
