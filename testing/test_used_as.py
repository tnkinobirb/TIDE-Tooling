import pytest

from tide_tooling import DataRef, TIDE, UsedAs
from tide_tooling.tooling_core.engine.events import Cause
from tide_tooling.tooling_core.engine.tide_logging import (
    DependencyUseRecord,
    ExecRecord,
    ExpandedDepRecord,
    InputUseRecord,
    Inputs,
    Trace,
    WriteRecord,
)
from tide_tooling.tooling_core.engine.utils import tide_snapshot_value


def make_record(seq_id, exec_id, actor, *, deps=(), writes=(), inputs=None):
    return ExecRecord(
        seq_id=seq_id,
        exec_id=exec_id,
        ocean_id="ocean",
        runner_id="run",
        pipeline_id="pipeline",
        node_id=actor,
        func=None,
        func_name=actor,
        inputs=inputs or Inputs({}),
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
        started_at=seq_id,
        finished_at=seq_id + 1,
        duration_ms=1,
        error=None,
        node_options={"preserve_exec_id": True},
        writes=list(writes),
        emitted_nodes=[],
    )


def make_read(
    input_name,
    input_index,
    value,
    writer,
    *,
    input_path=None,
    lineage=(),
    mutable=False,
):
    input_record = InputUseRecord(input_name, None, None, None, None, [])
    read = DependencyUseRecord(
        (input_name, input_record),
        None,
        "static",
        "global:value",
        value,
        0,
        writer.runner_id,
        writer.producer_id,
        writer.exec_id,
        input_index=input_index,
        input_path=input_path,
        snapshot_lineage=list(lineage),
        observed_mutable=mutable,
    )
    input_record.dependencies = [read]
    return input_record, read


def make_used_as_trace():
    stored = {"items": [1]}
    writer = make_record(
        0,
        "writer",
        "writer",
        writes=[WriteRecord(
            Cause(description="write"),
            stored,
            0,
            "global:value",
        )],
    )

    args, first = make_read("args", 0, stored, writer, input_path=(0,))
    _, second = make_read("args", 1, stored, writer, input_path=())
    args.dependencies.append(second)
    static = make_record(
        1,
        "static",
        "static",
        deps=[first, second],
        inputs=Inputs({"args": args}),
    )

    dynamic_input, expanded = make_read("kwargs", None, stored, writer)
    expanded = ExpandedDepRecord(
        expanded.associated_input,
        expanded.dep,
        expanded.type,
        expanded.unscoped_reference,
        expanded.resolved_value,
        expanded.resolved_version,
        expanded.producer_source,
        expanded.producer_actor,
        expanded.producer_exec_id,
        DataRef("@value*", "global"),
    )
    dynamic = DependencyUseRecord(
        ("kwargs", dynamic_input),
        None,
        "dynamic",
        "global:*",
        expanded_to=[expanded],
    )
    dynamic_input.dependencies = [dynamic]
    dynamic_exec = make_record(
        2,
        "dynamic",
        "dynamic",
        deps=[dynamic],
        inputs=Inputs({"kwargs": dynamic_input}),
    )

    runtime_input, runtime_read = make_read(
        "runtime_read",
        None,
        stored,
        writer,
    )
    runtime_inputs = Inputs({})
    runtime_inputs.runtime_read = runtime_input
    runtime_exec = make_record(
        3,
        "runtime",
        "runtime",
        deps=[runtime_read],
        inputs=runtime_inputs,
    )
    return Trace(
        log=[writer, static, dynamic_exec, runtime_exec],
        ocean_id="ocean",
    )


def test_used_as_returns_every_exact_use_without_collapsing_one_execution():
    used = make_used_as_trace().used_as("global:value[v0]")

    assert isinstance(used, UsedAs)
    assert len(used) == 4
    assert [entry["seq_id"] for entry in used.uses] == [1, 1, 2, 3]
    assert [entry["read_type"] for entry in used.uses] == [
        "static",
        "static",
        "dynamic",
        "runtime_read",
    ]
    assert len(used.by_seq_id[1]) == 2
    assert used.to_dict()["state"] == {
        "reference": "global:value",
        "version": 0,
        "id": "global:value[v0]",
    }


def test_used_as_seq_id_filters_without_changing_result_shape():
    used = make_used_as_trace().used_as(
        "global:value",
        version=0,
        seq_id=1,
    )

    assert len(used) == 2
    assert [entry["seq_id"] for entry in used.to_dict()["uses"]] == [1, 1]


def test_used_as_reports_mutation_against_stored_authority():
    stored = {"items": [1]}
    observed = {"items": [1, 2]}
    writer = make_record(
        0,
        "writer",
        "writer",
        writes=[WriteRecord(
            Cause(description="write"),
            stored,
            0,
            "global:value",
        )],
    )
    lineage = [
        tide_snapshot_value(stored, boundary="stored").to_dict(),
        tide_snapshot_value(observed, boundary="used").to_dict(),
    ]
    input_record, read = make_read(
        "args",
        0,
        observed,
        writer,
        lineage=lineage,
        mutable=True,
    )
    reader = make_record(
        1,
        "reader",
        "reader",
        deps=[read],
        inputs=Inputs({"args": input_record}),
    )
    used = Trace(log=[writer, reader], ocean_id="ocean").used_as(
        "global:value[v0]"
    )

    assert used.uses[0]["observed"]["differs_from_stored"] is True
    assert used.uses[0]["observed_mutable"] is True
    assert "list *" not in used.show(color=False)
    assert "dict *" in used.show(color=False)
    assert "changed from stored: yes" in used.show(color=False)


def test_used_as_values_are_detached_and_model_bundle_keeps_state_value():
    trace = make_used_as_trace()
    used = trace.used_as("global:value[v0]")
    values = used.values
    values[0]["items"].append("caller mutation")

    assert trace.used_as("global:value[v0]").values[0] == {"items": [1]}
    restored = TIDE.from_bundle(trace.to_bundle())
    assert restored.value_of("global:value[v0]") == {"items": [1]}


def test_used_as_requires_an_exact_state_and_known_sequence():
    trace = make_used_as_trace()

    with pytest.raises(ValueError, match="exact State version"):
        trace.used_as("global:value")
    with pytest.raises(KeyError, match="Unknown TIDE State"):
        trace.used_as("global:missing[v0]")
    with pytest.raises(KeyError, match="seq_id"):
        trace.used_as("global:value[v0]", seq_id=99)
