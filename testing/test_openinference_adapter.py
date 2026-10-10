import pytest

from tide_tooling import OpenInferenceAdapter, TIDE


def _span(
    span_id,
    name,
    *,
    trace_id="trace-1",
    parent=None,
    input_value=...,
    output_value=...,
    input_mime=None,
    output_mime=None,
    kind="CHAIN",
):
    attributes = {"openinference.span.kind": kind}
    if input_value is not ...:
        attributes["input.value"] = input_value
    if output_value is not ...:
        attributes["output.value"] = output_value
    if input_mime is not None:
        attributes["input.mime_type"] = input_mime
    if output_mime is not None:
        attributes["output.mime_type"] = output_mime
    return {
        "traceId": trace_id,
        "spanId": span_id,
        "parentSpanId": parent,
        "name": name,
        "attributes": attributes,
        "status": {"code": "OK"},
    }


def test_openinference_projects_span_ancestry_and_only_explicit_local_io():
    trace = TIDE.from_openinference([
        _span(
            "agent",
            "researcher",
            input_value='{"question":"why"}',
            input_mime="application/json",
        ),
        _span(
            "tool",
            "search",
            parent="agent",
            output_value='{"result":"because"}',
            output_mime="application/json",
            kind="TOOL",
        ),
    ])
    external = next(record for record in trace.log if record.func_name == "TIDE: External")
    parent = next(record for record in trace.log if record.func_name == "researcher")
    child = next(record for record in trace.log if record.func_name == "search")

    assert child.emitted_by_exec_id == parent.exec_id
    assert [record.exec_id for record in trace.children_of(parent.exec_id)] == [
        child.exec_id
    ]
    assert [record.exec_id for record in trace.parents_of(child.exec_id)] == [
        parent.exec_id
    ]

    assert len(trace.reads) == 1
    read = trace.reads[0]
    assert read.resolved_value.type_name == "TIDEUnknownType"
    assert read.resolved_value.representation == '{"question":"why"}'
    assert "application/json" in read.resolved_value.reason
    assert read.producer_exec_id == external.exec_id
    assert read.resolved_version == 0
    assert read.unscoped_reference == "openinference:trace-1/agent/input"

    write = child.writes[0]
    assert write.stored_result.type_name == "TIDEUnknownType"
    assert write.stored_result.representation == '{"result":"because"}'
    assert "application/json" in write.stored_result.reason
    assert write.stored_version == 0
    assert write.stored_target == "openinference:trace-1/tool/output"
    assert child.output_value.type_name == "TIDEUnknownType"
    assert child.output_value.representation == '{"result":"because"}'
    assert child.node_options["span_kind"] == "TOOL"
    shown = trace.show(color=False)
    assert "opaque<TIDEUnknownType>" in shown
    assert "wrote: str" not in shown


def test_openinference_absent_io_creates_no_state_and_invalid_carriers_reject():
    empty = TIDE.from_openinference([_span("plain", "plain span")])
    assert empty.reads == ()
    assert empty.writes == ()
    assert empty.to_model(enrich=False)["S"] == []

    with pytest.raises(ValueError, match="input.value must be a string"):
        TIDE.from_openinference([_span("none", "invalid", input_value=None)])
    with pytest.raises(ValueError, match="output.mime_type"):
        span = _span("mime", "invalid mime", output_value="value")
        span["attributes"]["output.mime_type"] = 1
        TIDE.from_openinference([span])


def test_openinference_does_not_infer_cross_span_value_flow():
    trace = TIDE.from_openinference([
        _span("parent", "parent", output_value="same"),
        _span("child", "child", parent="parent", input_value="same"),
    ])
    external = next(record for record in trace.log if record.func_name == "TIDE: External")
    parent = next(record for record in trace.log if record.func_name == "parent")
    child = next(record for record in trace.log if record.func_name == "child")
    read = trace.reads[0]

    assert child.emitted_by_exec_id == parent.exec_id
    assert read.producer_exec_id == external.exec_id
    assert read.resolved_value.reason == (
        "OpenInference recorded a serialised value"
    )
    assert any(
        write.stored_target != read.unscoped_reference
        for write in parent.writes
    )


def test_openinference_requires_span_kind_and_rejects_duplicate_identity():
    missing_kind = _span("one", "one")
    del missing_kind["attributes"]["openinference.span.kind"]
    with pytest.raises(ValueError, match="requires openinference.span.kind"):
        OpenInferenceAdapter([missing_kind]).to_trace()

    with pytest.raises(ValueError, match="appears more than once"):
        OpenInferenceAdapter([
            _span("same", "first"),
            _span("same", "second"),
        ])


def test_openinference_parent_identity_is_trace_scoped_and_order_independent():
    trace = TIDE.from_openinference([
        _span("child", "child", trace_id="a", parent="parent"),
        _span("parent", "parent", trace_id="a"),
        _span("same", "same a", trace_id="a"),
        _span("same", "same b", trace_id="b"),
        _span("orphan", "orphan", trace_id="b", parent="parent"),
    ])
    records = {record.func_name: record for record in trace.log}

    assert records["child"].emitted_by_exec_id == records["parent"].exec_id
    assert records["orphan"].emitted_by_exec_id is None
    assert records["same a"].exec_id != records["same b"].exec_id


def test_openinference_otlp_source_lookup_and_bundle_round_trip():
    source = {
        "resourceSpans": [{
            "resource": {
                "attributes": [{
                    "key": "service.name",
                    "value": {"stringValue": "assistant-service"},
                }],
            },
            "scopeSpans": [{
                "scope": {"name": "openinference.instrumentation"},
                "spans": [{
                    "traceId": "trace-1",
                    "spanId": "llm",
                    "name": "chat model",
                    "attributes": [
                        {
                            "key": "openinference.span.kind",
                            "value": {"stringValue": "LLM"},
                        },
                        {
                            "key": "input.value",
                            "value": {"stringValue": "hello"},
                        },
                        {
                            "key": "input.mime_type",
                            "value": {"stringValue": "text/plain"},
                        },
                        {
                            "key": "output.value",
                            "value": {"stringValue": "hi"},
                        },
                        {
                            "key": "output.mime_type",
                            "value": {"stringValue": "text/plain"},
                        },
                    ],
                }],
            }],
        }],
    }

    trace = OpenInferenceAdapter(source).to_trace()
    record = next(
        record
        for record in trace.log
        if record.func_name != "TIDE: External"
    )
    source_entry = trace.host_record[record.exec_id]
    assert source_entry["span"]["spanId"] == "llm"
    assert source_entry["resource"]["attributes"][0]["key"] == "service.name"
    assert source_entry["scope"]["name"] == "openinference.instrumentation"
    assert record.producer_id.source_runner == "assistant-service"

    restored = TIDE.from_bundle(trace.to_bundle())
    restored_record = next(
        record
        for record in restored.log
        if record.func_name != "TIDE: External"
    )
    assert restored.host_record[restored_record.exec_id] == source_entry
    assert restored.reads[0].resolved_value.type_name == "TIDEUnknownType"
    assert restored.reads[0].resolved_value.representation == "hello"
    assert "text/plain" in restored.reads[0].resolved_value.reason
    restored_output = restored_record.writes[0]
    assert restored_output.stored_result.type_name == "TIDEUnknownType"
    assert restored_output.stored_result.representation == "hi"
    assert "text/plain" in restored_output.stored_result.reason
    assert restored_record.node_options["span_kind"] == "LLM"
