from tide_tooling import OpenTelemetryAdapter, TIDE


def _span(span_id, name, *, trace_id="trace-1", parent=None, status="OK"):
    return {
        "traceId": trace_id,
        "spanId": span_id,
        "parentSpanId": parent,
        "name": name,
        "kind": "SPAN_KIND_INTERNAL",
        "startTimeUnixNano": f"{span_id}000",
        "endTimeUnixNano": f"{span_id}999",
        "status": {"code": status},
        "attributes": [{"key": "ignored.value", "value": {"stringValue": "source only"}}],
    }


def _otlp(spans):
    return {
        "resourceSpans": [{
            "resource": {
                "attributes": [{
                    "key": "service.name",
                    "value": {"stringValue": "orders-service"},
                }],
            },
            "scopeSpans": [{
                "scope": {"name": "orders.instrumentation"},
                "spans": spans,
            }],
        }],
    }


def test_opentelemetry_projects_executions_and_explicit_parent_ancestry_only():
    source = _otlp([
        _span("01", "HTTP GET /orders"),
        _span("02", "SELECT orders", parent="01"),
    ])

    trace = TIDE.from_opentelemetry(source)
    root, child = trace.log

    assert len(trace.log) == 2
    assert trace.writes == ()
    assert trace.reads == ()
    assert child.emitted_by_exec_id == root.exec_id
    assert [record.exec_id for record in trace.children_of(root.exec_id)] == [child.exec_id]
    assert [record.exec_id for record in trace.parents_of(child.exec_id)] == [root.exec_id]
    assert trace.path(
        root.exec_id,
        child.exec_id,
        "exec_record",
        "exec_record",
        group=False,
    ).to_dict()["paths"]
    assert trace.upstream(
        child.exec_id,
        "exec_record",
        grouped=False,
    ).to_dict()["paths"]
    assert trace.downstream(
        root.exec_id,
        "exec_record",
        grouped=False,
    ).to_dict()["paths"]
    assert root.producer_id.source_runner == "orders-service"
    assert root.producer_id.source_actor == "HTTP GET /orders"
    assert root.status == "succeeded"


def test_opentelemetry_host_record_is_directly_keyed_by_literal_exec_id():
    source = _otlp([_span("01", "HTTP GET /orders")])
    trace = OpenTelemetryAdapter(source).to_trace()
    record = trace.log[0]

    source_entry = trace.host_record[record.exec_id]
    assert source_entry["span"]["spanId"] == "01"
    assert source_entry["resource"]["attributes"][0]["key"] == "service.name"
    assert source_entry["scope"]["name"] == "orders.instrumentation"

    restored = TIDE.from_bundle(trace.to_bundle())
    restored_record = restored.log[0]
    assert restored.host_record[restored_record.exec_id] == source_entry


def test_opentelemetry_missing_parent_does_not_invent_relationship_or_state():
    trace = TIDE.from_opentelemetry(
        _otlp([_span("02", "SELECT orders", parent="not-exported")])
    )
    record = trace.log[0]

    assert record.emitted_by_exec_id is None
    assert trace.parents_of(record.exec_id).to_dict()["executions"] == []
    assert trace.to_model(enrich=False)["S"] == []


def test_opentelemetry_duplicate_span_identity_is_rejected():
    source = _otlp([
        _span("01", "first"),
        _span("01", "second"),
    ])

    try:
        OpenTelemetryAdapter(source)
    except ValueError as error:
        assert "appears more than once" in str(error)
    else:
        raise AssertionError("duplicate OpenTelemetry Span identity was accepted")


def test_opentelemetry_parent_identity_is_trace_scoped_and_order_independent():
    trace = TIDE.from_opentelemetry(_otlp([
        _span("child", "child", trace_id="a", parent="parent"),
        _span("parent", "parent", trace_id="a"),
        _span("same", "same a", trace_id="a"),
        _span("same", "same b", trace_id="b"),
        _span("orphan", "orphan", trace_id="b", parent="parent"),
    ]))
    records = {record.func_name: record for record in trace.log}

    assert records["child"].emitted_by_exec_id == records["parent"].exec_id
    assert records["orphan"].emitted_by_exec_id is None
    assert records["same a"].exec_id != records["same b"].exec_id


def test_emission_path_and_directional_streams_describe_the_same_route():
    trace = TIDE.from_opentelemetry(_otlp([
        _span("01", "request"),
        _span("02", "query", parent="01"),
        _span("03", "format", parent="02"),
    ]))
    root, middle, leaf = trace.log

    path_query = trace.path(
        root.exec_id,
        leaf.exec_id,
        "exec_record",
        "exec_record",
        group=False,
    )
    downstream_query = trace.downstream(
        root.exec_id,
        "exec_record",
        grouped=False,
    )
    upstream_query = trace.upstream(
        leaf.exec_id,
        "exec_record",
        grouped=False,
    )

    path = path_query.to_dict()["paths"][0]
    downstream = downstream_query.to_dict()["paths"][0]
    upstream = upstream_query.to_dict()["paths"][0]

    expected = [root.exec_id, middle.exec_id, leaf.exec_id]
    assert path["executions"] == expected
    assert downstream["executions"] == expected
    assert list(reversed(upstream["executions"])) == expected

    assert downstream["handoffs"] == path["handoffs"]
    assert list(reversed(upstream["handoffs"])) == path["handoffs"]
    assert all(
        handoff["type"] == "emission_edge"
        for step in path["handoffs"]
        for handoff in step
    )
    assert "EMIT ->" in path_query.show(color=False)
    assert "EMIT ->" in downstream_query.show(color=False)
    assert "EMIT ->" in upstream_query.show(color=False)
