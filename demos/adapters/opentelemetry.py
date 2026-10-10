"""Project OpenTelemetry Spans as TIDE executions and native ancestry."""

from tide_tooling import TIDE
COLOUR = True

def span(span_id, name, *, parent=None, status="STATUS_CODE_OK"):
    return {
        "traceId": "4bf92f3577b34da6a3ce929d0e0e4736",
        "spanId": span_id,
        "parentSpanId": parent,
        "name": name,
        "kind": "SPAN_KIND_INTERNAL",
        "startTimeUnixNano": "1660296024000000000",
        "endTimeUnixNano": "1660296025000000000",
        "status": {"code": status},
        "attributes": [{
            "key": "demo.detail",
            "value": {"stringValue": "retained in host_record only"},
        }],
    }


def main():
    source = {
        "resourceSpans": [{
            "resource": {
                "attributes": [{
                    "key": "service.name",
                    "value": {"stringValue": "orders-service"},
                }],
            },
            "scopeSpans": [{
                "scope": {"name": "orders.instrumentation"},
                "spans": [
                    span("00f067aa0ba902b7", "HTTP GET /orders"),
                    span(
                        "b7ad6b7169203331",
                        "SELECT orders",
                        parent="00f067aa0ba902b7",
                    ),
                ],
            }],
        }],
    }

    trace = TIDE.from_opentelemetry(source)
    trace.show(color=COLOUR)

    root, child = trace.log
    print("\nCHILDREN")
    trace.children_of(root.exec_id).show(show_in_out=False, color=COLOUR)

    print("\nPARENTS")
    trace.parents_of(child.exec_id).show(show_in_out=False, color=COLOUR)

    print("\nPATH")
    trace.path(
        root.exec_id,
        child.exec_id,
        "exec_record",
        "exec_record",
        group=False,
    ).show(color=COLOUR)

    print("\nUPSTREAM")
    trace.upstream(
        child.exec_id,
        "exec_record",
        grouped=False,
    ).show(color=COLOUR)

    print("\nDOWNSTREAM")
    trace.downstream(
        root.exec_id,
        "exec_record",
        grouped=False,
    ).show(color=COLOUR)

    print("\nEXACT SOURCE SPAN")
    print(trace.host_record[child.exec_id]["span"])

    print("\nVALUE QUERY NOTE")
    print(
        "Generic OpenTelemetry Spans establish executions and ancestry, "
        "not TIDE States. value_of(), used_as(), produced_by() and "
        "consumed_by() become meaningful only when a documented "
        "convention-specific projection supplies explicit State evidence."
    )
    print("Represented States:", trace.to_model(enrich=False)["S"])


if __name__ == "__main__":
    main()
