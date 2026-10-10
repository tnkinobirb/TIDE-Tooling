"""Show the same small execution story through every TIDE adapter."""

from pathlib import Path
import sys
from tempfile import TemporaryDirectory

from tide_tooling import TIDE, node, pipeline


RAW_ORDERS = [3, -1, 2]
COLOURS = ("36", "35", "34", "33", "32")


def clean_orders_data(raw_orders):
    return sorted(order for order in raw_orders if order >= 0)


def build_report_data(clean_orders):
    return {"orders": clean_orders, "count": len(clean_orders)}


@node(args=("@raw_orders",), targets="clean_orders")
def clean_orders(raw_orders):
    return clean_orders_data(raw_orders)


@node(args=("@clean_orders",), targets="report")
def build_report(clean_orders):
    return build_report_data(clean_orders)


@pipeline
def native_pipeline():
    clean_orders()
    build_report()


def section(index, title, fact):
    colour = COLOURS[index % len(COLOURS)]
    rule = "=" * 72
    print(f"\n\033[1;{colour}m{rule}\n{index + 1}. {title}\n{fact}\n{rule}\033[0m")


def comparison_section(index, target, fact):
    section(index, f"NATIVE TIDE vs {target}", fact)


def native_trace():
    tide = TIDE()
    tide.declare("raw_orders", RAW_ORDERS)
    return tide.ocean.run_pipeline(native_pipeline, mode="sequential")


def now_trace():
    with TemporaryDirectory(
        prefix="tide-adapter-showcase-",
        ignore_cleanup_errors=True,
    ) as project_dir:
        return TIDE.now(
            Path(__file__),
            "--now-workload",
            project_dir=project_dir,
        )


def openlineage_trace():
    def dataset(name):
        return {"namespace": "showcase", "name": name}

    def event(run_id, job, inputs, outputs, time):
        return {
            "eventType": "COMPLETE",
            "eventTime": time,
            "run": {"runId": run_id},
            "job": {"namespace": "showcase", "name": job},
            "inputs": [dataset(name) for name in inputs],
            "outputs": [dataset(name) for name in outputs],
        }

    return TIDE.from_openlineage([
        event("clean", "clean_orders", ["raw_orders"], ["clean_orders"],
              "2026-10-06T09:00:00Z"),
        event("report", "build_report", ["clean_orders"], ["report"],
              "2026-10-06T09:01:00Z"),
    ])


def openinference_trace():
    return TIDE.from_openinference([
        {
            "traceId": "showcase",
            "spanId": "clean",
            "name": "clean_orders",
            "attributes": {
                "openinference.span.kind": "CHAIN",
                "input.value": repr(RAW_ORDERS),
                "output.value": repr(clean_orders_data(RAW_ORDERS)),
            },
        },
        {
            "traceId": "showcase",
            "spanId": "report",
            "parentSpanId": "clean",
            "name": "build_report",
            "attributes": {
                "openinference.span.kind": "CHAIN",
                "input.value": repr(clean_orders_data(RAW_ORDERS)),
                "output.value": repr(
                    build_report_data(clean_orders_data(RAW_ORDERS))
                ),
            },
        },
    ])


def opentelemetry_trace():
    def span(span_id, name, parent=None):
        value = {
            "traceId": "showcase",
            "spanId": span_id,
            "name": name,
            "startTimeUnixNano": "1",
            "endTimeUnixNano": "2",
            "status": {"code": "STATUS_CODE_OK"},
        }
        if parent is not None:
            value["parentSpanId"] = parent
        return value

    return TIDE.from_opentelemetry({
        "resourceSpans": [{
            "resource": {"attributes": []},
            "scopeSpans": [{
                "scope": {"name": "showcase"},
                "spans": [
                    span("clean", "clean_orders"),
                    span("report", "build_report", "clean"),
                ],
            }],
        }],
    })


def workload():
    cleaned = clean_orders_data(RAW_ORDERS)
    return build_report_data(cleaned)


def main():
    traces = (
        ("NATIVE TIDE", "Explicit execution and State evidence.", native_trace),
        ("noWorkflow", "Observed Python evaluations and activation ancestry.", now_trace),
        ("OpenLineage", "Explicit job input/output Dataset evidence.", openlineage_trace),
        ("OpenInference", "Span-local serialized input/output evidence.", openinference_trace),
        ("OpenTelemetry", "Span execution and parent/child evidence only.", opentelemetry_trace),
    )
    for index, (title, fact, build) in enumerate(traces):
        section(index, title, fact)
        trace = build()
        trace.show(color=True)

    n_trace = native_trace()
    ol_trace = openlineage_trace()
    comparison_section(
        5,
        "OpenLineage",
        "Compare explicit native State flow with Dataset lineage.",
    )
    n_trace.compare(
        ol_trace,
        represent_unknown_producers=True,
    ).show(full=True)
    oi_trace = openinference_trace()
    comparison_section(
        6,
        "OpenInference",
        "Compare explicit native State flow with Span-local input/output evidence.",
    )
    n_trace.compare(
        oi_trace,
        represent_unknown_producers=True,
    ).show(full=True)
    ot_trace = opentelemetry_trace()
    comparison_section(
        7,
        "OpenTelemetry",
        "Compare explicit native State flow with structural Span ancestry.",
    )
    n_trace.compare(
        ot_trace,
        represent_unknown_producers=True,
    ).show(full=True)

if __name__ == "__main__":
    workload() if "--now-workload" in sys.argv else main()
