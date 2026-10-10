"""Project explicit OpenLineage RunEvents into a native TIDE Trace."""

from tide_tooling import TIDE


def dataset(name, *, version=None):
    value = {"namespace": "warehouse", "name": name}
    if version is not None:
        value["facets"] = {
            "version": {"datasetVersion": version},
        }
    return value


def run_event(run_id, job, event_type, *, inputs=(), outputs=(), time):
    return {
        "eventType": event_type,
        "eventTime": time,
        "run": {"runId": run_id},
        "job": {"namespace": "analytics", "name": job},
        "inputs": list(inputs),
        "outputs": list(outputs),
        "producer": "https://example.test/openlineage-demo",
    }


def main():
    events = [
        run_event(
            "clean-1",
            "clean_orders",
            "START",
            inputs=[dataset("raw_orders")],
            time="2026-10-06T09:00:00Z",
        ),
        run_event(
            "clean-1",
            "clean_orders",
            "COMPLETE",
            outputs=[dataset("clean_orders", version="source-v7")],
            time="2026-10-06T09:01:00Z",
        ),
        run_event(
            "clean-2",
            "clean_orders",
            "COMPLETE",
            inputs=[dataset("clean_orders", version="source-v7")],
            outputs=[dataset("clean_orders", version="source-v8")],
            time="2026-10-06T10:01:00Z",
        ),
        run_event(
            "report-1",
            "build_report",
            "COMPLETE",
            inputs=[dataset("clean_orders", version="source-v8")],
            outputs=[dataset("orders_report")],
            time="2026-10-06T10:02:00Z",
        ),
    ]

    trace = TIDE.from_openlineage(events)
    trace.show(color=True)

    print("\nFIRST CLEAN ORDERS STATE")
    clean_orders = next(
        write.stored_target
        for record in trace.log
        if record.func_name == "clean_orders"
        for write in record.writes
    )
    trace.investigate(clean_orders).show(
        explore_tool_tips=False,
        color=True,
    )


if __name__ == "__main__":
    main()
