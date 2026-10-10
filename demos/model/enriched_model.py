"""Project enriched State and execution evidence from a TIDE Model."""

from tide_tooling import TIDE


MODEL = {
    "schema": "TIDE-Model",
    "schema_version": 1,
    "host": "enriched-model-demo",
    "host_record": {
        "source": "example import",
        "report_payload": '{"status":"ready"}',
        "report_mime_type": "application/json",
    },
    "E": ["load-exec", "report-exec"],
    "S": ["orders-state", "report-state"],
    "I": [["report-exec", "orders-state"]],
    "O": [
        ["load-exec", "orders-state"],
        ["report-exec", "report-state"],
    ],
    "executions": {
        "load-exec": {
            "actor": {
                "name": "load_orders",
                "ocean": "imported-ocean",
                "runner": "orders-run",
            },
            "status": "succeeded",
        },
        "report-exec": {
            "actor": {
                "name": "build_report",
                "ocean": "imported-ocean",
                "runner": "orders-run",
            },
            "status": "succeeded",
        },
    },
    "states": {
        "orders-state": {
            "value_type": list,
            "value": [{"order_id": 1}, {"order_id": 2}],
        },
        "report-state": {
            "value_type": dict,
        },
    },
}


def main():
    validation = TIDE.report_model_validity(MODEL)
    assert validation.semantically_valid, validation["errors"]

    trace = TIDE.from_model(MODEL)
    trace.show(values=False)

    print("\nMATERIALISED VALUE")
    trace.value_of("__model__:orders-state", show=True)

    print("\nRECORDED VALUE WITHOUT RAW BACKING")
    trace.value_of("__model__:report-state", show=True)

    print("\nSOURCE CONTEXT")
    print(trace.host_record)


if __name__ == "__main__":
    main()
