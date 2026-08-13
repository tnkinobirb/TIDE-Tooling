"""Validate and import a small conforming TIDE Model."""

from tide_tooling import TIDE


MODEL = {
    "schema": "TIDE-Model",
    "schema_version": 1,
    "host": "demo-host",
    "I": [
        {"exec_id": "clean-exec", "state_id": "raw-state"},
        {"exec_id": "report-exec", "state_id": "clean-state"},
    ],
    "O": [
        {"exec_id": "load-exec", "state_id": "raw-state"},
        {"exec_id": "clean-exec", "state_id": "clean-state"},
        {"exec_id": "report-exec", "state_id": "report-state"},
    ],
    "executions": {
        "load-exec": {"actor": "load"},
        "clean-exec": {"actor": "clean"},
        "report-exec": {"actor": "report"},
    },
    "states": {
        "raw-state": {
            "reference": "model:raw_data",
            "version": 0,
            "value": " loaded raw data ",
        },
        "clean-state": {
            "reference": "model:clean_data",
            "version": 0,
            "value": "loaded raw data",
        },
        "report-state": {
            "reference": "model:report",
            "version": 0,
            "value": {"status": "ready"},
        },
    },
}


def main():
    validation = TIDE.validate_model(MODEL)
    assert validation["conforms"], validation["errors"]

    trace = TIDE.from_model(MODEL)
    trace.show()
    trace.find("raw-state").show()
    trace.path("load", "report", "actor", "actor").show()
    trace.value_of("__model__:report-state", show=True)


if __name__ == "__main__":
    main()
