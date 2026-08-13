"""Reload and query the bundle produced by data_pipeline.py."""

from tide_tooling import TIDE
from pathlib import Path

def main():
    path = Path(__file__).with_name("data_pipeline_trace.json")
    trace = TIDE.from_bundle(path)
    trace.show()
    trace.find("PATH").show()
    trace.path("load", "clean", "actor", "actor").show()
    raw = trace.value_of("global:raw_data", show=True)
    processed = trace.value_of("global:processed_data", show=True)
    trace.value_of("global:LOAD_PATH", show=True)
    trace.value_of("global:LOAD_FILE", show=True)
    trace.diff(raw, processed).show()


if __name__ == "__main__":
    main()
