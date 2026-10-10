"""Run a small CSV pipeline and save its Trace bundle."""

from tide_tooling import TIDE, Trace
from pathlib import Path
from functions import load, clean, process, report, export

# Keep the recorded demo paths portable across clones and operating systems.
# The README and test suite run demos from the repository root.
LOAD_PATH = Path(__file__).resolve().parents[1] / "pipeline"
EXP_PATH = LOAD_PATH / "processed_data.csv"

def pipeline() -> Trace:
    tide = TIDE()
    
    tide.declare("LOAD_FILE", "data.csv")
    tide.declare("LOAD_PATH", LOAD_PATH)
    tide.declare("EXP_PATH", EXP_PATH)
    
    tide.exec(load, reads=["LOAD_PATH", "LOAD_FILE"], writes=["raw_data"], return_trace=True).show()
    tide.exec(clean, reads=["raw_data"], writes=["clean_data"])
    tide.exec(process, reads=["clean_data"], writes=["processed_data"])
    tide.exec(report, reads=["processed_data"], writes=["report"])
    tide.exec(export, reads=["processed_data", "EXP_PATH"], writes=["export_path"])
    return tide.trace()

def main():
    from time import perf_counter

    start = perf_counter()

    trace = pipeline()
    after_pipeline = perf_counter()

    trace.show()
    after_show = perf_counter()

    trace.to_bundle(path=LOAD_PATH / "data_pipeline_trace.json", serialise_events=False)
    bundled=  TIDE.from_bundle(LOAD_PATH / "data_pipeline_trace.json")
    bundled.show()
    bundled.value_of("global:clean_data", show=True)
    after_bundle = perf_counter()

    print()
    print(f"pipeline():  {after_pipeline - start:.4f}s")
    print(f"show():      {after_show - after_pipeline:.4f}s")
    print(f"to_bundle(): {after_bundle - after_show:.4f}s")
    print(f"TOTAL:       {after_bundle - start:.4f}s")


    bundle = trace.to_bundle(serialise_events=False)
    restored = TIDE.from_bundle(bundle)
    trace.compare(restored, mapping="literal").show()
    trace.diff(restored.to_model(), trace.to_model()).show()
    assert restored.to_model() == trace.to_model()

    assert restored.corruption == trace.corruption
    assert restored.divergence == trace.divergence
    assert restored.event_log is None
        
if __name__ == "__main__":
    main()
