"""Run a small CSV pipeline and save its Trace bundle."""

from tide_tooling import TIDE, Trace
from pathlib import Path
from _adjusted_functions import load, new_clean, old_process, report, export

# Keep the recorded demo paths portable across clones and operating systems.
# The README and test suite run demos from the repository root.
LOAD_PATH = Path(__file__).resolve().parents[1] / "pipeline"
EXP_PATH = LOAD_PATH / "processed_data.csv"

def adjusted_pipeline() -> Trace:
    tide = TIDE()
    
    tide.declare("LOAD_FILE", "data.csv")
    tide.declare("LOAD_PATH", LOAD_PATH)
    tide.declare("EXP_PATH", EXP_PATH)
    
    tide.exec(load, reads=["LOAD_PATH", "LOAD_FILE"], writes=["raw_data"])
    tide.exec(new_clean, reads=["raw_data"], writes=["clean_data"])
    tide.exec(old_process, reads=["clean_data"], writes=["processed_data"])
    tide.exec(report, reads=["processed_data"], writes=["report"])
    tide.exec(export, reads=["processed_data", "EXP_PATH"], writes=["export_path"])
    return tide.trace()

def main():
    trace = adjusted_pipeline()
    trace.show()
    trace.to_bundle(path=LOAD_PATH / "data_pipeline_trace.json")


if __name__ == "__main__":
    main()
