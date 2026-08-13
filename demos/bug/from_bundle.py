"""Reload the bundles produced by bug_pipeline.py."""

from tide_tooling import TIDE
from pathlib import Path
LOAD_PATH = Path(__file__).parent

def load_tide_func_trace():
    tide = TIDE()
    return tide.from_bundle(LOAD_PATH / "tide_func_trace.json")
     
def load_tide_node_trace():
    tide = TIDE()
    return tide.from_bundle(LOAD_PATH / "tide_node_trace.json")
     
def load_ocean_trace_trace():
    tide = TIDE()
    return tide.from_bundle(LOAD_PATH / "tide_ocean_trace.json")
     
def main():
    load_tide_func_trace().show()
    load_tide_node_trace().show()
    load_ocean_trace_trace().show()


if __name__ == "__main__":
    main()
