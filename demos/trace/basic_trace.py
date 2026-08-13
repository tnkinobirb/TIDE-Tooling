"""Create, inspect, and bundle a basic three-step Trace."""

from tide_tooling import TIDE
from pathlib import Path

def basic_trace():
    tide = TIDE()
    tide.event("A", writes={"A_OUT": "!A"})
    tide.event("B", reads=["A_OUT"], writes={"B_OUT": "!B"})
    tide.event("C", reads=["B_OUT"])
    return tide.trace()

def main():
    trace = basic_trace()
    trace.show()
    trace.investigate("B", "actor").show()
    trace.path("A", "C", "actor", "actor").show()
    trace.to_bundle(path=Path(__file__).with_name("basic_trace.json"))


if __name__ == "__main__":
    main()
