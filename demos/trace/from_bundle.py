"""Reload and query the bundle produced by basic_trace.py."""

from pathlib import Path
import json
from tide_tooling import TIDE
def main():
    bundle_path = Path(__file__).with_name("basic_trace.json")
    with bundle_path.open(encoding="utf-8") as file:
        bundle = json.load(file)

    trace = TIDE.from_bundle(bundle)
    trace.show()
    trace.find("B").show()
    trace.value_of("global:B_OUT", show=True)


if __name__ == "__main__":
    main()
