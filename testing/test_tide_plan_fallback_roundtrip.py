from tide_tooling import TIDE, node, pipeline
from tide_tooling.tooling_core.engine.classes import DataRef


def test_dynamic_plan_fallback_round_trips_without_opaque_type():
    @node(targets=DataRef("csv", "mem", literal=True))
    def produce():
        return 1

    @node(args=("@mem:csv:*",))
    def consume(values):
        return values

    @pipeline
    def flow():
        produce()
        consume()

    trace = TIDE().ocean.run_pipeline(flow, mode="sequential")
    restored = TIDE.from_bundle(trace.to_bundle())

    assert restored._tide_plan == trace._tide_plan

    dynamic_values = tuple(trace._tide_plan["D"].values())
    restored_values = tuple(restored._tide_plan["D"].values())

    assert dynamic_values
    assert all(
        value.__class__.__name__ == "__fallback__"
        for value in dynamic_values
    )
    assert all(
        value.__class__.__name__ == "__fallback__"
        for value in restored_values
    )
