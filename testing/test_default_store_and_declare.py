from concurrent.futures import ThreadPoolExecutor
import threading

import pytest

from tide_tooling.tooling_core.engine.classes import DataRef
from tide_tooling.tooling_core.engine.errors import MissingStoreError
from tide_tooling import Ocean, TIDE, node, pipeline


def test_constructor_default_store_must_already_exist():
    with pytest.raises(MissingStoreError):
        TIDE(default_store="custom")


def test_tide_default_store_applies_while_ocean_materialises_pipeline():
    @node(args=("@raw_orders",), targets="clean_orders")
    def clean(raw_orders):
        return sorted(raw_orders)

    @pipeline
    def clean_pipeline():
        clean()

    tide = TIDE(default_store="global")
    tide.declare("raw_orders", [3, 1, 2])
    trace = tide.ocean.run_pipeline(clean_pipeline, mode="sequential")

    execution = next(record for record in trace.log if record.func_name == "clean")
    assert execution.deps[0].unscoped_reference == "global:raw_orders"
    assert execution.writes[0].stored_target == "global:clean_orders"
    assert trace.value_of("global:clean_orders", 0) == [1, 2, 3]


def test_run_pipeline_default_store_override_is_call_scoped():
    @node(targets="value")
    def produce():
        return "result"

    @pipeline
    def produce_pipeline():
        produce()

    ocean = Ocean(default_store="global")
    trace = ocean.run_pipeline(
        produce_pipeline,
        mode="sequential",
        default_store="mem",
    )

    execution = next(record for record in trace.log if record.func_name == "produce")
    assert execution.writes[0].stored_target == "mem_0:value"
    assert ocean.default_store == "global"


def test_run_node_default_store_override_reaches_execution_context():
    @node
    def observe_store():
        return DataRef("observed").store

    ocean = Ocean(default_store="mem")

    assert ocean.run_node(
        observe_store.as_node(),
        default_store="global",
    ) == "global"
    assert ocean.default_store == "mem"


def test_dictionary_event_creates_store_for_default_event_and_exec_use():
    def produce(value):
        return value + "-from-exec"

    tide = TIDE()
    tide.event("seed", writes={"custom:seed": "from-event"})
    tide.set_default_store("custom")
    tide.exec(produce, args=["@seed"], writes=["result"])

    assert tide.default_store == "custom"
    assert tide.ocean.store_registry.exists("custom")
    assert tide.ocean.store_router.get("custom", "seed") == "from-event"
    assert tide.trace().value_of("custom:result", 0) == "from-event-from-exec"


def test_set_default_store_switches_unscoped_event_writes():
    tide = TIDE()
    tide.event("stores", writes={"first:seed": None, "second:seed": None})

    tide.set_default_store("first")
    tide.declare("value", "first-value")

    tide.set_default_store("second")
    tide.declare("value", "second-value")

    assert tide.ocean.store_router.get("first", "value") == "first-value"
    assert tide.ocean.store_router.get("second", "value") == "second-value"
    assert tide.trace().value_of("first:value", 0) == "first-value"
    assert tide.trace().value_of("second:value", 0) == "second-value"


def test_declare_is_a_tracked_tide_user_event():
    tide = TIDE()
    tide.event("store", writes={"custom:seed": None})
    tide.set_default_store("custom")
    tide.declare("answer", 42)

    record = next(record for record in tide.trace().log if record.func_name == "TIDE_USER")

    assert record.func_name == "TIDE_USER"
    assert record.writes[0].stored_target == "custom:answer"
    assert record.writes[0].stored_result == 42
    assert record.writes[0].cause.description == "declared"


def test_store_context_restores_the_previous_data_ref_default():
    previous_store = DataRef.default_store
    DataRef.default_store = "outside"

    try:
        tide = TIDE()
        tide.event("store", writes={"custom:seed": None})
        tide.set_default_store("custom")
        tide.event("event", writes={"value": "event"})

        def produce():
            return "exec"

        tide.exec(produce, writes=["result"])

        assert DataRef.default_store == "outside"
    finally:
        DataRef.default_store = previous_store


def test_store_context_restores_after_exec_failure():
    previous_store = DataRef.default_store
    DataRef.default_store = "outside"

    try:
        tide = TIDE()
        tide.event("store", writes={"custom:seed": None})
        tide.set_default_store("custom")

        def fail():
            raise RuntimeError("expected failure")

        with pytest.raises(RuntimeError, match="expected failure"):
            tide.exec(fail, writes=["result"])

        assert DataRef.default_store == "outside"
    finally:
        DataRef.default_store = previous_store


def test_default_store_must_be_a_non_empty_string():
    with pytest.raises(ValueError):
        TIDE(default_store="")


def test_default_store_is_isolated_between_concurrent_tide_instances():
    barrier = threading.Barrier(2)
    first = TIDE()
    second = TIDE()
    first.event("store", writes={"first_context:seed": None})
    second.event("store", writes={"second_context:seed": None})
    first.set_default_store("first_context")
    second.set_default_store("second_context")

    def execute(tide):
        def observe_store():
            barrier.wait(timeout=5)
            return DataRef("observed").store

        return tide.exec(observe_store, writes="result")

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            first_result = executor.submit(execute, first)
            second_result = executor.submit(execute, second)

            assert first_result.result(timeout=10) == "first_context"
            assert second_result.result(timeout=10) == "second_context"

        assert first.trace().value_of("first_context:result", 0) == (
            "first_context"
        )
        assert second.trace().value_of("second_context:result", 0) == (
            "second_context"
        )
        assert DataRef.default_store == "mem"
    finally:
        first.ocean.close()
        second.ocean.close()
