from concurrent.futures import ThreadPoolExecutor
import threading

import pytest

from tide_tooling.tooling_core.engine.classes import DataRef
from tide_tooling import TIDE


def test_constructor_default_store_is_created_and_used_by_event_and_exec():
    def produce(value):
        return value + "-from-exec"

    tide = TIDE(default_store="custom")
    tide.event("seed", writes={"seed": "from-event"})
    tide.exec(produce, args=["@seed"], writes=["result"])

    assert tide.default_store == "custom"
    assert tide.ocean.store_registry.exists("custom")
    assert tide.ocean.store_router.get("custom", "seed") == "from-event"
    assert tide.trace().value_of("custom:result", 0) == "from-event-from-exec"


def test_set_default_store_switches_unscoped_event_writes():
    tide = TIDE()

    tide.set_default_store("first")
    tide.declare("value", "first-value")

    tide.set_default_store("second")
    tide.declare("value", "second-value")

    assert tide.ocean.store_router.get("first", "value") == "first-value"
    assert tide.ocean.store_router.get("second", "value") == "second-value"
    assert tide.trace().value_of("first:value", 0) == "first-value"
    assert tide.trace().value_of("second:value", 0) == "second-value"


def test_declare_is_a_tracked_tide_user_event():
    tide = TIDE(default_store="custom")
    tide.declare("answer", 42)

    record = tide.trace().exec_log[0]

    assert record.func_name == "TIDE_USER"
    assert record.writes[0].stored_target == "custom:answer"
    assert record.writes[0].stored_result == 42
    assert record.writes[0].cause == "declared"


def test_store_context_restores_the_previous_data_ref_default():
    previous_store = DataRef.default_store
    DataRef.default_store = "outside"

    try:
        tide = TIDE(default_store="custom")
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
        tide = TIDE(default_store="custom")

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
    first = TIDE(default_store="first_context")
    second = TIDE(default_store="second_context")

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
        first.close()
        second.close()
