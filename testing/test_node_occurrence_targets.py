import threading
import time

from tide_tooling import Ocean, TIDE, node, pipeline
from tide_tooling.tooling_core.engine.classes import DataRef, Event
from tide_tooling.tooling_core.engine.event_payloads import State
from tide_tooling.tooling_core.engine.events import Cause, RouterEvents
from tide_tooling.tooling_core.engine.utils import tide_snapshot_value


def wait_until(predicate, timeout=2):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("condition was not reached")


def emit_write(tide, key, value):
    tide.ocean.event_interface.emit_event_to_ocean(Event(ocean_id=tide.ocean.id, source="probe", source_id="probe", type=RouterEvents.STORE_WRITE, cause=Cause(description="runtime repeat probe"), payload=State(DataRef(key, "global"), value, tide_snapshot_value(value, boundary="write"))))


def test_repeated_nested_node_calls_receive_unique_implicit_targets():
    @node
    def produce(value):
        return value

    @node
    def consume(value):
        return value

    @pipeline
    def repeated():
        return [consume(produce(value)) for value in (1, 2, 3, 4)]

    trace = Ocean().run_pipeline(repeated, mode="sequential")
    producers = [record for record in trace.log if record.func_name == "produce"]
    consumers = [record for record in trace.log if record.func_name == "consume"]

    assert {record.output_value for record in consumers} == {1, 2, 3, 4}
    assert len({record.writes[0].stored_target for record in producers}) == 4
    assert {record.writes[0].stored_version for record in producers} == {0}

def test_repeated_explicit_targets_remain_raw_and_shared():
    @node(targets="shared")
    def produce(value):
        return value

    @pipeline
    def repeated():
        return [produce(1), produce(2)]

    trace = Ocean().run_pipeline(repeated, mode="sequential")
    producers = [record for record in trace.log if record.func_name == "produce"]

    assert [record.writes[0].stored_target for record in producers] == ["mem_0:shared"] * 2
    assert [record.writes[0].stored_version for record in producers] == [0, 1]


def test_share_implicit_target_applies_only_to_implicit_targets():
    @node(share_implicit_target=True)
    def produce(value):
        return value

    @node
    def consume(value):
        return value

    @pipeline
    def repeated():
        return [consume(produce(1)), consume(produce(2))]

    trace = Ocean().run_pipeline(repeated, mode="sequential")
    producers = [record for record in trace.log if record.func_name == "produce"]

    assert len({record.writes[0].stored_target for record in producers}) == 1
    assert [record.writes[0].stored_version for record in producers] == [0, 1]


def test_same_node_runtime_repeats_keep_one_target_with_distinct_exec_versions(monkeypatch):
    calls = []

    @node(targets="global:observed", force_update=True)
    def observe(value):
        calls.append(value)
        return value

    @pipeline
    def live_pipeline():
        return observe("@global:source")

    tide = TIDE()
    runner_started = threading.Event()
    runner_holder = []
    original_start = tide.ocean._start_runner

    def start_persistent(runner_id, stay_alive=True):
        original_start(runner_id, stay_alive=True)
        runner_holder.append(tide.ocean._get_runner(runner_id))
        runner_started.set()

    monkeypatch.setattr(tide.ocean, "_start_runner", start_persistent)
    traces = []
    errors = []

    def execute():
        try:
            traces.append(tide.ocean.run_pipeline(live_pipeline, mode="sequential", stay_alive=True))
        except Exception as error:
            errors.append(error)

    worker = threading.Thread(target=execute)
    worker.start()
    assert runner_started.wait(timeout=2)

    emit_write(tide, "source", "first")
    wait_until(lambda: calls == ["first"])
    emit_write(tide, "source", "second")
    wait_until(lambda: calls == ["first", "second"])

    runner_holder[0].stay_alive = False
    worker.join(timeout=10)
    assert not worker.is_alive()
    assert errors == []

    executions = [record for record in traces[0].log if record.func_name == "observe"]
    assert len({record.node_id for record in executions}) == 1
    assert len({record.exec_id for record in executions}) == 2
    assert len({record.writes[0].stored_target for record in executions}) == 1
    assert executions[0].writes[0].stored_target == "global:observed"
    assert [record.writes[0].stored_version for record in executions] == [0, 1]
