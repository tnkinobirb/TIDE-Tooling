"""Ordering contracts for events routed through Ocean.

Path tests prove the reconstructed graph. These tests prove that the runtime
events used to build that graph crossed the central EventRouter in a valid
order.
"""

import time

import pytest

from tide_tooling import DELETE, EMIT, END, READ, RUN, WRITE, TIDE, node, pipeline

from tide_tooling.tooling_core.engine.classes import DataRef, Node, Pipeline
from tide_tooling.tooling_core.engine.errors import (
    MissingExecutionEvidenceError,
    PipelineDeadlockError,
)
from tide_tooling.tooling_core.engine.executor import ExecMeta
from tide_tooling.tooling_core.engine.event_payloads import PipelineRun, Reference, State
from tide_tooling.tooling_core.engine.events import (
    Cause,
    Event,
    Effects,
    ExecutorEvents,
    LogEvents,
    OceanEvents,
    PipelineEvents,
    RouterEvents,
)
from tide_tooling.tooling_core.bundling.TIDEOpaqueValue import TIDEOpaqueValue
from tide_tooling.tooling_core.engine.PipelineRunner import PipelineRunner
from tide_tooling.tooling_core.engine.tide_runtime_logging import ExecutionLog
from tide_tooling.tooling_core.engine.utils import _is_tide_exec_uuid, create_tide_exec_uuid, tide_snapshot_value


def _event_types(tide):
    return [entry.type for entry in tide.ocean.event_logger.events]


def _event_indexes(tide, event_type):
    return [
        index
        for index, entry in enumerate(tide.ocean.event_logger.events)
        if entry.type == event_type
    ]


def test_public_event_operation_takes_one_fifo_scheduler_turn():
    tide = TIDE()
    earlier = Event(
        ocean_id=tide.ocean.id,
        source="test",
        source_id="test",
        type=LogEvents.RUNNER_DEADLOCK,
        cause=Cause(description="queued first"),
        payload={},
    )
    tide.ocean.consume_event(earlier)

    tide.event("writer", writes={"global:value": 1})

    events = tide.ocean.event_logger.events
    earlier_index = next(
        index for index, event in enumerate(events)
        if event.event_id == earlier.event_id
    )
    exec_index = next(
        index for index, event in enumerate(events)
        if event.type == LogEvents.EVENT_EXEC
    )
    state_index = next(
        index for index, event in enumerate(events)
        if event.type == PipelineEvents.DEP_AVAILABLE
        and getattr(event.payload.ref, "key", None) == "value"
    )

    assert earlier_index < exec_index < state_index
    assert tide.trace().value_of("global:value") == [1]


def test_store_registration_rejects_accepted_scheduler_work():
    tide = TIDE()
    tide.ocean.consume_event(Event(
        ocean_id=tide.ocean.id,
        source="test",
        source_id="test",
        type=LogEvents.RUNNER_DEADLOCK,
        cause=Cause(description="pending"),
        payload={},
    ))

    with pytest.raises(RuntimeError, match="while Ocean is scheduling"):
        tide.register_store("late")

    tide.ocean.tick(external=True)


def test_emitted_definition_with_unavailable_dependency_is_valid_evidence():
    @node
    def never_runs(value):
        return value

    @node
    def emitter():
        return EMIT(never_runs.as_node("@global:missing"))

    @pipeline
    def flow():
        return emitter()

    tide = TIDE()
    trace = tide.ocean.run_pipeline(flow, mode="sequential")

    assert [record.func_name for record in trace.log] == ["emitter"]
    emitted = trace.log[0].emitted_nodes[0]
    assert emitted.func_name == "never_runs"
    assert emitted.declared_deps == [DataRef("missing", "global")]
    assert trace.deadlocked is True
    assert trace.deadlock == {
        0: {
            emitted.base_node: (DataRef("missing", "global"),),
        }
    }
    assert "DEADLOCKED: 1 waiting node" in trace.show(color=False)
    assert trace.waiting.to_dict() == {
        "type": "waiting",
        "deadlocked": True,
        "count": 1,
        "definitions": [{
            "runner_id": 0,
            "node_definition_id": str(emitted.base_node),
            "references": ["global:missing"],
        }],
    }
    waiting_show = trace.waiting.show(color=False)
    assert str(emitted.base_node) in waiting_show
    assert "global:missing" in waiting_show
    restored = TIDE.from_bundle(trace.to_bundle())
    assert restored.deadlock == trace.deadlock
    assert restored.waiting.to_dict() == trace.waiting.to_dict()
    assert not any(
        event.type == ExecutorEvents.NODE_READY
        and event.payload.node_snapshot["func_name"] == "never_runs"
        for event in trace.events
    )


def test_sequential_dependency_chain_routes_events_in_causal_order():
    @node(targets=["global:a"])
    def make_a():
        return 1

    @node(targets=["global:b"])
    def make_b(value):
        return value + 1

    @pipeline
    def chain():
        return make_b(make_a())

    tide = TIDE()
    trace = tide.ocean.run_pipeline(chain, mode="sequential")

    assert _event_types(tide) == [
        OceanEvents.REGISTER_PIPELINE,
        OceanEvents.RUN_PIPELINE,
        LogEvents.RUNNER_PLAN,
        ExecutorEvents.NODE_READY,
        PipelineEvents.RESULT_READY,
        RouterEvents.STORE_WRITE,
        PipelineEvents.DEP_AVAILABLE,
        ExecutorEvents.NODE_READY,
        PipelineEvents.RESULT_READY,
        RouterEvents.STORE_WRITE,
        PipelineEvents.DEP_AVAILABLE,
        OceanEvents.KILL_RUNNER,
    ]
    assert [record.seq_id for record in trace.log] == list(range(len(trace.log)))
    assert [record.event_seq_id for record in trace.log] == [
        index
        for index, event in enumerate(trace.events)
        if event.type == PipelineEvents.RESULT_READY
    ]
    assert trace.waiting.to_dict() == {
        "type": "waiting",
        "deadlocked": False,
        "count": 0,
        "definitions": [],
    }
    assert "none" in trace.waiting.show(color=False)
    assert trace.path(
        "global:a",
        "make_b",
        "reference",
        "actor",
    ).exists()
    assert not trace.path(
        "global:b",
        "make_a",
        "reference",
        "actor",
    ).exists()


def test_parallel_join_waits_for_both_dependency_events_without_ordering_siblings():
    @node(targets=["global:left"])
    def left():
        return "left"

    @node(targets=["global:right"])
    def right():
        return "right"

    @node(targets=["global:joined"])
    def join(left_value, right_value):
        return left_value, right_value

    @pipeline
    def fork_join():
        return join(left(), right())

    tide = TIDE()
    trace = tide.ocean.run_pipeline(fork_join, mode="thread")

    ready = _event_indexes(tide, ExecutorEvents.NODE_READY)
    available = _event_indexes(tide, PipelineEvents.DEP_AVAILABLE)

    assert len(ready) == 3
    assert len(available) == 3
    assert available[0] < ready[2]
    assert available[1] < ready[2]
    assert trace.path(
        "global:left",
        "join",
        "reference",
        "actor",
    ).exists()
    assert trace.path(
        "global:right",
        "join",
        "reference",
        "actor",
    ).exists()
    assert not trace.path(
        "global:left",
        "right",
        "reference",
        "actor",
    ).exists()


def test_only_central_store_router_consumes_router_events():
    @node(targets=["global:value"])
    def write_value():
        return "value"

    @pipeline
    def one_write():
        return write_value()

    tide = TIDE()
    tide.ocean.run_pipeline(one_write, mode="sequential")
    tide.ocean.run_pipeline(one_write, mode="sequential")

    router = tide.ocean.event_router
    assert len(router.subscribers[RouterEvents.STORE_WRITE]) == 1
    assert len(router.subscribers[RouterEvents.FORCE_WRITE]) == 1
    assert len(router.subscribers[RouterEvents.DELETE]) == 1

    assert not tide.ocean.store_router.event_queue
    assert all(
        not runner.store_router.event_queue
        for runner in tide.ocean.runners.values()
    )

    assert len(tide.ocean.runners) == 2
    assert len(_event_indexes(tide, RouterEvents.STORE_WRITE)) == 2
    assert len(_event_indexes(tide, PipelineEvents.DEP_AVAILABLE)) == 2


def test_queued_delete_invalidates_dependency_before_ready_work_is_submitted():
    @node(targets=["global:out"])
    def consume(value):
        return value

    @pipeline
    def dependent_pipeline():
        return consume("@global:seed")

    tide = TIDE()
    tide.ocean.store_router.set("global", "seed", 1)

    registered = tide.ocean.register_pipeline(dependent_pipeline())
    runner = tide.ocean._create_pipeline_runner(
        registered,
        "sequential",
        {},
    )
    start_event = Event(ocean_id=0, source="ocean", source_id=tide.ocean.id, type=OceanEvents.RUN_PIPELINE, cause=Cause(description="test run"), payload=PipelineRun(registered.id, registered.name, "sequential", {}, False, None, None))
    runner.remember_initial_ready_nodes(start_event)
    runner.runner_start_event_id = start_event.event_id
    runner.trace_event_start_id = start_event.event_id
    tide.ocean.event_router.subscribed_runners[runner.id] = runner
    runner.run(stay_alive=False)

    available = Event(ocean_id=0, 
        source="store_router",
        source_id=tide.ocean.store_router.id,
        target_runner_id=runner.id,
        type=PipelineEvents.DEP_AVAILABLE,
        cause=Cause(description="test"),
        payload=State(DataRef("seed", "global"), 1, tide_snapshot_value(1, boundary="write")),
    )
    deleted = Event(ocean_id=0, 
        source="store_router",
        source_id=tide.ocean.store_router.id,
        target_runner_id=runner.id,
        type=PipelineEvents.DEP_DELETED,
        cause=Cause(description="test"),
        payload=Reference(DataRef("seed", "global")),
    )

    # Both state changes have already reached the central router before the
    # runner gets its scheduling turn.
    tide.ocean.event_router.route(available)
    tide.ocean.event_router.route(deleted)
    runner.tick()

    assert _event_types(tide) == [
        OceanEvents.REGISTER_PIPELINE,
        PipelineEvents.DEP_AVAILABLE,
        PipelineEvents.DEP_DELETED,
    ]
    assert not runner.running_nodes
    assert not runner.runtime_dag.ready_nodes_queue
    assert ExecutorEvents.NODE_READY not in [
        work.event.type
        for work in list(tide.ocean.event_queue.queue)
    ]
    blocked = [
        work.event
        for work in list(tide.ocean.event_queue.queue)
        if work.event.type == LogEvents.NODE_BLOCKED
    ]
    assert len(blocked) == 1
    assert blocked[0].cause.event_id == deleted.event_id
    assert blocked[0].payload.ref == DataRef("seed", "global")


def test_dependency_update_records_block_then_restores_readiness():
    @node(targets=["global:out"])
    def consume(value):
        return value

    @pipeline
    def dependent_pipeline():
        return consume("@global:seed")

    tide = TIDE()
    tide.ocean.store_router.set("global", "seed", 1)
    registered = tide.ocean.register_pipeline(dependent_pipeline())
    runner = tide.ocean._create_pipeline_runner(registered, "sequential", {})
    ref = DataRef("seed", "global")
    runner.on_dep_available(Event(ocean_id=0, 
        source="test",
        source_id="test",
        type=PipelineEvents.DEP_AVAILABLE,
        cause=Cause(description="test"),
        payload=State(ref, 1, tide_snapshot_value(1, boundary="write")),
    ))
    updated = Event(ocean_id=0, 
        source="test",
        source_id="test",
        type=PipelineEvents.DEP_UPDATED,
        cause=Cause(description="test"),
        payload=State(ref, 2, tide_snapshot_value(2, boundary="write")),
    )

    runner.on_dep_update(updated)

    blocked = [
        work.event
        for work in list(tide.ocean.event_queue.queue)
        if work.event.type == LogEvents.NODE_BLOCKED
    ]
    assert len(blocked) == 1
    assert blocked[0].cause.event_id == updated.event_id
    assert all(not missing for missing in runner.runtime_dag.forward_map.values())


def test_write_then_delete_effect_routes_store_state_changes_in_order():
    @node(
        effects=[
            WRITE("global:prepared", "ready"),
            DELETE("global:prepared"),
        ]
    )
    def prepare():
        return None

    @pipeline
    def effect_pipeline():
        return prepare()

    tide = TIDE()
    trace = tide.ocean.run_pipeline(effect_pipeline, mode="sequential")

    relevant = []
    for entry in tide.ocean.event_logger.events:
        event_type = entry.type
        if event_type == LogEvents.EFFECT:
            relevant.append((event_type, entry.payload.effect))
        elif event_type in {
            PipelineEvents.RESULT_READY,
            RouterEvents.STORE_WRITE,
            RouterEvents.DELETE,
            PipelineEvents.DEP_AVAILABLE,
            PipelineEvents.DEP_DELETED,
        }:
            relevant.append((event_type, None))

    assert relevant == [
        (PipelineEvents.RESULT_READY, None),
        (LogEvents.EFFECT, Effects.WRITE),
        (RouterEvents.STORE_WRITE, None),
        (PipelineEvents.DEP_AVAILABLE, None),
        (LogEvents.EFFECT, Effects.DELETE),
        (RouterEvents.DELETE, None),
        (PipelineEvents.DEP_DELETED, None),
    ]

    effect_entries = [
        entry
        for entry in tide.ocean.event_logger.events
        if entry.type == LogEvents.EFFECT
    ]
    producer = next(
        record
        for record in trace.log
        if record.func_name == "prepare"
    )

    assert len(effect_entries) == 2
    assert all(
        entry.exec_id == producer.exec_id
        for entry in effect_entries
    )
    assert trace.inspect(
        (Effects.WRITE, ("global:prepared", 0))
    ).exists
    assert trace.inspect(
        (Effects.DELETE, ("global:prepared", 0))
    ).exists


def test_failed_runtime_read_does_not_invent_an_effect_handoff():
    @node(
        effects=[
            WRITE("global:ephemeral", "temporary"),
            DELETE("global:ephemeral"),
        ]
    )
    def create_then_delete():
        return None

    @pipeline
    def deletion_pipeline():
        return create_then_delete()

    def read_deleted():
        return READ("global:ephemeral")

    tide = TIDE()
    tide.ocean.run_pipeline(deletion_pipeline, mode="sequential")
    tide.exec(read_deleted)
    trace = tide.trace()

    effect_entries = [
        (index, entry)
        for index, entry in enumerate(tide.ocean.event_logger.events)
        if entry.type == LogEvents.EFFECT
    ]
    effect_indexes = {
        entry.payload.effect: index
        for index, entry in effect_entries
    }

    assert effect_indexes[Effects.WRITE] < effect_indexes[Effects.DELETE]
    assert effect_indexes[Effects.DELETE] < effect_indexes[Effects.READ]

    consumer = next(
        record
        for record in trace.execs.values()
        if record.func_name == "read_deleted"
    )
    read_effect = next(
        entry
        for _, entry in effect_entries
        if entry.payload.effect == Effects.READ
    )

    assert read_effect.exec_id == consumer.exec_id
    assert not trace.path(
        (Effects.DELETE, ("global:ephemeral", 0)),
        consumer.exec_id,
        "effect",
        "exec_record",
    ).exists()
    assert not trace.path(
        (Effects.WRITE, ("global:ephemeral", 0)),
        consumer.exec_id,
        "effect",
        "exec_record",
    ).exists()


def test_dynamic_concretisation_precedes_execution_and_maps_exact_paths():
    tide = TIDE()
    tide.event(
        "seed dynamic values",
        writes={
            "@alpha": "alpha",
            "@alpha_one": "alpha-one",
            "@beta": "beta",
        },
    )

    def consume_dynamic(values):
        return tuple(values)

    tide.exec(
        consume_dynamic,
        reads=["global:*alpha*:2:2"],
        writes="dynamic_result",
    )
    trace = tide.trace()

    concretised = [
        (index, entry)
        for index, entry in enumerate(tide.ocean.event_logger.events)
        if entry.type == LogEvents.DEP_CONCRETISED
    ]
    ready = _event_indexes(tide, ExecutorEvents.NODE_READY)

    assert len(concretised) == 1
    assert len(ready) == 1
    assert concretised[0][0] < ready[0]

    _, entry = concretised[0]
    input_name, dynamic_dep, concrete_refs, node_snapshot = entry.payload
    node_id = node_snapshot["node_id"]
    assert node_id == 0
    assert input_name == "args"
    assert dynamic_dep.dynamic
    assert {
        f"{ref.store}:{ref.key}"
        for ref in concrete_refs
    } == {
        "global:@alpha",
        "global:@alpha_one",
    }

    consumer = next(
        record
        for record in trace.log
        if record.func_name == "consume_dynamic"
    )
    assert len(consumer.deps) == 1
    dynamic_read = consumer.deps[0]
    assert dynamic_read.type == "dynamic"
    assert {
        expanded.unscoped_reference
        for expanded in dynamic_read.expanded_to
    } == {
        "global:@alpha",
        "global:@alpha_one",
    }

    for reference in ("global:@alpha", "global:@alpha_one"):
        assert trace.path(
            reference,
            "consume_dynamic",
            "reference",
            "actor",
        ).exists()

    assert not trace.path(
        "global:@beta",
        "consume_dynamic",
        "reference",
        "actor",
    ).exists()


def test_exec_record_uses_dispatch_evidence_not_reusable_node_state(
    monkeypatch,
):
    @node(targets=["global:consumed"])
    def consume(value):
        return value

    @pipeline
    def evidence_pipeline():
        return consume("@global:seed")

    tide = TIDE()
    tide.ocean.store_router.set("global", "seed", 7)
    original_create_trace = tide.ocean._trace_from_runner
    captured_meta = {}

    def mutate_reusable_node_before_reconstruction(runner):
        result_entry = next(
            entry
            for entry in tide.ocean.event_logger.events
            if entry.type == PipelineEvents.RESULT_READY
            and entry.target_runner_id == runner.id
        )
        execution_meta = result_entry.payload.exec_meta
        captured_meta["value"] = execution_meta

        reusable = runner.index.node_index.node_by_id[0]

        def corrupted_function():
            return "corrupted"

        reusable.func = corrupted_function
        reusable.args.raw = ("corrupted-raw",)
        reusable.args.evaluated = ("corrupted-evaluated",)
        reusable.args.normalised = ("corrupted-normalised",)
        reusable.args.resolved = ("corrupted-resolved",)
        reusable.deps.clear()
        reusable.targets.clear()
        reusable.dynamic_to_static_deps.clear()
        reusable.force_update = True

        return original_create_trace(runner)

    monkeypatch.setattr(
        tide.ocean,
        "_trace_from_runner",
        mutate_reusable_node_before_reconstruction,
    )

    trace = tide.ocean.run_pipeline(
        evidence_pipeline,
        mode="sequential",
    )
    record = next(
        entry
        for entry in trace.log
        if entry.func_name == "consume"
    )
    execution_meta = captured_meta["value"]

    assert execution_meta.node_snapshot is not None
    assert execution_meta.node_snapshot["func_name"] == "consume"
    assert record.func_name == "consume"
    assert record.inputs.args.raw == ("@global:seed",)
    assert record.inputs.args.evaluated == ("@global:seed",)
    assert record.inputs.args.resolved == (7,)
    assert [dep.unscoped_reference for dep in record.deps] == [
        "global:seed"
    ]
    assert record.declared_deps == [DataRef("seed", "global")]
    assert record.declared_targets == [DataRef("consumed", "global")]
    assert record.runtime_effects == []
    assert record.runtime_targets == [DataRef("consumed", "global")]
    assert record.node_options == {
        "force_update": False,
        "one_shot": False,
        "unpack": False,
        "share_implicit_target": False,
    }
    assert record.output_value == 7

    imported = TIDE.from_bundle(trace.to_bundle().to_dict())
    imported_record = next(
        entry
        for entry in imported.log
        if entry.func_name == "consume"
    )
    assert [
        dependency.unscoped_reference
        for dependency in imported_record.deps
    ] == ["global:seed"]
    assert imported_record.output_value == 7
    assert imported_record.declared_deps == [DataRef("seed", "global")]
    assert imported_record.declared_targets == [
        DataRef("consumed", "global")
    ]
    assert imported_record.runtime_targets == [
        DataRef("consumed", "global")
    ]
    assert imported_record.node_options == record.node_options
    assert imported.value_of("global:consumed") == [7]


def test_execution_log_rejects_exec_meta_without_dispatch_evidence():
    meta = ExecMeta(
        create_tide_exec_uuid(),
        result="completed-but-unverifiable",
        status="succeeded",
    )

    with pytest.raises(
        MissingExecutionEvidenceError,
        match="immutable execution evidence",
    ):
        ExecutionLog(0).log(
            0,
            exec_meta=meta,
            inputs=object(),
            dependencies=[],
        )


def test_dispatch_snapshot_copies_repeated_large_input_once(monkeypatch):
    import tide_tooling.tooling_core.engine.PipelineRunner as runner_module

    large_input = [{"index": index} for index in range(10_000)]
    dispatch_captures = []
    dispatch_sources = {}
    original_snapshot = runner_module.tide_snapshot_value

    def counted_snapshot(value, *args, **kwargs):
        boundary = kwargs.get("boundary", "unknown")
        if boundary.startswith("dispatch."):
            dispatch_captures.append((id(value), boundary))
            dispatch_sources[boundary] = value
        return original_snapshot(value, *args, **kwargs)

    monkeypatch.setattr(
        runner_module,
        "tide_snapshot_value",
        counted_snapshot,
    )

    @node(targets=["global:count"])
    def count_values(values):
        return len(values)

    @pipeline
    def flow():
        return count_values(large_input)

    tide = TIDE()
    tide.ocean.run_pipeline(flow, mode="sequential")
    result_event = next(
        entry
        for entry in tide.ocean.event_logger.events
        if entry.type == PipelineEvents.RESULT_READY
    )
    evidence = result_event.payload.exec_meta.node_snapshot
    captured_input = evidence["inputs"]["args"]
    captured_source = dispatch_sources["dispatch.args.raw"]

    assert sum(
        identity == id(captured_source)
        for identity, _ in dispatch_captures
    ) == 1
    raw_graph = captured_input["raw"].value_bundle
    evaluated_graph = captured_input["evaluated"].value_bundle
    assert raw_graph == evaluated_graph
    assert raw_graph is not evaluated_graph
    raw_graph["records"].clear()
    assert captured_input["raw"].value_bundle == evaluated_graph

    live_large = captured_source[0]
    frozen_large = captured_input["raw"].restore()[0]
    assert frozen_large is not live_large
    live_large[0]["index"] = "mutated-after-run"
    assert frozen_large[0]["index"] == 0


def test_end_instance_logs_effect_and_stops_downstream_execution():
    calls = []

    @node(
        targets=["global:stop_signal"],
        effects=END(),
    )
    def stop_here():
        calls.append("stop")
        return "stop"

    @node(targets=["global:too_late"])
    def should_not_run(value):
        calls.append("downstream")
        return value

    @pipeline
    def ending_pipeline():
        return should_not_run(stop_here())

    tide = TIDE()
    trace = tide.ocean.run_pipeline(ending_pipeline, mode="sequential")

    assert calls == ["stop"]
    assert [record.func_name for record in trace.log] == ["stop_here"]
    assert trace.find(Effects.END).exists("effect") == (True, False)

    effect_index = next(
        index
        for index, entry in enumerate(tide.ocean.event_logger.events)
        if entry.type == LogEvents.EFFECT
        and entry.payload.effect == Effects.END
    )
    kill_index = next(
        index
        for index, entry in enumerate(tide.ocean.event_logger.events)
        if entry.type == OceanEvents.KILL_RUNNER
    )
    effect_entry = tide.ocean.event_logger.events[effect_index]

    assert effect_index < kill_index
    assert effect_entry.exec_id is not None
    assert effect_entry.payload.targets == (0,)


def test_returning_end_class_stops_current_pipeline_without_storing_effect_class():
    @node(targets=["global:should_not_exist"])
    def stop_by_return():
        return END

    @pipeline
    def ending_pipeline():
        return stop_by_return()

    tide = TIDE()
    trace = tide.ocean.run_pipeline(ending_pipeline, mode="sequential")

    assert trace.find(Effects.END).exists("effect") == (True, False)
    assert trace.find("global:should_not_exist").exists("reference") == (
        False,
        False,
    )
    assert not tide.ocean.store_router.exists(
        "global",
        "should_not_exist",
    )


def test_run_effect_starts_registered_pipeline_and_records_parent_child_lifecycle():
    calls = []

    @node(targets=["global:child_output"])
    def child_work():
        calls.append("child")
        return "child-result"

    @pipeline
    def child_pipeline():
        return child_work()

    tide = TIDE()
    child = tide.ocean.register_pipeline(child_pipeline())

    @node(effects=RUN(child.id, mode="sequential"))
    def launch_child():
        calls.append("parent")
        return "launched"

    @pipeline
    def parent_pipeline():
        return launch_child()

    tide.ocean.run_pipeline(parent_pipeline, mode="sequential")

    assert calls == ["parent", "child"]
    assert len(tide.ocean.runners) == 2

    run_effect_index = next(
        index
        for index, entry in enumerate(tide.ocean.event_logger.events)
        if entry.type == LogEvents.EFFECT
        and entry.payload.effect == Effects.RUN
    )
    child_start_index = next(
        index
        for index, entry in enumerate(tide.ocean.event_logger.events)
        if index > run_effect_index
        and entry.type == OceanEvents.RUN_PIPELINE
        and entry.exec_id is not None
    )
    child_ready_index = next(
        index
        for index, entry in enumerate(tide.ocean.event_logger.events)
        if index > child_start_index
        and entry.type == ExecutorEvents.NODE_READY
        and entry.source_id == 1
    )

    assert run_effect_index < child_start_index < child_ready_index
    run_effect = tide.ocean.event_logger.events[run_effect_index]
    child_start = tide.ocean.event_logger.events[child_start_index]
    assert child_start.exec_id == run_effect.exec_id
    assert child_start.payload == PipelineRun(child.id, child.name, "sequential", {}, False, None, None)

    combined_trace = tide.ocean._trace_from_runner(
        tide.ocean.runners[0],
    )
    assert combined_trace.find(Effects.RUN).exists("effect") == (True, False)
    assert {
        record.func_name
        for record in combined_trace.log
    } == {
        "launch_child",
        "child_work",
    }

    child_display = combined_trace.show(color=False)
    full_display = combined_trace.show(color=False, full_trace=True)
    assert "child_work" in child_display and "launch_child" in child_display
    assert "child_work" in full_display and "launch_child" in full_display
    assert f"run -> {child.id!r}" in full_display


def test_emit_effect_registers_child_before_child_execution_and_maps_path():
    @node(targets=["global:emitted_output"])
    def emitted_child():
        return "emitted"

    @node
    def emitter():
        return EMIT(emitted_child.as_node())

    @pipeline
    def emitting_pipeline():
        return emitter()

    tide = TIDE()
    trace = tide.ocean.run_pipeline(emitting_pipeline, mode="sequential")

    emitted_index = next(
        index
        for index, entry in enumerate(tide.ocean.event_logger.events)
        if entry.type == LogEvents.NODE_EMITTED
    )
    child_ready_index = next(
        index
        for index, entry in enumerate(tide.ocean.event_logger.events)
        if index > emitted_index
        and entry.type == ExecutorEvents.NODE_READY
    )

    assert emitted_index < child_ready_index
    assert [record.func_name for record in trace.log] == [
        "emitter",
        "emitted_child",
    ]
    emitter_record, child_record = trace.log
    assert child_record.emitted_by_exec_id == emitter_record.exec_id
    assert trace.path(
        "emitter",
        "emitted_child",
        "actor",
        "actor",
    ).exists()
    assert trace.path(
        emitter_record.exec_id,
        child_record.exec_id,
        "exec_record",
        "exec_record",
    ).exists()
    assert trace.path(
        child_record.exec_id,
        emitter_record.exec_id,
        "exec_record",
        "exec_record",
        direction="upstream",
    ).exists()

    imported = TIDE.from_bundle(trace.to_bundle().to_dict())
    assert imported.path(
        "emitter",
        "emitted_child",
        "actor",
        "actor",
    ).exists()
    imported_emitter, imported_child = imported.log
    assert imported.path(
        imported_emitter.exec_id,
        imported_child.exec_id,
        "exec_record",
        "exec_record",
    ).exists()
    assert imported.path(
        imported_child.exec_id,
        imported_emitter.exec_id,
        "exec_record",
        "exec_record",
        direction="upstream",
    ).exists()


def test_returned_node_is_emitted_executed_once_and_maps_portably():
    calls = []

    @node(targets=["global:returned_emission"])
    def returned_child():
        calls.append("child")
        return "returned-child-result"

    @node
    def returning_emitter():
        calls.append("emitter")
        return returned_child.as_node()

    @pipeline
    def returning_emission_pipeline():
        return returning_emitter()

    tide = TIDE()
    trace = tide.ocean.run_pipeline(
        returning_emission_pipeline,
        mode="sequential",
    )

    emitted_events = [
        (index, entry)
        for index, entry in enumerate(tide.ocean.event_logger.events)
        if entry.type == LogEvents.NODE_EMITTED
    ]
    child_ready_index = next(
        index
        for index, entry in enumerate(tide.ocean.event_logger.events)
        if entry.type == ExecutorEvents.NODE_READY
        and entry.source_id == 0
        and index > emitted_events[0][0]
    )

    assert calls == ["emitter", "child"]
    assert len(emitted_events) == 1
    assert emitted_events[0][0] < child_ready_index
    assert [record.func_name for record in trace.log] == [
        "returning_emitter",
        "returned_child",
    ]

    emitter_record, child_record = trace.log
    assert len(emitter_record.emitted_nodes) == 1
    assert emitter_record.emitted_nodes[0].func_name == "returned_child"
    assert emitter_record.emitted_nodes[0].declared_targets == [
        DataRef("returned_emission", "global")
    ]
    assert trace.value_of("global:returned_emission") == [
        "returned-child-result"
    ]
    assert trace.path(
        "returning_emitter",
        "returned_child",
        "actor",
        "actor",
    ).exists()

    imported = TIDE.from_bundle(trace.to_bundle().to_dict())
    imported_emission = imported.log[0].emitted_nodes[0]
    assert imported_emission.func_name == "returned_child"
    assert imported_emission.declared_targets == [
        DataRef("returned_emission", "global")
    ]
    assert imported.value_of("global:returned_emission") == [
        "returned-child-result"
    ]
    assert imported.path(
        "returning_emitter",
        "returned_child",
        "actor",
        "actor",
    ).exists()


def test_node_lists_work_for_returned_and_explicit_emissions():
    def run_list_emission(explicit):
        calls = []

        @node(targets=["global:first_emitted"])
        def first_emitted():
            calls.append("first")
            return "first-result"

        @node(targets=["global:second_emitted"])
        def second_emitted():
            calls.append("second")
            return "second-result"

        @node
        def list_emitter():
            calls.append("emitter")
            emitted = [
                first_emitted.as_node(),
                second_emitted.as_node(),
            ]
            return EMIT(emitted) if explicit else emitted

        @pipeline
        def list_emission_pipeline():
            return list_emitter()

        tide = TIDE()
        trace = tide.ocean.run_pipeline(
            list_emission_pipeline,
            mode="sequential",
        )
        return tide, trace, calls

    for explicit in (False, True):
        tide, trace, calls = run_list_emission(explicit)

        assert calls[0] == "emitter"
        assert set(calls[1:]) == {"first", "second"}
        assert trace.log[0].func_name == "list_emitter"
        assert {
            record.func_name
            for record in trace.log[1:]
        } == {"first_emitted", "second_emitted"}

        emitted_events = [
            (index, entry)
            for index, entry in enumerate(tide.ocean.event_logger.events)
            if entry.type == LogEvents.NODE_EMITTED
        ]
        assert len(emitted_events) == 1
        assert len(emitted_events[0][1].payload.emitted_node_snapshots) == 2

        emitter_record = trace.log[0]
        children = trace.log[1:]
        assert [
            emitted.func_name
            for emitted in emitter_record.emitted_nodes
        ] == ["first_emitted", "second_emitted"]
        assert trace.value_of("global:first_emitted") == [
            "first-result"
        ]
        assert trace.value_of("global:second_emitted") == [
            "second-result"
        ]

        for child_name in ("first_emitted", "second_emitted"):
            assert trace.path(
                "list_emitter",
                child_name,
                "actor",
                "actor",
            ).exists()

        imported = TIDE.from_bundle(trace.to_bundle().to_dict())
        for child_name in ("first_emitted", "second_emitted"):
            assert imported.path(
                "list_emitter",
                child_name,
                "actor",
                "actor",
            ).exists()


def test_emission_occurrences_preserve_declared_then_runtime_order():
    @node(targets=["global:declared_first"])
    def declared_first():
        return "declared-first"

    @node(targets=["global:declared_second"])
    def declared_second():
        return "declared-second"

    @node(targets=["global:runtime_child"])
    def runtime_child():
        return "runtime-child"

    @node(effects=[
        EMIT(declared_first.as_node()),
        EMIT(declared_second.as_node()),
    ])
    def emitter():
        return runtime_child.as_node()

    @pipeline
    def flow():
        return emitter()

    trace = TIDE().ocean.run_pipeline(flow, mode="sequential")
    occurrences = trace.log[0].emitted_nodes

    assert [record.func_name for record in occurrences] == [
        "declared_first",
        "declared_second",
        "runtime_child",
    ]
    assert [record.origin for record in occurrences] == [
        "declared",
        "declared",
        "runtime",
    ]

    imported = TIDE.from_bundle(trace.to_bundle())
    assert [
        (record.func_name, record.origin)
        for record in imported.log[0].emitted_nodes
    ] == [
        ("declared_first", "declared"),
        ("declared_second", "declared"),
        ("runtime_child", "runtime"),
    ]


def test_repeated_emission_is_two_occurrences_but_one_dag_node():
    calls = []

    @node(targets=["global:child"])
    def child():
        calls.append("child")
        return "child"

    @node
    def emitter():
        emitted = child.as_node()
        return EMIT([emitted, emitted])

    @pipeline
    def flow():
        return emitter()

    trace = TIDE().ocean.run_pipeline(flow, mode="sequential")
    occurrences = trace.log[0].emitted_nodes

    assert calls == ["child"]
    assert len(occurrences) == 2
    assert [record.node_id for record in occurrences] == [
        occurrences[0].node_id,
        occurrences[0].node_id,
    ]
    assert [record.origin for record in occurrences] == [
        "runtime",
        "runtime",
    ]

    imported = TIDE.from_bundle(trace.to_bundle().to_dict())
    imported_occurrences = imported.log[0].emitted_nodes
    assert len(imported_occurrences) == 2
    assert [record.node_id for record in imported_occurrences] == [
        imported_occurrences[0].node_id,
        imported_occurrences[0].node_id,
    ]


def test_each_emitter_keeps_its_own_relationship_to_a_reused_node():
    @node(targets=["global:shared_child"])
    def shared_child():
        return "shared-child"

    @node
    def first_emitter():
        return shared_child.as_node()

    @node
    def second_emitter():
        return shared_child.as_node()

    @pipeline
    def flow():
        return [first_emitter(), second_emitter()]

    trace = TIDE().ocean.run_pipeline(flow, mode="sequential")

    for emitter in ("first_emitter", "second_emitter"):
        assert trace.path(
            emitter,
            "shared_child",
            "actor",
            "actor",
        ).exists()

    first_record, second_record, child_record = trace.log
    assert child_record.emitted_by_exec_id == first_record.exec_id
    second_path = trace.path(
        second_record.exec_id,
        child_record.producer_id,
        "exec_record",
        "actor",
    ).get_path()
    assert second_path["executions"] == [second_record.exec_id]
    assert second_path["handoffs"] == []
    assert len(second_path["endpoint_evidence"]) == 1

    shown = trace.show(color=False)
    assert "emit -> shared_child" in shown
    assert "emitted:" in shown
    assert "shared_child - 1 execution" in shown
    assert f"shared_child:{trace.log[0].emitted_nodes[0].node_id}" not in shown
    assert "shared_child (registered by EXEC_RECORD[0])" in shown


def test_tide_event_does_not_turn_emit_into_a_runner_effect():
    @node
    def child():
        return None

    tide = TIDE()

    with pytest.raises(TypeError, match="Unsupported TIDE event effect"):
        tide.event("external emitter", effects=EMIT(child.as_node()))


def test_emitted_definition_uses_emission_snapshot_not_live_node_state(
    monkeypatch,
):
    @node(targets=["global:child_output"])
    def child(value):
        return value

    @node
    def emitter():
        return EMIT(child.as_node("@global:seed"))

    @pipeline
    def flow():
        return emitter()

    tide = TIDE()
    tide.ocean.store_router.set("global", "seed", "seed-value")
    original_create_trace = tide.ocean._trace_from_runner

    def mutate_child_before_reconstruction(runner):
        emitted_event = next(
            entry
            for entry in tide.ocean.event_logger.events
            if entry.type == LogEvents.NODE_EMITTED
        )
        emitted_id = emitted_event.payload.emitted_node_snapshots[0]["node_id"]
        reusable = runner.index.node_index.node_by_id[emitted_id]

        def corrupted():
            return "corrupted"

        reusable.func = corrupted
        reusable.args.raw = ("corrupted",)
        reusable.deps.clear()
        reusable.targets.clear()
        reusable.effects.clear()
        return original_create_trace(runner)

    monkeypatch.setattr(
        tide.ocean,
        "_trace_from_runner",
        mutate_child_before_reconstruction,
    )

    trace = tide.ocean.run_pipeline(flow, mode="sequential")
    emitted = trace.log[0].emitted_nodes[0]

    assert emitted.func_name == "child"
    assert emitted.inputs.args.raw == ("@global:seed",)
    assert [
        DataRef.to_string(dependency, prefix=False)
        for dependency in emitted.declared_deps
    ] == [
        "global:seed"
    ]
    assert emitted.declared_targets == [
        DataRef("child_output", "global")
    ]


def test_execution_id_is_canonical_across_ready_read_result_and_write_events():
    @node(targets=["global:source"])
    def source():
        return 1

    @node(targets=["global:result"])
    def consume(value):
        return value + 1

    @pipeline
    def flow():
        return consume(source())

    tide = TIDE()
    trace = tide.ocean.run_pipeline(flow, mode="sequential")
    consume_record = next(
        record for record in trace.log
        if record.func_name == "consume"
    )
    related_ids = {
        entry.exec_id
        for entry in tide.ocean.event_logger.events
        if entry.type in {
            ExecutorEvents.NODE_READY,
            PipelineEvents.RESULT_READY,
            RouterEvents.STORE_WRITE,
        }
        and entry.exec_id == consume_record.exec_id
    }

    assert related_ids == {consume_record.exec_id}
    assert _is_tide_exec_uuid(consume_record.exec_id)


def test_read_receipts_preserve_repeated_nested_input_paths():
    @node(targets=["global:source"])
    def source():
        return {"value": 1}

    @node(targets=["global:result"])
    def consume(payload):
        return payload

    @pipeline
    def flow():
        value = source()
        return consume([value, {"again": value}])

    trace = TIDE().ocean.run_pipeline(flow, mode="sequential")
    consume_record = next(
        record for record in trace.log
        if record.func_name == "consume"
    )
    reads = [
        dependency
        for dependency in consume_record.deps
        if dependency.type == "static"
        and dependency.unscoped_reference == "global:source"
    ]

    assert len(reads) == 2
    assert [read.input_path for read in reads] == [
        (0, 0),
        (0, 1, "again"),
    ]
    assert all(read.resolved_version == 0 for read in reads)


def test_in_place_input_mutation_is_attributed_to_consumer_execution():
    @node(targets=["global:data"])
    def source():
        return {"items": [1]}

    @node(targets=["global:done"])
    def mutate(value):
        value["items"].append(2)
        return True

    @pipeline
    def flow():
        return mutate(source())

    trace = TIDE().ocean.run_pipeline(flow, mode="sequential")
    source_record, mutation_record = trace.log
    read = mutation_record.deps[0]
    assert source_record.writes[0].stored_result == {"items": [1]}
    assert read.resolved_value == {"items": [1]}
    assert read.resolved_version == 0
    assert read.observed_mutable is True
    assert [
        entry["boundary"]
        for entry in read.snapshot_lineage
    ] == ["read", "post_execution"]
    assert not any(write.stored_target == "global:data" for write in mutation_record.writes)

    imported = TIDE.from_bundle(trace.to_bundle().to_dict())
    imported_mutation = imported.log[1]
    imported_read = imported_mutation.deps[0]
    assert imported_read.input_path == (0,)
    assert imported_read.observed_mutable is True
    assert [
        {key: value for key, value in entry.items() if key != "trusted"}
        for entry in imported_read.snapshot_lineage
    ] == [
        {key: value for key, value in entry.items() if key != "trusted"}
        for entry in read.snapshot_lineage
    ]
    assert [
        entry["trusted"]
        for entry in imported_read.snapshot_lineage
    ] == [False, False]
    assert not any(write.stored_target == "global:data" for write in imported_mutation.writes)


def test_result_to_write_mutation_is_preserved_in_write_lineage(monkeypatch):
    original_store_result = PipelineRunner._store_node_result

    def mutate_before_write(self, node, *args, **kwargs):
        if isinstance(node.result, dict):
            node.result["items"].append(2)
        return original_store_result(self, node, *args, **kwargs)

    monkeypatch.setattr(
        PipelineRunner,
        "_store_node_result",
        mutate_before_write,
    )

    @node(targets=["global:data"])
    def source():
        return {"items": [1]}

    @pipeline
    def flow():
        return source()

    trace = TIDE().ocean.run_pipeline(flow, mode="sequential")
    write = trace.log[0].writes[0]

    assert write.stored_result == {"items": [1, 2]}
    assert write.observed_mutable is True
    assert [
        entry["boundary"]
        for entry in write.snapshot_lineage
    ] == ["result", "write"]


def test_external_mutation_between_write_and_read_remains_a_use_observation():
    value = {"items": [1]}
    tide = TIDE()
    tide.declare("data", value)

    value["items"].append(2)
    tide.exec(
        lambda observed: len(observed["items"]),
        reads="data",
        writes="count",
    )
    trace = tide.trace()
    declared, consumer = trace.log

    assert declared.writes[0].stored_result == {"items": [1]}
    assert consumer.deps[0].resolved_value == {"items": [1, 2]}
    assert consumer.deps[0].resolved_version == 0
    assert consumer.deps[0].producer_exec_id == declared.exec_id
    assert consumer.deps[0].observed_mutable is True
    assert [
        entry["boundary"]
        for entry in consumer.deps[0].snapshot_lineage
    ] == ["write", "read"]

    used = trace.used_as("global:data[v0]")
    assert trace.value_of("global:data", 0) == {"items": [1]}
    assert used.values == ({"items": [1, 2]},)
    assert used.uses[0]["observed"]["differs_from_stored"] is True


def test_hash_only_snapshot_becomes_opaque_evidence_not_hash_string():
    class HashOnly:
        def __deepcopy__(self, memo):
            raise TypeError("cannot copy")

        def __getstate__(self):
            raise TypeError("cannot serialise")

        def __repr__(self):
            return "<HashOnly>"

    @node(targets=["global:value"])
    def source():
        return HashOnly()

    @node(targets=["global:type_name"])
    def consume(value):
        return type(value).__name__

    @pipeline
    def flow():
        return consume(source())

    trace = TIDE().ocean.run_pipeline(flow, mode="sequential")
    write_value = trace.log[0].writes[0].stored_result
    read_value = trace.log[1].deps[0].resolved_value

    assert isinstance(write_value, TIDEOpaqueValue)
    assert isinstance(read_value, TIDEOpaqueValue)
    assert write_value.value_hash == read_value.value_hash
    assert not isinstance(write_value, str)

    imported = TIDE.from_bundle(trace.to_bundle().to_dict())
    imported_value = imported.log[0].writes[0].stored_result
    assert isinstance(imported_value, TIDEOpaqueValue)
    assert imported_value.value_hash == write_value.value_hash


def test_plain_primary_inputs_become_untracked_external_states():
    @node(targets=["global:result"])
    def consume(left, payload=None):
        return left + payload["value"]

    @pipeline
    def flow():
        return consume(2, payload={"value": 3})

    trace = TIDE().ocean.run_pipeline(flow, mode="sequential")
    observers = [
        record
        for record in trace.log
        if record.func_name == "TIDE: External"
    ]
    consumer = next(
        record for record in trace.log
        if record.func_name == "consume"
    )

    assert len(observers) == 2
    assert len(consumer.deps) == 2
    assert [dependency.input_path for dependency in consumer.deps] == [
        (0,),
        ("payload",),
    ]
    assert all(
        dependency.unscoped_reference.startswith("__untracked___")
        for dependency in consumer.deps
    )
    assert {
        dependency.producer_exec_id
        for dependency in consumer.deps
    } == {
        observer.exec_id
        for observer in observers
    }
    assert all(
        dependency.producer_actor.source_actor == "TIDE: External"
        for dependency in consumer.deps
    )
    assert all(
        observer.writes[0].cause.event_id == trace.pipeline_starting_event.event_id
        for observer in observers
    )


def test_untracked_external_write_precedes_read_and_node_execution():
    @node(targets=["global:result"])
    def consume(value):
        return value

    @pipeline
    def flow():
        return consume(3)

    tide = TIDE()
    tide.ocean.run_pipeline(flow, mode="sequential")
    event_types = _event_types(tide)

    observer_write = event_types.index(RouterEvents.STORE_WRITE)
    node_ready = event_types.index(ExecutorEvents.NODE_READY)
    assert observer_write < node_ready


def test_equal_untracked_values_at_distinct_positions_keep_distinct_identity():
    @node(targets=["global:result"])
    def consume(first, second):
        return first + second

    @pipeline
    def flow():
        return consume(7, 7)

    trace = TIDE().ocean.run_pipeline(flow, mode="sequential")
    consumer = next(
        record for record in trace.log
        if record.func_name == "consume"
    )

    assert [dependency.input_path for dependency in consumer.deps] == [
        (0,),
        (1,),
    ]
    assert len({
        dependency.unscoped_reference
        for dependency in consumer.deps
    }) == 2
    assert len({
        dependency.producer_exec_id
        for dependency in consumer.deps
    }) == 2
    assert [dependency.resolved_value for dependency in consumer.deps] == [
        7,
        7,
    ]


def test_fully_represented_input_does_not_create_untracked_children():
    @node(targets=["global:source"])
    def source():
        return {"items": [1, 2]}

    @node(targets=["global:result"])
    def consume(value):
        return len(value["items"])

    @pipeline
    def flow():
        return consume(source())

    trace = TIDE().ocean.run_pipeline(flow, mode="sequential")
    assert not any(
        record.func_name == "TIDE: External"
        for record in trace.log
    )
    consumer = next(
        record for record in trace.log
        if record.func_name == "consume"
    )
    assert len(consumer.deps) == 1
    assert consumer.deps[0].unscoped_reference == "global:source"


def test_mixed_nested_input_tracks_whole_input_and_child_dependencies():
    @node(targets=["global:source"])
    def source():
        return {"represented": True}

    @node(targets=["global:result"])
    def consume(payload):
        return payload

    @pipeline
    def flow():
        represented = source()
        return consume([
            represented,
            {"plain": [1, 2]},
            represented,
        ])

    trace = TIDE().ocean.run_pipeline(flow, mode="sequential")
    consumer = next(
        record for record in trace.log
        if record.func_name == "consume"
    )

    assert [
        dependency.input_path
        for dependency in consumer.deps
    ] == [
        (0, 0),
        (0, 2),
        (0,),
    ]
    assert any(
        record.func_name == "TIDE: External"
        for record in trace.log
    )


def test_untracked_input_mutation_creates_a_consumer_attributed_version(capsys):
    @node(targets=["global:result"])
    def mutate(payload):
        payload["items"].append(2)
        return True

    @pipeline
    def flow():
        return mutate({"items": [1]})

    trace = TIDE().ocean.run_pipeline(flow, mode="sequential")
    observer, consumer = trace.log
    dependency = consumer.deps[0]
    assert observer.writes[0].stored_result == {"items": [1]}
    assert dependency.resolved_value == {"items": [1]}
    assert dependency.observed_mutable is True
    assert [
        entry["boundary"]
        for entry in dependency.snapshot_lineage
    ] == ["read", "post_execution"]
    assert consumer.mutated_during == {
        dependency.unscoped_reference: ["args"],
    }
    assert f"mutated: {dependency.unscoped_reference}" in trace.show(color=False)
    capsys.readouterr()

    model = trace.to_model(enrich=True)
    assert repr(consumer.producer_occurrence_id) in model["executions"]
    round_tripped_model = TIDE.from_model(model).to_model(
        enrich=True,
    )
    assert round_tripped_model["executions"] == model["executions"]

    imported = TIDE.from_bundle(trace.to_bundle().to_dict())
    imported_consumer = next(
        record for record in imported.log
        if record.func_name == "mutate"
    )
    assert imported_consumer.mutated_during == consumer.mutated_during
    assert not any(write.stored_target == dependency.unscoped_reference for write in consumer.writes)


def test_untracked_state_path_and_bundle_round_trip_preserve_occurrence():
    @node(targets=["global:result"])
    def consume(payload):
        return len(payload)

    @pipeline
    def flow():
        return consume(["same", "same"])

    trace = TIDE().ocean.run_pipeline(flow, mode="sequential")
    consumer = next(
        record for record in trace.log
        if record.func_name == "consume"
    )
    dependency = consumer.deps[0]

    assert dependency.input_path == (0,)
    assert trace.path(
        dependency.unscoped_reference,
        consumer.exec_id,
        "reference",
        "exec_record",
    ).exists()

    imported = TIDE.from_bundle(trace.to_bundle().to_dict())
    imported_consumer = next(
        record for record in imported.log
        if record.func_name == "consume"
    )
    imported_dependency = imported_consumer.deps[0]

    assert imported_dependency.input_path == (0,)
    assert (
        imported_dependency.unscoped_reference
        == dependency.unscoped_reference
    )
    assert (
        imported_dependency.producer_exec_id
        == dependency.producer_exec_id
    )
    assert imported.path(
        imported_dependency.unscoped_reference,
        imported_consumer.exec_id,
        "reference",
        "exec_record",
    ).exists()


def test_hash_only_plain_input_is_opaque_evidence_but_runtime_is_unchanged():
    class HashOnly:
        def __deepcopy__(self, memo):
            raise TypeError("cannot copy")

        def __getstate__(self):
            raise TypeError("cannot serialise")

        def __repr__(self):
            return "<HashOnlyInput>"

    original = HashOnly()

    @node(targets=["global:result"])
    def consume(value):
        return value is original

    @pipeline
    def flow():
        return consume(original)

    trace = TIDE().ocean.run_pipeline(flow, mode="sequential")
    observer, consumer = trace.log

    assert consumer.writes[0].stored_result is True
    assert isinstance(observer.writes[0].stored_result, TIDEOpaqueValue)
    assert isinstance(consumer.deps[0].resolved_value, TIDEOpaqueValue)
    assert (
        observer.writes[0].stored_result.value_hash
        == consumer.deps[0].resolved_value.value_hash
    )

    imported = TIDE.from_bundle(trace.to_bundle().to_dict())
    imported_observer = imported.log[0]
    assert isinstance(
        imported_observer.writes[0].stored_result,
        TIDEOpaqueValue,
    )


def test_static_reference_cycle_fails_before_any_node_executes():
    first = Node(
        lambda value: value,
        args="@global:second",
        targets="global:first",
        node_name="first",
    )
    second = Node(
        lambda value: value,
        args="@global:first",
        targets="global:second",
        node_name="second",
    )
    tide = TIDE(timeout=1)

    trace = tide.ocean.run_pipeline(
        Pipeline([first, second], name="cycle"),
        mode="sequential",
    )

    runner = next(iter(tide.ocean.runners.values()))
    assert trace.log == ()
    assert isinstance(runner.deadlock, PipelineDeadlockError)
    assert "unresolved dependencies" in str(runner.deadlock)
    assert trace.deadlocked is True
    assert not tide.ocean.executor.running_tasks
    tide.ocean.close()


def test_read_then_rewrite_same_reference_is_not_a_static_cycle():
    update = Node(
        lambda value: value + 1,
        args="@global:state",
        targets="global:state",
        node_name="update",
        one_shot=True,
    )
    tide = TIDE(timeout=1)
    registered = tide.ocean.register_pipeline(
        Pipeline([update], name="update")
    )
    runner = tide.ocean._create_pipeline_runner(
        registered,
        "sequential",
        {},
    )

    indexed = runner.index.node_index.node_by_definition_id[update.id]
    assert runner.runtime_dag.forward_map[indexed.id]
    tide.ocean.close()


def test_unresolved_external_reference_records_actionable_deadlock():
    waiting = Node(
        lambda value: value,
        args="@global:missing",
        targets="global:result",
        node_name="waiting",
    )
    tide = TIDE(timeout=1)

    trace = tide.ocean.run_pipeline(
        Pipeline([waiting], name="deadlock"),
        mode="sequential",
    )

    runner = next(iter(tide.ocean.runners.values()))
    assert trace.log == ()
    assert isinstance(runner.deadlock, PipelineDeadlockError)
    assert "global:missing" in str(runner.deadlock)
    assert trace.deadlocked is True

    tide.ocean.close()


def test_ocean_timeout_is_configurable_per_tide_instance():
    @node(targets=["global:result"])
    def slow():
        time.sleep(0.05)
        return "done"

    @pipeline
    def flow():
        return slow()

    tide = TIDE(timeout=0.001)
    with pytest.raises(TimeoutError, match="pipeline evidence"):
        tide.ocean.run_pipeline(flow, mode="thread")
    tide.ocean.close()


def test_ocean_context_manager_closes_executor_pools_idempotently():
    @node(targets=["global:result"])
    def produce():
        return "done"

    @pipeline
    def flow():
        return produce()

    tide = TIDE(timeout=1)
    with tide.ocean:
        trace = tide.ocean.run_pipeline(flow, mode="sequential")
        assert trace.value_of("global:result", 0) == "done"

    assert tide.ocean.closed is True
    assert tide.ocean.executor._closed is True
    tide.ocean.close()
