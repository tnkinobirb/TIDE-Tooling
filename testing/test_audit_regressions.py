import pytest
from types import SimpleNamespace
from tide_tooling.tooling_core.engine.DependencyTools import DependencyTools
from tide_tooling.tooling_core.engine.NodeProcessor import NodeProcessor
from tide_tooling.tooling_core.engine.classes import (
    DataRef,
    Node,
    Pipeline,
    ProcessStates,
    RunnerNode,
)
from tide_tooling.tooling_core.engine.errors import (
    EventHandlerError,
    StoreReferenceAmbiguityError,
)
from tide_tooling.tooling_core.engine.event_payloads import (
    EffectOccurrence,
    Nodes,
    PipelineRun,
    Reference,
    State,
)
from tide_tooling.tooling_core.engine.event_occurrences import EventLogger
from tide_tooling.tooling_core.engine.executor import _execute_with_timing
from tide_tooling.tooling_core.engine.events import (
    Cause,
    Event,
    Effects,
    LogEvents,
    OceanEvents,
    PipelineEvents,
    RouterEvents,
)
from tide_tooling.tooling_core.engine.utils import (
    create_tide_exec_uuid,
    tide_snapshot_value,
    tide_value_hasher,
)
from tide_tooling.tooling_core.engine.registries import StoreRegistry
from tide_tooling.tooling_core.engine.routers import EventRouter, StoreRouter
from tide_tooling.tooling_core.engine.stores import MemoryKVStore, StoreTemplate
from tide_tooling.tooling_core.engine.tide_rendering import display_identity
from tide_tooling.tooling_core.engine.tide_logging import (
    Actor,
    DependencyUseRecord,
    ExecRecord,
    ExpandedDepRecord,
    InputUseRecord,
    Inputs,
    WriteRecord,
)
from tide_tooling.tooling_core.engine.tide_runtime_logging import ExecutionLog
from tide_tooling import TIDE


def test_display_identity_never_changes_the_full_identity():
    identity = "execution-" + ("0123456789" * 8)

    assert display_identity(identity, full_ids=True) == identity

    shortened = display_identity(identity, full_ids=False, max_length=30)
    assert len(shortened) == 30
    assert shortened.startswith("execution-")
    assert shortened.endswith(identity[-8:])
    assert "\u2026" in shortened


def test_tide_does_not_allocate_a_process_pool_until_process_mode_is_used():
    tide = TIDE()
    try:
        assert tide.ocean.executor.process_pool_executor is None
    finally:
        tide.ocean.close()


def test_executor_preserves_completed_runtime_effects_on_ordinary_failure():
    from tide_tooling.tooling_core.engine.classes import EffectRuntime, _current_effect_runtime

    effect = {"effect": "observed-before-failure"}
    runtime = EffectRuntime(exec_id=create_tide_exec_uuid())

    def fail_after_effect():
        _current_effect_runtime.get().effects.append(effect)
        raise RuntimeError("ordinary failure")

    meta = _execute_with_timing(runtime.exec_id, fail_after_effect, (), {}, runtime)

    assert meta.status == "failed"
    assert isinstance(meta.error, RuntimeError)
    assert meta.runtime_effects == (effect,)


def make_exec_record(*, exec_id, effects=None, writes=None):
    return ExecRecord(
        seq_id=0,
        exec_id=exec_id,
        ocean_id="audit",
        runner_id="source",
        pipeline_id="pipeline",
        node_id="source",
        func=lambda: None,
        func_name="source",
        inputs={},
        deps=[],
        effects=effects or [],
        declared_effects=[],
        declared_targets=[],
        runtime_effects=[],
        runtime_targets=[],
        declared_deps=[],
        unscoped_store_refs=[],
        status="complete",
        started_at=None,
        finished_at=None,
        duration_ms=0,
        error=None,
        node_options={},
        writes=writes or [],
    )


def test_exec_record_prefix_must_begin_the_identifier():
    assert make_exec_record(exec_id="TIDE_EXEC:valid").exec_id == "TIDE_EXEC:valid"
    assert make_exec_record(exec_id="wrong:TIDE_EXEC:embedded").exec_id == "TIDE_EXEC:wrong:TIDE_EXEC:embedded"


def test_runner_node_keeps_only_direct_execution_state():
    runner = RunnerNode(Node(func=lambda: None), id=0)

    assert runner.executing is False
    assert runner.evaluated is False
    assert runner.status is None
    assert not hasattr(runner, "state")
    assert not hasattr(runner, "defer_flags")


def test_runner_node_unresolve_invalidates_result_and_input():
    runner = RunnerNode(Node(func=lambda: None), id=0)
    runner.args.update_state(ProcessStates.EVALUATED, ("evaluated",))
    runner.args.update_state(ProcessStates.NORMALISED, ("normalised",))
    runner.args.update_state(ProcessStates.RESOLVED, ("resolved",))
    runner.evaluated = True
    runner.result = "result"

    runner._unresolve_input(runner.args)

    assert runner.evaluated is False
    assert runner.result is None
    assert not runner.args.has_been(ProcessStates.RESOLVED)


def test_runtime_reads_retain_selected_dynamic_expansions():
    input_record = InputUseRecord("args", None, None, None, None, [])
    selected = object()
    dependency = DependencyUseRecord(
        ("args", input_record),
        DataRef(key="x*", store="mem"),
        type="dynamic",
        expanded_to=[selected],
    )
    input_record.dependencies.append(dependency)

    ExecutionLog("ocean").append_runtime_reads(
        Inputs({"args": input_record}),
        [dependency],
        [],
    )

    assert dependency.expanded_to == [selected]


def test_inputs_repr_lists_only_input_slots_with_evidence():
    empty = Inputs({})
    used = Inputs({
        "args": InputUseRecord(
            "args",
            ("raw",),
            ("evaluated",),
            ("normalised",),
            ("resolved",),
            [],
        ),
    })
    runtime = Inputs({})
    runtime.runtime_read = InputUseRecord(
        "runtime_read",
        (),
        (),
        (),
        (),
        [DataRef(key="@A", store="global")],
    )

    assert repr(empty) == "Inputs([])"
    assert repr(used) == "Inputs(['args'])"
    assert repr(runtime) == "Inputs(['runtime_read'])"


def test_dependency_resolver_preserves_the_original_error():
    def reject_reference(_):
        raise TypeError("resolver exploded")

    processor = NodeProcessor(
        SimpleNamespace(resolve_exact=reject_reference),
    )

    with pytest.raises(TypeError, match="resolver exploded"):
        processor._resolve_dependencies(
            DataRef(key="@A", store="global"),
            lambda ref: ref,
            node=None,
        )


def test_dynamic_resolver_uses_the_refs_selected_for_the_node():
    dynamic = DataRef(key="@item*", store="global", max=1)
    concrete = DataRef(key="@item_one", store="global")
    node = SimpleNamespace(
        dynamic_to_static_deps={dynamic: [concrete]},
    )
    processor = NodeProcessor(
        SimpleNamespace(
            matching_refs=lambda ref: (concrete,),
            resolve_exact=lambda ref: "resolved",
        ),
    )

    resolved, reads = processor._resolve_dependencies(
        dynamic,
        lambda ref: ref,
        node,
    )

    assert resolved == ["resolved"]
    assert len(reads) == 1
    assert reads[0][0] == concrete

def test_store_registry_reports_local_global_name_ambiguity_directly():
    registry = StoreRegistry()
    registry._locals["same"] = object()
    registry._globals["same"] = object()

    with pytest.raises(
        StoreReferenceAmbiguityError,
        match="registered as both local and global",
    ):
            registry.get("same")


def test_store_router_preserves_runner_zero_when_routing_delete():
    emitted = []
    registry = StoreRegistry()
    registry.register(MemoryKVStore(
        name="mem_0",
        store_template={"key": "value"},
        scope="local",
        parent_runner_id=0,
    ))
    router = StoreRouter(
        registry,
        event_interface=SimpleNamespace(
            emit_event_to_ocean=emitted.append,
            event_subscriber=lambda *args: None,
        ),
    )

    delete_event = Event(ocean_id=0, 
        source="runner",
        source_id=9,
        target_runner_id=0,
        type=RouterEvents.DELETE,
        cause=Cause(description="test"),
        payload=Reference(DataRef(key="key", store="mem_0")),
        exec_id="execution",
    )
    router.on_delete(delete_event)

    assert registry.get("mem_0").exists("key") is False
    assert len(emitted) == 1
    assert emitted[0].target_runner_id == 0
    assert emitted[0].type == PipelineEvents.DEP_DELETED
    assert emitted[0].cause.event_id == delete_event.event_id
    assert emitted[0].payload == delete_event.payload
    assert emitted[0].exec_id == delete_event.exec_id


@pytest.mark.parametrize(
    ("existing", "value", "force", "expected_type"),
    [
        (None, "new", False, PipelineEvents.DEP_AVAILABLE),
        ("old", "new", False, PipelineEvents.DEP_UPDATED),
        ("same", "same", False, PipelineEvents.DEP_AVAILABLE),
        ("same", "same", True, PipelineEvents.DEP_UPDATED),
    ],
)
def test_store_router_write_outcome_matrix(existing, value, force, expected_type):
    emitted = []
    initial = {} if existing is None else {"key": existing}
    registry = StoreRegistry()
    registry.register(MemoryKVStore(name="global", store_template=initial, scope="global"))
    router = StoreRouter(registry, SimpleNamespace(
        emit_event_to_ocean=emitted.append,
        event_subscriber=lambda *args: None,
    ))
    state = State(DataRef("key", "global"), value, tide_snapshot_value(value, boundary="write"))
    event_type = RouterEvents.FORCE_WRITE if force else RouterEvents.STORE_WRITE
    event = Event(source="runner", source_id=3, type=event_type, cause=Cause(description="test"), ocean_id=0, target_runner_id=3, payload=state, exec_id="execution")

    if force:
        router.on_force_write(event)
    else:
        router.on_store_write(event)

    assert registry.get("global").get("key") == value
    assert len(emitted) == 1
    assert emitted[0].type == expected_type
    assert emitted[0].cause.event_id == event.event_id
    assert emitted[0].target_runner_id == "*"
    # The consequence is a new occurrence captured at the Store mutation
    # turn, not an alias of the earlier queued request payload.
    assert emitted[0].payload is not state
    assert emitted[0].payload.ref == state.ref
    assert emitted[0].payload.value == state.value
    assert (
        emitted[0].payload.snapshot.value_hash
        == tide_value_hasher(value)
    )
    assert emitted[0].exec_id == event.exec_id


def test_store_router_missing_delete_emits_no_dependency_event():
    emitted = []
    registry = StoreRegistry()
    registry.register(MemoryKVStore(name="global", scope="global"))
    router = StoreRouter(registry, SimpleNamespace(
        emit_event_to_ocean=emitted.append,
        event_subscriber=lambda *args: None,
    ))
    event = Event(source="runner", source_id=3, type=RouterEvents.DELETE, cause=Cause(description="test"), ocean_id=0, target_runner_id=3, payload=Reference(DataRef("missing", "global")))

    router.on_delete(event)

    assert emitted == []


def test_event_router_single_enum_subscription_is_idempotent():
    routed = []
    router = EventRouter(EventLogger())

    router.subscribe(RouterEvents.DELETE, routed.append)
    router.subscribe(RouterEvents.DELETE, routed.append)
    router.route(Event(source="test", source_id="test", type=RouterEvents.DELETE, cause=Cause(description="test"), ocean_id=0, payload=Reference(DataRef("key", "global"))))

    assert len(routed) == 1


def test_event_router_can_unsubscribe_an_absent_single_enum_consumer():
    router = EventRouter(EventLogger())

    router.unsubscribe(RouterEvents.DELETE, lambda event: None)

    assert router.subscribers[RouterEvents.DELETE] == []


def test_event_router_records_log_events_without_delivering_them():
    logger = EventLogger()
    router = EventRouter(logger)
    event = Event(
        source="test",
        source_id="test",
        type=LogEvents.EFFECT,
        cause=Cause(description="test"),
        ocean_id=0,
        payload=EffectOccurrence(Effects.WRITE, (), None, "runtime"),
    )

    router.route(event)

    assert len(logger.occurrences) == 1
    assert logger.events[0].event_id == event.event_id


def test_event_router_routes_pipeline_event_to_exact_runner():
    logger = EventLogger()
    received = []
    router = EventRouter(logger)
    router.subscribed_runners[3] = SimpleNamespace(consume_event=received.append)
    event = Event(source="test", source_id="test", type=PipelineEvents.BUILD, cause=Cause(description="test"), ocean_id=0, target_runner_id=3, payload=Reference(DataRef("key", "global")))

    router.route(event)

    assert logger.events[0].event_id == event.event_id
    assert received == [event]


def test_event_router_broadcasts_same_pipeline_event_to_active_runners():
    first = []
    second = []
    router = EventRouter(EventLogger())
    router.subscribed_runners = {
        1: SimpleNamespace(consume_event=first.append),
        2: SimpleNamespace(consume_event=second.append),
    }
    event = Event(source="test", source_id="test", type=PipelineEvents.BUILD, cause=Cause(description="test"), ocean_id=0, target_runner_id="*", payload=Reference(DataRef("key", "global")))

    router.route(event)

    assert first == [event]
    assert second == [event]


@pytest.mark.parametrize("target", [None, "missing"])
def test_event_router_rejects_missing_pipeline_destination_after_logging(target):
    logger = EventLogger()
    router = EventRouter(logger)
    event = Event(source="test", source_id="test", type=PipelineEvents.BUILD, cause=Cause(description="test"), ocean_id=0, target_runner_id=target, payload=Reference(DataRef("key", "global")))

    with pytest.raises(ValueError):
        router.route(event)

    assert logger.events[0].event_id == event.event_id


def test_event_router_logs_before_calling_ordinary_consumer():
    logger = EventLogger()
    router = EventRouter(logger)
    event = Event(source="test", source_id="test", type=RouterEvents.DELETE, cause=Cause(description="test"), ocean_id=0, payload=Reference(DataRef("key", "global")))

    def consume(received):
        assert received is event
        assert len(logger.occurrences) == 1

    router.subscribe(RouterEvents.DELETE, consume)
    router.route(event)


def test_store_template_instances_do_not_share_default_state():
    first = StoreTemplate()
    second = StoreTemplate()

    first.set("key", "value")

    assert first.scope == "global"
    assert second.dump() == {}


def test_memory_store_uses_an_explicit_empty_template():
    template = {}
    store = MemoryKVStore(store_template=template)

    store.set("key", "value")

    assert template == {}
    assert store.dump() == {"key": "value"}


def test_diff_does_not_treat_duck_typed_objects_as_model_adapters():
    tide = TIDE()
    tide.event("Producer", writes={"@A": "a"})
    trace = tide.trace()

    class BrokenModelAdapter:
        def __init__(self):
            self.calls = 0

        def to_model(
            self,
            *,
            enrich=True,
            identity_mode="literal",
        ):
            self.calls += 1
            raise TypeError("adapter implementation failed")

    adapter = BrokenModelAdapter()

    compared = trace.diff(adapter)

    assert compared.same is False
    assert adapter.calls == 0


def test_diff_treats_no_argument_model_adapters_as_ordinary_values():
    tide = TIDE()
    tide.event("Producer", writes={"@A": "a"})
    trace = tide.trace()
    model = trace.to_model(enrich=False)

    class NoArgumentModelAdapter:
        def to_model(self):
            return model

    assert trace.diff(NoArgumentModelAdapter()).same is False


def test_query_construction_is_silent_until_show(capsys):
    tide = TIDE()
    tide.event("Producer", writes={"@A": "a"})
    trace = tide.trace()

    trace.find("missing").first()
    trace.inspect("Producer", "actor", grouped=False)
    trace.investigate("missing", "actor", grouped=False)

    assert capsys.readouterr().out == ""


def test_record_equality_handles_array_values_and_dependency_objects():
    numpy = pytest.importorskip("numpy")
    left_value = numpy.array([1, 2, 3])
    right_value = numpy.array([1, 2, 3])
    dep = DependencyUseRecord(
        ("args", SimpleNamespace(
            raw=left_value,
            evaluated=left_value,
            normalised=left_value,
            resolved=left_value,
        )),
        DataRef(key="@A", store="global"),
        "static",
        "global:@A",
        left_value,
        0,
        "source",
        Actor("ocean", "source", "Producer"),
        "exec",
    )
    matching_dep = DependencyUseRecord(
        ("args", SimpleNamespace(
            raw=right_value,
            evaluated=right_value,
            normalised=right_value,
            resolved=right_value,
        )),
        DataRef(key="@A", store="global"),
        "static",
        "global:@A",
        right_value,
        0,
        "source",
        Actor("ocean", "source", "Producer"),
        "exec",
    )
    left_input = InputUseRecord(
        "args",
        left_value,
        left_value,
        left_value,
        left_value,
        [dep],
    )
    right_input = InputUseRecord(
        "args",
        right_value,
        right_value,
        right_value,
        right_value,
        [matching_dep],
    )
    expanded_left = ExpandedDepRecord(
        ("args", SimpleNamespace(
            raw=left_value,
            evaluated=left_value,
            normalised=left_value,
            resolved=left_value,
        )),
        None,
        "static",
        "global:@A",
        left_value,
        0,
        "source",
        Actor("ocean", "source", "Producer"),
        "exec",
        [],
    )
    expanded_right = ExpandedDepRecord(
        ("args", SimpleNamespace(
            raw=right_value,
            evaluated=right_value,
            normalised=right_value,
            resolved=right_value,
        )),
        None,
        "static",
        "global:@A",
        right_value,
        0,
        "source",
        Actor("ocean", "source", "Producer"),
        "exec",
        [],
    )

    assert dep == matching_dep
    assert left_input == right_input
    assert expanded_left == expanded_right
    assert WriteRecord(Cause(description="test"), left_value, 0, "global:@A") == WriteRecord(
        Cause(description="test"),
        right_value,
        0,
        "global:@A",
    )

    with_runtime_read = Inputs({})
    without_runtime_read = Inputs({})
    with_runtime_read.runtime_read = left_input

    assert with_runtime_read != without_runtime_read


def test_pipeline_runner_can_register_a_new_node_or_reject_bad_payload():
    tide = TIDE()
    base = Node(func=lambda: "base", targets=["global:base"])
    definition = Pipeline(nodes=[base], name="register-test")
    definition = tide.ocean.register_pipeline(definition)
    runner = tide.ocean._create_pipeline_runner(
        definition,
        "sequential",
        {},
    )
    added = Node(func=lambda: "added", targets=["global:added"])

    runner.on_register_node(Event(ocean_id=0, 
        source="test",
        source_id="test",
        type=PipelineEvents.REGISTER_NODE,
        cause=Cause(description="test"),
        payload=Nodes((added,), ({},)),
    ))

    indexed = runner.index.node_index.node_by_definition_id[added.id]
    assert indexed.node is added
    assert indexed.id in runner.runtime_dag.forward_map

    with pytest.raises(EventHandlerError):
        runner.on_register_node(Event(ocean_id=0, 
            source="test",
            source_id="test",
            type=PipelineEvents.REGISTER_NODE,
            cause=Cause(description="test"),
            payload=["not-a-node"],
        ))


def test_build_accepts_dynamic_reference_without_explicit_count():
    tide = TIDE()
    tide.ocean.store_router.set("global", "@item_one", "value")
    dynamic = DataRef(key="@item*", store="global", max=1)
    consumer = Node(
        func=lambda values: values,
        args=(dynamic,),
        targets=["global:out"],
    )
    definition = Pipeline(nodes=[consumer], name="build-test")
    definition = tide.ocean.register_pipeline(definition)
    runner = tide.ocean._create_pipeline_runner(
        definition,
        "sequential",
        {},
    )

    runner.on_build(Event(ocean_id=0, 
        source="test",
        source_id="test",
        type=PipelineEvents.BUILD,
        cause=Cause(description="test"),
        payload=Reference(DataRef(key="out", store="global")),
    ))


def test_capture_model_query_and_bundle_preserve_exact_relationships():
    tide = TIDE()
    tide.event("Produce", writes={"@shared": {"value": 1}})
    tide.event(
        "Left",
        reads={"@shared": {"value": 1}},
        writes={"@left": "left"},
    )
    tide.event(
        "Right",
        reads={"@shared": {"value": 1}},
        writes={"@right": "right"},
    )

    captured = tide.trace()
    model = captured.to_model(enrich=False)
    report = TIDE.report_model_validity(model)
    projected = TIDE.from_model(model)
    restored = TIDE.from_bundle(projected.to_bundle())

    handoffs = list(report.direct_handoffs)
    producer_id = handoffs[0][0]
    left_id, right_id = (
        handoff[2]
        for handoff in handoffs
    )

    assert report["semantically_valid"]
    assert len(report.direct_handoffs) == 2
    assert projected.path(
        producer_id,
        left_id,
        "exec_id",
        "exec_id",
    ).exists()
    assert projected.path(
        producer_id,
        right_id,
        "exec_id",
        "exec_id",
    ).exists()
    restored_model = restored.to_model(enrich=False)
    assert restored_model.get("host") == model.get("host")
    assert {
        (edge["exec_id"], edge["state_id"])
        for edge in restored_model["I"]
    } == {
        (edge["exec_id"], edge["state_id"])
        for edge in model["I"]
    }
    assert {
        (edge["exec_id"], edge["state_id"])
        for edge in restored_model["O"]
    } == {
        (edge["exec_id"], edge["state_id"])
        for edge in model["O"]
    }


def test_downstream_show_renders_the_recorded_handoff_once():
    tide = TIDE()
    tide.event("Producer", writes={"@A": "a"})
    tide.event("Consumer", reads={"@A": "a"}, writes={"@B": "b"})
    trace = tide.trace()

    producer = trace.log[0]
    stream = trace.downstream(producer.exec_id, "exec_record", depth=0)
    output = stream.show(color=False)

    assert stream.count() == 1
    assert output.count("global:@A[v0]") == 1
    assert "EXEC_RECORD[1] Consumer" in output
