import pytest
from types import SimpleNamespace

from tide_tooling.tooling_core.cli.event_runtime import _parse_cli_cmd_into_event
from tide_tooling.tooling_core.cli.utils import (
    _ast_parse_value_from_string,
    _safe_parse_func_from_string,
)
from tide_tooling.tooling_core.engine.DependencyTools import DependencyTools
from tide_tooling.tooling_core.engine.DictProcessor import DictProcessor
from tide_tooling.tooling_core.engine.NodeProcessor import NodeProcessor
from tide_tooling.tooling_core.engine.classes import (
    DataRef,
    Node,
    NodeDeferralFlags,
    NodeState,
    Pipeline,
    ProcessStates,
    RunnerNode,
)
from tide_tooling.tooling_core.engine.errors import (
    EventHandlerError,
    StoreReferenceAmbiguityError,
)
from tide_tooling.tooling_core.engine.events import (
    Event,
    LogEvents,
    OceanEvents,
    PipelineEvents,
    RouterEvents,
)
from tide_tooling.tooling_core.engine.registries import StoreRegistry
from tide_tooling.tooling_core.engine.routers import EventRouter, StoreRouter
from tide_tooling.tooling_core.engine.stores import MemoryKVStore, StoreTemplate
from tide_tooling.tooling_core.engine.tide_rendering import display_identity
from tide_tooling.tooling_core.engine.tide_logging import (
    Actor,
    DependencyUseRecord,
    EffectRecord,
    ExecRecord,
    ExpandedDepRecord,
    InputUseRecord,
    Inputs,
    LogTools,
    WriteRecord,
)
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
        tide.close()


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


def test_runner_node_uses_the_exported_state_and_deferral_types():
    runner = RunnerNode(Node(func=lambda: None), id="runner")

    assert isinstance(runner.state, NodeState)
    assert isinstance(runner.defer_flags, NodeDeferralFlags)
    assert (
        RunnerNode.__dataclass_fields__["state"].default_factory
        is NodeState
    )
    assert (
        RunnerNode.__dataclass_fields__["defer_flags"].default_factory
        is NodeDeferralFlags
    )


@pytest.mark.parametrize(
    "field_name",
    [
        "func",
        "args",
        "kwargs",
        "p_args",
        "p_kwargs",
        "k_args",
        "k_kwargs",
    ],
)
def test_every_deferral_flag_is_reported(field_name):
    flags = NodeDeferralFlags()

    assert flags.any() is False

    setattr(flags, field_name, True)

    assert flags.any() is True


def test_runner_node_runtime_reset_resets_inputs_without_bad_self_argument():
    runner = RunnerNode(Node(func=lambda: None), id="runner")
    runner.args.update_state(ProcessStates.EVALUATED, ("evaluated",))
    runner.args.update_state(ProcessStates.NORMALISED, ("normalised",))
    runner.args.update_state(ProcessStates.RESOLVED, ("resolved",))
    runner.status = "complete"

    runner._reset_runtime_state()

    assert runner.status is None
    assert not runner.args.has_been(ProcessStates.RESOLVED)


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


def test_dependency_resolver_preserves_the_original_error_as_cause():
    tools = object.__new__(DependencyTools)

    def reject_reference(_):
        raise TypeError("resolver exploded")

    with pytest.raises(
        ValueError,
        match="in_place_dependency_resolver error: resolver exploded",
    ) as raised:
        tools.in_place_dependency_resolver(
            reject_reference,
            node=None,
            input_value=DataRef(key="@A", store="global"),
        )

    assert isinstance(raised.value.__cause__, TypeError)


def test_dynamic_resolver_uses_one_as_the_default_required_count():
    tools = object.__new__(DependencyTools)
    dynamic = DataRef(key="@item*", store="global", max=1)
    concrete = DataRef(key="@item_one", store="global")
    node = SimpleNamespace(
        dynamic_to_static_deps={dynamic: [concrete]},
    )
    tools._snapshot_available_deps = lambda ref: ["@item_one"]
    tools.resolve = lambda ref: "resolved"

    resolved, reads = tools.in_place_dependency_resolver(
        lambda ref: ref,
        node=node,
        input_value=dynamic,
    )

    assert resolved == ["resolved"]
    assert len(reads) == 1
    assert reads[0][0] == concrete


def test_callable_input_can_receive_the_memory_store_snapshot():
    processor = NodeProcessor(SimpleNamespace(
        dump_store=lambda store: {"source": 41},
    ))

    def consume_store(store, increment):
        return store["source"] + increment

    result = processor._callable_input_evaluator(
        consume_store,
        inj_args=(1,),
        inject_data_store=True,
    )

    assert result == 42


def test_cli_relative_function_error_names_the_supplied_value():
    with pytest.raises(Exception) as raised:
        _safe_parse_func_from_string(".sqrt")

    message = str(raised.value)

    assert ".sqrt starts with '.'" in message
    assert "name 'func_name' is not defined" not in message


def test_cli_value_parser_preserves_container_shape_and_all_refs():
    parsed_list = _ast_parse_value_from_string("['@left', '@right']")
    parsed_tuple = _ast_parse_value_from_string("('@left', '@right')")

    assert isinstance(parsed_list, list)
    assert isinstance(parsed_tuple, tuple)
    assert [value.key for value in parsed_list] == ["@left", "@right"]
    assert [value.key for value in parsed_tuple] == ["@left", "@right"]


class RecordingEventInterface:
    def __init__(self):
        self.events = []

    def emit_event_to_ocean(self, event):
        self.events.append(event)


class AliasOnlyStoreRouter:
    def __init__(self, aliases=None):
        self.store_alias = aliases or {}


def test_cli_empty_input_emits_nothing():
    interface = RecordingEventInterface()

    result = _parse_cli_cmd_into_event(
        "",
        interface,
        AliasOnlyStoreRouter(),
    )

    assert result is None
    assert interface.events == []


def test_cli_kill_emits_the_selected_runner():
    interface = RecordingEventInterface()

    _parse_cli_cmd_into_event(
        "kill 0",
        interface,
        AliasOnlyStoreRouter(),
    )

    assert len(interface.events) == 1
    event = interface.events[0]
    assert event.event_type == OceanEvents.KILL_PIPELINE
    assert event.target_runner_id == 0


@pytest.mark.parametrize("command", ["run", "run_pipeline"])
def test_cli_run_without_a_pipeline_emits_nothing(command):
    interface = RecordingEventInterface()

    result = _parse_cli_cmd_into_event(
        command,
        interface,
        AliasOnlyStoreRouter(),
    )

    assert result is None
    assert interface.events == []


def test_cli_run_emits_pipeline_and_mode():
    interface = RecordingEventInterface()

    _parse_cli_cmd_into_event(
        "run 4 process",
        interface,
        AliasOnlyStoreRouter(),
    )

    assert len(interface.events) == 1
    event = interface.events[0]
    assert event.event_type == OceanEvents.RUN_PIPELINE
    assert event.payload == (4, "process", {}, True)


def test_cli_delete_uses_the_third_argument_as_runner_id():
    interface = RecordingEventInterface()

    _parse_cli_cmd_into_event(
        "delete key mem 7",
        interface,
        AliasOnlyStoreRouter(),
    )

    assert len(interface.events) == 1
    event = interface.events[0]
    assert event.event_type == RouterEvents.DELETE
    assert event.target_runner_id == 7
    assert event.payload[:2] == ("mem", "key")


def test_cli_set_runner_zero_emits_one_scoped_write():
    interface = RecordingEventInterface()

    _parse_cli_cmd_into_event(
        "set key value mem 0",
        interface,
        AliasOnlyStoreRouter(),
    )

    assert len(interface.events) == 1
    event = interface.events[0]
    assert event.event_type == RouterEvents.STORE_WRITE
    assert event.target_runner_id == 0
    assert event.payload[:3] == ("store_0", "key", "value")


def test_cli_set_broadcast_expands_each_store_alias_once():
    interface = RecordingEventInterface()

    _parse_cli_cmd_into_event(
        "set key value mem",
        interface,
        AliasOnlyStoreRouter({"mem": ["store_1", "store_2"]}),
    )

    assert [
        event.payload[:3]
        for event in interface.events
    ] == [
        ("store_1", "key", "value"),
        ("store_2", "key", "value"),
    ]


@pytest.mark.parametrize("command", ["set", "update"])
def test_cli_write_commands_preserve_an_explicit_empty_string(command):
    interface = RecordingEventInterface()

    _parse_cli_cmd_into_event(
        f'{command} key "" global',
        interface,
        AliasOnlyStoreRouter(),
    )

    assert len(interface.events) == 1
    assert interface.events[0].payload[:3] == ("global", "key", "")


@pytest.mark.parametrize(
    ("command", "expected_store", "expected_runner"),
    [
        ("update key value global", "global", "*"),
        ("update key value mem 7", "store_7", 7),
    ],
)
def test_cli_update_emits_exactly_one_write(
    command,
    expected_store,
    expected_runner,
):
    interface = RecordingEventInterface()

    result = _parse_cli_cmd_into_event(
        command,
        interface,
        AliasOnlyStoreRouter(),
    )

    assert result is None
    assert len(interface.events) == 1
    event = interface.events[0]
    assert event.event_type == RouterEvents.STORE_WRITE
    assert event.target_runner_id == expected_runner
    assert event.payload[:3] == (expected_store, "key", "value")


def test_cli_unknown_command_returns_its_command_token():
    interface = RecordingEventInterface()

    event = _parse_cli_cmd_into_event(
        "unknown extra words",
        interface,
        AliasOnlyStoreRouter(),
    )

    assert event.event_type == "unknown"
    assert interface.events == []


def test_cli_build_without_a_key_never_emits_an_invalid_build():
    interface = RecordingEventInterface()

    result = _parse_cli_cmd_into_event(
        "build",
        interface,
        AliasOnlyStoreRouter(),
    )

    assert result is None
    assert interface.events == []


def test_cli_build_emits_the_documented_store_and_key():
    interface = RecordingEventInterface()

    _parse_cli_cmd_into_event(
        "build report global 3",
        interface,
        AliasOnlyStoreRouter(),
    )

    assert len(interface.events) == 1
    event = interface.events[0]
    assert event.event_type == PipelineEvents.BUILD
    assert event.target_runner_id == 3
    assert event.payload == ("global", "report")


def test_dict_processor_flatten_honours_custom_separator_recursively():
    flattened = DictProcessor.flatten(
        {"outer": {"inner": {"value": 1}}},
        sep="/",
    )

    assert flattened == {"outer/inner/value": 1}
    assert DictProcessor.unflatten(flattened, sep="/") == {
        "outer": {"inner": {"value": 1}}
    }


def test_store_registry_reports_local_global_name_ambiguity_directly():
    registry = StoreRegistry()
    registry._locals["same"] = object()
    registry._stores["same"] = object()

    with pytest.raises(
        StoreReferenceAmbiguityError,
        match="registered as both local and global",
    ):
            registry.get("same")


def test_store_router_reports_alias_ambiguity_directly():
    registry = StoreRegistry()
    router = StoreRouter(
        registry,
        event_interface=SimpleNamespace(
            emit_event_to_ocean=lambda event: None,
            event_subscriber=lambda *args: None,
        ),
        store_alias={"mem": ["mem_1", "mem_2"]},
        subscribe_events=False,
    )

    with pytest.raises(
        StoreReferenceAmbiguityError,
        match="resolved to multiple stores",
    ) as raised:
        router.get_store("mem")

    assert raised.value.name == "mem"


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
        store_alias={"mem": ["mem_0"]},
        subscribe_events=False,
    )

    router.on_delete(Event(
        source="runner",
        source_id=9,
        target_runner_id=0,
        event_type=RouterEvents.DELETE,
        payload=("mem", "key", "user", "test"),
    ))

    assert registry.get("mem_0").exists("key") is False
    assert len(emitted) == 1
    assert emitted[0].target_runner_id == 0
    assert emitted[0].event_type == PipelineEvents.DEP_DELETED


def test_event_router_single_enum_subscription_is_idempotent():
    routed = []
    router = EventRouter(SimpleNamespace(log_event=lambda event: None))

    router.subscribe(RouterEvents.DELETE, routed.append)
    router.subscribe(RouterEvents.DELETE, routed.append)
    router.route(Event(event_type=RouterEvents.DELETE))

    assert len(routed) == 1


def test_event_router_can_unsubscribe_an_absent_single_enum_consumer():
    router = EventRouter(SimpleNamespace(log_event=lambda event: None))

    router.unsubscribe(RouterEvents.DELETE, lambda event: None)

    assert router.subscribers[RouterEvents.DELETE] == []


def test_store_template_instances_do_not_share_default_state():
    first = StoreTemplate()
    second = StoreTemplate()

    first.store["key"] = "value"

    assert first.scope == "global"
    assert second.store == {}


def test_memory_store_uses_an_explicit_empty_template():
    template = {}
    store = MemoryKVStore(store_template=template)

    store.set("key", "value")

    assert template == {"key": "value"}


def test_duck_typed_model_internal_type_error_is_not_masked_or_retried():
    tide = TIDE()
    tide.event("Producer", writes={"@A": "a"})
    trace = tide.trace()

    class BrokenModelAdapter:
        def __init__(self):
            self.calls = 0

        def to_model(
            self,
            *,
            include_hashes=True,
            identity_mode="host",
        ):
            self.calls += 1
            raise TypeError("adapter implementation failed")

    adapter = BrokenModelAdapter()

    with pytest.raises(TypeError, match="adapter implementation failed"):
        trace.diff(adapter)

    assert adapter.calls == 1


def test_duck_typed_no_argument_model_adapter_remains_supported():
    tide = TIDE()
    tide.event("Producer", writes={"@A": "a"})
    trace = tide.trace()
    model = trace.to_model(include_hashes=False)

    class NoArgumentModelAdapter:
        def to_model(self):
            return model

    assert trace.diff(NoArgumentModelAdapter()).same


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
    assert WriteRecord("test", left_value, 0, "global:@A") == WriteRecord(
        "test",
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
    definition = Pipeline(stages=[base], name="register-test")
    definition = tide.ocean.register_pipeline(definition)
    runner = tide.ocean._create_pipeline_runner(
        definition,
        "sequential",
        {},
    )
    added = Node(func=lambda: "added", targets=["global:added"])

    runner.on_register_node(Event(
        event_type=PipelineEvents.REGISTER_NODE,
        payload=added,
    ))

    indexed = runner.index.node_index.node_by_name["<lambda>"]
    assert indexed.node is added
    assert indexed.state.indexed is True
    assert indexed.id in runner.runtime_dag.forward_map

    with pytest.raises(EventHandlerError):
        runner.on_register_node(Event(
            event_type=PipelineEvents.REGISTER_NODE,
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
    definition = Pipeline(stages=[consumer], name="build-test")
    definition = tide.ocean.register_pipeline(definition)
    runner = tide.ocean._create_pipeline_runner(
        definition,
        "sequential",
        {},
    )

    runner.on_build(Event(
        event_type=PipelineEvents.BUILD,
        payload=("global", "out"),
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
    model = captured.to_model(include_hashes=False)
    report = TIDE.validate_model(model)
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
    restored_model = restored.to_model(include_hashes=False)
    assert restored_model["schema"] == model["schema"]
    assert restored_model["schema_version"] == model["schema_version"]
    assert restored_model["host"] == model["host"]
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


def test_lines_from_edges_returns_actor_heading_and_edge_line_once():
    tide = TIDE()
    tide.event("Producer", writes={"@A": "a"})
    tide.event("Consumer", reads={"@A": "a"}, writes={"@B": "b"})
    trace = tide.trace()

    producer = trace.exec_log[0]
    consumer = trace.exec_log[1]
    edge = trace.index.producer_exec_id_to_consumer_input_edges[
        producer.exec_id
    ][0]

    lines = trace._lines_from_edges([edge], "downstream")

    assert len(lines) == 2
    assert lines[0] == repr(
        trace.inspect(consumer.exec_id, "exec_id").object
    )
    assert lines[1].startswith("      └─ via global:@A[v0], affected ")


def test_blocked_missing_ref_builds_affected_record_before_attaching_it():
    cause = ("MissingRefFallback",)
    blocked_ref = DataRef(key="@A", store="global")
    versioned_blocker = ("global:@A", 0)
    effect = EffectRecord(cause, versioned_blocker, origin="runtime")
    record = make_exec_record(
        exec_id="blocked-source",
        effects=[effect],
        writes=[WriteRecord("audit", "a", 0, "global:@A")],
    )
    events = [
        {
            "event_type": LogEvents.EVENT_EXEC,
            "payload": record,
            "source_id": "source",
            "exec_id": record.exec_id,
        },
        {
            "event_type": LogEvents.NODE_BLOCKED,
            "payload": (
                "blocked-node",
                blocked_ref,
                [("args", "input")],
                cause,
            ),
            "source_id": "runner",
            "exec_id": record.exec_id,
        },
    ]

    LogTools(ocean_id="audit").reconstruct_causality(events)

    affected = list(effect.affected.values())

    assert len(affected) == 1
    assert affected[0].affectee == Actor(
        "audit",
        "runner",
        "blocked-node",
    )
    assert affected[0].targets == versioned_blocker
    assert affected[0].affected_inputs == [
        {"name": "args", "index": None}
    ]
