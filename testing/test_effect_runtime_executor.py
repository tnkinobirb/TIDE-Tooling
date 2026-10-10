from tide_tooling import DELETE_NOW, READ, WRITE_NOW
from tide_tooling.tooling_core.engine.classes import (
    DELETE,
    READ_EFFECT,
    EffectRuntime,
    __TEMPLATE_VAL__,
    _current_effect_runtime,
)
from tide_tooling.tooling_core.engine.executor import _execute_with_timing
from tide_tooling.tooling_core.engine.PipelineRunner import PipelineRunner
from tide_tooling.tooling_core.engine.stores import MemoryKVStore
from tide_tooling.tooling_core.engine.utils import create_tide_exec_uuid
from types import SimpleNamespace

def test_effect_callables_dispatch_actual_effect_objects():
    observed = []
    runtime = EffectRuntime(
        exec_id=create_tide_exec_uuid(),
        read_handler=lambda runtime, effect: observed.append(effect) or "read",
        write_handler=lambda runtime, effect: observed.append(effect) or True,
        delete_handler=lambda runtime, effect: observed.append(effect) or True,
    )
    token = _current_effect_runtime.set(runtime)
    try:
        assert READ("global:value") == "read"
        assert WRITE_NOW("global:value", 2) is True
        assert DELETE_NOW("global:value") is True
    finally:
        _current_effect_runtime.reset(token)

    assert isinstance(observed[0], READ_EFFECT)
    assert observed[1].targets[0][1] == 2
    assert isinstance(observed[2], DELETE)


def test_memory_store_observation_distinguishes_missing_from_none():
    store = MemoryKVStore(name="global", store_template={"present": None})

    assert store.get("missing") is __TEMPLATE_VAL__
    assert store.get("present") is None
    assert store.pop("missing") is __TEMPLATE_VAL__
    assert store.pop("present") is None


def test_runtime_read_observes_the_live_runner_store():
    store = MemoryKVStore(name="global", store_template={"value": "before"})
    emitted = []
    runner = SimpleNamespace(
        id="runner",
        ocean_id=0,
        store_router=SimpleNamespace(id="store", get_store=lambda _: store),
        _unscope_data_ref=lambda ref: ref,
        event_router_route=emitted.append,
    )
    runtime = EffectRuntime(
        exec_id=create_tide_exec_uuid(),
        node_ready_event_id="ready",
    )

    # The Store can change after runtime construction and before this READ.
    store.set("value", "after")

    assert PipelineRunner._apply_read_now(
        runner,
        runtime,
        READ_EFFECT("global:value"),
    ) == "after"
    assert len(emitted) == 1
    assert emitted[0].exec_id == runtime.exec_id


def test_executor_transports_runtime_effect_summary_without_buffering_events():
    success_effect = READ_EFFECT("global:success")

    def succeed():
        runtime = _current_effect_runtime.get()
        runtime.effects.append(success_effect)
        return "done"

    success_runtime = EffectRuntime(exec_id=create_tide_exec_uuid())
    success = _execute_with_timing(success_runtime.exec_id, succeed, (), {}, success_runtime)
    assert success.result == "done"
    assert [effect.kind for effect in success.runtime_effects] == [success_effect.kind]
    assert success.runtime_immediate_effect_count == 0

    failure_effect = READ_EFFECT("global:failure")

    def fail():
        runtime = _current_effect_runtime.get()
        runtime.effects.append(failure_effect)
        raise RuntimeError("failed after effect")

    failure_runtime = EffectRuntime(exec_id=create_tide_exec_uuid())
    failure = _execute_with_timing(failure_runtime.exec_id, fail, (), {}, failure_runtime)
    assert failure.status == "failed"
    assert [effect.kind for effect in failure.runtime_effects] == [failure_effect.kind]
    assert failure.runtime_immediate_effect_count == 1
