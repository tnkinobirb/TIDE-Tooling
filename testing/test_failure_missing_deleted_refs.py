import threading

import pytest

from tide_tooling import (
    DELETE,
    RAISE,
    RUN,
    TIDE,
    WRITE,
    RaiseHandlingError,
    node,
    pipeline,
)
from tide_tooling.tooling_core.engine.events import Effects, LogEvents, PipelineEvents
from tide_tooling.tooling_core.engine.errors import NoExecRecordError


# ================================================================
# FAILURE / MISSING / DELETED REF TESTS
# ================================================================
#
# Revised after review:
#   - .event writes can import/write from an unknown source without failing
#     when reads are not declared.
#   - .event with reads=list/ref requirement should fail honestly when missing.
#   - .exec missing/deleted reads are the important execution failure cases.
#   - deletion can be done directly through tide.ocean.store_router.delete(...)
#     or by monkeypatching around concretisation/resolution.
#
# Core invariant:
#   Missing/deleted refs must not create fake successful executions or paths.
# ================================================================


def first_exec(trace, seq_id):
    return trace.inspect(seq_id, "seq_id").object


def actor(trace, name, grouped=False):
    return trace.inspect(name, "actor", grouped=grouped)


def ref(trace, name, version=None):
    if version is None:
        return trace.inspect(name, "reference")

    return trace.inspect(name, "reference", version=version)


def refs_from_writes(exec_record):
    return [
        (write.stored_target, write.stored_version)
        for write in exec_record.writes
    ]


def wait_thread(thread, timeout=None):
    thread.join(timeout=timeout)

    assert not thread.is_alive()


def test_raise_writes_error_as_value_and_does_not_leak_task():
    def fail_explicitly():
        raise RAISE(ValueError("invalid payload"))

    tide = TIDE()
    result = tide.exec(fail_explicitly, writes="handled_error")
    trace = tide.trace()
    handled_record = trace.exec_log[-1]

    assert isinstance(result, ValueError)
    assert str(result) == "invalid payload"
    stored_error = tide.ocean.store_router.get("global", "handled_error")
    assert isinstance(stored_error, ValueError)
    assert str(stored_error) == "invalid payload"
    assert not tide.ocean.executor.running_tasks
    assert PipelineEvents.RESULT_READY in [
        entry["event_type"]
        for entry in tide.ocean.event_logger.log
    ]
    assert handled_record.func_name == "fail_explicitly"
    assert handled_record.status == "handled_raise"
    assert handled_record.handled_raise is True
    assert isinstance(handled_record.error, ValueError)
    assert str(handled_record.error) == "invalid payload"
    assert isinstance(handled_record.output_value, ValueError)
    assert str(handled_record.output_value) == "invalid payload"
    assert refs_from_writes(handled_record) == [("global:handled_error", 0)]


def test_raise_preserves_nested_effect_descriptions_for_callers():
    raised = RAISE(
        "stop now",
        effects=[{"effect": "audit", "reason": "invalid"}],
        preserve_output={"accepted": False},
    )

    payload = raised.to_dict()

    assert isinstance(raised.exception, Exception)
    assert str(raised.exception) == "stop now"
    assert payload["effect"] == "RAISE"
    assert payload["preserve_output"] == {"accepted": False}
    assert payload["effects"] == [
        {"effect": "audit", "reason": "invalid"}
    ]


def test_raise_dispatches_effects_and_returns_fallback_without_stopping():
    tide = TIDE()
    tide.declare("cleanup", "remove me")

    def fail_with_fallthrough():
        raise RAISE(
            ValueError("invalid payload"),
            effects=[
                WRITE(
                    "global:failure_marker",
                    preserve_output={"handled": True},
                ),
                DELETE("global:cleanup"),
            ],
            preserve_output={"accepted": False},
        )

    result = tide.exec(fail_with_fallthrough, writes="handled_result")
    trace = tide.trace()
    handled_record = trace.exec_log[-1]

    assert result == {"accepted": False}
    assert tide.ocean.store_router.get(
        "global",
        "handled_result",
    ) == {"accepted": False}
    assert tide.ocean.store_router.get(
        "global",
        "failure_marker",
    ) == {"handled": True}
    assert not tide.ocean.store_router.exists("global", "cleanup")
    assert handled_record.status == "handled_raise"
    assert handled_record.handled_raise is True
    assert isinstance(handled_record.error, ValueError)
    assert handled_record.output_value == {"accepted": False}
    assert [type(effect) for effect in handled_record.runtime_effects] == [
        WRITE,
        DELETE,
    ]
    assert {
        effect_record.effect
        for effect_record in handled_record.effects.values()
    } == {"write", "DELETE"}

    imported = TIDE.from_bundle(trace.to_bundle().to_dict())
    imported_record = imported.exec_log[-1]
    assert imported_record.status == "handled_raise"
    assert imported_record.handled_raise is True
    assert imported_record.output_value == {"accepted": False}
    assert [type(effect) for effect in imported_record.runtime_effects] == [
        WRITE,
        DELETE,
    ]
    assert len(imported_record.effects) == 2


def test_raise_run_effect_launches_recovery_and_pipeline_continues():
    calls = []

    @node
    def recover():
        calls.append("recovery")
        return "recovered"

    @pipeline
    def recovery_pipeline():
        return recover()

    tide = TIDE()
    registered_recovery = tide.ocean.register_pipeline(
        recovery_pipeline()
    )

    @node
    def fail():
        calls.append("failed")
        raise RAISE(
            ValueError("recoverable failure"),
            effects=[
                RUN(
                    registered_recovery.id,
                    mode="sequential",
                )
            ],
        )

    @pipeline
    def failing_pipeline():
        return fail()

    trace = tide.ocean.run_pipeline(
        failing_pipeline,
        mode="sequential",
    )

    assert calls == ["failed", "recovery"]
    assert any(
        entry["event_type"] == LogEvents.EFFECT
        and entry["payload"][0] == Effects.RUN
        for entry in tide.ocean.event_logger.log
    )
    assert any(
        record.func_name == "fail"
        and record.status == "handled_raise"
        and record.handled_raise
        for record in trace.exec_log
    )


@pytest.mark.parametrize("mode", ["sequential", "thread"])
def test_ocean_node_error_always_carries_failed_tide_trace(mode):
    @node(targets=["global:should_not_exist"])
    def fail():
        raise RuntimeError("ordinary failure")

    @pipeline
    def flow():
        return fail()

    tide = TIDE()
    with pytest.raises(RuntimeError, match="ordinary failure") as raised:
        tide.ocean.run_pipeline(flow, mode=mode)

    failed_trace = raised.value.tide_trace
    failed_record = failed_trace.exec_log[-1]
    assert failed_record.func_name == "fail"
    assert failed_record.status == "failed"
    assert failed_record.error is raised.value
    assert failed_record.writes == []


@pytest.mark.parametrize("mode", ["sequential", "thread"])
def test_ocean_raise_is_written_and_consumed_without_stopping(mode):
    observed = []

    @node(targets=["global:handled"])
    def fail():
        raise RAISE(
            ValueError("explicit failure"),
            preserve_output="fallback",
        )

    @node
    def consume(value):
        observed.append(value)
        return value

    @pipeline
    def flow():
        return consume(fail())

    tide = TIDE()
    trace = tide.ocean.run_pipeline(flow, mode=mode)

    handled_record = next(
        record for record in trace.exec_log if record.func_name == "fail"
    )
    consumer_record = next(
        record for record in trace.exec_log if record.func_name == "consume"
    )
    assert observed == ["fallback"]
    assert handled_record.status == "handled_raise"
    assert handled_record.handled_raise is True
    assert isinstance(handled_record.error, ValueError)
    assert refs_from_writes(handled_record) == [("global:handled", 0)]
    assert consumer_record.status == "succeeded"
    assert consumer_record.inputs.args.dependencies


def test_returned_raise_uses_the_same_controlled_value_semantics():
    def return_controlled_raise():
        return RAISE(
            ValueError("returned signal"),
            preserve_output=False,
        )

    tide = TIDE()
    result = tide.exec(return_controlled_raise, writes="fallback")
    record = tide.trace().exec_log[-1]

    assert result is False
    assert tide.ocean.store_router.get("global", "fallback") is False
    assert record.status == "handled_raise"
    assert record.handled_raise is True
    assert isinstance(record.error, ValueError)


def test_raise_handling_failure_is_explicit_and_carries_both_errors():
    def invalid_controlled_raise():
        raise RAISE(
            ValueError("user failure"),
            effects=[{"effect": "not-a-runtime-effect"}],
        )

    tide = TIDE()
    with pytest.raises(RaiseHandlingError) as raised:
        tide.exec(invalid_controlled_raise)

    assert isinstance(raised.value.raised_error, ValueError)
    assert str(raised.value.raised_error) == "user failure"
    assert isinstance(raised.value.handling_error, TypeError)
    assert "RAISE effects must be TIDE Effect objects" in str(
        raised.value.handling_error
    )
    assert raised.value.tide_trace.exec_log[-1].status == "handled_raise"


def test_event_read_list_imports_stored_ref():
    tide = TIDE()
    tide.ocean.store_router.set("global", "@imported", "value-from-outside")
    tide.event("ImportUnknown", reads=["@imported"])

    trace = tide.trace()

    imported_exec = first_exec(trace, 0)

    assert refs_from_writes(imported_exec) == [
        ("global:@imported", 0)
    ]

    imported_ref = ref(trace, "global:@imported", version=0)
    importer = actor(trace, "ImportUnknown", grouped=False)

    assert trace.path(imported_ref, importer).exists()


def test_event_reads_list_missing_ref_fails_honestly():
    tide = TIDE()

    with pytest.raises(Exception):
        tide.event("NeedsMissing", reads=["@missing"], writes={"@out": "out"})
    

    assert not tide.trace().find("NeedsMissing").exists("actor")[0]
    assert not tide.trace().find("NeedsMissing").exists("actor")[1]


def test_event_handles_missing_ref_dict():
    tide = TIDE()

    tide.event("NeedsMissing", reads={"@missing": "missing"}, writes={"@out": "out"})

    assert not tide.trace().find("NeedsMissing").exists("actor")[0]
    assert tide.trace().find("NeedsMissing").exists("actor")[1]


def test_exec_missing_static_ref_does_not_execute():
    def UseMissing(value):
        return value

    tide = TIDE()
    errors = []
    results = []

    try:
        results.append(
            tide.exec(
                UseMissing,
                args=["@@missing"],
                writes=["@out"],
            )
        )
    except Exception as error:
        errors.append(error)

    trace = tide.trace()

    assert errors
    assert results == []
    assert not trace.find("UseMissing").exists("actor")[0]
    assert trace.find("global:@out").exists("reference") == (False, False)


def test_exec_deleted_static_ref_before_execution_does_not_execute():
    def UseDeleted(value):
        return value

    tide = TIDE()

    tide.event("SeedA", writes={"@A": "A"})
    tide.ocean.store_router.delete("global", "@A")

    errors = []
    results = []

    try:
        results.append(
            tide.exec(
                UseDeleted,
                args=["@@A"],
                writes=["@out"],
            )
        )
    except Exception as error:
        errors.append(error)

    trace = tide.trace()

    assert errors
    assert results == []
    assert not trace.find("UseDeleted").exists("actor")[0]
    assert trace.find("global:@out").exists("reference") == (False, False)


def test_event_deleted_static_ref_before_read_fails_honestly():
    tide = TIDE()

    tide.event("SeedA", writes={"@A": "A"})
    tide.ocean.store_router.delete("global", "@A")

    errors = []

    try:
        tide.event("UseDeletedEvent", reads=["@A"], writes={"@out": "out"})
    except Exception as error:
        errors.append(error)

    trace = tide.trace()

    assert errors
    assert not trace.find("UseDeletedEvent").exists("actor")[0]
    assert trace.find("global:@out").exists("reference") == (False, False)




def dynamic_pattern(
    pattern: str,
    minimum: int,
    returned: int,
    store: str = "global",
    strip_leading: bool = False,
) -> str:
    value = f"{store}:{pattern}:{minimum}:{returned}"

    if strip_leading:
        return value

    return f"@{value}"


def test_exec_deleted_after_static_dependency_available_before_resolution_does_not_execute(monkeypatch):
    def use_static(value):
        return value

    tide = TIDE()
    results = []
    errors = []
    deleted = threading.Event()

    tide.event("SeedA", writes={"@A": "A"})

    original_emit_event_to_ocean = tide.ocean.event_interface.emit_event_to_ocean

    def intercept_event(event):
        original_emit_event_to_ocean(event)

        if event.event_type == PipelineEvents.DEP_AVAILABLE and not deleted.is_set():
            deleted.set()
            tide.ocean.store_router.delete("global", "@A")

    monkeypatch.setattr(
        tide.ocean.event_interface,
        "emit_event_to_ocean",
        intercept_event,
    )

    def execute():
        try:
            results.append(
                tide.exec(
                    use_static,
                    args=["@@A"],
                    writes=["@out"],
                )
            )
        except Exception as error:
            errors.append(error)

    worker = threading.Thread(target=execute)
    worker.start()
    wait_thread(worker)

    trace = tide.trace()

    assert deleted.is_set()
    assert isinstance(errors[0], NoExecRecordError)
    assert errors[0].tide_trace is not None

    assert trace.events
    
    event_names = [
        event["event_type"] if isinstance(event, dict) else event.event_type
        for event in trace.events
    ]

    assert LogEvents.MISSING_REFS in event_names

    assert results == []
    assert not trace.find("use_static").exists("actor")[0]
    assert trace.find("global:@out").exists("reference") == (False, False)


def test_exec_deleted_after_dynamic_concretisation_before_resolution_does_not_execute(monkeypatch):
    def consume_dynamic(values):
        return values

    tide = TIDE()
    results = []
    errors = []
    deleted = threading.Event()

    original_emit_event_to_ocean = tide.ocean.event_interface.emit_event_to_ocean

    def intercept_event(event):
        original_emit_event_to_ocean(event)

        if event.event_type == LogEvents.DEP_CONCRETISED and not deleted.is_set():
            deleted.set()
            tide.ocean.store_router.delete("global", "@alpha_one")

    monkeypatch.setattr(
        tide.ocean.event_interface,
        "emit_event_to_ocean",
        intercept_event,
    )

    tide.event("SeedDynamic", writes={"@alpha": "!alpha", "@alpha_one": "!alpha_one"})

    def execute():
        try:
            results.append(
                tide.exec(
                    consume_dynamic,
                    reads=[
                        dynamic_pattern(
                            "@alpha*",
                            2, 
                            1,#lil nuance - if return is lower than min e.g. 1 - then if that ref succesfully resolves, no error. 
                            strip_leading=True,
                        )
                    ],
                    writes=["@out"],
                )
            )
        except Exception as error:
            errors.append(error)

    worker = threading.Thread(target=execute)
    worker.start()
    wait_thread(worker)

    trace = tide.trace()
    trace.show()


    assert deleted.is_set()
    assert errors
    assert results == []
    assert not trace.find("consume_dynamic").exists("actor")[0]
    assert trace.find("global:@out").exists("reference") == (False, False)

# @pytest.mark.xfail(
#     reason=(
#         "Desired future contract: missing static refs should expose a structured "
#         "failure/fallback event in trace/stream, not only a Python exception."
#     ),
#     strict=False,
# )
def test_exec_missing_static_ref_records_failure_or_fallback_event():
    def UseMissing(value):
        return value

    tide = TIDE()

    with pytest.raises(NoExecRecordError):
        tide.exec(UseMissing, args=["@@missing"], writes=["@out"])

    # trace = tide.trace()
    # assert trace.events
    
    # event_names = [
    #     event["event_type"]
    #     for event in trace.events
    # ]
    # print(event_names)

    # assert LogEvents.MISSING_REFS in event_names


# @pytest.mark.xfail(
#     reason=(
#         "Desired future contract: inspect missing exact reference returns a "
#         "structured nonexistent inspected object instead of raising or behaving "
#         "like a normal ref."
#     ),
#     strict=False,
# )
def test_inspect_missing_ref_returns_structured_nonexistent_object():
    tide = TIDE()

    tide.event("A", writes={"@A": "A"})

    trace = tide.trace()
    inspected = trace.inspect("global:@missing", "reference", version=0)

    assert inspected.exists is False
    assert inspected.type == "reference"
    assert inspected.id == "global:@missing"


# @pytest.mark.xfail(
#     reason=(
#         "Desired future contract: path(missing_ref, actor) should return a "
#         "Pathed no-path object rather than requiring caller-side None checks."
#     ),
#     strict=False,
# )
def test_path_from_structured_missing_ref_to_actor_returns_no_path():
    tide = TIDE()

    tide.event("A", writes={"@A": "A"})
    tide.event("UseA", reads={"@A": "A"})

    trace = tide.trace()

    missing_ref = trace.inspect("global:@missing", "reference", version=0)
    use_a = actor(trace, "UseA", grouped=False)

    path = trace.path(missing_ref, use_a)

    assert not path.exists()
