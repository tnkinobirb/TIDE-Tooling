import pytest

from tide_tooling import DELETE_NOW, READ, TIDE, Effects, node, pipeline
from tide_tooling.tooling_core.engine.classes import DataRef
from tide_tooling.tooling_core.engine.errors import DeleteNowInsideProcessPoolError
from tide_tooling.tooling_core.engine.events import LogEvents, PipelineEvents


def _delete_now_from_process_worker():
    return DELETE_NOW("global:temporary")


def _effect_kinds(tide):
    return [
        entry.payload.effect
        for entry in tide.ocean.event_logger.events
        if entry.type == LogEvents.EFFECT
    ]


def test_delete_now_is_safe_and_side_effect_free_outside_tide_runtime():
    assert DELETE_NOW("global:anything") is False


def test_delete_now_requires_one_static_reference():
    with pytest.raises(TypeError, match="string or DataRef"):
        DELETE_NOW(["global:a", "global:b"])

    with pytest.raises(ValueError, match="one static reference"):
        DELETE_NOW(DataRef("item_*", "global"))


def test_delete_now_removes_live_value_and_returns_boolean_status():
    tide = TIDE()
    tide.declare("temporary", "remove me")

    result = tide.exec(lambda: DELETE_NOW("global:temporary"))

    assert result is True
    assert not tide.ocean.store_router.exists("global", "temporary")
    assert tide.trace().find(Effects.DELETE).exists("effect") == (True, False)


def test_delete_now_missing_reference_returns_false_and_records_attempt():
    tide = TIDE()

    result = tide.exec(lambda: DELETE_NOW("global:already_absent"))

    assert result is False
    assert tide.trace().find(Effects.DELETE).exists("effect") == (True, False)
    assert PipelineEvents.DEP_DELETED not in [
        entry.type
        for entry in tide.ocean.event_logger.events
    ]


def test_read_after_delete_now_in_same_node_observes_missing_reference():
    tide = TIDE()
    tide.declare("temporary", "present")

    def controller():
        deleted = DELETE_NOW("global:temporary")
        observed = READ("global:temporary", fallback="missing")
        return deleted, observed

    assert tide.exec(controller) == (True, "missing")
    assert _effect_kinds(tide)[-2:] == [Effects.DELETE, Effects.READ]


def test_in_function_read_delete_read_effect_order_is_preserved():
    tide = TIDE()
    tide.declare("temporary", "present")

    def controller():
        before = READ("global:temporary")
        deleted = DELETE_NOW("global:temporary")
        after = READ("global:temporary", fallback="missing")
        return before, deleted, after

    assert tide.exec(controller) == ("present", True, "missing")
    assert _effect_kinds(tide)[-3:] == [
        Effects.READ,
        Effects.DELETE,
        Effects.READ,
    ]


def test_delete_now_works_inside_an_ocean_pipeline_without_returning_effect():
    @node(targets="global:decision")
    def controller():
        if READ("global:temporary") == "present":
            DELETE_NOW("global:temporary")
        return "continued"

    @pipeline
    def controlled_pipeline():
        controller()

    tide = TIDE()
    tide.declare("temporary", "present")
    trace = tide.ocean.run_pipeline(controlled_pipeline, mode="thread")

    assert trace.value_of("global:decision") == ["continued"]
    assert not tide.ocean.store_router.exists("global", "temporary")
    assert trace.find(Effects.DELETE).exists("effect") == (True, False)


def test_delete_now_fails_under_explicit_process_execution():
    process_node = node(
        targets="global:deleted",
    )(_delete_now_from_process_worker)

    @pipeline
    def process_pipeline():
        process_node()

    tide = TIDE()
    tide.declare("temporary", "present")
    with pytest.raises(DeleteNowInsideProcessPoolError):
        tide.ocean.run_pipeline(process_pipeline, mode="process")

    assert tide.ocean.store_router.exists("global", "temporary")
