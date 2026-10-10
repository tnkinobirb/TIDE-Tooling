import pytest

from tide_tooling.tooling_core.engine.event_payloads import (
    EffectOccurrence,
    PipelineRun,
    State,
)
from tide_tooling.tooling_core.engine.events import Cause


def test_cause_requires_one_form_of_explanation():
    linked = Cause(event_id="event-id", description="result_ready")
    assert linked.event_id == "event-id"
    assert linked.description == "result_ready"
    assert Cause(description="external request").description == "external request"

    with pytest.raises(ValueError):
        Cause()



def test_pipeline_run_has_one_fixed_shape():
    payload = PipelineRun("pipeline", None, "thread", {}, True, None, None)

    assert payload._fields == (
        "pipeline_id",
        "pipeline_name",
        "mode",
        "local_store_templates",
        "stay_alive",
        "runner_name",
        "dynamic_ref_plan",
    )


def test_state_keeps_reference_value_and_snapshot_together():
    payload = State("ref", "value", "snapshot")

    assert tuple(payload) == ("ref", "value", "snapshot", True, None)


def test_effect_occurrence_defaults_are_explicit():
    payload = EffectOccurrence("read", ("target",), "output", "runtime")

    assert payload.immediate is False
    assert payload.found is None
