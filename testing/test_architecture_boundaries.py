from inspect import signature

from tide_tooling import TIDE
from tide_tooling.tooling_core.engine.tide_logging import Trace


def test_trace_has_no_removed_replay_or_records_constructors():
    assert not hasattr(Trace, "from_events")
    assert not hasattr(Trace, "from_records")


def test_trace_constructor_has_no_derived_plan_authority():
    assert "plan" not in signature(Trace).parameters


def test_native_bundle_has_one_semantic_trace_format():
    tide = TIDE()
    tide.declare("value", 1)
    manifest = tide.trace().to_bundle().manifest

    assert manifest["content"] == "trace"
    assert manifest["run"]["kind"] == "tide_run"
    assert "authority" not in manifest
    assert manifest["events"] is None
    assert "plan" not in manifest["run"]["trace"]
    assert "tide_plan" in manifest["run"]["trace"]


def test_model_projection_enters_trace_explicitly_then_bundles_natively():
    tide = TIDE()
    tide.declare("value", 1)
    model = tide.trace().to_model(enrich=True)

    projected = TIDE.from_model(model)
    bundle = projected.to_bundle()

    assert projected.event_log is None
    assert bundle.manifest["content"] == "trace"
    assert bundle.manifest["run"]["kind"] == "tide_run"
    assert "authority" not in bundle.manifest


def test_event_sidecar_is_opt_in_only():
    tide = TIDE()
    tide.declare("value", 1)
    trace = tide.trace()

    native = trace.to_bundle()
    retained = trace.to_bundle(serialise_events=True)

    assert native.manifest["events"] is None
    assert retained.manifest["events"] is not None
    assert native.manifest["run"] == retained.manifest["run"]
