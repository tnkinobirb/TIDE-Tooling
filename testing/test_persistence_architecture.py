"""Architecture gates for persisted Trace evidence."""

from tide_tooling import TIDE, node, pipeline


def _planned_trace():
    @node(targets="global:value")
    def produce():
        return 1

    @node(args=("@global:value",), targets="global:result")
    def consume(value):
        return value + 1

    @pipeline
    def flow():
        produce()
        consume()

    return TIDE().ocean.run_pipeline(flow, mode="sequential")


def test_bundle_has_one_native_trace_storage_contract():
    trace = _planned_trace()
    manifest = trace.to_bundle().manifest

    assert manifest["content"] == "trace"
    assert manifest["run"]["kind"] == "tide_run"
    assert "authority" not in manifest
    assert "model" not in manifest
    assert "graph" not in manifest


def test_event_evidence_is_sidecar_only():
    trace = _planned_trace()

    without_events = trace.to_bundle(serialise_events=False)
    with_events = trace.to_bundle(serialise_events=True)

    assert without_events.manifest["run"] == with_events.manifest["run"]
    assert without_events.manifest["events"] is None

    if trace.event_log is None:
        assert with_events.manifest["events"] is None
    else:
        assert with_events.manifest["events"] is not None


def test_only_tide_plan_is_persisted():
    trace = _planned_trace()
    trace_meta = trace.to_bundle().manifest["run"]["trace"]

    assert "tide_plan" in trace_meta
    assert "plan" not in trace_meta


def test_restored_plan_is_derived_from_persisted_blueprint():
    trace = _planned_trace()
    original_plan = trace.plan

    restored = TIDE.from_bundle(trace.to_bundle())

    assert restored._tide_plan == trace._tide_plan
    assert restored.plan == original_plan


def test_restored_trace_reexports_without_rewriting_evidence():
    trace = _planned_trace()
    original = trace.to_bundle()

    restored = TIDE.from_bundle(original)
    reexported = restored.to_bundle()

    assert restored.from_cas is True
    assert reexported.to_dict() == original.to_dict()
    assert reexported.bundle_hash == original.bundle_hash


def test_model_is_an_explicit_projection_not_bundle_storage():
    trace = _planned_trace()
    model = trace.to_model(enrich=True)

    projected = TIDE.from_model(model)
    bundled = projected.to_bundle()

    assert bundled.manifest["content"] == "trace"
    assert bundled.manifest["run"]["kind"] == "tide_run"
    assert "model" not in bundled.manifest
