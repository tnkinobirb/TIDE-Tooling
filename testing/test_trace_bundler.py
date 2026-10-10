import pytest

from tide_tooling import TIDE
from tide_tooling.tooling_core.bundling.TIDEBundle import TIDEBundle
from tide_tooling.tooling_core.bundling.TIDEBundler import TIDEBundler
from tide_tooling.tooling_core.engine.event_occurrences import (
    FrozenEventLog,
    FrozenEventLogBundle,
)


def _event_trace():
    tide = TIDE()
    tide.event("writer", writes={"global:x": {"items": [1, 2]}})
    tide.event("reader", reads={"global:x": {"items": [1, 2]}})
    return tide.trace()


def test_trace_bundle_is_native_tiderun_plus_cas():
    bundle = _event_trace().to_bundle()
    manifest = bundle.manifest

    assert manifest["kind"] == "tide_bundle"
    assert manifest["content"] == "trace"
    assert set(manifest) == {
        "kind",
        "codec_version",
        "content",
        "run",
        "values",
        "events",
        "metadata",
    }
    assert manifest["run"]["kind"] == "tide_run"
    assert manifest["events"] is None
    assert "authority" not in manifest
    assert manifest["values"]
    assert bundle.cas_bytes


def test_event_sidecar_is_optional_and_not_trace_authority():
    source = _event_trace()
    assert source.events_verified is True
    native = source.to_bundle()
    retained = source.to_bundle(serialise_events=True)

    assert native.manifest["events"] is None
    assert retained.manifest["events"] is not None
    assert native.manifest["run"] == retained.manifest["run"]
    assert native.manifest["values"] == retained.manifest["values"]

    native_trace = TIDE.from_bundle(native)
    retained_trace = TIDE.from_bundle(retained)

    assert native_trace.to_model(enrich=False) == retained_trace.to_model(
        enrich=False
    )
    assert native_trace.event_log is None
    assert retained_trace.event_log is not None
    assert native_trace.events_verified is None
    assert retained_trace.events_verified is True


def test_mismatched_event_sidecar_is_marked_without_rewriting_tiderun():
    source = _event_trace()
    first = source.to_bundle(serialise_events=True).to_dict()
    second = _event_trace().to_bundle(serialise_events=True).to_dict()

    first["manifest"]["events"] = second["manifest"]["events"]

    restored = TIDE.from_bundle(first)

    assert restored.events_verified is False
    assert [record.exec_id for record in restored.log] == [
        record.exec_id for record in source.log
    ]


def test_event_verification_can_be_skipped():
    bundle = _event_trace().to_bundle(serialise_events=True)

    restored = TIDE.from_bundle(bundle, verify_events=False)

    assert restored.events_verified is None
    assert restored.event_log is not None

    produced = _event_trace().to_bundle(
        serialise_events=True,
        verify_events=False,
    )
    assert produced.trace().events_verified is None


def test_restored_trace_reexports_the_identical_source_bundle():
    bundle_1 = _event_trace().to_bundle(serialise_events=True)
    trace_1 = TIDE.from_bundle(bundle_1)
    bundle_2 = trace_1.to_bundle()
    trace_2 = TIDE.from_bundle(bundle_2)
    bundle_3 = trace_2.to_bundle()

    assert bundle_1.to_dict() == bundle_2.to_dict() == bundle_3.to_dict()
    assert bundle_1.bundle_hash == bundle_2.bundle_hash == bundle_3.bundle_hash


def test_frozen_event_sidecar_rejects_changed_occurrence_bytes():
    payload = _event_trace().to_bundle(
        serialise_events=True
    ).manifest["events"]

    # FrozenEventLogBundle stores one hashed Event graph. Mutating any
    # encoded Event record without recomputing event_log_hash must fail.
    records = payload["events"]["records"]
    object_id = next(iter(records))
    records[object_id]["tampered"] = True

    with pytest.raises(ValueError, match="content hash"):
        FrozenEventLogBundle.from_dict(payload)

def test_trace_event_sidecar_is_stable_projection_of_one_frozen_log():
    trace = _event_trace()

    first = trace.event_log.to_event_bundle().to_dict()
    second = trace.event_log.to_event_bundle().to_dict()

    # Projection is deterministic but returns detached payload dictionaries.
    assert first == second
    assert first is not second
    assert first["events"] == second["events"]

    # Capturing an already-frozen log is intentionally idempotent.
    assert trace.event_log is FrozenEventLog.capture(trace.event_log)

def test_model_projection_bundles_as_native_trace_not_model_authority():
    model = _event_trace().to_model(enrich=True)
    trace = TIDE.from_model(model)
    bundle = trace.to_bundle()
    restored = TIDE.from_bundle(bundle)

    assert bundle.manifest["content"] == "trace"
    assert bundle.manifest["run"]["kind"] == "tide_run"
    assert "authority" not in bundle.manifest
    assert restored.to_model(enrich=True) == trace.to_model(enrich=True)


def test_standalone_values_bundle_has_values_content_and_preserves_aliases():
    shared = []
    value = [shared, shared]
    bundle = TIDEBundle.from_values([value])
    loaded = bundle._loaded_state()["values"][0]

    assert bundle.content == "values"
    assert set(bundle.manifest) == {
        "kind",
        "codec_version",
        "content",
        "items",
        "metadata",
    }
    assert loaded[0] is loaded[1]


def test_generic_value_graph_preserves_cycles():
    value = []
    value.append(value)
    bundler = TIDEBundler()
    restored = bundler.deserialise(bundler.bundle(value))

    assert restored[0] is restored


def test_native_trace_manifest_rejects_legacy_authority_field():
    payload = _event_trace().to_bundle().to_dict()
    payload["manifest"]["authority"] = {"type": "event_log"}

    with pytest.raises(
        ValueError,
        match="Invalid native Trace bundle manifest",
    ):
        TIDEBundle.from_dict(payload)
