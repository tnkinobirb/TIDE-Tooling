import copy

import pytest

from tide_tooling import TIDE
from tide_tooling.tooling_core.bundling.TIDEBundler import TIDEBundler
from tide_tooling.tooling_core.bundling.TIDERunSerialiser import (
    TIDERunSerialiser,
)


def source_trace():
    tide = TIDE()
    tide.declare("source", "alpha")

    def consume(value):
        return value.upper()

    tide.exec(
        consume,
        reads="source",
        writes="result",
    )
    return tide.trace()


def fail_if_serialised(*args, **kwargs):
    raise AssertionError(
        "TIDERunSerialiser was called for imported Bundle evidence"
    )


def divergent_payload(trace):
    payload = trace.to_bundle().to_dict()

    states = list(
        trace.index.value_hash_by_ref_version.items()
    )
    assert len(states) >= 2

    expected_state, expected_hash = states[0]
    donor_hash = next(
        value_hash
        for _, value_hash in states[1:]
        if value_hash != expected_hash
    )

    values = payload["manifest"]["values"]

    expected_entry = values[expected_hash]
    donor_entry = values[donor_hash]

    expected_entry["bundle"] = copy.deepcopy(
        donor_entry["bundle"]
    )
    expected_entry["root_cas_ref"] = donor_entry[
        "root_cas_ref"
    ]

    return payload, expected_state, expected_hash, donor_hash


def test_live_trace_still_serialises_normally():
    trace = source_trace()
    bundle = trace.to_bundle()

    assert bundle.content == "trace"
    assert trace.from_cas is False


def test_restored_trace_reexports_exact_source_without_serialiser(monkeypatch):
    source = source_trace()
    original = source.to_bundle()
    restored = TIDE.from_bundle(original)

    monkeypatch.setattr(
        TIDERunSerialiser,
        "serialise_with_values",
        fail_if_serialised,
    )

    reexported = restored.to_bundle()

    assert restored.from_cas is True
    assert reexported.to_dict() == original.to_dict()
    assert reexported.bundle_hash == original.bundle_hash


def test_divergent_restored_trace_reexports_exact_tampered_bundle(monkeypatch):
    source = source_trace()
    payload, state, expected_hash, donor_hash = divergent_payload(source)

    restored = TIDE.from_bundle(payload)

    assert restored.divergent is True
    assert (
        restored.index.value_hash_by_ref_version[state]
        == expected_hash
    )
    assert any(
        warning.get("type") == "value_hash_mismatch"
        and warning.get("expected") == expected_hash
        and warning.get("observed") == donor_hash
        for warning in restored.divergence["warnings"]
    )

    monkeypatch.setattr(
        TIDERunSerialiser,
        "serialise_with_values",
        fail_if_serialised,
    )

    reexported = restored.to_bundle()

    # Exact imported evidence + CAS envelope, not a reconstructed rewrite.
    assert reexported.to_dict() == payload

    restored_again = TIDE.from_bundle(reexported)
    assert (
        restored_again.index.value_hash_by_ref_version[state]
        == expected_hash
    )
    assert restored_again.divergent is True


def test_direct_bundler_refuses_to_serialise_restored_trace():
    restored = TIDE.from_bundle(source_trace().to_bundle())

    with pytest.raises(
        ValueError,
        match="will not serialise a Trace restored from a Bundle",
    ):
        TIDEBundler().bundle_trace(restored)


def test_restored_trace_cannot_add_new_event_sidecar():
    restored = TIDE.from_bundle(
        source_trace().to_bundle(serialise_events=False)
    )

    with pytest.raises(
        ValueError,
        match="did not retain Event evidence",
    ):
        restored.to_bundle(serialise_events=True)


def test_existing_event_sidecar_is_preserved_on_reexport():
    source = source_trace()
    original = source.to_bundle(serialise_events=True)
    restored = TIDE.from_bundle(original)

    # Default False is not interpreted as "strip imported evidence".
    reexported = restored.to_bundle()

    assert reexported.to_dict() == original.to_dict()
    assert reexported.manifest["events"] is not None


def test_restored_trace_cannot_change_bundle_metadata():
    restored = TIDE.from_bundle(source_trace().to_bundle())

    with pytest.raises(
        ValueError,
        match="cannot change Bundle metadata",
    ):
        restored.to_bundle(
            handle={"metadata": {"changed": True}}
        )


def test_focused_view_of_restored_trace_refuses_rebundling():
    restored = TIDE.from_bundle(source_trace().to_bundle())
    exec_id = next(iter(restored.execs))

    focused = restored._trace_for_exec(exec_id)

    assert focused.from_cas is True

    with pytest.raises(
        RuntimeError,
        match="will not reserialise imported evidence",
    ):
        focused.to_bundle()
