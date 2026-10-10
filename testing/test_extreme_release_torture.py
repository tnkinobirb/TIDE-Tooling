"""Composed release gate for TIDE's hardest evidence boundaries.

Each chamber keeps a distinct truth condition: valid evidence, authenticated
sidecar disagreement, divergent backing, invalid relationships, and concurrent
NOW evidence are never blended into one impossible Trace.
"""

from __future__ import annotations

import copy
import threading
from importlib.util import find_spec

import pytest

from tide_tooling import (
    TIDE,
    TIDEBundle,
    TIDEOpaqueValue,
    WRITE_NOW,
    node,
    pipeline,
)
from tide_tooling.tooling_core.engine.classes import DataRef
from tide_tooling.tooling_core.engine.events import (
    LogEvents,
    PipelineEvents,
)
from tide_tooling.tooling_core.engine.tide_records import ImmediateEffectRecord
from testing.test_execution_relationship_torture import _query_contract
from testing.test_forensic_trust_contract import (
    _event_fingerprint,
    _fingerprint,
)
from testing.test_persistence_trust_contract import _divergent_bundle
from testing.test_real_world_trust_test import (
    DYNAMIC_SELECTOR,
    _TruthLedger,
    _build_workload,
    _ledger_facts,
    _trace_facts,
)


pytestmark = pytest.mark.skipif(
    find_spec("cryptography") is None,
    reason="optional signing dependency is not installed",
)


def _run_truth_workload(root):
    root.mkdir()
    ledger = _TruthLedger()
    workload, _ = _build_workload(root, ledger)
    tide = TIDE()
    try:
        trace = tide.ocean.run_pipeline(
            workload,
            mode="sequential",
            dynamic_ref_plan={
                DYNAMIC_SELECTOR: ["global:batch_a", "global:batch_b"],
            },
        )
    finally:
        tide.ocean.close()
    return trace, ledger


def _assert_canonical_truth(trace, expected_facts, expected_queries, expected_model):
    assert _trace_facts(trace) == expected_facts
    assert _query_contract(trace) == expected_queries
    assert trace.to_model(identity_mode="literal") == expected_model
    assert TIDE.report_model_validity(expected_model)["semantically_valid"] is True
    assert trace.corrupted is False
    assert trace.divergent is False


def _canonical_signed_fixed_point(tmp_path, keys):
    source, ledger = _run_truth_workload(tmp_path / "canonical")
    expected_facts = _ledger_facts(ledger)
    expected_queries = _query_contract(source)
    expected_model = source.to_model(identity_mode="literal")
    expected_fingerprint = _fingerprint(source)
    expected_events = _event_fingerprint(source.events)
    _assert_canonical_truth(
        source,
        expected_facts,
        expected_queries,
        expected_model,
    )

    merged = source.value_of("global:merged", 0)
    assert merged["left"] is merged["right"]
    assert any(
        isinstance(record, ImmediateEffectRecord)
        for record in source.log
    )

    bundle = source.to_bundle(
        serialise_events=True,
        sign=keys.private_key,
    )
    canonical_payload = bundle.to_dict()
    canonical_hash = bundle.bundle_hash

    for _ in range(4):
        assert bundle.verify(keys.public_key) is True
        restored = TIDE.from_bundle(bundle, verify=keys.public_key)
        assert restored.events_verified is True
        assert source.diff(restored).same
        assert _fingerprint(restored) == expected_fingerprint
        assert _event_fingerprint(restored.events) == expected_events
        _assert_canonical_truth(
            restored,
            expected_facts,
            expected_queries,
            expected_model,
        )
        bundle = restored.to_bundle()
        assert bundle.to_dict() == canonical_payload
        assert bundle.bundle_hash == canonical_hash

    skipped = TIDE.from_bundle(bundle, verify_events=False)
    assert skipped.events_verified is None
    assert source.diff(skipped).same
    _assert_canonical_truth(
        skipped,
        expected_facts,
        expected_queries,
        expected_model,
    )
    return source, bundle, expected_facts, expected_queries, expected_model


def _authenticated_sidecar_mismatch(
    tmp_path,
    source,
    bundle,
    expected_facts,
    expected_queries,
    expected_model,
    keys,
):
    other, _ = _run_truth_workload(tmp_path / "other")
    other_bundle = other.to_bundle(serialise_events=True)
    mismatched = bundle.to_dict()
    mismatched["manifest"]["events"] = copy.deepcopy(
        other_bundle.manifest["events"]
    )
    mismatched.pop("signature", None)
    authenticated = TIDEBundle.from_dict(mismatched).sign(keys.private_key)

    assert authenticated.verify(keys.public_key) is True
    restored = TIDE.from_bundle(authenticated, verify=keys.public_key)
    assert restored.events_verified is False
    assert source.diff(restored).same
    _assert_canonical_truth(
        restored,
        expected_facts,
        expected_queries,
        expected_model,
    )


def _authentication_and_relationship_fail_closed(bundle, source, keys, wrong_keys):
    tampered = bundle.to_dict()
    tampered["manifest"]["metadata"]["tampered"] = True
    with pytest.raises(ValueError, match="signature verification failed"):
        TIDE.from_bundle(tampered, verify=keys.public_key)
    with pytest.raises(ValueError, match="signature verification failed"):
        TIDE.from_bundle(bundle, verify=wrong_keys.public_key)

    malformed = source.to_bundle().to_dict()
    records = [
        record
        for record in malformed["manifest"]["run"]["records"]
        if record["record_type"] == "exec"
    ]
    by_name = {record["func_name"]: record for record in records}
    by_name["aggregate"]["deps"][0]["producer_exec_id"] = by_name[
        "load_a"
    ]["exec_id"]
    authenticated = TIDEBundle.from_dict(malformed).sign(
        wrong_keys.private_key
    )
    assert authenticated.verify(wrong_keys.public_key) is True
    with pytest.raises(ValueError, match="did not write its claimed State"):
        TIDE.from_bundle(authenticated, verify=wrong_keys.public_key)


def _signed_divergence(keys):
    source, payload, state, expected_hash, donor_hash = _divergent_bundle()
    signed = TIDEBundle.from_dict(payload).sign(keys.private_key)
    assert signed.verify(keys.public_key) is True

    restored = TIDE.from_bundle(signed, verify=keys.public_key)
    assert restored.divergent is True
    assert source.diff(restored).same
    assert restored.index.value_hash_by_ref_version[state] == expected_hash
    assert any(
        warning.get("type") == "value_hash_mismatch"
        and warning.get("expected") == expected_hash
        and warning.get("observed") == donor_hash
        for warning in restored.divergence["warnings"]
    )
    value = restored.value_of(state[0], state[1])
    assert isinstance(value, TIDEOpaqueValue)
    assert value.value_hash == expected_hash

    reexported = restored.to_bundle()
    assert reexported.to_dict() == signed.to_dict()
    restored_again = TIDE.from_bundle(reexported, verify=keys.public_key)
    assert restored_again.divergent is True
    assert restored_again.index.value_hash_by_ref_version[state] == expected_hash


def _concurrent_now_linearisation(keys):
    start = threading.Barrier(4)

    def make_writer(label):
        @node(targets=[f"global:finished_{label}"])
        def writer():
            start.wait(timeout=2)
            assert WRITE_NOW("global:race", label) is True
            return label

        return writer

    writers = [make_writer(label) for label in ("a", "b", "c", "d")]

    @pipeline
    def race():
        return [writer() for writer in writers]

    tide = TIDE()
    try:
        trace = tide.ocean.run_pipeline(race, mode="thread")
        state_events = [
            event
            for event in trace.events
            if event.type in {
                PipelineEvents.DEP_AVAILABLE,
                PipelineEvents.DEP_UPDATED,
            }
            and DataRef.to_string(event.payload.ref, prefix=False)
            == "global:race"
        ]
        positions = {
            event.event_id: index
            for index, event in enumerate(trace.events)
        }

        assert len(state_events) == 4
        assert {event.payload.value for event in state_events} == {"a", "b", "c", "d"}
        race_states = {
            state
            for state in trace.index.value_hash_by_ref_version
            if state[0] == "global:race"
        }
        assert race_states == {
            ("global:race", 0),
            ("global:race", 1),
            ("global:race", 2),
            ("global:race", 3),
        }
        assert tide.ocean.store_router.get("global", "race") == (
            state_events[-1].payload.value
        )
        assert trace.value_of("global:race")[-1] == state_events[-1].payload.value

        for state_event in state_events:
            effect_event = next(
                event
                for event in trace.events
                if event.event_id == state_event.cause.event_id
            )
            assert effect_event.type == LogEvents.EFFECT
            assert positions[effect_event.event_id] < positions[state_event.event_id]

        for state in race_states:
            assert len(trace.index.producer_exec_ids_by_ref_version[state]) == 1

        signed = trace.to_bundle(
            serialise_events=True,
            sign=keys.private_key,
        )
        restored = TIDE.from_bundle(signed, verify=keys.public_key)
        assert restored.events_verified is True
        assert trace.diff(restored).same
        assert _fingerprint(trace) == _fingerprint(restored)
        assert trace.to_model(identity_mode="literal") == restored.to_model(
            identity_mode="literal"
        )
    finally:
        tide.ocean.close()

    assert not tide.ocean.running


def test_extreme_release_torture_gate(tmp_path):
    keys = TIDE.generate_signing_keys()
    other_keys = TIDE.generate_signing_keys()

    source, bundle, facts, queries, model = _canonical_signed_fixed_point(
        tmp_path,
        keys,
    )
    _authenticated_sidecar_mismatch(
        tmp_path,
        source,
        bundle,
        facts,
        queries,
        model,
        other_keys,
    )
    _authentication_and_relationship_fail_closed(
        bundle,
        source,
        keys,
        other_keys,
    )
    _signed_divergence(other_keys)
    _concurrent_now_linearisation(keys)
