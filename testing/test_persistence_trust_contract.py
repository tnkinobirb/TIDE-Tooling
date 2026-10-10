"""Release-gate tests for TIDE persisted execution evidence.

These tests are intentionally stricter than ordinary feature tests.

Contract:
- persisted evidence may be reconstructed, indexed, queried and presented;
- missing evidence must never be invented;
- unknown evidence must never be silently ignored;
- duplicate occurrence evidence must never be silently collapsed;
- CAS/backing values must never replace recorded State identity;
- optional Event sidecars must never alter native Trace semantics;
- a load -> re-bundle cycle must not rewrite recorded evidence.

Run:
    pytest -q testing/test_persistence_trust_contract.py
"""

from __future__ import annotations

import copy

import pytest

from tide_tooling import (
    DELETE_NOW,
    EMIT,
    READ,
    TIDE,
    WRITE_NOW,
    node,
    pipeline,
)
from tide_tooling.tooling_core.bundling.TIDEBundle import TIDEBundle
from tide_tooling.tooling_core.bundling.TIDEBundler import TIDEBundler
from tide_tooling.tooling_core.bundling.TIDEOpaqueValue import TIDEOpaqueValue
from tide_tooling.tooling_core.bundling.TIDERunSerialiser import (
    TIDERunSerialiser,
)
from tide_tooling.tooling_core.engine.classes import __fallback__
from tide_tooling.tooling_core.engine.events import PipelineEvents
from tide_tooling.tooling_core.engine.tide_runtime_logging import (
    trace_from_event_log,
)
from tide_tooling.tooling_core.engine.utils import tide_value_hasher


FAIL_CLOSED = (KeyError, TypeError, ValueError, AttributeError)


def _source_trace():
    tide = TIDE()
    tide.declare("source", "alpha")

    def consume(value):
        return {
            "value": value,
            "length": len(value),
        }

    tide.exec(
        consume,
        reads="source",
        writes="result",
    )
    return tide.trace()


def _payload(*, serialise_events=False):
    return _source_trace().to_bundle(
        serialise_events=serialise_events
    ).to_dict()


def _restore(payload):
    return TIDE.from_bundle(copy.deepcopy(payload))


def _run(payload):
    return payload["manifest"]["run"]


def _exec_records(run):
    return [
        record
        for record in run["records"]
        if record["record_type"] == "exec"
    ]


def _first_write(run):
    for record in _exec_records(run):
        if record["writes"]:
            return record["writes"][0]
    raise AssertionError("fixture produced no write")


def _first_static_dependency(run):
    for record in _exec_records(run):
        for dependency in record["deps"]:
            if dependency["type"] == "static":
                return dependency
    raise AssertionError("fixture produced no static dependency")


def _first_record_with_inputs(run):
    for record in _exec_records(run):
        if record["inputs"]:
            return record
    raise AssertionError("fixture produced no recorded inputs")


def _immediate_payload():
    @node(targets=["global:done"])
    def writer():
        WRITE_NOW("global:now", 1)
        return True

    tide = TIDE()
    tide.exec(writer)
    return tide.trace().to_bundle().to_dict()


def _affected_payload():
    tide = TIDE()
    tide.declare("source", "present")

    def remove():
        DELETE_NOW("global:source")

    def observe_missing():
        return READ("global:source", fallback="missing")

    tide.exec(remove)
    tide.exec(observe_missing)
    return tide.trace().to_bundle().to_dict()


def _emission_payload():
    @node(targets="global:emitted")
    def child():
        return "emitted"

    @node
    def emitter():
        return EMIT(child.as_node())

    @pipeline
    def flow():
        emitter()

    tide = TIDE()
    trace = tide.ocean.run_pipeline(flow, mode="sequential")
    return trace.to_bundle().to_dict()


def _unexecuted_emission_payload():
    @node(targets="global:never")
    def child(value):
        return value

    @node
    def emitter():
        return EMIT(child.as_node("@global:missing"))

    @pipeline
    def flow():
        emitter()

    tide = TIDE()
    trace = tide.ocean.run_pipeline(flow, mode="sequential")
    return trace.to_bundle().to_dict()


def _unresolved_ready_payload():
    @node(targets=["global:done"])
    def writer():
        return True

    tide = TIDE()
    tide.exec(writer)
    source = tide.trace()
    result_index = next(
        index
        for index, event in enumerate(source.events)
        if event.type == PipelineEvents.RESULT_READY
    )
    prefix = trace_from_event_log(
        source.events[:result_index],
        ocean_id=source.ocean_id,
    )
    return prefix.to_bundle().to_dict()


def _add_nested_node_snapshot_value(payload, depth):
    nested = {"kind": "literal", "value": None}
    for _ in range(depth):
        nested = {"kind": "list", "value": [nested]}
    _run(payload)["ready_attempts"][0]["node_snapshot"]["value"][
        "resource_probe"
    ] = nested


def _add_wide_node_snapshot_value(payload, width):
    _run(payload)["ready_attempts"][0]["node_snapshot"]["value"][
        "resource_probe"
    ] = {
        "kind": "list",
        "value": [
            {"kind": "literal", "value": index}
            for index in range(width)
        ],
    }


def _assert_rejected(payload):
    with pytest.raises(FAIL_CLOSED):
        _restore(payload)


def test_trace_value_entries_have_one_valid_hash_authority():
    payload = _payload()
    values = payload["manifest"]["values"]

    assert values
    assert all("value_hash" not in entry for entry in values.values())

    value_hash, entry = next(iter(values.items()))
    values.pop(value_hash)
    values["not-a-tide-hash"] = entry

    with pytest.raises(ValueError, match="valid TIDE value hash"):
        _restore(payload)


def test_tiderun_snapshot_hash_must_be_a_valid_tide_identity():
    payload = _payload()

    def corrupt_first_snapshot(value):
        if isinstance(value, dict):
            if "value_hash" in value and "capture_method" in value:
                value["value_hash"] = "not-a-tide-hash"
                return True
            return any(corrupt_first_snapshot(item) for item in value.values())
        if isinstance(value, list):
            return any(corrupt_first_snapshot(item) for item in value)
        return False

    assert corrupt_first_snapshot(_run(payload))

    with pytest.raises(ValueError, match="snapshot value_hash"):
        _restore(payload)


@pytest.mark.parametrize(
    ("field", "replacement", "message"),
    [
        (
            "value_hash",
            tide_value_hasher("unrelated snapshot donor"),
            "does not authenticate value_hash",
        ),
        (
            "type_name",
            "ForgedSnapshotType",
            "does not agree with type_name",
        ),
    ],
)
def test_tiderun_exact_snapshot_claims_authenticate_their_evidence(
    field,
    replacement,
    message,
):
    payload = _payload()

    def corrupt_first_exact_snapshot(value):
        if isinstance(value, dict):
            if (
                value.get("capture_method") == "serialised"
                and value.get("exact") is True
                and value.get("value_bundle") is not None
            ):
                value[field] = replacement
                return True
            return any(
                corrupt_first_exact_snapshot(item)
                for item in value.values()
            )
        if isinstance(value, list):
            return any(
                corrupt_first_exact_snapshot(item)
                for item in value
            )
        return False

    assert corrupt_first_exact_snapshot(_run(payload))

    with pytest.raises(ValueError, match=message):
        _restore(payload)


@pytest.mark.parametrize(
    "class_name",
    [
        "__fallback__",
        "__TIDE_TEMPLATE_VALUE__",
        "_TemplateMarker",
        "TideRunTemplate",
    ],
)
def test_user_class_names_cannot_claim_tide_template_semantics(class_name):
    user_type = type(class_name, (), {})
    reference = TIDERunSerialiser()._value_ref(user_type())

    assert reference["available"] is True
    assert "hash" in reference


def test_real_tide_fallback_remains_the_template_sentinel():
    serialiser = TIDERunSerialiser()

    assert serialiser._value_ref(__fallback__()) == {"available": False}


# ---------------------------------------------------------------------------
# C1: every field emitted by the current v1 TideRun schema is required.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "field",
    ["kind", "codec_version", "trace", "records", "ready_attempts"],
)
def test_missing_tiderun_top_level_field_is_rejected(field):
    payload = _payload()
    _run(payload).pop(field)
    _assert_rejected(payload)


def test_every_emitted_trace_metadata_field_is_required():
    baseline = _payload()
    fields = tuple(_run(baseline)["trace"])

    assert fields

    for field in fields:
        payload = copy.deepcopy(baseline)
        _run(payload)["trace"].pop(field)

        with pytest.raises(FAIL_CLOSED):
            _restore(payload)


def test_every_emitted_exec_record_field_is_required():
    baseline = _payload()
    source_record = _exec_records(_run(baseline))[0]
    fields = tuple(source_record)

    assert fields

    for field in fields:
        payload = copy.deepcopy(baseline)
        target = _exec_records(_run(payload))[0]
        target.pop(field)

        with pytest.raises(FAIL_CLOSED):
            _restore(payload)


def test_every_emitted_write_field_is_required():
    baseline = _payload()
    fields = tuple(_first_write(_run(baseline)))

    assert fields

    for field in fields:
        payload = copy.deepcopy(baseline)
        _first_write(_run(payload)).pop(field)

        with pytest.raises(FAIL_CLOSED):
            _restore(payload)


def test_every_emitted_static_dependency_field_is_required():
    baseline = _payload()
    fields = tuple(_first_static_dependency(_run(baseline)))

    assert fields

    for field in fields:
        payload = copy.deepcopy(baseline)
        _first_static_dependency(_run(payload)).pop(field)

        with pytest.raises(FAIL_CLOSED):
            _restore(payload)


def test_value_reference_requires_availability_and_hash():
    baseline = _payload()

    missing_available = copy.deepcopy(baseline)
    value_ref = _first_write(_run(missing_available))["stored_result"]
    assert value_ref["available"] is True
    value_ref.pop("available")
    _assert_rejected(missing_available)

    missing_hash = copy.deepcopy(baseline)
    value_ref = _first_write(_run(missing_hash))["stored_result"]
    assert value_ref["available"] is True
    value_ref.pop("hash")
    _assert_rejected(missing_hash)


def test_dangling_tiderun_value_hash_is_rejected():
    payload = _payload()
    value_ref = _first_write(_run(payload))["stored_result"]
    value_ref["hash"] = "sha3_512_tide:" + ("0" * 128)

    with pytest.raises(KeyError, match="missing from bundle"):
        _restore(payload)


def test_write_with_missing_state_identity_is_rejected():
    payload = _payload()
    write = _first_write(_run(payload))

    write["version"] = None

    with pytest.raises(ValueError, match="missing State"):
        _restore(payload)


# ---------------------------------------------------------------------------
# C2: evidence the decoder does not understand must not be silently dropped.
# These are v1 schema-integrity gates, not forward-compatibility tests.
# A future schema should increment codec_version.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "location",
    ["run", "trace", "exec", "write", "dependency"],
)
def test_unknown_tiderun_field_is_rejected(location):
    payload = _payload()
    run = _run(payload)

    if location == "run":
        target = run
    elif location == "trace":
        target = run["trace"]
    elif location == "exec":
        target = _exec_records(run)[0]
    elif location == "write":
        target = _first_write(run)
    else:
        target = _first_static_dependency(run)

    target["__unknown_persisted_evidence__"] = {
        "must_not_be_silently_ignored": True,
    }

    _assert_rejected(payload)


# ---------------------------------------------------------------------------
# C3: duplicate occurrence evidence must not be silently overwritten/deduped.
# ---------------------------------------------------------------------------


def test_duplicate_input_name_is_rejected_instead_of_overwritten():
    payload = _payload()
    record = _first_record_with_inputs(_run(payload))

    duplicate = copy.deepcopy(record["inputs"][0])
    record["inputs"].append(duplicate)

    with pytest.raises((TypeError, ValueError), match="duplicate|Duplicate"):
        _restore(payload)


def test_same_state_write_occurrences_are_preserved():
    payload = _payload()
    record = next(
        record
        for record in _exec_records(_run(payload))
        if record["writes"]
    )

    before = len(record["writes"])
    record["writes"].append(copy.deepcopy(record["writes"][0]))

    restored = _restore(payload)

    restored_record = next(
        item
        for item in restored.log
        if item.exec_id == record["exec_id"]
    )

    assert len(restored_record.writes) == before + 1

# ---------------------------------------------------------------------------
# Relationship-integrity gates.
# ---------------------------------------------------------------------------


def test_duplicate_exec_id_is_rejected():
    payload = _payload()
    records = _exec_records(_run(payload))
    assert len(records) >= 2

    records[1]["exec_id"] = records[0]["exec_id"]

    with pytest.raises(ValueError, match="Duplicate"):
        _restore(payload)


def test_read_cannot_claim_an_execution_that_did_not_produce_its_state():
    payload = _payload()
    records = _exec_records(_run(payload))
    dependency = _first_static_dependency(_run(payload))

    consumer = next(
        record
        for record in records
        if dependency in record["deps"]
    )
    dependency["producer_exec_id"] = consumer["exec_id"]

    with pytest.raises(ValueError, match="producer"):
        _restore(payload)


def test_persisted_read_cannot_reference_an_execution_outside_the_tiderun():
    payload = _payload()
    dependency = _first_static_dependency(_run(payload))

    dependency["producer_exec_id"] = "TIDE_EXEC:does-not-exist"

    with pytest.raises(ValueError, match="unknown execution"):
        _restore(payload)


def test_persisted_effect_cannot_affect_an_execution_outside_the_tiderun():
    payload = _affected_payload()
    affected = next(
        affected
        for record in _exec_records(_run(payload))
        for effect in record["effects"]
        for affected in effect["affected"]
    )

    affected["affected_exec_id"] = "TIDE_EXEC:does-not-exist"

    with pytest.raises(ValueError, match="unknown execution"):
        _restore(payload)


def test_persisted_effect_cannot_erase_a_represented_affected_execution():
    payload = _affected_payload()
    affected = next(
        affected
        for record in _exec_records(_run(payload))
        for effect in record["effects"]
        for affected in effect["affected"]
    )

    affected["affected_exec_id"] = None

    with pytest.raises(ValueError, match="affected execution identity"):
        _restore(payload)


def test_persisted_emission_cannot_claim_an_execution_outside_the_tiderun():
    payload = _emission_payload()
    child = next(
        record
        for record in _exec_records(_run(payload))
        if record["emitted_by_exec_id"] is not None
    )

    child["emitted_by_exec_id"] = "TIDE_EXEC:does-not-exist"

    with pytest.raises(ValueError, match="unknown execution"):
        _restore(payload)


def test_persisted_emission_cannot_erase_its_owner_identity():
    payload = _unexecuted_emission_payload()
    emitted = next(
        emitted
        for record in _exec_records(_run(payload))
        for emitted in record["emitted_nodes"]
    )

    emitted["emitted_by_exec_id"] = None

    with pytest.raises(ValueError, match="owner identity"):
        _restore(payload)


def test_persisted_immediate_reference_must_resolve_to_its_execution():
    payload = _immediate_payload()
    immediate = next(
        record
        for record in _run(payload)["records"]
        if record["record_type"] == "immediate_effect"
    )

    immediate["execution_reference"]["resolved_seq_id"] += 1

    with pytest.raises(ValueError, match="resolved_seq_id"):
        _restore(payload)


def test_persisted_ready_attempt_must_agree_with_its_execution():
    payload = _immediate_payload()
    attempt = _run(payload)["ready_attempts"][0]

    attempt["actor"]["name"] = "forged-actor"

    with pytest.raises(ValueError, match="ReadyAttempt"):
        _restore(payload)


def test_unresolved_ready_attempt_must_agree_with_its_node_snapshot():
    payload = _unresolved_ready_payload()
    attempt = _run(payload)["ready_attempts"][0]

    attempt["actor"]["name"] = "forged-actor"

    with pytest.raises(ValueError, match="func_name"):
        _restore(payload)


# ---------------------------------------------------------------------------
# Untrusted Bundle resource boundaries.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("trusted", [False, True])
def test_native_tiderun_depth_is_bounded_in_all_trust_modes(trusted):
    payload = _immediate_payload()
    _add_nested_node_snapshot_value(payload, 80)

    with pytest.raises(ValueError, match="depth"):
        TIDE.from_bundle(
            payload,
            trusted=trusted,
            resource_limits={"max_deserialise_depth": 64},
        )


def test_direct_bundle_constructor_enforces_native_tiderun_depth():
    payload = _immediate_payload()
    _add_nested_node_snapshot_value(payload, 80)
    bundler = TIDEBundler(
        resource_limits={"max_deserialise_depth": 64}
    )

    with pytest.raises(ValueError, match="Bundle depth"):
        TIDEBundle(payload, bundler=bundler)


def test_direct_bundle_constructor_enforces_native_container_width():
    payload = _immediate_payload()
    _add_wide_node_snapshot_value(payload, 65)
    bundler = TIDEBundler(resource_limits={"max_container_items": 64})

    with pytest.raises(ValueError, match="Bundle container items"):
        TIDEBundle(payload, bundler=bundler)


def test_bundle_cas_record_count_is_bounded_before_hydration():
    payload = _payload()
    assert len(payload["cas"]) > 1

    with pytest.raises(ValueError, match="CAS records"):
        TIDE.from_bundle(
            payload,
            resource_limits={"max_cas_records": 1},
        )


@pytest.mark.parametrize("trusted", [False, True])
def test_event_graph_record_count_is_bounded_in_all_trust_modes(trusted):
    payload = _payload(serialise_events=True)
    records = payload["manifest"]["events"]["events"]["records"]
    assert len(records) > 1

    with pytest.raises(ValueError, match="Event graph records"):
        TIDE.from_bundle(
            payload,
            trusted=trusted,
            resource_limits={"max_event_records": 1},
        )


# ---------------------------------------------------------------------------
# Optional Event evidence is a sidecar only.
# ---------------------------------------------------------------------------


def test_event_sidecar_does_not_change_native_trace_semantics():
    source = _source_trace()

    without_events = source.to_bundle(serialise_events=False)
    with_events = source.to_bundle(serialise_events=True)

    # Sidecar presence must not change TideRun at all.
    assert (
        without_events.manifest["run"]
        == with_events.manifest["run"]
    )

    plain = TIDE.from_bundle(without_events)
    sided = TIDE.from_bundle(with_events)

    assert plain.event_log is None
    assert sided.event_log is not None

    assert (
        dict(plain.index.value_hash_by_ref_version)
        == dict(sided.index.value_hash_by_ref_version)
    )
    assert plain.to_model(enrich=True) == sided.to_model(enrich=True)
    assert plain.corruption == sided.corruption
    assert plain.divergence == sided.divergence


def test_malformed_event_sidecar_is_rejected_not_used_as_trace_authority():
    payload = _payload(serialise_events=True)
    sidecar = payload["manifest"]["events"]

    assert isinstance(sidecar, dict)
    assert "count" in sidecar

    sidecar["count"] += 1

    _assert_rejected(payload)


# ---------------------------------------------------------------------------
# Clean persistence should be a semantic fixed point.
# ---------------------------------------------------------------------------


def test_clean_load_rebundle_load_is_a_native_fixed_point():
    source = _source_trace()
    first = source.to_bundle()
    restored_once = TIDE.from_bundle(first)
    second = restored_once.to_bundle()
    restored_twice = TIDE.from_bundle(second)

    assert first.manifest["run"] == second.manifest["run"]
    assert (
        dict(source.index.value_hash_by_ref_version)
        == dict(restored_once.index.value_hash_by_ref_version)
        == dict(restored_twice.index.value_hash_by_ref_version)
    )
    assert (
        source.to_model(enrich=True)
        == restored_once.to_model(enrich=True)
        == restored_twice.to_model(enrich=True)
    )
    assert restored_once.divergence == {}
    assert restored_twice.divergence == {}


# ---------------------------------------------------------------------------
# A/C boundary: corrupt backing values may diverge, but recorded identity must
# survive both load AND re-export.  This is a critical release gate.
# ---------------------------------------------------------------------------


def _divergent_bundle():
    source = _source_trace()
    bundle = source.to_bundle()
    payload = bundle.to_dict()

    state_items = list(
        source.index.value_hash_by_ref_version.items()
    )
    assert len(state_items) >= 2

    expected_state, expected_hash = state_items[0]
    donor_hash = next(
        value_hash
        for _, value_hash in state_items[1:]
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

    return (
        source,
        payload,
        expected_state,
        expected_hash,
        donor_hash,
    )


def test_divergent_backing_value_does_not_replace_recorded_state_identity():
    _, payload, state, expected_hash, donor_hash = _divergent_bundle()

    restored = _restore(payload)

    assert restored.index.value_hash_by_ref_version[state] == expected_hash
    assert restored.divergent is True
    assert any(
        warning.get("type") == "value_hash_mismatch"
        and warning.get("expected") == expected_hash
        and warning.get("observed") == donor_hash
        for warning in restored.divergence["warnings"]
    )
    restored_value = restored.value_of(state[0], state[1])
    assert isinstance(restored_value, TIDEOpaqueValue)
    assert restored_value.value_hash == expected_hash


def test_divergent_backing_cannot_rewrite_recorded_semantic_status():
    payload = _payload()
    run = _run(payload)
    record = next(
        record
        for record in _exec_records(run)
        if record["status"]["available"] is True
    )
    status_hash = record["status"]["hash"]
    donor_hash = next(
        value_hash
        for value_hash in payload["manifest"]["values"]
        if value_hash != status_hash
    )
    values = payload["manifest"]["values"]
    values[status_hash]["bundle"] = copy.deepcopy(
        values[donor_hash]["bundle"]
    )
    values[status_hash]["root_cas_ref"] = values[donor_hash][
        "root_cas_ref"
    ]

    with pytest.raises(
        ValueError,
        match="semantic value backing does not authenticate",
    ):
        _restore(payload)


def test_divergent_trace_rebundle_does_not_rewrite_recorded_state_identity():
    """A reconstructed backing value must never become new historical evidence."""
    _, payload, state, expected_hash, donor_hash = _divergent_bundle()

    restored_once = _restore(payload)
    rebundled = restored_once.to_bundle()
    restored_twice = TIDE.from_bundle(rebundled)

    assert (
        restored_once.index.value_hash_by_ref_version[state]
        == expected_hash
    )
    assert (
        restored_twice.index.value_hash_by_ref_version[state]
        == expected_hash
    )

    # Divergence remains a backing-integrity fact; re-export must not "heal"
    # it by rewriting the recorded State to donor_hash.
    assert restored_twice.divergent is True
    assert any(
        warning.get("type") == "value_hash_mismatch"
        and warning.get("expected") == expected_hash
        and warning.get("observed") == donor_hash
        for warning in restored_twice.divergence["warnings"]
    )
