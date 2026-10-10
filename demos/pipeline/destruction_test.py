"""Controlled destructive tests for native TIDE Bundle integrity.

Place this file next to ``demos/pipeline/data_pipeline.py`` and run:

    python demos/pipeline/destruction_test.py

The original Bundle is never mutated. Each test damages a fresh ``to_dict()``
copy so the cases stay independent.
"""

from copy import deepcopy

from tide_tooling import TIDE

from data_pipeline import pipeline


def _section(title):
    print()
    print("=" * 78)
    print(title)
    print("=" * 78)


def _write_value_hashes(bundle_dict):
    """Return distinct available value hashes used by persisted writes."""
    hashes = []

    run = bundle_dict["manifest"]["run"]

    for record in run["records"]:
        for write in record.get("writes", ()):
            value_ref = write.get("stored_result")

            if (
                isinstance(value_ref, dict)
                and value_ref.get("available") is True
            ):
                value_hash = value_ref["hash"]

                if value_hash not in hashes:
                    hashes.append(value_hash)

    return hashes


def _two_safe_value_hashes(bundle_dict):
    """Prefer two write values; fall back to any two persisted values."""
    hashes = _write_value_hashes(bundle_dict)

    if len(hashes) < 2:
        hashes = list(bundle_dict["manifest"]["values"])

    if len(hashes) < 2:
        raise RuntimeError(
            "Destruction demo needs at least two persisted values"
        )

    return hashes[0], hashes[1]


def _expect_failure(title, bundle_dict):
    _section(title)

    try:
        restored = TIDE.from_bundle(bundle_dict)
    except Exception as error:
        print(f"EXPECTED FAILURE: {type(error).__name__}: {error}")
        return

    print("UNEXPECTED SUCCESS")
    print("divergence:", restored.divergence)
    restored.show()


    raise AssertionError(
        f"{title} unexpectedly reconstructed a Trace"
    )


def clean_round_trip(source_trace, bundle):
    _section("0. CLEAN ROUND TRIP")

    restored = TIDE.from_bundle(bundle.to_dict())

    print("divergence:", restored.divergence)
    print("corruption:", restored.corruption)
    restored.show()

    assert restored.to_model() == source_trace.to_model()
    assert restored.divergence == source_trace.divergence


def semantic_value_hash_mismatch(bundle):
    """Valid CAS storage, but one semantic value identity points to another graph."""
    _section("1. VALUE HASH MISMATCH -> DIVERGENCE, NOT FAILURE")

    damaged = bundle.to_dict()
    values = damaged["manifest"]["values"]

    expected_hash, donor_hash = _two_safe_value_hashes(damaged)

    print("expected value hash:", expected_hash)
    print("donor value hash:   ", donor_hash)

    # Keep the semantic key/identity as ``expected_hash``, but replace its
    # persisted value graph with another internally valid graph.
    values[expected_hash]["bundle"] = deepcopy(
        values[donor_hash]["bundle"]
    )
    values[expected_hash]["root_cas_ref"] = (
        values[donor_hash]["root_cas_ref"]
    )

    restored = TIDE.from_bundle(damaged)

    print("divergence:", restored.divergence)
    restored.show()

    warnings = restored.divergence.get("warnings", [])

    assert any(
        warning.get("type") == "value_hash_mismatch"
        and warning.get("expected") == expected_hash
        for warning in warnings
    )
    restored.value_of("global:LOAD_FILE[v0]")
    restored.value_of("global:LOAD_FILE[v0]", show=True)
    restored.investigate("global:LOAD_FILE[v0]").show()
    restored.inspect("global:LOAD_FILE[v0]").show()
    restored.why("global:LOAD_FILE[v0]")
    restored.consumed_by("global:LOAD_FILE[v0]").show()
    restored.produced_by("global:LOAD_FILE[v0]").show()
    print("\nPASS: understandable mismatch was preserved and flagged.")


def missing_cas_record(bundle):
    """Delete storage evidence required to reconstruct a persisted value."""
    damaged = bundle.to_dict()
    value_hash, _ = _two_safe_value_hashes(damaged)

    root_cas_ref = damaged["manifest"]["values"][
        value_hash
    ]["root_cas_ref"]

    print()
    print("Deleting CAS reference:", root_cas_ref)
    del damaged["cas"][root_cas_ref]

    _expect_failure(
        "2. MISSING CAS EVIDENCE -> FAILURE",
        damaged,
    )


def tampered_cas_bytes(bundle):
    """Keep a CAS reference but replace the bytes behind it."""
    damaged = bundle.to_dict()
    value_hash, _ = _two_safe_value_hashes(damaged)

    root_cas_ref = damaged["manifest"]["values"][
        value_hash
    ]["root_cas_ref"]

    print()
    print("Tampering CAS reference:", root_cas_ref)
    damaged["cas"][root_cas_ref] = b"TIDE_DESTRUCTION_TEST"

    _expect_failure(
        "3. CAS CONTENT DOES NOT MATCH REFERENCE -> FAILURE",
        damaged,
    )


def missing_required_tiderun_field(bundle):
    """Remove evidence the v1 TideRun serializer always emits."""
    damaged = bundle.to_dict()

    exec_record = next(
        record
        for record in damaged["manifest"]["run"]["records"]
        if record["record_type"] == "exec"
    )

    print()
    print("Deleting TideRun field: exec_id")
    del exec_record["exec_id"]

    _expect_failure(
        "4. MISSING REQUIRED TIDERUN FIELD -> FAILURE",
        damaged,
    )


def wrong_tiderun_codec_version(bundle):
    damaged = bundle.to_dict()

    print()
    print("Changing TideRun codec_version: 1 -> 999")
    damaged["manifest"]["run"]["codec_version"] = 999

    _expect_failure(
        "5. WRONG TIDERUN CODEC VERSION -> FAILURE",
        damaged,
    )


def wrong_bundle_codec_version(bundle):
    damaged = bundle.to_dict()

    print()
    print("Changing Bundle codec_version: 1 -> 999")
    damaged["manifest"]["codec_version"] = 999

    _expect_failure(
        "6. WRONG BUNDLE CODEC VERSION -> FAILURE",
        damaged,
    )


def main():
    _section("BUILD SOURCE TRACE")

    source_trace = pipeline()
    bundle = source_trace.to_bundle(serialise_events=False)

    print("bundle hash:", bundle.bundle_hash)
    print("bundle codec:", bundle.manifest["codec_version"])
    print("TideRun codec:", bundle.manifest["run"]["codec_version"])
    print("values:", len(bundle.manifest["values"]))
    print("CAS records:", len(bundle.cas_bytes))

    clean_round_trip(source_trace, bundle)
    semantic_value_hash_mismatch(bundle)
    missing_cas_record(bundle)
    tampered_cas_bytes(bundle)
    missing_required_tiderun_field(bundle)
    wrong_tiderun_codec_version(bundle)
    wrong_bundle_codec_version(bundle)

    _section("DESTRUCTION TEST COMPLETE")
    print("All corruption cases behaved as expected.")


if __name__ == "__main__":
    main()
