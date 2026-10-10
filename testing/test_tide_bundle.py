from importlib.util import find_spec

import pytest

from tide_tooling import TIDE
from tide_tooling.tooling_core.bundling import TIDEBundle
from tide_tooling.tooling_core.bundling.TIDEBundle import _from_json_safe, _json_safe
from tide_tooling.tooling_core.bundling.TIDEBundler import TIDEBundler
from tide_tooling.tooling_core.engine.events import Cause
from tide_tooling.tooling_core.engine.tide_logging import (
    ExecRecord,
    Inputs,
    Trace,
    WriteRecord,
)
from tide_tooling.tooling_core.engine.utils import tide_value_hasher


requires_signing = pytest.mark.skipif(
    find_spec("cryptography") is None,
    reason="optional signing dependency is not installed",
)


def make_trace(values, ref_versions):
    records = []
    for seq_id, ((reference, version), value_hash) in enumerate(
        ref_versions.items()
    ):
        records.append(ExecRecord(
            seq_id=seq_id,
            exec_id=f"execution-{seq_id}",
            ocean_id="ocean",
            runner_id="runner",
            pipeline_id=None,
            node_id=f"writer-{seq_id}",
            func=None,
            func_name="writer",
            inputs=Inputs({}),
            deps=[],
            effects=[],
            declared_effects=[],
            declared_targets=[],
            runtime_effects=[],
            runtime_targets=[],
            declared_deps=[],
            unscoped_store_refs=[],
            status="succeeded",
            started_at=None,
            finished_at=None,
            duration_ms=None,
            error=None,
            output_value=None,
            writes=[WriteRecord(
                Cause(description="test fixture"),
                values[value_hash],
                version,
                reference,
            )],
            emitted_nodes=[],
            node_options={"preserve_exec_id": True},
        ))
    trace = Trace(
        log=records,
        name="TIDE: .trace()",
        ocean_id="ocean",
    )
    return trace


def test_bundle_values_returns_detached_hydrated_values():
    source = {"items": [1]}
    value_hash = tide_value_hasher(source)
    bundle = TIDEBundle.from_trace(
        make_trace(
            {value_hash: source},
            {("global:value", 0): value_hash},
        )
    )

    bundle.values[value_hash]["items"].append(2)

    assert bundle.value_of(value_hash) == {"items": [1]}


def test_outer_json_decoder_rejects_duplicate_equivalent_set_values():
    with pytest.raises(ValueError, match="duplicate-equivalent"):
        _from_json_safe({
            "$tide_json_type": "set",
            "items": [True, 1],
        })


def test_outer_json_decoder_enforces_aggregate_decoded_byte_budget():
    encoded = _json_safe([b"ab", b"cd"])

    with pytest.raises(ValueError, match="decoded bytes"):
        _from_json_safe(encoded, max_bytes=3)


def test_tide_bundle_declares_and_hydrates_a_trace_bundle():
    first = {"value": [b"first"]}
    second = ["second", 2]
    first_hash = tide_value_hasher(first)
    second_hash = tide_value_hasher(second)
    trace = make_trace(
        {first_hash: first, second_hash: second},
        {
            ("global:@A", 0): first_hash,
            ("global:@A", 1): second_hash,
            ("testing:@B", 0): first_hash,
        },
    )

    bundle = TIDEBundle.from_trace(
        trace,
        handle={"metadata": {"bundle": "ergonomic"}},
    )

    assert bundle.manifest["metadata"] == {"bundle": "ergonomic"}
    assert bundle.value_of(first_hash) == first
    assert bundle.value_at("global:@A", 1) == second
    assert bundle.versions("global:@A") == [0, 1]
    assert bundle.references_for(first_hash) == [
        {"ref": "global:@A", "version": 0},
        {"ref": "testing:@B", "version": 0},
    ]
    assert all(type(content) is bytes for content in bundle.cas_bytes.values())


def test_bundle_carries_exact_trace_corruption():
    clean = make_trace({}, {})
    corrupted = Trace((),
        name="TIDE: .trace()",
        ocean_id="ocean",
        missing_referenced_events={"missing": ("consequence",)},
    )

    assert TIDEBundle.from_trace(clean).corruption == {
        "missing_causal_events": {},
        "causal_order_violations": {},
        "evidence_violations": {},
    }

    bundle = TIDEBundle.from_trace(corrupted)

    assert bundle.corruption == {
        "missing_causal_events": {
            "missing": ("consequence",),
        },
        "causal_order_violations": {},
        "evidence_violations": {},
    }
    assert TIDEBundle.from_dict(bundle.to_dict()).corruption == bundle.corruption
    assert clean.corrupted is False
    assert corrupted.corrupted is True
    assert TIDEBundle.from_trace(clean).corrupted is False
    assert bundle.corrupted is True


def test_bundle_hash_identifies_the_complete_portable_representation():
    bundle = TIDEBundle.from_trace(make_trace({}, {}))
    restored = TIDEBundle.from_dict(bundle.to_dict())
    changed = TIDEBundle.from_trace(Trace((),
        name="TIDE: .trace()",
        ocean_id="ocean",
        missing_referenced_events={"missing": ("consequence",)},
    ))

    assert restored.bundle_hash == bundle.bundle_hash
    assert changed.bundle_hash != bundle.bundle_hash


def test_bundle_hash_is_independent_of_dictionary_insertion_order():
    bundle = TIDEBundle.from_trace(make_trace({}, {}))
    payload = bundle.to_dict()

    reordered = {
        key: payload[key]
        for key in reversed(tuple(payload))
    }
    reordered["manifest"] = {
        key: payload["manifest"][key]
        for key in reversed(tuple(payload["manifest"]))
    }

    assert TIDEBundle(reordered).bundle_hash == bundle.bundle_hash


@requires_signing
def test_bundle_signing_authenticates_canonical_content_and_round_trips():
    keys = TIDE.generate_signing_keys()
    unsigned = TIDEBundle.from_trace(make_trace({}, {}))
    signed = unsigned.sign(keys.private_key)
    restored = TIDEBundle.from_dict(_json_safe(signed.to_dict()))

    assert signed is not unsigned
    assert unsigned.signature is None
    assert signed.signature is not None
    assert signed.bundle_hash == unsigned.bundle_hash
    assert signed.verify(keys.public_key) is True
    assert restored.verify(keys.public_key) is True


@requires_signing
def test_bundle_verification_rejects_tampered_content_and_wrong_key():
    keys = TIDE.generate_signing_keys()
    other_keys = TIDE.generate_signing_keys()
    signed = TIDEBundle.from_trace(make_trace({}, {})).sign(keys.private_key)
    tampered = signed.to_dict()
    tampered["manifest"]["metadata"]["tampered"] = True

    with pytest.raises(ValueError, match="signature verification failed"):
        TIDEBundle.from_dict(tampered).verify(keys.public_key)

    with pytest.raises(ValueError, match="signature verification failed"):
        signed.verify(other_keys.public_key)


@requires_signing
def test_unsigned_bundle_cannot_satisfy_signature_verification():
    keys = TIDE.generate_signing_keys()
    bundle = TIDEBundle.from_trace(make_trace({}, {}))

    with pytest.raises(ValueError, match="not signed"):
        bundle.verify(keys.public_key)


@requires_signing
def test_trace_signing_and_tide_from_bundle_verification():
    keys = TIDE.generate_signing_keys()
    trace = make_trace({}, {})
    signed = trace.to_bundle(sign=keys.private_key)

    restored = TIDE.from_bundle(signed, verify=keys.public_key)

    assert restored.diff(trace).same


@requires_signing
def test_generated_signing_keys_save_standard_pem_files(tmp_path):
    keys = TIDE.generate_signing_keys()
    private_path = keys.save_private(tmp_path / "private.pem")
    public_path = keys.save_public(tmp_path / "public.pem")

    assert private_path.read_bytes().startswith(b"-----BEGIN PRIVATE KEY-----")
    assert public_path.read_bytes().startswith(b"-----BEGIN PUBLIC KEY-----")


def test_signing_dependency_is_required_only_when_signing_is_used(monkeypatch):
    from tide_tooling.tooling_core.bundling import TIDESigning

    real_import_module = TIDESigning.import_module

    def missing_cryptography(name):
        if name.startswith("cryptography"):
            raise ModuleNotFoundError(name)
        return real_import_module(name)

    monkeypatch.setattr(TIDESigning, "import_module", missing_cryptography)

    with pytest.raises(ImportError, match=r"pip install tide-tooling\[signing\]"):
        TIDE.generate_signing_keys()


def test_tide_bundle_round_trips_its_portable_payload():
    value = {"shared": [b"tide"]}
    value_hash = tide_value_hasher(value)
    original = TIDEBundle.from_trace(
        make_trace(
            {value_hash: value},
            {("global:@A", 3): value_hash},
        )
    )
    restored = TIDEBundle.from_dict(original.to_dict())

    assert restored.value_at("global:@A", 3) == value
    assert restored.values[value_hash] == value


def test_tide_bundle_returns_all_versions_when_version_is_omitted():
    first = [1]
    second = [2]
    first_hash = tide_value_hasher(first)
    second_hash = tide_value_hasher(second)
    bundle = TIDEBundle.from_trace(
        make_trace(
            {first_hash: first, second_hash: second},
            {
                ("global:@A", 0): first_hash,
                ("global:@A", 1): second_hash,
            },
        )
    )

    assert bundle.value_at("global:@A") == [first, second]


def test_json_safe_round_trip_escapes_the_reserved_type_key():
    value = {
        "$tide_json_type": "ordinary-host-field",
        "nested": {1: "non-string key"},
    }

    encoded = _json_safe(value)

    assert encoded["$tide_json_type"] == "dict"
    assert _from_json_safe(encoded) == value


def test_json_safe_set_and_non_string_dict_output_is_deterministic():
    first = _json_safe({("b", 2), ("a", 1)})
    second = _json_safe({("a", 1), ("b", 2)})
    mapping = _json_safe({2: "second", 1: "first"})

    assert first == second
    assert first["items"] == sorted(
        first["items"],
        key=lambda item: repr(item),
    )
    assert [item[0] for item in mapping["items"]] == [1, 2]


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (
            {"$tide_json_type": "tuple", "items": "not-a-list"},
            "tuple items must be a list",
        ),
        (
            {"$tide_json_type": "dict", "items": [["missing-value"]]},
            "dict item must be a pair",
        ),
        (
            {
                "$tide_json_type": "set",
                "items": [[1, 2]],
            },
            "set items must be hashable",
        ),
        (
            {
                "$tide_json_type": "dict",
                "items": [[1, "first"], [1, "second"]],
            },
            "Duplicate .*TIDE JSON dict key",
        ),
    ],
)
def test_json_safe_rejects_malformed_tagged_containers(payload, message):
    with pytest.raises((TypeError, ValueError), match=message):
        _from_json_safe(payload)


@pytest.mark.parametrize("value_type", ["tuple", "set", "frozenset", "dict"])
def test_json_safe_limits_tagged_container_items(value_type):
    items = [
        ["key", "value"],
        ["other", "value"],
        ["third", "value"],
    ]
    if value_type != "dict":
        items = ["first", "second", "third"]
    payload = {
        "$tide_json_type": value_type,
        "items": items,
    }

    with pytest.raises(ValueError, match=f"JSON {value_type} items 3 > 2"):
        _from_json_safe(payload, max_container_items=2)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -float("inf")])
def test_json_safe_rejects_non_finite_numbers(value):
    with pytest.raises(ValueError, match="Non-finite TIDE JSON value"):
        _json_safe(value)

    with pytest.raises(ValueError, match="Non-finite TIDE JSON value"):
        _from_json_safe(value)


def test_bundle_file_loader_rejects_non_standard_json_numbers(tmp_path):
    path = tmp_path / "non-finite.json"
    path.write_text('{"manifest": NaN, "cas": {}}', encoding="utf-8")

    with pytest.raises(ValueError, match="Non-finite TIDE JSON value"):
        TIDEBundle.from_file(path)
