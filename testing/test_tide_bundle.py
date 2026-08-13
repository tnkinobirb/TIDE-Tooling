from types import SimpleNamespace

import pytest

from tide_tooling.tooling_core.bundling import TIDEBundle
from tide_tooling.tooling_core.bundling.TIDEBundle import _from_json_safe, _json_safe
from tide_tooling.tooling_core.bundling.TIDEBundler import TIDEBundler
from tide_tooling.tooling_core.engine.utils import tide_value_hasher


def make_trace(values, ref_versions):
    return SimpleNamespace(
        name="TIDE: .trace()",
        ocean_id="ocean",
        run="run_1",
        pipeline="TIDE: UNKNOWN",
        index=SimpleNamespace(
            value_by_hash=values,
            value_hash_by_ref_version=ref_versions,
        ),
    )


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


def test_tide_bundle_round_trips_its_portable_payload():
    value = {"shared": [b"tide"]}
    value_hash = tide_value_hasher(value)
    original = TIDEBundler().declare(
        make_trace(
            {value_hash: value},
            {("global:@A", 3): value_hash},
        )
    )
    restored = TIDEBundle.from_dict(original.to_dict())

    assert restored.value_at("global:@A", 3) == value
    assert restored.values == {value_hash: value}


def test_tide_bundle_requires_a_version_when_a_reference_is_ambiguous():
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

    with pytest.raises(ValueError):
        bundle.value_at("global:@A")


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
            "Duplicate TIDE JSON dict key",
        ),
    ],
)
def test_json_safe_rejects_malformed_tagged_containers(payload, message):
    with pytest.raises(ValueError, match=message):
        _from_json_safe(payload)


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
