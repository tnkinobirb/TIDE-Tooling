import importlib

import pytest

from tide_tooling.tooling_core.bundling.TIDEBundler import TIDEBundler
from tide_tooling.tooling_core.bundling.TIDESerialiser import TIDESerialiser
from tide_tooling.tooling_core.bundling.TIDEDeserialiser import TIDEDeserialiser
from tide_tooling.tooling_core.bundling.TIDEOpaqueValue import TIDEOpaqueValue


@pytest.fixture
def serialiser():
    return TIDESerialiser()


@pytest.fixture
def deserialiser():
    return TIDEDeserialiser()


class ExampleObject:
    def __init__(self, name, count):
        self.name = name
        self.count = count


class ExampleSlots:
    __slots__ = ["name", "count"]

    def __init__(self, name, count):
        self.name = name
        self.count = count


class ExampleGetSet:
    def __init__(self, value):
        self.value = value

    def __getstate__(self):
        return {"value": self.value}

    def __setstate__(self, state):
        self.value = state["value"]


def unsupported_function():
    return "not portable"


def test_deserialise_round_trips_primitive(serialiser, deserialiser):
    record = serialiser.serialise(1 + 2j)

    assert deserialiser.deserialise(record) == 1 + 2j


def test_deserialise_round_trips_raw_bytes(serialiser, deserialiser):
    values = [
        b"value",
        bytearray(b"value"),
        memoryview(b"value"),
    ]

    for value in values:
        record = serialiser.serialise(value)
        restored = deserialiser.deserialise(record)

        assert type(restored) is type(value)
        assert bytes(restored) == bytes(value)


def test_deserialise_round_trips_containers(serialiser, deserialiser):
    values = [
        [1, b"value"],
        (1, b"value"),
        {1, 2},
        frozenset({1, 2}),
        {"value": [1, b"nested"]},
    ]

    for value in values:
        record = serialiser.serialise(value)
        restored = deserialiser.deserialise(record)

        assert type(restored) is type(value)
        assert restored == value


def test_deserialise_bundle_preserves_aliases_and_cycles(deserialiser):
    shared = ["value"]
    bundle = TIDEBundler().bundle([shared, shared])
    restored = deserialiser.deserialise_bundle(bundle)

    assert restored[0] is restored[1]

    cycle = []
    cycle.append(cycle)
    cycle_bundle = TIDEBundler().bundle(cycle)
    cycle_restored = deserialiser.deserialise_bundle(cycle_bundle)

    assert cycle_restored[0] is cycle_restored


def test_trusted_deserialiser_rebuilds_object_state_modes():
    deserialiser = TIDEDeserialiser(trusted=True)
    values = [
        ExampleObject("tide", 1),
        ExampleSlots("ocean", 2),
        ExampleGetSet(3),
    ]

    for value in values:
        bundle = TIDEBundler().bundle(value)
        restored = deserialiser.deserialise_bundle(bundle)

        assert type(restored) is type(value)

        if isinstance(value, ExampleSlots):
            assert restored.name == value.name
            assert restored.count == value.count
        else:
            assert restored.__dict__ == value.__dict__


def test_object_state_loading_is_untrusted_and_opaque_by_default(
    monkeypatch,
):
    bundle = TIDEBundler().bundle(ExampleObject("tide", 1))

    def reject_import(*args, **kwargs):
        raise AssertionError("untrusted loading imported recorded code")

    monkeypatch.setattr(importlib, "import_module", reject_import)

    restored = TIDEDeserialiser().deserialise_bundle(bundle)

    assert isinstance(restored, TIDEOpaqueValue)
    assert restored.type_name == "ExampleObject"
    assert restored.module == __name__
    assert restored.qualname == "ExampleObject"
    assert "untrusted bundles" in restored.reason


def test_deserialiser_trust_mode_must_be_explicit_bool():
    with pytest.raises(TypeError):
        TIDEDeserialiser(trusted="yes")

    with pytest.raises(TypeError):
        TIDEBundler(trusted="yes")


def test_deserialise_rejects_unknown_serialiser(deserialiser):
    record = {"serialiser": "missing"}

    with pytest.raises(NotImplementedError):
        deserialiser.deserialise(record)


def test_deserialise_rejects_unknown_codec_version(deserialiser):
    record = {
        "serialiser": "primitive",
        "codec": "json",
        "codec_version": 2,
        "payload": b"1",
    }

    with pytest.raises(ValueError):
        deserialiser.deserialise(record)


def test_deserialise_rejects_an_unresolvable_bundle_reference(deserialiser):
    bundle = {
        "root": "object_0",
        "records": {
            "object_0": {
                "kind": "list",
                "serialiser": "container",
                "container_type": "list",
                "items": [{"$ref": "missing"}],
                "metadata": {},
            }
        },
    }

    with pytest.raises(KeyError):
        deserialiser.deserialise_bundle(bundle)


def test_deserialiser_rejects_oversized_containers_before_rebuilding():
    primitive = {
        "serialiser": "primitive",
        "codec": "json",
        "codec_version": 1,
        "payload": b"1",
    }
    record = {
        "serialiser": "container",
        "container_type": "list",
        "items": [primitive, primitive],
    }

    with pytest.raises(ValueError, match="container items 2 > 1"):
        TIDEDeserialiser(max_container_items=1).deserialise(record)


def test_deserialiser_rejects_reference_depth_over_budget():
    bundle = TIDEBundler().bundle([[[[["tide"]]]]])

    with pytest.raises(ValueError, match="deserialisation depth"):
        TIDEDeserialiser(max_depth=3).deserialise_bundle(bundle)


def test_deserialiser_rejects_too_many_value_records():
    bundle = TIDEBundler().bundle([["first"], ["second"]])

    with pytest.raises(ValueError, match="value records"):
        TIDEDeserialiser(max_records=1).deserialise_bundle(bundle)


def test_deserialiser_resource_limits_require_positive_integers():
    with pytest.raises(ValueError, match="max_depth"):
        TIDEDeserialiser(max_depth=0)

    with pytest.raises(ValueError, match="max_container_items"):
        TIDEDeserialiser(max_container_items=True)


def test_unsupported_values_default_to_portable_opaque_values():
    serialiser = TIDESerialiser()
    record = serialiser.serialise(unsupported_function)
    restored = TIDEDeserialiser().deserialise(record)

    assert record["serialiser"] == "opaque"
    assert isinstance(restored, TIDEOpaqueValue)
    assert restored.type_name == "function"
    assert restored.module == __name__
    assert "unsupported_function" in restored.representation


def test_unsupported_values_can_use_strict_failure_mode():
    with pytest.raises(NotImplementedError):
        TIDEBundler(unsupported="strict").bundle(unsupported_function)
