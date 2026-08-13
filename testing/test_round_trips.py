import math

import pytest

from tide_tooling.tooling_core.bundling.TIDEBundler import TIDEBundler
from tide_tooling.tooling_core.bundling.TIDECas import TIDECas


class DictObject:
    def __init__(self, name, count):
        self.name = name
        self.count = count


class SlotsObject:
    __slots__ = ["name", "count"]

    def __init__(self, name, count):
        self.name = name
        self.count = count


class GetSetObject:
    def __init__(self, value):
        self.value = value

    def __getstate__(self):
        return {"value": self.value}

    def __setstate__(self, state):
        self.value = state["value"]


@pytest.fixture
def bundler():
    return TIDEBundler()


@pytest.fixture
def cas():
    return TIDECas()


def round_trip(value, bundler, cas, handle=None):
    bundle = bundler.bundle(value, handle=handle)
    manifest = cas.store_bundle(bundle)
    loaded_bundle = cas.load_bundle(manifest)

    return bundler.deserialise(loaded_bundle), bundle, manifest


def test_round_trip_supported_primitives(bundler, cas):
    values = [
        None,
        True,
        False,
        0,
        -1,
        1.5,
        -0.0,
        1 + 2j,
        "tide",
        "tïdé",
    ]

    for value in values:
        restored, _, _ = round_trip(value, bundler, cas)

        assert type(restored) is type(value)
        assert restored == value

        if isinstance(value, float) and value == 0:
            assert math.copysign(1, restored) == math.copysign(1, value)


def test_round_trip_supported_byte_values(bundler, cas):
    values = [
        b"tide-bytes",
        bytearray(b"tide-bytearray"),
        memoryview(b"tide-memoryview"),
    ]

    for value in values:
        restored, _, _ = round_trip(value, bundler, cas)

        assert type(restored) is type(value)
        assert bytes(restored) == bytes(value)


def test_round_trip_supported_containers(bundler, cas):
    values = [
        ["tide", b"bytes", 1],
        ("tide", b"bytes", 1),
        {"tide", "ocean"},
        frozenset({"tide", "ocean"}),
        {
            "nested": [b"bytes", {"value": 1}],
            2: ("tuple",),
        },
    ]

    for value in values:
        restored, _, _ = round_trip(value, bundler, cas)

        assert type(restored) is type(value)
        assert restored == value


def test_round_trip_supported_object_state_modes(bundler, cas):
    bundler = TIDEBundler(trusted=True)
    values = [
        DictObject("tide", 1),
        SlotsObject("ocean", 2),
        GetSetObject({"value": [3]}),
    ]

    for value in values:
        restored, _, _ = round_trip(value, bundler, cas)

        assert type(restored) is type(value)

        if isinstance(value, SlotsObject):
            assert restored.name == value.name
            assert restored.count == value.count
        else:
            assert restored.__dict__ == value.__dict__


def test_round_trip_preserves_aliases_and_mutable_cycles(bundler, cas):
    shared = ["tide"]
    restored, _, _ = round_trip([shared, shared], bundler, cas)

    assert restored[0] is restored[1]

    cycle = []
    cycle.append(cycle)
    cycle_restored, _, _ = round_trip(cycle, bundler, cas)

    assert cycle_restored[0] is cycle_restored


def test_round_trip_keeps_handle_metadata_outside_value_records(bundler, cas):
    restored, bundle, manifest = round_trip(
        ["tide"],
        bundler,
        cas,
        handle={"metadata": {"ref": "@A", "version": 1}},
    )

    root_record = bundle["records"][bundle["root"]]

    assert restored == ["tide"]
    assert bundle["metadata"] == {"ref": "@A", "version": 1}
    assert manifest["metadata"] == {"ref": "@A", "version": 1}
    assert root_record["metadata"] == {}
