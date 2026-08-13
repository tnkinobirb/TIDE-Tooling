import io
from dataclasses import dataclass
from pathlib import Path

import pytest

from tide_tooling.tooling_core.bundling.TIDESerialiser import TIDESerialiser


@pytest.fixture
def serialiser():
    return TIDESerialiser()


@dataclass
class ExampleData:
    name: str
    value: int


class ExampleSlots:
    __slots__ = ["value"]

    def __init__(self, value):
        self.value = value


class ExampleFileLike:
    def write(self, value):
        return value


def test_get_byte_value_kind_handles_bytes_and_streams(serialiser):
    assert serialiser._get_byte_value_kind(b"value") == "bytes"
    assert serialiser._get_byte_value_kind(
        bytearray(b"value")
    ) == "bytes_like"
    assert serialiser._get_byte_value_kind(
        memoryview(b"value")
    ) == "bytes_like"
    assert serialiser._get_byte_value_kind(
        io.BytesIO(b"value")
    ) == "byte_stream"
    assert serialiser._get_byte_value_kind(
        io.StringIO("value")
    ) == "text_stream"
    assert serialiser._get_byte_value_kind(
        ExampleFileLike()
    ) == "file_like"


def test_get_value_kind_handles_common_explicit_types(serialiser):
    assert serialiser.get_value_kind(None) == "none"
    assert serialiser.get_value_kind(1) == "int"
    assert serialiser.get_value_kind({"value": 1}) == "dict"
    assert serialiser.get_value_kind([1, 2]) == "list"
    assert serialiser.get_value_kind(
        ExampleData("value", 1)
    ) == "dataclass"
    assert serialiser.get_value_kind(
        Path("value.txt")
    ) == "path"


def test_choose_serialiser_selects_explicit_strategies(serialiser):
    assert serialiser.choose_serialiser(1) == "primitive"
    assert serialiser.choose_serialiser(b"value") == "bytes"
    assert serialiser.choose_serialiser([1, 2]) == "container"
    assert serialiser.choose_serialiser(
        ExampleData("value", 1)
    ) == "dataclass"
    assert serialiser.choose_serialiser(
        Path("value.txt")
    ) == "path"


def test_choose_serialiser_selects_stream_strategies(serialiser):
    assert serialiser.choose_serialiser(
        io.BytesIO(b"value")
    ) == "stream"

    assert serialiser.choose_serialiser(
        io.StringIO("value")
    ) == "text_stream"


def test_serialise_returns_a_primitive_record(serialiser):
    record = serialiser.serialise(1)

    assert record["kind"] == "int"
    assert record["serialiser"] == "primitive"
    assert record["codec"] == "json"
    assert record["codec_version"] == 1
    assert record["payload"] == b"1"
    assert record["metadata"] == {}


def test_serialise_returns_a_raw_bytes_record(serialiser):
    record = serialiser.serialise(bytearray(b"value"))

    assert record["kind"] == "bytes_like"
    assert record["type"] == "bytearray"
    assert record["serialiser"] == "bytes"
    assert record["codec"] == "raw"
    assert record["codec_version"] == 1
    assert record["payload"] == b"value"
    assert record["metadata"] == {}


def test_serialise_returns_consistent_container_records(serialiser):
    dictionary = serialiser.serialise(
        {"values": [1, 2]}
    )
    sequence = serialiser.serialise([1, 2])
    tuple_value = serialiser.serialise((1, 2))

    for record in [dictionary, sequence, tuple_value]:
        assert record["serialiser"] == "container"
        assert isinstance(record["items"], list)

    assert dictionary["kind"] == "dict"
    assert dictionary["container_type"] == "dict"

    assert sequence["kind"] == "list"
    assert sequence["container_type"] == "list"

    assert tuple_value["kind"] == "tuple"
    assert tuple_value["container_type"] == "tuple"

    nested_value = dictionary["items"][0]["value"]
    assert nested_value["metadata"] == {}


def test_serialise_with_refs_returns_local_reference_records(serialiser):
    referenced_values = []

    def reference_value(value):
        reference = f"object_{len(referenced_values)}"
        referenced_values.append(value)
        return reference

    record = serialiser.serialise_with_refs(
        {"values": [1, 2]},
        reference_value,
    )

    assert record["metadata"] == {}
    assert record["items"] == [
        {
            "key": {"$ref": "object_0"},
            "value": {"$ref": "object_1"},
        }
    ]
    assert referenced_values == ["values", [1, 2]]


def test_normalise_record_defaults_invalid_metadata_to_an_empty_dict():
    record = TIDESerialiser._normalise_record(
        {"metadata": "not a dict"}
    )

    assert record["metadata"] == {}


def test_get_object_state_records_dict_and_slots(serialiser):
    class ExampleObject:
        def __init__(self):
            self.value = 1

    dictionary_state = serialiser.get_object_state(
        ExampleObject()
    )

    slot_state = serialiser.get_object_state(
        ExampleSlots(1)
    )

    assert dictionary_state["kind"] == "object"
    assert dictionary_state["has_dict"] is True
    assert dictionary_state["dict"] == {"value": 1}

    assert slot_state["has_slots"] is True
    assert slot_state["slots"] == {"value": 1}


def test_serialise_returns_an_object_state_record(serialiser):
    class ExampleObject:
        def __init__(self):
            self.value = 1

    record = serialiser.serialise(ExampleObject())

    assert record["kind"] == "object"
    assert record["serialiser"] == "object_state"
    assert record["codec"] == "state"
    assert record["state"]["mode"] == "dict_slots"
    assert record["state"]["slots"] is None


def test_get_value_kind_handles_numpy_when_available(serialiser):
    numpy = pytest.importorskip("numpy")

    assert serialiser.get_value_kind(
        numpy.array([1, 2])
    ) == "numpy_array"

    assert serialiser.get_value_kind(
        numpy.int64(1)
    ) == "numpy_scalar"

