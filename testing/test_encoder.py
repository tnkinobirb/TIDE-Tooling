import math

import pytest

from tide_tooling.tooling_core.bundling.TIDEEncoder import TIDEEncoder


@pytest.fixture
def encoder():
    return TIDEEncoder()


def test_encode_python_primatives_round_trips_atomic_values(encoder):
    values = [
        None,
        True,
        False,
        1,
        -1,
        1.5,
        -0.0,
        "value",
        "é",
        1 + 2j,
    ]

    for value in values:
        encoded = encoder.encode_python_primatives(value)
        decoded = encoder.decode_python_primatives(encoded)

        assert decoded == value

    decoded_negative_zero = encoder.decode_python_primatives(
        encoder.encode_python_primatives(-0.0)
    )

    assert math.copysign(1.0, decoded_negative_zero) == -1.0


def test_encode_python_primatives_is_deterministic(encoder):
    assert encoder.encode_python_primatives("é") == (
        '"é"'.encode("utf-8")
    )

    assert encoder.encode_python_primatives(1) == b"1"

    assert encoder.encode_python_primatives(1 + 2j) == (
        b'{"$type":"complex","imag":2.0,"real":1.0}'
    )


def test_encode_python_primatives_rejects_containers_and_bytes(encoder):
    with pytest.raises(TypeError):
        encoder.encode_python_primatives([1, 2])

    with pytest.raises(TypeError):
        encoder.encode_python_primatives({"value": 1})

    with pytest.raises(TypeError):
        encoder.encode_python_primatives(b"value")


def test_encode_python_primatives_rejects_non_finite_floats(encoder):
    for value in [math.nan, math.inf, -math.inf]:
        with pytest.raises(ValueError):
            encoder.encode_python_primatives(value)
