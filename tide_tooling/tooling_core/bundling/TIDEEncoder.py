import json


class TIDEEncoder:
    def encode_python_primatives(
        self,
        value: None | bool | int | float | complex | str,
    ):
        primitive_types = [
            type(None),
            bool,
            int,
            float,
            complex,
            str,
        ]

        if type(value) not in primitive_types:
            raise TypeError(
                f"Unsupported primitive type: {type(value).__name__}"
            )

        if isinstance(value, complex):
            value = {
                "$type": "complex",
                "real": value.real,
                "imag": value.imag,
            }

        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")

    def _reject_non_finite_json_value(self, value):
        raise ValueError(f"Non-finite JSON value: {value}")

    def decode_python_primatives(self, encoded: bytes):
        value = json.loads(
            encoded.decode("utf-8"),
            parse_constant=self._reject_non_finite_json_value,
        )

        if isinstance(value, dict):
            if set(value) != {"$type", "real", "imag"}:
                raise ValueError("Unsupported primitive object")

            if value["$type"] != "complex":
                raise ValueError(
                    f"Unsupported primitive tag: {value['$type']}"
                )

            if type(value["real"]) not in [int, float]:
                raise ValueError(
                    "Complex real value must be numeric"
                )

            if type(value["imag"]) not in [int, float]:
                raise ValueError(
                    "Complex imaginary value must be numeric"
                )

            return complex(value["real"], value["imag"])

        if type(value) not in [
            type(None),
            bool,
            int,
            float,
            str,
        ]:
            raise ValueError(
                "Encoded value is not a Python primitive"
            )

        return value

    def encode_bytes(self, value: bytes | bytearray | memoryview):
        if not isinstance(value, (bytes, bytearray, memoryview)):
            raise TypeError(
                f"Unsupported byte type: {type(value).__name__}"
            )

        return bytes(value)

    def decode_bytes(self, encoded: bytes, value_type: str = "bytes"):
        if not isinstance(encoded, bytes):
            raise TypeError("Encoded byte payload must be bytes")

        if value_type == "bytearray":
            return bytearray(encoded)

        if value_type == "memoryview":
            return memoryview(encoded)

        if value_type == "bytes":
            return bytes(encoded)

        raise ValueError(f"Unsupported byte type: {value_type}")
