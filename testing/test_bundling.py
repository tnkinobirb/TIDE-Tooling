import hashlib
import unittest

from tide_tooling.tooling_core.bundling.TIDEBundler import TIDEBundler
from tide_tooling.tooling_core.bundling.TIDECas import TIDECas
from tide_tooling.tooling_core.bundling.TIDEDeserialiser import TIDEDeserialiser
from tide_tooling.tooling_core.bundling.TIDESerialiser import TIDESerialiser


class DictObject:
    def __init__(self, name, count):
        self.name = name
        self.count = count


class SlotsObject:
    __slots__ = ("name", "count")

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


class Handle:
    def __init__(self, metadata):
        self.metadata = metadata


class BundlingTests(unittest.TestCase):
    def test_inline_values_round_trip(self):
        serialiser = TIDESerialiser()
        deserialiser = TIDEDeserialiser()
        values = [
            None,
            True,
            42,
            3.5,
            2 + 3j,
            "tide 🌊",
            b"bytes\x00",
            bytearray(b"bytearray\x00"),
            memoryview(b"memoryview\x00"),
            [1, 2],
            (1, 2),
            {1, 2},
            frozenset({1, 2}),
            {"value": [1, b"nested"], "tuple": (2, 3)},
        ]

        for value in values:
            with self.subTest(value=type(value).__name__):
                rebuilt = deserialiser.deserialise(
                    serialiser.serialise(value)
                )

                if isinstance(value, memoryview):
                    self.assertIsInstance(rebuilt, memoryview)
                    self.assertEqual(bytes(rebuilt), bytes(value))
                else:
                    self.assertIs(type(rebuilt), type(value))
                    self.assertEqual(rebuilt, value)

    def test_bundle_uses_local_refs_and_preserves_aliases(self):
        shared = ["shared"]
        bundle = TIDEBundler().bundle([shared, shared])
        root = bundle["records"][bundle["root"]]

        self.assertEqual(root["items"][0], root["items"][1])
        self.assertIn("$ref", root["items"][0])

        rebuilt = TIDEDeserialiser().deserialise_bundle(bundle)
        self.assertIs(rebuilt[0], rebuilt[1])

    def test_bundle_round_trips_a_list_cycle(self):
        value = []
        value.append(value)
        bundle = TIDEBundler().bundle(value)
        rebuilt = TIDEDeserialiser().deserialise_bundle(bundle)

        self.assertIs(rebuilt[0], rebuilt)

    def test_bundle_round_trips_object_state_modes(self):
        values = [
            DictObject("tide", 1),
            SlotsObject("ocean", 2),
            GetSetObject(3),
        ]

        for value in values:
            with self.subTest(value=type(value).__name__):
                bundler = TIDEBundler(trusted=True)
                rebuilt = bundler.deserialise(bundler.bundle(value))

                self.assertIs(type(rebuilt), type(value))
                self.assertEqual(
                    self._object_values(rebuilt),
                    self._object_values(value),
                )

    def test_bundle_round_trips_an_object_cycle(self):
        value = DictObject("tide", 1)
        value.self = value
        bundle = TIDEBundler().bundle(value)
        rebuilt = TIDEDeserialiser(
            trusted=True
        ).deserialise_bundle(bundle)

        self.assertIs(rebuilt.self, rebuilt)

    def test_handle_metadata_is_bundle_metadata(self):
        bundle = TIDEBundler().bundle(
            [1],
            handle=Handle({"ref": "@A", "version": 2}),
        )

        self.assertEqual(
            bundle["metadata"],
            {"ref": "@A", "version": 2},
        )
        self.assertEqual(
            bundle["records"][bundle["root"]]["metadata"],
            {},
        )
        self.assertEqual(
            TIDEBundler().bundle(
                [1],
                handle={"metadata": "not a dict"},
            )["metadata"],
            {},
        )
        self.assertEqual(
            TIDESerialiser._normalise_record(
                {"metadata": "not a dict"}
            )["metadata"],
            {},
        )

    def test_cas_store_and_load_bundle(self):
        value = {"data": [b"x", b"y"]}
        bundle = TIDEBundler().bundle(
            value,
            handle={"metadata": {"ref": "@data", "version": 1}},
        )
        cas = TIDECas(self._tide_value_hasher)
        manifest = cas.store_bundle(bundle)
        restored_bundle = cas.load_bundle(manifest)
        rebuilt = TIDEDeserialiser().deserialise_bundle(restored_bundle)

        self.assertEqual(rebuilt, value)
        self.assertEqual(restored_bundle["metadata"], bundle["metadata"])
        self.assertTrue(
            all(
                reference.startswith("sha3_512_tide:")
                for reference in manifest["record_refs"].values()
            )
        )
        self.assertTrue(
            all(
                isinstance(cas.get_bytes(reference), bytes)
                for reference in manifest["record_refs"].values()
            )
        )

    @staticmethod
    def _object_values(value):
        if hasattr(value, "__dict__"):
            return {
                key: item
                for key, item in value.__dict__.items()
                if key != "self"
            }

        return {
            "name": value.name,
            "count": value.count,
        }

    @staticmethod
    def _tide_value_hasher(value):
        if not isinstance(value, bytes):
            raise TypeError("TIDECas must hash canonical bytes")

        hashed = hashlib.sha3_512(value).hexdigest()
        return f"sha3_512_tide:{hashed}"


if __name__ == "__main__":
    unittest.main()
