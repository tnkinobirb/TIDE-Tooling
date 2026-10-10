import hashlib
import os
import subprocess
import sys
import unittest

from tide_tooling.tooling_core.bundling.TIDEBundler import TIDEBundler
from tide_tooling.tooling_core.bundling.TIDECas import TIDECas
from tide_tooling.tooling_core.bundling.TIDEDeserialiser import TIDEDeserialiser
from tide_tooling.tooling_core.bundling.TIDESerialiser import TIDESerialiser
from tide_tooling.tooling_core.engine.utils import tide_value_hasher


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


def unsupported_function():
    pass


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
        bundler = TIDEBundler()
        bundle = bundler.bundle([shared, shared])
        root = bundle["records"][bundle["root"]]

        self.assertEqual(root["items"][0], root["items"][1])
        self.assertIn("$ref", root["items"][0])
        cached, _ = bundler.object_ids[id(shared)]
        self.assertIs(cached, shared)

        rebuilt = TIDEDeserialiser().deserialise_bundle(bundle)
        self.assertIs(rebuilt[0], rebuilt[1])

    def test_bundle_identity_memo_rejects_a_different_object(self):
        original = []
        replacement = []
        bundler = TIDEBundler()
        bundler.records = {}
        bundler.next_object_index = 0
        bundler.object_ids = {
            id(replacement): (original, "object_0"),
        }

        with self.assertRaisesRegex(RuntimeError, "identity collision"):
            bundler._add_value(replacement)

    def test_bundle_round_trips_a_list_cycle(self):
        value = []
        value.append(value)
        bundle = TIDEBundler().bundle(value)
        rebuilt = TIDEDeserialiser().deserialise_bundle(bundle)

        self.assertIs(rebuilt[0], rebuilt)

    def test_bundle_rejects_reference_records_with_extra_fields(self):
        shared = []
        bundle = TIDEBundler().bundle([shared, shared])
        root = bundle["records"][bundle["root"]]
        root["items"][0]["unexpected"] = True

        with self.assertRaisesRegex(ValueError, "only contain \\$ref"):
            TIDEDeserialiser().deserialise_bundle(bundle)

    def test_bundle_rejects_distinct_keys_that_decode_equal(self):
        serialiser = TIDESerialiser()
        record = {
            "serialiser": "container",
            "codec": "items",
            "codec_version": 1,
            "container_type": "dict",
            "items": [
                {
                    "key": serialiser.serialise(True),
                    "value": serialiser.serialise("bool"),
                },
                {
                    "key": serialiser.serialise(1),
                    "value": serialiser.serialise("int"),
                },
            ],
            "metadata": {},
        }

        with self.assertRaisesRegex(ValueError, "Duplicate decoded dict key"):
            TIDEDeserialiser().deserialise(record)

    def test_bundle_graph_rejects_distinct_keys_that_decode_equal(self):
        serialiser = TIDESerialiser()
        bundler = TIDEBundler()
        bundle = bundler.bundle({})
        root = bundle["records"][bundle["root"]]
        root["items"] = [
            {
                "key": serialiser.serialise(True),
                "value": serialiser.serialise("bool"),
            },
            {
                "key": serialiser.serialise(1),
                "value": serialiser.serialise("int"),
            },
        ]

        with self.assertRaisesRegex(ValueError, "Duplicate decoded dict key"):
            TIDEDeserialiser().deserialise_bundle(bundle)

    def test_unordered_value_hash_is_stable_across_hash_seeds(self):
        script = (
            "from tide_tooling.tooling_core.engine.utils import "
            "tide_value_hasher; "
            "print(tide_value_hasher({'alpha', 'beta', 'gamma'})); "
            "print(tide_value_hasher(frozenset({'alpha', 'beta', 'gamma'})))"
        )
        outputs = []
        for seed in ("1", "2"):
            environment = dict(os.environ)
            environment["PYTHONHASHSEED"] = seed
            outputs.append(
                subprocess.check_output(
                    [sys.executable, "-c", script],
                    env=environment,
                    text=True,
                ).splitlines()
            )

        self.assertEqual(outputs[0], outputs[1])
        self.assertNotEqual(outputs[0][0], outputs[0][1])

    def test_unordered_container_rejects_non_canonical_members(self):
        with self.assertRaisesRegex(ValueError, "non-canonical value"):
            TIDEBundler().bundle({unsupported_function})

        with self.assertRaisesRegex(ValueError, "non-canonical value"):
            tide_value_hasher({unsupported_function})

    def test_dictionary_insertion_order_remains_part_of_value_identity(self):
        first = {"alpha": 1, "beta": 2}
        second = {"beta": 2, "alpha": 1}

        self.assertEqual(first, second)
        self.assertNotEqual(
            tide_value_hasher(first),
            tide_value_hasher(second),
        )

    def test_hash_looking_string_is_hashed_as_an_ordinary_runtime_value(self):
        value_hash = tide_value_hasher(42)

        self.assertNotEqual(tide_value_hasher(value_hash), value_hash)

    def test_value_hashes_are_separated_by_exact_python_type(self):
        values = [
            None,
            b"null",
            1,
            b"1",
            b"value",
            bytearray(b"value"),
            memoryview(b"value"),
        ]

        hashes = [tide_value_hasher(value) for value in values]

        self.assertEqual(len(hashes), len(set(hashes)))

    def test_trace_bundle_hashes_and_stores_one_serialisation(self):
        class StatefulValue:
            def __init__(self):
                self.calls = 0

            def __getstate__(self):
                self.calls += 1
                return {"captured_call": self.calls}

        value = StatefulValue()
        complete = TIDEBundler().bundle_trace([value])

        self.assertEqual(value.calls, 1)
        self.assertEqual(
            complete["manifest"]["content"],
            "values",
        )

    def test_user_class_named_tide_opaque_value_cannot_supply_an_identity(self):
        impostor_type = type("TIDEOpaqueValue", (), {})
        impostor = impostor_type()
        impostor.value_hash = tide_value_hasher(42)

        self.assertNotEqual(tide_value_hasher(impostor), impostor.value_hash)

    def test_bundle_round_trips_object_state_modes(self):
        values = [
            DictObject("tide", 1),
            SlotsObject("ocean", 2),
            GetSetObject(3),
        ]

        for value in values:
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
