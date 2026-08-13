import pytest

from tide_tooling.tooling_core.bundling.TIDEBundler import TIDEBundler
from tide_tooling.tooling_core.bundling.TIDECas import TIDECas
from tide_tooling.tooling_core.bundling.TIDESerialiser import TIDESerialiser
from tide_tooling.tooling_core.engine.utils import tide_value_hasher


def test_cas_addresses_canonical_record_bytes_with_tide_value_hasher():
    record = TIDESerialiser().serialise({"value": [1, b"bytes"]})
    cas = TIDECas()
    reference = cas.put(record)

    assert reference == tide_value_hasher(cas.get_bytes(reference))
    assert reference.startswith("sha3_512_tide:")
    assert cas.get(reference) == record


def test_cas_deduplicates_identical_records_without_rewriting_them():
    record = TIDESerialiser().serialise([1, 2])
    cas = TIDECas()
    first_reference = cas.put(record)
    second_reference = cas.put(record)

    assert first_reference == second_reference
    assert len(cas) == 1


def test_cas_get_returns_a_copy_of_the_stored_record():
    record = TIDESerialiser().serialise({"value": 1})
    cas = TIDECas()
    reference = cas.put(record)
    loaded = cas.get(reference)
    loaded["metadata"]["changed"] = True

    assert cas.get(reference)["metadata"] == {}


def test_cas_store_and_load_bundle_preserves_local_graph_and_metadata():
    shared = [b"value"]
    bundle = TIDEBundler().bundle(
        {"first": shared, "second": shared},
        handle={"metadata": {"ref": "@A", "version": 1}},
    )
    cas = TIDECas()
    manifest = cas.store_bundle(bundle)
    restored_bundle = cas.load_bundle(manifest)

    assert manifest["root"] == bundle["root"]
    assert restored_bundle == bundle
    assert all(
        reference.startswith("sha3_512_tide:")
        for reference in manifest["record_refs"].values()
    )


def test_cas_rebuilds_a_bundle_from_exported_bytes_in_a_fresh_instance():
    shared = [b"value"]
    value = {"first": shared, "second": shared, "complex": 1 + 2j}
    bundle = TIDEBundler().bundle(value)
    source_cas = TIDECas()
    manifest = source_cas.store_bundle(bundle)
    transferred_bytes = source_cas.export_bytes()
    fresh_cas = TIDECas().import_bytes(transferred_bytes)
    restored = TIDEBundler().deserialise(
        fresh_cas.load_bundle(manifest)
    )

    assert all(type(content) is bytes for content in source_cas.records.values())
    assert fresh_cas.export_bytes() == transferred_bytes
    assert restored == value
    assert restored["first"] is restored["second"]


def test_cas_rejects_imported_bytes_with_a_mismatched_reference():
    cas = TIDECas()

    with pytest.raises(ValueError):
        cas.import_bytes({"sha3_512_tide:wrong": b"value"})


def test_cas_rejects_unknown_references():
    cas = TIDECas()

    with pytest.raises(KeyError):
        cas.get("sha3_512_tide:missing")

    with pytest.raises(KeyError):
        cas.get_bytes("sha3_512_tide:missing")


def test_cas_rejects_duplicate_keys_in_canonical_dict_records():
    cas = TIDECas()
    malformed = {
        "$type": "dict",
        "items": [
            ["metadata", {"$type": "dict", "items": []}],
            ["metadata", {"$type": "dict", "items": []}],
        ],
    }
    content = __import__("json").dumps(
        malformed,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")

    with pytest.raises(ValueError, match="Duplicate canonical dict key"):
        cas._decode_canonical_bytes(content)
