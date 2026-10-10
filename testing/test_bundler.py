from tide_tooling.tooling_core.bundling.TIDEBundler import TIDEBundler
from tide_tooling import DataRef


class Handle:
    def __init__(self, metadata):
        self.metadata = metadata


def test_bundle_declares_a_root_and_local_records():
    bundle = TIDEBundler().bundle({"value": [1, 2]})

    assert bundle["root"] == "object_0"
    assert bundle["metadata"] == {}
    assert set(bundle) == {"root", "records", "metadata"}
    assert bundle["root"] in bundle["records"]


def test_bundle_container_items_reference_child_records():
    bundle = TIDEBundler().bundle({"value": [1, 2]})
    root = bundle["records"][bundle["root"]]
    item = root["items"][0]

    assert root["container_type"] == "dict"
    assert item["key"]["$ref"] in bundle["records"]
    assert item["value"]["$ref"] in bundle["records"]


def test_bundle_reuses_a_local_id_for_an_aliased_value():
    shared = ["value"]
    bundle = TIDEBundler().bundle([shared, shared])
    root = bundle["records"][bundle["root"]]

    assert root["items"][0] == root["items"][1]


def test_data_ref_is_safe_structural_data_in_untrusted_bundles():
    reference = DataRef(
        ("key", 1),
        "global",
        count=2,
        max=1,
        literal=True,
    )
    bundler = TIDEBundler()

    restored = bundler.deserialise(bundler.bundle(reference))

    assert restored == reference
    assert isinstance(restored, DataRef)


def test_bundle_accepts_handle_metadata_without_leaking_it_into_records():
    bundle = TIDEBundler().bundle(
        [1],
        handle=Handle({"ref": "@A", "version": 2}),
    )
    root = bundle["records"][bundle["root"]]

    assert bundle["metadata"] == {"ref": "@A", "version": 2}
    assert root["metadata"] == {}


def test_bundle_defaults_missing_or_invalid_handle_metadata_to_an_empty_dict():
    bundler = TIDEBundler()

    assert bundler.bundle([1], handle=None)["metadata"] == {}
    assert bundler.bundle(
        [1],
        handle={"metadata": "not a dict"},
    )["metadata"] == {}
