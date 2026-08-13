from tide_tooling import TIDE


def _versioned_trace():
    tide = TIDE()
    tide.event("A0", writes={"@A": "A-v0"})
    tide.event("B0", reads={"@A": "A-v0"}, writes={"@B0": "from-v0"})
    tide.event("A1", writes={"@A": "A-v1"})
    tide.event("B1", reads={"@A": "A-v1"}, writes={"@B1": "from-v1"})
    return tide.trace()


def _read_versions(trace, seq_id):
    record = trace.inspect(seq_id, "seq_id").object
    return [
        (read.unscoped_reference, read.resolved_version)
        for read in record.reads
    ]


def test_bundle_round_trip_preserves_reference_versions_and_reads():
    restored = TIDE.from_bundle(_versioned_trace().to_bundle())

    assert _read_versions(restored, 1) == [("global:@A", 0)]
    assert _read_versions(restored, 3) == [("global:@A", 1)]
    assert restored.value_of("global:@A", 0) == "A-v0"
    assert restored.value_of("global:@A", 1) == "A-v1"

    a_v0 = restored.inspect("global:@A", "reference", version=0)
    a_v1 = restored.inspect("global:@A", "reference", version=1)
    b0 = restored.inspect("B0", "actor", grouped=False)
    b1 = restored.inspect("B1", "actor", grouped=False)

    assert restored.path(a_v0, b0).exists()
    assert restored.path(a_v1, b1).exists()
    assert not restored.path(a_v0, b1).exists()
    assert not restored.path(a_v1, b0).exists()


def test_bundle_round_trip_preserves_same_value_across_store_references():
    tide = TIDE()
    tide.ocean.register_store("testing")
    tide.event("Global", writes={"@A": "same"})
    tide.event("Testing", writes={"testing:@A": "same"})

    restored = TIDE.from_bundle(tide.trace().to_bundle())
    inspected = restored.inspect("same", "value")

    assert set(inspected.history) == {
        ("global:@A", 0),
        ("testing:@A", 0),
    }
