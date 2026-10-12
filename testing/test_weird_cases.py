"""Small, unusual cases that should preserve recorded truth."""

from tide_tooling import RUN, TIDE
from tide_tooling.tooling_core.engine.classes import Node, Pipeline
from tide_tooling.tooling_core.engine.events import OceanEvents


def test_empty_pipeline_is_a_valid_run_with_no_execution():
    tide = TIDE()
    trace = tide.ocean.run_pipeline(Pipeline(nodes=[]), mode="sequential")

    assert trace.log == ()
    assert any(event.type == OceanEvents.RUN_PIPELINE for event in trace.events)
    tide.ocean.close()


def test_unicode_reference_keeps_zero_and_false_as_distinct_state_versions():
    tide = TIDE()
    tide.event("same actor", writes={"global:🐦": 0})
    tide.event("same actor", reads=["global:🐦"], writes={"global:🐦": False})
    trace = tide.trace()
    ref = "global:🐦"

    assert type(trace.value_of(ref, 0)) is int
    assert type(trace.value_of(ref, 1)) is bool
    assert trace.index.value_hash_by_ref_version[(ref, 0)] != trace.index.value_hash_by_ref_version[(ref, 1)]
    assert trace.log[1].deps[0].producer_exec_id == trace.log[0].exec_id
    tide.ocean.close()


def test_nested_duplicate_reads_survive_external_change_and_bundle():
    tide = TIDE()
    tide.event("producer", writes={"global:data": {"items": [1]}})

    def inspect_payload(payload):
        return payload["left"]["items"] + payload["again"][0]["items"]

    argument = {"left": "@global:data", "again": ["@global:data"]}
    tide.exec(inspect_payload, args=[argument], writes="before")
    tide.event("outside", reads={"global:data": {"items": [1, 2]}})
    tide.exec(inspect_payload, args=[argument], writes="after")
    trace = tide.trace()

    before = next(
        record for record in trace.log
        if any(write.stored_target == "global:before" for write in record.writes)
    )
    after = next(
        record for record in trace.log
        if any(write.stored_target == "global:after" for write in record.writes)
    )
    assert [read.input_path for read in before.deps] == [(0, "left"), (0, "again", 0), (0,)]
    assert [read.resolved_version for read in before.deps] == [0, 0, 0]
    assert [read.resolved_version for read in after.deps] == [2, 2, 0]
    assert trace.value_of("global:data", 0) == {"items": [1]}
    assert trace.value_of("global:data", 1) == {"items": [1, 2]}
    assert trace.value_of("global:data", 2) == {"items": [1]}

    rebuilt = TIDE.from_bundle(trace.to_bundle())
    rebuilt_before = next(
        record for record in rebuilt.log
        if any(write.stored_target == "global:before" for write in record.writes)
    )
    rebuilt_after = next(
        record for record in rebuilt.log
        if any(write.stored_target == "global:after" for write in record.writes)
    )
    assert [read.input_path for read in rebuilt_before.deps] == [
        (0, "left"), (0, "again", 0), (0,),
    ]
    assert [read.resolved_version for read in rebuilt_after.deps] == [2, 2, 0]
    tide.ocean.close()


def test_child_runs_write_separate_versions_without_cached_child_traces():
    tide = TIDE()
    child = tide.ocean.register_pipeline(Pipeline(nodes=[
        Node(lambda: "child", targets="global:shared"),
    ]))
    parent = Pipeline(nodes=[
        Node(
            lambda number=number: number,
            targets=f"global:parent_{number}",
            effects=RUN(child.id, mode="sequential"),
        )
        for number in range(3)
    ])
    trace = tide.ocean.run_pipeline(parent, mode="sequential")

    assert len(trace.log) == 6
    assert len({record.runner_id for record in trace.log}) == 4
    assert list(trace.index.versions_by_ref["global:shared"]) == [0, 1, 2]
    assert len({
        tuple(trace.index.producer_exec_ids_by_ref_version[("global:shared", version)])
        for version in range(3)
    }) == 3
    assert tide.ocean.last_trace is trace
    tide.ocean.close()
