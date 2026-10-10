from tide_tooling import OpenLineageAdapter, TIDE


def _dataset(name, version=None):
    dataset = {"namespace": "warehouse", "name": name}
    if version is not None:
        dataset["facets"] = {"version": {"datasetVersion": version}}
    return dataset


def _event(run_id, event_type, *, inputs=(), outputs=(), job_name=None):
    return {
        "eventType": event_type,
        "eventTime": "2026-09-06T12:00:00Z",
        "run": {"runId": run_id},
        "job": {"namespace": "jobs", "name": job_name or f"job-{run_id}"},
        "inputs": list(inputs),
        "outputs": list(outputs),
        "producer": "https://example.test/producer",
    }


def test_openlineage_additive_run_events_project_once():
    events = [
        _event("prepare", "START", inputs=[_dataset("raw", "1")]),
        _event("prepare", "COMPLETE", outputs=[_dataset("ready", "1")]),
        _event("analyse", "COMPLETE", inputs=[_dataset("ready", "1")], outputs=[_dataset("report", "1")]),
    ]

    model = OpenLineageAdapter(events).to_model()
    assert model["host_record"] == events

    assert model["E"] == ["analyse", "prepare"]
    assert model["I"] == [
        ["analyse", '["warehouse","ready",0]'],
        ["prepare", '["warehouse","raw",0]'],
    ]
    assert model["O"] == [
        ["analyse", '["warehouse","report",0]'],
        ["prepare", '["warehouse","ready",0]'],
    ]
    assert "states" not in model
    assert model["executions"]["prepare"]["actor"] == {
        "ocean": "OpenLineage",
        "runner": "jobs",
        "name": "job-prepare",
    }
    assert model["executions"]["prepare"]["status"] == "succeeded"
    assert model["executions"]["prepare"]["start"] == "2026-09-06T12:00:00Z"
    assert model["executions"]["prepare"]["end"] == "2026-09-06T12:00:00Z"

    trace = TIDE.from_openlineage(events)
    assert trace.host == "OpenLineage"
    assert trace.host_record == events
    assert len(trace.log) == 3
    assert [record.exec_id for record in trace.log] == [
        "TIDE: External",
        "analyse",
        "prepare",
    ]
    assert trace.find("analyse").first("exec").object.object.exec_id == "analyse"


def test_openlineage_repeated_dataset_outputs_are_distinct_state_occurrences():
    events = [
        _event("first", "COMPLETE", outputs=[_dataset("shared")]),
        _event("second", "COMPLETE", outputs=[_dataset("shared")]),
    ]

    model = OpenLineageAdapter(events).to_model()

    assert model["O"] == [
        ["first", '["warehouse","shared",0]'],
        ["second", '["warehouse","shared",1]'],
    ]
    assert model["S"] == [
        '["warehouse","shared",0]',
        '["warehouse","shared",1]',
    ]
    assert TIDE.report_model_validity(model)["semantically_valid"] is True

    trace = TIDE.from_openlineage(events)
    writes = [write for record in trace.log for write in record.writes]
    assert [write.stored_version for write in writes] == [0, 1]
    assert {write.stored_target for write in writes} == {
        "__openlineage__:warehouse/shared"
    }


def test_openlineage_dataset_version_facet_does_not_replace_tide_versioning():
    events = [
        _event("first", "COMPLETE", outputs=[_dataset("shared", "1")]),
        _event("second", "COMPLETE", outputs=[_dataset("shared", "2")]),
    ]

    model = OpenLineageAdapter(events).to_model()
    assert model["S"] == [
        '["warehouse","shared",0]',
        '["warehouse","shared",1]',
    ]
    assert "states" not in model


def test_openlineage_input_uses_latest_prior_dataset_write():
    events = [
        _event("produce", "COMPLETE", outputs=[_dataset("shared")]),
        _event("consume", "COMPLETE", inputs=[_dataset("shared")]),
    ]

    model = OpenLineageAdapter(events).to_model()
    assert model["O"] == [["produce", '["warehouse","shared",0]']]
    assert model["I"] == [["consume", '["warehouse","shared",0]']]

    trace = TIDE.from_openlineage(events)
    consumer = next(record for record in trace.log if record.exec_id == "consume")
    assert consumer.deps[0].producer_exec_id == "produce"
    assert consumer.deps[0].resolved_version == 0


def test_openlineage_input_does_not_use_a_future_dataset_write():
    trace = TIDE.from_openlineage([
        _event("consume", "COMPLETE", inputs=[_dataset("shared")]),
        _event("produce", "COMPLETE", outputs=[_dataset("shared")]),
    ])
    external = next(
        record for record in trace.log
        if record.exec_id == "TIDE: External"
    )
    consumer = next(record for record in trace.log if record.exec_id == "consume")
    producer = next(record for record in trace.log if record.exec_id == "produce")

    assert external.seq_id < consumer.seq_id
    assert consumer.deps[0].producer_exec_id == external.exec_id
    assert consumer.deps[0].unscoped_reference == producer.writes[0].stored_target
    assert consumer.deps[0].resolved_version == 0
    assert producer.writes[0].stored_version == 1

    restored = TIDE.from_bundle(trace.to_bundle())
    restored_consumer = next(
        record for record in restored.log
        if record.exec_id == "consume"
    )
    assert restored_consumer.deps[0].producer_exec_id == external.exec_id


def test_openlineage_unknown_dataset_value_keeps_its_recorded_model_type_in_queries():
    trace = TIDE.from_openlineage([
        _event("produce", "COMPLETE", outputs=[_dataset("shared")]),
        _event("consume", "COMPLETE", inputs=[_dataset("shared")]),
    ])

    investigation = trace.investigate(trace.writes[0].stored_target)
    result = investigation.to_dict()

    expected = "opaque<TIDEUnknownValue>"
    assert result["history"][0]["value_type"] == expected
    assert result["writes"][0]["value_type"] == expected
    assert result["reads"][0]["value_type"] == expected
    shown = investigation.show(color=False)
    assert f"as {expected}" in shown
    assert "sha3_512_tide:" not in shown


def test_openlineage_repeated_job_runs_share_actor_with_distinct_occurrences():
    events = [
        _event("run-a", "COMPLETE", job_name="clean"),
        _event("run-b", "COMPLETE", job_name="clean"),
    ]

    trace = TIDE.from_openlineage(events)
    first = next(record for record in trace.log if record.exec_id == "run-a")
    second = next(record for record in trace.log if record.exec_id == "run-b")

    assert first.producer_id == second.producer_id
    assert first.producer_id.source_runner == "jobs"
    assert first.producer_id.source_actor == "clean"
    assert first.producer_occurrence_id.occurrence_id == 0
    assert second.producer_occurrence_id.occurrence_id == 1
