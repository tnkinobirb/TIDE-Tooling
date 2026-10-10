import tide_tooling

from tide_tooling import ReadEdge, TIDE


def test_read_write_link_is_not_a_public_or_path_data_type():
    assert not hasattr(tide_tooling, "ReadWriteLink")


def test_trace_path_keeps_the_recorded_handoff_and_projects_it_for_output():
    tide = TIDE()
    tide.event("Producer", writes={"@A": "a"})
    tide.event("Consumer", reads=["@A"], writes={"@B": "b"})
    trace = tide.trace()

    source = trace.inspect("global:@A", "reference", version=0)
    target = trace.inspect("Consumer", "actor", grouped=False)
    pathed = trace.path(source, target)
    path = next(iter(pathed.paths.values()))[0]

    assert len(path["handoffs"]) == 1
    assert len(path["handoffs"][0]) == 1
    assert isinstance(path["handoffs"][0][0], ReadEdge)
    assert path["endpoint_evidence"] == []

    data = pathed.to_dict()["paths"][0]
    assert data["handoffs"][0] == [{
        "type": "read_edge",
        "producer_exec_id": trace.log[0].exec_id,
        "consumer_exec_id": trace.log[1].exec_id,
        "state": {
            "ref": "global:@A",
            "version": 0,
            "value_type": "str",
            "divergent": False,
        },
        "input": {"name": "args", "index": 0},
    }]

    output = pathed.show(color=False)
    assert "global:@A[v0]" in output
    assert "ReadWriteLink" not in output
