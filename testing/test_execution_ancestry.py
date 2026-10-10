import json

from tide_tooling import EMIT, TIDE, node, pipeline


def _emission_trace():
    @node(targets="global:seed")
    def seed():
        return 4

    @node(targets="global:child_result")
    def child(value):
        return value + 1

    @node
    def parent():
        return EMIT(child.as_node("@global:seed"))

    @pipeline
    def flow():
        seed()
        parent()

    return TIDE().ocean.run_pipeline(flow, mode="sequential")


def test_execution_ancestry_is_derived_from_exact_emission_evidence():
    trace = _emission_trace()
    parent = next(record for record in trace.log if record.func_name == "parent")
    child = next(record for record in trace.log if record.func_name == "child")

    children = trace.children_of(parent.exec_id)
    parents = trace.parents_of(child.seq_id)

    assert [record.exec_id for record in children] == [child.exec_id]
    assert [record.exec_id for record in parents] == [parent.exec_id]
    assert not trace.parents_of(parent.exec_id)
    assert not trace.children_of(child.exec_id)

    payload = children.to_dict()
    json.dumps(payload)
    assert payload["direction"] == "children"
    assert payload["source"]["exec_id"] == parent.exec_id
    assert payload["executions"][0]["exec_id"] == child.exec_id
    assert payload["executions"][0]["reads"][0]["reference"] == "global:seed"
    assert payload["executions"][0]["writes"][0]["reference"] == "global:child_result"


def test_execution_ancestry_rendering_is_not_evidence_or_diff_state():
    trace = _emission_trace()
    parent = next(record for record in trace.log if record.func_name == "parent")
    result = trace.children_of(parent.exec_id)
    before = trace.diff(trace).to_dict()

    full = result.show(show_in_out=True, color=False)
    compact = result.show(show_in_out=False, color=False)

    assert "EXEC_RECORD[" in full
    assert "used:" in full
    assert "wrote:" in full
    assert "EXEC_RECORD[" in compact
    assert "used:" not in compact
    assert "wrote:" not in compact
    assert result.to_dict()["executions"][0]["reads"]
    assert trace.diff(trace).to_dict() == before


def test_execution_ancestry_appears_in_trace_inspect_and_investigate():
    trace = _emission_trace()
    parent = next(record for record in trace.log if record.func_name == "parent")
    child = next(record for record in trace.log if record.func_name == "child")

    shown = trace.show(color=False)
    assert f"parent_exec: EXEC_RECORD[{parent.seq_id}]" in shown

    inspected = trace.inspect(child.exec_id)
    assert inspected.to_dict()["parents"][0]["exec_id"] == parent.exec_id
    assert "RELATIONSHIPS:" in inspected.show(color=False)

    investigated = trace.investigate(parent.exec_id)
    assert investigated.to_dict()["children"][0]["exec_id"] == child.exec_id
    assert "child: EXEC_RECORD[" in investigated.show(color=False)


def test_execution_ancestry_survives_bundle_round_trip():
    trace = _emission_trace()
    restored = TIDE.from_bundle(trace.to_bundle().to_dict())
    parent = next(record for record in restored.log if record.func_name == "parent")
    child = next(record for record in restored.log if record.func_name == "child")

    assert restored.children_of(parent.exec_id).to_dict()["executions"][0][
        "exec_id"
    ] == child.exec_id
    assert restored.parents_of(child.exec_id).to_dict()["executions"][0][
        "exec_id"
    ] == parent.exec_id
