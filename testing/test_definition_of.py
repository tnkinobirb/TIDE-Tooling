import json

import pytest

from tide_tooling import Definition, TIDE, node, pipeline
from tide_tooling.tooling_core.engine.classes import EMIT


def test_definition_of_execution_exposes_complete_raw_declaration(capsys):
    @node(
        args=(1,),
        p_args=(2,),
        p_kwargs={"p": 3},
        k_args=(4,),
        k_kwargs={"k": 5},
        targets=["global:result"],
        force_update=True,
        one_shot=True,
        unpack=True,
        share_implicit_target=True,
    )
    def calculate(value):
        return (value,)

    @pipeline
    def flow():
        return calculate()

    trace = TIDE().ocean.run_pipeline(flow, mode="sequential")
    execution = next(
        record for record in trace.log
        if record.func_name == "calculate"
    )

    by_seq = trace.definition_of(execution.seq_id)
    by_exec = trace.definition_of(execution.exec_id)
    by_definition = trace.definition_of(execution.base_node_id)

    assert isinstance(by_seq, Definition)
    assert by_seq.to_dict() == by_exec.to_dict() == by_definition.to_dict()
    assert by_seq.args == (1,)
    assert by_seq.kwargs == {}
    assert by_seq.p_args == (2,)
    assert by_seq.p_kwargs == {"p": 3}
    assert by_seq.k_args == (4,)
    assert by_seq.k_kwargs == {"k": 5}
    assert by_seq.node_options == {
        "force_update": True,
        "one_shot": True,
        "unpack": True,
        "share_implicit_target": True,
    }

    payload = by_seq.to_dict()
    json.dumps(payload)
    assert payload["declaration"]["args"] == [1]
    assert payload["declaration"]["kwargs"] == {}
    assert payload["declaration"]["p_args"] == [2]
    assert payload["declaration"]["p_kwargs"] == {"p": 3}
    assert payload["declaration"]["k_args"] == [4]
    assert payload["declaration"]["k_kwargs"] == {"k": 5}
    assert payload["declaration"]["targets"] == ["global:result"]
    assert len(payload["executions"]) == 1

    shown = by_seq.show(color=False)
    capsys.readouterr()
    assert "DEFINITION | 1 execution" in shown
    assert "DECLARED INPUTS:" in shown
    for name in Definition.INPUT_NAMES:
        assert f"{name}:" in shown


def test_definition_of_emitted_unexecuted_node_survives_rebundling():
    @node(
        p_args=(2,),
        p_kwargs={"p": 3},
        k_args=(4,),
        k_kwargs={"k": 5},
        targets=["global:child_result"],
        force_update=True,
        one_shot=True,
        unpack=True,
        share_implicit_target=True,
    )
    def child(value, **kwargs):
        return value

    emitted_definition = child.as_node(
        "@global:missing",
        label="declared",
    )

    @node
    def emitter():
        return EMIT(emitted_definition)

    @pipeline
    def flow():
        return emitter()

    trace = TIDE().ocean.run_pipeline(flow, mode="sequential")
    emitted = trace.log[0].emitted_nodes[0]
    source = trace.definition_of(emitted.base_node)

    assert len(source) == 0
    assert source.emitted is True
    assert source.args == ("@global:missing",)
    assert source.kwargs == {"label": "declared"}
    assert source.p_args == (2,)
    assert source.p_kwargs == {"p": 3}
    assert source.k_args == (4,)
    assert source.k_kwargs == {"k": 5}
    assert source.emitted_by_exec_id == trace.log[0].exec_id

    restored_once = TIDE.from_bundle(trace.to_bundle())
    restored_twice = TIDE.from_bundle(restored_once.to_bundle())
    assert restored_once.definition_of(emitted.base_node).to_dict() == source.to_dict()
    assert restored_twice.definition_of(emitted.base_node).to_dict() == source.to_dict()


def test_repeated_emission_occurrences_expose_one_registration():
    @node
    def child():
        return "child"

    @node
    def emitter():
        definition = child.as_node()
        return EMIT([definition, definition])

    @pipeline
    def flow():
        return emitter()

    trace = TIDE().ocean.run_pipeline(flow, mode="sequential")
    occurrences = trace.log[0].emitted_nodes
    definition = trace.definition_of(occurrences[0].base_node)

    assert len(occurrences) == 2
    assert definition.emitted is True
    assert definition.emitted_by_exec_id == trace.log[0].exec_id
    assert len(definition) == 1


def test_definition_of_rejects_unknown_or_untyped_selectors():
    trace = TIDE().trace()

    with pytest.raises(KeyError, match="Node definition"):
        trace.definition_of("missing-definition")
    with pytest.raises(TypeError, match="Execution identity"):
        trace.definition_of(object())
