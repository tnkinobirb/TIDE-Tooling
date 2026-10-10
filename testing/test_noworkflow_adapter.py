from importlib.util import find_spec
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

from tide_tooling import Effects, NOWAdapter, NOWMeta, TIDE
from tide_tooling.tooling_core.bundling.TIDEOpaqueValue import TIDEOpaqueValue


def _activation(identity, name, start, finish, output, line, evaluation_id=None):
    return SimpleNamespace(
        id=identity,
        name=name,
        start_checkpoint=start,
        finish_checkpoint=finish,
        this_evaluation=SimpleNamespace(
            id=identity if evaluation_id is None else evaluation_id,
            repr=output,
            member_container=None,
            memberships_as_collection=[],
        ),
        line=line,
    )


def _evaluation(identity, activation_id, value, name, kind, mode, line):
    return SimpleNamespace(
        id=identity,
        activation_id=activation_id,
        checkpoint=identity / 100,
        repr=value,
        member_container=None,
        memberships_as_collection=[],
        code_component=SimpleNamespace(
            name=name,
            type=kind,
            mode=mode,
            first_char_line=line,
        ),
    )


def _dependency(dependent_id, dependency_id, dependency_activation_id=0):
    return SimpleNamespace(
        dependent_id=dependent_id,
        dependency_id=dependency_id,
        dependency_activation_id=dependency_activation_id,
    )


def _file_access(identity, activation_id, name, mode, before, after):
    return SimpleNamespace(
        id=identity,
        activation_id=activation_id,
        name=name,
        mode=mode,
        checkpoint=identity / 100,
        content_hash_before=before,
        content_hash_after=after,
    )


def _trial(identity, *, activations, evaluations=(), dependencies=(), file_accesses=()):
    return SimpleNamespace(
        id=identity,
        script="example.py",
        finished=True,
        activations=list(activations),
        evaluations=list(evaluations),
        dependencies=list(dependencies),
        file_accesses=list(file_accesses),
    )


def test_now_activations_are_context_and_evaluations_own_values_and_dependencies():
    outer = _activation(1, "outer", 0.01, 0.10, "6", 1)
    inner = _activation(2, "calculate", 0.02, 0.08, "6", 2)
    evaluations = [
        _evaluation(1, 0, "outer", "outer()", "call", "r", 1),
        _evaluation(2, 1, "calculate", "calculate()", "call", "r", 2),
        _evaluation(3, 2, "3", "value", "name", "r", 3),
        _evaluation(4, 2, "6", "value * 2", "mult", "r", 4),
    ]
    trace = NOWAdapter(".", trial=_trial(
        "nested",
        activations=[outer, inner],
        evaluations=evaluations,
        dependencies=[_dependency(2, 1), _dependency(4, 3)],
    )).to_trace()

    outer_record = trace.index.exec_by_exec_id["TIDE_EXEC:noworkflow:nested:1"]
    inner_record = trace.index.exec_by_exec_id["TIDE_EXEC:noworkflow:nested:2"]
    call_record = trace.index.exec_by_exec_id[
        "TIDE_EXEC:noworkflow:nested:evaluation:2"
    ]
    value_record = trace.index.exec_by_exec_id[
        "TIDE_EXEC:noworkflow:nested:evaluation:3"
    ]
    result_record = trace.index.exec_by_exec_id[
        "TIDE_EXEC:noworkflow:nested:evaluation:4"
    ]

    assert outer_record.writes == [] and outer_record.deps == []
    assert inner_record.writes == [] and inner_record.deps == []
    assert [write.stored_target for write in value_record.writes] == [
        "evaluation:3"
    ]
    assert isinstance(value_record.writes[0].stored_result, TIDEOpaqueValue)
    assert value_record.writes[0].stored_result.representation == "3"
    assert [(read.unscoped_reference, read.producer_exec_id) for read in result_record.deps] == [
        ("evaluation:3", value_record.exec_id)
    ]
    assert [record.exec_id for record in trace.children_of(outer_record.exec_id)] == [
        call_record.exec_id
    ]
    assert [record.exec_id for record in trace.children_of(call_record.exec_id)] == [
        inner_record.exec_id
    ]
    assert [record.exec_id for record in trace.children_of(inner_record.exec_id)] == [
        value_record.exec_id,
        result_record.exec_id,
    ]
    assert trace.events == ()


def test_now_repeated_code_occurrences_retain_their_actual_parent():
    first = _activation(1, "first", 0.01, 0.04, "1", 1, evaluation_id=10)
    second = _activation(2, "second", 0.05, 0.08, "1", 2, evaluation_id=11)
    evaluations = [
        _evaluation(20, 1, "1", "value", "name", "r", 9),
        _evaluation(21, 2, "1", "value", "name", "r", 9),
    ]
    trace = NOWAdapter(".", trial=_trial(
        "repeated", activations=[first, second], evaluations=evaluations
    )).to_trace()
    first_eval = trace.index.exec_by_exec_id[
        "TIDE_EXEC:noworkflow:repeated:evaluation:20"
    ]
    second_eval = trace.index.exec_by_exec_id[
        "TIDE_EXEC:noworkflow:repeated:evaluation:21"
    ]
    assert first_eval.base_node_id == second_eval.base_node_id
    assert first_eval.node_id != second_eval.node_id
    assert trace.parents_of(first_eval.exec_id).executions[0].func_name == "first"
    assert trace.parents_of(second_eval.exec_id).executions[0].func_name == "second"


def test_now_file_access_is_direct_evidence_on_exact_activation():
    trace = NOWAdapter(".", trial=_trial(
        "files",
        activations=[
            _activation(1, "read", 0.01, 0.02, "data", 1),
            _activation(2, "write", 0.03, 0.04, "None", 2),
        ],
        file_accesses=[
            _file_access(1, 1, "input.txt", "rb", "input-hash", "input-hash"),
            _file_access(2, 2, "output.txt", "w", None, "output-hash"),
        ],
    )).to_trace()
    read = trace.index.exec_by_exec_id["TIDE_EXEC:noworkflow:files:1"]
    write = trace.index.exec_by_exec_id["TIDE_EXEC:noworkflow:files:2"]
    assert [(effect.effect, effect.targets) for effect in read.effects] == [
        (Effects.READ, ("file:input.txt", 0))
    ]
    assert [(effect.effect, effect.targets) for effect in write.effects] == [
        (Effects.WRITE, ("file:output.txt", 0))
    ]
    assert [(item.stored_target, item.stored_result) for item in write.writes] == [
        ("file:output.txt", "output-hash")
    ]
    assert trace.events == ()


def test_now_file_versions_follow_access_checkpoint_order():
    first_access = _file_access(1, 1, "shared.txt", "w", None, "first")
    second_access = _file_access(2, 2, "shared.txt", "w", "first", "second")
    first_access.checkpoint = 0.1
    second_access.checkpoint = 0.3
    trace = NOWAdapter(".", trial=_trial(
        "file-order",
        activations=[
            _activation(1, "first", 0.0, 0.4, "None", 1),
            _activation(2, "second", 0.1, 0.2, "None", 2),
        ],
        file_accesses=[second_access, first_access],
    )).to_trace()
    versions = {
        write.stored_result: (write.stored_version, write.updated)
        for record in trace.log
        for write in record.writes
    }
    assert versions == {"first": (0, False), "second": (1, True)}


def test_now_does_not_invent_file_delete_and_rejects_orphan_access():
    activation = _activation(1, "unlink", 0.01, 0.02, "None", 1)
    trace = NOWAdapter(".", trial=_trial(
        "unlink",
        activations=[activation],
        file_accesses=[_file_access(1, 1, "deleted.txt", "r", None, "created")],
    )).to_trace()
    assert Effects.DELETE not in {effect.effect for effect in trace.log[0].effects}
    with pytest.raises(ValueError, match="unknown activation"):
        NOWAdapter(".", trial=_trial(
            "orphan",
            activations=[activation],
            file_accesses=[_file_access(2, 99, "orphan.txt", "rb", "h", "h")],
        )).to_trace()


def test_now_metadata_and_focused_views_remain_truthful(capsys):
    activation = _activation(1, "calculate", 1, 2, "6", 14)
    evaluation = _evaluation(2, 1, "6", "value", "name", "r", 14)
    trace = NOWAdapter(".", trial=_trial(
        "metadata", activations=[activation], evaluations=[evaluation]
    )).to_trace()
    activation_record = trace.index.exec_by_exec_id[
        "TIDE_EXEC:noworkflow:metadata:1"
    ]
    evaluation_record = trace.index.exec_by_exec_id[
        "TIDE_EXEC:noworkflow:metadata:evaluation:2"
    ]
    assert activation_record.status is None
    assert activation_record.duration_ms == 1000
    assert activation_record.now() == NOWMeta(
        trial_id="metadata",
        activation_id=1,
        function="calculate",
        line=14,
        output="6",
    )
    activation_record.now().show()
    assert "NOW ACTIVATION [1]" in capsys.readouterr().out
    activation_focus = trace.inspect(activation_record.exec_id).now().trace()
    evaluation_focus = trace.inspect(evaluation_record.exec_id).now().trace()
    assert trace.scope == "complete"
    assert activation_focus.scope == "ambiguous"
    assert {record.exec_id for record in activation_focus.log} == {
        activation_record.exec_id,
        evaluation_record.exec_id,
    }
    focused_show = activation_focus.show(color=False)
    assert " - 1 execution" in focused_show
    assert " - 0 executions" not in focused_show
    assert len(activation_focus.to_model()["O"]) == 1
    assert len(evaluation_focus.to_model()["O"]) == 1


@pytest.mark.parametrize("output_type", ["<class 'int'>", None])
def test_now_evaluation_type_and_relationships_survive_bundle(output_type):
    activation = _activation(1, "calculate", 0.01, 0.03, "captured", 2)
    evaluation = _evaluation(2, 1, "captured", "value", "name", "r", 2)
    if output_type is not None:
        evaluation.memberships_as_collection.append(SimpleNamespace(
            key=".__class__", member=SimpleNamespace(repr=output_type)
        ))
    trace = NOWAdapter(".", trial=_trial(
        "bundle", activations=[activation], evaluations=[evaluation]
    )).to_trace()
    rebuilt = TIDE.from_bundle(trace.to_bundle())
    for candidate in (trace, rebuilt):
        context = candidate.index.exec_by_exec_id["TIDE_EXEC:noworkflow:bundle:1"]
        value = candidate.index.exec_by_exec_id[
            "TIDE_EXEC:noworkflow:bundle:evaluation:2"
        ]
        assert "output_type" not in value.node_options
        assert isinstance(value.output_value, TIDEOpaqueValue)
        assert value.output_value.type_name == (
            output_type or "TIDEUnknownType"
        )
        assert value.output_value.representation == "captured"
        descriptor = value.writes[0].snapshot_lineage[0]
        assert descriptor["type"] == (output_type or "TIDEUnknownType")
        assert descriptor["repr"] == "captured"
        assert value.output_value.value_hash == descriptor["value_hash"]
        assert value.writes[0].stored_result.value_hash == descriptor["value_hash"]
        assert context.writes == []
        assert candidate.children_of(context.exec_id).executions[0].exec_id == value.exec_id
        assert candidate.children_of(context.exec_id).to_dict()[
            "executions"
        ][0]["writes"][0]["value_type"] == (
            output_type or "opaque<TIDEUnknownType>"
        )


def test_now_retains_root_evaluations_without_inventing_a_parent():
    root = _evaluation(2, 0, "open", "open", "global", "r", 1)
    local = _evaluation(3, 1, "open", "open", "name", "r", 2)
    trace = NOWAdapter(".", trial=_trial(
        "root",
        activations=[_activation(1, "__main__", 0.01, 0.03, "None", 1)],
        evaluations=[root, local],
        dependencies=[_dependency(3, 2)],
    )).to_trace()
    root_record = trace.index.exec_by_exec_id[
        "TIDE_EXEC:noworkflow:root:evaluation:2"
    ]
    local_record = trace.index.exec_by_exec_id[
        "TIDE_EXEC:noworkflow:root:evaluation:3"
    ]
    assert not trace.parents_of(root_record.exec_id)
    assert local_record.deps[0].producer_exec_id == root_record.exec_id
def test_tide_now_delegates_to_adapter(monkeypatch):
    sentinel = object()
    monkeypatch.setattr(NOWAdapter, "capture", lambda *args, **kwargs: sentinel)
    assert TIDE.now("example.py") is sentinel


def test_tide_from_now_delegates_to_existing_project_adapter(monkeypatch):
    sentinel = object()
    monkeypatch.setattr(NOWAdapter, "to_trace", lambda self: sentinel)
    assert TIDE.from_now(".", trial=object()) is sentinel


def test_tide_now_capture_uses_main_collection(monkeypatch, tmp_path):
    script = tmp_path / "workflow.py"
    script.write_text("result = 42\n", encoding="utf-8")
    sentinel = object()
    calls = []

    monkeypatch.setattr(NOWAdapter, "_require_noworkflow", lambda: None)
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda command, **kwargs: calls.append((command, kwargs)),
    )
    monkeypatch.setattr(NOWAdapter, "to_trace", lambda self: sentinel)

    assert NOWAdapter.capture(script) is sentinel
    assert calls[0][0][4:6] == ["-c", "main"]


def test_tide_now_capture_still_handles_zero_argument_pipeline(tmp_path):
    if find_spec("noworkflow") is None:
        pytest.skip("noWorkflow is not installed")
    script = tmp_path / "pipeline.py"
    script.write_text(
        """
from tide_tooling import TIDE, node, pipeline
@node(targets=["value"])
def start():
    return "start"
@pipeline
def flow():
    start()
TIDE().ocean.run_pipeline(flow)
""".lstrip(),
        encoding="utf-8",
    )
    project_root = Path(__file__).resolve().parents[1]
    environment = os.environ.copy()
    environment["PYTHONPATH"] = os.pathsep.join(
        part for part in (str(project_root), environment.get("PYTHONPATH")) if part
    )
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "from tide_tooling import TIDE; "
                f"trace = TIDE.now({str(script)!r}, "
                f"project_dir={str(tmp_path / 'now')!r}); "
                "assert any(record.node_options.get('record_kind') == "
                "'evaluation' for record in trace.log)"
            ),
        ],
        cwd=project_root,
        env=environment,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
