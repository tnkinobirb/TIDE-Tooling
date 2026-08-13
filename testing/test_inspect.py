import pytest
from collections import defaultdict
from typing import Any, Callable
import re
from tide_tooling import TIDE
from tide_tooling.core import (
    Actor,
    DataRef,
    DependencyUseRecord,
    ExecRecord,
    Inputs,
    InputUseRecord,
    Inspected,
    NodeInput,
    Object,
    RunnerNodeInput,
    WriteRecord,
    __TIDE_TEMPLATE_VALUE__,
    tide_value_hasher,
)
from tide_tooling.tooling_core.engine.tide_logging import InspectedGroup


__TEMPLATE_VALUE__ = __TIDE_TEMPLATE_VALUE__()



ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def strip_ansi(text):
    return ANSI_RE.sub("", text)


class DedupedList(list):
    def append(self, item):
        if item not in self:
            super().append(item)

    def extend(self, items):
        for item in items:
            self.append(item)


def create_input(
    runner_input=False,
    name="args",
    raw=None,
    evaluated=None,
    normalised=None,
    resolved=None,
):
    if not runner_input:
        return NodeInput(raw=raw)

    return RunnerNodeInput(
        raw=raw,
        evaluated=evaluated,
        normalised=normalised,
        resolved=resolved,
    )


def create_input_use_record(name: str, input: RunnerNodeInput, deps: list):
    return InputUseRecord(
        name,
        input.raw,
        input.evaluated,
        input.normalised,
        input.resolved,
        deps,
    )


def create_inputs_object(
    args=None,
    kwargs=None,
    p_args=None,
    p_kwargs=None,
    k_args=None,
    k_kwargs=None,
):
    inputs = {
        name: value
        for name, value in locals().items()
        if value is not None
    }

    use_records = {
        name: create_input_use_record(_input[0], _input[1], _input[2])
        for name, _input in inputs.items()
    }

    return Inputs(use_records)


def create_read_record(
    dep: DataRef,
    associated_input="args",
    type: str = "static",
    unscoped_ref: DataRef = None,
    resolved_val: Any = None,
    resolved_ver: int = None,
    producer_source: int | str = None,
    producer_actor: int | str = None,
    producer_exec_id: int = None,
    expanded_to: list = None,
    input_index: int = None,
):
    return DependencyUseRecord(
        associated_input,
        dep,
        type,
        unscoped_ref,
        resolved_val,
        resolved_ver,
        producer_source,
        producer_actor,
        producer_exec_id,
        expanded_to,
        input_index,
    )


def create_exec_record(
    seq_id,
    ocean_id="TIDE: Ocean",
    runner_id="TIDE: EventCapture",
    pipeline_id="TIDE: .event()",
    actor_name="TEST_ACTOR",
    func="unknown",
    func_name="unknown",
    inputs=None,
    deps=None,
    effects=None,
    declared_deps=None,
    declared_effects=None,
    declared_targets=None,
    runtime_effects=None,
    runtime_targets=None,
    unscoped_store_refs=None,
    status="succeeded",
    started_at="unknown",
    finished_at="unknown",
    duration_ms="unknown",
    error=None,
    node_id=None,
    node_options="unknown",
    output_value=__TEMPLATE_VALUE__,
    resolve_output="unknown",
    writes=None,
    emitted_by=None,
    emitted_nodes=None,
    base_node_id="unknown",
    base_node_content_id="unknown",
    content_id="unknown",
    exec_id=__TEMPLATE_VALUE__,
):
    if func_name is None:
        func_name = actor_name

    unknown_repr = "unknown"

    return ExecRecord(
        seq_id=seq_id,
        exec_id=exec_id,
        ocean_id=ocean_id,
        runner_id=runner_id,
        pipeline_id=pipeline_id,
        node_id=node_id if node_id is not None else actor_name,
        func=func,
        func_name=func_name,
        inputs=inputs or {},
        deps=deps or [],
        effects=effects or [],
        declared_deps=declared_deps or [],
        declared_effects=declared_effects or [],
        declared_targets=declared_targets or [],
        runtime_effects=runtime_effects or [],
        runtime_targets=runtime_targets or [],
        unscoped_store_refs=unscoped_store_refs or [],
        status=status,
        started_at=started_at,
        finished_at=finished_at,
        duration_ms=duration_ms,
        error=error or None,
        node_options=node_options or {},
        output_value=(
            output_value
            if not isinstance(output_value, __TIDE_TEMPLATE_VALUE__)
            else __TEMPLATE_VALUE__
        ),
        resolve_output=resolve_output or unknown_repr,
        writes=writes or [],
        emitted_by=emitted_by or [],
        emitted_nodes=emitted_nodes or [],
        base_node_id=base_node_id or unknown_repr,
        base_node_content_id=base_node_content_id or unknown_repr,
        content_id=content_id,
    )


def create_write_record(
    cause: str = "unknown",
    stored_result=__TEMPLATE_VALUE__,
    stored_version=0,
    stored_target=__TEMPLATE_VALUE__,
):
    return WriteRecord(
        cause,
        stored_result,
        stored_version,
        stored_target,
    )


def create_expected_dict(
    obj: Any,
    history: list[int | tuple[str, int]],
    upstream: list[int],
    downstream: list[int],
    oceans: list[int | str] = None,
    runners: list[int | str] = None,
    pipelines: list[int | str] = None,
    version=None,
):
    return locals()


def assert_input(
    to_assert: tuple[str, NodeInput | RunnerNodeInput],
    input_tuple: tuple[str, NodeInput | RunnerNodeInput],
):
    assert to_assert[0] == input_tuple[0]

    to_assert_input = to_assert[1]
    test_input = input_tuple[1]

    if isinstance(to_assert_input, NodeInput):
        assert isinstance(test_input, NodeInput)
        assert to_assert_input.raw == test_input.raw
        return

    if isinstance(to_assert_input, RunnerNodeInput):
        assert isinstance(test_input, RunnerNodeInput)
        assert to_assert_input.raw == test_input.raw
        assert to_assert_input.evaluated == test_input.evaluated
        assert to_assert_input.normalised == test_input.normalised
        assert to_assert_input.resolved == test_input.resolved
        return

    raise TypeError("Input is not a RunnerNodeInput or NodeInput!")


def assert_read_record(
    to_assert: DependencyUseRecord,
    expected_record: DependencyUseRecord,
    extensive=False,
):
    assert to_assert.dep == expected_record.dep
    assert_input(
        to_assert.associated_input,
        expected_record.associated_input,
    )
    assert to_assert.producer_actor == expected_record.producer_actor
    assert to_assert.producer_source == expected_record.producer_source
    assert to_assert.resolved_value == expected_record.resolved_value
    assert to_assert.resolved_version == expected_record.resolved_version
    assert to_assert.type == expected_record.type
    assert (
        to_assert.unscoped_reference
        == expected_record.unscoped_reference
    )
    assert to_assert.input_index == expected_record.input_index

    if extensive:
        assert (
            to_assert.producer_exec_id
            == expected_record.producer_exec_id
        )


def assert_write_record(
    to_assert: WriteRecord,
    expected_record: WriteRecord,
):
    assert to_assert.cause == expected_record.cause
    assert to_assert.stored_target == expected_record.stored_target
    assert to_assert.stored_version == expected_record.stored_version
    assert to_assert.stored_result == expected_record.stored_result


def assert_exec_record(
    exec_from_inspect: ExecRecord,
    expected_exec: ExecRecord,
    ocean=False,
    extensive=False,
):
    assert exec_from_inspect.seq_id == expected_exec.seq_id
    assert exec_from_inspect.runner_id == expected_exec.runner_id
    assert exec_from_inspect.pipeline_id == expected_exec.pipeline_id
    assert exec_from_inspect.node_id == expected_exec.node_id
    assert exec_from_inspect.func == expected_exec.func
    assert exec_from_inspect.func_name == expected_exec.func_name
    assert exec_from_inspect.inputs == expected_exec.inputs

    assert len(exec_from_inspect.deps) == len(expected_exec.deps)
    for idx, read in enumerate(exec_from_inspect.deps):
        assert_read_record(read, expected_exec.deps[idx])

    assert len(exec_from_inspect.writes) == len(expected_exec.writes)
    for idx, write in enumerate(exec_from_inspect.writes):
        assert_write_record(write, expected_exec.writes[idx])

    assert exec_from_inspect.effects == expected_exec.effects
    assert (
        exec_from_inspect.declared_deps
        == expected_exec.declared_deps
    )
    assert (
        exec_from_inspect.declared_effects
        == expected_exec.declared_effects
    )
    assert (
        exec_from_inspect.declared_targets
        == expected_exec.declared_targets
    )
    assert (
        exec_from_inspect.unscoped_store_refs
        == expected_exec.unscoped_store_refs
    )
    assert exec_from_inspect.output_value == expected_exec.output_value
    assert exec_from_inspect.status == expected_exec.status
    assert exec_from_inspect.error == expected_exec.error

    if ocean:
        assert (
            exec_from_inspect.base_node_content_id
            == expected_exec.base_node_content_id
        )
        assert exec_from_inspect.content_id == expected_exec.content_id
        assert (
            exec_from_inspect.resolve_output
            == expected_exec.resolve_output
        )
        assert exec_from_inspect.emitted_by == expected_exec.emitted_by
        assert (
            exec_from_inspect.emitted_nodes
            == expected_exec.emitted_nodes
        )
        assert (
            exec_from_inspect.node_options
            == expected_exec.node_options
        )

    if extensive:
        assert exec_from_inspect.exec_id == expected_exec.exec_id
        assert exec_from_inspect.started_at == expected_exec.started_at
        assert exec_from_inspect.finished_at == expected_exec.finished_at
        assert exec_from_inspect.duration_ms == expected_exec.duration_ms


def assert_inspected_exec(
    inspect,
    inspected_exec: Inspected,
    exec: ExecRecord,
    expected_seq_id: int,
    expected_repr: str,
    associated_actor: Actor,
    upstream_seq_ids: list,
    downstream_seq_ids: list,
    source_oceans: list,
    source_runners: list,
    source_pipelines: list = None,
    ocean_source=False,
):
    assert inspected_exec.type == "exec_record"
    assert (
        repr(inspected_exec.object)
        == f"EXEC_RECORD[{expected_seq_id}] "
        f"{associated_actor.source_actor}"
    )

    assert_inspected_fields(
        inspect,
        inspected_exec,
        exec,
        "exec_record",
        expected_seq_id,
        expected_repr=expected_repr,
        history=[],
        upstream=upstream_seq_ids,
        downstream=downstream_seq_ids,
        oceans=source_oceans,
        runners=source_runners,
        pipelines=source_pipelines,
    )

    exec_obj: Object = inspected_exec.object

    if source_pipelines is None:
        source_pipelines = ["TIDE: .trace()"]

    assert exec_obj.type == "exec_record"
    assert exec_obj.source_ocean == source_oceans
    assert exec_obj.source_runner == source_runners
    assert exec_obj.source_pipeline == source_pipelines
    assert exec_obj.version is None

    assert_exec_record(
        exec_obj.object,
        exec,
        ocean=ocean_source,
    )

    assert len(exec_obj.reads) == len(exec.deps)
    for idx, read in enumerate(exec_obj.reads):
        assert_read_record(read, exec.deps[idx])

    assert len(exec_obj.writes) == len(exec.writes)
    for idx, write in enumerate(exec_obj.writes):
        assert_write_record(write, exec.writes[idx])


def assert_inspected_fields(
    inspect: Callable,
    inspected: Inspected,
    source_obj: Any,
    type: str,
    id: Any,
    expected_repr: str,
    history: list,
    upstream: list,
    downstream: list,
    oceans: list,
    runners: list,
    pipelines: list[tuple],
    version=None,
    grouped=False,
):
    inspected_repr = repr(inspected.object)

    assert inspected.type == type

    if type == "exec_record":
        assert inspected.object.object.seq_id == id
    else:
        assert inspected.id == id
        assert inspected.object.object == source_obj

    assert inspected_repr == expected_repr

    calculated_history = DedupedList()
    calculated_upstream = defaultdict(list)
    calculated_downstream = defaultdict(list)

    if type in ["value", "reference"]:
        calculated_history = history
    else:
        for seq_id in history:
            inspected_exec: Inspected = inspect(seq_id, "seq_id")
            calculated_history.append(inspected_exec.id)

    for seq_id in upstream:
        inspected_exec: Inspected = inspect(seq_id, "seq_id")
        calculated_upstream[
            inspected_exec.object.object.producer_id
        ].append(inspected_exec.id)

    for seq_id in downstream:
        inspected_exec: Inspected = inspect(seq_id, "seq_id")
        calculated_downstream[
            inspected_exec.object.object.producer_id
        ].append(inspected_exec.id)

    assert inspected.history == calculated_history
    assert inspected.upstream == calculated_upstream
    assert inspected.version == version
    assert inspected.downstream == calculated_downstream
    assert inspected.source_oceans == oceans
    assert inspected.source_runners == runners
    assert inspected.source_pipelines == pipelines
    assert inspected.grouped == grouped

    calculated_writes = DedupedList()
    calculated_reads = DedupedList()

    if inspected.type in ["value", "reference"]:
        if inspected.type == "reference":
            history = [
                (inspected.id, ver)
                for ver in history
            ]

        for seq_id in downstream:
            inspected_exec = inspect(seq_id, "seq_id")

            for read in inspected_exec.reads:
                if (
                    read.unscoped_reference,
                    read.resolved_version,
                ) in history:
                    calculated_reads.append(read)

        for seq_id in upstream:
            inspected_exec = inspect(seq_id, "seq_id")

            for write in inspected_exec.writes:
                if (
                    write.stored_target,
                    write.stored_version,
                ) in history:
                    calculated_writes.append(write)

    elif type == "exec_record":
        calculated_reads.extend(source_obj.deps)
        calculated_writes.extend(source_obj.writes)

    else:
        for seq_id in history:
            inspected_exec = inspect(seq_id, "seq_id")
            calculated_reads.extend(inspected_exec.object.reads)
            calculated_writes.extend(inspected_exec.object.writes)

    assert len(inspected.reads) == len(calculated_reads)

    for idx, read in enumerate(inspected.reads):
        assert_read_record(read, calculated_reads[idx])

    assert len(inspected.writes) == len(calculated_writes)

    for idx, write in enumerate(inspected.writes):
        assert_write_record(write, calculated_writes[idx])


def assert_inspected_ref(
    inspect: Callable,
    inspected_ref: Inspected,
    ref: str,
    version_history: list,
    upstream_seq_ids: list,
    downstream_seq_ids: list,
    source_oceans,
    source_runners,
    source_pipelines: list = None,
    version=None,
):
    assert_inspected_fields(
        inspect=inspect,
        inspected=inspected_ref,
        source_obj=ref,
        type="reference",
        id=ref,
        expected_repr=(
            f"REFERENCE[{ref}[v{version}]]"
            if version is not None
            else f"REFERENCE[{ref}]"
        ),
        history=version_history,
        upstream=upstream_seq_ids,
        downstream=downstream_seq_ids,
        oceans=source_oceans,
        runners=source_runners,
        pipelines=(
            source_pipelines
            if source_pipelines is not None
            else ["TIDE: .trace()"]
        ),
        version=version,
        grouped=False,
    )


def assert_inspected_actor(
    inspect: Callable,
    inspected_actor: Inspected,
    actor: Actor,
    history_seq_ids: list,
    upstream_seq_ids: list,
    downstream_seq_ids: list,
    source_pipelines: list = None,
):
    assert_inspected_fields(
        inspect=inspect,
        inspected=inspected_actor,
        source_obj=actor,
        type="actor",
        id=actor,
        expected_repr=f"ACTOR[{actor.source_actor}]",
        history=history_seq_ids,
        upstream=upstream_seq_ids,
        downstream=downstream_seq_ids,
        oceans=[actor.source_ocean],
        runners=[actor.source_runner],
        pipelines=(
            source_pipelines
            if source_pipelines is not None
            else ["TIDE: .trace()"]
        ),
        version=None,
        grouped=False,
    )


def assert_inspected_group(
    inspect: Callable,
    type: str,
    inspected_group: Inspected,
    group_history: list[int],
    expected_obj_asserts: list[dict],
):
    assert type == inspected_group.type
    assert inspected_group.grouped
    assert isinstance(inspected_group.object, list)
    assert type in ["actor", "reference"]

    if type == "actor":
        group_history = [
            inspect(seq_id, "seq_id").id
            for seq_id in group_history
        ]

    assert inspected_group.history == group_history

    for idx, obj in enumerate(inspected_group.object):
        expected = expected_obj_asserts[idx]
        expected_source_obj = expected["obj"]
        history = expected["history"]
        upstream_seqs = expected["upstream"]
        downstream_seqs = expected["downstream"]
        source_oceans = expected["oceans"]
        source_runners = expected["runners"]
        source_pipelines = expected["pipelines"]
        version = expected["version"]

        if type == "actor":
            assert_inspected_actor(
                inspect,
                inspected_actor=inspect(obj, type),
                actor=expected_source_obj,
                history_seq_ids=history,
                upstream_seq_ids=upstream_seqs,
                downstream_seq_ids=downstream_seqs,
                source_pipelines=source_pipelines,
            )

        elif type == "reference":
            inspected_ref = inspect(
                obj,
                type,
                version,
            )

            assert_inspected_ref(
                inspect,
                inspected_ref=inspected_ref,
                ref=expected_source_obj,
                version_history=history,
                upstream_seq_ids=upstream_seqs,
                downstream_seq_ids=downstream_seqs,
                source_oceans=source_oceans,
                source_runners=source_runners,
                source_pipelines=source_pipelines,
                version=version,
            )


def assert_inspected_value(
    inspect: Callable,
    inspected_val: Inspected,
    expected_val_or_hash: str | Any,
    version_history: list,
    upstream_seq_ids: list,
    downstream_seq_ids: list,
    source_oceans,
    source_runners,
    source_pipelines: list = None,
):
    assert_inspected_fields(
        inspect=inspect,
        inspected=inspected_val,
        source_obj=expected_val_or_hash,
        type="value",
        id=tide_value_hasher(expected_val_or_hash),
        expected_repr=(
            f"VALUE[{tide_value_hasher(expected_val_or_hash)}]"
        ),
        history=version_history,
        upstream=upstream_seq_ids,
        downstream=downstream_seq_ids,
        oceans=source_oceans,
        runners=source_runners,
        pipelines=(
            source_pipelines
            if source_pipelines is not None
            else ["TIDE: .trace()"]
        ),
        grouped=False,
    )


def B(a, b, seperate_b):
    return "!B"


def X(a, ab, abc):
    return None


@pytest.fixture(scope="module")
def inspect_context():
    tide = TIDE()
    tide.ocean.register_store("testing")

    tide.event("A", writes={"@A": "!A"})
    tide.event(
        "B",
        reads=["@A"],
        writes={
            "@B": "!B",
            "testing:@B": "!B",
        },
    )
    tide.exec(
        B,
        args=[
            "@@A",
            "@@B",
            "@testing:@B",
        ],
        writes=["@B"],
    )
    tide.event(
        B,
        reads={
            "@A": "!A",
            "@B": "!B",
        },
        writes={"@B": "!B"},
    )
    tide.event(
        "C",
        reads={"@B": "!B"},
        writes={"@C": "!C"},
    )
    tide.event(
        "X",
        writes={"@C": "!C"},
    )
    tide.exec(
        X,
        args=[
            "@@C",
            "@testing:@B",
            "@testing:@B",
        ],
        writes=["@B"],
    )

    trace = tide.trace()
    ocean_id = trace.ocean_id
    inspect = trace.inspect

    return {
        "trace": trace,
        "inspect": inspect,
        "ocean_id": ocean_id,
        "b_event_actor": Actor(
            ocean_id,
            "TIDE: .event()",
            "B",
        ),
        "b_exec_actor": Actor(
            ocean_id,
            "TIDE: .exec(0)",
            "B",
        ),
    }


def test_inspected_to_dict_exposes_compact_contract(inspect_context):
    inspected = inspect_context["trace"].inspect(3, "seq_id")
    data = inspected.to_dict()

    assert set(data) == {
        "type",
        "id",
        "ocean",
        "source",
        "pipeline",
        "history",
        "upstream_seeds",
        "downstream_seeds",
        "grouped",
        "exists",
    }
    assert data["type"] == "exec_record"
    assert data["id"] == inspected.id
    assert data["grouped"] is False
    assert data["exists"] is True
    assert "reads" not in data
    assert "writes" not in data


def test_inspected_show_projects_compact_contract(inspect_context):
    inspected = inspect_context["trace"].inspect(3, "seq_id")
    output = strip_ansi(inspected.show())

    assert "INSPECT EXEC_RECORD[3] B" in output
    assert "type: exec_record" in output
    assert f"id: {inspected.id}" in output
    assert "grouped: false" in output
    assert "exists: true" in output
    assert "history: none" in output
    assert "upstream_seeds:" in output
    assert "downstream_seeds:" in output
    assert "\nreads:" not in output
    assert "\nwrites:" not in output


def test_inspected_group_show_preserves_group_identity(inspect_context):
    inspected = inspect_context["trace"].inspect("B", "actor")
    output = strip_ansi(inspected.show())

    assert "INSPECT ACTOR GROUPED[B]" in output
    assert "id: B" in output
    assert "grouped: true" in output


def test_inspected_group_repr_and_defaults_are_safe():
    inspected = InspectedGroup([], "actor", "A")

    assert inspected.history == []
    assert inspected.upstream == {}
    assert inspected.downstream == {}
    assert inspected.to_dict()["upstream_seeds"] == []
    assert inspected.to_dict()["downstream_seeds"] == []
    assert "InspectedGroup(actor: A" in repr(inspected)


def test_inspected_show_uses_dim_section_labels(inspect_context):
    inspected = inspect_context["trace"].inspect(3, "seq_id")
    output = inspected.show()

    assert "\033[2mOBJECT:\033[0m" in output
    assert "\033[2m\033[1mOBJECT:" not in output


def test_inspected_group_show_lists_concrete_members(inspect_context):
    inspected = inspect_context["trace"].inspect("B", "actor")
    output = strip_ansi(inspected.show())

    assert "members:" in output
    assert "[0] ACTOR[" in output
    assert "[1] ACTOR[" in output
    assert f"ACTOR[{inspect_context['b_event_actor'].id}]" in output
    assert f"ACTOR[{inspect_context['b_exec_actor'].id}]" in output


def test_inspected_group_show_expands_multi_source_fields(inspect_context):
    inspected = inspect_context["trace"].inspect("B", "actor")
    output = strip_ansi(inspected.show())

    assert "ocean:" in output
    assert "source:" in output
    assert "pipeline:" in output
    assert "  [0]" in output


def test_inspected_group_show_does_not_duplicate_history_label(inspect_context):
    inspected = inspect_context["trace"].inspect("B", "actor")
    output = strip_ansi(inspected.show())

    assert "HISTORY:\n  [0]" in output
    assert "HISTORY:\nhistory:" not in output


def test_inspected_group_show_uses_inspect_colour_roles(inspect_context):
    inspected = inspect_context["trace"].inspect("B", "actor")
    output = inspected.show()

    assert "\033[2mtype:\033[0m \033[37mactor\033[0m" in output
    assert "\033[34m[0]\033[0m \033[36mACTOR[" in output
    assert "\033[2msource:\033[0m" in output
    assert "\033[36m" in output


def test_inspected_reference_show_marks_used_reference_green(inspect_context):
    inspected = inspect_context["trace"].inspect(
        "global:@A",
        "reference",
        version=0,
    )
    output = inspected.show()

    assert "\033[2mid:\033[0m \033[32mglobal:@A\033[0m" in output


def test_inspected_show_uses_cyan_for_identity_values(inspect_context):
    inspected = inspect_context["trace"].inspect("B", "actor")
    output = inspected.show()

    assert "\033[36mGROUPED[B]\033[0m" in output
    assert "\033[36mTIDE_EXEC:" in output


def test_inspect_b_event_exec_record(inspect_context):
    trace = inspect_context["trace"]
    inspect = inspect_context["inspect"]
    ocean_id = inspect_context["ocean_id"]
    b_event_actor = inspect_context["b_event_actor"]
    b_event_exec_inspected = inspect(3, "seq_id")

    b_event_deps = [
        DataRef("@A", "global"),
        DataRef("@B", "global"),
    ]
    b_event_targets = [
        DataRef("@B", "global"),
    ]

    args_input = create_input(
        name="args",
        runner_input=True,
        raw=("@A", "@B"),
        evaluated=("@A", "@B"),
        normalised=tuple(b_event_deps),
        resolved=("!A", "!B"),
    )

    expected_b_event_read_records = [
        create_read_record(
            associated_input=("args", args_input),
            dep=DataRef("@A", "global"),
            unscoped_ref="global:@A",
            resolved_val="!A",
            resolved_ver=0,
            producer_source="TIDE: .event()",
            producer_actor=Actor(
                ocean_id,
                "TIDE: .event()",
                "A",
            ),
            input_index=0,
        ),
        create_read_record(
            associated_input=("args", args_input),
            dep=DataRef("@B", "global"),
            unscoped_ref="global:@B",
            resolved_val="!B",
            resolved_ver=1,
            producer_source="TIDE: .exec(0)",
            producer_actor=Actor(
                ocean_id,
                "TIDE: .exec(0)",
                "B",
            ),
            input_index=1,
        ),
    ]

    expected_b_event_write_records = [
        create_write_record(
            stored_result="!B",
            stored_target="global:@B",
            stored_version=2,
            cause="unknown",
        ),
    ]

    b_event_inputs = create_inputs_object(
        args=(
            "args",
            args_input,
            b_event_deps,
        )
    )

    expected_b_event = create_exec_record(
        seq_id=3,
        ocean_id=ocean_id,
        pipeline_id="TIDE: .event(B[2])",
        runner_id="TIDE: .event()",
        node_id="unknown",
        actor_name="B",
        func=B,
        func_name="B",
        inputs=b_event_inputs,
        deps=expected_b_event_read_records,
        effects=None,
        declared_deps=b_event_deps,
        declared_effects=[],
        declared_targets=b_event_targets,
        unscoped_store_refs=["global"],
        error=None,
        output_value=None,
        writes=expected_b_event_write_records,
    )

    assert_inspected_exec(
        inspect,
        inspected_exec=b_event_exec_inspected,
        exec=expected_b_event,
        associated_actor=b_event_actor,
        expected_seq_id=3,
        upstream_seq_ids=[0, 2],
        downstream_seq_ids=[4],
        source_oceans=[ocean_id],
        source_runners=["TIDE: .event()"],
        source_pipelines=[
            (
                "TIDE: .event(B[2])",
                "TIDE: .event(B[2])",
            )
        ],
        expected_repr="EXEC_RECORD[3] B",
    )


def test_inspect_b_runtime_exec_record(inspect_context):
    inspect = inspect_context["inspect"]
    ocean_id = inspect_context["ocean_id"]
    b_exec_actor = inspect_context["b_exec_actor"]
    b_exec_inspected = inspect(2, "seq_id")

    b_exec_deps = [
        DataRef("@A", "global"),
        DataRef("@B", "global"),
        DataRef("@B", "testing"),
    ]
    b_exec_targets = [
        DataRef("@B", "global"),
    ]

    args_input = create_input(
        name="args",
        runner_input=True,
        raw=(
            "@@A",
            "@@B",
            "@testing:@B",
        ),
        evaluated=(
            "@@A",
            "@@B",
            "@testing:@B",
        ),
        normalised=(
            DataRef("@A", "global"),
            DataRef("@B", "global"),
            DataRef("@B", "testing"),
        ),
        resolved=(
            "!A",
            "!B",
            "!B",
        ),
    )

    expected_b_exec_read_records = [
        create_read_record(
            associated_input=("args", args_input),
            dep=DataRef("@A", "global"),
            unscoped_ref="global:@A",
            resolved_val="!A",
            resolved_ver=0,
            producer_source="TIDE: .event()",
            producer_actor=Actor(
                ocean_id,
                "TIDE: .event()",
                "A",
            ),
            input_index=0,
        ),
        create_read_record(
            associated_input=("args", args_input),
            dep=DataRef("@B", "global"),
            unscoped_ref="global:@B",
            resolved_val="!B",
            resolved_ver=0,
            producer_source="TIDE: .event()",
            producer_actor=Actor(
                ocean_id,
                "TIDE: .event()",
                "B",
            ),
            input_index=1,
        ),
        create_read_record(
            associated_input=("args", args_input),
            dep=DataRef("@B", "testing"),
            unscoped_ref="testing:@B",
            resolved_val="!B",
            resolved_ver=0,
            producer_source="TIDE: .event()",
            producer_actor=Actor(
                ocean_id,
                "TIDE: .event()",
                "B",
            ),
            input_index=2,
        ),
    ]

    expected_b_exec_write_records = [
        create_write_record(
            stored_result="!B",
            stored_target="global:@B",
            stored_version=1,
            cause="TIDE: node write",
        ),
    ]

    exec_inputs = create_inputs_object(
        args=(
            "args",
            args_input,
            b_exec_deps,
        )
    )

    expected_b_exec = create_exec_record(
        seq_id=2,
        ocean_id=ocean_id,
        pipeline_id=0,
        runner_id="TIDE: .exec(0)",
        node_id=0,
        actor_name="B",
        func=B,
        func_name="B",
        inputs=exec_inputs,
        deps=expected_b_exec_read_records,
        effects=None,
        declared_deps=b_exec_deps,
        declared_effects=[],
        declared_targets=b_exec_targets,
        unscoped_store_refs=[
            "global",
            "testing",
        ],
        error=None,
        output_value="!B",
        writes=expected_b_exec_write_records,
    )

    assert_inspected_exec(
        inspect,
        inspected_exec=b_exec_inspected,
        exec=expected_b_exec,
        associated_actor=b_exec_actor,
        expected_seq_id=2,
        upstream_seq_ids=[0, 1],
        downstream_seq_ids=[3],
        source_oceans=[ocean_id],
        source_runners=["TIDE: .exec(0)"],
        source_pipelines=[
            (
                0,
                "TIDE: .exec(B[0])",
            )
        ],
        expected_repr="EXEC_RECORD[2] B",
    )


def test_inspect_b_event_actor(inspect_context):
    trace = inspect_context["trace"]
    b_event_actor = inspect_context["b_event_actor"]

    assert_inspected_actor(
        trace.inspect,
        inspected_actor=trace.inspect(
            b_event_actor,
            "actor",
        ),
        actor=b_event_actor,
        history_seq_ids=[1, 3],
        upstream_seq_ids=[0, 2],
        downstream_seq_ids=[2, 4, 6],
        source_pipelines=[
            (
                f"TIDE: .event(B[{n}])",
                f"TIDE: .event(B[{n}])",
            )
            for n in [1, 2]
        ],
    )


def test_inspect_global_b_reference_version_one(inspect_context):
    trace = inspect_context["trace"]
    ocean_id = inspect_context["ocean_id"]

    assert_inspected_ref(
        trace.inspect,
        inspected_ref=trace.inspect(
            "global:@B",
            "reference",
            version=1,
        ),
        ref="global:@B",
        version_history=[0, 1],
        upstream_seq_ids=[2],
        downstream_seq_ids=[3],
        source_oceans=[ocean_id],
        source_runners=["TIDE: .exec(0)"],
        source_pipelines=[
            (
                0,
                "TIDE: .exec(B[0])",
            )
        ],
        version=1,
    )


@pytest.mark.parametrize(
    "value_or_hash",
    [
        "!B",
        pytest.param(
            tide_value_hasher("!B"),
            id="hash",
        ),
    ],
)
def test_inspect_b_value(inspect_context, value_or_hash):
    trace = inspect_context["trace"]
    ocean_id = inspect_context["ocean_id"]
    inspected_value = trace.inspect(
        "!B",
        "value",
    )

    assert (
        trace.index.value_by_hash[
            tide_value_hasher(value_or_hash)
        ]
        == "!B"
    )

    assert_inspected_value(
        inspect=trace.inspect,
        inspected_val=inspected_value,
        expected_val_or_hash="!B",
        version_history=[
            ("global:@B", 0),
            ("testing:@B", 0),
            ("global:@B", 1),
            ("global:@B", 2),
        ],
        upstream_seq_ids=[1, 2, 3],
        downstream_seq_ids=[2, 6, 3, 4],
        source_oceans=[ocean_id],
        source_runners=[
            "TIDE: .event()",
            "TIDE: .exec(0)",
        ],
        source_pipelines=[
            (
                "TIDE: .event(B[1])",
                "TIDE: .event(B[1])",
            ),
            (
                0,
                "TIDE: .exec(B[0])",
            ),
            (
                "TIDE: .event(B[2])",
                "TIDE: .event(B[2])",
            ),
        ],
    )


def test_inspect_grouped_b_actors(inspect_context):
    trace = inspect_context["trace"]
    b_event_actor = inspect_context["b_event_actor"]
    b_exec_actor = inspect_context["b_exec_actor"]

    expected_actor_group = [
        create_expected_dict(
            obj=b_event_actor,
            history=[1, 3],
            upstream=[0, 2],
            downstream=[2, 4, 6],
            pipelines=[
                (
                    f"TIDE: .event(B[{n}])",
                    f"TIDE: .event(B[{n}])",
                )
                for n in [1, 2]
            ],
        ),
        create_expected_dict(
            obj=b_exec_actor,
            history=[2],
            upstream=[0, 1],
            downstream=[3],
            pipelines=[
                (
                    0,
                    "TIDE: .exec(B[0])",
                )
            ],
        ),
    ]

    assert_inspected_group(
        inspect=trace.inspect,
        type="actor",
        inspected_group=trace.inspect(
            "B",
            "actor",
        ),
        group_history=[1, 2, 3],
        expected_obj_asserts=expected_actor_group,
    )


def test_inspect_grouped_b_references(inspect_context):
    trace = inspect_context["trace"]
    ocean_id = inspect_context["ocean_id"]

    expected_ref_group_no_ver = [
        create_expected_dict(
            obj="global:@B",
            history=[0, 1, 2, 3],
            upstream=[1, 2, 3, 6],
            downstream=[2, 3, 4],
            oceans=[ocean_id],
            runners=[
                "TIDE: .event()",
                "TIDE: .exec(0)",
                "TIDE: .exec(1)",
            ],
            pipelines=[
                (
                    "TIDE: .event(B[1])",
                    "TIDE: .event(B[1])",
                ),
                (
                    0,
                    "TIDE: .exec(B[0])",
                ),
                (
                    "TIDE: .event(B[2])",
                    "TIDE: .event(B[2])",
                ),
                (
                    1,
                    "TIDE: .exec(X[1])",
                ),
            ],
        ),
        create_expected_dict(
            obj="testing:@B",
            history=[0],
            upstream=[1],
            downstream=[2, 6],
            oceans=[ocean_id],
            runners=["TIDE: .event()"],
            pipelines=[
                (
                    "TIDE: .event(B[1])",
                    "TIDE: .event(B[1])",
                )
            ],
        ),
    ]

    assert_inspected_group(
        inspect=trace.inspect,
        type="reference",
        inspected_group=trace.inspect(
            "B",
            "reference",
        ),
        group_history=[
            ("global:@B", 0),
            ("global:@B", 1),
            ("global:@B", 2),
            ("global:@B", 3),
            ("testing:@B", 0),
        ],
        expected_obj_asserts=expected_ref_group_no_ver,
    )


def test_inspect_grouped_b_references_version_zero(inspect_context):
    trace = inspect_context["trace"]
    ocean_id = inspect_context["ocean_id"]

    expected_ref_group_ver = [
        create_expected_dict(
            obj="global:@B",
            version=0,
            history=[0],
            upstream=[1],
            downstream=[2],
            oceans=[ocean_id],
            runners=["TIDE: .event()"],
            pipelines=[
                (
                    "TIDE: .event(B[1])",
                    "TIDE: .event(B[1])",
                )
            ],
        ),
        create_expected_dict(
            obj="testing:@B",
            version=0,
            history=[0],
            upstream=[1],
            downstream=[2, 6],
            oceans=[ocean_id],
            runners=["TIDE: .event()"],
            pipelines=[
                (
                    "TIDE: .event(B[1])",
                    "TIDE: .event(B[1])",
                )
            ],
        ),
    ]

    assert_inspected_group(
        inspect=trace.inspect,
        type="reference",
        inspected_group=trace.inspect(
            "@B",
            "reference",
            version=0,
        ),
        group_history=[
            ("global:@B", 0),
            ("testing:@B", 0),
        ],
        expected_obj_asserts=expected_ref_group_ver,
    )


@pytest.mark.parametrize(
    "kind",
    [
        "reference",
        "actor",
        "value",
    ],
)
def test_inspect_missing_object_returns_nonexistent(inspect_context, kind):
    inspected = inspect_context["trace"].inspect(
        "doesnt_exist",
        kind,
    )

    assert inspected is not None


def test_inspect_missing_exec_record_doesnt_raise_key_error(inspect_context):
    e = None
    try:
        inspect_context["trace"].inspect(
            "doesnt_exist",
            "exec_record",
        )
    except KeyError as e:
        e
    
    assert e == None
