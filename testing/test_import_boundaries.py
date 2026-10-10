import concurrent.futures
from pathlib import Path
import subprocess
import sys

import pytest


def _read_source_from_explicit_process_node():
    from tide_tooling import READ

    return READ("global:source")


def test_engine_directory_does_not_shadow_stdlib_logging():
    """The TIDE-specific module name must not shadow Python's logging."""
    project_root = Path(__file__).resolve().parents[1]
    engine_directory = project_root / "tide_tooling" / "tooling_core" / "engine"

    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import concurrent.futures; "
                "from concurrent.futures import ThreadPoolExecutor; "
                "import logging; "
                "assert logging.__file__ is not None; "
                "assert not logging.__file__.replace('\\\\', '/').endswith(" 
                "'/tide_tooling/tooling_core/engine/tide_logging.py'); "
                "assert ThreadPoolExecutor is concurrent.futures.ThreadPoolExecutor"
            ),
        ],
        cwd=engine_directory,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr


def test_public_tide_import_exposes_pipeline_api():
    from tide_tooling import TIDE, node, pipeline

    assert TIDE is not None
    assert callable(node)
    assert callable(pipeline)
    assert concurrent.futures.ThreadPoolExecutor is not None


def test_node_wrappers_execute_like_normal_functions_outside_pipeline_builds():
    from tide_tooling import node

    @node
    def add_one(value):
        return value + 1

    @node(targets=["result"])
    def double(value):
        return value * 2

    assert add_one(2) == 3
    assert double(3) == 6
    assert add_one.raw(4) == 5

    node_definition = double.as_node(5)
    assert node_definition.func is double.raw
    assert node_definition.args.raw == (5,)


def test_node_wrappers_support_manual_event_tracing_with_real_values():
    from tide_tooling import TIDE, node

    @node
    def create_data():
        return {"value": 2}

    @node
    def process_data(raw_data):
        return {"value": raw_data["value"] * 2}

    @node
    def trust_check_data(processed_data):
        return processed_data["value"] == 4

    tide = TIDE()

    data = create_data()
    tide.event(create_data, writes={"raw": data})

    processed_data = process_data(data)
    tide.event(
        process_data,
        reads=["raw"],
        writes={"processed": processed_data},
    )

    trust = trust_check_data(processed_data)
    tide.event(
        trust_check_data,
        reads=["processed"],
        writes={"trust": trust},
    )

    trace = tide.trace()
    assert data == {"value": 2}
    assert processed_data == {"value": 4}
    assert trust is True
    assert [record.func_name for record in trace.exec_log] == [
        "create_data",
        "process_data",
        "trust_check_data",
    ]
    assert trace.value_of("global:trust", 0) is True


def test_effects_are_ordinary_descriptive_values_outside_tide_runtime():
    from tide_tooling import READ, WRITE, node
    from tide_tooling.tooling_core.engine.classes import __TIDE_TEMPLATE_VALUE__

    @node
    def request_write():
        return WRITE("global:result", preserve_output=3)

    effect = request_write()

    assert effect.to_dict() == {
        "effect": "WRITE",
        "preserve_output": 3,
        "targets": ["global:result"],
    }
    assert str(effect) == "WRITE(targets=['global:result'])"
    assert isinstance(
        READ("global:unavailable"),
        __TIDE_TEMPLATE_VALUE__,
    )


def test_read_effect_returns_value_and_records_runtime_evidence(capsys):
    from tide_tooling import Effects, READ, TIDE

    tide = TIDE()
    tide.declare("source", 41)

    def copy_source():
        return READ("global:source")

    output = tide.exec(copy_source, writes="copied")
    trace = tide.trace()
    effects = trace.exec_log[-1].effects

    assert output == 41
    assert trace.value_of("global:copied", 0) == 41
    assert len(effects) == 1
    assert effects[0].effect == Effects.READ
    assert effects[0].targets == ("global:source", 0)
    assert effects[0].output == 41

    read_record = trace.exec_log[-1].deps[-1]
    assert read_record.associated_input[0] == "runtime_read"
    assert read_record.unscoped_reference == "global:source"
    assert read_record.resolved_version == 0
    assert read_record.resolved_value == 41
    assert read_record.producer_exec_id is not None
    assert trace.path(
        "global:source",
        "global:copied",
        "reference",
        "reference",
    ).exists()

    trace.show(color=False)
    plain = capsys.readouterr().out
    assert "read: global:source[v0] via READ effect" in plain
    assert "used: global:source[v0]" not in plain
    assert "effects:" in plain
    assert "read -> global:source[v0]" in plain

    trace.show(color=True, simple=True)
    coloured = capsys.readouterr().out
    assert "\x1b[36mread:\x1b[0m" in coloured
    assert "via READ effect" in coloured

    imported = TIDE.from_bundle(trace.to_bundle())
    imported_read = imported.exec_log[-1].deps[-1]
    assert imported_read.associated_input[0] == "runtime_read"
    assert imported_read.unscoped_reference == "global:source"
    assert imported.path(
        "global:source",
        "global:copied",
        "reference",
        "reference",
    ).exists()


def test_read_effect_requeues_explicit_process_execution_to_thread_pool():
    from tide_tooling.tooling_core.engine.classes import Node
    from tide_tooling import Effects, TIDE

    tide = TIDE()
    tide.declare("source", 41)
    trace, record, output = tide.ocean.run_node(
        Node(
            func=_read_source_from_explicit_process_node,
            targets=["global:copied"],
        ),
        mode="process",
    )

    assert output == 41
    assert record.status == "succeeded"
    assert trace.value_of("global:copied", 0) == 41
    assert record.deps[-1].unscoped_reference == "global:source"
    assert record.effects[0].effect == Effects.READ


def test_read_effect_uses_template_fallback_and_still_records_attempt():
    from tide_tooling import Effects, READ, TIDE
    from tide_tooling.tooling_core.engine.classes import __TIDE_TEMPLATE_VALUE__

    def read_missing():
        return READ("global:not_there")

    tide = TIDE()
    output = tide.exec(read_missing, writes="fallback")
    trace = tide.trace()
    effect = trace.exec_log[-1].effects[0]

    assert isinstance(output, __TIDE_TEMPLATE_VALUE__)
    assert isinstance(
        trace.value_of("global:fallback", 0),
        __TIDE_TEMPLATE_VALUE__,
    )
    assert effect.effect == Effects.READ
    assert effect.targets == ("global:not_there", 0)
    assert isinstance(effect.output, __TIDE_TEMPLATE_VALUE__)


def test_public_decorators_run_pipeline_after_import():
    """Exercise the public workflow from the original shadowing report."""
    from tide_tooling import TIDE, node, pipeline

    def create_data():
        return {number: None for number in range(20)}

    def process_data(raw_data):
        return dict(raw_data)

    @node(targets=["trust_flag"])
    def trust_check_data(processed_data):
        return all(isinstance(value, int) for value in processed_data.values())

    @pipeline
    def subtle_bug():
        return trust_check_data(process_data(create_data()))

    tide = TIDE()
    trace = tide.ocean.run_pipeline(subtle_bug)

    assert trace is not None
    # Only trust_check_data is a node in this exact example. The two ordinary
    # functions run while the decorated pipeline graph is being constructed.
    # Their resulting plain dict is now represented by one observer execution
    # before trust_check_data consumes it.
    observer, trust_check = trace.exec_log
    assert observer.func_name == "TIDE_OBSERVER"
    assert trust_check.func_name == "trust_check_data"
    assert trust_check.deps[0].producer_exec_id == observer.exec_id


def test_public_decorators_build_and_run_three_link_pipeline():
    from tide_tooling import TIDE, node, pipeline

    @node(targets=["raw_data"])
    def create_data():
        return {number: None for number in range(20)}

    @node(targets=["processed_data"])
    def process_data(raw_data):
        return dict(raw_data)

    @node(targets=["trust_flag"])
    def trust_check_data(processed_data):
        return all(isinstance(value, int) for value in processed_data.values())

    @pipeline
    def subtle_bug():
        return trust_check_data(process_data(create_data()))

    trace = TIDE().ocean.run_pipeline(subtle_bug)

    assert trace is not None
    assert [record.func_name for record in trace.exec_log] == [
        "create_data",
        "process_data",
        "trust_check_data",
    ]


@pytest.mark.parametrize(
    ("produced_value", "expected_type"),
    [
        ({0: None, 1: 10}, dict),
        ([None, 10], list),
        ((None, 10), tuple),
        ("raw-data", str),
        (42, int),
    ],
)
def test_targetless_nested_node_preserves_single_result_shape(
    produced_value, expected_type
):
    """A node's single implicit output is one value, not a one-item list."""
    from tide_tooling import TIDE, node, pipeline

    observed = {}

    @node
    def producer():
        return produced_value

    @node(targets=["observed_type"])
    def consumer(value):
        observed["value"] = value
        return type(value).__name__

    @pipeline
    def shape_pipeline():
        return consumer(producer())

    trace = TIDE().ocean.run_pipeline(shape_pipeline)

    assert trace is not None
    assert type(observed["value"]) is expected_type
    assert observed["value"] == produced_value


def test_line_by_line_pipeline_collects_chain_without_return():
    from tide_tooling import TIDE, node, pipeline

    @node
    def create_data():
        return {0: None, 1: 10}

    @node
    def process_data(raw_data):
        return dict(raw_data)

    @node(targets=["trust_flag"])
    def trust_check_data(processed_data):
        return all(isinstance(value, int) for value in processed_data.values())

    @pipeline
    def line_by_line():
        raw_data = create_data()
        processed_data = process_data(raw_data)
        trust_check_data(processed_data)

    built = line_by_line()
    trace = TIDE().ocean.run_pipeline(built)

    assert len(built.stages) == 1
    assert len(built.stages[0].nodes) == 1
    assert built.stages[0].nodes[0].node_name == "trust_check_data"
    assert [record.func_name for record in trace.exec_log] == [
        "create_data",
        "process_data",
        "trust_check_data",
    ]


def test_line_by_line_pipeline_keeps_disconnected_nodes_as_roots():
    from tide_tooling import TIDE, node, pipeline

    @node(targets=["left"])
    def write_left():
        return "left"

    @node(targets=["right"])
    def write_right():
        return "right"

    @pipeline
    def disconnected():
        write_left()
        write_right()

    built = disconnected()
    trace = TIDE().ocean.run_pipeline(built)

    assert [node.node_name for node in built.stages[0].nodes] == [
        "write_left",
        "write_right",
    ]
    assert {record.func_name for record in trace.exec_log} == {
        "write_left",
        "write_right",
    }


def test_empty_line_by_line_pipeline_raises_clear_error():
    from tide_tooling import pipeline

    @pipeline
    def empty_pipeline():
        pass

    with pytest.raises(ValueError, match="did not build or return any nodes"):
        empty_pipeline()


def test_parameterised_pipeline_builds_fresh_nodes_for_each_call():
    from tide_tooling import node, pipeline

    @node(targets=["global:result"])
    def echo(value):
        return value

    @pipeline
    def parameterised(value):
        return echo(value)

    first = parameterised("first")
    second = parameterised("second")
    first_node = first.stages[0].nodes[0]
    second_node = second.stages[0].nodes[0]

    assert first is not second
    assert first_node is not second_node
    assert first_node.args.raw == ("first",)
    assert second_node.args.raw == ("second",)


def test_repeated_node_calls_create_distinct_occurrences_in_one_build():
    from tide_tooling import node, pipeline

    @node
    def observe(value):
        return value

    @pipeline
    def repeated():
        observe("first")
        observe("second")

    built = repeated()
    nodes = built.stages[0].nodes

    assert len(nodes) == 2
    assert nodes[0] is not nodes[1]
    assert [node.args.raw for node in nodes] == [
        ("first",),
        ("second",),
    ]


def test_tide_exec_unwraps_decorated_node_function():
    from tide_tooling import TIDE, node

    @node(targets=["decorator_target"])
    def add_one(value):
        return value + 1

    tide = TIDE()
    output = tide.exec(add_one, args=[2], writes=["exec_target"])
    trace = tide.trace()

    assert output == 3
    assert trace.value_of("global:exec_target", 0) == 3
    assert "global:decorator_target" not in trace.index.seen_refs
    assert trace.exec_log[-1].func_name == "add_one"


def test_tide_exec_accepts_called_node_definition_and_inherits_inputs_and_targets():
    from tide_tooling import TIDE, node

    @node(targets=["global:doubled"])
    def double(value):
        return value * 2

    node_definition = double.as_node(4)
    tide = TIDE()
    output = tide.exec(node_definition)

    assert output == 8
    assert tide.trace().value_of("global:doubled", 0) == 8


def test_tide_exec_context_builds_and_executes_nested_node_graph():
    from tide_tooling import TIDE, node

    @node
    def create_data():
        return {"value": 2}

    @node
    def process_data(raw_data):
        return {"value": raw_data["value"] * 2}

    @node(targets=["global:trust"])
    def trust_check_data(processed_data):
        return processed_data["value"] == 4

    tide = TIDE()
    with tide.exec as execute:
        output = execute(
            trust_check_data(process_data(create_data()))
        )

    trace = tide.trace()

    assert output is True
    assert [record.func_name for record in trace.exec_log] == [
        "create_data",
        "process_data",
        "trust_check_data",
    ]
    assert trace.value_of("global:trust", 0) is True
    assert create_data() == {"value": 2}


def test_tide_exec_is_quiet_until_show_is_requested(capsys):
    from tide_tooling import TIDE, node

    @node(targets=["global:result"])
    def produce():
        return "value"

    tide = TIDE()
    tide.exec(produce.as_node())

    assert capsys.readouterr().out == ""

    tide.trace().show(color=False)
    shown = capsys.readouterr().out

    assert "TRACE | 1 execution found" in shown
    assert "produce" in shown


def test_tide_exec_accepts_a_single_write_string():
    from tide_tooling import TIDE

    def produce():
        return "value"

    tide = TIDE()
    output = tide.exec(produce, writes="result")

    assert output == "value"
    assert tide.trace().value_of("global:result", 0) == "value"


def test_single_decorator_effect_is_inherited_by_tide_exec():
    from tide_tooling import Effects, TIDE, WRITE, node

    @node(effects=WRITE("global:effect_output", preserve_output=7))
    def produce():
        return 5

    tide = TIDE()
    output = tide.exec(produce)
    trace = tide.trace()
    record = trace.exec_log[-1]

    assert output == 5
    assert trace.value_of("global:effect_output", 0) == 7
    assert record.declared_effects[0].targets[0].key == "effect_output"
    assert record.effects[0].effect == Effects.WRITE


def test_tide_exec_accepts_an_explicit_effect():
    from tide_tooling import Effects, TIDE, WRITE

    def produce():
        return "ordinary output"

    tide = TIDE()
    output = tide.exec(
        produce,
        effects=WRITE("global:effect_output", preserve_output=11),
    )
    tide.exec(
        lambda value: value + 1,
        reads="global:effect_output",
        writes="after_effect",
    )
    trace = tide.trace()
    producer_record = trace.exec_log[0]
    consumer_record = trace.exec_log[1]
    effect_record = producer_record.effects[0]

    assert output == "ordinary output"
    assert trace.value_of("global:effect_output", 0) == 11
    assert effect_record.effect == Effects.WRITE
    assert effect_record.targets == ("global:effect_output", 0)
    assert trace.index.effect_records_by_type[Effects.WRITE] == [effect_record]
    assert trace.index.effect_records_by_target[
        (Effects.WRITE, "global:effect_output")
    ] == [effect_record]
    assert trace.index.effect_records_by_versioned_target[
        (Effects.WRITE, "global:effect_output", 0)
    ] == [effect_record]
    assert consumer_record.exec_id in {
        affected.affected_exec_id
        for entry in trace.index.effect_type_to_affected[Effects.WRITE]
        for affected in entry.values()
    }
    assert trace.index.effect_affected_input_positions[
        (
            Effects.WRITE,
            "global:effect_output",
            0,
            consumer_record.exec_id,
        )
    ]

    inspected_type = trace.find(Effects.WRITE).first("effect")
    inspected_target = trace.inspect(
        (Effects.WRITE, "global:effect_output")
    )
    inspected_version = trace.inspect(
        (Effects.WRITE, ("global:effect_output", 0))
    )

    assert inspected_type.exists
    assert inspected_type.id == Effects.WRITE
    assert inspected_target.exists
    assert inspected_target.id == (
        Effects.WRITE,
        "global:effect_output",
    )
    assert inspected_version.exists
    assert inspected_version.version == 0
    assert producer_record.exec_id in inspected_version.history


def test_tide_event_accepts_and_applies_reference_effects():
    from tide_tooling import DELETE, Effects, TIDE, WRITE

    tide = TIDE()
    tide.event(
        "effect writer",
        effects=WRITE("global:event_effect", preserve_output=13),
    )
    write_trace = tide.trace()
    write_record = write_trace.exec_log[-1]

    assert write_trace.value_of("global:event_effect", 0) == 13
    assert write_record.declared_effects[0].preserve_output == 13
    assert write_record.effects[0].effect == Effects.WRITE

    tide.event("effect deleter", effects=DELETE("global:event_effect"))
    delete_trace = tide.trace()
    delete_record = delete_trace.exec_log[-1]

    assert not tide.ocean.store_router.exists("global", "event_effect")
    assert delete_record.effects[0].effect == Effects.DELETE


def test_effect_lists_support_investigate_path_and_diff(capsys):
    from tide_tooling import Effects, TIDE, WRITE

    tide = TIDE()
    tide.declare("seed", 2)

    def produce(seed):
        return seed

    tide.exec(
        produce,
        reads="seed",
        effects=[
            WRITE("global:effect_output", preserve_output=7),
            WRITE("global:audit_output", preserve_output=9),
        ],
    )
    tide.exec(
        lambda value: value + 1,
        reads="effect_output",
        writes="consumed_output",
    )
    trace = tide.trace()
    effect_producer = next(
        record
        for record in trace.exec_log
        if record.func_name == "produce"
    )
    effect_consumer = next(
        record
        for record in trace.exec_log
        if record.func_name == "<lambda>"
    )

    investigated = trace.investigate(Effects.WRITE, "effect")
    data = investigated.to_dict()

    assert data["summary"] == {
        "effect_count": 2,
        "target_count": 2,
        "producer_count": 1,
        "affected_execution_count": 1,
    }
    investigated.show(color=False)
    shown = capsys.readouterr().out
    assert "WRITE -> global:effect_output[v0]" in shown
    assert "WRITE -> global:audit_output[v0]" in shown
    assert "affected EXEC_RECORD" in shown

    effect_path = trace.path(
        (Effects.WRITE, ("global:effect_output", 0)),
        effect_consumer.exec_id,
        "effect",
        "exec_record",
    )
    assert effect_path.exists()
    effect_path.show(color=False)
    shown = capsys.readouterr().out
    assert "affected by WRITE effect" in shown
    assert "global:effect_output[v0]" in shown
    assert trace.path(
        "global:seed",
        (Effects.WRITE, ("global:effect_output", 0)),
        "reference",
        "effect",
    ).exists()

    compared = trace.diff(
        (Effects.WRITE, ("global:effect_output", 0)),
        (Effects.WRITE, ("global:audit_output", 0)),
    )
    assert not compared.same
    assert effect_producer.exec_id in investigated.inspected.history


def test_trace_uses_the_single_exec_source_for_its_header():
    from tide_tooling import TIDE, node

    @node
    def produce():
        return "value"

    tide = TIDE()
    tide.exec(produce.as_node(), writes="result")
    trace = tide.trace()

    assert trace.run == "TIDE: .exec(0)"
    assert trace.pipeline == "TIDE: .exec(produce[0])"
    assert trace.execution_sources == ({
        "run": "TIDE: .exec(0)",
        "pipeline_id": 0,
        "pipeline": "TIDE: .exec(produce[0])",
    },)


def test_trace_shows_each_source_when_exec_calls_are_combined(capsys):
    from tide_tooling import TIDE

    def first():
        return 1

    def second(value):
        return value + 1

    tide = TIDE()
    tide.exec(first, writes="first")
    tide.exec(second, reads="first", writes="second")
    trace = tide.trace()

    assert trace.run == "MULTIPLE (2)"
    assert trace.pipeline == "MULTIPLE (2)"

    trace.show(color=False)
    shown = capsys.readouterr().out

    assert "Tide Runs: MULTIPLE (2)" in shown
    assert "Pipelines: MULTIPLE (2)" in shown
    assert "Sources:    2" in shown
    assert "TIDE: .exec(first[0])" in shown
    assert "TIDE: .exec(second[1])" in shown
