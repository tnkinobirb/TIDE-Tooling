import pytest

from tide_tooling import DataRef, Ocean, TIDE, Trace, node, pipeline
from tide_tooling.tooling_core.engine.tide_logging import DependencyUseRecord, ExecRecord, ExpandedDepRecord, WriteRecord
from tide_tooling.tooling_core.engine.errors import DataRefInitError, NoExecRecordError
from tide_tooling.tooling_core.engine.utils import tide_snapshot_value

import threading
import time

from tide_tooling.tooling_core.engine.classes import Event


from tide_tooling.tooling_core.engine.events import LogEvents, RouterEvents

def dynamic_pattern(
    pattern: str,
    minimum: int,
    returned: int,
    store: str = "global",
    strip_leading: bool = False,
) -> str:
    value = f"{store}:{pattern}:{minimum}:{returned}"

    if strip_leading:
        return value

    return f"@{value}"

def make_exec(seq_id, name, deps=None, writes=None):
    return ExecRecord(
        seq_id=seq_id,
        exec_id=f"{seq_id:032x}",
        ocean_id=0,
        runner_id="tests",
        pipeline_id="tests",
        node_id=name,
        func=lambda: None,
        func_name=name,
        inputs={},
        deps=deps or [],
        effects=[],
        declared_effects=[],
        declared_targets=[],
        runtime_effects=[],
        runtime_targets=[],
        declared_deps=[],
        unscoped_store_refs=[],
        status="complete",
        started_at=None,
        finished_at=None,
        duration_ms=0,
        error=None,
        node_options={},
        writes=writes or [],
    )


def create_dynamic_index_trace(returned_refs):
    producers = []

    for seq_id, (name, ref, value) in enumerate([
        ("Alpha", "global:@alpha", "!alpha"),
        ("AlphaOne", "global:@alpha_one", "!alpha_one"),
        ("BetaAlpha", "global:@beta_alpha", "!beta_alpha"),
        ("Beta", "global:@beta", "!beta"),
    ]):
        producers.append(
            make_exec(
                seq_id,
                name,
                writes=[WriteRecord("test", value, 0, ref)],
            )
        )

    by_ref = {
        write.stored_target: producer
        for producer in producers
        for write in producer.writes
    }

    associated_input = ("reads", object())

    expanded = [
        ExpandedDepRecord(
            associated_input,
            None,
            "static",
            ref,
            by_ref[ref].writes[0].stored_result,
            0,
            "tests",
            by_ref[ref].producer_id,
            by_ref[ref].exec_id,
            [],
        )
        for ref in returned_refs
    ]

    dynamic = DependencyUseRecord(
        associated_input,
        None,
        "dynamic",
        "global:*alpha*",
        expanded_to=expanded,
    )

    consumer = make_exec(len(producers), "Consumer", deps=[dynamic])

    return Trace(
        event_log=None,
        exec_log=[*producers, consumer],
        runner="tests",
        ocean_id=0,
    )


def test_dynamic_parent_preserves_only_returned_expansions():
    returned = {
        "global:@alpha",
        "global:@alpha_one",
        "global:@beta_alpha",
    }

    trace = create_dynamic_index_trace(returned)
    dynamic = trace.exec_log[-1].deps[0]

    assert {
        expanded.unscoped_reference
        for expanded in dynamic.expanded_to
    } == returned


def test_dynamic_return_count_is_preserved_by_index():
    trace = create_dynamic_index_trace({
        "global:@alpha",
        "global:@alpha_one",
    })

    dynamic = trace.exec_log[-1].deps[0]

    assert len(dynamic.expanded_to) == 2
    assert len(trace.index.consumer_exec_id_to_producer_input_edges[trace.exec_log[-1].exec_id]) == 2


def test_dynamic_expansions_are_indexed_as_exact_ref_version_facts():
    returned = {
        "global:@alpha",
        "global:@alpha_one",
    }

    trace = create_dynamic_index_trace(returned)
    index = trace.index
    consumer_id = trace.exec_log[-1].exec_id

    assert list(index.reads_by_exec_id[consumer_id]) == [trace.exec_log[-1].deps[0]]

    for ref in returned:
        assert consumer_id in index.ref_version_to_consumer_exec_ids[(ref, 0)]


def test_dynamic_unreturned_candidates_are_not_indexed_as_consumed():
    trace = create_dynamic_index_trace({
        "global:@alpha",
    })

    index = trace.index
    consumer_id = trace.exec_log[-1].exec_id

    for ref in {
        "global:@alpha_one",
        "global:@beta_alpha",
        "global:@beta",
    }:
        assert consumer_id not in index.ref_version_to_consumer_exec_ids[(ref, 0)]


def test_dynamic_forward_and_reverse_edges_match_exactly():
    trace = create_dynamic_index_trace({
        "global:@alpha",
        "global:@alpha_one",
        "global:@beta_alpha",
    })

    index = trace.index
    consumer_id = trace.exec_log[-1].exec_id
    reverse_edges = index.consumer_exec_id_to_producer_input_edges[consumer_id]

    assert len(reverse_edges) == 3

    for edge in reverse_edges:
        forward_edges = index.producer_exec_id_to_consumer_input_edges[edge.producer_exec_id]
        assert any(forward_edge is edge for forward_edge in forward_edges)


def seed_dynamic_store(tide, values=None, store="global"):
    values = values or {
        "@alpha": "!alpha",
        "@alpha_one": "!alpha_one",
        "@beta_alpha": "!beta_alpha",
        "@beta": "!beta",
    }

    target_store = tide.ocean.store_registry.get(store)

    for ref, value in values.items():
        target_store.set(ref, value)


def run_dynamic_consumer(
    tide: TIDE,
    *,
    pattern="*alpha*",
    minimum=1,
    returned=1,
):
    calls = []

    def consume_dynamic(*values):
        calls.append(values)
        return tuple(values)

    tide.exec(
        consume_dynamic,
        reads=[
            dynamic_pattern(
                pattern,
                minimum,
                returned,
                strip_leading=True,
            )
        ],
    )

    return calls


def test_dynamic_runtime_does_not_execute_before_minimum_is_satisfied():
    tide = TIDE()

    seed_dynamic_store(
        tide,
        values={
            "@alpha": "!alpha",
        },
    )
    

    with pytest.raises(NoExecRecordError):
        run_dynamic_consumer(
            tide,
            pattern="*alpha*",
            minimum=2,
            returned=2,
        )



def test_dynamic_runtime_does_not_execute_when_pattern_has_no_matches():
    tide = TIDE()

    seed_dynamic_store(
        tide,
        values={
            "@alpha": "!alpha",
        },
    )
    with pytest.raises(NoExecRecordError):
        run_dynamic_consumer(
            tide,
            pattern="*missing*",
            minimum=1,
            returned=1,
        )


def test_dynamic_runtime_executes_when_minimum_is_reached():
    tide = TIDE()

    seed_dynamic_store(
        tide,
        values={
            "@alpha": "!alpha",
            "@alpha_one": "!alpha_one",
        },
    )

    calls = run_dynamic_consumer(
        tide,
        pattern="*alpha*",
        minimum=2,
        returned=2,
    )

    assert len(calls) == 1
    assert len(calls[0]) == 1
    assert len(calls[0][0]) == 2
    assert set(calls[0][0]) == {
        "!alpha",
        "!alpha_one",
    }


def test_dynamic_runtime_returns_exact_declared_count():
    tide = TIDE()

    seed_dynamic_store(tide)

    calls = run_dynamic_consumer(
        tide,
        pattern="*alpha*",
        minimum=2,
        returned=1,
    )

    assert len(calls) == 1
    assert len(calls[0]) == 1
    assert len(calls[0][0]) == 1
    assert set(calls[0][0]) == {
        "!alpha",
    }


def test_dynamic_numeric_star_resolves_exactly_the_numeric_count():
    tide = TIDE()
    seed_dynamic_store(tide)
    calls = []

    def consume_dynamic(values):
        calls.append(values)

    tide.exec(consume_dynamic, reads=["global:*alpha*:2:*"])

    assert len(calls) == 1
    assert calls[0] == ["!alpha", "!alpha_one"]


def test_dynamic_runtime_default_minimum_is_one_and_max_is_unbounded():
    tide = TIDE()
    seed_dynamic_store(tide)
    calls = []

    def consume_dynamic(values):
        calls.append(values)

    tide.exec(consume_dynamic, reads=["global:*alpha*"])

    assert len(calls) == 1
    assert set(calls[0]) == {
        "!alpha",
        "!alpha_one",
        "!beta_alpha",
    }


@pytest.mark.parametrize(
    ("text", "key", "minimum", "maximum"),
    [
        ("mem:csv:*", "csv", 1, None),
        ("mem:csv:*:*", "csv", 1, None),
        ("mem:csv*:*:*", "csv*", 1, None),
        ("mem:csv:1:*", "csv", 1, 1),
        ("mem:csv:*:1", "csv", 1, 1),
        ("mem:csv:5", "csv", 5, 5),
        ("mem:csv:5:*", "csv", 5, 5),
        ("mem:csv:*:5", "csv", 5, 5),
        ("mem:csv:5:3", "csv", 5, 3),
    ],
)
def test_dynamic_count_syntax_keeps_store_and_key_boundary(
    text,
    key,
    minimum,
    maximum,
):
    ref = DataRef.from_string(text)

    assert ref.store == "mem"
    assert ref.key == key
    assert ref.count == minimum
    assert ref.max == maximum
    assert ref.dynamic is True

    reparsed = DataRef.from_string(
        DataRef.to_string(ref, strip_leading=True)
    )
    assert reparsed.store == ref.store
    assert reparsed.key == ref.key
    assert reparsed.count == minimum
    assert reparsed.max == maximum


@pytest.mark.parametrize(
    ("text", "canonical"),
    [
        ("mem:csv:*", "mem:csv:*"),
        ("mem:csv:*:*", "mem:csv:*"),
        ("mem:csv*:*:*", "mem:csv*:*"),
        ("mem:csv*:1", "mem:csv*:1"),
        ("mem:csv*:1:*", "mem:csv*:1"),
        ("mem:csv*:*:1", "mem:csv*:1"),
        ("mem:csv:5", "mem:csv:5"),
        ("mem:csv:5:*", "mem:csv:5"),
        ("mem:csv:*:5", "mem:csv:5"),
        ("mem:csv:5:3", "mem:csv:5:3"),
        ("mem:csv*", "mem:csv*"),
        ("mem:*csv", "mem:*csv"),
        ("mem:*csv*", "mem:*csv*"),
    ],
)
def test_dynamic_reference_serialisation_uses_compact_public_grammar(
    text,
    canonical,
):
    assert DataRef.to_string(
        DataRef.from_string(text),
        strip_leading=True,
    ) == canonical


@pytest.mark.parametrize(
    "text",
    [
        "mem:csv:*:5:*",
        "mem:csv:*:5:10",
    ],
)
def test_dynamic_count_syntax_rejects_extra_wildcard_field(text):
    with pytest.raises(ValueError, match=r"STORE:KEY\[:MIN\[:MAX\]\]"):
        DataRef.from_string(text)


def test_dynamic_count_syntax_rejects_maximum_above_minimum():
    with pytest.raises(
        DataRefInitError,
        match=r"MAX 10 > MIN 5",
    ):
        DataRef.from_string("mem:csv:5:10")


def test_direct_dynamic_reference_rejects_maximum_above_default_minimum():
    with pytest.raises(
        DataRefInitError,
        match=r"MAX 2 > MIN 1",
    ):
        DataRef(key="csv*", store="mem", max=2)


def test_dynamic_runtime_waits_for_minimum_then_resolves_only_maximum():
    tide = TIDE()
    seed_dynamic_store(
        tide,
        values={
            "@alpha_0": "!alpha-0",
            "@alpha_1": "!alpha-1",
            "@alpha_2": "!alpha-2",
            "@alpha_3": "!alpha-3",
            "@alpha_4": "!alpha-4",
        },
    )
    calls = []

    def consume_dynamic(values):
        calls.append(values)

    tide.exec(consume_dynamic, reads=["global:*alpha*:5:3"])
    trace = tide.trace()
    dynamic_read = trace.exec_log[-1].deps[0]

    assert len(calls) == 1
    assert calls[0] == ["!alpha-0", "!alpha-1", "!alpha-2"]
    assert len(dynamic_read.expanded_to) == 3
    assert len(trace.concretisations["global:*alpha*:5:3"]) == 3


def test_dynamic_star_star_resolves_and_concretises_every_found_match():
    tide = TIDE()
    seed_dynamic_store(
        tide,
        values={
            "@alpha_0": "!alpha-0",
            "@alpha_1": "!alpha-1",
            "@alpha_2": "!alpha-2",
            "@alpha_3": "!alpha-3",
        },
    )
    calls = []

    def consume_dynamic(values):
        calls.append(values)

    tide.exec(consume_dynamic, reads=["global:*alpha*:*:*"])
    trace = tide.trace()
    dynamic_read = trace.exec_log[-1].deps[0]

    assert calls == [[
        "!alpha-0",
        "!alpha-1",
        "!alpha-2",
        "!alpha-3",
    ]]
    assert len(dynamic_read.expanded_to) == 4
    assert trace.concretisations["global:*alpha*:*"] == (
        ("global:@alpha_0", 0),
        ("global:@alpha_1", 0),
        ("global:@alpha_2", 0),
        ("global:@alpha_3", 0),
    )


@pytest.mark.parametrize(
    "selector",
    [
        "global:*alpha*:1",
        "global:*alpha*:1:*",
        "global:*alpha*:*:1",
    ],
)
def test_dynamic_exact_one_forms_resolve_only_one(selector):
    tide = TIDE()
    seed_dynamic_store(tide)
    calls = []

    def consume_dynamic(values):
        calls.append(values)

    tide.exec(consume_dynamic, reads=[selector])
    trace = tide.trace()

    assert calls == [["!alpha"]]
    assert len(trace.exec_log[-1].deps[0].expanded_to) == 1
    assert len(trace.concretisations["global:*alpha*:1"]) == 1


@pytest.mark.parametrize("key", ["csv*", "*csv", "*csv*"])
def test_wildcard_anywhere_in_key_is_dynamic(key):
    ref = DataRef.from_string(f"mem:{key}")

    assert ref.store == "mem"
    assert ref.key == key
    assert ref.count is None
    assert ref.max is None
    assert ref.dynamic is True


def test_dynamic_default_minimum_reference_resolves_exact_key():
    ocean = Ocean()
    calls = []

    @node(targets=DataRef.literal("csv", "mem"))
    def write_one():
        return "one"

    @node(args=("@mem:csv:*",))
    def consume_csv(values):
        calls.append(values)

    @pipeline
    def flow():
        write_one()
        consume_csv()

    ocean.run_pipeline(flow, mode="sequential")

    assert calls == [["one"]]

    


def test_dynamic_runtime_selection_is_deterministic():
    def selected_values():
        tide = TIDE()
        seed_dynamic_store(tide)

        calls = run_dynamic_consumer(
            tide,
            pattern="*alpha*",
            minimum=2,
            returned=2,
        )

        assert len(calls) == 1
        return calls[0]

    first = selected_values()
    second = selected_values()

    assert first == second



def test_dynamic_ref_deleted_after_concretisation_before_resolution_does_not_execute(monkeypatch):
    tide = TIDE()
    calls = []
    results = []
    errors = []
    deleted = threading.Event()

    def consume_dynamic(values):
        calls.append(values)
        return values

    original_emit_event_to_ocean = tide.ocean.event_interface.emit_event_to_ocean

    def intercept_event(event):
        original_emit_event_to_ocean(event)

        if event.event_type == LogEvents.DEP_CONCRETISED and not deleted.is_set():
            deleted.set()
            tide.ocean.store_registry.get("global").delete("@alpha_one")

    monkeypatch.setattr(
        tide.ocean.event_interface,
        "emit_event_to_ocean",
        intercept_event,
    )

    seed_dynamic_store(
        tide,
        values={
            "@alpha": "!alpha",
            "@alpha_one": "!alpha_one",
        },
    )

    def execute():
        try:
            results.append(
                tide.exec(
                    consume_dynamic,
                    reads=[
                        dynamic_pattern(
                            "*alpha*",
                            2,
                            2,
                            strip_leading=True,
                        )
                    ],
                )
            )
        except Exception as error:
            errors.append(error)
    

    worker = threading.Thread(target=execute)
    worker.start()
    worker.join(timeout=None)

    assert not worker.is_alive()
    assert deleted.is_set()
    assert len(errors) == 1
    assert isinstance(errors[0], NoExecRecordError)
    assert calls == []
    assert results == []



def wait_until(
    predicate,
    timeout=2,
    interval=0.01,
    message="condition was not reached",
    tick=None,
):
    deadline = time.time() + timeout

    while time.time() < deadline:
        if tick is not None:
            tick()

        if predicate():
            return

        time.sleep(interval)

    raise AssertionError(message)


def emit_store_write(tide, key, value):
    tide.ocean.event_interface.emit_event_to_ocean(
        Event(
            source="tests",
            source_id="tests",
            target_runner_id=None,
            event_type=RouterEvents.STORE_WRITE,
            payload=(
                "global",
                key,
                value,
                "tests",
                "dynamic ref runtime test",
                tide_snapshot_value(value, boundary="write")
            ),
        )
    )

def start_waiting_dynamic_consumer(
    tide,
    monkeypatch,
    *,
    pattern="*alpha*",
    minimum=2,
    returned=2,
):
    calls = []
    errors = []
    results = []

    runner_started = threading.Event()
    runner_holder = []

    original_start_runner = tide.ocean._start_runner

    def intercept_start_runner(runner_id, stay_alive=True):
        original_start_runner(
            runner_id,
            stay_alive=stay_alive,
        )

        runner = tide.ocean._get_runner(runner_id)

        runner_holder.append(runner)
        runner_started.set()

    monkeypatch.setattr(
        tide.ocean,
        "_start_runner",
        intercept_start_runner,
    )

    def consume_dynamic(values):
        calls.append(values)
        return values

    def execute():
        try:
            results.append(
                tide.exec(
                    consume_dynamic,
                    reads=[
                        dynamic_pattern(
                            pattern,
                            minimum,
                            returned,
                            strip_leading=True,
                        )
                    ],
                )
            )
        except Exception as error:
            errors.append(error)

    worker = threading.Thread(target=execute)
    worker.start()

    assert runner_started.wait(timeout=2)
    assert len(runner_holder) == 1

    runner = runner_holder[0]

    wait_until(
        lambda: runner.poll is False,
        message="runner never completed its initial dependency poll",
    )

    wait_until(
        lambda: bool(runner.runtime_dag.waiting_dynamic_deps),
        message="dynamic dependency was never registered as waiting",
    )

    nodes = list(runner.index.node_index.node_by_id.values())
    assert len(nodes) == 1

    node = nodes[0]

    dynamic_deps = list(
        runner.runtime_dag.waiting_dynamic_deps.keys()
    )

    assert len(dynamic_deps) == 1

    dynamic_dep = dynamic_deps[0]

    assert runner.stay_alive is False
    assert node.dynamic_to_static_deps[dynamic_dep] == []

    return (
        worker,
        runner,
        node,
        dynamic_dep,
        calls,
        errors,
        results,
    )

def test_dynamic_runtime_live_writes_execute_only_after_minimum(monkeypatch):
    tide = TIDE()

    (
        worker,
        runner,
        node,
        dynamic_dep,
        calls,
        errors,
        results,
    ) = start_waiting_dynamic_consumer(
        tide,
        monkeypatch,
        pattern="*alpha*",
        minimum=2,
        returned=2,
    )

    emit_store_write(tide, "@beta", "!beta")

    global_store = tide.ocean.store_registry.get("global")

    wait_until(
        lambda: "@beta" in global_store.store,
        message="non-matching write was not processed",
    )

    assert node.dynamic_to_static_deps[dynamic_dep] == []
    assert calls == []

    emit_store_write(tide, "@alpha", "!alpha")

    wait_until(
        lambda: len(node.dynamic_to_static_deps[dynamic_dep]) == 1,
        message="first matching dynamic ref was not concretised",
    )

    assert calls == []
    assert worker.is_alive()

    emit_store_write(tide, "@alpha_one", "!alpha_one")

    wait_until(
        lambda: len(calls) == 1,
        message="consumer did not execute after minimum was reached",
    )

    runner.stay_alive = False

    worker.join(timeout=None)

    assert not worker.is_alive()
    assert errors == []
    assert len(results) == 1
    assert calls == [[
        "!alpha",
        "!alpha_one",
    ]]


def test_dynamic_runtime_duplicate_live_key_does_not_increase_match_count(monkeypatch):
    tide = TIDE()

    (
        worker,
        runner,
        node,
        dynamic_dep,
        calls,
        errors,
        results,
    ) = start_waiting_dynamic_consumer(
        tide,
        monkeypatch,
        pattern="*alpha*",
        minimum=2,
        returned=2,
    )

    emit_store_write(tide, "@alpha", "!alpha-v1")

    wait_until(
        lambda: len(node.dynamic_to_static_deps[dynamic_dep]) == 1,
        message="first dynamic ref was not concretised",
    )

    emit_store_write(tide, "@alpha", "!alpha-v2")

    wait_until(
        lambda: len(node.dynamic_to_static_deps[dynamic_dep]) == 1,
        message="first dynamic ref was not concretised",
    )

    global_store = tide.ocean.store_registry.get("global")

    wait_until(
        lambda: global_store.store.get("@alpha") == "!alpha-v2",
        message="updated store value was not processed",
    )

    assert len(node.dynamic_to_static_deps[dynamic_dep]) == 1
    assert calls == []
    assert worker.is_alive()

    emit_store_write(tide, "@alpha_one", "!alpha_one")

    wait_until(
        lambda: len(calls) == 1,
        message="consumer did not execute after second distinct ref arrived",
    )

    runner.stay_alive = False

    worker.join(timeout=None)

    assert not worker.is_alive()
    assert errors == []
    assert len(results) == 1
    assert len(calls) == 1
    assert calls[0] == [
        "!alpha-v2",
        "!alpha_one",
    ]


def test_dynamic_runtime_trace_builds_exact_expanded_records_and_edges():
    tide = TIDE()

    seed_dynamic_store(
        tide,
        values={
            "@alpha": "!alpha",
            "@alpha_one": "!alpha_one",
            "@beta_alpha": "!beta_alpha",
            "@beta": "!beta",
        },
    )

    calls = run_dynamic_consumer(
        tide,
        pattern="*alpha*",
        minimum=2,
        returned=2,
    )

    assert len(calls) == 1

    trace = tide.trace()

    consumer = next(
        record
        for record in trace.exec_log
        if record.func_name == "consume_dynamic"
    )

    assert len(consumer.deps) == 1

    dynamic = consumer.deps[0]

    assert dynamic.type == "dynamic"
    assert len(dynamic.expanded_to) == 2

    expanded_refs = {
        expanded.unscoped_reference
        for expanded in dynamic.expanded_to
    }

    assert expanded_refs == {
        "global:@alpha",
        "global:@alpha_one",
    }

    for expanded in dynamic.expanded_to:
        assert expanded.type == "static"
        assert expanded.resolved_version == 0
        assert expanded.producer_exec_id is not None

        producer = trace.index.exec_by_exec_id[expanded.producer_exec_id]

        assert any(
            write.stored_target == expanded.unscoped_reference
            and write.stored_version == expanded.resolved_version
            for write in producer.writes
        )

    consumer_id = consumer.exec_id

    reverse_edges = trace.index.consumer_exec_id_to_producer_input_edges[consumer_id]

    assert len(reverse_edges) == 2
    assert {
        edge.produced_ref
        for edge in reverse_edges
    } == expanded_refs

    for edge in reverse_edges:
        assert edge.consumer_exec_id == consumer_id
        assert edge.produced_ver == 0

        forward_edges = trace.index.producer_exec_id_to_consumer_input_edges[
            edge.producer_exec_id
        ]

        assert any(
            forward_edge is edge
            for forward_edge in forward_edges
        )

        assert consumer_id in trace.index.ref_version_to_consumer_exec_ids[
            (edge.produced_ref, edge.produced_ver)
        ]


def test_to_model_exports_every_dynamic_expansion_and_round_trips_them():
    trace = create_dynamic_index_trace([
        "global:@alpha",
        "global:@alpha_one",
    ])

    model = trace.to_model(
        include_hashes=False,
        incidence_format="compact",
    )

    output_state_by_exec = {
        edge["e"]: edge["s"]
        for edge in model["O"]
    }
    consumer_exec_id = trace.exec_log[-1].exec_id
    expected_inputs = {
        (consumer_exec_id, output_state_by_exec[trace.exec_log[0].exec_id]),
        (consumer_exec_id, output_state_by_exec[trace.exec_log[1].exec_id]),
    }

    assert {
        (edge["e"], edge["s"])
        for edge in model["I"]
    } == expected_inputs

    restored = TIDE.from_model(model)
    restored_consumer = next(
        record
        for record in restored.exec_log
        if record.exec_id == consumer_exec_id
    )

    assert len(restored_consumer.deps) == 2


# ================================================================
# DYNAMIC RUNTIME EXPANDED_TO SEMANTICS
# ================================================================
#
# These tests use the runtime fixture and wildcard helpers defined above.
# ================================================================


@pytest.fixture
def dynamic_runtime_expanded_trace():
    tide = TIDE()

    seed_dynamic_store(
        tide,
        values={
            "@alpha": "!alpha",
            "@alpha_one": "!alpha_one",
            "@beta_alpha": "!beta_alpha",
            "@beta": "!beta",
        },
    )

    calls = run_dynamic_consumer(
        tide,
        pattern="*alpha*",
        minimum=2,
        returned=2,
    )

    assert len(calls) == 1

    return tide.trace()


def dynamic_runtime_consumer(trace):
    return next(
        record
        for record in trace.exec_log
        if record.func_name == "consume_dynamic"
    )


def dynamic_runtime_read(trace):
    consumer = dynamic_runtime_consumer(trace)

    assert len(consumer.deps) == 1

    dynamic = consumer.deps[0]

    assert dynamic.type == "dynamic"

    return dynamic


def dynamic_runtime_expanded_records(trace):
    dynamic = dynamic_runtime_read(trace)

    assert dynamic.expanded_to

    return dynamic.expanded_to


def expanded_from_of(expanded_record):
    if hasattr(expanded_record, "expanded_from"):
        return expanded_record.expanded_from

    raise AssertionError("ExpandedDepRecord has no expanded_from/to_link__t backlink")


def test_dynamic_runtime_read_has_expanded_to_records(dynamic_runtime_expanded_trace):
    dynamic = dynamic_runtime_read(dynamic_runtime_expanded_trace)

    assert len(dynamic.expanded_to) == 2
    assert {
        expanded.unscoped_reference
        for expanded in dynamic.expanded_to
    } == {
        "global:@alpha",
        "global:@alpha_one",
    }


def test_dynamic_runtime_expanded_records_keep_concrete_dep_separate(dynamic_runtime_expanded_trace):
    dynamic = dynamic_runtime_read(dynamic_runtime_expanded_trace)

    for expanded in dynamic.expanded_to:
        assert expanded.type == "static"
        assert expanded.unscoped_reference != dynamic.unscoped_reference

        # NOTE:
        # Runtime expanded records may store dep as a DataRef or leave dep as
        # None while preserving the concrete ref through unscoped_reference.
        # The important runtime contract is that the concrete consumed ref is
        # not represented as the dynamic pattern itself.
        if expanded.dep is not None and dynamic.dep is not None:
            assert expanded.dep != dynamic.dep


def test_dynamic_runtime_expanded_records_preserve_concrete_ref_version_and_producer(dynamic_runtime_expanded_trace):
    trace = dynamic_runtime_expanded_trace
    expanded_records = dynamic_runtime_expanded_records(trace)

    assert [
        (
            expanded.unscoped_reference,
            expanded.resolved_version,
            expanded.type,
        )
        for expanded in expanded_records
    ] == [
        ("global:@alpha", 0, "static"),
        ("global:@alpha_one", 0, "static"),
    ]

    for expanded in expanded_records:
        assert expanded.producer_exec_id is not None

        producer = trace.index.exec_by_exec_id[expanded.producer_exec_id]

        assert any(
            write.stored_target == expanded.unscoped_reference
            and write.stored_version == expanded.resolved_version
            for write in producer.writes
        )


def test_dynamic_runtime_expanded_records_are_indexed_as_exact_consumed_versions(dynamic_runtime_expanded_trace):
    trace = dynamic_runtime_expanded_trace
    consumer = dynamic_runtime_consumer(trace)

    for expanded in dynamic_runtime_expanded_records(trace):
        consumers = trace.index.ref_version_to_consumer_exec_ids[
            (
                expanded.unscoped_reference,
                expanded.resolved_version,
            )
        ]

        assert consumer.exec_id in consumers


def test_dynamic_runtime_expanded_records_match_reverse_edges(dynamic_runtime_expanded_trace):
    trace = dynamic_runtime_expanded_trace
    consumer = dynamic_runtime_consumer(trace)

    reverse_edges = trace.index.consumer_exec_id_to_producer_input_edges[
        consumer.exec_id
    ]

    assert {
        (
            edge.produced_ref,
            edge.produced_ver,
        )
        for edge in reverse_edges
    } == {
        (
            expanded.unscoped_reference,
            expanded.resolved_version,
        )
        for expanded in dynamic_runtime_expanded_records(trace)
    }


def test_dynamic_runtime_concrete_expanded_refs_path_to_consumer(dynamic_runtime_expanded_trace):
    trace = dynamic_runtime_expanded_trace
    consumer_actor = trace.inspect("consume_dynamic", "actor", grouped=False)

    for expanded in dynamic_runtime_expanded_records(trace):
        expanded_ref = trace.inspect(
            expanded.unscoped_reference,
            "reference",
            version=expanded.resolved_version,
        )

        assert trace.path(expanded_ref, consumer_actor).exists()



def test_dynamic_runtime_expanded_records_link_back_to_declared_dynamic_dep(dynamic_runtime_expanded_trace):
    dynamic = dynamic_runtime_read(dynamic_runtime_expanded_trace)

    for expanded in dynamic.expanded_to:
        assert expanded_from_of(expanded) == dynamic.dep
