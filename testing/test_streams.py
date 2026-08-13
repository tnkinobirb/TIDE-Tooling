from tide_tooling import TIDE, Trace
from tide_tooling.tooling_core.engine.events import Effects
from tide_tooling.tooling_core.engine.tide_logging import Actor, AffectedStackLink, DependencyUseRecord, EffectRecord, ExecRecord, ExpandedDepRecord, InputUseRecord, WriteRecord


def create_stream_fixture():
    tide = TIDE()

    tide.event("A", writes={"@A": "!A"})
    tide.event("B", reads=["@A"], writes={"@B": "!B"})
    tide.event("C", reads=["@B"], writes={"@C": "!C"})
    tide.event("X", reads=["@A", "@C"], writes={"@X": "!X", "@A": "!A"})
    tide.event("A", writes={"@A1": "!A1"})
    tide.event("Y", reads=["@X"])

    return tide.trace()


def exec_id(trace: Trace, seq_id: int) -> str:
    return trace.exec_log[seq_id].exec_id


def edge_tuple(edge) -> tuple:
    return edge.producer_exec_id, edge.consumer_exec_id, edge.produced_ref, edge.produced_ver


def root_edges(stream_object, root_exec_id: str) -> list:
    return list(stream_object.stream.get(root_exec_id, {}).get("root", []))


def make_exec(seq_id: int, name: str, deps=None, writes=None, effects=None, inputs=None) -> ExecRecord:
    return ExecRecord(
        seq_id=seq_id,
        exec_id=f"{seq_id:032x}",
        ocean_id=0,
        runner_id="tests",
        pipeline_id="tests",
        node_id=name,
        func=lambda: None,
        func_name=name,
        inputs=inputs or {},
        deps=deps or [],
        effects=effects or {},
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


def create_dynamic_trace():
    producer = make_exec(0, "A", writes=[WriteRecord("test", "!A", 0, "global:@A")])
    associated_input = ("payload", object())
    expanded = ExpandedDepRecord(associated_input, None, "static", "global:@A", "!A", 0, "tests", producer.producer_id, producer.exec_id, [])
    dynamic = DependencyUseRecord(associated_input, None, "dynamic", "global:@*", expanded_to=[expanded])
    consumer = make_exec(1, "D", deps=[dynamic])

    return Trace(event_log=None, exec_log=[producer, consumer], runner="tests", ocean_id=0)


def create_multi_store_trace():
    global_writer = make_exec(0, "GlobalWriter", writes=[WriteRecord("test", "!GLOBAL", 0, "global:@A")])
    mem_writer = make_exec(1, "MemWriter", writes=[WriteRecord("test", "!MEM", 0, "mem:@A")])

    global_dep = DependencyUseRecord(("global_input", object()), None, "static", "global:@A", "!GLOBAL", 0, "tests", global_writer.producer_id, global_writer.exec_id)
    mem_dep = DependencyUseRecord(("mem_input", object()), None, "static", "mem:@A", "!MEM", 0, "tests", mem_writer.producer_id, mem_writer.exec_id)
    global_consumer = make_exec(2, "GlobalConsumer", deps=[global_dep])
    mem_consumer = make_exec(3, "MemConsumer", deps=[mem_dep])

    return Trace(event_log=None, exec_log=[global_writer, mem_writer, global_consumer, mem_consumer], runner="tests", ocean_id=0)


def create_effect_trace():
    writer = make_exec(0, "Writer", writes=[WriteRecord("test", "!A", 0, "global:@A")])
    input_use = InputUseRecord("payload", None, None, None, None, [])
    dep = DependencyUseRecord(("payload", input_use), None, "static", "global:@A", "!A", 0, "tests", writer.producer_id, writer.exec_id)
    input_use.dependencies = [dep]
    consumer = make_exec(1, "Consumer", deps=[dep], inputs={"payload": input_use})

    effect = EffectRecord(Effects.WRITE, ("global:@A", 0), preserved_output="!A")
    effect_producer = make_exec(2, "EffectProducer", effects={(Effects.WRITE, ("global:@A", 0)): effect})

    return Trace(event_log=None, exec_log=[writer, consumer, effect_producer], runner="tests", ocean_id=0)


def test_stream_edges_preserve_affected_input():
    trace = create_multi_store_trace()
    global_edge = root_edges(trace.downstream(trace.inspect(0, "seq_id")), exec_id(trace, 0))[0]
    mem_edge = root_edges(trace.downstream(trace.inspect(1, "seq_id")), exec_id(trace, 1))[0]

    assert edge_tuple(global_edge) == (exec_id(trace, 0), exec_id(trace, 2), "global:@A", 0)
    assert edge_tuple(mem_edge) == (exec_id(trace, 1), exec_id(trace, 3), "mem:@A", 0)
    assert global_edge.consumer_input[0] == "global_input"
    assert mem_edge.consumer_input[0] == "mem_input"


def test_reference_version_downstream_does_not_follow_sibling_outputs():
    trace = create_stream_fixture()
    inspected = trace.inspect("@A", "reference", version=1, grouped=False)
    stream = trace.downstream(inspected)

    assert list(trace.index.ref_version_to_consumer_exec_ids[("global:@A", 1)]) == []
    assert dict(stream.stream) == {}
    assert dict(stream.paths) == {}
    assert stream.count() == 0


def test_reference_version_target_refs_are_exact():
    trace = create_stream_fixture()
    inspected = trace.inspect("@A", "reference", version=1, grouped=False)

    assert [(read.unscoped_reference, read.resolved_version) for read in inspected.reads] == []
    assert [(write.stored_target, write.stored_version) for write in inspected.writes] == [("global:@A", 1)]
    assert trace._get_target_refs(inspected, "downstream") == []
    assert trace._get_target_refs(inspected, "upstream") == [("global:@A", 1)]


def test_stream_to_dict_preserves_roots_edges_and_direction():
    trace = create_stream_fixture()
    stream = trace.downstream(
        trace.inspect("A", "actor", grouped=False),
        depth=0,
    )
    data = stream.to_dict()

    assert data["direction"] == "downstream"
    assert data["roots"] == [exec_id(trace, 0)]
    assert set(data["next"]) == {exec_id(trace, 0)}
    assert {entry["exec"] for entry in data["next"][exec_id(trace, 0)]} == {exec_id(trace, 1), exec_id(trace, 3)}
    assert {(edge["producer_exec"], edge["consumer_exec"], edge["ref"], edge["version"]) for edge in data["edges"].values()} == {
        (exec_id(trace, 0), exec_id(trace, 1), "global:@A", 0),
        (exec_id(trace, 0), exec_id(trace, 3), "global:@A", 0),
    }
    assert all(edge["affected_input"] for edge in data["edges"].values())


def test_stream_show_keeps_reference_roots_separate(capsys):
    trace = create_stream_fixture()
    stream = trace.downstream(trace.inspect("@A", "reference", grouped=False))
    shown = stream.show()
    printed = capsys.readouterr().out

    assert "EXEC_RECORD[0] A" in shown
    assert "EXEC_RECORD[4] A" not in shown
    assert shown in printed


def test_stream_show_uses_shared_header_and_returns_printed_output(capsys):
    trace = create_stream_fixture()
    stream = trace.downstream(trace.inspect("@A", "reference", grouped=False))

    shown = stream.show()
    printed = capsys.readouterr().out

    assert "DOWNSTREAM REFERENCE[global:@A]" in shown
    assert "PATHS:" in shown
    assert shown in printed


def test_stream_show_renders_complete_numbered_tide_chains(capsys):
    trace = create_stream_fixture()

    shown = trace.downstream(
        trace.inspect("@A", "reference", grouped=False)
    ).show(color=False)
    capsys.readouterr()

    assert "PATH 1" in shown
    assert "PATH 2" in shown
    assert "At 0 to 1" not in shown
    assert (
        "EXEC_RECORD[0] A\n"
        "└── global:@A[v0]\n"
        "    └── EXEC_RECORD[1] B\n"
        "        └── global:@B[v0]\n"
        "            └── EXEC_RECORD[2] C"
    ) in shown


def test_stream_show_reverses_each_complete_chain_upstream(capsys):
    trace = create_stream_fixture()

    shown = trace.upstream(
        trace.inspect("Y", "actor", grouped=False)
    ).show(color=False)
    capsys.readouterr()

    assert (
        "EXEC_RECORD[5] Y\n"
        "└── global:@X[v0]\n"
        "    └── EXEC_RECORD[3] X\n"
        "        └── global:@A[v0]\n"
        "            └── EXEC_RECORD[0] A"
    ) in shown


def test_stream_show_distinguishes_repeated_actor_occurrences(capsys):
    tide = TIDE()
    tide.event("A", writes={"@seed": 1})
    tide.event("A", reads=["@seed"], writes={"@next": 2})
    tide.event("finish", reads=["@next"])
    trace = tide.trace()

    shown = trace.downstream(
        trace.inspect("A", "actor", grouped=True)
    ).show(color=False)
    capsys.readouterr()

    assert "EXEC_RECORD[0] A" in shown
    assert "EXEC_RECORD[1] A" in shown


def test_grouped_stream_show_uses_kind_and_grouped_identity(capsys):
    trace = create_stream_fixture()
    stream = trace.downstream(trace.inspect("A", "actor", grouped=True))

    shown = stream.show()
    capsys.readouterr()

    assert "DOWNSTREAM ACTOR GROUPED[A]" in shown


def test_stream_show_reports_empty_stream_without_fake_edges(capsys):
    trace = create_stream_fixture()
    stream = trace.downstream(trace.inspect("global:@missing", "reference", grouped=False))

    shown = stream.show()
    capsys.readouterr()

    assert "no paths found" in shown.lower()
    assert "global:@A" not in shown


def test_stream_queries_preserve_grouped_false_for_raw_actor_identifiers():
    trace = create_stream_fixture()

    downstream = trace.downstream(
        "A",
        type="actor",
        grouped=False,
    )
    upstream = trace.upstream(
        "X",
        type="actor",
        grouped=False,
    )

    assert downstream.inspected.grouped is False
    assert upstream.inspected.grouped is False


def test_execs_from_stream_uses_directional_related_execs():
    trace = create_stream_fixture()
    downstream = trace.downstream(
        trace.inspect("A", "actor", grouped=False),
        depth=0,
    )
    upstream = trace.upstream(
        trace.inspect("X", "actor", grouped=False),
        depth=0,
    )

    assert set(downstream.execs_from_stream(downstream.stream, "downstream")) == {exec_id(trace, 1), exec_id(trace, 3)}
    assert set(upstream.execs_from_stream(upstream.stream, "upstream")) == {exec_id(trace, 0), exec_id(trace, 2)}


def test_paths_remain_isolated_by_root():
    trace = create_stream_fixture()
    stream = trace.downstream(
        trace.inspect("@A", "reference", grouped=False),
        depth=0,
    )

    assert set(stream.paths) == {exec_id(trace, 0)}
    assert all(len(path) == 1 for paths in stream.paths.values() for path in paths)
    assert {edge_tuple(path[0]) for path in stream.paths[exec_id(trace, 0)]} == {
        (exec_id(trace, 0), exec_id(trace, 1), "global:@A", 0),
        (exec_id(trace, 0), exec_id(trace, 3), "global:@A", 0),
    }


def test_downstream_depth_builds_deeper_layers():
    trace = create_stream_fixture()
    stream = trace.downstream(trace.inspect("A", "actor", grouped=False), depth=3)
    root = exec_id(trace, 0)

    assert {edge_tuple(edge) for edge in stream.stream[root]["root"]} == {
        (exec_id(trace, 0), exec_id(trace, 1), "global:@A", 0),
        (exec_id(trace, 0), exec_id(trace, 3), "global:@A", 0),
    }
    assert {edge_tuple(edge) for edge in stream.stream[root][0]} == {
        (exec_id(trace, 1), exec_id(trace, 2), "global:@B", 0),
        (exec_id(trace, 3), exec_id(trace, 5), "global:@X", 0),
    }
    assert {edge_tuple(edge) for edge in stream.stream[root][1]} == {
        (exec_id(trace, 2), exec_id(trace, 3), "global:@C", 0),
    }
    assert 2 not in stream.stream[root]
    assert set(stream.stream[root]) == {"root", 0, 1}


def test_downstream_depth_preserves_complete_branches():
    trace = create_stream_fixture()
    stream = trace.downstream(trace.inspect("A", "actor", grouped=False), depth=3)
    root = exec_id(trace, 0)

    actual = {
        tuple(edge_tuple(edge) for edge in path)
        for path in stream.paths[root]
    }

    expected = {
        (
            (exec_id(trace, 0), exec_id(trace, 3), "global:@A", 0),
            (exec_id(trace, 3), exec_id(trace, 5), "global:@X", 0),
        ),
        (
            (exec_id(trace, 0), exec_id(trace, 1), "global:@A", 0),
            (exec_id(trace, 1), exec_id(trace, 2), "global:@B", 0),
            (exec_id(trace, 2), exec_id(trace, 3), "global:@C", 0),
            (exec_id(trace, 3), exec_id(trace, 5), "global:@X", 0),
        ),
    }

    assert actual == expected


def test_downstream_depth_stops_at_requested_layer():
    trace = create_stream_fixture()
    stream = trace.downstream(trace.inspect("A", "actor", grouped=False), depth=1)
    root = exec_id(trace, 0)

    assert set(stream.stream[root]) == {"root", 0}
    assert {edge_tuple(edge) for edge in stream.stream[root][0]} == {
        (exec_id(trace, 1), exec_id(trace, 2), "global:@B", 0),
        (exec_id(trace, 3), exec_id(trace, 5), "global:@X", 0),
    }


def test_dynamic_dependency_indexes_concrete_edge():
    trace = create_dynamic_trace()
    stream = trace.upstream(trace.inspect(1, "seq_id"))
    edges = root_edges(stream, exec_id(trace, 1))

    assert len(edges) == 1
    edge = edges[0]
    assert edge_tuple(edge) == (exec_id(trace, 0), exec_id(trace, 1), "global:@A", 0)
    assert edge.consumer_input[0] == "payload"
    assert trace.index.ref_version_to_producer_exec_id[("global:@A", 0)] == exec_id(trace, 0)
    assert list(trace.index.ref_version_to_consumer_exec_ids[("global:@A", 0)]) == [exec_id(trace, 1)]


def test_effect_creates_affected_stack_link_with_affected_input():
    trace = create_effect_trace()
    stream = trace.downstream(trace.inspect(2, "seq_id"))
    edges = root_edges(stream, exec_id(trace, 2))

    assert len(edges) == 1
    edge = edges[0]
    assert isinstance(edge, AffectedStackLink)
    assert edge_tuple(edge) == (exec_id(trace, 2), exec_id(trace, 1), "global:@A", 0)
    assert [name for name, _ in edge.consumer_inputs] == ["payload"]
    assert edge.cause_effect == Effects.WRITE


def test_same_key_in_different_stores_stays_separate():
    trace = create_multi_store_trace()
    global_stream = trace.downstream(trace.inspect("global:@A", "reference", version=0, grouped=False))
    mem_stream = trace.downstream(trace.inspect("mem:@A", "reference", version=0, grouped=False))

    global_edges = {edge_tuple(edge) for edge in root_edges(global_stream, exec_id(trace, 0))}
    mem_edges = {edge_tuple(edge) for edge in root_edges(mem_stream, exec_id(trace, 1))}

    assert trace.index.ref_version_to_producer_exec_id[("global:@A", 0)] == exec_id(trace, 0)
    assert trace.index.ref_version_to_producer_exec_id[("mem:@A", 0)] == exec_id(trace, 1)
    assert global_edges == {(exec_id(trace, 0), exec_id(trace, 2), "global:@A", 0)}
    assert mem_edges == {(exec_id(trace, 1), exec_id(trace, 3), "mem:@A", 0)}
    assert not root_edges(global_stream, exec_id(trace, 1)) 
    assert not root_edges(mem_stream, exec_id(trace, 0))
    assert global_edges.isdisjoint(mem_edges)


def test_actor_equality_uses_other_actor_id():
    assert Actor(0, "runner", "A") == Actor(0, "runner", "A")
    assert Actor(0, "runner", "A") != Actor(0, "runner", "B")
    assert Actor(0, "runner-a", "A") != Actor(0, "runner-b", "A")
    assert Actor(0, "runner", "A") != object()
