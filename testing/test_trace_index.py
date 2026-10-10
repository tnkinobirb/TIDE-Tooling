from tide_tooling import TIDE, Trace
from tide_tooling.tooling_core.engine.events import Effects
from tide_tooling.tooling_core.engine.tide_logging import (
    AffectedStackLink,
    DependencyUseRecord,
    EffectRecord,
    EmittedNodeRecord,
    ExecRecord,
    ExpandedDepRecord,
    InputUseRecord,
    WriteRecord,
)


def create_trace_index_fixture() -> Trace:
    tide = TIDE()

    tide.event("A", writes={"@A": "!A"})
    tide.event("B", reads=["@A"], writes={"@B": "!B"})
    tide.event("C", reads=["@B"], writes={"@C": "!C"})
    tide.event("X", reads=["@A", "@C"], writes={"@X": "!X", "@A": "!A"})
    tide.event("A", writes={"@A1": "!A1"})
    tide.event("Y", reads=["@X"])

    return tide.trace()


def exec_id(trace: Trace, seq_id: int) -> str:
    return trace.inspect(seq_id, "seq_id").id


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
        effects=effects or [],
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


def test_emission_path_exists_without_an_emitted_execution():
    emitted = EmittedNodeRecord(
        ocean_id=0,
        runner_id="tests",
        pipeline_id="tests",
        node_id="child-node",
        func=lambda: None,
        func_name="Child",
        inputs={},
        deps=[],
        declared_targets=[],
        declared_effects=[],
        origin="runtime",
        content_id=b"child-content",
        base_node="child-base",
        base_node_content_id=b"child-base-content",
    )
    emitter = make_exec(0, "Emitter")
    emitter.emitted_nodes = [emitted]

    trace = Trace(
        event_log=None,
        exec_log=[emitter],
        runner="tests",
        ocean_id=0,
    )

    assert trace.inspect(
        emitted.producer_id,
        "actor",
        grouped=False,
    ).exists
    path = trace.path(
        emitter.producer_id,
        emitted.producer_id,
        "actor",
        "actor",
        group=False,
    )

    assert path.exists()
    rendered = path.show(color=False, explore_tool_tips=False)
    assert "Child" in rendered
    assert "emitted by" in rendered


def create_dynamic_trace() -> Trace:
    producer = make_exec(
        0,
        "A",
        writes=[WriteRecord("test", "!A", 0, "global:@A")],
    )

    associated_input = ("payload", object())

    expanded = ExpandedDepRecord(
        associated_input,
        None,
        "static",
        "global:@A",
        "!A",
        0,
        "tests",
        producer.producer_id,
        producer.exec_id,
        [],
    )

    dynamic = DependencyUseRecord(
        associated_input,
        None,
        "dynamic",
        "global:@*",
        expanded_to=[expanded],
    )

    consumer = make_exec(1, "D", deps=[dynamic])

    return Trace(
        event_log=None,
        exec_log=[producer, consumer],
        runner="tests",
        ocean_id=0,
    )


def create_multi_store_trace() -> Trace:
    global_writer = make_exec(
        0,
        "GlobalWriter",
        writes=[WriteRecord("test", "!GLOBAL", 0, "global:@A")],
    )

    mem_writer = make_exec(
        1,
        "MemWriter",
        writes=[WriteRecord("test", "!MEM", 0, "mem:@A")],
    )

    global_dep = DependencyUseRecord(
        ("global_input", object()),
        None,
        "static",
        "global:@A",
        "!GLOBAL",
        0,
        "tests",
        global_writer.producer_id,
        global_writer.exec_id,
    )

    mem_dep = DependencyUseRecord(
        ("mem_input", object()),
        None,
        "static",
        "mem:@A",
        "!MEM",
        0,
        "tests",
        mem_writer.producer_id,
        mem_writer.exec_id,
    )

    global_consumer = make_exec(2, "GlobalConsumer", deps=[global_dep])
    mem_consumer = make_exec(3, "MemConsumer", deps=[mem_dep])

    return Trace(
        event_log=None,
        exec_log=[
            global_writer,
            mem_writer,
            global_consumer,
            mem_consumer,
        ],
        runner="tests",
        ocean_id=0,
    )


def create_effect_trace() -> Trace:
    writer = make_exec(
        0,
        "Writer",
        writes=[WriteRecord("test", "!A", 0, "global:@A")],
    )

    input_use = InputUseRecord(
        "payload",
        None,
        None,
        None,
        None,
        [],
    )

    dep = DependencyUseRecord(
        ("payload", input_use),
        None,
        "static",
        "global:@A",
        "!A",
        0,
        "tests",
        writer.producer_id,
        writer.exec_id,
    )

    input_use.dependencies = [dep]

    consumer = make_exec(
        1,
        "Consumer",
        deps=[dep],
        inputs={"payload": input_use},
    )

    effect = EffectRecord(
        Effects.WRITE,
        ("global:@A", 0),
        origin="declared",
        output="!A",
    )

    effect_producer = make_exec(
        2,
        "EffectProducer",
        effects=[effect],
    )

    return Trace(
        event_log=None,
        exec_log=[writer, consumer, effect_producer],
        runner="tests",
        ocean_id=0,
    )


def test_trace_index_empty_build():
    trace = Trace(
        event_log=None,
        exec_log=[],
        runner="tests",
        ocean_id=77,
    )

    index = trace.index

    assert index.source_ocean_id == 77
    assert index.exec_by_exec_id == {}
    assert list(index.seen_refs) == []
    assert dict(index.ref_version_to_producer_exec_id) == {}
    assert dict(index.value_hash_by_ref_version) == {}


def test_trace_index_exec_actor_and_sequence_truth():
    trace = create_trace_index_fixture()
    index = trace.index

    assert len(index.exec_by_exec_id) == 6

    for seq_id, log in enumerate(trace.exec_log):
        assert log.seq_id == seq_id
        assert index.exec_by_exec_id[log.exec_id] is log
        assert log.exec_id in index.actor_to_exec_ids[log.producer_id]
        assert log.producer_id in index.func_name_to_actors[log.producer_id.source_actor]


def test_trace_index_write_truth_is_consistent_across_indexes():
    trace = create_trace_index_fixture()
    index = trace.index

    writer_id = exec_id(trace, 0)
    ref_version = ("global:@A", 0)
    write = index.ref_ver_to_write_egde[ref_version]
    value_hash = index.value_hash_by_ref_version[ref_version]

    assert write in index.writes_by_exec_id[writer_id]
    assert writer_id in index.ref_to_writer_exec_ids["global:@A"]
    assert index.ref_version_to_producer_exec_id[ref_version] == writer_id
    assert 0 in index.versions_by_ref["global:@A"]
    assert "global:@A" in index.seen_refs

    assert index.value_by_hash[value_hash] == "!A"
    assert index.value_repr_by_hash[value_hash] == repr("!A")
    assert ref_version in index.value_hash_to_versioned_refs[value_hash]
    assert writer_id in index.value_hash_to_producer_exec_ids[value_hash]


def test_trace_index_read_truth_is_consistent_across_indexes():
    trace = create_trace_index_fixture()
    index = trace.index

    producer_id = exec_id(trace, 0)
    consumer_id = exec_id(trace, 1)
    ref_version = ("global:@A", 0)
    value_hash = index.value_hash_by_ref_version[ref_version]

    read_records = list(index.ref_ver_to_read_egdes[ref_version])
    parent_edges = list(index.consumer_exec_id_to_producer_input_edges[consumer_id])
    child_edges = list(index.producer_exec_id_to_consumer_input_edges[producer_id])

    assert len(read_records) == 2
    assert consumer_id in index.ref_to_consumer_exec_ids["global:@A"]
    assert consumer_id in index.ref_version_to_consumer_exec_ids[ref_version]
    assert consumer_id in index.value_hash_to_consumer_exec_ids[value_hash]

    matching_parent_edges = [
        edge
        for edge in parent_edges
        if edge.producer_exec_id == producer_id
        and edge.consumer_exec_id == consumer_id
        and edge.produced_ref == "global:@A"
        and edge.produced_ver == 0
    ]

    assert len(matching_parent_edges) == 1
    assert matching_parent_edges[0] in child_edges
    assert matching_parent_edges[0].consumer_input[0] == "args"


def test_trace_index_same_ref_versions_remain_separate():
    trace = create_trace_index_fixture()
    index = trace.index

    version_zero = ("global:@A", 0)
    version_one = ("global:@A", 1)

    assert set(index.versions_by_ref["global:@A"]) == {0, 1}
    assert index.ref_version_to_producer_exec_id[version_zero] == exec_id(trace, 0)
    assert index.ref_version_to_producer_exec_id[version_one] == exec_id(trace, 3)

    assert set(index.ref_version_to_consumer_exec_ids[version_zero]) == {
        exec_id(trace, 1),
        exec_id(trace, 3),
    }
    assert list(index.ref_version_to_consumer_exec_ids[version_one]) == []


def test_trace_index_same_value_keeps_distinct_versioned_refs():
    tide = TIDE()

    tide.event("A", writes={"@A": "!SAME"})
    tide.event("B", writes={"@B": "!SAME"})
    tide.event("C", writes={"@A": "!SAME"})

    trace = tide.trace()
    index = trace.index

    refs = {
        ("global:@A", 0),
        ("global:@B", 0),
        ("global:@A", 1),
    }

    hashes = {
        index.value_hash_by_ref_version[ref_version]
        for ref_version in refs
    }

    assert len(hashes) == 1

    value_hash = hashes.pop()

    assert set(index.value_hash_to_versioned_refs[value_hash]) == refs
    assert set(index.value_hash_to_producer_exec_ids[value_hash]) == {
        exec_id(trace, 0),
        exec_id(trace, 1),
        exec_id(trace, 2),
    }


def test_trace_index_dynamic_dependency_indexes_expanded_fact():
    trace = create_dynamic_trace()
    index = trace.index

    producer_id = exec_id(trace, 0)
    consumer_id = exec_id(trace, 1)
    ref_version = ("global:@A", 0)

    assert index.ref_version_to_producer_exec_id[ref_version] == producer_id
    assert list(index.ref_version_to_consumer_exec_ids[ref_version]) == [consumer_id]
    assert len(index.reads_by_exec_id[consumer_id]) == 1
    assert index.reads_by_exec_id[consumer_id][0].type == "dynamic"
    assert len(index.ref_ver_to_read_egdes[ref_version]) == 1

    edges = list(index.consumer_exec_id_to_producer_input_edges[consumer_id])

    assert len(edges) == 1
    assert edges[0].producer_exec_id == producer_id
    assert edges[0].consumer_exec_id == consumer_id
    assert edges[0].produced_ref == "global:@A"
    assert edges[0].produced_ver == 0
    assert edges[0].consumer_input[0] == "payload"


def test_trace_index_store_scope_is_part_of_reference_identity():
    trace = create_multi_store_trace()
    index = trace.index

    global_ref = ("global:@A", 0)
    mem_ref = ("mem:@A", 0)

    assert index.ref_version_to_producer_exec_id[global_ref] == exec_id(trace, 0)
    assert index.ref_version_to_producer_exec_id[mem_ref] == exec_id(trace, 1)

    assert list(index.ref_version_to_consumer_exec_ids[global_ref]) == [exec_id(trace, 2)]
    assert list(index.ref_version_to_consumer_exec_ids[mem_ref]) == [exec_id(trace, 3)]

    assert index.value_hash_by_ref_version[global_ref] != index.value_hash_by_ref_version[mem_ref]


def test_trace_index_effect_truth_and_affected_link_are_consistent():
    trace = create_effect_trace()
    index = trace.index

    effect_exec_id = exec_id(trace, 2)
    affected_exec_id = exec_id(trace, 1)
    effect_key = (Effects.WRITE, "global:@A")
    ref_version = ("global:@A", 0)

    effect_records = list(index.effects_by_exec_id[effect_exec_id])
    affected_edges = [
        edge
        for edge in index.producer_exec_id_to_consumer_input_edges[effect_exec_id]
        if isinstance(edge, AffectedStackLink)
    ]

    assert len(effect_records) == 1
    assert effect_records[0] in index.ref_version_to_effects[ref_version]
    assert effect_exec_id in index.effect_to_producer_exec_ids[effect_key]
    assert effect_exec_id in index.effect_type_to_producer_exec_ids[Effects.WRITE]
    assert affected_exec_id in index.effect_to_touched_exec_ids[effect_key]

    assert len(affected_edges) == 1

    edge = affected_edges[0]

    assert edge.producer_exec_id == effect_exec_id
    assert edge.consumer_exec_id == affected_exec_id
    assert edge.produced_ref == "global:@A"
    assert edge.produced_ver == 0
    assert edge.cause_effect == Effects.WRITE
    assert edge.consumer_inputs == [{"name": "payload", "index": None}]
    assert edge in index.consumer_exec_id_to_producer_input_edges[affected_exec_id]


def test_trace_index_integrated_fixture_is_internally_coherent():
    trace = create_trace_index_fixture()
    index = trace.index

    expected_refs = {
        "global:@A",
        "global:@A1",
        "global:@B",
        "global:@C",
        "global:@X",
    }

    expected_ref_versions = {
        ("global:@A", 0),
        ("global:@A", 1),
        ("global:@A1", 0),
        ("global:@B", 0),
        ("global:@C", 0),
        ("global:@X", 0),
    }

    assert set(index.seen_refs) == expected_refs
    assert set(index.value_hash_by_ref_version) == expected_ref_versions
    assert set(index.ref_version_to_producer_exec_id) == expected_ref_versions

    for ref_version, producer_id in index.ref_version_to_producer_exec_id.items():
        assert producer_id in index.exec_by_exec_id
        assert ref_version in index.value_hash_by_ref_version

    for ref_version, consumer_ids in index.ref_version_to_consumer_exec_ids.items():
        assert ref_version in index.value_hash_by_ref_version

        for consumer_id in consumer_ids:
            assert consumer_id in index.exec_by_exec_id

    expected_edges = {
        (exec_id(trace, 0), exec_id(trace, 1), "global:@A", 0),
        (exec_id(trace, 0), exec_id(trace, 3), "global:@A", 0),
        (exec_id(trace, 1), exec_id(trace, 2), "global:@B", 0),
        (exec_id(trace, 2), exec_id(trace, 3), "global:@C", 0),
        (exec_id(trace, 3), exec_id(trace, 5), "global:@X", 0),
    }

    actual_edges = {
        (
            edge.producer_exec_id,
            edge.consumer_exec_id,
            edge.produced_ref,
            edge.produced_ver,
        )
        for edges in index.producer_exec_id_to_consumer_input_edges.values()
        for edge in edges
        if not isinstance(edge, AffectedStackLink)
    }

    assert actual_edges == expected_edges


def create_trace_index_exec_input_fixture() -> Trace:
    tide = TIDE()

    def produce_seed():
        return "!seed"

    def consume_all_inputs(*args, **kwargs):
        return None

    tide.exec(produce_seed, writes=["@seed"])

    tide.exec(
        consume_all_inputs,
        args=["@@seed"],
        kwargs={"kwarg_seed": "@@seed"},
        p_args=("@@seed",),
        p_kwargs={"p_kwarg_seed":"@@seed"},
        k_args=("@@seed",),
        k_kwargs={"k_kwarg_seed": "@@seed"},
    )

    return tide.trace()


def exec_id(trace: Trace, seq_id: int) -> str:
    return trace.inspect(seq_id, "seq_id").id


def test_trace_index_exec_preserves_args_input():
    trace = create_trace_index_exec_input_fixture()
    index = trace.index

    producer_id = exec_id(trace, 0)
    consumer_id = exec_id(trace, 1)

    matching_edges = [
        edge
        for edge in index.consumer_exec_id_to_producer_input_edges[consumer_id]
        if edge.producer_exec_id == producer_id
        and edge.produced_ref == "global:@seed"
        and edge.produced_ver == 0
    ]

    input_names = {
        edge.consumer_input[0]
        for edge in matching_edges
    }

    assert "args" in input_names


def test_trace_index_exec_preserves_all_input_categories():
    trace = create_trace_index_exec_input_fixture()
    index = trace.index

    producer_id = exec_id(trace, 0)
    consumer_id = exec_id(trace, 1)

    matching_edges = [
        edge
        for edge in index.consumer_exec_id_to_producer_input_edges[consumer_id]
        if edge.producer_exec_id == producer_id
        and edge.produced_ref == "global:@seed"
        and edge.produced_ver == 0
    ]

    input_names = {
        edge.consumer_input[0]
        for edge in matching_edges
    }

    assert input_names == {
        "args",
        "kwargs",
        "p_args",
        "p_kwargs",
        "k_args",
        "k_kwargs",
    }


def test_trace_index_same_value_across_stores_keeps_distinct_ref_truth():
    global_writer = make_exec(
        0,
        "GlobalWriter",
        writes=[WriteRecord("test", "!SAME", 0, "global:@A")],
    )

    mem_writer = make_exec(
        1,
        "MemWriter",
        writes=[WriteRecord("test", "!SAME", 0, "mem:@A")],
    )

    trace = Trace(
        event_log=None,
        exec_log=[global_writer, mem_writer],
        runner="tests",
        ocean_id=0,
    )

    index = trace.index

    global_ref_version = ("global:@A", 0)
    mem_ref_version = ("mem:@A", 0)

    global_hash = index.value_hash_by_ref_version[global_ref_version]
    mem_hash = index.value_hash_by_ref_version[mem_ref_version]

    assert global_hash == mem_hash

    assert global_ref_version in index.value_hash_to_versioned_refs[global_hash]
    assert mem_ref_version in index.value_hash_to_versioned_refs[global_hash]

    assert index.ref_version_to_producer_exec_id[global_ref_version] == exec_id(trace, 0)
    assert index.ref_version_to_producer_exec_id[mem_ref_version] == exec_id(trace, 1)

    assert set(index.versions_by_ref["global:@A"]) == {0}
    assert set(index.versions_by_ref["mem:@A"]) == {0}


def test_trace_index_same_ref_version_used_by_two_inputs_dedupes_consumer_but_preserves_edges():
    tide = TIDE()

    def produce_seed():
        return "!seed"

    def consume_seed(*args, **kwargs):
        return None

    tide.exec(produce_seed, writes=["@seed"])
    tide.exec(
        consume_seed,
        args=["@@seed"],
        kwargs={"seed": "@@seed"},
    )

    trace = tide.trace()
    index = trace.index

    producer_id = exec_id(trace, 0)
    consumer_id = exec_id(trace, 1)
    ref_version = ("global:@seed", 0)

    assert list(index.ref_version_to_consumer_exec_ids[ref_version]) == [consumer_id]

    matching_edges = [
        edge
        for edge in index.consumer_exec_id_to_producer_input_edges[consumer_id]
        if edge.producer_exec_id == producer_id
        and edge.consumer_exec_id == consumer_id
        and edge.produced_ref == "global:@seed"
        and edge.produced_ver == 0
    ]

    assert len(matching_edges) == 2
    assert {edge.consumer_input[0] for edge in matching_edges} == {
        "args",
        "kwargs",
    }


def test_trace_index_one_version_can_have_multiple_consumers():
    tide = TIDE()

    tide.event("A", writes={"@seed": "!seed"})
    tide.event("B", reads=["@seed"])
    tide.event("C", reads=["@seed"])

    trace = tide.trace()
    index = trace.index

    producer_id = exec_id(trace, 0)
    consumer_ids = {
        exec_id(trace, 1),
        exec_id(trace, 2),
    }

    ref_version = ("global:@seed", 0)

    assert set(index.ref_version_to_consumer_exec_ids[ref_version]) == consumer_ids
    assert set(index.ref_to_consumer_exec_ids["global:@seed"]) == consumer_ids

    child_edges = [
        edge
        for edge in index.producer_exec_id_to_consumer_input_edges[producer_id]
        if edge.produced_ref == "global:@seed"
        and edge.produced_ver == 0
    ]

    assert {edge.consumer_exec_id for edge in child_edges} == consumer_ids
    assert len(child_edges) == 2


def test_trace_index_same_actor_can_own_multiple_execs_without_duplicate_actor_registration():
    def same():
        return None

    tide = TIDE()

    tide.event(same)
    tide.event(same)
    tide.event(same)

    trace = tide.trace()
    index = trace.index

    actor = trace.exec_log[0].producer_id
    actor_exec_ids = [
        exec_id(trace, 0),
        exec_id(trace, 1),
        exec_id(trace, 2),
    ]

    assert all(log.producer_id == actor for log in trace.exec_log)
    assert list(index.actor_to_exec_ids[actor]) == actor_exec_ids
    assert list(index.func_name_to_actors[actor.source_actor]) == [actor]


def test_trace_index_forward_and_reverse_edges_are_the_same_objects():
    trace = create_trace_index_fixture()
    index = trace.index

    for producer_id, edges in index.producer_exec_id_to_consumer_input_edges.items():
        for edge in edges:
            assert edge.producer_exec_id == producer_id

            reverse_edges = index.consumer_exec_id_to_producer_input_edges[
                edge.consumer_exec_id
            ]

            assert any(reverse_edge is edge for reverse_edge in reverse_edges)

    for consumer_id, edges in index.consumer_exec_id_to_producer_input_edges.items():
        for edge in edges:
            assert edge.consumer_exec_id == consumer_id

            forward_edges = index.producer_exec_id_to_consumer_input_edges[
                edge.producer_exec_id
            ]

            assert any(forward_edge is edge for forward_edge in forward_edges)


def test_trace_indexes_from_separate_traces_do_not_share_facts():
    tide_a = TIDE()
    
    tide_a.event("A", writes={"@seed": "!A"})
    trace_a = tide_a.trace()

    tide_b = TIDE()
    tide_b.event("B", writes={"@seed": "!B"})
    trace_b = tide_b.trace()

    index_a = trace_a.index
    index_b = trace_b.index

    exec_a = exec_id(trace_a, 0)
    exec_b = exec_id(trace_b, 0)

    actor_a = trace_a.exec_log[0].producer_id
    actor_b = trace_b.exec_log[0].producer_id

    ref_version = ("global:@seed", 0)

    hash_a = index_a.value_hash_by_ref_version[ref_version]
    hash_b = index_b.value_hash_by_ref_version[ref_version]

    assert exec_a in index_a.exec_by_exec_id
    assert exec_b in index_b.exec_by_exec_id

    assert exec_b not in index_a.exec_by_exec_id
    assert exec_a not in index_b.exec_by_exec_id

    assert actor_a in index_a.actor_to_exec_ids
    assert actor_b in index_b.actor_to_exec_ids

    assert actor_b not in index_a.actor_to_exec_ids
    assert actor_a not in index_b.actor_to_exec_ids

    assert index_a.ref_version_to_producer_exec_id[ref_version] == exec_a
    assert index_b.ref_version_to_producer_exec_id[ref_version] == exec_b

    assert index_a.value_by_hash[hash_a] == "!A"
    assert index_b.value_by_hash[hash_b] == "!B"


def assert_trace_index_integrity(index):
    for (ref, ver), producer_id in index.ref_version_to_producer_exec_id.items():
        ref_version = (ref, ver)

        assert producer_id in index.exec_by_exec_id
        assert ref in index.seen_refs
        assert ver in index.versions_by_ref[ref]
        assert ref_version in index.value_hash_by_ref_version

        value_hash = index.value_hash_by_ref_version[ref_version]

        assert ref_version in index.value_hash_to_versioned_refs[value_hash]
        assert producer_id in index.value_hash_to_producer_exec_ids[value_hash]

    for (ref, ver), consumer_ids in index.ref_version_to_consumer_exec_ids.items():
        ref_version = (ref, ver)

        assert ref in index.seen_refs
        assert ref_version in index.value_hash_by_ref_version

        value_hash = index.value_hash_by_ref_version[ref_version]

        for consumer_id in consumer_ids:
            assert consumer_id in index.exec_by_exec_id
            assert consumer_id in index.ref_to_consumer_exec_ids[ref]
            assert consumer_id in index.value_hash_to_consumer_exec_ids[value_hash]

    for consumer_id, edges in index.consumer_exec_id_to_producer_input_edges.items():
        assert consumer_id in index.exec_by_exec_id

        for edge in edges:
            assert edge.consumer_exec_id == consumer_id
            assert edge.producer_exec_id in index.exec_by_exec_id
            assert edge.produced_ref in index.seen_refs

            forward_edges = index.producer_exec_id_to_consumer_input_edges[
                edge.producer_exec_id
            ]

            assert any(forward_edge is edge for forward_edge in forward_edges)

    for producer_id, edges in index.producer_exec_id_to_consumer_input_edges.items():
        assert producer_id in index.exec_by_exec_id

        for edge in edges:
            assert edge.producer_exec_id == producer_id
            assert edge.consumer_exec_id in index.exec_by_exec_id
            assert edge.produced_ref in index.seen_refs

            reverse_edges = index.consumer_exec_id_to_producer_input_edges[
                edge.consumer_exec_id
            ]

            assert any(reverse_edge is edge for reverse_edge in reverse_edges)


def test_trace_index_integrity_invariant_for_integrated_fixture():
    trace = create_trace_index_fixture()

    assert_trace_index_integrity(trace.index)


def test_trace_index_integrity_invariant_for_exec_input_fixture():
    trace = create_trace_index_exec_input_fixture()

    assert_trace_index_integrity(trace.index)
