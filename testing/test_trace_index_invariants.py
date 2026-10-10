"""Cross-index facts must agree on the same recorded occurrences."""

import pytest

from tide_tooling import TIDE, WRITE
from tide_tooling.tooling_core.engine.events import Effects
from tide_tooling.tooling_core.engine.tide_links import EffectEdge, ReadEdge


def _versioned_trace():
    tide = TIDE()
    tide.event("source", writes={"global:x": "first"})
    tide.event("consumer", reads=["global:x"], writes={"global:y": "first"})
    tide.event("source", writes={"global:x": "second"})
    tide.event("consumer", reads=["global:x"])
    return tide.trace()


def test_exact_ref_versions_agree_across_records_values_and_edges():
    trace = _versioned_trace()
    index = trace._index
    first_source, first_consumer, second_source, second_consumer = trace._log

    for source, consumer, version in (
        (first_source, first_consumer, 0),
        (second_source, second_consumer, 1),
    ):
        key = ("global:x", version)
        write = source.writes[0]
        read = consumer.deps[0]

        assert list(index.write_records_by_ref_version[key]) == [write]
        assert list(index.producer_exec_ids_by_ref_version[key]) == [source.exec_id]
        assert list(index.read_records_by_ref_version[key]) == [read]
        assert list(index.consumer_exec_ids_by_ref_version[key]) == [consumer.exec_id]
        assert index.value_by_hash[index.value_hash_by_ref_version[key]] == write.stored_result

        edges = [
            edge for edge in index.producer_edges_by_exec_id[consumer.exec_id]
            if isinstance(edge, ReadEdge) and edge.read is read
        ]
        assert len(edges) == 1
        assert any(
            reverse is edges[0]
            for reverse in index.consumed_edges_by_exec_id[source.exec_id]
        )

    assert list(index.versions_by_ref["global:x"]) == [0, 1]
    assert index.value_hash_by_ref_version[("global:x", 0)] == index.value_hash_by_ref_version[("global:y", 0)]
    assert ("global:x", 0) in index.ref_versions_by_value_hash[index.value_hash_by_ref_version[("global:y", 0)]]
    assert ("global:y", 0) in index.ref_versions_by_value_hash[index.value_hash_by_ref_version[("global:x", 0)]]


def test_execution_actor_and_sequence_indexes_share_the_log_records():
    trace = _versioned_trace()
    index = trace._index

    for execution in trace._log:
        assert index.exec_by_exec_id[execution.exec_id] is execution
        assert execution.exec_id in index.actor_to_exec_ids[execution.producer_id]
        assert execution.exec_id in index.exec_ids_by_runner_node[(execution.runner_id, execution.node_id)]
        assert execution.producer_id in index.func_name_to_actors[execution.producer_id.source_actor]

    assert len(index.actor_to_exec_ids[trace._log[0].producer_id]) == 2
    assert len(index.actor_to_exec_ids[trace._log[1].producer_id]) == 2


def test_repeated_reads_keep_both_positions_but_one_consumer_identity():
    tide = TIDE()
    tide.event("source", writes={"@A": 1})
    tide.exec(lambda left, right: left + right, args=["@@A", "@@A"])
    trace = tide.trace()
    index = trace._index
    source, consumer = trace._log
    key = ("global:@A", 0)

    assert len(index.read_records_by_ref_version[key]) == 2
    assert [read.input_index for read in index.read_records_by_ref_version[key]] == [0, 1]
    assert list(index.consumer_exec_ids_by_ref_version[key]) == [consumer.exec_id]
    assert len(index.producer_edges_by_exec_id[consumer.exec_id]) == 2
    assert all(
        any(reverse is edge for reverse in index.consumed_edges_by_exec_id[source.exec_id])
        for edge in index.producer_edges_by_exec_id[consumer.exec_id]
    )


def test_effect_occurrence_and_both_edge_directions_share_one_fact():
    tide = TIDE()
    tide.exec(lambda: "ordinary", effects=WRITE("global:effect_output", 11))
    tide.exec(lambda value: value + 1, reads="global:effect_output")
    trace = tide.trace()
    index = trace._index
    producer, consumer = trace._log
    record = producer.effects[0]

    occurrences = list(index.effect_occurrences_by_event_id[record.event_id])
    assert len(occurrences) == 1
    occurrence = occurrences[0]
    assert index.effect_by_occurrence[occurrence] == (producer.exec_id, record)
    assert occurrence in index.effect_occurrences_by_type[Effects.WRITE]
    assert occurrence in index.effect_occurrences_by_target[(Effects.WRITE, "global:effect_output")]
    assert occurrence in index.effect_occurrences_by_ref_version[(Effects.WRITE, "global:effect_output", 0)]

    edges = [
        edge for edge in index.producer_edges_by_exec_id[consumer.exec_id]
        if isinstance(edge, EffectEdge) and edge.effect_record is record
    ]
    assert len(edges) == 1
    assert edges[0].affected_record in index.affected_records_by_effect_occurrence[occurrence]
    assert any(reverse is edges[0] for reverse in index.consumed_edges_by_exec_id[producer.exec_id])


def test_separate_traces_do_not_share_index_facts():
    first = _versioned_trace()
    second = _versioned_trace()

    assert first._index is not second._index
    assert first._index.exec_by_exec_id.keys().isdisjoint(second._index.exec_by_exec_id)
    assert list(first._index.write_records_by_ref_version[("global:x", 0)]) == [first._log[0].writes[0]]
    assert list(second._index.write_records_by_ref_version[("global:x", 0)]) == [second._log[0].writes[0]]


def test_trace_index_and_relationship_edges_are_structurally_immutable():
    trace = _versioned_trace()
    consumer = trace.log[1]
    edge = trace.index.producer_edges_by_exec_id[consumer.exec_id][0]

    with pytest.raises(AttributeError, match="sealed Trace evidence"):
        trace.index.exec_by_exec_id = {}
    with pytest.raises(AttributeError):
        edge.consumer = None

    assert trace.index.producer_edges_by_exec_id[consumer.exec_id][0] is edge
