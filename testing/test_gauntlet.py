"""Deterministic stress tests for TIDE's small public contracts.

The oracles here are deliberately independent and simpler than production:
sets for ``K = (E, S, I, O)`` and an explicit stack for graph reachability.
"""

from __future__ import annotations

import random
from collections import Counter

from tide_tooling import EMIT, TIDE, WRITE, node, pipeline


SEED = 0x71DE


def _model_oracle(model):
    executions = set(model["E"])
    states = set(model["S"])
    inputs = {tuple(pair) for pair in model["I"]}
    outputs = {tuple(pair) for pair in model["O"]}

    handoffs = {
        (producer, state, consumer)
        for producer, state in outputs
        for consumer, consumed_state in inputs
        if consumed_state == state
    }
    adjacency = {execution: set() for execution in executions}
    for producer, _, consumer in handoffs:
        adjacency[producer].add(consumer)

    reachability = set()
    for source in executions:
        pending = list(adjacency[source])
        reached = set()
        while pending:
            target = pending.pop()
            if target in reached:
                continue
            reached.add(target)
            pending.extend(adjacency[target])
        reachability.update((source, target) for target in reached)

    producer_counts = Counter(state for _, state in outputs)
    valid = (
        all(producer_counts[state] == 1 for state in states)
        and not any(source == target for source, target in reachability)
    )
    return handoffs, reachability, valid


def _random_model(rng, case):
    executions = [f"e{case}_{index}" for index in range(rng.randrange(1, 8))]
    states = [f"s{case}_{index}" for index in range(rng.randrange(0, 8))]
    incidence = [
        (execution, state)
        for execution in executions
        for state in states
    ]
    return {
        "E": executions,
        "S": states,
        "I": [list(pair) for pair in incidence if rng.random() < 0.28],
        "O": [list(pair) for pair in incidence if rng.random() < 0.24],
    }


def test_500_model_structures_match_an_independent_oracle():
    rng = random.Random(SEED)

    for case in range(500):
        model = _random_model(rng, case)
        handoffs, reachability, valid = _model_oracle(model)
        report = TIDE.report_model_validity(model)

        assert set(report.direct_handoffs) == handoffs
        assert set(report.reachability) == reachability
        assert report["semantically_valid"] is valid

        execution_map = {identity: f"right_{identity}" for identity in model["E"]}
        state_map = {identity: f"right_{identity}" for identity in model["S"]}
        renamed = {
            "E": list(reversed(execution_map.values())),
            "S": list(reversed(state_map.values())),
            "I": [
                [execution_map[execution], state_map[state]]
                for execution, state in reversed(model["I"])
            ],
            "O": [
                [execution_map[execution], state_map[state]]
                for execution, state in reversed(model["O"])
            ],
        }
        if renamed["I"]:
            renamed["I"].append(list(renamed["I"][0]))
        if renamed["O"]:
            renamed["O"].append(list(renamed["O"][0]))

        compared = TIDE.from_model(model).compare(
            renamed,
            mapping="supplied",
            execution_map=execution_map,
            state_map=state_map,
        )
        assert compared.complete_agreement is True
        assert compared.judgments_correspond is True


def _reachable(adjacency, source, target):
    pending = list(adjacency[source])
    reached = set()
    while pending:
        current = pending.pop()
        if current == target:
            return True
        if current in reached:
            continue
        reached.add(current)
        pending.extend(adjacency[current])
    return False


def test_random_dag_paths_and_bundle_restores_match_an_independent_oracle():
    rng = random.Random(SEED)

    for case in range(20):
        size = 9
        adjacency = {source: set() for source in range(size)}
        for source in range(size):
            for target in range(source + 1, size):
                if rng.random() < 0.24:
                    adjacency[source].add(target)

        tide = TIDE()
        for actor in range(size):
            reads = [
                f"edge_{source}_{actor}"
                for source in range(actor)
                if actor in adjacency[source]
            ]
            writes = {
                f"edge_{actor}_{target}": f"{case}:{actor}:{target}"
                for target in adjacency[actor]
            }
            tide.event(
                f"actor_{actor}",
                reads=reads,
                writes=writes,
            )

        traces = (
            tide.trace(),
            TIDE.from_bundle(tide.trace().to_bundle().to_dict()),
        )
        for trace in traces:
            exec_ids = {
                int(record.func_name.removeprefix("actor_")): record.exec_id
                for record in trace.log
            }
            for source in range(size):
                for target in range(size):
                    # Same-endpoint queries have their own direct-incidence
                    # contract; this oracle tests graph traversal only.
                    if source == target:
                        continue
                    expected = _reachable(adjacency, source, target)
                    assert trace.path(
                        exec_ids[source],
                        exec_ids[target],
                        "exec_record",
                        "exec_record",
                    ).exists() is expected
                    assert trace.path(
                        exec_ids[target],
                        exec_ids[source],
                        "exec_record",
                        "exec_record",
                        direction="upstream",
                    ).exists() is expected


def test_emission_occurrences_order_duplicates_paths_and_bundle_as_one_contract():
    calls = []

    @node(targets="global:declared_child")
    def declared_child():
        calls.append("declared_child")
        return "declared"

    @node(targets="global:runtime_child")
    def runtime_child():
        calls.append("runtime_child")
        return "runtime"

    @node(effects=[EMIT(declared_child.as_node()), EMIT(declared_child.as_node())])
    def emitter():
        calls.append("emitter")
        emitted = runtime_child.as_node()
        return EMIT([emitted, emitted, emitted])

    @pipeline
    def flow():
        return emitter()

    trace = TIDE().ocean.run_pipeline(flow, mode="sequential")
    restored = TIDE.from_bundle(trace.to_bundle().to_dict())
    expected = [
        ("declared_child", "declared"),
        ("declared_child", "declared"),
        ("runtime_child", "runtime"),
        ("runtime_child", "runtime"),
        ("runtime_child", "runtime"),
    ]

    assert calls.count("emitter") == 1
    assert calls.count("declared_child") == 1
    assert calls.count("runtime_child") == 1
    for candidate in (trace, restored):
        assert [
            (occurrence.func_name, occurrence.origin)
            for occurrence in candidate.log[0].emitted_nodes
        ] == expected
        declared_path = candidate.path(
            "emitter", "declared_child", "actor", "actor"
        )
        runtime_path = candidate.path(
            "emitter", "runtime_child", "actor", "actor"
        )
        assert declared_path.count() == 1
        assert runtime_path.count() == 1
        assert len(declared_path.get_path()["handoffs"][0]) == 2
        assert len(runtime_path.get_path()["handoffs"][0]) == 3
        assert len(candidate.log) == 3


def test_equal_writes_remain_ordered_occurrences_through_bundle_restore():
    @node(
        targets="global:trajectory",
        effects=[
            WRITE("global:trajectory", "same"),
            WRITE("global:trajectory", "same"),
        ],
    )
    def write_trajectory():
        return WRITE("global:trajectory", "same", output="same")

    @pipeline
    def flow():
        return write_trajectory()

    trace = TIDE().ocean.run_pipeline(flow, mode="sequential")
    restored = TIDE.from_bundle(trace.to_bundle().to_dict())

    for candidate in (trace, restored):
        record = candidate.log[0]
        assert [write.stored_version for write in record.writes] == [0, 1, 2, 3]
        assert candidate.value_of("global:trajectory") == [
            "same", "same", "same", "same"
        ]
        assert [effect.targets for effect in record.effects] == [
            ("global:trajectory", 1),
            ("global:trajectory", 2),
            ("global:trajectory", 3),
        ]
        assert [effect.origin for effect in record.effects] == [
            "declared", "declared", "runtime"
        ]
