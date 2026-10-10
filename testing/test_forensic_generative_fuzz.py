"""Deterministic generative / property-style forensic fuzzing for TIDE.

This suite is deliberately dependency-free: no Hypothesis requirement is added
to TIDE. Fixed seeds make every failure reproducible.

It complements:
    test_forensic_trust_contract.py
    test_forensic_fault_injection.py

Properties under test
---------------------
1. Generated valid DAG models must always validate.
2. Structural canonicalisation must ignore host IDs, ordering and duplicates.
3. One-fact semantic mutations must never silently compare as the same history.
4. Bundle/model round-trips must preserve exact relationship kernels.
5. Generated runtime histories must agree with an independent reachability
   oracle before and after persistence.
"""

from __future__ import annotations

import copy
import random

import pytest

from tide_tooling import TIDE


MODEL_CASES = 512
MODEL_CHUNK_SIZE = 16
MODEL_CHUNKS = MODEL_CASES // MODEL_CHUNK_SIZE
RUNTIME_CASES = 48
MASTER_SEED = 0x71DE_F0A5

assert MODEL_CASES % MODEL_CHUNK_SIZE == 0


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------


def _pairs(value):
    result = set()
    for entry in value:
        if isinstance(entry, dict):
            result.add((entry["exec_id"], entry["state_id"]))
        else:
            result.add(tuple(entry))
    return result


def _kernel(model):
    return {
        "E": tuple(model["E"]),
        "S": tuple(model["S"]),
        "I": frozenset(_pairs(model["I"])),
        "O": frozenset(_pairs(model["O"])),
    }


def _identity_maps(model):
    return (
        {identity: identity for identity in model["E"]},
        {identity: identity for identity in model["S"]},
    )


def _compare(source, target):
    execution_map, state_map = _identity_maps(source)
    return TIDE.from_model(source).compare(
        target,
        mapping="supplied",
        execution_map=execution_map,
        state_map=state_map,
    )


def _assert_mutation_detected(source, mutated, *, label, case):
    validity = TIDE.report_model_validity(mutated)

    if validity["semantically_valid"] is False:
        return

    compared = _compare(source, mutated)

    assert compared.assessable is True, (case, label)
    assert compared.conformant is False, (
        "semantic mutation silently compared as conforming",
        case,
        label,
        source,
        mutated,
    )

    deltas = compared["deltas"]
    assert (
        deltas["I_minus"]
        or deltas["I_plus"]
        or deltas["O_minus"]
        or deltas["O_plus"]
    ), (
        "non-conformance reported without a relationship delta",
        case,
        label,
    )


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


# ---------------------------------------------------------------------------
# Generated valid TIDE models
# ---------------------------------------------------------------------------


def _generate_model(rng, case):
    size = rng.randint(4, 11)

    executions = [
        f"case{case}:exec:{index}"
        for index in range(size)
    ]
    states = []
    outputs = []
    inputs = []
    states_by_producer = {}

    # Every execution produces between one and three exact States.
    for producer_index, execution in enumerate(executions):
        produced = []

        for local_index in range(rng.randint(1, 3)):
            state = (
                f"case{case}:state:"
                f"{producer_index}:{local_index}"
            )
            states.append(state)
            produced.append(state)
            outputs.append([execution, state])

        states_by_producer[producer_index] = produced

    # Forward-only reads guarantee acyclicity.
    for consumer_index in range(1, size):
        candidate_producers = list(range(consumer_index))
        rng.shuffle(candidate_producers)

        # Some executions deliberately read nothing.
        selected = candidate_producers[
            : rng.randint(0, len(candidate_producers))
        ]

        for producer_index in selected:
            # Pick one or more States from that producer.
            produced = list(states_by_producer[producer_index])
            rng.shuffle(produced)

            for state in produced[: rng.randint(1, len(produced))]:
                edge = [executions[consumer_index], state]
                if edge not in inputs:
                    inputs.append(edge)

    return {
        "host": f"generated:{case}",
        "E": executions,
        "S": states,
        "I": inputs,
        "O": outputs,
    }


def _relabel_shuffle_duplicate(model, rng, case):
    execution_ids = list(model["E"])
    state_ids = list(model["S"])

    exec_map = {
        old: f"host-{case}-e-{index}-{rng.randrange(1_000_000)}"
        for index, old in enumerate(execution_ids)
    }
    state_map = {
        old: f"host-{case}-s-{index}-{rng.randrange(1_000_000)}"
        for index, old in enumerate(state_ids)
    }

    relabelled = {
        "host": f"renamed:{case}",
        "E": [exec_map[value] for value in execution_ids],
        "S": [state_map[value] for value in state_ids],
        "I": [
            [exec_map[execution], state_map[state]]
            for execution, state in model["I"]
        ],
        "O": [
            [exec_map[execution], state_map[state]]
            for execution, state in model["O"]
        ],
    }

    rng.shuffle(relabelled["E"])
    rng.shuffle(relabelled["S"])
    rng.shuffle(relabelled["I"])
    rng.shuffle(relabelled["O"])

    # Duplicate incidence is semantically set-valued and must not alter
    # canonical structure.
    if relabelled["I"] and rng.random() < 0.75:
        relabelled["I"].append(
            copy.deepcopy(rng.choice(relabelled["I"]))
        )
    if relabelled["O"] and rng.random() < 0.75:
        relabelled["O"].append(
            copy.deepcopy(rng.choice(relabelled["O"]))
        )

    return relabelled


def _structural(model):
    return TIDE.from_model(model).to_model(
        enrich=False,
        incidence_format="pairs",
        identity_mode="structural",
    )


def _mutation_missing_producer(model, rng):
    mutated = copy.deepcopy(model)
    mutated["O"].pop(rng.randrange(len(mutated["O"])))
    return mutated


def _mutation_duplicate_producer(model, rng):
    mutated = copy.deepcopy(model)

    original_producer, state = rng.choice(mutated["O"])
    alternatives = [
        execution
        for execution in mutated["E"]
        if execution != original_producer
    ]
    mutated["O"].append([rng.choice(alternatives), state])
    return mutated


def _mutation_reassign_producer(model, rng):
    mutated = copy.deepcopy(model)

    index = rng.randrange(len(mutated["O"]))
    original_producer, state = mutated["O"][index]
    alternatives = [
        execution
        for execution in mutated["E"]
        if execution != original_producer
    ]
    mutated["O"][index] = [rng.choice(alternatives), state]
    return mutated


def _mutation_remove_read(model, rng):
    if not model["I"]:
        return None

    mutated = copy.deepcopy(model)
    mutated["I"].pop(rng.randrange(len(mutated["I"])))
    return mutated


def _mutation_false_forward_read(model, rng):
    """Add/remove one forward read while preserving DAG direction."""
    producer_by_state = {
        state: execution
        for execution, state in model["O"]
    }
    exec_index = {
        execution: index
        for index, execution in enumerate(model["E"])
    }

    candidates = []

    for state, producer in producer_by_state.items():
        producer_index = exec_index[producer]

        for consumer_index in range(producer_index + 1, len(model["E"])):
            consumer = model["E"][consumer_index]
            edge = [consumer, state]
            candidates.append(edge)

    if not candidates:
        return None

    mutated = copy.deepcopy(model)
    edge = copy.deepcopy(rng.choice(candidates))

    if edge in mutated["I"]:
        mutated["I"].remove(edge)
    else:
        mutated["I"].append(edge)

    return mutated


def _mutation_cycle(model, rng):
    """Force a backwards ancestry edge from a later State to an ancestor."""
    if len(model["E"]) < 2:
        return None

    producer_by_state = {
        state: execution
        for execution, state in model["O"]
    }
    exec_index = {
        execution: index
        for index, execution in enumerate(model["E"])
    }

    later_states = [
        state
        for state, producer in producer_by_state.items()
        if exec_index[producer] > 0
    ]
    if not later_states:
        return None

    state = rng.choice(later_states)
    producer_index = exec_index[producer_by_state[state]]
    earlier_consumer = model["E"][rng.randrange(producer_index)]

    mutated = copy.deepcopy(model)
    edge = [earlier_consumer, state]

    if edge not in mutated["I"]:
        mutated["I"].append(edge)

    return mutated


MUTATORS = (
    ("missing_producer", _mutation_missing_producer),
    ("duplicate_producer", _mutation_duplicate_producer),
    ("reassign_producer", _mutation_reassign_producer),
    ("remove_read", _mutation_remove_read),
    ("false_forward_read", _mutation_false_forward_read),
    ("cycle", _mutation_cycle),
)


@pytest.mark.parametrize(
    "chunk",
    range(MODEL_CHUNKS),
    ids=lambda chunk: f"model-chunk-{chunk:02d}",
)
def test_generative_valid_models_and_mutations_by_chunk(chunk):
    """512 total DAGs split into visible deterministic chunks."""
    first_case = chunk * MODEL_CHUNK_SIZE
    last_case = first_case + MODEL_CHUNK_SIZE
    mutation_count = 0

    for case in range(first_case, last_case):
        # Each case owns its seed. Running a single chunk/case therefore does
        # not depend on how many earlier cases pytest executed.
        model_rng = random.Random(
            MASTER_SEED ^ (case * 0x9E3779B97F4A7C15)
        )
        source = _generate_model(model_rng, case)

        validity = TIDE.report_model_validity(source)

        assert validity["semantically_valid"] is True, (
            "generator produced invalid source model",
            case,
            source,
            validity,
        )
        assert validity["unique_creation"] is True
        assert validity["acyclic_ancestry"] is True
        assert not validity["producer_count_errors"]

        # Model import/export must retain exact relationship semantics.
        imported = TIDE.from_model(source).to_model(
            enrich=False,
            incidence_format="pairs",
        )
        assert _pairs(imported["I"]) == _pairs(source["I"])
        assert _pairs(imported["O"]) == _pairs(source["O"])
        assert set(imported["E"]) == set(source["E"])
        assert set(imported["S"]) == set(source["S"])

        # Structural view must be invariant to arbitrary host labels/order.
        relabelled = _relabel_shuffle_duplicate(
            source,
            model_rng,
            case,
        )

        canonical_source = _structural(source)
        canonical_relabelled = _structural(relabelled)

        assert _pairs(canonical_source["I"]) == _pairs(
            canonical_relabelled["I"]
        ), case
        assert _pairs(canonical_source["O"]) == _pairs(
            canonical_relabelled["O"]
        ), case
        assert len(canonical_source["E"]) == len(
            canonical_relabelled["E"]
        ), case
        assert len(canonical_source["S"]) == len(
            canonical_relabelled["S"]
        ), case

        # Structural canonicalisation must itself be idempotent.
        repeated = TIDE.from_model(
            canonical_source,
            identity_mode="structural",
        ).to_model(
            enrich=False,
            incidence_format="pairs",
            identity_mode="structural",
        )
        assert _pairs(repeated["I"]) == _pairs(canonical_source["I"])
        assert _pairs(repeated["O"]) == _pairs(canonical_source["O"])

        # Inject each independent semantic fault class.
        for mutator_index, (label, mutator) in enumerate(MUTATORS):
            mutation_rng = random.Random(
                MASTER_SEED
                ^ (case * 0xD1B54A32D192ED03)
                ^ (mutator_index * 0x94D049BB133111EB)
            )
            mutated = mutator(source, mutation_rng)

            if mutated is None or mutated == source:
                continue

            mutation_count += 1
            _assert_mutation_detected(
                source,
                mutated,
                label=label,
                case=case,
            )

    # With 16 models and six mutation classes, most chunks exercise close to
    # 96 mutations. This lower bound protects the chunk from silently
    # degenerating while allowing cases where a read/cycle mutation is
    # inapplicable.
    assert mutation_count >= 70, (
        chunk,
        first_case,
        last_case,
        mutation_count,
    )


# ---------------------------------------------------------------------------
# Runtime-level generated histories
# ---------------------------------------------------------------------------


def _runtime_adjacency(rng, size):
    adjacency = {
        source: set()
        for source in range(size)
    }

    for source in range(size):
        for target in range(source + 1, size):
            if rng.random() < 0.27:
                adjacency[source].add(target)

    return adjacency


def _runtime_trace(case, adjacency):
    tide = TIDE()

    size = len(adjacency)

    for actor in range(size):
        reads = [
            f"global:edge_{source}_{actor}"
            for source in range(actor)
            if actor in adjacency[source]
        ]
        writes = {
            f"global:edge_{actor}_{target}":
                {
                    "case": case,
                    "producer": actor,
                    "consumer": target,
                }
            for target in adjacency[actor]
        }

        # Ensure otherwise-isolated executions still have evidence.
        if not writes:
            writes = {
                f"global:terminal_{actor}": {
                    "case": case,
                    "actor": actor,
                }
            }

        tide.event(
            f"actor_{actor}",
            reads=reads,
            writes=writes,
        )

    return tide.trace()


@pytest.mark.parametrize("case", range(RUNTIME_CASES))
def test_generative_runtime_dag_oracle_bundle_and_queries(case):
    """48 actual TIDE histories checked against a separately generated DAG."""
    rng = random.Random(MASTER_SEED ^ (case * 0x9E3779B1))
    size = rng.randint(5, 10)
    adjacency = _runtime_adjacency(rng, size)

    source = _runtime_trace(case, adjacency)
    restored = TIDE.from_bundle(
        source.to_bundle().to_dict(),
        trusted=True,
    )

    for trace in (source, restored):
        records = {
            int(record.func_name.removeprefix("actor_")):
                record
            for record in trace.execs.values()
        }

        assert set(records) == set(range(size))

        model = trace.to_model(
            enrich=True,
            incidence_format="pairs",
        )
        validity = TIDE.report_model_validity(model)

        assert validity["semantically_valid"] is True
        assert validity["unique_creation"] is True
        assert validity["acyclic_ancestry"] is True

        # Every generated direct edge must exist as one exact shared State.
        for producer, consumers in adjacency.items():
            for consumer in consumers:
                reference = f"global:edge_{producer}_{consumer}"

                assert trace.path(
                    records[producer].exec_id,
                    records[consumer].exec_id,
                    "exec_record",
                    "exec_record",
                ).exists()

                assert trace.path(
                    reference,
                    records[consumer].exec_id,
                    "reference",
                    "exec_record",
                ).exists()

        # Exhaustively compare all execution pairs to independent reachability.
        for start in range(size):
            for end in range(size):
                if start == end:
                    continue

                expected = _reachable(adjacency, start, end)

                assert trace.path(
                    records[start].exec_id,
                    records[end].exec_id,
                    "exec_record",
                    "exec_record",
                ).exists() is expected, (
                    case,
                    start,
                    end,
                    adjacency,
                )

                assert trace.path(
                    records[end].exec_id,
                    records[start].exec_id,
                    "exec_record",
                    "exec_record",
                    direction="upstream",
                ).exists() is expected, (
                    case,
                    "upstream",
                    start,
                    end,
                    adjacency,
                )

    # Persistence cannot alter the represented kernel.
    source_model = source.to_model(
        enrich=False,
        incidence_format="pairs",
    )
    restored_model = restored.to_model(
        enrich=False,
        incidence_format="pairs",
    )

    assert _pairs(source_model["I"]) == _pairs(restored_model["I"])
    assert _pairs(source_model["O"]) == _pairs(restored_model["O"])


def test_generative_fuzz_configuration_is_intentionally_large():
    """Prevent an accidental edit from reducing this to token fuzzing."""
    assert MODEL_CASES >= 500
    assert MODEL_CHUNKS >= 32
    assert MODEL_CHUNK_SIZE >= 16
    assert MODEL_CHUNKS * MODEL_CHUNK_SIZE == MODEL_CASES
    assert RUNTIME_CASES >= 40
    assert len(MUTATORS) >= 6
