"""Adversarial forensic fault-injection tests for TIDE.

This suite complements test_forensic_trust_contract.py.

The trust contract proves that independent TIDE views agree on valid evidence.
This suite attacks that evidence. A one-fact mutation must never be silently
accepted as the same clean history.

Allowed outcomes for injected faults:
    1. reject the evidence,
    2. mark it invalid/corrupt/divergent, or
    3. report a non-conforming semantic delta.

Silent clean acceptance is a test failure.
"""

from __future__ import annotations

import copy
import random

import pytest

from tide_tooling import TIDE
from tide_tooling.tooling_core.engine.event_occurrences import FrozenEventLogBundle
from tide_tooling.tooling_core.engine.utils import tide_value_hasher


# ---------------------------------------------------------------------------
# Small independent fixtures/helpers
# ---------------------------------------------------------------------------


def _valid_chain_model(size=5):
    """Create a valid chain with one unique State per handoff."""
    executions = [f"exec-{index}" for index in range(size)]
    states = [f"state-{index}" for index in range(size)]

    outputs = [
        [executions[index], states[index]]
        for index in range(size)
    ]
    inputs = [
        [executions[index + 1], states[index]]
        for index in range(size - 1)
    ]

    return {
        "host": "fault-injection",
        "E": executions,
        "S": states,
        "I": inputs,
        "O": outputs,
    }


def _identity_maps(model):
    return (
        {identity: identity for identity in model["E"]},
        {identity: identity for identity in model["S"]},
    )


def _compare_same_identity(source, target):
    execution_map, state_map = _identity_maps(source)
    return TIDE.from_model(source).compare(
        target,
        mapping="supplied",
        execution_map=execution_map,
        state_map=state_map,
    )


def _assert_not_silently_same(source, mutated):
    """Mutation must be invalid or explicitly non-conforming."""
    validity = TIDE.report_model_validity(mutated)

    if validity["semantically_valid"] is False:
        return

    report = _compare_same_identity(source, mutated)
    assert report.assessable is True
    assert report.conformant is False
    assert (
        report["deltas"]["I_minus"]
        or report["deltas"]["I_plus"]
        or report["deltas"]["O_minus"]
        or report["deltas"]["O_plus"]
    )


def _simple_trace():
    tide = TIDE()
    tide.event("source", writes={"global:a": {"value": [1, 2, 3]}})
    tide.event(
        "middle",
        reads=["global:a"],
        writes={"global:b": {"derived": True}},
    )
    tide.event(
        "sink",
        reads=["global:b"],
        writes={"global:c": "done"},
    )
    return tide.trace()


def _trace_fingerprint(trace):
    return {
        "execs": tuple(
            (
                record.exec_id,
                repr(record.producer_occurrence_id),
                record.status,
                tuple(
                    (
                        write.stored_target,
                        write.stored_version,
                        tide_value_hasher(write.stored_result),
                    )
                    for write in record.writes
                ),
                tuple(
                    (
                        dep.unscoped_reference,
                        dep.resolved_version,
                        dep.producer_exec_id,
                        tide_value_hasher(dep.resolved_value),
                    )
                    for dep in record.deps
                    if dep.type != "dynamic"
                ),
            )
            for record in trace.execs.values()
        ),
        "states": tuple(sorted(
            trace.index.value_hash_by_ref_version.items()
        )),
    }


# ---------------------------------------------------------------------------
# Model fault injection
# ---------------------------------------------------------------------------


def test_fault_injection_missing_producer_is_rejected():
    source = _valid_chain_model()
    mutated = copy.deepcopy(source)
    mutated["O"].pop(2)

    validity = TIDE.report_model_validity(mutated)

    assert validity["semantically_valid"] is False
    assert validity["producer_count_errors"]


def test_fault_injection_duplicate_producer_is_rejected():
    source = _valid_chain_model()
    mutated = copy.deepcopy(source)

    state = mutated["S"][2]
    wrong_exec = mutated["E"][0]
    mutated["O"].append([wrong_exec, state])

    validity = TIDE.report_model_validity(mutated)

    assert validity["semantically_valid"] is False
    assert validity["unique_creation"] is False


def test_fault_injection_cycle_is_rejected():
    source = _valid_chain_model()
    mutated = copy.deepcopy(source)

    # Final State is produced by exec-4. Making exec-0 consume it creates:
    # exec-0 -> ... -> exec-4 -> exec-0.
    mutated["I"].append(["exec-0", "state-4"])

    validity = TIDE.report_model_validity(mutated)

    assert validity["semantically_valid"] is False
    assert validity["acyclic_ancestry"] is False


def test_fault_injection_removed_read_is_valid_but_nonconforming():
    source = _valid_chain_model()
    mutated = copy.deepcopy(source)
    removed = mutated["I"].pop(1)

    assert TIDE.report_model_validity(mutated)["semantically_valid"] is True

    report = _compare_same_identity(source, mutated)

    assert report.assessable is True
    assert report.conformant is False
    assert tuple(removed) in {
        tuple(edge)
        for edge in report["deltas"]["I_minus"]
    }


def test_fault_injection_reassigned_producer_cannot_pass_as_same_history():
    source = _valid_chain_model()
    mutated = copy.deepcopy(source)

    # Reassign one State to another producer while keeping one producer exactly.
    mutated["O"][2][0] = "exec-1"

    _assert_not_silently_same(source, mutated)


def test_fault_injection_added_false_read_cannot_pass_as_same_history():
    source = _valid_chain_model()
    mutated = copy.deepcopy(source)

    # This relation is acyclic but false relative to the source history.
    mutated["I"].append(["exec-4", "state-0"])

    assert TIDE.report_model_validity(mutated)["semantically_valid"] is True

    report = _compare_same_identity(source, mutated)
    assert report.conformant is False
    assert ("exec-4", "state-0") in {
        tuple(edge)
        for edge in report["deltas"]["I_plus"]
    }


# ---------------------------------------------------------------------------
# Deterministic model mutation fuzzing
# ---------------------------------------------------------------------------


def _random_valid_dag_model(rng, size):
    executions = [f"e{index}" for index in range(size)]
    states = []
    inputs = []
    outputs = []

    state_index = 0

    # Give every execution at least one produced State.
    own_state = {}
    for producer in range(size):
        state = f"s{state_index}"
        state_index += 1
        states.append(state)
        own_state[producer] = state
        outputs.append([executions[producer], state])

    # Add forward-only producer -> consumer handoffs.
    for producer in range(size):
        for consumer in range(producer + 1, size):
            if rng.random() < 0.32:
                inputs.append([
                    executions[consumer],
                    own_state[producer],
                ])

    return {
        "host": "fault-fuzz",
        "E": executions,
        "S": states,
        "I": inputs,
        "O": outputs,
    }


@pytest.mark.parametrize("seed", range(24))
def test_fault_fuzzer_one_relation_mutation_is_never_silent(seed):
    """Generate valid DAGs and mutate one semantic fact deterministically."""
    rng = random.Random(0x71DE0000 + seed)
    source = _random_valid_dag_model(
        rng,
        size=rng.randint(5, 9),
    )

    source_validity = TIDE.report_model_validity(source)
    assert source_validity["semantically_valid"] is True

    mutation = seed % 4
    mutated = copy.deepcopy(source)

    if mutation == 0:
        # Remove one producer -> necessarily invalid producer count.
        mutated["O"].pop(rng.randrange(len(mutated["O"])))

    elif mutation == 1:
        # Add duplicate producer for a State -> unique creation violation.
        original_producer, state = rng.choice(mutated["O"])
        alternatives = [
            execution
            for execution in mutated["E"]
            if execution != original_producer
        ]
        mutated["O"].append([rng.choice(alternatives), state])

    elif mutation == 2:
        # Add a forward false read. Validity should normally remain valid,
        # but comparison must expose the added relationship.
        consumer_index = rng.randrange(1, len(mutated["E"]))
        producer_index = rng.randrange(0, consumer_index)
        state = next(
            state_id
            for exec_id, state_id in mutated["O"]
            if exec_id == mutated["E"][producer_index]
        )
        candidate = [mutated["E"][consumer_index], state]

        if candidate not in mutated["I"]:
            mutated["I"].append(candidate)
        else:
            # If randomly selected edge already exists, remove it instead.
            mutated["I"].remove(candidate)

    else:
        # Reassign one State's producer. Depending on topology this may make
        # the model invalid or remain valid-but-different. Either is acceptable;
        # silent equivalence is not.
        index = rng.randrange(len(mutated["O"]))
        original_exec, state = mutated["O"][index]
        alternatives = [
            execution
            for execution in mutated["E"]
            if execution != original_exec
        ]
        mutated["O"][index] = [
            rng.choice(alternatives),
            state,
        ]

    _assert_not_silently_same(source, mutated)


# ---------------------------------------------------------------------------
# Bundle / CAS integrity injection
# ---------------------------------------------------------------------------


def test_fault_injection_cas_byte_mutation_is_rejected():
    trace = _simple_trace()
    complete = trace.to_bundle().to_dict()
    tampered = copy.deepcopy(complete)

    reference = next(iter(tampered["cas"]))
    tampered["cas"][reference] = (
        tampered["cas"][reference]
        + b"\x00FAULT-INJECTED"
    )

    with pytest.raises(ValueError):
        TIDE.from_bundle(
            tampered,
            trusted=True,
        )


def test_fault_injection_cas_reference_substitution_is_rejected():
    trace = _simple_trace()
    complete = trace.to_bundle().to_dict()
    tampered = copy.deepcopy(complete)

    reference, content = next(iter(tampered["cas"].items()))
    del tampered["cas"][reference]
    tampered["cas"]["sha3_512_tide:" + "0" * 128] = content

    with pytest.raises((ValueError, KeyError)):
        TIDE.from_bundle(
            tampered,
            trusted=True,
        )


@pytest.mark.parametrize(
    "mutation",
    ["event_hash", "outer_count", "graph_count"],
)
def test_fault_injection_event_envelope_tampering_is_rejected(mutation):
    trace = _simple_trace()
    complete = trace.to_bundle(
        serialise_events=True,
    ).to_dict()
    tampered = copy.deepcopy(complete)

    event_bundle = tampered["manifest"]["events"]
    assert event_bundle is not None

    if mutation == "event_hash":
        event_bundle["event_log_hash"] = (
            "sha3_512_tide_event_log:"
            + "f" * 128
        )
    elif mutation == "outer_count":
        event_bundle["count"] += 1
    elif mutation == "graph_count":
        event_bundle["events"]["count"] += 1
    else:
        raise AssertionError(mutation)

    with pytest.raises((ValueError, TypeError)):
        TIDE.from_bundle(
            tampered,
            trusted=True,
        )


def test_fault_injection_event_bundle_direct_validation_rejects_hash_lie():
    trace = _simple_trace()
    event_bundle = trace.event_log.to_event_bundle().to_dict()

    event_bundle["event_log_hash"] = (
        "sha3_512_tide_event_log:"
        + "a" * 128
    )

    with pytest.raises(ValueError, match="content hash"):
        FrozenEventLogBundle.from_dict(event_bundle)


# ---------------------------------------------------------------------------
# Evidence immutability / query non-interference
# ---------------------------------------------------------------------------


def test_fault_injection_mutating_value_of_result_cannot_rewrite_evidence():
    trace = _simple_trace()

    before = _trace_fingerprint(trace)
    value = trace.value_of("global:a", 0)

    value["value"].append(999)
    value["injected"] = True

    after = _trace_fingerprint(trace)
    reread = trace.value_of("global:a", 0)

    assert before == after
    assert reread == {"value": [1, 2, 3]}
    assert "injected" not in reread


def test_fault_injection_mutating_bundle_dict_cannot_rewrite_bundle():
    trace = _simple_trace()
    bundle = trace.to_bundle()

    before_hash = bundle.bundle_hash
    exported = bundle.to_dict()

    exported["manifest"]["metadata"]["fault"] = True
    exported["cas"].clear()

    assert bundle.bundle_hash == before_hash
    assert bundle.to_dict()["cas"]
    assert "fault" not in bundle.to_dict()["manifest"]["metadata"]


def test_fault_injection_queries_are_observational_not_mutating():
    trace = _simple_trace()
    before = _trace_fingerprint(trace)

    # Exercise several independent public answer surfaces repeatedly.
    for _ in range(5):
        assert trace.value_of("global:a", 0) == {"value": [1, 2, 3]}
        assert trace.inspect(
            "global:b",
            "reference",
            version=0,
            grouped=False,
        ).exists
        assert trace.investigate(
            "middle",
            "actor",
            grouped=False,
        )
        assert trace.path(
            "source",
            "sink",
            "actor",
            "actor",
        ).exists()

    assert _trace_fingerprint(trace) == before


def test_fault_injection_bundle_round_trip_is_stable_after_hostile_reads():
    trace = _simple_trace()

    # Attempt to mutate every detached value returned by the public value API.
    for reference in ("global:a", "global:b", "global:c"):
        value = trace.value_of(reference, 0)
        if isinstance(value, dict):
            value["hostile"] = "mutation"
        elif isinstance(value, list):
            value.append("mutation")

    restored = TIDE.from_bundle(
        trace.to_bundle().to_dict(),
        trusted=True,
    )

    assert _trace_fingerprint(restored) == _trace_fingerprint(trace)
    assert restored.value_of("global:a", 0) == {"value": [1, 2, 3]}
    assert restored.value_of("global:b", 0) == {"derived": True}
    assert restored.value_of("global:c", 0) == "done"
