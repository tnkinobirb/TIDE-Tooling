"""Standalone forensic trust contract for TIDE.

This test deliberately does not import fixtures or helpers from any other test
module. It builds its own workflow, derives its own occurrence-level oracle,
and requires the Event, Trace, TraceIndex, query, Model, and Bundle surfaces to
tell the same story.

The goal is not merely "the workflow ran". The goal is that independently
derived representations cannot silently disagree about who executed, which
State version was produced or consumed, what value identity that State had,
or which producer-consumer relationships existed.
"""

from __future__ import annotations

from collections import defaultdict

import pytest

from tide_tooling import DELETE_NOW, READ, Ocean, TIDE, node, pipeline
from tide_tooling.tooling_core.engine.events import Effects
from tide_tooling.tooling_core.engine.utils import tide_value_hasher


def _build_forensic_pipeline():
    """Create a small but hostile DAG with versioning, branching and Effects."""

    @node(targets="global:seed")
    def seed():
        shared = ["seed", 1]
        # Alias is intentional: Bundle must not silently rewrite value shape.
        return {"left": shared, "right": shared}

    @node(args=("@global:seed",), targets="global:working")
    def transform(seed_value):
        return {
            "payload": seed_value,
            "stage": "working",
        }

    @node(
        args=("@global:working",),
        targets=["global:working", "global:repeat_done"],
        unpack=True,
    )
    def repeat_equal(working):
        # Equal value, new write occurrence/version.
        return working, True

    @node(
        args=("@global:working", "@global:repeat_done"),
        targets="global:left",
    )
    def left(working, _done):
        return {"branch": "left", "working": working}

    @node(
        args=("@global:working", "@global:repeat_done"),
        targets="global:right",
    )
    def right(working, _done):
        return {"branch": "right", "working": working}

    @node(
        args=("@global:left", "@global:right"),
        targets=["global:result", "global:temporary"],
        unpack=True,
    )
    def join(left_value, right_value):
        return (
            {
                "left": left_value,
                "right": right_value,
            },
            "remove-me",
        )

    @node(
        args=("@global:result",),
        targets="global:audit",
    )
    def audit(result):
        # Runtime READ is deliberately separate from declared dependency input.
        observed = READ("global:working")
        return {
            "result": result,
            "observed": observed,
        }

    @node(
        args=("@global:temporary",),
        targets="global:cleanup",
    )
    def cleanup(temporary):
        removed = DELETE_NOW("global:temporary")
        missing = DELETE_NOW("global:never")
        return {
            "temporary": temporary,
            "removed": removed,
            "missing": missing,
        }

    @pipeline
    def forensic_flow():
        seed()
        transform()
        repeat_equal()
        left()
        right()
        join()
        audit()
        cleanup()

    return forensic_flow


@pytest.fixture(scope="module")
def forensic_run():
    ocean = Ocean()
    trace = ocean.run_pipeline(
        _build_forensic_pipeline(),
        mode="sequential",
    )
    try:
        yield ocean, trace
    finally:
        ocean.close()


def _concrete_reads(record):
    for dependency in record.deps:
        if dependency.type == "dynamic":
            yield from dependency.expanded_to or ()
        else:
            yield dependency


def _model_exec_id(record):
    """Mirror the documented literal TIDE Model execution identity rule."""
    occurrence = record.producer_occurrence_id
    return repr(occurrence) if occurrence is not None else record.exec_id


def _state_id(reference, version):
    return f"{reference}[v{version}]"


def _relation(model, name):
    result = set()
    for entry in model[name]:
        if isinstance(entry, dict):
            result.add((entry["exec_id"], entry["state_id"]))
        else:
            result.add(tuple(entry))
    return result


def _snapshot_hash(record, field, fallback):
    snapshot = (
        record._tide_snapshot(field)
        if hasattr(record, "_tide_snapshot")
        else None
    )
    return (
        snapshot.value_hash
        if snapshot is not None
        else tide_value_hasher(fallback)
    )


def _fingerprint(trace):
    """Portable occurrence facts that persistence must preserve exactly."""
    writes = []
    reads = []
    effects = []

    for execution in trace.execs.values():
        for write in execution.writes:
            writes.append((
                execution.exec_id,
                write.stored_target,
                write.stored_version,
                _snapshot_hash(
                    write,
                    "stored_result",
                    write.stored_result,
                ),
                getattr(write.cause, "event_id", None),
                getattr(write.cause, "description", None),
            ))

        for read in _concrete_reads(execution):
            reads.append((
                execution.exec_id,
                read.producer_exec_id,
                read.unscoped_reference,
                read.resolved_version,
                _snapshot_hash(
                    read,
                    "resolved_value",
                    read.resolved_value,
                ),
                read.observation_id,
            ))

        for effect in execution.effects:
            effects.append((
                execution.exec_id,
                effect.event_id,
                effect.effect.value,
                repr(effect.targets),
                effect.immediate,
                effect.target_existed,
                tuple(sorted(
                    affected.affected_exec_id
                    for affected in effect.affected.values()
                    if affected.affected_exec_id is not None
                )),
            ))

    return {
        "executions": tuple(
            (
                record.seq_id,
                record.exec_id,
                repr(record.producer_occurrence_id),
                record.producer_id.source_actor,
                record.status,
                record.started_at,
                record.finished_at,
                record.duration_ms,
            )
            for record in trace.execs.values()
        ),
        "writes": tuple(sorted(writes)),
        "reads": tuple(sorted(reads)),
        "effects": tuple(sorted(effects)),
        "state_hashes": tuple(sorted(
            trace.index.value_hash_by_ref_version.items()
        )),
    }


def _assert_event_causality(trace):
    events = tuple(trace.events)
    event_ids = [event.event_id for event in events]

    assert events
    assert len(event_ids) == len(set(event_ids))
    assert trace.missing_referenced_events == {}
    assert trace.causal_order_violations == {}

    positions = {
        event_id: position
        for position, event_id in enumerate(event_ids)
    }

    for event in events:
        cause_id = (
            event.cause.event_id
            if event.cause is not None
            else None
        )
        if cause_id is None:
            continue

        assert cause_id in positions, (
            "Event points to causal evidence absent from the retained stream",
            event.event_id,
            cause_id,
        )
        assert positions[cause_id] < positions[event.event_id], (
            "Event cause must precede its consequence",
            cause_id,
            event.event_id,
        )


def _derive_record_oracle(trace):
    """Derive truth from normalized execution evidence only.

    This intentionally does not consult TraceIndex, Model validity, path(),
    investigate(), or bundle metadata.
    """
    records = tuple(trace.execs.values())

    executions = {
        record.exec_id: record
        for record in records
    }
    assert len(executions) == len(records)

    state_producers = {}
    state_hashes = {}
    reads = []
    direct_handoffs = set()

    for execution in records:
        for write in execution.writes:
            state = (write.stored_target, write.stored_version)

            assert state not in state_producers, (
                "Two executions claim the same State occurrence",
                state,
            )

            state_producers[state] = execution.exec_id
            state_hashes[state] = _snapshot_hash(
                write,
                "stored_result",
                write.stored_result,
            )

    for execution in records:
        for read in _concrete_reads(execution):
            state = (
                read.unscoped_reference,
                read.resolved_version,
            )

            assert state in state_producers, (
                "Successful read has no recorded State producer",
                execution.exec_id,
                state,
            )
            assert read.producer_exec_id == state_producers[state], (
                "Read producer identity disagrees with recorded write",
                execution.exec_id,
                state,
                read.producer_exec_id,
                state_producers[state],
            )
            assert _snapshot_hash(
                read,
                "resolved_value",
                read.resolved_value,
            ) == state_hashes[state], (
                "Read observed a different value identity for one State",
                execution.exec_id,
                state,
            )

            reads.append((execution.exec_id, read, state))
            direct_handoffs.add((
                state_producers[state],
                state,
                execution.exec_id,
            ))

    adjacency = defaultdict(set)
    for producer_exec_id, _, consumer_exec_id in direct_handoffs:
        adjacency[producer_exec_id].add(consumer_exec_id)

    reachability = set()
    for source in executions:
        pending = list(adjacency[source])
        reached = set()

        while pending:
            target = pending.pop()
            if target in reached:
                continue
            reached.add(target)
            reachability.add((source, target))
            pending.extend(adjacency[target])

    return {
        "records": records,
        "executions": executions,
        "state_producers": state_producers,
        "state_hashes": state_hashes,
        "reads": tuple(reads),
        "direct_handoffs": direct_handoffs,
        "reachability": reachability,
    }


def _assert_cross_view_agreement(trace, *, check_events):
    oracle = _derive_record_oracle(trace)
    records = oracle["records"]
    index = trace.index

    # ------------------------------------------------------------------
    # Execution identity / status / timing
    # ------------------------------------------------------------------
    assert len({record.exec_id for record in records}) == len(records)
    assert len({record.seq_id for record in records}) == len(records)

    occurrences = [
        record.producer_occurrence_id
        for record in records
        if record.producer_occurrence_id is not None
    ]
    assert len(set(occurrences)) == len(occurrences)

    for record in records:
        assert record.error is None
        assert record.status is not None

        if record.started_at is not None and record.finished_at is not None:
            assert record.started_at <= record.finished_at
        if record.duration_ms is not None:
            assert record.duration_ms >= 0

        assert index.exec_by_exec_id[record.exec_id].exec_id == record.exec_id

        if record.producer_occurrence_id is not None:
            assert (
                index.actor_occurrence_to_exec_id[
                    record.producer_occurrence_id
                ]
                == record.exec_id
            )

    assert set(index.exec_by_exec_id) == set(oracle["executions"])

    if check_events:
        _assert_event_causality(trace)

    # ------------------------------------------------------------------
    # Writes / State authority / value query / model O
    # ------------------------------------------------------------------
    expected_model_outputs = set()

    for state, producer_exec_id in oracle["state_producers"].items():
        reference, version = state
        producer = oracle["executions"][producer_exec_id]

        assert tuple(
            index.producer_exec_ids_by_ref_version[state]
        ) == (producer_exec_id,)

        indexed_writes = tuple(
            index.write_records_by_ref_version[state]
        )
        assert len(indexed_writes) == 1

        assert index.value_hash_by_ref_version[state] == (
            oracle["state_hashes"][state]
        )
        assert tide_value_hasher(
            trace.value_of(reference, version)
        ) == oracle["state_hashes"][state]

        producer_endpoint = trace.inspect(
            producer_exec_id,
            "exec_record",
            grouped=False,
        )
        state_endpoint = trace.inspect(
            reference,
            "reference",
            version=version,
            grouped=False,
        )
        assert trace.path(
            producer_endpoint,
            state_endpoint,
        ).exists()

        expected_model_outputs.add((
            _model_exec_id(producer),
            _state_id(reference, version),
        ))

    model = trace.to_model(
        enrich=True,
        incidence_format="pairs",
    )
    validity = TIDE.report_model_validity(model)

    assert validity["semantically_valid"] is True
    assert expected_model_outputs == _relation(model, "O")

    # ------------------------------------------------------------------
    # Reads / exact producer / query summaries / model I
    # ------------------------------------------------------------------
    expected_model_inputs = set()
    expected_model_handoffs = set()

    for execution in records:
        concrete_reads = tuple(_concrete_reads(execution))

        investigated = trace.investigate(
            execution.exec_id,
            "exec_record",
            grouped=False,
        ).to_dict()

        assert investigated["object"]["id"] == execution.exec_id
        assert investigated["summary"]["read_count"] == len(concrete_reads)
        assert investigated["summary"]["write_count"] == len(
            execution.writes
        )

        for read in concrete_reads:
            state = (
                read.unscoped_reference,
                read.resolved_version,
            )
            producer_exec_id = oracle["state_producers"][state]
            producer = oracle["executions"][producer_exec_id]

            assert execution.exec_id in (
                index.consumer_exec_ids_by_ref_version[state]
            )
            assert read in index.read_records_by_ref_version[state]
            assert read.producer_exec_id == producer_exec_id
            assert index.value_hash_by_ref_version[state] == (
                oracle["state_hashes"][state]
            )

            expected_model_inputs.add((
                _model_exec_id(execution),
                _state_id(*state),
            ))
            expected_model_handoffs.add((
                _model_exec_id(producer),
                _state_id(*state),
                _model_exec_id(execution),
            ))

            assert trace.path(
                producer.exec_id,
                execution.exec_id,
                "exec_record",
                "exec_record",
            ).exists()

            state_endpoint = trace.inspect(
                read.unscoped_reference,
                "reference",
                version=read.resolved_version,
                grouped=False,
            )
            consumer_endpoint = trace.inspect(
                execution.exec_id,
                "exec_record",
                grouped=False,
            )
            assert trace.path(
                state_endpoint,
                consumer_endpoint,
            ).exists()

    assert expected_model_inputs == _relation(model, "I")
    assert expected_model_handoffs == set(validity.direct_handoffs)

    # ------------------------------------------------------------------
    # Effect occurrence identity / deletion semantics
    # ------------------------------------------------------------------
    effect_records = [
        (execution, effect)
        for execution in records
        for effect in execution.effects
    ]
    assert effect_records

    for execution, effect in effect_records:
        occurrences = tuple(
            index.effect_occurrences_by_event_id[effect.event_id]
        )
        assert occurrences

        assert any(
            index.effect_by_occurrence[occurrence][0] == execution.exec_id
            and index.effect_by_occurrence[occurrence][1].effect
            == effect.effect
            and index.effect_by_occurrence[occurrence][1].targets
            == effect.targets
            for occurrence in occurrences
        )

    successful_delete = next(
        effect
        for _, effect in effect_records
        if effect.effect == Effects.DELETE
        and effect.target_existed is True
    )
    failed_delete = next(
        effect
        for _, effect in effect_records
        if effect.effect == Effects.DELETE
        and effect.target_existed is False
    )

    assert successful_delete.targets in index.deleted_ref_versions
    assert failed_delete.targets not in index.deleted_ref_versions

    read_effect = next(
        effect
        for _, effect in effect_records
        if effect.effect == Effects.READ
    )
    assert read_effect.immediate is True
    assert read_effect.target_existed is True

    # ------------------------------------------------------------------
    # Reachability: independently derived oracle vs Model vs path()
    # ------------------------------------------------------------------
    expected_model_reachability = {
        (
            _model_exec_id(oracle["executions"][source]),
            _model_exec_id(oracle["executions"][target]),
        )
        for source, target in oracle["reachability"]
    }
    assert expected_model_reachability == set(validity.reachability)

    for source in records:
        for target in records:
            if source.exec_id == target.exec_id:
                continue

            expected = (
                source.exec_id,
                target.exec_id,
            ) in oracle["reachability"]

            assert trace.path(
                source.exec_id,
                target.exec_id,
                "exec_record",
                "exec_record",
            ).exists() is expected

    return oracle, model


def test_forensic_contract_all_tide_surfaces_agree(forensic_run):
    """The live run must have one occurrence-level truth across all views."""
    _, trace = forensic_run

    oracle, model = _assert_cross_view_agreement(
        trace,
        check_events=True,
    )

    # Equal write value must still be a distinct State occurrence.
    working_states = sorted(
        state
        for state in oracle["state_producers"]
        if state[0] == "global:working"
    )
    assert working_states == [
        ("global:working", 0),
        ("global:working", 1),
    ]
    assert (
        oracle["state_hashes"][working_states[0]]
        == oracle["state_hashes"][working_states[1]]
    )

    # Branch/join shape is independently visible in Model judgments.
    actor_to_model_exec = {
        metadata["actor"]["name"]: exec_id
        for exec_id, metadata in model["executions"].items()
    }
    reachability = set(
        TIDE.report_model_validity(model).reachability
    )

    assert (
        actor_to_model_exec["seed"],
        actor_to_model_exec["join"],
    ) in reachability
    assert (
        actor_to_model_exec["left"],
        actor_to_model_exec["join"],
    ) in reachability
    assert (
        actor_to_model_exec["right"],
        actor_to_model_exec["join"],
    ) in reachability


def test_forensic_contract_bundle_preserves_exact_occurrence_truth(forensic_run):
    """TideRun persistence must preserve identity, not just graph shape."""
    _, source = forensic_run

    restored = TIDE.from_bundle(
        source.to_bundle().to_dict(),
        trusted=True,
    )

    source_oracle, source_model = _assert_cross_view_agreement(
        source,
        check_events=True,
    )
    restored_oracle, restored_model = _assert_cross_view_agreement(
        restored,
        check_events=False,
    )

    # Exact occurrence-level persistence.
    assert _fingerprint(restored) == _fingerprint(source)

    # Same producer identity per State and same causal read source.
    assert restored_oracle["state_producers"] == (
        source_oracle["state_producers"]
    )
    assert restored_oracle["state_hashes"] == source_oracle["state_hashes"]

    source_reads = {
        (
            execution_id,
            state,
            read.producer_exec_id,
            read.observation_id,
        )
        for execution_id, read, state in source_oracle["reads"]
    }
    restored_reads = {
        (
            execution_id,
            state,
            read.producer_exec_id,
            read.observation_id,
        )
        for execution_id, read, state in restored_oracle["reads"]
    }
    assert restored_reads == source_reads

    # The interoperable kernel and its judgments must remain identical.
    execution_map = {
        identity: identity
        for identity in source_model["E"]
    }
    state_map = {
        identity: identity
        for identity in source_model["S"]
    }
    comparison = TIDE.from_model(source_model).compare(
        restored_model,
        mapping="supplied",
        execution_map=execution_map,
        state_map=state_map,
    )

    assert comparison.complete_agreement is True
    assert comparison.judgments_correspond is True


# ======================================================================
# HARDENING LAYER: Event semantics, persistence, tamper resistance,
# agentic emission, dynamic/external boundaries, failures and deadlocks.
# ======================================================================


def _event_fingerprint(events):
    return tuple(
        (
            event.event_id,
            event.type,
            event.exec_id,
            getattr(event, "source", None),
            getattr(event, "source_id", None),
            getattr(event, "target_runner_id", None),
            (
                event.cause.event_id
                if getattr(event, "cause", None) is not None
                else None
            ),
            (
                event.cause.description
                if getattr(event, "cause", None) is not None
                else None
            ),
        )
        for event in events
    )


def _assert_event_to_trace_semantics(trace):
    """Tie raw Event occurrences to normalized Trace occurrences."""
    from tide_tooling.tooling_core.engine.events import (
        ExecutorEvents,
        LogEvents,
        PipelineEvents,
    )

    events = tuple(trace.events)
    positions = {
        event.event_id: index
        for index, event in enumerate(events)
    }
    records = tuple(trace.execs.values())

    result_ready = [
        event
        for event in events
        if event.type == PipelineEvents.RESULT_READY
    ]
    assert len(result_ready) == len(records)

    result_by_exec = {
        event.payload.exec_meta.id: event
        for event in result_ready
    }
    assert set(result_by_exec) == {
        record.exec_id for record in records
    }

    for record in records:
        result_event = result_by_exec[record.exec_id]
        assert record.event_seq_id == positions[result_event.event_id]
        assert result_event.payload.exec_meta.status == record.status
        assert (
            result_event.payload.exec_meta.execution_mode
            == "sequential"
        )

    effect_events = {
        event.event_id: event
        for event in events
        if event.type == LogEvents.EFFECT
    }

    for record in records:
        for effect in record.effects:
            assert effect.event_id in effect_events
            event = effect_events[effect.event_id]

            assert event.exec_id == record.exec_id
            assert event.payload.effect == effect.effect

    # Every runtime execution must have one readiness occurrence before result.
    ready_events = [
        event
        for event in events
        if event.type == ExecutorEvents.NODE_READY
    ]
    assert len(ready_events) == len(records)

    ready_by_exec = {
        event.exec_id: event
        for event in ready_events
    }
    assert set(ready_by_exec) == {
        record.exec_id for record in records
    }

    for record in records:
        assert (
            positions[ready_by_exec[record.exec_id].event_id]
            < record.event_seq_id
        )


def test_forensic_event_semantics_match_normalized_trace(forensic_run):
    """Event evidence and normalized evidence cannot drift independently."""
    _, trace = forensic_run

    _assert_event_causality(trace)
    _assert_event_to_trace_semantics(trace)


def test_forensic_event_sidecar_round_trip_is_exact(forensic_run):
    """Optional raw Event persistence must preserve exact causal identities."""
    _, source = forensic_run

    bundle = source.to_bundle(serialise_events=True)

    assert bundle.event_log is not None
    assert _event_fingerprint(bundle.events) == _event_fingerprint(
        source.events
    )

    restored = TIDE.from_bundle(
        bundle.to_dict(),
        trusted=True,
    )

    assert _event_fingerprint(restored.events) == _event_fingerprint(
        source.events
    )

    # Imported traces re-export their retained evidence exactly.
    reexported = restored.to_bundle()
    assert reexported.to_dict() == bundle.to_dict()
    assert reexported.bundle_hash == bundle.bundle_hash


def test_forensic_bundle_rejects_cas_tampering(forensic_run):
    """Changing bytes without changing their content address must be rejected."""
    import copy

    _, source = forensic_run
    complete = source.to_bundle().to_dict()
    tampered = copy.deepcopy(complete)

    reference = next(iter(tampered["cas"]))
    original = tampered["cas"][reference]
    assert type(original) is bytes

    tampered["cas"][reference] = original + b"\x00FORENSIC-TAMPER"

    with pytest.raises(ValueError):
        TIDE.from_bundle(
            tampered,
            trusted=True,
        )


def test_forensic_bundle_preserves_alias_identity(forensic_run):
    """Equal-looking values are not enough; object graph aliasing must survive."""
    _, source = forensic_run

    live_seed = source.value_of("global:seed", 0)
    assert live_seed["left"] is live_seed["right"]

    restored = TIDE.from_bundle(
        source.to_bundle().to_dict(),
        trusted=True,
    )
    restored_seed = restored.value_of("global:seed", 0)

    assert restored_seed == live_seed
    assert restored_seed["left"] is restored_seed["right"]


def _actor_model_id(model, actor_name):
    matches = [
        exec_id
        for exec_id, metadata in model["executions"].items()
        if metadata["actor"]["name"] == actor_name
    ]
    assert len(matches) == 1
    return matches[0]


def _state_model_id(model, reference, version=0):
    expected = f"{reference}[v{version}]"
    matches = [state_id for state_id in model["S"] if state_id == expected]
    assert len(matches) == 1
    return matches[0]


def _identity_maps(model):
    return (
        {identity: identity for identity in model["E"]},
        {identity: identity for identity in model["S"]},
    )


def test_forensic_validator_detects_missing_duplicate_and_cyclic_evidence(
    forensic_run,
):
    """The validator must reject several independent classes of false history."""
    import copy

    _, trace = forensic_run
    source = trace.to_model(
        enrich=True,
        incidence_format="pairs",
    )

    # 1. Remove one producer: a State is now producerless.
    missing_producer = copy.deepcopy(source)
    missing_producer["O"].pop(0)

    report = TIDE.report_model_validity(missing_producer)
    assert report["semantically_valid"] is False
    assert report["producer_count_errors"]

    # 2. Add a second producer for one State: unique creation is broken.
    duplicate_producer = copy.deepcopy(source)
    producer, state = tuple(duplicate_producer["O"][0])
    wrong_producer = next(
        exec_id
        for exec_id in duplicate_producer["E"]
        if exec_id != producer
    )
    duplicate_producer["O"].append([wrong_producer, state])

    report = TIDE.report_model_validity(duplicate_producer)
    assert report["semantically_valid"] is False
    assert report["unique_creation"] is False

    # 3. Add a backwards dependency from the first execution to a terminal
    # State. This creates an execution-ancestry cycle.
    cyclic = copy.deepcopy(source)
    seed_exec = _actor_model_id(cyclic, "seed")
    cleanup_state = _state_model_id(
        cyclic,
        "global:cleanup",
        0,
    )
    cyclic["I"].append([seed_exec, cleanup_state])

    report = TIDE.report_model_validity(cyclic)
    assert report["semantically_valid"] is False
    assert report["acyclic_ancestry"] is False


def test_forensic_comparator_detects_valid_but_false_relationships(
    forensic_run,
):
    """A structurally valid lie must still be non-conforming."""
    import copy

    _, trace = forensic_run
    source = trace.to_model(
        enrich=True,
        incidence_format="pairs",
    )
    target = copy.deepcopy(source)

    left_state = _state_model_id(
        target,
        "global:left",
        0,
    )
    right_exec = _actor_model_id(target, "right")

    target["O"] = [
        [
            right_exec if state_id == left_state else exec_id,
            state_id,
        ]
        for exec_id, state_id in target["O"]
    ]

    # Still one producer per State and still acyclic.
    target_report = TIDE.report_model_validity(target)
    assert target_report["semantically_valid"] is True

    execution_map, state_map = _identity_maps(source)
    compared = TIDE.from_model(source).compare(
        target,
        mapping="supplied",
        execution_map=execution_map,
        state_map=state_map,
    )

    assert compared.assessable is True
    assert compared.conformant is False
    assert compared["deltas"]["O_minus"]
    assert compared["deltas"]["O_plus"]


def test_forensic_dynamic_fanin_tracks_exact_expansions_and_paths():
    """Dynamic selectors must not invent or omit concrete producer States."""
    tide = TIDE()

    tide.event(
        "dynamic_seed",
        writes={
            "@alpha": "alpha",
            "@alpha_one": "alpha-one",
            "@beta": "beta",
        },
    )

    def consume_dynamic(values):
        return tuple(values)

    tide.exec(
        consume_dynamic,
        reads=["global:*alpha*:2:2"],
        writes="dynamic_result",
    )
    source = tide.trace()
    restored = TIDE.from_bundle(
        source.to_bundle().to_dict(),
    )

    for trace in (source, restored):
        consumer = next(
            record
            for record in trace.execs.values()
            if record.func_name == "consume_dynamic"
        )

        assert len(consumer.deps) == 1
        dynamic = consumer.deps[0]
        assert dynamic.type == "dynamic"

        expanded = tuple(dynamic.expanded_to)
        assert {
            (
                read.unscoped_reference,
                read.resolved_version,
            )
            for read in expanded
        } == {
            ("global:@alpha", 0),
            ("global:@alpha_one", 0),
        }

        assert all(
            read.producer_exec_id is not None
            for read in expanded
        )

        assert trace.path(
            "global:@alpha",
            consumer.exec_id,
            "reference",
            "exec_record",
        ).exists()
        assert trace.path(
            "global:@alpha_one",
            consumer.exec_id,
            "reference",
            "exec_record",
        ).exists()
        assert not trace.path(
            "global:@beta",
            consumer.exec_id,
            "reference",
            "exec_record",
        ).exists()


def test_forensic_external_undeclared_input_is_explicit_and_persistent():
    """Host literals must be represented as explicit external provenance."""
    @node(targets=["global:source"])
    def source():
        return {"value": 1}

    @node(targets=["global:nested"])
    def nested(value):
        return value

    @pipeline
    def flow():
        value = source()
        nested([value, {"plain": 2}])

    source_trace = TIDE().ocean.run_pipeline(
        flow,
        mode="sequential",
    )
    restored = TIDE.from_bundle(
        source_trace.to_bundle().to_dict(),
    )

    for trace in (source_trace, restored):
        nested_record = next(
            record
            for record in trace.execs.values()
            if record.func_name == "nested"
        )

        untracked = next(
            dependency
            for dependency in nested_record.deps
            if dependency.dep.is_autogenerated
        )

        assert (
            untracked.producer_actor.source_actor
            == "TIDE: External"
        )
        assert untracked.producer_exec_id is not None
        assert untracked.resolved_value == [
            {"value": 1},
            {"plain": 2},
        ]

        normal = trace.show(
            color=False,
            show_undeclared=False,
        )
        explicit = trace.show(
            color=False,
            show_undeclared=True,
        )

        assert "TIDE: External" not in normal
        assert "TIDE: External" in explicit
        assert "__untracked__" in explicit


def test_forensic_emitted_node_lineage_is_occurrence_exact_and_persistent():
    """Agentic/runtime-emitted work must retain who emitted what."""
    from tide_tooling import EMIT

    calls = []

    @node(targets="global:child")
    def child():
        calls.append("child")
        return "child-result"

    @node
    def emitter():
        calls.append("emitter")
        emitted = child.as_node()
        return EMIT([emitted, emitted])

    @pipeline
    def flow():
        return emitter()

    source = TIDE().ocean.run_pipeline(
        flow,
        mode="sequential",
    )
    restored = TIDE.from_bundle(
        source.to_bundle().to_dict(),
    )

    assert calls == ["emitter", "child"]

    for trace in (source, restored):
        emitter_record = next(
            record
            for record in trace.execs.values()
            if record.func_name == "emitter"
        )
        child_record = next(
            record
            for record in trace.execs.values()
            if record.func_name == "child"
        )

        assert len(emitter_record.emitted_nodes) == 2
        assert all(
            emitted.func_name == "child"
            for emitted in emitter_record.emitted_nodes
        )
        assert all(
            emitted.emitted_by_exec_id == emitter_record.exec_id
            for emitted in emitter_record.emitted_nodes
        )
        assert (
            child_record.emitted_by_exec_id
            == emitter_record.exec_id
        )

        path = trace.path(
            emitter_record.exec_id,
            child_record.exec_id,
            "exec_record",
            "exec_record",
        )
        assert path.exists()
        assert path.count() == 1

        handoffs = path.get_path()["handoffs"]
        assert len(handoffs[0]) == 2


def test_forensic_deadlock_records_waiting_without_inventing_execution():
    """Missing dependencies must be explicit absence, not phantom success."""
    from tide_tooling.tooling_core.engine.classes import Node, Pipeline
    from tide_tooling.tooling_core.engine.errors import PipelineDeadlockError

    waiting = Node(
        lambda value: value,
        args="@global:missing",
        targets="global:result",
        node_name="waiting",
    )
    tide = TIDE(timeout=1)

    try:
        trace = tide.ocean.run_pipeline(
            Pipeline([waiting], name="forensic-deadlock"),
            mode="sequential",
        )

        runner = next(iter(tide.ocean.runners.values()))

        assert trace.log == ()
        assert trace.deadlocked is True
        assert isinstance(
            runner.deadlock,
            PipelineDeadlockError,
        )
        assert "global:missing" in str(runner.deadlock)

        assert trace.waiting.to_dict()["count"] == 1
        assert not trace.find(
            "global:result"
        ).exists("reference")[0]

        restored = TIDE.from_bundle(
            trace.to_bundle().to_dict(),
        )
        assert restored.deadlock == trace.deadlock
        assert restored.waiting.to_dict() == trace.waiting.to_dict()
        assert restored.log == ()
    finally:
        tide.ocean.close()


def test_forensic_failed_execution_records_failure_without_output():
    """A failed execution may exist; its declared output State must not."""
    ocean = Ocean()

    @node(targets="global:never_written")
    def fail():
        raise RuntimeError("forensic failure")

    @pipeline
    def flow():
        fail()

    try:
        with pytest.raises(
            RuntimeError,
            match="forensic failure",
        ) as caught:
            ocean.run_pipeline(
                flow,
                mode="sequential",
            )

        trace = caught.value.tide_trace
        assert trace is not None

        failed = next(
            record
            for record in trace.execs.values()
            if record.func_name == "fail"
        )

        assert failed.status == "failed"
        assert isinstance(
            failed.error,
            RuntimeError,
        )
        assert not failed.writes

        model = trace.to_model(
            enrich=True,
            incidence_format="pairs",
        )
        assert _relation(model, "O") == set()

        assert not trace.find(
            "global:never_written"
        ).exists("reference")[0]
    finally:
        ocean.close()


def test_forensic_repeated_actor_occurrences_remain_distinct_through_bundle():
    """Actor identity must never collapse exact execution occurrences."""
    tide = TIDE()
    tide.declare("source", "present")

    def remove():
        return DELETE_NOW("global:source")

    def observe_missing():
        return READ(
            "global:source",
            fallback="missing",
        )

    tide.exec(remove)
    tide.exec(observe_missing)
    tide.exec(observe_missing)

    source = tide.trace()
    restored = TIDE.from_bundle(
        source.to_bundle().to_dict(),
    )

    for trace in (source, restored):
        observers = [
            record
            for record in trace.execs.values()
            if record.func_name == "observe_missing"
        ]
        assert len(observers) == 2

        assert len({
            record.exec_id
            for record in observers
        }) == 2
        assert len({
            record.producer_occurrence_id
            for record in observers
        }) == 2

        deletion = next(
            effect
            for record in trace.execs.values()
            for effect in record.effects
            if effect.effect == Effects.DELETE
        )

        assert set(deletion.affected) == {
            record.exec_id
            for record in observers
        }

        read_effects = [
            effect
            for record in observers
            for effect in record.effects
            if effect.effect == Effects.READ
        ]
        assert len(read_effects) == 2
        assert len({
            effect.event_id
            for effect in read_effects
        }) == 2
        assert all(
            effect.immediate is True
            for effect in read_effects
        )

        shown = trace.show(color=False)
        assert shown.count("read -> global:source") == 2


# ======================================================================
# SATURATION LAYER: executor parity, randomized graph oracle, isolation,
# structural determinism, replay idempotence, and Event envelope tamper.
# ======================================================================


def _kernel(model):
    return {
        key: model[key]
        for key in ("E", "S", "I", "O")
    }


def _structural_kernel(trace):
    return _kernel(trace.to_model(
        enrich=False,
        incidence_format="pairs",
        identity_mode="structural",
    ))


def _process_seed_forensic():
    return 10


def _process_step_forensic(value):
    return value + 1


def _process_final_forensic(value):
    return value * 2


@pytest.mark.parametrize("mode", ["sequential", "thread"])
def test_forensic_full_workflow_is_consistent_across_executor_modes(mode):
    """The same evidence contract must hold outside the sequential scheduler."""
    ocean = Ocean()
    try:
        trace = ocean.run_pipeline(
            _build_forensic_pipeline(),
            mode=mode,
        )

        _assert_cross_view_agreement(
            trace,
            check_events=True,
        )

        completed = [
            event.payload.exec_meta
            for event in trace.events
            if getattr(event, "payload", None) is not None
            and hasattr(event.payload, "exec_meta")
        ]
        assert completed
        assert {
            meta.execution_mode
            for meta in completed
        } == {mode}
    finally:
        ocean.close()


@pytest.mark.parametrize("mode", ["sequential", "thread", "process"])
def test_forensic_simple_chain_has_same_structure_in_every_execution_mode(mode):
    """Executor implementation must not change the represented dependency DAG."""
    from tide_tooling.tooling_core.engine.classes import Node, Pipeline

    first = Node(
        _process_seed_forensic,
        targets=["global:mode_seed"],
        node_name="mode_seed",
    )
    second = Node(
        _process_step_forensic,
        args="@global:mode_seed",
        targets=["global:mode_step"],
        node_name="mode_step",
    )
    third = Node(
        _process_final_forensic,
        args="@global:mode_step",
        targets=["global:mode_final"],
        node_name="mode_final",
    )

    ocean = Ocean()
    try:
        trace = ocean.run_pipeline(
            Pipeline(
                [first, second, third],
                name=f"forensic-{mode}",
            ),
            mode=mode,
        )

        assert trace.value_of("global:mode_final", 0) == 22

        seed_record = next(
            record
            for record in trace.execs.values()
            if record.func_name == "_process_seed_forensic"
        )
        step_record = next(
            record
            for record in trace.execs.values()
            if record.func_name == "_process_step_forensic"
        )
        final_record = next(
            record
            for record in trace.execs.values()
            if record.func_name == "_process_final_forensic"
        )

        assert trace.path(
            seed_record.exec_id,
            step_record.exec_id,
            "exec_record",
            "exec_record",
        ).exists()
        assert trace.path(
            step_record.exec_id,
            final_record.exec_id,
            "exec_record",
            "exec_record",
        ).exists()
        assert trace.path(
            seed_record.exec_id,
            final_record.exec_id,
            "exec_record",
            "exec_record",
        ).exists()

        model = trace.to_model(
            enrich=True,
            incidence_format="pairs",
        )
        assert TIDE.report_model_validity(model)["semantically_valid"] is True

        completed = [
            event.payload.exec_meta
            for event in trace.events
            if getattr(event, "payload", None) is not None
            and hasattr(event.payload, "exec_meta")
        ]
        assert len(completed) == 3
        assert {
            meta.execution_mode
            for meta in completed
        } == {mode}
    finally:
        ocean.close()


def _oracle_reachable(adjacency, source, target):
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


def test_forensic_random_dags_paths_models_and_bundles_match_independent_oracle():
    """Stress arbitrary acyclic shapes instead of trusting one hand-written DAG."""
    import random

    rng = random.Random(0x71DE)

    for case in range(16):
        size = 8
        adjacency = {
            source: set()
            for source in range(size)
        }

        for source in range(size):
            for target in range(source + 1, size):
                if rng.random() < 0.30:
                    adjacency[source].add(target)

        tide = TIDE()

        for actor in range(size):
            reads = [
                f"edge_{source}_{actor}"
                for source in range(actor)
                if actor in adjacency[source]
            ]
            writes = {
                f"edge_{actor}_{target}":
                    f"case={case};{actor}->{target}"
                for target in adjacency[actor]
            }

            tide.event(
                f"actor_{actor}",
                reads=reads,
                writes=writes,
            )

        source = tide.trace()
        restored = TIDE.from_bundle(
            source.to_bundle().to_dict(),
        )

        for trace in (source, restored):
            exec_ids = {
                int(record.func_name.removeprefix("actor_")):
                    record.exec_id
                for record in trace.execs.values()
            }

            model = trace.to_model(
                enrich=True,
                incidence_format="pairs",
            )
            validity = TIDE.report_model_validity(model)
            assert validity["semantically_valid"] is True

            for start in range(size):
                for end in range(size):
                    if start == end:
                        continue

                    expected = _oracle_reachable(
                        adjacency,
                        start,
                        end,
                    )

                    assert trace.path(
                        exec_ids[start],
                        exec_ids[end],
                        "exec_record",
                        "exec_record",
                    ).exists() is expected

                    assert trace.path(
                        exec_ids[end],
                        exec_ids[start],
                        "exec_record",
                        "exec_record",
                        direction="upstream",
                    ).exists() is expected


def test_forensic_repeated_runs_on_one_ocean_do_not_cross_contaminate():
    """A later Trace must never silently absorb occurrences from an earlier run."""
    ocean = Ocean()

    try:
        first = ocean.run_pipeline(
            _build_forensic_pipeline(),
            mode="sequential",
        )
        second = ocean.run_pipeline(
            _build_forensic_pipeline(),
            mode="sequential",
        )

        first_ids = {
            record.exec_id
            for record in first.execs.values()
        }
        first_event_ids = {
            event.event_id
            for event in first.events
        }
        first_runner_ids = set(first.runs)
        first_fingerprint = _fingerprint(first)

        second_ids = {
            record.exec_id
            for record in second.execs.values()
        }
        second_event_ids = {
            event.event_id
            for event in second.events
        }
        second_runner_ids = set(second.runs)

        # Re-running in the same Ocean may legitimately execute a different
        # number of occurrences because the global Store persists and updated
        # States can trigger fresh reactive work. Isolation is about occurrence
        # ownership, not equal counts.
        assert first_ids
        assert second_ids
        assert first_ids.isdisjoint(second_ids)
        assert first_event_ids.isdisjoint(second_event_ids)
        assert first_runner_ids.isdisjoint(second_runner_ids)

        # The already-returned first Trace is immutable evidence: a later run
        # cannot retroactively alter it.
        assert _fingerprint(first) == first_fingerprint

        # Each Trace index contains exactly its own reconstructed execution
        # occurrences.
        assert set(first.index.exec_by_exec_id) == first_ids
        assert set(second.index.exec_by_exec_id) == second_ids

        # Each Trace begins at its own root run boundary.
        assert first.trace_starting_event is not None
        assert second.trace_starting_event is not None
        assert (
            first.trace_starting_event.event_id
            != second.trace_starting_event.event_id
        )
        assert first.events[0].event_id == first.trace_starting_event.event_id
        assert second.events[0].event_id == second.trace_starting_event.event_id

        # A Trace query cannot resolve an execution occurrence owned only by
        # another run.
        foreign_first = next(iter(first_ids))
        foreign_second = next(iter(second_ids))

        assert second.inspect(
            foreign_first,
            "exec_record",
            grouped=False,
        ).exists is False
        assert first.inspect(
            foreign_second,
            "exec_record",
            grouped=False,
        ).exists is False
    finally:
        ocean.close()


def test_forensic_structural_identity_ignores_literal_ids_order_and_duplicates(
    forensic_run,
):
    """Canonical structure must depend on topology, not host-assigned labels."""
    import copy

    _, trace = forensic_run
    literal = trace.to_model(
        enrich=False,
        incidence_format="pairs",
    )

    execution_map = {
        old: f"renamed-exec-{index}"
        for index, old in enumerate(reversed(literal["E"]))
    }
    state_map = {
        old: f"renamed-state-{index}"
        for index, old in enumerate(reversed(literal["S"]))
    }

    renamed = {
        "E": [
            execution_map[exec_id]
            for exec_id in reversed(literal["E"])
        ],
        "S": [
            state_map[state_id]
            for state_id in reversed(literal["S"])
        ],
        "I": [
            [
                execution_map[exec_id],
                state_map[state_id],
            ]
            for exec_id, state_id in reversed(literal["I"])
        ],
        "O": [
            [
                execution_map[exec_id],
                state_map[state_id],
            ]
            for exec_id, state_id in reversed(literal["O"])
        ],
    }

    if renamed["I"]:
        renamed["I"].append(
            copy.deepcopy(renamed["I"][0])
        )
    if renamed["O"]:
        renamed["O"].append(
            copy.deepcopy(renamed["O"][0])
        )

    canonical_source = TIDE.from_model(literal).to_model(
        enrich=False,
        incidence_format="pairs",
        identity_mode="structural",
    )
    canonical_renamed = TIDE.from_model(renamed).to_model(
        enrich=False,
        incidence_format="pairs",
        identity_mode="structural",
    )

    assert _kernel(canonical_source) == _kernel(canonical_renamed)

    # Canonicalisation itself must be idempotent.
    repeated = TIDE.from_model(
        canonical_source,
        identity_mode="structural",
    ).to_model(
        enrich=False,
        incidence_format="pairs",
        identity_mode="structural",
    )
    assert _kernel(repeated) == _kernel(canonical_source)


def test_forensic_reconstructing_same_runner_is_idempotent(forensic_run):
    """Reading the same retained runtime evidence twice must not mutate truth."""
    ocean, original = forensic_run

    runner = ocean.runners[original.runs[0]]

    first = ocean._trace_from_runner(runner)
    second = ocean._trace_from_runner(runner)

    assert _fingerprint(first) == _fingerprint(second)
    assert _fingerprint(first) == _fingerprint(original)

    assert first.to_model(
        enrich=True,
        incidence_format="pairs",
    ) == second.to_model(
        enrich=True,
        incidence_format="pairs",
    )

    assert _event_fingerprint(first.events) == _event_fingerprint(
        second.events
    )


@pytest.mark.parametrize(
    "mutation",
    ["hash", "outer_count", "graph_count"],
)
def test_forensic_event_sidecar_rejects_envelope_tampering(
    forensic_run,
    mutation,
):
    """Portable raw Event evidence must reject altered integrity metadata."""
    import copy

    _, source = forensic_run
    complete = source.to_bundle(
        serialise_events=True,
    ).to_dict()
    tampered = copy.deepcopy(complete)

    events = tampered["manifest"]["events"]
    assert events is not None

    if mutation == "hash":
        events["event_log_hash"] = (
            "sha3_512_tide_event_log:"
            + "0" * 128
        )
    elif mutation == "outer_count":
        events["count"] += 1
    elif mutation == "graph_count":
        events["events"]["count"] += 1
    else:
        raise AssertionError(mutation)

    with pytest.raises((ValueError, TypeError)):
        TIDE.from_bundle(
            tampered,
            trusted=True,
        )


def test_forensic_source_and_bundle_structural_views_are_identical(
    forensic_run,
):
    """Persistence may rebuild objects, but never change canonical structure."""
    _, source = forensic_run
    restored = TIDE.from_bundle(
        source.to_bundle().to_dict(),
    )

    assert _structural_kernel(source) == _structural_kernel(restored)

