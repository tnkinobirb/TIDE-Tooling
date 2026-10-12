from __future__ import annotations

from collections import defaultdict
from copy import deepcopy

from .classes import DataRef
from .event_payloads import (
    DependencyConcretised,
    EffectOccurrence,
    EmittedNodes,
    MissingReferences,
    NodeBlocked,
    Nodes,
    NodeSnapshot,
    PipelineRegistration,
    PipelineRun,
    RunnerPlan,
    Reference,
    Result,
    State,
    Work,
)
from .events import Effects, Event, ExecutorEvents, LogEvents, OceanEvents, PipelineEvents, RouterEvents, Cause
from .tide_records import (
    Actor,
    ActorOccurrence,
    AffectedRecord,
    EffectRecord,
    ExecRecord,
    ImmediateEffectRecord,
    ReadyAttemptRecord,
)
from .utils import TIDEValueSnapshot, ensure_tide_snapshot, tide_snapshot_value
from .tide_runtime_logging import ConstructionState, ExecutionLog, snapshot_lineage
from .event_occurrences import validate_event
from .errors import ActorOccurrenceAlreadyAssignedError


def analyse_event_causality(events):
    """Describe causal corruption without participating in reconstruction."""
    positions = {
        event.event_id: index
        for index, event in enumerate(events)
    }
    missing = defaultdict(list)
    out_of_order = defaultdict(list)

    for index, event in enumerate(events):
        cause_event_id = event.cause.event_id if event.cause else None
        if cause_event_id is None:
            continue
        cause_index = positions.get(cause_event_id)
        if cause_index is None:
            missing[cause_event_id].append(event.event_id)
        elif cause_index >= index:
            out_of_order[cause_event_id].append(event.event_id)

    return dict(missing), dict(out_of_order)

def record_store_write(event: Event, state: ConstructionState):
    # Restore the written State.
    ref = event.payload.ref
    value = event.payload.value
    snapshot = event.payload.snapshot
    exec_log = state.exec_log
    target = DataRef.to_string(ref, prefix=False)
    write_snapshot = ensure_tide_snapshot(snapshot, boundary="write")

    # Resolve the Store command and the Event that requested it.
    cause_event = state.events_by_id.get(event.cause.event_id) if event.cause and event.cause.event_id else None
    command_event = (
        cause_event
        if cause_event and cause_event.type in {RouterEvents.STORE_WRITE, RouterEvents.FORCE_WRITE}
        else None
    )
    write_event = (
        state.events_by_id.get(command_event.cause.event_id)
        if command_event and command_event.cause and command_event.cause.event_id
        else cause_event
    )

    # Resolve the causal result when this is a normal node write.
    result_snapshot = (
        ensure_tide_snapshot(write_event.payload.exec_meta.snapshot, boundary="result")
        if write_event and write_event.type == PipelineEvents.RESULT_READY
        else None
    )
    mutation_observed = (
        result_snapshot is not None
        and result_snapshot.value_hash != write_snapshot.value_hash
    )
    prior_snapshot = result_snapshot

    # Advance the target State version and preserve known mutability.
    version = state.versioned_references.get(target, -1) + 1
    state.versioned_references[target] = version

    if mutation_observed:
        state.mutable_references.setdefault(target, version)

    mutable = target in state.mutable_references
    if mutable:
        write_snapshot = write_snapshot.mark_mutable()

    immediate_record = None

    # Resolve an existing completed runtime execution.
    if event.exec_id is not None and event.exec_id in exec_log.exec_id_to_record:
        exec_id = event.exec_id
        record = exec_log.exec_id_to_record[exec_id]
        runner_id = record.runner_id
        node_id = record.node_id

    # Resolve an immediate write only through its already-seen Effect Event.
    elif (
        event.exec_id is not None
        and write_event is not None
        and write_event.type == LogEvents.EFFECT
        and write_event.event_id in state.immediate_record_by_event_id
    ):
        immediate_record = state.immediate_record_by_event_id[
            write_event.event_id
        ]
        if immediate_record.exec_id != event.exec_id:
            state.evidence_violations["immediate_write_owner"].append(
                event.event_id
            )
            return
        exec_id = event.exec_id
        record = immediate_record
        attempt = state.ready_attempt_by_exec_id.get(exec_id)
        runner_id = attempt.runner_id if attempt is not None else event.source_id
        node_id = (
            attempt.node_definition_id
            if attempt is not None
            else event.source_id
        )

    # Materialise one stable placeholder for an external State occurrence.
    else:
        seq_id = len(exec_log.record_log)
        # Reuse an execution identity only when an already-seen Store command
        # established it. A consequence cannot borrow the identity of a
        # future causal Event merely because the string is present.
        grounded_exec_id = (
            event.exec_id
            if command_event is not None and event.exec_id is not None
            else event.event_id
        )
        exec_id = exec_log.log(seq_id, exec_id=grounded_exec_id)
        record = exec_log.exec_id_to_record[exec_id]
        record.event_seq_id = state.current_event_seq_id
        producer_event = command_event or event
        if event.payload.is_poll:
            runner_id = "TIDE: External"
            node_id = "TIDE: External"
        else:
            runner_id = producer_event.source
            node_id = producer_event.source_id
        exec_log._assign_external_identity(record, runner_id, node_id)
        tick_actor_occurrence_id(record, state)

    # Register the State's producer.
    producer = (runner_id, node_id, version)
    state.ref_to_last_producer[target] = producer
    state.ref_to_last_value[target] = (write_snapshot, version)
    state.producer_to_exec_id[(target, version)] = exec_id

    # Append the write to its execution.
    write_cause = (
        write_event.cause
        if write_event and write_event.type == LogEvents.EVENT_EXEC
        else command_event.cause
        if command_event
        else event.cause
    )
    if event.exec_id is not None and isinstance(record, ExecRecord):
        exec_log.append_write_log(
            exec_id,
            write_snapshot,
            target,
            version,
            write_cause,
            snapshot_lineage=snapshot_lineage(prior_snapshot, write_snapshot),
            observed_mutable=mutable,
            updated=event.type == PipelineEvents.DEP_UPDATED,
        )
    elif isinstance(record, ImmediateEffectRecord):
        exec_log.append_pending_write(
            immediate_record,
            write_snapshot,
            target,
            version,
            write_cause,
            snapshot_lineage=snapshot_lineage(prior_snapshot, write_snapshot),
            observed_mutable=mutable,
            updated=event.type == PipelineEvents.DEP_UPDATED,
        )
    else:
        exec_log.append_write_log(
            exec_id,
            write_snapshot,
            target,
            version,
            write_cause,
            snapshot_lineage=snapshot_lineage(prior_snapshot, write_snapshot),
            observed_mutable=mutable,
            updated=event.type == PipelineEvents.DEP_UPDATED,
        )

    # Retain the latest immutable State boundary.
    state.ref_to_last_snapshot[target] = write_snapshot


def read_effect(event: Event, state: ConstructionState):
    # Unpack the occurrence and its owner at this exact stream position.
    occurrence = event.payload
    exec_log = state.exec_log
    execution = exec_log.exec_id_to_record.get(event.exec_id)
    immediate_record = None
    owner = execution

    if occurrence.immediate:
        attempt = state.ready_attempt_by_exec_id.get(event.exec_id)
        prior_records = state.immediate_records_by_exec_id[event.exec_id]
        ambiguous = (
            prior_records[0].execution_reference.ambiguous_occurrence
            if prior_records
            else None
        )
        immediate_record = exec_log.create_immediate_effect_record(
            seq_id=len(exec_log.record_log),
            event_id=event.event_id,
            event_seq_id=state.current_event_seq_id,
            exec_id=event.exec_id,
            attempt=attempt,
            ambiguous_occurrence=ambiguous,
        )
        state.immediate_records_by_exec_id[event.exec_id].append(
            immediate_record
        )
        state.immediate_record_by_event_id[event.event_id] = immediate_record
        owner = immediate_record
    elif execution is None:
        state.evidence_violations["effect_without_execution"].append(
            event.event_id
        )
        return
    output = occurrence.output

    # Preserve one record even for a targetless RAISE.
    targets = occurrence.targets or (None,)

    for index, target in enumerate(targets):
        target_found = occurrence.found[index] if occurrence.found is not None else None

        # Resolve the target identity and State version.
        if occurrence.effect == Effects.WRITE:
            ref, _ = target
            target_ref = DataRef.to_string(ref, prefix=False)

            if target_found is False:
                record_target = (target_ref, None)
            else:
                version = state.versioned_references.get(target_ref, -1) + 1
                record_target = (target_ref, version)
        elif isinstance(target, DataRef):
            target_ref = DataRef.to_string(target, prefix=False)
            version = (
                None
                if target_found is False
                else state.versioned_references.get(target_ref, 0)
            )
            record_target = (target_ref, version)
        else:
            target_ref = None
            record_target = target

        # Update State evidence when a successful READ observes mutation.
        successful_read = occurrence.effect == Effects.READ and target_found is True
        read_lineage = None
        read_mutable = False

        if successful_read and target_ref in state.ref_to_last_producer:
            previous_snapshot = state.ref_to_last_snapshot.get(target_ref)
            read_snapshot = ensure_tide_snapshot(occurrence.output, boundary="read")

            if previous_snapshot is not None and previous_snapshot.value_hash != read_snapshot.value_hash:
                state.mutable_references.setdefault(target_ref, version)

            read_mutable = target_ref in state.mutable_references
            if read_mutable:
                read_snapshot = read_snapshot.mark_mutable()

            output = read_snapshot
            read_lineage = snapshot_lineage(previous_snapshot, read_snapshot)
            state.ref_to_last_snapshot[target_ref] = read_snapshot
            state.ref_to_last_value[target_ref] = (read_snapshot, version)

        # Construct and index the authoritative target-level Effect record.
        effect_record = EffectRecord(
            occurrence.effect,
            record_target,
            event_id=event.event_id,
            origin=occurrence.origin,
            output=output,
            immediate=occurrence.immediate,
            target_existed=target_found if occurrence.effect in {Effects.READ, Effects.DELETE} else None,
        )
        owner.effects.append(effect_record)
        state.effect_records_by_event_id[event.event_id].append(effect_record)

        # Attach a failed READ to the successful DELETE which removed its State.
        if occurrence.effect == Effects.READ and target_found is False:
            deleted_version = state.versioned_references.get(target_ref)
            delete_target = (target_ref, deleted_version)

            delete_record = state.delete_effect_by_ref_version.get(delete_target) if deleted_version is not None else None

            if delete_record is not None:
                delete_record.affected[event.exec_id] = AffectedRecord(
                    Effects.DELETE,
                    delete_target,
                    owner.producer_id,
                    [{"name": "runtime_read", "index": None}],
                    event.exec_id,
                )

        # Add the complete dependency produced by a successful READ.
        if successful_read and target_ref in state.ref_to_last_producer:
            producer_exec_id = state.producer_to_exec_id[(target_ref, version)]

            if immediate_record is not None:
                producer = exec_log.exec_id_to_record.get(producer_exec_id)
                producer_attempt = state.ready_attempt_by_exec_id.get(
                    producer_exec_id
                )
                exec_log.append_pending_runtime_effect_read(
                    immediate_record,
                    target_ref,
                    read_snapshot,
                    version,
                    producer_exec_id,
                    producer_source=(
                        producer.runner_id
                        if producer is not None
                        else producer_attempt.runner_id
                        if producer_attempt is not None
                        else None
                    ),
                    producer_actor=(
                        producer.producer_id
                        if producer is not None
                        else producer_attempt.actor
                        if producer_attempt is not None
                        else None
                    ),
                    snapshot_lineage=read_lineage,
                    observed_mutable=read_mutable,
                    observation_id=state.observe(),
                )
            else:
                exec_log.append_runtime_effect_reads(
                    event.exec_id,
                    target_ref,
                    read_snapshot,
                    version,
                    producer_exec_id,
                    snapshot_lineage=read_lineage,
                    observed_mutable=read_mutable,
                    observation_id=state.observe(),
                )


def read_dependency_deleted(event: Event, state: ConstructionState):
    # Resolve the exact DELETE Event and reference confirmed as removed.
    cause_event_id = event.cause.event_id
    target_ref = DataRef.to_string(event.payload.ref, prefix=False)

    # Select the matching target record from the causal Effect occurrence.
    delete_record = next(
        (
            record
            for record in state.effect_records_by_event_id.get(cause_event_id, ())
            if record.effect == Effects.DELETE and record.targets[0] == target_ref
        ),
        None,
    )
    if delete_record is not None:
        state.delete_effect_by_ref_version[delete_record.targets] = delete_record

    # Preserve historical versions and their producers, but end current
    # availability at this exact stream position. Later reads cannot borrow
    # the deleted State merely because it existed earlier in history.
    state.ref_to_last_producer.pop(target_ref, None)
    state.ref_to_last_value.pop(target_ref, None)
    state.ref_to_last_snapshot.pop(target_ref, None)


def read_node_blocked(event: Event, state: ConstructionState):
    # NODE_BLOCKED names the readiness attempt which did not execute.  Its
    # cause independently records why that transition occurred.
    if event.exec_id is not None:
        state.blocked_exec_ids.add(event.exec_id)
        state.ready_exec_ids.discard(event.exec_id)
        attempt = state.ready_attempt_by_exec_id.get(event.exec_id)
        if attempt is not None:
            attempt.outcome = "blocked"


def read_missing_references(event: Event, state: ConstructionState):
    # Failed input resolution is another explicit terminal readiness outcome.
    if event.exec_id is not None:
        state.blocked_exec_ids.add(event.exec_id)
        state.ready_exec_ids.discard(event.exec_id)
        attempt = state.ready_attempt_by_exec_id.get(event.exec_id)
        if attempt is not None:
            attempt.outcome = "missing_references"


def read_kill_runner(event: Event, state: ConstructionState):
    if event.target_runner_id is not None:
        state.ended_runner_ids.add(event.target_runner_id)


def read_runner_deadlock(event: Event, state: ConstructionState):
    # Record the complete waiting-definition snapshot from this settlement.
    # It describes no execution and changes no State.
    state.exec_log.deadlock[event.source_id] = {
        node_definition_id: tuple(dependencies)
        for node_definition_id, dependencies in event.payload.items()
    }


def read_runner_plan(event: Event, state: ConstructionState):
    # Retain the exact plan and the explicit Runner-start evidence.
    state.exec_log.runner_plans[event.source_id] = deepcopy(
        event.payload.tide_plan
    )
    state.started_runner_ids.add(event.source_id)
    if state.current_event_in_root_run_tree:
        state.causal_runner_ids.add(event.source_id)


def read_node_ready(event: Event, state: ConstructionState):
    snapshot = event.payload.node_snapshot
    if event.exec_id is not None:
        if event.exec_id in state.ready_attempt_by_exec_id:
            state.evidence_violations["duplicate_node_ready"].append(
                event.event_id
            )
        else:
            actor = Actor(
                state.exec_log.ocean_id,
                snapshot["runner_id"],
                snapshot["func_name"],
            )
            state.ready_attempt_by_exec_id[event.exec_id] = ReadyAttemptRecord(
                event_id=event.event_id,
                event_seq_id=state.current_event_seq_id,
                exec_id=event.exec_id,
                actor=actor,
                node_definition_id=snapshot["base_node_id"],
                runner_id=snapshot["runner_id"],
                pipeline_id=snapshot["pipeline_id"],
                node_snapshot=deepcopy(snapshot),
            )

    # Build the declared input shape at the dispatch boundary.
    inputs, dependencies = state.exec_log.create_input_log(
        snapshot
    )

    # Resolve factual read receipts only against State already seen at this
    # exact point in the Event stream. Missing State evidence remains
    # explicitly producerless and is never repaired by a later Event.
    observations = []
    for receipt in event.payload.node_snapshot["reads"]:
        reference = receipt["reference"]
        prior_value = state.ref_to_last_value.get(reference)
        version = prior_value[1] if prior_value is not None else None
        producer_exec_id = (
            state.producer_to_exec_id.get((reference, version))
            if version is not None
            else None
        )
        producer = state.exec_log.exec_id_to_record.get(producer_exec_id)
        producer_attempt = state.ready_attempt_by_exec_id.get(
            producer_exec_id
        )
        previous_snapshot = state.ref_to_last_snapshot.get(reference)
        read_snapshot = ensure_tide_snapshot(receipt["snapshot"], boundary="read")
        observed_mutable = (
            previous_snapshot is not None
            and previous_snapshot.value_hash != read_snapshot.value_hash
        )

        if observed_mutable:
            state.mutable_references.setdefault(reference, version)
            read_snapshot = read_snapshot.mark_mutable()

        if prior_value is not None:
            state.ref_to_last_snapshot[reference] = read_snapshot
            state.ref_to_last_value[reference] = (read_snapshot, version)
        observations.append({
            "input_name": receipt["input_name"],
            "reference": reference,
            "value": read_snapshot,
            "version": version,
            "producer_exec_id": producer_exec_id,
            "producer_source": (
                producer.runner_id
                if producer is not None
                else producer_attempt.runner_id
                if producer_attempt is not None
                else None
            ),
            "producer_actor": (
                producer.producer_id
                if producer is not None
                else producer_attempt.actor
                if producer_attempt is not None
                else None
            ),
            "input_path": receipt["input_path"],
            "snapshot_lineage": snapshot_lineage(previous_snapshot, read_snapshot),
            "observed_mutable": reference in state.mutable_references,
            "observation_id": state.observe(),
        })

    # Append the reads actually realised while resolving those inputs.
    state.exec_log.append_runtime_reads(inputs, dependencies, observations)
    state.ready_inputs_by_event_id[event.event_id] = (
        event.exec_id,
        inputs,
        dependencies,
    )
    if event.exec_id is not None:
        state.ready_exec_ids.add(event.exec_id)

def tick_actor_occurrence_id(exec_record: ExecRecord, state: ConstructionState) -> None:
    if isinstance(exec_record.producer_occurrence_id, ActorOccurrence):
        raise ActorOccurrenceAlreadyAssignedError(f"Execution has already been assigned an ActorOccurrence: {exec_record.exec_id}", exec_record_id=exec_record.exec_id)

    actor_name = exec_record.producer_id.source_actor
    state.actor_occurrence_id[actor_name] += 1
    occurrence_id = ActorOccurrence(
        exec_record.producer_id,
        int(state.actor_occurrence_id[actor_name])
        )

    exec_record.producer_occurrence_id = occurrence_id

def read_result_ready(event: Event, state: ConstructionState):
    # Consume only the already-seen NODE_READY Event named by this result.
    ready = state.ready_inputs_by_event_id.get(
        event.cause.event_id if event.cause else None
    )
    if ready is not None and ready[0] == event.exec_id:
        _, inputs, dependencies = ready
    else:
        # A corrupt/out-of-order result still proves an execution occurred,
        # but cannot borrow versions or producers from a future NODE_READY.
        inputs, dependencies = state.exec_log.create_input_log(
            event.payload.exec_meta.node_snapshot
        )
        observations = []
        for receipt in event.payload.exec_meta.node_snapshot.get("reads", ()):
            observations.append({
                "input_name": receipt["input_name"],
                "reference": receipt["reference"],
                "value": ensure_tide_snapshot(
                    receipt["snapshot"],
                    boundary="read",
                ),
                "version": None,
                "producer_exec_id": None,
                "input_path": receipt["input_path"],
                "snapshot_lineage": [],
                "observed_mutable": False,
                "observation_id": state.observe(),
            })
        state.exec_log.append_runtime_reads(
            inputs,
            dependencies,
            observations,
        )

    # Register the completed execution in occurrence order.
    seq_id = len(state.exec_log.record_log)
    
    state.exec_log.log(seq_id, event.payload.exec_meta, inputs, dependencies)
    execution: ExecRecord = state.exec_log.exec_id_to_record[event.exec_id]
    execution.event_seq_id = state.current_event_seq_id

    tick_actor_occurrence_id(execution, state)
    state.ready_exec_ids.discard(event.exec_id)
    attempt = state.ready_attempt_by_exec_id.get(event.exec_id)
    if attempt is not None:
        attempt.outcome = "completed"

    # Resolve each earlier immediate window through this explicit result only.
    for immediate_record in state.immediate_records_by_exec_id.get(
        event.exec_id,
        (),
    ):
        try:
            state.exec_log.resolve_immediate_effect_occurrence_reference(
                immediate_record,
                execution,
            )
        except ValueError:
            state.evidence_violations["immediate_owner_mismatch"].append(
                immediate_record.event_id
            )

    # Preserve the exact post-execution read snapshots carried by RESULT_READY.
    post_snapshots = {
        (
            entry["input_name"],
            tuple(entry.get("input_path", ())),
            entry["reference"],
        ): ensure_tide_snapshot(
            entry["snapshot"],
            boundary="post_execution",
        )
        for entry in event.payload.exec_meta.post_read_snapshots
    }

    for dependency in dependencies:
        occurrences = dependency.expanded_to or () if dependency.type == "dynamic" else (dependency,)
        for occurrence in occurrences:
            key = (
                occurrence.associated_input[0],
                tuple(occurrence.input_path or ()),
                occurrence.unscoped_reference,
            )
            post_snapshot = post_snapshots.get(key)
            if post_snapshot is None:
                continue

            # The NODE_READY reader already captured the value which this
            # dependency actually observed.  Reuse that evidence here rather
            # than restoring and capturing it a second time: a second capture
            # can change exactness/trust metadata and would create a second
            # authority for the same observation.
            read_snapshot = occurrence._tide_snapshot("resolved_value")
            if read_snapshot is None:
                read_snapshot = ensure_tide_snapshot(
                    occurrence.resolved_value,
                    boundary="read",
                )
            if read_snapshot.value_hash == post_snapshot.value_hash:
                continue

            post_snapshot = post_snapshot.mark_mutable()
            occurrence.observed_mutable = True
            occurrence.snapshot_lineage = snapshot_lineage(read_snapshot, post_snapshot)
            input_name = occurrence.associated_input[0]
            mutated_inputs = execution.mutated_during.setdefault(occurrence.unscoped_reference, [])
            if input_name not in mutated_inputs:
                mutated_inputs.append(input_name)
            state.mutable_references.setdefault(occurrence.unscoped_reference, occurrence.resolved_version)
            if event.payload.exec_meta.execution_mode != "process":
                state.ref_to_last_snapshot[occurrence.unscoped_reference] = post_snapshot
                state.ref_to_last_value[occurrence.unscoped_reference] = (
                    post_snapshot,
                    occurrence.resolved_version,
                )


def read_node_emitted(event: Event, state: ConstructionState):
    # Resolve origin only from the already-seen causal Effect. A late cause is
    # corruption metadata and never repairs this occurrence.
    effect_event = state.events_by_id.get(
        event.cause.event_id if event.cause else None
    )
    origin = (
        effect_event.payload.origin
        if effect_event is not None
        and effect_event.type == LogEvents.EFFECT
        else "unknown"
    )

    # Emission is definition evidence. It never waits for dependency State.
    if event.exec_id not in state.exec_log.exec_id_to_record:
        return
    state.exec_log.append_emitted_nodes(
        event.exec_id,
        event.payload.emitted_node_snapshots,
        origin,
    )


def read_dependency_state(event: Event, state: ConstructionState):
    # Availability-only evidence says only that an existing State was
    # available. It is neither a stored-value occurrence nor an input-use
    # observation, so it cannot change the reconstructed State value.
    target = DataRef.to_string(event.payload.ref, prefix=False)
    previous_snapshot = state.ref_to_last_snapshot.get(target)
    if event.payload.is_poll and previous_snapshot is not None:
        poll_snapshot = ensure_tide_snapshot(
            event.payload.snapshot,
            boundary="write",
        )
        if previous_snapshot.value_hash == poll_snapshot.value_hash:
            return

    # The dependency Event itself proves availability. Its already-seen cause
    # may attribute that fact, but an absent or unusual cause never delays it.
    record_store_write(event, state)


def read_event_exec(event: Event, state: ConstructionState):
    exec_log = state.exec_log

    # Never silently discard a second occurrence claiming the same identity.
    if event.exec_id in exec_log.exec_id_to_record:
        raise ValueError(f"Duplicate execution: {event.exec_id}")

    # Isolate mutable record evidence while retaining shared immutable NOW metadata.
    now_meta = event.payload.now_meta
    memo = {id(now_meta): now_meta} if now_meta is not None else None
    record = deepcopy(event.payload, memo)

    # Resolve every observed read against the State available at this Event.
    for dependency in record.deps:
        dep_use_records = (
            dependency.expanded_to or ()
            if dependency.type == "dynamic"
            else (dependency,)
        )

        for dep_use_record in dep_use_records:
            reference = dep_use_record.unscoped_reference

            read_snapshot = ensure_tide_snapshot(
                dep_use_record.resolved_value,
                boundary="read",
            )

            prior_value = state.ref_to_last_value.get(reference)

            if prior_value is None:
                # EVENT_EXEC proves that this value was observed, but the
                # execution did not produce it. Materialise the first known
                # State under the existing external producer identity.
                version = state.versioned_references.get(reference, -1) + 1
                external_exec_id = (
                    f"TIDE: External:{event.event_id}:{reference}"
                )

                external_seq_id = len(exec_log.record_log)
                external_exec_id = exec_log.log(
                    external_seq_id,
                    exec_id=external_exec_id,
                )
                external_record = exec_log.exec_id_to_record[
                    external_exec_id
                ]
                external_record.event_seq_id = state.current_event_seq_id

                exec_log._assign_external_identity(
                    external_record,
                    "TIDE: External",
                    "TIDE: External",
                )
                tick_actor_occurrence_id(external_record, state)

                write_snapshot = read_snapshot.at_boundary("write")

                state.versioned_references[reference] = version
                state.ref_to_last_producer[reference] = (
                    "TIDE: External",
                    "TIDE: External",
                    version,
                )
                state.ref_to_last_value[reference] = (
                    write_snapshot,
                    version,
                )
                state.ref_to_last_snapshot[reference] = write_snapshot
                state.producer_to_exec_id[
                    (reference, version)
                ] = external_exec_id

                exec_log.append_write_log(
                    external_exec_id,
                    write_snapshot,
                    reference,
                    version,
                    Cause(description="TIDE: observed value"),
                    snapshot_lineage=snapshot_lineage(
                        None,
                        write_snapshot,
                    ),
                    observed_mutable=False,
                    updated=False,
                )

                dep_use_record.resolved_value = read_snapshot
                dep_use_record.resolved_version = version
                dep_use_record.producer_source = (
                    external_record.runner_id
                )
                dep_use_record.producer_actor = (
                    external_record.producer_id
                )
                dep_use_record.producer_exec_id = external_exec_id
                dep_use_record.snapshot_lineage = snapshot_lineage(
                    write_snapshot,
                    read_snapshot,
                )
                dep_use_record.observed_mutable = False
                dep_use_record.observation_id = state.observe()

                # The read snapshot is now the latest observed boundary for
                # this same State/version.
                state.ref_to_last_snapshot[reference] = read_snapshot
                state.ref_to_last_value[reference] = (
                    read_snapshot,
                    version,
                )
                continue

            _, version = prior_value
            producer_exec_id = state.producer_to_exec_id[
                (reference, version)
            ]
            producer = exec_log.exec_id_to_record[producer_exec_id]
            previous_snapshot = state.ref_to_last_snapshot[reference]

            observed_state_transition = (
                previous_snapshot.value_hash != read_snapshot.value_hash
            )

            if observed_state_transition:
                # A changed observation is a later State occurrence, not a
                # mutation of the already-recorded State version. Restore the
                # old append_observed_mutation() semantics inside generic
                # reconstruction.
                version = state.versioned_references.get(reference, -1) + 1
                observer_exec_id = (
                    f"TIDE_OBSERVER:{event.event_id}:{reference}:v{version}"
                )
                observer_seq_id = len(exec_log.record_log)
                observer_exec_id = exec_log.log(
                    observer_seq_id,
                    exec_id=observer_exec_id,
                )
                observer_record = exec_log.exec_id_to_record[
                    observer_exec_id
                ]
                observer_record.event_seq_id = state.current_event_seq_id

                exec_log._assign_external_identity(
                    observer_record,
                    "TIDE_OBSERVER",
                    "observed_mutation",
                )
                tick_actor_occurrence_id(observer_record, state)

                write_snapshot = read_snapshot.at_boundary(
                    "write"
                ).mark_mutable()
                lineage = snapshot_lineage(
                    previous_snapshot,
                    write_snapshot,
                )

                state.versioned_references[reference] = version
                state.mutable_references.setdefault(reference, version)
                state.ref_to_last_producer[reference] = (
                    "TIDE_OBSERVER",
                    "observed_mutation",
                    version,
                )
                state.ref_to_last_value[reference] = (
                    write_snapshot,
                    version,
                )
                state.ref_to_last_snapshot[reference] = write_snapshot
                state.producer_to_exec_id[
                    (reference, version)
                ] = observer_exec_id

                exec_log.append_write_log(
                    observer_exec_id,
                    write_snapshot,
                    reference,
                    version,
                    Cause(
                        description="Observed mutable change before read"
                    ),
                    snapshot_lineage=lineage,
                    observed_mutable=True,
                    updated=True,
                )

                producer_exec_id = observer_exec_id
                producer = observer_record
                read_snapshot = read_snapshot.mark_mutable()
            elif reference in state.mutable_references:
                read_snapshot = read_snapshot.mark_mutable()

            dep_use_record.resolved_value = read_snapshot
            dep_use_record.resolved_version = version
            dep_use_record.producer_source = producer.runner_id
            dep_use_record.producer_actor = producer.producer_id
            dep_use_record.producer_exec_id = producer_exec_id

            # The observer write owns the old-State -> new-State transition.
            # This read observes the newly established State; it must not
            # claim a second in-State mutation.
            if observed_state_transition:
                dep_use_record.snapshot_lineage = snapshot_lineage(
                    write_snapshot,
                    read_snapshot,
                )
                dep_use_record.observed_mutable = False
            else:
                dep_use_record.snapshot_lineage = snapshot_lineage(
                    previous_snapshot,
                    read_snapshot,
                )
                dep_use_record.observed_mutable = (
                    reference in state.mutable_references
                )
            dep_use_record.observation_id = state.observe()

            state.ref_to_last_snapshot[reference] = read_snapshot
            state.ref_to_last_value[reference] = (
                read_snapshot,
                version,
            )

    record.seq_id = len(exec_log.record_log)
    record.event_seq_id = state.current_event_seq_id
    tick_actor_occurrence_id(record, state)
    exec_log.exec_id_to_record[record.exec_id] = record
    exec_log.exec_log.append(record)
    exec_log.record_log.append(record)

    # EVENT_EXEC is itself completed execution evidence. Imported sources may
    # carry recorded writes directly rather than repeat Store consequence
    # Events, so seed the forward-fold State index from those exact writes.
    for write in record.writes:
        target = write.stored_target
        version = write.stored_version
        existing = state.producer_to_exec_id.get((target, version))

        if existing is not None and existing != record.exec_id:
            raise ValueError(
                f"State {target}[v{version}] has multiple Event producers"
            )

        write_snapshot = ensure_tide_snapshot(
            write.stored_result,
            boundary="write",
        )
        state.versioned_references[target] = max(
            state.versioned_references.get(target, -1),
            version,
        )
        state.ref_to_last_producer[target] = (
            record.runner_id,
            record.node_id,
            version,
        )
        state.ref_to_last_value[target] = (
            write_snapshot,
            version,
        )
        state.ref_to_last_snapshot[target] = write_snapshot
        state.producer_to_exec_id[
            (target, version)
        ] = record.exec_id



class EventLogReader:
    def __init__(self, ocean_id):
        self.ocean_id = ocean_id

    READERS = {
        OceanEvents.KILL_RUNNER: read_kill_runner,
        PipelineEvents.DEP_AVAILABLE: read_dependency_state,
        PipelineEvents.DEP_UPDATED: read_dependency_state,
        PipelineEvents.DEP_DELETED: read_dependency_deleted,
        LogEvents.NODE_BLOCKED: read_node_blocked,
        LogEvents.RUNNER_DEADLOCK: read_runner_deadlock,
        LogEvents.RUNNER_PLAN: read_runner_plan,
        LogEvents.MISSING_REFS: read_missing_references,
        ExecutorEvents.NODE_READY: read_node_ready,
        PipelineEvents.RESULT_READY: read_result_ready,
        LogEvents.EFFECT: read_effect,
        LogEvents.EVENT_EXEC: read_event_exec,
        LogEvents.NODE_EMITTED: read_node_emitted,
    }

    def _construct_log(self, event_log):
        # Initialise reconstruction-only state.
        state = ConstructionState(exec_log=ExecutionLog(self.ocean_id))
        events = tuple(event_log)
        if events:
            root = events[0]
            if (
                root.type == OceanEvents.RUN_PIPELINE
                and root.source == "ocean"
                and root.source_id == root.ocean_id
                and root.exec_id is None
            ):
                state.root_run_event_id = root.event_id

        # Apply each occurrence once in original order. Reconstruction has no
        # knowledge of future Event identities.
        for event_seq_id, event in enumerate(events):
            state.current_event_seq_id = event_seq_id
            state.current_event_in_root_run_tree = (
                event.event_id == state.root_run_event_id
                or event.cause.event_id in state.root_run_event_ids
            )
            if state.current_event_in_root_run_tree:
                state.root_run_event_ids.add(event.event_id)
            if event.ocean_id != self.ocean_id:
                raise ValueError(
                    "Event stream ocean identity disagrees with its reader boundary: "
                    f"{event.ocean_id!r} != {self.ocean_id!r}"
                )
            validate_event(event)
            if event.event_id in state.events_by_id:
                raise ValueError(f"Duplicate Event occurrence: {event.event_id}")
            state.events_by_id[event.event_id] = event

            reader = self.READERS.get(event.type)
            if reader is not None:
                reader(event, state)

        # Analyse corruption independently after the forward fold. These maps
        # are presentation metadata and cannot affect any reconstructed fact.
        (
            state.missing_referenced_events,
            state.causal_order_violations,
        ) = analyse_event_causality(events)

        # Expose the completed reconstruction.
        state.exec_log.missing_referenced_events = dict(state.missing_referenced_events)
        state.exec_log.causal_order_violations = dict(state.causal_order_violations)
        state.exec_log.ready_exec_ids = frozenset(state.ready_exec_ids)
        state.exec_log.blocked_exec_ids = frozenset(state.blocked_exec_ids)
        state.exec_log.ready_attempts = tuple(
            state.ready_attempt_by_exec_id.values()
        )
        state.exec_log.started_runner_ids = frozenset(
            state.started_runner_ids
        )
        state.exec_log.causal_runner_ids = frozenset(
            state.causal_runner_ids
        )
        state.exec_log.ended_runner_ids = frozenset(state.ended_runner_ids)
        state.exec_log.evidence_violations = {
            key: tuple(values)
            for key, values in state.evidence_violations.items()
        }

 
        return state
