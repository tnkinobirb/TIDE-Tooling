from __future__ import annotations

"""Mutable machinery used internally while folding Events."""

from dataclasses import dataclass, field
from typing import Any

import copy
from collections import defaultdict

from tide_tooling.tooling_core.engine.classes import (
    DataRef,
    __fallback__,
)
from tide_tooling.tooling_core.engine.errors import MissingExecutionEvidenceError
from tide_tooling.tooling_core.engine.events import Event
from tide_tooling.tooling_core.engine.tide_records import (
    Actor,
    AmbiguousActorOccurrence,
    DependencyUseRecord,
    EffectRecord,
    EmittedNodeRecord,
    ExecRecord,
    ExpandedDepRecord,
    InputUseRecord,
    Inputs,
    ImmediateEffectRecord,
    ImmediateExecutionReference,
    ReadyAttemptRecord,
    WriteRecord,
)
from tide_tooling.tooling_core.engine.utils import (
    TIDEValueSnapshot,
    create_tide_exec_uuid,
    ensure_tide_snapshot,
)


def snapshot_lineage(*snapshots):
    # Retain each available immutable snapshot in boundary order.
    return [snapshot.to_dict() for snapshot in snapshots if isinstance(snapshot, TIDEValueSnapshot)]


@dataclass
class ConstructionState:
    # Hold the ExecutionLog currently being constructed.
    exec_log: ExecutionLog

    # Resolve exact causal Events.
    events_by_id: dict[str, Event] = field(default_factory=dict)

    # Report causal Event references absent from the completed stream.
    missing_referenced_events: dict[str, list[str]] = field(default_factory=lambda: defaultdict(list))

    # Report causal Events which occur after their consequences.
    causal_order_violations: dict[str, list[str]] = field(default_factory=lambda: defaultdict(list))

    # Actor occurrence tracking
    actor_occurrence_id: str = field(default_factory=lambda: defaultdict(lambda: -1))

    # Resolve Effect occurrences and successful deletion consequences.
    effect_records_by_event_id: dict[str, list[EffectRecord]] = field(default_factory=lambda: defaultdict(list))
    delete_effect_by_ref_version: dict[tuple[str, int], EffectRecord] = field(default_factory=dict)

    # Track reference versions and observed mutability.
    versioned_references: dict[str, int] = field(default_factory=dict)
    mutable_references: dict[str, int] = field(default_factory=dict)

    # Track the latest State and its producing execution.
    producer_to_exec_id: dict[tuple[str, int], str] = field(default_factory=dict)
    ref_to_last_producer: dict[str, tuple[Any, Any, int]] = field(default_factory=dict)
    ref_to_last_value: dict[str, tuple[Any, int]] = field(default_factory=dict)
    ref_to_last_snapshot: dict[str, TIDEValueSnapshot] = field(default_factory=dict)

    # Index readiness evidence by its own Event identity. RESULT_READY may
    # consume only the already-seen Event named by its cause.
    ready_inputs_by_event_id: dict[
        str,
        tuple[str, Inputs, list[DependencyUseRecord]],
    ] = field(default_factory=dict)

    # Preserve explicit readiness outcomes without inventing executions.
    ready_exec_ids: set[str] = field(default_factory=set)
    blocked_exec_ids: set[str] = field(default_factory=set)

    # Retain readiness and immediate-occurrence ownership in stream order.
    ready_attempt_by_exec_id: dict[str, ReadyAttemptRecord] = field(default_factory=dict)
    immediate_records_by_exec_id: dict[str, list[ImmediateEffectRecord]] = field(default_factory=lambda: defaultdict(list))
    immediate_record_by_event_id: dict[str, ImmediateEffectRecord] = field(default_factory=dict)

    # Retain the exact Event position currently being reduced.
    current_event_seq_id: int = -1

    # Derive runner scope only from start/end Events.
    started_runner_ids: set[Any] = field(default_factory=set)
    causal_runner_ids: set[Any] = field(default_factory=set)
    ended_runner_ids: set[Any] = field(default_factory=set)
    root_run_event_id: str | None = None
    root_run_event_ids: set[str] = field(default_factory=set)
    current_event_in_root_run_tree: bool = False

    # Retain non-causal evidence contradictions without repairing facts.
    evidence_violations: dict[str, list[str]] = field(default_factory=lambda: defaultdict(list))

    # Assign factual read-observation order while consuming the Event stream.
    # This is deliberately independent from execution completion ``seq_id``.
    next_observation_id: int = 0

    def observe(self) -> int:
        observation_id = self.next_observation_id
        self.next_observation_id += 1
        return observation_id


class ExecutionLog:
    def __init__(self, ocean_id):
        # Store completed executions by order and identity.
        self.ocean_id = ocean_id
        self.exec_log: list[ExecRecord] = []
        self.record_log: list[ExecRecord | ImmediateEffectRecord] = []
        self.exec_id_to_record: dict[str, ExecRecord] = {}
        self.missing_referenced_events: dict[str, list[str]] = {}
        self.causal_order_violations: dict[str, list[str]] = {}
        self.ready_exec_ids: frozenset[str] = frozenset()
        self.blocked_exec_ids: frozenset[str] = frozenset()
        self.deadlock = {}
        self.runner_plans = {}
        self.ready_attempts = ()
        self.started_runner_ids = frozenset()
        self.causal_runner_ids = frozenset()
        self.ended_runner_ids = frozenset()
        self.evidence_violations = {}

    def _producer_actor(self, producer_exec_id):
        # Resolve the Actor represented by the producing execution.
        return self.exec_id_to_record[producer_exec_id].producer_id

    @staticmethod
    def _restore_execution_evidence(value):
        # Isolate restored values from their frozen Event evidence.
        if isinstance(value, TIDEValueSnapshot):
            return value.restore()
        return copy.deepcopy(value)

    def create_input_log(self, node_snapshot):
        """Build dispatch evidence without consulting future or live State.

        The node snapshot establishes declared input/dependency structure.
        Exact State versions and producers are applied later from read
        receipts against the reducer's already-seen State history.
        """
        # Find the concrete references captured for one dynamic dependency.
        def expanded_dependencies(dynamic_dependency):
            for expansion in node_snapshot.get("dynamic_dependencies", ()):
                dependency = self._restore_execution_evidence(expansion["dependency"])
                if dependency == dynamic_dependency:
                    return expansion.get("expanded", ())
            return ()

        input_records = {}
        all_dependencies = []

        # Restore each frozen input boundary.
        for name, captured_input in node_snapshot.get("inputs", {}).items():
            # Input values already are immutable value-graph snapshots.  Keep
            # those carriers through reconstruction so Trace sealing does not
            # serialize the same potentially-large value graph again.
            raw = captured_input["raw"]
            evaluated = captured_input["evaluated"]
            normalised = captured_input["normalised"]
            resolved = captured_input["resolved"]

            input_record = InputUseRecord(name, raw, evaluated, normalised, resolved, [])
            associated_input = (name, input_record)
            dependencies = []

            # Reconstruct each declared dependency occurrence.
            for input_index, captured_dependency in enumerate(captured_input.get("dependencies", ())):
                dependency = self._restore_execution_evidence(captured_dependency["dependency"])
                reference = captured_dependency["unscoped_reference"]

                # Preserve one dynamic dependency and its concrete expansions.
                if dependency.dynamic:
                    expanded_records = []

                    for captured_expansion in expanded_dependencies(dependency):
                        expanded_dependency = self._restore_execution_evidence(captured_expansion["dependency"])
                        expanded_reference = captured_expansion["unscoped_reference"]

                        expanded_records.append(ExpandedDepRecord(
                            associated_input=associated_input,
                            dep=expanded_dependency,
                            type="static",
                            unscoped_ref=expanded_reference,
                            resolved_val=__fallback__(),
                            resolved_ver=None,
                            producer_source=None,
                            producer_actor=None,
                            producer_exec_id=None,
                            expanded_from=dependency,
                            input_index=input_index,
                        ))

                    dependency_record = DependencyUseRecord(
                        associated_input=associated_input,
                        dep=dependency,
                        type="dynamic",
                        unscoped_ref=reference,
                        expanded_to=expanded_records,
                        input_index=input_index,
                    )
                    dependencies.append(dependency_record)
                    continue

                dependencies.append(DependencyUseRecord(
                    associated_input=associated_input,
                    dep=dependency,
                    type="static",
                    unscoped_ref=reference,
                    resolved_val=__fallback__(),
                    resolved_ver=None,
                    producer_source=None,
                    producer_actor=None,
                    producer_exec_id=None,
                    input_index=input_index,
                ))

            # Attach the records to both input-level and execution-level views.
            input_record.dependencies = dependencies
            input_records[name] = input_record
            all_dependencies.extend(dependencies)

        return Inputs(input_records), all_dependencies

    def append_runtime_effect_reads(
        self,
        exec_id,
        reference,
        value,
        version,
        producer_exec_id,
        snapshot_lineage=None,
        observed_mutable=False,
        observation_id=None,
    ):
        # Resolve the consuming and producing executions.
        execution = self.exec_id_to_record[exec_id]
        producer = self.exec_id_to_record[producer_exec_id]

        # Create the shared runtime-read input when first needed.
        input_name = "runtime_read"
        input_record = getattr(execution.inputs, input_name, None)

        if input_record is None:
            input_record = InputUseRecord(
                name=input_name,
                raw=None,
                evaluated=None,
                normalised=None,
                resolved=None,
                dependencies=[],
            )
            setattr(execution.inputs, input_name, input_record)

        # Append the exact observed dependency.
        dependency = DependencyUseRecord(
            associated_input=(input_name, input_record),
            dep=DataRef.from_string(reference, prefix=False),
            type="static",
            unscoped_ref=reference,
            resolved_val=value,
            resolved_ver=version,
            producer_source=producer.runner_id,
            producer_actor=producer.producer_id,
            producer_exec_id=producer_exec_id,
            input_index=len(input_record.dependencies),
            snapshot_lineage=snapshot_lineage,
            observed_mutable=observed_mutable,
            observation_id=observation_id,
        )
        input_record.dependencies.append(dependency)
        execution.deps.append(dependency)

    @staticmethod
    def append_pending_runtime_effect_read(
        immediate_record,
        reference,
        value,
        version,
        producer_exec_id,
        *,
        producer_source=None,
        producer_actor=None,
        snapshot_lineage=None,
        observed_mutable=False,
        observation_id=None,
    ):
        dependency = DependencyUseRecord(
            associated_input=None,
            dep=DataRef.from_string(reference, prefix=False),
            type="static",
            unscoped_ref=reference,
            resolved_val=value,
            resolved_ver=version,
            producer_source=producer_source,
            producer_actor=producer_actor,
            producer_exec_id=producer_exec_id,
            input_index=len(immediate_record.deps),
            snapshot_lineage=snapshot_lineage,
            observed_mutable=observed_mutable,
            observation_id=observation_id,
        )
        immediate_record.deps.append(dependency)
        return dependency

    def resolve_immediate_effect_occurrence_reference(
        self,
        immediate_record,
        execution,
    ):
        """Resolve one chronological Effect window to its completed owner.

        This is the only operation allowed to replace provisional ownership.
        The window's own sequence position and retained evidence never move.
        """
        reference = immediate_record.execution_reference
        if reference.exec_id != execution.exec_id:
            raise ValueError(
                "Immediate Effect owner mismatch: "
                f"{reference.exec_id} != {execution.exec_id}"
            )
        if reference.resolved:
            if (
                reference.resolved_seq_id != execution.seq_id
                or reference.resolved_actor_occurrence
                != execution.producer_occurrence_id
            ):
                raise ValueError(
                    f"Immediate Effect {immediate_record.event_id} "
                    "was resolved to a different execution"
                )
            return
        if reference.actor is not None and reference.actor != execution.producer_id:
            raise ValueError(
                f"Immediate Effect {immediate_record.event_id} "
                "contradicts its NODE_READY Actor"
            )

        reference.resolved_seq_id = execution.seq_id
        reference.resolved_actor_occurrence = execution.producer_occurrence_id

        execution.effects.extend(immediate_record.effects)
        execution.writes.extend(immediate_record.writes)
        for write in immediate_record.writes:
            runtime_target = DataRef.from_string(
                write.stored_target,
                prefix=False,
            )
            if runtime_target not in execution.runtime_targets:
                execution.runtime_targets.append(runtime_target)

        if immediate_record.deps:
            input_name = "runtime_read"
            input_record = getattr(execution.inputs, input_name, None)
            if input_record is None:
                input_record = InputUseRecord(
                    name=input_name,
                    raw=None,
                    evaluated=None,
                    normalised=None,
                    resolved=None,
                    dependencies=[],
                )
                setattr(execution.inputs, input_name, input_record)

            for dependency in immediate_record.deps:
                dependency.associated_input = (input_name, input_record)
                dependency.input_index = len(input_record.dependencies)
                input_record.dependencies.append(dependency)
                execution.deps.append(dependency)

    def append_runtime_reads(self, inputs, dependencies, observations):
        consumed = set()

        # Apply each actual read occurrence to its input record.
        for observation in observations:
            input_name = observation["input_name"]
            reference = observation["reference"]
            input_record = getattr(inputs, input_name, None)

            if input_record is None:
                input_record = InputUseRecord(
                    name=input_name,
                    raw=None,
                    evaluated=None,
                    normalised=None,
                    resolved=None,
                    dependencies=[],
                )
                setattr(inputs, input_name, input_record)

            # Match this read to one static or expanded dependency occurrence.
            matched = None

            for dependency in input_record.dependencies:
                occurrences = (
                    dependency.expanded_to or ()
                    if dependency.type == "dynamic"
                    else (dependency,)
                )

                for occurrence in occurrences:
                    if id(occurrence) in consumed:
                        continue
                    if occurrence.unscoped_reference == reference:
                        matched = occurrence
                        break

                if matched is not None:
                    break

            # Preserve a factual read not present in the declared dependency shape.
            if matched is None:
                matched = DependencyUseRecord(
                    associated_input=(input_name, input_record),
                    dep=DataRef.from_string(reference, prefix=False),
                    type="static",
                    unscoped_ref=reference,
                    input_index=observation.get("input_index"),
                )
                input_record.dependencies.append(matched)
                dependencies.append(matched)

            consumed.add(id(matched))

            # Apply the exact State and producer evidence.
            producer_exec_id = observation.get("producer_exec_id")
            producer = (
                self.exec_id_to_record.get(producer_exec_id)
                if producer_exec_id is not None
                else None
            )

            matched.associated_input = (input_name, input_record)
            matched.resolved_value = observation["value"]
            matched.resolved_version = observation["version"]
            matched.producer_source = (
                producer.runner_id
                if producer is not None
                else observation.get("producer_source")
            )
            matched.producer_actor = (
                producer.producer_id
                if producer is not None
                else observation.get("producer_actor")
            )
            matched.producer_exec_id = producer_exec_id
            matched.input_path = tuple(observation.get("input_path") or ())
            matched.snapshot_lineage = list(observation.get("snapshot_lineage") or ())
            matched.observed_mutable = observation.get("observed_mutable", False)
            matched.observation_id = observation.get("observation_id")

    def log(self, seq_id, exec_meta=None, inputs=None, dependencies=None, exec_id=None):
        # Build a placeholder identity for an externally observed write.
        if exec_meta is None:
            template_value = __fallback__()
            unknown = repr(template_value)
            exec_id = exec_id or create_tide_exec_uuid()

            node_snapshot = {
                "runner_id": unknown,
                "pipeline_id": unknown,
                "node_id": unknown,
                "func": unknown,
                "func_name": unknown,
                "base_node_id": None,
                "base_node_content_id": None,
                "content_id": unknown,
                "node_options": unknown,
                "emitted_by_exec_id": None,
            }
            inputs = Inputs({})
            dependencies = []
            declared_deps = []
            declared_effects = []
            declared_targets = []
            runtime_effects = []
            unscoped_store_refs = None
            output_value = template_value
            status = unknown
            started_at = unknown
            finished_at = unknown
            duration_ms = unknown
            error = None
            handled_raise = False

        # Build a completed runtime execution from its frozen evidence.
        else:
            node_snapshot = exec_meta.node_snapshot
            if node_snapshot is None:
                raise MissingExecutionEvidenceError(
                    f"Execution {exec_meta.id} has no immutable execution evidence"
                )
            if inputs is None or dependencies is None:
                raise MissingExecutionEvidenceError(
                    f"Execution {exec_meta.id} has no reconstructed inputs"
                )

            exec_id = exec_meta.id
            declared_deps = self._restore_execution_evidence(node_snapshot["declared_deps"])
            declared_effects = self._restore_execution_evidence(node_snapshot["declared_effects"])
            declared_targets = self._restore_execution_evidence(node_snapshot["declared_targets"])
            runtime_effects = copy.deepcopy(exec_meta.runtime_effects)
            unscoped_store_refs = list(node_snapshot["unscoped_store_refs"])
            output_value = ensure_tide_snapshot(
                exec_meta.snapshot,
                boundary="result",
            )
            status = exec_meta.status
            started_at = exec_meta.start_time
            finished_at = exec_meta.end_time
            duration_ms = exec_meta.duration
            error = exec_meta.error
            handled_raise = exec_meta.handled_raise

        # Reject two records claiming the same execution identity.
        if exec_id in self.exec_id_to_record:
            raise ValueError(f"Duplicate execution: {exec_id}")

        # Construct the execution record from normalized evidence.
        record = ExecRecord(
            seq_id=seq_id,
            exec_id=exec_id,
            ocean_id=self.ocean_id,
            runner_id=node_snapshot["runner_id"],
            pipeline_id=node_snapshot["pipeline_id"],
            node_id=node_snapshot["node_id"],
            func=node_snapshot["func"],
            func_name=node_snapshot["func_name"],
            inputs=inputs,
            deps=dependencies,
            effects=[],
            declared_deps=declared_deps,
            declared_effects=declared_effects,
            declared_targets=declared_targets,
            runtime_effects=runtime_effects,
            runtime_targets=[],
            unscoped_store_refs=unscoped_store_refs,
            status=status,
            started_at=started_at,
            finished_at=finished_at,
            duration_ms=duration_ms,
            error=error,
            handled_raise=handled_raise,
            output_value=output_value,
            writes=[],
            emitted_nodes=[],
            base_node_id=node_snapshot["base_node_id"],
            base_node_content_id=node_snapshot["base_node_content_id"],
            content_id=node_snapshot["content_id"],
            node_options=node_snapshot["node_options"],
            emitted_by_exec_id=node_snapshot["emitted_by_exec_id"],
        )

        # Store the same completed record by order and identity.
        self.exec_log.append(record)
        self.record_log.append(record)
        self.exec_id_to_record[record.exec_id] = record
        return record.exec_id

    def create_immediate_effect_record(
        self,
        *,
        seq_id,
        event_id,
        event_seq_id,
        exec_id,
        attempt,
        ambiguous_occurrence=None,
    ):
        actor = attempt.actor if attempt is not None else None
        node_definition_id = (
            attempt.node_definition_id if attempt is not None else None
        )
        ambiguous = ambiguous_occurrence
        if ambiguous is None and actor is not None:
            ambiguous = AmbiguousActorOccurrence(actor, seq_id, exec_id)
        record = ImmediateEffectRecord(
            seq_id=seq_id,
            event_id=event_id,
            event_seq_id=event_seq_id,
            exec_id=exec_id,
            execution_reference=ImmediateExecutionReference(
                exec_id=exec_id,
                actor=actor,
                node_definition_id=node_definition_id,
                ambiguous_occurrence=ambiguous,
            ),
        )
        self.record_log.append(record)
        return record

    @staticmethod
    def append_pending_write(
        immediate_record,
        stored_value,
        target,
        version,
        cause,
        *,
        snapshot_lineage=None,
        observed_mutable=False,
        updated=False,
    ):
        write = WriteRecord(
            cause=cause,
            stored_result=stored_value,
            stored_version=version,
            stored_target=target,
            snapshot_lineage=snapshot_lineage,
            observed_mutable=observed_mutable,
            updated=updated,
        )
        immediate_record.writes.append(write)
        return write

    def _assign_external_identity(self, record, runner_id, node_id):
        # Assign the observed producer identity to a placeholder execution.
        record.runner_id = runner_id
        record.node_id = node_id
        record.func_name = node_id
        record.producer_id = Actor(self.ocean_id, runner_id, node_id)

    def append_write_log(
        self,
        exec_id,
        stored_value,
        target,
        version,
        cause,
        snapshot_lineage=None,
        observed_mutable=False,
        updated=False,
    ):
        # Resolve the execution which produced this write.
        record = self.exec_id_to_record[exec_id]

        # Retain the target once while preserving first-write order.
        runtime_target = DataRef.from_string(target, prefix=False)
        if runtime_target not in record.runtime_targets:
            record.runtime_targets.append(runtime_target)

        # Append the exact write occurrence.
        record.writes.append(WriteRecord(
            cause=cause,
            stored_result=stored_value,
            stored_version=version,
            stored_target=target,
            snapshot_lineage=snapshot_lineage,
            observed_mutable=observed_mutable,
            updated=updated,
        ))

    def create_definition_inputs(self, node_snapshot):
        """Restore declared input shapes without claiming execution reads."""
        input_records = {}
        for name, captured_input in node_snapshot.get("inputs", {}).items():
            input_records[name] = InputUseRecord(
                name=name,
                raw=captured_input["raw"],
                evaluated=__fallback__(),
                normalised=__fallback__(),
                resolved=__fallback__(),
                dependencies=[],
            )
        return Inputs(input_records)

    def append_emitted_nodes(self, exec_id, node_snapshots, origin):
        # Resolve the execution which emitted these nodes.
        execution = self.exec_id_to_record[exec_id]

        # Reconstruct each emitted node from its frozen definition.
        for node_snapshot in node_snapshots:
            if not isinstance(node_snapshot, dict):
                raise MissingExecutionEvidenceError(
                    f"Execution {exec_id} emitted a node without a snapshot"
                )

            inputs = self.create_definition_inputs(node_snapshot)
            declared_deps = self._restore_execution_evidence(
                node_snapshot["declared_deps"]
            )
            declared_targets = self._restore_execution_evidence(node_snapshot["declared_targets"])
            declared_effects = self._restore_execution_evidence(node_snapshot["declared_effects"])

            execution.emitted_nodes.append(EmittedNodeRecord(
                ocean_id=self.ocean_id,
                runner_id=node_snapshot["runner_id"],
                pipeline_id=node_snapshot["pipeline_id"],
                node_id=node_snapshot["node_id"],
                func=node_snapshot["func"],
                func_name=node_snapshot["func_name"],
                inputs=inputs,
                declared_deps=declared_deps,
                declared_targets=declared_targets,
                declared_effects=declared_effects,
                origin=origin,
                content_id=node_snapshot["content_id"],
                base_node=node_snapshot["base_node_id"],
                base_node_content_id=node_snapshot["base_node_content_id"],
                node_options=node_snapshot["node_options"],
                emitted_by_exec_id=node_snapshot["emitted_by_exec_id"],
            ))




def trace_from_event_log(
    event_log,
    *,
    ocean_id,
    runner=None,
    pipelines=None,
    name="TIDE: .trace()",
):
    from tide_tooling.tooling_core.engine.event_occurrences import FrozenEventLog
    from tide_tooling.tooling_core.engine.event_readers import EventLogReader
    from tide_tooling.tooling_core.engine.tide_logging import Trace

    if not isinstance(event_log, FrozenEventLog):
        event_log = FrozenEventLog.capture(event_log)

    start_event_id = None
    selected_runner_id = None

    # A Runner scopes one specific root run.
    if runner is not None:
        start_event_id = getattr(runner, "trace_event_start_id", None)
        runner_start_event_id = getattr(
            runner,
            "runner_start_event_id",
            None,
        )

        if (
            start_event_id is not None
            and runner_start_event_id is not None
            and runner_start_event_id != start_event_id
        ):
            raise ValueError(
                "Trace construction requires the root Runner boundary; "
                "focused child-run traces are derived views"
            )

        if start_event_id is not None:
            event_log = event_log.slice_from(start_event_id)

        selected_runner_id = runner.id

    events = event_log.events

    # An untouched Ocean still has a valid empty Trace.
    if not events:
        return Trace(
            (),
            event_log=event_log,
            events_verified=True,
            pipelines=pipelines,
            runner=selected_runner_id,
            ocean_id=ocean_id,
            name=name,
        )

    # Keep the runtime boundary honest before reduction.
    stream_ocean_id = events[0].ocean_id
    if stream_ocean_id != ocean_id:
        raise ValueError(
            "Event stream ocean identity disagrees with its runtime boundary: "
            f"{stream_ocean_id!r} != {ocean_id!r}"
        )

    event_hosts = {
        event.host
        for event in events
        if event.host is not None
    }
    if len(event_hosts) > 1:
        raise ValueError(
            "Event stream contains conflicting host identities"
        )

    host = next(iter(event_hosts), None)

    # Event interpretation happens exactly once here.
    state = EventLogReader(ocean_id)._construct_log(events)
    execution_log = state.exec_log

    boundary_event = (
        state.events_by_id.get(start_event_id)
        if start_event_id is not None
        else None
    )

    return Trace(
        execution_log.record_log,
        event_log=event_log,
        events_verified=True,
        pipelines=pipelines,
        runner=selected_runner_id,
        ocean_id=ocean_id,
        name=name,
        host=host,
        trace_starting_event=boundary_event,
        pipeline_starting_event=boundary_event,
        tide_plan=(
            execution_log.runner_plans.get(selected_runner_id)
            if selected_runner_id is not None
            else None
        ),
        missing_referenced_events=execution_log.missing_referenced_events,
        causal_order_violations=execution_log.causal_order_violations,
        evidence_violations=execution_log.evidence_violations,
        ready_exec_ids=execution_log.ready_exec_ids,
        blocked_exec_ids=execution_log.blocked_exec_ids,
        deadlock=execution_log.deadlock,
        ready_attempts=execution_log.ready_attempts,
        started_runner_ids=execution_log.started_runner_ids,
        causal_runner_ids=execution_log.causal_runner_ids,
        ended_runner_ids=execution_log.ended_runner_ids,
    )
