from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tide_tooling.tooling_core.engine.PipelineRunner import PipelineRunner

from tide_tooling.tooling_core.engine.events         import Cause, Effects
from tide_tooling.tooling_core.engine.classes        import DataRef, __fallback__
from tide_tooling.tooling_core.engine.utils          import (
    DedupedList,
    detached_tide_value,
    deduped_dict,
    safe_equal,
)
from tide_tooling.tooling_core.engine.errors         import InvalidObjectKindError
from tide_tooling.tooling_core.engine.tide_records   import (
    Actor,
    ActorOccurrence,
    AffectedRecord,
    DependencyUseRecord,
    EffectRecord,
    EmittedNodeRecord,
    ExecRecord,
    ImmediateEffectRecord,
    UnresolvedExecutionRecord,
    ReadyAttemptRecord,
    ExpandedDepRecord,
    InputUseRecord,
    Inputs,
    WriteRecord,
)
from tide_tooling.tooling_core.engine.tide_links     import (
    EffectEdge,
    EmissionEdge,
    ReadEdge,
)
from tide_tooling.tooling_core.engine.tide_index     import TraceIndex
from tide_tooling.tooling_core.engine.tide_evidence  import (
    TraceEvidenceStore,
    seal_exec_records,
    validate_exec_record_relationships,
)
from tide_tooling.tooling_core.engine.tide_query_results import (
    Compared,
    ConsumedBy,
    Definition,
    Downstream,
    ExecutionAncestry,
    ExecValues,
    Inspected,
    InspectedGroup,
    Investigate,
    Nearby,
    Object,
    Pathed,
    ProducedBy,
    Searched,
    Stream,
    Upstream,
    UsedAs,
    Waiting,
    _build_compared,
    _build_object_compared,
    _state_occurrence_text,
)
import copy
from .utils import tide_value_hasher

from collections                import defaultdict    
from typing                     import Any
from .tide_rendering import (
    SHOW_CYAN,
    SHOW_DIM,
    SHOW_GREEN,
    SHOW_RED,
    SHOW_RESET,
    SHOW_WHITE,
    SHOW_YELLOW,
    _show_options,
    _show_render,
    show_header,
    show_output,
    show_section,
    strip_ansi,
)


def _detached_error(error, memo):
    if error is None:
        return None
    if not isinstance(error, BaseException):
        return copy.deepcopy(error, memo)

    try:
        detached = type(error)(*copy.deepcopy(error.args, memo))
    except Exception:
        detached = Exception(*copy.deepcopy(error.args, memo))
    memo[id(error)] = detached

    for name, value in getattr(error, "__dict__", {}).items():
        # A failed Trace may be attached to its exception.  Copying it here
        # recursively reaches live runners/thread locks; it is contextual
        # navigation, not part of the exception value itself.
        if name == "tide_trace":
            continue
        setattr(detached, name, copy.deepcopy(value, memo))
    return detached


def _detached_exec_records(records):
    """Clone public execution evidence without traversing live failure state."""
    memo = {}
    detached_records = []
    for record in records:
        detached = copy.copy(record)
        memo[id(record)] = detached
        detached_records.append(detached)

    for record, detached in zip(records, detached_records, strict=True):
        for name, value in vars(record).items():
            if name == "error":
                cloned = _detached_error(value, memo)
            else:
                cloned = copy.deepcopy(value, memo)
            setattr(detached, name, cloned)
    return tuple(detached_records)


def _project_executions(records, completed, ready_attempts, evidence_store):
    """Build the single execution-shaped projection of Trace evidence."""
    attempts = {attempt.exec_id: attempt for attempt in ready_attempts}
    grouped = {}
    for record in records:
        if not isinstance(record, ImmediateEffectRecord):
            continue
        if record.execution_reference.resolved:
            continue
        grouped.setdefault(record.exec_id, []).append(record)

    projected = list(completed)
    projection_memo = {}
    for exec_id, windows in grouped.items():
        first = windows[0]
        attempt = attempts.get(exec_id)
        actor = first.producer_id
        attempt_snapshot = (
            attempt.node_snapshot
            if attempt is not None
            and isinstance(attempt.node_snapshot, dict)
            else {}
        )

        def attempt_value(name, default):
            value = attempt_snapshot.get(name, default)
            from tide_tooling.tooling_core.engine.utils import TIDEValueSnapshot
            return value.restore() if isinstance(value, TIDEValueSnapshot) else value

        unresolved = UnresolvedExecutionRecord(
            ambiguous=True,
            immediate_windows=tuple(windows),
            seq_id=first.seq_id,
            event_seq_id=first.event_seq_id,
            exec_id=exec_id,
            producer_id=actor,
            producer_occurrence_id=first.producer_occurrence_id,
            source_ocean_id=actor.source_ocean,
            runner_id=attempt.runner_id if attempt is not None else actor.source_runner,
            pipeline_id=attempt.pipeline_id if attempt is not None else None,
            node_id=(
                attempt.node_definition_id
                if attempt is not None
                else first.execution_reference.node_definition_id
            ),
            base_node_id=(
                attempt.node_definition_id
                if attempt is not None
                else first.execution_reference.node_definition_id
            ),
            func_name=actor.source_actor,
            func=attempt_snapshot.get("func"),
            content_id=(
                attempt.node_definition_id
                if attempt is not None
                else first.execution_reference.node_definition_id
            ),
            deps=tuple(dependency for window in windows for dependency in window.deps),
            writes=tuple(write for window in windows for write in window.writes),
            effects=tuple(effect for window in windows for effect in window.effects),
            inputs={},
            emitted_nodes=(),
            emitted_by_exec_id=None,
            declared_deps=tuple(attempt_value("declared_deps", ())),
            declared_targets=tuple(attempt_value("declared_targets", ())),
            declared_effects=tuple(attempt_snapshot.get("declared_effects", ())),
            runtime_effects=(),
            runtime_targets=(),
            unscoped_store_refs=tuple(attempt_snapshot.get("unscoped_store_refs", ())),
            node_options=dict(attempt_snapshot.get("node_options", {})),
            base_node_content_id=attempt_snapshot.get("base_node_content_id"),
            status="ambiguous",
            started_at=None,
            finished_at=None,
            duration_ms=None,
            error=None,
        )
        unresolved._tide_seal_evidence(evidence_store, projection_memo)
        projected.append(unresolved)

    return tuple(sorted(projected, key=lambda record: record.seq_id))


def _same_effect_record(left, right):
    """Match one Effect fact across owned/detached Trace projections."""
    return (
        left.event_id == right.event_id
        and left.effect == right.effect
        and safe_equal(left.targets, right.targets)
        and left.origin == right.origin
        and left.immediate == right.immediate
        and left.target_existed == right.target_existed
        and safe_equal(left.output, right.output)
    )

__all__ = [
    "Trace",
    "TraceIndex",
    "Actor",
    "AffectedRecord",
    "DependencyUseRecord",
    "EffectRecord",
    "EmittedNodeRecord",
    "ExecRecord",
    "ExpandedDepRecord",
    "InputUseRecord",
    "Inputs",
    "WriteRecord",
    "EffectEdge",
    "EmissionEdge",
    "ReadEdge",
    "Compared",
    "ConsumedBy",
    "Downstream",
    "ExecValues",
    "Inspected",
    "InspectedGroup",
    "Investigate",
    "Nearby",
    "Object",
    "Pathed",
    "ProducedBy",
    "Searched",
    "Stream",
    "Upstream",
    "_build_compared",
    "_build_object_compared",
    "strip_ansi",
]

__TEMPLATE__ =  __fallback__()
DEFAULT_TIDE_STORES = [
    "mem",
    "__tide__",
    "__untracked__",
    "__model__",
    "global",
]
DEFAULT_STREAM_MAX_PATHS = 1000

class Trace:
    def __setattr__(self, name, value):
        if getattr(self, "_tide_trace_sealed", False):
            raise AttributeError("Trace evidence is sealed")
        object.__setattr__(self, name, value)

    @staticmethod
    def _validate_record_identities(records):
        seen_exec_ids = set()
        seen_seq_ids = set()
        seen_occurrences = set()

        for record in records:
            if record.seq_id in seen_seq_ids:
                raise ValueError(
                    f"Duplicate Trace sequence: {record.seq_id}"
                )
            seen_seq_ids.add(record.seq_id)

            if not isinstance(record, ExecRecord):
                continue

            producer = record.producer_id
            if producer.source_ocean != record.source_ocean_id:
                raise ValueError(
                    "Execution producer ocean does not match source_ocean_id"
                )
            if producer.source_runner != record.runner_id:
                raise ValueError(
                    "Execution producer runner does not match runner_id"
                )
            if producer.source_actor != record.func_name:
                raise ValueError(
                    "Execution producer actor does not match func_name"
                )
            if record.exec_id in seen_exec_ids:
                raise ValueError(
                    f"Duplicate execution identity: {record.exec_id}"
                )
            seen_exec_ids.add(record.exec_id)

            occurrence = record.producer_occurrence_id
            if occurrence is None:
                continue
            if occurrence.actor != producer:
                raise ValueError(
                    "ActorOccurrence actor does not match execution producer"
                )
            if (
                not isinstance(occurrence.occurrence_id, int)
                or isinstance(occurrence.occurrence_id, bool)
                or occurrence.occurrence_id < 0
            ):
                raise ValueError(
                    "ActorOccurrence occurrence_id must be a non-negative int"
                )
            if occurrence in seen_occurrences:
                raise ValueError(
                    f"Duplicate ActorOccurrence identity: {occurrence}"
                )
            seen_occurrences.add(occurrence)

    def __init__(
        self,
        log,
        *,
        event_log=None,
        events_verified=None,
        _execution_records=None,
        _index=None,
        pipelines=None,
        runner=None,
        ocean_id=None,
        name="TIDE: .trace()",
        from_cas=False,
        bundle_context=None,
        model_context=None,
        host=None,
        host_record=None,
        trace_starting_event=None,
        pipeline_starting_event=None,
        tide_plan=None,
        missing_referenced_events=None,
        causal_order_violations=None,
        ready_exec_ids=None,
        blocked_exec_ids=None,
        deadlock=None,
        ready_attempts=None,
        started_runner_ids=None,
        causal_runner_ids=None,
        ended_runner_ids=None,
        evidence_violations=None,
        _state_authorities=None,
        _value_backing=None,
        _source_bundle=None,
        _evidence_store=None,
        _require_closed_evidence=False,
    ):
        """Construct a Trace from normalized execution evidence.

        ``event_log`` is retained source evidence only. Trace construction does
        not inspect Event types or payloads and does not reconstruct facts from
        the Event stream.
        """
        if ocean_id is None:
            raise ValueError("Trace requires a source ocean_id")

        if events_verified not in {True, False, None}:
            raise TypeError("events_verified must be True, False, or None")

        if event_log is None and events_verified is not None:
            raise ValueError(
                "events_verified requires retained Event evidence"
            )

        if (
            runner is not None
            and not isinstance(runner, (int, str))
            and not all(
                hasattr(runner, field)
                for field in ("id", "pipeline", "index")
            )
        ):
            raise ValueError(f"Trace received an invalid runner: {runner}")

        selected_runner_id = (
            runner
            if isinstance(runner, (int, str))
            else getattr(runner, "id", None)
        )
        supplied_trace_start_id = (
            trace_starting_event
            if isinstance(trace_starting_event, (int, str))
            else getattr(trace_starting_event, "event_id", None)
        )
        supplied_pipeline_start_id = (
            pipeline_starting_event
            if isinstance(pipeline_starting_event, (int, str))
            else getattr(pipeline_starting_event, "event_id", None)
        )

        # Own the normalized evidence.
        self._name = name
        self._ocean_id = ocean_id
        self._event_log = event_log
        self._events_verified = events_verified
        self._evidence = _evidence_store or TraceEvidenceStore()

        incoming_records = tuple(log or ())
        owned_records = (
            incoming_records
            if all(
                getattr(record, "_tide_sealed", False)
                for record in incoming_records
            )
            else _detached_exec_records(incoming_records)
        )
        records = tuple(sorted(
            owned_records,
            key=lambda record: record.seq_id,
        ))
        self._validate_record_identities(records)
        self._records = seal_exec_records(records, self._evidence)
        self._log = tuple(
            record
            for record in self._records
            if isinstance(record, ExecRecord)
        )

        # Retain supplied source metadata. No metadata source is an authority
        # over the normalized Trace evidence.
        self._pipeline_names = {
            pipeline_id: getattr(pipeline, "name", pipeline)
            for pipeline_id, pipeline in (pipelines or {}).items()
        }
        self._trace_starting_event_id = supplied_trace_start_id
        self._pipeline_starting_event_id = supplied_pipeline_start_id
        self._tide_plan = copy.deepcopy(tide_plan)
        self._missing_referenced_events = {
            event_id: tuple(referencing_event_ids)
            for event_id, referencing_event_ids in (
                missing_referenced_events or {}
            ).items()
        }
        self._causal_order_violations = {
            event_id: tuple(referencing_event_ids)
            for event_id, referencing_event_ids in (
                causal_order_violations or {}
            ).items()
        }
        self._ready_exec_ids = frozenset(ready_exec_ids or ())
        self._blocked_exec_ids = frozenset(blocked_exec_ids or ())
        self._deadlock = copy.deepcopy(deadlock or {})

        incoming_attempts = tuple(ready_attempts or ())
        owned_attempts = (
            incoming_attempts
            if all(
                getattr(attempt, "_tide_sealed", False)
                for attempt in incoming_attempts
            )
            else _detached_exec_records(incoming_attempts)
        )
        self._ready_attempts = seal_exec_records(
            owned_attempts,
            self._evidence,
        )
        self._started_runner_ids = frozenset(started_runner_ids or ())
        self._causal_runner_ids = frozenset(causal_runner_ids or ())
        self._ended_runner_ids = frozenset(ended_runner_ids or ())
        self._evidence_violations = {
            key: tuple(values)
            for key, values in (evidence_violations or {}).items()
        }
        self._selected_runner_id = selected_runner_id

        # Build the execution-shaped query surface and its indexes.
        self._execution_records = (
            tuple(_execution_records)
            if _execution_records is not None
            else _project_executions(
                self._records,
                self._log,
                self._ready_attempts,
                self._evidence,
            )
        )
        if _index is not None:
            self._index = _index
        else:
            if _state_authorities is None:
                state_authorities = validate_exec_record_relationships(
                    self._execution_records,
                    record_log=self._records,
                    ready_attempts=self._ready_attempts,
                )
            else:
                # The imported TideRun already recorded State value identity.
                # Validate relationships without re-deriving identity from
                # hydrated Python values.
                validate_exec_record_relationships(
                    self._execution_records,
                    require_closed=_require_closed_evidence,
                    validate_state_values=False,
                    record_log=self._records,
                    ready_attempts=self._ready_attempts,
                )
                state_authorities = dict(_state_authorities)

            self._index = TraceIndex().build(
                self._execution_records,
                state_authorities=state_authorities,
                value_backing=_value_backing,
                ready_exec_ids=self._ready_exec_ids,
                blocked_exec_ids=self._blocked_exec_ids,
            ).seal()

        # Retain import/re-export context without letting it reinterpret Trace.
        self._from_cas = bool(from_cas)
        self._source_bundle = _source_bundle
        if self._source_bundle is not None and not self._from_cas:
            raise ValueError(
                "Only a Trace restored from a Bundle may retain a source Bundle"
            )
        self._bundle_context = (
            copy.deepcopy(bundle_context)
            if isinstance(bundle_context, dict)
            else {}
        )
        self._model_context = (
            copy.deepcopy(model_context)
            if isinstance(model_context, dict)
            else {}
        )
        self._host = copy.deepcopy(
            host if host is not None else self._model_context.get("host")
        )
        self._host_record = copy.deepcopy(host_record)
        self._divergence = copy.deepcopy(
            self._bundle_context.get("divergence") or {}
        )

        # Plan is a derived Trace view over captured source intent and
        # normalized execution evidence. It is never an independent authority.
        self._plan = self._materialise_plan()

        # Corruption is derived only from normalized reader facts already
        # supplied to Trace, never from replay or bundle/model authority.
        self._corruption = copy.deepcopy(self._current_corruption())
        self._tide_trace_sealed = True

    @property
    def name(self):
        return self._name

    @property
    def ocean_id(self):
        return self._ocean_id

    @property
    def from_cas(self):
        return self._from_cas

    @property
    def host(self):
        return copy.deepcopy(self._host)

    @property
    def missing_referenced_events(self):
        return dict(self._missing_referenced_events)

    @property
    def causal_order_violations(self):
        return dict(self._causal_order_violations)

    @property
    def ready_exec_ids(self):
        return self._ready_exec_ids

    @property
    def blocked_exec_ids(self):
        return self._blocked_exec_ids

    @property
    def deadlocked(self):
        return any(bool(waiting) for waiting in self._deadlock.values())

    @property
    def deadlock(self):
        return copy.deepcopy(self._deadlock)

    @property
    def waiting(self):
        return Waiting(self._deadlock)

    @property
    def scope(self):
        return (
            "complete"
            if self.pipeline_ended
            and not self.unresolved_ready_attempts
            else "ambiguous"
        )

    @property
    def pipeline_ended(self):
        return (
            self._selected_runner_id is not None
            and self._selected_runner_id in self._ended_runner_ids
        )

    @property
    def ready_attempts(self):
        return self._ready_attempts

    @property
    def unresolved_ready_attempts(self):
        return tuple(
            attempt
            for attempt in self._ready_attempts
            if attempt.outcome == "ready"
            and (
                self._selected_runner_id is None
                or attempt.runner_id in self._scope_runner_ids
            )
        )

    @property
    def _scope_runner_ids(self):
        if self._causal_runner_ids:
            return self._causal_runner_ids
        if self._selected_runner_id is not None:
            return frozenset((self._selected_runner_id,))
        return frozenset()


    @property
    def events(self):
        return self._event_log.events if self._event_log is not None else ()

    @property
    def event_log(self):
        return self._event_log

    @property
    def events_verified(self):
        return self._events_verified

    def _boundary_event(self, event_id):
        if event_id is None:
            return None
        return next(
            (
                event
                for event in self.events
                if event.event_id == event_id
            ),
            None,
        )

    @property
    def trace_starting_event(self):
        return self._boundary_event(self._trace_starting_event_id)

    @property
    def pipeline_starting_event(self):
        return self._boundary_event(self._pipeline_starting_event_id)

    @property
    def log(self):
        return self._records

    @property
    def pipelines(self):
        return copy.deepcopy(self._pipeline_names)

    @property
    def bundle_context(self):
        return copy.deepcopy(self._bundle_context)

    @property
    def divergence(self):
        return copy.deepcopy(self._divergence)

    @property
    def divergent(self):
        return bool(self.divergence.get("warnings"))

    @property
    def model_context(self):
        return copy.deepcopy(self._model_context)

    @property
    def host_record(self):
        return copy.deepcopy(self._host_record)

    @property
    def _execs(self):
        return self._index.exec_by_exec_id

    @property
    def index(self):
        return self._index

    @property
    def execs(self):
        return self._index.exec_by_exec_id

    @property
    def plan(self):
        return copy.deepcopy(self._plan)

    def _derived_view(self, *, log, ocean_id=None, name=None):
        """Construct a sealed presentation view without mutating this Trace."""
        return Trace(
            tuple(log),
            pipelines=self._pipeline_names,
            ocean_id=self.ocean_id if ocean_id is None else ocean_id,
            name=self.name if name is None else name,
            from_cas=self.from_cas,
            bundle_context=self._bundle_context,
            model_context=self._model_context,
            host=self._host,
            host_record=self._host_record,
            tide_plan=self._tide_plan,
            trace_starting_event=self.trace_starting_event,
            pipeline_starting_event=self.pipeline_starting_event,
            missing_referenced_events=self._missing_referenced_events,
            causal_order_violations=self._causal_order_violations,
            ready_exec_ids=self._ready_exec_ids,
            blocked_exec_ids=self._blocked_exec_ids,
            deadlock=self._deadlock,
            ready_attempts=self._ready_attempts,
            started_runner_ids=self._started_runner_ids,
            causal_runner_ids=self._causal_runner_ids,
            ended_runner_ids=self._ended_runner_ids,
            evidence_violations=self._evidence_violations,
            runner=self._selected_runner_id,
            _evidence_store=self._evidence,
        )

    @staticmethod
    def _plan_reference(reference):
        if isinstance(reference, DataRef):
            return DataRef.to_string(reference, prefix=False)
        return str(reference)

    @staticmethod
    def _dynamic_plan_reads(record, declared):
        if record is None:
            return ()

        reads = []
        for dependency in record.deps or ():
            if dependency.type != "dynamic" or dependency.dep != declared:
                continue
            for expanded in dependency.expanded_to or ():
                if not any(
                    read.unscoped_reference == expanded.unscoped_reference
                    for read in reads
                ):
                    reads.append(expanded)
        return tuple(reads)

    @staticmethod
    def _static_plan_read(record, declared):
        if record is None:
            return None
        return next((
            dependency
            for dependency in record.deps or ()
            if dependency.type != "dynamic"
            and dependency.dep == declared
        ), None)

    def _materialise_plan(self):
        if self._tide_plan is None:
            return None

        node_specs = list(self._tide_plan.get("nodes", ()))
        specs_by_id = {
            spec["definition_id"]: spec
            for spec in node_specs
        }
        selected_counts = defaultdict(int)
        occurrences = []

        for record in self._log:
            spec = specs_by_id.get(record.base_node_id)
            if spec is None:
                continue
            if spec["one_shot"] and selected_counts[record.base_node_id]:
                continue
            selected_counts[record.base_node_id] += 1
            occurrences.append((spec, record))

        used_occurrences = defaultdict(set)
        for _, projected in occurrences:
            occurrence = projected.producer_occurrence_id
            if isinstance(occurrence, ActorOccurrence):
                used_occurrences[occurrence.actor.source_actor].add(
                    occurrence.occurrence_id
                )

        next_occurrence = {
            actor_name: max(occurrences) + 1
            for actor_name, occurrences in used_occurrences.items()
            if occurrences
        }
        for spec in node_specs:
            definition_id = spec["definition_id"]
            if selected_counts[definition_id]:
                continue

            actor = Actor(
                self.ocean_id,
                spec["runner_id"],
                spec["actor_name"],
            )
            occurrence_id = next_occurrence.get(actor.source_actor, 0)
            next_occurrence[actor.source_actor] = occurrence_id + 1
            occurrences.append((
                spec,
                ActorOccurrence(actor, occurrence_id),
            ))

        external_actor = Actor(
            self.ocean_id,
            "TIDE: External",
            "TIDE: External",
        )

        executions = []
        states = []
        inputs = []
        outputs = []
        seen_states = set()
        seen_inputs = set()
        seen_outputs = set()
        latest_version_by_ref = {}
        seen_executions = set()

        def add_execution(exec_id):
            if exec_id not in seen_executions:
                seen_executions.add(exec_id)
                executions.append(exec_id)

        def external_execution(read):
            if read is not None and read.producer_exec_id is not None:
                producer = self._index.exec_by_exec_id.get(
                    read.producer_exec_id
                )
                if (
                    producer is not None
                    and producer.producer_id == external_actor
                    and isinstance(
                        producer.producer_occurrence_id,
                        ActorOccurrence,
                    )
                ):
                    return repr(producer.producer_occurrence_id)

            occurrence_id = next_occurrence.get(
                external_actor.source_actor,
                0,
            )
            next_occurrence[external_actor.source_actor] = occurrence_id + 1
            return repr(ActorOccurrence(external_actor, occurrence_id))

        def state_id(reference, version):
            return f"{reference}[v{version}]"

        def ensure_state(reference, version):
            identity = state_id(reference, version)
            created = identity not in seen_states
            if identity not in seen_states:
                seen_states.add(identity)
                states.append(identity)
            return identity, created

        for spec, occurrence in occurrences:
            if isinstance(occurrence, ActorOccurrence):
                exec_id = repr(occurrence)
                record = None
            else:
                exec_id = repr(occurrence.producer_occurrence_id)
                record = occurrence
            add_execution(exec_id)

            for dependency in spec["dependencies"]:
                if dependency["dynamic"]:
                    reads = self._dynamic_plan_reads(
                        record,
                        dependency["declared"],
                    )
                else:
                    read = self._static_plan_read(
                        record,
                        dependency["declared"],
                    )
                    reads = ((
                        read.unscoped_reference
                        if read is not None
                        else dependency["reference"],
                        read,
                    ),)

                if dependency["dynamic"]:
                    reads = tuple(
                        (read.unscoped_reference, read)
                        for read in reads
                    )

                for reference, read in reads:
                    reference = self._plan_reference(reference)
                    version = latest_version_by_ref.setdefault(reference, 0)
                    identity, created = ensure_state(reference, version)
                    if created:
                        producer_id = external_execution(read)
                        add_execution(producer_id)
                        output_edge = (producer_id, identity)
                        if output_edge not in seen_outputs:
                            seen_outputs.add(output_edge)
                            outputs.append({
                                "exec_id": producer_id,
                                "state_id": identity,
                            })
                    edge = (exec_id, identity)
                    if edge not in seen_inputs:
                        seen_inputs.add(edge)
                        inputs.append({
                            "exec_id": exec_id,
                            "state_id": identity,
                        })

            for target in spec["targets"]:
                reference = self._plan_reference(target)
                version = latest_version_by_ref.get(reference, -1) + 1
                latest_version_by_ref[reference] = version
                identity, _ = ensure_state(reference, version)
                edge = (exec_id, identity)
                if edge not in seen_outputs:
                    seen_outputs.add(edge)
                    outputs.append({
                        "exec_id": exec_id,
                        "state_id": identity,
                    })

        plan = {
            "E": executions,
            "S": states,
            "I": inputs,
            "O": outputs,
        }

        return plan

    def _current_corruption(self):
        corruption = {
            "missing_causal_events": dict(self.missing_referenced_events),
            "causal_order_violations": dict(self.causal_order_violations),
            "evidence_violations": dict(self._evidence_violations),
        }
        if self.pipeline_ended and self.unresolved_ready_attempts:
            corruption["incomplete_executions"] = {
                attempt.exec_id: attempt.event_id
                for attempt in self.unresolved_ready_attempts
            }
        return corruption

    @property
    def corruption(self):
        return copy.deepcopy(self._corruption)

    @property
    def corrupted(self):
        return any(bool(details) for details in self.corruption.values())

    @property
    def reads(self):
        return tuple(
            read
            for execution in self._execution_records
            for dependency in execution.deps
            for read in (
                dependency.expanded_to
                if dependency.type == "dynamic"
                else (dependency,)
            )
        )

    @property
    def writes(self):
        return tuple(
            write
            for execution in self._execution_records
            for write in execution.writes
        )

    @property
    def values(self):
        return tuple(
            detached_tide_value(value)
            for value in self._index.value_by_hash.values()
        )

    @property
    def runs(self):
        return tuple(dict.fromkeys(
            execution.runner_id for execution in self._execution_records
        ))

    def _sources(self):
        return tuple(dict.fromkeys(
            (execution.runner_id, execution.pipeline_id)
            for execution in self._execution_records
        ))

    def to_dict(self, *, serialise_events=False):
        """Return the Trace summary, optionally serialising retained Events."""
        if not isinstance(serialise_events, bool):
            raise TypeError("serialise_events must be a bool")

        events = self.events
        if serialise_events and self._event_log is not None:
            events = self._event_log.to_event_bundle().to_dict()

        return {
            "type": "trace",
            "name": self.name,
            "ocean_id": self.ocean_id,
            "sources": [
                {
                    "run": runner_id,
                    "pipeline_id": pipeline_id,
                    "pipeline": self._get_pipeline_named_tuple_from_id(
                        pipeline_id
                    )[1],
                }
                for runner_id, pipeline_id in self._sources()
            ],
            "from_cas": self.from_cas,
            "bundle_context": copy.deepcopy(self._bundle_context),
            "model_context": copy.deepcopy(self._model_context),
            "host": copy.deepcopy(self.host),
            "host_record": copy.deepcopy(self._host_record),
            "events": events,
            "value_hashes": sorted(self._index.value_by_hash),
            "references": sorted(self._index.seen_refs),
            "ref_versions": [
                {
                    "ref": ref,
                    "version": version,
                    "value_hash": value_hash,
                }
                for (ref, version), value_hash in sorted(
                    self._index.value_hash_by_ref_version.items(),
                    key=repr,
                )
            ],
        }

    def _tide_diff_evidence(self):
        """Project only recorded, identity-scoped Trace evidence."""
        from tide_tooling.tooling_core.bundling.TIDERunSerialiser import (
            TIDERunSerialiser,
        )

        return TIDERunSerialiser().evidence_claims(self)

    def _resolve_diff_operand(self, operand, kind=None, version=None):
        """Resolve a typed trace identifier to the object that should be diffed."""
        detected_kind = self._infer_kind(operand)
        resolved_kind = kind or detected_kind

        # Preserve the original two-value API for ordinary Python values.
        # Only unambiguous trace identifiers are inferred automatically.
        should_resolve = kind is not None or detected_kind in {
            "reference",
            "hash",
            "effect",
        }
        if not should_resolve:
            if version is not None:
                raise ValueError("A diff version requires a typed reference operand")
            return operand

        if resolved_kind in {"reference", "ref", "dep"}:
            if not isinstance(operand, str):
                raise TypeError("A reference diff operand must be a string")

            found = self.find(operand, version).first("reference")
            if found is None:
                raise KeyError(f"Unknown TIDE reference: {operand}")
            return self.value_of(found.id, version=version)

        if version is not None and resolved_kind != "effect":
            raise ValueError(
                "Only reference and effect diff operands support versions"
            )

        if resolved_kind == "hash":
            return self.value_of(operand)
        if resolved_kind == "value":
            return operand

        if resolved_kind == "actor" and isinstance(operand, str):
            inspected = self.find(operand).first("actor")
        else:
            inspected = self.inspect(
                operand,
                resolved_kind,
                version=version,
            )

        if inspected is None or not inspected.exists:
            raise KeyError(f"Unknown TIDE {resolved_kind}: {operand}")
        if resolved_kind == "effect":
            evidence = self.investigate(inspected).to_dict()
            return {
                "kind": evidence["kind"],
                "summary": evidence["summary"],
                "history": evidence["history"],
            }
        if isinstance(inspected, InspectedGroup):
            return [item.object for item in inspected.object]
        return inspected.object.object

    def diff(
        self,
        source,
        target=__fallback__,
        source_type=None,
        target_type=None,
        source_ver=None,
        target_ver=None,
    ):
        """Compare Trace evidence or two values through the ordinary diff."""

        target_omitted = (
            isinstance(target, type)
            and issubclass(target, __fallback__)
        ) or isinstance(target, __fallback__)

        if target_omitted:
            if any(
                option is not None
                for option in (
                    source_type,
                    target_type,
                    source_ver,
                    target_ver,
                )
            ):
                raise TypeError(
                    "Typed Trace.diff needs both a source and a target"
                )
            return _build_compared(self, source)

        resolved_source = self._resolve_diff_operand(
            source,
            source_type,
            source_ver,
        )
        resolved_target = self._resolve_diff_operand(
            target,
            target_type,
            target_ver,
        )
        return _build_compared(resolved_source, resolved_target)

    def compare(
        self,
        other,
        *,
        mapping="structural",
        execution_map=None,
        state_map=None,
        represent_unknown_producers=False,
    ):
        if not isinstance(represent_unknown_producers, bool):
            raise TypeError("represent_unknown_producers must be a bool")

        source = self.to_model(enrich=False)
        target = (
            other.to_model(enrich=False)
            if isinstance(other, Trace)
            else copy.deepcopy(other)
        )

        if represent_unknown_producers:
            from tide_tooling.tooling_core.engine.tide_model import TIDEModel

            source, _ = TIDEModel._assign_external_producer(
                TIDEModel.normalise(source)
            )
            target, _ = TIDEModel._assign_external_producer(
                TIDEModel.normalise(target)
            )

        from tide_tooling.comparator import (
            _compare_tide_models,
            _prepare_tide_comparison,
        )

        source, target, execution_map, state_map = (
            _prepare_tide_comparison(
                source,
                target,
                mapping=mapping,
                execution_map=execution_map,
                state_map=state_map,
            )
        )

        return _compare_tide_models(
            source,
            target,
            mapping=mapping,
            execution_map=execution_map,
            state_map=state_map,
        )

    def _trace_for_exec(self, exec_id):
        record = self._index.exec_by_exec_id[exec_id]
        # A focused execution retains its exact recorded emission subtree.
        # Dropping those descendants would leave the emitter claiming that
        # nodes which did execute had zero executions in the derived Trace.
        exec_ids = {exec_id}
        pending = [exec_id]
        while pending:
            parent_exec_id = pending.pop()
            for child_exec_id in self._index.child_exec_ids_by_exec_id.get(
                parent_exec_id,
                (),
            ):
                if child_exec_id in exec_ids:
                    continue
                exec_ids.add(child_exec_id)
                pending.append(child_exec_id)
        records = [
            candidate
            for candidate in self._execution_records
            if candidate.exec_id in exec_ids
        ]
        # A focused execution view is derived evidence, not a second
        # Event-authoritative run scope. Imported evidence stays imported, but
        # the focused view has no exact source Bundle to re-export.
        return Trace(
            records,
            pipelines=self._pipeline_names,
            runner=record.runner_id,
            ocean_id=self.ocean_id,
            from_cas=self.from_cas,
            name=f"{self.name}: {record.func_name}",
            host=self.host,
            host_record=self._host_record,
        )














    def to_model(
        self,
        *,
        override_host_as=None,
        enrich=True,
        incidence_format="explicit",
        identity_mode="literal",
    ):
        """Project this Trace as a TIDE Model."""
        from tide_tooling.tooling_core.engine.tide_model import TIDEModel

        return TIDEModel.from_trace(
            self,
            override_host_as=override_host_as,
            enrich=enrich,
            incidence_format=incidence_format,
            identity_mode=identity_mode,
        )


    def to_bundle(
        self,
        handle=None,
        unsupported="opaque",
        type=None,
        kind=None,
        path=None,
        serialise_events=False,
        verify_events=True,
        sign=None,
    ):
        """Create or re-export a portable Trace Bundle.

        A live Trace is serialised once into TideRun + CAS.

        A restored Trace never passes reconstructed values back through the
        serializer. It re-exports the exact source TIDEBundle retained during
        import. Export options (``type``, ``kind`` and ``path``) remain valid,
        but serialization options cannot rewrite imported evidence.
        """
        from tide_tooling.tooling_core.bundling.TIDEBundle import TIDEBundle
        from tide_tooling.tooling_core.bundling.TIDEBundler import TIDEBundler

        if not isinstance(serialise_events, bool):
            raise TypeError("serialise_events must be a bool")
        if not isinstance(verify_events, bool):
            raise TypeError("verify_events must be a bool")

        if self.from_cas:
            bundle = self._source_bundle

            if bundle is None:
                raise RuntimeError(
                    "This Trace contains imported Bundle evidence but no exact "
                    "source Bundle is retained. TIDE will not reserialise "
                    "imported evidence."
                )

            if not isinstance(bundle, TIDEBundle):
                raise TypeError(
                    "Restored Trace source Bundle must be a TIDEBundle"
                )

            if handle is not None:
                raise ValueError(
                    "A restored Trace cannot change Bundle metadata during "
                    "to_bundle(); it may only re-export its source Bundle."
                )

            if unsupported != "opaque":
                raise ValueError(
                    "unsupported= is a live-serialization option and cannot "
                    "be applied to a restored Trace."
                )

            if serialise_events and bundle.manifest.get("events") is None:
                raise ValueError(
                    "The source Bundle did not retain Event evidence; a "
                    "restored Trace cannot create a new Event sidecar."
                )

            # False does not strip an Event sidecar already in the source.
            # Re-export means preserving the imported envelope exactly.
        else:
            bundler = TIDEBundler(
                unsupported=unsupported,
                verify_events=verify_events,
            )
            bundle = TIDEBundle.from_trace(
                self,
                handle=handle,
                bundler=bundler,
                serialise_events=serialise_events,
            )

        if sign is not None:
            bundle = bundle.sign(sign)

        if type is None and kind is None and path is None:
            return bundle

        export_type = type or ("file" if path is not None else "dict")
        export_kind = kind or (
            "json" if export_type == "file" else "python"
        )
        return bundle.export(
            type=export_type,
            kind=export_kind,
            path=path,
        )

    @staticmethod
    def _is_val_hash(hash):
        return isinstance(hash, str) and hash.startswith("sha3_512_tide:")

    @staticmethod
    def _split_state_identity(value, version):
        if not isinstance(value, str) or not value.endswith("]"):
            return value, version

        reference, marker, suffix = value.rpartition("[v")
        if not marker or not suffix[:-1].isdigit():
            return value, version

        identity_version = int(suffix[:-1])
        if version is not None and version != identity_version:
            raise ValueError("Supplied version conflicts with the State identity")
        return reference, identity_version

    @staticmethod
    def _split_actor_occurrence(value, version):
        if not isinstance(value, str) or not value.endswith("]"):
            return value, version

        actor, marker, suffix = value.rpartition("[")
        if not marker or not suffix[:-1].isdigit():
            return value, version

        occurrence = int(suffix[:-1])
        if version is not None and version != occurrence:
            raise ValueError("Supplied version conflicts with the Actor occurrence")
        return actor, occurrence

    def _pattern_match(self, ref, pattern):
        import fnmatch
        return fnmatch.fnmatch(ref, f"*{pattern}*")

    def find(self, value, version=None):
        value, version = self._split_state_identity(value, version)
        actor_value, actor_version = self._split_actor_occurrence(value, version)
        if actor_value not in self._index.func_name_to_actors:
            actor_value, actor_version = value, version
        execs = DedupedList()
        actors, refs, effects, values = [], [], [], []
        nearby_actors, nearby_refs, nearby_execs = [], [], []

        # An exact execution identity is always a find match.
        try:
            if value in self._execs:
                execs.add(self.inspect(value, "exec_record"))
        except TypeError:
            pass

        if isinstance(value, str):
            if value in self._index.value_by_hash:
                values.append(self.inspect(value, "hash"))

            if value in self._index.seen_refs:
                inspected = self.inspect(value, "reference", version)
                if version is None or version in inspected.history:
                    refs.append(inspected)

            for reference in self._index.seen_refs:
                candidate = (
                    reference
                    if ":" in value
                    else reference.split(":", 1)[-1]
                )
                if reference != value and self._pattern_match(candidate, value):
                    inspected = self.inspect(reference, "reference", version)
                    if version is None or version in inspected.history:
                        nearby_refs.append(inspected)

            
            for name, matched_actors in self._index.func_name_to_actors.items():
                if isinstance(name, str) and self._pattern_match(name, actor_value):
                    for actor in matched_actors:
                        inspected = self.inspect(actor, "actor", version=actor_version)
                        if inspected.exists:
                            nearby_actors.append(inspected)

            # Function-name matches retain their exact execution occurrences.
            nearby_execs.extend(
                self.inspect(execution.exec_id, "exec_record")
                for execution in self._execution_records
                if isinstance(execution.func_name, str)
                and self._pattern_match(execution.func_name, value)
            )

        if isinstance(value, Actor) or (
            isinstance(value, tuple) and len(value) == 3
        ):
            actor = value if isinstance(value, Actor) else Actor(*value)
            if actor in self._index.actor_to_exec_ids:
                actors.append(self.inspect(actor, "actor", version=version))

                

        if isinstance(value, int):
            execution = next(
                (record for record in self._execution_records if record.seq_id == value),
                None,
            )
            if execution is not None:
                execs.add(self.inspect(execution.exec_id, "exec_record"))

        if isinstance(value, Effects):
            inspected = self.inspect(value, "effect")
            if inspected.exists:
                effects.append(inspected)
        elif isinstance(value, tuple) and len(value) == 2:
            inspected = self.inspect(value, "effect", version)
            if inspected.exists:
                effects.append(inspected)

        nearby = Nearby(
            value,
            DedupedList(nearby_actors),
            DedupedList(nearby_refs),
            DedupedList(nearby_execs),
        )
        return Searched(value, execs, actors, refs, effects, values, nearby)

    def _execution_for(self, identifier):
        try:
            execution = self._execs.get(identifier)
        except TypeError:
            execution = None

        if execution is not None:
            return execution

        if isinstance(identifier, int) and not isinstance(identifier, bool):
            execution = next((record for record in self._execution_records if record.seq_id == identifier), None)
        elif isinstance(identifier, str):
            execution = None
        else:
            raise TypeError("Execution identity must be a seq_id or exec_id")

        if execution is None:
            raise KeyError(f"Execution {identifier!r} was not found")

        return execution

    def inputs_outputs_of(self, identifier):
        return ExecValues(self._execution_for(identifier))

    def inputs_of(self, identifier):
        values = self.inputs_outputs_of(identifier)
        values.view = "inputs"
        return values

    def output_of(self, identifier):
        values = self.inputs_outputs_of(identifier)
        values.view = "output"
        return values

    @staticmethod
    def _definition_inputs(record):
        records = (
            dict(record.inputs.items())
            if hasattr(record.inputs, "items")
            else {}
        )
        required = Definition.INPUT_NAMES
        if not set(required).issubset(records):
            raise RuntimeError(
                "Recorded Node definition is missing declared input slots"
            )
        return {
            name: records[name].raw
            for name in required
        }

    @staticmethod
    def _definition_options(record):
        retained = getattr(record, "node_options", {}) or {}
        return {
            name: retained[name]
            for name in (
                "force_update",
                "one_shot",
                "unpack",
                "share_implicit_target",
            )
            if name in retained
        }

    @staticmethod
    def _definition_field(record, name):
        if name == "definition_id":
            return getattr(
                record,
                "base_node_id",
                getattr(record, "base_node", None),
            )
        if name == "inputs":
            return Trace._definition_inputs(record)
        if name == "options":
            return Trace._definition_options(record)
        return getattr(record, name)

    def definition_of(self, identifier):
        """Return the exact retained definition for an execution or ID."""
        execution = None
        try:
            execution = self._execution_for(identifier)
        except KeyError:
            if not isinstance(identifier, str):
                raise

        definition_id = (
            execution.base_node_id
            if execution is not None
            else identifier
        )
        if definition_id is None:
            raise KeyError(
                f"Execution {identifier!r} has no recorded Node definition"
            )

        executions = tuple(
            record
            for record in self._execution_records
            if record.base_node_id == definition_id
        )
        emitted = tuple(
            node
            for record in self._execution_records
            for node in record.emitted_nodes
            if node.base_node == definition_id
        )
        emitted_by_exec_ids = {
            node.emitted_by_exec_id
            for node in emitted
        }
        origins = {node.origin for node in emitted}
        if len(emitted_by_exec_ids) > 1 or len(origins) > 1:
            raise RuntimeError(
                "Conflicting registration evidence for Node definition: "
                f"{definition_id!r}"
            )
        authorities = (*emitted, *executions)
        if not authorities:
            raise KeyError(
                f"Node definition {definition_id!r} was not found"
            )

        authority = authorities[0]
        field_names = (
            "base_node_content_id",
            "func_name",
            "inputs",
            "declared_deps",
            "declared_targets",
            "declared_effects",
            "options",
        )
        for other in authorities[1:]:
            for field_name in field_names:
                expected = self._definition_field(authority, field_name)
                actual = self._definition_field(other, field_name)
                if not safe_equal(expected, actual):
                    raise RuntimeError(
                        "Conflicting retained evidence for Node definition "
                        f"{definition_id!r}: {field_name} disagrees"
                    )

        sources = []
        for record in authorities:
            source = {
                "ocean_id": getattr(
                    record,
                    "source_ocean_id",
                    getattr(record, "ocean_id", None),
                ),
                "runner_id": record.runner_id,
                "pipeline_id": record.pipeline_id,
            }
            if source not in sources:
                sources.append(source)

        return Definition(
            definition_id=definition_id,
            content_id=authority.base_node_content_id,
            name=authority.func_name,
            sources=sources,
            inputs=self._definition_inputs(authority),
            declared_deps=authority.declared_deps,
            declared_targets=authority.declared_targets,
            declared_effects=authority.declared_effects,
            node_options=self._definition_options(authority),
            emitted=bool(emitted),
            emitted_by_exec_id=(
                next(iter(emitted_by_exec_ids)) if emitted else None
            ),
            origin=(next(iter(origins)) if emitted else None),
            executions=executions,
        )

    def used_as(self, reference, version=None, seq_id=None):
        """Return every recorded use of one exact State occurrence."""
        if isinstance(reference, Inspected):
            if reference.type != "reference":
                raise TypeError("used_as requires an exact State reference")
            if (
                version is not None
                and reference.version is not None
                and version != reference.version
            ):
                raise ValueError(
                    "Supplied version conflicts with the inspected reference"
                )
            version = reference.version if version is None else version
            reference = reference.id

        reference, version = self._split_state_identity(reference, version)
        if not isinstance(reference, str):
            raise TypeError("used_as requires an exact State reference")
        if version is None:
            raise ValueError("used_as requires an exact State version")
        if not isinstance(version, int) or isinstance(version, bool) or version < 0:
            raise ValueError("used_as version must be a non-negative integer")
        if seq_id is not None and (
            not isinstance(seq_id, int)
            or isinstance(seq_id, bool)
            or seq_id < 0
        ):
            raise ValueError("used_as seq_id must be a non-negative integer or None")

        state = (reference, version)
        stored_hash = self._index.value_hash_by_ref_version.get(state)
        if stored_hash is None:
            raise KeyError(f"Unknown TIDE State: {reference}[v{version}]")
        stored_snapshot = self._index._value_snapshots_by_hash.get(stored_hash)
        if stored_snapshot is None:
            raise RuntimeError("TIDE State has no retained value snapshot")

        selected = tuple(
            execution
            for execution in self._execution_records
            if seq_id is None or execution.seq_id == seq_id
        )
        if seq_id is not None and not selected:
            raise KeyError(f"Execution seq_id {seq_id!r} was not found")

        observations = []
        for execution in selected:
            for dependency in execution.deps:
                dynamic = dependency.type == "dynamic"
                for read in self._index._concretised_from(dependency):
                    if (
                        read.unscoped_reference,
                        read.resolved_version,
                    ) != state:
                        continue
                    snapshot = read._tide_snapshot("resolved_value")
                    if snapshot is None:
                        raise RuntimeError(
                            "TIDE State use has no retained value snapshot"
                        )
                    associated = read.associated_input
                    input_name = (
                        associated[0]
                        if isinstance(associated, (tuple, list)) and associated
                        else associated
                        if isinstance(associated, str)
                        else associated.get("name")
                        if isinstance(associated, dict)
                        else getattr(associated, "name", None)
                    )
                    read_type = (
                        "dynamic"
                        if dynamic
                        else "runtime_read"
                        if input_name == "runtime_read"
                        else read.type
                    )
                    observations.append((
                        execution,
                        read,
                        snapshot,
                        read_type,
                    ))

        return UsedAs(
            reference,
            version,
            stored_snapshot,
            observations,
        )

    def _value_hash_divergence(self, value_hash):
        return next(
            (
                warning
                for warning in self.divergence.get("warnings", ())
                if isinstance(warning, dict)
                and warning.get("type") == "value_hash_mismatch"
                and warning.get("expected") == value_hash
            ),
            None,
        )

    def _recorded_state_value_descriptor(self, reference, version):
        # EventReaders put immutable snapshot descriptors in snapshot_lineage.
        # Presentation may read those facts but must never replace them with
        # facts inferred from a reconstructed bundle value.
        state = (reference, version)
        value_hash = self._index.value_hash_by_ref_version.get(state)
        if value_hash is None:
            return None

        records = (
            tuple(
                self._index.write_records_by_ref_version.get(
                    state,
                    (),
                )
            )
            + tuple(
                self._index.read_records_by_ref_version.get(
                    state,
                    (),
                )
            )
        )

        for record in records:
            lineage = getattr(record, "snapshot_lineage", ()) or ()
            for descriptor in reversed(lineage):
                if (
                    isinstance(descriptor, dict)
                    and descriptor.get("value_hash") == value_hash
                ):
                    return descriptor

        return {"value_hash": value_hash}

    def _recorded_type_for_hash(self, value_hash):
        types = {
            descriptor.get("type")
            for state in self._index.ref_versions_by_value_hash.get(
                value_hash,
                (),
            )
            for descriptor in (
                self._recorded_state_value_descriptor(*state),
            )
            if isinstance(descriptor, dict)
            and isinstance(descriptor.get("type"), str)
        }
        if len(types) == 1:
            return next(iter(types))
        if len(types) > 1:
            return "multiple"
        return None

    def _warn_divergent_value_access(
        self,
        value_hash,
        *,
        reference=None,
        version=None,
        stacklevel=3
    ):
        mismatch = self._value_hash_divergence(value_hash)
        if mismatch is None:
            return

        from warnings import warn

        target = (
            f"{reference}[v{version}]"
            if reference is not None and version is not None
            else value_hash
        )
        recorded_type = (
            self._recorded_state_value_descriptor(reference, version).get("type")
            if reference is not None
            and version is not None
            and isinstance(
                self._recorded_state_value_descriptor(reference, version),
                dict,
            )
            else self._recorded_type_for_hash(value_hash)
        )
        reconstructed = self._index.value_by_hash.get(value_hash)
        reconstructed_type = (
            type(reconstructed).__name__
            if reconstructed is not None
            else "unknown"
        )

        warn(
            f"Divergent value for {target}:\n"
            f"  recorded as:      {recorded_type or 'unknown'}\n"
            f"  reconstructed as: {reconstructed_type}",
            RuntimeWarning,
            stacklevel=stacklevel,
        )

    def value_of(self, reference_or_hash, version=None, show=False):
        from tide_tooling.tooling_core.engine.utils import detached_tide_value

        if isinstance(reference_or_hash, Inspected):
            if reference_or_hash.type != "reference":
                raise TypeError("value_of requires a reference or value hash")
            if version is not None and reference_or_hash.version is not None and version != reference_or_hash.version:
                raise ValueError("Supplied version conflicts with the inspected reference")
            version = reference_or_hash.version if version is None else version
            reference_or_hash = reference_or_hash.id

        reference_or_hash, version = self._split_state_identity(
            reference_or_hash,
            version,
        )

        if not isinstance(reference_or_hash, str):
            raise TypeError("value_of requires a reference or value hash")
        if not isinstance(show, bool):
            raise TypeError("show must be a bool")

        # Resolve an exact value hash.
        if self._is_val_hash(reference_or_hash):
            if version is not None:
                raise ValueError("A value hash does not use a version")
            if reference_or_hash not in self._index.value_by_hash:
                raise KeyError(f"Unknown TIDE value hash: {reference_or_hash}")

            value = self._index.value_by_hash[reference_or_hash]
            if show:
                self._show_value_of(
                    value,
                    value_hash=reference_or_hash,
                )
            else:
                self._warn_divergent_value_access(
                    reference_or_hash,
                    stacklevel=3,
                )
            return detached_tide_value(value)

        # Resolve an exact reference.
        if reference_or_hash in self._index.versions_by_ref:
            return self._value_of_reference(reference_or_hash, version=version, show=show)

        # Resolve an unqualified or partial reference.
        searched = self.find(reference_or_hash, version)
        references = sorted({
            inspected.id
            for inspected in searched.get_exact("reference") + searched.get_nearby("reference")
        })

        if not references:
            raise KeyError(f"Unknown TIDE reference: {reference_or_hash}")

        if len(references) == 1:
            return self._value_of_reference(references[0], version=version, show=show)

        values = []
        for reference in references:
            found = self._value_of_reference(reference, version=version, show=show)
            values.extend(found if version is None else [found])
        return values

    def _value_of_reference(self, reference, *, version=None, show=False):
        versions = list(self._index.versions_by_ref.get(reference, ()))
        if not versions:
            raise KeyError(f"Unknown TIDE reference: {reference}")

        selected_versions = sorted(versions) if version is None else [version]
        versioned_values = []

        for selected_version in selected_versions:
            ref_version = (reference, selected_version)
            value_hash = self._index.value_hash_by_ref_version.get(ref_version)
            if value_hash is None or value_hash not in self._index.value_by_hash:
                raise KeyError(f"Unknown TIDE reference version: {reference}[v{selected_version}]")
            versioned_values.append((selected_version, value_hash, self._index.value_by_hash[value_hash]))

        if version is not None:
            value_hash = versioned_values[0][1]
            value = versioned_values[0][2]
            if show:
                self._show_value_of(
                    value,
                    reference=reference,
                    version=version,
                    value_hash=value_hash,
                )
            else:
                self._warn_divergent_value_access(
                    value_hash,
                    reference=reference,
                    version=version,
                    stacklevel=4,
                )
            from tide_tooling.tooling_core.engine.utils import detached_tide_value
            return detached_tide_value(value)

        from tide_tooling.tooling_core.engine.utils import detached_tide_value
        values = [
            detached_tide_value(value)
            for _, _, value in versioned_values
        ]
        if show:
            self._show_value_of(
                values,
                reference=reference,
                versions=versioned_values,
            )
        else:
            warned_hashes = set()
            for value_version, value_hash, _ in versioned_values:
                if value_hash in warned_hashes:
                    continue
                warned_hashes.add(value_hash)
                self._warn_divergent_value_access(
                    value_hash,
                    reference=reference,
                    version=value_version,
                )
        return values

    def show_value_of(self, reference_or_hash, version=None):
        return self.value_of(reference_or_hash, version=version, show=True)

    def _show_value_of(
        self,
        value,
        reference=None,
        version=None,
        value_hash=None,
        versions=None,
    ):
        if versions is not None:
            divergent_versions = {
                value_version: mismatch
                for value_version, version_hash, _ in versions
                for mismatch in (
                    self._value_hash_divergence(version_hash),
                )
                if mismatch is not None
            }
            summary = (
                f"{len(versions)} "
                f"{'value' if len(versions) == 1 else 'values'} found"
            )
            if divergent_versions:
                summary += f" | {SHOW_RED}DIVERGENT{SHOW_RESET}"
            identifier = f"Reference: {reference}"
        else:
            mismatch = self._value_hash_divergence(value_hash)
            summary = "1 value found"
            if mismatch is not None:
                summary += f" | {SHOW_RED}DIVERGENT{SHOW_RESET}"
            identifier = (
                f"Reference: {reference}[v{version}]"
                if reference is not None
                else f"Value hash: {value_hash}"
            )

        lines = show_header("VALUE", summary, identifier)

        if versions is not None:
            lines.extend(show_section("VERSIONS"))
            for value_version, version_hash, version_value in versions:
                mismatch = self._value_hash_divergence(version_hash)
                descriptor = self._recorded_state_value_descriptor(
                    reference,
                    value_version,
                )
                recorded_type = (
                    descriptor.get("type")
                    if isinstance(descriptor, dict)
                    else None
                )

                marker = (
                    f"{SHOW_RED}!{SHOW_RESET}"
                    if mismatch is not None
                    else ""
                )
                lines.append(
                    f"  {marker}{SHOW_CYAN}[v{value_version}]{SHOW_RESET} "
                    f"{SHOW_DIM}{version_hash}{SHOW_RESET}"
                )

                if mismatch is not None:
                    lines.append(
                        f"      {SHOW_DIM}recorded as:{SHOW_RESET} "
                        f"{SHOW_WHITE}{recorded_type or 'unknown'}{SHOW_RESET}"
                    )
                    lines.append(
                        f"      {SHOW_DIM}reconstructed as:{SHOW_RESET} "
                        f"{SHOW_WHITE}{self._show_value_name(version_value)}{SHOW_RESET}"
                    )
                    lines.append(
                        f"      {SHOW_DIM}reconstructed hash:{SHOW_RESET} "
                        f"{SHOW_RED}{mismatch.get('observed')}{SHOW_RESET}"
                    )
                    lines.append(
                        f"      {SHOW_DIM}reconstructed value:{SHOW_RESET} "
                        f"{SHOW_WHITE}{repr(version_value)}{SHOW_RESET}"
                    )
                else:
                    lines.append(
                        f"      {SHOW_DIM}type:{SHOW_RESET} "
                        f"{SHOW_WHITE}{recorded_type or self._show_value_name(version_value)}{SHOW_RESET}"
                    )
                    lines.append(
                        f"      {SHOW_DIM}value:{SHOW_RESET} "
                        f"{SHOW_WHITE}{repr(version_value)}{SHOW_RESET}"
                    )
        else:
            mismatch = self._value_hash_divergence(value_hash)

            if mismatch is not None:
                descriptor = (
                    self._recorded_state_value_descriptor(
                        reference,
                        version,
                    )
                    if reference is not None
                    else None
                )
                recorded_type = (
                    descriptor.get("type")
                    if isinstance(descriptor, dict)
                    else self._recorded_type_for_hash(value_hash)
                )

                lines.extend(
                    show_section("RECORDED AS")
                )
                lines.append(
                    f"  {SHOW_DIM}type:{SHOW_RESET} "
                    f"{SHOW_WHITE}{recorded_type or 'unknown'}{SHOW_RESET}"
                )
                lines.append(
                    f"  {SHOW_DIM}hash:{SHOW_RESET} "
                    f"{SHOW_CYAN}{value_hash}{SHOW_RESET}"
                )
                if (
                    isinstance(descriptor, dict)
                    and isinstance(descriptor.get("repr"), str)
                ):
                    lines.append(
                        f"  {SHOW_DIM}value:{SHOW_RESET} "
                        f"{SHOW_WHITE}{descriptor['repr']}{SHOW_RESET}"
                    )

                lines.extend(show_section("RECONSTRUCTED AS"))
                lines.append(
                    f"  {SHOW_DIM}type:{SHOW_RESET} "
                    f"{SHOW_WHITE}{self._show_value_name(value)}{SHOW_RESET}"
                )
                lines.append(
                    f"  {SHOW_DIM}hash:{SHOW_RESET} "
                    f"{SHOW_RED}{mismatch.get('observed')}{SHOW_RESET}"
                )
                lines.append(
                    f"  {SHOW_DIM}value:{SHOW_RESET} "
                    f"{SHOW_WHITE}{repr(value)}{SHOW_RESET}"
                )
            else:
                descriptor = (
                    self._recorded_state_value_descriptor(
                        reference,
                        version,
                    )
                    if reference is not None
                    else None
                )
                recorded_type = (
                    descriptor.get("type")
                    if isinstance(descriptor, dict)
                    else None
                )

                lines.extend(show_section("VALUE"))
                lines.append(
                    f"  {SHOW_DIM}hash:{SHOW_RESET} "
                    f"{SHOW_CYAN}{value_hash}{SHOW_RESET}"
                )
                lines.append(
                    f"  {SHOW_DIM}type:{SHOW_RESET} "
                    f"{SHOW_WHITE}{recorded_type or self._show_value_name(value)}{SHOW_RESET}"
                )
                lines.append(
                    f"  {SHOW_DIM}value:{SHOW_RESET} "
                    f"{SHOW_WHITE}{repr(value)}{SHOW_RESET}"
                )

        return show_output(lines)

    def show(
        self,
        color=True,
        simple=False,
        colour=None,
        structural=False,
        full_trace=False,
        show_sources=False,
        show_undeclared=False,
        values=False,
    ):
        """Render this Trace, optionally through its canonical TIDE structure.

        ``structural=True`` uses the same canonicalisation as structural model
        export and diff, then renders that view with the ordinary Trace layout
        and colour rules. The default view retains the Trace's supplied
        identities and enrichment.
        """
        # Select the requested presentation boundary.
        if not isinstance(full_trace, bool):
            raise TypeError("show full_trace must be a bool")
        if not isinstance(show_sources, bool):
            raise TypeError("show show_sources must be a bool")
        if not isinstance(show_undeclared, bool):
            raise TypeError("show show_undeclared must be a bool")
        if not isinstance(values, bool):
            raise TypeError("show values must be a bool")

        def is_autogenerated(reference):
            return DataRef.from_string(
                reference,
                prefix=False,
            ).is_autogenerated

        def is_autogenerated_execution(execution):
            writes = tuple(execution.writes)
            return (
                execution.producer_id.source_actor == "TIDE: External"
                and writes
                and all(
                    is_autogenerated(write.stored_target)
                    for write in writes
                )
            )

        if not full_trace and self._scope_runner_ids:
            shown_exec_ids = {
                execution.exec_id
                for execution in self._execution_records
                if (
                    execution.runner_id in self._scope_runner_ids
                    or (
                        show_undeclared
                        and is_autogenerated_execution(execution)
                    )
                )
            }
            shown_trace = self._derived_view(
                log=tuple(
                    record
                    for record in self._records
                    if record.exec_id in shown_exec_ids
                ),
            )
            return shown_trace.show(
                color=color,
                simple=simple,
                colour=colour,
                structural=structural,
                full_trace=True,
                show_sources=show_sources,
                show_undeclared=show_undeclared,
                values=values,
            )

        color, simple = _show_options(color, simple, colour)
        if not isinstance(structural, bool):
            raise TypeError("show structural must be a bool")
        if structural:
            from tide_tooling.core import TIDE

            structural_model = self.to_model(
                enrich=False,
                identity_mode="structural",
            )
            structural_trace = TIDE.from_model(
                structural_model,
                name=f"{self.name} [structural]",
            )
            structural_trace = structural_trace._derived_view(
                log=structural_trace._log,
                ocean_id="structural",
            )
            return structural_trace.show(
                color=color,
                simple=simple,
                show_sources=show_sources,
                show_undeclared=show_undeclared,
                values=values,
            )
        # 
        # Tide Run 0 
        # Pipeline [name or id]
        # Executions
        # Writes
        # Status
        #

        def not_template(value):
            return True if not isinstance(value, __fallback__) else False


        def dependency_occurrence_id(dependency):
            return dependency.observation_id

        def write_occurrence_id(write):
            cause = getattr(write, "cause", None)
            return (
                getattr(cause, "event_id", None),
                write.stored_target,
                write.stored_version,
            )

        def effect_occurrence_id(effect):
            return effect.event_id

        immediate_dependencies = {
            dependency_occurrence_id(dependency)
            for record in self._records
            if isinstance(record, ImmediateEffectRecord)
            for dependency in record.deps
        }
        immediate_writes = {
            write_occurrence_id(write)
            for record in self._records
            if isinstance(record, ImmediateEffectRecord)
            for write in record.writes
        }
        immediate_effects = {
            effect_occurrence_id(effect)
            for record in self._records
            if isinstance(record, ImmediateEffectRecord)
            for effect in record.effects
        }

        def shown_dependencies(execution):
            return tuple(
                dependency
                for dependency in execution.deps
                if (
                    isinstance(execution, ImmediateEffectRecord)
                    or dependency_occurrence_id(dependency) not in immediate_dependencies
                )
                if show_undeclared or not is_autogenerated(dependency.unscoped_reference)
            )

        def shown_writes(execution):
            return tuple(
                write
                for write in execution.writes
                if (
                    isinstance(execution, ImmediateEffectRecord)
                    or write_occurrence_id(write) not in immediate_writes
                )
                if show_undeclared or not is_autogenerated(write.stored_target)
            )

        shown_log = tuple(
            execution
            for execution in self._execution_records
            if show_undeclared or not is_autogenerated_execution(execution)
        )
        shown_exec_ids = {execution.exec_id for execution in shown_log}
        shown_records = tuple(
            record
            for record in self._records
            if record.exec_id in shown_exec_ids
            or isinstance(record, ImmediateEffectRecord)
        )

        def ref_colour(ref, ver):
            consumers = self._index.consumer_exec_ids_by_ref_version.get(
                (ref, ver),
                [],
            )
            return (
                SHOW_RED
                if (ref, ver) in self._index.deleted_ref_versions
                or not consumers
                else SHOW_GREEN
            )

        divergent_states = {
            state
            for warning in self.divergence.get("warnings", ())
            if isinstance(warning, dict)
            and warning.get("type") == "value_hash_mismatch"
            and isinstance(warning.get("expected"), str)
            for state in self._index.ref_versions_by_value_hash.get(
                warning["expected"],
                (),
            )
        }

        def divergent_state_marker(ref, ver):
            return (
                f"{SHOW_RED}!{SHOW_RESET}"
                if (ref, ver) in divergent_states
                else ""
            )

        def effect_ref_colour(log, effect_entry):
            effect = effect_entry.effect
            ref, ver = effect_entry.targets

            if effect == Effects.DELETE:
                return SHOW_RED if effect_entry.target_existed is True else SHOW_YELLOW
            if effect == Effects.READ:
                return (
                    SHOW_GREEN
                    if effect_entry.target_existed is True
                    else SHOW_RED
                ) 
            if ver == None:
                return SHOW_YELLOW
            
            return ref_colour(ref, ver)

        def lines_from_write(
            cause,
            result_name,
            ref,
            ver,
            updated,
            observed_mutable,
        ):
            cause = cause.description
            link = "as"
            action = "updated:" if updated else "wrote:"
            colour = ref_colour(ref, ver)
            state = _state_occurrence_text(
                ref,
                ver,
                observed_mutable=observed_mutable,
            )
            marker = divergent_state_marker(ref, ver)
            lines = []
            if cause.startswith("TIDE:"):
                cause_str = cause.split(" ", 1)[1]

                if "TIDE: Emitted Node Polling" in cause:
                    action =  "found"
                    cause = "polled emitted node deps"
                
                match cause_str:
                    case "user declared value":
                        action = "user declared"
                        cause = ""
                        link = "as"
                        return [
                            f"    {SHOW_DIM}{action}{SHOW_RESET} "
                            f"{SHOW_WHITE}{result_name}{SHOW_RESET} "
                            f"{SHOW_DIM}{link}{SHOW_RESET} "
                            f"{marker}{colour}{state}{SHOW_RESET}"
                        ]
                        
                    case "observed value":
                        action = "updated"
                        cause = "value changed with no actor"
                        link = "as"
                        return [
                            f"    {SHOW_DIM}{action}{SHOW_RESET} "
                            f"{colour}{state}{SHOW_RESET} "
                            f"{SHOW_DIM}{link}{SHOW_RESET} "
                            f"{SHOW_WHITE}{result_name}{SHOW_RESET} "
                            f"{SHOW_DIM}because {cause}{SHOW_RESET}"
                        ]
                    case "Pre-runtime Polling":
                        action = "found"
                        cause = "polling"
                        link = "as"
            lines.append(
                f"    {SHOW_DIM}{action}{SHOW_RESET} "
                f"{SHOW_WHITE}{result_name}{SHOW_RESET} "
                f"{SHOW_DIM}{link}{SHOW_RESET} "
                f"{marker}{colour}{state}{SHOW_RESET}"
            )
            if cause and str(cause).lower() != "unknown":
                lines[-1] += (
                    f" {SHOW_DIM}because {cause}{SHOW_RESET}"
                )
            return lines
                    
        
        execution_count = len(shown_log)
        execution_word = "execution" if execution_count == 1 else "executions"
        read_count = sum(
            len(self._index._concretised_from(dependency))
            for execution in shown_log
            for dependency in shown_dependencies(execution)
        )
        read_word = "read" if read_count == 1 else "reads"
        write_count = sum(len(shown_writes(execution)) for execution in shown_log)
        write_word = "write" if write_count == 1 else "writes"
        summary = (
            f"{execution_count} {execution_word} shown | "
            f"{write_count} {write_word} | "
            f"{read_count} {read_word}"
        )
        if self.divergent:
            summary += f" | {SHOW_RED}DIVERGENT{SHOW_RESET}"
        elif self.scope == "complete":
            summary += f" | {SHOW_GREEN}COMPLETE{SHOW_RESET}"
        else:
            summary += f" | {SHOW_YELLOW}AMBIGUOUS{SHOW_RESET}"
        if self.missing_referenced_events:
            missing_count = len(self.missing_referenced_events)
            summary += f" | {SHOW_RED}CORRUPTED: {missing_count} missing causal Events{SHOW_RESET}"
        if self.causal_order_violations:
            violation_count = len(self.causal_order_violations)
            summary += f" | {SHOW_RED}CORRUPTED: {violation_count} causal ordering violations{SHOW_RESET}"
        if self.deadlocked:
            waiting_count = sum(
                len(waiting)
                for waiting in self._deadlock.values()
            )
            summary += (
                f" | {SHOW_RED}DEADLOCKED: {waiting_count} "
                f"waiting node{'s' if waiting_count != 1 else ''}{SHOW_RESET}"
            )
        if self.host is not None:
            summary += f" | host: {self.host}"
        sources = tuple(
            {
                "run": runner_id,
                "pipeline_id": pipeline_id,
                "pipeline": self._get_pipeline_named_tuple_from_id(
                    pipeline_id
                )[1],
            }
            for runner_id, pipeline_id in dict.fromkeys(
                (execution.runner_id, execution.pipeline_id)
                for execution in shown_log
            )
        )
        source_runs = tuple(dict.fromkeys(source["run"] for source in sources))
        source_pipelines = tuple(dict.fromkeys(source["pipeline"] for source in sources))
        run_label = "Tide Run" if len(source_runs) <= 1 else "Tide Runs"
        pipeline_label = "Pipeline" if len(source_pipelines) <= 1 else "Pipelines"
        run = (
            source_runs[0]
            if len(source_runs) == 1
            else f"MULTIPLE ({len(source_runs)})"
            if source_runs
            else "TIDE: UNKNOWN"
        )
        pipeline = (
            source_pipelines[0]
            if len(source_pipelines) == 1
            else f"MULTIPLE ({len(source_pipelines)})"
            if source_pipelines
            else "TIDE: UNKNOWN"
        )
        identifier = (
            f"Ocean: {self.ocean_id} | "
            f"{run_label}: {run} | "
            f"{pipeline_label}: {pipeline}"
        )
        lines = show_header("TRACE", summary, identifier)
        lines.extend(show_section("RUN"))
        if self._pipeline_starting_event_id is not None:
            start_seq_id = next(
                (
                    index
                    for index, event in enumerate(self.events)
                    if event.event_id
                    == self._pipeline_starting_event_id
                ),
                None,
            )
            marker = "---- PIPELINE START"
            if start_seq_id is not None:
                marker += f" [Event: {start_seq_id}]"
            lines.append(f"{SHOW_GREEN}{marker} ----{SHOW_RESET}")
        if show_sources:
            lines.append(
                f"{SHOW_DIM}Sources:    "
                f"{len(sources)}{SHOW_RESET}"
            )
            if len(sources) > 1:
                for source_idx, source in enumerate(sources):
                    lines.append(
                        f"  {SHOW_CYAN}[{source_idx}]{SHOW_RESET} "
                        f"{SHOW_WHITE}{source['run']}{SHOW_RESET} "
                        f"{SHOW_DIM}| Pipeline: {source['pipeline']}{SHOW_RESET}"
                    )
        lines.extend(show_section("EXECUTIONS"))

        presentation_items = list(shown_records)
        presentation_items.extend(
            attempt
            for attempt in self._ready_attempts
            if attempt.outcome != "completed"
            and (
                self._selected_runner_id is None
                or attempt.runner_id in self._scope_runner_ids
            )
        )
        presentation_items.sort(
            key=lambda item: (
                getattr(item, "event_seq_id", None) is None,
                getattr(item, "event_seq_id", 0) or 0,
            )
        )

        for log in presentation_items:
            if isinstance(log, ReadyAttemptRecord):
                outcome = (
                    "ATTEMPTED NODE_READY"
                    if log.outcome == "ready"
                    else f"NODE_READY {log.outcome.upper()}"
                )
                lines.append(
                    f"{SHOW_RED if self.scope == 'complete' and log.outcome == 'ready' else SHOW_YELLOW}"
                    f"[Event: {log.event_seq_id}] {outcome}: "
                    f"{log.actor.source_actor}{SHOW_RESET}"
                )
                continue

            producer = log.producer_id
            producer_id_repr = (
                producer.source_actor
                if producer is not None
                else "TIDE: Unknown Attempt"
            )
            if isinstance(log, ImmediateEffectRecord):
                status = (
                    f"EXEC_RECORD[{log.execution_reference.resolved_seq_id}]"
                    if log.execution_reference.resolved
                    else repr(log.execution_reference.ambiguous_occurrence)
                )
                lines.append(
                    f"{SHOW_CYAN}{log.seq_id}: IMMEDIATE EFFECT "
                    f"{producer_id_repr} -> {status}{SHOW_RESET}"
                )
            else:
                lines.append(f"{SHOW_CYAN}{log.seq_id}: {producer_id_repr}{SHOW_RESET}")

            parent_exec_id = getattr(log, "emitted_by_exec_id", None)
            if parent_exec_id is not None:
                parent = self._execs.get(parent_exec_id)
                parent_display = (
                    f"EXEC_RECORD[{parent.seq_id}]"
                    if parent is not None
                    else parent_exec_id
                )
                lines.append(
                    f"     {SHOW_DIM}parent_exec:{SHOW_RESET} "
                    f"{SHOW_CYAN}{parent_display}{SHOW_RESET}"
                )

            # 
            # handle dependencies 
            deps = shown_dependencies(log)
            if not show_undeclared:
                for dependency in log.deps:
                    if is_autogenerated(dependency.unscoped_reference):
                        value_type = self._show_value_name(dependency.resolved_value)
                        lines.append(
                            f"     {SHOW_DIM}used:{SHOW_RESET} "
                            f"{SHOW_WHITE}{value_type}{SHOW_RESET} "
                            f"{SHOW_DIM}as an{SHOW_RESET} "
                            f"{SHOW_WHITE}undeclared_input{SHOW_RESET}"
                        )
            if not_template(deps) and deps:
                for dep_entry in deps:
                    dep_entry: DependencyUseRecord
                    ref = dep_entry.unscoped_reference
                    dep_type = dep_entry.type
                    if dep_type == "dynamic":
                        lines.append(
                            f"     {SHOW_DIM}used:{SHOW_RESET} "
                            f"{SHOW_GREEN}{ref}[dynamic]{SHOW_RESET}"
                        )
                        for expanded_entry in dep_entry.expanded_to or ():
                            expanded_state = _state_occurrence_text(
                                expanded_entry.unscoped_reference,
                                expanded_entry.resolved_version,
                                observed_mutable=(
                                    expanded_entry.observed_mutable
                                ),
                            )
                            lines.append(
                                f"        {SHOW_DIM}->{SHOW_RESET} "
                                f"{divergent_state_marker(expanded_entry.unscoped_reference, expanded_entry.resolved_version)}"
                                f"{SHOW_GREEN}{expanded_state}{SHOW_RESET}"
                            )
                        continue

                    state = _state_occurrence_text(
                        ref,
                        dep_entry.resolved_version,
                        observed_mutable=dep_entry.observed_mutable,
                    )
                    marker = divergent_state_marker(
                        ref,
                        dep_entry.resolved_version,
                    )
                    associated_input = getattr(
                        dep_entry,
                        "associated_input",
                        None,
                    )
                    is_effect_read = (
                        isinstance(associated_input, tuple)
                        and associated_input
                        and associated_input[0] == "runtime_read"
                    )

                    if is_effect_read:
                        lines.append(
                            f"     {SHOW_CYAN}read:{SHOW_RESET} "
                            f"{SHOW_GREEN}{state}{SHOW_RESET} "
                            f"{SHOW_DIM}via READ effect{SHOW_RESET}"
                        )
                    else:
                        lines.append(
                            f"     {SHOW_DIM}used:{SHOW_RESET} "
                            f"{marker}{SHOW_GREEN}{state}{SHOW_RESET}"
                        )

            for reference in getattr(log, "mutated_during", {}):
                lines.append(
                    f"     {SHOW_YELLOW}mutated:{SHOW_RESET} "
                    f"{SHOW_WHITE}{reference}{SHOW_RESET}"
                )

            
            # 
            # handle stored values
            error = getattr(log, "error", None)
            if not_template(error) and error:
                if getattr(log, "handled_raise", False):
                    lines.append(
                        f"      {SHOW_YELLOW}RAISE handled: "
                        f"{error}{SHOW_RESET}"
                    )
                else:
                    lines.append(
                        f"      {SHOW_RED}ERROR: {error}{SHOW_RESET}"
                    )

            for write in shown_writes(log):
                stored_result = write.stored_result
                stored_version = write.stored_version
                if not_template(stored_result):
                    target_ref = write.stored_target
                    descriptor = self._recorded_state_value_descriptor(
                        target_ref,
                        stored_version,
                    )
                    result_name = (
                        descriptor.get("type")
                        if isinstance(descriptor, dict)
                        and isinstance(descriptor.get("type"), str)
                        else self._show_value_name(stored_result)
                    )
                    if result_name in {"TIDEUnknownType", "TIDEUnknownValue"}:
                        result_name = f"opaque<{result_name}>"
                    if values:
                        recorded_repr = (
                            descriptor.get("repr")
                            if isinstance(descriptor, dict)
                            and isinstance(descriptor.get("repr"), str)
                            else None
                        )
                        rendered_value = (
                            recorded_repr
                            if recorded_repr is not None
                            else repr(stored_result)
                        )
                        result_name = f"{result_name} {rendered_value}"
                    _lines_from_write = lines_from_write(
                        write.cause,
                        result_name,
                        target_ref,
                        stored_version,
                        write.updated,
                        write.observed_mutable,
                    )
                    # action = "wrote" if not (cause.startswith("TIDE") and "Polling" in cause) else "dep available"
                    
                    lines.extend(_lines_from_write)
            

            #
            # handle effects
            effects = tuple(
                effect
                for effect in log.effects
                if isinstance(log, ImmediateEffectRecord)
                or effect_occurrence_id(effect) not in immediate_effects
            )
            if not_template(effects) and effects:
                if effects: lines.append(f"  {SHOW_DIM}effects:{SHOW_RESET}")
                for effect_entry in effects:
                    effect = effect_entry.effect
                    target = effect_entry.targets

                    # State Effects have a reference and version; other Effects do not.
                    if effect in {Effects.READ, Effects.WRITE, Effects.DELETE}:
                        colour = effect_ref_colour(log, effect_entry)
                        if target[1] is None:
                            target_text = target[0]
                        else:
                            target_text = f"{target[0]}[v{target[1]}]"
                    elif effect == Effects.EMIT:
                        emitted = next((emitted for emitted in log.emitted_nodes if emitted.content_id == target), None)
                        if emitted is None:
                            colour = SHOW_YELLOW
                            target_text = repr(target)
                        elif emitted.emitted_by_exec_id != log.exec_id:
                            owner = self._execs.get(emitted.emitted_by_exec_id)
                            colour = SHOW_YELLOW
                            target_text = emitted.func_name
                            if owner is not None:
                                target_text += f" (registered by EXEC_RECORD[{owner.seq_id}])"
                        else:
                            node = (emitted.runner_id, emitted.node_id)
                            executed = any(
                                self._execs[candidate_id].emitted_by_exec_id == log.exec_id
                                for candidate_id in self._index.exec_ids_by_runner_node.get(node, ())
                            )
                            colour = SHOW_GREEN if executed else SHOW_CYAN
                            target_text = emitted.func_name
                    else:
                        colour = SHOW_RESET
                        target_text = repr(target)

                    lines.append(
                        f"        {SHOW_CYAN}{effect}{SHOW_RESET} "
                        f"{SHOW_DIM}->{SHOW_RESET} "
                        f"{colour}{target_text}{SHOW_RESET}"
                    )

            if isinstance(log, ImmediateEffectRecord):
                continue

            owned_emissions = []
            seen_emitted_nodes = set()
            for emitted in log.emitted_nodes:
                node = (emitted.runner_id, emitted.node_id)
                if emitted.emitted_by_exec_id != log.exec_id or node in seen_emitted_nodes:
                    continue
                seen_emitted_nodes.add(node)
                execution_count = sum(
                    self._execs[candidate_id].emitted_by_exec_id == log.exec_id
                    for candidate_id in self._index.exec_ids_by_runner_node.get(node, ())
                )
                owned_emissions.append((emitted.func_name, execution_count))

            if owned_emissions:
                lines.append(f"  {SHOW_DIM}emitted:{SHOW_RESET}")
                for actor_name, execution_count in owned_emissions:
                    execution_word = "execution" if execution_count == 1 else "executions"
                    lines.append(
                        f"        {SHOW_CYAN}{actor_name}{SHOW_RESET} "
                        f"{SHOW_DIM}- {execution_count} {execution_word}{SHOW_RESET}"
                    )

            
        if self.divergent:
            lines.append(
                f"{SHOW_RED}---- DIVERGENT ----{SHOW_RESET}"
            )
        elif self.scope == "complete":
            lines.append(
                f"{SHOW_GREEN}---- PIPELINE END ----{SHOW_RESET}"
            )
        else:
            lines.append(
                f"{SHOW_YELLOW}---- PIPELINE END NOT REPRESENTED: "
                f"AMBIGUOUS SCOPE ----{SHOW_RESET}"
            )

        return _show_render(lines, color)

    @staticmethod
    def _show_value_name(value):
        from tide_tooling.tooling_core.bundling.TIDEOpaqueValue import (
            TIDEOpaqueValue,
        )

        if isinstance(value, TIDEOpaqueValue):
            return value.type_name

        return type(value).__name__ if not isinstance(value, type) else value.__name__

            
    def _infer_kind(self, value):
        if isinstance(value, (Inspected, Object)):
            return value.type

        try:
            if value in self._execs:
                return "exec_record"
        except TypeError:
            pass

        if isinstance(value, Effects):
            return "effect"
        if (
            isinstance(value, tuple)
            and len(value) == 2
            and isinstance(value[0], Effects)
        ):
            return "effect"

        if isinstance(value, Actor):
            return "actor"
        if (
            isinstance(value, tuple)
            and len(value) == 3
            and all(isinstance(part, (int, str)) for part in value)
        ):
            return "actor"

        if self._is_val_hash(value):
            return "hash"

        if isinstance(value, str):
            if value in self._index.seen_refs:
                return "reference"
            if ":" in value:
                store = value.split(":", 1)[0]
                seen_stores = {
                    reference.split(":", 1)[0]
                    for reference in self._index.seen_refs
                }
                if store in DEFAULT_TIDE_STORES or store in seen_stores:
                    return "reference"
            return "value"

        if isinstance(value, int) and any(
            record.seq_id == value for record in self._records
        ):
            return "seq_id"

        return "value"

    def inspect(self, value, kind=None, version=None, grouped=True):
        if isinstance(value, Inspected):
            if value.trace is None:
                value.trace = self
            return value

        if isinstance(value, Object):
            kind = kind or value.type
            value = value.id

        kind = {
            "exec": "exec_record",
            "exec_id": "exec_record",
            "ref": "reference",
            "dep": "reference",
        }.get(kind, kind)

        if kind == "actor" and isinstance(value, str):
            actor_name, occurrence = self._split_actor_occurrence(value, version)
            if actor_name != value:
                exact = next((record for record in self._execution_records if repr(record.producer_occurrence_id) == value), None)
                if exact is not None:
                    return self.inspect_actor(*exact.producer_id.id, version=occurrence)

        if kind == "reference" and isinstance(value, str):
            reference, state_version = self._split_state_identity(value, version)
            if reference != value:
                exact = self.inspect_ref(reference, state_version)
                if exact.exists:
                    return exact
                nearby = self.find(reference, state_version).first("reference")
                if nearby is not None:
                    return self.inspect_ref(nearby.id, state_version)

        actor_value, actor_version = self._split_actor_occurrence(value, version)
        if kind == "actor" or (
            kind is None
            and actor_value in self._index.func_name_to_actors
            and actor_version is not None
        ):
            value, version = actor_value, actor_version
        value, version = self._split_state_identity(value, version)

        kind = kind or self._infer_kind(value)
        fallback = Inspected(
            Object(value, kind, value),
            trace=self,
        )

        if kind == "seq_id":
            record = next(
                (record for record in self._records if record.seq_id == value),
                None,
            )
            if isinstance(record, ExecRecord):
                return self.inspect_exec(record.exec_id)
            if isinstance(record, ImmediateEffectRecord):
                return self.inspect_immediate_effect(record)
            return fallback

        if kind == "immediate_effect_record":
            record = next(
                (
                    record
                    for record in self._records
                    if isinstance(record, ImmediateEffectRecord)
                    and (
                        record.seq_id == value
                        or record.event_id == value
                    )
                ),
                None,
            )
            return (
                self.inspect_immediate_effect(record)
                if record is not None
                else fallback
            )

        if kind == "exec_record":
            return self.inspect_exec(value)

        if kind in {"value", "hash"}:
            return self.inspect_value(value, kind)

        if kind == "reference":
            if isinstance(value, str) and ":" not in value:
                if grouped:
                    return self.inspect_as_group(value, "reference", version)

                match = self.find(value, version).first("reference")
                return (
                    self.inspect_ref(match.id, version)
                    if match is not None
                    else fallback
                )

            return self.inspect_ref(value, version)

        if kind == "actor":
            if isinstance(value, str):
                if grouped and version is None:
                    return self.inspect_as_group(value, "actor")

                match = self.find(value, version).first("actor")
                return (
                    self.inspect_actor(*match.id.id, version=version)
                    if match is not None
                    else fallback
                )

            actor = value if isinstance(value, Actor) else Actor(*value)
            return self.inspect_actor(*actor.id, version=version)

        if kind == "effect":
            return self.inspect_effect(value, version)

        raise InvalidObjectKindError(
            f"{value} | type={kind} is not known to TIDE",
            kind,
        )
    def _get_pipeline_named_tuple_from_id(self, pipeline_id, runner_id=None):
        return pipeline_id, self._pipeline_names.get(pipeline_id, pipeline_id)

    def _dep_records(self, record: DependencyUseRecord):
        return record.expanded_to if record.type == "dynamic" else [record]

    def inspect_exec(self, exec_id):
        retained = self._execs.get(exec_id)
        execution = retained
        if execution is None:
            return Inspected(Object(exec_id, "exec_record", exec_id), trace=self)

        # Describe the recorded execution and its source.
        obj = Object(execution, "exec_record", exec_id, exists=True)
        obj.source_ocean = [execution.source_ocean_id]
        obj.source_runner = [execution.runner_id]
        pipeline_name = self._pipeline_names.get(execution.pipeline_id)
        obj.source_pipeline = (
            [(execution.pipeline_id, pipeline_name)]
            if pipeline_name is not None
            else []
        )

        obj.reads = execution.deps
        obj.writes = execution.writes
        obj.effects = execution.effects

        # Derive the executions which led into this one.
        upstream = deduped_dict()
        for edge in self._index.producer_edges_by_exec_id[exec_id]:
            producer = self._execs.get(edge.producer_exec_id)
            if producer is not None:
                upstream[producer.producer_id].add(producer.exec_id)

        # Derive the executions which followed from this one.
        downstream = deduped_dict()
        for edge in self._index.consumed_edges_by_exec_id[exec_id]:
            consumer = self._execs.get(edge.consumer_exec_id)
            if consumer is not None:
                downstream[consumer.producer_id].add(consumer.exec_id)

        return Inspected(
            obj,
            upstream=upstream,
            downstream=downstream,
            trace=self,
        )

    def _ancestry_source(self, value, kind=None):
        inspected = self.inspect(value, kind=kind, grouped=False)
        if inspected.type != "exec_record" or not inspected.exists:
            return inspected, None
        return inspected, inspected.object.object

    def parents_of(self, value, kind=None):
        inspected, execution = self._ancestry_source(value, kind)
        parents = ()
        if execution is not None:
            parent_exec_id = self._index.parent_exec_id_by_exec_id.get(
                execution.exec_id
            )
            parent = self._execs.get(parent_exec_id)
            if parent is not None:
                parents = (parent,)
        return ExecutionAncestry(self, execution, parents, "parents")

    def children_of(self, value, kind=None):
        inspected, execution = self._ancestry_source(value, kind)
        children = ()
        if execution is not None:
            children = tuple(
                sorted(
                    (
                        self._execs[exec_id]
                        for exec_id in self._index.child_exec_ids_by_exec_id.get(
                            execution.exec_id,
                            (),
                        )
                        if exec_id in self._execs
                    ),
                    key=lambda child: child.seq_id,
                )
            )
        return ExecutionAncestry(self, execution, children, "children")

    def inspect_immediate_effect(self, record):
        obj = Object(
            record,
            "immediate_effect_record",
            record.seq_id,
            exists=True,
        )
        actor = record.producer_id
        if actor is not None:
            obj.source_ocean = [actor.source_ocean]
            obj.source_runner = [actor.source_runner]
        obj.reads = record.deps
        obj.writes = record.writes
        obj.effects = record.effects
        return Inspected(obj, trace=self)

    def inspect_value(self, value_or_hash, kind):
        value_hash = value_or_hash if kind == "hash" else tide_value_hasher(value_or_hash)
        captured = self._index.value_by_hash.get(value_hash, value_or_hash)
        obj = Object(detached_tide_value(captured), "value", value_hash)
        history = list(self._index.ref_versions_by_value_hash.get(value_hash, ()))

        if not history:
            return Inspected(obj, trace=self)

        obj.exists = True
        upstream = deduped_dict()
        downstream = deduped_dict()
        source_oceans = DedupedList()
        source_runners = DedupedList()
        source_pipelines = DedupedList()

        # Collect every State occurrence carrying this value.
        for ref_version in history:
            obj.reads.extend(tuple(
                self._index.read_records_by_ref_version[ref_version]
            ))

            obj.writes.extend(tuple(
                self._index.write_records_by_ref_version.get(ref_version, ())
            ))

            # Derive producing execution evidence.
            for producer_exec_id in self._index.producer_exec_ids_by_ref_version.get(
                ref_version,
                (),
            ):
                producer = self._execs.get(producer_exec_id)
                if producer is not None:
                    upstream[producer.producer_id].add(producer.exec_id)
                    source_oceans.add(producer.source_ocean_id)
                    source_runners.add(producer.runner_id)

                    pipeline_name = self._pipeline_names.get(producer.pipeline_id)
                    if pipeline_name is not None:
                        source_pipelines.add((producer.pipeline_id, pipeline_name))

            # Derive consuming execution evidence.
            for consumer_exec_id in self._index.consumer_exec_ids_by_ref_version[ref_version]:
                consumer = self._execs.get(consumer_exec_id)
                if consumer is not None:
                    downstream[consumer.producer_id].add(consumer.exec_id)

        obj.source_ocean = source_oceans
        obj.source_runner = source_runners
        obj.source_pipeline = source_pipelines
        return Inspected(obj, history, upstream, downstream, trace=self)

    def inspect_ref(self, reference, version=None):
        if isinstance(reference, DataRef):
            scoped_reference = DataRef.to_string(reference, prefix=False)
            reference = (
                reference.key
                if reference.key in self._index.seen_refs
                else scoped_reference
            )

        obj = Object(reference, "reference", reference)
        history = list(self._index.versions_by_ref.get(reference, ()))

        if not history or (version is not None and version not in history):
            return Inspected(obj, trace=self)

        obj.exists = True
        obj.version = version
        selected_versions = history if version is None else [version]

        # Collect the exact read and write evidence.
        for selected_version in selected_versions:
            ref_version = (reference, selected_version)
            obj.reads.extend(tuple(
                self._index.read_records_by_ref_version[ref_version]
            ))

            obj.writes.extend(tuple(
                self._index.write_records_by_ref_version.get(ref_version, ())
            ))

        # Derive producing and consuming executions.
        upstream = deduped_dict()
        downstream = deduped_dict()
        source_oceans = DedupedList()
        source_runners = DedupedList()
        source_pipelines = DedupedList()

        for selected_version in selected_versions:
            ref_version = (reference, selected_version)
            for producer_exec_id in self._index.producer_exec_ids_by_ref_version.get(
                ref_version,
                (),
            ):
                producer = self._execs.get(producer_exec_id)

                if producer is not None:
                    upstream[producer.producer_id].add(producer.exec_id)
                    source_oceans.add(producer.source_ocean_id)
                    source_runners.add(producer.runner_id)

                    pipeline_name = self._pipeline_names.get(producer.pipeline_id)
                    if pipeline_name is not None:
                        source_pipelines.add((producer.pipeline_id, pipeline_name))

            for consumer_exec_id in self._index.consumer_exec_ids_by_ref_version[ref_version]:
                consumer = self._execs.get(consumer_exec_id)
                if consumer is not None:
                    downstream[consumer.producer_id].add(consumer.exec_id)

        obj.source_ocean = source_oceans
        obj.source_runner = source_runners
        obj.source_pipeline = source_pipelines
        return Inspected(obj, history, upstream, downstream, trace=self)

    def inspect_as_group(self, value, kind, version=None):
        if kind not in {"reference", "actor"}:
            return Inspected(Object(value, kind, value))

        searched = self.find(value, version)
        matches = DedupedList(searched.get_exact(kind) + searched.get_nearby(kind))

        objects = []
        history = DedupedList()
        actor_exec_ids = DedupedList()
        reads = DedupedList()
        writes = DedupedList()
        upstream = deduped_dict()
        downstream = deduped_dict()
        source_oceans = DedupedList()
        source_runners = DedupedList()
        source_pipelines = DedupedList()

        # Combine each matching exact inspection.
        for inspected in matches:
            objects.append(inspected.object)
            reads.extend(inspected.reads)
            writes.extend(inspected.writes)
            source_oceans.extend(inspected.source_oceans or ())
            source_runners.extend(inspected.source_runners or ())
            source_pipelines.extend(inspected.source_pipelines or ())

            if kind == "reference":
                history.extend((inspected.id, selected_version) for selected_version in inspected.history)
            else:
                actor_exec_ids.extend(inspected.history)

            for source, exec_ids in inspected.upstream.items():
                upstream[source].extend(exec_ids)

            for source, exec_ids in inspected.downstream.items():
                downstream[source].extend(exec_ids)

        # Actor history follows recorded execution order.
        if kind == "actor":
            history.extend(sorted(actor_exec_ids, key=lambda exec_id: self._execs[exec_id].seq_id))

        return InspectedGroup(
            objects,
            kind,
            value,
            history,
            upstream,
            downstream,
            source_oceans,
            source_runners,
            source_pipelines,
            version=version,
            reads=reads,
            writes=writes,
            trace=self,
        )

    def inspect_actor(self, ocean_id, runner_id, actor_name, version=None):
        actor = Actor(ocean_id, runner_id, actor_name)
        obj = Object(actor, "actor", actor)
        if version is None:
            exec_ids = list(self._index.actor_to_exec_ids.get(actor, ()))
        else:
            exec_id = self._index.actor_occurrence_to_exec_id.get((actor, version))
            if exec_id is None: exec_ids = None
            else: exec_ids = (exec_id,)

                    

        if not exec_ids:
            return Inspected(obj, trace=self)

        obj.exists = True
        obj.source_ocean = [ocean_id]
        obj.source_runner = [runner_id]
        obj.source_pipeline = DedupedList()
        obj.version = version
        upstream = deduped_dict()
        downstream = deduped_dict()

        # Combine evidence from every execution of this actor.
        for exec_id in exec_ids:
            execution = self._execs[exec_id]
            inspected = self.inspect_exec(exec_id)
            detached_execution = inspected.object.object

            obj.reads.extend(detached_execution.deps)
            obj.writes.extend(detached_execution.writes)
            obj.effects.extend(detached_execution.effects)
            obj.source_pipeline.extend(inspected.source_pipelines)

            for source, related_exec_ids in inspected.upstream.items():
                upstream[source].extend(related_exec_ids)

            for source, related_exec_ids in inspected.downstream.items():
                downstream[source].extend(related_exec_ids)

            # Infer actual executions of nodes this execution registered.
            for emitted_node in execution.emitted_nodes:
                if emitted_node.emitted_by_exec_id != exec_id:
                    continue

                node_key = (emitted_node.runner_id, emitted_node.node_id)
                for candidate_id in self._index.exec_ids_by_runner_node[node_key]:
                    candidate = self._execs[candidate_id]
                    downstream[candidate.producer_id].add(candidate_id)

            # Infer the execution which registered this execution's node.
            emitter = self._execs.get(execution.emitted_by_exec_id)
            if emitter is not None:
                upstream[emitter.producer_id].add(emitter.exec_id)

        return Inspected(obj, exec_ids, upstream, downstream, trace=self)

    def inspect_effect(self, identifier, version=None):
        # Parse the Effect query.
        if isinstance(identifier, Effects):
            effect, target = identifier, None
        elif isinstance(identifier, tuple) and len(identifier) == 2:
            effect, target = identifier
        else:
            raise TypeError("Effect inspection requires Effects.KIND or (Effects.KIND, target)")

        if not isinstance(effect, Effects):
            raise TypeError("Effect inspection requires an Effects value")

        state_effect = effect in {Effects.READ, Effects.WRITE, Effects.DELETE}
        embedded_version = None
        if state_effect and isinstance(target, tuple) and len(target) == 2:
            target, embedded_version = target

        selected_version = version if version is not None else embedded_version

        # Resolve matching target-level occurrences.
        if target is None:
            effect_id = effect
            occurrences = self._index.effect_occurrences_by_type.get(effect, ())
        elif selected_version is None:
            effect_id = (effect, target)
            occurrences = self._index.effect_occurrences_by_target.get((effect, target), ())
        else:
            effect_id = (effect, (target, selected_version))
            occurrences = self._index.effect_occurrences_by_ref_version.get((effect, target, selected_version), ())

        obj = Object(effect_id, "effect", effect_id, exists=bool(occurrences))
        obj.version = selected_version
        producer_exec_ids = DedupedList()
        upstream = deduped_dict()
        downstream = deduped_dict()
        source_oceans = DedupedList()
        source_runners = DedupedList()
        source_pipelines = DedupedList()

        # Collect each exact Effect occurrence.
        for occurrence in occurrences:
            producer_exec_id, record = self._index.effect_by_occurrence[occurrence]
            obj.effects.append(record)
            producer_exec_ids.add(producer_exec_id)

            producer = self._execs.get(producer_exec_id)
            if producer is not None:
                upstream[producer.producer_id].add(producer_exec_id)
                source_oceans.add(producer.source_ocean_id)
                source_runners.add(producer.runner_id)

                pipeline_name = self._pipeline_names.get(producer.pipeline_id)
                if pipeline_name is not None:
                    source_pipelines.add((producer.pipeline_id, pipeline_name))

            for affected in self._index.affected_records_by_effect_occurrence[occurrence]:
                affected_execution = self._execs.get(affected.affected_exec_id)
                if affected_execution is not None:
                    downstream[affected_execution.producer_id].add(affected_execution.exec_id)
                elif affected.affected_inputs:
                    downstream[affected.affectee].add(affected.affected_inputs)

        obj.source_ocean = source_oceans
        obj.source_runner = source_runners
        obj.source_pipeline = source_pipelines
        return Inspected(obj, producer_exec_ids, upstream, downstream, trace=self)

    @staticmethod
    def _state_history(source):
        if source.type == "value" or source.grouped:
            return list(source.history)

        versions = source.history if source.version is None else [source.version]
        return [(source.id, version) for version in versions]

    def _normalise_query_source(self, source, kind, version, grouped):
        if isinstance(source, Investigate):
            source = source.inspected

        if isinstance(source, Inspected):
            return source

        kind = kind or self._infer_kind(source)
        if grouped:
            return self.inspect(source, kind, version, grouped=True)

        return self.find(source, version).first(kind)

    @staticmethod
    def _execution_history(source):
        if source.type == "exec_record":
            return [source.id] if source.exists else []
        if source.type == "actor":
            return list(source.history)

        raise TypeError(
            "relationship queries require a State, Actor, or ExecRecord"
        )

    def produced_by(
        self,
        source,
        kind=None,
        version=None,
        group=True,
    ):
        source = self._normalise_query_source(
            source,
            kind,
            version,
            group,
        )
        if source is None:
            return None

        by_exec = deduped_dict()

        # State -> executions which produced those exact State occurrences.
        if source.type in {"reference", "value"}:
            for state in self._state_history(source):
                for exec_id in self._index.producer_exec_ids_by_ref_version[
                    state
                ]:
                    by_exec[exec_id].append(state)
            execs = list(by_exec)

        # Actor/ExecRecord -> State occurrences written by those executions.
        else:
            execs = self._execution_history(source)
            for exec_id in execs:
                for write in self._execs[exec_id].writes:
                    by_exec[exec_id].append((
                        write.stored_target,
                        write.stored_version,
                    ))

        refs = DedupedList()
        refs.extend(
            state
            for states in by_exec.values()
            for state in states
        )
        return ProducedBy(self.inspect, source, execs, refs, by_exec)

    def consumed_by(
        self,
        source,
        kind=None,
        version=None,
        group=True,
    ):
        source = self._normalise_query_source(
            source,
            kind,
            version,
            group,
        )
        if source is None:
            return None

        by_exec = deduped_dict()

        # State -> executions which consumed those exact State occurrences.
        if source.type in {"reference", "value"}:
            execs = DedupedList()
            for state in self._state_history(source):
                for exec_id in self._index.consumer_exec_ids_by_ref_version.get(
                    state,
                    (),
                ):
                    execs.add(exec_id)
                    by_exec[exec_id].append(state)

        # Actor/ExecRecord -> State occurrences read by those executions.
        else:
            execs = self._execution_history(source)
            for exec_id in execs:
                for dependency in self._execs[exec_id].deps:
                    for read in self._index._concretised_from(dependency):
                        by_exec[exec_id].append((
                            read.unscoped_reference,
                            read.resolved_version,
                        ))

        refs = DedupedList()
        refs.extend(
            state
            for states in by_exec.values()
            for state in states
        )
        return ConsumedBy(self.inspect, source, execs, refs, by_exec)

    def upstream(
        self,
        source,
        type=None,
        grouped=True,
        depth=None,
        max_paths=DEFAULT_STREAM_MAX_PATHS,
    ):
        return self._direction_stream(
            source,
            type,
            grouped,
            depth,
            max_paths,
            "upstream",
        )


    def downstream(
        self,
        source,
        type=None,
        grouped=True,
        depth=None,
        max_paths=DEFAULT_STREAM_MAX_PATHS,
    ):
        return self._direction_stream(
            source,
            type,
            grouped,
            depth,
            max_paths,
            "downstream",
        )


    def _starting_exec_ids(self, source):
        # Actor and ExecRecord histories already contain execution IDs.
        if source.type in {"actor", "exec_record"}:
            return self._execution_history(source)

        # State histories begin at the execution which produced each State.
        if source.type in {"reference", "value"}:
            exec_ids = DedupedList()
            for state in self._state_history(source):
                exec_ids.extend(
                    self._index.producer_exec_ids_by_ref_version[state]
                )
            return exec_ids

        # Effect histories contain the executions which produced those Effects.
        if source.type == "effect":
            return list(source.history)

        raise TypeError(
            "streams require a State, Actor, ExecRecord, or Effect"
        )


    def _direction_stream(
        self,
        source,
        kind,
        grouped,
        depth,
        max_paths,
        direction,
    ):
        # Accept only upstream/downstream and their supported limits.
        if direction not in {"upstream", "downstream"}:
            raise ValueError("direction must be 'upstream' or 'downstream'")
        if depth is not None and (
            isinstance(depth, bool)
            or not isinstance(depth, int)
            or depth < 0
        ):
            raise ValueError("depth must be a non-negative int or None")
        if max_paths is not None and (
            isinstance(max_paths, bool)
            or not isinstance(max_paths, int)
            or max_paths < 1
        ):
            raise ValueError("max_paths must be a positive int or None")

        # Resolve the endpoint whose execution relationships will be followed.
        if isinstance(source, Investigate):
            source = source.inspected
        if not isinstance(source, Inspected):
            source = self.inspect(source, kind, grouped=grouped)

        starting_exec_ids = self._starting_exec_ids(source)
        source_states = (
            set(self._state_history(source))
            if direction == "downstream"
            and source.type in {"reference", "value"}
            else None
        )
        source_effects = (
            tuple(source.object.effects)
            if direction == "downstream" and source.type == "effect"
            else None
        )

        paths = defaultdict(list)
        path_count = 0
        truncated = False

        # Group parallel handoffs by the one next execution they lead to.
        def grouped_handoffs(edges):
            by_exec_id = {}
            for edge in edges:
                next_exec_id = (
                    edge.producer_exec_id
                    if direction == "upstream"
                    else edge.consumer_exec_id
                )
                if next_exec_id is None:
                    continue
                by_exec_id.setdefault(next_exec_id, []).append(edge)
            return by_exec_id.items()

        # Walk every execution path without revisiting an execution in that path.
        for starting_exec_id in starting_exec_ids:
            starting_edges = [
                edge
                for _, handoffs in self._path_handoffs(
                    starting_exec_id,
                    direction,
                )
                for edge in handoffs
            ]

            # A downstream State or Effect starts only through its exact evidence.
            if source_states is not None:
                starting_edges = [
                    edge
                    for edge in starting_edges
                    if (
                        isinstance(edge, ReadEdge)
                        and (
                            edge.read.unscoped_reference,
                            edge.read.resolved_version,
                        )
                        in source_states
                    ) or (
                        isinstance(edge, EffectEdge)
                        and edge.effect_record.targets in source_states
                    )
                ]
            if source_effects is not None:
                starting_edges = [
                    edge
                    for edge in starting_edges
                    if isinstance(edge, EffectEdge)
                    and any(
                        _same_effect_record(edge.effect_record, effect)
                        for effect in source_effects
                    )
                ]

            for following_exec_id, handoffs in grouped_handoffs(starting_edges):
                stack = [(
                    following_exec_id,
                    [starting_exec_id, following_exec_id],
                    [handoffs],
                    depth,
                    {starting_exec_id, following_exec_id},
                )]

                while stack and not truncated:
                    (
                        current_exec_id,
                        executions,
                        path_handoffs,
                        remaining,
                        visited,
                    ) = stack.pop()

                    following = []
                    if remaining != 0:
                        for candidate_exec_id, candidate_handoffs in self._path_handoffs(
                            current_exec_id,
                            direction,
                        ):
                            if candidate_exec_id is None:
                                continue
                            if candidate_exec_id not in visited:
                                following.append((
                                    candidate_exec_id,
                                    candidate_handoffs,
                                ))

                    # A path is complete at its requested depth or final execution.
                    if not following:
                        if max_paths is not None and path_count >= max_paths:
                            truncated = True
                            break
                        paths[starting_exec_id].append({
                            "executions": executions,
                            "handoffs": path_handoffs,
                        })
                        path_count += 1
                        continue

                    next_remaining = (
                        None if remaining is None else remaining - 1
                    )
                    for candidate_exec_id, candidate_handoffs in reversed(following):
                        stack.append((
                            candidate_exec_id,
                            [*executions, candidate_exec_id],
                            [*path_handoffs, candidate_handoffs],
                            next_remaining,
                            visited | {candidate_exec_id},
                        ))

            if truncated:
                break

        stream = defaultdict(lambda: defaultdict(list))

        # Derive the shallow layer view from the complete paths.
        for starting_exec_id, complete_paths in paths.items():
            projected = {}
            order = 0
            for path in complete_paths:
                for index, handoffs in enumerate(path["handoffs"]):
                    layer = "root" if index == 0 else index - 1
                    rank = -1 if layer == "root" else layer
                    for handoff in handoffs:
                        if handoff not in projected:
                            projected[handoff] = (rank, order, layer)
                            order += 1
                        elif rank < projected[handoff][0]:
                            _, first_order, _ = projected[handoff]
                            projected[handoff] = (rank, first_order, layer)

            for handoff, (_, _, layer) in sorted(
                projected.items(),
                key=lambda item: item[1][:2],
            ):
                stream[starting_exec_id][layer].append(handoff)

        # Return the existing public result for the requested direction.
        result = Upstream if direction == "upstream" else Downstream
        return result(
            source,
            stream,
            paths,
            inspect=self.inspect,
            truncated=truncated,
            max_paths=max_paths,
        )

    @staticmethod
    def _effect_history(source):
        # Return the exact Effect records represented by this endpoint.
        objects = source.object if source.grouped else (source.object,)
        return [
            effect
            for item in objects
            for effect in item.effects
        ]


    def _path_handoffs(self, exec_id, direction):
        # Begin with the same recorded handoffs as directional streams.
        edges_by_exec = (
            self._index.producer_edges_by_exec_id
            if direction == "upstream"
            else self._index.consumed_edges_by_exec_id
        )
        handoffs = list(edges_by_exec.get(exec_id, ()))

        execution = self._index.exec_by_exec_id[exec_id]

        # Follow an executed emitted node back to its registering execution.
        if direction == "upstream":
            emitter_id = execution.emitted_by_exec_id
            emitter = self._index.exec_by_exec_id.get(emitter_id)
            if emitter is not None:
                handoffs.extend(
                    EmissionEdge(emitter, emitted, exec_id)
                    for emitted in emitter.emitted_nodes
                    if emitted.emitted_by_exec_id == emitter_id
                    and emitted.runner_id == execution.runner_id
                    and emitted.node_id == execution.node_id
                )

        # Follow each registered emitted node to its execution occurrences.
        else:
            for emitted in execution.emitted_nodes:
                node = (emitted.runner_id, emitted.node_id)
                emitted_exec_ids = (
                    [
                        candidate_id
                        for candidate_id in self._index.exec_ids_by_runner_node.get(
                            node,
                            (),
                        )
                        if self._index.exec_by_exec_id[
                            candidate_id
                        ].emitted_by_exec_id == exec_id
                    ]
                    if emitted.emitted_by_exec_id == exec_id
                    else []
                )
                if emitted_exec_ids:
                    handoffs.extend(
                        EmissionEdge(execution, emitted, emitted_exec_id)
                        for emitted_exec_id in emitted_exec_ids
                    )
                else:
                    handoffs.append(EmissionEdge(execution, emitted))

        # Keep every handoff between the same adjacent executions in one step.
        by_exec_id = {}
        terminal_handoffs = []
        for handoff in handoffs:
            next_exec_id = (
                handoff.producer_exec_id
                if direction == "upstream"
                else handoff.consumer_exec_id
            )
            if next_exec_id is None:
                terminal_handoffs.append((None, [handoff]))
            else:
                by_exec_id.setdefault(next_exec_id, []).append(handoff)

        return [*by_exec_id.items(), *terminal_handoffs]

    def _direct_path_evidence(
        self,
        root_exec_id,
        source,
        target,
        target_exec_ids,
        direction,
    ):
        execution = self._index.exec_by_exec_id[root_exec_id]

        # X -> X is proved only by that execution's evidence in this direction.
        if (
            source.type in {"actor", "exec_record"}
            and target.type in {"actor", "exec_record"}
            and root_exec_id in target_exec_ids
        ):
            if direction == "downstream":
                return list(execution.writes)
            return [
                read
                for dependency in execution.deps
                for read in self._index._concretised_from(dependency)
            ]

        # Execution/Actor -> exact State or Effect produced by that execution.
        if (
            direction == "downstream"
            and source.type in {"actor", "exec_record"}
        ):
            if target.type in {"reference", "value"}:
                target_states = set(self._state_history(target))
                return [
                    write
                    for write in execution.writes
                    if (
                        write.stored_target,
                        write.stored_version,
                    ) in target_states
                ]

            if target.type == "effect":
                target_effects = self._effect_history(target)
                return [
                    effect
                    for effect in execution.effects
                    if any(
                        _same_effect_record(effect, target_effect)
                        for target_effect in target_effects
                    )
                ]

            return []

        # Exact State or Effect -> its producing Execution/Actor.
        if (
            direction != "upstream"
            or target.type not in {"actor", "exec_record"}
            or root_exec_id not in target_exec_ids
        ):
            return []

        if source.type in {"reference", "value"}:
            source_states = set(self._state_history(source))
            return [
                write
                for write in execution.writes
                if (
                    write.stored_target,
                    write.stored_version,
                ) in source_states
            ]

        if source.type == "effect":
            source_effects = self._effect_history(source)
            return [
                effect
                for effect in execution.effects
                if any(
                    _same_effect_record(effect, source_effect)
                    for source_effect in source_effects
                )
            ]

        return []

    def _path_ending(
        self,
        handoffs,
        next_exec_id,
        target,
        target_exec_ids,
        direction,
        deep_link,
    ):
        # Actor and ExecRecord endpoints end at their execution occurrence.
        if target.type in {"actor", "exec_record"}:
            if next_exec_id in target_exec_ids:
                return []

            # An emitted Actor can exist before its node has executed.
            if target.type == "actor":
                objects = (
                    target.object
                    if target.grouped
                    else (target.object,)
                )
                target_actors = {
                    item.object
                    for item in objects
                }
                evidence = [
                    handoff
                    for handoff in handoffs
                    if isinstance(handoff, EmissionEdge)
                    and handoff.emitted_node.producer_id in target_actors
                ]
                if evidence:
                    return evidence

            return None

        # A State carried by this edge is already exact endpoint evidence.
        if target.type in {"reference", "value"}:
            target_states = set(self._state_history(target))
            for handoff in handoffs:
                handoff_state = None
                if isinstance(handoff, ReadEdge):
                    handoff_state = (
                        handoff.read.unscoped_reference,
                        handoff.read.resolved_version,
                    )
                elif isinstance(handoff, EffectEdge):
                    handoff_state = handoff.effect_record.targets
                if handoff_state in target_states:
                    return []

            # A deeper downstream path may end at a write by the next execution.
            if (
                direction == "downstream"
                and deep_link
                and next_exec_id is not None
            ):
                evidence = [
                    write
                    for write in self._index.exec_by_exec_id[
                        next_exec_id
                    ].writes
                    if (
                        write.stored_target,
                        write.stored_version,
                    ) in target_states
                ]
                return evidence or None

            return None

        # An Effect is exact on its edge or as output of the next execution.
        target_effects = self._effect_history(target)
        if any(
            isinstance(handoff, EffectEdge)
            and any(
                _same_effect_record(handoff.effect_record, target_effect)
                for target_effect in target_effects
            )
            for handoff in handoffs
        ):
            return []

        if (
            direction == "downstream"
            and deep_link
            and next_exec_id is not None
        ):
            evidence = [
                effect
                for effect in self._index.exec_by_exec_id[
                    next_exec_id
                ].effects
                if any(
                    _same_effect_record(effect, target_effect)
                    for target_effect in target_effects
                )
            ]
            return evidence or None

        return None

    def _normalise_path_endpoint(
        self,
        endpoint,
        kind,
        version,
        grouped,
    ):
        # Preserve a missing loose query as an inspectable empty path endpoint.
        raw_endpoint = endpoint
        if isinstance(endpoint, Investigate):
            endpoint_kind = endpoint.inspected.type
        elif isinstance(endpoint, Inspected):
            endpoint_kind = endpoint.type
        else:
            endpoint_kind = self._infer_kind(endpoint)
        kind = kind or endpoint_kind
        endpoint = self._normalise_query_source(
            endpoint,
            kind,
            version,
            grouped,
        )
        if endpoint is not None:
            return endpoint
        return self.inspect(
            raw_endpoint,
            kind,
            version,
            grouped=False,
        )


    def path(
        self,
        source,
        target,
        source_type=None,
        target_type=None,
        source_ver=None,
        target_ver=None,
        group=True,
        deep_link=True,
        direction="downstream",
    ):
        # Accept only a supported direction.
        if direction not in {"upstream", "downstream"}:
            raise ValueError("direction must be 'upstream' or 'downstream'")

        # Resolve both endpoints once through the shared query semantics.
        source = self._normalise_path_endpoint(
            source,
            source_type,
            source_ver,
            group,
        )
        target = self._normalise_path_endpoint(
            target,
            target_type,
            target_ver,
            group,
        )

        source_exec_ids = self._starting_exec_ids(source)
        target_exec_ids = self._starting_exec_ids(target)
        target_exec_id_set = set(target_exec_ids)
        source_states = (
            set(self._state_history(source))
            if source.type in {"reference", "value"}
            else None
        )
        source_effects = (
            self._effect_history(source)
            if source.type == "effect"
            else None
        )
        paths = defaultdict(list)

        for root_exec_id in source_exec_ids:
            # Record direct incidence without manufacturing a handoff.
            direct_evidence = self._direct_path_evidence(
                root_exec_id,
                source,
                target,
                target_exec_id_set,
                direction,
            )
            if direct_evidence:
                paths[root_exec_id].append({
                    "executions": [root_exec_id],
                    "handoffs": [],
                    "endpoint_evidence": direct_evidence,
                })

            stack = [(
                root_exec_id,
                [root_exec_id],
                [],
                {root_exec_id},
                True,
            )]
            while stack:
                (
                    current_exec_id,
                    executions,
                    path_handoffs,
                    visited,
                    is_root,
                ) = stack.pop()

                for next_exec_id, handoffs in reversed(
                    self._path_handoffs(current_exec_id, direction)
                ):
                    # A downstream State or Effect starts at its exact evidence.
                    if is_root and direction == "downstream":
                        if source_states is not None:
                            handoffs = [
                                handoff
                                for handoff in handoffs
                                if (
                                    (
                                        handoff.read.unscoped_reference,
                                        handoff.read.resolved_version,
                                    )
                                    if isinstance(handoff, ReadEdge)
                                    else handoff.effect_record.targets
                                    if isinstance(handoff, EffectEdge)
                                    else None
                                )
                                in source_states
                            ]
                        if source_effects is not None:
                            handoffs = [
                                handoff
                                for handoff in handoffs
                                if isinstance(handoff, EffectEdge)
                                and any(
                                    _same_effect_record(
                                        handoff.effect_record,
                                        source_effect,
                                    )
                                    for source_effect in source_effects
                                )
                            ]
                        if not handoffs:
                            continue

                    next_executions = (
                        executions
                        if next_exec_id is None
                        else [*executions, next_exec_id]
                    )
                    next_handoffs = (
                        path_handoffs
                        if next_exec_id is None
                        else [*path_handoffs, handoffs]
                    )
                    endpoint_evidence = self._path_ending(
                        handoffs,
                        next_exec_id,
                        target,
                        target_exec_id_set,
                        direction,
                        deep_link,
                    )

                    # Retain the complete path when this step reaches the target.
                    if endpoint_evidence is not None:
                        paths[root_exec_id].append({
                            "executions": next_executions,
                            "handoffs": next_handoffs,
                            "endpoint_evidence": endpoint_evidence,
                        })

                    # Continue only when this step did not reach the target.
                    if (
                        endpoint_evidence is None
                        and deep_link
                        and next_exec_id is not None
                        and next_exec_id not in visited
                    ):
                        stack.append((
                            next_exec_id,
                            next_executions,
                            next_handoffs,
                            visited | {next_exec_id},
                            False,
                        ))

        return Pathed(
            source,
            target,
            paths,
            direction,
            inspect=self.inspect,
        )

    def investigate(
        self,
        inspected: Any,
        kind: str | None = None,
        version: int=None,
        grouped: bool = True,
    ) -> Investigate:
        if not isinstance(inspected, Inspected):
            inspected = self.inspect(
                inspected,
                kind=kind,
                version=version,
                grouped=grouped,
            )

        return Investigate(
            trace=self,
            inspect=self.inspect,
            inspected=inspected,
        )

    def why(
        self,
        inspected: Any,
        kind: str | None = None,
        version: int | None = None,
        grouped: bool = True,
        **show_options,
    ):
        return self.investigate(
            inspected,
            kind=kind,
            version=version,
            grouped=grouped,
        ).show(**show_options)


    # def to_dict(self):
    #     return {
    #         "type": self.type, # what is this object
    #         "id": self.id, # how can I find this object
    #         "ocean": self.source_ocean, # what ocean instance was I observed in...? ish
    #         "source": self.source_runner, # what source did I come from,
    #         "pipeline": self.source_pipeline, # what graph was I a part of 
    #         "version": self.version # am i versioned? what version am i  
    #     }
