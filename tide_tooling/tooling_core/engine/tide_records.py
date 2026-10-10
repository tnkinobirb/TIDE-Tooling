"""Execution evidence records exposed by TIDE traces.

These classes contain captured evidence and identity. Trace reconstruction,
indexing, queries, and display orchestration remain in ``tide_logging``.
``tide_logging`` re-exports these names for compatibility with older imports.
"""

from __future__ import annotations
 
from typing import Any, NamedTuple

from tide_tooling.tooling_core.engine.classes import DataRef, __fallback__
from tide_tooling.tooling_core.engine.events import Cause, Effects
from tide_tooling.tooling_core.engine.tide_rendering import _show_fields
from tide_tooling.tooling_core.engine.utils import safe_equal
from tide_tooling.tooling_core.engine.utils import tide_value_hasher
from tide_tooling.tooling_core.engine.tide_evidence import SealableEvidence


__TEMPLATE__ = __fallback__()


def _diff(subject, other):
    # Imported only when a user asks for a diff, after tide_logging has
    # completed importing and re-exported these record classes.
    from tide_tooling.tooling_core.engine.tide_query_results import _build_compared

    return _build_compared(subject, other)


class Actor(SealableEvidence):
    def __init__(self, source_ocean, source_runner, source_actor):
        self.id = (source_ocean, source_runner, source_actor)
        self.source_ocean = source_ocean
        self.source_runner = (
            source_runner
            if not self._is_template(source_runner)
            else "unknown"
        )
        self.source_actor = (
            source_actor
            if not self._is_template(source_actor)
            else "unknown"
        )

    @staticmethod
    def _is_template(value):
        return isinstance(value, __fallback__)

    def __hash__(self):
        return hash((
            self.source_ocean,
            self.source_runner,
            self.source_actor,
        ))

    def __repr__(self):
        return f"Actor{self.id}"

    def __eq__(self, other):
        return isinstance(other, Actor) and self.id == other.id

    def to_dict(self):
        return {
            "id": (
                f"({self.source_ocean}, {self.source_runner}, "
                f"{self.source_actor})"
            ),
            "ocean": self.source_ocean,
            "source": self.source_runner,
            "name": self.source_actor,
        }

    def diff(self, other):
        return _diff(self, other)

    def show(self, color=True, simple=False, colour=None):
        return _show_fields(
            "ACTOR",
            repr(self),
            self.to_dict(),
            color=color,
            simple=simple,
            colour=colour,
        )

    def to_string(self) -> str:
        return str(self.source_actor)
        

class ActorOccurrence(NamedTuple):
    actor: Actor
    occurrence_id: int                   

    def to_string(self):
        return self.actor.to_string() + f"[{self.occurrence_id}]"

    def __repr__(self):
        return self.to_string()


class AmbiguousActorOccurrence(NamedTuple):
    """Execution activity whose RESULT_READY is outside the Event scope."""

    actor: Actor
    occurrence_id: int
    exec_id: str

    def to_string(self):
        return self.actor.to_string() + f"[!{self.occurrence_id}]"

    def __repr__(self):
        return self.to_string()


class ImmediateExecutionReference(SealableEvidence):
    """The one owner reference carried by an immediate Trace window."""

    def __init__(
        self,
        *,
        exec_id,
        actor=None,
        node_definition_id=None,
        ambiguous_occurrence=None,
        resolved_seq_id=None,
        resolved_actor_occurrence=None,
    ):
        self.exec_id = exec_id
        self.actor = actor
        self.node_definition_id = node_definition_id
        self.ambiguous_occurrence = ambiguous_occurrence
        self.resolved_seq_id = resolved_seq_id
        self.resolved_actor_occurrence = resolved_actor_occurrence

    @property
    def resolved(self):
        return self.resolved_seq_id is not None


class ImmediateEffectRecord(SealableEvidence):
    """Chronological Trace window created by one immediate Effect Event."""

    _tide_value_fields = frozenset()

    def __init__(
        self,
        *,
        seq_id,
        event_id,
        event_seq_id,
        exec_id,
        execution_reference,
    ):
        self.seq_id = seq_id
        self.event_id = event_id
        self.event_seq_id = event_seq_id
        self.exec_id = exec_id
        self.execution_reference = execution_reference
        self.effects = []
        self.deps = []
        self.writes = []

    @property
    def producer_id(self):
        return self.execution_reference.actor

    @property
    def producer_occurrence_id(self):
        return (
            self.execution_reference.resolved_actor_occurrence
            or self.execution_reference.ambiguous_occurrence
        )


class UnresolvedExecutionRecord(SealableEvidence):
    """Sealed execution-shaped projection for unresolved runtime activity."""

    _tide_value_fields = frozenset()

    def __init__(self, **facts):
        for name, value in facts.items():
            setattr(self, name, value)


class ReadyAttemptRecord(SealableEvidence):
    """Exact readiness evidence retained independently from execution."""

    def __init__(
        self,
        *,
        event_id,
        event_seq_id,
        exec_id,
        actor,
        node_definition_id,
        runner_id,
        pipeline_id,
        node_snapshot=None,
    ):
        self.event_id = event_id
        self.event_seq_id = event_seq_id
        self.exec_id = exec_id
        self.actor = actor
        self.node_definition_id = node_definition_id
        self.runner_id = runner_id
        self.pipeline_id = pipeline_id
        self.node_snapshot = node_snapshot
        self.outcome = "ready"

class EmittedNodeRecord(SealableEvidence):
    _tide_value_fields = frozenset()
    def __init__(
        self,
        ocean_id,
        runner_id,
        pipeline_id,
        node_id,
        func,
        func_name,
        inputs,
        declared_deps,
        declared_targets,
        declared_effects,
        origin,
        content_id,
        base_node,
        base_node_content_id,
        node_options,
        emitted_by_exec_id=None,
    ):
        self.ocean_id = ocean_id
        self.runner_id = runner_id
        self.pipeline_id = pipeline_id
        self.node_id = node_id
        self.producer_id = Actor(ocean_id, runner_id, func_name)
        self.func = func
        self.func_name = func_name
        self.inputs = inputs
        # An emitted node is definition evidence, not an execution.  These
        # selectors declare what a future execution would require; they are
        # never resolved DependencyUseRecords.
        self.declared_deps = list(declared_deps or ())
        self.declared_targets = declared_targets
        self.declared_effects = declared_effects
        if origin not in {"declared", "runtime", "unknown"}:
            raise ValueError(
                "EmittedNodeRecord origin must be 'declared', 'runtime', "
                "or 'unknown'"
            )
        self.origin = origin
        self.content_id = content_id
        self.base_node = base_node
        self.base_node_content_id = base_node_content_id
        self.node_options = dict(node_options or {})
        self.emitted_by_exec_id = emitted_by_exec_id

class ExecRecord(SealableEvidence):
    _tide_value_fields = frozenset({
        "output_value",
        "error",
        "node_options",
        "now_meta",
    })
    def __init__(
        self,
        seq_id,
        exec_id,
        ocean_id,
        runner_id,
        pipeline_id,
        node_id,
        func,
        func_name,
        inputs,
        deps,
        effects,
        declared_effects,
        declared_targets,
        runtime_effects,
        runtime_targets,
        declared_deps,
        unscoped_store_refs,
        status,
        started_at,
        finished_at,
        duration_ms,
        error,
        node_options,
        output_value=__TEMPLATE__,
        writes=None,
        emitted_nodes=None,
        base_node_id=None,
        base_node_content_id=None,
        content_id=None,
        handled_raise=False,
        mutated_during=None,
        now_meta=None,
        emitted_by_exec_id=None,
    ):
        self.seq_id = seq_id
        options = node_options or {}
        identity_options = options if isinstance(options, dict) else {}
        if identity_options.get("preserve_exec_id") is True:
            self.exec_id = exec_id
        elif isinstance(exec_id, __fallback__):
            self.exec_id = exec_id
        else:
            self.exec_id = (
                f"TIDE_EXEC:{exec_id}"
                if not exec_id.startswith("TIDE_EXEC:")
                else exec_id
            )
        self.source_ocean_id = ocean_id
        self.producer_id = Actor(ocean_id, runner_id, func_name)
        self.producer_occurrence_id = None
        self.runner_id = runner_id
        self.pipeline_id = pipeline_id
        self.node_id = node_id
        self.func = func
        self.func_name = func_name
        self.inputs = inputs
        self.deps = deps
        self.effects = list(effects or ())
        self.affected = []

        self.declared_deps = declared_deps
        self.declared_effects = declared_effects
        self.declared_targets = declared_targets
        self.runtime_effects = runtime_effects or []
        self.runtime_targets = runtime_targets or []
        self.unscoped_store_refs = unscoped_store_refs

        self.status = status
        self.started_at = started_at
        self.finished_at = finished_at
        self.duration_ms = duration_ms
        self.error = error
        self.handled_raise = handled_raise
        self.mutated_during = {
            reference: list(input_names)
            for reference, input_names in (mutated_during or {}).items()
        }
        self.now_meta = now_meta

        self.output_value = output_value
        self.writes = writes or []

        self.emitted_nodes = emitted_nodes or []
        self.emitted_by_exec_id = emitted_by_exec_id
        self.base_node_id = base_node_id
        self.base_node_content_id = base_node_content_id
        self.content_id = content_id
        self.node_options = options

    def now(self):
        return self.now_meta


class AffectedRecord(SealableEvidence):
    def __init__(
        self,
        effect,
        targets,
        affectee,
        affected_inputs,
        affected_exec_id=None,
    ):
        self.effect = effect
        self.targets = targets
        self.affectee = affectee
        self.affected_inputs = affected_inputs
        self.affected_exec_id = affected_exec_id


class EffectRecord(SealableEvidence):
    _tide_value_fields = frozenset({"targets", "output"})
    def __init__(
        self,
        effect,
        targets,
        *,
        event_id: str,
        origin: str,
        immediate: bool,
        output=__TEMPLATE__,
        affected=None,
        target_existed=None,
    ):
        if not isinstance(event_id, str):
            raise TypeError("EffectRecord event_id must be a string")
        if origin not in {"declared", "runtime"}:
            raise ValueError(
                "EffectRecord origin must be 'declared' or 'runtime'"
            )
        if not isinstance(immediate, bool):
            raise TypeError("EffectRecord immediate must be a bool")
        if effect in {Effects.READ, Effects.DELETE}:
            if not isinstance(target_existed, bool):
                raise TypeError("READ and DELETE EffectRecords require target_existed")
        elif target_existed is not None:
            raise ValueError("target_existed is valid only for READ and DELETE")

        self.event_id = event_id
        self.effect = effect
        self.targets = targets
        self.output = output
        self.origin = origin
        self.immediate = immediate
        self.target_existed = target_existed
        self.affected = affected if affected is not None else {}


def _associated_input_key(associated_input):
    if not associated_input:
        return None

    if isinstance(associated_input, dict):
        return tuple(sorted(associated_input.keys()))

    if isinstance(associated_input, tuple) and len(associated_input) == 2:
        name, input_obj = associated_input
        return (
            name,
            tide_value_hasher(getattr(input_obj, "raw", None)),
            tide_value_hasher(getattr(input_obj, "evaluated", None)),
            tide_value_hasher(getattr(input_obj, "normalised", None)),
            tide_value_hasher(getattr(input_obj, "resolved", None)),
        )

    return repr(associated_input)


class InputUseRecord(SealableEvidence):
    _tide_value_fields = frozenset({"raw", "evaluated", "normalised", "resolved"})
    def __init__(
        self,
        name,
        raw,
        evaluated,
        normalised,
        resolved,
        dependencies,
    ):
        self.name = name
        self.raw = raw
        self.evaluated = evaluated
        self.normalised = normalised
        self.resolved = resolved
        self.dependencies = dependencies

    def __eq__(self, other):
        if not isinstance(other, InputUseRecord):
            return False

        return all([
            self.name == other.name,
            safe_equal(self.raw, other.raw),
            safe_equal(self.evaluated, other.evaluated),
            safe_equal(self.normalised, other.normalised),
            safe_equal(self.resolved, other.resolved),
            len(self.dependencies) == len(other.dependencies),
            all(
                safe_equal(own, expected)
                for own, expected in zip(
                    self.dependencies,
                    other.dependencies,
                    strict=True,
                )
            ),
        ])


class WriteRecord(SealableEvidence):
    _tide_value_fields = frozenset({"stored_result"})
    def __init__(
        self,
        cause: Cause,
        stored_result,
        stored_version,
        stored_target,
        snapshot_lineage=None,
        observed_mutable=False,
        updated=False,
    ):
        if not isinstance(cause, Cause):
            raise TypeError("WriteRecord cause must be a Cause")
        self.cause = cause
        self.stored_result = stored_result
        self.stored_version = stored_version
        self.stored_target = stored_target
        self.snapshot_lineage = list(snapshot_lineage or ())
        self.observed_mutable = observed_mutable
        self.updated = updated

    def __eq__(self, other):
        if not isinstance(other, WriteRecord):
            return False

        return all([
            self.cause == other.cause,
            safe_equal(self.stored_result, other.stored_result),
            self.stored_version == other.stored_version,
            self.stored_target == other.stored_target,
            self.snapshot_lineage == other.snapshot_lineage,
            self.observed_mutable == other.observed_mutable,
            self.updated == other.updated,
        ])


class DependencyUseRecord(SealableEvidence):
    _tide_value_fields = frozenset({"resolved_value", "input_path"})
    def __init__(
        self,
        associated_input: tuple,
        dep: DataRef,
        type: str,
        unscoped_ref: DataRef = None,
        resolved_val: Any = None,
        resolved_ver: int = None,
        producer_source: int | str = None,
        producer_actor: int | str = None,
        producer_exec_id: int = None,
        expanded_to: list = None,
        input_index: int = None,
        input_path=None,
        snapshot_lineage=None,
        observed_mutable=False,
        observation_id=None,
    ):
        self.dep = dep
        self.type = type
        self.unscoped_reference = unscoped_ref

        if type == "dynamic":
            self.associated_input = associated_input
            self.expanded_to: list[ExpandedDepRecord] = expanded_to
            self.input_index = input_index
            self.input_path = input_path
            self.snapshot_lineage = list(snapshot_lineage or ())
            self.observed_mutable = observed_mutable
            self.observation_id = observation_id
            return

        self.associated_input = associated_input
        self.resolved_value = resolved_val
        self.resolved_version = resolved_ver
        self.producer_source = producer_source
        self.producer_actor = producer_actor
        self.producer_exec_id = producer_exec_id
        self.input_index = input_index
        self.input_path = input_path
        self.snapshot_lineage = list(snapshot_lineage or ())
        self.observed_mutable = observed_mutable
        self.observation_id = observation_id

    def __repr__(self):
        return f"DepUseRecord({self.unscoped_reference})"

    def __eq__(self, other):
        if not isinstance(other, DependencyUseRecord):
            return False

        if self.type != other.type:
            return False

        if self.type == "dynamic":
            return all([
                self.dep == other.dep,
                self.unscoped_reference == other.unscoped_reference,
                self._associated_input_key()
                == other._associated_input_key(),
                list(self.expanded_to or [])
                == list(other.expanded_to or []),
                self.input_index == other.input_index,
                self.input_path == other.input_path,
                self.snapshot_lineage == other.snapshot_lineage,
                self.observed_mutable == other.observed_mutable,
                self.observation_id == other.observation_id,
            ])

        return all([
            self.dep == other.dep,
            self.unscoped_reference == other.unscoped_reference,
            tide_value_hasher(self.resolved_value)
            == tide_value_hasher(other.resolved_value),
            self.resolved_version == other.resolved_version,
            self.producer_source == other.producer_source,
            self.producer_actor == other.producer_actor,
            self.producer_exec_id == other.producer_exec_id,
            self._associated_input_key()
            == other._associated_input_key(),
            self.input_index == other.input_index,
            self.input_path == other.input_path,
            self.snapshot_lineage == other.snapshot_lineage,
            self.observed_mutable == other.observed_mutable,
            self.observation_id == other.observation_id,
        ])

    def _associated_input_key(self):
        return _associated_input_key(self.associated_input)


class ExpandedDepRecord(SealableEvidence):
    _tide_value_fields = frozenset({"resolved_value", "input_path"})
    def __init__(
        self,
        associated_input,
        dep: DataRef,
        type: str,
        unscoped_ref: DataRef,
        resolved_val: Any,
        resolved_ver: int,
        producer_source: int | str,
        producer_actor: int | str,
        producer_exec_id: int,
        expanded_from: list,
        input_index: int = None,
        input_path=None,
        snapshot_lineage=None,
        observed_mutable=False,
        observation_id=None,
    ):
        self.associated_input = associated_input
        self.dep = dep
        self.type = type
        self.unscoped_reference = unscoped_ref
        self.resolved_value = resolved_val
        self.resolved_version = resolved_ver
        self.producer_source = producer_source
        self.producer_actor = producer_actor
        self.producer_exec_id = producer_exec_id
        self.expanded_from = expanded_from
        self.input_index = input_index
        self.input_path = input_path
        self.snapshot_lineage = list(snapshot_lineage or ())
        self.observed_mutable = observed_mutable
        self.observation_id = observation_id

    def __eq__(self, other):
        if not isinstance(other, ExpandedDepRecord):
            return False

        return all([
            self.dep == other.dep,
            self.type == other.type,
            self.unscoped_reference == other.unscoped_reference,
            tide_value_hasher(self.resolved_value)
            == tide_value_hasher(other.resolved_value),
            self.resolved_version == other.resolved_version,
            self.producer_source == other.producer_source,
            self.producer_actor == other.producer_actor,
            self.producer_exec_id == other.producer_exec_id,
            self._associated_input_key()
            == other._associated_input_key(),
            list(self.expanded_from or [])
            == list(other.expanded_from or []),
            self.input_index == other.input_index,
            self.input_path == other.input_path,
            self.snapshot_lineage == other.snapshot_lineage,
            self.observed_mutable == other.observed_mutable,
            self.observation_id == other.observation_id,
        ])

    def _associated_input_key(self):
        return _associated_input_key(self.associated_input)


class Inputs(SealableEvidence):
    def __init__(self, input_log_dict):
        nothing_args = [(), (), (), (), []]
        nothing_kwargs = [{}, {}, {}, {}, []]
        self.args = input_log_dict.get(
            "args",
            InputUseRecord("args", *nothing_args),
        )
        self.kwargs = input_log_dict.get(
            "kwargs",
            InputUseRecord("kwargs", *nothing_kwargs),
        )
        self.p_args = input_log_dict.get(
            "p_args",
            InputUseRecord("p_args", *nothing_args),
        )
        self.p_kwargs = input_log_dict.get(
            "p_kwargs",
            InputUseRecord("p_kwargs", *nothing_kwargs),
        )
        self.k_args = input_log_dict.get(
            "k_args",
            InputUseRecord("k_args", *nothing_args),
        )
        self.k_kwargs = input_log_dict.get(
            "k_kwargs",
            InputUseRecord("k_kwargs", *nothing_kwargs),
        )

        # Preserve factual inputs added outside the six canonical argument
        # buckets (for example runtime READ evidence). Reconstruction already
        # supplies these InputUseRecords; Inputs owns the complete input set.
        for name, input_record in input_log_dict.items():
            if not hasattr(self, name):
                setattr(self, name, input_record)

    def items(self):
        # Runtime effects can add factual inputs after argument resolution.
        return (
            (name, value)
            for name, value in vars(self).items()
            if not name.startswith("_tide_")
        )

    def __repr__(self):
        used = []
        for name, input_use_log in self.items():
            if (
                input_use_log.raw
                or input_use_log.evaluated
                or input_use_log.dependencies
            ):
                used.append(name)
        return f"Inputs({used})"

    def __eq__(self, other):
        if not isinstance(other, Inputs):
            return False

        own_inputs = dict(self.items())
        other_inputs = dict(other.items())
        return (
            own_inputs.keys() == other_inputs.keys()
            and all(
                own_inputs[name] == other_inputs[name]
                for name in own_inputs
            )
        )
