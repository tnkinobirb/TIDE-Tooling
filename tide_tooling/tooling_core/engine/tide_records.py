"""Execution evidence records exposed by TIDE traces.

These classes contain captured evidence and identity. Trace reconstruction,
indexing, queries, and display orchestration remain in ``tide_logging``.
``tide_logging`` re-exports these names for compatibility with older imports.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from tide_tooling.tooling_core.engine.classes import DataRef, __TIDE_TEMPLATE_VALUE__
from tide_tooling.tooling_core.engine.tide_rendering import _show_fields
from tide_tooling.tooling_core.engine.utils import safe_equal
from tide_tooling.tooling_core.engine.utils import tide_value_hasher


__TEMPLATE__ = __TIDE_TEMPLATE_VALUE__()


def _diff(subject, other):
    # Imported only when a user asks for a diff, after tide_logging has
    # completed importing and re-exported these record classes.
    from tide_tooling.tooling_core.engine.tide_query_results import _build_compared

    return _build_compared(subject, other)


class Actor:
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
        return isinstance(value, __TIDE_TEMPLATE_VALUE__)

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


class EmittedNodeRecord:
    def __init__(
        self,
        ocean_id,
        runner_id,
        pipeline_id,
        node_id,
        func,
        func_name,
        inputs,
        deps,
        declared_targets,
        declared_effects,
        emitted_by,
        content_id,
        base_node,
        base_node_content_id,
    ):
        self.ocean_id = ocean_id
        self.runner_id = runner_id
        self.pipeline_id = pipeline_id
        self.node_id = node_id
        self.producer_id = Actor(ocean_id, runner_id, func_name)
        self.func = func
        self.func_name = func_name
        self.inputs = inputs
        self.deps = deps
        self.declared_targets = declared_targets
        self.declared_effects = declared_effects
        self.emitted_by = emitted_by
        self.content_id = content_id
        self.base_node = base_node
        self.base_node_content_id = base_node_content_id


class EmittedNodesRecord:
    def __init__(self, emitted_nodes):
        self.emitted_nodes = emitted_nodes


class ExecRecord:
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
        resolve_output=None,
        writes=None,
        emitted_by=None,
        emitted_nodes=None,
        base_node_id=None,
        base_node_content_id=None,
        content_id=None,
        handled_raise=False,
    ):
        self.seq_id = seq_id
        options = node_options or {}
        identity_options = options if isinstance(options, dict) else {}
        if identity_options.get("preserve_exec_id") is True:
            self.exec_id = exec_id
        elif isinstance(exec_id, __TIDE_TEMPLATE_VALUE__):
            self.exec_id = exec_id
        else:
            self.exec_id = (
                f"TIDE_EXEC:{exec_id}"
                if "TIDE_EXEC:" not in exec_id
                else exec_id
            )
        self.source_ocean_id = ocean_id
        self.producer_id = Actor(ocean_id, runner_id, func_name)
        self.runner_id = runner_id
        self.pipeline_id = pipeline_id
        self.node_id = node_id
        self.func = func
        self.func_name = func_name
        self.inputs = inputs
        self.deps = deps
        self.effects = effects or {}
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

        self.output_value = output_value
        self.resolve_output = (
            resolve_output
            if resolve_output is not None
            else []
        )
        self.writes = writes or []

        self.emitted_by = emitted_by
        self.emitted_nodes = emitted_nodes or []
        self.base_node_id = base_node_id
        self.base_node_content_id = base_node_content_id
        self.content_id = content_id
        self.node_options = options


class AffectedRecord:
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


class EffectRecord:
    def __init__(
        self,
        effect,
        targets,
        output=__TEMPLATE__,
        preserved_output=__TEMPLATE__,
        affected=None,
    ):
        self.effect = effect
        self.targets = targets
        self.output = output
        self.preserved_output = preserved_output
        self.affected = affected if affected is not None else defaultdict()


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


class InputUseRecord:
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

        own_dependencies = [
            getattr(dependency, "dep", dependency)
            for dependency in self.dependencies
        ]
        other_dependencies = [
            getattr(dependency, "dep", dependency)
            for dependency in other.dependencies
        ]

        return all([
            self.name == other.name,
            safe_equal(self.raw, other.raw),
            safe_equal(self.evaluated, other.evaluated),
            safe_equal(self.normalised, other.normalised),
            safe_equal(self.resolved, other.resolved),
            len(own_dependencies) == len(other_dependencies),
            all(
                safe_equal(own, expected)
                for own, expected in zip(
                    own_dependencies,
                    other_dependencies,
                    strict=True,
                )
            ),
        ])


class WriteRecord:
    def __init__(
        self,
        cause,
        stored_result,
        stored_version,
        stored_target,
        snapshot_lineage=None,
        observed_mutable=False,
    ):
        if (
            not isinstance(cause, str)
            or "TIDE: .event(<function B" in cause
        ):
            raise ValueError(
                f"Cause {cause} for write "
                f"{(stored_target, stored_version)} was invalid!"
            )
        self.cause = cause
        self.stored_result = stored_result
        self.stored_version = stored_version
        self.stored_target = stored_target
        self.snapshot_lineage = list(snapshot_lineage or ())
        self.observed_mutable = observed_mutable

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
        ])


class DependencyUseRecord:
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
        ])

    def _associated_input_key(self):
        return _associated_input_key(self.associated_input)


class ExpandedDepRecord:
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
        ])

    def _associated_input_key(self):
        return _associated_input_key(self.associated_input)


class Inputs:
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

    def items(self):
        # Runtime effects can add factual inputs after argument resolution.
        return vars(self).items()

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

        own_inputs = vars(self)
        other_inputs = vars(other)
        return (
            own_inputs.keys() == other_inputs.keys()
            and all(
                own_inputs[name] == other_inputs[name]
                for name in own_inputs
            )
        )
