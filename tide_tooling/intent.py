"""Small prospective projection attached to a runtime Trace."""

from __future__ import annotations

import copy
import warnings
from collections import defaultdict
from dataclasses import dataclass
from typing import Mapping

from tide_tooling.tooling_core.engine.classes import DataRef


def _ref_name(reference):
    if isinstance(reference, DataRef):
        return DataRef.to_string(reference, strip_leading=True)
    return str(reference)


def _pair(entry):
    if isinstance(entry, dict):
        return entry["exec_id"], entry["state_id"]
    return entry


def _public_ref(reference, runner=None):
    ref = _ref_name(reference)
    if runner is None or ":" not in ref:
        return ref
    store, key = ref.rsplit(":", 1)
    aliases = getattr(getattr(runner, "store_router", None), "store_alias", {})
    for public_store, scoped_stores in aliases.items():
        if store in scoped_stores:
            return f"{public_store}:{key}"
    return ref


@dataclass(frozen=True)
class IntentIdentityMap:
    """All prospective IDs and every actual ID normalized into that space."""

    plan: tuple[tuple, ...]
    actual: dict[str, tuple]
    _plan_model_ids: dict[tuple, str]

    @property
    def plan_only(self):
        return tuple(sorted(set(self.plan) - set(self.actual.values()), key=repr))

    @property
    def actual_only(self):
        return tuple(sorted(set(self.actual.values()) - set(self.plan), key=repr))

    @property
    def paired(self):
        actual_by_identity = {
            identity: actual_id
            for actual_id, identity in self.actual.items()
        }
        return {
            self._plan_model_ids[identity]: actual_by_identity[identity]
            for identity in self.plan
            if identity in actual_by_identity
        }

    def to_dict(self):
        return {
            "plan": [list(identity) for identity in self.plan],
            "actual": {
                actual_id: list(identity)
                for actual_id, identity in self.actual.items()
            },
            "plan_only": [list(identity) for identity in self.plan_only],
            "actual_only": [list(identity) for identity in self.actual_only],
        }


@dataclass(frozen=True)
class IntentMap:
    """Complete two-sided alignment over Execution and State carriers."""

    executions: IntentIdentityMap
    states: IntentIdentityMap
    _plan_model: dict
    _actual_model: dict

    @property
    def execution_map(self):
        return self.executions.paired

    @property
    def state_map(self):
        return self.states.paired

    @property
    def plan_only(self):
        return {
            "executions": self.executions.plan_only,
            "states": self.states.plan_only,
        }

    @property
    def actual_only(self):
        return {
            "executions": self.executions.actual_only,
            "states": self.states.actual_only,
        }

    def compare(self):
        """Apply the existing TIDE comparator to the paired map projection."""
        from tide_tooling.comparator import comparator

        report = comparator(
            self._plan_model,
            self._actual_model,
            execution_map=self.execution_map,
            state_map=self.state_map,
        )
        report["intent_map"] = self.to_dict()
        report["identity_alignment_complete"] = not any((
            self.executions.plan_only,
            self.executions.actual_only,
            self.states.plan_only,
            self.states.actual_only,
        ))
        report["intent_equal"] = (
            report["complete_agreement"]
            and report["identity_alignment_complete"]
        )
        return report

    def to_dict(self):
        return {
            "executions": self.executions.to_dict(),
            "states": self.states.to_dict(),
        }


def _normalise_expected(dynamic_refs):
    if dynamic_refs is None:
        return {}, {}
    if not isinstance(dynamic_refs, Mapping):
        raise TypeError("intent_dynamic_refs must be a dictionary")

    normalised = {}
    declared_names = {}
    for selector, concrete_refs in dynamic_refs.items():
        declared_name = (
            selector
            if isinstance(selector, str)
            else _ref_name(selector)
        )
        if declared_name.startswith("@"):
            declared_name = declared_name[1:]
        dynamic_ref = (
            selector
            if isinstance(selector, DataRef)
            else DataRef.from_string(selector, strip_leading=False)
        )
        if not dynamic_ref.dynamic:
            raise ValueError(
                f"Intent dynamic reference {_ref_name(dynamic_ref)!r} is not dynamic"
            )
        if isinstance(concrete_refs, (str, DataRef)):
            concrete_refs = (concrete_refs,)
        refs = tuple(
            value
            if isinstance(value, DataRef)
            else DataRef.from_string(value, strip_leading=False)
            for value in concrete_refs
        )
        if any(reference.dynamic for reference in refs):
            raise ValueError(
                f"Intent concretisations for {_ref_name(dynamic_ref)!r} "
                "must be static references"
            )
        canonical_name = _ref_name(dynamic_ref)
        normalised[canonical_name] = refs
        declared_names[canonical_name] = declared_name
    return normalised, declared_names


def _actual_concretisations(trace, runner=None):
    sigma = defaultdict(set)
    for record in trace.exec_log:
        for dependency in record.deps or ():
            if getattr(dependency, "type", None) != "dynamic":
                continue
            selector = _ref_name(dependency.dep)
            for concrete in dependency.expanded_to or ():
                sigma[selector].add((
                    _public_ref(concrete.unscoped_reference, runner),
                    concrete.resolved_version,
                ))
    return {
        selector: tuple(sorted(states, key=repr))
        for selector, states in sorted(sigma.items())
    }


def _actual_model(trace):
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="Trace States have no host-identified producer:.*",
            category=RuntimeWarning,
        )
        return trace.to_model(
            include_metadata=True,
            include_hashes=False,
            assign_unknown=True,
        )


def _actual_keys(trace, model, runner=None):
    exec_ids = {
        execution
        for relation in (model["I"], model["O"])
        for execution, _ in (_pair(entry) for entry in relation)
    }
    execution_keys = {}
    unknown_seq_ids = []
    for record in trace.exec_log:
        exec_id = record.exec_id
        actor = record.producer_id.source_actor
        if (
            exec_id not in exec_ids
            and "unknown" in exec_ids
            and actor in {"TIDE", "TIDE: Polling"}
        ):
            unknown_seq_ids.append(record.seq_id)
            continue
        execution_keys[exec_id] = (actor, record.seq_id)

    if "unknown" in exec_ids:
        execution_keys["unknown"] = (
            "unknown",
            min(unknown_seq_ids) if unknown_seq_ids else 0,
        )

    state_keys = {}
    for state_id, metadata in model.get("states", {}).items():
        if "reference" in metadata and "version" in metadata:
            state_keys[state_id] = (
                _public_ref(metadata["reference"], runner),
                metadata["version"],
            )
    return execution_keys, state_keys


def _identity_map(plan_model_ids, actual):
    return IntentIdentityMap(
        plan=tuple(sorted(plan_model_ids, key=repr)),
        actual=dict(sorted(actual.items(), key=lambda item: repr(item[1]))),
        _plan_model_ids=dict(plan_model_ids),
    )


def build_trace_intent(trace, runner):
    """Build prospective structure and total alignment at Trace init."""
    trace.concretisations = _actual_concretisations(trace, runner)
    if not all(hasattr(runner, name) for name in ("index", "pipeline")):
        return None

    expected, declared_dynamic_names = _normalise_expected(
        getattr(runner, "intent_dynamic_refs", None)
    )
    trace.concretisations = {
        declared_dynamic_names.get(selector, selector): states
        for selector, states in trace.concretisations.items()
    }
    topo_ids = tuple(runner.dag_to_run.topo_order)
    intent_node_ids = set(topo_ids)
    nodes_by_id = runner.index.node_index.node_by_id
    definition_by_id = runner.index.node_index.node_id_to_definiton_node
    explicit_definitions = tuple(
        node
        for stage in runner.pipeline.stages
        for node in stage.nodes
    )
    explicit_ids = tuple(
        node_id
        for definition in explicit_definitions
        for node_id, candidate in definition_by_id.items()
        if candidate is definition
    )
    selected_ids = (
        explicit_ids
        if set(explicit_ids) == intent_node_ids
        else topo_ids
    )
    nodes = tuple(nodes_by_id[node_id] for node_id in selected_ids)
    versions = defaultdict(lambda: -1)
    latest = {}
    inputs = set()
    outputs = set()
    executions = {}
    states = {}
    plan_exec_by_key = {}
    plan_state_by_key = {}
    sigma = defaultdict(dict)

    next_execution_seq = 0
    input_exec_id = None

    def execution(actor):
        nonlocal next_execution_seq
        sequence = next_execution_seq
        next_execution_seq += 1
        exec_id = f"intent-exec:{actor}[{sequence}]"
        key = (actor, sequence)
        plan_exec_by_key[key] = exec_id
        executions[exec_id] = {
            "actor": {"name": actor, "source": "intent"},
            "intent_key": list(key),
        }
        return exec_id

    def produce(exec_id, reference):
        ref = _ref_name(reference)
        versions[ref] += 1
        version = versions[ref]
        key = (ref, version)
        state_id = f"intent-state:{ref}[v{version}]"
        outputs.add((exec_id, state_id))
        latest[ref] = state_id
        plan_state_by_key[key] = state_id
        states[state_id] = {
            "reference": ref,
            "version": version,
            "intent_key": [ref, version],
        }
        return state_id

    def external_state(reference):
        nonlocal input_exec_id
        ref = _ref_name(reference)
        if ref in latest:
            return latest[ref]
        if input_exec_id is None:
            input_exec_id = execution("unknown")
        return produce(input_exec_id, reference)

    # Initial States are projected before declared node occurrences.
    seen_refs = set()
    initial_refs = []
    for node in nodes:
        for dependency in node.deps:
            candidates = (
                expected.get(_ref_name(dependency), ())
                if dependency.dynamic
                else (dependency,)
            )
            for candidate in candidates:
                ref = _ref_name(candidate)
                if ref not in seen_refs:
                    initial_refs.append(candidate)
                    seen_refs.add(ref)
        seen_refs.update(_ref_name(target) for target in node.targets)
    for reference in initial_refs:
        external_state(reference)

    for node in nodes:
        exec_id = execution(node.node_name)
        for dependency in node.deps:
            if not dependency.dynamic:
                inputs.add((
                    exec_id,
                    latest.get(_ref_name(dependency))
                    or external_state(dependency),
                ))
                continue

            selector = _ref_name(dependency)
            concrete_refs = expected.get(selector, ())
            sigma[selector]
            minimum = 1 if dependency.count is None else dependency.count
            if concrete_refs and len(concrete_refs) < minimum:
                raise ValueError(
                    f"Intent dynamic reference {selector!r} expects "
                    f"{len(concrete_refs)} States but MIN is {minimum}"
                )
            if dependency.max is not None:
                concrete_refs = concrete_refs[:dependency.max]
            for concrete in concrete_refs:
                ref = _ref_name(concrete)
                state_id = sigma[selector].get(ref)
                if state_id is None:
                    state_id = latest.get(ref) or external_state(concrete)
                    sigma[selector][ref] = state_id
                inputs.add((exec_id, state_id))

        for target in node.targets:
            produce(exec_id, target)

    model = {
        "schema": "TIDE-Model",
        "schema_version": 1,
        "host": "Ocean Intent Graph",
        "E": sorted(executions),
        "S": sorted(states),
        "I": [
            {"exec_id": execution_id, "state_id": state_id}
            for execution_id, state_id in sorted(inputs)
        ],
        "O": [
            {"exec_id": execution_id, "state_id": state_id}
            for execution_id, state_id in sorted(outputs)
        ],
        "executions": executions,
        "states": states,
        "host_record": {
            "projection_kind": "intent",
            "observed": False,
            "pipeline": runner.pipeline.name,
        },
    }

    from tide_tooling.core import TIDE

    validation = TIDE.validate_model(model)
    if not validation["semantically_valid"]:
        raise ValueError(
            "Intent projection failed TIDE validation: "
            + "; ".join(validation["errors"])
        )
    plan = TIDE.from_model(
        model,
        name=f"TIDE: Intent ({runner.pipeline.name})",
    )
    plan.concretisations = {
        declared_dynamic_names.get(selector, selector): tuple(
            sorted(
                (
                    (states[state_id]["reference"], states[state_id]["version"])
                    for state_id in concrete.values()
                ),
                key=repr,
            )
        )
        for selector, concrete in sorted(sigma.items())
    }

    actual_model = _actual_model(trace)
    actual_exec_by_key, actual_state_by_key = _actual_keys(
        trace,
        actual_model,
        runner,
    )
    plan.map = IntentMap(
        executions=_identity_map(plan_exec_by_key, actual_exec_by_key),
        states=_identity_map(plan_state_by_key, actual_state_by_key),
        _plan_model=copy.deepcopy(model),
        _actual_model=copy.deepcopy(actual_model),
    )
    return plan


__all__ = ["IntentIdentityMap", "IntentMap", "build_trace_intent"]
