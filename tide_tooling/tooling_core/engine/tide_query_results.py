from __future__ import annotations

"""Structured query results and comparison views for TIDE traces.

These objects contain and render evidence already selected by ``Trace``.
Query execution and trace ownership remain in ``tide_logging``.  The legacy
module re-exports every public name here for import compatibility.
"""

import copy
from collections import defaultdict
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from tide_tooling.tooling_core.engine.tide_logging import Trace

from tide_tooling.tooling_core.engine.classes import DataRef, __fallback__
from tide_tooling.tooling_core.engine.events import Effects
from tide_tooling.tooling_core.engine.tide_links import (
    EffectEdge,
    EmissionEdge,
    ReadEdge,
)
from tide_tooling.tooling_core.engine.tide_records import (
    DependencyUseRecord,
    EffectRecord,
    ExecRecord,
    WriteRecord,
)
from tide_tooling.tooling_core.engine.tide_rendering import (
    SHOW_BOLD,
    SHOW_CYAN,
    SHOW_DIM,
    SHOW_GREEN,
    SHOW_RED,
    SHOW_RESET,
    SHOW_SECTION_WIDTH,
    SHOW_TREE_BRANCH,
    SHOW_TREE_LAST,
    SHOW_TREE_PIPE,
    SHOW_WHITE,
    SHOW_YELLOW,
    _show_options,
    _show_render,
    display_identity,
    show_header,
    show_identity,
    show_query_display,
    show_section,
    shorten_identity,
)
from tide_tooling.tooling_core.engine.utils import DedupedList, safe_equal


__TEMPLATE__ = __fallback__()


def _state_occurrence_text(ref, version, *, observed_mutable=False):
    """Render one State occurrence without changing its canonical identity."""
    versioned = str(ref) if version is None else f"{ref}[v{version}]"
    return f"{versioned}{'*' if observed_mutable else ''}"


def _recorded_state_descriptor(trace, ref, version):
    if trace is None:
        return None

    state = (ref, version)
    index = trace._index
    value_hash = index.value_hash_by_ref_version.get(state)
    if value_hash is None:
        return None

    records = (
        tuple(index.write_records_by_ref_version.get(state, ()))
        + tuple(index.read_records_by_ref_version.get(state, ()))
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


def _state_query_evidence(trace, ref, version):
    state_id = _state_occurrence_text(ref, version)

    if trace is None:
        return {
            "state": {"ref": ref, "version": version, "id": state_id},
            "recorded_as": {"type": None, "hash": None, "repr": None},
            "divergent": False,
            "reconstructed_as": None,
        }

    index = trace._index
    value_hash = index.value_hash_by_ref_version.get((ref, version))
    descriptor = _recorded_state_descriptor(trace, ref, version) or {}
    recorded_type = descriptor.get("type")
    recorded_repr = descriptor.get("repr")
    type_claimed = "type" in descriptor
    snapshot = index._value_snapshots_by_hash.get(value_hash)
    backing = index.value_by_hash.get(value_hash)
    from tide_tooling.tooling_core.bundling.TIDEOpaqueValue import (
        TIDEOpaqueValue,
    )
    if not type_claimed and isinstance(backing, TIDEOpaqueValue):
        if recorded_type is None:
            recorded_type = backing.type_name
        if recorded_repr is None:
            recorded_repr = backing.representation
    if not type_claimed and snapshot is not None:
        if recorded_type is None:
            recorded_type = snapshot.type_name
        if recorded_repr is None:
            recorded_repr = snapshot.representation
    warning = next(
        (
            warning
            for warning in trace.divergence.get("warnings", ())
            if isinstance(warning, dict)
            and warning.get("type") == "value_hash_mismatch"
            and warning.get("expected") == value_hash
        ),
        None,
    )

    reconstructed = None
    if warning is not None:
        reconstructed = {
            "type": (
                backing.type_name
                if isinstance(backing, TIDEOpaqueValue)
                else type(backing).__name__
                if backing is not None
                else None
            ),
            "hash": warning.get("observed"),
        }

    if recorded_type in {"TIDEUnknownType", "TIDEUnknownValue"}:
        recorded_type = f"opaque<{recorded_type}>"

    return {
        "state": {"ref": ref, "version": version, "id": state_id},
        "recorded_as": {
            "type": recorded_type,
            "hash": value_hash,
            "repr": recorded_repr,
        },
        "divergent": warning is not None,
        "reconstructed_as": reconstructed,
    }


def _query_divergence(trace, states):
    seen = set()
    divergent = []

    for ref, version in states:
        state = (ref, version)
        if state in seen:
            continue
        seen.add(state)

        evidence = _state_query_evidence(trace, ref, version)
        if evidence["divergent"]:
            divergent.append(evidence)

    return {
        "divergent": bool(divergent),
        "states": divergent,
    }


def _inspected_states(inspected):
    states = []

    if inspected.type == "reference":
        if inspected.grouped:
            states.extend(
                tuple(item)
                for item in inspected.history
                if isinstance(item, (tuple, list)) and len(item) == 2
            )
        else:
            versions = (
                [inspected.version]
                if inspected.version is not None
                else list(inspected.history)
            )
            states.extend((inspected.id, version) for version in versions)

    elif inspected.type == "value":
        states.extend(
            tuple(item)
            for item in inspected.history
            if isinstance(item, (tuple, list)) and len(item) == 2
        )

    for write in inspected.writes or ():
        ref = getattr(write, "stored_target", None)
        version = getattr(write, "stored_version", None)
        if ref is not None:
            states.append((ref, version))

    for dependency in inspected.reads or ():
        reads = (
            dependency.expanded_to or ()
            if getattr(dependency, "type", None) == "dynamic"
            else (dependency,)
        )
        for read in reads:
            ref = getattr(read, "unscoped_reference", None)
            version = getattr(read, "resolved_version", None)
            if ref is not None:
                states.append((ref, version))

    effects = getattr(inspected.object, "effects", ()) or ()
    for effect in effects:
        if (
            getattr(effect, "effect", None)
            in {Effects.READ, Effects.WRITE, Effects.DELETE}
            and isinstance(getattr(effect, "targets", None), tuple)
            and len(effect.targets) == 2
        ):
            states.append(tuple(effect.targets))

    return states


class Waiting:
    """Definitions and exact References recorded as waiting at settlement."""

    def __init__(self, deadlock):
        self._deadlock = copy.deepcopy(deadlock or {})

    @staticmethod
    def _reference(reference):
        if isinstance(reference, DataRef):
            return DataRef.to_string(reference, prefix=False)
        return str(reference)

    @property
    def entries(self):
        return tuple(
            {
                "runner_id": runner_id,
                "node_definition_id": str(node_definition_id),
                "references": tuple(
                    self._reference(reference)
                    for reference in references
                ),
            }
            for runner_id, waiting in self._deadlock.items()
            for node_definition_id, references in waiting.items()
        )

    def __len__(self):
        return len(self.entries)

    def __bool__(self):
        return bool(self.entries)

    def to_dict(self):
        return {
            "type": "waiting",
            "deadlocked": bool(self),
            "count": len(self),
            "definitions": [
                {
                    "runner_id": _query_json_value(entry["runner_id"]),
                    "node_definition_id": entry["node_definition_id"],
                    "references": list(entry["references"]),
                }
                for entry in self.entries
            ],
        }

    def show(self, color=True, simple=False, colour=None):
        color, simple = _show_options(color, simple, colour)
        count = len(self)
        lines = show_header(
            "WAITING",
            f"{count} waiting {'definition' if count == 1 else 'definitions'}",
        )
        if not self.entries:
            lines.append(f"  {SHOW_DIM}none{SHOW_RESET}")
            return _show_render(lines, color)

        for entry in self.entries:
            lines.append(
                f"  {SHOW_CYAN}{entry['node_definition_id']}{SHOW_RESET} "
                f"{SHOW_DIM}(runner {entry['runner_id']}){SHOW_RESET}"
            )
            for reference in entry["references"]:
                lines.append(
                    f"    {SHOW_RED}waiting for:{SHOW_RESET} "
                    f"{SHOW_WHITE}{reference}{SHOW_RESET}"
                )
        return _show_render(lines, color)


class Definition:
    """One exact recorded Node definition and its executions."""

    INPUT_NAMES = (
        "args",
        "kwargs",
        "p_args",
        "p_kwargs",
        "k_args",
        "k_kwargs",
    )

    def __init__(
        self,
        *,
        definition_id,
        content_id,
        name,
        sources,
        inputs,
        declared_deps,
        declared_targets,
        declared_effects,
        node_options,
        emitted,
        emitted_by_exec_id,
        origin,
        executions,
    ):
        self.definition_id = definition_id
        self.content_id = copy.deepcopy(content_id)
        self.name = name
        self.sources = tuple(copy.deepcopy(list(sources)))
        self.inputs = copy.deepcopy(dict(inputs))
        for input_name in self.INPUT_NAMES:
            setattr(self, input_name, self.inputs[input_name])
        self.declared_deps = copy.deepcopy(list(declared_deps or ()))
        self.declared_targets = copy.deepcopy(list(declared_targets or ()))
        self.declared_effects = copy.deepcopy(list(declared_effects or ()))
        self.node_options = copy.deepcopy(dict(node_options or {}))
        self.emitted = bool(emitted)
        self.emitted_by_exec_id = emitted_by_exec_id
        self.origin = origin
        self.executions = tuple(executions)

    def __len__(self):
        return len(self.executions)

    def __bool__(self):
        return True

    @staticmethod
    def _reference(reference):
        if isinstance(reference, DataRef):
            return DataRef.to_string(reference, prefix=False)
        return str(reference)

    @staticmethod
    def _execution(execution):
        occurrence = execution.producer_occurrence_id
        return {
            "seq_id": execution.seq_id,
            "exec_id": execution.exec_id,
            "actor_occurrence": (
                str(occurrence) if occurrence is not None else None
            ),
            "status": getattr(execution, "status", None),
        }

    def to_dict(self):
        return {
            "type": "definition",
            "definition_id": str(self.definition_id),
            "content_id": _query_json_value(self.content_id),
            "name": self.name,
            "sources": _query_json_value(self.sources),
            "declaration": {
                **{
                    name: _query_json_value(self.inputs[name])
                    for name in self.INPUT_NAMES
                },
                "dependencies": [
                    self._reference(reference)
                    for reference in self.declared_deps
                ],
                "targets": [
                    self._reference(reference)
                    for reference in self.declared_targets
                ],
                "effects": _query_json_value(self.declared_effects),
                "options": _query_json_value(self.node_options),
            },
            "emitted": self.emitted,
            "emitted_by_exec_id": self.emitted_by_exec_id,
            "origin": self.origin,
            "executions": [
                self._execution(execution)
                for execution in self.executions
            ],
        }

    def show(self, color=True, simple=False, colour=None):
        color, simple = _show_options(color, simple, colour)
        count = len(self)
        lines = show_header(
            "DEFINITION",
            f"{count} {'execution' if count == 1 else 'executions'}",
        )
        lines.append(
            f"{SHOW_DIM}Definition:{SHOW_RESET} "
            f"{SHOW_CYAN}{self.definition_id}{SHOW_RESET}"
        )
        lines.extend(show_section("IDENTITY"))
        lines.append(f"  {SHOW_DIM}name:{SHOW_RESET} {SHOW_WHITE}{self.name}{SHOW_RESET}")
        lines.append(
            f"  {SHOW_DIM}content id:{SHOW_RESET} "
            f"{SHOW_WHITE}{repr(self.content_id)}{SHOW_RESET}"
        )
        for source in self.sources:
            lines.append(
                f"  {SHOW_DIM}source:{SHOW_RESET} "
                f"{SHOW_WHITE}ocean {source['ocean_id']}, "
                f"runner {source['runner_id']}, "
                f"pipeline {source['pipeline_id']}{SHOW_RESET}"
            )

        lines.extend(show_section("DECLARED INPUTS"))
        for name in self.INPUT_NAMES:
            lines.append(
                f"  {SHOW_DIM}{name}:{SHOW_RESET} "
                f"{SHOW_WHITE}{repr(self.inputs[name])}{SHOW_RESET}"
            )

        lines.extend(show_section("DECLARATION"))
        dependencies = [self._reference(item) for item in self.declared_deps]
        targets = [self._reference(item) for item in self.declared_targets]
        lines.append(
            f"  {SHOW_DIM}dependencies:{SHOW_RESET} "
            f"{SHOW_WHITE}{dependencies or 'none'}{SHOW_RESET}"
        )
        lines.append(
            f"  {SHOW_DIM}targets:{SHOW_RESET} "
            f"{SHOW_WHITE}{targets or 'none'}{SHOW_RESET}"
        )
        lines.append(
            f"  {SHOW_DIM}effects:{SHOW_RESET} "
            f"{SHOW_WHITE}{self.declared_effects or 'none'}{SHOW_RESET}"
        )
        lines.append(
            f"  {SHOW_DIM}options:{SHOW_RESET} "
            f"{SHOW_WHITE}{self.node_options}{SHOW_RESET}"
        )

        lines.extend(show_section("EXECUTIONS"))
        if not self.executions:
            lines.append(f"  {SHOW_DIM}none{SHOW_RESET}")
        for execution in self.executions:
            occurrence = execution.producer_occurrence_id
            lines.append(
                f"  {SHOW_CYAN}{execution.seq_id}:{SHOW_RESET} "
                f"{SHOW_WHITE}{occurrence or execution.func_name}{SHOW_RESET} "
                f"{SHOW_DIM}({execution.status}){SHOW_RESET}"
            )
        if self.emitted:
            lines.append(
                f"  {SHOW_DIM}emitted by:{SHOW_RESET} "
                f"{SHOW_WHITE}{self.emitted_by_exec_id}{SHOW_RESET}"
            )
        return _show_render(lines, color)


class UsedAs:
    """Every recorded use of one exact State occurrence."""

    def __init__(
        self,
        reference,
        version,
        stored_snapshot,
        observations,
    ):
        self.reference = reference
        self.version = version
        self._stored_snapshot = stored_snapshot
        self._observations = tuple(observations)

    @property
    def state_id(self):
        return f"{self.reference}[v{self.version}]"

    @property
    def stored_hash(self):
        return self._stored_snapshot.value_hash

    @property
    def stored_type(self):
        return self._stored_snapshot.type_name

    @staticmethod
    def _input_name(read):
        associated = read.associated_input
        if isinstance(associated, (tuple, list)):
            return associated[0] if associated else None
        if isinstance(associated, dict):
            return associated.get("name")
        if isinstance(associated, str):
            return associated
        return getattr(associated, "name", None)

    def _entry(self, execution, read, snapshot, read_type):
        occurrence = execution.producer_occurrence_id
        return {
            "observation_id": getattr(read, "observation_id", None),
            "seq_id": execution.seq_id,
            "exec_id": execution.exec_id,
            "actor": execution.producer_id.to_dict(),
            "actor_occurrence": (
                repr(occurrence) if occurrence is not None else None
            ),
            "input": {
                "name": self._input_name(read),
                "index": read.input_index,
                "path": _query_json_value(read.input_path),
            },
            "read_type": read_type,
            "observed": {
                "hash": snapshot.value_hash,
                "type": snapshot.type_name,
                "differs_from_stored": (
                    snapshot.value_hash != self.stored_hash
                ),
            },
            "observed_mutable": bool(read.observed_mutable),
            "snapshot_lineage": _query_json_value(
                list(read.snapshot_lineage or ())
            ),
        }

    @property
    def uses(self):
        return tuple(
            self._entry(execution, read, snapshot, read_type)
            for execution, read, snapshot, read_type in self._observations
        )

    @property
    def by_seq_id(self):
        grouped = defaultdict(list)
        for use in self.uses:
            grouped[use["seq_id"]].append(use)
        return {
            seq_id: tuple(entries)
            for seq_id, entries in grouped.items()
        }

    @property
    def values(self):
        return tuple(
            snapshot.restore()
            for _, _, snapshot, _ in self._observations
        )

    def __len__(self):
        return len(self._observations)

    def __bool__(self):
        return bool(self._observations)

    def to_dict(self):
        return {
            "state": {
                "reference": self.reference,
                "version": self.version,
                "id": self.state_id,
            },
            "stored": {
                "hash": self.stored_hash,
                "type": self.stored_type,
            },
            "uses": list(self.uses),
        }

    def show(self, color=True, simple=False, colour=None):
        color, simple = _show_options(color, simple, colour)
        count = len(self)
        lines = show_header(
            "USED AS",
            f"{count} {'use' if count == 1 else 'uses'} found",
            f"State: {self.state_id}",
        )
        lines.extend(show_section("STORED"))
        lines.append(
            f"  {SHOW_DIM}type:{SHOW_RESET} "
            f"{SHOW_CYAN}{self.stored_type}{SHOW_RESET}"
        )
        lines.append(
            f"  {SHOW_DIM}hash:{SHOW_RESET} "
            f"{SHOW_WHITE}{self.stored_hash}{SHOW_RESET}"
        )
        lines.extend(show_section("USES"))
        if not self._observations:
            lines.append(f"  {SHOW_DIM}none{SHOW_RESET}")
        for use in self.uses:
            changed = use["observed"]["differs_from_stored"]
            marker = " *" if changed else ""
            input_entry = use["input"]
            input_name = input_entry["name"] or "unknown"
            input_display = (
                f"{input_name}[{input_entry['index']}]"
                if input_entry["index"] is not None
                else str(input_name)
            )
            lines.append(
                f"  {SHOW_CYAN}EXEC_RECORD[{use['seq_id']}]{SHOW_RESET} "
                f"{SHOW_WHITE}{use['actor']['name']}{SHOW_RESET}"
            )
            if use["observation_id"] is not None:
                lines.append(
                    f"    {SHOW_DIM}observation:{SHOW_RESET} "
                    f"{SHOW_WHITE}{use['observation_id']}{SHOW_RESET}"
                )
            lines.append(
                f"    {SHOW_DIM}input:{SHOW_RESET} "
                f"{SHOW_WHITE}{input_display}{SHOW_RESET} "
                f"({use['read_type']})"
            )
            lines.append(
                f"    {SHOW_DIM}observed:{SHOW_RESET} "
                f"{SHOW_CYAN}{use['observed']['type']}{marker}{SHOW_RESET}"
            )
            lines.append(
                f"    {SHOW_DIM}hash:{SHOW_RESET} "
                f"{SHOW_WHITE}{use['observed']['hash']}{SHOW_RESET}"
            )
            changed_text = "yes" if changed else "no"
            changed_color = SHOW_YELLOW if changed else SHOW_GREEN
            lines.append(
                f"    {SHOW_DIM}changed from stored:{SHOW_RESET} "
                f"{changed_color}{changed_text}{SHOW_RESET}"
            )
            lineage = use["snapshot_lineage"]
            if lineage:
                hashes = [
                    entry.get("value_hash")
                    for entry in lineage
                    if isinstance(entry, dict) and entry.get("value_hash")
                ]
                if hashes:
                    lines.append(
                        f"    {SHOW_DIM}lineage:{SHOW_RESET} "
                        f"{SHOW_WHITE}{' -> '.join(hashes)}{SHOW_RESET}"
                    )
        return _show_render(lines, color=color)


class ExecValues:
    def __init__(self, execution, view="all"):
        self.execution = execution
        self.view = view
        self.inputs = {
            name: input_record.resolved
            for name, input_record in execution.inputs.items()
        }
        self.raw_inputs = {
            name: input_record.raw
            for name, input_record in execution.inputs.items()
        }
        self.args = self.inputs["args"]
        self.kwargs = self.inputs["kwargs"]
        self.p_args = self.inputs["p_args"]
        self.p_kwargs = self.inputs["p_kwargs"]
        self.k_args = self.inputs["k_args"]
        self.k_kwargs = self.inputs["k_kwargs"]
        self.output = execution.output_value

    @staticmethod
    def _is_empty(value):
        return isinstance(value, (tuple, dict, list)) and len(value) == 0

    @property
    def input_types(self):
        return {
            name: type(value).__name__
            for name, value in self.inputs.items()
            if not self._is_empty(value)
        }

    @property
    def output_type(self):
        snapshot = self.execution._tide_snapshot("output_value")
        if snapshot is not None:
            if snapshot.type_name in {"TIDEUnknownType", "TIDEUnknownValue"}:
                return f"opaque<{snapshot.type_name}>"
            return snapshot.type_name
        return type(self.output).__name__

    def to_dict(self, raw=False):
        if not isinstance(raw, bool):
            raise TypeError("to_dict raw must be a bool")

        data = {
            "exec_id": self.execution.exec_id,
            "seq_id": self.execution.seq_id,
        }
        if self.view in {"all", "inputs"}:
            inputs = self.raw_inputs if raw else self.inputs
            data["inputs"] = _query_json_value(inputs)
        if self.view in {"all", "output"}:
            data["output"] = _query_json_value(self.output)
        return data

    def show(self, color=True, simple=False, colour=None, raw=False):
        color, simple = _show_options(color, simple, colour)
        if not isinstance(raw, bool):
            raise TypeError("show raw must be a bool")

        view  = "INPUTS + OUTPUT" if self.view == "all" else self.view.upper()
        title = f"{view} OF EXEC_RECORD[{self.execution.seq_id}] {self.execution.func_name}"
        if self.view == "inputs":
            title += " | RESOLVED" if raw is False else " | RAW"
        lines = show_header(title, identifier=self.execution.exec_id)

        if self.view in {"all", "inputs"}:
            inputs = self.raw_inputs if raw else self.inputs
            input_types = {
                name: type(value).__name__
                for name, value in inputs.items()
                if not self._is_empty(value)
            }
            lines.extend(show_section("INPUTS"))
            if not input_types:
                lines.append(f"  {SHOW_DIM}none{SHOW_RESET}")
            for name in input_types:
                value = inputs[name]
                lines.append(f"  {SHOW_DIM}{name}:{SHOW_RESET} {SHOW_CYAN}{type(value).__name__}{SHOW_RESET}")
                lines.append(f"    {SHOW_WHITE}{repr(value)}{SHOW_RESET}")

        if self.view in {"all", "output"}:
            lines.extend(show_section("OUTPUT"))
            lines.append(f"  {SHOW_DIM}output:{SHOW_RESET} {SHOW_CYAN}{self.output_type}{SHOW_RESET}")
            lines.append(f"    {SHOW_WHITE}{repr(self.output)}{SHOW_RESET}")

        return _show_render(lines, color=color)


def _query_json_value(value):
    """Return a JSON-compatible public query payload value."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value

    if isinstance(value, dict):
        return {
            str(key): _query_json_value(item)
            for key, item in value.items()
        }

    if isinstance(value, (list, tuple)):
        return [_query_json_value(item) for item in value]

    if isinstance(value, set):
        return [
            _query_json_value(item)
            for item in sorted(value, key=repr)
        ]

    if hasattr(value, "to_dict") and callable(value.to_dict):
        return _query_json_value(value.to_dict())

    return repr(value)


def _diff_payload(value):
    evidence_projection = getattr(value, "_tide_diff_evidence", None)
    if callable(evidence_projection):
        return _diff_payload(evidence_projection())

    if isinstance(value, dict):
        return {
            key: _diff_payload(item)
            for key, item in value.items()
        }

    if isinstance(value, list):
        return [_diff_payload(item) for item in value]

    if isinstance(value, tuple):
        return tuple(_diff_payload(item) for item in value)

    if isinstance(value, set):
        return {
            _diff_payload(item)
            for item in value
        }

    if isinstance(value, frozenset):
        return frozenset(
            _diff_payload(item)
            for item in value
        )

    if hasattr(value, "to_dict") and callable(value.to_dict):
        return _diff_payload(value.to_dict())

    return value


def _diff_values(before, after, path=""):
    changes = {
        "changed": {},
        "added": {},
        "removed": {},
    }

    if isinstance(before, dict) and isinstance(after, dict):
        before_keys = set(before)
        after_keys = set(after)

        for key in sorted(before_keys - after_keys, key=repr):
            key_path = f"{path}.{key}" if path else str(key)
            changes["removed"][key_path] = before[key]

        for key in sorted(after_keys - before_keys, key=repr):
            key_path = f"{path}.{key}" if path else str(key)
            changes["added"][key_path] = after[key]

        for key in sorted(before_keys & after_keys, key=repr):
            key_path = f"{path}.{key}" if path else str(key)
            nested = _diff_values(before[key], after[key], key_path)
            for section, values in nested.items():
                changes[section].update(values)

        return changes

    if isinstance(before, (list, tuple)) and isinstance(after, (list, tuple)):
        shared = min(len(before), len(after))
        for index in range(shared):
            nested = _diff_values(
                before[index],
                after[index],
                f"{path}[{index}]" if path else f"[{index}]",
            )
            for section, values in nested.items():
                changes[section].update(values)

        for index in range(shared, len(before)):
            key_path = f"{path}[{index}]" if path else f"[{index}]"
            changes["removed"][key_path] = before[index]

        for index in range(shared, len(after)):
            key_path = f"{path}[{index}]" if path else f"[{index}]"
            changes["added"][key_path] = after[index]

        if type(before) is not type(after):
            changes["changed"][path or "value"] = {
                "before": before,
                "after": after,
            }

        return changes

    if isinstance(before, (set, frozenset)) and isinstance(after, (set, frozenset)):
        removed = [item for item in before if item not in after]
        added = [item for item in after if item not in before]
        key_path = path or "value"
        if removed:
            changes["removed"][key_path] = sorted(removed, key=repr)
        if added:
            changes["added"][key_path] = sorted(added, key=repr)
        return changes

    if not safe_equal(before, after):
        changes["changed"][path or "value"] = {
            "before": before,
            "after": after,
        }

    return changes


def _build_compared(subject, other):
    before_type = type(subject).__name__
    after_type = type(other).__name__
    before = _diff_payload(subject)
    after = _diff_payload(other)
    changes = _diff_values(before, after)
    return Compared(
        before_type=before_type,
        after_type=after_type,
        before=before,
        after=after,
        **changes,
    )


def _diff_inspected_target(value):
    """Return the underlying TIDE object represented by an inspection view."""
    if isinstance(value, Investigate):
        value = value.inspected

    if isinstance(value, Inspected):
        if value.grouped:
            return [
                getattr(item, "object", item)
                for item in value.object
            ]

        inspected_object = value.object
        return getattr(inspected_object, "object", inspected_object)

    return value


def _build_object_compared(subject, other, diff_object=True):
    """Build a diff for inspection views or their underlying objects.

    ``diff_object=True`` is the ergonomic default: an ``Inspected`` or
    ``Investigate`` view is compared by the actual object it represents.
    Passing ``False`` preserves the full view-level comparison.
    """
    if diff_object:
        subject = _diff_inspected_target(subject)
        other = _diff_inspected_target(other)

    return _build_compared(subject, other)


class Stream:
    def __init__(
        self,
        source,
        stream,
        paths,
        direction,
        *,
        inspect=None,
        truncated=False,
        max_paths=None,
    ):
        # A directional result always has an explicit direction.
        if direction not in {"upstream", "downstream"}:
            raise ValueError(
                "direction must be 'upstream' or 'downstream'"
            )

        # Retain the source and the two frozen traversal projections.
        self.source = source
        self.stream = {
            root_exec_id: {
                layer: list(handoffs)
                for layer, handoffs in layers.items()
            }
            for root_exec_id, layers in stream.items()
        }
        self.paths = {
            root_exec_id: [
                {
                    "executions": list(path["executions"]),
                    "handoffs": [
                        list(handoffs)
                        for handoffs in path["handoffs"]
                    ],
                    "endpoint_evidence": list(
                        path.get("endpoint_evidence", ())
                    ),
                }
                for path in root_paths
            ]
            for root_exec_id, root_paths in paths.items()
        }
        self.direction = direction
        self.inspect = inspect
        self.truncated = bool(truncated)
        self.max_paths = max_paths

    def exists(self):
        return bool(self.paths)

    def count(self):
        return sum(
            len(root_paths)
            for root_paths in self.paths.values()
        )

    def diff(self, other):
        return _build_compared(self, other)

    def __len__(self):
        return self.count()

    def __bool__(self):
        return self.exists()

    @property
    def trace(self):
        return getattr(self.inspect, "__self__", None)

    def _state_view(self, ref, version):
        evidence = _state_query_evidence(
            self.trace,
            ref,
            version,
        )
        return {
            "ref": ref,
            "version": version,
            "value_type": evidence["recorded_as"]["type"],
            "divergent": evidence["divergent"],
        }

    @staticmethod
    def _state_identity_from_evidence(evidence):
        if isinstance(evidence, ReadEdge):
            return (
                evidence.read.unscoped_reference,
                evidence.read.resolved_version,
            )

        if isinstance(evidence, WriteRecord):
            return (
                evidence.stored_target,
                evidence.stored_version,
            )

        if isinstance(evidence, DependencyUseRecord):
            return (
                evidence.unscoped_reference,
                evidence.resolved_version,
            )

        if isinstance(evidence, EffectEdge):
            record = evidence.effect_record
        elif isinstance(evidence, EffectRecord):
            record = evidence
        else:
            return None

        if (
            record.effect in {Effects.READ, Effects.WRITE, Effects.DELETE}
            and isinstance(record.targets, tuple)
            and len(record.targets) == 2
        ):
            return record.targets

        return None

    def _evidence_state_view(self, evidence):
        state = self._state_identity_from_evidence(evidence)
        if state is None:
            return None
        return self._state_view(*state)

    def _all_traversal_evidence(self):
        for root_paths in self.paths.values():
            for path in root_paths:
                for handoffs in path["handoffs"]:
                    yield from handoffs
                yield from path.get("endpoint_evidence", ())

        for layers in getattr(self, "stream", {}).values():
            for handoffs in layers.values():
                yield from handoffs

    def _has_divergence(self):
        return any(
            state is not None and state["divergent"]
            for state in (
                self._evidence_state_view(evidence)
                for evidence in self._all_traversal_evidence()
            )
        )

    def _state_relation_text(
        self,
        ref,
        version,
        *,
        evidence=None,
        colour=SHOW_GREEN,
    ):
        state = self._state_text(
            ref,
            version,
            evidence,
        )
        view = self._state_view(ref, version)
        marker = (
            f"{SHOW_RED}!{SHOW_RESET}"
            if view["divergent"]
            else ""
        )
        value_type = view["value_type"] or "unknown"
        return (
            f"{marker}{colour}{state}{SHOW_RESET} "
            f"{SHOW_DIM}as{SHOW_RESET} "
            f"{SHOW_WHITE}{value_type}{SHOW_RESET}"
        )

    def _effect_line(self, record):
        effect = str(record.effect).upper()
        colour = (
            SHOW_RED
            if record.effect == Effects.DELETE
            else SHOW_GREEN
        )

        if (
            record.effect in {Effects.READ, Effects.WRITE, Effects.DELETE}
            and isinstance(record.targets, tuple)
            and len(record.targets) == 2
        ):
            target = self._state_relation_text(
                *record.targets,
                evidence=record,
                colour=colour,
            )
        else:
            target = (
                f"{colour}"
                f"{self._effect_target_text(record)}"
                f"{SHOW_RESET}"
            )

        return (
            f"{colour}{effect}{SHOW_RESET} "
            f"{SHOW_DIM}at{SHOW_RESET} {target}"
        )


    def _execution_display(self, exec_id, actor=None):
        # Show a stable occurrence label without exposing an opaque ID.
        if exec_id is None:
            actor_name = getattr(actor, "source_actor", actor)
            return str(actor_name)

        if self.inspect is not None:
            try:
                return repr(self.inspect(exec_id, "exec_record").object)
            except (KeyError, TypeError, ValueError):
                pass

        actor_name = getattr(actor, "source_actor", actor)
        if actor_name is not None:
            return str(actor_name)
        return shorten_identity(exec_id)

    @staticmethod
    def _input_position(read):
        # Preserve an input's name and iterable position as separate data.
        associated_input = read.associated_input
        if isinstance(associated_input, (tuple, list)):
            name = associated_input[0] if associated_input else None
        elif isinstance(associated_input, dict):
            name = associated_input.get("name")
        elif isinstance(associated_input, str):
            name = associated_input
        else:
            name = getattr(associated_input, "name", None)

        if name is None:
            return None
        return {
            "name": name,
            "index": read.input_index,
        }


    def _evidence_dict(self, evidence):
        # Project each supported evidence object without changing its meaning.
        if isinstance(evidence, ReadEdge):
            return {
                "type": "read_edge",
                "producer_exec_id": evidence.producer_exec_id,
                "consumer_exec_id": evidence.consumer_exec_id,
                "state": {
                    **self._state_view(
                        evidence.read.unscoped_reference,
                        evidence.read.resolved_version,
                    ),
                },
                "input": self._input_position(evidence.read),
            }

        if isinstance(evidence, EffectEdge):
            return {
                "type": "effect_edge",
                "producer_exec_id": evidence.producer_exec_id,
                "consumer_exec_id": evidence.consumer_exec_id,
                "event_id": evidence.effect_record.event_id,
                "effect": evidence.effect_record.effect,
                "target": _query_json_value(
                    evidence.effect_record.targets
                ),
                "affected_inputs": _query_json_value(
                    evidence.affected_record.affected_inputs
                ),
                "state": self._evidence_state_view(evidence),
            }

        if isinstance(evidence, EmissionEdge):
            emitted = evidence.emitted_node
            return {
                "type": "emission_edge",
                "producer_exec_id": evidence.producer_exec_id,
                "consumer_exec_id": evidence.consumer_exec_id,
                "emitted_node": {
                    "runner_id": emitted.runner_id,
                    "node_id": emitted.node_id,
                    "actor": _query_json_value(emitted.producer_id),
                    "origin": emitted.origin,
                },
            }

        if isinstance(evidence, WriteRecord):
            return {
                "type": "write",
                "state": {
                    **self._state_view(
                        evidence.stored_target,
                        evidence.stored_version,
                    ),
                },
                "cause": {
                    "event_id": evidence.cause.event_id,
                    "description": evidence.cause.description,
                },
            }

        if isinstance(evidence, DependencyUseRecord):
            return {
                "type": "dependency_use",
                "producer_exec_id": evidence.producer_exec_id,
                "state": {
                    **self._state_view(
                        evidence.unscoped_reference,
                        evidence.resolved_version,
                    ),
                },
                "input": self._input_position(evidence),
            }

        if isinstance(evidence, EffectRecord):
            return {
                "type": "effect",
                "event_id": evidence.event_id,
                "effect": evidence.effect,
                "target": _query_json_value(evidence.targets),
                "origin": evidence.origin,
                "immediate": evidence.immediate,
                "target_existed": evidence.target_existed,
                "state": self._evidence_state_view(evidence),
            }

        raise TypeError(
            f"unsupported path evidence: {type(evidence).__name__}"
        )


    @staticmethod
    def _endpoint_dict(endpoint):
        # Retain only the query identity represented by this result.
        return {
            "type": endpoint.type,
            "id": _query_json_value(endpoint.id),
            "version": endpoint.version,
            "grouped": endpoint.grouped,
            "exists": endpoint.exists,
        }



    @staticmethod
    def _state_text(ref, version, evidence=None):
        # Remove only the private carrier scope used for imported model States.
        if isinstance(ref, str) and ref.startswith((
            "__model__:",
            "__model_int__:",
        )):
            ref = ref.split(":", 1)[1]
        return _state_occurrence_text(
            ref,
            version,
            observed_mutable=bool(
                getattr(evidence, "observed_mutable", False)
            ),
        )


    @classmethod
    def _effect_target_text(cls, record):
        # Versioned State Effects use State notation; other targets remain literal.
        if (
            record.effect in {Effects.READ, Effects.WRITE, Effects.DELETE}
            and isinstance(record.targets, tuple)
            and len(record.targets) == 2
        ):
            return cls._state_text(*record.targets)
        return str(_query_json_value(record.targets))


    def _path_evidence_line(
        self,
        evidence,
        show_input_positions,
    ):
        # A read edge carries one exact State between two executions.
        if isinstance(evidence, ReadEdge):
            state = self._state_text(
                evidence.read.unscoped_reference,
                evidence.read.resolved_version,
                evidence.read,
            )
            line = self._state_relation_text(
                evidence.read.unscoped_reference,
                evidence.read.resolved_version,
                evidence=evidence.read,
            )
            position = self._input_position(evidence.read)
            if show_input_positions and position is not None:
                input_name = position["name"]
                if position["index"] is not None:
                    input_name = f"{input_name}[{position['index']}]"
                line += (
                    f" {SHOW_DIM}via{SHOW_RESET} "
                    f"{SHOW_WHITE}{input_name}{SHOW_RESET}"
                )
            return line

        # An affected Effect edge carries its recorded Effect and target.
        if isinstance(evidence, EffectEdge):
            return self._effect_line(evidence.effect_record)

        # An emission edge names the exact emitted Actor and optional execution.
        if isinstance(evidence, EmissionEdge):
            actor = evidence.emitted_node.producer_id
            actor_name = getattr(actor, "source_actor", actor)
            line = (
                f"{SHOW_CYAN}EMIT{SHOW_RESET} "
                f"{SHOW_DIM}->{SHOW_RESET} "
                f"{SHOW_CYAN}{actor_name}{SHOW_RESET}"
            )
            return line

        # A direct write ends at the exact State incidence.
        if isinstance(evidence, WriteRecord):
            state = self._state_text(
                evidence.stored_target,
                evidence.stored_version,
                evidence,
            )
            return (
                f"{SHOW_DIM}wrote{SHOW_RESET} "
                + self._state_relation_text(
                    evidence.stored_target,
                    evidence.stored_version,
                    evidence=evidence,
                )
            )

        # A direct dependency use identifies the exact input State incidence.
        if isinstance(evidence, DependencyUseRecord):
            state = self._state_text(
                evidence.unscoped_reference,
                evidence.resolved_version,
                evidence,
            )
            line = (
                f"{SHOW_DIM}read{SHOW_RESET} "
                + self._state_relation_text(
                    evidence.unscoped_reference,
                    evidence.resolved_version,
                    evidence=evidence,
                )
            )
            position = self._input_position(evidence)
            if show_input_positions and position is not None:
                input_name = position["name"]
                if position["index"] is not None:
                    input_name = f"{input_name}[{position['index']}]"
                line += (
                    f" {SHOW_DIM}via{SHOW_RESET} "
                    f"{SHOW_WHITE}{input_name}{SHOW_RESET}"
                )
            return line

        # A direct Effect ends at that exact recorded Effect occurrence.
        if isinstance(evidence, EffectRecord):
            return self._effect_line(evidence)

        raise TypeError(
            f"unsupported path evidence: {type(evidence).__name__}"
        )



    def to_dict(self):
        return {
            "source": self._endpoint_dict(self.source),
            "direction": self.direction,
            "exists": self.exists(),
            "path_count": self.count(),
            "truncated": self.truncated,
            "max_paths": self.max_paths,
            "divergent": self._has_divergence(),
            "paths": [
                {
                    "executions": [
                        _query_json_value(exec_id)
                        for exec_id in path["executions"]
                    ],
                    "handoffs": [
                        [
                            self._evidence_dict(handoff)
                            for handoff in handoffs
                        ]
                        for handoffs in path["handoffs"]
                    ],
                    "endpoint_evidence": [
                        self._evidence_dict(evidence)
                        for evidence in path["endpoint_evidence"]
                    ],
                }
                for root_paths in self.paths.values()
                for path in root_paths
            ],
            "stream": [
                {
                    "root_exec_id": root_exec_id,
                    "layers": [
                        {
                            "layer": layer,
                            "handoffs": [
                                self._evidence_dict(handoff)
                                for handoff in handoffs
                            ],
                        }
                        for layer, handoffs in layers.items()
                    ],
                }
                for root_exec_id, layers in self.stream.items()
            ],
        }

    def _stream_edge_line(self, edge):
        # Render the exact State or Effect carried by this execution edge.
        if isinstance(edge, ReadEdge):
            state = self._state_text(
                edge.read.unscoped_reference,
                edge.read.resolved_version,
                edge.read,
            )
            return self._state_relation_text(
                edge.read.unscoped_reference,
                edge.read.resolved_version,
                evidence=edge.read,
            )

        if isinstance(edge, EmissionEdge):
            return self._path_evidence_line(
                edge,
                show_input_positions=False,
            )

        return self._effect_line(edge.effect_record)


    def _lines_from_complete_paths(self):
        # Render each complete handoff path in occurrence order.
        lines = []
        path_number = 0

        for root_paths in self.paths.values():
            for path in root_paths:
                path_number += 1
                if path_number > 1:
                    lines.append("")

                lines.append(
                    f"{SHOW_DIM}PATH {path_number}{SHOW_RESET}"
                )
                lines.append(
                    f"{SHOW_CYAN}"
                    f"{self._execution_display(path['executions'][0])}"
                    f"{SHOW_RESET}"
                )

                # Show every handoff in a step before its one next execution.
                for index, handoffs in enumerate(path["handoffs"]):
                    indent = "    " * index
                    for handoff in handoffs:
                        lines.append(
                            f"{indent}{SHOW_DIM}{SHOW_TREE_BRANCH}"
                            f"{SHOW_RESET} "
                            f"{self._stream_edge_line(handoff)}"
                        )

                    next_exec_id = path["executions"][index + 1]
                    lines.append(
                        f"{indent}{SHOW_DIM}{SHOW_TREE_LAST}"
                        f"{SHOW_RESET} {SHOW_CYAN}"
                        f"{self._execution_display(next_exec_id)}"
                        f"{SHOW_RESET}"
                    )

        return lines


    def show(
        self,
        color=True,
        simple=False,
        colour=None,
    ):
        color, simple = _show_options(color, simple, colour)
        data = self.to_dict()
        query = show_query_display(
            self.source.type,
            self.source.id,
            None if self.source.grouped else str(self.source.object),
            self.source.grouped,
            self.source.version,
        )
        path_count = self.count()
        origins = DedupedList()
        if (
            not path_count
            and self.direction == "upstream"
            and self.source.type in {"reference", "value"}
        ):
            origins.extend(
                exec_id
                for exec_ids in self.source.upstream.values()
                for exec_id in exec_ids
            )

        if path_count:
            summary = f"{path_count} {'path' if path_count == 1 else 'paths'} found"
        elif origins:
            summary = (
                f"{len(origins)} "
                f"{'producer' if len(origins) == 1 else 'producers'} found | "
                "no upstream handoffs"
            )
        else:
            summary = "no paths found"
        if self.truncated:
            summary += f" | truncated at {self.max_paths} paths"
        if data["divergent"]:
            summary += f" | {SHOW_RED}DIVERGENT{SHOW_RESET}"

        lines = show_header(
            f"{self.direction.upper()} {query}",
            summary,
            show_identity(self.source.id, self.source.grouped),
        )
        lines.extend(show_section("ORIGIN" if origins else "PATHS"))

        if origins:
            lines.extend(
                f"{SHOW_CYAN}{self._execution_display(exec_id)}{SHOW_RESET}"
                for exec_id in origins
            )
        elif self.paths:
            lines.extend(self._lines_from_complete_paths())
        else:
            lines.append(
                f"{SHOW_YELLOW}{SHOW_TREE_LAST} No paths found{SHOW_RESET}"
            )

        return _show_render(lines, color)

class Upstream(Stream):
    def __init__(
        self,
        source,
        stream,
        paths,
        *,
        inspect=None,
        truncated=False,
        max_paths=None,
    ):
        super().__init__(
            source,
            stream,
            paths,
            "upstream",
            inspect=inspect,
            truncated=truncated,
            max_paths=max_paths,
        )


class Downstream(Stream):
    def __init__(
        self,
        source,
        stream,
        paths,
        *,
        inspect=None,
        truncated=False,
        max_paths=None,
    ):
        super().__init__(
            source,
            stream,
            paths,
            "downstream",
            inspect=inspect,
            truncated=truncated,
            max_paths=max_paths,
        )

class Pathed(Stream):
    def __init__(
        self,
        source,
        target,
        paths,
        direction,
        *,
        inspect=None,
    ):
        # A path result always has an explicit direction.
        if direction not in {"upstream", "downstream"}:
            raise ValueError(
                "direction must be 'upstream' or 'downstream'"
            )

        # Retain the selected endpoints and complete paths unchanged.
        self.source = source
        self.target = target
        self.paths = {
            root_exec_id: [
                {
                    "executions": list(path["executions"]),
                    "handoffs": [
                        list(handoffs)
                        for handoffs in path["handoffs"]
                    ],
                    "endpoint_evidence": list(
                        path["endpoint_evidence"]
                    ),
                }
                for path in root_paths
            ]
            for root_exec_id, root_paths in paths.items()
        }
        self.direction = direction
        self.inspect = inspect

    def exists(self):
        return bool(self.paths)

    def count(self):
        return sum(
            len(root_paths)
            for root_paths in self.paths.values()
        )

    def get_path(self, number=0):
        # Return the requested complete path in public occurrence order.
        if number < 0:
            return None
        for index, path in enumerate(
            path
            for root_paths in self.paths.values()
            for path in root_paths
        ):
            if index == number:
                return path
        return None

    def __len__(self):
        return self.count()

    def __bool__(self):
        return self.exists()

    def __repr__(self):
        return (
            f"Pathed[{self.source} -> {self.target} | "
            f"{self.direction}, {self.count()} paths]"
        )

    def to_dict(self):
        return {
            "source": self._endpoint_dict(self.source),
            "target": self._endpoint_dict(self.target),
            "direction": self.direction,
            "exists": self.exists(),
            "path_count": self.count(),
            "divergent": self._has_divergence(),
            "paths": [
                {
                    "executions": [
                        _query_json_value(exec_id)
                        for exec_id in path["executions"]
                    ],
                    "handoffs": [
                        [
                            self._evidence_dict(handoff)
                            for handoff in handoffs
                        ]
                        for handoffs in path["handoffs"]
                    ],
                    "endpoint_evidence": [
                        self._evidence_dict(evidence)
                        for evidence in path["endpoint_evidence"]
                    ],
                }
                for root_paths in self.paths.values()
                for path in root_paths
            ],
        }

    def _lines_from_paths(self, show_input_positions):
        # Render each complete evidence path in occurrence order.
        lines = []
        path_number = 0

        for root_exec_id, root_paths in self.paths.items():
            for path in root_paths:
                path_number += 1
                if path_number > 1:
                    lines.append("")

                lines.append(
                    f"{SHOW_DIM}PATH {path_number}{SHOW_RESET}"
                )
                lines.append(
                    f"{SHOW_CYAN}"
                    f"{self._execution_display(path['executions'][0])}"
                    f"{SHOW_RESET}"
                )

                # Show every handoff in a step before its one next execution.
                for index, handoffs in enumerate(path["handoffs"]):
                    indent = "    " * index
                    for handoff in handoffs:
                        line = self._path_evidence_line(
                            handoff,
                            show_input_positions,
                        )
                        lines.append(
                            f"{indent}{SHOW_DIM}{SHOW_TREE_BRANCH}"
                            f"{SHOW_RESET} {line}"
                        )

                    next_exec_id = path["executions"][index + 1]
                    lines.append(
                        f"{indent}{SHOW_DIM}{SHOW_TREE_LAST}"
                        f"{SHOW_RESET} {SHOW_CYAN}"
                        f"{self._execution_display(next_exec_id)}"
                        f"{SHOW_RESET}"
                    )

                # Show only the existing records which prove a terminal endpoint.
                evidence_indent = "    " * len(path["handoffs"])
                endpoint_evidence = path["endpoint_evidence"]
                for index, evidence in enumerate(endpoint_evidence):
                    connector = (
                        SHOW_TREE_LAST
                        if index == len(endpoint_evidence) - 1
                        else SHOW_TREE_BRANCH
                    )
                    line = self._path_evidence_line(
                        evidence,
                        show_input_positions,
                    )
                    lines.append(
                        f"{evidence_indent}{SHOW_DIM}{connector}"
                        f"{SHOW_RESET} {line}"
                    )

        return lines


    def show(
        self,
        full_ids=True,
        show_input_positions=False,
        color=True,
        simple=False,
        colour=None,
    ):
        color, simple = _show_options(color, simple, colour)
        data = self.to_dict()
        arrow = "->" if self.direction == "downstream" else "<-"
        source = show_query_display(
            self.source.type,
            self.source.id,
            None if self.source.grouped else str(self.source.object),
            self.source.grouped,
            self.source.version,
        )
        target = show_query_display(
            self.target.type,
            self.target.id,
            None if self.target.grouped else str(self.target.object),
            self.target.grouped,
            self.target.version,
        )

        path_count = self.count()
        summary = (
            f"{path_count} {'path' if path_count == 1 else 'paths'} found"
            if path_count
            else "no paths found"
        )
        if data["divergent"]:
            summary += f" | {SHOW_RED}DIVERGENT{SHOW_RESET}"

        identifier = (
            f"{show_identity(display_identity(self.source.id, full_ids=full_ids), self.source.grouped)} "
            f"{arrow} "
            f"{show_identity(display_identity(self.target.id, full_ids=full_ids), self.target.grouped)}"
        )
        lines = show_header(
            f"PATH {source} {arrow} {target}",
            summary,
            identifier,
        )
        lines.extend(show_section("PATHS"))

        if self.paths:
            lines.extend(self._lines_from_paths(show_input_positions))
        else:
            lines.append(
                f"{SHOW_YELLOW}{SHOW_TREE_LAST} No paths found{SHOW_RESET}"
            )

        return _show_render(lines, color)

class Nearby:
    def __init__(self, subject, actors=None, refs=None, execs=None):
        self.subject = subject
        self.actors = actors if actors is not None else []
        self.refs = refs if refs is not None else []
        self.execs = execs if execs is not None else []


class Searched:
    def __init__(self, subject, execs, actors, refs, effects, values, nearby):
        self.subject = subject
        self.execs = execs
        self.actors = actors
        self.refs = refs
        self.effects = effects
        self.values = values
        self.nearby = nearby

    @staticmethod
    def _canonical_kind(kind):
        match kind:
            case "actor" | "actors":
                return "actor"
            case "ref" | "refs" | "reference":
                return "reference"
            case "exec" | "execs" | "exec_record" | "seq_id":
                return "exec"
            case "value" | "values" | "hash":
                return "value"
            case "effect" | "effects":
                return "effect"
            case _:
                raise ValueError(f"Unknown search result type: {kind}")

    def get_type(self, kind):
        match self._canonical_kind(kind):
            case "actor":
                return {"exact": self.actors, "nearby": self.nearby.actors}
            case "reference":
                return {"exact": self.refs, "nearby": self.nearby.refs}
            case "exec":
                return {"exact": self.execs, "nearby": self.nearby.execs}
            case "value":
                return {"exact": self.values, "nearby": []}
            case "effect":
                return {"exact": self.effects, "nearby": []}

    def get_exact(self, kind):
        return list(self.get_type(kind)["exact"])

    def get_nearby(self, kind):
        return list(self.get_type(kind)["nearby"])

    def exists(self, kind):
        matches = self.get_type(kind)
        return bool(matches["exact"]), bool(matches["nearby"])

    def first(self, kind=None):
        if kind is not None:
            matches = self.get_type(kind)
            available = matches["exact"] or matches["nearby"]
            return available[0] if available else None

        for match_kind in ("actor", "reference", "exec", "value", "effect"):
            exact = self.get_type(match_kind)["exact"]
            if exact:
                return exact[0]

        for match_kind in ("actor", "reference", "exec"):
            nearby = self.get_type(match_kind)["nearby"]
            if nearby:
                return nearby[0]

        return None

    def any(self):
        return self.first() is not None

    def get(self, kind, scope="exact"):
        if scope == "exact":
            return self.get_exact(kind)
        if scope == "nearby":
            return self.get_nearby(kind)

        raise ValueError(f"Unknown search scope: {scope}")

    def to_dict(self):
        def serialise(matches):
            return [_query_json_value(match.to_dict()) for match in matches]

        return {
            "actors": {
                "exact": serialise(self.actors),
                "nearby": serialise(self.nearby.actors),
            },
            "refs": {
                "exact": serialise(self.refs),
                "nearby": serialise(self.nearby.refs),
            },
            "execs": {
                "exact": serialise(self.execs),
                "nearby": serialise(self.nearby.execs),
            },
            "effects": {"exact": serialise(self.effects)},
            "values": {"exact": serialise(self.values)},
        }

    def diff(self, other):
        return _build_compared(self, other)

    def show(
        self,
        kind="all",
        exact=True,
        nearby=True,
        color=True,
        simple=False,
        colour=None,
    ):
        color, simple = _show_options(color, simple, colour)
        kinds = self._show_kinds(kind)

        if not isinstance(exact, bool) or not isinstance(nearby, bool):
            raise TypeError("exact and nearby must be booleans")

        exact_matches = [
            (match_kind, match)
            for match_kind in kinds
            for match in self.get_exact(match_kind)
        ] if exact else []
        nearby_matches = [
            (match_kind, match)
            for match_kind in kinds
            for match in self.get_nearby(match_kind)
        ] if nearby else []
        total = len(exact_matches) + len(nearby_matches)
        match_word = "match" if total == 1 else "matches"
        lines = show_header(
            "FIND",
            f"{total} {match_word} found",
            f"Subject: {self.subject}",
        )

        if exact:
            lines.extend(show_section("EXACT"))
            lines.extend(
                self._show_match_lines(exact_matches)
                or [f"{SHOW_DIM}none{SHOW_RESET}"]
            )

        if nearby:
            lines.extend(show_section("NEARBY"))
            lines.extend(
                self._show_match_lines(nearby_matches)
                or [f"{SHOW_DIM}none{SHOW_RESET}"]
            )

        return _show_render(lines, color)

    def _show_kinds(self, kind):
        if kind is None or kind == "all":
            return ["actor", "reference", "exec", "value", "effect"]

        return [self._canonical_kind(kind)]

    @staticmethod
    def _show_match_lines(matches):
        lines = []

        for kind, match in matches:
            identifier = match.id
            if kind == "reference" and match.version is not None:
                identifier = f"{identifier}[v{match.version}]"

            lines.append(
                f"  {SHOW_CYAN}{kind.upper()}{SHOW_RESET} "
                f"{SHOW_WHITE}{identifier}{SHOW_RESET}"
            )

        return lines


class ByBase:
    @property
    def trace(self):
        return getattr(self.inspect, "__self__", None)

    def _relation_entry(self, exec_id, ref, version):
        evidence = _state_query_evidence(
            self.trace,
            ref,
            version,
        )
        return {
            "exec_id": exec_id,
            "ref": ref,
            "version": version,
            "id": evidence["state"]["id"],
            "value_type": evidence["recorded_as"]["type"],
            "divergence": evidence,
        }

    def _exec_record(self, exec_id) -> Object:
        return self.inspect(exec_id, "exec_record").object

    def _actor_for_exec(self, exec_id: Object):
        return self._exec_record(exec_id).object.producer_id

    def _actor_name_for_exec(self, exec_id):
        actor = self._actor_for_exec(exec_id)

        if hasattr(actor, "source_actor"):
            return actor.source_actor

        return str(actor)

    def _relation_is_mutable(self, exec_id, ref, version, *, produced):
        execution = self._exec_record(exec_id).object
        if produced:
            records = execution.writes
            return any(
                write.stored_target == ref
                and write.stored_version == version
                and write.observed_mutable
                for write in records
            )

        records = []
        for dependency in execution.deps:
            if dependency.type == "dynamic":
                records.extend(dependency.expanded_to or ())
            else:
                records.append(dependency)
        return any(
            read.unscoped_reference == ref
            and read.resolved_version == version
            and read.observed_mutable
            for read in records
        )

    @property
    def actors(self):
        return [
            self._actor_for_exec(exec_id)
            for exec_id in self.execs
        ]

    def to_dict(self):
        return {
            "id": _query_json_value(self.source.id),
            "type": self.source.type,
            "execs": list(self.execs),
            "actors": [actor.to_dict() for actor in self.actors],
            "refs": [list(ref) for ref in self.refs],
            "by_exec": {
                exec_id: [list(ref) for ref in refs]
                for exec_id, refs in self.by_exec.items()
            },
            "paths": {
                exec_id: [list(ref) for ref in refs]
                for exec_id, refs in self.by_exec.items()
            },
            "relations": [
                self._relation_entry(exec_id, ref, version)
                for exec_id, refs in self.by_exec.items()
                for ref, version in refs
            ],
            "divergence": _query_divergence(
                self.trace,
                self.refs,
            ),
        }

    def diff(self, other):
        return _build_compared(self, other)

    def show(self, color=True, simple=False, colour=None):
        color, simple = _show_options(color, simple, colour)
        if self.source.type in ["reference", "value"]:
            switch_order = True
        else:
            switch_order = False

        if isinstance(self, ProducedBy):
            action = "Produced by"
            ref_action = "produced"
            produced = True
        else:
            action = "Consumed by"
            ref_action = "consumed"
            produced = False

        source_display = show_query_display(
            self.source.type,
            self.source.id,
            None if self.source.grouped else str(self.source.object),
            self.source.grouped,
            getattr(self.source, "version", None),
        )
        title = (
            f"{action.upper()} {source_display}"
            if not switch_order
            else f"{source_display} {action.upper()}"
        )
        data = self.to_dict()
        summary = f"{len(self.execs)} {'execution' if len(self.execs) == 1 else 'executions'} found"
        if data["divergence"]["divergent"]:
            summary += f" | {SHOW_RED}DIVERGENT{SHOW_RESET}"
        lines = show_header(
            title,
            summary,
            show_identity(self.source.id, self.source.grouped),
        )
        lines.extend(show_section("RELATIONS"))

        for exec_id, refs in self.by_exec.items():
            exec_record = self._exec_record(exec_id)

            lines.append(
                f"  {SHOW_DIM}via:{SHOW_RESET} "
                f"{SHOW_CYAN}{exec_record}{SHOW_RESET}"
            )

            relations = [
                relation
                for relation in data["relations"]
                if relation["exec_id"] == exec_id
            ]
            for relation in relations:
                ref = relation["ref"]
                ver = relation["version"]
                state = _state_occurrence_text(
                    ref,
                    ver,
                    observed_mutable=self._relation_is_mutable(
                        exec_id,
                        ref,
                        ver,
                        produced=produced,
                    ),
                )
                marker = (
                    f"{SHOW_RED}!{SHOW_RESET}"
                    if relation["divergence"]["divergent"]
                    else ""
                )
                value_type = relation["value_type"] or "unknown"
                lines.append(
                    f"    {SHOW_DIM}{ref_action}:{SHOW_RESET} "
                    f"{marker}{SHOW_GREEN}{state}{SHOW_RESET} "
                    f"{SHOW_DIM}as{SHOW_RESET} "
                    f"{SHOW_WHITE}{value_type}{SHOW_RESET}"
                )

        if not self.by_exec:
            lines.append(f"{SHOW_YELLOW}└── No relations found{SHOW_RESET}")

        return _show_render(lines, color)


class ConsumedBy(ByBase):
    def __init__(self, inspect, source, execs, refs, consumed_by_exec):
        self.inspect = inspect
        self.source = source
        self.execs = execs
        self.refs = refs
        self.by_exec = consumed_by_exec

        super().__init__()

    def to_dict(self):
        return super().to_dict()

    def show(self, color=True, simple=False, colour=None):
        return super().show(
            color=color,
            simple=simple,
            colour=colour,
        )


class ProducedBy(ByBase):
    def __init__(self, inspect, source, execs, refs, produced_by_exec):
        self.inspect = inspect
        self.source = source
        self.execs = execs
        self.refs = refs
        self.by_exec = produced_by_exec

        super().__init__()

    def to_dict(self):
        return super().to_dict()

    def show(self, color=True, simple=False, colour=None):
        return super().show(
            color=color,
            simple=simple,
            colour=colour,
        )


class ExecutionAncestry:
    """Direct recorded attribution between parent and child executions."""

    def __init__(self, trace, source, executions, direction):
        self.trace = trace
        self._source_exec_id = (
            source.exec_id if source is not None else None
        )
        self._exec_ids = tuple(execution.exec_id for execution in executions)
        self.direction = direction

    @property
    def source(self):
        return self.trace.execs.get(self._source_exec_id)

    @property
    def executions(self):
        return tuple(
            self.trace.execs[exec_id]
            for exec_id in self._exec_ids
            if exec_id in self.trace.execs
        )

    def _retained_executions(self):
        return tuple(
            self.trace._index.exec_by_exec_id[exec_id]
            for exec_id in self._exec_ids
            if exec_id in self.trace._index.exec_by_exec_id
        )

    def __len__(self):
        return len(self._exec_ids)

    def __bool__(self):
        return bool(self._exec_ids)

    def __iter__(self):
        return iter(self.executions)

    @staticmethod
    def _concretised_dependencies(execution):
        for dependency in execution.deps:
            if dependency.type == "dynamic":
                yield from dependency.expanded_to or ()
            else:
                yield dependency

    def _read_entry(self, read):
        ref = read.unscoped_reference
        version = read.resolved_version
        evidence = _state_query_evidence(self.trace, ref, version)
        return {
            "reference": ref,
            "version": version,
            "state": _state_occurrence_text(
                ref,
                version,
                observed_mutable=bool(read.observed_mutable),
            ),
            "value_hash": evidence["recorded_as"]["hash"],
            "value_type": evidence["recorded_as"]["type"],
            "observed_mutable": bool(read.observed_mutable),
            "input": _query_json_value(read.associated_input),
        }

    def _write_entry(self, write):
        ref = write.stored_target
        version = write.stored_version
        evidence = _state_query_evidence(self.trace, ref, version)
        return {
            "reference": ref,
            "version": version,
            "state": _state_occurrence_text(
                ref,
                version,
                observed_mutable=bool(write.observed_mutable),
            ),
            "value_hash": evidence["recorded_as"]["hash"],
            "value_type": evidence["recorded_as"]["type"],
            "observed_mutable": bool(write.observed_mutable),
            "updated": bool(write.updated),
        }

    def _execution_entry(self, execution):
        return {
            "seq_id": execution.seq_id,
            "exec_id": execution.exec_id,
            "actor": execution.producer_id.to_dict(),
            "actor_occurrence": _query_json_value(
                execution.producer_occurrence_id
            ),
            "status": execution.status,
            "reads": [
                self._read_entry(read)
                for read in self._concretised_dependencies(execution)
            ],
            "writes": [
                self._write_entry(write)
                for write in execution.writes
            ],
        }

    def to_dict(self):
        return {
            "type": "execution_ancestry",
            "direction": self.direction,
            "source": {
                "seq_id": self.source.seq_id,
                "exec_id": self.source.exec_id,
                "actor": self.source.producer_id.to_dict(),
            } if self.source is not None else None,
            "count": len(self._exec_ids),
            "executions": [
                self._execution_entry(execution)
                for execution in self._retained_executions()
            ],
        }

    def show(
        self,
        show_in_out=True,
        color=True,
        simple=False,
        colour=None,
    ):
        color, simple = _show_options(color, simple, colour)
        if not isinstance(show_in_out, bool):
            raise TypeError("show show_in_out must be a bool")

        data = self.to_dict()
        relation = "PARENTS" if self.direction == "parents" else "CHILDREN"
        source = (
            f"EXEC_RECORD[{self.source.seq_id}] "
            f"{self.source.producer_id.source_actor}"
            if self.source is not None
            else "unknown execution"
        )
        count = len(self._exec_ids)
        summary = f"{count} {'execution' if count == 1 else 'executions'} found"
        lines = show_header(
            f"{relation} OF {source}",
            summary,
            self.source.exec_id if self.source is not None else None,
        )
        lines.extend(show_section("EXECUTIONS"))

        for execution, entry in zip(
            self._retained_executions(),
            data["executions"],
        ):
            lines.append(
                f"  {SHOW_CYAN}EXEC_RECORD[{execution.seq_id}]{SHOW_RESET} "
                f"{SHOW_WHITE}{execution.producer_id.source_actor}{SHOW_RESET}"
            )
            if not show_in_out:
                continue
            for read in entry["reads"]:
                lines.append(
                    f"    {SHOW_DIM}used:{SHOW_RESET} "
                    f"{SHOW_GREEN}{read['state']}{SHOW_RESET} "
                    f"{SHOW_DIM}as{SHOW_RESET} "
                    f"{SHOW_WHITE}{read['value_type'] or 'unknown'}{SHOW_RESET}"
                )
            for write in entry["writes"]:
                lines.append(
                    f"    {SHOW_DIM}{'updated' if write['updated'] else 'wrote'}:{SHOW_RESET} "
                    f"{SHOW_GREEN}{write['state']}{SHOW_RESET} "
                    f"{SHOW_DIM}as{SHOW_RESET} "
                    f"{SHOW_WHITE}{write['value_type'] or 'unknown'}{SHOW_RESET}"
                )

        if not self._exec_ids:
            lines.append(f"  {SHOW_DIM}none{SHOW_RESET}")

        return _show_render(lines, color)


class Investigate:
    def __init__(self, trace: Trace, inspect, inspected: Inspected):
        self.trace = trace
        self.index = trace._index
        self.inspect = inspect
        self.inspected = inspected

    def now(self):
        return self.inspected.now()

    def _versioned_ref_str(self, ref, ver):
        return f"{ref}[v{ver}]"

    def _actor_entity(self, actor, occurrence_id=None):
        if actor is None: 
            return None

        name = getattr(actor, "source_actor", str(actor))
        display_str = f"ACTOR[{name}]" 
        if occurrence_id is not None:
            display_str = f"ACTOR[{occurrence_id}]"
        return {
            "kind": "actor",
            "id": str(getattr(actor, "id", actor)),
            "display": display_str,
            "name": name,
            "version": occurrence_id.occurrence_id if occurrence_id is not None else None
        }

    def _exec_entity(self, exec_id):
        if exec_id is None:
            return None

        inspected = self.inspect(exec_id, "exec_record")
        exec_obj = inspected.object
        producer = getattr(exec_obj, "producer_id", None)
        occurrence_id = getattr(exec_obj, "producer_occurrence_id", None)

        return {
            "kind": "exec_record",
            "id": exec_id,
            "display": repr(exec_obj),
            "actor": self._actor_entity(producer, occurrence_id),
            "ocean": list(inspected.source_oceans or []),
            "source": list(inspected.source_runners or []),
            "pipeline": list(inspected.source_pipelines or []),
        }

    def _object_display(self):
        if not self.inspected.exists:
            return str(self.inspected.id)

        if self.inspected.type == "value":
            return repr(
                self.index.value_by_hash.get(
                    self.inspected.id,
                    self.inspected.object,
                )
            )
        
        if self.inspected.grouped:
            return f"GROUPED[{self.inspected.id}]"

        return repr(self.inspected.object)

    def _object_entity(self):
        inspected_dict = self.inspected.to_dict()

        return {
            **inspected_dict,
            "kind": self.inspected.type,
            "id": inspected_dict["id"],
            "display": self._object_display(),
            "version": self.inspected.version,
            "grouped": self.inspected.grouped,
        }

    def _input_uses(self, exec_id, ref, ver):
        if exec_id is None:
            return []

        execution = self.index.exec_by_exec_id.get(exec_id)
        if execution is None:
            return []

        return [
            {
                **input_use,
                "display": (
                    f"{input_use['name']}[{input_use['index']}]"
                    if input_use["index"] is not None
                    else str(input_use["name"])
                ),
            }
            for input_use in self.index._inputs_read_from(
                execution,
                (ref, ver),
            )
        ]

    def _consumer_entity(self, exec_id, ref, ver):
        consumer = self._exec_entity(exec_id)

        if consumer is None:
            return None

        return {
            **consumer,
            "input_uses": self._input_uses(exec_id, ref, ver),
        }

    def _versioned_ref_entry(self, ref, ver):
        versioned_ref = (ref, ver)
        producer_exec_ids = list(
            self.index.producer_exec_ids_by_ref_version.get(
                versioned_ref,
                [],
            )
        )
        consumer_exec_ids = list(
            self.index.consumer_exec_ids_by_ref_version.get(versioned_ref, [])
        )

        value_evidence = _state_query_evidence(
            self.trace,
            ref,
            ver,
        )

        return {
            "kind": "reference_version",
            "id": self._versioned_ref_str(ref, ver),
            "ref": ref,
            "version": ver,
            "value_type": value_evidence["recorded_as"]["type"],
            "divergence": value_evidence,
            "versioned_ref": {
                "ref": ref,
                "version": ver,
            },
            "value_hash": self.index.value_hash_by_ref_version.get(versioned_ref),
            "producer": (
                self._exec_entity(producer_exec_ids[0])
                if len(producer_exec_ids) == 1
                else None
            ),
            "producers": [
                self._exec_entity(exec_id)
                for exec_id in producer_exec_ids
            ],
            "consumers": [
                self._consumer_entity(exec_id, ref, ver)
                for exec_id in consumer_exec_ids
            ],
            "used": bool(consumer_exec_ids),
            "unused": not bool(consumer_exec_ids),
            "available": versioned_ref not in self.index.deleted_ref_versions,
            "healthy": (
                bool(consumer_exec_ids)
                and versioned_ref not in self.index.deleted_ref_versions
            ),
        }

    def _read_entry(self, record, exec_id=None):
        ref = getattr(record, "unscoped_reference", None)
        ver = getattr(record, "resolved_version", None)
        associated_input = getattr(record, "associated_input", None)
        if isinstance(associated_input, (tuple, list)):
            input_name = associated_input[0] if associated_input else None
        elif isinstance(associated_input, str):
            input_name = associated_input
        elif isinstance(associated_input, dict):
            input_name = associated_input.get("name")
        else:
            input_name = getattr(associated_input, "name", None)
        input_index = getattr(record, "input_index", None)

        input_use = None
        if input_name is not None:
            input_use = {
                "name": input_name,
                "index": input_index,
                "display": (
                    f"{input_name}[{input_index}]"
                    if input_index is not None
                    else str(input_name)
                ),
            }

        associated_input_entry = (
            {
                "name": input_name,
                "index": input_index,
            }
            if input_name is not None
            else None
        )

        if ref is None:
            return {
                "kind": "read",
                "record_type": getattr(record, "type", None),
                "display": repr(record),
                "associated_input": associated_input_entry,
                "input_use": input_use,
                "input_uses": [],
            }

        versioned_ref = self._versioned_ref_entry(ref, ver)
        versioned_ref["display"] = _state_occurrence_text(
            ref,
            ver,
            observed_mutable=bool(
                getattr(record, "observed_mutable", False)
            ),
        )
        indexed_input_uses = self._input_uses(exec_id, ref, ver)

        return {
            **versioned_ref,
            "kind": "read",
            "record_type": getattr(record, "type", None),
            "associated_input": associated_input_entry,
            "input_use": input_use,
            "input_uses": indexed_input_uses,
        }

    def _write_entry(self, record):
        ref = getattr(record, "stored_target", None)
        ver = getattr(record, "stored_version", None)

        if ref is None:
            return {
                "kind": "write",
                "display": repr(record),
            }

        versioned_ref = self._versioned_ref_entry(ref, ver)
        versioned_ref["display"] = _state_occurrence_text(
            ref,
            ver,
            observed_mutable=bool(
                getattr(record, "observed_mutable", False)
            ),
        )

        return {
            **versioned_ref,
            "kind": "write",
        }

    def _effect_entry(self, record):
        effect = record.effect
        target, version = record.targets

        def same_effect(candidate):
            return (
                candidate.event_id == record.event_id
                and candidate.effect == record.effect
                and safe_equal(candidate.targets, record.targets)
                and candidate.origin == record.origin
                and candidate.immediate == record.immediate
                and candidate.target_existed == record.target_existed
                and safe_equal(candidate.output, record.output)
            )

        occurrences = [
            occurrence
            for occurrence in self.index.effect_occurrences_by_event_id.get(
                record.event_id,
                (),
            )
            if same_effect(self.index.effect_by_occurrence[occurrence][1])
        ]
        producer_exec_ids = [
            self.index.effect_by_occurrence[occurrence][0]
            for occurrence in occurrences
        ]
        affected = []

        for occurrence in occurrences:
            for affected_record in self.index.affected_records_by_effect_occurrence[
                occurrence
            ]:
                actor = affected_record.affectee
                input_positions = []
                for item in affected_record.affected_inputs or []:
                    if isinstance(item, dict):
                        input_positions.append(str(item.get("name", "unknown")))
                    else:
                        input_positions.append(str(item))

                affected.append({
                    "actor": self._actor_entity(actor),
                    "execution": self._exec_entity(
                        affected_record.affected_exec_id
                    ),
                    "inputs": input_positions,
                })

        if effect == Effects.DELETE:
            available = False
            healthy = False
        elif effect == Effects.READ:
            available = record.target_existed is True
            healthy = available
        else:
            available = (
                (target, version) not in self.index.deleted_ref_versions
            )
            healthy = bool(
                self.index.consumer_exec_ids_by_ref_version.get(
                    (target, version),
                    [],
                )
            ) and available

        return {
            "kind": "effect",
            "effect": effect,
            "target": target,
            "version": version,
            "id": (effect, (target, version)),
            "display": f"{effect} -> {target}[v{version}]" if version is not None else f"{effect} -> {target}",
            "available": available,
            "healthy": healthy,
            "output": record.output,
            "producers": [
                self._exec_entity(exec_id)
                for exec_id in producer_exec_ids
            ],
            "affected": affected,
        }

    def _relation_entries(self, relations):
        entries = []

        for actor, exec_ids in (relations or {}).items():
            entries.append({
                "actor": self._actor_entity(actor),
                "executions": [
                    self._exec_entity(exec_id)
                    for exec_id in exec_ids
                ],
            })

        return entries

    def _exec_history_entry(self, exec_id):
        inspected_exec = self.inspect(exec_id, "exec_record")

        return {
            "execution": self._exec_entity(exec_id),
            "reads": [
                self._read_entry(record, exec_id=exec_id)
                for record in inspected_exec.reads
            ],
            "writes": [
                self._write_entry(record)
                for record in inspected_exec.writes
            ],
        }

    def _appearances(self):
        if not self.inspected.grouped:
            return []

        appearances = []

        for grouped_obj in self.inspected.object:
            inspected = self.inspect(
                grouped_obj.object,
                self.inspected.type,
                self.inspected.version,
            )
            inspected_data = inspected.to_dict()

            appearance = {
                "kind": inspected.type,
                "id": inspected_data["id"],
                "display": (
                    str(grouped_obj.object)
                    if inspected.type == "reference"
                    else repr(grouped_obj.object)
                ),
                "version": inspected.version,
                "executions": [],
                "versions": [],
                "used": None,
                "unused": None,
            }

            if inspected.type == "actor":
                appearance["executions"] = [
                    self._exec_entity(exec_id)
                    for exec_id in inspected.history
                ]

            elif inspected.type == "reference":
                appearance["versions"] = [
                    self._versioned_ref_entry(inspected.id, ver)
                    for ver in inspected.history
                ]
                appearance["used"] = any(
                    version["used"]
                    for version in appearance["versions"]
                )
                appearance["unused"] = (
                    bool(appearance["versions"])
                    and not appearance["used"]
                )

            appearances.append(appearance)

        return appearances

    def _navigation_entry(self, action, target, reason, direction=None):
        entry = {
            "action": action,
            "target": target,
            "reason": reason,
        }

        if direction is not None:
            entry["direction"] = direction

        return entry

    def _append_navigation(self, navigation, action):
        key = (
            action["action"],
            action["target"]["kind"],
            str(action["target"]["id"]),
            action.get("direction"),
            action["reason"],
        )

        existing = {
            (
                item["action"],
                item["target"]["kind"],
                str(item["target"]["id"]),
                item.get("direction"),
                item["reason"],
            )
            for item in navigation
        }

        if key not in existing:
            navigation.append(action)

    def to_dict(self):
        if self.inspected is None:
            return {}

        object_entity = self._object_entity()
        kind = object_entity["kind"]

        read_exec_id = (
            object_entity["id"]
            if kind == "exec_record"
            else None
        )
        reads = [
            self._read_entry(record, exec_id=read_exec_id)
            for record in (self.inspected.reads or [])
        ]
        writes = [
            self._write_entry(record)
            for record in (self.inspected.writes or [])
        ]

        data = {
            "type": "investigate",
            "kind": kind,
            "object": object_entity,
            "summary": {},
            "appearances": self._appearances(),
            "history": [],
            "reads": reads,
            "writes": writes,
            "upstream": self._relation_entries(self.inspected.upstream),
            "downstream": self._relation_entries(self.inspected.downstream),
            "parents": object_entity.get("parents", []),
            "children": object_entity.get("children", []),
            "navigation": [],
            "divergence": object_entity.get(
                "divergence",
                {"divergent": False, "states": []},
            ),
        }

        if kind == "exec_record" and self.inspected.exists and not self.inspected.grouped:
            values = self.trace.inputs_outputs_of(object_entity["id"])
            data["inputs"] = values.input_types
            data["output"] = values.output_type
            for relationship, reason in (
                (data["parents"], "parent execution"),
                (data["children"], "child execution"),
            ):
                for entry in relationship:
                    self._append_navigation(
                        data["navigation"],
                        self._navigation_entry(
                            "investigate",
                            self._exec_entity(entry["exec_id"]),
                            reason,
                        ),
                    )

        if kind in ["reference", "value"]:
            history = []

            for item in self.inspected.history:
                if kind == "reference" and not self.inspected.grouped:
                    ref, ver = object_entity["id"], item
                else:
                    ref, ver = item

                history.append(self._versioned_ref_entry(ref, ver))

            used_count = sum(1 for entry in history if entry["used"])
            unused_count = len(history) - used_count

            data["history"] = history
            data["summary"] = {
                "version_count": len(history),
                "used_version_count": used_count,
                "unused_version_count": unused_count,
                "all_consumed": bool(history) and unused_count == 0,
                "none_consumed": bool(history) and used_count == 0,
                "read_count": len(reads),
                "write_count": len(writes),
            }

            for entry in history:
                if entry["producer"] is not None:
                    self._append_navigation(
                        data["navigation"],
                        self._navigation_entry(
                            "investigate",
                            entry["producer"],
                            f"producer of {entry['id']}",
                        ),
                    )

                for consumer in entry["consumers"]:
                    self._append_navigation(
                        data["navigation"],
                        self._navigation_entry(
                            "investigate",
                            consumer,
                            f"consumer of {entry['id']}",
                        ),
                    )

        elif kind in ["actor", "exec_record"]:
            exec_ids = (
                list(self.inspected.history)
                if kind == "actor"
                else [object_entity["id"]]
                if self.inspected.exists
                else []
            )

            data["history"] = [
                self._exec_history_entry(exec_id)
                for exec_id in exec_ids
            ]
            data["summary"] = {
                "execution_count": len(data["history"]),
                "read_count": len(reads),
                "write_count": len(writes),
                "used_write_count": sum(
                    1
                    for write in writes
                    if write.get("used")
                ),
                "unused_write_count": sum(
                    1
                    for write in writes
                    if write.get("unused")
                ),
            }

            for relation in data["upstream"]:
                for execution in relation["executions"]:
                    self._append_navigation(
                        data["navigation"],
                        self._navigation_entry(
                            "investigate",
                            execution,
                            "upstream execution",
                        ),
                    )

        elif kind == "effect":
            data["history"] = [
                self._effect_entry(record)
                for record in self.inspected.object.effects
            ]
            producer_ids = {
                producer["id"]
                for entry in data["history"]
                for producer in entry["producers"]
                if producer is not None
            }
            affected_ids = {
                affected["execution"]["id"]
                for entry in data["history"]
                for affected in entry["affected"]
                if affected["execution"] is not None
            }
            data["summary"] = {
                "effect_count": len(data["history"]),
                "target_count": len({
                    (entry["target"], entry["version"])
                    for entry in data["history"]
                }),
                "producer_count": len(producer_ids),
                "affected_execution_count": len(affected_ids),
            }

            for entry in data["history"]:
                for producer in entry["producers"]:
                    self._append_navigation(
                        data["navigation"],
                        self._navigation_entry(
                            "investigate",
                            producer,
                            f"producer of {entry['display']}",
                        ),
                    )
                for affected in entry["affected"]:
                    if affected["execution"] is None:
                        continue
                    self._append_navigation(
                        data["navigation"],
                        self._navigation_entry(
                            "investigate",
                            affected["execution"],
                            f"affected by {entry['display']}",
                        ),
                    )

            for relation in data["downstream"]:
                for execution in relation["executions"]:
                    self._append_navigation(
                        data["navigation"],
                        self._navigation_entry(
                            "investigate",
                            execution,
                            "downstream execution",
                        ),
                    )

        self._append_navigation(
            data["navigation"],
            self._navigation_entry(
                "path",
                object_entity,
                "trace causal history upstream",
                direction="upstream",
            ),
        )
        self._append_navigation(
            data["navigation"],
            self._navigation_entry(
                "path",
                object_entity,
                "trace causal history downstream",
                direction="downstream",
            ),
        )

        return data

    def diff(self, other, diff_object=True):
        return _build_object_compared(
            self,
            other,
            diff_object=diff_object,
        )

    def show(
        self,
        full_ids=True,
        explore_tool_tips=False,
        show_input_positions=True,
        simple=False,
        color=True,
        colour=None,
        values=False,
    ):
        color, simple = _show_options(color, simple, colour)
        if not isinstance(values, bool):
            raise TypeError("show values must be a bool")
        RESET = "\033[0m"
        BOLD = "\033[1m"
        DIM = "\033[2m"

        CYAN = "\033[36m"
        GREEN = "\033[32m"
        YELLOW = "\033[33m"
        RED = "\033[31m"
        WHITE = "\033[37m"

        SECTION_WIDTH = 70

        def section(title):
            return [f"{DIM}{title}:{RESET}"]

        def plural(count, singular, plural_word=None):
            return singular if count == 1 else (plural_word or f"{singular}s")

        def format_input_uses(input_uses):
            grouped = defaultdict(list)

            for input_use in input_uses:
                grouped[input_use["name"]].append(input_use["index"])

            displays = []

            for name, indexes in grouped.items():
                indexes = [
                    index
                    for index in indexes
                    if index is not None
                ]

                if indexes:
                    displays.append(
                        f"{name}[{', '.join(str(index) for index in indexes)}]"
                    )
                else:
                    displays.append(name)

            return ", ".join(displays)

        def simple_appearance_display(appearance):
            kind = appearance["kind"]
            identifier = appearance["id"]

            if kind == "actor":
                if isinstance(identifier, dict):
                    identifier = identifier.get("name", identifier["id"])
                else:
                    identifier = getattr(
                        identifier,
                        "source_actor",
                        identifier,
                    )

            return f"{str(kind).upper()}[{identifier}]"

        def version_summary(summary):
            total = summary["version_count"]
            used_count = summary["used_version_count"]
            unused_count = summary["unused_version_count"]
            version_word = plural(total, "version")

            if used_count and unused_count:
                return (
                    f"{total} {version_word} found: "
                    f"{GREEN}{used_count} used{RESET}{DIM}, "
                    f"{RED}{unused_count} unused{RESET}{DIM}."
                )

            if used_count:
                return (
                    f"{total} {version_word} found; "
                    f"{GREEN}all were consumed{RESET}{DIM}."
                )

            return (
                f"{total} {version_word} found; "
                f"{YELLOW}none were consumed{RESET}{DIM}."
            )

        def value_text(entry):
            if not values:
                return ""

            value_hash = entry.get("value_hash")
            if value_hash not in self.index.value_by_hash:
                return ""

            value = self.index.value_by_hash[value_hash]
            evidence = _state_query_evidence(
                self.trace,
                entry["ref"],
                entry["version"],
            )["recorded_as"]
            value_type = evidence["type"] or type(value).__name__
            if value_type in {"TIDEUnknownType", "TIDEUnknownValue"}:
                value_type = f"opaque<{value_type}>"
            rendered = evidence["repr"] or repr(value)
            return f" {DIM}={RESET} {WHITE}{value_type} {rendered}{RESET}"

        if self.inspected is None:
            output = (
                f"{RED}{BOLD}Nothing to show{RESET} "
                f"- couldn't find reference object"
            )
            print(output)
            return output

        data = self.to_dict()
        obj = data["object"]
        kind = data["kind"]
        query = show_query_display(
            kind,
            obj["id"],
            obj["display"],
            obj["grouped"],
            obj["version"],
        )

        if kind == "value" and not obj["grouped"]:
            query = f"value {query}"

        if kind in ["reference", "value"]:
            summary = version_summary(data["summary"])
        elif kind in ["actor", "exec_record"]:
            count = data["summary"]["execution_count"]
            summary = f"{count} {plural(count, 'execution')} found"
        elif kind == "effect":
            count = data["summary"]["effect_count"]
            affected_count = data["summary"]["affected_execution_count"]
            summary = (
                f"{count} {plural(count, 'effect')} found | "
                f"{affected_count} affected "
                f"{plural(affected_count, 'execution')}"
            )
        else:
            summary = "renderer pending"

        if data["divergence"]["divergent"]:
            summary += f" | {RED}DIVERGENT{RESET}{DIM}"

        lines = [
            "",
            f"{CYAN}{'=' * SECTION_WIDTH}{RESET}",
            f"{CYAN}{BOLD}INVESTIGATE {query}{RESET} "
            f"{DIM}| {summary}{RESET}",
            f"{DIM}{show_identity(display_identity(obj['id'], full_ids=full_ids), obj['grouped'])}{RESET}",
            f"{CYAN}{'=' * SECTION_WIDTH}{RESET}",
        ]

        if data["appearances"]:
            lines.extend(section("APPEARED AS"))

            for appearance in data["appearances"]:
                if appearance["versions"]:
                    appearance_colour = (
                        GREEN
                        if appearance["used"]
                        else RED
                    )
                else:
                    appearance_colour = DIM

                appearance_display = appearance["display"]
                if simple:
                    appearance_display = simple_appearance_display(appearance)
                    if appearance["kind"] == "actor":
                        appearance_colour = DIM

                lines.append(
                    f"{appearance_colour}"
                    f"{appearance_display}"
                    f"{RESET}"
                )

                for version in appearance["versions"]:
                    colour = GREEN if version["used"] else RED
                    producer = version["producer"]

                    if producer is None:
                        lines.append(
                            f"  {colour}[v{version['version']}]{RESET}"
                        )
                    else:
                        lines.append(
                            f"  {colour}[v{version['version']}]{RESET} "
                            f"{DIM}by{RESET} "
                            f"{CYAN}{producer['display']}{RESET}"
                        )

                for execution in appearance["executions"]:
                    pipelines = execution.get("pipeline", [])

                    if simple:
                        lines.append(
                            f"{CYAN}  {execution['display']}{RESET}"
                        )
                        continue

                    if pipelines:
                        lines.append(
                            f"{CYAN}  {execution['display']}{RESET} "
                            f"{DIM}from pipeline{RESET} "
                            f"{WHITE}{pipelines[0]}{RESET}"
                        )
                    else:
                        lines.append(
                            f"{CYAN}  {execution['display']}{RESET}"
                        )
                lines.append("")

        if kind == "exec_record" and "inputs" in data:
            lines.extend(section("INPUTS"))
            if not data["inputs"]:
                lines.append(f"  {DIM}none{RESET}")
            for name, input_type in data["inputs"].items():
                lines.append(f"  {DIM}{name}:{RESET} {CYAN}{input_type}{RESET}")

            lines.extend(section("OUTPUT"))
            lines.append(f"  {DIM}output:{RESET} {CYAN}{data['output']}{RESET}")

            lines.extend(section("RELATIONSHIPS"))
            if not data["parents"] and not data["children"]:
                lines.append(f"  {DIM}none{RESET}")
            for label, relationships in (
                ("parent", data["parents"]),
                ("child", data["children"]),
            ):
                for entry in relationships:
                    lines.append(
                        f"  {DIM}{label}:{RESET} "
                        f"{CYAN}EXEC_RECORD[{entry['seq_id']}]{RESET} "
                        f"{WHITE}{entry['actor']['name']}{RESET}"
                    )
        
        lines.extend(section("HISTORY"))

        if kind in ["reference", "value"]:
            for item_index, item in enumerate(data["history"]):
                if item_index:
                    lines.append("")

                colour = GREEN if item["healthy"] else RED
                marker = (
                    f"{RED}!{RESET}"
                    if item["divergence"]["divergent"]
                    else ""
                )
                lines.append(
                    f"{marker}{colour}{item['id']}{RESET}{value_text(item)}"
                )

                producer = item["producer"]

                if producer is not None:
                    value_type = item.get("value_type") or "unknown"
                    lines.append(
                        f"{DIM}└── produced by{RESET} "
                        f"{CYAN}{producer['display']}{RESET} "
                        f"{DIM}as{RESET} "
                        f"{WHITE}{value_type}{RESET}"
                    )

                for consumer in item["consumers"]:
                    input_uses = consumer.get("input_uses", [])

                    if not show_input_positions or not input_uses:
                        value_type = item.get("value_type") or "unknown"
                        lines.append(
                            f"{DIM}    └── consumed by{RESET} "
                            f"{CYAN}{consumer['display']}{RESET} "
                            f"{DIM}as{RESET} "
                            f"{WHITE}{value_type}{RESET}"
                        )
                        continue

                    input_uses_display = format_input_uses(input_uses)

                    value_type = item.get("value_type") or "unknown"
                    lines.append(
                        f"{DIM}    └── consumed by{RESET} "
                        f"{CYAN}{consumer['display']}{RESET} "
                        f"{DIM}as{RESET} "
                        f"{WHITE}{value_type}{RESET} "
                        f"{DIM}via{RESET} "
                        f"{WHITE}{input_uses_display}{RESET}"
                    )

        elif kind in ["actor", "exec_record"]:
            for item_index, item in enumerate(data["history"]):
                if item_index:
                    lines.append("")

                execution = item["execution"]
                reads = item["reads"]
                writes = item["writes"]

                lines.append(f"{CYAN}{execution['display']}{RESET}")

                if reads:
                    read_branch = "└──" if not writes else "├──"
                    lines.append(f"{DIM}{read_branch} read{RESET}")

                    for idx, read in enumerate(reads):
                        connector = "└──" if idx == len(reads) - 1 else "├──"
                        prefix = "    " if not writes else "│   "
                        producer = read.get("producer")
                        read_id = read.get(
                            "display",
                            read.get("id", "unknown read"),
                        )

                        marker = (
                            f"{RED}!{RESET}"
                            if read["divergence"]["divergent"]
                            else ""
                        )
                        value_type = read.get("value_type") or "unknown"
                        line = (
                            f"{DIM}{prefix}{connector}{RESET} "
                            f"{marker}{GREEN}{read_id}{RESET} "
                            f"{DIM}as{RESET} "
                            f"{WHITE}{value_type}{RESET}"
                            f"{value_text(read)}"
                        )

                        input_use = read.get("input_use")
                        if show_input_positions and input_use is not None:
                            line += (
                                f" {DIM}via{RESET} "
                                f"{WHITE}{input_use['display']}{RESET}"
                            )

                        if producer is not None:
                            line += (
                                f" {DIM}from{RESET} "
                                f"{CYAN}{producer['display']}{RESET}"
                            )

                        lines.append(line)

                if writes:
                    lines.append(f"{DIM}└── wrote{RESET}")

                    for idx, write in enumerate(writes):
                        is_last_write = idx == len(writes) - 1
                        connector = "└──" if is_last_write else "├──"
                        colour = GREEN if write.get("healthy", False) else RED
                        write_id = write.get(
                            "display",
                            write.get("id", "unknown write"),
                        )

                        marker = (
                            f"{RED}!{RESET}"
                            if write["divergence"]["divergent"]
                            else ""
                        )
                        value_type = write.get("value_type") or "unknown"
                        lines.append(
                            f"{DIM}    {connector}{RESET} "
                            f"{marker}{colour}{write_id}{RESET} "
                            f"{DIM}as{RESET} "
                            f"{WHITE}{value_type}{RESET}"
                            f"{value_text(write)}"
                        )

                        child_prefix = (
                            "        "
                            if is_last_write
                            else "    │   "
                        )

                        consumer_lines = []
                        for consumer in write.get("consumers", []):
                            input_uses = consumer.get("input_uses", [])

                            if not show_input_positions or not input_uses:
                                consumer_lines.append((consumer, None))
                                continue

                            consumer_lines.append(
                                (
                                    consumer,
                                    {
                                        "display": format_input_uses(input_uses),
                                    },
                                )
                            )

                        for consumer_idx, (consumer, input_use) in enumerate(consumer_lines):
                            consumer_connector = (
                                "└──"
                                if consumer_idx == len(consumer_lines) - 1
                                else "├──"
                            )
                            value_type = write.get("value_type") or "unknown"
                            line = (
                                f"{DIM}{child_prefix}{consumer_connector} "
                                f"consumed by{RESET} "
                                f"{CYAN}{consumer['display']}{RESET} "
                                f"{DIM}as{RESET} "
                                f"{WHITE}{value_type}{RESET}"
                            )

                            if input_use is not None:
                                line += (
                                    f" {DIM}via{RESET} "
                                    f"{WHITE}{input_use['display']}{RESET}"
                                )

                            lines.append(line)

                if not reads and not writes:
                    lines.append(
                        f"{YELLOW}└── no recorded reads or writes{RESET}"
                    )

        elif kind == "effect":
            if not data["history"]:
                lines.append(f"{YELLOW}no matching effect evidence{RESET}")

            for entry_index, entry in enumerate(data["history"]):
                if entry_index:
                    lines.append("")

                colour = GREEN if entry["healthy"] else RED
                lines.append(
                    f"{CYAN}{str(entry['effect']).upper()}{RESET} "
                    f"{DIM}->{RESET} "
                    f"{colour}{entry['target']}[v{entry['version']}]{RESET}"
                )

                for producer in entry["producers"]:
                    lines.append(
                        f"{DIM}├── produced by{RESET} "
                        f"{CYAN}{producer['display']}{RESET}"
                    )

                for affected_index, affected in enumerate(entry["affected"]):
                    execution = affected["execution"]
                    if execution is None:
                        continue
                    connector = (
                        "└──"
                        if affected_index == len(entry["affected"]) - 1
                        else "├──"
                    )
                    line = (
                        f"{DIM}{connector} affected{RESET} "
                        f"{CYAN}{execution['display']}{RESET}"
                    )
                    if show_input_positions and affected["inputs"]:
                        line += (
                            f" {DIM}via{RESET} "
                            f"{WHITE}{', '.join(affected['inputs'])}{RESET}"
                        )
                    lines.append(line)

        else:
            lines.append(
                f"{YELLOW}No investigate renderer "
                f"for this object type yet.{RESET}"
            )

        if data["divergence"]["divergent"]:
            lines.extend(section("DIVERGENCE"))
            for state in data["divergence"]["states"]:
                recorded = state["recorded_as"]
                reconstructed = state["reconstructed_as"] or {}
                lines.append(
                    f"{RED}!{state['state']['id']}{RESET}"
                )
                lines.append(
                    f"  {DIM}recorded as:{RESET} "
                    f"{WHITE}{recorded.get('type') or 'unknown'}{RESET}"
                )
                lines.append(
                    f"  {DIM}reconstructed as:{RESET} "
                    f"{WHITE}{reconstructed.get('type') or 'unknown'}{RESET}"
                )

        if explore_tool_tips:
            lines.extend(section("EXPLORE"))

            for action in data["navigation"]:
                target = action["target"]
                direction = action.get("direction")
                command = (
                    f"{action['action']} "
                    f"{display_identity(target['id'], full_ids=full_ids)}"
                )

                if direction is not None:
                    command += f" {direction}"

                lines.append(
                    f"{DIM}{command}{RESET} "
                    f"{DIM}# {action['reason']}{RESET}"
                )

        return _show_render(lines, color)


class Object:
    def __init__(self, object=None, type=None, id=None, exists=False):
        self.object = object
        self.type = type
        self.id = id
        self.exists = exists

        self.source_ocean = []
        self.source_runner = []
        self.source_pipeline = []
        self.version = None

        self.reads = []
        self.writes = []
        self.effects = []

    def __repr__(self):
        if self.type == "exec_record":
            _str = f"{self.type.upper()}[{self.object.seq_id}]"
            self.object: ExecRecord
            obj_repr = shorten_identity(
                self.object.producer_id.source_actor
            )
            return _str + " " + obj_repr
        if self.type == "immediate_effect_record":
            return (
                f"IMMEDIATE_EFFECT_RECORD[{self.object.seq_id}] "
                f"{self.object.exec_id}"
            )
        if self.type == "actor":
            display_str = f"ACTOR[{shorten_identity(self.object.source_actor)}]"
            if self.version is not None:
                display_str = display_str.rsplit("]", 1)[0] + f'[{self.version}]]' 
            return display_str
        if self.type == "reference":
            version = self.version
            if self.version is not None:
                return f"{self.type.upper()}[{self.id}[v{version}]]"
            return f"{self.type.upper()}[{self.id}]"
        if self.type == "value":
            return f"VALUE[{self.id}]"
        # Unknown object kinds (currently including effects) may use enum or
        # tuple identifiers. Falling back to repr(self) recurses forever for
        # those identifiers, so use their value representation directly.
        return str(self.id)


class Inspected: # an overview of the object. probs include history, inputs, outputs, declared effects + deps, runtime effects + dep  
    def __init__(self, object, history=None, upstream=None, downstream=None, trace=None):
        self.object = object
        self.type = object.type
        self.id = object.id
        self.version = object.version

        self.history = history if history is not None else []
        self.upstream = upstream if upstream is not None else {}
        self.downstream = downstream if downstream is not None else {}
        self.trace = trace

        self.reads = object.reads
        self.writes = object.writes
        self.source_oceans = object.source_ocean
        self.source_runners = object.source_runner
        self.source_pipelines = object.source_pipeline
        self.grouped = False

    def __eq__(self, other):
        return all([
            self.type == other.type,
            self.id == other.id,
            self.history == other.history,
            self.version == other.version,
            (len(self.reads) + len(self.writes)) == (len(other.reads) + len(other.writes)),
            self.grouped == other.grouped
        ])
    
    def __repr__(self):
        return f"Inspected({self.type}: {self.object!r}, history[{list(self.history)[:3]}]..)"

    def to_dict(self):
        parents = []
        children = []
        if (
            self.type == "exec_record"
            and self.exists
            and not self.grouped
            and self.trace is not None
        ):
            def relation_entry(execution):
                return {
                    "seq_id": execution.seq_id,
                    "exec_id": execution.exec_id,
                    "actor": execution.producer_id.to_dict(),
                }

            parents = [
                relation_entry(execution)
                for execution in self.trace.parents_of(self.id).executions
            ]
            children = [
                relation_entry(execution)
                for execution in self.trace.children_of(self.id).executions
            ]

        data = {
            "type": self.type,
            "id": _query_json_value(self.id),
            "version": self.version,
            "source_oceans": _query_json_value(self.source_oceans),
            "source_runners": _query_json_value(self.source_runners),
            "source_pipelines": _query_json_value(self.source_pipelines),
            "history": _query_json_value(self.history),
            "upstream_seeds": _query_json_value([
                seed for seeds in self.upstream.values() for seed in seeds
            ]),
            "downstream_seeds": _query_json_value([
                seed for seeds in self.downstream.values() for seed in seeds
            ]),
            "grouped": self.grouped,
            "exists": self.exists,
            "divergence": _query_divergence(
                self.trace,
                _inspected_states(self),
            ),
        }
        if self.type == "exec_record" and self.exists and not self.grouped:
            data["parents"] = parents
            data["children"] = children
        return data

    def diff(self, other, diff_object=True):
        return _build_object_compared(
            self,
            other,
            diff_object=diff_object,
        )

    def now(self):
        if self.type != "exec_record":
            return None
        metadata = self.object.object.now()
        if metadata is None or self.trace is None:
            return metadata
        return metadata._bind(self.trace, self.id)

    def show(self, color=True, simple=False, colour=None):
        color, simple = _show_options(color, simple, colour)
        RESET = "\033[0m"
        BOLD = "\033[1m"
        DIM = "\033[2m"
        CYAN = "\033[36m"
        BLUE = "\033[34m"
        WHITE = "\033[37m"
        RED = "\033[31m"

        SECTION_WIDTH = 70

        def section(title):
            return [f"{DIM}{title}:{RESET}"]

        def format_scalar(value):
            if value is None:
                return "none"

            if isinstance(value, bool):
                return str(value).lower()

            return str(value)

        def format_field(name, value, colour=SHOW_WHITE):
            def render(value):
                scalar = format_scalar(value)
                return f"{colour}{scalar}{RESET}"

            label = f"{DIM}{name}:{RESET}"

            if isinstance(value, list):
                if not value:
                    return [f"{label} {render('none')}"]

                if len(value) == 1:
                    return [f"{label} {render(value[0])}"]

                return [
                    label
                ] + [
                    f"  {BLUE}[{index}]{RESET} {render(item)}"
                    for index, item in enumerate(value)
                ]

            return [f"{label} {render(value)}"]

        def member_display(member):
            member_type = getattr(member, "type", None)
            member_id = getattr(member, "id", None)

            if member_type is None or member_id is None:
                return str(member)

            member_id = getattr(member_id, "id", member_id)
            member_type = str(member_type).upper()
            version = getattr(member, "version", None)

            if member_type == "REFERENCE" and version is not None:
                member_id = f"{member_id}[v{version}]"

            if member_type == "ACTOR" and version is not None:
                member_id = f"{member_id}[{version}]"

            return f"{member_type}[{member_id}]"

        data = self.to_dict()
        query = show_query_display(
            data["type"],
            data["id"],
            None if data["grouped"] else str(self.object),
            data["grouped"],
            self.version,
        )

        lines = [
            "",
            f"{CYAN}{'=' * SECTION_WIDTH}{RESET}",
            (
                f"{CYAN}{BOLD}INSPECT {query}{RESET}"
                + (
                    f" {RED}| DIVERGENT{RESET}"
                    if data["divergence"]["divergent"]
                    else ""
                )
            ),
            f"{CYAN}{show_identity(data['id'], data['grouped'])}{RESET}",
            f"{CYAN}{'=' * SECTION_WIDTH}{RESET}",
        ]

        lines.extend(section("OBJECT"))
        lines.extend(format_field("type", data["type"]))
        lines.extend(format_field("version", data["version"]))
        id_colour = CYAN
        if self.type == "reference":
            id_colour = SHOW_GREEN if self.downstream else SHOW_RED
        lines.extend(format_field("id", data["id"], id_colour))
        lines.extend(format_field("grouped", data["grouped"]))
        lines.extend(format_field("exists", data["exists"]))

        if data["grouped"]:
            lines.extend(format_field(
                "members",
                [member_display(member) for member in self.object],
                CYAN,
            ))

        if data["divergence"]["divergent"]:
            lines.extend(section("DIVERGENCE"))
            for state in data["divergence"]["states"]:
                recorded = state["recorded_as"]
                reconstructed = state["reconstructed_as"] or {}
                lines.append(
                    f"  {RED}!{state['state']['id']}{RESET}"
                )
                lines.append(
                    f"    {DIM}recorded as:{RESET} "
                    f"{WHITE}{recorded.get('type') or 'unknown'}{RESET}"
                )
                lines.append(
                    f"    {DIM}reconstructed as:{RESET} "
                    f"{WHITE}{reconstructed.get('type') or 'unknown'}{RESET}"
                )

        lines.extend(section("SOURCE"))
        lines.extend(format_field("ocean", data["source_oceans"], CYAN))
        lines.extend(format_field("source", data["source_runners"], CYAN))
        lines.extend(format_field("pipeline", data["source_pipelines"], CYAN))

        if self.type == "exec_record" and self.exists and not self.grouped:
            lines.extend(section("RELATIONSHIPS"))
            lines.extend(format_field(
                "parents",
                [
                    f"EXEC_RECORD[{entry['seq_id']}] "
                    f"{entry['actor']['name']}"
                    for entry in data["parents"]
                ],
                CYAN,
            ))
            lines.extend(format_field(
                "children",
                [
                    f"EXEC_RECORD[{entry['seq_id']}] "
                    f"{entry['actor']['name']}"
                    for entry in data["children"]
                ],
                CYAN,
            ))

        lines.extend(section("HISTORY"))
        if data["history"]:
            history_colour = CYAN
            if self.type == "reference":
                history_colour = (
                    SHOW_GREEN
                    if self.downstream
                    else SHOW_RED
                )
            lines.extend([
                f"  {BLUE}[{index}]{RESET} "
                f"{history_colour}"
                f"{format_scalar(entry)}{RESET}"
                for index, entry in enumerate(data["history"])
            ])
        else:
            lines.append(
                f"{WHITE}history:{RESET} {DIM}none{RESET}"
            )

        lines.extend(section("SEEDS"))
        lines.extend(format_field(
            "upstream_seeds",
            data["upstream_seeds"],
            CYAN,
        ))
        lines.extend(format_field(
            "downstream_seeds",
            data["downstream_seeds"],
            CYAN,
        ))

        output = "\n".join(lines)

        return _show_render(output.split("\n"), color)
    
    @property
    def exists(self):
        return self.object.exists

class InspectedGroup(Inspected):
    def __init__(self, objects, kind, id, history=None, upstream=None, downstream=None, source_oceans=None, source_runners=None, source_pipelines=None, version=None, reads=None, writes=None, trace=None):
        self.object = list(objects)
        self.type = kind
        self.id = id
        self.version = version

        self.history = history if history is not None else []
        self.upstream = upstream if upstream is not None else {}
        self.downstream = downstream if downstream is not None else {}

        self.reads = reads if reads is not None else []
        self.writes = writes if writes is not None else []
        self.source_oceans = source_oceans if source_oceans is not None else []
        self.source_runners = source_runners if source_runners is not None else []
        self.source_pipelines = source_pipelines if source_pipelines is not None else []
        self.grouped = True
        self.trace = trace

    def __repr__(self):
        return (
            f"InspectedGroup({self.type}: {self.id}, "
            f"objects[{len(self.object)}], "
            f"history[{list(self.history)[:3]}]..)"
        )

    @property
    def exists(self):
        return any(object.exists for object in self.object)


def _group_diff_path(path):
    parts = str(path).split(".")

    for index, part in enumerate(parts):
        if "[" in part:
            group = ".".join(parts[: index + 1])
            field = ".".join(parts[index + 1 :])
            return group, field or None

    if len(parts) > 1:
        return ".".join(parts[:-1]), parts[-1]

    return None, str(path)


def _group_diff_entries(entries):
    grouped = {}

    for path, value in entries.items():
        group, field = _group_diff_path(path)
        grouped.setdefault(group, []).append((field, value))

    return grouped


def _append_structured_value(lines, value, indent, reset, dim, white):
    if isinstance(value, dict):
        if not value:
            lines.append(f"{indent}{dim}{{}}{reset}")
            return

        for key, item in value.items():
            label = f"{indent}{dim}{key}:{reset}"
            if isinstance(item, (dict, list, tuple, set, frozenset)):
                lines.append(label)
                _append_structured_value(
                    lines,
                    item,
                    f"{indent}  ",
                    reset,
                    dim,
                    white,
                )
            else:
                lines.append(f"{label} {white}{repr(item)}{reset}")
        return

    if isinstance(value, (set, frozenset)):
        kind = type(value).__name__
        if not value:
            lines.append(f"{indent}{dim}{kind}(){reset}")
            return

        lines.append(f"{indent}{dim}{kind}:{reset}")
        for item in sorted(value, key=repr):
            lines.append(
                f"{indent}  {dim}-{reset} "
                f"{white}{repr(item)}{reset}"
            )
        return

    if isinstance(value, (list, tuple)):
        if not value:
            empty = "()" if isinstance(value, tuple) else "[]"
            lines.append(f"{indent}{dim}{empty}{reset}")
            return

        for index, item in enumerate(value):
            label = f"{indent}[{index}]:"
            if isinstance(item, (dict, list, tuple, set, frozenset)):
                lines.append(f"{dim}{label}{reset}")
                _append_structured_value(
                    lines,
                    item,
                    f"{indent}  ",
                    reset,
                    dim,
                    white,
                )
            else:
                lines.append(
                    f"{dim}{label}{reset} {white}{repr(item)}{reset}"
                )
        return

    lines.append(f"{indent}{white}{repr(value)}{reset}")


def _append_changed_value_lines(
    lines,
    label,
    change,
    indent,
    reset,
    dim,
    white,
):
    before = change["before"]
    after = change["after"]
    type_changed = type(before) is not type(after)
    structured = isinstance(
        before,
        (dict, list, tuple, set, frozenset),
    ) or isinstance(
        after,
        (dict, list, tuple, set, frozenset),
    )

    if structured:
        lines.append(f"{indent}{dim}{label}:{reset}")
        if type_changed:
            lines.append(
                f"{indent}  {dim}type:{reset} "
                f"{white}{type(before).__name__}{reset} "
                f"{dim}->{reset} "
                f"{white}{type(after).__name__}{reset}"
            )

        lines.append(f"{indent}  {dim}before:{reset}")
        _append_structured_value(
            lines,
            before,
            f"{indent}    ",
            reset,
            dim,
            white,
        )
        lines.append(f"{indent}  {dim}after:{reset}")
        _append_structured_value(
            lines,
            after,
            f"{indent}    ",
            reset,
            dim,
            white,
        )
        return

    lines.append(
        f"{indent}{dim}{label}:{reset} "
        f"{white}{repr(before)}{reset} "
        f"{dim}->{reset} "
        f"{white}{repr(after)}{reset}"
    )


def _append_added_or_removed_value_lines(
    lines,
    label,
    value,
    indent,
    label_colour,
    reset,
    dim,
    white,
):
    if isinstance(value, (dict, list, tuple, set, frozenset)):
        lines.append(f"{indent}{label_colour}{label}{reset}:")
        _append_structured_value(
            lines,
            value,
            f"{indent}  ",
            reset,
            dim,
            white,
        )
        return

    lines.append(
        f"{indent}{label_colour}{label}{reset}: "
        f"{white}{repr(value)}{reset}"
    )


class Compared:
    def __init__(
        self,
        before_type,
        after_type,
        before,
        after,
        changed=None,
        added=None,
        removed=None,
    ):
        self.before_type = before_type
        self.after_type = after_type
        self.before = before
        self.after = after
        self.changed = changed or {}
        self.added = added or {}
        self.removed = removed or {}

    @property
    def object_type(self):
        if self.same_type:
            return self.before_type
        return f"{self.before_type} -> {self.after_type}"

    @property
    def same_values(self):
        return not any((self.changed, self.added, self.removed))

    @property
    def same_type(self):
        return self.before_type == self.after_type

    @property
    def same(self):
        return self.same_type and self.same_values

    @property
    def count(self):
        return int(not self.same_type) + sum(
            len(section)
            for section in (self.changed, self.added, self.removed)
        )

    def to_dict(self):
        return {
            "type": "diff",
            "object": self.object_type,
            "before_type": self.before_type,
            "after_type": self.after_type,
            "same": self.same,
            "same_type": self.same_type,
            "same_values": self.same_values,
            "changed": copy.deepcopy(self.changed),
            "added": copy.deepcopy(self.added),
            "removed": copy.deepcopy(self.removed),
        }

    def diff(self, other):
        return _build_compared(self, other)

    def show(self, color=True, simple=False, colour=None):
        color, simple = _show_options(
            color=color,
            simple=simple,
            colour=colour,
        )

        reset = SHOW_RESET if color else ""
        bold = SHOW_BOLD if color else ""
        dim = SHOW_DIM if color else ""
        cyan = SHOW_CYAN if color else ""
        green = SHOW_GREEN if color else ""
        yellow = SHOW_YELLOW if color else ""
        red = SHOW_RED if color else ""
        white = SHOW_WHITE if color else ""

        def _header_lines():
            difference_word = (
                "difference" if self.count == 1 else "differences"
            )
            status = (
                "unchanged"
                if self.same
                else f"{self.count} {difference_word}"
            )
            type_status = "same" if self.same_type else "different"

            return [
                "",
                f"{cyan}{'=' * SHOW_SECTION_WIDTH}{reset}",
                f"{cyan}{bold}DIFF{reset} | "
                f"{self.object_type} | {status}",
                f"{dim}payload:{reset} "
                f"{'same' if self.same_values else 'different'}",
                f"{dim}original types:{reset} {type_status}",
                f"{cyan}{'=' * SHOW_SECTION_WIDTH}{reset}",
            ]

        lines = _header_lines()
        sections = (
            ("CHANGED", self.changed, yellow),
            ("ADDED", self.added, green),
            ("REMOVED", self.removed, red),
        )

        for title, entries, section_colour in sections:
            if simple and not entries:
                continue

            lines.append(f"{dim}{title}:{reset}")
            if not entries:
                lines.append(f"  {dim}none{reset}")
                continue

            grouped = _group_diff_entries(entries)
            for group, group_entries in grouped.items():
                if group is None:
                    for field, value in group_entries:
                        if title == "CHANGED":
                            _append_changed_value_lines(
                                lines,
                                field,
                                value,
                                "  ",
                                reset,
                                dim,
                                white,
                            )
                        else:
                            _append_added_or_removed_value_lines(
                                lines,
                                field,
                                value,
                                "  ",
                                section_colour,
                                reset,
                                dim,
                                white,
                            )
                    continue

                lines.append(f"  {section_colour}{group}:{reset}")
                for field, value in group_entries:
                    indent = "    "
                    if title == "CHANGED":
                        _append_changed_value_lines(
                            lines,
                            field,
                            value,
                            indent,
                            reset,
                            dim,
                            white,
                        )
                    else:
                        _append_added_or_removed_value_lines(
                            lines,
                            field,
                            value,
                            indent,
                            section_colour,
                            reset,
                            dim,
                            white,
                        )

        return _show_render(lines, color)
