"""Explicit-map comparison for finite TIDE Model structures.

This module integrates the reference comparator shipped with the TIDE Model
artifact.  It deliberately does not discover, infer, repair, or extend an
identity map: callers supply the Execution and State correspondences.
"""

from __future__ import annotations

import copy
import inspect
import json
import warnings
from collections import defaultdict
from os import PathLike
from pathlib import Path

from tide_tooling.tooling_core.engine.tide_rendering import (
    SHOW_BOLD,
    SHOW_CYAN,
    SHOW_DIM,
    SHOW_GREEN,
    SHOW_RED,
    SHOW_RESET,
    SHOW_SECTION_WIDTH,
    SHOW_WHITE,
    SHOW_YELLOW,
    _show_options,
    _show_render,
)

__all__ = ["TIDEComparatorReport", "comparator"]


def _identity(value, location):
    if not isinstance(value, str) or not value:
        raise ValueError(f"{location} must be a non-empty string")
    return value


def _incidence_pair(entry, relation, index, name):
    location = f"{name}.{relation}[{index}]"
    if isinstance(entry, dict):
        if "exec_id" in entry and "state_id" in entry:
            execution, state = entry["exec_id"], entry["state_id"]
        elif "e" in entry and "s" in entry:
            execution, state = entry["e"], entry["s"]
        else:
            raise ValueError(
                f"{location} must contain exec_id/state_id or e/s"
            )
    elif isinstance(entry, (list, tuple)) and len(entry) == 2:
        execution, state = entry
    else:
        raise ValueError(
            f"{location} must be [execution, state] or an incidence object"
        )
    return (
        _identity(execution, f"{location} Execution"),
        _identity(state, f"{location} State"),
    )


def _call_to_model(candidate):
    to_model = candidate.to_model
    try:
        parameters = inspect.signature(to_model).parameters
    except (TypeError, ValueError):
        parameters = {}
    accepts_kwargs = any(
        parameter.kind == inspect.Parameter.VAR_KEYWORD
        for parameter in parameters.values()
    )
    options = {}
    for name, value in (
        ("include_hashes", False),
        ("incidence_format", "pairs"),
        ("identity_mode", "host"),
    ):
        if accepts_kwargs or name in parameters:
            options[name] = value

    comparison_options = getattr(
        candidate,
        "_tide_model_comparison_options",
        None,
    )
    if callable(comparison_options):
        for name, value in comparison_options().items():
            if accepts_kwargs or name in parameters:
                options[name] = value

    if options.get("assign_unknown") is True:
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message="Trace States have no host-identified producer:.*",
                category=RuntimeWarning,
            )
            return to_model(**options)
    return to_model(**options)


def _load_json(candidate, name):
    if isinstance(candidate, bytes):
        try:
            return json.loads(candidate.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError(f"{name} is not valid UTF-8 JSON: {error}") from error

    if isinstance(candidate, PathLike):
        path = Path(candidate)
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError(f"Could not load {name} JSON from {path}: {error}") from error

    if isinstance(candidate, str):
        stripped = candidate.lstrip()
        if stripped.startswith("{"):
            try:
                return json.loads(candidate)
            except json.JSONDecodeError as error:
                raise ValueError(f"{name} is not valid JSON: {error}") from error
        path = Path(candidate)
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError(f"Could not load {name} JSON from {path}: {error}") from error

    return candidate


def _import_structure(candidate, name):
    """Normalize a Trace, model dict, JSON text/bytes, or JSON file path."""
    candidate = _load_json(candidate, name)
    if not isinstance(candidate, dict) and hasattr(candidate, "to_model"):
        candidate = _call_to_model(candidate)
    if not isinstance(candidate, dict):
        raise ValueError(
            f"{name} must be a Trace, TIDE Model dictionary, or JSON input"
        )
    if "I" not in candidate or "O" not in candidate:
        raise ValueError(f"{name} must contain I and O")

    structure = {}
    for relation in ("I", "O"):
        entries = candidate[relation]
        if not isinstance(entries, list):
            raise ValueError(f"{name}.{relation} must be a JSON list")
        structure[relation] = {
            _incidence_pair(entry, relation, index, name)
            for index, entry in enumerate(entries)
        }
    return structure, candidate.get("host")


def _map_errors(mapping, name):
    if not isinstance(mapping, dict):
        return [f"{name} map must be a dictionary"]
    errors = []
    targets = []
    for source, target in mapping.items():
        if not isinstance(source, str) or not source:
            errors.append(f"{name} map source must be a non-empty string")
        if not isinstance(target, str) or not target:
            errors.append(f"{name} map target must be a non-empty string")
        else:
            targets.append(target)
    if len(set(targets)) != len(targets):
        errors.append(f"{name} map must be one-to-one")
    return errors


def _carriers(structure):
    incidence = structure["I"] | structure["O"]
    return (
        {execution for execution, _ in incidence},
        {state for _, state in incidence},
    )


def _judgment_sets(inputs, outputs):
    handoffs = {
        (producer, state, consumer)
        for producer, state in outputs
        for consumer, consumed_state in inputs
        if state == consumed_state
    }
    adjacency = defaultdict(set)
    for producer, _, consumer in handoffs:
        adjacency[producer].add(consumer)

    executions = {execution for execution, _ in inputs | outputs}
    reachability = set()
    for start in executions:
        pending = list(adjacency[start])
        visited = set()
        while pending:
            current = pending.pop()
            if current in visited:
                continue
            visited.add(current)
            reachability.add((start, current))
            pending.extend(adjacency[current])
    return handoffs, reachability


def _ordered(values):
    return tuple(sorted(values))


def _validity_report(structure):
    executions, states = _carriers(structure)
    producers = defaultdict(set)
    for execution, state in structure["O"]:
        producers[state].add(execution)
    producer_counts = {state: len(producers[state]) for state in states}
    unique_creation = all(count == 1 for count in producer_counts.values())
    handoffs, reachability = _judgment_sets(structure["I"], structure["O"])
    cyclic_executions = tuple(sorted(
        execution
        for execution in executions
        if (execution, execution) in reachability
    ))
    return {
        "valid": unique_creation and not cyclic_executions,
        "unique_creation": unique_creation,
        "acyclic_ancestry": not cyclic_executions,
        "producer_count_errors": {
            state: count
            for state, count in sorted(producer_counts.items())
            if count != 1
        },
        "cyclic_executions": cyclic_executions,
        "counts": {
            "executions": len(executions),
            "states": len(states),
            "inputs": len(structure["I"]),
            "outputs": len(structure["O"]),
            "direct_handoffs": len(handoffs),
            "reachability": len(reachability),
        },
    }


def _judgment_report(structure):
    handoffs, reachability = _judgment_sets(structure["I"], structure["O"])
    return {
        "direct_handoffs": _ordered(handoffs),
        "reachability": _ordered(reachability),
    }


def _json_value(value):
    if isinstance(value, tuple):
        return [_json_value(item) for item in value]
    if isinstance(value, list):
        return [_json_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _json_value(item) for key, item in value.items()}
    return copy.deepcopy(value)


class TIDEComparatorReport(dict):
    """Mapping-compatible explicit-correspondence comparison report."""

    @property
    def assessable(self):
        return self["assessable"]

    @property
    def conformant(self):
        return self["conformant"]

    @property
    def whole_kernel_equal(self):
        return self["whole_kernel_equal"]

    @property
    def mapped_judgments_correspond(self):
        return self["judgments"].get("mapped_correspond")

    def to_dict(self):
        return _json_value(dict(self))

    @staticmethod
    def _render_incidence(value, relation):
        if relation == "I":
            return f"{value[0]} <- {value[1]}"
        return f"{value[0]} -> {value[1]}"

    @staticmethod
    def _append_values(lines, values, render, marker, colour, reset, dim, white):
        if not values:
            lines.append(f"  {dim}none{reset}")
            return
        for value in values:
            lines.append(
                f"  {colour}{marker}{reset} {white}{render(value)}{reset}"
            )

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

        if not self.assessable:
            status, status_colour = "UNASSESSABLE", red
        elif self.conformant:
            status, status_colour = "CONFORMANT", green
        else:
            status, status_colour = "NON-CONFORMANT", yellow

        k1_counts = self["validity"]["K1"]["counts"]
        k2_counts = self["validity"]["K2"]["counts"]
        source_host = self["sources"]["K1"].get("host") or "unspecified"
        target_host = self["sources"]["K2"].get("host") or "unspecified"
        lines = [
            "",
            f"{cyan}{'=' * SHOW_SECTION_WIDTH}{reset}",
            f"{cyan}{bold}TIDE COMPARATOR{reset} | "
            f"{status_colour}{status}{reset} | "
            f"{k1_counts['executions']}E/{k1_counts['states']}S "
            f"{dim}\u2192{reset} {k2_counts['executions']}E/{k2_counts['states']}S",
            f"{dim}explicit supplied Execution and State maps | "
            f"host: {source_host} \u2192 {target_host}{reset}",
            f"{cyan}{'=' * SHOW_SECTION_WIDTH}{reset}",
        ]

        if self["mapping_errors"]:
            lines.append(f"{dim}MAPPING ERRORS:{reset}")
            for error in self["mapping_errors"]:
                lines.append(f"  {red}{error}{reset}")

        lines.append(f"{dim}SUMMARY:{reset}")
        for name in ("K1", "K2"):
            valid = self["validity"][name]["valid"]
            lines.append(
                f"  {name} validity: "
                f"{green if valid else red}{'valid' if valid else 'invalid'}{reset}"
            )
        if self.assessable:
            lines.extend([
                f"  mapped incidence: "
                f"{green if self.conformant else yellow}"
                f"{'corresponds' if self.conformant else 'differs'}{reset}",
                f"  mapped judgments: "
                f"{green if self.mapped_judgments_correspond else yellow}"
                f"{'correspond' if self.mapped_judgments_correspond else 'differ'}{reset}",
                f"  whole kernel: "
                f"{green if self.whole_kernel_equal else dim}"
                f"{'equal' if self.whole_kernel_equal else 'not established'}{reset}",
            ])

        if not self.assessable:
            return _show_render(lines, color=color)

        differences = self["judgments"]["mapped_differences"]
        sections = (
            ("DIRECT HANDOFFS MISSING", differences["handoffs_missing"],
             lambda value: f"{value[0]} --{value[1]}--> {value[2]}", "-", red),
            ("DIRECT HANDOFFS UNSUPPORTED", differences["handoffs_unsupported"],
             lambda value: f"{value[0]} --{value[1]}--> {value[2]}", "+", green),
            ("REACHABILITY MISSING", differences["reachability_missing"],
             lambda value: f"{value[0]} \u2192 {value[1]}", "-", red),
            ("REACHABILITY UNSUPPORTED", differences["reachability_unsupported"],
             lambda value: f"{value[0]} \u2192 {value[1]}", "+", green),
        )
        for title, values, render, marker, marker_colour in sections:
            if simple and not values:
                continue
            lines.append(f"{dim}{title}:{reset}")
            self._append_values(
                lines, values, render, marker, marker_colour,
                reset, dim, white,
            )

        lines.append(f"{dim}MAPPED KERNEL DIFFERENCES:{reset}")
        for name, relation, marker, marker_colour in (
            ("I_minus", "I", "-", red),
            ("I_plus", "I", "+", green),
            ("O_minus", "O", "-", red),
            ("O_plus", "O", "+", green),
        ):
            values = self["deltas"][name]
            if simple and not values:
                continue
            lines.append(f"  {dim}{name}:{reset}")
            if not values:
                lines.append(f"    {dim}none{reset}")
            for value in values:
                lines.append(
                    f"    {marker_colour}{marker}{reset} {white}"
                    f"{self._render_incidence(value, relation)}{reset}"
                )

        if not simple:
            lines.append(f"{dim}SCOPE:{reset}")
            for name in ("K1", "K2"):
                scope = self["mapped_scope"][name]
                outside = self["outside"][name]
                absent = self["mapped_identities_absent"][name]
                lines.append(
                    f"  {name}: {len(scope['Executions'])} mapped executions, "
                    f"{len(scope['States'])} mapped states | "
                    f"outside {len(outside['I'])}I/{len(outside['O'])}O | "
                    f"absent {len(absent['Executions'])}E/"
                    f"{len(absent['States'])}S"
                )
        return _show_render(lines, color=color)


def comparator(source, target, *, execution_map, state_map):
    """Compare two normalized TIDE structures under supplied identity maps.

    ``source`` and ``target`` may independently be a Trace/TIDE object, a
    TIDE Model dictionary, JSON text/bytes, or a path to a JSON model.  The
    maps are mandatory and are never inferred or extended.
    """
    k1, source_host = _import_structure(source, "K1")
    k2, target_host = _import_structure(target, "K2")
    validity = {"K1": _validity_report(k1), "K2": _validity_report(k2)}
    judgments = {"K1": _judgment_report(k1), "K2": _judgment_report(k2)}
    mapping_errors = [
        *_map_errors(execution_map, "Execution"),
        *_map_errors(state_map, "State"),
    ]
    base = {
        "type": "tide_model_comparison",
        "sources": {
            "K1": {"host": source_host},
            "K2": {"host": target_host},
        },
        "assessable": not mapping_errors,
        "mapping_errors": mapping_errors,
        "validity": validity,
        "judgments": judgments,
    }
    if mapping_errors:
        base.update({
            "conformant": None,
            "carrier_coverage": {"K1": None, "K2": None},
            "whole_kernel_equal": None,
        })
        return TIDEComparatorReport(base)

    e1, s1 = _carriers(k1)
    e2, s2 = _carriers(k2)
    d_e, r_e = set(execution_map), set(execution_map.values())
    d_s, r_s = set(state_map), set(state_map.values())
    e1_m, e2_m = e1 & d_e, e2 & r_e
    s1_m, s2_m = s1 & d_s, s2 & r_s

    i1_m = {(e, s) for e, s in k1["I"] if e in e1_m and s in s1_m}
    o1_m = {(e, s) for e, s in k1["O"] if e in e1_m and s in s1_m}
    i2_m = {(e, s) for e, s in k2["I"] if e in e2_m and s in s2_m}
    o2_m = {(e, s) for e, s in k2["O"] if e in e2_m and s in s2_m}
    i1_out, o1_out = k1["I"] - i1_m, k1["O"] - o1_m
    i2_out, o2_out = k2["I"] - i2_m, k2["O"] - o2_m
    f_i1_m = {(execution_map[e], state_map[s]) for e, s in i1_m}
    f_o1_m = {(execution_map[e], state_map[s]) for e, s in o1_m}
    delta_i_minus, delta_i_plus = f_i1_m - i2_m, i2_m - f_i1_m
    delta_o_minus, delta_o_plus = f_o1_m - o2_m, o2_m - f_o1_m
    conformant = not any(
        (delta_i_minus, delta_i_plus, delta_o_minus, delta_o_plus)
    )

    h1_m, r1_m = _judgment_sets(i1_m, o1_m)
    h2_m, r2_m = _judgment_sets(i2_m, o2_m)
    f_h1_m = {
        (execution_map[p], state_map[s], execution_map[c])
        for p, s, c in h1_m
    }
    f_r1_m = {
        (execution_map[source_id], execution_map[target_id])
        for source_id, target_id in r1_m
    }
    k1_covered = e1 <= d_e and s1 <= d_s
    k2_covered = e2 <= r_e and s2 <= r_s

    judgments.update({
        "mapped_correspond": f_h1_m == h2_m and f_r1_m == r2_m,
        "mapped_differences": {
            "handoffs_missing": _ordered(f_h1_m - h2_m),
            "handoffs_unsupported": _ordered(h2_m - f_h1_m),
            "reachability_missing": _ordered(f_r1_m - r2_m),
            "reachability_unsupported": _ordered(r2_m - f_r1_m),
        },
    })
    base.update({
        "mapped_scope": {
            "K1": {"Executions": tuple(sorted(e1_m)), "States": tuple(sorted(s1_m))},
            "K2": {"Executions": tuple(sorted(e2_m)), "States": tuple(sorted(s2_m))},
        },
        "mapped_identities_absent": {
            "K1": {"Executions": tuple(sorted(d_e - e1)), "States": tuple(sorted(d_s - s1))},
            "K2": {"Executions": tuple(sorted(r_e - e2)), "States": tuple(sorted(r_s - s2))},
        },
        "deltas": {
            "I_minus": _ordered(delta_i_minus),
            "I_plus": _ordered(delta_i_plus),
            "O_minus": _ordered(delta_o_minus),
            "O_plus": _ordered(delta_o_plus),
        },
        "preservation": {"I": not delta_i_minus, "O": not delta_o_minus},
        "reflection": {"I": not delta_i_plus, "O": not delta_o_plus},
        "outside": {
            "K1": {"I": _ordered(i1_out), "O": _ordered(o1_out)},
            "K2": {"I": _ordered(i2_out), "O": _ordered(o2_out)},
        },
        "conformant": conformant,
        "carrier_coverage": {"K1": k1_covered, "K2": k2_covered},
        "whole_kernel_equal": conformant and k1_covered and k2_covered,
    })
    return TIDEComparatorReport(base)
