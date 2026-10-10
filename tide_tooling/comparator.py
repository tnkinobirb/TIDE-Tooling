"""Private comparison core for finite TIDE Models."""

from __future__ import annotations

import copy

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
from tide_tooling.tooling_core.engine.tide_model import TIDEModel


def _identity_sort_key(value):
    return type(value) is str, value


def _ordered(values):
    return [
        list(value) if isinstance(value, tuple) else value
        for value in sorted(
            values,
            key=lambda value: tuple(
                _identity_sort_key(item)
                for item in value
            ),
        )
    ]


def _map_errors(mapping, name):
    if not isinstance(mapping, dict):
        return [f"{name} map must be a dictionary"]

    errors = []
    targets = []
    for source, target in mapping.items():
        try:
            TIDEModel.identifier(source, f"{name} map source")
        except ValueError as error:
            errors.append(str(error))
        try:
            TIDEModel.identifier(target, f"{name} map target")
            targets.append(target)
        except ValueError as error:
            errors.append(str(error))

    if len(set(targets)) != len(targets):
        errors.append(f"{name} map must be one-to-one")

    return errors


def _incidence(model, relation):
    return {
        (edge["exec_id"], edge["state_id"])
        for edge in model[relation]
    }


def _judgments(executions, inputs, outputs):
    consumers_by_state = {}
    for execution, state in inputs:
        consumers_by_state.setdefault(state, set()).add(execution)

    handoffs = {
        (producer, state, consumer)
        for producer, state in outputs
        for consumer in consumers_by_state.get(state, ())
    }

    reachability = TIDEModel.directed_reachability(executions, handoffs)
    return handoffs, reachability


def _prepare_tide_comparison(
    source,
    target,
    *,
    mapping,
    execution_map,
    state_map,
):
    source = TIDEModel.normalise(source)
    target = TIDEModel.normalise(target)
    source_validity = TIDEModel.report_validity(source)
    target_validity = TIDEModel.report_validity(target)

    for name, report in (
        ("source", source_validity),
        ("target", target_validity),
    ):
        if not report.structurally_valid:
            raise ValueError(
                f"{name} is not a structurally valid TIDE Model: "
                + "; ".join(report["errors"])
            )

    if mapping not in {"structural", "literal", "supplied"}:
        raise ValueError(
            "mapping must be 'structural', 'literal', or 'supplied'"
        )

    maps_present = execution_map is not None or state_map is not None

    if mapping == "supplied":
        if execution_map is None or state_map is None:
            raise TypeError(
                "mapping='supplied' requires both execution_map and state_map"
            )
        return source, target, execution_map, state_map

    if maps_present:
        raise TypeError("Explicit maps require mapping='supplied'")

    if mapping == "structural":
        source = TIDEModel.normalise(TIDEModel.canonicalise(source))
        target = TIDEModel.normalise(TIDEModel.canonicalise(target))

    execution_ids = set(source["E"]) | set(target["E"])
    state_ids = set(source["S"]) | set(target["S"])

    execution_map = {exec_id: exec_id for exec_id in execution_ids}
    state_map = {state_id: state_id for state_id in state_ids}
    return source, target, execution_map, state_map


def _compare_tide_models(
    source,
    target,
    *,
    mapping,
    execution_map,
    state_map,
):
    source_validity = TIDEModel.report_validity(source)
    target_validity = TIDEModel.report_validity(target)
    mapping_errors = [
        *_map_errors(execution_map, "Execution"),
        *_map_errors(state_map, "State"),
    ]

    report = {
        "type": "tide_model_comparison",
        "mapping": mapping,
        "assessable": not mapping_errors,
        "mapping_errors": mapping_errors,
        "models": {
            "source": source_validity.to_dict(),
            "target": target_validity.to_dict(),
        },
    }

    if mapping_errors:
        report.update({
            "mapped_scope": None,
            "mapped_identities_absent": None,
            "outside": None,
            "deltas": None,
            "preservation": None,
            "reflection": None,
            "judgments_correspond": None,
            "conformant": None,
            "identity_coverage": None,
            "complete_agreement": None,
        })
        return TIDEModelComparisonReport(report)

    source_executions = set(source["E"])
    source_states = set(source["S"])
    target_executions = set(target["E"])
    target_states = set(target["S"])
    mapped_source_executions = source_executions & set(execution_map)
    mapped_source_states = source_states & set(state_map)
    mapped_target_executions = target_executions & set(execution_map.values())
    mapped_target_states = target_states & set(state_map.values())

    source_inputs = _incidence(source, "I")
    source_outputs = _incidence(source, "O")
    target_inputs = _incidence(target, "I")
    target_outputs = _incidence(target, "O")

    mapped_source_inputs = {
        edge for edge in source_inputs
        if edge[0] in mapped_source_executions
        and edge[1] in mapped_source_states
    }
    mapped_source_outputs = {
        edge for edge in source_outputs
        if edge[0] in mapped_source_executions
        and edge[1] in mapped_source_states
    }
    mapped_target_inputs = {
        edge for edge in target_inputs
        if edge[0] in mapped_target_executions
        and edge[1] in mapped_target_states
    }
    mapped_target_outputs = {
        edge for edge in target_outputs
        if edge[0] in mapped_target_executions
        and edge[1] in mapped_target_states
    }

    reflected_source_inputs = {
        (execution_map[execution], state_map[state])
        for execution, state in mapped_source_inputs
    }
    reflected_source_outputs = {
        (execution_map[execution], state_map[state])
        for execution, state in mapped_source_outputs
    }

    delta_i_minus = reflected_source_inputs - mapped_target_inputs
    delta_i_plus = mapped_target_inputs - reflected_source_inputs
    delta_o_minus = reflected_source_outputs - mapped_target_outputs
    delta_o_plus = mapped_target_outputs - reflected_source_outputs

    source_handoffs, source_reachability = _judgments(
        mapped_source_executions,
        mapped_source_inputs,
        mapped_source_outputs,
    )
    target_handoffs, target_reachability = _judgments(
        mapped_target_executions,
        mapped_target_inputs,
        mapped_target_outputs,
    )
    reflected_source_handoffs = {
        (
            execution_map[producer],
            state_map[state],
            execution_map[consumer],
        )
        for producer, state, consumer in source_handoffs
    }
    reflected_source_reachability = {
        (execution_map[source_id], execution_map[target_id])
        for source_id, target_id in source_reachability
    }

    source_coverage = (
        source_executions == set(execution_map)
        and source_states == set(state_map)
    )
    target_coverage = (
        target_executions == set(execution_map.values())
        and target_states == set(state_map.values())
    )
    conformant = not any((
        delta_i_minus,
        delta_i_plus,
        delta_o_minus,
        delta_o_plus,
    ))

    report.update({
        "mapped_scope": {
            "source": {
                "executions": sorted(
                    mapped_source_executions,
                    key=_identity_sort_key,
                ),
                "states": sorted(
                    mapped_source_states,
                    key=_identity_sort_key,
                ),
            },
            "target": {
                "executions": sorted(
                    mapped_target_executions,
                    key=_identity_sort_key,
                ),
                "states": sorted(
                    mapped_target_states,
                    key=_identity_sort_key,
                ),
            },
        },
        "mapped_identities_absent": {
            "source": {
                "executions": sorted(
                    set(execution_map) - source_executions,
                    key=_identity_sort_key,
                ),
                "states": sorted(
                    set(state_map) - source_states,
                    key=_identity_sort_key,
                ),
            },
            "target": {
                "executions": sorted(
                    set(execution_map.values()) - target_executions,
                    key=_identity_sort_key,
                ),
                "states": sorted(
                    set(state_map.values()) - target_states,
                    key=_identity_sort_key,
                ),
            },
        },
        "outside": {
            "source": {
                "executions": sorted(
                    source_executions - set(execution_map),
                    key=_identity_sort_key,
                ),
                "states": sorted(
                    source_states - set(state_map),
                    key=_identity_sort_key,
                ),
                "I": _ordered(source_inputs - mapped_source_inputs),
                "O": _ordered(source_outputs - mapped_source_outputs),
            },
            "target": {
                "executions": sorted(
                    target_executions - set(execution_map.values()),
                    key=_identity_sort_key,
                ),
                "states": sorted(
                    target_states - set(state_map.values()),
                    key=_identity_sort_key,
                ),
                "I": _ordered(target_inputs - mapped_target_inputs),
                "O": _ordered(target_outputs - mapped_target_outputs),
            },
        },
        "deltas": {
            "I_minus": _ordered(delta_i_minus),
            "I_plus": _ordered(delta_i_plus),
            "O_minus": _ordered(delta_o_minus),
            "O_plus": _ordered(delta_o_plus),
        },
        "preservation": {
            "I": not delta_i_minus,
            "O": not delta_o_minus,
        },
        "reflection": {
            "I": not delta_i_plus,
            "O": not delta_o_plus,
        },
        "judgments_correspond": (
            reflected_source_handoffs == target_handoffs
            and reflected_source_reachability == target_reachability
        ),
        "conformant": conformant,
        "identity_coverage": {
            "source": source_coverage,
            "target": target_coverage,
        },
        "complete_agreement": (
            conformant and source_coverage and target_coverage
        ),
    })
    return TIDEModelComparisonReport(report)


class TIDEModelComparisonReport(dict):
    """Mapped semantic comparison between two TIDE Models."""

    @property
    def assessable(self):
        return self["assessable"]

    @property
    def conformant(self):
        return self["conformant"]

    @property
    def complete_agreement(self):
        return self["complete_agreement"]

    @property
    def judgments_correspond(self):
        return self["judgments_correspond"]

    def to_dict(self):
        return copy.deepcopy(dict(self))

    @staticmethod
    def _append_values(
        lines,
        values,
        *,
        render,
        marker,
        colour,
        reset,
        dim,
        white,
        indent="    ",
    ):
        if not values:
            lines.append(f"{indent}{dim}none{reset}")
            return
        for value in values:
            lines.append(
                f"{indent}{colour}{marker}{reset} {white}{render(value)}{reset}"
            )

    def show(self, color=True, simple=False, colour=None, full=False):
        if not isinstance(full, bool):
            raise TypeError("show full must be a bool")

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
        elif self.complete_agreement:
            status, status_colour = "COMPLETE AGREEMENT", green
        elif self.conformant:
            status, status_colour = "CONFORMANT", green
        else:
            status, status_colour = "NON-CONFORMANT", yellow

        source = self["models"]["source"]
        target = self["models"]["target"]
        source_counts = source["counts"]
        target_counts = target["counts"]
        lines = [
            "",
            f"{cyan}{'=' * SHOW_SECTION_WIDTH}{reset}",
            f"{cyan}{bold}TIDE MODEL COMPARE{reset} | "
            f"{status_colour}{status}{reset} | "
            f"{source_counts['executions']}E/{source_counts['states']}S "
            f"{dim}→{reset} "
            f"{target_counts['executions']}E/{target_counts['states']}S",
            f"{dim}mapping: {self['mapping']} | "
            f"host: {source.get('host') or 'unspecified'} → "
            f"{target.get('host') or 'unspecified'}{reset}",
            f"{cyan}{'=' * SHOW_SECTION_WIDTH}{reset}",
            f"{dim}SUMMARY:{reset}",
            f"  source validity: "
            f"{green if source['semantically_valid'] else red}"
            f"{'valid' if source['semantically_valid'] else 'invalid'}{reset}",
            f"  target validity: "
            f"{green if target['semantically_valid'] else red}"
            f"{'valid' if target['semantically_valid'] else 'invalid'}{reset}",
        ]

        if not self.assessable:
            lines.append(f"{dim}MAPPING ERRORS:{reset}")
            for error in self["mapping_errors"]:
                lines.append(f"  {red}{error}{reset}")
            return _show_render(lines, color)

        lines.extend([
            f"  mapped incidence: "
            f"{green if self.conformant else yellow}"
            f"{'corresponds' if self.conformant else 'differs'}{reset}",
            f"  preservation: I "
            f"{'yes' if self['preservation']['I'] else 'no'} | O "
            f"{'yes' if self['preservation']['O'] else 'no'}",
            f"  reflection: I "
            f"{'yes' if self['reflection']['I'] else 'no'} | O "
            f"{'yes' if self['reflection']['O'] else 'no'}",
            f"  judgments: "
            f"{'correspond' if self.judgments_correspond else 'differ'}",
            f"  identity coverage: source "
            f"{'complete' if self['identity_coverage']['source'] else 'partial'}"
            f" | target "
            f"{'complete' if self['identity_coverage']['target'] else 'partial'}",
            f"  complete agreement: "
            f"{'yes' if self.complete_agreement else 'no'}",
        ])

        if not full:
            return _show_render(lines, color)

        absent = self["mapped_identities_absent"]
        if not simple or any(
            values
            for side in absent.values()
            for values in side.values()
        ):
            lines.extend(["", f"{dim}MAPPED IDENTITIES ABSENT:{reset}"])
            for side_name, side in absent.items():
                if simple and not any(side.values()):
                    continue
                lines.append(f"  {dim}{side_name.upper()}:{reset}")
                for label, key in (
                    ("Executions", "executions"),
                    ("States", "states"),
                ):
                    values = side[key]
                    if simple and not values:
                        continue
                    lines.append(f"    {dim}{label}:{reset}")
                    self._append_values(
                        lines,
                        values,
                        render=str,
                        marker="!",
                        colour=yellow,
                        reset=reset,
                        dim=dim,
                        white=white,
                        indent="      ",
                    )

        lines.append("")
        lines.append(f"{dim}MAPPED INCIDENCE:{reset}")
        for name, relation, marker, marker_colour in (
            ("I_minus", "I", "-", red),
            ("O_minus", "O", "-", red),
            ("I_plus", "I", "+", green),
            ("O_plus", "O", "+", green),
        ):
            values = self["deltas"][name]
            if simple and not values:
                continue
            lines.append(f"  {dim}{name}:{reset}")
            self._append_values(
                lines,
                values,
                render=(
                    (lambda value: f"{value[0]} ← {value[1]}")
                    if relation == "I"
                    else (lambda value: f"{value[0]} → {value[1]}")
                ),
                marker=marker,
                colour=marker_colour,
                reset=reset,
                dim=dim,
                white=white,
            )

        if self["mapping"] == "supplied":
            outside = self["outside"]
            if not simple or any(
                values
                for side in outside.values()
                for values in side.values()
            ):
                lines.extend(["", f"{dim}OUTSIDE MAPPING SCOPE:{reset}"])
                for side_name, side in outside.items():
                    if simple and not any(side.values()):
                        continue
                    lines.append(f"  {dim}{side_name.upper()}:{reset}")
                    for label, key, render in (
                        ("Executions", "executions", str),
                        ("States", "states", str),
                        (
                            "Input incidence",
                            "I",
                            lambda value: f"{value[0]} ← {value[1]}",
                        ),
                        (
                            "Output incidence",
                            "O",
                            lambda value: f"{value[0]} → {value[1]}",
                        ),
                    ):
                        values = side[key]
                        if simple and not values:
                            continue
                        lines.append(f"    {dim}{label}:{reset}")
                        self._append_values(
                            lines,
                            values,
                            render=render,
                            marker="~",
                            colour=yellow,
                            reset=reset,
                            dim=dim,
                            white=white,
                            indent="      ",
                        )

        return _show_render(lines, color)
