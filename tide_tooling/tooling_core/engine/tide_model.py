from __future__ import annotations

"""TIDE relation-kernel projection, canonicalisation, and comparison.

The literal model contract remains the default.  Structural canonicalisation
is an explicit representational view over typed ``I`` and ``O`` incidence.
``tide_logging`` re-exports these helpers for pre-alpha compatibility.
"""

import copy
import inspect
import warnings
from collections import defaultdict

from tide_tooling.tooling_core.engine.tide_query_results import TIDEModelCompared


def _is_tide_model_operand(value):
    if isinstance(value, dict):
        return all(field in value for field in ("E", "S", "I", "O"))

    return hasattr(value, "to_model") and callable(value.to_model)


def _tide_model_document(value, identity_mode="host"):
    if isinstance(value, dict):
        if not _is_tide_model_operand(value):
            raise TypeError(
                "A TIDE Model diff dict must contain E, S, I and O"
            )
        document = copy.deepcopy(value)
        if identity_mode == "structural":
            from tide_tooling.core import TIDE

            return TIDE.from_model(document).to_model(
                include_hashes=False,
                identity_mode="structural",
            )
        return document

    if hasattr(value, "to_model") and callable(value.to_model):
        to_model = value.to_model
        try:
            parameters = inspect.signature(to_model).parameters
        except (TypeError, ValueError):
            parameters = {}

        accepts_kwargs = any(
            parameter.kind == inspect.Parameter.VAR_KEYWORD
            for parameter in parameters.values()
        )
        options = {}
        if accepts_kwargs or "include_hashes" in parameters:
            options["include_hashes"] = False
        if accepts_kwargs or "identity_mode" in parameters:
            options["identity_mode"] = identity_mode

        comparison_options = getattr(
            value,
            "_tide_model_comparison_options",
            None,
        )
        if callable(comparison_options):
            for option_name, option_value in comparison_options().items():
                if accepts_kwargs or option_name in parameters:
                    options[option_name] = option_value

        if options.get("assign_unknown") is True:
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore",
                    message=(
                        "Trace States have no host-identified producer:.*"
                    ),
                    category=RuntimeWarning,
                )
                return to_model(**options)
        return to_model(**options)

    raise TypeError(
        "TIDE Model diff operands must be traces or TIDE Model dicts"
    )


def _tide_incidence_set(entries):
    incidence = set()
    for entry in entries:
        if isinstance(entry, dict):
            if "exec_id" in entry and "state_id" in entry:
                incidence.add((
                    entry["exec_id"],
                    entry["state_id"],
                ))
            else:
                incidence.add((entry["e"], entry["s"]))
        else:
            incidence.add((entry[0], entry[1]))
    return incidence


def _tide_incidence_entry(exec_id, state_id, incidence_format):
    if incidence_format == "explicit":
        return {"exec_id": exec_id, "state_id": state_id}
    if incidence_format == "compact":
        return {"e": exec_id, "s": state_id}
    return [exec_id, state_id]


def _sorted_tide_items(items):
    return tuple(sorted(items, key=lambda item: repr(item)))


def _tide_incidence_sort_key(edge):
    return (
        type(edge[0]).__name__,
        repr(edge[0]),
        type(edge[1]).__name__,
        repr(edge[1]),
    )


def _canonical_tide_structure(
    model,
    incidence_format="explicit",
    return_identity_maps=False,
):
    """Return an ID- and enrichment-independent canonical TIDE kernel.

    Canonicalisation treats Executions and States as two node kinds and
    input/output incidence as two typed edge kinds. It deliberately ignores
    host labels, values, hashes, and all optional enrichment. Aliases order
    each node kind by its ancestry depth. Among nodes at the same depth, the
    canonical search selects the lexicographically smallest complete typed-
    incidence encoding. Supplied array order is not semantic.
    """
    inputs = _tide_incidence_set(model["I"])
    outputs = _tide_incidence_set(model["O"])
    executions = list(_sorted_tide_items(set(model["E"])))
    states = list(_sorted_tide_items(set(model["S"])))
    exec_nodes = [("E", index) for index in range(len(executions))]
    state_nodes = [("S", index) for index in range(len(states))]
    nodes = exec_nodes + state_nodes
    exec_node_by_id = {
        (type(identity), identity): node
        for identity, node in zip(executions, exec_nodes, strict=True)
    }
    state_node_by_id = {
        (type(identity), identity): node
        for identity, node in zip(states, state_nodes, strict=True)
    }

    input_edges = [
        (
            exec_node_by_id[(type(exec_id), exec_id)],
            state_node_by_id[(type(state_id), state_id)],
        )
        for exec_id, state_id in inputs
    ]
    output_edges = [
        (
            exec_node_by_id[(type(exec_id), exec_id)],
            state_node_by_id[(type(state_id), state_id)],
        )
        for exec_id, state_id in outputs
    ]

    producer_by_state = {
        state_node: exec_node
        for exec_node, state_node in output_edges
    }
    adjacency = {
        exec_node: set()
        for exec_node in exec_nodes
    }
    indegree = {
        exec_node: 0
        for exec_node in exec_nodes
    }
    for consumer, state_node in input_edges:
        producer = producer_by_state.get(state_node)
        if producer is None or consumer in adjacency[producer]:
            continue
        adjacency[producer].add(consumer)
        indegree[consumer] += 1

    exec_depth = {
        exec_node: 0
        for exec_node in exec_nodes
    }
    pending = [
        exec_node
        for exec_node in exec_nodes
        if indegree[exec_node] == 0
    ]
    visited = 0
    while pending:
        exec_node = pending.pop()
        visited += 1
        for consumer in adjacency[exec_node]:
            exec_depth[consumer] = max(
                exec_depth[consumer],
                exec_depth[exec_node] + 1,
            )
            indegree[consumer] -= 1
            if indegree[consumer] == 0:
                pending.append(consumer)

    if visited != len(exec_nodes):
        raise ValueError(
            "structural identity canonicalisation requires acyclic "
            "occurrence ancestry"
        )

    state_depth = {
        state_node: (
            0
            if state_node not in producer_by_state
            else exec_depth[producer_by_state[state_node]] + 1
        )
        for state_node in state_nodes
    }
    node_depth = {
        **exec_depth,
        **state_depth,
    }

    neighbours = {node: [] for node in nodes}
    for exec_node, state_node in input_edges:
        neighbours[exec_node].append(("in", "I", state_node))
        neighbours[state_node].append(("out", "I", exec_node))
    for exec_node, state_node in output_edges:
        neighbours[exec_node].append(("out", "O", state_node))
        neighbours[state_node].append(("in", "O", exec_node))

    edge_signatures = defaultdict(list)
    for node, entries in neighbours.items():
        for direction, relation, neighbour in entries:
            edge_signatures[(node, neighbour)].append(
                (direction, relation)
            )
    edge_signatures = {
        pair: tuple(sorted(labels))
        for pair, labels in edge_signatures.items()
    }

    initial_signatures = {
        node: (
            node[0],
            node_depth[node],
            tuple(sorted(
                (direction, relation)
                for direction, relation, _ in neighbours[node]
            )),
        )
        for node in nodes
    }
    signature_order = {
        signature: index
        for index, signature in enumerate(
            sorted(set(initial_signatures.values()))
        )
    }
    initial_colours = {
        node: signature_order[signature]
        for node, signature in initial_signatures.items()
    }

    def refine(colours):
        while True:
            signatures = {
                node: (
                    colours[node],
                    tuple(sorted(
                        (
                            direction,
                            relation,
                            colours[neighbour],
                        )
                        for direction, relation, neighbour
                        in neighbours[node]
                    )),
                )
                for node in nodes
            }
            ordered = {
                signature: index
                for index, signature in enumerate(
                    sorted(set(signatures.values()))
                )
            }
            refined = {
                node: ordered[signature]
                for node, signature in signatures.items()
            }
            if refined == colours:
                return refined
            colours = refined

    best = {"code": None, "exec_order": None, "state_order": None}
    search_count = 0

    def has_automorphism(source, target, colours):
        """Prove whether one current-colour node can replace another."""
        if colours[source] != colours[target]:
            return False

        nodes_by_colour = defaultdict(tuple)
        grouped = defaultdict(list)
        for node, colour in colours.items():
            grouped[colour].append(node)
        nodes_by_colour.update({
            colour: tuple(cell)
            for colour, cell in grouped.items()
        })

        mapping = {source: target}
        used = {target}

        def compatible(left, right):
            if colours[left] != colours[right]:
                return False
            for mapped_left, mapped_right in mapping.items():
                if edge_signatures.get((left, mapped_left), ()) != (
                    edge_signatures.get((right, mapped_right), ())
                ):
                    return False
                if edge_signatures.get((mapped_left, left), ()) != (
                    edge_signatures.get((mapped_right, right), ())
                ):
                    return False
            return True

        def extend():
            if len(mapping) == len(nodes):
                return True

            selected = None
            candidates = None
            for left in nodes:
                if left in mapping:
                    continue
                available = [
                    right
                    for right in nodes_by_colour[colours[left]]
                    if right not in used and compatible(left, right)
                ]
                if not available:
                    return False
                if candidates is None or len(available) < len(candidates):
                    selected = left
                    candidates = available

            for right in candidates:
                mapping[selected] = right
                used.add(right)
                if extend():
                    return True
                used.remove(right)
                del mapping[selected]
            return False

        return extend()

    def visit(colours):
        nonlocal search_count
        search_count += 1
        if search_count > 100_000:
            raise ValueError(
                "structural identity canonicalisation exceeded its "
                "100000-state search limit"
            )

        colours = refine(colours)
        cells = defaultdict(list)
        for node, colour in colours.items():
            cells[colour].append(node)
        unresolved = [
            (colour, cell)
            for colour, cell in cells.items()
            if len(cell) > 1
        ]

        if not unresolved:
            exec_order = sorted(
                exec_nodes,
                key=lambda node: (exec_depth[node], colours[node]),
            )
            state_order = sorted(
                state_nodes,
                key=lambda node: (state_depth[node], colours[node]),
            )
            exec_alias = {
                node: index for index, node in enumerate(exec_order)
            }
            state_alias = {
                node: index for index, node in enumerate(state_order)
            }
            code = (
                tuple(sorted(
                    (exec_alias[exec_node], state_alias[state_node])
                    for exec_node, state_node in input_edges
                )),
                tuple(sorted(
                    (exec_alias[exec_node], state_alias[state_node])
                    for exec_node, state_node in output_edges
                )),
            )
            if best["code"] is None or code < best["code"]:
                best.update({
                    "code": code,
                    "exec_order": exec_order,
                    "state_order": state_order,
                })
            return

        _, cell = min(
            unresolved,
            key=lambda entry: (len(entry[1]), entry[0]),
        )
        next_colour = max(colours.values(), default=-1) + 1
        orbit_representatives = []
        for node in cell:
            if any(
                has_automorphism(representative, node, colours)
                for representative in orbit_representatives
            ):
                continue
            orbit_representatives.append(node)
            branch = dict(colours)
            branch[node] = next_colour
            visit(branch)

    visit(initial_colours)

    exec_alias = {
        node: f"exec_{index}"
        for index, node in enumerate(best["exec_order"])
    }
    state_alias = {
        node: f"state_{index}"
        for index, node in enumerate(best["state_order"])
    }

    canonical_inputs = sorted(
        (
            exec_alias[exec_node],
            state_alias[state_node],
        )
        for exec_node, state_node in input_edges
    )
    canonical_outputs = sorted(
        (
            exec_alias[exec_node],
            state_alias[state_node],
        )
        for exec_node, state_node in output_edges
    )
    canonical = {
        "schema": "TIDE-Model",
        "schema_version": 1,
        "host": model.get("host", "TIDE"),
        "E": [
            exec_alias[node]
            for node in best["exec_order"]
        ],
        "S": [
            state_alias[node]
            for node in best["state_order"]
        ],
        "I": [
            _tide_incidence_entry(
                exec_id,
                state_id,
                incidence_format,
            )
            for exec_id, state_id in canonical_inputs
        ],
        "O": [
            _tide_incidence_entry(
                exec_id,
                state_id,
                incidence_format,
            )
            for exec_id, state_id in canonical_outputs
        ],
    }
    if not return_identity_maps:
        return canonical

    return canonical, {
        "executions": {
            (type(executions[node[1]]), executions[node[1]]): alias
            for node, alias in exec_alias.items()
        },
        "states": {
            (type(states[node[1]]), states[node[1]]): alias
            for node, alias in state_alias.items()
        },
    }


def _build_tide_model_compared(
    subject,
    other,
    identity_mode="host",
):
    from tide_tooling.core import TIDE

    before = _tide_model_document(subject, identity_mode)
    after = _tide_model_document(other, identity_mode)
    before_report = TIDE.validate_model(before)
    after_report = TIDE.validate_model(after)

    before_kernel = {
        "E": set(before["E"]),
        "S": set(before["S"]),
        "I": _tide_incidence_set(before["I"]),
        "O": _tide_incidence_set(before["O"]),
    }
    after_kernel = {
        "E": set(after["E"]),
        "S": set(after["S"]),
        "I": _tide_incidence_set(after["I"]),
        "O": _tide_incidence_set(after["O"]),
    }
    kernel = {
        field: {
            "before_count": len(before_kernel[field]),
            "after_count": len(after_kernel[field]),
            "added": _sorted_tide_items(
                after_kernel[field] - before_kernel[field]
            ),
            "removed": _sorted_tide_items(
                before_kernel[field] - after_kernel[field]
            ),
        }
        for field in ("E", "S", "I", "O")
    }

    before_handoffs = set(before_report.direct_handoffs)
    after_handoffs = set(after_report.direct_handoffs)
    before_reachability = set(before_report.reachability)
    after_reachability = set(after_report.reachability)
    judgments = {
        "direct_handoffs": {
            "before_count": len(before_handoffs),
            "after_count": len(after_handoffs),
            "added": _sorted_tide_items(
                after_handoffs - before_handoffs
            ),
            "removed": _sorted_tide_items(
                before_handoffs - after_handoffs
            ),
        },
        "reachability": {
            "before_count": len(before_reachability),
            "after_count": len(after_reachability),
            "added": _sorted_tide_items(
                after_reachability - before_reachability
            ),
            "removed": _sorted_tide_items(
                before_reachability - after_reachability
            ),
        },
    }

    return TIDEModelCompared(
        before=before,
        after=after,
        before_report=before_report,
        after_report=after_report,
        kernel=kernel,
        judgments=judgments,
        identity_mode=identity_mode,
    )
