from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ExecSig:
    ins: tuple
    outs: tuple


@dataclass(frozen=True)
class StateSig:
    prods: tuple
    cons: tuple


class TIDECanonicaliser:
    """Canonically label a validated TIDE relation kernel."""

    def __init__(self, model):
        # Use distinct string labels internally; retain the original typed IDs.
        self.original_ids = {
            field: {repr(identity): identity for identity in model[field]}
            for field in ("E", "S")
        }
        self.exec_sigs, self.state_sigs = self._kernel_signatures({
            **{field: list(identities) for field, identities in self.original_ids.items()},
            **{relation: [(repr(e), repr(s)) for e, s in self._normalise_incidence(model[relation])]
               for relation in ("I", "O")},
        })

    def canonicalise(self, return_identity_maps=False):
        components = self._connected_components()

        # A connected Model uses the exact canonical search directly.
        if len(components) > 1:
            candidate = self._canonicalise_components(components)
            return self._emit(candidate, return_identity_maps)

        exec_colours, state_colours = self._initial_colours()
        candidate = self._resolve_ties(exec_colours, state_colours)
        return self._emit(candidate, return_identity_maps)

    def _connected_components(self):
        # Connect Executions and States through either I or O incidence.
        neighbours = {
            **{("E", execution): set() for execution in self.exec_sigs},
            **{("S", state): set() for state in self.state_sigs},
        }

        for execution, signature in self.exec_sigs.items():
            for state in (*signature.ins, *signature.outs):
                neighbours[("E", execution)].add(("S", state))
                neighbours[("S", state)].add(("E", execution))

        unseen = set(neighbours)
        components = []

        while unseen:
            pending = [unseen.pop()]
            executions = set()
            states = set()

            while pending:
                carrier, identity = pending.pop()

                if carrier == "E":
                    executions.add(identity)
                else:
                    states.add(identity)

                discovered = neighbours[(carrier, identity)] & unseen
                unseen.difference_update(discovered)
                pending.extend(discovered)

            components.append((executions, states))

        return components

    def _canonicalise_components(self, components):
        canonical_components = []

        for executions, states in components:
            # Canonicalise this independent ESIO component.
            component = {
                "E": list(executions),
                "S": list(states),
                "I": [
                    (execution, state)
                    for execution in executions
                    for state in self.exec_sigs[execution].ins
                ],
                "O": [
                    (execution, state)
                    for execution in executions
                    for state in self.exec_sigs[execution].outs
                ],
            }

            canonical, identity_maps = TIDECanonicaliser(
                component
            ).canonicalise(return_identity_maps=True)

            exec_position = {
                identity: index
                for index, identity in enumerate(canonical["E"])
            }
            state_position = {
                identity: index
                for index, identity in enumerate(canonical["S"])
            }

            ordered_executions = tuple(sorted(
                executions,
                key=lambda identity: exec_position[
                    identity_maps["executions"][identity]
                ],
            ))
            ordered_states = tuple(sorted(
                states,
                key=lambda identity: state_position[
                    identity_maps["states"][identity]
                ],
            ))

            # Equal canonical components may occur in any structural order.
            signature = (
                len(canonical["E"]),
                len(canonical["S"]),
                tuple(map(tuple, canonical["I"])),
                tuple(map(tuple, canonical["O"])),
            )
            stable_tie = (
                tuple(sorted(executions)),
                tuple(sorted(states)),
            )

            canonical_components.append((
                signature,
                stable_tie,
                ordered_executions,
                ordered_states,
            ))

        canonical_components.sort(
            key=lambda component: (component[0], component[1])
        )

        ordered_executions = tuple(
            execution
            for component in canonical_components
            for execution in component[2]
        )
        ordered_states = tuple(
            state
            for component in canonical_components
            for state in component[3]
        )

        exec_index = {
            execution: index
            for index, execution in enumerate(ordered_executions)
        }
        state_index = {
            state: index
            for index, state in enumerate(ordered_states)
        }

        inputs = tuple(sorted(
            (exec_index[execution], state_index[state])
            for execution in ordered_executions
            for state in self.exec_sigs[execution].ins
        ))
        outputs = tuple(sorted(
            (exec_index[execution], state_index[state])
            for execution in ordered_executions
            for state in self.exec_sigs[execution].outs
        ))

        return (
            (inputs, outputs),
            ordered_executions,
            ordered_states,
        )

    @staticmethod
    def _normalise_incidence(entries):
        pairs = set()
        for entry in entries:
            if isinstance(entry, dict):
                execution = entry.get("exec_id", entry.get("e"))
                state = entry.get("state_id", entry.get("s"))
            else:
                execution, state = entry
            pairs.add((execution, state))
        return pairs

    @classmethod
    def _kernel_signatures(cls, model):
        # Normalise carriers and incidence.
        executions = sorted(set(model["E"]))
        states = sorted(set(model["S"]))
        inputs = sorted(cls._normalise_incidence(model["I"]))
        outputs = sorted(cls._normalise_incidence(model["O"]))

        # Prepare each node's relations.
        exec_ins = {execution: [] for execution in executions}
        exec_outs = {execution: [] for execution in executions}
        state_prods = {state: [] for state in states}
        state_cons = {state: [] for state in states}

        # Record consumption.
        for execution, state in inputs:
            exec_ins[execution].append(state)
            state_cons[state].append(execution)

        # Record production.
        for execution, state in outputs:
            exec_outs[execution].append(state)
            state_prods[state].append(execution)

        # Freeze relations as signatures.
        exec_sigs = {
            execution: ExecSig(tuple(exec_ins[execution]), tuple(exec_outs[execution]))
            for execution in executions
        }
        state_sigs = {
            state: StateSig(
                tuple(state_prods[state]),
                tuple(state_cons[state]),
            )
            for state in states
        }
        return exec_sigs, state_sigs

    def _initial_colours(self):
        # Begin with no structural distinctions.
        exec_colours = {execution: 0 for execution in self.exec_sigs}
        state_colours = {state: 0 for state in self.state_sigs}
        return exec_colours, state_colours

    def _refine_once(self, exec_colours, state_colours):
        # Describe Executions through neighbouring State colours.
        next_exec_sigs = {
            execution: (
                exec_colours[execution],
                ExecSig(
                    tuple(sorted(state_colours[state] for state in sig.ins)),
                    tuple(sorted(state_colours[state] for state in sig.outs)),
                ),
            )
            for execution, sig in self.exec_sigs.items()
        }

        # Describe States through neighbouring Execution colours.
        next_state_sigs = {
            state: (
                state_colours[state],
                StateSig(
                    tuple(sorted(
                        exec_colours[execution]
                        for execution in sig.prods
                    )),
                    tuple(sorted(exec_colours[execution] for execution in sig.cons)),
                ),
            )
            for state, sig in self.state_sigs.items()
        }

        # Give equal descriptions equal colours.
        signatures = set(next_exec_sigs.values()) | set(next_state_sigs.values())
        colour_for = {
            signature: colour
            for colour, signature in enumerate(sorted(signatures, key=repr))
        }
        return (
            {execution: colour_for[sig] for execution, sig in next_exec_sigs.items()},
            {state: colour_for[sig] for state, sig in next_state_sigs.items()},
        )

    def _refine_colours(self, exec_colours, state_colours):
        def partition(colours):
            cells = {}
            for node, colour in colours.items():
                cells.setdefault(colour, set()).add(node)
            return {frozenset(cell) for cell in cells.values()}

        # Refine until no colour class changes.
        while True:
            next_exec_colours, next_state_colours = self._refine_once(
                exec_colours, state_colours
            )
            if (
                partition(next_exec_colours) == partition(exec_colours)
                and partition(next_state_colours) == partition(state_colours)
            ):
                return next_exec_colours, next_state_colours
            exec_colours, state_colours = next_exec_colours, next_state_colours

    def _canonical_candidate(self, exec_colours, state_colours):
        # Order each carrier by its unique colour.
        ordered_exec_ids = tuple(sorted(self.exec_sigs, key=exec_colours.__getitem__))
        ordered_state_ids = tuple(sorted(self.state_sigs, key=state_colours.__getitem__))

        # Replace identities with their candidate positions.
        exec_index = {execution: index for index, execution in enumerate(ordered_exec_ids)}
        state_index = {state: index for index, state in enumerate(ordered_state_ids)}

        # Encode only the canonical I and O relations.
        inputs = tuple(sorted(
            (exec_index[execution], state_index[state])
            for execution, sig in self.exec_sigs.items()
            for state in sig.ins
        ))
        outputs = tuple(sorted(
            (exec_index[execution], state_index[state])
            for execution, sig in self.exec_sigs.items()
            for state in sig.outs
        ))
        return (inputs, outputs), ordered_exec_ids, ordered_state_ids

    @staticmethod
    def _smallest_tied_cell(exec_colours, state_colours):
        # Group each carrier by colour.
        cells = {}
        for carrier, colours in (("Exec", exec_colours), ("State", state_colours)):
            for node, colour in colours.items():
                cells.setdefault((carrier, colour), []).append(node)

        # Select the smallest unresolved class.
        tied = [
            (carrier, colour, nodes)
            for (carrier, colour), nodes in cells.items()
            if len(nodes) > 1
        ]
        return min(tied, key=lambda cell: (len(cell[2]), cell[0], cell[1]), default=None)

    def _same_orbit(self, source, target, exec_colours, state_colours):
        # Keep Exec and State identities distinct.
        nodes = (
            [("Exec", execution) for execution in self.exec_sigs]
            + [("State", state) for state in self.state_sigs]
        )

        # A node may only map within its carrier and colour.
        def class_of(node):
            carrier, identity = node
            colours = exec_colours if carrier == "Exec" else state_colours
            return carrier, colours[identity]

        if class_of(source) != class_of(target):
            return False

        # Describe the I/O relationship between an Execution and State.
        def relation(execution, state):
            sig = self.exec_sigs[execution[1]]
            return state[1] in sig.ins, state[1] in sig.outs

        # Collect possible destinations for each carrier and colour.
        nodes_by_class = {}
        for node in nodes:
            nodes_by_class.setdefault(class_of(node), []).append(node)

        # Unique colour classes have one forced mapping.
        mapping = {}
        used = set()
        for class_nodes in nodes_by_class.values():
            if len(class_nodes) == 1:
                mapping[class_nodes[0]] = class_nodes[0]
                used.add(class_nodes[0])

        # Add the node renaming we are trying to prove.
        mapping[source] = target
        used.add(target)

        # Check every relationship whose two endpoints are now mapped.
        def compatible(node, match):
            if class_of(node) != class_of(match):
                return False

            for other, other_match in mapping.items():
                if node[0] == other[0]:
                    continue

                if node[0] == "Exec":
                    exec_a, state_a = node, other
                    exec_b, state_b = match, other_match
                else:
                    exec_a, state_a = other, node
                    exec_b, state_b = other_match, match

                if relation(exec_a, state_a) != relation(exec_b, state_b):
                    return False
            return True

        def extend():
            # A complete node renaming proves the orbit.
            if len(mapping) == len(nodes):
                return True

            # Find the legal matches for every unmapped node.
            choices = []
            for node in nodes:
                if node in mapping:
                    continue
                matches = [
                    match
                    for match in nodes_by_class[class_of(node)]
                    if match not in used and compatible(node, match)
                ]
                if not matches:
                    return False
                choices.append((node, matches))

            # Try the most constrained node first.
            node, matches = min(choices, key=lambda choice: len(choice[1]))

            # Try each match and undo failures.
            for match in matches:
                mapping[node] = match
                used.add(match)
                if extend():
                    return True
                used.remove(match)
                del mapping[node]
            return False

        return extend()

    def _resolve_ties(self, exec_colours, state_colours):
        best = None

        def visit(exec_colours, state_colours):
            nonlocal best

            # Expose distinctions implied by the current choice.
            exec_colours, state_colours = self._refine_colours(
                exec_colours, state_colours
            )
            tied_cell = self._smallest_tied_cell(exec_colours, state_colours)

            # Keep the smallest complete candidate.
            if tied_cell is None:
                candidate = self._canonical_candidate(exec_colours, state_colours)
                if best is None or candidate[0] < best[0]:
                    best = candidate
                return

            # Give each non-equivalent choice a unique colour.
            carrier, _, nodes = tied_cell
            next_colour = max(
                (*exec_colours.values(), *state_colours.values()), default=-1
            ) + 1
            representatives = []

            for node in nodes:
                if any(
                    self._same_orbit(
                        (carrier, representative),
                        (carrier, node),
                        exec_colours,
                        state_colours,
                    )
                    for representative in representatives
                ):
                    continue

                representatives.append(node)
                branch_exec_colours = dict(exec_colours)
                branch_state_colours = dict(state_colours)
                if carrier == "Exec":
                    branch_exec_colours[node] = next_colour
                else:
                    branch_state_colours[node] = next_colour
                visit(branch_exec_colours, branch_state_colours)

        visit(exec_colours, state_colours)
        return best

    def _emit(self, candidate, return_identity_maps):
        (inputs, outputs), ordered_exec_ids, ordered_state_ids = candidate
        exec_aliases = [f"exec_{index}" for index in range(len(ordered_exec_ids))]
        state_aliases = [f"state_{index}" for index in range(len(ordered_state_ids))]

        canonical = {
            "E": exec_aliases,
            "S": state_aliases,
            "I": [[exec_aliases[execution], state_aliases[state]] for execution, state in inputs],
            "O": [[exec_aliases[execution], state_aliases[state]] for execution, state in outputs],
        }
        if not return_identity_maps:
            return canonical

        return canonical, {
            "executions": {self.original_ids["E"][identity]: alias
                           for identity, alias in zip(ordered_exec_ids, exec_aliases, strict=True)},
            "states": {self.original_ids["S"][identity]: alias
                       for identity, alias in zip(ordered_state_ids, state_aliases, strict=True)},
        }
