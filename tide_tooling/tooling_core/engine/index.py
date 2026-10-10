from collections import defaultdict
from dataclasses import dataclass, field
from typing import DefaultDict

from .classes import DataRef, IntentDag, RunnerNode, RuntimeDag, __TEMPLATE_VAL__


def _project_pipeline_index_to_tide_plan(
    pipeline_index,
    dynamic_ref_plan=None,
    *,
    runner_id=None,
    resolve_ref=None,
):
    nodes = pipeline_index.node_index.node_list
    dynamic_ref_plan = dynamic_ref_plan or {}
    resolve_ref = resolve_ref or (lambda reference: reference)

    E = {node.node.id for node in nodes}
    I = {
        (node.node.id, resolve_ref(state))
        for node in nodes
        for state in node.deps
    }
    O = {
        (node.node.id, resolve_ref(state))
        for node in nodes
        for state in node.targets
    }
    S = {state for _, state in I | O}
    D = {
        state: dynamic_ref_plan.get(state, __TEMPLATE_VAL__)
        for node in nodes
        for state in node.deps
        if state.dynamic
    }
    node_specs = [
        {
            "definition_id": node.node.id,
            "runner_id": runner_id,
            "actor_name": node.func.__name__,
            "one_shot": node.one_shot,
            "dependencies": [
                {
                    "declared": dependency,
                    "reference": resolve_ref(dependency),
                    "dynamic": dependency.dynamic,
                }
                for dependency in node.deps
            ],
            "targets": [resolve_ref(target) for target in node.targets],
        }
        for node in nodes
    ]
    return {
        "E": E,
        "S": S,
        "I": I,
        "O": O,
        "D": D,
        "nodes": node_specs,
    }


@dataclass
class NodeIndex:
    # Preserve indexed node occurrence order.
    node_list: list[RunnerNode] = field(default_factory=list)

    # Resolve nodes by each identity used by the runtime.
    node_by_id: dict[int, RunnerNode] = field(default_factory=dict)
    node_by_definition_id: dict[str, RunnerNode] = field(default_factory=dict)
    node_by_target: DefaultDict[DataRef, list[RunnerNode]] = field(default_factory=lambda: defaultdict(list))
    node_by_content_id: dict[bytes, RunnerNode] = field(default_factory=dict)

    def index_node(self, definition, emitted_by_exec_id=None):
        # Return the runtime node already representing this definition.
        existing = self.node_by_definition_id.get(definition.id)
        if existing is not None:
            return existing

        # Give one newly discovered definition its runtime identity.
        node = RunnerNode(definition, len(self.node_list))
        node.emitted_by_exec_id = emitted_by_exec_id

        # Retain occurrence order and every required lookup.
        self.node_list.append(node)
        self.node_by_id[node.id] = node
        self.node_by_definition_id[definition.id] = node
        self.node_by_content_id.setdefault(node.content_id, node)

        for target in node.targets:
            self.node_by_target[target].append(node)

        return node

    def assign_implicit_target(self, definition, target):
        # Update one already-indexed producer when its nested use reveals a target.
        runner_node = self.node_by_definition_id[definition.id]
        if runner_node.targets or definition.targets != [target]:
            raise ValueError("Expected one newly inferred target")

        runner_node.targets = [target]
        self.node_by_target[target].append(runner_node)


@dataclass
class PipelineIndex:
    # Index declared and subsequently discovered nodes.
    node_index: NodeIndex = field(default_factory=NodeIndex)

    # Preserve the intended and final dependency structures.
    intent_dag: IntentDag = field(default_factory=IntentDag)
    post_runtime_dag: RuntimeDag | None = None
