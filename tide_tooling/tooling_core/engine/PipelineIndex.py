from collections import deque

from .index import PipelineIndex


class PipelineIndexBuilder:
    def __init__(self, pipeline, node_processor):
        self.pipeline = pipeline
        self.node_processor = node_processor
        self.index = PipelineIndex()

    def _index_declared_nodes(self):
        # Index every Pipeline definition in declaration order.
        for definition in self.pipeline.nodes:
            self.index.node_index.index_node(definition)

    def _index_dependencies(self, node):
        # Add this node and its discovered dependencies to the mandatory DAG.
        dag = self.index.intent_dag
        dag.forward_map[node.id].extend(node.deps)

        for dependency in node.deps:
            dag.reverse_map[dependency].append(node.id)
            waiting = dag.dynamic_deps if dependency.dynamic else dag.static_deps
            waiting[dependency].append(node.id)

    def _compile_nodes(self):
        # Compile indexed nodes and index definitions discovered in their inputs.
        pending = deque(self.index.node_index.node_list)

        while pending:
            node = pending.popleft()
            self.node_processor.compile_all_inputs(node)
            self._index_dependencies(node)

            for definition in node._input_nodes:
                existing = self.index.node_index.node_by_definition_id.get(definition.id)
                if existing is not None:
                    if not existing.targets and definition.targets:
                        self.index.node_index.assign_implicit_target(definition, definition.targets[0])
                    continue
                pending.append(self.index.node_index.index_node(definition))

    def build(self):
        # Build the complete node index and mandatory dependency DAG.
        self._index_declared_nodes()
        self._compile_nodes()

        # Seed nodes that have no dependencies.
        self.index.intent_dag.ready_nodes.extend(
            node_id
            for node_id, dependencies in self.index.intent_dag.forward_map.items()
            if not dependencies
        )
        return self.index
