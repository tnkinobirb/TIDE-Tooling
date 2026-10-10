import pytest

from tide_tooling import Ocean, node, pipeline
from tide_tooling.tooling_core.engine.classes import DataRef, Node
from tide_tooling.tooling_core.engine.index import NodeIndex


def test_nested_outputless_node_writes_only_its_discovered_link():
    received = []

    @node
    def get_data():
        return "data"

    @node
    def report_data(value):
        received.append(value)

    @pipeline
    def flow():
        report_data(get_data())

    definition = flow()
    producer = definition.nodes[0]
    original_content_id = producer.content_id

    with Ocean() as ocean:
        ocean.run_pipeline(definition, mode="sequential")
        indexed = ocean.runners[0].index.node_index
        runner_node = indexed.node_by_definition_id[producer.id]

        assert received == ["data"]
        assert len(runner_node.targets) == 1
        assert indexed.node_by_target[runner_node.targets[0]] == [runner_node]
        assert runner_node.content_id == original_content_id


def test_implicit_target_requires_an_already_indexed_targetless_node():
    definition = Node(lambda: None)
    target = DataRef("@internal", "__tide__")
    index = NodeIndex()

    with pytest.raises(KeyError):
        index.assign_implicit_target(definition, target)

    runner_node = index.index_node(definition)
    definition.targets = [target]
    index.assign_implicit_target(definition, target)

    assert runner_node.targets == [target]
    assert index.node_by_target[target] == [runner_node]

    with pytest.raises(ValueError, match="newly inferred target"):
        index.assign_implicit_target(definition, target)
