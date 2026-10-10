from tide_tooling.tooling_core.engine.classes import IntentDag, DataRef, RuntimeDag


def test_satisfy_and_unsatisfy_dependency_are_inverse_readiness_transitions():
    dependency = DataRef("input", "global")
    dag = IntentDag()
    dag.forward_map[1].append(dependency)
    dag.reverse_map[dependency].append(1)
    dag.static_deps[dependency].append(1)

    runtime = RuntimeDag(dag)

    assert runtime.satisfy_dependency(dependency) == [1]
    assert runtime.forward_map[1] == []
    assert list(runtime.ready_nodes_queue) == [1]
    assert dependency not in runtime.waiting_static_deps

    assert runtime.unsatisfy_dependency(1, dependency) is True
    assert runtime.forward_map[1] == [dependency]
    assert runtime.waiting_static_deps[dependency] == [1]
    assert list(runtime.ready_nodes_queue) == []
    assert runtime.reverse_map[dependency] == [1]

    assert runtime.unsatisfy_dependency(1, dependency) is False
    assert runtime.forward_map[1] == [dependency]
