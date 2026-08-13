from dataclasses import dataclass, field
from tide_tooling.tooling_core.engine.classes import RunnerNode,  Dag, Node, DataRef
from typing import Callable, DefaultDict
from collections import defaultdict
from tide_tooling.tooling_core import debug_print as print

## PipelineIndex dataclass definitions ##
# Index of stages
@dataclass
class StageIndex:
    stage_dict      : dict = field(default_factory=dict)

# Index nodes
@dataclass
class NodeIndex:
    node_counter    : int = 0
    node_list       : list[RunnerNode] = field(default_factory=list)
    target_list     : list[DataRef] = field(default_factory=list)
    node_by_id      : dict[int, RunnerNode] = field(default_factory=dict)
    node_by_name    : dict[str, RunnerNode] = field(default_factory=dict)
    node_by_target  : DefaultDict[DataRef, list[RunnerNode]] = field(default_factory=lambda: defaultdict(list))
    node_by_content_id  : dict[str, RunnerNode] = field(default_factory=dict)
    node_id_to_definiton_node: dict[int, Node] = field(default_factory=dict)
    

    def _next_node_id(self) -> int:
       nid = self.node_counter
       self.node_counter += 1
       return nid
    
    def _index_node(self, node: Node | RunnerNode): # index node
        if isinstance(node, Node): runner_node = RunnerNode(node, self._next_node_id())
        else: runner_node = node

        print(f"{runner_node.id} Indexing node!")
        if runner_node.node_name in self.node_by_name:
            print(f"_index_node warn: duplicate node found! {runner_node.node_name}")
            if runner_node.id == self.node_by_name[runner_node.node_name].id:
                # add option to replace instead of version  
                return 
            else:
                runner_node.original_node_name = runner_node.node_name
                runner_node.node_name  = f"{runner_node.node_name}"
                print(f"_index_node warn: found a NEW node [{runner_node.node_name}] with the same name as existing node: {runner_node.node_name}! Original = {runner_node.original_node_name} ")

        
        self.node_list.append(runner_node)
        self.node_id_to_definiton_node[runner_node.id] = node

        for target in runner_node.targets:    
            self.target_list.append(target) # should always  # TODO-NOTE: Support target lists [X]
            self.node_by_target[target].append(runner_node) # TODO-NOTE: Support target lists [X]

        self.node_by_name[runner_node.node_name] = runner_node 
        
        self.node_by_id[runner_node.id] = runner_node
        self.node_by_content_id[runner_node.content_id] = runner_node
        runner_node.state.indexed = True
        print(f"{runner_node.id} Indexing node successful!")
        return runner_node

# Index of DAG
@dataclass 
class DAGIndex:
    intent_dag      : Dag = field(default_factory=Dag)
    expanded_dag    : Dag = field(default_factory=Dag)
    post_runtime_dag: Dag = field(default_factory=Dag) 
    
# Collective Pipeline index
@dataclass 
class PipelineIndex:
    stage_index     : StageIndex  = field(default_factory=StageIndex)
    node_index      : NodeIndex = field(default_factory=NodeIndex)
    dag_index       : DAGIndex = field(default_factory=DAGIndex)
