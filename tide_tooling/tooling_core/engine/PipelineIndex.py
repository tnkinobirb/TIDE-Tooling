import time

from collections    import deque, defaultdict
from graphlib       import CycleError, TopologicalSorter

from .index         import PipelineIndex, StageIndex, NodeIndex, DAGIndex
from .classes   import IntentDag, Pipeline, Node, DataRef, RunnerNode, Event
from .errors        import PipelineIndexError, PipelineCycleError, StageIndexError, NodeIndexError, DAGIndexError, NodeFailed
from .utils import DedupedList
from .NodeProcessor import NodeProcessor
from tide_tooling.tooling_core import debug_print as print

# Index Builder
class PipelineIndexBuilder: #Index intent
    
    def __init__(self, pipeline: Pipeline, node_processor: NodeProcessor, event_interface=None, mode="rerun") -> None:
        # import pipeline to run
        self.pipeline: Pipeline = pipeline
        self.id = pipeline.id
        # Grab index
        self.index: PipelineIndex = PipelineIndex()
        # access index handles
        self.stage_idx: StageIndex = self.index.stage_index 
        self.node_index: NodeIndex = self.index.node_index
        self.dag_index: DAGIndex = self.index.dag_index
        
        self.node_processor = node_processor
        self.event_emitter = event_interface.emit_event_to_ocean
        self.mode = mode 
            
    # 1. Populate STAGE_INDEX
    def _stage_index_builder(self) -> None:#  CREATE ON_BUILD_FUNCTION
        # build dict of stages and their associated nodes
        try:
            self.stage_idx.stage_dict = {stage.name: (r for r in stage.nodes) for stage in self.pipeline.stages} 
        except Exception as e:
            raise StageIndexError(f"{self._stage_index_builder.__name__} error : {e}") from e  # change PipelineIndexError to StageIndexError??
        
    # 2. Populate node_INDEX
    def _node_index_builder(self) -> None: # TODO: Can practically use this logic in NodeIndex.index_node()
        # 
        try:
            self.node_index.node_list = list()
            self.node_index.node_id_to_definiton_node = {}
            for base_nodes in self.stage_idx.stage_dict.values():
                for node in base_nodes:
                    runner_node = RunnerNode(node, self.node_index._next_node_id())
                    self.node_index.node_list.append(runner_node)
                    self.node_index.node_id_to_definiton_node[runner_node.id] = node

            self.node_index.node_by_id       = {r.id: r for r in self.index.node_index.node_list}#
            self.node_index.node_by_name     = {r.node_name: r for r in self.index.node_index.node_list} 
            self.node_index.node_by_content_id= {r.content_id: r for r in self.index.node_index.node_list}
            
            for node in self.index.node_index.node_list:
                for target in node.targets:    
                    self.node_index.target_list.append(target) # should always  # TODO-NOTE: Support target lists [X]
                    self.node_index.node_by_target[target].append(node) # TODO-NOTE: Support target lists [X]
                node.state.indexed = True

        except Exception as e:
            raise NodeIndexError(f"{self._node_index_builder.__name__} error : {e}") from e 
                
    def _dag_index_builder(self) -> None:
        try:
            self.compile_dag(self.index.node_index.node_list, mode=self.mode)
            # self._try_concretise_static_deps(self.dag_index.intent_dag)
            # self._try_concretise_dynamic_deps(self.dag_index.intent_dag)
        except PipelineCycleError:
            raise
        except Exception as e:    
            raise DAGIndexError(e)
    # populate indexes
    def _pipeline_index_builder(self) -> None:
        try:
            self._stage_index_builder()
            self._node_index_builder()
            self._dag_index_builder()
        except PipelineCycleError:
            raise
        except Exception as e:
            raise PipelineIndexError(f"{self._pipeline_index_builder.__name__} error : {e}") from e 
    
    def _try_concretise_static_deps(self, dag: IntentDag) -> None:
        for dep, node_ids in dict(dag.static_deps).items():
            if self.node_processor.dep_tools.exists(dep):
                for node_id in list(node_ids):
                    dag.forward_map[node_id].remove(dep)
                    dag.static_deps[dep].remove(node_id)
                    if not dag.static_deps[dep]:
                        del dag.static_deps[dep]
                    print(f"[DEP_CONCRETISER][Node[{node_id}]] readied static {dep} at compile time!")

    def _try_concretise_dynamic_deps(self, dag: IntentDag) -> None:
        for dep, node_ids in dict(dag.dynamic_deps).items():
            if self.node_processor.dep_tools.exists(dep): 
                for node_id in list(node_ids):
                    dag.forward_map[node_id].remove(dep)
                    dag.dynamic_deps[dep].remove(node_id)
                    if not dag.dynamic_deps[dep]:
                        del dag.dynamic_deps[dep]
                    
                    got_key_refs: DedupedList[DataRef] = self.node_processor.dep_tools.get_key_refs(dep)
                    print(f"[DEP_CONCRETISER][Node[{node_id}]] readied dynamic {dep}! from: {got_key_refs} at compile time")

                    node: RunnerNode = self.node_index.node_by_id[node_id]
                    node.expanded_deps[dep].extend(got_key_refs)
                    for ref in got_key_refs:
                        node.static_to_dynamic_deps[ref].append(dep)
                    dag.reverse_map[dep].append(node_id)

    def compile_dag(self, node_list: list[Node], mode='rerun') -> None:
        try:
            intent_dag = IntentDag()
            forward_map = intent_dag.forward_map
            reverse_map = intent_dag.reverse_map

            worker_list = deque(node_list)
  
            while worker_list:

                node: RunnerNode = worker_list.popleft()   

                try: # compile not allowed to fail        
                    self.node_processor.compile_all_inputs(node)
                    
                    # if mode is cached, evaluate node deps availability and make emit dep available event.
                    if self.mode == 'cached': # TODO-NOTE: support target lists [X]
                        targets = node.targets
                        _exists = [self.node_processor.dep_tools.exists(target) for target in targets]
                        if targets and all(_exists): 
                            print(f"[COMPILER] Node [{node.id}] target already cached :)")
                            node.state.evaluated = True
                            for target in targets:
                                self.event_emitter(Event(source="indexer", source_id=self.id, target_runner_id='*', event_type='dependency_available', payload=(target.store, target.key)))
                except NodeFailed as e:
                    node_id = e.node_id
                    if node.id != node_id:
                        raise RuntimeError("[NodeFailedError] returned node_id from error did not match actual ready_node.id!!!!")
                    input_name = e.input_name
                    error = e.error
                    raise DAGIndexError(f"[IntentGraph{node.id}] Node[{node_id}]: failed to compile {input_name} for indexing! e: {error}")
                
                print(f"[INTENT_GRAPH_BUILDER] DISCOVERED DEPS in [{node.id}]: {node.deps}")
                
                print(f"[INTENT_GRAPH_BUILDER] DISCOVERED NODES in [{node.id}]: {node._input_nodes}")
                for i_node in node._input_nodes:
                    print(f"[{self.id}] registering node from {node.id}!")
                    runner_node = self.index.node_index._index_node(i_node)
                    worker_list.append(runner_node)
                                        
                forward_map[node.id] = node.deps # dependants -> dependency
                for target in node.targets:
                    if target not in reverse_map: # TODO-NOTE: support target lists [X]
                        reverse_map[target]
                for dep in node.deps: # s
                    reverse_map[dep].append(node.id)
                    pass # dependency -> dependants
                
            # populate index
            self.dag_index.intent_dag.forward_map= defaultdict(DedupedList, {k: v for k, v in forward_map.items()})
            self.dag_index.intent_dag.reverse_map= defaultdict(DedupedList, {k: v for k, v in reverse_map.items()})
            self.dag_index.intent_dag.ready_nodes = DedupedList(node_id for node_id, deps in forward_map.items() if not deps) # dump - if.store.exists, Node ready for resolution. prefix: if any key matches, mark as ready. 
            
            for node_id, deps in self.dag_index.intent_dag.forward_map.items():
                for dep in deps: 
                    if dep.dynamic:
                        self.dag_index.intent_dag.dynamic_deps[dep].append(node_id)
                    else:
                        self.dag_index.intent_dag.static_deps[dep].append(node_id)
            
             
            node_dependencies = {
                node.id: set()
                for node in self.index.node_index.node_list
            }
            for consumer in self.index.node_index.node_list:
                for dependency in consumer.deps:
                    if dependency.dynamic:
                        continue
                    for producer in self.node_index.node_by_target.get(
                        dependency,
                        (),
                    ):
                        # A node that reads and rewrites one reference is an
                        # update of an earlier version, not a recursive edge
                        # to the value it has not produced yet.
                        if producer.id == consumer.id:
                            continue
                        node_dependencies[consumer.id].add(producer.id)

            # Topological order is over node occurrences. DataRefs remain the
            # runtime availability graph and are not themselves graph nodes.
            try:
                self.dag_index.intent_dag.topo_order = tuple(
                    TopologicalSorter(node_dependencies).static_order()
                )
                self.dag_index.intent_dag.has_cycle = False
                print("PopulateIntentDagIndexSucessful!",self.dag_index.intent_dag.forward_map.keys())
            except CycleError as e:
                self.dag_index.intent_dag.topo_order = []
                self.dag_index.intent_dag.has_cycle = True
                self.dag_index.intent_dag.cycle_info = e
                cycle_ids = e.args[1] if len(e.args) > 1 else ()
                cycle_names = [
                    self.node_index.node_by_id[node_id].node_name
                    for node_id in cycle_ids
                    if node_id in self.node_index.node_by_id
                ]
                raise PipelineCycleError(
                    "Pipeline contains a dependency cycle: "
                    + " -> ".join(cycle_names)
                ) from e
        except PipelineCycleError:
            raise
        except Exception as e:
            raise DAGIndexError(f"{self.build.__name__} error : {e}") from e
        
    # init (populate) and return index
    def build(self) -> PipelineIndex:
        t0 = time.time()
        try:
            self._pipeline_index_builder()
        except PipelineCycleError:
            raise
        except Exception as e:
            raise PipelineIndexError(f"{self.build.__name__} error : {e}") from e 
        print(f"PipelineIndex finished in [{time.time()-t0:3f}]s")
        return self.index

