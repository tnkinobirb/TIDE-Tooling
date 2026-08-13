from typing import Callable, Tuple, Any, Union, Iterable, Generator
from .classes import Node, NodeInput, ProcessStates, DataRef, RunnerNode, RunnerNodeInput, Event
from .DependencyTools import DependencyTools
import inspect
from .errors import CallableDependencyError, NodeFailed, MissingRefDuringResolutionError, DynamicRefCountLessThanAvailableKeysError
from warnings import warn
from collections import defaultdict

class NodeProcessor: # __init__(DepTools)

    # contain everything needed to prepare node inputs and run node
    def __init__(self, dep_tools: DependencyTools):
        self.dep_tools = dep_tools

    ###################
    #  STAGE RUNNERS  #
    ###################
    def run_process_stage(self, state: ProcessStates, stage_fn: Callable, node_input: RunnerNodeInput, args: tuple = ()) -> RunnerNodeInput:
        '''
            Run a processing stage for NodeInput with a specific function and state
            e.g:
                - evaluated = evaluate_function(node.input_value.raw) 
                - UpdateInputState | update state with state=ProcessStates.EVALUATED and node.evaluated = new_value
        '''
        try:
            if node_input.has_been(state):
                return node_input, None

            previous_value = node_input.get_previous_value(state)
            new_value, stage_output = stage_fn(*args, previous_value)

            node_input.update_state(state, new_value)
            return node_input, stage_output
        except Exception:
            raise
    
    def run_compile_stage(self, node: RunnerNode, input_name: str, node_input: RunnerNodeInput) -> None:
        # eval, (normalise input, extract deps)
        node_input, _           = self.run_process_stage(state=ProcessStates.EVALUATED, stage_fn=self.evaluate_node_input, node_input=node_input, args=(node, input_name))
        node_input, found  = self.run_process_stage(state=ProcessStates.NORMALISED, stage_fn=self.normalise_input, node_input=node_input)
        # update found deps in input and to node for indexing
        found_deps, found_nodes = found 
        if found_deps:
            node_input.deps.extend(found_deps)
            for dep in found_deps:                    
                if dep not in node.deps:
                    node.deps.append(dep)


        node._input_nodes.extend(found_nodes)
        pass
    def run_resolve_stage(
        self,
        reference_unscoper: Callable,
        node: Node,
        node_input: NodeInput,
        observe_unrepresented=False,
    ) -> None:
        node_input, observations = self.run_process_stage(
            state=ProcessStates.RESOLVED,
            stage_fn=self.resolve_input,
            node_input=node_input,
            args=(reference_unscoper, node, observe_unrepresented),
        )
        return observations or ([], [])
 
        ###################
    
    ###################
    # EVALUATE  STAGE #
    ###################
    def evaluate_node_input(self, node: Node, input_name: str, input_value: Any) -> Any:
        try:
            if callable(input_value):
                # get args
                _args, _kwargs = self._get_injection_args(node, input_name)

                # evaluate the function (primary input callable) 
                evaluated_input = self._callable_input_evaluator(input_value, _args, _kwargs, inject_data_store=node.inject_data_store)
                
                return evaluated_input, None
            return input_value, None
        except Exception as e:
            raise RuntimeError(
                f"[EvaluateInputError] Node[{node.id}] e: {e}"
            ) from e

    ###################
    # NORMALISE STAGE #
    ###################
    def normalise_input(self, input_value: Any) -> Union[Any, list]:
        deps = list()
        nodes = list()
        ## normalise
        # if dep.str convert to DataRef and log dep
        if DataRef._is_dep_string(input_value):
            input_value = DataRef.from_string(input_value) # self.dep_tools.normalise_dep_str
            deps.append(input_value)
            return input_value, (deps, nodes)
        
        # log found dep and return DataRef
        if isinstance(input_value, DataRef):
            deps.append(input_value)
            return input_value, (deps, nodes)
        
        ## recurse ##
        # for each item in container - normalise value and update deps
        if isinstance(input_value, (tuple, set, list)):
             # convert lists to tuple as lists cannot contain unhashable values
            if isinstance(input_value, set):
                input_value = tuple(input_value)
                
            normalised = list()
            for v in input_value:
                normalised_input, found = self.normalise_input(v)
                normalised.append(normalised_input)
                deps.extend(found[0])
                nodes.extend(found[1])
                
            return type(input_value)(normalised), (deps, nodes)
        
        # for each item in dict - normalise value
        if isinstance(input_value, dict):
            normalised = {}
            for k, v in input_value.items():
                normalised_input, found = self.normalise_input(v)
                normalised[k] = normalised_input
                deps.extend(found[0])
                nodes.extend(found[1])
            return normalised, (deps, nodes)
        #return unchanged 

        if isinstance(input_value, Node):
            node = input_value
            nodes.append(node)
            if not node.targets: # if a node takes a node as input, create a target for the input node and register target as a dep. 
                node.targets = [DataRef(f'@tmp_node_link.{node.node_name}', "__tide__")]
            deps.extend(node.targets)
            # A nested node with one target represents one argument value.  Keeping
            # the target wrapped in a list makes the dependency resolver return
            # ``[value]`` and silently changes the producer's result shape before
            # passing it to the consumer.  Multiple targets remain a collection.
            normalised_output = node.targets[0] if len(node.targets) == 1 else node.targets
            return normalised_output, (deps, nodes) # TODO-NOTE: Support target lists [X]
        return input_value, (deps, nodes)

    ###################
    #  COMPILE STAGE  #
    ###################

    def compile_secondary_inputs(self, node: RunnerNode) -> None:
        for input_name, node_input in node.get_secondary_inputs().items():
            self.run_compile_stage(node, input_name, node_input)
    def compile_primary_inputs(self, node: RunnerNode) -> None:
        for input_name, node_input in node.get_primary_inputs().items():
            self.run_compile_stage(node, input_name, node_input)           
    def compile_all_inputs(self, node: RunnerNode) -> None:
        try:
            # evaluate and normalise injection args
            for input_name, node_input in node.get_secondary_inputs().items():
                self.run_compile_stage(node, input_name, node_input)
                for dep in node_input.deps:
                    node.dep_to_input_map[dep].append((input_name, node_input))
            # evaluate and normalise primary args
            for input_name, node_input in node.get_primary_inputs().items():
                self.run_compile_stage(node, input_name, node_input)
                for dep in node_input.deps:
                    node.dep_to_input_map[dep].append((input_name, node_input))
 
        except Exception as e:  
            warn(f"[InputResolutionError] Node[{node.id}], input={node_input} failed to compile due to {e}")
            raise NodeFailed(node.id, input_name, e)

    ###################
    #  RESOLVE STAGE  #
    ###################

   
    def resolve_input(
        self,
        reference_unscoper: Callable,
        node,
        observe_unrepresented,
        input_value: Any,
    ) -> Any:
        unrepresented = [] if observe_unrepresented else None
        resolved_value, read_deps = (
            self.dep_tools.in_place_dependency_resolver(
                reference_unscoper,
                node,
                input_value,
                unrepresented=unrepresented,
                collapse_root=False,
            )
        )
        return resolved_value, (read_deps, unrepresented or [])
    # def resolve_secondary_inputs(self, node: Node) -> None:
    #     for node_input in node.get_secondary_inputs().values():
    #         self.run_resolve_stage(node_input)               
    # def resolve_primary_inputs(self, node: Node) -> None:
    #     for node_input in node.get_primary_inputs().values():
    #         self.run_resolve_stage(node_input)           
    def resolve_all_inputs(
        self,
        reference_unscoper: Callable,
        node: Node,
        track_unrepresented=True,
    ) -> None:
        if not isinstance(track_unrepresented, bool):
            raise TypeError("track_unrepresented must be a bool")
        try:
            reads = defaultdict(list)
            unrepresented = defaultdict(list)
            # resolve injection arguments
            for input_name, node_input in node.get_secondary_inputs().items():
                input_reads, _ = self.run_resolve_stage(
                    reference_unscoper,
                    node,
                    node_input,
                )
                reads[input_name].extend(input_reads)
            # resolve primary arguments
            for input_name, node_input in node.get_primary_inputs().items():
                input_reads, input_unrepresented = self.run_resolve_stage(
                    reference_unscoper,
                    node,
                    node_input,
                    observe_unrepresented=track_unrepresented,
                )
                reads[input_name].extend(input_reads)
                unrepresented[input_name].extend(input_unrepresented)
            return reads, unrepresented
        except (MissingRefDuringResolutionError, DynamicRefCountLessThanAvailableKeysError) as e:
            raise e
        except Exception as e:  
            raise NodeFailed(node.id, input_name, f"[InputResolutionError] Node[{node.id}], input={input_name} failed to resolve due to {e}")

    ###################
    #     HELPERS     #
    ###################

    def _extract_nodes_from_result(self, result: Any) -> Generator[Node, None, None]:
        if isinstance(result, Node):
            yield result
            return
        if isinstance(result, (list, set, tuple)):
            for v in result:
                yield from self._extract_nodes_from_result(v)
            return
        if isinstance(result, dict):
            for v in result.values():
                yield from self._extract_nodes_from_result(v)
            return
        #add generator support
        return

    def _get_injection_args(self, node, callable_input_name):
        if callable_input_name == "args":
            _args = node.p_args.normalised
            _kwargs = node.p_kwargs.normalised
            return (_args, _kwargs)
        if callable_input_name == "kwargs":
            _args = node.k_args.normalised
            _kwargs = node.k_kwargs.normalised
            return (_args, _kwargs)  
        return (), {}
    
    def _callable_input_evaluator(
        self,
        func: Callable,
        inj_args: Any | tuple = (),
        inj_kwargs: dict | None = None,
        inject_data_store: bool = False,
    ) -> Any:

        func_sig = inspect.signature(func) 

        if inj_args is None:
            inj_args = tuple()

        if not isinstance(inj_args, (tuple, list)):
            inj_args = (inj_args,)
        
        if inj_kwargs is None:
            inj_kwargs = {}

        if not isinstance(inj_kwargs, dict):
            raise TypeError(f"[CALLABLE_INPUT_EVALUATOR] func={func.__name__} failed! inj_kwargs={type(inj_kwargs)} was not a dict!")

        try:
            # bind inputs to function
            try:
                if inject_data_store:
                    bound = func_sig.bind(
                        self.dep_tools.dump_store("mem"),
                        *inj_args,
                        **inj_kwargs,
                    )
                else:
                    bound = func_sig.bind(*inj_args, **inj_kwargs)    
            except TypeError as e: 
                raise CallableDependencyError(f"{self._callable_input_evaluator.__name__} Lambda function input error - failed to bind inputs [inject={inject_data_store}]{inj_args, inj_kwargs} to function!: {e} ") from e
        
            result = func(*bound.args, **bound.kwargs)
            return result 
        except Exception as e:
            warn(f"func {func.__name__} failed! : {e}")
            raise

    def _cache_result(self, node: Node, result: Any, resolved_result: Any=None) -> None:
        node.result = result
        node.resolved_result = resolved_result
        node.state.result_cached = True
    
    def _cache_emitted_nodes(self, node: Node, emitted_nodes: Iterable[Node]) -> None:
        node.emitted_nodes = list(emitted_nodes)

    def _fill_cache(self, node: Node, result: Any, resolved_result) -> None:
        self._cache_result(node, result, resolved_result)
        emitted_nodes = self._extract_nodes_from_result(result)
        self._cache_emitted_nodes(node, emitted_nodes)

    def _satisfy(self, node: Node) -> None:
        all_inputs = list(node.get_all_inputs().values())
        
        if not all((input.states[ProcessStates.RESOLVED] for input in all_inputs)):
            raise NodeFailed(node_id=node.id, input_name='', error=f'Could not satisfy node[{node.id}]! Are all inputs resolved??')
            
        if node.state.result_cached:
            node.state.evaluated = True
            pass
