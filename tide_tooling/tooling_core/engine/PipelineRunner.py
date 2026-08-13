from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tide_tooling.tooling_core.engine.routers import StoreRouter

import uuid
import time, copy

from enum               import StrEnum
from typing             import List, Any
from warnings           import warn
from queue              import Queue, Empty
from collections        import defaultdict
from tide_tooling.tooling_core               import debug_enabled, debug_print

_user_print = print
print = debug_print

from .index             import PipelineIndex
from .utils             import _print_dict_struct, _node_worker, DedupedList, deduped_dict, create_tide_exec_uuid, tide_snapshot_value
from .DependencyTools   import DependencyTools
from .PipelineIndex     import PipelineIndexBuilder
from .errors            import NodeDefinitionError, PipelineRuntimeError, PipelineDeadlockError, NodeToDagRegistrationError, EventHandlerError, NodeFailed, MissingRefDuringResolutionError, DynamicRefCountLessThanAvailableKeysError, RaiseHandlingError, TIDEError
from .classes           import Node, RunnerNode, RuntimeDag, Pipeline, Stage, Event, DataRef, RunnerNodeInput, END, DELETE, WRITE, RUN, __TIDE_TEMPLATE_VALUE__, Effect, EMIT
from .NodeProcessor     import NodeProcessor
from .events            import PipelineEvents, RouterEvents, ExecutorEvents, OceanEvents, LogEvents, Effects



class PipelineRunner: 

    _global_runner_count = 0

    def __init__(self, id:int=None, name:str=None, pipeline: Pipeline | List[Stage] | None=None, parallel='thread', event_interface=None, store_interface=None, mem_store_templates:dict[str, Any]|None=None, mode="rerun", max_attempts=400000, track_unrepresented=True, intent_dynamic_refs=None):# init takes data_store and pipeline_node_dict
        
        # | INITIALISATION OF RUNNER ATTRS |
        # Empty pipe
        if not pipeline:
            raise NodeDefinitionError("[PIPELINE] - Pipeline is None! Nothing to do | Watcher pipelines still in WIP!")
        if not isinstance(pipeline, Pipeline):
            raise NodeDefinitionError(f"[PIPELINE] - Pipeline is not a valid Pipeline obj! was {type(pipeline)}")

        # Instance counter
        self.id = id
        PipelineRunner._global_runner_count += 1
        if name:
            self.id = name 
        
        # Runner Configuration
        self.attempt_count = 0
        self.max_attempts = max_attempts  # Max tick attempts per idle state.
        if not isinstance(track_unrepresented, bool):
            raise TypeError("track_unrepresented must be a bool")
        self.track_unrepresented = track_unrepresented
        self.intent_dynamic_refs = intent_dynamic_refs
        self.poll = True            # if true, at the beginning of the run, dependencies will be polled for availability to ready dependent nodes for execution.
        self.mode = mode            # rerun node | cached (if target exists in store dont rerun ) # NOTE: strengthen semantics
        self.parallel = parallel    # execution/processing mode. NOTE: can probs be moved to nodes but also have blanket default  
        self.complete = False       # mark as complete if no waiting nodes and other conditions are true
        self.stay_alive = False     # dont die unless triggered by event | overrides self.complete
        self.build_mode = False     # if true, nodes are not released automatically and must be triggered through the build command. Execution will still not happen unless dependencies are available.
        
        # Runner State
        self.purgatory: bool = False
        self._start_time = time.time()
        self.running_nodes = {} # node.id -> node
        self.exec_id_to_node_id = {}
        self.failure = None
               
        # Queues 
        self.event_queue = Queue()
        
        # Metadata
        self.history = []

        # | WIRE INTERFACES | #
        # Subscribe to events + setup event emitter:
        event_interface.event_subscriber(PipelineEvents, self.consume_event)
        self.event_emitter= event_interface.emit_event_to_ocean
        
        # Create a scope store router instance 
        mem_store_templates = mem_store_templates or {}
        self.store_router: StoreRouter = store_interface.create_scoped_router(self.id, mem_store_templates=copy.deepcopy(mem_store_templates))

        # | SETUP PROCESSING TOOLS | # 
        # Node processing tools
        self.dep_tools = DependencyTools(store_router=self.store_router)
        self.node_processor: NodeProcessor = NodeProcessor(dep_tools=self.dep_tools)
    
        # | COMPILE PIPELINE FOR RUN | #       
        # Pipeline
        self.pipeline = pipeline
    
        # Build DAG | Fill pipeline index
        self.index: PipelineIndex = PipelineIndexBuilder(pipeline, self.node_processor, event_interface, mode=self.mode).build() # returns populated pipeline_index  

        # Select and convert indexed dag to RuntimeDag
        self.dag_to_run = self.index.dag_index.intent_dag
        self.runtime_dag = RuntimeDag(self.dag_to_run)
        

        # Event handlers
        self.event_handlers = {
            # State Events
            PipelineEvents.DEP_AVAILABLE : self.on_dep_available,
            PipelineEvents.DEP_DELETED : self.on_dep_delete,
            PipelineEvents.DEP_UPDATED : self.on_dep_update,
            PipelineEvents.RESULT_READY : self._on_node_completion,
            
            # Command Events
            PipelineEvents.REGISTER_NODE : self.on_register_node,
            PipelineEvents.RUN_NODE : self.on_node_run,
            PipelineEvents.BUILD : self.on_build,
            PipelineEvents.PRINT: self.on_print,
            PipelineEvents.WAITING : self.on_print_waiting,
            PipelineEvents.DONE : self.on_print_done,
            PipelineEvents.KILL : self.on_kill
        }

    ####################
    # EVENT MANAGEMENT #
    ####################  

    def _scope_store_reference(self, store):
        #already scoped (is root name, not alias)
        if store in self.store_router.store_alias:
            return store
        # is scoped to runner, else reference actual store . e.g. mem_1 -> runner_0 will acces mem_1 and not mem_0
        if store.endswith(f"_{self.id}"):
            store = store.rsplit("_", 1)[0]
        
        return store
    
    def _unscope_store_reference(self, store):
        if store in self.store_router.store_alias:
            store = store + f"_{self.id}"
        return store

    def _scope_data_ref(self, ref):
        store = self._scope_store_reference(ref.store)
        return DataRef(ref.key, store, ref.count, ref.max)

    def _unscope_data_ref(self, ref):
        store = self._unscope_store_reference(ref.store)
        return DataRef(ref.key, store, ref.count, ref.max)
    
    def consume_event(self, event):
        self.event_queue.put(event)

    def handle_event(self, event):
        self.event_handlers[event.event_type](event)

    def _process_event_queue(self):
        while not self.event_queue.empty():
            try:
                event = self.event_queue.get_nowait()
                self.handle_event(event)
                self.event_queue.task_done()
            except Empty:
                break
            except RaiseHandlingError:
                raise
            except Exception as e:
                raise EventHandlerError(f"Error while handling event[{event}]! : {e} ")

    #############
    # ON EVENTS #
    #############

    def on_kill(self, event):
        self.index.dag_index.post_runtime_dag = copy.deepcopy(self.runtime_dag)
        self.stay_alive = False
        print("Pipeline dying :(")
        return 
    def on_print(self, event):
        _user_print(f"Store = mem_{self.id}:")
        _print_dict_struct(self.dep_tools.dump_store(f"mem_{self.id}")) 

    def on_print_waiting(self, event):
        _user_print(f"\nRunner [{self.id}] waiting=")
        for nid, deps in self.runtime_dag.forward_map.items():                      
            if deps or not self.index.node_index.node_by_id[nid].state.evaluated:    
                node_name = self.index.node_index.node_by_id[nid].node_name
                _user_print(f"{nid}:{node_name} | {deps }")
        return

    def on_print_done(self, event):
        _user_print(f"Pipeline[{self.id}]\ndone=")
        for nid, deps in self.runtime_dag.forward_map.items():    
            done_node = self.index.node_index.node_by_id[nid]
            evaluated = done_node.state.evaluated                    
            if evaluated:
                _user_print(f"{nid}: {self.index.node_index.node_by_id[nid].node_name}")
        return

    def on_node_run(self, event):
        if not self.build_mode:
            print("Running a specific node is only allowed in build_mode!")
            return
        node_id = event.payload
        print(f"Pipeline[{self.id}] Attempting to run node [{node_id}]")
        if node_id not in self.runtime_dag.forward_map:
            print(f"Node[{node_id}] is not registered in this pipeline. retuning.")
            return
        if self.runtime_dag.forward_map[node_id]:
            print(f"Node[{node_id}] is still waiting on dependencies: {self.runtime_dag.forward_map[node_id]}")
            return
        node = self.index.node_index.node_by_id[node_id]
        self._on_node_ready(node)
        return

    def _effect_read_values(self):
        """Snapshot visible store values for READ calls inside a node."""
        values = {}
        registry = self.store_router.registry

        for store_name in dict.fromkeys(registry.__seen_stores__):
            if not registry.exists(store_name):
                continue
            for key, value in registry.get(store_name).dump().items():
                values[f"{store_name}:{key}"] = value

        for alias, physical_names in self.store_router.store_alias.items():
            if not isinstance(physical_names, (list, tuple, set)):
                physical_names = [physical_names]
            for physical_name in physical_names:
                if not registry.exists(physical_name):
                    continue
                for key, value in registry.get(physical_name).dump().items():
                    values[f"{alias}:{key}"] = value

        return values

    def _effect_runtime(self, node, exec_id):
        runtime = {
            "read_values": self._effect_read_values(),
            "effects": [],
            "inside_process_pool": self.parallel == "process",
            "default_store": DataRef.default_store,
        }

        runtime["delete_now"] = lambda ref: self._delete_now(
            node,
            exec_id,
            runtime,
            ref,
        )
        return runtime

    def _delete_now(self, node, exec_id, runtime, ref):
        """Synchronously remove ``ref`` for an in-function DELETE_NOW call."""
        pending_effects = runtime.setdefault("effects", [])

        target = self._unscope_data_ref(ref)
        found, _ = self.store_router.get_store(
            target.store
        ).pop_if_present(target.key)

        # READ uses a dispatch snapshot. Keep that private snapshot coherent so
        # a later READ in this same function observes the immediate deletion.
        read_values = runtime.setdefault("read_values", {})
        read_values.pop(DataRef.to_string(ref, strip_leading=True), None)
        read_values.pop(DataRef.to_string(target, strip_leading=True), None)

        effect = {
            "effect": Effects.DELETE,
            "reference": target,
            "found": found,
        }
        pending_effects.append(effect)

        return found

    def _emit_in_function_effects(self, node, exec_id, effects):
        for effect in effects or ():
            if not isinstance(effect, dict):
                continue

            effect_kind = effect.get("effect")
            if effect_kind not in {Effects.READ, Effects.DELETE}:
                continue

            target = self._unscope_data_ref(effect["reference"])
            node.runtime_effects.append(effect)

            targets = (
                {target: effect["value"]}
                if effect_kind == Effects.READ
                else [target]
            )

            self.event_emitter(Event(
                source="runner",
                source_id=self.id,
                event_type=LogEvents.EFFECT,
                payload=(
                    effect_kind,
                    exec_id,
                    targets,
                    __TIDE_TEMPLATE_VALUE__(),
                    {
                        "found": effect.get("found", False),
                        "immediate": effect_kind == Effects.DELETE,
                    },
                ),
                exec_id=exec_id,
            ))

            if effect_kind == Effects.DELETE and effect.get("found", False):
                # The live store was already mutated by DELETE_NOW. This event
                # updates DAG readiness after the execution receipt exists; it
                # deliberately does not route through StoreRouter.DELETE and
                # therefore cannot delete a later replacement value twice.
                self.event_emitter(Event(
                    source="store_router",
                    source_id=self.store_router.id,
                    target_runner_id=self.id,
                    event_type=PipelineEvents.DEP_DELETED,
                    payload=(
                        target.store,
                        target.key,
                        f"{node.func.__name__}[{node.id}]",
                        Effects.DELETE,
                    ),
                    exec_id=exec_id,
                ))

    def _snapshot_execution_evidence(self, node: RunnerNode):
        """Freeze occurrence-specific evidence before reusable node execution.

        RunnerNode is mutable runtime machinery and may be resolved or executed
        again. Historical ExecRecords must therefore be reconstructed from this
        dispatch snapshot rather than from the node's eventual live state.
        """
        snapshots_by_identity = {}

        def snapshot(value, boundary):
            identity = id(value)
            if identity not in snapshots_by_identity:
                snapshots_by_identity[identity] = tide_snapshot_value(
                    value,
                    boundary=boundary,
                )
            return snapshots_by_identity[identity].at_boundary(boundary)

        base_node = self.index.node_index.node_id_to_definiton_node.get(
            node.id
        )
        inputs = {}

        for input_name, node_input in node.get_all_inputs().items():
            dependencies = []
            for dependency in node_input.deps:
                dependencies.append({
                    "dependency": snapshot(
                        dependency,
                        f"dispatch.{input_name}.dependency",
                    ),
                    "unscoped_reference": (
                        f"{self._unscope_store_reference(dependency.store)}:"
                        f"{dependency.key}"
                    ),
                })

            inputs[input_name] = {
                "raw": snapshot(
                    node_input.raw,
                    f"dispatch.{input_name}.raw",
                ),
                "evaluated": snapshot(
                    node_input.evaluated,
                    f"dispatch.{input_name}.evaluated",
                ),
                "normalised": snapshot(
                    node_input.normalised,
                    f"dispatch.{input_name}.normalised",
                ),
                "resolved": snapshot(
                    node_input.resolved,
                    f"dispatch.{input_name}.resolved",
                ),
                "dependencies": dependencies,
            }

        dynamic_dependencies = []
        for dynamic_dependency, static_dependencies in (
            node.dynamic_to_static_deps.items()
        ):
            dynamic_dependencies.append({
                "dependency": snapshot(
                    dynamic_dependency,
                    "dispatch.dynamic_dependency",
                ),
                "expanded": [
                    {
                        "dependency": snapshot(
                            static_dependency,
                            "dispatch.expanded_dependency",
                        ),
                        "unscoped_reference": (
                            f"{self._unscope_store_reference(static_dependency.store)}:"
                            f"{static_dependency.key}"
                        ),
                    }
                    for static_dependency in static_dependencies
                ],
            })

        unscoped_store_refs = []
        references = [
            *node.targets,
            *node.deps,
            *[
                dependency
                for dependencies in node.dynamic_to_static_deps.values()
                for dependency in dependencies
            ],
        ]
        for reference in references:
            store = self._unscope_store_reference(reference.store)
            if store not in unscoped_store_refs:
                unscoped_store_refs.append(store)

        declared_deps = list(node.deps)
        declared_effects = list(node.effects)
        declared_targets = list(node.targets)

        return {
            "runner_id": self.id,
            "pipeline_id": self.pipeline.id,
            "node_id": node.id,
            "func": node.func,
            "func_name": node.func.__name__,
            "inputs": inputs,
            "dynamic_dependencies": dynamic_dependencies,
            "declared_deps": snapshot(
                declared_deps,
                "dispatch.declared_deps",
            ),
            "declared_effects": snapshot(
                declared_effects,
                "dispatch.declared_effects",
            ),
            "declared_targets": snapshot(
                declared_targets,
                "dispatch.declared_targets",
            ),
            "unscoped_store_refs": tuple(unscoped_store_refs),
            "resolve_output": node.resolve_output,
            "base_node_id": base_node.id if base_node else None,
            "base_node_content_id": (
                base_node.content_id if base_node else None
            ),
            "content_id": node.content_id,
            "node_options": {
                "recompute": node.recompute,
                "resolve_output": node.resolve_output,
                "inject_data_store": node.inject_data_store,
            },
        }

    def _on_node_ready(self, node: RunnerNode):
        try:
            exec_id = create_tide_exec_uuid()
            reads, unrepresented = self._resolve_node_inputs(node)
            self._represent_unnamed_inputs(
                node,
                exec_id,
                reads,
                unrepresented,
            )
            self._emit_read_events_from_reads(reads, exec_id)
        except DynamicRefCountLessThanAvailableKeysError as e:
            print(f"[DYNAMIC:ON_NODE_READY:ERROR] {node.node_name} failed to resolve due to unsatisfied count error ")
            missing_ref = e.ref
            available_keys = e.available_keys
            for static_ref in node.dynamic_to_static_deps[missing_ref]:
                if static_ref.key not in available_keys:
                    print(f"[INPUT_RESOLVER] {static_ref.key} was missing during dynamic resolution! Creating fallback delete dep event!")
                    self.event_emitter(Event(source="runner", source_id=self.id, event_type=LogEvents.MISSING_REFS, payload=(node, missing_ref, available_keys)))
                    self.event_emitter(Event(source="store_router", source_id=self.id, target_runner_id=self.id, event_type=PipelineEvents.DEP_DELETED, payload=(missing_ref.store, static_ref.key, "TIDE: InputResolver", "TIDE: MissingRefFallback")))             
            return
        except MissingRefDuringResolutionError as e:
            print(f"[ON_NODE_READY:ERROR] {node.node_name} failed to resolve due to missing error ")
            missing_ref = e.ref
            available_keys = e.available_keys
            print(f"[INPUT_RESOLVER] {missing_ref} missing during resolution! Creating fallback delete dep event!")
            self.event_emitter(Event(source="runner", source_id=self.id, event_type=LogEvents.MISSING_REFS, payload=(node, missing_ref, available_keys)))
            self.event_emitter(Event(source="store_router", source_id=self.id, target_runner_id=self.id, event_type=PipelineEvents.DEP_DELETED, payload=(missing_ref.store, missing_ref.key, "TIDE: InputResolver", "TIDE: MissingRefFallback")))
            return

        _MODE = self.parallel
        _WORKER = _node_worker
        effect_runtime = self._effect_runtime(node, exec_id)
        worker_effect_runtime = effect_runtime
        if self.parallel == "process":
            # The live callback is intentionally retained only on the parent
            # event. The process receives a serialisable marker and asks the
            # Executor to replay this occurrence in a shared-memory thread if
            # DELETE_NOW (or READ) is actually called.
            worker_effect_runtime = {
                key: value
                for key, value in effect_runtime.items()
                if key != "delete_now"
            }
            effect_runtime["inside_process_pool"] = False

        _WORKER_INPUTS = (
            node.func,
            node.args.resolved,
            node.kwargs.resolved,
            worker_effect_runtime,
        )
        _PAYLOAD = (node.id, (_WORKER, _WORKER_INPUTS, _MODE))
        
        node.state.executing = True
        self.exec_id_to_node_id[exec_id] = node.id
        
        
        
        ready_node_event = Event(source="runner", source_id=self.id, target_runner_id=None, event_type=ExecutorEvents.NODE_READY, payload=_PAYLOAD, exec_id=exec_id)
        ready_node_event.execution_evidence = (
            self._snapshot_execution_evidence(node)
        )
        ready_node_event.thread_effect_runtime = effect_runtime
        self.event_emitter(ready_node_event) 
        self.running_nodes[node.id] = node 

    def _handle_node_result(
        self,
        node: RunnerNode,
        exec_id: uuid = None,
        effects=None,
        store_result=True,
        preserve_effect_output_to_node_targets=True,
    ):

        

        def _store_preserved_output(preserved_output):
            if not isinstance(preserved_output, __TIDE_TEMPLATE_VALUE__):
                emitted_nodes = self.node_processor._extract_nodes_from_result(preserved_output)
                node.emitted_nodes.extend(DedupedList(emitted_nodes))
                if preserve_effect_output_to_node_targets:
                    self._store_node_result(node, exec_id, preserved_output)
            


        
        to_handle = (
            DedupedList(node.effects)
            if effects is None
            else DedupedList(effects)
        )
        if store_result:
            node_result = (
                node.resolved_output
                if node.resolve_output
                else node.result
            )

            if (
                isinstance(node_result, type)
                and issubclass(node_result, Effect)
            ) or isinstance(node_result, Effect):
                to_handle.append(node_result)
            else:
                self._store_node_result(node, exec_id)

        if not to_handle:
            return
        die = False
        for effect in to_handle:
            if effect not in node.effects:
                node.runtime_effects.append(effect)
            if isinstance(effect, type):
                if effect == END:
                    self.event_emitter(Event(source="runner", source_id=self.id, event_type=LogEvents.EFFECT, payload=(Effects.END, exec_id, [self.id], __TIDE_TEMPLATE_VALUE__()), exec_id=exec_id))
                    self.event_emitter(Event(source="runner", source_id=self.id, target_runner_id=self.id, event_type=OceanEvents.KILL_PIPELINE, exec_id=exec_id)) # make EFFECT handler for DELETE and END etc
                    
                    die = True
                # delete node.target 
                elif effect == DELETE:
                    
                    if node.targets: # TODO-NOTE: support target DedupedLists [X]
                        unscoped_targets = [self._unscope_data_ref(target) for target in node.targets]
                        self.event_emitter(Event(source="runner", source_id=self.id, event_type=LogEvents.EFFECT, payload=(Effects.DELETE, exec_id, unscoped_targets,  __TIDE_TEMPLATE_VALUE__()), exec_id=exec_id))
                        for unscoped_target in unscoped_targets:
                            self.event_emitter(Event(source="store_router", source_id=self.id, target_runner_id=self.id, event_type=RouterEvents.DELETE, payload=(unscoped_target.store, unscoped_target.key, f"{node.func.__name__}[{node.id}]", Effects.DELETE), exec_id=exec_id))
                # delete specified target | default node.target
            elif isinstance(effect, DELETE): # TODO-NOTE: WRAP IN ISINSTANCE Effect
                
                targets = effect.targets if effect.targets else node.targets  
                unscoped_targets = []
                
        
                for target in targets:
                    unscoped_targets.append(self._unscope_data_ref(target))
            
                
                self.event_emitter(Event(source="runner", source_id=self.id, event_type=LogEvents.EFFECT, payload=(Effects.DELETE, exec_id, unscoped_targets, effect.preserve_output), exec_id=exec_id))

                for unscoped_target in unscoped_targets:
                    self.event_emitter(Event(source="store_router", source_id=self.id, target_runner_id=self.id, event_type=RouterEvents.DELETE, payload=(unscoped_target.store, unscoped_target.key, f"{node.func.__name__}[{node.id}]", Effects.DELETE), exec_id=exec_id))

                # TODO-NOTE: add end.targets = [runner_id], for runner_id, end.

                _store_preserved_output(effect.preserve_output)
            
            elif isinstance(effect, END): # if an instance of delete, check preserve_output and save it to the store if preserve has a value
                
                
                _store_preserved_output(effect.preserve_output)
                

                targets = effect.targets
                if isinstance(targets, __TIDE_TEMPLATE_VALUE__):
                    targets = [self.id]

                self.event_emitter(Event(source="runner", source_id=self.id, event_type=LogEvents.EFFECT, payload=(Effects.END, exec_id, targets, effect.preserve_output), exec_id=exec_id))
                
                for target_id in targets:
                    if target_id == self.id:
                        die = True
                    self.event_emitter(Event(source="runner", source_id=self.id, target_runner_id=target_id, event_type=OceanEvents.KILL_PIPELINE, exec_id=exec_id)) # make EFFECT handler for DELETE and EN

        
            elif isinstance(effect, WRITE):   
                to_write = []

                preserved_output: Any = __TIDE_TEMPLATE_VALUE__()

                if not isinstance(effect.preserve_output, __TIDE_TEMPLATE_VALUE__):
                    preserved_output = effect.preserve_output
                    _store_preserved_output(effect.preserve_output)

                targets = effect.targets if effect.targets else node.targets

                if isinstance(targets, dict):
                    for target, value in targets.items():
                        to_write.append((self._unscope_data_ref(target), value))
                else:
                    for target in targets:
                        to_write.append((self._unscope_data_ref(target), preserved_output))
                        
                self.event_emitter(Event(source="runner", source_id=self.id, event_type=LogEvents.EFFECT, payload=(Effects.WRITE, exec_id, to_write, effect.preserve_output), exec_id=exec_id))

                for unscoped_target, value in to_write:
                    node.runtime_targets.append(unscoped_target)
                    self.event_emitter(Event(source="runner", source_id=self.id, target_runner_id=None, event_type=RouterEvents.STORE_WRITE, payload=(unscoped_target.store, unscoped_target.key, value, node.id, Effects.WRITE, tide_snapshot_value(value, boundary="write")), exec_id=exec_id))
            elif isinstance(effect, RUN):

                if isinstance(effect.targets, __TIDE_TEMPLATE_VALUE__):
                    raise ValueError(
                        "RUN requires at least one registered pipeline id"
                    )

                self.event_emitter(Event(source="runner", source_id=self.id, event_type=LogEvents.EFFECT, payload=(Effects.RUN, exec_id, effect.targets, effect.preserve_output), exec_id=exec_id))

                _store_preserved_output(effect.preserve_output)
                
                for pipeline_id in effect.targets:
                    self.event_emitter(Event(source="runner", source_id=self.id, target_runner_id=None, payload=(pipeline_id, effect.mode, effect.templates, effect.stay_alive, None), event_type=OceanEvents.RUN_PIPELINE, exec_id=exec_id))

                
            elif isinstance(effect, EMIT):
                for emitted_node in effect.nodes:
                    node.emitted_nodes.append(emitted_node)
        if die: return    




    def _on_node_completion(self, event): # take 'event' instead
        try:

            exec_id = event.exec_id
            node_id, result, meta = event.payload
            
            node: RunnerNode = self.running_nodes.get(node_id) 
            if not node:
                return
            print(f"[{node.id}] {node.node_name} Worker returned:", type(result))

            if meta.status == "failed" or meta.error is not None:
                if getattr(meta, "handled_raise", False):
                    pass
                else:
                    self.failure = meta.error or NodeFailed(
                        node.id,
                        "execution",
                        RuntimeError(f"Node {node.node_name} failed"),
                    )
                    node.state.executing = False
                    self.running_nodes.pop(node_id, None)
                    self.complete = True
                    self.event_emitter(
                        Event(
                            source="runner",
                            source_id=self.id,
                            target_runner_id=self.id,
                            event_type=OceanEvents.KILL_PIPELINE,
                            exec_id=exec_id,
                        )
                    )
                    return node

            
            if not node.state.evaluated:
                
                resolved_result = None
                try:
                    if node.resolve_output: resolved_result = self.dep_tools.in_place_dependency_resolver(self._unscope_data_ref, node, result) 
                except Exception as e:
                    print("Could not resolve output!")
                    raise e
                
                self.node_processor._fill_cache(node, result, resolved_result)        

                runtime_effects = (
                    getattr(meta, "runtime_effects", ()) or ()
                )
                self._emit_in_function_effects(
                    node,
                    exec_id,
                    runtime_effects,
                )

                if getattr(meta, "handled_raise", False):
                    handled_effects = [
                        effect
                        for effect in runtime_effects
                        if not (
                            isinstance(effect, dict)
                            and effect.get("effect") in {
                                Effects.READ,
                                Effects.DELETE,
                            }
                        )
                    ]
                    try:
                        for effect in handled_effects:
                            if not (
                                isinstance(effect, Effect)
                                or (
                                    isinstance(effect, type)
                                    and issubclass(effect, Effect)
                                )
                            ):
                                raise TypeError(
                                    "RAISE effects must be TIDE Effect "
                                    f"objects or classes, got "
                                    f"{type(effect).__name__}"
                                )
                        self._handle_node_result(
                            node,
                            exec_id,
                            effects=handled_effects,
                            store_result=True,
                            preserve_effect_output_to_node_targets=False,
                        )
                    except Exception as handling_error:
                        raise RaiseHandlingError(
                            node.id,
                            meta.error,
                            handling_error,
                        ) from handling_error
                else:
                    self._handle_node_result(node, exec_id)

                self._register_emitted_nodes(node.id, node.emitted_nodes, exec_id)  


                self.node_processor._satisfy(node)  

                # DedupedList node as evaluated
                if node._pending_unresolve_inputs:
                    
                    for cause, pending in dict(node._pending_unresolve_inputs.items()):
                        exec_id, data_ref, cause_effect = cause
                        affected_inputs = pending
                        
                        for input in affected_inputs:
                            node._unresolve_input(input)
                        del node._pending_unresolve_inputs[cause]
                        self.event_emitter(Event(source="runner", source_id=self.id, event_type=LogEvents.NODE_BLOCKED, payload=(node_id, data_ref, node.dep_to_input_map[data_ref], cause_effect), exec_id =exec_id ))
                    
                
            # remove from running nodes queue                                   
            
            
            
            node.state.executing = False
            self.running_nodes.pop(node_id) 

            return node
        except Exception as e:
            raise e

    def on_dep_available(self, event): # propogate dependencies and possibly ready node for execution
        # get store and key
        store, key, producer_node, *_= event.payload
        producer_context = f"from:{producer_node}" if producer_node else "?"
        store = self._scope_store_reference(store)
        # convert store and target into a DataRef
        available_dep_ref = self.dep_tools._create_data_ref(store, key)      

        # static deps
        self.runtime_dag.satisfy_downstream_deps(available_dep_ref)
        if available_dep_ref in self.runtime_dag.waiting_static_deps:
            del self.runtime_dag.waiting_static_deps[available_dep_ref]
            
        # dynamic deps
        for dep, node_ids in dict(self.runtime_dag.waiting_dynamic_deps).items():
            # if available dep matches a pattern in dynamic dep
            if self.dep_tools._pattern_match(available_dep_ref.key, dep.key):
                for node_id in DedupedList(node_ids):
                    # Get dependent nodes
                    node: RunnerNode = self.index.node_index.node_by_id[node_id]
                    # Link available_dep to dynamic_dep 
                    node.dynamic_to_static_deps[dep].append(available_dep_ref)
                    # Link dynamic_dep to available_dep
                    node.static_to_dynamic_deps[available_dep_ref].append(dep)
                    # Add available_dep to reverse_map[node_id]
                    self.runtime_dag.reverse_map[available_dep_ref].append(node_id)

                    print(f"[RNTIME_CONCRETISER][{self.id}][Node[{node_id}]] DynamicDep[{dep}] count:[{dep.count}] touched by {available_dep_ref} {producer_context}")

                    for input_name, input in node.dep_to_input_map[dep]:
                        scoped_ref = self._scope_data_ref(available_dep_ref)
                        if [input_name, input] in node.dep_to_input_map[scoped_ref]:
                            continue
                        node.dep_to_input_map[scoped_ref].append([input_name, input])

                    
                    # get dynamic_dep's count predicate
                    count = 0 if not dep.count else dep.count

                    # Check if dynamic_dep count is satisfied
                    if len(node.dynamic_to_static_deps[dep]) >= count: 
                        for input_name, input in node.dep_to_input_map[dep]:
                            self.event_emitter(Event(source="runner", source_id=self.id, target_runner_id=self.id, event_type=LogEvents.DEP_CONCRETISED, payload=(node_id, input_name, dep, node.dynamic_to_static_deps[dep])))
                        # satisfy deps on dependent nodes in forward_map
                        self.runtime_dag.satisfy_downstream_deps(dep)
                        # remove node from dynamic_dep waiting DedupedList
                        self.runtime_dag.waiting_dynamic_deps[dep].remove(node_id) 
                        # if no waiting nodes, remove dynamic_dep from waiting_deps
                        if not self.runtime_dag.waiting_dynamic_deps[dep]:    
                            del self.runtime_dag.waiting_dynamic_deps[dep]
                    
                        print(f"[RNTIME_CONCRETISER][{self.id}][Node[{node_id}]] satisfied DynamicDep[{dep}] count:[{dep.count}] ready! got {available_dep_ref} {producer_context}")     
                    print(f"[{self.id}] Dependency {available_dep_ref} satisfied Dep:{dep} on Node[{node_id}]")   
        return
    
    def on_dep_delete(self, event): # block node execution/invalidate ready deps on nodes
        # get store, and key to delete
        exec_id = event.exec_id
        unscoped_store, key, _, cause = event.payload
        store = self._scope_store_reference(unscoped_store)
        # convert to DataRef
        data_ref = self.dep_tools._create_data_ref(store, key)
        deleting_node_id = self.exec_id_to_node_id.get(exec_id)
        
        # if dep in reverse_map
        if self.runtime_dag.reverse_map[data_ref]:
            
            print(f"[DEP_DELETED][{self.id}]: {key} has been deleted, adding back to nodes")
            # for node_id dependent on ref
            for node_id in DedupedList(self.runtime_dag.reverse_map[data_ref]):

                
                # if ref not in forward_map[node_id]
                if data_ref not in self.runtime_dag.forward_map[node_id]:
                    # get node object
                    node: RunnerNode = self.index.node_index.node_by_id[node_id]

                    # A completed occurrence keeps the State it consumed.
                    if (
                        cause == Effects.DELETE
                        and node_id == deleting_node_id
                        and node.state.evaluated
                    ):
                        continue
                    
                    if node.one_shot and node.state.evaluated:
                        print("[DELETE] Node is one shot and already evaluated, not re-evaluating")
                        continue
                           
                    # remove node from ready queue
                    if node_id in self.runtime_dag.ready_nodes_queue:
                        self.runtime_dag.ready_nodes_queue.remove(node_id)
                    
                    print(f"[DEP_DELETED][{self.id}]: {key} has been added back to node [{node_id}] {node.node_name}")
                    

                    # unlink dependency from dynamic dependencies
                    if data_ref in node.static_to_dynamic_deps:
                        # Remove link between ref and node if not a declared node dep || reason: a node depends on the dynamic dep - not its dynamic deps produced refs  
                        if data_ref not in node.deps:
                            self.runtime_dag.reverse_map[data_ref].remove(node.id)

                        # If dep is linked to a dynamic dep - remove links between them
                        for dy_dep in DedupedList(node.static_to_dynamic_deps[data_ref]):
                            # Remove ref from dynamic_dep satisfaction register
                            node.dynamic_to_static_deps[dy_dep].remove(data_ref)
                            # Remove link between ref and dynamic_dep
                            node.static_to_dynamic_deps[data_ref].remove(dy_dep)
                            
                            print(f"[DEP_DELETED][{self.id}] Static dep {data_ref} removed from dynamic {dy_dep} aggregation DedupedList" )
                            
                            # if dy_dep's count is now unsatisfied
                            concretised_ref_count =  len(node.dynamic_to_static_deps[dy_dep])
                            required_count = dy_dep.count if dy_dep.count is not None else 1

                            if concretised_ref_count < required_count:
                                # Add the dynamic dep back to forward_map
                                self.runtime_dag.forward_map[node_id].append(dy_dep)
                                # Add dynamic dep back to waiting_deps
                                self.runtime_dag.waiting_dynamic_deps[dy_dep].append(node_id)
                                print(f"[DEP_DELETED][{self.id}] Dynamic dep {dy_dep} count of :{dy_dep.count}: now unsatisfied. Current count: {concretised_ref_count} " )

                    # if ref in node's declared deps 
                    if data_ref in node.deps:
                        # add it back
                        self.runtime_dag.forward_map[node_id].append(data_ref)
                        # register back as waiting_dep
                        self.runtime_dag.waiting_static_deps[data_ref].append(node_id)
                    
                    # Unresolve node inputs dependent on ref (partial-recomputation)
                    
                    for input_name, input in node.dep_to_input_map[data_ref]:
                        print(f"[DEP_DELETED][{self.id}] Un-resolved input {input_name} on Node[{node.id}]")
                        if node.state.executing:
                            node._pending_unresolve_inputs[exec_id, self._unscope_data_ref(data_ref), Effects.DELETE].append(input)
                        else:
                            node._unresolve_input(input)    
                    if not node.state.executing:
                        self.event_emitter(Event(source="runner", source_id=self.id, event_type=LogEvents.NODE_BLOCKED, payload=(node_id, self._unscope_data_ref(data_ref), node.dep_to_input_map[data_ref], cause), exec_id=exec_id))
        return

    def on_dep_update(self, event): # recompute nodes that depend on this dependency (re-run, or make waiting dep available) 
        try:
            exec_id = event.exec_id
            store, key, producer_node, _, cause = event.payload
            store = self._scope_store_reference(store)
            
            # create ref and collect nodes dependenct on ref
            data_ref = self.dep_tools._create_data_ref(store, key)
            dirty_nodes = self._collect_dirty_nodes(data_ref)
            # for affected_nodes - unsatisfy (invalidate and unresolve) node 
            for node_id in dirty_nodes:
                
                # Unresolve inputs dependent on ref (invalidates)
                node: RunnerNode = self.index.node_index.node_by_id[node_id]    

                # remove node from ready queue
                if node_id in self.runtime_dag.ready_nodes_queue:
                    self.runtime_dag.ready_nodes_queue.remove(node_id)
                
                if node.one_shot and node.state.evaluated:
                    print("[UPDATE] Node is one shot and already evaluated, not re-evaluating")
                    continue

                # add ref back to forward_map and re-register ref as waiting 
                self.runtime_dag.forward_map[node_id].append(data_ref)
                self.runtime_dag.waiting_static_deps[data_ref].append(node_id)

                for input_name, input in node.dep_to_input_map[data_ref]:
                    print(f"[DEP_DELETED][{self.id}] Un-resolved input {input_name} on Node[{node.id}]")
                    if node.state.executing:
                        node._pending_unresolve_inputs[exec_id, self._unscope_data_ref(data_ref), PipelineEvents.DEP_UPDATED].append(input)
                    else:
                        node._unresolve_input(input)  
                # if not node.state.executing:
                #     self.event_emitter(Event(source="runner", source_id=self.id, event_type=LogEvents.NODE_BLOCKED, payload=(node_id, self._unscope_data_ref(data_ref),  node.dep_to_input_map[data_ref], cause), exec_id =exec_id ))
                    
                
            # re-satisfy nodes dependent on ref 
            self.runtime_dag.satisfy_downstream_deps(data_ref)  
            return
        except Exception as e:
            raise e

    
    
    def on_build(self, event):
        
        def __are_deps_available(node: RunnerNode, available_deps: DedupedList = None):
            unavailable = []
            for dep in node.deps:
                # unscope for store checks
                unscoped_dep = self._unscope_data_ref(dep)
                
                # dynamic 
                if dep.dynamic:
                    req_count = dep.count if dep.count is not None else 1
                    if req_count <= len(node.dynamic_to_static_deps[dep]):
                        available = DedupedList(self._scope_data_ref(static_ref) for static_ref in node.dynamic_to_static_deps[dep])
                        available_deps.extend(available)
                        continue          
                    elif self.dep_tools.exists(unscoped_dep):
                        available_deps.append(dep) 
                        continue
                    else:  
                        unavailable.append(dep)
                        continue
                
                # static
                if self.dep_tools.exists(unscoped_dep):
                    available_deps.append(dep)
                    continue
                elif dep in available_deps:
                    available_deps.append(dep)
                    continue
                else:  
                    unavailable.append(dep)

                
            return available_deps, unavailable
        
        rerun = True
        store, key = event.payload
        store = self._scope_store_reference(store)

        data_ref = self.dep_tools._create_data_ref(store, key)
        needed_nodes = self._collect_needed_nodes(data_ref)

        available_nodes = DedupedList()
        blocked_nodes = DedupedList()
        available_deps = DedupedList()
        progress = True

        for node_id in needed_nodes: # TODO-NOTE: Calculate which node to use if 
            
            node = self.index.node_index.node_by_id[node_id]
            available_deps.extend(node.targets)
            
            available_deps, unavailable = __are_deps_available(node, available_deps)
            if unavailable: 
                print(f"[BUILD] Node[{node_id}] WAITING on {unavailable} ")
                blocked_nodes.extend(node_id)
                progress = False
                continue
            else: 
                available_nodes.append(node_id)
                print(f"[BUILD] Node[{node_id}] ready!")
                continue
        
        if not progress:
            print(f"[BUILD] Build WAITING ON [{blocked_nodes}]")
            return

        if progress: # leaving like this for now, end cases shouldnt hit now 
            for node_id in available_nodes:
                node = self.index.node_index.node_by_id[node_id]
                if node.state.evaluated:
                    for target in node.targets:
                        if not self.dep_tools.exists(target): # nd no waiting deps
                            node._invalidate()
                            print(f"[BUILD] Invalidated Node[{node_id}] as evaluated but its target is not available, re-evaluating!")
                            self._on_node_ready(node)
                            break
                    pass
                
                if node.state.evaluated and rerun:
                    node._invalidate()
                    print(f"[BUILD] Invalidated Node[{node_id}] as evaluated but and rerun is True!, re-evaluating!")
                    self._on_node_ready(node)
                    
                    continue
                elif not node.state.evaluated and self.build_mode and not self.runtime_dag.forward_map[node_id]:
                    print(f"[BUILD_MODE] Node[{node_id}] ready!")

                    self._on_node_ready(node)
                    continue
                elif not node.state.evaluated and self.build_mode and self.runtime_dag.forward_map[node_id]:
                    print(f"[BUILD_MODE] Node[{node_id}] WAITING on {self.runtime_dag.forward_map[node_id]}")
                    continue
        return
    
    def on_register_node(self, event):
        payload = event.payload

        if isinstance(payload, Node):
            nodes = [payload]
        elif isinstance(payload, (list, set, tuple)) and all(
            isinstance(node, Node)
            for node in payload
        ):
            nodes = list(payload)
        else:
            raise EventHandlerError(
                "RegisterNodes payload must be a Node or an iterable "
                f"containing only Nodes, got {type(payload).__name__}"
            )

        for definition in nodes:
            try:
                print(f"[{self.id}] registering node")
                runner_node = self.index.node_index._index_node(definition)

                self.node_processor.compile_all_inputs(runner_node)
                self.runtime_dag.register_node(
                    runner_node,
                    runner_node.deps,
                )
            except Exception as e:
                raise NodeToDagRegistrationError(
                    f"[{self.id}] Failed to register node "
                    f"{definition.node_name}: {e}"
                ) from e

    ####################
    # NODE INP MANAGER #
    ####################    
    def _compile_node_inputs(self, node: Node) -> None:
        if not node:
            raise PipelineRuntimeError("Ready_task [node] in runner was None!") 

        if not node.state.compiled:
            try:
                self.node_processor.compile_all_inputs(node) 
                node.state.compiled = True
            except Exception as e:
                raise PipelineRuntimeError(f"[{node.id}] Node failed to compile at runtime!: {e}") from e

    def _emit_read_events_from_reads(self, reads: dict, exec_id: str):
        for input_name, read in reads.items():
            if read:
                read_event = Event(source="runner", source_id=self.id, event_type=LogEvents.READ, payload=(input_name, read), exec_id=exec_id)
                self.event_emitter(read_event)

    def _represent_unnamed_inputs(
        self,
        node,
        exec_id,
        reads,
        unrepresented,
    ):
        """Represent plain runtime inputs as ordinary versioned TIDE states."""
        exec_token = exec_id.split(":", 1)[-1]
        node_name = "".join(
            character
            if character.isalnum() or character in "-_."
            else "_"
            for character in str(node.node_name)
        )

        for input_name, occurrences in unrepresented.items():
            for occurrence, (value, snapshot, input_path) in enumerate(
                occurrences
            ):
                # The observer is a distinct execution occurrence, but its
                # identity must be assigned when the observation happens.
                # Causal reconstruction may replay this event log more than
                # once (notably across consecutive direct TIDE.exec calls),
                # so assigning an ID during reconstruction would invent a new
                # observer execution and State version on every replay.
                observer_exec_id = create_tide_exec_uuid()
                key = (
                    f"{node_name}.{input_name}.{occurrence}.{exec_token}"
                )
                ref = self._unscope_data_ref(
                    DataRef(key, "__unnamed__")
                )
                write_snapshot = snapshot.at_boundary("write")
                read_snapshot = snapshot.at_boundary("read")

                self.event_emitter(Event(
                    source="TIDE_OBSERVER",
                    source_id="TIDE_OBSERVER",
                    target_runner_id=self.id,
                    event_type=RouterEvents.STORE_WRITE,
                    payload=(
                        ref.store,
                        ref.key,
                        write_snapshot.materialise(),
                        ref.key,
                        "observed unrepresented value",
                        write_snapshot,
                    ),
                    exec_id=observer_exec_id,
                ))
                reads[input_name].append((
                    ref,
                    value,
                    read_snapshot,
                    None,
                    input_path,
                ))
        
    
    def _resolve_node_inputs(self, node: Node) -> None:
        print(f"[{node.id} {node.node_name} starting evalutation!]")

        if not node:
            raise PipelineRuntimeError("Ready_task [node] in runner was None!")

        if not node.state.resolved:
            try:
                read_deps = self.node_processor.resolve_all_inputs(
                    self._unscope_data_ref,
                    node,
                    track_unrepresented=self.track_unrepresented,
                )
                node.state.resolved = True
                return read_deps

                
                
            except (MissingRefDuringResolutionError, DynamicRefCountLessThanAvailableKeysError) as e:
                raise e
            except Exception as e:
                raise PipelineRuntimeError(f"[{node.id}] Node failed to resolve at runtime!: {e}") from e
        
    ######################
    #    NODE MANAGER    #
    ######################
    def _get_ready_node(self):
        ready_node_id = self.runtime_dag.ready_nodes_queue.popleft()
        return self.index.node_index.node_by_id[ready_node_id]
    
    def _process_ready_nodes(self):
        while self.runtime_dag.ready_nodes_queue:
            node: Node = self._get_ready_node()
            if node.state.evaluated:
                continue

            self._on_node_ready(node)
    
    def _on_build_mode(self, node):
        self.ready_build_nodes.append(node)

    def _store_node_result(self, node, exec_id=None, result_override:Any=__TIDE_TEMPLATE_VALUE__()):
        event_type = RouterEvents.STORE_WRITE if not node.recompute else RouterEvents.FORCE_WRITE 

        result_to_store = (node.resolved_result if node.resolve_output else node.result) if isinstance(result_override, __TIDE_TEMPLATE_VALUE__) else result_override 

        cause = "TIDE: node write" if isinstance(result_override, __TIDE_TEMPLATE_VALUE__) else "TIDE: effect"        
   

        if node.targets: # TODO-NOTE: support target lists [x]
            if node.unpack:
                if not isinstance(result_to_store, (list, tuple)):
                    raise NodeDefinitionError(
                        f"Node {node.node_name} uses unpack=True but returned "
                        f"{type(result_to_store).__name__}, not a list or tuple"
                    )
                if len(result_to_store) != len(node.targets):
                    raise NodeDefinitionError(
                        f"Node {node.node_name} uses unpack=True with "
                        f"{len(node.targets)} targets but returned "
                        f"{len(result_to_store)} values"
                    )
                target_values = zip(node.targets, result_to_store, strict=True)
            else:
                target_values = ((target, result_to_store) for target in node.targets)

            for target, target_value in target_values:        
                store_write_event = Event(source="runner", source_id=self.id, target_runner_id=None, event_type=event_type, payload=(target.store, target.key, target_value, node.id, cause, tide_snapshot_value(target_value, boundary="write")), exec_id=exec_id)
                self.event_emitter(store_write_event)
                print(f"Emitting store write event! rnr: {self.id} [{target.store}].set[{target.key}]") 
                
            
    
    def _register_emitted_nodes(self, emitter_id, emitted_nodes: tuple[Node], exec_id):
        emitted = []
        emitted_evidence = []
        for emitted_node in emitted_nodes:
            
            # index node
            if emitted_node.content_id in self.index.node_index.node_by_content_id:
                emitted_node = self.index.node_index.node_by_content_id[emitted_node.content_id]
                emitted_node.emitted = True
                emitted_node.emitted_by.append(emitter_id)

            else:
                emitted_node = self.index.node_index._index_node(emitted_node)
                
                emitted_node.emitted = True
                emitted_node.emitted_by.append(emitter_id)   

                # extract deps
                self.node_processor.compile_all_inputs(emitted_node)

                # register node to dag with deps and release if no deps
            self.runtime_dag.register_node(emitted_node, emitted_node.deps)

            # # try resolve deps
            for dep in emitted_node.deps:
                if dep in self.runtime_dag.waiting_static_deps:
                    continue
                if dep in self.runtime_dag.waiting_dynamic_deps:
                    continue
            
                unscoped_dep = self._unscope_data_ref(dep)
                if self.dep_tools.exists(unscoped_dep):
                    refs = self.dep_tools.get_key_refs(unscoped_dep)
                    if dep.dynamic: emitted_node.dynamic_to_static_deps[dep].extend(refs)
                    for ref in refs:
                        value = self.dep_tools.get(ref)
                        self.event_emitter(Event(source="runner", source_id=self.id, target_runner_id=self.id, event_type=PipelineEvents.DEP_AVAILABLE, payload=(ref.store, ref.key, "TIDE: Polling", value, "TIDE: Emitted Node Polling")))

                        # self.runtime_dag.satisfy_downstream_deps(scoped_expanded_ref)
            emitted.append(emitted_node.id)
            emitted_evidence.append(
                self._snapshot_execution_evidence(emitted_node)
            )

        emitted_node_meta = (
            emitter_id,
            tuple(emitted),
            tuple(emitted_evidence),
        )
        if emitted:
            self.event_emitter(Event(source="runner", source_id=self.id, event_type=LogEvents.NODE_EMITTED, payload=emitted_node_meta, exec_id=exec_id))

    def _record_node_in_history(self, node: Node):
        self.history.append({"id": node.id, "node_name": node.node_name, "required_func_deps" : node.deps ,"args_deps" : node.args.deps, "kwargs_deps" : node.cache.kwargs_input_dependencies, "k_args_deps": node.cache.k_args_input_dependencies,"k_kwargs_deps": node.cache.k_kwargs_input_dependencies, "p_args_deps": node.cache.p_args_input_dependencies, "p_kwargs_deps": node.cache.p_kwargs_input_dependencies,"result_type": type(node.cache.func_cache), "emitted_nodes": [node.id for node in node.cache.func_emitted_nodes],"runtime" : time.time()-self._start_time, "touched_nodes": node.touched_nodes, "released_nodes" : node.released_nodes} ) 
        print(f"--- [{self.history[-1]['id']}] {self.index.node_index.node_by_id[self.history[-1]['id']].node_name} finished in [{self.history[-1]['runtime']:.3f}]s ---\n")      
        print("LOOP", "ready=", DedupedList(self.runtime_dag.ready_nodes_queue), "running=", [n.id for n in self.running_nodes.values()])   

    def _collect_dirty_nodes(self, data_ref: DataRef): # who is affected by dep_change

        dirty_nodes = DedupedList()
        for node_id in self.runtime_dag.reverse_map[data_ref]:
            dirty_nodes.append(node_id)
        
        for node_id in self.runtime_dag.forward_map.keys():
            node: RunnerNode = self.index.node_index.node_by_id[node_id]
            if data_ref in node.static_to_dynamic_deps:
                dirty_nodes.append(node_id)
                continue

        return dirty_nodes
    
    def _collect_needed_nodes(self, data_ref: DataRef, visited=None): # who is needed to build dep
        if visited is None:
            visited = DedupedList()
        if data_ref in visited:
            visited.append(data_ref)

        visited.append(data_ref)
        

        needed_nodes = DedupedList()
        if data_ref in self.index.node_index.node_by_target: # BUG-NOTE: TEST
            for node in self.index.node_index.node_by_target[data_ref]: 
                needed_nodes.append(node.id)
                for dep in node.deps:
                    needed_nodes.extend(self._collect_needed_nodes(dep, visited))
        else:
            if len(visited) == 1: print(f"Build target {data_ref} relies on an external producer! No node produces this target. Build returning.")
        return needed_nodes

    ######################
    # RUNNER STATE MNGMT #
    ######################
    # def log(self, log_str:str):

    #!!!!!!!
    def _poll_waiting_deps(self):
        seen = DedupedList()
        print("Pre-runtime poll of available deps!")
        for dep in DedupedList(self.runtime_dag.waiting_static_deps.keys()):
            unscoped_store_reference = self._unscope_store_reference(dep.store)
            unscoped_ref = self._unscope_data_ref(dep)
            if self.dep_tools.exists(unscoped_ref):
                del self.runtime_dag.waiting_static_deps[dep]
                value = self.dep_tools.get(unscoped_ref)
                
                if (unscoped_store_reference, dep.key) not in seen:
                    seen.extend((unscoped_store_reference, dep.key))
                    self.event_emitter(Event(source="runner", source_id=self.id, target_runner_id=self.id, event_type=PipelineEvents.DEP_AVAILABLE, payload=(unscoped_store_reference, dep.key, "TIDE: Polling", value, "TIDE: Pre-runtime Polling")))
                # self.runtime_dag.satisfy_downstream_deps(dep)
        
        # dynamic dep handling
        for dep, node_ids in dict(self.runtime_dag.waiting_dynamic_deps).items(): # for waiting dyn dep
            unscoped_ref = self._unscope_data_ref(dep)
            if self.dep_tools.exists(unscoped_ref): # if min count is reached    
                got_key_refs = self.dep_tools.get_key_refs(unscoped_ref) # get all key refs that match the pattern
                waiting_node_ids = DedupedList(node_ids)
                
                for ref in DedupedList(got_key_refs): # for each unscoped_ref
                    static_value = self.dep_tools.get(ref) 
                    if (ref.store, ref.key) not in seen:
                        seen.extend((ref.store, ref.key))
                        self.event_emitter(Event(source="runner", source_id=self.id, target_runner_id=self.id, event_type=PipelineEvents.DEP_AVAILABLE, payload=(ref.store, ref.key, "TIDE: Polling", static_value, "TIDE: Pre-runtime Polling")))
                    
                    got_scoped_ref = self._scope_data_ref(ref)
                    for node_id in waiting_node_ids: # for each dependent node
                        
                        node: RunnerNode = self.index.node_index.node_by_id[node_id] # get the node
                        node.dynamic_to_static_deps[dep].append(got_scoped_ref) # register reference in nodes dep maps
                        node.static_to_dynamic_deps[got_scoped_ref].append(dep) 
                        self.runtime_dag.forward_map[node_id].append(got_scoped_ref)
                        self.runtime_dag.reverse_map[got_scoped_ref].append(node_id) # register in reverse_map
                        
                       # for each dependent input of dynamic dep, add got_ref to input
                        for input_name, input in node.dep_to_input_map[dep]:
                            if got_scoped_ref in node.dep_to_input_map and (input_name, input) in node.dep_to_input_map[got_scoped_ref]:
                                continue
                            node.dep_to_input_map[got_scoped_ref].append((input_name, input))

                # Release only after every selected pre-existing match has
                # been concretised.  MIN controls readiness; MAX controls the
                # selected values and may be unbounded.
                required_count = dep.count if dep.count is not None else 1
                for node_id in waiting_node_ids:
                    node = self.index.node_index.node_by_id[node_id]
                    curr_count = len(node.dynamic_to_static_deps[dep])
                    print(f"[RNTIME_DEP_POLL][{self.id}][Node[{node_id}]] DynamicDep[{dep}] curr_count:[{curr_count}]")
                    if curr_count < required_count:
                        continue

                    for input_name, input in node.dep_to_input_map[dep]:
                        self.event_emitter(Event(source="runner", source_id=self.id, target_runner_id=self.id, event_type=LogEvents.DEP_CONCRETISED, payload=(node_id, input_name, dep, node.dynamic_to_static_deps[dep])))

                    if node_id in self.runtime_dag.waiting_dynamic_deps.get(dep, []):
                        self.runtime_dag.waiting_dynamic_deps[dep].remove(node_id)
                    self.runtime_dag.satisfy_downstream_deps(dep)
                    if not self.runtime_dag.waiting_dynamic_deps.get(dep):
                        self.runtime_dag.waiting_dynamic_deps.pop(dep, None)

                    print(f"[RNTIME_DEP_POLL][{self.id}][Node[{node_id}]] satisfied DynamicDep[{dep}] count:[{dep.count}] ready! got {unscoped_ref}")
        self.poll = False
        return
       
    ###### ##############
    # STATE MANAGEMENT #
    ####################
    def _is_idle(self):
        if not self.running_nodes and self.event_queue.empty() and not self.runtime_dag.ready_nodes_queue and not self.store_router.event_queue:
            return True
        else:
            return False

    def deadlock(self) -> bool:
        if not self.runtime_dag.ready_nodes_queue and any(self.runtime_dag.forward_map.values()):
            return True
        return False

    
    def purgatory_check(self):
        inactive = (
            not self.runtime_dag.ready_nodes_queue
            and not self.running_nodes
            and self.event_queue.empty()
        )

        if inactive:
            self.purgatory = True
            return True

        self.attempt_count = 0
        self.purgatory = False
        return False
                    
     
    def _print_waiting_and_done_nodes(self):
        if debug_enabled():
            warn(("Purgatory State - either done or deadlock"))
        print("\nwaiting=")
        for nid, deps in self.runtime_dag.forward_map.items():
            waiting_node = self.index.node_index.node_by_id[nid]                       
            if deps:    
                print(f"{nid} - {waiting_node.node_name}: {tuple(DataRef.to_string(dep, strip_leading=True) for dep in deps)}") 
        print("\ndone=")
        for nid, deps in self.runtime_dag.forward_map.items():  
            done_node = self.index.node_index.node_by_id[nid] 
            evaluated = done_node.state.evaluated                    
            if evaluated:
                print(f"{nid}: {self.index.node_index.node_by_id[nid].node_name}")

    ####################
    # LOG   MANAGEMENT #
    ####################
    def _log_stale_nodes_to_history(self):
        for node_id in self.runtime_dag.forward_map.keys():
            stale_node = self.index.node_index.node_by_id[node_id]
            if not stale_node.resolved_flag:
                self.history.append({"stale_node_id" : stale_node.id, 'node_name': stale_node.node_name, "needed": stale_node.cache.func_input_dependencies})
                print(f"STALE_NODE_ID : {stale_node.id} | {stale_node.node_name} \n" + f"    waiting_on: {self.runtime_dag.forward_map[stale_node.id]} | {DedupedList(self.index.node_index.node_by_target[dep].id if dep in self.index.node_index.node_by_target else 'unknown_dep_producer' for dep in self.runtime_dag.forward_map[stale_node.id])}")
        
        with open('runtime_trace.txt', 'w') as file:
            file.write('DAG deadlock: unresolved tasks remain!:')
            for item in self.history:
                file.write(f"{item}\n")

    def _save_runtime_trace(self):
        with open('runtime_trace.txt', 'w') as file:
            for item in self.history:
                file.write(f"{item}\n")
   
    ################
    # RUN PIPELINE #
    ################
    def tick(self):
        try:
            # Apply already-routed dependency updates/deletions before
            # submitting work from the ready queue. Otherwise a node can be
            # launched with a dependency that was invalidated before this
            # runner tick began.
            self._process_event_queue()

            if not self.build_mode:
                self._process_ready_nodes()


            if self.purgatory_check():
                self.attempt_count += 1
                waiting_nodes_set = self.runtime_dag.forward_map.values()
                no_waiting_nodes = all(not waiting_node_set for waiting_node_set in waiting_nodes_set)
                no_waiting_refs = (not self.runtime_dag.waiting_dynamic_deps and not self.runtime_dag.waiting_static_deps)

                if self.attempt_count == 10:
                    self._print_waiting_and_done_nodes()
                    return
                
                attempt_limit_reached = (self.attempt_count > self.max_attempts)
                if no_waiting_nodes and no_waiting_refs and self.event_queue.empty() and (self.attempt_count > 1000): #event delay buffer:
                    self._print_waiting_and_done_nodes()
                    self.complete = True
                    return

                if attempt_limit_reached and not self.stay_alive:
                    self._print_waiting_and_done_nodes()
                    unresolved = {
                        self.index.node_index.node_by_id[node_id].node_name: [
                            DataRef.to_string(dep, strip_leading=True)
                            for dep in deps
                        ]
                        for node_id, deps in self.runtime_dag.forward_map.items()
                        if (
                            deps
                            and not self.index.node_index
                            .node_by_id[node_id].state.evaluated
                        )
                    }
                    if not unresolved:
                        self.complete = True
                        return
                    raise PipelineDeadlockError(
                        f"Pipeline [{self.id}] cannot make progress; "
                        f"unresolved dependencies: {unresolved}"
                    )

        except (
            MissingRefDuringResolutionError,
            DynamicRefCountLessThanAvailableKeysError,
            PipelineDeadlockError,
            RaiseHandlingError,
        ) as e:
            raise e
        except Exception as e:
            raise PipelineRuntimeError(f"Pipeline [{self.id}] failed: {e}") from e
            
    def run_pipeline(self, dag_to_run): # add parallel?
        dag_to_run = dag_to_run if dag_to_run else self.dag_to_run
        # print("[PipelineRunner] - PIPELINE RUNNING")

    def run(self, stay_alive: bool = True):
        self.stay_alive = stay_alive

        self._start_time = time.time()
        print("[PipelineRunner] - PIPELINE ALIVE")
        #Maybe allow config to list up store state here

        # Poll waiting deps for availability 
        if self.poll:
            self._poll_waiting_deps()
        # index expanded dag 
        self.index.dag_index.expanded_dag = copy.deepcopy(self.runtime_dag)
