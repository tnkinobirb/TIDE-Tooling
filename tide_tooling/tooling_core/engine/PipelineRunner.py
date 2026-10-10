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

print = debug_print

from .index             import PipelineIndex, _project_pipeline_index_to_tide_plan
from .utils             import _node_worker, DedupedList, deduped_dict, create_tide_exec_uuid, ensure_tide_snapshot, tide_snapshot_value
from .DependencyTools   import DependencyTools
from .PipelineIndex     import PipelineIndexBuilder
from .errors            import NodeDefinitionError, PipelineRuntimeError, PipelineDeadlockError, NodeToDagRegistrationError, EventHandlerError, NodeFailed, MissingRefDuringResolutionError, DynamicRefCountLessThanAvailableKeysError, RaiseHandlingError, TIDEError
from .classes           import Node, RunnerNode, RuntimeDag, Pipeline, Event, DataRef, RunnerNodeInput, END, DELETE, WRITE, RUN, RAISE, __fallback__, Effect, EMIT, EffectRuntime
from .NodeProcessor     import NodeProcessor
from .events            import Cause, PipelineEvents, RouterEvents, ExecutorEvents, OceanEvents, LogEvents, Effects
from .event_payloads    import DependencyConcretised, EffectOccurrence, EmittedNodes, MissingReferences, NodeBlocked, Nodes, NodeSnapshot, PipelineRun, Reference, Result, State, Work



class PipelineRunner:
    def __init__(
        self,
        id,
        ocean_id,
        pipeline,
        event_interface,
        store_router,
        now_coordinator,
        synchronous_event_router,
        pipeline_lookup,
        parallel="thread",
        dynamic_ref_plan=None,
        name=None,
    ):
        # A Runner executes one concrete Pipeline definition.
        if not isinstance(pipeline, Pipeline):
            raise NodeDefinitionError(
                "PipelineRunner requires a Pipeline"
            )

        # Retain runner identity and execution configuration.
        self.id = id
        self.ocean_id = ocean_id
        self.name = name
        self.pipeline = pipeline
        self.parallel = parallel
        self.dynamic_ref_plan = dynamic_ref_plan

        # Retain lifecycle and live execution state.
        self.poll = True
        self.complete = False
        self.stay_alive = False
        self.build_mode = False
        self.purgatory = False
        self._end_requested = False
        self.running_nodes = {}
        self.completed_node_ids = set()
        self.ready_node_causes = {}
        # Assign the ordinary execution identity when one readiness attempt
        # begins.  The same identity is carried by NODE_READY, RESULT_READY,
        # NODE_BLOCKED, or MISSING_REFS; the causal Event remains in ``cause``.
        self.ready_exec_ids = {}
        self.failure = None
        self.deadlock = None
        self.deadlock_reported = False

        # Receive routed Events and emit resulting Events through Ocean.
        self.event_queue = Queue()
        self.event_emitter = event_interface.emit_event_to_ocean
        self.now_coordinator = now_coordinator
        self.event_router_route = synchronous_event_router
        self.pipeline_lookup = pipeline_lookup

        # Resolve this Runner's Stores through Ocean's StoreRouter and registry.
        self.store_router = store_router
        self.dep_tools = DependencyTools(self.store_router)
        self.node_processor = NodeProcessor(self.dep_tools)

        # Index Pipeline intent and project its TIDE plan.
        self.index = PipelineIndexBuilder(
            self.pipeline,
            self.node_processor,
        ).build()
        self._tide_plan = self._create_plan()

        # Copy indexed dependencies into mutable runtime readiness.
        self.runtime_dag = RuntimeDag(self.index.intent_dag)

        # Handle only Events routed to this Runner.
        self.event_handlers = {
            PipelineEvents.DEP_AVAILABLE: self.on_dep_available,
            PipelineEvents.DEP_DELETED: self.on_dep_delete,
            PipelineEvents.DEP_UPDATED: self.on_dep_update,
            PipelineEvents.RESULT_READY: self._on_node_completion,
            PipelineEvents.REGISTER_NODE: self.on_register_node,
            PipelineEvents.RUN_NODE: self.on_node_run,
            PipelineEvents.BUILD: self.on_build,
        }

    def _unscope_data_ref(self, ref):
        # Convert Pipeline intent into this Runner's actual Store reference.
        store = self.store_router.registry.resolve_runner_store(
            self.id,
            ref.store,
        )
        return DataRef(
            key=ref.key,
            store=store,
            count=ref.count,
            max=ref.max,
            literal=ref.literal,
            prefix=ref.prefix,
        )

    def _scope_data_ref(self, ref):
        # Convert this Runner's actual Store reference back into Pipeline intent.
        store = self.store_router.registry.resolve_pipeline_store(
            self.id,
            ref.store,
        )
        return DataRef(
            key=ref.key,
            store=store,
            count=ref.count,
            max=ref.max,
            literal=ref.literal,
            prefix=ref.prefix,
        )

    def _create_plan(self):
        # Retain this Runner's indexed TIDE plan and dynamic-ref plan.
        return _project_pipeline_index_to_tide_plan(
            self.index,
            self.dynamic_ref_plan,
            runner_id=self.id,
            resolve_ref=self._unscope_data_ref,
        )

    def consume_event(self, event):
        self.event_queue.put(event)

    def _process_event_queue(self):
        while True:
            try:
                event = self.event_queue.get_nowait()
            except Empty:
                return

            try:
                self.event_handlers[event.type](event)
            except RaiseHandlingError:
                raise
            except Exception as error:
                raise EventHandlerError(
                    f"Error while handling Event {event.event_id}"
                ) from error
            finally:
                self.event_queue.task_done()

    def _remember_node_ready_cause(self, node_ids, event):
        for node_id in node_ids or ():
            self.ready_node_causes[node_id] = Cause.from_event(event)
            if node_id not in self.ready_exec_ids:
                self.ready_exec_ids[node_id] = create_tide_exec_uuid()

    def remember_initial_ready_nodes(self, event):
        self._remember_node_ready_cause(self.runtime_dag.ready_nodes_queue, event)

    def _concretise_dep_link(self, node, dynamic_ref, concrete_ref):
        # Link one concrete reference through every dynamic dependency index.
        node.dynamic_to_static_deps[dynamic_ref].append(concrete_ref)
        node.static_to_dynamic_deps[concrete_ref].append(dynamic_ref)
        self.runtime_dag.reverse_map[concrete_ref].append(node.id)

        for input_name, node_input in node.dep_to_input_map[dynamic_ref]:
            node.dep_to_input_map[concrete_ref].append((input_name, node_input))

    def on_dep_available(self, event):
        # Satisfy nodes waiting for this exact static reference.
        available_ref = self._scope_data_ref(event.payload.ref)
        released_nodes = self.runtime_dag.satisfy_dependency(available_ref)
        self._remember_node_ready_cause(released_nodes, event)

        # Add this reference to every persistent dynamic dependency.
        for dynamic_ref, node_ids in tuple(self.runtime_dag.reverse_map.items()):
            if not dynamic_ref.dynamic:
                continue

            same_store = available_ref.store == dynamic_ref.store
            matching_key = self.dep_tools.matches(available_ref.key, dynamic_ref.key)

            if not (same_store and matching_key):
                continue

            required = 1 if dynamic_ref.count is None else dynamic_ref.count
            satisfied = False
            waiting_node_ids = self.runtime_dag.waiting_dynamic_deps.get(
                dynamic_ref,
                (),
            )

            for node_id in node_ids:
                node = self.index.node_index.node_by_id[node_id]
                self._concretise_dep_link(node, dynamic_ref, available_ref)

                # Already-satisfied nodes retain the candidate without re-readying.
                if node_id not in waiting_node_ids:
                    continue

                if (len(node.dynamic_to_static_deps[dynamic_ref]) < required):
                    continue

                satisfied = True

                # Record the concrete references selected for each input.
                concrete_refs = tuple(
                    self._unscope_data_ref(ref)
                    for ref in node.dynamic_to_static_deps[dynamic_ref][
                        :dynamic_ref.max
                    ]
                )

                for input_name, _ in node.dep_to_input_map[dynamic_ref]:
                    self.event_emitter(Event(
                        ocean_id=self.ocean_id,
                        source="runner",
                        source_id=self.id,
                        type=LogEvents.DEP_CONCRETISED,
                        cause=Cause.from_event(event),
                        payload=DependencyConcretised(
                            input_name,
                            self._unscope_data_ref(dynamic_ref),
                            concrete_refs,
                            self._snapshot_execution_evidence(node),
                        ),
                        exec_id=event.exec_id,
                    ))

            # Equal dynamic predicates become satisfied together.
            if satisfied:
                released_nodes = self.runtime_dag.satisfy_dependency(dynamic_ref)
                self._remember_node_ready_cause(released_nodes, event)

    def on_dep_delete(self, event):
        deleted_ref = self._scope_data_ref(event.payload.ref)

        # Check every node that has depended on the deleted State.
        for node_id in tuple(self.runtime_dag.reverse_map.get(deleted_ref, ())):
            node = self.index.node_index.node_by_id[node_id]
            missing_dependencies = self.runtime_dag.forward_map[node_id]

            # Ignore nodes that can never execute again.
            if node.status == "exhausted":
                self.ready_exec_ids.pop(node_id, None)
                continue

            unsatisfied_dependencies = DedupedList()

            # Remove the deleted State from its dynamic dependencies.
            for dynamic_ref in tuple(node.static_to_dynamic_deps.get(deleted_ref, ())):
                node.dynamic_to_static_deps[dynamic_ref].remove(deleted_ref)
                node.static_to_dynamic_deps[deleted_ref].remove(dynamic_ref)

                required = 1 if dynamic_ref.count is None else dynamic_ref.count

                # Record only dependencies that cross SATISFIED -> UNSATISFIED.
                below_minimum = len(node.dynamic_to_static_deps[dynamic_ref]) < required
                if below_minimum and dynamic_ref not in missing_dependencies:
                    unsatisfied_dependencies.append(dynamic_ref)

            # A deleted exact dependency also becomes unsatisfied.
            if deleted_ref in node.deps and deleted_ref not in missing_dependencies:
                unsatisfied_dependencies.append(deleted_ref)

            # Stop when no dependency became unsatisfied.
            if not unsatisfied_dependencies:
                continue

            # If executing, remember the dependencies and change nothing else.
            if node.executing:
                node._pending_deleted_dependencies.extend(unsatisfied_dependencies)
                continue

            # Otherwise unsatisfy the dependencies and unresolve their inputs.
            was_ready = not missing_dependencies
            for dependency in unsatisfied_dependencies:
                self.runtime_dag.unsatisfy_dependency(node_id, dependency)

                for _, node_input in node.dep_to_input_map[dependency]:
                    node._unresolve_input(node_input)

            self.ready_node_causes.pop(node_id, None)

            # Record the one READY -> BLOCKED transition.
            if was_ready:
                blocked_exec_id = self.ready_exec_ids.pop(node_id, None)
                input_names = tuple(dict.fromkeys(
                    input_name
                    for dependency in unsatisfied_dependencies
                    for input_name, _ in node.dep_to_input_map[dependency]
                ))
                self.event_emitter(Event(
                    ocean_id=self.ocean_id,
                    source="runner",
                    source_id=self.id,
                    type=LogEvents.NODE_BLOCKED,
                    cause=Cause.from_event(event),
                    payload=NodeBlocked(
                        self._unscope_data_ref(deleted_ref),
                        input_names,
                        self._snapshot_execution_evidence(node),
                    ),
                    exec_id=blocked_exec_id,
                ))

    def on_dep_update(self, event):
        updated_ref = self._scope_data_ref(event.payload.ref)

        # Check every node that currently consumes the updated State.
        for node_id in tuple(self.runtime_dag.reverse_map.get(updated_ref, ())):
            node = self.index.node_index.node_by_id[node_id]
            missing_dependencies = self.runtime_dag.forward_map[node_id]

            # Ignore nodes that can never execute again.
            if node.status == "exhausted":
                self.ready_exec_ids.pop(node_id, None)
                continue

            updated_dependencies = DedupedList()

            # Collect the exact and dynamic dependencies affected by the update.
            if updated_ref in node.deps and updated_ref not in missing_dependencies:
                updated_dependencies.append(updated_ref)

            for dynamic_ref in node.static_to_dynamic_deps.get(updated_ref, ()):
                if dynamic_ref not in missing_dependencies:
                    updated_dependencies.append(dynamic_ref)

            # Stop when this node no longer consumes the State.
            if not updated_dependencies:
                continue

            # If executing, remember the updated dependencies and change nothing else.
            if node.executing:
                node._pending_updated_dependencies.extend(updated_dependencies)
                continue

            was_ready = not missing_dependencies
            self.ready_node_causes.pop(node_id, None)

            # Invalidate every affected dependency before checking availability.
            for dependency in updated_dependencies:
                self.runtime_dag.unsatisfy_dependency(node_id, dependency)

                for _, node_input in node.dep_to_input_map[dependency]:
                    node._unresolve_input(node_input)

            # Record the one READY -> BLOCKED transition.
            if was_ready:
                blocked_exec_id = self.ready_exec_ids.pop(node_id, None)
                input_names = tuple(dict.fromkeys(
                    input_name
                    for dependency in updated_dependencies
                    for input_name, _ in node.dep_to_input_map[dependency]
                ))
                self.event_emitter(Event(
                    ocean_id=self.ocean_id,
                    source="runner",
                    source_id=self.id,
                    type=LogEvents.NODE_BLOCKED,
                    cause=Cause.from_event(event),
                    payload=NodeBlocked(
                        self._unscope_data_ref(updated_ref),
                        input_names,
                        self._snapshot_execution_evidence(node),
                    ),
                    exec_id=blocked_exec_id,
                ))

            # A still-available dependency becomes satisfied again.
            for dependency in updated_dependencies:
                # Leave the dependency blocked if it is no longer available.
                if not self.dep_tools.is_available(self._unscope_data_ref(dependency)):
                    continue

                released = self.runtime_dag.satisfy_dependency(dependency)
                self._remember_node_ready_cause(released, event)

    def _poll_waiting_deps(self):
        existing = {}

        # Find every available static dependency.
        for dependency in tuple(self.runtime_dag.waiting_static_deps):
            ref = self._unscope_data_ref(dependency)

            if self.dep_tools.is_available(ref):
                existing[ref] = self.dep_tools.resolve_exact(ref)

        # Link every existing dynamic member before availability can release a node.
        for dependency, node_ids in tuple(self.runtime_dag.waiting_dynamic_deps.items()):
            ref = self._unscope_data_ref(dependency)

            for concrete_ref in self.dep_tools.matching_refs(ref):
                existing[concrete_ref] = self.dep_tools.resolve_exact(concrete_ref)
                scoped_ref = self._scope_data_ref(concrete_ref)

                for node_id in tuple(node_ids):
                    node = self.index.node_index.node_by_id[node_id]
                    self._concretise_dep_link(node, dependency, scoped_ref)

        # Send every pre-existing State through normal dependency handling.
        for ref, value in existing.items():
            self.event_emitter(Event(
                ocean_id=self.ocean_id,
                source="runner",
                source_id=self.id,
                type=PipelineEvents.DEP_AVAILABLE,
                cause=Cause(self.runner_start_event_id, str(OceanEvents.RUN_PIPELINE)),
                target_runner_id=self.id,
                payload=State(
                    ref,
                    value,
                    tide_snapshot_value(value, boundary="write"),
                    state_occurrence=False,
                    value_type=type(value),
                ),
            ))

        self.poll = False

    def _resolve_node_inputs(self, node):
        if node is None:
            raise PipelineRuntimeError("Ready node was None")

        reads, unrepresented = self.node_processor.resolve_all_inputs(
            self._unscope_data_ref,
            node,
        )
        return reads, unrepresented

    def _emit_missing_references(self, node, cause, error, exec_id):
        # Normalize the Store observations made by the failed resolution.
        available_refs = tuple(
            self._unscope_data_ref(
                ref
                if isinstance(ref, DataRef)
                else DataRef(ref, error.ref.store)
            )
            for ref in error.available_keys
        )

        # Record the attempted dispatch without inventing an execution.
        missing_event = Event(
            ocean_id=self.ocean_id,
            source="runner",
            source_id=self.id,
            type=LogEvents.MISSING_REFS,
            cause=cause,
            payload=MissingReferences(
                self._unscope_data_ref(error.ref),
                available_refs,
                self._snapshot_execution_evidence(node),
            ),
            exec_id=exec_id,
        )
        self.event_emitter(missing_event)
        return missing_event

    def _on_node_ready(self, node, cause):
        # An exhausted one-shot node cannot create another execution.
        if node.status == "exhausted":
            return

        exec_id = self.ready_exec_ids.pop(node.id, None)
        if exec_id is None:
            exec_id = create_tide_exec_uuid()

        # Resolve inputs before creating an execution identity.
        try:
            reads, unrepresented = self._resolve_node_inputs(node)
        except DynamicRefCountLessThanAvailableKeysError as error:
            missing_event = self._emit_missing_references(
                node,
                cause,
                error,
                exec_id,
            )
            available_keys = set(error.available_keys)

            for concrete_ref in node.dynamic_to_static_deps[error.ref]:
                if concrete_ref.key not in available_keys:
                    self.event_emitter(Event(
                        ocean_id=self.ocean_id,
                        source="runner",
                        source_id=self.id,
                        type=PipelineEvents.DEP_DELETED,
                        cause=Cause.from_event(missing_event),
                        target_runner_id=self.id,
                        payload=Reference(self._unscope_data_ref(concrete_ref)),
                        exec_id=exec_id,
                    ))
            return

        except MissingRefDuringResolutionError as error:
            missing_event = self._emit_missing_references(
                node,
                cause,
                error,
                exec_id,
            )
            self.event_emitter(Event(
                ocean_id=self.ocean_id,
                source="runner",
                source_id=self.id,
                type=PipelineEvents.DEP_DELETED,
                cause=Cause.from_event(missing_event),
                target_runner_id=self.id,
                payload=Reference(self._unscope_data_ref(error.ref)),
                exec_id=exec_id,
            ))
            return

        # Assign identity only once resolution has genuinely reached execution.
        self._represent_untracked_inputs(node, exec_id, reads, unrepresented, cause)

        # Freeze reconstruction evidence and construct live Executor work.
        node_snapshot = self._snapshot_execution_evidence(node, reads)
        runtime = self._create_effect_runtime(exec_id)
        work = Work(node.func, node.args.resolved, node.kwargs.resolved, self.parallel, runtime, node_snapshot)

        # Register all live state before publishing the execution.
        node.executing = True
        ready_event = Event(ocean_id=self.ocean_id, source="runner", source_id=self.id, type=ExecutorEvents.NODE_READY, cause=cause, payload=work, exec_id=exec_id)
        runtime.node_ready_event_id = ready_event.event_id
        self.running_nodes[node.id] = node
        self.event_emitter(ready_event)

    def _represent_untracked_inputs(
        self,
        runner_node,
        exec_id,
        reads,
        unrepresented,
        cause,
    ):
        # Build stable reference components for this pending execution.
        exec_token = exec_id.split(":", 1)[-1]
        node_name = "".join(
            character
            if character.isalnum() or character in "-_."
            else "_"
            for character in str(runner_node.node_name)
        )

        # Represent each otherwise-untracked input occurrence as a TIDE State.
        for input_name, occurrences in unrepresented.items():
            for occurrence, (value, value_snapshot, input_path) in enumerate(occurrences):
                observer_exec_id = create_tide_exec_uuid()
                key = f"{node_name}.{input_name}.{occurrence}.{exec_token}"
                ref = self._unscope_data_ref(DataRef(key=key, store="__untracked__"))
                write_snapshot = value_snapshot.at_boundary("write")
                read_snapshot = value_snapshot.at_boundary("read")

                # The observer produces the otherwise-unrepresented State.
                self.event_emitter(Event(
                    ocean_id=self.ocean_id,
                    source="TIDE: External",
                    source_id="TIDE: External",
                    type=RouterEvents.STORE_WRITE,
                    cause=Cause(cause.event_id, "TIDE: untracked input"),
                    target_runner_id=self.id,
                    payload=State(
                        ref,
                        write_snapshot.restore(),
                        write_snapshot,
                        value_type=type(value),
                    ),
                    exec_id=observer_exec_id,
                ))

                # Attach the genuine input occurrence to its synthetic observer State.
                reads[input_name].append((
                    ref,
                    value,
                    read_snapshot,
                    None,
                    input_path,
                ))

    def _snapshot_execution_evidence(
        self,
        runner_node: RunnerNode,
        reads=None,
    ):
        # Snapshot each live object once while retaining boundary-specific views.
        snapshots_by_identity = {}

        def snapshot(value, boundary):
            identity = id(value)
            cached = snapshots_by_identity.get(identity)
            if cached is None:
                captured = tide_snapshot_value(
                    value,
                    boundary=boundary,
                )
                # Keep the original object alive for as long as its id is a
                # cache key.  Several callers pass temporary containers, and
                # Python may otherwise recycle their ids during this capture.
                snapshots_by_identity[identity] = (value, captured)
            else:
                original, captured = cached
                if original is not value:
                    raise RuntimeError("Snapshot identity collision")

            return captured.at_boundary(boundary)

        # Preserve every representation of each runtime input.
        inputs = {}
        for input_name, node_input in runner_node.get_all_inputs().items():
            dependencies = []
            for dependency in node_input.deps:
                unscoped = self._unscope_data_ref(dependency)
                dependencies.append({
                    "dependency": snapshot(
                        dependency,
                        f"dispatch.{input_name}.dependency",
                    ),
                    "unscoped_reference": DataRef.to_string(
                        unscoped,
                        prefix=False,
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

        # Preserve each dynamic dependency and its selected concrete references.
        dynamic_dependencies = []
        for dynamic_dependency, static_dependencies in runner_node.dynamic_to_static_deps.items():
            selected_dependencies = static_dependencies[
                :dynamic_dependency.max
            ]
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
                        "unscoped_reference": DataRef.to_string(
                            self._unscope_data_ref(static_dependency),
                            prefix=False,
                        ),
                    }
                    for static_dependency in selected_dependencies
                ],
            })

        # Retain every actual Store touched by declared or concretised references.
        references = [
            *runner_node.targets,
            *runner_node.deps,
            *[
                dependency
                for dependencies in runner_node.dynamic_to_static_deps.values()
                for dependency in dependencies
            ],
        ]
        unscoped_store_refs = []
        for reference in references:
            store = self._unscope_data_ref(reference).store
            if store not in unscoped_store_refs:
                unscoped_store_refs.append(store)

        # Preserve the exact reads selected for this dispatch.
        read_receipts = []
        for input_name, occurrences in (reads or {}).items():
            for ref, value, read_snapshot, parent, input_path in occurrences:
                read_receipts.append({
                    "input_name": input_name,
                    "reference": DataRef.to_string(
                        ref,
                        prefix=False,
                    ),
                    "value": snapshot(
                        value,
                        "dispatch.read.value",
                    ),
                    "snapshot": ensure_tide_snapshot(
                        read_snapshot,
                        boundary="read",
                    ),
                    "parent": (
                        self._unscope_data_ref(parent)
                        if parent is not None
                        else None
                    ),
                    "input_path": tuple(input_path or ()),
                })

        # Freeze the complete occurrence evidence consumed by reconstruction.
        return {
            "runner_id": self.id,
            "pipeline_id": self.pipeline.id,
            "node_id": runner_node.id,
            "func": runner_node.func,
            "func_name": runner_node.func.__name__,
            "inputs": inputs,
            "dynamic_dependencies": dynamic_dependencies,
            "declared_deps": snapshot(
                list(runner_node.deps),
                "dispatch.declared_deps",
            ),
            # Effect descriptors are typed Event structure.  Freeze their
            # current graph, then let the single Event codec encode that
            # structure and its embedded runtime values.
            "declared_effects": copy.deepcopy(list(runner_node.effects)),
            "declared_targets": snapshot(
                list(runner_node.targets),
                "dispatch.declared_targets",
            ),
            "unscoped_store_refs": tuple(unscoped_store_refs),
            "base_node_id": runner_node.node.id,
            "base_node_content_id": runner_node.node.content_id,
            "content_id": runner_node.content_id,
            "emitted_by_exec_id": runner_node.emitted_by_exec_id,
            "node_options": {
                "force_update": runner_node.force_update,
                "one_shot": runner_node.one_shot,
                "unpack": runner_node.unpack,
                "share_implicit_target": runner_node.share_implicit_target,
            },
            "reads": tuple(read_receipts),
        }

    def _snapshot_node_definition(self, runner_node: RunnerNode):
        """Freeze only the definition established by NODE_EMITTED.

        Emission does not prove that any dependency was available or used, so
        this payload deliberately contains no resolved input stages, concrete
        dynamic expansions, or read receipts.
        """
        snapshots_by_identity = {}

        def snapshot(value, boundary):
            identity = id(value)
            cached = snapshots_by_identity.get(identity)
            if cached is None:
                captured = tide_snapshot_value(value, boundary=boundary)
                snapshots_by_identity[identity] = (value, captured)
            else:
                original, captured = cached
                if original is not value:
                    raise RuntimeError("Snapshot identity collision")
            return captured.at_boundary(boundary)

        inputs = {}
        for input_name, node_input in runner_node.get_all_inputs().items():
            inputs[input_name] = {
                "raw": snapshot(
                    node_input.raw,
                    f"definition.{input_name}.raw",
                ),
            }

        references = [*runner_node.targets, *runner_node.deps]
        unscoped_store_refs = []
        for reference in references:
            store = self._unscope_data_ref(reference).store
            if store not in unscoped_store_refs:
                unscoped_store_refs.append(store)

        return {
            "runner_id": self.id,
            "pipeline_id": self.pipeline.id,
            "node_id": runner_node.id,
            "func": runner_node.func,
            "func_name": runner_node.func.__name__,
            "inputs": inputs,
            "declared_deps": snapshot(
                list(runner_node.deps),
                "definition.declared_deps",
            ),
            "declared_effects": copy.deepcopy(list(runner_node.effects)),
            "declared_targets": snapshot(
                list(runner_node.targets),
                "definition.declared_targets",
            ),
            "unscoped_store_refs": tuple(unscoped_store_refs),
            "base_node_id": runner_node.node.id,
            "base_node_content_id": runner_node.node.content_id,
            "content_id": runner_node.content_id,
            "emitted_by_exec_id": runner_node.emitted_by_exec_id,
            "node_options": {
                "force_update": runner_node.force_update,
                "one_shot": runner_node.one_shot,
                "unpack": runner_node.unpack,
                "share_implicit_target": runner_node.share_implicit_target,
            },
        }

    def _process_ready_nodes(self):
        if self._end_requested:
            return
        while self.runtime_dag.ready_nodes_queue:
            node_id = self.runtime_dag.ready_nodes_queue.popleft()
            cause = self.ready_node_causes.pop(node_id)
            node = self.index.node_index.node_by_id[node_id]

            if node.status == "exhausted":
                continue

            if not node.evaluated:
                self._on_node_ready(node, cause)

    def _create_effect_runtime(self, exec_id):
        return EffectRuntime(
            exec_id=exec_id,
            default_store=DataRef.default_store,
            read_handler=self._read_now,
            write_handler=self._write_now,
            delete_handler=self._delete_now,
        )

    def _read_now(self, runtime, effect):
        return self.now_coordinator("read", self.id, runtime, effect)

    def _write_now(self, runtime, effect):
        return self.now_coordinator("write", self.id, runtime, effect)

    def _delete_now(self, runtime, effect):
        return self.now_coordinator("delete", self.id, runtime, effect)

    def _apply_now(self, operation, runtime, effect):
        handlers = {
            "read": self._apply_read_now,
            "write": self._apply_write_now,
            "delete": self._apply_delete_now,
        }
        try:
            handler = handlers[operation]
        except KeyError as error:
            raise ValueError(
                f"Unsupported immediate operation: {operation!r}"
            ) from error
        return handler(runtime, effect)

    def _apply_read_now(self, runtime, effect):
        # Resolve the one requested reference through this Runner.
        ref = self._unscope_data_ref(effect.targets[0])
        resolved_effect = effect.__class__(ref, fallback=effect.fallback)

        # Read the live Store once and retain whether the reference existed.
        value = self.store_router.get_store(ref.store).get(ref.key)
        found = not isinstance(value, __fallback__)
        if not found:
            value = effect.fallback

        # Publish the exact observation before the function resumes.
        snapshot = tide_snapshot_value(value, boundary="read")
        self.event_router_route(Event(ocean_id=self.ocean_id, source="runner", source_id=self.id, type=LogEvents.EFFECT, cause=Cause(runtime.node_ready_event_id, str(ExecutorEvents.NODE_READY)), payload=EffectOccurrence(Effects.READ, (ref,), snapshot, "runtime", True, (found,)), exec_id=runtime.exec_id))
        runtime.effects.append(resolved_effect)
        return value

    def _apply_write_now(self, runtime, effect):
        # Resolve the one requested reference through this Runner.
        supplied_ref, value = effect.targets[0]
        ref = self._unscope_data_ref(supplied_ref)
        snapshot = tide_snapshot_value(value, boundary="write")

        found = self.store_router.exists(store=ref.store)

        resolved_effect = WRITE(ref, value)

        effect_event = Event(ocean_id=self.ocean_id, source="runner", source_id=self.id, type=LogEvents.EFFECT, cause=Cause(runtime.node_ready_event_id, str(ExecutorEvents.NODE_READY)), payload=EffectOccurrence(Effects.WRITE, ((ref, value),), effect.output, "runtime", True, found=(found, )), exec_id=runtime.exec_id)
        self.event_router_route(effect_event)

    
        
        # updated=True means this is an update.
        if found:
            updated = self.store_router.get_store(ref.store).set(ref.key, value)
            self.event_router_route(Event(ocean_id=self.ocean_id, source="store_router", source_id=self.store_router.id, type=PipelineEvents.DEP_UPDATED if updated else PipelineEvents.DEP_AVAILABLE, cause=Cause.from_event(effect_event), target_runner_id=self.id, payload=State(ref, value, snapshot, value_type=type(value)), exec_id=runtime.exec_id))

        runtime.effects.append(resolved_effect)
        return found

    def _apply_delete_now(self, runtime, effect):
        # Resolve the one requested reference through this Runner.
        ref = self._unscope_data_ref(effect.targets[0])
        resolved_effect = DELETE(ref)

        # Delete atomically and retain whether the reference existed.
        value = self.store_router.get_store(ref.store).pop(ref.key)
        found = not isinstance(value, __fallback__)

        # Publish the exact outcome before the function resumes.
        effect_event = Event(ocean_id=self.ocean_id, source="runner", source_id=self.id, type=LogEvents.EFFECT, cause=Cause(runtime.node_ready_event_id, str(ExecutorEvents.NODE_READY)), payload=EffectOccurrence(Effects.DELETE, (ref,), found, "runtime", True, (found,)), exec_id=runtime.exec_id)
        self.event_router_route(effect_event)

        # Only a successful deletion creates a State consequence.
        if found:
            self.event_router_route(Event(ocean_id=self.ocean_id, source="store_router", source_id=self.store_router.id, type=PipelineEvents.DEP_DELETED, cause=Cause.from_event(effect_event), target_runner_id=self.id, payload=Reference(ref), exec_id=runtime.exec_id))
        runtime.effects.append(resolved_effect)
        return found

    def _handle_node_result(self, node, meta, result_event_id):
        exec_id = meta.id
        result = node.result
        emitted_occurrences = []
        reference_available = {}

        # A returned Effect may supply the node's normal result.
        returned_effect = result() if isinstance(result, type) and result in {END, DELETE} else result
        if isinstance(returned_effect, Effect):
            if not isinstance(returned_effect.output, __fallback__):
                written_refs = self._store_node_result(node, exec_id, result_event_id, returned_effect.output)
                reference_available.update((ref, True) for ref in written_refs)
        else:
            written_refs = self._store_node_result(node, exec_id, result_event_id)
            reference_available.update((ref, True) for ref in written_refs)

        # Declared Effects always follow the normal result write.
        for effect in node.effects:
            emitted_occurrences.extend(self._handle_effect(effect, origin="declared", cause=Cause(result_event_id, str(PipelineEvents.RESULT_READY)), node=node, exec_id=exec_id, reference_available=reference_available))

        # A controlled RAISE is one runtime occurrence before its fall-through.
        if meta.handled_raise:
            immediate_count = meta.runtime_immediate_effect_count
            raise_effect = RAISE(meta.error, effects=meta.runtime_effects[immediate_count:], output=meta.result)
            emitted_occurrences.extend(self._handle_effect(raise_effect, origin="runtime", cause=Cause(result_event_id, str(PipelineEvents.RESULT_READY)), node=node, exec_id=exec_id, reference_available=reference_available))

        # Otherwise handle the Effect or Nodes returned by the function.
        elif isinstance(returned_effect, Effect):
            emitted_occurrences.extend(self._handle_effect(returned_effect, origin="runtime", cause=Cause(result_event_id, str(PipelineEvents.RESULT_READY)), node=node, exec_id=exec_id, reference_available=reference_available))
        else:
            returned_nodes = tuple(self.node_processor._extract_nodes_from_result(node.result))
            if returned_nodes:
                emitted_occurrences.extend(self._handle_effect(EMIT(returned_nodes), origin="runtime", cause=Cause(result_event_id, str(PipelineEvents.RESULT_READY)), node=node, exec_id=exec_id, reference_available=reference_available))

        return emitted_occurrences

    def _store_node_result(self, node, exec_id, result_event_id, result_override=__fallback__()):
        # Select the value and final write type.
        result = node.result
        if not isinstance(result_override, __fallback__):
            result = result_override

        event_type = RouterEvents.FORCE_WRITE if node.force_update else RouterEvents.STORE_WRITE

        # Align one result with every target or unpack positional results.
        if not node.targets:
            return ()

        if node.unpack:
            if not isinstance(result, (list, tuple)):
                raise NodeDefinitionError(f"Node {node.node_name} uses unpack=True but returned {type(result).__name__}, not a list or tuple")
            if len(result) != len(node.targets):
                raise NodeDefinitionError(f"Node {node.node_name} uses unpack=True with {len(node.targets)} targets but returned {len(result)} values")
            target_values = zip(node.targets, result, strict=True)
        else:
            target_values = ((target, result) for target in node.targets)

        # Queue each write and return its exact reference for Effect ordering.
        written_refs = []
        for target, value in target_values:
            ref = self._unscope_data_ref(target)
            snapshot = tide_snapshot_value(value, boundary="write")
            written_refs.append(ref)
            self.event_emitter(Event(ocean_id=self.ocean_id, source="runner", source_id=self.id, type=event_type, cause=Cause(result_event_id, str(PipelineEvents.RESULT_READY)), target_runner_id=self.id, payload=State(ref, value, snapshot, value_type=type(value)), exec_id=exec_id))

        return tuple(written_refs)

    def _handle_effect(self, effect, *, origin, cause, node, exec_id, reference_available):
        # Normalize only bare Effects that need no arguments.
        if isinstance(effect, type):
            if effect in {END, DELETE}:
                effect = effect()
            else:
                raise TypeError(f"{effect.__name__} requires constructor arguments")

        if not isinstance(effect, Effect):
            raise TypeError(f"Expected Effect, got {type(effect).__name__}")

        found = None

        # Resolve the immutable occurrence operands.
        if isinstance(effect, DELETE):
            supplied_targets = effect.targets or node.targets
            if not supplied_targets:
                raise ValueError("DELETE requires at least one target")

            targets = tuple(dict.fromkeys(self._unscope_data_ref(target) for target in supplied_targets))
            found = tuple(
                reference_available[target]
                if target in reference_available
                else self.store_router.exists(target.store, target.key)
                for target in targets
            )
            reference_available.update(
                (target, False)
                for target in targets
            )

        elif isinstance(effect, WRITE):
            resolved_writes = []
            for supplied_target, value in effect.targets:
                target = self._unscope_data_ref(supplied_target)
                if any(existing_target == target for existing_target, _ in resolved_writes):
                    raise ValueError(f"WRITE targets resolve to the same reference: {target}")
                resolved_writes.append((target, value))

            targets = tuple(resolved_writes)

            found = tuple(
                self.store_router.exists(target.store)
                for target, _ in targets
            )

            reference_available.update(
                (target, target_found)
                for (target, _), target_found
                in zip(targets, found)
            )

        elif isinstance(effect, END):
            targets = (self.id,) if effect.targets is None else effect.targets
        elif isinstance(effect, RUN):
            targets = effect.targets
        elif isinstance(effect, EMIT):
            targets = tuple(emitted_node.content_id for emitted_node in effect.nodes)
        elif isinstance(effect, RAISE):
            targets = ()
        else:
            raise TypeError(f"Unsupported Effect: {type(effect).__name__}")

        # Freeze the Effect occurrence before its consequences.
        output = tide_snapshot_value(effect.output, boundary="effect") if isinstance(effect, RAISE) else effect.output
        effect_event = Event(ocean_id=self.ocean_id, source="runner", source_id=self.id, type=LogEvents.EFFECT, cause=cause, payload=EffectOccurrence(effect.kind, targets, output, origin, False, found), exec_id=exec_id)
        self.event_emitter(effect_event)
        emitted_occurrences = []

        # Emit concrete consequences from the Effect occurrence.
        if isinstance(effect, WRITE):
            event_type = RouterEvents.FORCE_WRITE if node.force_update else RouterEvents.STORE_WRITE
            for target, value in targets:
                snapshot = tide_snapshot_value(value, boundary="write")
                self.event_emitter(Event(ocean_id=self.ocean_id, source="runner", source_id=self.id, type=event_type, cause=Cause.from_event(effect_event), target_runner_id=self.id, payload=State(target, value, snapshot, value_type=type(value)), exec_id=exec_id))

        elif isinstance(effect, DELETE):
            for target in targets:
                self.event_emitter(Event(ocean_id=self.ocean_id, source="runner", source_id=self.id, type=RouterEvents.DELETE, cause=Cause.from_event(effect_event), target_runner_id=self.id, payload=Reference(target), exec_id=exec_id))

        elif isinstance(effect, END):
            if self.id in targets:
                self._end_requested = True
            for runner_id in targets:
                self.event_emitter(Event(ocean_id=self.ocean_id, source="runner", source_id=self.id, type=OceanEvents.KILL_RUNNER, cause=Cause.from_event(effect_event), target_runner_id=runner_id, exec_id=exec_id))

        elif isinstance(effect, RUN):
            for pipeline_id in targets:
                pipeline, _ = self.pipeline_lookup(pipeline_id)
                payload = PipelineRun(
                    pipeline_id,
                    pipeline.name,
                    effect.mode,
                    effect.local_store_templates,
                    effect.stay_alive,
                    effect.runner_name,
                    effect.dynamic_ref_plan,
                )
                self.event_emitter(Event(ocean_id=self.ocean_id, source="runner", source_id=self.id, type=OceanEvents.RUN_PIPELINE, cause=Cause.from_event(effect_event), payload=payload, exec_id=exec_id))

        elif isinstance(effect, EMIT):
            emitted_occurrences.append((effect.nodes, effect_event))

        elif isinstance(effect, RAISE):
            for fallthrough_effect in effect.effects:
                emitted_occurrences.extend(self._handle_effect(fallthrough_effect, origin="runtime", cause=Cause.from_event(effect_event), node=node, exec_id=exec_id, reference_available=reference_available))

        # An Effect output containing Nodes becomes a causal EMIT.
        if not isinstance(effect.output, __fallback__) and not isinstance(effect, EMIT):
            output_nodes = tuple(self.node_processor._extract_nodes_from_result(effect.output))
            if output_nodes:
                emitted_occurrences.extend(self._handle_effect(EMIT(output_nodes), origin=origin, cause=Cause.from_event(effect_event), node=node, exec_id=exec_id, reference_available=reference_available))

        return emitted_occurrences

    def _on_node_completion(self, event):
        meta = event.payload.exec_meta
        node = self.running_nodes.get(meta.node_id)
        if node is None:
            return

        try:
            # Raw failures preserve immediate Effects but perform no result work.
            if meta.status == "failed" and not meta.handled_raise:
                self.failure = meta.error or NodeFailed(node.id, "execution", RuntimeError(f"Node {node.node_name} failed"))
                self.complete = True
                self.event_emitter(Event(ocean_id=self.ocean_id, source="runner", source_id=self.id, type=OceanEvents.KILL_RUNNER, cause=Cause.from_event(event), target_runner_id=self.id, exec_id=event.exec_id))
                return node

            if node.evaluated:
                return node

            # Retain the completed result.
            self.node_processor._fill_cache(node, meta.result)

            # Apply normal-result, declared, and returned/RAISE phases.
            try:
                emitted_occurrences = self._handle_node_result(node, meta, event.event_id)
            except Exception as handling_error:
                if not meta.handled_raise:
                    raise
                raise RaiseHandlingError(node.id, meta.error, handling_error) from handling_error

            self._register_emitted_nodes(emitted_occurrences, event.exec_id)
            self.node_processor._satisfy(node)
            self.completed_node_ids.add(node.id)

            # A completed one-shot node has no possible future execution.
            if node.one_shot:
                node.status = "exhausted"
                node._pending_deleted_dependencies.clear()
                node._pending_updated_dependencies.clear()
                return node

            # The execution has finished, so pending dependency changes may now apply.
            node.executing = False
            deleted_dependencies = tuple(node._pending_deleted_dependencies)
            updated_dependencies = tuple(node._pending_updated_dependencies)
            pending_dependencies = DedupedList(deleted_dependencies)
            pending_dependencies.extend(updated_dependencies)

            # Unsatisfy every dependency changed during execution.
            for dependency in pending_dependencies:
                self.runtime_dag.unsatisfy_dependency(node.id, dependency)

                # Unresolve the inputs mapped to that dependency.
                for _, node_input in node.dep_to_input_map[dependency]:
                    node._unresolve_input(node_input)

            # Updated dependencies become satisfied again only if still available.
            for dependency in updated_dependencies:
                if not self.dep_tools.is_available(self._unscope_data_ref(dependency)):
                    continue

                released = self.runtime_dag.satisfy_dependency(dependency)
                self._remember_node_ready_cause(released, event)

            node._pending_deleted_dependencies.clear()
            node._pending_updated_dependencies.clear()

            return node

        finally:
            # Completion and failure both clear live execution state.
            node.executing = False
            self.running_nodes.pop(node.id, None)

    def _register_emitted_nodes(self, emitted_occurrences, exec_id):
        for emitted_nodes, effect_event in emitted_occurrences:
            emitted_snapshots = []
            existing_states = []

            for emitted_node in emitted_nodes:
                # Reuse an equivalent registered definition or index the new one.
                existing = self.index.node_index.node_by_content_id.get(emitted_node.content_id)
                if existing is not None:
                    emitted_node = existing
                else:
                    emitted_node = self.index.node_index.index_node(
                        emitted_node,
                        emitted_by_exec_id=exec_id,
                    )
                    self.node_processor.compile_all_inputs(emitted_node)

                # Register new runtime work and preserve its readiness cause.
                registered = self.runtime_dag.register_node(emitted_node)
                if registered and emitted_node.id in self.runtime_dag.ready_nodes_queue:
                    self._remember_node_ready_cause(
                        (emitted_node.id,),
                        effect_event,
                    )

                # Observe pre-existing values required by a newly registered node.
                if registered:
                    for dep in emitted_node.deps:
                        ref = self._unscope_data_ref(dep)
                        existing_refs = self.dep_tools.matching_refs(ref)
                        for existing_ref in existing_refs:
                            value = self.dep_tools.resolve_exact(existing_ref)
                            snapshot = tide_snapshot_value(value, boundary="write")
                            existing_states.append(State(
                                existing_ref,
                                value,
                                snapshot,
                                state_occurrence=False,
                                value_type=type(value),
                            ))

                # Snapshot every occurrence, including reused definitions.
                emitted_snapshots.append(
                    self._snapshot_node_definition(emitted_node)
                )

            # Record the complete emission before releasing its dependencies.
            emitted_event = Event(ocean_id=self.ocean_id, source="runner", source_id=self.id, type=LogEvents.NODE_EMITTED, cause=Cause.from_event(effect_event), payload=EmittedNodes(tuple(emitted_snapshots)), exec_id=exec_id)
            self.event_emitter(emitted_event)

            for state in existing_states:
                self.event_emitter(Event(ocean_id=self.ocean_id, source="runner", source_id=self.id, type=PipelineEvents.DEP_AVAILABLE, cause=Cause.from_event(emitted_event), target_runner_id=self.id, payload=state))

    def on_register_node(self, event):
        if not isinstance(event.payload, Nodes):
            raise EventHandlerError(f"REGISTER_NODE requires a Nodes payload, got {type(event.payload).__name__}")

        for definition in event.payload.nodes:
            try:
                # Convert the definition into registered runtime work.
                runner_node = self.index.node_index.index_node(definition)
                self.node_processor.compile_all_inputs(runner_node)
                registered = self.runtime_dag.register_node(runner_node)

                # A dependency-free node was readied by this command.
                if registered and runner_node.id in self.runtime_dag.ready_nodes_queue:
                    self._remember_node_ready_cause(
                        (runner_node.id,),
                        event,
                    )

            except Exception as error:
                raise NodeToDagRegistrationError(f"Runner {self.id!r} failed to register node {definition.node_name!r}: {error}") from error

    def on_node_run(self, event):
        if not isinstance(event.payload, NodeSnapshot):
            raise EventHandlerError(f"RUN_NODE requires a NodeSnapshot payload, got {type(event.payload).__name__}")

        # Explicit node execution belongs only to build mode.
        if not self.build_mode:
            return

        node_id = event.payload.node_id
        if node_id not in self.index.node_index.node_by_id:
            return

        node = self.index.node_index.node_by_id[node_id]

        if node.status == "exhausted":
            return

        # Do not duplicate work or bypass unresolved dependencies.
        if node.executing or self.runtime_dag.forward_map.get(node_id, ()):
            return

        # Direct dispatch consumes any readiness already queued for this node.
        try:
            self.runtime_dag.ready_nodes_queue.remove(node_id)
        except ValueError:
            pass
        self.ready_node_causes.pop(node_id, None)

        # RUN_NODE means the requested execution must be observable.
        if node.evaluated:
            node._invalidate()

        self._on_node_ready(node, Cause.from_event(event))

    def on_build(self, event):
        if not isinstance(event.payload, Reference):
            raise EventHandlerError(f"BUILD requires a Reference payload, got {type(event.payload).__name__}")

        # BUILD addresses the producer of one exact Runner-scoped reference.
        target = self._scope_data_ref(event.payload.ref)
        producers = self.index.node_index.node_by_target.get(target, ())

        for node in producers:
            if node.id not in self.index.node_index.node_by_id:
                continue

            if node.status == "exhausted":
                continue

            # RuntimeDag remains the authority for whether work can run now.
            if node.executing or self.runtime_dag.forward_map.get(node.id, ()):
                continue

            # Direct dispatch consumes any readiness already queued for this node.
            try:
                self.runtime_dag.ready_nodes_queue.remove(node.id)
            except ValueError:
                pass
            self.ready_node_causes.pop(node.id, None)

            # BUILD always makes an observable new execution when ready.
            if node.evaluated:
                node._invalidate()

            self._on_node_ready(node, Cause.from_event(event))

    def _is_idle(self):
        # Check whether this Runner has work to process.
        return (
            not self.running_nodes
            and self.event_queue.empty()
            and not self.runtime_dag.ready_nodes_queue
        )

    def purgatory_check(self):
        # Retain local inactivity for Ocean's scheduling decisions.
        self.purgatory = self._is_idle()

        if not self.purgatory:
            self.complete = False

        return self.purgatory

    def _deadlock_waiting(self):
        # Describe every definition still waiting at global settlement.
        unresolved = defaultdict(DedupedList)
        for node_id, deps in self.runtime_dag.forward_map.items():
            if not deps or node_id in self.completed_node_ids:
                continue
            node_definition_id = self.index.node_index.node_by_id[
                node_id
            ].node.id
            unresolved[node_definition_id].extend(
                self._unscope_data_ref(dep)
                for dep in deps
            )
        return {
            node_definition_id: tuple(dependencies)
            for node_definition_id, dependencies in unresolved.items()
        }

    def _deadlock_error(self):
        # Dependency state classifies a settled outcome; it does not decide settlement.
        unresolved = {
            str(node_definition_id): tuple(
                DataRef.to_string(dep, prefix=False)
                for dep in deps
            )
            for node_definition_id, deps in self._deadlock_waiting().items()
        }

        if not unresolved:
            return None

        return PipelineDeadlockError(
            f"Pipeline [{self.id}] cannot make progress; unresolved dependencies: {unresolved}"
        )

    def tick(self):
        try:
            # Apply routed State changes before starting ready work.
            self._process_event_queue()

            if not self.build_mode:
                self._process_ready_nodes()

            self.purgatory_check()

        except RaiseHandlingError:
            raise
        except Exception as error:
            raise PipelineRuntimeError(f"Pipeline [{self.id}] failed: {error}") from error

    def run(self, stay_alive=True):
        self.stay_alive = stay_alive

        # Represent Store values that predate this Runner.
        if self.poll:
            self._poll_waiting_deps()

    def on_kill(self, event):
        # Preserve the final runtime structure for inspection.
        self.index.post_runtime_dag = copy.deepcopy(self.runtime_dag)
        self.stay_alive = False
