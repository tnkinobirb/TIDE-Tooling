from __future__ import annotations

"""Capture and reconstruction of runtime evidence into a TIDE ``Trace``.

This module owns mutable log-building machinery.  The immutable/queryable
``Trace`` remains in ``tide_logging`` and is imported only when reconstruction
actually creates one, avoiding a module-ownership cycle.  ``tide_logging``
re-exports these classes for pre-alpha compatibility.
"""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from tide_tooling.tooling_core.engine.PipelineRunner import PipelineRunner

import copy
from collections import defaultdict

from tide_tooling.tooling_core import debug_print
from tide_tooling.tooling_core.engine.classes import (
    DataRef,
    RunnerNode,
    RunnerNodeInput,
    __TIDE_TEMPLATE_VALUE__,
)
from tide_tooling.tooling_core.engine.errors import MissingExecutionEvidenceError
from tide_tooling.tooling_core.engine.events import (
    Effects,
    ExecutorEvents,
    LogEvents,
    OceanEvents,
    PipelineEvents,
    RouterEvents,
    event_type_is,
)
from tide_tooling.tooling_core.engine.executor import ExecMeta
from tide_tooling.tooling_core.engine.tide_records import (
    Actor,
    AffectedRecord,
    DependencyUseRecord,
    EffectRecord,
    EmittedNodeRecord,
    ExecRecord,
    ExpandedDepRecord,
    InputUseRecord,
    Inputs,
    WriteRecord,
)
from tide_tooling.tooling_core.engine.utils import (
    TIDEValueSnapshot,
    create_tide_exec_uuid,
    deduped_dict,
    ensure_tide_snapshot,
    tide_snapshot_value,
    tide_value_hasher,
)


class ExecutionLog:
    _exec_counter = 0 
    def __init__(self, ocean_id):
        self.ocean_id = ocean_id
        self.exec_log = list()
        self.producer_to_exec_id = {}
        self.exec_id_to_log = {}
        self.ref_to_last_producer = {}
        self.ref_to_last_value = {}
        self.ref_to_last_snapshot = {}
        self.last_concretised_as = {} #dynamic dep -> 'concretised' static refs (deps)
        self.last_emitted_by = {}
        self.effects_produced_by_exec = defaultdict(list) #exec_id to effects produced
        self.versioned_effect_to_effect_obect = defaultdict(list)
        self.last_blocked_by = {}
        self.last_blocked = {}
        self.exec_opaque_value_snapshots = {}


    

    def next_seq_id(self):
        id = len(self.exec_log)
        return id

    def _producer_actor(self, producer_exec_id, producer_source, producer_node):
        producer_record = self.exec_id_to_log.get(producer_exec_id)
        if producer_record is not None:
            return producer_record.producer_id
        return Actor(self.ocean_id, producer_source, producer_node)

    def _unscope_str_ref(self, runner, ref):
        unscoped_store = runner._unscope_store_reference(ref.store)
        return f"{unscoped_store}:{ref.key}"

    @staticmethod
    def _materialise_execution_evidence(value):
        if isinstance(value, TIDEValueSnapshot):
            return value.materialise()
        return copy.deepcopy(value)

    def _create_input_log_from_evidence(self, evidence):
        """Build input records from one immutable dispatch occurrence."""
        all_node_deps = []
        inputs_log = {}
        dynamic_dependencies = evidence.get("dynamic_dependencies", ())

        def expanded_dependencies(dynamic_dependency):
            for expansion in dynamic_dependencies:
                captured_dependency = self._materialise_execution_evidence(
                    expansion["dependency"]
                )
                if captured_dependency == dynamic_dependency:
                    return expansion.get("expanded", ())
            return ()

        for name, captured_input in evidence.get("inputs", {}).items():
            raw = self._materialise_execution_evidence(
                captured_input["raw"]
            )
            evaluated = self._materialise_execution_evidence(
                captured_input["evaluated"]
            )
            normalised = self._materialise_execution_evidence(
                captured_input["normalised"]
            )
            resolved = self._materialise_execution_evidence(
                captured_input["resolved"]
            )
            frozen_runner_input = RunnerNodeInput(raw=raw)
            frozen_runner_input.evaluated = evaluated
            frozen_runner_input.normalised = normalised
            frozen_runner_input.resolved = resolved

            input_record = InputUseRecord(
                name=name,
                raw=raw,
                evaluated=evaluated,
                normalised=normalised,
                resolved=resolved,
                dependencies=[],
            )
            associated_input = (name, frozen_runner_input)
            data_refs = []

            for index, dependency_entry in enumerate(
                captured_input.get("dependencies", ())
            ):
                dependency = self._materialise_execution_evidence(
                    dependency_entry["dependency"]
                )
                unscoped_ref = dependency_entry["unscoped_reference"]
                unknown_source = ("External", "Unknown", None)
                writer_info = self.ref_to_last_producer.get(
                    unscoped_ref,
                    unknown_source,
                )
                producer_source, producer_node, producer_version = writer_info
                producer_exec_id = self.producer_to_exec_id.get(
                    (
                        producer_source,
                        producer_node,
                        producer_version,
                    ),
                    writer_info,
                )

                if dependency.dynamic:
                    expanded_records = []
                    for expanded_entry in expanded_dependencies(dependency):
                        static_dependency = (
                            self._materialise_execution_evidence(
                                expanded_entry["dependency"]
                            )
                        )
                        static_ref = expanded_entry[
                            "unscoped_reference"
                        ]
                        static_writer = self.ref_to_last_producer.get(
                            static_ref,
                            unknown_source,
                        )
                        (
                            static_source,
                            static_node,
                            static_version,
                        ) = static_writer
                        static_exec_id = self.producer_to_exec_id.get(
                            (
                                static_source,
                                static_node,
                                static_version,
                            ),
                            static_writer,
                        )
                        expanded_records.append(ExpandedDepRecord(
                            associated_input=associated_input,
                            dep=static_dependency,
                            type="static",
                            unscoped_ref=static_ref,
                            resolved_val=self.ref_to_last_value.get(
                                static_ref,
                                (None, None),
                            )[0],
                            resolved_ver=static_version,
                            producer_source=static_source,
                            producer_actor=self._producer_actor(
                                static_exec_id,
                                static_source,
                                static_node,
                            ),
                            producer_exec_id=static_exec_id,
                            expanded_from=dependency,
                            input_index=index,
                        ))

                    data_refs.append(DependencyUseRecord(
                        associated_input=associated_input,
                        dep=dependency,
                        type="dynamic",
                        unscoped_ref=unscoped_ref,
                        expanded_to=expanded_records,
                        input_index=index,
                    ))
                    continue

                data_refs.append(DependencyUseRecord(
                    associated_input=associated_input,
                    dep=dependency,
                    type="static",
                    unscoped_ref=unscoped_ref,
                    resolved_val=self.ref_to_last_value.get(
                        unscoped_ref,
                        (None, None),
                    )[0],
                    resolved_ver=producer_version,
                    producer_source=producer_source,
                    producer_actor=self._producer_actor(
                        producer_exec_id,
                        producer_source,
                        producer_node,
                    ),
                    producer_exec_id=producer_exec_id,
                    input_index=index,
                ))

            input_record.dependencies = data_refs
            inputs_log[name] = input_record
            all_node_deps.extend(data_refs)

        return Inputs(inputs_log), all_node_deps

    def create_input_log(
        self,
        execution_meta_data,
    ):
        evidence = getattr(
            execution_meta_data,
            "execution_evidence",
            None,
        )
        if evidence is None:
            exec_id = getattr(execution_meta_data, "id", "unknown")
            raise MissingExecutionEvidenceError(
                f"Execution {exec_id} has no immutable execution evidence"
            )

        return self._create_input_log_from_evidence(evidence)

    # def create_targets_logs 
    
    # def create_targets_logs 
    
    def create_effects_log(self, effects_produced_by_exec: tuple):
        effects_log = []
     
        for effect, targets in effects_produced_by_exec:
            effect_entry = EffectRecord(
                effect=effect,
                targets=targets
                )
            effects_log.append(effect_entry)
        return effects_log

    def append_runtime_read_dependency(self, exec_id, reference, value):
        """Record an observed READ as a normal, post-runtime dependency."""
        exec_record = self.exec_id_to_log[exec_id]
        input_name = "runtime_read"

        producer_info = self.ref_to_last_producer.get(reference)
        if producer_info is None:
            # A fallback READ is still preserved as EffectRecord evidence, but
            # there is no producer/version from which a graph edge can exist.
            return None

        input_record = getattr(exec_record.inputs, input_name, None)
        if input_record is None:
            input_record = InputUseRecord(
                name=input_name,
                raw=None,
                evaluated=None,
                normalised=None,
                resolved=None,
                dependencies=[],
            )
            setattr(exec_record.inputs, input_name, input_record)

        producer_source, producer_node, producer_ver = producer_info
        producer_exec_id = None
        if producer_ver is not None:
            producer_exec_id = self.producer_to_exec_id.get(
                (producer_source, producer_node, producer_ver)
            )

        dependency = DependencyUseRecord(
            associated_input=(input_name, input_record),
            dep=DataRef.from_string(reference, strip_leading=False),
            type="static",
            unscoped_ref=reference,
            resolved_val=value,
            resolved_ver=producer_ver,
            producer_source=producer_source,
            producer_actor=self._producer_actor(
                producer_exec_id,
                producer_source,
                producer_node,
            ),
            producer_exec_id=producer_exec_id,
            input_index=len(input_record.dependencies),
        )
        input_record.dependencies.append(dependency)
        exec_record.deps.append(dependency)
        return dependency

    @staticmethod
    def _resolved_dependency_occurrences(exec_record, input_name):
        input_record = getattr(exec_record.inputs, input_name, None)
        if input_record is None:
            return []

        occurrences = []
        for dependency in input_record.dependencies:
            if dependency.type == "dynamic":
                occurrences.extend(dependency.expanded_to or ())
            else:
                occurrences.append(dependency)
        return occurrences

    def apply_observed_reads(self, exec_id, observations):
        """Make realised READ receipts authoritative for dependency values."""
        exec_record = self.exec_id_to_log[exec_id]
        consumed_occurrences = set()

        for observation in observations:
            input_name = observation["input_name"]
            reference = observation["reference"]
            input_record = getattr(exec_record.inputs, input_name, None)

            if input_record is None:
                input_record = InputUseRecord(
                    input_name,
                    None,
                    None,
                    None,
                    None,
                    [],
                )
                setattr(exec_record.inputs, input_name, input_record)

            matched = None
            created = False
            for dependency in self._resolved_dependency_occurrences(
                exec_record,
                input_name,
            ):
                if id(dependency) in consumed_occurrences:
                    continue
                if dependency.unscoped_reference == reference:
                    matched = dependency
                    break

            if matched is None:
                matched = DependencyUseRecord(
                    associated_input=(input_name, input_record),
                    dep=DataRef.from_string(
                        reference,
                        strip_leading=False,
                    ),
                    type="static",
                    unscoped_ref=reference,
                    input_index=observation.get("input_index"),
                )
                input_record.dependencies.append(matched)
                exec_record.deps.append(matched)
                created = True

            consumed_occurrences.add(id(matched))
            producer_source, producer_node, producer_version = observation[
                "producer"
            ]
            producer_exec_id = observation.get("producer_exec_id")

            if created:
                matched.associated_input = (input_name, input_record)
            matched.unscoped_reference = reference
            matched.resolved_value = observation["value"]
            matched.resolved_version = observation["version"]
            matched.producer_source = producer_source
            matched.producer_actor = self._producer_actor(
                producer_exec_id,
                producer_source,
                producer_node,
            )
            matched.producer_exec_id = producer_exec_id
            matched.input_path = observation.get("input_path")
            matched.snapshot_lineage = list(
                observation.get("snapshot_lineage", ())
            )
            matched.observed_mutable = observation.get(
                "observed_mutable",
                False,
            )

        # A dynamic dependency may track more matching candidates than the
        # node actually receives so MIN can be evaluated independently from
        # MAX.  Keep only the realised READ occurrences in the public
        # provenance record.
        for dependency in exec_record.deps:
            if dependency.type != "dynamic":
                continue
            dependency.expanded_to = [
                expanded
                for expanded in dependency.expanded_to or ()
                if id(expanded) in consumed_occurrences
            ]

        return exec_record


    def log(self, seq_id, runner: PipelineRunner=None, node: RunnerNode=None, execution_meta_data:ExecMeta=None, use_assigned_id=False):
        try:
            template_value = __TIDE_TEMPLATE_VALUE__()
            unknown = execution_meta_data is None
            unknown_repr = repr(template_value)

            if unknown:
                if runner is not None or node is not None:
                    raise MissingExecutionEvidenceError(
                        "A runtime node cannot be logged without ExecMeta "
                        "execution evidence"
                    )
                exec_id = (
                    use_assigned_id
                    if use_assigned_id is not False
                    else create_tide_exec_uuid()
                )
                identity = {
                    "runner_id": unknown_repr,
                    "pipeline_id": unknown_repr,
                    "node_id": unknown_repr,
                    "func": unknown_repr,
                    "func_name": unknown_repr,
                    "base_node_id": None,
                    "base_node_content_id": None,
                    "content_id": unknown_repr,
                    "resolve_output": unknown_repr,
                    "node_options": unknown_repr,
                }
                inputs_log = {}
                all_node_deps = []
                declared_deps = []
                declared_effects = []
                declared_targets = []
                runtime_effects = []
                unscoped_store_refs = None
            else:
                exec_id = execution_meta_data.id
                evidence = getattr(
                    execution_meta_data,
                    "execution_evidence",
                    None,
                )
                if evidence is None:
                    raise MissingExecutionEvidenceError(
                        f"Execution {exec_id} has no immutable execution "
                        "evidence"
                    )
                inputs_log, all_node_deps = self.create_input_log(
                    execution_meta_data
                )
                identity = {
                    key: evidence[key]
                    for key in (
                        "runner_id",
                        "pipeline_id",
                        "node_id",
                        "func",
                        "func_name",
                        "base_node_id",
                        "base_node_content_id",
                        "content_id",
                        "resolve_output",
                        "node_options",
                    )
                }
                declared_deps = self._materialise_execution_evidence(
                    evidence["declared_deps"]
                )
                declared_effects = (
                    self._materialise_execution_evidence(
                        evidence["declared_effects"]
                    )
                )
                declared_targets = (
                    self._materialise_execution_evidence(
                        evidence["declared_targets"]
                    )
                )
                unscoped_store_refs = list(
                    evidence["unscoped_store_refs"]
                )

                runtime_effects = copy.deepcopy(
                    execution_meta_data.runtime_effects
                )

            actor = Actor(
                self.ocean_id,
                identity["runner_id"],
                identity["func_name"],
            )
            output_value = (
                ensure_tide_snapshot(
                    execution_meta_data.snapshot,
                    boundary="result",
                ).materialise()
                if execution_meta_data
                else template_value
            )

            log_entry = ExecRecord(
                seq_id=seq_id,
                exec_id=exec_id,
                ocean_id=self.ocean_id,
                runner_id=identity["runner_id"],
                pipeline_id=identity["pipeline_id"],
                node_id=identity["node_id"],
                func=identity["func"],
                func_name=identity["func_name"],
                inputs=inputs_log,
                deps=all_node_deps,
                effects={},
                declared_deps=declared_deps,
                declared_effects=declared_effects,
                declared_targets=declared_targets,
                runtime_effects=runtime_effects,
                # Actual occurrence targets are populated from STORE_WRITE
                # receipts, never from the reusable node's accumulated list.
                runtime_targets=[],
                unscoped_store_refs=unscoped_store_refs,

                status=(
                    unknown_repr
                    if unknown
                    else execution_meta_data.status
                ),
                started_at=(
                    unknown_repr
                    if unknown
                    else execution_meta_data.start_time
                ),
                finished_at=(
                    unknown_repr
                    if unknown
                    else execution_meta_data.end_time
                ),
                duration_ms=(
                    unknown_repr
                    if unknown
                    else execution_meta_data.duration
                ),
                error=None if unknown else execution_meta_data.error,
                handled_raise=(
                    False
                    if unknown
                    else execution_meta_data.handled_raise
                ),

                output_value=output_value,
                resolve_output=identity["resolve_output"],
                writes=[],
                emitted_by=self.last_emitted_by.get(actor, []),
                emitted_nodes=[],
                base_node_id=identity["base_node_id"],
                base_node_content_id=identity[
                    "base_node_content_id"
                ],
                content_id=identity["content_id"],
                node_options=identity["node_options"],
            )
            
            
            self.exec_id_to_log[log_entry.exec_id] = log_entry
            self.exec_log.append(log_entry)
            return log_entry.exec_id

        except Exception as e:
            debug_print(f"Log Error: {e}")
            raise e

    def _retarget_exec_record_identity(self, exec_record, runner_id, node_id, pipeline_id=None):
        actor_name = (
            "TIDE_OBSERVER"
            if runner_id == "TIDE_OBSERVER"
            else node_id
        )
        exec_record.runner_id = runner_id
        exec_record.node_id = node_id
        exec_record.func_name = actor_name

        if hasattr(exec_record, "source_runner"):
            exec_record.source_runner = runner_id

        if pipeline_id is not None:
            if hasattr(exec_record, "source_pipeline"):
                exec_record.source_pipeline = pipeline_id

            if hasattr(exec_record, "pipeline_id"):
                exec_record.pipeline_id = pipeline_id

        exec_record.producer_id = Actor(
            self.ocean_id,
            runner_id,
            actor_name,
        )

    def append_write_log(
        self,
        seq_id,
        exec_id,
        runner_id,
        node_id,
        stored_value: Any,
        unscoped_target,
        versioned_references,
        cause,
        snapshot_lineage=None,
        observed_mutable=False,
    ):
        target_ref = unscoped_target
        
      

        stored_version = versioned_references[target_ref]
        self.ref_to_last_producer[target_ref]= (runner_id, node_id, stored_version)
        self.ref_to_last_value[target_ref] = (stored_value, stored_version)

        if not exec_id:
            exec_id = self.log(seq_id)
            log_entry = self.exec_id_to_log[exec_id]

            self._retarget_exec_record_identity(
                log_entry,
                runner_id,
                node_id,
                cause,
            )
        elif (
            exec_id not in self.exec_id_to_log
            and runner_id == "TIDE_OBSERVER"
            and cause == "observed unrepresented value"
        ):
            # Unrepresented-input observations are emitted before the node's
            # execution receipt and therefore have no ordinary ExecMeta event
            # to create their record. Reuse the existing assigned-ID path so
            # the occurrence identity carried by STORE_WRITE is reconstructed
            # once and remains stable when cumulative event history is replayed.
            self.log(seq_id, use_assigned_id=exec_id)
            log_entry = self.exec_id_to_log[exec_id]
            self._retarget_exec_record_identity(
                log_entry,
                runner_id,
                node_id,
                cause,
            )
            
        if exec_id not in self.exec_id_to_log:
            raise RuntimeError(f"Exec: {exec_id} not in self.exec_id_to_log")
             
        if exec_id and exec_id in self.exec_id_to_log:
            self.producer_to_exec_id[(runner_id, node_id, stored_version)] = exec_id
            log_entry = self.exec_id_to_log[exec_id]
            log_entry.seq_id = seq_id
            runtime_target = DataRef.from_string(
                unscoped_target,
                strip_leading=False,
            )
            if runtime_target not in log_entry.runtime_targets:
                log_entry.runtime_targets.append(runtime_target)
            log_entry.writes.append(WriteRecord(
                cause=cause,
                stored_result=stored_value, 
                stored_version=stored_version, 
                stored_target=unscoped_target,
                snapshot_lineage=snapshot_lineage,
                observed_mutable=observed_mutable,
        ))
            
        return exec_id

    def append_emitted_nodes(
        self,
        exec_id,
        emitted_node_ids,
        emitted_evidence,
    ):
        exec_log: ExecRecord = self.exec_id_to_log[exec_id]        
        if len(emitted_node_ids) != len(emitted_evidence):
            raise MissingExecutionEvidenceError(
                f"Execution {exec_id} emitted-node evidence is incomplete"
            )

        _emitted_nodes = []
        for emitted_node_id, evidence in zip(
            emitted_node_ids,
            emitted_evidence,
            strict=True,
        ):
            if not isinstance(evidence, dict):
                raise MissingExecutionEvidenceError(
                    f"Emitted node {emitted_node_id} has no immutable "
                    "definition evidence"
                )
            if evidence.get("node_id") != emitted_node_id:
                raise MissingExecutionEvidenceError(
                    f"Emitted node evidence mismatch: expected "
                    f"{emitted_node_id}, got {evidence.get('node_id')}"
                )
            inputs_log, all_node_deps = (
                self._create_input_log_from_evidence(evidence)
            )
            declared_targets = self._materialise_execution_evidence(
                evidence["declared_targets"]
            )
            declared_effects = self._materialise_execution_evidence(
                evidence["declared_effects"]
            )
            
            emitted_producer_id = Actor(
                self.ocean_id,
                evidence["runner_id"],
                evidence["func_name"],
            )
            emitter_producer_id = exec_log.producer_id
            self.last_emitted_by[emitted_producer_id] = (emitter_producer_id, exec_id)

            emitted_node_record = EmittedNodeRecord(
                ocean_id = self.ocean_id, 
                runner_id=evidence["runner_id"],
                pipeline_id=evidence["pipeline_id"],
                node_id=evidence["node_id"],
                func=evidence["func"],
                func_name=evidence["func_name"],
                inputs= inputs_log,
                deps=all_node_deps, 
                declared_targets=declared_targets,
                declared_effects=declared_effects,
                emitted_by=(emitter_producer_id, exec_id),
                content_id=evidence["content_id"],
                base_node=evidence["base_node_id"],
                base_node_content_id=evidence[
                    "base_node_content_id"
                ],
                
            )
            _emitted_nodes.append(emitted_node_record)

    
        exec_log.emitted_nodes = _emitted_nodes
        pass


class LogTools:
    def __init__(self, ocean_id=None, runners=None, pipelines=None):
        self.log = []
        self.ocean_id = ocean_id
        self.exec_log: ExecutionLog = ExecutionLog(ocean_id)
        self.runners = runners if runners is not None else {}
        self.pipelines = pipelines if pipelines is not None else {}
        self.dep_to_producer = {}
        self.dep_to_updater = {}
        self.missing_refs_with_no_known_writer = deduped_dict()
        self.registered_poll_records = {}
        
        


    def create_trace(self, runner, event_log):
        from tide_tooling.tooling_core.engine.tide_logging import Trace

        start_id = getattr(runner, "trace_event_start_id", None)
        run_events = (
            event_log
            if start_id is None
            else [entry for entry in event_log if entry["id"] >= start_id]
        )
        exec_log, _ = self.reconstruct_causality(run_events)
        return Trace(run_events, exec_log, runner, self.ocean_id, all_known_runners=self.runners, all_known_pipelines=self.pipelines)
    
    
    def create_log(self, event_log):
        for entry in event_log:
            cleaned_entry = self.clean_entry(entry)
            self.log.append(cleaned_entry)

        # Graph rendering is deliberately outside the core package.  The
        # reconstructed execution log remains available to callers; an
        # external graphing adapter can consume the trace/bundle instead.
        self.reconstruct_causality(event_log)
        
        
        

        # return self.create_trace(event_log)
                

    def find_producers_of_dependencies(self, node):
        producers = []
        for dep in node.deps:
            if dep.dynamic: # what deps did dynamic ref get. who producde those 
                associated_deps = node.dynamic_to_static_deps[dep]
                
                for static_dep in associated_deps:
                    target = (static_dep.store, static_dep.key)   
                    source_runner_id, producer = self.dep_to_producer.get(target) or ("External", None) # If no marked producer, assume External.
                     
                continue

            target = (dep.store, dep.key)   
            source_runner_id, producer = self.dep_to_producer.get(target) or ("External", None) # If no marked producer, assume External.

            if source_runner_id != "External":
                producer: RunnerNode = producer if self.runners[source_runner_id].index.node_index.node_by_id.get(producer) else None
            producers.append((source_runner_id, producer, target))
            continue
        return producers

    def fill_dep_to_updater_map(self, event_log):
        for entry in event_log:
            event_type = entry["event_type"]
            payload = entry["payload"]
            source_id = entry["source_id"]
            # Store Written 
            if event_type in [PipelineEvents.DEP_UPDATED]:
                store, key, producer_node, *_= payload
                self.dep_to_updater[(store, key)].append(source_id, producer_node)

    def fill_dep_to_producer_map(self, event_log):
        for entry in event_log:
            event_type = entry["event_type"]
            payload = entry["payload"]
            source_id = entry["source_id"]
            # Store Written 
            if event_type in [RouterEvents.STORE_WRITE]:
                producer_node, store, key, _ = payload
                self.dep_to_producer[(store, key)] = (source_id, producer_node)
 

    def reconstruct_causality(self, event_log):
        self.exec_log = ExecutionLog(self.ocean_id)
        entry_to_node = defaultdict(list)

        edges = []
        dot_edges = []
        last_writer = {}
        writer_to_version = {}
        pending_reads = defaultdict(list)
        exec_result_snapshots = {}
        # self.fill_dep_to_producer_map(event_log)
        # self.fill_dep_to_updater_map(event_log)
        versioned_references = defaultdict(int)

        # Graphviz used to be constructed here as a side effect of causal
        # reconstruction.  Keep reconstruction independent of presentation
        # dependencies; graphing belongs in an external module.
        dot = None

        def add_edge(a, b):
            # Retain the causal edge collection used by the formatter while
            # intentionally not rendering it in the core package.
            if (a, b) not in dot_edges:
                dot_edges.append((a, b))

        def get_ref_version(ref):
            return versioned_references[ref]

        def version_ref(ref):
            if ref not in versioned_references:
                versioned_references[ref] = 0
            else:
                versioned_references[ref] += 1

            current_version = versioned_references[ref]
            versioned_ref = ref + f" | v{current_version}"
            return versioned_ref

        def next_exec_id():
            return len(self.exec_log.exec_log)

        def snapshot_lineage(*snapshots):
            return [
                snapshot.to_dict()
                for snapshot in snapshots
                if isinstance(snapshot, TIDEValueSnapshot)
            ]

        def snapshot_at_path(snapshot, input_name, input_path):
            if not isinstance(snapshot, TIDEValueSnapshot) or not snapshot.exact:
                return None

            try:
                value = snapshot.value[input_name]
                for part in input_path or ():
                    value = value[part]
                return tide_snapshot_value(
                    value,
                    boundary="post_execution",
                )
            except (KeyError, IndexError, TypeError):
                return None

        def producer_receipt(reference):
            producer = self.exec_log.ref_to_last_producer.get(
                reference,
                ("External", "Unknown", None),
            )
            producer_exec_id = self.exec_log.producer_to_exec_id.get(
                producer,
            )
            return producer, producer_exec_id

        def append_observed_mutation(
            reference,
            snapshot,
            *,
            exec_id,
            runner_id,
            node_id,
            cause,
            lineage,
        ):
            snapshot = snapshot.mark_mutable()
            version_ref(reference)
            value = snapshot.materialise()

            if exec_id not in self.exec_log.exec_id_to_log:
                self.exec_log.log(
                    next_exec_id(),
                    use_assigned_id=exec_id,
                )

            self.exec_log.append_write_log(
                seq_id=self.exec_log.exec_id_to_log[exec_id].seq_id,
                exec_id=exec_id,
                runner_id=runner_id,
                node_id=node_id,
                stored_value=value,
                unscoped_target=reference,
                versioned_references=versioned_references,
                cause=cause,
                snapshot_lineage=lineage,
                observed_mutable=True,
            )
            self.exec_log.ref_to_last_snapshot[reference] = snapshot
            return snapshot
                
        
        # print("event count:", len(event_log))

        # for idx, entry in enumerate(event_log):
        #     event_type = entry["event_type"]
        #     event_exec_id = entry["exec_id"]

        #     payload_exec_id = None
        #     payload_name = None

        #     if event_type == LogEvents.EVENT_EXEC:
        #         payload_exec_id = entry["payload"].exec_id
        #         payload_name = entry["payload"].func_name

        #     print(
        #         idx,
        #         event_type,
        #         "event.exec_id=",
        #         event_exec_id,
        #         "payload.exec_id=",
        #         payload_exec_id,
        #         "payload.name=",
        #         payload_name,
        #     )

        for entry in event_log:
            event_type = entry["event_type"]
            payload = entry["payload"]
            source_id = entry["source_id"]


            if event_type in [LogEvents.EFFECT]:
                if len(payload) == 5:
                    (
                        effect,
                        exec_id,
                        targets,
                        preserved_output,
                        effect_metadata,
                    ) = payload
                else:
                    effect, exec_id, targets, preserved_output = payload
                    effect_metadata = {}

                if isinstance(targets, dict):
                    effect_targets = list(targets.items())
                elif isinstance(targets, (list, tuple, set)):
                    effect_targets = []
                    for target in targets:
                        if (
                            effect == Effects.WRITE
                            and isinstance(target, tuple)
                            and len(target) == 2
                            and isinstance(target[0], DataRef)
                        ):
                            effect_targets.append(target)
                        else:
                            effect_targets.append((target, preserved_output))
                else:
                    effect_targets = [(targets, preserved_output)]

                for data_ref, value in effect_targets:
                    if isinstance(data_ref, DataRef): 
                        s_target = DataRef.to_string(data_ref, strip_leading=True)
                        if effect == Effects.WRITE:
                            ver = (
                                versioned_references[s_target] + 1
                                if s_target in versioned_references
                                else 0
                            )
                        else:
                            ver = versioned_references.get(s_target, 0)
                        versioned = (s_target, ver)
                    else:
                        versioned = ("Runner", 0)

                    self.exec_log.effects_produced_by_exec[exec_id].append((effect, versioned))
                    effect_record = EffectRecord(
                        effect,
                        versioned,
                        output=value,
                        preserved_output=preserved_output,
                    )
                    self.exec_log.exec_id_to_log[exec_id].effects[
                        (effect, versioned)
                    ] = effect_record

                    if (
                        effect == Effects.READ
                        and isinstance(data_ref, DataRef)
                        and effect_metadata.get("found", True)
                    ):
                        self.exec_log.append_runtime_read_dependency(
                            exec_id,
                            s_target,
                            value,
                        )


                continue
            
            if event_type in [LogEvents.DEP_CONCRETISED]:
                node_id, input_name, dynamic_dep, concretised_refs = payload
                self.exec_log.last_concretised_as[(source_id, node_id, input_name, dynamic_dep)] = concretised_refs
                continue
            
            if event_type in [LogEvents.READ]:
                exec_id = entry["exec_id"]
                input_name, reads = payload
                
                for read in reads:
                    if len(read) == 5:
                        ref, _, snapshot, parent, input_path = read
                    else:
                        ref, _, snapshot, parent = read
                        input_path = ()

                    snapshot = ensure_tide_snapshot(
                        snapshot,
                        boundary="read",
                    )
                    ref_str = DataRef.to_string(
                        ref,
                        strip_leading=True,
                    )
                    previous_snapshot = (
                        self.exec_log.ref_to_last_snapshot.get(ref_str)
                    )

                    if (
                        previous_snapshot is not None
                        and previous_snapshot.value_hash
                        != snapshot.value_hash
                    ):
                        observer_exec_id = create_tide_exec_uuid()
                        snapshot = append_observed_mutation(
                            ref_str,
                            snapshot,
                            exec_id=observer_exec_id,
                            runner_id="TIDE_OBSERVER",
                            node_id="observed_mutation",
                            cause="Observed mutable change before read",
                            lineage=snapshot_lineage(
                                previous_snapshot,
                                snapshot,
                            ),
                        )

                    version = versioned_references.get(ref_str, 0)
                    producer, producer_exec_id = producer_receipt(ref_str)
                    pending_reads[exec_id].append({
                        "input_name": input_name,
                        "input_path": tuple(input_path or ()),
                        "reference": ref_str,
                        "version": version,
                        "value": snapshot.materialise(),
                        "snapshot": snapshot,
                        "parent": parent,
                        "producer": producer,
                        "producer_exec_id": producer_exec_id,
                        "snapshot_lineage": snapshot_lineage(
                            previous_snapshot,
                            snapshot,
                        ),
                        "observed_mutable": (
                            previous_snapshot is not None
                            and previous_snapshot.value_hash
                            != snapshot.value_hash
                        ),
                    })
                
                continue

            if event_type in [LogEvents.NODE_BLOCKED]:
                exec_id = entry["exec_id"]
                node_id, blocked_dep, affected_inputs, cause_effect = payload

                blocked_ref = DataRef.to_string(
                    blocked_dep,
                    strip_leading=True,
                )
                versioned_blocker = (
                    blocked_ref,
                    versioned_references[blocked_ref],
                )
                affected = AffectedRecord(
                    cause_effect,
                    versioned_blocker,
                    affectee=Actor(self.ocean_id, source_id, node_id),
                    affected_inputs=dict(affected_inputs),
                )

                is_missing_ref_fallback = (
                    isinstance(
                        cause_effect,
                        (tuple, list, set, frozenset),
                    )
                    and "MissingRefFallback" in cause_effect
                )
                if is_missing_ref_fallback:
                    exec_log: ExecRecord = self.exec_log.exec_id_to_log[exec_id]
                    exec_log.effects[(cause_effect, versioned_blocker)].affected[affected.affectee] = affected
                
                # self.exec_log.versioned_effect_to_effect_obect[(cause_effect, versioned_blocker)].add(affected)
                self.exec_log.last_blocked_by[(source_id, node_id)] = (cause_effect, versioned_blocker) 
                self.exec_log.last_blocked[cause_effect] = ((source_id, node_id), affected_inputs)
                continue

            if event_type in [LogEvents.NODE_EMITTED]:
                exec_id = entry["exec_id"]
                emitter_id, emitted_ids, emitted_evidence = payload
      
                self.exec_log.append_emitted_nodes(
                    exec_id,
                    emitted_ids,
                    emitted_evidence,
                )
                continue
             # For every NODE_READY event
            if event_type in [ExecutorEvents.NODE_READY]:
                node_id, _ = payload
                entry_to_node[node_id].append(entry)
                
                # get ready node (potential consumer)
                consumer_runner = self.runners[source_id]
                consumer_node: RunnerNode = consumer_runner.index.node_index.node_by_id[node_id]  
                # define consumer node
                consumer_node_name = consumer_node.node_name.split("@", 1)[0]
                consumer_node_label = f"{source_id} | {consumer_node_name}#{consumer_node.id}"   
                
                # if consumer was emitted by another node
                if consumer_node.emitted:
                    # get emitter node
                    for emitter_node_id in consumer_node.emitted_by:
                        emitter_node: RunnerNode = self.runners[source_id].index.node_index.node_by_id[emitter_node_id]  
                        emitter_node_name = emitter_node.node_name.split("@", 1)[0]
                        emitter_node_label = f"{source_id} | {emitter_node_name}#{emitter_node.id}"    
                        
                        add_edge(emitter_node_label, consumer_node_label)
                
                for dep, static_deps in consumer_node.dynamic_to_static_deps.items():
                    # what static deps link to dynamic deps
                    _unscoped_dyn_store_ref = consumer_runner._unscope_store_reference(dep.store)
                    dyn_dep_label = f"{_unscoped_dyn_store_ref}:{dep.key} | {dep.count} , {dep.max}"
                    for static_dep in static_deps:
                        _unscoped_store_ref = consumer_runner._unscope_store_reference(static_dep.store)
                        _unscoped_ref = f"{_unscoped_store_ref}:{static_dep.key}"
                        _producer_label = last_writer.get(_unscoped_ref, "External")
                        _version = get_ref_version(_unscoped_ref)
                        _versioned_ref = _unscoped_ref + f" | v{_version}"
                        add_edge(_producer_label, dyn_dep_label)
                        add_edge(dyn_dep_label, _versioned_ref)
                        add_edge(_versioned_ref, consumer_node_label)

                for dep in consumer_node.deps:  
                    if dep.dynamic: continue        
                    dep_store = dep.store
                    dep_key = dep.key      

                    unscoped_store_ref = consumer_runner._unscope_store_reference(dep_store)
                    unscoped_ref = f"{unscoped_store_ref}:{dep_key}"
                
                    version = get_ref_version(unscoped_ref)
                    versioned_producer_node_target = unscoped_ref + f" | v{version}"
                    
                continue
                   
            if event_type in [PipelineEvents.RESULT_READY, LogEvents.EVENT_EXEC]:
                
                
                if event_type == LogEvents.EVENT_EXEC:
                    
                    external_event_exec_entry = payload
              
                    external_event_exec_entry.seq_id = next_exec_id()


                    if external_event_exec_entry.exec_id in self.exec_log.exec_id_to_log:
                        continue

                    self.exec_log.exec_id_to_log[external_event_exec_entry.exec_id] = external_event_exec_entry
                    self.exec_log.exec_log.append(external_event_exec_entry)

                    # Critical: imported event execs also define producer/version truth.
                    for write in external_event_exec_entry.writes:
                        ref = write.stored_target
                        ver = write.stored_version
                        val = write.stored_result

                        self.exec_log.ref_to_last_producer[ref] = (
                            external_event_exec_entry.runner_id,
                            external_event_exec_entry.func_name,
                            ver,
                        )
                        self.exec_log.ref_to_last_value[ref] = (val, ver)
                        imported_snapshot = tide_snapshot_value(
                            val,
                            boundary="write",
                        )
                        self.exec_log.ref_to_last_snapshot[ref] = (
                            imported_snapshot
                        )
                        versioned_references[ref] = ver

                        self.exec_log.producer_to_exec_id[
                            (external_event_exec_entry.runner_id, external_event_exec_entry.func_name, ver)
                        ] = external_event_exec_entry.exec_id
         
                    continue
                producer_node_id, _, exec_meta = payload 
                target_runner_id = entry["target_runner_id"]
                target_runner: PipelineRunner = self.runners[target_runner_id]
                producer_node: RunnerNode = target_runner.index.node_index.node_by_id[producer_node_id]
        

            
                self.exec_log.log(
                    next_exec_id(),
                    target_runner,
                    producer_node,
                    exec_meta,
                )
                exec_result_snapshots[exec_meta.id] = ensure_tide_snapshot(
                    exec_meta.snapshot,
                    boundary="result",
                )

                observations = pending_reads.pop(exec_meta.id, [])
                mutation_writes = []
                seen_mutations = set()
                after_inputs = getattr(
                    exec_meta,
                    "input_snapshot_after",
                    None,
                )

                for observation in observations:
                    post_execution = snapshot_at_path(
                        after_inputs,
                        observation["input_name"],
                        observation["input_path"],
                    )
                    read_snapshot = observation["snapshot"]

                    if (
                        post_execution is None
                        or post_execution.value_hash
                        == read_snapshot.value_hash
                    ):
                        continue

                    observation["observed_mutable"] = True
                    observation["snapshot_lineage"] = snapshot_lineage(
                        read_snapshot,
                        post_execution,
                    )

                    mutation_key = (
                        observation["reference"],
                        post_execution.value_hash,
                    )
                    if mutation_key in seen_mutations:
                        continue
                    seen_mutations.add(mutation_key)

                    # A process worker mutates an isolated copy. Preserve the
                    # observation on the read, but do not invent a persisted
                    # state transition in the parent store.
                    if exec_meta.execution_mode != "process":
                        mutation_writes.append((
                            observation["reference"],
                            post_execution,
                            observation["snapshot_lineage"],
                        ))

                self.exec_log.apply_observed_reads(
                    exec_meta.id,
                    observations,
                )

                for reference, post_execution, lineage in mutation_writes:
                    append_observed_mutation(
                        reference,
                        post_execution,
                        exec_id=exec_meta.id,
                        runner_id=target_runner.id,
                        node_id=producer_node.id,
                        cause="Observed mutation during execution",
                        lineage=lineage,
                    )

                continue
 
            if event_type in [RouterEvents.STORE_WRITE, RouterEvents.FORCE_WRITE]:
                try:
                    exec_id = entry["exec_id"]
                    if len(payload) == 6:
                        (
                            store,
                            key,
                            stored_value,
                            producer_node_id,
                            cause,
                            snapshot,
                        ) = payload
                    else:
                        (
                            store,
                            key,
                            stored_value,
                            producer_node_id,
                            cause,
                        ) = payload
                        snapshot = stored_value

                    snapshot = ensure_tide_snapshot(
                        snapshot,
                        boundary="write",
                    )
                    result_snapshot = exec_result_snapshots.get(exec_id)
                    observed_mutable = bool(
                        result_snapshot is not None
                        and cause == "TIDE: node write"
                        and result_snapshot.value_hash
                        != snapshot.value_hash
                    )
                    if observed_mutable:
                        snapshot = snapshot.mark_mutable()
                    lineage = snapshot_lineage(
                        result_snapshot,
                        snapshot,
                    )
                    historical_value = snapshot.materialise()

                    
                    if isinstance(source_id, (int, str)) and isinstance(producer_node_id, (int, str)):
                        producer_runner: PipelineRunner = self.runners.get(source_id)
                        if producer_runner:
                            unscoped_store_ref = producer_runner._unscope_store_reference(store)
                            unscoped_ref = f"{unscoped_store_ref}:{key}"
                            producer_node: RunnerNode = producer_runner.index.node_index.node_by_id[producer_node_id]
                            producer_node_name = producer_node.node_name.split("@", 1)[0]
                            producer_node_label = f"{producer_runner.id} | {producer_node_name}#{producer_node_id}"   
                        else:
                            unscoped_ref = f"{store}:{key}"
                            producer_node_label = f"{source_id} | {producer_node_id}"  
                            
                    
                        
                    elif source_id == "TIDE: .event()":
                        unscoped_store_ref = store
                        unscoped_ref = f"{unscoped_store_ref}:{key}"
                        producer_node_label = f"{source_id} | {producer_node_id}"
                    else:
                        target_runner_id = entry["target_runner_id"]
                        if target_runner_id == "*":
                            continue
                        
                        target_runner: PipelineRunner = self.runners[target_runner_id]
                        unscoped_store_ref = target_runner._unscope_store_reference(store)
                        unscoped_ref = f"{unscoped_store_ref}:{key}"

                        external_source = (source_id if source_id else "External", producer_node_id if producer_node_id else "unknown")
                        producer_node_label = f"{external_source[0]} | {external_source[1]}"
                    
                    
 

                    versioned_producer_node_target = version_ref(unscoped_ref)
                    ver = versioned_references[unscoped_ref]
                    self.exec_log.ref_to_last_producer[unscoped_ref]= (source_id, producer_node_id, ver)
                    self.exec_log.ref_to_last_value[unscoped_ref] = (
                        historical_value,
                        ver,
                    )
                    self.exec_log.ref_to_last_snapshot[unscoped_ref] = snapshot
      
                    
                    last_writer[unscoped_ref] = producer_node_label
                    # if isinstance(source_id, int) and isinstance(producer_node_id, int):
                    #     last_writer_id[unscoped_ref] = (producer_runner.id, producer_node.id)
                    # else:
                    #     last_writer_id[unscoped_ref] = ("External", "unknown")
                    
                    writer = (source_id, producer_node_id)
                    writer_to_version[writer] = (unscoped_ref, get_ref_version(unscoped_ref))
                    # version_to_writer[(unscoped_ref, get_ref_version(unscoped_ref))]  = writer
                    # 
                    add_edge(producer_node_label, versioned_producer_node_target)
                    
                    self.exec_log.append_write_log(
                        next_exec_id(),
                        exec_id,
                        source_id,
                        producer_node_id,
                        historical_value,
                        unscoped_ref,
                        versioned_references,
                        cause,
                        snapshot_lineage=lineage,
                        observed_mutable=observed_mutable,
                    )
                    
                    continue
                except Exception as e:
                    debug_print(e)
                    raise e                

            # if event_type in [PipelineEvents.DEP_DELETED]:
            #     unscoped_store_ref, key, source, cause = payload

            if event_type in [PipelineEvents.DEP_AVAILABLE, PipelineEvents.DEP_UPDATED]:
                unscoped_store_ref, key, source, value, cause = payload 
                target_ref = f"{unscoped_store_ref}:{key}"
                exec_id = entry["exec_id"]
                creation_time = entry["creation_time"]
                # The originating STORE_WRITE already records observer-created
                # unnamed state. Its follow-up availability notification only
                # wakes runners; treating it as another external production
                # would invent a duplicate state version.
                if cause == "observed unrepresented value":
                    continue
                event_value_hash = tide_value_hasher(value)
                poll_snapshot = tide_snapshot_value(
                    value,
                    boundary="read",
                )
                previous_poll_snapshot = (
                    self.exec_log.ref_to_last_snapshot.get(target_ref)
                )
                if "TIDE:" in cause and "Polling" in cause:
                    debug_print("DEBUG!FOUND NEW POLL RECORD REGISTERED", (target_ref, creation_time, event_value_hash))
                    if (target_ref, creation_time, event_value_hash) in self.registered_poll_records:
                        debug_print("DEBUG! POLL RECORD ALREADY REGISTERED ", (target_ref, creation_time, event_value_hash))
                        poll_exec_log = self.registered_poll_records[(target_ref, creation_time, event_value_hash)]
                        ver = versioned_references[target_ref]
                        self.exec_log.ref_to_last_producer[target_ref]= (source, "TIDE: Polling", ver)
                        historical_value = poll_snapshot.materialise()
                        self.exec_log.ref_to_last_value[target_ref] = (
                            historical_value,
                            ver,
                        )
                        self.exec_log.ref_to_last_snapshot[target_ref] = (
                            poll_snapshot
                        )
                        self.exec_log.exec_id_to_log[poll_exec_log.exec_id]= poll_exec_log
                        self.exec_log.exec_log.append(poll_exec_log)
                        continue
                    else: debug_print(f"DEBUG! NEW POLL TO BE REGISTERED {(target_ref, creation_time, event_value_hash)}")
                    if target_ref in self.exec_log.ref_to_last_value:
                        if (
                            previous_poll_snapshot is not None
                            and previous_poll_snapshot.value_hash
                            == event_value_hash
                        ):
                            continue
                        pass
                    
                    
                        
                    
                    # if event_type == PipelineEvents.DEP_AVAILABLE and source != "TIDE: External":
                    #     continue

                if not exec_id: # last_writer.get(target_ref) and not self.exec_log.ref_to_last_producer.get(target_ref)
                    version_ref(target_ref)
                    ver = versioned_references[target_ref]
                    self.exec_log.ref_to_last_producer[target_ref]= (source, None, ver)
                    historical_value = poll_snapshot.materialise()
                    self.exec_log.ref_to_last_value[target_ref] = (
                        historical_value,
                        ver,
                    )
                    self.exec_log.ref_to_last_snapshot[target_ref] = (
                        poll_snapshot
                    )
                    next_seq_id = next_exec_id()
                    poll_exec_id = self.exec_log.append_write_log(
                        next_seq_id,
                        exec_id,
                        source,
                        source,
                        historical_value,
                        target_ref,
                        versioned_references,
                        cause,
                        snapshot_lineage=snapshot_lineage(
                            previous_poll_snapshot,
                            poll_snapshot,
                        ),
                        observed_mutable=previous_poll_snapshot is not None,
                    )
                    if "TIDE:" in cause and "Polling" in cause:
                        debug_print(f"DEBUG! POLL RECORD REGISTERED {(target_ref, creation_time, event_value_hash)}")
                        self.registered_poll_records[(target_ref, creation_time, event_value_hash)] = self.exec_log.exec_id_to_log[poll_exec_id]

                continue

        
        # format edges for printing
        formatted_edges = self.format_edges(edges)
        debug_print(formatted_edges)
        return self.exec_log.exec_log, dot

        


    def format_edges(self, edges):
        lines = []
        # runner_to_edges = defaultdict()
        grouped = defaultdict(lambda: defaultdict(list))
        references = defaultdict(lambda: defaultdict(list))
        # group edges by consumer node
        for edge in edges:
            if edge["type"] == "reference":
                references[edge["to_pipeline"]][edge["to_node"]].append(edge)
                continue
            grouped[edge["to_pipeline"]][(edge["to_node_id"], edge["to_node"])].append(edge)
  

        for to_pipeline, grouped_edges in grouped.items():
            lines.append(f"\n Runner [{to_pipeline}]:")            
                  
            for consumer, _edges in grouped_edges.items():
                to_node_id, to_node = consumer 
                lines.append(f"\n    {to_node_id} | {to_node}:")
                for edge in _edges:
                    store, key = edge["dep"]
                    source = f"[{edge['from_pipeline']} | {edge['from_node_id']} | {edge['from_node']}]"
                    if edge["from_pipeline"] == "External": source = "[External]"
                    lines.append(f"        {source} -> {store}:{key}")

            lines.append("\n    Dynamic References:")    
            for to_node, _edges in references[to_pipeline].items():
                lines.append(f"\n        {to_node}:")
                for edge in _edges:
                    source = f"[{edge['from_pipeline']} | {edge['from_node_id']} | {edge['from_node']}]"
                    if edge["from_pipeline"] == "External": source = "[External]"
                    store, key = edge["dep"]
                    lines.append(f"             {source} -> {store}:{key}")  

        return "\n".join(lines)

           

            
    def clean_entry(self, entry):
        source = entry["source"]
        source_id = entry["source_id"]
        event_type = entry["event_type"]
        target_runner_id = entry["target_runner_id"]
        

        if target_runner_id is None:
            if event_type_is(event_type, ExecutorEvents):
                target_runner_id = f"executor({source_id})"
            if event_type_is(event_type, OceanEvents):
                target_runner_id = f"ocean({source_id})"
            if event_type_is(event_type, RouterEvents):
                target_runner_id = f"store_router({source_id})"
        else:
            target_runner_id = f"runner:{target_runner_id}"

        payload = entry["payload"]
        context = ""
        if event_type == ExecutorEvents.NODE_READY:
            node_id, _ = payload
            context = f"Node[{node_id}] | Ready"
        if event_type == RouterEvents.STORE_WRITE:
            node_id, store, key, value = payload
            context = f"Node[{node_id}] | Stored: {type(value)} -> {store}:{key}"
        if event_type in [PipelineEvents.DEP_UPDATED, PipelineEvents.DEP_AVAILABLE]:
            store, key, producer_node, *_ = payload
            context = f"{event_type.capitalize()} -> {store}:{key} produced by Node[{producer_node}]" 
        if event_type in [PipelineEvents.DEP_DELETED]:
            store, key, _ = payload
            context = f"{event_type.capitalize()} -> {store}:{key}" 
        if event_type == PipelineEvents.RESULT_READY:
            node_id, value, _= payload
            context = f"Node[{node_id}] | Produced -> {type(value)}" 

        log_str = f"[{source}({source_id}) -> {target_runner_id}] {context or event_type} "
        return log_str


class EventLogger:
    def __init__(self):
        self.log = []
        self.user_log = []

    def log_event(self, event):
        id = event.id
        source = event.source
        source_id = event.source_id
        event_type = event.event_type
        target_runner_id = event.target_runner_id
        payload = event.payload
        exec_id = event.exec_id
        creation_time = event.creation_time
        log_entry = {
            "id": id,
            "source": source, 
            "source_id": source_id, 
            "event_type": event_type, 
            "target_runner_id": target_runner_id,
            "payload": payload, 
            "exec_id": exec_id,
            "creation_time": creation_time
        }

        self.log.append(log_entry) 
        return
