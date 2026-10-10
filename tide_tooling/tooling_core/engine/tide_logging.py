from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tide_tooling.tooling_core.engine.PipelineRunner import PipelineRunner

from tide_tooling.tooling_core.engine.events         import Effects
from tide_tooling.tooling_core.engine.classes        import DataRef, __TIDE_TEMPLATE_VALUE__
from tide_tooling.tooling_core.engine.utils          import (
    DedupedList,
    deduped_dict,
    _is_tide_exec_uuid,
    snapshot_json_dict,
)
from tide_tooling.tooling_core.engine.errors         import InvalidObjectKindError
from tide_tooling.tooling_core.engine.tide_records   import (
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
from tide_tooling.tooling_core.engine.tide_links     import (
    AffectedStackLink,
    EmittedStackLink,
    ReadWriteLink,
    StackLink,
)
from tide_tooling.tooling_core.engine.tide_index     import TraceIndex
from tide_tooling.tooling_core.engine.tide_query_results import (
    Compared,
    ConsumedBy,
    Downstream,
    Inspected,
    InspectedGroup,
    Investigate,
    Nearby,
    Object,
    Pathed,
    ProducedBy,
    Searched,
    Stream,
    TIDEModelCompared,
    Upstream,
    _build_compared,
    _build_object_compared,
)
from tide_tooling.tooling_core.engine.tide_model import (
    _build_tide_model_compared,
    _canonical_tide_structure,
    _is_tide_model_operand,
    _sorted_tide_items,
    _tide_incidence_entry,
    _tide_incidence_set,
    _tide_incidence_sort_key,
    _tide_model_document,
)
from tide_tooling.tooling_core.engine.tide_runtime_logging import (
    EventLogger,
    ExecutionLog,
    LogTools,
)

import copy, warnings
from .utils import tide_value_hasher

from collections                import defaultdict    
from typing                     import Any
from .tide_rendering import (
    SHOW_CYAN,
    SHOW_DIM,
    SHOW_GREEN,
    SHOW_RED,
    SHOW_RESET,
    SHOW_WHITE,
    SHOW_YELLOW,
    _show_options,
    _show_render,
    show_header,
    show_output,
    show_section,
    strip_ansi,
)

__all__ = [
    "Trace",
    "TraceIndex",
    "Actor",
    "AffectedRecord",
    "DependencyUseRecord",
    "EffectRecord",
    "EmittedNodeRecord",
    "ExecRecord",
    "ExpandedDepRecord",
    "InputUseRecord",
    "Inputs",
    "WriteRecord",
    "AffectedStackLink",
    "EmittedStackLink",
    "ReadWriteLink",
    "StackLink",
    "Compared",
    "ConsumedBy",
    "Downstream",
    "Inspected",
    "InspectedGroup",
    "Investigate",
    "Nearby",
    "Object",
    "Pathed",
    "ProducedBy",
    "Searched",
    "Stream",
    "TIDEModelCompared",
    "Upstream",
    "_build_compared",
    "_build_object_compared",
    "_build_tide_model_compared",
    "_canonical_tide_structure",
    "_is_tide_model_operand",
    "_sorted_tide_items",
    "_tide_incidence_entry",
    "_tide_incidence_set",
    "_tide_incidence_sort_key",
    "_tide_model_document",
    "EventLogger",
    "ExecutionLog",
    "LogTools",
    "strip_ansi",
]

__TEMPLATE__ =  __TIDE_TEMPLATE_VALUE__()
DEFAULT_TIDE_STORES = [
    "mem",
    "__tide__",
    "__unnamed__",
    "__model__",
    "global",
]
DEFAULT_STREAM_MAX_PATHS = 1000





























        
    


    




            

# class OfBase:
#     def to_dict(self):
#         return {
#             "id": self.source.id,
#             "execs": self.execs,
#             "paths": {exec_id: [path.to_dict() for path in paths] for exec_id, paths in self.paths_by_exec.items()}
#         }
    
#     def show(self):
#         if isinstance(self, ProducersOf):
#             relation = f"Producers of"
#         else:
#             relation = f"Consumers of"

#         lines = []
#         for source, paths in self.paths_by_exec.items():
#             print("")    
#             print(f"{relation} {self.inspect(source).object}:")
#             for path in paths:
#                 path: Pathed
#                 for exec_id, paths in path.paths.items():
#                     exec_repr = self.inspect(exec_id, "exec_record").object
#                     print(f"  via: {exec_repr}")
#                     for path in paths:
#                         for edge in path:
#                             if isinstance(edge, str): 
#                                 print(f"    {edge}")
#                             else:
#                                 edge: StackLink | AffectedStackLink
#                                 ref = edge.produced_ref
#                                 ver = edge.produced_ver
#                                 print(f"      {ref}[v{ver}]")
        
#         pass



# class ConsumersOf(OfBase):
#     def __init__(self, inspect, source, execs, paths_by_exec):
#         self.inspect = inspect
#         self.source = source
#         self.execs = execs
#         self.paths_by_exec = paths_by_exec

#     def to_dict(self):
#         return super().to_dict()

#     def show(self):
#         return super().show()

# class ProducersOf(OfBase):
#     def __init__(self, inspect, source, execs, paths_by_exec):
#         self.inspect = inspect
#         self.source = source
#         self.execs = execs
#         self.paths_by_exec = paths_by_exec

#     def to_dict(self):
#         return super().to_dict()

#     def show(self):
#         return super().show()
    
class Trace:
    def __init__(self, event_log=None, exec_log=None, runner=None, ocean_id=None, name:str="TIDE: .trace()", all_known_runners: dict=None, all_known_pipelines: dict=None, from_cas=False, bundle_context=None, source_context=None, model_context=None, host=None, host_record=None):
        self.name = name
        self.events = event_log or []
            # raise ValueError("No events!")
        
        if ocean_id is None:
            raise ValueError("Trace requires a source ocean_id")
        
        self.ocean_id = ocean_id
        self.from_cas = bool(from_cas)
        self.bundle_context = (
            dict(bundle_context)
            if isinstance(bundle_context, dict)
            else {}
        )
        self.model_context = (
            copy.deepcopy(model_context)
            if isinstance(model_context, dict)
            else {}
        )
        self.host = copy.deepcopy(
            host
            if host is not None
            else self.model_context.get("host")
        )
        self.host_record = (
            snapshot_json_dict(host_record, location="host_record")
            if host_record is not None
            else None
        )

        # if event_log:
        #     if isinstance(event_log[0], ExecEvent | ExecRecord):
        #         # for event in event_log:
        #         #     for log in event_log:
        #         #         for ref, read in log.reads.items():
        #         #             self.reads.append(f"@{ref}:{read}")
        #         # for log in event_log: 
        #         #     for ref, write in log.writes.items():
        #         #         self.writes.append()
        #         # self.events = tuple(event_log)
        #         # self.reads = tuple(f"@{ref}:{read}" for log in event_log for ref, read in log.reads.items()) 
        #         # self.writes = tuple(f"@{ref}:{write}" for log in event_log for ref, write in log.writes.items()) # reF:val for write in writes
        #         # self.values = tuple(value for log in event_log for value in list(log.reads.values()) + list(log.writes.values())) # val for val in [writes] + [values.items()]  

        #         self.events = tuple()
        #         self.reads = tuple()      
        #         self.writes = tuple()
        #         self.values = tuple()
        #     else:
        #         node_ready_events = tuple(entry for entry in event_log if entry["event_type"] in [ExecutorEvents.NODE_READY])
        #         node_write_events = tuple(entry for entry in event_log if entry["event_type"] in [RouterEvents.STORE_WRITE, RouterEvents.FORCE_WRITE])

        #         self.events = tuple(event_log)
        #         self.releases = tuple(event["payload"][0] for event in node_ready_events) 
        #         self.writes = tuple(f"@{event['payload'][0]}:{event['payload'][1]}" for event in node_write_events) # reF:val for write in writes
        #         self.values = tuple(event["payload"][2] for event in node_write_events) # val for val in [writes] + [values.items()]        
        self.all_known_pipelines = all_known_pipelines if all_known_pipelines is not None else {}
        self.all_known_runners = all_known_runners if all_known_runners is not None else {}
        self.runner = runner
        self.run = None
        self.pipeline = "TIDE: UNKNOWN"
        if hasattr(runner, "pipeline") and hasattr(runner, "id") and hasattr(runner, "index"):
            self.run = runner.id
            self.runner = runner
            self.pipeline = runner.pipeline.name
        elif isinstance(runner, (int, str)):
            self.run = runner 
            self.runner = runner
            self.pipeline = "TIDE: UNKNOWN"
        elif runner is not None:
            raise ValueError(f"TraceInit: Runner was an invalid type {runner}")
        
        

        self.exec_log = tuple(sorted(exec_log, key=lambda val: val.seq_id))

        self._source_pipeline_names = {
            (source.get("run"), source.get("pipeline_id")): source.get("pipeline")
            for source in source_context or ()
            if isinstance(source, dict) and source.get("pipeline") is not None
        }

        self.pipeline_index = {}
        for exec_record in self.exec_log:
            pipeline_id = getattr(exec_record, "pipeline_id", None)
            if pipeline_id is None:
                continue

            pipeline_id, pipeline_name = (
                self._get_pipeline_named_tuple_from_id(
                    pipeline_id,
                    getattr(exec_record, "runner_id", None),
                )
            )
            self.pipeline_index[pipeline_id] = pipeline_name

        self.execution_sources = self._build_execution_sources(source_context)
        self.source_runs = tuple(dict.fromkeys(
            source["run"] for source in self.execution_sources
        ))
        self.source_pipelines = tuple(dict.fromkeys(
            source["pipeline"] for source in self.execution_sources
        ))

        if len(self.source_runs) == 1:
            self.run = self.source_runs[0]
        elif len(self.source_runs) > 1:
            self.run = f"MULTIPLE ({len(self.source_runs)})"

        if len(self.source_pipelines) == 1:
            self.pipeline = self.source_pipelines[0]
        elif len(self.source_pipelines) > 1:
            self.pipeline = f"MULTIPLE ({len(self.source_pipelines)})"
        

        self.reads = []
        self.writes = []
        self.values = DedupedList()

        for exec in self.exec_log:

            for read in exec.deps or []:
                if read.type == "dynamic":
                    for expanded_read in read.expanded_to or []:
                        self.reads.append(expanded_read)
                        self.values.append(expanded_read.resolved_value)
                else:
                    self.reads.append(read)
                    self.values.append(read.resolved_value)

            for write in exec.writes or []:
                self.writes.append(write)
                self.values.append(write.stored_result)


        self.reads = tuple(self.reads)
        self.writes = tuple(self.writes)
        self.values = tuple(self.values)
        
        index = TraceIndex(ocean_id)
        self.index = index.build(self.exec_log)

        # Intent is compiled only at the completed Trace boundary.
        from tide_tooling.intent import build_trace_intent

        self.intent = build_trace_intent(self, runner)

    def to_dict(self):
        from tide_tooling.tooling_core.bundling.TIDERunSerialiser import TIDERunSerialiser

        return {
            "type": "trace",
            "name": self.name,
            "ocean_id": self.ocean_id,
            "run": getattr(self, "run", None),
            "pipeline": getattr(self, "pipeline", None),
            "sources": [dict(source) for source in self.execution_sources],
            "from_cas": self.from_cas,
            "bundle_context": copy.deepcopy(self.bundle_context),
            "model_context": copy.deepcopy(self.model_context),
            "host": copy.deepcopy(self.host),
            "host_record": copy.deepcopy(self.host_record),
            "value_hashes": sorted(self.index.value_by_hash),
            "references": sorted(self.index.seen_refs),
            "ref_versions": [
                {
                    "ref": ref,
                    "version": version,
                    "value_hash": value_hash,
                }
                for (ref, version), value_hash in sorted(
                    self.index.value_hash_by_ref_version.items(),
                    key=repr,
                )
            ],
            "run_evidence": TIDERunSerialiser().serialise(self),
        }

    def _build_execution_sources(self, source_context=None):
        supplied_names = {}
        for source in source_context or ():
            if not isinstance(source, dict):
                continue
            supplied_names[(
                source.get("run"),
                source.get("pipeline_id"),
            )] = source.get("pipeline")

        sources = []
        seen = set()

        for record in self.exec_log:
            run_id = getattr(record, "runner_id", None)
            pipeline_id = getattr(record, "pipeline_id", None)
            source_key = (run_id, pipeline_id)

            if source_key in seen:
                continue

            pipeline_name = supplied_names.get(source_key)
            if pipeline_name is None:
                _, pipeline_name = self._get_pipeline_named_tuple_from_id(
                    pipeline_id
                )

            # Individual .event() records carry actor-specific pipeline IDs,
            # but together they belong to the one public event source.
            if (
                run_id == "TIDE: .event()"
                and isinstance(pipeline_name, str)
                and pipeline_name.startswith("TIDE: .event(")
            ):
                pipeline_name = "TIDE: .event()"

            source = {
                "run": run_id,
                "pipeline_id": pipeline_id,
                "pipeline": pipeline_name,
            }
            sources.append(source)
            seen.add(source_key)

        # Event records may have distinct internal IDs but represent one
        # public run/pipeline source. Collapse identical display identities.
        collapsed = []
        display_seen = set()
        for source in sources:
            display_key = (source["run"], source["pipeline"])
            if display_key in display_seen:
                continue
            collapsed.append(source)
            display_seen.add(display_key)

        return tuple(collapsed)

    def _resolve_diff_operand(self, operand, kind=None, version=None):
        """Resolve a typed trace identifier to the object that should be diffed."""
        detected_kind = self._infer_kind(operand)
        resolved_kind = kind or detected_kind

        # Preserve the original two-value API for ordinary Python values.
        # Only unambiguous trace identifiers are inferred automatically.
        should_resolve = kind is not None or detected_kind in {
            "reference",
            "hash",
            "effect",
        }
        if not should_resolve:
            if version is not None:
                raise ValueError("A diff version requires a typed reference operand")
            return operand

        if not self._is_valid_obj_kind(resolved_kind):
            raise InvalidObjectKindError(
                f"{operand} | type={resolved_kind} is not known or does not "
                "exist in TIDE!",
                resolved_kind,
            )

        if resolved_kind in {"reference", "ref", "dep"}:
            if not isinstance(operand, str):
                raise TypeError("A reference diff operand must be a string")

            found = self.find(operand, version).first("reference")
            if found is None:
                raise KeyError(f"Unknown TIDE reference: {operand}")
            return self.value_of(found.id, version=version)

        if version is not None and resolved_kind != "effect":
            raise ValueError(
                "Only reference and effect diff operands support versions"
            )

        if resolved_kind == "hash":
            return self.value_of(operand)
        if resolved_kind == "value":
            return operand

        if resolved_kind == "actor" and isinstance(operand, str):
            inspected = self.find(operand).first("actor")
        else:
            inspected = self.inspect(
                operand,
                resolved_kind,
                version=version,
            )

        if inspected is None or not inspected.exists:
            raise KeyError(f"Unknown TIDE {resolved_kind}: {operand}")
        if resolved_kind == "effect":
            evidence = self.investigate(inspected).to_dict()
            return {
                "kind": evidence["kind"],
                "summary": evidence["summary"],
                "history": evidence["history"],
            }
        if isinstance(inspected, InspectedGroup):
            return [item.object for item in inspected.object]
        return inspected.object.object

    def _tide_model_comparison_options(self):
        """Return conversion options required by this Trace's model origin.

        Public ``to_model()`` remains strict by default.  A Trace imported
        through ``assign_unknown=True`` records that representational choice
        in ``model_context`` so internal model comparisons can reconstruct the
        same semantically valid comparison document without asking the caller to
        repeat an already-made import decision.
        """
        model_context = (
            self.model_context
            if isinstance(self.model_context, dict)
            else {}
        )
        return {
            "assign_unknown": bool(
                model_context.get("assigned_unknown_states")
            ),
        }

    def diff(
        self,
        source,
        target=__TIDE_TEMPLATE_VALUE__,
        source_type=None,
        target_type=None,
        source_ver=None,
        target_ver=None,
        TIDE=True,
        identity_mode="host",
    ):
        """Compare traces, direct values, or typed identifiers from this trace.

        ``trace.diff(other_trace)`` compares literal host-ID TIDE Models and
        reports exact kernel and judgment-set changes. Set
        ``identity_mode="structural"`` for deterministic occurrence aliases
        when comparing equivalent projections with different host IDs. Pass
        ``TIDE=False`` to retain the full trace-object/evidence diff. Two
        operands keep the existing direct-value behaviour, while optional
        path-style type and version arguments resolve trace references, hashes,
        actors, executions, or effects before comparison.
        """
        if not isinstance(TIDE, bool):
            raise TypeError("Trace.diff TIDE must be a bool")
        if identity_mode == "canonical":
            identity_mode = "structural"
        if not isinstance(identity_mode, str):
            raise TypeError("Trace.diff identity_mode must be a string")
        if identity_mode not in {"host", "structural"}:
            raise ValueError(
                "Trace.diff identity_mode must be 'host' or 'structural'"
            )
        if not TIDE and identity_mode != "host":
            raise ValueError(
                "Trace.diff structural identity requires TIDE=True"
            )

        target_omitted = (
            isinstance(target, type)
            and issubclass(target, __TIDE_TEMPLATE_VALUE__)
        ) or isinstance(target, __TIDE_TEMPLATE_VALUE__)
        if target_omitted:
            if any(
                option is not None
                for option in (
                    source_type,
                    target_type,
                    source_ver,
                    target_ver,
                )
            ):
                raise TypeError(
                    "Typed Trace.diff needs both a source and a target"
                )
            if TIDE and _is_tide_model_operand(source):
                return _build_tide_model_compared(
                    self,
                    source,
                    identity_mode=identity_mode,
                )
            return _build_compared(self, source)

        resolved_source = self._resolve_diff_operand(
            source,
            source_type,
            source_ver,
        )
        resolved_target = self._resolve_diff_operand(
            target,
            target_type,
            target_ver,
        )
        return _build_compared(resolved_source, resolved_target)

    def comparator(self, target, *, execution_map, state_map):
        """Compare this Trace under explicit Execution and State maps."""
        from tide_tooling.comparator import comparator

        return comparator(
            self,
            target,
            execution_map=execution_map,
            state_map=state_map,
        )

    def to_model(
        self,
        *,
        host=None,
        include_metadata=False,
        include_values=False,
        include_hashes=True,
        incidence_format="explicit",
        identity_mode="host",
        assign_unknown=False,
    ):
        """Export the interoperable ``K = (E, S, I, O)`` projection.

        TIDE references and versions remain an internal Trace detail. Each
        exact internal State is assigned one opaque ``state_id``; state IDs
        imported by :meth:`TIDE.from_model` are preserved. Incidence can be
        emitted as explicit dictionaries, compact ``{e, s}`` dictionaries,
        or JSON-compatible ordered pairs. ``identity_mode="structural"`` is
        an explicit comparison view that canonically relabels the typed
        incidence structure and excludes values and enrichment; the default
        ``"host"`` export stays literal. Export is strict about represented
        producers by default. ``assign_unknown=True`` warns and maps TIDE's
        internal unknown observations to one tool-side ``unknown`` Execution.
        """
        for option_name, option in {
            "include_metadata": include_metadata,
            "include_values": include_values,
            "include_hashes": include_hashes,
            "assign_unknown": assign_unknown,
        }.items():
            if not isinstance(option, bool):
                raise TypeError(f"{option_name} must be a bool")

        if host is not None and (
            not isinstance(host, str) or not host.strip()
        ):
            raise ValueError("host must be a non-empty string")

        if not isinstance(incidence_format, str):
            raise TypeError("incidence_format must be a string")
        if incidence_format not in {"explicit", "compact", "pairs"}:
            raise ValueError(
                "incidence_format must be 'explicit', 'compact', or 'pairs'"
            )

        if identity_mode == "canonical":
            identity_mode = "structural"
        if not isinstance(identity_mode, str):
            raise TypeError("identity_mode must be a string")
        if identity_mode not in {"host", "structural"}:
            raise ValueError(
                "identity_mode must be 'host' or 'structural'"
            )

        model_context = (
            self.model_context
            if isinstance(self.model_context, dict)
            else {}
        )
        is_model_projection = (
            model_context.get("schema") == "TIDE-Model"
        )
        selected_host = host
        if selected_host is None:
            selected_host = model_context.get("host") or "TIDE"

        def reference_string(reference):
            if isinstance(reference, DataRef):
                return DataRef.to_string(reference, strip_leading=True)
            if isinstance(reference, str):
                return reference
            raise TypeError(
                f"Trace contains an unsupported State reference: {reference!r}"
            )

        records = list(self.exec_log)

        def is_unknown_producer_record(record):
            node_options = (
                record.node_options
                if isinstance(record.node_options, dict)
                else {}
            )
            if node_options.get("source") == "tide_model_boundary":
                return True
            if node_options.get("assigned_unknown") is True:
                return True

            actor = getattr(record, "producer_id", None)
            actor_name = getattr(actor, "source_actor", None)
            actor_source = getattr(actor, "source_runner", None)
            causes = {
                getattr(write, "cause", None)
                for write in record.writes or ()
            }
            if actor_name == "unknown" and actor_source == "unknown":
                return True
            return (
                actor_name in {"TIDE", "TIDE: Polling"}
                and bool(causes)
                and causes <= {
                    "TIDE: observed value",
                    "TIDE: user declared value",
                    "TIDE: Pre-runtime Polling",
                    "TIDE: Emitted Node Polling",
                }
            )

        unknown_records = {
            id(record)
            for record in records
            if is_unknown_producer_record(record)
        }
        model_records = [
            record
            for record in records
            if assign_unknown or id(record) not in unknown_records
        ]
        model_exec_id_by_trace_id = {}
        execution_enrichments = {}
        context_exec_enrichments = {
            entry["exec_id"]: copy.deepcopy(entry.get("metadata", {}))
            for entry in model_context.get("execution_enrichment", ())
            if isinstance(entry, dict) and "exec_id" in entry
        }
        for record in model_records:
            node_options = (
                record.node_options
                if isinstance(record.node_options, dict)
                else {}
            )
            unknown_producer = id(record) in unknown_records
            host_exec_id = (
                "unknown"
                if unknown_producer
                else node_options.get(
                    "model_exec_id",
                    record.exec_id,
                )
            )
            exec_id = host_exec_id
            model_exec_id_by_trace_id[record.exec_id] = exec_id

            if include_metadata and not unknown_producer:
                if host_exec_id in context_exec_enrichments:
                    enrichment = context_exec_enrichments[host_exec_id]
                elif is_model_projection:
                    # Generated Trace actors and internal IDs make an
                    # imported model inspectable, but they are not evidence
                    # supplied by that model and must not leak back out as
                    # enrichment.
                    enrichment = {}
                else:
                    enrichment = {}
                    actor = getattr(record, "producer_id", None)
                    if actor is not None:
                        enrichment["actor"] = {
                            "name": actor.source_actor,
                            "source": actor.source_runner,
                            "ocean": actor.source_ocean,
                        }
                    for output_name, attribute_name in (
                        ("start", "started_at"),
                        ("end", "finished_at"),
                        ("time", "duration_ms"),
                        ("status", "status"),
                        ("error", "error"),
                        ("pipeline_id", "pipeline_id"),
                        ("node_id", "node_id"),
                    ):
                        value = getattr(record, attribute_name, None)
                        if value is not None:
                            enrichment[output_name] = (
                                repr(value)
                                if output_name == "error"
                                else value
                            )
                if enrichment:
                    execution_enrichments[exec_id] = enrichment

        state_id_by_internal = {}
        state_order = list(
            model_context.get(
                "state_ids",
                model_context.get("S", ()),
            )
        )
        context_state_by_id = {
            entry["state_id"]: {
                key: value
                for key, value in entry.items()
                if key != "state_id"
            }
            for entry in model_context.get("state_enrichment", ())
            if isinstance(entry, dict) and "state_id" in entry
        }

        context_state_by_projected_id = {}
        for projection in model_context.get("state_projection", ()):
            if not isinstance(projection, dict):
                continue
            if not {
                "state_id",
                "reference",
                "version",
            }.issubset(projection):
                continue
            internal_id = (
                projection["reference"],
                projection["version"],
            )
            host_state_id = projection["state_id"]
            state_id = host_state_id
            state_id_by_internal[internal_id] = state_id
            context_state_by_projected_id[state_id] = (
                context_state_by_id.get(host_state_id, {})
            )

        value_by_internal = {}

        def state_id_for(reference, version, value):
            reference = reference_string(reference)
            internal_id = (reference, version)
            if internal_id not in state_id_by_internal:
                state_id = f"state_{len(state_order)}"
                state_id_by_internal[internal_id] = state_id
                state_order.append(state_id)
            if value is not None and not isinstance(
                value,
                __TIDE_TEMPLATE_VALUE__,
            ):
                value_by_internal.setdefault(internal_id, value)
            return state_id_by_internal[internal_id], internal_id

        seen_inputs = set()
        seen_outputs = set()
        tool_unknown_states = []

        for record in records:
            if id(record) in unknown_records and not assign_unknown:
                for write in record.writes or ():
                    state_id, _ = state_id_for(
                        write.stored_target,
                        write.stored_version,
                        write.stored_result,
                    )
                    tool_unknown_states.append(state_id)
                continue

            exec_id = model_exec_id_by_trace_id[record.exec_id]
            for dependency in record.deps or ():
                if getattr(dependency, "type", None) == "dynamic":
                    resolved_dependencies = dependency.expanded_to or ()
                else:
                    resolved_dependencies = (dependency,)
                for resolved in resolved_dependencies:
                    state_id, _ = state_id_for(
                        resolved.unscoped_reference,
                        resolved.resolved_version,
                        resolved.resolved_value,
                    )
                    edge = (exec_id, state_id)
                    seen_inputs.add(edge)

            for write in record.writes or ():
                state_id, _ = state_id_for(
                    write.stored_target,
                    write.stored_version,
                    write.stored_result,
                )
                if id(record) in unknown_records:
                    tool_unknown_states.append(state_id)
                edge = (exec_id, state_id)
                seen_outputs.add(edge)

        inputs = [
            _tide_incidence_entry(exec_id, state_id, incidence_format)
            for exec_id, state_id in sorted(
                seen_inputs,
                key=_tide_incidence_sort_key,
            )
        ]
        outputs = [
            _tide_incidence_entry(exec_id, state_id, incidence_format)
            for exec_id, state_id in sorted(
                seen_outputs,
                key=_tide_incidence_sort_key,
            )
        ]

        state_enrichments = {}
        internal_by_state_id = {
            state_id: internal_id
            for internal_id, state_id in state_id_by_internal.items()
        }
        for state_id in state_order:
            enrichment = {}
            context_state = context_state_by_projected_id.get(
                state_id,
                context_state_by_id.get(state_id, {}),
            )
            internal_id = internal_by_state_id.get(state_id)
            if (
                include_metadata
                and internal_id is not None
                and not is_model_projection
            ):
                enrichment["reference"] = internal_id[0]
                enrichment["version"] = internal_id[1]
            value = (
                value_by_internal.get(internal_id)
                if internal_id is not None
                else None
            )

            if include_hashes:
                value_hash = context_state.get("value_hash")
                if (
                    value is not None
                    and not is_model_projection
                    and not context_state
                ):
                    value_hash = tide_value_hasher(value)
                if value_hash is not None:
                    enrichment["value_hash"] = value_hash

            if include_values and value is not None:
                from tide_tooling.tooling_core.bundling.TIDEOpaqueValue import TIDEOpaqueValue

                if not isinstance(value, TIDEOpaqueValue):
                    enrichment["value"] = copy.deepcopy(value)
            if enrichment:
                state_enrichments[state_id] = enrichment

        execution_ids = sorted(
            {
                model_exec_id_by_trace_id[record.exec_id]
                for record in model_records
            },
            key=lambda value: (type(value).__name__, repr(value)),
        )
        state_ids = list(dict.fromkeys(state_order))
        represented_exec_ids = {
            (type(exec_id), exec_id)
            for exec_id in execution_ids
        }
        represented_state_ids = {
            (type(state_id), state_id)
            for state_id in state_ids
        }
        execution_enrichments = {
            exec_id: enrichment
            for exec_id, enrichment in execution_enrichments.items()
            if (type(exec_id), exec_id) in represented_exec_ids
        }
        state_enrichments = {
            state_id: enrichment
            for state_id, enrichment in state_enrichments.items()
            if (type(state_id), state_id) in represented_state_ids
        }

        model = {
            "schema": "TIDE-Model",
            "schema_version": 1,
            "host": selected_host,
            "E": execution_ids,
            "S": state_ids,
            "I": inputs,
            "O": outputs,
        }
        if execution_enrichments:
            model["executions"] = execution_enrichments
        if state_enrichments:
            model["states"] = state_enrichments
        host_record = getattr(self, "host_record", None)
        if host_record is not None:
            model["host_record"] = copy.deepcopy(host_record)

        from tide_tooling.core import TIDE

        report = TIDE.validate_model(model)
        if assign_unknown:
            output_state_keys = set()
            for edge in outputs:
                if isinstance(edge, dict):
                    state_id = edge.get("state_id", edge.get("s"))
                else:
                    state_id = edge[1]
                output_state_keys.add((type(state_id), state_id))
            missing_states = [
                state_id
                for state_id in {
                    edge[1]
                    for edge in _tide_incidence_set(inputs)
                    | _tide_incidence_set(outputs)
                }
                if (type(state_id), state_id) not in output_state_keys
            ]
            if missing_states:
                if "unknown" not in model["E"]:
                    model["E"].append("unknown")
                    model["E"].sort(
                        key=lambda value: (type(value).__name__, repr(value))
                    )
                for state_id in sorted(
                    missing_states,
                    key=lambda value: (type(value).__name__, repr(value)),
                ):
                    outputs.append(
                        _tide_incidence_entry(
                            "unknown",
                            state_id,
                            incidence_format,
                        )
                    )
                tool_unknown_states.extend(missing_states)
                outputs = [
                    _tide_incidence_entry(
                        exec_id,
                        state_id,
                        incidence_format,
                    )
                    for exec_id, state_id in sorted(
                        _tide_incidence_set(outputs),
                        key=_tide_incidence_sort_key,
                    )
                ]
                model["O"] = outputs
                report = TIDE.validate_model(model)

            if tool_unknown_states:
                seen_warning_states = []
                seen_warning_keys = set()
                for state_id in tool_unknown_states:
                    key = (type(state_id), state_id)
                    if key not in seen_warning_keys:
                        seen_warning_keys.add(key)
                        seen_warning_states.append(state_id)
                warnings.warn(
                    "Trace States have no host-identified producer: "
                    + ", ".join(repr(state) for state in seen_warning_states)
                    + ". assign_unknown=True emitted the tool-side "
                    "Execution 'unknown'; this does not establish host "
                    "provenance.",
                    RuntimeWarning,
                    stacklevel=2,
                )
        if not report["semantically_valid"]:
            suggestion = (
                "; pass assign_unknown=True only if a tool-side unknown "
                "producer is acceptable"
                if any(
                    "has no represented producer" in error
                    for error in report["errors"]
                )
                else ""
            )
            raise ValueError(
                "Trace cannot be exported as a semantically valid TIDE Model: "
                + "; ".join(report["errors"])
                + suggestion
            )

        if identity_mode == "structural":
            model = _canonical_tide_structure(
                model,
                incidence_format=incidence_format,
            )
            report = TIDE.validate_model(model)
            if not report["semantically_valid"]:
                raise ValueError(
                    "Trace structural export is not a semantically valid TIDE Model: "
                    + "; ".join(report["errors"])
                )
        return model

    def to_bundle(
        self,
        handle=None,
        unsupported="opaque",
        type=None,
        kind=None,
        path=None,
    ):
        """Create, and optionally export, a portable trace bundle.

        With no export options this returns a ``TIDEBundle`` as before.
        Supplying ``type``, ``kind`` or ``path`` forwards the completed bundle
        through ``TIDEBundle.export`` and returns that export result.
        """
        from tide_tooling.tooling_core.bundling.TIDEBundle import TIDEBundle
        from tide_tooling.tooling_core.bundling.TIDEBundler import TIDEBundler

        bundler = TIDEBundler(unsupported=unsupported)
        bundle = TIDEBundle.from_trace(
            self,
            handle=handle,
            bundler=bundler,
        )

        if type is None and kind is None and path is None:
            return bundle

        export_type = type or ("file" if path is not None else "dict")
        export_kind = kind or (
            "json" if export_type == "file" else "python"
        )
        return bundle.export(
            type=export_type,
            kind=export_kind,
            path=path,
        )

    @staticmethod
    def _is_val_hash(hash):
        return isinstance(hash, str) and hash.startswith("sha3_512_tide:")
    @staticmethod
    def _is_valid_exec_uuid(uuid):
        return _is_tide_exec_uuid(uuid)

    def _pattern_match_references(self, reference_candidate):
        refs = DedupedList()
        
        

        if ":" not in reference_candidate:
            seen_refs = DedupedList()
            seen_stores = DedupedList()
            for ref in self.index.seen_refs:
                store, key = ref.split(":", 1)
                seen_refs.append(key)
                seen_stores.append(store)
            
            seen_refs = [ref.split(":", 1)[1] for ref in self.index.seen_refs]

            for idx, ref in enumerate(seen_refs):
                if self._pattern_match(ref, reference_candidate):
                    found_ref = self.index.seen_refs[idx]
                    refs.add(found_ref)
        else:
            seen_refs = self.index.seen_refs
        
            for ref in seen_refs:
                if self._pattern_match(ref, reference_candidate):
                    refs.add(ref)
        return refs

    def _pattern_match_func_names(self, func_name_candidate):
        names = DedupedList()
        for name in self.index.func_name_to_actors.keys():
            if self._pattern_match(name, func_name_candidate):
                names.add(name)
        return names
    
    def _pattern_match(self, ref, pattern):
        import fnmatch
        return fnmatch.fnmatch(ref, f"*{pattern}*")

    def find(self, value: Any, version=None) -> Searched:
        # take an object
        # find what it is. 
        # find candidates
        # inspect candidates,
        # fill Searched object for each matching candidate
        # fill SearchResult with SearchResult.found = [Searched, ]
        
        
        handled_refs = set()
        handled_actors = set()
        ref_matches  = list()
        actor_matches= list()
        exec_matches = list()
        value_matches= list()
        effect_matches=list()

        close_ref_matches= list()
        close_actor_matches =list()

        # VALUE MATCH
        # inspected = self.inspect_value(value, "value")
        # if inspected.history and inspected.object.id not in value_matches:
        #     value_matches.append(inspected)

        # EXEC UUID MATCH | VALUE HASH MATCH | REFERENCE MATCH | ACTOR MATCH
        if isinstance(value, str):
            # EXEC UUID MATCH
            if self._is_valid_exec_uuid(value):
                if self.index.exec_by_exec_id.get(value, []):
                    exec_matches.append(self.inspect(value, "exec_record"))
            
            # VALUE HASH MATCH
            if self._is_val_hash(value) and value not in value_matches:
                value_matches.append(self.inspect(value, "hash"))

            # REFERENCE MATCH
            if value in self.index.seen_refs and value not in ref_matches:
                if value not in handled_refs:    
                    inspected_ref = self.inspect(value, "reference")
                    if version is None:
                        handled_refs.add(value)
                        ref_matches.append(inspected_ref)
                    else: 
                        if version in inspected_ref.history:
                            inspected_ref = self.inspect(value, "reference", version)
                            handled_refs.add(value)
                            ref_matches.append(inspected_ref)

                    
            
            
            # PATTERN MATCH REFERENCE RELATIVES
            if not isinstance(self.runner, (int, str)):
                stores = self.runner.store_router.registry.__seen_stores__
            else:
                stores = ["global", "__tide__", "__unnamed__"]
            for store in stores:
                candidate = f"{store}:{value}"
                if candidate in self.index.seen_refs:
                    if candidate not in handled_refs:
                        
                        inspected_ref = self.inspect(candidate, "reference")
                        
                        if version is not None: 
                            if version not in inspected_ref.history:
                                pass
                            else:
                                inspected_ref = self.inspect(candidate, "reference", version)
                                                
                        handled_refs.add(candidate)
                        close_ref_matches.append(inspected_ref)

            for ref in self._pattern_match_references(value):
                if ref not in handled_refs:
                    inspected_ref = self.inspect(ref, "reference")
                    if version is not None: 
                        if version not in inspected_ref.history:
                            pass
                        else:
                            inspected_ref = self.inspect(ref, "reference", version)

                    handled_refs.add(ref)
                    close_ref_matches.append(inspected_ref)

            # ACTOR NAME MATCHES ARE NEARBY, NOT EXACT
            if value in self.index.func_name_to_actors:
                for actor in self.index.func_name_to_actors.get(value, []):
                    if actor in handled_actors:
                        continue

                    handled_actors.add(actor)   
                    close_actor_matches.append(
                        self.inspect(actor, "actor")
                    )

            # PATTERN MATCH ACTOR RELATIVES
            for name in self._pattern_match_func_names(value):
                for actor in self.index.func_name_to_actors.get(name, []):
                    if actor in handled_actors:
                        continue

                    handled_actors.add(actor)
                    close_actor_matches.append(
                        self.inspect(actor, "actor")
                    )
                

        # ACTOR MATCH
        if isinstance(value, tuple) and len(value) == 3 or isinstance(value, Actor): 
            if not isinstance(value, Actor):
                actor = Actor(*value)
            else:
                actor = value
            if actor in self.index.actor_to_exec_ids and actor not in actor_matches:
                actor_matches.append(self.inspect(actor, "actor"))

        # EXEC SEQ_ID MATCH                
        if isinstance(value, int):

            if value < len(self.exec_log):
                match = self.exec_log[value]
                if match.exec_id not in exec_matches:
                    exec_matches.append(self.inspect(match.exec_id, "exec_record"))
        
        # EFFECT MATCHES
        if isinstance(value, Effects):
            inspected_effect = self.inspect(value, "effect")
            if inspected_effect.exists:
                effect_matches.append(inspected_effect)
        elif isinstance(value, tuple) and len(value) == 2:
            inspected_effect = self.inspect(value, "effect", version)
            if inspected_effect.exists:
                effect_matches.append(inspected_effect)

        nearby = Nearby(value, close_actor_matches, close_ref_matches)
        searched =Searched(self.inspect, value, exec_matches, actor_matches, ref_matches, effect_matches, value_matches, nearby) 
        return searched

    def value_of(self, reference_or_hash, version=None, show=False):
        if isinstance(reference_or_hash, Inspected):
            if reference_or_hash.type != "reference":
                raise TypeError(
                    "TIDE value lookup needs an inspected reference, "
                    "reference string or value hash"
                )

            inspected_version = reference_or_hash.version
            if (
                version is not None
                and inspected_version is not None
                and version != inspected_version
            ):
                raise ValueError(
                    "TIDE value lookup version conflicts with the inspected "
                    "reference version"
                )

            reference_or_hash = reference_or_hash.id
            if version is None:
                version = inspected_version

        if not isinstance(reference_or_hash, str):
            raise TypeError(
                "TIDE value lookup needs an inspected reference, "
                "reference string or value hash"
            )
        if not isinstance(show, bool):
            raise TypeError("TIDE value lookup show must be a bool")

        if self._is_val_hash(reference_or_hash):
            if version is not None:
                raise ValueError("A TIDE value hash does not use a version")

            try:
                value = self.index.value_by_hash[reference_or_hash]
            except KeyError as error:
                raise KeyError(
                    f"Unknown TIDE value hash: {reference_or_hash}"
                ) from error

            if show:
                self._show_value_of(
                    value,
                    value_hash=reference_or_hash,
                )
            return value

        if reference_or_hash in self.index.versions_by_ref:
            return self._value_of_reference(
                reference_or_hash,
                version=version,
                show=show,
            )

        searched = self.find(reference_or_hash)
        references = {
            inspected.id
            for inspected in (
                searched.get_exact("reference")
                + searched.get_nearby("reference")
            )
        }

        if not references:
            raise KeyError(f"Unknown TIDE reference: {reference_or_hash}")

        if version is not None:
            references = {
                reference
                for reference in references
                if version in self.index.versions_by_ref.get(reference, ())
            }
            if not references:
                raise KeyError(
                    f"Unknown TIDE reference version: "
                    f"{reference_or_hash}[v{version}]"
                )

        references = sorted(references, key=str)
        if len(references) == 1:
            return self._value_of_reference(
                references[0],
                version=version,
                show=show,
            )

        values = []
        for reference in references:
            found = self._value_of_reference(
                reference,
                version=version,
                show=show,
            )
            values.extend(found if version is None else [found])
        return values

    def _value_of_reference(self, reference, *, version=None, show=False):
        if version is not None:
            try:
                value_hash = self.index.value_hash_by_ref_version[
                    (reference, version)
                ]
                value = self.index.value_by_hash[value_hash]
            except KeyError as error:
                raise KeyError(
                    f"Unknown TIDE reference version: {reference}[v{version}]"
                ) from error

            if show:
                self._show_value_of(
                    value,
                    reference=reference,
                    version=version,
                    value_hash=value_hash,
                )
            return value

        versions = self.index.versions_by_ref.get(reference, [])
        if not versions:
            raise KeyError(f"Unknown TIDE reference: {reference}")

        versioned_values = [
            (
                value_version,
                self.index.value_hash_by_ref_version[
                    (reference, value_version)
                ],
                self.index.value_by_hash[
                    self.index.value_hash_by_ref_version[
                        (reference, value_version)
                    ]
                ],
            )
            for value_version in sorted(versions)
        ]
        values = [value for _, _, value in versioned_values]

        if show:
            self._show_value_of(
                values,
                reference=reference,
                versions=versioned_values,
            )
        return values

    def show_value_of(self, reference_or_hash, version=None):
        return self.value_of(reference_or_hash, version=version, show=True)

    def _show_value_of(
        self,
        value,
        reference=None,
        version=None,
        value_hash=None,
        versions=None,
    ):
        if versions is not None:
            summary = (
                f"{len(versions)} "
                f"{'value' if len(versions) == 1 else 'values'} found"
            )
            identifier = f"Reference: {reference}"
        elif reference is not None:
            summary = "1 value found"
            identifier = f"Reference: {reference}[v{version}]"
        else:
            summary = "1 value found"
            identifier = f"Value hash: {value_hash}"

        lines = show_header("VALUE", summary, identifier)

        if versions is not None:
            lines.extend(show_section("VERSIONS"))
            for value_version, version_hash, version_value in versions:
                lines.append(
                    f"  {SHOW_CYAN}[v{value_version}]{SHOW_RESET} "
                    f"{SHOW_DIM}{version_hash}{SHOW_RESET}"
                )
                lines.append(
                    f"      {SHOW_DIM}type:{SHOW_RESET} "
                    f"{SHOW_WHITE}{type(version_value).__name__}{SHOW_RESET}"
                )
                lines.append(
                    f"      {SHOW_DIM}value:{SHOW_RESET} "
                    f"{SHOW_WHITE}{repr(version_value)}{SHOW_RESET}"
                )
        else:
            lines.extend(show_section("VALUE"))
            lines.append(
                f"  {SHOW_DIM}hash:{SHOW_RESET} "
                f"{SHOW_CYAN}{value_hash}{SHOW_RESET}"
            )
            lines.append(
                f"  {SHOW_DIM}type:{SHOW_RESET} "
                f"{SHOW_WHITE}{type(value).__name__}{SHOW_RESET}"
            )
            lines.append(
                f"  {SHOW_DIM}value:{SHOW_RESET} "
                f"{SHOW_WHITE}{repr(value)}{SHOW_RESET}"
            )

        return show_output(lines)
    
    def show(
        self,
        color=True,
        simple=False,
        colour=None,
        structural=False,
    ):
        """Render this Trace, optionally through its canonical TIDE structure.

        ``structural=True`` uses the same canonicalisation as structural model
        export and diff, then renders that view with the ordinary Trace layout
        and colour rules. The default view retains the Trace's supplied
        identities and enrichment.
        """
        color, simple = _show_options(color, simple, colour)
        if not isinstance(structural, bool):
            raise TypeError("show structural must be a bool")
        if structural:
            from tide_tooling.core import TIDE

            model_options = self._tide_model_comparison_options()
            if model_options.get("assign_unknown") is True:
                with warnings.catch_warnings():
                    warnings.filterwarnings(
                        "ignore",
                        message=(
                            "Trace States have no host-identified producer:.*"
                        ),
                        category=RuntimeWarning,
                    )
                    structural_model = self.to_model(
                        include_hashes=False,
                        identity_mode="structural",
                        **model_options,
                    )
            else:
                structural_model = self.to_model(
                    include_hashes=False,
                    identity_mode="structural",
                )
            structural_trace = TIDE.from_model(
                structural_model,
                name=f"{self.name} [structural]",
            )
            structural_trace.ocean_id = "structural"
            return structural_trace.show(
                color=color,
                simple=simple,
            )
        # 
        # Tide Run 0 
        # Pipeline [name or id]
        # Executions
        # Writes
        # Status
        #

        def not_template(value):
            return True if not isinstance(value, __TIDE_TEMPLATE_VALUE__) else False

        def ref_colour(ref, ver):
            consumers = self.index.ref_version_to_consumer_exec_ids.get(
                (ref, ver),
                [],
            )
            return (
                SHOW_RED
                if (ref, ver) in self.index.deleted_ref_versions
                or not consumers
                else SHOW_GREEN
            )

        def effect_ref_colour(log, effect_entry):
            effect = effect_entry.effect
            ref, ver = effect_entry.targets

            if effect == Effects.DELETE:
                return SHOW_RED
            if effect == Effects.READ:
                return (
                    SHOW_GREEN
                    if effect_entry.target_existed is True
                    else SHOW_RED
                )
            return ref_colour(ref, ver)

        def lines_from_write(cause, result_name, ref, ver):
            link = "as"
            action = "wrote:"
            colour = ref_colour(ref, ver)
            lines = []
            if cause.startswith("TIDE:"):
                cause_str = cause.split(" ", 1)[1]

                if "TIDE: Emitted Node Polling" in cause:
                    action =  "found"
                    cause = "polled emitted node deps"
                
                match cause_str:
                    case "user declared value":
                        action = "user declared"
                        cause = ""
                        link = "as"
                        return [
                            f"    {SHOW_DIM}{action}{SHOW_RESET} "
                            f"{SHOW_WHITE}{result_name}{SHOW_RESET} "
                            f"{SHOW_DIM}{link}{SHOW_RESET} "
                            f"{colour}{ref}[v{ver}]{SHOW_RESET}"
                        ]
                        
                    case "observed value":
                        action = "updated"
                        cause = "value changed with no actor"
                        link = "as"
                        return [
                            f"    {SHOW_DIM}{action}{SHOW_RESET} "
                            f"{colour}{ref}[v{ver}]{SHOW_RESET} "
                            f"{SHOW_DIM}{link}{SHOW_RESET} "
                            f"{SHOW_WHITE}{result_name}{SHOW_RESET} "
                            f"{SHOW_DIM}because {cause}{SHOW_RESET}"
                        ]
                    case "Pre-runtime Polling":
                        action = "found"
                        cause = "polling"
                        link = "as"
            lines.append(
                f"    {SHOW_DIM}{action}{SHOW_RESET} "
                f"{SHOW_WHITE}{result_name}{SHOW_RESET} "
                f"{SHOW_DIM}{link}{SHOW_RESET} "
                f"{colour}{ref}[v{ver}]{SHOW_RESET}"
            )
            if cause and str(cause).lower() != "unknown":
                lines[-1] += (
                    f" {SHOW_DIM}because {cause}{SHOW_RESET}"
                )
            return lines
                    
        
        execution_count = len(self.exec_log)
        execution_word = "execution" if execution_count == 1 else "executions"
        read_count = len(self.reads)
        read_word = "read" if read_count == 1 else "reads"
        write_count = len(self.writes)
        write_word = "write" if write_count == 1 else "writes"
        summary = (
            f"{execution_count} {execution_word} found | "
            f"{write_count} {write_word} | "
            f"{read_count} {read_word}"
        )
        if self.host is not None:
            summary += f" | host: {self.host}"
        run_label = "Tide Run" if len(self.source_runs) <= 1 else "Tide Runs"
        pipeline_label = (
            "Pipeline" if len(self.source_pipelines) <= 1 else "Pipelines"
        )
        identifier = (
            f"Ocean: {self.ocean_id} | "
            f"{run_label}: {self.run} | "
            f"{pipeline_label}: {self.pipeline}"
        )
        lines = show_header("TRACE", summary, identifier)
        lines.extend(show_section("RUN"))
        lines.append(
            f"{SHOW_DIM}Sources:    "
            f"{len(self.execution_sources)}{SHOW_RESET}"
        )
        if len(self.execution_sources) > 1:
            for source_idx, source in enumerate(self.execution_sources):
                lines.append(
                    f"  {SHOW_CYAN}[{source_idx}]{SHOW_RESET} "
                    f"{SHOW_WHITE}{source['run']}{SHOW_RESET} "
                    f"{SHOW_DIM}| Pipeline: {source['pipeline']}{SHOW_RESET}"
                )
        lines.extend(show_section("EXECUTIONS"))

        for idx, log in enumerate(self.exec_log):
            producer_id_repr = log.producer_id.source_actor
            lines.append(f"{SHOW_CYAN}{idx}: {producer_id_repr}{SHOW_RESET}")

            # 
            # handle dependencies 
            deps = log.deps
            if not_template(deps) and deps:
                for dep_entry in deps:
                    dep_entry: DependencyUseRecord
                    ref = dep_entry.unscoped_reference
                    dep_type = dep_entry.type

                    if dep_type == "dynamic":
                        version = "[dynamic]"
                    else:
                        resolved_ver = dep_entry.resolved_version
                        version = f"[v{resolved_ver}]"
                        associated_input = getattr(
                            dep_entry,
                            "associated_input",
                            None,
                        )
                        is_effect_read = (
                            isinstance(associated_input, tuple)
                            and associated_input
                            and associated_input[0] == "runtime_read"
                        )

                        if is_effect_read:
                            lines.append(
                                f"     {SHOW_CYAN}read:{SHOW_RESET} "
                                f"{SHOW_GREEN}{ref}{version}{SHOW_RESET} "
                                f"{SHOW_DIM}via READ effect{SHOW_RESET}"
                            )
                        else:
                            lines.append(
                                f"     {SHOW_DIM}used:{SHOW_RESET} "
                                f"{SHOW_GREEN}{ref}{version}{SHOW_RESET}"
                            )

                        if dep_type == "dynamic":
                            for expanded_entry in dep_entry.expanded_to:
                                expanded_ref = expanded_entry.unscoped_reference
                                expanded_ver = expanded_entry.resolved_version
                                lines.append(
                                    f"        {SHOW_DIM}->{SHOW_RESET} "
                                    f"{SHOW_GREEN}{expanded_ref}[v{expanded_ver}]{SHOW_RESET}"
                                )

            
            # 
            # handle stored values
            error = log.error
            if not_template(error) and error:
                if getattr(log, "handled_raise", False):
                    lines.append(
                        f"      {SHOW_YELLOW}RAISE handled: "
                        f"{error}{SHOW_RESET}"
                    )
                else:
                    lines.append(
                        f"      {SHOW_RED}ERROR: {error}{SHOW_RESET}"
                    )

            for write in log.writes:
                stored_result = write.stored_result
                stored_version = write.stored_version
                if not_template(stored_result):
                    target_ref = write.stored_target
                    result_name = self._show_value_name(stored_result)
                    _lines_from_write = lines_from_write(write.cause, result_name, target_ref, stored_version) 
                    # action = "wrote" if not (cause.startswith("TIDE") and "Polling" in cause) else "dep available"
                    
                    lines.extend(_lines_from_write)
            

            #
            # handle effects
            effects = log.effects
            if not_template(effects) and effects:
                if effects: lines.append(f"  {SHOW_DIM}effects:{SHOW_RESET}")
                for effect_entry in effects:
                    effect = effect_entry.effect
                    targets = effect_entry.targets
                    colour = effect_ref_colour(log, effect_entry)
                    lines.append(
                        f"        {SHOW_CYAN}{effect}{SHOW_RESET} "
                        f"{SHOW_DIM}->{SHOW_RESET} "
                        f"{colour}{targets[0]}[v{targets[1]}]{SHOW_RESET}"
                    )

            
            #
            #  handle emitted nodes            
            logged_emitted_nodes = log.emitted_nodes
            if not_template(logged_emitted_nodes) and logged_emitted_nodes:
                if logged_emitted_nodes: lines.append(f"  {SHOW_DIM}emitted:{SHOW_RESET}")
                for emitted in logged_emitted_nodes:
                    emitted_id = emitted.node_id
                    emitted_func_name = emitted.func_name
                    lines.append(
                        f"        {SHOW_CYAN}[{emitted_id}] {emitted_func_name}{SHOW_RESET}"
                    )
    
                lines.append("")

        return _show_render(lines, color)

    @staticmethod
    def _show_value_name(value):
        if value.__class__.__name__ == "TIDEOpaqueValue":
            value_hash = getattr(value, "value_hash", "unhashed")
            return f"opaque<{value.type_name}>[{value_hash[:20]}]"

        return type(value).__name__ if not isinstance(value, type) else value.__name__

            
    # exec_record, actor, reference effect, value, 
    def _infer_kind(self, input):
        if isinstance(input, Inspected):
            return "value"

        # Imported TIDE Models retain the host's opaque Execution identifiers.
        # Resolve an exact indexed identity before applying native TIDE
        # heuristics such as sequence-number or value inference.
        try:
            if input in self.index.exec_by_exec_id:
                return "exec_record"
        except TypeError:
            pass

        kind = "unknown"
        if self._is_val_hash(input):
            #print(f"Infered {input} as hash. If this is an wrong, please use trace.inspect(obj, kind='hash' or 'hash')")
            return "hash"
        # is input actor
        if isinstance(input, tuple):
            if len(input) == 2 and isinstance(input[0], Effects):
                kind = "effect"
            elif len(input) == 3 and isinstance(input[0], int):
                if isinstance(input[1], (int, str)) and isinstance(input[2], (int, str)):
                    #print(f"Infered {input} as Actor. If this is a value, please use trace.inspect(obj, kind='value') as input")
                    kind = "actor"
                    
        # is reference or exec_id 
        elif isinstance(input, str):
            if _is_tide_exec_uuid(input):
                kind ="exec_record"
            elif ":" in input:
                split = input.split(":")[0]
                self.runner: PipelineRunner
                if hasattr(self.runner, "store_router") and hasattr(self.runner.store_router, "registry") and hasattr(self.runner.store_router, "__seen_stores__"):
                    if split in self.runner.store_router.registry.__seen_stores__:
                    #print(f"Infered {input} as reference. If this is an wrong, please use trace.inspect(obj, kind='ref' or 'reference')")
                        kind='reference'
                elif any(split.startswith(store) for store in DEFAULT_TIDE_STORES):
                    kind = 'reference' 
            else:
                #print(f"Infered {input} as Value. If this is a value, please use trace.inspect(obj, kind='') as input")
                kind = "value"
        elif isinstance(input, int):
            if input < len(self.exec_log):
                return "seq_id"
            else:
                kind = "value"
        else:          
            #print(f"Infered {input} as Value. If this is wrong, please use trace.inspect(obj, kind='value') as input")
            kind = "value"
            
        return kind
    
    def _is_valid_obj_kind(self, candidate):
        return candidate in [
            "seq_id",
            "exec",
            "exec_id",
            "exec_record",
            "value",
            "hash",
            "ref", 
            "reference",
            "dep", 
            "actor", 
            "effect" 
        ]

    def inspect(
        self,
        reference: Any,
        kind: str | None = None,
        version=None,
        grouped: bool = True,
    ) -> Inspected | InspectedGroup:
        
        # if not kind:
        #     raise RuntimeError(f"[INSPECT] {reference} used kind {kind}")    
        if isinstance(reference, Inspected):
            return reference
        if isinstance(reference, Object):
            reference = reference.id
        if not kind:
            kind = self._infer_kind(reference)  
        
        if not self._is_valid_obj_kind(kind):
            raise InvalidObjectKindError(f"{reference} | type={kind} is not known or does not exist in TIDE!", kind)
        object = Object(object=reference, type=kind, id=reference)
        inspected = Inspected(object)
        fallback = Inspected(object)
        if kind == "seq_id":
            if reference < len(self.exec_log):
                exec_log = self.exec_log[reference]
                reference = exec_log.exec_id
                kind = "exec_id" 

        if kind == "exec" or kind=="exec_record" or kind == "exec_id":   
            inspected = self.inspect_exec(reference)

        if kind == "value" or kind == "hash":
            inspected = self.inspect_value(reference, kind)

        if kind == "ref" or kind == "reference" or kind == "dep":
            
            if isinstance(reference, str) and ":" not in reference:
                if not grouped:
                    inspected_ref = self.find(reference, version).first("reference")
                    reference = inspected_ref.id
                else:
                    inspected = self.inspect_as_group(reference, "reference", version)
                    return inspected
            
            inspected = self.inspect_ref(reference, kind, version)

        if kind == "actor": # add an Actor(object)\
            if isinstance(reference, str):
                if not grouped:
                    inspected_actor = self.find(reference).first("actor")
                    if inspected_actor and inspected_actor.object.source_ocean:
                        reference = inspected_actor.id
                else:
                    inspected = self.inspect_as_group(reference, "actor")
                    return inspected
            if isinstance(reference, Actor):
                reference = (reference.source_ocean, reference.source_runner, reference.source_actor)
            if isinstance(reference, str):
                return inspected

            inspected = self.inspect_actor(*reference)
            
                
           

        if kind == "effect":
            inspected = self.inspect_effect(reference, version)

        if inspected.exists:
            return inspected
        return fallback
    def _get_pipeline_named_tuple_from_id(self, pipeline_id, runner_id=None):
        supplied_name = self._source_pipeline_names.get(
            (runner_id, pipeline_id)
        )
        if supplied_name is not None:
            return pipeline_id, supplied_name

        pipeline_tuple = self.all_known_pipelines.get(pipeline_id, None)
        if pipeline_tuple is not None:
            pipeline_name = pipeline_tuple[0].name
        else:
            pipeline_name = pipeline_id
        named_tuple = (pipeline_id, pipeline_name)
        return named_tuple

    def _dep_records(self, record: DependencyUseRecord):
        return record.expanded_to if record.type == "dynamic" else [record]

    def inspect_exec(self, exec_id: str):
        try:
            exec_log: ExecRecord = self.index.exec_by_exec_id[exec_id]
        except KeyError:
            object = Object(object=exec_id, type="exec_record", id=exec_id)
            inspected = Inspected(object)
            return inspected

        obj = Object(exec_log)
        obj.type = "exec_record"
        obj.id = exec_log.exec_id
        obj.source_runner = [exec_log.runner_id]
        obj.source_ocean = [exec_log.source_ocean_id]
        obj.source_pipeline = [self._get_pipeline_named_tuple_from_id(
            exec_log.pipeline_id,
            exec_log.runner_id,
        )]


        def strip_expansion(dynamic_dep_record: DependencyUseRecord):
            if dynamic_dep_record.type == "dynamic":
                stripped = copy.deepcopy(dynamic_dep_record)
                stripped.expanded_to = []
                return stripped
            return dynamic_dep_record

        obj.declared = {
            "deps": copy.deepcopy(exec_log.declared_deps),
            "effects": copy.deepcopy(exec_log.declared_effects),
            "targets": copy.deepcopy(exec_log.declared_targets),
        }

        used_deps = DedupedList()
        for dep in exec_log.deps:
            for dep_record in self._dep_records(dep):
                used_deps.append((dep_record.unscoped_reference, dep_record.resolved_version))

            
        obj.used = {
            "deps": used_deps,
            "effects": exec_log.effects,
            "targets": [(write.stored_target, write.stored_version) for write in exec_log.writes],
        }

        obj.reads = exec_log.deps
        obj.writes = exec_log.writes
        obj.effects = exec_log.effects

        obj.output = __TEMPLATE__

        upstream = deduped_dict()

        # for _, record in exec_log.inputs.items():
        #     record: InputUseRecord
        #     dep_records = record.dependencies
        for dependency in exec_log.deps:
            for dep_record in self._dep_records(dependency):
                dep_record: DependencyUseRecord
                producer_exec_id = dep_record.producer_exec_id
                if producer_exec_id is None:
                    continue

                producer_exec = self.index.exec_by_exec_id.get(producer_exec_id)
                if producer_exec is None:
                    continue

                upstream[producer_exec.producer_id].append(producer_exec_id)

        downstream = deduped_dict()
        for record in exec_log.writes:
            record: WriteRecord
            reference = record.stored_target
            version = record.stored_version
            consumer_exec_ids = self.index.ref_version_to_consumer_exec_ids.get((reference, version), [])
            
            for id in consumer_exec_ids:
                consumer_exec = self.index.exec_by_exec_id[id]    
                downstream[consumer_exec.producer_id].append(id)

        inspected = Inspected(obj, None, upstream, downstream)

        return inspected

    def inspect_value(self, value_or_hash: Any, kind: str):
        hashed = tide_value_hasher(value_or_hash) if kind == "value" else value_or_hash

        if kind == "hash":
            value = self.index.value_by_hash[hashed]
            obj = Object(value)
        else:
            obj = Object(value_or_hash)
        obj.id = hashed
        obj.type = "value"

        producer_exec_ids = self.index.value_hash_to_producer_exec_ids.get(hashed, [])

        upstream = deduped_dict()

        source_oceans = DedupedList()
        source_runners = DedupedList()
        source_pipelines = DedupedList()
        for exec_id in producer_exec_ids:
            exec_log = self.index.exec_by_exec_id[exec_id]
            source_oceans.add(exec_log.source_ocean_id)
            source_runners.add(exec_log.runner_id)
            source_pipelines.add(self._get_pipeline_named_tuple_from_id(exec_log.pipeline_id))
            upstream[exec_log.producer_id].append(exec_log.exec_id)

        obj.source_ocean = source_oceans
        obj.source_runner = source_runners
        obj.source_pipeline = source_pipelines

        history = self.index.value_hash_to_versioned_refs.get(hashed, [])

        for ver_ref in history:
            
            if ver_ref in self.index.ref_ver_to_read_egdes:
                obj.reads.extend(self.index.ref_ver_to_read_egdes[ver_ref])
            if ver_ref in self.index.ref_ver_to_write_egde:
                obj.writes.append(self.index.ref_ver_to_write_egde[ver_ref])


        downstream = deduped_dict()
        for exec_id in self.index.value_hash_to_consumer_exec_ids.get(hashed, []):
            exec_log = self.index.exec_by_exec_id[exec_id]
            downstream[exec_log.producer_id].append(exec_log.exec_id)

        inspected = Inspected(obj, history, upstream, downstream)
        return inspected

    def inspect_ref(self, reference: str, kind: str, version: int = None):
        obj = Object(reference)
        obj.type = "reference"
        obj.id = reference
        full_version_history = list(self.index.versions_by_ref.get(reference, []))
        if not full_version_history:
            return Inspected(obj)

        if version is not None:
            obj.version = version
            if version not in full_version_history:
                return Inspected(obj)

        if version is not None:
            version_history = [
                value_version
                for value_version in full_version_history
                if value_version <= version
            ]
        else:
            version_history = full_version_history

        for ver in version_history:
            if version is not None and version != ver:
                continue
            ver_ref = (reference, ver)
            
            if ver_ref in self.index.ref_ver_to_read_egdes:
                obj.reads.extend(self.index.ref_ver_to_read_egdes[(reference, ver)])
            if ver_ref in self.index.ref_ver_to_write_egde:
                obj.writes.append(self.index.ref_ver_to_write_egde[(reference, ver)])

        if kind != "dep":
            producer_exec_ids = self.index.ref_to_writer_exec_ids.get(reference, [])

            source_oceans = DedupedList()
            source_runners = DedupedList()
            source_pipelines = DedupedList()
            for exec_id in producer_exec_ids:
                for ver in version_history:
                    if version is not None: 
                        if version != ver or exec_id != self.index.ref_version_to_producer_exec_id[(reference, ver)]:
                            continue
                        
                    exec_log = self.index.exec_by_exec_id[exec_id]
                    source_oceans.add(exec_log.source_ocean_id)
                    source_runners.add(exec_log.runner_id)
                    source_pipelines.add(self._get_pipeline_named_tuple_from_id(exec_log.pipeline_id))
            obj.source_ocean = source_oceans
            obj.source_runner = source_runners
            obj.source_pipeline = source_pipelines

        downstream = deduped_dict()
        if version is not None:
            consumer_exec_ids = self.index.ref_version_to_consumer_exec_ids.get((reference, version), [])
        else:
            consumer_exec_ids = self.index.ref_to_consumer_exec_ids.get(reference, [])

        for exec_id in consumer_exec_ids:
            for ver in version_history:
                if version is not None: 
                    if version != ver or exec_id not in  self.index.ref_version_to_consumer_exec_ids[(reference, ver)]:
                        continue
                exec_log = self.index.exec_by_exec_id[exec_id]

                downstream[exec_log.producer_id].append(exec_log.exec_id)

        upstream = deduped_dict()
        for v in version_history:
            producer_exec_id = self.index.ref_version_to_producer_exec_id.get((reference, v))
            if producer_exec_id is None:
                continue
            for ver in version_history:
                if version is not None: 
                    if version != ver or producer_exec_id != self.index.ref_version_to_producer_exec_id[(reference, ver)]:
                        continue
                exec_log = self.index.exec_by_exec_id[producer_exec_id]
                
                upstream[exec_log.producer_id].append(exec_log.exec_id)

        inspected = Inspected(obj, version_history, upstream, downstream)

        return inspected

    # def inspect_ref_as_group(self, ref_name:str):

    def inspect_as_group(self, reference: str, kind=None, version=None):
        if not kind:
            kind = self._infer_kind(reference)

        if kind == "reference":
            find_kind = "refs"
        elif kind == "actor":
            find_kind = "actors"
        else:
            object = Object(reference, kind, reference)
            return Inspected(object)
        found = self.find(reference, version)
        
        if not found:
            object = Object(reference, kind, reference)
            return Inspected(object)
        
        found_kind = getattr(found.nearby, f"{find_kind}")
        
        objects = []
        source_oceans = DedupedList()
        source_runners = DedupedList()
        source_pipelines = DedupedList()
        history = DedupedList()
        history_dict = deduped_dict()
        upstream = deduped_dict()
        downstream = deduped_dict()

        reads = DedupedList()
        writes = DedupedList()

        for found_inspected in found_kind:
            found_inspected: Inspected
            if version is not None:
                if version not in found_inspected.history:
                    continue
                found_inspected = self.inspect(found_inspected.id, "reference", version)
            objects.append(found_inspected.object)
            reads.extend(found_inspected.object.reads)
            writes.extend(found_inspected.object.writes)
            source_oceans.extend(found_inspected.object.source_ocean)
            source_runners.extend(found_inspected.object.source_runner)
            source_pipelines.extend(found_inspected.object.source_pipeline)
            if found_inspected.type == "reference":
                history.extend([(found_inspected.object.id, ver) for ver in found_inspected.history])
            else:
                curr_hist = [self.inspect(exec_id, "exec_record") for exec_id in found_inspected.history]
                for inspected_exec in curr_hist:
                    history_dict[inspected_exec.object.object.seq_id] = inspected_exec.id

            for source_id, execs in found_inspected.upstream.items():
                upstream[source_id].extend(execs)
            
            for source_id, execs in found_inspected.downstream.items():
                downstream[source_id].extend(execs)
        if not history:    
           history = [history_dict[idx] for idx in sorted(history_dict)]        
            

        inspected_group = InspectedGroup(objects, kind, reference, history, upstream, downstream, source_oceans, source_runners, source_pipelines, version=version, reads=reads, writes=writes)
        return inspected_group

    def inspect_actor(self, ocean_id: int | None, source: str | int, actor: str | int):
        actor = Actor(ocean_id, source, actor)

        obj = Object(actor)
        obj.type = "actor"
        obj.id = actor

        exec_ids = self.index.actor_to_exec_ids.get(actor, [])

        obj.source_ocean = [ocean_id]
        obj.source_runner = [source]
        
        obj.source_pipeline = DedupedList()

        upstream = deduped_dict()
        downstream = deduped_dict()

        # Emission is a causal actor relationship even when no value
        # dependency connects the two nodes. The event log already records
        # and indexes this relationship; expose it to the same path surface
        # used by read/write dependencies.
        for emitted_actor in self.index.actors_by_emitter_actor.get(actor, []):
            downstream[emitted_actor].extend(
                self.index.actor_to_exec_ids.get(emitted_actor, [])
            )

        for emitter_exec_id in (
            self.index.emitted_actor_to_emitter_exec_ids.get(actor, [])
        ):
            emitter_log = self.index.exec_by_exec_id[emitter_exec_id]
            upstream[emitter_log.producer_id].append(emitter_exec_id)

        for exec_id in exec_ids:
            exec_inspected = self.inspect(exec_id, "exec_record")
            reads = self.index.reads_by_exec_id.get(exec_id, [])
            writes = self.index.writes_by_exec_id.get(exec_id, [])

            obj.reads.extend(reads)
            obj.writes.extend(writes)
            
            obj.source_pipeline.extend(exec_inspected.object.source_pipeline)

            
            for record in reads:
                for dep_record in self._dep_records(record):
                    dep_record: DependencyUseRecord
                    reference = dep_record.unscoped_reference
                    version = dep_record.resolved_version
                    producer_exec_log = self.index.exec_by_exec_id[
                        self.index.ref_version_to_producer_exec_id[(reference, version)]
                    ]
                    upstream[producer_exec_log.producer_id].append(producer_exec_log.exec_id)

            for record in writes:
                record: WriteRecord
                reference = record.stored_target
                version = record.stored_version
                exec_logs = [
                    self.index.exec_by_exec_id[consumer_exec_id]
                    for consumer_exec_id in self.index.ref_version_to_consumer_exec_ids.get((reference, version), [])
                ]
                for exec_log in exec_logs:
                    downstream[exec_log.producer_id].append(exec_log.exec_id)

            obj.effects.extend(exec_inspected.object.effects)

        inspected = Inspected(obj, exec_ids, upstream, downstream)

        return inspected

    def inspect_effect(self, reference, version):
        effect = None
        target = None
        embedded_version = None

        if isinstance(reference, Effects):
            effect = reference
        elif isinstance(reference, tuple) and len(reference) == 2:
            effect = reference[0]
            target = reference[1]
            if isinstance(target, tuple) and len(target) == 2:
                target, embedded_version = target
        else:
            raise TypeError(
                "Trace effect identifiers must be Effects.KIND, "
                "(Effects.KIND, reference), or "
                "(Effects.KIND, (reference, version))"
            )

        if not isinstance(effect, Effects):
            raise TypeError("Trace effect kind must be an Effects value")

        selected_version = version if version is not None else embedded_version
        if target is None:
            effect_id = effect
            records = self.index.effect_records_by_type.get(effect, [])
            producer_exec_ids = self.index.effect_type_to_producer_exec_ids.get(
                effect,
                [],
            )
            all_affected = self.index.effect_type_to_affected.get(effect, [])
        elif selected_version is None:
            effect_id = (effect, target)
            records = self.index.effect_records_by_target.get(effect_id, [])
            producer_exec_ids = self.index.effect_to_producer_exec_ids.get(
                effect_id,
                [],
            )
            all_affected = self.index.effect_to_affected.get(effect_id, [])
        else:
            effect_id = (effect, (target, selected_version))
            records = self.index.effect_records_by_versioned_target.get(
                (effect, target, selected_version),
                [],
            )
            producer_exec_ids = (
                self.index.effect_versioned_target_to_producer_exec_ids.get(
                    (effect, target, selected_version),
                    [],
                )
            )
            all_affected = self.index.effect_versioned_target_to_affected.get(
                (effect, (target, selected_version)),
                [],
            )

        obj = Object(effect_id)
        obj.id = effect_id
        obj.type = "effect"
        obj.version = selected_version
        obj.effects.extend(records)

        source_oceans = DedupedList()
        source_runners = DedupedList()
        source_pipelines = DedupedList()

        upstream = deduped_dict()

        for exec_id in producer_exec_ids:
            exec_log = self.index.exec_by_exec_id[exec_id]
            source_oceans.add(exec_log.source_ocean_id)
            source_runners.add(exec_log.runner_id)
            source_pipelines.add(self._get_pipeline_named_tuple_from_id(exec_log.pipeline_id))
            upstream[exec_log.producer_id].append(exec_log.exec_id)

        obj.source_ocean = source_oceans
        obj.source_runner = source_runners
        obj.source_pipeline = source_pipelines

        downstream = deduped_dict()

        for affected in all_affected:
            for affectee, record in affected.items():
                record: AffectedRecord
                affected_exec_id = record.affected_exec_id
                affected_actor = affectee
                affected_inputs = record.affected_inputs
                downstream[affected_actor].append(affected_exec_id or affected_inputs)

        inspected = Inspected(
            obj,
            history=list(producer_exec_ids),
            upstream=upstream,
            downstream=downstream,
        )
        return inspected

    @staticmethod
    def _validate_stream_limits(depth, max_paths):
        if depth is not None and (
            isinstance(depth, bool)
            or not isinstance(depth, int)
            or depth < 0
        ):
            raise ValueError("stream depth must be a non-negative int or None")
        if max_paths is not None and (
            isinstance(max_paths, bool)
            or not isinstance(max_paths, int)
            or max_paths < 1
        ):
            raise ValueError("stream max_paths must be a positive int or None")

    def _stream_adjacency(self, direction):
        if direction == "upstream":
            return (
                self.index.consumer_exec_id_to_producer_input_edges,
                "producer_exec_id",
            )
        if direction == "downstream":
            return (
                self.index.producer_exec_id_to_consumer_input_edges,
                "consumer_exec_id",
            )
        raise ValueError(
            "stream direction must be 'upstream' or 'downstream'"
        )

    @staticmethod
    def _stream_root_state_versions(inspected, direction):
        """Constrain a downstream State query to that exact State.

        Traversal is Execution-based after the first handoff.  A reference or
        value query must therefore select its own State-labelled root edges
        before traversal begins; otherwise sibling outputs of the same
        producer appear as false downstream ancestry.
        """
        if direction != "downstream":
            return None

        if inspected.type == "reference":
            if inspected.grouped:
                return set(inspected.history)

            versions = (
                [inspected.version]
                if inspected.version is not None
                else inspected.history
            )
            return {
                (inspected.id, version)
                for version in versions
            }

        if inspected.type == "value":
            return set(inspected.history)

        if inspected.type == "effect":
            return {
                record.targets
                for record in inspected.object.effects
                if record.targets[0] != "Runner"
            }

        return None

    def _get_stream(self, inspected, depth, direction, max_paths):
        """Walk exact relationship paths, then derive a unique layer view."""
        self._validate_stream_limits(depth, max_paths)
        adjacency, next_relation = self._stream_adjacency(direction)
        seeds = self._execution_seeds(inspected)
        root_state_versions = self._stream_root_state_versions(
            inspected,
            direction,
        )
        paths_by_root = defaultdict(list)
        path_keys_by_root = defaultdict(set)
        path_count = 0
        truncated = False

        def add_path(root_exec_id, path):
            nonlocal path_count, truncated
            path_key = tuple(Stream._edge_key(edge) for edge in path)
            if path_key in path_keys_by_root[root_exec_id]:
                return True
            if max_paths is not None and path_count >= max_paths:
                truncated = True
                return False
            path_keys_by_root[root_exec_id].add(path_key)
            paths_by_root[root_exec_id].append(path)
            path_count += 1
            return True

        for root_exec_id in seeds:
            for root_edge in adjacency[root_exec_id]:
                if truncated:
                    break
                if (
                    root_state_versions is not None
                    and (
                        root_edge.produced_ref,
                        root_edge.produced_ver,
                    ) not in root_state_versions
                ):
                    continue
                next_exec_id = getattr(root_edge, next_relation)
                stack = [(
                    next_exec_id,
                    [root_edge],
                    depth,
                    {root_exec_id, next_exec_id},
                )]

                while stack and not truncated:
                    current_exec_id, path, remaining, visited = stack.pop()
                    if remaining == 0:
                        add_path(root_exec_id, path)
                        continue

                    candidates = []
                    for edge in adjacency[current_exec_id]:
                        candidate_exec_id = getattr(edge, next_relation)
                        if candidate_exec_id in visited:
                            continue
                        candidates.append((edge, candidate_exec_id))

                    if not candidates:
                        add_path(root_exec_id, path)
                        continue

                    next_remaining = (
                        None if remaining is None else remaining - 1
                    )
                    for edge, candidate_exec_id in reversed(candidates):
                        stack.append((
                            candidate_exec_id,
                            [*path, edge],
                            next_remaining,
                            visited | {candidate_exec_id},
                        ))
            if truncated:
                break

        stream_stack = defaultdict(lambda: defaultdict(list))
        for root_exec_id, paths in paths_by_root.items():
            edge_projection = {}
            projection_order = 0
            for path in paths:
                for index, edge in enumerate(path):
                    layer = "root" if index == 0 else index - 1
                    edge_key = Stream._edge_key(edge)
                    layer_rank = -1 if layer == "root" else layer
                    projected = edge_projection.get(edge_key)
                    if projected is None:
                        edge_projection[edge_key] = {
                            "edge": edge,
                            "layer": layer,
                            "layer_rank": layer_rank,
                            "order": projection_order,
                        }
                        projection_order += 1
                    elif layer_rank < projected["layer_rank"]:
                        projected["layer"] = layer
                        projected["layer_rank"] = layer_rank

            for projected in sorted(
                edge_projection.values(),
                key=lambda item: (item["layer_rank"], item["order"]),
            ):
                stream_stack[root_exec_id][projected["layer"]].append(
                    projected["edge"]
                )

        return stream_stack, paths_by_root, truncated


    def upstream(
        self,
        inspected: Any,
        type: str | None = None,
        grouped: bool = True,
        depth: int | None = None,
        max_paths: int | None = DEFAULT_STREAM_MAX_PATHS,
    ) -> Upstream:
        if not isinstance(inspected, Inspected):
            inspected = self.inspect(
                inspected,
                type,
                grouped=grouped,
            )
        stream_stack, stream_paths, truncated = self._get_stream(
            inspected,
            depth,
            "upstream",
            max_paths,
        )
        return Upstream(
            inspected,
            stream_stack,
            stream_paths,
            inspect=self.inspect,
            truncated=truncated,
            max_paths=max_paths,
        )


    def downstream(
        self,
        inspected: Any,
        type: str | None = None,
        grouped: bool = True,
        depth: int | None = None,
        max_paths: int | None = DEFAULT_STREAM_MAX_PATHS,
    ) -> Downstream:
        if not isinstance(inspected, Inspected):
            inspected = self.inspect(
                inspected,
                type,
                grouped=grouped,
            )
        stream_stack, stream_paths, truncated = self._get_stream(
            inspected,
            depth,
            "downstream",
            max_paths,
        )
        return Downstream(
            inspected,
            stream_stack,
            stream_paths,
            inspect=self.inspect,
            truncated=truncated,
            max_paths=max_paths,
        )

    def _execution_seeds(self, inspected: Inspected) -> list[str]:
        """Return the execution occurrences represented by a query endpoint."""
        if not isinstance(inspected, Inspected):
            raise ValueError("execution seeding requires an Inspected endpoint")

        seeds = DedupedList()

        if inspected.type == "exec_record":
            return [inspected.object.id]
        elif inspected.type == "actor":
            seeds.extend(inspected.history)
        elif inspected.type in ["reference", "value"]:
            if inspected.type == "reference" and not inspected.grouped:
                versions = (
                    [inspected.version]
                    if inspected.version is not None
                    else inspected.history
                )
                history = [
                    (inspected.id, version)
                    for version in versions
                ]
            else:
                history = inspected.history

            for ref_ver in history:
                seeds.add(
                    self.index.ref_version_to_producer_exec_id[ref_ver]
                )
            # to find consumers of ref use trace.producers_of(ref)

        elif inspected.type == "effect":
            seeds.extend(inspected.history)

        else:
            raise NotImplementedError(f"Seeding currently only works for execs, refs, actors, values, and effects | was {inspected.type}")

        return seeds
    
    def _get_target_refs(self, inspected_root: Inspected, direction:str):
        if inspected_root.type in ["reference", "value"]:
            if direction == "upstream":
                target_refs = [
                    (write.stored_target, write.stored_version)
                    for write in inspected_root.writes
                ]
            else:
                target_refs = [
                    (read.unscoped_reference, read.resolved_version)
                    for read in inspected_root.reads
                ]

            return list(target_refs)
        elif inspected_root.type == "effect":
            return [
                record.targets
                for record in inspected_root.object.effects
                if record.targets[0] != "Runner"
            ]
        else:
            return []
        

    def _lines_from_edges(self, edges, direction, indent="      "):
        if direction == "upstream":
            actor = "producer"
        else:
            actor = "consumer"

        lines = []
        connector = "└─"
        source_to_lines = defaultdict(list)
        for depth, edge in enumerate(edges):
            current_indent = indent if not depth == 0 else indent * 1
            because = (
                f"{edge.produced_ref}[v{edge.produced_ver}]"
                if isinstance(edge, StackLink)
                else f"{edge.cause_effect} at {edge.produced_ref}[v{edge.produced_ver}]"
            )

            via = (
                edge.consumer_input
                if isinstance(edge, StackLink)
                else [inputs[0] for inputs in edge.consumer_inputs]
            )

            to_link__exe = getattr(edge, f'{actor}_exec_id')
            inspected = self.inspect(to_link__exe, "exec_id")
            to_link__acto = repr(inspected.object)
            
            source_to_lines[to_link__acto].append(f"{current_indent}{connector} via {because}, affected {via}")

        for actor_source, actor_lines in source_to_lines.items():
            lines.append(actor_source)
            lines.extend(actor_lines)
        
        return lines

    @staticmethod
    def _path_target_states(target):
        """Return the exact State occurrences represented by an endpoint."""
        if target.type not in {"reference", "value"}:
            return set()
        return {
            (write.stored_target, write.stored_version)
            for write in target.writes
        }

    def _edge_reaches_path_target(
        self,
        edge,
        next_exec,
        source,
        target,
        target_execs,
        direction,
        deep_link,
    ):
        """Decide endpoint reachability from graph facts, never display text."""
        if isinstance(edge, EmittedStackLink):
            if target.type == "actor":
                return (
                    edge.consumer_actor == target.id
                    or (
                        target.grouped
                        and edge.consumer_actor.source_actor == target.id
                    )
                )

        if target.type not in {"reference", "value"}:
            return next_exec in target_execs

        if (
            direction == "upstream"
            and not deep_link
            and source.type in {"reference", "value"}
        ):
            return False

        target_states = self._path_target_states(target)
        edge_state = (edge.produced_ref, edge.produced_ver)
        if edge_state in target_states:
            return True

        if direction != "downstream" or not deep_link:
            return False

        candidate = self.index.exec_by_exec_id.get(next_exec)
        if candidate is None:
            return False
        return any(
            (write.stored_target, write.stored_version) in target_states
            for write in candidate.writes
        )

    def _edge_path_explanation(
        self,
        edge,
        next_exec,
        source,
        target,
        direction,
        deep_link,
        terminal,
    ):
        """Project one traversed edge into the existing display contract."""
        if isinstance(edge, EmittedStackLink):
            return []

        ref = edge.produced_ref
        ver = edge.produced_ver
        producer = getattr(
            edge.producer_actor,
            "source_actor",
            str(edge.producer_actor),
        )
        consumer = getattr(
            edge.consumer_actor,
            "source_actor",
            str(edge.consumer_actor),
        )

        if isinstance(edge, AffectedStackLink):
            if direction == "downstream":
                return [f"read {ref}[v{ver}] from {producer}"]
            return [f"wrote {ref}[v{ver}] used by {consumer}"]

        value_query = source.type == "value" or target.type == "value"
        if direction == "downstream":
            action = "read as" if value_query else "read"
            lines = [f"{action} {ref}[v{ver}] from {producer}"]

            if deep_link and target.type in {"reference", "value"}:
                target_states = self._path_target_states(target)
                candidate = self.index.exec_by_exec_id.get(next_exec)
                if candidate is not None:
                    write_action = "written as" if value_query else "wrote"
                    for write in candidate.writes:
                        state = (write.stored_target, write.stored_version)
                        if state in target_states and state != (ref, ver):
                            lines.append(
                                f"{write_action} {state[0]}[v{state[1]}]"
                            )
            return lines

        action = "written as" if value_query else "wrote"
        if value_query or (
            terminal
            and
            source.type == "reference" and target.type == "reference"
        ):
            return [f"{action} {ref}[v{ver}]"]
        return [f"{action} {ref}[v{ver}] used by {consumer}"]

    def _direct_path_explanation(
        self,
        root_exec,
        source,
        target,
        target_execs,
        direction,
    ):
        """Explain a direct endpoint incidence without traversing an edge."""
        if direction == "downstream" and source.type in {"reference", "value"}:
            return []

        record = self.index.exec_by_exec_id[root_exec]
        reads = [
            (read.unscoped_reference, read.resolved_version)
            for dependency in record.deps
            for read in self._dep_records(dependency)
        ]
        writes = [
            (write.stored_target, write.stored_version)
            for write in record.writes
        ]

        if source.type in {"reference", "value"}:
            source_states = self._path_target_states(source)
            writes = [state for state in writes if state in source_states]

        if target.type in {"reference", "value"}:
            target_states = self._path_target_states(target)
            reads = []
            writes = [state for state in writes if state in target_states]
        elif root_exec not in target_execs:
            return []
        elif direction == "downstream":
            reads = []
        elif source.type == "value":
            reads = []
        elif source.type != "reference":
            writes = []

        value_query = source.type == "value" or target.type == "value"
        lines = []
        for ref, ver in reads:
            producer_exec = self.index.ref_version_to_producer_exec_id[
                (ref, ver)
            ]
            producer = self.index.exec_by_exec_id[
                producer_exec
            ].producer_id.source_actor
            action = "read as" if value_query else "read"
            lines.append(f"{action} {ref}[v{ver}] from {producer}")

        action = "written as" if value_query else "wrote"
        lines.extend(f"{action} {ref}[v{ver}]" for ref, ver in writes)
        return lines

    def path(
        self,
        source: Any,
        target: Any,
        source_type: str | None = None,
        target_type: str | None = None,
        source_ver=None,
        target_ver=None,
        group: bool = True,
        deep_link: bool = True,
        direction: str = "downstream",
    ) -> Pathed:
        if isinstance(source, Investigate):
            source = source.inspected
            source_type = source.type
            
        if not isinstance(source, Inspected):
            if not source_type:    
                source_type = self._infer_kind(source)
            if not self._is_valid_obj_kind(source_type):
                raise InvalidObjectKindError(f"{source} | type={source_type} is not known or does not exist in TIDE!", source_type)
            
            if group:
                source = self.inspect(source, source_type, source_ver)
            else:
                source_match = self.find(source, source_ver).first(source_type)
                source = (
                    source_match
                    if source_match is not None
                    else self.inspect(source, source_type, source_ver)
                )
        
        if isinstance(target, Investigate):
            target = target.inspected
            target_type = target.type

        if not isinstance(target, Inspected):
            if not target_type:
                target_type = self._infer_kind(target)
            if not self._is_valid_obj_kind(target_type):
                raise InvalidObjectKindError(f"{target} | type={target_type} is not known or does not exist in TIDE!", target_type)    
            
            if group:
                target = self.inspect(target, target_type, target_ver)
            else:
                target_match = self.find(target, target_ver).first(target_type)
                target = (
                    target_match
                    if target_match is not None
                    else self.inspect(target, target_type, target_ver)
                )
        
        



        target_seeds = self._execution_seeds(target)
        valid_paths = defaultdict(list)
        source_seeds = self._execution_seeds(source)
        adjacency, next_relation = self._stream_adjacency(direction)
        root_state_versions = self._stream_root_state_versions(
            source,
            direction,
        )

        for root_exec_id in source_seeds:
            # 1. Direct/root proof:
            link = self._direct_path_explanation(
                root_exec_id,
                source,
                target,
                target_seeds,
                direction,
            )

            if link:
                valid_paths[root_exec_id].append(link)

            # 2. Edge-walk proof.  A stack frame either expands one execution
            # or evaluates one pending edge.  Reversing expansion order keeps
            # the same deterministic depth-first order as the former recursive
            # walk without depending on Python's call-stack limit.
            stack = [("expand", root_exec_id, [], {root_exec_id}, True)]
            while stack:
                frame, current_exec_id, branch_path, visited, is_root = stack.pop()

                if frame == "expand":
                    edges = adjacency[current_exec_id]
                    for curr_step in reversed(edges):
                        if (
                            is_root
                            and root_state_versions is not None
                            and (
                                curr_step.produced_ref,
                                curr_step.produced_ver,
                            ) not in root_state_versions
                        ):
                            continue
                        stack.append((
                            curr_step,
                            current_exec_id,
                            branch_path,
                            visited,
                            is_root,
                        ))
                    continue

                curr_step = frame
                if is_root and source.type == "effect":
                    effect_id = source.id
                    effect_kind = (
                        effect_id[0]
                        if isinstance(effect_id, tuple)
                        else effect_id
                    )
                    effect_refs = set(
                        self._get_target_refs(source, direction)
                    )
                    if effect_kind in {Effects.WRITE, Effects.DELETE}:
                        if not isinstance(curr_step, AffectedStackLink):
                            continue
                        if curr_step.cause_effect != effect_kind:
                            continue
                        if effect_refs and (
                            curr_step.produced_ref,
                            curr_step.produced_ver,
                        ) not in effect_refs:
                            continue

                next_exec = getattr(curr_step, next_relation)
                next_path = [*branch_path, curr_step]
                terminal = self._edge_reaches_path_target(
                    curr_step,
                    next_exec,
                    source,
                    target,
                    target_seeds,
                    direction,
                    deep_link,
                )
                link = self._edge_path_explanation(
                    curr_step,
                    next_exec,
                    source,
                    target,
                    direction,
                    deep_link,
                    terminal,
                )
                if link:
                    next_path.extend(link)

                # Match the target before suppressing a revisited execution.
                # This preserves a closing edge such as X -> ... -> X, while
                # the visited set still prevents expansion around the cycle.
                if terminal:
                    valid_paths[root_exec_id].append(next_path)
                    continue

                if (
                    deep_link
                    and next_exec is not None
                    and next_exec not in visited
                ):
                    stack.append((
                        "expand",
                        next_exec,
                        next_path,
                        visited | {next_exec},
                        False,
                    ))

        pathed = Pathed(self.inspect, self.index, source, source_seeds, target, target_seeds, valid_paths, direction)
        return pathed

    def produced_by(
        self,
        source: Any,
        kind: str | None = None,
        version=None,
        group: bool = True,
    ) -> ProducedBy | None:
        if not isinstance(source, Inspected):
            kind = kind or self._infer_kind(source)

            if group:
                source = self.inspect(source, kind=kind, version=version)
            else:
                source = self.find(source, version).first(kind)

            if not source:
                return None

        produced_refs_by_exec = deduped_dict()

        if source.type in ["reference", "value"]:
            execs = []

            if source.type == "reference":
                for ver_ref in source.history:
                    if not source.grouped:
                        ver_ref = (source.id, source.version)

                    producer_exec = self.index.ref_version_to_producer_exec_id[ver_ref]
                    execs.append(producer_exec)
                    produced_refs_by_exec[producer_exec].append(ver_ref)

            if source.type == "value":
                for ver_ref in source.history:
                    producer_exec = self.index.ref_version_to_producer_exec_id[ver_ref]
                    execs.append(producer_exec)
                    produced_refs_by_exec[producer_exec].append(ver_ref)

            produced_refs = DedupedList(
                ver_ref
                for ver_refs in produced_refs_by_exec.values()
                for ver_ref in ver_refs
            )

            return ProducedBy(
                self.inspect,
                source,
                execs,
                produced_refs,
                produced_refs_by_exec,
            )

        if source.type == "exec_record":
            execs = [source.id]

        elif source.type == "actor":
            execs = list(source.history)

        else:
            raise RuntimeWarning(
                f".produced_by() received an invalid type! was {source.type}"
            )

        for inspected in [
            self.inspect(exec_id, "exec_record")
            for exec_id in execs
        ]:
            for write in inspected.object.writes:
                write: WriteRecord

                ref = write.stored_target
                ver = write.stored_version

                produced_refs_by_exec[inspected.id].append((ref, ver))

        produced_refs = DedupedList(
            ver_ref
            for ver_refs in produced_refs_by_exec.values()
            for ver_ref in ver_refs
        )

        return ProducedBy(
            self.inspect,
            source,
            execs,
            produced_refs,
            produced_refs_by_exec,
        )

    def consumed_by(
        self,
        source: Any,
        kind: str | None = None,
        version=None,
        group: bool = True,
    ) -> ConsumedBy | None:
        if not isinstance(source, Inspected):
            kind = kind or self._infer_kind(source)

            if group:
                source = self.inspect(source, kind=kind, version=version)
            else:
                source = self.find(source, version).first(kind)

            if not source:
                return None

        consumed_refs_by_exec = defaultdict(lambda: DedupedList())

        if source.type in ["reference", "value"]:
            execs = []

            if source.type == "reference":
                for ver_ref in source.history:
                    if not source.grouped:
                        ver_ref = (source.id, source.version)

                    for exec_id in self.index.ref_version_to_consumer_exec_ids[ver_ref]:
                        execs.append(exec_id)
                        consumed_refs_by_exec[exec_id].append(ver_ref)

            if source.type == "value":
                for ver_ref in source.history:
                    for exec_id in self.index.ref_version_to_consumer_exec_ids[ver_ref]:
                        execs.append(exec_id)
                        consumed_refs_by_exec[exec_id].append(ver_ref)

            consumed_refs = DedupedList(
                ver_ref
                for ver_refs in consumed_refs_by_exec.values()
                for ver_ref in ver_refs
            )

            return ConsumedBy(
                self.inspect,
                source,
                execs,
                consumed_refs,
                consumed_refs_by_exec,
            )

        if source.type == "exec_record":
            execs = [source.id]

        elif source.type == "actor":
            execs = list(source.history)

        else:
            raise RuntimeWarning(
                f".consumed_by() received an invalid type! was {source.type}"
            )

        for inspected in [
            self.inspect(exec_id, "exec_record")
            for exec_id in execs
        ]:
            for dep_use in inspected.object.reads:
                queue = self.index._dep_queue(dep_use)

                for record in queue:
                    record: DependencyUseRecord

                    ref = record.unscoped_reference
                    ver = record.resolved_version

                    consumed_refs_by_exec[inspected.id].append((ref, ver))

        consumed_refs = DedupedList(
            ver_ref
            for ver_refs in consumed_refs_by_exec.values()
            for ver_ref in ver_refs
        )

        return ConsumedBy(
            self.inspect,
            source,
            execs,
            consumed_refs,
            consumed_refs_by_exec,
        )
                
    def investigate(
        self,
        inspected: Any,
        kind: str | None = None,
        version=None,
        grouped: bool = True,
    ) -> Investigate:
        if not isinstance(inspected, Inspected):
            if kind:
                found = self.find(inspected, version)
                
                if not found.exists(kind)[0]:
                    if grouped and isinstance(inspected, str) and kind in ["reference", "actor"]:
                        inspected = self.inspect_as_group(inspected, kind=kind, version=version)
                    else:    
                        found = self.find(inspected).first(kind)
                        if found is None:
                            inspected = self.inspect(inspected, kind=kind, version=version)
                        else:
                            inspected = found

                        inspected = self.inspect(inspected, kind=kind, version=version)
                else:
                    inspected = found.first(kind)

            else:
                kind = self._infer_kind(inspected)
                inspected = self.inspect(inspected, kind=kind, version=version)

                    

        return Investigate(
            trace=self,
            inspect=self.inspect,
            inspected=inspected,
        )


    # def to_dict(self):
    #     return {
    #         "type": self.type, # what is this object
    #         "id": self.id, # how can I find this object
    #         "ocean": self.source_ocean, # what ocean instance was I observed in...? ish
    #         "source": self.source_runner, # what source did I come from,
    #         "pipeline": self.source_pipeline, # what graph was I a part of 
    #         "version": self.version # am i versioned? what version am i  
    #     }
