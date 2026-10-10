"""Portable semantic encoding for native TIDE Trace evidence.



This is the modern continuation of the original TIDERunSerialiser design:

record types are encoded explicitly, while arbitrary captured Python values

are referenced by their TIDE value hash and stored by the outer Bundler.

"""



from __future__ import annotations


from tide_tooling.tooling_core.engine.classes import __fallback__



try:

    from tide_tooling.tooling_core.engine.utils import tide_value_hasher

except ImportError:  # Allows the module to be inspected standalone.

    tide_value_hasher = None



_MISSING_TIDE_RUN_VALUE = object()




class TIDERunSerialiser:

    KIND = "tide_run"

    CODEC_VERSION = 1



    def __init__(self, value_hasher=None):

        self.value_hasher = value_hasher or tide_value_hasher

        if self.value_hasher is None:

            raise ImportError(

                "TIDERunSerialiser requires "

                "tide_tooling.tooling_core.engine.utils.tide_value_hasher"

            )

        self._values = {}
        self._evidence_claims_only = False



    # ------------------------------------------------------------------

    # Public API

    # ------------------------------------------------------------------

    def serialise(self, trace):

        """Serialise one native Trace without embedding arbitrary values."""

        run, _ = self.serialise_with_values(trace)

        return run



    def serialise_with_values(self, trace):

        """Return ``(run, values)`` where values maps TIDE hash -> value.



        The outer TIDEBundler owns persistence of the value graph.  This codec

        owns only the semantic run schema and the references into that graph.

        """

        self._values = {}



        self._require_attrs(
            trace,
            ("log", "ready_attempts"),
            "Live Trace",
        )
        records = trace.log
        if not isinstance(records, (list, tuple)):
            records = tuple(records)

        ready_attempts = trace.ready_attempts



        run = {

            "kind": self.KIND,

            "codec_version": self.CODEC_VERSION,

            "trace": self._trace_metadata(trace),

            "records": [self._serialise_record(record) for record in records],

            "ready_attempts": [

                self._serialise_ready_attempt(attempt)

                for attempt in ready_attempts

            ],

        }

        return run, dict(self._values)


    def evidence_claims(self, trace):
        """Return only identity-scoped facts recorded by one Trace.

        Persistence context, loader policy, backing-value topology, query
        indexes, and other derived views are deliberately excluded.
        """

        self._values = {}
        previous_mode = self._evidence_claims_only
        self._evidence_claims_only = True
        try:
            records = {}
            for record in trace.log:
                encoded = self._serialise_record(record)
                if encoded["record_type"] == "exec":
                    identity = ("execution", encoded["exec_id"])
                else:
                    identity = ("immediate_effect", encoded["event_id"])
                if identity in records:
                    raise ValueError(
                        f"Duplicate Trace evidence identity: {identity!r}"
                    )
                records[identity] = encoded

            attempts = {}
            for attempt in trace.ready_attempts:
                encoded = self._serialise_ready_attempt(attempt)
                identity = encoded["event_id"]
                if identity in attempts:
                    raise ValueError(
                        "Duplicate ReadyAttempt evidence identity: "
                        f"{identity!r}"
                    )
                attempts[identity] = encoded

            return {
                "trace": self._trace_evidence_metadata(trace),
                "records": records,
                "ready_attempts": attempts,
            }
        finally:
            self._evidence_claims_only = previous_mode


    def _trace_evidence_metadata(self, trace):
        return {
            "ocean_id": trace.ocean_id,
            "runner": trace._selected_runner_id,
            "trace_starting_event_id": trace._trace_starting_event_id,
            "pipeline_starting_event_id": trace._pipeline_starting_event_id,
            "tide_plan": self._value_ref(trace._tide_plan),
            "missing_referenced_events": self._value_ref(
                trace._missing_referenced_events
            ),
            "causal_order_violations": self._value_ref(
                trace._causal_order_violations
            ),
            "ready_exec_ids": sorted(trace._ready_exec_ids, key=repr),
            "blocked_exec_ids": sorted(trace._blocked_exec_ids, key=repr),
            "deadlock": self._value_ref(trace._deadlock),
            "started_runner_ids": sorted(
                trace._started_runner_ids,
                key=repr,
            ),
            "causal_runner_ids": sorted(
                trace._causal_runner_ids,
                key=repr,
            ),
            "ended_runner_ids": sorted(trace._ended_runner_ids, key=repr),
            "evidence_violations": self._value_ref(
                trace._evidence_violations
            ),
        }



    @property

    def values(self):

        """Values referenced by the most recent serialisation session."""

        return dict(self._values)



    @staticmethod
    def _require_attrs(value, fields, label):
        missing = [
            field
            for field in fields
            if not hasattr(value, field)
        ]
        if missing:
            raise ValueError(
                f"{label} is missing required evidence fields: "
                f"{sorted(missing)}"
            )

    # ------------------------------------------------------------------

    # Trace metadata

    # ------------------------------------------------------------------

    def _trace_metadata(self, trace):

        self._require_attrs(
            trace,
            (
                "name", "ocean_id", "_selected_runner_id", "pipelines",
                "bundle_context", "model_context", "host", "host_record",
                "_trace_starting_event_id", "_pipeline_starting_event_id",
                "_tide_plan", "_missing_referenced_events",
                "_causal_order_violations", "_ready_exec_ids",
                "_blocked_exec_ids", "_deadlock", "_started_runner_ids",
                "_causal_runner_ids", "_ended_runner_ids",
                "_evidence_violations",
            ),
            "Live Trace",
        )

        return {

            "name": getattr(trace, "name", "TIDE: .trace()"),

            "ocean_id": getattr(trace, "ocean_id", None),

            "runner": getattr(trace, "_selected_runner_id", None),

            "pipelines": self._value_ref(getattr(trace, "pipelines", {})),

            "bundle_context": self._value_ref(

                getattr(trace, "bundle_context", {})

            ),

            "model_context": self._value_ref(

                getattr(trace, "model_context", {})

            ),

            "host": self._value_ref(getattr(trace, "host", None)),

            "host_record": self._value_ref(

                getattr(trace, "host_record", None)

            ),

            "trace_starting_event_id": getattr(

                trace, "_trace_starting_event_id", None

            ),

            "pipeline_starting_event_id": getattr(

                trace, "_pipeline_starting_event_id", None

            ),

            "tide_plan": self._value_ref(getattr(trace, "_tide_plan", None)),

            "missing_referenced_events": self._value_ref(

                getattr(trace, "_missing_referenced_events", {})

            ),

            "causal_order_violations": self._value_ref(

                getattr(trace, "_causal_order_violations", {})

            ),

            "ready_exec_ids": list(getattr(trace, "_ready_exec_ids", ())),

            "blocked_exec_ids": list(getattr(trace, "_blocked_exec_ids", ())),

            "deadlock": self._value_ref(getattr(trace, "_deadlock", {})),

            "started_runner_ids": list(

                getattr(trace, "_started_runner_ids", ())

            ),

            "causal_runner_ids": list(

                getattr(trace, "_causal_runner_ids", ())

            ),

            "ended_runner_ids": list(

                getattr(trace, "_ended_runner_ids", ())

            ),

            "evidence_violations": self._value_ref(

                getattr(trace, "_evidence_violations", {})

            ),

        }



    # ------------------------------------------------------------------

    # Native records

    # ------------------------------------------------------------------

    def _serialise_record(self, record):

        name = type(record).__name__

        if name == "ExecRecord":

            return self._serialise_exec_record(record)

        if name == "ImmediateEffectRecord":

            return self._serialise_immediate_effect_record(record)

        raise TypeError(f"Unsupported TIDE run record: {name}")



    def _serialise_exec_record(self, record):

        self._require_attrs(
            record,
            (
                "seq_id", "exec_id", "source_ocean_id", "runner_id",
                "pipeline_id", "node_id", "func_name",
                "producer_occurrence_id", "inputs", "deps", "effects",
                "affected", "declared_deps", "declared_effects",
                "declared_targets", "runtime_effects", "runtime_targets",
                "unscoped_store_refs", "status", "started_at",
                "finished_at", "duration_ms", "error", "handled_raise",
                "mutated_during", "now_meta", "output_value", "writes",
                "emitted_nodes", "emitted_by_exec_id", "base_node_id",
                "base_node_content_id", "content_id", "node_options",
            ),
            "ExecRecord",
        )

        return {

            "record_type": "exec",

            "seq_id": getattr(record, "seq_id", None),

            "event_seq_id": getattr(record, "event_seq_id", None),

            "exec_id": getattr(record, "exec_id", None),

            "ocean_id": getattr(record, "source_ocean_id", None),

            "runner_id": getattr(record, "runner_id", None),

            "pipeline_id": getattr(record, "pipeline_id", None),

            "node_id": self._value_ref(getattr(record, "node_id", None)),

            "func_name": getattr(record, "func_name", None),

            "producer_occurrence": self._serialise_occurrence(

                getattr(record, "producer_occurrence_id", None)

            ),

            "inputs": self._serialise_inputs(getattr(record, "inputs", None)),

            "deps": [

                self._serialise_dependency(dependency)

                for dependency in getattr(record, "deps", ()) or ()

            ],

            "effects": [

                self._serialise_effect_record(effect)

                for effect in getattr(record, "effects", ()) or ()

            ],

            "affected": self._value_ref(getattr(record, "affected", [])),

            "declared_deps": [

                self._serialise_data_ref(reference)

                for reference in getattr(record, "declared_deps", ()) or ()

            ],

            "declared_effects": [

                self._serialise_effect_descriptor(effect)

                for effect in getattr(record, "declared_effects", ()) or ()

            ],

            "declared_targets": [

                self._serialise_data_ref(reference)

                for reference in getattr(record, "declared_targets", ()) or ()

            ],

            "runtime_effects": [

                self._serialise_effect_descriptor(effect)

                for effect in getattr(record, "runtime_effects", ()) or ()

            ],

            "runtime_targets": [

                self._serialise_data_ref(reference)

                for reference in getattr(record, "runtime_targets", ()) or ()

            ],

            "unscoped_store_refs": self._value_ref(

                getattr(record, "unscoped_store_refs", None)

            ),

            "status": self._value_ref(getattr(record, "status", None)),

            "started_at": self._value_ref(getattr(record, "started_at", None)),

            "finished_at": self._value_ref(getattr(record, "finished_at", None)),

            "duration_ms": self._value_ref(getattr(record, "duration_ms", None)),

            "error": self._value_ref(getattr(record, "error", None)),

            "handled_raise": bool(getattr(record, "handled_raise", False)),

            "mutated_during": self._value_ref(

                getattr(record, "mutated_during", {})

            ),

            "now_meta": self._serialise_now_meta(
                getattr(record, "now_meta", None)
            ),

            "output_value": self._value_ref(

                getattr(record, "output_value", _MISSING_TIDE_RUN_VALUE)

            ),

            "writes": [

                self._serialise_write(write)

                for write in getattr(record, "writes", ()) or ()

            ],

            "emitted_nodes": [

                self._serialise_emitted_node(node)

                for node in getattr(record, "emitted_nodes", ()) or ()

            ],

            "emitted_by_exec_id": getattr(record, "emitted_by_exec_id", None),

            "base_node_id": self._value_ref(

                getattr(record, "base_node_id", None)

            ),

            "base_node_content_id": self._value_ref(

                getattr(record, "base_node_content_id", None)

            ),

            "content_id": self._value_ref(getattr(record, "content_id", None)),

            "node_options": self._value_ref(

                getattr(record, "node_options", {})

            ),

        }



    def _serialise_immediate_effect_record(self, record):

        self._require_attrs(
            record,
            (
                "seq_id", "event_id", "event_seq_id", "exec_id",
                "execution_reference", "effects", "deps", "writes",
            ),
            "ImmediateEffectRecord",
        )

        return {

            "record_type": "immediate_effect",

            "seq_id": getattr(record, "seq_id", None),

            "event_id": getattr(record, "event_id", None),

            "event_seq_id": getattr(record, "event_seq_id", None),

            "exec_id": getattr(record, "exec_id", None),

            "execution_reference": self._serialise_execution_reference(

                getattr(record, "execution_reference", None)

            ),

            "effects": [

                self._serialise_effect_record(effect)

                for effect in getattr(record, "effects", ()) or ()

            ],

            "deps": [

                self._serialise_dependency(
                    dependency,
                    bind_input=False,
                )

                for dependency in getattr(record, "deps", ()) or ()

            ],

            "writes": [

                self._serialise_write(write)

                for write in getattr(record, "writes", ()) or ()

            ],

        }



    def _serialise_ready_attempt(self, attempt):

        self._require_attrs(
            attempt,
            (
                "event_id", "event_seq_id", "exec_id", "actor",
                "node_definition_id", "runner_id", "pipeline_id",
                "node_snapshot", "outcome",
            ),
            "ReadyAttemptRecord",
        )

        return {

            "event_id": getattr(attempt, "event_id", None),

            "event_seq_id": getattr(attempt, "event_seq_id", None),

            "exec_id": getattr(attempt, "exec_id", None),

            "actor": self._serialise_actor(getattr(attempt, "actor", None)),

            "node_definition_id": self._value_ref(

                getattr(attempt, "node_definition_id", None)

            ),

            "runner_id": getattr(attempt, "runner_id", None),

            "pipeline_id": getattr(attempt, "pipeline_id", None),

            "node_snapshot": self._serialise_node_snapshot(
                getattr(attempt, "node_snapshot", None)
            ),

            "outcome": getattr(attempt, "outcome", "ready"),

        }




    # ------------------------------------------------------------------
    # Native semantic values
    # ------------------------------------------------------------------

    _EFFECT_FIELDS = {
        "EMIT": frozenset({"output", "nodes"}),
        "END": frozenset({"output", "targets"}),
        "RUN": frozenset({
            "output", "targets", "mode", "local_store_templates",
            "stay_alive", "runner_name", "dynamic_ref_plan",
        }),
        "DELETE": frozenset({"output", "targets"}),
        "WRITE": frozenset({"output", "targets"}),
        "READ_EFFECT": frozenset({"output", "targets", "fallback"}),
        "RAISE": frozenset({"output", "exception", "effects"}),
    }

    def _serialise_effect_descriptor(self, effect):
        class_name = getattr(effect, "__name__", type(effect).__name__)

        if isinstance(effect, type):
            if class_name not in self._EFFECT_FIELDS:
                raise TypeError(f"Unsupported TIDE Effect class: {class_name}")
            return {"form": "type", "class": class_name}

        if isinstance(effect, dict):
            return {"form": "mapping", "value": self._value_ref(effect)}

        if class_name not in self._EFFECT_FIELDS:
            return {"form": "value", "value": self._value_ref(effect)}

        attributes = {
            name: getattr(effect, name)
            for name in vars(effect)
            if not name.startswith("_tide_")
        }
        expected = self._EFFECT_FIELDS[class_name]
        actual = set(attributes)
        if actual != set(expected):
            missing = sorted(set(expected) - actual)
            unknown = sorted(actual - set(expected))
            raise ValueError(
                f"{class_name} Effect fields do not match TideRun v1: "
                f"missing {missing}, unknown {unknown}"
            )

        payload = {}
        for name in sorted(expected):
            value = attributes[name]
            if class_name == "RAISE" and name == "effects":
                payload[name] = [
                    self._serialise_effect_descriptor(item)
                    for item in value
                ]
            else:
                payload[name] = self._value_ref(value)

        return {
            "form": "object",
            "class": class_name,
            "payload": payload,
        }

    def _serialise_now_meta(self, metadata):
        if metadata is None:
            return None

        self._require_attrs(
            metadata,
            (
                "trial_id", "activation_id", "evaluation_id",
                "function", "line", "output",
            ),
            "NOWMeta",
        )
        if type(metadata).__name__ != "NOWMeta":
            raise TypeError("ExecRecord.now_meta must be NOWMeta or None")

        return {
            "trial_id": metadata.trial_id,
            "activation_id": metadata.activation_id,
            "evaluation_id": metadata.evaluation_id,
            "function": metadata.function,
            "line": metadata.line,
            "output": self._value_ref(metadata.output),
        }

    def _serialise_tide_snapshot(self, snapshot):
        self._require_attrs(
            snapshot,
            (
                "value_hash", "capture_method", "type_name",
                "representation", "boundary", "exact",
                "observed_mutable", "trusted",
            ),
            "TIDEValueSnapshot",
        )

        snapshot = snapshot._for_persistence()

        value_bundle = snapshot.value_bundle
        value = snapshot.value

        claim = {
            "value_hash": snapshot.value_hash,
            "capture_method": snapshot.capture_method,
            "type_name": snapshot.type_name,
            "representation": snapshot.representation,
            "boundary": snapshot.boundary,
            "exact": bool(snapshot.exact),
            "observed_mutable": bool(snapshot.observed_mutable),
        }
        if self._evidence_claims_only:
            return claim

        return {
            **claim,
            "trusted": bool(snapshot.trusted),
            "value_bundle": (
                None if value_bundle is None else self._value_ref(value_bundle)
            ),
            "value": None if value is None else self._value_ref(value),
        }

    @staticmethod
    def _snapshot_lineage_claims(lineage):
        fields = (
            "value_hash",
            "capture_method",
            "type",
            "repr",
            "boundary",
            "exact",
            "observed_mutable",
        )
        claims = []
        for snapshot in lineage or ():
            if not isinstance(snapshot, dict):
                raise TypeError("Snapshot lineage entries must be dictionaries")
            claims.append({
                field: snapshot[field]
                for field in fields
                if field in snapshot
            })
        return claims

    def _serialise_snapshot_lineage(self, lineage):
        lineage = list(lineage or ())
        if self._evidence_claims_only:
            return self._snapshot_lineage_claims(lineage)
        return self._value_ref(lineage)

    def _serialise_node_snapshot_value(self, value):
        if self._is_template(value):
            return {"kind": "template"}

        if type(value).__name__ == "TIDEValueSnapshot":
            return {
                "kind": "snapshot",
                "value": self._serialise_tide_snapshot(value),
            }

        class_name = getattr(value, "__name__", type(value).__name__)
        if class_name in self._EFFECT_FIELDS:
            return {
                "kind": "effect",
                "value": self._serialise_effect_descriptor(value),
            }

        if hasattr(value, "store") and hasattr(value, "key"):
            return {
                "kind": "data_ref",
                "value": self._serialise_data_ref(value),
            }

        if value is None or isinstance(value, (str, int, float, bool)):
            return {"kind": "literal", "value": value}

        if isinstance(value, dict):
            if any(not isinstance(key, str) for key in value):
                return {"kind": "value", "value": self._value_ref(value)}
            return {
                "kind": "dict",
                "value": {
                    key: self._serialise_node_snapshot_value(item)
                    for key, item in value.items()
                },
            }

        if isinstance(value, list):
            return {
                "kind": "list",
                "value": [
                    self._serialise_node_snapshot_value(item)
                    for item in value
                ],
            }

        if isinstance(value, tuple):
            return {
                "kind": "tuple",
                "value": [
                    self._serialise_node_snapshot_value(item)
                    for item in value
                ],
            }

        return {"kind": "value", "value": self._value_ref(value)}

    def _serialise_node_snapshot(self, snapshot):
        if snapshot is None:
            return None
        if not isinstance(snapshot, dict):
            raise TypeError("ReadyAttempt node_snapshot must be a dict")
        return self._serialise_node_snapshot_value(snapshot)


    # ------------------------------------------------------------------

    # Inputs and dependency evidence

    # ------------------------------------------------------------------

    def _serialise_inputs(self, inputs):

        if inputs is None:
            raise ValueError(
                "Inputs evidence is missing; TIDE will not serialise it as empty"
            )

        items = getattr(inputs, "items", None)
        if not callable(items):
            raise TypeError("Inputs evidence must expose items()")

        entries = list(items())
        for name, input_record in entries:
            self._require_attrs(
                input_record,
                ("raw", "evaluated", "normalised", "resolved"),
                f"InputUseRecord {name}",
            )

        return [

            {

                "name": name,

                "raw": self._value_ref(getattr(input_record, "raw", None)),

                "evaluated": self._value_ref(

                    getattr(input_record, "evaluated", None)

                ),

                "normalised": self._value_ref(

                    getattr(input_record, "normalised", None)

                ),

                "resolved": self._value_ref(

                    getattr(input_record, "resolved", None)

                ),

            }

            for name, input_record in entries

        ]



    def _serialise_dependency(self, dependency, *, bind_input=True):

        self._require_attrs(
            dependency,
            (
                "type", "dep", "unscoped_reference", "associated_input",
                "input_index", "input_path", "snapshot_lineage",
                "observed_mutable", "observation_id",
            ),
            "DependencyUseRecord",
        )
        if dependency.type == "dynamic":
            self._require_attrs(
                dependency,
                ("expanded_to",),
                "Dynamic DependencyUseRecord",
            )
        elif dependency.type == "static":
            self._require_attrs(
                dependency,
                (
                    "resolved_value", "resolved_version",
                    "producer_source", "producer_actor",
                    "producer_exec_id",
                ),
                "Static DependencyUseRecord",
            )
        else:
            raise ValueError(
                f"Unsupported DependencyUseRecord type: {dependency.type}"
            )

        entry = {

            "type": getattr(dependency, "type", "static"),

            "dep": self._serialise_data_ref(getattr(dependency, "dep", None)),

            "reference": self._serialise_reference_identity(

                getattr(dependency, "unscoped_reference", None)

            ),

            "input_name": (
                self._input_name(dependency)
                if bind_input
                else None
            ),

            "input_index": getattr(dependency, "input_index", None),

            "input_path": self._value_ref(

                getattr(dependency, "input_path", None)

            ),

            "snapshot_lineage": self._serialise_snapshot_lineage(
                getattr(dependency, "snapshot_lineage", ())
            ),

            "observed_mutable": bool(

                getattr(dependency, "observed_mutable", False)

            ),

            "observation_id": getattr(dependency, "observation_id", None),

        }

        if entry["type"] == "dynamic":

            entry["expanded"] = [

                self._serialise_expanded_dependency(expanded)

                for expanded in getattr(dependency, "expanded_to", ()) or ()

            ]

            return entry



        entry.update(self._serialise_resolved_dependency(dependency))

        return entry



    def _serialise_expanded_dependency(self, dependency):

        self._require_attrs(
            dependency,
            (
                "type", "dep", "unscoped_reference", "associated_input",
                "input_index", "input_path", "snapshot_lineage",
                "observed_mutable", "observation_id", "expanded_from",
                "resolved_value", "resolved_version", "producer_source",
                "producer_actor", "producer_exec_id",
            ),
            "ExpandedDepRecord",
        )

        entry = {

            "type": getattr(dependency, "type", "static"),

            "dep": self._serialise_data_ref(getattr(dependency, "dep", None)),

            "reference": self._serialise_reference_identity(

                getattr(dependency, "unscoped_reference", None)

            ),

            "input_name": self._input_name(dependency),

            "input_index": getattr(dependency, "input_index", None),

            "input_path": self._value_ref(

                getattr(dependency, "input_path", None)

            ),

            "snapshot_lineage": self._serialise_snapshot_lineage(
                getattr(dependency, "snapshot_lineage", ())
            ),

            "observed_mutable": bool(

                getattr(dependency, "observed_mutable", False)

            ),

            "observation_id": getattr(dependency, "observation_id", None),

            "expanded_from": self._serialise_data_ref(

                getattr(dependency, "expanded_from", None)

            ),

        }

        entry.update(self._serialise_resolved_dependency(dependency))

        return entry



    def _serialise_resolved_dependency(self, dependency):

        return {

            "resolved_value": self._value_ref(

                getattr(dependency, "resolved_value", _MISSING_TIDE_RUN_VALUE)

            ),

            "version": getattr(dependency, "resolved_version", None),

            "producer_source": getattr(dependency, "producer_source", None),

            "producer_actor": self._serialise_actor(

                getattr(dependency, "producer_actor", None)

            ),

            "producer_exec_id": getattr(dependency, "producer_exec_id", None),

        }



    # ------------------------------------------------------------------

    # Writes, effects, emitted definitions

    # ------------------------------------------------------------------

    def _serialise_write(self, write):

        self._require_attrs(
            write,
            (
                "cause", "stored_result", "stored_version",
                "stored_target", "snapshot_lineage",
                "observed_mutable", "updated",
            ),
            "WriteRecord",
        )

        return {

            "cause": self._serialise_cause(getattr(write, "cause", None)),

            "stored_result": self._value_ref(

                getattr(write, "stored_result", _MISSING_TIDE_RUN_VALUE)

            ),

            "version": getattr(write, "stored_version", None),

            "reference": self._serialise_reference_identity(

                getattr(write, "stored_target", None)

            ),

            "snapshot_lineage": self._serialise_snapshot_lineage(
                getattr(write, "snapshot_lineage", ())
            ),

            "observed_mutable": bool(

                getattr(write, "observed_mutable", False)

            ),

            "updated": bool(getattr(write, "updated", False)),

        }



    def _serialise_effect_record(self, effect):

        self._require_attrs(
            effect,
            (
                "event_id", "effect", "targets", "output", "origin",
                "immediate", "target_existed", "affected",
            ),
            "EffectRecord",
        )

        affected = [

            self._serialise_affected_record(record)

            for record in getattr(effect, "affected", {}).values()

        ]

        return {

            "event_id": getattr(effect, "event_id", None),

            "effect": self._effect_value(getattr(effect, "effect", None)),

            "targets": self._value_ref(getattr(effect, "targets", None)),

            "output": self._value_ref(

                getattr(effect, "output", _MISSING_TIDE_RUN_VALUE)

            ),

            "origin": getattr(effect, "origin", None),

            "immediate": bool(getattr(effect, "immediate", False)),

            "target_existed": getattr(effect, "target_existed", None),

            "affected": affected,

        }



    def _serialise_affected_record(self, record):

        self._require_attrs(
            record,
            (
                "effect", "targets", "affectee", "affected_inputs",
                "affected_exec_id",
            ),
            "AffectedRecord",
        )

        return {

            "effect": self._effect_value(getattr(record, "effect", None)),

            "targets": self._value_ref(getattr(record, "targets", None)),

            "affectee": self._serialise_actor(

                getattr(record, "affectee", None)

            ),

            "affected_inputs": [

                dict(item)

                for item in getattr(record, "affected_inputs", ()) or ()

            ],

            "affected_exec_id": getattr(record, "affected_exec_id", None),

        }



    def _serialise_emitted_node(self, node):

        self._require_attrs(
            node,
            (
                "ocean_id", "runner_id", "pipeline_id", "node_id",
                "func_name", "inputs", "declared_deps",
                "declared_targets", "declared_effects", "origin",
                "content_id", "base_node", "base_node_content_id",
                "node_options", "emitted_by_exec_id",
            ),
            "EmittedNodeRecord",
        )

        return {

            "ocean_id": getattr(node, "ocean_id", None),

            "runner_id": getattr(node, "runner_id", None),

            "pipeline_id": getattr(node, "pipeline_id", None),

            "node_id": self._value_ref(getattr(node, "node_id", None)),

            "func_name": getattr(node, "func_name", None),

            "inputs": self._serialise_inputs(getattr(node, "inputs", None)),

            "declared_deps": [

                self._serialise_data_ref(reference)

                for reference in getattr(node, "declared_deps", ()) or ()

            ],

            "declared_targets": [

                self._serialise_data_ref(reference)

                for reference in getattr(node, "declared_targets", ()) or ()

            ],

            "declared_effects": [

                self._serialise_effect_descriptor(effect)

                for effect in getattr(node, "declared_effects", ()) or ()

            ],

            "origin": getattr(node, "origin", None),

            "content_id": self._value_ref(getattr(node, "content_id", None)),

            "base_node": self._value_ref(getattr(node, "base_node", None)),

            "base_node_content_id": self._value_ref(

                getattr(node, "base_node_content_id", None)

            ),

            "node_options": self._value_ref(

                getattr(node, "node_options", {})

            ),

            "emitted_by_exec_id": getattr(node, "emitted_by_exec_id", None),

        }



    # ------------------------------------------------------------------

    # Identity carriers

    # ------------------------------------------------------------------

    def _serialise_execution_reference(self, reference):

        if reference is None:

            return None

        self._require_attrs(
            reference,
            (
                "exec_id", "actor", "node_definition_id",
                "ambiguous_occurrence", "resolved_seq_id",
                "resolved_actor_occurrence",
            ),
            "ImmediateExecutionReference",
        )

        return {

            "exec_id": getattr(reference, "exec_id", None),

            "actor": self._serialise_actor(getattr(reference, "actor", None)),

            "node_definition_id": self._value_ref(

                getattr(reference, "node_definition_id", None)

            ),

            "ambiguous_occurrence": self._serialise_occurrence(

                getattr(reference, "ambiguous_occurrence", None)

            ),

            "resolved_seq_id": getattr(reference, "resolved_seq_id", None),

            "resolved_actor_occurrence": self._serialise_occurrence(

                getattr(reference, "resolved_actor_occurrence", None)

            ),

        }



    @staticmethod

    def _serialise_actor(actor):

        if actor is None:

            return None

        if not all(

            hasattr(actor, field)

            for field in ("source_ocean", "source_runner", "source_actor")

        ):

            raise TypeError(f"Unsupported TIDE Actor: {actor!r}")

        return {

            "ocean": actor.source_ocean,

            "runner": actor.source_runner,

            "name": actor.source_actor,

        }



    def _serialise_occurrence(self, occurrence):

        if occurrence is None:

            return None

        if not hasattr(occurrence, "actor") or not hasattr(

            occurrence, "occurrence_id"

        ):

            raise TypeError(f"Unsupported Actor occurrence: {occurrence!r}")

        value = {

            "actor": self._serialise_actor(occurrence.actor),

            "occurrence_id": occurrence.occurrence_id,

        }

        if hasattr(occurrence, "exec_id"):

            value["exec_id"] = occurrence.exec_id

            value["ambiguous"] = True

        else:

            value["ambiguous"] = False

        return value



    def _serialise_cause(self, cause):

        if cause is None:

            return None

        self._require_attrs(
            cause,
            ("event_id", "description"),
            "Cause",
        )

        return {

            "event_id": getattr(cause, "event_id", None),

            "description": getattr(cause, "description", None),

        }



    def _serialise_data_ref(self, reference):

        if reference is None:

            return None

        self._require_attrs(
            reference,
            (
                "store", "key", "count", "max", "literal",
                "prefix", "source_str",
            ),
            "DataRef",
        )

        if not hasattr(reference, "store") or not hasattr(reference, "key"):

            raise TypeError(f"Unsupported TIDE DataRef: {reference!r}")

        return {

            "store": reference.store,

            "key": self._value_ref(reference.key),

            "count": getattr(reference, "count", None),

            "max": getattr(reference, "max", None),

            "literal": bool(getattr(reference, "literal", False)),

            "prefix": bool(getattr(reference, "prefix", False)),

            "source_str": getattr(reference, "source_str", None),

        }



    def _serialise_reference_identity(self, reference):

        if reference is None:

            return None

        if hasattr(reference, "store") and hasattr(reference, "key"):

            return {"kind": "data_ref", "value": self._serialise_data_ref(reference)}

        return {"kind": "value", "value": self._value_ref(reference)}



    # ------------------------------------------------------------------

    # Value graph references

    # ------------------------------------------------------------------

    def _value_ref(self, value):

        if self._is_template(value):

            return {"available": False}

        value_hash = self.value_hasher(value)

        existing = self._values.get(value_hash)

        if existing is not None and existing is not value:

            # Same semantic hash may legitimately be represented by a distinct

            # but equal object.  Keep the first value; the hash is the identity.

            pass

        else:

            self._values.setdefault(value_hash, value)

        return {"available": True, "hash": value_hash}



    @staticmethod

    def _input_name(dependency):

        associated_input = getattr(dependency, "associated_input", None)

        if isinstance(associated_input, tuple) and associated_input:

            return associated_input[0]

        return None



    @staticmethod

    def _effect_value(effect):

        return getattr(effect, "value", effect)



    @staticmethod

    def _is_template(value):

        return (

            value is _MISSING_TIDE_RUN_VALUE

            or isinstance(value, __fallback__)

        )
