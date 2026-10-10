"""Restore native TIDE Trace evidence encoded by TIDERunSerialiser."""



from __future__ import annotations


from tide_tooling.tooling_core.engine.utils import is_tide_value_hash





class TideRunTemplate:

    """Fallback used only when the engine's unknown sentinel is unavailable."""





class TIDERunDeserialiser:

    KIND = "tide_run"

    CODEC_VERSION = 1

    def __init__(self, *, trusted=False):
        if not isinstance(trusted, bool):
            raise TypeError("trusted must be a bool")
        self.trusted = trusted

    @staticmethod
    def _require_value_hash(value, label):
        if not is_tide_value_hash(value):
            raise ValueError(f"{label} must be a valid TIDE value hash")
        return value

    RUN_FIELDS = frozenset({
        "kind", "codec_version", "trace", "records", "ready_attempts",
    })
    TRACE_FIELDS = frozenset({
        "name", "ocean_id", "runner", "pipelines", "bundle_context",
        "model_context", "host", "host_record",
        "trace_starting_event_id", "pipeline_starting_event_id",
        "tide_plan", "missing_referenced_events",
        "causal_order_violations", "ready_exec_ids", "blocked_exec_ids",
        "deadlock", "started_runner_ids", "causal_runner_ids",
        "ended_runner_ids", "evidence_violations",
    })
    EXEC_FIELDS = frozenset({
        "record_type", "seq_id", "event_seq_id", "exec_id", "ocean_id",
        "runner_id", "pipeline_id", "node_id", "func_name",
        "producer_occurrence", "inputs", "deps", "effects", "affected",
        "declared_deps", "declared_effects", "declared_targets",
        "runtime_effects", "runtime_targets", "unscoped_store_refs",
        "status", "started_at", "finished_at", "duration_ms", "error",
        "handled_raise", "mutated_during", "now_meta", "output_value",
        "writes", "emitted_nodes", "emitted_by_exec_id", "base_node_id",
        "base_node_content_id", "content_id", "node_options",
    })
    IMMEDIATE_FIELDS = frozenset({
        "record_type", "seq_id", "event_id", "event_seq_id", "exec_id",
        "execution_reference", "effects", "deps", "writes",
    })
    READY_FIELDS = frozenset({
        "event_id", "event_seq_id", "exec_id", "actor",
        "node_definition_id", "runner_id", "pipeline_id",
        "node_snapshot", "outcome",
    })
    INPUT_FIELDS = frozenset({
        "name", "raw", "evaluated", "normalised", "resolved",
    })
    DEP_COMMON_FIELDS = frozenset({
        "type", "dep", "reference", "input_name", "input_index",
        "input_path", "snapshot_lineage", "observed_mutable",
        "observation_id",
    })
    STATIC_DEP_FIELDS = DEP_COMMON_FIELDS | frozenset({
        "resolved_value", "version", "producer_source", "producer_actor",
        "producer_exec_id",
    })
    DYNAMIC_DEP_FIELDS = DEP_COMMON_FIELDS | frozenset({"expanded"})
    EXPANDED_DEP_FIELDS = STATIC_DEP_FIELDS | frozenset({"expanded_from"})
    WRITE_FIELDS = frozenset({
        "cause", "stored_result", "version", "reference",
        "snapshot_lineage", "observed_mutable", "updated",
    })
    EFFECT_FIELDS = frozenset({
        "event_id", "effect", "targets", "output", "origin", "immediate",
        "target_existed", "affected",
    })
    AFFECTED_FIELDS = frozenset({
        "effect", "targets", "affectee", "affected_inputs",
        "affected_exec_id",
    })
    EMITTED_NODE_FIELDS = frozenset({
        "ocean_id", "runner_id", "pipeline_id", "node_id", "func_name",
        "inputs", "declared_deps", "declared_targets", "declared_effects",
        "origin", "content_id", "base_node", "base_node_content_id",
        "node_options", "emitted_by_exec_id",
    })
    EXECUTION_REFERENCE_FIELDS = frozenset({
        "exec_id", "actor", "node_definition_id", "ambiguous_occurrence",
        "resolved_seq_id", "resolved_actor_occurrence",
    })
    ACTOR_FIELDS = frozenset({"ocean", "runner", "name"})
    OCCURRENCE_FIELDS = frozenset({"actor", "occurrence_id", "ambiguous"})
    AMBIGUOUS_OCCURRENCE_FIELDS = OCCURRENCE_FIELDS | frozenset({"exec_id"})
    CAUSE_FIELDS = frozenset({"event_id", "description"})
    DATA_REF_FIELDS = frozenset({
        "store", "key", "count", "max", "literal", "prefix", "source_str",
    })
    REFERENCE_IDENTITY_FIELDS = frozenset({"kind", "value"})

    @staticmethod
    def _require_exact_fields(record, expected, label):
        actual = set(record)
        if actual == set(expected):
            return

        missing = sorted(set(expected) - actual)
        unknown = sorted(actual - set(expected))
        parts = []
        if missing:
            parts.append(f"missing {missing}")
        if unknown:
            parts.append(f"unknown {unknown}")
        raise ValueError(
            f"{label} fields do not match TideRun v1: "
            + ", ".join(parts)
        )



    # ------------------------------------------------------------------

    # Public API

    # ------------------------------------------------------------------

    def deserialise(self, run, values, *, divergent_value_hashes=()):

        """Compatibility API: return only the chronological native records."""

        return self.deserialise_trace(
            run,
            values,
            divergent_value_hashes=divergent_value_hashes,
        )["records"]



    def deserialise_trace(
        self,
        run,
        values,
        *,
        divergent_value_hashes=(),
    ):

        self._validate_run(run)

        if not isinstance(values, dict):

            raise TypeError("TideRun values must be a dict")



        self._types = self._engine_types()

        self._values = values
        self._divergent_value_hashes = frozenset(divergent_value_hashes)
        for value_hash in self._divergent_value_hashes:
            self._require_value_hash(
                value_hash,
                "TideRun divergent value hash",
            )



        records = [

            self._deserialise_record(record)

            for record in run["records"]

        ]

        ready_attempts = [

            self._deserialise_ready_attempt(attempt)

            for attempt in run["ready_attempts"]

        ]

        trace = self._deserialise_trace_metadata(run["trace"])

        state_authorities = self._project_state_authorities(

            run["records"],

            records,

        )



        return {

            "records": records,

            "ready_attempts": ready_attempts,

            "trace": trace,

            "state_authorities": state_authorities,

        }



    # ------------------------------------------------------------------

    # Recorded State value identities

    # ------------------------------------------------------------------

    @staticmethod

    def _recorded_value_hash(reference):

        if not isinstance(reference, dict):

            raise TypeError("TideRun value reference must be a dict")



        available = reference["available"]



        if available is False:

            return None

        if available is not True:

            raise ValueError(

                "TideRun value reference requires availability"

            )



        return TIDERunDeserialiser._require_value_hash(
            reference["hash"],
            "TideRun value reference hash",
        )



    def _project_state_authorities(self, raw_records, records):

        """Project the value identity recorded for each State occurrence."""

        if len(raw_records) != len(records):

            raise ValueError(

                "TideRun record count changed during deserialisation"

            )



        authorities = {}

        observations = []



        def state_of(read):

            state = (

                read.unscoped_reference,

                read.resolved_version,

            )

            if state[0] is None or state[1] is None:

                raise ValueError(

                    "TideRun resolved State is missing reference or version"

                )

            return state



        def record_write(raw_write, write):

            value_hash = self._recorded_value_hash(

                raw_write["stored_result"]

            )

            if value_hash is None:

                return



            state = (

                write.stored_target,

                write.stored_version,

            )

            if state[0] is None or state[1] is None:

                raise ValueError(

                    "TideRun write is missing State reference or version"

                )



            existing = authorities.get(state)

            if existing is not None and existing != value_hash:

                raise ValueError(

                    "TideRun writes disagree for State: "

                    f"{state[0]}[v{state[1]}]"

                )

            authorities[state] = value_hash



        def record_read(raw_read, read):

            value_hash = self._recorded_value_hash(

                raw_read["resolved_value"]

            )

            if value_hash is None:

                return



            observations.append({

                "state": state_of(read),

                "observation_id": read.observation_id,

                "observation_hash": value_hash,

                "observed_mutable": bool(

                    read.observed_mutable

                ),

                "snapshot_lineage": list(

                    read.snapshot_lineage or ()

                ),

            })



        for raw_record, record in zip(

            raw_records,

            records,

            strict=True,

        ):

            # A resolved ImmediateEffectRecord is only a chronological window
            # into its containing ExecRecord. Its semantic evidence is already
            # represented by that execution and must not be projected twice.
            if (
                raw_record["record_type"] == "immediate_effect"
                and record.execution_reference.resolved
            ):
                continue

            raw_writes = raw_record["writes"]

            writes = record.writes

            if len(raw_writes) != len(writes):

                raise ValueError(

                    "TideRun write count changed during deserialisation"

                )



            for raw_write, write in zip(

                raw_writes,

                writes,

                strict=True,

            ):

                record_write(raw_write, write)



            raw_dependencies = raw_record["deps"]

            dependencies = record.deps

            if len(raw_dependencies) != len(dependencies):

                raise ValueError(

                    "TideRun dependency count changed during deserialisation"

                )



            for raw_dependency, dependency in zip(

                raw_dependencies,

                dependencies,

                strict=True,

            ):

                if raw_dependency["type"] == "dynamic":

                    raw_expanded = raw_dependency["expanded"]

                    expanded = dependency.expanded_to



                    if len(raw_expanded) != len(expanded):

                        raise ValueError(

                            "TideRun expanded dependency count changed "

                            "during deserialisation"

                        )



                    for raw_read, read in zip(

                        raw_expanded,

                        expanded,

                        strict=True,

                    ):

                        record_read(raw_read, read)

                    continue



                record_read(raw_dependency, dependency)



        if observations:

            from tide_tooling.tooling_core.engine.tide_evidence import (

                validate_state_observations,

            )



            authorities, _ = validate_state_observations(

                observations,

                authoritative_hash_by_state=authorities,

                label="TideRun",

            )



        return authorities



    # ------------------------------------------------------------------

    # Trace metadata

    # ------------------------------------------------------------------

    def _deserialise_trace_metadata(self, record):

        if not isinstance(record, dict):

            raise TypeError("TideRun trace metadata must be a dict")

        self._require_exact_fields(record, self.TRACE_FIELDS, "TideRun trace metadata")

        return {

            "name": record["name"],

            "ocean_id": record["ocean_id"],

            "runner": record["runner"],

            "pipelines": self._resolve_value_ref(record["pipelines"]),

            "bundle_context": self._resolve_value_ref(

                record["bundle_context"]

            ),

            "model_context": self._resolve_value_ref(

                record["model_context"]

            ),

            "host": self._resolve_value_ref(record["host"]),

            "host_record": self._resolve_value_ref(record["host_record"]),

            "trace_starting_event_id": record["trace_starting_event_id"],

            "pipeline_starting_event_id": record["pipeline_starting_event_id"],

            "tide_plan": self._resolve_value_ref(record["tide_plan"]),

            "missing_referenced_events": self._resolve_value_ref(

                record["missing_referenced_events"]

            ),

            "causal_order_violations": self._resolve_value_ref(

                record["causal_order_violations"]

            ),

            "ready_exec_ids": tuple(record["ready_exec_ids"]),

            "blocked_exec_ids": tuple(record["blocked_exec_ids"]),

            "deadlock": self._resolve_value_ref(record["deadlock"]),

            "started_runner_ids": tuple(record["started_runner_ids"]),

            "causal_runner_ids": tuple(record["causal_runner_ids"]),

            "ended_runner_ids": tuple(record["ended_runner_ids"]),

            "evidence_violations": self._resolve_value_ref(

                record["evidence_violations"]

            ),

        }



    # ------------------------------------------------------------------

    # Native records

    # ------------------------------------------------------------------

    def _deserialise_record(self, record):

        if not isinstance(record, dict):

            raise TypeError("TideRun record must be a dict")

        record_type = record["record_type"]

        if record_type == "exec":

            return self._deserialise_exec_record(record)

        if record_type == "immediate_effect":

            return self._deserialise_immediate_effect_record(record)

        raise ValueError(f"Unsupported TideRun record type: {record_type}")



    def _deserialise_exec_record(self, record):

        if not isinstance(record, dict):
            raise TypeError("TideRun exec record must be a dict")
        self._require_exact_fields(record, self.EXEC_FIELDS, "TideRun exec record")

        inputs = self._deserialise_inputs(record["inputs"])

        deps = [

            self._deserialise_dependency(entry, inputs)

            for entry in record["deps"]

        ]



        exec_record = self._types["ExecRecord"](

            seq_id=record["seq_id"],

            exec_id=record["exec_id"],

            ocean_id=record["ocean_id"],

            runner_id=record["runner_id"],

            pipeline_id=record["pipeline_id"],

            node_id=self._resolve_value_ref(record["node_id"]),

            func=None,

            func_name=record["func_name"],

            inputs=self._types["Inputs"](inputs),

            deps=deps,

            effects=[

                self._deserialise_effect_record(effect)

                for effect in record["effects"]

            ],

            declared_effects=[

                self._deserialise_effect_descriptor(effect)

                for effect in record["declared_effects"]

            ],

            declared_targets=[

                self._deserialise_data_ref(reference)

                for reference in record["declared_targets"]

            ],

            runtime_effects=[

                self._deserialise_effect_descriptor(effect)

                for effect in record["runtime_effects"]

            ],

            runtime_targets=[

                self._deserialise_data_ref(reference)

                for reference in record["runtime_targets"]

            ],

            declared_deps=[

                self._deserialise_data_ref(reference)

                for reference in record["declared_deps"]

            ],

            unscoped_store_refs=self._resolve_value_ref(

                record["unscoped_store_refs"]

            ),

            status=self._resolve_value_ref(record["status"]),

            started_at=self._resolve_value_ref(record["started_at"]),

            finished_at=self._resolve_value_ref(record["finished_at"]),

            duration_ms=self._resolve_value_ref(record["duration_ms"]),

            error=self._resolve_value_ref(record["error"]),

            node_options=self._resolve_value_ref(record["node_options"]),

            output_value=self._resolve_value_ref(
                record["output_value"],
                allow_divergence=True,
            ),

            writes=[

                self._deserialise_write(write)

                for write in record["writes"]

            ],

            emitted_nodes=[

                self._deserialise_emitted_node(node)

                for node in record["emitted_nodes"]

            ],

            base_node_id=self._resolve_value_ref(record["base_node_id"]),

            base_node_content_id=self._resolve_value_ref(

                record["base_node_content_id"]

            ),

            content_id=self._resolve_value_ref(record["content_id"]),

            handled_raise=record["handled_raise"],

            mutated_during=self._resolve_value_ref(record["mutated_during"]),

            now_meta=self._deserialise_now_meta(record["now_meta"]),

            emitted_by_exec_id=record["emitted_by_exec_id"],

        )



        if record["event_seq_id"] is not None:

            exec_record.event_seq_id = record["event_seq_id"]

        exec_record.producer_occurrence_id = self._deserialise_occurrence(

            record["producer_occurrence"]

        )

        exec_record.affected = self._resolve_value_ref(record["affected"])

        return exec_record



    def _deserialise_immediate_effect_record(self, record):

        if not isinstance(record, dict):
            raise TypeError("TideRun immediate record must be a dict")
        self._require_exact_fields(record, self.IMMEDIATE_FIELDS, "TideRun immediate record")

        immediate = self._types["ImmediateEffectRecord"](

            seq_id=record["seq_id"],

            event_id=record["event_id"],

            event_seq_id=record["event_seq_id"],

            exec_id=record["exec_id"],

            execution_reference=self._deserialise_execution_reference(

                record["execution_reference"]

            ),

        )

        immediate.effects = [

            self._deserialise_effect_record(effect)

            for effect in record["effects"]

        ]

        immediate.deps = [

            self._deserialise_dependency(dependency, {})

            for dependency in record["deps"]

        ]

        immediate.writes = [

            self._deserialise_write(write)

            for write in record["writes"]

        ]

        return immediate



    def _deserialise_ready_attempt(self, record):

        if not isinstance(record, dict):

            raise TypeError("TideRun ready attempt must be a dict")

        self._require_exact_fields(record, self.READY_FIELDS, "TideRun ready attempt")

        attempt = self._types["ReadyAttemptRecord"](

            event_id=record["event_id"],

            event_seq_id=record["event_seq_id"],

            exec_id=record["exec_id"],

            actor=self._deserialise_actor(record["actor"]),

            node_definition_id=self._resolve_value_ref(

                record["node_definition_id"]

            ),

            runner_id=record["runner_id"],

            pipeline_id=record["pipeline_id"],

            node_snapshot=self._deserialise_node_snapshot(
                record["node_snapshot"]
            ),

        )

        attempt.outcome = record["outcome"]

        return attempt




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

    NOW_META_FIELDS = frozenset({
        "trial_id", "activation_id", "evaluation_id", "function",
        "line", "output",
    })

    SNAPSHOT_FIELDS = frozenset({
        "value_hash", "capture_method", "type_name", "representation",
        "boundary", "exact", "observed_mutable", "trusted",
        "value_bundle", "value",
    })

    def _deserialise_effect_descriptor(self, record):
        if not isinstance(record, dict):
            raise TypeError("TideRun Effect descriptor must be a dict")

        form = record.get("form")

        if form == "type":
            self._require_exact_fields(
                record,
                {"form", "class"},
                "TideRun Effect type descriptor",
            )
            class_name = record["class"]
            if class_name not in self._EFFECT_FIELDS:
                raise ValueError(
                    f"Unsupported TideRun Effect class: {class_name}"
                )
            return self._types[class_name]

        if form in {"mapping", "value"}:
            self._require_exact_fields(
                record,
                {"form", "value"},
                "TideRun Effect value descriptor",
            )
            return self._resolve_value_ref(record["value"])

        if form != "object":
            raise ValueError(
                f"Unsupported TideRun Effect descriptor form: {form}"
            )

        self._require_exact_fields(
            record,
            {"form", "class", "payload"},
            "TideRun Effect object descriptor",
        )
        class_name = record["class"]
        if class_name not in self._EFFECT_FIELDS:
            raise ValueError(
                f"Unsupported TideRun Effect class: {class_name}"
            )

        payload = record["payload"]
        if not isinstance(payload, dict):
            raise TypeError("TideRun Effect payload must be a dict")
        expected = self._EFFECT_FIELDS[class_name]
        self._require_exact_fields(
            payload,
            expected,
            f"TideRun {class_name} Effect payload",
        )

        attributes = {}
        for name in expected:
            value = payload[name]
            if class_name == "RAISE" and name == "effects":
                if not isinstance(value, list):
                    raise TypeError("TideRun RAISE effects must be a list")
                attributes[name] = tuple(
                    self._deserialise_effect_descriptor(item)
                    for item in value
                )
            else:
                attributes[name] = self._resolve_value_ref(value)

        effect_type = self._types[class_name]
        effect = effect_type.__new__(effect_type)
        if isinstance(effect, Exception):
            Exception.__init__(effect)
        for name, value in attributes.items():
            setattr(effect, name, value)
        return effect

    def _deserialise_now_meta(self, record):
        if record is None:
            return None
        if not isinstance(record, dict):
            raise TypeError("TideRun NOWMeta must be a dict")
        self._require_exact_fields(
            record,
            self.NOW_META_FIELDS,
            "TideRun NOWMeta",
        )

        return self._types["NOWMeta"](
            trial_id=record["trial_id"],
            activation_id=record["activation_id"],
            evaluation_id=record["evaluation_id"],
            function=record["function"],
            line=record["line"],
            output=self._resolve_value_ref(
                record["output"],
                allow_divergence=True,
            ),
        )

    def _deserialise_tide_snapshot(self, record):
        if not isinstance(record, dict):
            raise TypeError("TideRun TIDEValueSnapshot must be a dict")
        self._require_exact_fields(
            record,
            self.SNAPSHOT_FIELDS,
            "TideRun TIDEValueSnapshot",
        )

        value_bundle = (
            None
            if record["value_bundle"] is None
            else self._resolve_value_ref(record["value_bundle"])
        )
        value = (
            None
            if record["value"] is None
            else self._resolve_value_ref(record["value"])
        )

        return self._types["TIDEValueSnapshot"](
            value_hash=self._require_value_hash(
                record["value_hash"],
                "TideRun snapshot value_hash",
            ),
            value_bundle=value_bundle,
            value=value,
            capture_method=record["capture_method"],
            type_name=record["type_name"],
            representation=record["representation"],
            boundary=record["boundary"],
            exact=record["exact"],
            observed_mutable=record["observed_mutable"],
            trusted=bool(record["trusted"] and self.trusted),
        )

    def _deserialise_node_snapshot_value(
        self,
        record,
        *,
        root=False,
        allow_divergence=False,
    ):
        if not isinstance(record, dict):
            raise TypeError("TideRun node snapshot value must be a dict")

        kind = record.get("kind")

        if kind == "template":
            self._require_exact_fields(
                record,
                {"kind"},
                "TideRun node snapshot template",
            )
            return self._template_value()

        self._require_exact_fields(
            record,
            {"kind", "value"},
            "TideRun node snapshot value",
        )
        value = record["value"]

        if kind == "snapshot":
            return self._deserialise_tide_snapshot(value)

        if kind == "effect":
            return self._deserialise_effect_descriptor(value)

        if kind == "data_ref":
            return self._deserialise_data_ref(value)

        if kind == "literal":
            if value is not None and not isinstance(
                value, (str, int, float, bool)
            ):
                raise TypeError(
                    "TideRun node snapshot literal is not primitive"
                )
            return value

        if kind == "dict":
            if not isinstance(value, dict):
                raise TypeError("TideRun node snapshot dict must be a dict")
            return {
                key: self._deserialise_node_snapshot_value(
                    item,
                    allow_divergence=(
                        allow_divergence or (root and key == "func")
                    ),
                )
                for key, item in value.items()
            }

        if kind in {"list", "tuple"}:
            if not isinstance(value, list):
                raise TypeError(
                    "TideRun node snapshot sequence must be a list"
                )
            restored = [
                self._deserialise_node_snapshot_value(
                    item,
                    allow_divergence=allow_divergence,
                )
                for item in value
            ]
            return restored if kind == "list" else tuple(restored)

        if kind == "value":
            return self._resolve_value_ref(
                value,
                allow_divergence=allow_divergence,
            )

        raise ValueError(
            f"Unsupported TideRun node snapshot value kind: {kind}"
        )

    def _deserialise_node_snapshot(self, record):
        if record is None:
            return None
        value = self._deserialise_node_snapshot_value(record, root=True)
        if not isinstance(value, dict):
            raise TypeError(
                "Restored ReadyAttempt node_snapshot must be a dict"
            )
        return value


    # ------------------------------------------------------------------

    # Inputs and dependencies

    # ------------------------------------------------------------------

    def _deserialise_inputs(self, entries):

        if not isinstance(entries, (list, tuple)):

            raise TypeError("TideRun inputs must be a list")

        inputs = {}

        for entry in entries:

            if not isinstance(entry, dict):

                raise TypeError("TideRun input entry must be a dict")

            self._require_exact_fields(entry, self.INPUT_FIELDS, "TideRun input entry")

            name = entry["name"]

            if not isinstance(name, str):

                raise TypeError("TideRun input name must be text")

            if name in inputs:
                raise ValueError(f"Duplicate TideRun input name: {name}")

            inputs[name] = self._types["InputUseRecord"](

                name=name,

                raw=self._resolve_value_ref(
                    entry["raw"], allow_divergence=True
                ),

                evaluated=self._resolve_value_ref(
                    entry["evaluated"], allow_divergence=True
                ),

                normalised=self._resolve_value_ref(
                    entry["normalised"], allow_divergence=True
                ),

                resolved=self._resolve_value_ref(
                    entry["resolved"], allow_divergence=True
                ),

                dependencies=[],

            )

        return inputs



    def _input_record(self, inputs, name):

        if name is None:

            return None

        try:

            return inputs[name]

        except KeyError as error:

            raise ValueError(

                f"TideRun dependency references missing input: {name}"

            ) from error



    def _deserialise_snapshot_lineage(self, reference):
        """Restore descriptor-only snapshot lineage under current load trust."""
        lineage = self._resolve_value_ref(reference)
        if lineage is None:
            return []
        if not isinstance(lineage, (list, tuple)):
            raise TypeError("TideRun snapshot lineage must be a list")

        restored = []
        outer_trusted = bool(getattr(self, "trusted", False))
        for entry in lineage:
            if not isinstance(entry, dict):
                raise TypeError(
                    "TideRun snapshot lineage entries must be dicts"
                )
            item = dict(entry)
            if "trusted" in item:
                item["trusted"] = bool(
                    item["trusted"] and outer_trusted
                )
            restored.append(item)
        return restored


    def _deserialise_dependency(self, record, inputs):

        if not isinstance(record, dict):

            raise TypeError("TideRun dependency must be a dict")



        dependency_type = record["type"]
        if dependency_type == "dynamic":
            expected_fields = self.DYNAMIC_DEP_FIELDS
        elif dependency_type == "static":
            expected_fields = self.STATIC_DEP_FIELDS
        else:
            raise ValueError(f"Unsupported TideRun dependency type: {dependency_type}")
        self._require_exact_fields(record, expected_fields, "TideRun dependency")

        input_name = record["input_name"]

        input_record = self._input_record(inputs, input_name)

        associated_input = (

            (input_name, input_record)

            if input_record is not None

            else None

        )

        dependency_type = record["type"]



        common = {

            "associated_input": associated_input,

            "dep": self._deserialise_data_ref(record["dep"]),

            "type": dependency_type,

            "unscoped_ref": self._deserialise_reference_identity(

                record["reference"]

            ),

            "input_index": record["input_index"],

            "input_path": self._resolve_value_ref(record["input_path"]),

            "snapshot_lineage": self._deserialise_snapshot_lineage(
                record["snapshot_lineage"]
            ),

            "observed_mutable": record["observed_mutable"],

            "observation_id": record["observation_id"],

        }



        if dependency_type == "dynamic":

            expanded = [

                self._deserialise_expanded_dependency(

                    entry,

                    associated_input,

                )

                for entry in record["expanded"]

            ]

            dependency = self._types["DependencyUseRecord"](

                expanded_to=expanded,

                **common,

            )

        elif dependency_type == "static":

            dependency = self._types["DependencyUseRecord"](

                resolved_val=self._resolve_value_ref(

                    record["resolved_value"],

                    allow_divergence=True,

                ),

                resolved_ver=record["version"],

                producer_source=record["producer_source"],

                producer_actor=self._deserialise_actor(

                    record["producer_actor"]

                ),

                producer_exec_id=record["producer_exec_id"],

                **common,

            )

        else:

            raise ValueError(

                f"Unsupported TideRun dependency type: {dependency_type}"

            )



        if input_record is not None:

            input_record.dependencies.append(dependency)

        return dependency



    def _deserialise_expanded_dependency(self, record, associated_input):

        if not isinstance(record, dict):

            raise TypeError("TideRun expanded dependency must be a dict")

        self._require_exact_fields(record, self.EXPANDED_DEP_FIELDS, "TideRun expanded dependency")

        return self._types["ExpandedDepRecord"](

            associated_input=associated_input,

            dep=self._deserialise_data_ref(record["dep"]),

            type=record["type"],

            unscoped_ref=self._deserialise_reference_identity(

                record["reference"]

            ),

            resolved_val=self._resolve_value_ref(
                record["resolved_value"],
                allow_divergence=True,
            ),

            resolved_ver=record["version"],

            producer_source=record["producer_source"],

            producer_actor=self._deserialise_actor(

                record["producer_actor"]

            ),

            producer_exec_id=record["producer_exec_id"],

            expanded_from=self._deserialise_data_ref(

                record["expanded_from"]

            ),

            input_index=record["input_index"],

            input_path=self._resolve_value_ref(record["input_path"]),

            snapshot_lineage=self._resolve_value_ref(

                record["snapshot_lineage"]

            ),

            observed_mutable=record["observed_mutable"],

            observation_id=record["observation_id"],

        )



    # ------------------------------------------------------------------

    # Writes, effects, emitted definitions

    # ------------------------------------------------------------------

    def _deserialise_write(self, record):

        if not isinstance(record, dict):

            raise TypeError("TideRun write must be a dict")

        self._require_exact_fields(record, self.WRITE_FIELDS, "TideRun write")

        return self._types["WriteRecord"](

            cause=self._deserialise_cause(record["cause"]),

            stored_result=self._resolve_value_ref(
                record["stored_result"],
                allow_divergence=True,
            ),

            stored_version=record["version"],

            stored_target=self._deserialise_reference_identity(

                record["reference"]

            ),

            snapshot_lineage=self._resolve_value_ref(

                record["snapshot_lineage"]

            ),

            observed_mutable=record["observed_mutable"],

            updated=record["updated"],

        )



    def _deserialise_effect_record(self, record):

        if not isinstance(record, dict):

            raise TypeError("TideRun effect must be a dict")

        self._require_exact_fields(record, self.EFFECT_FIELDS, "TideRun effect")

        effect_kind = self._effect_kind(record["effect"])

        affected = {}

        for entry in record["affected"]:

            affected_record = self._deserialise_affected_record(entry)

            if affected_record.affectee is None:
                raise ValueError("TideRun affected record is missing an affectee")

            affected_identity = (
                affected_record.affected_exec_id
                if affected_record.affected_exec_id is not None
                else affected_record.affectee
            )
            if affected_identity in affected:
                raise ValueError(
                    f"Duplicate TideRun affected identity: {affected_identity}"
                )
            affected[affected_identity] = affected_record

        return self._types["EffectRecord"](

            effect=effect_kind,

            targets=self._resolve_value_ref(
                record["targets"],
                allow_divergence=True,
            ),

            event_id=record["event_id"],

            origin=record["origin"],

            immediate=record["immediate"],

            output=self._resolve_value_ref(
                record["output"],
                allow_divergence=True,
            ),

            affected=affected,

            target_existed=record["target_existed"],

        )



    def _deserialise_affected_record(self, record):

        if not isinstance(record, dict):

            raise TypeError("TideRun affected record must be a dict")

        self._require_exact_fields(record, self.AFFECTED_FIELDS, "TideRun affected record")

        return self._types["AffectedRecord"](

            effect=self._effect_kind(record["effect"]),

            targets=self._resolve_value_ref(
                record["targets"],
                allow_divergence=True,
            ),

            affectee=self._deserialise_actor(record["affectee"]),

            affected_inputs=[

                dict(item)

                for item in record["affected_inputs"]

            ],

            affected_exec_id=record["affected_exec_id"],

        )



    def _deserialise_emitted_node(self, record):

        if not isinstance(record, dict):

            raise TypeError("TideRun emitted node must be a dict")

        self._require_exact_fields(record, self.EMITTED_NODE_FIELDS, "TideRun emitted node")

        inputs = self._deserialise_inputs(record["inputs"])

        return self._types["EmittedNodeRecord"](

            ocean_id=record["ocean_id"],

            runner_id=record["runner_id"],

            pipeline_id=record["pipeline_id"],

            node_id=self._resolve_value_ref(record["node_id"]),

            func=None,

            func_name=record["func_name"],

            inputs=self._types["Inputs"](inputs),

            declared_deps=[

                self._deserialise_data_ref(reference)

                for reference in record["declared_deps"]

            ],

            declared_targets=[

                self._deserialise_data_ref(reference)

                for reference in record["declared_targets"]

            ],

            declared_effects=[

                self._resolve_value_ref(effect)

                for effect in record["declared_effects"]

            ],

            origin=record["origin"],

            content_id=self._resolve_value_ref(record["content_id"]),

            base_node=self._resolve_value_ref(record["base_node"]),

            base_node_content_id=self._resolve_value_ref(

                record["base_node_content_id"]

            ),

            node_options=self._resolve_value_ref(record["node_options"]),

            emitted_by_exec_id=record["emitted_by_exec_id"],

        )



    # ------------------------------------------------------------------

    # Identity carriers

    # ------------------------------------------------------------------

    def _deserialise_execution_reference(self, record):

        if record is None:

            return None

        if not isinstance(record, dict):

            raise TypeError("TideRun execution reference must be a dict")

        self._require_exact_fields(record, self.EXECUTION_REFERENCE_FIELDS, "TideRun execution reference")

        return self._types["ImmediateExecutionReference"](

            exec_id=record["exec_id"],

            actor=self._deserialise_actor(record["actor"]),

            node_definition_id=self._resolve_value_ref(

                record["node_definition_id"]

            ),

            ambiguous_occurrence=self._deserialise_occurrence(

                record["ambiguous_occurrence"]

            ),

            resolved_seq_id=record["resolved_seq_id"],

            resolved_actor_occurrence=self._deserialise_occurrence(

                record["resolved_actor_occurrence"]

            ),

        )



    def _deserialise_actor(self, record):

        if record is None:

            return None

        if not isinstance(record, dict):

            raise TypeError("TideRun actor must be a dict")

        self._require_exact_fields(record, self.ACTOR_FIELDS, "TideRun actor")

        return self._types["Actor"](

            record["ocean"],

            record["runner"],

            record["name"],

        )



    def _deserialise_occurrence(self, record):

        if record is None:

            return None

        if not isinstance(record, dict):

            raise TypeError("TideRun Actor occurrence must be a dict")

        expected_fields = (
            self.AMBIGUOUS_OCCURRENCE_FIELDS
            if record["ambiguous"]
            else self.OCCURRENCE_FIELDS
        )
        self._require_exact_fields(record, expected_fields, "TideRun Actor occurrence")

        actor = self._deserialise_actor(record["actor"])

        occurrence_id = record["occurrence_id"]

        if record["ambiguous"]:

            return self._types["AmbiguousActorOccurrence"](

                actor,

                occurrence_id,

                record["exec_id"],

            )

        return self._types["ActorOccurrence"](actor, occurrence_id)



    def _deserialise_cause(self, record):

        if record is None:

            return None

        if not isinstance(record, dict):

            raise TypeError("TideRun Cause must be a dict")

        self._require_exact_fields(record, self.CAUSE_FIELDS, "TideRun Cause")

        return self._types["Cause"](

            event_id=record["event_id"],

            description=record["description"],

        )



    def _deserialise_data_ref(self, record):

        if record is None:

            return None

        if not isinstance(record, dict):

            raise TypeError("TideRun DataRef must be a dict")

        self._require_exact_fields(record, self.DATA_REF_FIELDS, "TideRun DataRef")

        return self._types["DataRef"](

            key=self._resolve_value_ref(record["key"]),

            store=record["store"],

            count=record["count"],

            max=record["max"],

            literal=record["literal"],

            prefix=record["prefix"],

            source_str=record["source_str"],

        )



    def _deserialise_reference_identity(self, record):

        if record is None:

            return None

        if not isinstance(record, dict):

            raise TypeError("TideRun reference identity must be a dict")

        self._require_exact_fields(record, self.REFERENCE_IDENTITY_FIELDS, "TideRun reference identity")

        kind = record["kind"]

        if kind == "data_ref":

            return self._deserialise_data_ref(record["value"])

        if kind == "value":

            return self._resolve_value_ref(record["value"])

        raise ValueError(f"Unsupported TideRun reference identity: {kind}")



    # ------------------------------------------------------------------

    # Values and engine types

    # ------------------------------------------------------------------

    def _resolve_value_ref(self, reference, *, allow_divergence=False):

        if reference is None:

            return None

        if not isinstance(reference, dict):

            raise TypeError("TideRun value reference must be a dict")

        available = reference.get("available")
        expected_fields = (
            {"available"}
            if available is False
            else {"available", "hash"}
        )
        self._require_exact_fields(reference, expected_fields, "TideRun value reference")

        if reference["available"] is False:

            return self._template_value()

        if reference["available"] is not True:

            raise ValueError("TideRun value reference requires availability")

        value_hash = self._require_value_hash(
            reference["hash"],
            "TideRun value reference hash",
        )

        if value_hash not in self._values:

            raise KeyError(

                f"TideRun value hash is missing from bundle: {value_hash}"

            )

        if (
            value_hash in self._divergent_value_hashes
            and not allow_divergence
        ):
            raise ValueError(
                "TideRun semantic value backing does not authenticate "
                f"its recorded claim: {value_hash}"
            )

        value = self._values[value_hash]
        from tide_tooling.tooling_core.engine.utils import (
            TIDEValueSnapshot,
            tide_value_hasher,
        )

        try:
            backing_hash = tide_value_hasher(value)
        except Exception:
            backing_hash = None
        if backing_hash != value_hash:
            if allow_divergence:
                return TIDEValueSnapshot(
                    value_hash=value_hash,
                    capture_method="hash",
                    type_name=getattr(value, "type_name", type(value).__name__),
                    representation=getattr(
                        value,
                        "representation",
                        repr(value)[:1024],
                    ),
                    boundary="bundle.runtime_value",
                    exact=False,
                    observed_mutable=bool(
                        getattr(value, "observed_mutable", False)
                    ),
                    trusted=False,
                )

        return value



    def _effect_kind(self, value):

        try:

            return self._types["Effects"](value)

        except (TypeError, ValueError):

            return value



    @staticmethod

    def _engine_types():

        try:

            from tide_tooling.tooling_core.engine.classes import (
                DataRef,
                DELETE,
                EMIT,
                END,
                RAISE,
                READ_EFFECT,
                RUN,
                WRITE,
                __fallback__,
            )
            from tide_tooling.TIDEAdapters import NOWMeta
            from tide_tooling.tooling_core.engine.utils import TIDEValueSnapshot

            from tide_tooling.tooling_core.engine.events import Cause, Effects

            from tide_tooling.tooling_core.engine.tide_records import (

                Actor,

                ActorOccurrence,

                AffectedRecord,

                AmbiguousActorOccurrence,

                DependencyUseRecord,

                EffectRecord,

                EmittedNodeRecord,

                ExecRecord,

                ExpandedDepRecord,

                ImmediateEffectRecord,

                ImmediateExecutionReference,

                InputUseRecord,

                Inputs,

                ReadyAttemptRecord,

                WriteRecord,

            )

        except Exception as error:

            raise ImportError(

                "TideRun rebuild requires the TIDE engine record classes"

            ) from error



        return {

            "Actor": Actor,

            "ActorOccurrence": ActorOccurrence,

            "AffectedRecord": AffectedRecord,

            "AmbiguousActorOccurrence": AmbiguousActorOccurrence,

            "Cause": Cause,

            "DataRef": DataRef,
            "DELETE": DELETE,
            "EMIT": EMIT,
            "END": END,
            "NOWMeta": NOWMeta,
            "RAISE": RAISE,
            "READ_EFFECT": READ_EFFECT,
            "RUN": RUN,
            "TIDEValueSnapshot": TIDEValueSnapshot,
            "WRITE": WRITE,

            "DependencyUseRecord": DependencyUseRecord,

            "EffectRecord": EffectRecord,

            "Effects": Effects,

            "EmittedNodeRecord": EmittedNodeRecord,

            "ExecRecord": ExecRecord,

            "ExpandedDepRecord": ExpandedDepRecord,

            "ImmediateEffectRecord": ImmediateEffectRecord,

            "ImmediateExecutionReference": ImmediateExecutionReference,

            "InputUseRecord": InputUseRecord,

            "Inputs": Inputs,

            "ReadyAttemptRecord": ReadyAttemptRecord,

            "WriteRecord": WriteRecord,

            "__fallback__": __fallback__,

        }



    def _template_value(self):

        fallback = self._types.get("__fallback__")

        return fallback() if fallback is not None else TideRunTemplate()



    @classmethod

    def _validate_run(cls, run):

        if not isinstance(run, dict):

            raise TypeError("TideRun must be a dict")

        cls._require_exact_fields(run, cls.RUN_FIELDS, "TideRun")

        if run["kind"] != cls.KIND:

            raise ValueError("Unsupported TideRun kind")

        if run["codec_version"] != cls.CODEC_VERSION:

            raise ValueError("Unsupported TideRun codec version")

        if not isinstance(run["trace"], dict):

            raise TypeError("TideRun trace metadata must be a dict")

        if not isinstance(run["records"], list):

            raise TypeError("TideRun records must be a list")

        if not isinstance(run["ready_attempts"], list):

            raise TypeError("TideRun ready_attempts must be a list")
