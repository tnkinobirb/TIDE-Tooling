class TideRunTemplate:
    """Fallback used only when the engine's template sentinel is unavailable."""
    pass


class TIDERunDeserialiser:
    def deserialise(self, run, values):
        self._validate_run(run)

        if not isinstance(values, dict):
            raise TypeError("TideRun values must be a dict")

        self._types = self._engine_types()

        return [
            self._deserialise_exec_record(record, values)
            for record in run["exec_records"]
        ]

    def _deserialise_exec_record(self, record, values):
        if not isinstance(record, dict):
            raise TypeError("TideRun exec record must be a dict")

        inputs = {}
        deps = [
            self._deserialise_dependency(entry, values, inputs)
            for entry in record.get("reads", ())
        ]
        writes = [
            self._deserialise_write(entry, values)
            for entry in record.get("writes", ())
        ]
        effects = self._deserialise_effects(
            record.get("effects", ()),
            values,
        )
        declared_effects = [
            self._deserialise_effect_descriptor(entry)
            for entry in record.get("declared_effects", ())
        ]
        runtime_effects = [
            self._deserialise_effect_descriptor(entry)
            for entry in record.get("runtime_effects", ())
        ]
        emitted_nodes = [
            self._deserialise_emitted_node(entry, values)
            for entry in record.get("emitted_nodes", ())
        ]
        output_value = self._resolve_available_value(
            record.get("output_value_hash"),
            record.get("output_value_available"),
            values,
        )

        return self._types["ExecRecord"](
            seq_id=record.get("seq_id"),
            exec_id=record.get("exec_id"),
            ocean_id=record.get("ocean_id"),
            runner_id=record.get("runner_id"),
            pipeline_id=record.get("pipeline_id"),
            node_id=record.get("node_id"),
            func=None,
            func_name=record.get("func_name"),
            inputs=inputs,
            deps=deps,
            effects=effects,
            declared_effects=declared_effects,
            declared_targets=self._deserialise_ref_list(
                record.get("declared_targets", ())
            ),
            runtime_effects=runtime_effects,
            runtime_targets=self._deserialise_ref_list(
                record.get("runtime_targets", ())
            ),
            declared_deps=self._deserialise_ref_list(
                record.get("declared_deps", ())
            ),
            unscoped_store_refs=list(
                record.get("unscoped_store_refs", ())
            ),
            status=record.get("status"),
            started_at=record.get("started_at"),
            finished_at=record.get("finished_at"),
            duration_ms=record.get("duration_ms"),
            error=record.get("error"),
            handled_raise=record.get("handled_raise", False),
            node_options=record.get("node_options") or {},
            output_value=output_value,
            resolve_output=record.get("resolve_output"),
            writes=writes,
            emitted_by=self._deserialise_emitted_by(record.get("emitted_by")),
            emitted_nodes=emitted_nodes,
            base_node_id=self._content_id_value(
                record.get("base_node_id")
            ),
            base_node_content_id=self._content_id_value(
                record.get("base_node_content_id")
            ),
            content_id=self._content_id_value(record.get("content_id")),
        )

    def _deserialise_dependency(self, record, values, inputs):
        if not isinstance(record, dict):
            raise TypeError("TideRun dependency must be a dict")

        input_name = record.get("input_name") or "unknown"
        input_record = inputs.setdefault(
            input_name,
            self._types["InputUseRecord"](
                input_name,
                None,
                None,
                None,
                None,
                [],
            ),
        )
        associated_input = (input_name, input_record)
        dependency_type = record.get("type", "static")

        if dependency_type == "dynamic":
            expanded = [
                self._deserialise_resolved_dependency(
                    entry,
                    values,
                    associated_input,
                )
                for entry in record.get("expanded", ())
            ]
            dependency = self._types["DependencyUseRecord"](
                associated_input=associated_input,
                dep=self._data_ref(record.get("reference")),
                type="dynamic",
                unscoped_ref=record.get("reference"),
                expanded_to=expanded,
                input_index=record.get("input_index"),
                input_path=tuple(record.get("input_path") or ()),
                snapshot_lineage=record.get("snapshot_lineage") or (),
                observed_mutable=record.get("observed_mutable", False),
            )
        elif dependency_type == "static":
            dependency = self._deserialise_resolved_dependency(
                record,
                values,
                associated_input,
            )
        else:
            raise ValueError(
                f"Unsupported TideRun dependency type: {dependency_type}"
            )

        input_record.dependencies.append(dependency)
        return dependency

    def _deserialise_resolved_dependency(
        self,
        record,
        values,
        associated_input,
    ):
        value = self._resolve_record_value(record, values)

        return self._types["DependencyUseRecord"](
            associated_input=associated_input,
            dep=self._data_ref(record.get("reference")),
            type="static",
            unscoped_ref=record.get("reference"),
            resolved_val=value,
            resolved_ver=record.get("version"),
            producer_source=record.get("producer_source"),
            producer_actor=self._deserialise_actor(record.get("producer_actor")),
            producer_exec_id=record.get("producer_exec_id"),
            input_index=record.get("input_index"),
            input_path=tuple(record.get("input_path") or ()),
            snapshot_lineage=record.get("snapshot_lineage") or (),
            observed_mutable=record.get("observed_mutable", False),
        )

    def _deserialise_write(self, record, values):
        if not isinstance(record, dict):
            raise TypeError("TideRun write must be a dict")

        return self._types["WriteRecord"](
            record.get("cause"),
            self._resolve_record_value(record, values),
            record.get("version"),
            record.get("reference"),
            snapshot_lineage=record.get("snapshot_lineage") or (),
            observed_mutable=record.get("observed_mutable", False),
        )

    def _deserialise_effects(self, effects, values):
        if not isinstance(effects, (list, tuple)):
            raise TypeError("TideRun effects must be a list")

        rebuilt = {}

        for record in effects:
            if not isinstance(record, dict):
                raise TypeError("TideRun effect must be a dict")

            targets = (record.get("reference"), record.get("version"))
            effect_kind = self._effect_kind(record.get("effect"))
            affected = {}

            for entry in record.get("affected", ()):
                actor = self._deserialise_actor(entry.get("actor"))

                if actor is None:
                    continue

                affected[actor] = self._types["AffectedRecord"](
                    effect_kind,
                    targets,
                    actor,
                    list(entry.get("inputs", ())),
                    entry.get("exec_id"),
                )

            output = self._resolve_optional_value_hash(
                record.get("output_value_hash"),
                values,
            )
            preserved_output_hash = record.get(
                "preserved_output_value_hash"
            )
            if preserved_output_hash is not None:
                preserved_output = self._resolve_value_hash(
                    preserved_output_hash,
                    values,
                )
            else:
                preserved_output = (
                    True
                    if record.get("preserved")
                    else self._template_value()
                )
            effect = self._types["EffectRecord"](
                effect=effect_kind,
                targets=targets,
                output=output,
                preserved_output=preserved_output,
                affected=affected,
            )
            rebuilt[(effect.effect, targets)] = effect

        return rebuilt

    def _deserialise_effect_descriptor(self, record):
        if not isinstance(record, dict):
            raise TypeError("TideRun effect descriptor must be a dict")

        form = record.get("form")
        class_name = record.get("class")
        value = self._descriptor_value(record.get("value"))

        if form == "type":
            return self._types.get(class_name, class_name)

        if form == "runtime":
            if isinstance(value, dict) and "effect" in value:
                value["effect"] = self._effect_kind(value["effect"])
            return value

        if form == "object" and class_name in {"WRITE", "DELETE"}:
            effect_type = self._types[class_name]
            targets = value.get("targets", [])
            preserve_output = value.get(
                "preserve_output",
                self._template_value(),
            )
            return effect_type(targets, preserve_output=preserve_output)

        if form == "object":
            return value

        if form == "value":
            return value

        raise ValueError(f"Unsupported TideRun effect descriptor form: {form}")

    def _descriptor_value(self, value):
        if isinstance(value, list):
            return [self._descriptor_value(item) for item in value]

        if not isinstance(value, dict):
            return value

        marker = value.get("$tide_effect_value")
        if marker == "template":
            return self._template_value()
        if marker == "reference":
            return self._data_ref(value.get("reference"))
        if marker == "repr":
            return {
                "type": value.get("type", "unknown"),
                "repr": value.get("repr", "<unavailable>"),
            }
        if marker is not None:
            raise ValueError(f"Unknown TideRun effect value marker: {marker}")

        return {
            key: self._descriptor_value(item)
            for key, item in value.items()
        }

    def _effect_kind(self, value):
        try:
            return self._types["Effects"](value)
        except (TypeError, ValueError):
            return value

    def _deserialise_actor(self, record):
        if record is None:
            return None

        if not isinstance(record, dict):
            raise TypeError("TideRun actor must be a dict")

        return self._types["Actor"](
            record.get("ocean"),
            record.get("runner"),
            record.get("name"),
        )

    def _deserialise_emitted_by(self, record):
        if record is None:
            return None

        if not isinstance(record, dict):
            raise TypeError("TideRun emitted_by must be a dict")

        actor = self._deserialise_actor(record.get("actor"))

        if actor is None:
            return None

        return actor, record.get("exec_id")

    def _deserialise_emitted_node(self, record, values):
        if not isinstance(record, dict):
            raise TypeError("TideRun emitted node must be a dict")

        inputs = {}
        deps = [
            self._deserialise_dependency(entry, values, inputs)
            for entry in record.get("reads", ())
        ]
        return self._types["EmittedNodeRecord"](
            ocean_id=record.get("ocean_id"),
            runner_id=record.get("runner_id"),
            pipeline_id=record.get("pipeline_id"),
            node_id=record.get("node_id"),
            func=None,
            func_name=record.get("func_name"),
            inputs=self._types["Inputs"](inputs),
            deps=deps,
            declared_targets=self._deserialise_ref_list(
                record.get("declared_targets", ())
            ),
            declared_effects=[
                self._deserialise_effect_descriptor(entry)
                for entry in record.get("declared_effects", ())
            ],
            emitted_by=self._deserialise_emitted_by(record.get("emitted_by")),
            content_id=self._content_id_value(record.get("content_id")),
            base_node=self._content_id_value(
                record.get("base_node_id")
            ),
            base_node_content_id=self._content_id_value(
                record.get("base_node_content_id")
            ),
        )

    def _deserialise_ref_list(self, references):
        if not isinstance(references, (list, tuple)):
            raise TypeError("TideRun reference collection must be a list")
        return [self._data_ref(reference) for reference in references]

    def _data_ref(self, reference):
        if not isinstance(reference, str):
            raise ValueError("TideRun dependency evidence requires a reference")

        return self._types["DataRef"].from_string(
            reference,
            strip_leading=False,
        )

    @staticmethod
    def _engine_types():
        try:
            from tide_tooling.tooling_core.engine.classes import DataRef
            from tide_tooling.tooling_core.engine.classes import DELETE, WRITE
            from tide_tooling.tooling_core.engine.events import Effects
            from tide_tooling.tooling_core.engine.tide_records import (
                Actor,
                AffectedRecord,
                DependencyUseRecord,
                EffectRecord,
                EmittedNodeRecord,
                ExecRecord,
                InputUseRecord,
                Inputs,
                WriteRecord,
            )
        except Exception as error:
            raise ImportError(
                "TideRun rebuild requires the TIDE engine record classes"
            ) from error

        return {
            "Actor": Actor,
            "AffectedRecord": AffectedRecord,
            "DataRef": DataRef,
            "DELETE": DELETE,
            "DependencyUseRecord": DependencyUseRecord,
            "EffectRecord": EffectRecord,
            "Effects": Effects,
            "EmittedNodeRecord": EmittedNodeRecord,
            "ExecRecord": ExecRecord,
            "InputUseRecord": InputUseRecord,
            "Inputs": Inputs,
            "WriteRecord": WriteRecord,
            "WRITE": WRITE,
        }

    @staticmethod
    def _resolve_value_hash(value_hash, values):
        if value_hash is None:
            raise ValueError(
                "TideRun evidence requires a bundled value hash"
            )

        if value_hash not in values:
            raise KeyError(
                f"TideRun value hash is missing from the bundle: {value_hash}"
            )

        return values[value_hash]

    def _resolve_optional_value_hash(self, value_hash, values):
        if value_hash is None:
            return self._template_value()
        return self._resolve_value_hash(value_hash, values)

    def _resolve_record_value(self, record, values):
        value_hash = record.get("value_hash")
        if value_hash is None and record.get("value_available") is False:
            return self._template_value()
        return self._resolve_value_hash(value_hash, values)

    def _resolve_available_value(
        self,
        value_hash,
        value_available,
        values,
    ):
        if value_available is False:
            return self._template_value()
        return self._resolve_value_hash(value_hash, values)

    @staticmethod
    def _content_id_value(value):
        # Older bundles encoded None as repr(None), i.e. the string "None".
        return None if value == "None" else value

    @staticmethod
    def _template_value():
        try:
            from tide_tooling.tooling_core.engine.classes import __TIDE_TEMPLATE_VALUE__
        except Exception:
            return TideRunTemplate()

        return __TIDE_TEMPLATE_VALUE__()

    @staticmethod
    def _validate_run(run):
        if not isinstance(run, dict):
            raise TypeError("TideRun must be a dict")

        if run.get("kind") != "tide_run":
            raise ValueError("Unsupported TideRun kind")

        if run.get("codec_version") != 2:
            raise ValueError("Unsupported TideRun codec version")

        if not isinstance(run.get("exec_records"), list):
            raise TypeError("TideRun exec_records must be a list")
