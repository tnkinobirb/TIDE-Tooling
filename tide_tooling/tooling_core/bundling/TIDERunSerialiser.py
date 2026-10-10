try:
    from tide_tooling.tooling_core.engine.utils import tide_value_hasher
except ImportError:
    tide_value_hasher = None


class TIDERunSerialiser:
    def __init__(self, value_hasher=None):
        self.value_hasher = value_hasher or tide_value_hasher

        if self.value_hasher is None:
            raise ImportError(
                "TIDERunSerialiser requires "
                "tide_tooling.tooling_core.engine.utils.tide_value_hasher"
            )

    def serialise(self, trace):
        exec_log = getattr(trace, "exec_log", ())

        if not isinstance(exec_log, (list, tuple)):
            raise TypeError("Trace exec_log must be a list or tuple")

        return {
            "kind": "tide_run",
            "trace": self._trace_metadata(trace),
            "exec_records": [
                self._serialise_exec_record(record)
                for record in exec_log
            ],
        }

    def _serialise_exec_record(self, record):
        output_value = getattr(record, "output_value", None)
        output_available = (
            hasattr(record, "output_value")
            and not self._is_template(output_value)
        )
        return {
            "seq_id": getattr(record, "seq_id", None),
            "exec_id": getattr(record, "exec_id", None),
            "ocean_id": getattr(record, "source_ocean_id", None),
            "runner_id": getattr(record, "runner_id", None),
            "pipeline_id": getattr(record, "pipeline_id", None),
            "node_id": getattr(record, "node_id", None),
            "func_name": getattr(record, "func_name", None),
            "content_id": self._content_id_value(
                getattr(record, "content_id", None)
            ),
            "status": getattr(record, "status", None),
            "started_at": getattr(record, "started_at", None),
            "finished_at": getattr(record, "finished_at", None),
            "duration_ms": getattr(record, "duration_ms", None),
            "error": self._error_value(getattr(record, "error", None)),
            "handled_raise": bool(
                getattr(record, "handled_raise", False)
            ),
            "node_options": self._dict_value(
                getattr(record, "node_options", None)
            ),
            "output_value_hash": (
                self._value_hash(output_value)
                if output_available
                else None
            ),
            "output_value_available": output_available,
            "resolve_output": getattr(record, "resolve_output", None),
            "declared_deps": [
                self._reference_value(reference)
                for reference in getattr(record, "declared_deps", ()) or ()
            ],
            "declared_targets": [
                self._reference_value(reference)
                for reference in getattr(record, "declared_targets", ()) or ()
            ],
            "runtime_targets": [
                self._reference_value(reference)
                for reference in getattr(record, "runtime_targets", ()) or ()
            ],
            "unscoped_store_refs": list(
                getattr(record, "unscoped_store_refs", ()) or ()
            ),
            "base_node_id": self._content_id_value(
                getattr(record, "base_node_id", None)
            ),
            "base_node_content_id": self._content_id_value(
                getattr(record, "base_node_content_id", None)
            ),
            "reads": [
                self._serialise_dependency(dependency)
                for dependency in getattr(record, "deps", ()) or ()
            ],
            "writes": [
                self._serialise_write(write)
                for write in getattr(record, "writes", ()) or ()
            ],
            "effects": [
                self._serialise_effect(effect)
                for effect in self._effect_values(
                    getattr(record, "effects", None)
                )
            ],
            "declared_effects": [
                self._serialise_effect_descriptor(effect)
                for effect in getattr(record, "declared_effects", ()) or ()
            ],
            "runtime_effects": [
                self._serialise_effect_descriptor(effect)
                for effect in getattr(record, "runtime_effects", ()) or ()
            ],
            "emitted_nodes": [
                self._serialise_emitted_node(node)
                for node in getattr(record, "emitted_nodes", ()) or ()
            ],
        }

    def _serialise_dependency(self, dependency):
        entry = self._serialise_resolved_dependency(dependency)
        entry["type"] = getattr(dependency, "type", "static")

        if entry["type"] == "dynamic":
            entry["expanded"] = [
                self._serialise_resolved_dependency(expanded)
                for expanded in getattr(dependency, "expanded_to", ()) or ()
            ]

        return entry

    def _serialise_resolved_dependency(self, dependency):
        resolved_value = getattr(dependency, "resolved_value", None)
        return {
            "reference": self._reference_value(
                getattr(dependency, "unscoped_reference", None)
            ),
            "version": getattr(dependency, "resolved_version", None),
            "value_hash": self._value_hash(resolved_value),
            "value_available": not self._is_template(resolved_value),
            "producer_source": getattr(dependency, "producer_source", None),
            "producer_actor": self._actor_value(
                getattr(dependency, "producer_actor", None)
            ),
            "producer_exec_id": getattr(
                dependency,
                "producer_exec_id",
                None,
            ),
            "input_name": self._input_name(dependency),
            "input_index": getattr(dependency, "input_index", None),
            "input_path": self._descriptor_value(
                list(getattr(dependency, "input_path", ()) or ())
            ),
            "snapshot_lineage": self._descriptor_value(
                list(
                    getattr(
                        dependency,
                        "snapshot_lineage",
                        (),
                    ) or ()
                )
            ),
            "observed_mutable": bool(
                getattr(dependency, "observed_mutable", False)
            ),
        }

    def _serialise_write(self, write):
        stored_result = getattr(write, "stored_result", None)
        return {
            "reference": self._reference_value(
                getattr(write, "stored_target", None)
            ),
            "version": getattr(write, "stored_version", None),
            "value_hash": self._value_hash(stored_result),
            "value_available": not self._is_template(stored_result),
            "cause": getattr(write, "cause", None),
            "snapshot_lineage": self._descriptor_value(
                list(getattr(write, "snapshot_lineage", ()) or ())
            ),
            "observed_mutable": bool(
                getattr(write, "observed_mutable", False)
            ),
        }

    def _serialise_effect(self, effect):
        targets = getattr(effect, "targets", None)
        target_ref, target_version = self._effect_target(targets)
        affected = []

        for actor, record in getattr(effect, "affected", {}).items():
            affected.append(
                {
                    "actor": self._actor_value(actor),
                    "exec_id": getattr(record, "affected_exec_id", None),
                    "inputs": self._affected_inputs(record),
                }
            )

        output = getattr(effect, "output", None)
        effect_value = self._effect_value(getattr(effect, "effect", None))
        entry = {
            "effect": effect_value,
            "reference": target_ref,
            "version": target_version,
            "output_value_hash": (
                self._value_hash(output)
                if not self._is_template(output)
                else None
            ),
            "output_value_available": not self._is_template(output),
            "origin": getattr(effect, "origin", None),
            "affected": affected,
        }
        if effect_value in {"read", "DELETE"}:
            target_existed = getattr(effect, "target_existed", None)
            if target_existed is None:
                raise ValueError(
                    f"{effect_value} effect is missing target existence evidence"
                )
            entry["target_existed"] = bool(target_existed)
        return entry

    def _serialise_effect_descriptor(self, effect):
        if isinstance(effect, type):
            return {
                "form": "type",
                "class": effect.__name__,
            }

        if isinstance(effect, dict):
            return {
                "form": "runtime",
                "value": self._descriptor_value(effect),
            }

        if hasattr(effect, "to_dict"):
            return {
                "form": "object",
                "class": type(effect).__name__,
                "value": self._descriptor_value(effect.to_dict()),
            }

        return {
            "form": "value",
            "value": self._descriptor_value(effect),
        }

    def _descriptor_value(self, value):
        if self._is_template(value):
            return {"$tide_effect_value": "template"}

        if hasattr(value, "store") and hasattr(value, "key"):
            return {
                "$tide_effect_value": "reference",
                "reference": self._reference_value(value),
            }

        if isinstance(value, dict):
            return {
                str(key): self._descriptor_value(item)
                for key, item in value.items()
            }

        if isinstance(value, (list, tuple, set)):
            return [self._descriptor_value(item) for item in value]

        enum_value = getattr(value, "value", None)
        if isinstance(enum_value, (str, int, float, bool)):
            return enum_value

        if value is None or isinstance(value, (str, int, float, bool)):
            return value

        return {
            "$tide_effect_value": "repr",
            "type": type(value).__name__,
            "repr": repr(value)[:1024],
        }

    def _serialise_emitted_node(self, node):
        return {
            "ocean_id": getattr(node, "ocean_id", None),
            "runner_id": getattr(node, "runner_id", None),
            "pipeline_id": getattr(node, "pipeline_id", None),
            "node_id": getattr(node, "node_id", None),
            "func_name": getattr(node, "func_name", None),
            "origin": getattr(node, "origin", None),
            "content_id": self._content_id_value(
                getattr(node, "content_id", None)
            ),
            "reads": [
                self._serialise_dependency(dependency)
                for dependency in getattr(node, "deps", ()) or ()
            ],
            "declared_targets": [
                self._reference_value(reference)
                for reference in getattr(node, "declared_targets", ()) or ()
            ],
            "declared_effects": [
                self._serialise_effect_descriptor(effect)
                for effect in getattr(node, "declared_effects", ()) or ()
            ],
            "base_node_id": self._content_id_value(
                getattr(node, "base_node", None)
            ),
            "base_node_content_id": self._content_id_value(
                getattr(node, "base_node_content_id", None)
            ),
        }

    @staticmethod
    def _trace_metadata(trace):
        return {
            name: value
            for name, value in {
                "name": getattr(trace, "name", None),
                "ocean_id": getattr(trace, "ocean_id", None),
                "run": getattr(trace, "run", None),
                "pipeline": getattr(trace, "pipeline", None),
                "sources": (
                    [dict(source) for source in trace.execution_sources]
                    if getattr(trace, "execution_sources", None)
                    else None
                ),
                "model_context": (
                    dict(trace.model_context)
                    if getattr(trace, "model_context", None)
                    else None
                ),
                "host": getattr(trace, "host", None),
                "host_record_value_hash": (
                    tide_value_hasher(trace.host_record)
                    if getattr(trace, "host_record", None) is not None
                    else None
                ),
            }.items()
            if value is not None
        }

    @staticmethod
    def _dict_value(value):
        return dict(value) if isinstance(value, dict) else {}

    @staticmethod
    def _error_value(value):
        return None if value is None else repr(value)

    @staticmethod
    def _content_id_value(value):
        return None if value is None else repr(value)

    @staticmethod
    def _reference_value(value):
        if isinstance(value, str):
            return value

        if hasattr(value, "store") and hasattr(value, "key"):
            to_string = getattr(type(value), "to_string", None)
            if callable(to_string):
                return to_string(value, strip_leading=True)
            return f"{value.store}:{value.key}"

        if value is None:
            return None

        raise TypeError(f"Unsupported Tide reference: {value!r}")

    @staticmethod
    def _actor_value(actor):
        if actor is None:
            return None

        if all(
            hasattr(actor, name)
            for name in ["source_ocean", "source_runner", "source_actor"]
        ):
            return {
                "ocean": actor.source_ocean,
                "runner": actor.source_runner,
                "name": actor.source_actor,
            }

        raise TypeError(f"Unsupported Tide actor: {actor!r}")

    @staticmethod
    def _input_name(dependency):
        associated_input = getattr(dependency, "associated_input", None)

        if isinstance(associated_input, tuple) and associated_input:
            return associated_input[0]

        return None

    @staticmethod
    def _effect_values(effects):
        if effects is None:
            return ()
        if not isinstance(effects, (list, tuple)):
            raise TypeError("ExecRecord.effects must be an ordered list")
        return effects

    @staticmethod
    def _effect_target(targets):
        if not isinstance(targets, (list, tuple)) or len(targets) != 2:
            raise TypeError("Tide effect targets must be a ref/version pair")

        return targets[0], targets[1]

    @staticmethod
    def _effect_value(effect):
        return getattr(effect, "value", effect)

    @staticmethod
    def _affected_inputs(record):
        return [dict(item) for item in record.affected_inputs or ()]

    def _value_hash(self, value):
        if self._is_template(value):
            return None

        return self.value_hasher(value)

    @staticmethod
    def _is_template(value):
        return value.__class__.__name__ == "__TIDE_TEMPLATE_VALUE__"
