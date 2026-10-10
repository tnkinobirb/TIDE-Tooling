"""The single boundary for interpreting and projecting TIDE Models."""

from collections import defaultdict
import copy
import json
from os import PathLike
from pathlib import Path

from tide_tooling.tooling_core.engine.TIDECanonicaliser import TIDECanonicaliser
from tide_tooling.tooling_core.engine.classes import DataRef, __fallback__
from tide_tooling.tooling_core.engine.events import Cause
from tide_tooling.tooling_core.engine.tide_records import (
    Actor,
    ActorOccurrence,
    DependencyUseRecord,
    EmittedNodeRecord,
    ExecRecord,
    InputUseRecord,
    Inputs,
    WriteRecord,
)
from tide_tooling.tooling_core.engine.utils import (
    tide_snapshot_value,
)
from tide_tooling.tooling_core.engine.tide_rendering import (
    SHOW_BOLD, SHOW_CYAN, SHOW_DIM, SHOW_GREEN, SHOW_RED, SHOW_RESET,
    SHOW_SECTION_WIDTH, SHOW_WHITE, SHOW_YELLOW, _show_options, _show_render,
)


class TIDEModelValidityReport(dict):
    """Structural validity, semantic validity, and judgments for one Model."""

    structurally_valid = property(lambda self: self["structurally_valid"])
    semantically_valid = property(lambda self: self["semantically_valid"])

    @property
    def direct_handoffs(self):
        judgments = self["judgments"]
        if judgments is None:
            return None
        return tuple(
            (item["producer_exec_id"], item["state_id"], item["consumer_exec_id"])
            for item in judgments["direct_handoffs"]
        )

    @property
    def reachability(self):
        judgments = self["judgments"]
        if judgments is None:
            return None
        return tuple(
            (item["source_exec_id"], item["target_exec_id"])
            for item in judgments["reachability"]
        )

    def to_dict(self):
        return copy.deepcopy(dict(self))

    def show(self, color=True, simple=False, colour=None):
        color, simple = _show_options(color=color, simple=simple, colour=colour)
        valid = self.get("semantically_valid")
        status = "VALID" if valid else "INVALID" if valid is False else "UNASSESSABLE"
        status_color = SHOW_GREEN if valid else SHOW_RED
        counts = self.get("counts", {})
        host = self.get("host") or "unspecified"

        def display_id(identity):
            return json.dumps(identity) if isinstance(identity, str) else str(identity)

        lines = [
            "",
            f"{SHOW_CYAN}{'=' * SHOW_SECTION_WIDTH}{SHOW_RESET}",
            f"{SHOW_CYAN}{SHOW_BOLD}TIDE MODEL{SHOW_RESET} | "
            f"{status_color}{status}{SHOW_RESET} | "
            f"{counts.get('executions', 0)} executions | "
            f"{counts.get('states', 0)} states",
            f"{SHOW_DIM}Host: {host}{SHOW_RESET}",
            f"{SHOW_CYAN}{'=' * SHOW_SECTION_WIDTH}{SHOW_RESET}",
        ]
        if not simple:
            lines.append(f"{SHOW_DIM}VALIDITY:{SHOW_RESET}")
            for check in ("structurally_valid", "unique_creation", "acyclic_ancestry"):
                passed = self.get(check)
                result = (
                    f"{SHOW_GREEN}pass{SHOW_RESET}" if passed
                    else f"{SHOW_YELLOW}not established{SHOW_RESET}"
                )
                lines.append(f"  {check.replace('_', ' ')}: {result}")
            direct_handoffs = self.direct_handoffs or ()
            reachability = self.reachability or ()
            lines.extend([
                f"{SHOW_DIM}JUDGMENTS:{SHOW_RESET}",
                f"  Direct handoffs: {len(direct_handoffs)}",
            ])
            if direct_handoffs:
                for producer, state, consumer in direct_handoffs:
                    lines.append(
                        f"    {SHOW_WHITE}{display_id(producer)}{SHOW_RESET} "
                        f"--{SHOW_CYAN}{display_id(state)}{SHOW_RESET}--> "
                        f"{SHOW_WHITE}{display_id(consumer)}{SHOW_RESET}"
                    )
            else:
                lines.append(f"    {SHOW_DIM}none{SHOW_RESET}")
            lines.append(f"  Reachability: {len(reachability)}")
            if reachability:
                for source, target in reachability:
                    lines.append(
                        f"    {SHOW_WHITE}{display_id(source)}{SHOW_RESET} "
                        f"\u2192 {SHOW_WHITE}{display_id(target)}{SHOW_RESET}"
                    )
            else:
                lines.append(f"    {SHOW_DIM}none{SHOW_RESET}")
            lines.append(f"{SHOW_DIM}ERRORS:{SHOW_RESET}")
            errors = self.get("errors", ())
            if errors:
                lines.extend(f"  {SHOW_RED}{error}{SHOW_RESET}" for error in errors)
            else:
                lines.append(f"  {SHOW_DIM}none{SHOW_RESET}")
        return _show_render(lines, color=color)


class TIDEModel:
    """Own the complete contract between Model dictionaries and Traces."""

    @staticmethod
    def identifier(value, location):
        if not (type(value) is int or isinstance(value, str) and value.strip()):
            raise ValueError(f"{location} must be a non-empty string or integer")
        return value

    @staticmethod
    def _list(value, location):
        if not isinstance(value, (list, tuple, set, frozenset)):
            raise TypeError(f"{location} must be a list, tuple, set or frozenset")
        return list(value)

    @staticmethod
    def identity_sort_key(value):
        return type(value) is str, value

    @staticmethod
    def _incidence_pair(entry, relation, index):
        if isinstance(entry, dict):
            explicit = "exec_id" in entry or "state_id" in entry
            compact = "e" in entry or "s" in entry
            if explicit and compact:
                raise TypeError(
                    f"{relation}[{index}] cannot mix compact {{e, s}} and explicit "
                    "{exec_id, state_id} incidence keys"
                )
            if "exec_id" in entry and "state_id" in entry:
                return entry["exec_id"], entry["state_id"]
            if "e" in entry and "s" in entry:
                return entry["e"], entry["s"]
            raise TypeError(
                f"{relation}[{index}] must contain either "
                "{e, s} or {exec_id, state_id}"
            )
        if isinstance(entry, (list, tuple)) and len(entry) == 2:
            return entry[0], entry[1]
        raise TypeError(
            f"{relation}[{index}] must be "
            "{e, s}, {exec_id, state_id}, or [e, s]"
        )

    @classmethod
    def _normalise_incidence(cls, entries, relation):
        incidence = set()
        for index, entry in enumerate(cls._list(entries, relation)):
            execution, state = cls._incidence_pair(entry, relation, index)
            incidence.add((
                cls.identifier(execution, f"{relation}[{index}].execution"),
                cls.identifier(state, f"{relation}[{index}].state"),
            ))
        return [
            {"exec_id": execution, "state_id": state}
            for execution, state in sorted(
                incidence,
                key=lambda pair: (
                    cls.identity_sort_key(pair[0]), cls.identity_sort_key(pair[1])
                ),
            )
        ]

    @classmethod
    def normalise_carrier(cls, entries, field):
        return sorted(
            {
                cls.identifier(entry, f"{field}[{index}]")
                for index, entry in enumerate(cls._list(entries, field))
            },
            key=cls.identity_sort_key,
        )

    @staticmethod
    def _load(model):
        if isinstance(model, dict):
            return copy.deepcopy(model)
        if isinstance(model, PathLike):
            model = Path(model).read_text(encoding="utf-8")
        if isinstance(model, (str, bytes)):
            model = json.loads(model)
        if not isinstance(model, dict):
            raise TypeError("model must be a dictionary, JSON object or JSON path")
        return model

    @classmethod
    def normalise(cls, model):
        model = cls._load(model)
        missing = [field for field in ("E", "S", "I", "O") if field not in model]
        if missing:
            raise ValueError("missing " + ", ".join(missing))
        normalised = {
            **model,
            "E": cls.normalise_carrier(model["E"], "E"),
            "S": cls.normalise_carrier(model["S"], "S"),
            "I": cls._normalise_incidence(model["I"], "I"),
            "O": cls._normalise_incidence(model["O"], "O"),
        }
        state_enrichments = normalised.get("states")
        if isinstance(state_enrichments, dict):
            for enrichment in state_enrichments.values():
                if isinstance(enrichment, dict):
                    enrichment.pop("reference", None)
                    enrichment.pop("version", None)
                    enrichment.pop("value_hash", None)
        return normalised

    @staticmethod
    def _structure_errors(model):
        errors = []
        executions, states = set(model["E"]), set(model["S"])
        for relation in ("I", "O"):
            for edge in model[relation]:
                execution, state = edge["exec_id"], edge["state_id"]
                if execution not in executions:
                    errors.append(
                        f"incidence: {relation} references Execution {execution!r} outside E"
                    )
                if state not in states:
                    errors.append(
                        f"incidence: {relation} references State {state!r} outside S"
                    )
        return errors

    @staticmethod
    def _direct_handoffs(model):
        consumers = defaultdict(set)
        for edge in model["I"]:
            consumers[edge["state_id"]].add(edge["exec_id"])
        return {
            (edge["exec_id"], edge["state_id"], consumer)
            for edge in model["O"] for consumer in consumers[edge["state_id"]]
        }

    @staticmethod
    def directed_reachability(executions, handoffs):
        adjacency = {execution: set() for execution in executions}
        for producer, _, consumer in handoffs:
            adjacency[producer].add(consumer)
        reachability = set()
        for source in executions:
            pending = list(adjacency[source])
            while pending:
                target = pending.pop()
                if (source, target) in reachability:
                    continue
                reachability.add((source, target))
                pending.extend(adjacency[target])
        return reachability

    @classmethod
    def _judgments(cls, model):
        handoffs = cls._direct_handoffs(model)
        reachability = cls.directed_reachability(model["E"], handoffs)
        key = lambda relation: tuple(cls.identity_sort_key(item) for item in relation)
        return {
            "direct_handoffs": [
                {"producer_exec_id": producer, "state_id": state, "consumer_exec_id": consumer}
                for producer, state, consumer in sorted(handoffs, key=key)
            ],
            "reachability": [
                {"source_exec_id": source, "target_exec_id": target}
                for source, target in sorted(reachability, key=key)
            ],
        }

    @classmethod
    def _semantics(cls, model, judgments):
        producers_by_state = {state: set() for state in model["S"]}
        for edge in model["O"]:
            producers_by_state[edge["state_id"]].add(edge["exec_id"])
        producer_count_errors = [
            {"state_id": state, "producer_count": len(producers)}
            for state, producers in producers_by_state.items() if len(producers) != 1
        ]
        cyclic_executions = [
            relation["source_exec_id"] for relation in judgments["reachability"]
            if relation["source_exec_id"] == relation["target_exec_id"]
        ]
        errors = []
        for issue in producer_count_errors:
            state = issue["state_id"]
            producers = sorted(producers_by_state[state], key=cls.identity_sort_key)
            if producers:
                errors.append(
                    f"unique_creation: State {state!r} has multiple represented "
                    f"producers {producers!r}"
                )
            else:
                errors.append(f"unique_creation: State {state!r} has no represented producer")
        if cyclic_executions:
            errors.append(
                f"acyclic_ancestry: Executions reach themselves {cyclic_executions!r}"
            )
        return {
            "semantically_valid": not producer_count_errors and not cyclic_executions,
            "unique_creation": not producer_count_errors,
            "acyclic_ancestry": not cyclic_executions,
            "producer_count_errors": producer_count_errors,
            "cyclic_executions": cyclic_executions,
            "errors": errors,
        }

    @staticmethod
    def _report(model=None, *, host=None, structural_errors=(), judgments=None, semantics=None):
        counts = {
            "executions": len(model["E"]) if model is not None else 0,
            "states": len(model["S"]) if model is not None else 0,
            "inputs": len(model["I"]) if model is not None else 0,
            "outputs": len(model["O"]) if model is not None else 0,
            "direct_handoffs": len(judgments["direct_handoffs"]) if judgments else 0,
            "reachability": len(judgments["reachability"]) if judgments else 0,
        }
        return TIDEModelValidityReport({
            "type": "tide_model_validity", "host": host,
            "structurally_valid": model is not None and not structural_errors,
            "semantically_valid": semantics["semantically_valid"] if semantics else None,
            "unique_creation": semantics["unique_creation"] if semantics else None,
            "acyclic_ancestry": semantics["acyclic_ancestry"] if semantics else None,
            "producer_count_errors": semantics["producer_count_errors"] if semantics else [],
            "cyclic_executions": semantics["cyclic_executions"] if semantics else [],
            "judgments": judgments, "counts": counts,
            "errors": [*structural_errors, *(semantics["errors"] if semantics else ())],
        })

    @classmethod
    def report_validity(cls, model, *, host=None):
        try:
            model = cls.normalise(model)
        except (TypeError, ValueError) as error:
            return cls._report(host=host, structural_errors=[f"kernel_shape: {error}"])
        host = host if host is not None else model.get("host")
        structural_errors = cls._structure_errors(model)
        if structural_errors:
            return cls._report(model, host=host, structural_errors=structural_errors)
        judgments = cls._judgments(model)
        semantics = cls._semantics(model, judgments)
        return cls._report(model, host=host, judgments=judgments, semantics=semantics)

    @staticmethod
    def canonicalise(model):
        canonical, identity_maps = TIDECanonicaliser(model).canonicalise(
            return_identity_maps=True
        )
        if "host" in model:
            canonical["host"] = copy.deepcopy(model["host"])
        for field, identity_map in identity_maps.items():
            if identity_map:
                canonical[field] = {
                    canonical_id: {"original_id": original_id}
                    for original_id, canonical_id in identity_map.items()
                }
        return canonical

    @staticmethod
    def _assign_external_producer(model):
        produced = {edge["state_id"] for edge in model["O"]}
        states = [state for state in model["S"] if state not in produced]
        if not states:
            return model, None
        exec_id = "TIDE: External"
        if exec_id in model["E"]:
            raise ValueError(
                "External projection cannot use the existing "
                "Execution 'TIDE: External'"
            )
        model["E"].insert(0, exec_id)
        model["O"].extend(
            {"exec_id": exec_id, "state_id": state_id} for state_id in states
        )
        return model, {"exec_id": exec_id, "state_ids": states}

    @staticmethod
    def _canonical_external_producer(model, external_producer):
        original_exec_id = external_producer["exec_id"]
        original_state_ids = set(external_producer["state_ids"])
        exec_id = next(
            canonical_id
            for canonical_id, enrichment in model["executions"].items()
            if enrichment["original_id"] == original_exec_id
        )
        state_ids = [
            canonical_id
            for canonical_id, enrichment in model["states"].items()
            if enrichment["original_id"] in original_state_ids
        ]
        return {"exec_id": exec_id, "state_ids": state_ids}

    @classmethod
    def to_trace(
        cls,
        model,
        *,
        host=None,
        name="TIDE: .from_model()",
        identity_mode="literal",
        _state_projection=None,
    ):
        """Project a structurally valid Model into a Trace."""
        from tide_tooling.tooling_core.engine.ocean import Ocean
        from tide_tooling.tooling_core.engine.tide_logging import Trace

        if identity_mode not in {"literal", "structural"}:
            raise ValueError("identity_mode must be 'literal' or 'structural'")
        report = cls.report_validity(model, host=host)
        if not report.structurally_valid:
            raise ValueError(
                "Trace cannot be constructed from a structurally invalid "
                "TIDE Model: " + "; ".join(report["errors"])
            )

        raw_model = cls.normalise(model)
        if host is not None:
            raw_model["host"] = host
        projected_model, external_producer = cls._assign_external_producer(
            copy.deepcopy(raw_model)
        )
        if identity_mode == "structural":
            projected_model = cls.normalise(cls.canonicalise(projected_model))
            if external_producer is not None:
                external_producer = cls._canonical_external_producer(
                    projected_model,
                    external_producer,
                )

        ocean = Ocean()
        try:
            return cls._construct_trace(
                projected_model,
                ocean=ocean,
                raw_model=raw_model,
                name=name,
                identity_mode=identity_mode,
                external_producer=external_producer,
                Trace=Trace,
                state_projection=_state_projection,
            )
        finally:
            ocean.close()

    @classmethod
    def _construct_trace(
        cls,
        model,
        *,
        ocean,
        raw_model,
        name,
        identity_mode,
        external_producer,
        Trace,
        state_projection,
    ):
        states, values, value_backing = cls._project_states(
            model,
            state_projection,
        )
        actors = cls._project_actors(model, ocean.id)
        executions = cls._project_executions(model, states, actors)
        records = cls._project_records(executions, values)

        occurrences = set()
        for record in records:
            occurrence = record.producer_occurrence_id
            if occurrence is None:
                continue
            if occurrence in occurrences:
                raise ValueError(
                    "TIDE Model contains a duplicate ActorOccurrence: "
                    f"{occurrence!r}"
                )
            occurrences.add(occurrence)

        # raw_model is passive interoperability/re-export context only.
        # A Model projection is not a selected runtime Runner view.
        # Each projected execution already retains its own Actor/runner
        # identity, so adding a synthetic selected runner would incorrectly
        # scope presentation to a runner that none of the records belong to.
        return Trace(
            records,
            ocean_id=ocean.id,
            name=name,
            _state_authorities={
                (state["reference"], state["version"]): values[state_id].value_hash
                for state_id, state in states.items()
            },
            _value_backing=value_backing,
            model_context={
                "raw_model": raw_model,
                "identity_mode": identity_mode,
                "external_producer": external_producer,
            },
            host=model.get("host"),
            host_record=model.get("host_record"),
        )

    @classmethod
    def _project_states(cls, model, state_projection=None):
        states, values, value_backing = {}, {}, {}
        state_projection = state_projection or {}
        enrichments = model.get("states", {})
        for state_id in model["S"]:
            enrichment = enrichments.get(state_id, {}) if isinstance(enrichments, dict) else {}
            enrichment = enrichment if isinstance(enrichment, dict) else {}
            projected = state_projection.get(state_id)
            if projected is None:
                data_ref = DataRef(
                    key=state_id,
                    store="__model_int__" if type(state_id) is int else "__model__",
                    literal=True,
                )
                version = 0
            else:
                store, key, version = projected
                data_ref = DataRef(key=key, store=store, literal=True)
            states[state_id] = {
                "state_id": state_id,
                "ref": data_ref,
                "reference": DataRef.to_string(data_ref, prefix=False),
                "version": version,
            }
            values[state_id] = cls._materialise_state(
                state_id,
                enrichment if isinstance(enrichment, dict) else {},
            )
            if "value" in enrichment:
                value_backing[values[state_id].value_hash] = enrichment["value"]
        return states, values, value_backing

    @staticmethod
    def _project_actors(model, ocean_id):
        actors = {}
        enrichments = model.get("executions", {})
        for exec_id in model["E"]:
            enrichment = enrichments.get(exec_id, {}) if isinstance(enrichments, dict) else {}
            enrichment = enrichment if isinstance(enrichment, dict) else {}
            actor = enrichment.get("actor", {})
            actor = actor if isinstance(actor, dict) else {}
            actors[exec_id] = Actor(
                actor.get("ocean", ocean_id),
                actor.get("runner", f"TIDE: .from_model({model.get('host') or 'unknown'})"),
                actor.get("name", str(exec_id)),
            )
        return actors

    @staticmethod
    def _project_executions(model, states, actors):
        inputs = {exec_id: [] for exec_id in model["E"]}
        outputs = {exec_id: [] for exec_id in model["E"]}
        for edge in model["I"]:
            inputs[edge["exec_id"]].append(states[edge["state_id"]])
        for edge in model["O"]:
            outputs[edge["exec_id"]].append(states[edge["state_id"]])
        enrichments = model.get("executions", {})
        enrichments = enrichments if isinstance(enrichments, dict) else {}
        return [
            {
                "seq_id": seq_id,
                "exec_id": exec_id,
                "actor": actors[exec_id],
                "inputs": inputs[exec_id],
                "outputs": outputs[exec_id],
                "enrichment": enrichments.get(exec_id, {})
                if isinstance(enrichments.get(exec_id, {}), dict) else {},
            }
            for seq_id, exec_id in enumerate(model["E"])
        ]

    @classmethod
    def _project_records(cls, executions, values):
        producers = {}
        for execution in executions:
            for state in execution["outputs"]:
                producers.setdefault(state["state_id"], []).append(execution)
        records = [
            cls._project_record(execution, values, producers)
            for execution in executions
        ]
        records_by_exec_id = {record.exec_id: record for record in records}
        for execution, child in zip(executions, records, strict=True):
            parent_exec_id = execution["enrichment"].get(
                "emitted_by_exec_id"
            )
            if parent_exec_id is None:
                continue
            parent = records_by_exec_id.get(parent_exec_id)
            if parent is None:
                raise ValueError(
                    "TIDE Model emitted_by_exec_id references an Execution "
                    f"outside E: {parent_exec_id!r}"
                )
            child.emitted_by_exec_id = parent.exec_id
            parent.emitted_nodes.append(EmittedNodeRecord(
                ocean_id=child.source_ocean_id,
                runner_id=child.runner_id,
                pipeline_id=child.pipeline_id,
                node_id=child.node_id,
                func=child.func,
                func_name=child.func_name,
                inputs=child.inputs,
                declared_deps=child.declared_deps,
                declared_targets=child.declared_targets,
                declared_effects=child.declared_effects,
                origin="runtime",
                content_id=child.content_id,
                base_node=child.base_node_id,
                base_node_content_id=child.base_node_content_id,
                node_options=child.node_options,
                emitted_by_exec_id=parent.exec_id,
            ))
        return records

    @staticmethod
    def _project_record(execution, values, producers_by_state):
        actor = execution["actor"]
        inputs, outputs = execution["inputs"], execution["outputs"]
        enrichment = execution["enrichment"]
        node_options = enrichment.get("node_options", {})
        if not isinstance(node_options, dict):
            raise TypeError("TIDE Model execution node_options must be a dict")

        def unavailable_inputs():
            return tuple(__fallback__() for _ in inputs)

        input_record = InputUseRecord(
            name="args", raw=unavailable_inputs(), evaluated=unavailable_inputs(),
            normalised=unavailable_inputs(),
            resolved=tuple(values[state["state_id"]] for state in inputs),
            dependencies=[],
        )
        dependencies = []
        for input_index, state in enumerate(inputs):
            for producer in producers_by_state.get(state["state_id"]) or (None,):
                producer_actor = producer["actor"] if producer is not None else None
                dependencies.append(DependencyUseRecord(
                    associated_input=("args", input_record), dep=state["ref"], type="static",
                    unscoped_ref=state["reference"], resolved_val=values[state["state_id"]],
                    resolved_ver=state["version"],
                    producer_source=producer_actor.source_runner if producer_actor else None,
                    producer_actor=producer_actor,
                    producer_exec_id=producer["exec_id"] if producer else None,
                    input_index=input_index, input_path=[input_index],
                ))
        input_record.dependencies = dependencies
        writes = [
            WriteRecord(
                cause=Cause(description="TIDE: model output"),
                stored_result=values[state["state_id"]],
                stored_version=state["version"],
                stored_target=state["reference"],
            )
            for state in outputs
        ]
        record = ExecRecord(
            seq_id=execution["seq_id"], exec_id=execution["exec_id"],
            ocean_id=actor.source_ocean, runner_id=actor.source_runner,
            pipeline_id=enrichment.get("pipeline"),
            node_id=enrichment.get("node", str(execution["exec_id"])),
            func=None, func_name=actor.source_actor,
            inputs=Inputs({"args": input_record}), deps=dependencies, writes=writes,
            declared_deps=[], declared_targets=[], effects=[],
            declared_effects=[], runtime_effects=[], runtime_targets=[],
            unscoped_store_refs=list(dict.fromkeys(
                state["ref"].store for state in [*inputs, *outputs]
            )),
            status=enrichment.get("status"), started_at=enrichment.get("start"),
            finished_at=enrichment.get("end"), duration_ms=enrichment.get("duration"),
            error=enrichment.get("error"),
            node_options={
                "source": "tide_model",
                "preserve_exec_id": True,
                **node_options,
            },
            output_value=(
                values[outputs[0]["state_id"]]
                if len(outputs) == 1
                else __fallback__()
            ),
            emitted_nodes=[],
            base_node_id=enrichment.get("base_node"),
            base_node_content_id=enrichment.get("base_node_content_id"),
            content_id=enrichment.get("content_id"),
        )
        occurrence_id = enrichment.get("occurrence")
        if occurrence_id is not None:
            if not isinstance(occurrence_id, int) or isinstance(occurrence_id, bool) or occurrence_id < 0:
                raise ValueError(
                    "TIDE Model execution occurrence must be a non-negative integer"
                )
            record.producer_occurrence_id = ActorOccurrence(actor, occurrence_id)
        return record

    @staticmethod
    def _materialise_state(state_id, enrichment):
        from tide_tooling.tooling_core.bundling.TIDEOpaqueValue import TIDEOpaqueValue

        value = enrichment.get("value")
        if isinstance(value, TIDEOpaqueValue) and "value_type" not in enrichment:
            return tide_snapshot_value(value, boundary="model")

        value_type = enrichment.get("value_type")
        if isinstance(value_type, type):
            value_type = value_type.__name__
        value_type = value_type or (
            "TIDEUnknownType" if "value" in enrichment else "TIDEUnknownValue"
        )
        opaque = TIDEOpaqueValue(
            type_name=value_type,
            representation=f"<value unavailable for State {state_id!r}>",
            reason="TIDE Model State supplied without an associated value",
        )
        return tide_snapshot_value(opaque, boundary="model")

    @staticmethod
    def _trace_executions(records, host):
        execs = {}
        for record in records:
            if host is None and record.producer_occurrence_id is not None:
                execs[repr(record.producer_occurrence_id)] = record
            else:
                execs[record.exec_id] = record


        return execs

    @staticmethod
    def _execution_records(execution):
        return execution if isinstance(execution, tuple) else (execution,)

    @staticmethod
    def _state_key(reference, version):
        if isinstance(reference, DataRef):
            reference = DataRef.to_string(
                reference,
                prefix=False,
            )
        return reference, version

    @classmethod
    def _trace_states(cls, trace, records):
        states = {}

        def observe(reference, version, value, snapshot=None):
            occurrence = cls._state_key(reference, version)
            reference, version = occurrence

            if occurrence not in states:
                states[occurrence] = {
                    "state_id": f"{reference}[v{version}]",
                    "reference": reference,
                    "version": version,
                    "value": value,
                    "snapshot": snapshot,
                }
                return

            projected = states[occurrence]
            if (
                isinstance(projected["value"], __fallback__)
                and not isinstance(value, __fallback__)
            ):
                projected["value"] = value
                projected["snapshot"] = snapshot

        for record in records:
            for write in record.writes or ():
                observe(
                    write.stored_target,
                    write.stored_version,
                    write.stored_result,
                    (
                        write._tide_snapshot("stored_result")
                        if hasattr(write, "_tide_snapshot")
                        else None
                    ),
                )

        for record in records:
            for dependency in record.deps or ():
                for occurrence in trace._dep_records(dependency):
                    observe(
                        occurrence.unscoped_reference,
                        occurrence.resolved_version,
                        occurrence.resolved_value,
                        (
                            occurrence._tide_snapshot("resolved_value")
                            if hasattr(occurrence, "_tide_snapshot")
                            else None
                        ),
                    )

        return states

    @classmethod
    def _trace_incidence(cls, trace, executions, states):
        inputs = []
        outputs = []
        seen_inputs = set()
        seen_outputs = set()

        for exec_id, execution in executions.items():
            for record in cls._execution_records(execution):
                for dependency in record.deps or ():
                    for occurrence in trace._dep_records(dependency):
                        state_key = cls._state_key(
                            occurrence.unscoped_reference,
                            occurrence.resolved_version,
                        )
                        state_id = states[state_key]["state_id"]
                        edge = (exec_id, state_id)
                        if edge not in seen_inputs:
                            seen_inputs.add(edge)
                            inputs.append({
                                "exec_id": exec_id,
                                "state_id": state_id,
                            })

                for write in record.writes or ():
                    state_key = cls._state_key(
                        write.stored_target,
                        write.stored_version,
                    )
                    state_id = states[state_key]["state_id"]
                    edge = (exec_id, state_id)
                    if edge not in seen_outputs:
                        seen_outputs.add(edge)
                        outputs.append({
                            "exec_id": exec_id,
                            "state_id": state_id,
                        })

        return inputs, outputs

    @classmethod
    def _add_trace_enrichment(
        cls,
        trace,
        model,
        executions,
        states,
        *,
        enrich=True,
    ):
        model = {**model}

        if not enrich:
            return model

        execution_enrichments = {}
        attempt_by_exec_id = {
            attempt.exec_id: attempt
            for attempt in trace._ready_attempts
        }
        for exec_id, execution in executions.items():
            records = cls._execution_records(execution)
            record = records[0]
            actor = record.producer_id
            enrichment = {
                "actor": {
                    "name": actor.source_actor,
                    "ocean": actor.source_ocean,
                    "runner": actor.source_runner,
                }
            }
            occurrence = record.producer_occurrence_id
            if isinstance(occurrence, ActorOccurrence):
                enrichment["occurrence"] = occurrence.occurrence_id

            unresolved = bool(getattr(record, "ambiguous", False))
            attempt = attempt_by_exec_id.get(record.exec_id)
            if unresolved:
                enrichment["ambiguous"] = True
                enrichment["source_exec_id"] = record.exec_id

            for name, value in (
                (
                    "pipeline",
                    (
                        getattr(attempt, "pipeline_id", None)
                        if unresolved
                        else getattr(record, "pipeline_id", None)
                    ),
                ),
                (
                    "node",
                    (
                        getattr(attempt, "node_definition_id", None)
                        if unresolved
                        else getattr(record, "node_id", None)
                    ),
                ),
                (
                    "status",
                    "ambiguous" if unresolved else getattr(record, "status", None),
                ),
                ("start", getattr(record, "started_at", None)),
                ("end", getattr(record, "finished_at", None)),
                ("duration", getattr(record, "duration_ms", None)),
                (
                    "error",
                    (
                        repr(record.error)
                        if getattr(record, "error", None) is not None
                        else None
                    ),
                ),
            ):
                if value is not None:
                    enrichment[name] = value

            execution_enrichments[exec_id] = enrichment

        if execution_enrichments:
            model["executions"] = execution_enrichments

        state_enrichments = {}
        for state in states.values():
            enrichment = {}
            value = state["value"]

            if not isinstance(value, __fallback__):
                from tide_tooling.tooling_core.bundling.TIDEOpaqueValue import TIDEOpaqueValue

                ref_version = (
                    state["reference"],
                    state["version"],
                )
                value_hash = trace._index.value_hash_by_ref_version.get(
                    ref_version
                )
                snapshot = state.get("snapshot")
                if value_hash is None and snapshot is not None:
                    value_hash = snapshot.value_hash
                    retained_value = snapshot.restore()
                elif value_hash is not None:
                    retained_value = trace._index.value_by_hash[value_hash]
                else:
                    raise ValueError(
                        "Model State has no retained value identity: "
                        f"{state['state_id']}"
                    )
                if not isinstance(retained_value, TIDEOpaqueValue):
                    enrichment["value"] = retained_value
                enrichment["value_hash"] = value_hash
                enrichment["value_type"] = (
                    snapshot.type_name
                    if snapshot is not None
                    else (
                        retained_value.type_name
                        if isinstance(retained_value, TIDEOpaqueValue)
                        else type(retained_value).__name__
                    )
                )

            state_enrichments[state["state_id"]] = enrichment

        if state_enrichments:
            model["states"] = state_enrichments

        return model

    @staticmethod
    def _select_enrichment(raw_model, *, host, enrich=True):
        model = copy.deepcopy(raw_model)

        if host is None:
            model.pop("host", None)
        else:
            model["host"] = host

        if not enrich:
            model = {
                name: model[name]
                for name in ("host", "E", "S", "I", "O")
                if name in model
            }

        return model

    @staticmethod
    def _validate_model_export_options(
        *,
        override_host_as,
        enrich,
        incidence_format,
        identity_mode,
    ):
        if (
            override_host_as is not None
            and not isinstance(override_host_as, str)
        ):
            raise TypeError("override_host_as must be a string or None")
        if (
            isinstance(override_host_as, str)
            and not override_host_as.strip()
        ):
            raise ValueError("override_host_as must not be empty")

        if not isinstance(enrich, bool):
            raise TypeError("enrich must be a bool")

        if incidence_format not in {"explicit", "compact", "pairs"}:
            raise ValueError(
                "incidence_format must be 'explicit', 'compact', or 'pairs'"
            )

        if identity_mode not in {"literal", "structural"}:
            raise ValueError(
                "identity_mode must be 'literal' or 'structural'"
            )

    @staticmethod
    def _incidence_entry(exec_id, state_id, incidence_format):
        if incidence_format == "explicit":
            return {"exec_id": exec_id, "state_id": state_id}
        if incidence_format == "compact":
            return {"e": exec_id, "s": state_id}
        return [exec_id, state_id]

    @classmethod
    def _format_incidence(cls, model, incidence_format):
        model = {**model}

        for relation in ("I", "O"):
            pairs = (
                (
                    entry["exec_id"],
                    entry["state_id"],
                )
                if isinstance(entry, dict)
                else entry
                for entry in model[relation]
            )

            model[relation] = [
                cls._incidence_entry(
                    exec_id,
                    state_id,
                    incidence_format,
                )
                for exec_id, state_id in pairs
            ]

        return model

    @classmethod
    def _project_trace(
        cls,
        trace,
        *,
        override_host_as=None,
        enrich=True,
    ):
        host = trace.host if override_host_as is None else override_host_as

        raw_model = trace._model_context.get("raw_model")
        if raw_model is not None:
            return cls._select_enrichment(
                raw_model,
                host=host,
                enrich=enrich,
            )

        executions = cls._trace_executions(trace._execution_records, host)
        states = cls._trace_states(trace, trace._execution_records)
        inputs, outputs = cls._trace_incidence(trace, executions, states)

        model = {
            "E": list(executions),
            "S": [state["state_id"] for state in states.values()],
            "I": inputs,
            "O": outputs,
        }

        if host is not None:
            model["host"] = host

        model = cls._add_trace_enrichment(
            trace,
            model,
            executions,
            states,
            enrich=enrich,
        )
        return model

    @classmethod
    def from_trace(
        cls,
        trace,
        *,
        override_host_as=None,
        enrich=True,
        incidence_format="explicit",
        identity_mode="literal",
    ):
        """Project this Trace as an interoperable TIDE Model."""
        cls._validate_model_export_options(
            override_host_as=override_host_as,
            enrich=enrich,
            incidence_format=incidence_format,
            identity_mode=identity_mode,
        )

        model = cls._project_trace(
            trace,
            override_host_as=override_host_as,
            enrich=enrich,
        )

        report = cls.report_validity(model)
        if not report.structurally_valid:
            raise ValueError(
                "Trace cannot be exported as a structurally valid "
                "TIDE Model: " + "; ".join(report["errors"])
            )

        if identity_mode == "structural":
            model = cls.canonicalise(model)

        model = cls._format_incidence(model, incidence_format)

        # Model export never carries Trace/Event persistence evidence.
        model.pop("events", None)
        model.pop("corruption", None)

        return model
