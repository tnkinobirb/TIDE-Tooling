from __future__ import annotations

import subprocess
import sys
import copy
import os
from collections import defaultdict
from dataclasses import asdict, dataclass, field, is_dataclass, replace
from enum import Enum
from importlib import import_module
import json
from pathlib import Path


@dataclass(frozen=True)
class NOWMeta:
    trial_id: str
    activation_id: int | None
    function: str | None
    line: int | None
    output: str
    evaluation_id: int | None = None
    _trace: object = field(default=None, compare=False, repr=False)
    _exec_id: str | None = field(default=None, compare=False, repr=False)

    def _bind(self, trace, exec_id):
        return replace(self, _trace=trace, _exec_id=exec_id)

    def trace(self):
        if self._trace is None or self._exec_id is None:
            raise RuntimeError(
                "NOWMeta.trace() requires metadata obtained through "
                "Trace inspection"
            )
        return self._trace._trace_for_exec(self._exec_id)

    def to_dict(self):
        return {
            "trial_id": self.trial_id,
            "activation_id": self.activation_id,
            "evaluation_id": self.evaluation_id,
            "function": self.function,
            "line": self.line,
            "output": self.output,
        }

    @classmethod
    def from_dict(cls, value):
        return cls(
            trial_id=value["trial_id"],
            activation_id=value["activation_id"],
            evaluation_id=value.get("evaluation_id"),
            function=value["function"],
            line=value["line"],
            output=value["output"],
        )

    def show(self):
        # Describe an evaluation without claiming it has its own activation.
        if self.evaluation_id is not None:
            heading = f"NOW EVALUATION [{self.evaluation_id}]\n"
        else:
            heading = (
                f"NOW ACTIVATION [{self.activation_id}]\n"
                f"function: {self.function}\n"
            )
        rendered = heading + (
            f"line: {self.line if self.line is not None else 'unknown'}\n"
            f"output: {self.output}"
        )
        print(rendered)
        return rendered


class NOWAdapter:
    """Capture or project one noWorkflow trial as a TIDE Trace."""

    def __init__(self, project_dir, trial=None):
        self.project_dir = Path(project_dir).resolve()
        self.trial = trial

    @classmethod
    def capture(cls, script, *script_args, project_dir=None):
        script = Path(script).resolve()
        if not script.is_file():
            raise FileNotFoundError(script)

        project_dir = Path(project_dir or script.parent).resolve()
        project_dir.mkdir(parents=True, exist_ok=True)
        cls._require_noworkflow()
        command = [
            sys.executable,
            "-m",
            "noworkflow",
            "run",
            "-c",
            "main",
            "--dir",
            str(project_dir),
            str(script),
            *(str(argument) for argument in script_args),
        ]
        environment = os.environ.copy()
        environment["TIDE_NOW_CAPTURE"] = "1"
        subprocess.run(
            command,
            cwd=script.parent,
            env=environment,
            check=True,
        )
        return cls(project_dir).to_trace()

    def to_trace(self):
        trial = self.trial if self.trial is not None else self._load_trial()
        if any(
            activation.finish_checkpoint is None
            and activation.start_checkpoint is None
            for activation in trial.activations
        ):
            raise ValueError("noWorkflow activation has no checkpoint")
        activations = list(trial.activations)
        evaluations = list(getattr(trial, "evaluations", ()))
        evaluation_by_id = {evaluation.id: evaluation for evaluation in evaluations}
        evaluations_by_activation = defaultdict(list)
        for evaluation in evaluations:
            evaluations_by_activation[evaluation.activation_id].append(evaluation)

        dependency_records_by_evaluation = defaultdict(list)
        for dependency in trial.dependencies:
            dependency_records_by_evaluation[
                dependency.dependent_id
            ].append(dependency)

        records = self._records(
            trial,
            activations,
            dependency_records_by_evaluation,
            evaluations_by_activation,
            evaluation_by_id,
        )
        self._attach_file_accesses(trial, records)
        records.sort(key=self._record_chronology)
        for seq_id, record in enumerate(records):
            record.seq_id = seq_id

        from tide_tooling.tooling_core.engine.tide_logging import Trace
        ocean_id = f"noworkflow:{trial.id}"
        runner_id = f"trial:{trial.id}"
        trial_finished = bool(getattr(trial, "finished", False))
        return Trace(
            records,
            ocean_id=ocean_id,
            pipelines={trial.script: trial.script},
            runner=runner_id,
            name=f"noWorkflow trial {trial.id}",
            host="noWorkflow",
            host_record={
                "trial_id": str(trial.id),
                "script": str(trial.script),
            },
            started_runner_ids=(runner_id,),
            causal_runner_ids=(runner_id,),
            ended_runner_ids=(runner_id,) if trial_finished else (),
        )

    @staticmethod
    def _record_chronology(record):
        checkpoint = (
            record.finished_at
            if record.finished_at is not None
            else record.started_at
        )
        return (checkpoint, record.exec_id)

    @staticmethod
    def _attach_file_accesses(trial, records):
        from tide_tooling.tooling_core.engine.events import Cause, Effects
        from tide_tooling.tooling_core.engine.tide_records import EffectRecord, WriteRecord

        records_by_activation = {
            record.node_options.get("activation_id"): record
            for record in records
            if record.node_options.get("record_kind") == "activation"
        }
        versions = {}
        for access in sorted(
            getattr(trial, "file_accesses", ()),
            key=lambda item: (item.checkpoint, item.id),
        ):
            if access.checkpoint is None:
                raise ValueError("noWorkflow file access has no checkpoint")
            owner = records_by_activation.get(access.activation_id)
            if owner is None:
                raise ValueError(
                    "noWorkflow file access referenced an unknown activation"
                )

            reference = f"file:{access.name}"
            before = access.content_hash_before
            after = access.content_hash_after
            mode = access.mode or ""
            event_id = f"NOW_FILE:{trial.id}:{access.id}"
            current_version = versions.get(reference, 0)

            if before is not None and (mode.startswith("r") or "+" in mode):
                owner.effects.append(EffectRecord(
                    Effects.READ,
                    (reference, current_version),
                    event_id=f"{event_id}:read",
                    origin="runtime",
                    immediate=False,
                    output=before,
                    target_existed=True,
                ))

            write_observed = after is not None and (
                before != after or mode.startswith(("w", "x"))
            )
            if not write_observed:
                continue

            version = (
                versions[reference] + 1
                if reference in versions
                else 0
            )
            versions[reference] = version
            owner.effects.append(EffectRecord(
                Effects.WRITE,
                (reference, version),
                event_id=f"{event_id}:write",
                origin="runtime",
                immediate=False,
                output=True,
            ))
            owner.writes.append(WriteRecord(
                Cause(description="observed noWorkflow file write"),
                after,
                version,
                reference,
                updated=version > 0,
            ))

    def _records(
        self,
        trial,
        activations,
        dependency_records_by_evaluation,
        evaluations_by_activation,
        evaluation_by_id,
    ):
        from tide_tooling.tooling_core.engine.tide_records import (
            InputUseRecord,
        )

        activation_exec_ids = {
            activation.id: f"TIDE_EXEC:noworkflow:{trial.id}:{activation.id}"
            for activation in activations
        }
        evaluation_exec_ids = {
            evaluation.id: (
                f"TIDE_EXEC:noworkflow:{trial.id}:evaluation:{evaluation.id}"
            )
            for evaluation in evaluation_by_id.values()
        }
        records = []

        for activation in activations:
            now_meta = NOWMeta(
                trial_id=str(trial.id),
                activation_id=activation.id,
                function=activation.name,
                line=getattr(activation, "line", None),
                output=activation.this_evaluation.repr,
            )
            records.append(self._now_record(
                trial=trial,
                exec_id=activation_exec_ids[activation.id],
                name=activation.name,
                node_id=f"activation:{activation.id}",
                input_record=InputUseRecord("args", (), (), (), (), []),
                dependencies=[],
                output=None,
                output_reference=None,
                started_at=activation.start_checkpoint,
                finished_at=activation.finish_checkpoint,
                now_meta=now_meta,
                options={
                    "record_kind": "activation",
                    "activation_id": activation.id,
                },
                base_node_id=(
                    f"noworkflow:activation:{activation.name}:"
                    f"{getattr(activation, 'line', None)}"
                ),
                produces_value=False,
            ))

        records.extend(self._evaluation_records(
            trial,
            evaluations_by_activation,
            evaluation_by_id,
            dependency_records_by_evaluation,
            evaluation_exec_ids,
        ))
        self._link_now_occurrences(
            records,
            activations,
            evaluation_by_id,
            activation_exec_ids,
            evaluation_exec_ids,
        )
        return records

    def _evaluation_records(
        self,
        trial,
        evaluations_by_activation,
        evaluation_by_id,
        dependencies_by_evaluation,
        evaluation_exec_ids,
    ):
        from tide_tooling.tooling_core.engine.tide_records import InputUseRecord

        records = []

        for activation_id, evaluations in evaluations_by_activation.items():
            for evaluation in evaluations:
                component = evaluation.code_component
                kind = (
                    {"r": "read", "w": "write"}.get(component.mode, "name")
                    if component.type == "name"
                    else component.type
                )
                name = f"{kind} {component.name}"
                value = evaluation.repr
                value_snapshot = self._evaluation_snapshot(
                    value,
                    self._evaluation_type(evaluation),
                )
                reference = f"evaluation:{evaluation.id}"
                input_record = InputUseRecord(
                    "args", (), (), (), (), []
                )
                now_meta = NOWMeta(
                    trial_id=str(trial.id),
                    activation_id=activation_id,
                    function=None,
                    line=component.first_char_line,
                    output=value,
                    evaluation_id=evaluation.id,
                )
                record = self._now_record(
                    trial=trial,
                    exec_id=evaluation_exec_ids[evaluation.id],
                    name=name,
                    node_id=f"evaluation:{evaluation.id}",
                    input_record=input_record,
                    dependencies=[],
                    output=value_snapshot,
                    output_reference=reference,
                    started_at=evaluation.checkpoint,
                    finished_at=evaluation.checkpoint,
                    now_meta=now_meta,
                    options={
                        "record_kind": "evaluation",
                        "activation_id": activation_id,
                        "evaluation_id": evaluation.id,
                        "mode": component.mode,
                    },
                    base_node_id=(
                        f"noworkflow:evaluation:{component.type}:"
                        f"{component.name}:{component.first_char_line}"
                    ),
                    produces_value=True,
                )
                self._append_evaluation_dependencies(
                    record,
                    dependencies_by_evaluation[evaluation.id],
                    evaluation_by_id,
                    evaluation_exec_ids,
                )
                records.append(record)

        return records

    @staticmethod
    def _evaluation_type(evaluation):
        # Read the runtime type recorded by NOW, following shared values.
        container = evaluation.member_container or evaluation
        return next(
            (
                membership.member.repr
                for membership in container.memberships_as_collection
                if membership.key == ".__class__"
            ),
            None,
        )

    @staticmethod
    def _evaluation_snapshot(representation, recorded_type):
        """Keep NOW's representation and runtime type as one value claim."""
        from tide_tooling.tooling_core.engine.utils import (
            TIDEValueSnapshot,
            tide_value_hasher,
        )

        type_name = recorded_type or "TIDEUnknownType"
        return TIDEValueSnapshot(
            value_hash=tide_value_hasher((
                "noworkflow",
                type_name,
                representation,
            )),
            capture_method="hash",
            type_name=type_name,
            representation=representation,
            boundary="noworkflow.evaluation",
            exact=False,
        )

    @staticmethod
    def _now_record(
        *, trial, exec_id, name, node_id, input_record, dependencies,
        output, output_reference, started_at, finished_at,
        now_meta=None, options=None, base_node_id=None, produces_value=False,
    ):
        from tide_tooling.tooling_core.engine.classes import __fallback__
        from tide_tooling.tooling_core.engine.events import Cause
        from tide_tooling.tooling_core.engine.tide_records import (
            ExecRecord,
            Inputs,
            WriteRecord,
        )

        writes = (
            [WriteRecord(
                Cause(description="observed noWorkflow evaluation"),
                output,
                0,
                output_reference,
                snapshot_lineage=[output.to_dict()],
            )]
            if produces_value
            else []
        )
        return ExecRecord(
            seq_id=0,
            exec_id=exec_id,
            ocean_id=f"noworkflow:{trial.id}",
            runner_id=f"trial:{trial.id}",
            pipeline_id=trial.script,
            node_id=node_id,
            func=name,
            func_name=name,
            inputs=Inputs({"args": input_record}),
            deps=dependencies,
            effects=[],
            declared_effects=[],
            declared_targets=[],
            runtime_effects=[],
            runtime_targets=[],
            declared_deps=[],
            unscoped_store_refs=None,
            status=None,
            started_at=started_at,
            finished_at=finished_at,
            duration_ms=(
                max(0.0, (finished_at - started_at) * 1000)
                if started_at is not None and finished_at is not None
                else None
            ),
            error=None,
            node_options={
                "capture_origin": "noworkflow",
                "preserve_exec_id": True,
                **(options or {}),
            },
            output_value=output if produces_value else __fallback__(),
            writes=writes,
            now_meta=now_meta,
            base_node_id=base_node_id,
            base_node_content_id=base_node_id,
            content_id=base_node_id,
        )

    @classmethod
    def _append_evaluation_dependencies(
        cls,
        record,
        dependencies,
        evaluation_by_id,
        evaluation_exec_ids,
    ):
        from tide_tooling.tooling_core.engine.classes import DataRef
        from tide_tooling.tooling_core.engine.tide_records import (
            Actor,
            DependencyUseRecord,
        )

        input_record = record.inputs.args
        values = []
        source_ids = set()
        for dependency in dependencies:
            source_id = dependency.dependency_id
            if source_id in source_ids:
                continue
            source_ids.add(source_id)

            source = evaluation_by_id.get(source_id)
            producer_exec_id = evaluation_exec_ids.get(source_id)
            if source is None or producer_exec_id is None:
                continue
            reference = f"evaluation:{source_id}"
            source_component = source.code_component
            source_kind = (
                {"r": "read", "w": "write"}.get(
                    source_component.mode,
                    "name",
                )
                if source_component.type == "name"
                else source_component.type
            )
            producer_name = f"{source_kind} {source_component.name}"
            source_snapshot = cls._evaluation_snapshot(
                source.repr,
                cls._evaluation_type(source),
            )
            record.deps.append(DependencyUseRecord(
                associated_input=("args", input_record),
                dep=DataRef(reference, "noworkflow", literal=True),
                type="static",
                unscoped_ref=reference,
                resolved_val=source_snapshot,
                resolved_ver=0,
                producer_source=record.runner_id,
                producer_actor=Actor(
                    record.source_ocean_id,
                    record.runner_id,
                    producer_name,
                ),
                producer_exec_id=producer_exec_id,
                input_index=len(values),
                snapshot_lineage=[source_snapshot.to_dict()],
            ))
            values.append(source_snapshot.restore())

        input_record.raw = tuple(values)
        input_record.evaluated = tuple(values)
        input_record.normalised = tuple(values)
        input_record.resolved = tuple(values)
        input_record.dependencies = record.deps

    @staticmethod
    def _link_now_occurrences(
        records,
        activations,
        evaluation_by_id,
        activation_exec_ids,
        evaluation_exec_ids,
    ):
        from tide_tooling.tooling_core.engine.tide_records import EmittedNodeRecord

        records_by_exec_id = {record.exec_id: record for record in records}

        def link(parent_exec_id, child_exec_id):
            parent = records_by_exec_id.get(parent_exec_id)
            child = records_by_exec_id.get(child_exec_id)
            if parent is None or child is None:
                return
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

        for evaluation in evaluation_by_id.values():
            link(
                activation_exec_ids.get(evaluation.activation_id),
                evaluation_exec_ids[evaluation.id],
            )

        for activation in activations:
            link(
                evaluation_exec_ids.get(activation.this_evaluation.id),
                activation_exec_ids[activation.id],
            )

    def _load_trial(self):
        self._require_noworkflow()
        persistence_config = import_module(
            "noworkflow.now.persistence"
        ).persistence_config
        Trial = import_module(
            "noworkflow.now.persistence.models"
        ).Trial

        persistence_config.connect_existing(str(self.project_dir))
        return Trial()

    @staticmethod
    def _require_noworkflow():
        try:
            import_module("noworkflow")
        except ImportError as error:
            raise ImportError(
                "TIDE.now requires the optional dependency: pip install tide-tooling[now]"
            ) from error


class OpenLineageAdapter:
    """Project explicit OpenLineage run and dataset evidence into a TIDE Model."""

    def __init__(self, events):
        # Decode explicit JSON text or bytes before normalising events.
        if isinstance(events, (str, bytes)):
            events = json.loads(events)

        # Treat one event object as a one-element event collection.
        if isinstance(events, dict) or not hasattr(events, "__iter__"):
            self.host_record = self._plain(events)
            events = [events]
        else:
            events = list(events)
            self.host_record = self._plain(events)

        self.events = [self._plain(event) for event in events]

        if not all(isinstance(event, dict) for event in self.events):
            raise TypeError("OpenLineage input must contain RunEvent objects")

    def to_model(self):
        runs = {}
        states = set()
        state_projection = {}
        inputs = []
        outputs = []
        latest_state = {}
        next_version = defaultdict(int)
        actor_occurrences = defaultdict(int)

        def state_for(dataset_id, *, produced):
            if not produced and dataset_id in latest_state:
                return latest_state[dataset_id]

            version = next_version[dataset_id]
            next_version[dataset_id] += 1
            state_id = self._state_id(dataset_id, version)
            states.add(state_id)
            namespace, name = dataset_id
            state_projection[state_id] = (
                "__openlineage__",
                f"{namespace}/{name}",
                version,
            )
            latest_state[dataset_id] = state_id
            return state_id

        # One runId is one execution. Repeated lifecycle Events add facts to
        # that execution; they never create another execution or State write.
        for event in self.events:
            run_id = self._required(event.get("run", {}), "runId", "run")
            job = event.get("job", {})
            job_id = (self._required(job, "namespace", "job"), self._required(job, "name", "job"))
            record = runs.get(run_id)
            if record is None:
                occurrence = actor_occurrences[job_id]
                actor_occurrences[job_id] += 1
                record = runs[run_id] = {
                    "job": job_id,
                    "inputs": {},
                    "outputs": {},
                    "occurrence": occurrence,
                    "status": None,
                    "start": None,
                    "end": None,
                }
            if record["job"] != job_id:
                raise ValueError(f"OpenLineage run {run_id!r} names more than one Job")

            event_type = event.get("eventType")
            event_time = event.get("eventTime")
            if event_type in {"START", "RUNNING", "COMPLETE", "FAIL", "ABORT"}:
                record["status"] = {
                    "START": "running",
                    "RUNNING": "running",
                    "COMPLETE": "succeeded",
                    "FAIL": "failed",
                    "ABORT": "aborted",
                }[event_type]
            if event_type == "START" and record["start"] is None:
                record["start"] = event_time
            if event_type in {"COMPLETE", "FAIL", "ABORT"}:
                record["end"] = event_time

            # Inputs are observations of the latest State already established
            # for that Dataset. With no represented producer, the State is an
            # ordinary external TIDE input State.
            for dataset in event.get("inputs") or ():
                dataset_id = self._dataset_id(dataset)
                if dataset_id in record["inputs"]:
                    continue
                state_id = state_for(dataset_id, produced=False)
                record["inputs"][dataset_id] = state_id
                inputs.append([run_id, state_id])

            # Every distinct output Dataset of this Run is one normal TIDE
            # write occurrence and therefore advances that Reference version.
            for dataset in event.get("outputs") or ():
                dataset_id = self._dataset_id(dataset)
                if dataset_id in record["outputs"]:
                    continue
                state_id = state_for(dataset_id, produced=True)
                record["outputs"][dataset_id] = state_id
                outputs.append([run_id, state_id])

        model = {
            "host": "OpenLineage",
            "host_record": copy.deepcopy(self.host_record),
            "E": sorted(runs),
            "S": sorted(states),
            "I": sorted(inputs),
            "O": sorted(outputs),
            "executions": {
                run_id: {
                    "actor": {
                        "ocean": "OpenLineage",
                        "runner": record["job"][0],
                        "name": record["job"][1],
                    },
                    "occurrence": record["occurrence"],
                    "pipeline": record["job"][0],
                    "status": record["status"],
                    "start": record["start"],
                    "end": record["end"],
                }
                for run_id, record in runs.items()
            },
        }
        self._state_projection = state_projection
        return model

    def to_trace(self, **from_model_options):
        from tide_tooling.tooling_core.engine.tide_model import TIDEModel

        model = self.to_model()
        return TIDEModel.to_trace(
            model,
            _state_projection=self._state_projection,
            **from_model_options,
        )

    @classmethod
    def _dataset_id(cls, dataset):
        dataset = cls._plain(dataset)
        namespace = cls._required(dataset, "namespace", "dataset")
        name = cls._required(dataset, "name", "dataset")
        return namespace, name

    @staticmethod
    def _state_id(dataset_id, version):
        namespace, name = dataset_id
        return json.dumps(
            [namespace, name, version],
            ensure_ascii=False,
            separators=(",", ":"),
        )

    @classmethod
    def _plain(cls, value):
        if isinstance(value, dict):
            return {str(key): cls._plain(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [cls._plain(item) for item in value]
        if isinstance(value, Enum):
            return value.value
        if is_dataclass(value):
            return cls._plain(asdict(value))
        if hasattr(value, "to_dict") and callable(value.to_dict):
            return cls._plain(value.to_dict())
        if hasattr(value, "__attrs_attrs__"):
            return {field.name: cls._plain(getattr(value, field.name)) for field in value.__attrs_attrs__}
        if isinstance(value, (str, int, float, bool)) or value is None:
            return value
        raise TypeError(f"Unsupported OpenLineage value: {type(value).__name__}")

    @staticmethod
    def _required(value, name, owner):
        found = value.get(name) if isinstance(value, dict) else None
        if not isinstance(found, str) or not found:
            raise ValueError(f"OpenLineage {owner}.{name} must be a non-empty string")
        return found


class OpenTelemetryAdapter:
    """Project OpenTelemetry Spans as executions and explicit ancestry only."""

    host = "OpenTelemetry"
    source = "opentelemetry"

    def __init__(self, spans):
        source = self._plain(spans)
        self._entries = []
        self.host_record = {}

        for span, resource, scope in self._spans(source):
            trace_id = self._trace_id(span)
            span_id = self._span_id(span)
            exec_id = self._exec_id(trace_id, span_id)
            if exec_id in self.host_record:
                raise ValueError(
                    f"{self.host} Span appears more than once: {exec_id!r}"
                )
            entry = {
                "span": span,
                "resource": resource,
                "scope": scope,
            }
            self.host_record[exec_id] = copy.deepcopy(entry)
            self._entries.append((exec_id, trace_id, span_id, entry))

    def to_model(self):
        executions = {}
        states = {}
        inputs = []
        outputs = []
        occurrence_by_actor = defaultdict(int)

        for exec_id, trace_id, span_id, entry in self._entries:
            span = entry["span"]
            name = span.get("name")
            if not isinstance(name, str) or not name:
                raise ValueError(f"{self.host} Span requires a non-empty name")

            namespace = self._actor_namespace(
                entry["resource"],
                entry["scope"],
                trace_id,
            )
            actor_key = (namespace, name)
            occurrence = occurrence_by_actor[actor_key]
            occurrence_by_actor[actor_key] += 1
            span_states, span_inputs, span_outputs, value_options = (
                self._span_model_evidence(span, trace_id, span_id, exec_id)
            )
            states.update(span_states)
            inputs.extend(span_inputs)
            outputs.extend(span_outputs)
            definition_id = json.dumps(
                [namespace, name],
                ensure_ascii=False,
                separators=(",", ":"),
            )
            executions[exec_id] = {
                "actor": {
                    "name": name,
                    "ocean": self.host,
                    "runner": namespace,
                },
                "occurrence": occurrence,
                "pipeline": trace_id,
                "node": definition_id,
                "base_node": definition_id,
                "base_node_content_id": definition_id,
                "content_id": definition_id,
                "status": self._status(span.get("status")),
                "start": self._first(
                    span,
                    "start_time",
                    "startTime",
                    "startTimeUnixNano",
                ),
                "end": self._first(
                    span,
                    "end_time",
                    "endTime",
                    "endTimeUnixNano",
                ),
                "node_options": {
                    "source": self.source,
                    "preserve_exec_id": True,
                    **value_options,
                },
            }

        # Parent Span identity is execution ancestry, not a value dependency.
        for exec_id, trace_id, _, entry in self._entries:
            parent_span_id = self._parent_span_id(entry["span"])
            if parent_span_id is None:
                continue
            parent_exec_id = self._exec_id(trace_id, parent_span_id)
            if parent_exec_id in executions:
                executions[exec_id]["emitted_by_exec_id"] = parent_exec_id

        return {
            "host": self.host,
            "host_record": copy.deepcopy(self.host_record),
            "E": list(executions),
            "S": list(states),
            "I": inputs,
            "O": outputs,
            "executions": executions,
            "states": states,
        }

    def to_trace(self):
        from tide_tooling.tooling_core.engine.tide_model import TIDEModel

        model = self.to_model()
        return TIDEModel.to_trace(
            model,
            _state_projection=getattr(self, "_state_projection", None),
        )

    @staticmethod
    def _span_model_evidence(span, trace_id, span_id, exec_id):
        return {}, [], [], {}

    @classmethod
    def _spans(cls, value, resource=None, scope=None):
        if isinstance(value, list):
            for item in value:
                yield from cls._spans(item, resource, scope)
            return
        if not isinstance(value, dict):
            raise TypeError(f"{cls.host} input must contain Span objects")

        resource = value.get("resource", resource)
        scope = value.get(
            "scope",
            value.get("instrumentationLibrary", scope),
        )
        if any(key in value for key in ("span_id", "spanId", "context")):
            yield value, resource, scope
            return

        found = False
        for key in (
            "resourceSpans",
            "scopeSpans",
            "instrumentationLibrarySpans",
            "spans",
        ):
            if key in value:
                found = True
                yield from cls._spans(value[key], resource, scope)
        if not found:
            raise TypeError(f"{cls.host} input does not contain Spans")

    @classmethod
    def _actor_namespace(cls, resource, scope, trace_id):
        resource_attributes = cls._attributes(
            resource.get("attributes") if isinstance(resource, dict) else None
        )
        service_name = resource_attributes.get("service.name")
        if service_name is not None:
            return str(service_name)
        if isinstance(scope, dict):
            scope_name = scope.get("name")
            if scope_name is not None:
                return str(scope_name)
        return f"trace:{trace_id}"

    @classmethod
    def _attributes(cls, value):
        if value is None:
            return {}
        if isinstance(value, dict):
            return {str(key): cls._attribute_value(item) for key, item in value.items()}
        if isinstance(value, list):
            attributes = {}
            for entry in value:
                if not isinstance(entry, dict) or "key" not in entry:
                    raise TypeError(
                        f"{cls.host} attributes must contain key/value entries"
                    )
                attributes[str(entry["key"])] = cls._attribute_value(
                    entry.get("value")
                )
            return attributes
        raise TypeError(
            f"{cls.host} attributes must be a dict or key/value list"
        )

    @classmethod
    def _attribute_value(cls, value):
        if not isinstance(value, dict):
            return value
        for key in (
            "stringValue",
            "boolValue",
            "intValue",
            "doubleValue",
            "bytesValue",
        ):
            if key in value:
                return cls._attribute_value(value[key])
        return {str(key): cls._attribute_value(item) for key, item in value.items()}

    @staticmethod
    def _first(value, *names):
        for name in names:
            if value.get(name) is not None:
                return value[name]
        return None

    @classmethod
    def _span_id(cls, span):
        context = span.get("context") or {}
        value = cls._first(span, "span_id", "spanId")
        if value is None and isinstance(context, dict):
            value = cls._first(context, "span_id", "spanId")
        if value is None or not str(value):
            raise ValueError(f"{cls.host} Span requires a Span ID")
        return str(value)

    @classmethod
    def _trace_id(cls, span):
        context = span.get("context") or {}
        value = cls._first(span, "trace_id", "traceId")
        if value is None and isinstance(context, dict):
            value = cls._first(context, "trace_id", "traceId")
        if value is None or not str(value):
            raise ValueError(f"{cls.host} Span requires a Trace ID")
        return str(value)

    @classmethod
    def _parent_span_id(cls, span):
        value = cls._first(span, "parent_span_id", "parentSpanId")
        if value is None and isinstance(span.get("parent"), dict):
            value = cls._first(span["parent"], "span_id", "spanId")
        return None if value in {None, ""} else str(value)

    @staticmethod
    def _exec_id(trace_id, span_id):
        return json.dumps(
            [trace_id, span_id],
            ensure_ascii=False,
            separators=(",", ":"),
        )

    @staticmethod
    def _status(status):
        if isinstance(status, dict):
            status = status.get(
                "code",
                status.get("status_code", status.get("statusCode")),
            )
        if status is None:
            return None
        normalised = str(status).upper()
        if normalised in {"1", "OK", "STATUS_CODE_OK"}:
            return "succeeded"
        if normalised in {"2", "ERROR", "STATUS_CODE_ERROR"}:
            return "failed"
        if normalised in {"0", "UNSET", "STATUS_CODE_UNSET"}:
            return None
        return str(status)

    @classmethod
    def _plain(cls, value):
        if isinstance(value, dict):
            return {str(key): cls._plain(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [cls._plain(item) for item in value]
        if isinstance(value, Enum):
            return value.value
        if is_dataclass(value):
            return cls._plain(asdict(value))
        if hasattr(value, "to_dict") and callable(value.to_dict):
            return cls._plain(value.to_dict())
        if hasattr(value, "__attrs_attrs__"):
            return {
                field.name: cls._plain(getattr(value, field.name))
                for field in value.__attrs_attrs__
            }
        if isinstance(value, (str, int, float, bool)) or value is None:
            return value
        raise TypeError(
            f"Unsupported {cls.host} value: {type(value).__name__}"
        )


class OpenInferenceAdapter(OpenTelemetryAdapter):
    """Project only explicit OpenInference Span ancestry and local I/O."""

    host = "OpenInference"
    source = "openinference"

    def to_model(self):
        model = super().to_model()
        self._state_projection = {
            state_id: (
                "openinference",
                f"{trace_id}/{span_id}/{role}",
                0,
            )
            for state_id in model["S"]
            for trace_id, span_id, role in (json.loads(state_id),)
        }
        return model

    @classmethod
    def _span_model_evidence(cls, span, trace_id, span_id, exec_id):
        attributes = cls._attributes(span.get("attributes"))
        span_kind = attributes.get("openinference.span.kind")
        if not isinstance(span_kind, str) or not span_kind:
            raise ValueError(
                "OpenInference Span requires openinference.span.kind"
            )

        states = {}
        inputs = []
        outputs = []

        if "input.value" in attributes:
            value = cls._openinference_value(attributes, "input")
            state_id = cls._state_id(trace_id, span_id, "input")
            states[state_id] = {
                "value": value,
            }
            inputs.append([exec_id, state_id])

        if "output.value" in attributes:
            value = cls._openinference_value(attributes, "output")
            state_id = cls._state_id(trace_id, span_id, "output")
            states[state_id] = {
                "value": value,
            }
            outputs.append([exec_id, state_id])

        return states, inputs, outputs, {"span_kind": span_kind}

    @staticmethod
    def _state_id(trace_id, span_id, role):
        return json.dumps(
            [trace_id, span_id, role],
            ensure_ascii=False,
            separators=(",", ":"),
        )

    @staticmethod
    def _openinference_value(attributes, role):
        from tide_tooling.tooling_core.bundling.TIDEOpaqueValue import (
            TIDEOpaqueValue,
        )
        representation = attributes[f"{role}.value"]
        if not isinstance(representation, str):
            raise ValueError(
                f"OpenInference {role}.value must be a string"
            )
        mime_type = attributes.get(f"{role}.mime_type")
        if mime_type is not None and (
            not isinstance(mime_type, str) or not mime_type
        ):
            raise ValueError(
                f"OpenInference {role}.mime_type must be a non-empty string"
            )
        reason = "OpenInference recorded a serialised value"
        if mime_type is not None:
            reason += f" with MIME type {mime_type}"
        return TIDEOpaqueValue(
            type_name="TIDEUnknownType",
            representation=representation,
            reason=reason,
        )


__all__ = [
    "NOWAdapter",
    "NOWMeta",
    "OpenInferenceAdapter",
    "OpenLineageAdapter",
    "OpenTelemetryAdapter",
]
