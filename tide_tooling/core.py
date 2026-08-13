# tide_tooling/core.py
from tide_tooling.tooling_core.engine.classes import (
    DataRef,
    DELETE,
    DELETE_NOW,
    EMIT,
    END,
    Event,
    Effect,
    Node,
    NodeInput as NodeInput,
    Pipeline as Pipeline,
    RUN,
    RunnerNodeInput,
    WRITE,
    __TIDE_TEMPLATE_VALUE__,
)
from tide_tooling.tooling_core.engine.events import (
    Effects,
    LogEvents,
    RouterEvents as RouterEvents,
)
# from tide_tooling.tooling_core.engine.decorators import pipeline, node
from tide_tooling.tooling_core.engine.ocean import Ocean 
from tide_tooling.tooling_core.engine.tide_links import (
    ReadWriteLink as ReadWriteLink,
    StackLink as StackLink,
)
from tide_tooling.tooling_core.engine.tide_query_results import (
    Compared as Compared,
    Inspected as Inspected,
    Investigate as Investigate,
    Object as Object,
)
from tide_tooling.tooling_core.engine.tide_logging import Trace
from tide_tooling.tooling_core.engine.tide_records import (
    Actor,
    DependencyUseRecord,
    EffectRecord,
    ExecRecord,
    InputUseRecord,
    Inputs,
    WriteRecord,
)
from tide_tooling.tooling_core.engine.tide_rendering import (
    SHOW_BOLD,
    SHOW_CYAN,
    SHOW_DIM,
    SHOW_GREEN,
    SHOW_RED,
    SHOW_RESET,
    SHOW_SECTION_WIDTH,
    SHOW_WHITE,
    SHOW_YELLOW,
    _show_options,
    _show_render,
    strip_ansi as strip_ansi,
)
from tide_tooling.tooling_core.engine.events import ExecEvent
from collections import defaultdict
from contextlib import contextmanager
from functools import update_wrapper
from os import PathLike
from tide_tooling.tooling_core.engine.utils import (
    snapshot_json_dict,
    tide_snapshot_value,
    tide_value_hasher,
)
from typing import Any, Callable
from tide_tooling.tooling_core.engine.errors import (
    NoExecRecordError,
    PipelineDeadlockError,
)
from tide_tooling.tooling_core.engine.stores import MemoryKVStore
from tide_tooling.tooling_core.engine.decorators import current_pipeline_build
from tide_tooling.tooling_core import debug_print
import copy
import warnings


class TIDEModelReport(dict):
    """A mapping-compatible conformance report for ``K = (I, O)``."""

    @property
    def direct_handoffs(self):
        return tuple(
            (
                judgment["producer_exec_id"],
                judgment["state_id"],
                judgment["consumer_exec_id"],
            )
            for judgment in self.get("judgments", {}).get(
                "direct_handoffs",
                (),
            )
        )

    @property
    def reachability(self):
        return tuple(
            (
                judgment["source_exec_id"],
                judgment["target_exec_id"],
            )
            for judgment in self.get("judgments", {}).get(
                "reachability",
                (),
            )
        )

    def to_dict(self):
        return {
            key: value
            for key, value in self.items()
        }

    def show(self, color=True, simple=False, colour=None):
        color, simple = _show_options(
            color=color,
            simple=simple,
            colour=colour,
        )
        conforms = bool(self.get("conforms"))
        status = "VALID" if conforms else "INVALID"
        status_color = SHOW_GREEN if conforms else SHOW_RED
        counts = self.get("counts", {})
        host = self.get("host") or "unspecified"

        lines = [
            "",
            f"{SHOW_CYAN}{'=' * SHOW_SECTION_WIDTH}{SHOW_RESET}",
            f"{SHOW_CYAN}{SHOW_BOLD}TIDE MODEL{SHOW_RESET} | "
            f"{status_color}{status}{SHOW_RESET} | "
            f"{counts.get('executions', 0)} executions | "
            f"{counts.get('states', 0)} states",
            f"{SHOW_DIM}Host: {host} | "
            f"Schema: {self.get('schema', 'TIDE-Model')} "
            f"v{self.get('schema_version', 1)}{SHOW_RESET}",
            f"{SHOW_CYAN}{'=' * SHOW_SECTION_WIDTH}{SHOW_RESET}",
        ]

        if not simple:
            lines.append(f"{SHOW_DIM}CONFORMANCE:{SHOW_RESET}")
            for check, passed in self.get("checks", {}).items():
                label = check.replace("_", " ")
                if passed:
                    result = f"{SHOW_GREEN}pass{SHOW_RESET}"
                else:
                    result = (
                        f"{SHOW_YELLOW}not established{SHOW_RESET}"
                    )
                lines.append(f"  {label}: {result}")

            direct_handoffs = self.direct_handoffs
            reachability = self.reachability
            lines.extend([
                f"{SHOW_DIM}JUDGMENTS:{SHOW_RESET}",
                f"  Direct handoffs: {len(direct_handoffs)}",
            ])
            if direct_handoffs:
                for producer, state, consumer in direct_handoffs:
                    lines.append(
                        f"    {SHOW_WHITE}{producer}{SHOW_RESET} "
                        f"--{SHOW_CYAN}{state}{SHOW_RESET}--> "
                        f"{SHOW_WHITE}{consumer}{SHOW_RESET}"
                    )
            else:
                lines.append(f"    {SHOW_DIM}none{SHOW_RESET}")

            lines.append(f"  Reachability: {len(reachability)}")
            if reachability:
                for source, target in reachability:
                    lines.append(
                        f"    {SHOW_WHITE}{source}{SHOW_RESET} "
                        f"\u2192 {SHOW_WHITE}{target}{SHOW_RESET}"
                    )
            else:
                lines.append(f"    {SHOW_DIM}none{SHOW_RESET}")

            lines.append(f"{SHOW_DIM}ERRORS:{SHOW_RESET}")
            errors = self.get("errors", ())
            if errors:
                for error in errors:
                    lines.append(f"  {SHOW_RED}{error}{SHOW_RESET}")
            else:
                lines.append(f"  {SHOW_DIM}none{SHOW_RESET}")

        return _show_render(lines, color=color)


class _TIDEExecFacade:
    """Keep ``tide.exec(...)`` callable while also supporting ``with`` builds."""

    def __init__(self, tide, execute):
        self._tide = tide
        self._execute = execute
        self._context_tokens = []
        update_wrapper(self, execute)

    def __call__(self, *args, **kwargs):
        return self._execute(*args, **kwargs)

    def __enter__(self):
        token = current_pipeline_build.set([])
        self._context_tokens.append(token)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        token = self._context_tokens.pop()
        current_pipeline_build.reset(token)
        return False


class TIDE:
    seq_id = -1
    
    def __init__(
        self,
        default_store="global",
        timeout=None,
        max_idle_attempts=400000,
        track_unrepresented=True,
    ):
        self.ocean = Ocean(
            timeout=timeout,
            max_idle_attempts=max_idle_attempts,
            track_unrepresented=track_unrepresented,
        )
        self._default_store = None
        self.tide_exec_log = ExecutionLog(self.ocean.id)
        self._event_capture = EventCapture(self.ocean, self.tide_exec_log)
        self.event = self._event_capture.event
        self.set_default_store(default_store)
        # The facade preserves the existing call API and adds:
        # ``with tide.exec as execute: execute(node_graph)``.
        self.exec = _TIDEExecFacade(self, self.exec)

    def close(self, wait=True):
        """Release resources owned by this TIDE instance."""
        self.ocean.close(wait=wait)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        return False

    @staticmethod
    def _validate_store_name(store):
        if not isinstance(store, str) or not store.strip():
            raise ValueError("default_store must be a non-empty string")
        return store

    def _ensure_store(self, store):
        store = self._validate_store_name(store)
        if not self.ocean.store_registry.exists(store):
            self.ocean.register_store(
                name=store,
                store_class=MemoryKVStore,
                scope="global",
            )
        return store

    @property
    def default_store(self):
        return self._default_store

    def set_default_store(self, store):
        """Select and prepare the store used by unscoped TIDE references."""
        store = self._ensure_store(store)
        self._default_store = store
        self._event_capture.set_default_store(store)
        self.tide_exec_log.fallback_store = store
        return self

    @contextmanager
    def _store_context(self, store=None):
        selected_store = self._validate_store_name(store or self._default_store)
        token = DataRef.set_default_store(selected_store)
        try:
            yield selected_store
        finally:
            DataRef.reset_default_store(token)
        

    def _next_exec_record_seq_id(self):
        self.seq_id += 1
        return self.seq_id

    def _is_preruntime_polling_record(self, record):
        if record.producer_id.source_actor == "TIDE: Polling":
            return True

        for write in record.writes:
            if write.cause == "TIDE: Pre-runtime Polling":
                return True

        return False
    
    def register_store(self, name:str=None, store_class=MemoryKVStore, scope:str="global", store_template:Any=None):
        return self.ocean.register_store(name=name, store_class=store_class, scope=scope, store_template=store_template)

    def exec(
        self,
        func: Callable | Node,
        args: list | tuple | None = None,
        kwargs: dict | None = None,
        writes: str | list[str] | tuple[str, ...] | None = None,
        default_store: str | None = None,
        reads: str | list[str] | tuple[str, ...] | None = None,
        p_args: tuple | None = None,
        p_kwargs: dict | None = None,
        k_args: tuple | None = None,
        k_kwargs: dict | None = None,
        effects=None,
        return_id: bool = False,
    ):
        """Execute one callable and optionally write its returned outputs.

        ``writes`` names the reference or ordered references that receive the
        callable's returned output. It does not accept a reference-to-value
        dictionary. Pass supplied values to ``TIDE.event(..., writes={...})``;
        use a ``WRITE`` effect for an additional write emitted while the
        callable runs.
        """
        selected_store = self._ensure_store(default_store or self._default_store)
        with self._store_context(selected_store):
            return self._exec(
                func,
                args=args,
                kwargs=kwargs,
                writes=writes,
                default_store=selected_store,
                reads=reads,
                p_args=p_args,
                p_kwargs=p_kwargs,
                k_args=k_args,
                k_kwargs=k_kwargs,
                effects=effects,
                return_id=return_id,
            )

    def _exec(
        self,
        func: Callable | Node,
        args: list | tuple | None = None,
        kwargs: dict | None = None,
        writes: str | list[str] | tuple[str, ...] | None = None,
        default_store: str | None = None,
        reads: str | list[str] | tuple[str, ...] | None = None,
        p_args: tuple | None = None,
        p_kwargs: dict | None = None,
        k_args: tuple | None = None,
        k_kwargs: dict | None = None,
        effects=None,
        return_id: bool = False,
    ):
        if isinstance(func, Node):
            node_definition = func
            func = node_definition.func
            if reads is None:
                args = node_definition.args.raw if args is None else args
                kwargs = node_definition.kwargs.raw if kwargs is None else kwargs
            p_args = node_definition.p_args.raw if p_args is None else p_args
            p_kwargs = node_definition.p_kwargs.raw if p_kwargs is None else p_kwargs
            k_args = node_definition.k_args.raw if k_args is None else k_args
            k_kwargs = node_definition.k_kwargs.raw if k_kwargs is None else k_kwargs
            effects = node_definition.effects if effects is None else effects
            if writes is None:
                writes = [DataRef.to_string(target, strip_leading=True) for target in node_definition.targets]
        elif getattr(func, "__tide_node_wrapper__", False):
            if effects is None:
                effects = getattr(func, "__tide_node_effects__", None)
            func = func.__tide_node_func__

        if reads is not None and args is not None or reads is not None and kwargs is not None:
            raise AttributeError("Cannot use both reads and args/kwargs for one object")
        
        if reads is not None and not isinstance(reads, (list, tuple)):
            reads = [reads]

        if reads is not None:
            args = ["@" + ref for ref in reads]

        if not isinstance(func, Callable):
            raise RuntimeError(f"tide.exec() was called with a non callable func! the suspect - {type(func)}")
        seq_id = self._next_exec_record_seq_id() # tick event seq_id
        args = args or ()
        kwargs = kwargs or {}
        writes = [] if writes is None else writes
        if isinstance(writes, dict):
            raise TypeError(
                "tide.exec writes must be a reference or a list/tuple of "
                "references; use TIDE.event(writes={...}) for supplied "
                "values or a WRITE effect for a write emitted at runtime"
            )
        if not isinstance(writes, (list, tuple)):
            writes = [writes]
        writes_with_store = []

        if not isinstance(args, (list, tuple)):
            args = (args, )
        # for arg in args :
        #     if isinstance(arg, str):
        #         if DataRef._is_dep_string(arg):
        #             if ":" not in arg:
        #                 arg = arg.split("@", 1)[1]
        #                 arg = DataRef(arg, default_store)
        #             else:
        #                 arg = DataRef.from_string(arg)
        #     norm_args.append(arg)
        args = tuple(args)
        for str_ref in writes:
            if not isinstance(str_ref, str):
                raise TypeError(
                    "tide.exec writes must contain only reference strings"
                )
            if ":" not in str_ref:
                writes_with_store.append(f"{default_store}:{str_ref}")
            else:
                writes_with_store.append(str_ref)
            
        tide_node = Node(func, args=args, kwargs=kwargs, p_args=p_args, p_kwargs=p_kwargs, k_args=k_args, k_kwargs=k_kwargs, targets=writes_with_store, effects=effects)
        
        try:
            node_trace, exec_record, output = self.ocean.run_node(
                tide_node,
                pipeline_name=f"TIDE: .exec({tide_node.node_name}[{seq_id}])",
                overload_runner_source=f"TIDE: .exec({seq_id})",
                show=False
                )
        except PipelineDeadlockError as error:
            failed_trace = getattr(error, "tide_trace", None)
            if failed_trace is not None:
                self._event_capture.events.extend(failed_trace.events)
            wrapped_error = NoExecRecordError(
                f"{func.__name__} did not execute because its dependencies "
                "could not be resolved"
            )
            if failed_trace is not None:
                wrapped_error.tide_trace = failed_trace
            raise wrapped_error from error
        
        self._event_capture.events.extend(node_trace.events)

        if not exec_record:
            raise NoExecRecordError(
                f"{func.__name__} did not execute or has no exec_record!"
            )

        if self.ocean.thread:
            self.ocean.thread.join()

        imported_records = [
            record
            for record in node_trace.exec_log
            if record.exec_id not in self._event_capture.exec_log.exec_id_to_log
        ]

        polling_records = [
            record
            for record in imported_records
            if self._is_preruntime_polling_record(record)
        ]

        normal_records = [
            record
            for record in imported_records
            if not self._is_preruntime_polling_record(record)
        ]
        
        debug_print("\n--- IMPORT PARTITION ---")

        for imported_record in [*polling_records, *normal_records]:
            # print(
            #     "[BEFORE LOG]",
            #     "actor=", imported_record.producer_id.source_actor,
            #     "old_seq=", imported_record.seq_id,
            #     "writes=", [(w.stored_target, w.cause) for w in imported_record.writes],
            # )

       
            imported_record = self._event_capture.exec_log._log_exec_event(imported_record)

            # print(
            #     "[AFTER LOG]",
            #     "actor=", imported_record.producer_id.source_actor,
            #     "new_seq=", imported_record.seq_id,
            #     "writes=", [(w.stored_target, w.cause) for w in imported_record.writes],
            # )
            
            # print("[CAPTURE IDS]", [(
            #     r.seq_id,
            #     r.exec_id,
            #     r.producer_id.source_actor,
            #     [(w.stored_target, w.cause) for w in r.writes],
            #     )
            #     for r in self._event_capture.exec_log.exec_log
            #     ])


            self._event_capture.events.append(imported_record)
            self._event_capture.exec_log.exec_log.append(imported_record)

            
            # print("[IMPORTED IDS]", [(
            #     r.seq_id,
            #     r.exec_id,
            #     r.producer_id.source_actor,
            #     [(w.stored_target, w.cause) for w in r.writes],
            #     r.exec_id in self._event_capture.exec_log.exec_id_to_log,
            #     )
            #     for r in node_trace.exec_log
            #     ])

            # print(
            #     "[CAPTURE STATE AFTER APPEND]",
            #     "len=", len(self._event_capture.exec_log.exec_log),
            #     "seqs=", [
            #         (r.seq_id, r.producer_id.source_actor)
            #         for r in self._event_capture.exec_log.exec_log
            #     ],
            # )

        if return_id:
            return (output, seq_id)
        return output

    def declare(self, ref, value, return_id=False):
        """Declare a user-owned value into the current default store."""
        return self.event(
            "TIDE_USER",
            writes={ref: value},
            cause="declared",
            return_id=return_id,
        )



    def trace(self, name:str=None) -> Trace:
        if name is None:
            name = "TIDE: .trace()"
        return self._event_capture.trace(name)

    @classmethod
    def from_model(
        cls,
        model,
        *,
        host=None,
        name="TIDE: .from_model()",
        identity_mode="host",
        assign_unknown=False,
    ) -> Trace:
        """Project a conforming ``K = (I, O)`` document into a Trace.

        By default, imported ExecRecords retain each host-supplied Execution
        identifier exactly. ``identity_mode="structural"`` instead gives the
        Trace deterministic occurrence aliases for local comparison; those
        aliases are a view and do not assert cross-host correspondence.

        ``state_id`` is the interoperable State identity. Internal TIDE
        references use that exact identifier beneath the ``__model__`` scope
        so host identity remains visible and correlatable on the ordinary
        Trace query surface. The scope is a tool projection and is not part of
        the model contract. Producerless States are invalid by default.
        ``assign_unknown=True`` warns and creates a tool-side ``unknown``
        producer without asserting that the host supplied that provenance.
        """
        if identity_mode == "canonical":
            identity_mode = "structural"
        if identity_mode not in {"host", "structural"}:
            raise ValueError(
                "identity_mode must be 'host' or 'structural'"
            )
        if not isinstance(assign_unknown, bool):
            raise TypeError("assign_unknown must be a bool")
        if not isinstance(model, dict):
            received = type(model).__name__
            trace_guidance = (
                ". Call trace.to_model(...) if a model document is required"
                if isinstance(model, Trace)
                else ""
            )
            raise TypeError(
                "TIDE.from_model() requires a dict containing I and O; "
                f"received {received}{trace_guidance}"
            )

        model_document = model
        normalised, report = cls._normalise_model_document(
            model_document,
            host=host,
        )
        assigned_unknown_states = ()
        if (
            not report["conforms"]
            and assign_unknown
            and any(
                error.startswith("every_state_has_producer:")
                for error in report["errors"]
            )
        ):
            repaired_model, assigned_unknown_states = (
                cls._model_assign_unknown_producer(model_document)
            )
            model_document = repaired_model
            normalised, report = cls._normalise_model_document(
                model_document,
                host=host,
            )
            if report["conforms"]:
                warnings.warn(
                    "TIDE Model States have no represented producer: "
                    + ", ".join(repr(state) for state in assigned_unknown_states)
                    + ". assign_unknown=True introduced the tool-side "
                    "Execution 'unknown'; this does not establish host "
                    "provenance or make the original model conforming.",
                    RuntimeWarning,
                    stacklevel=2,
                )
        if not report["conforms"]:
            suggestion = (
                "; pass assign_unknown=True only if a tool-side unknown "
                "producer is acceptable"
                if any(
                    error.startswith("every_state_has_producer:")
                    for error in report["errors"]
                )
                else ""
            )
            raise ValueError(
                "TIDE Model conformance failed: "
                + "; ".join(report["errors"])
                + suggestion
            )

        if identity_mode == "structural":
            from tide_tooling.tooling_core.engine.tide_model import _canonical_tide_structure

            model_document, identity_maps = _canonical_tide_structure(
                model_document,
                return_identity_maps=True,
            )
            assigned_unknown_states = tuple(
                identity_maps["states"][(type(state_id), state_id)]
                for state_id in assigned_unknown_states
            )
            normalised, report = cls._normalise_model_document(
                model_document,
                host=host,
            )
            if not report["conforms"]:
                raise ValueError(
                    "TIDE structural model conformance failed: "
                    + "; ".join(report["errors"])
                )

        tide = cls()
        try:
            return tide._trace_from_model(
                normalised,
                name=name,
                identity_mode=identity_mode,
                assigned_unknown_states=assigned_unknown_states,
            )
        finally:
            tide.close()

    @classmethod
    def validate_model(cls, model, *, host=None):
        """Return a projection-relative TIDE Model conformance report."""
        _, report = cls._normalise_model_document(model, host=host)
        return report

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
        """Export this instance's Trace as the interoperable TIDE Model."""
        return self.trace().to_model(
            host=host,
            include_metadata=include_metadata,
            include_values=include_values,
            include_hashes=include_hashes,
            incidence_format=incidence_format,
            identity_mode=identity_mode,
            assign_unknown=assign_unknown,
        )

    def comparator(self, target, *, execution_map, state_map):
        """Compare this TIDE Trace under explicit identity maps."""
        from tide_tooling.comparator import comparator

        return comparator(
            self.trace(),
            target,
            execution_map=execution_map,
            state_map=state_map,
        )

    @classmethod
    def _model_assign_unknown_producer(cls, model):
        """Return a copied model with missing producers assigned to ``unknown``."""
        repaired = copy.deepcopy(model)
        inputs = cls._model_list(repaired["I"], "I")
        outputs = cls._model_list(repaired["O"], "O")

        represented_states = {}
        for relation, entries in (("I", inputs), ("O", outputs)):
            for index, entry in enumerate(entries):
                _, state_id = cls._model_incidence_pair(
                    entry,
                    relation,
                    index,
                )
                state_id = cls._model_identifier(
                    state_id,
                    f"{relation}[{index}].state",
                )
                represented_states[(type(state_id), state_id)] = state_id

        produced_state_keys = set()
        for index, entry in enumerate(outputs):
            _, state_id = cls._model_incidence_pair(entry, "O", index)
            state_id = cls._model_identifier(
                state_id,
                f"O[{index}].state",
            )
            produced_state_keys.add((type(state_id), state_id))

        missing_states = tuple(
            represented_states[key]
            for key in sorted(
                represented_states.keys() - produced_state_keys,
                key=lambda item: cls._model_identity_sort_key(item[1]),
            )
        )
        if not missing_states:
            return repaired, ()

        represented_executions = {}
        for relation, entries in (("I", inputs), ("O", outputs)):
            for index, entry in enumerate(entries):
                exec_id, _ = cls._model_incidence_pair(
                    entry,
                    relation,
                    index,
                )
                exec_id = cls._model_identifier(
                    exec_id,
                    f"{relation}[{index}].execution",
                )
                represented_executions[(type(exec_id), exec_id)] = exec_id
        if any(
            type(exec_id) is str and exec_id == "unknown"
            for exec_id in represented_executions.values()
        ):
            raise ValueError(
                "assign_unknown cannot infer producers because the model "
                "already represents an Execution with ID 'unknown'"
            )
        outputs.extend(
            {"exec_id": "unknown", "state_id": state_id}
            for state_id in missing_states
        )
        if "E" in repaired:
            executions = cls._model_list(repaired["E"], "E")
            executions.append("unknown")
            repaired["E"] = executions
        repaired["O"] = outputs
        return repaired, missing_states

    @staticmethod
    def _model_identifier(value, location):
        if isinstance(value, bool) or not isinstance(value, (str, int)):
            raise ValueError(
                f"{location} must be a non-empty string or integer"
            )
        if isinstance(value, str) and not value.strip():
            raise ValueError(f"{location} cannot be empty")
        return value

    @staticmethod
    def _model_list(value, location):
        if not isinstance(value, (list, tuple)):
            raise TypeError(f"{location} must be a list")
        return list(value)

    @staticmethod
    def _model_identity_sort_key(value):
        return type(value).__name__, repr(value)

    @staticmethod
    def _model_incidence_pair(entry, relation, index):
        if isinstance(entry, dict):
            has_explicit_key = (
                "exec_id" in entry or "state_id" in entry
            )
            has_compact_key = "e" in entry or "s" in entry
            if has_explicit_key and has_compact_key:
                raise TypeError(
                    f"{relation}[{index}] cannot mix compact "
                    "{e, s} and explicit "
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
    def _normalise_model_incidence(cls, entries, relation):
        incidence_by_key = {}
        for index, entry in enumerate(entries):
            exec_id, state_id = cls._model_incidence_pair(
                entry,
                relation,
                index,
            )
            exec_id = cls._model_identifier(
                exec_id,
                f"{relation}[{index}].execution",
            )
            state_id = cls._model_identifier(
                state_id,
                f"{relation}[{index}].state",
            )
            key = (
                (type(exec_id), exec_id),
                (type(state_id), state_id),
            )
            incidence_by_key.setdefault(key, (exec_id, state_id))

        return sorted(
            incidence_by_key.values(),
            key=lambda edge: (
                cls._model_identity_sort_key(edge[0]),
                cls._model_identity_sort_key(edge[1]),
            ),
        )

    @classmethod
    def _model_compatibility_carrier(
        cls,
        model,
        field,
        derived,
        identity_name,
    ):
        if field not in model:
            return

        entries = cls._model_list(model[field], field)
        asserted = {}
        for index, entry in enumerate(entries):
            identity = cls._model_identifier(entry, f"{field}[{index}]")
            asserted[(type(identity), identity)] = identity
        derived_by_key = {
            (type(identity), identity): identity
            for identity in derived
        }
        if asserted.keys() != derived_by_key.keys():
            raise ValueError(
                f"{field} compatibility carrier must equal the "
                f"{identity_name} identities derived from I and O"
            )

    @staticmethod
    def _model_enrichments(value, location):
        if value is None:
            return {}
        if not isinstance(value, dict):
            raise TypeError(f"{location} must be a dict keyed by model ID")

        enrichments = {}
        for identity, enrichment in value.items():
            if enrichment is None:
                enrichment = {}
            if not isinstance(enrichment, dict):
                raise TypeError(
                    f"{location}[{identity!r}] must be a dict"
                )
            enrichments[identity] = dict(enrichment)
        return enrichments

    @classmethod
    def _normalise_model_document(cls, model, *, host=None):
        report = TIDEModelReport({
            "conforms": False,
            "schema": "TIDE-Model",
            "schema_version": 1,
            "host": host,
            "checks": {
                "kernel_shape": False,
                "identity": False,
                "incidence": False,
                "unique_creation": False,
                "every_state_has_producer": False,
                "acyclic_occurrence_ancestry": False,
            },
            "counts": {
                "executions": 0,
                "states": 0,
                "inputs": 0,
                "outputs": 0,
                "direct_handoffs": 0,
                "reachability": 0,
            },
            "judgments": {
                "direct_handoffs": [],
                "reachability": [],
            },
            "errors": [],
        })

        if not isinstance(model, dict):
            report["errors"].append("model must be a dict containing I and O")
            return None, report

        missing_fields = [
            field_name
            for field_name in ("I", "O")
            if field_name not in model
        ]
        if missing_fields:
            report["errors"].append(
                "kernel_shape: missing " + ", ".join(missing_fields)
            )
            return None, report

        schema = model.get("schema", "TIDE-Model")
        schema_version = model.get("schema_version", 1)
        model_host = model.get("host")
        if schema != "TIDE-Model":
            report["errors"].append(
                f"kernel_shape: unsupported schema {schema!r}"
            )
        if (
            isinstance(schema_version, bool)
            or not isinstance(schema_version, int)
            or schema_version != 1
        ):
            report["errors"].append(
                f"kernel_shape: unsupported schema_version {schema_version!r}"
            )
        if host is not None and model_host is not None and host != model_host:
            report["errors"].append(
                "kernel_shape: host argument conflicts with model host"
            )
        selected_host = host if host is not None else model_host
        if selected_host is not None and (
            not isinstance(selected_host, str) or not selected_host.strip()
        ):
            report["errors"].append(
                "kernel_shape: host must be a non-empty string"
            )
        report["host"] = selected_host

        host_record = model.get("host_record")
        if host_record is not None:
            try:
                host_record = snapshot_json_dict(
                    host_record,
                    location="host_record",
                )
            except (TypeError, ValueError) as error:
                report["errors"].append(f"kernel_shape: {error}")

        try:
            raw_inputs = cls._model_list(model["I"], "I")
            raw_outputs = cls._model_list(model["O"], "O")
            execution_enrichments = cls._model_enrichments(
                model.get("executions"),
                "executions",
            )
            state_enrichments = cls._model_enrichments(
                model.get("states"),
                "states",
            )
        except (TypeError, ValueError) as error:
            report["errors"].append(f"kernel_shape: {error}")
            return None, report

        if report["errors"]:
            return None, report
        report["checks"]["kernel_shape"] = True

        try:
            inputs = cls._normalise_model_incidence(raw_inputs, "I")
            outputs = cls._normalise_model_incidence(raw_outputs, "O")
        except (TypeError, ValueError) as error:
            report["errors"].append(f"incidence: {error}")
            return None, report

        report["checks"]["incidence"] = True
        report["counts"]["inputs"] = len(inputs)
        report["counts"]["outputs"] = len(outputs)

        execution_id_by_key = {}
        state_id_by_key = {}
        for exec_id, state_id in inputs + outputs:
            execution_id_by_key.setdefault(
                (type(exec_id), exec_id),
                exec_id,
            )
            state_id_by_key.setdefault(
                (type(state_id), state_id),
                state_id,
            )
        execution_ids = sorted(
            execution_id_by_key.values(),
            key=cls._model_identity_sort_key,
        )
        state_ids = sorted(
            state_id_by_key.values(),
            key=cls._model_identity_sort_key,
        )

        executions = []
        execution_by_id = {}
        states = []
        state_by_id = {}

        try:
            cls._model_compatibility_carrier(
                model,
                "E",
                execution_ids,
                "Execution",
            )
            cls._model_compatibility_carrier(
                model,
                "S",
                state_ids,
                "State",
            )

            for index, exec_id in enumerate(execution_ids):
                source = dict(execution_enrichments.get(exec_id, {}))
                source["exec_id"] = exec_id
                execution = {
                    "exec_id": exec_id,
                    "source": source,
                    "occurrence": index,
                }
                actor = source.get("actor")
                if isinstance(actor, dict):
                    actor = actor.get("name", actor.get("actor"))
                if actor is not None and (
                    not isinstance(actor, str) or not actor
                ):
                    raise ValueError(
                        f"executions[{exec_id!r}].actor must have a "
                        "non-empty name"
                    )
                executions.append(execution)
                execution_by_id[exec_id] = execution

            for index, state_id in enumerate(state_ids):
                source = dict(state_enrichments.get(state_id, {}))
                source["state_id"] = state_id
                value = source.get("value", __TIDE_TEMPLATE_VALUE__())
                value_hash = source.get("value_hash")
                if value_hash is not None:
                    hash_prefix = "sha3_512_tide:"
                    hash_body = (
                        value_hash[len(hash_prefix):]
                        if isinstance(value_hash, str)
                        and value_hash.startswith(hash_prefix)
                        else ""
                    )
                    if (
                        len(hash_body) != 128
                        or any(
                            character not in "0123456789abcdef"
                            for character in hash_body
                        )
                    ):
                        raise ValueError(
                            f"states[{state_id!r}].value_hash must be a "
                            "SHA3-512 TIDE hash"
                        )
                if not isinstance(value, __TIDE_TEMPLATE_VALUE__):
                    computed_hash = tide_value_hasher(value)
                    if value_hash is not None and value_hash != computed_hash:
                        raise ValueError(
                            f"State {state_id!r} value does not match value_hash"
                        )
                    value_hash = computed_hash
                state = {
                    "state_id": state_id,
                    "source": source,
                    "occurrence": index,
                    "value": value,
                    "value_hash": value_hash,
                }
                states.append(state)
                state_by_id[state_id] = state

            unknown_exec_enrichments = [
                exec_id
                for exec_id in execution_enrichments
                if exec_id not in execution_by_id
            ]
            if unknown_exec_enrichments:
                raise ValueError(
                    "executions enrichment references unknown exec_id "
                    f"{unknown_exec_enrichments[0]!r}"
                )

            unknown_state_enrichments = [
                state_id
                for state_id in state_enrichments
                if state_id not in state_by_id
            ]
            if unknown_state_enrichments:
                raise ValueError(
                    "states enrichment references unknown state_id "
                    f"{unknown_state_enrichments[0]!r}"
                )
        except (TypeError, ValueError) as error:
            report["errors"].append(f"identity: {error}")
            return None, report

        report["checks"]["identity"] = True
        report["counts"]["executions"] = len(executions)
        report["counts"]["states"] = len(states)

        producer_by_state = {}
        for exec_id, state_id in outputs:
            if state_id in producer_by_state:
                prior = producer_by_state[state_id]
                report["errors"].append(
                    "unique_creation: State "
                    f"{state_id!r} is output by {prior!r} and {exec_id!r}"
                )
                return None, report
            producer_by_state[state_id] = exec_id
        report["checks"]["unique_creation"] = True

        states_without_producer = [
            state["state_id"]
            for state in states
            if state["state_id"] not in producer_by_state
        ]
        if states_without_producer:
            state_id = states_without_producer[0]
            report["errors"].append(
                "every_state_has_producer: State "
                f"{state_id!r} has no represented producer in O"
            )
            return None, report
        report["checks"]["every_state_has_producer"] = True

        adjacency = {execution["exec_id"]: set() for execution in executions}
        indegree = {execution["exec_id"]: 0 for execution in executions}
        direct_handoffs = []
        for consumer_exec_id, state_id in inputs:
            producer_exec_id = producer_by_state[state_id]
            direct_handoffs.append({
                "producer_exec_id": producer_exec_id,
                "state_id": state_id,
                "consumer_exec_id": consumer_exec_id,
            })
            if consumer_exec_id not in adjacency[producer_exec_id]:
                adjacency[producer_exec_id].add(consumer_exec_id)
                indegree[consumer_exec_id] += 1

        ready = [
            execution["exec_id"]
            for execution in executions
            if indegree[execution["exec_id"]] == 0
        ]
        topological_execution_ids = []
        visited = 0
        while ready:
            ready.sort(
                key=cls._model_identity_sort_key,
                reverse=True,
            )
            exec_id = ready.pop()
            topological_execution_ids.append(exec_id)
            visited += 1
            for consumer_exec_id in sorted(
                adjacency[exec_id],
                key=cls._model_identity_sort_key,
            ):
                indegree[consumer_exec_id] -= 1
                if indegree[consumer_exec_id] == 0:
                    ready.append(consumer_exec_id)

        if visited != len(executions):
            report["errors"].append(
                "acyclic_occurrence_ancestry: incidence contains a cycle"
            )
            return None, report

        reachability = []
        execution_ids = [
            execution["exec_id"]
            for execution in executions
        ]
        for source_exec_id in execution_ids:
            reachable = set()
            pending = list(adjacency[source_exec_id])
            while pending:
                target_exec_id = pending.pop()
                if target_exec_id in reachable:
                    continue
                reachable.add(target_exec_id)
                pending.extend(adjacency[target_exec_id])

            for target_exec_id in execution_ids:
                if target_exec_id in reachable:
                    reachability.append({
                        "source_exec_id": source_exec_id,
                        "target_exec_id": target_exec_id,
                    })

        report["checks"]["acyclic_occurrence_ancestry"] = True
        report["judgments"]["direct_handoffs"] = direct_handoffs
        report["judgments"]["reachability"] = reachability
        report["counts"]["direct_handoffs"] = len(direct_handoffs)
        report["counts"]["reachability"] = len(reachability)
        report["conforms"] = True

        inputs_by_exec = defaultdict(list)
        outputs_by_exec = defaultdict(list)
        for exec_id, state_id in inputs:
            inputs_by_exec[exec_id].append(state_id)
        for exec_id, state_id in outputs:
            outputs_by_exec[exec_id].append(state_id)

        return {
            "schema": "TIDE-Model",
            "schema_version": 1,
            "host": selected_host,
            "executions": executions,
            "execution_by_id": execution_by_id,
            "states": states,
            "state_by_id": state_by_id,
            "inputs": inputs,
            "outputs": outputs,
            "inputs_by_exec": inputs_by_exec,
            "outputs_by_exec": outputs_by_exec,
            "producer_by_state": producer_by_state,
            "topological_execution_ids": topological_execution_ids,
            "execution_enrichments": execution_enrichments,
            "state_enrichments": state_enrichments,
            "host_record": host_record,
        }, report

    def _trace_from_model(
        self,
        model,
        *,
        name,
        identity_mode,
        assigned_unknown_states=(),
    ):
        state_projection = {}
        value_by_state = {}
        state_id_by_reference = {}
        for state in model["states"]:
            state_id = state["state_id"]
            data_ref = DataRef.literal(
                key=state_id,
                store="__model__",
            )
            reference = DataRef.to_string(
                data_ref,
                strip_leading=True,
            )
            prior_state_id = state_id_by_reference.get(reference)
            if (
                prior_state_id is not None
                and (
                    type(prior_state_id) is not type(state_id)
                    or prior_state_id != state_id
                )
            ):
                raise ValueError(
                    "TIDE Model State identifiers "
                    f"{prior_state_id!r} and {state_id!r} project to the same "
                    f"Trace reference {reference!r}"
                )
            state_id_by_reference[reference] = state_id
            state_projection[state_id] = {
                "state_id": state_id,
                "ref": data_ref,
                "reference": reference,
                "version": 0,
            }
            value_by_state[state_id] = self._materialise_model_state(
                state_id,
                state,
            )

        executions = []
        assigned_unknown_state_keys = {
            (type(state_id), state_id)
            for state_id in assigned_unknown_states
        }
        ordered_source_executions = [
            model["execution_by_id"][exec_id]
            for exec_id in model["topological_execution_ids"]
        ]
        for seq_id, source_execution in enumerate(
            ordered_source_executions
        ):
            source = source_execution["source"]
            raw_exec_id = source_execution["exec_id"]
            actor, ocean_id, runner_id, actor_supplied = (
                self._model_actor_fields(
                    source,
                    seq_id,
                    model["host"],
                    raw_exec_id,
                    identity_mode,
            ))
            trace_exec_id = (
                raw_exec_id
                if identity_mode == "host"
                else f"exec_{seq_id}"
            )
            executions.append({
                "source": source,
                "seq_id": seq_id,
                "raw_exec_id": raw_exec_id,
                "exec_id": trace_exec_id,
                "actor": actor,
                "actor_supplied": actor_supplied,
                "identity_mode": identity_mode,
                "assigned_unknown": any(
                    (type(state_id), state_id)
                    in assigned_unknown_state_keys
                    for state_id in model["outputs_by_exec"][raw_exec_id]
                ),
                "ocean_id": ocean_id,
                "runner_id": runner_id,
                "pipeline_id": source.get(
                    "pipeline_id",
                    "TIDE: .from_model()",
                ),
                "inputs": [
                    state_projection[state_id]
                    for state_id in model["inputs_by_exec"][raw_exec_id]
                ],
                "outputs": [
                    state_projection[state_id]
                    for state_id in model["outputs_by_exec"][raw_exec_id]
                ],
            })

        execution_by_id = {
            execution["raw_exec_id"]: execution
            for execution in executions
        }
        producer_by_state = {
            state_id: execution_by_id[exec_id]
            for state_id, exec_id in model["producer_by_state"].items()
        }
        records = [
            self._model_exec_record(
                execution,
                value_by_state,
                producer_by_state,
            )
            for execution in executions
        ]
        source_context = [
            {
                "run": execution["runner_id"],
                "pipeline_id": execution["pipeline_id"],
                "pipeline": execution["source"].get(
                    "pipeline",
                    execution["pipeline_id"],
                ),
            }
            for execution in executions
        ]
        model_context = {
            "schema": model["schema"],
            "schema_version": model["schema_version"],
            "host": model["host"],
            "identity_mode": identity_mode,
            "assigned_unknown_states": list(assigned_unknown_states),
            "execution_ids": [
                execution["exec_id"]
                for execution in ordered_source_executions
            ],
            "state_ids": [
                state["state_id"]
                for state in model["states"]
            ],
            "I": [
                {"exec_id": exec_id, "state_id": state_id}
                for exec_id, state_id in model["inputs"]
            ],
            "O": [
                {"exec_id": exec_id, "state_id": state_id}
                for exec_id, state_id in model["outputs"]
            ],
            "execution_enrichment": [
                {
                    "exec_id": execution["exec_id"],
                    "metadata": {
                        key: value
                        for key, value in execution["source"].items()
                        if key != "exec_id"
                    },
                }
                for execution in ordered_source_executions
                if len(execution["source"]) > 1
            ],
            "state_enrichment": [
                {
                    "state_id": state["state_id"],
                    "value_hash": state["value_hash"],
                }
                for state in model["states"]
                if state["value_hash"] is not None
            ],
            "state_projection": [
                {
                    "state_id": state_id,
                    "reference": projection["reference"],
                    "version": projection["version"],
                }
                for state_id, projection in state_projection.items()
            ],
        }

        return Trace(
            event_log=[],
            exec_log=records,
            runner="TIDE: .from_model()",
            ocean_id=self.ocean.id,
            name=name,
            source_context=source_context,
            model_context=model_context,
            host=model["host"],
            host_record=model.get("host_record"),
        )

    @staticmethod
    def _materialise_model_state(state_id, evidence):
        from tide_tooling.tooling_core.bundling.TIDEOpaqueValue import TIDEOpaqueValue

        value = evidence["value"]
        value_hash = evidence["value_hash"]
        if not isinstance(value, __TIDE_TEMPLATE_VALUE__):
            snapshot = tide_snapshot_value(value, boundary="model")
            if value_hash is not None and snapshot.value_hash != value_hash:
                raise ValueError(
                    f"TIDE Model State {state_id!r} changed during capture"
                )
            return snapshot.materialise()

        opaque = TIDEOpaqueValue(
            type_name="TIDEModelState",
            representation=f"<value unavailable for State {state_id!r}>",
            reason="TIDE Model State supplied without an associated value",
            value_hash=value_hash,
        )
        if opaque.value_hash is None:
            opaque.value_hash = tide_value_hasher(opaque)
        return opaque

    def _model_actor_fields(
        self,
        execution,
        occurrence,
        host,
        raw_exec_id,
        identity_mode,
    ):
        actor = execution.get("actor")
        actor_supplied = actor is not None
        ocean_id = execution.get(
            "ocean_id",
            execution.get("ocean", self.ocean.id),
        )
        runner_id = execution.get(
            "runner_id",
            f"TIDE: .from_model({host or 'unknown'})",
        )

        if isinstance(actor, dict):
            ocean_id = actor.get("ocean", actor.get("ocean_id", ocean_id))
            runner_id = actor.get(
                "source",
                actor.get("runner_id", runner_id),
            )
            actor = actor.get("name", actor.get("actor"))

        if actor is None:
            actor = str(raw_exec_id)
        if not isinstance(actor, str) or not actor:
            raise ValueError(
                "TIDE Model actor metadata must have a non-empty name"
            )
        return actor, ocean_id, runner_id, actor_supplied

    @staticmethod
    def _model_exec_record(
        execution,
        value_by_state,
        producer_by_state,
    ):
        source = execution["source"]
        input_record = InputUseRecord(
            "args",
            tuple(state["reference"] for state in execution["inputs"]),
            tuple(state["reference"] for state in execution["inputs"]),
            tuple(state["reference"] for state in execution["inputs"]),
            tuple(
                value_by_state[state["state_id"]]
                for state in execution["inputs"]
            ),
            [],
        )
        dependencies = []

        for input_index, state in enumerate(execution["inputs"]):
            state_id = state["state_id"]
            producer = producer_by_state[state_id]
            producer_source = producer["runner_id"]
            producer_actor = Actor(
                producer["ocean_id"],
                producer["runner_id"],
                producer["actor"],
            )
            producer_exec_id = producer["exec_id"]

            dependencies.append(DependencyUseRecord(
                associated_input=("args", input_record),
                dep=state["ref"],
                type="static",
                unscoped_ref=state["reference"],
                resolved_val=value_by_state[state_id],
                resolved_ver=state["version"],
                producer_source=producer_source,
                producer_actor=producer_actor,
                producer_exec_id=producer_exec_id,
                input_index=input_index,
                input_path=[input_index],
            ))

        input_record.dependencies = dependencies
        writes = [
            WriteRecord(
                cause="TIDE: model output",
                stored_result=value_by_state[state["state_id"]],
                stored_version=state["version"],
                stored_target=state["reference"],
            )
            for state in execution["outputs"]
        ]
        output_value = (
            writes[0].stored_result
            if len(writes) == 1
            else __TIDE_TEMPLATE_VALUE__()
        )

        return ExecRecord(
            seq_id=execution["seq_id"],
            exec_id=execution["exec_id"],
            ocean_id=execution["ocean_id"],
            runner_id=execution["runner_id"],
            pipeline_id=execution["pipeline_id"],
            node_id=source.get("node_id", str(execution["raw_exec_id"])),
            func=None,
            func_name=execution["actor"],
            inputs=Inputs({"args": input_record}),
            deps=dependencies,
            effects={},
            declared_effects=[],
            declared_targets=[
                state["ref"] for state in execution["outputs"]
            ],
            runtime_effects=[],
            runtime_targets=[],
            declared_deps=[
                state["ref"] for state in execution["inputs"]
            ],
            unscoped_store_refs=["__model__"],
            status=source.get("status", "succeeded"),
            started_at=source.get("started_at", source.get("start")),
            finished_at=source.get("finished_at", source.get("end")),
            duration_ms=source.get("duration_ms", source.get("time")),
            error=source.get("error"),
            node_options={
                "source": "tide_model",
                "model_exec_id": execution["raw_exec_id"],
                "model_actor_supplied": execution["actor_supplied"],
                "model_identity_mode": execution["identity_mode"],
                "assigned_unknown": execution["assigned_unknown"],
                "preserve_exec_id": True,
                "model_input_state_ids": [
                    state["state_id"] for state in execution["inputs"]
                ],
                "model_output_state_ids": [
                    state["state_id"] for state in execution["outputs"]
                ],
            },
            output_value=output_value,
            resolve_output=False,
            writes=writes,
            emitted_by=None,
            emitted_nodes=[],
            content_id=source.get("content_id"),
        )

    def to_bundle(
        self,
        handle=None,
        unsupported="opaque",
        type=None,
        kind=None,
        path=None,
    ):
        """Create, and optionally export, this TIDE instance's bundle."""
        return self.trace().to_bundle(
            handle=handle,
            unsupported=unsupported,
            type=type,
            kind=kind,
            path=path,
        )

    @classmethod
    def from_bundle(
        cls,
        bundle,
        *,
        trusted=False,
        resource_limits=None,
    ):
        """Load a bundle and return its inspectable :class:`Trace`.

        ``bundle`` may be a ``TIDEBundle``, its Python dict payload, or a
        path to a JSON export.  The classmethod form intentionally supports
        both ``TIDE.from_bundle(...)`` and ``TIDE().from_bundle(...)``.

        Object-state reconstruction is disabled by default. Pass
        ``trusted=True`` only for a bundle whose producer and contents are
        trusted, because trusted loading may import recorded Python types and
        invoke their state restoration hooks.
        """
        from tide_tooling.tooling_core.bundling.TIDEBundle import TIDEBundle
        from tide_tooling.tooling_core.bundling.TIDEBundler import TIDEBundler

        if not isinstance(trusted, bool):
            raise TypeError("trusted must be a bool")

        bundler = TIDEBundler(
            trusted=trusted,
            resource_limits=resource_limits,
        )

        if isinstance(bundle, TIDEBundle):
            imported = TIDEBundle.from_dict(
                bundle.to_dict(),
                bundler=bundler,
            )
        elif isinstance(bundle, (str, PathLike)):
            imported = TIDEBundle.from_file(
                bundle,
                bundler=bundler,
            )
        elif isinstance(bundle, dict):
            imported = TIDEBundle.from_dict(
                bundle,
                bundler=bundler,
            )
        else:
            raise TypeError(
                "TIDE.from_bundle expects a TIDEBundle, dict, or JSON path"
            )

        return imported.trace()



class ExecutionLog:
    EXEC_ID_PREPEND = "TIDE_EXEC:"
    _exec_count = 0
    def __init__(self, ocean_id, event_log=None, pipeline_id:int|str="TIDE: .event()", fallback_store="global"):
        if not event_log:
            event_log = [] 
        self.ocean_id = ocean_id
        self.pipeline_id = pipeline_id
        self.event_log = event_log
        self.exec_log = []
        self.exec_id_to_log = {}
        self.ref_to_last_producer = {}
        self.ref_to_last_value = {}
        self.ref_to_last_snapshot = {}
        self.versioned_refs = defaultdict(lambda: -1)
        self.fallback_store = fallback_store
        
    
    def next_seq_id(self):
        id = self._exec_count
        self._exec_count += 1
        return id

    def _build_exec_log(self):
        # for event in self.event_log:
        #     self.exec_log.append(self._log_exec_event(event)) 
        # convert str to DataRef when the log is rebuilt by a caller.
        pass
    def _ensure_ref_declared(self, ref: DataRef, value) -> dict:
        ref_str = DataRef.to_string(ref,strip_leading=True)

        last = self.ref_to_last_producer.get(ref_str)
        

        if last is not None:
            last_snapshot = self.ref_to_last_snapshot.get(ref_str)
            new_snapshot = tide_snapshot_value(value, boundary="read")
            last_hash = (
                last_snapshot.value_hash
                if last_snapshot is not None
                else tide_value_hasher(
                    self.ref_to_last_value.get(ref_str, (None, None))[0]
                )
            )
            new_hash = new_snapshot.value_hash

            if last_hash == new_hash:
                return last

            declare_event = ExecEvent(
                actor="TIDE",
                reads={},
                writes={ref: value},
                source="TIDE: External",
                cause="TIDE: observed value",
                status="succeeded",
                metadata={},
            )

            declare_record = self._log_exec_event(declare_event)
            self.exec_log.append(declare_record)

            return self.ref_to_last_producer[ref_str]

        
        
        
        declare_event = ExecEvent(
            actor="TIDE",
            reads={},
            writes={ref_str: value},
            source="TIDE: External",
            cause="TIDE: user declared value",
            status="succeeded",
            metadata={},
        )



        declare_record = self._log_exec_event(declare_event)
        self.exec_log.append(declare_record)

        return self.ref_to_last_producer[ref_str]

    def _ensure_ref_data_ref(self, ref:str|DataRef, strip_leading=True):
        if not isinstance(ref, DataRef):
            if ":" not in ref:
                ref = DataRef(ref, self.fallback_store)
            else:
                ref = DataRef.from_string(ref, strip_leading=strip_leading)
        return ref

    def _dep_records(self, record: DependencyUseRecord):
        return record.expanded_to if record.type == "dynamic" else [record]

    def _patch_imported_exec_reads(self, event: ExecRecord):

        for dep_record in event.deps:
            for record in self._dep_records(dep_record): 
                ref = record.unscoped_reference
                ver = record.resolved_version

                last = self.ref_to_last_producer.get(ref)

                if not last:
                    continue

                if last["version"] != ver:
                    continue

                record.producer_exec_id = last["exec_id"]
                record.producer_actor = last["actor"]
                record.producer_source = last["actor"].source_runner

    def _should_skip_imported_external_seed_write(self, event: ExecRecord, write: WriteRecord) -> bool:
        ref = write.stored_target
        imported_ver = write.stored_version
        imported_value = write.stored_result

        if imported_ver != 0:
            return False
        
        # if event.runner_id.startswith("TIDE: .exec"):
        #     return False 
        
        
        is_preruntime_polling_actor = event.producer_id.source_actor == "TIDE: Polling"
        debug_print("BECAUSEEEE", event.producer_id.source_actor, write.cause)
        is_preruntime_polling_write = write.cause == "TIDE: Pre-runtime Polling"
        is_emitted_node_polling_write = write.cause == "TIDE: Emitted Node Polling"

        if not (is_preruntime_polling_actor or is_preruntime_polling_write or is_emitted_node_polling_write):
            return False

        if ref not in self.ref_to_last_producer:
            return False

        if ref not in self.ref_to_last_value:
            return False

        last_value, last_ver = self.ref_to_last_value[ref]

        if last_ver is None:
            return False

        if tide_value_hasher(last_value) != tide_value_hasher(imported_value):
            return False

        write.stored_version = last_ver

        return True

    @staticmethod
    def _effect_kind(effect):
        if isinstance(effect, WRITE):
            return Effects.WRITE
        if isinstance(effect, DELETE):
            return Effects.DELETE
        if isinstance(effect, END):
            return Effects.END
        if isinstance(effect, RUN):
            return Effects.RUN
        if isinstance(effect, EMIT):
            return Effects.EMIT
        raise TypeError(f"Unsupported TIDE event effect: {effect!r}")

    def _append_event_effect_records(self, record, effects):
        for effect in effects:
            kind = self._effect_kind(effect)
            targets = getattr(effect, "targets", None)
            if isinstance(effect, EMIT):
                targets = ["Runner"]
            elif isinstance(targets, __TIDE_TEMPLATE_VALUE__):
                targets = ["Runner"]
            elif not isinstance(targets, (list, tuple, set)):
                targets = [targets]

            for target in targets:
                if isinstance(target, DataRef):
                    target = DataRef.to_string(target, strip_leading=True)
                    version = self.versioned_refs.get(target, 0)
                else:
                    target = "Runner" if target is None else str(target)
                    version = 0

                effect_record = EffectRecord(
                    kind,
                    (target, version),
                    output=getattr(effect, "preserve_output", __TIDE_TEMPLATE_VALUE__()),
                    preserved_output=getattr(effect, "preserve_output", __TIDE_TEMPLATE_VALUE__()),
                )
                record.effects[(kind, (target, version))] = effect_record


    def _log_exec_event(self, event: ExecEvent | ExecRecord) -> ExecRecord:
        if isinstance(event, ExecRecord):
            self._patch_imported_exec_reads(event)
            event.seq_id = self.next_seq_id()
            self.exec_id_to_log[event.exec_id] = event
            for write in event.writes:
                if self._should_skip_imported_external_seed_write(event, write):
                    continue
                write: WriteRecord
                ref = write.stored_target
                ver = self.versioned_refs[ref] + 1 #if ref in self.versioned_refs else self.versioned_refs[ref]
                write.stored_version = ver
                val = write.stored_result
                
                self.ref_to_last_producer[ref] = {
                    "exec_id": event.exec_id,
                    "actor": event.producer_id,
                    "version": ver,
                }
                self.ref_to_last_value[ref] = (val, ver)
                self.ref_to_last_snapshot[ref] = tide_snapshot_value(
                    val,
                    boundary="write",
                )
                self.versioned_refs[ref] = ver
            return event
        


        deps = []
        read_inputs = [] 
        read_raw = []
        input_records = {}
        
        
        raw_input = []
        normalised_input = []
        resolved_input = []
        input_deps = normalised_input
            

        for raw_ref, value in event.reads.items():
            raw_input.append(raw_ref)

            ref = self._ensure_ref_data_ref(raw_ref, strip_leading=False)
            normalised_input.append(ref)
            read_snapshot = tide_snapshot_value(value, boundary="read")
            read_value = read_snapshot.materialise()
            resolved_input.append(read_value)
            ref_str = DataRef.to_string(ref, strip_leading=True)

            last = self._ensure_ref_declared(ref, value)

            producer_exec_id = last["exec_id"] if last else None
            

            producer_actor = last["actor"] if last else "TIDE: External"
            producer_source = last["actor"].source_runner if last else Actor("unknown", "unknown", "unknown")
            producer_ver = last["version"] if last else 0

            dep = DependencyUseRecord(
                associated_input=None,
                dep=ref,
                type="static",
                unscoped_ref=ref_str,
                resolved_val=read_value,
                resolved_ver=producer_ver,
                producer_source=producer_source, 
                producer_actor=producer_actor,
                producer_exec_id=producer_exec_id,
                input_path=(len(deps),),
                snapshot_lineage=[read_snapshot.to_dict()],
            )

            deps.append(dep) 
            read_raw.append(raw_ref)
            read_inputs.append(value)

        associated_input = ("args", RunnerNodeInput(raw=tuple(raw_input), evaluated=tuple(raw_input), normalised=tuple(normalised_input), resolved=tuple(resolved_input), deps=list(input_deps)))

        for input_index, dep in enumerate(deps):
            dep: DependencyUseRecord
            dep.associated_input = associated_input
            dep.input_index = input_index
            
       

        input_records[associated_input[0]] = InputUseRecord(
            name=associated_input[0],
            raw=associated_input[1].raw,
            evaluated=associated_input[1].evaluated,
            normalised=associated_input[1].normalised,
            resolved=associated_input[1].resolved,
            dependencies=deps,
        )

        inputs = Inputs(input_records)
        if isinstance(event.actor, str):
            actor_name = event.actor
            func = "unknown"
            func_name = event.actor
        else:
            actor_name = event.actor.__name__
            func = event.actor
            func_name = event.actor.__name__
        if self.pipeline_id=="TIDE: .event()":
            pipeline_id = f"TIDE: .event({actor_name}[{event.seq_id}])"
        else: 
            pipeline_id = self.pipeline_id
        tide_exec_id = f"{self.EXEC_ID_PREPEND}{event.uuid}"
        record = ExecRecord(
            seq_id=self.next_seq_id(),
            exec_id=tide_exec_id,
            ocean_id=self.ocean_id,
            runner_id=event.source,
            pipeline_id=pipeline_id,
            node_id="unknown",
            func=func,
            func_name=func_name,
            inputs=inputs,
            deps=deps,
            effects={},
            declared_deps=[DataRef.from_string(read, strip_leading=False) for read in event.reads.keys()],
            declared_effects=list(event.effects),
            declared_targets=[DataRef.from_string(write, strip_leading=False) for write in event.writes.keys()],
            runtime_effects=[],
            runtime_targets=[],
            unscoped_store_refs=list(DataRef.to_string(self._ensure_ref_data_ref(str_ref, strip_leading=False), strip_leading=True).split(":", 1)[0] for str_ref in event.writes),
            status=event.status,
            started_at=event.metadata.get("started_at"),
            finished_at=event.metadata.get("finished_at"),
            duration_ms=event.metadata.get("duration_ms"),
            error=event.metadata.get("error"),
            output_value=None,
            resolve_output=True,
            writes=[],
            emitted_by=[],
            emitted_nodes=[],
            base_node_id=None,
            base_node_content_id=None,
            content_id=None,
            node_options={},
        )

        for ref, value in event.writes.items():
            ref = self._ensure_ref_data_ref(ref, strip_leading=False)
            ref_str = DataRef.to_string(ref, strip_leading=True)
            write_snapshot = tide_snapshot_value(value, boundary="write")
            historical_value = write_snapshot.materialise()
            ver = self.versioned_refs[ref_str] + 1 
            self.versioned_refs[ref_str] = ver

            record.writes.append(
                WriteRecord(
                    cause=f"{event.cause}",
                    stored_result=historical_value,
                    stored_version=ver,
                    stored_target=ref_str,
                    snapshot_lineage=[write_snapshot.to_dict()],
                )
            )

            self.ref_to_last_producer[ref_str] = {
                "exec_id": tide_exec_id,
                "actor": record.producer_id,
                "version": ver,
            }
            self.ref_to_last_value[ref_str] = (historical_value, ver)
            self.ref_to_last_snapshot[ref_str] = write_snapshot


        self._append_event_effect_records(record, event.effects)

        self.exec_id_to_log[tide_exec_id] = record
        return record

class EventCapture:
    seq_id = -1
    def __init__(self, source_ocean: Ocean, exec_log: ExecutionLog):
        self._source_ocean = source_ocean
        self._source_ocean_id = source_ocean.id
        self._source_runner = "TIDE: .event()"
        self._source_pipeline = "TIDE: .event()"
        self._default_store = "global"
        self._seen_value_hashes = set()
        self.events = []
        self.exec_log = exec_log

    def set_default_store(self, store):
        self._default_store = store
        self.exec_log.fallback_store = store
        return self

    @contextmanager
    def _store_context(self):
        token = DataRef.set_default_store(self._default_store)
        self.exec_log.fallback_store = self._default_store
        try:
            yield
        finally:
            DataRef.reset_default_store(token)

    def _to_exec_log(self, name:str=None):
        if not name:
            name = self._source_pipeline

        exec_log = ExecutionLog(
            self._source_ocean_id,
            self.events,
            pipeline_id=name,
            fallback_store=self._default_store,
        )
        exec_log._build_exec_log()
        return exec_log.exec_log

    def _next_exec_record_seq_id(self):
        self.seq_id += 1
        return self.seq_id

    def _normalise_ref_strs_to_data_ref(self, ref: str):
        if isinstance(ref, str):
            if ":" not in ref:
                ref = DataRef(ref, self._default_store)
            else:
                ref = DataRef.from_string(ref,strip_leading=False) 
        return ref

    def event(
        self,
        actor: str | Callable,
        reads: dict | list | str | None = None,
        writes: dict | None = None,
        source: str | None = None,
        cause: str | None = None,
        effects=None,
        return_id: bool = False,
    ):
        """Record one observed occurrence.

        ``writes`` maps references to the values actually written. A bare
        collection of target names is not sufficient evidence for an event.
        """
        with self._store_context():
            return self._event(
                actor,
                reads=reads,
                writes=writes,
                source=source,
                cause=cause,
                effects=effects,
                return_id=return_id,
            )

    def _event(
        self,
        actor: str | Callable,
        reads: dict | list | str | None = None,
        writes: dict | None = None,
        source: str | None = None,
        cause: str | None = None,
        effects=None,
        return_id: bool = False,
    ):

        actor = actor
        reads = reads or {}
        if not isinstance(reads, (list, dict)):
            reads = [reads]
        writes = dict(writes or {}) if isinstance(writes or {}, dict) else writes
        source = source or self._source_runner
        cause = cause or "unknown"

        if effects is None:
            effects = []
        elif isinstance(effects, Effect):
            effects = [effects]
        elif isinstance(effects, (list, tuple, set)):
            effects = list(effects)
        else:
            raise TypeError(
                "tide.event effects must be an Effect or an iterable of Effects"
            )

        if any(not isinstance(effect, Effect) for effect in effects):
            raise TypeError("tide.event effects must contain only Effect objects")

        for effect in effects:
            if not isinstance(effect, WRITE):
                continue
            if isinstance(effect.preserve_output, __TIDE_TEMPLATE_VALUE__):
                continue
            for target in effect.targets:
                writes.setdefault(target, effect.preserve_output)

        seq_id =self._next_exec_record_seq_id()
        event_exec = ExecEvent(seq_id=seq_id, actor=actor, reads=reads, writes=writes, source=source, cause=cause, effects=effects)
        if isinstance(reads, list):
            
            ref_to_values = {}
            for idx, ref in enumerate(reads):
                ref = self._normalise_ref_strs_to_data_ref(ref)
                if self._source_ocean.store_router.exists(ref.store, ref.key):    
                    val = self._source_ocean.store_router.get(ref.store, ref.key)
                    ref_to_values[ref] = val
                else:
                    raise RuntimeError(f"[ERROR] ref {ref} from .event({actor}) {reads} was not available! " + "\n" + 
                                       "please use .exec() to produce this reference, or use a store template!" + "\n" + 
                                       "tide.ocean() supports declaring work which can wait on external writes!")
            reads = ref_to_values
            event_exec.reads = reads
                       
        if not isinstance(writes, dict):
            raise TypeError(
                "tide.event writes must be a dict mapping references to "
                f"values, got {type(writes).__name__}"
            )
        
        
        exec_record = self.exec_log._log_exec_event(event_exec)
        self.exec_log.exec_log.append(exec_record)
        if self._source_ocean.done:
            self._source_ocean.done=False
        self._source_ocean.consume_event(Event(source="TIDE: .event()", event_type=LogEvents.EVENT_EXEC, payload=exec_record))
        self.events.append(Event(source="TIDE: .event()", event_type=LogEvents.EVENT_EXEC, payload=exec_record))
        self._source_ocean.tick(external=True)
        for ref, val in dict(writes).items():
            ref = self._normalise_ref_strs_to_data_ref(ref)
            if self._source_ocean.done:
                self._source_ocean.done=False
            self._source_ocean.store_router.set(ref.store, ref.key, val, force=True)

        for effect in effects:
            if not isinstance(effect, DELETE):
                continue
            for ref in effect.targets:
                ref = self._normalise_ref_strs_to_data_ref(ref)
                if self._source_ocean.store_router.exists(ref.store, ref.key):
                    self._source_ocean.store_router.delete(ref.store, ref.key)
        self._source_ocean.tick(external=True)
            # self._source_ocean.consume_event(Event(source=f"runner", source_id=self._source_runner, event_type=RouterEvents.STORE_WRITE, payload=(ref.store, ref.key, val, actor if isinstance(actor, str) else actor.__name__, ".event() write"), exec_id=exec_record.exec_id))

        if return_id:
            return seq_id
           
        
        
        
        
            


    def trace(self, name:str="TIDE: .trace()"):
        exec_log = self.exec_log.exec_log
        trace = Trace(event_log=self.events, runner=self._source_runner, ocean_id=self._source_ocean_id, exec_log=exec_log, name=name, all_known_pipelines=self._source_ocean.pipelines, all_known_runners=self._source_ocean.runners)
        
        return trace
