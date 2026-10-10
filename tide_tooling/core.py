from tide_tooling.tooling_core.engine.classes import (
    DataRef,
    DELETE,
    DELETE_NOW,
    END,
    Event,
    Effect,
    Node,
    NodeInput as NodeInput,
    Pipeline as Pipeline,
    RAISE,
    RUN,
    RunnerNodeInput,
    WRITE,
    WRITE_NOW,
    __fallback__,
)
from tide_tooling.tooling_core.engine.events import (
    Cause,
    Effects,
    ExecutorEvents,
    LogEvents,
    PipelineEvents,
    RouterEvents as RouterEvents,
)
from tide_tooling.tooling_core.engine.event_payloads import (
    EffectOccurrence,
    State,
)
# from tide_tooling.tooling_core.engine.decorators import pipeline, node
from tide_tooling.tooling_core.engine.ocean import Ocean
from tide_tooling.tooling_core.engine.tide_runtime_logging import trace_from_event_log
from tide_tooling.tooling_core.engine.tide_links import (
    ReadEdge as ReadEdge,
    EffectEdge as EffectEdge,
    EmissionEdge as EmissionEdge,
)
from tide_tooling.tooling_core.engine.tide_query_results import (
    Compared as Compared,
    Inspected as Inspected,
    Investigate as Investigate,
    Object as Object,
)
from tide_tooling.tooling_core.engine.tide_logging import Trace
from tide_tooling.tooling_core.engine.tide_model import (
    TIDEModel,
    TIDEModelValidityReport,
)
from tide_tooling.tooling_core.engine.tide_records import (
    Actor,
    ActorOccurrence,
    DependencyUseRecord,
    ExecRecord,
    InputUseRecord,
    Inputs,
    WriteRecord,
)
from tide_tooling.tooling_core.engine.tide_rendering import (
    strip_ansi as strip_ansi,
)
from functools import update_wrapper
from os import PathLike
from tide_tooling.tooling_core.engine.utils import (
    create_tide_exec_uuid,
    tide_snapshot_value,
    tide_value_hasher
)
from typing import Any, Callable
from tide_tooling.tooling_core.engine.errors import (
    MissingStoreError,
    NoExecRecordError,
    PipelineDeadlockError,
)
from tide_tooling.tooling_core.engine.stores import MemoryKVStore
from tide_tooling.tooling_core.engine.decorators import current_pipeline_build

import copy
import threading
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
    event_seq_id = -1

    @staticmethod
    def generate_signing_keys():
        """Generate an explicit Ed25519 Bundle signing key pair."""
        from tide_tooling.tooling_core.bundling.TIDESigning import (
            generate_signing_keys,
        )

        return generate_signing_keys()

    def __init__(
        self,
        default_store="global",
        timeout=None,
    ):
        self.ocean = Ocean(default_store=default_store, timeout=timeout)
        self._default_store = None
        self.set_default_store(default_store)
        # The facade preserves the existing call API and adds:
        # ``with tide.exec as execute: execute(node_graph)``.
        self.exec = _TIDEExecFacade(self, self.exec)
    @property
    def default_store(self):
        return self._default_store
    def set_default_store(self, store):
        """Select the existing store used by unscoped TIDE references."""
        if not isinstance(store, str) or not store.strip():
            raise ValueError("Store name must be a non-empty string")
        if not self.ocean.store_registry.exists(store, True) and not self.ocean.store_registry.exists(store):
            raise MissingStoreError(f"Store {store!r} is not registered")
        self._default_store = store
        self.ocean.default_store = store
        return self
    def _next_exec_record_seq_id(self):
        self.seq_id += 1
        return self.seq_id
    @staticmethod
    def _normalise_event_ref(reference):
        # Event State identity uses the complete semantic reference.
        reference = DataRef.from_string(reference, prefix=False)
        return DataRef(
            reference.key,
            reference.store,
            reference.count,
            reference.max,
            literal=reference.literal,
        )
    @staticmethod
    def _event_effect_kind(effect):
        if isinstance(effect, WRITE):
            return Effects.WRITE
        if isinstance(effect, DELETE):
            return Effects.DELETE
        if isinstance(effect, END):
            return Effects.END
        if isinstance(effect, RUN):
            return Effects.RUN
        raise TypeError(f"Unsupported TIDE event effect: {effect!r}")
    @staticmethod
    def _capture_event_effect(effect):
        """Freeze Effect structure while retaining runtime value objects."""
        captured = copy.copy(effect)
        if isinstance(effect, WRITE):
            captured.targets = tuple(
                (copy.copy(reference), value)
                for reference, value in effect.targets
            )
        elif hasattr(effect, "targets"):
            targets = effect.targets
            captured.targets = (
                None
                if targets is None
                else tuple(copy.copy(target) for target in targets)
            )
        if isinstance(effect, RUN):
            captured.local_store_templates = dict(
                effect.local_store_templates
            )
        return captured
    def _event_target_runner(self, reference):
        store = self.ocean.store_router.get_store(reference.store)
        return "*" if store.scope == "global" else store.parent_runner_id
    def _event_exec_record(self, seq_id, actor, raw_reads, reads, writes, source, effects):
        # Represent the exact values observed by this event.
        raw = tuple(raw_reads)
        normalised = tuple(reads)
        resolved = tuple(reads.values())
        input_record = InputUseRecord("args", raw, raw, normalised, resolved, [])
        associated_input = ("args", input_record)
        dependencies = []
        for input_index, (reference, value) in enumerate(reads.items()):
            ref_string = DataRef.to_string(reference, prefix=False)
            read_snapshot = tide_snapshot_value(value, boundary="read")
            dependencies.append(DependencyUseRecord(
                associated_input=associated_input,
                dep=reference,
                type="static",
                unscoped_ref=ref_string,
                resolved_val=read_snapshot,
                input_index=input_index,
                input_path=(input_index,),
                snapshot_lineage=[read_snapshot.to_dict()],
            ))
        input_record.dependencies = dependencies
        actor_name = actor if isinstance(actor, str) else actor.__name__
        func = "unknown" if isinstance(actor, str) else actor
        record = ExecRecord(
            seq_id=-1,
            exec_id=create_tide_exec_uuid(),
            ocean_id=self.ocean.id,
            runner_id=source,
            pipeline_id=f"TIDE: .event({actor_name}[{seq_id}])",
            node_id="unknown",
            func=func,
            func_name=actor_name,
            inputs=Inputs({"args": input_record}),
            deps=dependencies,
            effects=[],
            declared_deps=list(reads),
            declared_effects=list(effects),
            declared_targets=list(writes),
            runtime_effects=[],
            runtime_targets=[],
            unscoped_store_refs=[reference.store for reference in writes],
            status=None,
            started_at=None,
            finished_at=None,
            duration_ms=None,
            error=None,
            output_value=None,
            writes=[],
            emitted_nodes=[],
            base_node_id=None,
            base_node_content_id=None,
            content_id=None,
            node_options={},
        )
        return record
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
        """Record one observed occurrence in this Ocean's Event stream."""
        reads = {} if reads is None else reads
        supplied_reads = isinstance(reads, dict)
        if not isinstance(reads, (list, dict)):
            reads = [reads]
        reads = dict(reads) if supplied_reads else tuple(reads)
        writes = {} if writes is None else writes
        if not isinstance(writes, dict):
            raise TypeError(
                "tide.event writes must be a dict mapping references to "
                f"values, got {type(writes).__name__}"
            )
        writes = dict(writes)
        source = "TIDE: .event()" if source is None else source
        cause = "unknown" if cause is None else cause
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
        if any(isinstance(effect, RAISE) for effect in effects):
            raise TypeError(
                "RAISE is execution control and cannot be a declared event Effect"
            )
        for effect in effects:
            self._event_effect_kind(effect)
        effects = tuple(effects)
        default_store = self._default_store
        return self.ocean._coordinate_tide_event(lambda: self._apply_event_request(
            actor=actor,
            reads=reads,
            supplied_reads=supplied_reads,
            writes=writes,
            source=source,
            cause=cause,
            effects=effects,
            return_id=return_id,
            default_store=default_store,
        ))
    def _apply_event_request(
        self,
        *,
        actor,
        reads,
        supplied_reads,
        writes,
        source,
        cause,
        effects,
        return_id,
        default_store,
    ):
        """Apply one complete public Event operation under scheduler ownership."""
        seq_id = self.event_seq_id + 1
        self.event_seq_id = seq_id
        raw_reads = tuple(reads)
        effects = tuple(
            self._capture_event_effect(effect)
            for effect in effects
        )
        writes = dict(writes)
        for effect in effects:
            if isinstance(effect, WRITE):
                for target, value in effect.targets:
                    writes.setdefault(target, value)
        token = DataRef.set_default_store(default_store)
        try:
            # Resolve existing reads or accept explicitly observed values.
            if not supplied_reads:
                observed_reads = {}
                for reference in reads:
                    reference = self._normalise_event_ref(reference)
                    if not self.ocean.store_router.exists(reference.store, reference.key):
                        raise RuntimeError(
                            f"[ERROR] ref {reference} from .event({actor}) was not available"
                        )
                    observed_reads[reference] = self.ocean.store_router.get(reference.store, reference.key)
            else:
                observed_reads = {
                    self._normalise_event_ref(reference): value
                    for reference, value in reads.items()
                }
            normalised_writes = {
                self._normalise_event_ref(reference): value
                for reference, value in writes.items()
            }
            if (
                threading.get_ident()
                == self.ocean._scheduler_owner_thread_id
            ):
                # Record the event execution before its effects and writes.
                exec_record = self._event_exec_record(seq_id, actor, raw_reads, observed_reads, normalised_writes, source, effects)
                exec_event = Event(
                    ocean_id=self.ocean.id,
                    source="TIDE: .event()",
                    source_id=str(actor),
                    type=LogEvents.EVENT_EXEC,
                    cause=Cause(description=cause),
                    payload=exec_record,
                    exec_id=exec_record.exec_id,
                )
                self.ocean._route_scheduler_consequence(exec_event)
                # Preserve declared Effect occurrences exactly as observed.
                for effect in effects:
                    kind = self._event_effect_kind(effect)
                    raw_targets = getattr(effect, "targets", None)
                    targets = (raw_targets,) if raw_targets is None or not isinstance(raw_targets, (list, tuple, set)) else tuple(raw_targets)
                    found = None
                    if kind == Effects.DELETE:
                        outcomes = []
                        for target in targets:
                            if target is None:
                                outcomes.append(False)
                                continue
                            reference = self._normalise_event_ref(target)
                            removed = self.ocean.store_router.get_store(reference.store).pop(reference.key)
                            outcomes.append(not isinstance(removed, __fallback__))
                        found = tuple(outcomes)
                    self.ocean._route_scheduler_consequence(Event(
                        ocean_id=self.ocean.id,
                        source="TIDE: .event()",
                        source_id=str(actor),
                        type=LogEvents.EFFECT,
                        cause=Cause.from_event(exec_event),
                        payload=EffectOccurrence(kind, targets, getattr(effect, "output", __fallback__()), "declared", False, found),
                        exec_id=exec_record.exec_id,
                    ))
                # Write the observed values and record their State consequences.
                for reference, value in normalised_writes.items():
                    if (
                        not self.ocean.store_registry.exists(reference.store)
                        and not self.ocean.store_registry.exists(
                            reference.store,
                            template=True,
                        )
                    ):
                        self.ocean._register_store(reference.store)
                    update = self.ocean.store_router.set(reference.store, reference.key, value, force=True)
                    self.ocean._route_scheduler_consequence(Event(
                        ocean_id=self.ocean.id,
                        source="store_router",
                        source_id=self.ocean.store_router.id,
                        type=PipelineEvents.DEP_UPDATED if update else PipelineEvents.DEP_AVAILABLE,
                        cause=Cause.from_event(exec_event),
                        target_runner_id=self._event_target_runner(reference),
                        payload=State(
                            reference,
                            value,
                            tide_snapshot_value(value, boundary="write"),
                            value_type=type(value),
                        ),
                        exec_id=exec_record.exec_id,
                    ))
                self.ocean._wake_event.set()
            else:
                raise RuntimeError(
                    "TIDE Event requests require Ocean scheduler ownership"
                )
        finally:
            DataRef.reset_default_store(token)
        if return_id:
            return seq_id
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
        return_trace: bool = False
    ):
        """Execute one callable and optionally write its returned outputs.
        ``writes`` names the reference or ordered references that receive the
        callable's returned output. It does not accept a reference-to-value
        dictionary. Pass supplied values to ``TIDE.event(..., writes={...})``;
        use a ``WRITE`` effect for an additional write emitted while the
        callable runs.
        """
        selected_store = default_store or self._default_store
        if not isinstance(selected_store, str) or not selected_store.strip():
            raise ValueError("Store name must be a non-empty string")
        self.ocean.store_router.get_store(selected_store)
        token = DataRef.set_default_store(selected_store)
        try:
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
                return_trace=return_trace
            )
        finally:
            DataRef.reset_default_store(token)
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
        return_trace: bool = False
    ):
        node_options = {}
        if isinstance(func, Node):
            node_definition = func
            node_options = {
                "node_name": node_definition.node_name,
                "force_update": node_definition.force_update,
                "one_shot": True, # node_definition.one_shot,
                "unpack": node_definition.unpack,
                "share_implicit_target": node_definition.share_implicit_target,
            }
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
                writes = [DataRef.to_string(target, prefix=False) for target in node_definition.targets]
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
        tide_node = Node(func, args=args, kwargs=kwargs, p_args=p_args, p_kwargs=p_kwargs, k_args=k_args, k_kwargs=k_kwargs, targets=writes_with_store, effects=effects, **node_options)
        if return_trace:
            return self.ocean.run_node(
            tide_node,
            mode="sequential",
            return_trace=True,
        )
        output = self.ocean.run_node(
            tide_node,
            mode="sequential",
        )
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
    def trace(self, name: str = None) -> Trace:
        """Construct one Trace from this Ocean's frozen Event evidence."""
        with self.ocean._tick_lock:
            event_log = self.ocean.event_logger.freeze()

        return trace_from_event_log(
            event_log,
            ocean_id=self.ocean.id,
            pipelines={
                pipeline_id: pipeline
                for pipeline_id, (pipeline, _) in self.ocean.pipelines.items()
            },
            name=name or "TIDE: .trace()",
        )
    @classmethod
    def now(cls, script, *script_args, project_dir=None) -> Trace:
        """Run a Python script under noWorkflow and return its TIDE projection."""
        from tide_tooling.TIDEAdapters import NOWAdapter
        return NOWAdapter.capture(script, *script_args, project_dir=project_dir)
    @classmethod
    def from_now(cls, project_dir, *, trial=None) -> Trace:
        """Project an existing noWorkflow project or Trial."""
        from tide_tooling.TIDEAdapters import NOWAdapter
        return NOWAdapter(project_dir, trial=trial).to_trace()
    @classmethod
    def from_model(
        cls,
        model,
        *,
        host=None,
        name="TIDE: .from_model()",
        identity_mode="literal"
    ) -> Trace:
        """Project a structurally valid TIDE Model into a Trace."""
        return TIDEModel.to_trace(
            model,
            host=host,
            name=name,
            identity_mode=identity_mode,
        )
    @classmethod
    def from_openlineage(cls, events, **from_model_options) -> Trace:
        """Project OpenLineage RunEvents through the TIDE Model boundary."""
        from tide_tooling.TIDEAdapters import OpenLineageAdapter
        return OpenLineageAdapter(events).to_trace(**from_model_options)
    @classmethod
    def from_opentelemetry(cls, spans) -> Trace:
        """Project OpenTelemetry Spans through the TIDE Model boundary."""
        from tide_tooling.TIDEAdapters import OpenTelemetryAdapter
        return OpenTelemetryAdapter(spans).to_trace()
    @classmethod
    def from_openinference(cls, spans) -> Trace:
        """Project OpenInference Spans through the TIDE Model boundary."""
        from tide_tooling.TIDEAdapters import OpenInferenceAdapter
        return OpenInferenceAdapter(spans).to_trace()
    def to_model(
        self,
        *,
        override_host_as=None,
        enrich=True,
        incidence_format="explicit",
        identity_mode="literal",
    ):
        """Export this instance's Trace as the interoperable TIDE Model."""
        return self.trace().to_model(
            override_host_as=override_host_as,
            enrich=enrich,
            incidence_format=incidence_format,
            identity_mode=identity_mode,
        )
    @classmethod
    def report_model_validity(cls, model, *, host=None):
        return TIDEModel.report_validity(model, host=host)
    def to_bundle(
        self,
        handle=None,
        unsupported="opaque",
        type=None,
        kind=None,
        path=None,
        serialise_events=False,
        verify_events=True,
        sign=None,
    ):
        """Create, and optionally export, this TIDE instance's bundle."""
        return self.trace().to_bundle(
            handle=handle,
            unsupported=unsupported,
            type=type,
            kind=kind,
            path=path,
            serialise_events=serialise_events,
            verify_events=verify_events,
            sign=sign,
        )
    @classmethod
    def from_bundle(
        cls,
        bundle,
        *,
        trusted=False,
        verify_events=True,
        resource_limits=None,
        verify=None,
    ):
        """Load a bundle and return its inspectable :class:`Trace`.
        ``bundle`` may be a ``TIDEBundle``, its Python dict payload, or a
        path to a JSON export.  The classmethod form intentionally supports
        both ``TIDE.from_bundle(...)`` and ``TIDE().from_bundle(...)``.
        Object-state reconstruction is disabled by default. Pass
        ``trusted=True`` only for a bundle whose producer and contents are
        trusted, because trusted loading may import recorded Python types and
        invoke their state restoration hooks.
        Bundle loading restores the native Trace representation. Retained
        Events, when present, are optional source evidence and do not replace
        the bundled Trace as its construction authority.
        """
        from tide_tooling.tooling_core.bundling.TIDEBundle import TIDEBundle
        from tide_tooling.tooling_core.bundling.TIDEBundler import TIDEBundler
        if not isinstance(trusted, bool):
            raise TypeError("trusted must be a bool")
        if not isinstance(verify_events, bool):
            raise TypeError("verify_events must be a bool")
        bundler = TIDEBundler(
            trusted=trusted,
            verify_events=verify_events,
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
        if verify is not None:
            imported.verify(verify)
        return imported.trace()
