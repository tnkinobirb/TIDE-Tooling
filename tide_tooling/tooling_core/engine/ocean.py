from queue                          import Queue, Empty
from typing                         import Any
from collections                    import defaultdict
import copy
import threading
from dataclasses                    import dataclass, field
from contextvars                    import copy_context
from contextlib                    import contextmanager
from tide_tooling.tooling_core                           import debug_print as print
from tide_tooling.tooling_core.engine.classes            import DataRef, Node, Pipeline
from tide_tooling.tooling_core.engine.stores             import MemoryKVStore
from tide_tooling.tooling_core.engine.events             import Cause, LogEvents, OceanEvents, PipelineEvents, Event
from tide_tooling.tooling_core.engine.event_payloads     import PipelineRun, PipelineRegistration, RunnerPlan
from tide_tooling.tooling_core.engine.event_occurrences      import EventLogger
from tide_tooling.tooling_core.engine.tide_runtime_logging import trace_from_event_log
from tide_tooling.tooling_core.engine.routers            import EventRouter, StoreRouter
from tide_tooling.tooling_core.engine.executor           import Executor
from tide_tooling.tooling_core.engine.interfaces         import EventInterface
from tide_tooling.tooling_core.engine.registries         import StoreRegistry
from tide_tooling.tooling_core.engine.PipelineRunner     import PipelineRunner
from tide_tooling.tooling_core.engine.errors             import StoreAlreadyRegisteredError, NoExecRecordError
_USE_OCEAN_TIMEOUT = object()
class NowRequestFailed(RuntimeError):
    """One immediate Store operation could not be completed by Ocean."""
@dataclass
class _EventWork:
    event: Event
@dataclass(kw_only=True)
class _SynchronousWork:
    completed: threading.Event = field(default_factory=threading.Event)
    result: Any = None
    error: BaseException | None = None
    scheduler_error: BaseException | None = None
@dataclass(kw_only=True)
class _TIDEEventRequest(_SynchronousWork):
    apply: Any
@dataclass(kw_only=True)
class _RegisterPipelineRequest(_SynchronousWork):
    pipeline: Pipeline
    name: str | None
    local_store_templates: dict[str, Any]
@dataclass(kw_only=True)
class _NowRequest(_SynchronousWork):
    operation: str
    runner_id: int | str
    runtime: Any
    effect: Any
class Ocean:
    __ocean_count__ = 0
    def __init__(
        self,
        default_store="mem",
        timeout=None,
    ):
        # Retain Ocean identity and run policy.
        self.id = Ocean.__ocean_count__
        Ocean.__ocean_count__ += 1
        self.timeout = timeout
        self.default_store = default_store
        # Retain Ocean lifecycle state.
        self.done = False
        self.closed = False
        self.last_trace = None
        # Retain Ocean scheduler-thread state.
        self.thread = None
        self._parent_runner_id = None
        self._scheduler_error = None
        self._scheduler_owner_thread_id = None
        self._tick_lock = threading.RLock()
        self._run_lock = threading.Lock()
        self._wake_event = threading.Event()
        # Retain Pipeline and Runner ownership.
        self.pipelines = {}
        self.runners = {}
        self.running = {}
        # Route Ocean Events to their lifecycle transitions.
        self.handlers = {
            OceanEvents.QUIT: self._on_quit,
            OceanEvents.REGISTER_PIPELINE: self._on_register_pipeline,
            OceanEvents.RUN_PIPELINE: self._on_run_pipeline,
            OceanEvents.KILL_RUNNER: self._on_kill_runner,
        }
        # Accept every externally produced scheduler operation through one
        # FIFO ingress.  Only Ocean's scheduler may apply or route it.
        self._scheduler_queue = Queue()
        # Retain the public/internal name while the queue now carries typed
        # scheduler work rather than bare Events.
        self.event_queue = self._scheduler_queue
        # Own the shared Event network.
        self.event_logger = EventLogger()
        self.event_router = EventRouter(self.event_logger)
        self.event_router.subscribe(OceanEvents, self._handle_ocean_event)
        self.event_interface = EventInterface(
            self.id,
            self.consume_event,
            self.event_router.subscribe,
        )
        # Own the shared live Store network.
        self.store_registry = StoreRegistry()
        self.store_router = StoreRouter(
            self.store_registry,
            self.event_interface,
            consequence_emitter=self.event_router.route,
        )
        self.register_store("global", MemoryKVStore)
        self.register_store("mem", MemoryKVStore, scope="local")
        self.register_store("__tide__", MemoryKVStore, scope="local")
        self.register_store("__untracked__", MemoryKVStore, scope="local")
        # Own shared execution.
        self.executor = Executor(self.event_interface)
    def register_store(
        self,
        name=None,
        store_class=MemoryKVStore,
        scope="global",
        store_template=None,
    ):
        # Setup-only registry mutation cannot race accepted scheduler work.
        with self._tick_lock:
            if (
                self._scheduler_owner_thread_id is not None
                or self.running
                or not self._scheduler_queue.empty()
                or (self.thread is not None and self.thread.is_alive())
            ):
                raise RuntimeError(
                    "Store registration is unavailable while Ocean is scheduling"
                )
            return self._register_store(
                name=name,
                store_class=store_class,
                scope=scope,
                store_template=store_template,
            )
    def _register_store(
        self,
        name=None,
        store_class=MemoryKVStore,
        scope="global",
        store_template=None,
    ):
        # Construct and register one Store definition or instance.
        self.store_registry.register(store_class(
            name=name,
            scope=scope,
            store_template=store_template,
        ))
    def register_pipeline(
        self,
        pipeline,
        name=None,
        local_store_templates=None,
    ):
        # Normalise the supported Pipeline definition carriers.
        if isinstance(pipeline, list):
            pipeline = Pipeline(pipeline, name=name)
        elif not isinstance(pipeline, Pipeline):
            raise TypeError(
                "register_pipeline() requires a Pipeline or list of Nodes"
            )
        # Validate only caller-owned shape before entering scheduler order.
        if name is not None:
            if not isinstance(name, str):
                raise TypeError("Pipeline name must be a string or None")
        # Normalise the Pipeline's local Store seeds.
        if local_store_templates is None:
            local_store_templates = {}
        elif not isinstance(local_store_templates, dict):
            raise TypeError("local_store_templates must be a dict or None")
        request = _RegisterPipelineRequest(
            pipeline=pipeline,
            name=name,
            local_store_templates=dict(local_store_templates),
        )
        return self._coordinate_synchronous_work(request)
    def _on_register_pipeline(self, event):
        # Assign Ocean identity and retain the registered definition.
        pipeline = event.payload.pipeline
        pipeline.id = self._next_pipeline_id()
        self.pipelines[pipeline.id] = (
            pipeline,
            event.payload.local_store_templates,
        )
    def run_pipeline(
        self,
        pipeline,
        mode="auto",
        local_store_templates=None,
        stay_alive=False,
        timeout=_USE_OCEAN_TIMEOUT,
        dynamic_ref_plan=None,
        name=None,
        default_store=None,
    ):
        # One synchronous run_pipeline call owns the Ocean at a time.
        if not self._run_lock.acquire(blocking=False):
            raise RuntimeError(
                "Ocean.run_pipeline does not support concurrent calls"
            )
        try:
            selected_store = (
                self.default_store if default_store is None else default_store
            )
            token = DataRef.set_default_store(selected_store)
            try:
                return self._run_pipeline(
                    pipeline,
                    mode,
                    local_store_templates,
                    stay_alive,
                    timeout,
                    dynamic_ref_plan,
                    name,
                )
            finally:
                DataRef.reset_default_store(token)
        finally:
            self._run_lock.release()
    def _run_pipeline(
        self,
        pipeline,
        mode,
        local_store_templates,
        stay_alive,
        timeout,
        dynamic_ref_plan,
        name,
    ):
        # Reject runs after shutdown or during an existing scheduling run.
        if self.closed:
            raise RuntimeError("Ocean is closed")
        if self.thread is not None and self.thread.is_alive():
            raise RuntimeError("Ocean already has an active scheduling thread")
        if isinstance(pipeline, list):
            pipeline = Pipeline(pipeline)
        # Materialise an @pipeline-decorated definition.
        if callable(pipeline) and not isinstance(pipeline, Pipeline):
            pipeline = pipeline()
        if not isinstance(pipeline, Pipeline):
            raise TypeError(
                "run_pipeline() requires a Pipeline or "
                "@pipeline-decorated callable"
            )
        # Accept only local Store seed mappings.
        if local_store_templates is None:
            local_store_templates = {}
        elif not isinstance(local_store_templates, dict):
            raise TypeError(
                "local_store_templates must be a dict or None"
            )
        # Register a new Pipeline or inherit its registered Store seeds.
        if pipeline.id in self.pipelines:
            pipeline, registered_templates = self.pipelines[pipeline.id]
            if not local_store_templates:
                local_store_templates = registered_templates
        else:
            pipeline = self.register_pipeline(
                pipeline,
                local_store_templates=local_store_templates,
            )
        # Start one causal run tree.
        self._parent_runner_id = None
        self._scheduler_error = None
        self.consume_event(Event(
            ocean_id=self.id,
            source="ocean",
            source_id=self.id,
            type=OceanEvents.RUN_PIPELINE,
            cause=Cause(description="pipeline run"),
            payload=PipelineRun(
                pipeline.id,
                pipeline.name,
                mode,
                dict(local_store_templates),
                stay_alive,
                name,
                dynamic_ref_plan,
            ),
        ))
        self.start_threaded(stay_alive=False)
        # The scheduler thread ending means the causal run tree has settled.
        wait_timeout = (
            self.timeout
            if timeout is _USE_OCEAN_TIMEOUT
            else timeout
        )
        self.thread.join(timeout=wait_timeout)
        if self.thread.is_alive():
            raise TimeoutError(
                "Timed out waiting for TIDE pipeline evidence; "
                "the timeout does not terminate a running Python worker"
            )
        # Propagate an unexpected scheduler failure with available evidence.
        if self._scheduler_error is not None:
            error = self._scheduler_error
            if self._parent_runner_id in self.runners:
                trace = self._trace_from_runner(
                    self.runners[self._parent_runner_id]
                )
                self.last_trace = trace
                error.tide_trace = trace
            raise error
        if self._parent_runner_id not in self.runners:
            raise RuntimeError(
                "The run completed without creating its parent PipelineRunner"
            )
        # Reconstruct the complete causal run tree from its parent Runner.
        parent_runner = self.runners[self._parent_runner_id]
        trace = self._trace_from_runner(parent_runner)
        self.last_trace = trace
        # Preserve the parent Runner's ordinary TIDE failure handling.
        if parent_runner.failure is not None:
            parent_runner.failure.tide_trace = trace
            raise parent_runner.failure
        return trace
    def _trace_from_runner(self, runner):
        """Delegate settled runtime evidence to the tracing boundary."""
        return trace_from_event_log(
            self.event_logger.freeze(),
            ocean_id=self.id,
            runner=runner,
            pipelines={
                pipeline_id: pipeline
                for pipeline_id, (pipeline, _) in self.pipelines.items()
            },
        )
    def start_threaded(self, stay_alive=True):
        if self.closed:
            raise RuntimeError("Ocean is closed")
        if self.thread is not None and self.thread.is_alive():
            raise RuntimeError("Ocean already has an active scheduling thread")
        self.done = False
        self._scheduler_error = None
        def run_ocean():
            try:
                self.start(stay_alive)
            except BaseException as error:
                self._poison_scheduler(error)
        caller_context = copy_context()
        self.thread = threading.Thread(
            target=caller_context.run,
            args=(run_ocean,),
            daemon=True,
        )
        self.thread.start()
        return self.thread
    def run_node(
        self,
        node,
        mode="sequential",
        local_store_templates=None,
        stay_alive=False,
        timeout=_USE_OCEAN_TIMEOUT,
        dynamic_ref_plan=None,
        name=None,
        return_trace=False,
        default_store=None,
    ):
        if not isinstance(node, Node):
            raise TypeError("run_node() requires a Node")
        node_def_id = node.id
        pipeline = Pipeline(
            nodes=[node],
            name=f"TIDE: .run_node({node.node_name})",
        )
        trace = self.run_pipeline(
            pipeline,
            mode=mode,
            local_store_templates=local_store_templates,
            stay_alive=stay_alive,
            timeout=timeout,
            dynamic_ref_plan=dynamic_ref_plan,
            name=name,
            default_store=default_store,
        )
        if return_trace:
            return trace
        exec_records = [
            record
            for record in trace._log
            if record.pipeline_id == pipeline.id
            and record.base_node_id == node_def_id
        ]
        if not exec_records:
            error = NoExecRecordError(
                f"Node {node.node_name!r} did not produce an execution record"
            )
            error.tide_trace = trace
            raise error
        return exec_records[-1].output_value
    def _on_run_pipeline(self, event):
        payload = event.payload
        pipeline, registered_templates = self._get_pipeline(
            payload.pipeline_id
        )
        local_store_templates = (
            payload.local_store_templates
            or registered_templates
        )
        runner = self._create_pipeline_runner(
            pipeline,
            payload.mode,
            local_store_templates,
            dynamic_ref_plan=payload.dynamic_ref_plan,
            name=payload.runner_name,
        )
        # Retain this Runner's own starting Event.
        runner.runner_start_event_id = event.event_id
        # Retain the causal boundary inherited from the source Runner.
        source_runner = self.runners.get(event.source_id)
        runner.trace_event_start_id = (
            source_runner.trace_event_start_id
            if event.exec_id is not None and source_runner is not None
            else event.event_id
        )
        # Publish the exact immutable plan before any work from this Runner.
        # Its sole purpose is restoring the same plan onto a later Trace.
        self.consume_event(Event(
            ocean_id=self.id,
            source="runner",
            source_id=runner.id,
            type=LogEvents.RUNNER_PLAN,
            cause=Cause.from_event(event),
            target_runner_id=runner.id,
            payload=RunnerPlan(copy.deepcopy(runner._tide_plan)),
        ))
        # An Ocean-originated Runner is the parent of this run tree.
        if (
            event.source == "ocean"
            and event.source_id == self.id
            and event.exec_id is None
        ):
            self._parent_runner_id = runner.id
        runner.remember_initial_ready_nodes(event)
        self.event_router.subscribed_runners[runner.id] = runner
        self._start_runner(runner.id, payload.stay_alive)
    def _on_kill_runner(self, event):
        runner_id = event.target_runner_id
        runner = self.running.pop(runner_id, None)
        if runner is None:
            return
        # Stop routing and executing work for this Runner.
        self.event_router.subscribed_runners.pop(runner_id, None)
        self.event_router.retired_runner_ids.add(runner_id)
        self.executor.cancel_runner(runner_id)
        runner.on_kill(event)
    def _next_pipeline_id(self):
        return len(self.pipelines)
    def _next_runner_id(self):
        return len(self.runners)
    def _get_pipeline(self, pipeline_id):
        # Return one registered Pipeline definition and its Store seeds.
        if pipeline_id not in self.pipelines:
            raise KeyError(
                f"Pipeline {pipeline_id!r} is not registered"
            )
        return self.pipelines[pipeline_id]
    def _create_pipeline_runner(
        self,
        pipeline,
        mode,
        local_store_templates=None,
        dynamic_ref_plan=None,
        name=None,
    ):
        # Assign this Pipeline occurrence its Runner identity.
        runner_id = self._next_runner_id()
        # Create the actual local Stores owned by this Runner.
        self._create_runner_stores(
            runner_id,
            local_store_templates,
        )
        # Create one Runner for this Pipeline occurrence.
        runner = PipelineRunner(
            id=runner_id,
            ocean_id=self.id,
            pipeline=pipeline,
            event_interface=self.event_interface,
            store_router=self.store_router,
            now_coordinator=self._coordinate_now,
            synchronous_event_router=self._route_scheduler_consequence,
            pipeline_lookup=self._get_pipeline,
            parallel=mode,
            dynamic_ref_plan=dynamic_ref_plan,
            name=name,
        )
        self.runners[runner.id] = runner
        return runner
    def _get_runner(self, runner_id):
        # Return one registered Runner.
        if runner_id not in self.runners:
            raise KeyError(
                f"Runner {runner_id!r} is not registered"
            )
        return self.runners[runner_id]
    def _start_runner(
        self,
        runner_id,
        stay_alive=False,
    ):
        # Register and start one active Runner.
        runner = self._get_runner(runner_id)
        self.running[runner.id] = runner
        runner.run(stay_alive)
    def _advance_pipeline_runners(self):
        # Advance every Runner or request removal after proven settlement.
        for runner in list(self.running.values()):
            if runner.purgatory and runner.complete and runner._is_idle() and not runner.stay_alive:
                self.consume_event(Event(
                    ocean_id=self.id,
                    source="ocean",
                    source_id=self.id,
                    type=OceanEvents.KILL_RUNNER,
                    cause=Cause(
                        description="automatic runner completion"
                    ),
                    target_runner_id=runner.id,
                ))
                continue
            runner.tick()
    def _idle(self):
        # Report whether the complete Ocean has no work in flight.
        return (
            self._scheduler_queue.empty()
            and not self.store_router.event_queue
            and self.executor.idle()
            and all(
                runner._is_idle()
                for runner in self.running.values()
            )
        )
    def tick(
        self,
        stay_alive=False,
        external=False,
    ):
        # Allow only one scheduling turn at a time.
        with self._scheduler_context():
            try:
                try:
                    work = self._scheduler_queue.get_nowait()
                except Empty:
                    work = None
                if work is not None:
                    try:
                        if isinstance(work, _EventWork):
                            self.event_router.route(work.event)
                        elif isinstance(work, _SynchronousWork):
                            self._complete_synchronous_work(work)
                        else:
                            raise TypeError(
                                "Unsupported scheduler work: "
                                f"{type(work).__name__}"
                            )
                    finally:
                        self._scheduler_queue.task_done()
                # Advance each runtime authority in dependency order.
                self.store_router.tick()
                self.executor.tick()
                self._advance_pipeline_runners()
                # Classify every locally inactive Runner at settlement.
                if self._idle():
                    for runner in self.running.values():
                        if not runner.purgatory:
                            continue
                        runner.complete = True
                        runner.deadlock = runner._deadlock_error()
                        waiting = runner._deadlock_waiting()
                        if (
                            waiting
                            and not runner.stay_alive
                            and not runner.deadlock_reported
                        ):
                            runner.deadlock_reported = True
                            self.consume_event(Event(
                                ocean_id=self.id,
                                source="runner",
                                source_id=runner.id,
                                type=LogEvents.RUNNER_DEADLOCK,
                                cause=Cause(
                                    description=(
                                        "runner settled with unavailable "
                                        "dependencies"
                                    )
                                ),
                                target_runner_id=runner.id,
                                payload=waiting,
                            ))
                if (
                    not external
                    and not stay_alive
                    and not self.running
                    and self._idle()
                ):
                    self.done = True
            finally:
                pass
    @contextmanager
    def _scheduler_context(self):
        """Give one synchronous operation explicit scheduler ownership."""
        with self._tick_lock:
            previous_owner = self._scheduler_owner_thread_id
            self._scheduler_owner_thread_id = threading.get_ident()
            try:
                yield
            finally:
                self._scheduler_owner_thread_id = previous_owner
    def _wait_for_scheduler_work(self, stay_alive):
        # Continue immediately while routed work is waiting.
        if (
            not self._scheduler_queue.empty()
            or self.store_router.event_queue
        ):
            return
        # Poll only while Python execution is still running.
        if self.executor.running_tasks:
            wait_timeout = 0.001
        # Recheck a persistent Runner's stay-alive flag even without an Event.
        elif (
            self.running
            and all(
                runner.stay_alive and runner.purgatory
                for runner in self.running.values()
            )
        ):
            wait_timeout = 0.01
        elif stay_alive and not self.running:
            wait_timeout = None
        else:
            return
        # Clear, recheck, then wait so an arriving Event cannot be missed.
        self._wake_event.clear()
        if (
            not self._scheduler_queue.empty()
            or self.store_router.event_queue
        ):
            return
        self._wake_event.wait(timeout=wait_timeout)
    def start(self, stay_alive=True):
        if self.closed:
            raise RuntimeError("Ocean is closed")
        self.done = False
        try:
            while not self.done:
                self.tick(stay_alive=stay_alive)
                if not self.done:
                    self._wait_for_scheduler_work(stay_alive)
        except BaseException as error:
            self._fail_pending_synchronous_work(error)
            raise
        finally:
            if self.done:
                self._fail_pending_synchronous_work(
                    RuntimeError("Ocean scheduler stopped")
                )
    def consume_event(self, event):
        # Queue one Event and wake the Ocean scheduler.
        if not isinstance(event, Event):
            raise TypeError("Ocean scheduler accepts only Event evidence")
        if self.closed:
            raise RuntimeError("Ocean is closed")
        if event.ocean_id != self.id:
            raise ValueError(
                "Event ocean identity disagrees with its destination Ocean: "
                f"{event.ocean_id!r} != {self.id!r}"
            )
        with self._tick_lock:
            self._scheduler_queue.put(_EventWork(event))
        self._wake_event.set()
    def _poison_scheduler(self, error):
        """Permanently close an Ocean after an unexpected scheduler fault."""
        self._scheduler_error = error
        self.done = True
        self.closed = True
        self._wake_event.set()
        self._fail_pending_synchronous_work(error)
        # No queued Event or partially active Runner may survive into another
        # run. The immutable Event history remains available for diagnosis.
        while True:
            try:
                self._scheduler_queue.get_nowait()
            except Empty:
                break
            else:
                self._scheduler_queue.task_done()
        active_runner_ids = tuple(self.running)
        for runner_id in active_runner_ids:
            self.executor.cancel_runner(runner_id)
            self.event_router.retired_runner_ids.add(runner_id)
        self.running.clear()
        self.event_router.subscribed_runners.clear()
        self.executor.shutdown(wait=False, cancel_futures=True)
    def _coordinate_now(self, operation, runner_id, runtime, effect):
        """Apply one immediate operation at Ocean's linearization point."""
        return self._coordinate_synchronous_work(_NowRequest(
            operation=operation,
            runner_id=runner_id,
            runtime=runtime,
            effect=effect,
        ))
    def _coordinate_tide_event(self, apply):
        return self._coordinate_synchronous_work(
            _TIDEEventRequest(apply=apply)
        )
    def _coordinate_synchronous_work(self, request):
        if threading.get_ident() == self._scheduler_owner_thread_id:
            return self._apply_synchronous_work(request)
        with self._tick_lock:
            if self.closed:
                raise RuntimeError("Ocean is closed")
            self._scheduler_queue.put(request)
            scheduler_running = (
                self.thread is not None and self.thread.is_alive()
            )
        self._wake_event.set()
        if scheduler_running:
            request.completed.wait()
        else:
            while not request.completed.is_set():
                self.tick(external=True)
        if request.error is not None:
            if request.scheduler_error is not None:
                raise request.error from request.scheduler_error
            raise request.error
        return request.result
    def _complete_synchronous_work(self, request):
        try:
            request.result = self._apply_synchronous_work(request)
        except BaseException as error:
            if isinstance(request, _NowRequest):
                request.error = NowRequestFailed(
                    f"Immediate {request.operation} operation failed"
                )
                request.scheduler_error = error
            else:
                request.error = error
        finally:
            request.completed.set()
    def _apply_synchronous_work(self, request):
        if isinstance(request, _TIDEEventRequest):
            return request.apply()
        if isinstance(request, _RegisterPipelineRequest):
            if request.pipeline.id in self.pipelines:
                return self.pipelines[request.pipeline.id][0]
            if request.name is not None:
                request.pipeline.name = request.name
            event = Event(
                ocean_id=self.id,
                source="ocean",
                source_id=self.id,
                type=OceanEvents.REGISTER_PIPELINE,
                cause=Cause(description="public pipeline registration"),
                payload=PipelineRegistration(
                    request.pipeline,
                    request.local_store_templates,
                ),
            )
            self._route_scheduler_consequence(event)
            return request.pipeline
        if isinstance(request, _NowRequest):
            return self._apply_now_request(
                request.operation,
                request.runner_id,
                request.runtime,
                request.effect,
            )
        raise TypeError(
            "Unsupported synchronous scheduler work: "
            f"{type(request).__name__}"
        )
    def _apply_now_request(self, operation, runner_id, runtime, effect):
        runner = self._get_runner(runner_id)
        return runner._apply_now(operation, runtime, effect)
    def _route_scheduler_consequence(self, event):
        if threading.get_ident() != self._scheduler_owner_thread_id:
            raise RuntimeError(
                "Only Ocean's scheduler may synchronously route an Event"
            )
        self.event_router.route(event)
    def _fail_pending_synchronous_work(self, scheduler_error):
        retained = []
        while True:
            try:
                work = self._scheduler_queue.get_nowait()
            except Empty:
                break
            try:
                if isinstance(work, _SynchronousWork):
                    if isinstance(work, _NowRequest):
                        work.error = NowRequestFailed(
                            f"Immediate {work.operation} operation was interrupted"
                        )
                    else:
                        work.error = RuntimeError(
                            "Synchronous scheduler operation was interrupted"
                        )
                    work.scheduler_error = scheduler_error
                    work.completed.set()
                else:
                    retained.append(work)
            finally:
                self._scheduler_queue.task_done()
        for work in retained:
            self._scheduler_queue.put(work)
    def _handle_ocean_event(self, event):
        # Dispatch one Ocean Event to its registered transition.
        self.handlers[event.type](event)
    def _on_quit(self, event):
        # End the active Ocean scheduler.
        self.done = True
        self._wake_event.set()
    def close(self, wait=True):
        # Stop the scheduler and wake it if it is waiting.
        if self.closed:
            return
        if self._run_lock.locked() and self._scheduler_error is None:
            self._scheduler_error = RuntimeError(
                "Ocean closed before the active run produced evidence"
            )
        self.done = True
        self._wake_event.set()
        # Wait for the scheduler unless close() was called by that scheduler.
        if (
            wait
            and self.thread is not None
            and self.thread.is_alive()
            and self.thread is not threading.current_thread()
        ):
            self.thread.join()
        # Stop outstanding execution and release the worker pool.
        with self._tick_lock:
            self.executor.shutdown(
                wait=wait,
                cancel_futures=True,
            )
        self.closed = True
    def __enter__(self):
        # A closed Ocean cannot be reopened.
        if self.closed:
            raise RuntimeError("Ocean is closed")
        return self
    def __exit__(self, exc_type, exc_value, traceback):
        # Leaving the context always releases Ocean resources.
        self.close()
        return False
    def _create_runner_stores(self, runner_id, local_store_templates=None):
        local_store_templates = local_store_templates or {}
        # Accept seeds only for registered local Store templates.
        for store_name in sorted(local_store_templates):
            if self.store_registry.exists(store_name):
                raise ValueError(
                    f"Cannot seed or overwrite global Store {store_name!r}"
                )
            if not self.store_registry.exists(store_name, template=True):
                raise ValueError(
                    f"{store_name!r} is not a registered local Store template"
                )
        # Create one actual local Store for this Runner from each template.
        for store_name, template in self.store_registry._local_templates.items():
            actual_name = f"{store_name}_{runner_id}"
            if self.store_registry.exists(actual_name):
                continue
            initial = copy.deepcopy(
                local_store_templates.get(store_name)
            )
            self.store_registry.register(type(template)(
                name=actual_name,
                store_template=initial,
                scope="local",
                parent_runner_id=runner_id,
            ))
