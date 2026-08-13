from queue                          import Queue, Empty
from typing                         import Any
from collections                    import defaultdict
import threading
from contextvars                    import copy_context
from tide_tooling.tooling_core                           import debug_print as print

from tide_tooling.tooling_core.engine.classes            import Node, Pipeline
from tide_tooling.tooling_core.engine.stores             import MemoryKVStore
from tide_tooling.tooling_core.engine.events             import OceanEvents, PipelineEvents, Event
from tide_tooling.tooling_core.engine.tide_runtime_logging    import EventLogger, LogTools
from tide_tooling.tooling_core.engine.routers            import EventRouter, StoreRouter
from tide_tooling.tooling_core.engine.executor           import Executor
from tide_tooling.tooling_core.engine.interfaces         import EventInterface, StoreInterface
from tide_tooling.tooling_core.engine.registries         import StoreRegistry
from tide_tooling.tooling_core.engine.PipelineRunner     import PipelineRunner 
from tide_tooling.tooling_core.engine.errors             import StoreAlreadyRegisteredError

_USE_OCEAN_TIMEOUT = object()

class Ocean:
    __ocean_count__ = 0
    def __init__(
        self,
        deep_copy=True,
        timeout=None,
        max_idle_attempts=400000,
        track_unrepresented=True,
    ):
        if not isinstance(track_unrepresented, bool):
            raise TypeError("track_unrepresented must be a bool")
        self.id = Ocean.__ocean_count__
        Ocean.__ocean_count__ += 1    
        self.done = False
        self.timeout = timeout
        self.max_idle_attempts = max_idle_attempts
        self.track_unrepresented = track_unrepresented
        self.closed = False
        self.last_trace = None
        self.thread=None
        self.trace_bus = None
        self._tick_lock = threading.RLock()
        self._run_lock = threading.Lock()
        self._wake_event = threading.Event()
        self.runners = {}
        self.running = {}
        self.pipelines = {}
        self.pipeline_to_runners = defaultdict(list)
        self.runner_to_trace = {}
        
        
        self.handlers = {

            OceanEvents.SET: self._on_set,
            OceanEvents.QUIT: self._on_quit,
            OceanEvents.DELETE: self._on_delete,

            OceanEvents.RUN_PIPELINE: self._on_run_pipeline,
            OceanEvents.KILL_PIPELINE: self._on_kill_pipeline,
            OceanEvents.REGISTER_PIPELINE: self._on_register_pipeline,

        }
        # TODO: - Keep an Execution and StoreRouter ref-> value snapshot index. Then flag is exec val and store_set value are different. If so flag mutable/changed, and attr unknown prod.
        # TODO: The same with StoreRouter read -> DepUseRecord. If StoreRouter snapshot is different to read attr unknown producer and flag mutable/changed.    
        # Pipeline Event Queue
        self.event_queue = Queue()

        # Store Registery    
        self.store_registry: StoreRegistry = StoreRegistry()

        # EventLogger, Router, and Interface (For PipelineRunner(s)) and subscribe ocean to ocean events 
        self.event_logger = EventLogger()
        self.event_router = EventRouter(self.event_logger)
        self.event_router.subscribe(OceanEvents, self._handle_ocean_event)
        self.event_interface = EventInterface(self.consume_event, self.event_router.subscribe)

        # Store Router + default stores init
        self.store_router = StoreRouter(self.store_registry, self.event_interface) 
        self.register_store("global", MemoryKVStore)
        self.register_store("mem", MemoryKVStore, scope="local")
        self.register_store("__tide__", MemoryKVStore, scope="local")
        self.register_store("__unnamed__", MemoryKVStore, scope="local")
        self.store_interface = StoreInterface(self._create_scoped_router)

        # Executor - executes work | Sequential, Threaded, Process, Auto
        self.executor = Executor(event_interface=self.event_interface)
        
        # Logging tools        
        self.log_tools = LogTools(ocean_id=self.id, runners=self.runners, pipelines=self.pipelines)

    # -- User API -- #

    # | SEND EVENT INTO TIDE | # TODO: add safety features
    def consume_event(self, event: Event, verbose=False) -> None:
        """Enqueue an event safely from any producer thread.

        Event handling is performed by ``tick`` under one Ocean-wide lock;
        producer threads never execute handlers directly.
        """
        self.event_queue.put(event)
        self._wake_event.set()

        if verbose:
            print(f"[OCEAN] Consumed {event.event_type}")    
    
    # | REGISTERING STORES |
    def register_store(self, name:str=None, store_class=MemoryKVStore, scope:str="global", store_template:Any=None):
        if not name: name = store_class.reference 
        store = store_class(name = name, scope=scope, store_template=store_template)
        
        if self.store_registry.exists(name):
            raise StoreAlreadyRegisteredError(f"Store:{name}|{scope} is already registered! Please choose a different name :D", store)

        self.store_registry.register(store)  

    # | REGISTER A PIPELINE DEFINITON | #
    def register_pipeline(self, pipeline: Pipeline | list[Node], name=None, mem_store_templates: dict[str, Any] | None=None) -> None:
        if isinstance(pipeline, Pipeline) and name:
            pipeline.name = name
        mem_store_templates = mem_store_templates or {}
        if isinstance(pipeline, list):
            pipeline = Pipeline(stages=pipeline, name=name) 
        if pipeline.id is not None and pipeline.id in self.pipelines:
            print(f"[OCEAN] Pipeline [{pipeline.name}] is already registered") 
            return self.pipelines[pipeline.id][0] 
        pipeline.id = self._next_pipeline_id()
        self.pipelines[pipeline.id] = (pipeline, mem_store_templates)
        print(f"[OCEAN] Registered Pipeline [{pipeline.id}] {pipeline.name}")

       
        return pipeline
    
    # | START A PIPELINE RUN FROM SPECIFIED PIPELINE DEFINITON |
    def run_pipeline(self, pipeline: Pipeline, mode='auto', mem_store_templates: dict[str, Any] | None = None, stay_alive=False, overload_runner_ids_with: str = None, timeout=_USE_OCEAN_TIMEOUT, intent_dynamic_refs=None):
        """Run one synchronous public pipeline call.

        An Ocean may manage runners created by effects, but its public
        ``run_pipeline`` result bus belongs to one caller at a time.
        """
        if not self._run_lock.acquire(blocking=False):
            raise RuntimeError(
                "Ocean.run_pipeline does not support concurrent public calls"
            )
        try:
            return self._run_pipeline(
                pipeline,
                mode=mode,
                mem_store_templates=mem_store_templates,
                stay_alive=stay_alive,
                overload_runner_ids_with=overload_runner_ids_with,
                timeout=timeout,
                intent_dynamic_refs=intent_dynamic_refs,
            )
        finally:
            self._run_lock.release()

    def _run_pipeline(self, pipeline: Pipeline, mode='auto', mem_store_templates: dict[str, Any] | None = None, stay_alive=False, overload_runner_ids_with: str = None, timeout=_USE_OCEAN_TIMEOUT, intent_dynamic_refs=None):
        if self.closed:
            raise RuntimeError("Ocean is closed")
        if self.thread is not None and self.thread.is_alive():
            raise RuntimeError(
                "Ocean already has an active scheduling thread; finish or "
                "close that run before starting another"
            )

        # ``@pipeline`` exposes a cached builder function.  Accept that public
        # form as well as an already-built Pipeline so callers do not need to
        # know about the decorator's internal wrapper.
        if callable(pipeline) and not isinstance(pipeline, Pipeline):
            pipeline = pipeline()

        if not isinstance(pipeline, Pipeline):
            raise TypeError(
                "run_pipeline() expected a Pipeline or an @pipeline-decorated "
                f"callable, got {type(pipeline).__name__}"
            )

        mem_store_templates = mem_store_templates or {}
        if pipeline.id in self.pipelines and not mem_store_templates:
            pipeline, mem_store_templates = self.pipelines[pipeline.id]
        else:
            pipeline = self.register_pipeline(pipeline=pipeline, mem_store_templates=mem_store_templates)

        self.consume_event(Event(source="ocean", source_id=self.id, target_runner_id=None, event_type=OceanEvents.RUN_PIPELINE, payload=(pipeline.id, mode, mem_store_templates, stay_alive, overload_runner_ids_with, intent_dynamic_refs)))
        trace_bus = self.start_threaded(stay_alive=stay_alive)
        wait_timeout = self.timeout if timeout is _USE_OCEAN_TIMEOUT else timeout

        try:
            result = trace_bus.get(timeout=wait_timeout)
            if isinstance(result, Exception):
                raise result
            if (
                isinstance(result, tuple)
                and len(result) == 2
                and isinstance(result[1], Exception)
            ):
                trace, error = result
                self.last_trace = trace
                error.tide_trace = trace
                raise error

            if self.thread is not None:
                self.thread.join(timeout=5)

            self.last_trace = result
            return result

        except Empty:
            if self.thread is not None:
                self.thread.join(timeout=1)

            raise TimeoutError(
                "Timed out waiting for TIDE pipeline evidence; the timeout "
                "does not terminate a running Python worker"
            )
    
    def run_node(self, node:Node, mode='sequential', stay_alive=False, pipeline_name:str=None, overload_runner_source:str=None, show=False):
        if not pipeline_name: 
            pipeline_name = f"PIPELINE[{len(self.pipelines)}]"
        node_pipeline = Pipeline(stages=[node], name=pipeline_name)
        node_pipeline._trace_include_prior_events = True
        print(node_pipeline.stages)
        trace = self.run_pipeline(pipeline=node_pipeline, mode=mode, stay_alive=stay_alive, overload_runner_ids_with=overload_runner_source)
        runner_source = overload_runner_source if overload_runner_source is not None else 0
        inspected_actor = trace.inspect((self.id, runner_source, node.node_name), "actor")
        if not inspected_actor.history:
            return trace, None, None
        inspected_exec = trace.inspect(inspected_actor.history[-1], "exec_record")
        exec_log = inspected_exec.object.object
        output = exec_log.output_value
        # if overload_runner_source is not None:
        #     exec_log.source_runner = overload_runner_source
        #     exec_log.source_pipeline = pipeline_name
        #     producer = exec_log.producer_id
        #     producer.id = (producer.source_ocean, overload_runner_source, producer.source_actor)
        if show:
            trace.show()
        return trace, exec_log, output
    
    # | CREATE REPORT OF RUN | # TODO: finish
    def create_runner_report(self, runner_id):
        raise NotImplementedError(
            "Graph reports are provided by an external TIDE graphing module"
        )


    # -- OCEAN CONTROLS -- #
    def _next_pipeline_id(self):
        return len(self.pipelines)
    
    def _next_runner_id(self):
        return len(self.runners)

    # | GET PIPELINE DEFINITON | 
    def _get_pipeline(self, pipeline_id: int = None) -> Pipeline:
        
        registered = self.pipelines.get(pipeline_id, None)
        if not registered:
            print(
                f"pipeline_id {pipeline_id!r} is not registered; "
                f"known pipelines: {list(self.pipelines)}"
            )
            return (None, None)
        pipeline, pipeline_store_templates = registered
        return pipeline, pipeline_store_templates
    
    # | CREATE PIPELINE RUNNER (INSTANCE) WITH A SELECTED PIPELINE DEFINITON |
    def _create_pipeline_runner(self, pipeline: Pipeline, parallel_mode: str, mem_store_templates: dict[str, Any], name=None, intent_dynamic_refs=None) -> PipelineRunner:
        # parallelise/thread runner instances. 
        runner = PipelineRunner(
            id=self._next_runner_id(),
            name=name,
            pipeline=pipeline,
            parallel=parallel_mode,
            event_interface=self.event_interface,
            store_interface=self.store_interface,
            mem_store_templates=mem_store_templates,
            max_attempts=self.max_idle_attempts,
            track_unrepresented=self.track_unrepresented,
            intent_dynamic_refs=intent_dynamic_refs,
        )
        self.register_pipeline(pipeline)

       
        self._register_runner(runner)
        self.pipeline_to_runners[pipeline.id].append(runner.id)
        return runner
    
    # | REGISTER PIPELINE RUNNER |
    def _register_runner(self, runner: PipelineRunner) -> None:
        print(f"[OCEAN] Registered Runner [{runner.id}]")
        self.runners[runner.id] = runner

    # | GET PIPELINE RUNNER | 
    def _get_runner(self, runner_id: int) -> PipelineRunner:
        runner = self.runners.get(runner_id)
        if not runner:
            print(f"no runner found with runner_id: {runner_id}")
            return None
        return runner 

    # | GET ALL RUNNER IDS |
    def _get_all_runner_ids(self):
        runner_ids = [id for id in self.runners.keys()]
        if not runner_ids:
            print("No runners found at the moment!")
            return
        return runner_ids
    
    # | CREATE A SCOPED STORE ROUTER INSTANCE FOR PIPELINE RUNNERS |
    def _create_scoped_router(self, runner_id, mem_store_templates: dict[str, dict] | None = None):
        mem_store_templates = mem_store_templates or {}
        aliases = {}

        for store_name in sorted(mem_store_templates):
            if store_name in self.store_registry._stores:
                raise ValueError(
                    "mem_store_templates cannot seed or overwrite global store "
                    f"{store_name!r}; initialise global data once with "
                    "Ocean.register_store(..., scope='global', "
                    "store_template=...)."
                )
            if store_name not in self.store_registry._local_templates:
                raise ValueError(
                    "mem_store_templates can only seed registered local "
                    f"MemoryKVStore stores; {store_name!r} is not registered "
                    "as a local store."
                )
            if not isinstance(
                self.store_registry._local_templates[store_name],
                MemoryKVStore,
            ):
                raise ValueError(
                    "mem_store_templates can only seed registered local "
                    f"MemoryKVStore stores; {store_name!r} uses "
                    f"{type(self.store_registry._local_templates[store_name]).__name__}."
                )

        for name, store in list(self.store_registry._local_templates.items()):
            store_name = name
            scoped_name = store_name + f"_{runner_id}"

            if not self.store_registry.exists(scoped_name):
                store_template = mem_store_templates.get(store_name, None)
                self.store_registry.register(type(store)(name=scoped_name, store_template=store_template, scope="local", parent_runner_id=runner_id))

            self.store_router.store_alias[store_name].add(scoped_name)
            aliases.update({store_name: scoped_name})

        return StoreRouter(
            self.store_registry,
            self.event_interface,
            aliases,
            subscribe_events=False,
        )
    
    # | START A PIPELINE RUN AND REGISTER AS RUNNING |
    def _start_runner(self, runner_id: PipelineRunner=None, stay_alive: bool=False) -> None:
        runner = self._get_runner(runner_id)
        self.running[runner_id] = runner # register runner as running
        self.running[runner_id].run(stay_alive)

    # | TICK ALL ACTIVE ('RUNNING') PIPELINE RUNNERS
    def _advance_pipeline_runners(self):
        ocean_has_pending_work = (
            not self.event_queue.empty()
            or bool(self.store_router.event_queue)
            or not self.executor.idle()
        )

        for runner in list(self.running.values()):
            if runner.purgatory and not runner.stay_alive and runner.complete:
                print(f"killing runner {runner.id}")
                self.consume_event(
                    Event(
                        source="ocean",
                        source_id=self.id,
                        target_runner_id=runner.id,
                        event_type=OceanEvents.KILL_PIPELINE,
                    )
                )
                continue

            if ocean_has_pending_work and runner.purgatory:
                continue

            runner.tick()
                
    def tick(self, stay_alive=False, external=False):
        """Advance one serialized Ocean scheduling turn.

        Only one caller may route events, mutate stores, collect executor
        results, or advance runners at a time, even if ``tick`` is invoked
        concurrently outside the normal Ocean thread.
        """
        with self._tick_lock:
            try:
                event = self.event_queue.get_nowait()
            except Empty:
                event = None

            if event is not None:
                try:
                    self.event_router.route(event)
                finally:
                    self.event_queue.task_done()

            self.store_router.tick()
            self.executor.tick()
            self._advance_pipeline_runners()

            if self._idle():
                if external:
                    print("the tide wanes")
                    return

                if not stay_alive and not self.running:
                    print("The tide wanes | ocean is idle!")
                    self.done = True
                    print("DONE")
                    return

        

        
            

    # | START OCEAN CLI s
    def start(self, stay_alive=True) -> None:
        if self.closed:
            raise RuntimeError("Ocean is closed")
        self.done = False
        print("The ocean roars!")
        # if stay_alive:
        #     start_event_cli(self.event_interface, self.store_router)
        
        while not self.done:
            self.tick(stay_alive)
            if not self.done:
                self._wait_for_scheduler_work(stay_alive)
        print("Finished")

        self._on_quit(None)
        
        
    
    def start_threaded(self, stay_alive=True):
        if self.closed:
            raise RuntimeError("Ocean is closed")
        if self.thread is not None and self.thread.is_alive():
            raise RuntimeError("Ocean already has an active scheduling thread")
        self.done = False
        trace_bus = Queue()
        self.trace_bus = trace_bus

        def run_ocean():
            try:
                self.start(stay_alive)
            except Exception as error:
                if self.running:
                    runner_id, runner = next(
                        reversed(self.running.items())
                    )
                    self.executor.cancel_runner(runner_id)
                    trace = self.log_tools.create_trace(
                        runner,
                        self.event_logger.log,
                    )
                    self.last_trace = trace
                    self.runner_to_trace[runner_id] = trace
                    error.tide_trace = trace
                    self.running.pop(runner_id, None)
                    self.event_router.subscribed_runners.pop(
                        runner_id,
                        None,
                    )
                trace_bus.put(error)

        caller_context = copy_context()
        self.thread = threading.Thread(
            target=caller_context.run,
            args=(run_ocean,),
            daemon=True,
        )
        self.thread.start()

        return trace_bus

    # -- OCEAN ON-EVENTS -- #

    # | EVENT CONSUMER |
    def _handle_ocean_event(self, event: Event) -> None:
        self.handlers[event.event_type](event)
    
    # | ON_EVENT: QUIT OCEAN | # TODO: doesnt actually work rn i thihk at least in event runtime
    def _on_quit(self, event: Event):
        print("quitting...")
        self.done = True
        return

    # | ON_EVENT: SET VALUE TO STORE |
    def _on_set(self, event: Event):
        raise NotImplementedError(
            "OceanEvents.SET has no store-write payload contract; use "
            "RouterEvents.STORE_WRITE"
        )
    
    # | ON_EVENT: DELETE VALUE FROM STORE | 
    def _on_delete(self, event: Event):
        raise NotImplementedError(
            "OceanEvents.DELETE has no store-delete payload contract; use "
            "RouterEvents.DELETE"
        )
    
    # | ON_EVENT: BUILD | NOTE: currently handled in event_runtime, think about moving logic here.
    def _on_build(self, event: Event):
        pass

    # | ON_EVENT: REGISTER PIPELINE | NOTE: No current register pipeline event implementation
    def _on_register_pipeline(self, event: Event):
        raise NotImplementedError(
            "OceanEvents.REGISTER_PIPELINE has no event payload contract; "
            "call Ocean.register_pipeline()"
        )

    # | ON_EVENT: REGISTER PIPELINE |
    def _on_run_pipeline(self, event: Event):
        # Unpack event
        if len(event.payload) == 5:
            (
                pipeline_id,
                parallel_mode,
                mem_store_templates,
                stay_alive,
                name,
            ) = event.payload
            intent_dynamic_refs = None
        else:
            (
                pipeline_id,
                parallel_mode,
                mem_store_templates,
                stay_alive,
                name,
                intent_dynamic_refs,
            ) = event.payload
        pipeline, pipeline_store_templates = self._get_pipeline(pipeline_id)
        # Choose store templates
        mem_store_templates = mem_store_templates or pipeline_store_templates or {} # pipeline_store_templates always defaults already but {} to be safe however probs issue is falling back
        # Create a pipeline runner instance
        runner = self._create_pipeline_runner(
            pipeline,
            parallel_mode,
            mem_store_templates,
            name=name,
            intent_dynamic_refs=intent_dynamic_refs,
        )
        parent_runner = self.runners.get(event.source_id)
        if getattr(pipeline, "_trace_include_prior_events", False):
            runner.trace_event_start_id = None
        elif event.exec_id is not None and parent_runner is not None:
            runner.trace_event_start_id = getattr(
                parent_runner,
                "trace_event_start_id",
                event.id,
            )
        else:
            runner.trace_event_start_id = event.id
        # subscribe the runner to PipelineEvents
        self.event_router.subscribed_runners[runner.id] = runner
        print(f"Subscribed runner to router {self.event_router.subscribed_runners}")
        # start pipeline runner
        self._start_runner(runner.id, stay_alive=stay_alive)

    # | ON_EVENT: KILL PIPELINE |
    def _on_kill_pipeline(self, event: Event):
        # get runner id to kill
        target_runner_id = event.target_runner_id
        # if not running - return
        if target_runner_id not in self.runners:
            print("Runner doesnt exist!")
            return
        # if running:
        if target_runner_id in self.running:
            # remove from running list and unsub from PipelineEvents
            del self.running[target_runner_id]
            del self.event_router.subscribed_runners[target_runner_id]
            
            # Get the actual runner and call runner.kill() 
            target_runner = self.runners[target_runner_id]
            self.executor.cancel_runner(target_runner_id)
            target_runner.on_kill(Event(source="ocean", source_id=self.id, target_runner_id=target_runner_id, event_type=PipelineEvents.KILL))
            
            # Create a trace and process logs
            trace = self.log_tools.create_trace(target_runner, self.event_logger.log)
            self.last_trace = trace
            outcome = (
                (trace, target_runner.failure)
                if target_runner.failure is not None
                else trace
            )
            self.trace_bus.put(outcome)
            print("PUT trace into bus")
            self.runner_to_trace[target_runner_id] = trace
            # trace.show()
            
            return
        print(f"Specified runner {target_runner_id} is not running!")
        return

    def close(self, wait=True):
        """Stop Ocean activity and release executor resources."""
        if self.closed:
            return
        self.done = True
        self._wake_event.set()
        if (
            self.trace_bus is not None
            and self.thread is not None
            and self.thread.is_alive()
        ):
            self.trace_bus.put(
                RuntimeError("Ocean closed before the active run produced evidence")
            )
        if (
            self.thread is not None
            and self.thread.is_alive()
            and self.thread is not threading.current_thread()
        ):
            self.thread.join(timeout=5 if wait else 0)
        with self._tick_lock:
            self.executor.shutdown(wait=wait, cancel_futures=True)
        self.closed = True

    def __enter__(self):
        if self.closed:
            raise RuntimeError("Ocean is closed")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        return False
    
    
# helpers keeping the ocean clean 
    def _wait_for_scheduler_work(self, stay_alive):
        """Avoid a busy loop while preserving prompt event handling.

        Worker completion is polled at a short interval because futures do not
        write directly into Ocean's event queue.  A purgatory runner explicitly
        kept alive, or an otherwise empty stay-alive Ocean, waits longer for an
        external event.  ``consume_event`` wakes each wait immediately.
        """
        if not self.event_queue.empty() or self.store_router.event_queue:
            return

        wait_timeout = None
        if self.executor.running_tasks:
            wait_timeout = 0.001
        elif self.running and all(
            runner.stay_alive and runner.purgatory
            for runner in self.running.values()
        ):
            wait_timeout = 0.01
        elif stay_alive and not self.running:
            wait_timeout = 0.05

        if wait_timeout is None:
            return

        self._wake_event.clear()
        if not self.event_queue.empty() or self.store_router.event_queue:
            return
        self._wake_event.wait(timeout=wait_timeout)

    def _idle(self) -> bool:
        # print(f"[OCEAN {id(self)}] IDLE CHECK q={id(self.event_queue)} size={self.event_queue.qsize()}")
        queue_empty = self.event_queue.empty()
        exec_idle = self.executor.idle()
        all_runners_idle = all([running._is_idle() for running in self.running.values()])
        store_idle = not self.store_router.event_queue

            
        if queue_empty and exec_idle and all_runners_idle and store_idle:
            return True
        return False 
    
    def _build_graph_from_dag(self, dag, name, node_index, runner_id):
        # Graphviz rendering was intentionally removed from the core package.
        # Keep this compatibility hook so older callers fail with a useful
        # boundary rather than a missing optional dependency.
        raise NotImplementedError(
            "Graph rendering is provided by an external TIDE graphing module"
        )
