from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, wait, FIRST_COMPLETED
from .classes import FakeFuture, Event, RAISE, __TIDE_TEMPLATE_VALUE__
from pickle import PickleError
from queue import Queue
from tide_tooling.tooling_core.engine.utils import (
    _is_tide_exec_uuid,
    create_tide_exec_uuid,
    tide_snapshot_value,
)
from .events import ExecutorEvents, PipelineEvents
from .errors import (
    MissingExecutionEvidenceError,
    ReadInsideProcessPoolError,
)
from functools import partial
from multiprocessing import get_context
from tide_tooling.tooling_core import debug_print as print
from .utils import _unwrap_node_worker_result

_EXECUTION_TEMPLATE_VALUE = __TIDE_TEMPLATE_VALUE__()


class ExecMeta:
    def __init__(
        self,
        assigned_id,
        start_time="unknown",
        end_time="unknown",
        duration="unknown",
        result=None,
        status="unknown",
        error=None,
        runtime_effects=None,
        input_snapshot_before=None,
        input_snapshot_after=None,
        execution_mode=None,
        execution_evidence=None,
        handled_raise=False,
    ):
        if not _is_tide_exec_uuid(assigned_id):
            raise ValueError(
                f"ExecMeta requires a canonical TIDE execution id: {assigned_id}"
            )
        self.id = assigned_id
        self.start_time = start_time
        self.end_time = end_time
        self.duration = duration
        self.result = result
        self.snapshot = tide_snapshot_value(result, boundary="result")
        self.status = status
        self.error = error
        self.runtime_effects_snapshot = tide_snapshot_value(
            list(runtime_effects or ()),
            boundary="runtime_effects",
        )
        self.runtime_effects = self.runtime_effects_snapshot.materialise()
        self.input_snapshot_before = input_snapshot_before
        self.input_snapshot_after = input_snapshot_after
        self.execution_mode = execution_mode
        self.execution_evidence = execution_evidence
        self.handled_raise = handled_raise


def _execute_with_timing(func, assigned_id, *args, **kwargs):
    """Execute a node and return its value with portable execution metadata.

    This function intentionally lives at module scope so explicit process-pool
    execution can pickle it.  The previous nested closure could never be sent
    reliably to a ProcessPoolExecutor.
    """
    import time

    status = "succeeded"
    exception = None
    handled_raise = False
    start_time_utc = time.time()
    runtime_effects = []
    observed_inputs = {
        "args": args[1] if len(args) > 1 else args,
        "kwargs": args[2] if len(args) > 2 else kwargs,
    }
    input_snapshot_before = tide_snapshot_value(
        observed_inputs,
        boundary="input_before",
    )

    try:
        result = func(*args, **kwargs)
        result, runtime_effects = _unwrap_node_worker_result(result)
        if isinstance(result, RAISE):
            signal = result
            handled_raise = True
            status = "handled_raise"
            exception = signal.exception
            result = (
                signal.exception
                if isinstance(
                    signal.preserve_output,
                    __TIDE_TEMPLATE_VALUE__,
                )
                else signal.preserve_output
            )
            runtime_effects = [
                *runtime_effects,
                *signal.effects,
            ]
    except RAISE as signal:
        handled_raise = True
        status = "handled_raise"
        exception = signal.exception
        result = (
            signal.exception
            if isinstance(
                signal.preserve_output,
                __TIDE_TEMPLATE_VALUE__,
            )
            else signal.preserve_output
        )
        runtime_effects = list(
            getattr(signal, "_tide_runtime_effects", signal.effects)
        )
    except Exception as error:
        result = _EXECUTION_TEMPLATE_VALUE
        runtime_effects = []
        status = "failed"
        exception = error

    input_snapshot_after = tide_snapshot_value(
        observed_inputs,
        boundary="input_after",
    )
    end_time_utc = time.time()
    duration = end_time_utc - start_time_utc
    meta = ExecMeta(
        assigned_id,
        start_time_utc,
        end_time_utc,
        duration,
        result,
        status,
        exception,
        runtime_effects,
        input_snapshot_before,
        input_snapshot_after,
        handled_raise=handled_raise,
    )
    return result, meta


class Executor:
    _VALID_EVENT_TYPES = ['node_ready']
    _VALID_EXECUTION_MODES = {"auto", "thread", "process", "sequential"}
    __TIDE_TEMPLATE_VALUE = _EXECUTION_TEMPLATE_VALUE

    def __init__(self, event_interface, deep_copy=True):
        self.id = 0
        self.event_emitter = event_interface.emit_event_to_ocean
        #subscribe to events
        self.event_queue = Queue()
        event_interface.event_subscriber(ExecutorEvents, self.event_queue.put)
        self.snapshot = deep_copy
        self.waiting_tasks = Queue()
        self.running_tasks = {} #future: node_id
        self.done_tasks = [] #payload: result
        self.thread_pool_executor = ThreadPoolExecutor(max_workers=4)
        self.process_pool_executor = None
        self.opaque_value_snapshot_by_last_exec_id = {}
        self._closed = False

        self.event_handlers = {
            ExecutorEvents.NODE_READY: self.on_ready_node
        }

    def idle(self):
        if self.event_queue.empty() and not self.running_tasks.values() and self.waiting_tasks.empty() and not self.done_tasks:
            return True
        return False

    ############## 
    #   EVENTS   #
    ##############
    def handle_event(self, event):
        self.event_handlers[event.event_type](event)

    def _requeue_on_thread(self, future, reason, *, runtime_read=False):
        node_ready_event = self.running_tasks.pop(future)
        print(f"process execution unavailable; falling back to thread pool: {reason}")
        _, payload = node_ready_event.payload
        worker, args, _ = payload

        if runtime_read:
            args = list(args)
            effect_runtime = getattr(
                node_ready_event,
                "thread_effect_runtime",
                None,
            )
            if effect_runtime is None:
                effect_runtime = dict(args[3] or {})
            effect_runtime["inside_process_pool"] = False
            effect_runtime["effects"] = []
            args[3] = effect_runtime
            args = tuple(args)

        future = self.route_task_to_executor(
            worker=worker,
            assigned_id=node_ready_event.exec_id,
            payload=args,
            mode="thread",
        )
        node_id, _ = node_ready_event.payload
        node_ready_event.payload = (
            node_id,
            (worker, args, "thread"),
        )
        self.running_tasks[future] = node_ready_event

    def _handle_pickling_error(self, future, error):
        self._requeue_on_thread(future, error)

    ############### 
    # EXC WRAPPER #
    ###############
    @staticmethod
    def timed_execution(func, assigned_id):
        return partial(_execute_with_timing, func, assigned_id)


    ############### 
    #  EXECUTION  #
    ###############

    def submit(
        self,
        worker,
        payload: tuple = (),
        mode="auto",
        assigned_id=None,
    ):
        future = self.route_task_to_executor(
            worker,
            assigned_id or create_tide_exec_uuid(),
            payload,
            mode=mode,
        )
        return future

    def on_ready_node(self, node_ready_event):
        _, payload = node_ready_event.payload
        assigned_exec_id = node_ready_event.exec_id
        worker, args, mode = payload 
        future = self.route_task_to_executor(worker=worker, assigned_id=assigned_exec_id, payload=args, mode=mode) 
        self.running_tasks[future] = node_ready_event

    def route_task_to_executor(self, worker, assigned_id:str, payload:tuple=(), mode='auto'):
        if self._closed:
            raise RuntimeError("Executor is closed")
        if mode not in self._VALID_EXECUTION_MODES:
            raise ValueError(
                f"execution mode must be one of "
                f"{sorted(self._VALID_EXECUTION_MODES)}, got {mode!r}"
            )
        _WORKER = self.timed_execution(func=worker, assigned_id=assigned_id)
        _PAYLOAD = payload
        if mode == 'auto':
            # Arbitrary user callables (closures, lambdas, notebook functions,
            # decorated functions) are often not process-pickleable.  Auto mode
            # therefore uses threads; callers can still request ``process``
            # explicitly when their worker and payload satisfy multiprocessing
            # requirements.
            future = self.thread_pool_executor.submit(_WORKER, *_PAYLOAD)
            print("submitted task to thread pool")
        elif mode == 'thread':
            future = self.thread_pool_executor.submit(_WORKER, *_PAYLOAD)
            print("submitted task to thread pool")
        elif mode == 'process':
            if self.process_pool_executor is None:
                self.process_pool_executor = ProcessPoolExecutor(
                    max_workers=4,
                    mp_context=get_context("spawn"),
                )
            future = self.process_pool_executor.submit(_WORKER, *_PAYLOAD)
            print("submitted task to process pool")
        elif mode == 'sequential':
            try:
                result = _WORKER(*_PAYLOAD)
                future = FakeFuture(result)
            except Exception as e:
                raise e
            print("submitted (executed) task sequentially!")
        return future


    ################ 
    #  COLLECTION  #
    ################

    def get_result_from_future(self, future):
        return future.result()
    
    def collect_results(self):
        """Collect results that are ready without blocking the Ocean loop.

        Worker completion is polled by successive scheduling turns.  Waiting
        here previously prevented Ocean from routing kill, quit, and store
        events for the entire duration of a long-running worker.
        """
        done, _ = wait(
            self.running_tasks.keys(),
            timeout=0,
            return_when=FIRST_COMPLETED,
        )

        for future in done:
            node_ready_event = self.running_tasks.get(future)
            runner_id = node_ready_event.source_id
            node_id, _ = node_ready_event.payload
            meta = None
            try:
                result, meta = self.get_result_from_future(future)
                meta: ExecMeta
                _, task_payload = node_ready_event.payload
                _, _, execution_mode = task_payload
                meta.execution_mode = execution_mode
                meta.execution_evidence = getattr(
                    node_ready_event,
                    "execution_evidence",
                    None,
                )
                if meta.execution_evidence is None:
                    raise MissingExecutionEvidenceError(
                        f"Execution {meta.id} completed without its immutable "
                        "NODE_READY evidence"
                    )
                if (
                    isinstance(meta.error, ReadInsideProcessPoolError)
                    and not meta.handled_raise
                ):
                    self._requeue_on_thread(
                        future,
                        meta.error,
                        runtime_read=True,
                    )
                    continue
            except PickleError as e:
                self._handle_pickling_error(future, e)
                continue
            except ReadInsideProcessPoolError as e:
                self._requeue_on_thread(future, e, runtime_read=True)
                continue
            except Exception as e:
                self.running_tasks.pop(future, None)
                raise e

                        
            del self.running_tasks[future]

            payload = (node_id, result, meta)        

                    
            
            completed_node_event = Event(source="executor", source_id=self.id, target_runner_id=runner_id, event_type=PipelineEvents.RESULT_READY, payload=payload, exec_id=meta.id)
            
            self.done_tasks.append(completed_node_event)

    def cancel_runner(self, runner_id):
        """Discard pending executor work belonging to one failed runner."""
        for future, ready_event in list(self.running_tasks.items()):
            if ready_event.source_id != runner_id:
                continue
            cancel = getattr(future, "cancel", None)
            if callable(cancel):
                cancel()
            self.running_tasks.pop(future, None)

        self.done_tasks = [
            event
            for event in self.done_tasks
            if event.target_runner_id != runner_id
        ]

    def shutdown(self, wait=True, cancel_futures=True):
        """Release thread/process pools. Safe to call more than once."""
        if self._closed:
            return
        self._closed = True

        if cancel_futures:
            for future in list(self.running_tasks):
                cancel = getattr(future, "cancel", None)
                if callable(cancel):
                    cancel()
            self.running_tasks.clear()
            self.done_tasks.clear()

        self.thread_pool_executor.shutdown(
            wait=wait,
            cancel_futures=cancel_futures,
        )
        if self.process_pool_executor is not None:
            self.process_pool_executor.shutdown(
                wait=wait,
                cancel_futures=cancel_futures,
            )
            
    def emit_results(self):
        for event in list(self.done_tasks):
            self.done_tasks.remove(event)
            self.event_emitter(event)

    def tick(self):
        while not self.event_queue.empty():
            event = self.event_queue.get_nowait()
            self.handle_event(event)

        if self.running_tasks:
            self.collect_results()
        
        if self.done_tasks:
            self.emit_results()
