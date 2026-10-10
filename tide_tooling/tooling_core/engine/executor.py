from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, wait, FIRST_COMPLETED
from .classes import FakeFuture, Event, RAISE, __fallback__
from dataclasses import replace
import time
from queue import Queue
from tide_tooling.tooling_core.engine.utils import (
    _is_tide_exec_uuid,
    tide_snapshot_value,
)
from .events import Cause, ExecutorEvents, PipelineEvents
from .event_payloads import Result
from .errors import MissingExecutionEvidenceError
from multiprocessing import get_context
from .utils import _node_worker

_EXECUTION_TEMPLATE_VALUE = __fallback__()


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
        runtime_immediate_effect_count=0,
        post_read_snapshots=None,
        execution_mode=None,
        node_snapshot=None,
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
        self.value_type = type(result)
        self.snapshot = tide_snapshot_value(result, boundary="result")
        self.status = status
        self.error = error
        # Runtime Effects are typed execution evidence owned by RESULT_READY's
        # Event graph.  They are not a runtime value and must not cross a
        # second, generic value-serialisation boundary.
        self.runtime_effects = tuple(runtime_effects or ())
        self.runtime_immediate_effect_count = int(
            runtime_immediate_effect_count
        )
        self.post_read_snapshots = tuple(post_read_snapshots or ())
        self.execution_mode = execution_mode
        self.node_snapshot = node_snapshot
        self.node_id = node_snapshot["node_id"] if node_snapshot is not None else None
        self.handled_raise = handled_raise


def _execute_with_timing(
    assigned_id,
    node_func,
    resolved_args,
    resolved_kwargs,
    runtime,
    node_snapshot=None,
):
    # Retain the live inputs whose mutation this execution can cause.
    observed_inputs = {
        "args": resolved_args,
        "kwargs": resolved_kwargs,
    }

    # Execute once and retain its elapsed time and default outcome.
    started_at = time.time()
    status = "succeeded"
    error = None
    handled_raise = False
    runtime_effects = []
    runtime_immediate_effect_count = 0

    try:
        result, runtime_effects = _node_worker(
            node_func,
            resolved_args,
            resolved_kwargs,
            runtime,
        )

        # A returned RAISE is a handled result with fall-through Effects.
        if isinstance(result, RAISE):
            signal = result
            status = "handled_raise"
            error = signal.exception
            handled_raise = True
            result = signal.exception if isinstance(signal.output, __fallback__) else signal.output
            runtime_immediate_effect_count = len(runtime_effects)
            runtime_effects.extend(signal.effects)

    except RAISE as signal:
        # A raised RAISE is also a handled result and retains worker evidence.
        status = "handled_raise"
        error = signal.exception
        handled_raise = True
        result = signal.exception if isinstance(signal.output, __fallback__) else signal.output
        runtime_immediate_effect_count = len(runtime.effects)
        runtime_effects = [
            *runtime.effects,
            *signal.effects,
        ]

    except Exception as signal:
        # An ordinary failure returns evidence instead of escaping its Future.
        status = "failed"
        error = signal
        result = _EXECUTION_TEMPLATE_VALUE
        runtime_effects = list(runtime.effects)
        runtime_immediate_effect_count = len(runtime_effects)

    # Capture each realised read at its post-execution boundary. RESULT_READY
    # carries these exact snapshots; reconstruction never restores and
    # serialises the runtime value again.
    finished_at = time.time()
    post_read_snapshots = []
    snapshots_by_identity = {}
    for receipt in (node_snapshot or {}).get("reads", ()):
        input_name = receipt["input_name"]
        if input_name not in observed_inputs:
            continue
        try:
            value = observed_inputs[input_name]
            for part in receipt.get("input_path", ()):
                value = value[part]
        except (KeyError, IndexError, TypeError):
            continue

        identity = id(value)
        cached = snapshots_by_identity.get(identity)
        if cached is None:
            captured = tide_snapshot_value(
                value,
                boundary="post_execution",
            )
            snapshots_by_identity[identity] = (value, captured)
        else:
            original, captured = cached
            if original is not value:
                raise RuntimeError("Snapshot identity collision")

        post_read_snapshots.append({
            "input_name": input_name,
            "reference": receipt["reference"],
            "input_path": tuple(receipt.get("input_path", ())),
            "snapshot": captured,
        })

    return ExecMeta(
        assigned_id,
        started_at,
        finished_at,
        (finished_at - started_at) * 1000,
        result,
        status,
        error,
        runtime_effects,
        runtime_immediate_effect_count,
        post_read_snapshots,
        handled_raise=handled_raise,
    )


class Executor:
    _VALID_EXECUTION_MODES = {"auto", "thread", "process", "sequential"}

    def __init__(self, event_interface):
        self.id = 0
        self.event_emitter = event_interface.emit_event_to_ocean
        self.event_queue = Queue()
        event_interface.event_subscriber(ExecutorEvents, self.event_queue.put)
        self.running_tasks = {}
        self.thread_pool_executor = ThreadPoolExecutor(max_workers=4)
        self.process_pool_executor = None
        self._closed = False

    def idle(self):
        return self.event_queue.empty() and not self.running_tasks

    def _submit(self, node_ready_event):
        # Submit exactly the node execution carried by NODE_READY.
        if self._closed:
            raise RuntimeError("Executor is closed")
        work = node_ready_event.payload
        requested_mode = work.mode
        if requested_mode not in self._VALID_EXECUTION_MODES:
            raise ValueError(
                f"execution mode must be one of "
                f"{sorted(self._VALID_EXECUTION_MODES)}, got {requested_mode!r}"
            )
        mode = "thread" if requested_mode == "auto" else requested_mode

        # Process workers cannot carry live Store handlers.
        runtime = work.effect_runtime
        if mode == "process":
            runtime = replace(
                runtime,
                effects=[],
                inside_process_pool=True,
                read_handler=None,
                write_handler=None,
                delete_handler=None,
            )

        payload = (
            node_ready_event.exec_id,
            work.function,
            work.args,
            work.kwargs,
            runtime,
            work.node_snapshot,
        )

        # Route the one execution through its requested mode.
        if mode == "thread":
            future = self.thread_pool_executor.submit(
                _execute_with_timing,
                *payload,
            )
        elif mode == "process":
            if self.process_pool_executor is None:
                self.process_pool_executor = ProcessPoolExecutor(
                    max_workers=4,
                    mp_context=get_context("spawn"),
                )
            future = self.process_pool_executor.submit(
                _execute_with_timing,
                *payload,
            )
        else:
            future = FakeFuture(_execute_with_timing(*payload))

        self.running_tasks[future] = (node_ready_event, mode)
    
    def _collect_results(self):
        # Collect only completed work without blocking Ocean.
        done, _ = wait(
            self.running_tasks,
            timeout=0,
            return_when=FIRST_COMPLETED,
        )

        for future in done:
            node_ready_event, mode = self.running_tasks[future]
            work = node_ready_event.payload

            try:
                meta = future.result()
            except Exception:
                # Explicit process and infrastructure failures remain explicit.
                self.running_tasks.pop(future)
                raise

            # RESULT_READY must retain the immutable NODE_READY evidence.
            if work.node_snapshot is None:
                self.running_tasks.pop(future)
                raise MissingExecutionEvidenceError(
                    f"Execution {meta.id} completed without its immutable "
                    "NODE_READY evidence"
                )
            meta.execution_mode = mode
            meta.node_snapshot = work.node_snapshot
            meta.node_id = work.node_snapshot["node_id"]
            self.running_tasks.pop(future)

            # Return the completed execution to its PipelineRunner.
            self.event_emitter(Event(
                ocean_id=node_ready_event.ocean_id,
                source="executor",
                source_id=self.id,
                type=PipelineEvents.RESULT_READY,
                cause=Cause.from_event(node_ready_event),
                target_runner_id=node_ready_event.source_id,
                payload=Result(meta),
                exec_id=meta.id,
            ))

    def cancel_runner(self, runner_id):
        # Remove pending work belonging to exactly this runner.
        for future, (ready_event, mode) in list(self.running_tasks.items()):
            if ready_event.source_id != runner_id:
                continue
            if mode != "sequential":
                future.cancel()
            self.running_tasks.pop(future)

    def shutdown(self, wait=True, cancel_futures=True):
        # Close each allocated pool once.
        if self._closed:
            return
        self._closed = True
        if cancel_futures:
            for future, (_, mode) in self.running_tasks.items():
                if mode != "sequential":
                    future.cancel()
            self.running_tasks.clear()
        self.thread_pool_executor.shutdown(
            wait=wait,
            cancel_futures=cancel_futures,
        )
        if self.process_pool_executor is not None:
            self.process_pool_executor.shutdown(
                wait=wait,
                cancel_futures=cancel_futures,
            )

    def tick(self):
        # Submit queued NODE_READY Events, then collect completed work.
        while not self.event_queue.empty():
            self._submit(self.event_queue.get_nowait())
        if self.running_tasks:
            self._collect_results()
