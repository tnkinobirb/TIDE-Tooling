#!/usr/bin/env python3
"""
Deep-but-small TIDE runtime diagnostic.

Purpose:
    Diagnose why a single same-reference read/write TIDE.exec can remain inside
    the call for minutes even when the Node is one_shot=True.

This script intentionally does NOT run hundreds of executions.

It runs THREE isolated one-exec scenarios:
    1. no_write
    2. read_write_chain       (src -> dst)
    3. same_ref_one_shot      (rw -> rw, one_shot=True)

Each scenario runs in its own subprocess. While the one TIDE.exec call is live,
a watchdog samples internal Ocean state once per second:

    - process RSS/private memory
    - event counts + event-type histogram
    - last events
    - Ocean runners/running/pipelines/retained traces
    - local Store instances
    - Ocean / Executor / Runner queue sizes
    - Executor running tasks + modes
    - runner completion/purgatory state
    - runtime DAG queue/map sizes
    - each runner node's:
        one_shot
        executing
        evaluated
        result_cached
    - current value of the relevant Store key
    - Python threads

If a scenario does not finish by --timeout, the subprocess exits without
allowing the stuck execution to consume the machine indefinitely.

No third-party dependencies.
"""

from __future__ import annotations

import argparse
import collections
import ctypes
import gc
import inspect
import json
import multiprocessing as mp
import os
import platform
import queue
import subprocess
import sys
import threading
import time
import traceback
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------

def find_repo_root(explicit: str | None = None) -> Path:
    starts: list[Path] = []
    if explicit:
        starts.append(Path(explicit).expanduser().resolve())
    starts.extend([Path.cwd().resolve(), Path(__file__).resolve().parent])

    seen: set[Path] = set()
    for start in starts:
        for candidate in (start, *start.parents):
            if candidate in seen:
                continue
            seen.add(candidate)
            if (candidate / "tide_tooling").is_dir():
                return candidate

    raise RuntimeError(
        "Could not find a repository root containing tide_tooling/. "
        "Run inside the repo or pass --repo-root."
    )


def git_text(repo: Path, *args: str) -> str | None:
    # Per-command safe.directory avoids needing a permanent global Git config.
    try:
        return subprocess.check_output(
            [
                "git",
                "-c",
                f"safe.directory={repo.as_posix()}",
                "-C",
                str(repo),
                *args,
            ],
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
    except Exception:
        return None


def safe_len(value: Any) -> int | None:
    try:
        return len(value)
    except Exception:
        return None


def safe_qsize(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return value.qsize()
    except Exception:
        try:
            return len(value)
        except Exception:
            return None


def enum_name(value: Any) -> str:
    if value is None:
        return "None"
    raw = getattr(value, "value", None)
    if raw is not None:
        return str(raw)
    return str(value)


def process_memory() -> dict[str, int | None]:
    if os.name != "nt":
        return {
            "rss_bytes": None,
            "private_bytes": None,
            "peak_rss_bytes": None,
        }

    class PROCESS_MEMORY_COUNTERS_EX(ctypes.Structure):
        _fields_ = [
            ("cb", ctypes.c_ulong),
            ("PageFaultCount", ctypes.c_ulong),
            ("PeakWorkingSetSize", ctypes.c_size_t),
            ("WorkingSetSize", ctypes.c_size_t),
            ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
            ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
            ("PagefileUsage", ctypes.c_size_t),
            ("PeakPagefileUsage", ctypes.c_size_t),
            ("PrivateUsage", ctypes.c_size_t),
        ]

    counters = PROCESS_MEMORY_COUNTERS_EX()
    counters.cb = ctypes.sizeof(counters)
    handle = ctypes.windll.kernel32.GetCurrentProcess()
    ok = ctypes.windll.psapi.GetProcessMemoryInfo(
        handle,
        ctypes.byref(counters),
        counters.cb,
    )
    if not ok:
        return {
            "rss_bytes": None,
            "private_bytes": None,
            "peak_rss_bytes": None,
        }

    return {
        "rss_bytes": int(counters.WorkingSetSize),
        "private_bytes": int(counters.PrivateUsage),
        "peak_rss_bytes": int(counters.PeakWorkingSetSize),
    }


def mb(value: int | None) -> float | None:
    return None if value is None else value / (1024 * 1024)


def fmt_mb(value: int | None) -> str:
    converted = mb(value)
    return "?" if converted is None else f"{converted:,.1f} MB"


# ---------------------------------------------------------------------------
# Runtime introspection
# ---------------------------------------------------------------------------

def store_value_snapshot(ocean: Any, store_name: str, key: str) -> dict[str, Any]:
    try:
        store = ocean.store_router.get_store(store_name)
        found, value = store.get_if_present(key)
        return {
            "store": store_name,
            "key": key,
            "found": bool(found),
            "value_repr": repr(value)[:300] if found else None,
        }
    except Exception as error:
        return {
            "store": store_name,
            "key": key,
            "error": f"{type(error).__name__}: {error}",
        }


def runner_snapshot(runner: Any) -> dict[str, Any]:
    result: dict[str, Any] = {
        "id": getattr(runner, "id", None),
        "name": getattr(runner, "name", None),
        "complete": getattr(runner, "complete", None),
        "purgatory": getattr(runner, "purgatory", None),
        "stay_alive": getattr(runner, "stay_alive", None),
        "poll": getattr(runner, "poll", None),
        "failure": (
            repr(getattr(runner, "failure", None))
            if getattr(runner, "failure", None) is not None
            else None
        ),
        "running_nodes": safe_len(getattr(runner, "running_nodes", None)),
        "completed_node_ids": safe_len(
            getattr(runner, "completed_node_ids", None)
        ),
        "ready_node_cause_event_ids": safe_len(
            getattr(runner, "ready_node_cause_event_ids", None)
        ),
        "event_queue": safe_qsize(getattr(runner, "event_queue", None)),
        "local_stores": dict(getattr(runner, "local_stores", {}) or {}),
    }

    runtime_dag = getattr(runner, "runtime_dag", None)
    if runtime_dag is not None:
        forward_map = getattr(runtime_dag, "forward_map", {}) or {}
        reverse_map = getattr(runtime_dag, "reverse_map", {}) or {}
        waiting_dynamic = getattr(runtime_dag, "waiting_dynamic_deps", {}) or {}

        try:
            forward_edges = sum(len(v) for v in forward_map.values())
        except Exception:
            forward_edges = None
        try:
            reverse_edges = sum(len(v) for v in reverse_map.values())
        except Exception:
            reverse_edges = None

        result["runtime_dag"] = {
            "ready_nodes_queue": safe_len(
                getattr(runtime_dag, "ready_nodes_queue", None)
            ),
            "forward_map_nodes": safe_len(forward_map),
            "forward_edges": forward_edges,
            "reverse_map_refs": safe_len(reverse_map),
            "reverse_edges": reverse_edges,
            "waiting_dynamic_deps": safe_len(waiting_dynamic),
        }

    index = getattr(runner, "index", None)
    node_index = getattr(index, "node_index", None) if index is not None else None
    node_by_id = (
        getattr(node_index, "node_by_id", {}) or {}
        if node_index is not None
        else {}
    )

    nodes = []
    for node_id, node in list(node_by_id.items())[:20]:
        state = getattr(node, "state", None)
        nodes.append({
            "id": node_id,
            "name": getattr(node, "node_name", None),
            "one_shot": getattr(node, "one_shot", None),
            "recompute": getattr(node, "recompute", None),
            "executing": getattr(state, "executing", None),
            "evaluated": getattr(state, "evaluated", None),
            "resolved": getattr(state, "resolved", None),
            "result_cached": getattr(state, "result_cached", None),
            "deps": [repr(x) for x in list(getattr(node, "deps", ()) or ())[:10]],
            "targets": [
                repr(x) for x in list(getattr(node, "targets", ()) or ())[:10]
            ],
            "pending_unresolve_inputs": safe_len(
                getattr(node, "_pending_unresolve_inputs", None)
            ),
        })
    result["nodes"] = nodes
    return result


def executor_snapshot(executor: Any) -> dict[str, Any]:
    running_tasks = getattr(executor, "running_tasks", {}) or {}
    modes = collections.Counter()
    tasks = []

    for future, value in list(running_tasks.items())[:20]:
        mode = None
        ready_event = None
        if isinstance(value, tuple):
            if len(value) >= 1:
                ready_event = value[0]
            if len(value) >= 2:
                mode = value[1]
        modes[str(mode)] += 1
        tasks.append({
            "mode": mode,
            "future_done": (
                bool(future.done()) if hasattr(future, "done") else None
            ),
            "future_cancelled": (
                bool(future.cancelled())
                if hasattr(future, "cancelled")
                else None
            ),
            "exec_id": getattr(ready_event, "exec_id", None),
            "source_id": getattr(ready_event, "source_id", None),
        })

    return {
        "closed": getattr(executor, "_closed", None),
        "event_queue": safe_qsize(getattr(executor, "event_queue", None)),
        "running_tasks": safe_len(running_tasks),
        "running_task_modes": dict(modes),
        "tasks": tasks,
    }


def event_snapshot(ocean: Any) -> dict[str, Any]:
    log = list(getattr(getattr(ocean, "event_logger", None), "log", ()) or ())
    histogram = collections.Counter(enum_name(getattr(event, "type", None)) for event in log)

    last = []
    for event in log[-12:]:
        cause = getattr(event, "cause", None)
        last.append({
            "type": enum_name(getattr(event, "type", None)),
            "event_id": getattr(event, "event_id", None),
            "exec_id": getattr(event, "exec_id", None),
            "source": getattr(event, "source", None),
            "source_id": getattr(event, "source_id", None),
            "target_runner_id": getattr(event, "target_runner_id", None),
            "cause_event_id": getattr(cause, "event_id", None),
            "cause_description": getattr(cause, "description", None),
        })

    return {
        "count": len(log),
        "histogram": dict(sorted(histogram.items())),
        "last": last,
    }


def ocean_snapshot(
    tide: Any,
    *,
    elapsed_s: float,
    watched_store: str,
    watched_key: str,
) -> dict[str, Any]:
    ocean = tide.ocean
    registry = getattr(ocean, "store_registry", None)
    local_store_instances = getattr(ocean, "local_store_instances", {}) or {}

    try:
        local_instance_count = sum(
            len(mapping) for mapping in local_store_instances.values()
        )
    except Exception:
        local_instance_count = None

    router = getattr(ocean, "event_router", None)
    store_router = getattr(ocean, "store_router", None)

    snapshot = {
        "elapsed_s": elapsed_s,
        "memory": process_memory(),
        "ocean": {
            "done": getattr(ocean, "done", None),
            "closed": getattr(ocean, "closed", None),
            "thread_alive": bool(
                getattr(ocean, "thread", None)
                and ocean.thread.is_alive()
            ),
            "runners": safe_len(getattr(ocean, "runners", None)),
            "running": safe_len(getattr(ocean, "running", None)),
            "pipelines": safe_len(getattr(ocean, "pipelines", None)),
            "retained_trace": int(getattr(ocean, "last_trace", None) is not None),
            "pipeline_to_runners": {
                str(k): len(v)
                for k, v in list(
                    (getattr(ocean, "pipeline_to_runners", {}) or {}).items()
                )[:20]
            },
            "event_queue": safe_qsize(getattr(ocean, "event_queue", None)),
            "store_router_event_queue": safe_qsize(
                getattr(store_router, "event_queue", None)
            ),
            "registered_global_stores": safe_len(
                getattr(registry, "_stores", None)
            ),
            "registered_local_templates": safe_len(
                getattr(registry, "_local_templates", None)
            ),
            "local_store_instances": local_instance_count,
            "subscribed_runners": safe_len(
                getattr(router, "subscribed_runners", None)
            ),
        },
        "executor": executor_snapshot(getattr(ocean, "executor", None)),
        "events": event_snapshot(ocean),
        "runners": [
            runner_snapshot(runner)
            for _, runner in list(
                (getattr(ocean, "runners", {}) or {}).items()
            )[-10:]
        ],
        "watched_value": store_value_snapshot(
            ocean,
            watched_store,
            watched_key,
        ),
        "threads": [
            {
                "name": thread.name,
                "ident": thread.ident,
                "daemon": thread.daemon,
                "alive": thread.is_alive(),
            }
            for thread in threading.enumerate()
        ],
    }
    return snapshot


# ---------------------------------------------------------------------------
# Child scenario worker
# ---------------------------------------------------------------------------

def child_worker(
    repo_root: str,
    scenario: str,
    timeout_s: float,
    interval_s: float,
    out_queue: Any,
) -> None:
    try:
        repo = Path(repo_root).resolve()
        sys.path.insert(0, str(repo))

        import tide_tooling
        from tide_tooling import TIDE, Node

        def add_one(value):
            return value + 1

        tide = TIDE()
        watched_store = "global"
        watched_key = "unused"

        if scenario == "no_write":
            watched_key = "unused"

            def operation():
                return tide.exec(add_one, args=[1])

        elif scenario == "read_write_chain":
            tide.declare("src", 0)
            watched_key = "dst"
            node = Node(
                add_one,
                args=["@src"],
                targets=["global:dst"],
                one_shot=True,
            )

            def operation():
                return tide.exec(node)

        elif scenario == "same_ref_one_shot":
            tide.declare("rw", 0)
            watched_key = "rw"
            node = Node(
                add_one,
                args=["@rw"],
                targets=["global:rw"],
                one_shot=True,
            )

            def operation():
                return tide.exec(node)

        else:
            raise ValueError(f"Unknown scenario: {scenario}")

        state: dict[str, Any] = {
            "done": False,
            "result_repr": None,
            "error": None,
            "traceback": None,
        }

        def execute():
            try:
                result = operation()
                state["result_repr"] = repr(result)[:500]
            except BaseException as error:
                state["error"] = f"{type(error).__name__}: {error}"
                state["traceback"] = traceback.format_exc()
            finally:
                state["done"] = True

        exec_thread = threading.Thread(
            target=execute,
            name=f"diagnostic-exec-{scenario}",
            daemon=True,
        )

        started = time.perf_counter()
        out_queue.put({
            "kind": "scenario_start",
            "scenario": scenario,
            "package": str(Path(tide_tooling.__file__).resolve()),
            "memory": process_memory(),
        })

        exec_thread.start()

        next_sample = 0.0
        while True:
            elapsed = time.perf_counter() - started

            if elapsed >= next_sample:
                snapshot = ocean_snapshot(
                    tide,
                    elapsed_s=elapsed,
                    watched_store=watched_store,
                    watched_key=watched_key,
                )
                out_queue.put({
                    "kind": "snapshot",
                    "scenario": scenario,
                    "snapshot": snapshot,
                })
                next_sample += interval_s

            if state["done"]:
                final_snapshot = ocean_snapshot(
                    tide,
                    elapsed_s=elapsed,
                    watched_store=watched_store,
                    watched_key=watched_key,
                )
                out_queue.put({
                    "kind": "scenario_done",
                    "scenario": scenario,
                    "elapsed_s": elapsed,
                    "state": state,
                    "snapshot": final_snapshot,
                })
                try:
                    tide.ocean.close(wait=False)
                except Exception:
                    pass
                return

            if elapsed >= timeout_s:
                final_snapshot = ocean_snapshot(
                    tide,
                    elapsed_s=elapsed,
                    watched_store=watched_store,
                    watched_key=watched_key,
                )
                out_queue.put({
                    "kind": "scenario_timeout",
                    "scenario": scenario,
                    "elapsed_s": elapsed,
                    "state": state,
                    "snapshot": final_snapshot,
                })
                # Do not wait on the stuck exec. The process has only a daemon
                # execution thread, so returning here allows the child process
                # to exit rather than consuming memory forever.
                try:
                    tide.ocean.close(wait=False)
                except Exception:
                    pass
                return

            time.sleep(min(0.05, interval_s / 10))

    except BaseException as error:
        out_queue.put({
            "kind": "child_failure",
            "scenario": scenario,
            "error": f"{type(error).__name__}: {error}",
            "traceback": traceback.format_exc(),
        })


# ---------------------------------------------------------------------------
# Parent printing / report collection
# ---------------------------------------------------------------------------

def print_snapshot(scenario: str, snap: dict[str, Any], previous: dict[str, Any] | None) -> None:
    elapsed = snap["elapsed_s"]
    memory = snap["memory"]
    ocean = snap["ocean"]
    executor = snap["executor"]
    events = snap["events"]

    event_delta = None
    if previous is not None:
        event_delta = events["count"] - previous["events"]["count"]

    suffix = (
        f" (+{event_delta})"
        if event_delta is not None
        else ""
    )

    print(
        f"[WATCH] {scenario:>20} | {elapsed:>6.1f}s | "
        f"RSS={fmt_mb(memory.get('rss_bytes'))} | "
        f"private={fmt_mb(memory.get('private_bytes'))} | "
        f"events={events['count']}{suffix} | "
        f"runners={ocean['runners']} running={ocean['running']} | "
        f"pipes={ocean['pipelines']} traces={ocean['retained_trace']} | "
        f"OceanQ={ocean['event_queue']} "
        f"StoreQ={ocean['store_router_event_queue']} "
        f"ExecQ={executor['event_queue']} "
        f"tasks={executor['running_tasks']}",
        flush=True,
    )

    value = snap.get("watched_value", {})
    if value.get("found"):
        print(
            f"        store {value['store']}:{value['key']} = "
            f"{value.get('value_repr')}",
            flush=True,
        )

    if snap["runners"]:
        runner = snap["runners"][-1]
        dag = runner.get("runtime_dag", {})
        print(
            f"        runner#{runner['id']} "
            f"complete={runner['complete']} "
            f"purgatory={runner['purgatory']} "
            f"running_nodes={runner['running_nodes']} "
            f"completed={runner['completed_node_ids']} "
            f"RunnerQ={runner['event_queue']} "
            f"ready={dag.get('ready_nodes_queue')} "
            f"fwd_edges={dag.get('forward_edges')} "
            f"rev_edges={dag.get('reverse_edges')}",
            flush=True,
        )

        for node in runner.get("nodes", []):
            print(
                f"        node#{node['id']} "
                f"one_shot={node['one_shot']} "
                f"executing={node['executing']} "
                f"evaluated={node['evaluated']} "
                f"cached={node['result_cached']} "
                f"pending_unresolve={node['pending_unresolve_inputs']}",
                flush=True,
            )

    histogram = events.get("histogram", {})
    interesting = [
        "node_ready",
        "result_ready",
        "dep_available",
        "dep_updated",
        "store_write",
        "force_write",
        "node_blocked",
        "kill_runner",
    ]
    chosen = {
        key: histogram[key]
        for key in interesting
        if key in histogram
    }
    if chosen:
        print(f"        events {chosen}", flush=True)


def run_scenario(
    ctx: Any,
    repo: Path,
    scenario: str,
    timeout_s: float,
    interval_s: float,
) -> dict[str, Any]:
    out_queue = ctx.Queue()
    process = ctx.Process(
        target=child_worker,
        args=(
            str(repo),
            scenario,
            timeout_s,
            interval_s,
            out_queue,
        ),
        name=f"tide-diag-{scenario}",
    )

    print(
        f"\n[SCENARIO] {scenario} | one exec only | "
        f"watchdog timeout={timeout_s:.0f}s",
        flush=True,
    )
    process.start()

    messages: list[dict[str, Any]] = []
    previous_snapshot = None
    terminal = None
    parent_deadline = time.monotonic() + timeout_s + 10.0

    while time.monotonic() < parent_deadline:
        try:
            message = out_queue.get(timeout=0.25)
        except queue.Empty:
            if not process.is_alive():
                break
            continue

        messages.append(message)
        kind = message.get("kind")

        if kind == "snapshot":
            snap = message["snapshot"]
            print_snapshot(scenario, snap, previous_snapshot)
            previous_snapshot = snap

        elif kind == "scenario_start":
            mem = message.get("memory", {})
            print(
                f"[SCENARIO] child started | "
                f"RSS={fmt_mb(mem.get('rss_bytes'))} | "
                f"import={message.get('package')}",
                flush=True,
            )

        elif kind == "scenario_done":
            terminal = message
            print(
                f"[SCENARIO] DONE {scenario} | "
                f"{message['elapsed_s']:.3f}s | "
                f"result={message['state'].get('result_repr')} | "
                f"error={message['state'].get('error')}",
                flush=True,
            )
            break

        elif kind == "scenario_timeout":
            terminal = message
            print(
                f"[SCENARIO] TIMEOUT {scenario} after "
                f"{message['elapsed_s']:.1f}s — one TIDE.exec never returned.",
                flush=True,
            )
            print_snapshot(
                scenario,
                message["snapshot"],
                previous_snapshot,
            )
            break

        elif kind == "child_failure":
            terminal = message
            print(
                f"[SCENARIO] CHILD FAILURE {scenario}: "
                f"{message.get('error')}",
                flush=True,
            )
            break

    process.join(timeout=2)
    if process.is_alive():
        print(
            f"[SCENARIO] terminating child {scenario} so it cannot "
            "keep consuming RAM.",
            flush=True,
        )
        process.terminate()
        process.join(timeout=3)

    return {
        "scenario": scenario,
        "terminal": terminal,
        "messages": messages,
        "child_exitcode": process.exitcode,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Deep, tiny TIDE stuck-exec diagnostic."
    )
    parser.add_argument("--repo-root", default=None)
    parser.add_argument(
        "--timeout",
        type=float,
        default=20.0,
        help="Maximum seconds allowed for each single TIDE.exec.",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Watchdog snapshot interval in seconds.",
    )
    parser.add_argument(
        "--output",
        default="tide_stuck_exec_diagnostic.json",
    )
    parser.add_argument(
        "--scenario",
        choices=[
            "all",
            "no_write",
            "read_write_chain",
            "same_ref_one_shot",
        ],
        default="all",
    )
    args = parser.parse_args()

    repo = find_repo_root(args.repo_root)

    branch = git_text(repo, "branch", "--show-current")
    commit = git_text(repo, "rev-parse", "HEAD")
    status = git_text(repo, "status", "--porcelain")

    print("[TIDE DEEP DIAG] Starting", flush=True)
    print(f"[TIDE DEEP DIAG] repo   : {repo}", flush=True)
    print(f"[TIDE DEEP DIAG] branch : {branch}", flush=True)
    print(f"[TIDE DEEP DIAG] commit : {commit}", flush=True)
    print(
        f"[TIDE DEEP DIAG] dirty  : {bool(status)}",
        flush=True,
    )

    scenarios = (
        ["no_write", "read_write_chain", "same_ref_one_shot"]
        if args.scenario == "all"
        else [args.scenario]
    )

    # Spawn is Windows' normal process mode and keeps each scenario genuinely
    # isolated from retained state in the previous scenario.
    ctx = mp.get_context("spawn")

    report = {
        "metadata": {
            "repo": str(repo),
            "branch": branch,
            "commit": commit,
            "dirty": bool(status),
            "python": sys.version,
            "platform": platform.platform(),
        },
        "configuration": vars(args),
        "scenarios": [],
    }

    for scenario in scenarios:
        report["scenarios"].append(
            run_scenario(
                ctx,
                repo,
                scenario,
                args.timeout,
                args.interval,
            )
        )
        gc.collect()

    output = Path(args.output).expanduser()
    if not output.is_absolute():
        output = Path.cwd() / output
    output.write_text(
        json.dumps(report, indent=2, default=str),
        encoding="utf-8",
    )

    print(f"\n[TIDE DEEP DIAG] report: {output}", flush=True)
    print("[TIDE DEEP DIAG] finished.", flush=True)


if __name__ == "__main__":
    mp.freeze_support()
    main()
