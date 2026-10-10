#!/usr/bin/env python3
"""Lightweight TIDE retention + latency diagnostic.

Small enough for a 16 GB machine. It prints every execution and records:
- raw Python, TIDE init/close, event, declare, fresh one-shot exec
- long-lived exec growth with and without a read/write version chain
- process RSS/private memory (Windows stdlib, no psutil required)
- tracemalloc current/peak Python allocations
- Ocean retained-state counts (runners, pipelines, traces, events, stores, etc.)
- tiny trace/value/model timings

The report is written as JSON for later comparison with the refactor.
"""
from __future__ import annotations

import argparse
import ctypes
import gc
import inspect
import json
import os
import platform
import statistics
import subprocess
import sys
import threading
import time
import tracemalloc
from pathlib import Path
from typing import Any, Callable

REFERENCE_COMMIT = "2c25992bcfc4763b29e341ba195ac5f52189f187"


def find_repo_root(explicit: str | None = None) -> Path:
    starts = []
    if explicit:
        starts.append(Path(explicit).expanduser().resolve())
    starts.extend([Path.cwd().resolve(), Path(__file__).resolve().parent])
    seen = set()
    for start in starts:
        for candidate in (start, *start.parents):
            if candidate in seen:
                continue
            seen.add(candidate)
            if (candidate / "tide_tooling").is_dir():
                return candidate
    raise RuntimeError("Could not find repo root containing tide_tooling/. Use --repo-root.")


def git_text(repo: Path, *args: str) -> str | None:
    try:
        return subprocess.check_output(
            ["git", "-C", str(repo), *args],
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
    except Exception:
        return None


def windows_process_memory() -> dict[str, int] | None:
    if os.name != "nt":
        return None

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

    c = PROCESS_MEMORY_COUNTERS_EX()
    c.cb = ctypes.sizeof(c)
    handle = ctypes.windll.kernel32.GetCurrentProcess()
    ok = ctypes.windll.psapi.GetProcessMemoryInfo(handle, ctypes.byref(c), c.cb)
    if not ok:
        return None
    return {
        "rss_bytes": int(c.WorkingSetSize),
        "private_bytes": int(c.PrivateUsage),
        "peak_rss_bytes": int(c.PeakWorkingSetSize),
        "peak_private_bytes": int(c.PeakPagefileUsage),
    }


def memory_snapshot() -> dict[str, Any]:
    out = windows_process_memory() or {}
    current, peak = tracemalloc.get_traced_memory()
    out.update({
        "tracemalloc_current_bytes": current,
        "tracemalloc_peak_bytes": peak,
    })
    return out


def mb(value: int | None) -> float | None:
    return None if value is None else value / (1024 * 1024)


def format_mem(m: dict[str, Any]) -> str:
    parts = []
    if m.get("rss_bytes") is not None:
        parts.append(f"RSS={mb(m['rss_bytes']):,.1f} MB")
    if m.get("private_bytes") is not None:
        parts.append(f"private={mb(m['private_bytes']):,.1f} MB")
    parts.append(f"py={mb(m.get('tracemalloc_current_bytes', 0)):,.1f} MB")
    return " | ".join(parts)


def close_tide(tide: Any) -> None:
    ocean = getattr(tide, "ocean", None)
    close = getattr(ocean, "close", None)
    if callable(close):
        close()


def safe_len(value: Any) -> int | None:
    try:
        return len(value)
    except Exception:
        return None


def ocean_counts(tide: Any) -> dict[str, Any]:
    ocean = tide.ocean
    registry = getattr(ocean, "store_registry", None)
    router = getattr(ocean, "event_router", None)
    executor = getattr(ocean, "executor", None)
    local_groups = getattr(ocean, "local_store_instances", None)
    try:
        local_instances = sum(len(v) for v in local_groups.values())
    except Exception:
        local_instances = None
    return {
        "runners": safe_len(getattr(ocean, "runners", None)),
        "running": safe_len(getattr(ocean, "running", None)),
        "pipelines": safe_len(getattr(ocean, "pipelines", None)),
        "retained_trace": int(getattr(ocean, "last_trace", None) is not None),
        "event_log": safe_len(getattr(getattr(ocean, "event_logger", None), "log", None)),
        "global_stores": safe_len(getattr(registry, "_stores", None)),
        "local_store_templates": safe_len(getattr(registry, "_local_templates", None)),
        "local_store_instances": local_instances,
        "subscribed_runners": safe_len(getattr(router, "subscribed_runners", None)),
        "executor_running_tasks": safe_len(getattr(executor, "running_tasks", None)),
        "python_threads": threading.active_count(),
    }


def timed_once(fn: Callable[[], Any]) -> tuple[Any, float]:
    start = time.perf_counter_ns()
    result = fn()
    return result, (time.perf_counter_ns() - start) / 1_000_000


def stats(values: list[float]) -> dict[str, float]:
    if not values:
        return {}
    ordered = sorted(values)
    p95 = ordered[min(len(ordered) - 1, round((len(ordered) - 1) * 0.95))]
    return {
        "min": min(values),
        "median": statistics.median(values),
        "mean": statistics.fmean(values),
        "p95": p95,
        "max": max(values),
    }


def repeated_fresh(name: str, repeats: int, operation: Callable[[], Any]) -> dict[str, Any]:
    print(f"\n[SMALL] {name} | repeats={repeats}", flush=True)
    samples = []
    for i in range(repeats):
        _, elapsed = timed_once(operation)
        samples.append(elapsed)
        print(f"[SMALL] {name} {i+1}/{repeats} | {elapsed:,.3f} ms", flush=True)
    return {"name": name, "samples_ms": samples, "stats_ms": stats(samples)}


def run_growth_scenario(
    *, name: str, TIDE: Any, count: int,
    setup: Callable[[Any], None], execute: Callable[[Any, int], Any],
) -> dict[str, Any]:
    print(f"\n[GROWTH] START {name} | executions={count}", flush=True)
    gc.collect()
    before_create = memory_snapshot()
    tide = TIDE()
    setup(tide)
    after_setup = memory_snapshot()
    rows = []

    try:
        for i in range(1, count + 1):
            before = memory_snapshot()
            _, elapsed = timed_once(lambda: execute(tide, i))
            after = memory_snapshot()
            counts = ocean_counts(tide)
            row = {
                "execution": i,
                "latency_ms": elapsed,
                "memory_before": before,
                "memory_after": after,
                "ocean": counts,
            }
            rows.append(row)
            print(
                f"[GROWTH] {name} #{i:02d} | {elapsed:>10,.3f} ms | "
                f"{format_mem(after)} | runners={counts['runners']} | "
                f"pipes={counts['pipelines']} | traces={counts['retained_trace']} | "
                f"events={counts['event_log']} | localStores={counts['local_store_instances']}",
                flush=True,
            )
        before_close = memory_snapshot()
        counts_before_close = ocean_counts(tide)
    finally:
        close_tide(tide)

    del tide
    gc.collect()
    after_close_gc = memory_snapshot()
    latencies = [r["latency_ms"] for r in rows]
    rss = [r["memory_after"].get("rss_bytes") for r in rows if r["memory_after"].get("rss_bytes") is not None]
    private = [r["memory_after"].get("private_bytes") for r in rows if r["memory_after"].get("private_bytes") is not None]

    print(
        f"[GROWTH] DONE {name} | setup={format_mem(after_setup)} | "
        f"pre-close={format_mem(before_close)} | after-close+gc={format_mem(after_close_gc)}",
        flush=True,
    )
    return {
        "name": name,
        "executions": count,
        "memory_before_create": before_create,
        "memory_after_setup": after_setup,
        "rows": rows,
        "latency_stats_ms": stats(latencies),
        "rss_growth_bytes": rss[-1] - rss[0] if len(rss) >= 2 else None,
        "private_growth_bytes": private[-1] - private[0] if len(private) >= 2 else None,
        "ocean_before_close": counts_before_close,
        "memory_before_close": before_close,
        "memory_after_close_gc": after_close_gc,
    }


def query_fixture(TIDE: Any, add_one: Callable[[int], int], length: int) -> dict[str, Any]:
    print(f"\n[QUERY] tiny chain length={length}", flush=True)
    tide = TIDE()
    try:
        tide.declare("q0", 0)
        for i in range(length):
            print(f"[QUERY] build {i+1}/{length}", flush=True)
            tide.exec(add_one, reads=[f"q{i}"], writes=f"q{i+1}")
        trace, trace_ms = timed_once(tide.trace)
        _, value_ms = timed_once(lambda: trace.value_of(f"q{length}"))
        model, model_ms = timed_once(lambda: trace.to_model(enrich=False))
        validity_ms = None
        if hasattr(TIDE, "report_model_validity"):
            _, validity_ms = timed_once(lambda: TIDE.report_model_validity(model))
        result = {
            "chain_length": length,
            "trace_ms": trace_ms,
            "value_of_final_ref_ms": value_ms,
            "to_model_ms": model_ms,
            "model_validity_ms": validity_ms,
            "trace_exec_records": len(trace.log),
            "trace_events": len(trace.events),
            "ocean": ocean_counts(tide),
            "memory": memory_snapshot(),
        }
        print(
            f"[QUERY] trace={trace_ms:,.3f} ms | value_of={value_ms:,.3f} ms | "
            f"model={model_ms:,.3f} ms | log={len(trace.log)} | events={len(trace.events)} | "
            f"{format_mem(result['memory'])}",
            flush=True,
        )
        return result
    finally:
        close_tide(tide)


def run(args: argparse.Namespace) -> dict[str, Any]:
    repo = find_repo_root(args.repo_root)
    sys.path.insert(0, str(repo))
    import tide_tooling
    from tide_tooling import TIDE

    imported = Path(tide_tooling.__file__).resolve()
    try:
        imported.relative_to(repo)
    except ValueError:
        raise RuntimeError(f"Imported TIDE outside repo. repo={repo} imported={imported}")

    commit = git_text(repo, "rev-parse", "HEAD")
    branch = git_text(repo, "branch", "--show-current")
    dirty = bool(git_text(repo, "status", "--porcelain"))

    print("[TIDE DIAG] Lightweight retention benchmark", flush=True)
    print(f"[TIDE DIAG] repo   : {repo}", flush=True)
    print(f"[TIDE DIAG] branch : {branch}", flush=True)
    print(f"[TIDE DIAG] commit : {commit}", flush=True)
    print(f"[TIDE DIAG] import : {imported}", flush=True)
    print(f"[TIDE DIAG] start  : {format_mem(memory_snapshot())}", flush=True)

    def add_one(value):
        return value + 1

    report = {
        "metadata": {
            "repo": str(repo), "branch": branch, "commit": commit,
            "reference_commit": REFERENCE_COMMIT, "dirty": dirty,
            "python": sys.version, "platform": platform.platform(),
            "imported_package": str(imported), "TIDE_file": inspect.getsourcefile(TIDE),
        },
        "configuration": vars(args),
        "starting_memory": memory_snapshot(),
    }

    start = time.perf_counter_ns()
    for _ in range(args.raw_iterations):
        add_one(1)
    report["raw_python"] = {
        "iterations": args.raw_iterations,
        "us_per_call": (time.perf_counter_ns() - start) / args.raw_iterations / 1_000,
    }
    print(f"\n[BASELINE] raw Python = {report['raw_python']['us_per_call']:.4f} us/call", flush=True)

    def init_close():
        t = TIDE(); close_tide(t)
    report["init_close"] = repeated_fresh("TIDE init+close", args.simple_repeats, init_close)

    def event_once():
        t = TIDE()
        try: t.event("bench_event", writes={"x": 1})
        finally: close_tide(t)
    report["event_write"] = repeated_fresh("fresh TIDE.event(write)", args.simple_repeats, event_once)

    def declare_once():
        t = TIDE()
        try: t.declare("x", 1)
        finally: close_tide(t)
    report["declare"] = repeated_fresh("fresh TIDE.declare", args.simple_repeats, declare_once)

    def fresh_exec_once():
        t = TIDE()
        try: t.exec(add_one, args=[1])
        finally: close_tide(t)
    report["fresh_exec"] = repeated_fresh("fresh one-shot TIDE.exec", args.simple_repeats, fresh_exec_once)

    report["growth_no_write"] = run_growth_scenario(
        name="long_lived_exec_no_write", TIDE=TIDE, count=args.no_write_execs,
        setup=lambda t: None,
        execute=lambda t, _: t.exec(add_one, args=[1]),
    )

    def setup_rw_chain(tide):
        tide.declare("rw_0", 0)

    def execute_rw_chain(tide, index):
        # Acyclic version-chain style benchmark:
        # rw_0 -> rw_1 -> rw_2 -> ...
        # Each execution reads the previous reference and writes a NEW one,
        # so no node can trigger itself by updating its own dependency.
        return tide.exec(
            add_one,
            reads=[f"rw_{index - 1}"],
            writes=f"rw_{index}",
        )

    report["growth_read_write"] = run_growth_scenario(
        name="long_lived_exec_read_write_chain",
        TIDE=TIDE,
        count=args.read_write_execs,
        setup=setup_rw_chain,
        execute=execute_rw_chain,
    )

    report["small_queries"] = query_fixture(TIDE, add_one, args.query_chain)
    gc.collect()
    report["ending_memory"] = memory_snapshot()
    print(f"\n[TIDE DIAG] end    : {format_mem(report['ending_memory'])}", flush=True)
    return report


def main() -> None:
    p = argparse.ArgumentParser(description="Small TIDE latency/retention diagnostic")
    p.add_argument("--repo-root", default=None)
    p.add_argument("--output", default="tide_lightweight_safe_chain_benchmark.json")
    p.add_argument("--simple-repeats", type=int, default=3)
    p.add_argument("--no-write-execs", type=int, default=8)
    p.add_argument("--read-write-execs", type=int, default=10)
    p.add_argument("--query-chain", type=int, default=5)
    p.add_argument("--raw-iterations", type=int, default=20_000)
    args = p.parse_args()

    tracemalloc.start()
    started = time.perf_counter()
    report = run(args)
    report["total_elapsed_s"] = time.perf_counter() - started

    output = Path(args.output).expanduser()
    if not output.is_absolute():
        output = Path.cwd() / output
    output.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    print(f"[TIDE DIAG] finished in {report['total_elapsed_s']:.2f}s", flush=True)
    print(f"[TIDE DIAG] JSON report: {output}", flush=True)


if __name__ == "__main__":
    main()
