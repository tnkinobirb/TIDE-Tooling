#!/usr/bin/env python3
"""
Heavier but safe TIDE benchmark.

Key rule:
    No self-read/write node.
    The read/write workload is always acyclic:
        rw_0 -> rw_1 -> rw_2 -> ...

Presets:
    --preset medium   default, useful pre-refactor baseline
    --preset heavy    stronger stress pass
    --preset full     long run, still avoids the known rw -> rw feedback case

The benchmark reports:
- raw Python baseline
- TIDE init/close
- fresh event/declare/exec costs
- long-lived no-write exec growth
- long-lived acyclic read/write chain growth
- tiny/full trace rebuild
- value_of
- produced_by
- downstream/path where available
- to_model
- report_model_validity where available
- compare(self,self) where available
- bundle round-trip where available
- fresh lifecycle timing
- tracemalloc growth and Ocean retained structure sizes

It prints progress throughout and writes JSON.
"""

from __future__ import annotations

import argparse
import gc
import inspect
import json
import os
import platform
import statistics
import subprocess
import sys
import time
import tracemalloc
from pathlib import Path
from typing import Any, Callable


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
    raise RuntimeError("Could not find repo root containing tide_tooling/")


def git_text(repo: Path, *args: str) -> str | None:
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


def ms(fn: Callable[[], Any]) -> tuple[Any, float]:
    t0 = time.perf_counter_ns()
    result = fn()
    return result, (time.perf_counter_ns() - t0) / 1_000_000


def safe_len(value: Any) -> int | None:
    try:
        return len(value)
    except Exception:
        return None


def traced_memory() -> dict[str, int]:
    current, peak = tracemalloc.get_traced_memory()
    return {
        "current_bytes": current,
        "peak_bytes": peak,
    }


def mib(value: int | None) -> float | None:
    return None if value is None else value / (1024 * 1024)


def fmt_mem(snapshot: dict[str, int]) -> str:
    return (
        f"py={mib(snapshot['current_bytes']):,.1f} MB "
        f"peak={mib(snapshot['peak_bytes']):,.1f} MB"
    )


def stats(values: list[float]) -> dict[str, float]:
    if not values:
        return {}
    ordered = sorted(values)
    p95_idx = min(len(ordered) - 1, round((len(ordered) - 1) * 0.95))
    return {
        "min": min(values),
        "median": statistics.median(values),
        "mean": statistics.fmean(values),
        "p95": ordered[p95_idx],
        "max": max(values),
    }


def close_tide(tide: Any) -> None:
    try:
        tide.ocean.close()
    except Exception:
        pass


def ocean_counts(tide: Any) -> dict[str, Any]:
    ocean = tide.ocean
    registry = getattr(ocean, "store_registry", None)
    router = getattr(ocean, "event_router", None)
    executor = getattr(ocean, "executor", None)
    local_instances = getattr(ocean, "local_store_instances", {}) or {}

    try:
        local_store_instances = sum(len(v) for v in local_instances.values())
    except Exception:
        local_store_instances = None

    return {
        "runners": safe_len(getattr(ocean, "runners", None)),
        "running": safe_len(getattr(ocean, "running", None)),
        "pipelines": safe_len(getattr(ocean, "pipelines", None)),
        "retained_trace": int(getattr(ocean, "last_trace", None) is not None),
        "event_log": safe_len(
            getattr(getattr(ocean, "event_logger", None), "log", None)
        ),
        "local_store_instances": local_store_instances,
        "registered_global_stores": safe_len(
            getattr(registry, "_stores", None)
        ),
        "registered_local_templates": safe_len(
            getattr(registry, "_local_templates", None)
        ),
        "subscribed_runners": safe_len(
            getattr(router, "subscribed_runners", None)
        ),
        "executor_running_tasks": safe_len(
            getattr(executor, "running_tasks", None)
        ),
    }


PRESETS = {
    "medium": {
        "raw_iterations": 100_000,
        "fresh_repeats": 7,
        "lifecycle_repeats": 7,
        "no_write_execs": 40,
        "read_write_execs": 50,
        "query_chain": 25,
        "query_repeats": 25,
        "trace_repeats": 15,
        "model_repeats": 15,
        "compare_repeats": 5,
        "bundle_repeats": 3,
    },
    "heavy": {
        "raw_iterations": 500_000,
        "fresh_repeats": 12,
        "lifecycle_repeats": 12,
        "no_write_execs": 80,
        "read_write_execs": 100,
        "query_chain": 50,
        "query_repeats": 75,
        "trace_repeats": 30,
        "model_repeats": 30,
        "compare_repeats": 10,
        "bundle_repeats": 5,
    },
    "full": {
        "raw_iterations": 1_000_000,
        "fresh_repeats": 20,
        "lifecycle_repeats": 20,
        "no_write_execs": 150,
        "read_write_execs": 200,
        "query_chain": 100,
        "query_repeats": 150,
        "trace_repeats": 50,
        "model_repeats": 50,
        "compare_repeats": 20,
        "bundle_repeats": 10,
    },
}


def repeated_fresh(name: str, repeats: int, factory: Callable[[], None]) -> dict[str, Any]:
    vals = []
    print(f"\n[FRESH] {name} | repeats={repeats}", flush=True)
    for i in range(repeats):
        _, elapsed = ms(factory)
        vals.append(elapsed)
        print(
            f"[FRESH] {name} {i+1:02d}/{repeats:02d} | {elapsed:,.3f} ms",
            flush=True,
        )
    return {
        "samples_ms": vals,
        "stats_ms": stats(vals),
    }


def run_growth(
    *,
    name: str,
    TIDE: Any,
    count: int,
    setup: Callable[[Any], None],
    execute: Callable[[Any, int], Any],
    print_every: int,
) -> dict[str, Any]:
    gc.collect()
    tide = TIDE()
    setup(tide)

    start_mem = traced_memory()
    rows = []
    print(
        f"\n[GROWTH] START {name} | executions={count} | "
        f"{fmt_mem(start_mem)}",
        flush=True,
    )

    try:
        for i in range(1, count + 1):
            before = traced_memory()
            _, elapsed = ms(lambda: execute(tide, i))
            after = traced_memory()
            counts = ocean_counts(tide)

            row = {
                "execution": i,
                "latency_ms": elapsed,
                "memory_before": before,
                "memory_after": after,
                "ocean": counts,
            }
            rows.append(row)

            if i == 1 or i == count or i % print_every == 0:
                print(
                    f"[GROWTH] {name} #{i:03d} | "
                    f"{elapsed:>9,.3f} ms | "
                    f"{fmt_mem(after)} | "
                    f"runners={counts['runners']} "
                    f"pipes={counts['pipelines']} "
                    f"traces={counts['retained_trace']} "
                    f"events={counts['event_log']} "
                    f"localStores={counts['local_store_instances']}",
                    flush=True,
                )

        pre_close = traced_memory()
        counts = ocean_counts(tide)

    finally:
        close_tide(tide)

    del tide
    gc.collect()
    post_close = traced_memory()

    values = [r["latency_ms"] for r in rows]
    first_quarter = values[: max(1, len(values) // 4)]
    last_quarter = values[-max(1, len(values) // 4):]

    print(
        f"[GROWTH] DONE {name} | "
        f"median={statistics.median(values):,.3f} ms | "
        f"firstQ={statistics.median(first_quarter):,.3f} ms | "
        f"lastQ={statistics.median(last_quarter):,.3f} ms | "
        f"pre-close={fmt_mem(pre_close)} | "
        f"post-close+gc={fmt_mem(post_close)}",
        flush=True,
    )

    return {
        "name": name,
        "rows": rows,
        "stats_ms": stats(values),
        "first_quarter_median_ms": statistics.median(first_quarter),
        "last_quarter_median_ms": statistics.median(last_quarter),
        "slowdown_ratio_last_vs_first": (
            statistics.median(last_quarter) / statistics.median(first_quarter)
            if statistics.median(first_quarter) else None
        ),
        "memory_start": start_mem,
        "memory_pre_close": pre_close,
        "memory_post_close_gc": post_close,
        "ocean_pre_close": counts,
    }


def repeated_live(name: str, repeats: int, fn: Callable[[], Any]) -> dict[str, Any]:
    vals = []
    print(f"\n[QUERY] {name} | repeats={repeats}", flush=True)
    for i in range(repeats):
        _, elapsed = ms(fn)
        vals.append(elapsed)
        if i == 0 or i + 1 == repeats or (i + 1) % max(1, repeats // 5) == 0:
            print(
                f"[QUERY] {name} {i+1:03d}/{repeats:03d} | "
                f"{elapsed:,.3f} ms",
                flush=True,
            )
    return {
        "samples_ms": vals,
        "stats_ms": stats(vals),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--preset",
        choices=tuple(PRESETS),
        default="medium",
    )
    parser.add_argument("--repo-root", default=None)
    parser.add_argument("--output", default=None)
    parser.add_argument(
        "--print-every",
        type=int,
        default=10,
        help="Progress interval for long growth loops.",
    )
    args = parser.parse_args()

    cfg = dict(PRESETS[args.preset])
    repo = find_repo_root(args.repo_root)
    sys.path.insert(0, str(repo))

    import tide_tooling
    from tide_tooling import TIDE

    imported = Path(tide_tooling.__file__).resolve()
    try:
        imported.relative_to(repo)
    except ValueError:
        raise RuntimeError(
            f"Imported tide_tooling outside repo.\nrepo={repo}\nimport={imported}"
        )

    tracemalloc.start()
    total_start = time.perf_counter()

    branch = git_text(repo, "branch", "--show-current")
    commit = git_text(repo, "rev-parse", "HEAD")
    dirty = bool(git_text(repo, "status", "--porcelain"))

    print("[TIDE BENCH] Safe heavier benchmark", flush=True)
    print(f"[TIDE BENCH] preset : {args.preset}", flush=True)
    print(f"[TIDE BENCH] repo   : {repo}", flush=True)
    print(f"[TIDE BENCH] branch : {branch}", flush=True)
    print(f"[TIDE BENCH] commit : {commit}", flush=True)
    print(f"[TIDE BENCH] dirty  : {dirty}", flush=True)
    print(f"[TIDE BENCH] import : {imported}", flush=True)
    print(f"[TIDE BENCH] config : {cfg}", flush=True)

    def add_one(value):
        return value + 1

    report: dict[str, Any] = {
        "metadata": {
            "preset": args.preset,
            "repo": str(repo),
            "branch": branch,
            "commit": commit,
            "dirty": dirty,
            "python": sys.version,
            "platform": platform.platform(),
            "imported_package": str(imported),
            "TIDE_file": inspect.getsourcefile(TIDE),
        },
        "config": cfg,
        "start_memory": traced_memory(),
    }

    # Raw Python
    t0 = time.perf_counter_ns()
    for _ in range(cfg["raw_iterations"]):
        add_one(1)
    raw_ns = time.perf_counter_ns() - t0
    report["raw_python_us_per_call"] = (
        raw_ns / cfg["raw_iterations"] / 1_000
    )
    print(
        f"\n[BASELINE] raw Python = "
        f"{report['raw_python_us_per_call']:.4f} us/call",
        flush=True,
    )

    # Fresh tiny operations.
    def init_close():
        t = TIDE()
        close_tide(t)

    report["fresh_init_close"] = repeated_fresh(
        "TIDE init+close",
        cfg["fresh_repeats"],
        init_close,
    )

    def event_once():
        t = TIDE()
        try:
            t.event("bench", writes={"x": 1})
        finally:
            close_tide(t)

    report["fresh_event_write"] = repeated_fresh(
        "TIDE.event(write)",
        cfg["fresh_repeats"],
        event_once,
    )

    def declare_once():
        t = TIDE()
        try:
            t.declare("x", 1)
        finally:
            close_tide(t)

    report["fresh_declare"] = repeated_fresh(
        "TIDE.declare",
        cfg["fresh_repeats"],
        declare_once,
    )

    def exec_once():
        t = TIDE()
        try:
            t.exec(add_one, args=[1])
        finally:
            close_tide(t)

    report["fresh_exec"] = repeated_fresh(
        "TIDE.exec(no-write)",
        cfg["fresh_repeats"],
        exec_once,
    )

    # Fresh lifecycle includes declare -> two acyclic execs -> trace -> model.
    def lifecycle():
        t = TIDE()
        try:
            t.declare("life_0", 0)
            t.exec(add_one, reads=["life_0"], writes="life_1")
            t.exec(add_one, reads=["life_1"], writes="life_2")
            trace = t.trace()
            trace.to_model(enrich=False)
        finally:
            close_tide(t)

    report["fresh_lifecycle"] = repeated_fresh(
        "fresh lifecycle declare+2exec+trace+model",
        cfg["lifecycle_repeats"],
        lifecycle,
    )

    # Long-lived facade growth.
    report["growth_no_write"] = run_growth(
        name="long_lived_exec_no_write",
        TIDE=TIDE,
        count=cfg["no_write_execs"],
        setup=lambda t: None,
        execute=lambda t, _: t.exec(add_one, args=[1]),
        print_every=args.print_every,
    )

    def setup_chain(t):
        t.declare("rw_0", 0)

    def chain_exec(t, i):
        return t.exec(
            add_one,
            reads=[f"rw_{i - 1}"],
            writes=f"rw_{i}",
        )

    report["growth_read_write_chain"] = run_growth(
        name="long_lived_exec_read_write_chain",
        TIDE=TIDE,
        count=cfg["read_write_execs"],
        setup=setup_chain,
        execute=chain_exec,
        print_every=args.print_every,
    )

    # Build a larger query fixture.
    print(
        f"\n[FIXTURE] building acyclic query chain length={cfg['query_chain']}",
        flush=True,
    )
    fixture = TIDE()
    fixture.declare("q_0", 0)
    for i in range(1, cfg["query_chain"] + 1):
        fixture.exec(
            add_one,
            reads=[f"q_{i - 1}"],
            writes=f"q_{i}",
        )
        if i == 1 or i == cfg["query_chain"] or i % args.print_every == 0:
            print(
                f"[FIXTURE] build {i:03d}/{cfg['query_chain']:03d}",
                flush=True,
            )

    trace = fixture.trace()
    final_ref = f"q_{cfg['query_chain']}"
    chain_exec_records = [
        r for r in trace.log if getattr(r, "func_name", None) == "add_one"
    ]
    first_exec_id = (
        chain_exec_records[0].exec_id if chain_exec_records else None
    )
    last_exec_id = (
        chain_exec_records[-1].exec_id if chain_exec_records else None
    )

    report["fixture"] = {
        "trace_log_len": len(trace.log),
        "trace_events_len": len(trace.events),
        "ocean": ocean_counts(fixture),
        "memory": traced_memory(),
    }

    report["trace_rebuild"] = repeated_live(
        "fixture.trace()",
        cfg["trace_repeats"],
        fixture.trace,
    )

    report["value_of"] = repeated_live(
        "trace.value_of(final)",
        cfg["query_repeats"],
        lambda: trace.value_of(final_ref),
    )

    if hasattr(trace, "produced_by"):
        report["produced_by"] = repeated_live(
            "trace.produced_by(final)",
            cfg["query_repeats"],
            lambda: trace.produced_by(final_ref, "reference"),
        )

    if hasattr(trace, "downstream") and first_exec_id is not None:
        report["downstream"] = repeated_live(
            "trace.downstream(first_exec)",
            cfg["query_repeats"],
            lambda: trace.downstream(first_exec_id, "exec_record"),
        )

    if (
        hasattr(trace, "path")
        and first_exec_id is not None
        and last_exec_id is not None
    ):
        report["path"] = repeated_live(
            "trace.path(first,last)",
            cfg["query_repeats"],
            lambda: trace.path(
                first_exec_id,
                last_exec_id,
                source_type="exec_record",
                target_type="exec_record",
            ),
        )

    report["to_model"] = repeated_live(
        "trace.to_model(enrich=False)",
        cfg["model_repeats"],
        lambda: trace.to_model(enrich=False),
    )

    model = trace.to_model(enrich=False)

    if hasattr(TIDE, "report_model_validity"):
        report["model_validity"] = repeated_live(
            "TIDE.report_model_validity(model)",
            cfg["model_repeats"],
            lambda: TIDE.report_model_validity(model),
        )

    if hasattr(trace, "compare"):
        report["compare_self"] = repeated_live(
            "trace.compare(self, structural)",
            cfg["compare_repeats"],
            lambda: trace.compare(trace, mapping="structural"),
        )

    if hasattr(trace, "to_bundle") and hasattr(TIDE, "from_bundle"):
        bundle = trace.to_bundle()
        report["bundle_roundtrip"] = repeated_live(
            "TIDE.from_bundle(trace.to_bundle())",
            cfg["bundle_repeats"],
            lambda: TIDE.from_bundle(bundle),
        )

    if hasattr(TIDE, "from_model"):
        report["from_model"] = repeated_live(
            "TIDE.from_model(model)",
            cfg["bundle_repeats"],
            lambda: TIDE.from_model(model),
        )

    close_tide(fixture)
    del fixture, trace
    gc.collect()

    report["end_memory"] = traced_memory()
    report["total_elapsed_s"] = time.perf_counter() - total_start

    output = (
        Path(args.output).expanduser()
        if args.output
        else Path.cwd() / f"tide_safe_heavy_benchmark_{args.preset}.json"
    )
    if not output.is_absolute():
        output = Path.cwd() / output

    output.write_text(
        json.dumps(report, indent=2, default=str),
        encoding="utf-8",
    )

    print(
        f"\n[TIDE BENCH] DONE in {report['total_elapsed_s']:.2f}s | "
        f"end {fmt_mem(report['end_memory'])}",
        flush=True,
    )
    print(f"[TIDE BENCH] JSON: {output}", flush=True)


if __name__ == "__main__":
    main()
