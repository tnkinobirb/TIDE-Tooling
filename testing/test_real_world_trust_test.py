"""Real-world trust chain across runtime truth, Trace, Bundle and adapters."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from collections import defaultdict
from importlib.util import find_spec
from pathlib import Path

import pytest

from tide_tooling import (
    DELETE_NOW,
    Effects,
    Ocean,
    OpenLineageAdapter,
    TIDE,
    node,
    pipeline,
)
from tide_tooling.tooling_core.bundling.TIDEBundler import TIDEBundler


DYNAMIC_SELECTOR = "global:batch_*:2:2"


def _canonical_digest(value):
    """Independent JSON value identity; deliberately does not use TIDE."""
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _public_reference(reference):
    value = str(reference)
    return value[1:] if value.startswith("@") else value


class _TruthLedger:
    """Minimal application-owned oracle, independent of TIDE internals."""

    def __init__(self):
        self.entries = []
        self.versions = defaultdict(lambda: -1)

    def record(self, execution, *, reads=(), writes=(), effects=()):
        read_facts = []
        for reference in reads:
            reference = _public_reference(reference)
            read_facts.append((reference, self.versions[reference]))

        write_facts = []
        for reference, value in writes:
            reference = _public_reference(reference)
            self.versions[reference] += 1
            write_facts.append((
                reference,
                self.versions[reference],
                _canonical_digest(value),
            ))

        self.entries.append({
            "execution": execution,
            "reads": tuple(read_facts),
            "writes": tuple(write_facts),
            "effects": tuple(effects),
        })

    @property
    def executions(self):
        return {entry["execution"] for entry in self.entries}

    @property
    def reads(self):
        return {
            (entry["execution"], reference, version)
            for entry in self.entries
            for reference, version in entry["reads"]
        }

    @property
    def writes(self):
        return {
            (entry["execution"], reference, version, digest)
            for entry in self.entries
            for reference, version, digest in entry["writes"]
        }


def _write_inputs(root):
    inputs = root / "inputs"
    inputs.mkdir()
    (inputs / "a.json").write_text(
        json.dumps([
            {"id": "a", "value": 2},
            {"id": "b", "value": 3},
        ]),
        encoding="utf-8",
    )
    (inputs / "b.json").write_text(
        json.dumps([
            {"id": "b", "value": 3},
            {"id": "c", "value": 5},
            {"id": "broken", "value": None},
        ]),
        encoding="utf-8",
    )
    return inputs


def _build_workload(root, ledger):
    inputs = _write_inputs(root)
    output = root / "summary.json"

    @node(targets="global:batch_a")
    def load_a():
        value = json.loads((inputs / "a.json").read_text(encoding="utf-8"))
        ledger.record("load_a", writes=(("global:batch_a", value),))
        return value

    @node(targets="global:batch_b")
    def load_b():
        value = json.loads((inputs / "b.json").read_text(encoding="utf-8"))
        ledger.record("load_b", writes=(("global:batch_b", value),))
        return value

    @node(args=(f"@{DYNAMIC_SELECTOR}",), targets="global:merged")
    def merge(batches):
        by_id = {}
        for batch in batches:
            for row in batch:
                if row.get("value") is not None:
                    by_id.setdefault(row["id"], row)
        shared = sorted(by_id.values(), key=lambda row: row["id"])
        value = {"left": shared, "right": shared}
        ledger.record(
            "merge",
            reads=("global:batch_a", "global:batch_b"),
            writes=(("global:merged", value),),
        )
        return value

    @node(
        args=("@global:merged",),
        targets=["global:merged", "global:repeat_done"],
        unpack=True,
    )
    def repeat_equal(merged):
        ledger.record(
            "repeat_equal",
            reads=("global:merged",),
            writes=(
                ("global:merged", merged),
                ("global:repeat_done", True),
            ),
        )
        return merged, True

    @node(
        args=("@global:merged", "@global:repeat_done"),
        targets=["global:summary", "global:temporary"],
        unpack=True,
    )
    def aggregate(merged, _repeat_done):
        rows = merged["left"]
        value = {
            "count": len(rows),
            "total": sum(row["value"] for row in rows),
            "ids": [row["id"] for row in rows],
        }
        temporary = {"remove": True}
        ledger.record(
            "aggregate",
            reads=("global:merged", "global:repeat_done"),
            writes=(
                ("global:summary", value),
                ("global:temporary", temporary),
            ),
        )
        return value, temporary

    @node(args=("@global:summary",), targets="global:export_path")
    def export(summary):
        output.write_text(
            json.dumps(summary, sort_keys=True),
            encoding="utf-8",
        )
        exported = str(output)
        ledger.record(
            "export",
            reads=("global:summary",),
            writes=(("global:export_path", exported),),
            effects=(("file_write", str(output)),),
        )
        return exported

    @node(args=("@global:temporary",), targets="global:cleanup")
    def cleanup(temporary):
        removed = DELETE_NOW("global:temporary")
        missing = DELETE_NOW("global:never")
        value = {
            "temporary": temporary,
            "removed": removed,
            "missing": missing,
        }
        ledger.record(
            "cleanup",
            reads=("global:temporary",),
            writes=(("global:cleanup", value),),
            effects=(
                ("delete", "global:temporary", True),
                ("delete", "global:never", False),
            ),
        )
        return value

    @pipeline
    def real_world_trust_test():
        load_a()
        load_b()
        merge()
        repeat_equal()
        aggregate()
        export()
        cleanup()

    return real_world_trust_test, output


def _trace_facts(trace):
    executions = {record.func_name for record in trace.execs.values()}
    reads = set()
    writes = set()

    for record in trace.log:
        for dependency in record.deps:
            dependencies = (
                dependency.expanded_to
                if dependency.type == "dynamic"
                else (dependency,)
            )
            for resolved in dependencies or ():
                reads.add((
                    record.func_name,
                    _public_reference(resolved.unscoped_reference),
                    resolved.resolved_version,
                ))
        for write in record.writes:
            writes.add((
                record.func_name,
                _public_reference(write.stored_target),
                write.stored_version,
                _canonical_digest(write.stored_result),
            ))

    return {
        "executions": executions,
        "reads": reads,
        "writes": writes,
    }


def _ledger_facts(ledger):
    return {
        "executions": ledger.executions,
        "reads": ledger.reads,
        "writes": ledger.writes,
    }


def _dataset(reference, version):
    return {
        "namespace": "real-world-trust",
        "name": reference,
        "facets": {"version": {"datasetVersion": str(version)}},
    }


def _openlineage_events(ledger):
    events = []
    for entry in ledger.entries:
        events.append({
            "eventType": "COMPLETE",
            "eventTime": "2026-09-29T00:00:00Z",
            "run": {"runId": entry["execution"]},
            "job": {
                "namespace": "real-world-trust",
                "name": entry["execution"],
            },
            "inputs": [
                _dataset(reference, version)
                for reference, version in entry["reads"]
            ],
            "outputs": [
                _dataset(reference, version)
                for reference, version, _ in entry["writes"]
            ],
            "producer": "https://tide-tooling.test/trust-ledger",
        })
    return events


def _openlineage_relations(model):
    def state(identity):
        namespace, reference, version = json.loads(identity)
        assert namespace == "real-world-trust"
        return reference, int(version)

    return {
        "executions": set(model["E"]),
        "reads": {
            (execution, *state(state_id))
            for execution, state_id in model["I"]
        },
        "writes": {
            (execution, *state(state_id))
            for execution, state_id in model["O"]
        },
    }


def test_real_world_trust_test_native_bundle_and_openlineage_agree(tmp_path):
    ledger = _TruthLedger()
    workload, output = _build_workload(tmp_path, ledger)
    ocean = Ocean()
    try:
        trace = ocean.run_pipeline(
            workload,
            mode="sequential",
            dynamic_ref_plan={
                DYNAMIC_SELECTOR: ["global:batch_a", "global:batch_b"],
            },
        )
    finally:
        ocean.close()

    expected = _ledger_facts(ledger)
    assert _trace_facts(trace) == expected
    assert json.loads(output.read_text(encoding="utf-8")) == {
        "count": 3,
        "ids": ["a", "b", "c"],
        "total": 10,
    }

    generations = []
    rebuilt = trace
    for _ in range(3):
        rebuilt = TIDE.from_bundle(rebuilt.to_bundle().to_dict())
        generations.append(_trace_facts(rebuilt))
    assert generations == [expected, expected, expected]

    merged_v0 = rebuilt.value_of("global:merged", 0)
    merged_v1 = rebuilt.value_of("global:merged", 1)
    assert merged_v0["left"] is merged_v0["right"]
    assert merged_v1["left"] is merged_v1["right"]
    assert merged_v0 == merged_v1
    assert merged_v0 is not merged_v1

    delete_facts = {
        (effect.targets[0], effect.target_existed)
        for record in rebuilt.log
        for effect in record.effects
        if effect.effect == Effects.DELETE
    }
    assert delete_facts == {
        ("global:temporary", True),
        ("global:never", False),
    }

    # Caller mutation of one restored value must not rewrite retained history.
    observed = rebuilt.value_of("global:summary", 0)
    observed["ids"].append("caller-mutation")
    assert rebuilt.value_of("global:summary", 0)["ids"] == ["a", "b", "c"]

    lineage_model = OpenLineageAdapter(_openlineage_events(ledger)).to_model()
    lineage = _openlineage_relations(lineage_model)
    assert lineage["executions"] == expected["executions"]
    assert lineage["reads"] == expected["reads"]
    assert lineage["writes"] == {
        (execution, reference, version)
        for execution, reference, version, _ in expected["writes"]
    }

    lineage_trace = TIDE.from_openlineage(_openlineage_events(ledger))
    lineage_rebuilt = TIDE.from_bundle(lineage_trace.to_bundle().to_dict())
    assert lineage_rebuilt.to_model(identity_mode="literal")["I"] == lineage_trace.to_model(
        identity_mode="literal"
    )["I"]

    # A plausible existing execution still cannot claim another producer's
    # exact State relationship.
    tampered = trace.to_bundle().to_dict()
    records = [
        record
        for record in tampered["manifest"]["run"]["records"]
        if record["record_type"] == "exec"
    ]
    by_name = {record["func_name"]: record for record in records}
    by_name["aggregate"]["deps"][0]["producer_exec_id"] = by_name[
        "load_a"
    ]["exec_id"]
    with pytest.raises(ValueError, match="did not write its claimed State"):
        # Bundle loading only decodes persisted state. Exact relationship
        # validation belongs to Trace construction.
        TIDE.from_bundle(tampered)


def test_real_world_trust_test_bundle_loads_in_a_fresh_process(tmp_path):
    ledger = _TruthLedger()
    workload, _ = _build_workload(tmp_path, ledger)
    ocean = Ocean()
    try:
        trace = ocean.run_pipeline(
            workload,
            mode="sequential",
            dynamic_ref_plan={
                DYNAMIC_SELECTOR: ["global:batch_a", "global:batch_b"],
            },
        )
    finally:
        ocean.close()

    bundle_path = tmp_path / "real_world_trust_test.json"
    trace.to_bundle(path=bundle_path)
    code = (
        "import json,sys; from tide_tooling import TIDE; "
        "t=TIDE.from_bundle(sys.argv[1]); "
        "print(json.dumps({'executions':sorted(r.func_name for r in t.execs.values()),"
        "'summary':t.value_of('global:summary',0)},sort_keys=True))"
    )
    completed = subprocess.run(
        [sys.executable, "-c", code, str(bundle_path)],
        check=True,
        capture_output=True,
        text=True,
    )
    restored = json.loads(completed.stdout)
    assert set(restored["executions"]) == ledger.executions
    assert restored["summary"] == {
        "count": 3,
        "ids": ["a", "b", "c"],
        "total": 10,
    }


def test_real_world_trust_test_noworkflow_observes_same_work(tmp_path):
    if find_spec("noworkflow") is None:
        pytest.skip("noWorkflow is not installed")

    _write_inputs(tmp_path)
    script = tmp_path / "real_world_trust_test.py"
    script.write_text(
        """
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent

def load_a():
    return json.loads((ROOT / "inputs" / "a.json").read_text(encoding="utf-8"))

def load_b():
    return json.loads((ROOT / "inputs" / "b.json").read_text(encoding="utf-8"))

def merge(batches):
    by_id = {}
    for batch in batches:
        for row in batch:
            if row.get("value") is not None:
                by_id.setdefault(row["id"], row)
    shared = sorted(by_id.values(), key=lambda row: row["id"])
    return {"left": shared, "right": shared}

def repeat_equal(merged):
    return merged

def aggregate(merged):
    rows = merged["left"]
    return {
        "count": len(rows),
        "total": sum(row["value"] for row in rows),
        "ids": [row["id"] for row in rows],
    }

def export(summary):
    path = ROOT / "now_summary.json"
    path.write_text(json.dumps(summary, sort_keys=True), encoding="utf-8")
    return str(path)

def cleanup():
    return {"removed": True, "missing": False}

first = load_a()
second = load_b()
merged = merge([first, second])
merged = repeat_equal(merged)
summary = aggregate(merged)
export(summary)
cleanup()
""".lstrip(),
        encoding="utf-8",
    )

    project_root = Path(__file__).resolve().parents[1]
    project_dir = tmp_path / "now"
    result_path = tmp_path / "now_trace_summary.json"
    environment = os.environ.copy()
    environment["PYTHONPATH"] = os.pathsep.join(
        part
        for part in (
            str(project_root),
            environment.get("PYTHONPATH"),
        )
        if part
    )
    code = (
        "import json,sys; from pathlib import Path; from tide_tooling import TIDE; "
        "t=TIDE.now(sys.argv[1],project_dir=sys.argv[2]); "
        "Path(sys.argv[3]).write_text(json.dumps(sorted({r.func_name for r in t.log})),encoding='utf-8')"
    )
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            code,
            str(script),
            str(project_dir),
            str(result_path),
        ],
        cwd=project_root,
        env=environment,
        capture_output=True,
        text=True,
        timeout=90,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr

    observed_functions = set(json.loads(result_path.read_text(encoding="utf-8")))
    expected_functions = {
        "load_a",
        "load_b",
        "merge",
        "repeat_equal",
        "aggregate",
        "export",
        "cleanup",
    }
    assert expected_functions <= observed_functions
    assert json.loads((tmp_path / "now_summary.json").read_text(encoding="utf-8")) == {
        "count": 3,
        "ids": ["a", "b", "c"],
        "total": 10,
    }
