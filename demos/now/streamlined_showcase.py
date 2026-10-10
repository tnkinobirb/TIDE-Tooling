"""Capture ordinary Python with NOW, then explore it as a TIDE Trace."""

from collections import Counter
from hashlib import sha256
import json
from pathlib import Path
from statistics import fmean
import sys
from tempfile import TemporaryDirectory

from tide_tooling import TIDE


def normalise(words):
    return [word.strip().lower() for word in words]


def summarise(words):
    return {
        "counts": dict(Counter(words)),
        "mean_length": round(fmean(map(len, words)), 2),
    }


def fingerprint(summary):
    payload = json.dumps(summary, sort_keys=True).encode()
    return sha256(payload).hexdigest()[:12]


def workload():
    words = normalise([" Tide ", "Trace", " tide", "NOW "])
    summary = summarise(words)
    report = dict(summary)
    report["fingerprint"] = fingerprint(summary)
    print(report)


def showcase():
    # NOW reruns this file and TIDE projects the captured trial into a Trace.
    with TemporaryDirectory(
        prefix="tide-now-",
        ignore_cleanup_errors=True,
    ) as project_dir:
        trace = TIDE.now(
            Path(__file__),
            "--workload",
            project_dir=project_dir,
        )

    fingerprint_exec = next(
        record for record in trace.log
        if record.func_name == "fingerprint"
    )

    # Inspect one call and the Python operations captured inside it.
    inspected = trace.inspect(fingerprint_exec.exec_id, "exec_record")
    inspected.now().show()
    # trace.show(show_undeclared=True, colour=False)

    # Activation context and captured operations remain separate facts.
    trace.parents_of(fingerprint_exec.exec_id).show(show_in_out=False)
    trace.children_of(fingerprint_exec.exec_id).show()

    validity = TIDE.report_model_validity(trace.to_model())
    print("\nStructurally valid TIDE Model:", validity.structurally_valid)

    trace.why("evaluation:1[v0]", color=False)


if __name__ == "__main__":
    workload() if "--workload" in sys.argv else showcase()
