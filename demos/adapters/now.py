"""Capture ordinary Python with noWorkflow and inspect its native TIDE ancestry."""

from hashlib import sha256
import json
from pathlib import Path
import sys
from tempfile import TemporaryDirectory

from tide_tooling import TIDE


def fingerprint(value):
    payload = json.dumps(value, sort_keys=True).encode()
    return sha256(payload).hexdigest()[:12]


def workload():
    summary = {
        "records": 3,
        "valid": True,
    }
    report = dict(summary)
    report["fingerprint"] = fingerprint(summary)
    print(report)


def main():
    with TemporaryDirectory(
        prefix="tide-now-adapter-",
        ignore_cleanup_errors=True,
    ) as project_dir:
        trace = TIDE.now(
            Path(__file__),
            "--workload",
            project_dir=project_dir,
        )

    trace.show(color=True)

    fingerprint_exec = next(
        record for record in trace.log
        if record.func_name == "fingerprint"
    )

    print("\nFINGERPRINT CONTEXT")
    trace.inspect(fingerprint_exec.exec_id, "exec_record").now().show()
    trace.parents_of(fingerprint_exec.exec_id).show(show_in_out=False)
    trace.children_of(fingerprint_exec.exec_id).show(show_in_out=False)


if __name__ == "__main__":
    workload() if "--workload" in sys.argv else main()
