"""Capture noWorkflow's IPAW workflow and inspect one exact activation.

Verified with noWorkflow 2.1.3: NOW's 124 activations and 883 evaluations
become 1,007 TIDE execution records, while all 57 file accesses become 57
TIDE Effect records.
"""

from pathlib import Path
import shutil
from tempfile import TemporaryDirectory

from tide_tooling import TIDE


HERE = Path(__file__).resolve().parent
INPUTS = tuple(f"anatomy{index}.img" for index in range(1, 5))


def main():
    # Copy the workflow so its generated files do not modify this example.
    with TemporaryDirectory(
        prefix="tide-ipaw-",
        ignore_cleanup_errors=True,
    ) as directory:
        project = Path(directory)
        shutil.copytree(HERE / "workflow", project, dirs_exist_ok=True)

        trace = TIDE.now(
            project / "experiment.py",
            *INPUTS,
            "reference.img",
            project_dir=project,
        )

    # Show the captured NOW activation and its exact TIDE projection.
    activation = next(
        record for record in trace.log
        if record.func_name == "align_warp"
    )
    inspected = trace.inspect(activation.exec_id, "exec_record")
    inspected.now().show()
    inspected.now().trace().show()


if __name__ == "__main__":
    main()
