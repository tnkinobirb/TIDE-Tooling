"""Smoke-test every shipped demo as a user would run it."""

from __future__ import annotations

import ast
from importlib.util import find_spec
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEMO_ROOT = PROJECT_ROOT / "demos"

# Producer/importer pairs are deliberately ordered so the first script creates
# the bundle consumed by the second. Keep helpers in the manifest too: this
# makes adding an untested demo file a test failure rather than silent drift.
DEMO_ORDER = (
    "start_here/shortest_route.py",
    "adapters/adapter_showcase.py",
    "adapters/now.py",
    "adapters/openinference.py",
    "adapters/openlineage.py",
    "adapters/opentelemetry.py",
    "readme/explicit_tide.py",
    "readme/explicit_ocean.py",
    "readme/implicit_ocean.py",
    "readme/passive_tide.py",
    "readme/register_stores.py",
    "readme/exec_relationships.py",
    "effects/effects_demo.py",
    "effects/emission_demo.py",
    "model/enriched_model.py",
    "model/from_model.py",
    "now/ipaw/run.py",
    "now/ipaw/workflow/experiment.py",
    "now/ipaw/workflow/external.py",
    "now/streamlined_showcase.py",
    "pipeline/functions.py",
    "pipeline/data_pipeline.py",
    "pipeline/from_bundle.py",
    "pipeline/destruction_test.py",
    "comparator/_base_functions.py",
    "comparator/_base_data_pipeline.py",
    "comparator/_adjusted_functions.py",
    "comparator/_adjusted_data_pipeline.py",
    "comparator/comparator.py",
    "trace/basic_trace.py",
    "trace/from_bundle.py",
    "bug/bug_pipeline.py",
    "bug/from_bundle.py",
)

# Imported support modules remain smoke-tested but are not presented as
# standalone commands in the README.
NON_ENTRYPOINT_DEMOS = {
    "now/ipaw/workflow/experiment.py",
    "now/ipaw/workflow/external.py",
    "pipeline/functions.py",
    "comparator/_base_functions.py",
    "comparator/_base_data_pipeline.py",
    "comparator/_adjusted_functions.py",
    "comparator/_adjusted_data_pipeline.py",
}

NOWORKFLOW_DEMOS = {
    "adapters/adapter_showcase.py",
    "adapters/now.py",
    "now/ipaw/run.py",
    "now/streamlined_showcase.py",
}


def test_readme_python_examples_parse():
    readme = (PROJECT_ROOT / "README.md").read_text(encoding="utf-8")
    python_blocks = re.findall(r"```python\s*\n(.*?)```", readme, re.DOTALL)

    assert python_blocks
    for block_number, block in enumerate(python_blocks, start=1):
        try:
            ast.parse(block)
        except SyntaxError as error:
            pytest.fail(
                f"README Python block {block_number} is invalid: {error}"
            )


def test_readme_introduces_capture_before_export_and_links_ocean_docs():
    readme = (PROJECT_ROOT / "README.md").read_text(encoding="utf-8")

    quickstart_index = readme.index("\n## Quickstart\n")
    inspect_index = readme.index("\n## Inspect a Trace\n")
    export_index = readme.index("\n## Export and compare\n")

    assert quickstart_index < inspect_index < export_index
    assert "docs/Ocean_Rundown.md" in readme



def test_demo_manifest_covers_every_python_file():
    discovered = {
        path.relative_to(DEMO_ROOT).as_posix()
        for path in DEMO_ROOT.rglob("*.py")
    }
    declared = set(DEMO_ORDER)

    assert len(DEMO_ORDER) == len(declared), "DEMO_ORDER contains duplicates"
    assert declared == discovered, (
        f"Untested demos: {sorted(discovered - declared)}; "
        f"missing demo files: {sorted(declared - discovered)}"
    )
    assert NON_ENTRYPOINT_DEMOS <= declared
    assert NOWORKFLOW_DEMOS <= declared

def test_every_demo_runs_without_error(tmp_path):
    copied_root = tmp_path / "demos"
    shutil.copytree(DEMO_ROOT, copied_root)

    environment = os.environ.copy()
    existing_pythonpath = environment.get("PYTHONPATH")
    environment["PYTHONPATH"] = os.pathsep.join(
        part
        for part in (str(PROJECT_ROOT), existing_pythonpath)
        if part
    )

    for relative_path in DEMO_ORDER:
        if (
            relative_path in NOWORKFLOW_DEMOS
            and find_spec("noworkflow") is None
        ):
            continue
        completed = subprocess.run(
            [sys.executable, str(copied_root / relative_path)],
            cwd=tmp_path,
            env=environment,
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        if completed.returncode:
            pytest.fail(
                f"Demo {relative_path} exited with {completed.returncode}.\n"
                f"STDOUT:\n{completed.stdout[-4000:]}\n"
                f"STDERR:\n{completed.stderr[-4000:]}"
            )
