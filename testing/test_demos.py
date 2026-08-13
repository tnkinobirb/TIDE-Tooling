"""Smoke-test every shipped demo as a user would run it."""

from __future__ import annotations

import ast
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
    "readme/explicit_tide.py",
    "readme/explicit_ocean.py",
    "readme/implicit_ocean.py",
    "readme/passive_tide.py",
    "readme/register_stores.py",
    "readme/exec_relationships.py",
    "effects/effects_demo.py",
    "model/from_model.py",
    "pipeline/functions.py",
    "pipeline/data_pipeline.py",
    "pipeline/from_bundle.py",
    "trace/basic_trace.py",
    "trace/from_bundle.py",
    "bug/bug_pipeline.py",
    "bug/from_bundle.py",
)

# Imported support modules remain smoke-tested but are not presented as
# standalone commands in the README.
NON_ENTRYPOINT_DEMOS = {
    "pipeline/functions.py",
}


def test_demo_manifest_is_complete_and_documented():
    discovered = {
        path.relative_to(DEMO_ROOT).as_posix()
        for path in DEMO_ROOT.rglob("*.py")
        if "__pycache__" not in path.parts
    }
    assert set(DEMO_ORDER) == discovered

    readme = (PROJECT_ROOT / "README.md").read_text(encoding="utf-8")
    for relative_path in DEMO_ORDER:
        if relative_path in NON_ENTRYPOINT_DEMOS:
            assert f"demos/{relative_path}" not in readme
            continue
        assert f"demos/{relative_path}" in readme


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


def test_readme_introduces_tide_before_ocean():
    readme = (PROJECT_ROOT / "README.md").read_text(encoding="utf-8")

    tide_index = readme.index("\n## TIDE\n")
    export_index = readme.index("\n## Export — *experimental*\n")
    ocean_index = readme.index("\n## Ocean\n")

    assert tide_index < export_index < ocean_index
    assert readme.index("\n### TIDE Model\n") < ocean_index


def test_checked_in_pipeline_bundle_uses_portable_paths():
    bundle = (DEMO_ROOT / "pipeline" / "data_pipeline_trace.json").read_text(
        encoding="utf-8"
    )

    assert "PosixPath('/" not in bundle
    assert re.search(r"WindowsPath\(['\"]?[A-Za-z]:", bundle) is None


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
