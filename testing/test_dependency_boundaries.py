"""Keep the installed TIDE package free of third-party runtime imports."""

import ast
from pathlib import Path
import sys

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SHIPPED_SOURCE_GROUPS = ("public", "internal")
ALLOWED_IMPORT_ROOTS = frozenset(sys.stdlib_module_names) | {"tide_tooling"}


def _absolute_import_roots(source_path):
    tree = ast.parse(source_path.read_text(encoding="utf-8"), filename=str(source_path))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            yield from (alias.name.partition(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            yield node.module.partition(".")[0]


@pytest.mark.parametrize("source_group", SHIPPED_SOURCE_GROUPS)
def test_shipped_modules_import_only_stdlib_or_tide_modules(source_group):
    unexpected = []

    package_root = PROJECT_ROOT / "tide_tooling"
    internal_root = package_root / "tooling_core"
    if source_group == "internal":
        source_paths = sorted(internal_root.rglob("*.py"))
    else:
        source_paths = [
            path
            for path in sorted(package_root.rglob("*.py"))
            if internal_root not in path.parents
        ]

    for source_path in source_paths:
        for imported_root in _absolute_import_roots(source_path):
            if imported_root not in ALLOWED_IMPORT_ROOTS:
                unexpected.append(
                    f"{source_path.relative_to(PROJECT_ROOT)} imports {imported_root}"
                )

    assert unexpected == [], (
        "Shipped modules must import only the Python standard library or TIDE "
        "modules:\n" + "\n".join(unexpected)
    )
