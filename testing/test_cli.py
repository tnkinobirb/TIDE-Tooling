from pathlib import Path

from tide_tooling import TIDE
from tide_tooling.cli import main


def _bundle(tmp_path):
    tide = TIDE()
    tide.event("seed", writes={"global:value": 1})
    path = tmp_path / "run.json"
    tide.trace().to_bundle(path=path)
    return path


def _changed_bundle(tmp_path):
    tide = TIDE()
    tide.event("seed", writes={"global:value": 2})
    path = tmp_path / "changed.json"
    tide.trace().to_bundle(path=path)
    return path


def test_cli_loads_one_bundle_and_runs_trace_commands(tmp_path, capsys):
    bundle = _bundle(tmp_path)

    assert main([str(bundle), "show", "--no-color"]) == 0
    assert "TRACE" in capsys.readouterr().out

    assert main([
        str(bundle),
        "why",
        "seed[0]",
        "--kind",
        "actor",
        "--no-color",
    ]) == 0
    assert "INVESTIGATE" in capsys.readouterr().out

    assert main([
        str(bundle),
        "consumed-by",
        "seed[0]",
        "--kind",
        "actor",
        "--no-color",
    ]) == 0
    assert "CONSUMED BY" in capsys.readouterr().out

    assert main([
        str(bundle),
        "produced-by",
        "global:value",
        "--kind",
        "reference",
        "--no-color",
    ]) == 0
    assert "PRODUCED BY" in capsys.readouterr().out


def test_cli_diffs_and_compares_two_bundles(tmp_path, capsys):
    source = _bundle(tmp_path)
    target = _changed_bundle(tmp_path)

    assert main([str(source), "diff", str(target), "--no-color"]) == 0
    assert "DIFF | Trace" in capsys.readouterr().out

    assert main([
        str(source),
        "compare",
        str(target),
        "--mapping",
        "literal",
        "--full",
        "--no-color",
    ]) == 0
    compared = capsys.readouterr().out
    assert "TIDE MODEL COMPARE" in compared
    assert "mapping: literal" in compared


def test_cli_now_refuses_existing_bundle_before_capture(
    tmp_path,
    monkeypatch,
    capsys,
):
    script = tmp_path / "workflow.py"
    script.write_text("print('must not run')", encoding="utf-8")
    output = tmp_path / "existing.json"
    output.write_text("existing", encoding="utf-8")
    called = False

    def capture(*args, **kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(TIDE, "now", capture)

    assert main([
        "now",
        str(script),
        "-o",
        str(output),
    ]) == 2
    assert called is False
    assert output.read_text(encoding="utf-8") == "existing"
    error = capsys.readouterr().err
    assert f"Bundle already exists: {output}" in error
    assert "Use --force to overwrite it." in error


def test_cli_now_force_captures_and_exports_to_the_exact_path(
    tmp_path,
    monkeypatch,
):
    script = tmp_path / "workflow.py"
    script.write_text("pass", encoding="utf-8")
    output = tmp_path / "existing.json"
    output.write_text("existing", encoding="utf-8")
    captured = {}

    class FakeTrace:
        def to_bundle(self, *, path):
            captured["output"] = Path(path)

    def capture(script_path, *script_args, project_dir=None):
        captured["script"] = Path(script_path)
        captured["args"] = script_args
        captured["project_dir"] = project_dir
        return FakeTrace()

    monkeypatch.setattr(TIDE, "now", capture)

    assert main([
        "now",
        str(script),
        "-o",
        str(output),
        "--force",
        "--",
        "one",
        "two",
    ]) == 0
    assert captured == {
        "script": script.resolve(),
        "args": ("one", "two"),
        "project_dir": None,
        "output": output.resolve(),
    }
