"""Cross-platform tests for terminal output encoding."""

import sys

from tide_tooling.tooling_core.engine.tide_rendering import _show_render, show_output


class CP1252Stream:
    encoding = "cp1252"

    def __init__(self):
        self.parts = []

    def write(self, value):
        value.encode(self.encoding)
        self.parts.append(value)
        return len(value)

    def flush(self):
        return None


def test_show_render_degrades_glyphs_for_cp1252_without_changing_result(monkeypatch):
    stream = CP1252Stream()
    monkeypatch.setattr(sys, "stdout", stream)

    rendered = _show_render(["load └── raw_data │ clean"], color=False)

    assert rendered == "load └── raw_data │ clean"
    assert "load +-- raw_data | clean" in "".join(stream.parts)


def test_show_output_degrades_glyphs_for_cp1252(monkeypatch):
    stream = CP1252Stream()
    monkeypatch.setattr(sys, "stdout", stream)

    rendered = show_output(["…", "└─"])

    assert rendered == "…\n└─"
    assert "…\n+-" in "".join(stream.parts)
