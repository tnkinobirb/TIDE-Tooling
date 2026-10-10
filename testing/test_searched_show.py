import sys
from types import SimpleNamespace
import types

sys.modules.setdefault("graphviz", types.ModuleType("graphviz"))
sys.modules.setdefault("jsonpickle", types.ModuleType("jsonpickle"))

from tide_tooling.tooling_core.engine.tide_logging import Nearby, Searched


def match(identifier, version=None):
    return SimpleNamespace(id=identifier, version=version)


def test_searched_show_filters_to_nearby_results_for_one_type():
    searched = Searched(
        subject="@B",
        execs=[],
        actors=[],
        refs=[],
        effects=[],
        values=[],
        nearby=Nearby(
            "@B",
            refs=[match("global:@B"), match("testing:@B")],
        ),
    )

    shown = searched.show(
        kind="reference",
        exact=False,
        nearby=True,
    )

    assert "FIND" in shown
    assert "NEARBY" in shown
    assert "global:@B" in shown
    assert "testing:@B" in shown
    assert "EXACT" not in shown


def test_searched_show_filters_to_exact_results_for_one_type():
    searched = Searched(
        subject="global:@B",
        execs=[],
        actors=[],
        refs=[match("global:@B", version=1)],
        effects=[],
        values=[],
        nearby=Nearby("global:@B"),
    )

    shown = searched.show(
        kind="reference",
        exact=True,
        nearby=False,
    )

    assert "EXACT" in shown
    assert "global:@B[v1]" in shown
    assert "NEARBY" not in shown
