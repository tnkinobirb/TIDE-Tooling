import io
from contextlib import redirect_stdout

import pytest

from tide_tooling import TIDE
from tide_tooling.core import tide_value_hasher


@pytest.fixture
def trace():
    tide = TIDE()
    tide.event("WriteA0", writes={"@A": "first"})
    tide.event("WriteA1", writes={"@A": "second"})
    return tide.trace()


def test_value_of_an_unversioned_reference_returns_every_version(trace):
    assert trace.value_of("global:@A") == ["first", "second"]


def test_value_of_a_reference_version_returns_one_value(trace):
    assert trace.value_of("global:@A", 1) == "second"


def test_value_of_accepts_an_inspected_reference_and_its_version(trace):
    inspected = trace.inspect("global:@A", "reference", version=1)

    assert trace.value_of(inspected) == "second"


def test_value_of_rejects_a_conflicting_inspected_reference_version(trace):
    inspected = trace.inspect("global:@A", "reference", version=1)

    with pytest.raises(ValueError, match="conflicts"):
        trace.value_of(inspected, version=0)


def test_value_of_rejects_an_inspected_non_reference(trace):
    inspected = trace.inspect(trace.exec_log[0].exec_id, "exec_record")

    with pytest.raises(TypeError, match="inspected reference"):
        trace.value_of(inspected)


def test_value_of_a_tide_hash_returns_one_value(trace):
    assert trace.value_of(tide_value_hasher("first")) == "first"


def test_value_of_finds_a_loose_reference_and_rejects_missing_versions(trace):
    assert trace.value_of("@A") == ["first", "second"]

    with pytest.raises(KeyError):
        trace.value_of("global:@A", 99)


def test_value_of_returns_values_for_every_discovered_ref_and_version():
    tide = TIDE()
    tide.event("WriteA0", writes={"@A": "a0"})
    tide.event("WriteA1", writes={"@A": "a1"})
    tide.event("WriteB0", writes={"@B": "b0"})

    trace = tide.trace()

    assert trace.value_of("global:@") == ["a0", "a1", "b0"]


def test_value_of_one_discovered_reference_keeps_the_exact_return_shape():
    tide = TIDE()
    tide.event("WriteA0", writes={"@A": "a0"})
    tide.event("WriteA1", writes={"@A": "a1"})

    assert tide.trace().value_of("@A") == ["a0", "a1"]


def test_value_of_exact_reference_wins_before_broader_discovery():
    tide = TIDE()
    tide.event("WriteA", writes={"@A": "a"})
    tide.event("WriteAB", writes={"@AB": "ab"})

    assert tide.trace().value_of("global:@A") == ["a"]


def test_value_of_discovery_applies_a_requested_version_to_every_match():
    tide = TIDE()
    tide.event("WriteA0", writes={"@A": "a0"})
    tide.event("WriteA1", writes={"@A": "a1"})
    tide.event("WriteB0", writes={"@B": "b0"})
    tide.event("WriteB1", writes={"@B": "b1"})

    assert tide.trace().value_of("global:@", version=1) == ["a1", "b1"]


def test_value_of_missing_discovery_raises_unknown_reference(trace):
    with pytest.raises(KeyError, match="Unknown TIDE reference"):
        trace.value_of("missing")


def test_value_of_show_displays_each_match_with_the_usual_value_view():
    tide = TIDE()
    tide.event("WriteA0", writes={"@A": "a0"})
    tide.event("WriteA1", writes={"@A": "a1"})
    output = io.StringIO()

    with redirect_stdout(output):
        result = tide.trace().value_of("global:@", show=True)

    assert result == ["a0", "a1"]
    shown = output.getvalue()
    assert shown.count("VALUE") == 1
    assert "Reference: global:@A" in shown
    assert "[v0]" in shown
    assert "[v1]" in shown


def test_value_of_show_displays_one_value_view_per_discovered_reference():
    tide = TIDE()
    tide.event("WriteA", writes={"@A": "a"})
    tide.event("WriteB", writes={"@B": "b"})
    output = io.StringIO()

    with redirect_stdout(output):
        result = tide.trace().value_of("global:@", show=True)

    assert result == ["a", "b"]
    shown = output.getvalue()
    assert shown.count("Reference: global:@A") == 1
    assert shown.count("Reference: global:@B") == 1


def test_value_of_rejects_non_boolean_show(trace):
    with pytest.raises(TypeError, match="show must be a bool"):
        trace.value_of("global:@", show="yes")


def test_plural_value_lookup_methods_are_removed(trace):
    assert not hasattr(trace, "values" + "_of")
    assert not hasattr(trace, "show_values" + "_of")
