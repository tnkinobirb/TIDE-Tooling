import copy

from tide_tooling import TIDE
from tide_tooling.tooling_core.engine.tide_rendering import strip_ansi


def _trace():
    tide = TIDE()
    tide.declare("source", "alpha")
    tide.declare("donor", 42)

    def first(value):
        return f"{value}-one"

    def second(value):
        return f"{value}-two"

    tide.exec(first, reads="source", writes="middle")
    tide.exec(second, reads="middle", writes="result")
    return tide.trace()


def _ref(trace, suffix):
    return next(
        ref
        for ref in trace.index.seen_refs
        if ref.split(":", 1)[-1] == suffix
    )


def _divergent_trace():
    source = _trace()
    source_ref = _ref(source, "source")
    donor_ref = _ref(source, "donor")

    expected_hash = source.index.value_hash_by_ref_version[
        (source_ref, 0)
    ]
    donor_hash = source.index.value_hash_by_ref_version[
        (donor_ref, 0)
    ]

    payload = source.to_bundle().to_dict()
    values = payload["manifest"]["values"]

    values[expected_hash]["bundle"] = copy.deepcopy(
        values[donor_hash]["bundle"]
    )
    values[expected_hash]["root_cas_ref"] = values[
        donor_hash
    ]["root_cas_ref"]

    return TIDE.from_bundle(payload), source_ref


def _state_dicts(value):
    if isinstance(value, dict):
        if {
            "ref",
            "version",
            "value_type",
            "divergent",
        }.issubset(value):
            yield value
        for child in value.values():
            yield from _state_dicts(child)
    elif isinstance(value, list):
        for child in value:
            yield from _state_dicts(child)


def test_consumed_by_does_not_repeat_actor_name():
    trace = _trace()
    source_ref = _ref(trace, "source")

    shown = strip_ansi(
        trace.consumed_by(
            source_ref,
            kind="reference",
            group=False,
        ).show()
    )

    assert "EXEC_RECORD" in shown
    assert "first" in shown
    assert "(first)" not in shown


def test_path_shows_recorded_state_type():
    trace = _trace()
    source_ref = _ref(trace, "source")
    result_ref = _ref(trace, "result")

    shown = strip_ansi(
        trace.path(
            source_ref,
            result_ref,
            source_type="reference",
            target_type="reference",
            group=False,
        ).show()
    )

    assert f"{source_ref}[v0] as str" in shown


def test_divergent_path_is_lightweight_but_explicit():
    trace, source_ref = _divergent_trace()
    result_ref = _ref(trace, "result")

    query = trace.path(
        source_ref,
        result_ref,
        source_type="reference",
        target_type="reference",
        group=False,
    )
    shown = strip_ansi(query.show())

    assert "DIVERGENT" in shown
    assert f"!{source_ref}[v0] as str" in shown
    assert "recorded as:" not in shown
    assert "reconstructed as:" not in shown

    states = list(_state_dicts(query.to_dict()))
    source_state = next(
        state
        for state in states
        if state["ref"] == source_ref
        and state["version"] == 0
    )
    assert source_state["value_type"] == "str"
    assert source_state["divergent"] is True
    assert query.to_dict()["divergent"] is True


def test_divergent_downstream_is_lightweight_but_explicit():
    trace, source_ref = _divergent_trace()

    query = trace.downstream(
        source_ref,
        type="reference",
        grouped=False,
    )
    shown = strip_ansi(query.show())

    assert "DIVERGENT" in shown
    assert f"!{source_ref}[v0] as str" in shown
    assert "recorded as:" not in shown
    assert "reconstructed as:" not in shown
    assert query.to_dict()["divergent"] is True


def test_divergent_upstream_carries_the_same_state_marker():
    trace, source_ref = _divergent_trace()
    result_ref = _ref(trace, "result")

    query = trace.upstream(
        result_ref,
        type="reference",
        grouped=False,
    )
    shown = strip_ansi(query.show())

    assert "DIVERGENT" in shown
    assert f"!{source_ref}[v0] as str" in shown
    assert "recorded as:" not in shown
    assert "reconstructed as:" not in shown
    assert query.to_dict()["divergent"] is True
