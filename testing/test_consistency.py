import inspect
from types import SimpleNamespace

from tide_tooling.tooling_core.engine.tide_logging import (
    Actor,
    EffectEdge,
    Investigate,
    Inspected,
    Nearby,
    Object,
    Pathed,
    ProducedBy,
    Searched,
    Upstream,
)
from tide_tooling import TIDE


RENDERABLES = [
    Actor,
    Upstream,
    Pathed,
    Searched,
    ProducedBy,
    Investigate,
    Inspected,
]


def test_renderable_show_methods_share_optional_display_controls():
    for renderable in RENDERABLES:
        parameters = inspect.signature(renderable.show).parameters

        assert "color" in parameters, renderable.__name__
        assert "simple" in parameters, renderable.__name__
        assert "colour" in parameters, renderable.__name__


def test_renderable_show_color_false_is_plain_text():
    tide = TIDE()
    trace = tide.trace()
    inspected = Inspected(
        Object(Actor(0, "run", "writer"), "actor", "writer")
    )
    nearby = Nearby("writer")
    searched = Searched(
        "writer",
        [],
        [],
        [],
        [],
        [],
        nearby,
    )
    objects = [
        Actor(0, "run", "writer"),
        inspected,
        Investigate(trace, trace.inspect, inspected),
        searched,
        Upstream(inspected, {}, {}),
        Pathed(
            inspected,
            inspected,
            {},
            "downstream",
            inspect=trace.inspect,
        ),
        ProducedBy(trace.inspect, inspected, [], [], {}),
    ]

    for obj in objects:
        output = obj.show(color=False, simple=True)
        assert "\x1b[" not in output, type(obj).__name__


def test_effect_edge_keeps_record_ownership_and_derived_exec_ids():
    producer = SimpleNamespace(exec_id="producer-exec")
    effect_record = object()
    affected_record = SimpleNamespace(affected_exec_id="consumer-exec")
    edge = EffectEdge(producer, effect_record, affected_record)

    assert edge.producer is producer
    assert edge.effect_record is effect_record
    assert edge.affected_record is affected_record
    assert edge.producer_exec_id == "producer-exec"
    assert edge.consumer_exec_id == "consumer-exec"
