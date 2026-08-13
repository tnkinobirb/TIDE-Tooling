import inspect

from tide_tooling.tooling_core.engine.tide_logging import (
    AffectedStackLink,
    Actor,
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
    AffectedStackLink,
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
        trace.inspect,
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
        AffectedStackLink(
            "producer-exec",
            Actor(0, "run", "producer"),
            "effect",
            "global:@value",
            0,
            "consumer-exec",
            Actor(0, "run", "consumer"),
            ["args"],
        ),
        inspected,
        Investigate(trace, trace.inspect, inspected),
        searched,
        Upstream(inspected, {}, {}),
        Pathed(
            trace.inspect,
            trace.index,
            inspected,
            [],
            inspected,
            [],
            {},
            direction="downstream",
        ),
        ProducedBy(trace.inspect, inspected, [], [], {}),
    ]

    for obj in objects:
        output = obj.show(color=False, simple=True)
        assert "\x1b[" not in output, type(obj).__name__


def test_affected_stack_link_is_serialisable_and_diffable():
    first = AffectedStackLink(
        "producer-exec",
        Actor(0, "run", "producer"),
        "effect",
        "global:@value",
        0,
        "consumer-exec",
        Actor(0, "run", "consumer"),
        ["args"],
    )
    second = AffectedStackLink(
        "producer-exec",
        Actor(0, "run", "producer"),
        "effect",
        "global:@other",
        0,
        "consumer-exec",
        Actor(0, "run", "consumer"),
        ["args"],
    )

    assert first.to_dict()["ref"] == "global:@value"
    assert first.diff(second).changed["ref"] == {
        "before": "global:@value",
        "after": "global:@other",
    }
