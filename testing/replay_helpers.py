"""Test-only access to the forward Event reducer's construction state."""

from tide_tooling.tooling_core.engine.event_occurrences import (
    FrozenEventLog,
    TIDEEventDeserialiser,
    TIDEEventSerialiser,
)
from tide_tooling.tooling_core.engine.event_readers import EventLogReader


def replay_events(events, *, expected_ocean_id=None, **_unused):
    frozen = FrozenEventLog.capture(events)
    materialised = frozen.events
    if not materialised:
        raise ValueError("Event replay requires at least one Event")
    ocean_id = materialised[0].ocean_id
    if expected_ocean_id is not None and ocean_id != expected_ocean_id:
        raise ValueError("Event stream ocean identity disagrees with its boundary")
    return EventLogReader(ocean_id)._construct_log(materialised)


def serialise_events(events):
    return TIDEEventSerialiser().serialise(events)


def deserialise_events(graph, **options):
    return TIDEEventDeserialiser(**options).deserialise(graph)
