from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tide_tooling.tooling_core.engine.classes import Event

from typing import Callable


class EventInterface:
    def __init__(self, ocean_id, event_emitter: Callable, event_subscriber) -> None:
        self.ocean_id = ocean_id
        self._event_emitter = event_emitter # or ocean_queue
        self.event_subscriber = event_subscriber
    
    def emit_event_to_ocean(self, event: Event) -> None:
        self._event_emitter(event)
#
#
