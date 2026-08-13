from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tide_tooling.tooling_core.engine.classes import Event

from typing import Callable


class StoreInterface:
    def __init__(self, create_scoped_router: Callable):
        self.create_scoped_router = create_scoped_router

        
class EventInterface:
    def __init__(self, event_emitter: Callable, event_subscriber) -> None:
        self._event_emitter = event_emitter # or ocean_queue
        self.event_subscriber = event_subscriber
    
    def emit_event_to_ocean(self, event: Event) -> None:
        self._event_emitter(event)
#
#
