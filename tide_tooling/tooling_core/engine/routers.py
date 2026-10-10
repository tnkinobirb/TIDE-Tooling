from __future__ import annotations
from threading                    import RLock
from enum                       import StrEnum
from typing                     import Any
from collections                import defaultdict, deque

from tide_tooling.tooling_core.engine.event_occurrences import (
    EventLogger,
)
from tide_tooling.tooling_core.engine.classes        import Event, DataRef, __fallback__
from tide_tooling.tooling_core.engine.errors         import MissingDepKeyError
from tide_tooling.tooling_core.engine.events         import Cause, RouterEvents, PipelineEvents, LogEvents
from tide_tooling.tooling_core.engine.event_payloads import Reference, State
from tide_tooling.tooling_core.engine.registries     import StoreRegistry
from tide_tooling.tooling_core.engine.utils          import tide_snapshot_value
from tide_tooling.tooling_core                       import debug_print as print


# STORE ROUTER 
# 
class StoreRouter:
    _router_count = 0
    def __init__(
        self,
        registry: StoreRegistry,
        event_interface,
        consequence_emitter=None,
    ) -> None:
       
        self.id = StoreRouter._router_count
        StoreRouter._router_count+=1
        self.registry = registry    
        self.event_queue = deque()
        self._event_lock = RLock()
        self.event_emitter = event_interface.emit_event_to_ocean
        self.consequence_emitter = consequence_emitter or self.event_emitter

        event_interface.event_subscriber(RouterEvents, self.consume_event)

        self.event_handlers = {
            RouterEvents.STORE_WRITE: self.on_store_write,
            RouterEvents.FORCE_WRITE: self.on_force_write, 
            RouterEvents.DELETE: self.on_delete
        }
    
    def tick(self):
        """Process queued store events serially in their arrival order."""
        with self._event_lock:
            while self.event_queue:
                event = self.event_queue.pop()
                self.event_handlers[event.type](event)

    def get_store(self, store):
        # Select the exact Store named by the reference.
        return self.registry.get(store)

    def consume_event(self, event):
        with self._event_lock:
            self.event_queue.appendleft(event)

    def _set_and_emit(self, event, force):
        # Set one value and emit its dependency consequence.
        requested = event.payload
        state = requested._replace(
            snapshot=tide_snapshot_value(
                requested.value,
                boundary="write",
            ),
        )
        update = self.set(state.ref.store, state.ref.key, state.value, force)

        if update is __fallback__:
            return

        store = self.get_store(state.ref.store)
        target_runner_id = "*" if store.scope == "global" else event.target_runner_id
        event_type = PipelineEvents.DEP_UPDATED if update else PipelineEvents.DEP_AVAILABLE

        self.consequence_emitter(Event(
            ocean_id=event.ocean_id,
            source="store_router",
            source_id=self.id,
            type=event_type,
            cause=Cause.from_event(event),
            target_runner_id=target_runner_id,
            payload=state,
            exec_id=event.exec_id,
        ))

    def on_store_write(self, event):
        self._set_and_emit(event, False)
    
    def on_force_write(self, event):
        self._set_and_emit(event, True)

    def on_delete(self, event):
        ref = event.payload.ref
        value = self.get_store(ref.store).pop(ref.key)
        if isinstance(value, __fallback__):
            return

        store = self.get_store(ref.store)
        target_runner_id = "*" if store.scope == "global" else event.target_runner_id
        self.consequence_emitter(Event(
            ocean_id=event.ocean_id,
            source="store_router",
            source_id=self.id,
            type=PipelineEvents.DEP_DELETED,
            cause=Cause.from_event(event),
            target_runner_id=target_runner_id,
            payload=Reference(ref),
            exec_id=event.exec_id,
        ))
        
        

    def get(self, store: str, key) -> Any:
        # Read one key from the exact Store.
        value = self.get_store(store).get(key)
        if isinstance(value, __fallback__):
            raise MissingDepKeyError(
                f"STORE[{store}].GET({key}) failed: key was not found"
            )
        return value

    def set(
        self,
        store: str,
        key,
        value: Any,
        force: bool = False,
    ) -> bool:
        if not self.exists(store):
            return __fallback__
        # Write one key and return the Store's update judgement.
        return self.get_store(store).set(key, value, force=force)

    def exists(self, store: str, key=None) -> bool:
        # With no key, report whether the exact Store exists.
        if key is None:
            return self.registry.exists(store)

        # With a key, report whether it exists in the exact Store.
        return self.get_store(store).exists(key)

    def keys(self, store) -> list[str]:
        # Snapshot the exact Store's keys.
        return self.get_store(store).keys()

    def dump(self, store) -> dict[str, Any]:
        # Snapshot the exact Store's values.
        return self.get_store(store).dump()
    
    def delete(self, store, key) -> None:
        # Remove one key when present.
        self.get_store(store).pop(key)

class EventRouter:
    def __init__(self, logger: EventLogger):
        self.logger = logger
        self.subscribers = defaultdict(list)
        self.subscribed_runners = {}
        self.retired_runner_ids = set()

    def unsubscribe(self, event_types, consumer):
        event_types = (event_types,) if isinstance(event_types, StrEnum) else event_types
        for event_type in event_types:
            if consumer in self.subscribers[event_type]:
                self.subscribers[event_type].remove(consumer)

    
    def subscribe(self, event_types, consumer):
        event_types = (event_types,) if isinstance(event_types, StrEnum) else event_types
        for event_type in event_types:
            if consumer not in self.subscribers[event_type]:
                self.subscribers[event_type].append(consumer)

    def route(self, event: Event):
        self.logger.append(event)

        if isinstance(event.type, LogEvents):
            return

        if isinstance(event.type, PipelineEvents):
            target_runner_id = event.target_runner_id

            if target_runner_id is None:
                raise ValueError(f"{event.type} requires target_runner_id")

            if target_runner_id == "*":
                for runner in list(self.subscribed_runners.values()):
                    runner.consume_event(event)
                return

            runner = self.subscribed_runners.get(target_runner_id)
            if runner is None:
                if target_runner_id in self.retired_runner_ids:
                    return
                raise ValueError(f"Runner {target_runner_id!r} is not subscribed")

            runner.consume_event(event)
            return

        consumers = self.subscribers.get(event.type)
        if not consumers:
            raise ValueError(
                f"No Event subscriber is registered for {event.type!r}"
            )

        for consumer in consumers:
            consumer(event)
