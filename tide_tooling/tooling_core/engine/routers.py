from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tide_tooling.tooling_core.engine.PipelineRunner import PipelineRunner

import time  
from threading                    import RLock
from enum                       import StrEnum
from typing                     import DefaultDict, Callable, Any
from collections                import defaultdict, deque

from tide_tooling.tooling_core.engine.utils          import tide_value_hasher, DedupedList, deduped_dict, tide_snapshot_value
from tide_tooling.tooling_core.engine.tide_runtime_logging import EventLogger
from tide_tooling.tooling_core.engine.classes        import Event, DataRef
from tide_tooling.tooling_core.engine.errors         import MissingDepKeyError, StoreReferenceAmbiguityError
from tide_tooling.tooling_core.engine.events         import RouterEvents, PipelineEvents, LogEvents, event_type_is
from tide_tooling.tooling_core.engine.registries     import StoreRegistry
from tide_tooling.tooling_core                       import debug_print as print


# STORE ROUTER 
# 
class StoreRouter:
    _VALID_EVENT_TYPES = ['store_write']
    _router_count = 0
    def __init__(
        self,
        registry: StoreRegistry,
        event_interface,
        store_alias: DefaultDict[str, DedupedList[str]] = None,
        subscribe_events: bool = True,
        deep_copy: bool=True
    ) -> None:
       
        self.id = StoreRouter._router_count
        self.snapshot = deep_copy
        StoreRouter._router_count+=1
        self.registry = registry    
        self.event_queue = deque()
        self._event_lock = RLock()
        self.event_emitter = event_interface.emit_event_to_ocean
        self.stored_opaque_value_snapshot_by_exec_id = {}

        # exec_ids the one event-consuming StoreRouter. Runner-scoped
        # routers share the registry and provide alias resolution/direct store
        # access, but must not receive every RouterEvent as well. Subscribing
        # them caused one undrained duplicate event queue per runner.
        if subscribe_events:
            event_interface.event_subscriber(RouterEvents, self.consume_event)
        self.store_alias = store_alias or deduped_dict()

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
                self.event_handlers[event.event_type](event)

    def get_store(self, store):
        alias = self.store_alias.get(store, store)

        if isinstance(alias, list) or isinstance(alias, DedupedList):
            if len(alias) == 1:
                alias = alias[0]
            else:
                raise StoreReferenceAmbiguityError(
                    f"Store alias {store!r} resolved to multiple stores: {alias}",
                    store,
                )

        return self.registry.get(alias)

    def consume_event(self, event):
        with self._event_lock:
            self.event_queue.appendleft(event)

    def _snapshot_value(self, exec_id, value):
        snapshot = tide_snapshot_value(value, boundary="write")
        if not snapshot.exact:
            self.stored_opaque_value_snapshot_by_exec_id[exec_id] = snapshot
        return snapshot.materialise()

    def on_store_write(self, event, force:bool=False):
        if event.event_type == RouterEvents.STORE_WRITE:
            exec_id = event.exec_id
            runner_id = event.source_id if event.source == "runner" else event.target_runner_id
            store, target, value, producer_node, cause, snapshot= event.payload
            scoped_stores = self.resolve_store_reference(store, runner_id)

            for store in scoped_stores:
                update = self.set(store, target, value, force)
                
                store_obj = self.registry.get(store)
                if store_obj.scope == "global": runner_id = "*"                
                if update:#TODO: add snapshot to dep_updated/available if needed
                    self.event_emitter(Event(source="store_router", source_id=self.id, target_runner_id=runner_id, event_type=PipelineEvents.DEP_UPDATED, payload=(store, target, producer_node, value, cause), exec_id=exec_id))                 
                    
                else:
                    self.event_emitter(Event(source="store_router", source_id=self.id, target_runner_id=runner_id, event_type=PipelineEvents.DEP_AVAILABLE, payload=(store, target, producer_node, value, cause), exec_id=exec_id))
            return
    
    def on_force_write(self, event):
        event.event_type = RouterEvents.STORE_WRITE
        self.on_store_write(event, True)

    def on_delete(self, event):
        if event.event_type == RouterEvents.DELETE:
            runner_id = (
                event.source_id
                if event.source == "runner"
                and event.target_runner_id is None
                else event.target_runner_id
            )
            if len(event.payload) == 6:
                (
                    store,
                    key,
                    source,
                    cause,
                    _,
                    effect_metadata,
                ) = event.payload
            else:
                store, key, source, cause = event.payload
                effect_metadata = None
            scoped_stores = self.resolve_store_reference(store, runner_id)
            exec_id = event.exec_id
            target_existed = False
            for scoped_store in scoped_stores:
                found, _ = self.get_store(scoped_store).pop_if_present(key)
                target_existed = target_existed or found
                
                store_obj = self.registry.get(scoped_store)
                if store_obj.scope == "global": runner_id = "*"             
                if found:
                    self.event_emitter(Event(source="store_router", source_id=self.id, target_runner_id=runner_id, event_type=PipelineEvents.DEP_DELETED, payload=(scoped_store, key, source, cause), exec_id=exec_id))

            if effect_metadata is not None:
                effect_metadata["found"] = target_existed
            return
        
        

    def get(self, store: str, target=None) -> Any:
        t0=time.time()

        if not target:
            raise NotImplementedError("Current dump store behaviour is restricted as not fully handled behaviour")
            if self.exists(store):
                return self.dump(store)
        
        if self.exists(store, target):
            try:
                value = self.get_store(store).get(target)
                
                print(f"STORE[{store}].EXISTS({target}) in [{time.time()-t0}]s")
                return value
            except Exception:
                raise
        

        print(f"[ERROR] STORE[{store}].GET({target}) FAILED. THE KEY WAS NOT FOUND IN THE STORE")
        raise MissingDepKeyError(f"[ERROR] STORE[{store}].GET({target}) FAILED. THE KEY WAS NOT FOUND IN THE STORE")

    # def scoped_set(self, store: str, key: str, value: Any, runner_id: int = None):
    #     scoped_stores = self.resolve_store_reference(store, runner_id)

    #     for scoped_store in scoped_stores:
    #         self.set(scoped_store, key, value)
    #     return
    
    # def scoped_delete(self, store: str, key:str, runner_id:int=None):
    #     scoped_stores = self.resolve_store_reference(store, runner_id)

    #     for scoped_store in scoped_stores: 
    #         print(f"[DELETED] STORE[{scoped_store}][{key}]!")
    #         self.delete(scoped_store, key)
    #     return

    def set(
        self,
        store: str,
        target: DataRef,
        value: Any,
        force: bool = False,
    ) -> bool:
        try:
            update = True
            if self.exists(store, target):
                try:
                    _hash = self.hash(store, target)
                    if tide_value_hasher(value) != _hash:
                        print(f"Different data already exists at {target}, setting updated value!")
                        self.get_store(store).set(target, value)
                        return update
                    else:
                        print(f"Data exists at {target} and matches previous hash. "
                            f"{'Returning dep_available event and Returning!' if not force else 'Force updating from node.recompute!'}")
                        self.get_store(store).set(target, value)
                        return False if not force else update
                except Exception:
                    print(f"Failed to checksum {store, target} value! refreshing dependency!")
                    # self.delete(store, target)
                    self.get_store(store).set(target, value)
                    return update
            self.get_store(store).set(target, value)  
            return False
        except Exception as error:
            print(f"STORE[{store}].SET({target}) FAILED! Error: {error}")
            raise

            

    def exists(self, store: str, target=None) -> bool:
        if not target:
            if self.registry.exists(store):
                return True

            if store in self.store_alias:
                alias = self.store_alias.get(store)
                if isinstance(alias, list) or isinstance(alias, DedupedList):
                    return any(self.registry.exists(item) for item in alias)
                return self.registry.exists(alias)

            return False

        try:
            store = self.get_store(store)
            return store.exists(target)
        except Exception:
            return False

    def keys(self, store) -> list[str]:
        return self.get_store(store).keys()

    def dump(self, store) -> dict[str, Any]:
        store = self.get_store(store)
        dump = store.dump()
        return dump
    
    def delete(self, store, key) -> None:
        if not self.exists(store, key):
            print(f"[DELETE] STORE[{store}][{key}] doesn't exist")
            return 
        self.get_store(store).delete(key)
        print(f"[DELETED] STORE[{store}][{key}]!")
        return 
    
    def resolve_store_reference(self, store, runner_id):
        if store in self.store_alias:
            if runner_id is not None:
                scoped_store = f"{store}_{runner_id}"
                if self.registry.exists(scoped_store):
                    return [scoped_store]

            return list(self.store_alias[store])

        return [store]

    def hash(self, store, target) -> str:
        return tide_value_hasher(self.get(store, target))

class EventRouter:
    def __init__(self, logger: EventLogger):
        self.logger = logger
        self.subscribers = defaultdict(list)
        self.subscribed_runners = {}

    def unsubscribe(self, event_enum: StrEnum, consumer: Callable):
        if isinstance(event_enum, StrEnum):
            if consumer in self.subscribers[event_enum]:
                self.subscribers[event_enum].remove(consumer)
            return

        for event_type in event_enum:
            if consumer in self.subscribers[event_type]:
                self.subscribers[event_type].remove(consumer)

    
    def subscribe(self, event_enum: StrEnum, consumer: Callable):
        if isinstance(event_enum, StrEnum):
            if consumer not in self.subscribers[event_enum]:
                self.subscribers[event_enum].append(consumer)
            return

        for event_type in event_enum:
            if consumer not in self.subscribers[event_type]:
                self.subscribers[event_type].append(consumer)
        return

    def route(self, event: Event):
        self.logger.log_event(event)

        if event_type_is(event.event_type, LogEvents):
            return

        if event_type_is(event.event_type, PipelineEvents):
            target_runner_id = event.target_runner_id

            if target_runner_id is None:
                print(
                    f"[EVENT_ROUTER] Pipeline event {event.event_type} missing "
                    f"target_runner_id | source={event.source} source_id={event.source_id}"
                )
                return

            if target_runner_id == "*":
                for runner in list(self.subscribed_runners.values()):
                    runner.consume_event(event)
                return

            if target_runner_id not in self.subscribed_runners:
                print(
                    f"[EVENT_ROUTER] Failed to route event to Runner "
                    f"{target_runner_id} | Does not exist or is not registered"
                )
                return

            self.subscribed_runners[target_runner_id].consume_event(event)
            return

        consumers = self.subscribers.get(event.event_type)
        if not consumers:
            raise ValueError(
                f"No event subscriber is registered for {event.event_type!r}"
            )

        for consumer in consumers:
            consumer(event)
            
    def does_runner_care(self, runner: PipelineRunner, event):
        #does runner care about event?
        #is it a valid event?
        # if event.target_runner_id != runner.id:
        #     return False 
        if not event_type_is(event.event_type, PipelineEvents):
            return False
        #is it dep_available?
        if event.event_type in [PipelineEvents.DEP_AVAILABLE]:
            store, key, *_ = event.payload 
            store = runner._scope_store_reference(store)
            ref = runner.dep_tools._create_data_ref(store, key)
            
            if ref in runner.runtime_dag.reverse_map:
                return True
        #is it dep_update/delete?
        if event.event_type in [PipelineEvents.DEP_UPDATED, PipelineEvents.DEP_DELETED]:
            #is dep in reverse_map?
            store, key, *_ = event.payload 
            store = runner._scope_store_reference(store)
            ref = runner.dep_tools._create_data_ref(store, key)
            
            if ref in runner.runtime_dag.reverse_map:
                return True
        return False
