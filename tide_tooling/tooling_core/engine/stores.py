from threading import RLock

from .classes import __TEMPLATE_VAL__
from .utils import tide_value_hasher


class StoreTemplate:
    reference = "store_template"
    store_template = dict

    def __init__(self, name=None, store_template=None, scope="global", parent_runner_id=None):
        # Retain the Store identity and ownership required by TIDE.
        self.name = name or self.reference
        self.parent_runner_id = parent_runner_id

        # Accept only the two Store ownership scopes understood by TIDE.
        if scope not in {"local", "global"}:
            raise ValueError("StoreTemplate.scope must be 'local' or 'global'")
        self.scope = scope

        # Instantiate the Store class's authoritative backing type.
        default_store = self.store_template()
        if store_template is not None and not isinstance(store_template, type(default_store)):
            raise TypeError(
                f"{self.__class__.__name__}.store_template requires "
                f"{type(default_store).__name__}, got {type(store_template).__name__}"
            )

        # Own the backing container while preserving the supplied runtime
        # values themselves. Caller mutation of the seed mapping must never
        # bypass Ocean, Store locking, or Event publication.
        self._store = (
            type(default_store)(store_template)
            if store_template is not None
            else default_store
        )
        self._lock = RLock()

    def dump(self):
        # Return a snapshot rather than the live mapping.
        with self._lock:
            return dict(self._store)

    def get(self, key, default=__TEMPLATE_VAL__):
        # Read one value using ordinary mapping semantics.
        with self._lock:
            return self._store.get(key, default)

    def set(self, key, value, force=False):
        with self._lock:
            # A new reference becomes available.
            if key not in self._store:
                self._store[key] = value
                return False

            # An existing changed or forced reference is updated.
            updated = force or tide_value_hasher(self._store[key]) != tide_value_hasher(value)
            self._store[key] = value
            return updated

    def exists(self, key):
        with self._lock:
            return key in self._store

    def keys(self):
        # Return a snapshot rather than the live key view.
        with self._lock:
            return list(self._store)

    def pop(self, key, default=__TEMPLATE_VAL__):
        # Observe and remove one value as one operation.
        with self._lock:
            return self._store.pop(key, default)


class MemoryKVStore(StoreTemplate):
    reference = "mem"
    store_template = dict
