from threading import RLock
from typing import Callable, Any

##Data-Stores
class StoreTemplate:
    reference: str="store_template"
    default_store_init: Callable = dict

    def __init__(self, parent_runner_id=None):
        self.store = self.default_store_init()
        self.scope = "global"
        self.parent_runner_id = parent_runner_id
    def dump(self) -> dict[str, Any]:
        raise NotImplementedError
    
    def get(self, key) -> Any:
        raise NotImplementedError
        
    def set(self, key, value) -> None:
        raise NotImplementedError
    
    def exists(self, key) -> bool:
        raise NotImplementedError

    def keys(self) -> list[str]:
        raise NotImplementedError
    
    def delete(self, key) -> None:
        raise NotImplementedError

    def pop(self, key):
        """Remove and return one value.

        Store implementations may override this to make the operation atomic.
        """
        value = self.get(key)
        self.delete(key)
        return value

    def pop_if_present(self, key):
        """Return ``(found, value)`` while removing ``key`` when present."""
        if not self.exists(key):
            return False, None
        return True, self.pop(key)
    



class MemoryKVStore(StoreTemplate):
    """In-memory key/value store with atomic mapping operations.

    ``get``, ``set``, ``exists``, ``keys``, ``delete``, and ``dump`` are
    serialized per store instance.  The lock protects the mapping operation;
    it does not make a mutable value returned by ``get`` immutable or protect
    direct mutation through a caller-supplied ``store_template`` reference.
    """

    reference = "mem"
    default_store_init: Callable = dict
    def __init__(self, name:str=None, store_template: dict=None, scope="global", parent_runner_id=None, verbose=False) -> None:
        self.name = name or MemoryKVStore.reference
        self.parent_runner_id = parent_runner_id
        self._lock = RLock()
        if store_template is not None:
            self.store = store_template
        else:
            self.store = MemoryKVStore.default_store_init()
        self.scope = scope

        if verbose:
            print("STORE", name, store_template, id(self.store))
        pass

    def dump(self) -> dict[str, Any]:
        with self._lock:
            return dict(self.store)

    def exists(self, name) -> bool:
        with self._lock:
            return name in self.store

    def get(self, key) -> Any:
        with self._lock:
            return self.store[key]

    def set(self, key, value) -> None:
        with self._lock:
            self.store[key] = value
        return

    def keys(self) -> list[str]:
        with self._lock:
            return list(self.store.keys())
    
    def delete(self, key) -> None:
        with self._lock:
            del self.store[key]

    def pop(self, key):
        with self._lock:
            return self.store.pop(key)

    def pop_if_present(self, key):
        with self._lock:
            if key not in self.store:
                return False, None
            return True, self.store.pop(key)
