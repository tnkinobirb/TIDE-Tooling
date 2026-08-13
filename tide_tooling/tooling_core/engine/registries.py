from typing import Any
from tide_tooling.tooling_core.engine.errors import MissingStoreError, StoreReferenceAmbiguityError
from collections import defaultdict
from tide_tooling.tooling_core import debug_print as print

class StoreRegistry:
    # needs to hold instances of stores. register store, get store
    def __init__(self) -> None:
        self._stores = {}
        self._locals = {}
        self._local_templates = {}
        self.__seen_stores__ = list()

    def register(self, data_store_obj) -> None:
        try:
            name = data_store_obj.name

            if data_store_obj.scope == "local":
                self._locals[name] = data_store_obj

                if getattr(data_store_obj, "parent_runner_id", None) is None:
                    self._local_templates[name] = data_store_obj
            else:
                self._stores[name] = data_store_obj

            self.__seen_stores__.append(name)

            print(f"StoreRegistry successfully registered {name}")
        except Exception as e:
            raise e
        return

    def unregister(self, name):
        if self.exists(name):
            if name in self._stores:
                del self._stores[name]
            if name in self._locals:
                del self._locals[name]
            if name in self._local_templates:
                del self._local_templates[name]
            print(f"StoreRegistry successfully unregistered {name}")

    def get(self, name) -> Any:
        if self.exists(name):
            if name in self._locals and name in self._stores:
                raise StoreReferenceAmbiguityError(
                    f"Store name {name!r} is registered as both local and global",
                    name,
                )
            if name in self._locals:
                return self._locals[name]
            else:
                if name in self._stores:
                    return self._stores[name]

        print(f"StoreRegistry.GET({name}) failed! Has the store been registered with StoreRegistry.register?")
        raise MissingStoreError(f"StoreRegistry.GET({name}) failed! Has the store been registered with StoreRegistry.register?")

    def exists(self, name) -> bool:
        if name in self._stores or name in self._locals:
            return True
        return False
