from .errors import (
    MissingStoreError,
    StoreAlreadyRegisteredError,
    StoreReferenceAmbiguityError,
)
from .stores import StoreTemplate


class StoreRegistry:
    def __init__(self):
        # Keep definitions separate from accessible Store instances.
        self._globals = {}
        self._locals = {}
        self._local_templates = {}

    def register(self, store: StoreTemplate):
        # Accept only the Store contract TIDE can construct and consume.
        if not isinstance(store, StoreTemplate):
            raise TypeError("StoreRegistry requires a StoreTemplate implementation")

        name = store.name

        # A Store name may represent only one registered thing.
        if name in self._globals or name in self._locals or name in self._local_templates:
            raise StoreAlreadyRegisteredError(f"Store {name!r} is already registered", store)

        # Register a local template or actual Runner-local Store.
        if store.scope == "local":
            target = self._local_templates if store.parent_runner_id is None else self._locals
            target[name] = store
            return

        # Register a global Store.
        self._globals[name] = store

    def resolve_runner_store(self, runner_id, possible_alias):
        # Resolve a local alias to the actual Store registered for this Runner.
        runner_store = f"{possible_alias}_{runner_id}"
        return runner_store if runner_store in self._locals else possible_alias

    def resolve_pipeline_store(self, runner_id, possible_actual):
        # Leave global, logical, and unknown Store names unchanged.
        store = self._locals.get(possible_actual)
        suffix = f"_{runner_id}"

        if store is None or store.parent_runner_id != runner_id:
            return possible_actual
        if not possible_actual.endswith(suffix):
            return possible_actual

        # Return the local template name only when it really exists.
        possible_alias = possible_actual[:-len(suffix)]
        return possible_alias if possible_alias in self._local_templates else possible_actual

    def get(self, name):
        # Reject an ambiguous actual Store name.
        if name in self._locals and name in self._globals:
            raise StoreReferenceAmbiguityError(
                f"Store name {name!r} is registered as both local and global", name
            )

        # Return an actual accessible Store.
        if name in self._locals:
            return self._locals[name]
        if name in self._globals:
            return self._globals[name]
        raise MissingStoreError(f"StoreRegistry.GET({name}) failed: Store is not registered")

    def exists(self, name, template=False):
        # Check definitions or accessible Stores explicitly.
        if template:
            return name in self._local_templates
        return name in self._globals or name in self._locals

    def unregister(self, name):
        # Remove this name from whichever registry role owns it.
        self._globals.pop(name, None)
        self._locals.pop(name, None)
        self._local_templates.pop(name, None)
