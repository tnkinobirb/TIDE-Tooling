import fnmatch

from .classes import DataRef
from .errors import MissingRefDuringResolutionError


class DependencyTools:
    def __init__(self, store_router):
        self.store_router = store_router

    @staticmethod
    def matches(key, pattern):
        # Test one concrete key against one dependency pattern.
        return fnmatch.fnmatch(key, "*" if pattern == "!" else pattern)

    def matching_refs(self, ref):
        # Return one exact literal reference when it exists.
        if ref.literal:
            if not self.store_router.exists(ref.store, ref.key):
                return ()

            return (DataRef(
                key=ref.key,
                store=ref.store,
                literal=True,
            ),)

        # Return every pattern match in deterministic order.
        keys = sorted(
            key
            for key in self.store_router.keys(ref.store)
            if self.matches(key, ref.key)
        )
        return tuple(
            DataRef(
                key=key,
                store=ref.store,
                literal=any(
                    token in key
                    for token in "*?["
                ),
            )
            for key in keys
        )

    def is_available(self, ref):
        # Availability means the required number of references exists.
        required = ref.count if ref.count is not None else 1
        return len(self.matching_refs(ref)) >= required

    def resolve_exact(self, ref):
        # Resolve one exact reference or report the observed absence.
        if not self.store_router.exists(ref.store, ref.key):
            raise MissingRefDuringResolutionError(
                error_str=f"{ref} was not available at resolution time",
                ref=ref,
                available_keys=(),
            )
        return self.store_router.get(ref.store, ref.key)
