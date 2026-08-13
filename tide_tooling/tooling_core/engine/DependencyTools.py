from .errors import DependencyExtractionError, DependencyToolsInitError, CallableDependencyError, NodeResolutionError, PipelineRuntimeError, MissingRefDuringResolutionError, DynamicRefCountLessThanAvailableKeysError
from .index import PipelineIndex 
from .DictProcessor import DictProcessor

import time
from warnings import warn 
from typing import Generator, Callable, Optional, List, Any, Hashable
from .classes import Node, DataRef, RunnerNode
from tide_tooling.tooling_core.engine.routers import StoreRouter
from .errors import MissingDepKeyError
import fnmatch
from tide_tooling.tooling_core.engine.utils import tide_snapshot_value, DedupedList
import re
from tide_tooling.tooling_core import debug_print as print

# init with data_store_dict and data_store_index (pipeline])
class DependencyTools:
    def __init__(self, store_router: StoreRouter) -> None: 

        self.store_router: StoreRouter = store_router
        
        if not store_router or not isinstance(store_router, StoreRouter):
            raise DependencyToolsInitError(f"{self.__class__.__name__}'s store_router is type: {type(store_router)}. Must be a valid StoreRouter obj")

    def exists(self, ref: DataRef) -> bool:    
        available_keys = self._snapshot_available_deps(ref)
        required_count = ref.count if ref.count is not None else 1
        return len(available_keys) >= required_count

    def count(self, ref:DataRef) -> bool:
        available_keys = self._snapshot_available_deps(ref)
        return len(available_keys)

    def get(self, ref: DataRef) -> Any:
        available_keys = self._snapshot_available_deps(ref)
        got = [self.store_router.get(ref.store, key) for key in available_keys]
        if ref.max is not None:
            got = got[:ref.max] 
        if len(got) > 1:
            return got 
        if not got:
            raise MissingRefDuringResolutionError(error_str=f"{ref} was not available at resolution time!", ref=ref, available_keys=available_keys)
        return got[0]
    
    def get_key_refs(self, ref:DataRef, ignore_max=False) -> set[DataRef]:
        available_keys = self._snapshot_available_deps(ref)
        available_refs = self._convert_keys_to_data_ref(ref.store, available_keys)
        return available_refs

    
    def store(self, ref: DataRef, value) -> None:
        key = ref.key
        store = ref.store
        self.store_router.set(store, key, value)
        
    def dump_store(self, store: str) -> Any:
        return self.store_router.dump(store)
    
    def dump_store_keys(self, store: str) -> List[Any]:
        return list(self.dump_store(store).keys())

    def  resolve(self, ref: DataRef) -> List[Any] | Any:
        
        if ref.dynamic: # if any prefix/postfix match to key - dep is available
            store = ref.store
            max = ref.max
            
            available_keys = list(self._snapshot_available_deps(ref))
            
            if max is not None:
                available_keys = available_keys[:max]

            new_refs = [DataRef(key=k, store=store) for k in available_keys]

            return [self.resolve(ref) for ref in new_refs]
        if ref.key:
            return self.get(ref)
        
        raise ValueError(f"invalid DataRef: {ref}")

    # Nested Node detection
    @staticmethod
    def node_extractor(obj: Any):
        if isinstance(obj, Node):
            yield obj
            return
        if isinstance(obj, (list, tuple, set)):
            for v in obj:
                yield from DependencyTools.node_extractor(v)
            return
        if isinstance(obj, dict):
            for v in obj.values():
                yield from DependencyTools.node_extractor(v)
            return
        #add generator support
        return
    
    # Look for and resolve and @name.space.paths | call lambdas.callables. in any list, tuple, or dict. ignore all other values
    def in_place_dependency_resolver(
        self,
        reference_unscoper: Callable,
        node: RunnerNode,
        input_value: Any,
        read_deps: list = None,
        input_path: tuple = (),
        unrepresented: list = None,
        collapse_root: bool = True,
    ) -> Any: # from @name.space.paths 
        """ 
        Takes an object, recursively walk the object and resolve any found dependencies.  
        """
        if read_deps is None:
            # A read is an occurrence, not a set membership. Equal references
            # supplied at two positions must produce two READ receipts.
            read_deps = []
        try:
            def resolve_occurrence(value, path, collapse):
                if DataRef._is_dep_string(value):
                    value = DataRef.from_string(value)

                if isinstance(value, DataRef):
                    if value.dynamic:
                        print(
                            f"[RESOLVER] Resolving DYNAMIC dep_ref: "
                            f"[({value}) ~> {node.dynamic_to_static_deps[value]}"
                        )

                        available_keys = self._snapshot_available_deps(
                            reference_unscoper(value)
                        )
                        required_count = (
                            value.count
                            if value.count is not None
                            else 1
                        )
                        if required_count > len(available_keys):
                            raise DynamicRefCountLessThanAvailableKeysError(
                                f"{value}.count was more than available keys!",
                                value,
                                available_keys,
                            )

                        wanted_refs = list(
                            node.dynamic_to_static_deps[value]
                        )[:value.max]
                        resolved_list = []
                        for occurrence, ref in enumerate(wanted_refs):
                            unscoped_ref = reference_unscoper(ref)
                            resolved = self.resolve(unscoped_ref)
                            resolved_list.append(resolved)
                            read_deps.append((
                                unscoped_ref,
                                resolved,
                                tide_snapshot_value(
                                    resolved,
                                    boundary="read",
                                ),
                                reference_unscoper(value),
                                path + (occurrence,),
                            ))
                        return resolved_list, True

                    print(f"[RESOLVER] Resolving dep_ref: [({value})]")
                    unscoped_ref = reference_unscoper(value)
                    resolved = self.resolve(unscoped_ref)
                    read_deps.append((
                        unscoped_ref,
                        resolved,
                        tide_snapshot_value(resolved, boundary="read"),
                        None,
                        path,
                    ))
                    return resolved, True

                if isinstance(value, (list, tuple, set)):
                    if isinstance(value, set):
                        warn(
                            "[RESOLVER] in_place_dependency_resolver found "
                            "a set as a dep_container - converting to tuple "
                            "as unhashable values may be found!"
                        )
                        value = tuple(value)

                    unnamed_start = (
                        len(unrepresented)
                        if unrepresented is not None
                        else 0
                    )
                    container = []
                    contains_dependency = False
                    for idx, child in enumerate(value):
                        resolved, child_has_dependency = resolve_occurrence(
                            child,
                            path + (idx,),
                            True,
                        )
                        container.append(resolved)
                        contains_dependency = (
                            contains_dependency or child_has_dependency
                        )

                    resolved_container = type(value)(container)
                    if (
                        unrepresented is not None
                        and collapse
                        and not contains_dependency
                    ):
                        del unrepresented[unnamed_start:]
                        unrepresented.append((
                            resolved_container,
                            tide_snapshot_value(
                                resolved_container,
                                boundary="read",
                            ),
                            path,
                        ))
                    return resolved_container, contains_dependency

                if isinstance(value, dict):
                    unnamed_start = (
                        len(unrepresented)
                        if unrepresented is not None
                        else 0
                    )
                    container = {}
                    contains_dependency = False
                    for key, child in value.items():
                        resolved, child_has_dependency = resolve_occurrence(
                            child,
                            path + (key,),
                            True,
                        )
                        container[key] = resolved
                        contains_dependency = (
                            contains_dependency or child_has_dependency
                        )

                    if (
                        unrepresented is not None
                        and collapse
                        and not contains_dependency
                    ):
                        del unrepresented[unnamed_start:]
                        unrepresented.append((
                            container,
                            tide_snapshot_value(
                                container,
                                boundary="read",
                            ),
                            path,
                        ))
                    return container, contains_dependency

                if unrepresented is not None:
                    unrepresented.append((
                        value,
                        tide_snapshot_value(value, boundary="read"),
                        path,
                    ))
                return value, False

            resolved, _ = resolve_occurrence(
                input_value,
                input_path,
                collapse_root,
            )
            return resolved, read_deps
        except (TypeError, ValueError, KeyError) as e:
            raise ValueError(
                f"{self.in_place_dependency_resolver.__name__} error: {e}"
            ) from e

    def _create_data_ref(self, store: str, key: Hashable = None, count:int=None, max=None):
        return DataRef(store=store, key=key, count=count, max=max)

   
    def _convert_keys_to_data_ref(self, store: str, keys: list[str]) -> list[DataRef]:
        converted_refs = list()
        for key in keys:
            new_ref = self._create_data_ref(store=store, key=key) # convert to DataRefs
            converted_refs.append(new_ref)
        return converted_refs
    
    def _snapshot_available_deps(self, ref: DataRef) -> Hashable | list[Hashable] | None :
    
      
        available_keys = self.dump_store_keys(ref.store)

        matched_keys = [k for k in available_keys if fnmatch.fnmatch(k, ref.key)]
        sorted_keys = sorted(reversed(matched_keys), key=lambda ref: ref)
        return sorted_keys

    def _pattern_match(self, ref, pattern):
        return fnmatch.fnmatch(ref, pattern)
        
    def _all_deps_available(self, deps: set[DataRef]) -> bool:
        for dep in deps:
            if not self.exists(dep):
                return False
        return True
         
