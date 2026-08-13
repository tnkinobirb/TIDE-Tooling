from functools import lru_cache
import inspect
import copy
import math
from dataclasses import dataclass, replace

from typing import Any
from warnings import warn
from .errors import PipelineRuntimeError
from uuid import uuid4  
from collections import defaultdict

def create_tide_exec_uuid():
    return f"TIDE_EXEC:{uuid4().hex}"

def _is_tide_exec_uuid(input):
    if isinstance(input, str):
        if "TIDE_EXEC:" in input:
            if len("TIDE_EXEC:") + 32 == len(input):
                return True
    return False

class DedupedList(list):
    def append(self, item):
        if item not in self:
            super().append(item)

    def extend(self, items):
        for item in items:
            self.append(item)

    def add(self, item):
        self.append(item)
  
def deduped_dict():
    return defaultdict(DedupedList)


def safe_equal(left, right):
    """Compare evidence values whose equality may return a non-bool object."""
    try:
        return bool(left == right)
    except Exception:
        return repr(left) == repr(right)


def snapshot_json_dict(value, *, location="value"):
    """Validate and isolate one JSON-compatible dictionary.

    This deliberately accepts only the data model shared by JSON itself:
    string-keyed dictionaries, lists, strings, finite numbers, booleans, and
    ``None``. Tuples, sets, arbitrary objects, non-string keys, non-finite
    floats, and recursive containers are rejected instead of being silently
    coerced.
    """
    if not isinstance(value, dict):
        raise TypeError(f"{location} must be a JSON-compatible dict")

    active_containers = set()

    def snapshot(item, path):
        if item is None or isinstance(item, (str, bool, int)):
            return item

        if isinstance(item, float):
            if not math.isfinite(item):
                raise ValueError(f"{path} must contain only finite numbers")
            return item

        if isinstance(item, (dict, list)):
            container_id = id(item)
            if container_id in active_containers:
                raise ValueError(f"{path} must not contain a recursive value")
            active_containers.add(container_id)
            try:
                if isinstance(item, dict):
                    result = {}
                    for key, nested in item.items():
                        if not isinstance(key, str):
                            raise TypeError(
                                f"{path} keys must be strings, got "
                                f"{type(key).__name__}"
                            )
                        result[key] = snapshot(
                            nested,
                            f"{path}[{key!r}]",
                        )
                    return result

                return [
                    snapshot(nested, f"{path}[{index}]")
                    for index, nested in enumerate(item)
                ]
            finally:
                active_containers.remove(container_id)

        raise TypeError(
            f"{path} contains non-JSON value {type(item).__name__}"
        )

    return snapshot(value, location)


import hashlib

@dataclass(frozen=True)
class TIDEValueSnapshot:
    """Immutable evidence captured at one runtime value boundary.

    ``value`` is populated only when TIDE could isolate a rebuildable value.
    A hash-only capture is kept as a descriptor and must never be substituted
    for the runtime value itself.
    """

    value_hash: str
    value: Any = None
    capture_method: str = "hash"
    type_name: str = "unknown"
    representation: str = "<snapshot unavailable>"
    boundary: str = "unknown"
    exact: bool = False
    observed_mutable: bool = False

    def at_boundary(self, boundary):
        return replace(self, boundary=boundary)

    def mark_mutable(self):
        return replace(self, observed_mutable=True)

    def materialise(self):
        if self.exact:
            try:
                return copy.deepcopy(self.value)
            except Exception:
                return self.value

        from tide_tooling.tooling_core.bundling.TIDEOpaqueValue import TIDEOpaqueValue

        return TIDEOpaqueValue(
            type_name=self.type_name,
            representation=self.representation,
            reason="TIDE snapshot retained hash-only evidence",
            value_hash=self.value_hash,
            observed_mutable=self.observed_mutable,
        )

    def to_dict(self):
        return {
            "value_hash": self.value_hash,
            "capture_method": self.capture_method,
            "type": self.type_name,
            "repr": self.representation,
            "boundary": self.boundary,
            "exact": self.exact,
            "observed_mutable": self.observed_mutable,
        }


def _safe_snapshot_repr(value):
    try:
        return repr(value)[:1024]
    except Exception:
        return f"<{type(value).__name__} repr unavailable>"


def tide_snapshot_value(value, deep_copy=True, boundary="unknown"):
    """Capture an isolated value or an explicit hash-only descriptor."""
    value_hash = tide_value_hasher(value)
    type_name = type(value).__name__
    representation = _safe_snapshot_repr(value)

    if deep_copy:
        try:
            return TIDEValueSnapshot(
                value_hash=value_hash,
                value=copy.deepcopy(value),
                capture_method="deepcopy",
                type_name=type_name,
                representation=representation,
                boundary=boundary,
                exact=True,
            )
        except Exception:
            pass

        # Some values expose serialisable state despite rejecting deepcopy.
        try:
            from tide_tooling.tooling_core.bundling.TIDEBundler import TIDEBundler

            # This record was produced from the live in-process value rather
            # than received from an external bundle, so object-state
            # reconstruction is a trusted internal snapshot operation.
            bundler = TIDEBundler(
                unsupported="strict",
                trusted=True,
            )
            rebuilt = bundler.deserialise(bundler.bundle(value))
            return TIDEValueSnapshot(
                value_hash=value_hash,
                value=rebuilt,
                capture_method="serialised",
                type_name=type_name,
                representation=representation,
                boundary=boundary,
                exact=True,
            )
        except Exception:
            pass

    return TIDEValueSnapshot(
        value_hash=value_hash,
        capture_method="hash",
        type_name=type_name,
        representation=representation,
        boundary=boundary,
        exact=False,
    )


def ensure_tide_snapshot(snapshot, boundary="unknown"):
    if isinstance(snapshot, TIDEValueSnapshot):
        if snapshot.boundary == "unknown" and boundary != "unknown":
            return snapshot.at_boundary(boundary)
        return snapshot
    return tide_snapshot_value(snapshot, boundary=boundary)


def _tide_hash_bytes(value: bytes):
    return f"sha3_512_tide:{hashlib.sha3_512(value).hexdigest()}"


def tide_value_serialised_bytes(value: Any):
    """Return the canonical bytes used for supported TIDE value identity."""
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)

    if type(value) in [type(None), bool, int, float, complex, str]:
        from tide_tooling.tooling_core.bundling.TIDEEncoder import TIDEEncoder

        return TIDEEncoder().encode_python_primatives(value)

    from tide_tooling.tooling_core.bundling.TIDEBundler import TIDEBundler
    from tide_tooling.tooling_core.bundling.TIDECas import TIDECas

    bundle = TIDEBundler().bundle(value)
    return TIDECas(value_hasher=_tide_hash_bytes)._canonical_bytes(bundle)


def _legacy_tide_value_bytes(value: Any):
    if isinstance(value, str) and value.startswith("sha3_512_tide:"):
        if len(value) > len("sha3_512_tide:"):
            return None
        else:
            raise ValueError("tide_value_hasher: found a tide hash with no attached key...")

    if value is None:
        return value.__repr__().encode("utf-8")

    if _pickle_test(value):
        if isinstance(value, str):
            value = value.encode("utf-8")
        try:
            return bytes(value)
        except TypeError:
            return repr(value).encode("utf-8")

    return repr(value).encode("utf-8")


def tide_value_hasher(value: Any):
    opaque_hash = getattr(value, "value_hash", None)

    if (
        value.__class__.__name__ == "TIDEOpaqueValue"
        and isinstance(opaque_hash, str)
        and opaque_hash.startswith("sha3_512_tide:")
    ):
        return opaque_hash

    if isinstance(value, str) and value.startswith("sha3_512_tide:"):
        if len(value) > len("sha3_512_tide:"):
            return value

        raise ValueError(
            "tide_value_hasher: found a tide hash with no attached key..."
        )

    try:
        return _tide_hash_bytes(tide_value_serialised_bytes(value))
    except (NotImplementedError, TypeError, ValueError):
        return _tide_hash_bytes(_legacy_tide_value_bytes(value))
# lil func signature cache
@lru_cache(None)
def _func_signature_cacher(fn):
    return inspect.signature(fn) 

def _print_dict_struct(_dict, padding = ""):
    for k, v in _dict.items():
        if isinstance(v, dict):
            print(padding + k + "/")
            _print_dict_struct(v, padding+"    ")
        else:
            print(padding+ k+":", type(v).__name__)


# def _draw_dag_graph(dag_forward_map):
#     dot = Digraph(comment="TIDE DAG")

#     for node, deps in dag_forward_map.items():
#         dot.node(str(node))

#         for dep in deps:
#             dot.edge(str(dep), str(node))

#     dot.render("tide_dag", view=True)





_NODE_WORKER_RESULT = "__TIDE_NODE_WORKER_RESULT_V1__"


def _node_worker(
    node_func,
    resolved_args,
    resolved_kwargs,
    effect_runtime=None,
):

    
    resolved_args = (None, ) if resolved_args is None else resolved_args
    resolved_kwargs = {} if resolved_kwargs is None else resolved_kwargs

    if not isinstance(resolved_args, (tuple, list)):
        resolved_args = (resolved_args,)

    from .classes import DataRef, RAISE, _current_effect_runtime

    token = _current_effect_runtime.set(effect_runtime)
    store_token = None
    if isinstance(effect_runtime, dict):
        default_store = effect_runtime.get("default_store")
        if default_store is not None:
            store_token = DataRef.set_default_store(default_store)
    try:
        # bind inputs to function
        try:
            inspect.signature(node_func).bind(*resolved_args, **resolved_kwargs)
        except TypeError as e: 
           
            raise PipelineRuntimeError(f"{_node_worker.__name__} Lambda function input error - failed to bind inputs [{type(resolved_args), type(resolved_kwargs)} to function!: {e} ") from e
        
        #evaluate callable with inputs - and inject data_store if specified
        result = node_func(*resolved_args, **resolved_kwargs)

        runtime_effects = (
            list(effect_runtime.get("effects", ()))
            if isinstance(effect_runtime, dict)
            else []
        )
        return (_NODE_WORKER_RESULT, result, runtime_effects)
    except RAISE as error:
        observed_effects = (
            list(effect_runtime.get("effects", ()))
            if isinstance(effect_runtime, dict)
            else []
        )
        error._tide_runtime_effects = [
            *observed_effects,
            *error.effects,
        ]
        raise
    except Exception as e:
        # warn(f"func {node_func.__name__} failed! : {e}")
        raise e  # Catch exception - but dont fail hard
    finally:
        if store_token is not None:
            DataRef.reset_default_store(store_token)
        _current_effect_runtime.reset(token)


def _unwrap_node_worker_result(result):
    if (
        isinstance(result, tuple)
        and len(result) == 3
        and result[0] == _NODE_WORKER_RESULT
    ):
        return result[1], list(result[2])
    return result, []

def _pickle_test(obj):
    import pickle

    try:
        pickle.dumps(obj)
        # print(f"{type(obj)} Pickled SUCCessfully")
        return True
    except Exception:
        # print(f"{type(obj)} PICKLE Failed!: {e}")
        return False
