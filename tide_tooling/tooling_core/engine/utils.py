import inspect
import copy
from dataclasses import dataclass, field

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


import hashlib

@dataclass(frozen=True)
class _FrozenSnapshotDict:
    items: tuple


@dataclass(frozen=True)
class _FrozenSnapshotList:
    items: tuple


@dataclass(frozen=True)
class _FrozenSnapshotTuple:
    items: tuple


@dataclass(frozen=True)
class _FrozenSnapshotSet:
    items: tuple


@dataclass(frozen=True)
class _FrozenSnapshotFrozenSet:
    items: tuple


def _freeze_snapshot_graph(value):
    """Own one immutable copy of an already-serialised value graph."""
    if isinstance(value, dict):
        return _FrozenSnapshotDict(tuple(
            (
                _freeze_snapshot_graph(key),
                _freeze_snapshot_graph(item),
            )
            for key, item in value.items()
        ))
    if isinstance(value, list):
        return _FrozenSnapshotList(tuple(
            _freeze_snapshot_graph(item)
            for item in value
        ))
    if isinstance(value, tuple):
        return _FrozenSnapshotTuple(tuple(
            _freeze_snapshot_graph(item)
            for item in value
        ))
    if isinstance(value, set):
        return _FrozenSnapshotSet(tuple(
            _freeze_snapshot_graph(item)
            for item in value
        ))
    if isinstance(value, frozenset):
        return _FrozenSnapshotFrozenSet(tuple(
            _freeze_snapshot_graph(item)
            for item in value
        ))
    return copy.deepcopy(value)


def _thaw_snapshot_graph(value):
    """Return a caller-owned graph without exposing retained evidence."""
    if isinstance(value, _FrozenSnapshotDict):
        return {
            _thaw_snapshot_graph(key): _thaw_snapshot_graph(item)
            for key, item in value.items
        }
    if isinstance(value, _FrozenSnapshotList):
        return [_thaw_snapshot_graph(item) for item in value.items]
    if isinstance(value, _FrozenSnapshotTuple):
        return tuple(_thaw_snapshot_graph(item) for item in value.items)
    if isinstance(value, _FrozenSnapshotSet):
        return {_thaw_snapshot_graph(item) for item in value.items}
    if isinstance(value, _FrozenSnapshotFrozenSet):
        return frozenset(_thaw_snapshot_graph(item) for item in value.items)
    return copy.deepcopy(value)


@dataclass(frozen=True, init=False)
class TIDEValueSnapshot:
    """Immutable evidence captured at one runtime value boundary.

    ``value_bundle`` is populated only when one serialised graph can both
    establish identity and rebuild the captured value.  A hash-only capture is
    kept as a descriptor and must never be substituted for the runtime value.
    """

    value_hash: str
    _value_bundle: Any = field(default=None, repr=False)
    _value: Any = field(default=None, repr=False)
    capture_method: str = "hash"
    type_name: str = "unknown"
    representation: str = "<snapshot unavailable>"
    boundary: str = "unknown"
    exact: bool = False
    observed_mutable: bool = False
    trusted: bool = True

    class _IntegrityError(ValueError):
        pass

    def __init__(
        self,
        value_hash,
        value_bundle=None,
        value=None,
        capture_method="hash",
        type_name="unknown",
        representation="<snapshot unavailable>",
        boundary="unknown",
        exact=False,
        observed_mutable=False,
        trusted=True,
    ):
        if type(exact) is not bool:
            raise self._IntegrityError("snapshot exact must be bool")
        if type(observed_mutable) is not bool:
            raise self._IntegrityError(
                "snapshot observed_mutable must be bool"
            )
        if type(trusted) is not bool:
            raise self._IntegrityError("snapshot trusted must be bool")
        object.__setattr__(self, "value_hash", value_hash)
        object.__setattr__(
            self,
            "_value_bundle",
            (
                _freeze_snapshot_graph(value_bundle)
                if value_bundle is not None
                else None
            ),
        )
        object.__setattr__(
            self,
            "_value",
            detached_tide_value(value) if value is not None else None,
        )
        object.__setattr__(self, "capture_method", capture_method)
        object.__setattr__(self, "type_name", type_name)
        object.__setattr__(self, "representation", representation)
        object.__setattr__(self, "boundary", boundary)
        object.__setattr__(self, "exact", exact)
        object.__setattr__(self, "observed_mutable", observed_mutable)
        object.__setattr__(self, "trusted", trusted)
        self._validate_integrity()

    def _integrity_error(self, message):
        raise self._IntegrityError(
            "TIDEValueSnapshot integrity failure "
            f"at {self.boundary!r} ({self.capture_method!r}): {message}"
        )

    def _validate_integrity(self):
        if not is_tide_value_hash(self.value_hash):
            self._integrity_error("value_hash is not a valid TIDE value hash")
        if not isinstance(self.type_name, str):
            self._integrity_error("type_name must be text")
        if not isinstance(self.representation, str):
            self._integrity_error("representation must be text")
        if not isinstance(self.boundary, str):
            self._integrity_error("boundary must be text")
        if type(self.exact) is not bool:
            self._integrity_error("exact must be bool")
        if type(self.observed_mutable) is not bool:
            self._integrity_error("observed_mutable must be bool")
        if type(self.trusted) is not bool:
            self._integrity_error("trusted must be bool")

        value_bundle = self.value_bundle
        value = self.value
        if self.capture_method == "serialised":
            if not self.exact or value_bundle is None or value is not None:
                self._integrity_error(
                    "serialised capture requires exact bundle evidence only"
                )
            self._validate_serialised_integrity(value_bundle)
            return
        if self.capture_method == "deepcopy":
            if value_bundle is not None or (self.exact != (value is not None)):
                self._integrity_error(
                    "deepcopy capture exactness must match retained evidence"
                )
            if not self.exact:
                return
            if tide_value_hasher(value) != self.value_hash:
                self._integrity_error(
                    "materialised value does not authenticate value_hash"
                )
            if type(value).__name__ != self.type_name:
                self._integrity_error(
                    "materialised value does not agree with type_name"
                )
            return
        if self.capture_method == "hash":
            if self.exact or value_bundle is not None or value is not None:
                self._integrity_error(
                    "hash capture must contain inexact hash-only evidence"
                )
            return
        self._integrity_error("unsupported capture_method")

    def _validate_serialised_integrity(self, value_bundle):
        from tide_tooling.tooling_core.bundling.TIDEBundler import TIDEBundler
        from tide_tooling.tooling_core.bundling.TIDEOpaqueValue import (
            TIDEOpaqueValue,
        )

        try:
            root_record = value_bundle["records"][value_bundle["root"]]
            restored = TIDEBundler(
                unsupported="strict",
                trusted=self.trusted,
            ).deserialise(copy.deepcopy(value_bundle))
        except Exception as error:
            self._integrity_error(
                f"serialised value evidence cannot be restored: {error}"
            )

        retained_opaque_hash = (
            restored.value_hash
            if root_record.get("serialiser") == "opaque"
            and isinstance(restored, TIDEOpaqueValue)
            and is_tide_value_hash(restored.value_hash)
            else None
        )
        expected_hash = (
            retained_opaque_hash
            or tide_value_hash_from_bundle(value_bundle)
        )
        if expected_hash != self.value_hash:
            self._integrity_error(
                "serialised value evidence does not authenticate value_hash"
            )

        represented_type = (
            restored.type_name
            if isinstance(restored, TIDEOpaqueValue)
            and root_record.get("serialiser") != "opaque"
            else type(restored).__name__
        )
        if represented_type != self.type_name:
            self._integrity_error(
                "serialised value evidence does not agree with type_name"
            )

    @property
    def value_bundle(self):
        if self._value_bundle is None:
            return None
        return _thaw_snapshot_graph(self._value_bundle)

    @property
    def value(self):
        if self._value is None:
            return None
        return detached_tide_value(self._value)

    def _copy_with(self, **changes):
        values = {
            "value_hash": self.value_hash,
            "value_bundle": self.value_bundle,
            "value": self.value,
            "capture_method": self.capture_method,
            "type_name": self.type_name,
            "representation": self.representation,
            "boundary": self.boundary,
            "exact": self.exact,
            "observed_mutable": self.observed_mutable,
            "trusted": self.trusted,
        }
        values.update(changes)
        return type(self)(**values)

    def __deepcopy__(self, memo):
        """Retain the immutable snapshot carrier across evidence ownership copies."""
        memo[id(self)] = self
        return self

    def at_boundary(self, boundary):
        return self._copy_with(boundary=boundary)

    def mark_mutable(self):
        return self._copy_with(observed_mutable=True)

    def _for_persistence(self):
        if self.capture_method != "deepcopy":
            return self
        return type(self)(
            value_hash=self.value_hash,
            capture_method="deepcopy",
            type_name=self.type_name,
            representation=self.representation,
            boundary=self.boundary,
            exact=False,
            observed_mutable=self.observed_mutable,
            trusted=self.trusted,
        )

    def restore(self):
        # Rebuild from the exact graph which established this snapshot's hash.
        if self.exact and self.value_bundle is not None:
            from tide_tooling.tooling_core.bundling.TIDEBundler import TIDEBundler
            from tide_tooling.tooling_core.bundling.TIDEOpaqueValue import TIDEOpaqueValue

            restored = TIDEBundler(
                unsupported="strict",
                trusted=self.trusted,
            ).deserialise(self.value_bundle)
            if isinstance(restored, TIDEOpaqueValue):
                restored.value_hash = self.value_hash
            return restored

        if self.capture_method == "deepcopy" and self.exact:
            # Unsupported internal evidence may not have a lossless generic
            # graph codec.  Retain one isolated capture and detach it again on
            # every read; never expose the retained object itself.
            return detached_tide_value(self.value)

        # Preserve hash-only evidence as an explicit opaque value.
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
            "trusted": self.trusted,
        }


def _safe_snapshot_repr(value):
    try:
        return repr(value)[:1024]
    except Exception:
        return f"<{type(value).__name__} repr unavailable>"


def tide_snapshot_value(value, deep_copy=True, boundary="unknown"):
    """Capture one authoritative value graph or explicit hash-only evidence."""
    type_name = type(value).__name__
    representation = _safe_snapshot_repr(value)

    if deep_copy:
        try:
            from tide_tooling.tooling_core.bundling.TIDEBundler import TIDEBundler
            from tide_tooling.tooling_core.bundling.TIDEDeserialiser import (
                TIDERestorationMismatchError,
            )

            bundler = TIDEBundler(
                unsupported="strict",
                trusted=True,
            )
            value_bundle = bundler.bundle(value)
            from tide_tooling.tooling_core.bundling.TIDEOpaqueValue import (
                TIDEOpaqueValue,
            )
            retained_hash = getattr(value, "value_hash", None)
            value_hash = (
                retained_hash
                if isinstance(value, TIDEOpaqueValue)
                and is_tide_value_hash(retained_hash)
                else tide_value_hash_from_bundle(value_bundle)
            )
            # Trusted reconstruction validates that dictionary state applied
            # through __setstate__ remains the state which was recorded.
            bundler.deserialise(copy.deepcopy(value_bundle))
            return TIDEValueSnapshot(
                value_hash=value_hash,
                value_bundle=value_bundle,
                capture_method="serialised",
                type_name=type_name,
                representation=representation,
                boundary=boundary,
                exact=True,
            )
        except TIDERestorationMismatchError:
            return TIDEValueSnapshot(
                value_hash=value_hash,
                capture_method="hash",
                type_name=type_name,
                representation=representation,
                boundary=boundary,
                exact=False,
            )
        except Exception:
            pass

        # Some internal TIDE structures deliberately contain callables or
        # other values outside the generic persisted-value codec.  A single
        # isolated deepcopy is still exact runtime evidence; keep that capture
        # rather than replacing the whole structure with an opaque marker.
        try:
            captured = copy.deepcopy(value)
            return TIDEValueSnapshot(
                value_hash=tide_value_hasher(captured),
                value=captured,
                capture_method="deepcopy",
                type_name=type_name,
                representation=representation,
                boundary=boundary,
                exact=True,
            )
        except Exception:
            pass

    return TIDEValueSnapshot(
        value_hash=tide_value_hasher(value),
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


def detached_tide_value(value: Any):
    """Return a caller-owned copy without exposing retained evidence."""
    if isinstance(value, memoryview):
        return memoryview(bytes(value))

    try:
        detached = copy.deepcopy(value)
    except Exception as error:
        raise TypeError(
            "Recorded TIDE value cannot be returned as a detached copy: "
            f"{type(value).__name__}"
        ) from error

    if detached is value and not _deeply_immutable_tide_value(value):
        raise TypeError(
            "Recorded TIDE value did not produce an independent copy: "
            f"{type(value).__name__}"
        )
    return detached


def _deeply_immutable_tide_value(value: Any):
    if isinstance(value, type) or inspect.isroutine(value):
        # Classes and functions used as TIDE effect/injection markers are
        # semantic identities.  Python deliberately does not clone them.
        return True
    if value is None or type(value) in {
        bool,
        int,
        float,
        complex,
        str,
        bytes,
        range,
    }:
        return True
    if isinstance(value, tuple):
        return all(_deeply_immutable_tide_value(item) for item in value)
    if isinstance(value, frozenset):
        return all(_deeply_immutable_tide_value(item) for item in value)
    return False


def _tide_hash_bytes(value: bytes):
    return f"sha3_512_tide:{hashlib.sha3_512(value).hexdigest()}"


def is_tide_value_hash(value: Any):
    prefix = "sha3_512_tide:"
    if not isinstance(value, str) or not value.startswith(prefix):
        return False
    digest = value[len(prefix):]
    return len(digest) == 128 and all(
        character in "0123456789abcdef"
        for character in digest
    )


def tide_value_serialised_bytes(value: Any):
    """Return the canonical bytes used for supported TIDE value identity."""
    from tide_tooling.tooling_core.bundling.TIDEBundler import TIDEBundler

    bundle = TIDEBundler().bundle(value)
    return tide_value_bundle_bytes(bundle)


def tide_value_bundle_bytes(bundle: dict):
    """Return canonical identity bytes for one already-serialised value graph."""
    from tide_tooling.tooling_core.bundling.TIDECas import TIDECas

    return TIDECas(value_hasher=_tide_hash_bytes).canonical_bytes(bundle)


def tide_value_hash_from_bundle(bundle: dict):
    """Hash the same value graph which will be retained by a Bundle."""
    return _tide_hash_bytes(tide_value_bundle_bytes(bundle))


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
    from tide_tooling.tooling_core.bundling.TIDEOpaqueValue import (
        TIDEOpaqueValue,
    )

    opaque_hash = getattr(value, "value_hash", None)

    if (
        isinstance(value, TIDEOpaqueValue)
        and is_tide_value_hash(opaque_hash)
    ):
        return opaque_hash

    try:
        return _tide_hash_bytes(tide_value_serialised_bytes(value))
    except (NotImplementedError, TypeError, ValueError) as error:
        from tide_tooling.tooling_core.bundling.TIDESerialiser import (
            TIDEUnorderedValueError,
        )

        if isinstance(error, TIDEUnorderedValueError):
            raise

        value_type = type(value)
        type_name = (
            f"{value_type.__module__}:{value_type.__qualname__}"
        ).encode("utf-8")
        payload = _legacy_tide_value_bytes(value)
        return _tide_hash_bytes(
            b"tide_legacy_value_v1\0"
            + len(type_name).to_bytes(8, "big")
            + type_name
            + payload
        )

def _node_worker(
    node_func,
    resolved_args,
    resolved_kwargs,
    runtime,
):
    from .classes import DataRef, EffectRuntime, _current_effect_runtime

    # Require the execution's typed runtime carrier.
    if not isinstance(runtime, EffectRuntime):
        raise TypeError("_node_worker requires an EffectRuntime")

    # Expose this execution's Effect runtime and default Store.
    runtime_token = _current_effect_runtime.set(runtime)
    store_token = DataRef.set_default_store(runtime.default_store)

    try:
        # Validate ordinary inputs without noWorkflow's injected parameters.
        if not isinstance(resolved_args, (list, tuple)):
            resolved_args = (resolved_args,)

        signature = inspect.signature(node_func)
        signature = signature.replace(parameters=[
            parameter
            for parameter in signature.parameters.values()
            if not parameter.name.startswith("__now_")
        ])

        try:
            signature.bind(*resolved_args, **resolved_kwargs)
        except TypeError as error:
            raise PipelineRuntimeError(
                f"_node_worker input binding failed: {error}"
            ) from error

        # Execute exactly once and return its observed evidence.
        result = node_func(*resolved_args, **resolved_kwargs)
        return result, list(runtime.effects)

    finally:
        DataRef.reset_default_store(store_token)
        _current_effect_runtime.reset(runtime_token)

def _pickle_test(obj):
    import pickle

    try:
        pickle.dumps(obj)
        # print(f"{type(obj)} Pickled SUCCessfully")
        return True
    except Exception:
        # print(f"{type(obj)} PICKLE Failed!: {e}")
        return False
