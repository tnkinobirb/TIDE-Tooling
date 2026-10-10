import io
import json
import os
import types
from contextlib import contextmanager
from dataclasses import is_dataclass
from enum import Enum

from .TIDEEncoder import TIDEEncoder
from .TIDEOpaqueValue import TIDEOpaqueValue


class TIDEUnorderedValueError(ValueError):
    pass


# Create a serialiser to handle these objects:
# Python primative objects | Full rebuildability
# File-like objects objects | Full rebuildability
# Arbitrary python objects |
#
# Use __getstate__ | __dict__ /+/ __slots__ -> rebuild with __new__ + __setstate__
# If not __slots__ and not __dict__ rebuild with __new__ and __init__???
#
# Unknown? | Unserialisable
# Opaque | Not directly rebuildable


class TIDESerialiser:
    def __init__(self, encoder=None, unsupported="opaque"):
        if unsupported not in ["opaque", "strict"]:
            raise ValueError("unsupported must be 'opaque' or 'strict'")

        self.encoder = encoder or TIDEEncoder()
        self.unsupported = unsupported
        self.serialisers = {
            "primitive": self._serialise_primitive,
            "bytes": self._serialise_bytes,
            "container": self._serialise_container,
            "data_ref": self._serialise_data_ref,
            "tide_fallback": self._serialise_tide_fallback,
            "object_state": self._serialise_object_state,
            "opaque": self._serialise_opaque,
            "path": self._serialise_path
        }
        self._session_depth = 0
        self._object_state_cache = {}

    @contextmanager
    def graph_session(self):
        """Retain one factual object-state observation per value graph."""
        root_session = self._session_depth == 0
        if root_session:
            self._object_state_cache = {}

        self._session_depth += 1
        try:
            yield
        finally:
            self._session_depth -= 1
            if root_session:
                self._object_state_cache = {}

    def serialise(self, obj: object):
        with self.graph_session():
            return self._serialise(obj, self.serialise)

    def serialise_with_refs(self, obj: object, reference_value):
        if not callable(reference_value):
            raise TypeError("reference_value must be callable")

        return self._serialise(
            obj,
            lambda value: {"$ref": reference_value(value)},
        )

    def _serialise(self, obj: object, serialise_value):
        serialiser = self.choose_serialiser(obj)
        handler = self.serialisers.get(serialiser)

        if (
            serialiser == "opaque"
            and self.unsupported == "strict"
            and not isinstance(obj, TIDEOpaqueValue)
        ):
            raise NotImplementedError(
                f"Unsupported TIDE value: {type(obj).__name__}"
            )

        if handler is None:
            if self.unsupported == "strict":
                raise NotImplementedError(
                    f"No serialiser for strategy: {serialiser}"
                )

            handler = self._serialise_opaque

        try:
            record = handler(obj, serialise_value)
        except TIDEUnorderedValueError:
            raise
        except Exception as error:
            if self.unsupported == "strict":
                raise

            record = self._serialise_opaque(
                obj,
                serialise_value,
                reason=str(error),
            )

        return self._normalise_record(record)

    @staticmethod
    def _normalise_record(record: dict):
        if not isinstance(record, dict):
            raise TypeError("Serialiser handlers must return a dict")

        normalised = dict(record)
        metadata = normalised.get("metadata")
        normalised["metadata"] = (
            dict(metadata)
            if isinstance(metadata, dict)
            else {}
        )
        return normalised

    def _serialise_primitive(self, obj: object, serialise_value):
        return {
            "kind": self.get_value_kind(obj),
            "serialiser": "primitive",
            "codec": "json",
            "codec_version": 1,
            "payload": self.encoder.encode_python_primatives(obj),
        }

    def _serialise_bytes(self, obj: object, serialise_value):
        return {
            "kind": self.get_value_kind(obj),
            "type": type(obj).__name__,
            "serialiser": "bytes",
            "codec": "raw",
            "codec_version": 1,
            "payload": self.encoder.encode_bytes(obj),
        }

    @staticmethod
    def _serialise_data_ref(obj, serialise_value):
        return {
            "kind": "data_ref",
            "serialiser": "data_ref",
            "codec": "tide_structure",
            "codec_version": 1,
            "key": serialise_value(obj.key),
            "store": obj.store,
            "count": obj.count,
            "max": obj.max,
            "literal": obj.literal,
            "prefix": obj.prefix,
            "source_str": obj.source_str,
        }

    @staticmethod
    def _serialise_tide_fallback(obj, serialise_value):
        return {
            "kind": "tide_fallback",
            "serialiser": "tide_fallback",
            "codec": "tide_structure",
            "codec_version": 1,
        }

    @staticmethod
    def _serialise_opaque(obj: object, serialise_value, reason=None):
        if isinstance(obj, TIDEOpaqueValue):
            return obj.to_record()

        object_class = obj.__class__

        try:
            representation = repr(obj)
        except Exception:
            representation = f"<{object_class.__name__} repr unavailable>"

        return {
            "kind": "opaque",
            "serialiser": "opaque",
            "codec": "descriptor",
            "codec_version": 1,
            "type": object_class.__name__,
            "module": getattr(obj, "__module__", object_class.__module__),
            "qualname": getattr(
                obj,
                "__qualname__",
                object_class.__qualname__,
            ),
            "repr": representation[:1024],
            "reason": reason or "unsupported TIDE value",
        }

    @staticmethod
    def _serialise_value(value: object, serialise_value):
        return serialise_value(value)

    def _serialise_container(self, value: object, serialise_value):
        if isinstance(value, dict):
            return self._serialise_dict(value, serialise_value)

        if isinstance(value, list):
            container_type = "list"
        elif isinstance(value, tuple):
            container_type = "tuple"
        elif isinstance(value, set):
            container_type = "set"
        elif isinstance(value, frozenset):
            container_type = "frozenset"
        else:
            raise TypeError(
                f"Unsupported container value: {type(value).__name__}"
            )

        items = list(value)
        if container_type in {"set", "frozenset"}:
            keyed = [
                (self._canonical_order_key(item), item)
                for item in items
            ]
            keys = [key for key, _ in keyed]
            if len(keys) != len(set(keys)):
                raise TIDEUnorderedValueError(
                    "Unordered container contains canonically "
                    "indistinguishable values"
                )
            items = [item for _, item in sorted(keyed, key=lambda pair: pair[0])]

        return {
            "kind": self.get_value_kind(value),
            "serialiser": "container",
            "container_type": container_type,
            "items": [
                self._serialise_value(item, serialise_value)
                for item in items
            ],
        }

    def _serialise_path(self, obj, serialise_value):
        if not isinstance(obj, os.PathLike):
            raise TypeError("Path serialiser requires an os.PathLike value")

        path_text = os.fspath(obj)
        if not isinstance(path_text, str):
            raise TypeError("TIDE Path values must resolve to text")

        class_name = type(obj).__name__
        flavour = (
            "windows"
            if "Windows" in class_name
            else "posix"
            if "Posix" in class_name
            else "native"
        )

        return {
            "kind": "path",
            "serialiser": "path",
            "codec": "path",
            "codec_version": 1,
            "flavour": flavour,
            "value": path_text,
        }

    def _canonical_order_key(self, value):
        canonical = self._canonical_order_value(value, {}, "$root")
        return json.dumps(
            canonical,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )

    def _canonical_order_value(self, value, active, path):
        identity = id(value)
        if identity in active:
            return {"kind": "cycle", "path": active[identity]}

        value_type = type(value)
        type_name = f"{value_type.__module__}:{value_type.__qualname__}"

        if value is None or value_type in {bool, int, float, complex, str}:
            return {
                "kind": "primitive",
                "type": type_name,
                "value": self.encoder.encode_python_primatives(value).hex(),
            }

        if isinstance(value, (bytes, bytearray, memoryview)):
            return {
                "kind": "bytes",
                "type": type_name,
                "value": bytes(value).hex(),
            }

        if isinstance(value, Enum):
            active[identity] = path
            try:
                return {
                    "kind": "enum",
                    "type": type_name,
                    "value": self._canonical_order_value(
                        value.value,
                        active,
                        f"{path}.value",
                    ),
                }
            finally:
                del active[identity]

        if isinstance(value, (list, tuple)):
            active[identity] = path
            try:
                return {
                    "kind": type(value).__name__,
                    "items": [
                        self._canonical_order_value(
                            item,
                            active,
                            f"{path}[{index}]",
                        )
                        for index, item in enumerate(value)
                    ],
                }
            finally:
                del active[identity]

        if isinstance(value, (set, frozenset)):
            active[identity] = path
            try:
                items = [
                    self._canonical_order_value(
                        item,
                        active,
                        f"{path}{{item}}",
                    )
                    for item in value
                ]
                return {
                    "kind": type(value).__name__,
                    "items": sorted(
                        items,
                        key=lambda item: json.dumps(
                            item,
                            sort_keys=True,
                            separators=(",", ":"),
                            ensure_ascii=False,
                        ),
                    ),
                }
            finally:
                del active[identity]

        if isinstance(value, dict):
            active[identity] = path
            try:
                items = [
                    [
                        self._canonical_order_value(
                            key,
                            active,
                            f"{path}.key",
                        ),
                        self._canonical_order_value(
                            item,
                            active,
                            f"{path}.value",
                        ),
                    ]
                    for key, item in value.items()
                ]
                return {
                    "kind": "dict",
                    "items": sorted(
                        items,
                        key=lambda item: json.dumps(
                            item[0],
                            sort_keys=True,
                            separators=(",", ":"),
                            ensure_ascii=False,
                        ),
                    ),
                }
            finally:
                del active[identity]

        if self._is_data_ref(value):
            active[identity] = path
            try:
                return {
                    "kind": "data_ref",
                    "type": type_name,
                    "key": self._canonical_order_value(
                        value.key,
                        active,
                        f"{path}.key",
                    ),
                    "store": value.store,
                    "count": value.count,
                    "max": value.max,
                    "literal": value.literal,
                    "prefix": value.prefix,
                    "source_str": value.source_str,
                }
            finally:
                del active[identity]

        if self.choose_serialiser(value) != "object_state":
            raise TIDEUnorderedValueError(
                "Unordered container contains a non-canonical value: "
                f"{type_name}"
            )

        active[identity] = path
        try:
            state = self.get_object_state(value)
            state_value = (
                state["get_state"]
                if state["get_state"] is not None and state["set_state"]
                else {
                    "dict": state["dict"],
                    "slots": state["slots"],
                }
            )
            return {
                "kind": "object_state",
                "type": type_name,
                "state": self._canonical_order_value(
                    state_value,
                    active,
                    f"{path}.state",
                ),
            }
        finally:
            del active[identity]

    def _serialise_dict(self, value: dict, serialise_value):
        if not isinstance(value, dict):
            raise TypeError(
                "Expected a dict"
            )

        return {
            "kind": "dict",
            "serialiser": "container",
            "container_type": "dict",
            "items": [
                {
                    "key": self._serialise_value(key, serialise_value),
                    "value": self._serialise_value(item, serialise_value),
                }
                for key, item in value.items()
            ],
        }

    def _serialise_object_state(self, obj: object, serialise_value):
        module_info = self.get_module_and_type(obj)
        state = self.get_object_state(obj)

        if state["get_state"] is not None and state["set_state"]:
            state_data = {
                "mode": "getset",
                "value": self._serialise_value(
                    state["get_state"],
                    serialise_value,
                ),
            }

        elif state["has_dict"] or state["has_slots"]:
            state_data = {
                "mode": "dict_slots",
                "dict": (
                    self._serialise_dict(
                        state["dict"],
                        serialise_value,
                    )
                    if state["dict"] is not None
                    else None
                ),
                "slots": (
                    self._serialise_dict(
                        state["slots"],
                        serialise_value,
                    )
                    if state["has_slots"]
                    else None
                ),
            }

        else:
            raise TypeError(
                f"Object cannot expose rebuildable state: {obj!r}"
            )

        return {
            **module_info,
            "kind": "object",
            "serialiser": "object_state",
            "codec": "state",
            "codec_version": 1,
            "state": state_data,
            "exception_args": (
                self._serialise_value(obj.args, serialise_value)
                if isinstance(obj, BaseException)
                else None
            ),
        }

    def _get_byte_value_kind(self, obj: object):
        if isinstance(obj, bytes):
            return "bytes"

        if isinstance(obj, (bytearray, memoryview)):
            return "bytes_like"

        if isinstance(obj, io.TextIOBase):
            return "text_stream"

        if isinstance(obj, (io.BufferedIOBase, io.RawIOBase)):
            return "byte_stream"

        if isinstance(obj, io.IOBase):
            return "stream"

        if any([
            callable(getattr(obj, "read", None)),
            callable(getattr(obj, "write", None)),
        ]):
            return "file_like"

        return "object"

    def get_value_kind(self, obj: object):
        byte_kind = self._get_byte_value_kind(obj)

        if byte_kind != "object":
            return byte_kind

        if obj is None:
            return "none"

        if isinstance(obj, Enum):
            return "enum"

        if isinstance(obj, (bool, int, float, complex, str)):
            return type(obj).__name__

        if isinstance(obj, dict):
            return "dict"

        if isinstance(obj, list):
            return "list"

        if isinstance(obj, tuple):
            return "tuple"

        if isinstance(obj, set):
            return "set"

        if isinstance(obj, frozenset):
            return "frozenset"

        if is_dataclass(obj) and not isinstance(obj, type):
            return "dataclass"

        if isinstance(obj, os.PathLike):
            return "path"

        object_class = obj.__class__
        object_module = object_class.__module__
        object_name = object_class.__name__

        if object_module.startswith("numpy"):
            if object_name in ["ndarray", "matrix", "memmap"]:
                return "numpy_array"

            if hasattr(obj, "dtype"):
                return "numpy_scalar"

            return "numpy_object"

        return "object"

    def choose_serialiser(self, obj: object):
        if isinstance(obj, TIDEOpaqueValue):
            return "opaque"

        from tide_tooling.tooling_core.engine.classes import __fallback__
        if isinstance(obj, __fallback__):
            return "tide_fallback"

        if self._is_data_ref(obj):
            return "data_ref"

        if isinstance(
            obj,
            (
                types.FunctionType,
                types.BuiltinFunctionType,
                types.MethodType,
                types.GeneratorType,
                types.CoroutineType,
                types.AsyncGeneratorType,
                types.ModuleType,
            ),
        ):
            return "opaque"

        value_kind = self.get_value_kind(obj)

        match value_kind:
            case "none" | "bool" | "int" | "float" | "complex" | "str":
                return "primitive"

            case "bytes" | "bytes_like":
                return "bytes"

            case "text_stream":
                return "text_stream"

            case "byte_stream" | "stream" | "file_like":
                return "stream"

            case "dict" | "list" | "tuple" | "set" | "frozenset":
                return "container"

            case "dataclass":
                return "dataclass"

            case "numpy_array":
                return "numpy_array"

            case "numpy_scalar":
                return "numpy_scalar"

            case "path":
                return "path"

            case "enum":
                return "enum"

            case "object":
                return "object_state"

            case _:
                return "opaque"

    @staticmethod
    def _is_data_ref(value):
        try:
            from tide_tooling.tooling_core.engine.classes import DataRef
        except ImportError:
            return False
        return isinstance(value, DataRef)

    @staticmethod
    def get_module_and_type(obj):
        object_class = obj.__class__

        return {
            "module": object_class.__module__,
            "qualname": object_class.__qualname__,
            "name": object_class.__name__,
        }

    def get_object_state(self, obj: object):
        identity = id(obj)
        cached = self._object_state_cache.get(identity)
        if cached is not None:
            original, state = cached
            if original is not obj:
                raise RuntimeError("Serialiser object identity collision")
            return state

        object_class = obj.__class__

        get_state_method = getattr(obj, "__getstate__", None)
        set_state_method = getattr(obj, "__setstate__", None)

        get_state = (
            get_state_method()
            if callable(get_state_method)
            else None
        )

        set_state = callable(set_state_method)

        obj_dict = getattr(obj, "__dict__", None)

        if isinstance(obj_dict, dict):
            obj_dict = dict(obj_dict)

        obj_slots = {}
        slot_names = []

        for class_type in object_class.__mro__:
            slots = class_type.__dict__.get("__slots__", ())

            if isinstance(slots, str):
                slots = (slots,)

            for slot in slots:
                if slot in ["__dict__", "__weakref__"]:
                    continue

                if slot not in slot_names:
                    slot_names.append(slot)

                if hasattr(obj, slot):
                    obj_slots[slot] = getattr(obj, slot)

        module_info = self.get_module_and_type(obj)

        state_data = {
            "kind": self.get_value_kind(obj),
            "has_dict": obj_dict is not None,
            "has_slots": bool(slot_names),
            "set_state": set_state,
            "get_state": get_state,
            "dict": obj_dict,
            "slots": obj_slots,
        }

        state = {
            **module_info,
            **state_data,
        }
        # Keep the original alive so its id cannot be reused, and ensure
        # canonical ordering and retained storage use the same observation.
        self._object_state_cache[identity] = (obj, state)
        return state
