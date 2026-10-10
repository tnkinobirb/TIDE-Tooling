# Deserialise data serialised by TIDESerialiser.

import importlib

from .TIDEEncoder import TIDEEncoder
from .TIDEOpaqueValue import TIDEOpaqueValue


class TIDERestorationMismatchError(ValueError):
    """Recorded object state was changed by its restoration hook."""


class TIDEDeserialiser:
    def __init__(
        self,
        encoder=None,
        trusted=False,
        max_depth=256,
        max_container_items=100_000,
        max_records=100_000,
    ):
        if not isinstance(trusted, bool):
            raise TypeError("trusted must be a bool")
        for name, value in {
            "max_depth": max_depth,
            "max_container_items": max_container_items,
            "max_records": max_records,
        }.items():
            if not isinstance(value, int) or isinstance(value, bool) or value < 1:
                raise ValueError(f"{name} must be a positive integer")

        self.encoder = encoder or TIDEEncoder()
        self.trusted = trusted
        self.max_depth = max_depth
        self.max_container_items = max_container_items
        self.max_records = max_records

    def deserialise(self, record: dict):
        return self._deserialise_value(record, depth=0)

    def deserialise_bundle(self, bundle: dict):
        if not isinstance(bundle, dict):
            raise TypeError("Bundle must be a dict")

        root = bundle.get("root")
        records = bundle.get("records")

        if not isinstance(records, dict):
            raise TypeError("Bundle records must be a dict")
        self._check_limit("value records", len(records), self.max_records)

        return self._deserialise_graph(root, records)

    def _deserialise_graph(self, root, records):
        cache = {}
        allocated = set()
        completed = set()
        building = set()
        populating = set()

        for object_id, record in records.items():
            shell = self._allocate_shell(record)
            if shell is not None:
                cache[object_id] = shell
                allocated.add(object_id)

        def construct(object_id, depth=0):
            self._check_depth(depth)
            if object_id not in records:
                raise KeyError(f"Unknown bundle reference: {object_id}")
            if object_id in allocated or object_id in completed:
                return cache[object_id]
            if object_id in building:
                raise ValueError(
                    "Bundle contains an impossible immutable-only cycle"
                )

            building.add(object_id)
            try:
                value = self._deserialise_record(
                    records[object_id],
                    resolve_ref=lambda reference, reference_depth: construct(
                        reference,
                        reference_depth,
                    ),
                    cache=cache,
                    object_id=object_id,
                    depth=depth,
                )
                cache[object_id] = value
                completed.add(object_id)
                return value
            finally:
                building.remove(object_id)

        for object_id in records:
            if object_id not in allocated:
                construct(object_id)

        def populate(object_id, depth=0):
            self._check_depth(depth)
            if object_id not in records:
                raise KeyError(f"Unknown bundle reference: {object_id}")
            if object_id not in allocated:
                return construct(object_id, depth)
            if object_id in completed or object_id in populating:
                return cache[object_id]

            populating.add(object_id)
            try:
                self._populate_shell(
                    records[object_id],
                    cache[object_id],
                    resolve_ref=lambda reference, reference_depth: populate(
                        reference,
                        reference_depth,
                    ),
                    cache=cache,
                    object_id=object_id,
                    depth=depth,
                )
                completed.add(object_id)
                return cache[object_id]
            finally:
                populating.remove(object_id)

        # Populate object shells before container shells. Hash-based
        # containers may contain immutable tuples which reference those
        # objects; hashing an unpopulated object would use an identity that
        # does not yet exist. Mutable fields may safely retain an allocated
        # container shell until the later container phase.
        population_order = sorted(
            allocated,
            key=lambda object_id: (
                records[object_id].get("serialiser") == "container",
                object_id,
            ),
        )
        for object_id in population_order:
            populate(object_id)

        return populate(root)

    def _allocate_shell(self, record):
        if not isinstance(record, dict):
            raise TypeError("Serialised record must be a dict")

        if record.get("serialiser") == "container":
            container_type = record.get("container_type")
            if container_type == "list":
                return []
            if container_type == "dict":
                return {}
            if container_type == "set":
                return set()

        if record.get("serialiser") == "object_state" and self.trusted:
            self._validate_codec(record, "state")
            object_class = self._resolve_class(record)
            return object_class.__new__(object_class)

        return None

    def _populate_shell(
        self,
        record,
        shell,
        resolve_ref,
        cache,
        object_id,
        depth,
    ):
        serialiser = record.get("serialiser")
        if serialiser == "container":
            return self._populate_container(
                record,
                shell,
                resolve_ref,
                cache,
                depth,
            )
        if serialiser == "object_state" and self.trusted:
            return self._populate_object_state(
                record,
                shell,
                resolve_ref,
                cache,
                depth,
            )
        raise TypeError(f"Record does not support allocation: {serialiser}")

    def _deserialise_value(
        self,
        record: dict,
        resolve_ref=None,
        cache=None,
        object_id=None,
        depth=0,
    ):
        self._check_depth(depth)
        if not isinstance(record, dict):
            raise TypeError("Serialised record must be a dict")

        if "$ref" in record:
            if set(record) != {"$ref"}:
                raise ValueError(
                    "Bundle reference records may only contain $ref"
                )
            if resolve_ref is None:
                raise ValueError("Cannot resolve a bundle reference")

            return resolve_ref(record["$ref"], depth + 1)

        return self._deserialise_record(
            record,
            resolve_ref=resolve_ref,
            cache=cache,
            object_id=object_id,
            depth=depth,
        )

    def _deserialise_record(
        self,
        record: dict,
        resolve_ref=None,
        cache=None,
        object_id=None,
        depth=0,
    ):
        self._check_depth(depth)
        if not isinstance(record, dict):
            raise TypeError("Serialised record must be a dict")

        serialiser = record.get("serialiser")

        if serialiser == "primitive":
            self._validate_codec(record, "json")
            return self.encoder.decode_python_primatives(
                record.get("payload")
            )

        if serialiser == "bytes":
            self._validate_codec(record, "raw")
            return self.encoder.decode_bytes(
                record.get("payload"),
                record.get("type", "bytes"),
            )

        if serialiser == "container":
            return self._deserialise_container(
                record,
                resolve_ref=resolve_ref,
                cache=cache,
                object_id=object_id,
                depth=depth,
            )

        if serialiser == "data_ref":
            self._validate_codec(record, "tide_structure")
            from tide_tooling.tooling_core.engine.classes import DataRef
            return DataRef(
                key=self._deserialise_value(
                    record["key"],
                    resolve_ref=resolve_ref,
                    cache=cache,
                    depth=depth + 1,
                ),
                store=record["store"],
                count=record["count"],
                max=record["max"],
                literal=record["literal"],
                prefix=record["prefix"],
                source_str=record["source_str"],
            )

        if serialiser == "tide_fallback":
            self._validate_codec(record, "tide_structure")
            from tide_tooling.tooling_core.engine.classes import __fallback__
            return __fallback__()

        if serialiser == "path":
            self._validate_codec(record, "path")

            import os
            from pathlib import Path, PurePosixPath, PureWindowsPath

            value = record.get("value")
            flavour = record.get("flavour", "native")

            if not isinstance(value, str):
                raise TypeError("TIDE Path payload must be text")

            if flavour == "windows":
                return (
                    Path(value)
                    if os.name == "nt"
                    else PureWindowsPath(value)
                )

            if flavour == "posix":
                return (
                    Path(value)
                    if os.name != "nt"
                    else PurePosixPath(value)
                )

            if flavour == "native":
                return Path(value)

            raise ValueError(
                f"Unsupported TIDE Path flavour: {flavour}"
            )

        if serialiser == "object_state":
            if not self.trusted:
                self._validate_codec(record, "state")
                module = record.get("module")
                qualname = record.get("qualname")
                type_name = record.get("type") or (
                    qualname.rsplit(".", 1)[-1]
                    if isinstance(qualname, str)
                    else "unknown"
                )
                return TIDEOpaqueValue(
                    type_name=type_name,
                    representation=(
                        f"<{module}.{qualname} recorded object state>"
                        if module and qualname
                        else "<recorded object state>"
                    ),
                    reason=(
                        "object-state reconstruction is disabled for "
                        "untrusted bundles"
                    ),
                    module=module,
                    qualname=qualname,
                )
            return self._deserialise_object_state(
                record,
                resolve_ref=resolve_ref,
                cache=cache,
                object_id=object_id,
                depth=depth,
            )

        if serialiser == "opaque":
            self._validate_codec(record, "descriptor")
            return TIDEOpaqueValue(
                type_name=record.get("type", "unknown"),
                representation=record.get("repr", "<opaque value>"),
                reason=record.get("reason", "unsupported TIDE value"),
                module=record.get("module"),
                qualname=record.get("qualname"),
                value_hash=record.get("value_hash"),
                observed_mutable=record.get("observed_mutable", False),
            )

        raise NotImplementedError(
            f"No deserialiser for strategy: {serialiser}"
        )

    @staticmethod
    def _validate_codec(record: dict, expected_codec: str):
        codec_version = record.get("codec_version")

        if codec_version != 1:
            raise ValueError(
                f"Unsupported codec version: {codec_version}"
            )

        if record.get("codec") != expected_codec:
            raise ValueError(
                f"Unsupported {record.get('serialiser')} codec: "
                f"{record.get('codec')}"
            )

    def _deserialise_container(
        self,
        record: dict,
        resolve_ref=None,
        cache=None,
        object_id=None,
        depth=0,
    ):
        self._check_depth(depth)
        container_type = record.get("container_type")
        items = record.get("items")

        if not isinstance(items, list):
            raise TypeError("Container items must be a list")
        self._check_limit(
            "container items",
            len(items),
            self.max_container_items,
        )

        if container_type == "dict":
            value = {}
            self._cache_value(cache, object_id, value)

            for item in items:
                if not isinstance(item, dict):
                    raise TypeError("Dict items must be key/value records")

                if "key" not in item or "value" not in item:
                    raise ValueError("Dict item must include key and value")

                key = self._deserialise_value(
                    item["key"],
                    resolve_ref=resolve_ref,
                    cache=cache,
                    depth=depth + 1,
                )
                item_value = self._deserialise_value(
                    item["value"],
                    resolve_ref=resolve_ref,
                    cache=cache,
                    depth=depth + 1,
                )
                try:
                    if key in value:
                        raise ValueError(
                            f"Duplicate decoded dict key: {key!r}"
                        )
                    value[key] = item_value
                except TypeError as error:
                    raise ValueError(
                        "Decoded dict key must be hashable"
                    ) from error

            return value

        if container_type == "list":
            value = []
            self._cache_value(cache, object_id, value)
            value.extend(
                self._deserialise_value(
                    item,
                    resolve_ref=resolve_ref,
                    cache=cache,
                    depth=depth + 1,
                )
                for item in items
            )
            return value

        values = [
            self._deserialise_value(
                item,
                resolve_ref=resolve_ref,
                cache=cache,
                depth=depth + 1,
            )
            for item in items
        ]

        if container_type == "tuple":
            return tuple(values)

        if container_type == "set":
            value = set(values)
            self._cache_value(cache, object_id, value)
            return value

        if container_type == "frozenset":
            return frozenset(values)

        raise ValueError(
            f"Unsupported container type: {container_type}"
        )

    def _populate_container(
        self,
        record,
        value,
        resolve_ref,
        cache,
        depth,
    ):
        container_type = record.get("container_type")
        items = record.get("items")
        if not isinstance(items, list):
            raise TypeError("Container items must be a list")
        self._check_limit(
            "container items",
            len(items),
            self.max_container_items,
        )

        if container_type == "list":
            value.extend(
                self._deserialise_value(
                    item,
                    resolve_ref=resolve_ref,
                    cache=cache,
                    depth=depth + 1,
                )
                for item in items
            )
            return value

        if container_type == "set":
            value.update(
                self._deserialise_value(
                    item,
                    resolve_ref=resolve_ref,
                    cache=cache,
                    depth=depth + 1,
                )
                for item in items
            )
            return value

        if container_type != "dict":
            raise ValueError(
                f"Unsupported allocated container type: {container_type}"
            )

        for item in items:
            if not isinstance(item, dict):
                raise TypeError("Dict items must be key/value records")
            if "key" not in item or "value" not in item:
                raise ValueError("Dict item must include key and value")
            key = self._deserialise_value(
                item["key"],
                resolve_ref=resolve_ref,
                cache=cache,
                depth=depth + 1,
            )
            item_value = self._deserialise_value(
                item["value"],
                resolve_ref=resolve_ref,
                cache=cache,
                depth=depth + 1,
            )
            try:
                if key in value:
                    raise ValueError(
                        f"Duplicate decoded dict key: {key!r}"
                    )
                value[key] = item_value
            except TypeError as error:
                raise ValueError(
                    "Decoded dict key must be hashable"
                ) from error
        return value

    def _deserialise_object_state(
        self,
        record: dict,
        resolve_ref=None,
        cache=None,
        object_id=None,
        depth=0,
    ):
        self._check_depth(depth)
        self._validate_codec(record, "state")
        object_class = self._resolve_class(record)
        obj = object_class.__new__(object_class)
        self._cache_value(cache, object_id, obj)

        return self._populate_object_state(
            record,
            obj,
            resolve_ref,
            cache,
            depth,
        )

    def _populate_object_state(
        self,
        record,
        obj,
        resolve_ref,
        cache,
        depth,
    ):
        self._validate_codec(record, "state")

        state = record.get("state")

        if not isinstance(state, dict):
            raise TypeError("Object state must be a dict")

        mode = state.get("mode")

        if mode == "getset":
            set_state = getattr(obj, "__setstate__", None)

            if not callable(set_state):
                raise ValueError("Object state requires __setstate__")

            restored_state = self._deserialise_value(
                state.get("value"),
                resolve_ref=resolve_ref,
                cache=cache,
                depth=depth + 1,
            )
            set_state(restored_state)
            if isinstance(restored_state, dict):
                from tide_tooling.tooling_core.engine.utils import safe_equal

                current_state = getattr(obj, "__dict__", None)
                if not isinstance(current_state, dict) or any(
                    key not in current_state
                    or not safe_equal(current_state[key], recorded)
                    for key, recorded in restored_state.items()
                ):
                    raise TIDERestorationMismatchError(
                        "Object __setstate__ changed recorded state"
                    )
            self._restore_exception_args(record, obj, resolve_ref, cache, depth)
            return obj

        if mode != "dict_slots":
            raise ValueError(f"Unsupported object state mode: {mode}")

        obj_dict = self._deserialise_optional_value(
            state.get("dict"),
            resolve_ref=resolve_ref,
            cache=cache,
            depth=depth + 1,
        )
        obj_slots = self._deserialise_optional_value(
            state.get("slots"),
            resolve_ref=resolve_ref,
            cache=cache,
            depth=depth + 1,
        )

        if obj_dict is not None:
            if not isinstance(obj_dict, dict):
                raise TypeError("Object dict state must rebuild to a dict")

            current_dict = getattr(obj, "__dict__", None)

            if not isinstance(current_dict, dict):
                if obj_dict:
                    raise TypeError("Object does not support __dict__ state")
            else:
                current_dict.update(obj_dict)

        if obj_slots is not None:
            if not isinstance(obj_slots, dict):
                raise TypeError("Object slots state must rebuild to a dict")

            for slot, value in obj_slots.items():
                setattr(obj, slot, value)

        self._restore_exception_args(record, obj, resolve_ref, cache, depth)

        return obj

    def _restore_exception_args(self, record, obj, resolve_ref, cache, depth):
        encoded = record.get("exception_args")
        if encoded is None:
            return
        if not isinstance(obj, BaseException):
            raise TypeError("Only exceptions may carry exception_args")
        args = self._deserialise_value(
            encoded,
            resolve_ref=resolve_ref,
            cache=cache,
            depth=depth + 1,
        )
        if not isinstance(args, tuple):
            raise TypeError("Exception args must rebuild to a tuple")
        obj.args = args

    def _deserialise_optional_value(
        self,
        record,
        resolve_ref=None,
        cache=None,
        depth=0,
    ):
        if record is None:
            return None

        return self._deserialise_value(
            record,
            resolve_ref=resolve_ref,
            cache=cache,
            depth=depth,
        )

    def _check_depth(self, depth):
        if depth > self.max_depth:
            raise ValueError(
                "TIDE bundle resource limit exceeded: "
                f"deserialisation depth {depth} > {self.max_depth}"
            )

    @staticmethod
    def _check_limit(name, actual, maximum):
        if actual > maximum:
            raise ValueError(
                "TIDE bundle resource limit exceeded: "
                f"{name} {actual} > {maximum}"
            )

    @staticmethod
    def _cache_value(cache, object_id, value):
        if cache is not None and object_id is not None:
            cache[object_id] = value

    @staticmethod
    def _resolve_class(record: dict):
        module_name = record.get("module")
        qualname = record.get("qualname")

        if not isinstance(module_name, str) or not isinstance(qualname, str):
            raise TypeError("Object record requires module and qualname")

        if "<locals>" in qualname:
            raise ValueError("Cannot rebuild a local class")

        object_class = importlib.import_module(module_name)

        for name in qualname.split("."):
            object_class = getattr(object_class, name)

        if not isinstance(object_class, type):
            raise TypeError("Object record does not resolve to a class")

        return object_class
