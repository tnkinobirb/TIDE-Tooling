"""Portable storage for native TIDE Trace evidence and value graphs."""

import base64
import hashlib
import json
import math
import os
from copy import deepcopy
from pathlib import Path

from .TIDEBundler import TIDEBundler
from .TIDECas import TIDECas
from .TIDESigning import validate_signature_record


_JSON_TYPE = "$tide_json_type"


def _bundle_file_path(path):
    if not isinstance(path, (str, os.PathLike)):
        raise TypeError("TIDE bundle path must be a string or path-like object")
    return Path(os.fspath(path)).expanduser()


def _json_safe(value):
    if isinstance(value, bytes):
        return {
            _JSON_TYPE: "bytes",
            "base64": base64.b64encode(value).decode("ascii"),
        }
    if isinstance(value, bytearray):
        return {
            _JSON_TYPE: "bytearray",
            "base64": base64.b64encode(bytes(value)).decode("ascii"),
        }
    if isinstance(value, memoryview):
        return {
            _JSON_TYPE: "memoryview",
            "base64": base64.b64encode(value.tobytes()).decode("ascii"),
        }
    if isinstance(value, tuple):
        return {
            _JSON_TYPE: "tuple",
            "items": [_json_safe(item) for item in value],
        }
    if isinstance(value, set):
        items = [_json_safe(item) for item in value]
        items.sort(
            key=lambda item: json.dumps(
                item,
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        return {_JSON_TYPE: "set", "items": items}
    if isinstance(value, frozenset):
        items = [_json_safe(item) for item in value]
        items.sort(
            key=lambda item: json.dumps(
                item,
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        return {_JSON_TYPE: "frozenset", "items": items}
    if isinstance(value, dict):
        if (
            _JSON_TYPE not in value
            and all(isinstance(key, str) for key in value)
        ):
            return {
                key: _json_safe(item)
                for key, item in value.items()
            }

        items = [
            [_json_safe(key), _json_safe(item)]
            for key, item in value.items()
        ]
        items.sort(
            key=lambda item: json.dumps(
                item[0],
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        return {
            _JSON_TYPE: "dict",
            "items": items,
        }
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError(f"Non-finite TIDE JSON value: {value}")
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    raise TypeError(
        "TIDE Bundle contains a non-portable value: "
        f"{type(value).__name__}"
    )


def _from_json_safe(
    value,
    *,
    max_depth=256,
    max_container_items=100_000,
    max_bytes=512 * 1024 * 1024,
    _depth=0,
    _budget=None,
):
    if _budget is None:
        _budget = {"remaining": max_bytes}

    if _depth > max_depth:
        raise ValueError(
            "TIDE bundle resource limit exceeded: JSON depth"
        )

    if isinstance(value, list):
        if len(value) > max_container_items:
            raise ValueError(
                "TIDE bundle resource limit exceeded: list items"
            )
        return [
            _from_json_safe(
                item,
                max_depth=max_depth,
                max_container_items=max_container_items,
                max_bytes=max_bytes,
                _depth=_depth + 1,
                _budget=_budget,
            )
            for item in value
        ]

    if not isinstance(value, dict):
        if isinstance(value, float) and not math.isfinite(value):
            raise ValueError(f"Non-finite TIDE JSON value: {value}")
        return value

    if len(value) > max_container_items:
        raise ValueError(
            "TIDE bundle resource limit exceeded: object items"
        )

    value_type = value.get(_JSON_TYPE)

    if value_type is None:
        return {
            key: _from_json_safe(
                item,
                max_depth=max_depth,
                max_container_items=max_container_items,
                max_bytes=max_bytes,
                _depth=_depth + 1,
                _budget=_budget,
            )
            for key, item in value.items()
        }

    if value_type in {"bytes", "bytearray", "memoryview"}:
        encoded = value.get("base64")
        if not isinstance(encoded, str):
            raise ValueError("TIDE JSON byte payload must be text")

        payload = base64.b64decode(
            encoded.encode("ascii"),
            validate=True,
        )

        if len(payload) > _budget["remaining"]:
            raise ValueError(
                "TIDE bundle resource limit exceeded: decoded bytes"
            )
        _budget["remaining"] -= len(payload)

        if value_type == "bytearray":
            return bytearray(payload)
        if value_type == "memoryview":
            return memoryview(payload)
        return payload

    if value_type in {"tuple", "set", "frozenset"}:
        items = value.get("items")
        if not isinstance(items, list):
            raise TypeError(
                f"TIDE JSON {value_type} items must be a list"
            )
        if len(items) > max_container_items:
            raise ValueError(
                "TIDE bundle resource limit exceeded: "
                f"JSON {value_type} items {len(items)} "
                f"> {max_container_items}"
            )

        decoded = [
            _from_json_safe(
                item,
                max_depth=max_depth,
                max_container_items=max_container_items,
                max_bytes=max_bytes,
                _depth=_depth + 1,
                _budget=_budget,
            )
            for item in items
        ]

        if value_type == "tuple":
            return tuple(decoded)

        try:
            unique = set(decoded)
        except TypeError as error:
            raise ValueError(
                f"TIDE JSON {value_type} items must be hashable"
            ) from error

        if len(unique) != len(decoded):
            raise ValueError(
                f"TIDE JSON {value_type} contains "
                "duplicate-equivalent values"
            )

        return set(decoded) if value_type == "set" else frozenset(decoded)

    if value_type == "dict":
        items = value.get("items")
        if not isinstance(items, list):
            raise TypeError("TIDE JSON dict items must be a list")
        if len(items) > max_container_items:
            raise ValueError(
                "TIDE bundle resource limit exceeded: "
                f"JSON dict items {len(items)} > {max_container_items}"
            )

        decoded = {}

        for pair in items:
            if not isinstance(pair, list) or len(pair) != 2:
                raise TypeError("TIDE JSON dict item must be a pair")

            key = _from_json_safe(
                pair[0],
                max_depth=max_depth,
                max_container_items=max_container_items,
                max_bytes=max_bytes,
                _depth=_depth + 1,
                _budget=_budget,
            )

            if key in decoded:
                raise ValueError(
                    f"Duplicate decoded TIDE JSON dict key: {key!r}"
                )

            decoded[key] = _from_json_safe(
                pair[1],
                max_depth=max_depth,
                max_container_items=max_container_items,
                max_bytes=max_bytes,
                _depth=_depth + 1,
                _budget=_budget,
            )

        return decoded

    raise ValueError(f"Unknown TIDE JSON value type: {value_type}")


class TIDEBundle:
    """Immutable storage envelope for a native Trace or standalone values."""

    def __init__(self, complete, bundler=None):
        if not isinstance(complete, dict):
            raise TypeError(
                "TIDEBundle complete payload must be a dict"
            )

        self._bundler = bundler or TIDEBundler()
        self._bundler.validate_trace_bundle_resources(complete)
        self._bundler._validate_trace_manifest(
            complete.get("manifest")
        )
        validate_signature_record(complete.get("signature"))

        self._complete = deepcopy(complete)
        self._loaded = None

    @classmethod
    def from_trace(
        cls,
        trace,
        handle=None,
        bundler=None,
        serialise_events=False,
        verify_events=True,
    ):
        if bundler is not None and verify_events is not True:
            raise ValueError(
                "Pass verify_events through the supplied TIDEBundler"
            )
        bundler = bundler or TIDEBundler(verify_events=verify_events)

        return cls(
            bundler.bundle_trace(
                trace,
                handle=handle,
                serialise_events=serialise_events,
            ),
            bundler=bundler,
        )

    @classmethod
    def from_values(cls, values, handle=None, bundler=None):
        if not isinstance(values, (list, tuple)):
            raise TypeError(
                "TIDEBundle values must be a list or tuple"
            )

        bundler = bundler or TIDEBundler()

        return cls(
            bundler.bundle_trace(
                values,
                handle=handle,
            ),
            bundler=bundler,
        )

    @classmethod
    def from_dict(
        cls,
        complete,
        bundler=None,
        trusted=False,
        verify_events=True,
        resource_limits=None,
    ):
        if bundler is not None and (
            resource_limits is not None
            or trusted
            or verify_events is not True
        ):
            raise ValueError(
                "Pass trust/resource options through "
                "the supplied TIDEBundler"
            )

        bundler = bundler or TIDEBundler(
            trusted=trusted,
            verify_events=verify_events,
            resource_limits=resource_limits,
        )
        limits = bundler.resource_limits

        decoded = _from_json_safe(
            complete,
            max_depth=limits["max_deserialise_depth"],
            max_container_items=limits["max_container_items"],
            max_bytes=limits["max_cas_total_bytes"],
        )

        return cls(decoded, bundler=bundler)

    @classmethod
    def from_file(
        cls,
        path,
        bundler=None,
        trusted=False,
        verify_events=True,
        resource_limits=None,
    ):
        path = _bundle_file_path(path)

        if bundler is not None and (
            resource_limits is not None
            or trusted
            or verify_events is not True
        ):
            raise ValueError(
                "Pass trust/resource options through "
                "the supplied TIDEBundler"
            )

        bundler = bundler or TIDEBundler(
            trusted=trusted,
            verify_events=verify_events,
            resource_limits=resource_limits,
        )

        if (
            path.stat().st_size
            > bundler.resource_limits["max_bundle_file_bytes"]
        ):
            raise ValueError(
                "TIDE bundle resource limit exceeded: file bytes"
            )

        with path.open("r", encoding="utf-8") as stream:
            return cls.from_dict(
                json.load(stream),
                bundler=bundler,
            )

    # ------------------------------------------------------------------
    # Stored bundle surface
    # ------------------------------------------------------------------
    @property
    def manifest(self):
        return deepcopy(self._complete["manifest"])

    @property
    def content(self):
        return self._complete["manifest"]["content"]

    @property
    def bundle_hash(self):
        return (
            "sha3_512_tide_bundle:"
            + hashlib.sha3_512(self._canonical_bytes()).hexdigest()
        )

    def _canonical_bytes(self):
        complete = deepcopy(self._complete)
        complete.pop("signature", None)
        return TIDECas().canonical_bytes(_json_safe(complete))

    @property
    def signature(self):
        return deepcopy(self._complete.get("signature"))

    def sign(self, private_key):
        """Return a new Bundle authenticated by ``private_key``."""
        from .TIDESigning import sign_bytes

        complete = self.to_dict()
        complete.pop("signature", None)
        complete["signature"] = sign_bytes(
            self._canonical_bytes(),
            private_key,
        )
        return type(self)(complete, bundler=self._bundler)

    def verify(self, public_key):
        """Verify this Bundle against ``public_key``."""
        from .TIDESigning import verify_bytes

        return verify_bytes(
            self._canonical_bytes(),
            self._complete.get("signature"),
            public_key,
        )

    @property
    def cas_bytes(self):
        return {
            reference: bytes(content)
            for reference, content in self._complete.get(
                "cas",
                {},
            ).items()
        }

    @property
    def event_log(self):
        if self.content != "trace":
            return None
        return self._loaded_state().get("event_log")

    @property
    def events(self):
        event_log = self.event_log
        return event_log.events if event_log is not None else ()

    @property
    def values(self):
        loaded = self._loaded_state()

        if self.content == "values":
            from tide_tooling.tooling_core.engine.utils import (
                detached_tide_value,
                tide_value_hasher,
            )

            return {
                tide_value_hasher(value): detached_tide_value(value)
                for value in loaded["values"]
            }

        from tide_tooling.tooling_core.engine.utils import (
            detached_tide_value,
        )

        return {
            value_hash: detached_tide_value(value)
            for value_hash, value in loaded["values"].items()
        }

    # These remain convenience views over native Trace evidence. They are not
    # separately persisted bundle authorities.
    @property
    def corruption(self):
        return self.trace().corruption

    @property
    def corrupted(self):
        return self.trace().corrupted

    @property
    def divergence(self):
        return self.trace().divergence

    @property
    def divergent(self):
        return self.trace().divergent

    def to_dict(self):
        return deepcopy(self._complete)

    def diff(self, other):
        from tide_tooling.tooling_core.engine.tide_query_results import (
            _build_compared,
        )

        return _build_compared(self, other)

    def show(self, color=True, simple=False, colour=None):
        if colour is not None:
            color = colour

        from tide_tooling.tooling_core.engine.tide_rendering import (
            show_output,
        )

        lines = [
            "",
            "=" * 70,
            f"BUNDLE | {self.content}",
            "=" * 70,
        ]

        if not simple:
            lines.extend([
                f"Hash: {self.bundle_hash}",
                f"Values: {len(self.values)}",
                f"CAS records: {len(self.cas_bytes)}",
            ])

            if self.content == "trace":
                lines.append(
                    "Events: "
                    + (
                        str(len(self.events))
                        if self.event_log is not None
                        else "not serialised"
                    )
                )

        return show_output(lines)

    # ------------------------------------------------------------------
    # Export / import
    # ------------------------------------------------------------------
    def export(self, type="dict", kind="python", path=None):
        if type not in {"dict", "file"}:
            raise ValueError(
                "Bundle export type must be 'dict' or 'file'"
            )
        if kind not in {"python", "json"}:
            raise ValueError(
                "Bundle export kind must be 'python' or 'json'"
            )

        payload = (
            self.to_dict()
            if kind == "python"
            else _json_safe(self.to_dict())
        )

        if type == "dict":
            if path is not None:
                raise ValueError(
                    "A path is only valid for file exports"
                )
            return payload

        if kind != "json" or path is None:
            raise ValueError(
                "File exports require kind='json' and a path"
            )

        path = Path(path)

        with path.open("w", encoding="utf-8") as stream:
            json.dump(
                payload,
                stream,
                ensure_ascii=False,
                indent=2,
                allow_nan=False,
            )
            stream.write("\n")

        return path

    # ------------------------------------------------------------------
    # Hydration / native projections
    # ------------------------------------------------------------------
    def hydrate(self, cas=None):
        if self._loaded is None or cas is not None:
            self._loaded = self._bundler.load_trace_bundle(
                self._complete,
                cas=cas,
                source_bundle_hash=self.bundle_hash,
            )

        return self

    def trace(self):
        """Rebuild this bundle's native Trace evidence."""
        return self._bundler.rebuild_trace(
            self._loaded_state(),
            source_bundle=self,
        )

    def value_of(self, value_hash):
        if not isinstance(value_hash, str):
            raise TypeError(
                "TIDE value hash must be a string"
            )

        try:
            return self.values[value_hash]
        except KeyError as error:
            raise KeyError(
                f"Unknown TIDE value hash: {value_hash}"
            ) from error

    def value_at(self, raw_ref, version=None):
        return self.trace().value_of(
            raw_ref,
            version=version,
        )

    def versions(self, raw_ref):
        versions = {
            version
            for reference, version
            in self.trace().index.value_hash_by_ref_version
            if reference == raw_ref
        }

        if not versions:
            raise KeyError(
                f"Unknown TIDE reference: {raw_ref}"
            )

        return sorted(versions)

    def references_for(self, value_hash):
        return [
            {
                "ref": reference,
                "version": version,
            }
            for (
                reference,
                version,
            ), stored_hash
            in self.trace().index.value_hash_by_ref_version.items()
            if stored_hash == value_hash
        ]

    def _loaded_state(self):
        if self._loaded is None:
            self.hydrate()

        return self._loaded
