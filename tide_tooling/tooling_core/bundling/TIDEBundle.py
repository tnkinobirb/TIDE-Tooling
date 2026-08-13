import base64
import json
import math
import os
from copy import deepcopy
from pathlib import Path

from .TIDEBundler import TIDEBundler


_JSON_TYPE = "$tide_json_type"

_WINDOWS_ESCAPE_REPAIRS = {
    "\a": r"\a",
    "\b": r"\b",
    "\t": r"\t",
    "\n": r"\n",
    "\v": r"\v",
    "\f": r"\f",
    "\r": r"\r",
}


def _json_sort_key(value):
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def _reject_non_finite_json_value(value):
    raise ValueError(f"Non-finite TIDE JSON value: {value}")


def _bundle_file_path(path):
    """Normalise supported path objects and recover common Windows escapes."""
    if not isinstance(path, (str, os.PathLike)):
        raise TypeError("TIDE bundle path must be a string or path-like object")

    raw_path = os.fspath(path)
    if not isinstance(raw_path, str):
        raise TypeError("TIDE bundle path must resolve to text, not bytes")

    candidate = Path(raw_path).expanduser()
    if candidate.exists():
        return candidate

    # A non-raw Windows literal such as ``"demos\bug\trace.json"`` reaches
    # Python with control characters in place of ``\b`` and ``\t``. If the
    # literal path does not exist, recover those separators before failing.
    repaired_path = raw_path
    for control_character, escaped_text in _WINDOWS_ESCAPE_REPAIRS.items():
        repaired_path = repaired_path.replace(control_character, escaped_text)

    # Also accept Windows-style separators when a bundle is being inspected
    # on a non-Windows host (for example in CI or after moving a project).
    if os.sep != "\\":
        repaired_path = repaired_path.replace("\\", os.sep)

    if repaired_path != raw_path:
        repaired_candidate = Path(repaired_path).expanduser()
        if repaired_candidate.exists():
            return repaired_candidate
        candidate = repaired_candidate

    return candidate


def _json_safe(value):
    """Encode bundle values that JSON cannot represent natively."""
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
        return {
            _JSON_TYPE: "set",
            "items": sorted(items, key=_json_sort_key),
        }

    if isinstance(value, frozenset):
        items = [_json_safe(item) for item in value]
        return {
            _JSON_TYPE: "frozenset",
            "items": sorted(items, key=_json_sort_key),
        }

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
        return {
            _JSON_TYPE: "dict",
            "items": sorted(
                items,
                key=lambda item: _json_sort_key(item[0]),
            ),
        }

    if isinstance(value, list):
        return [_json_safe(item) for item in value]

    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError(f"Non-finite TIDE JSON value: {value}")

    if value is None or isinstance(value, (bool, int, float, str)):
        return value

    return {
        _JSON_TYPE: "repr",
        "type": type(value).__name__,
        "repr": repr(value)[:1024],
    }


def _from_json_safe(
    value,
    *,
    max_depth=256,
    max_container_items=100_000,
    max_bytes=64 * 1024 * 1024,
    _depth=0,
):
    if _depth > max_depth:
        raise ValueError(
            "TIDE bundle resource limit exceeded: "
            f"JSON depth {_depth} > {max_depth}"
        )

    if isinstance(value, list):
        if len(value) > max_container_items:
            raise ValueError(
                "TIDE bundle resource limit exceeded: "
                f"JSON list items {len(value)} > {max_container_items}"
            )
        return [
            _from_json_safe(
                item,
                max_depth=max_depth,
                max_container_items=max_container_items,
                max_bytes=max_bytes,
                _depth=_depth + 1,
            )
            for item in value
        ]

    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError(f"Non-finite TIDE JSON value: {value}")

    if not isinstance(value, dict):
        return value
    if len(value) > max_container_items:
        raise ValueError(
            "TIDE bundle resource limit exceeded: "
            f"JSON object items {len(value)} > {max_container_items}"
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
            )
            for key, item in value.items()
        }

    if value_type in {"bytes", "bytearray", "memoryview"}:
        try:
            encoded = value["base64"]
            if not isinstance(encoded, str):
                raise ValueError("TIDE JSON byte payload must be text")
            if len(encoded) > ((max_bytes + 2) // 3) * 4:
                raise ValueError(
                    "TIDE bundle resource limit exceeded: "
                    f"encoded byte payload exceeds {max_bytes} bytes"
                )
            payload = base64.b64decode(
                encoded.encode("ascii"),
                validate=True,
            )
        except (KeyError, UnicodeEncodeError, ValueError) as error:
            if "resource limit exceeded" in str(error):
                raise
            raise ValueError("Invalid TIDE JSON byte payload") from error
        if len(payload) > max_bytes:
            raise ValueError(
                "TIDE bundle resource limit exceeded: "
                f"decoded byte payload {len(payload)} > {max_bytes}"
            )

        if value_type == "bytearray":
            return bytearray(payload)
        if value_type == "memoryview":
            return memoryview(payload)
        return payload

    if value_type in {"tuple", "set", "frozenset"}:
        encoded_items = value.get("items")
        if not isinstance(encoded_items, list):
            raise ValueError(
                f"TIDE JSON {value_type} items must be a list"
            )
        items = [
            _from_json_safe(
                item,
                max_depth=max_depth,
                max_container_items=max_container_items,
                max_bytes=max_bytes,
                _depth=_depth + 1,
            )
            for item in encoded_items
        ]
        if value_type == "tuple":
            return tuple(items)
        try:
            if value_type == "set":
                return set(items)
            return frozenset(items)
        except TypeError as error:
            raise ValueError(
                f"TIDE JSON {value_type} items must be hashable"
            ) from error

    if value_type == "dict":
        encoded_items = value.get("items")
        if not isinstance(encoded_items, list):
            raise ValueError("TIDE JSON dict items must be a list")

        decoded = {}
        for item in encoded_items:
            if not isinstance(item, list) or len(item) != 2:
                raise ValueError("TIDE JSON dict item must be a pair")
            key = _from_json_safe(
                item[0],
                max_depth=max_depth,
                max_container_items=max_container_items,
                max_bytes=max_bytes,
                _depth=_depth + 1,
            )
            item_value = _from_json_safe(
                item[1],
                max_depth=max_depth,
                max_container_items=max_container_items,
                max_bytes=max_bytes,
                _depth=_depth + 1,
            )
            try:
                if key in decoded:
                    raise ValueError(
                        f"Duplicate TIDE JSON dict key: {key!r}"
                    )
                decoded[key] = item_value
            except TypeError as error:
                raise ValueError(
                    "TIDE JSON dict key must be hashable"
                ) from error
        return decoded

    if value_type == "repr":
        return {
            "type": value.get("type", "unknown"),
            "repr": value.get("repr", "<unavailable>"),
        }

    raise ValueError(f"Unknown TIDE JSON value type: {value_type}")


class TIDEBundle:
    """Portable TIDE values, reference indexes, CAS bytes and TideRun evidence."""

    def __init__(self, complete: dict, bundler=None):
        if not isinstance(complete, dict):
            raise TypeError("TIDEBundle complete payload must be a dict")

        self._bundler = bundler or TIDEBundler()
        self._bundler.validate_trace_bundle_resources(complete)
        self._complete = deepcopy(complete)
        self._loaded = None

    @classmethod
    def from_trace(cls, trace, handle=None, bundler=None):
        bundler = bundler or TIDEBundler()
        return cls(
            bundler.bundle_trace(trace, handle=handle),
            bundler=bundler,
        )

    @classmethod
    def from_values(cls, values, handle=None, bundler=None):
        if not isinstance(values, (list, tuple)):
            raise TypeError("TIDEBundle values must be a list or tuple")

        return cls.from_trace(values, handle=handle, bundler=bundler)

    @classmethod
    def from_dict(
        cls,
        complete: dict,
        bundler=None,
        trusted=False,
        resource_limits=None,
    ):
        if not isinstance(complete, dict):
            raise TypeError("TIDEBundle payload must be a dict")

        if bundler is not None and resource_limits is not None:
            raise ValueError(
                "Pass resource_limits through the supplied TIDEBundler"
            )
        bundler = bundler or TIDEBundler(
            trusted=trusted,
            resource_limits=resource_limits,
        )
        limits = bundler.resource_limits
        decoded = _from_json_safe(
            complete,
            max_depth=limits["max_deserialise_depth"],
            max_container_items=limits["max_container_items"],
            max_bytes=limits["max_cas_record_bytes"],
        )
        return cls(decoded, bundler=bundler)

    @classmethod
    def from_file(
        cls,
        path,
        bundler=None,
        trusted=False,
        resource_limits=None,
    ):
        """Load a JSON-exported bundle from a filesystem path."""
        path = _bundle_file_path(path)
        if bundler is not None and resource_limits is not None:
            raise ValueError(
                "Pass resource_limits through the supplied TIDEBundler"
            )
        bundler = bundler or TIDEBundler(
            trusted=trusted,
            resource_limits=resource_limits,
        )
        file_size = path.stat().st_size
        maximum = bundler.resource_limits["max_bundle_file_bytes"]
        if file_size > maximum:
            raise ValueError(
                "TIDE bundle resource limit exceeded: "
                f"bundle file bytes {file_size} > {maximum}"
            )
        with path.open("r", encoding="utf-8") as stream:
            return cls.from_dict(
                json.load(
                    stream,
                    parse_constant=_reject_non_finite_json_value,
                ),
                bundler=bundler,
            )

    @property
    def manifest(self):
        return deepcopy(self._complete.get("manifest"))

    @property
    def cas_bytes(self):
        return {
            reference: bytes(content)
            for reference, content in self._complete.get("cas", {}).items()
        }

    @property
    def values(self):
        return dict(self._loaded_state()["values"])

    def to_dict(self):
        return {
            "manifest": self.manifest,
            "cas": self.cas_bytes,
        }

    def diff(self, other):
        from tide_tooling.tooling_core.engine.tide_query_results import _build_compared

        return _build_compared(self, other)

    def show(self, color=True, simple=False, colour=None):
        if colour is not None:
            color = colour
        from tide_tooling.tooling_core.engine.tide_rendering import (
            SHOW_BOLD,
            SHOW_CYAN,
            SHOW_DIM,
            SHOW_RESET,
            SHOW_SECTION_WIDTH,
            show_output,
        )

        if not isinstance(color, bool):
            raise TypeError("TIDEBundle.show color must be a bool")
        if not isinstance(simple, bool):
            raise TypeError("TIDEBundle.show simple must be a bool")

        reset = SHOW_RESET if color else ""
        bold = SHOW_BOLD if color else ""
        dim = SHOW_DIM if color else ""
        cyan = SHOW_CYAN if color else ""
        manifest = self.manifest or {}
        lines = [
            "",
            f"{cyan}{'=' * SHOW_SECTION_WIDTH}{reset}",
            f"{cyan}{bold}BUNDLE{reset} | "
            f"{manifest.get('kind', 'unknown')}",
            f"{dim}{manifest.get('codec_version', 'unknown')} codec{reset}",
            f"{cyan}{'=' * SHOW_SECTION_WIDTH}{reset}",
        ]

        if not simple:
            lines.extend([
                f"{dim}VALUES:{reset} "
                f"{len(manifest.get('values', {}))}",
                f"{dim}REFERENCES:{reset} "
                f"{len(manifest.get('ref_index', {}))}",
                f"{dim}CAS RECORDS:{reset} {len(self.cas_bytes)}",
                f"{dim}METADATA:{reset} "
                f"{repr(manifest.get('metadata', {}))}",
            ])

        return show_output(lines)

    def export(self, type="dict", kind="python", path=None):
        """Export this bundle as a Python payload or JSON file.

        ``kind='python'`` returns the native payload, including CAS bytes.
        ``kind='json'`` returns a JSON-safe dict when ``type='dict'`` or
        writes that payload to ``path`` when ``type='file'``.
        """
        if type not in {"dict", "file"}:
            raise ValueError("Bundle export type must be 'dict' or 'file'")

        if kind not in {"python", "json"}:
            raise ValueError("Bundle export kind must be 'python' or 'json'")

        if kind == "python":
            payload = self.to_dict()
        else:
            payload = _json_safe(self.to_dict())

        if type == "dict":
            if path is not None:
                raise ValueError("A path is only valid for file exports")
            return payload

        if kind != "json":
            raise ValueError("File exports currently require kind='json'")

        if path is None:
            raise ValueError("File exports require a path")

        path = Path(path)
        with path.open("w", encoding="utf-8") as stream:
            json.dump(
                payload,
                stream,
                ensure_ascii=False,
                sort_keys=True,
                indent=2,
                allow_nan=False,
            )
            stream.write("\n")

        return path

    def hydrate(self, cas=None):
        if self._loaded is None or cas is not None:
            self._loaded = self._bundler.load_trace_bundle(
                self._complete,
                cas=cas,
            )

        return self

    def value_of(self, value_hash: str):
        if not isinstance(value_hash, str):
            raise TypeError("TIDE value hash must be a string")

        try:
            return self._loaded_state()["values"][value_hash]
        except KeyError as error:
            raise KeyError(f"Unknown TIDE value hash: {value_hash}") from error

    def value_at(self, raw_ref: str, version=None):
        if not isinstance(raw_ref, str):
            raise TypeError("TIDE raw reference must be a string")

        try:
            versions = self._loaded_state()["ref_index"][raw_ref]["versions"]
        except KeyError as error:
            raise KeyError(f"Unknown TIDE reference: {raw_ref}") from error

        if version is None:
            if len(versions) != 1:
                raise ValueError(
                    f"TIDE reference has multiple versions; provide version: "
                    f"{raw_ref}"
                )

            entry = next(iter(versions.values()))
        else:
            try:
                entry = versions[str(version)]
            except KeyError as error:
                raise KeyError(
                    f"Unknown TIDE reference version: {raw_ref}[v{version}]"
                ) from error

        return self.value_of(entry["value_hash"])

    def versions(self, raw_ref: str):
        if not isinstance(raw_ref, str):
            raise TypeError("TIDE raw reference must be a string")

        try:
            return sorted(
                int(version)
                for version in self._loaded_state()["ref_index"][raw_ref][
                    "versions"
                ]
            )
        except KeyError as error:
            raise KeyError(f"Unknown TIDE reference: {raw_ref}") from error

    def references_for(self, value_hash: str):
        if not isinstance(value_hash, str):
            raise TypeError("TIDE value hash must be a string")

        try:
            return [
                dict(reference)
                for reference in self._loaded_state()["value_index"][value_hash]
            ]
        except KeyError as error:
            raise KeyError(f"Unknown TIDE value hash: {value_hash}") from error

    def trace(self):
        return self._bundler.rebuild_trace(self._loaded_state())

    def _loaded_state(self):
        if self._loaded is None:
            self.hydrate()

        return self._loaded
