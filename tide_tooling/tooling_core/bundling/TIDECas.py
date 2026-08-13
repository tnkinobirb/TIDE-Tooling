import base64
import json
import math

try:
    from tide_tooling.tooling_core.engine.utils import tide_value_hasher
except ImportError:
    tide_value_hasher = None


class TIDECas:
    def __init__(self, value_hasher=None):
        self.value_hasher = value_hasher or tide_value_hasher

        if self.value_hasher is None:
            raise ImportError(
                "TIDECas requires tide_tooling.tooling_core.engine.utils.tide_value_hasher"
            )

        self.records = {}

    def put(self, record: dict):
        if not isinstance(record, dict):
            raise TypeError("CAS records must be dicts")

        content = self._canonical_bytes(record)
        reference = self.value_hasher(content)
        self.records.setdefault(reference, content)
        return reference

    def get(self, reference: str):
        if reference not in self.records:
            raise KeyError(f"Unknown CAS reference: {reference}")

        return self._decode_canonical_bytes(
            self.records[reference]
        )

    def get_bytes(self, reference: str):
        if reference not in self.records:
            raise KeyError(f"Unknown CAS reference: {reference}")

        return bytes(self.records[reference])

    def export_bytes(self):
        return {
            reference: bytes(content)
            for reference, content in self.records.items()
        }

    def import_bytes(self, records: dict):
        if not isinstance(records, dict):
            raise TypeError("CAS byte records must be a dict")

        for reference, content in records.items():
            if not isinstance(reference, str):
                raise TypeError("CAS references must be strings")

            if not isinstance(content, (bytes, bytearray, memoryview)):
                raise TypeError("CAS content must be bytes")

            content = bytes(content)

            if self.value_hasher(content) != reference:
                raise ValueError(
                    f"CAS content does not match reference: {reference}"
                )

            existing = self.records.get(reference)

            if existing is not None and existing != content:
                raise ValueError(
                    f"CAS reference already has different content: {reference}"
                )

            self.records[reference] = content

        return self

    def store_bundle(self, bundle: dict):
        if not isinstance(bundle, dict):
            raise TypeError("Bundle must be a dict")

        records = bundle.get("records")

        if not isinstance(records, dict):
            raise TypeError("Bundle records must be a dict")

        return {
            "root": bundle.get("root"),
            "record_refs": {
                object_id: self.put(record)
                for object_id, record in records.items()
            },
            "metadata": self._normalise_metadata(
                bundle.get("metadata")
            ),
        }

    def load_bundle(self, manifest: dict):
        if not isinstance(manifest, dict):
            raise TypeError("CAS manifest must be a dict")

        record_refs = manifest.get("record_refs")

        if not isinstance(record_refs, dict):
            raise TypeError("CAS manifest record_refs must be a dict")

        return {
            "root": manifest.get("root"),
            "records": {
                object_id: self.get(reference)
                for object_id, reference in record_refs.items()
            },
            "metadata": self._normalise_metadata(
                manifest.get("metadata")
            ),
        }

    def __len__(self):
        return len(self.records)

    def _canonical_bytes(self, value):
        return json.dumps(
            self._canonicalise(value),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")

    def _decode_canonical_bytes(self, content):
        if not isinstance(content, bytes):
            raise TypeError("CAS content must be bytes")

        value = json.loads(
            content.decode("utf-8"),
            parse_constant=self._reject_non_finite_json_value,
        )
        record = self._decanonicalise(value)

        if not isinstance(record, dict):
            raise ValueError("CAS record must decode to a dict")

        return record

    def _canonicalise(self, value):
        if value is None or isinstance(value, (bool, int, str)):
            return value

        if isinstance(value, float):
            if not math.isfinite(value):
                raise ValueError(f"Non-finite CAS value: {value}")

            return value

        if isinstance(value, bytes):
            return {
                "$type": "bytes",
                "base64": base64.b64encode(value).decode("ascii"),
            }

        if isinstance(value, (list, tuple)):
            return [self._canonicalise(item) for item in value]

        if isinstance(value, dict):
            items = [
                [
                    self._canonicalise(key),
                    self._canonicalise(item),
                ]
                for key, item in value.items()
            ]
            items.sort(
                key=lambda item: json.dumps(
                    item[0],
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=False,
                    allow_nan=False,
                )
            )
            return {
                "$type": "dict",
                "items": items,
            }

        raise TypeError(
            f"Unsupported CAS value: {type(value).__name__}"
        )

    def _decanonicalise(self, value):
        if value is None or isinstance(value, (bool, int, str)):
            return value

        if isinstance(value, float):
            if not math.isfinite(value):
                raise ValueError(f"Non-finite CAS value: {value}")

            return value

        if isinstance(value, list):
            return [self._decanonicalise(item) for item in value]

        if not isinstance(value, dict):
            raise ValueError(
                f"Unsupported canonical CAS value: {type(value).__name__}"
            )

        value_type = value.get("$type")

        if value_type == "bytes":
            if set(value) != {"$type", "base64"}:
                raise ValueError("Invalid canonical bytes value")

            encoded = value["base64"]

            if not isinstance(encoded, str):
                raise ValueError("Canonical bytes base64 must be a string")

            try:
                return base64.b64decode(encoded.encode("ascii"), validate=True)
            except (UnicodeEncodeError, ValueError) as error:
                raise ValueError("Invalid canonical bytes base64") from error

        if value_type == "dict":
            if set(value) != {"$type", "items"}:
                raise ValueError("Invalid canonical dict value")

            items = value["items"]

            if not isinstance(items, list):
                raise ValueError("Canonical dict items must be a list")

            decoded = {}

            for item in items:
                if not isinstance(item, list) or len(item) != 2:
                    raise ValueError("Canonical dict item must be a pair")

                key = self._decanonicalise(item[0])
                item_value = self._decanonicalise(item[1])

                try:
                    if key in decoded:
                        raise ValueError(
                            f"Duplicate canonical dict key: {key!r}"
                        )
                    decoded[key] = item_value
                except TypeError as error:
                    raise ValueError(
                        "Canonical dict key must be hashable"
                    ) from error

            return decoded

        raise ValueError(f"Unsupported canonical CAS type: {value_type}")

    @staticmethod
    def _reject_non_finite_json_value(value):
        raise ValueError(f"Non-finite CAS value: {value}")

    @staticmethod
    def _normalise_metadata(metadata):
        return dict(metadata) if isinstance(metadata, dict) else {}
