from copy import deepcopy

from .TIDEDeserialiser import TIDEDeserialiser
from .TIDECas import TIDECas
from .TIDEOpaqueValue import TIDEOpaqueValue
from .TIDERunDeserialiser import TIDERunDeserialiser
from .TIDERunSerialiser import TIDERunSerialiser
from .TIDESerialiser import TIDESerialiser


DEFAULT_RESOURCE_LIMITS = {
    "max_bundle_file_bytes": 256 * 1024 * 1024,
    "max_cas_records": 100_000,
    "max_cas_record_bytes": 64 * 1024 * 1024,
    "max_cas_total_bytes": 512 * 1024 * 1024,
    "max_values": 100_000,
    "max_exec_records": 250_000,
    "max_ref_versions": 1_000_000,
    "max_manifest_record_refs": 1_000_000,
    "max_value_records": 100_000,
    "max_deserialise_depth": 256,
    "max_container_items": 100_000,
}


class TIDEBundler:
    def __init__(
        self,
        serialiser=None,
        deserialiser=None,
        run_serialiser=None,
        run_deserialiser=None,
        unsupported="opaque",
        trusted=False,
        resource_limits=None,
    ):
        if unsupported not in ["opaque", "strict"]:
            raise ValueError("unsupported must be 'opaque' or 'strict'")
        if not isinstance(trusted, bool):
            raise TypeError("trusted must be a bool")

        self.resource_limits = self._normalise_resource_limits(
            resource_limits
        )
        self.serialiser = serialiser or TIDESerialiser(
            unsupported=unsupported
        )
        self.deserialiser = deserialiser or TIDEDeserialiser(
            trusted=trusted,
            max_depth=self.resource_limits["max_deserialise_depth"],
            max_container_items=self.resource_limits[
                "max_container_items"
            ],
            max_records=self.resource_limits["max_value_records"],
        )
        self.run_serialiser = run_serialiser or TIDERunSerialiser()
        self.run_deserialiser = run_deserialiser or TIDERunDeserialiser()
        self.trusted = trusted

    def bundle(self, value: object, handle=None):
        self.records = {}
        self.object_ids = {}
        self.next_object_index = 0

        return {
            "root": self._add_value(value),
            "records": self.records,
            "metadata": self._get_handle_metadata(handle),
        }

    def deserialise(self, bundle: dict):
        return self.deserialiser.deserialise_bundle(bundle)

    def declare(self, trace_or_values, handle=None):
        from .TIDEBundle import TIDEBundle

        return TIDEBundle.from_trace(
            trace_or_values,
            handle=handle,
            bundler=self,
        )

    def bundle_trace(self, trace_or_values, cas=None, handle=None):
        cas = cas if cas is not None else TIDECas()
        values, ref_versions, trace_metadata = self._get_trace_values(
            trace_or_values,
            cas,
        )
        manifest = {
            "kind": "tide_trace_values",
            "codec_version": 1,
            "value_hash_codec": "tide_serialised_bytes_v1",
            "values": {},
            "ref_index": {},
            "value_index": {},
            "cas_index": {},
            "metadata": self._get_handle_metadata(handle),
            "trace": trace_metadata,
        }

        for value_hash, value in values.items():
            bundle_manifest = cas.store_bundle(self.bundle(value))
            root_cas_ref = bundle_manifest["record_refs"][
                bundle_manifest["root"]
            ]
            manifest["values"][value_hash] = {
                "root_cas_ref": root_cas_ref,
                "bundle": bundle_manifest,
            }
            manifest["value_index"][value_hash] = []
            manifest["cas_index"].setdefault(
                root_cas_ref,
                [],
            ).append(value_hash)

        for (raw_ref, version), value_hash in ref_versions.items():
            if value_hash not in manifest["values"]:
                raise ValueError(
                    "Trace ref/version points to a value missing from "
                    "TraceIndex.value_by_hash: "
                    f"{raw_ref}[v{version}]"
                )

            root_cas_ref = manifest["values"][value_hash]["root_cas_ref"]
            ref_entry = {
                "value_hash": value_hash,
                "root_cas_ref": root_cas_ref,
            }
            manifest["ref_index"].setdefault(
                raw_ref,
                {"versions": {}},
            )["versions"][str(version)] = ref_entry
            manifest["value_index"][value_hash].append(
                {"ref": raw_ref, "version": version}
            )

        if not isinstance(trace_or_values, (list, tuple)):
            manifest["run"] = self.run_serialiser.serialise(
                trace_or_values
            )

        return {
            "manifest": manifest,
            "cas": cas.export_bytes(),
        }

    def load_trace_bundle(self, complete, cas=None):
        if not isinstance(complete, dict):
            raise TypeError("Imported trace bundle must be a dict")

        self.validate_trace_bundle_resources(complete)
        manifest = deepcopy(complete.get("manifest"))
        cas_bytes = complete.get("cas")
        self._validate_trace_manifest(manifest)

        if not isinstance(cas_bytes, dict):
            raise TypeError("Imported trace bundle CAS must be a dict")

        cas = cas if cas is not None else TIDECas()
        cas.import_bytes(cas_bytes)
        values = {}

        for value_hash, value_entry in manifest["values"].items():
            if not isinstance(value_entry, dict):
                raise TypeError("Trace value entry must be a dict")

            bundle_manifest = value_entry.get("bundle")
            root_cas_ref = value_entry.get("root_cas_ref")

            if not isinstance(bundle_manifest, dict):
                raise TypeError("Trace value bundle must be a dict")

            bundle_root = bundle_manifest.get("root")
            record_refs = bundle_manifest.get("record_refs")

            if not isinstance(record_refs, dict):
                raise TypeError("Trace value record_refs must be a dict")

            if record_refs.get(bundle_root) != root_cas_ref:
                raise ValueError(
                    "Trace value root_cas_ref does not match its bundle"
                )

            values[value_hash] = self.deserialise(
                cas.load_bundle(bundle_manifest)
            )

            if isinstance(values[value_hash], TIDEOpaqueValue):
                values[value_hash].value_hash = value_hash

        manifest, values = self._normalise_trace_value_hashes(
            manifest,
            values,
            cas,
        )

        run = manifest.get("run")
        exec_logs = (
            self.run_deserialiser.deserialise(run, values)
            if run is not None
            else []
        )

        return {
            "manifest": manifest,
            "cas": cas,
            "values": values,
            "ref_index": manifest["ref_index"],
            "value_index": manifest["value_index"],
            "cas_index": manifest["cas_index"],
            "run": run,
            "exec_logs": exec_logs,
        }

    def validate_trace_bundle_resources(self, complete):
        """Reject oversized imported structures before copying or decoding."""
        if not isinstance(complete, dict):
            raise TypeError("Imported trace bundle must be a dict")

        manifest = complete.get("manifest")
        cas_bytes = complete.get("cas")

        if isinstance(cas_bytes, dict):
            self._check_resource_limit(
                "CAS records",
                len(cas_bytes),
                "max_cas_records",
            )
            total_bytes = 0
            for content in cas_bytes.values():
                if not isinstance(content, (bytes, bytearray, memoryview)):
                    continue
                content_size = len(content)
                self._check_resource_limit(
                    "CAS record bytes",
                    content_size,
                    "max_cas_record_bytes",
                )
                total_bytes += content_size
                self._check_resource_limit(
                    "CAS total bytes",
                    total_bytes,
                    "max_cas_total_bytes",
                )

        if not isinstance(manifest, dict):
            return

        values = manifest.get("values")
        if isinstance(values, dict):
            self._check_resource_limit(
                "manifest values",
                len(values),
                "max_values",
            )
            total_record_refs = 0
            for value_entry in values.values():
                if not isinstance(value_entry, dict):
                    continue
                bundle = value_entry.get("bundle")
                if not isinstance(bundle, dict):
                    continue
                record_refs = bundle.get("record_refs")
                if not isinstance(record_refs, dict):
                    continue
                self._check_resource_limit(
                    "value records",
                    len(record_refs),
                    "max_value_records",
                )
                total_record_refs += len(record_refs)
                self._check_resource_limit(
                    "manifest record references",
                    total_record_refs,
                    "max_manifest_record_refs",
                )

        ref_index = manifest.get("ref_index")
        if isinstance(ref_index, dict):
            version_count = 0
            for ref_entry in ref_index.values():
                if not isinstance(ref_entry, dict):
                    continue
                versions = ref_entry.get("versions")
                if isinstance(versions, dict):
                    version_count += len(versions)
                    self._check_resource_limit(
                        "reference versions",
                        version_count,
                        "max_ref_versions",
                    )

        run = manifest.get("run")
        if isinstance(run, dict):
            records = run.get("exec_records")
            if isinstance(records, list):
                self._check_resource_limit(
                    "execution records",
                    len(records),
                    "max_exec_records",
                )

    def rebuild_trace(self, loaded):
        if not isinstance(loaded, dict):
            raise TypeError("Loaded trace bundle must be a dict")

        run = loaded.get("run")
        exec_logs = loaded.get("exec_logs")

        if run is None:
            raise ValueError("Loaded trace bundle has no TideRun evidence")

        if not isinstance(exec_logs, list):
            raise TypeError("Loaded trace bundle exec_logs must be a list")

        try:
            from tide_tooling.tooling_core.engine.tide_logging import Trace
        except Exception as error:
            raise ImportError(
                "Trace rebuild requires the TIDE engine runtime"
            ) from error

        trace = run.get("trace", {})
        host_record_value_hash = trace.get("host_record_value_hash")
        host_record = (
            loaded.get("values", {}).get(host_record_value_hash)
            if host_record_value_hash is not None
            else None
        )

        return Trace(
            event_log=[],
            exec_log=exec_logs,
            runner=trace.get("run"),
            ocean_id=trace.get("ocean_id"),
            name=trace.get("name", "TIDE: .trace()"),
            from_cas=True,
            source_context=trace.get("sources"),
            model_context=trace.get("model_context"),
            host=trace.get("host"),
            host_record=host_record,
            bundle_context={
                "kind": loaded["manifest"].get("kind"),
                "codec_version": loaded["manifest"].get("codec_version"),
                "value_count": len(loaded.get("values", {})),
                "opaque_value_hashes": [
                    value_hash
                    for value_hash, value in loaded.get("values", {}).items()
                    if isinstance(value, TIDEOpaqueValue)
                ],
            },
        )

    def _add_value(self, value: object):
        value_identity = id(value)

        if value_identity in self.object_ids:
            return self.object_ids[value_identity]

        object_id = f"object_{self.next_object_index}"
        self.next_object_index += 1
        self.object_ids[value_identity] = object_id
        self.records[object_id] = None
        self.records[object_id] = self.serialiser.serialise_with_refs(
            value,
            self._add_value,
        )
        return object_id

    @staticmethod
    def _get_trace_values(trace_or_values, cas):
        if isinstance(trace_or_values, (list, tuple)):
            return (
                {
                    cas.value_hasher(value): value
                    for value in trace_or_values
                },
                {},
                {},
            )

        index = getattr(trace_or_values, "index", None)

        if index is None:
            raise TypeError(
                "bundle_trace expects a Trace or a list/tuple of values"
            )

        indexed_values = getattr(index, "value_by_hash", None)
        indexed_ref_versions = getattr(
            index,
            "value_hash_by_ref_version",
            None,
        )

        if not isinstance(indexed_values, dict):
            raise TypeError(
                "TraceIndex.value_by_hash must be a dict"
            )

        if not isinstance(indexed_ref_versions, dict):
            raise TypeError(
                "TraceIndex.value_hash_by_ref_version must be a dict"
            )

        values = {
            cas.value_hasher(value): value
            for value in indexed_values.values()
        }
        host_record = getattr(trace_or_values, "host_record", None)
        host_record_value_hash = None
        if host_record is not None:
            host_record_value_hash = cas.value_hasher(host_record)
            values[host_record_value_hash] = host_record
        for record in getattr(trace_or_values, "exec_log", ()) or ():
            output_value = getattr(record, "output_value", None)
            if (
                hasattr(record, "output_value")
                and output_value.__class__.__name__
                != "__TIDE_TEMPLATE_VALUE__"
            ):
                values[cas.value_hasher(output_value)] = output_value
            effects = getattr(record, "effects", None)
            effect_values = (
                effects.values()
                if hasattr(effects, "values")
                else effects or ()
            )
            for effect in effect_values:
                for name in ("output", "preserved_output"):
                    value = getattr(effect, name, None)
                    if value.__class__.__name__ == "__TIDE_TEMPLATE_VALUE__":
                        continue
                    values[cas.value_hasher(value)] = value
        ref_versions = {
            ref_version: cas.value_hasher(indexed_values[value_hash])
            for ref_version, value_hash in indexed_ref_versions.items()
            if value_hash in indexed_values
        }

        missing_ref_values = [
            ref_version
            for ref_version, value_hash in indexed_ref_versions.items()
            if value_hash not in indexed_values
        ]

        if missing_ref_values:
            raw_ref, version = missing_ref_values[0]
            raise ValueError(
                "Trace ref/version points to a value missing from "
                "TraceIndex.value_by_hash: "
                f"{raw_ref}[v{version}]"
            )

        return (
            values,
            ref_versions,
            {
                name: value
                for name, value in {
                    "name": getattr(trace_or_values, "name", None),
                    "ocean_id": getattr(trace_or_values, "ocean_id", None),
                    "run": getattr(trace_or_values, "run", None),
                    "pipeline": getattr(trace_or_values, "pipeline", None),
                    "host_record_value_hash": host_record_value_hash,
                    "sources": (
                        [
                            dict(source)
                            for source in trace_or_values.execution_sources
                        ]
                        if getattr(
                            trace_or_values,
                            "execution_sources",
                            None,
                        )
                        else None
                    ),
                }.items()
                if value is not None
            },
        )

    @staticmethod
    def _validate_trace_manifest(manifest):
        if not isinstance(manifest, dict):
            raise TypeError("Imported trace manifest must be a dict")

        if manifest.get("kind") != "tide_trace_values":
            raise ValueError("Unsupported trace manifest kind")

        if manifest.get("codec_version") != 1:
            raise ValueError("Unsupported trace manifest codec version")

        for key in ["values", "ref_index", "value_index", "cas_index"]:
            if not isinstance(manifest.get(key), dict):
                raise TypeError(f"Trace manifest {key} must be a dict")

        value_hash_codec = manifest.get("value_hash_codec")

        if value_hash_codec not in [None, "tide_serialised_bytes_v1"]:
            raise ValueError("Unsupported trace value hash codec")

    @staticmethod
    def _normalise_trace_value_hashes(manifest, values, cas):
        if manifest.get("value_hash_codec") == "tide_serialised_bytes_v1":
            return manifest, values

        hash_map = {
            value_hash: cas.value_hasher(value)
            for value_hash, value in values.items()
        }
        manifest["values"] = {
            hash_map[value_hash]: value_entry
            for value_hash, value_entry in manifest["values"].items()
        }
        manifest["value_index"] = {
            hash_map[value_hash]: [dict(reference) for reference in references]
            for value_hash, references in manifest["value_index"].items()
        }
        manifest["cas_index"] = {
            cas_ref: [hash_map[value_hash] for value_hash in value_hashes]
            for cas_ref, value_hashes in manifest["cas_index"].items()
        }

        for ref_entry in manifest["ref_index"].values():
            for version_entry in ref_entry["versions"].values():
                value_hash = version_entry["value_hash"]
                version_entry["value_hash"] = hash_map.get(
                    value_hash,
                    value_hash,
                )

        TIDEBundler._normalise_run_value_hashes(
            manifest.get("run"),
            hash_map,
        )
        manifest["value_hash_codec"] = "tide_serialised_bytes_v1"

        return (
            manifest,
            {
                hash_map[value_hash]: value
                for value_hash, value in values.items()
            },
        )

    @staticmethod
    def _normalise_run_value_hashes(run, hash_map):
        if not isinstance(run, dict):
            return

        for record in run.get("exec_records", []):
            for read in record.get("reads", []):
                TIDEBundler._normalise_value_hash_entry(read, hash_map)

                for expanded in read.get("expanded", []):
                    TIDEBundler._normalise_value_hash_entry(
                        expanded,
                        hash_map,
                    )

            for write in record.get("writes", []):
                TIDEBundler._normalise_value_hash_entry(write, hash_map)

            for effect in record.get("effects", []):
                TIDEBundler._normalise_named_value_hash(
                    effect,
                    "output_value_hash",
                    hash_map,
                )
                TIDEBundler._normalise_named_value_hash(
                    effect,
                    "preserved_output_value_hash",
                    hash_map,
                )
            TIDEBundler._normalise_named_value_hash(
                record,
                "output_value_hash",
                hash_map,
            )

    @staticmethod
    def _normalise_value_hash_entry(entry, hash_map):
        if not isinstance(entry, dict):
            return

        value_hash = entry.get("value_hash")

        if value_hash is not None:
            entry["value_hash"] = hash_map.get(value_hash, value_hash)

    @staticmethod
    def _normalise_named_value_hash(entry, name, hash_map):
        if not isinstance(entry, dict):
            return

        value_hash = entry.get(name)
        if value_hash is not None:
            entry[name] = hash_map.get(value_hash, value_hash)

    @staticmethod
    def _get_handle_metadata(handle):
        if isinstance(handle, dict):
            metadata = handle.get("metadata")
        else:
            metadata = getattr(handle, "metadata", None)

        return dict(metadata) if isinstance(metadata, dict) else {}

    @staticmethod
    def _normalise_resource_limits(resource_limits):
        if resource_limits is None:
            return dict(DEFAULT_RESOURCE_LIMITS)
        if not isinstance(resource_limits, dict):
            raise TypeError("resource_limits must be a dict or None")

        unknown = set(resource_limits) - set(DEFAULT_RESOURCE_LIMITS)
        if unknown:
            raise ValueError(
                "Unknown TIDE bundle resource limits: "
                + ", ".join(sorted(unknown))
            )

        limits = dict(DEFAULT_RESOURCE_LIMITS)
        limits.update(resource_limits)
        for name, value in limits.items():
            if not isinstance(value, int) or isinstance(value, bool) or value < 1:
                raise ValueError(
                    f"TIDE bundle resource limit {name} must be a "
                    "positive integer"
                )
        return limits

    def _check_resource_limit(self, label, actual, limit_name):
        maximum = self.resource_limits[limit_name]
        if actual > maximum:
            raise ValueError(
                "TIDE bundle resource limit exceeded: "
                f"{label} {actual} > {maximum} ({limit_name})"
            )
