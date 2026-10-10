"""Native TIDE Trace bundle encoding.



A Trace bundle contains one native TideRun plus the arbitrary values referenced

by that run. Values are stored through TIDESerialiser/TIDEDeserialiser and

TIDECas. Raw Events are an optional sidecar only; they are never used to

reconstruct the Trace.



Standalone value bundles remain supported, but they do not project a Trace.

"""



from __future__ import annotations



from copy import deepcopy



from .TIDECas import TIDECas

from .TIDEDeserialiser import TIDEDeserialiser

from .TIDEOpaqueValue import TIDEOpaqueValue

from .TIDERunDeserialiser import TIDERunDeserialiser

from .TIDERunSerialiser import TIDERunSerialiser

from .TIDESerialiser import TIDESerialiser

from ..engine.utils import (
    is_tide_value_hash,
    tide_value_hash_from_bundle,
)





DEFAULT_RESOURCE_LIMITS = {

    "max_bundle_file_bytes": 256 * 1024 * 1024,

    "max_values": 100_000,

    "max_events": 1_000_000,

    "max_event_records": 1_000_000,

    "max_exec_records": 250_000,

    "max_ready_attempts": 250_000,

    "max_cas_records": 100_000,

    "max_cas_record_bytes": 64 * 1024 * 1024,

    "max_cas_total_bytes": 512 * 1024 * 1024,

    "max_value_records": 100_000,

    "max_deserialise_depth": 256,

    "max_container_items": 100_000,

}





class TIDEBundler:

    """Encode and decode native Trace storage.



    TideRun is the semantic persistence format for Trace evidence.

    TIDECas stores arbitrary value-graph records referenced by TideRun.

    A FrozenEventLogBundle may be retained as an optional sidecar.

    The Bundle manifest binds that sidecar to its TideRun without making the
    Event log a second reconstruction authority.

    """



    KIND = "tide_bundle"

    CODEC_VERSION = 1

    def __init__(

        self,

        serialiser=None,

        deserialiser=None,

        run_serialiser=None,

        run_deserialiser=None,

        unsupported="opaque",

        trusted=False,

        verify_events=True,

        resource_limits=None,

    ):

        if unsupported not in {"opaque", "strict"}:

            raise ValueError("unsupported must be 'opaque' or 'strict'")

        if not isinstance(trusted, bool):

            raise TypeError("trusted must be a bool")

        if not isinstance(verify_events, bool):

            raise TypeError("verify_events must be a bool")

        self.resource_limits = self._normalise_resource_limits(resource_limits)



        self.serialiser = serialiser or TIDESerialiser(

            unsupported=unsupported,

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

        self.run_deserialiser = (
            run_deserialiser
            or TIDERunDeserialiser(trusted=trusted)
        )

        self.trusted = trusted

        self.verify_events = verify_events



    def _verify_event_correspondence(self, run, run_state, event_log):

        """Compare one optional Event sidecar with its authoritative TideRun."""

        if event_log is None or not self.verify_events:

            return None

        try:
            from tide_tooling.tooling_core.engine.events import (
                LogEvents,
                OceanEvents,
            )
            from tide_tooling.tooling_core.engine.tide_logging import Trace
            from tide_tooling.tooling_core.engine.tide_runtime_logging import (
                trace_from_event_log,
            )
            from types import SimpleNamespace

            trace_meta = run_state["trace"]
            events = event_log.events
            root_event_id = (
                events[0].event_id
                if events
                and events[0].type == OceanEvents.RUN_PIPELINE
                and events[0].source == "ocean"
                and events[0].source_id == events[0].ocean_id
                and events[0].exec_id is None
                else None
            )
            root_runner_id = None
            if root_event_id is not None and trace_meta["runner"] is not None:
                root_runner_id = next((
                    event.source_id
                    for event in events
                    if getattr(event.cause, "event_id", None) == root_event_id
                    and event.type == LogEvents.RUNNER_PLAN
                    and event.source_id is not None
                ), None)

            event_runner = (
                None
                if trace_meta["runner"] is None
                else SimpleNamespace(
                    id=root_runner_id,
                    trace_event_start_id=root_event_id,
                    runner_start_event_id=root_event_id,
                )
            )
            event_trace = trace_from_event_log(
                event_log,
                ocean_id=trace_meta["ocean_id"],
                runner=event_runner,
            )
            authoritative_trace = Trace(
                run_state["records"],
                ocean_id=trace_meta["ocean_id"],
                runner=trace_meta["runner"],
                trace_starting_event=trace_meta["trace_starting_event_id"],
                pipeline_starting_event=(
                    trace_meta["pipeline_starting_event_id"]
                ),
                tide_plan=trace_meta["tide_plan"],
                missing_referenced_events=(
                    trace_meta["missing_referenced_events"]
                ),
                causal_order_violations=(
                    trace_meta["causal_order_violations"]
                ),
                ready_exec_ids=trace_meta["ready_exec_ids"],
                blocked_exec_ids=trace_meta["blocked_exec_ids"],
                deadlock=trace_meta["deadlock"],
                ready_attempts=run_state["ready_attempts"],
                started_runner_ids=trace_meta["started_runner_ids"],
                causal_runner_ids=trace_meta["causal_runner_ids"],
                ended_runner_ids=trace_meta["ended_runner_ids"],
                evidence_violations=trace_meta["evidence_violations"],
                _state_authorities=run_state["state_authorities"],
                _require_closed_evidence=True,
            )

            return authoritative_trace.diff(event_trace).same

        except Exception:

            return False



    # ------------------------------------------------------------------

    # Arbitrary value graphs

    # ------------------------------------------------------------------

    def bundle(self, value, handle=None):

        """Encode one arbitrary Python value graph."""

        self.records = {}

        self.object_ids = {}

        self.next_object_index = 0



        with self.serialiser.graph_session():

            return {

                "root": self._add_value(value),

                "records": self.records,

                "metadata": self._get_handle_metadata(handle),

            }



    def deserialise(self, bundle):

        """Decode one arbitrary Python value graph."""

        return self.deserialiser.deserialise_bundle(bundle)



    def _add_value(self, value):

        identity = id(value)

        cached = self.object_ids.get(identity)



        if cached is not None:

            original, object_id = cached

            if original is not value:

                raise RuntimeError("Bundle object identity collision")

            return object_id



        object_id = f"object_{self.next_object_index}"

        self.next_object_index += 1

        self.object_ids[identity] = (value, object_id)



        # Install the identity before descending so cycles can refer back to it.

        self.records[object_id] = None

        self.records[object_id] = self.serialiser.serialise_with_refs(

            value,

            self._add_value,

        )

        return object_id



    # ------------------------------------------------------------------

    # Public bundle construction

    # ------------------------------------------------------------------

    def bundle_trace(

        self,

        trace_or_values,

        cas=None,

        handle=None,

        serialise_events=False,

    ):

        """Bundle a native Trace or an explicit collection of values.



        ``serialise_events`` changes only whether raw Event evidence is

        retained alongside a Trace. It never changes the Trace persistence

        format or reconstruction authority.

        """

        if not isinstance(serialise_events, bool):

            raise TypeError("serialise_events must be a bool")



        if (

            not isinstance(trace_or_values, (list, tuple))

            and getattr(trace_or_values, "from_cas", False)

        ):

            raise ValueError(

                "TIDEBundler will not serialise a Trace restored from a "

                "Bundle. Use Trace.to_bundle() to re-export its retained "

                "source Bundle."

            )



        cas = cas if cas is not None else TIDECas()

        if not isinstance(cas, TIDECas):

            raise TypeError("cas must be a TIDECas or None")



        metadata = self._get_handle_metadata(handle)



        if isinstance(trace_or_values, (list, tuple)):

            if serialise_events:

                raise ValueError(

                    "serialise_events is valid only for Trace bundles"

                )

            return self._bundle_values(

                trace_or_values,

                cas=cas,

                metadata=metadata,

            )



        return self._bundle_native_trace(

            trace_or_values,

            cas=cas,

            metadata=metadata,

            serialise_events=serialise_events,

        )



    def _bundle_native_trace(

        self,

        trace,

        *,

        cas,

        metadata,

        serialise_events,

    ):

        if not hasattr(trace, "log") or not hasattr(trace, "ocean_id"):

            raise TypeError(

                "bundle_trace expects a Trace or a list/tuple of values"

            )



        run, values = self.run_serialiser.serialise_with_values(trace)



        if len(values) > self.resource_limits["max_values"]:

            raise ValueError(

                "TIDE bundle resource limit exceeded: values"

            )



        value_entries = self._store_hashed_values(values, cas)



        event_payload = None

        event_log = None

        if serialise_events:

            event_log = getattr(trace, "event_log", None)

            if event_log is not None:

                event_payload = event_log.to_event_bundle().to_dict()


        if event_log is not None and serialise_events:

            run_state = self.run_deserialiser.deserialise_trace(run, values)

            self._verify_event_correspondence(run, run_state, event_log)



        manifest = {

            "kind": self.KIND,

            "codec_version": self.CODEC_VERSION,

            "content": "trace",

            "run": run,

            "values": value_entries,

            "events": event_payload,

            "metadata": metadata,

        }



        complete = {

            "manifest": manifest,

            "cas": cas.export_bytes(),

        }

        self.validate_trace_bundle_resources(complete)

        self._validate_trace_manifest(manifest)

        return complete



    def _bundle_values(self, values, *, cas, metadata):

        if len(values) > self.resource_limits["max_values"]:

            raise ValueError(

                "TIDE bundle resource limit exceeded: values"

            )



        items = [

            self._store_value(value, cas)

            for value in values

        ]



        manifest = {

            "kind": self.KIND,

            "codec_version": self.CODEC_VERSION,

            "content": "values",

            "items": items,

            "metadata": metadata,

        }



        complete = {

            "manifest": manifest,

            "cas": cas.export_bytes(),

        }

        self.validate_trace_bundle_resources(complete)

        self._validate_trace_manifest(manifest)

        return complete



    def _store_hashed_values(self, values, cas):

        entries = {}



        for value_hash, value in values.items():

            if not isinstance(value_hash, str):

                raise TypeError("TideRun value hashes must be strings")



            entry = self._store_value(value, cas)

            entries[value_hash] = entry



        return entries



    def _store_value(self, value, cas):

        graph = self.bundle(value)

        graph_manifest = cas.store_bundle(graph)

        root = graph_manifest.get("root")

        record_refs = graph_manifest.get("record_refs", {})

        root_cas_ref = record_refs.get(root)



        if root_cas_ref is None:

            raise ValueError("Bundled value has no CAS root reference")



        return {

            "root_cas_ref": root_cas_ref,

            "bundle": graph_manifest,

        }



    # ------------------------------------------------------------------

    # Loading

    # ------------------------------------------------------------------

    def load_trace_bundle(

        self,

        complete,

        cas=None,

        source_bundle_hash=None,

    ):

        if not isinstance(complete, dict):

            raise TypeError("Imported TIDE Bundle must be a dict")



        self.validate_trace_bundle_resources(complete)



        manifest = deepcopy(complete.get("manifest"))

        self._validate_trace_manifest(manifest)



        cas_bytes = complete.get("cas")

        if not isinstance(cas_bytes, dict):

            raise TypeError("Imported TIDE Bundle CAS must be a dict")



        cas = cas if cas is not None else TIDECas()

        if not isinstance(cas, TIDECas):

            raise TypeError("cas must be a TIDECas or None")

        cas.import_bytes(cas_bytes)



        content = manifest["content"]



        if content == "values":

            return {

                "manifest": manifest,

                "content": "values",

                "source_bundle_hash": source_bundle_hash,

                "cas": cas,

                "values": [

                    self._load_value_entry(entry, cas)

                    for entry in manifest["items"]

                ],

            }



        values = {}

        warnings = []



        for value_hash, entry in manifest["values"].items():

            value, warning = self._load_hashed_value_entry(

                value_hash,

                entry,

                cas,

            )



            values[value_hash] = value



            if warning is not None:

                warnings.append(warning)



        run = manifest["run"]

        run_state = self.run_deserialiser.deserialise_trace(
            run,
            values,
            divergent_value_hashes={
                warning["expected"]
                for warning in warnings
                if warning.get("type") == "value_hash_mismatch"
            },
        )



        event_bundle = None

        event_log = None

        if manifest.get("events") is not None:

            from tide_tooling.tooling_core.engine.event_occurrences import (

                FrozenEventLog,

                FrozenEventLogBundle,

            )



            event_bundle = FrozenEventLogBundle.from_dict(

                manifest["events"]

            )

            event_log = FrozenEventLog.from_event_bundle(

                event_bundle,

                trusted=self.trusted,

            )

        events_verified = self._verify_event_correspondence(

            run,

            run_state,

            event_log,

        )



        return {

            "manifest": manifest,

            "content": "trace",

            "source_bundle_hash": source_bundle_hash,

            "cas": cas,

            "values": values,

            "run": run,

            "run_state": run_state,

            "event_bundle": event_bundle,

            "event_log": event_log,

            "events_verified": events_verified,

            "divergence": {

                "warnings": warnings,

            },

        }



    def _load_hashed_value_entry(self, value_hash, entry, cas):

        graph = self._load_value_graph(entry, cas)

        root_record = graph["records"][graph["root"]]
        retained_opaque_hash = (
            root_record.get("value_hash")
            if root_record.get("serialiser") == "opaque"
            and is_tide_value_hash(root_record.get("value_hash"))
            else None
        )
        observed_hash = (
            retained_opaque_hash
            or tide_value_hash_from_bundle(graph)
        )



        warning = None



        if observed_hash != value_hash:

            warning = {

                "type": "value_hash_mismatch",

                "expected": value_hash,

                "observed": observed_hash,

            }



        value = self.deserialise(graph)

        if warning is not None:

            value = TIDEOpaqueValue(

                type_name=(
                    value.type_name
                    if isinstance(value, TIDEOpaqueValue)
                    else type(value).__name__
                ),

                representation=repr(value)[:1024],

                reason=(
                    "Bundle backing value does not authenticate its "
                    "recorded TIDE identity"
                ),

                value_hash=value_hash,

            )



        if isinstance(value, TIDEOpaqueValue):

            value.value_hash = value_hash



        return value, warning



    def _load_value_entry(self, entry, cas):

        graph = self._load_value_graph(entry, cas)

        return self.deserialise(graph)



    def _load_value_graph(self, entry, cas):

        if not isinstance(entry, dict):

            raise TypeError("TIDE value entry must be a dict")



        graph_manifest = entry.get("bundle")

        root_cas_ref = entry.get("root_cas_ref")



        if not isinstance(graph_manifest, dict):

            raise TypeError("TIDE value bundle must be a dict")

        if not isinstance(root_cas_ref, str):

            raise TypeError("TIDE value root_cas_ref must be a string")



        root = graph_manifest.get("root")

        record_refs = graph_manifest.get("record_refs")



        if not isinstance(record_refs, dict):

            raise TypeError("TIDE value record_refs must be a dict")

        if record_refs.get(root) != root_cas_ref:

            raise ValueError(

                "TIDE value root_cas_ref does not match its bundle"

            )



        return cas.load_bundle(graph_manifest)



    # ------------------------------------------------------------------

    # Native Trace reconstruction

    # ------------------------------------------------------------------

    def rebuild_trace(self, loaded, source_bundle=None):

        if not isinstance(loaded, dict):
            raise TypeError("Loaded TIDE Bundle state must be a dict")

        if loaded["content"] != "trace":
            raise ValueError(
                "A standalone values Bundle does not project a Trace"
            )

        run_state = loaded["run_state"]
        if not isinstance(run_state, dict):
            raise TypeError("Loaded Trace bundle has no native TideRun state")

        expected_run_state = {
            "records",
            "ready_attempts",
            "trace",
            "state_authorities",
        }
        if set(run_state) != expected_run_state:
            missing = sorted(expected_run_state - set(run_state))
            unknown = sorted(set(run_state) - expected_run_state)
            raise ValueError(
                "Normalized TideRun state shape changed: "
                f"missing={missing}, unknown={unknown}"
            )

        records = run_state["records"]
        ready_attempts = run_state["ready_attempts"]
        trace_meta = run_state["trace"]
        state_authorities = run_state["state_authorities"]

        if not isinstance(records, list):
            raise TypeError("Loaded TideRun records must be a list")
        if not isinstance(ready_attempts, list):
            raise TypeError("Loaded TideRun ready_attempts must be a list")
        if not isinstance(trace_meta, dict):
            raise TypeError("Loaded TideRun trace metadata must be a dict")
        if not isinstance(state_authorities, dict):
            raise TypeError(
                "Loaded TideRun state_authorities must be a dict"
            )

        ocean_id = trace_meta["ocean_id"]
        if ocean_id is None:
            raise ValueError("Bundled Trace is missing ocean_id")

        name = trace_meta["name"]
        if not isinstance(name, str):
            raise TypeError("Bundled Trace name must be text")

        pipelines = trace_meta["pipelines"]
        bundle_context = trace_meta["bundle_context"]
        model_context = trace_meta["model_context"]
        missing_referenced_events = trace_meta["missing_referenced_events"]
        causal_order_violations = trace_meta["causal_order_violations"]
        deadlock = trace_meta["deadlock"]
        evidence_violations = trace_meta["evidence_violations"]

        for label, value in {
            "pipelines": pipelines,
            "bundle_context": bundle_context,
            "model_context": model_context,
            "missing_referenced_events": missing_referenced_events,
            "causal_order_violations": causal_order_violations,
            "deadlock": deadlock,
            "evidence_violations": evidence_violations,
        }.items():
            if not isinstance(value, dict):
                raise TypeError(
                    f"Bundled Trace {label} must be a dict"
                )

        for label in (
            "ready_exec_ids",
            "blocked_exec_ids",
            "started_runner_ids",
            "causal_runner_ids",
            "ended_runner_ids",
        ):
            if not isinstance(trace_meta[label], tuple):
                raise TypeError(
                    f"Bundled Trace {label} must be a tuple"
                )

        loaded_divergence = loaded["divergence"]
        if not isinstance(loaded_divergence, dict):
            raise TypeError("Loaded Bundle divergence must be a dict")

        loaded_warnings = loaded_divergence["warnings"]
        if not isinstance(loaded_warnings, list):
            raise TypeError("Loaded Bundle divergence warnings must be a list")

        bundle_context = deepcopy(bundle_context)

        if loaded_warnings:
            divergence = bundle_context.get("divergence")
            if divergence is None:
                divergence = {}
            elif not isinstance(divergence, dict):
                raise TypeError(
                    "Bundled Trace divergence context must be a dict"
                )
            else:
                divergence = deepcopy(divergence)

            existing_warnings = divergence.get("warnings")
            if existing_warnings is None:
                existing_warnings = []
            elif not isinstance(existing_warnings, list):
                raise TypeError(
                    "Bundled Trace divergence context warnings must be a list"
                )
            else:
                existing_warnings = list(existing_warnings)

            existing_warnings.extend(deepcopy(loaded_warnings))
            divergence["warnings"] = existing_warnings
            bundle_context["divergence"] = divergence

        try:
            from tide_tooling.tooling_core.engine.tide_logging import Trace
        except Exception as error:
            raise ImportError(
                "Trace rebuild requires the TIDE engine runtime"
            ) from error

        return Trace(
            records,
            event_log=loaded["event_log"],
            events_verified=loaded["events_verified"],
            pipelines=pipelines,
            runner=trace_meta["runner"],
            ocean_id=ocean_id,
            name=name,
            from_cas=True,
            bundle_context=bundle_context,
            model_context=model_context,
            host=trace_meta["host"],
            host_record=trace_meta["host_record"],
            trace_starting_event=trace_meta["trace_starting_event_id"],
            pipeline_starting_event=trace_meta["pipeline_starting_event_id"],
            tide_plan=trace_meta["tide_plan"],
            missing_referenced_events=missing_referenced_events,
            causal_order_violations=causal_order_violations,
            ready_exec_ids=trace_meta["ready_exec_ids"],
            blocked_exec_ids=trace_meta["blocked_exec_ids"],
            deadlock=deadlock,
            ready_attempts=ready_attempts,
            started_runner_ids=trace_meta["started_runner_ids"],
            causal_runner_ids=trace_meta["causal_runner_ids"],
            ended_runner_ids=trace_meta["ended_runner_ids"],
            evidence_violations=evidence_violations,
            _state_authorities=state_authorities,
            _value_backing=loaded["values"],
            _require_closed_evidence=True,
            _source_bundle=source_bundle,
        )


    # ------------------------------------------------------------------

    # Validation / resource limits

    # ------------------------------------------------------------------

    def validate_trace_bundle_resources(self, complete):

        if not isinstance(complete, dict):

            raise TypeError("Imported TIDE Bundle must be a dict")

        self._validate_nested_bundle_resources(complete)



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



                size = len(content)

                self._check_resource_limit(

                    "CAS record bytes",

                    size,

                    "max_cas_record_bytes",

                )

                total_bytes += size

                self._check_resource_limit(

                    "CAS total bytes",

                    total_bytes,

                    "max_cas_total_bytes",

                )



        if not isinstance(manifest, dict):

            return



        content = manifest.get("content")



        if content == "values":

            items = manifest.get("items")

            if isinstance(items, list):

                self._check_resource_limit(

                    "values",

                    len(items),

                    "max_values",

                )

                self._validate_value_entries_resources(items)

            return



        values = manifest.get("values")

        if isinstance(values, dict):

            self._check_resource_limit(

                "values",

                len(values),

                "max_values",

            )

            self._validate_value_entries_resources(values.values())



        run = manifest.get("run")

        if isinstance(run, dict):

            records = run.get("records")

            if isinstance(records, list):

                self._check_resource_limit(

                    "Trace records",

                    len(records),

                    "max_exec_records",

                )



            attempts = run.get("ready_attempts")

            if isinstance(attempts, list):

                self._check_resource_limit(

                    "ready attempts",

                    len(attempts),

                    "max_ready_attempts",

                )



        events = manifest.get("events")

        if isinstance(events, dict):

            count = events.get("count")

            if isinstance(count, int):

                self._check_resource_limit(

                    "events",

                    count,

                    "max_events",

                )



            graph = events.get("events")

            if isinstance(graph, dict):

                records = graph.get("records")

                if isinstance(records, dict):

                    self._check_resource_limit(

                        "Event graph records",

                        len(records),

                        "max_event_records",

                    )


    def _validate_nested_bundle_resources(
        self,
        value,
        *,
        depth=0,
        active=None,
    ):
        if depth > self.resource_limits["max_deserialise_depth"]:
            raise ValueError(
                "TIDE bundle resource limit exceeded: Bundle depth"
            )

        if not isinstance(
            value,
            (dict, list, tuple, set, frozenset),
        ):
            return

        self._check_resource_limit(
            "Bundle container items",
            len(value),
            "max_container_items",
        )

        active = set() if active is None else active
        identity = id(value)
        if identity in active:
            raise ValueError("Imported TIDE Bundle contains a container cycle")

        active.add(identity)
        children = value.values() if isinstance(value, dict) else value
        try:
            for child in children:
                self._validate_nested_bundle_resources(
                    child,
                    depth=depth + 1,
                    active=active,
                )
        finally:
            active.remove(identity)



    def _validate_value_entries_resources(self, entries):

        total_records = 0



        for entry in entries:

            if not isinstance(entry, dict):

                continue



            graph = entry.get("bundle")

            if not isinstance(graph, dict):

                continue



            record_refs = graph.get("record_refs")

            if not isinstance(record_refs, dict):

                continue



            self._check_resource_limit(

                "value records",

                len(record_refs),

                "max_value_records",

            )

            total_records += len(record_refs)



            self._check_resource_limit(

                "CAS records referenced by values",

                total_records,

                "max_cas_records",

            )



    @classmethod

    def _validate_trace_manifest(cls, manifest):

        if not isinstance(manifest, dict):

            raise TypeError("Imported TIDE Bundle manifest must be a dict")

        if manifest.get("kind") != cls.KIND:

            raise ValueError("Unsupported TIDE Bundle kind")

        if manifest.get("codec_version") != cls.CODEC_VERSION:

            raise ValueError("Unsupported TIDE Bundle codec version")

        if not isinstance(manifest.get("metadata"), dict):

            raise TypeError("TIDE Bundle metadata must be a dict")



        content = manifest.get("content")



        if content == "trace":

            cls._validate_native_trace_manifest(manifest)

            return



        if content == "values":

            cls._validate_values_manifest(manifest)

            return



        raise ValueError("Unsupported TIDE Bundle content type")



    @staticmethod

    def _validate_native_trace_manifest(manifest):

        expected = {

            "kind",

            "codec_version",

            "content",

            "run",

            "values",

            "events",

            "metadata",

        }

        if set(manifest) != expected:

            unexpected = sorted(set(manifest) - expected)

            missing = sorted(expected - set(manifest))

            details = []

            if missing:

                details.append("missing: " + ", ".join(missing))

            if unexpected:

                details.append("unexpected: " + ", ".join(unexpected))

            raise ValueError(

                "Invalid native Trace bundle manifest"

                + (": " + "; ".join(details) if details else "")

            )



        run = manifest.get("run")

        if not isinstance(run, dict):

            raise TypeError("TIDE Trace bundle run must be a dict")

        if run.get("kind") != TIDERunSerialiser.KIND:

            raise ValueError("Unsupported TideRun kind")

        if run.get("codec_version") != TIDERunSerialiser.CODEC_VERSION:

            raise ValueError("Unsupported TideRun codec version")



        values = manifest.get("values")

        if not isinstance(values, dict):

            raise TypeError("TIDE Trace bundle values must be a dict")

        for value_hash, entry in values.items():

            if not is_tide_value_hash(value_hash):

                raise ValueError(

                    "TIDE Trace value key must be a valid TIDE value hash"

                )

            TIDEBundler._validate_value_entry(entry)



        events = manifest.get("events")

        if events is None:
            return

        if events is not None:

            from tide_tooling.tooling_core.engine.event_occurrences import (

                FrozenEventLogBundle,

            )

            FrozenEventLogBundle.from_dict(events)



    @staticmethod

    def _validate_values_manifest(manifest):

        expected = {

            "kind",

            "codec_version",

            "content",

            "items",

            "metadata",

        }

        if set(manifest) != expected:

            unexpected = sorted(set(manifest) - expected)

            missing = sorted(expected - set(manifest))

            details = []

            if missing:

                details.append("missing: " + ", ".join(missing))

            if unexpected:

                details.append("unexpected: " + ", ".join(unexpected))

            raise ValueError(

                "Invalid values bundle manifest"

                + (": " + "; ".join(details) if details else "")

            )



        items = manifest.get("items")

        if not isinstance(items, list):

            raise TypeError("TIDE values bundle items must be a list")

        for entry in items:

            TIDEBundler._validate_value_entry(entry)



    @staticmethod

    def _validate_value_entry(entry):

        if not isinstance(entry, dict):

            raise TypeError("TIDE value entry must be a dict")



        allowed = {"root_cas_ref", "bundle"}

        if not set(entry).issubset(allowed):

            raise ValueError("TIDE value entry contains unknown fields")



        root_cas_ref = entry.get("root_cas_ref")

        graph = entry.get("bundle")



        if not isinstance(root_cas_ref, str):

            raise TypeError("TIDE value root_cas_ref must be a string")

        if not isinstance(graph, dict):

            raise TypeError("TIDE value bundle must be a dict")



        root = graph.get("root")

        record_refs = graph.get("record_refs")



        if not isinstance(record_refs, dict):

            raise TypeError("TIDE value bundle record_refs must be a dict")

        if record_refs.get(root) != root_cas_ref:

            raise ValueError(

                "TIDE value root_cas_ref does not match its bundle"

            )



    # ------------------------------------------------------------------

    # Helpers

    # ------------------------------------------------------------------

    @staticmethod

    def _get_handle_metadata(handle):

        if handle is None:

            return {}



        metadata = getattr(handle, "metadata", None)



        if isinstance(metadata, dict):

            return deepcopy(metadata)



        if isinstance(handle, dict) and isinstance(handle.get("metadata"), dict):

            return deepcopy(handle["metadata"])



        return {}



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

            if (

                not isinstance(value, int)

                or isinstance(value, bool)

                or value < 1

            ):

                raise ValueError(

                    f"TIDE bundle resource limit {name} "

                    "must be a positive integer"

                )



        return limits



    def _check_resource_limit(self, label, actual, limit_name):

        maximum = self.resource_limits[limit_name]

        if actual > maximum:

            raise ValueError(

                "TIDE bundle resource limit exceeded: "

                f"{label} {actual} > {maximum} ({limit_name})"

            )
