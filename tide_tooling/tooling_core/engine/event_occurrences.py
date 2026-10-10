"""Capture, validate, encode, and restore historical Event occurrences."""
from functools import lru_cache
import base64
import copy
import gc
import hashlib
import inspect
import json
from dataclasses import dataclass, field, fields, is_dataclass
from enum import Enum
from types import ModuleType


from tide_tooling.tooling_core.bundling.TIDEDeserialiser import (
    TIDEDeserialiser,
)
from tide_tooling.tooling_core.bundling.TIDESerialiser import TIDESerialiser


_JSON_TYPE = "$tide_event_json_type"


def _json_safe(value):
    if isinstance(value, bytes):
        return {_JSON_TYPE: "bytes", "base64": base64.b64encode(value).decode("ascii")}
    if isinstance(value, bytearray):
        return {_JSON_TYPE: "bytearray", "base64": base64.b64encode(bytes(value)).decode("ascii")}
    if isinstance(value, memoryview):
        return {_JSON_TYPE: "memoryview", "base64": base64.b64encode(value.tobytes()).decode("ascii")}
    if isinstance(value, tuple):
        return {_JSON_TYPE: "tuple", "items": [_json_safe(item) for item in value]}
    if isinstance(value, set):
        return {_JSON_TYPE: "set", "items": [_json_safe(item) for item in value]}
    if isinstance(value, frozenset):
        return {_JSON_TYPE: "frozenset", "items": [_json_safe(item) for item in value]}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    return value


def _from_json_safe(
    value,
    *,
    max_depth=256,
    max_container_items=100_000,
    max_bytes=64 * 1024 * 1024,
    _depth=0,
    _byte_budget=None,
):
    if _byte_budget is None:
        _byte_budget = {"remaining": max_bytes}
    if _depth > max_depth:
        raise ValueError("Event JSON exceeds maximum depth")
    if isinstance(value, list):
        if len(value) > max_container_items:
            raise ValueError("Event JSON exceeds maximum container items")
        return [
            _from_json_safe(
                item,
                max_depth=max_depth,
                max_container_items=max_container_items,
                max_bytes=max_bytes,
                _depth=_depth + 1,
                _byte_budget=_byte_budget,
            )
            for item in value
        ]
    if not isinstance(value, dict):
        return value
    if len(value) > max_container_items:
        raise ValueError("Event JSON exceeds maximum container items")


    value_type = value.get(_JSON_TYPE)
    if value_type in {"bytes", "bytearray", "memoryview"}:
        encoded = value.get("base64")
        if not isinstance(encoded, str):
            raise ValueError("Event byte payload must be text")
        remaining = _byte_budget["remaining"]
        if len(encoded) > ((remaining + 2) // 3) * 4:
            raise ValueError("Event byte payload exceeds maximum size")
        try:
            decoded = base64.b64decode(
                encoded.encode("ascii"),
                validate=True,
            )
        except (UnicodeEncodeError, ValueError) as error:
            raise ValueError("Invalid Event byte payload") from error
        if len(decoded) > remaining:
            raise ValueError("Event byte payload exceeds maximum size")
        _byte_budget["remaining"] -= len(decoded)
        if value_type == "bytearray":
            return bytearray(decoded)
        if value_type == "memoryview":
            return memoryview(decoded)
        return decoded
    if value_type in {"tuple", "set", "frozenset"}:
        encoded_items = value.get("items")
        if not isinstance(encoded_items, list):
            raise ValueError("Event container items must be a list")
        if len(encoded_items) > max_container_items:
            raise ValueError("Event JSON exceeds maximum container items")
        items = [
            _from_json_safe(
                item,
                max_depth=max_depth,
                max_container_items=max_container_items,
                max_bytes=max_bytes,
                _depth=_depth + 1,
                _byte_budget=_byte_budget,
            )
            for item in encoded_items
        ]
        if value_type == "tuple":
            return tuple(items)
        try:
            decoded = set(items) if value_type == "set" else frozenset(items)
        except TypeError as error:
            raise ValueError("Event set items must be hashable") from error
        if len(decoded) != len(items):
            raise ValueError("Event set contains duplicate-equivalent values")
        return decoded
    if value_type is not None:
        raise ValueError(f"Unsupported Event JSON value: {value_type}")
    return {
        key: _from_json_safe(
            item,
            max_depth=max_depth,
            max_container_items=max_container_items,
            max_bytes=max_bytes,
            _depth=_depth + 1,
            _byte_budget=_byte_budget,
        )
        for key, item in value.items()
    }


@lru_cache(maxsize=1)
def _known_types():
    from tide_tooling.TIDEAdapters import NOWMeta
    from tide_tooling.tooling_core.engine.classes import (
        DELETE, EMIT, END, RAISE, READ_EFFECT, RUN, WRITE,
        DataRef, Effect, EffectRuntime, Node, NodeInput, Pipeline,
        ProcessStates, RunnerNode, RunnerNodeInput, __fallback__,
    )
    from tide_tooling.tooling_core.engine.event_payloads import (
        DependencyConcretised, EffectOccurrence, EmittedNodes,
        MissingReferences, NodeBlocked, NodeSnapshot, Nodes,
        PipelineRegistration, PipelineRun, RunnerPlan, Reference, Result, State, Work,
    )
    from tide_tooling.tooling_core.engine.events import (
        Cause, Effects, Event, ExecutorEvents, LogEvents, OceanEvents,
        PipelineEvents, RouterEvents,
    )
    from tide_tooling.tooling_core.engine.executor import ExecMeta
    from tide_tooling.tooling_core.engine.stores import MemoryKVStore, StoreTemplate
    from tide_tooling.tooling_core.engine.tide_records import (
        Actor, ActorOccurrence, AffectedRecord, DependencyUseRecord,
        EffectRecord, EmittedNodeRecord, ExecRecord, ExpandedDepRecord,
        InputUseRecord, Inputs, WriteRecord,
    )
    from tide_tooling.tooling_core.engine.utils import (
        TIDEValueSnapshot,
        _FrozenSnapshotDict,
        _FrozenSnapshotFrozenSet,
        _FrozenSnapshotList,
        _FrozenSnapshotSet,
        _FrozenSnapshotTuple,
    )


    known = (
        Event, Cause,
        OceanEvents, PipelineEvents, ExecutorEvents, RouterEvents, LogEvents,
        Effects, ProcessStates,
        PipelineRegistration, PipelineRun, RunnerPlan, State, Reference, Nodes,
        NodeSnapshot, Result, Work, EffectOccurrence, EmittedNodes,
        NodeBlocked, DependencyConcretised, MissingReferences,
        ExecMeta, ExecRecord, TIDEValueSnapshot,
        _FrozenSnapshotDict, _FrozenSnapshotList, _FrozenSnapshotTuple,
        _FrozenSnapshotSet, _FrozenSnapshotFrozenSet,
        Actor, ActorOccurrence, EmittedNodeRecord, AffectedRecord,
        EffectRecord, InputUseRecord, WriteRecord, DependencyUseRecord,
        ExpandedDepRecord, Inputs,
        Pipeline, Node, RunnerNode, RunnerNodeInput, NodeInput, DataRef,
        Effect, EMIT, END, RUN, DELETE, WRITE, READ_EFFECT, RAISE,
        EffectRuntime, NOWMeta, StoreTemplate, MemoryKVStore, __fallback__,
    )
    return {
        f"{value_type.__module__}:{value_type.__qualname__}": value_type
        for value_type in known
    }


def _type_name(value_type):
    return f"{value_type.__module__}:{value_type.__qualname__}"


class TIDEEventSerialiser:
    def __init__(self, unsupported="opaque"):
        self.serialiser = TIDESerialiser(unsupported=unsupported)


    def serialise(self, events):
        from tide_tooling.tooling_core.engine.events import Event


        if not isinstance(events, (list, tuple)):
            raise TypeError("Events must be a list or tuple")
        events = tuple(events)
        if any(not isinstance(event, Event) for event in events):
            raise TypeError("Event collections must contain only Event objects")


        self.records = {}
        self.object_ids = {}
        self.next_object_index = 0
        with self.serialiser.graph_session():
            root = self._add(events)
        return {
            "kind": "tide_events",
            "codec_version": 1,
            "count": len(events),
            "root": root,
            "records": _json_safe(self.records),
        }


    def _add(self, value):
        identity = id(value)
        if identity in self.object_ids:
            original, object_id = self.object_ids[identity]
            if original is not value:
                raise RuntimeError("Event object identity collision")
            return object_id


        object_id = f"event_object_{self.next_object_index}"
        self.next_object_index += 1
        # Keep the object alive until the complete Event graph has been
        # encoded so Python cannot reuse its id for another temporary value.
        self.object_ids[identity] = (value, object_id)
        self.records[object_id] = None
        record = (
            self._serialise_known(value)
            or self.serialiser.serialise_with_refs(value, self._add)
        )
        if record.get("serialiser") == "opaque" and not record.get("value_hash"):
            try:
                from tide_tooling.tooling_core.engine.utils import tide_value_hasher


                record["value_hash"] = tide_value_hasher(value)
            except Exception:
                pass
        self.records[object_id] = record
        return object_id


    def _serialise_known(self, value):
        registry = _known_types()

        from tide_tooling.tooling_core.engine.utils import TIDEValueSnapshot
        if isinstance(value, TIDEValueSnapshot):
            value = value._for_persistence()


        if isinstance(value, type):
            type_name = _type_name(value)
            if type_name in registry and registry[type_name] is value:
                return {"serialiser": "tide_event", "form": "class", "type": type_name}
            return None


        value_type = type(value)
        type_name = _type_name(value_type)
        if type_name not in registry or registry[type_name] is not value_type:
            return None


        if isinstance(value, Enum):
            return {
                "serialiser": "tide_event",
                "form": "enum",
                "type": type_name,
                "value": {"$ref": self._add(value.value)},
            }


        if isinstance(value, tuple) and hasattr(value, "_fields"):
            field_values = {name: getattr(value, name) for name in value._fields}
            form = "namedtuple"
        elif is_dataclass(value):
            field_values = {field.name: getattr(value, field.name) for field in fields(value)}
            form = "object"
        else:
            field_values = dict(vars(value))
            if isinstance(value, BaseException):
                field_values["__exception_args__"] = value.args
            value_hash = getattr(value_type, "__hash__", None)
            form = (
                "hashable_object"
                if value_hash not in {None, object.__hash__}
                else "object"
            )


        return {
            "serialiser": "tide_event",
            "form": form,
            "type": type_name,
            "fields": {
                name: {"$ref": self._add(field_value)}
                for name, field_value in field_values.items()
            },
        }


class TIDEEventDeserialiser(TIDEDeserialiser):
    def __init__(
        self,
        *args,
        max_bytes=64 * 1024 * 1024,
        validate_events=True,
        **kwargs,
    ):
        if not isinstance(max_bytes, int) or isinstance(max_bytes, bool) or max_bytes < 1:
            raise ValueError("max_bytes must be a positive integer")
        super().__init__(*args, **kwargs)
        self.max_bytes = max_bytes
        self.validate_events = bool(validate_events)


    def deserialise(self, event_graph):
        self._decoded_snapshots = []
        if not isinstance(event_graph, dict):
            raise TypeError("Serialised Events must be a dict")
        if event_graph.get("kind") != "tide_events":
            raise ValueError("Unsupported Events kind")
        if event_graph.get("codec_version") != 1:
            raise ValueError("Unsupported Events codec version")


        count = event_graph.get("count")
        if not isinstance(count, int) or isinstance(count, bool) or count < 0:
            raise TypeError("Serialised Event count must be a non-negative integer")
        encoded_records = event_graph.get("records")
        if not isinstance(encoded_records, dict):
            raise TypeError("Serialised Event records must be a dict")
        self._check_limit(
            "Event records",
            len(encoded_records),
            self.max_records,
        )
        records = _from_json_safe(
            encoded_records,
            max_depth=self.max_depth,
            max_container_items=self.max_container_items,
            max_bytes=self.max_bytes,
        )
        if not isinstance(records, dict):
            raise TypeError("Serialised Event records must be a dict")
        self._check_limit("Event records", len(records), self.max_records)


        events = self._deserialise_graph(event_graph.get("root"), records)
        if not isinstance(events, tuple):
            raise TypeError("Serialised Events root must be a tuple")
        if len(events) != count:
            raise ValueError("Serialised Event count does not match its graph")
        for snapshot in self._decoded_snapshots:
            snapshot._validate_integrity()
        if self.validate_events:
            validate_events(events)
        return events


    def _allocate_shell(self, record):
        if (
            isinstance(record, dict)
            and record.get("serialiser") == "tide_event"
            and record.get("form") == "object"
        ):
            registry = _known_types()
            type_name = record.get("type")
            if type_name not in registry:
                raise ValueError(f"Unsupported Event graph type: {type_name}")
            return registry[type_name].__new__(registry[type_name])
        return super()._allocate_shell(record)


    def _populate_shell(
        self,
        record,
        shell,
        resolve_ref,
        cache,
        object_id,
        depth,
    ):
        if (
            isinstance(record, dict)
            and record.get("serialiser") == "tide_event"
            and record.get("form") == "object"
        ):
            encoded_fields = record.get("fields")
            if not isinstance(encoded_fields, dict):
                raise TypeError("Event graph object fields must be a dict")
            for name, encoded in encoded_fields.items():
                field_value = self._deserialise_value(
                    encoded,
                    resolve_ref=resolve_ref,
                    cache=cache,
                    depth=depth + 1,
                )
                if name == "__exception_args__":
                    object.__setattr__(shell, "args", field_value)
                else:
                    object.__setattr__(shell, name, field_value)
            from tide_tooling.tooling_core.engine.utils import TIDEValueSnapshot
            if isinstance(shell, TIDEValueSnapshot):
                object.__setattr__(
                    shell,
                    "trusted",
                    bool(shell.trusted and self.trusted),
                )
                self._decoded_snapshots.append(shell)
            return shell
        return super()._populate_shell(
            record,
            shell,
            resolve_ref,
            cache,
            object_id,
            depth,
        )


    def _deserialise_record(
        self,
        record,
        resolve_ref=None,
        cache=None,
        object_id=None,
        depth=0,
    ):
        if (
            isinstance(record, dict)
            and record.get("serialiser") == "tide_event"
        ):
            return self._deserialise_known(
                record,
                resolve_ref,
                cache,
                object_id,
                depth,
            )

        return super()._deserialise_record(
            record,
            resolve_ref=resolve_ref,
            cache=cache,
            object_id=object_id,
            depth=depth,
        )


    def _deserialise_known(self, record, resolve_ref, cache, object_id, depth):
        self._check_depth(depth)
        registry = _known_types()
        type_name = record.get("type")
        if type_name not in registry:
            raise ValueError(f"Unsupported Event graph type: {type_name}")
        value_type = registry[type_name]
        form = record.get("form")


        if form == "class":
            return value_type
        if form == "enum":
            value = self._deserialise_value(
                record["value"],
                resolve_ref=resolve_ref,
                cache=cache,
                depth=depth + 1,
            )
            return value_type(value)


        encoded_fields = record.get("fields")
        if not isinstance(encoded_fields, dict):
            raise TypeError("Event graph object fields must be a dict")


        if form == "namedtuple":
            field_values = {
                name: self._deserialise_value(
                    value,
                    resolve_ref=resolve_ref,
                    cache=cache,
                    depth=depth + 1,
                )
                for name, value in encoded_fields.items()
            }
            return value_type(**field_values)
        if form not in {"object", "hashable_object"}:
            raise ValueError(f"Unsupported Event graph object form: {form}")


        value = value_type.__new__(value_type)
        if form == "object":
            self._cache_value(cache, object_id, value)
        for name, encoded in encoded_fields.items():
            field_value = self._deserialise_value(
                encoded,
                resolve_ref=resolve_ref,
                cache=cache,
                depth=depth + 1,
            )
            if name == "__exception_args__":
                object.__setattr__(value, "args", field_value)
            else:
                object.__setattr__(value, name, field_value)
        if form == "hashable_object":
            self._cache_value(cache, object_id, value)
        return value


def validate_event(event):
    """Validate one captured Event against the single Event schema."""
    from tide_tooling.tooling_core.engine.event_payloads import validate_event_payload
    from tide_tooling.tooling_core.engine.events import Cause, Event


    if not isinstance(event, Event):
        raise TypeError("Event graph must contain only Event objects")
    if not isinstance(event.cause, Cause):
        raise TypeError("Event cause must be a Cause")
    if event.cause.description is None:
        raise ValueError("Cause requires a description")


    validate_event_payload(event.type, event.payload)


def validate_events(events):
    for event in events:
        validate_event(event)


_EVENT_ATOMIC_TYPES = (type(None), bool, int, float, complex, str, bytes, range)


def _event_mutable_identities(value, seen=None):
    """Return mutable identities reachable through one Event graph.

    Immutable structural shells are traversed but need not be detached.
    TIDEValueSnapshot is already an immutable ownership boundary with
    detached accessors, and runtime callables are process-local identities
    rather than mutable Event data.
    """
    from tide_tooling.tooling_core.engine.utils import TIDEValueSnapshot

    if seen is None:
        seen = set()
    identity = id(value)
    if identity in seen:
        return set()
    seen.add(identity)

    if (
        isinstance(value, _EVENT_ATOMIC_TYPES)
        or isinstance(value, (Enum, ModuleType, TIDEValueSnapshot, type))
        or inspect.isroutine(value)
    ):
        return set()

    immutable_shell = isinstance(value, (tuple, frozenset)) or (
        is_dataclass(value)
        and getattr(type(value), "__dataclass_params__").frozen
    )
    identities = set() if immutable_shell else {identity}
    for item in gc.get_referents(value):
        identities.update(_event_mutable_identities(item, seen))
    return identities


def _detach_event(event):
    """Materialise an Event without retaining any mutable source alias."""
    try:
        detached = copy.deepcopy(event)
        if (
            _event_mutable_identities(event)
            & _event_mutable_identities(detached)
        ):
            raise TypeError("Event deepcopy retained mutable source aliases")
    except Exception:
        graph = TIDEEventSerialiser(
            unsupported="opaque",
        ).serialise((event,))
        detached = TIDEEventDeserialiser(
            trusted=False,
            max_records=max(1, len(graph["records"])),
        ).deserialise(graph)[0]

    validate_event(detached)
    return detached


@dataclass(frozen=True)
class EventOccurrence:
    """One Event detached at its publication boundary."""
    _event: object = field(repr=False, compare=False)
    @classmethod
    def capture(cls, event):
        """Detach one published Event exactly once at publication.

        Ordinary Event graphs use deepcopy directly. If a runtime value
        deliberately cannot be copied, fall back to the canonical Event codec:
        known TIDE structure is rebuilt exactly and unsupported runtime values
        become explicit opaque evidence rather than aborting publication.
        """
        return cls(_detach_event(event))

    @property
    def event(self):
        return _detach_event(self._event)


@dataclass(frozen=True)
class FrozenEventLogBundle:
    """Canonical portable storage for one frozen Event history."""
    _encoded: str
    @staticmethod
    def _event_log_hash(events):
        encoded = json.dumps(
            events,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        return (
            "sha3_512_tide_event_log:"
            + hashlib.sha3_512(encoded).hexdigest()
        )
    @classmethod
    def from_dict(cls, value):
        if not isinstance(value, dict):
            raise TypeError("Frozen Event log bundle must be a dict")
        if value.get("kind") != "tide_frozen_event_log":
            raise ValueError("Unsupported Frozen Event log bundle kind")
        if value.get("codec_version") != 1:
            raise ValueError("Unsupported Frozen Event log bundle version")
        events = value.get("events")
        if not isinstance(events, dict):
            raise TypeError("Frozen Event log events must be a dict")
        event_count = events.get("count")
        if (
            not isinstance(event_count, int)
            or isinstance(event_count, bool)
            or event_count < 0
        ):
            raise TypeError(
                "TIDE Event graph count must be a non-negative integer"
            )
        if value.get("count") != event_count:
            raise ValueError(
                "Frozen Event log count does not match Event graph"
            )
        if value.get("event_log_hash") != cls._event_log_hash(events):
            raise ValueError("Frozen Event log content hash does not match")
        return cls(json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
        ))
    @classmethod
    def create(cls, event_log):
        if not isinstance(event_log, FrozenEventLog):
            raise TypeError("event_log must be a FrozenEventLog")
        events = TIDEEventSerialiser().serialise(event_log.events)
        return cls.from_dict({
            "kind": "tide_frozen_event_log",
            "codec_version": 1,
            "count": len(event_log),
            "events": events,
            "event_log_hash": cls._event_log_hash(events),
        })
    def to_dict(self):
        return json.loads(self._encoded)


@dataclass(frozen=True)
class FrozenEventLog:
    """One immutable, ordered history of complete Event occurrences."""


    _occurrences: tuple[EventOccurrence, ...]


    def __post_init__(self):
        if not all(
            isinstance(occurrence, EventOccurrence)
            for occurrence in self._occurrences
        ):
            raise TypeError(
                "FrozenEventLog accepts only EventOccurrence objects"
            )
        object.__setattr__(
            self,
            "_occurrences",
            tuple(
                EventOccurrence.capture(occurrence._event)
                for occurrence in self._occurrences
            ),
        )


    @classmethod
    def capture(cls, events):
        from tide_tooling.tooling_core.engine.events import Event


        if isinstance(events, FrozenEventLog):
            return events
        captured = []
        for event in events:
            if not isinstance(event, Event):
                raise TypeError("FrozenEventLog accepts only Event objects")
            captured.append(EventOccurrence(event))
        return cls(tuple(captured))


    @classmethod
    def from_event_bundle(cls, bundle, *, trusted=False):
        if not isinstance(bundle, FrozenEventLogBundle):
            raise TypeError("bundle must be a FrozenEventLogBundle")


        value = bundle.to_dict()
        graph = value["events"]


        events = TIDEEventDeserialiser(
            trusted=trusted,
            max_records=max(1, len(graph["records"])),
        ).deserialise(graph)


        return cls(tuple(
            EventOccurrence(event)
            for event in events
        ))


    def __len__(self):
        return len(self._occurrences)


    @property
    def events(self):
        return tuple(
            occurrence.event
            for occurrence in self._occurrences
        )


    def slice_from(self, event_id):
        try:
            index = next(
                index
                for index, event in enumerate(self.events)
                if event.event_id == event_id
            )
        except StopIteration as error:
            raise ValueError("Trace starting Event was not found") from error
        return type(self)(self._occurrences[index:])


    def to_event_bundle(self):
        return FrozenEventLogBundle.create(self)


class EventLogger:
    """Append-only storage for already-captured Event occurrences."""


    def __init__(self):
        self._occurrences = []


    def append(self, event):
        self._occurrences.append(EventOccurrence.capture(event))


    @property
    def occurrences(self):
        return tuple(self._occurrences)


    def freeze(self):
        return FrozenEventLog(tuple(self._occurrences))


    @property
    def events(self):
        return tuple(
            occurrence.event
            for occurrence in self._occurrences
        )
