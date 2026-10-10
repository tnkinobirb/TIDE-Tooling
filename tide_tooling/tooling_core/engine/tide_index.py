"""Indexes exact execution, State-version, value, and effect evidence."""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType

from tide_tooling.tooling_core.engine.events import Effects
from tide_tooling.tooling_core.engine.tide_links import EffectEdge, ReadEdge
from tide_tooling.tooling_core.engine.tide_records import AffectedRecord, ActorOccurrence
from tide_tooling.tooling_core.engine.utils import (
    DedupedList,
    TIDEValueSnapshot,
    deduped_dict,
    ensure_tide_snapshot,
    tide_value_hasher,
)
from tide_tooling.tooling_core.engine.tide_evidence import RestoringValueMap


class _FrozenMultiMap(Mapping):
    """Immutable defaultdict-like view used by a sealed TraceIndex."""

    def __init__(self, values):
        self._values = MappingProxyType({
            key: tuple(items)
            for key, items in values.items()
        })

    def __getitem__(self, key):
        return self._values.get(key, ())

    def __iter__(self):
        return iter(self._values)

    def __len__(self):
        return len(self._values)

    def __contains__(self, key):
        return key in self._values

    def get(self, key, default=None):
        return self._values.get(key, default)


class TraceIndex:
    def __setattr__(self, name, value):
        if getattr(self, "_tide_sealed", False):
            raise AttributeError("TraceIndex is sealed Trace evidence")
        object.__setattr__(self, name, value)

    def __init__(self):
        self._tide_sealed = False
        # Retain explicit readiness outcomes which do not create ExecRecords.
        self.ready_exec_ids = frozenset()
        self.blocked_exec_ids = frozenset()
        # Index executions and their actors.
        self.exec_by_exec_id = {}
        self.exec_ids_by_runner_node = deduped_dict()
        self.exec_ids_by_definition_id = deduped_dict()
        self.actor_to_exec_ids = deduped_dict()
        self.actor_occurrence_to_exec_id: dict[ActorOccurrence, str] = {}
        self.func_name_to_actors = deduped_dict()
        self.parent_exec_id_by_exec_id = {}
        self.child_exec_ids_by_exec_id = deduped_dict()

        # Index evidence by exact reference occurrence.
        self.read_records_by_ref_version = deduped_dict()
        self.write_records_by_ref_version = deduped_dict()
        self.versions_by_ref = deduped_dict()
        self.seen_refs = DedupedList()

        # Index the execution relationships of each occurrence.
        self.producer_exec_ids_by_ref_version = deduped_dict()
        self.consumer_exec_ids_by_ref_version = deduped_dict()
        # Index captured values and their State occurrences.
        self._value_snapshots_by_hash = {}
        self.value_by_hash = RestoringValueMap({})
        self.value_hash_by_ref_version = {}
        self.ref_versions_by_value_hash = deduped_dict()
        self.observed_value_hashes_by_ref_version = deduped_dict()

        # Index every execution relationship from both ends.
        self.producer_edges_by_exec_id = deduped_dict()
        self.consumed_edges_by_exec_id = deduped_dict()

        # Retain each target-level Effect occurrence and its producer.
        self.effect_by_occurrence = {}
        self.effect_occurrences_by_event_id = deduped_dict()
        self.effect_occurrences_by_type = deduped_dict()
        self.effect_occurrences_by_target = deduped_dict()
        self.effect_occurrences_by_ref_version = deduped_dict()
        self.affected_records_by_effect_occurrence = deduped_dict()
        self.deleted_ref_versions = set()

        

    @staticmethod
    def _concretised_from(dependency):
        if dependency.type == "dynamic":
            return tuple(dependency.expanded_to or ())
        return (dependency,)

    def _register_value(self, value):
        snapshot = (
            value
            if isinstance(value, TIDEValueSnapshot)
            else ensure_tide_snapshot(value, boundary="trace.index")
        )
        value_hash = snapshot.value_hash
        retained = self._value_snapshots_by_hash.get(value_hash)
        if retained is None or (not retained.exact and snapshot.exact):
            self._value_snapshots_by_hash[value_hash] = snapshot
        return value_hash

    def _register_hash_only(self, value_hash, lineage):
        if value_hash in self._value_snapshots_by_hash:
            return
        evidence = next(
            (
                entry
                for entry in lineage or ()
                if isinstance(entry, dict)
                and entry.get("value_hash") == value_hash
            ),
            {},
        )
        self._register_value(TIDEValueSnapshot(
            value_hash=value_hash,
            capture_method="hash",
            type_name=evidence.get("type", "unknown"),
            representation=evidence.get("repr", "<snapshot unavailable>"),
            boundary=evidence.get("boundary", "trace.index.authority"),
            exact=False,
            observed_mutable=bool(evidence.get("observed_mutable", False)),
            trusted=bool(evidence.get("trusted", True)),
        ))

    def _establish_state_value(self, value, ref, version):
        value_hash = self._register_value(value)
        ref_version = (ref, version)
        existing = self.value_hash_by_ref_version.get(ref_version)
        if existing is None:
            self.value_hash_by_ref_version[ref_version] = value_hash
            self.ref_versions_by_value_hash[value_hash].add(ref_version)
        elif existing != value_hash:
            self.observed_value_hashes_by_ref_version[ref_version].add(
                value_hash
            )

    def _observe_state_value(
        self,
        value,
        ref,
        version,
        *,
        authoritative_hash=None,
        lineage=None,
    ):
        value_hash = self._register_value(value)
        ref_version = (ref, version)
        authoritative = self.value_hash_by_ref_version.get(ref_version)
        if authoritative is None:
            # Validation is the sole authority for producerless States.  A
            # first mutable observation may prove an earlier stored identity
            # than the value which that read happened to observe.
            authoritative = authoritative_hash or value_hash
            self._register_hash_only(authoritative, lineage)
            self.value_hash_by_ref_version[ref_version] = authoritative
            self.ref_versions_by_value_hash[authoritative].add(ref_version)
        if authoritative != value_hash:
            self.observed_value_hashes_by_ref_version[ref_version].add(
                value_hash
            )

    def _index_read_as_edge(self, execution, read, state_authorities):
        exec_id = execution.exec_id
        ref = read.unscoped_reference
        version = read.resolved_version
        ref_version = (ref, version)

        self.seen_refs.add(ref)
        self.read_records_by_ref_version[ref_version].add(read)
        self.versions_by_ref[ref].add(version)
        self.consumer_exec_ids_by_ref_version[ref_version].add(exec_id)
        snapshot = (
            read._tide_snapshot("resolved_value")
            if hasattr(read, "_tide_snapshot")
            else None
        )
        self._observe_state_value(
            snapshot or read.resolved_value,
            ref,
            version,
            authoritative_hash=state_authorities.get(ref_version),
            lineage=getattr(read, "snapshot_lineage", ()),
        )

        edge = ReadEdge(execution, read)
        self.producer_edges_by_exec_id[exec_id].add(edge)
        if read.producer_exec_id is not None:
            self.consumed_edges_by_exec_id[read.producer_exec_id].add(edge)

    def _index_write(self, execution, write):
        exec_id = execution.exec_id
        ref = write.stored_target
        version = write.stored_version
        ref_version = (ref, version)

        self.seen_refs.add(ref)
        self.write_records_by_ref_version[ref_version].add(write)
        self.versions_by_ref[ref].add(version)
        self.producer_exec_ids_by_ref_version[ref_version].add(exec_id)
        snapshot = (
            write._tide_snapshot("stored_result")
            if hasattr(write, "_tide_snapshot")
            else None
        )
        self._establish_state_value(
            snapshot or write.stored_result,
            ref,
            version,
        )

    @staticmethod
    def _effect_target(record):
        target = record.targets
        state_effect = record.effect in {
            Effects.READ,
            Effects.WRITE,
            Effects.DELETE,
        }
        if (
            state_effect
            and isinstance(target, tuple)
            and len(target) == 2
            and isinstance(target[0], str)
        ):
            return target[0], target[1], True

        try:
            hash(target)
        except TypeError:
            target = repr(target)

        return target, None, False

    def _index_effect(self, execution, record):
        event_id = record.event_id
        target_position = len(self.effect_occurrences_by_event_id[event_id])
        occurrence = (event_id, target_position)
        effect = record.effect
        target, version, versioned = self._effect_target(record)

        self.effect_by_occurrence[occurrence] = (execution.exec_id, record)
        self.effect_occurrences_by_event_id[event_id].add(occurrence)
        self.effect_occurrences_by_type[effect].add(occurrence)
        self.effect_occurrences_by_target[(effect, target)].add(occurrence)

        if versioned:
            ref_version = (target, version)
            self.seen_refs.add(target)
            self.effect_occurrences_by_ref_version[
                (effect, target, version)
            ].add(occurrence)
            if effect == Effects.DELETE and record.target_existed:
                self.deleted_ref_versions.add(ref_version)
            elif effect == Effects.WRITE:
                self.deleted_ref_versions.discard(ref_version)

        for affected in record.affected.values():
            self._index_effect_as_edge(
                execution,
                record,
                occurrence,
                affected,
            )

        return occurrence

    def _index_effect_as_edge(
        self,
        execution,
        record,
        occurrence,
        affected,
    ):
        self.affected_records_by_effect_occurrence[occurrence].add(affected)
        if affected.affected_exec_id is None:
            return

        target, version, versioned = self._effect_target(record)
        if record.effect == Effects.WRITE and versioned:
            self.consumer_exec_ids_by_ref_version[
                (target, version)
            ].add(affected.affected_exec_id)

        edge = EffectEdge(execution, record, affected)
        self.producer_edges_by_exec_id[affected.affected_exec_id].add(edge)
        self.consumed_edges_by_exec_id[execution.exec_id].add(edge)

    def _inputs_read_from(self, execution, ref_version):
        affected_inputs = []

        for _, input_record in execution.inputs.items():
            for dependency in input_record.dependencies:
                for read in self._concretised_from(dependency):
                    if (
                        read.unscoped_reference,
                        read.resolved_version,
                    ) != ref_version:
                        continue

                    affected_input = {
                        "name": input_record.name,
                        "index": read.input_index,
                    }
                    if affected_input not in affected_inputs:
                        affected_inputs.append(affected_input)

        return affected_inputs

    def _index_write_effect_edges(self, execution, record, occurrence):
        target, version, versioned = self._effect_target(record)
        if record.effect != Effects.WRITE or not versioned:
            return

        existing = {
            affected.affected_exec_id
            for affected in self.affected_records_by_effect_occurrence[
                occurrence
            ]
        }

        for consumer_exec_id in self.consumer_exec_ids_by_ref_version[
            (target, version)
        ]:
            if consumer_exec_id in existing:
                continue

            consumer = self.exec_by_exec_id[consumer_exec_id]
            affected = AffectedRecord(
                Effects.WRITE,
                (target, version),
                consumer.producer_id,
                self._inputs_read_from(consumer, (target, version)),
                consumer_exec_id,
            )
            self._index_effect_as_edge(
                execution,
                record,
                occurrence,
                affected,
            )

    def build(
        self,
        log,
        *,
        state_authorities=None,
        value_backing=None,
        ready_exec_ids=(),
        blocked_exec_ids=(),
    ):
        executions = tuple(sorted(log, key=lambda execution: execution.seq_id))
        state_authorities = dict(state_authorities or {})
        value_backing = dict(value_backing or {})

        # A Bundle's value table is backing storage keyed by the value hash
        # recorded in TideRun. The value itself may disagree with that key;
        # that disagreement is an observation, never a replacement identity.
        for value_hash, value in value_backing.items():
            if not isinstance(value_hash, str):
                raise TypeError("Trace value backing hashes must be strings")
            self._value_snapshots_by_hash[value_hash] = ensure_tide_snapshot(
                value,
                boundary="bundle.import",
            )

        # Seed the exact State identities recorded by the evidence source
        # before inspecting any hydrated Python values.
        for state, value_hash in state_authorities.items():
            if (
                not isinstance(state, tuple)
                or len(state) != 2
            ):
                raise TypeError(
                    "Trace State authority keys must be (reference, version)"
                )
            if not isinstance(value_hash, str):
                raise TypeError("Trace State authority hashes must be strings")

            existing = self.value_hash_by_ref_version.get(state)
            if existing is not None and existing != value_hash:
                raise ValueError(
                    "Trace received conflicting State authorities: "
                    f"{state[0]}[v{state[1]}]"
                )

            self.value_hash_by_ref_version[state] = value_hash
            self.ref_versions_by_value_hash[value_hash].add(state)

            if value_hash not in self._value_snapshots_by_hash:
                self._register_hash_only(value_hash, ())

        self.ready_exec_ids = frozenset(ready_exec_ids or ())
        self.blocked_exec_ids = frozenset(blocked_exec_ids or ())

        for execution in executions:
            exec_id = execution.exec_id
            actor = execution.producer_id
            actor_occurrence_id = execution.producer_occurrence_id

            self.exec_by_exec_id[exec_id] = execution
            self.exec_ids_by_runner_node[
                (execution.runner_id, execution.node_id)
            ].add(exec_id)
            if execution.base_node_id is not None:
                self.exec_ids_by_definition_id[
                    execution.base_node_id
                ].add(exec_id)
            self.actor_to_exec_ids[actor].add(exec_id)
            if actor_occurrence_id is not None:
                self.actor_occurrence_to_exec_id[
                    actor_occurrence_id
                ] = exec_id
            self.func_name_to_actors[actor.source_actor].add(actor)
            parent_exec_id = getattr(execution, "emitted_by_exec_id", None)
            if parent_exec_id is not None:
                self.parent_exec_id_by_exec_id[exec_id] = parent_exec_id
                self.child_exec_ids_by_exec_id[parent_exec_id].add(exec_id)

            for write in execution.writes:
                self._index_write(execution, write)

        for execution in executions:
            for dependency in execution.deps:
                for read in self._concretised_from(dependency):
                    self._index_read_as_edge(
                        execution,
                        read,
                        state_authorities,
                    )

        for execution in executions:
            for record in execution.effects:
                occurrence = self._index_effect(execution, record)
                self._index_write_effect_edges(
                    execution,
                    record,
                    occurrence,
                )

        self.value_by_hash = RestoringValueMap(
            self._value_snapshots_by_hash
        )
        return self

    def seal(self):
        """Freeze a completed index without changing query semantics."""
        multi_maps = (
            "exec_ids_by_runner_node",
            "exec_ids_by_definition_id",
            "actor_to_exec_ids",
            "func_name_to_actors",
            "child_exec_ids_by_exec_id",
            "read_records_by_ref_version",
            "write_records_by_ref_version",
            "versions_by_ref",
            "producer_exec_ids_by_ref_version",
            "consumer_exec_ids_by_ref_version",
            "ref_versions_by_value_hash",
            "observed_value_hashes_by_ref_version",
            "producer_edges_by_exec_id",
            "consumed_edges_by_exec_id",
            "effect_occurrences_by_event_id",
            "effect_occurrences_by_type",
            "effect_occurrences_by_target",
            "effect_occurrences_by_ref_version",
            "affected_records_by_effect_occurrence",
        )
        for name in multi_maps:
            setattr(self, name, _FrozenMultiMap(getattr(self, name)))

        for name in (
            "exec_by_exec_id",
            "actor_occurrence_to_exec_id",
            "parent_exec_id_by_exec_id",
            "value_hash_by_ref_version",
            "effect_by_occurrence",
        ):
            setattr(self, name, MappingProxyType(dict(getattr(self, name))))

        self.seen_refs = tuple(self.seen_refs)
        self.deleted_ref_versions = frozenset(self.deleted_ref_versions)
        self._value_snapshots_by_hash = MappingProxyType(
            dict(self._value_snapshots_by_hash)
        )
        object.__setattr__(self, "_tide_sealed", True)
        return self
