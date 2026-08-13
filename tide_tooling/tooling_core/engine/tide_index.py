"""Indexes exact execution, State-version, value, and effect evidence."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from tide_tooling.tooling_core.engine.classes import DataRef, __TIDE_TEMPLATE_VALUE__
from tide_tooling.tooling_core.engine.events import Effects
from tide_tooling.tooling_core.engine.tide_links import AffectedStackLink, EmittedStackLink, StackLink
from tide_tooling.tooling_core.engine.tide_records import (
    AffectedRecord,
    DependencyUseRecord,
    EffectRecord,
    EmittedNodeRecord,
    ExecRecord,
    WriteRecord,
)
from tide_tooling.tooling_core.engine.utils import DedupedList, deduped_dict, tide_value_hasher

class TraceIndex:
    def __init__(self, ocean_id):
        self.source_ocean_id = ocean_id

        # exec / actor
        self.exec_by_exec_id = {}
        self.actor_to_exec_ids = deduped_dict()
        self.func_name_to_actors = deduped_dict()
        self.actors_by_emitter_actor = deduped_dict()
        self.actor_to_emitter_actors = deduped_dict()
        self.emitted_actor_to_emitter_exec_ids = deduped_dict()

        # reads / writes
        self.reads_by_exec_id = deduped_dict()
        self.writes_by_exec_id = deduped_dict()
        self.ref_ver_to_read_egdes  = deduped_dict()
        self.ref_ver_to_write_egde = deduped_dict()
        self.ref_to_writer_exec_ids = deduped_dict()
        self.ref_to_consumer_exec_ids = deduped_dict()
        self.versions_by_ref = deduped_dict()
        self.seen_refs = DedupedList()

        # ref-version truth
        self.ref_version_to_producer_exec_id = {}
        self.ref_version_to_consumer_exec_ids = deduped_dict()

        # value truth
        self.value_by_hash = {}
        self.value_repr_by_hash = {}
        self.value_hash_by_ref_version = {}
        self.value_hash_to_versioned_refs = deduped_dict()
        self.value_hash_to_producer_exec_ids = deduped_dict()
        self.value_hash_to_consumer_exec_ids = deduped_dict()

        # causal stack edges
        self.input_and_pos_by_exec_input_name_and_ver_ref = defaultdict(lambda: defaultdict(lambda: deduped_dict()))
        self.consumer_exec_id_to_producer_input_edges = deduped_dict()
        self.producer_exec_id_to_consumer_input_edges = deduped_dict()

        # future effects
        self.effects_by_exec_id = deduped_dict()
        self.effect_type_to_affected = deduped_dict()
        self.effect_to_affected = deduped_dict()
        self.effect_versioned_target_to_affected = deduped_dict()
        self.effect_type_to_producer_exec_ids = deduped_dict()
        self.effect_to_producer_exec_ids = deduped_dict()
        self.effect_versioned_target_to_producer_exec_ids = deduped_dict()
        self.effect_type_to_versioned_effects = deduped_dict()
        self.effect_to_touched_exec_ids = deduped_dict()
        self.ref_version_to_effects = deduped_dict()
        self.effect_records_by_type = deduped_dict()
        self.effect_records_by_target = deduped_dict()
        self.effect_records_by_versioned_target = deduped_dict()
        self.effect_affected_input_positions = deduped_dict()
        self.deleted_ref_versions = set()

    def _effects_items(self, effects):
        if hasattr(effects, "items"):
            return effects.items()
        return enumerate(effects or [])

    def _dep_queue(self, record):
        return record.expanded_to if record.type == "dynamic" else [record]

    def _index_value_touch(self, hashed, val, ref, ver):
        self.value_by_hash[hashed] = val
        self.value_repr_by_hash[hashed] = repr(val) 
        self.value_hash_by_ref_version[(ref, ver)] = hashed
        self.value_hash_to_versioned_refs[hashed].add((ref, ver))


    def _index_ref_touch(self, ref, exec_id, actor):
        self.seen_refs.add(ref)

    def _index_input_edge(self, log: ExecRecord, dep_use_record:DependencyUseRecord):
        exec_id = log.exec_id
        producer_exec_id = dep_use_record.producer_exec_id
        ref = dep_use_record.unscoped_reference
        ver = dep_use_record.resolved_version

        input_edge = StackLink({
            "affected_input": dep_use_record.associated_input,
            "affected_input_index": dep_use_record.input_index,
            "affected_actor": log.producer_id,
            "affected_exec_id": exec_id,
            "producer_actor": dep_use_record.producer_actor,
            "producer_exec_id": producer_exec_id,
            "produced_ref": ref,
            "produced_ref_version": ver,
            
        })

        self.consumer_exec_id_to_producer_input_edges[exec_id].append(input_edge)
        if producer_exec_id is not None:
            self.producer_exec_id_to_consumer_input_edges[producer_exec_id].append(input_edge)

    def _index_dep_record(self, log: ExecRecord, dep_use_record: DependencyUseRecord):
        exec_id = log.exec_id
        ref = dep_use_record.unscoped_reference
        ver = dep_use_record.resolved_version
        val = dep_use_record.resolved_value
        hashed = tide_value_hasher(val)

        self._index_input_edge(log, dep_use_record)
        self.input_and_pos_by_exec_input_name_and_ver_ref[exec_id][dep_use_record.associated_input[0]][(ref, ver)].append(dep_use_record.input_index)

        self._index_ref_touch(ref, exec_id, log.producer_id)
        self.ref_to_consumer_exec_ids[ref].add(exec_id)
        self.versions_by_ref[ref].add(ver)

        self.ref_version_to_consumer_exec_ids[(ref, ver)].add(exec_id)

        self._index_value_touch(hashed, val, ref, ver)

        self.value_hash_to_consumer_exec_ids[hashed].add(exec_id)

    def _index_write_record(self, log, record: WriteRecord):
        exec_id = log.exec_id
        ref = record.stored_target
        ver = record.stored_version
        val = record.stored_result
        hashed = tide_value_hasher(val)

        self._index_ref_touch(ref, exec_id, log.producer_id)
        self.ref_to_writer_exec_ids[ref].add(exec_id)

        self.ref_version_to_producer_exec_id[(ref, ver)] = exec_id
        self.versions_by_ref[ref].add(ver)

        self._index_value_touch(hashed, val, ref, ver)
        self.value_hash_to_producer_exec_ids[hashed].add(exec_id)


    def _effect_is_preserved(self, preserved_output: __TIDE_TEMPLATE_VALUE__ | Any):
        return False if isinstance(preserved_output, __TIDE_TEMPLATE_VALUE__) else True

    def _index_affect_record(self, effect, target, ver, actor, affect_record: AffectedRecord):

        self.effect_type_to_affected[effect].append({actor: affect_record})
        self.effect_to_affected[(effect, target)].append({actor: affect_record})
        self.effect_versioned_target_to_affected[(effect, (target, ver))].append({actor: affect_record})

        if effect == Effects.WRITE and affect_record.affected_exec_id:
            self.ref_version_to_consumer_exec_ids[(target, ver)].add(affect_record.affected_exec_id)

    def _affected_inputs_for(self, affectee_log: ExecRecord, targets):
        affected_inputs = DedupedList()

        for _, input_use_record in affectee_log.inputs.items():
            for dependency in input_use_record.dependencies:
                for dep_record in self._dep_queue(dependency):
                    reference = dep_record.unscoped_reference
                    version = dep_record.resolved_version

                    if (reference, version) == targets:
                        affected_inputs.add((input_use_record.name, input_use_record))

        return affected_inputs

    def _index_effect_record(self, log, record):
        record: EffectRecord
        exec_id = log.exec_id
        effect = record.effect
        targets = record.targets
        preserved_output = record.preserved_output

        target = targets[0]
        ver = targets[1]

        if target != "Runner":
            self.seen_refs.add(target)
            if effect == Effects.DELETE:
                self.deleted_ref_versions.add((target, ver))
            elif effect == Effects.WRITE:
                self.deleted_ref_versions.discard((target, ver))

        self.effect_records_by_type[effect].append(record)
        self.effect_records_by_target[(effect, target)].append(record)
        self.effect_records_by_versioned_target[
            (effect, target, ver)
        ].append(record)
        self.effect_type_to_versioned_effects[effect].add(
            (effect, (target, ver))
        )

        preserved_output = self._effect_is_preserved(preserved_output)

        affectees = []

        if effect in [Effects.WRITE] or preserved_output:
            affectees = self.ref_version_to_consumer_exec_ids[(target, ver)]
            self.effect_type_to_versioned_effects[effect].add((effect, (target, ver)))

        indexed_affected_exec_ids = set()
        for actor , affect_record in record.affected.items():
            affect_record: AffectedRecord
            self._index_affect_record(effect, target, ver, actor, affect_record)
            affected_exec_id = affect_record.affected_exec_id
            if affected_exec_id:
                indexed_affected_exec_ids.add(affected_exec_id)
                affected_log = self.exec_by_exec_id.get(affected_exec_id)
                if affected_log is not None:
                    affected_stack_link = AffectedStackLink(
                        log.exec_id,
                        log.producer_id,
                        effect,
                        target,
                        ver,
                        affected_exec_id,
                        affected_log.producer_id,
                        affect_record.affected_inputs,
                    )
                    self.consumer_exec_id_to_producer_input_edges[
                        affected_exec_id
                    ].append(affected_stack_link)
                    self.producer_exec_id_to_consumer_input_edges[
                        log.exec_id
                    ].append(affected_stack_link)
                    self.effect_to_touched_exec_ids[
                        (effect, target)
                    ].add(affected_exec_id)
            self.effect_affected_input_positions[
                (effect, target, ver, affect_record.affected_exec_id)
            ].extend(affect_record.affected_inputs or [])

        for affectee_id in affectees:
            if affectee_id in indexed_affected_exec_ids:
                continue
            affectee_log = self.exec_by_exec_id[affectee_id]
            affected_inputs = self._affected_inputs_for(affectee_log, (target, ver))
            affected = AffectedRecord(effect, (target, ver), affectee_log.producer_id, affected_inputs, affectee_id)

            self.ref_to_consumer_exec_ids[target].add(affectee_id)
            self.ref_version_to_consumer_exec_ids[(target, ver)].add(affectee_id)

            self.effect_to_touched_exec_ids[(effect, target)].add(affectee_id)

            affected_stack_link = AffectedStackLink(
                log.exec_id,
                log.producer_id,
                effect,
                target,
                ver,
                affectee_id,
                affectee_log.producer_id,
                affected_inputs,
            )

            self.consumer_exec_id_to_producer_input_edges[affectee_id].append(affected_stack_link)
            self.producer_exec_id_to_consumer_input_edges[log.exec_id].append(affected_stack_link)

            record.affected[affectee_log.producer_id] = affected
            self.effect_type_to_affected[effect].append({affected.affectee: affected})
            self.effect_to_affected[(effect, target)].append({affected.affectee: affected})
            self.effect_versioned_target_to_affected[(effect, (target, ver))].append({affected.affectee: affected})
            self.effect_affected_input_positions[
                (effect, target, ver, affectee_id)
            ].extend(affected_inputs)

        self.effects_by_exec_id[exec_id].append(record)
        if target != "Runner":
            self.ref_version_to_effects[(target, ver)].add(record)

        self.effect_to_producer_exec_ids[(effect, target)].add(exec_id)
        self.effect_versioned_target_to_producer_exec_ids[(effect, target, ver)].add(exec_id)
        self.effect_to_touched_exec_ids[(effect, target)].add(exec_id)
        self.effect_type_to_producer_exec_ids[effect].add(exec_id)

    def _index_emitted_node_record(self, log:ExecRecord, record: EmittedNodeRecord):
        emitter_actor = record.emitted_by[0]
        self.actors_by_emitter_actor[emitter_actor].add(log.producer_id)
        self.actor_to_emitter_actors[log.producer_id].add(emitter_actor)

    def _index_emitted_by(self, log: ExecRecord):
        if not log.emitted_by:
            return

        emitter_actor = log.emitted_by[0]
        emitter_exec_id = log.emitted_by[1]

        self.actors_by_emitter_actor[emitter_actor].add(log.producer_id)
        self.actor_to_emitter_actors[log.producer_id].add(emitter_actor)
        self.emitted_actor_to_emitter_exec_ids[log.producer_id].add(emitter_exec_id)

        emitter_log = self.exec_by_exec_id.get(emitter_exec_id)
        if emitter_log is None:
            return

        emitted_link = EmittedStackLink(
            emitter_exec_id,
            emitter_log.producer_id,
            log.exec_id,
            log.producer_id,
        )
        self.consumer_exec_id_to_producer_input_edges[log.exec_id].append(
            emitted_link
        )
        self.producer_exec_id_to_consumer_input_edges[emitter_exec_id].append(
            emitted_link
        )

    @staticmethod
    def _runtime_read_found(log, target):
        for runtime_effect in getattr(log, "runtime_effects", ()) or ():
            if not isinstance(runtime_effect, dict):
                continue
            if runtime_effect.get("effect") != Effects.READ:
                continue

            reference = runtime_effect.get("reference")
            if isinstance(reference, DataRef):
                reference = DataRef.to_string(
                    reference,
                    strip_leading=True,
                )
            if reference == target:
                return runtime_effect.get("found")

        return None

    def _link_deleted_runtime_reads(self, exec_logs):
        last_delete_by_ref_version = {}

        for log in exec_logs:
            for _, record in self._effects_items(log.effects):
                target, version = record.targets
                versioned_target = (target, version)

                if record.effect == Effects.DELETE:
                    last_delete_by_ref_version[versioned_target] = record
                    continue

                if record.effect != Effects.READ:
                    continue
                if self._runtime_read_found(log, target) is not False:
                    continue

                delete_record = last_delete_by_ref_version.get(
                    versioned_target
                )
                if delete_record is None:
                    continue

                delete_record.affected[log.producer_id] = AffectedRecord(
                    Effects.DELETE,
                    versioned_target,
                    log.producer_id,
                    ["runtime_read"],
                    log.exec_id,
                )

    def build(self, exec_logs):
        exec_logs = list(exec_logs)

        # Pass one establishes complete producer/consumer truth. Effects can
        # point at executions that occur later in the log, so indexing them in
        # the same forward pass silently missed downstream affectees.
        for idx, log in enumerate(exec_logs):
            log: ExecRecord
            log.seq_id = idx
            exec_id = log.exec_id

            self.exec_by_exec_id[exec_id] = log
            self.reads_by_exec_id[exec_id].extend(log.deps)
            for dep_record in log.deps:
                queue = [dep_record]
                if dep_record.type == "dynamic":
                    queue = [record for record in dep_record.expanded_to]
                        
                for record in queue:
                    ref = record.unscoped_reference
                    ver = record.resolved_version
                    self.ref_ver_to_read_egdes[(ref, ver)].append(record)

            self.writes_by_exec_id[exec_id].extend(log.writes)
            for write_record in log.writes:
                ref = write_record.stored_target
                ver = write_record.stored_version
                self.ref_ver_to_write_egde[(ref, ver)] = write_record

            self.actor_to_exec_ids[log.producer_id].add(exec_id)
            self.func_name_to_actors[log.producer_id.source_actor].add(log.producer_id)

            for record in log.deps:
                record: DependencyUseRecord

                for dep_use_record in self._dep_queue(record):
                    self._index_dep_record(log, dep_use_record)

            for record in log.writes:
                record: WriteRecord
                self._index_write_record(log, record)

            self._index_emitted_by(log)

        # A failed runtime READ after DELETE is causally affected by the
        # deletion, but it is not a successful value dependency.
        self._link_deleted_runtime_reads(exec_logs)

        # Pass two indexes effects against the now-complete dependency graph.
        for log in exec_logs:
            for _, record in self._effects_items(log.effects):
                record: EffectRecord
                self._index_effect_record(log, record)

            for record in log.emitted_nodes:
                self._index_emitted_node_record(log, record)

        return self

