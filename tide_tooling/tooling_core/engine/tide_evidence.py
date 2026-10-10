"""Sealed in-memory evidence owned by a :class:`Trace`.

Reconstruction deliberately uses mutable builders.  A Trace does not: it
seals their structure once and retains runtime values as the exact snapshots
which established their identities.  Public value reads restore a caller-
owned value from that snapshot instead of exposing retained history.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType

from .utils import TIDEValueSnapshot, ensure_tide_snapshot, tide_value_hasher


class SealableEvidence:
    """Mutable while reconstructed; structurally immutable once Trace-owned."""

    _tide_value_fields = frozenset()

    def __getattribute__(self, name):
        value = object.__getattribute__(self, name)
        if (
            name in object.__getattribute__(self, "_tide_value_fields")
            and isinstance(value, TIDEValueSnapshot)
        ):
            return value.restore()
        return value

    def __setattr__(self, name, value):
        if getattr(self, "_tide_sealed", False):
            raise AttributeError(
                f"{type(self).__name__} is sealed Trace evidence"
            )
        object.__setattr__(self, name, value)

    def _tide_snapshot(self, name):
        value = object.__getattribute__(self, name)
        return value if isinstance(value, TIDEValueSnapshot) else None

    def _tide_seal_evidence(self, store, memo):
        for name, value in tuple(vars(self).items()):
            if name in self._tide_value_fields:
                value = store.register(value, boundary=f"trace.{name}")
            else:
                value = _freeze_structure(value, store, memo)
            object.__setattr__(self, name, value)
        object.__setattr__(self, "_tide_sealed", True)
        return self


class FrozenList(list):
    """Shell-allocatable list evidence which rejects public mutation."""

    @staticmethod
    def _immutable(*args, **kwargs):
        raise TypeError("sealed Trace evidence sequence is immutable")

    __setitem__ = _immutable
    __delitem__ = _immutable
    append = _immutable
    clear = _immutable
    extend = _immutable
    insert = _immutable
    pop = _immutable
    remove = _immutable
    reverse = _immutable
    sort = _immutable

    def __iadd__(self, other):
        return self._immutable(other)

    def __imul__(self, other):
        return self._immutable(other)

    def __deepcopy__(self, memo):
        return self


class FrozenDict(dict):
    """Dict-shaped structural evidence which rejects every mutation."""

    @staticmethod
    def _immutable(*args, **kwargs):
        raise TypeError("sealed Trace evidence mapping is immutable")

    __setitem__ = _immutable
    __delitem__ = _immutable
    clear = _immutable
    pop = _immutable
    popitem = _immutable
    setdefault = _immutable
    update = _immutable

    def __deepcopy__(self, memo):
        return self


class RestoringValueMap(Mapping):
    """Read-only value-hash mapping which never exposes retained evidence."""

    def __init__(self, snapshots):
        self._snapshots = MappingProxyType(dict(snapshots))

    def __getitem__(self, value_hash):
        return self._snapshots[value_hash].restore()

    def __iter__(self):
        return iter(self._snapshots)

    def __len__(self):
        return len(self._snapshots)

    def get_snapshot(self, value_hash):
        return self._snapshots.get(value_hash)


class TraceEvidenceStore:
    """Own and seal one Trace's execution-value evidence."""

    def __init__(self):
        self._by_identity = {}
        self._by_hash = {}

    @classmethod
    def from_value_bundles(cls, values, value_bundles, *, trusted=False):
        store = cls()
        for value_hash, value in values.items():
            value_bundle = value_bundles.get(value_hash)
            if value_bundle is None:
                continue
            snapshot = TIDEValueSnapshot(
                value_hash=value_hash,
                value_bundle=value_bundle,
                capture_method="serialised",
                type_name=type(value).__name__,
                representation=repr(value)[:1024],
                boundary="bundle.import",
                exact=True,
                trusted=trusted,
            )
            store._by_hash[value_hash] = snapshot
            store._by_identity[id(value)] = (value, snapshot)
        return store

    def register(self, value, *, boundary="trace"):
        if isinstance(value, TIDEValueSnapshot):
            snapshot = value if value.boundary != "unknown" else value.at_boundary(boundary)
        else:
            identity = id(value)
            cached = self._by_identity.get(identity)
            if cached is not None:
                original, snapshot = cached
                if original is not value:
                    raise RuntimeError("Trace evidence value identity collision")
                return snapshot
            snapshot = ensure_tide_snapshot(value, boundary=boundary)
            # Retain the original only while this store lives so Python cannot
            # recycle its id into another registration.
            self._by_identity[identity] = (value, snapshot)

        existing = self._by_hash.get(snapshot.value_hash)
        if existing is None:
            self._by_hash[snapshot.value_hash] = snapshot
        return self._by_hash[snapshot.value_hash]

    def snapshot(self, value_hash):
        return self._by_hash[value_hash]

    def restore(self, value_hash):
        return self.snapshot(value_hash).restore()

    @property
    def values(self):
        return RestoringValueMap(self._by_hash)


def _freeze_structure(value, store, memo):
    identity = id(value)
    if identity in memo:
        return memo[identity]

    if getattr(value, "_tide_sealed", False):
        memo[identity] = value
        return value
    if hasattr(value, "_tide_seal_evidence"):
        memo[identity] = value
        value._tide_seal_evidence(store, memo)
        return value
    if type(value) is list:
        frozen = FrozenList()
        memo[identity] = frozen
        for item in value:
            list.append(frozen, _freeze_structure(item, store, memo))
        return frozen
    if isinstance(value, dict):
        frozen = FrozenDict()
        memo[identity] = frozen
        effect_class = getattr(value, "_tide_effect_class", None)
        if effect_class is not None:
            frozen._tide_effect_class = effect_class
        if getattr(value, "_tide_node_definition", False):
            frozen._tide_node_definition = True
        for key, item in value.items():
            dict.__setitem__(
                frozen,
                _freeze_structure(key, store, memo),
                _freeze_structure(item, store, memo),
            )
        return frozen
    if type(value) is set:
        frozen = frozenset(_freeze_structure(item, store, memo) for item in value)
        memo[identity] = frozen
        return frozen
    if type(value) is tuple:
        frozen = tuple(_freeze_structure(item, store, memo) for item in value)
        memo[identity] = frozen
        return frozen
    return value


def seal_exec_records(records, store):
    """Seal already-owned reconstruction records without cloning values."""
    memo = {}
    return tuple(_freeze_structure(record, store, memo) for record in records)


def _affected_input_is_recorded(record, affected_input, target):
    if not isinstance(affected_input, dict):
        return False
    expected_name = affected_input.get("name")
    expected_index = affected_input.get("index")
    target_state = (
        target
        if isinstance(target, tuple) and len(target) == 2
        else None
    )
    for dependency in record.deps or ():
        concrete = (
            dependency.expanded_to or ()
            if dependency.type == "dynamic"
            else (dependency,)
        )
        for read in concrete:
            associated = read.associated_input
            input_name = (
                associated[0]
                if isinstance(associated, tuple) and associated
                else None
            )
            if input_name != expected_name:
                continue
            if read.input_index != expected_index:
                continue
            if target_state is not None and (
                read.unscoped_reference,
                read.resolved_version,
            ) != target_state:
                continue
            return True
    if expected_name == "runtime_read" and target_state is not None:
        target_reference, _ = target_state
        for effect in record.effects or ():
            effect_kind = getattr(effect.effect, "value", effect.effect)
            effect_target = effect.targets
            if (
                effect_kind == "read"
                and isinstance(effect_target, tuple)
                and effect_target
                and effect_target[0] == target_reference
                and effect.target_existed is False
            ):
                return True
    return False


def validate_state_observation(
    *,
    state,
    authoritative_hash,
    previous_hash,
    observation_hash,
    observed_mutable,
    snapshot_lineage,
    label="Trace",
):
    """Validate one used-input observation against its State history.

    The stored State hash is authoritative.  Producerless evidence establishes
    that authority from its first observation (or the start of its first
    explicit mutation lineage).  Every later transition must continue from the
    last proven observation; the mutable flag alone never authorises a change.
    """
    lineage = list(snapshot_lineage or ())
    lineage_start = (
        lineage[0].get("value_hash")
        if lineage and isinstance(lineage[0], dict)
        else None
    )
    lineage_end = (
        lineage[-1].get("value_hash")
        if lineage and isinstance(lineage[-1], dict)
        else None
    )

    if authoritative_hash is None:
        authoritative_hash = (
            lineage_start
            if observed_mutable and len(lineage) >= 2
            else observation_hash
        )
    if previous_hash is None:
        previous_hash = authoritative_hash

    transition_claimed = observed_mutable or observation_hash != previous_hash
    if not transition_claimed:
        return authoritative_hash, observation_hash

    valid_lineage = (
        observed_mutable
        and len(lineage) >= 2
        and lineage_start == previous_hash
        # A read may record the observation before an in-place mutation (the
        # lineage start), or the newly observed value (the lineage end).
        and observation_hash in {lineage_start, lineage_end}
    )
    if not valid_lineage:
        raise ValueError(
            f"{label} read value disagrees with its State without valid "
            "mutable snapshot lineage: {state[0]}[v{state[1]}]"
        )
    return authoritative_hash, lineage_end


def validate_state_observations(
    observations,
    *,
    authoritative_hash_by_state=None,
    label="Trace",
):
    """Validate State observations in their recorded observation chronology.

    Execution ``seq_id`` is completion order and is not a read clock.  Mutable
    histories with more than one observation therefore require the explicit
    observation IDs captured while the evidence source was consumed.
    """
    authoritative = dict(authoritative_hash_by_state or {})
    latest = dict(authoritative)
    by_state = {}
    used_ids = set()

    for observation in observations:
        state = observation["state"]
        observation_id = observation.get("observation_id")
        if observation_id is not None:
            if (
                not isinstance(observation_id, int)
                or isinstance(observation_id, bool)
                or observation_id < 0
            ):
                raise TypeError(
                    f"{label} observation_id must be a non-negative integer"
                )
            if observation_id in used_ids:
                raise ValueError(
                    f"Duplicate {label} observation_id: {observation_id}"
                )
            used_ids.add(observation_id)
        by_state.setdefault(state, []).append(observation)

    for state, state_observations in by_state.items():
        ordered_history = (
            len(state_observations) > 1
            and any(
                observation["observed_mutable"]
                for observation in state_observations
            )
        )
        if ordered_history and any(
            observation.get("observation_id") is None
            for observation in state_observations
        ):
            raise ValueError(
                f"{label} mutable State history lacks observation order: "
                f"{state[0]}[v{state[1]}]"
            )
        if all(
            observation.get("observation_id") is not None
            for observation in state_observations
        ):
            state_observations = sorted(
                state_observations,
                key=lambda observation: observation["observation_id"],
            )

        current_authority = authoritative.get(state)
        previous = latest.get(state)
        for observation in state_observations:
            current_authority, previous = validate_state_observation(
                state=state,
                authoritative_hash=current_authority,
                previous_hash=previous,
                observation_hash=observation["observation_hash"],
                observed_mutable=observation["observed_mutable"],
                snapshot_lineage=observation["snapshot_lineage"],
                label=label,
            )
        authoritative[state] = current_authority
        latest[state] = previous

    return authoritative, latest


def validate_exec_record_relationships(
    exec_logs,
    *,
    label="Trace",
    require_closed=False,
    validate_state_values=True,
    record_log=(),
    ready_attempts=(),
):
    """Validate every relationship whose endpoints this evidence represents.

    A focused Trace may retain a relationship to an execution outside its
    selected log. A complete persisted TideRun may not.
    """
    exec_ids = set()
    seq_ids = set()
    occurrences = set()
    records_by_exec_id = {}
    producers_by_state = {}
    value_hashes_by_state = {}
    effects_by_event_id = {}

    def evidence_hash(owner, field):
        snapshot = (
            owner._tide_snapshot(field)
            if hasattr(owner, "_tide_snapshot")
            else None
        )
        if snapshot is not None:
            return snapshot.value_hash
        return tide_value_hasher(getattr(owner, field))

    for record in exec_logs:
        if record.exec_id in exec_ids:
            raise ValueError(f"Duplicate {label} exec_id: {record.exec_id!r}")
        exec_ids.add(record.exec_id)
        records_by_exec_id[record.exec_id] = record
        if record.seq_id in seq_ids:
            raise ValueError(f"Duplicate {label} seq_id: {record.seq_id!r}")
        seq_ids.add(record.seq_id)
        occurrence = getattr(record, "producer_occurrence_id", None)
        if occurrence is not None:
            if occurrence in occurrences:
                raise ValueError(
                    f"Duplicate {label} ActorOccurrence: {occurrence!r}"
                )
            occurrences.add(occurrence)
        for write in getattr(record, "writes", ()) or ():
            state = (write.stored_target, write.stored_version)
            producers_by_state.setdefault(state, set()).add(record.exec_id)
            if validate_state_values:
                value_hashes_by_state.setdefault(state, set()).add(
                    evidence_hash(write, "stored_result")
                )
        for effect in getattr(record, "effects", ()) or ():
            effects_by_event_id.setdefault(effect.event_id, []).append(
                (record.exec_id, effect)
            )

    for state, value_hashes in value_hashes_by_state.items():
        if len(value_hashes) > 1:
            raise ValueError(
                f"{label} writes disagree for State: "
                f"{state[0]}[v{state[1]}]"
            )

    for event_id, occurrences in effects_by_event_id.items():
        first_exec_id, first = occurrences[0]
        expected = (
            first_exec_id,
            first.effect,
            first.origin,
            first.immediate,
            evidence_hash(first, "output"),
        )
        for exec_id, effect in occurrences[1:]:
            actual = (
                exec_id,
                effect.effect,
                effect.origin,
                effect.immediate,
                evidence_hash(effect, "output"),
            )
            if actual != expected:
                raise ValueError(
                    f"{label} Effect records disagree for Event: {event_id}"
                )

    def require_exec(identity, relationship):
        if identity is None:
            return False
        if identity not in exec_ids and require_closed:
            raise ValueError(
                f"{label} {relationship} references unknown execution: "
                f"{identity!r}"
            )
        return identity in exec_ids

    attempts_by_exec_id = {}
    for attempt in ready_attempts:
        if attempt.exec_id in attempts_by_exec_id:
            raise ValueError(
                f"Duplicate {label} ReadyAttempt exec_id: {attempt.exec_id!r}"
            )
        attempts_by_exec_id[attempt.exec_id] = attempt

        if attempt.outcome not in {"ready", "blocked", "completed"}:
            raise ValueError(
                f"{label} ReadyAttempt has invalid outcome: "
                f"{attempt.outcome!r}"
            )
        if attempt.actor.source_runner != attempt.runner_id:
            raise ValueError(
                f"{label} ReadyAttempt Actor does not match runner_id"
            )

        node_snapshot = attempt.node_snapshot
        if isinstance(node_snapshot, Mapping):
            def snapshot_value(name):
                value = node_snapshot.get(name)
                return (
                    value.restore()
                    if isinstance(value, TIDEValueSnapshot)
                    else value
                )

            expected_snapshot_facts = {
                "runner_id": attempt.runner_id,
                "pipeline_id": attempt.pipeline_id,
                "func_name": attempt.actor.source_actor,
                "base_node_id": attempt.node_definition_id,
            }
            for name, expected in expected_snapshot_facts.items():
                if name in node_snapshot and snapshot_value(name) != expected:
                    raise ValueError(
                        f"{label} ReadyAttempt {name} disagrees with its "
                        "node_snapshot"
                    )

        execution = records_by_exec_id.get(attempt.exec_id)
        if execution is None:
            if attempt.outcome == "completed" and require_closed:
                raise ValueError(
                    f"{label} completed ReadyAttempt references unknown "
                    f"execution: {attempt.exec_id!r}"
                )
            continue

        execution_is_ambiguous = bool(
            getattr(execution, "ambiguous", False)
        )
        expected_outcome = (
            "ready" if execution_is_ambiguous else "completed"
        )
        if attempt.outcome != expected_outcome:
            raise ValueError(
                f"{label} represented execution disagrees with its "
                "ReadyAttempt outcome"
            )
        if attempt.actor != execution.producer_id:
            raise ValueError(
                f"{label} ReadyAttempt Actor does not match execution"
            )
        if attempt.runner_id != execution.runner_id:
            raise ValueError(
                f"{label} ReadyAttempt runner does not match execution"
            )
        if attempt.pipeline_id != execution.pipeline_id:
            raise ValueError(
                f"{label} ReadyAttempt pipeline does not match execution"
            )
        if (
            attempt.node_definition_id is not None
            and attempt.node_definition_id
            != getattr(execution, "base_node_id", None)
        ):
            raise ValueError(
                f"{label} ReadyAttempt definition does not match execution"
            )

    for record in record_log:
        reference = getattr(record, "execution_reference", None)
        if reference is None:
            continue

        if record.exec_id != reference.exec_id:
            raise ValueError(
                f"{label} Immediate Effect owner exec_id disagrees"
            )

        ambiguous = reference.ambiguous_occurrence
        if ambiguous is not None:
            if ambiguous.exec_id != reference.exec_id:
                raise ValueError(
                    f"{label} ambiguous immediate occurrence exec_id "
                    "disagrees"
                )
            if reference.actor is not None and ambiguous.actor != reference.actor:
                raise ValueError(
                    f"{label} ambiguous immediate occurrence Actor disagrees"
                )

        if reference.resolved:
            if reference.resolved_actor_occurrence is None:
                raise ValueError(
                    f"{label} resolved immediate reference has no "
                    "ActorOccurrence"
                )
            if not require_exec(
                reference.exec_id,
                "resolved immediate execution",
            ):
                continue

            execution = records_by_exec_id[reference.exec_id]
            if getattr(execution, "ambiguous", False):
                raise ValueError(
                    f"{label} resolved immediate reference points to an "
                    "unresolved execution"
                )
            if reference.resolved_seq_id != execution.seq_id:
                raise ValueError(
                    f"{label} immediate reference resolved_seq_id does not "
                    "match execution"
                )
            if (
                reference.resolved_actor_occurrence
                != execution.producer_occurrence_id
            ):
                raise ValueError(
                    f"{label} immediate reference ActorOccurrence does not "
                    "match execution"
                )
            if reference.actor is not None and reference.actor != execution.producer_id:
                raise ValueError(
                    f"{label} immediate reference Actor does not match "
                    "execution"
                )
            if (
                reference.node_definition_id is not None
                and reference.node_definition_id
                != getattr(execution, "base_node_id", None)
            ):
                raise ValueError(
                    f"{label} immediate reference definition does not "
                    "match execution"
                )
        else:
            if reference.resolved_actor_occurrence is not None:
                raise ValueError(
                    f"{label} unresolved immediate reference carries a "
                    "resolved ActorOccurrence"
                )
            represented = records_by_exec_id.get(reference.exec_id)
            if represented is not None and not getattr(
                represented,
                "ambiguous",
                False,
            ):
                raise ValueError(
                    f"{label} immediate reference remains unresolved despite "
                    "its represented execution"
                )

        attempt = attempts_by_exec_id.get(reference.exec_id)
        if attempt is not None:
            if reference.actor is not None and reference.actor != attempt.actor:
                raise ValueError(
                    f"{label} immediate reference Actor does not match "
                    "ReadyAttempt"
                )
            if (
                reference.node_definition_id is not None
                and reference.node_definition_id != attempt.node_definition_id
            ):
                raise ValueError(
                    f"{label} immediate reference definition does not match "
                    "ReadyAttempt"
                )

    authoritative_hash_by_state = {
        state: next(iter(value_hashes))
        for state, value_hashes in value_hashes_by_state.items()
        if value_hashes
    }
    state_observations = []

    for record in exec_logs:
        emitted_by_exec_id = getattr(record, "emitted_by_exec_id", None)
        emitter_is_local = require_exec(
            emitted_by_exec_id,
            "emitted_by_exec_id",
        )
        if emitted_by_exec_id is not None and emitter_is_local:
            emitter = records_by_exec_id[emitted_by_exec_id]
            if not any(
                emitted.emitted_by_exec_id == emitter.exec_id
                and emitted.runner_id == record.runner_id
                and emitted.node_id == record.node_id
                for emitted in getattr(emitter, "emitted_nodes", ()) or ()
            ):
                raise ValueError(
                    f"{label} emitted execution is not owned by its "
                    "claimed emitter"
                )
        for dependency in getattr(record, "deps", ()) or ():
            concrete = (
                dependency.expanded_to or ()
                if dependency.type == "dynamic"
                else (dependency,)
            )
            for read in concrete:
                producer_is_local = require_exec(
                    read.producer_exec_id,
                    "producer_exec_id",
                )
                state = (read.unscoped_reference, read.resolved_version)
                producers = producers_by_state.get(state, set())
                if read.producer_exec_id is None:
                    if producers:
                        raise ValueError(
                            f"{label} read erased the producer of its State: "
                            f"{state[0]}[v{state[1]}]"
                        )
                    if (
                        getattr(read, "producer_actor", None) is not None
                        or getattr(read, "producer_source", None) is not None
                    ):
                        raise ValueError(
                            f"{label} external read has partial producer identity"
                        )
                elif producer_is_local:
                    if read.producer_exec_id not in producers:
                        raise ValueError(
                            f"{label} producer_exec_id did not write its claimed "
                            f"State: {state[0]}[v{state[1]}]"
                        )
                    producer = records_by_exec_id[read.producer_exec_id]
                    if read.producer_actor != producer.producer_id:
                        raise ValueError(
                            f"{label} producer_actor does not match producer_exec_id"
                        )
                    if read.producer_source != producer.runner_id:
                        raise ValueError(
                            f"{label} producer_source does not match producer_exec_id"
                        )
                if validate_state_values:
                    state_observations.append({
                        "state": state,
                        "observation_id": getattr(
                            read,
                            "observation_id",
                            None,
                        ),
                        "observation_hash": evidence_hash(
                            read,
                            "resolved_value",
                        ),
                        "observed_mutable": bool(
                            getattr(read, "observed_mutable", False)
                        ),
                        "snapshot_lineage": list(
                            getattr(read, "snapshot_lineage", ()) or ()
                        ),
                    })
        for effect in getattr(record, "effects", ()) or ():
            for affected in effect.affected.values():
                if (
                    affected.affected_exec_id is None
                    and require_closed
                    and any(
                        candidate.producer_id == affected.affectee
                        and all(
                            _affected_input_is_recorded(
                                candidate,
                                affected_input,
                                effect.targets,
                            )
                            for affected_input in affected.affected_inputs
                        )
                        for candidate in records_by_exec_id.values()
                    )
                ):
                    raise ValueError(
                        f"{label} affected execution identity was erased"
                    )
                affected_is_local = require_exec(
                    affected.affected_exec_id,
                    "affected_exec_id",
                )
                if (
                    affected.affected_exec_id is not None
                    and affected_is_local
                ):
                    affected_record = records_by_exec_id[
                        affected.affected_exec_id
                    ]
                    if affected_record.producer_id != affected.affectee:
                        raise ValueError(
                            f"{label} affected_exec_id does not match the "
                            "recorded affected Actor"
                        )
                    for affected_input in affected.affected_inputs:
                        if not _affected_input_is_recorded(
                            affected_record,
                            affected_input,
                            effect.targets,
                        ):
                            raise ValueError(
                                f"{label} affected execution does not contain "
                                "the claimed affected input"
                            )
        for emitted in getattr(record, "emitted_nodes", ()) or ():
            if emitted.emitted_by_exec_id is None and require_closed:
                raise ValueError(
                    f"{label} emitted node owner identity was erased"
                )
            owner_is_local = require_exec(
                emitted.emitted_by_exec_id,
                "emitted node identity",
            )
            if owner_is_local:
                owner = records_by_exec_id[emitted.emitted_by_exec_id]
                if not any(
                    candidate.emitted_by_exec_id == owner.exec_id
                    and candidate.runner_id == emitted.runner_id
                    and candidate.node_id == emitted.node_id
                    and candidate.content_id == emitted.content_id
                    for candidate in getattr(owner, "emitted_nodes", ()) or ()
                ):
                    raise ValueError(
                        f"{label} emitted node was not registered by its "
                        "claimed execution"
                    )

    if validate_state_values:
        authoritative_hash_by_state, _ = validate_state_observations(
            state_observations,
            authoritative_hash_by_state=authoritative_hash_by_state,
            label=label,
        )
    return authoritative_hash_by_state
