from __future__ import annotations

from enum import StrEnum
from typing import Any
from dataclasses import KW_ONLY, dataclass, field
from uuid import uuid4
import time


@dataclass(frozen=True)
class Cause:
    event_id: str | None = None
    description: str | None = None

    def __post_init__(self):
        if self.description is None:
            raise ValueError("Cause requires a description")

    @classmethod
    def from_event(cls, event):
        return cls(event.event_id, str(event.type))


@dataclass(frozen=True)
class Event:
    source: str
    source_id: int | str
    type: StrEnum
    cause: Cause

    _: KW_ONLY
    ocean_id: int | str

    target_runner_id: int | str | None = None
    payload: Any = None
    exec_id: str | None = None
    host: str | None = None

    event_id: str = field(init=False, default_factory=lambda: uuid4().hex)
    creation_time: float = field(init=False, default_factory=time.time)


def _causal_events(events, root):
    # Select the root and every Event causally descended from it.
    causal_ids = {root.event_id}
    selected = []

    for event in events:
        if (
            event.event_id == root.event_id
            or event.cause.event_id in causal_ids
        ):
            causal_ids.add(event.event_id)
            selected.append(event)

    return tuple(selected)

class OceanEvents(StrEnum):
    # Pipeline Controls
    REGISTER_PIPELINE = "register_pipeline"
    RUN_PIPELINE = "start_runner"
    KILL_RUNNER = "kill_runner"

    # Ocean controls
    QUIT = "quit"

class PipelineEvents(StrEnum):
    DEP_AVAILABLE = "dependency_available"
    DEP_UPDATED = "dependency_updated"
    DEP_DELETED = "dependency_deleted"
    
    BUILD = "build"
    REGISTER_NODE = "register_node"
    RUN_NODE = "run_node"
            
    RESULT_READY = "result_ready"

class ExecutorEvents(StrEnum):
    NODE_READY = "node_ready"

class RouterEvents(StrEnum):
    STORE_WRITE = "store_write"
    FORCE_WRITE = "force_write"
    DELETE = "delete_target"

class LogEvents(StrEnum):
    RUNNER_PLAN = "runner_plan"
    NODE_EMITTED = "node_emitted"
    NODE_BLOCKED = "node_blocked"
    RUNNER_DEADLOCK = "runner_deadlock"
    DEP_CONCRETISED = "dep_concretised"
    EFFECT = "effect"
    EVENT_EXEC = "event_exec"
    MISSING_REFS = "missing_ref_error"

class Effects(StrEnum):
    END = "END"
    DELETE = "DELETE"
    RUN = "run"
    WRITE = "write"
    READ = "read"
    EMIT = "emit"
    RAISE = "raise"
