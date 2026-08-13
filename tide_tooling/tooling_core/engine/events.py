from __future__ import annotations

from enum import StrEnum
from typing import Any, Callable
from dataclasses import dataclass, field
from uuid import uuid4
import time
from tide_tooling.tooling_core import debug_print as print


def event_type_is(value, event_enum):
    """Return whether *value* is a member or value of an event Enum.

    ``value in SomeEnum`` raises ``TypeError`` for raw values on Python 3.11,
    while Python 3.12+ accepts them. Converting explicitly keeps routing
    semantics identical across every supported Python version.
    """
    try:
        event_enum(value)
    except (TypeError, ValueError):
        return False
    return True


# from tide_tooling.tooling_core.engine.tide_logging import Actor
class Event:

    _event_counter = 0 
    
    def __init__(self, source:str=None, source_id:int=None, target_runner_id:int=None, event_type:Event=None, payload:Any=None, exec_id:int=None):
        try:
            self.id = Event._event_counter 
            Event._event_counter  += 1

            self.source = source
            self.source_id = source_id

            self.target_runner_id = target_runner_id
            # if not self.target_runner_id:
            #     raise RuntimeError()
           
            self.event_type = event_type
            self.payload = payload
            self.exec_id = exec_id
            self.creation_time = time.time()
        except Exception:
            print(f"EventInitError({source},{source_id},{target_runner_id},{event_type})")
            raise 

@dataclass
class ExecEvent:
    id: int = field(init=False)

    actor: str | Callable
    reads: dict[str, Any]          # ref -> value
    writes: dict[str, Any]         # ref -> value
    uuid: str = field(init=False, default_factory=lambda:uuid4().hex)
    source: str = "TIDE: .trace()"
    cause: str = "unknown"
    status: str = "succeeded"
    effects: list[Any] = field(default_factory=list)
    emitted: list[Any] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    seq_id: int = None

class OceanEvents(StrEnum):
    # Pipeline Controls
    REGISTER_PIPELINE = "register_pipeline"
    RUN_PIPELINE = "start_runner"
    KILL_PIPELINE = "kill_pipeline"

    # Store controls
    SET = "set"
    DELETE = "delete"

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
    
    PRINT = "print"
    WAITING = "print_waiting"
    DONE = "done"
    KILL = "kill"

class ExecutorEvents(StrEnum):
    NODE_READY = "node_ready"

class RouterEvents(StrEnum):
    STORE_WRITE = "store_write"
    FORCE_WRITE = "force_write"
    DELETE = "delete_target"

class LogEvents(StrEnum):
    NODE_EMITTED = "node_emitted"
    NODE_BLOCKED = "node_blocked"
    DEP_CONCRETISED = "dep_concretised"
    EFFECT = "effect"
    EVENT_EXEC = "event_exec"
    MISSING_REFS = "missing_ref_error"
    READ='read'

class Effects(StrEnum):
    END = "END"
    DELETE = "DELETE"
    RUN = "run"
    WRITE = "write"
    READ = "read"
    EMIT = "emit"
    RAISE = "raise"
    
class Causes(StrEnum):
    NODE = "cause:node"
    EFFECT = "cause:effect"
