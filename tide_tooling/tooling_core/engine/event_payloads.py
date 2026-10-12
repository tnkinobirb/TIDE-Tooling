from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Literal, NamedTuple

if TYPE_CHECKING:
    from .classes import DataRef, EffectRuntime, Node, Pipeline
    from .events import Effects
    from .executor import ExecMeta
    from .utils import TIDEValueSnapshot


class PipelineRegistration(NamedTuple):
    pipeline: Pipeline
    local_store_templates: dict[str, Any]


class PipelineRun(NamedTuple):
    pipeline_id: Any
    pipeline_name: str | None
    mode: str
    local_store_templates: dict[str, Any]
    stay_alive: bool
    runner_name: str | None
    dynamic_ref_plan: Any


class RunnerPlan(NamedTuple):
    tide_plan: dict[str, Any] | None


class State(NamedTuple):
    ref: DataRef
    value: Any
    snapshot: TIDEValueSnapshot
    is_poll: bool = False
    value_type: type | None = None


class Reference(NamedTuple):
    ref: DataRef


class Nodes(NamedTuple):
    nodes: tuple[Node, ...]
    node_snapshots: tuple[dict[str, Any], ...]


class NodeSnapshot(NamedTuple):
    node_id: Any
    snapshot: dict[str, Any]


class Result(NamedTuple):
    exec_meta: ExecMeta


class Work(NamedTuple):
    function: Callable[..., Any]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    mode: str
    effect_runtime: EffectRuntime
    node_snapshot: dict[str, Any]


class EffectOccurrence(NamedTuple):
    effect: Effects
    targets: tuple[Any, ...]
    output: Any
    origin: Literal["declared", "runtime"]
    immediate: bool = False
    found: tuple[bool, ...] | None = None


class EmittedNodes(NamedTuple):
    emitted_node_snapshots: tuple[dict[str, Any], ...]


class NodeBlocked(NamedTuple):
    ref: DataRef
    input_names: tuple[str, ...]
    node_snapshot: dict[str, Any]


class DependencyConcretised(NamedTuple):
    input_name: str
    dynamic_ref: DataRef
    concrete_refs: tuple[DataRef, ...]
    node_snapshot: dict[str, Any]


class MissingReferences(NamedTuple):
    missing_ref: DataRef
    available_refs: tuple[DataRef, ...]
    node_snapshot: dict[str, Any]


# The payload contract belongs to Events. Replay and interchange consume this
# schema; neither owns or duplicates it.
from .events import (
    ExecutorEvents,
    LogEvents,
    OceanEvents,
    PipelineEvents,
    RouterEvents,
)
from .tide_records import ExecRecord


EVENT_PAYLOAD_TYPES = {
    OceanEvents.REGISTER_PIPELINE: PipelineRegistration,
    OceanEvents.RUN_PIPELINE: PipelineRun,
    OceanEvents.KILL_RUNNER: type(None),
    OceanEvents.QUIT: type(None),
    PipelineEvents.DEP_AVAILABLE: State,
    PipelineEvents.DEP_UPDATED: State,
    PipelineEvents.DEP_DELETED: Reference,
    PipelineEvents.BUILD: Reference,
    PipelineEvents.REGISTER_NODE: Nodes,
    PipelineEvents.RUN_NODE: NodeSnapshot,
    PipelineEvents.RESULT_READY: Result,
    ExecutorEvents.NODE_READY: Work,
    RouterEvents.STORE_WRITE: State,
    RouterEvents.FORCE_WRITE: State,
    RouterEvents.DELETE: Reference,
    LogEvents.EFFECT: EffectOccurrence,
    LogEvents.EVENT_EXEC: ExecRecord,
    LogEvents.NODE_EMITTED: EmittedNodes,
    LogEvents.DEP_CONCRETISED: DependencyConcretised,
    LogEvents.NODE_BLOCKED: NodeBlocked,
    LogEvents.RUNNER_DEADLOCK: dict,
    LogEvents.RUNNER_PLAN: RunnerPlan,
    LogEvents.MISSING_REFS: MissingReferences,
}


def validate_event_payload(event_type, payload):
    expected = EVENT_PAYLOAD_TYPES.get(event_type)
    if expected is None:
        raise TypeError(f"Unsupported Event type: {event_type}")
    if not isinstance(payload, expected):
        raise TypeError(
            f"{event_type} requires {expected.__name__}, "
            f"got {type(payload).__name__}"
        )
