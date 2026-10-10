from __future__ import annotations

from dataclasses import dataclass, field
from contextvars import ContextVar
from typing import Callable, DefaultDict, List, Self, Dict, Any, Hashable, Iterable, ClassVar
from .errors import NodeDefinitionError, RuntimeDagConsumptionError, DataRefInitError, MissingDepKeyError, MissingStoreError, ReadInsideProcessPoolError, WriteNowInsideProcessPoolError, DeleteNowInsideProcessPoolError
from collections import deque, defaultdict
from tide_tooling.tooling_core.engine.events import Event 
from warnings import warn
from tide_tooling.tooling_core.engine.utils import DedupedList
from tide_tooling.tooling_core.engine.tide_evidence import SealableEvidence
import copy
import time
from enum import Enum, StrEnum
from .events import RouterEvents, PipelineEvents, ExecutorEvents, Effects
import threading
from uuid import uuid4
import re
from tide_tooling.tooling_core import debug_print as print
# Do

## Pipeline and Node definitions ##


class __fallback__:

    def __repr__(self):
        return "unknown"

    def __eq__(self, other):
        return isinstance(other, __fallback__)
    pass
        
__TEMPLATE_VAL__ = __fallback__()

@dataclass
class Pipeline:
    nodes: list[Node] | tuple[Node, ...] | Node = field(default_factory=list)
    name: str | None = None
    id: int | str | None = None

    def __post_init__(self):
        # Normalise a Node or collection of Nodes into one list.
        if isinstance(self.nodes, Node):
            self.nodes = [self.nodes]
        elif isinstance(self.nodes, (list, tuple)):
            self.nodes = list(self.nodes)
        else:
            raise NodeDefinitionError(
                "Pipeline nodes must be a Node or a list/tuple "
                "containing Node objects"
            )

        # Reject non-Node entries within the supplied collection.
        if not all(isinstance(node, Node) for node in self.nodes):
            raise NodeDefinitionError(
                "Pipeline nodes must contain only Node objects"
            )

        # A Pipeline name is optional but must be textual when supplied.
        if self.name is not None and not isinstance(self.name, str):
            raise NodeDefinitionError(
                "Pipeline name must be a string or None"
            )


_data_ref_default_store = ContextVar(
    "tide_data_ref_default_store",
    default="mem",
)


class _DataRefContext(type):
    @property
    def default_store(cls):
        return _data_ref_default_store.get()

    @default_store.setter
    def default_store(cls, store):
        cls.set_default_store(store)


@dataclass(frozen=True)
class DataRef(metaclass=_DataRefContext):
    key: Hashable
    store: str | None = None
    count: int | None = None
    max: int | None = None
    literal: bool = False
    prefix: bool = field(default=True, compare=False, hash=False, repr=False)
    source_str: str | None = field(default=None, compare=False, hash=False, repr=False)

    def __post_init__(self):
        # An omitted Store uses the current context-local default.
        if self.store is None:
            object.__setattr__(self, "store", type(self).default_store)
        if self.key == "*" and not self.literal:
            object.__setattr__(self, "key", "!")

        # A DataRef identifies one hashable key in one Store.
        if not isinstance(self.store, str) or not self.store.strip():
            raise DataRefInitError("DataRef store must be a non-empty string")
        if not isinstance(self.key, Hashable):
            raise DataRefInitError("DataRef key must be hashable")
        if not isinstance(self.prefix, bool):
            raise DataRefInitError("DataRef prefix must be a bool")

        # Count says how many matching States make the reference ready.
        if self.count is not None and (not isinstance(self.count, int) or isinstance(self.count, bool) or self.count < 0):
            raise DataRefInitError("DataRef count must be a non-negative integer or None")

        # Max says how many matching States are selected.
        if self.max is not None and (not isinstance(self.max, int) or isinstance(self.max, bool) or self.max < 0):
            raise DataRefInitError("DataRef max must be a non-negative integer or None")

        # Zero count is the ordinary default readiness contract.
        if self.count == 0:
            object.__setattr__(self, "count", None)
            if self.max == 0:
                object.__setattr__(self, "max", None)

        required = 1 if self.count is None else self.count
        if self.max is not None and self.max > required:
            raise DataRefInitError("DataRef max cannot exceed count")

    @property
    def dynamic(self):
        # Literal keys never represent patterns.
        if self.literal:
            return False
        return self.count is not None or self.max is not None or (isinstance(self.key, str) and (self.key == "!" or any(token in self.key for token in "*?[")))

    @property
    def is_autogenerated(self):
        return self.store == "__untracked__" or self.store.startswith("__untracked___")

    @classmethod
    def set_default_store(cls, store):
        if not isinstance(store, str) or not store.strip():
            raise DataRefInitError("DataRef default store must be a non-empty string")
        return _data_ref_default_store.set(store)

    @classmethod
    def reset_default_store(cls, token):
        _data_ref_default_store.reset(token)

    @classmethod
    def from_string(cls, value: str | DataRef, prefix: bool = True):
        # An existing DataRef is already parsed.
        if isinstance(value, DataRef):
            return value
        if not isinstance(value, str):
            raise TypeError(f"DataRef must be created from str, not {type(value)}")
        if not isinstance(prefix, bool):
            raise TypeError("DataRef prefix must be a bool")

        has_prefix = prefix and value.startswith("@")
        text = value[1:] if has_prefix else value
        source_str = text

        # Model identifiers are literal host identities.
        if text.startswith("__model__:"):
            return cls(key=text.removeprefix("__model__:"), store="__model__", literal=True, prefix=has_prefix, source_str=source_str)

        if re.search(r":\*:\d+:(?:\d+|\*)$", text):
            raise ValueError("Invalid DataRef string; expected STORE:KEY[:MIN[:MAX]]")

        parts = text.split(":")
        count = None
        maximum = None

        # Read the optional MIN and MAX fields from the end.
        if len(parts) >= 3 and parts[-2:] == ["*", "*"]:
            parts = parts[:-2]
            count = 1
        elif len(parts) >= 2 and parts[-2] == "*" and parts[-1].isdigit():
            count = int(parts[-1])
            maximum = count
            parts = parts[:-2]
        elif len(parts) >= 2 and parts[-2].isdigit() and parts[-1] == "*":
            count = int(parts[-2])
            parts = parts[:-2]
        elif len(parts) >= 2 and parts[-2].isdigit() and parts[-1].isdigit():
            count = int(parts[-2])
            maximum = int(parts[-1])
            parts = parts[:-2]
        elif parts[-1].isdigit():
            count = int(parts[-1])
            maximum = count
            parts = parts[:-1]
        elif len(parts) >= 2 and parts[-1] == "*":
            parts = parts[:-1]
            count = 1

        # The final colon separates the Store from the key.
        remaining = ":".join(parts)
        if ":" in remaining:
            store, key = remaining.rsplit(":", 1)
        else:
            store = cls.default_store
            key = remaining
        if not store or not key:
            raise ValueError(f"Invalid DataRef string: {value!r}")

        return cls(key=key, store=store, count=count, max=maximum, prefix=has_prefix, source_str=source_str)

    @staticmethod
    def to_string(data_ref: DataRef, prefix: bool | None = None):
        # Format the resolved Store, key, and explicit dynamic bounds.
        if prefix is None:
            prefix = data_ref.prefix
        elif not isinstance(prefix, bool):
            raise TypeError("DataRef prefix must be a bool or None")

        text = f"{data_ref.store}:{data_ref.key}"
        if data_ref.count is not None or data_ref.max is not None:
            minimum = 1 if data_ref.count is None else data_ref.count
            maximum = "*" if data_ref.max is None else data_ref.max
            text += f":{minimum}:{maximum}"
        return f"@{text}" if prefix else text

class ProcessStates(Enum):
    RAW = "raw"
    EVALUATED = "evaluated"
    NORMALISED = "normalised"
    RESOLVED = "resolved"

@dataclass
class RunnerNode:
    node: Node
    id: int

    # Runtime projection of the Node definition.
    content_id: bytes = field(init=False)
    node_name: str = field(init=False)
    func: Callable = field(init=False)
    effects: list[Effect] = field(init=False)
    targets: list[DataRef] = field(init=False)

    args: RunnerNodeInput = field(init=False)
    kwargs: RunnerNodeInput = field(init=False)
    p_args: RunnerNodeInput = field(init=False)
    p_kwargs: RunnerNodeInput = field(init=False)
    k_args: RunnerNodeInput = field(init=False)
    k_kwargs: RunnerNodeInput = field(init=False)

    force_update: bool = field(init=False)
    one_shot: bool = field(init=False)
    unpack: bool = field(init=False)
    share_implicit_target: bool = field(init=False)

    # Dependencies discovered while compiling this occurrence.
    deps: DedupedList[DataRef] = field(default_factory=DedupedList)
    dep_to_input_map: DefaultDict[
        DataRef,
        DedupedList[tuple[str, RunnerNodeInput]],
    ] = field(default_factory=lambda: defaultdict(DedupedList))
    dynamic_to_static_deps: DefaultDict[
        DataRef,
        DedupedList[DataRef],
    ] = field(default_factory=lambda: defaultdict(DedupedList))
    static_to_dynamic_deps: DefaultDict[
        DataRef,
        DedupedList[DataRef],
    ] = field(default_factory=lambda: defaultdict(DedupedList))
    _input_nodes: DedupedList[Node] = field(default_factory=DedupedList)

    # Preserve the execution which registered an emitted occurrence.
    emitted_by_exec_id: str | None = None

    # Retain the current execution state.
    executing: bool = False
    evaluated: bool = False
    result: Any = None
    status: str | None = None

    # Delay dependency changes until the current execution completes.
    _pending_deleted_dependencies: DedupedList[DataRef] = field(default_factory=DedupedList)
    _pending_updated_dependencies: DedupedList[DataRef] = field(default_factory=DedupedList)

    def __post_init__(self):
        # Project the immutable definition used by this runtime occurrence.
        self.content_id = self.node.content_id
        self.node_name = self.node.node_name
        self.func = self.node.func
        self.effects = self.node.effects
        self.targets = self.node.targets

        # Give every definition input its own runtime carrier.
        self.args = RunnerNodeInput(self.node.args.raw)
        self.kwargs = RunnerNodeInput(self.node.kwargs.raw)
        self.p_args = RunnerNodeInput(self.node.p_args.raw)
        self.p_kwargs = RunnerNodeInput(self.node.p_kwargs.raw)
        self.k_args = RunnerNodeInput(self.node.k_args.raw)
        self.k_kwargs = RunnerNodeInput(self.node.k_kwargs.raw)

        # Project the definition's execution policy.
        self.force_update = self.node.force_update
        self.one_shot = self.node.one_shot
        self.unpack = self.node.unpack
        self.share_implicit_target = self.node.share_implicit_target

    def _invalidate(self):
        # Clear the completed result so this occurrence may execute again.
        self.evaluated = False
        self.result = None

    def _unresolve_input(self, node_input):
        # Invalidate this occurrence and its affected input.
        self._invalidate()
        node_input.unresolve()

    def get_primary_inputs(self):
        return {
            "args": self.args,
            "kwargs": self.kwargs,
        }

    def get_all_inputs(self):
        return {
            "p_args": self.p_args,
            "p_kwargs": self.p_kwargs,
            "k_args": self.k_args,
            "k_kwargs": self.k_kwargs,
            "args": self.args,
            "kwargs": self.kwargs,
        }

@dataclass
class RunnerNodeInput:
    raw: Any
    evaluated: Any = None
    normalised: Any = None
    resolved: Any = None

    deps: list[DataRef] = field(default_factory=list)
    state: ProcessStates = field(
        default=ProcessStates.RAW,
        init=False,
    )

    _PROCESS_ORDER: ClassVar[tuple[ProcessStates, ...]] = (
        ProcessStates.RAW,
        ProcessStates.EVALUATED,
        ProcessStates.NORMALISED,
        ProcessStates.RESOLVED,
    )

    def has_been(self, state):
        # A later stage includes every earlier completed stage.
        return (
            self._PROCESS_ORDER.index(self.state)
            >= self._PROCESS_ORDER.index(state)
        )

    def get_previous_value(self, state):
        # Read the value produced immediately before the requested stage.
        index = self._PROCESS_ORDER.index(state)
        if index == 0:
            raise ValueError(
                "RAW has no previous input value"
            )

        previous = self._PROCESS_ORDER[index - 1]
        return getattr(self, previous.value)

    def update_state(self, state, value):
        # Inputs progress through exactly one stage at a time.
        current = self._PROCESS_ORDER.index(self.state)
        requested = self._PROCESS_ORDER.index(state)

        if requested != current + 1:
            raise RuntimeError(
                f"Cannot transition input from "
                f"{self.state.value} to {state.value}"
            )

        setattr(self, state.value, value)
        self.state = state

    def unresolve(self):
        # Return a resolved input to its normalised dependency structure.
        if self.state is ProcessStates.RESOLVED:
            self.resolved = None
            self.state = ProcessStates.NORMALISED

@dataclass
class NodeInput:
    raw: Any = None

def _normalise_node_input(value, default):
    # Preserve an existing NodeInput or wrap one raw input.
    if isinstance(value, NodeInput):
        return value
    return NodeInput(default if value is None else value)


def _normalise_node_effects(effects):
    # Normalise the supported Effect carriers into one ordered list.
    if effects is None:
        effects = []
    elif isinstance(effects, Effect) or (isinstance(effects, type) and issubclass(effects, Effect)):
        effects = [effects]
    elif isinstance(effects, (list, tuple, DedupedList)):
        effects = list(effects)
    else:
        raise NodeDefinitionError("Node effects must be an Effect or an ordered list/tuple of Effects")

    # Reject runtime Effect functions used as declared Effects.
    runtime_effect_functions = (READ, WRITE_NOW, DELETE_NOW)
    for effect in effects:
        if any(effect is runtime_effect for runtime_effect in runtime_effect_functions):
            raise NodeDefinitionError(
                f"{effect.__name__} is a runtime Effect function and must be called inside a Node; "
                "declared effects require Effect objects or Effect classes"
            )

    # Validate every declared Effect.
    for effect in effects:
        if effect is RAISE or isinstance(effect, RAISE):
            raise NodeDefinitionError("RAISE is execution control and cannot be a declared Node Effect")
        if not (isinstance(effect, Effect) or (isinstance(effect, type) and issubclass(effect, Effect))):
            raise NodeDefinitionError("Node effects must contain only Effect objects or Effect classes")

    # Instantiate the argument-free Effect classes.
    return [effect() if effect is END or effect is DELETE else effect for effect in effects]


def _node_content_id(node):
    # Identify every definition field which affects execution.
    definition = (
        node.func,
        node.args.raw,
        node.kwargs.raw,
        node.p_args.raw,
        node.p_kwargs.raw,
        node.k_args.raw,
        node.k_kwargs.raw,
        tuple(node.targets),
        tuple(node.effects),
        node.node_name,
        node.force_update,
        node.one_shot,
        node.unpack,
        node.share_implicit_target,
    )
    return repr(definition).encode("utf-8")


@dataclass
class Node:
    func: Callable
    args: Any = field(default_factory=tuple)
    kwargs: Any = field(default_factory=dict)
    p_args: Any = field(default_factory=tuple)
    p_kwargs: Any = field(default_factory=dict)
    k_args: Any = field(default_factory=tuple)
    k_kwargs: Any = field(default_factory=dict)
    targets: str | DataRef | list[str | DataRef] | None = None
    effects: Effect | type[Effect] | list[Effect] | tuple[Effect, ...] | None = None
    node_name: str | None = None
    force_update: bool = False
    one_shot: bool = False
    unpack: bool = False
    share_implicit_target: bool = False
    id: str = field(init=False)
    content_id: bytes = field(init=False)

    def __post_init__(self):
        # Require the execution callable.
        if not callable(self.func):
            raise NodeDefinitionError("Node func must be callable")

        # Give this definition occurrence its identity.
        self.id = uuid4().hex

        # Normalise every input boundary.
        self.args = _normalise_node_input(self.args, ())
        self.kwargs = _normalise_node_input(self.kwargs, {})
        self.p_args = _normalise_node_input(self.p_args, ())
        self.p_kwargs = _normalise_node_input(self.p_kwargs, {})
        self.k_args = _normalise_node_input(self.k_args, ())
        self.k_kwargs = _normalise_node_input(self.k_kwargs, {})

        # Normalise explicitly declared output references.
        if not isinstance(self.targets, (list, str, DataRef, type(None))):
            raise NodeDefinitionError("Node targets must be a string, DataRef, list of either, or None")
        targets = [] if self.targets is None else self.targets if isinstance(self.targets, list) else [self.targets]
        self.targets = []
        for target in targets:
            if not isinstance(target, (str, DataRef)):
                raise NodeDefinitionError("Node targets must contain only strings or DataRefs")
            target = DataRef.from_string(target, prefix=False) if isinstance(target, str) else target
            if target not in self.targets:
                self.targets.append(target)

        # Normalise ordered declared Effects.
        self.effects = _normalise_node_effects(self.effects)

        # Retain the explicit label or derive it from the callable.
        if self.node_name is not None and not isinstance(self.node_name, str):
            raise NodeDefinitionError("Node name must be a string or None")
        self.node_name = self.node_name if self.node_name is not None else self.func.__name__

        # Require every execution option to be Boolean.
        options = {
            "force_update": self.force_update,
            "one_shot": self.one_shot,
            "unpack": self.unpack,
            "share_implicit_target": self.share_implicit_target,
        }
        invalid_options = [name for name, value in options.items() if not isinstance(value, bool)]
        if invalid_options:
            raise NodeDefinitionError("Node options must be bool: " + ", ".join(invalid_options))

        # Identify the complete execution-affecting definition.
        self.content_id = _node_content_id(self)



@dataclass
class IntentDag:
    # Node occurrence -> declared dependencies.
    forward_map: DefaultDict[int, DedupedList[DataRef]] = field(default_factory=lambda: defaultdict(DedupedList))
    # Dependency -> consuming node occurrences.
    reverse_map: DefaultDict[DataRef, DedupedList[int]] = field(default_factory=lambda: defaultdict(DedupedList))
    # Partition dependency predicates by kind.
    static_deps: DefaultDict[DataRef, DedupedList[int]] = field(default_factory=lambda: defaultdict(DedupedList))
    dynamic_deps: DefaultDict[DataRef, DedupedList[int]] = field(default_factory=lambda: defaultdict(DedupedList))
    # Nodes with no declared dependencies begin ready.
    ready_nodes: DedupedList[int] = field(default_factory=DedupedList)
    
class RuntimeDag:
    def __init__(self, dag):
        if not isinstance(dag, IntentDag):
            raise RuntimeDagConsumptionError(
                "RuntimeDag requires an IntentDag"
            )

        # Copy the static DAG into independently mutable runtime state.
        self.forward_map = copy.deepcopy(dag.forward_map)
        self.reverse_map = copy.deepcopy(dag.reverse_map)
        self.waiting_static_deps = copy.deepcopy(dag.static_deps)
        self.waiting_dynamic_deps = copy.deepcopy(dag.dynamic_deps)
        self.ready_nodes_queue = deque(
            copy.deepcopy(dag.ready_nodes)
        )

    def register_node(self, node):
        # Register one runtime-discovered node only once.
        if node.id in self.forward_map:
            return False

        dependencies = DedupedList(node.deps)
        self.forward_map[node.id] = dependencies

        for dependency in dependencies:
            self.reverse_map[dependency].append(node.id)

            waiting = (
                self.waiting_dynamic_deps
                if dependency.dynamic
                else self.waiting_static_deps
            )
            waiting[dependency].append(node.id)

        # Dependency-free runtime work joins the ready queue.
        if not dependencies:
            self.ready_nodes_queue.append(node.id)

        return True

    def satisfy_dependency(self, dependency):
        # Satisfy this exact dependency predicate for every dependant.
        released_nodes = DedupedList()

        for node_id in tuple(
            self.reverse_map.get(dependency, ())
        ):
            dependencies = self.forward_map.get(node_id)

            if dependencies is None or dependency not in dependencies:
                continue

            dependencies.remove(dependency)

            if not dependencies:
                self.ready_nodes_queue.append(node_id)
                released_nodes.append(node_id)

        # No node remains waiting on the satisfied predicate.
        waiting = (
            self.waiting_dynamic_deps
            if dependency.dynamic
            else self.waiting_static_deps
        )
        waiting.pop(dependency, None)

        return released_nodes

    def unsatisfy_dependency(self, node_id, dependency):
        # Make one previously satisfied predicate unsatisfied again.
        dependencies = self.forward_map[node_id]

        if dependency in dependencies:
            return False

        dependencies.append(dependency)

        waiting = (
            self.waiting_dynamic_deps
            if dependency.dynamic
            else self.waiting_static_deps
        )
        waiting[dependency].append(node_id)

        # A blocked node can no longer remain ready.
        try:
            self.ready_nodes_queue.remove(node_id)
        except ValueError:
            pass

        return True

# class SQLite(StoreTemplate):


# class RedisStore(StoreTemplate):


class FakeFuture:
    

    def __init__(self, result: Any=None, exception: Exception=None) -> FakeFuture:
        FINISHED = 'FINISHED'
        self._condition = threading.Condition()
        self._state = FINISHED
        self._result = result
        self._exception = exception

    def result(self) -> Any:
        if self._exception is not None:
            raise self._exception 
        return self._result     


class Effect(SealableEvidence):
    kind: ClassVar[Effects]
    _tide_value_fields = frozenset({"output"})

    def __init__(self, output: Any = __TEMPLATE_VAL__):
        self.output = output

    def to_dict(self):
        payload = {"effect": self.kind.value}
        payload.update({
            name: getattr(self, name)
            for name in vars(self)
            if not name.startswith("_tide_")
        })
        return payload

    def __str__(self):
        details = [
            f"{name}={value!r}"
            for name, value in self.to_dict().items()
            if name not in {"effect", "output"}
        ]
        return f"{self.kind}({', '.join(details)})"

    def __repr__(self):
        return str(self)

class EMIT(Effect):
    kind = Effects.EMIT
    _tide_value_fields = frozenset({"output", "nodes"})

    def __init__(self, nodes: Node | Iterable[Node], output: Any = __TEMPLATE_VAL__):
        super().__init__(output)

        nodes = (nodes,) if isinstance(nodes, Node) else tuple(nodes)

        if not nodes:
            raise ValueError("EMIT requires at least one Node")

        if any(not isinstance(node, Node) for node in nodes):
            raise TypeError("EMIT accepts only Node definitions")

        self.nodes = nodes
        

class END(Effect):
    kind = Effects.END

    def __init__(self, runner_ids: int | str | Iterable[int | str] | None = None, output: Any = __TEMPLATE_VAL__):
        super().__init__(output)

        if runner_ids is None:
            self.targets = None
            return

        targets = (runner_ids,) if isinstance(runner_ids, (int, str)) else tuple(runner_ids)

        if not targets:
            raise ValueError("END requires at least one Runner target")

        if any(not isinstance(runner_id, (int, str)) for runner_id in targets):
            raise TypeError("END targets must be Runner IDs")

        self.targets = targets

class RUN(Effect):
    kind = Effects.RUN
    _tide_value_fields = frozenset({
        "output",
        "local_store_templates",
        "dynamic_ref_plan",
    })

    def __init__(self, pipeline_ids: int | str | Iterable[int | str], mode: str = "auto", local_store_templates: dict[str, Any] | None = None, stay_alive: bool = False, runner_name: str | None = None, dynamic_ref_plan: Any = None, output: Any = __TEMPLATE_VAL__):
        super().__init__(output)

        targets = (pipeline_ids,) if isinstance(pipeline_ids, (int, str)) else tuple(pipeline_ids)

        if not targets:
            raise ValueError("RUN requires at least one Pipeline target")

        if any(not isinstance(pipeline_id, (int, str)) for pipeline_id in targets):
            raise TypeError("RUN targets must be Pipeline IDs")

        if runner_name is not None and len(targets) != 1:
            raise ValueError("runner_name requires exactly one Pipeline target")

        self.targets = targets
        self.mode = mode
        self.local_store_templates = dict(local_store_templates or {})
        self.stay_alive = stay_alive
        self.runner_name = runner_name
        self.dynamic_ref_plan = dynamic_ref_plan




class DELETE(Effect):
    kind = Effects.DELETE

    def __init__(self, references: str | DataRef | Iterable[str | DataRef] | None = None, output: Any = __TEMPLATE_VAL__):
        super().__init__(output)

        if references is None:
            self.targets = None
            return

        raw_targets = (references,) if isinstance(references, (str, DataRef)) else tuple(references)
        targets = []
        for target in raw_targets:
            ref = DataRef.from_string(target, prefix=False) if isinstance(target, str) else target

            if not isinstance(ref, DataRef):
                raise TypeError("DELETE targets must be strings or DataRefs")

            if ref.dynamic:
                raise ValueError("DELETE requires static references")

            if ref not in targets:
                targets.append(ref)

        if not targets:
            raise ValueError("DELETE requires at least one target")

        self.targets = tuple(targets)

class WRITE(Effect):
    kind = Effects.WRITE
    _tide_value_fields = frozenset({"output", "targets"})

    def __init__(self, references: dict[str | DataRef, Any] | str | DataRef | Iterable[str | DataRef], value: Any = __TEMPLATE_VAL__, *, output: Any = __TEMPLATE_VAL__):
        super().__init__(output)

        mapped = isinstance(references, dict)
        if mapped:
            if value is not __TEMPLATE_VAL__:
                raise ValueError("WRITE cannot receive both mapped values and value")
            raw_writes = tuple(references.items())
        else:
            if value is __TEMPLATE_VAL__:
                raise ValueError("WRITE requires a value for its references")
            raw_references = (references,) if isinstance(references, (str, DataRef)) else tuple(references)
            raw_writes = tuple((reference, value) for reference in raw_references)

        targets = []
        for reference, write_value in raw_writes:
            ref = DataRef.from_string(reference, prefix=False) if isinstance(reference, str) else reference

            if not isinstance(ref, DataRef):
                raise TypeError("WRITE targets must be strings or DataRefs")

            if ref.dynamic:
                raise ValueError("WRITE requires static references")

            if any(existing_ref == ref for existing_ref, _ in targets):
                if mapped:
                    raise ValueError(f"WRITE contains duplicate normalized target: {ref}")
                continue

            targets.append((ref, write_value))

        if not targets:
            raise ValueError("WRITE requires at least one target")

        self.targets = tuple(targets)

class READ_EFFECT(Effect):
    kind = Effects.READ
    _tide_value_fields = frozenset({"output", "fallback"})

    def __init__(self, reference: str | DataRef, fallback: Any = __TEMPLATE_VAL__):
        super().__init__()

        ref = DataRef.from_string(reference, prefix=False) if isinstance(reference, str) else reference

        if not isinstance(ref, DataRef):
            raise TypeError("READ requires a string or DataRef")

        if ref.dynamic:
            raise ValueError("READ requires one static reference")

        self.targets = (ref,)
        self.fallback = fallback


class RAISE(Effect, Exception):
    kind = Effects.RAISE
    _tide_value_fields = frozenset({"output", "exception"})

    def __init__(self, exception: Exception | type[Exception] | str | None = None, effects: Effect | type[Effect] | Iterable[Effect | type[Effect]] | None = None, output: Any = __TEMPLATE_VAL__):
        if exception is None:
            exception = Exception("TIDE controlled RAISE")
        elif isinstance(exception, str):
            exception = Exception(exception)
        elif (
            isinstance(exception, type)
            and issubclass(exception, Exception)
        ):
            exception = exception()

        if not isinstance(exception, Exception):
            raise TypeError("RAISE requires an Exception, Exception type, string, or None")

        if effects is None:
            effects = ()
        elif isinstance(effects, Effect) or (isinstance(effects, type) and issubclass(effects, Effect)):
            effects = (effects,)
        else:
            effects = tuple(effects)

        for effect in effects:
            if not (isinstance(effect, Effect) or (isinstance(effect, type) and issubclass(effect, Effect))):
                raise TypeError("RAISE effects must be Effect objects or classes")

        result = exception if output is __TEMPLATE_VAL__ else output
        Effect.__init__(self, result)
        Exception.__init__(self, str(exception))

        self.exception = exception
        self.effects = effects

    def to_dict(self):
        return {
            "effect": self.kind.value,
            "exception": repr(self.exception),
            "output": self.output,
            "effects": self.effects,
        }

    def __str__(self):
        return f"RAISE(exception={self.exception!r})"


@dataclass
class EffectRuntime:
    # Immediate occurrence Events are published directly through Ocean.  This
    # carrier retains only the runtime Effect summary returned with ExecMeta.
    exec_id: str
    effects: list[Effect] = field(default_factory=list)
    node_ready_event_id: str | None = None
    inside_process_pool: bool = False
    default_store: str = "global"
    read_handler: Callable | None = None
    write_handler: Callable | None = None
    delete_handler: Callable | None = None

    def __deepcopy__(self, memo):
        copied = type(self)(
            exec_id=self.exec_id,
            effects=[],
            node_ready_event_id=self.node_ready_event_id,
            inside_process_pool=self.inside_process_pool,
            default_store=self.default_store,
            read_handler=None,
            write_handler=None,
            delete_handler=None,
        )

        memo[id(self)] = copied
        copied.effects = copy.deepcopy(self.effects, memo)

        return copied

    def read(self, effect):
        if self.inside_process_pool:
            raise ReadInsideProcessPoolError()
        return self.read_handler(self, effect)

    def write_now(self, effect):
        if self.inside_process_pool:
            raise WriteNowInsideProcessPoolError()
        return self.write_handler(self, effect)

    def delete_now(self, effect):
        if self.inside_process_pool:
            raise DeleteNowInsideProcessPoolError()
        return self.delete_handler(self, effect)


_current_effect_runtime = ContextVar(
    "current_tide_effect_runtime",
    default=None,
)


def READ(reference: str | DataRef, fallback: Any = __TEMPLATE_VAL__):
    """Read a reference during TIDE execution and record the attempted read.

    Outside a TIDE node runtime there is no store context, so the fallback is
    returned without side effects. The default fallback is TIDE's template
    sentinel.
    """
    effect = READ_EFFECT(reference, fallback)

    runtime = _current_effect_runtime.get()
    if runtime is None:
        return effect.fallback

    return runtime.read(effect)


def WRITE_NOW(reference: str | DataRef, value: Any) -> bool:
    if not isinstance(reference, (str, DataRef)):
        raise TypeError("WRITE_NOW requires a string or DataRef")
    effect = WRITE(reference, value)
    runtime = _current_effect_runtime.get()

    if runtime is None:
        return __fallback__ 
     
    return runtime.write_now(effect)


def DELETE_NOW(reference: str | DataRef) -> bool:
    """Delete one reference during TIDE execution and report success.

    Unlike :class:`DELETE`, this is an imperative runtime operation: the live
    store is changed before the function continues.  Outside a TIDE node there
    is no store to mutate, so ``False`` is returned without side effects.
    """
    ref = DataRef.from_string(reference, prefix=False) if isinstance(reference, str) else reference
    if not isinstance(ref, DataRef):
        raise TypeError("DELETE_NOW requires a string or DataRef")
    if ref.dynamic:
        raise ValueError("DELETE_NOW requires one static reference")
    effect = DELETE(ref)

    runtime = _current_effect_runtime.get()
    if runtime is None:
        return False

    return runtime.delete_now(effect)
        
