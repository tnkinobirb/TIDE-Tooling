from __future__ import annotations

from dataclasses import dataclass, field
from contextvars import ContextVar
from typing import Callable, DefaultDict, List, Self, Dict, Any, Hashable, Iterable, ClassVar
from .errors import NodeDefinitionError, StageDefinitionError, NodeToDagRegistrationError, RuntimeDagConsumptionError, DataRefInitError, MissingDepKeyError, MissingStoreError, ReadInsideProcessPoolError, DeleteNowInsideProcessPoolError
from collections import deque, defaultdict
from tide_tooling.tooling_core.engine.events import Event 
from warnings import warn
from tide_tooling.tooling_core.engine.utils import DedupedList
import time
from enum import Enum, StrEnum
from .events import RouterEvents, PipelineEvents, ExecutorEvents, Effects
import threading
from uuid import uuid4
import re
from tide_tooling.tooling_core import debug_print as print
# Do

## Pipeline Node amd Stage Definitions ##


class __TIDE_TEMPLATE_VALUE__:

    def __repr__(self):
        return "unknown"

    def __eq__(self, other):
        return isinstance(other, __TIDE_TEMPLATE_VALUE__)
    pass
        
__TEMPLATE_VAL__ = __TIDE_TEMPLATE_VALUE__()

# Stage - DedupedList or tuple of Nodes dataclasses and a name
@dataclass
class Pipeline:
    stages: DedupedList[Node] | DedupedList[Stage] = field(default_factory=DedupedList)
    name: str = None
    id: int | str = None
    _pipeline_counter: int = 0
    
    def __post_init__(self): 
        #apply and increment id
        Pipeline._pipeline_counter += 1

        if not self.name:
            self.name = f"Pipeline_{self.id}"

        if isinstance(self.stages, Node):
            self.stages = [Stage("default_stage", [self.stages])]
            return
        if isinstance(self.stages[0], Node):
            self.stages = [Stage("default_stage", self.stages)]
            return
@dataclass
class Stage:
    id: int = field(init=False)
    name: str
    nodes: DedupedList[Node] | tuple[Node]

    _stage_counter: int = 0

    def __post_init__(self):
        self.id = Stage._stage_counter
        Stage._stage_counter += 1
        # DedupedList name - if no name given
        if not self.name:
            self.name = f"stage_{self.id}"
        elif not isinstance(self.name, str):
            raise StageDefinitionError(f"StageDefinitonError[@SessionID:{self.id}] | Name [self.name]: {self.name} must be a string!")
        
        if not isinstance(self.nodes, (list, tuple)):
            raise StageDefinitionError(f"StageDefinitonError[@SessionID:{self.id}] | Node_DedupedList [self.nodes]: Node_DedupedList [{self.nodes}] must be a DedupedList or tuple - was {type(self.nodes)}!")

        invalid = [n for n in self.nodes if not isinstance(n, Node)]
        if invalid:
            raise StageDefinitionError(f"StageDefinitonError[@SessionID:{self.id}] | Node_DedupedList [self.nodes] must only contain Node objects! Node Processing/Error-Checking Error? | Invalid_nodes: [{invalid}]")


_data_ref_default_store = ContextVar(
    "tide_data_ref_default_store",
    default="mem",
)


class _DataRefMeta(type):
    @property
    def default_store(cls):
        return _data_ref_default_store.get()

    @default_store.setter
    def default_store(cls, store):
        cls.set_default_store(store)


class DataRef(metaclass=_DataRefMeta):
    @classmethod
    def set_default_store(cls, store):
        if not isinstance(store, str) or not store.strip():
            raise DataRefInitError(
                "DataRef default store must be a non-empty string"
            )
        return _data_ref_default_store.set(store)

    @classmethod
    def reset_default_store(cls, token):
        _data_ref_default_store.reset(token)

    def __init__(self, key: str=None, store: str=None, count: int=None, max: int=None) -> None:

        #if targets not hashable, 
        if not isinstance(store, str):
            if store is None:
                store = DataRef.default_store
            else:
                raise DataRefInitError(f"data_ref.store reference was not a string! was {type(store)} e.g: mem, disk, redis")
        if not isinstance(key, Hashable):
            raise DataRefInitError(f"data_ref.key {type(key)}:{key} was not hashable! please provide a hashable value. To disable this check - please DedupedList HashableKeyCheck to False in the config")
        
        if count is not None and (not isinstance(count, int) or isinstance(count, bool) or count < 0):
            raise DataRefInitError("data_ref.count must be a non-negative integer or None")
        if max is not None and (not isinstance(max, int) or isinstance(max, bool) or max < 0):
            raise DataRefInitError("data_ref.max must be a non-negative integer or None")

        minimum = 1 if count is None else count
        if max is not None and max > minimum:
            raise DataRefInitError(
                "data_ref.max cannot exceed data_ref.count (MIN): "
                f"MAX {max} > MIN {minimum}"
            )

        self.key = key
        self.store = store
        self.dynamic = False
        self.count = count # count ignored if not dynamic
        self.max = max

        if count is not None or max is not None or any(token in self.key for token in "*?["):
            self.dynamic = True

    @classmethod
    def literal(cls, key, store):
        """Create a reference whose key is data, not dependency syntax."""
        reference = cls(key=str(key), store=store)
        reference.dynamic = False
        reference.count = None
        reference.max = None
        return reference

        # if isinstance(key, str):
        #     if "@" in key: # if converting from stre @ should be stripped bbbefore - temporary work around
        #         self.key =  key.split("@", 1)[1] # remove @ from @dep.string

        #     if ":" in self.key and not store: # could lead to bug is store chosen and user presumes :is part of the sttr. for now startswith @ and : are special characters
        #         self.store, self.key = self.key.split(":", 1)
        #     if "*" in self.key:   
        #         self.dynamic = True  
        #         return
    @staticmethod
    def _is_dep_string(_str, strict=True):
        if isinstance(_str, str):
            # Dep strings are deemed incomplete if like @dep.str. and will be ignored. Must be like @dep.str 
            if _str.startswith("@") and not _str.endswith("."):
                if _str == "@": # dep str must contain a path value
                    warn("dep_extractor found a dep as '@'! Check that tuple or DedupedList is correcty defined for a single dep_string. e.g: (@dep_string, ) or {@dep_string, }")
                    return False
                return True 
            elif not strict and ":" in _str and not _str.endswith("."):
                return True
            
        return False

    @staticmethod # 
    def from_string(target_str: str, strip_leading=True, verbose=False) -> DataRef:
        if isinstance(target_str, DataRef):
            return target_str
        if isinstance(target_str, str):
            # print(f"[TARGET_STR_DEBUG_CONVERTER] - FOUND {target_str} as {type(target_str)}, repr={repr(target_str)}")
            # if verbose:
            #     print(f"converting dep_Ref {target_str} to DataRef")
            if strip_leading: 
                target_key = target_str.split("@", 1)[1] if target_str.startswith("@") else target_str  # remove first @ from @dep.string
            else:
                target_key = target_str
            target_store = DataRef.default_store
            count = None
            max = None

            # TIDE Model State identifiers are opaque host identifiers. A
            # colon, numeric suffix or ``*`` inside one is literal identity,
            # never TIDE dependency syntax.
            model_prefix = "__model__:"
            if target_key.startswith(model_prefix):
                return DataRef.literal(
                    target_key[len(model_prefix):],
                    "__model__",
                )

            parts = target_key.split(":")

            # Pull MIN/MAX options from the end. The public grammar is
            # STORE:KEY[:MIN[:MAX]]. A bare ``*`` in the MIN position marks a
            # dynamic reference with the default minimum; ``*`` in MAX means
            # unbounded. Wildcards inside KEY independently make it dynamic.
            if re.search(r":\*:\d+:(?:\d+|\*)$", target_key):
                raise ValueError(
                    "Invalid dependency reference format: "
                    f"{target_str}; expected STORE:KEY[:MIN[:MAX]]"
                )

            if len(parts) >= 4 and parts[-2:] == ["*", "*"]:
                # Explicit ``MIN=*, MAX=*`` means the default minimum and
                # every match available when resolution freezes.
                parts.pop()
                parts.pop()
                count = 1
            elif (
                len(parts) >= 2
                and parts[-2] == "*"
                and parts[-1].isdigit()
            ):
                selected = int(parts.pop())
                parts.pop()
                count = selected
                max = selected
            elif (
                len(parts) >= 2
                and parts[-2].isdigit()
                and (parts[-1].isdigit() or parts[-1] == "*")
            ):
                max_token = parts.pop()
                count = int(parts.pop())
                max = count if max_token == "*" else int(max_token)
            elif parts and parts[-1].isdigit():
                count = int(parts.pop())
                max = count
            elif len(parts) >= 3 and parts[-1] == "*":
                parts.pop()
                count = 1

            # Whatever remains is ``key`` or ``store:key``. Runtime-scoped
            # stores may themselves contain colons (for example
            # ``__tide___TIDE: .exec(0)``), so the final colon is the only
            # safe store/key boundary.
            remaining = ":".join(parts)
            if not remaining:
                raise ValueError(
                    f"Invalid dependency reference format: {target_str}"
                )

            if ":" in remaining:
                target_store, target_key = remaining.rsplit(":", 1)
                if not target_store or not target_key:
                    raise ValueError(
                        f"Invalid dependency reference format: {target_str}"
                    )
            else:
                target_key = remaining

            converted_ref = DataRef(key=target_key, store=target_store, count=count, max=max)
            if verbose:
                print(f"converted:{converted_ref} dynamic = {converted_ref.dynamic}")
            return converted_ref
            
        raise TypeError(f"{target_str} must be a str, was type({type(target_str)})") 
    
    @staticmethod
    def to_string(data_ref: DataRef, strip_leading=False):
        ref_string = f"@{data_ref.store}:{data_ref.key}"
        if strip_leading: ref_string = ref_string.split("@", 1)[1]
        if data_ref.dynamic:
            key_is_dynamic = any(token in data_ref.key for token in "*?[")
            if data_ref.count is None and data_ref.max is None:
                return ref_string
            if data_ref.count == 1 and data_ref.max is None:
                return ref_string + ":*"
            minimum = 1 if data_ref.count is None else data_ref.count
            if data_ref.max is None:
                return ref_string + f":{minimum}:*"
            if data_ref.max == minimum:
                return ref_string + f":{minimum}"
            ref_string = ref_string + f":{minimum}:{data_ref.max}"

        return ref_string
    
    @staticmethod    
    def _normalise_data_refs(targets: str | DataRef | DedupedList[str | DataRef]):
        normalised_targets = []

        if isinstance(targets, list):    
            for target in targets:
                if not isinstance(target, (DataRef, str)): # TODO-NOTE: Support targets DedupedLists [X]
                    raise NodeDefinitionError(f"DataRefDefError | Target - [{type(target)}, {target}] must be str, DataRef or a DedupedList of either!")
                
                normalised = DataRef.from_string(target, strip_leading=False) if isinstance(target, str) else target
                    
                if normalised not in normalised_targets:
                    normalised_targets.append(normalised)
            return normalised_targets

        if isinstance(targets, str): # TODO-NOTE: Support targets DedupedLists [X]
            normalised_targets = [DataRef.from_string(targets, strip_leading=False)]

        if isinstance(targets, DataRef):
            normalised_targets = [targets]

        return normalised_targets
    
    def __hash__(self) -> int:
        return hash((self.store, self.key)) 

    def __eq__(self, other: Any) -> bool:
        return (isinstance(other, DataRef) and self.store == other.store and self.key == other.key and self.max == other.max and self.count == other.count)

    def __repr__(self) -> str:
        minimum = 1 if self.dynamic and self.count is None else self.count
        maximum = "*" if self.dynamic and self.max is None else self.max
        return f"DataRef({self.key}, {self.store}{(f', need:{minimum},' if minimum is not None else '')} {(f'return:{maximum}' if maximum is not None else '')})"

class ProcessStates(Enum):
    RAW = "raw"
    EVALUATED = "evaluated"
    NORMALISED = "normalised"
    RESOLVED = "resolved"

@dataclass
class NodeState:
    indexed : bool = False
    compiled: bool = False
    resolved: bool = False
    executing: bool = False
    evaluated: bool = False
    result_cached: bool = False


@dataclass 
class NodeDeferralFlags:   
    # func defer flag
    func    : bool = False
    # primary args defer flag
    args    : bool = False
    # primary kwargs defer flag
    kwargs  : bool = False
    
    # secondary args defer flags
    p_args  : bool = False
    p_kwargs: bool = False
    # secondary kwargs defer flags
    k_args  : bool = False
    k_kwargs: bool = False
    # is any input is deferred? else False
    def any(self) -> bool:
        return any((
            self.func,
            self.args,
            self.kwargs,
            self.p_args,
            self.p_kwargs,
            self.k_args,
            self.k_kwargs,
        ))
    
@dataclass
class RunnerNode:
    node: Node
    id: int
    content_id: int = None
    deps: DedupedList[DataRef] = field(default_factory=DedupedList)
    effects: DedupedList[Effect] = field(default_factory=DedupedList)
    defer_flags: NodeDeferralFlags = field(default_factory=NodeDeferralFlags)
    deferred: bool = False

    result: DataRef = None
    result_cached: bool = False

    # Time
    status: str = None
    start_time: float = None
    end_time: float = None
    duration: float = None

    emitted: bool = False
    emitted_by: DedupedList = field(default_factory=DedupedList)

    # Task name 
    original_node_name: str | None = None # if route is a duplicate, node_name is appended with _id so maintain original for traceability

    state: NodeState = field(default_factory=NodeState)

    dep_to_input_map: DefaultDict[DataRef, DedupedList[str, NodeInput]] = field(default_factory=lambda: defaultdict(DedupedList))

    dynamic_to_static_deps: DefaultDict[DataRef, DedupedList[DataRef]] = field(default_factory=lambda: defaultdict(DedupedList))
    static_to_dynamic_deps: DefaultDict[DataRef, DedupedList[DataRef]] = field(default_factory=lambda: defaultdict(DedupedList))
    
    touched_nodes: DedupedList[Node] = field(default_factory=DedupedList)
    released_nodes: DedupedList[Node] = field(default_factory=DedupedList)
    emitted_nodes: DedupedList[Node] = field(default_factory=DedupedList)

    runtime_targets: DedupedList[DataRef] = field(default_factory=DedupedList)
    runtime_effects: DedupedList[DataRef] = field(default_factory=DedupedList)

    resolve_output: bool = False
    resolved_output: Any = None
    recompute: bool = False
    one_shot: bool = False

    _input_nodes: DedupedList = field(default_factory=DedupedList)
    _pending_unresolve_inputs: DefaultDict = field(default_factory=lambda: defaultdict(DedupedList))



    def __post_init__(self): 
        #apply and increment id

        self.content_id = self.node.content_id
        self.node_name: str = self.node.node_name 

        # Node function / Task
        self.func: Callable = self.node.func
        self.effects: DedupedList[Effect] = self.node.effects
        self.targets: DedupedList[DataRef] = self.node.targets # TODO-NOTE: Support targets DedupedLists [X]

        # Primary Inputs satisfied_nodes = DedupedList(dependent_node_id)
        self.args:   NodeInput | Any | str | DedupedList | tuple = RunnerNodeInput(self.node.args.raw) 
        self.kwargs: NodeInput | dict = RunnerNodeInput(self.node.kwargs.raw) 

        # Secondary Inputs
        self.p_args   : NodeInput | tuple = RunnerNodeInput(self.node.p_args.raw) 
        self.p_kwargs : NodeInput | dict = RunnerNodeInput(self.node.p_kwargs.raw) 
        self.k_args   : NodeInput | tuple = RunnerNodeInput(self.node.k_args.raw)               
        self.k_kwargs : NodeInput | dict = RunnerNodeInput(self.node.k_kwargs.raw)

        self.inject_data_store: bool = self.node.inject_data_store
        self.resolve_output: bool = self.node.resolve_output
        self.recompute: bool = self.node.recompute
        self.unpack: bool = self.node.unpack

 # For Runner
    def _invalidate(self):
        self._reset_execution_state()

    def _unresolve(self):
        self.state.resolved = False
        self._reset_execution_state()

        self.dynamic_to_static_deps: DefaultDict[DataRef: DedupedList[DataRef]] = defaultdict(DedupedList)
        self.static_to_dynamic_deps: DefaultDict[DataRef: DedupedList[DataRef]] = defaultdict(DedupedList)

        for input in self.get_all_inputs().values():
            input: RunnerNodeInput
            input.unresolve()
        
    def _unresolve_input(self, input: RunnerNodeInput):
        self.state.resolved = False
        self._reset_execution_state()
        
        input.unresolve()

    def _reset_execution_state(self):
        self.state.evaluated = False
        self.result: DataRef = None
        self.result_cached: bool = False

        # Time
        self.status: str = None
        self.start_time: float = None
        self.end_time: float = None
        self.duration: float = None

        self.touched_nodes.clear()
        self.released_nodes.clear()
        self.emitted_nodes = DedupedList()


    def _reset_runtime_state(self):

        self._reset_execution_state()
    
        for input in self.get_all_inputs().values():
            input: RunnerNodeInput
            input.unresolve()

                
    def get_secondary_inputs(self):
        return {"p_args": self.p_args, "p_kwargs": self.p_kwargs, "k_args": self.k_args, "k_kwargs": self.k_kwargs}

    def get_primary_inputs(self):
        return {"args": self.args, "kwargs": self.kwargs}

    def get_all_inputs(self):
        return {
            "p_args": self.p_args, "p_kwargs": self.p_kwargs, "k_args": self.k_args, "k_kwargs": self.k_kwargs,
            "args": self.args, "kwargs": self.kwargs
        }

@dataclass
class RunnerNodeInput:
    raw: Any
    evaluated: Any = None
    normalised: Any = None
    resolved: Any = None

    _VALID_PROCESS_ORDER = [
        ProcessStates.RAW, 
        ProcessStates.EVALUATED, 
        ProcessStates.NORMALISED, 
        ProcessStates.RESOLVED
    ]

    deps: list = field(default_factory=list)
    states: Dict[ProcessStates, bool] = field(default_factory=lambda: {state: False for state in ProcessStates})
    
    @property
    def state(self) -> ProcessStates:
        if self.has_been(ProcessStates.RESOLVED): return ProcessStates.RESOLVED
        if self.has_been(ProcessStates.NORMALISED): return ProcessStates.NORMALISED
        if self.has_been(ProcessStates.EVALUATED): return ProcessStates.EVALUATED
        return ProcessStates.RAW

    def has_been(self, processed_state: ProcessStates) -> bool:  
        if not isinstance(processed_state, ProcessStates):
            raise TypeError(f"{processed_state} is not a valid state type: use ProcessStates enum [RAW | EVALUATED | NORMALISED | RESOLVED]!")  
        return self.states[processed_state]

    def can_transition(self, current_state: ProcessStates, new_state: ProcessStates) -> bool:
        return self._VALID_PROCESS_ORDER.index(new_state) == self._VALID_PROCESS_ORDER.index(current_state) + 1

    def update_state(self, processed_state: ProcessStates, processed_input: Any) -> Self:
        if not isinstance(processed_state, ProcessStates):
            raise TypeError(f"{processed_state} is not a valid state type: use ProcessStates enum [RAW | EVALUATED | NORMALISED | RESOLVED]!") 
        
        current_state = self.state

        if processed_state == current_state and current_state == ProcessStates.RESOLVED:
            pass
        else: 
            if not self.can_transition(current_state, processed_state):
                raise RuntimeError(f"couldnt transition from {current_state.value} to {processed_state.value}! Make sure progression order is 'raw, evaluated, normalised, and resolved'!")
        
        self.set_state_value(processed_state, processed_input)
        self.states[processed_state] = True
    
    def set_state_value(self, processed_state: ProcessStates, value: Any) -> None:
        if self.state == processed_state:
            return 
        state = processed_state.value
        setattr(self, f"{state}", value)

    def get_value(self, state: ProcessStates):
        if not isinstance(state, ProcessStates):
            raise TypeError(f"{state} is not a valid state type: use ProcessStates enum [RAW | EVALUATED | NORMALISED | RESOLVED]!") 
        return getattr(self, state.value)
    
    def unresolve(self):
        if self.states[ProcessStates.RESOLVED]:
            self.states[ProcessStates.RESOLVED] = False
            self.resolved = None

    def get_previous_state(self, state):
        if not isinstance(state, ProcessStates) or state == ProcessStates.RAW:
            raise TypeError(f"{state} is not a valid state type: use ProcessStates enum [EVALUATED | NORMALISED | RESOLVED]!")
        return self._VALID_PROCESS_ORDER[self._VALID_PROCESS_ORDER.index(state) - 1]

    def get_previous_value(self, state: ProcessStates):
        previous_state = self.get_previous_state(state)
        return getattr(self, previous_state.value)

@dataclass
class NodeInput:
    raw: Any = None

# Node - A 
@dataclass 
class Node: 
 
    # Node ID
    id: int = field(init=False)
    content_id: int = field(init=False)


    # Node function / Targets
    func: Callable = None 

    # Primary Inputs 
    args:   NodeInput | Any | str | DedupedList | tuple = field(default_factory=tuple) 
    kwargs: NodeInput | dict = field(default_factory=dict) 

    # Secondary Inputs
    p_args   : NodeInput | tuple = field(default_factory=tuple)
    p_kwargs : NodeInput | dict = field(default_factory=dict)
    k_args   : NodeInput | tuple = field(default_factory=tuple)               
    k_kwargs : NodeInput | dict = field(default_factory=dict)
    
    targets: str | DataRef | List[str | DataRef] | None = None # TODO-NOTE: Support targets DedupedLists [X]
    effects: DedupedList = field(default_factory=DedupedList)

    node_name: str | None = None

    inject_data_store: bool = False
    resolve_output: bool = False
    recompute:bool = False
    one_shot:bool = False
    unpack: bool = False


    

    # Keep counter of seen nodes to return a new id for that function, able to index against route_DedupedList
    _node_counter: ClassVar[int] = 0  

    def __post_init__(self): 
        #apply and increment id
        self.id = uuid4().hex
        # Node._node_counter += 1

        self.content_id = repr((str(self.func.__name__), self.args, self.kwargs, self.p_args, self.p_kwargs, self.k_args, self.k_kwargs, self.targets, self.unpack)).encode("utf-8") 
        
        # #prevalidation
        # if not self.args and not callable(self.args): #self.args.raw
        #     raise NodeDefinitionError(f"RouteDefinitonError[@NodeID:{self.id}][{self.node_name if self.node_name else self.targets}] | PosArgs: {self.args} must be a Any or DataRef!")
        
        self.args     = NodeInput(raw=self.args if self.args is not None else tuple()) 
        self.p_args   = NodeInput(raw=self.p_args if self.p_args is not None else tuple())
        self.p_kwargs = NodeInput(raw=self.p_kwargs if self.p_kwargs is not None else {})
        
        self.kwargs   = NodeInput(raw=self.kwargs if self.kwargs is not None else {}) 
        self.k_args   = NodeInput(raw=self.k_args if self.k_args is not None else tuple())
        self.k_kwargs = NodeInput(raw=self.k_kwargs if self.k_kwargs is not None else {})
        
        if self.effects is None:
            self.effects = DedupedList()
        elif isinstance(self.effects, Effect) or (
            isinstance(self.effects, type)
            and issubclass(self.effects, Effect)
        ):
            self.effects = DedupedList([self.effects])
        elif isinstance(self.effects, (list, tuple, set, DedupedList)):
            self.effects = DedupedList(self.effects)
        else:
            raise NodeDefinitionError(
                "EffectDefError | effects must be an Effect or an iterable "
                f"of Effects, was {type(self.effects).__name__}"
            )

        for effect in self.effects:
            if not isinstance(effect, Effect):
                if not (
                    isinstance(effect, type)
                    and issubclass(effect, Effect)
                ):
                    raise NodeDefinitionError(f"EffectDefError[@NodeID:{self.id}] | Effects : [{type(effect)}, {effect}] must be an valid Event class! [RETURN, WRITE, DELETE, RAISE, END, RUN or EFFECT - from tide_tooling import WRITE, RAISE, ... ]")

        if not isinstance(self.targets, (list, str, DataRef)) and self.targets is not None: # TODO-NOTE: Support targets DedupedLists [X]
            raise NodeDefinitionError(f"RouteDefinitonError[@NodeID:{self.id}] | Targets: {self.targets} must be a DedupedList!")
        
        if not callable(self.func):
            raise NodeDefinitionError(f"RouteDefinitionError[@NodeID:{self.id}] | Function : {self.func} must be callable")

        if not isinstance(self.unpack, bool):
            raise NodeDefinitionError(
                f"RouteDefinitionError[@NodeID:{self.id}] | unpack must be a bool"
            )
        
        self.targets = DataRef._normalise_data_refs(self.targets)
        self.node_name = self.node_name or f"{self.func.__name__}"  
                
        # post validation
        if not (isinstance(self.args, (NodeInput))): #self.args.raw
            raise NodeDefinitionError(f"RouteDefinitonError[@NodeID:{self.id}][{self.node_name}] | PosArgs: {self.args} must be a string, DedupedList, DedupedList, or tuple or callable!")
        if not (isinstance(self.kwargs, (NodeInput))): #self.kwargs.raw
            raise NodeDefinitionError(f"RouteDefinitonError[[@NodeID:{self.id}][{self.node_name}] | RouteKwargs: {self.kwargs} must be a dict or a callable!")



@dataclass 
class Dag:
    forward_map: DefaultDict[int, DedupedList[DataRef]] = field(default_factory = lambda: defaultdict(DedupedList))
    reverse_map: DefaultDict[DataRef, DedupedList[int]] = field(default_factory = lambda: defaultdict(DedupedList))

    static_deps: DefaultDict[DataRef, DedupedList[int]] = field(default_factory = lambda: defaultdict(DedupedList))
    dynamic_deps: DefaultDict[DataRef, DedupedList[int]] = field(default_factory = lambda: defaultdict(DedupedList))

    ready_nodes: DedupedList = field(default_factory = DedupedList)

    topo_order: DedupedList = field(default_factory=DedupedList)
    has_cycle: bool = False
    cycle_info: None = None

    def _register_entry(self, node_id:int , deps: Iterable[DataRef]):
        if isinstance(deps, DataRef):
            deps = {deps, }
        self.forward_map[node_id].extend(deps)
        for dep in deps:
            if dep.dynamic:
                self.dynamic_deps[dep].append(node_id)
            else:
                self.static_deps[dep].append(node_id)
            self.reverse_map[dep].append(node_id)
            pass

@dataclass
class IntentDag(Dag):
    def __post__init__():
        super().__init__()

@dataclass
class ExpandedDag(Dag):
    def __post__init__():
        super().__init__()    
    
@dataclass
class RuntimeDag:
   
    dag_to_run: Dag|None=None

    def __post_init__(self):
        if self.dag_to_run is None:
            raise RuntimeDagConsumptionError("Error consuming DAG to run!! No dag object was given!")
        if not isinstance(self.dag_to_run, (Dag)):
            raise RuntimeDagConsumptionError("Error consuming DAG to run!! DAG was not a valid IntentDag or ExpandedDag object")
        else:
            try:
                import copy 
                self.forward_map = copy.deepcopy(self.dag_to_run.forward_map)
                self.reverse_map = copy.deepcopy(self.dag_to_run.reverse_map)
                self.ready_nodes_queue = deque(copy.deepcopy(self.dag_to_run.ready_nodes))

                self.waiting_static_deps = copy.deepcopy(self.dag_to_run.static_deps)
                self.waiting_dynamic_deps = copy.deepcopy(self.dag_to_run.dynamic_deps)

   
                print("Successfully consumed Dag")
            except Exception as e:
                raise NodeToDagRegistrationError(f"RuntimeDagError - Error consuming DAG! {e}") from e
    
    def register_node(self, node, deps) -> bool:
        ''' Registers a node to the DAG. This includes updating ready_nodes_queue if registering_node has no deps '''

        if not isinstance(deps, list):
            warn(f"Register_node Error! Deps must be a LIST. Was {type(deps)}", UserWarning)
            raise NodeToDagRegistrationError(f"Error registering node to DAG! [{node.id} | {node.node_name}] Deps must be a DedupedList. Was {type(deps)}!")
        if self.forward_map[node.id]:
            warn(f"Register_node id:[{node.id}] ALREADY has an existing Dep DedupedList! Is this a duplicate Node!?")
            return
            raise NodeToDagRegistrationError(f"Error registering node to DAG! [{node.id} | {node.node_name}] already has an existing entry!")
        
        #register node
        try:
            if node.id not in self.forward_map:
                self.forward_map[node.id] = DedupedList(deps)
                for target in node.targets:
                    if target not in self.reverse_map: # TODO-NOTE: Support targets DedupedLists [X]
                        self.reverse_map[target]
                for dep in deps:
                    self.reverse_map[dep].append(node.id)
                    if dep.dynamic:
                        self.waiting_dynamic_deps[dep].append(node.id)
           
            if not self.forward_map[node.id]:
                self.release_node(node.id)

            print(f"Registered Node [{node.id}]!")
            
        except Exception as e:
            raise NodeToDagRegistrationError(f"Error registering node to DAG! [{node.id} | {node.node_name}] : {e}") from e

    def release_node(self, node_id):
        self.ready_nodes_queue.appendleft(node_id) # add to 

    # def satisfy_node_dependency(self, node_id: int, dependency: DataRef) -> None:
    #     node_dependencies = self.forward_map[node_id]
    #     node_dependencies.remove(dependency)

    #     print(f"[{node_id}] {dependency} satified")
        
    #     if not self.forward_map[node_id]:
    #         print(f"[{node_id}] satified for RELEASE by {dependency}")
    #         self.release_node(node_id)

    def satisfy_downstream_deps(self, node_or_target:Node| DataRef=None):
        touched_nodes = DedupedList()
        released_nodes = DedupedList()

        if not node_or_target:
            print("[satisfy_downstream_deps] Please provide at least a node to satisfy targets")
            raise ValueError("Failed to satisfy downstream deps! Node and Targets were both none. Nothing to release")

        if isinstance(node_or_target, DataRef):
            id = node_or_target
            targets = node_or_target
        else:
            id = node_or_target.id 
            targets = node_or_target.targets

        if targets is not None:
            
            if targets in self.reverse_map:
                for dependent_node_id in DedupedList(self.reverse_map[targets]):
                    # remove dep from dep_node forward maps
                    if dependent_node_id in self.forward_map and targets in self.forward_map[dependent_node_id]:
                        try:
 
                            self.forward_map[dependent_node_id].remove(targets)

                        except ValueError:
                            warn(f"[SATISFY] dep {targets} was not present in forward map, is this a duplicate event?")
                            continue
                        except KeyError:
                            warn(f"[SATISFY] node_id {dependent_node_id} was not present in forward map, is this a duplicate event?")
                        touched_nodes.append(dependent_node_id)

                        # if dep_node no longer has any deps - release it
                        if not list(self.forward_map[dependent_node_id]):
                            del self.forward_map[dependent_node_id]
                            released_nodes.append(dependent_node_id)
                            print(f"[{id}] READIED {dependent_node_id} for RELEASE")
                            self.release_node(dependent_node_id)
                    
                released_display = (
                    "NO NODES" if not released_nodes else released_nodes
                )
                touched_display = (
                    "NO NODES" if not touched_nodes else touched_nodes
                )
                print(
                    f"[{id}] RELEASED [{released_display}] "
                    f"| TOUCHED [{touched_display}]!"
                )
                return touched_nodes, released_nodes    

        print(f"[{id}] NOTHING TO RELEASE | NO targets TARGETs | NO DEPENDENT NODES!")   
        return None, None

@dataclass
class PostRuntimeDag(Dag):
    def __post__init__() -> None:
        super().__init__()



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


class Effect:
    def __init__(self):
        self.preserve_output = __TEMPLATE_VAL__

    @property
    def kind(self):
        return type(self).__name__

    @staticmethod
    def _plain_value(value):
        if isinstance(value, __TIDE_TEMPLATE_VALUE__):
            return repr(value)
        if isinstance(value, DataRef):
            return DataRef.to_string(value, strip_leading=True)
        if isinstance(value, Node):
            return value.node_name
        if isinstance(value, dict):
            return {
                Effect._plain_value(key): Effect._plain_value(item)
                for key, item in value.items()
            }
        if isinstance(value, (list, tuple, set, DedupedList)):
            return [Effect._plain_value(item) for item in value]
        return value

    def to_dict(self):
        payload = {"effect": self.kind}
        payload.update({
            name: self._plain_value(value)
            for name, value in vars(self).items()
        })
        return payload

    def __str__(self):
        details = [
            f"{name}={value!r}"
            for name, value in self.to_dict().items()
            if name not in {"effect", "preserve_output"}
        ]
        return f"{self.kind}({', '.join(details)})"

    def __repr__(self):
        return str(self)

class EMIT(Effect):
    def __init__(self, nodes: DedupedList=None):
        super().__init__()
        if isinstance(nodes, Node):
            nodes = [nodes]
        self.nodes = nodes
        

class END(Effect):
    def __init__(self, runner_ids: int | DedupedList =__TEMPLATE_VAL__, preserve_output: Any=__TEMPLATE_VAL__):
        super().__init__()
        if isinstance(runner_ids, int):
            runner_ids = [runner_ids]
             
        self.targets = runner_ids
        self.preserve_output = preserve_output

class RUN(Effect):
    def __init__(self, runner_ids: int | DedupedList =__TEMPLATE_VAL__, mode="auto", stay_alive=False, templates=None, preserve_output: Any=__TEMPLATE_VAL__):
        super().__init__()
        if not templates: templates = {}
        if isinstance(runner_ids, int):
            runner_ids = [runner_ids]
             
        self.targets = runner_ids
        self.mode = mode
        self.templates = templates
        self.stay_alive = stay_alive
        self.preserve_output = preserve_output




class DELETE(Effect):
    def __init__(self, data_refs_or_dep_strs: str | DataRef | DedupedList[str | DataRef], preserve_output: Any=__TEMPLATE_VAL__):
        super().__init__()
        
        data_refs = DataRef._normalise_data_refs(data_refs_or_dep_strs)
    
        self.preserve_output = preserve_output
        self.targets = data_refs

class WRITE(Effect):
    def __init__(self, data_refs_or_dep_strs: str | DataRef | DedupedList[str | DataRef], preserve_output: Any=__TEMPLATE_VAL__):
        super().__init__()
        data_refs = DataRef._normalise_data_refs(data_refs_or_dep_strs)
    
        self.preserve_output = preserve_output
        self.targets = data_refs

class RAISE(Exception):
    def __init__(self, exception: Exception | str=None, effects:DedupedList=None, preserve_output:Any=__TEMPLATE_VAL__):
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
            raise TypeError(
                "RAISE exception must be an Exception, Exception type, "
                "string, or None"
            )

        super().__init__(str(exception))
        self.exception = exception
        self.preserve_output = preserve_output
        self.effects = list(effects or ())

    def to_dict(self):
        return {
            "effect": type(self).__name__,
            "exception": repr(self.exception),
            "preserve_output": Effect._plain_value(self.preserve_output),
            "effects": Effect._plain_value(self.effects),
        }

    def __str__(self):
        return f"RAISE(exception={self.exception!r})"


_current_effect_runtime = ContextVar(
    "current_tide_effect_runtime",
    default=None,
)


def READ(reference, fallback=__TEMPLATE_VAL__):
    """Read a reference during TIDE execution and record the attempted read.

    Outside a TIDE node runtime there is no store context, so the fallback is
    returned without side effects. The default fallback is TIDE's template
    sentinel.
    """
    refs = DataRef._normalise_data_refs(reference)
    if len(refs) != 1:
        raise ValueError("READ requires exactly one static TIDE reference")

    ref = refs[0]
    if ref.dynamic:
        raise ValueError("READ dynamic references are not supported yet")

    runtime = _current_effect_runtime.get()
    if runtime is None:
        return fallback

    if runtime.get("inside_process_pool"):
        raise ReadInsideProcessPoolError(
            "READ cannot observe TIDE's live store from inside a process pool; "
            "retry this execution in the thread pool"
        )

    ref_key = DataRef.to_string(ref, strip_leading=True)
    read_values = runtime.get("read_values", {})
    found = ref_key in read_values
    value = read_values[ref_key] if found else fallback
    runtime.setdefault("effects", []).append({
        "effect": Effects.READ,
        "reference": ref,
        "found": found,
        "value": value,
    })
    return value


def DELETE_NOW(reference):
    """Delete one reference during TIDE execution and report success.

    Unlike :class:`DELETE`, this is an imperative runtime operation: the live
    store is changed before the function continues.  Outside a TIDE node there
    is no store to mutate, so ``False`` is returned without side effects.
    """
    refs = DataRef._normalise_data_refs(reference)
    if len(refs) != 1:
        raise ValueError("DELETE_NOW requires exactly one static TIDE reference")

    ref = refs[0]
    if ref.dynamic:
        raise ValueError("DELETE_NOW dynamic references are not supported yet")

    runtime = _current_effect_runtime.get()
    if runtime is None:
        return False

    if runtime.get("inside_process_pool"):
        raise DeleteNowInsideProcessPoolError(
            "DELETE_NOW cannot mutate TIDE's live store from inside a "
            "process pool; retry this execution in the thread pool"
        )

    delete_now = runtime.get("delete_now")
    if not callable(delete_now):
        raise RuntimeError("DELETE_NOW runtime is missing its live-store handler")

    return bool(delete_now(ref))
        
