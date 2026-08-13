import functools
import contextvars
from warnings import warn
from functools import wraps
from tide_tooling.tooling_core.engine.classes import Pipeline, Node
from tide_tooling.tooling_core.engine.errors import NodeDefinitionError

current_pipeline_build = contextvars.ContextVar("current_pipeline_build", default=None)

def _collect_pipeline_node(node):
    collected = current_pipeline_build.get()
    if collected is not None and all(existing is not node for existing in collected):
        collected.append(node)
    return node

def _nested_nodes(value):
    if isinstance(value, Node):
        yield value
        return
    if isinstance(value, dict):
        for nested in value.values():
            yield from _nested_nodes(nested)
        return
    if isinstance(value, (list, tuple, set)):
        for nested in value:
            yield from _nested_nodes(nested)

def _pipeline_roots(collected):
    consumed = set()
    for collected_node in collected:
        inputs = (
            collected_node.args.raw,
            collected_node.kwargs.raw,
            collected_node.p_args.raw,
            collected_node.p_kwargs.raw,
            collected_node.k_args.raw,
            collected_node.k_kwargs.raw,
        )
        consumed.update(id(nested) for nested in _nested_nodes(inputs))
    return [node for node in collected if id(node) not in consumed]

def _mark_node_wrapper(wrapper, func, as_node, effects=None):
    wrapper.__tide_node_wrapper__ = True
    wrapper.__tide_node_func__ = func
    wrapper.raw = func
    wrapper.as_node = as_node
    wrapper.__tide_node_effects__ = effects
    return wrapper

def pipeline(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        collected = []
        token = current_pipeline_build.set(collected)
        try:
            result = func(*args, **kwargs)
        finally:
            current_pipeline_build.reset(token)

        stages = result
        if result is None:
            stages = _pipeline_roots(collected)
            if not stages:
                raise ValueError(f"@pipeline {func.__name__} did not build or return any nodes")

        pipeline = Pipeline(stages=stages, name=func.__name__)
        return pipeline

    return wrapper


def node(
    *d_args,
    targets=None,
    args=None,
    kwargs=None,
    p_args=None,
    p_kwargs=None,
    k_args=None,
    k_kwargs=None,
    effects=None,
    unpack=False,
    **d_kwargs,
):
    if len(d_args) == 1 and callable(d_args[0]) and not d_kwargs:
        func = d_args[0]

        def as_node(*call_args, **call_kwargs):
            built_node = Node(func=func, args=call_args, kwargs=call_kwargs)
            return _collect_pipeline_node(built_node)

        @wraps(func)
        def wrapper(*args, **kwargs):
            if current_pipeline_build.get() is None:
                return func(*args, **kwargs)
            return as_node(*args, **kwargs)

        return _mark_node_wrapper(wrapper, func, as_node, effects=[])
    # else: targets = d_args[0] if isinstance(d_args, (str, list)) and not targets else None
        
    def decorator(func):
        def as_node(*call_args, **call_kwargs):
            _effects =  [] if effects is None else effects
            _targets = [] if targets is None else targets

            declared_inputs = args is not None or kwargs is not None
            called_with_inputs = bool(call_args or call_kwargs)
            if declared_inputs and called_with_inputs:
                raise NodeDefinitionError(
                    f"Node {func.__name__} has declaration-time inputs and "
                    "cannot also receive call-time inputs"
                )

            node_args = args if declared_inputs and args is not None else call_args
            node_kwargs = kwargs if declared_inputs and kwargs is not None else call_kwargs
     
            built_node = Node(
                func=func, 
                args=node_args,
                kwargs=node_kwargs,
                p_args=p_args,
                p_kwargs= p_kwargs,
                k_args=k_args,
                k_kwargs=k_kwargs,
                effects=_effects,
                targets=_targets,
                unpack=unpack,
                **d_kwargs
            )
            return _collect_pipeline_node(built_node)

        @wraps(func)
        def wrapper(*args, **kwargs):
            if current_pipeline_build.get() is None:
                return func(*args, **kwargs)
            return as_node(*args, **kwargs)

        return _mark_node_wrapper(wrapper, func, as_node, effects=effects)
    return decorator

current_route = contextvars.ContextVar("current_route", default=None)

def route_function(source_def: dict[str: str], source_param: str="sources"):
    
    def wrapper(func):
        # set func metadata
        func._source_def = source_def
        func._source_param = source_param
        func._helper_trace = {}
        
        # if run , set as current_route value, 
        @functools.wraps(func)
        def wrapped_route(*args, **kwargs):
            # set func as current_route
            token = current_route.set(func)
            wrapped_route._last_call_args = args
            wrapped_route._last_call_kwargs = kwargs
            try:
                #run 
                return func(*args, **kwargs)
            finally:

                # if func._helper_trace:
                #     print(f"[HELPER TRACE SUMMARY for {func.__name__}]")
                #     for name, count in func._helper_trace.items():
                #         print(f"  - {name}: {count}\n")
                current_route.reset(token)
        return wrapped_route
    return wrapper        

#first draft route_helper func
def route_helper(func):
    '''
        @route_helper decorator:
            Decorate a function to append it's name to the @route_function's [_helper_trace] dict when called.
            Useful for tracking which helpers were called inside the original @route_function that called it.
            - If @route_helper is used on a non-@route_function, it will warn, and do nothing.
            Usage example: 
                
                @route_function
                def func(): <- has func._helper_trace dict
                    helper_func()
                    return
                
                @route_helper
                def helper_func():
                    return
            
    '''
    @functools.wraps(func)
    def decorator(*args, **kwargs):
        try:
            active = current_route.get()
            if active is not None and hasattr(active, "_helper_trace"):
                if getattr(active, "_helper_trace") is None:
                    warn("_helper_trace was found but is None — did @route_function forget to initialize it?")
                if func.__name__ not in active._helper_trace:
                    active._helper_trace[func.__name__] = 1
                else:
                    active._helper_trace[func.__name__] += 1
                
                #DEBUG_LOG(f"[HELPER-TRACE LOGGED : {func.__name__}] | Trace {active.__name__}: {active._helper_trace} |") <- add functionality to catch debug_logs
            elif active is not None:
                warn(f"{active.__name__}@route_function has no ._helper_trace dict. check @route_function wrapper?")
            else:
                warn(f"{func.__name__}@route_helper used outside of an active @route_function context.")
            return func(*args, **kwargs)
        except Exception: # find actual cases soon 
            raise #RuntimeError(f"problem appending @helper {func.__name__} onto helper_trace!") from e 
    return decorator      
