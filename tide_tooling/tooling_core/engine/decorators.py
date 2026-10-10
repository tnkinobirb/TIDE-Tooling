import contextvars
from functools import wraps
from tide_tooling.tooling_core.engine.classes import Pipeline, Node
from tide_tooling.tooling_core.engine.errors import NodeDefinitionError

current_pipeline_build = contextvars.ContextVar("current_pipeline_build", default=None)

def pipeline(func):
    if not callable(func):
        raise TypeError("@pipeline requires a callable")

    @wraps(func)
    def wrapper(*args, **kwargs):
        # Collect definitions created by this Pipeline invocation.
        collected = []
        token = current_pipeline_build.set(collected)
        try:
            returned = func(*args, **kwargs)
        finally:
            current_pipeline_build.reset(token)

        # Use an explicit return or every definition created by the invocation.
        nodes = collected if returned is None else returned
        return Pipeline(nodes=nodes, name=func.__name__)

    return wrapper


def node(
    _func=None,
    *,
    targets=None,
    args=None,
    kwargs=None,
    p_args=None,
    p_kwargs=None,
    k_args=None,
    k_kwargs=None,
    effects=None,
    node_name=None,
    force_update=False,
    one_shot=False,
    unpack=False,
    share_implicit_target=False,
):
    def decorator(func):
        if not callable(func):
            raise NodeDefinitionError("@node requires a callable")

        def as_node(*call_args, **call_kwargs):
            # Do not combine declaration-time and call-time inputs.
            declared_inputs = args is not None or kwargs is not None
            called_with_inputs = bool(call_args or call_kwargs)
            if declared_inputs and called_with_inputs:
                raise NodeDefinitionError(
                    f"Node {func.__name__} has declaration-time inputs and cannot also receive call-time inputs"
                )

            # Select the explicit or ordinary function-call inputs.
            node_args = args if args is not None else call_args
            node_kwargs = kwargs if kwargs is not None else call_kwargs
            definition = Node(
                func=func,
                args=node_args,
                kwargs=node_kwargs,
                p_args=p_args,
                p_kwargs=p_kwargs,
                k_args=k_args,
                k_kwargs=k_kwargs,
                targets=targets,
                effects=effects,
                node_name=node_name,
                force_update=force_update,
                one_shot=one_shot,
                unpack=unpack,
                share_implicit_target=share_implicit_target,
            )

            # Retain definitions created inside the active Pipeline build.
            collected = current_pipeline_build.get()
            if collected is not None:
                collected.append(definition)
            return definition

        @wraps(func)
        def wrapper(*call_args, **call_kwargs):
            # Execute normally outside a Pipeline build.
            if current_pipeline_build.get() is None:
                return func(*call_args, **call_kwargs)
            return as_node(*call_args, **call_kwargs)

        # Expose the original callable and explicit definition constructor.
        wrapper.raw = func
        wrapper.as_node = as_node
        wrapper.__tide_node_wrapper__ = True
        wrapper.__tide_node_func__ = func
        wrapper.__tide_node_effects__ = effects
        return wrapper

    # Support bare @node and configured @node(...).
    if _func is None:
        return decorator
    if not callable(_func):
        raise NodeDefinitionError(
            "@node accepts only a callable positional argument; configure it using keyword arguments"
        )
    return decorator(_func)
