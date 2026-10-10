from functools import wraps
from importlib import import_module


def install_now_argument_binding_patch():
    """Install TIDE's in-memory noWorkflow argument-binding compatibility patch."""
    Collector = import_module(
        "noworkflow.now.collection.prov_execution.collector"
    ).Collector

    original = Collector.match_arguments
    if getattr(original, "_tide_now_compat", False):
        return

    @wraps(original)
    def match_arguments(
        collector,
        activation,
        function_def,
        original_args,
        original_kwargs,
        default_values,
        default_dependencies,
        params,
    ):
        args, vararg, kwarg, kwonlyargs = params
        accepts_arguments = args or vararg or kwarg or kwonlyargs

        # In-memory noWorkflow patch: a callable passed to another function can
        # leave its caller's argument evidence aliased into a later zero-argument
        # call. Give that call an empty binding view; the caller's evidence stays
        # intact and remains recorded against the call where it was observed.
        if not original_args and not original_kwargs and not accepts_arguments:
            dependencies = activation.dependencies[1]
            if any(
                dependency.mode.startswith("argument")
                for dependency in dependencies.dependencies
            ):
                activation.dependencies[1] = dependencies.clone(extra_only=True)

        return original(
            collector,
            activation,
            function_def,
            original_args,
            original_kwargs,
            default_values,
            default_dependencies,
            params,
        )

    match_arguments._tide_now_compat = True
    Collector.match_arguments = match_arguments
