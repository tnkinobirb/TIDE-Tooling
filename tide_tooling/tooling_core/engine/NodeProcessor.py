import inspect
from collections import defaultdict

from .classes import DataRef, Node, ProcessStates
from .errors import (
    CallableDependencyError,
    DynamicRefCountLessThanAvailableKeysError,
    MissingRefDuringResolutionError,
    NodeFailed,
)
from .utils import tide_snapshot_value


class NodeProcessor:
    def __init__(self, dep_tools):
        self.dep_tools = dep_tools

    def _run_stage(self, node_input, state, transform, *args):
        # Apply one unfinished stage to the previous input boundary.
        if node_input.has_been(state):
            return None

        value, evidence = transform(
            node_input.get_previous_value(state),
            *args,
        )
        node_input.update_state(state, value)
        return evidence

    def evaluate_input(self, value, node, input_name):
        # Evaluate callable inputs; leave ordinary values unchanged.
        if not callable(value):
            return value, None

        args, kwargs = self._injection_inputs(node, input_name)
        return (
            self._evaluate_callable(
                value,
                args,
                kwargs,
            ),
            None,
        )

    def _injection_inputs(self, node, input_name):
        # Select inputs supplied to an args/kwargs factory.
        if input_name == "args":
            return node.p_args.normalised, node.p_kwargs.normalised
        if input_name == "kwargs":
            return node.k_args.normalised, node.k_kwargs.normalised
        return (), {}

    def _evaluate_callable(self, function, args, kwargs):
        # Normalise the supplied call shape.
        if args is None:
            args = ()
        elif not isinstance(args, (list, tuple)):
            args = (args,)

        if kwargs is None:
            kwargs = {}
        if not isinstance(kwargs, dict):
            raise TypeError("Callable input kwargs must be a dict")

        # Bind before calling so signature failures are explicit.
        try:
            bound = inspect.signature(function).bind(*args, **kwargs)
        except TypeError as error:
            raise CallableDependencyError(
                f"Failed to bind callable input: {error}"
            ) from error
        return function(*bound.args, **bound.kwargs)

    def normalise_input(self, value):
        # Convert one dependency string into a DataRef.
        if (
            isinstance(value, str)
            and value.startswith("@")
            and value != "@"
            and not value.endswith(".")
        ):
            value = DataRef.from_string(value)

        # Retain one dependency.
        if isinstance(value, DataRef):
            return value, ([value], [])

        # Represent one nested Node through its output targets.
        if isinstance(value, Node):
            if not value.targets:
                target_id = (
                    value.node_name
                    if value.share_implicit_target
                    else value.id
                )
                value.targets = [
                    DataRef(
                        f"@tmp_node_link.{target_id}",
                        "__tide__",
                    )
                ]

            normalised = (
                value.targets[0]
                if len(value.targets) == 1
                else value.targets
            )
            return normalised, (list(value.targets), [value])

        # Normalise every dictionary value.
        if isinstance(value, dict):
            normalised = {}
            deps = []
            input_nodes = []

            for key, child in value.items():
                child, found = self.normalise_input(child)
                normalised[key] = child
                deps.extend(found[0])
                input_nodes.extend(found[1])

            return normalised, (deps, input_nodes)

        # Normalise every sequence value.
        if isinstance(value, (list, tuple, set)):
            source = (
                sorted(value, key=repr)
                if isinstance(value, set)
                else value
            )
            normalised = []
            deps = []
            input_nodes = []

            for child in source:
                child, found = self.normalise_input(child)
                normalised.append(child)
                deps.extend(found[0])
                input_nodes.extend(found[1])

            container = (
                tuple(normalised)
                if isinstance(value, set)
                else type(value)(normalised)
            )
            return container, (deps, input_nodes)

        # Ordinary values require no normalisation.
        return value, ([], [])

    def _compile_input(self, node, input_name, node_input):
        # Evaluate one input, then normalise its discovered contents.
        self._run_stage(
            node_input,
            ProcessStates.EVALUATED,
            self.evaluate_input,
            node,
            input_name,
        )
        deps, input_nodes = self._run_stage(
            node_input,
            ProcessStates.NORMALISED,
            self.normalise_input,
        )

        # Attach exactly what normalisation discovered.
        node_input.deps.extend(deps)
        node.deps.extend(deps)
        node._input_nodes.extend(input_nodes)
        for dep in deps:
            node.dep_to_input_map[dep].append((input_name, node_input))

    def compile_all_inputs(self, node):
        # Compile every input through the same two stages.
        for input_name, node_input in node.get_all_inputs().items():
            try:
                self._compile_input(node, input_name, node_input)
            except Exception as error:
                raise NodeFailed(node.id, input_name, error) from error

    def _resolve_dependencies(
        self,
        value,
        reference_unscoper,
        node,
        path=(),
    ):
        # Resolve one dependency occurrence.
        if isinstance(value, DataRef):
            if value.dynamic:
                dynamic_ref = reference_unscoper(value)
                available_refs = self.dep_tools.matching_refs(dynamic_ref)
                required = value.count if value.count is not None else 1
                if len(available_refs) < required:
                    raise DynamicRefCountLessThanAvailableKeysError(
                        f"{value}.count was more than available keys!",
                        value,
                        tuple(ref.key for ref in available_refs),
                    )

            selected_refs = (
                node.dynamic_to_static_deps[value][:value.max]
                if value.dynamic
                else (value,)
            )
            dynamic_ref = (
                reference_unscoper(value)
                if value.dynamic
                else None
            )
            resolved_values = []
            reads = []

            for occurrence, selected in enumerate(selected_refs):
                ref = reference_unscoper(selected)
                resolved = self.dep_tools.resolve_exact(ref)
                resolved_values.append(resolved)
                reads.append((
                    ref,
                    resolved,
                    tide_snapshot_value(resolved, boundary="read"),
                    dynamic_ref,
                    path + (occurrence,) if value.dynamic else path,
                ))

            return (
                resolved_values if value.dynamic else resolved_values[0],
                reads,
            )

        # Resolve dependencies inside a dictionary.
        if isinstance(value, dict):
            resolved = {}
            reads = []

            for key, child in value.items():
                child, child_reads = self._resolve_dependencies(
                    child,
                    reference_unscoper,
                    node,
                    path + (key,),
                )
                resolved[key] = child
                reads.extend(child_reads)

            return resolved, reads

        # Resolve dependencies inside a list or tuple.
        if isinstance(value, (list, tuple)):
            resolved = []
            reads = []

            for index, child in enumerate(value):
                child, child_reads = self._resolve_dependencies(
                    child,
                    reference_unscoper,
                    node,
                    path + (index,),
                )
                resolved.append(child)
                reads.extend(child_reads)

            return type(value)(resolved), reads

        # Ordinary values remain unchanged.
        return value, []

    def _resolve_input(
        self,
        reference_unscoper,
        node,
        node_input,
    ):
        if node_input.has_been(ProcessStates.RESOLVED):
            return [], []

        value = node_input.get_previous_value(ProcessStates.RESOLVED)

        # Resolve one scalar input as one complete input.
        if not isinstance(value, (dict, list, tuple)):
            resolved, reads = self._resolve_dependencies(
                value,
                reference_unscoper,
                node,
            )
            node_input.update_state(ProcessStates.RESOLVED, resolved)
            unrepresented = []
            if not reads:
                unrepresented.append((
                    resolved,
                    tide_snapshot_value(resolved, boundary="read"),
                    (),
                ))
            return reads, unrepresented

        # Resolve each actual positional or keyword input once.
        is_mapping = isinstance(value, dict)
        items = value.items() if is_mapping else enumerate(value)
        resolved = {} if is_mapping else []
        reads = []
        unrepresented = []

        for position, child in items:
            whole_input_is_represented = isinstance(child, DataRef)
            child, child_reads = self._resolve_dependencies(
                child,
                reference_unscoper,
                node,
                (position,),
            )
            if is_mapping:
                resolved[position] = child
            else:
                resolved.append(child)
            reads.extend(child_reads)

            # Observe the whole input unless one dependency represents it exactly.
            if not whole_input_is_represented:
                unrepresented.append((
                    child,
                    tide_snapshot_value(child, boundary="read"),
                    (position,),
                ))

        resolved = resolved if is_mapping else type(value)(resolved)
        node_input.update_state(ProcessStates.RESOLVED, resolved)
        return reads, unrepresented

    def resolve_all_inputs(
        self,
        reference_unscoper,
        node,
    ):
        reads = defaultdict(list)
        unrepresented = defaultdict(list)

        # Resolve every input through the same final stage.
        for input_name, node_input in node.get_all_inputs().items():
            try:
                input_reads, input_unrepresented = self._resolve_input(
                    reference_unscoper,
                    node,
                    node_input,
                )
            except (
                MissingRefDuringResolutionError,
                DynamicRefCountLessThanAvailableKeysError,
            ):
                raise
            except Exception as error:
                raise NodeFailed(node.id, input_name, error) from error

            reads[input_name].extend(input_reads)
            unrepresented[input_name].extend(input_unrepresented)

        return reads, unrepresented

    def _extract_nodes_from_result(self, value):
        # Yield every nested Node from one returned value.
        if isinstance(value, Node):
            yield value
            return
        if isinstance(value, (list, tuple, set)):
            for child in value:
                yield from self._extract_nodes_from_result(child)
            return
        if isinstance(value, dict):
            for child in value.values():
                yield from self._extract_nodes_from_result(child)

    def _fill_cache(self, node, result):
        # Store one completed result.
        node.result = result

    def _satisfy(self, node):
        # A completed node requires every input to be resolved.
        if not all(
            node_input.has_been(ProcessStates.RESOLVED)
            for node_input in node.get_all_inputs().values()
        ):
            raise NodeFailed(
                node.id,
                "",
                f"Node {node.id} has unresolved inputs",
            )
        node.evaluated = True
