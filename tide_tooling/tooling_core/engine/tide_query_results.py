from __future__ import annotations

"""Structured query results and comparison views for TIDE traces.

These objects contain and render evidence already selected by ``Trace``.
Query execution and trace ownership remain in ``tide_logging``.  The legacy
module re-exports every public name here for import compatibility.
"""

import copy
from collections import defaultdict
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from tide_tooling.tooling_core.engine.tide_logging import Trace

from tide_tooling.tooling_core.engine.classes import __TIDE_TEMPLATE_VALUE__
from tide_tooling.tooling_core.engine.events import Effects
from tide_tooling.tooling_core.engine.tide_links import (
    AffectedStackLink,
    EmittedStackLink,
    ReadWriteLink,
    StackLink,
)
from tide_tooling.tooling_core.engine.tide_records import ExecRecord
from tide_tooling.tooling_core.engine.tide_rendering import (
    SHOW_BOLD,
    SHOW_CYAN,
    SHOW_DIM,
    SHOW_GREEN,
    SHOW_RED,
    SHOW_RESET,
    SHOW_SECTION_WIDTH,
    SHOW_TREE_BRANCH,
    SHOW_TREE_LAST,
    SHOW_TREE_PIPE,
    SHOW_WHITE,
    SHOW_YELLOW,
    _show_options,
    _show_render,
    display_identity,
    show_header,
    show_identity,
    show_query_display,
    show_section,
    shorten_identity,
)
from tide_tooling.tooling_core.engine.utils import DedupedList, safe_equal


__TEMPLATE__ = __TIDE_TEMPLATE_VALUE__()


def _query_json_value(value):
    """Return a JSON-compatible public query payload value."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value

    if isinstance(value, dict):
        return {
            str(key): _query_json_value(item)
            for key, item in value.items()
        }

    if isinstance(value, (list, tuple)):
        return [_query_json_value(item) for item in value]

    if isinstance(value, set):
        return [
            _query_json_value(item)
            for item in sorted(value, key=repr)
        ]

    if hasattr(value, "to_dict") and callable(value.to_dict):
        return _query_json_value(value.to_dict())

    return repr(value)


def _diff_payload(value):
    if isinstance(value, dict):
        return {
            key: _diff_payload(item)
            for key, item in value.items()
        }

    if isinstance(value, list):
        return [_diff_payload(item) for item in value]

    if isinstance(value, tuple):
        return tuple(_diff_payload(item) for item in value)

    if isinstance(value, set):
        return {
            _diff_payload(item)
            for item in value
        }

    if hasattr(value, "to_dict") and callable(value.to_dict):
        try:
            return _diff_payload(value.to_dict())
        except Exception:
            return repr(value)

    return value


def _diff_values(before, after, path=""):
    changes = {
        "changed": {},
        "added": {},
        "removed": {},
    }

    if isinstance(before, dict) and isinstance(after, dict):
        before_keys = set(before)
        after_keys = set(after)

        for key in sorted(before_keys - after_keys, key=repr):
            key_path = f"{path}.{key}" if path else str(key)
            changes["removed"][key_path] = before[key]

        for key in sorted(after_keys - before_keys, key=repr):
            key_path = f"{path}.{key}" if path else str(key)
            changes["added"][key_path] = after[key]

        for key in sorted(before_keys & after_keys, key=repr):
            key_path = f"{path}.{key}" if path else str(key)
            nested = _diff_values(before[key], after[key], key_path)
            for section, values in nested.items():
                changes[section].update(values)

        return changes

    if isinstance(before, (list, tuple)) and isinstance(after, (list, tuple)):
        shared = min(len(before), len(after))
        for index in range(shared):
            nested = _diff_values(
                before[index],
                after[index],
                f"{path}[{index}]" if path else f"[{index}]",
            )
            for section, values in nested.items():
                changes[section].update(values)

        for index in range(shared, len(before)):
            key_path = f"{path}[{index}]" if path else f"[{index}]"
            changes["removed"][key_path] = before[index]

        for index in range(shared, len(after)):
            key_path = f"{path}[{index}]" if path else f"[{index}]"
            changes["added"][key_path] = after[index]

        if type(before) is not type(after):
            changes["changed"][path or "value"] = {
                "before": before,
                "after": after,
            }

        return changes

    if isinstance(before, (set, frozenset)) and isinstance(after, (set, frozenset)):
        removed = [item for item in before if item not in after]
        added = [item for item in after if item not in before]
        key_path = path or "value"
        if removed:
            changes["removed"][key_path] = sorted(removed, key=repr)
        if added:
            changes["added"][key_path] = sorted(added, key=repr)
        return changes

    if not safe_equal(before, after):
        changes["changed"][path or "value"] = {
            "before": before,
            "after": after,
        }

    return changes


def _build_compared(subject, other):
    before = _diff_payload(subject)
    after = _diff_payload(other)
    changes = _diff_values(before, after)
    return Compared(
        object_type=type(subject).__name__,
        before=before,
        after=after,
        **changes,
    )


def _diff_inspected_target(value):
    """Return the underlying TIDE object represented by an inspection view."""
    if isinstance(value, Investigate):
        value = value.inspected

    if isinstance(value, Inspected):
        if value.grouped:
            return [
                getattr(item, "object", item)
                for item in value.object
            ]

        inspected_object = value.object
        return getattr(inspected_object, "object", inspected_object)

    return value


def _build_object_compared(subject, other, diff_object=True):
    """Build a diff for inspection views or their underlying objects.

    ``diff_object=True`` is the ergonomic default: an ``Inspected`` or
    ``Investigate`` view is compared by the actual object it represents.
    Passing ``False`` preserves the full view-level comparison.
    """
    if diff_object:
        subject = _diff_inspected_target(subject)
        other = _diff_inspected_target(other)

    return _build_compared(subject, other)


class Stream:

    def _execution_display(self, exec_id, actor=None):
        """Show a stable occurrence label without exposing an opaque ID."""
        inspect = getattr(self, "inspect", None)
        if inspect is not None:
            try:
                return repr(inspect(exec_id, "exec_record").object)
            except (KeyError, TypeError, ValueError):
                pass

        actor_name = getattr(actor, "source_actor", actor)
        if actor_name is not None:
            return str(actor_name)
        return shorten_identity(exec_id)

    @staticmethod
    def _stream_edge_label(edge):
        if isinstance(edge, EmittedStackLink):
            return "EMIT", SHOW_CYAN

        reference = edge.produced_ref
        if (
            isinstance(reference, str)
            and reference.startswith("__model__:")
        ):
            reference = reference.split(":", 1)[1]

        state = f"{reference}[v{edge.produced_ver}]"
        if isinstance(edge, AffectedStackLink):
            colour = (
                SHOW_RED
                if edge.cause_effect == Effects.DELETE
                else SHOW_GREEN
            )
            return f"{str(edge.cause_effect).upper()} at {state}", colour
        return state, SHOW_GREEN

    def _lines_from_complete_paths(self):
        """Render every causal branch as its own E -> S -> E chain."""
        lines = []
        path_number = 0

        for root_exec_id, paths in self.paths.items():
            for path in paths:
                path_number += 1
                if path_number > 1:
                    lines.append("")

                lines.append(
                    f"{SHOW_DIM}PATH {path_number}{SHOW_RESET}"
                )

                if not path:
                    continue

                first_edge = path[0]
                if self.direction == "upstream":
                    root_actor = first_edge.consumer_actor
                    root_relation = "consumer_exec_id"
                    next_actor = "producer_actor"
                    next_relation = "producer_exec_id"
                else:
                    root_actor = first_edge.producer_actor
                    root_relation = "producer_exec_id"
                    next_actor = "consumer_actor"
                    next_relation = "consumer_exec_id"

                root_id = getattr(first_edge, root_relation, root_exec_id)
                lines.append(
                    f"{SHOW_CYAN}"
                    f"{self._execution_display(root_id, root_actor)}"
                    f"{SHOW_RESET}"
                )

                level = 0
                for edge in path:
                    edge_label, edge_colour = self._stream_edge_label(edge)
                    lines.append(
                        f"{'    ' * level}{SHOW_DIM}{SHOW_TREE_LAST}"
                        f"{SHOW_RESET} {edge_colour}{edge_label}{SHOW_RESET}"
                    )
                    level += 1

                    related_exec_id = getattr(edge, next_relation)
                    related_actor = getattr(edge, next_actor)
                    lines.append(
                        f"{'    ' * level}{SHOW_DIM}{SHOW_TREE_LAST}"
                        f"{SHOW_RESET} {SHOW_CYAN}"
                        f"{self._execution_display(related_exec_id, related_actor)}"
                        f"{SHOW_RESET}"
                    )
                    level += 1

        return lines

    @staticmethod
    def _freeze_edge_value(value):
        if isinstance(value, dict):
            return tuple(
                (key, Stream._freeze_edge_value(entry))
                for key, entry in sorted(value.items())
            )
        if isinstance(value, (list, tuple)):
            return tuple(
                Stream._freeze_edge_value(entry)
                for entry in value
            )
        if isinstance(value, set):
            return tuple(sorted(
                (Stream._freeze_edge_value(entry) for entry in value),
                key=repr,
            ))
        try:
            hash(value)
        except TypeError:
            return repr(value)
        return value

    @classmethod
    def _edge_key(cls, edge):
        return (
            type(edge).__name__,
            cls._freeze_edge_value(edge.to_dict()),
        )

    @staticmethod
    def _execution_route(root_exec_id, path, direction):
        next_relation = (
            "producer_exec_id"
            if direction == "upstream"
            else "consumer_exec_id"
        )
        route = [root_exec_id]
        for edge in path:
            next_exec_id = getattr(edge, next_relation, None)
            if next_exec_id is not None:
                route.append(next_exec_id)
        return tuple(route)

    def count(self):
        return sum(
            len(paths)
            for paths in self.paths.values()
        )

    def execution_routes(self):
        return {
            self._execution_route(root_exec_id, path, self.direction)
            for root_exec_id, paths in self.paths.items()
            for path in paths
        }

    def execution_route_count(self):
        return len(self.execution_routes())

    @staticmethod
    def to_dict(stream_object: Upstream | Downstream, direction="unknown"):
        paths_by_root = stream_object.paths

        if direction == "upstream":
            root_actor = "consumer"
            next_actor = "producer"
        else:
            root_actor = "producer"
            next_actor = "consumer"

        root_execs = DedupedList()
        next = defaultdict(list)
        next_seen = defaultdict(set)
        edges = {}
        edge_id_by_key = {}
        path_edges = defaultdict(list)

        for path_root_exec_id, root_paths in paths_by_root.items():
            for path in root_paths:
                if not path:
                    continue
                root_edge = path[0]
                path_root_exec_id = getattr(
                    root_edge,
                    f"{root_actor}_exec_id",
                )
                root_execs.append(path_root_exec_id)

                encoded_path = []
                for edge in path:
                    edge_key = Stream._edge_key(edge)
                    edge_x = edge_id_by_key.get(edge_key)
                    if edge_x is None:
                        edge_x = f"edge_{len(edges)}"
                        edge_id_by_key[edge_key] = edge_x
                        edges[edge_x] = edge.to_dict()
                    encoded_path.append(edge_x)

                    curr_exec_id = getattr(edge, f"{root_actor}_exec_id")
                    next_exec_id = getattr(edge, f"{next_actor}_exec_id")
                    next_key = (next_exec_id, edge_x)
                    if next_key not in next_seen[curr_exec_id]:
                        next_seen[curr_exec_id].add(next_key)
                        next[curr_exec_id].append({
                            "exec": next_exec_id,
                            "edge": edge_x,
                        })

                path_edges[path_root_exec_id].append(encoded_path)

        return {
            "direction": direction,
            "roots": root_execs,
            "next": dict(next),
            "edges": edges,
            "paths": dict(path_edges),
            "path_count": stream_object.count(),
            "execution_route_count": (
                stream_object.execution_route_count()
            ),
            "truncated": bool(
                getattr(stream_object, "truncated", False)
            ),
            "max_paths": getattr(stream_object, "max_paths", None),
        }

    def diff(self, other):
        return _build_compared(self, other)

    @staticmethod
    def show(
        stream_object,
        direction: str = "unknown",
        color=True,
        simple=False,
        colour=None,
    ):
        color, simple = _show_options(color, simple, colour)

        inspected = getattr(stream_object, "inspected", None)
        query = (
            show_query_display(
                inspected.type,
                inspected.id,
                None if inspected.grouped else str(inspected.object),
                inspected.grouped,
                getattr(inspected, "version", None),
            )
            if inspected is not None
            else stream_object.__name__()
        )

        path_count = stream_object.count()
        route_count = stream_object.execution_route_count()
        summary = f"{path_count} {'path' if path_count == 1 else 'paths'} found"
        if route_count != path_count:
            summary += (
                f" | {route_count} execution "
                f"{'route' if route_count == 1 else 'routes'}"
            )
        if getattr(stream_object, "truncated", False):
            summary += f" | truncated at {stream_object.max_paths} paths"
        identifier = (
            show_identity(inspected.id, inspected.grouped)
            if inspected is not None
            else None
        )

        lines = show_header(
            f"{stream_object.__name__().upper()} {query}",
            summary,
            identifier,
        )
        lines.extend(show_section("PATHS"))

        if stream_object.paths:
            lines.extend(stream_object._lines_from_complete_paths())
        else:
            lines.append(f"{SHOW_YELLOW}└── No paths found{SHOW_RESET}")

        return _show_render(lines, color)

    @staticmethod
    def execs_from_stream(stream, direction):
        root = "consumer" if direction == "downstream" else "producer"
        seen = set()
        exec_ids = []

        for root_id, depths in stream.items():
            for depth, edges in depths.items():
                for edge in edges:
                    exec_id = getattr(edge, f"{root}_exec_id")

                    if exec_id not in seen:
                        seen.add(exec_id)
                        exec_ids.append(exec_id)

        return exec_ids


class Upstream(Stream):
    def __init__(
        self,
        inspected: Inspected,
        actor_stack: dict,
        stream_paths: list,
        *,
        inspect=None,
        truncated=False,
        max_paths=None,
    ):
        self.inspected = inspected
        self.stream = actor_stack
        self.paths = stream_paths
        self.direction = "upstream"
        self.inspect = inspect
        self.truncated = bool(truncated)
        self.max_paths = max_paths

    def show(self, color=True, simple=False, colour=None):
        return super().show(
            self,
            "upstream",
            color=color,
            simple=simple,
            colour=colour,
        )

    def to_dict(self):
        return super().to_dict(self, "upstream")

    def __name__(self):
        return "Upstream"


class Downstream(Stream):
    def __init__(
        self,
        inspected: Inspected,
        actor_stack: dict,
        stream_paths: list,
        *,
        inspect=None,
        truncated=False,
        max_paths=None,
    ):
        self.inspected = inspected
        self.stream = actor_stack
        self.paths = stream_paths
        self.direction = "downstream"
        self.inspect = inspect
        self.truncated = bool(truncated)
        self.max_paths = max_paths

    def show(self, color=True, simple=False, colour=None):
        return super().show(
            self,
            "downstream",
            color=color,
            simple=simple,
            colour=colour,
        )

    def to_dict(self):
        return super().to_dict(self, "downstream")
    
    def __name__(self):
        return "Downstream"


class Pathed(Stream):
    def __init__(self, inspect, index, source: Inspected, source_seeds , targeted: Inspected, target_seeds, paths, direction: str="unknown"):
        if direction == "unknown":
            raise ValueError("direction cannot be unknown, please specify")
        self.index = index
        self.inspect = inspect 
        self.source = source if isinstance(source, Inspected) else self.inspect(source)
        self.source_seeds = source_seeds
        self.targeted = targeted if isinstance(targeted, Inspected) else self.inspect(targeted)
        self.target_seeds = target_seeds
        self.direction = direction
        self.paths = {}
        for source_id, source_paths in paths.items():
            normalised_paths = []
            for path in source_paths:
                normalised_path = []
                previous_stack_link = None
                for hop in path:
                    if isinstance(hop, str):
                        hop = ReadWriteLink.from_text(hop)
                    if isinstance(hop, ReadWriteLink) and previous_stack_link is not None:
                        hop.attach_input_use(previous_stack_link.input_use())
                        if hop.relation == "from":
                            hop.producer = previous_stack_link.producer_actor
                        elif hop.relation == "used by":
                            hop.consumer = previous_stack_link.consumer_actor
                        elif hop.type == "write" and hop.producer is None:
                            hop.producer = previous_stack_link.producer_actor
                    normalised_path.append(hop)
                    previous_stack_link = (
                        hop
                        if isinstance(hop, StackLink)
                        else None
                    )
                normalised_paths.append(normalised_path)
            self.paths[source_id] = normalised_paths
 
   
        # super().show(self, direction)
        # self.show()


    def exists(self):
        return bool(self.paths)
    
    
    def count(self):
        return sum(
            len(paths)
            for paths in self.paths.values()
        )



    
    def get_path(self, number:int=0):
        '''defaults to first path if available'''
        all_paths = [
            path
            for source_paths in self.paths.values()
            for path in source_paths
        ]
        return all_paths[number] if 0 <= number < len(all_paths) else None
    
    def edges(self):
        return [
            edge
            for source_paths in self.paths.values()
            for path in source_paths
            for edge in path
        ]
    
    def _line_with_input_suffix(self, line, edge):
        if not isinstance(edge, StackLink):
            return line

        suffix = edge.input_suffix()

        if not suffix:
            return line

        if " from " in line:
            left, right = line.split(" from ", 1)
            return f"{left}{suffix} from {right}"

        if " used by " in line:
            left, right = line.split(" used by ", 1)
            return f"{left}{suffix} used by {right}"

        return f"{line}{suffix}"


    def _edge_to_dict(self, edge):
        if isinstance(edge, (StackLink, AffectedStackLink, ReadWriteLink)):
            return edge.to_dict()

        return edge


    def _source_display(self, exec_id):
        return self.inspect(exec_id, "exec_record").object


    def _path_query_display(self, inspected):
        return show_query_display(
            inspected.type,
            inspected.id,
            None if inspected.grouped else str(inspected.object),
            inspected.grouped,
            getattr(inspected, "version", None),
        )


    def _display_reference(self, reference):
        """Render imported State identity without its private Trace scope."""
        if (
            isinstance(reference, str)
            and reference.startswith("__model__:")
        ):
            return reference.split(":", 1)[1]
        return reference


    def _edge_read_text(self, edge):
        reference = self._display_reference(edge.produced_ref)
        return f"{reference}[v{edge.produced_ver}]"


    def _edge_input_display(self, edge):
        if not isinstance(edge, StackLink):
            return None

        use = edge.input_use()

        if use is None:
            return None

        return use["display"]


    def _edge_actor_display(self, actor):
        return actor.source_actor if hasattr(actor, "source_actor") else str(actor)


    def _edge_link_line(self, edge, direction, show_input_positions, dim, cyan, green, white, reset):
        ref_text = self._edge_read_text(edge)
        input_display = self._edge_input_display(edge)

        if direction == "upstream":
            consumer_actor = self._edge_actor_display(edge.consumer_actor)
            line = f"{dim}wrote{reset} {green}{ref_text}{reset} {dim}used by{reset} {cyan}{consumer_actor}{reset}"

            if show_input_positions and input_display:
                line += f" {dim}via{reset} {white}{input_display}{reset}"

            return line

        producer_actor = self._edge_actor_display(edge.producer_actor)
        line = f"{dim}read{reset} {green}{ref_text}{reset}"

        if show_input_positions and input_display:
            line += f" {dim}via{reset} {white}{input_display}{reset}"

        line += f" {dim}from{reset} {cyan}{producer_actor}{reset}"
        return line





    def _path_terminal_line(
       self,
       edge,
       text,
        show_input_positions,
        dim,
        white,
       reset,
       green="",
       cyan="",
       red="",
    ):
        input_display = None

        if isinstance(edge, EmittedStackLink):
            emitter = self._edge_actor_display(edge.producer_actor)
            emitted = self._edge_actor_display(edge.consumer_actor)
            if self.direction == "upstream":
                return (
                    f"{cyan}{emitter}{reset} {dim}emitted{reset} "
                    f"{cyan}{emitted}{reset}"
                )
            return (
                f"{dim}emitted by{reset} {cyan}{emitter}{reset}"
            )

        if show_input_positions and isinstance(edge, StackLink):
            input_display = self._edge_input_display(edge)

        if isinstance(edge, AffectedStackLink):
            effect = str(edge.cause_effect).upper()
            reference = self._display_reference(edge.produced_ref)
            ref_text = f"{reference}[v{edge.produced_ver}]"
            effect_ref_colour = (
                red
                if edge.cause_effect == Effects.DELETE
                or (edge.produced_ref, edge.produced_ver)
                in self.index.deleted_ref_versions
                else green
            )

            if self.direction == "upstream":
                consumer = self._edge_actor_display(edge.consumer_actor)
                return (
                    f"{cyan}{effect}{reset} {dim}effect at{reset} "
                    f"{effect_ref_colour}{ref_text}{reset} {dim}affected{reset} "
                    f"{cyan}{consumer}{reset}"
                )

            producer = self._edge_actor_display(edge.producer_actor)
            return (
                f"{dim}affected by{reset} {cyan}{effect}{reset} "
                f"{dim}effect at{reset} {effect_ref_colour}{ref_text}{reset} "
                f"{dim}from{reset} {cyan}{producer}{reset}"
            )

        link = ReadWriteLink.coerce(text)
        action = link.action
        reference = self._display_reference(link.ref)
        ref_text = f"{reference}[v{link.ver}]"
        relation = link.relation
        actor_text = link._actor_display(link.actor)

        ref_colour = green

        if action in ["wrote", "written as"] and red:
            try:
                ref, version = ref_text.rsplit("[v", 1)
                version = int(version.rstrip("]"))
            except (ValueError, TypeError):
                pass
            else:
                consumers = self.index.ref_version_to_consumer_exec_ids.get(
                    (ref, version),
                    [],
                )
                if not consumers:
                    ref_colour = red

        if action is None:
            rendered = f"{white}{text}{reset}"
        else:
            rendered = f"{dim}{action}{reset} {ref_colour}{ref_text}{reset}"

        if relation == "from":
            if input_display:
                rendered += f" {dim}via{reset} {white}{input_display}{reset}"

            rendered += f" {dim}from{reset} {cyan}{actor_text}{reset}"
            return rendered

        if relation == "used by":
            rendered += f" {dim}used by{reset} {cyan}{actor_text}{reset}"

            if input_display:
                rendered += f" {dim}via{reset} {white}{input_display}{reset}"

            return rendered

        if input_display:
            rendered += f" {dim}via{reset} {white}{input_display}{reset}"

        return rendered




    def _lines_from_paths(self, show_input_positions=False, colours=None):
        colours = colours or {}
        reset = colours.get("reset", "")
        dim = colours.get("dim", "")
        cyan = colours.get("cyan", "")
        green = colours.get("green", "")
        white = colours.get("white", "")
        red = colours.get("red", "")

        lines = []

        for source_index, (source_exec_id, paths) in enumerate(self.paths.items()):
            if source_index:
                lines.append("")

            source_display = self._source_display(source_exec_id)
            path_word = "path" if len(paths) == 1 else "paths"
            route_count = len({
                self._execution_route(
                    source_exec_id,
                    path,
                    self.direction,
                )
                for path in paths
            })
            route_suffix = ""
            if route_count != len(paths):
                route_suffix = (
                    f" | {route_count} execution "
                    f"{'route' if route_count == 1 else 'routes'}"
                )

            lines.append(
                f"{cyan}{source_display}{reset} "
                f"{dim}| {len(paths)} {path_word}{route_suffix}{reset}"
            )

            for path_index, path in enumerate(paths):
                is_last_path = path_index == len(paths) - 1
                path_connector = (
                    SHOW_TREE_LAST
                    if is_last_path
                    else SHOW_TREE_BRANCH
                )
                path_prefix = (
                    "    "
                    if is_last_path
                    else SHOW_TREE_PIPE
                )

                lines.append(f"{dim}{path_connector} path {path_index}{reset}")

                pending_edge = None
                last_connected_exec = None

                for hop in path:
                    if isinstance(hop, (StackLink, AffectedStackLink)):
                        pending_edge = hop
                        continue

                    if not isinstance(hop, ReadWriteLink):
                        continue

                    terminal_text = self._path_terminal_line(
                        pending_edge,
                        hop,
                        show_input_positions,
                        dim,
                        white,
                        reset,
                        green=green,
                        cyan=cyan,
                        red=red,
                    )

                    if pending_edge is None:
                        if (
                            last_connected_exec
                            and hop.type == "write"
                            and hop.action in ("wrote", "written as")
                        ):
                            lines.append(
                                f"{dim}{path_prefix}    {SHOW_TREE_LAST}{reset} "
                                f"{terminal_text}{reset}"
                            )
                            continue

                        lines.append(
                            f"{dim}{path_prefix}{SHOW_TREE_LAST}{reset} "
                            f"{terminal_text}{reset}"
                        )
                        continue

                    connected_exec_id = (
                        pending_edge.producer_exec_id
                        if self.direction == "upstream"
                        else pending_edge.consumer_exec_id
                    )
                    connected_exec = self.inspect(
                        connected_exec_id,
                        "exec_record",
                    ).object

                    lines.append(
                        f"{dim}{path_prefix}{SHOW_TREE_LAST}{reset} "
                        f"{cyan}{connected_exec}{reset} "
                        f"{dim}|{reset} "
                        f"{terminal_text}{reset}"
                    )

                    last_connected_exec = connected_exec
                    pending_edge = None

        return lines







    def show(
        self,
        full_ids=True,
        explore_tool_tips=True,
        show_input_positions=False,
        color=True,
        simple=False,
        colour=None,
    ):
        color, simple = _show_options(color, simple, colour)
        if simple:
            explore_tool_tips = False
        RESET = "\033[0m"
        BOLD = "\033[1m"
        DIM = "\033[2m"

        CYAN = "\033[36m"
        GREEN = "\033[32m"
        YELLOW = "\033[33m"
        RED = "\033[31m"
        WHITE = "\033[37m"

        SECTION_WIDTH = 70

        def section(title):
            return [f"{DIM}{title}:{RESET}"]

        def plural(count, singular, plural_word=None):
            return singular if count == 1 else (plural_word or f"{singular}s")

        source = self.source
        target = self.targeted
        arrow = "->" if self.direction == "downstream" else "<-"
        source_repr = self._path_query_display(source)
        target_repr = self._path_query_display(target)

        path_count = self.count() if self.exists() else 0
        route_count = self.execution_route_count() if self.exists() else 0
        summary = (
            f"{path_count} {plural(path_count, 'path')} found"
            if path_count
            else f"{YELLOW}no paths found{RESET}{DIM}"
        )
        if path_count and route_count != path_count:
            summary += (
                f" | {route_count} execution "
                f"{plural(route_count, 'route')}"
            )

        lines = [
            "",
            f"{CYAN}{'=' * SECTION_WIDTH}{RESET}",
            f"{CYAN}{BOLD}PATH {source_repr} {arrow} {target_repr}{RESET} "
            f"{DIM}| {summary}{RESET}",
            f"{DIM}{show_identity(display_identity(source.id, full_ids=full_ids), source.grouped)} "
            f"{arrow} "
            f"{show_identity(display_identity(target.id, full_ids=full_ids), target.grouped)}{RESET}",
            f"{CYAN}{'=' * SECTION_WIDTH}{RESET}",
        ]

        if not self.exists():
            lines.extend(section("PATHS"))
            lines.append(f"{YELLOW}{SHOW_TREE_LAST} No paths!{RESET}")
        else:
            lines.extend(section("PATHS"))
            lines.extend(
                self._lines_from_paths(
                    show_input_positions=show_input_positions,
                    colours={
                        "reset": RESET,
                        "dim": DIM,
                        "cyan": CYAN,
                        "green": GREEN,
                        "yellow": YELLOW,
                        "red": RED,
                        "white": WHITE,
                    },
                )
            )

        if explore_tool_tips:
            lines.extend(section("EXPLORE"))
            lines.append(
                f"{DIM}path(source, target, direction='{self.direction}'){RESET} "
                f"{DIM}# current path query{RESET}"
            )

        return _show_render(lines, color)



    def to_dict(self):
        return {
            "source": self.source.id,
            "target": self.targeted.id,
            "direction": self.direction,
            "exists": self.exists(),
            "path_count": self.count(),
            "execution_route_count": self.execution_route_count(),
            "path_by_root": {
                self.inspect(source, "exec_record").id: [
                    [
                        self._edge_to_dict(edge)
                        for edge in path
                    ]
                    for path in paths
                ]
                for source, paths in self.paths.items()
            },
        }

    
    def __len__(self):
        return self.count()

    def __bool__(self):
        return self.exists()

    def __name__(self):
        return "Pathed"

    def __repr__(self):
        path_count = self.count()
        path_word = "path" if path_count == 1 else "paths"
        return f"Pathed[{self.source} -> {self.targeted} | {self.direction}, {path_count} {path_word}]"


class Nearby:
    def __init__(self, subject, actors=None, refs=None):
        self.subject = subject
        self.actors= actors or []
        self.refs=refs or []


class Searched:
    def __init__(self, inspect, subject, execs, actors, refs, effects, values, nearby: Nearby):
        self.inspect = inspect
        self.subject = subject
        self.exec = execs
        self.actors = actors
        self.refs = refs
        self.effects = effects
        self.values = values
        self.nearby = nearby

    def first(self, _type: str = None):
        found, close = (None, None)

        if _type is None:
            exact = [
                ("actor", self.actors),
                ("reference", self.refs),
                ("exec", self.exec),
                ("value", self.values),
                ("effect", self.effects),
            ]
            nearby = [
                ("actor", self.nearby.actors),
                ("reference", self.nearby.refs),
            ]

            for match_type, matches in exact:
                if not matches:
                    continue

                found = matches[0]
                if found.grouped:
                    return found.object[0]
                return found

            for match_type, matches in nearby:
                if not matches:
                    continue

                found = matches[0]
                if found.grouped:
                    return found.object[0]
                return found

            return None

        match _type:
            case "ref" | "reference":
                found = self.refs[0] if self.refs else None
                close = self.nearby.refs[0] if self.nearby.refs else None
            case "value" | "hash":
                found = self.values[0] if self.values else None
            case "actor":
                found = self.actors[0] if self.actors else None
                close = self.nearby.actors[0] if self.nearby.actors else None
            case "exec" | "exec_record" | "seq_id":
                found = self.exec[0] if self.exec else None
            case "effect":
                found = self.effects[0] if self.effects else None

        if found:
            if found.grouped:
                return found.object[0]
            return found

        if close:
            if close.grouped:
                return close.object[0]
            return close

        return None

    def any(self):
        return any([self.exec, self.actors, self.refs, self.effects, self.values, self.nearby.refs, self.nearby.actors])

    def exists(self, _type):
        found, close = (None, None)

        match _type:
            case "ref" | "reference":
                found = self.refs[0] if self.refs else None
                close = self.nearby.refs[0] if self.nearby.refs else None
            case "value" | "hash":
                found = self.values[0] if self.values else None
            case "actor":
                found = self.actors[0] if self.actors else None
                close = self.nearby.actors[0] if self.nearby.actors else None
            case "exec" | "exec_record" | "seq_id":
                found = self.exec[0] if self.exec else None
            case "effect":
                found = self.effects[0] if self.effects else None

        return (bool(found), bool(close))

    def get_type(self, type: str):
        match type:
            case "actor" | "actors":
                return {"exact": self.actors, "nearby": self.nearby.actors}
            case "ref" | "refs" | "reference":
                return {"exact": self.refs, "nearby": self.nearby.refs}
            case "exec" | "execs" | "exec_record":
                return {"exact": self.exec, "nearby": []}
            case "value" | "values" | "hash":
                return {"exact": self.values, "nearby": []}
            case "effect" | "effects":
                return {"exact": self.effects, "nearby": []}

    def get(self, type, scope: str = "exact"):
        if scope == "exact":
            return self.get_exact(type)

        return self.get_nearby(type)

    def get_exact(self, type):
        return list(self.get_type(type)["exact"])

    def get_nearby(self, type):
        return list(self.get_type(type)["nearby"])

    def to_dict(self):
        def serialise_matches(matches):
            return [
                _query_json_value(match.to_dict())
                for match in matches
            ]

        return {
            "actors": {
                "exact": serialise_matches(self.actors),
                "nearby": serialise_matches(self.nearby.actors),
            },
            "refs": {
                "exact": serialise_matches(self.refs),
                "nearby": serialise_matches(self.nearby.refs),
            },
            "values": {
                "exact": serialise_matches(self.values),
            },
            "execs": {
                "exact": serialise_matches(self.exec),
            },
            "effects": {
                "exact": serialise_matches(self.effects),
            },
        }

    def diff(self, other):
        return _build_compared(self, other)

    def show(
        self,
        type="all",
        exact=True,
        nearby=True,
        color=True,
        simple=False,
        colour=None,
    ):
        color, simple = _show_options(color, simple, colour)
        types = self._show_types(type)

        if not isinstance(exact, bool) or not isinstance(nearby, bool):
            raise TypeError("exact and nearby must be booleans")

        exact_matches = [
            (type_name, match)
            for type_name in types
            for match in self.get_exact(type_name)
        ] if exact else []
        nearby_matches = [
            (type_name, match)
            for type_name in types
            for match in self.get_nearby(type_name)
        ] if nearby else []
        total = len(exact_matches) + len(nearby_matches)
        match_word = "match" if total == 1 else "matches"
        lines = show_header(
            "FIND",
            f"{total} {match_word} found",
            f"Subject: {self.subject}",
        )

        if exact:
            lines.extend(show_section("EXACT"))
            lines.extend(
                self._show_match_lines(exact_matches)
                or [f"{SHOW_DIM}none{SHOW_RESET}"]
            )

        if nearby:
            lines.extend(show_section("NEARBY"))
            lines.extend(
                self._show_match_lines(nearby_matches)
                or [f"{SHOW_DIM}none{SHOW_RESET}"]
            )

        return _show_render(lines, color)

    @staticmethod
    def _show_types(type):
        aliases = {
            "actor": "actor",
            "actors": "actor",
            "ref": "reference",
            "refs": "reference",
            "reference": "reference",
            "exec": "exec",
            "execs": "exec",
            "exec_record": "exec",
            "value": "value",
            "values": "value",
            "hash": "value",
            "effect": "effect",
            "effects": "effect",
        }

        if type is None or type == "all":
            return ["actor", "reference", "exec", "value", "effect"]

        if type not in aliases:
            raise ValueError(f"Unknown search result type: {type}")

        return [aliases[type]]

    @staticmethod
    def _show_match_lines(matches):
        lines = []

        for type_name, match in matches:
            identifier = match.id
            version = getattr(match, "version", None)

            if type_name == "reference" and version is not None:
                identifier = f"{identifier}[v{version}]"

            lines.append(
                f"  {SHOW_CYAN}{type_name.upper()}{SHOW_RESET} "
                f"{SHOW_WHITE}{identifier}{SHOW_RESET}"
            )

        return lines


class ByBase:
    def _exec_record(self, exec_id) -> Object:
        return self.inspect(exec_id, "exec_record").object

    def _actor_for_exec(self, exec_id: Object):
        return self._exec_record(exec_id).object.producer_id

    def _actor_name_for_exec(self, exec_id):
        actor = self._actor_for_exec(exec_id)

        if hasattr(actor, "source_actor"):
            return actor.source_actor

        return str(actor)

    @property
    def actors(self):
        return [
            self._actor_for_exec(exec_id)
            for exec_id in self.execs
        ]

    def to_dict(self):
        return {
            "id": _query_json_value(self.source.id),
            "type": self.source.type,
            "execs": list(self.execs),
            "actors": [actor.to_dict() for actor in self.actors],
            "refs": [list(ref) for ref in self.refs],
            "by_exec": {
                exec_id: [list(ref) for ref in refs]
                for exec_id, refs in self.by_exec.items()
            },
            "paths": {
                exec_id: [list(ref) for ref in refs]
                for exec_id, refs in self.by_exec.items()
            },
        }

    def diff(self, other):
        return _build_compared(self, other)

    def show(self, color=True, simple=False, colour=None):
        color, simple = _show_options(color, simple, colour)
        if self.source.type in ["reference", "value"]:
            switch_order = True
        else:
            switch_order = False

        if isinstance(self, ProducedBy):
            action = "Produced by"
            ref_action = "produced"
        else:
            action = "Consumed by"
            ref_action = "consumed"

        source_display = show_query_display(
            self.source.type,
            self.source.id,
            None if self.source.grouped else str(self.source.object),
            self.source.grouped,
            getattr(self.source, "version", None),
        )
        title = (
            f"{action.upper()} {source_display}"
            if not switch_order
            else f"{source_display} {action.upper()}"
        )
        summary = f"{len(self.execs)} {'execution' if len(self.execs) == 1 else 'executions'} found"
        lines = show_header(
            title,
            summary,
            show_identity(self.source.id, self.source.grouped),
        )
        lines.extend(show_section("RELATIONS"))

        for exec_id, refs in self.by_exec.items():
            exec_record = self._exec_record(exec_id)
            actor_name = self._actor_name_for_exec(exec_id)

            lines.append(
                f"  {SHOW_DIM}via:{SHOW_RESET} "
                f"{SHOW_CYAN}{exec_record}{SHOW_RESET} "
                f"({SHOW_CYAN}{actor_name}{SHOW_RESET})"
            )

            for ref, ver in refs:
                lines.append(
                    f"    {SHOW_DIM}{ref_action}:{SHOW_RESET} "
                    f"{SHOW_GREEN}{ref}[v{ver}]{SHOW_RESET}"
                )

        if not self.by_exec:
            lines.append(f"{SHOW_YELLOW}└── No relations found{SHOW_RESET}")

        return _show_render(lines, color)


class ConsumedBy(ByBase):
    def __init__(self, inspect, source, execs, refs, consumed_by_exec):
        self.inspect = inspect
        self.source = source
        self.execs = execs
        self.refs = refs
        self.by_exec = consumed_by_exec

        super().__init__()

    def to_dict(self):
        return super().to_dict()

    def show(self, color=True, simple=False, colour=None):
        return super().show(
            color=color,
            simple=simple,
            colour=colour,
        )


class ProducedBy(ByBase):
    def __init__(self, inspect, source, execs, refs, produced_by_exec):
        self.inspect = inspect
        self.source = source
        self.execs = execs
        self.refs = refs
        self.by_exec = produced_by_exec

        super().__init__()

    def to_dict(self):
        return super().to_dict()

    def show(self, color=True, simple=False, colour=None):
        return super().show(
            color=color,
            simple=simple,
            colour=colour,
        )


class Investigate:
    def __init__(self, trace: Trace, inspect, inspected: Inspected):
        self.trace = trace
        self.index = trace.index
        self.inspect = inspect
        self.inspected = inspected

    def _versioned_ref_str(self, ref, ver):
        return f"{ref}[v{ver}]"

    def _actor_entity(self, actor):
        if actor is None:
            return None

        name = getattr(actor, "source_actor", str(actor))

        return {
            "kind": "actor",
            "id": str(getattr(actor, "id", actor)),
            "display": f"ACTOR[{name}]",
            "name": name,
        }

    def _exec_entity(self, exec_id):
        if exec_id is None:
            return None

        inspected = self.inspect(exec_id, "exec_record")
        exec_obj = inspected.object
        producer = getattr(exec_obj, "producer_id", None)

        return {
            "kind": "exec_record",
            "id": exec_id,
            "display": repr(exec_obj),
            "actor": self._actor_entity(producer),
            "ocean": list(inspected.source_oceans or []),
            "source": list(inspected.source_runners or []),
            "pipeline": list(inspected.source_pipelines or []),
        }

    def _object_display(self):
        if self.inspected.type == "value":
            return self.index.value_repr_by_hash.get(
                self.inspected.id,
                repr(self.inspected.object),
            )
        
        if self.inspected.grouped:
            return f"GROUPED[{self.inspected.id}]"

        return repr(self.inspected.object)

    def _object_entity(self):
        inspected_dict = self.inspected.to_dict()

        return {
            **inspected_dict,
            "kind": self.inspected.type,
            "id": inspected_dict["id"],
            "display": self._object_display(),
            "version": self.inspected.version,
            "grouped": self.inspected.grouped,
        }

    def _input_uses(self, exec_id, ref, ver):
        if exec_id is None:
            return []

        uses_by_name = self.index.input_and_pos_by_exec_input_name_and_ver_ref.get(exec_id, {})
        input_uses = []

        for input_name, uses_by_ref_version in uses_by_name.items():
            for input_index in uses_by_ref_version.get((ref, ver), []):
                input_uses.append({
                    "name": input_name,
                    "index": input_index,
                    "display": (
                        f"{input_name}[{input_index}]"
                        if input_index is not None
                        else str(input_name)
                    ),
                })

        return input_uses

    def _consumer_entity(self, exec_id, ref, ver):
        consumer = self._exec_entity(exec_id)

        if consumer is None:
            return None

        return {
            **consumer,
            "input_uses": self._input_uses(exec_id, ref, ver),
        }

    def _versioned_ref_entry(self, ref, ver):
        versioned_ref = (ref, ver)
        producer_exec_id = self.index.ref_version_to_producer_exec_id.get(versioned_ref)
        consumer_exec_ids = list(
            self.index.ref_version_to_consumer_exec_ids.get(versioned_ref, [])
        )

        return {
            "kind": "reference_version",
            "id": self._versioned_ref_str(ref, ver),
            "ref": ref,
            "version": ver,
            "versioned_ref": {
                "ref": ref,
                "version": ver,
            },
            "value_hash": self.index.value_hash_by_ref_version.get(versioned_ref),
            "producer": self._exec_entity(producer_exec_id),
            "consumers": [
                self._consumer_entity(exec_id, ref, ver)
                for exec_id in consumer_exec_ids
            ],
            "used": bool(consumer_exec_ids),
            "unused": not bool(consumer_exec_ids),
            "available": versioned_ref not in self.index.deleted_ref_versions,
            "healthy": (
                bool(consumer_exec_ids)
                and versioned_ref not in self.index.deleted_ref_versions
            ),
        }

    def _read_entry(self, record, exec_id=None):
        ref = getattr(record, "unscoped_reference", None)
        ver = getattr(record, "resolved_version", None)
        associated_input = getattr(record, "associated_input", None)
        if isinstance(associated_input, (tuple, list)):
            input_name = associated_input[0] if associated_input else None
        elif isinstance(associated_input, str):
            input_name = associated_input
        elif isinstance(associated_input, dict):
            input_name = associated_input.get("name")
        else:
            input_name = getattr(associated_input, "name", None)
        input_index = getattr(record, "input_index", None)

        input_use = None
        if input_name is not None:
            input_use = {
                "name": input_name,
                "index": input_index,
                "display": (
                    f"{input_name}[{input_index}]"
                    if input_index is not None
                    else str(input_name)
                ),
            }

        associated_input_entry = (
            {
                "name": input_name,
                "index": input_index,
            }
            if input_name is not None
            else None
        )

        if ref is None:
            return {
                "kind": "read",
                "record_type": getattr(record, "type", None),
                "display": repr(record),
                "associated_input": associated_input_entry,
                "input_use": input_use,
                "input_uses": [],
            }

        versioned_ref = self._versioned_ref_entry(ref, ver)
        indexed_input_uses = self._input_uses(exec_id, ref, ver)

        return {
            **versioned_ref,
            "kind": "read",
            "record_type": getattr(record, "type", None),
            "associated_input": associated_input_entry,
            "input_use": input_use,
            "input_uses": indexed_input_uses,
        }

    def _write_entry(self, record):
        ref = getattr(record, "stored_target", None)
        ver = getattr(record, "stored_version", None)

        if ref is None:
            return {
                "kind": "write",
                "display": repr(record),
            }

        versioned_ref = self._versioned_ref_entry(ref, ver)

        return {
            **versioned_ref,
            "kind": "write",
        }

    def _effect_entry(self, record):
        effect = record.effect
        target, version = record.targets
        producer_exec_ids = list(
            self.index.effect_versioned_target_to_producer_exec_ids.get(
                (effect, target, version),
                [],
            )
        )
        affected = []

        for actor, affected_record in record.affected.items():
            input_positions = []
            for item in affected_record.affected_inputs or []:
                if isinstance(item, tuple) and item:
                    input_positions.append(str(item[0]))
                else:
                    input_positions.append(str(item))

            affected.append({
                "actor": self._actor_entity(actor),
                "execution": self._exec_entity(
                    affected_record.affected_exec_id
                ),
                "inputs": input_positions,
            })

        preserved = not isinstance(
            record.preserved_output,
            __TIDE_TEMPLATE_VALUE__,
        )
        if effect == Effects.DELETE:
            available = False
            healthy = False
        elif effect == Effects.READ:
            read_states = [
                self.index._runtime_read_found(
                    self.index.exec_by_exec_id[exec_id],
                    target,
                )
                for exec_id in producer_exec_ids
            ]
            available = any(found is True for found in read_states)
            healthy = available
        else:
            available = (
                (target, version) not in self.index.deleted_ref_versions
            )
            healthy = bool(
                self.index.ref_version_to_consumer_exec_ids.get(
                    (target, version),
                    [],
                )
            ) and available

        return {
            "kind": "effect",
            "effect": effect,
            "target": target,
            "version": version,
            "id": (effect, (target, version)),
            "display": f"{effect} -> {target}[v{version}]",
            "available": available,
            "healthy": healthy,
            "output": record.output,
            "preserved": preserved,
            "preserved_output": (
                record.preserved_output if preserved else None
            ),
            "producers": [
                self._exec_entity(exec_id)
                for exec_id in producer_exec_ids
            ],
            "affected": affected,
        }

    def _relation_entries(self, relations):
        entries = []

        for actor, exec_ids in (relations or {}).items():
            entries.append({
                "actor": self._actor_entity(actor),
                "executions": [
                    self._exec_entity(exec_id)
                    for exec_id in exec_ids
                ],
            })

        return entries

    def _exec_history_entry(self, exec_id):
        inspected_exec = self.inspect(exec_id, "exec_record")

        return {
            "execution": self._exec_entity(exec_id),
            "reads": [
                self._read_entry(record, exec_id=exec_id)
                for record in inspected_exec.reads
            ],
            "writes": [
                self._write_entry(record)
                for record in inspected_exec.writes
            ],
        }

    def _appearances(self):
        if not self.inspected.grouped:
            return []

        appearances = []

        for grouped_obj in self.inspected.object:
            inspected = self.inspect(
                grouped_obj.object,
                self.inspected.type,
                self.inspected.version,
            )
            inspected_data = inspected.to_dict()

            appearance = {
                "kind": inspected.type,
                "id": inspected_data["id"],
                "display": (
                    str(grouped_obj.object)
                    if inspected.type == "reference"
                    else repr(grouped_obj.object)
                ),
                "version": inspected.version,
                "executions": [],
                "versions": [],
                "used": None,
                "unused": None,
            }

            if inspected.type == "actor":
                appearance["executions"] = [
                    self._exec_entity(exec_id)
                    for exec_id in inspected.history
                ]

            elif inspected.type == "reference":
                appearance["versions"] = [
                    self._versioned_ref_entry(inspected.id, ver)
                    for ver in inspected.history
                ]
                appearance["used"] = any(
                    version["used"]
                    for version in appearance["versions"]
                )
                appearance["unused"] = (
                    bool(appearance["versions"])
                    and not appearance["used"]
                )

            appearances.append(appearance)

        return appearances

    def _navigation_entry(self, action, target, reason, direction=None):
        entry = {
            "action": action,
            "target": target,
            "reason": reason,
        }

        if direction is not None:
            entry["direction"] = direction

        return entry

    def _append_navigation(self, navigation, action):
        key = (
            action["action"],
            action["target"]["kind"],
            str(action["target"]["id"]),
            action.get("direction"),
            action["reason"],
        )

        existing = {
            (
                item["action"],
                item["target"]["kind"],
                str(item["target"]["id"]),
                item.get("direction"),
                item["reason"],
            )
            for item in navigation
        }

        if key not in existing:
            navigation.append(action)

    def to_dict(self):
        if self.inspected is None:
            return {}

        object_entity = self._object_entity()
        kind = object_entity["kind"]

        read_exec_id = (
            object_entity["id"]
            if kind == "exec_record"
            else None
        )
        reads = [
            self._read_entry(record, exec_id=read_exec_id)
            for record in (self.inspected.reads or [])
        ]
        writes = [
            self._write_entry(record)
            for record in (self.inspected.writes or [])
        ]

        data = {
            "type": "investigate",
            "kind": kind,
            "object": object_entity,
            "summary": {},
            "appearances": self._appearances(),
            "history": [],
            "reads": reads,
            "writes": writes,
            "upstream": self._relation_entries(self.inspected.upstream),
            "downstream": self._relation_entries(self.inspected.downstream),
            "navigation": [],
        }

        if kind in ["reference", "value"]:
            history = []

            for item in self.inspected.history:
                if kind == "reference" and not self.inspected.grouped:
                    ref, ver = object_entity["id"], item
                else:
                    ref, ver = item

                history.append(self._versioned_ref_entry(ref, ver))

            used_count = sum(1 for entry in history if entry["used"])
            unused_count = len(history) - used_count

            data["history"] = history
            data["summary"] = {
                "version_count": len(history),
                "used_version_count": used_count,
                "unused_version_count": unused_count,
                "all_consumed": bool(history) and unused_count == 0,
                "none_consumed": bool(history) and used_count == 0,
                "read_count": len(reads),
                "write_count": len(writes),
            }

            for entry in history:
                if entry["producer"] is not None:
                    self._append_navigation(
                        data["navigation"],
                        self._navigation_entry(
                            "investigate",
                            entry["producer"],
                            f"producer of {entry['id']}",
                        ),
                    )

                for consumer in entry["consumers"]:
                    self._append_navigation(
                        data["navigation"],
                        self._navigation_entry(
                            "investigate",
                            consumer,
                            f"consumer of {entry['id']}",
                        ),
                    )

        elif kind in ["actor", "exec_record"]:
            exec_ids = (
                list(self.inspected.history)
                if kind == "actor"
                else [object_entity["id"]]
            )

            data["history"] = [
                self._exec_history_entry(exec_id)
                for exec_id in exec_ids
            ]
            data["summary"] = {
                "execution_count": len(data["history"]),
                "read_count": len(reads),
                "write_count": len(writes),
                "used_write_count": sum(
                    1
                    for write in writes
                    if write.get("used")
                ),
                "unused_write_count": sum(
                    1
                    for write in writes
                    if write.get("unused")
                ),
            }

            for relation in data["upstream"]:
                for execution in relation["executions"]:
                    self._append_navigation(
                        data["navigation"],
                        self._navigation_entry(
                            "investigate",
                            execution,
                            "upstream execution",
                        ),
                    )

        elif kind == "effect":
            data["history"] = [
                self._effect_entry(record)
                for record in self.inspected.object.effects
            ]
            producer_ids = {
                producer["id"]
                for entry in data["history"]
                for producer in entry["producers"]
                if producer is not None
            }
            affected_ids = {
                affected["execution"]["id"]
                for entry in data["history"]
                for affected in entry["affected"]
                if affected["execution"] is not None
            }
            data["summary"] = {
                "effect_count": len(data["history"]),
                "target_count": len({
                    (entry["target"], entry["version"])
                    for entry in data["history"]
                }),
                "producer_count": len(producer_ids),
                "affected_execution_count": len(affected_ids),
            }

            for entry in data["history"]:
                for producer in entry["producers"]:
                    self._append_navigation(
                        data["navigation"],
                        self._navigation_entry(
                            "investigate",
                            producer,
                            f"producer of {entry['display']}",
                        ),
                    )
                for affected in entry["affected"]:
                    if affected["execution"] is None:
                        continue
                    self._append_navigation(
                        data["navigation"],
                        self._navigation_entry(
                            "investigate",
                            affected["execution"],
                            f"affected by {entry['display']}",
                        ),
                    )

            for relation in data["downstream"]:
                for execution in relation["executions"]:
                    self._append_navigation(
                        data["navigation"],
                        self._navigation_entry(
                            "investigate",
                            execution,
                            "downstream execution",
                        ),
                    )

        self._append_navigation(
            data["navigation"],
            self._navigation_entry(
                "path",
                object_entity,
                "trace causal history upstream",
                direction="upstream",
            ),
        )
        self._append_navigation(
            data["navigation"],
            self._navigation_entry(
                "path",
                object_entity,
                "trace causal history downstream",
                direction="downstream",
            ),
        )

        return data

    def diff(self, other, diff_object=True):
        return _build_object_compared(
            self,
            other,
            diff_object=diff_object,
        )

    def show(
        self,
        full_ids=True,
        explore_tool_tips=False,
        show_input_positions=True,
        simple=False,
        color=True,
        colour=None,
    ):
        color, simple = _show_options(color, simple, colour)
        RESET = "\033[0m"
        BOLD = "\033[1m"
        DIM = "\033[2m"

        CYAN = "\033[36m"
        GREEN = "\033[32m"
        YELLOW = "\033[33m"
        RED = "\033[31m"
        WHITE = "\033[37m"

        SECTION_WIDTH = 70

        def section(title):
            return [f"{DIM}{title}:{RESET}"]

        def plural(count, singular, plural_word=None):
            return singular if count == 1 else (plural_word or f"{singular}s")

        def format_input_uses(input_uses):
            grouped = defaultdict(list)

            for input_use in input_uses:
                grouped[input_use["name"]].append(input_use["index"])

            displays = []

            for name, indexes in grouped.items():
                indexes = [
                    index
                    for index in indexes
                    if index is not None
                ]

                if indexes:
                    displays.append(
                        f"{name}[{', '.join(str(index) for index in indexes)}]"
                    )
                else:
                    displays.append(name)

            return ", ".join(displays)

        def simple_appearance_display(appearance):
            kind = appearance["kind"]
            identifier = appearance["id"]

            if kind == "actor":
                if isinstance(identifier, dict):
                    identifier = identifier.get("name", identifier["id"])
                else:
                    identifier = getattr(
                        identifier,
                        "source_actor",
                        identifier,
                    )

            return f"{str(kind).upper()}[{identifier}]"

        def version_summary(summary):
            total = summary["version_count"]
            used_count = summary["used_version_count"]
            unused_count = summary["unused_version_count"]
            version_word = plural(total, "version")

            if used_count and unused_count:
                return (
                    f"{total} {version_word} found: "
                    f"{GREEN}{used_count} used{RESET}{DIM}, "
                    f"{RED}{unused_count} unused{RESET}{DIM}."
                )

            if used_count:
                return (
                    f"{total} {version_word} found; "
                    f"{GREEN}all were consumed{RESET}{DIM}."
                )

            return (
                f"{total} {version_word} found; "
                f"{YELLOW}none were consumed{RESET}{DIM}."
            )

        if self.inspected is None:
            output = (
                f"{RED}{BOLD}Nothing to show{RESET} "
                f"- couldn't find reference object"
            )
            print(output)
            return output

        data = self.to_dict()
        obj = data["object"]
        kind = data["kind"]
        query = show_query_display(
            kind,
            obj["id"],
            obj["display"],
            obj["grouped"],
            obj["version"],
        )

        if kind == "value" and not obj["grouped"]:
            query = f"value {query}"

        if kind in ["reference", "value"]:
            summary = version_summary(data["summary"])
        elif kind in ["actor", "exec_record"]:
            count = data["summary"]["execution_count"]
            summary = f"{count} {plural(count, 'execution')} found"
        elif kind == "effect":
            count = data["summary"]["effect_count"]
            affected_count = data["summary"]["affected_execution_count"]
            summary = (
                f"{count} {plural(count, 'effect')} found | "
                f"{affected_count} affected "
                f"{plural(affected_count, 'execution')}"
            )
        else:
            summary = "renderer pending"

        lines = [
            "",
            f"{CYAN}{'=' * SECTION_WIDTH}{RESET}",
            f"{CYAN}{BOLD}INVESTIGATE {query}{RESET} "
            f"{DIM}| {summary}{RESET}",
            f"{DIM}{show_identity(display_identity(obj['id'], full_ids=full_ids), obj['grouped'])}{RESET}",
            f"{CYAN}{'=' * SECTION_WIDTH}{RESET}",
        ]

        if data["appearances"]:
            lines.extend(section("APPEARED AS"))

            for appearance in data["appearances"]:
                if appearance["versions"]:
                    appearance_colour = (
                        GREEN
                        if appearance["used"]
                        else RED
                    )
                else:
                    appearance_colour = DIM

                appearance_display = appearance["display"]
                if simple:
                    appearance_display = simple_appearance_display(appearance)
                    if appearance["kind"] == "actor":
                        appearance_colour = DIM

                lines.append(
                    f"{appearance_colour}"
                    f"{appearance_display}"
                    f"{RESET}"
                )

                for version in appearance["versions"]:
                    colour = GREEN if version["used"] else RED
                    producer = version["producer"]

                    if producer is None:
                        lines.append(
                            f"  {colour}[v{version['version']}]{RESET}"
                        )
                    else:
                        lines.append(
                            f"  {colour}[v{version['version']}]{RESET} "
                            f"{DIM}by{RESET} "
                            f"{CYAN}{producer['display']}{RESET}"
                        )

                for execution in appearance["executions"]:
                    pipelines = execution.get("pipeline", [])

                    if simple:
                        lines.append(
                            f"{CYAN}  {execution['display']}{RESET}"
                        )
                        continue

                    if pipelines:
                        lines.append(
                            f"{CYAN}  {execution['display']}{RESET} "
                            f"{DIM}from pipeline{RESET} "
                            f"{WHITE}{pipelines[0]}{RESET}"
                        )
                    else:
                        lines.append(
                            f"{CYAN}  {execution['display']}{RESET}"
                        )
                lines.append("")
        
        lines.extend(section("HISTORY"))

        if kind in ["reference", "value"]:
            for item_index, item in enumerate(data["history"]):
                if item_index:
                    lines.append("")

                colour = GREEN if item["healthy"] else RED
                lines.append(f"{colour}{item['id']}{RESET}")

                producer = item["producer"]

                if producer is not None:
                    lines.append(
                        f"{DIM}└── produced by{RESET} "
                        f"{CYAN}{producer['display']}{RESET}"
                    )

                for consumer in item["consumers"]:
                    input_uses = consumer.get("input_uses", [])

                    if not show_input_positions or not input_uses:
                        lines.append(
                            f"{DIM}    └── consumed by{RESET} "
                            f"{CYAN}{consumer['display']}{RESET}"
                        )
                        continue

                    input_uses_display = format_input_uses(input_uses)

                    lines.append(
                        f"{DIM}    └── consumed by{RESET} "
                        f"{CYAN}{consumer['display']}{RESET} "
                        f"{DIM}via{RESET} "
                        f"{WHITE}{input_uses_display}{RESET}"
                    )

        elif kind in ["actor", "exec_record"]:
            for item_index, item in enumerate(data["history"]):
                if item_index:
                    lines.append("")

                execution = item["execution"]
                reads = item["reads"]
                writes = item["writes"]

                lines.append(f"{CYAN}{execution['display']}{RESET}")

                if reads:
                    read_branch = "└──" if not writes else "├──"
                    lines.append(f"{DIM}{read_branch} read{RESET}")

                    for idx, read in enumerate(reads):
                        connector = "└──" if idx == len(reads) - 1 else "├──"
                        prefix = "    " if not writes else "│   "
                        producer = read.get("producer")
                        read_id = read.get("id", read.get("display", "unknown read"))

                        line = (
                            f"{DIM}{prefix}{connector}{RESET} "
                            f"{GREEN}{read_id}{RESET}"
                        )

                        input_use = read.get("input_use")
                        if show_input_positions and input_use is not None:
                            line += (
                                f" {DIM}via{RESET} "
                                f"{WHITE}{input_use['display']}{RESET}"
                            )

                        if producer is not None:
                            line += (
                                f" {DIM}from{RESET} "
                                f"{CYAN}{producer['display']}{RESET}"
                            )

                        lines.append(line)

                if writes:
                    lines.append(f"{DIM}└── wrote{RESET}")

                    for idx, write in enumerate(writes):
                        is_last_write = idx == len(writes) - 1
                        connector = "└──" if is_last_write else "├──"
                        colour = GREEN if write.get("healthy", False) else RED
                        write_id = write.get(
                            "id",
                            write.get("display", "unknown write"),
                        )

                        lines.append(
                            f"{DIM}    {connector}{RESET} "
                            f"{colour}{write_id}{RESET}"
                        )

                        child_prefix = (
                            "        "
                            if is_last_write
                            else "    │   "
                        )

                        consumer_lines = []
                        for consumer in write.get("consumers", []):
                            input_uses = consumer.get("input_uses", [])

                            if not show_input_positions or not input_uses:
                                consumer_lines.append((consumer, None))
                                continue

                            consumer_lines.append(
                                (
                                    consumer,
                                    {
                                        "display": format_input_uses(input_uses),
                                    },
                                )
                            )

                        for consumer_idx, (consumer, input_use) in enumerate(consumer_lines):
                            consumer_connector = (
                                "└──"
                                if consumer_idx == len(consumer_lines) - 1
                                else "├──"
                            )
                            line = (
                                f"{DIM}{child_prefix}{consumer_connector} "
                                f"consumed by{RESET} "
                                f"{CYAN}{consumer['display']}{RESET}"
                            )

                            if input_use is not None:
                                line += (
                                    f" {DIM}via{RESET} "
                                    f"{WHITE}{input_use['display']}{RESET}"
                                )

                            lines.append(line)

                if not reads and not writes:
                    lines.append(
                        f"{YELLOW}└── no recorded reads or writes{RESET}"
                    )

        elif kind == "effect":
            if not data["history"]:
                lines.append(f"{YELLOW}no matching effect evidence{RESET}")

            for entry_index, entry in enumerate(data["history"]):
                if entry_index:
                    lines.append("")

                colour = GREEN if entry["healthy"] else RED
                lines.append(
                    f"{CYAN}{str(entry['effect']).upper()}{RESET} "
                    f"{DIM}->{RESET} "
                    f"{colour}{entry['target']}[v{entry['version']}]{RESET}"
                )

                for producer in entry["producers"]:
                    lines.append(
                        f"{DIM}├── produced by{RESET} "
                        f"{CYAN}{producer['display']}{RESET}"
                    )

                for affected_index, affected in enumerate(entry["affected"]):
                    execution = affected["execution"]
                    if execution is None:
                        continue
                    connector = (
                        "└──"
                        if affected_index == len(entry["affected"]) - 1
                        else "├──"
                    )
                    line = (
                        f"{DIM}{connector} affected{RESET} "
                        f"{CYAN}{execution['display']}{RESET}"
                    )
                    if show_input_positions and affected["inputs"]:
                        line += (
                            f" {DIM}via{RESET} "
                            f"{WHITE}{', '.join(affected['inputs'])}{RESET}"
                        )
                    lines.append(line)

        else:
            lines.append(
                f"{YELLOW}No investigate renderer "
                f"for this object type yet.{RESET}"
            )

        if explore_tool_tips:
            lines.extend(section("EXPLORE"))

            for action in data["navigation"]:
                target = action["target"]
                direction = action.get("direction")
                command = (
                    f"{action['action']} "
                    f"{display_identity(target['id'], full_ids=full_ids)}"
                )

                if direction is not None:
                    command += f" {direction}"

                lines.append(
                    f"{DIM}{command}{RESET} "
                    f"{DIM}# {action['reason']}{RESET}"
                )

        return _show_render(lines, color)


class Object:
    def __init__(self, object=None, type=None, id=None):
        # Universal
        self.type: str = type # ref | exec_record | actor | value
        self.id: int = id
        self.object: Any = object
        self.source_ocean: list = None
        self.source_runner:list = None
        self.source_pipeline:list = None
        self.version: int = None

        self.reads = []
        self.writes = []
        self.effects = []
        self.emitted_by = {}
        # For execution
        self.source_id: int = None # f"{self.source_ocean}{self.source_runner}{self.type[0]}{self.id}" # ocean
        
        self.declared: list = None # deps -> inputs: deps | effects -> inputs: effects | targets -> inputs: targets | Declared
        self.runtime: list = None # deps -> inputs: deps | effects -> inputs: effects | targets -> inputs: targets | Runtime
        self.used: list = None # deps -> inputs: deps | effects -> inputs: effects | targets -> inputs: targets | Used
        self.all: list = None # deps -> inputs: deps | effects -> inputs: effects | targets -> inputs: targets | Declared + Runtime
        
        self.output: Any = __TEMPLATE__

    def __repr__(self):
        if self.type == "exec_record":
            _str = f"{self.type.upper()}[{self.object.seq_id}]"
            self.object: ExecRecord
            obj_repr = shorten_identity(
                self.object.producer_id.source_actor
            )
            return _str + " " + obj_repr
        if self.type == "actor":
            return f"ACTOR[{shorten_identity(self.object.source_actor)}]"
        if self.type == "reference":
            version = self.version
            if self.version is not None:
                return f"{self.type.upper()}[{self.id}[v{version}]]"
            return f"{self.type.upper()}[{self.id}]"
        if self.type == "value":
            return f"VALUE[{self.id}]"
        # Unknown object kinds (currently including effects) may use enum or
        # tuple identifiers. Falling back to repr(self) recurses forever for
        # those identifiers, so use their value representation directly.
        return str(self.id)
    @property
    def exists(self):
        if isinstance(self.source_ocean, list):
            return bool(self.source_ocean)    
        return bool(self.source_ocean is not None)


class Inspected: # an overview of the object. probs include history, inputs, outputs, declared effects + deps, runtime effects + dep  
    def __init__(self, object, history: list=None, upstream=None, downstream=None):
        self.type: Object = object.type
        self.id = object.id
        self.object: Object = object # ref | node | exec_record/id
        self.history: list = history if history else [] # previous entries of same object.
        self.upstream = upstream or {}
        self.downstream = downstream or {}
        self.version = object.version
        self.reads = object.reads
        self.writes = object.writes
        self.source_oceans = object.source_ocean
        self.source_runners = object.source_runner
        self.source_pipelines = object.source_pipeline
        self.grouped=False

        if (
            self.type == "exec_record"
            and hasattr(self.object.object, "seq_id")
            and hasattr(self.object.object, "producer_id")
        ):
            self.__object__repr__ = repr(self.object)
        elif (
            self.type == "actor"
            and hasattr(self.object.object, "source_actor")
        ):
            self.__object__repr__ = repr(self.object)
        else:
            self.__object__repr__ = repr(self.object.object)

    def assign_obj_repr(self, str):
        self.__object__repr__ = str

    def __eq__(self, other):
        return all([
            self.type == other.type,
            self.id == other.id,
            self.history == other.history,
            self.version == other.version,
            (len(self.reads) + len(self.writes)) == (len(other.reads) + len(other.writes)),
            self.grouped == other.grouped
        ])
    
    def __repr__(self):
        return f"Inspected({self.type}: {self.__object__repr__}, history[{list(self.history)[:3]}]..)"
        
    def to_dict(self):

        return {
            "type": self.type, # what is this object
            "id": _query_json_value(self.id), # how can I find this object
            "ocean": _query_json_value(self.source_oceans), # what ocean instance was I observed in...? ish
            "source": _query_json_value(self.source_runners), # what source did I come from,
            "pipeline": _query_json_value(self.source_pipelines), # what graph was I a part of
            "history": _query_json_value(self.history),
            "upstream_seeds": _query_json_value(
                [seed for seeds in self.upstream.values() for seed in seeds]
            ),
            "downstream_seeds": _query_json_value(
                [seed for seeds in self.downstream.values() for seed in seeds]
            ),
            "grouped": self.grouped,
            "exists": self.exists
        }

    def diff(self, other, diff_object=True):
        return _build_object_compared(
            self,
            other,
            diff_object=diff_object,
        )

    def show(self, color=True, simple=False, colour=None):
        color, simple = _show_options(color, simple, colour)
        RESET = "\033[0m"
        BOLD = "\033[1m"
        DIM = "\033[2m"
        CYAN = "\033[36m"
        BLUE = "\033[34m"
        WHITE = "\033[37m"

        SECTION_WIDTH = 70

        def section(title):
            return [f"{DIM}{title}:{RESET}"]

        def format_scalar(value):
            if value is None:
                return "none"

            if isinstance(value, bool):
                return str(value).lower()

            return str(value)

        def format_field(name, value, colour=SHOW_WHITE):
            def render(value):
                scalar = format_scalar(value)
                return f"{colour}{scalar}{RESET}"

            label = f"{DIM}{name}:{RESET}"

            if isinstance(value, list):
                if not value:
                    return [f"{label} {render('none')}"]

                if len(value) == 1:
                    return [f"{label} {render(value[0])}"]

                return [
                    label
                ] + [
                    f"  {BLUE}[{index}]{RESET} {render(item)}"
                    for index, item in enumerate(value)
                ]

            return [f"{label} {render(value)}"]

        def member_display(member):
            member_type = getattr(member, "type", None)
            member_id = getattr(member, "id", None)

            if member_type is None or member_id is None:
                return str(member)

            member_id = getattr(member_id, "id", member_id)
            member_type = str(member_type).upper()
            version = getattr(member, "version", None)

            if member_type == "REFERENCE" and version is not None:
                member_id = f"{member_id}[v{version}]"

            return f"{member_type}[{member_id}]"

        data = self.to_dict()
        query = show_query_display(
            data["type"],
            data["id"],
            None if data["grouped"] else str(self.object),
            data["grouped"],
            self.version,
        )

        lines = [
            "",
            f"{CYAN}{'=' * SECTION_WIDTH}{RESET}",
            f"{CYAN}{BOLD}INSPECT {query}{RESET}",
            f"{CYAN}{show_identity(data['id'], data['grouped'])}{RESET}",
            f"{CYAN}{'=' * SECTION_WIDTH}{RESET}",
        ]

        lines.extend(section("OBJECT"))
        lines.extend(format_field("type", data["type"]))
        id_colour = CYAN
        if self.type == "reference":
            id_colour = SHOW_GREEN if self.downstream else SHOW_RED
        lines.extend(format_field("id", data["id"], id_colour))
        lines.extend(format_field("grouped", data["grouped"]))
        lines.extend(format_field("exists", data["exists"]))

        if data["grouped"]:
            lines.extend(format_field(
                "members",
                [member_display(member) for member in self.object],
                CYAN,
            ))

        lines.extend(section("SOURCE"))
        lines.extend(format_field("ocean", data["ocean"], CYAN))
        lines.extend(format_field("source", data["source"], CYAN))
        lines.extend(format_field("pipeline", data["pipeline"], CYAN))

        lines.extend(section("HISTORY"))
        if data["history"]:
            history_colour = CYAN
            if self.type == "reference":
                history_colour = (
                    SHOW_GREEN
                    if self.downstream
                    else SHOW_RED
                )
            lines.extend([
                f"  {BLUE}[{index}]{RESET} "
                f"{history_colour}"
                f"{format_scalar(entry)}{RESET}"
                for index, entry in enumerate(data["history"])
            ])
        else:
            lines.append(
                f"{WHITE}history:{RESET} {DIM}none{RESET}"
            )

        lines.extend(section("SEEDS"))
        lines.extend(format_field(
            "upstream_seeds",
            data["upstream_seeds"],
            CYAN,
        ))
        lines.extend(format_field(
            "downstream_seeds",
            data["downstream_seeds"],
            CYAN,
        ))

        output = "\n".join(lines)

        return _show_render(output.split("\n"), color)
    
    @property
    def exists(self):
        if self.type == "reference":
            return bool(self.history)
        return bool(self.object.exists)


class InspectedGroup(Inspected):
    def __init__(self, objects: list[Object, ], type: str, id: str, history: list=None, upstream=None, downstream=None, source_oceans=None, source_runners=None, source_pipelines=None, version=None, reads=None, writes=None):
        self.type = type
        self.id = id
        self.object = objects
        self.history = history or []
        self.upstream = upstream or {}
        self.downstream = downstream or {}
        
        self.source_oceans = source_oceans or []
        self.source_runners= source_runners or []
        self.source_pipelines= source_pipelines or []
        
        self.reads = reads or []
        self.writes = writes or []

        self.grouped = True
        
        self.version = version

    def __repr__(self):
        return (
            f"InspectedGroup({self.type}: {self.id}, "
            f"objects[{len(self.object)}], "
            f"history[{list(self.history)[:3]}]..)"
        )

    @property
    def exists(self):
        return any(object.exists for object in self.object)


def _group_diff_path(path):
    parts = str(path).split(".")

    for index, part in enumerate(parts):
        if "[" in part:
            group = ".".join(parts[: index + 1])
            field = ".".join(parts[index + 1 :])
            return group, field or None

    if len(parts) > 1:
        return ".".join(parts[:-1]), parts[-1]

    return None, str(path)


def _group_diff_entries(entries):
    grouped = {}

    for path, value in entries.items():
        group, field = _group_diff_path(path)
        grouped.setdefault(group, []).append((field, value))

    return grouped


def _append_structured_value(lines, value, indent, reset, dim, white):
    if isinstance(value, dict):
        if not value:
            lines.append(f"{indent}{dim}{{}}{reset}")
            return

        for key, item in value.items():
            label = f"{indent}{dim}{key}:{reset}"
            if isinstance(item, (dict, list, tuple)):
                lines.append(label)
                _append_structured_value(
                    lines,
                    item,
                    f"{indent}  ",
                    reset,
                    dim,
                    white,
                )
            else:
                lines.append(
                    f"{label} {white}{repr(item)}{reset}"
                )
        return

    if isinstance(value, (list, tuple)):
        if not value:
            lines.append(f"{indent}{dim}[]{reset}")
            return

        for index, item in enumerate(value):
            label = f"{indent}[{index}]:"
            if isinstance(item, (dict, list, tuple)):
                lines.append(f"{dim}{label}{reset}")
                _append_structured_value(
                    lines,
                    item,
                    f"{indent}  ",
                    reset,
                    dim,
                    white,
                )
            else:
                lines.append(
                    f"{dim}{label}{reset} {white}{repr(item)}{reset}"
                )
        return

    lines.append(f"{white}{repr(value)}{reset}")


def _append_changed_entry(lines, label, value, indent, reset, dim, white):
    before = value["before"]
    after = value["after"]
    type_changed = type(before) is not type(after)

    if isinstance(before, (dict, list, tuple)) or isinstance(
        after,
        (dict, list, tuple),
    ):
        lines.append(f"{indent}{dim}{label}:{reset}")
        if type_changed:
            lines.append(
                f"{indent}  {dim}type:{reset} "
                f"{white}{type(before).__name__}{reset} "
                f"{dim}->{reset} "
                f"{white}{type(after).__name__}{reset}"
            )
        lines.append(f"{indent}  {dim}before:{reset}")
        _append_structured_value(
            lines,
            before,
            f"{indent}    ",
            reset,
            dim,
            white,
        )
        lines.append(f"{indent}  {dim}after:{reset}")
        _append_structured_value(
            lines,
            after,
            f"{indent}    ",
            reset,
            dim,
            white,
        )
        return

    lines.append(
        f"{indent}{dim}{label}:{reset} "
        f"{white}{repr(before)}{reset} "
        f"{dim}->{reset} "
        f"{white}{repr(after)}{reset}"
    )


def _append_value_entry(
    lines,
    label,
    value,
    indent,
    label_colour,
    reset,
    dim,
    white,
):
    """Render an added/removed field without splitting scalar values."""
    if isinstance(value, (dict, list, tuple)):
        lines.append(
            f"{indent}{label_colour}{label}{reset}:"
        )
        _append_structured_value(
            lines,
            value,
            f"{indent}  ",
            reset,
            dim,
            white,
        )
        return

    lines.append(
        f"{indent}{label_colour}{label}{reset}: "
        f"{white}{repr(value)}{reset}"
    )


class Compared:
    def __init__(self, object_type, before, after, changed=None, added=None, removed=None):
        self.object_type = object_type
        self.before = before
        self.after = after
        self.changed = changed or {}
        self.added = added or {}
        self.removed = removed or {}

    @property
    def same(self):
        return not any((self.changed, self.added, self.removed))

    @property
    def count(self):
        return sum(
            len(section)
            for section in (self.changed, self.added, self.removed)
        )

    def to_dict(self):
        return {
            "type": "diff",
            "object": self.object_type,
            "same": self.same,
            "changed": copy.deepcopy(self.changed),
            "added": copy.deepcopy(self.added),
            "removed": copy.deepcopy(self.removed),
        }

    def diff(self, other):
        return _build_compared(self, other)

    def show(self, color=True, simple=False, colour=None):
        if colour is not None:
            color = colour
        if not isinstance(color, bool):
            raise TypeError("Compared.show color must be a bool")
        if not isinstance(simple, bool):
            raise TypeError("Compared.show simple must be a bool")

        reset = SHOW_RESET if color else ""
        bold = SHOW_BOLD if color else ""
        dim = SHOW_DIM if color else ""
        cyan = SHOW_CYAN if color else ""
        green = SHOW_GREEN if color else ""
        yellow = SHOW_YELLOW if color else ""
        red = SHOW_RED if color else ""
        white = SHOW_WHITE if color else ""

        status = "unchanged" if self.same else f"{self.count} changes"
        lines = [
            "",
            f"{cyan}{'=' * SHOW_SECTION_WIDTH}{reset}",
            f"{cyan}{bold}DIFF{reset} | {self.object_type} | {status}",
            f"{dim}{'same' if self.same else 'different'}{reset}",
            f"{cyan}{'=' * SHOW_SECTION_WIDTH}{reset}",
        ]

        sections = (
            ("CHANGED", self.changed, yellow),
            ("ADDED", self.added, green),
            ("REMOVED", self.removed, red),
        )

        for title, entries, colour in sections:
            if simple and not entries:
                continue

            lines.append(f"{dim}{title}:{reset}")
            if not entries:
                lines.append(f"  {dim}none{reset}")
                continue

            grouped = _group_diff_entries(entries)
            for group, group_entries in grouped.items():
                if group is None:
                    for field, value in group_entries:
                        if title == "CHANGED":
                            _append_changed_entry(
                                lines,
                                field,
                                value,
                                "  ",
                                reset,
                                dim,
                                white,
                            )
                        else:
                            _append_value_entry(
                                lines,
                                field,
                                value,
                                "  ",
                                colour,
                                reset,
                                dim,
                                white,
                            )
                    continue

                lines.append(f"  {colour}{group}:{reset}")
                for field, value in group_entries:
                    indent = "    "

                    nested_group = None
                    nested_field = field
                    if field and "." in field:
                        nested_group, nested_field = _group_diff_path(field)

                    if nested_group is not None:
                        lines.append(
                            f"{indent}{colour}{nested_group}:{reset}"
                        )
                        nested_indent = f"{indent}  "
                        if title == "CHANGED":
                            _append_changed_entry(
                                lines,
                                nested_field or "value",
                                value,
                                nested_indent,
                                reset,
                                dim,
                                white,
                            )
                        elif nested_field is None:
                            if isinstance(value, (dict, list, tuple)):
                                _append_structured_value(
                                    lines,
                                    value,
                                    nested_indent,
                                    reset,
                                    dim,
                                    white,
                                )
                            else:
                                lines[-1] += (
                                    f" {white}{repr(value)}{reset}"
                                )
                        else:
                            _append_value_entry(
                                lines,
                                nested_field,
                                value,
                                nested_indent,
                                colour,
                                reset,
                                dim,
                                white,
                            )
                        continue

                    if title == "CHANGED":
                        _append_changed_entry(
                            lines,
                            field or "value",
                            value,
                            indent,
                            reset,
                            dim,
                            white,
                        )
                    elif field is None:
                        if isinstance(value, (dict, list, tuple)):
                            _append_structured_value(
                                lines,
                                value,
                                indent,
                                reset,
                                dim,
                                white,
                            )
                        else:
                            lines[-1] += (
                                f" {white}{repr(value)}{reset}"
                            )
                    else:
                        _append_value_entry(
                            lines,
                            field,
                            value,
                            indent,
                            colour,
                            reset,
                            dim,
                            white,
                        )

        return _show_render(lines, color)


class TIDEModelCompared(Compared):
    """A semantic diff of two valid TIDE Model kernels and judgments."""

    _KERNEL_LABELS = {
        "I": "INPUT INCIDENCE",
        "O": "OUTPUT INCIDENCE",
    }

    def __init__(
        self,
        *,
        before,
        after,
        before_report,
        after_report,
        kernel,
        judgments,
        identity_mode="host",
    ):
        added = {
            field: copy.deepcopy(change["added"])
            for field, change in kernel.items()
            if change["added"]
        }
        removed = {
            field: copy.deepcopy(change["removed"])
            for field, change in kernel.items()
            if change["removed"]
        }
        for name, change in judgments.items():
            if change["added"]:
                added[name] = copy.deepcopy(change["added"])
            if change["removed"]:
                removed[name] = copy.deepcopy(change["removed"])

        super().__init__(
            object_type="TIDE Model",
            before=before,
            after=after,
            added=added,
            removed=removed,
        )
        self.before_report = before_report
        self.after_report = after_report
        self.kernel = kernel
        self.judgments = judgments
        self.identity_basis = (
            "canonical I/O incidence structure (values and enrichment excluded)"
            if identity_mode == "structural"
            else "literal Execution and State identities in I/O"
        )

    @staticmethod
    def _section_same(section):
        return not section["added"] and not section["removed"]

    @property
    def same_kernel(self):
        return all(
            self._section_same(section)
            for section in self.kernel.values()
        )

    @property
    def same_judgments(self):
        return all(
            self._section_same(section)
            for section in self.judgments.values()
        )

    @property
    def same(self):
        return self.same_kernel and self.same_judgments

    @property
    def kernel_count(self):
        return sum(
            len(section["added"]) + len(section["removed"])
            for section in self.kernel.values()
        )

    @property
    def judgment_count(self):
        return sum(
            len(section["added"]) + len(section["removed"])
            for section in self.judgments.values()
        )

    @property
    def count(self):
        return self.kernel_count + self.judgment_count

    @staticmethod
    def _serialise_incidence(edge):
        return {
            "exec_id": edge[0],
            "state_id": edge[1],
        }

    @staticmethod
    def _serialise_handoff(handoff):
        return {
            "producer_exec_id": handoff[0],
            "state_id": handoff[1],
            "consumer_exec_id": handoff[2],
        }

    @staticmethod
    def _serialise_reachability(edge):
        return {
            "source_exec_id": edge[0],
            "target_exec_id": edge[1],
        }

    @classmethod
    def _serialise_kernel_section(cls, field, section):
        serialise = (
            cls._serialise_incidence
            if field in {"I", "O"}
            else lambda value: value
        )
        return {
            "before_count": section["before_count"],
            "after_count": section["after_count"],
            "added": [
                serialise(value)
                for value in section["added"]
            ],
            "removed": [
                serialise(value)
                for value in section["removed"]
            ],
        }

    @classmethod
    def _serialise_judgment_section(cls, name, section):
        serialise = (
            cls._serialise_handoff
            if name == "direct_handoffs"
            else cls._serialise_reachability
        )
        return {
            "before_count": section["before_count"],
            "after_count": section["after_count"],
            "added": [
                serialise(value)
                for value in section["added"]
            ],
            "removed": [
                serialise(value)
                for value in section["removed"]
            ],
        }

    def to_dict(self):
        return {
            "type": "tide_model_diff",
            "object": self.object_type,
            "same": self.same,
            "same_kernel": self.same_kernel,
            "same_judgments": self.same_judgments,
            "identity_basis": self.identity_basis,
            "before": {
                "host": self.before.get("host"),
                "counts": copy.deepcopy(
                    self.before_report["counts"]
                ),
            },
            "after": {
                "host": self.after.get("host"),
                "counts": copy.deepcopy(
                    self.after_report["counts"]
                ),
            },
            "kernel": {
                field: self._serialise_kernel_section(
                    field,
                    section,
                )
                for field, section in self.kernel.items()
            },
            "judgments": {
                name: self._serialise_judgment_section(
                    name,
                    section,
                )
                for name, section in self.judgments.items()
            },
            "changed": {},
            "added": copy.deepcopy(self.added),
            "removed": copy.deepcopy(self.removed),
        }

    @staticmethod
    def _render_identity(value):
        return str(value)

    @classmethod
    def _render_kernel_item(cls, field, value):
        if field == "I":
            return (
                f"{cls._render_identity(value[0])} "
                f"<- {cls._render_identity(value[1])}"
            )
        if field == "O":
            return (
                f"{cls._render_identity(value[0])} "
                f"-> {cls._render_identity(value[1])}"
            )
        return cls._render_identity(value)

    @classmethod
    def _render_handoff(cls, value):
        return (
            f"{cls._render_identity(value[0])} "
            f"--{cls._render_identity(value[1])}--> "
            f"{cls._render_identity(value[2])}"
        )

    @classmethod
    def _render_reachability(cls, value):
        return (
            f"{cls._render_identity(value[0])} "
            f"\u2192 {cls._render_identity(value[1])}"
        )

    @staticmethod
    def _append_delta_entries(
        lines,
        section,
        render,
        *,
        indent,
        reset,
        dim,
        green,
        red,
        white,
    ):
        entries = False
        for value in section["added"]:
            entries = True
            lines.append(
                f"{indent}{green}+{reset} "
                f"{white}{render(value)}{reset}"
            )
        for value in section["removed"]:
            entries = True
            lines.append(
                f"{indent}{red}-{reset} "
                f"{white}{render(value)}{reset}"
            )
        if not entries:
            lines.append(f"{indent}{dim}none{reset}")

    def show(self, color=True, simple=False, colour=None):
        color, simple = _show_options(
            color=color,
            simple=simple,
            colour=colour,
        )
        reset = SHOW_RESET if color else ""
        bold = SHOW_BOLD if color else ""
        dim = SHOW_DIM if color else ""
        cyan = SHOW_CYAN if color else ""
        green = SHOW_GREEN if color else ""
        yellow = SHOW_YELLOW if color else ""
        red = SHOW_RED if color else ""
        white = SHOW_WHITE if color else ""

        status = "UNCHANGED" if self.same else "CHANGED"
        status_colour = green if self.same else yellow
        before_counts = self.before_report["counts"]
        after_counts = self.after_report["counts"]
        before_host = self.before.get("host") or "unspecified"
        after_host = self.after.get("host") or "unspecified"

        lines = [
            "",
            f"{cyan}{'=' * SHOW_SECTION_WIDTH}{reset}",
            f"{cyan}{bold}TIDE DIFF{reset} | "
            f"{status_colour}{status}{reset} | "
            f"{before_counts['executions']}E/"
            f"{before_counts['states']}S "
            f"{dim}\u2192{reset} "
            f"{after_counts['executions']}E/"
            f"{after_counts['states']}S",
            f"{dim}{self.identity_basis} | "
            f"host: {before_host} \u2192 {after_host}{reset}",
            f"{cyan}{'=' * SHOW_SECTION_WIDTH}{reset}",
            f"{dim}SUMMARY:{reset}",
            f"  kernel: "
            f"{green if self.same_kernel else yellow}"
            f"{'unchanged' if self.same_kernel else 'changed'}"
            f"{reset} {dim}| {self.kernel_count} "
            f"added/removed facts{reset}",
            f"  judgments: "
            f"{green if self.same_judgments else yellow}"
            f"{'unchanged' if self.same_judgments else 'changed'}"
            f"{reset} {dim}| {self.judgment_count} "
            f"added/removed judgments{reset}",
        ]

        lines.append(f"{dim}DIRECT HANDOFFS:{reset}")
        self._append_delta_entries(
            lines,
            self.judgments["direct_handoffs"],
            self._render_handoff,
            indent="  ",
            reset=reset,
            dim=dim,
            green=green,
            red=red,
            white=white,
        )

        lines.append(f"{dim}REACHABILITY:{reset}")
        self._append_delta_entries(
            lines,
            self.judgments["reachability"],
            self._render_reachability,
            indent="  ",
            reset=reset,
            dim=dim,
            green=green,
            red=red,
            white=white,
        )

        lines.append(f"{dim}KERNEL:{reset}")
        for field, section in self.kernel.items():
            if simple and self._section_same(section):
                continue
            lines.append(
                f"  {cyan}{self._KERNEL_LABELS[field]}{reset} "
                f"{dim}| {section['before_count']} \u2192 "
                f"{section['after_count']}{reset}"
            )
            self._append_delta_entries(
                lines,
                section,
                lambda value, current=field: (
                    self._render_kernel_item(current, value)
                ),
                indent="    ",
                reset=reset,
                dim=dim,
                green=green,
                red=red,
                white=white,
            )

        if simple and self.same_kernel:
            lines.append(f"  {dim}none{reset}")

        return _show_render(lines, color)
