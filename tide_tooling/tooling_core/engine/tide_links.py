"""Typed relationship links used by TIDE path and stream results."""

from __future__ import annotations

import re

from tide_tooling.tooling_core.engine.tide_rendering import (
    _show_fields,
    _show_options,
)


def _diff(subject, other):
    from tide_tooling.tooling_core.engine.tide_query_results import _build_compared

    return _build_compared(subject, other)


class StackLink:
    def __init__(self, input_edge, depth="unknown"):
        self.consumer_input = input_edge["affected_input"]
        self.consumer_input_index = input_edge.get(
            "affected_input_index"
        )
        self.consumer_actor = input_edge["affected_actor"]
        self.consumer_exec_id = input_edge["affected_exec_id"]
        self.producer_actor = input_edge["producer_actor"]
        self.producer_exec_id = input_edge["producer_exec_id"]
        self.produced_ref = input_edge["produced_ref"]
        self.produced_ver = input_edge["produced_ref_version"]
        self.at_depth = depth

    def __repr__(self):
        return (
            f"StackLink({self.producer_actor}->"
            f"{self.consumer_actor}@{self.at_depth})"
        )

    def input_use(self):
        if not self.consumer_input:
            return None

        name = self.consumer_input[0]
        index = self.consumer_input_index
        display = (
            f"{name}[{index}]"
            if index is not None
            else str(name)
        )
        return {"name": name, "index": index, "display": display}

    def input_uses(self):
        use = self.input_use()
        return [use] if use else []

    def input_suffix(self):
        use = self.input_use()
        return f" via {use['display']}" if use else ""

    def to_dict(self):
        return {
            "producer_exec": self.producer_exec_id,
            "consumer_exec": self.consumer_exec_id,
            "ref": self.produced_ref,
            "version": self.produced_ver,
            "affected_input": (
                [self.consumer_input[0]]
                if self.consumer_input
                else []
            ),
            "input_index": self.consumer_input_index,
            "input_use": self.input_use(),
            "input_uses": self.input_uses(),
        }

    def diff(self, other):
        return _diff(self, other)

    def show(self, color=True, simple=False, colour=None):
        return _show_fields(
            "STACK LINK",
            repr(self),
            self.to_dict(),
            color=color,
            simple=simple,
            colour=colour,
        )


class EmittedStackLink(StackLink):
    """A causal hop from an emitter execution to its emitted child."""

    def __init__(
        self,
        producer_exec_id,
        producer_actor,
        consumer_exec_id,
        consumer_actor,
        depth="unknown",
    ):
        self.consumer_input = None
        self.consumer_input_index = None
        self.consumer_actor = consumer_actor
        self.consumer_exec_id = consumer_exec_id
        self.producer_actor = producer_actor
        self.producer_exec_id = producer_exec_id
        self.produced_ref = "TIDE: emitted node"
        self.produced_ver = 0
        self.at_depth = depth

    def __repr__(self):
        return (
            "EmittedStackLink("
            f"{self.producer_actor}->{self.consumer_actor}@{self.at_depth}"
            ")"
        )

    def to_dict(self):
        return {
            "type": "emit",
            "producer_exec": self.producer_exec_id,
            "consumer_exec": self.consumer_exec_id,
            "producer_actor": self.producer_actor,
            "consumer_actor": self.consumer_actor,
        }


class ReadWriteLink:
    """A typed terminal hop in a TIDE path."""

    _TEXT_PATTERN = re.compile(
        r"^(read as|read|written as|wrote)\s+(.+?)\[v([^\]]+)\]"
        r"(?:\s+(from|used by)\s+(.+))?$"
    )

    def __init__(
        self,
        type=None,
        ref=None,
        ver=None,
        producer=None,
        *,
        text=None,
        consumer=None,
        action=None,
        relation=None,
    ):
        if (
            text is None
            and ref is None
            and ver is None
            and producer is None
            and isinstance(type, str)
            and type.startswith((
                "read ",
                "read as ",
                "wrote ",
                "written as ",
            ))
        ):
            text = type
            type = None

        if text is not None:
            parsed = self._parse_text(text)
            type = parsed["type"]
            ref = parsed["ref"]
            ver = parsed["ver"]
            action = parsed["action"]
            relation = parsed["relation"]
            producer = parsed["producer"]
            consumer = parsed["consumer"]

        if type not in {"read", "write"}:
            raise ValueError(
                "ReadWriteLink type must be 'read' or 'write', "
                f"got {type!r}"
            )

        self.type = type
        self.ref = ref
        self.ver = ver
        self.producer = producer
        self.consumer = consumer
        self.action = action or (
            "read"
            if type == "read"
            else "wrote"
        )
        self.relation = relation
        self._input_use = None
        self._input_uses = []

    @classmethod
    def _parse_text(cls, text):
        if not isinstance(text, str):
            raise TypeError(
                "ReadWriteLink text must be a str, got "
                f"{type(text).__name__}"
            )

        match = cls._TEXT_PATTERN.fullmatch(text.strip())
        if match is None:
            raise ValueError(
                f"Could not parse ReadWriteLink text: {text!r}"
            )

        action, ref, version, relation, actor = match.groups()
        try:
            version = int(version)
        except ValueError:
            if version == "None":
                version = None

        link_type = "read" if action.startswith("read") else "write"
        return {
            "type": link_type,
            "ref": ref,
            "ver": version,
            "action": action,
            "relation": relation,
            "producer": actor if relation == "from" else None,
            "consumer": actor if relation == "used by" else None,
        }

    @classmethod
    def from_text(cls, text):
        return cls(text=text)

    @classmethod
    def coerce(cls, value):
        if isinstance(value, cls):
            return value
        if isinstance(value, str):
            return cls.from_text(value)
        raise TypeError(
            "Path hop must be a StackLink or ReadWriteLink, got "
            f"{type(value).__name__}"
        )

    @staticmethod
    def _actor_display(actor):
        if actor is None:
            return None
        return getattr(actor, "source_actor", str(actor))

    @staticmethod
    def _actor_dict(actor):
        if actor is None:
            return None
        if hasattr(actor, "to_dict"):
            return actor.to_dict()
        return actor

    @property
    def version(self):
        return self.ver

    @property
    def actor(self):
        return (
            self.producer
            if self.relation == "from"
            else self.consumer
        )

    @property
    def input_use(self):
        return self._input_use

    @property
    def input_uses(self):
        return list(self._input_uses)

    def attach_input_use(self, input_use):
        self._input_use = input_use
        self._input_uses = [input_use] if input_use else []
        return self

    @property
    def display(self):
        value = f"{self.ref}[v{self.ver}]"
        rendered = f"{self.action} {value}"

        if self.relation == "from" and self.producer is not None:
            rendered += (
                f" from {self._actor_display(self.producer)}"
            )
        elif (
            self.relation == "used by"
            and self.consumer is not None
        ):
            rendered += (
                f" used by {self._actor_display(self.consumer)}"
            )

        return rendered

    def __str__(self):
        return self.display

    def __repr__(self):
        return (
            f"ReadWriteLink(type={self.type!r}, ref={self.ref!r}, "
            f"ver={self.ver!r}, producer={self.producer!r}, "
            f"consumer={self.consumer!r})"
        )

    def __eq__(self, other):
        if isinstance(other, str):
            return self.display == other
        if not isinstance(other, ReadWriteLink):
            return False
        return (
            self.type,
            self.ref,
            self.ver,
            self.producer,
            self.consumer,
            self.action,
            self.relation,
        ) == (
            other.type,
            other.ref,
            other.ver,
            other.producer,
            other.consumer,
            other.action,
            other.relation,
        )

    def __hash__(self):
        return hash((
            self.type,
            self.ref,
            self.ver,
            repr(self.producer),
            repr(self.consumer),
            self.action,
            self.relation,
        ))

    def to_dict(self):
        return {
            "type": self.type,
            "action": self.action,
            "ref": self.ref,
            "version": self.ver,
            "producer": self._actor_dict(self.producer),
            "consumer": self._actor_dict(self.consumer),
            "relation": self.relation,
            "input_use": self.input_use,
            "input_uses": self.input_uses,
            "display": self.display,
        }

    def diff(self, other):
        return _diff(self, other)

    def show(self, color=True, simple=False, colour=None):
        return _show_fields(
            "READ/WRITE LINK",
            self.display,
            self.to_dict(),
            color=color,
            simple=simple,
            colour=colour,
        )


class AffectedStackLink:
    def __init__(
        self,
        producer_exec_id,
        producer_actor,
        cause_effect,
        produced_ref,
        produced_ref_version,
        affected_exec_id,
        affected_actor,
        affected_inputs,
        depth="unknown",
    ):
        self.producer_exec_id = producer_exec_id
        self.producer_actor = producer_actor
        self.cause_effect = cause_effect
        self.produced_ref = produced_ref
        self.produced_ver = produced_ref_version
        self.consumer_exec_id = affected_exec_id
        self.consumer_actor = affected_actor
        self.consumer_inputs = affected_inputs
        self.at_depth = depth

    def to_dict(self):
        return {
            "producer_exec": self.producer_exec_id,
            "consumer_exec": self.consumer_exec_id,
            "cause": self.cause_effect,
            "ref": self.produced_ref,
            "version": self.produced_ver,
            "affected_inputs": self.consumer_inputs,
        }

    def diff(self, other):
        return _diff(self, other)

    def show(self, color=True, simple=False, colour=None):
        color, simple = _show_options(color, simple, colour)
        return _show_fields(
            "AFFECTED STACK LINK",
            repr(self),
            self.to_dict(),
            color=color,
            simple=simple,
        )
