"""Typed relationship links used by TIDE path and stream results."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, eq=False, slots=True)
class ReadEdge:
    consumer: object
    read: object

    @property
    def producer_exec_id(self):
        return self.read.producer_exec_id

    @property
    def consumer_exec_id(self):
        return self.consumer.exec_id


@dataclass(frozen=True, eq=False, slots=True)
class EffectEdge:
    producer: object
    effect_record: object
    affected_record: object

    @property
    def producer_exec_id(self):
        return self.producer.exec_id

    @property
    def consumer_exec_id(self):
        return self.affected_record.affected_exec_id


@dataclass(frozen=True, eq=False, slots=True)
class EmissionEdge:
    emitter: object
    emitted_node: object
    emitted_exec_id: object = None

    @property
    def producer_exec_id(self):
        return self.emitter.exec_id

    @property
    def consumer_exec_id(self):
        return self.emitted_exec_id
