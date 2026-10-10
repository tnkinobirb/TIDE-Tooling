import pytest

from tide_tooling import DELETE, DELETE_NOW, READ, TIDE, WRITE, Effects, node


def _effect_signature(record):
    output = (
        "<template>"
        if record.output.__class__.__name__ == "__fallback__"
        else record.output
    )
    return (
        record.effect,
        record.targets,
        output,
        record.origin,
        getattr(record, "target_existed", None),
    )


def test_repeated_effects_are_ordered_realised_occurrences():
    tide = TIDE()
    tide.declare("source", "present")

    def exercise_effects():
        first = READ("global:source")
        first_delete = DELETE_NOW("global:source")
        fallback = READ("global:source", fallback="missing")
        second_delete = DELETE_NOW("global:source")
        return first, first_delete, fallback, second_delete

    result = tide.exec(exercise_effects, writes="result")
    source = tide.trace()
    record = source.log[-1]

    assert result == ("present", True, "missing", False)
    assert [_effect_signature(effect) for effect in record.effects] == [
        (Effects.READ, ("global:source", 0), "present", "runtime", True),
        (Effects.DELETE, ("global:source", 0), True, "runtime", True),
        (Effects.READ, ("global:source", None), "missing", "runtime", False),
        (Effects.DELETE, ("global:source", None), False, "runtime", False),
    ]
    assert [
        dependency.associated_input[0]
        for dependency in record.deps
    ] == ["runtime_read"]
    assert [
        affected.affected_inputs
        for affected in record.effects[1].affected.values()
    ] == [[{"name": "runtime_read", "index": None}]]
    assert record.effects[3].affected == {}

    rebuilt = TIDE.from_bundle(source.to_bundle().to_dict())
    assert [
        _effect_signature(effect)
        for effect in rebuilt.log[-1].effects
    ] == [
        _effect_signature(effect)
        for effect in record.effects
    ]


def test_delete_effect_retains_every_affected_occurrence_of_one_actor():
    tide = TIDE()
    tide.declare("source", "present")

    def remove():
        DELETE_NOW("global:source")

    def observe_missing():
        return READ("global:source", fallback="missing")

    tide.exec(remove)
    tide.exec(observe_missing)
    tide.exec(observe_missing)
    trace = tide.trace()
    executions = tuple(trace.execs.values())
    delete = executions[1].effects[0]

    assert {
        affected.affected_exec_id
        for affected in delete.affected.values()
    } == {executions[2].exec_id, executions[3].exec_id}

    restored = TIDE.from_bundle(trace.to_bundle())
    restored_executions = tuple(restored.execs.values())
    restored_delete = restored_executions[1].effects[0]
    assert {
        affected.affected_exec_id
        for affected in restored_delete.affected.values()
    } == {
        restored_executions[2].exec_id,
        restored_executions[3].exec_id,
    }


def test_duplicate_declared_effects_survive_bundle_round_trip():
    tide = TIDE()
    tide.event(
        "repeat writes",
        effects=[
            WRITE("global:result", "first"),
            WRITE("global:result", "second"),
        ],
    )

    source = tide.trace()
    rebuilt = TIDE.from_bundle(source.to_bundle().to_dict())

    assert [_effect_signature(effect) for effect in source.log[0].effects] == [
        (Effects.WRITE, ("global:result", 0), "<template>", "declared", None),
        (Effects.WRITE, ("global:result", 0), "<template>", "declared", None),
    ]
    assert [
        _effect_signature(effect)
        for effect in rebuilt.log[0].effects
    ] == [
        _effect_signature(effect)
        for effect in source.log[0].effects
    ]


def test_identical_node_effect_declarations_are_not_deduplicated():
    delete = DELETE("global:source")

    @node(effects=[delete, delete])
    def remove_twice():
        return None

    tide = TIDE()
    tide.declare("source", "present")
    tide.exec(remove_twice)
    effects = tide.trace().log[-1].effects

    assert [_effect_signature(effect) for effect in effects] == [
        (
            Effects.DELETE,
                ("global:source", 0),
            "<template>",
            "declared",
            True,
        ),
        (
            Effects.DELETE,
                ("global:source", None),
            "<template>",
            "declared",
            False,
        ),
    ]
    assert "\x1b[33mglobal:source\x1b[0m" in tide.trace().show(color=True)


def test_declared_effect_output_does_not_replace_node_output():
    @node(
        effects=[
            WRITE("global:effect_output", "effect"),
        ]
    )
    def produce():
        return "ordinary"

    tide = TIDE()
    tide.exec(produce, writes="ordinary_output")
    trace = tide.trace()

    assert trace.value_of("global:ordinary_output", 0) == "ordinary"
    assert trace.value_of("global:effect_output", 0) == "effect"


def test_returned_effect_can_still_supply_the_node_output():
    def produce_effect():
        return WRITE("global:effect_output", "effect", output="effect")

    tide = TIDE()
    tide.exec(produce_effect, writes="ordinary_output")
    trace = tide.trace()

    assert trace.value_of("global:ordinary_output", 0) == "effect"
    assert trace.value_of("global:effect_output", 0) == "effect"


def test_mixed_effects_preserve_occurrence_and_post_result_write_order():
    @node(
        effects=[
            WRITE("global:collision", "declared"),
            DELETE("global:declared_delete"),
        ]
    )
    def exercise_every_effect_phase():
        READ("global:source")
        DELETE_NOW("global:runtime_delete")
        return WRITE("global:collision", "returned", output="returned")

    tide = TIDE()
    tide.declare("source", "present")
    tide.declare("runtime_delete", "present")
    tide.declare("declared_delete", "present")
    tide.exec(exercise_every_effect_phase, writes="collision")
    source = tide.trace()
    record = source.log[-1]

    assert [_effect_signature(effect) for effect in record.effects] == [
        (Effects.READ, ("global:source", 0), "present", "runtime", True),
        (
            Effects.DELETE,
            ("global:runtime_delete", 0),
            True,
            "runtime",
            True,
        ),
        (
            Effects.WRITE,
            ("global:collision", 1),
            "<template>",
            "declared",
            None,
        ),
        (
            Effects.DELETE,
            ("global:declared_delete", 0),
            "<template>",
            "declared",
            True,
        ),
        (
            Effects.WRITE,
            ("global:collision", 2),
            "returned",
            "runtime",
            None,
        ),
    ]
    assert [
        source.value_of("global:collision", version)
        for version in range(3)
    ] == ["returned", "declared", "returned"]

    rebuilt = TIDE.from_bundle(source.to_bundle().to_dict())
    assert [
        _effect_signature(effect)
        for effect in rebuilt.log[-1].effects
    ] == [
        _effect_signature(effect)
        for effect in record.effects
    ]
    assert [
        rebuilt.value_of("global:collision", version)
        for version in range(3)
    ] == ["returned", "declared", "returned"]


def test_none_read_fallback_is_not_confused_with_missing_output():
    def read_missing():
        return READ("global:missing", fallback=None)

    tide = TIDE()
    tide.exec(read_missing, writes="fallback")
    source = tide.trace()
    rebuilt = TIDE.from_bundle(source.to_bundle().to_dict())

    source_effect = source.log[-1].effects[0]
    rebuilt_effect = rebuilt.log[-1].effects[0]
    assert source_effect.output is None
    assert source_effect.target_existed is False
    assert rebuilt_effect.output is None
    assert rebuilt_effect.target_existed is False
