import pytest

from tide_tooling import DELETE, EMIT, END, RAISE, READ_EFFECT, RUN, WRITE, DataRef, Effect, Effects, node
from tide_tooling.tooling_core.engine.classes import __fallback__
from tide_tooling.tooling_core.engine.errors import NodeDefinitionError


@node
def example_node():
    return None


def test_every_concrete_effect_has_an_explicit_kind():
    effects = [
        EMIT(example_node.as_node()),
        END(),
        RUN("pipeline"),
        DELETE("global:item"),
        WRITE("global:item", 3),
        READ_EFFECT("global:item"),
        RAISE("stop"),
    ]

    assert all(isinstance(effect, Effect) for effect in effects)
    assert [effect.kind for effect in effects] == [
        Effects.EMIT,
        Effects.END,
        Effects.RUN,
        Effects.DELETE,
        Effects.WRITE,
        Effects.READ,
        Effects.RAISE,
    ]


def test_effect_targets_are_normalized_without_losing_order_or_multiplicity():
    emitted = EMIT([example_node.as_node(), example_node.as_node()])
    run = RUN(["one", "one"], local_store_templates={"mem": object()})
    delete = DELETE(["global:first", "global:first", "global:second"])

    assert len(emitted.nodes) == 2
    assert run.targets == ("one", "one")
    assert tuple(ref.key for ref in delete.targets) == ("first", "second")


def test_write_separates_store_values_from_effect_output():
    effect = WRITE({"global:first": 1, "global:second": None}, output="result")

    assert tuple((ref.key, value) for ref, value in effect.targets) == (("first", 1), ("second", None))
    assert effect.output == "result"

    without_output = WRITE("global:first", 1)
    assert isinstance(without_output.output, __fallback__)


def test_effect_constructors_reject_empty_dynamic_and_ambiguous_operands():
    with pytest.raises(ValueError, match="EMIT requires at least one Node"):
        EMIT([])
    with pytest.raises(ValueError, match="RUN requires at least one Pipeline target"):
        RUN([])
    with pytest.raises(ValueError, match="DELETE requires static references"):
        DELETE("global:items*")
    with pytest.raises(ValueError, match="WRITE requires a value"):
        WRITE("global:item")
    with pytest.raises(ValueError, match="both mapped values and value"):
        WRITE({"global:item": 1}, 2)
    with pytest.raises(ValueError, match="duplicate normalized target"):
        WRITE({"global:item": 1, DataRef("item", "global"): 2})


def test_bare_defaultable_effects_normalize_and_raise_cannot_be_declared():
    @node(targets="global:item", effects=[END, DELETE])
    def valid_defaults():
        return None

    assert [type(effect) for effect in valid_defaults.as_node().effects] == [END, DELETE]

    with pytest.raises(NodeDefinitionError, match="RAISE is execution control"):
        node(effects=RAISE("stop"))(lambda: None).as_node()
