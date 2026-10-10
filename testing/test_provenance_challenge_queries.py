import copy

import pytest

from tide_tooling import TIDE


CHALLENGE_MODEL = {
    "host": "First Provenance Challenge",
    "I": [
        *[
            [f"e:0{index}:align_warp", f"s:anatomy_header_{index}"]
            for index in range(1, 5)
        ],
        *[
            [f"e:0{index}:align_warp", f"s:anatomy_image_{index}"]
            for index in range(1, 5)
        ],
        *[
            [f"e:0{index}:align_warp", "s:reference_header"]
            for index in range(1, 5)
        ],
        *[
            [f"e:0{index}:align_warp", "s:reference_image"]
            for index in range(1, 5)
        ],
        *[
            [f"e:0{index + 4}:reslice", f"s:warp_parameters_{index}"]
            for index in range(1, 5)
        ],
        *[
            ["e:09:softmean", f"s:resliced_header_{index}"]
            for index in range(1, 5)
        ],
        *[
            ["e:09:softmean", f"s:resliced_image_{index}"]
            for index in range(1, 5)
        ],
        ["e:10:slicer", "s:atlas_header"],
        ["e:10:slicer", "s:atlas_image"],
        ["e:11:slicer", "s:atlas_header"],
        ["e:11:slicer", "s:atlas_image"],
        ["e:12:slicer", "s:atlas_header"],
        ["e:12:slicer", "s:atlas_image"],
        ["e:13:convert", "s:atlas_x_slice"],
        ["e:14:convert", "s:atlas_y_slice"],
        ["e:15:convert", "s:atlas_z_slice"],
    ],
    "O": [
        *[
            [f"e:0{index}:align_warp", f"s:warp_parameters_{index}"]
            for index in range(1, 5)
        ],
        *[
            [f"e:0{index + 4}:reslice", f"s:resliced_header_{index}"]
            for index in range(1, 5)
        ],
        *[
            [f"e:0{index + 4}:reslice", f"s:resliced_image_{index}"]
            for index in range(1, 5)
        ],
        ["e:09:softmean", "s:atlas_header"],
        ["e:09:softmean", "s:atlas_image"],
        ["e:10:slicer", "s:atlas_x_slice"],
        ["e:11:slicer", "s:atlas_y_slice"],
        ["e:12:slicer", "s:atlas_z_slice"],
        ["e:13:convert", "s:atlas_x_graphic"],
        ["e:14:convert", "s:atlas_y_graphic"],
        ["e:15:convert", "s:atlas_z_graphic"],
    ],
}
CHALLENGE_MODEL["E"] = sorted({
    execution
    for execution, _ in CHALLENGE_MODEL["I"] + CHALLENGE_MODEL["O"]
})
CHALLENGE_MODEL["S"] = sorted({
    state
    for _, state in CHALLENGE_MODEL["I"] + CHALLENGE_MODEL["O"]
})


@pytest.fixture
def challenge_trace():
    return TIDE.from_model(
        copy.deepcopy(CHALLENGE_MODEL),
    )


def edge_tuple(edge):
    return (
        edge.producer_exec_id,
        edge.consumer_exec_id,
        edge.read.unscoped_reference,
        edge.read.resolved_version,
    )


def test_unbounded_upstream_walks_complete_challenge_ancestry_once_per_layer(
    challenge_trace,
):
    stream = challenge_trace.upstream(
        "atlas_x_slice",
        "reference",
    )
    root = "e:10:slicer"

    assert stream.count() == 4
    assert stream.truncated is False
    assert {
        layer: len(edges)
        for layer, edges in stream.stream[root].items()
    } == {
        "root": 2,
        0: 8,
        1: 4,
        2: 16,
    }

    edge_keys = {
        edge_tuple(edge)
        for edges in stream.stream[root].values()
        for edge in edges
    }
    assert len(edge_keys) == 30
    assert sum(
        len(layer["handoffs"])
        for root in stream.to_dict()["stream"]
        for layer in root["layers"]
    ) == 30


def test_stream_depth_zero_is_direct_and_positive_depth_adds_layers(
    challenge_trace,
):
    direct = challenge_trace.upstream(
        "atlas_x_slice",
        "reference",
        depth=0,
    )
    one_layer = challenge_trace.upstream(
        "atlas_x_slice",
        "reference",
        depth=1,
    )

    assert direct.count() == 1
    assert set(direct.stream["e:10:slicer"]) == {"root"}
    assert one_layer.count() == 4
    assert {
        layer: len(edges)
        for layer, edges in one_layer.stream["e:10:slicer"].items()
    } == {"root": 2, 0: 8}


def test_unbounded_stream_has_an_explicit_path_budget(challenge_trace):
    stream = challenge_trace.upstream(
        "atlas_x_slice",
        "reference",
        max_paths=2,
    )

    assert stream.count() == 2
    assert stream.truncated is True
    assert stream.to_dict()["truncated"] is True
    assert "truncated at 2 paths" in stream.show(color=False)


def test_path_groups_parallel_handoffs_into_execution_paths(
    challenge_trace,
):
    from_softmean = challenge_trace.path(
        "softmean",
        "atlas_x_graphic",
        "actor",
        "reference",
    )
    from_align = challenge_trace.path(
        "align_warp",
        "atlas_x_graphic",
        "actor",
        "reference",
    )

    assert from_softmean.count() == 1
    assert from_align.count() == 4

    output = from_align.show(color=False)
    assert "4 paths found" in output
    assert "e:13:convert" in output

    exported = challenge_trace.to_model(enrich=False)
    producers = {
        edge["exec_id"]
        for edge in exported["O"]
        if edge["state_id"] == "s:atlas_x_graphic"
    }
    assert producers == {"e:13:convert"}
    assert TIDE.report_model_validity(exported)["unique_creation"] is False


def test_model_compare_reuses_recorded_external_producer_policy(challenge_trace):
    compared = challenge_trace.compare(challenge_trace, mapping="literal")

    assert compared.complete_agreement is True
    assert compared["models"]["source"]["semantically_valid"] is False
    assert compared["models"]["target"]["semantically_valid"] is False

    represented = challenge_trace.compare(
        challenge_trace,
        mapping="literal",
        represent_unknown_producers=True,
    )
    assert represented.complete_agreement is True
    assert represented["models"]["source"]["semantically_valid"] is True
    assert represented["models"]["target"]["semantically_valid"] is True

    restored = TIDE.from_bundle(challenge_trace.to_bundle())
    assert restored.model_context["external_producer"]
    assert restored.compare(challenge_trace, mapping="literal").complete_agreement

    exported = challenge_trace.to_model(enrich=False)
    assert TIDE.report_model_validity(exported)["semantically_valid"] is False


def test_from_model_rejects_a_trace_with_an_actionable_boundary_message(
    challenge_trace,
):
    with pytest.raises(ValueError, match="structurally invalid TIDE Model"):
        TIDE.from_model(challenge_trace)

    exported = challenge_trace.to_model(enrich=False)
    reimported = TIDE.from_model(
        exported,
    )
    assert reimported.compare(
        challenge_trace,
        mapping="literal",
    ).complete_agreement is True
