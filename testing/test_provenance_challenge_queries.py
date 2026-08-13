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


@pytest.fixture
def challenge_trace():
    with pytest.warns(RuntimeWarning, match="does not establish host provenance"):
        return TIDE.from_model(
            copy.deepcopy(CHALLENGE_MODEL),
            assign_unknown=True,
        )


def edge_tuple(edge):
    return (
        edge.producer_exec_id,
        edge.consumer_exec_id,
        edge.produced_ref,
        edge.produced_ver,
    )


def test_unbounded_upstream_walks_complete_challenge_ancestry_once_per_layer(
    challenge_trace,
):
    stream = challenge_trace.upstream(
        "atlas_x_slice",
        "reference",
    )
    root = "e:10:slicer"

    assert stream.count() == 64
    assert stream.execution_route_count() == 4
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
    assert len(stream.to_dict()["edges"]) == 30


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

    assert direct.count() == 2
    assert set(direct.stream["e:10:slicer"]) == {"root"}
    assert one_layer.count() == 16
    assert {
        layer: len(edges)
        for layer, edges in one_layer.stream["e:10:slicer"].items()
    } == {"root": 2, 0: 8}


def test_unbounded_stream_has_an_explicit_path_budget(challenge_trace):
    stream = challenge_trace.upstream(
        "atlas_x_slice",
        "reference",
        max_paths=10,
    )

    assert stream.count() == 10
    assert stream.truncated is True
    assert stream.to_dict()["truncated"] is True
    assert "truncated at 10 paths" in stream.show(color=False)


def test_path_distinguishes_exact_state_paths_from_execution_routes(
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

    assert from_softmean.count() == 2
    assert from_softmean.execution_route_count() == 1
    assert from_align.count() == 16
    assert from_align.execution_route_count() == 4

    output = from_align.show(color=False)
    assert "16 paths found | 4 execution routes" in output
    assert "e:13:convert" in output

    with pytest.warns(RuntimeWarning, match="does not establish host provenance"):
        exported = challenge_trace.to_model(
            include_hashes=False,
            assign_unknown=True,
        )
    producers = {
        edge["exec_id"]
        for edge in exported["O"]
        if edge["state_id"] == "s:atlas_x_graphic"
    }
    assert producers == {"e:13:convert"}
    assert TIDE.validate_model(exported)["checks"]["unique_creation"] is True


def test_model_diff_reuses_recorded_unknown_assignment_policy(challenge_trace):
    compared = challenge_trace.diff(challenge_trace)

    assert compared.same is True
    assert compared.before_report["conforms"] is True
    assert compared.after_report["conforms"] is True

    restored = TIDE.from_bundle(challenge_trace.to_bundle())
    assert restored.model_context["assigned_unknown_states"]
    assert restored.diff(challenge_trace).same is True

    with pytest.raises(ValueError, match="assign_unknown=True"):
        challenge_trace.to_model(include_hashes=False)


def test_from_model_rejects_a_trace_with_an_actionable_boundary_message(
    challenge_trace,
):
    with pytest.raises(
        TypeError,
        match=r"received Trace.*trace\.to_model",
    ):
        TIDE.from_model(challenge_trace, assign_unknown=True)

    with pytest.warns(RuntimeWarning, match="does not establish host provenance"):
        model_with_unknown = challenge_trace.to_model(
            include_hashes=False,
            assign_unknown=True,
        )
    reimported = TIDE.from_model(model_with_unknown)
    assert reimported.diff(challenge_trace).same is True
