import copy
import random

import pytest

from tide_tooling import TIDE, Trace
from tide_tooling.tooling_core.engine.tide_model import TIDEModel


BASE_MODEL = {
    "host": "left-host",
    "E": ["source", "left", "right", "merge", "analyse", "archive"],
    "S": [
        "source-left", "source-right", "ready-left", "ready-right",
        "merged", "analysis", "archive-record",
    ],
    "I": [
        {"e": "left", "s": "source-left"},
        {"e": "right", "s": "source-right"},
        {"e": "merge", "s": "ready-left"},
        {"e": "merge", "s": "ready-right"},
        {"e": "analyse", "s": "merged"},
        {"e": "archive", "s": "merged"},
    ],
    "O": [
        {"e": "source", "s": "source-left"},
        {"e": "source", "s": "source-right"},
        {"e": "left", "s": "ready-left"},
        {"e": "right", "s": "ready-right"},
        {"e": "merge", "s": "merged"},
        {"e": "analyse", "s": "analysis"},
        {"e": "archive", "s": "archive-record"},
    ],
}


def test_model_contract_has_one_implementation_owner():
    assert not hasattr(TIDE, "_normalise_model")
    assert not hasattr(TIDE, "_trace_from_model")
    assert not hasattr(Trace, "_project_trace_to_model")
    assert not hasattr(Trace, "_reconcile_event_model")
    assert callable(TIDEModel.normalise)
    assert callable(TIDEModel.to_trace)
    assert callable(TIDEModel.from_trace)


def _pairs(entries):
    return {
        (
            entry.get("exec_id", entry.get("e")),
            entry.get("state_id", entry.get("s")),
        )
        if isinstance(entry, dict)
        else tuple(entry)
        for entry in entries
    }


def _renamed(model):
    exec_ids = {
        exec_id
        for exec_id, _ in _pairs(model["I"]) | _pairs(model["O"])
    }
    state_ids = {
        state_id
        for _, state_id in _pairs(model["I"]) | _pairs(model["O"])
    }
    exec_map = {
        exec_id: f"host-exec-{index + 40}"
        for index, exec_id in enumerate(sorted(exec_ids, reverse=True))
    }
    state_map = {
        state_id: f"host-state-{index + 80}"
        for index, state_id in enumerate(sorted(state_ids, reverse=True))
    }

    renamed = {
        "host": "right-host",
        "E": [exec_map[exec_id] for exec_id in reversed(model["E"])],
        "S": [state_map[state_id] for state_id in reversed(model["S"])],
        "I": [
            [exec_map[exec_id], state_map[state_id]]
            for exec_id, state_id in reversed(tuple(_pairs(model["I"])))
        ],
        "O": [
            {"exec_id": exec_map[exec_id], "state_id": state_map[state_id]}
            for exec_id, state_id in reversed(tuple(_pairs(model["O"])))
        ],
        "executions": {
            exec_map[exec_id]: {"actor": f"actor-{index}"}
            for index, exec_id in enumerate(sorted(exec_ids))
        },
        "states": {
            state_map[state_id]: {"value": f"value-{index}"}
            for index, state_id in enumerate(sorted(state_ids))
        },
        "host_record": {"native": "ignored by structural identity"},
    }
    renamed["I"].append(copy.deepcopy(renamed["I"][0]))
    renamed["O"].append(copy.deepcopy(renamed["O"][0]))
    return renamed


def test_structural_model_is_invariant_to_ids_order_duplicates_and_enrichment():
    renamed = _renamed(BASE_MODEL)

    left = TIDE.from_model(BASE_MODEL).to_model(
        enrich=False,
        identity_mode="structural",
    )
    right = TIDE.from_model(renamed).to_model(
        enrich=False,
        identity_mode="structural",
    )

    assert left["I"] == right["I"]
    assert left["O"] == right["O"]
    assert set(left) == {"host", "E", "S", "I", "O", "executions", "states"}
    assert "host_record" not in right
    for field, carrier in (("executions", "E"), ("states", "S")):
        assert {entry["original_id"] for entry in right[field].values()} == set(renamed[carrier])
        assert all(set(entry) == {"original_id"} for entry in right[field].values())


def test_structural_model_keeps_origin_without_schema_metadata():
    canonical = TIDE.from_model({
        "host": "NOW",
        "E": ["run"],
        "S": ["result"],
        "I": [],
        "O": [["run", "result"]],
    }).to_model(enrich=False, identity_mode="structural")

    assert canonical == {
        "host": "NOW",
        "E": ["exec_0"],
        "S": ["state_0"],
        "I": [],
        "O": [{"exec_id": "exec_0", "state_id": "state_0"}],
        "executions": {"exec_0": {"original_id": "run"}},
        "states": {"state_0": {"original_id": "result"}},
    }


def test_structural_import_keeps_explicit_host_when_model_omits_it():
    canonical = TIDE.from_model({
        "E": ["run"],
        "S": ["result"],
        "I": [],
        "O": [["run", "result"]],
    }, host="NOW", identity_mode="structural").to_model(
        enrich=False,
        identity_mode="structural",
    )

    assert canonical["host"] == "NOW"


def test_structural_model_is_idempotent():
    canonical = TIDE.from_model(BASE_MODEL).to_model(
        enrich=False,
        identity_mode="structural",
    )
    repeated = TIDE.from_model(
        canonical,
        identity_mode="structural",
    ).to_model(
        enrich=False,
        identity_mode="structural",
    )

    # Canonical structure is idempotent; enrichment identifies this invocation's input IDs.
    assert {field: repeated[field] for field in ("host", "E", "S", "I", "O")} == {
        field: canonical[field] for field in ("host", "E", "S", "I", "O")
    }
    for field, carrier in (("executions", "E"), ("states", "S")):
        assert {entry["original_id"] for entry in repeated[field].values()} == set(canonical[carrier])


def test_structural_import_builds_only_canonical_trace_identities():
    trace = TIDE.from_model(
        _renamed(BASE_MODEL),
        identity_mode="structural",
    )

    assert all(
        str(record.exec_id).startswith("exec_")
        for record in trace.log
    )
    assert all(
        str(record.func_name).startswith("exec_")
        for record in trace.log
    )
    references = {
        reference
        for record in trace.log
        for reference in (
            *(dependency.unscoped_reference for dependency in record.deps),
            *(write.stored_target for write in record.writes),
        )
    }
    assert all(reference.startswith("__model__:state_") for reference in references)


def test_symmetric_branches_do_not_need_host_identity_tie_breaking():
    first = {
        "E": ["source", "sink-left", "sink-right"],
        "S": ["left", "right"],
        "I": [
            ["sink-left", "left"],
            ["sink-right", "right"],
        ],
        "O": [
            ["source", "left"],
            ["source", "right"],
        ],
    }
    second = {
        "E": ["producer-z", "consumer-a", "consumer-b"],
        "S": ["state-a", "state-b"],
        "I": [
            ["consumer-b", "state-b"],
            ["consumer-a", "state-a"],
        ],
        "O": [
            ["producer-z", "state-a"],
            ["producer-z", "state-b"],
        ],
    }

    first_view = TIDE.from_model(first).to_model(
        enrich=False,
        identity_mode="structural",
    )
    second_view = TIDE.from_model(second).to_model(
        enrich=False,
        identity_mode="structural",
    )

    assert first_view["I"] == second_view["I"]
    assert first_view["O"] == second_view["O"]


def test_large_symmetric_fan_out_is_pruned_by_proven_interchangeability():
    branch_count = 12
    model = {
        "E": ["producer"] + [
            f"consumer-{index}" for index in range(branch_count)
        ],
        "S": [f"state-{index}" for index in range(branch_count)],
        "I": [
            [f"consumer-{index}", f"state-{index}"]
            for index in range(branch_count)
        ],
        "O": [
            ["producer", f"state-{index}"]
            for index in range(branch_count)
        ],
    }

    canonical = TIDE.from_model(model).to_model(
        enrich=False,
        identity_mode="structural",
    )

    assert len(canonical["I"]) == branch_count
    assert len(canonical["O"]) == branch_count
    assert {
        edge["exec_id"]
        for edge in canonical["O"]
    } == {"exec_0"}


def test_disconnected_symmetric_components_are_canonicalised_independently():
    component_count = 40
    original = {
        "E": [f"execution-{index}" for index in range(component_count)],
        "S": [f"state-{index}" for index in range(component_count)],
        "I": [],
        "O": [
            [f"execution-{index}", f"state-{index}"]
            for index in range(component_count)
        ],
    }
    relabelled = {
        "E": [f"host-execution-{index}" for index in reversed(range(component_count))],
        "S": [f"host-state-{index}" for index in reversed(range(component_count))],
        "I": [],
        "O": [
            [f"host-execution-{index}", f"host-state-{index}"]
            for index in reversed(range(component_count))
        ],
    }

    original_view = TIDE.from_model(original).to_model(
        enrich=False,
        identity_mode="structural",
    )
    relabelled_view = TIDE.from_model(relabelled).to_model(
        enrich=False,
        identity_mode="structural",
    )

    for field in ("E", "S", "I", "O"):
        assert original_view[field] == relabelled_view[field]


def test_structural_view_is_stable_across_relabelled_dag_cases():
    random_source = random.Random(20260731)

    for _ in range(24):
        execution_count = random_source.randint(1, 7)
        inputs = []
        outputs = []
        for producer in range(execution_count):
            state_id = f"state-{producer}"
            outputs.append([f"exec-{producer}", state_id])
            for consumer in range(producer + 1, execution_count):
                if random_source.random() < 0.35:
                    inputs.append([f"exec-{consumer}", state_id])

        original = {
            "E": [f"exec-{index}" for index in range(execution_count)],
            "S": [f"state-{index}" for index in range(execution_count)],
            "I": inputs,
            "O": outputs,
        }
        permutation = list(range(execution_count))
        random_source.shuffle(permutation)
        exec_map = {
            f"exec-{index}": f"host-exec-{permutation[index] + 100}"
            for index in range(execution_count)
        }
        state_map = {
            f"state-{index}": f"host-state-{permutation[index] + 200}"
            for index in range(execution_count)
        }
        relabelled = {
            "E": [exec_map[exec_id] for exec_id in original["E"]],
            "S": [state_map[state_id] for state_id in original["S"]],
            "I": [
                [exec_map[exec_id], state_map[state_id]]
                for exec_id, state_id in reversed(inputs)
            ],
            "O": [
                [exec_map[exec_id], state_map[state_id]]
                for exec_id, state_id in reversed(outputs)
            ],
        }

        original_view = TIDE.from_model(original).to_model(
            enrich=False,
            identity_mode="structural",
        )
        relabelled_view = TIDE.from_model(relabelled).to_model(
            enrich=False,
            identity_mode="structural",
        )

        assert original_view["I"] == relabelled_view["I"]
        assert original_view["O"] == relabelled_view["O"]


def test_structural_comparison_still_distinguishes_different_relations():
    changed = copy.deepcopy(BASE_MODEL)
    changed["I"] = [
        edge
        for edge in changed["I"]
        if edge != {"e": "archive", "s": "merged"}
    ]

    compared = TIDE.from_model(BASE_MODEL).compare(
        TIDE.from_model(changed),
        mapping="structural",
    )

    assert compared.conformant is False
    assert compared.complete_agreement is False


def test_empty_relation_kernel_has_a_stable_structural_view():
    canonical = TIDE.from_model({"E": [], "S": [], "I": [], "O": []}).to_model(
        enrich=False,
        identity_mode="structural",
    )

    assert canonical["I"] == []
    assert canonical["O"] == []
    assert canonical == TIDE.from_model(canonical).to_model(
        enrich=False,
        identity_mode="structural",
    )


def test_structural_import_keeps_tool_assigned_producer_internal():
    trace = TIDE.from_model(
        {
            "E": ["consumer"],
            "S": ["boundary"],
            "I": [["consumer", "boundary"]],
            "O": [],
        },
        identity_mode="structural",
    )

    external = trace.model_context["external_producer"]
    assigned = [
        record
        for record in trace.log
        if record.exec_id == external["exec_id"]
    ]

    assert len(assigned) == 1
    assert assigned[0].exec_id.startswith("exec_")
    exported = trace.to_model(
        enrich=False,
        identity_mode="structural",
    )
    assert exported["O"] == []
    assert TIDE.report_model_validity(exported)["unique_creation"] is False
