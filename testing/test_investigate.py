import re
ANSI_ESCAPE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
def strip_ansi(value: str) -> str:
    return ANSI_ESCAPE.sub("", value)

import pytest

from tide_tooling import TIDE
from tide_tooling.core import Actor, tide_value_hasher
from tide_tooling.tooling_core.engine.events import Effects
from tide_tooling.tooling_core.engine.tide_logging import (
    DependencyUseRecord,
    EffectRecord,
    ExecRecord,
    InputUseRecord,
    Trace,
    WriteRecord,
)





def B(a, b, separate_b):
    return "!B"


def X(a, ab, abc):
    return None


def make_effect_exec(seq_id, name, deps=None, writes=None, effects=None, inputs=None):
    return ExecRecord(
        seq_id=seq_id,
        exec_id=f"{seq_id:032x}",
        ocean_id=0,
        runner_id="tests",
        pipeline_id="tests",
        node_id=name,
        func=lambda: None,
        func_name=name,
        inputs=inputs or {},
        deps=deps or [],
        effects=effects or [],
        declared_effects=[],
        declared_targets=[],
        runtime_effects=[],
        runtime_targets=[],
        declared_deps=[],
        unscoped_store_refs=[],
        status="complete",
        started_at=None,
        finished_at=None,
        duration_ms=0,
        error=None,
        node_options={},
        writes=writes or [],
    )


@pytest.fixture(scope="module")
def trace():
    tide = TIDE()
    tide.ocean.register_store("testing")

    tide.event("A", writes={"@A": "!A"})
    tide.event("B", reads=["@A"], writes={"@B": "!B", "testing:@B": "!B"})
    tide.exec(B, args=["@@A", "@@B", "@testing:@B"], writes=["@B"])
    tide.event(B, reads={"@A": "!A", "@B": "!B"}, writes={"@B": "!B"})
    tide.event("C", reads={"@B": "!B"}, writes={"@C": "!C"})
    tide.event("X", writes={"@C": "!C"})
    tide.exec(X, args=["@@C", "@testing:@B", "@testing:@B"], writes=["@B"])

    return tide.trace()


@pytest.fixture
def effect_trace():
    writer = make_effect_exec(
        0,
        "Writer",
        writes=[WriteRecord("test", "!A", 0, "global:@A")],
    )

    input_use = InputUseRecord(
        "payload",
        None,
        None,
        None,
        None,
        [],
    )
    dep = DependencyUseRecord(
        ("payload", input_use),
        None,
        "static",
        "global:@A",
        "!A",
        0,
        "tests",
        writer.producer_id,
        writer.exec_id,
    )
    input_use.dependencies = [dep]

    consumer = make_effect_exec(
        1,
        "Consumer",
        deps=[dep],
        inputs={"payload": input_use},
    )
    effect = EffectRecord(
        Effects.WRITE,
        ("global:@A", 0),
        origin="declared",
        output="!A",
    )
    effect_producer = make_effect_exec(
        2,
        "EffectProducer",
        effects=[effect],
    )

    return Trace(
        event_log=None,
        exec_log=[writer, consumer, effect_producer],
        runner="tests",
        ocean_id=0,
    )
def relation_exec_ids(relations):
    return [
        execution["id"]
        for relation in relations
        for execution in relation["executions"]
    ]



def assert_version_entry(entry, ref, version, producer_seq, consumer_seqs):
    assert entry["kind"] == "reference_version"
    assert entry["id"] == f"{ref}[v{version}]"
    assert entry["ref"] == ref
    assert entry["version"] == version
    assert entry["versioned_ref"] == {"ref": ref, "version": version}

    producer = entry["producer"]
    assert producer is not None
    assert producer["display"].startswith(f"EXEC_RECORD[{producer_seq}]")

    assert [
        consumer["display"].split("]")[0] + "]"
        for consumer in entry["consumers"]
    ] == [
        f"EXEC_RECORD[{seq}]"
        for seq in consumer_seqs
    ]

    assert entry["used"] is bool(consumer_seqs)
    assert entry["unused"] is not bool(consumer_seqs)


def test_investigate_exec_record_to_dict(trace):
    inspected = trace.inspect(3, "seq_id")
    data = trace.investigate(inspected).to_dict()

    assert data["type"] == "investigate"
    assert data["kind"] == "exec_record"
    assert data["object"]["id"] == inspected.id
    assert data["object"]["display"] == "EXEC_RECORD[3] B"
    assert data["object"]["grouped"] is False

    assert data["summary"] == {
        "execution_count": 1,
        "read_count": 2,
        "write_count": 1,
        "used_write_count": 1,
        "unused_write_count": 0,
    }

    assert len(data["history"]) == 1
    history = data["history"][0]

    assert history["execution"]["id"] == inspected.id
    assert [
        read["id"]
        for read in history["reads"]
    ] == [
        "global:@A[v0]",
        "global:@B[v1]",
    ]
    assert [
        write["id"]
        for write in history["writes"]
    ] == [
        "global:@B[v2]",
    ]

    assert [
        read["id"]
        for read in data["reads"]
    ] == [
        "global:@A[v0]",
        "global:@B[v1]",
    ]
    assert [
        write["id"]
        for write in data["writes"]
    ] == [
        "global:@B[v2]",
    ]

    assert len(data["upstream"]) == 2
    assert len(data["downstream"]) == 1
    assert len(relation_exec_ids(data["upstream"])) == 2
    assert len(relation_exec_ids(data["downstream"])) == 1


def test_investigate_actor_to_dict(trace):
    actor = Actor(trace.ocean_id, "TIDE: .event()", "B")
    inspected = trace.inspect(actor, "actor")
    data = trace.investigate(inspected).to_dict()

    assert data["kind"] == "actor"
    assert data["object"]["id"] == actor.to_dict()
    assert data["object"]["display"] == "ACTOR[B]"
    assert data["object"]["grouped"] is False

    assert data["summary"] == {
        "execution_count": 2,
        "read_count": 3,
        "write_count": 3,
        "used_write_count": 3,
        "unused_write_count": 0,
    }

    assert [
        entry["execution"]["display"]
        for entry in data["history"]
    ] == [
        "EXEC_RECORD[1] B",
        "EXEC_RECORD[3] B",
    ]

    assert [
        read["id"]
        for read in data["reads"]
    ] == [
        "global:@A[v0]",
        "global:@A[v0]",
        "global:@B[v1]",
    ]
    assert [
        write["id"]
        for write in data["writes"]
    ] == [
        "global:@B[v0]",
        "testing:@B[v0]",
        "global:@B[v2]",
    ]


def test_investigate_show_keeps_sections_and_provenance_dim(trace):
    investigated = trace.investigate("global:@B", "reference")
    output = investigated.show(explore_tool_tips=False)

    assert "\033[2mHISTORY:\033[0m" in output
    assert "\033[2m└── produced by\033[0m" in output
    assert "\033[2m\033[1mHISTORY:" not in output


def test_investigate_grouped_actor_to_dict(trace):
    data = trace.investigate("B", "actor").to_dict()

    assert data["kind"] == "actor"
    assert data["object"]["grouped"] is True
    assert data["object"]["display"] == "GROUPED[B]"

    assert data["summary"]["execution_count"] == 3
    assert data["summary"]["read_count"] == 6
    assert data["summary"]["write_count"] == 4
    assert data["summary"]["used_write_count"] == 4
    assert data["summary"]["unused_write_count"] == 0

    assert len(data["appearances"]) == 2
    assert [
        appearance["display"]
        for appearance in data["appearances"]
    ] == [
        f"Actor({trace.ocean_id}, 'TIDE: .event()', 'B')",
        f"Actor({trace.ocean_id}, 'TIDE: .exec(0)', 'B')",
    ]

    assert [
        entry["execution"]["display"]
        for entry in data["history"]
    ] == [
        "EXEC_RECORD[1] B",
        "EXEC_RECORD[2] B",
        "EXEC_RECORD[3] B",
    ]


def test_investigate_reference_version_to_dict(trace):
    inspected = trace.inspect("global:@B", "reference", version=1)
    data = trace.investigate(inspected).to_dict()

    assert data["kind"] == "reference"
    assert data["object"]["id"] == "global:@B"
    assert data["object"]["version"] == 1
    assert data["object"]["grouped"] is False

    assert data["summary"] == {
        "version_count": 2,
        "used_version_count": 2,
        "unused_version_count": 0,
        "all_consumed": True,
        "none_consumed": False,
        "read_count": 1,
        "write_count": 1,
    }

    assert [
        entry["id"]
        for entry in data["history"]
    ] == [
        "global:@B[v0]",
        "global:@B[v1]",
    ]

    assert_version_entry(
        data["history"][0],
        "global:@B",
        0,
        producer_seq=1,
        consumer_seqs=[2],
    )
    assert_version_entry(
        data["history"][1],
        "global:@B",
        1,
        producer_seq=2,
        consumer_seqs=[3],
    )


def test_investigate_grouped_reference_to_dict(trace):
    data = trace.investigate("B", "reference").to_dict()

    assert data["kind"] == "reference"
    assert data["object"]["grouped"] is True
    assert data["object"]["display"] == 'GROUPED[B]'

    assert data["summary"] == {
        "version_count": 5,
        "used_version_count": 4,
        "unused_version_count": 1,
        "all_consumed": False,
        "none_consumed": False,
        "read_count": 6,
        "write_count": 5,
    }

    assert [
        entry["id"]
        for entry in data["history"]
    ] == [
        "global:@B[v0]",
        "global:@B[v1]",
        "global:@B[v2]",
        "global:@B[v3]",
        "testing:@B[v0]",
    ]

    assert [
        appearance["id"]
        for appearance in data["appearances"]
    ] == [
        "global:@B",
        "testing:@B",
    ]

    assert data["history"][3]["unused"] is True
    assert data["history"][3]["consumers"] == []


def test_investigate_grouped_reference_version_to_dict(trace):
    data = trace.investigate("B", "reference", version=0).to_dict()

    assert data["kind"] == "reference"
    assert data["object"]["grouped"] is True
    assert data["object"]["version"] == 0

    assert data["summary"]["version_count"] == 2
    assert data["summary"]["used_version_count"] == 2
    assert data["summary"]["unused_version_count"] == 0
    assert data["summary"]["all_consumed"] is True

    assert [
        entry["id"]
        for entry in data["history"]
    ] == [
        "global:@B[v0]",
        "testing:@B[v0]",
    ]


def test_investigate_value_to_dict(trace):
    inspected = trace.inspect("!B", "value")
    data = trace.investigate(inspected).to_dict()

    assert data["kind"] == "value"
    assert data["object"]["id"] == tide_value_hasher("!B")
    assert data["object"]["grouped"] is False

    assert data["summary"] == {
        "version_count": 4,
        "used_version_count": 4,
        "unused_version_count": 0,
        "all_consumed": True,
        "none_consumed": False,
        "read_count": 6,
        "write_count": 4,
    }

    assert [
        entry["id"]
        for entry in data["history"]
    ] == [
        "global:@B[v0]",
        "testing:@B[v0]",
        "global:@B[v1]",
        "global:@B[v2]",
    ]


@pytest.mark.parametrize(
    ("investigated", "kind", "version", "expected_header", "expected_lines"),
    [
        (
            "event_b_actor",
            "actor",
            None,
            "INVESTIGATE ACTOR[B]",
            ["EXEC_RECORD[1] B", "global:@B[v0]", "EXEC_RECORD[3] B"],
        ),
        (
            "B",
            "actor",
            None,
            "INVESTIGATE ACTOR GROUPED[B]",
            ["APPEARED AS:", "EXEC_RECORD[2] B", "global:@B[v2]"],
        ),
        (
            "global:@B",
            "reference",
            0,
            "INVESTIGATE REFERENCE[global:@B[v0]]",
            ["global:@B[v0]", "produced by EXEC_RECORD[1] B"],
        ),
        (
            "!B",
            "value",
            None,
            "INVESTIGATE value '!B'",
            ["testing:@B[v0]", "global:@B[v2]"],
        ),
        (
            3,
            "seq_id",
            None,
            "INVESTIGATE EXEC_RECORD[3] B",
            ["global:@A[v0]", "global:@B[v1]", "global:@B[v2]"],
        ),
    ],
)
def test_investigate_show_smoke(
    trace,
    investigated,
    kind,
    version,
    expected_header,
    expected_lines,
):
    if investigated == "event_b_actor":
        investigated = Actor(trace.ocean_id, "TIDE: .event()", "B")
    inspected = trace.inspect(investigated, kind, version)
    output = strip_ansi(trace.investigate(inspected).show(explore_tool_tips=False))

    assert expected_header in output

    for line in expected_lines:
        assert line in output


def test_investigate_simple_show_compacts_actor_appearances(trace):
    investigated = trace.investigate("B", "actor")

    output = strip_ansi(
        investigated.show(
            explore_tool_tips=False,
            simple=True,
        )
    )

    assert "APPEARED AS:" in output
    assert "ACTOR[B]" in output
    assert "EXEC_RECORD[1] B" in output
    assert "from pipeline" not in output


def test_investigate_simple_actor_appearance_ids_are_dim(trace):
    investigated = trace.investigate("B", "actor")
    output = investigated.show(
        explore_tool_tips=False,
        simple=True,
    )

    assert "\033[2mACTOR[B]\033[0m" in output


def test_investigate_simple_show_preserves_history(trace):
    investigated = trace.investigate("B", "actor")

    default_output = strip_ansi(
        investigated.show(explore_tool_tips=False)
    )
    simple_output = strip_ansi(
        investigated.show(
            explore_tool_tips=False,
            simple=True,
        )
    )

    assert default_output.split("HISTORY:", 1)[1] == simple_output.split(
        "HISTORY:",
        1,
    )[1]


def test_investigate_simple_show_compacts_reference_appearances(trace):
    investigated = trace.investigate("B", "reference")
    output = strip_ansi(
        investigated.show(
            explore_tool_tips=False,
            simple=True,
        )
    )

    assert "REFERENCE[global:@B]" in output
    assert "REFERENCE[testing:@B]" in output


def test_investigate_navigation_has_path_actions(trace):
    data = trace.investigate(trace.inspect(3, "seq_id")).to_dict()

    path_actions = [
        entry
        for entry in data["navigation"]
        if entry["action"] == "path"
    ]

    assert {
        entry["direction"]
        for entry in path_actions
    } == {
        "upstream",
        "downstream",
    }

def test_investigate_exec_preserves_repeated_input_positions_in_dict(trace):
    data = trace.investigate(trace.inspect(6, "seq_id")).to_dict()
    reads = data["history"][0]["reads"]

    assert [read["id"] for read in reads] == [
        "global:@C[v1]",
        "testing:@B[v0]",
        "testing:@B[v0]",
    ]

    assert [read["input_use"] for read in reads] == [
        {"name": "args", "index": 0, "display": "args[0]"},
        {"name": "args", "index": 1, "display": "args[1]"},
        {"name": "args", "index": 2, "display": "args[2]"},
    ]


def test_investigate_reference_consumer_groups_repeated_positions_in_dict(trace):
    data = trace.investigate("testing:@B", "reference", version=0).to_dict()
    history = data["history"]

    assert len(history) == 1

    consumers = {
        consumer["display"]: consumer
        for consumer in history[0]["consumers"]
    }

    assert [
        input_use["display"]
        for input_use in consumers["EXEC_RECORD[2] B"]["input_uses"]
    ] == ["args[2]"]

    assert [
        input_use["display"]
        for input_use in consumers["EXEC_RECORD[6] X"]["input_uses"]
    ] == ["args[1]", "args[2]"]


def test_investigate_show_with_input_positions(trace):
    exec_output = strip_ansi(
        trace.investigate(
            trace.inspect(6, "seq_id")
        ).show(
            explore_tool_tips=False,
            show_input_positions=True,
        )
    )

    assert "global:@C[v1] via args[0] from EXEC_RECORD[5] X" in exec_output
    assert "testing:@B[v0] via args[1] from EXEC_RECORD[1] B" in exec_output
    assert "testing:@B[v0] via args[2] from EXEC_RECORD[1] B" in exec_output

    actor_output = strip_ansi(
        trace.investigate(
            Actor(trace.ocean_id, "TIDE: .event()", "B"),
            "actor",
        ).show(
            explore_tool_tips=False,
            show_input_positions=True,
        )
    )

    assert "consumed by EXEC_RECORD[6] X via args[1, 2]" in actor_output
    assert actor_output.count(
        "consumed by EXEC_RECORD[6] X via args[1, 2]"
    ) == 1


def test_investigate_show_without_input_positions_keeps_read_occurrences(trace):
    exec_output = strip_ansi(
        trace.investigate(
            trace.inspect(6, "seq_id")
        ).show(
            explore_tool_tips=False,
            show_input_positions=False,
        )
    )

    assert "via args" not in exec_output
    assert exec_output.count(
        "testing:@B[v0] from EXEC_RECORD[1] B"
    ) == 2

    actor_output = strip_ansi(
        trace.investigate(
            Actor(trace.ocean_id, "TIDE: .event()", "B"),
            "actor",
        ).show(
            explore_tool_tips=False,
            show_input_positions=False,
        )
    )

    assert "via args" not in actor_output
    assert actor_output.count(
        "consumed by EXEC_RECORD[6] X"
    ) == 1


def test_investigate_effect_show_renders_producer_target_and_affected_execution(
    effect_trace,
):
    investigated = effect_trace.investigate(Effects.WRITE, "effect")
    data = investigated.to_dict()

    assert data["type"] == "investigate"
    assert data["kind"] == "effect"
    assert data["summary"] == {
        "effect_count": 1,
        "target_count": 1,
        "producer_count": 1,
        "affected_execution_count": 1,
    }
    assert data["history"][0]["target"] == "global:@A"
    assert data["history"][0]["version"] == 0
    assert data["history"][0]["producers"][0]["display"].endswith(
        "EffectProducer"
    )
    assert data["history"][0]["affected"][0]["execution"][
        "display"
    ].endswith("Consumer")

    output = strip_ansi(
        investigated.show(
            explore_tool_tips=False,
        )
    )

    assert "1 effect found | 1 affected execution" in output
    assert "WRITE -> global:@A[v0]" in output
    assert "produced by EXEC_RECORD[2] EffectProducer" in output
    assert "affected EXEC_RECORD[1] Consumer via payload" in output
