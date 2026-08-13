from tide_tooling import TIDE
from tide_tooling.core import Trace, ExecRecord, Actor

def test_event_semantically_equals_exec():
    def assert_exec_record_semantic_equivalence(a: ExecRecord, b: ExecRecord):
        assert isinstance(a, ExecRecord)
        assert isinstance(b, ExecRecord)

        def actor_name(actor):
            if actor is None:
                return None

            return getattr(
                actor,
                "source_actor",
                getattr(actor, "func_name", str(actor)),
            )

        def read_semantics(record):
            if getattr(record, "type", None) == "dynamic":
                return tuple(sorted(read_semantics(expanded) for expanded in record.expanded_to))

            ref = getattr(
                record,
                "unscoped_reference",
                getattr(record, "reference", None),
            )

            version = getattr(
                record,
                "resolved_version",
                getattr(record, "version", None),
            )

            producer = (
                getattr(record, "producer_actor", None)
                or getattr(record, "source_actor", None)
                or getattr(record, "producer_id", None)
            )

            producer_name = actor_name(producer)

            return (ref, version, producer_name)

        def write_semantics(record):
            ref = getattr(
                record,
                "stored_target",
                getattr(record, "reference", None),
            )

            version = getattr(
                record,
                "stored_version",
                getattr(record, "version", None),
            )

            return (ref, version)

        def normalise(value):
            if isinstance(value, Actor):
                return actor_name(value)

            if isinstance(value, dict):
                return tuple(sorted(
                    (normalise(key), normalise(item))
                    for key, item in value.items()
                ))

            if isinstance(value, (list, tuple, set)):
                return tuple(sorted(
                    (normalise(item) for item in value),
                    key=repr,
                ))

            if callable(value):
                return getattr(
                    value,
                    "__qualname__",
                    getattr(value, "__name__", str(value)),
                )

            if hasattr(value, "__dict__"):
                return tuple(sorted(
                    (key, normalise(item))
                    for key, item in vars(value).items()
                    if key not in {
                        "exec_id",
                        "seq_id",
                        "ocean_id",
                        "source_ocean_id",
                        "runner_id",
                        "pipeline_id",
                        "node_id",
                        "started_at",
                        "finished_at",
                        "duration_ms",
                        "base_node_id",
                        "base_node_content_id",
                        "content_id",
                    }
                ))

            return value

        def semantic_projection(record):
            reads = getattr(
                record,
                "reads",
                getattr(record, "deps", []),
            ) or []

            writes = getattr(record, "writes", []) or []

            return {
                "actor": actor_name(record.producer_id),
                "func_name": record.func_name,

                "reads": tuple(
                    sorted(
                        (read_semantics(read) for read in reads),
                        key=repr,
                    )
                ),

                "writes": tuple(
                    sorted(
                        (write_semantics(write) for write in writes),
                        key=repr,
                    )
                ),

                "effects": normalise(record.effects or {}),
                "affected": normalise(record.affected or []),

                "emitted_by": actor_name(record.emitted_by),
                "emitted_nodes": normalise(record.emitted_nodes or []),

                "status": record.status,
                "error": normalise(record.error),
            }

        a_semantics = semantic_projection(a)
        b_semantics = semantic_projection(b)

        mismatches = []

        for field in a_semantics:
            if a_semantics[field] != b_semantics[field]:
                mismatches.append(
                    f"{field}:\n"
                    f"  a = {a_semantics[field]!r}\n"
                    f"  b = {b_semantics[field]!r}"
                )

        assert not mismatches, (
            "ExecRecords are not semantically equivalent:\n\n"
            + "\n\n".join(mismatches)
        )

    def same(a=None):
        return "!seed"
    
    def create_same_event()-> Trace:
        tide = TIDE()
        tide.event(same, writes={"@seed": "!seed"})
        tide.event(same, reads=["@seed"])
        trace = tide.trace()
        trace.show()
        return trace

    def create_same_exec() -> Trace:
        tide = TIDE()
        tide.exec(same, writes=["@seed"])
        tide.exec(same, reads=["@seed"])
        trace = tide.trace()
        trace.show()
        return trace
    
    def create_same_event_str_func() -> Trace:
        tide = TIDE()
        tide.event("same", writes={"@seed": "!seed"})
        tide.event("same", reads=["@seed"])
        trace = tide.trace()
        trace.show()
        return trace
    
    event_trace = create_same_event()
    exec_trace = create_same_exec()
    str_trace = create_same_event_str_func()

    event_exec_log = event_trace.exec_log
    tide_exec_log = exec_trace.exec_log
    str_exec_log = str_trace.exec_log
    

    assert len(event_exec_log) == len(tide_exec_log)
    expected_length = len(event_exec_log)    
    assert len(str_exec_log) == expected_length
    for num in range(expected_length):
        event_exec = event_exec_log[num]
        tide_exec = tide_exec_log[num]
        str_exec = str_exec_log[num]

        assert_exec_record_semantic_equivalence(event_exec, tide_exec)
        assert_exec_record_semantic_equivalence(event_exec, str_exec)


def create_trace_fixture() -> Trace:
    tide = TIDE()

    tide.event("A", writes={"@seed": "!seed-0"})
    tide.event("B", reads=["@seed"], writes={"@result": "!result"})
    tide.event("C", writes={"@seed": "!seed-1"})

    return tide.trace()


def test_trace_has_events_log():
    trace = create_trace_fixture()
    assert trace.events

def test_trace_preserves_exec_order():
    trace = create_trace_fixture()

    assert [record.func_name for record in trace.exec_log] == ["A", "B", "C"]


def test_trace_contains_expected_read_amount():
    trace = create_trace_fixture()

    assert sum(len(record.deps or []) for record in trace.exec_log) == 1


def test_trace_contains_expected_write_amount():
    trace = create_trace_fixture()

    assert sum(len(record.writes or []) for record in trace.exec_log) == 3


def test_trace_versions_repeated_writes():
    trace = create_trace_fixture()

    seed_writes = [
        write
        for record in trace.exec_log
        for write in record.writes
        if write.stored_target == "global:@seed"
    ]

    assert [(write.stored_target, write.stored_version) for write in seed_writes] == [
        ("global:@seed", 0),
        ("global:@seed", 1),
    ]

def test_trace_contains_expected_execs():
    trace = create_trace_fixture()

    assert len(trace.exec_log) == 3

    assert trace.exec_log[0].seq_id == 0
    assert trace.exec_log[0].func_name == "A"

    assert trace.exec_log[1].seq_id == 1
    assert trace.exec_log[1].func_name == "B"

    assert trace.exec_log[2].seq_id == 2
    assert trace.exec_log[2].func_name == "C"


def test_trace_reads_expected_ref_and_version():
    trace = create_trace_fixture()

    a_exec = trace.exec_log[0]
    b_exec = trace.exec_log[1]
    c_exec = trace.exec_log[2]

    assert len(a_exec.deps or []) == 0
    assert len(b_exec.deps or []) == 1
    assert len(c_exec.deps or []) == 0

    read = b_exec.deps[0]

    assert read.unscoped_reference == "global:@seed"
    assert read.resolved_version == 0


def test_trace_writes_expected_ref_and_version():
    trace = create_trace_fixture()

    a_exec = trace.exec_log[0]
    b_exec = trace.exec_log[1]
    c_exec = trace.exec_log[2]

    assert len(a_exec.writes or []) == 1
    assert len(b_exec.writes or []) == 1
    assert len(c_exec.writes or []) == 1

    assert (
        a_exec.writes[0].stored_target,
        a_exec.writes[0].stored_version,
    ) == ("global:@seed", 0)

    assert (
        b_exec.writes[0].stored_target,
        b_exec.writes[0].stored_version,
    ) == ("global:@result", 0)

    assert (
        c_exec.writes[0].stored_target,
        c_exec.writes[0].stored_version,
    ) == ("global:@seed", 1)

def test_trace_links_read_to_expected_producer():
    trace = create_trace_fixture()

    a_exec = trace.exec_log[0]
    b_exec = trace.exec_log[1]

    write = a_exec.writes[0]
    read = b_exec.deps[0]

    assert read.unscoped_reference == write.stored_target
    assert read.resolved_version == write.stored_version
    assert read.producer_exec_id == a_exec.exec_id
    assert read.producer_actor == a_exec.producer_id
    assert read.producer_source == a_exec.runner_id

def test_trace_read_matches_producer_write():
    trace = create_trace_fixture()

    a_exec = trace.exec_log[0]
    b_exec = trace.exec_log[1]

    write = a_exec.writes[0]
    read = b_exec.deps[0]

    assert read.unscoped_reference == write.stored_target
    assert read.resolved_version == write.stored_version
    assert read.resolved_value == write.stored_result   

def test_trace_requires_ocean_id():
    try:
        Trace(event_log=[], exec_log=[])
        assert False, "Trace should reject missing ocean_id"
    except ValueError:
        pass

def test_event_trace_contains_expected_runner_metadata():
    trace = create_trace_fixture()

    assert trace.runner is not None
    assert trace.run == "TIDE: .event()"
    assert trace.pipeline == "TIDE: .event()"
    assert trace.execution_sources == ({
        "run": "TIDE: .event()",
        "pipeline_id": "TIDE: .event(A[0])",
        "pipeline": "TIDE: .event()",
    },)

def test_trace_preserves_known_runners_and_pipelines():
    known_runners = {"runner": "runner-data"}
    known_pipelines = {"pipeline": "pipeline-data"}

    trace = Trace(
        event_log=[],
        exec_log=[],
        runner=0,
        ocean_id=0,
        all_known_runners=known_runners,
        all_known_pipelines=known_pipelines,
    )

    assert trace.all_known_runners == known_runners
    assert trace.all_known_pipelines == known_pipelines


def test_trace_indexes_pipeline_ids_and_names_from_known_execs():
    trace = create_trace_fixture()

    assert trace.pipeline_index
    assert set(trace.pipeline_index) == {
        record.pipeline_id
        for record in trace.exec_log
    }

    for record in trace.exec_log:
        inspected = trace.inspect(record.exec_id, "exec_record")
        pipeline_id, pipeline_name = inspected.object.source_pipeline[0]

        assert pipeline_id == record.pipeline_id
        assert trace.pipeline_index[pipeline_id] == pipeline_name

def test_trace_sorts_exec_log_by_seq_id():
    trace = create_trace_fixture()
    trace.show()

    unordered_exec_log = (
        trace.exec_log[2],
        trace.exec_log[0],
        trace.exec_log[1],
    )

    rebuilt_trace = Trace(
        event_log=list(unordered_exec_log),
        exec_log=list(unordered_exec_log),
        runner=0,
        ocean_id=trace.ocean_id,
    )

    assert [record.seq_id for record in rebuilt_trace.exec_log] == [0, 1, 2]
    assert rebuilt_trace.reads == trace.reads
    assert rebuilt_trace.writes == trace.writes
    assert rebuilt_trace.values == trace.values

def test_trace_read_writes_values():
    trace = create_trace_fixture()

    assert len(trace.exec_log) == 3
    assert len(trace.reads) == 1
    assert len(trace.writes) == 3
    assert len(trace.values) == 3

    read = trace.reads[0]

    assert read.unscoped_reference == "global:@seed"
    assert read.resolved_version == 0
    assert read.resolved_value == "!seed-0"

    assert [
        (write.stored_target, write.stored_version, write.stored_result)
        for write in trace.writes
    ] == [
        ("global:@seed", 0, "!seed-0"),
        ("global:@result", 0, "!result"),
        ("global:@seed", 1, "!seed-1"),
    ]

    assert trace.values == (
        "!seed-0",
        "!result",
        "!seed-1",
    )

