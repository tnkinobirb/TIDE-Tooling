import pytest

from tide_tooling import Ocean, TIDE, node, pipeline
from tide_tooling.tooling_core.engine.errors import NodeDefinitionError, StoreAlreadyRegisteredError

# ================================================================
# OCEAN COMPATIBILITY TESTS
# ================================================================
#
# Gap area #7 from tide_next_gaps_1_3_5_7_9.txt.
#
# Purpose:
#   Keep raw Ocean runtime assumptions separate from TIDE wrapper assumptions.
#
# Important split:
#   Raw Ocean / Node default unscoped refs may route to local mem.
#   TIDE.event / TIDE.exec default unscoped refs should route to global.
#
# These tests should not accidentally impose TIDE wrapper semantics onto raw
# Ocean, or raw Ocean local-mem semantics onto TIDE.
# ================================================================
import re

ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def strip_ansi(text):
    return ANSI_RE.sub("", text)

def refs_from_reads(exec_record):
    return [
        (read.unscoped_reference, read.resolved_version)
        for read in exec_record.reads
    ]


def refs_from_writes(exec_record):
    return [
        (write.stored_target, write.stored_version)
        for write in exec_record.writes
    ]


def get_exec(trace, seq_id):
    return trace.inspect(seq_id, "seq_id").object


def actor(trace, name, grouped=False):
    return trace.inspect(name, "actor", grouped=grouped)


def ref(trace, name, version=None):
    if version is None:
        return trace.inspect(name, "reference")

    return trace.inspect(name, "reference", version=version)


def registered_store_names(tide):
    registry = tide.ocean.store_router.registry

    return (set(registry._stores.keys()), set(registry._locals.keys()))


def register_seeded_store(ocean, name, values):
    # NOTE:
    # Current project usage supports register_store(name, kv_dict).
    # Fallback keeps this compatible if the local call shape is keyword-based.
    try:
        return ocean.register_store(name, values)
    except TypeError:
        return ocean.register_store(name, store_template=values)


def test_node_declaration_inputs_and_unpacked_targets_run_without_call_inputs():
    ocean = Ocean()

    @node(targets="got_data")
    def get_data():
        return "data"

    @node(
        targets=["report", "report_available_flag"],
        args=("@got_data",),
        unpack=True,
    )
    def report_data(data):
        return {"report": data}, True

    @pipeline
    def declared_pipeline():
        get_data()
        report_data()

    trace = ocean.run_pipeline(declared_pipeline)

    assert trace.value_of("mem_0:report") == [{"report": "data"}]
    assert trace.value_of("report") == [{"report": "data"}, True]
    assert trace.value_of("report_available_flag") == [True]


def test_ocean_can_disable_unrepresented_primary_input_tracking():
    ocean = Ocean(track_unrepresented=False)

    @node(targets="global:result")
    def consume(value):
        return value + 1

    @pipeline
    def flow():
        consume(2)

    trace = ocean.run_pipeline(flow, mode="sequential")
    consumer = next(
        record for record in trace.exec_log
        if record.func_name == "consume"
    )

    assert trace.value_of("global:result", 0) == 3
    assert consumer.deps == []
    assert not any(
        record.func_name == "TIDE_OBSERVER"
        for record in trace.exec_log
    )


def test_tide_passes_unrepresented_tracking_setting_to_direct_exec():
    tide = TIDE(track_unrepresented=False)

    result = tide.exec(
        lambda value: value + 1,
        args=(2,),
        writes="result",
    )
    trace = tide.trace()
    consumer = next(
        record for record in trace.exec_log
        if record.producer_id.source_actor != "TIDE_OBSERVER"
    )

    assert result == 3
    assert trace.value_of("global:result", 0) == 3
    assert consumer.deps == []
    assert not any(
        record.func_name == "TIDE_OBSERVER"
        for record in trace.exec_log
    )


def test_direct_exec_reconstruction_does_not_replay_unnamed_observation():
    tide = TIDE()

    def get_data(data_available_flag):
        return "data"

    def report_data(data):
        return {"report": data}

    tide.exec(get_data, args=True, writes=["got_data"])
    first_trace = tide.trace()
    observer_exec_id = next(
        record.exec_id
        for record in first_trace.exec_log
        if record.func_name == "TIDE_OBSERVER"
    )

    tide.exec(report_data, reads=["got_data"], writes=["report"])
    tide.exec(lambda: None)
    trace = tide.trace()

    observer_records = [
        record
        for record in trace.exec_log
        if record.func_name == "TIDE_OBSERVER"
    ]
    unnamed_writes = [
        write
        for record in observer_records
        for write in record.writes
        if write.stored_target.startswith("__unnamed__")
    ]

    assert [record.exec_id for record in observer_records] == [
        observer_exec_id
    ]
    assert len(unnamed_writes) == 1
    assert unnamed_writes[0].stored_version == 0

    get_data_record = next(
        record for record in trace.exec_log
        if record.func_name == "get_data"
    )
    assert [
        (read.unscoped_reference, read.resolved_version)
        for read in get_data_record.deps
    ] == [(unnamed_writes[0].stored_target, 0)]


@pytest.mark.parametrize("runtime", [Ocean, TIDE])
def test_unrepresented_tracking_setting_must_be_boolean(runtime):
    with pytest.raises(TypeError, match="track_unrepresented must be a bool"):
        runtime(track_unrepresented="no")


def test_node_declaration_inputs_reject_call_time_inputs():
    @node(args=("@declared",))
    def consume(value):
        return value

    @pipeline
    def invalid_pipeline():
        consume("@called")

    with pytest.raises(NodeDefinitionError, match="declaration-time inputs"):
        invalid_pipeline()


def test_tide_event_default_unscoped_write_is_global_not_mem():
    tide = TIDE()

    tide.event("A", writes={"@A": "A"})

    trace = tide.trace()
    a_exec = get_exec(trace, 0)

    assert refs_from_writes(a_exec) == [
        ("global:@A", 0)
    ]
    assert trace.find("mem_0:@A").exists("reference") == (False, False)


def test_tide_exec_default_unscoped_write_is_global_not_mem():
    def WriteA():
        return "A"

    tide = TIDE()

    tide.exec(WriteA, writes=["@A"])

    trace = tide.trace()
    write_exec = get_exec(trace, 0)

    assert refs_from_writes(write_exec) == [
        ("global:@A", 0)
    ]
    assert trace.find("mem_0:@A").exists("reference") == (False, False)


def test_tide_exec_unscoped_read_reads_global_ref():
    def UseA(a):
        return a

    tide = TIDE()

    tide.event("A", writes={"@A": "A"})
    tide.exec(UseA, args=["@@A"], writes=["@out"])

    trace = tide.trace()
    use_exec = get_exec(trace, 1)

    assert refs_from_reads(use_exec) == [
        ("global:@A", 0)
    ]
    assert refs_from_writes(use_exec) == [
        ("global:@out", 0)
    ]


def test_tide_registered_store_seed_read_preserves_store_scope():
    def UseSeed(seed):
        return seed

    tide = TIDE()
    register_seeded_store(
        tide.ocean,
        "testing",
        {
            "@seed": "seed-value",
        },
    )

    tide.exec(UseSeed, args=["@testing:@seed"], writes=["@out"])

    trace = tide.trace()
    use_exec = get_exec(trace, 1) if len(trace.exec_log) > 1 else get_exec(trace, 0)

    assert refs_from_reads(use_exec) == [
        ("testing:@seed", 0)
    ]


def test_tide_registered_store_seed_read_does_not_create_global_alias():
    def UseSeed(seed):
        return seed

    tide = TIDE()
    register_seeded_store(
        tide.ocean,
        "testing",
        {
            "@seed": "seed-value",
        },
    )

    tide.exec(UseSeed, args=["@testing:@seed"], writes=["@out"])

    trace = tide.trace()

    assert trace.find("testing:@seed", version=0).exists("reference") == (True, False)
    assert trace.find("global:@seed", version=0).exists("reference") == (False, False)


def test_tide_store_router_registered_stores_include_global_mem_and_tide():
    tide = TIDE()

    globals, locals = registered_store_names(tide)

    assert "global" in globals
    assert "__tide__" in locals
    assert "mem" in locals 

def test_tide_exec_registers_runner_local_physical_stores_without_changing_global_default():
    def WriteA():
        return "A"

    tide = TIDE()

    before_globals, before_locals = registered_store_names(tide)

    _, exec_pipeline_id = tide.exec(WriteA, writes=["@A"], return_id=True)

    after_globals, after_locals = registered_store_names(tide) 
    trace = tide.trace()
    write_exec = get_exec(trace, 0)

    assert refs_from_writes(write_exec) == [
        ("global:@A", 0)
    ]

    new_globals = after_globals - before_globals
    new_locals = after_locals - before_locals
    print(before_globals, before_locals)
    print(after_globals, after_locals)
    assert not new_globals
    print(exec_pipeline_id)
    print(after_locals)
    assert any(name == f"mem_TIDE: .exec({exec_pipeline_id})" for name in new_locals)
    assert any(name == f"__tide___TIDE: .exec({exec_pipeline_id})" for name in new_locals)


def test_tide_explicit_mem_ref_routes_to_physical_runner_store_when_supported():
    def WriteMem():
        return "mem-value"

    tide = TIDE()

    tide.exec(WriteMem, writes=["mem:@local"])

    trace = tide.trace()
    write_exec = get_exec(trace, 0)
    writes = refs_from_writes(write_exec)

    assert len(writes) == 1

    written_ref, version = writes[0]
    trace.show()
    assert written_ref.startswith(f"mem_TIDE")
    assert written_ref.endswith(":@local")
    assert version == 0


def test_trace_show_uses_shared_header_and_returns_printed_output(capsys):
    tide = TIDE()
    tide.event("A", writes={"@A": "A"})

    shown = tide.trace().show()
    printed = capsys.readouterr().out

    assert "TRACE" in shown
    assert "RUN:" in shown
    assert "EXECUTIONS:" in shown
    assert "Ocean:" in shown
    assert shown in printed


def test_trace_show_summary_includes_read_and_write_counts(capsys):
    tide = TIDE()
    tide.event("A", writes={"@A": "A"})
    tide.event("B", reads=["@A"])

    shown = tide.trace().show()
    capsys.readouterr()

    assert "2 executions found | 1 write | 1 read" in shown
    assert "\033[2mSources:    1\033[0m" in shown
    assert "TBT" not in shown


def test_trace_show_marks_consumed_and_unused_versions_by_usage(capsys):
    tide = TIDE()
    tide.event("Used", writes={"@used": "used"})
    tide.event("Consumer", reads=["@used"])
    tide.event("Unused", writes={"@unused": "unused"})

    shown = tide.trace().show()
    capsys.readouterr()

    assert "\033[32mglobal:@used[v0]\033[0m" in shown
    assert "\033[31mglobal:@unused[v0]\033[0m" in shown


def test_trace_show_omits_unknown_write_causes(capsys):
    tide = TIDE()
    tide.event("A", writes={"@A": "A"})

    shown = strip_ansi(tide.trace().show())
    capsys.readouterr()     

    assert "wrote: str as global:@A[v0]" in shown
    assert "because unknown" not in shown


def test_tide_global_and_mem_same_key_stay_distinct_when_supported():
    def WriteMem():
        return "mem-value"

    tide = TIDE()

    tide.event("WriteGlobal", writes={"@shared": "global-value"})
    tide.exec(WriteMem, writes=["mem:@shared"])

    trace = tide.trace()

    global_ref = trace.find("global:@shared", version=0)
    assert global_ref.exists("reference") == (True, False)

    mem_refs = [
        match.id
        for match in trace.find("@shared").nearby.refs
        if str(match.id).startswith("mem_TIDE")
    ]

    assert mem_refs


def test_tide_trace_reconstruction_preserves_ocean_and_pipeline_identity_fields():
    def WriteA():
        return "A"

    tide = TIDE()
    tide.exec(WriteA, writes=["@A"])

    trace = tide.trace()
    exec_record = get_exec(trace, 0)

    assert trace.ocean_id is not None
    assert exec_record.object.pipeline_id is not None
    assert exec_record.object.producer_id is not None
    assert exec_record.object.producer_id.source_ocean == trace.ocean_id
    assert exec_record.object.source_ocean_id == trace.ocean_id
    


# @pytest.mark.xfail(
#     reason=(
#         "Raw Ocean/Node compatibility should test unwrapped Ocean default local "
#         "mem behavior directly once a stable public raw-Ocean test helper exists."
#     ),
#     strict=False,
# )
def test_raw_ocean_default_unscoped_node_write_routes_to_local_mem():
    @node(targets=["@A"])
    def WriteA():
        return "A"

    
    tide = TIDE()

    ocean = tide.ocean

    # NOTE:
    # Placeholder: replace with real raw Ocean node/pipeline construction.
    # Do not use tide.exec here, because TIDE wrapper intentionally defaults to global.

    trace, *_ = ocean.run_node(node=WriteA.as_node())

    assert trace.find("mem_0:@A").exists("reference") == (True, False)


# @pytest.mark.xfail(
#     reason=(
#         "Raw Ocean local mem isolation should be tested once raw pipeline/runner "
#         "construction is exposed as a stable test helper."
#     ),
#     strict=False,
# )
def test_raw_ocean_local_mem_is_per_runner_or_pipeline():
    tide = TIDE()

    ocean = tide.ocean

    @node(targets=["@local"])
    def WriteA():
        return "A"
    
    @node(targets=["@diff_local"])
    def WriteADiff():
        return "A"
        
    tide = TIDE()

    ocean = tide.ocean
    trace_one, *_ = ocean.run_node(node=WriteA.as_node())
    trace_two, *_ = ocean.run_node(node=WriteA.as_node())
    trace_three, *_ = ocean.run_node(node=WriteADiff.as_node())

    assert trace_one.find("mem_0:@local").exists("reference") == trace_two.find("mem_1:@local").exists("reference")
    assert (False, False) == trace_one.find("mem_0:@diff_local").exists("reference")
    assert (False, False) == trace_two.find("mem_1:@diff_local").exists("reference")
    assert (True, False)  == trace_three.find("mem_2:@diff_local").exists("reference")

# ================================================================
# OCEAN COMPATIBILITY — STORE ROUTER AND PHYSICAL STORE SEMANTICS
# ================================================================
#
# These tests cover store-router imports/deletes, physical mem identity,
# source metadata, and mixed-scope trace reconstruction.
# ================================================================


def physical_mem_write_refs(trace):
    refs = []

    for exec_record in trace.exec_log:
        for stored_ref, version in refs_from_writes(exec_record):
            if stored_ref.startswith("mem_"):
                refs.append((stored_ref, version))

    return refs

# @pytest.mark.xfail(reason="Behaviour not implemented", strict=False)
def test_registering_same_store_twice_is_defined_behavior():
    tide = TIDE()

    tide.ocean.register_store("testing")

    before = registered_store_names(tide)

    with pytest.raises(StoreAlreadyRegisteredError):
        tide.ocean.register_store("testing")


def test_registered_store_names_remain_inspectable_after_exec_runner_registration():
    def WriteA():
        return "A"

    tide = TIDE()
    tide.ocean.register_store("testing")

    before_global, before_local = registered_store_names(tide)

    tide.exec(WriteA, writes=["@A"])

    after_global, before_local = registered_store_names(tide)

    assert "global" in after_global
    assert "mem" in before_local
    assert "__tide__" in before_local
    assert "testing" in after_global
    assert before_global.issubset(after_global)


def test_register_store_with_seed_dict_does_not_mutate_original_seed_dict():
    seed = {
        "@seed": "seed-value",
    }

    tide = TIDE()
    register_seeded_store(tide.ocean, "testing", seed)

    assert seed == {
        "@seed": "seed-value",
    }


def test_store_router_set_then_tide_event_reads_list_imports_as_version_zero():
    tide = TIDE()

    tide.ocean.store_router.set("global", "@imported", "imported-value")
    tide.event("ImportStored", reads=["@imported"])

    trace = tide.trace()
    import_exec = get_exec(trace, 0)

    assert refs_from_writes(import_exec) == [
        ("global:@imported", 0)
    ]

    imported_ref = ref(trace, "global:@imported", version=0)
    importer = actor(trace, "ImportStored", grouped=False)

    assert trace.path(imported_ref, importer).exists()


def test_store_router_set_then_tide_exec_reads_imports_as_version_zero():
    def UseImported(value):
        return value

    tide = TIDE()

    tide.ocean.store_router.set("global", "@imported", "imported-value")
    tide.exec(UseImported, args=["@@imported"], writes=["@out"])

    trace = tide.trace()
    use_exec = get_exec(trace, 1)

    assert refs_from_reads(use_exec) == [
        ("global:@imported", 0)
    ]
    assert refs_from_writes(use_exec) == [
        ("global:@out", 0)
    ]


def test_store_router_delete_then_tide_exec_read_fails_honestly():
    def UseDeleted(value):
        return value

    tide = TIDE()

    tide.ocean.store_router.set("global", "@deleted", "deleted-value")
    tide.ocean.store_router.delete("global", "@deleted")

    with pytest.raises(Exception):
        tide.exec(UseDeleted, args=["@@deleted"], writes=["@out"])

    trace = tide.trace()

    assert not trace.find("UseDeleted").exists("actor")[0]
    assert trace.find("global:@out").exists("reference") == (False, False)


def test_store_router_delete_then_tide_event_reads_list_fails_honestly():
    tide = TIDE()

    tide.ocean.store_router.set("global", "@deleted", "deleted-value")
    tide.ocean.store_router.delete("global", "@deleted")

    with pytest.raises(Exception):
        tide.event("UseDeletedEvent", reads=["@deleted"])

    trace = tide.trace()

    assert not trace.find("UseDeletedEvent").exists("actor")[0]


def test_two_tide_exec_runs_create_distinct_physical_mem_stores():
    def WriteFirst():
        return "first"

    def WriteSecond():
        return "second"

    tide = TIDE()

    tide.exec(WriteFirst, writes=["mem:@local"])
    tide.exec(WriteSecond, writes=["mem:@local"])

    trace = tide.trace()
    mem_refs = physical_mem_write_refs(trace)

    assert len(mem_refs) == 2
    assert mem_refs[0][0] != mem_refs[1][0]
    assert mem_refs[0][0].endswith(":@local")
    assert mem_refs[1][0].endswith(":@local")


def test_explicit_mem_write_in_second_exec_does_not_alias_first_exec_mem_store():
    def WriteFirst():
        return "first"

    def WriteSecond():
        return "second"

    tide = TIDE()

    tide.exec(WriteFirst, writes=["mem:@shared"])
    tide.exec(WriteSecond, writes=["mem:@shared"])

    trace = tide.trace()

    first_exec_record = get_exec(trace, 0)
    second_exec_record = get_exec(trace, 1)

    first_writes = refs_from_writes(first_exec_record)
    second_writes = refs_from_writes(second_exec_record)

    assert len(first_writes) == 1
    assert len(second_writes) == 1

    assert first_writes[0][0] != second_writes[0][0]
    assert first_writes[0][0].endswith(":@shared")
    assert second_writes[0][0].endswith(":@shared")


def test_physical_mem_store_ref_is_trace_identity_not_user_input_identity():
    def WriteMem():
        return "mem-value"

    tide = TIDE()

    tide.exec(WriteMem, writes=["mem:@local"])

    trace = tide.trace()
    write_exec = get_exec(trace, 0)
    written_ref, version = refs_from_writes(write_exec)[0]

    assert written_ref != "mem:@local"
    assert written_ref.startswith("mem_")
    assert written_ref.endswith(":@local")
    assert version == 0


def test_find_plain_mem_key_finds_nearby_physical_mem_refs_not_exact_logical_ref():
    def WriteMem():
        return "mem-value"

    tide = TIDE()

    tide.exec(WriteMem, writes=["mem:@local"])

    trace = tide.trace()
    searched = trace.find("@local")

    assert searched.exists("reference") == (False, True)

    nearby_ids = [
        match.id
        for match in searched.nearby.refs
    ]

    assert any(str(ref_id).startswith("mem_") and str(ref_id).endswith(":@local") for ref_id in nearby_ids)


def test_pipeline_ids_restart_per_ocean_instance():
    def WriteA():
        return "A"

    tide_a = TIDE()
    tide_b = TIDE()

    tide_a.exec(WriteA, writes=["@A"])
    tide_b.exec(WriteA, writes=["@A"])

    exec_a = get_exec(tide_a.trace(), 0)
    exec_b = get_exec(tide_b.trace(), 0)

    assert exec_a.object.pipeline_id == exec_b.object.pipeline_id


def test_ocean_ids_are_distinct_across_tide_instances():
    def WriteA():
        return "A"

    tide_a = TIDE()
    tide_b = TIDE()

    tide_a.exec(WriteA, writes=["@A"])
    tide_b.exec(WriteA, writes=["@A"])

    trace_a = tide_a.trace()
    trace_b = tide_b.trace()

    assert trace_a.ocean_id != trace_b.ocean_id


def test_exec_source_pipeline_uses_pipeline_id_not_ocean_id():
    def WriteA():
        return "A"

    tide = TIDE()

    tide.exec(WriteA, writes=["@A"])

    trace = tide.trace()
    exec_record = get_exec(trace, 0)

    assert exec_record.object.pipeline_id is not None
    assert exec_record.object.source_ocean_id is not None
    assert exec_record.object.pipeline_id != exec_record.object.source_ocean_id


def test_event_source_pipeline_uses_event_context_not_exec_pipeline_id():
    tide = TIDE()

    tide.event("EventA", writes={"@A": "A"})

    trace = tide.trace()
    event_exec = get_exec(trace, 0)

    assert event_exec.object.source_ocean_id == trace.ocean_id
    assert event_exec.object.pipeline_id is not None

    # NOTE:
    # .event provenance should be explicitly event-context-shaped, not mistaken
    # for a normal .exec pipeline runner.
    assert "event" in str(event_exec.object.pipeline_id).lower() or "event" in str(event_exec.object.producer_id).lower()


def test_trace_rebuild_preserves_exec_count_after_mem_and_global_writes():
    def WriteGlobal():
        return "global"

    def WriteMem():
        return "mem"

    tide = TIDE()

    tide.exec(WriteGlobal, writes=["@global_out"])
    tide.exec(WriteMem, writes=["mem:@mem_out"])

    trace = tide.trace()

    assert len(trace.exec_log) == 2


def test_trace_rebuild_preserves_store_scopes_after_mixed_global_testing_mem_refs():
    def WriteMem():
        return "mem"

    tide = TIDE()
    tide.ocean.register_store("testing")

    tide.event("GlobalA", writes={"@A": "global"})
    tide.event("TestingA", writes={"testing:@A": "testing"})
    tide.exec(WriteMem, writes=["mem:@A"])

    trace = tide.trace()

    assert trace.find("global:@A", version=0).exists("reference") == (True, False)
    assert trace.find("testing:@A", version=0).exists("reference") == (True, False)

    mem_matches = [
        match.id
        for match in trace.find("@A").nearby.refs
        if str(match.id).startswith("mem_")
    ]

    assert mem_matches


def test_trace_rebuild_preserves_runner_ids_for_exec_records():
    def WriteFirst():
        return "first"

    def WriteSecond():
        return "second"

    tide = TIDE()

    tide.exec(WriteFirst, writes=["@first"])
    tide.exec(WriteSecond, writes=["@second"])

    trace = tide.trace()
    first = get_exec(trace, 0)
    second = get_exec(trace, 1)

    assert first.object.producer_id != second.object.producer_id


def test_trace_rebuild_preserves_actor_ids_for_event_and_exec_records():
    def ExecActor():
        return "exec"

    tide = TIDE()

    tide.event("EventActor", writes={"@event": "event"})
    tide.exec(ExecActor, writes=["@exec"])

    trace = tide.trace()

    event_exec = get_exec(trace, 0)
    exec_exec = get_exec(trace, 1)

    assert event_exec.object.producer_id != exec_exec.object.producer_id
    assert "EventActor" == event_exec.object.producer_id.source_actor
    assert "ExecActor" == exec_exec.object.producer_id.source_actor
