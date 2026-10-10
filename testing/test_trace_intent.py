from tide_tooling import IntentIdentityMap, IntentMap, Ocean, node, pipeline


def test_trace_init_builds_intent_and_exact_total_maps():
    ocean = Ocean()

    @node(targets="global:raw")
    def load():
        return "raw"

    @node(args=("@global:raw",), targets="global:report")
    def report(raw):
        return raw.upper()

    @pipeline
    def flow():
        load()
        report()

    trace = ocean.run_pipeline(flow, mode="sequential")

    assert trace.intent is not None
    assert trace.intent.host == "Ocean Intent Graph"
    assert trace.intent.map.executions.plan == (("load", 0), ("report", 1))
    assert set(trace.intent.map.executions.actual.values()) == {
        ("load", 0),
        ("report", 1),
    }
    assert trace.intent.map.states.plan == (
        ("global:raw", 0),
        ("global:report", 0),
    )
    assert set(trace.intent.map.states.actual.values()) == set(
        trace.intent.map.states.plan
    )
    assert trace.intent.map.plan_only == {"executions": (), "states": ()}
    assert trace.intent.map.actual_only == {"executions": (), "states": ()}
    assert trace.intent.map.compare().complete_agreement is True


def test_total_identity_map_retains_both_kinds_of_unpaired_identity():
    identities = IntentIdentityMap(
        plan=(("load", 0), ("missing", 1)),
        actual={
            "run-load": ("load", 0),
            "run-extra": ("extra", 2),
        },
        _plan_model_ids={
            ("load", 0): "plan-load",
            ("missing", 1): "plan-missing",
        },
    )

    assert identities.paired == {"plan-load": "run-load"}
    assert identities.plan_only == (("missing", 1),)
    assert identities.actual_only == (("extra", 2),)
    assert identities.to_dict()["plan"] == [
        ["load", 0],
        ["missing", 1],
    ]


def test_dynamic_selector_keeps_expected_and_actual_state_parentage():
    ocean = Ocean()
    consumed = []

    @node(targets="mem:csv")
    def write_csv():
        return "row"

    @node(args=("@mem:csv:*",), targets="global:count")
    def collect(values):
        consumed.append(values)
        return len(values)

    @pipeline
    def flow():
        write_csv()
        collect()

    trace = ocean.run_pipeline(
        flow,
        mode="sequential",
        intent_dynamic_refs={"mem:csv:*": ["mem:csv"]},
    )

    assert consumed == [["row"]]
    assert trace.intent.concretisations == {
        "mem:csv:*": (("mem:csv", 0),),
    }
    assert trace.concretisations == {
        "mem:csv:*": (("mem:csv", 0),),
    }
    assert trace.intent.map.compare().complete_agreement is True


def test_intent_and_actual_concretisations_apply_max_after_minimum():
    ocean = Ocean()
    consumed = []

    @node(targets="mem:csv_0")
    def write_csv_0():
        return "row-0"

    @node(targets="mem:csv_1")
    def write_csv_1():
        return "row-1"

    @node(targets="mem:csv_2")
    def write_csv_2():
        return "row-2"

    @node(targets="mem:csv_3")
    def write_csv_3():
        return "row-3"

    @node(targets="mem:csv_4")
    def write_csv_4():
        return "row-4"

    @node(args=("@mem:csv_*:5:3",), targets="global:count")
    def collect(values):
        consumed.append(values)
        return len(values)

    @pipeline
    def flow():
        write_csv_0()
        write_csv_1()
        write_csv_2()
        write_csv_3()
        write_csv_4()
        collect()

    expected = [f"mem:csv_{index}" for index in range(5)]
    trace = ocean.run_pipeline(
        flow,
        mode="sequential",
        intent_dynamic_refs={"mem:csv_*:5:3": expected},
    )
    selected = tuple((reference, 0) for reference in expected[:3])

    assert consumed == [["row-0", "row-1", "row-2"]]
    assert trace.intent.concretisations == {
        "mem:csv_*:5:3": selected,
    }
    assert trace.concretisations == {
        "mem:csv_*:5:3": selected,
    }
    assert trace.intent.map.compare()["intent_equal"] is True


def test_numeric_star_uses_numeric_bound_for_plan_and_actual_selection():
    ocean = Ocean()
    ocean.register_store(
        "seeded",
        store_template={
            f"csv_{index}": f"row-{index}"
            for index in range(7)
        },
    )
    consumed = []

    @node(args=("@seeded:csv_*:5:*",), targets="global:count")
    def collect(values):
        consumed.append(values)
        return len(values)

    @pipeline
    def flow():
        collect()

    expected = [f"seeded:csv_{index}" for index in range(7)]
    selected = tuple((reference, 0) for reference in expected[:5])
    trace = ocean.run_pipeline(
        flow,
        mode="sequential",
        intent_dynamic_refs={"seeded:csv_*:5:*": expected},
    )

    assert consumed == [[f"row-{index}" for index in range(5)]]
    assert trace.intent.concretisations == {
        "seeded:csv_*:5:*": selected,
    }
    assert trace.concretisations == {
        "seeded:csv_*:5:*": selected,
    }
    assert len(trace.exec_log[-1].deps[0].expanded_to) == 5


def test_imported_trace_has_no_invented_intent_but_is_inspectable():
    from tide_tooling import TIDE

    trace = TIDE.from_model({
        "E": ["p", "c"],
        "S": ["s"],
        "I": [["c", "s"]],
        "O": [["p", "s"]],
    })

    assert trace.intent is None
    assert trace.concretisations == {}


def test_repeated_actor_uses_the_global_planned_and_observed_sequence():
    ocean = Ocean()

    @node(targets="global:value")
    def step():
        return "value"

    @pipeline
    def flow():
        step()
        step()

    trace = ocean.run_pipeline(flow, mode="sequential")

    assert trace.intent.map.executions.plan == (("step", 0), ("step", 1))
    assert set(trace.intent.map.executions.actual.values()) == {
        ("step", 0),
        ("step", 1),
    }
    assert trace.intent.map.states.plan == (
        ("global:value", 0),
        ("global:value", 1),
    )
    assert trace.intent.map.compare()["intent_equal"] is True


def test_unspecified_dynamic_set_is_retained_and_not_invented():
    ocean = Ocean()

    @node(targets="mem:csv")
    def write_csv():
        return "row"

    @node(args=("@mem:csv:*",))
    def collect(values):
        return values

    @pipeline
    def flow():
        write_csv()
        collect()

    trace = ocean.run_pipeline(flow, mode="sequential")

    assert trace.intent.concretisations == {"mem:csv:*": ()}
    assert trace.concretisations == {
        "mem:csv:*": (("mem:csv", 0),),
    }
    assert trace.intent.map.compare()["intent_equal"] is False


def test_failed_planned_write_remains_on_the_plan_side_of_total_state_map():
    ocean = Ocean()

    @node(targets="global:never_written")
    def fail():
        raise RuntimeError("planned failure")

    @pipeline
    def flow():
        fail()

    try:
        ocean.run_pipeline(flow, mode="sequential")
    except RuntimeError as error:
        trace = error.tide_trace
    else:
        raise AssertionError("pipeline should fail")

    assert trace.intent.map.states.plan_only == (
        ("global:never_written", 0),
    )
    assert trace.intent.map.states.actual_only == ()
    report = trace.intent.map.compare()
    assert report["complete_agreement"] is False
    assert report["identity_alignment_complete"] is False
    assert report["intent_equal"] is False


def test_nested_unsequenced_expression_uses_stable_topological_order():
    ocean = Ocean()

    @node(targets="global:raw")
    def load():
        return "raw"

    @node(targets="global:clean")
    def clean(raw):
        return raw.upper()

    @node(targets="global:report")
    def report(cleaned):
        return f"report:{cleaned}"

    @pipeline
    def flow():
        return report(clean(load()))

    trace = ocean.run_pipeline(flow, mode="sequential")

    assert trace.intent.map.executions.plan == (
        ("clean", 1),
        ("load", 0),
        ("report", 2),
    )
    assert trace.intent.map.compare()["intent_equal"] is True
