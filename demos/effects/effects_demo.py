"""Runnable showcase for TIDE's public reference-effect workflow."""

from tide_tooling import DELETE, READ, Effects, TIDE, WRITE, node


@node(
    effects=[
        WRITE(
            "global:prepared",
            preserve_output={"status": "prepared", "count": 3},
        ),
        WRITE(
            "global:audit",
            preserve_output={"event": "prepare", "accepted": True},
        ),
        DELETE(
            "global:prepared",
            preserve_output={"deleted": "prepared"},
        ),
    ]
)
def prepare(source):
    return {"received": source}


def consume_prepared():
    # READ is imperative runtime evidence. It also becomes a factual causal
    # dependency for this completed run.
    return READ("global:prepared")


def build_effect_trace():
    tide = TIDE()
    tide.declare("source", [1, 2, 3])

    # The list declared on @node is inherited by tide.exec().
    tide.exec(prepare, reads="source")
    tide.exec(consume_prepared, writes="consumed")

    # Manual events accept the same public effect objects.
    tide.event(
        "manual marker",
        effects=WRITE(
            "global:temporary_marker",
            preserve_output="remove me",
        ),
    )
    tide.event(
        "manual cleanup",
        effects=DELETE("global:temporary_marker"),
    )

    return tide.trace()


if __name__ == "__main__":
    trace = build_effect_trace()

    print("\n1. COMPLETE TRACE")
    trace.show()

    print("\n2. FIND WRITE EFFECTS")
    trace.find(Effects.WRITE).show(type="effect")

    print("\n3. INVESTIGATE WRITE EFFECTS")
    trace.investigate(Effects.WRITE, "effect").show()

    consumer = next(
        record
        for record in trace.exec_log
        if record.func_name == "consume_prepared"
    )

    print("\n4. PATH FROM ONE EFFECT TO ITS CONSUMER")
    trace.path(
        (Effects.WRITE, ("global:prepared", 0)),
        consumer.exec_id,
        "effect",
        "exec_record",
    ).show()
    trace.path(
        (Effects.DELETE, ("global:prepared", 0)),
        consumer.exec_id,
        "effect",
        "exec_record",
    ).show()

    print("\n5. DIFF TWO VERSIONED EFFECTS")
    trace.diff(
        (Effects.WRITE, ("global:prepared", 0)),
        (Effects.WRITE, ("global:audit", 0)),
    ).show()

    print("\n6. EXPORT AND RELOAD THE COMPLETE EFFECT TRACE")
    portable_bundle = trace.to_bundle().to_dict()
    imported_trace = TIDE.from_bundle(portable_bundle)
    imported_trace.show()

    print("\n7. QUERY THE IMPORTED EFFECT EVIDENCE")
    imported_trace.investigate(Effects.WRITE, "effect").show(
        show_input_positions=True,
    )

    imported_consumer = next(
        record
        for record in imported_trace.exec_log
        if record.func_name == "consume_prepared"
    )
    imported_trace.path(
        (Effects.WRITE, ("global:prepared", 0)),
        imported_consumer.exec_id,
        "effect",
        "exec_record",
    ).show()
    imported_trace.path(
        (Effects.DELETE, ("global:prepared", 0)),
        imported_consumer.exec_id,
        "effect",
        "exec_record",
    ).show()

    print("\n8. DELETE FOLLOWED BY A MISSING RUNTIME READ")
    deleted_tide = TIDE()
    deleted_tide.declare("ephemeral", {"available": True})
    deleted_tide.event(
        "delete ephemeral",
        effects=DELETE("global:ephemeral"),
    )

    def read_deleted():
        return READ("global:ephemeral")

    deleted_tide.exec(read_deleted, writes="fallback")
    deleted_trace = TIDE.from_bundle(
        deleted_tide.trace().to_bundle().to_dict()
    )
    deleted_trace.investigate(Effects.DELETE, "effect").show(
        show_input_positions=True,
    )
    deleted_reader = next(
        record
        for record in deleted_trace.exec_log
        if record.func_name == "read_deleted"
    )
    deleted_trace.path(
        (Effects.DELETE, ("global:ephemeral", 0)),
        deleted_reader.exec_id,
        "effect",
        "exec_record",
    ).show()
