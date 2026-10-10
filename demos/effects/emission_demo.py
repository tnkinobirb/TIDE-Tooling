"""Show ordered emission relationships through the public API."""

from tide_tooling import EMIT, TIDE, node, pipeline


@node(targets="global:declared_child")
def declared_child():
    return "declared-child"


@node(targets="global:runtime_child")
def runtime_child():
    return "runtime-child"


@node(effects=[EMIT(declared_child.as_node())])
def emitter():
    emitted = runtime_child.as_node()
    return EMIT([emitted, emitted])


@pipeline
def emission_pipeline():
    return emitter()


if __name__ == "__main__":
    trace = TIDE().ocean.run_pipeline(
        emission_pipeline,
        mode="sequential",
    )
    trace.show()
    emitter_record = trace.log[0]
    print("ORDERED EMISSION OCCURRENCES")
    for occurrence in emitter_record.emitted_nodes:
        print(f"{occurrence.origin}: {occurrence.func_name}")

    print("\nEMISSION PATHS")
    trace.path("emitter", "declared_child", "actor", "actor").show()
    trace.path("emitter", "runtime_child", "actor", "actor").show()

    print("\nRESTORED EMISSION PATH")
    restored = TIDE.from_bundle(trace.to_bundle().to_dict())
    restored.path("emitter", "runtime_child", "actor", "actor").show()
