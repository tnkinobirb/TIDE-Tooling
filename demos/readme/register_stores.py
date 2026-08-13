"""Register global Stores and seed local memory Stores per pipeline run."""

from tide_tooling import Ocean, TIDE, node, pipeline


@node(args="@data_store:stored_data")
def print_stored_data(value):
    print(value)


@pipeline
def seeded_pipeline():
    print_stored_data()


def main():
    # Stores can be registered through TIDE's Ocean.
    tide = TIDE()
    tide.ocean.register_store(name="via_tide", scope="local")

    ocean = Ocean()
    ocean.register_store(name="via_ocean", scope="global")

    # A local or global Store can be initialised once at registration.
    ocean.register_store(
        name="global_seeded",
        scope="global",
        store_template={"key": "data"},
    )

    # Per-run templates only create registered local MemoryKVStore instances.
    ocean.register_store(name="data_store", scope="local")
    ocean.run_pipeline(
        seeded_pipeline,
        mem_store_templates={
            "data_store": {
                "stored_data": "DATA",
            },
        },
    )


if __name__ == "__main__":
    main()
