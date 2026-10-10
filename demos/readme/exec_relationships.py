"""Query producer/consumer, upstream/downstream, and path relationships."""

from tide_tooling import TIDE

def load():
    return "loaded_raw_data"

def clean(raw_data):
    return "clean_data"


def main():
    tide = TIDE()
    tide.exec(load, writes="raw_data")
    tide.exec(clean, reads=["raw_data"], writes="clean_out")
    trace = tide.trace()

    trace.produced_by("load", "actor").show()
    trace.consumed_by("clean", "actor").show()
    trace.upstream("clean", "actor").show()
    trace.downstream("load", "actor").show()
    trace.path(
        source="load",
        target="clean",
        source_type="actor",
        target_type="actor",
    ).show()
    trace.path(
        source="clean",
        target="load",
        source_type="actor",
        target_type="actor",
        direction="upstream",
    ).show()


if __name__ == "__main__":
    main()
