"""Recommended Ocean workflow using explicit References."""

from tide_tooling import Ocean, node, pipeline

ocean = Ocean()

@node(targets="got_data")
def get_data():
    return "data"

@node(
    targets=["report", "report_available_flag"],
    args=("@got_data",),
    unpack=True, # splits an output container across all targets if len(output) == len(targets) 
)
def report_data(data):
    return {"report": data}, True

@pipeline
def pipeline():
    get_data()
    report_data()

def main():
    trace = ocean.run_pipeline(pipeline)
    trace.show()
    trace.find("report").show()
    trace.value_of("mem_0:report", show=True)
    trace.value_of("mem_0:report_available_flag", show=True)


if __name__ == "__main__":
    main()
