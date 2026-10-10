from tide_tooling import TIDE, Trace
from _base_data_pipeline import base_pipeline
from _adjusted_data_pipeline import adjusted_pipeline

def main():
    # create Traces with TIDE-Tooling
    base_trace: Trace = base_pipeline()
    other_trace: Trace = adjusted_pipeline()

    # compare execution relationships
    base_trace.compare(other_trace, mapping="structural").show()
    base_trace.compare(other_trace, mapping="literal").show()

    # supply explicit exec and state maps
    exec_map = {"clean[0]": "new_clean[0]"}
    state_map = {
        "global:raw_data[v0]": "global:raw_data[v0]",
        "global:clean_data[v0]": "global:clean_data[v0]"
        }

    # compare within explicit mapped scope 
    base_trace.compare(other_trace, mapping="supplied", execution_map=exec_map, state_map=state_map).show()
    
    # # compare object differences
    # base_trace.diff(other_trace).show()


if __name__ == "__main__":
    main()



