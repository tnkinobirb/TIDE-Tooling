"""Capture the same deliberately buggy workflow through four TIDE APIs."""

from tide_tooling import TIDE, Trace, pipeline, node
from pathlib import Path
SAVE_PATH = Path(__file__).parent

debug_tide = TIDE()

@node
def create_data():    
    data = {num: None for num in range(20)}
    debug_tide.event("create_data", writes={"data": data})
    return data

@node
def process_data(raw_data):
    debug_tide.event("process_data", reads={"data": raw_data})
    # debug_tide.trace(name="debug").show()
    to_process = dict(raw_data)
    for idx, num in to_process.items():
        # if number is even, check if stock is not  0
        if idx%2 == 0:
            # subtle bug - when num is None, it is not changed to -1!
            to_process[idx] = num if num != 0 else -1
    processed = to_process 
    return processed

@node(targets=["trust_flag"])
def trust_check_data(processed_data):
    trust_data = all(isinstance(num, int) for num in processed_data.values())
    return trust_data    


def tide_node_trace() -> Trace:
    tide = TIDE()
    with tide.exec as execute:
        execute(trust_check_data(process_data(create_data())))
    return tide.trace()

def tide_func_trace() -> Trace:
    tide = TIDE()
    tide.exec(create_data, writes="raw")
    tide.exec(process_data, reads="raw", writes="processed")
    tide.exec(trust_check_data, reads="processed", writes="trust")
    return tide.trace()

def tide_event_trace() -> Trace:
    tide = TIDE()
   
    data = create_data()
    tide.event(create_data, writes={"raw": data})
   
    processed_data = process_data(data)
    tide.event(process_data, reads=["raw"], writes={"processed": processed_data})
   
    trust = trust_check_data(processed_data)
    tide.event(trust_check_data, reads=["processed"], writes={"trust": trust})

    return tide.trace()

def ocean_trace() -> Trace:
    tide = TIDE()

    @pipeline
    def subtle_bug():
        data = create_data()
        processed = process_data(data)
        trust = trust_check_data(processed)

    return tide.ocean.run_pipeline(subtle_bug)

def main():
    traces = {
        "tide_event_trace.json": tide_event_trace(),
        "tide_func_trace.json": tide_func_trace(),
        "tide_node_trace.json": tide_node_trace(),
        "tide_ocean_trace.json": ocean_trace(),
    }
    for filename, trace in traces.items():
        trace.show()
        trace.to_bundle(path=SAVE_PATH / filename)


if __name__ == "__main__":
    main()
