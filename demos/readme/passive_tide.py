"""TIDE workflow using a passive Python value as an input."""

from tide_tooling import TIDE, Trace

def get_data(data_available_flag):
    return "data"

def report_data(data):
    return {"report": data}

def pipeline() -> Trace:
    tide = TIDE()

    # "data_available" is never explicitly registered as a Reference.
    data_available = True    
    # Ocean creates one for it, but can be toggled-off
    # Toggling off means the data is not persisted, tracked or shown explicitly with versioning, so data may silently change over time. 
    
    tide.exec(get_data, args=data_available, writes=["got_data"])
    tide.exec(report_data, reads=["got_data"], writes=["report"])
    
    return tide.trace()

def main():
    pipeline().show()


if __name__ == "__main__":
    main()
