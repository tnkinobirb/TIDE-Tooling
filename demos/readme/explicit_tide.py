"""Recommended TIDE workflow using explicit References."""

from tide_tooling import TIDE, Trace

def get_data(data_available_flag):
    return "data"

def report_data(data):
    return {"report": data}

def pipeline() -> Trace:
    tide = TIDE()

    # declare and persist True as data_available Reference. 
    # References used through TIDE default to the global Store.
    tide.declare("data_available", True)
    
    tide.exec(get_data, reads=["data_available"], writes=["raw_data"])
    tide.exec(report_data, reads=["raw_data"], writes=["report"])
    return tide.trace()

def main():
    pipeline().show()


if __name__ == "__main__":
    main()
