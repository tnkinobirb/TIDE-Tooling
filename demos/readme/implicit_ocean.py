"""Ocean workflow using implicit internal References between Nodes."""

from tide_tooling import Ocean, node, pipeline

ocean = Ocean()

@node
def get_data():
    return "data"

@node 
def report_data(data):
    return {"report": data}

@pipeline
def pipeline():
    # Ocean creates a __tide__ reference to link the two nodes.
    report_data(get_data()) 

def main():
    ocean.run_pipeline(pipeline).show()


if __name__ == "__main__":
    main()
