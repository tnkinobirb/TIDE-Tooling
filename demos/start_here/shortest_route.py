"""Shortest runnable route from execution to an inspectable TIDE Trace."""

from tide_tooling import TIDE, Trace


def pipeline() -> Trace:
    # DECLARE WORK
    tide = TIDE()
    
    # Declare and persist a value under a Reference.
    tide.declare("declared", "START: I am now data declared as a Reference")

    # Execute a function through Ocean via TIDE.
    tide.exec(print, reads=["declared"], writes=["print_done"])

    # Declare an external or non-computational event.
    tide.event(
        "human_verifier",
        reads=["print_done"],
        writes={"human_verification_done": True},
    )

    # Produce an inspectable Trace.
    return tide.trace()


def main():
    trace = pipeline()
    trace.show()

    # UNCOMMENT TO TRY OUT
    # trace.investigate("print", "actor").show()
    # trace.investigate("human_verifier", "actor").show()

    # trace.downstream("declared", "reference").show()
    # trace.upstream("human_verification_done", "reference").show()

    # trace.path(
    #     "declared",
    #     "human_verification_done",
    #     "reference",
    #     "reference",
    # ).show()

    # Find and inspect values.
    # trace.find("declared").show()
    # trace.value_of("global:declared", show=True)

    # ADVANCED — MAY SKIP
    #
    # In `reads`, a string is already interpreted as a Reference. In raw
    # `args`, `kwargs`, and injection inputs, prepend `@` to distinguish a
    # Reference from a literal value:
    #
    # tide.exec(print, args=["@declared"], writes=["print_done"])
    #
    # These input forms support nested lists, tuples, and dictionaries that
    # contain References. Sets are converted to tuples to provide stable,
    # hashable inputs.
    #
    # References declared through TIDE default to the `global` Store. Ocean
    # Nodes default to the runner-local `mem` Store. Each Ocean run gives its
    # local Stores physical names such as `mem_0`, while `mem` remains the
    # public alias inside that run. Explicit Stores use forms such as:
    #
    # global:declared
    # mem:declared
    #
    # Dynamic References use STORE:KEY_PATTERN:MIN:MAX. MIN is the number of
    # matches required for satisfaction; MAX is the number selected from those
    # satisfied matches. Numeric MAX cannot exceed numeric MIN. For example:
    #
    # declared*           match the declared* key pattern in the default Store
    # declared*:5:1       require five matches; resolve one
    # global:declared*:5:1 use the global Store explicitly


if __name__ == "__main__":
    main()
