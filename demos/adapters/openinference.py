from tide_tooling import TIDE

COLOUR = True

spans = [
    {
        "traceId": "demo-trace",
        "spanId": "agent",
        "name": "research agent",
        "attributes": {
            "openinference.span.kind": "AGENT",
            "input.value": '{"question":"Why?"}',
            "input.mime_type": "application/json",
            "llm.model_name": "source metadata only",
        },
    },
    {
        "traceId": "demo-trace",
        "spanId": "llm",
        "parentSpanId": "agent",
        "name": "chat model",
        "attributes": {
            "openinference.span.kind": "LLM",
            "input.value": '{"prompt":"Explain"}',
            "input.mime_type": "application/json",
            "output.value": '{"answer":"Because."}',
            "output.mime_type": "application/json",
        },
    },
]

trace = TIDE.from_openinference(spans)
trace.show(color=COLOUR, show_undeclared=True)

parent = next(
    record for record in trace.log
    if record.func_name == "research agent"
)
child = next(
    record for record in trace.log
    if record.func_name == "chat model"
)
print("\nCHILDREN")
trace.children_of(parent.exec_id).show(color=COLOUR)
print("\nPARENTS")
trace.parents_of(child.exec_id).show(color=COLOUR)
print("\nEMISSION PATH")
trace.path(
    parent.exec_id,
    child.exec_id,
    "exec_record",
    "exec_record",
    group=False,
).show(color=COLOUR)

print("\nEXACT SOURCE SPAN")
print(trace.host_record[child.exec_id]["span"])
print(
    "\nOpenInference input/output States are local to their Span; "
    "matching values never invent cross-Span data flow."
)
