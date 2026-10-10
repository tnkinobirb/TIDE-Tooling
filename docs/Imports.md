# Imports

**TIDE imports external execution evidence by translating it into either a TIDE Model or a Trace.**

TIDE adapters project supported external evidence into a TIDE Model, which can then be projected into a TIDE Trace. 

```text
TIDE Adapters → TIDE Model

then
 
TIDE Model    → TIDE.from_model() → Trace 
```

The import boundary is based on what the source actually records. TIDE does not
invent host provenance.

When a represented State has no represented producer, the projected Trace uses
`TIDE: External` as the producer boundary:

```text
TIDE: External
      │
      ▼
     State
      │
      ▼
   consumer
```

`TIDE: External` means only that the producer is outside or unknown to the
represented evidence. It does not identify the real external producer.

The original host/model evidence remains unchanged; the external producer is a
TIDE-side Trace representation of the missing boundary.

---
## TIDE Model

Import a TIDE Model with:

```python
from tide_tooling import TIDE

trace = TIDE.from_model(model)
```

The resulting Trace supports the normal query surface:

```python
trace.show()
trace.find("clean").show()
trace.upstream("clean", "actor").show()
```

Producerless States are represented under the `TIDE: External` `actor` within the projected
Trace. This allows relationship queries to still have explicit producer boundaries.

The source Model itself is unchanged; only the TIDE Trace represents these producerless States.

For Model semantics, see [`TIDE_Model.md`](TIDE_Model.md).

---
## Host adapters

External formats can first be projected into the TIDE Model:

```text
host evidence
     │
     ▼
   adapter
     │
     ▼
TIDE Model
     │
     ▼
TIDE.from_model()
     │
     ▼
   Trace
```

The adapter determines which relationships are supported by the host evidence.
TIDE then projects those relationships into its Trace representation.

---
## OpenLineage

OpenLineage `RunEvent` evidence can be imported directly:

```python
trace = TIDE.from_openlineage(events)
```

The adapter maps:

```text
Run → Execution
input Dataset → consumed State
output Dataset → produced State
```

Repeated events for the same `runId` are combined as additive observations of
that one execution. The Job namespace and name provide its Actor/definition
identity, while lifecycle events supply status and timestamps when present.

Each distinct Dataset output occurrence becomes one explicit State identity in
`S`. Repeated outputs therefore remain distinct occurrences. An OpenLineage
`datasetVersion` facet does not replace or define those TIDE State identities.

OpenLineage does not ordinarily capture the Dataset's runtime value or Python
type, so that value evidence remains unknown/opaque. The adapter preserves the
recorded lineage rather than inventing a value, hash, type, or finer-grained
dependency. TIDE renders this absence of recorded value evidence as
`opaque<TIDEUnknownValue>`.

The intermediate Model is also available:

```python
from tide_tooling import OpenLineageAdapter

model = OpenLineageAdapter(events).to_model()
trace = TIDE.from_model(model)
```

If an input Dataset has no represented producer in the imported evidence, the
projected Trace places that State behind `TIDE: External`.

Only explicitly listed input/output Datasets become relationships. Job names,
timestamps and shared namespaces do not imply additional edges.

---
## OpenTelemetry

OpenTelemetry Span evidence can be imported directly:

```python
trace = TIDE.from_opentelemetry(spans)
```

The generic adapter deliberately projects only what every Span establishes:

```text
Span            -> Execution
parent Span ID  -> directly attributed child Execution
```

TIDE v0.0.3 stores that direct parent relationship in
`emitted_by_exec_id`. Here "emitted" means only that the child work is
attributed to the parent Execution; it does not claim that a native TIDE
node-emission operation occurred.

It does not create States or input/output relationships from Span nesting,
attributes, timing or names. Attributes, Span Events and Links remain available
in `trace.host_record` as source evidence without gaining invented TIDE
semantics.

Every execution ID directly indexes its normalised source entry:

```python
record = trace.log[0]
source = trace.host_record[record.exec_id]
span = source["span"]
resource = source["resource"]
scope = source["scope"]
```

The service name, when explicitly present in the Resource, scopes the Actor.
Otherwise the instrumentation scope name is used, followed by the Trace ID as
the final factual fallback.

---
## OpenInference

OpenInference span evidence can be imported with:

```python
trace = TIDE.from_openinference(spans)
```

OpenInference is projected as OpenTelemetry Span evidence plus only its
explicit local I/O:

```text
Span                 -> Execution
parent_span_id       -> emitted child Execution
input.value present  -> local producerless State read by that Execution
output.value present -> local State written by that Execution
```

`openinference.span.kind` is required. OpenInference records `input.value` and
`output.value` as serialised strings, optionally describing their format with
the corresponding MIME attribute. TIDE retains that exact representation and
the MIME type without parsing it or claiming the original runtime value was a
Python string. These values render as `opaque<TIDEUnknownType>`: a represented
value exists, but its original runtime type is not established. Each remains a
Span-local State occurrence; matching representations in two Spans do not
establish a cross-Span dependency.

If either value attribute is absent, no corresponding State is invented.
Other OpenInference attributes remain available in the directly keyed source
record:

```python
record = trace.log[0]
source = trace.host_record[record.exec_id]
```

The adapter does not use messages, tools, documents, model metadata, matching
values, `graph.node.parent_id`, Links, names or timing to invent additional
relationships.

---
## noWorkflow

noWorkflow produces richer Trace evidence directly rather than first reducing
the source to a TIDE Model.

Run a script under noWorkflow with:

```python
trace = TIDE.now(
    "experiment.py",
    project_dir="capture-project",
)
```

`TIDE.now()` launches noWorkflow in a separate Python process using
main-script collection. The script runs from its own directory. The
`project_dir` identifies where noWorkflow stores the capture; if omitted it
defaults to the script's directory.

Import an existing noWorkflow project or Trial with:

```python
trace = TIDE.from_now(project_dir)
```

or:

```python
trace = TIDE.from_now(project_dir, trial=trial)
```

Captured activations and evaluations become TIDE Executions. Activations are
execution contexts; Evaluations retain the values and exact dependency edges
recorded by noWorkflow. Recorded file accesses are projected directly onto
their owning Activation as Trace Effect evidence.

The adapter maps recorded noWorkflow evidence:

```
activation → context Execution
contained evaluation → emitted child Execution
call evaluation → emitted nested Activation

evaluation → Execution
evaluation dependency → consumed State
evaluation value → produced State

file read → READ Effect
file write → WRITE Effect + produced file State
```

Evaluation values use noWorkflow's recorded representation and type evidence;
the adapter does not reconstruct the original live Python objects. A written
file State carries noWorkflow's recorded content hash rather than the file
contents.

Activations do not independently reproduce Evaluation values or dependencies.
Execution nesting is available through `trace.parents_of(...)` and
`trace.children_of(...)`; both relationships are derived from emitted
occurrence evidence retained by the Trace.

The integration is optional:

```console
pip install tide-tooling[now]
```

Projection eagerly materialises the selected noWorkflow Trial as an ordinary
TIDE Trace. The resulting Trace is not a lazy database view, and projection
does not alter the noWorkflow project. See
[`Notes_on_noWorkflow.md`](Notes_on_noWorkflow.md) for capture compatibility
and known upstream limitations.

---
## Native Bundle import

A Bundle is already a TIDE-native preservation format:

```python
trace = TIDE.from_bundle(bundle)
```

Bundle import restores the richer Trace rather than translating another
execution representation.

See [`Bundles.md`](Bundles.md).

---
## Writing another adapter

The simplest interoperability boundary for another source is usually the TIDE
Model:

```python
def to_tide_model(source):
    return {
        "E": ...,
        "S": ...,
        "I": ...,
        "O": ...,
    }

trace = TIDE.from_model(to_tide_model(source))
```

The adapter should only encode relationships established by the source and all Identities must represent exact occurrences (rather than just the name) of Executions or States to allow for the correct TIDE interpretation:

```text
source says Execution consumed State → I
source says Execution produced State → O
source does not identify a producer  → leave it unrepresented
                                     → TIDE projects TIDE: External
```

Host-specific metadata can remain enrichment without becoming a TIDE
relationship.

---
## Identity on import

Literal identities are preserved by default:

```python
trace = TIDE.from_model(model, identity_mode="literal")
```

A deterministic structural identity view can also be requested:

```python
trace = TIDE.from_model(model, identity_mode="structural")
```

Structural identity changes the identity view; it does not add host
relationships.

---
## Summary

```text
Bundle → TIDE.from_bundle()
TIDE Model → TIDE.from_model()
OpenLineage → TIDE.from_openlineage()
OpenInference → TIDE.from_openinference()
noWorkflow script → TIDE.now()
existing noWorkflow capture → TIDE.from_now()

unrepresented producer
→ TIDE: External in the projected Trace
→ source evidence remains unchanged
```

Import is a translation boundary: TIDE makes external execution evidence
queryable without changing what the source claimed.
