# Queries

**Trace queries let you move from recorded execution to the specific identity, relationship, or value you want to understand.**

The simplest query flow is:

```
Trace
  │
  ├── show()         see the whole execution
  ├── find()         locate something
  ├── inspect()      inspect its identity and history
  ├── investigate()  inspect its provenance relationships
  ├── why()          show an investigation directly
  ├── produced_by()  find direct outputs / producers
  ├── consumed_by()  find direct inputs / consumers
  ├── upstream()     follow what led to it
  ├── downstream()   follow what it affected
  ├── path()         find how two things are connected
  └── value_of()     recover recorded values
```

Most query results support `.show()` for a human-readable view and  
`.to_dict()` for programmatic use.

---
## `show()`

`trace.show()` gives the quickest overview of what happened:

```
trace.show()
```

It surfaces recorded Executions, States, Effects, and identifiers that can then  
be used in more specific queries.

A common start is:

```
trace.show()
trace.find("clean").show()
trace.investigate("clean", "actor").show()
```

---
## Corruption

`trace.corruption` reports causal evidence that could not be reconstructed
cleanly:

```python
{
    "missing_causal_events": {},
    "causal_order_violations": {},
}
```

A missing causal Event was referenced but is absent from the completed stream.
A causal-order violation means the referenced cause occurs after its recorded
consequence. Reconstruction retains the original evidence order; it does not
reorder or invent Events to repair either case. `trace.show()` marks detected
corruption in its header.

---
## `waiting`

`trace.waiting` exposes the exact waiting definitions and unavailable
References recorded by `RUNNER_DEADLOCK` evidence:

```python
trace.waiting.show()
waiting = trace.waiting.to_dict()
```

The structured result contains one entry per node definition, including its
runner identity and the References it was still waiting for. It does not poll
Stores or infer why a node did not execute; it only presents the deadlock
snapshot retained by the Trace. An ordinary non-deadlocked Trace returns an
empty result with `deadlocked=False`.

---
## Query identities

All Trace queries use the same recorded identities. `find()` discovers them;  
`inspect()` and `investigate()` explain them; relationship queries use them as  
their starting points and endpoints.

A Trace may show:

```
1: clean
    used: global:raw_data[v0]
    wrote: str as global:clean_data[v0]
```

Use the identity shown by the Trace and its corresponding query options:

| Identity Type | Example value from Trace | How to query it               |
| ------------- | ------------------------ | ----------------------------- |
| Sequential ID | `1`                      | `kind="seq_id"`               |
| Actor group   | `"clean"`                | `kind="actor"`                |
| Reference     | `"global:raw_data"`      | `kind="reference"`            |
| State         | `"global:raw_data"`      | `kind="reference", version=0` |

A _sequential id_ selects one exact Execution occurrence.

An Actor groups the Executions performed by the same producer. To query a particular execution occurrence of an Actor , please supply the exact `kind="actor", version=occurrence_n`. 

Querying a Reference without an explicit version will reference its entire history rather than one particular State.

You may need to query actual output data or Effects.

| Identity Type | Example values            | How to query it |
| ------------- | ------------------------- | --------------- |
| Value         | `data.csv`                | `kind="value"`  |
| Effect        | `Effects.WRITE`           | `kind="effect"` |
| Effect target | `(Effects.WRITE, target)` | `kind="effect"` |

**TIDE Models** export Execution identities as *Actor occurrences*.
For example: `"clean[0]"` - meaning the first occurrence of `clean`.

When querying, we specify the occurrence by supplying a `version`. 

| TIDE Model Identity Type | Example values         | How to query it           |
| ------------------------ | ---------------------- | ------------------------- |
| Exact Actor occurrence   | `"clean"`              | `kind="actor", version=0` |
| Exact State occurrence   | `"mem:clean_data"`     | `kind="reference", version=0` |
 
 For Store, Reference, and State semantics, see  
`Stores_and_References.md`.

---
## `find()`

`find()` searches across the shared Trace identities described above.

```
trace.find("clean").show()
```

`find()` separates exact matches from nearby matches, so a partial identity can  
be used to discover the complete one:

```
trace.find("result").show()
trace.find("global:result").show()
```

A `Searched` result can also be used programmatically:

```
found = trace.find("clean")

found.first("actor")
found.exists("actor")
found.get_exact("reference")
found.get_nearby("reference")
```

---
## `inspect()`

`inspect()` gives the structured view of one recorded identity.

```
trace.inspect(
    "global:result",
    "reference",
).show()
```

Think:

```
What recorded object is this?
```

It exposes identity, source information, history, reads, writes, and nearby  
relationship seeds.

Actor and Reference names are grouped when they represent multiple recorded  
occurrences. Supply `version` or an exact execution handle when one occurrence  
is required.

---
## `investigate()`

`investigate()` focuses on how an identity participated in execution.

```
trace.investigate(
    "clean",
    "actor",
).show()
```

Think:

```
How did this object participate in execution?
```

For Actors and Executions it surfaces execution history and surrounding  
relationships. For References it shows recorded versions with their producers  
and consumers.

Effects can be investigated in the same way:

```
from tide_tooling import Effects

trace.investigate(
    Effects.WRITE,
    "effect",
).show()
```

For Effect semantics, see `Effects.md`.

---
## `parents_of()` and `children_of()`

These queries expose direct recorded execution ancestry:

```python
trace.parents_of("execution_id").show()
trace.children_of("execution_id").show()
```

In v0.0.3, child records carry this relationship as
`emitted_by_exec_id`. It means that the child work was directly attributed to
the parent Execution. It does not require or imply a native TIDE node-emission
operation.

---
## `why()`

`why()` is the direct display form of `investigate()`:

```
trace.why("clean", "actor")
```

It is equivalent to:

```
trace.investigate("clean", "actor").show()
```

Use `investigate()` when the structured result is needed. Use `why()` when the
human-readable explanation is the result you want.

---
## Relationship queries

Relationship queries operate on the producer-consumer structure recorded by the  
Trace.

### `produced_by()` and `consumed_by()`

Use these for direct input/output relationships:

```
trace.produced_by("id").show()
trace.consumed_by("id").show()
```

For a State:

```
produced_by() → the Execution that produced it
consumed_by() → the Executions that consumed it
```

For an Execution:

```
produced_by() → the States it produced
consumed_by() → the States it consumed
```

### Execution inputs and outputs

An exact Execution can also be queried for the values it received and returned,  
using either its integer `seq_id` or string `exec_id`:

```
values = trace.inputs_outputs_of(7)
inputs = trace.inputs_of(7)
output = trace.output_of(7)
```

These return an `ExecValues` result. Inputs are resolved by default: they are  
the values actually supplied to the Execution after dependency resolution.

```
trace.inputs_of(7).show()
trace.inputs_of(7).to_dict()
```

Use `raw=True` to inspect the originally declared input representation instead:

```
trace.inputs_of(7).show(raw=True)
trace.inputs_of(7).to_dict(raw=True)
```

`raw=True` changes only the input representation. The output remains the actual  
recorded output.

The two input forms can therefore be compared directly:

```
raw = trace.inputs_of(7).to_dict(raw=True)
resolved = trace.inputs_of(7).to_dict()

trace.diff(raw, resolved).show()
```

### Node definitions

`definition_of()` selects the exact recorded Node definition behind an
Execution, using a `seq_id`, `exec_id`, or exact `node_definition_id`:

```
definition = trace.definition_of(7)
definition.show()
definition.to_dict()
```

The result exposes all six raw declaration slots (`args`, `kwargs`, `p_args`,
`p_kwargs`, `k_args`, and `k_kwargs`), declared dependencies, targets,
Effects, execution options, every source runner/pipeline, and every Execution
of that definition. It also works for an emitted definition which was
registered but never executed.

This query reports retained definition evidence. It does not reconstruct an
executable `Node`, resolve unavailable dependencies, or infer why a definition
did not execute. Use `trace.waiting` for the exact dependencies recorded as
unavailable at settlement.

Conceptually:

```
execution identity
        ↓
    ExecRecord
        ↓
   ExecValues
   ├── raw inputs
   ├── resolved inputs
   └── recorded output
```

---
### `upstream()` and `downstream()`

Use these when you want to traverse beyond the direct relationship:

```
upstream()   → What led to this?
downstream() → What followed from this?
```

```
trace.upstream(
    "global:result",
    "reference",
).show()

trace.downstream(
    "global:raw_data",
    "reference",
).show()
```

Given:

```
load
  │
  ▼
raw_data[v0]
  │
  ▼
clean
  │
  ▼
clean_data[v0]
  │
  ▼
report
```

`upstream()` walks toward earlier producer-consumer relationships;  
`downstream()` walks toward later ones.

Both accept a depth limit:

```
trace.upstream(
    "global:result",
    "reference",
    depth=2,
).show()
```

---
### `path()`

`path()` asks whether and how two recorded objects are connected.

```
trace.path(
    "load",
    "report",
    "actor",
    "actor",
).show()
```

Any TIDE identity type may be used as endpoints but are always first normalised  
to Execution and State occurrences.

A path can cross multiple Executions and States:

```
load
  │
  ▼
raw_data[v0]
  │
  ▼
clean
  │
  ▼
clean_data[v0]
  │
  ▼
report
```

Traversal is downstream by default. The opposite direction can be requested:

```
trace.path(
    "report",
    "load",
    "actor",
    "actor",
    direction="upstream",
).show()
```

`path()` returns a `Pathed` result:

```
result = trace.path(
    "load",
    "report",
    "actor",
    "actor",
)

result.exists()
result.count()
result.get_path()
result.edges()
```

---
## `value_of()`

`value_of()` recovers values recorded for a Reference.

```
trace.value_of("global:result")
```

Without a version, it returns the recorded values across Reference versions.

For one exact State:

```
trace.value_of(
    "global:result",
    version=0,
)
```

Use `show=True` to display the value with its recorded identity:

```
trace.value_of(
    "global:result",
    version=0,
    show=True,
)
```

A TIDE value hash may also be supplied directly:

```
trace.value_of(value_hash)
```

For Reference, State, and version semantics, see  
`Stores_and_References.md`.

---
## `used_as()`

`used_as()` compares one exact State's stored value identity with every value
actually observed by its consuming executions.

```
uses = trace.used_as("global:result[v0]")

uses.show()
uses.to_dict()
```

Restrict the result to one consuming execution sequence with `seq_id`:

```
trace.used_as("global:result[v0]", seq_id=7)
```

The result preserves separate uses when one execution consumes the same State
more than once. `uses.by_seq_id` groups those observations without collapsing
them, while `uses.values` returns detached copies of the observed values.

`value_of()` remains the stored State authority. `used_as()` reports the
recorded input observations of that authority, including value hashes, types,
input positions, mutability, snapshot lineage, and `observation_id`. The
observation ID records read chronology; execution `seq_id` records completion
order, so concurrent executions may have those orders reversed.

---
## `diff()`

`diff()` compares recorded Trace evidence or specific recorded objects.

```
trace.diff(other_trace).show()
```

or:

```
trace.diff(
    "global:before",
    "global:after",
    "reference",
    "reference",
).show()
```

`diff()` compares the concrete evidence itself. For execution-relationship  
comparison through the TIDE Model, use `compare()` instead.

See `Comparison.md`.

---
## Summary

```
See the execution
→ show()

Locate an identity
→ find()

Inspect what it is
→ inspect()

Inspect how it participated
→ investigate()

Ask for a direct producer / consumer relationship
→ produced_by() / consumed_by()

Inspect the values supplied to / returned by an Execution
→ inputs_outputs_of() / inputs_of() / output_of()

Traverse execution relationships
→ upstream() / downstream()

Find one connection
→ path()

Recover recorded values
→ value_of()

Compare recorded evidence
→ diff()
```

Start broad when the exact identity is unknown, then narrow to the occurrence,  
relationship, or value you need.
