# Ocean Rundown

**Ocean is TIDE's native execution runtime. It runs Nodes when their dependencies are ready and records the resulting execution evidence.**

A Pipeline declares work and its Reference relationships. Ocean turns that
declaration into runtime execution.
```text
Pipeline
   │
   ▼
Ocean
   │
   ├── watches References
   ├── runs ready Nodes
   ├── reacts to runtime State changes
   └── records execution evidence
   │
   ▼
Trace
```

Native `TIDE.exec()` also uses Ocean underneath.

---
## Nodes are declared work

A **Node** describes work that Ocean may execute.

```python
from tide_tooling import node

@node(
    args="@global:start_value",
    targets="global:end_value",
)
def add_one(n):
    return n + 1
```

The Node declares:

```text
global:start_value
        │
        ▼
     add_one
        │
        ▼
global:end_value
```

The Node is the declaration.

Each time its callable actually runs, TIDE records an Execution occurrence.

For the wider Node API and execution options, see [`Nodes.md`](Nodes.md).

---
## Pipelines collect Nodes

A **Pipeline** groups Node declarations into one runnable definition.

```python
from tide_tooling import pipeline

@pipeline
def flow():
    load()
    clean()
    report()
```

A Pipeline describes intended work. Calling `ocean.run_pipeline()` creates a
runtime run from that definition.

```python
trace = ocean.run_pipeline(flow)
```

`run_pipeline()` accepts a `Pipeline`, an `@pipeline`-decorated callable, or a
list of materialised `Node` objects, and returns the resulting Trace.

```python
trace = ocean.run_pipeline([
    load.as_node(),
    clean.as_node(),
])
```

The list form is shorthand for constructing `Pipeline([...])`; the entries
are Nodes rather than undecorated Python callables.

---
## Dependencies determine execution order

declared Pipeline order does not determine runtime execution order.

References do.

```python
from tide_tooling import Ocean, node, pipeline

ocean = Ocean()

@node(targets="raw_data")
def load():
    return "data"

@node(args="@raw_data")
def clean(data):
    return data.strip()

@pipeline
def flow():
    clean()
    load()
```

Even though `clean()` appears first in the Pipeline definition, it requires:

```text
mem:raw_data
```

so it cannot run until that Reference is available.

Conceptually:

```text
load
  │
  ▼
raw_data becomes available
  │
  ▼
clean becomes ready
  │
  ▼
clean executes
```

Ocean schedules from dependency readiness rather than declaration order.

---
## Plan and runtime

The declared Node and Reference relationships form the Pipeline's **Plan**.

```text
Plan
────────────────────
load → raw_data → clean
```

A Pipeline run begins from that declared structure, then Ocean tracks what is
actually ready and what actually executes.

```text
declared Plan
      │
      ▼
runtime readiness
      │
      ▼
realised Executions
```

The declaration therefore describes possible work and dependencies; the Trace
records the execution that actually occurred.

---
## Writes can enable execution

When a Node writes a Reference, Ocean can react to the new runtime State.

Three things may happen:
- The Reference is created and written to for the first time.
- The Reference exists, and is updated with a new value.
- The Reference exists, and already contains the same value at that location.

When a Reference is written to for the first time, it can satisfy waiting dependencies. A Node becomes ready once all of its dependency requirements are satisfied.

```text
Node A
  │
  ▼
state[v0] 
  │
  ▼
Node B 
```

If a Reference is *updated* (the Reference exists but is written to with a new value), it can cause already executed Nodes to execute again with the updated value.
Re-execution is not forced and is still dependent on all of the Node's declared dependencies being available.

```text
Node A
  │
  ▼
state[v1] <- Node A updates state from v0 to v1 
  │
  ▼
Node B    <- Node B may execute again
```

This enables cyclic execution behaviour.

For example:

```text
current_value[v0]
        │
        ▼
    add_one
        │
        ▼
current_value[v1]
        │
        ▼
    add_one
```

By default, a Node may execute again when an updated dependency makes it ready.

To limit a Node to one Execution, declare it with `one_shot=True`.

```python
from tide_tooling import node

@node(one_shot=True, args="@current_value")
def add_one(n)
	return n
```

Writes supplying the same value already stored under a Reference are still versioned, but do not cause already executed dependent Nodes to execute again.

To treat a write to an existing Reference as an update even when the value is unchanged, use `force_update=True`
```python
@node(force_update=True)
def add_one(n)
	return n
```

---
## Deletes can block work

Using the `DELETE` effect, existing References may be removed/deleted.

```python
from tide_tooling import node, DELETE

# Note: because "delete_me" is declared without an @ within raw `args`, 
# it is treated as a string - NOT as a Reference 

@node(args="delete_me") 
def delete_ref(ref):
	return DELETE(ref)
```

When an existing Reference is deleted, respective satisfied dependencies become unsatisfied.
- If a Node was satisfied and scheduled for execution, it is removed from the queue, unsatisfied and made to wait for that dependency. 
- If a Node is executing, execution will continue; upon completion, the Node is unsatisfied and made to wait for potential re-execution.  Nodes with `one_shot=True` can only execute once and therefore are not considered for re-execution.

```
Node A
  X <- Node A deletes state
  |
  ▼
state    
  |
  X 
Node B  <- Node B's `state` dependency becomes unsatisfied, if previously satisfied
```

---
## Dynamic References

A dependency does not always need to identify one exact Reference ahead of
time.

For example:

```text
global:user_*
```

may resolve from the References available in the Store at runtime.

```text
global:user_*
      │
      ├── global:user_1
      ├── global:user_2
      └── global:user_3
```

Dynamic dependency satisfaction is dependent on its declared minimum `count` and whether the number of References that match it's pattern meet that `count` requirement.  

Dynamic References are covered in more details within `Stores_and_References.md`.

---
## Stores can belong to Ocean or a be scoped to a run

Ocean may use global Stores shared across Pipeline runs and local Stores
instantiated separately for each run.

```python
ocean.register_store(
    "cache",
    scope="global",
)

ocean.register_store(
    "working",
    scope="local",
)
```

Local Stores may also be initialised with different starting values per run using
`local_store_templates`.

```python
trace = ocean.run_pipeline(
    flow,
    local_store_templates={
        "working": {
            "start_value": 42,
        },
    },
)
```

---
## Runtime structure may be dynamic

Ocean execution order is not dependent on the original Pipeline definition.

For an example, a Pipeline may be declared with one Node which then emits more Nodes:

```python
from tide_tooling import pipeline
@node
def to_emit()
	return

@node
def add_one():
	return to_emit.as_node()

@pipeline
def flow():
	add_one()
	return
```

A Node may also use the `EMIT` effect to do so.

```python
from tide_tooling import nodem EMIT

@node()
def node_to_emit():
	return 

@node
def emit_node():
	return EMIT(node_to_emit.as_node()) #<- emit Node
```

`RUN` can start additional Pipeline runs and `END` can stop Pipeline runs.

These operations are retained as execution evidence.

For Effect semantics, see [Effects](Effects.md).

---
## Ocean produces a Trace

While Ocean runs work, TIDE records evidence about what occurred.

That includes realised execution relationships such as:

```text
Execution
├── States consumed
├── States produced
├── Effects
├── emitted Nodes
└── runtime metadata
```

When the Pipeline run settles:

```python
trace = ocean.run_pipeline(flow)
```

it returns an inspectable Trace.

```python
trace.show()
trace.upstream("id").show()
trace.downstream("id").show()
```

The Pipeline describes intended work.

Ocean performs the runtime scheduling.

The Trace preserves what happened.

---
## Summary

```text
Node
│
│ declared work
▼
Pipeline
│
│ declared Node / Reference relationships
▼
Plan
│
│ Ocean watches runtime dependencies
▼
References become ready
│
▼
Nodes execute
│
├── read States
├── write States
├── realise Effects
└── may change runtime structure
│
▼
more References may become ready
│
▼
more execution may occur
│
▼
Trace
```

Ocean is the runtime layer between a declared Pipeline and its realised
execution history.

It schedules Nodes from Reference availability, reacts to runtime State
changes, supports dynamic execution structure, and supplies the evidence TIDE
turns into a Trace.
