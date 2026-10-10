# Nodes

**A Node declares work that Ocean may execute.**

A Node combines a Python callable with the inputs, output References, Effects,
and execution options needed to run that work through Ocean.

The Node is the declaration.

Each time its callable actually runs, TIDE records an **Execution occurrence**.

```text
Node definition
      │
      ▼
Ocean executes it
      │
      ▼
Execution occurrence
```

For scheduling and Pipeline runtime behaviour, see
[`Ocean_Rundown.md`](Ocean_Rundown.md).

---
## Quickest path: `TIDE.exec()`

For one-off execution, `TIDE.exec()` is the shortest way to run a callable
through TIDE as Node-shaped work.

```python
from tide_tooling import TIDE

tide = TIDE()

def add_one(n):
    return n + 1

tide.declare("start_value", 41)

result = tide.exec(
    add_one,
    reads="start_value",
    writes="end_value",
)
```

`reads` declares the References consumed by the callable and `writes` declares
the References that receive its ordinary result.

Internally, each `tide.exec()` call creates a Node and a one-Node Pipeline,
runs it through Ocean, and returns that Execution's output.

```text
callable
   │
   ▼
TIDE.exec()
   │
   ├── creates a per-call Node
   ├── runs it through Ocean
   └── records the Execution
   │
   ▼
output
```

The generated Node belongs to that `.exec()` invocation rather than acting as
a reusable `@node` declaration.

The execution evidence still remains available through the same TIDE instance:

```python
trace = tide.trace()
```

Use `TIDE.exec()` for direct one-off execution.

Use `@node` with `tide.ocean` when the Node definition itself needs to be reused in Pipelines,
passed around, or emitted at runtime. The below sections covers this case.

---
## Creating a reusable Node

The simplest Node wraps a Python function:

```python
from tide_tooling import node

@node
def load():
    return "data"
```

Configuration is supplied through `@node(...)`:

```python
@node(
    args="@global:start_value",
    targets="global:end_value",
)
def add_one(n):
    return n + 1
```

This declares:

```text
global:start_value
        │
        ▼
     add_one
        │
        ▼
global:end_value
```

A Node may also have it's arguments supplied later:

```python
from tide_tooling import node, pipeline

# Define the Node with 'args'

@node(targets="global:end_value")
def add_one(n):
    return n + 1

# Define the pipeline, and supply the input

@pipeline
def flow():
	add_one.as_node(0) # <- TIDE tracks whole inputs. 
					   # To see them use trace.show(show_undeclared=True)
```

With default `trace.show`, this shows:

```text
   add_one
	  │
 	  ▼
global:end_value
```

With `trace.show(show_undeclared=True)`, the undeclared input evidence is also shown:

```text
__untracked__:add_one.args.<int>.<uuid> 
        │
        ▼
     add_one
        │
        ▼
global:end_value
```

---
## `@node` inside and outside a Pipeline

Outside a Pipeline build, a decorated function behaves like the original
Python function:

```python
@node
def add_one(n):
    return n + 1

result = add_one(1)
```

Inside an `@pipeline`, calling it creates a Node definition instead:

```python
from tide_tooling import pipeline

@pipeline
def flow():
    add_one("@global:start_value")
```

A Node definition can also be created explicitly:

```python
definition = add_one.as_node("@global:start_value")
```

This is useful when a Node object itself needs to be passed, emitted, or
assembled into a Pipeline or Node definition.

For example, to emit a Node:
```python
@node
def node_to_emit():
	return

@node
def emit_node():
    return node_to_emit.as_node()
```

---
## Node names

By default, a Node uses the wrapped function's name.

An explicit name may be supplied:

```python
@node(node_name="normalise_customer")
def clean():
    ...
```

---
## Inputs

A Node's ordinary callable inputs are described through `args` and `kwargs`.

```python
@node(
    args=("@global:a", "@global:b"),
    kwargs={"scale": 2},
)
def combine(a, b, scale=1):
    return (a + b) * scale
```

Strings beginning with `@` are treated as Reference dependencies:

```text
@global:a
@global:b
```

Ordinary values remain ordinary Python inputs.

The `@` is only input syntax. It is not part of Reference identity.

For Reference syntax and Store behaviour, see
[`Stores_and_References.md`](Stores_and_References.md).

---
## Targets

`targets` declares the References that receive a Node's ordinary result.

```python
@node(targets="global:result")
def produce():
    return 42
```

With multiple targets, the same result is written to each target by default:

```python
@node(
    targets=[
        "global:left",
        "global:right",
    ]
)
def produce():
    return 42
```

```text
             ┌── global:left  → 42
produce ─────┤
             └── global:right → 42
```

---
## Unpacking results across targets

Use `unpack=True` when one returned list or tuple should be split across
multiple targets.

```python
@node(
    targets=[
        "global:report",
        "global:ready",
    ],
    unpack=True,
)
def report(data):
    return {"report": data}, True
```

This writes:

```text
global:report → {"report": data}
global:ready  → True
```

The returned value must be a list or tuple with the same number of items as
declared targets.

---
## Nodes can link to other Nodes

A Node may receive another Node directly:

```python
@node
def load():
    return "data"

@node
def clean(data):
    return data.strip()

@pipeline
def flow():
    clean(load())
```

If the upstream Node has no explicit target, Ocean creates an internal
Reference to connect the two definitions.

```text
load
  │
  ▼
internal Reference
  │
  ▼
clean
```

This provides convenient Python-style composition while Ocean still executes
from Reference dependencies rather than source order.

---
## Effects

A Node may declare runtime Effect objects:

```python
from tide_tooling import DELETE, WRITE

@node(
    effects=[
        WRITE("global:prepared", True),
        DELETE("global:temporary"),
    ]
)
def prepare(data):
    return data
```

Live Effect functions such as `READ`, `WRITE_NOW`, and `DELETE_NOW` are called
inside the function rather than declared through `effects=`.

For Effect semantics, see [`Effects.md`](Effects.md).

---
## Re-execution

A Node definition may produce more than one Execution occurrence.

If one of its dependencies changes, Ocean may make the Node ready again:

```text
state[v0]
   │
   ▼
 Node
   │
   ▼
state[v1]
   │
   └──── dependency changes
             │
             ▼
           Node
```

## `one_shot`

Use `one_shot=True` when a Node must execute at most once:

```python
@node(
    args="@global:start_value",
    one_shot=True,
)
def initialise(value):
    ...
```

After its first completed execution, Ocean will not re-evaluate that Node even
if its dependencies later change.

`one_shot` constrains runtime multiplicity; it does not change the Node's
dependency declaration.

---
## `force_update`

Ordinary Store writes can avoid propagating an update when the written value
matches the existing value stored under an existing Reference.

`force_update=True` gives the Node's ordinary and Effect writes force-write
behaviour:

```python
@node(
    targets="global:result",
    force_update=True,
)
def refresh():
    return 42
```

This allows an equal-value write to still be treated as an update by Ocean.

For more on `one_shot` and `force_update` semantics, please reference [Ocean Rundown][docs/Ocean_Rundown].

---
## Advanced input construction

`args` and `kwargs` may themselves be callables.

When they are callable, Ocean evaluates them to construct the actual inputs.

```python
def build_args(a, b=None):
    return a, b

@node(
    args=build_args,
    p_args="@global:a",
    p_kwargs={"b": "@global:b"},
)
def work(values):
    ...
```

`p_args` and `p_kwargs` provide inputs to the callable used for `args`.

`k_args` and `k_kwargs` do the same for a callable used for `kwargs`.

With References such as `@!` or `@STORE:!` Ocean resolves all available dependencies within the referenced store when at least one Reference exists within it.

---
## Shared implicit targets

`share_implicit_target=True` makes implicitly linked occurrences of the same
Node definition share its name-based internal target. By default each
occurrence receives a target based on its unique definition identity.

---
## Summary

```text
Node
├── callable
├── args / kwargs
│    ├── ordinary values
│    └── Reference dependencies
├── targets
├── Effects
└── execution options
     ├── one_shot
     ├── force_update
     ├── unpack
     ├── share_implicit_target
     └── advanced input / output controls
```

A Node describes **what work may execute and what runtime data it depends on or
produces**.

Ocean decides when that Node is ready to run.

The resulting Trace records each Execution occurrence and the exact States it
consumed and produced.
