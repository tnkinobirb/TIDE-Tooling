# Effects

**Effects let an Execution interact with runtime state beyond its ordinary declared inputs and outputs.**

They can read, write, or delete References during execution, emit Nodes (further Executions), control Pipeline runs, or record a controlled exception.

Realised Effects are recorded as execution evidence and remain inspectable through the resulting Trace.

TIDE exposes two broad Effect styles:

```text
Live Effects
├── READ
├── WRITE_NOW
└── DELETE_NOW

Effect objects
├── WRITE
├── DELETE
├── EMIT
├── RUN
├── END
└── RAISE
```

**Live Effects** are **functions** and happen immediately while a Node executes.

**Effect objects** are declarable on Nodes and returnable by Node Executions. These are handled by Ocean as part of its Execution result processing.

## Effect objects

Effect objects may carry an `output`.

`output` is assigned the default value: `__fallback__`, but may be reassigned:
```python
from tide_tooling import DELETE, __fallback__

DELETE("global:write", output=__fallback__)
```
### Declared

Nodes may be defined with multiple **Declared Effects**. 
A *declared* Effect's `output` does not replace the Execution's result.

```python
from tide_tooling import WRITE, DELETE, node

@node(effects=[
    WRITE("global:result", 42),
    DELETE("global:stale_data")])
def produce():
    return None
```

### Returned

Executions may directly return a single Effect object as its output.
A *returned* Effect's assigned `.output` acts as the substitute for that Execution's result.
This prevents a TIDE Effect object from propagating into other downstream Executions.

If a returned Effect has no explicit `.output`, the returned Effect is still handled normally, but the Node's declared target writes are skipped. This is explicit behaviour and allows for pre-declared writes to be prevented if runtime behaviour necessitates it.

```python
from tide_tooling import DELETE, node

# Explicit output: "produced" is written to the declared target: "produced_out".

@node(targets=["produced_out"])
def produce():
    return DELETE("global:stale_data", output="produced")


# No explicit output: the declared target "produced_out" is not written to.

@node(targets=["produced_out"])
def produce():
    return DELETE("global:stale_data")
```

When returning Effects, Ocean recognises only singly returned Effects. An Execution that returns a list or tuple of Effect objects will not be recognised for handling.

---
### `WRITE` & `DELETE`

`WRITE` stores a value at one or more References.
`DELETE` removes one or more References.

```python
from tide_tooling import WRITE, DELETE

# Write to a Reference
return WRITE("global:write", 42)

# Delete a Reference
return DELETE("global:delete")
```

Multiple References may be written/deleted together:

```python
# Write one value to multiple References
return WRITE(["global:left", "global:right"], 42)

# Write multiple Reference:value pairs
return WRITE({
    "global:left": 1,
    "global:right": 2,
})

# Delete multiple references
return DELETE([
    "global:temporary_a",
    "global:temporary_b",
])
```

---
### `EMIT`

`EMIT` introduces one or more Node definitions during runtime.

```python
from tide_tooling import EMIT

return EMIT(child_node)
```

An emitted Node previously not seen becomes *registered*.
The Execution directly responsible for that Node's emission is assigned as the parent.
Further emissions of the same Node definition are recorded as evidence, but do not replace the assigned responsibility.

---
### `RUN`

`RUN` starts one or more Pipeline runs under the same Ocean.

```python
from tide_tooling import RUN

return RUN(pipeline_id)
```

Pipeline-run options such as execution mode, local Store templates, `stay_alive`, runner name, and dynamic Reference planning may also be supplied.

---
### `END`

`END` ends one or more Pipeline runs.

```python
from tide_tooling import END

return END(runner_id)
```

When no runner ID is supplied, the current Pipeline run may be targeted by the runtime.

---
### `RAISE`

`RAISE` represents an exception.

`RAISE`'s error is recorded and continues pipeline execution carrying forward the defined `output` value.

Standard `raise` works as normal and halts the current Pipeline run.

```python
from tide_tooling import RAISE

return RAISE("validation failed")
```

Additional Effects may also be supplied:

```python
from tide_tooling import DELETE, RAISE

return RAISE(
    "validation failed",
    effects=DELETE("global:temporary"),
)
```

---
## Live Effects

Live Effects happen immediately while the function is still executing. Their result is available to later code in that same function.

Live Effects are only supported within *sequential or thread-pool execution*.

---
### `READ`

`READ` retrieves the value stored at a Reference while the function is still executing.

```python
from tide_tooling import READ

def consume():
    value = READ("global:prepared")
    return value
```

If available, `READ` returns the value stored at the target Reference.
Otherwise, `READ` returns its designated `fallback` value.

A fallback may be supplied explicitly. TIDE's default fallback value is:

```python
from tide_tooling import __fallback__

value = READ(
    "global:optional",
    fallback=__fallback__,
)
```

---

### `WRITE_NOW`

`WRITE_NOW` writes a Reference immediately while the function is still executing.

```python
from tide_tooling import WRITE_NOW

def work():
    written = WRITE_NOW(
        "global:progress",
        50,
    )
    ...
```

Returns `True` when the immediate write is carried out.
Returns `False` when the referenced Store is unavailable.

When the Store is unavailable, the attempted Effect is still recorded, but no State is produced.

---
### `DELETE_NOW`

`DELETE_NOW` removes a Reference immediately while the function is still executing.

```python
from tide_tooling import DELETE_NOW

def work():
    removed = DELETE_NOW("global:temporary")
    ...
```

Returns `True` when the immediate deletion is carried out.
Returns `False` when the complete Reference is not found.

---
## Summary

Effects extend an Execution beyond its ordinary declared inputs and outputs.

TIDE has two Effect styles:

- **Effect objects** are handled by Ocean as part of Execution result processing. They may be declared on a Node or returned directly by an Execution. A Node may declare multiple Effects, but Ocean handles only one directly returned Effect. For returned Effects, `.output` becomes the Execution's ordinary result; without an explicit `.output`, declared target writes are skipped. Declared Effect outputs do not replace the Execution's result.
- **Live Effects** are functions called during the Execution itself. `READ` returns a referenced value or fallback, while `WRITE_NOW` and `DELETE_NOW` mutate runtime state immediately and return operation status. Live Effects are supported only in sequential or thread-pool execution.

```text
READ
→ read value stored at Reference

WRITE / DELETE (WRITE_NOW / DELETE_NOW)
→ change Reference state

RAISE
→ record a controlled exception while carrying forward an output

EMIT
→ introduce Nodes during runtime

RUN / END
→ control Pipeline runs
```
