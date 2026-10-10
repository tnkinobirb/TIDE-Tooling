**Stores hold runtime values. References identify those values within TIDE.**

Native TIDE uses Stores and References to represent the data read and written
during execution.

---
## Stores

A **Store** holds runtime values.

For example:

```text
global
├── start_value → 42
└── end_value   → 43
```

TIDE provides a `global` Store by default.

Additional Stores can be registered:

```python
tide.register_store("cache")
```

Stores may be:

- `global` — shared under the Ocean;
- `local` — instantiated separately for each Pipeline run.

```python
tide.register_store("cache", scope="global")
tide.register_store("working", scope="local")
```

For local Store lifecycle and scheduling, see
[`Ocean_Rundown.md`](Ocean_Rundown.md).

---
## References

A **Reference** identifies a location in a Store.

```text
STORE:KEY
```

For example:

```text
global:start_value
cache:result
```

The Store is part of Reference identity:

```text
global:result ≠ cache:result
```

even if both currently contain the same value.

Native TIDE uses `global` as its default Store, so:

```python
tide.declare("start_value", 42)
```

addresses:

```text
global:start_value
```

The default Store may be changed:

```python
tide.set_default_store("cache")
```

or selected for one execution:

```python
tide.exec(
    use_answer,
    reads=["answer"],
    writes=["result"],
    default_store="cache",
)
```

Explicitly scoped References are unaffected by the selected default.

---
## Reading and writing References

Executions can consume values through `reads` and produce values through
`writes`.

```python
def add_one(n):
    return n + 1

tide.declare("start_value", 42)

tide.exec(
    add_one,
    reads=["start_value"],
    writes=["end_value"],
)
```

This represents:

```text
global:start_value
        │
        ▼
     add_one
        │
        ▼
global:end_value
```

Values from multiple `reads` are supplied as positional inputs in the same
order.

`declare()` also records the supplied value as execution evidence in the
resulting Trace.

---
## Versions and States

A Reference can be reused:

```text
global:count → 1
global:count → 2
global:count → 3
```

The Reference remains:

```text
global:count
```

but TIDE records each observed occurrence with a version:

```text
global:count[v0]
global:count[v1]
global:count[v2]
```

A versioned Reference occurrence is a **State**.

This lets TIDE distinguish:

```text
global:count[v0] → execution A
global:count[v1] → execution B
```

rather than reducing both reads to the same unversioned Reference.

Versions begin independently for each Reference.

A Store represents the live runtime value at a Reference. A Trace preserves
the States observed during execution.

Pre-existing Store contents do not automatically become prior Trace history.

Recorded versions can be queried directly:

```python
trace.value_of("global:count")
trace.value_of("global:count", version=0)
```

Other Trace queries also retain State versions so producer-consumer
relationships remain attached to the exact occurrence used.

See [`Queries.md`](Queries.md) for the wider Trace query surface.

---
## Dynamic References

A Reference may also describe a set of possible concrete References.

For example:

```text
global:user_*
```

may resolve against:

```text
global:user_1
global:user_2
global:user_3
```

The execution ultimately consumes the concrete References and exact States
that matched, rather than the pattern itself.

```text
global:user_*
      │
      ▼
concrete References
      │
      ▼
exact States consumed
```

Dynamic References may also specify minimum and maximum match counts:

```text
STORE:KEY[:MIN[:MAX]]
```

| Reference           | Meaning                                                          |
| ------------------- | ---------------------------------------------------------------- |
| `global:!`          | Wait for any key, then resolve all available matches.            |
| `global:user_*`     | Wait for at least one match, then resolve all available matches. |
| `global:user_*:5`   | Wait for five matches and resolve five.                          |
| `global:user_*:5:3` | Wait for five matches and resolve three.                         |
| `global:user_*:5:*` | Wait for five matches and resolve all available matches.         |

The Trace records which concrete Reference versions were actually consumed.

---
## Dependencies and execution readiness

In native execution, References also act as Ocean dependencies.

An execution that requires:

```text
global:start_value
```

cannot consume it until that Reference is available in the referenced Store.

Ocean reacts as References become available, are updated, or are deleted while
determining what can execute.

That scheduling behaviour belongs to Ocean rather than to Reference identity
itself.

See [`Ocean_Rundown.md`](Ocean_Rundown.md).

---
## Effects

References may also be changed through runtime Effects such as writes or
deletes.

The Reference identifies the affected runtime data; the Effect records the
operation that occurred.

See [`Effects.md`](Effects.md).

---
## `DataRef`

References are usually written as strings:

```python
"global:start_value"
```

Lower-level APIs may use the structured `DataRef` form:

```python
from tide_tooling import DataRef

ref = DataRef(
    "start_value",
    "global",
)
```

`DataRef` also carries information required by dynamic References, including
match counts.

While using  `args` or `kwargs` with `TIDE.exec()`, References require `@` to be distinguishable against ordinary string values. 

```python
tide.exec(
    use_answer,
    args=("@global:start_value", ),
    kwargs={"data": "@global:result"},
    default_store="cache",
)
```

The `@` marks the string as a Reference/dependency. It is not part
of Reference identity.   

For ordinary `TIDE.declare()`, `reads` and `writes` inputs are automatically treated as References. 

---
## Summary

```text
Store
  │
  └── Reference
        │
        ├── live runtime value
        │
        ├── State v0
        ├── State v1
        └── State v2
```

A **Store** tells TIDE where runtime data lives.

A **Reference** identifies which runtime data is being addressed.

A **version** identifies which recorded occurrence was used.

Together, these let TIDE preserve exact producer-consumer relationships as
data is read, written, reused, and resolved dynamically.
