# TIDE Model

**The TIDE Model is a small mathematical representation of exact execution relationships.**

```text
K = (E, S, I, O)

I ⊆ E × S
O ⊆ E × S
```

where:

```text
E → Execution occurrences
S → State occurrences
I → Execution consumed State
O → Execution produced State
```

An Execution or State may be represented in `E` or `S` without appearing in `I` or `O`.

---
## Reading the Model

```text
load[0] → raw[v0] → clean[0] → clean_data[v0]
```

can be represented as:

```python

# Execution and State occurrence identities
E = {
    "load[0]",
    "clean[0]",
}

S = {
    "raw[v0]",
    "clean_data[v0]",
}

# (Execution x State) input/output incidence

I = {
    ("clean[0]", "raw[v0]"),
}

O = {
    ("load[0]", "raw[v0]"),
    ("clean[0]", "clean_data[v0]"),
}
```

```text
I: Execution ← State
O: Execution → State
```

Together they determine producer-consumer relationships.

---
## Direct handoffs and reachability

If one Execution produces a State and another consumes it:

```text
load[0] → raw[v0]
clean[0] ← raw[v0]
```

TIDE derives:

```text
load[0] --raw[v0]--> clean[0]
```

From direct handoffs, TIDE can also derive transitive Execution reachability.

These are judgments derived from `I` and `O`; they are not additional Model
relations.

---
## Semantic Validity 

TIDE Semantic validity is a property of a TIDE Model that meets certain criteria.

Semantic validity is not required to derive judgments or compare represented execution relationships; it determines whether those judgments can be interpreted without ambiguity.

A TIDE Model is semantically valid when:

```text
1. every represented State has exactly one represented producer
2. Execution ancestry is acyclic
```

Formally:

```text
∀s ∈ S, ∃! e : (e, s) ∈ O
```

and no Execution may reach itself through producer-consumer ancestry.

For example, a cycle is ambiguous when determining an ancestor.

```
step[0] → state_a[v0] → step[1] → state_b[v0] → step[0]
```

Representing a state with more than one producer is also ambiguous when determining who exactly is responsible for its creation.  

A State with no represented producer in `O` is not necessarily producerless in reality; its production is simply absent from the represented evidence. Its producer answer within that evidence remains determinate—the empty set—and its execution relationships remain comparable. The structure is semantically invalid because the missing production boundary may permit a later Execution to be attributed as producing that State even when it did not.

---
## Trace ↔ Model

A Trace can be projected to the Model surface:

```python
model = trace.to_model()
```

and a Model document can be projected into an inspectable Trace:

```python
trace = TIDE.from_model(model)
```

Model projection contains the Model evidence only. Recorded Events and
Bundle-specific causal corruption evidence do not belong to the Model surface.

When the imported Model contains a producerless State, TIDE represents the
missing producer boundary in the projected Trace as:

```text
TIDE: External → State
```

`TIDE: External` does not identify the real producer. It means that the
producer is outside or unknown to the represented evidence.

The original Model remains unchanged as source evidence; the external boundary
belongs to the Trace projection.

---
## Model validation

```python
report = TIDE.report_model_validity(model)
report.show()
```

Useful fields include:

```python
report.semantically_valid
report.direct_handoffs
report.reachability
report["errors"]
```

Tooling also reports document-level structural validity separately from Model
semantic validity.

---
## Identity views

Literal identities are preserved by default:

```python
trace.to_model(identity_mode="literal")
```

A deterministic structural identity view can also be requested:

```python
trace.to_model(identity_mode="structural")
```

Structural identity changes labels used for comparison, not relationship
semantics.

See [`Comparison.md`](Comparison.md).

---
## Summary

```text
Mathematical Model
K = (E, S, I, O)

E → represented Execution occurrences
S → represented State occurrences
I → Execution consumed State
O → Execution produced State

Semantic Validity
→ every represented State has exactly one producer
→ Execution ancestry is acyclic

Model import
→ producerless States become TIDE: External boundaries in the Trace
→ source Model remains unchanged
```

The mathematical Model is the relationship contract. For more information on how the model was derived and the semantics around it, please reference the [TIDE Model Paper](https://zenodo.org/records/22116437).
