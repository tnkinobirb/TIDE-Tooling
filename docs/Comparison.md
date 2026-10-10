# Comparison

**TIDE supports two different kinds of comparison: evidence diffing and execution-relationship comparison.**

```text
Trace.diff()
→ compare recorded Trace objects or evidence

Trace.compare()
→ compare TIDE execution relationships
```

They answer different questions.

```text
diff
"What recorded things changed?"

compare
"What execution relationships changed?"
```

---
## `diff()`

Use `diff()` when you want to compare recorded Trace information or two
specific recorded objects.

```python
trace.diff(other_trace).show()
```

It can also compare values or typed Trace objects:

```python
trace.diff(
    "global:before",
    "global:after",
    "reference",
    "reference",
).show()
```

`diff()` is the concrete evidence view.

It is useful when the identities and recorded details themselves are the thing
you want to compare.

---
## `compare()`

Use `compare()` when you want to compare the execution relationships represented
by two Traces or TIDE Models.

```python
comparison = trace.compare(other_trace)

comparison.show()
```

Comparison operates on the TIDE Model relationship surface:

```text
K = (E, S, I, O)

E = Execution identities
S = State identities
I = Execution consumed State
O = Execution produced State
```

Conceptually:

```text
Trace A                     Trace B
   │                           │
   ▼                           ▼
TIDE Model                 TIDE Model
   │                           │
   └──────── compare ──────────┘
                │
                ▼
       relationship report
```

The richer Trace evidence is not the comparison surface here. `compare()`
only compares represented Execution-State relationships.

For the Model itself, see [`TIDE_Model.md`](TIDE_Model.md).

---
## Mapping modes

Comparison needs a rule for deciding which Execution and State identities on
one side correspond to identities on the other.

TIDE exposes three modes:

```text
structural
literal
supplied
```

They answer different questions.

| Mapping      | Question                                                                                         |
| ------------ | ------------------------------------------------------------------------------------------------ |
| `structural` | Do these Models have the same execution relationship structure, ignoring literal identity names? |
| `literal`    | Do these Models correspond under the identities exactly as recorded?                             |
| `supplied`   | Do these selected relationships correspond under the identity mapping I supplied?                |

--
## Structural comparison

Structural comparison is the default:

```python
trace.compare(
    other_trace,
    mapping="structural",
).show()
```

TIDE canonicalises both Models before comparing them.

For example, these may structurally correspond:

```text
Trace A

load[0]
   │
   ▼
raw[v0]
   │
   ▼
clean[0]
```

```text
Trace B

fetch[0]
   │
   ▼
input[v0]
   │
   ▼
normalise[0]
```

The literal names differ, but the represented producer-consumer structure may
be the same.

Structural comparison therefore asks:

```text
same relationship structure?
```

It does **not** assert that differently named host objects are the same
real-world object.

It only gives them a deterministic structural comparison identity via the
[*1-WL / colour refinement*](https://en.wikipedia.org/wiki/Colour_refinement_algorithm)
algorithm.

---
## Literal comparison

Literal comparison preserves the identities already present in each Model:

```python
trace.compare(
    other_trace,
    mapping="literal",
).show()
```

This asks:

```text
same relationships
under the exact recorded identities?
```

For example:

```text
source:
clean[0] → global:clean_data[v0]

target:
new_clean[0] → global:clean_data[v0]
```

may have the same shape but still differ literally because:

```text
clean[0] != new_clean[0]
```

Literal comparison is therefore useful when identity itself is meaningful.

---

## Supplied comparison

Use `mapping="supplied"` when you already know which identities should
correspond.

```python
comparison = trace.compare(
    other_trace,
    mapping="supplied",
    execution_map={
        "clean[0]": "new_clean[0]",
    },
    state_map={
        "global:raw_data[v0]": "global:raw_data[v0]",
        "global:clean_data[v0]": "global:clean_data[v0]",
    },
)

comparison.show()
```

The maps are explicit one-to-one correspondences:

```text
source Execution → target Execution
source State     → target State
```

TIDE does not infer, repair, or extend supplied maps.

This makes supplied comparison useful for a deliberately scoped question such
as:

```text
"Under this correspondence, do these selected relationships agree?"
```

---

## Partial supplied mappings

A supplied mapping does not need to cover the entire Model.

For example:

```text
Source
A → x → B → y → C

Target
P → X → Q → Y → R
```

A caller may choose to compare only:

```text
A ↔ P
x ↔ X
B ↔ Q
```

Relationships outside that mapping remain visible as:

```text
OUTSIDE MAPPING SCOPE
```

They are not automatically treated as disagreements.

This distinction matters:

```text
outside mapping scope
→ the identity exists, but no correspondence was asserted for it

mapped identity absent
→ the mapping refers to an identity that is not present on that side
```

---

## Reading the comparison report

A comparison report looks like:

```text
TIDE MODEL COMPARE | COMPLETE AGREEMENT | 8E/8S → 8E/8S
mapping: structural

SUMMARY:
	[...]
	mapped incidence: corresponds
	preservation: I yes | O yes
	reflection: I yes | O yes
	judgments: correspond
	identity coverage: source complete | target complete
	complete agreement: yes
	
	MAPPED IDENTITIES ABSENT:
	  SOURCE:
	    Executions:
	      none
	    States:
	      none
	  TARGET:
	    Executions:
	      none
	    States:
	      none
	      
	MAPPED INCIDENCE:
	  I_minus:
	    none
	  I_plus:
	    none
	  O_minus:
	    none
	  O_plus:
	    none
	
```

This describes agreement across execution relationships.

The below example describes disagreement:
```
TIDE MODEL COMPARE | NON-CONFORMANT | 8E/8S → 8E/8S
mapping: literal 

SUMMARY:
	[...]
	mapped incidence: differs
	preservation: I no | O no
	reflection: I no | O no
	judgments: differ
	identity coverage: source partial | target partial
	complete agreement: no
	
	MAPPED IDENTITIES ABSENT:
	  SOURCE:
	    Executions:
	      ! new_clean[0]
	      ! old_process[0]
	    States:
	      none
	  TARGET:
	    Executions:
	      ! clean[0]
	      ! process[0]
	    States:
	      none	
	
	MAPPED INCIDENCE:
	  I_minus:
	    - clean[0] ← global:raw_data[v0]
	    - process[0] ← global:clean_data[v0]
	  O_minus:
	    - clean[0] → global:clean_data[v0]
	    - process[0] → global:processed_data[v0]
	  I_plus:
	    + new_clean[0] ← global:raw_data[v0]
	    + old_process[0] ← global:clean_data[v0]
	  O_plus:
	    + new_clean[0] → global:clean_data[v0]
	    + old_process[0] → global:processed_data[v0]
	
	
```
 
Below are the relevant names and definitions needed for interpreting the report: 
### Comparison Judgments
| Judgment             | Meaning                                                                      |
| -------------------- | ---------------------------------------------------------------------------- |
| `COMPLETE AGREEMENT` | mapped incidence corresponds and identity coverage is complete on both sides |
| `CONFORMANT`         | mapped incidence corresponds, but identity coverage is not complete          |
| `NON-CONFORMANT`     | mapped input or output incidence differs                                     |
| `UNASSESSABLE`       | the comparison cannot be evaluated under the supplied mapping                |
## Incidence Comparison
| Field              | Meaning                                       |
| ------------------ | --------------------------------------------- |
| `I_minus`          | source input missing from target              |
| `I_plus`           | target input missing from source              |
| `O_minus`          | source output missing from target             |
| `O_plus`           | target output missing from source             |
| `preservation`     | whether source relationships appear in target |
| `reflection`       | whether target relationships appear in source |
| `mapped incidence` | whether mapped `I` and `O` correspond overall |
## Mapping scope
| Field                      | Meaning                                                                           |
| -------------------------- | --------------------------------------------------------------------------------- |
| `identity coverage`        | whether all represented Executions and States are covered by the mapping          |
| `MAPPED IDENTITIES ABSENT` | mapped identities that are not present in the relevant Model                      |
| `OUTSIDE MAPPING SCOPE`    | represented identities and relationships for which no correspondence was asserted |

For example, this summary:

```
TIDE MODEL COMPARE | CONFORMANT | 8E/8S → 8E/8S
mapping: supplied 

SUMMARY:
  [...]
  mapped incidence: corresponds
  preservation: I yes | O yes
  reflection: I yes | O yes
  judgments: correspond
  identity coverage: source partial | target partial
  complete agreement: no
```

means the mapped relationships agree in both directions, but some identities lie outside the mapping, so the result is `CONFORMANT` rather than `COMPLETE AGREEMENT`.

Model validity is reported alongside comparison but remains a separate question; see `TIDE_Model.md`.

---
## Summary

```text
diff()
→ what recorded evidence changed?

compare(mapping="structural")
→ do the relationship structures correspond?

compare(mapping="literal")
→ do the relationships correspond under recorded identities?

compare(mapping="supplied")
→ do the mapped relationships correspond under my supplied identities?
```

Comparison is therefore about **correspondence between execution
relationships**, with the identity rule made explicit.
