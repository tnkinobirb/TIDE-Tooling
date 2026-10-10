# TIDE-Tooling

> **Status:** Pre-alpha prototype.

TIDE-Tooling is an experimental Python execution and inspection toolkit.

It currently has two main parts:

- **TIDE** — records and inspects execution through a **Trace**.
- **Ocean** — runs work whose dependencies and execution structure can change at runtime.

If you are new to the project, the shortest route is:

```text
declare work
    ↓
Ocean runs it
    ↓
Trace
    ↓
TIDE inspects it
```

```console
git clone https://github.com/tnkinobirb/TIDE-Tooling.git
cd TIDE-Tooling
python -m pip install .
```

This README describes the implemented prototype and its current public interfaces.

### Example

```python
from tide_tooling import TIDE, Trace

def pipeline() -> Trace:
    tide = TIDE()

    # Declare and persist a value under a Reference.
    tide.declare("declared", "START: I am now data declared as a Reference")

    # Execute a function through Ocean via TIDE.
    tide.exec(print, reads=["declared"], writes=["print_done"])

    # Record an external or non-computational event.
    tide.event(
        "human_verifier",
        reads=["print_done"],
        writes={"human_verification_done": True},
    )

    # Produce an inspectable Trace.
    return tide.trace()

trace = pipeline()
trace.show()
```

The complete example includes exploratory queries and advanced Reference notes:

[`demos/start_here/shortest_route.py`](demos/start_here/shortest_route.py)

```console
python demos/start_here/shortest_route.py
```

Usage of TIDE-Tooling requires adherence to [LICENCE.md](LICENCE.md). 
For general usage or licensing questions, contact `tidetooling@gmail.com`.

### Example index

The README introduces the project in stages. These are the complete executable examples; run producer/importer pairs in the listed order.

| Area | Complete example | Run |
| --- | --- | --- |
| Start here | [`demos/start_here/shortest_route.py`](demos/start_here/shortest_route.py) | `python demos/start_here/shortest_route.py` |
| Explicit TIDE | [`demos/readme/explicit_tide.py`](demos/readme/explicit_tide.py) | `python demos/readme/explicit_tide.py` |
| Passive values | [`demos/readme/passive_tide.py`](demos/readme/passive_tide.py) | `python demos/readme/passive_tide.py` |
| Execution relationships | [`demos/readme/exec_relationships.py`](demos/readme/exec_relationships.py) | `python demos/readme/exec_relationships.py` |
| Effects | [`demos/effects/effects_demo.py`](demos/effects/effects_demo.py) | `python demos/effects/effects_demo.py` |
| Emission | [`demos/effects/emission_demo.py`](demos/effects/emission_demo.py) | `python demos/effects/emission_demo.py` |
| TIDE Model import | [`demos/model/from_model.py`](demos/model/from_model.py) | `python demos/model/from_model.py` |
| Basic Trace and export | [`demos/trace/basic_trace.py`](demos/trace/basic_trace.py) | `python demos/trace/basic_trace.py` |
| Basic Trace import | [`demos/trace/from_bundle.py`](demos/trace/from_bundle.py) | `python demos/trace/from_bundle.py` |
| Explicit Ocean | [`demos/readme/explicit_ocean.py`](demos/readme/explicit_ocean.py) | `python demos/readme/explicit_ocean.py` |
| Implicit Ocean links | [`demos/readme/implicit_ocean.py`](demos/readme/implicit_ocean.py) | `python demos/readme/implicit_ocean.py` |
| Stores and templates | [`demos/readme/register_stores.py`](demos/readme/register_stores.py) | `python demos/readme/register_stores.py` |
| Pipeline and export | [`demos/pipeline/data_pipeline.py`](demos/pipeline/data_pipeline.py) | `python demos/pipeline/data_pipeline.py` |
| Pipeline bundle import | [`demos/pipeline/from_bundle.py`](demos/pipeline/from_bundle.py) | `python demos/pipeline/from_bundle.py` |
| Bug captured four ways | [`demos/bug/bug_pipeline.py`](demos/bug/bug_pipeline.py) | `python demos/bug/bug_pipeline.py` |
| Bug bundle import | [`demos/bug/from_bundle.py`](demos/bug/from_bundle.py) | `python demos/bug/from_bundle.py` |

---

## TIDE

**TIDE is the primary execution-recording and inspection interface.** It provides the shortest route into TIDE-Tooling.

TIDE can:

- declare and persist supplied Values under References with `tide.declare()`;
- execute and record Python work with `tide.exec()`;
- record external or non-computational evidence with `tide.event()`;
- produce an inspectable execution history with `tide.trace()`.

The result is a **Trace**, which can be viewed, searched, inspected, queried for execution relationships, compared, or exported. For most users, TIDE is the recommended starting point.

### Trace

A **Trace** represents recorded execution history. It is the main object used to inspect, query, and compare that history.

TIDE can record or import execution evidence and create a Trace from it.

### Execution

An **Execution** is one occurrence of work that may consume or produce data.

A callable or external source may perform or represent an Execution.

---

### Recording Execution

TIDE provides a few direct interfaces for building execution history.

`tide.event()` records supplied execution evidence.

`tide.trace()` creates a Trace from the execution recorded by that TIDE instance.

For example:

```python
from tide_tooling import TIDE

tide = TIDE()

tide.event(
    "load",
    writes={"raw_data": " HELLO "}
)

tide.event(
    "clean",
    reads=["raw_data"],
    writes={"clean_data": "hello"}
)

trace = tide.trace()
trace.show()
```

TIDE can also execute and record work directly.

`tide.declare()` registers a supplied Value under a Reference.

`tide.exec()` executes a callable and records the resulting Execution.

For example:

```python
from tide_tooling import TIDE

def clean(data):
    return data.strip().lower()

tide = TIDE()

tide.declare("raw_data", " HELLO ")

tide.exec(
    clean,
    reads=["raw_data"],
    writes=["clean_data"]
)

trace = tide.trace()
trace.show()
```

The resulting Trace contains the declaration of `raw_data`, the Execution of `clean`, its read of `raw_data`, and its write to `clean_data`.

Complete example: [`demos/readme/explicit_tide.py`](demos/readme/explicit_tide.py) (`python demos/readme/explicit_tide.py`)

---

### Inspecting a Trace

A Trace can first be viewed directly:

```python
trace.show()
```

If you do not know an exact recorded identifier, it can be searched:

```python
trace.find("clean").show()
```

A recorded Reference can also be used to retrieve its Value:

```python
trace.value_of("global:clean_data", show=True)
```

TIDE can inspect and investigate recorded:

- Executions;
- Actors;
- References;
- Reference Versions;
- Values;
- Effects.

An **Actor** identifies the work or source associated with an Execution.

One Actor may be associated with many Executions.

For deeper inspection:

```python
trace.investigate(id, type).show()
```

`trace.show()` and `trace.find()` surface recorded identifiers that can be used for inspection. More specific identifier forms and query behaviour can be learned as needed rather than up front.

Complete example: [`demos/trace/basic_trace.py`](demos/trace/basic_trace.py) (`python demos/trace/basic_trace.py`)

---

### Execution Relationships

A **Reference Version** identifies one recorded state of a Reference.

For example:

```text
data[v0] | version 0
data[v1] | version 1
```

represent two distinct recorded versions of `data`.

A Reference may also be associated with the same Value more than once.

This does not mean the recorded states are the same:

```text
data[v0] = "info"
data[v1] = "info"
```

but:

```text
data[v0] != data[v1]
```

Because reads and writes retain their recorded Reference Versions, TIDE can connect a produced state to Executions which consumed that same state.

For example:

```text
load
  │
  └── raw_data[v0]
          │
          └── clean
```

means `load` produced `raw_data[v0]` and `clean` consumed that same recorded version.

TIDE exposes queries for:

- produced-by / consumed-by;
- upstream / downstream;
- paths between recorded execution objects.

```python
from tide_tooling import TIDE

tide = TIDE()

def load():
    return "loaded_raw_data"

def clean(raw_data):
    return "clean_data"

tide.exec(load, writes="raw_data")
tide.exec(clean, reads=["raw_data"], writes="clean_out")
trace = tide.trace()

# Produced/Consumed by
trace.produced_by("load", "actor").show()
trace.consumed_by("clean", "actor").show()

# Upstream/Downstream
trace.upstream("clean", "actor").show()
trace.downstream("load", "actor").show()

# Path downstream
trace.path(
    source="load",
    target="clean",
    source_type="actor",
    target_type="actor",
).show()

# Path upstream
trace.path(
    source="clean",
    target="load",
    source_type="actor",
    target_type="actor",
    direction="upstream",
).show()

```

`trace.path()` follows the recorded directed relationships. It does not select
a shortest path by default. A query from an object to itself is allowed when a
recorded relationship chain returns to that object; the query does not itself
decide whether the represented execution structure is semantically valid.

Complete example: [`demos/readme/exec_relationships.py`](demos/readme/exec_relationships.py) (`python demos/readme/exec_relationships.py`)


This keeps the Reference Version which connects the Executions visible rather than reducing the relationship to only:

```text
load → clean
```

---

### Effects

An **Effect** is a realised operation associated with an Execution. Effects may
be declared on a Node or returned while that Node executes.

`ExecRecord.effects` preserves realised effect occurrences as an ordered list:

- every realised target produces one effect record;
- equal effects are not deduplicated;
- declared effects occur in declaration order;
- returned runtime effects follow in occurrence order;
- `origin` records whether an occurrence was `declared` or `runtime`;
- `output` records the realised effect value;
- READ and DELETE records include `target_existed` when that runtime evidence
  is available.

Normal-result writes occur before declared and returned runtime WRITE effects.
Filtering the resulting writes for one target gives that target's recorded
version trajectory.

Effects can be searched and inspected through the same Trace interface:

```python
from tide_tooling import Effects

trace.find(Effects.WRITE).show(type="effect")
trace.investigate(Effects.WRITE, "effect").show()
```

Complete example: [`demos/effects/effects_demo.py`](demos/effects/effects_demo.py) (`python demos/effects/effects_demo.py`)

---

### Emission

Emission records one relationship:

```text
emitter → emitted Node
```

Every emitted list item is retained as an ordered occurrence. Declared
emissions precede returned runtime emissions. Repeated emission of the same
Node preserves repeated relationship evidence without executing that DAG Node
more than once. The relationship participates in Trace history and path queries
whether or not the emitted Node later executes, and bundle restoration preserves
its order, duplicates, and origin.

Complete example: [`demos/effects/emission_demo.py`](demos/effects/emission_demo.py) (`python demos/effects/emission_demo.py`)

---

### Comparing Traces — *experimental*

A **Diff** compares recorded objects or Traces.

```python
trace.diff(other_trace).show()
```

By default, Trace identity is compared using the identities present in the Trace.

A structural comparison instead compares execution structure rather than literal identifiers:

```python
trace.diff(
    other_trace,
    identity_mode="structural"
).show()
```

Structural comparison does not assert that differently named host objects are automatically the same real-world object. It provides a structural view for comparison.

The Effects demo includes a Trace comparison alongside effect recording and bundle round trips: [`demos/effects/effects_demo.py`](demos/effects/effects_demo.py) (`python demos/effects/effects_demo.py`)

---

## Export — *experimental*

A Trace currently has two main outward paths:

```text
                Trace
                  │
          ┌───────┴───────┐
          │               │
        Bundle          TIDE Model
          │               │
 preserve Trace      preserve execution
 information         relationships
```

### Bundle — *experimental*

A **TIDE Bundle** is used to preserve and move Trace information.

```python
bundle = trace.to_bundle()
```

A Bundle preserves the execution evidence and Values required by the supported Trace reconstruction path.

A Trace can then be rebuilt from it:

```python
rebuilt_trace = TIDE.from_bundle(bundle)
```

The rebuilt Trace recreates its query indexes from the preserved execution records.

```text
Trace
  ↓
Bundle
  ↓
Trace
```

Bundle round-trip examples:

- Pipeline: [`demos/pipeline/data_pipeline.py`](demos/pipeline/data_pipeline.py) → [`demos/pipeline/from_bundle.py`](demos/pipeline/from_bundle.py)
- Basic Trace: [`demos/trace/basic_trace.py`](demos/trace/basic_trace.py) → [`demos/trace/from_bundle.py`](demos/trace/from_bundle.py)
- Bug investigation: [`demos/bug/bug_pipeline.py`](demos/bug/bug_pipeline.py) → [`demos/bug/from_bundle.py`](demos/bug/from_bundle.py)

Run each producer before its corresponding importer.

### TIDE Model

The **TIDE Model** is different from a Bundle.

A Bundle aims to preserve the richer TIDE Trace.

The TIDE Model intentionally reduces a Trace to the candidate structure
`K=(E,S,I,O)`: represented Execution identities `E`, represented State
identities `S`, input incidence `I`, and output incidence `O`. `E` and `S`
are authoritative; the tooling does not reconstruct them from incidence.
Occurrence identities are non-empty strings, matching the artifact's JSON
model and executable reference comparator.

```text
Trace
  ↓
TIDE Model

Execution ── State ──> Execution
```

A Trace can be projected to the Model:

```python
model = trace.to_model()
```

A Model can be validated:

```python
report = TIDE.validate_model(model)
assert report["semantically_valid"], report["errors"]
```

Structural validity checks that every incidence endpoint belongs to its
declared carrier. Semantic validity separately checks unique State creation
and acyclic Execution ancestry. Handoff and non-empty reachability judgments
remain determined and reportable when a structurally valid Model is
semantically invalid.

A semantically valid Model can also be projected into an inspectable Trace:

```python
model_trace = TIDE.from_model(model)
```

The TIDE Model does not attempt to preserve every piece of information held by a Trace.

Its purpose is to preserve a smaller shared execution-relationship structure with fixed validity and query rules.

Two Models can be compared under supplied one-to-one identity maps:

```python
from tide_tooling import comparator

comparison = comparator(
    source_model,
    target_model,
    execution_map={"source-exec": "target-exec"},
    state_map={"source-state": "target-state"},
)
```

`comparison.conformant` means mapped input and output incidence agrees.
Outside identities/incidence, mapped identities absent from a structure, and
semantic validity are reported separately. `comparison.complete_agreement`
requires conformance and exact identity coverage on both sides. The maps are
never inferred, repaired, or extended.

Complete import example: [`demos/model/from_model.py`](demos/model/from_model.py) (`python demos/model/from_model.py`)

Find out more in the [TIDE Model Artifact](https://github.com/tnkinobirb/TIDE-Model-Artifact).

---

## Ocean

**Ocean is the dynamic execution runtime.** It declares, schedules, and executes work.

### Declaration

A **Node** is a declared unit of work. It contains a function and may declare inputs, outputs, and write targets.

```python
from tide_tooling import node, Node

Node(func=print, args="PrintMe", kwargs=None, targets="PrintComplete") 

# or 

@node(args="PrintMe", targets="PrintComplete")
def custom_print(*args, **kwargs):
    return print(*args, **kwargs)

# Node also supports injection inputs for args or kwargs inputs that are functions.
# These are p_args, p_kwargs, k_args, k_kwargs
# Example:

AB = lambda a, b=None: a, b
C  = lambda c=None: c

def function(ab, c=None):
    return ab, c

Node(
    func=function,
    args=AB,
    kwargs=C,
    p_args="A",
    p_kwargs={"b": "B"},
    k_kwargs={"c": "C"},
    targets="function_out",
)

# or

@node(
    args=AB,
    kwargs=C,
    p_args="A",
    p_kwargs={"b": "B"},
    k_kwargs={"c": "C"},
    targets="function_out",
)
def function(ab, c=None):
    return ab, c

```

A **Pipeline** is a collection of declared Nodes. The graph representing a Pipeline is known as an **Intent** graph.

```python
from tide_tooling import pipeline, Pipeline

@node(args="PrintMe", targets="PrintOut")
def custom_print(*args, **kwargs):
    print(*args, **kwargs)
    return args

@node(args="@PrintOut", targets="PrintText")
def get_text(print_out):
    return print_out

@pipeline
def pipeline():
    get_text()      # Declaration order does not determine execution order.
    custom_print()  # Execution depends on dependency availability.
    
# Run with Ocean.

from tide_tooling import Ocean

ocean = Ocean()
trace = ocean.run_pipeline(pipeline)
trace.show()
trace.value_of("mem_0:PrintOut", show=True)
```


### Graphs

A **Runtime** graph starts from Intent and is used for tracking live execution; it may expand as runtime state changes.

It tracks which Nodes can execute as dependencies become satisfied or invalidated.

A **Graph** refers to a Directed Acyclic Graph (DAG).

A graph is represented as a dictionary mapping a Node to its dependencies.

For example:

```python
dag = {
    "node_0": [],
    "node_1": ["ref_0"]
}
```

means:

```text
"node_0" depends on nothing.
"node_1" depends on "ref_0".
```

`ref_0` is a Reference, not another Node.

### References, Stores and Dependencies

A **Reference**, or *Data Reference*, identifies data stored at a specified location.

A Reference's data may be:

- produced by Nodes;
- written externally to a Store.

A static Reference uses:

```text
STORE:KEY
```

- `STORE` is the Store name, such as `"mem"` or `"global"`.
- `KEY` is the exact identifier for data in that Store, such as `"csv_1"` or `"numpy_array_0"`.

Examples:

```text
mem:csv_1
global:numpy_array_0
```

```python
# In a read context:
unscoped_ref = "reference"
scoped_ref = "global:reference"

# In a raw argument context:
unscoped_arg = "@reference"
scoped_arg = "@global:reference"

# As a class
from tide_tooling import DataRef

ref = DataRef(key="reference", store="global")

```

References may also be *dynamic*:

```text
STORE:KEY:MIN:MAX
```
where:

- `KEY` = pattern-matched against available Store keys rather than requiring an exact match;
- `MIN` = minimum number of matches needed for the Reference to be satisfied;
- `MAX` = number of satisfied matches to resolve.

`MIN` defaults to `1`. `MAX` may be omitted or written as `*` to return every available match when the Node resolves. When both are numeric, `MAX` cannot exceed `MIN`.

Examples:

```text
mem:csv*        - match csv*; require at least one; resolve all
mem:*csv        - match *csv; require at least one; resolve all
mem:csv*:*:*    - match csv*; explicitly require at least one; resolve all
mem:csv*:5      - match csv*; require and resolve five
mem:csv*:5:1    - match csv*; require five; resolve one
```

```python
# In a read context:
unscoped_pattern = "reference*"
scoped_pattern = "global:reference*"
unscoped_counted = "reference:5:1"
scoped_counted = "global:reference:5:1"

# In a raw argument context:
unscoped_arg = "@reference*"
scoped_arg = "@global:reference*"

# As a class
from tide_tooling import DataRef

ref = DataRef(
    key="reference*",
    store="global",
    count=1,
    max=None,
)

```

A **Store** persists data under a Reference. During runtime, Store state may satisfy or invalidate References.

Stores may be registered with either scope:

- `"global"` — one Store referenceable across multiple Pipeline runs;
- `"local"` — one Store per run, referenceable within that run.

A Store can be registered directly through Ocean or through the Ocean owned by a TIDE instance:

```python
from tide_tooling import TIDE, Ocean

# Via TIDE context:
tide = TIDE()
tide.ocean.register_store(name="via_tide", scope="local")

# Direct Ocean:
ocean = Ocean()
ocean.register_store(name="via_ocean", scope="global")

# A local or global Store may be initialized with its own template.
store_template = {"key": "data"}
ocean.register_store(
    name="global_seeded",
    scope="global",
    store_template=store_template,
)
```

This registration template initializes the Store. Per-run `mem_store_templates` are deliberately narrower: they can only seed registered, runner-local `MemoryKVStore` stores and never overwrite global Stores.

```python
from tide_tooling import Ocean, node, pipeline

ocean = Ocean()
ocean.register_store(name="data_store", scope="local")

@node(args="@data_store:stored_data")
def print_stored_data(value):
    print(value)

@pipeline
def seeded_pipeline():
    print_stored_data()

ocean.run_pipeline(
    seeded_pipeline,
    mem_store_templates={
        "data_store": {"stored_data": "DATA"},
    },
)
```

Complete example: [`demos/readme/register_stores.py`](demos/readme/register_stores.py) (`python demos/readme/register_stores.py`)


A **Dependency** is a Data Reference declared as an input to a Node.

```python
from tide_tooling import TIDE

tide = TIDE()
tide.exec(print, reads=["reference_dependency"])

# "reference_dependency" is now a dependency required by print.

```

---

### Ocean Examples

#### Via TIDE — recommended / explicit References

```python
from tide_tooling import TIDE, Trace

def get_data(data_available_flag):
    return "data"

def report_data(data):
    return {"report": data}

def pipeline() -> Trace:
    tide = TIDE()

    # Declare and persist True as the data_available Reference.
    # References with no Store specified use the default Store.
    tide.declare("data_available", True)

    tide.exec(
        get_data,
        reads=["data_available"],
        writes=["raw_data"]
    )

    tide.exec(
        report_data,
        reads=["raw_data"],
        writes=["clean_data"]
    )

    return tide.trace()

trace = pipeline()
trace.show()
```

Complete example: [`demos/readme/explicit_tide.py`](demos/readme/explicit_tide.py) (`python demos/readme/explicit_tide.py`)

#### Via Ocean — recommended / explicit References

```python
from tide_tooling import Ocean, node, pipeline

ocean = Ocean()

@node(targets="got_data")
def get_data():
    return "data"

@node(
    targets=["report", "report_available_flag"],
    args=("@got_data",),
    unpack=True,  # split an output container across targets
)
def report_data(data):
    return {"report": data}, True

@pipeline
def pipeline():
    get_data()
    report_data()

trace = ocean.run_pipeline(pipeline)

trace.show()
trace.find("report").show()
trace.value_of("mem_0:report", show=True)
trace.value_of("mem_0:report_available_flag", show=True)
```

Complete example: [`demos/readme/explicit_ocean.py`](demos/readme/explicit_ocean.py) (`python demos/readme/explicit_ocean.py`)

#### Via TIDE — not recommended / passive Values

```python
from tide_tooling import TIDE, Trace

def get_data(data_available_flag):
    return "data"

def report_data(data):
    return {"report": data}

def pipeline() -> Trace:
    tide = TIDE()

    # data_available is never explicitly registered as a Reference.
    data_available = True

    # Ocean may create a Reference for passive data automatically.
    # This behaviour can be toggled off.
    tide.exec(
        get_data,
        args=data_available,
        writes=["got_data"]
    )

    tide.exec(
        report_data,
        reads=["got_data"],
        writes=["clean_data"]
    )

    return tide.trace()

trace = pipeline()
trace.show()
```

Complete example: [`demos/readme/passive_tide.py`](demos/readme/passive_tide.py) (`python demos/readme/passive_tide.py`)

#### Via Ocean — implicit

```python
from tide_tooling import Ocean, node, pipeline

ocean = Ocean()

@node
def get_data():
    return "data"

@node
def report_data(data):
    return {"report": data}

@pipeline
def pipeline():
    # Ocean creates an internal Reference to link the two Nodes.
    report_data(get_data())

trace = ocean.run_pipeline(pipeline)
trace.show()
```

Complete example: [`demos/readme/implicit_ocean.py`](demos/readme/implicit_ocean.py) (`python demos/readme/implicit_ocean.py`)

---

That should be enough to get started. Additional examples are available in [`./demos`](demos).

A good starting point is:

```console
python demos/start_here/shortest_route.py
```
