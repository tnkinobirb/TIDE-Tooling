# TIDE-Tooling

> **Status:** Pre-alpha prototype.
>
>  **Note:** *Documentation is work-in-progress and subject to change*

**Capture, inspect, and compare execution.**

TIDE-Tooling is a Python toolkit for execution provenance.

It turns supported execution evidence into a **Trace**: a queryable
representation of what executed, what data was observed, and how they were
related.

Use TIDE-Tooling when you need to understand how a result was produced, what happened upstream or downstream, or
how execution relationships changed across runs.

The core package has **zero runtime dependencies**. Optional integrations are
installed separately.

This README provides a quickstart overview of TIDE-Tooling. See
`docs/` for further detail.

Users of `noworkflow` capture should also read the current
[notes on noWorkflow integration](docs/Notes_on_noWorkflow.md), including
known upstream limitations and TIDE's narrow in-memory compatibility patch.

---
## Install

### Core

```bash
git clone https://github.com/tnkinobirb/TIDE-Tooling.git
cd TIDE-Tooling
python -m pip install .
```

### With noWorkflow

```bash
python -m pip install ".[now]"
```

### With pytest

```bash
python -m pip install ".[test]"
```

### All optional dependencies

```bash
python -m pip install ".[now,test,signing]"
```

---
## Quickstart

The quickest route to a Trace is to run an existing Python script with the
optional noWorkflow integration:

```bash
# Capture a Python script as a TIDE Bundle
tide now workflow.py -o workflow.tide.json

# Pass arguments to the captured script
tide now workflow.py -o workflow.tide.json -- arg1 arg2
```
*For an overview of commands, please see [cli-commands](docs/cli_commands.md)*

You can also capture execution directly within Python script.

with `now`:
```python
from pathlib import Path
from tide_tooling import TIDE

trace = TIDE.now(Path("path/to/script.py"))
trace.show()
```
with native `TIDE`:
```python
from tide_tooling import TIDE

def add_one(n):
    return n + 1

tide = TIDE()
tide.declare("start_value", 42)
tide.exec(add_one, reads=["start_value"], writes=["end_value"])

trace = tide.trace()
trace.show()
```
Native TIDE uses **[Stores and References](docs/Stores_and_References.md)** to identify and
represent runtime data.

---
## Choose your workflow | Create a Trace

### Native TIDE

Use native TIDE when you want to capture execution explicitly through the
zero-dependency core.

```python
from tide_tooling import TIDE

def add_one(n):
    return n + 1

tide = TIDE()
tide.declare("start_value", 42)
tide.exec(add_one, reads=["start_value"], writes=["end_value"])

trace = tide.trace()
```

Native TIDE is built around **Stores** and **References**:

- A **Store** holds values.
- A **Reference** identifies a location in a Store.

For example:

```text
Store:      global
Reference:  global:start_value
```

The names supplied to `declare()`, `reads`, and `writes` identify References.
Values from `reads` are supplied to the callable as positional inputs, and
outputs are written to the References named in `writes`.

```text
global:start_value
        │
        ▼
     add_one
        │
        ▼
global:end_value
```

For Store, Reference, and version semantics, see
[`docs/Stores_and_References.md`](docs/Stores_and_References.md).

Native execution is handled by Ocean; see
[`docs/Ocean_Rundown.md`](docs/Ocean_Rundown.md).

For runtime Effects, see [`docs/Effects.md`](docs/Effects.md).

### noWorkflow

Use the optional noWorkflow integration when you want automatic, fine-grained
capture of Python execution without instrumenting the script with TIDE.

`TIDE.now()` runs and captures a script directly. `TIDE.from_now()` projects
an existing noWorkflow project or Trial into a Trace.

```python
from pathlib import Path
from tide_tooling import TIDE

trace = TIDE.now(
    Path("path/to/script.py"),
    project_dir=Path("path/to/capture-project"),
)
```

`TIDE.now()` launches noWorkflow then projects the recorded trial into a Trace. `project_dir` is
noWorkflow capture storage location.

*`TIDE.now()` defaults to using `now ... -c main` collection*

To import an existing capture:

```python
trace = TIDE.from_now(Path("path/to/capture-project"))
```

See [`docs/Imports.md`](docs/Imports.md) for the projection mapping and
[`docs/Notes_on_noWorkflow.md`](docs/Notes_on_noWorkflow.md) for current
capture limitations and compatibility scope.

### Existing evidence

TIDE-Tooling also provides experimental import support for other execution
formats.

#### OpenLineage

```python
import json
from pathlib import Path
from tide_tooling import TIDE

events = json.loads(Path("path/to/openlineage.json").read_text())
trace = TIDE.from_openlineage(events)
```

#### OpenInference

```python
import json
from pathlib import Path
from tide_tooling import TIDE

spans = json.loads(Path("path/to/openinference.json").read_text())
trace = TIDE.from_openinference(spans)
```

#### OpenTelemetry

```python
import json
from pathlib import Path
from tide_tooling import TIDE

spans = json.loads(Path("path/to/opentelemetry.json").read_text())
trace = TIDE.from_opentelemetry(spans)
```

#### TIDE Bundle

```python
from pathlib import Path
from tide_tooling import TIDE

trace = TIDE.from_bundle(Path("path/to/tide_bundle.json"))
```
#### TIDE Model

```python
import json
from pathlib import Path
from tide_tooling import TIDE

model = json.loads(Path("path/to/tide_model.json").read_text())
trace = TIDE.from_model(model)
```

Each Trace can be inspected with the same queries.

For more on imports, adapters and accepted inputs, see [`docs/Imports.md`](docs/Imports.md).

---
## Inspect a Trace

The same query interface is available regardless of how the Trace was created.

### See the Trace

```python
trace.show()
```

Displays the recorded execution history and the identities available to other
queries.

### Investigate one object

```python
trace.investigate("id").show()
```

Shows the selected object's provenance and surrounding execution history.

### Follow upstream

```python
trace.upstream("id").show()
```

Shows what led to an execution or recorded data occurrence.

### Follow downstream

```python
trace.downstream("id").show()
```

Shows what followed from an execution or recorded data occurrence.

### Follow direct execution ancestry

```python
trace.parents_of("execution_id").show()
trace.children_of("execution_id").show()
```

Shows child work directly attributed to a parent Execution. In v0.0.3 this
relationship is stored as `emitted_by_exec_id`; it records direct parentage and
does not require a native TIDE node-emission mechanism.

### Find what consumed something

```python
trace.consumed_by("id").show()
```

For a State, shows the Executions that consumed it. For an Execution, shows
the States it consumed.

### Find what produced something

```python
trace.produced_by("id").show()
```

For a State, shows the Execution that produced it. For an Execution, shows
the States it produced.

### Find a path

```python
trace.path("root", "target", direction="downstream").show()
```

Finds execution paths between two Executions, States, or a mixture of both.

### Find objects

```python
trace.find("name_or_partial_id")
```

Finds recorded objects by name, partial identity, Reference, execution
identity, or recorded value hash.

### Inspect runtime values

```python
trace.value_of("reference")
```

Returns recorded values for a Reference.

To select a specific version:

```python
trace.value_of("reference", version=1)
```

`value_of()` returns the value itself rather than a query-result object. Use
`show=True` when you also want it displayed:

```python
trace.value_of("global:end_value", show=True)
```

### Compare recorded objects

```python
trace.diff("left", "right").show()
```

Compares recorded objects, values, References, Effects, or Trace-level
evidence.

For query identifiers, object kinds, versions, and disambiguation, see
[`docs/Queries.md`](docs/Queries.md).

---
## Command line

Capture a Python script directly into a Bundle:

```bash
tide now workflow.py -o workflow.tide.json
```

Existing output is preserved unless `--force` is supplied. Script arguments
follow `--`.

Run Trace queries against a Bundle:

```bash
tide workflow.tide.json show
tide workflow.tide.json why clean --kind actor
tide workflow.tide.json upstream clean --kind actor
```

Diff concrete evidence or compare represented execution relationships:

```bash
tide first.json diff second.json
tide first.json compare second.json --mapping structural
tide first.json compare second.json --mapping literal --full
```

Explicit supplied mappings remain available through the Python API.

---
## Export and compare

### Compare Traces

`Trace.compare()` compares the execution relationships represented by two
Traces through the TIDE Model.

Three mapping modes are available:

- `literal` — use the identities already present when those identities should correspond.
- `structural` — compare the structure of runs when runtime identifiers differ.
- `supplied` — use an explicit correspondence between Executions and States.

```python
comparison = trace_0.compare(
    trace_1,
    mapping="structural",
)
comparison.show()
```

For supplied correspondence and comparator semantics, mapping behaviour,
conformance, and report fields, see
[`docs/Comparison.md`](docs/Comparison.md).

For ordinary object, value, Reference, Effect, or whole-Trace differences,
use `trace.diff(...)` instead.

### Export a Bundle

A **TIDE Bundle** preserves richer Trace information and runtime values where
possible into serialisable json.

```python
from pathlib import Path

trace.to_bundle(
    path=Path("bundle_export.json"),
)
```

A Bundle can be loaded back into a Trace:

```python
trace = TIDE.from_bundle(Path("bundle_export.json"))
```

Bundles can optionally be authenticated with an explicit Ed25519 key pair:

```python
keys = TIDE.generate_signing_keys()
bundle = trace.to_bundle(sign=keys.private_key)
restored = TIDE.from_bundle(bundle, verify=keys.public_key)
```

Install signing support with `pip install tide-tooling[signing]`. Unsigned
Bundle creation and loading do not import or require that optional dependency.

For Bundle contents, value serialization, and reconstruction behaviour, see
[`docs/Bundles.md`](docs/Bundles.md).

### Export a Model

A **TIDE Model** is an intentionally reduced representation of a Trace's
execution relationships, used for TIDE comparison and interoperability.

Its core contains:

```text
E = Executions
S = States
I = Inputs
O = Outputs
```

```python
model = trace.to_model()
```

A Model may carry enrichment, but it is not intended to be a lossless
serialization of a full TIDE Trace.

For the TIDE Model, its execution relationship structure, and enrichment, see
[`docs/TIDE_Model.md`](docs/TIDE_Model.md).

---
## Documentation

For deeper detail:

- Terminal/cli commands → [`cli_commands.md`](docs/cli_commands.md)
- Recording and representing runtime data → [`docs/Stores_and_References.md`](docs/Stores_and_References.md)
- Querying Traces → [`docs/Queries.md`](docs/Queries.md)
- Importing execution evidence → [`docs/Imports.md`](docs/Imports.md)
- noWorkflow capture notes → [`docs/Notes_on_noWorkflow.md`](docs/Notes_on_noWorkflow.md)
- Comparing runs → [`docs/Comparison.md`](docs/Comparison.md)
- Runtime Effects → [`docs/Effects.md`](docs/Effects.md)
- Native execution and scheduling → [`docs/Ocean_Rundown.md`](docs/Ocean_Rundown.md)
- TIDE Models → [`docs/TIDE_Model.md`](docs/TIDE_Model.md)
- TIDE Bundles → [`docs/Bundles.md`](docs/Bundles.md)
- v0.0.3 changes → [`CHANGELOG.md`](CHANGELOG.md)
