# Changelog

## v0.0.3 (pre-alpha)

Preamble:
> v0.0.3 is a major general refactor of practically the whole of TIDE-Tooling. The main execution, Event, reconstruction, Trace, query, Model, comparison, adapter, Bundle and public facade systems were stripped down and rebuilt around clearer responsibilities. This changelog therefore focuses on the main user-visible changes rather than every internal replacement.

General Refactor:
- Refactored the full native TIDE execution path from Pipeline and Ocean execution through Events, ExecutionLog reconstruction and Trace creation.
- Added `list[Node]` as a direct `Ocean.run_pipeline()` input, equivalent to constructing a `Pipeline` from that list.
- Simplified the public `TIDE` facade and kept the core package at zero runtime dependencies.
- Made recorded evidence the authority throughout Trace reconstruction and querying. Missing information is no longer silently replaced with convenient inferred values.
- Normalised external or unobserved producers under `TIDE: External`.

Trace and Queries:
- Expanded and simplified `Trace` querying through `find`, `inspect`, `investigate`, `why`, `produced_by`, `consumed_by`, `upstream`, `downstream` and `path`.
- Added direct Execution input and output access through `inputs_of`, `output_of` and `inputs_outputs_of`.
- Added `definition_of` for inspecting the complete recorded Node declaration behind an Execution or exact definition identity.
- Added resolved and raw Execution value views.
- Added explicit undeclared input evidence, toggleable through `trace.show(show_undeclared=True)`.
- Added ordinary evidence and value comparison through `Trace.diff()`.
- Added causal corruption reporting through `Trace.corruption` and the Trace header.

TIDE Model and Comparison:
- Rebuilt Trace to TIDE Model projection around explicit Execution, State, Input and Output sets.
- Added structural, literal and explicitly supplied comparison mappings.
- Added `TIDEModelComparisonReport` and `TIDEModelValidityReport` with TIDE-styled `.show()` output.
- Added structural and semantic Model validation without preventing comparison solely because a Model is semantically invalid.
- Added deterministic canonicalisation for structurally comparing runs from different Pipelines.

Imports and noWorkflow:
- Added and consolidated noWorkflow, OpenLineage, OpenTelemetry and OpenInference adapters.
- Added `TIDE.now()` and `TIDE.from_now()` for capturing or importing noWorkflow trials.
- Added `NOWMeta` inspection and focused subtraces for exact noWorkflow Executions.
- noWorkflow Activations now represent execution context, Evaluations alone
  own captured value States and exact dependency edges, and direct attribution
  evidence retains Activation/Evaluation nesting.
- Added native `Trace.parents_of()` and `Trace.children_of()` execution
  queries with compact or input/output-aware rendering.
- Added an in-memory noWorkflow argument-binding compatibility patch without modifying the installed noWorkflow package.
- noWorkflow capture runs in a subprocess with package-scoped collection; existing projects can be projected separately without rerunning capture.

Bundles:
- Rebuilt Trace Bundles around a portable manifest, execution evidence, Reference/version indexes and content-addressed values.
- Added direct `Trace.to_bundle()` and `TIDE.from_bundle()` round trips.
- Added trusted and untrusted loading plus Bundle resource limits.
- Bundles retain native execution and causal corruption evidence, and can optionally retain Events and their payloads when `serialise_events=True`.
- Added optional Event-sidecar correspondence verification through `Trace.events_verified`; TideRun remains the Bundle reconstruction authority.
- Added dictionary-order-independent canonical Bundle hashing and optional explicit Ed25519 signing through `tide-tooling[signing]`.

Command Line:
- Added `tide now` for capturing scripts directly into Bundles.
- Added Bundle-backed Trace show and query commands.
- Added Bundle `diff` and TIDE Model `compare` commands.
- Existing Bundle output is protected unless `--force` is supplied.
- Added Bundle-backed `consumed-by` and `produced-by` relationship queries.

Effects and Runtime Evidence:
- Simplified declared and live Effect handling while retaining `READ`, `WRITE`, `DELETE`, `EMIT`, `RUN`, `END`, `RAISE`, `WRITE_NOW` and `DELETE_NOW`.
- Made normal writes and runtime updates visibly distinct in Trace output.
- Preserved duplicate reads, writes, Effects and emitted-node occurrences as recorded evidence.
- Made emitted-node ownership and Execution relationships directly inspectable.

Documentation and Tests:
- Rewrote the main README and documentation around the current pre-alpha API.
- Added broad native, query, comparison, adapter, Bundle, CLI, adversarial and weird-case coverage.
- Added a real noWorkflow IPAW workflow capture example.

Compatibility:
- TIDE-Tooling remains pre-alpha.
- v0.0.3 does not provide backwards compatibility for older pre-alpha Bundle formats.
