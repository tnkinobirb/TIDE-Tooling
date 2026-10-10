# v0.0.3 release notes

v0.0.3 is a broad pre-alpha refactor of TIDE-Tooling. The canonical,
maintained change list is [`../CHANGELOG.md`](../CHANGELOG.md).

The main user-visible areas are:

- rebuilt native Pipeline, Ocean, Event, reconstruction and Trace paths;
- expanded Trace queries, direct execution ancestry and evidence comparison;
- rebuilt TIDE Model projection, validation and run comparison;
- optional noWorkflow, OpenLineage, OpenTelemetry and OpenInference imports;
- portable TIDE Bundles, optional signing and Bundle-backed CLI queries;
- simplified Effect handling while preserving recorded duplicate occurrences;
- direct `list[Node]` support in `Ocean.run_pipeline()`; and
- rewritten documentation and broader regression coverage.

TIDE-Tooling remains pre-alpha. v0.0.3 does not promise compatibility with
older pre-alpha Bundle formats.
