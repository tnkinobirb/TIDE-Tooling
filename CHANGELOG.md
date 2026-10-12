# Changelog

## v0.0.4 (pre-alpha)

Preamble:
> v0.0.4 is a small update to TIDE-Tooling specifically fixing Store-poll Event handling semantics to correctly detect and version unobserved changes in stored values.

Store Polling:
- Adjusted the guard in `read_dependency_state()` to compare the polled stored-value hash with the last observed hash. If they differ, handling now falls through to create a new write record for the Reference/value pair.

Display:
- `Trace.show()` now hides Tide Run and Pipeline source metadata by default. Pass `show_sources=True`, or use `--sources` in the CLI, to display it.
