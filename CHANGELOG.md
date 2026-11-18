# Changelog

## v0.0.8 (pre-alpha)

Exception typing and facade:
- Added parent `TIDEError` to `tide_tooling.__init__` imports.
- All TIDE exceptions now use `TIDEError` as their base class.

Bundle validation:
- `TIDE.from_bundle()` invalid input errors now include the received type.

noWorkflow:
- Updated noWorkflow's required version to 2.2.0.
- Adjusted tests to expect a noWorkflow issue previously causing tests to fail (`#294`).

## v0.0.7 (pre-alpha)

Bundle reconstruction-integrity assertions: 
- Fixed a bug where `bundles` containing `Path` objects generated on one platform ['linux'|'windows'] and loaded on another caused an intergrity failure due to the host systems differing on native `Path` intepretation.  

## v0.0.6 (pre-alpha)

Trace to_dict: 
- `Trace.to_dict()` now uses `serialise_events=True` as default. `to_dict()` should always be serialisable.

## v0.0.5 (pre-alpha)

Trace export:
- `Trace.to_dict()` now includes execution sequence IDs for sources and actors, and producer and consumer execution sequence IDs for reference versions.

## v0.0.4 (pre-alpha)

Preamble:
> v0.0.4 is a small update to TIDE-Tooling specifically fixing Store-poll Event handling semantics to correctly detect and version unobserved changes in stored values.

Store Polling:
- Adjusted the guard in `read_dependency_state()` to compare the polled stored-value hash with the last observed hash. If they differ, handling now falls through to create a new write record for the Reference/value pair.

Display:
- `Trace.show()` now hides Tide Run and Pipeline source metadata by default. Pass `show_sources=True`, or use `--sources` in the CLI, to display it.
