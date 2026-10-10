TIDE-Tooling changelog:

### v0.0.1 -> v0.0.2:
Effect handling changes.
- Identifier revision preventing deduplication
- `fallback_value` removed from effects
- `origin` distinguishes runtime vs declared effects
- The completion order of effects relative to a node is:

    - Live effects: [READ, WRITE, DELETE_NOW]; effects used within a function.
    - Node declared writes.
    - Node declared effects.
    - Node runtime effects; effects returned by a node as its result. 

Node Emission + EMIT.
- Removed `.emitted_by` from nodes. 

Pathing:
- Pathing now uses data stacks to avoid python recursion depth errors on valid paths.
- Removed and split `terminal_link` responsibility into: `_edge_reaches_path_target` and `_edge_path_explanation` 

TIDE Model alignment:
- Updated comparator, validity and model exports/imports to reflect the new revision of the TIDE paper: https://github.com/tnkinobirb/TIDE-Model-Artifact/commit/12041c64af094cd9827febefa8f17bfbdcc2a3e5 

Documentation and Examples:
- Updated README to reflect changes
- Corrected broken upstream/downstream examples
