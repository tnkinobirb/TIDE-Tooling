# Bundles

**A TIDE Bundle serialises Trace evidence, faithfully where possible, so it may be persisted, moved, and rebuilt later.**

```text
Trace
  │
  ▼
Bundle
  │
  ▼
Trace
```

It is different from the TIDE Model:

```text
Bundle
→ preserve and rebuild the richer Trace

TIDE Model
→ reduce execution to the smaller E / S / I / O relationship model
```

For the reduced relationship representation, see
[`TIDE_Model.md`](TIDE_Model.md).

---

## Creating a Bundle

The shortest route is:

```python
bundle = trace.to_bundle()
```

This returns a `TIDEBundle`.

```text
Trace
  │
  └── to_bundle()
          │
          ▼
      TIDEBundle
```

A Bundle contains two main pieces:

```
Bundle
├── manifest
│    ├── Value records
│    ├── Reference/version indexes
│    ├── execution evidence
│    ├── optional Events
│    └── causal corruption evidence
│
└── CAS
     └── content-addressed serialised value data
```

The manifest describes how information fits together. The CAS stores serialised value content referenced by that manifest.

`bundle.corruption` retains the source Trace's exact missing-cause and causal-order
evidence. Native TideRun evidence is the Bundle reconstruction authority. When
`serialise_events=True`, the frozen Event stream and payloads are retained as
optional source evidence alongside it. When Event verification is enabled, TIDE
compares that sidecar with the authoritative TideRun through the normal Event
reconstruction and evidence-diff path. A mismatch is reported by
`trace.events_verified`; it never replaces or rewrites the TideRun evidence.

---

## Saving a Bundle

A Bundle can be exported directly to a JSON file:

```python
from pathlib import Path

trace.to_bundle(
    path=Path("trace.json"),
)
```

When `path` is supplied, `to_bundle()` uses the JSON file export automatically.

You can also keep the native Python payload:

```python
payload = trace.to_bundle().to_dict()
```

or request a JSON-safe dictionary:

```python
payload = trace.to_bundle().export(
    type="dict",
    kind="json",
)
```

So the common forms are:

```text
trace.to_bundle()
→ TIDEBundle

trace.to_bundle().to_dict()
→ native Python bundle payload

trace.to_bundle(path="trace.json")
→ JSON bundle file
```

---

## Loading a Bundle

`TIDE.from_bundle()` rebuilds an inspectable Trace:

```python
from tide_tooling import TIDE

trace = TIDE.from_bundle("trace.json")
```

It also accepts a `TIDEBundle` directly:

```python
bundle = original_trace.to_bundle()

restored = TIDE.from_bundle(bundle)
```

or the Bundle's Python dictionary payload:

```python
payload = bundle.to_dict()

restored = TIDE.from_bundle(payload)
```

The rebuilt Trace can use the normal query surface:

```python
restored.show()
restored.find("clean").show()
restored.investigate("clean", "actor").show()
restored.value_of("global:result", show=True)
```

The native TideRun is the Bundle reconstruction authority. Retained Events,
when present, are optional source evidence and are not replayed to replace the
bundled Trace.

Loading rejects malformed graphs, invalid hashes, broken execution
relationships, and resource-limit violations. Understandable backing-value
mismatches remain explicit in `Trace.divergence` without replacing recorded
State identity.

### Optional Event verification

If a Bundle retains Events, correspondence verification is enabled by default:

```python
trace = TIDE.from_bundle(bundle)
```

`trace.events_verified` has exactly three meanings:

```text
True   verification ran and the Event sidecar agreed with TideRun
False  verification ran and the Event sidecar disagreed with TideRun
None   verification was skipped or no Event sidecar was retained
```

A failed verification does not prevent the Trace from loading. TideRun remains
the Bundle reconstruction authority. Verification can be skipped explicitly:

```python
trace = TIDE.from_bundle(bundle, verify_events=False)
```

### Optional Bundle signing

Signing uses the optional `cryptography` dependency:

```bash
pip install tide-tooling[signing]
```

Generate an explicit Ed25519 key pair, save it if required, and sign a Bundle:

```python
from tide_tooling import TIDE

keys = TIDE.generate_signing_keys()
keys.save_private("private.pem")
keys.save_public("public.pem")

bundle = trace.to_bundle(sign=keys.private_key)
```

Verify either the Bundle itself or while loading it:

```python
bundle.verify(keys.public_key)

restored = TIDE.from_bundle(
    bundle,
    verify=keys.public_key,
)
```

The signature authenticates the canonical Bundle content. Authentication is
separate from `trusted`, `divergence`, `corruption`, and `events_verified`.
Adding a signature does not change the Bundle's content identity or
`bundle_hash`. The private key PEM is saved unencrypted; protect it accordingly.

For Trace queries, see [`Queries.md`](Queries.md).

---

## Bundle Values and References

A `TIDEBundle` can also be inspected before rebuilding the Trace.

```python
bundle.show()
```

Recorded Values are indexed by their TIDE value hash:

```python
value = bundle.value_of(value_hash)
```

A Value can also be recovered through an exact Reference version:

```python
value = bundle.value_at(
    "global:result",
    version=0,
)
```

Reference history can be inspected with:

```python
bundle.versions("global:result")
```

and a value hash can be traced back to the Reference versions that contain it:

```python
bundle.references_for(value_hash)
```

Conceptually:

```text
Reference + version
        │
        ▼
    value hash
        │
        ▼
 serialised Value
```

---

## Unsupported Values

By default:

```python
trace.to_bundle()
```

uses:

```text
unsupported="opaque"
```

If a Python value cannot be safely represented by the supported serialisers,
TIDE records an opaque description rather than failing the entire Bundle.

When loaded, that value is represented as a `TIDEOpaqueValue`.

If unsupported values should instead stop bundling immediately:

```python
trace.to_bundle(
    unsupported="strict",
)
```

This makes unsupported value serialisation an error.

---

## Trusted loading

Bundle loading is untrusted by default:

```python
trace = TIDE.from_bundle(bundle)
```

Ordinary supported values can still be restored, but recorded Python object
state is not reconstructed as a live object. It is exposed opaquely instead.

For a Bundle whose producer and contents are trusted:

```python
trace = TIDE.from_bundle(
    bundle,
    trusted=True,
)
```

Trusted loading may import recorded Python types and invoke their state
restoration behaviour.

Use `trusted=True` only for Bundles you trust.

---

## Bundle round trips

A normal Bundle round trip is:

```python
bundle = trace.to_bundle()
restored = TIDE.from_bundle(bundle)
```

or through a file:

```python
trace.to_bundle(path="trace.json")

restored = TIDE.from_bundle("trace.json")
```

The purpose of the round trip is to preserve the supported Trace evidence and
Values so the rebuilt Trace can still be inspected and queried.

The first Bundle establishes the portable snapshot. A restored Trace has
`from_cas is True`, and every subsequent Trace → Bundle round trip is an exact
fixed point. A divergent import remains explicitly divergent across later Bundle exports;
reconstructed backing values do not rewrite recorded State identity.

```text
live execution
      │
      ▼
    Trace
      │
      ▼
    Bundle
      │
      ▼
restored Trace
      │
      ▼
 show / find / investigate / path / value_of
```

---

## Summary

```text
Need to keep or move a Trace?
→ Bundle

Create:
trace.to_bundle()

Save:
trace.to_bundle(path="trace.json")

Load:
TIDE.from_bundle("trace.json")

Inspect without rebuilding:
bundle.show()
bundle.value_at(...)
bundle.versions(...)

Unsupported values:
opaque by default
strict if requested

Untrusted Bundle:
trusted=False

Trusted object reconstruction:
trusted=True

Optional signing:
pip install tide-tooling[signing]
```
