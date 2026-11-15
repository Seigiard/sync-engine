# Changelog

## 0.6.0 — 2026-11-15

### Output cleanup

- Engine-managed handlers can receive an optional, session-bound
  `cleanup(outputs)` callback. It is available during initial work and later
  live work when the engine owns the output session.
- Cleanup validates the complete set of application-selected relative output
  paths before removing any path. It protects the output root and engine state,
  rejects traversal, absolute paths and symlink ancestors, and finishes before
  the output lease is released.
- A captured cleanup callback fails after its session scope closes. The
  standalone `createWorkScheduler` still has no output lease and does not
  provide this callback.

### Source observation

- `SourceObservationFailed` keeps its existing `path`, `cause` and `message`
  fields and can now include the optional
  `reason: "unsupported-path"` classification.
- Unsupported paths and unavailable or incomplete source reads remain
  observation failures. They are not proof that a source is absent.
- `removeAssociatedOutputs` removes projections only after a successful
  observation confirms `state: "absent"`. It retains outputs on observation
  failure.

### Freshness and live recovery

- A recovered live pass starts a fresh freshness batch and commits its own
  successful results. Results from that pass can be reused by a later
  unchanged pass or a new engine session.
- The retained freshness format and default state location stay compatible.
  This change does not require a cache purge or change the storage format.
- Freshness codec and source-snapshot internals are separated without changing
  the package-root API.

### Consumer compatibility

- Existing package-root imports remain valid.
- Existing one-argument handlers remain valid. Consumers that use an
  intermediate handler can forward the optional cleanup argument only when
  they adopt output cleanup.
- The package continues to require the exact `effect@4.0.1` peer.

For the required package steps and optional API adoption, see the
[version-update guide](https://github.com/Seigiard/sync-engine/blob/main/docs/upgrading.md).
