# Updating from `0.5.6` to `0.6.0`

This guide is for every application that consumes
`@seigiard/sync-engine`. It describes the package version update to `0.6.0`.
It does not name or change a consuming application.

## Required package update

Use the exact registry version `0.6.0` after publication is complete.

1. Set `@seigiard/sync-engine` to that exact registry version in the consumer
   manifest. Do not use a version range, a `file:` dependency, a local
   checkout, or a local archive for the shipped dependency.
2. Regenerate the consumer lockfile with its package manager. Commit the
   resulting lockfile with the manifest change.
3. Rebuild the consumer's dependency-containing image. A restart does not
   install the new package or lockfile.
4. Run the consumer checks for startup, output safety, retry and publication,
   and cooperative shutdown.

The package release uses the repository's staged publishing procedure. A local
development archive is useful for local testing, but it is not a shipped
release. Do not publish a local archive as the consumer dependency.

## Compatibility

No source change is required for existing package-root imports or one-argument
handlers. The handler type now permits a second optional argument:

```ts
handle: (work: W, cleanup?: OutputCleanup) =>
  Effect.Effect<readonly W[], E, R>
```

An existing handler may keep accepting only `work`. A wrapper that adopts
cleanup can preserve the optional contract and forward it:

```ts
const handle = (work: Work, cleanup?: OutputCleanup) =>
  applicationHandler(work, cleanup);
```

Only forward `cleanup` when the delegated handler can use it. Do not make it a
required argument or invent a fallback when it is absent.

Handlers supplied to `runInitialPass`, `openSynchronization` and the live
session APIs can receive the session-bound callback. A standalone
`createWorkScheduler` has no output lease and no output-cleanup callback. Use
`openSynchronization` when work must be tied to an owned output tree.

The retained freshness format and default state location remain compatible.
Existing retained results do not need to be deleted. Keep the exact
`effect@4.0.1` peer and the matching development dependency.

## Optional output cleanup

Output cleanup is an adoption choice, not a required package-update step. The
application remains responsible for:

- deciding from its domain authority that a source or projection is obsolete;
- selecting the application-owned output projections;
- running the required publication and cascade work after a removal.

The engine provides deletion safety, not application authority. The optional
callback is bound to the current output session. It validates the complete
output list before removing any path, rejects root, traversal, absolute,
symlink-ancestor and engine-state paths, and fails after the session scope
closes. Do not retain it for use outside that scope.

## Source observation and absence

`SourceObservationFailed.reason` is optional. Existing consumers can continue
to use `path`, `cause` and `message`. When present,
`reason: "unsupported-path"` identifies an unsupported source path. Adopting
this classification is optional; it must not change the absence rules.

An unsupported path is not an absent source. Neither is an unavailable or
unreadable source root, an incomplete parent read, or any other observation
failure. Do not treat any of these failures as permission to delete output.

`removeAssociatedOutputs` still requires a successful observation with
`state: "absent"` before it removes its declared projections. A stale hint
returns `false`; an observation failure retains the outputs. This is separate
from application policy: the application decides what should be published,
while the engine checks that every deletion stays within the owned output
tree.

## Freshness after a recovered live pass

A failed live pass no longer leaves its uncommitted candidates or failure state
in the way of a later successful pass. Each live pass starts a fresh batch.
After a recovered pass publishes and commits successfully, a later unchanged
pass and a new engine session can reuse those retained results.

This is a retention fix, not a storage-format change. The freshness state
location and record format remain compatible. There is no mandatory cache
purge.

## Consumer checks

After the exact registry update, lockfile regeneration and image rebuild,
check the consumer's normal scenarios:

- **Startup:** the initial scan, declaration, minimum readiness and first
  publication complete with the new package.
- **Output safety:** obsolete projections are removed only from the intended
  output tree; failed or unsupported source observations keep prior output.
- **Retry and publication:** a failed handler or publication keeps usable prior
  output, and a later successful pass publishes and retains its own results.
- **Shutdown:** closing the session stops new admission, joins active cleanup
  and publication work, and releases output ownership only after that work
  finishes.

The package README is shipped with the source. The changelog and this guide
are repository documents; use the links in the README to find them.
