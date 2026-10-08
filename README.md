# Shared synchronization engine

Implementation of [Seigiard/opds-generator#49](https://github.com/Seigiard/opds-generator/issues/49).

Package identity: `@seigiard/sync-engine`.
Repository identity: `Seigiard/sync-engine`.

The library owns synchronization mechanisms. Applications own domain processing and publication requirements.
Bun on Linux/Docker and Effect 4 are the supported execution boundary.

## Local package: 0.3.1

`runInitialPass({ sourcePath, outputPath, declare, handle })` returns an Effect.
It scans regular source paths and directories, with relative path identity,
size and modification time. Source symlinks are excluded. Source/output trees
must be disjoint. Applications declare an `InitialPlan`: initial work and a
final publication Effect. Each handler returns required cascade work. The
engine drains that work before final publication. Applications keep domain
classification, extraction, rendering, path projection and publication rules.
`includeSource(relativePath)` is an optional application policy. The engine
checks it before stat or descent, including excluded directory subtrees.
Without that policy, regular source names are unrestricted.

`openSynchronization(options)` runs the same initial pass and returns a scoped
`WorkScheduler`. Keep it inside `Effect.scoped`; its output lease remains held
until the scope closes. `submit(work)` accepts subsequent application work.
`awaitCompletion` waits for pending, active and handler-returned required work.
`status` reports `working`, `complete`, `complete-with-errors`, `failed` or
`stopped`, with pending count, active work and `errors: [{ work, cause }]`.
`complete` means work finished, not verified freshness. Typed failures keep
independent work running and remain observable after the work drains.
`failureKey(work)` declares error identity independently from pending coalescing.
A successful retry clears that identity; repeated failures replace its record.

Applications can supply `key(work)` for refresh requests. A defined key combines
equivalent pending work and moves it behind intervening work. An active request
is separate: another request schedules one pending follow-up. Undefined keys
retain each request. Results publish as handlers finish; the scheduler neither
stages a whole-tree snapshot nor observes output writes.

`createWorkScheduler({ handle, key })` exposes the same scoped work mechanism
without scanning or acquiring an output lease. Use `openSynchronization` for
an application output tree. Defects and interruption stop this scheduler;
typed handler failures are recoverable work results. Scope close joins the owned consumer fiber
before releasing the output lease. Handlers retain their safe publication phases.

An initial source-read failure fails the pass. Initial handler failures drain
independent work and prevent final publication. Existing outputs stay available.
`ScanFailed` and `OutputOwnershipFailed` distinguish engine failures from the
application's error channel. Handlers own their interruptible preparation and
safe publication phases. All native filesystem Promise crossings are owned.

`acquireOutputTree(outputPath, statePath?)` supplies the same lease to legacy consumers
during migration. It returns an async release function. Effect consumers use
`Effect.acquireRelease` in a scope; Promise consumers must release it after
joining their work. Linux `flock` (`util-linux`) holds a persistent lock inode
at `engineStatePath(outputPath, statePath?)/lock`. A handshake confirms ownership. Release closes stdin
and joins the holder. Process death closes the pipe and releases the kernel
lock. Contention waits one second before failing. Keep the state inode stable
while owners may acquire it. Canonical-path aliases use the same configured state
area. Separate applications use separate, non-overlapping output roots.

`InitialPass.statePath` selects a persistent engine-owned state area. The default
is a SHA256-keyed sibling of the canonical output directory. This keeps engine
metadata outside unrestricted application projections. An application can select
an in-output area excluded by its source policy, such as OPDS `DATA/.sync-engine`.
All compositions that share an output must use the same canonical state area.
Docker containers must mount that area on the same shared persistent filesystem;
a shared output mount alone does not share a sibling on another filesystem.

`observeSourcePath(sourcePath, relativePath, fs?)` returns present or confirmed
absence. Missing or unreadable source roots fail with `SourceObservationFailed`.
Absent children require a successful parent read and a valid root; symlink paths
are excluded from source authority. `readSourceDirectory` preserves read errors.
The optional `SourceFileSystem` is an external read-only fault-injection boundary.

`removeAssociatedOutputs({ sourcePath, outputPath, statePath?, sourceRelativePath,
outputs }, fs?)` rechecks absence before removing application-declared relative
outputs. A stale hint returns false. Cleanup rejects root removal, traversal,
absolute projections, symlink ancestors and overlap with configured engine state.
Run it inside the owning session. The application returns required publication
cascades after removal. No object identity persists across source moves.

The exact `effect@4.0.1` peer keeps one runtime identity. The tarball ships
TypeScript source for Bun; it contains neither node_modules nor bundled Effect.
This local release is not an npm publication. Watchers, resync,
optional-work readiness, concurrency and reconciliation are later slices.

## Freshness

Supply optional `freshness: { describe(work), check? }` on `InitialPass`.
`describe` returns `{ sourcePaths, resultKind, processingVersion, outputPaths }`
for cacheable work, or `undefined` for unconditional work. Paths are relative
to the source/output roots. Applications own result kinds and processing
versions; the engine package version is not part of result freshness.

The default `check: "metadata"` compares source size, mtime and processing
version, and requires the declared outputs to exist. `check: "content"`
additionally reads and hashes regular files. Without a watcher hint, metadata
checks can miss content replacements that preserve both size and mtime.

`submit(work)` explicitly reprocesses its described results. Ordinary pass
admission uses `submit(work, { force: false, changedPaths: [] })`.
Source-relative `changedPaths` hints invalidate affected saved results even
when metadata matches. A hint during active work also prevents that earlier
read from being recorded as current. `force: true` bypasses all retained freshness.
Returned required work invalidates its dependent result, so a changed upstream
cannot shortcut the publication it requires.

Dirty records are removed before processing. Only a completed successful batch
records new freshness; a failure or interruption leaves work eligible for replay.
Sources are checked before/after processing and again at successful completion.
The output lease serializes retained-state access. `FreshnessFailed` reports
source/output-check and state-read/write failures. `openFreshness` is the adapter
for other engine compositions; give it the engine-owned state directory while
holding the output lease, then use its handle, invalidation and commit operations.

## Consumer shape check

- OPDS: required book/folder cascades precede root publication.
- TTRPG: a full listing admits tree classification and index work; optional
  parallel images need a later scheduling/readiness extension.
- OPML: required RSS cascades precede final OPML. Its private cache projection
  remains application logic.

## Verify and pack

```sh
COMPOSE_PROJECT_NAME=opds49-52 docker compose -f docker-compose.test.yml run --build --rm engine-test
COMPOSE_PROJECT_NAME=opds49-52 bun test/shared-mount-check.ts
COMPOSE_PROJECT_NAME=opds49-52 docker compose -f docker-compose.test.yml down
bun pm pack
```

OPDS's `docs/agents/shared-sync-engine.md` records the integration reproduction
command and temporary lifecycle selection seam. Copy a versioned packed
artifact to that consumer; direct checkout imports are not the release boundary.
# Live synchronization

`openLiveSynchronization(options)` retains the scoped output lease and performs
the initial publication. It adds engine-owned source scans, pass scheduling and
periodic reconciliation to `openSynchronization`.

The application supplies `declare(entries, request)` and the existing handler,
key and publication contracts. `request` contains `kind`, `force` and relative
`changedPaths`. Initial declaration prepares the initial publication. Later
declarations repair in place. The source tree remains authoritative.

The returned session exposes:

- `notify(relativePaths)`: retain watcher hints and schedule reconsideration.
- `requestPass({force})`: start a pass or combine into the pending follow-up.
- `awaitCompletion`: await admitted scans, processing, publication and follow-ups.
- `status`: observe the active pass, pending request and work completion separately.

A pass remains active until required work and final publication finish. Requests
during that interval guarantee a follow-up. Pending requests combine, retaining
every dirty path and any forced mode. A post-processing traversal detects source
size, mtime, kind and membership changes and requests repair before completion.
This includes the initial pass. Stable detectable sources converge after successful
processing; traversal does not provide a filesystem snapshot.

`reconcileIntervalMs` enables the scoped timer; zero disables it. Scope closure
stops the timer and pass consumer before the work scheduler releases its lease.
