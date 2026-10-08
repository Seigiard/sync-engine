# Shared synchronization engine

Implementation of [Seigiard/opds-generator#49](https://github.com/Seigiard/opds-generator/issues/49).

Package identity: `@seigiard/sync-engine`.
Repository identity: `Seigiard/sync-engine`.

The library owns synchronization mechanisms. Applications own domain processing and publication requirements.
Bun on Linux/Docker and Effect 4 are the supported execution boundary.

## Initial pass and scheduler

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
`Synchronization`, compatible with `WorkScheduler`. Keep it inside `Effect.scoped`; its output lease remains held
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
Applications own watcher transport;
the live API owns resync scheduling and reconciliation. Concurrency is a later slice.

## Minimum readiness

An `InitialPlan` may declare `minimum` work beside `work`. The initial pass runs
the minimum first. A minimum that cannot finish fails the open at once, before
the remaining work starts. When it drains without errors the engine runs the
optional `onMinimum` Effect, then submits `work`. The application decides what
the minimum is and what "ready" means; the engine reports only that the minimum
finished. Completion still waits for all required work. A plan without `minimum`
runs `onMinimum` right after declaration. Later live passes ignore `minimum`;
list work that must repeat in `work` as well. `LiveStatus.failure` carries the
cause of the last failed pass until a later pass succeeds.

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

The current temporary bridge stores freshness in a sibling directory named
`.sync-engine-state-<digest>`, using the canonical output path's full SHA256.
Configurable `statePath` and the OPDS `DATA/.sync-engine` composition belong to
the separate #52 ownership integration. The default external location must remain
compatible with existing retained records.

## Consumer shape check

- OPDS: required book/folder cascades precede root publication.
- TTRPG: a full listing admits tree classification and index work; optional
  parallel images need a later scheduling/readiness extension.
- OPML: required RSS cascades precede final OPML. Its private cache projection
  remains application logic.

## Verify, pack and release

The package ships TypeScript source (`src`, `README.md`, `package.json`) for Bun on Linux. `effect@4.0.1` is an exact
peer. `publishConfig.access` is `public` for the scoped name.

```sh
# 1. Lint, typecheck and tests in Linux/Docker (flock needs util-linux)
COMPOSE_PROJECT_NAME=sync-engine docker compose -f docker-compose.test.yml run --build --rm engine-test
COMPOSE_PROJECT_NAME=sync-engine bun test/shared-mount-check.ts
COMPOSE_PROJECT_NAME=sync-engine docker compose -f docker-compose.test.yml down
# 2. From a clean checkout: pack and verify identity, peer, exact inventory and byte equality
bun scripts/verify-pack.ts <destination>
```

`verify-pack.ts` fails on a dirty tree, a version or peer mismatch, an unexpected file, or any packed
file that differs from the checkout. It prints the commit, archive path, size, SHA256 and the SHA512
integrity string a lockfile records. A release is the `npm publish` of that archive's checkout; verify
the registry afterwards with `npm view @seigiard/sync-engine@<version> dist.integrity` and compare it to
the printed integrity.

Update a consumer in two steps. Development: copy the archive to the consumer's `vendor/` under a
content-qualified name, depend on `file:vendor/<name>` and rebuild its images; Bun caches file
dependencies by path, so reuse of a basename keeps the old copy. Release: replace that dependency
with the exact registry version, regenerate the lock, and rebuild. Never ship a consumer whose
runtime dependency is a local archive or checkout.

## Live synchronization

`openLiveSynchronization(options)` retains the scoped output lease and performs
the initial publication. It adds engine-owned source scans, pass scheduling and
periodic reconciliation to `openSynchronization`.

The application supplies `declare(entries, request)` and the existing handler,
key and publication contracts. `request` contains `kind`, `force` and relative
`changedPaths`. Initial declaration prepares the initial publication. Later
declarations repair in place. The source tree remains authoritative.
Live passes forward `force` and `changedPaths` to freshness-aware admission.
Declare every applicable result on every pass so processing-version or content
checks can select rebuilding even when size and mtime do not change.

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

## First-pass failure map and retry

`startLiveSynchronization(options)` returns a `LiveHandle` at once and runs the first pass in the
background. `openLiveSynchronization` is `start` plus `ready`. `ready` settles when the first pass
did. Requests made before that (`requestPass`, `notify`) report `queued`, combine, and run as one
follow-up pass after the open, keeping force and every hint.

Without `recovery` a failed first pass fails `ready`. With
`recovery: { existing }` the engine tolerates it while output is usable: `existing` reports earlier
output that already serves (read once before the first pass), and a published `minimum` counts
too. `status.availability` reports `"prior-output"`, `"minimum-publication"` or `null`. A tolerated
failure resolves `ready`, sets `status.failure` and `state: "failed"`, releases the lease and waits.
The next `requestPass`/`notify` (reports `started`) or reconcile tick reopens the session in the
same scope: a full first pass again, then any retained request. A disabled timer
(`reconcileIntervalMs: 0`) leaves only requests as retry triggers. Without usable output `ready` fails
and the application decides whether that is fatal. This is the one owner of retry and reconcile
scheduling; applications keep no timer of their own.

## Cooperative shutdown and restart

Close the session's Effect scope to stop it. Admission closes immediately,
pending work and follow-ups are discarded, and traversal and reconciliation stop.
The scope interrupts preparation and joins the owned consumer before releasing
output ownership. Native filesystem reads finish before cancellation proceeds.
Handlers own command termination, temporary resources and publication boundaries.
Use interruptible preparation and an uninterruptible publication phase to finish
related writes once publication starts. Interruption remains an Effect interruption.

The public state becomes `stopped` when admission closes. Active work can remain
visible while cleanup or safe publication finishes. After scope closure completes,
active work and the active pass are null. A stopped session rejects later admission.

The queue is in memory. A new scoped session scans sources and repeats unfinished
work through successful-only freshness. A write without recorded success is
eligible for replay; application handlers must tolerate that replay. Cooperative
shutdown preserves the handler's declared boundary, not atomic safety against
SIGKILL, a shutdown deadline, or power loss at every intermediate write.
