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
After that scope closes, the returned handle rejects admission and completion
without touching retained freshness.
`awaitCompletion` waits for pending, active and handler-returned required work.
`status` reports `working`, `complete`, `complete-with-errors`, `failed` or
`stopped`, with pending count, one active work item and `errors: [{ work, cause }]`.
With `concurrency` above one, `active` is a sample of active work, not a full list.
`complete` means work finished, not verified freshness. Typed failures keep
independent work running and remain observable after the work drains.
`failureKey(work)` declares error identity independently from pending coalescing.
A successful retry clears that identity; repeated failures replace its record.

Applications can supply `key(work)` for refresh requests. A defined key combines
equivalent pending work and moves it behind intervening work. An active request
is separate: another request schedules one pending follow-up. With `concurrency`
above one, keyed pending work waits while an equivalent key is active. Undefined keys
retain each request and can run in parallel. Results publish as handlers finish; the scheduler neither
stages a whole-tree snapshot nor observes output writes.

`createWorkScheduler({ handle, key, concurrency })` exposes the same scoped work mechanism
without scanning or acquiring an output lease. Use `openSynchronization` for
an application output tree. Defects and interruption stop this scheduler;
typed handler failures are recoverable work results. Scope close joins the owned consumer fiber
before releasing the output lease. Handlers retain their safe publication phases.

An initial source-read failure fails the pass. Initial handler failures drain
independent work and prevent final publication. Existing outputs stay available.
`ScanFailed` and `OutputOwnershipFailed` distinguish engine failures from the
application's error channel. Handlers own their interruptible preparation and
safe publication phases. Later live passes differ from the initial pass: typed
required-work failures remain visible in status, but final publication still runs
so consumers can keep serving prior successful results. All native filesystem Promise crossings are owned.

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
Absent children require a successful parent read, or a regular-file ancestor,
and a valid root; symlink paths are excluded from source authority. `readSourceDirectory` preserves read errors.
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
the live API owns resync scheduling and reconciliation. `concurrency` defaults to one.

## Minimum readiness

An `InitialPlan` may declare `minimum` work beside `work`. The initial pass runs
the minimum first. A typed minimum failure is reported after the minimum work drains,
and before the remaining work starts. When it drains without errors the engine runs the
optional `onMinimum` Effect, then submits `work`. The application decides what
the minimum is and what "ready" means; the engine reports only that the minimum
finished. Completion still waits for all required work. A plan without `minimum`
runs `onMinimum` right after declaration. Later live passes ignore `minimum`;
list work that must repeat in `work` as well. A live session opens with an initial
pass every time, including a reopen after a failed attempt, so the initial
declaration, the minimum and `onMinimum` run again then; `onMinimum` must tolerate that. `LiveStatus.failure` carries the
cause of the last recoverable failed pass or open until a later attempt succeeds.

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

By default, freshness is stored in a sibling directory named
`.sync-engine-state-<digest>`, using the canonical output path's full SHA256.
`statePath` overrides that location when an application needs a shared persistent
state area, such as OPDS `DATA/.sync-engine`. The default external location must
remain compatible with existing retained records.

## Consumer shape check

- OPDS: required book/folder cascades precede root publication.
- TTRPG: a full listing admits tree classification and index work; optional
  images can run as parallel engine work when keyed republish work is used.
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
# 2. From a clean checkout: pack and verify identity, peer, inventory and byte equality
bun scripts/verify-pack.ts <destination>
```

`verify-pack.ts` fails on a dirty tree, a version or peer mismatch, an unexpected file, or any packed
source/README file that differs from the checkout. For `package.json`, it checks package name,
version and peer dependencies instead of byte equality. It prints the commit, archive path, size,
SHA256 and the SHA512 integrity string a lockfile records. A release is the `npm publish` of that archive's checkout; verify
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
`changedPaths`. Initial declaration prepares the initial publication and uses the
minimum gate described above. Later declarations repair in place. The source tree remains authoritative.
Live passes forward `force` and `changedPaths` to freshness-aware admission.
Declare every applicable result on every pass so processing-version or content
checks can select rebuilding even when size and mtime do not change.
Detected directory mtime changes do not become subtree freshness hints while the
directory still exists as a directory; its changed children are reported instead.
Removed directories and file/directory kind changes still invalidate by prefix.
Explicit watcher hints passed to `notify` keep their prefix semantics.

The returned session exposes:

- `notify(relativePaths)`: retain watcher hints and schedule reconsideration.
- `requestPass({force})`: start a pass or combine into the pending follow-up. Until the
  running pass has claimed its request for `declare`, a new request joins that pass;
  after the claim it joins the pending follow-up instead. The same holds for `notify`.
- `awaitCompletion`: await admitted scans, processing, publication and follow-ups.
- `status`: observe the active pass, pending request and work completion separately.
  `followUp` is a pass that is scheduled to run. Data retained from a failed pass or
  attempt is not scheduled; it joins the next request and is not shown as `followUp`.

A pass remains active until required work and final publication finish. Typed
required-work failures in a later pass produce `complete-with-errors`; final
publication still runs, prior results for failed work remain, and the errors stay
visible in `status.work.errors`. Scoped resources that a later `declare` or `publish`
acquires belong to that pass and are released when it ends. If one of its handlers is still
running then (another worker died, or the session stops), they are released when the attempt
closes, after the scheduler stopped its handlers and before the lease is released.
A defect or interruption in any pass ends the current
attempt, releases the output lease, and follows the recovery policy below. Requests
during that interval guarantee a follow-up. Pending requests combine, retaining
every dirty path and any forced mode. A post-processing traversal detects source
size, mtime, kind and membership changes and requests repair before completion.
If that traversal fails after publication and freshness commit, the pass reports the
failure, but its request counts as applied and does not run again.
This includes the initial pass. Stable detectable sources converge after successful
processing; traversal does not provide a filesystem snapshot.

`reconcileIntervalMs` enables the scoped timer; zero disables it. A tick acts only
while the session is idle (finished, or failed and waiting); while a pass or attempt
runs, the tick is dropped, because the post-processing traversal already covers it.
Scope closure stops the timer and pass consumer before the work scheduler releases its lease.

## Attempt failure map and retry

`startLiveSynchronization(options)` returns a `LiveHandle` at once and runs the first attempt in the
background. `openLiveSynchronization` is `start` plus `ready`. `ready` settles when the first pass
did. Requests made before that (`requestPass`, `notify`) report `queued`, combine, and run as one
follow-up pass after the open, keeping force and every hint.

Without `recovery` a failed first pass or later pass defect stops the session. With
`recovery: { existing }` the engine tolerates it while output is usable: `existing` reports earlier
output that already serves (read once before the first pass), and a published `minimum` counts
too. A completed first publication also counts as usable output. `status.availability` reports
`"prior-output"`, `"minimum-publication"` or `null`; `"minimum-publication"` means this session has
published usable output, either minimum or full first publication. A tolerated failure resolves `ready`,
sets `status.failure` and releases the lease. One rule then decides what follows: if a request was
admitted during the failed attempt, another attempt starts at once in `state: "working"` and completion
stays outstanding; otherwise completion fails and the session waits in `state: "failed"`. The request
that started an attempt does not count, so a failure without new requests never repeats on its own.
Requests admitted while the failed attempt closes report `queued` and count as admitted during it.
The next `requestPass`/`notify` (reports `started`) or reconcile tick reopens a waiting session in the
same scope: a full first pass again, then every retained request. Force and hints retained from failed
attempts also invalidate freshness before that opening records anything. A disabled timer
(`reconcileIntervalMs: 0`) leaves only requests as retry triggers. Without usable output `ready` fails
for the first pass, or a later attempt stops admission. Admission closes as soon as such a failure is
detected (`state: "stopped"`); `ready` and completion fail only after the attempt has released the output
lease. If closing a failed attempt's scope fails too, for example a finalizer dies, that cause joins the
attempt's failure and the same rule applies. This is the one owner of retry and reconcile scheduling;
applications keep no timer of their own.

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
