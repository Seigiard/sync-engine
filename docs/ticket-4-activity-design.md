# Ticket 4 Activity Design Review

Decision: **go**. The complete later-pass activity has been implemented in
`src/live-activity.ts`. The public API and the live reducer contract are
unchanged.

## Baseline

The merged step-1 implementation is the baseline. It gives callers a typed
two-phase opening without adding a public API:

```ts
interface InternalOpening<W, E> {
  readonly synchronization: Synchronization<W, E>;
  readonly commitFreshness: Effect.Effect<void, E>;
}
```

`openSynchronizationInternal` owns validation, the lease, source scan,
declaration, minimum and required work, and publication. It returns before the
freshness commit so the live caller can preserve the opening invalidation
window (`src/internal.ts:15-16, 41-84`, `src/live.ts:167-195`). The public
opening composes the initial commit and later completion in `src/index.ts`.

## Before Interface

At the baseline, the live interpreter had two activities. The opening kept its
special freshness window and failure policy. A later pass assembled this
sequence in `runPass` (`8ae7862^:src/live.ts:217-261`):

```text
leading source scan and differences
  -> passClaim
  -> declare
  -> submit and awaitCompletion
  -> publish
  -> commitFreshness
  -> set committed and advance baseline
  -> trailing source scan
  -> close a drained pass scope
  -> classify pass failure and discharged progress
```

The caller also had to combine a pass-scope close cause with the activity
cause. If the scheduler had not drained, it had to leave that scope for
attempt close so handlers could finish before resources were released.
Admission, invalidation, attempt close, retry, and settlement were already
session responsibilities and remain so.

## Candidate B

The accepted interface is the typed Effect activity in
`src/live-activity.ts:6-16, 18-31, 45-85`:

```ts
runLaterActivity({
  pass,
  baseline,
  sourcePath,
  includeSource,
  passParent,
  synchronization,
  commitFreshness,
  claim,
  declare,
}): Effect<LaterActivityResult<E>>
```

The module owns the complete later sequence:

- leading scan and source differences;
- the claim point, immediately after that scan and before declaration;
- declaration, work submission, scheduler drain, publication and freshness commit;
- baseline advance before the trailing scan;
- trailing scan and its source differences;
- pass-scope fork and close when the scheduler is drained;
- combined pass-scope cleanup causes;
- `endsAttempt` classification and the `discharged` commit receipt;
- the complete success or failure result, including the advanced baseline.

The module leaves a pass scope open when the scheduler is failed or stopped.
`closeAttempt` then closes the attempt scope after the activity fiber has
joined (`src/live.ts:234-247`). This preserves handler lifetime while keeping
pass-scope cleanup inside the activity for every drained pass.

The session supplies one callback because the pure reducer owns admission and
claim state. The callback is invoked by the activity at its fixed claim point;
the caller no longer chooses when to claim. The caller only stores the returned
baseline and turns the discriminated result into the existing `passOk` or
`passFail` event (`src/live.ts:206-227`). This is result delivery, not a second
progress protocol.

## Obligation Comparison

### Removed from the live caller

- drain, publication and freshness-commit ordering;
- the mutable `committed` flag and its propagation through trailing scan and
  pass-scope cleanup;
- leading and trailing source-difference calculations;
- pass-scope fork, drained-status check, close and pass-cause joining;
- pass failure classification, including scheduler failure and interruption;
- construction of the complete later activity result.

### Retained session obligations

- admission and reducer state, including the claim callback's reducer event;
- the baseline as live session state, updated from the activity result;
- pre-admission invalidation and retargeting;
- attempt identity, attempt-scope ownership and `attemptClosed` handling;
- retry, follow-up payload retention and waiter settlement;
- session shutdown and output-lease finalization.

These facts are retained by the ticket. They are not added obligations of the
activity interface.

### Truly added interface obligations

- one typed `claim(changes)` callback, required to cross from the activity into
  the pure reducer at the existing claim point;
- one result discriminant with the existing success/failure fields and the
  baseline receipt.

There is no opening/later mode, configurable order callback, acknowledgement,
checkpoint command, or separate progress machine. The callback does not expose
timing to the caller, and the result does not require the caller to infer
whether commit completed. The removed ordering and progress obligations are
larger than these two typed values, so Candidate B passes the ticket's
two-part gate.

## Required Path Traces

### Failed publication

Initial publication remains in `openSynchronizationInternal` after required
work (`src/internal.ts:66-84`). A typed publication failure prevents the
opening result and its commit from being returned; the live interpreter emits
`openFail` (`src/live.ts:170-195`).

For a later pass, Candidate B runs publication before freshness commit
(`src/live-activity.ts:53-58`). A typed publication failure leaves
`discharged` false and returns `passFail`; the reducer retains the pass payload
(`src/live-machine.ts:311-324`). No shared opening/later policy was introduced.

### Opening invalidation

The opening still dispatches `freshnessReady`, then runs the opening hook, then
commits freshness (`src/live.ts:182-189`). The reducer sends every owed
invalidation to that opening before the commit (`src/live-machine.ts:267-271`).
The activity extraction does not wrap or move this handshake.

### Target change during admission

The request path captures the invalidation target, invalidates it before
admission, pauses at the existing hook, and dispatches with `appliedTo`
(`src/live.ts:250-261`). If the target changes, `admit` emits a retarget command
to the replacement attempt (`src/live-machine.ts:193-212`). This remains
outside the sequential activity module.

The reachable regression commit `46b3695` retains
`a delayed watcher invalidation retargets a replacement opening before restart`
in `test/live.test.ts`. Its isolated omission calibration produced `Original`
instead of the independent restart oracle's `Changed!`; the baseline test is
green and remains preservation evidence, not a new missing-behavior lock.

### Commit followed by trailing traversal failure

Candidate B sets its local commit receipt immediately after freshness commit,
advances the returned baseline, and only then scans the source
(`src/live-activity.ts:57-62`). A trailing scan failure returns `passFail` with
`discharged: true` (`src/live-activity.ts:74-84`). The reducer therefore does
not re-owe that payload (`src/live-machine.ts:311-315`).

The opening trailing scan remains different: it belongs to the opening and
maps to `openFail`, even if opening publication and commit already completed
(`src/live.ts:186-195`).

### Drained pass-scope cleanup failure after commit

When the scheduler is drained, Candidate B closes the pass scope after the
activity exit. A close failure is combined with the activity cause before the
result is built (`src/live-activity.ts:64-71`). If commit already completed,
the result still carries `discharged: true` and the advanced baseline. The
caller emits `passFail`; the reducer does not re-owe the request
(`src/live-machine.ts:311-315`). A typed close cause can keep the attempt;
an interruption, defect, or failed scheduler sets `endsAttempt` and enters
the existing attempt-close path.

This is distinct from `attemptClosed`. When a failed or stopped scheduler has
not drained, the activity deliberately leaves the child pass scope attached
to its parent. `closeAttempt` first joins the activity, then closes the attempt
scope and combines any attempt-finalizer cause into `attemptClosed`
(`src/live.ts:234-247`). The reducer applies that cause to retry and settlement
(`src/live-machine.ts:327-347`). Thus pass-scope cleanup and attempt-scope
cleanup do not overwrite commit progress or each other.

### Handler still using pass resources

The pass parent is forked during initial declaration before the scheduler is
created (`src/live.ts:174-181`, `src/internal.ts:42-45`). If a worker dies,
Candidate B observes the failed scheduler and does not close the pass child
scope. `closeAttempt` waits for the activity, and reverse finalization of the
attempt scope stops scheduler workers before releasing the earlier-registered
pass resource (`src/live.ts:234-247`). The session-shutdown finalizers at
`src/live.ts:264-271` are a separate outer-scope path and are not used as the
mechanism for a failed attempt.

The independent tests
`a later pass's resources are released only after the scheduler stopped the
handlers still using them` and
`a later pass with null work releases resources only after a handler stops when
another worker ends by defect` remain in `test/live.test.ts:1906-1983`.

## Compatibility And Verification

The reducer still owns admission, payload merging, attempt/pass identity,
invalidation, retry, follow-up, ready/completion settlement and shutdown. The
opening still owns minimum work, `onMinimum`, initial failure policy and its
invalidation window. No public signatures, errors or README behavior changed.

Checks completed for this implementation:

- `docker compose -f docker-compose.test.yml run --rm engine-test bun test test/live.test.ts`: **pass**, 58 pass, 0 fail.
- `docker compose -f docker-compose.test.yml run --rm --build engine-test`: **pass**, lint 0 warnings/0 errors, typecheck pass, 157 pass, 0 fail.
- `docker compose -f docker-compose.test.yml run --rm engine-test bun test test/shutdown.test.ts`: **pass**, 2 pass, 0 fail.
- `git diff --check`: **pass**.
- `bun run lint`: not available on host because `oxlint` is not installed; Docker lint is the repository check.
- `bun run typecheck`: not available on host because `@types/bun` is not installed; Docker typecheck is the repository check.
- `bun scripts/calibrate-live-model.ts --dry-run`: not applicable; `src/live-machine.ts` was unchanged.

The full Docker check includes the bounded live model and all runtime
fault/lifetime tests. The shutdown test was also run after the implementation
was corrected to keep the activity body interruptible while preserving
uninterruptible result delivery.
