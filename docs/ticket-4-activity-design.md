# Ticket 4 Activity Design Review

Decision: **no-go** for a new complete activity module.

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
freshness commit so live code can preserve the opening invalidation window.
`openSynchronization` composes the initial commit, and its public
`awaitCompletion` composes later scheduler completion with that commit. See
`src/internal.ts:9-16,41-85` and `src/index.ts:13-27`.

## Before Interface

The public initial caller has one explicit composition:

```text
openSynchronizationInternal(options)
  -> commitFreshness
  -> return synchronization with awaitCompletion + commitFreshness
```

The live caller has two deliberately different activities.

```text
opening:
  openSynchronizationInternal(wrapped options)
  -> dispatch freshnessReady
  -> afterOpeningPublication hook
  -> commitFreshness
  -> trailing source scan
  -> openOk/openFail

later pass:
  leading source scan and differences
  -> dispatch passClaim
  -> options.declare(entries, request)
  -> synchronization.submit(work, request)
  -> synchronization.awaitCompletion
  -> plan.publish
  -> commitFreshness
  -> record committed progress and baseline
  -> trailing source scan
  -> passOk/passFail
```

This sequence is visible in `src/live.ts:178-207` and `217-261`. The live
interpreter also owns the pass child scope and the attempt scope. It releases a
pass scope only after a drained scheduler, and defers it to attempt close when
handlers may still be running (`src/live.ts:238-247`).

## Candidate Interfaces

### Candidate A: three-call helper

```ts
runPublishedPass(synchronization, plan): Effect<CommitResult, E>
```

This can hide `submit`, `awaitCompletion`, `publish`, and `commitFreshness`,
but it cannot own the leading scan, reducer claim, baseline update, trailing
scan, scope close, failure classification, or `discharged` result. The live
caller would still assemble the protocol and keep the difficult obligations.
This fails the ticket gate explicitly.

### Candidate B: complete later activity

The smallest interface that could own the later sequence would be equivalent
to:

```ts
runLaterActivity({
  attempt,
  pass,
  baseline,
  synchronization,
  passScope,
  scanSource,
  claim: (changes) => Effect<PassRequest | undefined>,
  declare: (entries, request) => Effect<InitialPlan<W, E, R>>,
}): Effect<
  | { tag: "ok"; outcome: "complete" | "complete-with-errors"; changes: readonly string[] }
  | { tag: "fail"; cause: Cause.Cause<Failure>; endsAttempt: boolean; discharged: boolean }
>
```

This does move the central sequence and can return commit progress. It does
not remove caller knowledge: `claim` is the reducer's admission handshake,
`baseline` is live-session state, and `passScope` is owned by the attempt.
The caller must still decide when to dispatch `passClaim`, how to map the
result to `passOk`/`passFail`, when to close the scope, and how to combine a
cleanup cause with the activity cause. Making those decisions callbacks or
acknowledgements recreates the existing protocol behind a new name.

### Candidate C: one opening/later activity interface

```ts
runActivity({ mode: "opening" | "later", ... }): Effect<OpeningResult | PassResult>
```

This can hide more lines, but it adds a mode, a result union, and mode-specific
callbacks for minimum readiness, opening freshness, pass claim, publication
policy, and retry classification. Opening and later activities intentionally
have different failure policies, so the interface encodes rather than removes
the distinction. It also has to accept attempt cleanup ownership or move it
into the module, which would risk releasing resources while scheduler workers
still use them.

No candidate therefore satisfies the gate requirement: the live interpreter
does not stop knowing the ordering, reducer handshake, commit progress,
invalidation boundary, and lifetime boundary at the same time.

## Obligation Inventory

Current live caller knowledge:

- opening minimum readiness and the `freshnessReady` invalidation handshake;
- later claim timing, including the claim after leading scan and before
  declaration;
- drain, publication, freshness commit, baseline advance, and trailing scan
  order;
- initial failure policy versus later typed-handler failure policy;
- commit progress (`committed`) and the resulting request discharge;
- pre-admission invalidation target and retarget after the admission gap;
- pass scope and attempt scope ownership, including cleanup cause joining;
- activity outcome mapping and completion/ready settlement ordering.

Candidate B removes local statements for the central sequence, but adds or
retains knowledge as `claim`, `baseline`, `passScope`, an activity result
discriminant, and cleanup/result mapping. Candidate C adds `mode` and more
callbacks. Candidate A removes only three calls. These are not a net
reduction in caller obligations.

## Required Path Traces

### Failed publication

For the initial activity, `plan.publish` runs after successful required work
and before the opening result is returned (`src/internal.ts:66-84`). A typed
failure prevents the opening from returning and therefore prevents its commit;
the live interpreter maps it to `openFail` (`src/live.ts:202-206`). For a later
pass, publication runs before commit (`src/live.ts:227-232`). A typed
publication failure leaves `committed === false`, produces `passFail`, and
keeps the payload owed (`src/live.ts:251-260`, `src/live-machine.ts:311-324`).
One shared activity interface would need an explicit initial/later policy
mode.

### Opening invalidation

After the opening publication, live dispatches `freshnessReady`, runs the
opening hook, and only then commits (`src/live.ts:194-200`). A request in that
window invalidates the opening synchronization before admission
(`src/live.ts:284-295`); the reducer also sends every owed invalidation to the
opening before its commit (`src/live-machine.ts:267-271`). Hiding the opening
sequence behind an uninterruptible activity would require an invalidation
callback/acknowledgement and would recreate this handshake.

### Target change during admission

The request path captures the target before invalidation, pauses in the
admission gap, then dispatches the request with `appliedTo`
(`src/live.ts:284-295`). If the target changes, the reducer emits a retarget
command (`src/live-machine.ts:193-212`), and `execute` applies it to the new
attempt (`src/live.ts:129-151`). This concurrent invalidation must stay
outside the sequential activity command path. The retained regression in
commit `46b3695` (cherry-picked from `2278b9e`) proves that omitting retarget
causes restart output `Original` instead of the required `Changed!`; its
isolated mutant was red on that exact assertion.

### Commit followed by trailing traversal failure

The later pass sets `committed = true` immediately after freshness commit and
before the trailing scan (`src/live.ts:230-236`). A trailing scan failure
therefore reports `discharged: true` (`src/live.ts:251-260`), and the reducer
does not re-owe that payload (`src/live-machine.ts:311-315`). The initial
trailing scan is different: it belongs to opening and maps to `openFail`, so
the opening can fail after publication and commit (`src/live.ts:197-205`). A
complete result must preserve this distinction and commit progress, not just
return a generic error.

### Cleanup failure after commit

The pass body closes its child scope only when the scheduler is complete. A
failed or stopped scheduler defers that scope to attempt close, because a
handler can still be using its resources (`src/live.ts:238-247`). Attempt
close waits for the activity, closes the attempt scope, and combines a close
failure with the activity cause (`src/live.ts:268-281`). Moving scope close
into a lexical activity module either releases resources too early or requires
the module to accept the same parent scope, status check, and cleanup
acknowledgements.

### Handler still using pass resources

`entry.passes` is forked during initial declaration before scheduler workers
start (`src/live.ts:185-190`). On later failure, finalization stops activity
workers before the attempt scope releases the pass resource
(`src/live.ts:298-305`). Existing runtime tests assert handler-stop before
resource-release in `test/live.test.ts:1831-1908`. This lifetime boundary is
session/attempt ownership, not a self-contained pass activity.

## Result

The step-1 `InternalOpening` is retained unchanged. No new production
abstraction is landed. The independently calibrated retarget regression is
retained as behavior-preservation evidence, not presented as a missing
baseline behavior or a first test lock. Existing public tests remain the
behavior owners for publication, freshness, settlement, and resource
lifetime.
