# Proposal: deepen scheduler work accounting

Status: historical proposal; extraction deferred after consultation and a targeted test.
Baseline: `32b22cf`.

For the decision and subsequent test evidence, read [analysis and checked conclusions](scheduler-accounting-analysis.md). The hypothesis below records the proposal reviewed at the baseline; it is not an active implementation plan.

## Idea

Give one internal module ownership of the coupled work-accounting rules. Let the Effect execution code apply complete decisions instead of assembling transitions from separate changes to pending work, active work, keys, errors, and completion.

The public `WorkScheduler` interface is already small and deep. The opportunity is inside its implementation. The intended gain is locality of rules and leverage for deterministic verification, not fewer lines or a new public interface.

## Current evidence

Read `README.md:29–49` for the behavioral contract and `src/work.ts` for the implementation. The source anchors below refer to the baseline.

| Concern | Current owner |
| --- | --- |
| Pending, active, active keys, terminal state, errors, completion | `work.ts:34–44` |
| Latest keyed pending request replaces older equivalents | `work.ts:49–65` |
| Error identity and successful retry cleanup | `work.ts:67–71` |
| Runnable selection excludes a currently active key | `work.ts:78–86` |
| Drained completion and fatal failure | `work.ts:92–104` |
| Handler outcome, active release, errors, cascades, wake-up | `work.ts:106–147` |
| Shutdown and new submissions mutate the same state | `work.ts:150–179` |

The critical success transition is at `work.ts:119–136`: release active accounting, clear its key, clear its error, and admit returned required work before completion can be observed. A yield between those operations could expose a false drained state.

Historical changes justify looking here, but do not prove that extraction is needed. `63f4ac1` added keyed serialization. `d70a890` hardened scheduler behavior. `e72d344` and `9b3df31` fixed admission cost and large-queue handling. The current implementation is only 182 lines and has good file-level locality.

## Proposed ownership

The potential accounting module would own:

- pending replacement and its order relative to intervening work;
- eligibility of keyed and unkeyed work;
- the relationship between active work and active keys;
- independent failure identity, error replacement, and successful retry cleanup;
- admission of handler-returned required work as part of the outcome transition;
- suppression of late cascades after fatal failure or stop;
- the accounting facts that make work drained or terminal.

The Effect execution code would retain:

- handler execution and runtime cause handling;
- fibers, interruption, scope finalization, and joining;
- actual wake-up and waiter settlement;
- delivery of submissions and outcomes to the accounting owner.

Some responsibilities straddle the seam. Their ownership is an open design question: callback exceptions, cause classification, completion generations, and the atomic handoff from an accounting decision to Deferred settlement. Moving only half of any of these can create a second state machine.

No method names, types, file layout, immutable reducer, or event protocol are selected. A module can remain local to `work.ts`. An immutable copy of the whole queue per transition is not a requirement.

## What must disappear from the execution caller

The caller should no longer need to remember:

1. Which active collection and key collection to update together.
2. Which error record a retry clears.
3. Why releasing the last active item is insufficient when it returns cascades.
4. Why a late successful worker cannot revive work after a peer defect.
5. How pending replacement and active-key exclusion jointly select runnable work.

If the caller still sequences low-level helpers such as remove-active, clear-key, clear-error, enqueue, and test-drained, the proposal has not added depth.

## Deletion test and competing options

Delete the proposed module mentally. If coupled domain rules return to the runtime caller, the module earns its seam. If only a bag of helper functions returns, it was shallow.

Compare three options before committing to extraction:

1. Keep the current implementation and strengthen only the needed behavioral tests.
2. Concentrate full transitions locally within the existing scheduler module.
3. Give accounting an explicit internal interface and test it independently of Effect execution.

The strongest counterargument is that option 3 may add more interface obligations than it removes from a small, already-testable implementation. A compact caller is not enough: the combined accounting-plus-execution protocol must be simpler.

## Testability claim to challenge

The scheduler already has direct tests through `createWorkScheduler`. An accounting seam could make combinations of rules easier to enumerate without scheduling fibers:

- blocked keyed follow-up while another independent worker succeeds;
- typed failure followed by retry while an independent error remains;
- fatal outcome followed by a late successful outcome with cascades;
- duplicate unkeyed work alongside keyed replacement and returned work.

The independent oracle is the README behavior. A test that recomputes expected state with the same transition algorithm is not evidence. Keep real Effect tests for interruption, callback defects, joining, and waiter settlement: accounting checks cannot prove those runtime properties.

Useful preservation anchors in `test/work.test.ts`:

- `169–214`: a late worker cannot enqueue cascades after a peer defect;
- `216–347`: closed/failed admission, undefined work, and callback defects;
- `349–407`: keyed follow-up waits while different-key work starts;
- `409–490`: refreshed output and independent work after a typed failure;
- `492–534`: bounded key evaluation and a two-million-item pending queue.

The preceding architecture round ran 116 targeted tests across seven files, including all work tests, with zero failures. Docker lint and typecheck passed. Those results establish a baseline, not correctness of a future design. The unchanged live model and shared-mount check were not rerun.

## Constraints

- Preserve the public interface and README behavior unless a separate behavior change is approved.
- Preserve the accepted live design in `docs/ticket-4-activity-design.md`. This proposal concerns scheduler accounting, not another live refactor.
- Keep identity rules distinct: pending key, active-key exclusion, and failure key are not interchangeable.
- Preserve valid arbitrary work payloads, including undefined and null, duplicates, callback defects, and shutdown behavior.
- Preserve large-queue support and admission cost; do not turn compact in-place work into repeated full-state copying.
- Effect is the one real runtime adapter. Two caller compositions are not two adapters. Portability does not justify a pluggable seam here.
