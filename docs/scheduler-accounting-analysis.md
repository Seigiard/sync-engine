# Scheduler accounting: independent analysis

Date: 2026-10-10.
Baseline: `32b22cf`.
Proposal: [deepen scheduler work accounting](scheduler-accounting-proposal.md).

## Decision and follow-up

The user accepted deferring extraction after a targeted test passed through the current interface. Commit `f6d9109` adds `a successful retry clears its failure identity while preserving an independent error` to `test/work.test.ts`.

The test failed under two temporary mutations: omitted error cleanup and removal of all errors. With the original implementation restored, all 18 work tests, lint, and typecheck passed. This closes one test gap without demonstrating a production defect or a need for a new seam.

The consultation evidence below refers to the earlier baseline. Read the historical [proposal](scheduler-accounting-proposal.md) only when revisiting the extraction decision.

## Consultations

| Peer | Requested configuration | Verdict |
| --- | --- | --- |
| Claude Code | `--model fable --effort high` | **Defer**: a second interface is not yet justified by new behavioral leverage. Revisit when a new scheduler rule or an accounting defect supplies a concrete need. |
| OpenCode | `openai/gpt-6-astra`, build variant `high` | **Narrow**: first assess existing-interface tests, then concentrate complete transitions locally in `work.ts` if needed. |

Both received the same proposal and analysis rubric. They independently read the contract, implementation, tests, and relevant callers. Neither changed repository files or ran tests. Astra confirmed the resolved `build` variant as `high`; its override was scoped to the child process. Both consultations completed and their temporary agent tabs were closed.

Session references:

- Claude: `86563a51-26ac-4a5f-b61a-f2268d974839`.
- OpenCode: `ses_eda82ef43ffeMIBH27ylpd06VQ`.

## Shared findings

### There is useful responsibility to concentrate

The execution caller still coordinates active occurrences, active keys, error cleanup, cascade admission, and terminal-state handling. `src/work.ts:119–136` is the strongest example. These rules could form a deep internal module if its interface accepts full transitions and removes this sequencing knowledge from the caller.

Several details are already hidden: `enqueue`, `takeRunnable`, and `clearError` own their individual algorithms. Moving those helpers alone does not increase depth. The remaining opportunity concerns the relationship between their operations.

### The external module is already deep

`WorkScheduler` has a small interface and direct tests. The implementation is 182 lines, with useful file-level locality. There are no duplicate execution callers repeating its bookkeeping.

The new interface would introduce obligations of its own: callback failure behavior, outcome representation, synchronous handoff to Deferred settlement, and completion generation handling. A smaller worker loop is not enough evidence of net leverage.

### Effect must remain responsible for execution

Accounting checks cannot establish fiber joining, interruption, wake-up delivery, or waiter settlement. The runtime adapter and its integration tests remain necessary.

The seam must distinguish drained work from terminal admission and from joined handlers. In particular, `src/live-activity.ts:72–76` depends on the difference between drained and failed/stopped states when deciding whether pass resources can be released.

### Preserve identity and cost

Pending keys, captured active keys, failure keys, and individual unkeyed occurrences have different roles. A single generic identity would change behavior.

Callbacks can throw after a transition has partly mutated state. The new interface must account for this instead of assuming a total pure function. Caching keys at admission would also change callback evaluation points and memory use.

The two-million-item queue and key-evaluation checks are preservation evidence. They do not establish linear cost for the entire drain. A state-copying design needs its own justification and must not weaken those checks.

## Parent verification and qualifications

| Peer claim | Check and conclusion |
| --- | --- |
| Successful retry with an independent remaining error was not directly tested. | Confirmed at the baseline; this gap is now closed by `f6d9109`. README:35–36 supplies the independent contract. |
| Some non-completion checks use elapsed time. | Confirmed: `work.test.ts:104,152,267` use a 25 ms race; `198` polls active state. Replacing them requires a proven causal checkpoint, not merely an immediate status read. |
| All proposed combinations can use concurrency one. | This was too broad in Claude's opening summary. Scenarios about another active worker require concurrency and causal barriers; both peers' detailed reasoning recognizes this. A sequential failure/retry check can use one worker. |
| A local synchronous transition could protect the no-yield rule. | Plausible, not demonstrated by a patch. It can prevent a yield inside bookkeeping, but does not by itself prove atomic handoff to Deferred settlement. |
| An exhaustive accounting model would not be worthwhile. | Claude took this position; Astra left it conditional on a concrete enumeration benefit. This remains an open cost question, not an established fact. |

## When to revisit

Revisit when a new scheduling rule or a concrete accounting defect makes current verification costly. Compare a representative case through `createWorkScheduler` with a local owner of full synchronous transitions. Count added obligations as well as removed ones. A separately tested seam needs a demonstrated gain in combinations explored, diagnostics, or the cost of changing a real rule.

## Stop condition

Keep the current module if its interface supports stable checks for the named risks and the proposed seam still makes the execution caller coordinate bookkeeping or repair completion afterward. Also stop if the only gain is that the accounting implementation can now be tested separately, without a specific behavioral or maintenance benefit.
