import { Cause, Effect, Exit, Scope } from "effect";
import { scanSource, type InitialPlan, type ScanFailed, type SourceEntry, type Synchronization } from "./initial-pass.ts";
import type { FreshnessFailed } from "./freshness.ts";
import type { PassRequest } from "./live-machine.ts";

export interface LaterActivityOptions<W, E, R> {
  readonly baseline: readonly SourceEntry[];
  readonly sourcePath: string;
  readonly includeSource?: (relativePath: string) => boolean;
  readonly passParent: Scope.Closeable;
  readonly synchronization: Synchronization<W, E | FreshnessFailed>;
  readonly commitFreshness: Effect.Effect<void, E | FreshnessFailed>;
  readonly claim: (changes: readonly string[]) => Effect.Effect<PassRequest | undefined, never, R>;
  readonly declare: (entries: readonly SourceEntry[], request: PassRequest) => Effect.Effect<InitialPlan<W, E, R>, E, R>;
}

/**
 * Complete result of a later pass. `discharged` means the freshness commit
 * completed; `baseline` is the source snapshot advanced by that commit.
 */
export type LaterActivityResult<E> =
  | {
    readonly tag: "ok";
    readonly outcome: "complete" | "complete-with-errors";
    readonly baseline: readonly SourceEntry[];
    readonly changes: readonly string[];
  }
  | {
    readonly tag: "fail";
    readonly cause: Cause.Cause<E | FreshnessFailed | ScanFailed>;
    readonly endsAttempt: boolean;
    readonly discharged: boolean;
    readonly baseline: readonly SourceEntry[];
  };

export function differences(before: readonly SourceEntry[], after: readonly SourceEntry[]): string[] {
  const previous = new Map(before.map((entry) => [entry.path, entry]));
  const changed = new Set<string>();
  for (const entry of after) {
    const old = previous.get(entry.path);
    if (!old || old.kind !== entry.kind || (entry.kind === "file" && (old.size !== entry.size || old.mtimeMs !== entry.mtimeMs))) changed.add(entry.path);
    previous.delete(entry.path);
  }
  for (const path of previous.keys()) changed.add(path);
  return [...changed];
}

export function runLaterActivity<W, E, R>(options: LaterActivityOptions<W, E, R>): Effect.Effect<LaterActivityResult<E>, never, R> {
  return Effect.gen(function* () {
    // Set once the pass committed freshness: from then on its payload is applied, whatever fails afterwards.
    let committed = false;
    let baseline = options.baseline;
    const steps = Effect.gen(function* () {
      const entries = yield* scanSource(options.sourcePath, options.includeSource);
      const declared = yield* options.claim(differences(baseline, entries));
      // Only a stopped session refuses the claim; the pass then declares nothing.
      if (declared === undefined) return yield* Effect.interrupt;
      const plan = yield* options.declare(entries, declared);
      yield* options.synchronization.submit(plan.work, declared);
      yield* options.synchronization.awaitCompletion;
      yield* plan.publish;
      yield* options.commitFreshness;
      committed = true;
      baseline = entries;
      const outcome = (yield* options.synchronization.status).state === "complete-with-errors" ? "complete-with-errors" as const : "complete" as const;
      const after = yield* scanSource(options.sourcePath, options.includeSource);
      return { outcome, changes: differences(entries, after) };
    });
    const body = Effect.gen(function* () {
      const scope = yield* Scope.fork(options.passParent);
      const exit = yield* Effect.exit(steps.pipe(Scope.provide(scope)));
      // Only a drained scheduler proves that no worker still uses pass resources. Failed or stopped states may
      // still have workers awaiting cleanup, so defer release to attempt close; otherwise release the pass now.
      const status = yield* options.synchronization.status;
      if (status.state !== "complete" && status.state !== "complete-with-errors") return yield* exit;
      const closed = yield* Effect.exit(Scope.close(scope, exit));
      if (Exit.isSuccess(closed)) return yield* exit;
      return yield* Effect.failCause(Exit.isFailure(exit) ? Cause.combine(exit.cause, closed.cause) : closed.cause);
    });
    const exit = yield* Effect.exit(Effect.interruptible(body));
    if (Exit.isSuccess(exit)) return { tag: "ok", ...exit.value, baseline };

    const status = yield* options.synchronization.status;
    // Typed failures keep the attempt; a defect, an interruption or a failed scheduler ends it.
    const endsAttempt = exit.cause.reasons.some((reason) => reason._tag !== "Fail") || status.state === "failed";
    return {
      tag: "fail",
      cause: exit.cause as Cause.Cause<E | FreshnessFailed | ScanFailed>,
      endsAttempt,
      discharged: committed,
      baseline,
    };
  });
}
