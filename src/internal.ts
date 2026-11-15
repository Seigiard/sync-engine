import { Effect, type Scope } from "effect";
import { realpath } from "node:fs/promises";
import { OutputOwnershipFailed } from "./ownership.ts";
import { openOwnedOutput } from "./owned-output.ts";
import { openFreshness, type FreshnessFailed } from "./freshness.ts";
import { scanSource, ScanFailed } from "./source-scan.ts";
import type { InitialPass, Synchronization } from "./initial-pass.ts";
import { createWorkScheduler } from "./work.ts";

export interface InternalOpening<W, E> {
  /** Its awaitCompletion drains subsequent work without committing freshness. */
  readonly synchronization: Synchronization<W, E>;
  readonly beginFreshnessBatch: Effect.Effect<void, E>;
  readonly commitFreshness: Effect.Effect<void, E>;
}

/** Acquires and publishes an opening without committing freshness. */
export function openSynchronizationInternal<W, E, R>(options: InitialPass<W, E, R>): Effect.Effect<InternalOpening<W, E | FreshnessFailed>, E | FreshnessFailed | ScanFailed | OutputOwnershipFailed, R | Scope.Scope> {
  return Effect.gen(function* () {
    // Validate before creating output directories, including aliases through existing symlinks.
    const source = yield* Effect.tryPromise({
      try: () => realpath(options.sourcePath),
      catch: (cause) => new ScanFailed({ path: options.sourcePath, cause }),
    }).pipe(Effect.uninterruptible);

    const owned = yield* openOwnedOutput(source, options.outputPath, options.statePath);
    const entries = yield* scanSource(options.sourcePath, options.includeSource);
    const plan = yield* options.declare(entries);
    const rawFreshness = yield* openFreshness({ ...options, outputPath: owned.outputPath }, owned.statePath);
    yield* Effect.addFinalizer(() => owned.close);
    const scheduler = yield* createWorkScheduler({ ...options, handle: (work) => owned.admit(rawFreshness.handle(work, owned.cleanup)) });
    if (plan.minimum !== undefined && plan.minimum.length > 0) {
      yield* scheduler.submit(plan.minimum);
      yield* scheduler.awaitCompletion;
      const prepared = yield* scheduler.status;
      if (prepared.errors.length > 0) return yield* Effect.failCause(prepared.errors[0]!.cause);
    }
    if (options.onMinimum) yield* options.onMinimum;
    yield* scheduler.submit(plan.work);
    yield* scheduler.awaitCompletion;
    const completed = yield* scheduler.status;
    if (completed.errors.length > 0) return yield* Effect.failCause(completed.errors[0]!.cause);
    const beginFreshnessBatch = owned.admit(rawFreshness.beginBatch);
    const commitFreshness = owned.admit(rawFreshness.commit);
    const synchronization: Synchronization<W, E | FreshnessFailed> = {
      ...scheduler,
      submit: (work, input) => owned.admit(Effect.gen(function* () {
        yield* input === undefined ? rawFreshness.invalidateWork(work) : rawFreshness.invalidate(input);
        yield* scheduler.submit(work);
      })),
      awaitCompletion: Effect.suspend(() => owned.check(scheduler.awaitCompletion)),
    };
    yield* plan.publish;
    return { synchronization, beginFreshnessBatch, commitFreshness };
  });
}
