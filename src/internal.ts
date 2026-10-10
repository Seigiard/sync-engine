import { Deferred, Effect, Exit, type Scope } from "effect";
import { realpath } from "node:fs/promises";
import { relative, resolve, sep } from "node:path";
import { acquireOutputTree, canonicalDestination, engineStatePath, OutputOwnershipFailed } from "./ownership.ts";
import { openFreshness, type FreshnessFailed } from "./freshness.ts";
import { scanSource, ScanFailed, type InitialPass, type Synchronization } from "./initial-pass.ts";
import { createWorkScheduler } from "./work.ts";

export interface InternalOpening<W, E> {
  /** Its awaitCompletion drains subsequent work without committing freshness. */
  readonly synchronization: Synchronization<W, E>;
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

    const statePath = yield* engineStatePath(options.outputPath, options.statePath);
    yield* Effect.tryPromise({
      try: async () => {
        const output = await canonicalDestination(resolve(options.outputPath));
        const insideSource = relative(source, output);

        const sourceInsideOutput = relative(output, source);
        const isWithin = (path: string) => path === "" || (!path.startsWith(`..${sep}`) && path !== ".." && !path.startsWith(sep));

        if (isWithin(insideSource) || isWithin(sourceInsideOutput)) {
          throw new Error("Source and output trees must be disjoint");
        }
        if (isWithin(relative(source, statePath)) || isWithin(relative(statePath, source))) throw new Error("Source and state trees must be disjoint");
      },
      catch: (cause) => new OutputOwnershipFailed({ path: options.outputPath, cause }),
    }).pipe(Effect.uninterruptible);

    yield* Effect.acquireRelease(acquireOutputTree(options.outputPath, options.statePath), (release) => Effect.promise(release));
    const entries = yield* scanSource(options.sourcePath, options.includeSource);
    const plan = yield* options.declare(entries);
    const freshness = yield* openFreshness(options, statePath);
    const scheduler = yield* createWorkScheduler({ ...options, handle: freshness.handle });
    let closed = false;
    let inFlightFreshness = 0;
    let freshnessIdle = Deferred.makeUnsafe<void>();
    Deferred.doneUnsafe(freshnessIdle, Effect.void);
    const trackFreshness = (effect: Effect.Effect<void, E | FreshnessFailed>) => {
      return Effect.uninterruptibleMask((restore) => Effect.gen(function* () {
        if (closed) return yield* Effect.interrupt;
        if (inFlightFreshness === 0) freshnessIdle = Deferred.makeUnsafe<void>();
        inFlightFreshness += 1;
        const exit = yield* Effect.exit(restore(effect));
        inFlightFreshness -= 1;
        if (inFlightFreshness === 0) Deferred.doneUnsafe(freshnessIdle, Effect.void);
        if (Exit.isFailure(exit)) return yield* Effect.failCause(exit.cause);
        return exit.value;
      }));
    };
    yield* Effect.addFinalizer(() => Effect.gen(function* () {
      closed = true;
      yield* Deferred.await(freshnessIdle);
    }));
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
    const commitFreshness = trackFreshness(freshness.commit);
    const synchronization: Synchronization<W, E | FreshnessFailed> = {
      ...scheduler,
      submit: (work, input) => Effect.suspend(() => closed ? Effect.interrupt : trackFreshness(input === undefined ? freshness.invalidateWork(work) : freshness.invalidate(input)).pipe(Effect.andThen(scheduler.submit(work)))),
      awaitCompletion: Effect.suspend(() => closed ? Effect.interrupt : scheduler.awaitCompletion),
    };
    yield* plan.publish;
    return { synchronization, commitFreshness };
  });
}
