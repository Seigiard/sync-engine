import { Effect, type Scope } from "effect";
import { type OutputOwnershipFailed } from "./ownership.ts";
export { acquireOutputTree, engineStatePath, OutputOwnershipFailed } from "./ownership.ts";
export { observeSourcePath, readSourceDirectory, removeAssociatedOutputs, nativeSourceFileSystem, SourceObservationFailed, OutputCleanupFailed, type SourceFileSystem, type SourceObservation, type AssociatedOutputs } from "./source.ts";
export { createWorkScheduler, type WorkOptions, type WorkScheduler, type WorkStatus, type WorkFailure } from "./work.ts";
export { openLiveSynchronization, startLiveSynchronization, type LiveOptions, type LiveSynchronization, type LiveHandle, type PassRequest, type PassAdmission, type LiveStatus, type Availability } from "./live.ts";
import type { FreshnessFailed } from "./freshness.ts";
import { openSynchronizationInternal } from "./internal.ts";
import type { InitialPass, ScanFailed, Synchronization } from "./initial-pass.ts";
export { ScanFailed, scanSource, type InitialPass, type InitialPlan, type SourceEntry, type Synchronization } from "./initial-pass.ts";
export { openFreshness, FreshnessFailed, type FreshnessOptions, type ResultDescriptor, type WorkInput } from "./freshness.ts";

/** One initial pass; all returned cascades are required before final publication. */
export function runInitialPass<W, E, R>(options: InitialPass<W, E, R>): Effect.Effect<void, E | FreshnessFailed | ScanFailed | OutputOwnershipFailed, R> {
  return Effect.scoped(openSynchronization(options).pipe(Effect.asVoid));
}

/** Completes initial publication, then keeps the lease and scheduler in the caller's scope. */
export function openSynchronization<W, E, R>(options: InitialPass<W, E, R>): Effect.Effect<Synchronization<W, E | FreshnessFailed>, E | FreshnessFailed | ScanFailed | OutputOwnershipFailed, R | Scope.Scope> {
  return Effect.gen(function* () {
    const opening = yield* openSynchronizationInternal(options);
    yield* opening.commitFreshness;
    return {
      ...opening.synchronization,
      awaitCompletion: Effect.suspend(() => opening.synchronization.awaitCompletion.pipe(Effect.andThen(opening.commitFreshness))),
    };
  });
}
