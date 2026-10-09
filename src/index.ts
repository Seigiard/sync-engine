import { Data, Effect, type Scope } from "effect";
import { acquireOutputTree, canonicalDestination, engineStatePath, OutputOwnershipFailed } from "./ownership.ts";
export { acquireOutputTree, engineStatePath, OutputOwnershipFailed } from "./ownership.ts";
import { createWorkScheduler, type WorkOptions, type WorkScheduler } from "./work.ts";
export { observeSourcePath, readSourceDirectory, removeAssociatedOutputs, nativeSourceFileSystem, SourceObservationFailed, OutputCleanupFailed, type SourceFileSystem, type SourceObservation, type AssociatedOutputs } from "./source.ts";
export { createWorkScheduler, type WorkOptions, type WorkScheduler, type WorkStatus, type WorkFailure } from "./work.ts";
export { openLiveSynchronization, startLiveSynchronization, type LiveOptions, type LiveSynchronization, type LiveHandle, type PassRequest, type PassAdmission, type LiveStatus, type Availability } from "./live.ts";
import { lstat, readdir, realpath } from "node:fs/promises";
import { join, relative, resolve, sep } from "node:path";
import { openFreshness, type FreshnessOptions, type FreshnessFailed, type WorkInput } from "./freshness.ts";
export { openFreshness, FreshnessFailed, type FreshnessOptions, type ResultDescriptor, type WorkInput } from "./freshness.ts";

export interface SourceEntry {
  readonly path: string;
  readonly kind: "file" | "directory";
  readonly size: number;
  readonly mtimeMs: number;
}

export interface InitialPlan<W, E, R> {
  /**
   * Application-defined minimum: runs first. A typed failure is reported after the minimum drains
   * and before the remaining work starts.
   * It is not repeated by later passes; list work that must also run again in `work`.
   */
  readonly minimum?: readonly W[];
  /** The remaining required work. It gates completion, not the minimum. */
  readonly work: readonly W[];
  /** Runs after all initial work and its required cascades succeed. Later live passes still publish after typed work failures. */
  readonly publish: Effect.Effect<void, E, R>;
}

export interface InitialPass<W, E, R> extends WorkOptions<W, E, R> {
  readonly sourcePath: string;
  readonly outputPath: string;
  readonly statePath?: string;
  /** Application-owned source policy. Excluded paths are skipped before filesystem traversal. */
  readonly includeSource?: (relativePath: string) => boolean;
  readonly declare: (entries: readonly SourceEntry[]) => Effect.Effect<InitialPlan<W, E, R>, E, R>;
  /** Runs once, after the declared minimum finished without errors and before the remaining work starts. */
  readonly onMinimum?: Effect.Effect<void, E, R>;
  readonly freshness?: FreshnessOptions<W>;
}

interface OpenSynchronizationInternal<W, E, R> {
  readonly beforeCommit?: (synchronization: Synchronization<W, E | FreshnessFailed>) => Effect.Effect<void, E | FreshnessFailed, R>;
}

export interface Synchronization<W, E> extends WorkScheduler<W, E> {
  readonly submit: (work: readonly W[], input?: WorkInput) => Effect.Effect<void, E>;
}

export class ScanFailed extends Data.TaggedError("ScanFailed")<{
  readonly path: string;
  readonly cause: unknown;
}> {}

export function scanSource(sourcePath: string, includeSource?: (relativePath: string) => boolean): Effect.Effect<readonly SourceEntry[], ScanFailed> {
  const read = <A>(path: string, run: () => Promise<A>) => Effect.tryPromise({
    try: run,
    catch: (cause) => new ScanFailed({ path, cause }),
  }).pipe(Effect.uninterruptible);

  return Effect.gen(function* () {
    const root = yield* read(sourcePath, () => lstat(sourcePath));

    if (!root.isDirectory()) return yield* Effect.fail(new ScanFailed({ path: sourcePath, cause: "Source root must be a directory" }));
    const entries: SourceEntry[] = [];
    const folders = [""];

    while (folders.length > 0) {
      const folder = folders.pop()!;
      const absolute = join(sourcePath, folder);
      const names = yield* read(absolute, () => readdir(absolute));
      names.sort();

      for (const name of names) {
        const path = join(folder, name);
        if (includeSource !== undefined && !includeSource(path)) continue;
        const absolutePath = join(sourcePath, path);
        const info = yield* read(absolutePath, () => lstat(absolutePath));

        if (!info.isFile() && !info.isDirectory()) continue;
        entries.push({ path, kind: info.isDirectory() ? "directory" : "file", size: info.size, mtimeMs: info.mtimeMs });

        if (info.isDirectory()) folders.push(path);
      }
    }

    return entries;
  });
}

/** One initial pass; all returned cascades are required before final publication. */
export function runInitialPass<W, E, R>(options: InitialPass<W, E, R>): Effect.Effect<void, E | FreshnessFailed | ScanFailed | OutputOwnershipFailed, R> {
  return Effect.scoped(openSynchronization(options).pipe(Effect.asVoid));
}

/** Completes initial publication, then keeps the lease and scheduler in the caller's scope. */
export function openSynchronization<W, E, R>(options: InitialPass<W, E, R>, internal?: OpenSynchronizationInternal<W, E, R>): Effect.Effect<Synchronization<W, E | FreshnessFailed>, E | FreshnessFailed | ScanFailed | OutputOwnershipFailed, R | Scope.Scope> {
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
    yield* Effect.addFinalizer(() => Effect.sync(() => { closed = true; }));
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
    const synchronization: Synchronization<W, E | FreshnessFailed> = {
      ...scheduler,
      submit: (work: readonly W[], input?: WorkInput) => Effect.suspend(() => closed ? Effect.interrupt : (input === undefined ? freshness.invalidateWork(work) : freshness.invalidate(input)).pipe(Effect.andThen(scheduler.submit(work)))),
      awaitCompletion: Effect.suspend(() => closed ? Effect.interrupt : scheduler.awaitCompletion.pipe(Effect.andThen(Effect.suspend(() => closed ? Effect.interrupt : freshness.commit)))),
    };
    yield* plan.publish;
    if (internal?.beforeCommit) yield* internal.beforeCommit(synchronization);
    yield* freshness.commit;
    return synchronization;
  });
}
