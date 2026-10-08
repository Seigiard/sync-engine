import { Data, Effect, type Scope } from "effect";
import { createWorkScheduler, type WorkOptions, type WorkScheduler } from "./work.ts";
export { createWorkScheduler, type WorkOptions, type WorkScheduler, type WorkStatus } from "./work.ts";
import { lstat, mkdir, readdir, realpath } from "node:fs/promises";
import { dirname, join, relative, resolve, sep } from "node:path";

export interface SourceEntry {
  readonly path: string;
  readonly kind: "file" | "directory";
  readonly size: number;
  readonly mtimeMs: number;
}

export interface InitialPlan<W, E, R> {
  readonly work: readonly W[];
  /** Runs after all initial work and its required cascades succeed. */
  readonly publish: Effect.Effect<void, E, R>;
}

export interface InitialPass<W, E, R> extends WorkOptions<W, E, R> {
  readonly sourcePath: string;
  readonly outputPath: string;
  readonly declare: (entries: readonly SourceEntry[]) => Effect.Effect<InitialPlan<W, E, R>, E, R>;
}

export class ScanFailed extends Data.TaggedError("ScanFailed")<{
  readonly path: string;
  readonly cause: unknown;
}> {}

export class OutputOwnershipFailed extends Data.TaggedError("OutputOwnershipFailed")<{
  readonly path: string;
  readonly cause: unknown;
}> {}

/** A Linux advisory lease, shared with a consumer's legacy composition during migration. */
export function acquireOutputTree(outputPath: string): Effect.Effect<() => Promise<void>, OutputOwnershipFailed> {
  return Effect.tryPromise({
    try: async () => {
      await mkdir(outputPath, { recursive: true });
      const canonical = await realpath(outputPath);
      // The persistent lock inode must never be unlinked while owners can acquire it.
      // EOF releases the lock even if Bun is killed; the holder inherits only the read end.
      const holder = Bun.spawn([
        "flock", "-w", "1", "-E", "73", join(canonical, ".sync-engine.lock"),
        "sh", "-c", "printf 'locked\\n'; read -r _ || :",
      ], { stdin: "pipe", stdout: "pipe", stderr: "ignore" });
      const reader = holder.stdout.getReader();
      let released: Promise<void> | undefined;
      const release = () => released ??= (async () => {
        await holder.stdin.end();
        const exit = await holder.exited;

        if (exit !== 0) throw new Error(`Output lock holder exited with ${exit}`);
      })();

      try {
        const ready = await reader.read();

        if (ready.done || new TextDecoder().decode(ready.value) !== "locked\n") {
          await holder.exited;
          throw new Error(`Output is owned or lock acquisition failed (exit ${holder.exitCode})`);
        }

        return release;
      } catch (cause) {
        await holder.stdin.end();
        await holder.exited;
        throw cause;
      } finally {
        reader.releaseLock();
      }
    },
    catch: (cause) => new OutputOwnershipFailed({ path: outputPath, cause }),
  }).pipe(Effect.uninterruptible);
}

function scanSource(sourcePath: string): Effect.Effect<readonly SourceEntry[], ScanFailed> {
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

async function canonicalDestination(path: string): Promise<string> {
  try {
    return await realpath(path);
  } catch (cause) {
    if (!(cause instanceof Error) || !("code" in cause) || cause.code !== "ENOENT") throw cause;
    const parent = dirname(path);

    if (parent === path) throw cause;

    return join(await canonicalDestination(parent), relative(parent, path));
  }
}

/** One initial pass; all returned cascades are required before final publication. */
export function runInitialPass<W, E, R>(options: InitialPass<W, E, R>): Effect.Effect<void, E | ScanFailed | OutputOwnershipFailed, R> {
  return Effect.scoped(openSynchronization(options).pipe(Effect.asVoid));
}

/** Completes initial publication, then keeps the lease and scheduler in the caller's scope. */
export function openSynchronization<W, E, R>(options: InitialPass<W, E, R>): Effect.Effect<WorkScheduler<W, E>, E | ScanFailed | OutputOwnershipFailed, R | Scope.Scope> {
  return Effect.gen(function* () {
    // Validate before creating output directories, including aliases through existing symlinks.
    const source = yield* Effect.tryPromise({
      try: () => realpath(options.sourcePath),
      catch: (cause) => new ScanFailed({ path: options.sourcePath, cause }),
    }).pipe(Effect.uninterruptible);

    yield* Effect.tryPromise({
      try: async () => {
        const output = await canonicalDestination(resolve(options.outputPath));
        const insideSource = relative(source, output);

        const sourceInsideOutput = relative(output, source);
        const isWithin = (path: string) => path === "" || (!path.startsWith(`..${sep}`) && path !== ".." && !path.startsWith(sep));

        if (isWithin(insideSource) || isWithin(sourceInsideOutput)) {
          throw new Error("Source and output trees must be disjoint");
        }
      },
      catch: (cause) => new OutputOwnershipFailed({ path: options.outputPath, cause }),
    }).pipe(Effect.uninterruptible);

    yield* Effect.acquireRelease(acquireOutputTree(options.outputPath), (release) => Effect.promise(release));
    const entries = yield* scanSource(options.sourcePath);
    const plan = yield* options.declare(entries);
    const scheduler = yield* createWorkScheduler(options);
    yield* scheduler.submit(plan.work);
    yield* scheduler.awaitCompletion;
    yield* plan.publish;
    return scheduler;
  });
}
