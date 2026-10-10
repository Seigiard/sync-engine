import { Data, Effect } from "effect";
import { lstat, readdir } from "node:fs/promises";
import { join } from "node:path";
import type { FreshnessOptions, WorkInput } from "./freshness.ts";
import type { WorkOptions, WorkScheduler } from "./work.ts";

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
  /**
   * Runs once per initial pass, after the declared minimum finished without errors and before the remaining work starts.
   * A live session runs an initial pass on every opening, so a reopen after a failed attempt runs it again.
   */
  readonly onMinimum?: Effect.Effect<void, E, R>;
  readonly freshness?: FreshnessOptions<W>;
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
