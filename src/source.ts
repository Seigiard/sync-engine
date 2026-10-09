import { Data, Effect } from "effect";
import { lstat, readdir, realpath, rm } from "node:fs/promises";
import { dirname, isAbsolute, join, relative, resolve, sep } from "node:path";
import type { SourceEntry } from "./index.ts";
import { canonicalDestination } from "./ownership.ts";

export interface SourceStat {
  readonly size: number;
  readonly mtimeMs: number;
  readonly isFile: () => boolean;
  readonly isDirectory: () => boolean;
  readonly isSymbolicLink: () => boolean;
}

/** Injectable external filesystem boundary. Production uses native read-only operations. */
export interface SourceFileSystem {
  readonly lstat: (path: string) => Promise<SourceStat>;
  readonly readdir: (path: string) => Promise<string[]>;
}

export const nativeSourceFileSystem: SourceFileSystem = { lstat, readdir };

export type SourceObservation =
  | { readonly state: "present"; readonly entry: SourceEntry }
  | { readonly state: "absent" };

export class SourceObservationFailed extends Data.TaggedError("SourceObservationFailed")<{
  readonly path: string;
  readonly cause: unknown;
  readonly message: string;
}> {}

export class OutputCleanupFailed extends Data.TaggedError("OutputCleanupFailed")<{
  readonly path: string;
  readonly cause: unknown;
  readonly message: string;
}> {}

function confinedPath(root: string, path: string, allowRoot = false): string {
  if (isAbsolute(path) || path.split(sep).includes("..")) throw new Error("Expected a confined relative path");
  const absolute = resolve(root, path);
  const within = relative(resolve(root), absolute);
  if ((!allowRoot && within === "") || within === ".." || within.startsWith(`..${sep}`) || isAbsolute(within)) throw new Error("Path leaves its owned tree");
  return absolute;
}

const isMissing = (cause: unknown) => cause instanceof Error && "code" in cause && cause.code === "ENOENT";
const message = (cause: unknown) => cause instanceof Error ? cause.message : String(cause);

/** Confirm ancestors and root authority; a missing/unreadable root never means mass deletion. */
export function observeSourcePath(sourcePath: string, path: string, fs: SourceFileSystem = nativeSourceFileSystem): Effect.Effect<SourceObservation, SourceObservationFailed> {
  return Effect.tryPromise({
    try: async (): Promise<SourceObservation> => {
      const target = confinedPath(sourcePath, path, true);
      const root = resolve(sourcePath);
      const rootInfo = await fs.lstat(root);
      if (!rootInfo.isDirectory() || rootInfo.isSymbolicLink()) throw new Error("Source root must be a directory");
      const confirmRoot = async () => {
        const confirmedRoot = await fs.lstat(root);
        if (!confirmedRoot.isDirectory() || confirmedRoot.isSymbolicLink()) throw new Error("Source root is unavailable");
      };
      const components = relative(root, target).split(sep).filter(Boolean);
      let current = root;
      let info = rootInfo;
      for (const component of components) {
        const parent = current;
        current = join(current, component);
        try {
          info = await fs.lstat(current);
        } catch (cause) {
          if (!isMissing(cause)) throw cause;
          // A failed/incomplete directory read is not proof of removal.
          if ((await fs.readdir(parent)).includes(component)) throw cause;
          await confirmRoot();
          return { state: "absent" };
        }
        if (info.isSymbolicLink() || (!info.isFile() && !info.isDirectory())) throw new Error("Unsupported source path");
        if (current !== target && info.isFile()) {
          await confirmRoot();
          return { state: "absent" };
        }
      }
      return { state: "present", entry: { path: relative(root, target), kind: info.isDirectory() ? "directory" : "file", size: info.size, mtimeMs: info.mtimeMs } };
    },
    catch: (cause) => new SourceObservationFailed({ path: join(sourcePath, path), cause, message: message(cause) }),
  }).pipe(Effect.uninterruptible);
}

/** Read errors stay failures, including ENOENT after a prior successful observation. */
export function readSourceDirectory(sourcePath: string, path: string, fs: SourceFileSystem = nativeSourceFileSystem): Effect.Effect<readonly string[], SourceObservationFailed> {
  return observeSourcePath(sourcePath, path, fs).pipe(Effect.flatMap((observation) => {
    if (observation.state === "absent" || observation.entry.kind !== "directory") return Effect.fail(new SourceObservationFailed({ path: join(sourcePath, path), cause: "Not an observable source directory", message: "Not an observable source directory" }));
    return Effect.tryPromise({
      try: () => fs.readdir(join(sourcePath, path)),
      catch: (cause) => new SourceObservationFailed({ path: join(sourcePath, path), cause, message: message(cause) }),
    }).pipe(Effect.uninterruptible);
  }));
}

export interface AssociatedOutputs {
  readonly sourcePath: string;
  readonly outputPath: string;
  readonly statePath?: string;
  readonly sourceRelativePath: string;
  /** Application-owned projection, relative to the dedicated derived-data area. */
  readonly outputs: readonly string[];
}

/** Stale hints do nothing. Validate the whole declaration before removing any output. */
export function removeAssociatedOutputs(options: AssociatedOutputs, fs: SourceFileSystem = nativeSourceFileSystem): Effect.Effect<boolean, SourceObservationFailed | OutputCleanupFailed> {
  return Effect.gen(function* () {
    const observation = yield* observeSourcePath(options.sourcePath, options.sourceRelativePath, fs);
    if (observation.state === "present") return false;
    return yield* Effect.tryPromise({
      try: async () => {
        const root = await realpath(options.outputPath);
        const state = options.statePath === undefined ? undefined : await canonicalDestination(resolve(options.statePath));
        const paths: string[] = [];
        for (const output of options.outputs) {
          const path = confinedPath(root, output);
          if (state !== undefined) {
            const overlaps = (a: string, b: string) => { const within = relative(a, b); return within === "" || (!within.startsWith(`..${sep}`) && within !== ".." && !isAbsolute(within)); };
            if (overlaps(path, state) || overlaps(state, path)) throw new Error("Cleanup overlaps engine state");
          }
          // Check every existing ancestor, not just lexical containment. Never traverse an alias.
          let current = root;
          for (const component of relative(root, dirname(path)).split(sep).filter(Boolean)) {
            current = join(current, component);
            try {
              const info = await lstat(current);
              if (info.isSymbolicLink() || !info.isDirectory()) throw new Error("Output ancestor is not an owned directory");
            } catch (cause) {
              if (!isMissing(cause)) throw cause;
              break;
            }
          }
          paths.push(path);
        }
        for (const path of paths) await rm(path, { recursive: true, force: true });
        return true;
      },
      catch: (cause) => new OutputCleanupFailed({ path: options.outputPath, cause, message: message(cause) }),
    }).pipe(Effect.uninterruptible);
  });
}
