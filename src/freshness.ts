import { createHash } from "node:crypto";
import { lstat, mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { isAbsolute, join, normalize, sep } from "node:path";
import { Data, Effect } from "effect";
import type { WorkOptions } from "./work.ts";

export interface ResultDescriptor {
  readonly sourcePaths: readonly string[];
  readonly resultKind: string;
  readonly processingVersion: string;
  readonly outputPaths: readonly string[];
}

export interface FreshnessOptions<W> {
  readonly describe: (work: W) => ResultDescriptor | undefined;
  /** Metadata is cheaper; content additionally reads and hashes regular files. */
  readonly check?: "metadata" | "content";
}

export interface WorkInput {
  readonly changedPaths?: readonly string[];
  readonly force?: boolean;
}

export class FreshnessFailed extends Data.TaggedError("FreshnessFailed")<{
  readonly cause: unknown;
}> {}

const io = <A>(run: () => Promise<A>) => Effect.tryPromise({ try: run, catch: (cause) => new FreshnessFailed({ cause }) }).pipe(Effect.uninterruptible);

function checkedPath(root: string, path: string) {
  const normalized = normalize(path);
  if (isAbsolute(path) || normalized === ".." || normalized.startsWith(`..${sep}`)) throw new Error(`Expected a tree-relative path: ${path}`);
  return join(root, path);
}

/** Compose under the output lease. Persistence describes completed work, never the queue. */
export function openFreshness<W, E, R>(
  options: WorkOptions<W, E, R> & { readonly sourcePath: string; readonly outputPath: string; readonly freshness?: FreshnessOptions<W> },
  statePath: string,
) {
  return Effect.gen(function* () {
    const records = new Map<string, string>();
    const candidates = new Map<string, { descriptor: ResultDescriptor; stamp: string; revision: number }>();
    const revisions = new Map<string, number>();
    let failed = false;
    const path = join(statePath, "freshness.json");

    if (options.freshness) yield* io(async () => {
      await mkdir(statePath, { recursive: true });
      let text: string;
      try { text = await readFile(path, "utf8"); }
      catch (cause) {
        if (cause instanceof Error && "code" in cause && cause.code === "ENOENT") return;
        throw cause;
      }
      const saved: unknown = JSON.parse(text);
      if (saved === null || typeof saved !== "object" || Array.isArray(saved)) throw new Error("Invalid freshness state");
      for (const [key, value] of Object.entries(saved)) {
        const identity: unknown = JSON.parse(key);
        if (!Array.isArray(identity) || typeof identity[0] !== "string" || !Array.isArray(identity[1]) || !identity[1].every((item: unknown) => typeof item === "string") || typeof value !== "string") throw new Error("Invalid freshness record");
        records.set(key, value);
      }
    });

    let saving = Promise.resolve();
    const save = () => {
      if (!options.freshness) return Promise.resolve();
      const text = JSON.stringify(Object.fromEntries(records));
      const next = saving.then(async () => {
        await writeFile(`${path}.tmp`, text);
        await rename(`${path}.tmp`, path);
      });
      saving = next.catch(() => {});
      return next;
    };
    const identity = (descriptor: ResultDescriptor) => JSON.stringify([descriptor.resultKind, descriptor.sourcePaths]);
    const invalidateKey = (key: string) => {
      records.delete(key);
      candidates.delete(key);
      revisions.set(key, (revisions.get(key) ?? 0) + 1);
    };
    const stamp = async (descriptor: ResultDescriptor) => {
      const sources = [];
      for (const source of descriptor.sourcePaths) {
        const absolute = checkedPath(options.sourcePath, source);
        let info;
        try { info = await lstat(absolute); }
        catch (cause) {
          // A vanished source differs from a prior present source. If a handler confirms absence successfully,
          // the absent stamp can be retained until the source returns.
          if (cause instanceof Error && "code" in cause && cause.code === "ENOENT") { sources.push([source, null, null, null]); continue; }
          throw cause;
        }
        if (!info.isFile() && !info.isDirectory()) throw new Error(`Unsupported freshness source: ${source}`);
        const digest = options.freshness?.check === "content" && info.isFile()
          ? createHash("sha256").update(await readFile(absolute)).digest("hex") : null;
        sources.push([source, info.size, info.mtimeMs, digest]);
      }
      return JSON.stringify([descriptor.processingVersion, options.freshness?.check ?? "metadata", sources]);
    };
    const outputsExist = async (descriptor: ResultDescriptor) => {
      for (const output of descriptor.outputPaths) {
        try { await lstat(checkedPath(options.outputPath, output)); }
        catch (cause) {
          if (cause instanceof Error && "code" in cause && cause.code === "ENOENT") return false;
          throw cause;
        }
      }
      return true;
    };

    return {
      invalidateWork: (work: readonly W[]) => io(async () => {
        for (const item of work) {
          const descriptor = options.freshness?.describe(item);
          if (descriptor) invalidateKey(identity(descriptor));
        }
        await save();
      }),
      invalidate: (input: WorkInput = {}) => io(async () => {
        for (const key of new Set([...records.keys(), ...candidates.keys(), ...revisions.keys()])) {
          const [, sources]: [string, string[]] = JSON.parse(key);
          if (input.force || input.changedPaths?.some((hint) => sources.some((source) => source === hint || source.startsWith(`${hint}/`)))) invalidateKey(key);
        }
        await save();
      }),
      handle: (work: W): Effect.Effect<readonly W[], E | FreshnessFailed, R> => Effect.gen(function* () {
        const descriptor = options.freshness?.describe(work);
        const key = descriptor && identity(descriptor);
        const before = descriptor && (yield* io(() => stamp(descriptor)));
        if (descriptor && key !== undefined && records.get(key) === before && (yield* io(() => outputsExist(descriptor)))) return [];
        if (key !== undefined) {
          invalidateKey(key);
          yield* io(save);
        }
        const revision = key === undefined ? 0 : revisions.get(key)!;
        const downstream = yield* options.handle(work);
        // A returned dependency must run even when its own source stamp is unchanged.
        for (const item of downstream) {
          const dependent = options.freshness?.describe(item);
          if (dependent) invalidateKey(identity(dependent));
        }
        yield* io(save);
        if (descriptor && key !== undefined && before !== undefined) {
          const after = yield* io(() => stamp(descriptor));
          const present = yield* io(() => outputsExist(descriptor));
          if (revision === revisions.get(key) && before === after && present) candidates.set(key, { descriptor, stamp: before, revision });
        }
        return downstream;
      }).pipe(Effect.onExit((exit) => Effect.sync(() => {
        if (exit._tag === "Failure") failed = true;
      }))),
      commit: io(async () => {
        if (failed) { candidates.clear(); failed = false; return; }
        for (const [key, candidate] of Array.from(candidates)) {
          const current = await stamp(candidate.descriptor);
          const present = await outputsExist(candidate.descriptor);
          // Admission can invalidate a result while these filesystem reads are pending.
          if (candidate.revision === revisions.get(key) && candidate.stamp === current && present) records.set(key, candidate.stamp);
          if (candidates.get(key) === candidate) candidates.delete(key);
        }
        await save();
      }),
    };
  });
}
