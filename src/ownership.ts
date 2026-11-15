import { Data, Effect } from "effect";
import { createHash } from "node:crypto";
import { mkdir, realpath } from "node:fs/promises";
import { dirname, join, relative, resolve } from "node:path";

export class OutputOwnershipFailed extends Data.TaggedError("OutputOwnershipFailed")<{
  readonly path: string;
  readonly cause: unknown;
}> {}

export interface OutputIdentity {
  readonly outputPath: string;
  readonly statePath: string;
}

/** Bookkeeping is outside application projections, keyed by canonical output identity. */
export function engineStatePath(outputPath: string, statePath?: string): Effect.Effect<string, OutputOwnershipFailed> {
  return resolveOutputIdentity(outputPath, statePath).pipe(Effect.map((identity) => identity.statePath));
}

export function resolveOutputIdentity(outputPath: string, statePath?: string): Effect.Effect<OutputIdentity, OutputOwnershipFailed> {
  return Effect.tryPromise({
    try: async () => {
      const canonical = await canonicalDestination(resolve(outputPath));
      if (statePath !== undefined) {
        const state = await canonicalDestination(resolve(statePath));
        if (state === canonical) throw new Error("State area must be distinct from the output root");
        return { outputPath: canonical, statePath: state };
      }
      const key = createHash("sha256").update(canonical).digest("hex");
      return { outputPath: canonical, statePath: join(dirname(canonical), `.sync-engine-state-${key}`) };
    },
    catch: (cause) => new OutputOwnershipFailed({ path: outputPath, cause }),
  }).pipe(Effect.uninterruptible);
}

/** Linux lease: persistent inode in a separate state area; EOF releases an owned child. */
export function acquireOutputTree(outputPath: string, statePath?: string): Effect.Effect<() => Promise<void>, OutputOwnershipFailed> {
  return resolveOutputIdentity(outputPath, statePath).pipe(Effect.flatMap(acquireResolvedOutputTree));
}

export function acquireResolvedOutputTree(identity: OutputIdentity): Effect.Effect<() => Promise<void>, OutputOwnershipFailed> {
  return Effect.gen(function* () {
    yield* Effect.tryPromise({ try: () => mkdir(identity.outputPath, { recursive: true }), catch: (cause) => new OutputOwnershipFailed({ path: identity.outputPath, cause }) }).pipe(Effect.uninterruptible);
    return yield* Effect.tryPromise({
      try: async () => {
        await mkdir(identity.statePath, { recursive: true });
        const holder = Bun.spawn([
          "flock", "-w", "1", "-E", "73", join(identity.statePath, "lock"),
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
      catch: (cause) => new OutputOwnershipFailed({ path: identity.outputPath, cause }),
    }).pipe(Effect.uninterruptible);
  }).pipe(Effect.uninterruptible);
}

export async function canonicalDestination(path: string): Promise<string> {
  try {
    return await realpath(path);
  } catch (cause) {
    if (!(cause instanceof Error) || !("code" in cause) || cause.code !== "ENOENT") throw cause;
    const parent = dirname(path);
    if (parent === path) throw cause;
    return join(await canonicalDestination(parent), relative(parent, path));
  }
}
