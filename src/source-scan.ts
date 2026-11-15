import { Data, Effect } from "effect";
import { lstat, readdir } from "node:fs/promises";
import { join } from "node:path";
import type { SourceEntry } from "./source-snapshot.ts";

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
