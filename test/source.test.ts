import { test, expect } from "bun:test";
import { lstat, mkdir, mkdtemp, readFile, readdir, rm, symlink } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Effect, Exit } from "effect";
import { engineStatePath, observeSourcePath, openSynchronization, readSourceDirectory, removeAssociatedOutputs } from "../src/index.ts";

test("an incomplete source observation retains results but confirmed removal permits associated cleanup", async () => {
  // #given real trees and an external directory-read fault
  const root = await mkdtemp(join(tmpdir(), "sync-engine-source-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(join(sourcePath, "folder"), { recursive: true });
  await mkdir(join(outputPath, "folder"), { recursive: true });
  await Bun.write(join(outputPath, "folder", "result"), "Previous result");
  const fault = { lstat, readdir: async (_path: string): Promise<string[]> => { throw Object.assign(new Error("Unreadable folder"), { code: "EACCES" }); } };
  try {
    // #when a read fails and cleanup later receives independently confirmed absence
    const failedRead = await Effect.runPromiseExit(readSourceDirectory(sourcePath, "folder", fault));
    const stale = await Effect.runPromise(removeAssociatedOutputs({ sourcePath, outputPath, sourceRelativePath: "folder", outputs: ["folder"] }));
    const retained = await readFile(join(outputPath, "folder", "result"), "utf8");
    await rm(join(sourcePath, "folder"), { recursive: true });
    const uncertain = await Effect.runPromiseExit(removeAssociatedOutputs({ sourcePath, outputPath, sourceRelativePath: "folder", outputs: ["folder"] }, fault));
    const stillRetained = await readFile(join(outputPath, "folder", "result"), "utf8");
    const absent = await Effect.runPromise(observeSourcePath(sourcePath, "folder"));
    const removed = await Effect.runPromise(removeAssociatedOutputs({ sourcePath, outputPath, sourceRelativePath: "folder", outputs: ["folder"] }));
    // #then only a successful observation authorizes deletion
    expect({ failedRead: Exit.isFailure(failedRead), stale, retained, uncertain: Exit.isFailure(uncertain), stillRetained, absent: absent.state, removed, exists: await Bun.file(join(outputPath, "folder", "result")).exists() }).toEqual({ failedRead: true, stale: false, retained: "Previous result", uncertain: true, stillRetained: "Previous result", absent: "absent", removed: true, exists: false });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("a regular file ancestor confirms descendant absence for associated cleanup", async () => {
  // #given output for a source descendant whose parent path is now a regular file
  const root = await mkdtemp(join(tmpdir(), "sync-engine-source-file-ancestor-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  await mkdir(join(outputPath, "folder"), { recursive: true });
  await Bun.write(join(sourcePath, "folder"), "not a directory");
  await Bun.write(join(outputPath, "folder", "book"), "Previous result");
  try {
    // #when cleanup observes folder/book through the regular-file ancestor folder
    const observation = await Effect.runPromise(observeSourcePath(sourcePath, "folder/book"));
    const removed = await Effect.runPromise(removeAssociatedOutputs({ sourcePath, outputPath, sourceRelativePath: "folder/book", outputs: ["folder/book"] }));
    // #then the descendant is confirmed absent and stale output can be removed
    expect({ state: observation.state, removed, exists: await Bun.file(join(outputPath, "folder", "book")).exists() }).toEqual({ state: "absent", removed: true, exists: false });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("cleanup rejects traversal, root removal and symlink ancestors without touching unrelated data", async () => {
  // #given user data outside the dedicated output and an alias from inside it
  const root = await mkdtemp(join(tmpdir(), "sync-engine-confinement-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  await mkdir(outputPath);
  await mkdir(join(root, "user"));
  await Bun.write(join(root, "user", "keep"), "User data");
  await Bun.write(join(outputPath, "keep"), "Unrelated output");
  await symlink(join(root, "user"), join(outputPath, "alias"));
  try {
    // #when the public cleanup API receives unsafe application projections
    const failures = [];
    for (const outputs of [["../user"], [""], ["alias/keep"], [join(root, "user")]]) {
      failures.push(Exit.isFailure(await Effect.runPromiseExit(removeAssociatedOutputs({ sourcePath, outputPath, sourceRelativePath: "gone", outputs }))));
    }
    const missingRoot = await Effect.runPromiseExit(removeAssociatedOutputs({ sourcePath: join(root, "missing"), outputPath, sourceRelativePath: "gone", outputs: ["keep"] }));
    // #then neither incomplete authority nor unsafe declarations remove useful files
    expect({ failures, missingRoot: Exit.isFailure(missingRoot), user: await readFile(join(root, "user", "keep"), "utf8"), unrelated: await readFile(join(outputPath, "keep"), "utf8") }).toEqual({ failures: [true, true, true, true], missingRoot: true, user: "User data", unrelated: "Unrelated output" });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("a supported source directory can project to the old lease filename", async () => {
  // #given a legal source directory name that used to collide with bookkeeping
  const root = await mkdtemp(join(tmpdir(), "sync-engine-bookkeeping-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(join(sourcePath, ".sync-engine.lock"), { recursive: true });
  try {
    // #when the public engine retains output ownership during domain publication
    await Effect.runPromise(Effect.scoped(openSynchronization({
      sourcePath, outputPath,
      declare: () => Effect.succeed({ work: [".sync-engine.lock"], publish: Effect.void }),
      handle: (path: string) => Effect.tryPromise({
        try: async () => { await mkdir(join(outputPath, path)); await Bun.write(join(outputPath, path, "result"), "Projected result"); return []; },
        catch: (cause) => new Error(String(cause)),
      }).pipe(Effect.uninterruptible),
    })));
    // #then application output and engine bookkeeping have distinct locations
    expect(await readFile(join(outputPath, ".sync-engine.lock", "result"), "utf8")).toBe("Projected result");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("explicit in-output bookkeeping shares canonical aliases and is excluded from associated cleanup", async () => {
  // #given a dedicated state area and an alias to the same real output
  const root = await mkdtemp(join(tmpdir(), "sync-engine-explicit-state-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  const alias = join(root, "alias");
  const statePath = join(outputPath, ".sync-engine");
  await mkdir(sourcePath);
  await mkdir(statePath, { recursive: true });
  await symlink(outputPath, alias);
  await Bun.write(join(statePath, "saved"), "Persistent engine state");
  try {
    // #when an alias selects the state and cleanup attempts to remove its namespace
    const canonical = await Effect.runPromise(engineStatePath(alias, join(alias, ".sync-engine")));
    const cleanup = await Effect.runPromiseExit(removeAssociatedOutputs({ sourcePath, outputPath, statePath, sourceRelativePath: "gone", outputs: [".sync-engine"] }));
    // #then configuration preserves canonical identity and protects the state itself
    expect({ canonical, rejected: Exit.isFailure(cleanup), saved: await readFile(join(statePath, "saved"), "utf8") }).toEqual({ canonical: statePath, rejected: true, saved: "Persistent engine state" });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("a default state area overlapping the source root is rejected before source writes", async () => {
  // #given a legal source-root name that happens to equal the default external state area
  const root = await mkdtemp(join(tmpdir(), "sync-engine-state-source-"));
  const outputPath = join(root, "output");
  await mkdir(outputPath);
  const sourcePath = await Effect.runPromise(engineStatePath(outputPath));
  await mkdir(sourcePath);
  await Bun.write(join(sourcePath, "source.txt"), "Original source");
  try {
    // #when a public engine session tries to use that overlapping state layout
    const exit = await Effect.runPromiseExit(Effect.scoped(openSynchronization({ sourcePath, outputPath, declare: () => Effect.succeed({ work: [], publish: Effect.void }), handle: (_work: string) => Effect.succeed([]) })));
    // #then source authority remains read-only, including before a scan begins
    expect({ rejected: Exit.isFailure(exit), sourceNames: await readdir(sourcePath), bytes: await readFile(join(sourcePath, "source.txt"), "utf8") }).toEqual({ rejected: true, sourceNames: ["source.txt"], bytes: "Original source" });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
