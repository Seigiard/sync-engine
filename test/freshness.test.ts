import { test, expect } from "bun:test";
import { mkdir, mkdtemp, readFile, rm, stat, utimes } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Cause, Effect, Exit } from "effect";
import { openFreshness, openSynchronization, runInitialPass, type InitialPass } from "../src/index.ts";

const io = <A>(run: () => Promise<A>) => Effect.tryPromise({ try: run, catch: (cause) => new Error(String(cause)) }).pipe(Effect.uninterruptible);
const ancient = new Date("2020-01-01T00:00:00Z");

test("a new engine instance reuses a successfully published result without rewriting it", async () => {
  // #given an independently specified source and a real derived artifact
  const root = await mkdtemp(join(tmpdir(), "sync-freshness-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  await Bun.write(join(sourcePath, "note.txt"), "Original source");
  const options = {
    sourcePath, outputPath,
    declare: () => Effect.succeed({ work: ["note"], publish: Effect.void }),
    freshness: {
      describe: () => ({ sourcePaths: ["note.txt"], resultKind: "text", processingVersion: "v1", outputPaths: ["note"] }),
    },
    handle: () => io(async () => {
      await Bun.write(join(outputPath, "note"), `Published: ${await readFile(join(sourcePath, "note.txt"), "utf8")}`);
      return [] as string[];
    }),
  };

  try {
    await Effect.runPromise(runInitialPass(options));
    await utimes(join(outputPath, "note"), ancient, ancient);
    // #when a fresh scoped instance scans unchanged sources
    await Effect.runPromise(runInitialPass(options));
    // #then published content and its deliberately old timestamp survive
    expect({ text: await readFile(join(outputPath, "note"), "utf8"), mtime: (await stat(join(outputPath, "note"))).mtimeMs })
      .toEqual({ text: "Published: Original source", mtime: ancient.getTime() });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

async function textTree() {
  const root = await mkdtemp(join(tmpdir(), "sync-text-freshness-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  await Bun.write(join(sourcePath, "note.txt"), "Original source");
  const options = (version = "v1", check: "metadata" | "content" = "metadata"): InitialPass<string, Error, never> => ({
    sourcePath, outputPath,
    declare: () => Effect.succeed({ work: ["note"], publish: Effect.void }),
    freshness: { check, describe: () => ({ sourcePaths: ["note.txt"], resultKind: "text", processingVersion: version, outputPaths: ["note"] }) },
    handle: () => io(async () => {
      await Bun.write(join(outputPath, "note"), `Published: ${await readFile(join(sourcePath, "note.txt"), "utf8")}`);
      return [] as string[];
    }),
  });
  return { root, sourcePath, outputPath, options };
}

test.each(["size", "mtime", "version"])("an ordinary fresh instance rebuilds after a %s change", async (change) => {
  // #given a successfully retained real result with an independently old output timestamp
  const { root, sourcePath, outputPath, options } = await textTree();
  try {
    if (change === "size") await utimes(join(sourcePath, "note.txt"), ancient, ancient);
    await Effect.runPromise(runInitialPass(options()));
    await utimes(join(outputPath, "note"), ancient, ancient);
    // #when one applicable freshness component changes
    if (change === "size") {
      await Bun.write(join(sourcePath, "note.txt"), "Larger replacement source");
      await utimes(join(sourcePath, "note.txt"), ancient, ancient);
    }
    if (change === "mtime") await utimes(join(sourcePath, "note.txt"), ancient, ancient);
    await Effect.runPromise(runInitialPass(options(change === "version" ? "v2" : "v1")));
    // #then actual processing replaces the old output in place
    expect({ text: await readFile(join(outputPath, "note"), "utf8"), rebuilt: (await stat(join(outputPath, "note"))).mtimeMs !== ancient.getTime() })
      .toEqual({ text: change === "size" ? "Published: Larger replacement source" : "Published: Original source", rebuilt: true });
  } finally { await rm(root, { recursive: true, force: true }); }
});

test.each(["hint", "content"])("%s detection replaces content whose size and mtime are preserved", async (mode) => {
  // #given saved successful work and a source with an exact reproducible timestamp
  const { root, sourcePath, outputPath, options } = await textTree();
  const source = join(sourcePath, "note.txt");
  await utimes(source, ancient, ancient);
  const initial = options("v1", mode === "content" ? "content" : "metadata");
  try {
    await Effect.runPromise(runInitialPass(initial));
    const old = await stat(source);
    // #when a replacement preserves both default metadata fields
    await Bun.write(source, "Replaced source");
    await utimes(source, ancient, ancient);
    await Effect.runPromise(Effect.scoped(openSynchronization(initial).pipe(Effect.flatMap((session) =>
      mode === "hint" ? session.submit(["note"], { changedPaths: ["note.txt"] }).pipe(Effect.andThen(session.awaitCompletion)) : Effect.void,
    ))));
    const current = await stat(source);
    // #then the public output reflects the replacement and the metadata really matches
    expect({ text: await readFile(join(outputPath, "note"), "utf8"), sameSize: old.size === current.size, sameMtime: old.mtimeMs === current.mtimeMs })
      .toEqual({ text: "Published: Replaced source", sameSize: true, sameMtime: true });
  } finally { await rm(root, { recursive: true, force: true }); }
});

test("without a hint the default metadata check can hide an equal-stamp replacement", async () => {
  // #given saved metadata freshness and exact old source stamps
  const { root, sourcePath, outputPath, options } = await textTree();
  const source = join(sourcePath, "note.txt");
  await utimes(source, ancient, ancient);
  try {
    await Effect.runPromise(runInitialPass(options()));
    await utimes(join(outputPath, "note"), ancient, ancient);
    // #when content changes but metadata stays identical and no hint is delivered
    await Bun.write(source, "Replaced source");
    await utimes(source, ancient, ancient);
    await Effect.runPromise(runInitialPass(options()));
    // #then this documented detection limit leaves the previous real result unchanged
    expect({ text: await readFile(join(outputPath, "note"), "utf8"), mtime: (await stat(join(outputPath, "note"))).mtimeMs })
      .toEqual({ text: "Published: Original source", mtime: ancient.getTime() });
  } finally { await rm(root, { recursive: true, force: true }); }
});

test.each([
  { mode: "failure", outcome: "complete-with-errors", errors: 1 },
  { mode: "interruption", outcome: "interrupted", errors: null },
])("$mode after writing cannot mark unfinished work current on restart", async ({ mode, outcome, errors }) => {
  // #given prior success and equal-stamp replacement delivered through the public input
  const { root, sourcePath, outputPath, options } = await textTree();
  const source = join(sourcePath, "note.txt");
  await utimes(source, ancient, ancient);
  let entered = () => {};
  const written = new Promise<void>((resolve) => { entered = resolve; });
  const controller = new AbortController();
  try {
    await Effect.runPromise(runInitialPass(options()));
    const incomplete = {
      ...options(),
      handle: () => io(async () => {
        await Bun.write(join(outputPath, "note"), "Unfinished publication");
        entered();
      }).pipe(Effect.andThen(mode === "failure" ? Effect.fail(new Error("Cannot finish")) : Effect.never)),
    };
    // #when output is written but required work fails or gets interrupted before success
    const running = Effect.runPromiseExit(Effect.scoped(openSynchronization(incomplete).pipe(Effect.flatMap((session) =>
      io(async () => {
        await Bun.write(source, "Replaced source");
        await utimes(source, ancient, ancient);
      }).pipe(Effect.andThen(session.submit(["note"], { changedPaths: ["note.txt"] })), Effect.andThen(session.awaitCompletion), Effect.andThen(session.status)),
    ))), { signal: controller.signal });
    await written;
    if (mode === "interruption") controller.abort();
    const exit = await running;
    await Effect.runPromise(runInitialPass(options()));
    // #then a fresh engine discovers unfinished work and repairs its actual artifact
    expect({ outcome: Exit.isSuccess(exit) ? exit.value.state : Cause.hasInterruptsOnly(exit.cause) ? "interrupted" : "failed", errors: Exit.isSuccess(exit) ? exit.value.errors.length : null, text: await readFile(join(outputPath, "note"), "utf8") })
      .toEqual({ outcome, errors, text: "Published: Replaced source" });
  } finally { controller.abort(); await rm(root, { recursive: true, force: true }); }
});

test("a required dependent failure leaves the successful upstream eligible for replay", async () => {
  // #given an upstream text artifact whose final publication requires a dependent result
  const { root, outputPath, options } = await textTree();
  const descriptor = options().freshness!;
  const dependentOptions = (fail: boolean): InitialPass<string, Error, never> => ({
    ...options(),
    freshness: { ...descriptor, describe: (work) => work === "note" ? descriptor.describe(work) : undefined },
    handle: (work) => work === "note" ? options().handle(work).pipe(Effect.as(["dependent"]))
      : fail ? Effect.fail(new Error("Dependent failed")) : io(async () => {
        await Bun.write(join(outputPath, "dependent"), await readFile(join(outputPath, "note"), "utf8"));
        return [] as string[];
      }),
  });
  try {
    // #when downstream fails after upstream writing, then a fresh instance retries
    const failed = await Effect.runPromiseExit(runInitialPass(dependentOptions(true)));
    await utimes(join(outputPath, "note"), ancient, ancient);
    await Effect.runPromise(runInitialPass(dependentOptions(false)));
    // #then required publication exists and the upstream was replayed rather than cached
    expect({ failed: Exit.isFailure(failed), result: await readFile(join(outputPath, "dependent"), "utf8"), replayed: (await stat(join(outputPath, "note"))).mtimeMs !== ancient.getTime() })
      .toEqual({ failed: true, result: "Published: Original source", replayed: true });
  } finally { await rm(root, { recursive: true, force: true }); }
});

test("a source changed during processing is not recorded as a current result", async () => {
  // #given real work held after publishing the earlier source it actually read
  const { root, sourcePath, outputPath, options } = await textTree();
  let entered = () => {};
  let release = () => {};
  const published = new Promise<void>((resolve) => { entered = resolve; });
  const released = new Promise<void>((resolve) => { release = resolve; });
  try {
    const held = { ...options(), handle: () => options().handle("note").pipe(Effect.andThen(io(async () => {
      entered();
      await released;
      return [] as string[];
    }))) };
    // #when source mutation happens before work completion and a fresh instance later verifies
    const running = Effect.runPromise(runInitialPass(held));
    await published;
    await Bun.write(join(sourcePath, "note.txt"), "Later source");
    release();
    await running;
    const earlier = await readFile(join(outputPath, "note"), "utf8");
    await Effect.runPromise(runInitialPass(options()));
    // #then earlier work is available but cannot make the later source look processed
    expect({ earlier, repaired: await readFile(join(outputPath, "note"), "utf8") })
      .toEqual({ earlier: "Published: Original source", repaired: "Published: Later source" });
  } finally { release(); await rm(root, { recursive: true, force: true }); }
});

test("a hint during active equal-stamp work prevents an earlier read from becoming current", async () => {
  // #given a prior result and later work held after reading the real source
  const { root, sourcePath, outputPath, options } = await textTree();
  const source = join(sourcePath, "note.txt");
  await utimes(source, ancient, ancient);
  let entered = () => {};
  let release = () => {};
  const read = new Promise<void>((resolve) => { entered = resolve; });
  const released = new Promise<void>((resolve) => { release = resolve; });
  try {
    await Effect.runPromise(runInitialPass(options()));
    const held = { ...options(), handle: () => io(async () => {
      const text = await readFile(source, "utf8");
      entered();
      await released;
      await Bun.write(join(outputPath, "note"), `Published: ${text}`);
      return [] as string[];
    }) };
    // #when a public hint arrives during processing, even before later work is declared
    await Effect.runPromise(Effect.scoped(openSynchronization(held).pipe(Effect.flatMap((session) =>
      session.submit(["note"]).pipe(Effect.andThen(io(async () => {
        await read;
        await Bun.write(source, "Replaced source");
        await utimes(source, ancient, ancient);
      })), Effect.andThen(session.submit([], { changedPaths: ["note.txt"] })), Effect.andThen(Effect.sync(release)), Effect.andThen(session.awaitCompletion)),
    ))));
    await Effect.runPromise(runInitialPass(options()));
    // #then a fresh instance replays the dirty source instead of accepting the earlier read
    expect(await readFile(join(outputPath, "note"), "utf8")).toBe("Published: Replaced source");
  } finally { release(); await rm(root, { recursive: true, force: true }); }
});

test("work whose declared source disappeared reaches its handler instead of failing freshness", async () => {
  // #given a result recorded for a source that is then removed, with its output still present
  const { root, sourcePath, outputPath, options } = await textTree();
  const handled: string[] = [];
  try {
    await Effect.runPromise(runInitialPass(options()));
    await rm(join(sourcePath, "note.txt"));
    // #when a fresh instance declares the same work for the vanished source
    const outcome = await Effect.runPromise(runInitialPass({
      ...options(),
      handle: (work: string) => io(async () => { handled.push(work); await rm(join(outputPath, "note")); return [] as string[]; }),
    }).pipe(Effect.as("completed"), Effect.catchTag("FreshnessFailed", () => Effect.succeed("freshness failed"))));
    // #then absence is the handler's to confirm: it ran, and the pass did not fail on the missing stamp
    expect({ outcome, handled, output: await Bun.file(join(outputPath, "note")).exists() }).toEqual({ outcome: "completed", handled: ["note"], output: false });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("a failed freshness save does not poison later saves in the same session", async () => {
  // #given a retained-state writer whose state path fails once and then becomes writable again
  const { root, sourcePath, outputPath, options } = await textTree();
  const statePath = join(root, "state");
  try {
    // #when one save fails before the next real write succeeds
    const result = await Effect.runPromise(Effect.gen(function* () {
      const freshness = yield* openFreshness(options(), statePath);
      yield* freshness.handle("note");
      yield* freshness.commit;
      yield* io(async () => {
        await rm(statePath, { recursive: true, force: true });
        await Bun.write(statePath, "not a directory");
      });
      const failed = yield* freshness.invalidateWork(["note"]).pipe(Effect.as("saved"), Effect.catchTag("FreshnessFailed", () => Effect.succeed("failed")));
      yield* io(async () => {
        await rm(statePath, { force: true });
        await mkdir(statePath);
        await Bun.write(join(sourcePath, "note.txt"), "Recovered source");
      });
      yield* freshness.handle("note");
      yield* freshness.commit;
      return { failed, text: yield* io(() => readFile(join(outputPath, "note"), "utf8")) };
    }));
    // #then the later save is attempted and the recovered source is retained
    expect(result).toEqual({ failed: "failed", text: "Published: Recovered source" });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
