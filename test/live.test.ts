import { test, expect } from "bun:test";
import { mkdir, mkdtemp, readFile, rm, utimes } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Cause, Effect, Exit } from "effect";
import { openLiveSynchronization, startLiveSynchronization } from "../src/index.ts";

const io = <A>(run: () => Promise<A>) => Effect.tryPromise({ try: run, catch: (cause) => new Error(String(cause)) }).pipe(Effect.uninterruptible);

function gate() {
  const { promise, resolve } = Promise.withResolvers<void>();
  return { promise, open: () => resolve() };
}

test("a source replaced during initial processing converges without a watcher notice", async () => {
  // #given a real source whose old bytes have been read at a held publication boundary
  const root = await mkdtemp(join(tmpdir(), "sync-engine-live-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  await Bun.write(join(sourcePath, "document"), "Earlier source");
  const entered = gate();
  const release = gate();
  let first = true;
  try {
    const running = Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const session = yield* openLiveSynchronization({
        sourcePath, outputPath,
        declare: (entries) => Effect.succeed({ work: entries.filter((entry) => entry.kind === "file").map((entry) => entry.path), publish: Effect.void }),
        handle: (path: string) => io(async () => {
          const bytes = await readFile(join(sourcePath, path), "utf8");
          if (first) {
            first = false;
            entered.open();
            await release.promise;
          }
          await Bun.write(join(outputPath, "published"), bytes);
          return [];
        }),
      });
      yield* session.awaitCompletion;
      return yield* io(() => readFile(join(outputPath, "published"), "utf8"));
    })));
    // #when processing finishes after an unannounced replacement
    await entered.promise;
    await Bun.write(join(sourcePath, "document"), "Current replacement source");
    release.open();
    const result = await running;
    // #then the published result reflects the current source, not captured old bytes
    expect(result).toBe("Current replacement source");
  } finally {
    release.open();
    await rm(root, { recursive: true, force: true });
  }
});

test("a publication failure does not discard an already admitted forced follow-up", async () => {
  // #given a real published reference and a held update before a real failed read
  const root = await mkdtemp(join(tmpdir(), "sync-engine-follow-up-failure-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  await Bun.write(join(sourcePath, "document"), "Original publication");
  const entered = gate();
  const release = gate();
  let hold = false;
  let failPublication = false;
  try {
    const result = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const session = yield* openLiveSynchronization({
        sourcePath, outputPath,
        declare: () => Effect.succeed({
          work: ["document"],
          publish: io(async () => {
            if (failPublication) {
              failPublication = false;
              await readFile(join(root, "unavailable-publication-target"), "utf8");
            }
            await Bun.write(join(outputPath, "reference"), await readFile(join(sourcePath, "document"), "utf8"));
          }),
        }),
        handle: (_path: string) => io(async () => {
          if (hold) {
            hold = false;
            entered.open();
            await release.promise;
          }
          return [];
        }),
      });
      yield* io(() => Bun.write(join(sourcePath, "document"), "Current publication"));
      hold = true;
      failPublication = true;
      yield* session.requestPass();
      yield* io(() => entered.promise);
      // #when the forced follow-up is admitted before the current publication fails
      yield* session.requestPass({ force: true });
      release.open();
      yield* session.awaitCompletion;
      return yield* io(() => readFile(join(outputPath, "reference"), "utf8"));
    })));
    // #then the already admitted pass repairs publication instead of being dropped
    expect(result).toBe("Current publication");
  } finally {
    release.open();
    await rm(root, { recursive: true, force: true });
  }
});

test("combined requests during processing preserve a forced repair of unchanged sources", async () => {
  // #given two real sources and a previously published representation
  const root = await mkdtemp(join(tmpdir(), "sync-engine-forced-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  await Bun.write(join(sourcePath, "first"), "First original");
  await Bun.write(join(sourcePath, "second"), "Second unchanged");
  const entered = gate();
  const release = gate();
  let hold = false;
  try {
    const result = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const session = yield* openLiveSynchronization({
        sourcePath, outputPath,
        declare: (entries, request) => Effect.succeed({
          work: entries.filter((entry) => entry.kind === "file" && (request.kind === "initial" || request.force || request.changedPaths.includes(entry.path))).map((entry) => entry.path),
          publish: Effect.void,
        }),
        handle: (path: string) => io(async () => {
          const bytes = await readFile(join(sourcePath, path), "utf8");
          if (hold && path === "first") {
            hold = false;
            entered.open();
            await release.promise;
          }
          await Bun.write(join(outputPath, path), bytes);
          return [];
        }),
      });
      yield* io(async () => {
        hold = true;
        await Bun.write(join(sourcePath, "first"), "First current replacement");
      });
      yield* session.requestPass();
      yield* io(() => entered.promise);
      const before = yield* io(() => readFile(join(outputPath, "first"), "utf8"));
      yield* io(() => Bun.write(join(outputPath, "second"), "Damaged old representation"));
      const admissions = [yield* session.requestPass(), yield* session.requestPass({ force: true }), yield* session.requestPass()];
      release.open();
      yield* session.awaitCompletion;
      return {
        before, admissions,
        first: yield* io(() => readFile(join(outputPath, "first"), "utf8")),
        second: yield* io(() => readFile(join(outputPath, "second"), "utf8")),
      };
    })));
    // #then prior results stayed available and the combined follow-up retained forced mode
    expect(result).toEqual({ before: "First original", admissions: ["queued", "queued", "queued"], first: "First current replacement", second: "Second unchanged" });
  } finally {
    release.open();
    await rm(root, { recursive: true, force: true });
  }
});

test("a failed pass exposes its failure while earlier output stays available", async () => {
  // #given a completed real session whose source root is then removed
  const root = await mkdtemp(join(tmpdir(), "sync-engine-pass-failure-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  await Bun.write(join(sourcePath, "document"), "Original publication");

  try {
    const result = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const session = yield* openLiveSynchronization({
        sourcePath, outputPath,
        declare: () => Effect.succeed({ work: ["document"], publish: Effect.void }),
        handle: (path: string) => io(async () => {
          await Bun.write(join(outputPath, "reference"), await readFile(join(sourcePath, path), "utf8"));
          return [];
        }),
      });
      yield* session.awaitCompletion;
      yield* io(() => rm(sourcePath, { recursive: true, force: true }));
      // #when the next pass cannot read the source
      yield* session.requestPass();
      const outcome = yield* session.awaitCompletion.pipe(Effect.as("success"), Effect.catchTag("ScanFailed", () => Effect.succeed("scan failed")));
      const status = yield* session.status;

      return { outcome, state: status.state, failure: status.failure ? Cause.pretty(status.failure).includes(sourcePath) : null, reference: yield* io(() => readFile(join(outputPath, "reference"), "utf8")) };
    })));
    // #then the failure is a public fact and the earlier publication is intact
    expect(result).toEqual({ outcome: "scan failed", state: "failed", failure: true, reference: "Original publication" });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("typed required failures still publish prior results and remain visible in status", async () => {
  // #given a live session with a previously published required result
  const root = await mkdtemp(join(tmpdir(), "sync-engine-live-typed-failure-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  await Bun.write(join(sourcePath, "document"), "Original source");
  try {
    const result = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const session = yield* openLiveSynchronization({
        sourcePath, outputPath,
        declare: () => Effect.succeed({
          work: ["document"],
          publish: io(async () => Bun.write(join(outputPath, "reference"), await readFile(join(outputPath, "document"), "utf8"))),
        }),
        handle: (path: string) => io(() => readFile(join(sourcePath, path), "utf8")).pipe(Effect.flatMap((bytes) =>
          bytes === "Broken source"
            ? Effect.fail(new Error("Cannot prepare document"))
            : io(async () => { await Bun.write(join(outputPath, path), `Prepared: ${bytes}`); return []; }),
        )),
      });
      yield* io(() => Bun.write(join(sourcePath, "document"), "Broken source"));
      // #when required work fails with a typed error during a later live pass
      yield* session.requestPass({ force: true });
      yield* session.awaitCompletion;
      const status = yield* session.status;
      return {
        state: status.state,
        errors: status.work.errors.map((error) => error.work),
        result: yield* io(() => readFile(join(outputPath, "document"), "utf8")),
        reference: yield* io(() => readFile(join(outputPath, "reference"), "utf8")),
      };
    })));
    // #then the pass published with visible errors and retained the prior required result
    expect(result).toEqual({ state: "complete-with-errors", errors: ["document"], result: "Prepared: Original source", reference: "Prepared: Original source" });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("fatal first-pass failure rejects later admission instead of starting unreachable work", async () => {
  // #given a live handle whose first pass cannot recover
  const root = await mkdtemp(join(tmpdir(), "sync-engine-live-fatal-open-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  try {
    const result = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const session = yield* startLiveSynchronization({
        sourcePath, outputPath,
        declare: () => Effect.succeed({ work: ["document"], publish: Effect.void }),
        handle: (_path: string) => Effect.succeed([]),
      });
      const readyExit = yield* Effect.exit(session.ready);
      const ready = Exit.isFailure(readyExit) ? "failed" : "ready";
      // #when the application asks for another pass after the fatal first-pass failure
      const admission = yield* session.requestPass();
      const status = yield* session.status;
      return { ready, admission, state: status.state };
    })));
    // #then no unreachable pass is reported as started
    expect(result).toEqual({ ready: "failed", admission: "rejected", state: "stopped" });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("watcher hints survive a failed pass that stops before freshness invalidation", async () => {
  // #given a retained metadata result and a replacement with equal metadata
  const root = await mkdtemp(join(tmpdir(), "sync-engine-live-hint-carry-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  const stamp = new Date("2020-01-01T00:00:00Z");
  await mkdir(sourcePath);
  await Bun.write(join(sourcePath, "note.txt"), "Original");
  await utimes(join(sourcePath, "note.txt"), stamp, stamp);
  let failDeclare = false;
  try {
    const result = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const session = yield* openLiveSynchronization({
        sourcePath, outputPath,
        freshness: { describe: () => ({ sourcePaths: ["note.txt"], resultKind: "note", processingVersion: "v1", outputPaths: ["note.txt"] }) },
        declare: (_entries, request) => failDeclare
          ? Effect.fail(new Error(`declare failed for ${request.changedPaths.join(",")}`))
          : Effect.succeed({ work: ["note.txt"], publish: Effect.void }),
        handle: (path: string) => io(async () => {
          await Bun.write(join(outputPath, path), await readFile(join(sourcePath, path), "utf8"));
          return [];
        }),
      });
      yield* io(async () => {
        await Bun.write(join(sourcePath, "note.txt"), "Changed!");
        await utimes(join(sourcePath, "note.txt"), stamp, stamp);
      });
      failDeclare = true;
      // #when the hinted pass fails before it can invalidate freshness, then a plain retry runs
      yield* session.notify(["note.txt"]);
      const failed = yield* Effect.exit(session.awaitCompletion);
      failDeclare = false;
      yield* session.requestPass();
      yield* session.awaitCompletion;
      return { failed: Exit.isFailure(failed), text: yield* io(() => readFile(join(outputPath, "note.txt"), "utf8")) };
    })));
    // #then the retry still uses the carried watcher hint and rebuilds the equal-stamp result
    expect(result).toEqual({ failed: true, text: "Changed!" });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
