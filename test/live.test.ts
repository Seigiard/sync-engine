import { test, expect } from "bun:test";
import { mkdir, mkdtemp, readFile, rename, rm, stat, utimes } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Cause, Effect, Exit, Scope } from "effect";
import { acquireOutputTree, openLiveSynchronization, runInitialPass, startLiveSynchronization, type LiveHandle, type LiveSynchronization } from "../src/index.ts";
import { startLiveSynchronizationWithHooks } from "../src/live.ts";

const io = <A>(run: () => Promise<A>) => Effect.tryPromise({ try: run, catch: (cause) => new Error(String(cause)) }).pipe(Effect.uninterruptible);

function gate() {
  const { promise, resolve } = Promise.withResolvers<void>();
  return { promise, open: () => resolve() };
}

type PassSnapshot = { readonly kind: string; readonly force: boolean; readonly changedPaths: readonly string[] };

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

test("a failed later publication does not record freshness for unpublished output", async () => {
  // #given an initial retained output whose later live publication will fail
  const root = await mkdtemp(join(tmpdir(), "sync-engine-live-publish-before-commit-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  await Bun.write(join(sourcePath, "note.txt"), "Original");
  const stamp = new Date("2020-01-01T00:00:00Z");
  let failPublish = false;
  let handled = 0;
  const options = () => ({
    sourcePath,
    outputPath,
    reconcileIntervalMs: 0,
    freshness: { describe: () => ({ sourcePaths: ["note.txt"], resultKind: "note", processingVersion: "v1", outputPaths: ["published.txt"] }) },
    declare: () => Effect.succeed({
      work: ["note.txt"],
      publish: failPublish
        ? Effect.fail(new Error("publish failed"))
        : io(async () => { await Bun.write(join(outputPath, "published.txt"), "published"); }),
    }),
    handle: (_path: string) => Effect.sync(() => { handled += 1; return [] as string[]; }),
  });
  try {
    await Effect.runPromise(runInitialPass(options()));
    await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const session = yield* openLiveSynchronization(options());
      yield* io(async () => {
        await Bun.write(join(sourcePath, "note.txt"), "Changed!");
        await utimes(join(sourcePath, "note.txt"), stamp, stamp);
      });
      failPublish = true;
      yield* session.requestPass();
      yield* Effect.exit(session.awaitCompletion);
    })));
    handled = 0;
    failPublish = false;
    // #when a fresh session starts after the failed publication
    await Effect.runPromise(runInitialPass(options()));
    // #then freshness did not record the unpublished later pass as reusable
    expect(handled).toBe(1);
  } finally {
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
      let publishes = 0;
      const session = yield* openLiveSynchronization({
        sourcePath, outputPath,
        declare: () => Effect.succeed({
          work: ["document"],
          publish: io(async () => {
            publishes += 1;
            await Bun.write(join(outputPath, "reference"), `${publishes}:${await readFile(join(outputPath, "document"), "utf8")}`);
          }),
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
        publishes,
        result: yield* io(() => readFile(join(outputPath, "document"), "utf8")),
        reference: yield* io(() => readFile(join(outputPath, "reference"), "utf8")),
      };
    })));
    // #then the pass published with visible errors and retained the prior required result
    expect(result).toEqual({ state: "complete-with-errors", errors: ["document"], publishes: 2, result: "Prepared: Original source", reference: "2:Prepared: Original source" });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("failed pass hints merge into an already queued follow-up", async () => {
  // #given a retained metadata result and a held pass that will fail before freshness invalidation
  const root = await mkdtemp(join(tmpdir(), "sync-engine-live-carried-pending-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  const stamp = new Date("2020-01-01T00:00:00Z");
  await mkdir(sourcePath);
  await Bun.write(join(sourcePath, "note.txt"), "Original");
  await utimes(join(sourcePath, "note.txt"), stamp, stamp);
  let failNext = false;
  try {
    const result = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const entered = gate();
      const release = gate();
      const session = yield* openLiveSynchronization({
        sourcePath, outputPath,
        freshness: { describe: () => ({ sourcePaths: ["note.txt"], resultKind: "note", processingVersion: "v1", outputPaths: ["note.txt"] }) },
        declare: (_entries, request) => failNext
          ? io(async () => { entered.open(); await release.promise; throw new Error(`declare failed for ${request.changedPaths.join(",")}`); })
          : Effect.succeed({ work: request.changedPaths.includes("note.txt") ? ["note.txt"] : [], publish: Effect.void }),
        handle: (path: string) => io(async () => {
          await Bun.write(join(outputPath, path), await readFile(join(sourcePath, path), "utf8"));
          return [];
        }),
      });
      yield* io(async () => {
        await Bun.write(join(sourcePath, "note.txt"), "Changed!");
        await utimes(join(sourcePath, "note.txt"), stamp, stamp);
      });
      failNext = true;
      yield* session.notify(["note.txt"]);
      yield* io(() => entered.promise);
      // #when a follow-up is already queued before the failing pass returns
      yield* session.requestPass();
      failNext = false;
      release.open();
      yield* session.awaitCompletion;
      return yield* io(() => readFile(join(outputPath, "note.txt"), "utf8"));
    })));
    // #then the queued follow-up receives the failed pass hint and repairs equal-stamp output
    expect(result).toBe("Changed!");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("failed forced pass preserves force for an already queued follow-up", async () => {
  // #given retained work that only a forced retry will rebuild
  const root = await mkdtemp(join(tmpdir(), "sync-engine-live-carried-force-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  await Bun.write(join(sourcePath, "note.txt"), "Original");
  let failNext = false;
  try {
    const result = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const entered = gate();
      const release = gate();
      const session = yield* openLiveSynchronization({
        sourcePath, outputPath,
        freshness: { describe: () => ({ sourcePaths: ["note.txt"], resultKind: "note", processingVersion: "v1", outputPaths: ["note.txt"] }) },
        declare: (_entries, request) => failNext
          ? io(async () => { entered.open(); await release.promise; throw new Error(`declare failed force=${request.force}`); })
          : Effect.succeed({ work: request.force ? ["note.txt"] : [], publish: Effect.void }),
        handle: (path: string) => io(async () => {
          await Bun.write(join(outputPath, path), await readFile(join(sourcePath, path), "utf8"));
          return [];
        }),
      });
      yield* io(() => Bun.write(join(sourcePath, "note.txt"), "Forced"));
      failNext = true;
      yield* session.requestPass({ force: true });
      yield* io(() => entered.promise);
      yield* session.requestPass();
      failNext = false;
      release.open();
      yield* session.awaitCompletion;
      return yield* io(() => readFile(join(outputPath, "note.txt"), "utf8"));
    })));
    // #then the queued follow-up inherits force and rebuilds despite no hint
    expect(result).toBe("Forced");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("a later live pass handler defect stops admission and releases the lease", async () => {
  // #given an opened live session over a real output tree
  const root = await mkdtemp(join(tmpdir(), "sync-engine-live-later-defect-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  await Bun.write(join(sourcePath, "document"), "Original");
  let defect = false;
  try {
    const result = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const session = yield* openLiveSynchronization({
        sourcePath, outputPath,
        declare: () => Effect.succeed({ work: ["document"], publish: Effect.void }),
        handle: (path: string) => defect
          ? Effect.die("later handler defect")
          : io(async () => { await Bun.write(join(outputPath, path), await readFile(join(sourcePath, path), "utf8")); return []; }),
      });
      defect = true;
      yield* io(() => Bun.write(join(sourcePath, "document"), "Changed"));
      yield* session.requestPass({ force: true });
      const failed = yield* Effect.exit(session.awaitCompletion);
      const admission = yield* session.requestPass();
      const status = yield* session.status;
      const release = yield* acquireOutputTree(outputPath);
      yield* Effect.promise(release);
      return { failed: Exit.isFailure(failed), admission, state: status.state, active: status.work.active };
    })));
    // #then the session is terminal instead of keeping a failed scheduler alive
    expect(result).toEqual({ failed: true, admission: "rejected", state: "stopped", active: null });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("later live pass defects from scan declare publish and handle end the attempt", async () => {
  const cases = ["scan", "declare", "publish", "handle"] as const;
  for (const defect of cases) {
    // #given an opened live session for each pass phase that can defect
    const root = await mkdtemp(join(tmpdir(), `sync-engine-live-${defect}-defect-`));
    const sourcePath = join(root, "source");
    const outputPath = join(root, "output");
    await mkdir(sourcePath);
    await Bun.write(join(sourcePath, "document"), "Original");
    let fail = false;
    try {
      const result = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
        const session = yield* openLiveSynchronization({
          sourcePath,
          outputPath,
          includeSource: (path) => {
            if (fail && defect === "scan") throw new Error(`scan defect ${path}`);
            return true;
          },
          declare: () => fail && defect === "declare"
            ? Effect.die("declare defect")
            : Effect.succeed({
                work: ["document"],
                publish: fail && defect === "publish" ? Effect.die("publish defect") : Effect.void,
              }),
          handle: (path: string) => fail && defect === "handle"
            ? Effect.die("handle defect")
            : io(async () => { await Bun.write(join(outputPath, path), await readFile(join(sourcePath, path), "utf8")); return []; }),
        });
        fail = true;
        yield* session.requestPass({ force: true });
        const failed = yield* Effect.exit(session.awaitCompletion);
        const admission = yield* session.requestPass();
        const status = yield* session.status;
        const release = yield* acquireOutputTree(outputPath);
        yield* Effect.promise(release);
        return { defect, failed: Exit.isFailure(failed), admission, state: status.state };
      })));
      // #then every defect path stops admission and releases the attempt lease
      expect(result).toEqual({ defect, failed: true, admission: "rejected", state: "stopped" });
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  }
});

test("notify during the first pass prevents the initial stale read from being retained", async () => {
  // #given metadata freshness and an initial pass held after reading old bytes
  const root = await mkdtemp(join(tmpdir(), "sync-engine-live-opening-notify-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  const stamp = new Date("2020-01-01T00:00:00Z");
  await mkdir(sourcePath);
  await Bun.write(join(sourcePath, "note.txt"), "Original");
  await utimes(join(sourcePath, "note.txt"), stamp, stamp);
  let holdInitial = false;
  const activeRead = gate();
  const releaseInitial = gate();
  const watcherEntered = gate();
  const options = {
    sourcePath,
    outputPath,
    reconcileIntervalMs: 0,
    freshness: { describe: () => ({ sourcePaths: ["note.txt"], resultKind: "note", processingVersion: "v1", outputPaths: ["note.txt"] }) },
    declare: (_entries: unknown, request: { readonly kind: string }) => request.kind === "watcher"
      ? io(async () => { watcherEntered.open(); throw new Error("stop before watcher submit"); })
      : Effect.succeed({ work: ["note.txt"], publish: Effect.void }),
    handle: (path: string) => io(async () => {
      const bytes = await readFile(join(sourcePath, path), "utf8");
      if (holdInitial) {
        holdInitial = false;
        activeRead.open();
        await releaseInitial.promise;
      }
      await Bun.write(join(outputPath, path), bytes);
      return [] as string[];
    }),
  };
  const initialOptions = { ...options, declare: () => Effect.succeed({ work: ["note.txt"], publish: Effect.void }) };
  try {
    const scope = await Effect.runPromise(Scope.make());
    holdInitial = true;
    const session = await Effect.runPromise(startLiveSynchronization(options).pipe(Scope.provide(scope))) as LiveHandle<string, Error>;
    await activeRead.promise;
    // #when an equal-stamp replacement is notified before ready settles
    await Bun.write(join(sourcePath, "note.txt"), "Changed!");
    await utimes(join(sourcePath, "note.txt"), stamp, stamp);
    await Effect.runPromise(session.notify(["note.txt"]));
    releaseInitial.open();
    await Effect.runPromise(session.ready);
    await watcherEntered.promise;
    await Effect.runPromise(Effect.exit(session.awaitCompletion));
    await Effect.runPromise(Scope.close(scope, Exit.void));
    await Effect.runPromise(runInitialPass(initialOptions));
    // #then a fresh session replays the replacement instead of trusting the opening stale read
    expect(await readFile(join(outputPath, "note.txt"), "utf8")).toBe("Changed!");
  } finally {
    releaseInitial.open();
    watcherEntered.open();
    await rm(root, { recursive: true, force: true });
  }
});

test("a queued request retries immediately after a recoverable opening failure", async () => {
  // #given a warm live session whose first attempt is held and then fails recoverably
  const root = await mkdtemp(join(tmpdir(), "sync-engine-live-opening-retry-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  await mkdir(outputPath);
  await Bun.write(join(sourcePath, "document"), "Recovered");
  await Bun.write(join(outputPath, "reference"), "Prior");
  let failFirst = true;
  let attempts = 0;
  const entered = gate();
  const release = gate();
  try {
    const result = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const session = yield* startLiveSynchronization({
        sourcePath,
        outputPath,
        reconcileIntervalMs: 0,
        recovery: { existing: Effect.succeed(true) },
        declare: () => Effect.succeed({ work: ["document"], publish: Effect.void }),
        handle: (path: string) => io(async () => {
          attempts += 1;
          if (failFirst) {
            entered.open();
            await release.promise;
            failFirst = false;
            throw new Error("opening failed");
          }
          await Bun.write(join(outputPath, "reference"), await readFile(join(sourcePath, path), "utf8"));
          return [] as string[];
        }),
      });
      yield* io(() => entered.promise);
      // #when a resync is admitted while the opening attempt is still active
      const admission = yield* session.requestPass();
      release.open();
      yield* session.awaitCompletion;
      return { admission, attempts, reference: yield* io(() => readFile(join(outputPath, "reference"), "utf8")) };
    })));
    // #then the request admitted during the failed opening starts one more attempt, then runs as its follow-up
    expect(result).toEqual({ admission: "queued", attempts: 3, reference: "Recovered" });
  } finally {
    release.open();
    await rm(root, { recursive: true, force: true });
  }
});

test("a queued retry after repeated recoverable opening failures settles instead of hot looping", async () => {
  // #given a warm session whose opening work always fails
  const root = await mkdtemp(join(tmpdir(), "sync-engine-live-opening-loop-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  await mkdir(outputPath);
  await Bun.write(join(sourcePath, "document"), "Broken");
  await Bun.write(join(outputPath, "reference"), "Prior");
  let attempts = 0;
  const entered = gate();
  const release = gate();
  try {
    const result = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const session = yield* startLiveSynchronization({
        sourcePath,
        outputPath,
        reconcileIntervalMs: 0,
        recovery: { existing: Effect.succeed(true) },
        declare: () => Effect.succeed({ work: ["document"], publish: Effect.void }),
        handle: () => io(async () => {
          attempts += 1;
          if (attempts === 1) {
            entered.open();
            await release.promise;
          }
          throw new Error("still failing");
        }),
      });
      yield* io(() => entered.promise);
      // #when a request is queued behind a failed opening and the retry also fails
      const admission = yield* session.requestPass();
      release.open();
      const completion = yield* Effect.exit(session.awaitCompletion);
      yield* Effect.sleep(25);
      const status = yield* session.status;
      return { admission, completionFailed: Exit.isFailure(completion), attempts, state: status.state, followUp: status.followUp };
    })));
    // #then the retry's own trigger starts no further attempt, so the session waits instead of looping, with no follow-up scheduled
    expect(result).toEqual({ admission: "queued", completionFailed: true, attempts: 2, state: "failed", followUp: null });
  } finally {
    release.open();
    await rm(root, { recursive: true, force: true });
  }
});

test("a request-triggered reopen failure does not spend an immediate second attempt", async () => {
  // #given a warm session whose first opening has already failed recoverably
  const root = await mkdtemp(join(tmpdir(), "sync-engine-live-reopen-trigger-fail-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  await mkdir(outputPath);
  await Bun.write(join(sourcePath, "document"), "Still broken");
  await Bun.write(join(outputPath, "reference"), "Prior");
  let attempts = 0;
  try {
    const result = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const session = yield* startLiveSynchronization({
        sourcePath,
        outputPath,
        reconcileIntervalMs: 0,
        recovery: { existing: Effect.succeed(true) },
        declare: () => Effect.succeed({ work: ["document"], publish: Effect.void }),
        handle: () => Effect.sync(() => { attempts += 1; throw new Error("outage"); }),
      });
      yield* Effect.exit(session.awaitCompletion);
      const before = attempts;
      // #when a request starts a reopen and that reopen also fails
      const admission = yield* session.requestPass();
      const completion = yield* Effect.exit(session.awaitCompletion);
      yield* Effect.sleep(25);
      const status = yield* session.status;
      return { admission, failed: Exit.isFailure(completion), attemptsAfterRequest: attempts - before, state: status.state, followUp: status.followUp };
    })));
    // #then the trigger is consumed by that attempt and is not retried as if it had queued during the attempt
    expect(result).toEqual({ admission: "started", failed: true, attemptsAfterRequest: 1, state: "failed", followUp: null });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("a request admitted during a failed retry attempt starts another attempt", async () => {
  // #given a recoverable opening failure followed by a retry attempt
  const root = await mkdtemp(join(tmpdir(), "sync-engine-live-retry-arrival-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  await mkdir(outputPath);
  await Bun.write(join(sourcePath, "document"), "Recovered");
  await Bun.write(join(outputPath, "reference"), "Prior");
  let attempts = 0;
  const firstEntered = gate();
  const releaseFirst = gate();
  const retryEntered = gate();
  const releaseRetry = gate();
  try {
    const result = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const session = yield* startLiveSynchronization({
        sourcePath,
        outputPath,
        reconcileIntervalMs: 0,
        recovery: { existing: Effect.succeed(true) },
        declare: () => Effect.succeed({ work: ["document"], publish: Effect.void }),
        handle: (path: string) => io(async () => {
          attempts += 1;
          if (attempts === 1) {
            firstEntered.open();
            await releaseFirst.promise;
            throw new Error("opening failed");
          }
          if (attempts === 2) {
            retryEntered.open();
            await releaseRetry.promise;
            throw new Error("retry failed");
          }
          await Bun.write(join(outputPath, "reference"), await readFile(join(sourcePath, path), "utf8"));
          return [] as string[];
        }),
      });
      yield* io(() => firstEntered.promise);
      const firstAdmission = yield* session.requestPass();
      releaseFirst.open();
      yield* io(() => retryEntered.promise);
      // #when another request is admitted while the retry attempt runs
      const secondAdmission = yield* session.requestPass();
      releaseRetry.open();
      yield* session.awaitCompletion;
      const status = yield* session.status;
      return { firstAdmission, secondAdmission, attempts, state: status.state, reference: yield* io(() => readFile(join(outputPath, "reference"), "utf8")) };
    })));
    // #then that request starts another attempt even though the retry failed
    expect(result).toEqual({ firstAdmission: "queued", secondAdmission: "queued", attempts: 4, state: "complete", reference: "Recovered" });
  } finally {
    releaseFirst.open();
    releaseRetry.open();
    await rm(root, { recursive: true, force: true });
  }
});

test("reconcile ticks during a failing retry attempt do not keep completion outstanding", async () => {
  // #given a positive reconcile interval shorter than a held failing retry
  const root = await mkdtemp(join(tmpdir(), "sync-engine-live-retry-timer-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  await mkdir(outputPath);
  await Bun.write(join(sourcePath, "document"), "Outage");
  let attempts = 0;
  const firstEntered = gate();
  const releaseFirst = gate();
  const retryEntered = gate();
  const releaseRetry = gate();
  try {
    const result = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const session = yield* startLiveSynchronization({
        sourcePath,
        outputPath,
        reconcileIntervalMs: 1,
        recovery: { existing: Effect.succeed(true) },
        declare: () => Effect.succeed({ work: ["document"], publish: Effect.void }),
        handle: () => io(async () => {
          attempts += 1;
          if (attempts === 1) { firstEntered.open(); await releaseFirst.promise; throw new Error("opening failed"); }
          if (attempts === 2) { retryEntered.open(); await releaseRetry.promise; throw new Error("retry failed"); }
          throw new Error("unexpected extra attempt");
        }),
      });
      yield* io(() => firstEntered.promise);
      yield* session.requestPass();
      releaseFirst.open();
      yield* io(() => retryEntered.promise);
      yield* Effect.sleep(25);
      releaseRetry.open();
      const completion = yield* Effect.race(Effect.exit(session.awaitCompletion).pipe(Effect.map((exit) => Exit.isFailure(exit) ? "failed" : "succeeded")), Effect.sleep(250).pipe(Effect.as("hung")));
      const status = yield* session.status;
      return { completion, attempts, state: status.state };
    })));
    // #then timer ticks during the retry do not count as newer requests that keep completion pending
    expect(result).toEqual({ completion: "failed", attempts: 2, state: "failed" });
  } finally {
    releaseFirst.open();
    releaseRetry.open();
    await rm(root, { recursive: true, force: true });
  }
});

type PayloadScenario =
  | "opening-ok"
  | "opening-fail-retry-ok"
  | "opening-fail-retry-fail-reopen-ok"
  | "retry-ok"
  | "retry-fail-reopen-ok"
  | "reopen-ok"
  | "reopen-fail-later-ok"
  | "running-recoverable-defect";

async function runPayloadScenario(scenario: PayloadScenario) {
  const root = await mkdtemp(join(tmpdir(), `sync-engine-payload-model-${scenario}-`));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  await mkdir(outputPath);
  await Bun.write(join(sourcePath, "note.txt"), "Current");
  const followUps: PassSnapshot[] = [];
  const entered = gate();
  const release = gate();
  const retryEntered = gate();
  const releaseRetry = gate();
  let attempts = 0;
  let openings = 0;
  const shouldFail = (attempt: number) => {
    if (scenario === "opening-fail-retry-ok" || scenario === "retry-ok" || scenario === "reopen-ok") return attempt === 1;
    if (scenario === "opening-fail-retry-fail-reopen-ok") return attempt === 1 || attempt === 2;
    if (scenario === "retry-fail-reopen-ok") return attempt === 1 || attempt === 2;
    if (scenario === "reopen-fail-later-ok") return attempt === 1 || attempt === 2;
    return false;
  };
  try {
    const result = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const session = yield* startLiveSynchronization({
        sourcePath,
        outputPath,
        reconcileIntervalMs: 0,
        recovery: { existing: Effect.succeed(true) },
        declare: (_entries, request) => Effect.sync(() => {
          if (request.kind === "initial") openings += 1;
          else followUps.push({ kind: request.kind, force: request.force, changedPaths: [...request.changedPaths] });
          return { work: ["note.txt"], publish: Effect.void };
        }),
        handle: (path: string) => {
          const effect = io(async () => {
            attempts += 1;
            const attempt = attempts;
            if (scenario === "opening-fail-retry-fail-reopen-ok" && attempt === 1) { entered.open(); await release.promise; }
            if (scenario === "opening-fail-retry-fail-reopen-ok" && attempt === 2) { retryEntered.open(); await releaseRetry.promise; }
            const held = scenario !== "opening-fail-retry-fail-reopen-ok" && (scenario === "opening-ok" && attempt === 1
              || scenario === "opening-fail-retry-ok" && attempt === 1
              || scenario === "retry-ok" && attempt === 2
              || scenario === "retry-fail-reopen-ok" && attempt === 2
              || scenario === "reopen-ok" && attempt === 2
              || scenario === "reopen-fail-later-ok" && attempt === 2
              || scenario === "running-recoverable-defect" && attempt === 2);
            if (held) { entered.open(); await release.promise; }
            if (scenario === "running-recoverable-defect" && attempt === 2) throw new Error("recoverable running defect");
            if (shouldFail(attempt)) throw new Error("recoverable opening failure");
            await Bun.write(join(outputPath, path), await readFile(join(sourcePath, path), "utf8"));
            return [] as string[];
          });
          return scenario === "running-recoverable-defect" ? effect.pipe(Effect.orDie) : effect;
        },
      });

      if (scenario === "opening-ok" || scenario === "opening-fail-retry-ok") {
        yield* io(() => entered.promise);
        yield* session.requestPass({ force: true });
        yield* session.notify(["note.txt"]);
        release.open();
      } else if (scenario === "opening-fail-retry-fail-reopen-ok") {
        yield* io(() => entered.promise);
        yield* session.requestPass({ force: true });
        yield* session.notify(["note.txt"]);
        release.open();
        yield* io(() => retryEntered.promise);
        yield* session.requestPass();
        releaseRetry.open();
      } else if (scenario === "retry-ok" || scenario === "retry-fail-reopen-ok") {
        yield* session.requestPass();
        yield* io(() => entered.promise);
        yield* session.requestPass({ force: true });
        yield* session.notify(["note.txt"]);
        release.open();
      } else if (scenario === "reopen-ok" || scenario === "reopen-fail-later-ok") {
        yield* Effect.exit(session.awaitCompletion);
        yield* session.requestPass({ force: true });
        yield* session.notify(["note.txt"]);
        yield* io(() => entered.promise);
        release.open();
        if (scenario === "reopen-fail-later-ok") yield* Effect.exit(session.awaitCompletion).pipe(Effect.andThen(session.requestPass()));
      } else {
        yield* session.awaitCompletion;
        yield* session.requestPass();
        yield* io(() => entered.promise);
        yield* session.requestPass({ force: true });
        yield* session.notify(["note.txt"]);
        release.open();
      }

      yield* session.awaitCompletion;
      return { attempts, followUps };
    })));
    const payloadBearing = result.followUps.filter((request) => request.force || request.changedPaths.length > 0);
    const matching = payloadBearing.filter((request) => request.force && request.changedPaths.length === 1 && request.changedPaths[0] === "note.txt");
    return { attempts: result.attempts, openings, followUps: result.followUps, payloadBearing, matching };
  } finally {
    release.open();
    releaseRetry.open();
    await rm(root, { recursive: true, force: true });
  }
}

test.each([
  // `openings` counts initial declarations, so each label's retry or reopen is proven to have happened.
  ["during first opening -> open ok", "opening-ok", 1],
  ["during first opening -> open fail -> retry ok", "opening-fail-retry-ok", 2],
  ["during first opening -> open fail -> retry fail -> reopen ok", "opening-fail-retry-fail-reopen-ok", 3],
  ["during the retry attempt -> retry ok", "retry-ok", 2],
  ["during the retry attempt -> retry fail -> reopen ok", "retry-fail-reopen-ok", 3],
  ["during reopen -> open ok", "reopen-ok", 2],
  ["during reopen -> open fail -> later open ok", "reopen-fail-later-ok", 3],
  ["while running -> recoverable defect -> retry ok", "running-recoverable-defect", 2],
] as const)("request payload model preserves force and hints exactly once: %s", async (_label, scenario, openings) => {
  // #given a table cell that admits a forced request and a source hint at a distinct live state
  const result = await runPayloadScenario(scenario);
  // #then the cell reached its state, and the next successful follow-up declare receives their union exactly once
  expect({ openings: result.openings, matching: result.matching, payloadBearing: result.payloadBearing }).toEqual({
    openings,
    matching: [{ kind: "resync", force: true, changedPaths: ["note.txt"] }],
    payloadBearing: [{ kind: "resync", force: true, changedPaths: ["note.txt"] }],
  });
});

test.each(["force", "hint"])("a request admitted during a retry attempt preserves earlier %s payload", async (mode) => {
  // #given a recoverable opening whose queued retry carries force or a source hint
  const root = await mkdtemp(join(tmpdir(), `sync-engine-live-retry-payload-${mode}-`));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  await mkdir(outputPath);
  await Bun.write(join(sourcePath, "note.txt"), "Recovered");
  const firstEntered = gate();
  const releaseFirst = gate();
  const retryEntered = gate();
  const releaseRetry = gate();
  let attempts = 0;
  const followUps: PassSnapshot[] = [];
  try {
    const result = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const session = yield* startLiveSynchronization({
        sourcePath,
        outputPath,
        reconcileIntervalMs: 0,
        recovery: { existing: Effect.succeed(true) },
        declare: (_entries, request) => Effect.sync(() => {
          if (request.kind !== "initial") followUps.push({ kind: request.kind, force: request.force, changedPaths: [...request.changedPaths] });
          return { work: ["note.txt"], publish: Effect.void };
        }),
        handle: (path: string) => io(async () => {
          attempts += 1;
          if (attempts === 1) { firstEntered.open(); await releaseFirst.promise; throw new Error("opening failed"); }
          if (attempts === 2) { retryEntered.open(); await releaseRetry.promise; throw new Error("retry failed"); }
          await Bun.write(join(outputPath, path), await readFile(join(sourcePath, path), "utf8"));
          return [] as string[];
        }),
      });
      yield* io(() => firstEntered.promise);
      if (mode === "force") yield* session.requestPass({ force: true });
      else yield* session.notify(["note.txt"]);
      releaseFirst.open();
      yield* io(() => retryEntered.promise);
      yield* session.requestPass();
      releaseRetry.open();
      yield* session.awaitCompletion;
      return followUps.at(-1);
    })));
    // #then the successful follow-up receives the original retry payload exactly once
    expect(result).toEqual(mode === "force"
      ? { kind: "resync", force: true, changedPaths: [] }
      : { kind: "resync", force: false, changedPaths: ["note.txt"] });
  } finally {
    releaseFirst.open();
    releaseRetry.open();
    await rm(root, { recursive: true, force: true });
  }
});

test.each(["force", "hint"])("a reopen trigger preserves %s across a failed reopen and a later request", async (mode) => {
  // #given a failed recoverable session whose trigger attempt will fail before a later request succeeds
  const root = await mkdtemp(join(tmpdir(), `sync-engine-live-reopen-trigger-${mode}-`));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  await mkdir(outputPath);
  await Bun.write(join(sourcePath, "note.txt"), "Current");
  let fail = true;
  const followUps: Array<{ readonly kind: string; readonly force: boolean; readonly changedPaths: readonly string[] }> = [];
  try {
    const result = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const session = yield* startLiveSynchronization({
        sourcePath,
        outputPath,
        reconcileIntervalMs: 0,
        recovery: { existing: Effect.succeed(true) },
        declare: (_entries, request) => Effect.sync(() => {
          if (request.kind !== "initial") followUps.push({ kind: request.kind, force: request.force, changedPaths: request.changedPaths });
          return { work: ["note.txt"], publish: Effect.void };
        }),
        handle: (path: string) => fail
          ? Effect.fail(new Error("reopen failed"))
          : io(async () => { await Bun.write(join(outputPath, path), await readFile(join(sourcePath, path), "utf8")); return [] as string[]; }),
      });
      yield* Effect.exit(session.awaitCompletion);
      if (mode === "force") yield* session.requestPass({ force: true });
      else yield* session.notify(["note.txt"]);
      yield* Effect.exit(session.awaitCompletion);
      fail = false;
      yield* session.requestPass();
      yield* session.awaitCompletion;
      return followUps.at(-1);
    })));
    // #then the consumed trigger's force or hint reaches the successful follow-up instead of being overwritten
    expect(result).toEqual(mode === "force"
      ? { kind: "resync", force: true, changedPaths: [] }
      : { kind: "resync", force: false, changedPaths: ["note.txt"] });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test.each(["force", "hint"])("a reopen trigger preserves %s through a failed retry chain", async (mode) => {
  // #given a failed recoverable session that will fail the trigger attempt and the retry attempt that follows it
  const root = await mkdtemp(join(tmpdir(), `sync-engine-live-retry-trigger-${mode}-`));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  await mkdir(outputPath);
  await Bun.write(join(sourcePath, "note.txt"), "Current");
  let attempts = 0;
  const followUps: Array<{ readonly kind: string; readonly force: boolean; readonly changedPaths: readonly string[] }> = [];
  const triggerEntered = gate();
  const releaseTrigger = gate();
  const retryEntered = gate();
  const releaseRetry = gate();
  try {
    const result = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const session = yield* startLiveSynchronization({
        sourcePath,
        outputPath,
        reconcileIntervalMs: 0,
        recovery: { existing: Effect.succeed(true) },
        declare: (_entries, request) => Effect.sync(() => {
          if (request.kind !== "initial") followUps.push({ kind: request.kind, force: request.force, changedPaths: request.changedPaths });
          return { work: ["note.txt"], publish: Effect.void };
        }),
        handle: (path: string) => io(async () => {
          attempts += 1;
          if (attempts === 1) throw new Error("precondition failure");
          if (attempts === 2) { triggerEntered.open(); await releaseTrigger.promise; throw new Error("trigger failed"); }
          if (attempts === 3) { retryEntered.open(); await releaseRetry.promise; throw new Error("retry failed"); }
          await Bun.write(join(outputPath, path), await readFile(join(sourcePath, path), "utf8"));
          return [] as string[];
        }),
      });
      yield* Effect.exit(session.awaitCompletion);
      if (mode === "force") yield* session.requestPass({ force: true });
      else yield* session.notify(["note.txt"]);
      yield* io(() => triggerEntered.promise);
      yield* session.requestPass();
      releaseTrigger.open();
      yield* io(() => retryEntered.promise);
      yield* session.requestPass();
      releaseRetry.open();
      yield* session.awaitCompletion;
      return followUps.at(-1);
    })));
    // #then the original trigger survives the failed retry chain and refreshes the equal-stamp source
    expect(result).toEqual(mode === "force"
      ? { kind: "resync", force: true, changedPaths: [] }
      : { kind: "resync", force: false, changedPaths: ["note.txt"] });
  } finally {
    releaseTrigger.open();
    releaseRetry.open();
    await rm(root, { recursive: true, force: true });
  }
});

test("status exposes the reopen trigger while the attempt is running", async () => {
  // #given a failed recoverable session and a held reopen attempt
  const root = await mkdtemp(join(tmpdir(), "sync-engine-live-reopen-status-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  await mkdir(outputPath);
  await Bun.write(join(sourcePath, "document"), "Recovered");
  const entered = gate();
  const release = gate();
  let fail = true;
  let closeScope: Effect.Effect<void> | undefined;
  try {
    const scope = await Effect.runPromise(Scope.make());
    closeScope = Scope.close(scope, Exit.void);
    const session = await Effect.runPromise(startLiveSynchronization({
      sourcePath,
      outputPath,
      reconcileIntervalMs: 0,
      recovery: { existing: Effect.succeed(true) },
      declare: () => Effect.succeed({ work: ["document"], publish: Effect.void }),
      handle: (path: string) => fail ? Effect.fail(new Error("opening failed")) : io(async () => { entered.open(); await release.promise; await Bun.write(join(outputPath, "reference"), await readFile(join(sourcePath, path), "utf8")); return [] as string[]; }),
    }).pipe(Scope.provide(scope))) as LiveHandle<string, Error>;
    await Effect.runPromise(Effect.exit(session.awaitCompletion));
    fail = false;
    await Effect.runPromise(session.requestPass({ force: true }));
    await entered.promise;
    const result = await Effect.runPromise(session.status.pipe(Effect.map((status) => status.followUp)));
    // #then the trigger is visible while it waits to become the post-open follow-up
    expect(result).toEqual({ kind: "resync", force: true, changedPaths: [] });
  } finally {
    release.open();
    if (closeScope !== undefined) await Effect.runPromise(closeScope);
    await rm(root, { recursive: true, force: true });
  }
});

test("a completion waiter request after recoverable open failure starts the reopen", async () => {
  // #given a warm session whose opening fails with no queued retry
  const root = await mkdtemp(join(tmpdir(), "sync-engine-live-inline-completion-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  await mkdir(outputPath);
  await Bun.write(join(sourcePath, "document"), "Recovered");
  await Bun.write(join(outputPath, "reference"), "Prior");
  let failing = true;
  try {
    const result = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const session = yield* startLiveSynchronization({
        sourcePath,
        outputPath,
        reconcileIntervalMs: 0,
        recovery: { existing: Effect.succeed(true) },
        declare: () => Effect.succeed({ work: ["document"], publish: Effect.void }),
        handle: (path: string) => failing
          ? Effect.fail(new Error("opening failed"))
          : io(async () => { await Bun.write(join(outputPath, "reference"), await readFile(join(sourcePath, path), "utf8")); return [] as string[]; }),
      });
      // #when an awaitCompletion waiter requests a retry inline after the failure
      const admission = yield* Effect.exit(session.awaitCompletion).pipe(Effect.andThen(() => {
        failing = false;
        return session.requestPass();
      }));
      yield* session.awaitCompletion;
      return { admission, reference: yield* io(() => readFile(join(outputPath, "reference"), "utf8")) };
    })));
    // #then the request starts the reopen and runs instead of being queued forever
    expect(result).toEqual({ admission: "started", reference: "Recovered" });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("a request stopped during async invalidation stays rejected", async () => {
  // #given an opened session with a request paused after freshness invalidation
  const root = await mkdtemp(join(tmpdir(), "sync-engine-live-stop-gap-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  await Bun.write(join(sourcePath, "note.txt"), "Original");
  const invalidated = gate();
  const releaseInvalidation = gate();
  try {
    const scope = await Effect.runPromise(Scope.make());
    const session = await Effect.runPromise(startLiveSynchronizationWithHooks({
      sourcePath,
      outputPath,
      freshness: { describe: () => ({ sourcePaths: ["note.txt"], resultKind: "note", processingVersion: "v1", outputPaths: ["note.txt"] }) },
      declare: () => Effect.succeed({ work: ["note.txt"], publish: Effect.void }),
      handle: (path: string) => io(async () => { await Bun.write(join(outputPath, path), await readFile(join(sourcePath, path), "utf8")); return [] as string[]; }),
    }, {
      afterRequestInvalidation: io(async () => { invalidated.open(); await releaseInvalidation.promise; }).pipe(Effect.orDie),
    }).pipe(Scope.provide(scope))) as LiveHandle<string, Error>;
    await Effect.runPromise(session.ready);
    const notifying = Effect.runPromise(session.notify(["note.txt"]));
    await invalidated.promise;
    // #when the owning scope closes during the async invalidation gap
    await Effect.runPromise(Scope.close(scope, Exit.void));
    releaseInvalidation.open();
    const admission = await notifying;
    const status = await Effect.runPromise(session.status);
    const later = await Effect.runPromise(session.requestPass());
    // #then the in-flight request cannot bring the stopped session back to life
    expect({ admission, state: status.state, followUp: status.followUp, later }).toEqual({ admission: "rejected", state: "stopped", followUp: null, later: "rejected" });
  } finally {
    releaseInvalidation.open();
    await rm(root, { recursive: true, force: true });
  }
});

test("ready waiters cannot orphan an outstanding retry completion", async () => {
  // #given a recoverable opening failure with a queued retry and an existing completion waiter
  const root = await mkdtemp(join(tmpdir(), "sync-engine-live-ready-inline-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  await mkdir(outputPath);
  await Bun.write(join(sourcePath, "document"), "Recovered");
  await Bun.write(join(outputPath, "reference"), "Prior");
  let failFirst = true;
  const entered = gate();
  const release = gate();
  try {
    const scope = await Effect.runPromise(Scope.make());
    const session = await Effect.runPromise(startLiveSynchronization({
      sourcePath,
      outputPath,
      reconcileIntervalMs: 0,
      recovery: { existing: Effect.succeed(true) },
      declare: () => Effect.succeed({ work: ["document"], publish: Effect.void }),
      handle: (path: string) => io(async () => {
        if (failFirst) {
          failFirst = false;
          entered.open();
          await release.promise;
          throw new Error("opening failed");
        }
        await Bun.write(join(outputPath, "reference"), await readFile(join(sourcePath, path), "utf8"));
        return [] as string[];
      }),
    }).pipe(Scope.provide(scope))) as LiveHandle<string, Error>;
    const completion = Effect.runPromise(Effect.exit(session.awaitCompletion));
    const readyThenRequest = Effect.runPromise(session.ready.pipe(Effect.andThen(session.requestPass())));
    await entered.promise;
    await Effect.runPromise(session.requestPass());
    // #when ready resumes callers inline during retry setup
    release.open();
    const settled = await Effect.runPromise(Effect.race(Effect.promise(() => completion).pipe(Effect.as("settled")), Effect.sleep(250).pipe(Effect.as("hung"))));
    const admission = await readyThenRequest;
    await Effect.runPromise(Scope.close(scope, Exit.void));
    // #then the original completion waiter is not orphaned
    expect({ settled, admission }).toEqual({ settled: "settled", admission: "queued" });
  } finally {
    release.open();
    await rm(root, { recursive: true, force: true });
  }
});

test("a failed post-open follow-up does not reopen without a request", async () => {
  // #given after-initial changes that queue a follow-up which then defects
  const root = await mkdtemp(join(tmpdir(), "sync-engine-live-no-unrequested-reopen-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  await Bun.write(join(sourcePath, "document"), "Original");
  let failWatcher = false;
  let initialChanged = false;
  let declarations = 0;
  try {
    const result = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const session = yield* openLiveSynchronization({
        sourcePath,
        outputPath,
        reconcileIntervalMs: 0,
        recovery: { existing: Effect.succeed(true) },
        declare: (_entries, request) => Effect.sync(() => {
          declarations += 1;
          if (failWatcher && request.kind === "watcher") throw new Error("watcher defect");
          return { work: ["document"], publish: Effect.void };
        }),
        handle: (path: string) => io(async () => {
          await Bun.write(join(outputPath, "reference"), await readFile(join(sourcePath, path), "utf8"));
          if (!initialChanged) {
            initialChanged = true;
            failWatcher = true;
            await Bun.write(join(sourcePath, "document"), "Changed");
          }
          return [] as string[];
        }),
      });
      const failed = yield* Effect.exit(session.awaitCompletion);
      yield* Effect.sleep(50);
      const status = yield* session.status;
      return { failed: Exit.isFailure(failed), declarations, state: status.state, failure: status.failure !== null };
    })));
    // #then no unrequested reopen runs
    expect(result).toEqual({ failed: true, declarations: 2, state: "failed", failure: true });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("a successful first publication makes later defects recoverable on a cold start", async () => {
  // #given no prior output but recovery is configured for future attempts
  const root = await mkdtemp(join(tmpdir(), "sync-engine-live-cold-recovery-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  await Bun.write(join(sourcePath, "document"), "Original");
  let defect = false;
  try {
    const result = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const session = yield* openLiveSynchronization({
        sourcePath,
        outputPath,
        reconcileIntervalMs: 0,
        recovery: { existing: Effect.succeed(false) },
        declare: () => defect ? Effect.die("later defect") : Effect.succeed({ work: ["document"], publish: Effect.void }),
        handle: (path: string) => io(async () => { await Bun.write(join(outputPath, "reference"), await readFile(join(sourcePath, path), "utf8")); return []; }),
      });
      defect = true;
      yield* session.requestPass();
      const failed = yield* Effect.exit(session.awaitCompletion);
      const admission = yield* session.requestPass();
      const status = yield* session.status;
      return { failed: Exit.isFailure(failed), admission, state: status.state, availability: status.availability };
    })));
    // #then the completed first publication is usable output for recovery
    expect(result).toEqual({ failed: true, admission: "started", state: "working", availability: "minimum-publication" });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("publication before opening failure counts as usable output", async () => {
  // #given a cold start whose publication succeeds before an internal opening failure
  const root = await mkdtemp(join(tmpdir(), "sync-engine-live-published-before-fail-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  await Bun.write(join(sourcePath, "document"), "Published");
  try {
    const result = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const session = yield* startLiveSynchronizationWithHooks({
        sourcePath,
        outputPath,
        reconcileIntervalMs: 0,
        recovery: { existing: Effect.succeed(false) },
        declare: () => Effect.succeed({ work: ["document"], publish: Effect.void }),
        handle: (path: string) => io(async () => { await Bun.write(join(outputPath, "reference"), await readFile(join(sourcePath, path), "utf8")); return [] as string[]; }),
      }, { afterOpeningPublication: Effect.die("after publication") });
      const ready = yield* Effect.exit(session.ready);
      const status = yield* session.status;
      return { readyFailed: Exit.isFailure(ready), state: status.state, availability: status.availability, reference: yield* io(() => readFile(join(outputPath, "reference"), "utf8")) };
    })));
    // #then the session is recoverable because the first publication is usable output
    expect(result).toEqual({ readyFailed: false, state: "failed", availability: "minimum-publication", reference: "Published" });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("admitting a retry keeps the last failure visible until success", async () => {
  // #given a recoverable failed opening
  const root = await mkdtemp(join(tmpdir(), "sync-engine-live-failure-retained-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  await mkdir(outputPath);
  await Bun.write(join(sourcePath, "document"), "Recovered");
  await Bun.write(join(outputPath, "reference"), "Prior");
  let releaseRetry = false;
  try {
    const result = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const session = yield* startLiveSynchronization({
        sourcePath,
        outputPath,
        reconcileIntervalMs: 0,
        recovery: { existing: Effect.succeed(true) },
        declare: () => Effect.succeed({ work: ["document"], publish: Effect.void }),
        handle: (path: string) => releaseRetry
          ? io(async () => { await Bun.write(join(outputPath, "reference"), await readFile(join(sourcePath, path), "utf8")); return [] as string[]; })
          : Effect.fail(new Error("opening failed")),
      });
      yield* Effect.exit(session.awaitCompletion);
      releaseRetry = true;
      const admission = yield* session.requestPass();
      const during = yield* session.status;
      yield* session.awaitCompletion;
      const after = yield* session.status;
      return { admission, duringFailure: during.failure !== null, afterFailure: after.failure !== null };
    })));
    // #then failure remains visible during retry and clears only after success
    expect(result).toEqual({ admission: "started", duringFailure: true, afterFailure: false });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("fatal stop clears terminal follow-up state", async () => {
  // #given a fatal opening failure with a queued request
  const root = await mkdtemp(join(tmpdir(), "sync-engine-live-terminal-followup-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  const entered = gate();
  const release = gate();
  await mkdir(sourcePath);
  await Bun.write(join(sourcePath, "document"), "Original");
  try {
    const result = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const session = yield* startLiveSynchronization({
        sourcePath,
        outputPath,
        declare: () => Effect.succeed({ work: ["document"], publish: Effect.void }),
        handle: () => io(async () => { entered.open(); await release.promise; throw new Error("fatal open"); }),
      });
      yield* io(() => entered.promise);
      const admission = yield* session.requestPass();
      release.open();
      yield* Effect.exit(session.ready);
      const status = yield* session.status;
      return { admission, state: status.state, followUp: status.followUp };
    })));
    // #then terminal status does not expose a follow-up that can never run
    expect(result).toEqual({ admission: "queued", state: "stopped", followUp: null });
  } finally {
    release.open();
    await rm(root, { recursive: true, force: true });
  }
});

test("notify during the after-initial opening window invalidates the opening commit", async () => {
  // #given metadata freshness and an internal pause in the opening window after publication
  const root = await mkdtemp(join(tmpdir(), "sync-engine-live-after-initial-notify-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  const stamp = new Date("2020-01-01T00:00:00Z");
  await mkdir(sourcePath);
  await Bun.write(join(sourcePath, "note.txt"), "Original");
  await utimes(join(sourcePath, "note.txt"), stamp, stamp);
  let failWatcher = false;
  const watcherEntered = gate();
  const options = {
    sourcePath,
    outputPath,
    reconcileIntervalMs: 0,
    freshness: { describe: () => ({ sourcePaths: ["note.txt"], resultKind: "note", processingVersion: "v1", outputPaths: ["note.txt"] }) },
    declare: (_entries: unknown, request: { readonly kind: string }) => failWatcher && request.kind === "watcher"
      ? io(async () => { watcherEntered.open(); throw new Error("stop before watcher submit"); })
      : Effect.succeed({ work: ["note.txt"], publish: Effect.void }),
    handle: (path: string) => io(async () => { await Bun.write(join(outputPath, path), await readFile(join(sourcePath, path), "utf8")); return [] as string[]; }),
  };
  const initialOptions = { ...options, declare: () => Effect.succeed({ work: ["note.txt"], publish: Effect.void }) };
  try {
    const scope = await Effect.runPromise(Scope.make());
    let session!: LiveHandle<string, Error>;
    session = await Effect.runPromise(startLiveSynchronizationWithHooks(options, {
      afterOpeningPublication: io(async () => {
        await Bun.write(join(sourcePath, "note.txt"), "Changed!");
        await utimes(join(sourcePath, "note.txt"), stamp, stamp);
        failWatcher = true;
        await Effect.runPromise(session.notify(["note.txt"]));
      }).pipe(Effect.orDie),
    }).pipe(Scope.provide(scope))) as LiveHandle<string, Error>;
    await Effect.runPromise(session.ready);
    await watcherEntered.promise;
    await Effect.runPromise(Effect.exit(session.awaitCompletion));
    await Effect.runPromise(Scope.close(scope, Exit.void));
    await Effect.runPromise(runInitialPass(initialOptions));
    // #then a fresh session replays the replacement instead of trusting the opening commit
    expect(await readFile(join(outputPath, "note.txt"), "utf8")).toBe("Changed!");
  } finally {
    watcherEntered.open();
    await rm(root, { recursive: true, force: true });
  }
});

test("notify during active work prevents the active stale read from being retained", async () => {
  // #given retained metadata freshness and a force pass held after reading the old source
  const root = await mkdtemp(join(tmpdir(), "sync-engine-live-active-notify-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  const stamp = new Date("2020-01-01T00:00:00Z");
  await mkdir(sourcePath);
  await Bun.write(join(sourcePath, "note.txt"), "Original");
  await utimes(join(sourcePath, "note.txt"), stamp, stamp);
  const options = {
    sourcePath, outputPath,
    freshness: { describe: () => ({ sourcePaths: ["note.txt"], resultKind: "note", processingVersion: "v1", outputPaths: ["note.txt"] }) },
    declare: (_entries: unknown, request: { readonly kind: string }) => request.kind === "watcher"
      ? io(async () => { watcherEntered.open(); throw new Error("stop before watcher submit"); })
      : Effect.succeed({ work: ["note.txt"], publish: Effect.void }),
    handle: (path: string) => io(async () => {
      const bytes = await readFile(join(sourcePath, path), "utf8");
      if (holdActive) {
        holdActive = false;
        activeRead.open();
        await releaseActive.promise;
      }
      await Bun.write(join(outputPath, path), bytes);
      return [] as string[];
    }),
  };
  const initialOptions = {
    ...options,
    declare: () => Effect.succeed({ work: ["note.txt"], publish: Effect.void }),
  };
  let holdActive = false;
  const activeRead = gate();
  const releaseActive = gate();
  const watcherEntered = gate();
  try {
    const scope = await Effect.runPromise(Scope.make());
    const session = await Effect.runPromise(openLiveSynchronization(options).pipe(Scope.provide(scope))) as LiveSynchronization<string, Error>;
    holdActive = true;
    await Effect.runPromise(session.requestPass({ force: true }));
    await activeRead.promise;
    // #when an equal-stamp replacement is notified while the stale read is active
    await Bun.write(join(sourcePath, "note.txt"), "Changed!");
    await utimes(join(sourcePath, "note.txt"), stamp, stamp);
    await Effect.runPromise(session.notify(["note.txt"]));
    releaseActive.open();
    await watcherEntered.promise;
    await Effect.runPromise(Effect.exit(session.awaitCompletion));
    await Effect.runPromise(Scope.close(scope, Exit.void));
    await Effect.runPromise(runInitialPass(initialOptions));
    // #then a fresh session replays the replacement instead of trusting the active read's stale record
    expect(await readFile(join(outputPath, "note.txt"), "utf8")).toBe("Changed!");
  } finally {
    releaseActive.open();
    watcherEntered.open();
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

test("fatal first-pass completion waiters resume with terminal admission already closed", async () => {
  // #given a live handle whose unrecoverable first pass is awaited through awaitCompletion
  const root = await mkdtemp(join(tmpdir(), "sync-engine-live-fatal-waiter-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  try {
    const result = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const session = yield* startLiveSynchronization({
        sourcePath, outputPath,
        declare: () => Effect.succeed({ work: ["document"], publish: Effect.void }),
        handle: (_path: string) => Effect.succeed([]),
      });
      const completion = yield* Effect.exit(session.awaitCompletion);
      // #when the waiter asks for another pass immediately after completion failure
      const admission = yield* session.requestPass();
      const status = yield* session.status;
      return { completionFailed: Exit.isFailure(completion), admission, state: status.state };
    })));
    // #then no unreachable pass is admitted
    expect(result).toEqual({ completionFailed: true, admission: "rejected", state: "stopped" });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("adding a child file does not invalidate unchanged sibling files through the parent directory", async () => {
  // #given retained sibling files under the same directory
  const root = await mkdtemp(join(tmpdir(), "sync-engine-live-directory-add-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(join(sourcePath, "book"), { recursive: true });
  await Bun.write(join(sourcePath, "book", "one.txt"), "One");
  await Bun.write(join(sourcePath, "book", "two.txt"), "Two");
  try {
    const result = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const handled: string[] = [];
      const session = yield* openLiveSynchronization({
        sourcePath, outputPath,
        freshness: { describe: (path: string) => ({ sourcePaths: [path], resultKind: "file", processingVersion: "v1", outputPaths: [path] }) },
        declare: (entries) => Effect.succeed({ work: entries.filter((entry) => entry.kind === "file").map((entry) => entry.path), publish: Effect.void }),
        handle: (path: string) => io(async () => {
          handled.push(path);
          await mkdir(join(outputPath, "book"), { recursive: true });
          await Bun.write(join(outputPath, path), await readFile(join(sourcePath, path), "utf8"));
          return [];
        }),
      });
      handled.length = 0;
      // #when one child is added and the parent directory mtime changes
      yield* io(() => Bun.write(join(sourcePath, "book", "three.txt"), "Three"));
      yield* session.requestPass();
      yield* session.awaitCompletion;
      return handled;
    })));
    // #then only the new file needs processing
    expect(result).toEqual(["book/three.txt"]);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("directory removal and kind changes keep prefix invalidation", async () => {
  // #given retained records under a directory and at a path that will change kind
  const root = await mkdtemp(join(tmpdir(), "sync-engine-live-directory-kind-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  const stamp = new Date("2020-01-01T00:00:00Z");
  await mkdir(join(sourcePath, "gone"), { recursive: true });
  await Bun.write(join(sourcePath, "gone", "one.txt"), "Gone one");
  await Bun.write(join(sourcePath, "flip"), "File first");
  await utimes(join(sourcePath, "gone", "one.txt"), stamp, stamp);
  await utimes(join(sourcePath, "flip"), stamp, stamp);
  try {
    const result = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const handled: string[] = [];
      const requests: string[][] = [];
      const session = yield* openLiveSynchronization({
        sourcePath, outputPath,
        freshness: { describe: (path: string) => ({ sourcePaths: [path], resultKind: "file", processingVersion: "v1", outputPaths: [`out-${path.replaceAll("/", "__")}`] }) },
        declare: (entries, request) => Effect.sync(() => {
          requests.push([...request.changedPaths]);
          return { work: entries.filter((entry) => entry.kind === "file").map((entry) => entry.path), publish: Effect.void };
        }),
        handle: (path: string) => io(async () => {
          handled.push(path);
          await Bun.write(join(outputPath, `out-${path.replaceAll("/", "__")}`), await readFile(join(sourcePath, path), "utf8"));
          return [];
        }),
      });

      handled.length = 0;
      // #when a directory is removed, then recreated with equal child metadata
      yield* io(() => rm(join(sourcePath, "gone"), { recursive: true }));
      yield* session.requestPass();
      yield* session.awaitCompletion;
      const removalHints = requests.at(-1) ?? [];
      yield* io(() => mkdir(join(sourcePath, "gone")));
      yield* io(() => Bun.write(join(sourcePath, "gone", "one.txt"), "Gone one"));
      yield* io(() => utimes(join(sourcePath, "gone", "one.txt"), stamp, stamp));
      yield* session.requestPass();
      yield* session.awaitCompletion;
      const afterRemoval = [...handled];

      handled.length = 0;
      // #and a file path changes to a directory and back to the same file metadata
      const old = yield* io(() => stat(join(sourcePath, "flip")));
      yield* io(() => rm(join(sourcePath, "flip")));
      yield* io(() => mkdir(join(sourcePath, "flip")));
      yield* io(() => Bun.write(join(sourcePath, "flip", "child.txt"), "Child"));
      yield* session.requestPass();
      yield* session.awaitCompletion;
      const fileToDirectoryHints = requests.at(-1) ?? [];
      yield* io(() => rm(join(sourcePath, "flip"), { recursive: true }));
      yield* io(() => Bun.write(join(sourcePath, "flip"), "File first"));
      yield* io(() => utimes(join(sourcePath, "flip"), old.atime, old.mtime));
      yield* session.requestPass();
      yield* session.awaitCompletion;
      return { afterRemoval, afterKindChange: [...handled], removalHints, fileToDirectoryHints };
    })));
    // #then removed directories and kind changes drop the old retained records
    expect(result).toEqual({ afterRemoval: ["gone/one.txt"], afterKindChange: ["flip/child.txt", "flip"], removalHints: ["gone", "gone/one.txt"], fileToDirectoryHints: ["flip", "flip/child.txt"] });
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
          : Effect.succeed({ work: request.changedPaths.includes("note.txt") ? ["note.txt"] : [], publish: Effect.void }),
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

type ClosingWindow = "before attempt close" | "after attempt close";

async function runClosingWindowScenario(window: ClosingWindow, retry: "succeeds" | "fails after publication") {
  const root = await mkdtemp(join(tmpdir(), "sync-engine-live-closing-window-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  const stamp = new Date("2020-01-01T00:00:00Z");
  await mkdir(sourcePath);
  await Bun.write(join(sourcePath, "note.txt"), "Original");
  await utimes(join(sourcePath, "note.txt"), stamp, stamp);
  const followUps: PassSnapshot[] = [];
  let dieOnTrigger = false;
  let failRetryAfterPublication = false;
  const admissions: string[] = [];
  const options = {
    sourcePath,
    outputPath,
    reconcileIntervalMs: 0,
    recovery: { existing: Effect.succeed(true) },
    // Only note.txt is cacheable; the trigger always runs, so the failing pass carries no force or hint of its own.
    freshness: { describe: (path: string) => path === "note.txt" ? { sourcePaths: ["note.txt"], resultKind: "note", processingVersion: "v1", outputPaths: ["note.txt"] } : undefined },
    declare: (_entries: unknown, request: PassSnapshot) => Effect.sync(() => {
      if (request.kind !== "initial") followUps.push({ kind: request.kind, force: request.force, changedPaths: [...request.changedPaths] });
      return { work: ["note.txt", "trigger"], publish: Effect.void };
    }),
    handle: (path: string) => path === "trigger"
      ? (dieOnTrigger ? Effect.sync(() => { dieOnTrigger = false; }).pipe(Effect.andThen(Effect.die("recoverable running defect"))) : Effect.succeed([] as string[]))
      : io(async () => { await Bun.write(join(outputPath, path), await readFile(join(sourcePath, path), "utf8")); return [] as string[]; }),
  };
  try {
    const scope = await Effect.runPromise(Scope.make());
    let session!: LiveHandle<string, Error>;
    // #when the window of the failed attempt receives an equal-stamp replacement hint and a forced request
    let windowUsed = false;
    // One window only: a later close of the failed retry must not admit again.
    const admitInWindow = Effect.suspend(() => windowUsed ? Effect.void : io(async () => {
      windowUsed = true;
      await Bun.write(join(sourcePath, "note.txt"), "Changed!");
      await utimes(join(sourcePath, "note.txt"), stamp, stamp);
      if (retry === "fails after publication") failRetryAfterPublication = true;
      admissions.push(await Effect.runPromise(session.notify(["note.txt"])));
      admissions.push(await Effect.runPromise(session.requestPass({ force: true })));
    }).pipe(Effect.orDie));
    session = await Effect.runPromise(startLiveSynchronizationWithHooks(options, {
      beforeAttemptClose: () => window === "before attempt close" ? admitInWindow : Effect.void,
      afterAttemptClose: () => window === "after attempt close" ? admitInWindow : Effect.void,
      afterOpeningPublication: Effect.suspend(() => failRetryAfterPublication ? Effect.die("retry failed after publication") : Effect.void),
    }).pipe(Scope.provide(scope))) as LiveHandle<string, Error>;
    await Effect.runPromise(session.ready);
    dieOnTrigger = true;
    await Effect.runPromise(session.requestPass());
    const completion = await Effect.runPromise(Effect.exit(session.awaitCompletion));
    const status = await Effect.runPromise(session.status);
    const published = await readFile(join(outputPath, "note.txt"), "utf8");
    await Effect.runPromise(Scope.close(scope, Exit.void));
    // A fresh session trusts only retained freshness, so it shows whether the window's requests reached it.
    await Effect.runPromise(runInitialPass({ ...options, declare: () => Effect.succeed({ work: ["note.txt"], publish: Effect.void }) }));
    const payloadBearing = followUps.filter((request) => request.force || request.changedPaths.length > 0);
    return { admissions, completion: Exit.isSuccess(completion) ? "succeeded" : "failed", state: status.state, followUp: status.followUp, published, payloadBearing, restarted: await readFile(join(outputPath, "note.txt"), "utf8") };
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

test.each(["before attempt close", "after attempt close"] as const)("requests in the closing window of a recoverable running failure reach the retry once: %s", async (window) => {
  // #given an opened recoverable session whose follow-up pass dies, and a retry that succeeds
  const result = await runClosingWindowScenario(window, "succeeds");
  // #then both requests are queued for the retry, its declare gets their union once, and output and restart are current
  expect(result).toEqual({
    admissions: ["queued", "queued"],
    completion: "succeeded",
    state: "complete",
    followUp: null,
    published: "Changed!",
    payloadBearing: [{ kind: "resync", force: true, changedPaths: ["note.txt"] }],
    restarted: "Changed!",
  });
});

test.each(["before attempt close", "after attempt close"] as const)("requests in the closing window of a recoverable running failure reach freshness when the retry fails: %s", async (window) => {
  // #given an opened recoverable session whose follow-up pass dies, and a retry that fails after its publication
  const result = await runClosingWindowScenario(window, "fails after publication");
  // #then the session waits failed with nothing scheduled, and a restart rebuilds instead of trusting the old record
  expect(result).toEqual({
    admissions: ["queued", "queued"],
    completion: "failed",
    state: "failed",
    followUp: null,
    published: "Original",
    payloadBearing: [],
    restarted: "Changed!",
  });
});

test("a fatal first pass fails ready only after the output lease is released", async () => {
  // #given a cold session without recovery whose first pass fails, with the attempt close observed
  const root = await mkdtemp(join(tmpdir(), "sync-engine-live-fatal-lease-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  await Bun.write(join(sourcePath, "document"), "Original");
  const order: string[] = [];
  try {
    const result = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const session = yield* startLiveSynchronizationWithHooks({
        sourcePath,
        outputPath,
        declare: () => Effect.succeed({ work: ["document"], publish: Effect.void }),
        handle: () => Effect.fail(new Error("first pass failed")),
      }, {
        beforeAttemptClose: () => Effect.sync(() => { order.push("closing attempt"); }),
        afterAttemptClose: () => Effect.sync(() => { order.push("attempt scope closed"); }),
      });
      // #when ready fails, the caller immediately takes the output lease as a restart would
      const ready = yield* Effect.exit(session.ready);
      order.push("ready failed");
      const lease = yield* Effect.exit(acquireOutputTree(outputPath));
      if (Exit.isSuccess(lease)) yield* Effect.promise(lease.value);
      return { readyFailed: Exit.isFailure(ready), lease: Exit.isSuccess(lease) ? "acquired" : "contended", admission: yield* session.requestPass() };
    })));
    // #then the attempt scope, which holds the lease, closed before ready failed, and admission is closed
    expect({ ...result, order }).toEqual({ readyFailed: true, lease: "acquired", admission: "rejected", order: ["closing attempt", "attempt scope closed", "ready failed"] });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("scoped resources of a later pass are released when its failed attempt closes", async () => {
  // #given a recoverable session whose later declaration acquires a scoped resource and whose publication dies
  const root = await mkdtemp(join(tmpdir(), "sync-engine-live-pass-scope-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  await Bun.write(join(sourcePath, "document"), "Original");
  const resource = { acquired: 0, released: 0 };
  try {
    const result = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const session = yield* openLiveSynchronization({
        sourcePath,
        outputPath,
        reconcileIntervalMs: 0,
        recovery: { existing: Effect.succeed(true) },
        declare: (_entries, request) => request.kind === "initial"
          ? Effect.succeed({ work: ["document"], publish: Effect.void })
          : Effect.acquireRelease(Effect.sync(() => { resource.acquired += 1; }), () => Effect.sync(() => { resource.released += 1; }))
            .pipe(Effect.as({ work: ["document"], publish: Effect.die("publication defect") })),
        handle: (path: string) => io(async () => { await Bun.write(join(outputPath, path), await readFile(join(sourcePath, path), "utf8")); return [] as string[]; }),
      });
      // #when the later pass dies and its attempt closes while the session stays open
      yield* session.requestPass();
      const completion = yield* Effect.exit(session.awaitCompletion);
      return { failed: Exit.isFailure(completion), state: (yield* session.status).state, resource: { ...resource } };
    })));
    // #then the resource belonged to the failed attempt, not to the whole session
    expect(result).toEqual({ failed: true, state: "failed", resource: { acquired: 1, released: 1 } });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("a later pass's resources are released only after the scheduler stopped the handlers still using them", async () => {
  // #given two workers: one handler of a later pass is still running when the other dies
  const root = await mkdtemp(join(tmpdir(), "sync-engine-live-pass-order-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  await Bun.write(join(sourcePath, "document"), "Original");
  const order: string[] = [];
  const running = gate();
  try {
    const result = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const session = yield* openLiveSynchronization({
        sourcePath,
        outputPath,
        concurrency: 2,
        reconcileIntervalMs: 0,
        recovery: { existing: Effect.succeed(true) },
        declare: (_entries, request) => request.kind === "initial"
          ? Effect.succeed({ work: ["document"], publish: Effect.void })
          : Effect.acquireRelease(Effect.void, () => Effect.sync(() => { order.push("pass resource released"); }))
            .pipe(Effect.as({ work: ["still running", "dies"], publish: Effect.void })),
        handle: (work: string) => {
          if (work === "still running") return Effect.sync(() => running.open()).pipe(Effect.andThen(Effect.never), Effect.onInterrupt(() => Effect.sync(() => { order.push("handler stopped"); })));
          if (work === "dies") return Effect.promise(() => running.promise).pipe(Effect.andThen(Effect.die("handler defect")));
          return io(async () => { await Bun.write(join(outputPath, work), await readFile(join(sourcePath, work), "utf8")); return [] as string[]; });
        },
      });
      // #when the scheduler fails while the other handler is in flight, and the failed attempt closes
      yield* session.requestPass();
      const completion = yield* Effect.exit(session.awaitCompletion);
      return { failed: Exit.isFailure(completion), order: [...order] };
    })));
    // #then the running handler is stopped before the resource it may use is released
    expect(result).toEqual({ failed: true, order: ["handler stopped", "pass resource released"] });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("a successful later pass releases its scoped resources when it ends", async () => {
  // #given a healthy session whose later declarations each acquire a scoped resource
  const root = await mkdtemp(join(tmpdir(), "sync-engine-live-pass-release-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  await Bun.write(join(sourcePath, "document"), "Original");
  const resource = { acquired: 0, released: 0 };
  try {
    const result = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const session = yield* openLiveSynchronization({
        sourcePath,
        outputPath,
        reconcileIntervalMs: 0,
        declare: (_entries, request) => request.kind === "initial"
          ? Effect.succeed({ work: ["document"], publish: Effect.void })
          : Effect.acquireRelease(Effect.sync(() => { resource.acquired += 1; }), () => Effect.sync(() => { resource.released += 1; }))
            .pipe(Effect.as({ work: ["document"], publish: Effect.void })),
        handle: (path: string) => io(async () => { await Bun.write(join(outputPath, path), await readFile(join(sourcePath, path), "utf8")); return [] as string[]; }),
      });
      // #when three later passes succeed while the session stays open
      for (let pass = 0; pass < 3; pass += 1) {
        yield* session.requestPass();
        yield* session.awaitCompletion;
      }
      return { state: (yield* session.status).state, resource: { ...resource } };
    })));
    // #then no pass holds its resource after it ended
    expect(result).toEqual({ state: "complete", resource: { acquired: 3, released: 3 } });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("a failed traversal after the initial pass fails the opening although it published", async () => {
  // #given a session without recovery whose source root vanishes right after the initial publication
  const root = await mkdtemp(join(tmpdir(), "sync-engine-live-opening-traversal-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  await Bun.write(join(sourcePath, "document"), "Original");
  try {
    const scope = await Effect.runPromise(Scope.make());
    const session = await Effect.runPromise(startLiveSynchronizationWithHooks({
      sourcePath,
      outputPath,
      reconcileIntervalMs: 0,
      declare: () => Effect.succeed({ work: ["document"], publish: Effect.void }),
      handle: (path: string) => io(async () => { await Bun.write(join(outputPath, path), await readFile(join(sourcePath, path), "utf8")); return [] as string[]; }),
    }, {
      afterOpeningPublication: io(() => rename(sourcePath, join(root, "parked"))).pipe(Effect.orDie),
    }).pipe(Scope.provide(scope))) as LiveHandle<string, Error>;
    // #when the opening's trailing traversal fails
    const ready = await Effect.runPromise(Effect.exit(session.ready));
    const state = (await Effect.runPromise(session.status)).state;
    const published = await readFile(join(outputPath, "document"), "utf8");
    await Effect.runPromise(Scope.close(scope, Exit.void));
    // #then the opening fails with the traversal error and the session stops, as the README states
    expect({ ready: Exit.isFailure(ready) ? Cause.squash(ready.cause) : "succeeded", state, published }).toEqual({
      ready: expect.objectContaining({ _tag: "ScanFailed" }),
      state: "stopped",
      published: "Original",
    });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
