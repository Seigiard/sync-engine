import { test, expect } from "bun:test";
import { mkdir, mkdtemp, readFile, rm, stat, utimes } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Cause, Effect, Exit, Scope } from "effect";
import { acquireOutputTree, openLiveSynchronization, runInitialPass, startLiveSynchronization, type LiveHandle, type LiveSynchronization } from "../src/index.ts";

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
    // #then the pending request wakes the retry, then runs as the retained follow-up
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
    // #then the one-shot retry is consumed and the session waits instead of looping
    expect(result).toEqual({ admission: "queued", completionFailed: true, attempts: 2, state: "failed", followUp: null });
  } finally {
    release.open();
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

test("a stale wake after a failed post-open follow-up does not reopen without a request", async () => {
  // #given after-initial changes that queue a follow-up which then defects
  const root = await mkdtemp(join(tmpdir(), "sync-engine-live-stale-wake-"));
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
    // #then the stale wake is drained and no unrequested reopen runs
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

test("notify during the after-initial opening window invalidates the opening commit", async () => {
  // #given metadata freshness and a held after-initial scan after commit invalidation already ran
  const root = await mkdtemp(join(tmpdir(), "sync-engine-live-after-initial-notify-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  const stamp = new Date("2020-01-01T00:00:00Z");
  await mkdir(sourcePath);
  await Bun.write(join(sourcePath, "note.txt"), "Original");
  await utimes(join(sourcePath, "note.txt"), stamp, stamp);
  let scanCount = 0;
  let failWatcher = false;
  const afterInitialEntered = gate();
  const releaseAfterInitial = gate();
  const watcherEntered = gate();
  const options = {
    sourcePath,
    outputPath,
    reconcileIntervalMs: 0,
    includeSource: (path: string) => {
      if (path === "note.txt") {
        scanCount += 1;
        if (scanCount === 2) afterInitialEntered.open();
      }
      return true;
    },
    freshness: { describe: () => ({ sourcePaths: ["note.txt"], resultKind: "note", processingVersion: "v1", outputPaths: ["note.txt"] }) },
    declare: (_entries: unknown, request: { readonly kind: string }) => failWatcher && request.kind === "watcher"
      ? io(async () => { watcherEntered.open(); throw new Error("stop before watcher submit"); })
      : Effect.succeed({ work: ["note.txt"], publish: Effect.void }),
    handle: (path: string) => io(async () => { await Bun.write(join(outputPath, path), await readFile(join(sourcePath, path), "utf8")); return [] as string[]; }),
  };
  const initialOptions = { ...options, includeSource: undefined, declare: () => Effect.succeed({ work: ["note.txt"], publish: Effect.void }) };
  try {
    const scope = await Effect.runPromise(Scope.make());
    const session = await Effect.runPromise(startLiveSynchronization(options).pipe(Scope.provide(scope))) as LiveHandle<string, Error>;
    await afterInitialEntered.promise;
    // #when an equal-stamp replacement is notified after beforeCommit and before scheduler publication
    await Bun.write(join(sourcePath, "note.txt"), "Changed!");
    await utimes(join(sourcePath, "note.txt"), stamp, stamp);
    failWatcher = true;
    await Effect.runPromise(session.notify(["note.txt"]));
    releaseAfterInitial.open();
    await Effect.runPromise(session.ready);
    await watcherEntered.promise;
    await Effect.runPromise(Effect.exit(session.awaitCompletion));
    await Effect.runPromise(Scope.close(scope, Exit.void));
    await Effect.runPromise(runInitialPass(initialOptions));
    // #then a fresh session replays the replacement instead of trusting the opening commit
    expect(await readFile(join(outputPath, "note.txt"), "utf8")).toBe("Changed!");
  } finally {
    releaseAfterInitial.open();
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
