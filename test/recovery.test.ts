import { test, expect } from "bun:test";
import { access, mkdir, mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Effect, Scope, Exit } from "effect";
import { acquireOutputTree, openLiveSynchronization, startLiveSynchronization } from "../src/index.ts";

const io = <A>(run: () => Promise<A>) => Effect.tryPromise({ try: run, catch: (cause) => new Error(String(cause)) }).pipe(Effect.uninterruptible);

const exists = (path: string) => access(path).then(() => true, () => false);

function gate() {
  const { promise, resolve } = Promise.withResolvers<void>();
  return { promise, open: () => resolve() };
}

async function workspace(prefix: string) {
  const root = await mkdtemp(join(tmpdir(), prefix));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(outputPath);

  return { root, sourcePath, outputPath };
}

const copyDocument = (sourcePath: string, outputPath: string) => (path: string) =>
  io(async () => {
    await Bun.write(join(outputPath, "reference"), await readFile(join(sourcePath, path), "utf8"));

    return [];
  });

const waitFor = async (check: () => Promise<boolean>) => {
  for (let attempt = 0; attempt < 100; attempt++) {
    if (await check()) return true;
    await Bun.sleep(50);
  }

  return false;
};

test("a failed first pass keeps serving prior output, reports the failure and recovers on the next request", async () => {
  // #given earlier output and a source that cannot be read at start
  const { root, sourcePath, outputPath } = await workspace("sync-engine-warm-");
  await Bun.write(join(outputPath, "reference"), "Prior publication");

  try {
    const result = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          // #when the session opens
          const session = yield* startLiveSynchronization({
            sourcePath,
            outputPath,
            recovery: { existing: io(() => exists(join(outputPath, "reference"))).pipe(Effect.orDie) },
            declare: () => Effect.succeed({ work: ["document"], publish: Effect.void }),
            handle: copyDocument(sourcePath, outputPath),
          });
          yield* session.ready;
          const failed = yield* session.status;
          const kept = yield* io(() => readFile(join(outputPath, "reference"), "utf8"));
          // #and the source returns and a pass is requested
          yield* io(async () => {
            await mkdir(sourcePath);
            await Bun.write(join(sourcePath, "document"), "Current publication");
          });
          const admission = yield* session.requestPass();
          yield* session.awaitCompletion;
          const recovered = yield* session.status;

          return {
            failed: { state: failed.state, availability: failed.availability, failure: failed.failure !== null },
            kept,
            admission,
            recovered: { state: recovered.state, failure: recovered.failure },
            current: yield* io(() => readFile(join(outputPath, "reference"), "utf8")),
          };
        }),
      ),
    );
    // #then the open succeeded with the failure visible, then the same session repaired the output
    expect(result).toEqual({
      failed: { state: "failed", availability: "prior-output", failure: true },
      kept: "Prior publication",
      admission: "started",
      recovered: { state: "complete", failure: null },
      current: "Current publication",
    });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("a failed first pass without usable output fails the open", async () => {
  // #given no earlier output and an unreadable source
  const { root, sourcePath, outputPath } = await workspace("sync-engine-cold-");

  try {
    // #when the session opens with recovery enabled
    const outcome = await Effect.runPromise(
      Effect.scoped(
        openLiveSynchronization({
          sourcePath,
          outputPath,
          recovery: { existing: Effect.succeed(false) },
          declare: () => Effect.succeed({ work: ["document"], publish: Effect.void }),
          handle: copyDocument(sourcePath, outputPath),
        }).pipe(Effect.as("opened"), Effect.catchTag("ScanFailed", () => Effect.succeed("scan failed"))),
      ),
    );
    // #then recovery does not turn an empty deployment into a served one
    expect(outcome).toBe("scan failed");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("a pass failing after the minimum was published is served, and not a failed open", async () => {
  // #given no earlier output, a minimum that publishes and remaining work that fails
  const { root, sourcePath, outputPath } = await workspace("sync-engine-minimum-recovery-");
  await mkdir(sourcePath);

  try {
    const result = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const session = yield* openLiveSynchronization({
            sourcePath,
            outputPath,
            recovery: { existing: Effect.succeed(false) },
            declare: () => Effect.succeed({ minimum: ["minimum"], work: ["remaining"], publish: Effect.void }),
            onMinimum: Effect.void,
            handle: (work: string) => (work === "minimum" ? io(async () => (await Bun.write(join(outputPath, "minimum"), "ready"), [])) : Effect.fail(new Error("Remaining failed"))),
          });
          const status = yield* session.status;

          return { availability: status.availability, minimum: yield* io(() => readFile(join(outputPath, "minimum"), "utf8")) };
        }),
      ),
    );
    // #then the open resolved because the minimum is usable
    expect(result).toEqual({ availability: "minimum-publication", minimum: "ready" });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("the reconcile timer reopens a failed session without any request", async () => {
  // #given a served prior output, an unreadable source and a short owned timer
  const { root, sourcePath, outputPath } = await workspace("sync-engine-timer-");
  await Bun.write(join(outputPath, "reference"), "Prior publication");

  try {
    const result = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const session = yield* startLiveSynchronization({
            sourcePath,
            outputPath,
            reconcileIntervalMs: 100,
            recovery: { existing: Effect.succeed(true) },
            declare: () => Effect.succeed({ work: ["document"], publish: Effect.void }),
            handle: copyDocument(sourcePath, outputPath),
          });
          yield* session.ready;
          // #when the source returns and nobody asks
          yield* io(async () => {
            await mkdir(sourcePath);
            await Bun.write(join(sourcePath, "document"), "Timer publication");
          });
          const repaired = yield* io(() => waitFor(async () => (await readFile(join(outputPath, "reference"), "utf8")) === "Timer publication"));
          const status = yield* session.status;

          return { repaired, failure: status.failure };
        }),
      ),
    );
    // #then the library's own timer retried and repaired the output
    expect(result).toEqual({ repaired: true, failure: null });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("a disabled timer leaves a failed session waiting for a request", async () => {
  // #given the same failed warm session with the timer off
  const { root, sourcePath, outputPath } = await workspace("sync-engine-no-timer-");
  await Bun.write(join(outputPath, "reference"), "Prior publication");

  try {
    const result = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const session = yield* startLiveSynchronization({
            sourcePath,
            outputPath,
            recovery: { existing: Effect.succeed(true) },
            declare: () => Effect.succeed({ work: ["document"], publish: Effect.void }),
            handle: copyDocument(sourcePath, outputPath),
          });
          yield* session.ready;
          yield* io(async () => {
            await mkdir(sourcePath);
            await Bun.write(join(sourcePath, "document"), "Unrequested publication");
          });
          yield* io(() => Bun.sleep(400));

          return (yield* io(() => readFile(join(outputPath, "reference"), "utf8")));
        }),
      ),
    );
    // #then nothing retried by itself
    expect(result).toBe("Prior publication");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("requests made while the first pass is held combine and apply after it", async () => {
  // #given two sources and a first pass held inside its first handler
  const { root, sourcePath, outputPath } = await workspace("sync-engine-pre-open-");
  await mkdir(sourcePath);
  await Bun.write(join(sourcePath, "first"), "First original");
  await Bun.write(join(sourcePath, "second"), "Second unchanged");
  const entered = gate();
  const release = gate();
  let hold = true;

  try {
    const result = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const session = yield* startLiveSynchronization({
            sourcePath,
            outputPath,
            declare: (entries, request) =>
              Effect.succeed({
                work: entries
                  .filter((entry) => entry.kind === "file" && (request.kind === "initial" || request.force || request.changedPaths.includes(entry.path)))
                  .map((entry) => entry.path),
                publish: Effect.void,
              }),
            handle: (path: string) =>
              io(async () => {
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
          yield* io(() => entered.promise);
          // #when a hint and a forced request arrive before the open finished
          yield* io(() => Bun.write(join(outputPath, "second"), "Damaged old representation"));
          const admissions = [yield* session.notify(["first"]), yield* session.requestPass({ force: true })];
          release.open();
          yield* session.ready;
          yield* session.awaitCompletion;

          return { admissions, second: yield* io(() => readFile(join(outputPath, "second"), "utf8")) };
        }),
      ),
    );
    // #then both were admitted as queued and the forced mode repaired the unchanged source
    expect(result).toEqual({ admissions: ["queued", "queued"], second: "Second unchanged" });
  } finally {
    release.open();
    await rm(root, { recursive: true, force: true });
  }
});

test("a request made during a first pass that then fails does not block the next request from retrying", async () => {
  // #given prior output and a first pass held inside a handler that will fail
  const { root, sourcePath, outputPath } = await workspace("sync-engine-queued-then-failed-");
  await mkdir(sourcePath);
  await Bun.write(join(sourcePath, "document"), "Current publication");
  await Bun.write(join(outputPath, "reference"), "Prior publication");
  const entered = gate();
  const release = gate();
  let failing = true;

  try {
    const result = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const session = yield* startLiveSynchronization({
            sourcePath,
            outputPath,
            recovery: { existing: Effect.succeed(true) },
            declare: () => Effect.succeed({ work: ["document"], publish: Effect.void }),
            handle: (path: string) =>
              failing
                ? io(async () => {
                    entered.open();
                    await release.promise;
                    throw new Error("Cannot prepare document");
                  })
                : copyDocument(sourcePath, outputPath)(path),
          });
          yield* io(() => entered.promise);
          // #when a request arrives during the open and the open then fails
          const during = yield* session.requestPass();
          failing = false;
          release.open();
          yield* session.ready;
          const failed = yield* session.status;
          // #and a later request arrives
          const later = yield* session.requestPass();
          yield* session.awaitCompletion;

          return { during, failed: failed.state, later, current: yield* io(() => readFile(join(outputPath, "reference"), "utf8")) };
        }),
      ),
    );
    // #then the later request retried instead of queueing behind the stale one
    expect(result).toEqual({ during: "queued", failed: "failed", later: "started", current: "Current publication" });
  } finally {
    release.open();
    await rm(root, { recursive: true, force: true });
  }
});

test("a request made right after ready on an unchanged source starts a pass instead of queueing", async () => {
  // #given an opened session over an unchanged source
  const { root, sourcePath, outputPath } = await workspace("sync-engine-idle-after-ready-");
  await mkdir(sourcePath);
  await Bun.write(join(sourcePath, "document"), "Publication");

  try {
    const result = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const session = yield* openLiveSynchronization({
            sourcePath,
            outputPath,
            declare: () => Effect.succeed({ work: ["document"], publish: Effect.void }),
            handle: copyDocument(sourcePath, outputPath),
          });
          // #when a request arrives as soon as the open returned
          return yield* session.requestPass();
        }),
      ),
    );
    // #then nothing was pending, so the request is the pass
    expect(result).toBe("started");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("closing the scope while a failed session waits releases the output lease", async () => {
  // #given a failed warm session that has released its lease and waits for a retry
  const { root, sourcePath, outputPath } = await workspace("sync-engine-stop-failed-");
  await Bun.write(join(outputPath, "reference"), "Prior publication");

  try {
    const scope = await Effect.runPromise(Scope.make());
    const session = await Effect.runPromise(
      startLiveSynchronization({
        sourcePath,
        outputPath,
        recovery: { existing: Effect.succeed(true) },
        declare: () => Effect.succeed({ work: ["document"], publish: Effect.void }),
        handle: copyDocument(sourcePath, outputPath),
      }).pipe(Scope.provide(scope)),
    );
    await Effect.runPromise(session.ready);
    // #when the scope closes
    await Effect.runPromise(Scope.close(scope, Exit.void));
    const status = await Effect.runPromise(session.status);
    const release = await Effect.runPromise(acquireOutputTree(outputPath));
    await release();
    // #then the session is stopped, admission is rejected and the lease is free
    expect({ state: status.state, admission: await Effect.runPromise(session.requestPass()) }).toEqual({ state: "stopped", admission: "rejected" });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
