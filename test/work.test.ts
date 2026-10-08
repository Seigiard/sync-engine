import { test, expect } from "bun:test";
import { mkdir, mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Deferred, Effect, Exit } from "effect";
import { createWorkScheduler, openSynchronization, runInitialPass } from "../src/index.ts";

const io = <A>(run: () => Promise<A>) => Effect.tryPromise({ try: run, catch: (cause) => new Error(String(cause)) }).pipe(Effect.uninterruptible);

test("pending equivalent publication moves behind the work that invalidated it", async () => {
  // #given a required result which does not exist yet
  const root = await mkdtemp(join(tmpdir(), "sync-engine-work-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  try {
    // #when a publication is requested again after its required preparation
    const options = {
      sourcePath, outputPath,
      key: (work: string) => work === "publish" ? "publication" : undefined,
      declare: () => Effect.succeed({ work: ["publish", "prepare", "publish"], publish: Effect.void }),
      handle: (work: string) => io(async () => {
        if (work === "prepare") await Bun.write(join(outputPath, "required"), "Prepared target");
        else await Bun.write(join(outputPath, "reference"), await readFile(join(outputPath, "required"), "utf8"));
        return [];
      }),
    };
    await Effect.runPromise(runInitialPass(options));
    // #then the publication reads a completed required result
    expect(await readFile(join(outputPath, "reference"), "utf8")).toBe("Prepared target");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("required downstream publication keeps public completion working", async () => {
  // #given real output and a held downstream write
  const root = await mkdtemp(join(tmpdir(), "sync-engine-downstream-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  try {
    // #when the initial session submits preparation with required dependent work
    const observation = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const scheduler = yield* openSynchronization({
        sourcePath, outputPath,
        declare: () => Effect.succeed({ work: [], publish: Effect.void }),
        handle: (work: string) => work === "prepare"
          ? io(() => Bun.write(join(outputPath, "required"), "Prepared target")).pipe(Effect.as(["publish"]))
          : Effect.gen(function* () {
              yield* Deferred.succeed(entered, undefined);
              yield* Deferred.await(release);
              yield* io(async () => Bun.write(join(outputPath, "reference"), await readFile(join(outputPath, "required"), "utf8")));
              return [];
            }),
      });
      yield* scheduler.submit(["prepare"]);
      yield* Deferred.await(entered);
      const before = (yield* scheduler.status).state;
      const premature = yield* io(() => Bun.file(join(outputPath, "reference")).exists());
      yield* Deferred.succeed(release, undefined);
      yield* scheduler.awaitCompletion;
      return { before, premature, after: (yield* scheduler.status).state, reference: yield* io(() => readFile(join(outputPath, "reference"), "utf8")) };
    })));
    // #then completion includes the downstream artifact
    expect(observation).toEqual({ before: "working", premature: false, after: "complete", reference: "Prepared target" });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("independent work can run concurrently without reporting completion early", async () => {
  // #given two independent jobs guarded by separate barriers
  const root = await mkdtemp(join(tmpdir(), "sync-engine-concurrent-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  try {
    // #when a scheduler with two permits receives both jobs
    const observation = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const firstEntered = yield* Deferred.make<void>();
      const secondEntered = yield* Deferred.make<void>();
      const releaseFirst = yield* Deferred.make<void>();
      const releaseSecond = yield* Deferred.make<void>();
      const scheduler = yield* openSynchronization({
        sourcePath, outputPath,
        concurrency: 2,
        declare: () => Effect.succeed({ work: [], publish: Effect.void }),
        handle: (work: string) => Effect.gen(function* () {
          if (work === "first") yield* Deferred.succeed(firstEntered, undefined);
          else yield* Deferred.succeed(secondEntered, undefined);
          yield* Deferred.await(work === "first" ? releaseFirst : releaseSecond);
          yield* io(() => Bun.write(join(outputPath, work), work));
          return [];
        }),
      });
      yield* scheduler.submit(["first", "second"]);
      yield* Deferred.await(firstEntered);
      yield* Deferred.await(secondEntered);
      const before = yield* scheduler.status;
      yield* Deferred.succeed(releaseFirst, undefined);
      const afterFirst = yield* Effect.race(scheduler.awaitCompletion.pipe(Effect.as("completed")), Effect.sleep(25).pipe(Effect.as("pending")));
      yield* Deferred.succeed(releaseSecond, undefined);
      yield* scheduler.awaitCompletion;
      return {
        before: before.state,
        afterFirst,
        first: yield* io(() => readFile(join(outputPath, "first"), "utf8")),
        second: yield* io(() => readFile(join(outputPath, "second"), "utf8")),
        after: (yield* scheduler.status).state,
      };
    })));
    // #then both entered while completion was still pending
    expect(observation).toEqual({ before: "working", afterFirst: "pending", first: "first", second: "second", after: "complete" });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("duplicate unkeyed work can run concurrently without completing after the first copy", async () => {
  // #given two equal unkeyed jobs guarded by separate barriers
  const root = await mkdtemp(join(tmpdir(), "sync-engine-duplicate-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  try {
    // #when both equal jobs run at the same time and only the first is released
    const observation = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const firstEntered = yield* Deferred.make<void>();
      const secondEntered = yield* Deferred.make<void>();
      const releaseFirst = yield* Deferred.make<void>();
      const releaseSecond = yield* Deferred.make<void>();
      let seen = 0;
      const scheduler = yield* openSynchronization({
        sourcePath, outputPath, concurrency: 2,
        declare: () => Effect.succeed({ work: [], publish: Effect.void }),
        handle: (work: string) => Effect.gen(function* () {
          seen += 1;
          const index = seen;
          yield* Deferred.succeed(index === 1 ? firstEntered : secondEntered, undefined);
          yield* Deferred.await(index === 1 ? releaseFirst : releaseSecond);
          yield* io(() => Bun.write(join(outputPath, `${work}-${index}`), `${work}-${index}`));
          return [];
        }),
      });
      yield* scheduler.submit(["x", "x"]);
      yield* Deferred.await(firstEntered);
      yield* Deferred.await(secondEntered);
      yield* Deferred.succeed(releaseFirst, undefined);
      const afterFirst = yield* Effect.race(scheduler.awaitCompletion.pipe(Effect.as("completed")), Effect.sleep(25).pipe(Effect.as("pending")));
      yield* Deferred.succeed(releaseSecond, undefined);
      yield* scheduler.awaitCompletion;
      return {
        afterFirst,
        first: yield* io(() => readFile(join(outputPath, "x-1"), "utf8")),
        second: yield* io(() => readFile(join(outputPath, "x-2"), "utf8")),
        state: (yield* scheduler.status).state,
      };
    })));
    // #then public completion waits for the still-held duplicate
    expect(observation).toEqual({ afterFirst: "pending", first: "x-1", second: "x-2", state: "complete" });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("a defect stops other workers from enqueueing cascades after failure", async () => {
  // #given one worker that will defect and another held worker that would return a cascade
  const root = await mkdtemp(join(tmpdir(), "sync-engine-defect-stop-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  try {
    // #when the defect fails the scheduler while the other worker is still active
    const observation = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const heldEntered = yield* Deferred.make<void>();
      const releaseHeld = yield* Deferred.make<void>();
      const scheduler = yield* openSynchronization({
        sourcePath, outputPath, concurrency: 2,
        declare: () => Effect.succeed({ work: [], publish: Effect.void }),
        handle: (work: string) => work === "fatal"
          ? Deferred.await(heldEntered).pipe(Effect.andThen(Effect.die("fatal defect")))
          : work === "held"
            ? Effect.gen(function* () {
                yield* Deferred.succeed(heldEntered, undefined);
                yield* Deferred.await(releaseHeld);
                yield* io(() => Bun.write(join(outputPath, "held"), "held"));
                return ["cascade"];
              })
            : io(() => Bun.write(join(outputPath, "cascade"), "cascade")).pipe(Effect.as([])),
      });
      yield* scheduler.submit(["fatal", "held"]);
      const failedExit = yield* Effect.exit(scheduler.awaitCompletion);
      const failed = Exit.isFailure(failedExit) ? "failed" : "completed";
      yield* Deferred.succeed(releaseHeld, undefined);
      yield* Effect.sleep(25);
      const status = yield* scheduler.status;
      return {
        failed,
        state: status.state,
        held: yield* io(() => Bun.file(join(outputPath, "held")).exists()),
        cascade: yield* io(() => Bun.file(join(outputPath, "cascade")).exists()),
      };
    })));
    // #then the active worker's returned cascade is discarded after the defect
    expect(observation).toEqual({ failed: "failed", state: "failed", held: true, cascade: false });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("non-finite concurrency falls back to one worker", async () => {
  // #given a scheduler configured from a non-numeric external value
  const result = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
    const handled: string[] = [];
    const scheduler = yield* createWorkScheduler({
      concurrency: Number.NaN,
      handle: (work: string) => Effect.sync(() => {
        handled.push(work);
        return [] as string[];
      }),
    });
    // #when work is submitted through the public scheduler
    yield* scheduler.submit(["refresh"]);
    const completion = yield* Effect.race(scheduler.awaitCompletion.pipe(Effect.as("completed")), Effect.sleep(25).pipe(Effect.as("pending")));
    return { completion, handled, state: (yield* scheduler.status).state };
  })));
  // #then it is processed by the default single worker instead of hanging
  expect(result).toEqual({ completion: "completed", handled: ["refresh"], state: "complete" });
});

test("pending work with an active equivalent key waits while different-key work starts", async () => {
  // #given a held active publication, an equivalent follow-up and an independent publication
  const root = await mkdtemp(join(tmpdir(), "sync-engine-keyed-concurrent-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  try {
    // #when the scheduler has more than one permit
    const observation = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const firstEntered = yield* Deferred.make<void>();
      const sameEntered = yield* Deferred.make<void>();
      const otherEntered = yield* Deferred.make<void>();
      const releaseFirst = yield* Deferred.make<void>();
      const releaseSame = yield* Deferred.make<void>();
      const started: string[] = [];
      const scheduler = yield* openSynchronization({
        sourcePath,
        outputPath,
        concurrency: 2,
        key: (work: string) => work.startsWith("same:") ? "same" : work,
        declare: () => Effect.succeed({ work: [], publish: Effect.void }),
        handle: (work: string) => Effect.gen(function* () {
          started.push(work);
          if (work === "same:first") {
            yield* Deferred.succeed(firstEntered, undefined);
            yield* Deferred.await(releaseFirst);
          } else if (work === "same:second") {
            yield* Deferred.succeed(sameEntered, undefined);
            yield* Deferred.await(releaseSame);
          } else {
            yield* Deferred.succeed(otherEntered, undefined);
          }
          yield* io(() => Bun.write(join(outputPath, work.replace(":", "-")), started.join(",")));
          return [];
        }),
      });
      yield* scheduler.submit(["same:first"]);
      yield* Deferred.await(firstEntered);
      yield* scheduler.submit(["same:second", "other"]);
      yield* Deferred.await(otherEntered);
      const beforeRelease = [...started];
      yield* Deferred.succeed(releaseFirst, undefined);
      yield* Deferred.await(sameEntered);
      const afterRelease = [...started];
      yield* Deferred.succeed(releaseSame, undefined);
      yield* scheduler.awaitCompletion;
      return { beforeRelease, afterRelease, final: yield* io(() => readFile(join(outputPath, "same-second"), "utf8")) };
    })));

    // #then the equivalent follow-up starts only after the active same-key work finishes
    expect(observation).toEqual({
      beforeRelease: ["same:first", "other"],
      afterRelease: ["same:first", "other", "same:second"],
      final: "same:first,other,same:second",
    });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("an equivalent refresh requested while active publishes the later source", async () => {
  // #given a real mutable source and a held refresh that already read its earlier bytes
  const root = await mkdtemp(join(tmpdir(), "sync-engine-active-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  await Bun.write(join(sourcePath, "document"), "Earlier source");
  try {
    // #when another refresh arrives after a later source write
    const result = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      let first = true;
      const scheduler = yield* openSynchronization({
        sourcePath, outputPath,
        key: () => "refresh",
        declare: () => Effect.succeed({ work: [], publish: Effect.void }),
        handle: (_work: string) => Effect.gen(function* () {
          const bytes = yield* io(() => readFile(join(sourcePath, "document"), "utf8"));
          if (first) {
            first = false;
            yield* Deferred.succeed(entered, undefined);
            yield* Deferred.await(release);
          }
          yield* io(() => Bun.write(join(outputPath, "published"), bytes));
          return [];
        }),
      });
      yield* scheduler.submit(["refresh"]);
      yield* Deferred.await(entered);
      yield* io(() => Bun.write(join(sourcePath, "document"), "Later source"));
      yield* scheduler.submit(["refresh", "refresh"]);
      yield* Deferred.succeed(release, undefined);
      yield* scheduler.awaitCompletion;
      return yield* io(() => readFile(join(outputPath, "published"), "utf8"));
    })));
    // #then a pending follow-up repairs the first refresh's stale publication
    expect(result).toBe("Later source");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("a failed source retains its result while independent work finishes with an observable error", async () => {
  // #given previously published bytes and an unreadable replacement source
  const root = await mkdtemp(join(tmpdir(), "sync-engine-errors-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  await mkdir(outputPath);
  await Bun.write(join(outputPath, "broken"), "Earlier source");
  await Bun.write(join(sourcePath, "healthy"), "Independent source");
  try {
    // #when the public scheduler receives failed work followed by independent work
    const observation = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const session = yield* openSynchronization({
        sourcePath, outputPath,
        declare: () => Effect.succeed({ work: [], publish: Effect.void }),
        handle: (work: string) => io(async () => {
          const bytes = await readFile(join(sourcePath, work), "utf8");
          await Bun.write(join(outputPath, work), bytes);
          return [];
        }),
      });
      yield* session.submit(["broken", "healthy"]);
      yield* session.awaitCompletion;
      const status = yield* session.status;
      return {
        state: status.state,
        pending: status.pending,
        active: status.active,
        failed: status.errors.map((error) => error.work),
        retained: yield* io(() => readFile(join(outputPath, "broken"), "utf8")),
        independent: yield* io(() => readFile(join(outputPath, "healthy"), "utf8")),
      };
    })));
    // #then drained work remains distinguishable from successful processing
    expect(observation).toEqual({ state: "complete-with-errors", pending: 0, active: null, failed: ["broken"], retained: "Earlier source", independent: "Independent source" });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
