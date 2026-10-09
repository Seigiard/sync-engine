import { test, expect } from "bun:test";
import { mkdir, mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Deferred, Effect, Exit, Scope } from "effect";
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
      while ((yield* scheduler.status).active !== null) yield* Effect.sleep(1);
      const status = yield* scheduler.status;
      return {
        failed,
        state: status.state,
        pending: status.pending,
        errors: status.errors.length,
        held: yield* io(() => Bun.file(join(outputPath, "held")).exists()),
        cascade: yield* io(() => Bun.file(join(outputPath, "cascade")).exists()),
      };
    })));
    // #then the active worker's returned cascade is discarded after the defect
    expect(observation).toEqual({ failed: "failed", state: "failed", pending: 0, errors: 0, held: true, cascade: false });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("submit after a scheduler defect fails instead of succeeding as a no-op", async () => {
  // #given a scheduler that has already failed with a handler defect
  const result = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
    const scheduler = yield* createWorkScheduler<string, never, never>({
      handle: () => Effect.die("fatal defect"),
    });
    yield* scheduler.submit(["first"]);
    const completion = yield* Effect.exit(scheduler.awaitCompletion);
    // #when later work is submitted through the same public handle
    const submitted = yield* Effect.exit(scheduler.submit(["second"]));
    const status = yield* scheduler.status;
    return { completionFailed: Exit.isFailure(completion), submitFailed: Exit.isFailure(submitted), state: status.state, pending: status.pending };
  })));
  // #then admission still fails with the scheduler defect and no work is queued
  expect(result).toEqual({ completionFailed: true, submitFailed: true, state: "failed", pending: 0 });
});

test("submit on a closed standalone scheduler interrupts", async () => {
  // #given a scheduler handle whose owning scope has closed
  const scope = await Effect.runPromise(Scope.make());
  const scheduler = await Effect.runPromise(createWorkScheduler<string, never, never>({ handle: () => Effect.succeed([]) }).pipe(Scope.provide(scope)));
  await Effect.runPromise(Scope.close(scope, Exit.void));
  // #when work is submitted after close
  const submitted = await Effect.runPromise(Effect.exit(scheduler.submit(["late"])));
  // #then admission is interrupted instead of succeeding
  expect(Exit.isFailure(submitted) && submitted.cause.reasons.some((reason) => reason._tag === "Interrupt")).toBe(true);
});

test("non-finite concurrency falls back to one worker", async () => {
  // #given a scheduler configured from a non-numeric external value
  const result = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
    const firstEntered = yield* Deferred.make<void>();
    const releaseFirst = yield* Deferred.make<void>();
    const secondEntered = yield* Deferred.make<void>();
    const started: string[] = [];
    const scheduler = yield* createWorkScheduler({
      concurrency: Number.NaN,
      handle: (work: string) => Effect.gen(function* () {
        started.push(work);
        if (work === "first") {
          yield* Deferred.succeed(firstEntered, undefined);
          yield* Deferred.await(releaseFirst);
        } else {
          yield* Deferred.succeed(secondEntered, undefined);
        }
        return [] as string[];
      }),
    });
    // #when work is submitted through the public scheduler
    yield* scheduler.submit(["first", "second"]);
    yield* Deferred.await(firstEntered);
    const secondBeforeRelease = yield* Effect.race(Deferred.await(secondEntered).pipe(Effect.as("started")), Effect.sleep(25).pipe(Effect.as("blocked")));
    yield* Deferred.succeed(releaseFirst, undefined);
    yield* scheduler.awaitCompletion;
    return { secondBeforeRelease, started, state: (yield* scheduler.status).state };
  })));
  // #then it is processed by the default single worker instead of hanging
  expect(result).toEqual({ secondBeforeRelease: "blocked", started: ["first", "second"], state: "complete" });
});

test("undefined is a valid work item rather than an empty-queue sentinel", async () => {
  // #given a payload-free public work scheduler
  const result = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
    let handled = 0;
    const scheduler = yield* createWorkScheduler<void, never, never>({
      handle: (work) => Effect.sync(() => {
        if (work === undefined) handled += 1;
        return [] as void[];
      }),
    });
    // #when undefined is submitted as the work value
    yield* scheduler.submit([undefined]);
    yield* scheduler.awaitCompletion;
    return { handled, state: (yield* scheduler.status).state };
  })));
  // #then the handler sees the item exactly once
  expect(result).toEqual({ handled: 1, state: "complete" });
});

test("status reports an active undefined work item", async () => {
  // #given a held payload-free item
  const result = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
    const entered = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    const scheduler = yield* createWorkScheduler<void, never, never>({
      handle: () => Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release)), Effect.as([] as void[])),
    });
    // #when undefined is active
    yield* scheduler.submit([undefined]);
    yield* Deferred.await(entered);
    const active = (yield* scheduler.status).active;
    yield* Deferred.succeed(release, undefined);
    yield* scheduler.awaitCompletion;
    return { hasActiveProperty: Object.hasOwn({ active }, "active"), active };
  })));
  // #then status preserves the payload value instead of replacing it with null
  expect(result).toEqual({ hasActiveProperty: true, active: undefined });
});

test("key callback defects fail the scheduler instead of hanging completion", async () => {
  // #given an application key callback that throws
  const result = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
    const scheduler = yield* createWorkScheduler<string, never, never>({
      key: () => { throw new Error("bad key"); },
      handle: () => Effect.succeed([]),
    });
    // #when work is submitted
    const submitted = yield* Effect.exit(scheduler.submit(["work"]));
    const exit = yield* Effect.exit(scheduler.awaitCompletion);
    const status = yield* scheduler.status;
    return { submitFailed: Exit.isFailure(submitted), failed: Exit.isFailure(exit), state: status.state, pending: status.pending };
  })));
  // #then completion fails and pending work is cleared
  expect(result).toEqual({ submitFailed: true, failed: true, state: "failed", pending: 0 });
});

test("failureKey callback defects fail the scheduler instead of hanging completion", async () => {
  // #given a failure-key callback that throws while clearing a typed failure
  const result = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
    const scheduler = yield* createWorkScheduler<string, string, never>({
      failureKey: () => { throw new Error("bad failure key"); },
      handle: () => Effect.fail("typed failure"),
    });
    // #when work reaches the typed-failure path
    yield* scheduler.submit(["work"]);
    const exit = yield* Effect.exit(scheduler.awaitCompletion);
    const status = yield* scheduler.status;
    return { failed: Exit.isFailure(exit), state: status.state, pending: status.pending, errors: status.errors.length };
  })));
  // #then completion fails as a scheduler defect rather than hanging
  expect(result).toEqual({ failed: true, state: "failed", pending: 0, errors: 0 });
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

test("submitting keyed work evaluates each item's key once", async () => {
  // #given a scheduler whose single worker is held, so submitted work stays pending
  const items = Array.from({ length: 2000 }, (_, index) => `item-${index}`);
  let keyCalls = 0;
  const release = Promise.withResolvers<void>();
  const result = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
    const scheduler = yield* createWorkScheduler({
      key: (work: string) => { keyCalls += 1; return work; },
      handle: () => Effect.promise(() => release.promise).pipe(Effect.as([] as string[])),
    });
    // #when a full plan of distinct keyed items is submitted at once
    yield* scheduler.submit(items);
    const calls = keyCalls;
    release.resolve();
    return { calls, pending: (yield* scheduler.status).pending };
  })));
  // #then the cost is one key evaluation per submitted item, not one per pending pair
  expect(result).toEqual({ calls: 2000, pending: 2000 });
});

test("a keyed cascade enqueued behind a very large pending queue keeps the scheduler working", async () => {
  // #given a queue larger than any engine's call-argument limit (Bun 1.4.2 fails at about 700k, Node at 100k–150k)
  const size = 2_000_000;
  const items = Array.from({ length: size }, (_, index) => `item-${index}`);
  const firstHeld = Promise.withResolvers<void>();
  const secondStarted = Promise.withResolvers<void>();
  const result = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
    const scheduler = yield* createWorkScheduler({
      key: (work: string) => work,
      handle: (work: string) => work === "item-0"
        ? Effect.promise(() => firstHeld.promise).pipe(Effect.as(["cascade"]))
        : Effect.sync(() => secondStarted.resolve()).pipe(Effect.andThen(Effect.never)),
    });
    yield* scheduler.submit(items);
    // #when the first handler returns a keyed follow-up while the rest of the plan is still pending
    firstHeld.resolve();
    const next = yield* Effect.race(Effect.promise(() => secondStarted.promise).pipe(Effect.as("next item started" as const)), Effect.sleep(5000).pipe(Effect.as("no item started" as const)));
    const status = yield* scheduler.status;
    return { next, state: status.state, pending: status.pending };
  })));
  // #then the follow-up joins the queue and the next item runs; the scheduler does not fail
  expect(result).toEqual({ next: "next item started", state: "working", pending: size - 1 });
}, 30_000);
