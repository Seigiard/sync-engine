import { expect, test } from "bun:test";
import { mkdir, mkdtemp, readFile, rename, rm, utimes } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Cause, Effect, Exit, Fiber, Scope } from "effect";
import { startLiveSynchronizationWithHooks, type LiveHandle } from "../src/live.ts";

/*
 * Interpreter fault injection. Each cell makes one activity (the opening, a follow-up pass, or the close of a
 * failed attempt) exit in one way. Whatever the exit, `ready` and completion must settle, the session must not
 * stay busy, and the hint admitted with the work must be published exactly once. Every cell also proves that its
 * fault fired (`unfired`), and records what tells a faulty run from a fault-free one: how many openings ran and
 * whether the dying finalizer's cause reached the failed completion.
 */

type Activity = "opening" | "pass" | "close";
type ExitKind = "ok" | "typed failure" | "defect" | "interruption" | "failing finalizer" | "failing trailing traversal";
type Fault = "handler fails" | "handler dies" | "handler interrupts" | "publish fails" | "finalizer dies" | "trailing scan fails" | "close is held";

const io = <A>(run: () => Promise<A>) => Effect.tryPromise({ try: run, catch: (cause) => new Error(String(cause)) }).pipe(Effect.uninterruptible);

const closeOf = (scope: Scope.Closeable) => Effect.race(
  Effect.exit(Scope.close(scope, Exit.void)).pipe(Effect.map((exit) => Exit.isSuccess(exit) ? "closed" as const : "close failed" as const)),
  Effect.sleep(2000).pipe(Effect.as("hung" as const)),
);

const settle = <A, E>(effect: Effect.Effect<A, E>) => Effect.race(
  Effect.exit(effect).pipe(Effect.map((exit) => Exit.isSuccess(exit) ? "succeeded" as const : "failed" as const)),
  Effect.sleep(2000).pipe(Effect.as("hung" as const)),
);

// Settles like `settle`, and also reports whether a failure carries the dying finalizer's cause.
const settleWithCause = <A, E>(effect: Effect.Effect<A, E>) => Effect.race(
  Effect.exit(effect).pipe(Effect.map((exit) => Exit.isSuccess(exit)
    ? { settled: "succeeded" as const, finalizerCause: false }
    : { settled: "failed" as const, finalizerCause: Cause.pretty(exit.cause).includes("attempt finalizer failed") })),
  Effect.sleep(2000).pipe(Effect.as({ settled: "hung" as const, finalizerCause: false })),
);

function faultsFor(activity: Activity, kind: ExitKind): Set<Fault> {
  if (activity === "close") {
    // A close follows a failed attempt: the follow-up pass dies, then the close itself exits in `kind`.
    if (kind === "failing finalizer") return new Set(["handler dies", "finalizer dies"]);
    if (kind === "interruption") return new Set(["handler dies", "close is held"]);
    // A successful close is what `pass × defect` already runs.
    throw new Error(`no close cell for ${kind}`);
  }
  switch (kind) {
    case "ok": return new Set();
    case "typed failure": return new Set([activity === "opening" ? "handler fails" : "publish fails"]);
    case "defect": return new Set(["handler dies"]);
    case "interruption": return new Set(["handler interrupts"]);
    case "failing finalizer": return new Set(["finalizer dies", "handler dies"]);
    case "failing trailing traversal": return new Set(["trailing scan fails"]);
  }
}

async function runFaultCell(activity: Activity, kind: ExitKind) {
  const root = await mkdtemp(join(tmpdir(), "sync-engine-live-fault-"));
  const sourcePath = join(root, "source");
  const parkedPath = join(root, "parked");
  const outputPath = join(root, "output");
  const stamp = new Date("2020-01-01T00:00:00Z");
  await mkdir(sourcePath);
  await Bun.write(join(sourcePath, "note.txt"), "Original");
  await utimes(join(sourcePath, "note.txt"), stamp, stamp);
  // Each fault fires once, in the activity under test; recovery then runs without faults.
  const armed = faultsFor(activity, kind);
  const fire = (fault: Fault, inScope: boolean) => inScope && armed.delete(fault);
  const published: (readonly string[])[] = [];
  const openingHeld = Promise.withResolvers<void>();
  const closeHeld = Promise.withResolvers<void>();
  const closeHolding = Promise.withResolvers<void>();
  let initialDeclarations = 0;
  let followUpDeclarations = 0;
  const park = () => rename(sourcePath, parkedPath);
  const unpark = () => rename(parkedPath, sourcePath).catch(() => {});
  const options = {
    sourcePath,
    outputPath,
    reconcileIntervalMs: 0,
    recovery: { existing: Effect.succeed(true) },
    freshness: { describe: (path: string) => path === "note.txt" ? { sourcePaths: ["note.txt"], resultKind: "note", processingVersion: "v1", outputPaths: ["note.txt"] } : undefined },
    declare: (_entries: unknown, request: { readonly kind: string; readonly changedPaths: readonly string[] }) => Effect.gen(function* () {
      const initial = request.kind === "initial";
      if (initial) initialDeclarations += 1;
      else followUpDeclarations += 1;
      // The opening under test waits until its hint was admitted, so the hint arrives during that opening.
      if (initial && initialDeclarations === 1 && activity === "opening") yield* Effect.promise(() => openingHeld.promise);
      const finalizerHere = activity === "pass" ? !initial : initial;
      if (fire("finalizer dies", finalizerHere)) yield* Effect.addFinalizer(() => Effect.die("attempt finalizer failed"));
      return {
        work: ["note.txt"],
        publish: Effect.gen(function* () {
          if (fire("publish fails", !initial)) return yield* Effect.fail(new Error("publication failed"));
          if (!initial) published.push(request.changedPaths);
          if (fire("trailing scan fails", !initial)) yield* io(park);
        }),
      };
    }),
    handle: (path: string) => Effect.suspend(() => {
      // The opening under test is the first one; the pass under test is the first follow-up.
      const inScope = activity === "opening" ? initialDeclarations === 1 && followUpDeclarations === 0 : followUpDeclarations === 1;
      if (fire("handler fails", inScope)) return Effect.fail(new Error("handler failed"));
      if (fire("handler dies", inScope)) return Effect.die("handler defect");
      if (fire("handler interrupts", inScope)) return Effect.interrupt;
      return io(async () => { await Bun.write(join(outputPath, path), await readFile(join(sourcePath, path), "utf8")); return [] as string[]; });
    }),
  };
  try {
    const scope = await Effect.runPromise(Scope.make());
    const session = await Effect.runPromise(startLiveSynchronizationWithHooks(options, {
      afterOpeningPublication: Effect.suspend(() => fire("trailing scan fails", activity === "opening" && initialDeclarations === 1) ? io(park).pipe(Effect.orDie) : Effect.void),
      beforeAttemptClose: () => Effect.suspend(() => fire("close is held", true)
        ? Effect.sync(() => closeHolding.resolve()).pipe(Effect.andThen(Effect.promise(() => closeHeld.promise)))
        : io(unpark).pipe(Effect.orDie)),
    }).pipe(Scope.provide(scope))) as LiveHandle<string, Error>;
    // #when the activity under test exits as `kind`
    const result = await Effect.runPromise(Effect.gen(function* () {
      if (activity === "opening") {
        yield* session.notify(["note.txt"]);
        openingHeld.resolve();
      } else {
        yield* session.ready;
        yield* session.notify(["note.txt"]);
      }
      const hintPublished = () => published.filter((paths) => paths.includes("note.txt")).length;
      if (activity === "close" && kind === "interruption") {
        // Stop the session once the failed attempt's close is held.
        yield* Effect.promise(() => closeHolding.promise);
        const closing = yield* Effect.forkDetach(closeOf(scope));
        const completion = yield* settleWithCause(session.awaitCompletion);
        const closed = yield* Fiber.join(closing);
        closeHeld.resolve();
        return { ready: yield* settle(session.ready), completion: completion.settled, finalizerCause: completion.finalizerCause, state: (yield* session.status).state, recovered: yield* session.requestPass(), hintPublished: hintPublished(), openings: initialDeclarations, closed, unfired: [...armed] as readonly Fault[] };
      }
      const ready = yield* settle(session.ready);
      const completion = yield* settleWithCause(session.awaitCompletion);
      yield* io(unpark);
      const state = (yield* session.status).state;
      yield* session.requestPass();
      const recovered = yield* settle(session.awaitCompletion);
      const closed = yield* closeOf(scope);
      return { ready, completion: completion.settled, finalizerCause: completion.finalizerCause, state, recovered, hintPublished: hintPublished(), openings: initialDeclarations, closed, unfired: [...armed] as readonly Fault[] };
    }));
    return result;
  } finally {
    openingHeld.resolve();
    closeHolding.resolve();
    closeHeld.resolve();
    await rm(root, { recursive: true, force: true });
  }
}

// `openings` counts initial declarations: a failure that ends an attempt makes the next request reopen.
const recovers = { ready: "succeeded", completion: "succeeded", finalizerCause: false, state: "complete", recovered: "succeeded", hintPublished: 1, openings: 1, closed: "closed", unfired: [] } as const;
// A failed follow-up pass leaves the hint retained; the recovery request publishes it once.
const passFails = { ready: "succeeded", completion: "failed", finalizerCause: false, state: "failed", recovered: "succeeded", hintPublished: 1, openings: 1, closed: "closed", unfired: [] } as const;

test.each([
  // The hint arrives during the opening, so a failed opening is retried at once and completion still succeeds.
  ["opening", "ok", recovers],
  ["opening", "typed failure", { ...recovers, openings: 2 }],
  ["opening", "defect", { ...recovers, openings: 2 }],
  ["opening", "interruption", { ...recovers, openings: 2 }],
  ["opening", "failing finalizer", { ...recovers, openings: 2 }],
  ["opening", "failing trailing traversal", { ...recovers, openings: 2 }],
  ["pass", "ok", recovers],
  // A typed failure keeps the attempt; a defect, an interruption or a dying finalizer ends it.
  ["pass", "typed failure", passFails],
  ["pass", "defect", { ...passFails, openings: 2 }],
  ["pass", "interruption", { ...passFails, openings: 2 }],
  ["pass", "failing finalizer", { ...passFails, finalizerCause: true, openings: 2 }],
  // Published and committed before the traversal failed: the failure is visible, the hint is not run again.
  ["pass", "failing trailing traversal", passFails],
  ["close", "failing finalizer", { ...passFails, finalizerCause: true, openings: 2 }],
  ["close", "interruption", { ready: "succeeded", completion: "failed", finalizerCause: false, state: "stopped", recovered: "rejected", hintPublished: 0, openings: 1, closed: "closed", unfired: [] }],
] as const)("an interpreter activity reports one outcome whatever its exit: %s × %s", async (activity, kind, expected) => {
  // #given a recoverable session with prior output and one armed fault
  const result = await runFaultCell(activity, kind);
  // #then ready and completion settle, the session is not left busy, and the hint is published exactly once
  expect(result).toEqual(expected);
}, 20_000);
