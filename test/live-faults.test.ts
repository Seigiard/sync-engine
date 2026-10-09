import { expect, test } from "bun:test";
import { mkdir, mkdtemp, readFile, rename, rm, utimes } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Effect, Exit, Fiber, Scope } from "effect";
import { startLiveSynchronizationWithHooks, type LiveHandle } from "../src/live.ts";

/*
 * Interpreter fault injection. Each cell makes one activity (the opening, a follow-up pass, or the close of a
 * failed attempt) exit in one way. Whatever the exit, `ready` and completion must settle, the session must not
 * stay busy, and the hint admitted with the work must be published exactly once.
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

function faultsFor(activity: Activity, kind: ExitKind): Set<Fault> {
  if (activity === "close") {
    // A close follows a failed attempt: the follow-up pass dies, then the close itself exits in `kind`.
    if (kind === "failing finalizer") return new Set(["handler dies", "finalizer dies"]);
    if (kind === "interruption") return new Set(["handler dies", "close is held"]);
    return new Set(["handler dies"]);
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
      beforeAttemptClose: () => Effect.suspend(() => fire("close is held", true) ? Effect.promise(() => closeHeld.promise) : io(unpark).pipe(Effect.orDie)),
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
      if (armed.has("close is held")) {
        // Stop the session while the failed attempt's close is held.
        while (armed.has("handler dies")) yield* Effect.sleep(5);
        yield* Effect.sleep(25);
        const closing = yield* Effect.forkDetach(closeOf(scope));
        const completion = yield* settle(session.awaitCompletion);
        const closed = yield* Fiber.join(closing);
        closeHeld.resolve();
        return { ready: yield* settle(session.ready), completion, state: (yield* session.status).state, recovered: yield* session.requestPass(), hintPublished: published.filter((paths) => paths.includes("note.txt")).length, closed };
      }
      const ready = yield* settle(session.ready);
      const completion = yield* settle(session.awaitCompletion);
      yield* io(unpark);
      const state = (yield* session.status).state;
      yield* session.requestPass();
      const recovered = yield* settle(session.awaitCompletion);
      const closed = yield* closeOf(scope);
      return { ready, completion, state, recovered, hintPublished: published.filter((paths) => paths.includes("note.txt")).length, closed };
    }));
    return result;
  } finally {
    openingHeld.resolve();
    closeHeld.resolve();
    await rm(root, { recursive: true, force: true });
  }
}

const recovers = { ready: "succeeded", state: "complete", recovered: "succeeded", hintPublished: 1, closed: "closed" } as const;
// A failed follow-up pass leaves the hint retained; the recovery request publishes it once.
const passFails = { ready: "succeeded", completion: "failed", state: "failed", recovered: "succeeded", hintPublished: 1, closed: "closed" } as const;

test.each([
  // The hint arrives during the opening, so a failed opening is retried at once and completion still succeeds.
  ["opening", "ok", { ...recovers, completion: "succeeded" }],
  ["opening", "typed failure", { ...recovers, completion: "succeeded" }],
  ["opening", "defect", { ...recovers, completion: "succeeded" }],
  ["opening", "interruption", { ...recovers, completion: "succeeded" }],
  ["opening", "failing finalizer", { ...recovers, completion: "succeeded" }],
  ["opening", "failing trailing traversal", { ...recovers, completion: "succeeded" }],
  ["pass", "ok", { ...recovers, completion: "succeeded" }],
  ["pass", "typed failure", passFails],
  ["pass", "defect", passFails],
  ["pass", "interruption", passFails],
  ["pass", "failing finalizer", passFails],
  // Published and committed before the traversal failed: the failure is visible, the hint is not run again.
  ["pass", "failing trailing traversal", passFails],
  ["close", "ok", passFails],
  ["close", "failing finalizer", passFails],
  ["close", "interruption", { ready: "succeeded", completion: "failed", state: "stopped", recovered: "rejected", hintPublished: 0, closed: "closed" }],
] as const)("an interpreter activity reports one outcome whatever its exit: %s × %s", async (activity, kind, expected) => {
  // #given a recoverable session with prior output and one armed fault
  const result = await runFaultCell(activity, kind);
  // #then ready and completion settle, the session is not left busy, and the hint is published exactly once
  expect(result).toEqual(expected);
}, 20_000);
