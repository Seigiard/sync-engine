import { Deferred, Effect, Exit, Queue, type Cause, type Scope } from "effect";
import { openSynchronization, scanSource, type InitialPass, type InitialPlan, type SourceEntry, type ScanFailed, type OutputOwnershipFailed, type FreshnessFailed } from "./index.ts";
import type { WorkStatus } from "./work.ts";

export interface PassRequest {
  readonly kind: "initial" | "resync" | "watcher" | "reconcile";
  readonly force: boolean;
  /** Relative source paths; hints bypass source metadata equality. */
  readonly changedPaths: readonly string[];
}

export type PassAdmission = "started" | "queued" | "rejected";

export interface LiveOptions<W, E, R> extends Omit<InitialPass<W, E, R>, "declare"> {
  readonly declare: (entries: readonly SourceEntry[], request: PassRequest) => Effect.Effect<InitialPlan<W, E, R>, E, R>;
  /** Zero disables the owned periodic timer. */
  readonly reconcileIntervalMs?: number;
}

export interface LiveStatus<W> {
  readonly state: WorkStatus<W>["state"];
  readonly pass: PassRequest | null;
  readonly followUp: PassRequest | null;
  /** Cause of the last failed pass; a later successful pass clears it. */
  readonly failure: Cause.Cause<unknown> | null;
  readonly work: WorkStatus<W>;
}

export interface LiveSynchronization<W, E> {
  readonly requestPass: (request?: { readonly force?: boolean }) => Effect.Effect<PassAdmission>;
  readonly notify: (relativePaths: readonly string[]) => Effect.Effect<PassAdmission>;
  readonly awaitCompletion: Effect.Effect<void, E | ScanFailed>;
  readonly status: Effect.Effect<LiveStatus<W>>;
}

function differences(before: readonly SourceEntry[], after: readonly SourceEntry[]): string[] {
  const previous = new Map(before.map((entry) => [entry.path, entry]));
  const changed = new Set<string>();
  for (const entry of after) {
    const old = previous.get(entry.path);
    if (!old || old.kind !== entry.kind || old.size !== entry.size || old.mtimeMs !== entry.mtimeMs) changed.add(entry.path);
    previous.delete(entry.path);
  }
  for (const path of previous.keys()) changed.add(path);
  return [...changed];
}

/** Owns scan, processing, required publication and follow-up as one pass. */
export function openLiveSynchronization<W, E, R>(options: LiveOptions<W, E, R>): Effect.Effect<LiveSynchronization<W, E | FreshnessFailed>, E | FreshnessFailed | ScanFailed | OutputOwnershipFailed, R | Scope.Scope> {
  return Effect.gen(function* () {
    let baseline: readonly SourceEntry[] = [];
    const scheduler = yield* openSynchronization({
      ...options,
      declare: (entries) => {
        baseline = entries;
        return options.declare(entries, { kind: "initial", force: false, changedPaths: [] });
      },
    });
    const wake = yield* Queue.unbounded<void>();
    let state: LiveStatus<W>["state"] = "complete";
    let pending: PassRequest | null = null;
    let active: PassRequest | null = null;
    let failure: Cause.Cause<E | FreshnessFailed | ScanFailed> | null = null;
    let completion = Deferred.makeUnsafe<void, E | FreshnessFailed | ScanFailed>();
    Deferred.doneUnsafe(completion, Effect.void);

    const request = (next: PassRequest): PassAdmission => {
      if (state === "stopped") return "rejected";
      const busy = active !== null || pending !== null;
      if (state !== "working") completion = Deferred.makeUnsafe<void, E | FreshnessFailed | ScanFailed>();
      state = "working";
      pending = pending === null ? next : {
        kind: pending.kind === "resync" || next.kind === "resync" ? "resync" : next.kind,
        force: pending.force || next.force,
        changedPaths: [...new Set([...pending.changedPaths, ...next.changedPaths])],
      };
      if (!busy) Queue.offerUnsafe(wake, undefined);
      return busy ? "queued" : "started";
    };

    const runPass = (requested: PassRequest) => Effect.gen(function* () {
      const entries = yield* scanSource(options.sourcePath, options.includeSource);
      const changedPaths = [...new Set([...requested.changedPaths, ...differences(baseline, entries)])];
      active = { ...requested, changedPaths };
      const plan = yield* options.declare(entries, active);
      yield* scheduler.submit(plan.work, active);
      yield* scheduler.awaitCompletion;
      yield* plan.publish;
      baseline = entries;
      const after = yield* scanSource(options.sourcePath, options.includeSource);
      const changed = differences(entries, after);
      if (changed.length > 0) request({ kind: "watcher", force: false, changedPaths: changed });
    });

    const consume = Effect.gen(function* () {
      while (true) {
        yield* Queue.take(wake);
        while (pending !== null) {
          const next = pending;
          pending = null;
          active = next;
          const exit = yield* Effect.exit(runPass(next));
          active = null;
          if (Exit.isFailure(exit)) {
            failure = exit.cause;
          } else {
            failure = null;
          }
        }
        if (state === "working") {
          state = failure === null ? (yield* scheduler.status).state : "failed";
          Deferred.doneUnsafe(completion, failure === null ? Effect.void : Effect.failCause(failure));
        }
      }
    });
    yield* Effect.addFinalizer(() => Effect.sync(() => {
      state = "stopped";
      pending = null;
      Deferred.doneUnsafe(completion, Effect.interrupt);
    }));
    yield* Effect.forkScoped(consume.pipe(Effect.ensuring(Effect.sync(() => { active = null; }))));

    // An initial traversal is not a snapshot either.
    const afterInitial = yield* scanSource(options.sourcePath, options.includeSource);
    const initialChanges = differences(baseline, afterInitial);
    if (initialChanges.length > 0) request({ kind: "watcher", force: false, changedPaths: initialChanges });

    if ((options.reconcileIntervalMs ?? 0) > 0) {
      yield* Effect.forkScoped(Effect.gen(function* () {
        while (true) {
          yield* Effect.sleep(options.reconcileIntervalMs!);
          request({ kind: "reconcile", force: false, changedPaths: [] });
        }
      }));
    }

    return {
      requestPass: (input = {}) => Effect.sync(() => request({ kind: "resync", force: input.force ?? false, changedPaths: [] })),
      notify: (changedPaths) => Effect.sync(() => request({ kind: "watcher", force: false, changedPaths })),
      awaitCompletion: Effect.suspend(() => Deferred.await(completion)),
      status: Effect.gen(function* () {
        const work = yield* scheduler.status;
        return { state, pass: active, followUp: pending, failure, work };
      }),
    };
  });
}
