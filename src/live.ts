import { Deferred, Effect, Exit, Queue, Scope, type Cause } from "effect";
import { openSynchronization, scanSource, type InitialPass, type InitialPlan, type SourceEntry, type Synchronization, type ScanFailed, type OutputOwnershipFailed, type FreshnessFailed } from "./index.ts";
import type { WorkStatus } from "./work.ts";

export interface PassRequest {
  readonly kind: "initial" | "resync" | "watcher" | "reconcile";
  readonly force: boolean;
  /** Relative source paths; hints bypass source metadata equality. */
  readonly changedPaths: readonly string[];
}

export type PassAdmission = "started" | "queued" | "rejected";

/** `prior-output`: usable output existed before the session. `minimum-publication`: this session published the minimum. */
export type Availability = "prior-output" | "minimum-publication";

export interface LiveOptions<W, E, R> extends Omit<InitialPass<W, E, R>, "declare"> {
  readonly declare: (entries: readonly SourceEntry[], request: PassRequest) => Effect.Effect<InitialPlan<W, E, R>, E, R>;
  /** Zero disables the owned periodic timer. */
  readonly reconcileIntervalMs?: number;
  /**
   * Opt-in failure map for the first pass. Without it a failed first pass fails the open. With it, a failure is
   * tolerated while output is usable: `existing` reports earlier output that already serves, and a published
   * minimum also counts. The session then stays up, reports `LiveStatus.failure`, and reopens on the next
   * `requestPass`/`notify` or reconcile tick. Without usable output the open still fails.
   */
  readonly recovery?: { readonly existing: Effect.Effect<boolean, never, R> };
}

export interface LiveStatus<W> {
  readonly state: WorkStatus<W>["state"];
  readonly pass: PassRequest | null;
  readonly followUp: PassRequest | null;
  /** Cause of the last failed pass or open; a later successful one clears it. */
  readonly failure: Cause.Cause<unknown> | null;
  /** Null until output is usable. Only sessions that declare `recovery` or a `minimum` can report it. */
  readonly availability: Availability | null;
  readonly work: WorkStatus<W>;
}

export interface LiveSynchronization<W, E> {
  readonly requestPass: (request?: { readonly force?: boolean }) => Effect.Effect<PassAdmission>;
  readonly notify: (relativePaths: readonly string[]) => Effect.Effect<PassAdmission>;
  readonly awaitCompletion: Effect.Effect<void, E | ScanFailed>;
  readonly status: Effect.Effect<LiveStatus<W>>;
}

export interface LiveHandle<W, E> extends LiveSynchronization<W, E> {
  /**
   * Settles when the first pass did. It succeeds when the pass finished, or failed while output is usable
   * (see `LiveOptions.recovery`), and fails with the pass cause otherwise.
   */
  readonly ready: Effect.Effect<void, E | ScanFailed>;
}

const INITIAL: PassRequest = { kind: "initial", force: false, changedPaths: [] };

function differences(before: readonly SourceEntry[], after: readonly SourceEntry[]): string[] {
  const previous = new Map(before.map((entry) => [entry.path, entry]));
  const changed = new Set<string>();
  for (const entry of after) {
    const old = previous.get(entry.path);
    if (!old || old.kind !== entry.kind || (entry.kind === "file" && (old.size !== entry.size || old.mtimeMs !== entry.mtimeMs))) changed.add(entry.path);
    previous.delete(entry.path);
  }
  for (const path of previous.keys()) changed.add(path);
  return [...changed];
}

/**
 * Owns scan, processing, required publication, follow-up, the periodic timer and the retry of a failed first pass.
 * The handle is usable at once: requests made before the first pass finished combine and run after it.
 */
export function startLiveSynchronization<W, E, R>(options: LiveOptions<W, E, R>): Effect.Effect<LiveHandle<W, E | FreshnessFailed | OutputOwnershipFailed>, never, R | Scope.Scope> {
  type Failure = E | FreshnessFailed | ScanFailed | OutputOwnershipFailed;

  return Effect.gen(function* () {
    let availability: Availability | null = options.recovery !== undefined && (yield* options.recovery.existing) ? "prior-output" : null;
    let baseline: readonly SourceEntry[] = [];
    let scheduler: Synchronization<W, E | FreshnessFailed> | undefined;
    const wake = yield* Queue.unbounded<void>();
    // `opening`: the first pass of a session attempt runs. `waiting`: an attempt failed and only a request or tick retries.
    let opening = true;
    let waiting = false;
    let declaredMinimum = false;
    let state: LiveStatus<W>["state"] = "working";
    let pending: PassRequest | null = null;
    let active: PassRequest | null = null;
    let carriedChangedPaths = new Set<string>();
    let failure: Cause.Cause<Failure> | null = null;
    let terminal = false;
    let completion = Deferred.makeUnsafe<void, Failure>();
    const ready = Deferred.makeUnsafe<void, Failure>();
    const settled = Deferred.makeUnsafe<void>();

    const request = (next: PassRequest): PassAdmission => {
      if (state === "stopped") return "rejected";
      if (terminal) return "rejected";
      const changedPaths = [...new Set([...carriedChangedPaths, ...next.changedPaths])];
      carriedChangedPaths = new Set<string>();
      next = { ...next, changedPaths };
      const busy = !waiting && (opening || active !== null || pending !== null);
      if (state !== "working") completion = Deferred.makeUnsafe<void, Failure>();
      state = "working";
      pending = pending === null ? next : {
        kind: pending.kind === "resync" || next.kind === "resync" ? "resync" : next.kind,
        force: pending.force || next.force,
        changedPaths: [...new Set([...pending.changedPaths, ...next.changedPaths])],
      };
      if (!busy) {
        waiting = false;
        Queue.offerUnsafe(wake, undefined);
      }
      return busy ? "queued" : "started";
    };

    const runPass = (live: Synchronization<W, E | FreshnessFailed>, requested: PassRequest) => Effect.gen(function* () {
      const entries = yield* scanSource(options.sourcePath, options.includeSource);
      const changedPaths = [...new Set([...requested.changedPaths, ...differences(baseline, entries)])];
      active = { ...requested, changedPaths };
      const plan = yield* options.declare(entries, active);
      yield* live.submit(plan.work, active);
      yield* live.awaitCompletion;
      yield* plan.publish;
      baseline = entries;
      const after = yield* scanSource(options.sourcePath, options.includeSource);
      const changed = differences(entries, after);
      if (changed.length > 0) request({ kind: "watcher", force: false, changedPaths: changed });
    });

    const consume = (live: Synchronization<W, E | FreshnessFailed>) => Effect.gen(function* () {
      while (true) {
        while (pending !== null) {
          const next = pending;
          pending = null;
          active = next;
          const exit = yield* Effect.exit(runPass(live, next));
          if (Exit.isFailure(exit)) carriedChangedPaths = new Set([...carriedChangedPaths, ...next.changedPaths]);
          active = null;
          failure = Exit.isFailure(exit) ? exit.cause : null;
        }
        if (state === "working") {
          state = failure === null ? (yield* live.status).state : "failed";
          Deferred.doneUnsafe(completion, failure === null ? Effect.void : Effect.failCause(failure));
        }
        yield* Queue.take(wake);
      }
    });

    // One attempt owns its lease and scheduler in a scope of its own, so a failed attempt releases both before the retry.
    const attempt = Effect.acquireUseRelease(
      Scope.make(),
      (scope) => Effect.gen(function* () {
        // The traversal after the first pass belongs to the open: a change it finds is already pending when `ready` settles.
        const opened = yield* Effect.exit(Effect.gen(function* () {
          const synchronization = yield* openSynchronization({
            ...options,
            // A plan that declares no minimum has published nothing usable when `onMinimum` runs.
            onMinimum: Effect.sync(() => { if (declaredMinimum) availability ??= "minimum-publication"; }).pipe(Effect.andThen(options.onMinimum ?? Effect.void)),
            declare: (entries) => {
              baseline = entries;
              return options.declare(entries, INITIAL).pipe(Effect.tap((plan) => Effect.sync(() => { declaredMinimum = (plan.minimum?.length ?? 0) > 0; })));
            },
          });
          // An initial traversal is not a snapshot either.
          const afterInitial = yield* scanSource(options.sourcePath, options.includeSource);
          return { synchronization, afterInitial };
        }).pipe(Scope.provide(scope)));
        if (Exit.isFailure(opened)) return opened.cause;
        const live = opened.value.synchronization;
        scheduler = live;
        opening = false;
        failure = null;
        const initialChanges = differences(baseline, opened.value.afterInitial);
        if (initialChanges.length > 0) request({ kind: "watcher", force: false, changedPaths: initialChanges });
        Deferred.doneUnsafe(ready, Effect.void);
        Deferred.doneUnsafe(settled, Effect.void);
        return yield* consume(live);
      }).pipe(Effect.ensuring(Effect.sync(() => { active = null; }))),
      (scope, exit) => Scope.close(scope, exit),
    );

    const supervise = Effect.gen(function* () {
      while (true) {
        opening = true;
        const cause = yield* attempt;
        scheduler = undefined;
        opening = false;
        failure = cause;
        state = "failed";
        // Without usable output a served-but-empty deployment would be a lie.
        const fatal = options.recovery === undefined || availability === null;
        // Settle the flags before any Deferred: a waiter may resume inline and must see a consistent session.
        waiting = !fatal;
        if (fatal) {
          terminal = true;
          state = "stopped";
        }
        Deferred.doneUnsafe(completion, Effect.failCause(cause));

        if (fatal) {
          Deferred.doneUnsafe(ready, Effect.failCause(cause));
          return;
        }
        Deferred.doneUnsafe(ready, Effect.void);
        Deferred.doneUnsafe(settled, Effect.void);
        yield* Queue.take(wake);
      }
    });

    yield* Effect.forkScoped(supervise);

    if ((options.reconcileIntervalMs ?? 0) > 0) {
      yield* Effect.forkScoped(Effect.gen(function* () {
        // The timer starts after the first outcome, so it neither queues a pass behind the open nor races the retry.
        yield* Deferred.await(settled);
        while (true) {
          yield* Effect.sleep(options.reconcileIntervalMs!);
          request({ kind: "reconcile", force: false, changedPaths: [] });
        }
      }));
    }

    // Registered last, so it runs first when the scope closes: admission stops before the fibers are joined.
    yield* Effect.addFinalizer(() => Effect.sync(() => {
      state = "stopped";
      pending = null;
      Deferred.doneUnsafe(completion, Effect.interrupt);
      Deferred.doneUnsafe(ready, Effect.interrupt);
    }));

    return {
      requestPass: (input = {}) => Effect.sync(() => request({ kind: "resync", force: input.force ?? false, changedPaths: [] })),
      notify: (changedPaths) => Effect.sync(() => request({ kind: "watcher", force: false, changedPaths })),
      awaitCompletion: Effect.suspend(() => Deferred.await(completion)),
      ready: Effect.suspend(() => Deferred.await(ready)),
      status: Effect.gen(function* () {
        const work: WorkStatus<W> = scheduler !== undefined
          ? yield* scheduler.status
          : { state: state === "stopped" ? "stopped" : opening ? "working" : "failed", pending: 0, active: null, errors: [] };
        return { state, pass: opening && state !== "stopped" ? INITIAL : active, followUp: pending, failure, availability, work };
      }),
    };
  });
}

/** The first pass finished (or failed while output was usable, with `recovery`) before the session is returned. */
export function openLiveSynchronization<W, E, R>(options: LiveOptions<W, E, R>): Effect.Effect<LiveSynchronization<W, E | FreshnessFailed | OutputOwnershipFailed>, E | FreshnessFailed | ScanFailed | OutputOwnershipFailed, R | Scope.Scope> {
  return Effect.gen(function* () {
    const handle = yield* startLiveSynchronization(options);
    yield* handle.ready;
    return handle;
  });
}
