import { Deferred, Effect, Exit, Queue, Scope, type Cause } from "effect";
import { scanSource, type InitialPass, type InitialPlan, type SourceEntry, type Synchronization, type ScanFailed, type OutputOwnershipFailed, type FreshnessFailed } from "./index.ts";
import { openSynchronizationWithHooks, type InternalSynchronization } from "./internal.ts";
import type { WorkStatus } from "./work.ts";

export interface PassRequest {
  readonly kind: "initial" | "resync" | "watcher" | "reconcile";
  readonly force: boolean;
  /** Relative source paths; hints bypass source metadata equality. */
  readonly changedPaths: readonly string[];
}

export type PassAdmission = "started" | "queued" | "rejected";

/** `prior-output`: usable output existed before the session. `minimum-publication`: this session published usable output. */
export type Availability = "prior-output" | "minimum-publication";

interface LiveInternalHooks {
  /** Test seam for the opening window after publication and before freshness commit. */
  readonly afterOpeningPublication?: Effect.Effect<void>;
  /** Test seam after request invalidation and before admission bookkeeping. */
  readonly afterRequestInvalidation?: Effect.Effect<void>;
}

export interface LiveOptions<W, E, R> extends Omit<InitialPass<W, E, R>, "declare"> {
  readonly declare: (entries: readonly SourceEntry[], request: PassRequest) => Effect.Effect<InitialPlan<W, E, R>, E, R>;
  /** Zero disables the owned periodic timer. */
  readonly reconcileIntervalMs?: number;
  /**
   * Opt-in failure map for a failed attempt. Without it a defect or failed first pass fails the session. With it, a failure is
   * tolerated while output is usable: `existing` reports earlier output that already serves, and a successful
   * publication in this session also counts. If a request was already queued, the session performs one immediate
   * retry; if another request arrives during that retry and the retry fails, that newer request reopens at once.
   * Otherwise it reports `LiveStatus.failure` and reopens on the next `requestPass`/`notify` or reconcile tick.
   * Without usable output the open still fails.
   */
  readonly recovery?: { readonly existing: Effect.Effect<boolean, never, R> };
}

export interface LiveStatus<W> {
  readonly state: WorkStatus<W>["state"];
  readonly pass: PassRequest | null;
  readonly followUp: PassRequest | null;
  /** Cause of the last failed recoverable pass/open; a later successful attempt clears it. */
  readonly failure: Cause.Cause<unknown> | null;
  /** Null until output is usable. */
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
 * Owns scan, processing, required publication, follow-up, the periodic timer and the retry of a failed attempt.
 * The handle is usable at once: requests made before the first pass finished combine and run after it.
 */
export function startLiveSynchronization<W, E, R>(options: LiveOptions<W, E, R>): Effect.Effect<LiveHandle<W, E | FreshnessFailed | OutputOwnershipFailed>, never, R | Scope.Scope> {
  return startLiveSynchronizationWithHooks(options);
}

export function startLiveSynchronizationWithHooks<W, E, R>(options: LiveOptions<W, E, R>, internal: LiveInternalHooks = {}): Effect.Effect<LiveHandle<W, E | FreshnessFailed | OutputOwnershipFailed>, never, R | Scope.Scope> {
  type Failure = E | FreshnessFailed | ScanFailed | OutputOwnershipFailed;
  type MachineState = "opening" | "running" | "retrying-after-failed-opening" | "reopening" | "stopped-fatal" | "stopped";

  return Effect.gen(function* () {
    let availability: Availability | null = options.recovery !== undefined && (yield* options.recovery.existing) ? "prior-output" : null;
    let baseline: readonly SourceEntry[] = [];
    let scheduler: Synchronization<W, E | FreshnessFailed> | undefined;
    let openingScheduler: Synchronization<W, E | FreshnessFailed> | undefined;
    const wake = yield* Queue.unbounded<void>();
    let machine: MachineState = "opening";
    let declaredMinimum = false;
    let state: LiveStatus<W>["state"] = "working";
    let pending: PassRequest | null = null;
    let retryAfterFailedOpening: PassRequest | null = null;
    let reopenTrigger: PassRequest | null = null;
    let active: PassRequest | null = null;
    let carriedChangedPaths = new Set<string>();
    let carriedForce = false;
    let openingChangedPaths = new Set<string>();
    let openingForce = false;
    let failure: Cause.Cause<Failure> | null = null;
    let completion = Deferred.makeUnsafe<void, Failure>();
    const ready = Deferred.makeUnsafe<void, Failure>();
    const settled = Deferred.makeUnsafe<void>();

    const combine = (left: PassRequest, right: PassRequest): PassRequest => ({
      kind: left.kind === "resync" || right.kind === "resync" ? "resync" : right.kind,
      force: left.force || right.force,
      changedPaths: [...new Set([...left.changedPaths, ...right.changedPaths])],
    });

    const mergePending = (next: PassRequest) => {
      pending = pending === null ? next : combine(pending, next);
    };

    const invalidateThrough = (target: Synchronization<W, E | FreshnessFailed> | undefined, next: PassRequest) => Effect.gen(function* () {
      if (target === undefined) return;
      if (next.force || next.changedPaths.length > 0) yield* Effect.exit(target.submit([], { force: next.force, changedPaths: next.changedPaths }));
    });

    const drainWakeUnsafe = () => { while (Queue.takeUnsafe(wake) !== undefined) {} };

    const startWorking = () => {
      if (state !== "working") completion = Deferred.makeUnsafe<void, Failure>();
      state = "working";
    };

    const clearQueued = () => {
      pending = null;
      retryAfterFailedOpening = null;
      reopenTrigger = null;
      carriedChangedPaths = new Set<string>();
      carriedForce = false;
      openingChangedPaths = new Set<string>();
      openingForce = false;
    };

    const isStopped = () => state === "stopped";

    const request = (next: PassRequest): Effect.Effect<PassAdmission> => Effect.gen(function* () {
      if (isStopped()) return "rejected";
      const changedPaths = [...new Set([...carriedChangedPaths, ...next.changedPaths])];
      const force = carriedForce || next.force;
      carriedChangedPaths = new Set<string>();
      carriedForce = false;
      next = { ...next, force, changedPaths };
      const inOpeningWindow = machine === "opening" || machine === "retrying-after-failed-opening" || openingScheduler !== undefined;
      const invalidationTarget = inOpeningWindow ? openingScheduler ?? scheduler : scheduler;
      if (inOpeningWindow) {
        openingForce ||= force;
        for (const path of changedPaths) openingChangedPaths.add(path);
      }
      yield* invalidateThrough(invalidationTarget, next);
      yield* (internal.afterRequestInvalidation ?? Effect.void);
      if (isStopped()) return "rejected";
      const busy = (machine === "opening" || machine === "retrying-after-failed-opening" || active !== null || pending !== null) && machine !== "reopening";
      startWorking();
      mergePending(next);
      if (!busy) {
        machine = scheduler === undefined ? "reopening" : "running";
        Queue.offerUnsafe(wake, undefined);
      }
      return busy ? "queued" : "started";
    });

    const consumeOpeningInvalidation = () => {
      const input = { force: openingForce, changedPaths: [...openingChangedPaths] };
      openingForce = false;
      openingChangedPaths = new Set<string>();
      return input;
    };

    const shouldEndAttempt = (cause: Cause.Cause<Failure>, live: Synchronization<W, E | FreshnessFailed>) => Effect.gen(function* () {
      if (cause.reasons.some((reason) => reason._tag !== "Fail")) return true;
      return (yield* live.status).state === "failed";
    });

    const runPass = (live: InternalSynchronization<W, E | FreshnessFailed>, requested: PassRequest) => Effect.gen(function* () {
      const entries = yield* scanSource(options.sourcePath, options.includeSource);
      const changedPaths = [...new Set([...requested.changedPaths, ...differences(baseline, entries)])];
      active = { ...requested, changedPaths };
      const plan = yield* options.declare(entries, active);
      yield* live.submit(plan.work, active);
      yield* live.awaitCompletion;
      yield* plan.publish;
      yield* live.commitFreshness;
      baseline = entries;
      const after = yield* scanSource(options.sourcePath, options.includeSource);
      const changed = differences(entries, after);
      if (changed.length > 0) yield* request({ kind: "watcher", force: false, changedPaths: changed });
    });

    const consume = (live: InternalSynchronization<W, E | FreshnessFailed>) => Effect.gen(function* () {
      while (true) {
        while (pending !== null) {
          const next = {
            ...pending,
            force: pending.force || carriedForce,
            changedPaths: [...new Set([...carriedChangedPaths, ...pending.changedPaths])],
          };
          pending = null;
          carriedChangedPaths = new Set<string>();
          carriedForce = false;
          active = next;
          const exit = yield* Effect.exit(runPass(live, next));
          if (Exit.isFailure(exit)) {
            carriedChangedPaths = new Set([...carriedChangedPaths, ...next.changedPaths]);
            carriedForce ||= next.force;
          }
          active = null;
          failure = Exit.isFailure(exit) ? exit.cause : null;
          if (failure !== null && (yield* shouldEndAttempt(failure, live))) return failure;
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
          const synchronization = yield* openSynchronizationWithHooks({
            ...options,
            // A plan that declares no minimum has published nothing usable when `onMinimum` runs.
            onMinimum: Effect.sync(() => { if (declaredMinimum) availability ??= "minimum-publication"; }).pipe(Effect.andThen(options.onMinimum ?? Effect.void)),
            declare: (entries) => {
              baseline = entries;
              return options.declare(entries, INITIAL).pipe(Effect.tap((plan) => Effect.sync(() => { declaredMinimum = (plan.minimum?.length ?? 0) > 0; })));
            },
          }, {
            deferCommit: true,
            beforeCommit: (live) => Effect.gen(function* () {
              availability ??= "minimum-publication";
              openingScheduler = live;
              const input = consumeOpeningInvalidation();
              if (input.force || input.changedPaths.length > 0) yield* live.submit([], input);
              yield* (internal.afterOpeningPublication ?? Effect.void);
            }),
          });
          // An initial traversal is not a snapshot either.
          const afterInitial = yield* scanSource(options.sourcePath, options.includeSource);
          return { synchronization, afterInitial };
        }).pipe(Scope.provide(scope)));
        if (Exit.isFailure(opened)) return opened.cause;
        const live = opened.value.synchronization;
        scheduler = live;
        openingScheduler = live;
        machine = "running";
        state = "working";
        failure = null;
        if (retryAfterFailedOpening !== null) {
          mergePending(retryAfterFailedOpening);
          retryAfterFailedOpening = null;
        }
        if (reopenTrigger !== null) {
          mergePending(reopenTrigger);
          reopenTrigger = null;
        }
        const initialChanges = differences(baseline, opened.value.afterInitial);
        if (initialChanges.length > 0) mergePending({ kind: "watcher", force: false, changedPaths: initialChanges });
        consumeOpeningInvalidation();
        openingScheduler = undefined;
        Deferred.doneUnsafe(ready, Effect.void);
        Deferred.doneUnsafe(settled, Effect.void);
        return yield* consume(live);
      }).pipe(Effect.ensuring(Effect.sync(() => { active = null; }))),
      (scope, exit) => Scope.close(scope, exit),
    );

    const supervise = Effect.gen(function* () {
      while (true) {
        const attemptWasOneShotRetry = retryAfterFailedOpening !== null;
        if (machine === "reopening" && retryAfterFailedOpening === null && pending !== null) {
          reopenTrigger = reopenTrigger === null ? pending : combine(reopenTrigger, pending);
          pending = null;
        }
        machine = retryAfterFailedOpening === null ? "opening" : "retrying-after-failed-opening";
        const attemptExit = yield* Effect.exit(attempt);
        const cause = Exit.isFailure(attemptExit) ? attemptExit.cause as Cause.Cause<Failure> : attemptExit.value;
        scheduler = undefined;
        openingScheduler = undefined;
        failure = cause;
        state = "failed";
        // Without usable output a served-but-empty deployment would be a lie.
        const fatal = options.recovery === undefined || availability === null;
        const retryNow = !fatal && retryAfterFailedOpening === null && pending !== null;
        const wakeNewerPending = !fatal && attemptWasOneShotRetry && pending !== null;
        // Settle the flags before any Deferred: a waiter may resume inline and must see a consistent session.
        if (fatal) {
          machine = "stopped-fatal";
          state = "stopped";
          clearQueued();
        }
        if (retryNow) {
          retryAfterFailedOpening = pending;
          pending = null;
          machine = "retrying-after-failed-opening";
          state = "working";
        } else {
          retryAfterFailedOpening = null;
          if (!fatal) machine = "reopening";
          drainWakeUnsafe();
          if (wakeNewerPending) {
            state = "working";
            Queue.offerUnsafe(wake, undefined);
          } else {
            Deferred.doneUnsafe(completion, Effect.failCause(cause));
          }
        }

        if (fatal) {
          Deferred.doneUnsafe(ready, Effect.failCause(cause));
          return;
        }
        Deferred.doneUnsafe(ready, Effect.void);
        Deferred.doneUnsafe(settled, Effect.void);
        if (!retryNow && !wakeNewerPending) {
          yield* Queue.take(wake);
        }
      }
    });

    yield* Effect.forkScoped(supervise);

    if ((options.reconcileIntervalMs ?? 0) > 0) {
      yield* Effect.forkScoped(Effect.gen(function* () {
        // The timer starts after the first outcome, so it neither queues a pass behind the open nor races the retry.
          yield* Deferred.await(settled);
          while (true) {
            yield* Effect.sleep(options.reconcileIntervalMs!);
            yield* request({ kind: "reconcile", force: false, changedPaths: [] });
          }
        }));
    }

    // Registered last, so it runs first when the scope closes: admission stops before the fibers are joined.
    yield* Effect.addFinalizer(() => Effect.sync(() => {
      machine = "stopped";
      state = "stopped";
      clearQueued();
      Deferred.doneUnsafe(completion, Effect.interrupt);
      Deferred.doneUnsafe(ready, Effect.interrupt);
    }));

    return {
      requestPass: (input = {}) => Effect.gen(function* () {
        const force = input.force ?? false;
        const admission = yield* request({ kind: "resync", force, changedPaths: [] });
        return admission;
      }),
      notify: (changedPaths) => Effect.gen(function* () {
        const admission = yield* request({ kind: "watcher", force: false, changedPaths });
        return admission;
      }),
      awaitCompletion: Effect.suspend(() => Deferred.await(completion)),
      ready: Effect.suspend(() => Deferred.await(ready)),
      status: Effect.gen(function* () {
        const work: WorkStatus<W> = scheduler !== undefined
          ? yield* scheduler.status
          : { state: state === "stopped" ? "stopped" : machine === "opening" || machine === "retrying-after-failed-opening" ? "working" : "failed", pending: 0, active: null, errors: [] };
        return { state, pass: (machine === "opening" || machine === "retrying-after-failed-opening") && state !== "stopped" ? INITIAL : active, followUp: pending ?? retryAfterFailedOpening ?? reopenTrigger, failure, availability, work };
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
