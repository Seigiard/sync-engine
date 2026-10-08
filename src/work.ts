import { Cause, Deferred, Effect, Exit, Queue, type Scope } from "effect";

export interface WorkFailure<W, E = unknown> {
  readonly work: W;
  readonly cause: Cause.Cause<E>;
}

export interface WorkStatus<W, E = unknown> {
  readonly state: "working" | "complete" | "complete-with-errors" | "failed" | "stopped";
  readonly pending: number;
  readonly active: W | null;
  readonly errors: readonly WorkFailure<W, E>[];
}

export interface WorkScheduler<W, E> {
  readonly submit: (work: readonly W[]) => Effect.Effect<void, E>;
  /** Includes all required work returned by handlers, not freshness verification. */
  readonly awaitCompletion: Effect.Effect<void, E>;
  readonly status: Effect.Effect<WorkStatus<W, E>>;
}

export interface WorkOptions<W, E, R> {
  readonly handle: (work: W) => Effect.Effect<readonly W[], E, R>;
  /** Maximum number of independent work items that may run at the same time. Defaults to one. */
  readonly concurrency?: number;
  /** Only pending work with a defined key combines. Active work always gets a follow-up. */
  readonly key?: (work: W) => string | undefined;
  /** Error identity is separate from pending coalescing. A successful retry clears it. */
  readonly failureKey?: (work: W) => string | undefined;
}

export function createWorkScheduler<W, E, R>(options: WorkOptions<W, E, R>): Effect.Effect<WorkScheduler<W, E>, never, R | Scope.Scope> {
  return Effect.gen(function* () {
    const wake = yield* Queue.unbounded<void>();
    const pending: W[] = [];
    const active: W[] = [];
    const activeKeys = new Set<string>();
    const requestedConcurrency = options.concurrency ?? 1;
    const concurrency = Number.isFinite(requestedConcurrency) ? Math.max(1, Math.floor(requestedConcurrency)) : 1;
    let state: WorkStatus<W>["state"] = "complete";
    let failure: Cause.Cause<E> | undefined;
    const errors: WorkFailure<W, E>[] = [];
    let completion = Deferred.makeUnsafe<void, E>();
    Deferred.doneUnsafe(completion, Effect.void);

    const enqueue = (work: readonly W[]) => {
      for (const item of work) {
        const key = options.key?.(item);
        const previous = key === undefined ? -1 : pending.findIndex((candidate) => options.key?.(candidate) === key);
        // A repeated request reflects later changes: refresh after intervening work.
        if (previous !== -1) pending.splice(previous, 1);
        pending.push(item);
      }
    };

    const clearError = (work: W) => {
      const key = options.failureKey?.(work);
      const index = errors.findIndex((error) => key === undefined ? Object.is(error.work, work) : options.failureKey?.(error.work) === key);
      if (index !== -1) errors.splice(index, 1);
    };

    const removeActive = (work: W) => {
      const index = active.findIndex((item) => Object.is(item, work));
      if (index !== -1) active.splice(index, 1);
    };

    const takeRunnable = () => {
      const index = pending.findIndex((work) => {
        const key = options.key?.(work);

        return key === undefined || !activeKeys.has(key);
      });

      return index === -1 ? undefined : pending.splice(index, 1)[0];
    };

    const wakeWorkers = () => {
      for (let index = 0; index < concurrency; index += 1) Queue.offerUnsafe(wake, undefined);
    };

    const completeIfDrained = () => {
      if (state !== "working" || pending.length > 0 || active.length > 0) return;

      state = errors.length === 0 ? "complete" : "complete-with-errors";
      Deferred.doneUnsafe(completion, Effect.void);
    };

    const consume = Effect.gen(function* () {
      while (true) {
        yield* Queue.take(wake);
        if (state === "failed" || state === "stopped") continue;
        while (state === "working" && pending.length > 0) {
          const work = takeRunnable();
          if (work === undefined) break;
          const activeKey = options.key?.(work);
          active.push(work);
          if (activeKey !== undefined) activeKeys.add(activeKey);
          const exit = yield* Effect.exit(Effect.suspend(() => options.handle(work)));
          removeActive(work);
          if (activeKey !== undefined) activeKeys.delete(activeKey);
          if (state !== "working") return;
          if (Exit.isFailure(exit)) {
            if (exit.cause.reasons.every((reason) => reason._tag === "Fail")) {
              clearError(work);
              errors.push({ work, cause: exit.cause });
              wakeWorkers();
              completeIfDrained();
              continue;
            }
            failure = exit.cause;
            state = "failed";
            pending.length = 0;
            Deferred.doneUnsafe(completion, Effect.failCause(exit.cause));
            return;
          }
          clearError(work);
          // Required work joins pending before clearing active or reporting completion.
          enqueue(exit.value);
          wakeWorkers();
        }
        completeIfDrained();
      }
    });

    yield* Effect.forEach(Array.from({ length: concurrency }), () => Effect.forkScoped(consume), { discard: true });
    yield* Effect.addFinalizer(() => Effect.sync(() => {
      state = "stopped";
      pending.length = 0;
      active.length = 0;
      activeKeys.clear();
      Deferred.doneUnsafe(completion, Effect.interrupt);
    }));

    return {
      submit: (work) => Effect.suspend(() => {
        if (failure !== undefined) return Effect.failCause(failure);
        if (state === "stopped") return Effect.interrupt;
        if (work.length === 0) return Effect.void;
        const wasComplete = state === "complete" || state === "complete-with-errors";
        if (wasComplete) completion = Deferred.makeUnsafe<void, E>();
        state = "working";
        enqueue(work);
        wakeWorkers();
        return Effect.void;
      }),
      awaitCompletion: Effect.suspend(() => Deferred.await(completion)),
      status: Effect.sync(() => ({ state, pending: pending.length, active: active[0] ?? null, errors: [...errors] })),
    };
  });
}
