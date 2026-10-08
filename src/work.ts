import { Cause, Deferred, Effect, Exit, Queue, type Scope } from "effect";

export interface WorkStatus<W> {
  readonly state: "working" | "complete" | "failed" | "stopped";
  readonly pending: number;
  readonly active: W | null;
}

export interface WorkScheduler<W, E> {
  readonly submit: (work: readonly W[]) => Effect.Effect<void, E>;
  /** Includes all required work returned by handlers, not freshness verification. */
  readonly awaitCompletion: Effect.Effect<void, E>;
  readonly status: Effect.Effect<WorkStatus<W>>;
}

export interface WorkOptions<W, E, R> {
  readonly handle: (work: W) => Effect.Effect<readonly W[], E, R>;
  /** Only pending work with a defined key combines. Active work always gets a follow-up. */
  readonly key?: (work: W) => string | undefined;
}

export function createWorkScheduler<W, E, R>(options: WorkOptions<W, E, R>): Effect.Effect<WorkScheduler<W, E>, never, R | Scope.Scope> {
  return Effect.gen(function* () {
    const wake = yield* Queue.unbounded<void>();
    const pending: W[] = [];
    let active: W | null = null;
    let state: WorkStatus<W>["state"] = "complete";
    let failure: Cause.Cause<E> | undefined;
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

    const consume = Effect.gen(function* () {
      while (true) {
        yield* Queue.take(wake);
        while (pending.length > 0) {
          active = pending.shift()!;
          const work = active;
          const exit = yield* Effect.exit(Effect.suspend(() => options.handle(work)));
          if (Exit.isFailure(exit)) {
            failure = exit.cause;
            state = "failed";
            active = null;
            pending.length = 0;
            Deferred.doneUnsafe(completion, Effect.failCause(exit.cause));
            return;
          }
          // Required work joins pending before clearing active or reporting completion.
          enqueue(exit.value);
          active = null;
        }
        state = "complete";
        Deferred.doneUnsafe(completion, Effect.void);
      }
    });

    yield* Effect.addFinalizer(() => Effect.sync(() => {
      state = "stopped";
      pending.length = 0;
      Deferred.doneUnsafe(completion, Effect.interrupt);
    }));
    yield* Effect.forkScoped(consume.pipe(Effect.ensuring(Effect.sync(() => { active = null; }))));

    return {
      submit: (work) => Effect.suspend(() => {
        if (failure !== undefined) return Effect.failCause(failure);
        if (state === "stopped") return Effect.interrupt;
        if (work.length === 0) return Effect.void;
        const wasComplete = state === "complete";
        if (wasComplete) completion = Deferred.makeUnsafe<void, E>();
        state = "working";
        enqueue(work);
        if (wasComplete) Queue.offerUnsafe(wake, undefined);
        return Effect.void;
      }),
      awaitCompletion: Effect.suspend(() => Deferred.await(completion)),
      status: Effect.sync(() => ({ state, pending: pending.length, active })),
    };
  });
}
