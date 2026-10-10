import { Cause, Deferred, Effect, Exit, Fiber, Scope } from "effect";
import { scanSource, type InitialPass, type InitialPlan, type SourceEntry, type ScanFailed, type OutputOwnershipFailed, type FreshnessFailed } from "./index.ts";
import { openSynchronizationWithHooks, type InternalSynchronization } from "./internal.ts";
import { INITIAL, init, invalidationTarget, Payload, step, view, type Availability, type Command, type Event, type PassAdmission, type PassRequest, type Session, type Settlement } from "./live-machine.ts";
import type { WorkStatus } from "./work.ts";

export type { Availability, PassAdmission, PassRequest } from "./live-machine.ts";

interface LiveInternalHooks {
  /** Test seam for the opening window after publication and before freshness commit. */
  readonly afterOpeningPublication?: Effect.Effect<void>;
  /** Test seam after request invalidation and before admission bookkeeping. */
  readonly afterRequestInvalidation?: Effect.Effect<void>;
  /** Test seam in a failed attempt's close, after its activity ended and before its scope closes. */
  readonly beforeAttemptClose?: (attempt: number) => Effect.Effect<void>;
  /** Test seam after a failed attempt's scope closed and before the session learns that. */
  readonly afterAttemptClose?: (attempt: number) => Effect.Effect<void>;
}

export interface LiveOptions<W, E, R> extends Omit<InitialPass<W, E, R>, "declare"> {
  readonly declare: (entries: readonly SourceEntry[], request: PassRequest) => Effect.Effect<InitialPlan<W, E, R>, E, R>;
  /** Zero disables the owned periodic timer. A tick acts only while the session is idle. */
  readonly reconcileIntervalMs?: number;
  /**
   * Opt-in failure map for a failed attempt. Without it a defect or failed first pass fails the session. With it, a failure is
   * tolerated while output is usable: `existing` reports earlier output that already serves, and a successful
   * publication in this session also counts. A request admitted during the failed attempt starts one more attempt at
   * once; the request that started the attempt does not count. Otherwise it reports `LiveStatus.failure` and reopens on
   * the next `requestPass`/`notify` or reconcile tick. Without usable output the open still fails.
   */
  readonly recovery?: { readonly existing: Effect.Effect<boolean, never, R> };
}

export interface LiveStatus<W> {
  readonly state: WorkStatus<W>["state"];
  readonly pass: PassRequest | null;
  /** A follow-up pass that is scheduled to run; retained data of a failed pass joins the next request instead. */
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

/**
 * Interpreter of `live-machine.ts`. It decides nothing: `dispatch` commits the next session value, then executes the
 * commands, and every activity reports its outcome as exactly one event.
 */
export function startLiveSynchronizationWithHooks<W, E, R>(options: LiveOptions<W, E, R>, internal: LiveInternalHooks = {}): Effect.Effect<LiveHandle<W, E | FreshnessFailed | OutputOwnershipFailed>, never, R | Scope.Scope> {
  type Failure = E | FreshnessFailed | ScanFailed | OutputOwnershipFailed;
  type Live = InternalSynchronization<W, E | FreshnessFailed>;
  interface Attempt {
    readonly scope: Scope.Closeable;
    /** Parent of every later pass's scope. Forked during the initial declaration, so it closes after the scheduler stops and before the lease is released. */
    passes?: Scope.Closeable;
    live?: Live;
    activity?: Fiber.Fiber<void>;
  }

  return Effect.gen(function* () {
    const services = yield* Effect.context<R | Scope.Scope>();
    const existing = options.recovery !== undefined && (yield* options.recovery.existing);
    const first = init<Cause.Cause<Failure>>({ recovery: options.recovery !== undefined, availability: existing ? "prior-output" : null });
    let state: Session<Cause.Cause<Failure>> = first.state;
    const attempts = new Map<number, Attempt>();
    const fibers = new Set<Fiber.Fiber<void>>();
    const completions = new Map<number, Deferred.Deferred<void, Failure>>();
    const ready = Deferred.makeUnsafe<void, Failure>();
    // Written only by the one running activity (opening or pass); the machine never reads source entries.
    let baseline: readonly SourceEntry[] = [];

    const completionFor = (gen: number) => {
      let deferred = completions.get(gen);
      if (deferred === undefined) {
        deferred = Deferred.makeUnsafe<void, Failure>();
        completions.set(gen, deferred);
      }
      return deferred;
    };

    const settlement = (exit: Settlement<Cause.Cause<Failure>>): Effect.Effect<void, Failure> =>
      exit.tag === "success" ? Effect.void : exit.tag === "failure" ? Effect.failCause(exit.cause) : Effect.interrupt;

    // Detached fibers carry the session's services; the session finalizers own their interruption.
    const track = (effect: Effect.Effect<void, never, R | Scope.Scope>): Effect.Effect<Fiber.Fiber<void>> => Effect.gen(function* () {
      const fiber = yield* Effect.forkDetach(effect.pipe(Effect.provideContext(services)));
      fibers.add(fiber);
      fiber.addObserver(() => fibers.delete(fiber));
      return fiber;
    });

    // Starting activities first and settling Deferreds last means an inline-resumed waiter observes launched work.
    const execute = (commands: readonly Command<Cause.Cause<Failure>>[]): Effect.Effect<Cause.Cause<E | FreshnessFailed> | null> => Effect.gen(function* () {
      for (const command of commands) {
        if (command.tag === "startAttempt") yield* startAttempt(command.attempt);
        if (command.tag === "startPass") yield* startPass(command.attempt, command.pass);
        if (command.tag === "closeAttempt") yield* track(closeAttempt(command.attempt, command.cause));
      }
      let failure: Cause.Cause<E | FreshnessFailed> | null = null;
      for (const command of commands) {
        if (command.tag !== "invalidate") continue;
        const live = attempts.get(command.attempt)?.live;
        if (live === undefined) continue;
        const exit = yield* Effect.exit(live.submit([], { force: command.force, changedPaths: command.paths }));
        if (Exit.isFailure(exit)) failure ??= exit.cause;
      }
      for (const command of commands) {
        if (command.tag === "settleCompletion") {
          Deferred.doneUnsafe(completionFor(command.gen), settlement(command.exit));
          for (const gen of completions.keys()) if (gen < state.ids.gen) completions.delete(gen);
        }
        if (command.tag === "settleReady") Deferred.doneUnsafe(ready, settlement(command.exit));
      }
      return failure;
    });

    const dispatch = (event: Event<Cause.Cause<Failure>>): Effect.Effect<{ readonly admission: PassAdmission | null; readonly claimed: PassRequest | undefined; readonly failure: Cause.Cause<E | FreshnessFailed> | null }> => Effect.suspend(() => {
      const next = step(state, event);
      state = next.state;
      return execute(next.commands).pipe(Effect.map((failure) => ({ admission: next.admission, claimed: next.claimed, failure })));
    }).pipe(Effect.uninterruptible);

    const report = (event: Event<Cause.Cause<Failure>>): Effect.Effect<void> => dispatch(event).pipe(Effect.asVoid);

    /**
     * The one way an activity fiber (opening, pass or close) ends. Its body runs interruptibly under `Effect.exit`,
     * so success, typed failure, defect, interruption, a failing finalizer the body awaits and a failing trailing
     * step all become an exit. `outcome` turns that exit into exactly one event; if `outcome` itself fails, the pure
     * `fallback` supplies the event. The dispatch then runs uninterruptibly, so no exit path can skip it.
     */
    const activity = <A, X>(
      body: Effect.Effect<A, X, R>,
      outcome: (exit: Exit.Exit<A, X>) => Effect.Effect<Event<Cause.Cause<Failure>>>,
      fallback: (cause: Cause.Cause<unknown>) => Event<Cause.Cause<Failure>>,
    ): Effect.Effect<void, never, R> => Effect.uninterruptibleMask((restore) => Effect.gen(function* () {
      const exit = yield* Effect.exit(restore(body));
      const event = yield* Effect.exit(outcome(exit));
      yield* report(Exit.isSuccess(event) ? event.value : fallback(event.cause));
    }));

    const opening = (attempt: number, entry: Attempt): Effect.Effect<void, never, R> => {
      let declaredMinimum = false;
      const body = Effect.gen(function* () {
        yield* openSynchronizationWithHooks({
          ...options,
          // A plan that declares no minimum has published nothing usable when `onMinimum` runs.
          onMinimum: Effect.suspend(() => declaredMinimum ? report({ tag: "usable", attempt }) : Effect.void).pipe(Effect.andThen(options.onMinimum ?? Effect.void)),
          declare: (entries) => Effect.gen(function* () {
            baseline = entries;
            entry.passes = yield* Scope.fork(entry.scope);
            const plan = yield* options.declare(entries, INITIAL);
            declaredMinimum = (plan.minimum?.length ?? 0) > 0;
            return plan;
          }),
        }, {
          deferCommit: true,
          beforeCommit: (live) => Effect.gen(function* () {
            entry.live = live;
            const { failure } = yield* dispatch({ tag: "freshnessReady", attempt });
            if (failure !== null) return yield* Effect.failCause(failure);
            yield* (internal.afterOpeningPublication ?? Effect.void);
          }),
        }).pipe(Scope.provide(entry.scope));
        // The traversal after the first pass belongs to the open: a change it finds is already due when `ready` settles.
        const afterInitial = yield* scanSource(options.sourcePath, options.includeSource);
        return differences(baseline, afterInitial);
      });
      return activity(
        body,
        (exit) => Effect.succeed(Exit.isSuccess(exit) ? { tag: "openOk", attempt, changes: exit.value } : { tag: "openFail", attempt, cause: exit.cause }),
        (cause) => ({ tag: "openFail", attempt, cause: cause as Cause.Cause<Failure> }),
      );
    };

    const startAttempt = (attempt: number): Effect.Effect<void> => Effect.gen(function* () {
      // The machine starts an attempt only after the previous one closed, so older entries hold no resources.
      for (const key of attempts.keys()) if (key < attempt) attempts.delete(key);
      const entry: Attempt = { scope: yield* Scope.make() };
      attempts.set(attempt, entry);
      entry.activity = yield* track(opening(attempt, entry));
    });

    const runPass = (attempt: number, pass: number): Effect.Effect<void, never, R> => {
      const entry = attempts.get(attempt)!;
      // Set once the pass committed freshness: from then on its payload is applied, whatever fails afterwards.
      let committed = false;
      const steps = Effect.gen(function* () {
        const live = entry.live!;
        const entries = yield* scanSource(options.sourcePath, options.includeSource);
        const { claimed: declared } = yield* dispatch({ tag: "passClaim", pass, changes: differences(baseline, entries) });
        // Only a stopped session refuses the claim; the pass then declares nothing.
        if (declared === undefined) return yield* Effect.interrupt;
        const plan = yield* options.declare(entries, declared);
        yield* live.submit(plan.work, declared);
        yield* live.awaitCompletion;
        yield* plan.publish;
        yield* live.commitFreshness;
        committed = true;
        baseline = entries;
        const outcome = (yield* live.status).state === "complete-with-errors" ? "complete-with-errors" as const : "complete" as const;
        const after = yield* scanSource(options.sourcePath, options.includeSource);
        return { outcome, changes: differences(entries, after) };
      });
      const body = Effect.gen(function* () {
        const scope = yield* Scope.fork(entry.passes!);
        const exit = yield* Effect.exit(steps.pipe(Scope.provide(scope)));
        // Only a drained scheduler proves that no worker still uses pass resources. Failed or stopped states may
        // still have workers awaiting cleanup, so defer release to attempt close; otherwise release the pass now.
        const status = yield* entry.live!.status;
        if (status.state !== "complete" && status.state !== "complete-with-errors") return yield* exit;
        const closed = yield* Effect.exit(Scope.close(scope, exit));
        if (Exit.isSuccess(closed)) return yield* exit;
        return yield* Effect.failCause(Exit.isFailure(exit) ? Cause.combine(exit.cause, closed.cause) : closed.cause);
      });
      return activity(
        body,
        (exit) => Exit.isSuccess(exit)
          ? Effect.succeed({ tag: "passOk", pass, ...exit.value })
          // Typed failures keep the attempt; a defect, an interruption or a failed scheduler ends it.
          : Effect.gen(function* () {
            const schedulerFailed = entry.live !== undefined && (yield* entry.live.status).state === "failed";
            const endsAttempt = exit.cause.reasons.some((reason) => reason._tag !== "Fail") || schedulerFailed;
            return { tag: "passFail", pass, cause: exit.cause, endsAttempt, discharged: committed } as const;
          }),
        (cause) => ({ tag: "passFail", pass, cause: cause as Cause.Cause<Failure>, endsAttempt: true, discharged: committed }),
      );
    };

    const startPass = (attempt: number, pass: number): Effect.Effect<void> => Effect.gen(function* () {
      const entry = attempts.get(attempt)!;
      entry.activity = yield* track(runPass(attempt, pass));
    });

    const closeAttempt = (attempt: number, cause: Cause.Cause<Failure>): Effect.Effect<void, never, R> => {
      const body = Effect.gen(function* () {
        const entry = attempts.get(attempt)!;
        // The activity reported this failure as its last act; wait for it so nothing runs once the lease is released.
        if (entry.activity !== undefined) yield* Fiber.await(entry.activity);
        if (internal.beforeAttemptClose) yield* internal.beforeAttemptClose(attempt);
        // Finalizers finish even if the session stops meanwhile: a half-closed scope could keep the lease.
        yield* Scope.close(entry.scope, Exit.failCause(cause)).pipe(Effect.uninterruptible);
        if (internal.afterAttemptClose) yield* internal.afterAttemptClose(attempt);
      });
      // A failing finalizer, such as a rejected lease release, still closes the attempt; its cause joins the attempt's.
      const closed = (failure: Cause.Cause<unknown> | null): Event<Cause.Cause<Failure>> =>
        failure === null ? { tag: "attemptClosed", attempt } : { tag: "attemptClosed", attempt, cause: Cause.combine(cause, failure as Cause.Cause<Failure>) };
      return activity(body, (exit) => Effect.succeed(closed(Exit.isSuccess(exit) ? null : exit.cause)), (failure) => closed(failure));
    };

    const request = (payload: Payload): Effect.Effect<PassAdmission> => Effect.gen(function* () {
      // Invalidate before the admission step: a commit after admission then cannot record a read older than the hint.
      const target = invalidationTarget(state);
      const live = target === null ? undefined : attempts.get(target)?.live;
      let appliedTo: number | null = null;
      if (live !== undefined && Payload.invalidates(payload)) {
        const exit = yield* Effect.exit(live.submit([], { force: payload.force, changedPaths: payload.paths }));
        if (Exit.isSuccess(exit)) appliedTo = target;
      }
      yield* (internal.afterRequestInvalidation ?? Effect.void);
      const { admission } = yield* dispatch({ tag: "request", payload, appliedTo });
      return admission!;
    });

    // Finalizers run in reverse: admission stops first, then activities are joined, then attempt scopes release leases.
    // `Scope.close` is idempotent, so a scope its failed attempt already closed is skipped.
    yield* Effect.addFinalizer(() => Effect.forEach([...attempts.values()], (entry) => Scope.close(entry.scope, Exit.void), { discard: true }));
    yield* Effect.addFinalizer(() => Fiber.interruptAll([...fibers]));
    if ((options.reconcileIntervalMs ?? 0) > 0) {
      yield* Effect.forkScoped(Effect.forever(Effect.sleep(options.reconcileIntervalMs!).pipe(Effect.andThen(report({ tag: "tick" })))));
    }
    yield* Effect.addFinalizer(() => report({ tag: "stop" }));
    yield* execute(first.commands);

    return {
      requestPass: (input = {}) => request(Payload.of("resync", input.force ?? false, [])),
      notify: (changedPaths) => request(Payload.of("watcher", false, changedPaths)),
      awaitCompletion: Effect.suspend(() => Deferred.await(completionFor(state.ids.gen))),
      ready: Effect.suspend(() => Deferred.await(ready)),
      status: Effect.suspend(() => {
        const current = view(state);
        const live = current.work.attempt === null ? undefined : attempts.get(current.work.attempt)?.live;
        const work: Effect.Effect<WorkStatus<W>> = live !== undefined ? live.status : Effect.succeed({ state: current.work.fallback, pending: 0, active: null, errors: [] });
        return work.pipe(Effect.map((work) => ({ state: current.state, pass: current.pass, followUp: current.followUp, failure: current.failure, availability: current.availability, work })));
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
