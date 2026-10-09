/**
 * The live session as one immutable value and one pure transition function. No Effect runtime and no I/O live here:
 * `step` returns the next state and the commands an interpreter must execute, and outcomes come back as events.
 */

export interface PassRequest {
  readonly kind: "initial" | "resync" | "watcher" | "reconcile";
  readonly force: boolean;
  /** Relative source paths; hints bypass source metadata equality. */
  readonly changedPaths: readonly string[];
}

export type PassAdmission = "started" | "queued" | "rejected";

/** `prior-output`: usable output existed before the session. `minimum-publication`: this session published usable output. */
export type Availability = "prior-output" | "minimum-publication";

export const INITIAL: PassRequest = { kind: "initial", force: false, changedPaths: [] };

export type FollowUpKind = Exclude<PassRequest["kind"], "initial">;

/** Accepted request data. `kind` is null only in `Payload.empty`, the value that holds no request. */
export interface Payload {
  readonly kind: FollowUpKind | null;
  readonly force: boolean;
  readonly paths: readonly string[];
}

const empty: Payload = { kind: null, force: false, paths: [] };

export const Payload = {
  empty,
  of: (kind: FollowUpKind, force: boolean, paths: readonly string[]): Payload => ({ kind, force, paths: [...new Set(paths)] }),
  /** A monoid with `empty` as identity: force is kept, paths are united in order, and "resync" outranks the latest kind. */
  combine: (left: Payload, right: Payload): Payload => ({
    kind: left.kind === "resync" || right.kind === "resync" ? "resync" : right.kind ?? left.kind,
    force: left.force || right.force,
    paths: [...new Set([...left.paths, ...right.paths])],
  }),
  invalidates: (payload: Payload) => payload.force || payload.paths.length > 0,
  request: (payload: Payload): PassRequest => {
    if (payload.kind === null) throw new Error("An empty payload has no pass request");
    return { kind: payload.kind, force: payload.force, changedPaths: payload.paths };
  },
};

export type Outcome<C> = { readonly tag: "complete" } | { readonly tag: "complete-with-errors" } | { readonly tag: "failed"; readonly cause: C };

/** `carried`: due from before this attempt. `arrived`: a request was admitted during this attempt. */
export type Due = "none" | "carried" | "arrived";

/**
 * Busy phases (`opening`, `passing`, `closing`) own the outstanding completion generation `ids.gen`.
 * A pass is unclaimed (`declared: null`) until it claims its request for `declare`; requests until then join it.
 */
export type Phase<C> =
  | { readonly tag: "opening"; readonly attempt: number; readonly freshness: "pending" | "ready"; readonly due: Due }
  | { readonly tag: "passing"; readonly attempt: number; readonly pass: number; readonly payload: Payload; readonly declared: PassRequest | null; readonly due: "none" | "arrived" }
  | { readonly tag: "running"; readonly attempt: number; readonly last: Outcome<C> }
  | { readonly tag: "closing"; readonly attempt: number; readonly cause: C; readonly due: Due }
  | { readonly tag: "waiting"; readonly cause: C };

/** Last allocated identities. `gen` is always the current completion generation. */
export interface Ids {
  readonly attempt: number;
  readonly pass: number;
  readonly gen: number;
}

interface Shared<C> {
  readonly failure: C | null;
  readonly availability: Availability | null;
  readonly recovery: boolean;
  readonly ids: Ids;
}

export type Session<C> =
  | Shared<C> & { readonly tag: "live"; readonly phase: Phase<C>; readonly owed: Payload; readonly ready: "pending" | "settled" }
  /** A fatal failure: admission is closed and the attempt scope is closing before waiters learn the cause. */
  | Shared<C> & { readonly tag: "halting"; readonly attempt: number; readonly cause: C; readonly ready: "pending" | "settled" }
  | Shared<C> & { readonly tag: "stopped"; readonly attempt: number | null };

export type Event<C> =
  | { readonly tag: "request"; readonly payload: Payload; readonly appliedTo: number | null }
  | { readonly tag: "tick" }
  | { readonly tag: "stop" }
  | { readonly tag: "usable"; readonly attempt: number }
  | { readonly tag: "freshnessReady"; readonly attempt: number }
  | { readonly tag: "openOk"; readonly attempt: number; readonly changes: readonly string[] }
  | { readonly tag: "openFail"; readonly attempt: number; readonly cause: C }
  | { readonly tag: "passClaim"; readonly pass: number; readonly changes: readonly string[] }
  | { readonly tag: "passOk"; readonly pass: number; readonly outcome: "complete" | "complete-with-errors"; readonly changes: readonly string[] }
  | { readonly tag: "passFail"; readonly pass: number; readonly cause: C; readonly endsAttempt: boolean }
  | { readonly tag: "attemptClosed"; readonly attempt: number };

export type Settlement<C> = { readonly tag: "success" } | { readonly tag: "failure"; readonly cause: C } | { readonly tag: "interrupt" };

export type Command<C> =
  | { readonly tag: "startAttempt"; readonly attempt: number }
  | { readonly tag: "startPass"; readonly attempt: number; readonly pass: number }
  | { readonly tag: "invalidate"; readonly attempt: number; readonly force: boolean; readonly paths: readonly string[] }
  | { readonly tag: "closeAttempt"; readonly attempt: number; readonly cause: C }
  | { readonly tag: "settleCompletion"; readonly gen: number; readonly exit: Settlement<C> }
  | { readonly tag: "settleReady"; readonly exit: Settlement<C> };

export interface Step<C> {
  readonly state: Session<C>;
  readonly commands: readonly Command<C>[];
  /** Set only for `request` events. */
  readonly admission: PassAdmission | null;
  /** Set only for a `passClaim` of the current pass: the request its `declare` receives. */
  readonly claimed?: PassRequest;
}

type Live<C> = Extract<Session<C>, { readonly tag: "live" }>;

export function init<C>(config: { readonly recovery: boolean; readonly availability: Availability | null }): Step<C> {
  return {
    state: {
      tag: "live",
      phase: { tag: "opening", attempt: 1, freshness: "pending", due: "none" },
      owed: empty,
      ready: "pending",
      failure: null,
      availability: config.availability,
      recovery: config.recovery,
      ids: { attempt: 1, pass: 0, gen: 1 },
    },
    commands: [{ tag: "startAttempt", attempt: 1 }],
    admission: null,
  };
}

/** The attempt whose open freshness takes invalidation now. A closing attempt is excluded: its freshness is closing. */
export function invalidationTarget<C>(state: Session<C>): number | null {
  if (state.tag !== "live") return null;
  const phase = state.phase;
  if (phase.tag === "opening") return phase.freshness === "ready" ? phase.attempt : null;
  return phase.tag === "passing" || phase.tag === "running" ? phase.attempt : null;
}

export interface LiveView<C> {
  readonly state: "working" | "complete" | "complete-with-errors" | "failed" | "stopped";
  readonly pass: PassRequest | null;
  readonly followUp: PassRequest | null;
  readonly failure: C | null;
  readonly availability: Availability | null;
  /** Read work status from this attempt's scheduler when it has one, else report `fallback`. */
  readonly work: { readonly attempt: number | null; readonly fallback: "working" | "failed" | "stopped" };
}

export function view<C>(state: Session<C>): LiveView<C> {
  const shared = { failure: state.failure, availability: state.availability };
  if (state.tag !== "live") return { ...shared, state: "stopped", pass: null, followUp: null, work: { attempt: state.attempt, fallback: "stopped" } };
  const phase = state.phase;
  const owed = (due: boolean) => due ? Payload.request(state.owed) : null;
  switch (phase.tag) {
    case "opening":
      return { ...shared, state: "working", pass: INITIAL, followUp: owed(phase.due !== "none"), work: { attempt: null, fallback: "working" } };
    case "passing":
      return { ...shared, state: "working", pass: phase.declared ?? Payload.request(phase.payload), followUp: owed(phase.due === "arrived"), work: { attempt: phase.attempt, fallback: "working" } };
    case "running":
      return { ...shared, state: phase.last.tag, pass: null, followUp: null, work: { attempt: phase.attempt, fallback: "failed" } };
    case "closing":
      return { ...shared, state: "working", pass: null, followUp: owed(phase.due !== "none"), work: { attempt: phase.attempt, fallback: "failed" } };
    case "waiting":
      return { ...shared, state: "failed", pass: null, followUp: null, work: { attempt: null, fallback: "failed" } };
  }
}

const unchanged = <C>(state: Session<C>, admission: PassAdmission | null = null): Step<C> => ({ state, commands: [], admission });

const outstanding = <C>(phase: Phase<C>) => phase.tag === "opening" || phase.tag === "passing" || phase.tag === "closing";

const fatal = <C>(state: Live<C>) => !state.recovery || state.availability === null;

const watcher = (changes: readonly string[]) => Payload.of("watcher", false, changes);

const readyCommands = <C>(state: Live<C>): Command<C>[] => state.ready === "pending" ? [{ tag: "settleReady", exit: { tag: "success" } }] : [];

/** Moves all of `owed` into a new pass of the current generation. */
function startPass<C>(state: Live<C>, attempt: number, owed: Payload, gen = state.ids.gen): { state: Live<C>; command: Command<C> } {
  const pass = state.ids.pass + 1;
  return {
    state: { ...state, phase: { tag: "passing", attempt, pass, payload: owed, declared: null, due: "none" }, owed: empty, ids: { ...state.ids, pass, gen } },
    command: { tag: "startPass", attempt, pass },
  };
}

function admit<C>(state: Live<C>, payload: Payload, appliedTo: number | null): Step<C> {
  const owed = Payload.combine(state.owed, payload);
  const target = invalidationTarget(state);
  // The interpreter invalidated on `appliedTo` before this step; the target may have changed during that gap.
  const retarget: Command<C>[] = target !== null && target !== appliedTo && Payload.invalidates(payload)
    ? [{ tag: "invalidate", attempt: target, force: payload.force, paths: payload.paths }]
    : [];
  const phase = state.phase;
  switch (phase.tag) {
    case "passing":
      if (phase.declared === null) {
        return { state: { ...state, phase: { ...phase, payload: Payload.combine(phase.payload, payload) } }, commands: retarget, admission: "queued" };
      }
      return { state: { ...state, phase: { ...phase, due: "arrived" }, owed }, commands: retarget, admission: "queued" };
    case "opening":
    case "closing":
      return { state: { ...state, phase: { ...phase, due: "arrived" }, owed }, commands: retarget, admission: "queued" };
    case "running": {
      const next = startPass(state, phase.attempt, owed, state.ids.gen + 1);
      return { state: next.state, commands: [...retarget, next.command], admission: "started" };
    }
    case "waiting": {
      const attempt = state.ids.attempt + 1;
      return {
        state: { ...state, phase: { tag: "opening", attempt, freshness: "pending", due: "carried" }, owed, ids: { ...state.ids, attempt, gen: state.ids.gen + 1 } },
        commands: [{ tag: "startAttempt", attempt }],
        admission: "started",
      };
    }
  }
}

function stop<C>(state: Exclude<Session<C>, { readonly tag: "stopped" }>): Step<C> {
  const busy = state.tag === "halting" || outstanding(state.phase);
  const attempt = state.tag === "halting" ? state.attempt : state.phase.tag === "waiting" ? null : state.phase.attempt;
  const commands: Command<C>[] = [];
  if (busy) commands.push({ tag: "settleCompletion", gen: state.ids.gen, exit: { tag: "interrupt" } });
  if (state.ready === "pending") commands.push({ tag: "settleReady", exit: { tag: "interrupt" } });
  return { state: { tag: "stopped", attempt, failure: state.failure, availability: state.availability, recovery: state.recovery, ids: state.ids }, commands, admission: null };
}

/** Ends attempt `attempt` with `cause`, recoverably or fatally. `owed` must already hold every undischarged payload. */
function endAttempt<C>(state: Live<C>, attempt: number, cause: C, due: Due, owed: Payload): Step<C> {
  const command: Command<C> = { tag: "closeAttempt", attempt, cause };
  if (fatal(state)) {
    const { phase: _phase, owed: _owed, ...rest } = state;
    return { state: { ...rest, tag: "halting", attempt, cause, failure: cause }, commands: [command], admission: null };
  }
  return { state: { ...state, phase: { tag: "closing", attempt, cause, due }, owed, failure: cause }, commands: [command], admission: null };
}

export function step<C>(state: Session<C>, event: Event<C>): Step<C> {
  if (state.tag === "stopped") return unchanged(state, event.tag === "request" ? "rejected" : null);
  if (event.tag === "stop") return stop(state);
  if (state.tag === "halting") {
    if (event.tag === "request") return unchanged(state, "rejected");
    if (event.tag !== "attemptClosed" || event.attempt !== state.attempt) return unchanged(state);
    const failure: Settlement<C> = { tag: "failure", cause: state.cause };
    const commands: Command<C>[] = [{ tag: "settleCompletion", gen: state.ids.gen, exit: failure }];
    if (state.ready === "pending") commands.push({ tag: "settleReady", exit: failure });
    return { state: { tag: "stopped", attempt: state.attempt, failure: state.failure, availability: state.availability, recovery: state.recovery, ids: state.ids }, commands, admission: null };
  }

  const phase = state.phase;
  switch (event.tag) {
    case "request":
      return admit(state, event.payload, event.appliedTo);
    case "tick":
      // A reconcile tick carries no payload. While anything runs, the post-pass traversal already covers it.
      return phase.tag === "running" || phase.tag === "waiting" ? { ...admit(state, Payload.of("reconcile", false, []), null), admission: null } : unchanged(state);
    case "usable":
      if (phase.tag !== "opening" || phase.attempt !== event.attempt) return unchanged(state);
      return unchanged({ ...state, availability: state.availability ?? "minimum-publication" });
    case "freshnessReady": {
      if (phase.tag !== "opening" || phase.attempt !== event.attempt || phase.freshness === "ready") return unchanged(state);
      // Every undischarged payload, not only requests of this opening, must reach freshness before the opening commit.
      const commands: Command<C>[] = Payload.invalidates(state.owed) ? [{ tag: "invalidate", attempt: phase.attempt, force: state.owed.force, paths: state.owed.paths }] : [];
      return { state: { ...state, phase: { ...phase, freshness: "ready" }, availability: state.availability ?? "minimum-publication" }, commands, admission: null };
    }
    case "openOk": {
      if (phase.tag !== "opening" || phase.attempt !== event.attempt) return unchanged(state);
      const owed = event.changes.length > 0 ? Payload.combine(state.owed, watcher(event.changes)) : state.owed;
      const opened: Live<C> = { ...state, failure: null, ready: "settled" };
      if (phase.due !== "none" || event.changes.length > 0) {
        const next = startPass(opened, phase.attempt, owed);
        return { state: next.state, commands: [next.command, ...readyCommands(state)], admission: null };
      }
      return {
        state: { ...opened, phase: { tag: "running", attempt: phase.attempt, last: { tag: "complete" } }, owed },
        commands: [...readyCommands(state), { tag: "settleCompletion", gen: state.ids.gen, exit: { tag: "success" } }],
        admission: null,
      };
    }
    case "openFail":
      if (phase.tag !== "opening" || phase.attempt !== event.attempt) return unchanged(state);
      return endAttempt(state, phase.attempt, event.cause, phase.due, state.owed);
    case "passClaim": {
      if (phase.tag !== "passing" || phase.pass !== event.pass || phase.declared !== null) return unchanged(state);
      // Scan differences reach `declare` but stay out of the payload: the baseline still holds them if the pass fails.
      const request = Payload.request(phase.payload);
      const declared = { ...request, changedPaths: [...new Set([...request.changedPaths, ...event.changes])] };
      return { state: { ...state, phase: { ...phase, declared } }, commands: [], admission: null, claimed: declared };
    }
    case "passOk": {
      if (phase.tag !== "passing" || phase.pass !== event.pass) return unchanged(state);
      const owed = event.changes.length > 0 ? Payload.combine(state.owed, watcher(event.changes)) : state.owed;
      const passed: Live<C> = { ...state, failure: null };
      if (phase.due === "arrived" || event.changes.length > 0) {
        const next = startPass(passed, phase.attempt, owed);
        return { state: next.state, commands: [next.command], admission: null };
      }
      return {
        state: { ...passed, phase: { tag: "running", attempt: phase.attempt, last: { tag: event.outcome } }, owed },
        commands: [{ tag: "settleCompletion", gen: state.ids.gen, exit: { tag: "success" } }],
        admission: null,
      };
    }
    case "passFail": {
      if (phase.tag !== "passing" || phase.pass !== event.pass) return unchanged(state);
      // The failed pass is older than anything admitted during it.
      const owed = Payload.combine(phase.payload, state.owed);
      if (event.endsAttempt) return endAttempt(state, phase.attempt, event.cause, phase.due, owed);
      const failed: Live<C> = { ...state, failure: event.cause };
      if (phase.due === "arrived") {
        const next = startPass(failed, phase.attempt, owed);
        return { state: next.state, commands: [next.command], admission: null };
      }
      return {
        state: { ...failed, phase: { tag: "running", attempt: phase.attempt, last: { tag: "failed", cause: event.cause } }, owed },
        commands: [{ tag: "settleCompletion", gen: state.ids.gen, exit: { tag: "failure", cause: event.cause } }],
        admission: null,
      };
    }
    case "attemptClosed": {
      if (phase.tag !== "closing" || phase.attempt !== event.attempt) return unchanged(state);
      const closed: Live<C> = { ...state, ready: "settled" };
      // One rule replaces the one-shot retry: only a request admitted during the failed attempt earns another attempt.
      if (phase.due === "arrived") {
        const attempt = state.ids.attempt + 1;
        return {
          state: { ...closed, phase: { tag: "opening", attempt, freshness: "pending", due: "carried" }, ids: { ...state.ids, attempt } },
          commands: [{ tag: "startAttempt", attempt }, ...readyCommands(state)],
          admission: null,
        };
      }
      return {
        state: { ...closed, phase: { tag: "waiting", cause: phase.cause } },
        commands: [...readyCommands(state), { tag: "settleCompletion", gen: state.ids.gen, exit: { tag: "failure", cause: phase.cause } }],
        admission: null,
      };
    }
  }
}
