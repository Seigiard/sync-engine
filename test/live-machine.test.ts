import { expect, test } from "bun:test";
import { init, invalidationTarget, Payload, step, view, type Event, type PassRequest, type Session } from "../src/live-machine.ts";

/*
 * Bounded-exhaustive model check of the pure live session reducer.
 *
 * The harness plays the interpreter: it sends every enabled event and executes emitted commands. Its oracle is ghost
 * state built from requests it admitted, events it sent and commands it observed. The reducer's phase is read only to
 * name the step being checked and to locate the holders whose contents are compared with the ghost. After every step
 * it checks the invariants of the redesign; a violation names its invariant id in brackets, for example `[P1]`.
 */

type C = string;

type Move =
  | "notify" | "force" | "plain" | "tick" | "stop" | "gapNotify" | "gapForce" | "gapEnd"
  | "usable" | "freshnessReady" | "openOk" | "openOkChange" | "openFail"
  | "claim" | "claimChange" | "passOk" | "passOkErrors" | "passOkChange" | "passFailTyped" | "passFailEnds" | "passFailAfterCommit"
  | "attemptClosed" | "attemptClosedFailing";

interface Req {
  readonly token: string | null;
  readonly force: boolean;
  readonly gen: number;
  readonly attempt: number;
  /** Admitted while an attempt was busy after its trigger: it guarantees one more run. */
  readonly arrival: boolean;
  /** In the current pass payload (merged before its claim, or taken by `startPass`). */
  inPass: boolean;
  /** Attempts whose freshness received this request's invalidation. */
  applied: number[];
}

type Activity =
  | { readonly kind: "opening"; readonly attempt: number; usable: boolean; fresh: boolean }
  | { readonly kind: "pass"; readonly attempt: number; readonly pass: number; claimed: PassRequest | null };

interface Ghost {
  readonly recovery: boolean;
  state: Session<C>;
  held: Req[];
  activity: Activity | null;
  closing: number | null;
  openAttempt: number | null;
  opened: boolean;
  freshOpen: number | null;
  arrivals: number;
  triggered: boolean;
  currentSettled: boolean;
  /** Output is usable: prior output, or an accepted `usable`/`freshnessReady` event. */
  usable: boolean;
  readySettled: number;
  firstOutcome: boolean;
  stopped: boolean;
  gap: { readonly payload: Payload; readonly token: string; readonly target: number | null; readonly beforeStop: boolean } | null;
  attemptsInGen: number;
  nextToken: number;
}

interface Config {
  readonly name: string;
  readonly recovery: boolean;
  readonly prior: boolean;
}

const configs: readonly Config[] = [
  { name: "recovery off", recovery: false, prior: false },
  { name: "recovery with prior output", recovery: true, prior: true },
  { name: "recovery without prior output", recovery: true, prior: false },
];

const CAUSE = { open: "open failed", typed: "typed pass failure", defect: "pass defect", trailing: "trailing step failed", close: "close failed" } as const;

function start(config: Config): Ghost {
  const first = init<C>({ recovery: config.recovery, availability: config.prior ? "prior-output" : null });
  const ghost: Ghost = {
    recovery: config.recovery,
    state: first.state,
    held: [],
    activity: null,
    closing: null,
    openAttempt: null,
    opened: false,
    freshOpen: null,
    arrivals: 0,
    triggered: false,
    currentSettled: false,
    usable: config.prior,
    readySettled: 0,
    firstOutcome: false,
    stopped: false,
    gap: null,
    attemptsInGen: 0,
    nextToken: 0,
  };
  if (first.commands.length !== 1 || first.commands[0]!.tag !== "startAttempt") throw new Error("init must start exactly one attempt");
  ghost.openAttempt = 1;
  ghost.activity = { kind: "opening", attempt: 1, usable: false, fresh: false };
  ghost.attemptsInGen = 1;
  return ghost;
}

function clone(ghost: Ghost): Ghost {
  return {
    ...ghost,
    held: ghost.held.map((req) => ({ ...req, applied: [...req.applied] })),
    activity: ghost.activity === null ? null : { ...ghost.activity },
  };
}

function enabled(ghost: Ghost): Move[] {
  const moves: Move[] = [];
  if (ghost.stopped) moves.push("plain");
  else {
    moves.push("notify", "force", "plain", "tick", "stop");
    moves.push(...(ghost.gap === null ? ["gapNotify", "gapForce"] as const : ["gapEnd"] as const));
  }
  if (ghost.stopped && ghost.gap !== null) moves.push("gapEnd");
  const activity = ghost.activity;
  if (activity?.kind === "opening") {
    if (!ghost.stopped) {
      if (!activity.usable && !activity.fresh) moves.push("usable");
      if (!activity.fresh) moves.push("freshnessReady");
      else moves.push("openOk", "openOkChange");
    }
    moves.push("openFail");
  }
  if (activity?.kind === "pass") {
    if (!ghost.stopped) {
      if (activity.claimed === null) moves.push("claim", "claimChange", "passFailTyped");
      else moves.push("passOk", "passOkErrors", "passOkChange", "passFailTyped", "passFailAfterCommit");
    }
    moves.push("passFailEnds");
  }
  if (ghost.closing !== null) moves.push("attemptClosed", "attemptClosedFailing");
  return moves;
}

const invalidating = (req: Req) => req.force || req.token !== null;

const phaseTag = (state: Session<C>) => state.tag === "live" ? state.phase.tag : state.tag;

const busy = (state: Session<C>) => state.tag === "halting" || (state.tag === "live" && (state.phase.tag === "opening" || state.phase.tag === "passing" || state.phase.tag === "closing"));

interface Applied {
  readonly ghost: Ghost;
  readonly violations: readonly string[];
  readonly pair: string;
  readonly goals: readonly string[];
}

/** Executes one harness move: a reducer step plus the commands it emitted, with every invariant checked. */
function apply(source: Ghost, move: Move): Applied {
  const ghost = clone(source);
  const violations: string[] = [];
  const goals: string[] = [];
  const check = (ok: boolean, id: string, message: string) => { if (!ok) violations.push(`[${id}] ${message}`); };
  const pre = ghost.state;
  const fresh = () => `t${ghost.nextToken++}`;
  const pair = `${phaseTag(pre)} × ${move}`;

  // The interpreter invalidates on `invalidationTarget` before dispatch; a target must have open freshness.
  const invalidateBefore = (payload: Payload): number | null => {
    if (!Payload.invalidates(payload)) return null;
    const target = invalidationTarget(pre);
    if (target === null) return null;
    check(ghost.freshOpen === target && ghost.closing !== target, "F2", `admission invalidated attempt ${target} without open freshness`);
    return target;
  };

  let event: Event<C>;
  let admitted: { readonly token: string | null; readonly force: boolean; readonly applied: number[] } | null = null;
  switch (move) {
    case "notify":
    case "force":
    case "plain": {
      const token = move === "plain" ? null : fresh();
      const payload = move === "force" ? Payload.of("resync", true, [token!]) : move === "notify" ? Payload.of("watcher", false, [token!]) : Payload.of("resync", false, []);
      const appliedTo = invalidateBefore(payload);
      event = { tag: "request", payload, appliedTo };
      admitted = { token, force: payload.force, applied: appliedTo === null ? [] : [appliedTo] };
      break;
    }
    case "gapNotify":
    case "gapForce": {
      const token = fresh();
      const payload = move === "gapForce" ? Payload.of("resync", true, [token]) : Payload.of("watcher", false, [token]);
      ghost.gap = { payload, token, target: invalidateBefore(payload), beforeStop: !ghost.stopped };
      return { ghost, violations, pair, goals };
    }
    case "gapEnd": {
      const gap = ghost.gap!;
      ghost.gap = null;
      if (gap.beforeStop && ghost.stopped) goals.push("gap spans stop");
      const target = invalidationTarget(pre);
      if (gap.target !== null && target !== null && target !== gap.target) goals.push("gap spans an attempt change");
      event = { tag: "request", payload: gap.payload, appliedTo: gap.target };
      admitted = { token: gap.token, force: gap.payload.force, applied: gap.target === null ? [] : [gap.target] };
      break;
    }
    case "tick":
      event = { tag: "tick" };
      break;
    case "stop":
      event = { tag: "stop" };
      break;
    case "usable":
    case "freshnessReady":
      event = { tag: move, attempt: ghost.activity!.attempt };
      break;
    case "openOk":
    case "openOkChange":
      event = { tag: "openOk", attempt: ghost.activity!.attempt, changes: move === "openOkChange" ? [fresh()] : [] };
      break;
    case "openFail":
      event = { tag: "openFail", attempt: ghost.activity!.attempt, cause: CAUSE.open };
      break;
    case "claim":
    case "claimChange": {
      const activity = ghost.activity as Extract<Activity, { kind: "pass" }>;
      event = { tag: "passClaim", pass: activity.pass, changes: move === "claimChange" ? [fresh()] : [] };
      break;
    }
    case "passOk":
    case "passOkErrors":
    case "passOkChange": {
      const activity = ghost.activity as Extract<Activity, { kind: "pass" }>;
      event = { tag: "passOk", pass: activity.pass, outcome: move === "passOkErrors" ? "complete-with-errors" : "complete", changes: move === "passOkChange" ? [fresh()] : [] };
      break;
    }
    case "passFailTyped":
    case "passFailEnds":
    case "passFailAfterCommit": {
      const activity = ghost.activity as Extract<Activity, { kind: "pass" }>;
      const ends = move === "passFailEnds";
      const afterCommit = move === "passFailAfterCommit";
      event = { tag: "passFail", pass: activity.pass, cause: ends ? CAUSE.defect : afterCommit ? CAUSE.trailing : CAUSE.typed, endsAttempt: ends, discharged: afterCommit };
      break;
    }
    case "attemptClosed":
    case "attemptClosedFailing":
      event = move === "attemptClosed" ? { tag: "attemptClosed", attempt: ghost.closing! } : { tag: "attemptClosed", attempt: ghost.closing!, cause: CAUSE.close };
      break;
  }

  let result;
  try {
    result = step(pre, event);
  } catch (cause) {
    violations.push(`[S1] step threw: ${String(cause)}`);
    return { ghost, violations, pair, goals };
  }
  const post = result.state;
  ghost.state = post;
  const commands = result.commands;
  const has = (tag: string) => commands.some((command) => command.tag === tag);
  const live = pre.tag === "live";
  const prePhase = live ? pre.phase : null;

  // A1: closed admission and truthful answers.
  if (event.tag === "request") {
    if (!live) check(result.admission === "rejected" && commands.length === 0 && post === pre, "A1", `admission after stop/fatal answered ${result.admission}`);
    else {
      const starts = prePhase!.tag === "running" || prePhase!.tag === "waiting";
      check(result.admission === (starts ? "started" : "queued"), "A1", `admission in ${prePhase!.tag} answered ${result.admission}`);
    }
  }
  if (ghost.stopped) check(!has("startAttempt") && !has("startPass") && !has("invalidate"), "A1", "a command started work after stop");

  // Activity outcomes the reducer accepted (the harness only sends events of the activity in flight).
  const accepted = live && !(ghost.stopped);
  const activity = ghost.activity;
  const commitChecks = (attempt: number) => {
    for (const req of ghost.held) if (invalidating(req)) check(req.applied.includes(attempt), "F1", `commit of attempt ${attempt} before ${req.token ?? "a forced request"} reached its freshness`);
  };
  let mustStartPass: boolean | null = null;
  let changeReq: string | null = null;
  switch (event.tag) {
    case "usable":
      if (activity?.kind === "opening") activity.usable = true;
      if (accepted) ghost.usable = true;
      break;
    case "freshnessReady":
      if (activity?.kind === "opening") activity.fresh = true;
      ghost.freshOpen = event.attempt;
      if (accepted) ghost.usable = true;
      break;
    case "openOk":
      if (!accepted) break;
      ghost.activity = null;
      ghost.opened = true;
      ghost.firstOutcome = true;
      commitChecks(event.attempt);
      if (event.changes.length > 0) changeReq = event.changes[0]!;
      mustStartPass = ghost.arrivals > 0 || ghost.triggered || event.changes.length > 0;
      break;
    case "openFail":
      ghost.activity = null;
      break;
    case "passClaim": {
      if (!accepted) break;
      const pass = activity as Extract<Activity, { kind: "pass" }>;
      check(result.claimed !== undefined, "S1", "the current pass could not claim its request");
      const claimed = result.claimed;
      if (claimed === undefined) break;
      pass.claimed = claimed;
      for (const req of ghost.held) {
        const inClaim = req.token === null ? true : claimed.changedPaths.includes(req.token);
        if (req.inPass) {
          check(inClaim, "P1", `claim of pass ${pass.pass} lost ${req.token}`);
          check(!req.force || claimed.force, "P1", `claim of pass ${pass.pass} lost force of ${req.token}`);
          // The pass submit applies the claim: force invalidates everything, a hint only its own path.
          if (claimed.force || (!req.force && inClaim && req.token !== null)) req.applied.push(pass.attempt);
        } else if (req.token !== null) check(!inClaim, "P2", `claim of pass ${pass.pass} repeats ${req.token} still owed`);
      }
      break;
    }
    case "passOk": {
      if (!accepted) break;
      const pass = activity as Extract<Activity, { kind: "pass" }>;
      ghost.activity = null;
      commitChecks(pass.attempt);
      const arrivedAfterClaim = ghost.held.some((req) => !req.inPass);
      for (const req of ghost.held) if (req.inPass && req.attempt < pass.attempt) goals.push("retained payload discharged by a later attempt");
      ghost.held = ghost.held.filter((req) => !req.inPass);
      if (event.changes.length > 0) changeReq = event.changes[0]!;
      mustStartPass = arrivedAfterClaim || event.changes.length > 0;
      break;
    }
    case "passFail": {
      if (!accepted) { ghost.activity = null; break; }
      const pass = activity as Extract<Activity, { kind: "pass" }>;
      ghost.activity = null;
      if (event.discharged) {
        // The pass committed before the failure: its payload was applied and is discharged.
        commitChecks(pass.attempt);
        ghost.held = ghost.held.filter((req) => !req.inPass);
      }
      for (const req of ghost.held) req.inPass = false;
      if (!event.endsAttempt) mustStartPass = ghost.held.some((req) => req.arrival);
      break;
    }
    case "attemptClosed":
      if (pre.tag === "live" && prePhase!.tag === "closing") {
        check(has("startAttempt") === ghost.arrivals > 0, ghost.arrivals > 0 ? "G1" : "H1", `attempt ${event.attempt} closed with ${ghost.arrivals} arrivals; startAttempt ${has("startAttempt")}`);
      }
      ghost.closing = null;
      ghost.openAttempt = null;
      ghost.opened = false;
      ghost.firstOutcome = true;
      break;
    case "stop":
      ghost.stopped = true;
      ghost.firstOutcome = true;
      break;
    default:
      break;
  }
  if (mustStartPass !== null) check(has("startPass") === mustStartPass, mustStartPass ? "G1" : "H1", `after ${event.tag} startPass ${has("startPass")}, expected ${mustStartPass}`);

  // S2 and V1 on accepted outcomes.
  if (accepted && (event.tag === "openFail" || event.tag === "passFail")) {
    check(post.failure === event.cause, "S2", `failure after ${event.tag} is ${post.failure}`);
    const fatal = !ghost.recovery || !source.usable;
    if (event.tag === "openFail" || event.endsAttempt) check((post.tag === "halting") === fatal, "V1", `fatal decision ${post.tag} with availability ${pre.availability}`);
  } else if (accepted && (event.tag === "openOk" || event.tag === "passOk")) {
    check(post.failure === null, "S2", `failure kept after ${event.tag}`);
  } else if (!(event.tag === "attemptClosed" && event.cause !== undefined && pre.tag !== "stopped")) check(post.failure === pre.failure, "S2", `failure changed by ${event.tag}`);
  if (pre.availability !== null) check(post.availability === pre.availability, "V1", "availability changed after it was set");
  check((post.availability !== null) === ghost.usable, "V1", `availability ${post.availability} while usable output is ${ghost.usable} after ${event.tag}`);
  if (accepted && event.tag === "attemptClosed" && event.cause !== undefined) check(post.failure === event.cause, "S2", "a failed close did not report its cause");

  // Admitted requests join the ghost after the step's own outcome bookkeeping.
  // A request admitted after stop is already an A1 violation; only live admissions join the ghost.
  const admittedRequest = live && admitted !== null && result.admission !== "rejected";
  const tickStarts = event.tag === "tick" && live && (prePhase!.tag === "running" || prePhase!.tag === "waiting");
  if (admittedRequest || tickStarts) {
    const passing = prePhase?.tag === "passing" ? ghost.activity as Extract<Activity, { kind: "pass" }> : null;
    const arrival = prePhase!.tag === "opening" || prePhase!.tag === "closing" || (passing !== null && passing.claimed !== null);
    const merged = passing !== null && passing.claimed === null;
    if (arrival) ghost.arrivals += 1;
    if (prePhase!.tag === "closing") goals.push("request admitted while closing");
    ghost.held.push({ token: admitted?.token ?? null, force: admitted?.force ?? false, gen: post.ids.gen, attempt: post.ids.attempt, arrival, inPass: merged, applied: admitted?.applied ?? [] });
    if (tickStarts || prePhase!.tag === "waiting") check(has("startAttempt") || has("startPass"), "G1", `${event.tag} in ${prePhase!.tag} started nothing`);
  }
  if (event.tag === "tick" && !tickStarts) check(post === pre && commands.length === 0, "H1", `tick in ${phaseTag(pre)} changed the session`);
  if (changeReq !== null) ghost.held.push({ token: changeReq, force: false, gen: post.ids.gen, attempt: post.ids.attempt, arrival: true, inPass: false, applied: [] });

  // A fatal failure or a stop discards every held request.
  if (pre.tag === "live" && post.tag !== "live") ghost.held = [];

  // C2: a new generation only after the previous one settled.
  if (post.ids.gen !== pre.ids.gen) {
    check(post.ids.gen === pre.ids.gen + 1 && ghost.currentSettled, "C2", `generation ${post.ids.gen} created while ${pre.ids.gen} was outstanding`);
    ghost.currentSettled = false;
    ghost.attemptsInGen = 0;
  }

  for (const command of commands) {
    switch (command.tag) {
      case "startAttempt":
        check(!ghost.stopped && ghost.openAttempt === null && ghost.closing === null && ghost.activity === null, "L1", `attempt ${command.attempt} started while another scope or activity existed`);
        check((event.tag === "request" || event.tag === "tick") ? prePhase?.tag === "waiting" : event.tag === "attemptClosed" && ghost.arrivals > 0, "H1", `attempt ${command.attempt} started by ${event.tag}`);
        ghost.openAttempt = command.attempt;
        ghost.opened = false;
        ghost.freshOpen = null;
        ghost.arrivals = 0;
        ghost.triggered = true;
        ghost.activity = { kind: "opening", attempt: command.attempt, usable: false, fresh: false };
        ghost.attemptsInGen += 1;
        if (ghost.attemptsInGen >= 3) goals.push("three attempts in one generation");
        break;
      case "startPass":
        check(!ghost.stopped && ghost.openAttempt === command.attempt && ghost.opened && ghost.activity === null && ghost.closing === null, "L1", `pass ${command.pass} started without an idle open attempt`);
        ghost.activity = { kind: "pass", attempt: command.attempt, pass: command.pass, claimed: null };
        ghost.arrivals = 0;
        ghost.held = ghost.held.map((req) => ({ ...req, inPass: true, arrival: false }));
        break;
      case "invalidate":
        check(!ghost.stopped && ghost.freshOpen === command.attempt && ghost.closing !== command.attempt, "F2", `invalidate on attempt ${command.attempt} without open freshness`);
        for (const req of ghost.held) {
          if (command.force || (!req.force && req.token !== null && command.paths.includes(req.token))) req.applied.push(command.attempt);
        }
        break;
      case "closeAttempt":
        check(ghost.openAttempt === command.attempt && ghost.closing === null && ghost.activity === null, "L1", `close of attempt ${command.attempt} while it was not the ended attempt`);
        ghost.closing = command.attempt;
        ghost.freshOpen = null;
        break;
      case "settleCompletion":
        check(command.gen === post.ids.gen && !ghost.currentSettled, "C1", `generation ${command.gen} settled twice or out of order`);
        if (command.exit.tag === "success") check(ghost.held.length === 0, "P3", `completion succeeded while ${ghost.held.map((req) => req.token ?? "a trigger").join(", ")} was held`);
        ghost.currentSettled = true;
        break;
      case "settleReady":
        check(ghost.readySettled === 0, "R1", "ready settled twice");
        if (command.exit.tag === "failure") check(event.tag === "attemptClosed", "R1", `ready failed on ${event.tag} before the lease was released`);
        ghost.readySettled += 1;
        break;
    }
  }

  // State invariants after the step.
  if (post.tag === "live") {
    const phase = post.phase;
    const payload = phase.tag === "passing" ? phase.payload : null;
    const known = new Set(ghost.held.map((req) => req.token).filter((token): token is string => token !== null));
    for (const token of [...post.owed.paths, ...(payload?.paths ?? [])]) check(known.has(token), "P2", `holder contains ${token}, which is discharged or unknown`);
    for (const req of ghost.held) {
      const holder = req.inPass ? payload : post.owed;
      check(holder !== null, "P1", `${req.token ?? "a trigger"} is marked in a pass that does not exist`);
      if (holder === null) continue;
      if (req.token !== null) {
        const count = (post.owed.paths.includes(req.token) ? 1 : 0) + (payload?.paths.includes(req.token) ? 1 : 0);
        check(count === 1 && holder.paths.includes(req.token), "P1", `${req.token} is in ${count} holders or the wrong one`);
      }
      check(holder.kind !== null, "P1", `${req.token ?? "a trigger"} sits in an empty holder`);
      if (req.force) check(holder.force, "P1", `force of ${req.token} was lost`);
    }
  }
  const outstanding = busy(post);
  check(ghost.currentSettled === !outstanding, "C2", `generation ${post.ids.gen} settled=${ghost.currentSettled} in ${phaseTag(post)}`);
  if (post.tag === "live") {
    const inFlight = ghost.activity !== null || ghost.closing !== null;
    check(outstanding === inFlight, "W1", `${phaseTag(post)} with activity in flight: ${inFlight}`);
  }
  if (post.tag === "halting") check(ghost.closing !== null && ghost.activity === null, "W1", "halting without a closing scope");
  check(ghost.readySettled === (ghost.firstOutcome ? 1 : 0), "R1", `ready settled ${ghost.readySettled} times; first outcome ${ghost.firstOutcome}`);
  try {
    const shown = view(post);
    check((shown.state === "working") === (post.tag === "live" && outstanding), "S1", `view state ${shown.state} in ${phaseTag(post)}`);
    check((shown.state === "stopped") === (post.tag !== "live"), "S1", `view state ${shown.state} in ${post.tag}`);
    if (shown.state === "stopped") check(shown.pass === null && shown.followUp === null, "S1", "stopped view shows a pass or follow-up");
    if (shown.followUp !== null || shown.pass !== null) check(shown.state === "working", "S1", "a pass or follow-up is shown outside working");
    if (post.tag === "live" && (post.phase.tag === "running" || post.phase.tag === "waiting")) check(shown.followUp === null, "W1", "an idle session shows a follow-up");
    if (post.tag === "live" && post.phase.tag === "closing" && shown.followUp !== null) check(ghost.arrivals > 0, "W1", "a closing attempt shows a follow-up that its close will not start");
  } catch (cause) {
    violations.push(`[S1] view threw: ${String(cause)}`);
  }
  return { ghost, violations, pair, goals };
}

function canonical(ghost: Ghost): string {
  const state = ghost.state;
  const names = new Map<string, string>();
  ghost.held.forEach((req, index) => { if (req.token !== null) names.set(req.token, `h${index}`); });
  const name = (token: string) => {
    let known = names.get(token);
    if (known === undefined) { known = `u${names.size}`; names.set(token, known); }
    return known;
  };
  const base = state.ids;
  const attempt = (value: number | null) => value === null ? null : value - base.attempt;
  const pass = (value: number) => value - base.pass;
  const payload = (value: Payload | null) => value === null ? null : [value.kind, value.force, value.paths.map(name)];
  const request = (value: PassRequest | null) => value === null ? null : [value.kind, value.force, value.changedPaths.map(name)];
  let session: unknown;
  if (state.tag === "live") {
    const phase = state.phase;
    const shape = phase.tag === "opening" ? [phase.tag, attempt(phase.attempt), phase.freshness, phase.due]
      : phase.tag === "passing" ? [phase.tag, attempt(phase.attempt), pass(phase.pass), payload(phase.payload), request(phase.declared), phase.due]
      : phase.tag === "running" ? [phase.tag, attempt(phase.attempt), phase.last]
      : phase.tag === "closing" ? [phase.tag, attempt(phase.attempt), phase.cause, phase.due]
      : [phase.tag, phase.cause];
    session = [state.tag, shape, payload(state.owed), state.ready, state.failure, state.availability];
  } else if (state.tag === "halting") session = [state.tag, attempt(state.attempt), state.cause, state.ready, state.failure, state.availability];
  else session = [state.tag, attempt(state.attempt), state.failure, state.availability];
  const current = ghost.openAttempt;
  const held = ghost.held.map((req) => [req.token === null ? null : name(req.token), req.force, req.gen - base.gen, attempt(req.attempt), req.arrival, req.inPass, current !== null && req.applied.includes(current)]);
  const activity = ghost.activity === null ? null : ghost.activity.kind === "opening"
    ? ["opening", attempt(ghost.activity.attempt), ghost.activity.usable, ghost.activity.fresh]
    : ["pass", attempt(ghost.activity.attempt), pass(ghost.activity.pass), request(ghost.activity.claimed)];
  const gap = ghost.gap === null ? null : [payload(ghost.gap.payload), name(ghost.gap.token), attempt(ghost.gap.target), ghost.gap.beforeStop];
  return JSON.stringify([
    ghost.recovery, session, held, activity, attempt(ghost.closing), attempt(ghost.openAttempt), ghost.opened, attempt(ghost.freshOpen),
    ghost.arrivals > 0, ghost.triggered, ghost.currentSettled, ghost.usable, ghost.readySettled, ghost.firstOutcome, ghost.stopped, gap, Math.min(ghost.attemptsInGen, 3),
  ]);
}

interface Exploration {
  readonly sequences: number;
  readonly configurations: number;
  readonly transitions: number;
  readonly violations: ReadonlyMap<string, string>;
  readonly pairs: ReadonlySet<string>;
  readonly goals: ReadonlySet<string>;
}

/**
 * Every event sequence up to `depth`, memoized on a canonical configuration plus remaining depth. Keys are 64-bit
 * hashes to keep memory bounded; with about 1.4 million keys per run a collision, which could skip one configuration,
 * has a probability near 1e-7.
 */
function explore(config: Config, depth: number): Exploration {
  const memo = new Map<number | bigint, number>();
  const violations = new Map<string, string>();
  const pairs = new Set<string>();
  const goals = new Set<string>();
  let transitions = 0;
  const trail: Move[] = [];
  const visit = (ghost: Ghost, remaining: number): number => {
    if (remaining === 0) return 1;
    const key = Bun.hash(`${canonical(ghost)}|${remaining}`);
    const known = memo.get(key);
    if (known !== undefined) return known;
    let count = 1;
    for (const move of enabled(ghost)) {
      transitions += 1;
      const next = apply(ghost, move);
      pairs.add(next.pair);
      for (const goal of next.goals) goals.add(goal);
      if (next.violations.length > 0) {
        for (const violation of next.violations) {
          const id = violation.slice(1, violation.indexOf("]"));
          if (!violations.has(id)) violations.set(id, `${violation} | ${config.name}: ${[...trail, move].join(" → ")}`);
        }
        continue;
      }
      trail.push(move);
      count += visit(next.ghost, remaining - 1);
      trail.pop();
    }
    memo.set(key, count);
    return count;
  };
  const sequences = visit(start(config), depth);
  return { sequences, configurations: memo.size, transitions, violations, pairs, goals };
}

/** Runs one named sequence through the harness; returns the final ghost and every violation on the way. */
function replay(config: Config, moves: readonly Move[]) {
  let ghost = start(config);
  const violations: string[] = [];
  for (const move of moves) {
    if (!enabled(ghost).includes(move)) throw new Error(`${move} is not enabled in ${phaseTag(ghost.state)}`);
    const next = apply(ghost, move);
    violations.push(...next.violations);
    ghost = next.ghost;
  }
  return { ghost, violations, view: view(ghost.state) };
}

const DEPTH = Number(process.env.LIVE_MODEL_DEPTH ?? 10);

const requiredPairs = [
  "opening × notify", "opening × force", "opening × plain", "opening × tick", "opening × stop", "opening × usable", "opening × freshnessReady", "opening × openOk", "opening × openOkChange", "opening × openFail", "opening × gapEnd",
  "passing × notify", "passing × force", "passing × plain", "passing × tick", "passing × stop", "passing × claim", "passing × claimChange", "passing × passOk", "passing × passOkErrors", "passing × passOkChange", "passing × passFailTyped", "passing × passFailEnds", "passing × passFailAfterCommit", "passing × gapEnd",
  "running × notify", "running × force", "running × plain", "running × tick", "running × stop", "running × gapEnd",
  "closing × notify", "closing × force", "closing × plain", "closing × tick", "closing × stop", "closing × attemptClosed", "closing × attemptClosedFailing", "closing × gapEnd",
  "waiting × notify", "waiting × force", "waiting × plain", "waiting × tick", "waiting × stop", "waiting × gapEnd",
  "halting × plain", "halting × attemptClosed", "halting × attemptClosedFailing", "halting × stop",
  "stopped × plain", "stopped × openFail", "stopped × passFailEnds", "stopped × attemptClosed", "stopped × gapEnd",
];

const requiredGoals = [
  "three attempts in one generation", "request admitted while closing", "gap spans stop", "gap spans an attempt change", "retained payload discharged by a later attempt",
];

test("Payload combine is a monoid with empty as identity", () => {
  // #given every payload over a small domain of kinds, force and ordered path lists
  const kinds = ["resync", "watcher", "reconcile"] as const;
  const paths = [[], ["a"], ["b"], ["a", "b"], ["b", "a"]];
  const values = [Payload.empty, ...kinds.flatMap((kind) => [false, true].flatMap((force) => paths.map((list) => Payload.of(kind, force, list))))];
  // #when identity and associativity are evaluated on all pairs and triples
  const identity = values.filter((value) => JSON.stringify(Payload.combine(Payload.empty, value)) !== JSON.stringify(value) || JSON.stringify(Payload.combine(value, Payload.empty)) !== JSON.stringify(value));
  const associativity: string[] = [];
  for (const a of values) for (const b of values) for (const c of values) {
    if (JSON.stringify(Payload.combine(Payload.combine(a, b), c)) !== JSON.stringify(Payload.combine(a, Payload.combine(b, c)))) associativity.push(JSON.stringify([a, b, c]));
  }
  // #then both laws hold, and combining keeps force, every path and resync precedence
  expect({ values: values.length, identity, associativity: associativity.slice(0, 3), sample: Payload.combine(Payload.of("resync", true, ["a"]), Payload.of("watcher", false, ["b", "a"])) })
    .toEqual({ values: 31, identity: [], associativity: [], sample: { kind: "resync", force: true, paths: ["a", "b"] } });
});

test(`live session reducer holds every invariant on all event sequences up to depth ${DEPTH}`, () => {
  // #given three recovery configurations and the full harness alphabet
  const startedAt = performance.now();
  const runs = configs.map((config) => ({ config: config.name, ...explore(config, DEPTH) }));
  const seconds = Number(((performance.now() - startedAt) / 1000).toFixed(1));
  const pairs = new Set(runs.flatMap((run) => [...run.pairs]));
  const goals = new Set(runs.flatMap((run) => [...run.goals]));
  const violations = Object.fromEntries(runs.flatMap((run) => [...run.violations.entries()].map(([id, example]) => [`${run.config} ${id}`, example])));
  console.log(JSON.stringify({ depth: DEPTH, seconds, runs: runs.map((run) => ({ config: run.config, sequences: run.sequences, configurations: run.configurations, transitions: run.transitions })) }));
  // #then no invariant fails, and the search reached every required transition and goal
  expect({ violations, missingPairs: requiredPairs.filter((pair) => !pairs.has(pair)), missingGoals: requiredGoals.filter((goal) => !goals.has(goal)) })
    .toEqual({ violations: {}, missingPairs: [], missingGoals: [] });
}, { timeout: 600_000 });

const warm = configs[1]!;
const cold = configs[2]!;
const strict = configs[0]!;
const opened: Move[] = ["freshnessReady", "openOk"];

test.each([
  // Round 1: a fatal first pass must close admission instead of answering "started".
  ["r1 fatal first pass closes admission", strict, ["openFail", "attemptClosed", "plain"], (r: ReturnType<typeof replay>) => r.ghost.state.tag === "stopped"],
  // Round 1: hints of a pass that fails before invalidation stay held.
  ["r1 failed pass keeps its hints", warm, [...opened, "notify", "passFailTyped", "plain", "claim", "passOk"], (r: ReturnType<typeof replay>) => r.ghost.held.length === 0],
  // Round 2: a fatal completion waiter sees terminal admission.
  ["r2 fatal settle happens with admission already closed", strict, ["plain", "openFail", "plain", "attemptClosed"], (r: ReturnType<typeof replay>) => r.view.state === "stopped"],
  // Round 3: hints of a failed pass merge into an already pending follow-up.
  ["r3 failed pass hints join the pending follow-up", warm, [...opened, "notify", "claim", "notify", "passFailTyped", "claim", "passOk"], (r: ReturnType<typeof replay>) => r.ghost.held.length === 0],
  // Round 3/4: a later defect ends the attempt and, without recovery, stops the session.
  ["r3 later defect without recovery stops", strict, [...opened, "plain", "claim", "passFailEnds", "attemptClosed"], (r: ReturnType<typeof replay>) => r.ghost.state.tag === "stopped"],
  // Round 3: a notify during active work invalidates before the active commit.
  ["r3 notify during active work reaches freshness first", warm, [...opened, "plain", "claim", "notify", "passOk", "claim", "passOk"], (r: ReturnType<typeof replay>) => r.ghost.held.length === 0],
  // Round 4: notify and force during the first pass reach the opening commit.
  ["r4 opening requests invalidate before the opening commit", warm, ["notify", "force", ...opened, "claim", "passOk"], (r: ReturnType<typeof replay>) => r.ghost.held.length === 0],
  // Round 4: a request queued during a failed opening starts one immediate retry.
  ["r4 queued request retries a failed opening", warm, ["plain", "openFail", "attemptClosed", ...opened, "claim", "passOk"], (r: ReturnType<typeof replay>) => r.view.state === "complete"],
  // Round 5: a repeated recoverable failure settles instead of looping.
  ["r5 repeated opening failure settles", warm, ["plain", "openFail", "attemptClosed", "openFail", "attemptClosed"], (r: ReturnType<typeof replay>) => r.view.state === "failed" && r.ghost.currentSettled],
  // Round 5: no stale wake reopens after a failed post-open follow-up.
  ["r5 failed post-open follow-up waits", warm, ["freshnessReady", "openOkChange", "claim", "passFailEnds", "attemptClosed"], (r: ReturnType<typeof replay>) => r.view.state === "failed" && r.ghost.activity === null],
  // Round 5/6: a published first pass is usable output on a cold start.
  ["r5 first publication makes a later defect recoverable", cold, [...opened, "plain", "claim", "passFailEnds", "attemptClosed"], (r: ReturnType<typeof replay>) => r.view.state === "failed"],
  ["r6 publication before an opening failure is usable", cold, ["freshnessReady", "openFail", "attemptClosed"], (r: ReturnType<typeof replay>) => r.view.state === "failed"],
  // Round 5: a notify in the opening commit window reaches the opening freshness.
  ["r5 notify in the opening window reaches freshness", warm, ["freshnessReady", "notify", "openOk", "claim", "passOk"], (r: ReturnType<typeof replay>) => r.ghost.held.length === 0],
  // Round 6: a completion waiter's request after a recoverable failure starts the reopen.
  ["r6 request after recoverable failure starts the reopen", warm, ["openFail", "attemptClosed", "plain"], (r: ReturnType<typeof replay>) => r.view.state === "working"],
  // Round 6: a request whose invalidation gap spans stop stays rejected.
  ["r6 stop during the admission gap rejects", warm, [...opened, "gapNotify", "stop", "gapEnd"], (r: ReturnType<typeof replay>) => r.view.state === "stopped"],
  // Round 6: admitting a retry keeps the last failure.
  ["r6 admitted retry keeps the failure", warm, ["openFail", "attemptClosed", "plain"], (r: ReturnType<typeof replay>) => r.view.failure === CAUSE.open],
  // Round 6: terminal states show no follow-up.
  ["r6 fatal stop shows no follow-up", strict, ["plain", "openFail", "attemptClosed"], (r: ReturnType<typeof replay>) => r.view.followUp === null],
  // Round 7: a request-triggered reopen failure does not spend a second attempt.
  ["r7 reopen trigger is not retried", warm, ["openFail", "attemptClosed", "plain", "openFail", "attemptClosed"], (r: ReturnType<typeof replay>) => r.ghost.openAttempt === null],
  // Round 7: a request during the retry starts one more attempt.
  ["r7 request during the retry starts another attempt", warm, ["plain", "openFail", "attemptClosed", "plain", "openFail", "attemptClosed"], (r: ReturnType<typeof replay>) => r.ghost.openAttempt !== null],
  // Round 8: a reopen trigger survives a failed reopen and a later request.
  ["r8 reopen trigger merges with a later request", warm, ["openFail", "attemptClosed", "force", "openFail", "attemptClosed", "plain", ...opened, "claim", "passOk"], (r: ReturnType<typeof replay>) => r.ghost.held.length === 0],
  // Round 8: a forced trigger survives a failed retry chain.
  ["r8 force survives the retry chain", warm, ["openFail", "attemptClosed", "force", "plain", "openFail", "attemptClosed", "plain", "openFail", "attemptClosed", ...opened, "claim", "passOk"], (r: ReturnType<typeof replay>) => r.ghost.held.length === 0],
  // Round 9: a failed one-shot retry keeps its payload.
  ["r9 failed retry keeps its payload", warm, ["force", "openFail", "attemptClosed", "plain", "openFail", "attemptClosed", ...opened, "claim", "passOk"], (r: ReturnType<typeof replay>) => r.ghost.held.length === 0],
  // Round 9: a request in the closing window of a running failure reaches the next opening commit.
  ["r9 closing-window request reaches freshness", warm, [...opened, "plain", "claim", "passFailEnds", "notify", "attemptClosed", ...opened, "claim", "passOk"], (r: ReturnType<typeof replay>) => r.ghost.held.length === 0],
  // Round 9: ticks during a retry do not keep completion outstanding.
  ["r9 ticks during a retry are dropped", warm, ["plain", "openFail", "attemptClosed", "tick", "openFail", "attemptClosed"], (r: ReturnType<typeof replay>) => r.view.state === "failed" && r.ghost.currentSettled],
] as const)("replay of a past defect holds every invariant: %s", (_name, config, moves, outcome) => {
  // #given the event sequence that exposed the defect
  const result = replay(config, moves);
  // #then no invariant fails and the defect's specific outcome holds
  expect({ violations: result.violations, outcome: outcome(result) }).toEqual({ violations: [], outcome: true });
});
