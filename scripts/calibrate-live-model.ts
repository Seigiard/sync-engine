/**
 * Calibrates test/live-machine.test.ts. Each named mutation of src/live-machine.ts must turn the model test red and
 * name its expected invariant; the unmutated reducer must stay green. A mutation that stays green is a defect in the
 * model, not in the reducer.
 *
 * Usage: bun scripts/calibrate-live-model.ts [--depth 10] [--only M1,M7] [--parallel 3]
 */
import { cp, mkdir, mkdtemp, readFile, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

interface Mutation {
  readonly id: string;
  readonly name: string;
  readonly expect: string;
  readonly search: string;
  readonly replace: string;
}

const passingClaimed = `      }
      return { state: { ...state, phase: { ...phase, due: "arrived" }, owed }, commands: retarget, admission: "queued" };
    case "opening":`;

const mutations: readonly Mutation[] = [
  {
    id: "M1", name: "combine overwrites the held payload (round 8)", expect: "P1",
    search: `    force: left.force || right.force,
    paths: [...new Set([...left.paths, ...right.paths])],`,
    replace: `    force: right.kind === null ? left.force : right.force,
    paths: right.kind === null ? left.paths : right.paths,`,
  },
  {
    id: "M2", name: "a retry after an arrival drops the held payload (round 9)", expect: "P1",
    search: `state: { ...closed, phase: { tag: "opening", attempt, freshness: "pending", due: "carried" }, ids: { ...state.ids, attempt } },`,
    replace: `state: { ...closed, phase: { tag: "opening", attempt, freshness: "pending", due: "carried" }, owed: empty, ids: { ...state.ids, attempt } },`,
  },
  {
    id: "M3", name: "a request while closing is not an arrival (round 7)", expect: "G1",
    search: `    case "opening":
    case "closing":
      return { state: { ...state, phase: { ...phase, due: "arrived" }, owed }, commands: retarget, admission: "queued" };`,
    replace: `    case "closing":
      return { state: { ...state, owed }, commands: retarget, admission: "queued" };
    case "opening":
      return { state: { ...state, phase: { ...phase, due: "arrived" }, owed }, commands: retarget, admission: "queued" };`,
  },
  {
    id: "M4", name: "a closed attempt reopens whenever payload is held (round 5)", expect: "H1",
    search: `      if (phase.due === "arrived") {
        const attempt = state.ids.attempt + 1;`,
    replace: `      if (phase.due === "arrived" || state.owed.kind !== null) {
        const attempt = state.ids.attempt + 1;`,
  },
  {
    id: "M5", name: "stop settles an already settled generation", expect: "C1",
    search: `  if (busy) commands.push({ tag: "settleCompletion", gen: state.ids.gen, exit: { tag: "interrupt" } });`,
    replace: `  commands.push({ tag: "settleCompletion", gen: state.ids.gen, exit: { tag: "interrupt" } });`,
  },
  {
    id: "M6", name: "a failed attempt waits without settling completion", expect: "C2",
    search: `        commands: [...readyCommands(state), { tag: "settleCompletion", gen: state.ids.gen, exit: { tag: "failure", cause: phase.cause } }],`,
    replace: `        commands: [...readyCommands(state)],`,
  },
  {
    id: "M7", name: "the opening applies only payload that arrived during it (old slot)", expect: "F1",
    search: `const commands: Command<C>[] = Payload.invalidates(state.owed) ?`,
    replace: `const commands: Command<C>[] = phase.due === "arrived" && Payload.invalidates(state.owed) ?`,
  },
  {
    id: "M8", name: "a stopped session admits requests (round 6)", expect: "A1",
    search: `if (state.tag === "stopped") return unchanged(state, event.tag === "request" ? "rejected" : null);`,
    replace: `if (state.tag === "stopped") return unchanged(state, event.tag === "request" ? "queued" : null);`,
  },
  {
    id: "M9", name: "a fatal failure fails ready before the lease is released", expect: "R1",
    search: `return { state: { ...rest, tag: "halting", attempt, cause, failure: cause }, commands: [command], admission: null };`,
    replace: `return { state: { ...rest, tag: "halting", attempt, cause, failure: cause }, commands: state.ready === "pending" ? [command, { tag: "settleReady", exit: { tag: "failure", cause } }] : [command], admission: null };`,
  },
  {
    id: "M10", name: "a request during a claimed pass is not scheduled", expect: "G1",
    search: passingClaimed,
    replace: `      }
      return { state: { ...state, owed }, commands: retarget, admission: "queued" };
    case "opening":`,
  },
  {
    id: "M11", name: "ticks while busy count as arrivals (round 9)", expect: "H1",
    search: `return phase.tag === "running" || phase.tag === "waiting" ? {`,
    replace: `return phase.tag !== "passing" ? {`,
  },
  {
    id: "M12", name: "a request during a claimed pass starts a second pass", expect: "L1",
    search: passingClaimed,
    replace: `      }
      { const next = startPass(state, phase.attempt, owed); return { state: next.state, commands: [...retarget, next.command], admission: "queued" }; }
    case "opening":`,
  },
  {
    id: "M13", name: "a successful open keeps the last failure", expect: "S2",
    search: `const opened: Live<C> = { ...state, failure: null, ready: "settled" };`,
    replace: `const opened: Live<C> = { ...state, ready: "settled" };`,
  },
  {
    id: "M14", name: "publication does not make output usable (round 6)", expect: "V1",
    search: `{ ...state, phase: { ...phase, freshness: "ready" }, availability: state.availability ?? "minimum-publication" }`,
    replace: `{ ...state, phase: { ...phase, freshness: "ready" } }`,
  },
  {
    id: "M15", name: "the closing attempt stays an invalidation target (round 9 window)", expect: "F2",
    search: `return phase.tag === "passing" || phase.tag === "running" ? phase.attempt : null;`,
    replace: `return phase.tag === "passing" || phase.tag === "running" || phase.tag === "closing" ? phase.attempt : null;`,
  },
];

const argument = (name: string) => {
  const index = process.argv.indexOf(name);
  return index === -1 ? undefined : process.argv[index + 1];
};
const depth = argument("--depth") ?? process.env.LIVE_MODEL_DEPTH ?? "10";
const only = argument("--only")?.split(",");
const parallel = Number(argument("--parallel") ?? 3);
const root = resolve(import.meta.dir, "..");
const machinePath = "src/live-machine.ts";

interface Outcome {
  readonly id: string;
  readonly name: string;
  readonly expect: string;
  readonly red: boolean;
  readonly named: readonly string[];
  readonly seconds: number;
  readonly ok: boolean;
  readonly tail: string;
}

async function run(mutation: Mutation | null): Promise<Outcome> {
  const workspace = await mkdtemp(join(tmpdir(), "live-model-calibration-"));
  try {
    await mkdir(join(workspace, "test"));
    await cp(join(root, "src"), join(workspace, "src"), { recursive: true });
    await cp(join(root, "test/live-machine.test.ts"), join(workspace, "test/live-machine.test.ts"));
    await cp(join(root, "package.json"), join(workspace, "package.json"));
    await symlink(join(root, "node_modules"), join(workspace, "node_modules"));
    if (mutation !== null) {
      const source = await readFile(join(root, machinePath), "utf8");
      const matches = source.split(mutation.search).length - 1;
      if (matches !== 1) throw new Error(`${mutation.id}: anchor matched ${matches} times`);
      await writeFile(join(workspace, machinePath), source.replace(mutation.search, mutation.replace));
    }
    const startedAt = performance.now();
    const child = Bun.spawn(["bun", "test", "test/live-machine.test.ts"], { cwd: workspace, env: { ...process.env, LIVE_MODEL_DEPTH: depth }, stdout: "pipe", stderr: "pipe" });
    const [stdout, stderr, code] = await Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text(), child.exited]);
    const seconds = Number(((performance.now() - startedAt) / 1000).toFixed(1));
    const output = stdout + stderr;
    const named = [...new Set([...output.matchAll(/\[([A-Z]\d)\]/g)].map((match) => match[1]!))].sort();
    const red = code !== 0;
    const tail = output.trim().split("\n").slice(-25).join("\n");
    if (mutation === null) return { id: "baseline", name: "unmutated reducer", expect: "-", red, named, seconds, ok: !red, tail };
    return { id: mutation.id, name: mutation.name, expect: mutation.expect, red, named, seconds, ok: red && named.includes(mutation.expect), tail };
  } finally {
    await rm(workspace, { recursive: true, force: true });
  }
}

const queue: (Mutation | null)[] = [null, ...mutations.filter((mutation) => only === undefined || only.includes(mutation.id))];
const outcomes: Outcome[] = [];
await Promise.all(Array.from({ length: parallel }, async () => {
  for (let next = queue.shift(); next !== undefined; next = queue.shift()) {
    const outcome = await run(next);
    outcomes.push(outcome);
    if (!outcome.ok) console.log(`--- ${outcome.id} output tail\n${outcome.tail}\n---`);
    console.log(`${outcome.id}\t${outcome.red ? "RED" : "GREEN"}\texpected ${outcome.expect}\tnamed ${outcome.named.join(",") || "-"}\t${outcome.seconds}s\t${outcome.ok ? "ok" : "NOT CALIBRATED"}\t${outcome.name}`);
  }
}));
const order = (id: string) => id === "baseline" ? 0 : Number(id.slice(1));
outcomes.sort((left, right) => order(left.id) - order(right.id));
console.log(`\n| Mutation | Result | Expected | Named | Seconds | Calibrated |\n| --- | --- | --- | --- | --- | --- |`);
for (const outcome of outcomes) console.log(`| ${outcome.id} ${outcome.name} | ${outcome.red ? "RED" : "GREEN"} | ${outcome.expect} | ${outcome.named.join(", ") || "-"} | ${outcome.seconds} | ${outcome.ok ? "yes" : "NO"} |`);
console.log(`\ndepth ${depth}`);
if (outcomes.some((outcome) => !outcome.ok)) process.exit(1);
