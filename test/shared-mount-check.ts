import { strict as assert } from "node:assert";
import { resolve } from "node:path";

// Host runner; the public API and filesystem operations execute in separate Linux containers.
const project = process.env.COMPOSE_PROJECT_NAME ?? "opds49-52";
const identity = `${project}-lease-${process.pid}`;
const image = `${project}-engine-test`;
const source = resolve(import.meta.dir, "../src");
const mounts = ["--mount", `type=volume,src=${identity},dst=/derived`, "--mount", `type=bind,src=${source},dst=/app/src,readonly`];

async function command(args: string[]) {
  const child = Bun.spawn(args, { stdout: "pipe", stderr: "pipe" });
  const [code, stdout, stderr] = await Promise.all([child.exited, new Response(child.stdout).text(), new Response(child.stderr).text()]);
  return { code, stdout: stdout.trim(), stderr: stderr.trim() };
}

async function successful(args: string[]) {
  const result = await command(args);
  assert.equal(result.code, 0, `${args.join(" ")}\n${result.stderr}`);
  return result.stdout;
}

const imports = `import { Effect, Exit } from "effect"; import { stat, symlink } from "node:fs/promises"; import { acquireOutputTree, engineStatePath } from "/app/src/index.ts";`;
const holder = `${imports}
const release = await Effect.runPromise(acquireOutputTree("/derived", "/derived/.sync-engine"));
await Bun.write("/derived/.sync-engine/saved", "Persistent engine state");
console.log("LEASE_HELD");
while (!(await Bun.file("/derived/release").exists())) await Bun.sleep(50);
await release(); console.log("LEASE_RELEASED");`;

try {
  // #given one volume and an actual Linux owner whose stdin retains the flock child
  await successful(["docker", "volume", "create", identity]);
  await successful(["docker", "run", "-d", "--name", identity, "--label", `com.docker.compose.project=${project}`, ...mounts, image, "bun", "-e", holder]);
  const deadline = Date.now() + 10_000;
  while (true) {
    const logs = await successful(["docker", "logs", identity]);
    if (logs.includes("LEASE_HELD")) break;
    assert.equal(await successful(["docker", "inspect", "--format", "{{.State.Running}}", identity]), "true", logs);
    assert.ok(Date.now() < deadline, "Owner did not reach its acquisition handshake in 10 seconds");
    await Bun.sleep(100);
  }
  console.log("shared-mount: first container owns the configured DATA state area");

  // #when a second container mounts that volume under an alias and tries to acquire ownership
  const contender = await command(["docker", "run", "--rm", ...mounts, image, "bun", "-e", `${imports}
await symlink("/derived", "/alias");
const state = await Effect.runPromise(engineStatePath("/alias", "/alias/.sync-engine"));
const inode = (await stat(state + "/lock")).ino;
const exit = await Effect.runPromiseExit(acquireOutputTree("/alias", "/alias/.sync-engine"));
if (Exit.isSuccess(exit)) { await exit.value(); process.exit(2); }
console.log(JSON.stringify({state,inode,saved:await Bun.file(state + "/saved").text()}));
process.exit(73);`]);
  assert.equal(contender.code, 73, contender.stderr);
  const before: { state: string; inode: number; saved: string } = JSON.parse(contender.stdout);
  assert.deepEqual({ state: before.state, saved: before.saved }, { state: "/derived/.sync-engine", saved: "Persistent engine state" });
  console.log("shared-mount: alias contender was refused");

  await successful(["docker", "exec", identity, "bun", "-e", 'await Bun.write("/derived/release", "release")']);
  assert.equal(await successful(["docker", "wait", identity]), "0");
  const after = await successful(["docker", "run", "--rm", ...mounts, image, "bun", "-e", `${imports}
await symlink("/derived", "/alias");
const release = await Effect.runPromise(acquireOutputTree("/alias", "/alias/.sync-engine"));
const state = await Effect.runPromise(engineStatePath("/alias", "/alias/.sync-engine"));
console.log(JSON.stringify({state,inode:(await stat(state + "/lock")).ino,saved:await Bun.file(state + "/saved").text()}));
await release();`]);
  // #then a recreated container acquires the same inode and retains state bytes on that mount
  assert.deepEqual(JSON.parse(after), before);
  console.log("shared-mount: recreated container acquired the same inode and retained state; PASS");
} finally {
  await command(["docker", "exec", identity, "bun", "-e", 'await Bun.write("/derived/release", "release")']);
  await command(["docker", "stop", "-t", "10", identity]);
  await command(["docker", "rm", identity]);
  await successful(["docker", "volume", "rm", identity]);
}
