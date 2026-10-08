import { test, expect } from "bun:test";
import { access, mkdir, mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Effect } from "effect";
import { openSynchronization } from "../src/index.ts";

const io = <A>(run: () => Promise<A>) => Effect.tryPromise({ try: run, catch: (cause) => new Error(String(cause)) }).pipe(Effect.uninterruptible);

function gate() {
  const { promise, resolve } = Promise.withResolvers<void>();
  return { promise, open: () => resolve() };
}

const exists = (path: string) => access(path).then(() => true, () => false);

test("the minimum is published and reported before held remaining work finishes", async () => {
  // #given a source whose remaining work is held after the minimum was declared
  const root = await mkdtemp(join(tmpdir(), "sync-engine-minimum-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  await Bun.write(join(sourcePath, "document"), "Source");
  const entered = gate();
  const release = gate();
  const reported = gate();
  let atReport: { minimum: string; remaining: boolean } | undefined;

  try {
    const running = Effect.runPromise(Effect.scoped(openSynchronization({
      sourcePath, outputPath,
      declare: () => Effect.succeed({ minimum: ["minimum"], work: ["remaining"], publish: Effect.void }),
      onMinimum: io(async () => {
        atReport = { minimum: await readFile(join(outputPath, "minimum"), "utf8"), remaining: await exists(join(outputPath, "remaining")) };
        reported.open();
      }),
      handle: (work: string) => io(async () => {
        if (work === "remaining") {
          entered.open();
          await release.promise;
        }
        await Bun.write(join(outputPath, work), `${work} result`);

        return [];
      }),
    }).pipe(Effect.asVoid)));
    // #when the minimum drains while the remaining work is still held
    await Promise.race([reported.promise, running]);
    await Promise.race([entered.promise, Bun.sleep(2000)]);
    const heldRemaining = await exists(join(outputPath, "remaining"));
    release.open();
    await running;
    // #then readiness preceded the remaining output, and that output still completes
    expect({ atReport, heldRemaining, remaining: await readFile(join(outputPath, "remaining"), "utf8") })
      .toEqual({ atReport: { minimum: "minimum result", remaining: false }, heldRemaining: false, remaining: "remaining result" });
  } finally {
    release.open();
    await rm(root, { recursive: true, force: true });
  }
});

test("a minimum that cannot be prepared fails before readiness and before remaining work starts", async () => {
  // #given a minimum whose handler fails beside independent remaining work
  const root = await mkdtemp(join(tmpdir(), "sync-engine-minimum-failure-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  let reported = false;

  try {
    // #when the open runs
    const outcome = await Effect.runPromise(Effect.scoped(openSynchronization({
      sourcePath, outputPath,
      declare: () => Effect.succeed({ minimum: ["minimum"], work: ["remaining"], publish: Effect.void }),
      onMinimum: Effect.sync(() => { reported = true; }),
      handle: (work: string) => work === "minimum"
        ? Effect.fail(new Error("Cannot prepare minimum"))
        : io(async () => { await Bun.write(join(outputPath, "remaining"), "started"); return []; }),
    }).pipe(Effect.as("success"), Effect.catch((error) => Effect.succeed(error.message)))));
    // #then no readiness was reported and the remaining work never ran
    expect({ outcome, reported, remainingStarted: await exists(join(outputPath, "remaining")) })
      .toEqual({ outcome: "Cannot prepare minimum", reported: false, remainingStarted: false });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
