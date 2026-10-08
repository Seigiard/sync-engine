import { expect, test } from "bun:test";
import { mkdir, mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Cause, Effect, Exit } from "effect";
import { openLiveSynchronization, type LiveSynchronization } from "../src/index.ts";

const io = <A>(run: () => Promise<A>) => Effect.tryPromise({ try: run, catch: (cause) => new Error(String(cause)) }).pipe(Effect.uninterruptible);

test("scope shutdown closes pass admission before awaiting a started publication and restart replays it", async () => {
  // #given a published source held after a write, before successful completion
  const root = await mkdtemp(join(tmpdir(), "sync-stop-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  await Bun.write(join(sourcePath, "document"), "Current document");
  const opened = Promise.withResolvers<LiveSynchronization<string, Error>>();
  const entered = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  let held = false;
  const options = {
    sourcePath, outputPath,
    freshness: { describe: (path: string) => ({ sourcePaths: [path], resultKind: "document", processingVersion: "1", outputPaths: ["published"] }) },
    declare: () => Effect.succeed({ work: ["document"], publish: Effect.void }),
    handle: (_path: string) => io(async () => {
      await Bun.write(join(outputPath, "published"), await readFile(join(sourcePath, "document"), "utf8"));
      if (held) { entered.resolve(); await release.promise; }
      return [];
    }),
  };
  const controller = new AbortController();
  let finished = false;
  const running = Effect.runPromiseExit(Effect.scoped(Effect.gen(function* () {
    const session = yield* openLiveSynchronization(options);
    opened.resolve(session);
    yield* Effect.never;
  })), { signal: controller.signal }).then((exit) => { finished = true; return exit; });
  try {
    const session = await opened.promise;
    held = true;
    await Effect.runPromise(session.requestPass({ force: true }));
    await entered.promise;
    // #when shutdown starts while the handler's publication boundary must still finish
    controller.abort();
    await Bun.sleep(25);
    const during = {
      admission: await Effect.runPromise(session.requestPass()),
      notification: await Effect.runPromise(session.notify(["document"])),
      state: (await Effect.runPromise(session.status)).state,
      finished,
    };
    release.resolve();
    const exit = await running;
    // An independently damaged output proves restart did not trust unfinished success.
    await Bun.write(join(outputPath, "published"), "Unfinished publication");
    held = false;
    await Effect.runPromise(Effect.scoped(openLiveSynchronization(options)));
    // #then admission was closed immediately and startup repaired the actual artifact
    expect({ during, stopped: await Effect.runPromise(session.status), interrupted: Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause), published: await readFile(join(outputPath, "published"), "utf8") }).toEqual({
      during: { admission: "rejected", notification: "rejected", state: "stopped", finished: false },
      stopped: { state: "stopped", pass: null, followUp: null, failure: null, work: { state: "stopped", pending: 0, active: null, errors: [] } },
      interrupted: true, published: "Current document",
    });
  } finally {
    release.resolve();
    controller.abort();
    await running;
    await rm(root, { recursive: true, force: true });
  }
});

test("shutdown during initial traversal leaves prior output intact and releases ownership for a new scan", async () => {
  // #given real nested sources and an existing publication before initial traversal completes
  const root = await mkdtemp(join(tmpdir(), "sync-scan-stop-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(join(sourcePath, "folder"), { recursive: true });
  await mkdir(outputPath);
  await Bun.write(join(sourcePath, "folder", "document"), "Current nested source");
  await Bun.write(join(outputPath, "published"), "Previous publication");
  const controller = new AbortController();
  const options = {
    sourcePath, outputPath,
    declare: () => Effect.succeed({ work: ["folder/document"], publish: Effect.void }),
    handle: (path: string) => io(async () => {
      await Bun.write(join(outputPath, "published"), await readFile(join(sourcePath, path), "utf8"));
      return [];
    }),
  };
  try {
    // #when shutdown is delivered from the public source-filter boundary during traversal
    const exit = await Effect.runPromiseExit(Effect.scoped(openLiveSynchronization({
      ...options, includeSource: () => { controller.abort(); return true; },
    })), { signal: controller.signal });
    const before = await readFile(join(outputPath, "published"), "utf8");
    await Effect.runPromise(Effect.scoped(openLiveSynchronization(options)));
    // #then the cancelled scan did not publish and a fresh owner can rediscover current work
    expect({ interrupted: Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause), before, after: await readFile(join(outputPath, "published"), "utf8") }).toEqual({
      interrupted: true, before: "Previous publication", after: "Current nested source",
    });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
