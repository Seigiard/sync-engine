import { test, expect } from "bun:test";
import { mkdir, mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Effect } from "effect";
import { openLiveSynchronization } from "../src/index.ts";

const io = <A>(run: () => Promise<A>) => Effect.tryPromise({ try: run, catch: (cause) => new Error(String(cause)) }).pipe(Effect.uninterruptible);

function gate() {
  const { promise, resolve } = Promise.withResolvers<void>();
  return { promise, open: () => resolve() };
}

test("a source replaced during initial processing converges without a watcher notice", async () => {
  // #given a real source whose old bytes have been read at a held publication boundary
  const root = await mkdtemp(join(tmpdir(), "sync-engine-live-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  await Bun.write(join(sourcePath, "document"), "Earlier source");
  const entered = gate();
  const release = gate();
  let first = true;
  try {
    const running = Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const session = yield* openLiveSynchronization({
        sourcePath, outputPath,
        declare: (entries) => Effect.succeed({ work: entries.filter((entry) => entry.kind === "file").map((entry) => entry.path), publish: Effect.void }),
        handle: (path: string) => io(async () => {
          const bytes = await readFile(join(sourcePath, path), "utf8");
          if (first) {
            first = false;
            entered.open();
            await release.promise;
          }
          await Bun.write(join(outputPath, "published"), bytes);
          return [];
        }),
      });
      yield* session.awaitCompletion;
      return yield* io(() => readFile(join(outputPath, "published"), "utf8"));
    })));
    // #when processing finishes after an unannounced replacement
    await entered.promise;
    await Bun.write(join(sourcePath, "document"), "Current replacement source");
    release.open();
    const result = await running;
    // #then the published result reflects the current source, not captured old bytes
    expect(result).toBe("Current replacement source");
  } finally {
    release.open();
    await rm(root, { recursive: true, force: true });
  }
});

test("a publication failure does not discard an already admitted forced follow-up", async () => {
  // #given a real published reference and a held update before a real failed read
  const root = await mkdtemp(join(tmpdir(), "sync-engine-follow-up-failure-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  await Bun.write(join(sourcePath, "document"), "Original publication");
  const entered = gate();
  const release = gate();
  let hold = false;
  let failPublication = false;
  try {
    const result = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const session = yield* openLiveSynchronization({
        sourcePath, outputPath,
        declare: () => Effect.succeed({
          work: ["document"],
          publish: io(async () => {
            if (failPublication) {
              failPublication = false;
              await readFile(join(root, "unavailable-publication-target"), "utf8");
            }
            await Bun.write(join(outputPath, "reference"), await readFile(join(sourcePath, "document"), "utf8"));
          }),
        }),
        handle: (_path: string) => io(async () => {
          if (hold) {
            hold = false;
            entered.open();
            await release.promise;
          }
          return [];
        }),
      });
      yield* io(() => Bun.write(join(sourcePath, "document"), "Current publication"));
      hold = true;
      failPublication = true;
      yield* session.requestPass();
      yield* io(() => entered.promise);
      // #when the forced follow-up is admitted before the current publication fails
      yield* session.requestPass({ force: true });
      release.open();
      yield* session.awaitCompletion;
      return yield* io(() => readFile(join(outputPath, "reference"), "utf8"));
    })));
    // #then the already admitted pass repairs publication instead of being dropped
    expect(result).toBe("Current publication");
  } finally {
    release.open();
    await rm(root, { recursive: true, force: true });
  }
});

test("combined requests during processing preserve a forced repair of unchanged sources", async () => {
  // #given two real sources and a previously published representation
  const root = await mkdtemp(join(tmpdir(), "sync-engine-forced-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  await Bun.write(join(sourcePath, "first"), "First original");
  await Bun.write(join(sourcePath, "second"), "Second unchanged");
  const entered = gate();
  const release = gate();
  let hold = false;
  try {
    const result = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const session = yield* openLiveSynchronization({
        sourcePath, outputPath,
        declare: (entries, request) => Effect.succeed({
          work: entries.filter((entry) => entry.kind === "file" && (request.kind === "initial" || request.force || request.changedPaths.includes(entry.path))).map((entry) => entry.path),
          publish: Effect.void,
        }),
        handle: (path: string) => io(async () => {
          const bytes = await readFile(join(sourcePath, path), "utf8");
          if (hold && path === "first") {
            hold = false;
            entered.open();
            await release.promise;
          }
          await Bun.write(join(outputPath, path), bytes);
          return [];
        }),
      });
      yield* io(async () => {
        hold = true;
        await Bun.write(join(sourcePath, "first"), "First current replacement");
      });
      yield* session.requestPass();
      yield* io(() => entered.promise);
      const before = yield* io(() => readFile(join(outputPath, "first"), "utf8"));
      yield* io(() => Bun.write(join(outputPath, "second"), "Damaged old representation"));
      const admissions = [yield* session.requestPass(), yield* session.requestPass({ force: true }), yield* session.requestPass()];
      release.open();
      yield* session.awaitCompletion;
      return {
        before, admissions,
        first: yield* io(() => readFile(join(outputPath, "first"), "utf8")),
        second: yield* io(() => readFile(join(outputPath, "second"), "utf8")),
      };
    })));
    // #then prior results stayed available and the combined follow-up retained forced mode
    expect(result).toEqual({ before: "First original", admissions: ["queued", "queued", "queued"], first: "First current replacement", second: "Second unchanged" });
  } finally {
    release.open();
    await rm(root, { recursive: true, force: true });
  }
});
