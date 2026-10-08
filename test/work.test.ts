import { test, expect } from "bun:test";
import { mkdir, mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Deferred, Effect } from "effect";
import { openSynchronization, runInitialPass } from "../src/index.ts";

const io = <A>(run: () => Promise<A>) => Effect.tryPromise({ try: run, catch: (cause) => new Error(String(cause)) }).pipe(Effect.uninterruptible);

test("pending equivalent publication moves behind the work that invalidated it", async () => {
  // #given a required result which does not exist yet
  const root = await mkdtemp(join(tmpdir(), "sync-engine-work-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  try {
    // #when a publication is requested again after its required preparation
    const options = {
      sourcePath, outputPath,
      key: (work: string) => work === "publish" ? "publication" : undefined,
      declare: () => Effect.succeed({ work: ["publish", "prepare", "publish"], publish: Effect.void }),
      handle: (work: string) => io(async () => {
        if (work === "prepare") await Bun.write(join(outputPath, "required"), "Prepared target");
        else await Bun.write(join(outputPath, "reference"), await readFile(join(outputPath, "required"), "utf8"));
        return [];
      }),
    };
    await Effect.runPromise(runInitialPass(options));
    // #then the publication reads a completed required result
    expect(await readFile(join(outputPath, "reference"), "utf8")).toBe("Prepared target");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("required downstream publication keeps public completion working", async () => {
  // #given real output and a held downstream write
  const root = await mkdtemp(join(tmpdir(), "sync-engine-downstream-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  try {
    // #when the initial session submits preparation with required dependent work
    const observation = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const scheduler = yield* openSynchronization({
        sourcePath, outputPath,
        declare: () => Effect.succeed({ work: [], publish: Effect.void }),
        handle: (work: string) => work === "prepare"
          ? io(() => Bun.write(join(outputPath, "required"), "Prepared target")).pipe(Effect.as(["publish"]))
          : Effect.gen(function* () {
              yield* Deferred.succeed(entered, undefined);
              yield* Deferred.await(release);
              yield* io(async () => Bun.write(join(outputPath, "reference"), await readFile(join(outputPath, "required"), "utf8")));
              return [];
            }),
      });
      yield* scheduler.submit(["prepare"]);
      yield* Deferred.await(entered);
      const before = (yield* scheduler.status).state;
      const premature = yield* io(() => Bun.file(join(outputPath, "reference")).exists());
      yield* Deferred.succeed(release, undefined);
      yield* scheduler.awaitCompletion;
      return { before, premature, after: (yield* scheduler.status).state, reference: yield* io(() => readFile(join(outputPath, "reference"), "utf8")) };
    })));
    // #then completion includes the downstream artifact
    expect(observation).toEqual({ before: "working", premature: false, after: "complete", reference: "Prepared target" });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("an equivalent refresh requested while active publishes the later source", async () => {
  // #given a real mutable source and a held refresh that already read its earlier bytes
  const root = await mkdtemp(join(tmpdir(), "sync-engine-active-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  await Bun.write(join(sourcePath, "document"), "Earlier source");
  try {
    // #when another refresh arrives after a later source write
    const result = await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      let first = true;
      const scheduler = yield* openSynchronization({
        sourcePath, outputPath,
        key: () => "refresh",
        declare: () => Effect.succeed({ work: [], publish: Effect.void }),
        handle: (_work: string) => Effect.gen(function* () {
          const bytes = yield* io(() => readFile(join(sourcePath, "document"), "utf8"));
          if (first) {
            first = false;
            yield* Deferred.succeed(entered, undefined);
            yield* Deferred.await(release);
          }
          yield* io(() => Bun.write(join(outputPath, "published"), bytes));
          return [];
        }),
      });
      yield* scheduler.submit(["refresh"]);
      yield* Deferred.await(entered);
      yield* io(() => Bun.write(join(sourcePath, "document"), "Later source"));
      yield* scheduler.submit(["refresh", "refresh"]);
      yield* Deferred.succeed(release, undefined);
      yield* scheduler.awaitCompletion;
      return yield* io(() => readFile(join(outputPath, "published"), "utf8"));
    })));
    // #then a pending follow-up repairs the first refresh's stale publication
    expect(result).toBe("Later source");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
