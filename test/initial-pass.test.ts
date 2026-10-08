import { test, expect } from "bun:test";
import { mkdir, mkdtemp, readFile, readdir, rm, symlink } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Effect } from "effect";
import { runInitialPass } from "../src/index.ts";

const io = <A>(run: () => Promise<A>) => Effect.tryPromise({ try: run, catch: (cause) => new Error(String(cause)) }).pipe(Effect.uninterruptible);

test("regular source paths produce downstream work before final publication", async () => {
  // #given real files and a source symlink which must not enter the listing
  const root = await mkdtemp(join(tmpdir(), "sync-engine-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(join(sourcePath, "nested"), { recursive: true });
  await Bun.write(join(sourcePath, "nested", "document.txt"), "hello");
  await symlink(join(sourcePath, "nested"), join(sourcePath, "alias"));

  try {
    // #when application work writes a result, cascades a dependent write, then publishes
    await Effect.runPromise(runInitialPass({
      sourcePath, outputPath,
      declare: (entries) => Effect.succeed({
        work: ["prepare"],
        publish: io(async () => {
          const downstream = await readFile(join(outputPath, "downstream"), "utf8");
          await Bun.write(join(outputPath, "published"), `${entries.map((entry) => `${entry.kind}:${entry.path}`).join("\n")}\n${downstream}`);
        }),
      }),
      handle: (work) => io(async () => {
        if (work === "prepare") {
          await Bun.write(join(outputPath, "prepared"), "Required result");

          return ["dependent"];
        }

        await Bun.write(join(outputPath, "downstream"), `${await readFile(join(outputPath, "prepared"), "utf8")} exists`);

        return [];
      }),
    }));
    // #then the public artifact records only authoritative paths and completed dependencies
    expect(await readFile(join(outputPath, "published"), "utf8"))
      .toBe("directory:nested\nfile:nested/document.txt\nRequired result exists");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("a killed output owner does not block a new process from publishing", async () => {
  // #given a real child process holds the public output lease
  const root = await mkdtemp(join(tmpdir(), "sync-engine-killed-owner-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  const child = Bun.spawn([process.execPath, join(import.meta.dir, "lease-owner.ts"), outputPath], {
    stdin: "ignore", stdout: "pipe", stderr: "inherit",
  });

  try {
    const reader = child.stdout.getReader();
    const ready = await reader.read();
    reader.releaseLock();
    if (new TextDecoder().decode(ready.value).trim() !== "output acquired") throw new Error("Child did not acquire output");
    // #when the owner is killed and a new initial pass uses its output tree
    child.kill("SIGKILL");
    const exit = await child.exited;
    await Effect.runPromise(runInitialPass({
      sourcePath, outputPath,
      declare: () => Effect.succeed({ work: [], publish: io(() => Bun.write(join(outputPath, "published"), "Recovered publication")).pipe(Effect.asVoid) }),
      handle: () => Effect.succeed([]),
    }));
    // #then process termination is observed and publication succeeds without wiping output
    expect({ exit, published: await readFile(join(outputPath, "published"), "utf8") })
      .toEqual({ exit: 137, published: "Recovered publication" });
  } finally {
    if (child.exitCode === null) child.kill("SIGKILL");
    await child.exited;
    await rm(root, { recursive: true, force: true });
  }
});

test("a failed handler retains prior publication and releases ownership for replay", async () => {
  // #given a prior successful publication and an application that cannot prepare its work
  const root = await mkdtemp(join(tmpdir(), "sync-engine-failure-"));
  const sourcePath = join(root, "source");
  const outputPath = join(root, "output");
  await mkdir(sourcePath);
  await mkdir(outputPath);
  await Bun.write(join(outputPath, "published"), "Last successful result");
  const declare = () => Effect.succeed({
    work: ["prepare"],
    publish: io(() => Bun.write(join(outputPath, "published"), "Replacement")).pipe(Effect.asVoid),
  });

  try {
    // #when preparation fails, and a new initial pass retries after that failure
    const failure = await Effect.runPromise(runInitialPass({
      sourcePath, outputPath, declare, handle: () => Effect.fail(new Error("Cannot prepare")),
    }).pipe(Effect.as("unexpected success"), Effect.catch((error) => Effect.succeed(error.message))));
    const retained = await readFile(join(outputPath, "published"), "utf8");
    await Effect.runPromise(runInitialPass({ sourcePath, outputPath, declare, handle: () => Effect.succeed([]) }));
    // #then the failure is observable, retention precedes successful replay
    expect({ failure, retained, replay: await readFile(join(outputPath, "published"), "utf8") })
      .toEqual({ failure: "Cannot prepare", retained: "Last successful result", replay: "Replacement" });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("a missing source fails instead of publishing an empty replacement", async () => {
  // #given published data but no readable source root
  const root = await mkdtemp(join(tmpdir(), "sync-engine-read-"));
  const outputPath = join(root, "output");
  await mkdir(outputPath);
  await Bun.write(join(outputPath, "published"), "Last successful result");

  try {
    // #when the engine scans the absent root
    const outcome = await Effect.runPromise(runInitialPass({
      sourcePath: join(root, "absent"), outputPath,
      declare: () => Effect.succeed({ work: [], publish: io(() => Bun.write(join(outputPath, "published"), "Empty")).pipe(Effect.asVoid) }),
      handle: () => Effect.succeed([]),
    }).pipe(Effect.as("success"), Effect.catchTag("ScanFailed", () => Effect.succeed("read failure"))));
    // #then the published representation is unchanged
    expect({ outcome, published: await readFile(join(outputPath, "published"), "utf8") })
      .toEqual({ outcome: "read failure", published: "Last successful result" });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("an output inside the source is rejected before creating directories", async () => {
  // #given an output path nested under the authoritative source
  const root = await mkdtemp(join(tmpdir(), "sync-engine-source-"));
  const sourcePath = join(root, "source");
  await mkdir(sourcePath);

  try {
    // #when an application attempts to use that output
    const outcome = await Effect.runPromise(runInitialPass({
      sourcePath, outputPath: join(sourcePath, "new", "output"),
      declare: () => Effect.succeed({ work: [], publish: Effect.void }),
      handle: () => Effect.succeed([]),
    }).pipe(Effect.as("success"), Effect.catchTag("OutputOwnershipFailed", () => Effect.succeed("invalid output"))));
    // #then the source tree is still empty
    expect({ outcome, sourceEntries: await readdir(sourcePath) })
      .toEqual({ outcome: "invalid output", sourceEntries: [] });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
