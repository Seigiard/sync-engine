import { expect, test } from "bun:test";
import { cp, mkdir, mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";

const root = resolve(import.meta.dir, "..");
const fixtureDirectory = join(import.meta.dir, "fixtures", "pure-modules");
const pureModules = ["src/live-machine.ts", "src/source-snapshot.ts", "src/freshness-codec.ts"] as const;

type PureModule = (typeof pureModules)[number];

interface CheckResult {
  readonly exitCode: number;
  readonly stderr: string;
  readonly stdout: string;
}

async function checkFixture(module: PureModule, fixture: string): Promise<CheckResult> {
  const temporaryRoot = await mkdtemp(join(tmpdir(), "sync-engine-pure-modules-"));
  const checkerPath = join(temporaryRoot, "scripts", "check-pure-modules.ts");
  const source = await readFile(join(fixtureDirectory, fixture), "utf8");
  try {
    await mkdir(dirname(checkerPath), { recursive: true });
    await cp(join(root, "scripts", "check-pure-modules.ts"), checkerPath);
    for (const pureModule of pureModules) {
      const fixtureSource = pureModule === module ? source : await readFile(join(fixtureDirectory, "empty.ts"), "utf8");
      await Bun.write(join(temporaryRoot, pureModule), fixtureSource);
    }
    const child = Bun.spawn([process.execPath, "scripts/check-pure-modules.ts"], {
      cwd: temporaryRoot,
      stdout: "pipe",
      stderr: "pipe",
    });
    const [exitCode, stdout, stderr] = await Promise.all([
      child.exited,
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
    ]);
    return { exitCode, stdout, stderr };
  } finally {
    await rm(temporaryRoot, { recursive: true, force: true });
  }
}

for (const module of pureModules) {
  test(`${module} rejects a multiline runtime import`, async () => {
    // #given an independent multiline runtime-import source fixture in a protected module
    const result = await checkFixture(module, "multiline-runtime-import.ts");
    // #when the real pure-module checker runs from the repository root
    // #then it fails and names the runtime dependency in the protected module
    expect({
      exitCode: result.exitCode,
      stdout: result.stdout,
      reportsRuntimeDependency: result.stderr.includes('pure module imports runtime dependency "node:fs/promises"'),
    }).toEqual({
      exitCode: 1,
      stdout: "",
      reportsRuntimeDependency: true,
    });
  });

  test(`${module} rejects a multiline runtime re-export`, async () => {
    // #given an independent multiline runtime-re-export source fixture in a protected module
    const result = await checkFixture(module, "multiline-runtime-export.ts");
    // #when the real pure-module checker runs from the repository root
    // #then it fails and names the runtime dependency in the protected module
    expect({
      exitCode: result.exitCode,
      stdout: result.stdout,
      reportsRuntimeDependency: result.stderr.includes('pure module imports runtime dependency "node:fs/promises"'),
    }).toEqual({
      exitCode: 1,
      stdout: "",
      reportsRuntimeDependency: true,
    });
  });
}

test("the checker preserves static import rejection", async () => {
  // #given an independent one-line runtime-import source fixture in a protected module
  const result = await checkFixture("src/live-machine.ts", "runtime-import.ts");
  // #when the real pure-module checker runs from the repository root
  // #then it rejects the runtime dependency
  expect({
    exitCode: result.exitCode,
    stdout: result.stdout,
    reportsRuntimeDependency: result.stderr.includes('pure module imports runtime dependency "node:fs/promises"'),
  }).toEqual({
    exitCode: 1,
    stdout: "",
    reportsRuntimeDependency: true,
  });
});

test("the checker preserves dynamic import rejection", async () => {
  // #given an independent dynamic-import source fixture in a protected module
  const result = await checkFixture("src/live-machine.ts", "dynamic-import.ts");
  // #when the real pure-module checker runs from the repository root
  // #then it rejects the runtime dependency
  expect({
    exitCode: result.exitCode,
    stdout: result.stdout,
    reportsRuntimeDependency: result.stderr.includes('pure module imports runtime dependency "node:fs/promises"'),
  }).toEqual({
    exitCode: 1,
    stdout: "",
    reportsRuntimeDependency: true,
  });
});

test("the checker preserves require rejection", async () => {
  // #given an independent require-call source fixture in a protected module
  const result = await checkFixture("src/live-machine.ts", "require.ts");
  // #when the real pure-module checker runs from the repository root
  // #then it rejects the runtime dependency
  expect({
    exitCode: result.exitCode,
    stdout: result.stdout,
    reportsRuntimeDependency: result.stderr.includes('pure module imports runtime dependency "node:fs/promises"'),
  }).toEqual({
    exitCode: 1,
    stdout: "",
    reportsRuntimeDependency: true,
  });
});

test("the checker accepts a type-only import", async () => {
  // #given an independent type-only source fixture in a protected module
  const result = await checkFixture("src/live-machine.ts", "type-only-import.ts");
  // #when the real pure-module checker runs from the repository root
  // #then it accepts the type-only dependency
  expect(result).toEqual({
    exitCode: 0,
    stdout: "Pure module dependency check passed: src/live-machine.ts, src/source-snapshot.ts, src/freshness-codec.ts\n",
    stderr: "",
  });
});

test("the checker accepts an allowed pure relative import", async () => {
  // #given an independent allowed-relative-import source fixture in a protected module
  const result = await checkFixture("src/live-machine.ts", "allowed-pure-relative-import.fixture");
  // #when the real pure-module checker runs from the repository root
  // #then it accepts the dependency on another designated pure module
  expect(result).toEqual({
    exitCode: 0,
    stdout: "Pure module dependency check passed: src/live-machine.ts, src/source-snapshot.ts, src/freshness-codec.ts\n",
    stderr: "",
  });
});
