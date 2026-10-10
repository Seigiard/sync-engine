/**
 * Packs the checkout and verifies the archive against it: identity, peer, exact inventory and byte equality.
 * Prints the archive path, size, SHA256 and the SHA512 integrity string a consumer's lockfile records.
 * Run it from a clean checkout: `node scripts/verify-pack.ts [destination]` (Node 24+), or use Bun.
 */
import { createHash } from "node:crypto";
import { execFile } from "node:child_process";
import { mkdir, mkdtemp, readdir, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const destination = resolve(process.argv[2] ?? (await mkdtemp(join(tmpdir(), "sync-engine-pack-"))));
const manifest = JSON.parse(await readFile(join(root, "package.json"), "utf8")) as {
  name: string;
  version: string;
  peerDependencies?: Record<string, string>;
};

const execute = promisify(execFile);
const run = async ([command, ...args]: [string, ...string[]], cwd: string) => (await execute(command, args, { cwd })).stdout;

const failures: string[] = [];
const expect = (ok: boolean, message: string) => {
  if (!ok) failures.push(message);
};

const dirty = (await run(["git", "status", "--porcelain"], root)).trim();
if (dirty !== "") throw new Error(`checkout is not clean:\n${dirty}`);
const commit = (await run(["git", "rev-parse", "HEAD"], root)).trim();

await mkdir(destination, { recursive: true });
await run(["npm", "pack", "--ignore-scripts", "--pack-destination", destination], root);
const archive = join(destination, `${manifest.name.replace("@", "").replace("/", "-")}-${manifest.version}.tgz`);
const bytes = await readFile(archive);
const extracted = await mkdtemp(join(tmpdir(), "sync-engine-unpacked-"));

try {
  await run(["tar", "-xzf", archive, "-C", extracted], root);
  const packed = JSON.parse(await readFile(join(extracted, "package", "package.json"), "utf8")) as typeof manifest;
  expect(packed.name === manifest.name && packed.version === manifest.version, "packed name/version differ from package.json");
  expect(JSON.stringify(packed.peerDependencies) === JSON.stringify(manifest.peerDependencies), "packed peer dependencies differ");
  expect(manifest.peerDependencies?.effect !== undefined, "effect must stay a peer dependency");

  const inventory = async (base: string, prefix = ""): Promise<string[]> => {
    const entries = await readdir(join(base, prefix), { withFileTypes: true });
    const nested = await Promise.all(entries.map((entry) => (entry.isDirectory() ? inventory(base, join(prefix, entry.name)) : [join(prefix, entry.name)])));

    return nested.flat().sort();
  };

  const packedFiles = await inventory(join(extracted, "package"));
  const sources = (await readdir(join(root, "src"))).map((name) => join("src", name));
  const expected = ["README.md", "package.json", ...sources].sort();
  expect(JSON.stringify(packedFiles) === JSON.stringify(expected), `inventory differs:\n packed:   ${packedFiles.join(", ")}\n expected: ${expected.join(", ")}`);

  for (const file of packedFiles.filter((name) => name !== "package.json")) {
    const same = Buffer.compare(await readFile(join(extracted, "package", file)), await readFile(join(root, file))) === 0;
    expect(same, `${file} differs from the checkout`);
  }

  console.log(JSON.stringify({
    package: `${manifest.name}@${manifest.version}`,
    commit,
    archive,
    bytes: bytes.byteLength,
    sha256: createHash("sha256").update(bytes).digest("hex"),
    integrity: `sha512-${createHash("sha512").update(bytes).digest("base64")}`,
    inventory: packedFiles,
    peerDependencies: packed.peerDependencies,
  }, null, 2));
} finally {
  await rm(extracted, { recursive: true, force: true });
}

if (failures.length > 0) {
  console.error(failures.join("\n"));
  process.exit(1);
}
