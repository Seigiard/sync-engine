import { readFile } from "node:fs/promises";
import { dirname, relative, resolve } from "node:path";

const root = resolve(import.meta.dir, "..");
const pureModules = ["src/live-machine.ts", "src/source-snapshot.ts", "src/freshness-codec.ts"] as const;
const transpiler = new Bun.Transpiler({ loader: "ts" });

const modulePath = (path: string) => relative(root, path).replaceAll("\\", "/");

const isRelative = (specifier: string) => specifier.startsWith(".");

const checkSpecifier = (module: string, line: number, specifier: string, source: string) => {
  if (!isRelative(specifier)) {
    return `${module}:${line}: pure module imports runtime dependency "${specifier}"${source}`;
  }

  const target = modulePath(resolve(root, dirname(module), specifier));
  if (!pureModules.includes(target as (typeof pureModules)[number])) {
    return `${module}:${line}: pure module depends on non-pure module "${specifier}"${source}`;
  }

  return null;
};

const locationOf = (source: string, specifier: string, from: number) => {
  const doubleQuoted = source.indexOf(`"${specifier}"`, from);
  const singleQuoted = source.indexOf(`'${specifier}'`, from);
  const index = [doubleQuoted, singleQuoted].filter((candidate) => candidate >= 0).sort((left, right) => left - right)[0] ?? 0;
  const lineStart = source.lastIndexOf("\n", index - 1) + 1;
  const lineEnd = source.indexOf("\n", index);
  return {
    index,
    line: source.slice(0, index).split("\n").length,
    text: source.slice(lineStart, lineEnd < 0 ? source.length : lineEnd).trim(),
  };
};

const violations: string[] = [];
for (const module of pureModules) {
  const source = await readFile(resolve(root, module), "utf8");
  let searchFrom = 0;
  for (const dependency of transpiler.scanImports(source)) {
    const location = locationOf(source, dependency.path, searchFrom);
    searchFrom = location.index + dependency.path.length;
    const violation = checkSpecifier(module, location.line, dependency.path, ` (${location.text})`);
    if (violation !== null) violations.push(violation);
  }
}

if (violations.length > 0) {
  console.error("Pure module dependency check failed:");
  for (const violation of violations) console.error(`- ${violation}`);
  process.exit(1);
}

console.log(`Pure module dependency check passed: ${pureModules.join(", ")}`);
