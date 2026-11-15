import { readFile } from "node:fs/promises";
import { dirname, relative, resolve } from "node:path";

const root = resolve(import.meta.dir, "..");
const pureModules = ["src/live-machine.ts", "src/source-snapshot.ts", "src/freshness-codec.ts"] as const;
const importLine = /^\s*(?:import|export)\s+(?!type\b)(?:[^;"'\n]+\s+from\s+)?["']([^"']+)["']/;
const dynamicImport = /\b(?:import|require)\s*\(\s*["']([^"']+)["']\s*\)/g;

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

const violations: string[] = [];
for (const module of pureModules) {
  const source = await readFile(resolve(root, module), "utf8");
  const lines = source.split("\n");

  lines.forEach((text, index) => {
    const line = index + 1;
    const match = text.match(importLine);
    if (match?.[1] !== undefined) {
      const violation = checkSpecifier(module, line, match[1], ` (${text.trim()})`);
      if (violation !== null) violations.push(violation);
    }

    for (const dynamic of text.matchAll(dynamicImport)) {
      const specifier = dynamic[1];
      if (specifier !== undefined) {
        const violation = checkSpecifier(module, line, specifier, ` (${text.trim()})`);
        if (violation !== null) violations.push(violation);
      }
    }
  });
}

if (violations.length > 0) {
  console.error("Pure module dependency check failed:");
  for (const violation of violations) console.error(`- ${violation}`);
  process.exit(1);
}

console.log(`Pure module dependency check passed: ${pureModules.join(", ")}`);
