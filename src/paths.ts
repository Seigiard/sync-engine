import { isAbsolute, relative, sep } from "node:path";

export function isPathWithin(root: string, path: string): boolean {
  const within = relative(root, path);
  return within === "" || (within !== ".." && !within.startsWith(`..${sep}`) && !isAbsolute(within));
}

export function pathsOverlap(left: string, right: string): boolean {
  return isPathWithin(left, right) || isPathWithin(right, left);
}
