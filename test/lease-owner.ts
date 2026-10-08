import { Effect } from "effect";
import { acquireOutputTree } from "../src/index.ts";

await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  yield* Effect.acquireRelease(acquireOutputTree(process.argv[2]!), (release) => Effect.promise(release));
  console.log("output acquired");
  yield* Effect.never;
})));
