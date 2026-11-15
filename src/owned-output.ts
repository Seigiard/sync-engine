import { Deferred, Effect, Exit, type Scope } from "effect";
import { resolve } from "node:path";
import { acquireResolvedOutputTree, type OutputIdentity, OutputOwnershipFailed, resolveOutputIdentity } from "./ownership.ts";
import { pathsOverlap } from "./paths.ts";

export interface OwnedOutput extends OutputIdentity {
  readonly admit: <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>;
  readonly check: <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>;
  readonly close: Effect.Effect<void>;
}

export function openOwnedOutput(
  sourcePath: string,
  outputPath: string,
  statePath?: string,
): Effect.Effect<OwnedOutput, OutputOwnershipFailed, Scope.Scope> {
  return Effect.gen(function* () {
    const identity = yield* resolveOutputIdentity(outputPath, statePath);
    yield* Effect.tryPromise({
      try: async () => {
        const source = resolve(sourcePath);
        if (pathsOverlap(source, identity.outputPath)) throw new Error("Source and output trees must be disjoint");
        if (pathsOverlap(source, identity.statePath)) throw new Error("Source and state trees must be disjoint");
      },
      catch: (cause) => new OutputOwnershipFailed({ path: outputPath, cause }),
    }).pipe(Effect.uninterruptible);

    yield* Effect.acquireRelease(acquireResolvedOutputTree(identity), (release) => Effect.promise(release));
    let closed = false;
    let inFlight = 0;
    let idle = Deferred.makeUnsafe<void>();
    Deferred.doneUnsafe(idle, Effect.void);

    const admit = <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
      Effect.uninterruptibleMask((restore) => Effect.gen(function* () {
        if (closed) return yield* Effect.interrupt;
        if (inFlight === 0) idle = Deferred.makeUnsafe<void>();
        inFlight += 1;
        const exit = yield* Effect.exit(restore(effect));
        inFlight -= 1;
        if (inFlight === 0) Deferred.doneUnsafe(idle, Effect.void);
        if (Exit.isFailure(exit)) return yield* Effect.failCause(exit.cause);
        return exit.value;
      }));

    const check = <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
      Effect.suspend(() => closed ? Effect.interrupt : effect);

    const close = Effect.uninterruptible(Effect.gen(function* () {
      closed = true;
      yield* Deferred.await(idle);
    }));

    return { ...identity, admit, check, close };
  });
}
