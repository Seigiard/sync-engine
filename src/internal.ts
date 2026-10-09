import { Effect, type Scope } from "effect";
import { openSynchronization, type FreshnessFailed, type InitialPass, type OutputOwnershipFailed, type ScanFailed, type Synchronization } from "./index.ts";

export interface InternalSynchronization<W, E> extends Synchronization<W, E> {
  readonly commitFreshness: Effect.Effect<void, E>;
}

interface OpenSynchronizationInternal<W, E, R> {
  readonly deferCommit?: boolean;
  readonly beforeCommit?: (synchronization: InternalSynchronization<W, E | FreshnessFailed>) => Effect.Effect<void, E | FreshnessFailed, R>;
}

export const openSynchronizationWithHooks = openSynchronization as unknown as <W, E, R>(
  options: InitialPass<W, E, R>,
  internal?: OpenSynchronizationInternal<W, E, R>,
) => Effect.Effect<InternalSynchronization<W, E | FreshnessFailed>, E | FreshnessFailed | ScanFailed | OutputOwnershipFailed, R | Scope.Scope>;
