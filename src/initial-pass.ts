import { Effect } from "effect";
import type { FreshnessOptions, WorkInput } from "./freshness.ts";
import type { SourceEntry } from "./source-snapshot.ts";
import type { WorkOptions, WorkScheduler } from "./work.ts";

export type { SourceEntry } from "./source-snapshot.ts";

export interface InitialPlan<W, E, R> {
  /**
   * Application-defined minimum: runs first. A typed failure is reported after the minimum drains
   * and before the remaining work starts.
   * It is not repeated by later passes; list work that must also run again in `work`.
   */
  readonly minimum?: readonly W[];
  /** The remaining required work. It gates completion, not the minimum. */
  readonly work: readonly W[];
  /** Runs after all initial work and its required cascades succeed. Later live passes still publish after typed work failures. */
  readonly publish: Effect.Effect<void, E, R>;
}

export interface InitialPass<W, E, R> extends WorkOptions<W, E, R> {
  readonly sourcePath: string;
  readonly outputPath: string;
  readonly statePath?: string;
  /** Application-owned source policy. Excluded paths are skipped before filesystem traversal. */
  readonly includeSource?: (relativePath: string) => boolean;
  readonly declare: (entries: readonly SourceEntry[]) => Effect.Effect<InitialPlan<W, E, R>, E, R>;
  /**
   * Runs once per initial pass, after the declared minimum finished without errors and before the remaining work starts.
   * A live session runs an initial pass on every opening, so a reopen after a failed attempt runs it again.
   */
  readonly onMinimum?: Effect.Effect<void, E, R>;
  readonly freshness?: FreshnessOptions<W>;
}

export interface Synchronization<W, E> extends WorkScheduler<W, E> {
  readonly submit: (work: readonly W[], input?: WorkInput) => Effect.Effect<void, E>;
}
