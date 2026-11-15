export interface FreshnessIdentity {
  readonly resultKind: string;
  readonly sourcePaths: readonly string[];
}

export interface FreshnessSourceSnapshot {
  readonly path: string;
  readonly size: number | null;
  readonly mtimeMs: number | null;
  readonly digest: string | null;
}

export interface FreshnessSourceStamp {
  readonly processingVersion: string;
  readonly check: "metadata" | "content";
  readonly sources: readonly FreshnessSourceSnapshot[];
}

export function decodeFreshnessRecords(savedText: string): Map<string, string> {
  const saved: unknown = JSON.parse(savedText);
  if (!isObjectRecord(saved)) throw new Error("Invalid freshness state");

  const records = new Map<string, string>();
  for (const [key, value] of Object.entries(saved)) {
    decodeResultIdentity(key);
    if (typeof value !== "string") throw new Error("Invalid freshness record");
    records.set(key, value);
  }
  return records;
}

export function encodeFreshnessRecords(records: ReadonlyMap<string, string>): string {
  return JSON.stringify(Object.fromEntries(records));
}

export function decodeResultIdentity(key: string): FreshnessIdentity {
  const identity: unknown = JSON.parse(key);
  if (!isFreshnessIdentity(identity)) throw new Error("Invalid freshness record");
  return { resultKind: identity[0], sourcePaths: identity[1] };
}

export function encodeResultIdentity(descriptor: Pick<FreshnessIdentity, "resultKind" | "sourcePaths">): string {
  return JSON.stringify([descriptor.resultKind, descriptor.sourcePaths]);
}

export function encodeSourceStamp(stamp: FreshnessSourceStamp): string {
  return JSON.stringify([
    stamp.processingVersion,
    stamp.check,
    stamp.sources.map((source) => [source.path, source.size, source.mtimeMs, source.digest]),
  ]);
}

function isObjectRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function isFreshnessIdentity(value: unknown): value is [string, string[]] {
  return Array.isArray(value)
    && typeof value[0] === "string"
    && Array.isArray(value[1])
    && value[1].every((item: unknown) => typeof item === "string");
}
