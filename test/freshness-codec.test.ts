import { expect, test } from "bun:test";
import {
  decodeFreshnessRecords,
  encodeFreshnessRecords,
  encodeResultIdentity,
  encodeSourceStamp,
} from "../src/freshness-codec.ts";

test("the pure codec decodes the literal pre-refactor retained record entries", () => {
  // #given a literal record written by the pre-refactor persistence format
  const records = decodeFreshnessRecords(
    "{\"[\\\"text\\\",[\\\"note.txt\\\"]]\":\"[\\\"v1\\\",\\\"metadata\\\",[[\\\"note.txt\\\",15,1577836800000,null]]]\"}",
  );
  // #when the codec exposes its retained entries to freshness consumers
  const entries = [...records.entries()];
  // #then each legacy key and value has its independently specified meaning
  expect(entries).toEqual([[
    "[\"text\",[\"note.txt\"]]",
    "[\"v1\",\"metadata\",[[\"note.txt\",15,1577836800000,null]]]",
  ]]);
});

test("the pure codec encodes independently constructed legacy record entries", () => {
  // #given entries constructed without decoding persisted state
  const records = new Map([[
    "[\"text\",[\"note.txt\"]]",
    "[\"v1\",\"metadata\",[[\"note.txt\",15,1577836800000,null]]]",
  ]]);
  // #when the records are persisted
  const encoded = encodeFreshnessRecords(records);
  // #then their bytes retain the pre-refactor object protocol
  expect(encoded).toBe(
    "{\"[\\\"text\\\",[\\\"note.txt\\\"]]\":\"[\\\"v1\\\",\\\"metadata\\\",[[\\\"note.txt\\\",15,1577836800000,null]]]\"}",
  );
});

test("the pure codec keeps result identity independent of version and outputs", () => {
  // #given a result descriptor's identity fields
  const descriptor = { resultKind: "text", sourcePaths: ["notes/one.txt", "notes/two.txt"] };
  // #when its retained identity is encoded
  const identity = encodeResultIdentity(descriptor);
  // #then it uses the pre-refactor result-kind and source-path tuple
  expect(identity).toBe("[\"text\",[\"notes/one.txt\",\"notes/two.txt\"]]");
});

test.each([
  {
    name: "metadata",
    stamp: {
      processingVersion: "v1",
      check: "metadata" as const,
      sources: [{ path: "note.txt", size: 15, mtimeMs: 1577836800000, digest: null }],
    },
    expected: "[\"v1\",\"metadata\",[[\"note.txt\",15,1577836800000,null]]]",
  },
  {
    name: "content",
    stamp: {
      processingVersion: "v2",
      check: "content" as const,
      sources: [{
        path: "note.txt",
        size: 5,
        mtimeMs: 1577836800000,
        digest: "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
      }],
    },
    expected: "[\"v2\",\"content\",[[\"note.txt\",5,1577836800000,\"2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824\"]]]",
  },
  {
    name: "absent source",
    stamp: {
      processingVersion: "v1",
      check: "metadata" as const,
      sources: [{ path: "removed.txt", size: null, mtimeMs: null, digest: null }],
    },
    expected: "[\"v1\",\"metadata\",[[\"removed.txt\",null,null,null]]]",
  },
])("the pure codec preserves the $name source stamp", ({ stamp, expected }) => {
  // #given an independently specified persisted source snapshot
  // #when its stamp is encoded
  const encoded = encodeSourceStamp(stamp);
  // #then metadata, content, and absence retain their protocol meaning
  expect(encoded).toBe(expected);
});

test.each([
  { name: "top-level array", saved: "[]" },
  { name: "non-JSON identity key", saved: "{\"not json\":\"[\\\"v1\\\",\\\"metadata\\\",[]]\"}" },
  { name: "non-string identity source path", saved: "{\"[\\\"text\\\",[1]]\":\"[\\\"v1\\\",\\\"metadata\\\",[]]\"}" },
  { name: "non-string retained stamp", saved: "{\"[\\\"text\\\",[\\\"note.txt\\\"]]\":false}" },
])("the pure codec rejects malformed retained state: $name", ({ saved }) => {
  // #given a malformed persisted state shape
  // #when the codec decodes it
  const decode = () => decodeFreshnessRecords(saved);
  // #then the invalid protocol is rejected before it can become a retained record
  expect(decode).toThrow();
});
