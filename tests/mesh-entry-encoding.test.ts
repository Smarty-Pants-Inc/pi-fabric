// smarty-dev#2395: primary JSON and the namespace index share commit-local entry encodings.
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshIdentity, type MeshStateEntry } from "../src/mesh/store.js";

const identity: MeshIdentity = { id: "session:encoding", name: "雪😀", kind: "main" };
const roots: string[] = [];
const root = () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-entry-encoding-"));
  roots.push(directory);
  return directory;
};
afterEach(() => {
  vi.restoreAllMocks();
  for (const directory of roots.splice(0)) fs.rmSync(directory, { recursive: true, force: true });
});
const stringify = JSON.stringify;

/** Count entries traversed by any stringify, including an envelope's implicit traversal. */
const countEntryEncodings = () => {
  const counts = new Map<string, number>();
  const count = (entry: unknown) => {
    if (typeof entry !== "object" || entry === null || !("key" in entry) || !("updatedBy" in entry)) return;
    const key = String(entry.key);
    counts.set(key, (counts.get(key) ?? 0) + 1);
  };
  vi.spyOn(JSON, "stringify").mockImplementation(((value: unknown, ...rest: unknown[]) => {
    count(value);
    if (typeof value === "object" && value !== null && "entries" in value) {
      for (const entry of Object.values(value.entries as Record<string, unknown>)) count(entry);
    }
    return (stringify as (...args: unknown[]) => string | undefined)(value, ...rest);
  }) as typeof JSON.stringify);
  return counts;
};

const assertDiskEncoding = (directory: string) => {
  const text = fs.readFileSync(path.join(directory, "state.json"), "utf8");
  const state = JSON.parse(text) as { entries: Record<string, MeshStateEntry>; readGeneration: string };
  // Exact JSON.stringify representation: escaping, UTF-8, envelope and entry property order.
  expect(text).toBe(stringify(state));
  expect(text.startsWith('{"readGeneration":')).toBe(true);
  const signal = JSON.parse(fs.readFileSync(path.join(directory, "state.read-signal.json"), "utf8"));
  expect(signal.generation).toBe(state.readGeneration);
  const groups = new Map<string, MeshStateEntry[]>();
  for (const key of Object.keys(state.entries).sort()) {
    const second = key.indexOf("/", key.indexOf("/") + 1);
    if (key.indexOf("/") < 0 || second < 0) continue;
    const namespace = key.slice(0, second + 1);
    let group = groups.get(namespace);
    if (!group) groups.set(namespace, group = []);
    group.push(state.entries[key]!);
  }
  const expected = Object.fromEntries([...groups].map(([namespace, entries]) => {
    const hash = createHash("sha256");
    for (const entry of entries) hash.update(`${stringify(entry)}\n`);
    return [namespace, hash.digest("base64")];
  }));
  expect(signal.namespaces).toEqual(expected);
  return state;
};

describe("MeshStore one-pass entry encoding", () => {
  it("encodes retained entries once for put, delete and batch while keeping exact index bytes", async () => {
    const directory = root();
    const store = new MeshStore(directory, 64 * 1024, 100);
    const keys = ["test/beta/z", "test/beta/A", "test/alpha/a", "solo", "single/segment"];
    await store.writeBatch({ identity, ops: keys.map((key) => ({ kind: "put", key,
      value: { nested: [{ "10": "雪", "2": "😀", quote: '"\\\n\u0000', braces: "{[}]" }], bool: true, nil: null },
    })) });
    assertDiskEncoding(directory);
    const counts = countEntryEncodings();
    await store.put({ key: "other/heartbeats/a", value: 1, identity });
    for (const key of keys) expect(counts.get(key), key).toBe(1);
    assertDiskEncoding(directory);
    counts.clear();
    await store.delete({ key: "other/heartbeats/a" });
    for (const key of keys) expect(counts.get(key), key).toBe(1);
    expect(counts.has("other/heartbeats/a")).toBe(false);
    assertDiskEncoding(directory);
    counts.clear();
    await store.writeBatch({ identity, ops: [{ kind: "put", key: "test/beta/z", value: [0, "雪😀"] },
      { kind: "delete", key: "test/beta/A" }, { kind: "put", key: "other/heartbeats/b", value: false }] });
    for (const key of [...keys.filter((key) => key !== "test/beta/A"), "other/heartbeats/b"]) {
      expect(counts.get(key), key).toBe(1);
    }
    expect(counts.has("test/beta/A")).toBe(false);
    assertDiskEncoding(directory);
  });

  it("preserves legacy envelope fields, entry order, escaped keys and empty namespaces", async () => {
    const directory = root();
    const keys = ["test/beta/z", 'test/beta/quote"\\雪', "10", "2", "one/segment"];
    const entries = Object.fromEntries(keys.map((key, index) => [key, {
      updatedBy: identity, value: { n: index, text: "\ud800\udfff\u2028" }, key, updatedAt: 1, version: index + 1,
    }]));
    fs.writeFileSync(path.join(directory, "state.json"), stringify({
      legacy: { marker: "unchanged" }, format: 1, entries, versions: {}, highWater: keys.length,
    }));
    const store = new MeshStore(directory, 64 * 1024, 100);
    await store.put({ key: "test/new/a", value: 0, identity });
    const state = assertDiskEncoding(directory);
    expect(state).toHaveProperty("legacy", { marker: "unchanged" });
    for (const key of keys) expect(state.entries[key]).toEqual(entries[key]);
    await store.writeBatch({ identity, ops: Object.keys(state.entries).map((key) => ({ kind: "delete" as const, key }))
      .filter((op) => /^[a-zA-Z0-9][a-zA-Z0-9._:/-]*$/.test(op.key)) });
    // The escaped legacy key cannot be deleted through the public key validator; hashes still cover it.
    assertDiskEncoding(directory);
    const empty = root();
    const emptyStore = new MeshStore(empty, 64 * 1024, 100);
    await emptyStore.put({ key: "solo", value: null, identity });
    await emptyStore.delete({ key: "solo" });
    expect(assertDiskEncoding(empty).entries).toEqual({});
  });

  it("rehashes retained entries after a legacy writer copies both marker and stat", async () => {
    let now = 1700000000000;
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const directory = root();
    const writer = new MeshStore(directory, 64 * 1024, 100);
    const reader = new MeshStore(directory, 64 * 1024, 100, { readCacheMs: 2000 });
    const key = "test/legacy/a";
    await writer.put({ key, value: "old", identity });
    expect(reader.listAll("test/legacy/")[0]?.value).toBe("old");
    const file = path.join(directory, "state.json");
    const stat = fs.statSync.bind(fs);
    const frozen = stat(file);
    const state = JSON.parse(fs.readFileSync(file, "utf8"));
    const marker = state.readGeneration;
    state.entries[key].value = "new"; // same version, clock, marker and byte size
    fs.writeFileSync(file, stringify(state));
    vi.spyOn(fs, "statSync").mockImplementation(((target: fs.PathLike, ...rest: unknown[]) =>
      String(target) === file ? frozen : (stat as (...args: unknown[]) => fs.Stats)(target, ...rest)) as typeof fs.statSync);
    await writer.put({ key: "other/heartbeats/a", value: 1, identity });
    expect(assertDiskEncoding(directory).readGeneration).not.toBe(marker);
    now += 2001;
    expect(reader.listAll("test/legacy/")[0]?.value).toBe("new");
  });

  it("enforces the state byte cap on UTF-8 payloads before either file changes", async () => {
    vi.spyOn(Date, "now").mockReturnValue(1700000000000);
    const directory = root();
    const store = new MeshStore(directory, 64 * 1024, 100);
    const request = { key: "test/utf8/a", value: "雪😀".repeat(20), identity };
    await store.put(request);
    const before = fs.readFileSync(path.join(directory, "state.json"));
    const capped = new MeshStore(directory, Math.floor(before.length / 2), 100, { maxStateBytes: before.length });
    await expect(capped.put(request)).resolves.toBeDefined(); // same byte size, despite multibyte characters
    const canonical = fs.readFileSync(path.join(directory, "state.json"));
    const signal = fs.readFileSync(path.join(directory, "state.read-signal.json"));
    await expect(capped.put({ ...request, value: request.value + "雪" })).rejects.toThrow("Fabric mesh state exceeds");
    expect(fs.readFileSync(path.join(directory, "state.json"))).toEqual(canonical);
    expect(fs.readFileSync(path.join(directory, "state.read-signal.json"))).toEqual(signal);
  });
});
