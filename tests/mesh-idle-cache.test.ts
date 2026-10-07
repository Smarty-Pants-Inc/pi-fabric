// smarty-dev#2014: idle Mains re-parse and re-scan the shared mesh state far more than its
// changes require. These tests pin the observable contract of two minimal read-path savings:
//  1. confirmWritable revalidates physical identity without discarding unchanged bytes.
//     Legacy copied-marker changes still invalidate via nanosecond identity; adapters without
//     that identity retain the conservative fresh-read fallback (#164 Security S1).
//  2. prefix selections are reused within one parsed snapshot instead of rescanning every entry,
//     with the same invalidation and copy semantics as today.
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshIdentity, type MeshStoreOptions } from "../src/mesh/store.js";

const identity: MeshIdentity = { id: "session:test", name: "main", kind: "main", sessionId: "test" };
const LONG_CACHE_MS = 60 * 60 * 1000;
// Scratch under the repo's ignored-by-convention .local/, not the system temp.
const scratchBase = fileURLToPath(new URL("../.local/test-scratch/", import.meta.url));
const roots: string[] = [];

const newRoot = (): string => {
  fs.mkdirSync(scratchBase, { recursive: true });
  const root = fs.mkdtempSync(path.join(scratchBase, "mesh-idle-cache-"));
  roots.push(root);
  return root;
};
const storeAt = (root: string, options?: MeshStoreOptions): MeshStore => new MeshStore(root, 64 * 1024, 100, options);

afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

/** Reads of the shared state file (the parse path goes through fs.readFileSync). */
const spyStateReads = () => {
  const spy = vi.spyOn(fs, "readFileSync");
  return () => spy.mock.calls.filter(([file]) => String(file).endsWith(`${path.sep}state.json`)).length;
};

/** Whole-entries scans: Object.values/keys/entries over an object holding every key of the state. */
const spyEntryScans = (marker: string) => {
  const isEntries = (value: unknown): boolean =>
    typeof value === "object" && value !== null && !Array.isArray(value) && Object.prototype.hasOwnProperty.call(value, marker);
  const spies = [vi.spyOn(Object, "values"), vi.spyOn(Object, "keys"), vi.spyOn(Object, "entries")];
  const count = (): number => spies.reduce((total, spy) => total + spy.mock.calls.filter(([arg]) => isEntries(arg)).length, 0);
  return { count, reset: () => spies.forEach((spy) => spy.mockClear()) };
};

/** Moves Date.now forward without sleeping, to expire a read-cache window deterministically. */
const controllableClock = () => {
  const real = Date.now.bind(Date);
  let offset = 0;
  vi.spyOn(Date, "now").mockImplementation(() => real() + offset);
  return (milliseconds: number) => { offset += milliseconds; };
};

const seed = async (store: MeshStore, count = 40): Promise<void> => {
  await store.writeBatch({
    identity,
    ops: Array.from({ length: count }, (_, index) => [
      { kind: "put" as const, key: `p/${String(index).padStart(3, "0")}`, value: { n: index, tags: ["x"] } },
      { kind: "put" as const, key: `q/${String(index).padStart(3, "0")}`, value: { n: index } },
    ]).flat(),
  });
};

describe("MeshStore.confirmWritable revalidates canonical state", () => {
  it("revalidates an unchanged file after confirmation and preserves its snapshot", async () => {
    const root = newRoot();
    const writer = storeAt(root);
    const reader = storeAt(root, { readCacheMs: LONG_CACHE_MS });
    await writer.put({ key: "a/1", value: 1, identity });
    expect(reader.get("a/1")?.value).toBe(1);
    const token = reader.stateToken();
    await reader.confirmWritable();
    const reads = spyStateReads();
    expect(reader.get("a/1")?.value).toBe(1);
    expect(reader.listAll("a/").map((entry) => entry.key)).toEqual(["a/1"]);
    // Confirmation bypasses age, not the physical-generation gate. Unchanged nanosecond
    // metadata keeps the same canonical payload; copied-marker mutation probes below still
    // require a new snapshot, including adapters unable to supply high-resolution identity.
    expect(reads()).toBe(0);
    const revalidated = reader.stateToken();
    expect(revalidated).toBe(token);
    expect(reader.stateToken()).toBe(revalidated);
    expect(reads()).toBe(0);
  });

  it("sees another store's write made after confirmation at once, despite a long readCacheMs", async () => {
    const root = newRoot();
    const writer = storeAt(root);
    const reader = storeAt(root, { readCacheMs: LONG_CACHE_MS });
    await writer.put({ key: "a/1", value: 1, identity });
    reader.listAll();
    const token = reader.stateToken();
    await reader.confirmWritable();
    await writer.put({ key: "a/2", value: 2, identity });
    expect(reader.get("a/2")?.value).toBe(2);                    // confirmation ended the window
    expect(reader.listAll("a/").map((entry) => entry.key)).toEqual(["a/1", "a/2"]);
    expect(reader.stateToken()).not.toBe(token);
  });

  it("sees another store's write made before confirmation at once, despite a long readCacheMs", async () => {
    const root = newRoot();
    const writer = storeAt(root);
    const reader = storeAt(root, { readCacheMs: LONG_CACHE_MS });
    await writer.put({ key: "a/1", value: 1, identity });
    reader.listAll();
    await writer.put({ key: "a/2", value: 2, identity });
    expect(reader.get("a/2")).toBeUndefined();                  // inside the window: the recent parse
    await reader.confirmWritable();
    expect(reader.get("a/2")?.value).toBe(2);                   // confirmation: no older snapshot
    expect(reader.listAll("a/").map((entry) => entry.key)).toEqual(["a/1", "a/2"]);
  });

  it("sees its own write after confirmation at once", async () => {
    const root = newRoot();
    const reader = storeAt(root, { readCacheMs: LONG_CACHE_MS });
    await reader.put({ key: "a/1", value: 1, identity });
    await reader.confirmWritable();
    reader.listAll();
    await reader.put({ key: "a/2", value: 2, identity });
    expect(reader.listAll("a/").map((entry) => entry.key)).toEqual(["a/1", "a/2"]);
  });
});

describe("MeshStore prefix selections within one stateToken", () => {
  it("does not rescan every entry for repeated reads while the stateToken is unchanged", async () => {
    const root = newRoot();
    const reader = storeAt(root, { readCacheMs: LONG_CACHE_MS });
    await seed(reader);
    const token = reader.stateToken();
    expect(reader.listAll("p/")).toHaveLength(40);             // warm
    const scans = spyEntryScans("p/000");
    for (let index = 0; index < 10; index++) {
      expect(reader.listAll("p/")).toHaveLength(40);
      expect(reader.list("p/", 5).map((entry) => entry.key)).toEqual(["p/000", "p/001", "p/002", "p/003", "p/004"]);
      expect(reader.listAllShared("p/")).toHaveLength(40);
    }
    expect(reader.stateToken()).toBe(token);
    expect(scans.count()).toBe(0);
    // Explicit freshness revalidates the physical generation. With unchanged bytes the
    // snapshot and its selection memo remain valid; changed-owner probes below pin invalidation.
    for (let index = 0; index < 2; index++) {
      scans.reset();
      expect(reader.listAll("p/", { fresh: true })).toHaveLength(40);
      expect(scans.count()).toBe(0);
      expect(reader.stateToken({ fresh: true })).toBe(token);
      scans.reset();
      expect(reader.listAll("p/")).toHaveLength(40);
      expect(scans.count()).toBe(0);
    }
    // Another prefix may scan once, then is reused too; prefixes do not leak into each other.
    expect(reader.listAll("q/").map((entry) => entry.key)).toEqual(
      Array.from({ length: 40 }, (_, index) => `q/${String(index).padStart(3, "0")}`));
    scans.reset();
    for (let index = 0; index < 5; index++) {
      expect(reader.listAll("q/")).toHaveLength(40);
      expect(reader.listAll("")).toHaveLength(80);
      expect(reader.listAll("p/")).toHaveLength(40);
    }
    expect(scans.count()).toBeLessThanOrEqual(1);              // at most the first "" selection
  });

  it("reflects its own put, delete and writeBatch at once", async () => {
    const root = newRoot();
    const store = storeAt(root, { readCacheMs: LONG_CACHE_MS });
    await seed(store, 3);
    const keys = () => store.listAll("p/").map((entry) => entry.key);
    expect(keys()).toEqual(["p/000", "p/001", "p/002"]);
    await store.put({ key: "p/003", value: 3, identity });
    expect(keys()).toEqual(["p/000", "p/001", "p/002", "p/003"]);
    expect(store.list("p/", 100).map((entry) => entry.key)).toEqual(keys());
    await store.put({ key: "p/000", value: "changed", identity });
    expect(store.listAll("p/")[0]?.value).toBe("changed");
    expect(store.listAllShared("p/")[0]?.value).toBe("changed");
    await store.delete({ key: "p/001" });
    expect(keys()).toEqual(["p/000", "p/002", "p/003"]);
    expect(await store.delete({ key: "p/missing" })).toEqual({ deleted: false });
    expect(keys()).toEqual(["p/000", "p/002", "p/003"]);
    await store.writeBatch({ identity, ops: [
      { kind: "delete", key: "p/002" },
      { kind: "put", key: "p/004", value: 4 },
    ] });
    expect(keys()).toEqual(["p/000", "p/003", "p/004"]);
    expect(store.listAllShared("p/").map((entry) => entry.key)).toEqual(["p/000", "p/003", "p/004"]);
    expect(store.listAll("").map((entry) => entry.key).filter((key) => key.startsWith("p/"))).toEqual(keys());
  });

  it("reflects another store's write on a fresh read, and on a plain read once the window expires", async () => {
    const root = newRoot();
    const advance = controllableClock();
    const writer = storeAt(root);
    const reader = storeAt(root, { readCacheMs: 1_000 });
    await seed(writer, 2);
    expect(reader.listAll("p/").map((entry) => entry.key)).toEqual(["p/000", "p/001"]);
    await writer.put({ key: "p/002", value: 2, identity });
    expect(reader.listAll("p/")).toHaveLength(2);              // within the window: the recent parse
    expect(reader.listAll("p/", { fresh: true }).map((entry) => entry.key)).toEqual(["p/000", "p/001", "p/002"]);
    expect(reader.listAll("p/")).toHaveLength(3);              // plain reads share the fresh parse
    await writer.delete({ key: "p/000" });
    expect(reader.listAllShared("p/")).toHaveLength(3);
    advance(1_500);
    expect(reader.listAll("p/").map((entry) => entry.key)).toEqual(["p/001", "p/002"]);
    expect(reader.listAllShared("p/").map((entry) => entry.key)).toEqual(["p/001", "p/002"]);
    expect(reader.list("p/", 1).map((entry) => entry.key)).toEqual(["p/001"]);
  });

  it("drops a stale selection when its own write fails a compare-and-swap", async () => {
    const root = newRoot();
    const writer = storeAt(root);
    const reader = storeAt(root, { readCacheMs: LONG_CACHE_MS });
    await seed(writer, 1);
    expect(reader.listAll("p/")).toHaveLength(1);
    await writer.put({ key: "p/001", value: 1, identity });
    await expect(reader.put({ key: "p/001", value: "mine", identity, ifVersion: 0 })).rejects.toThrow("compare-and-swap");
    expect(reader.listAll("p/").map((entry) => entry.key)).toEqual(["p/000", "p/001"]);
  });

  it("keeps copy semantics: callers mutating get, list or listAll results do not change later reads", async () => {
    const root = newRoot();
    const store = storeAt(root, { readCacheMs: LONG_CACHE_MS });
    await seed(store, 3);
    const snapshot = JSON.stringify(store.listAll("p/"));
    for (let round = 0; round < 2; round++) {                 // second round reads any reused selection
      const all = store.listAll("p/");
      (all[0]!.value as { tags: string[] }).tags.push("mutated");
      all[1]!.key = "mutated";
      all.reverse();
      all.push({ ...all[0]!, key: "p/extra" });
      const page = store.list("p/", 2);
      (page[0]!.value as { n: number }).n = -1;
      page.length = 0;
      const one = store.get("p/002")!;
      (one.value as { n: number }).n = -2;
      one.version = -1;
      expect(JSON.stringify(store.listAll("p/"))).toBe(snapshot);
      expect(JSON.stringify(store.listAllShared("p/"))).toBe(snapshot);
      expect(store.list("p/", 100).map((entry) => entry.key)).toEqual(["p/000", "p/001", "p/002"]);
    }
  });

  it("returns listAllShared entries equal to listAll copies, read-only by contract", async () => {
    const root = newRoot();
    const store = storeAt(root, { readCacheMs: LONG_CACHE_MS });
    await seed(store, 3);
    const shared = store.listAllShared("p/");
    const copies = store.listAll("p/");
    expect(copies).toEqual(shared);
    expect(copies[0]).not.toBe(shared[0]);                      // listAll never hands out shared entries
    expect(store.get("p/000")).not.toBe(shared[0]);
  });
});
