import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshStoreOptions } from "../src/mesh/store.js";

// Keep reason: data safety at capacity/TTL boundaries, and the recorded PR-B lock-hold
// regression. Observe the real publication entry points and forward every filesystem call.
const stores: Array<{ scratch: string; store: MeshStore }> = [];
const from = { id: "session:capacity-headroom", name: "capacity-headroom", kind: "main" as const };
const packet = (dedupeKey?: string) => ({ topic: "mesh.capacity-headroom", from, ...(dedupeKey ? { dedupeKey } : {}) });
const createStore = (maxDedupeReceipts: number, options: MeshStoreOptions = {}) => {
  const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-capacity-headroom-"));
  // Keep the sibling FIFO queue inside this test's private temporary directory too.
  const store = new MeshStore(path.join(scratch, "mesh"), 1024, 100, {
    stateBackend: "file", maxDedupeReceipts, ...options,
  });
  stores.push({ scratch, store });
  return store;
};
const receipt = (store: MeshStore, key: string, suffix = ".json") =>
  path.join(store.root, "event-receipts", createHash("sha256").update(key).digest("hex") + suffix);

const observe = (store: MeshStore, hintError?: string) => {
  const directory = path.join(store.root, "event-receipts");
  const held = () => fs.existsSync(path.join(store.root, ".lock"));
  const listings: boolean[] = [];
  const reads: Array<{ file: string; locked: boolean }> = [];
  const mutations: Array<{ operation: string; locked: boolean }> = [];
  const readdir = fs.readdirSync;
  const read = fs.readFileSync;
  const rename = fs.renameSync;
  const remove = fs.rmSync;
  const inReceipts = (file: unknown) => typeof file === "string" && file.startsWith(directory + path.sep);
  vi.spyOn(fs, "readdirSync").mockImplementation((...args) => {
    if (args[0] === directory) {
      const locked = held();
      listings.push(locked);
      if (!locked && hintError) throw Object.assign(new Error("hint unavailable"), { code: hintError });
    }
    return Reflect.apply(readdir, fs, args);
  });
  vi.spyOn(fs, "readFileSync").mockImplementation((...args) => {
    if (inReceipts(args[0])) reads.push({ file: String(args[0]), locked: held() });
    return Reflect.apply(read, fs, args);
  });
  vi.spyOn(fs, "renameSync").mockImplementation((...args) => {
    if (args.some(inReceipts)) mutations.push({ operation: "rename", locked: held() });
    return Reflect.apply(rename, fs, args);
  });
  vi.spyOn(fs, "rmSync").mockImplementation((...args) => {
    if (inReceipts(args[0])) mutations.push({ operation: "remove", locked: held() });
    return Reflect.apply(remove, fs, args);
  });
  return { listings, reads, mutations };
};

const expectSerializedReceiptWork = (observed: ReturnType<typeof observe>) => {
  expect(observed.reads.every(read => read.locked)).toBe(true);
  expect(observed.mutations.length).toBeGreaterThan(0);
  expect(observed.mutations.every(mutation => mutation.locked)).toBe(true);
};

afterEach(() => {
  vi.restoreAllMocks();
  for (const { scratch, store } of stores.splice(0)) {
    store.closeState();
    fs.rmSync(scratch, { recursive: true, force: true });
  }
});

describe("per-operation dedupe capacity headroom", () => {
  it("keeps a populated receipt-directory listing off .lock without reading or retiring its payloads", async () => {
    const store = createStore(100_000);
    const keys = Array.from({ length: 64 }, (_, index) => `bulk-${index}`);
    for (const key of keys) await store.publish(packet(key));
    const before = keys.map(key => fs.readFileSync(receipt(store, key), "utf8"));
    const oldFiles = new Set(keys.map(key => receipt(store, key)));
    const observed = observe(store);
    const next = await store.publish(packet("after-bulk"));
    expect(next.sequence).toBe(65);
    expect(observed.listings).toEqual([false]);
    expect(observed.reads.some(read => oldFiles.has(read.file))).toBe(false);
    expectSerializedReceiptWork(observed);
    vi.restoreAllMocks();
    expect(keys.map(key => fs.readFileSync(receipt(store, key), "utf8"))).toEqual(before);
    expect(await store.publish(packet("after-bulk"))).toEqual(next);
  });

  it.each(["receipt", "pending", "both"])("keeps existing-key %s recovery on the direct BASE path without any bulk count", async state => {
    const store = createStore(100_000);
    const original = await store.publish(packet("existing"));
    if (state !== "receipt") {
      fs.writeFileSync(receipt(store, "existing", ".pending.json"), JSON.stringify({
        dedupeKey: original.dedupeKey, reservedSequence: original.sequence, eventId: original.id, liveOffset: 0,
      }));
    }
    if (state === "pending") fs.rmSync(receipt(store, "existing"));
    const observed = observe(store);
    expect(await store.publish(packet("existing"))).toEqual(original);
    expect(observed.listings).toEqual([]);
    expect(observed.reads.every(read => read.locked)).toBe(true);
    expect(observed.mutations.every(mutation => mutation.locked)).toBe(true);
    expect(store.read().map(event => event.id)).toEqual([original.id]);
    vi.restoreAllMocks();
    expect(await store.publishBatch([packet("existing"), packet("existing")])).toEqual([original, original]);
  });

  it.each([
    { cap: 10_003, full: false, evicted: false }, // 1 name + 1 reserve + 10,000 < cap
    { cap: 10_002, full: true, evicted: false }, // exact headroom boundary
    { cap: 10_001, full: true, evicted: false },
    { cap: 1, full: true, evicted: true }, // BASE within-TTL oldest-first eviction
  ])("lists off-lock and uses the full serialized fallback iff needed (cap=$cap)", async ({ cap, full, evicted }) => {
    const store = createStore(cap);
    const original = await store.publish(packet("old"));
    const observed = observe(store);
    const next = await store.publish(packet("new"));
    expect(observed.listings).toEqual(full ? [false, true] : [false]);
    expect(next.sequence).toBe(original.sequence + 1);
    expect(fs.existsSync(receipt(store, "old"))).toBe(!evicted);
    expect(observed.reads.some(read => read.file === receipt(store, "old"))).toBe(evicted);
    expectSerializedReceiptWork(observed);
    vi.restoreAllMocks();
    expect(await store.publish(packet("new"))).toEqual(next);
    expect(store.read().map(event => event.id)).toEqual([original.id, next.id]);
  });

  it.each(["EIO", "ENOENT"])("falls back on an unknown/error hint (%s), not a presumed empty directory", async code => {
    const store = createStore(100_000);
    const original = await store.publish(packet("old"));
    const observed = observe(store, code);
    const next = await store.publish(packet("new"));
    expect(observed.listings).toEqual([false, true]);
    expect(next.sequence).toBe(original.sequence + 1);
    expectSerializedReceiptWork(observed);
    vi.restoreAllMocks();
    expect(await store.publish(packet("old"))).toEqual(original);
    expect(await store.publish(packet("new"))).toEqual(next);
  });

  it.each([
    { cap: 10_004, full: false },
    { cap: 10_003, full: true },
  ])("reserves all batch keyed additions, not just one or the unkeyed inputs (cap=$cap)", async ({ cap, full }) => {
    const store = createStore(cap);
    const original = await store.publish(packet("old"));
    const observed = observe(store);
    const inputs = [packet("batch-first"), packet(), packet("batch-last")];
    const events = await store.publishBatch(inputs);
    expect(events).toHaveLength(3);
    expect(events.map(event => event.sequence)).toEqual([2, 3, 4]);
    expect(observed.listings).toEqual(full ? [false, true, true] : [false]);
    expectSerializedReceiptWork(observed);
    vi.restoreAllMocks();
    expect(await store.publish(packet("old"))).toEqual(original);
    expect(await store.publish(packet("batch-first"))).toEqual(events[0]);
    expect(await store.publish(packet("batch-last"))).toEqual(events[2]);
    expect(store.read().map(event => event.id)).toEqual([original.id, ...events.map(event => event.id)]);
  });

  it("counts names conservatively, including a protected receipt/intent pair and an unrelated name", async () => {
    const store = createStore(10_004);
    const original = await store.publish(packet("protected"));
    fs.writeFileSync(receipt(store, "protected", ".pending.json"), JSON.stringify({
      dedupeKey: original.dedupeKey, reservedSequence: original.sequence, eventId: original.id, liveOffset: 0,
    }));
    fs.writeFileSync(path.join(store.root, "event-receipts", "unrelated-name"), "not receipt payload");
    const observed = observe(store);
    const next = await store.publish(packet("new"));
    // Three names + reserve + slack == cap, although BASE counts just one protected key.
    expect(observed.listings).toEqual([false, true]);
    expect(next.sequence).toBe(2);
    expect(fs.existsSync(receipt(store, "protected", ".pending.json"))).toBe(true);
    expect(observed.reads.some(read => read.file === receipt(store, "protected"))).toBe(false);
    expectSerializedReceiptWork(observed);
  });

  it("observes headroom afresh for the next operation instead of caching the hint", async () => {
    const store = createStore(10_004);
    await store.publish(packet("old"));
    const observed = observe(store);
    await store.publish(packet("second"));
    expect(observed.listings).toEqual([false]);
    fs.writeFileSync(path.join(store.root, "event-receipts", "intervening-name"), "ignored by BASE");
    observed.listings.length = 0;
    await store.publish(packet("third"));
    expect(observed.listings).toEqual([false, true]);
    expectSerializedReceiptWork(observed);
  });

  it("never adds receipt directory work to unkeyed single or batch publication", async () => {
    const store = createStore(1);
    const original = await store.publish(packet("at-cap"));
    const observed = observe(store, "EIO");
    const single = await store.publish(packet());
    const batch = await store.publishBatch([packet(), packet()]);
    expect([single.sequence, ...batch.map(event => event.sequence)]).toEqual([2, 3, 4]);
    expect(observed.listings).toEqual([]);
    expect(observed.reads).toEqual([]);
    expect(observed.mutations).toEqual([]);
    vi.restoreAllMocks();
    expect(await store.publish(packet("at-cap"))).toEqual(original);
  });

  it("leaves expired receipts alone on the headroom path, but still expires them under lock during live compaction", async () => {
    let now = 1_800_000_000_000;
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const store = createStore(100_000, {
      dedupeReceiptTtlMs: 1000, maxEventLogBytes: 4096, retainedEventLogBytes: 1025,
    });
    const old = await store.publish(packet("expired"));
    now = old.createdAt + 1000;
    const observed = observe(store);
    const young = await store.publish(packet("young"));
    expect(observed.listings).toEqual([false]);
    expect(fs.existsSync(receipt(store, "expired"))).toBe(true);
    expect(observed.reads.some(read => read.file === receipt(store, "expired"))).toBe(false);
    observed.listings.length = 0;
    for (let index = 0; index < 20 && !fs.existsSync(path.join(store.root, "generation")); index++) {
      await store.publish({ ...packet(), text: "x".repeat(600) });
    }
    expect(fs.existsSync(path.join(store.root, "generation"))).toBe(true);
    expect(observed.listings.length).toBeGreaterThanOrEqual(2); // intent settlement, then TTL pruning
    expect(observed.listings.every(locked => locked)).toBe(true);
    expect(observed.reads.some(read => read.file === receipt(store, "expired") && read.locked)).toBe(true);
    expect(fs.existsSync(receipt(store, "expired"))).toBe(false);
    expect(fs.existsSync(receipt(store, "young"))).toBe(true);
    expectSerializedReceiptWork(observed);
    vi.restoreAllMocks();
    expect(await store.publish(packet("young"))).toEqual(young);
    const replacement = await store.publish(packet("expired"));
    expect(replacement.id).not.toBe(old.id);
    expect(replacement.sequence).toBeGreaterThan(young.sequence);
  });
});
