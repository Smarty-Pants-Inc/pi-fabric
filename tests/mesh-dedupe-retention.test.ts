import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MESH_ARCHIVE_CONFIG } from "../src/mesh/archive.js";
import { MeshStore, type MeshStoreOptions } from "../src/mesh/store.js";

const roots: string[] = [];
const from = { id: "session:retention", name: "retention", kind: "main" as const };
const day = 24 * 60 * 60 * 1000;
const packet = (dedupeKey: string) => ({ topic: "mesh.retention", from, dedupeKey });
const receipt = (store: MeshStore, key: string, suffix = ".json") =>
  path.join(store.root, "event-receipts", createHash("sha256").update(key).digest("hex") + suffix);
const createStore = (options: MeshStoreOptions = {}, archived = false) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-dedupe-retention-"));
  roots.push(root);
  if (archived) {
    const dir = path.join(root, "archive");
    fs.mkdirSync(dir);
    fs.writeFileSync(path.join(root, MESH_ARCHIVE_CONFIG), JSON.stringify({ version: 1, dir }));
  }
  return new MeshStore(root, 1024, 100, {
    maxEventLogBytes: 4096, retainedEventLogBytes: 1025, ...options,
  });
};
const clock = () => {
  let now = 1_800_000_000_000;
  vi.spyOn(Date, "now").mockImplementation(() => now);
  return { set: (value: number) => { now = value; } };
};
const generation = (store: MeshStore) => {
  const file = path.join(store.root, "generation");
  return fs.existsSync(file) ? Number(fs.readFileSync(file, "utf8")) : 0;
};
// Drive the real byte-triggered pass, not a private helper or a new maintenance timer.
const compact = async (store: MeshStore) => {
  const before = generation(store);
  for (let index = 0; index < 20; index++) {
    await store.publish({ topic: "mesh.retention", from, text: "x".repeat(600) });
    if (generation(store) > before) return;
  }
  throw new Error("Expected a byte-triggered event-log compaction");
};

afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("durable mesh dedupe receipt retention", () => {
  it.each([false, true])("expires a key at its configured TTL and allows republishing (archive=%s)", async archived => {
    const time = clock();
    const store = createStore({ dedupeReceiptTtlMs: 1000 }, archived);
    const original = await store.publish(packet("expires"));
    time.set(original.createdAt + 1000);
    // Expiry is enforced by compaction, not a timer or a scan during key lookup.
    expect(await store.publish(packet("expires"))).toEqual(original);
    await compact(store);
    expect(fs.existsSync(receipt(store, "expires"))).toBe(false);
    const restarted = new MeshStore(store.root, 1024, 100, { dedupeReceiptTtlMs: 1000 });
    const next = await restarted.publish(packet("expires"));
    expect(next.id).not.toBe(original.id);
    expect(next.sequence).toBeGreaterThan(original.sequence);
    expect(await restarted.publish(packet("expires"))).toEqual(next);
  });

  it("defaults to seven days and measures age from publication, not receipt mtime", async () => {
    const time = clock();
    const store = createStore();
    const old = await store.publish(packet("seven-days"));
    time.set(old.createdAt + day);
    const young = await store.publish(packet("six-days"));
    // A recently touched/recovered receipt cannot extend its publication TTL.
    const touched = new Date(old.createdAt + 7 * day);
    fs.utimesSync(receipt(store, "seven-days"), touched, touched);
    time.set(old.createdAt + 7 * day);
    await compact(store);
    expect(fs.existsSync(receipt(store, "seven-days"))).toBe(false);
    expect(fs.existsSync(receipt(store, "six-days"))).toBe(true);
    expect(await store.publish(packet("six-days"))).toEqual(young);
  });

  it.each([false, true])("evicts oldest first above the cap, keeps newest, and allows an evicted key (archive=%s)", async archived => {
    clock(); // Equal timestamps exercise the durable sequence tie-break, not directory order.
    const store = createStore({ maxDedupeReceipts: 2 }, archived);
    const first = await store.publish(packet("first"));
    const second = await store.publish(packet("second"));
    await compact(store);
    expect(fs.existsSync(receipt(store, "first"))).toBe(true); // At the cap: no eviction.
    expect(fs.existsSync(receipt(store, "second"))).toBe(true);
    const third = await store.publish(packet("third"));
    await compact(store);
    expect(fs.existsSync(receipt(store, "first"))).toBe(false);
    expect(fs.readdirSync(path.join(store.root, "event-receipts")).filter(name => /^[a-f0-9]{64}\.json$/.test(name))).toHaveLength(2);
    expect(await store.publish(packet("second"))).toEqual(second);
    expect(await store.publish(packet("third"))).toEqual(third);
    const next = await store.publish(packet("first"));
    expect(next.id).not.toBe(first.id);
    expect(next.sequence).toBeGreaterThan(third.sequence);
  });

  it("compaction preserves all younger receipts below the cap, including one just inside the TTL", async () => {
    const time = clock();
    const store = createStore({ dedupeReceiptTtlMs: 1000, maxDedupeReceipts: 10 });
    const first = await store.publish(packet("young-first"));
    time.set(first.createdAt + 500);
    const second = await store.publish(packet("young-second"));
    const before = ["young-first", "young-second"].map(key => fs.readFileSync(receipt(store, key), "utf8"));
    time.set(first.createdAt + 999);
    await compact(store);
    expect(["young-first", "young-second"].map(key => fs.readFileSync(receipt(store, key), "utf8"))).toEqual(before);
    expect(await store.publish(packet("young-first"))).toEqual(first);
    expect(await store.publish(packet("young-second"))).toEqual(second);
  });

  it.each([false, true])("never prunes an expired over-cap receipt while its pending intent is unresolved (archive=%s)", async archived => {
    const time = clock();
    const store = createStore({ dedupeReceiptTtlMs: 1000, maxDedupeReceipts: 1 }, archived);
    const original = await store.publish(packet("pending"));
    await store.publish(packet("newer"));
    const file = receipt(store, "pending");
    const before = fs.readFileSync(file, "utf8");
    const pending = receipt(store, "pending", ".pending.json");
    fs.writeFileSync(pending, JSON.stringify({
      dedupeKey: original.dedupeKey, reservedSequence: original.sequence,
      eventId: original.id, liveOffset: 0,
      ...(archived ? { archiveDir: path.join(store.root, "archive") } : {}),
    }));
    time.set(original.createdAt + 1000);
    const remove = fs.rmSync.bind(fs);
    const unavailable = vi.spyOn(fs, "rmSync").mockImplementation((target, options) => {
      if (target === pending) throw new Error("pending cleanup unavailable");
      remove(target, options);
    });
    await expect(compact(store)).rejects.toThrow("pending cleanup unavailable");
    expect(generation(store)).toBe(0);
    expect(fs.existsSync(pending)).toBe(true);
    expect(fs.readFileSync(file, "utf8")).toBe(before);
    expect(fs.existsSync(receipt(store, "newer"))).toBe(true);
    unavailable.mockRestore();
    // Ordinary retry still confirms the original identity and completes pending recovery.
    expect(await store.publish(packet("pending"))).toEqual(original);
    expect(fs.existsSync(pending)).toBe(false);
  });

  it.each(["dedupeReceiptTtlMs", "maxDedupeReceipts"] as const)("rejects invalid %s bounds", option => {
    for (const value of [0, -1, 1.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1]) {
      expect(() => createStore({ [option]: value })).toThrow(`${option} must be a positive safe integer`);
    }
  });
});
