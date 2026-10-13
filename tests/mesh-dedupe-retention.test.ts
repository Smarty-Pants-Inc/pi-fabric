import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MESH_ARCHIVE_CONFIG } from "../src/mesh/archive.js";
import { MeshStore, type MeshStoreOptions } from "../src/mesh/store.js";
import { MeshDedupeStoreFullError } from "../src/mesh.js";

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

  it.each([false, true])("never prunes an expired at-cap receipt while its pending intent is unresolved (archive=%s)", async archived => {
    const time = clock();
    const store = createStore({ dedupeReceiptTtlMs: 1000, maxDedupeReceipts: 2 }, archived);
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

  it.each([false, true])("fails closed at the pending-intent cap even after byte-triggered settlement fails (archive=%s)", async archived => {
    const store = createStore({ maxDedupeReceipts: 2 }, archived);
    const directory = path.join(store.root, "event-receipts");
    const rename = fs.renameSync.bind(fs);
    const failure = vi.spyOn(fs, "renameSync").mockImplementation((source, target) => {
      if (String(target).startsWith(directory + path.sep) && /^[a-f0-9]{64}\.json$/.test(path.basename(String(target)))) {
        throw new Error("receipt settlement unavailable");
      }
      rename(source, target);
    });
    for (const key of ["stranded-first", "stranded-second"]) {
      await expect(store.publish(packet(key))).rejects.toThrow("receipt settlement unavailable");
    }
    const originals = store.read().filter(event => event.dedupeKey);
    expect(originals).toHaveLength(2);
    expect(fs.readdirSync(directory).filter(name => name.endsWith(".pending.json"))).toHaveLength(2);
    await expect(compact(store)).rejects.toThrow("receipt settlement unavailable");
    expect(generation(store)).toBe(0);
    const before = fs.readFileSync(path.join(store.root, "events.jsonl"));
    const sequence = store.latestSequence();
    const restarted = new MeshStore(store.root, 1024, 100, { maxDedupeReceipts: 2 });
    for (let index = 0; index < 4; index++) {
      const rejected = await restarted.publish(packet(`overflow-${index}`)).catch(error => error);
      expect(fs.readFileSync(path.join(store.root, "events.jsonl"))).toEqual(before);
      expect(store.latestSequence()).toBe(sequence);
      expect(rejected).toBeInstanceOf(MeshDedupeStoreFullError);
      expect(rejected).toMatchObject({ code: "FABRIC_MESH_DEDUPE_STORE_FULL", retryable: true });
      expect(fs.existsSync(receipt(store, `overflow-${index}`, ".pending.json"))).toBe(false);
    }
    // The cap never blocks ordinary publication, even while settlement is unavailable.
    const roomy = new MeshStore(store.root, 1024, 100, { maxEventLogBytes: 64 * 1024, maxDedupeReceipts: 2 });
    expect((await roomy.publish({ topic: "mesh.retention", from, text: "unkeyed" })).sequence).toBe(sequence + 1);
    failure.mockRestore();
    for (const original of originals) {
      expect(await roomy.publish(packet(original.dedupeKey!))).toEqual(original);
      expect(await roomy.publish(packet(original.dedupeKey!))).toEqual(original);
    }
    expect(fs.readdirSync(directory).filter(name => name.endsWith(".pending.json"))).toEqual([]);
    await compact(store);
    const next = await store.publish(packet("admitted-after-settlement"));
    expect(next.sequence).toBeGreaterThan(sequence);
    expect(fs.readdirSync(directory).filter(name => /^[a-f0-9]{64}\.json$/.test(name))).toHaveLength(2);
  });

  it("counts a receipt and its protected intent once, and evicts only settled receipts before append", async () => {
    const store = createStore({ maxDedupeReceipts: 2 });
    const protectedEvent = await store.publish(packet("protected"));
    const settled = await store.publish(packet("settled"));
    const pending = receipt(store, "protected", ".pending.json");
    fs.writeFileSync(pending, JSON.stringify({ dedupeKey: "protected", reservedSequence: protectedEvent.sequence,
      eventId: protectedEvent.id, liveOffset: 0 }));
    const next = await store.publish(packet("new-slot"));
    expect(next.sequence).toBe(settled.sequence + 1);
    expect(fs.existsSync(receipt(store, "protected"))).toBe(true);
    expect(fs.existsSync(pending)).toBe(true);
    expect(fs.existsSync(receipt(store, "settled"))).toBe(false);
    expect(await store.publish(packet("protected"))).toEqual(protectedEvent);
    expect(await store.publish(packet("new-slot"))).toEqual(next);
  });

  it("a receipt-confirmation cleanup cannot unlink a newer same-key reservation", async () => {
    const store = createStore({ maxDedupeReceipts: 1 });
    const original = await store.publish(packet("cleanup-cas"));
    const file = receipt(store, "cleanup-cas");
    const inode = fs.statSync(file);
    const pending = receipt(store, "cleanup-cas", ".pending.json");
    const replacement = { dedupeKey: "cleanup-cas", reservedSequence: original.sequence + 1,
      eventId: "new-reservation", liveOffset: fs.statSync(path.join(store.root, "events.jsonl")).size };
    const close = fs.closeSync.bind(fs);
    let replaced = false;
    vi.spyOn(fs, "closeSync").mockImplementation(fd => {
      const opened = fs.fstatSync(fd);
      close(fd);
      if (!replaced && opened.ino === inode.ino && opened.dev === inode.dev &&
          !fs.existsSync(path.join(store.root, ".lock"))) {
        // Model eviction + a new reservation after old receipt confirmation, before cleanup CAS.
        fs.rmSync(file);
        fs.writeFileSync(pending, JSON.stringify(replacement));
        replaced = true;
      }
    });
    expect(await store.publish(packet("cleanup-cas"))).toEqual(original);
    expect(replaced).toBe(true);
    expect(JSON.parse(fs.readFileSync(pending, "utf8"))).toEqual(replacement);
    const before = fs.readFileSync(path.join(store.root, "events.jsonl"));
    await expect(store.publish(packet("overflow-after-cleanup"))).rejects.toBeInstanceOf(MeshDedupeStoreFullError);
    expect(fs.readFileSync(path.join(store.root, "events.jsonl"))).toEqual(before);
  });

  it.each([false, true])("a delayed finalizer cannot resurrect an evicted receipt or overwrite a replacement intent (replace=%s)", async replace => {
    const time = clock();
    const store = createStore({ dedupeReceiptTtlMs: 1000, maxDedupeReceipts: 1 });
    let release!: () => void;
    let queued!: () => void;
    const barrierQueued = new Promise<void>(resolve => { queued = resolve; });
    const immediate = vi.spyOn(globalThis, "setImmediate").mockImplementationOnce(callback => {
      release = () => callback();
      queued();
      return {} as NodeJS.Immediate;
    });
    const publishing = store.publish(packet("slow-finalizer"));
    let replacement: Promise<Awaited<typeof publishing>> | undefined;
    let released = false;
    try {
      await barrierQueued;
      const original = store.read()[0]!;
      expect(fs.existsSync(receipt(store, "slow-finalizer", ".pending.json"))).toBe(true);
      time.set(original.createdAt + 1000);
      await compact(new MeshStore(store.root, 1024, 100, {
        maxEventLogBytes: 4096, retainedEventLogBytes: 1025, dedupeReceiptTtlMs: 1000, maxDedupeReceipts: 1,
      }));
      expect(fs.existsSync(receipt(store, "slow-finalizer", ".pending.json"))).toBe(false);
      expect(fs.existsSync(receipt(store, "slow-finalizer"))).toBe(false);
      if (replace) {
        const append = fs.appendFileSync.bind(fs);
        let notify!: () => void;
        const appended = new Promise<void>(resolve => { notify = resolve; });
        vi.spyOn(fs, "appendFileSync").mockImplementation((file, data, options) => {
          append(file, data, options);
          if (file === path.join(store.root, "events.jsonl")) notify();
        });
        replacement = store.publish(packet("slow-finalizer"));
        await appended;
        const intent = JSON.parse(fs.readFileSync(receipt(store, "slow-finalizer", ".pending.json"), "utf8"));
        expect(intent.eventId).not.toBe(original.id);
        expect(intent.reservedSequence).toBeGreaterThan(original.sequence);
      }
      release(); released = true;
      expect(await publishing).toEqual(original);
      if (!replace) expect(fs.existsSync(receipt(store, "slow-finalizer"))).toBe(false);
      const latest = replacement ? await replacement : await store.publish(packet("replacement-slot"));
      expect(latest.id).not.toBe(original.id);
      expect(JSON.parse(fs.readFileSync(receipt(store, replace ? "slow-finalizer" : "replacement-slot"), "utf8"))).toEqual(latest);
      expect(fs.existsSync(receipt(store, "slow-finalizer", ".pending.json"))).toBe(false);
      expect(fs.readdirSync(path.join(store.root, "event-receipts")).filter(name => /^[a-f0-9]{64}\.json$/.test(name))).toHaveLength(1);
    } finally {
      if (!released) release?.();
      await publishing.catch(() => undefined);
      await replacement?.catch(() => undefined);
      immediate.mockRestore();
    }
  });

  it.each(["dedupeReceiptTtlMs", "maxDedupeReceipts"] as const)("rejects invalid %s bounds", option => {
    for (const value of [0, -1, 1.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1]) {
      expect(() => createStore({ [option]: value })).toThrow(`${option} must be a positive safe integer`);
    }
  });
});
