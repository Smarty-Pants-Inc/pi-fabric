import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as atomicWrite from "../src/core/atomic-write.js";
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

const observe = (store: MeshStore, hintError?: string, afterNativeCount?: () => void) => {
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
    const names = Reflect.apply(readdir, fs, args);
    // Match the native regression's pause: actual names are captured BEFORE this callback.
    if (args[0] === directory && !held()) afterNativeCount?.();
    return names;
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

// POSIX-only: the advisory sequence headroom FD is intentionally not used on Windows.
describe.skipIf(process.platform === "win32")("per-operation dedupe capacity headroom", () => {
  // PR #1927 P1-1: unsupported namespace preparation leaves a fresh directory absent.
  it.each(["single", "batch"])("uses only the BASE locked listing for a missing receipt directory with unsupported namespace preparation (%s)", async mode => {
    const namespace = vi.spyOn(atomicWrite, "namespaceBarrierAvailable").mockReturnValue(false);
    const store = createStore(100_000);
    const directory = path.join(store.root, "event-receipts");
    expect(fs.existsSync(directory)).toBe(false);
    const observed = observe(store);
    const publish = async (key: string) => mode === "single"
      ? store.publish(packet(key)) : (await store.publishBatch([packet(key)]))[0]!;

    const first = await publish("fresh");
    expect(namespace).toHaveBeenCalled();
    expect(observed.listings).toEqual([true]);
    expect(fs.existsSync(directory)).toBe(true);
    expect(first.sequence).toBe(1);
    expectSerializedReceiptWork(observed);

    observed.listings.length = 0;
    expect(await publish("fresh")).toEqual(first);
    expect(observed.listings).toEqual([]);

    const next = await publish("next");
    expect(observed.listings).toEqual([false]);
    expect(next.sequence).toBe(2);
    expectSerializedReceiptWork(observed);
    observed.listings.length = 0;
    expect(await publish("next")).toEqual(next);
    expect(observed.listings).toEqual([]);
    expect(store.read().map(event => event.id)).toEqual([first.id, next.id]);
  });

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

  it.each([
    { state: "advanced", mode: "single" }, { state: "advanced", mode: "batch" },
    { state: "same-value replacement", mode: "single" }, { state: "same-value replacement", mode: "batch" },
    { state: "missing", mode: "single" }, { state: "missing", mode: "batch" },
    { state: "malformed", mode: "single" }, { state: "malformed", mode: "batch" },
  ])("rejects a $state counter after the real off-lock count ($mode)", async ({ state, mode }) => {
    const store = createStore(100_000);
    const original = await store.publish(packet("old"));
    const counter = path.join(store.root, "sequence");
    let changed = false;
    const observed = observe(store, undefined, () => {
      if (changed) return;
      changed = true;
      if (state === "missing") fs.rmSync(counter);
      else atomicWrite.writeFileAtomic(counter, state === "advanced" ? "2" : state === "malformed" ? "broken" : "1");
    });
    const inputs = mode === "single" ? [packet("new")] : [packet("first"), packet(), packet("last")];
    const events = mode === "single" ? [await store.publish(inputs[0]!)] : await store.publishBatch(inputs);
    expect(changed).toBe(true);
    // An invalid grant is not refreshed after the first own reservation in a batch.
    expect(observed.listings).toEqual(mode === "single" ? [false, true] : [false, true, true]);
    const firstSequence = state === "advanced" ? 3 : 2;
    expect(events.map(event => event.sequence)).toEqual(inputs.map((_, index) => firstSequence + index));
    expectSerializedReceiptWork(observed);
    vi.restoreAllMocks();
    expect(await store.publish(packet("old"))).toEqual(original);
    for (let index = 0; index < inputs.length; index++) {
      if (inputs[index]!.dedupeKey) expect(await store.publish(inputs[index]!)).toEqual(events[index]);
    }
    expect(store.read().map(event => event.id)).toEqual([original.id, ...events.map(event => event.id)]);
  });

  it.each([
    { state: "missing", text: undefined },
    { state: "invalid JSON", text: "broken" },
    { state: "string", text: '"0"' },
    { state: "negative", text: "-1" },
    { state: "fractional", text: "1.5" },
    { state: "unsafe", text: "9007199254740992" },
  ])("does not turn a $state sequence into a known zero headroom stamp", async ({ text }) => {
    const store = createStore(100_000);
    const original = await store.publish(packet("old"));
    const counter = path.join(store.root, "sequence");
    if (text === undefined) fs.rmSync(counter);
    else atomicWrite.writeFileAtomic(counter, text);
    const observed = observe(store);
    const next = await store.publish(packet("new"));
    expect(observed.listings).toEqual([true]);
    expect(next.sequence).toBe(2);
    expectSerializedReceiptWork(observed);
    vi.restoreAllMocks();
    expect(await store.publish(packet("old"))).toEqual(original);
    expect(await store.publish(packet("new"))).toEqual(next);
  });

  it("accepts a real safe-integer zero stamp, unlike unknown sequence state", async () => {
    const store = createStore(100_000);
    await store.publish(packet("old"));
    atomicWrite.writeFileAtomic(path.join(store.root, "sequence"), "0");
    const observed = observe(store);
    const next = await store.publish(packet("new"));
    expect(observed.listings).toEqual([false]);
    expect(next.sequence).toBe(2);
    expectSerializedReceiptWork(observed);
  });

  it("falls back when opening the advisory sequence fails", async () => {
    const store = createStore(100_000);
    await store.publish(packet("old"));
    const counter = path.join(store.root, "sequence"), open = fs.openSync;
    let fail = true;
    vi.spyOn(fs, "openSync").mockImplementation((...args) => {
      if (args[0] === counter && !fs.existsSync(path.join(store.root, ".lock")) && fail) {
        fail = false;
        throw Object.assign(new Error("counter unavailable"), { code: "EIO" });
      }
      return Reflect.apply(open, fs, args);
    });
    const observed = observe(store);
    const next = await store.publish(packet("new"));
    expect(fail).toBe(false);
    expect(observed.listings).toEqual([true]);
    expect(next.sequence).toBe(2);
    expectSerializedReceiptWork(observed);
  });

  it("uses locked admission when counter validation itself encounters EIO", async () => {
    const store = createStore(100_000);
    await store.publish(packet("old"));
    const counter = path.join(store.root, "sequence"), lstat = fs.lstatSync;
    let counted = false, failed = false;
    vi.spyOn(fs, "lstatSync").mockImplementation((...args) => {
      if (args[0] === counter && counted && !failed && fs.existsSync(path.join(store.root, ".lock"))) {
        failed = true;
        throw Object.assign(new Error("stamp validation unavailable"), { code: "EIO" });
      }
      return Reflect.apply(lstat, fs, args);
    });
    const observed = observe(store, undefined, () => { counted = true; });
    const next = await store.publish(packet("new"));
    expect(failed).toBe(true);
    expect(observed.listings).toEqual([false, true]);
    expect(next.sequence).toBe(2);
    expectSerializedReceiptWork(observed);
  });

  it.each(["present", "EIO", "ENOTDIR"])("requires proven canonical lock absence after capture (%s)", async state => {
    const store = createStore(100_000);
    await store.publish(packet("old"));
    const lock = path.join(store.root, ".lock"), lstat = fs.lstatSync;
    let denied = false;
    vi.spyOn(fs, "lstatSync").mockImplementation((...args) => {
      if (args[0] === lock && !denied) {
        denied = true;
        if (state !== "present") throw Object.assign(new Error("idle state unknown"), { code: state });
        return Reflect.apply(lstat, fs, [path.join(store.root, "sequence"), ...args.slice(1)]);
      }
      return Reflect.apply(lstat, fs, args);
    });
    const observed = observe(store);
    const next = await store.publish(packet("new"));
    expect(denied).toBe(true);
    expect(observed.listings).toEqual([true]);
    expect(next.sequence).toBe(2);
    expectSerializedReceiptWork(observed);
  });

  it("invalidates a validated grant before repairing an actual torn live tail", async () => {
    const store = createStore(100_000);
    const original = await store.publish(packet("old"));
    fs.appendFileSync(path.join(store.root, "events.jsonl"), '{"partial":');
    const observed = observe(store);
    const next = await store.publish(packet("new"));
    expect(observed.listings).toEqual([false, true]);
    expect(next.sequence).toBe(2);
    expect(store.read().map(event => event.id)).toEqual([original.id, next.id]);
    expectSerializedReceiptWork(observed);
  });

  it.each(["success", "preparation error", "aborted"])("closes its pinned observation on %s", async outcome => {
    const store = createStore(100_000);
    await store.publish(packet("old"));
    const counter = path.join(store.root, "sequence"), open = fs.openSync, close = fs.closeSync;
    const pinned = new Set<number>();
    let captures = 0, releases = 0;
    vi.spyOn(fs, "openSync").mockImplementation((...args) => {
      const fd = Reflect.apply(open, fs, args);
      if (args[0] === counter && args[1] === "r" && !fs.existsSync(path.join(store.root, ".lock"))) {
        pinned.add(fd); captures++;
      }
      return fd;
    });
    vi.spyOn(fs, "closeSync").mockImplementation((...args) => {
      const result = Reflect.apply(close, fs, args);
      if (pinned.delete(args[0])) releases++;
      return result;
    });
    const controller = new AbortController();
    if (outcome === "aborted") controller.abort();
    const publication = store.publish({ ...packet("new"), signal: controller.signal,
      ...(outcome === "preparation error" ? { topic: "invalid topic" } : {}) });
    if (outcome === "success") expect((await publication).sequence).toBe(2);
    else await expect(publication).rejects.toThrow();
    expect(captures).toBe(1);
    expect(releases).toBe(captures);
    expect(pinned.size).toBe(0);
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

it("skips the advisory headroom hint on Windows and replays a fresh receipt", async () => {
  const store = createStore(100_000);
  const seeded = await store.publish(packet("seed"));
  const directory = path.join(store.root, "event-receipts");
  const counter = path.join(store.root, "sequence");
  expect(fs.readdirSync(directory)).toContain(path.basename(receipt(store, "seed")));
  expect(Number(fs.readFileSync(counter, "utf8"))).toBe(seeded.sequence);

  const originalPlatform = Object.getOwnPropertyDescriptor(process, "platform");
  if (!originalPlatform) throw new Error("process.platform descriptor is unavailable");
  Object.defineProperty(process, "platform", { ...originalPlatform, value: "win32" });
  try {
    const observed = observe(store);
    const lock = path.join(store.root, ".lock");
    const sequenceOpensOutsideLock: unknown[] = [];
    const open = fs.openSync;
    vi.spyOn(fs, "openSync").mockImplementation((...args) => {
      if (args[0] === counter && !fs.existsSync(lock)) sequenceOpensOutsideLock.push(args[1]);
      return Reflect.apply(open, fs, args);
    });

    const fresh = await store.publish(packet("fresh-windows"));
    expect(sequenceOpensOutsideLock).toEqual([]);
    expect(observed.listings).toEqual([true]);
    expect(fresh).toMatchObject({ id: expect.any(String), sequence: seeded.sequence + 1, dedupeKey: "fresh-windows" });
    expect(fresh.id).not.toBe(seeded.id);
    expect(store.read()).toEqual([seeded, fresh]);

    observed.listings.length = 0;
    expect(await store.publish(packet("fresh-windows"))).toEqual(fresh);
    expect(observed.listings).toEqual([]);
    expect(sequenceOpensOutsideLock).toEqual([]);
    expect(store.read()).toEqual([seeded, fresh]);
  } finally {
    Object.defineProperty(process, "platform", originalPlatform);
  }
});
