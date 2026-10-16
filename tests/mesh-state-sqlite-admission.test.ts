import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshLockTicket, meshLockQueueDirectory } from "../src/mesh/lock-queue.js";
import { SqliteStateStore } from "../src/mesh/state-sqlite.js";

const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const identity = { id: "admission-test", name: "admission-test", kind: "agent" as const };
async function fixture(options: Parameters<typeof SqliteStateStore.open>[3] = {}) {
  const parent = fs.mkdtempSync(path.join(os.tmpdir(), "sqlite-admission-")); roots.push(parent);
  const root = path.join(parent, "mesh"); fs.mkdirSync(root);
  const store = await SqliteStateStore.open(root, 65536, 1000, { initialize: "create", fifoAfterMs: 0, ...options });
  return { store, root, file: path.join(root, "state.db") };
}

// Forward the actual native watch, including its callback. No mocked queue, transaction, clock, or BEGIN.
function observeWatches() {
  const original = fs.watch.bind(fs), targets: string[] = [], events: string[] = [];
  vi.spyOn(fs, "watch").mockImplementation(((target: fs.PathLike, options: unknown, listener: unknown) => {
    targets.push(String(target));
    const callback = (typeof options === "function" ? options : listener) as (event: string, name: string | Buffer | null) => void;
    const forward = (event: string, name: string | Buffer | null) => { events.push(String(target)); callback(event, name); };
    return typeof options === "function" ? original(target, forward) : original(target, options as fs.WatchOptions, forward);
  }) as typeof fs.watch);
  return { targets, events };
}

describe("SQLite non-head admission uses native predecessor notifications", () => {
  it("watches a real predecessor receipt and hands off on removal, never watching state.db for directory release", async () => {
    const { store, file } = await fixture();
    const predecessor = new MeshLockTicket(file, "11111111-1111-1111-1111-111111111111", 10000);
    expect(predecessor.mayContend()).toBe(true);
    const queue = meshLockQueueDirectory(file);
    const receipt = path.join(queue, fs.readdirSync(queue)[0]!);
    const observed = observeWatches();
    const release = setTimeout(() => predecessor.close(), 50);
    try {
      await store.put({ key: "handoff", value: 1, identity });
      expect(store.get("handoff")?.value).toBe(1);
      // Baseline polling succeeds but installs no watch: this assertion must be red before the fix.
      expect(observed.targets).toContain(receipt);
      expect(observed.events).toContain(receipt);
      expect(observed.targets).not.toContain(file);
      expect(store.stats().writesWaiting).toBe(0);
      expect(fs.readdirSync(queue)).toEqual([]);
    } finally { clearTimeout(release); predecessor.close(); store.close(); }
  });

  it("a FIFO head blocked by a real SQLite writer never installs the .lock directory-release watcher on state.db", async () => {
    const { store, file } = await fixture();
    const raw = new DatabaseSync(file); raw.exec("BEGIN IMMEDIATE");
    const observed = observeWatches();
    const release = setTimeout(() => raw.exec("COMMIT"), 60);
    try {
      await store.put({ key: "head", value: 2, identity });
      expect(store.get("head")?.value).toBe(2);
      expect(store.stats().fifoJoins).toBeGreaterThan(0);
      expect(observed.targets).not.toContain(file);
    } finally { clearTimeout(release); if (raw.isTransaction) raw.exec("ROLLBACK"); raw.close(); store.close(); }
  });

  it("non-head acquisition deadline is bounded and leaves no waiter", async () => {
    const { store, file } = await fixture({ lockTimeoutMs: 100 });
    const predecessor = new MeshLockTicket(file, "22222222-2222-2222-2222-222222222222", 10000);
    predecessor.mayContend();
    // The raw writer prevents short-budget admission fallback from succeeding at 80%.
    const raw = new DatabaseSync(file); raw.exec("BEGIN IMMEDIATE");
    const began = performance.now();
    try {
      await expect(store.put({ key: "deadline", value: 3, identity })).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
      expect(performance.now() - began).toBeLessThan(1000);
      expect(store.get("deadline")).toBeUndefined();
      expect(store.stats().writesWaiting).toBe(0);
    } finally { raw.exec("ROLLBACK"); raw.close(); predecessor.close(); store.close(); }
  });

  it("abort interrupts a non-head wait and cleans its receipt", async () => {
    const controller = new AbortController();
    const { store, file } = await fixture({ writeSignal: controller.signal });
    const predecessor = new MeshLockTicket(file, "33333333-3333-3333-3333-333333333333", 10000);
    predecessor.mayContend();
    const queue = meshLockQueueDirectory(file);
    const abort = setTimeout(() => controller.abort(new Error("admission cancelled")), 40);
    const began = performance.now();
    try {
      await expect(store.put({ key: "abort", value: 4, identity })).rejects.toThrow("admission cancelled");
      expect(performance.now() - began).toBeLessThan(1000);
      expect(store.stats().writesWaiting).toBe(0);
      expect(fs.readdirSync(queue)).toHaveLength(1); // only our deliberately held predecessor
    } finally { clearTimeout(abort); predecessor.close(); store.close(); }
  });
});
