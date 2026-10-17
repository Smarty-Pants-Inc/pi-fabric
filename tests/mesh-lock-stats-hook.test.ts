import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { lockStatsHost, readLockStats, summarizeLockStats } from "../src/mesh/commit-stats.js";
import type { MeshIdentity } from "../src/mesh/store.js";

const lockKey = Symbol.for("pi-fabric.mesh.lock-stats");
const registry = globalThis as typeof globalThis & { [lockKey]?: { flush(): void; dispose(): void; stats: unknown } };
const temps: string[] = [];
const temp = (): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-lock-stats-hook-"));
  temps.push(root);
  return root;
};
const identity: MeshIdentity = { id: "main-1", name: "main", kind: "main" };
const classes = (root: string) => {
  registry[lockKey]!.flush();
  // The window ends one minute past now: a record stamped by a skewed test clock (below) can land in the
  // next wall-clock minute when the test runs near a minute boundary (smarty-dev#7651).
  const summary = summarizeLockStats(root, readLockStats(root), { minutes: 3, now: Date.now() + 120_000 });
  return Object.fromEntries(summary.classes.map(row => [row.lockClass, row]));
};
// The test environment disables the recorder (PI_FABRIC_LOCK_STATS=0): load a store module that
// captured the production default instead.
const loadStore = async (setting: string): Promise<typeof import("../src/mesh/store.js")> => {
  registry[lockKey]?.dispose();
  delete registry[lockKey];
  vi.stubEnv("PI_FABRIC_LOCK_STATS", setting);
  vi.resetModules();
  return import("../src/mesh/store.js");
};
let store: typeof import("../src/mesh/store.js");

beforeAll(async () => {
  store = await loadStore("");
  expect(registry[lockKey]!.stats).toBeDefined();
});
afterEach(() => {
  for (const root of temps.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("MeshStore lock-timing hook", () => {
  it("records every acquisition under its caller class with wait and hold", async () => {
    const root = temp();
    const mesh = new store.MeshStore(root, 64 * 1024, 100);
    await mesh.put({ key: "presence/a", value: { ok: true }, identity });
    await mesh.delete({ key: "presence/a" });
    await mesh.writeBatch({ identity, ops: [{ kind: "put", key: "topology/hosts/a", value: 1 }] });
    // The class comes from the explicit option of the bridge's own call site, never the identity text.
    for (let spoof = 0; spoof < 2; spoof++) {
      await mesh.writeBatch({ identity: { id: "bridge:spoof", name: "spoof", kind: "main" }, ops: [], prepare: () => [] });
    }
    await mesh.writeBatch({ identity: { id: "main-1", name: "peer", kind: "main" }, lockClass: "bridge", ops: [], prepare: () => [] });
    await mesh.publish({ topic: "team.auth", from: identity, text: "one" });
    await mesh.publishBatch([{ topic: "team.bridge", from: identity, text: "two" }]);
    expect(await mesh.exclusive(() => 7)).toBe(7);
    await mesh.confirmWritable();
    const recorded = classes(root);
    expect(Object.fromEntries(Object.entries(recorded).map(([name, row]) => [name, row.n]))).toEqual({
      "put/delete": 2, writeBatch: 3, bridge: 2, publish: 1, custody: 1, "heartbeat/confirm": 1,
    });
    for (const row of Object.values(recorded)) {
      expect(row.holdMs).toBeGreaterThan(0);
      expect(row.waitMaxMs).toBeGreaterThanOrEqual(0);
      expect(row.timeouts + row.tries).toBe(0);
    }
    const file = path.join(root, "lock-stats", `${lockStatsHost()}-${process.pid}.json`);
    // Only the stats file is new under lock-stats; no lock or temp file is left behind.
    expect(fs.readdirSync(path.join(root, "lock-stats"))).toEqual([path.basename(file)]);
  });

  it("counts a full-budget timeout apart from a failed bounded try, and a throwing operation as a hold", async () => {
    const root = temp();
    const mesh = new store.MeshStore(root, 64 * 1024, 100, { lockTimeoutMs: 100 });
    await mesh.exclusive(() => undefined);
    // A live owner (this process) holds the lock, so it is never stale.
    fs.mkdirSync(path.join(root, ".lock"));
    fs.writeFileSync(path.join(root, ".lock", "owner"), `held\n${process.pid}\n${Date.now()}\n`);
    await expect(mesh.exclusive(() => undefined)).rejects.toBeInstanceOf(store.MeshLockTimeoutError);
    await expect(mesh.withTryLock(() => mesh.confirmWritable(), 0)).rejects.toBeInstanceOf(store.MeshLockTimeoutError);
    fs.rmSync(path.join(root, ".lock"), { recursive: true, force: true });
    await expect(mesh.exclusive(() => { throw new Error("boom"); })).rejects.toThrow("boom");
    const recorded = classes(root);
    expect(recorded.custody).toMatchObject({ n: 2, timeouts: 1, tries: 0 });
    expect(recorded["heartbeat/confirm"]).toMatchObject({ n: 0, timeouts: 0, tries: 1 });
  });

  it("counts an ordinary write whose budget ran out before custody as a timeout, never a try", async () => {
    const root = temp();
    const mesh = new store.MeshStore(root, 64 * 1024, 100, { lockTimeoutMs: 100 });
    await mesh.put({ key: "presence/a", value: 0, identity }); // state exists: the snapshot path
    fs.mkdirSync(path.join(root, ".lock"));
    fs.writeFileSync(path.join(root, ".lock", "owner"), `held\n${process.pid}\n${Date.now()}\n`);
    // Snapshot preparation (its off-lock staging write) spends the whole budget: #withLock gets 0.
    const atomic = await import("../src/core/atomic-write.js");
    const realNow = Date.now.bind(Date);
    let skew = 0;
    const clock = vi.spyOn(Date, "now").mockImplementation(() => realNow() + skew);
    const original = atomic.writeFileAtomic;
    const staging = vi.spyOn(atomic, "writeFileAtomic").mockImplementation((...args: Parameters<typeof original>) => {
      skew += 1_000;
      return original(...args);
    });
    try {
      await expect(mesh.put({ key: "presence/a", value: 1, identity })).rejects.toBeInstanceOf(store.MeshLockTimeoutError);
      expect(staging).toHaveBeenCalled();
    } finally {
      staging.mockRestore();
      clock.mockRestore();
    }
    expect(classes(root)["put/delete"]).toMatchObject({ n: 1, timeouts: 1, tries: 0 });
    // A withTryLock scope and an explicit zero-wait exclusive are still bounded tries.
    await expect(mesh.withTryLock(() => mesh.put({ key: "presence/a", value: 2, identity }), 0))
      .rejects.toBeInstanceOf(store.MeshLockTimeoutError);
    await expect(mesh.exclusive(() => undefined, 0)).rejects.toBeInstanceOf(store.MeshLockTimeoutError);
    const recorded = classes(root);
    expect(recorded["put/delete"]).toMatchObject({ n: 1, timeouts: 1, tries: 1 });
    expect(recorded.custody).toMatchObject({ n: 0, timeouts: 0, tries: 1 });
  });

  it.each(["before the lock", "under the lock"])("a snapshot that keeps changing %s exhausts validation retries without a failed-acquisition count", async (where) => {
    const root = temp();
    const mesh = new store.MeshStore(root, 64 * 1024, 100, { lockTimeoutMs: 100 });
    await mesh.put({ key: "presence/a", value: 0, identity }); // state exists: the snapshot path
    const state = path.join(root, "state.json");
    const replace = (): void => { fs.copyFileSync(state, `${state}.swap`); fs.renameSync(`${state}.swap`, state); };
    const atomic = await import("../src/core/atomic-write.js");
    const realNow = Date.now.bind(Date);
    let skew = 0;
    const clock = vi.spyOn(Date, "now").mockImplementation(() => realNow() + skew);
    const original = atomic.writeFileAtomic;
    // Each off-lock preparation spends budget; a concurrent writer replaces the state before or after admission.
    const staging = vi.spyOn(atomic, "writeFileAtomic").mockImplementation((...args: Parameters<typeof original>) => {
      skew += 40;
      const result = original(...args);
      if (where === "before the lock") replace();
      return result;
    });
    const mkdir = fs.mkdirSync;
    const lockDir = vi.spyOn(fs, "mkdirSync").mockImplementation(((file: fs.PathLike, options?: unknown) => {
      const result = mkdir(file, options as undefined);
      if (where === "under the lock" && String(file) === path.join(root, ".lock")) replace();
      return result;
    }) as typeof fs.mkdirSync);
    let error: unknown;
    try {
      error = await mesh.put({ key: "presence/a", value: 1, identity }).catch((caught: unknown) => caught);
    } finally {
      lockDir.mockRestore();
      staging.mockRestore();
      clock.mockRestore();
    }
    expect(error).toBeInstanceOf(store.MeshSnapshotRetriesExhaustedError);
    expect(error).not.toBeInstanceOf(store.MeshLockTimeoutError);
    const refusal = error as InstanceType<typeof store.MeshSnapshotRetriesExhaustedError>;
    expect(refusal).toMatchObject({ code: "FABRIC_MESH_SNAPSHOT_RETRIES_EXHAUSTED" });
    expect(refusal.message.startsWith("FABRIC_MESH_SNAPSHOT_RETRIES_EXHAUSTED: ")).toBe(true);
    expect(refusal.message).not.toMatch(/Timed out|0 attempts/);
    expect(refusal.attempts).toBeGreaterThan(1); // validation attempts, not lock contention
    // Only snapshots invalidated under custody add successful admissions; neither path adds a timeout.
    const recorded = classes(root)["put/delete"]!;
    expect(recorded).toMatchObject({ n: 1 + (where === "before the lock" ? 0 : refusal.attempts), timeouts: 0, tries: 0 });
    expect(JSON.parse(fs.readFileSync(state, "utf8")).entries["presence/a"].value).toBe(0);
  });

  it("releases the native lock before recording, so bookkeeping never delays a contender", async () => {
    const root = temp();
    const mesh = new store.MeshStore(root, 64 * 1024, 100);
    const stats = registry[lockKey]!.stats as { acquired(...args: unknown[]): void };
    const record = stats.acquired.bind(stats);
    const acquired = vi.spyOn(stats, "acquired").mockImplementation((...args) => {
      expect(fs.existsSync(path.join(root, ".lock"))).toBe(false);
      record(...args);
    });
    try {
      await mesh.exclusive(() => undefined);
      expect(acquired).toHaveBeenCalledOnce();
    } finally { acquired.mockRestore(); }
  });

  it("records no failed try or timeout for a cold protocol-2 pending incarnation", async () => {
    const root = temp();
    const seed = new store.MeshStore(root, 64 * 1024, 100);
    await seed.put({ key: "presence/a", value: 0, identity }); // state exists: the snapshot path
    const atomic = await import("../src/core/atomic-write.js");
    let resolve!: (value: string | undefined) => void;
    const ready = new Promise<string | undefined>(done => { resolve = done; });
    const spy = vi.spyOn(atomic, "ownProcessIncarnation").mockReturnValue(ready);
    try {
      const cold = new store.MeshStore(root, 64 * 1024, 100, { lockProtocol: 2 });
      for (let attempt = 0; attempt < 2; attempt++) {
        await expect(cold.withTryLock(() => cold.put({ key: "presence/a", value: 1, identity }), 0))
          .rejects.toBeInstanceOf(store.MeshIncarnationPendingError);
      }
      await expect(cold.withTryLock(() => cold.confirmWritable(), 0)).rejects.toBeInstanceOf(store.MeshIncarnationPendingError);
      let recorded = classes(root);
      expect(recorded["put/delete"]).toMatchObject({ n: 1, tries: 0, timeouts: 0 });
      expect(recorded["heartbeat/confirm"]).toBeUndefined();
      resolve(undefined);
      await cold.put({ key: "presence/a", value: 2, identity });
      await cold.confirmWritable();
      recorded = classes(root);
      expect(recorded["put/delete"]).toMatchObject({ n: 2, tries: 0, timeouts: 0 });
      expect(recorded["heartbeat/confirm"]).toMatchObject({ n: 1, tries: 0, timeouts: 0 });
    } finally {
      resolve(undefined);
      spy.mockRestore();
    }
  });

  it("PI_FABRIC_LOCK_STATS=0 records nothing", async () => {
    const saved = registry[lockKey];
    try {
      const disabled = await loadStore("0");
      expect(registry[lockKey]!.stats).toBeUndefined();
      const root = temp();
      const mesh = new disabled.MeshStore(root, 64 * 1024, 100);
      await mesh.put({ key: "presence/a", value: 1, identity });
      await mesh.exclusive(() => undefined);
      registry[lockKey]!.flush();
      saved?.flush();
      expect(fs.existsSync(path.join(root, "lock-stats"))).toBe(false);
    } finally {
      delete registry[lockKey];
      if (saved) registry[lockKey] = saved;
      vi.unstubAllEnvs();
    }
  });
});
