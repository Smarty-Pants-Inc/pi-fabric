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
  const summary = summarizeLockStats(root, readLockStats(root), { minutes: 2, now: Date.now() + 60_000 });
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
    await mesh.writeBatch({ identity: { id: "bridge:peer", name: "peer", kind: "main" }, ops: [], prepare: () => [] });
    await mesh.publish({ topic: "team.auth", from: identity, text: "one" });
    await mesh.publishBatch([{ topic: "team.bridge", from: identity, text: "two" }]);
    expect(await mesh.exclusive(() => 7)).toBe(7);
    await mesh.confirmWritable();
    const recorded = classes(root);
    expect(Object.fromEntries(Object.entries(recorded).map(([name, row]) => [name, row.n]))).toEqual({
      "put/delete": 2, writeBatch: 1, bridge: 2, publish: 1, custody: 1, "heartbeat/confirm": 1,
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
