import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { StateProjector } from "../src/mesh/state-projector.js";
import { SqliteStateStore } from "../src/mesh/state-sqlite.js";
import type { MeshIdentity } from "../src/mesh/store.js";

// smarty-dev#6100 round 2: every complete checkpoint lets the next writer restart the WAL and fsync its header under
// the writer lock, so the routine checkpoints are bounded by volume (autocheckpoint) and age, not run every second.
// Each checkpoint is timed in stats() and logged (debug: every checkpoint; info: one summary line per minute).
const identity: MeshIdentity = { id: "tester", name: "tester", kind: "agent" };
const roots: string[] = [];
const stores: SqliteStateStore[] = [];
const projectors: StateProjector[] = [];
afterEach(async () => {
  vi.restoreAllMocks();
  for (const projector of projectors.splice(0)) await projector.stop();
  for (const store of stores.splice(0)) try { store.close(); } catch { /* closed by the test */ }
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const tempRoot = (label: string): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), `pi-fabric-cadence-${label}-`));
  roots.push(root);
  return root;
};
const openStore = async (root: string, options: Parameters<typeof SqliteStateStore.open>[3] = {}): Promise<SqliteStateStore> => {
  const store = await SqliteStateStore.open(root, 64 * 1024, 1_000, { initialize: "create", ...options });
  stores.push(store);
  return store;
};
const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));
const setImmediateTurn = (): Promise<void> => new Promise((resolve) => setImmediate(resolve));

describe("SQLite mesh checkpoint cadence and timing (smarty-dev#6100 round 2)", () => {
  it("syncs by age after a commit: one PASSIVE on the writer's path once unsynced frames are older than walSyncMaxAgeMs", async () => {
    const root = tempRoot("age");
    const store = await openStore(root, { walSyncMaxAgeMs: 200, checkpointLog: "off" });
    await store.put({ key: "k/0", value: 0, identity });
    await setImmediateTurn();
    // Frames are younger than the bound: no checkpoint yet (the autocheckpoint is far away at 4,000 pages).
    expect(store.stats().checkpointTiming.passive.count).toBe(0);
    await sleep(1_100); // past walSyncMaxAgeMs and the 1 s per-store check spacing
    await store.put({ key: "k/1", value: 1, identity });
    await setImmediateTurn();
    await setImmediateTurn();
    const timing = store.stats().checkpointTiming;
    expect(timing.passive.count).toBe(1);
    expect(timing.onWriterPath).toBe(1);
    expect(timing.passive.maxMs).toBeGreaterThanOrEqual(0);
    expect(store.stats().lastCheckpoint).toBeUndefined(); // the age sync is a bare PASSIVE, not checkpoint()
  }, 20_000);

  it("walSyncMaxAgeMs 0 disables the age sync", async () => {
    const root = tempRoot("age-off");
    const store = await openStore(root, { walSyncMaxAgeMs: 0, checkpointLog: "off" });
    await store.put({ key: "k/0", value: 0, identity });
    await sleep(1_100);
    await store.put({ key: "k/1", value: 1, identity });
    await setImmediateTurn();
    await setImmediateTurn();
    expect(store.stats().checkpointTiming.passive.count).toBe(0);
  }, 20_000);

  it("times every checkpoint, logs it at debug level, and writes one info summary per minute with counts", async () => {
    const root = tempRoot("log");
    const lines: string[] = [];
    vi.spyOn(process.stderr, "write").mockImplementation(((chunk: unknown) => { lines.push(String(chunk)); return true; }) as typeof process.stderr.write);
    const store = await openStore(root, { checkpointLog: "debug", walSyncMaxAgeMs: 0 });
    for (let step = 0; step < 5; step += 1) await store.put({ key: `k/${step}`, value: step, identity });
    store.checkpoint(0); // PASSIVE, then the guarded TRUNCATE (its own PASSIVE first; threshold 0 forces the attempt)
    const timing = store.stats().checkpointTiming;
    expect(timing.passive.count).toBe(2);
    expect(timing.truncate.count).toBe(1);
    expect(timing.onWriterPath).toBe(0);
    const debug = lines.filter((line) => line.startsWith("[mesh] state checkpoint "));
    expect(debug).toHaveLength(3);
    const passive = JSON.parse(debug[0]!.slice("[mesh] state checkpoint ".length)) as Record<string, unknown>;
    expect(passive).toMatchObject({ mode: "passive", via: "explicit", busy: 0, writerPath: false });
    expect(passive.frames).toBeGreaterThan(0);
    expect(typeof passive.ms).toBe("number");
    expect(typeof passive.walBytes).toBe("number");
    expect(JSON.parse(debug[2]!.slice("[mesh] state checkpoint ".length))).toMatchObject({ mode: "truncate", via: "explicit", busy: 0 });
    // The minute summary is emitted by the next commit after the window (no timer).
    expect(lines.some((line) => line.startsWith("[mesh] state checkpoints "))).toBe(false);
    const now = Date.now();
    vi.spyOn(Date, "now").mockReturnValue(now + 61_000);
    await store.put({ key: "k/after", value: 1, identity });
    vi.mocked(Date.now).mockRestore();
    const summaries = lines.filter((line) => line.startsWith("[mesh] state checkpoints "));
    expect(summaries).toHaveLength(1);
    expect(JSON.parse(summaries[0]!.slice("[mesh] state checkpoints ".length))).toMatchObject({ passive: 2, truncate: 1, busy: 0,
      onWriterPath: 0, lockTimeouts: 0 });
    expect(store.stats().maxCommitMs).toBeGreaterThanOrEqual(0);
    expect(store.stats().lockTimeouts).toBe(0);
  }, 20_000);

  it("checkpointLog off writes nothing", async () => {
    const root = tempRoot("off");
    const write = vi.spyOn(process.stderr, "write").mockImplementation((() => true) as typeof process.stderr.write);
    const store = await openStore(root, { checkpointLog: "off" });
    await store.put({ key: "k", value: 1, identity });
    store.checkpoint(0);
    expect(write).not.toHaveBeenCalled();
  });

  it("the projector checkpoints a dirty WAL by age, not every checkpointMs", async () => {
    const root = tempRoot("projector");
    const authority = await openStore(root, { checkpointLog: "off" });
    await authority.put({ key: "k/0", value: 0, identity });
    const projector = await StateProjector.open({ root, mode: "sqlite", verifyMs: 0, statusMs: 0, checkpointMs: 10, checkpointMaxAgeMs: 400 });
    projectors.push(projector);
    expect(await projector.tick()).toMatchObject({ role: "active", checkpoints: 1 }); // existing WAL work at start
    await authority.put({ key: "k/1", value: 1, identity });
    await sleep(30);
    expect(await projector.tick()).toMatchObject({ checkpoints: 1 }); // dirty, but younger than checkpointMaxAgeMs
    await sleep(450);
    expect(await projector.tick()).toMatchObject({ checkpoints: 2 });
  }, 20_000);
});
