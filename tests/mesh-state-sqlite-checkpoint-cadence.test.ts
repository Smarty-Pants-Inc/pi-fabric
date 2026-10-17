import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { StateProjector } from "../src/mesh/state-projector.js";
import { openNodeSqlite, SqliteStateStore } from "../src/mesh/state-sqlite.js";
import type { MeshIdentity } from "../src/mesh/store.js";

// smarty-dev#6100 round 2: every complete checkpoint lets the next writer restart the WAL and fsync its header under
// the writer lock, so the routine checkpoints are bounded by volume (autocheckpoint) and age, not run every second.
// Checkpoint timing and its log are main's (tests/mesh-state-sqlite-checkpoint-timing.test.ts); here only the cadence.
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

describe("SQLite mesh checkpoint cadence (smarty-dev#6100 round 2)", () => {
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

  // Review on adddb81d (P1): a due age sync that found writers waiting was retried only by a later commit's own check,
  // which again found them waiting, so a writer queue that never drains starved it; at a low frame rate the 4,000-page
  // autocheckpoint is minutes away, so nothing synced. Here the queue never drains: right after each store commit (before
  // the age sync's setImmediate), another connection takes the writer lock for 80 ms, so all three store writers are
  // waiting at every commit. ~12 commits/s of one small put: far below the autocheckpoint in the run.
  it("a steady writer queue for > 30 s still gets the age PASSIVE: unsynced frames stay within walSyncMaxAgeMs", async () => {
    const root = tempRoot("age-queue");
    const store = await openStore(root, { checkpointLog: "off" }); // default walSyncMaxAgeMs 30 s, autocheckpoint 4,000 pages
    const file = path.join(root, "state.db");
    const holder = openNodeSqlite(file);
    let release: NodeJS.Timeout | undefined;
    const hold = (): void => {
      if (release) return;
      holder.exec("BEGIN IMMEDIATE");
      release = setTimeout(() => { release = undefined; holder.exec("COMMIT"); }, 80);
    };
    const until = Date.now() + 36_000;
    let commits = 0;
    const writer = async (id: number): Promise<void> => {
      for (let i = 0; Date.now() < until; i += 1) {
        await store.put({ key: `w/${id}`, value: i, identity });
        commits += 1;
        hold(); // a microtask after this COMMIT: the other writers, and this one next, wait behind it
      }
    };
    // Unsynced-frame age: frames are in the WAL after the last backfill (state.db's mtime), so the oldest is at most this old.
    let maxUnsyncedMs = 0;
    const sample = (): void => {
      const database = fs.statSync(file);
      const wal = fs.statSync(`${file}-wal`, { throwIfNoEntry: false });
      if (wal && wal.size > 0 && wal.mtimeMs > database.mtimeMs) maxUnsyncedMs = Math.max(maxUnsyncedMs, Date.now() - database.mtimeMs);
    };
    const sampler = setInterval(sample, 100);
    try { await Promise.all([0, 1, 2].map(writer)); }
    finally {
      clearInterval(sampler);
      if (release) clearTimeout(release);
      if (holder.isTransaction) holder.exec("ROLLBACK");
      holder.close();
    }
    sample();
    const stats = store.stats();
    expect(commits).toBeGreaterThan(200);
    expect(stats.maxWaitMs).toBeGreaterThan(50); // the writers did queue
    expect(stats.checkpointTiming.passive.count).toBeGreaterThanOrEqual(1); // the age sync (nothing else runs a PASSIVE here)
    // walSyncMaxAgeMs + the 1 s check spacing + up to 4 commits of deferral (~0.4 s) + sampling slack.
    expect(maxUnsyncedMs).toBeLessThan(32_500);
  }, 60_000);

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
