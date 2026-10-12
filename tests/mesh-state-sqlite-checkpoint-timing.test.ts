import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshLockTimeoutError } from "../src/core/atomic-write.js";
import { openNodeSqlite, SqliteStateStore } from "../src/mesh/state-sqlite.js";
import type { MeshIdentity } from "../src/mesh/store.js";

// smarty-dev#6100 round 1: every checkpoint the store runs is timed in stats() and logged (debug: every checkpoint;
// info: one summary line per minute with commit maxima and lock timeouts). Observability only: no cadence changes.
const identity: MeshIdentity = { id: "tester", name: "tester", kind: "agent" };
const roots: string[] = [];
const stores: SqliteStateStore[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const store of stores.splice(0)) try { store.close(); } catch { /* closed by the test */ }
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const tempRoot = (label: string): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), `pi-fabric-ckpt-timing-${label}-`));
  roots.push(root);
  return root;
};
const openStore = async (root: string, options: Parameters<typeof SqliteStateStore.open>[3] = {}): Promise<SqliteStateStore> => {
  const store = await SqliteStateStore.open(root, 64 * 1024, 1_000, { initialize: "create", ...options });
  stores.push(store);
  return store;
};
const captureStderr = (): string[] => {
  const lines: string[] = [];
  vi.spyOn(process.stderr, "write").mockImplementation(((chunk: unknown) => { lines.push(String(chunk)); return true; }) as typeof process.stderr.write);
  return lines;
};
const SUMMARY = "[mesh] state checkpoints ";
const DEBUG = "[mesh] state checkpoint ";

describe("SQLite mesh checkpoint timing (smarty-dev#6100 round 1)", () => {
  it("times every checkpoint, logs it at debug level, and writes one info summary per minute with counts", async () => {
    const root = tempRoot("log");
    const lines = captureStderr();
    const store = await openStore(root, { checkpointLog: "debug" });
    for (let step = 0; step < 5; step += 1) await store.put({ key: `k/${step}`, value: step, identity });
    store.checkpoint(0); // PASSIVE, then the guarded TRUNCATE (its own PASSIVE first; threshold 0 forces the attempt)
    const timing = store.stats().checkpointTiming;
    expect(timing.passive.count).toBe(2);
    expect(timing.truncate.count).toBe(1);
    expect(timing.onWriterPath).toBe(0);
    const debug = lines.filter((line) => line.startsWith(DEBUG));
    expect(debug).toHaveLength(3);
    const passive = JSON.parse(debug[0]!.slice(DEBUG.length)) as Record<string, unknown>;
    expect(passive).toMatchObject({ mode: "passive", via: "explicit", busy: 0, writerPath: false });
    expect(passive.frames).toBeGreaterThan(0);
    expect(typeof passive.ms).toBe("number");
    expect(typeof passive.walBytes).toBe("number");
    expect(JSON.parse(debug[2]!.slice(DEBUG.length))).toMatchObject({ mode: "truncate", via: "explicit", busy: 0 });
    // The minute summary is emitted by the next commit after the window (no timer).
    expect(lines.some((line) => line.startsWith(SUMMARY))).toBe(false);
    const now = Date.now();
    vi.spyOn(Date, "now").mockReturnValue(now + 61_000);
    await store.put({ key: "k/after", value: 1, identity });
    vi.mocked(Date.now).mockRestore();
    const summaries = lines.filter((line) => line.startsWith(SUMMARY));
    expect(summaries).toHaveLength(1);
    const summary = JSON.parse(summaries[0]!.slice(SUMMARY.length)) as Record<string, unknown>;
    // commits: the five puts plus the one whose COMMIT closes the window.
    expect(summary).toMatchObject({ passive: 2, truncate: 1, busy: 0, onWriterPath: 0, lockTimeouts: 0, commits: 6 });
    for (const field of ["passiveMaxMs", "truncateMaxMs", "maxCommitMs", "maxWaitMs"]) expect(typeof summary[field]).toBe("number");
    expect(store.stats().maxCommitMs).toBeGreaterThanOrEqual(0);
    expect(store.stats().slowCommits).toBeGreaterThanOrEqual(0);
    expect(store.stats().lockTimeouts).toBe(0);
  }, 20_000);

  it("counts lock timeouts in stats() and in the minute summary", async () => {
    const root = tempRoot("timeout");
    const lines = captureStderr();
    const store = await openStore(root, { checkpointLog: "info", lockTimeoutMs: 100 });
    await store.put({ key: "k/0", value: 0, identity });
    const holder = openNodeSqlite(path.join(root, "state.db"));
    holder.exec("BEGIN IMMEDIATE");
    try {
      await expect(store.put({ key: "k/1", value: 1, identity })).rejects.toBeInstanceOf(MeshLockTimeoutError);
    } finally {
      holder.exec("ROLLBACK");
      holder.close();
    }
    expect(store.stats().lockTimeouts).toBe(1);
    expect(lines.some((line) => line.startsWith(DEBUG))).toBe(false); // info level: no per-checkpoint lines
    const now = Date.now();
    vi.spyOn(Date, "now").mockReturnValue(now + 61_000);
    await store.put({ key: "k/2", value: 2, identity });
    vi.mocked(Date.now).mockRestore();
    const summaries = lines.filter((line) => line.startsWith(SUMMARY));
    expect(summaries).toHaveLength(1);
    const summary = JSON.parse(summaries[0]!.slice(SUMMARY.length)) as Record<string, unknown>;
    expect(summary).toMatchObject({ lockTimeouts: 1 });
    expect(summary.maxWaitMs).toBeGreaterThanOrEqual(100);
  }, 20_000);

  it("checkpointLog off writes nothing", async () => {
    const root = tempRoot("off");
    const write = vi.spyOn(process.stderr, "write").mockImplementation((() => true) as typeof process.stderr.write);
    const store = await openStore(root, { checkpointLog: "off" });
    await store.put({ key: "k", value: 1, identity });
    store.checkpoint(0);
    expect(write).not.toHaveBeenCalled();
    expect(store.stats().checkpointTiming.passive.count).toBeGreaterThan(0);
  });

  it("PI_FABRIC_MESH_CHECKPOINT_LOG sets the level when the option is absent", async () => {
    const root = tempRoot("env");
    const lines = captureStderr();
    vi.stubEnv("PI_FABRIC_MESH_CHECKPOINT_LOG", "debug");
    try {
      const store = await openStore(root);
      await store.put({ key: "k", value: 1, identity });
      store.checkpoint(0);
      expect(lines.filter((line) => line.startsWith(DEBUG)).length).toBeGreaterThan(0);
    } finally {
      vi.unstubAllEnvs();
    }
  });

  it("leaves the durability settings at main's defaults", async () => {
    const root = tempRoot("pragmas");
    const store = await openStore(root, { checkpointLog: "off" });
    await store.put({ key: "k", value: 1, identity });
    const observer = openNodeSqlite(path.join(root, "state.db"));
    try {
      expect(observer.prepare("PRAGMA journal_mode").get()).toEqual({ journal_mode: "wal" });
    } finally {
      observer.close();
    }
    // The store's own connection: synchronous NORMAL (1), autocheckpoint 1,000 pages, journal_size_limit 16 MiB.
    const recorded: string[] = [];
    const recordingRoot = tempRoot("pragmas-recorded");
    await openStore(recordingRoot, {
      checkpointLog: "off",
      open: (file) => {
        const connection = openNodeSqlite(file);
        return { exec: (sql: string) => { recorded.push(sql); connection.exec(sql); }, prepare: (sql: string) => connection.prepare(sql),
          close: () => connection.close(), get isTransaction() { return connection.isTransaction; } };
      },
    });
    expect(recorded).toEqual(expect.arrayContaining(["PRAGMA synchronous = NORMAL", "PRAGMA wal_autocheckpoint = 1000",
      `PRAGMA journal_size_limit = ${16 * 1024 * 1024}`]));
  });
});
