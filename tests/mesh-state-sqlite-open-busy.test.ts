import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { isMeshLockTimeout } from "../src/core/atomic-write.js";
import { isMeshRetryableBusy, isMeshStateBusy } from "../src/mesh/state-backend.js";
import { StoreBridgeSide } from "../src/mesh/bridge.js";
import { SqliteStateStore } from "../src/mesh/state-sqlite.js";
import { MeshStore, type MeshIdentity, type MeshStoreOptions } from "../src/mesh/store.js";

// smarty-dev#6477 P0 (sqlite soak): wal_autocheckpoint=0 and no production checkpointer let the hub's WAL grow
// to 48 MB, and a raw "database is locked" (SQLITE_BUSY) escaped as fatal and stopped a mesh-bridge on restart.

type Database = { exec(sql: string): void; close(): void; prepare(sql: string): { get(): unknown } };
const { DatabaseSync } = process.getBuiltinModule("node:sqlite") as { DatabaseSync: new (file: string, options?: object) => Database };

const identity: MeshIdentity = { id: "tester", name: "tester", kind: "agent" };
const roots: string[] = [];
const stores: MeshStore[] = [];
const sqlite: SqliteStateStore[] = [];
const databases: Database[] = [];

const tempRoot = (label: string): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), `pi-fabric-sqmaint-${label}-`));
  roots.push(root);
  return root;
};
const open = (root: string, options: MeshStoreOptions = {}): MeshStore => {
  const store = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "sqlite", ...options });
  stores.push(store);
  return store;
};
const openStore = async (root: string, options: Parameters<typeof SqliteStateStore.open>[3] = {}): Promise<SqliteStateStore> => {
  const store = await SqliteStateStore.open(root, 256 * 1024, 1_000, { initialize: "create", ...options });
  sqlite.push(store);
  return store;
};
const raw = (root: string): Database => {
  const database = new DatabaseSync(path.join(root, "state.db"), { timeout: 0 });
  databases.push(database);
  return database;
};
/** Another process's exclusive hold: every other connection's read, write and checkpoint is SQLITE_BUSY. */
const holdExclusive = (root: string): Database => {
  const database = raw(root);
  database.exec("PRAGMA locking_mode = EXCLUSIVE");
  database.exec("BEGIN EXCLUSIVE");
  database.exec("UPDATE meta SET value = value WHERE name = 'created_at'");
  return database;
};
const release = (database: Database): void => {
  try { database.exec("ROLLBACK"); } catch { /* none open */ }
  database.close();
  databases.splice(databases.indexOf(database), 1);
};
const failure = (operation: () => unknown): unknown => {
  try { operation(); } catch (error) { return error; }
  throw new Error("expected a failure");
};
const expectRetryableBusy = (error: unknown): void => {
  expect(error).toBeInstanceOf(Error);
  expect((error as Error).message).not.toBe("database is locked");
  expect(isMeshStateBusy(error)).toBe(true);
  expect(isMeshLockTimeout(error)).toBe(true);
};
const walBytes = (root: string): number => fs.statSync(path.join(root, "state.db-wal"), { throwIfNoEntry: false })?.size ?? 0;
const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));
const pad = "x".repeat(6_000);

afterEach(() => {
  for (const database of databases.splice(0)) try { database.close(); } catch { /* closed */ }
  for (const store of stores.splice(0)) try { store.closeState(); } catch { /* closed */ }
  for (const store of sqlite.splice(0)) try { store.close(); } catch { /* closed */ }
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("SQLite busy never escapes raw (defect 2)", () => {
  it("maps a busy first open, busy reads and busy writes to the retryable FABRIC_MESH_STATE_BUSY", async () => {
    const root = tempRoot("busy");
    const seed = open(root);
    await seed.put({ key: "a", value: 1, identity });
    seed.closeState();

    let holder = holdExclusive(root);
    const fresh = open(root, { lockTimeoutMs: 150 });
    expectRetryableBusy(failure(() => fresh.get("a"))); // the synchronous first open
    expectRetryableBusy(failure(() => fresh.listAll("")));
    const write = await fresh.put({ key: "b", value: 2, identity }).then(() => undefined, (error: unknown) => error);
    expectRetryableBusy(write); // the asynchronous first open of a write
    release(holder);
    expect(fresh.get("a")?.value).toBe(1);

    holder = raw(root); // now on an OPEN store: another connection holds the write lock (EXCLUSIVE needs it closed)
    holder.exec("BEGIN IMMEDIATE");
    holder.exec("UPDATE meta SET value = value WHERE name = 'created_at'");
    expect(fresh.get("a")?.value).toBe(1); // WAL readers never wait for the writer
    expectRetryableBusy(await fresh.put({ key: "b", value: 2, identity }).then(() => undefined, (error: unknown) => error));
    release(holder);
    expect((await fresh.put({ key: "b", value: 2, identity })).version).toBeGreaterThan(0);
  });

  it("opening an imported root needs no write lock (a held writer does not fail a restart)", async () => {
    const root = tempRoot("open");
    const seed = open(root);
    await seed.put({ key: "a", value: 1, identity });
    seed.closeState();
    const writer = raw(root);
    writer.exec("BEGIN IMMEDIATE"); // another process holds the state write lock, as on a busy hub
    const restarted = open(root);
    expect(restarted.get("a")?.value).toBe(1);
    writer.exec("ROLLBACK");
  });

  it("a raw driver busy error is retryable for the bridge loop", async () => {
    const root = tempRoot("raw");
    const seed = open(root);
    await seed.put({ key: "a", value: 1, identity });
    seed.closeState();
    const holder = holdExclusive(root);
    const reader = raw(root);
    const error = failure(() => reader.prepare("SELECT count(*) FROM kv").get());
    expect((error as Error).message).toMatch(/locked|busy/i);
    expect(isMeshRetryableBusy(error)).toBe(true);
    expect(isMeshRetryableBusy(new Error("database disk image is malformed"))).toBe(false);
    release(holder);
  });

  it("a bridge side on a freshly opened store under an exclusive hold fails retryably, then recovers", async () => {
    const root = tempRoot("bridge");
    const seed = open(root);
    await seed.put({ key: "a", value: 1, identity });
    seed.closeState();
    const holder = holdExclusive(root);
    const side = new StoreBridgeSide(open(root, { lockTimeoutMs: 150 }), "remote-machine");
    for (const operation of [() => side.presence(), () => side.mirror({ hosts: [], participants: [] })]) {
      const error = await operation().then(() => undefined, (failure: unknown) => failure);
      if (error !== undefined) {
        expect((error as Error).message).not.toMatch(/^database is locked$/);
        expect(isMeshRetryableBusy(error)).toBe(true);
      }
    }
    release(holder);
    await expect(side.presence()).resolves.toBeDefined();
  });
});

describe("WAL stays bounded without a maintainer (defect 1)", () => {
  it("SQLite's built-in autocheckpoint is on and keeps a client-only WAL near 4 MiB", async () => {
    const root = tempRoot("wal");
    const store = await openStore(root);
    let max = 0;
    for (let n = 0; n < 1_500; n += 1) { // ~9 MB of commits, no maintainer, below the 64 MiB emergency threshold
      await store.put({ key: `k${n % 50}`, value: { n, pad }, identity });
      max = Math.max(max, walBytes(root));
    }
    expect(max).toBeLessThan(6 * 1024 * 1024);
    expect(store.stats().checkpoints.emergency).toBe(0);
  });
});
