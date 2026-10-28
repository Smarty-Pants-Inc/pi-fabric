import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { MeshLockTimeoutError } from "../src/core/atomic-write.js";
import { SqliteStateBackend } from "../src/mesh/state-backend.js";
import { MeshStateRetiredError, openNodeSqlite, SqliteStateStore, type SqliteConnection } from "../src/mesh/state-sqlite.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";

// Keep reason: data safety / authority boundary. Real WAL connections exercise concurrent commit,
// retirement and cached-view admission through the MeshStore API, not mocked fence call counts.
const identity: MeshIdentity = { id: "snapshot-test", name: "snapshot-test", kind: "agent" };
const roots: string[] = [];
const readers: MeshStore[] = [];
const writers: SqliteStateStore[] = [];
const rawConnections: SqliteConnection[] = [];
const KEY = "authority/owner";
const openReader = (root: string): MeshStore => {
  const reader = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "sqlite" });
  readers.push(reader);
  return reader;
};
const sqliteOf = (reader: MeshStore): SqliteStateStore => {
  const backend = reader.stateBackendHandle;
  if (!(backend instanceof SqliteStateBackend)) throw new Error("test requires real SQLite");
  return backend.store;
};
const fixture = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-read-snapshot-"));
  roots.push(root);
  const reader = openReader(root);
  await reader.put({ key: KEY, value: "old", identity });
  const writer = await SqliteStateStore.open(root, 64 * 1024, 1_000, { lockTimeoutMs: 0, busyTimeoutMs: 0 });
  writers.push(writer);
  return { root, reader, writer };
};

beforeEach(() => { vi.stubEnv("PI_FABRIC_MESH_STATE_BACKEND", ""); });
afterEach(() => {
  vi.unstubAllEnvs();
  for (const reader of readers.splice(0)) reader.closeState();
  for (const writer of writers.splice(0)) writer.close();
  for (const connection of rawConnections.splice(0)) connection.close();
  // Only directories allocated by this test file are removed.
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("MeshStore.withStateReadSnapshot (real SQLite WAL)", () => {
  it("allows a concurrent commit but pins every read, stamp and change feed until the next snapshot", async () => {
    const { root, reader, writer } = await fixture();
    const peer = openReader(root);
    const before = reader.stateRevision()!;
    const token = reader.stateToken(); // Warm the parsed snapshot and its data_version liveness check.
    let commit: ReturnType<SqliteStateStore["put"]> | undefined;
    const result = reader.withStateReadSnapshot(() => {
      expect(reader.get(KEY)?.value).toBe("old");
      expect(reader.stateToken()).toBe(token);
      commit = writer.put({ key: KEY, value: "new", identity });
      // SqliteStateStore's uncontended write commits synchronously before returning its Promise.
      // This independent connection sees the committed new row while the reader is still pinned.
      expect(writer.get(KEY)?.value).toBe("new");
      expect(writer.stateStamp()).not.toBe(before);
      expect(peer.stateView()?.view.get(KEY)?.value).toBe("new"); // Process-shared cache is now newer.
      expect(reader.get(KEY)?.value).toBe("old");
      expect(reader.list("authority/")[0]?.value).toBe("old");
      expect(reader.listAll("authority/")[0]?.value).toBe("old");
      expect(reader.listAllShared("authority/")[0]?.value).toBe("old");
      expect(reader.stateView()?.view.get(KEY)?.value).toBe("old");
      expect(reader.stateRevision()).toBe(before);
      expect(reader.cachedStateStamp()).toBe(before);
      expect(reader.stateUnchangedSince(before, ["authority/"])).toBe(true);
      expect(sqliteOf(reader).changesSince(1).changes).toEqual([]);
      expect(sqliteOf(reader).exportLive().state.entries[KEY]?.value).toBe("old");
      return 42;
    });
    expect(result).toBe(42);
    await commit;
    reader.withStateReadSnapshot(() => {
      expect(reader.get(KEY)?.value).toBe("new");
      expect(reader.stateToken()).not.toBe(token);
      expect(reader.stateView()?.view.get(KEY)?.value).toBe("new");
      expect(reader.stateUnchangedSince(before, ["authority/"])).toBe(false);
    });
  });

  it("pins before caller code's first read and refreshes a previously cached view", async () => {
    const { reader, writer } = await fixture();
    const oldToken = reader.stateToken();
    let commit: ReturnType<SqliteStateStore["put"]> | undefined;
    reader.withStateReadSnapshot(() => {
      commit = writer.put({ key: KEY, value: "new", identity });
      expect(writer.get(KEY)?.value).toBe("new");
      expect(reader.stateView()?.view.get(KEY)?.value).toBe("old");
      expect(reader.get(KEY, { fresh: true })?.value).toBe("old");
    });
    await commit;
    reader.withStateReadSnapshot(() => {
      // Historical token reads must not admit stale ownership inside a new live snapshot.
      expect(reader.get(KEY, { snapshot: oldToken })?.value).toBe("new");
      expect(reader.list("authority/", 100, { snapshot: oldToken })[0]?.value).toBe("new");
      expect(reader.listAll("authority/", { snapshot: oldToken })[0]?.value).toBe("new");
      expect(reader.listAllShared("authority/", { snapshot: oldToken })[0]?.value).toBe("new");
      expect(reader.stateView()?.view.get(KEY)?.value).toBe("new");
    });
    // Outside this API the established explicit historical-token contract is unchanged.
    expect(reader.get(KEY, { snapshot: oldToken })?.value).toBe("old");
  });

  it("does not import a process-shared cached view newer than its pinned transaction", async () => {
    const { root, reader, writer } = await fixture();
    const peer = openReader(root);
    reader.stateToken();
    reader.stateBackendHandle.dropCache();
    let commit: ReturnType<SqliteStateStore["put"]> | undefined;
    reader.withStateReadSnapshot(() => {
      commit = writer.put({ key: KEY, value: "new", identity });
      expect(peer.stateView()?.view.get(KEY)?.value).toBe("new");
      // No local cached view: rebuilding must reject a delta base newer than this transaction.
      expect(reader.stateView()?.view.get(KEY)?.value).toBe("old");
      expect(reader.listAllShared("authority/")[0]?.value).toBe("old");
    });
    await commit;
    expect(reader.withStateReadSnapshot(() => reader.stateView()?.view.get(KEY)?.value)).toBe("new");
  });

  it("can read while another connection already holds writer custody, without settling that writer", async () => {
    const { root, reader } = await fixture();
    const pending = openNodeSqlite(path.join(root, "state.db"));
    rawConnections.push(pending);
    pending.exec("BEGIN IMMEDIATE");
    try {
      pending.prepare("UPDATE kv SET value = ? WHERE key = ?").run(JSON.stringify("pending"), KEY);
      expect(reader.withStateReadSnapshot(() => reader.get(KEY)?.value)).toBe("old");
      expect(pending.isTransaction).toBe(true);
    } finally { pending.exec("ROLLBACK"); }
  });

  it("recognizes a retirement committed during the callback before any later cached read", async () => {
    const { reader, writer } = await fixture();
    const raw = sqliteOf(reader);
    const token = reader.stateToken();
    expect(raw.get(KEY)?.value).toBe("old"); // Populate the live-meta/data_version cache.
    let retired: Promise<number> | undefined;
    reader.withStateReadSnapshot(() => {
      retired = writer.retire();
      expect(writer.exportState().backend).toBe("retired");
      expect(reader.get(KEY)?.value).toBe("old");
      expect(reader.get(KEY, { snapshot: token })?.value).toBe("old");
      expect(reader.stateView()?.view.get(KEY)?.value).toBe("old");
    });
    await retired;
    expect(() => raw.get(KEY)).toThrow(MeshStateRetiredError);
    expect(() => reader.get(KEY, { snapshot: token })).toThrow(MeshStateRetiredError);
    expect(() => reader.listAllShared()).toThrow(MeshStateRetiredError);
    let entered = false;
    expect(() => reader.withStateReadSnapshot(() => { entered = true; })).toThrow(MeshStateRetiredError);
    expect(entered).toBe(false);
  });

  it.each(["backend", "epoch"] as const)("validates %s in the pinned transaction, even without a commit-counter change", async (name) => {
    const { root, reader } = await fixture();
    const raw = sqliteOf(reader);
    raw.get(KEY);
    reader.stateToken();
    const connection = openNodeSqlite(path.join(root, "state.db"));
    rawConnections.push(connection);
    connection.prepare("UPDATE meta SET value = ? WHERE name = ?").run(name === "backend" ? "retired" : 999, name);
    let entered = false;
    expect(() => reader.withStateReadSnapshot(() => { entered = true; })).toThrow(MeshStateRetiredError);
    expect(entered).toBe(false);
    expect(() => raw.get(KEY)).toThrow(MeshStateRetiredError);
  });

  it("releases the transaction after callback errors without replaying caller code", async () => {
    const { reader } = await fixture();
    const error = Object.assign(new Error("callback busy, not acquisition busy"), { code: "SQLITE_BUSY" });
    let calls = 0;
    expect(() => reader.withStateReadSnapshot(() => { calls += 1; throw error; })).toThrow(error);
    expect(calls).toBe(1);
    // A new BEGIN on this same connection proves the failed callback left no read transaction.
    expect(reader.withStateWriteFence(() => reader.get(KEY)?.value)).toBe("old");
    await reader.put({ key: KEY, value: "after-error", identity });
    expect(reader.withStateReadSnapshot(() => reader.get(KEY)?.value)).toBe("after-error");
  });

  it.each(["object", "function", "getter"] as const)("rejects a %s thenable and releases the read transaction", async (kind) => {
    const { reader } = await fixture();
    const value = kind === "function" ? Object.assign(() => undefined, { then: () => undefined })
      : kind === "getter" ? Object.defineProperty({}, "then", { get() { throw new Error("then getter"); } })
        : { then: () => undefined };
    expect(() => reader.withStateReadSnapshot(() => value)).toThrow(kind === "getter" ? "then getter" : "must be synchronous");
    expect(reader.withStateWriteFence(() => reader.get(KEY)?.value)).toBe("old");
    expect(reader.withStateReadSnapshot(() => 7)).toBe(7);
  });

  it("rejects writers before their async opening step can escape the callback, and never runs their hooks", async () => {
    const { reader } = await fixture();
    const writes: Promise<unknown>[] = [];
    let writerCallbacks = 0;
    reader.withStateReadSnapshot(() => {
      writes.push(reader.put({ key: KEY, value: "forbidden", identity }));
      writes.push(reader.delete({ key: KEY }));
      writes.push(reader.confirmWritable(() => { writerCallbacks += 1; }));
      writes.push(reader.writeBatch({ identity, ops: [], prepare: () => { writerCallbacks += 1; return []; } }));
      writes.push(reader.writeBatch({ identity, ops: [] }));
      writes.push(sqliteOf(reader).put({ key: KEY, value: "forbidden-direct", identity }));
      expect(() => reader.withStateWriteFence(() => { writerCallbacks += 1; })).toThrow(/must not call store writers/);
      expect(() => reader.withStateReadSnapshot(() => { writerCallbacks += 1; })).toThrow(/must not nest/);
      expect(reader.get(KEY)?.value).toBe("old");
    });
    for (const write of writes) await expect(write).rejects.toThrow(/must not call store writers/);
    expect(writerCallbacks).toBe(0);
    expect(reader.get(KEY)?.value).toBe("old");
    await reader.put({ key: KEY, value: "allowed-after", identity });
    expect(reader.get(KEY)?.value).toBe("allowed-after");
  });

  it.each(["only", "leading", "middle", "trailing"] as const)("treats %s missing change-feed commits as unknown, not unchanged", async (gap) => {
    const { root, reader, writer } = await fixture();
    const before = reader.stateRevision()!;
    const connection = openNodeSqlite(path.join(root, "state.db"));
    rawConnections.push(connection);
    if (gap !== "leading" && gap !== "only") await writer.put({ key: "unrelated/before", value: 1, identity });
    // Reproduce a legacy/raw writer: the committed authority row and commit counter move,
    // but no changes row is written. The retention floor still claims the feed is complete.
    connection.exec("BEGIN IMMEDIATE");
    try {
      connection.prepare("UPDATE kv SET value = ? WHERE key = ?").run(JSON.stringify("raw-owner"), KEY);
      connection.exec("UPDATE meta SET value = value + 1 WHERE name = 'commit_no'");
      connection.exec("COMMIT");
    } finally { if (connection.isTransaction) connection.exec("ROLLBACK"); }
    if (gap !== "trailing" && gap !== "only") await writer.put({ key: "unrelated/after", value: 2, identity });
    expect(sqliteOf(reader).changesSince(1).complete).toBe(true); // Floor-only proof is insufficient.
    reader.withStateReadSnapshot(() => {
      expect(reader.get(KEY)?.value).toBe("raw-owner");
      expect(reader.stateUnchangedSince(before, ["authority/"])).toBe(false);
      expect(reader.stateUnchangedSince(before, ["authority/"], new Set([KEY]))).toBe(false);
    });
  });

  it("still proves unrelated real commits unchanged when every distinct commit is covered", async () => {
    const { reader, writer } = await fixture();
    const before = reader.stateRevision()!;
    await writer.writeBatch({ identity, ops: [
      { kind: "put", key: "unrelated/a", value: 1 },
      { kind: "put", key: "unrelated/b", value: 2 },
    ] });
    await writer.put({ key: "unrelated/a", value: 3, identity });
    reader.withStateReadSnapshot(() => {
      expect(reader.stateRevision()).not.toBe(before);
      expect(reader.stateUnchangedSince(before, ["authority/"])).toBe(true);
      expect(reader.stateUnchangedSince(before, ["unrelated/"])).toBe(false);
    });
    await writer.put({ key: KEY, value: "real-new-owner", identity });
    expect(reader.withStateReadSnapshot(() => reader.stateUnchangedSince(before, ["authority/"]))).toBe(false);
  });

  it("preserves write-fence exclusion: a held event-admission fence still blocks takeovers", async () => {
    const { reader, writer } = await fixture();
    let takeover: ReturnType<SqliteStateStore["put"]> | undefined;
    reader.withStateWriteFence(() => {
      takeover = writer.put({ key: KEY, value: "takeover", identity });
      expect(writer.get(KEY)?.value).toBe("old");
      expect(reader.get(KEY)?.value).toBe("old");
    });
    await expect(takeover).rejects.toBeInstanceOf(MeshLockTimeoutError);
    expect(reader.get(KEY)?.value).toBe("old");
    await writer.put({ key: KEY, value: "takeover", identity });
    expect(reader.get(KEY)?.value).toBe("takeover");
  });
});
