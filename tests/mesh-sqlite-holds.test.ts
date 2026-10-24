import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, expect, it, vi } from "vitest";
import { importMeshState, rollbackMeshState } from "../src/mesh/backend-migration.js";
import { StateProjector } from "../src/mesh/state-projector.js";
import { SqliteStateStore } from "../src/mesh/state-sqlite.js";
import { StoreBridgeSide } from "../src/mesh/bridge.js";
import { CommitOutbox, withStateFence } from "../src/mesh/commit-outbox.js";
import { appendSqliteHold, DEFAULT_SQLITE_HOLD_MS, recordSqliteHolds, SQLITE_HOLDS_MAX_BYTES,
  sqliteHoldContext, sqliteHoldsPath, type SqliteHoldLine } from "../src/mesh/sqlite-holds.js";
import type { SqliteConnection, SqliteStatement } from "../src/mesh/state-sqlite.js";
import { MeshStore } from "../src/mesh/store.js";

// Keep: data safety / failure paths. Diagnostics run beside the writer and must never alter
// SQLite results, rollback behavior, the original exception, or the durable outbox protocol.
const roots: string[] = [];
const meshes: MeshStore[] = [];
const temp = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-sqlite-holds-"));
  roots.push(root);
  return root;
};
afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  for (const mesh of meshes.splice(0)) mesh.closeState();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const fake = () => {
  const state = { time: 0, transaction: false, beginMs: 10, stepMs: 40, commitMs: 5, rollbackMs: 3,
    beginError: undefined as unknown, stepError: undefined as unknown, prepareError: undefined as unknown,
    commitError: undefined as unknown, rollbackError: undefined as unknown, autoRollback: false };
  const row = { value: "same object" };
  const rows = [row];
  const result = { changes: 2n, lastInsertRowid: 7n };
  const step = () => {
    state.time += state.stepMs;
    if (state.stepError) {
      if (state.autoRollback) state.transaction = false;
      throw state.stepError;
    }
  };
  const statement: SqliteStatement = {
    run() { expect(this).toBe(statement); step(); return result; },
    get() { expect(this).toBe(statement); step(); return row; },
    all() { expect(this).toBe(statement); step(); return rows; },
  };
  const db: SqliteConnection = {
    exec(sql) {
      expect(this).toBe(db);
      if (sql === "BEGIN IMMEDIATE") {
        state.time += state.beginMs;
        if (state.beginError) throw state.beginError;
        state.transaction = true;
      } else if (sql === "COMMIT") {
        state.time += state.commitMs;
        if (state.commitError) throw state.commitError;
        state.transaction = false;
      } else if (sql === "ROLLBACK") {
        state.time += state.rollbackMs;
        if (state.rollbackError) throw state.rollbackError;
        state.transaction = false;
      } else { state.time += 2; }
    },
    prepare() { expect(this).toBe(db); if (state.prepareError) throw state.prepareError; return statement; },
    close() { expect(this).toBe(db); state.transaction = false; },
    get isTransaction() { return state.transaction; },
    attestDatabaseFile() { expect(this).toBe(db); return { canonicalPath: "state.db", device: "1", inode: "2" }; },
  };
  return { state, db, result, row, rows, now: () => state.time };
};
const read = (root: string): SqliteHoldLine[] => {
  const file = sqliteHoldsPath(root);
  return fs.existsSync(file) ? fs.readFileSync(file, "utf8").trim().split("\n").map(line => JSON.parse(line)) : [];
};

it("slow fake statement emits exactly one JSON line, after release, with wait/step/commit phases", async () => {
  const root = temp(), f = fake();
  const db = recordSqliteHolds(f.db, root, "test:slow", { thresholdMs: 20, now: f.now });
  const sql = db.prepare("UPDATE kv SET value = ? WHERE key = ?");
  sqliteHoldContext(db, "bridge:mirror", 0);
  f.state.time += 7; // Async acquisition/backoff time, supplied by the caller.
  db.exec("BEGIN IMMEDIATE");
  expect(sql.run("private value", "private key")).toBe(f.result);
  await Promise.resolve();
  expect(read(root)).toEqual([]);
  db.exec("COMMIT");
  await Promise.resolve();
  const lines = read(root);
  expect(lines).toHaveLength(1);
  expect(lines[0]).toMatchObject({ v: 1, tag: "bridge:mirror", pid: process.pid, waitMs: 17,
    holdMs: 45, commitMs: 5, rollbackMs: 0, outcome: "commit", phases: [
      { tag: expect.stringMatching(/^update:kv:[a-f0-9]{8}:run$/), count: 1, totalMs: 40, changes: 2, errors: 0 },
    ] });
  expect(lines[0]!.release).not.toBe("unknown");
  expect(JSON.stringify(lines)).not.toMatch(/private value|private key|UPDATE kv/);
  expect(f.state.transaction).toBe(false);
  expect(Number.isNaN(Date.parse(lines[0]!.at))).toBe(false);
});

it("fast and exactly-at-threshold holds perform no metrics I/O", async () => {
  const root = temp(), f = fake(), append = vi.fn();
  const db = recordSqliteHolds(f.db, root, "fast", { thresholdMs: 45, now: f.now, append });
  const sql = db.prepare("UPDATE kv SET value = ?");
  const mkdir = vi.spyOn(fs, "mkdirSync");
  const write = vi.spyOn(fs, "writeSync");
  const stat = vi.spyOn(fs, "lstatSync");
  for (let i = 0; i < 2; i++) { db.exec("BEGIN IMMEDIATE"); sql.run(); db.exec("COMMIT"); }
  await Promise.resolve();
  expect(append).not.toHaveBeenCalled();
  expect(mkdir).not.toHaveBeenCalled();
  expect(write).not.toHaveBeenCalled();
  expect(stat).not.toHaveBeenCalled();
});

it("0 disables the recorder by returning the original adapter; invalid env uses 2000ms", async () => {
  const f = fake(), root = temp();
  vi.stubEnv("PI_FABRIC_SQLITE_HOLD_MS", "0");
  expect(recordSqliteHolds(f.db, root, "off")).toBe(f.db);
  vi.stubEnv("PI_FABRIC_SQLITE_HOLD_MS", "not-a-number");
  const append = vi.fn();
  const db = recordSqliteHolds(f.db, root, "default", { now: f.now, append });
  const sql = db.prepare("SELECT value FROM meta");
  f.state.stepMs = DEFAULT_SQLITE_HOLD_MS - 5;
  db.exec("BEGIN IMMEDIATE"); sql.get(); db.exec("COMMIT");
  await Promise.resolve();
  expect(append).not.toHaveBeenCalled();
  f.state.stepMs++;
  db.exec("BEGIN IMMEDIATE"); sql.get(); db.exec("COMMIT");
  await Promise.resolve();
  expect(append).toHaveBeenCalledTimes(1);
});

it("busy acquisitions emit nothing and the eventual hold includes the whole original wait", async () => {
  const f = fake(), lines: SqliteHoldLine[] = [];
  const db = recordSqliteHolds(f.db, temp(), "busy", { thresholdMs: 1, now: f.now, append: (_, line) => lines.push(line) });
  const error = new Error("busy");
  f.state.beginError = error;
  sqliteHoldContext(db, "state:writeBatch", 0);
  expect(() => db.exec("BEGIN IMMEDIATE")).toThrow(error);
  expect(lines).toEqual([]);
  f.state.time += 30;
  f.state.beginError = undefined;
  sqliteHoldContext(db, "state:writeBatch", 0);
  db.exec("BEGIN IMMEDIATE"); db.exec("COMMIT");
  await Promise.resolve();
  expect(lines).toHaveLength(1);
  expect(lines[0]).toMatchObject({ waitMs: 50, holdMs: 5, tag: "state:writeBatch" });
});

it("ROLLBACK and a thrown statement preserve the original error and count the failing step", async () => {
  const f = fake(), root = temp();
  const db = recordSqliteHolds(f.db, root, "failure", { thresholdMs: 1, now: f.now });
  const sql = db.prepare("DELETE FROM kv WHERE key = ?");
  const original = new Error("original");
  f.state.stepError = original;
  let caught: unknown;
  try {
    db.exec("BEGIN IMMEDIATE");
    try { sql.run("key"); } finally { try { db.exec("ROLLBACK"); } catch { /* original stays */ } }
  } catch (error) { caught = error; }
  expect(caught).toBe(original);
  await Promise.resolve();
  expect(read(root)).toMatchObject([{ outcome: "rollback", holdMs: 43, commitMs: 0, rollbackMs: 3,
    phases: [{ count: 1, changes: 0, totalMs: 40, errors: 1 }] }]);
});

it.each(["get", "all"] as const)("a throwing %s step preserves its original exception and records once", async (method) => {
  const f = fake(), root = temp();
  const db = recordSqliteHolds(f.db, root, "read-error", { thresholdMs: 1, now: f.now });
  const sql = db.prepare("SELECT value FROM kv");
  f.state.stepError = new Error("read failed");
  db.exec("BEGIN IMMEDIATE");
  expect(() => sql[method]()).toThrow(f.state.stepError);
  db.exec("ROLLBACK");
  await Promise.resolve();
  expect(read(root)).toMatchObject([{ outcome: "rollback", phases: [{ errors: 1, count: 1, totalMs: 40 }] }]);
});

it("COMMIT errors remain original and are timed separately before a single rollback line", async () => {
  const f = fake(), root = temp();
  const db = recordSqliteHolds(f.db, root, "commit-error", { thresholdMs: 1, now: f.now });
  const original = new Error("commit failed");
  f.state.commitError = original;
  db.exec("BEGIN IMMEDIATE");
  try { expect(() => db.exec("COMMIT")).toThrow(original); }
  finally { db.exec("ROLLBACK"); }
  await Promise.resolve();
  expect(read(root)).toMatchObject([{ outcome: "rollback", holdMs: 8, commitMs: 5, rollbackMs: 3 }]);
});

it("an automatic SQLite rollback records once even if the caller skips ROLLBACK", async () => {
  const f = fake(), root = temp();
  const db = recordSqliteHolds(f.db, root, "auto-rollback", { thresholdMs: 1, now: f.now });
  const sql = db.prepare("UPDATE meta SET value = ?");
  f.state.stepError = new Error("SQLITE_FULL"); f.state.autoRollback = true;
  db.exec("BEGIN IMMEDIATE");
  expect(() => sql.run()).toThrow(f.state.stepError);
  if (db.isTransaction) db.exec("ROLLBACK");
  db.close();
  await Promise.resolve();
  expect(read(root)).toMatchObject([{ outcome: "rollback", phases: [{ errors: 1 }] }]);
});

it("a failed explicit rollback does not log before release; close records once", async () => {
  const f = fake(), root = temp();
  const db = recordSqliteHolds(f.db, root, "rollback-error", { thresholdMs: 1, now: f.now });
  f.state.rollbackError = new Error("rollback failed");
  db.exec("BEGIN IMMEDIATE");
  expect(() => db.exec("ROLLBACK")).toThrow(f.state.rollbackError);
  await Promise.resolve();
  expect(read(root)).toEqual([]);
  db.close();
  await Promise.resolve();
  expect(read(root)).toMatchObject([{ outcome: "close", holdMs: 3, rollbackMs: 3 }]);
});

it("never alters result objects or adapter receivers and resets per-hold aggregates", async () => {
  const f = fake(), root = temp();
  const db = recordSqliteHolds(f.db, root, "results", { thresholdMs: 1, now: f.now });
  const sql = db.prepare("SELECT value FROM kv WHERE key = ?");
  expect(db.attestDatabaseFile!()).toEqual(f.db.attestDatabaseFile!());
  expect(recordSqliteHolds(db, root, "twice")).toBe(db);
  for (let i = 0; i < 2; i++) {
    db.exec("BEGIN IMMEDIATE");
    expect(sql.run()).toBe(f.result); expect(sql.get()).toBe(f.row); expect(sql.all()).toBe(f.rows);
    db.exec("COMMIT");
  }
  await Promise.resolve();
  expect(read(root)).toHaveLength(2);
  for (const line of read(root)) {
    expect(line.phases).toHaveLength(3);
    expect(line.phases.map(phase => phase.count)).toEqual([1, 1, 1]);
    expect(line.phases.map(phase => phase.changes)).toEqual([2, 0, 0]);
  }
});

it("times prepare/exec inside custody and preserves preparation errors", async () => {
  const f = fake(), root = temp();
  const db = recordSqliteHolds(f.db, root, "prepare", { thresholdMs: 1, now: f.now });
  db.exec("BEGIN IMMEDIATE");
  db.exec("CREATE TABLE kv(key)");
  f.state.prepareError = new Error("bad query");
  expect(() => db.prepare("SELECT value FROM kv")).toThrow(f.state.prepareError);
  db.exec("ROLLBACK");
  await Promise.resolve();
  expect(read(root)[0]!.phases).toEqual(expect.arrayContaining([
    expect.objectContaining({ tag: expect.stringMatching(/:exec$/), count: 1, totalMs: 2 }),
    expect.objectContaining({ tag: expect.stringMatching(/:prepare$/), count: 1, errors: 1 }),
  ]));
});

it("swallows metrics callback failures without replacing success OR a body exception", async () => {
  const f = fake();
  const db = recordSqliteHolds(f.db, temp(), "metrics-error", { thresholdMs: 1, now: f.now,
    append: () => { throw new Error("diagnostics failed"); } });
  const sql = db.prepare("UPDATE kv SET value = ?");
  db.exec("BEGIN IMMEDIATE"); expect(sql.run()).toBe(f.result); expect(() => db.exec("COMMIT")).not.toThrow();
  f.state.stepError = new Error("body failed");
  db.exec("BEGIN IMMEDIATE");
  try { expect(() => sql.run()).toThrow(f.state.stepError); } finally { expect(() => db.exec("ROLLBACK")).not.toThrow(); }
});

it("swallows real filesystem failures on the metrics path", async () => {
  const root = temp(), f = fake();
  fs.writeFileSync(path.join(root, "metrics"), "not a directory");
  const db = recordSqliteHolds(f.db, root, "fs-error", { thresholdMs: 1, now: f.now });
  db.exec("BEGIN IMMEDIATE");
  expect(() => db.exec("COMMIT")).not.toThrow();
  expect(f.state.transaction).toBe(false);
});

it("bounds the current and previous generations; contention drops instead of waiting/growing", async () => {
  const root = temp(), f = fake(), lines: SqliteHoldLine[] = [];
  const db = recordSqliteHolds(f.db, root, "rotate", { thresholdMs: 1, now: f.now, append: (_, line) => lines.push(line) });
  db.exec("BEGIN IMMEDIATE"); db.exec("COMMIT");
  await Promise.resolve();
  const line = lines[0]!, file = sqliteHoldsPath(root);
  fs.mkdirSync(path.dirname(file));
  fs.writeFileSync(file, " ".repeat(SQLITE_HOLDS_MAX_BYTES));
  appendSqliteHold(root, line);
  expect(fs.statSync(file + ".1").size).toBe(SQLITE_HOLDS_MAX_BYTES);
  await Promise.resolve();
  expect(read(root)).toHaveLength(1);
  const size = fs.statSync(file).size;
  fs.writeFileSync(file + ".lock", "");
  appendSqliteHold(root, line);
  expect(fs.statSync(file).size).toBe(size);
  fs.utimesSync(file + ".lock", new Date(0), new Date(0));
  appendSqliteHold(root, line);
  await Promise.resolve();
  expect(read(root)).toHaveLength(2);
  expect(fs.statSync(file).size).toBeLessThanOrEqual(SQLITE_HOLDS_MAX_BYTES);
});

it("bounds statement cardinality on a connection, including dynamic prepare calls", async () => {
  const f = fake(), root = temp();
  const db = recordSqliteHolds(f.db, root, "bounded", { thresholdMs: 1, now: f.now });
  db.exec("BEGIN IMMEDIATE");
  for (let i = 0; i < 200; i++) db.prepare("SELECT " + i + " FROM kv").get();
  db.exec("COMMIT");
  await Promise.resolve();
  expect(read(root)[0]!.phases.length).toBeLessThanOrEqual(128);
  expect(read(root)[0]!.phases.find(phase => phase.tag === "other")?.count).toBeGreaterThan(0);
});

it("real MeshStore bridge mirror, fence, rollback, outbox recovery and retirement carry their caller tags", async () => {
  vi.stubEnv("PI_FABRIC_SQLITE_HOLD_MS", "0.000001");
  const root = temp();
  const mesh = new MeshStore(root, 64 * 1024, 100, { stateBackend: "sqlite" });
  meshes.push(mesh);
  const identity = { id: "diagnostic-test", name: "test", kind: "agent" as const };
  const side = new StoreBridgeSide(mesh, "peer");
  await side.mirror({ hosts: [], participants: [] });
  expect(await withStateFence(mesh, identity, () => 42)).toBe(42);
  const original = new Error("rollback body");
  await expect(mesh.writeBatch({ identity, ops: [], prepare: () => { throw original; } })).rejects.toBe(original);
  const effects: string[] = [];
  const outbox = new CommitOutbox(mesh, "test", identity, { effect: (payload: string) => { effects.push(payload); } });
  const plan = outbox.plan();
  await mesh.writeBatch({ identity, ops: [], prepare: () => plan.stage([], [{ kind: "effect", key: "one", payload: "ran" }], { durable: true }) });
  expect(await outbox.recover()).toBe(1);
  expect(effects).toEqual(["ran"]);
  expect(mesh.listAll(outbox.prefix, { fresh: true })).toEqual([]);
  await Promise.resolve();
  const lines = read(root);
  expect(lines.map(line => line.tag)).toEqual(expect.arrayContaining([
    "state:init", "bridge:mirror", "outbox:fence", "outbox:recover", "outbox:retire",
  ]));
  expect(lines.find(line => line.tag === "bridge:mirror")?.bridge).toBe(true);
  expect(lines.find(line => line.outcome === "rollback")).toBeDefined();
  expect(lines.find(line => line.tag === "outbox:retire")?.phases.some(phase => phase.changes > 0)).toBe(true);
});

it("real synchronous fence and exclusive custody preserve callback results and errors", async () => {
  vi.stubEnv("PI_FABRIC_SQLITE_HOLD_MS", "0.000001");
  const root = temp();
  const store = await SqliteStateStore.open(root, 64 * 1024, 100, { initialize: "detached" });
  try {
    expect(store.fenceSync(() => 7)).toBe(7);
    const original = new Error("fenced callback");
    expect(() => store.fenceSync(() => { throw original; })).toThrow(original);
    expect(await store.exclusive(() => 8)).toBe(8);
    await Promise.resolve();
    const lines = read(root);
    expect(lines.filter(line => line.tag === "state:fence").map(line => line.outcome)).toEqual(["rollback", "rollback"]);
    expect(lines.find(line => line.tag === "state:custody")?.outcome).toBe("commit");
  } finally { store.close(); }
});

it("real shadow projector records initialization, election, snapshot, record and release in the mesh metrics root", async () => {
  vi.stubEnv("PI_FABRIC_SQLITE_HOLD_MS", "0.000001");
  const root = temp();
  const mesh = new MeshStore(root, 64 * 1024, 100);
  meshes.push(mesh);
  const identity = { id: "projector-test", name: "test", kind: "agent" as const };
  await mesh.put({ key: "one", value: 1, identity });
  const projector = await StateProjector.open({ root, verifyMs: 0, statusMs: 0 });
  try {
    await projector.tick();
    await mesh.put({ key: "one", value: 2, identity });
    await projector.tick();
    expect(await projector.verify()).toEqual([]);
  } finally { await projector.stop(); }
  const lines = read(root);
  expect(lines.map(line => line.tag)).toEqual(expect.arrayContaining([
    "state:init", "projector:election", "projector:snapshot", "projector:record", "projector:release",
  ]));
  expect(fs.existsSync(sqliteHoldsPath(path.join(root, "state-projector")))).toBe(false);
  expect(lines.find(line => line.tag === "projector:snapshot")?.phases.some(phase => phase.changes > 0)).toBe(true);
});

it("real import/rollback records FULL migration transactions without changing the round trip", async () => {
  vi.stubEnv("PI_FABRIC_SQLITE_HOLD_MS", "0.000001");
  const root = temp();
  const mesh = new MeshStore(root, 64 * 1024, 100);
  meshes.push(mesh);
  const identity = { id: "migration-test", name: "test", kind: "agent" as const };
  await mesh.put({ key: "one", value: { original: true }, identity });
  await importMeshState(root);
  await rollbackMeshState(root);
  expect(mesh.get("one", { fresh: true })?.value).toEqual({ original: true });
  expect(read(root).filter(line => line.tag === "migration:transaction").length).toBeGreaterThanOrEqual(4);
});
