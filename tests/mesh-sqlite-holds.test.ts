import { spawn } from "node:child_process";
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
import { holdMeshFence, holdMeshFenceSync } from "../src/mesh/fence-lock.js";

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

it.each(["run", "get", "all"] as const)("%s bookkeeping failures preserve driver successes and original exceptions", async method => {
  const f = fake(), root = temp();
  const original = f.db.prepare("SELECT value FROM kv");
  let failClock = false;
  const invoke = <T,>(call: () => T): T => {
    try { return call(); } finally { failClock = true; }
  };
  vi.spyOn(f.db, "prepare").mockReturnValue({
    run: (...params) => invoke(() => original.run(...params)),
    get: (...params) => invoke(() => original.get(...params)),
    all: (...params) => invoke(() => original.all(...params)),
  });
  const db = recordSqliteHolds(f.db, root, "bookkeeping", { thresholdMs: 1, now: () => {
    if (failClock) { failClock = false; throw new Error("diagnostic clock failed"); }
    return f.now();
  } });
  const statement = db.prepare("SELECT value FROM kv");
  db.exec("BEGIN IMMEDIATE");
  expect(statement[method]()).toBe({ run: f.result, get: f.row, all: f.rows }[method]);
  f.state.stepError = new Error("original driver failure");
  expect(() => statement[method]()).toThrow(f.state.stepError);
  db.exec("ROLLBACK");
  await Promise.resolve();
  expect(read(root)).toMatchObject([{ outcome: "rollback", phases: [] }]);
});

it.each(["prepare", "exec"] as const)("%s bookkeeping failures preserve driver successes and original exceptions", async method => {
  const f = fake(), root = temp();
  let failClock = false;
  const original = new Error("original driver failure");
  const prepare = f.db.prepare.bind(f.db), exec = f.db.exec.bind(f.db);
  if (method === "prepare") {
    vi.spyOn(f.db, "prepare").mockImplementation(sql => {
      try { return prepare(sql); } finally { failClock = true; }
    });
  } else {
    vi.spyOn(f.db, "exec").mockImplementation(sql => {
      if (sql !== "CREATE TABLE kv(key)") return exec(sql);
      try { if (f.state.stepError) throw f.state.stepError; return exec(sql); }
      finally { failClock = true; }
    });
  }
  const db = recordSqliteHolds(f.db, root, "bookkeeping", { thresholdMs: 1, now: () => {
    if (failClock) { failClock = false; throw new Error("diagnostic clock failed"); }
    return f.now();
  } });
  const call = () => method === "prepare" ? db.prepare("SELECT value FROM kv") : db.exec("CREATE TABLE kv(key)");
  db.exec("BEGIN IMMEDIATE");
  expect(call).not.toThrow();
  f.state.prepareError = f.state.stepError = original;
  expect(call).toThrow(original);
  db.exec("ROLLBACK");
  await Promise.resolve();
  expect(read(root)).toMatchObject([{ outcome: "rollback", phases: [] }]);
});

it("a throwing changes getter cannot replace a successful run result or record a statement error", async () => {
  const f = fake(), root = temp();
  Object.defineProperty(f.result, "changes", { get() { throw new Error("diagnostic conversion failed"); } });
  const db = recordSqliteHolds(f.db, root, "changes", { thresholdMs: 1, now: f.now });
  const statement = db.prepare("UPDATE kv SET value = ?");
  db.exec("BEGIN IMMEDIATE");
  expect(statement.run()).toBe(f.result);
  db.exec("COMMIT");
  await Promise.resolve();
  expect(read(root)).toMatchObject([{ outcome: "commit", phases: [] }]);
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

const metricLine = (): SqliteHoldLine => ({ v: 1, at: new Date().toISOString(), tag: "test:append", pid: process.pid,
  release: "test", bridge: false, waitMs: 0, holdMs: 3, commitMs: 0, rollbackMs: 0, outcome: "commit", phases: [] });

// Keep: recorded real catch, PR #1582: a bridge's umask-0002 metrics directory stranded the append lock.
it.skipIf(process.platform === "win32")("appends every line and releases its lock in a 0775 metrics directory", () => {
  const root = temp(), file = sqliteHoldsPath(root);
  fs.mkdirSync(path.dirname(file));
  fs.chmodSync(path.dirname(file), 0o775);
  expect(fs.statSync(path.dirname(file)).mode & 0o777).toBe(0o775);
  for (let i = 0; i < 5; i++) {
    appendSqliteHold(root, { ...metricLine(), holdMs: i });
    expect(fs.existsSync(file + ".lock")).toBe(false);
  }
  expect(read(root).map(line => line.holdMs)).toEqual([0, 1, 2, 3, 4]);
});

it.skipIf(process.platform === "win32")("reclaims a stale append lock in a 0775 metrics directory", () => {
  const root = temp(), file = sqliteHoldsPath(root);
  fs.mkdirSync(path.dirname(file));
  fs.chmodSync(path.dirname(file), 0o775);
  fs.writeFileSync(file + ".lock", "");
  fs.utimesSync(file + ".lock", new Date(0), new Date(0));
  for (let i = 0; i < 3; i++) appendSqliteHold(root, { ...metricLine(), holdMs: i });
  expect(read(root).map(line => line.holdMs)).toEqual([0, 1, 2]);
  expect(fs.existsSync(file + ".lock")).toBe(false);
});

it("does not remove a successor lock installed while releasing our own lock", () => {
  const root = temp(), file = sqliteHoldsPath(root), lock = file + ".lock";
  const write = fs.writeSync.bind(fs);
  vi.spyOn(fs, "writeSync").mockImplementation((...args: Parameters<typeof fs.writeSync>) => {
    // Keep the old inode alive under another name so even inode reuse cannot mask a mismatch.
    fs.renameSync(lock, lock + ".old");
    fs.writeFileSync(lock, "successor");
    return write(...args);
  });
  appendSqliteHold(root, metricLine());
  expect(read(root)).toHaveLength(1);
  expect(fs.readFileSync(lock, "utf8")).toBe("successor");
});

it("does not reclaim a fresh successor swapped in after observing a stale lock", () => {
  const root = temp(), file = sqliteHoldsPath(root), lock = file + ".lock";
  fs.mkdirSync(path.dirname(file));
  fs.writeFileSync(lock, "stale");
  fs.utimesSync(lock, new Date(0), new Date(0));
  const lstat = fs.lstatSync.bind(fs);
  let swapped = false;
  vi.spyOn(fs, "lstatSync").mockImplementation((...args: Parameters<typeof fs.lstatSync>) => {
    const seen = lstat(...args);
    if (args[0] === lock && !swapped) {
      swapped = true;
      fs.renameSync(lock, lock + ".old");
      fs.writeFileSync(lock, "successor");
    }
    return seen;
  });
  appendSqliteHold(root, metricLine());
  expect(read(root)).toEqual([]);
  expect(fs.readFileSync(lock, "utf8")).toBe("successor");
});

// Keep: PR #1582 round 3 P3. ext4 can reuse dev/ino immediately; exercise the identity
// tuple deterministically rather than relying on a particular filesystem's inode allocator.
it.each(["release", "reclaim"] as const)("%s preserves a successor with reused dev/ino but a different ctime", mode => {
  const root = temp(), file = sqliteHoldsPath(root), lock = file + ".lock";
  fs.mkdirSync(path.dirname(file));
  let judged: fs.BigIntStats | undefined;
  const lstat = fs.lstatSync.bind(fs), write = fs.writeSync.bind(fs);
  const replace = () => {
    fs.renameSync(lock, lock + ".old");
    fs.writeFileSync(lock, "successor");
  };
  if (mode === "reclaim") {
    fs.writeFileSync(lock, "stale");
    fs.utimesSync(lock, new Date(0), new Date(0));
  } else {
    vi.spyOn(fs, "writeSync").mockImplementation((...args: Parameters<typeof fs.writeSync>) => {
      judged = fs.lstatSync(lock, { bigint: true });
      replace();
      return write(...args);
    });
  }
  let checkedTuple = false;
  vi.spyOn(fs, "lstatSync").mockImplementation((...args: Parameters<typeof fs.lstatSync>) => {
    const current = lstat(...args);
    if (args[0] !== lock) return current;
    if (mode === "reclaim" && !judged) {
      judged = current as fs.BigIntStats;
      replace();
      return current;
    }
    if (!judged) return current;
    checkedTuple = true;
    // Simulate immediate inode reuse, changing ONLY ctime in the judged identity tuple.
    return { ...judged, ctimeNs: judged.ctimeNs + 1n };
  });
  appendSqliteHold(root, metricLine());
  expect(checkedTuple).toBe(true);
  expect(fs.readFileSync(lock, "utf8")).toBe("successor");
  expect(read(root)).toHaveLength(mode === "release" ? 1 : 0);
});

it("four processes append complete bounded JSON lines through rotation without leaving lock or temporary files", async () => {
  const root = temp(), file = sqliteHoldsPath(root);
  fs.mkdirSync(path.dirname(file));
  if (process.platform !== "win32") fs.chmodSync(path.dirname(file), 0o775);
  const seed = JSON.stringify({ ...metricLine(), tag: "s".repeat(20_000) }) + "\n";
  fs.writeFileSync(file, seed.repeat(Math.floor(SQLITE_HOLDS_MAX_BYTES / Buffer.byteLength(seed))));
  const script = `import { createJiti } from "jiti"; import { pathToFileURL } from "node:url";
    const jiti = createJiti(pathToFileURL(process.cwd() + "/index.js").href);
    const { appendSqliteHold } = await jiti.import("./src/mesh/sqlite-holds.ts");
    process.send("ready");
    process.once("message", () => {
      for (let i = 0; i < 200; i++) appendSqliteHold(process.argv[1], {
        v: 1, at: new Date().toISOString(), tag: "p".repeat(20_000), pid: process.pid,
        release: "test", bridge: false, waitMs: 0, holdMs: i, commitMs: 0, rollbackMs: 0, outcome: "commit", phases: []
      });
      process.disconnect();
    });`;
  const children = Array.from({ length: 4 }, () => spawn(process.execPath, ["--input-type=module", "-e", script, root],
    { cwd: process.cwd(), stdio: ["ignore", "ignore", "pipe", "ipc"] }));
  const exits = children.map(child => new Promise<void>((resolve, reject) => {
    let stderr = "";
    child.stderr!.on("data", chunk => { stderr += String(chunk); });
    child.once("error", reject);
    child.once("exit", code => code === 0 ? resolve() : reject(new Error(`appender exit ${code}: ${stderr}`)));
  }));
  // Install rejection handlers before waiting for the ready barrier, including early import failures.
  const completed = Promise.all(exits);
  try {
    await Promise.race([completed.then(() => { throw new Error("appenders exited before ready"); }),
      Promise.all(children.map(child => new Promise<void>(resolve => child.once("message", () => resolve()))))]);
    for (const child of children) child.send("go");
    await completed;
  } finally {
    for (const child of children) if (child.exitCode === null && child.signalCode === null) child.kill();
    await Promise.allSettled(exits);
  }
  expect(fs.readdirSync(path.dirname(file)).sort()).toEqual(["sqlite-holds.jsonl", "sqlite-holds.jsonl.1"]);
  for (const generation of [file, file + ".1"]) {
    expect(fs.statSync(generation).size).toBeLessThanOrEqual(SQLITE_HOLDS_MAX_BYTES);
    const data = fs.readFileSync(generation, "utf8");
    expect(data.endsWith("\n")).toBe(true);
    const lines = data.trimEnd().split("\n").map(line => JSON.parse(line) as SqliteHoldLine);
    expect(lines.length).toBeGreaterThan(0);
    for (const line of lines) expect(line).toMatchObject({ v: 1, outcome: "commit", phases: [] });
  }
}, 30_000);

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

// Keep: PR #1582 round 3 P1. COMMIT's microtask used to write while an awaiting caller
// still held .lock and custody.lock. Observe actual JSONL writes, not merely append scheduling.
it.each(["fresh async", "fresh sync", "normal writes", "import/rollback"] as const)(
  "%s writes every slow diagnostic with both fence locks free", async mode => {
    vi.stubEnv("PI_FABRIC_SQLITE_HOLD_MS", "0.000001");
    const root = temp();
    const observations: Array<{ tag: string; mesh: boolean; custody: boolean }> = [];
    const write = fs.writeSync.bind(fs);
    vi.spyOn(fs, "writeSync").mockImplementation((...args: Parameters<typeof fs.writeSync>) => {
      const data = args[1];
      if (Buffer.isBuffer(data)) {
        const text = data.toString();
        if (text.startsWith('{"v":1,') && text.includes('"phases":')) {
          const line = JSON.parse(text) as SqliteHoldLine;
          observations.push({ tag: line.tag, mesh: fs.existsSync(path.join(root, ".lock")),
            custody: fs.existsSync(path.join(root, "custody.lock")) });
        }
      }
      return write(...args);
    });
    const identity = { id: "locks-free", name: "test", kind: "agent" as const };
    if (mode === "fresh async") {
      const store = await SqliteStateStore.open(root, 64 * 1024, 100, { initialize: "create" });
      store.close();
    } else if (mode === "fresh sync") {
      const store = SqliteStateStore.openSync(root, 64 * 1024, 100, { initialize: "create" });
      store.close();
    } else {
      const mesh = new MeshStore(root, 64 * 1024, 100, mode === "normal writes" ? { stateBackend: "sqlite" } : {});
      meshes.push(mesh);
      await mesh.put({ key: "one", value: 1, identity });
      if (mode === "import/rollback") {
        await importMeshState(root);
        await rollbackMeshState(root);
        expect(mesh.get("one", { fresh: true })?.value).toBe(1);
      }
    }
    await Promise.resolve();
    expect(observations.length).toBeGreaterThan(0);
    if (mode === "import/rollback") expect(observations.filter(line => line.tag === "migration:transaction").length).toBeGreaterThanOrEqual(4);
    else expect(observations.map(line => line.tag)).toContain("state:init");
    expect(observations).toEqual(observations.map(line => ({ ...line, mesh: false, custody: false })));
    expect(read(root)).toHaveLength(observations.length);
  });

it("holds pending lines across a real fence await and flushes only after the last fence releases on error", async () => {
  const first = temp(), second = temp(), f = fake();
  const observations: boolean[] = [];
  const db = recordSqliteHolds(f.db, first, "awaited-fence", { thresholdMs: 1, now: f.now,
    append: () => observations.push([first, second].some(root =>
      fs.existsSync(path.join(root, ".lock")) || fs.existsSync(path.join(root, "custody.lock")))) });
  const original = new Error("fenced body failed");
  await expect(holdMeshFence(first, { lockTimeoutMs: 1_000 }, async () => {
    db.exec("BEGIN IMMEDIATE"); db.exec("COMMIT");
    await new Promise<void>(resolve => setImmediate(resolve));
    expect(observations).toEqual([]);
    await holdMeshFence(second, { lockTimeoutMs: 1_000 }, async () => {
      db.exec("BEGIN IMMEDIATE"); db.exec("COMMIT");
      await Promise.resolve();
      expect(observations).toEqual([]);
    });
    await Promise.resolve();
    expect(observations).toEqual([]);
    throw original;
  })).rejects.toBe(original);
  await Promise.resolve();
  expect(observations).toEqual([false, false]);
});

it("a failed fence acquisition leaves future hold diagnostics enabled", async () => {
  const root = temp(), f = fake(), append = vi.fn();
  const db = recordSqliteHolds(f.db, root, "after-timeout", { thresholdMs: 1, now: f.now, append });
  await holdMeshFence(root, { lockTimeoutMs: 1_000 }, async () => {
    await expect(holdMeshFence(root, { lockTimeoutMs: 0 }, () => {})).rejects.toThrow();
    expect(() => holdMeshFenceSync(root, 0, () => {})).toThrow();
  });
  db.exec("BEGIN IMMEDIATE"); db.exec("COMMIT");
  await Promise.resolve();
  expect(append).toHaveBeenCalledTimes(1);
});

it("bounds pending diagnostic lines while a fence spans awaits", async () => {
  const root = temp(), f = fake(), append = vi.fn();
  const db = recordSqliteHolds(f.db, root, "bounded-pending", { thresholdMs: 1, now: f.now, append });
  await holdMeshFence(root, { lockTimeoutMs: 1_000 }, async () => {
    for (let i = 0; i < 140; i++) { db.exec("BEGIN IMMEDIATE"); db.exec("COMMIT"); }
    await Promise.resolve();
    expect(append).not.toHaveBeenCalled();
  });
  await Promise.resolve();
  expect(append).toHaveBeenCalledTimes(128);
  db.exec("BEGIN IMMEDIATE"); db.exec("COMMIT");
  await Promise.resolve();
  expect(append).toHaveBeenCalledTimes(129);
});
