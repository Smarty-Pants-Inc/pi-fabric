import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { pathToFileURL } from "node:url";
import { describe, expect, it } from "vitest";

const candidate = path.resolve(import.meta.dirname, "..");
const helper = pathToFileURL(path.join(candidate, "scripts/main-batch-observer.mjs")).href;
// Keep: private-root trust boundary, recorded attribution failures, and native rollback/busy
// failure paths that cannot cheaply be reached by an idle production Main.
const run = (body: string, setup = true) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "main-observer-test-"));
  const child = spawnSync(process.execPath, ["--enable-source-maps", "--input-type=module", "-e", `
    import fs from "node:fs";
    import path from "node:path";
    import { DatabaseSync, StatementSync } from "node:sqlite";
    import * as probe from ${JSON.stringify(helper)};
    const root = ${JSON.stringify(root)}, candidate = ${JSON.stringify(candidate)};
    const mesh = path.join(root, "mesh"); fs.mkdirSync(mesh);
    const wait = ms => new Promise(resolve => setTimeout(resolve, ms));
    const identity = { id: "fixture", name: "fixture", kind: "agent" };
    process.env.PI_FABRIC_LOCK_STATS = "1";
    process.env.PI_FABRIC_SQLITE_HOLD_MS = "0";
    ${setup ? `
      const { MeshStore } = await import(new URL("dist/mesh.js", ${JSON.stringify(pathToFileURL(candidate + path.sep).href)}));
      const initial = new MeshStore(mesh, 65536, 100, { stateBackend: "sqlite" });
      await initial.writeBatch({ identity, ops: [], prepare: () => [] }); initial.closeState();
      const makeObserver = options => probe.createMainBatchObserver({ candidate, root, mesh, role: "peerMain", label: "M01", ...options });
    ` : ""}
    ${body}
  `], { encoding: "utf8", timeout: 12000, maxBuffer: 8 * 1024 * 1024 });
  expect(child.error).toBeUndefined();
  expect(child.status, child.stderr + child.stdout).toBe(0);
  return JSON.parse(child.stdout);
};

describe("shared Main batch observer", () => {
  it("observes real startup and distinguishes empty, prepare, hooks, unapplied and applied work", () => {
    const result = run(`
      const observer = await makeObserver();
      const store = new MeshStore(mesh, 65536, 100, { stateBackend: "sqlite" });
      const effects = [];
      await store.writeBatch({ identity, ops: [], prepare: () => [] });
      const startup = observer.getCapture();
      observer.setPhase("idle");
      await store.writeBatch({ identity, ops: [] });
      await store.writeBatch({ identity, ops: [], prepare: () => [] });
      await store.writeBatch({ identity, ops: [], beforeCommit() { effects.push("before"); },
        afterCommit() { effects.push("after"); }, commitOutbox() { effects.push("outbox"); } });
      await store.writeBatch({ identity, ops: [], prepare: () => [{ kind: "put", key: "fixture/a", value: 1 }] });
      await store.writeBatch({ identity, ops: [{ kind: "put", key: "fixture/a", value: 2, ifVersion: 0, onConflict: "skip" }] });
      const capture = observer.getCapture(), detached = observer.getCapture();
      detached.calls.length = 0;
      const count = observer.getCapture().calls.length;
      const sql = probe.aggregateTransactions(capture), sites = probe.aggregateSites(capture);
      const actual = store.get("fixture/a").value;
      observer.restore(); store.closeState();
      console.log(JSON.stringify({ startup, capture, sql, sites, effects, actual, count }));
    `);
    expect(result.startup.calls).toHaveLength(1);
    expect(result.startup.calls[0]).toMatchObject({ phase: "startup", classification: "prepared-empty", succeeded: true });
    expect(result.startup.transactions.some((row: any) => row.batchId === result.startup.calls[0].id && row.succeeded && row.verb === "BEGIN IMMEDIATE")).toBe(true);
    expect(result.capture.rankValid, JSON.stringify(result.capture.observerErrors)).toBe(true);
    expect(result.capture.calls.slice(1).map((row: any) => row.classification)).toEqual([
      "hookless-empty", "prepared-empty", "hook-only", "applied-work", "input-unapplied",
    ]);
    expect(result.capture.calls[4]).toMatchObject({ inputOpsCount: 0, preparedOpsCount: 1, appliedOpsCount: 1 });
    expect(result.capture.calls.every((row: any) => /:\d+:\d+$/.test(row.callSite))).toBe(true);
    // The backend validates before the write-free legacy effect and again at COMMIT.
    expect(result.effects).toEqual(["before", "after", "before", "outbox"]);
    expect(result.capture.calls[3].hookAttempts).toEqual({ beforeCommit: 2, afterCommit: 1, commitOutbox: 1 });
    expect(result.actual).toBe(1);
    expect(result.count).toBe(6);
    expect(result.sql.reduce((sum: number, row: any) => sum + row.successfulBegins, 0)).toBe(4);
    expect(result.sql.every((row: any) => row.successfulBegins === row.complete + row.inflight + row.inferred + row.unpaired)).toBe(true);
    expect(result.sql.reduce((sum: number, row: any) => sum + row.correlatedLocks, 0)).toBe(4);
    expect(result.sites.reduce((sum: number, row: any) => sum + row.count, 0)).toBe(5);
  });

  it("preserves hook receivers, synchronous errors, promise identity and asynchronous hook return types", () => {
    const result = run(`
      const original = MeshStore.prototype.writeBatch;
      const token = new Error("same error"), promise = Promise.resolve([]), receiver = {};
      let behavior, returnedHook;
      MeshStore.prototype.writeBatch = function(input) {
        if (behavior === "sync-error") throw token;
        if (behavior === "hook") { returnedHook = input.prepare.call(receiver, "argument"); return promise; }
        return promise;
      };
      const observer = await makeObserver();
      const store = new MeshStore(mesh, 65536, 100, { stateBackend: "sqlite" });
      let sameError = false, sameReceiver = false, argument, syncReturned = false;
      behavior = "sync-error";
      try { store.writeBatch({ identity, ops: [] }); syncReturned = true; } catch (error) { sameError = error === token; }
      behavior = "hook";
      const hookPromise = Promise.resolve([]);
      const returned = store.writeBatch({ identity, ops: [], prepare(value) { sameReceiver = this === receiver; argument = value; return hookPromise; } });
      const samePromise = returned === promise, sameHookPromise = returnedHook === hookPromise;
      await returned;
      const capture = observer.getCapture();
      observer.restore(); MeshStore.prototype.writeBatch = original; store.closeState();
      console.log(JSON.stringify({ sameError, syncReturned, sameReceiver, argument, samePromise, sameHookPromise, capture }));
    `);
    expect(result).toMatchObject({ sameError: true, syncReturned: false, sameReceiver: true,
      argument: "argument", samePromise: true, sameHookPromise: true });
    expect(result.capture.calls[0]).toMatchObject({ succeeded: false, error: "Error: same error" });
    expect(result.capture.calls[1].hookRuns[0]).toMatchObject({ returnKind: "promise", succeeded: true });
    expect(result.capture.observerErrorCount).toBe(0);
  });

  it("records real hook errors without swallowing rejection or pretending committed outbox work rolled back", () => {
    const result = run(`
      const observer = await makeObserver(), errors = [];
      const store = new MeshStore(mesh, 65536, 100, { stateBackend: "sqlite" });
      observer.setPhase("idle");
      for (const name of ["prepare", "beforeCommit", "afterCommit", "commitOutbox"]) {
        const token = new Error(name);
        try { await store.writeBatch({ identity, ops: name === "commitOutbox" ? [{ kind: "put", key: "fixture/committed", value: 1 }] : [],
          [name]() { throw token; } }); } catch (error) { errors.push(error === token); }
      }
      const capture = observer.getCapture(), actual = store.get("fixture/committed")?.value;
      observer.restore(); store.closeState();
      console.log(JSON.stringify({ errors, capture, actual }));
    `);
    expect(result.errors).toEqual([true, true, true, true]);
    expect(result.actual).toBe(1);
    expect(result.capture.calls[3]).toMatchObject({ succeeded: false, resultsSource: "commitOutbox", appliedOpsCount: 1, classification: "applied-work" });
    expect(result.capture.calls.map((row: any) => row.hookRuns[0].succeeded)).toEqual([false, false, false, false]);
    expect(result.capture.calls.map((row: any) => Object.values(row.hookErrors)[0])).toEqual([1, 1, 1, 1]);
    expect(result.capture.rankValid).toBe(true);
    expect(result.capture.transactions.filter((row: any) => row.verb === "BEGIN IMMEDIATE").map((row: any) => row.holdStatus)).toEqual([
      "complete-rollback", "complete-rollback", "complete-rollback", "complete-commit",
    ]);
  });

  it("fences inherited timers after settled batches but retains live nested and asynchronous batch attribution", () => {
    const result = run(`
      const original = MeshStore.prototype.writeBatch;
      let release, timerFinished;
      const gate = new Promise(resolve => { release = resolve; });
      const timer = new Promise(resolve => { timerFinished = resolve; });
      const db = new DatabaseSync(path.join(mesh, "state.db"));
      let store;
      MeshStore.prototype.writeBatch = function(input) { return input.test(); };
      const observer = await makeObserver();
      store = new MeshStore(mesh, 65536, 100, { stateBackend: "sqlite" });
      observer.setPhase("idle");
      await store.writeBatch({ identity, ops: [], test() {
        setTimeout(() => { db.exec("BEGIN IMMEDIATE"); db.exec("COMMIT"); timerFinished(); }, 0);
        return Promise.resolve([]);
      } });
      await timer;
      const outer = store.writeBatch({ identity, ops: [], async test() {
        await gate;
        await store.writeBatch({ identity, ops: [], test() { db.exec("BEGIN IMMEDIATE"); db.exec("COMMIT"); return []; } });
        db.exec("BEGIN IMMEDIATE"); db.exec("ROLLBACK"); return [];
      } });
      const firstDrain = await observer.drain(0);
      release(); await outer;
      const drained = await observer.drain(1000), capture = observer.getCapture();
      observer.restore(); MeshStore.prototype.writeBatch = original; store.closeState(); db.close();
      console.log(JSON.stringify({ firstDrain, drained, capture }));
    `);
    const begins = result.capture.transactions.filter((row: any) => row.verb === "BEGIN IMMEDIATE");
    expect(begins.map((row: any) => row.batchId)).toEqual([null, 3, 2]);
    expect(result.capture.calls.map((row: any) => row.parentBatchId)).toEqual([null, null, 2]);
    expect(result.firstDrain).toMatchObject({ timedOut: true, pendingAtStart: 1 });
    expect(result.drained).toMatchObject({ timedOut: false, pendingAtStart: 0, liveBatches: 0 });
    expect(result.capture.rankValid).toBe(true);
  });

  it("attributes actual asynchronous busy retries to the live batch and correlates the acquired seam once", () => {
    const result = run(`
      const observer = await makeObserver();
      const store = new MeshStore(mesh, 65536, 100, { stateBackend: "sqlite", lockTimeoutMs: 500 });
      const blocker = new DatabaseSync(path.join(mesh, "state.db"));
      observer.setPhase("idle"); blocker.exec("BEGIN IMMEDIATE");
      const pending = store.writeBatch({ identity, ops: [], prepare: () => [] });
      const release = setTimeout(() => blocker.exec("COMMIT"), 10);
      await pending; clearTimeout(release);
      const fast = new MeshStore(mesh, 65536, 100, { stateBackend: "sqlite", lockTimeoutMs: 0 });
      blocker.exec("BEGIN IMMEDIATE");
      let refused = false;
      try { await fast.writeBatch({ identity, ops: [], prepare: () => [] }); } catch { refused = true; }
      blocker.exec("ROLLBACK");
      const capture = observer.getCapture(), ranked = probe.aggregateTransactions(capture);
      observer.restore(); store.closeState(); fast.closeState(); blocker.close();
      console.log(JSON.stringify({ capture, ranked, refused }));
    `);
    const attempts = result.capture.transactions.filter((row: any) => row.verb === "BEGIN IMMEDIATE" && row.batchId === 1);
    expect(attempts.filter((row: any) => !row.succeeded).length).toBeGreaterThan(0);
    expect(attempts.filter((row: any) => row.succeeded)).toHaveLength(1);
    expect(attempts.at(-1)).toMatchObject({ holdStatus: "complete-commit", lockSample: { correlation: "same-batch-store-release-order" } });
    expect(result.ranked.filter((row: any) => row.origin === "live-batch").reduce((sum: number, row: any) => sum + row.correlatedLocks, 0)).toBe(1);
    expect(result.refused).toBe(true);
    expect(result.capture.locks.filter((row: any) => row.method === "failed")).toHaveLength(1);
    expect(result.capture.transactions.find((row: any) => row.batchId === 2 && row.verb === "BEGIN IMMEDIATE")).toMatchObject({
      succeeded: false, holdStatus: "not-acquired", failedLockSample: { tried: true },
    });
    expect(result.capture.rankValid).toBe(true);
  });

  it("fences ended activity contexts while preserving awaited driver work and exact returns/errors", () => {
    const result = run(`
      const observer = await makeObserver();
      const store = new MeshStore(mesh, 65536, 100, { stateBackend: "sqlite" });
      observer.setPhase("traffic");
      const token = new Error("activity error");
      let sameError = false, timerDone;
      const timer = new Promise(resolve => { timerDone = resolve; });
      const sync = observer.runActivity("setup", () => {
        setTimeout(async () => { await store.writeBatch({ identity, ops: [] }); timerDone(); }, 0);
        return 17;
      });
      try { observer.runActivity("setup", () => { throw token; }); } catch (error) { sameError = error === token; }
      let direct, driverTimerDone;
      const driverTimer = new Promise(resolve => { driverTimerDone = resolve; });
      const returned = observer.runActivity("fixture-driver", () => direct = (async () => {
        await Promise.resolve(); await store.writeBatch({ identity, ops: [], prepare: () => [] });
        setTimeout(async () => { await store.writeBatch({ identity, ops: [] }); driverTimerDone(); }, 0);
        return 23;
      })());
      const samePromise = returned === direct, value = await returned;
      await timer; await driverTimer;
      await observer.runActivity("rejected-driver", () => Promise.reject(token)).catch(error => { sameError &&= error === token; });
      await store.writeBatch({ identity, ops: [] });
      const capture = observer.getCapture(), sites = probe.aggregateSites(capture, "traffic");
      observer.restore(); store.closeState();
      console.log(JSON.stringify({ sync, sameError, samePromise, value, capture, sites }));
    `);
    expect(result).toMatchObject({ sync: 17, sameError: true, samePromise: true, value: 23 });
    expect(result.capture.calls.map((row: any) => row.activity)).toEqual(["fixture-driver", "background", "background", "background"]);
    expect(result.capture.transactions.filter((row: any) => row.batchId === 1).every((row: any) => row.activity === "fixture-driver")).toBe(true);
    expect(result.sites.map((row: any) => row.activity)).toEqual(expect.arrayContaining(["fixture-driver", "background"]));
  });

  it("scopes all actual stores, retains auxiliary identities, rejects lexical and symlink escapes before constructor writes", () => {
    const result = run(`
      const observer = await makeObserver();
      const shared = new MeshStore(mesh, 65536, 100, { stateBackend: "sqlite" });
      const aux = new MeshStore(path.join(root, "aux"), 65536, 100, { stateBackend: "sqlite" });
      observer.setPhase("idle");
      await shared.writeBatch({ identity, ops: [], prepare: () => [] });
      await aux.writeBatch({ identity, ops: [], prepare: () => [] });
      const outside = fs.mkdtempSync(path.join(root, "../outside-observer-"));
      let lexical = false, symlink = false;
      try { new MeshStore(path.join(outside, "escape"), 65536, 100, { stateBackend: "sqlite" }); } catch { lexical = true; }
      if (process.platform !== "win32") {
        fs.symlinkSync(outside, path.join(root, "link"));
        try { new MeshStore(path.join(root, "link", "escape"), 65536, 100, { stateBackend: "sqlite" }); } catch { symlink = true; }
      }
      const capture = observer.getCapture(), actual = [...observer.stores].every(store => store instanceof MeshStore);
      const count = observer.stores.size, outsideFiles = fs.readdirSync(outside);
      observer.restore(); shared.closeState(); aux.closeState();
      console.log(JSON.stringify({ capture, actual, count, lexical, symlink, outsideFiles }));
    `);
    expect(result).toMatchObject({ actual: true, count: 2, lexical: true, outsideFiles: [] });
    if (process.platform !== "win32") expect(result.symlink).toBe(true);
    expect(result.capture.calls.map((row: any) => row.storeScope)).toEqual(["shared-mesh", "auxiliary-store"]);
    expect(new Set(result.capture.calls.map((row: any) => row.storeId)).size).toBe(2);
    const begins = result.capture.transactions.filter((row: any) => row.verb === "BEGIN IMMEDIATE");
    expect(begins.some((row: any) => row.database.scope === "other-sqlite" && row.database.storeScope === "auxiliary-store")).toBe(true);
    expect(new Set(begins.map((row: any) => row.database.inode)).size).toBe(2);
  });

  it("bounds raw calls, detail and errors, exposes complete scalar counters and invalidates partial ranks", () => {
    const result = run(`
      const observer = await makeObserver({ maxRecords: 2 });
      const store = new MeshStore(mesh, 65536, 100, { stateBackend: "sqlite" });
      observer.setPhase("idle");
      for (let i = 0; i < 12; i++) await store.writeBatch({ identity, ops: [], prepare: () => [] });
      const db = new DatabaseSync(path.join(mesh, "state.db"));
      db.exec("BEGIN IMMEDIATE; COMMIT;");
      const capture = observer.getCapture(), sites = probe.aggregateSites(capture), tx = probe.aggregateTransactions(capture);
      observer.restore(); db.close(); store.closeState();
      console.log(JSON.stringify({ capture, sites, tx }));
    `);
    expect(result.capture.calls).toHaveLength(2);
    expect(result.capture.transactions).toHaveLength(2);
    expect(result.capture.locks).toHaveLength(2);
    expect(result.capture.totals).toMatchObject({ calls: 12, settledCalls: 12, successfulBegins: 12, completeHolds: 12, inflightHolds: 0 });
    expect(result.capture.dropped).toBeGreaterThan(0);
    expect(result.capture.observerErrorCount).toBeGreaterThan(0);
    expect(result.capture.rankValid).toBe(false);
    expect([...result.sites, ...result.tx].every((row: any) => row.valid === false)).toBe(true);
  });

  it("keeps dozens of simultaneous calls disjoint and restores every native and store wrapper after setup failure", () => {
    const result = run(`
      const native = { exec: DatabaseSync.prototype.exec, prepare: DatabaseSync.prototype.prepare,
        close: DatabaseSync.prototype.close, run: StatementSync.prototype.run, batch: MeshStore.prototype.writeBatch };
      let failed = false;
      const registry = globalThis[Symbol.for("pi-fabric.mesh.lock-stats")], stats = registry.stats;
      registry.stats = undefined;
      try { await makeObserver(); } catch { failed = true; } finally { registry.stats = stats; }
      const afterFailure = DatabaseSync.prototype.exec === native.exec && MeshStore.prototype.writeBatch === native.batch
        && !Object.hasOwn(MeshStore.prototype, "root");
      const observer = await makeObserver();
      const a = new MeshStore(mesh, 65536, 100, { stateBackend: "sqlite" });
      const b = new MeshStore(path.join(root, "aux"), 65536, 100, { stateBackend: "sqlite" });
      observer.setPhase("idle");
      await Promise.all(Array.from({ length: 48 }, (_, i) => (i % 2 ? a : b).writeBatch({ identity, ops: [], prepare: () => [] })));
      const capture = observer.getCapture(), sites = probe.aggregateSites(capture);
      observer.restore(); observer.restore();
      const restored = DatabaseSync.prototype.exec === native.exec && DatabaseSync.prototype.prepare === native.prepare
        && DatabaseSync.prototype.close === native.close && StatementSync.prototype.run === native.run
        && MeshStore.prototype.writeBatch === native.batch && !Object.hasOwn(MeshStore.prototype, "root");
      a.closeState(); b.closeState();
      console.log(JSON.stringify({ failed, afterFailure, restored, capture, sites }));
    `);
    expect(result).toMatchObject({ failed: true, afterFailure: true, restored: true });
    expect(result.capture.calls).toHaveLength(48);
    expect(new Set(result.capture.calls.map((row: any) => row.id)).size).toBe(48);
    expect(result.capture.rankValid).toBe(true);
    expect(result.sites).toHaveLength(2);
    expect(result.sites.map((row: any) => row.count)).toEqual([24, 24]);
    expect(result.capture.calls.every((row: any) => row.parentBatchId === null)).toBe(true);
  });
});

describe("native SQLite custody pairing", () => {
  it("pairs actual connections, busy retries, commit, rollback, failed commit, automatic rollback and close without counting failed BEGIN as custody", () => {
    const result = run(`
      const file = path.join(mesh, "state.db"), a = new DatabaseSync(file), b = new DatabaseSync(file);
      const foreign = new DatabaseSync(path.join(root, "foreign.db"));
      a.exec("CREATE TABLE unique_value (value INTEGER UNIQUE); INSERT INTO unique_value VALUES (1)");
      a.exec("PRAGMA foreign_keys=ON; CREATE TABLE parent (id INTEGER PRIMARY KEY); CREATE TABLE child (id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)");
      let clock = 0;
      const rows = [], errors = [];
      const restore = probe.captureTransactions(probe.databaseIdentity(file),
        () => ({ phase: "idle", batchId: null, role: "peerMain", label: "M01", pid: process.pid, monotonicMs: clock }),
        row => rows.push(row), { now: () => clock, onError: (where, error) => errors.push({ where, error: String(error) }) });
      a.exec("BEGIN IMMEDIATE"); clock = 7;
      let busy = false;
      try { b.exec("BEGIN IMMEDIATE"); } catch { busy = true; }
      clock = 11; a.exec("COMMIT");
      clock = 13; b.exec("BEGIN IMMEDIATE"); clock = 31; b.exec("ROLLBACK");
      a.exec("BEGIN IMMEDIATE"); a.exec("INSERT INTO child VALUES (9)");
      let commitFailed = false;
      try { a.exec("COMMIT"); } catch { commitFailed = true; }
      const stillActive = a.isTransaction;
      clock = 40; a.exec("ROLLBACK");
      a.exec("BEGIN IMMEDIATE"); clock = 45;
      try { a.prepare("INSERT OR ROLLBACK INTO unique_value VALUES (?)").run(1); } catch {}
      const automaticallyEnded = !a.isTransaction;
      foreign.exec("BEGIN IMMEDIATE"); foreign.exec("COMMIT");
      b.exec("BEGIN IMMEDIATE"); clock = 60; b.close();
      const ranked = probe.aggregateTransactions({ calls: [], transactions: rows, locks: [] });
      a.close(); foreign.close(); restore();
      console.log(JSON.stringify({ rows, busy, commitFailed, stillActive, automaticallyEnded, errors, ranked }));
    `, false);
    expect(result).toMatchObject({ busy: true, commitFailed: true, stillActive: true, automaticallyEnded: true, errors: [] });
    const begins = result.rows.filter((row: any) => row.verb === "BEGIN IMMEDIATE");
    expect(begins.map((row: any) => row.holdStatus)).toEqual([
      "complete-commit", "not-acquired", "complete-rollback", "complete-rollback", "inferred-automatic-rollback", "complete-commit", "bound-close",
    ]);
    expect(begins[0]).toMatchObject({ holdMs: 11, acquireAttemptMs: 0, holdUpperBoundMs: 11 });
    expect(begins[2]).toMatchObject({ holdMs: 18, holdUpperBoundMs: 18 });
    expect(begins[1].holdId).toBeUndefined();
    expect(begins[1].busy).toBe(true);
    expect(begins[4]).toMatchObject({ holdMs: null, holdUpperBoundMs: 5 });
    expect(begins[6]).toMatchObject({ holdMs: null, holdUpperBoundMs: 15 });
    expect(begins[0].database.connectionId).not.toBe(begins[2].database.connectionId);
    expect(begins[0].database.inode).toBe(begins[2].database.inode);
    expect(begins[5].database.scope).toBe("other-sqlite");
    expect(result.rows.find((row: any) => row.verb === "COMMIT" && !row.succeeded).pairing).toBe("failed-release-still-inflight");
    expect(result.ranked.reduce((sum: number, row: any) => sum + row.successfulBegins, 0)).toBe(6);
    expect(result.ranked.every((row: any) => row.successfulBegins === row.complete + row.inflight + row.inferred + row.unpaired)).toBe(true);
    expect(result.ranked.every((row: any) => row.origin === "sql-stack" && row.stacks.length > 0)).toBe(true);
  });

  it("detects native iterator automatic rollback and restores the original iterator prototype", () => {
    const result = run(`
      const file = path.join(mesh, "state.db"), db = new DatabaseSync(file), rows = [], errors = [];
      db.exec("CREATE TABLE fixture (value INTEGER UNIQUE); INSERT INTO fixture VALUES (1)");
      const originalNext = Object.getPrototypeOf(db.prepare("SELECT 1").iterate()).next;
      const restore = probe.captureTransactions(probe.databaseIdentity(file), () => ({ phase: "idle", batchId: null }),
        row => rows.push(row), { onError: where => errors.push(where) });
      db.exec("BEGIN IMMEDIATE");
      let failed = false;
      try { db.prepare("INSERT OR ROLLBACK INTO fixture VALUES (?) RETURNING value").iterate(1).next(); } catch { failed = true; }
      const ended = !db.isTransaction;
      restore();
      const restored = Object.getPrototypeOf(db.prepare("SELECT 1").iterate()).next === originalNext;
      db.close(); console.log(JSON.stringify({ rows, failed, ended, restored, errors }));
    `, false);
    expect(result).toMatchObject({ failed: true, ended: true, restored: true, errors: [] });
    expect(result.rows[0]).toMatchObject({ holdStatus: "inferred-automatic-rollback", holdMs: null });
  });

  it("records inflight, prepared transaction verbs, unpaired release failures and restore bounds explicitly", () => {
    const result = run(`
      const file = path.join(mesh, "state.db"), db = new DatabaseSync(file), rows = [];
      const original = DatabaseSync.prototype.exec;
      const restore = probe.captureTransactions(probe.databaseIdentity(file), () => ({ phase: "idle", batchId: null }), row => rows.push(row));
      try { db.exec("COMMIT"); } catch {}
      db.prepare("BEGIN IMMEDIATE").run();
      const inflight = JSON.parse(JSON.stringify(rows));
      restore();
      const restored = DatabaseSync.prototype.exec === original;
      db.exec("ROLLBACK"); db.close();
      console.log(JSON.stringify({ inflight, rows, restored }));
    `, false);
    expect(result.restored).toBe(true);
    expect(result.inflight[0]).toMatchObject({ succeeded: false, pairing: "unpaired-failed-release" });
    expect(result.inflight[1]).toMatchObject({ verb: "BEGIN IMMEDIATE", succeeded: true, holdStatus: "inflight", holdMs: null });
    expect(result.rows[1]).toMatchObject({ holdStatus: "bound-observer-restore", holdMs: null });
  });
});
