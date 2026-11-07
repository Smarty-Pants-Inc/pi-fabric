// Diagnostic observer only. Load before the production entry; never compose a runtime here.
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { AsyncLocalStorage } from "node:async_hooks";
import { DatabaseSync, StatementSync } from "node:sqlite";
import { types } from "node:util";
import { fileURLToPath, pathToFileURL } from "node:url";

const ownFile = fileURLToPath(import.meta.url);
const bounds = [0.1, 0.5, 1, 2, 5, 10, 25, 50, 100, 250, 1000, 5000];
const sameDatabase = (a, b) => a.file === b.file && a.device === b.device && a.inode === b.inode;
export function databaseIdentity(file) {
  const canonical = fs.realpathSync(file), stat = fs.statSync(canonical, { bigint: true });
  assert(stat.isFile(), "SQLite main database must be a regular file");
  return { file: canonical, device: String(stat.dev), inode: String(stat.ino) };
}
const location = frame => {
  const text = frame?.replace(/^at\s+/, "") ?? "";
  const value = text.endsWith(")") ? text.slice(text.lastIndexOf("(") + 1, -1) : text.replace(/^async\s+/, "");
  return /:\d+:\d+$/.test(value) ? value : "unknown-source";
};
const frames = normalize => normalize(new Error().stack ?? "").split("\n").slice(1)
  .map(line => line.trim()).filter(line => !line.includes(ownFile) && !line.includes("<observer>"))
  .slice(0, 24).map(line => line.slice(0, 2048));
const errorText = error => String(error);
const patch = (undo, object, key, value) => {
  const descriptor = Object.getOwnPropertyDescriptor(object, key);
  Object.defineProperty(object, key, { configurable: true, writable: true, value,
    enumerable: descriptor?.enumerable ?? false });
  undo.push(() => descriptor ? Object.defineProperty(object, key, descriptor) : Reflect.deleteProperty(object, key));
};
const restoreAll = undo => { for (const restore of undo.splice(0).reverse()) restore(); };
const verbOf = sql => {
  if (typeof sql !== "string") return null;
  const text = sql.replace(/\/\*[\s\S]*?\*\/|--[^\n]*/g, " ").trim().replace(/;\s*$/, "").trim().toUpperCase();
  if (/^BEGIN(?:\s+(?:DEFERRED|IMMEDIATE|EXCLUSIVE))?(?:\s+TRANSACTION)?$/.test(text)) return text;
  if (/^(?:COMMIT|END)(?:\s+TRANSACTION)?$/.test(text)) return "COMMIT";
  if (/^ROLLBACK(?:\s+TRANSACTION)?$/.test(text)) return "ROLLBACK";
  if (/^RELEASE\s+(?:SAVEPOINT\s+)?[^;]+$/.test(text)) return "RELEASE";
  return null;
};

// Kept as a standalone receiver-bound seam for the original probe. BEGIN elapsedMs is
// acquisition-call time, never custody. The successful BEGIN row is updated at release.
// Additional options are internal/additive; the three-argument contract remains intact.
export function captureTransactions(meshDatabase, stamp, record, options = {}) {
  const undo = [], connections = new WeakMap(), statements = new WeakMap(), live = new Map();
  const iterators = new WeakMap(), iteratorPrototypes = new WeakSet();
  const maxLive = options.maxLive ?? 100000;
  const now = options.now ?? (() => performance.now());
  const normalize = options.normalize ?? (value => value);
  const onError = options.onError ?? (() => {});
  let sequence = 0, holds = 0, restored = false;
  const safe = (where, fn) => { try { return fn(); } catch (error) { onError(where, error); } };
  const emit = row => safe("transaction-record", () => record(row));
  const identity = db => {
    let item = connections.get(db);
    if (item) return item;
    item = { connectionId: ++sequence, openedFile: null, scope: "unknown-sqlite" };
    connections.set(db, item);
    safe("database-identity", () => {
      try {
      // location() needs the actual receiver, not a duck-typed adapter. It does no SQL.
      if (typeof db.location === "function") item.openedFile = db.location("main");
      else if (!db.isTransaction) item.openedFile = db.prepare("PRAGMA database_list").all().find(row => row.name === "main")?.file;
      if (!item.openedFile || item.openedFile === ":memory:") { item.identityStatus = "unavailable-or-memory"; return; }
      Object.assign(item, databaseIdentity(item.openedFile));
      item.scope = sameDatabase(item, meshDatabase) ? "mesh-state" : "other-sqlite";
      Object.assign(item, options.classify?.(item) ?? {});
      item.identityStatus = "canonical-file-inode";
      } catch (error) { item.identityError = normalize(errorText(error)); throw error; }
    });
    return item;
  };
  const release = (database, status, at, event, exact) => {
    const begin = live.get(database.connectionId);
    if (!begin) { if (event) event.pairing = "unpaired-release"; return; }
    live.delete(database.connectionId);
    begin.holdStatus = status;
    begin.releaseMonotonicMs = at;
    begin.releasePhase = event?.phase ?? null;
    begin.releaseVerb = event?.verb ?? status;
    begin.holdUpperBoundMs = Math.max(0, at - begin.acquiredMonotonicMs);
    // COMMIT/ROLLBACK success is an observed boundary, unlike close/automatic rollback.
    begin.holdMs = exact ? begin.holdUpperBoundMs : null;
    if (event) { event.holdId = begin.holdId; event.pairing = status; }
    safe("transaction-release", () => options.onRelease?.(begin));
  };
  const checkAutomatic = (db, database, event) => {
    safe("transaction-state", () => {
      if (live.has(database.connectionId) && !db.isTransaction) {
        release(database, "inferred-automatic-rollback", now(), event, false);
      }
    });
  };
  const invoke = (db, sql, action) => {
    if (restored) return action();
    const database = identity(db), verb = verbOf(sql);
    const stack = verb ? frames(normalize) : [];
    const context = safe("transaction-stamp", stamp) ?? {};
    const row = verb ? { ...context, database, verb, stack, callSite: location(stack[0]), succeeded: false } : null;
    const began = now();
    let result, failed = true;
    try { result = action(); failed = false; return result; }
    catch (error) { if (row) safe("transaction-error", () => { row.error = normalize(errorText(error)); row.errorCode = error?.code ?? null; }); throw error; }
    finally {
      safe("transaction-observation", () => {
        const at = now();
        if (row) {
          row.succeeded = !failed; row.elapsedMs = at - began;
          if (verb.startsWith("BEGIN")) {
            row.writer = /\b(IMMEDIATE|EXCLUSIVE)\b/.test(verb);
            row.acquireAttemptMs = row.elapsedMs;
            row.busy = failed && /busy|locked/i.test(row.error ?? "");
            if (!failed) {
              if (live.has(database.connectionId)) release(database, "inferred-unobserved-release", at, null, false);
              row.holdId = ++holds; row.acquiredMonotonicMs = at; row.holdStatus = "inflight";
              row.holdMs = null;
              if (live.size < maxLive) live.set(database.connectionId, row);
              else { row.holdStatus = "unpaired-capacity"; onError("live-transactions-capacity", new Error("live transaction bound exceeded")); }
            } else row.holdStatus = "not-acquired";
          } else if (!failed) {
            if (verb === "RELEASE" && db.isTransaction) row.pairing = "savepoint-still-inflight";
            else release(database, verb === "COMMIT" ? "complete-commit" : verb === "ROLLBACK"
              ? "complete-rollback" : "inferred-release-savepoint", at, row, verb !== "RELEASE");
          } else row.pairing = live.has(database.connectionId) ? "failed-release-still-inflight" : "unpaired-failed-release";
          emit(row);
        }
        if (failed) checkAutomatic(db, database, row);
        // An unparsed compound statement can contain transaction boundaries. Never claim exact
        // timing for it. A native state observation can only bound the unobserved release.
        if (!verb && live.has(database.connectionId) && !db.isTransaction) {
          release(database, "inferred-unobserved-release", at, null, false);
        }
        if (!verb && typeof sql === "string" && /(?:^|;)\s*(?:BEGIN|COMMIT|END|ROLLBACK|RELEASE)\b/i.test(sql)) {
          onError("compound-or-unsupported-transaction-sql", new Error("transaction boundary not individually observed"));
        }
      });
    }
  };
  try {
    const exec = DatabaseSync.prototype.exec, prepare = DatabaseSync.prototype.prepare, close = DatabaseSync.prototype.close;
    patch(undo, DatabaseSync.prototype, "exec", function (sql, ...args) {
      return invoke(this, sql, () => exec.call(this, sql, ...args));
    });
    patch(undo, DatabaseSync.prototype, "prepare", function (sql, ...args) {
      const database = identity(this);
      try {
        const statement = prepare.call(this, sql, ...args);
        statements.set(statement, { db: this, sql });
        return statement;
      } catch (error) { checkAutomatic(this, database, null); throw error; }
    });
    for (const method of ["run", "get", "all"]) {
      const original = StatementSync.prototype[method];
      patch(undo, StatementSync.prototype, method, function (...args) {
        const statement = statements.get(this);
        return statement ? invoke(statement.db, statement.sql, () => original.apply(this, args)) : original.apply(this, args);
      });
    }
    const iterate = StatementSync.prototype.iterate;
    if (typeof iterate === "function") patch(undo, StatementSync.prototype, "iterate", function (...args) {
      const statement = statements.get(this);
      let iterator;
      try { iterator = iterate.apply(this, args); }
      catch (error) { if (statement) checkAutomatic(statement.db, identity(statement.db), null); throw error; }
      if (!statement) return iterator;
      // Return the original iterator, not a proxy. Only its shared native prototype is
      // instrumented, and it is restored with the other descriptors.
      iterators.set(iterator, statement);
      const prototype = Object.getPrototypeOf(iterator);
      safe("iterator-instrumentation", () => {
        if (iteratorPrototypes.has(prototype)) return;
        iteratorPrototypes.add(prototype);
        for (const method of ["next", "return"]) {
          const original = prototype[method]; if (typeof original !== "function") continue;
          patch(undo, prototype, method, function (...params) {
            const context = iterators.get(this);
            if (!context) return original.apply(this, params);
            // A transaction control statement in an iterator has an ambiguous execution
            // boundary. Explicitly reject exact attribution rather than invent a hold.
            if (verbOf(context.sql)) safe("iterator-transaction-boundary", () => onError("iterator-transaction-boundary", new Error("unsupported transaction iterator boundary")));
            return invoke(context.db, verbOf(context.sql) ? null : context.sql, () => original.apply(this, params));
          });
        }
      });
      return iterator;
    });
    patch(undo, DatabaseSync.prototype, "close", function (...args) {
      const database = identity(this), stack = frames(normalize);
      const row = { ...safe("close-stamp", stamp), database, verb: "CLOSE", stack, callSite: location(stack[0]), succeeded: false };
      const started = now();
      try { const result = close.apply(this, args); row.succeeded = true; return result; }
      catch (error) { safe("close-error", () => { row.error = normalize(errorText(error)); }); throw error; }
      finally {
        safe("close-observation", () => {
          row.elapsedMs = now() - started;
          if (row.succeeded) release(database, "bound-close", now(), row, false);
          else row.pairing = "failed-close-still-inflight";
          emit(row);
        });
      }
    });
  } catch (error) { restoreAll(undo); throw error; }
  const restore = () => {
    if (restored) return;
    for (const database of [...live.values()].map(row => row.database)) release(database, "bound-observer-restore", now(), null, false);
    restored = true; restoreAll(undo);
  };
  restore.live = live;
  return restore;
}

const histogram = () => Array(bounds.length + 1).fill(0);
const addSpan = (site, ms) => {
  if (typeof ms !== "number") return;
  site.holdMs += ms; site.maxHoldMs = Math.max(site.maxHoldMs, ms);
  let index = bounds.findIndex(bound => ms <= bound); if (index < 0) index = bounds.length;
  site.holdHistogram[index]++;
};
const quantile = (hist, q) => {
  const n = hist.reduce((a, b) => a + b, 0); if (!n) return null;
  let sum = 0;
  for (let i = 0; i < hist.length; i++) { sum += hist[i]; if (sum >= Math.ceil(n * q)) return bounds[i] ?? ">5000"; }
  return null;
};
const siteKey = row => JSON.stringify([row.phase, row.role, row.label, row.pid, row.activity, row.storeRoot, row.storeId, row.callSite]);
const batchKey = (row, id = row.batchId) => JSON.stringify([row.pid ?? null, id]);
const identityFields = row => ({ phase: row.phase, role: row.role, label: row.label, pid: row.pid, activity: row.activity ?? "background",
  storeRoot: row.storeRoot ?? null, storeId: row.storeId ?? null, storeID: row.storeId ?? null, storeScope: row.storeScope ?? null });
const validCapture = capture => !capture.dropped && !capture.observerErrorCount;
const variants = (site, row) => {
  const key = (row.stack ?? []).join("\n");
  let variant = site._variants.get(key);
  if (!variant) { variant = { stack: row.stack ?? [], count: 0 }; site._variants.set(key, variant); site.stacks.push(variant); }
  variant.count++;
};
const finishSites = sites => [...sites.values()].map(({ _variants, ...site }) => site);
export function aggregateSites(capture, phase = "idle") {
  // Index once, including dozens of overlapping calls. No per-call transaction scans.
  const txCounts = new Map(), lockCounts = new Map(), timing = new Map();
  const metrics = row => {
    const key = batchKey(row);
    if (!timing.has(key)) timing.set(key, { sqlHoldMs: 0, sqlHoldUpperBoundMs: 0, busyAttempts: 0,
      auxiliaryTransactions: 0, otherTransactions: 0, unknownTransactions: 0, lockWaitMs: 0, lockHoldMs: 0, lockFailures: 0 });
    return timing.get(key);
  };
  for (const row of capture.transactions) if (row.verb === "BEGIN IMMEDIATE") {
    const key = batchKey(row), item = metrics(row);
    if (row.busy) item.busyAttempts++;
    if (!row.succeeded) continue;
    if (row.database.scope === "mesh-state") txCounts.set(key, (txCounts.get(key) ?? 0) + 1);
    else if (row.database.scope === "unknown-sqlite") item.unknownTransactions++;
    else if (row.database.storeScope === "auxiliary-store") item.auxiliaryTransactions++;
    else item.otherTransactions++;
    item.sqlHoldMs += row.holdMs ?? 0; item.sqlHoldUpperBoundMs += row.holdUpperBoundMs ?? 0;
  }
  for (const row of capture.locks) {
    const key = batchKey(row), item = metrics(row);
    if (row.method === "acquired") {
      lockCounts.set(key, (lockCounts.get(key) ?? 0) + 1);
      item.lockWaitMs += row.waitMs ?? row.args?.[1] ?? 0; item.lockHoldMs += row.holdMs ?? row.args?.[2] ?? 0;
    } else if (row.method === "failed") { item.lockFailures++; item.lockWaitMs += row.failedWaitMs ?? row.args?.[1] ?? 0; }
  }
  const sites = new Map();
  for (const row of capture.calls) {
    if (row.phase !== phase) continue;
    const callSite = row.callSite ?? location(row.stack?.[0]), key = siteKey({ ...row, callSite });
    if (!sites.has(key)) sites.set(key, { ...identityFields(row), callSite, stacks: [], _variants: new Map(),
      count: 0, inputOps: 0, preparedOps: 0, resultOps: 0, appliedOps: 0, failures: 0,
      hooks: {}, hookErrors: {}, keys: {}, transactions: 0, lockAcquisitions: 0, elapsedMs: 0,
      hookMs: 0, classifications: {}, resultsUnknownFailures: 0, sqlHoldMs: 0, sqlHoldUpperBoundMs: 0, busyAttempts: 0,
      auxiliaryTransactions: 0, otherTransactions: 0, unknownTransactions: 0, lockWaitMs: 0, lockHoldMs: 0, lockFailures: 0,
      timingPolicy: "SQL hold and lock hold overlap; never sum them", valid: validCapture(capture) });
    const site = sites.get(key); variants(site, row); site.count++;
    site.inputOps += row.inputOpsCount ?? row.ops.length;
    site.preparedOps += row.preparedOpsCount ?? row.prepared.flat().length;
    site.resultOps += row.resultOpsCount ?? row.results?.length ?? 0;
    site.appliedOps += row.appliedOpsCount ?? row.results?.filter(result => result.applied).length ?? 0;
    if (row.succeeded === false) { site.failures++; if (!row.resultsKnown) site.resultsUnknownFailures++; }
    site.elapsedMs += row.elapsedMs ?? 0;
    site.hookMs += row.hookMs ?? row.hookRuns.reduce((sum, hook) => sum + (hook.elapsedMs ?? 0), 0);
    for (const [name, count] of Object.entries(row.hookAttempts ?? {})) site.hooks[name] = (site.hooks[name] ?? 0) + count;
    if (!row.hookAttempts) for (const hook of row.hookRuns) site.hooks[hook.name] = (site.hooks[hook.name] ?? 0) + 1;
    for (const [name, count] of Object.entries(row.hookErrors ?? {})) site.hookErrors[name] = (site.hookErrors[name] ?? 0) + count;
    for (const op of [...row.ops, ...row.prepared.flat()]) {
      const key = `${op.kind} ${op.key}`; site.keys[key] = (site.keys[key] ?? 0) + 1;
    }
    const batch = batchKey(row, row.id);
    site.transactions += txCounts.get(batch) ?? 0; site.lockAcquisitions += lockCounts.get(batch) ?? 0;
    for (const [name, value] of Object.entries(timing.get(batch) ?? {})) site[name] += value;
    const kind = row.classification ?? "unclassified"; site.classifications[kind] = (site.classifications[kind] ?? 0) + 1;
  }
  return finishSites(sites).sort((a, b) => b.count - a.count);
}
export function aggregateTransactions(capture, phase = "idle") {
  const sites = new Map(), calls = new Map(capture.calls.map(row => [batchKey(row, row.id), row]));
  for (const row of capture.transactions) {
    if (row.phase !== phase || !row.verb.startsWith("BEGIN")) continue;
    const batch = calls.get(batchKey(row));
    const callSite = row.batchCallSite ?? batch?.callSite ?? (batch ? location(batch.stack?.[0]) : row.callSite ?? location(row.stack?.[0]));
    const origin = row.batchId != null ? "live-batch" : row.lockClass ? "lock-seam+sql-stack" : "sql-stack";
    const key = JSON.stringify([siteKey({ ...row, callSite }), row.database.file, row.database.device, row.database.inode,
      row.database.scope, row.lockClass, origin]);
    if (!sites.has(key)) sites.set(key, { ...identityFields(row), callSite, origin, lockClass: row.lockClass ?? null,
      database: row.database, connectionIds: [], stacks: [], _variants: new Map(), attempts: 0, successfulBegins: 0,
      writerBegins: 0, failedBegins: 0, busyAttempts: 0, complete: 0, inflight: 0, inferred: 0, unpaired: 0,
      acquireAttemptMs: 0, failedAttemptMs: 0, holdMs: 0, maxHoldMs: 0, holdUpperBoundMs: 0,
      holdHistogram: histogram(), lockWaitMs: 0, failedLockWaitMs: 0, lockHoldMs: 0, correlatedLocks: 0, valid: validCapture(capture) });
    const site = sites.get(key); variants(site, row); site.attempts++;
    if (!site.connectionIds.includes(row.database.connectionId)) site.connectionIds.push(row.database.connectionId);
    site.acquireAttemptMs += row.acquireAttemptMs ?? row.elapsedMs ?? 0;
    if (!row.succeeded) {
      site.failedBegins++; site.failedAttemptMs += row.elapsedMs ?? 0; if (row.busy) site.busyAttempts++;
      site.failedLockWaitMs += row.failedLockSample?.waitMs ?? 0; continue;
    }
    site.successfulBegins++; if (row.writer !== false) site.writerBegins++;
    if (row.holdStatus?.startsWith("complete-")) site.complete++;
    else if (row.holdStatus === "inflight") site.inflight++;
    else if (row.holdStatus?.startsWith("bound-") || row.holdStatus?.startsWith("inferred-")) site.inferred++;
    else site.unpaired++;
    addSpan(site, row.holdMs); site.holdUpperBoundMs += row.holdUpperBoundMs ?? 0;
    if (row.lockSample) { site.correlatedLocks++; site.lockWaitMs += row.lockSample.waitMs; site.lockHoldMs += row.lockSample.holdMs; }
  }
  return finishSites(sites).map(site => ({ ...site, holdBoundsMs: bounds,
    p50HoldMs: quantile(site.holdHistogram, 0.5), p95HoldMs: quantile(site.holdHistogram, 0.95),
    p99HoldMs: quantile(site.holdHistogram, 0.99), quantileMethod: "fixed histogram upper bounds",
    totalWaitMs: site.lockWaitMs + site.failedLockWaitMs,
    waitPolicy: "matched acquired/failed seam includes retries and backoff; acquisition-call time is separate",
    timingPolicy: "SQL custody and correlated lock seam are separate, never summed" }))
    .sort((a, b) => b.holdMs - a.holdMs || b.successfulBegins - a.successfulBegins);
}

let observerActive = false;
export async function createMainBatchObserver({ candidate, root, mesh, role, label, maxRecords = 100000 }) {
  assert(!observerActive, "only one batch observer may own wrappers in a process");
  assert(Number.isSafeInteger(maxRecords) && maxRecords > 0 && maxRecords <= 1000000, "invalid record bound");
  assert(typeof role === "string" && typeof label === "string", "role and label required");
  const privateStat = fs.lstatSync(root);
  assert(privateStat.isDirectory() && !privateStat.isSymbolicLink(), "private root must be a physical directory");
  if (process.platform !== "win32") assert.equal(privateStat.mode & 0o777, 0o700, "private root must have mode 0700");
  const privateRoot = fs.realpathSync(root), sharedRoot = fs.realpathSync(mesh);
  const inside = file => { const rel = path.relative(privateRoot, file); return rel === "" || (!rel.startsWith(`..${path.sep}`) && rel !== ".." && !path.isAbsolute(rel)); };
  assert(inside(sharedRoot), "shared store must be inside private root");
  const canonical = file => {
    let current = path.resolve(file), suffix = [];
    while (!fs.existsSync(current)) { const parent = path.dirname(current); assert(parent !== current); suffix.unshift(path.basename(current)); current = parent; }
    const result = path.join(fs.realpathSync(current), ...suffix);
    assert(inside(result), "store escape outside private root");
    return result;
  };
  const normalize = value => String(value).replaceAll(ownFile, "<observer>")
    .replaceAll(path.resolve(candidate), "<candidate>").replaceAll(privateRoot, "<scratch>").slice(0, 32768);
  const als = new AsyncLocalStorage(), activityAls = new AsyncLocalStorage(), activeActivities = new Set();
  const active = new Map(), stores = new Set(), metadata = new WeakMap();
  const inventory = [], calls = [], locks = [], transactions = [], phases = [], observerErrors = [], undo = [];
  const drops = { calls: 0, locks: 0, transactions: 0, details: 0, phases: 0, observerErrors: 0 };
  const totals = { calls: 0, settledCalls: 0, failedCalls: 0, inputOps: 0, preparedOps: 0, resultOps: 0, appliedOps: 0,
    transactions: 0, beginAttempts: 0, successfulBegins: 0, failedBegins: 0, busyAttempts: 0, locks: 0, acquiredLocks: 0,
    completeHolds: 0, inferredHolds: 0, unpairedHolds: 0 };
  let phase = "startup", sequence = 0, storeSequence = 0, observerErrorCount = 0, restored = false, stopSql;
  const capDetail = Math.min(maxRecords, 256);
  const bounded = (kind, list, row) => { if (list.length < maxRecords) list.push(row); else drops[kind]++; };
  const problem = (where, error) => {
    observerErrorCount++;
    bounded("observerErrors", observerErrors, { where, error: normalize(errorText(error)), at: Date.now(), monotonicMs: performance.now(), phase, role, label, pid: process.pid });
  };
  const safe = (where, fn) => { try { return fn(); } catch (error) { problem(where, error); } };
  const liveBatch = () => { const row = als.getStore(); return row && active.get(row.id) === row ? row : null; };
  const stamp = () => {
    const row = liveBatch();
    return { at: Date.now(), monotonicMs: performance.now(), phase, role, label, pid: process.pid,
      activity: activeActivities.has(activityAls.getStore()) ? activityAls.getStore().name : "background",
      batchId: row?.id ?? null, batchCallSite: row?.callSite ?? null, lockClass: row?.lockClass ?? null,
      storeRoot: row?.storeRoot ?? null, storeId: row?.storeId ?? null, storeID: row?.storeId ?? null, storeScope: row?.storeScope ?? null };
  };
  const storeInfo = (store, assignedRoot = store.root) => {
    const storeRoot = canonical(assignedRoot);
    let info = metadata.get(store);
    if (info) { assert.equal(storeRoot, info.storeRoot, "store root changed during observation"); return info; }
    assert(stores.size < Math.max(128, maxRecords), "store inventory bound exceeded");
    info = { storeRoot, storeId: ++storeSequence, storeID: storeSequence,
      storeScope: storeRoot === sharedRoot ? "shared-mesh" : "auxiliary-store" };
    metadata.set(store, info); stores.add(store); inventory.push(info); return info;
  };
  const describe = ops => {
    if (!Array.isArray(ops)) return [];
    if (ops.length > capDetail) drops.details += ops.length - capDetail;
    return ops.slice(0, capDetail).map(op => ({ kind: normalize(op.kind).slice(0, 128), key: normalize(op.key).slice(0, 512),
      valueKind: typeof op.value, ...(op.ifVersion === undefined ? {} : { ifVersion: op.ifVersion }),
      ...(op.onConflict === undefined ? {} : { onConflict: typeof op.onConflict === "function" ? "function" : op.onConflict }),
      condition: typeof op.condition === "function" }));
  };
  const setPhase = name => {
    assert(typeof name === "string" && name.length > 0 && name.length <= 128, "invalid phase name");
    phase = name; const marker = { phase, at: Date.now(), monotonicMs: performance.now() };
    bounded("phases", phases, marker); return { ...marker };
  };
  const runActivity = (name, fn) => {
    assert(typeof name === "string" && name.length > 0 && name.length <= 128 && typeof fn === "function", "invalid activity");
    const activity = { name };
    if (activeActivities.size >= Math.max(128, maxRecords)) {
      problem("live-activities-capacity", new Error("live activity bound exceeded"));
      return activityAls.run(null, fn);
    }
    activeActivities.add(activity);
    return activityAls.run(activity, () => {
      let result;
      try { result = fn(); } catch (error) { activeActivities.delete(activity); throw error; }
      if (types.isPromise(result)) {
        try { Promise.prototype.then.call(result,
          () => { activeActivities.delete(activity); }, () => { activeActivities.delete(activity); }); }
        catch (error) { activeActivities.delete(activity); problem("activity-settlement", error); }
      } else activeActivities.delete(activity);
      return result;
    });
  };
  const pendingHolds = new Map(), failedAttempts = new Map();
  const pendingKey = row => JSON.stringify([row.batchId, row.database?.storeRoot ?? row.storeRoot]);
  const onRelease = row => {
    if (row.holdStatus.startsWith("complete-")) totals.completeHolds++; else totals.inferredHolds++;
    // Only state.db writer holds can correspond to this seam. A read transaction or a
    // different database in the same directory is not a candidate for correlation.
    if (row.writer === false || !row.database.file || path.basename(row.database.file) !== "state.db"
      || !inventory.some(item => item.storeRoot === row.database.storeRoot)) return;
    // One queue per live batch/store, consumed once by the existing acquired seam.
    const key = pendingKey(row);
    if (pendingHolds.size >= Math.max(128, maxRecords) && !pendingHolds.has(key)) {
      problem("lock-correlation-capacity", new Error("pending hold bound exceeded")); return;
    }
    let queue = pendingHolds.get(key); if (!queue) pendingHolds.set(key, queue = []);
    if (queue.length < capDetail) queue.push(row); else { drops.details++; problem("lock-correlation-capacity", new Error("pending hold queue exceeded")); }
  };
  try {
    observerActive = true; setPhase("startup");
    const meshDatabase = databaseIdentity(path.join(sharedRoot, "state.db"));
    stopSql = captureTransactions(meshDatabase, stamp, row => {
      const database = row.database;
      if (!row.storeRoot || row.storeRoot !== database.storeRoot) {
        row.storeRoot = database.storeRoot ?? null; row.storeScope = database.storeScope ?? null;
        const matches = inventory.filter(item => item.storeRoot === row.storeRoot);
        row.storeId = matches.length === 1 ? matches[0].storeId : null; row.storeID = row.storeId;
      }
      totals.transactions++;
      if (row.verb.startsWith("BEGIN")) {
        totals.beginAttempts++;
        if (row.succeeded) { totals.successfulBegins++; if (row.holdStatus === "unpaired-capacity") totals.unpairedHolds++; }
        else {
          totals.failedBegins++; if (row.busy) totals.busyAttempts++;
          if (database.file && path.basename(database.file) === "state.db") {
            if (failedAttempts.size < Math.max(128, maxRecords) || failedAttempts.has(pendingKey(row))) failedAttempts.set(pendingKey(row), row);
            else problem("failed-attempt-correlation-capacity", new Error("failed attempt bound exceeded"));
          }
        }
      }
      bounded("transactions", transactions, row);
    }, { normalize, onError: problem, onRelease, maxLive: Math.max(128, maxRecords), classify: database => {
      const storeRoot = path.dirname(database.file);
      return { storeRoot, storeScope: !inside(storeRoot) ? "foreign-database" : path.basename(database.file) !== "state.db"
        ? "other-private-sqlite" : storeRoot === sharedRoot ? "shared-mesh" : "auxiliary-store" };
    } });
    undo.push(stopSql);
    const { MeshStore } = await import(pathToFileURL(path.join(candidate, "dist/mesh.js")).href);
    const prototype = MeshStore.prototype;
    // The compiled public class assigns root before creating any backend, directory or census.
    // Preserve that ordinary own data property, but reject a constructor escape before its I/O.
    const rootDescriptor = Object.getOwnPropertyDescriptor(prototype, "root");
    assert(!rootDescriptor, "unexpected MeshStore root descriptor");
    Object.defineProperty(prototype, "root", { configurable: true, set(value) {
      storeInfo(this, value);
      Object.defineProperty(this, "root", { value, writable: true, enumerable: true, configurable: true });
    } });
    undo.push(() => Reflect.deleteProperty(prototype, "root"));
    const originalBatch = prototype.writeBatch;
    for (const name of Object.getOwnPropertyNames(prototype)) {
      const descriptor = Object.getOwnPropertyDescriptor(prototype, name);
      if (name === "constructor" || name === "writeBatch" || typeof descriptor.value !== "function") continue;
      const original = descriptor.value;
      patch(undo, prototype, name, function (...args) { storeInfo(this); return original.apply(this, args); });
    }
    patch(undo, prototype, "writeBatch", function (input) {
      const info = storeInfo(this), parent = liveBatch(), stack = frames(normalize);
      const row = { ...stamp(), ...info, id: ++sequence, parentBatchId: parent?.id ?? null, stack, callSite: location(stack[0]),
        ops: [], hooks: {}, lockClass: input.lockClass ?? "writeBatch", lockTiming: null, sqliteHoldTag: input.sqliteHoldTag ?? null,
        prepared: [], hookRuns: [], hookAttempts: {}, hookErrors: {}, preparedOpsCount: 0, hookMs: 0, succeeded: null };
      row.batchId = row.id;
      totals.calls++; row.inputOpsCount = Array.isArray(input.ops) ? input.ops.length : 0; totals.inputOps += row.inputOpsCount;
      safe("batch-input", () => { row.ops = describe(input.ops); row.lockTiming = input.lockTiming ? { ...input.lockTiming } : null; });
      bounded("calls", calls, row);
      const noteResults = (result, source) => {
        if (!Array.isArray(result)) return;
        totals.resultOps -= row.resultOpsCount ?? 0; totals.appliedOps -= row.appliedOpsCount ?? 0;
        row.resultsKnown = true; row.resultsSource = source;
        row.resultOpsCount = result.length; row.appliedOpsCount = result.reduce((sum, item) => sum + (item.applied ? 1 : 0), 0);
        totals.resultOps += result.length; totals.appliedOps += row.appliedOpsCount;
        if (result.length > capDetail) drops.details += result.length - capDetail;
        row.results = result.slice(0, capDetail).map(({ key, applied, version }) => ({ key, applied, version }));
      };
      const wrapped = { ...input };
      for (const name of ["prepare", "beforeCommit", "afterCommit", "commitOutbox"]) {
        const hook = input[name]; row.hooks[name] = typeof hook === "function";
        if (!row.hooks[name]) continue;
        wrapped[name] = function (...args) {
          const run = { ...stamp(), name, succeeded: false }, began = performance.now();
          row.hookAttempts[name] = (row.hookAttempts[name] ?? 0) + 1;
          if (row.hookRuns.length < capDetail) row.hookRuns.push(run); else drops.details++;
          try {
            if (name === "commitOutbox") safe("outbox-results", () => noteResults(args[0]?.results, "commitOutbox"));
            const result = hook.apply(this, args);
            safe("hook-result", () => {
              run.returnKind = Array.isArray(result) ? "array" : types.isPromise(result) ? "promise" : typeof result;
              if (name === "prepare" && Array.isArray(result)) {
                run.preparedOpsCount = result.length; row.preparedOpsCount += result.length; totals.preparedOps += result.length;
                if (row.prepared.length < capDetail) row.prepared.push(describe(result)); else drops.details++;
              }
            });
            run.succeeded = true; return result;
          } catch (error) {
            row.hookErrors[name] = (row.hookErrors[name] ?? 0) + 1;
            safe("hook-error", () => { run.error = normalize(errorText(error)); }); throw error;
          } finally { run.elapsedMs = performance.now() - began; row.hookMs += run.elapsedMs; }
        };
      }
      const began = performance.now();
      const settle = (succeeded, result) => {
        active.delete(row.id); delete row.promise;
        const correlationKey = JSON.stringify([row.id, row.storeRoot]);
        pendingHolds.delete(correlationKey); failedAttempts.delete(correlationKey);
        row.succeeded = succeeded; row.elapsedMs = performance.now() - began; row.endedAt = Date.now(); row.endedMonotonicMs = performance.now();
        totals.settledCalls++; if (!succeeded) totals.failedCalls++;
        safe("batch-result", () => {
          if (!succeeded) row.error = normalize(errorText(result));
          else noteResults(result, "return");
          row.classification = row.appliedOpsCount > 0 ? "applied-work" : !succeeded && !row.resultsKnown ? "failed-results-unknown"
            : row.inputOpsCount > 0 ? "input-unapplied"
            : row.preparedOpsCount > 0 ? "prepared-unapplied" : row.hookAttempts.prepare ? "prepared-empty"
            : Object.values(row.hooks).some(Boolean) ? "hook-only" : "hookless-empty";
        });
      };
      if (active.size < Math.max(128, maxRecords)) active.set(row.id, row);
      else problem("live-batches-capacity", new Error("live batch bound exceeded"));
      return als.run(row, () => {
        let result;
        try { result = originalBatch.call(this, wrapped); }
        catch (error) { settle(false, error); throw error; }
        if (types.isPromise(result)) {
          row.promise = result;
          try { Promise.prototype.then.call(result, value => settle(true, value), error => settle(false, error)); }
          catch (error) { active.delete(row.id); delete row.promise; problem("batch-settlement", error); }
        } else settle(true, result);
        return result;
      });
    });
    const registry = globalThis[Symbol.for("pi-fabric.mesh.lock-stats")];
    assert(registry?.stats, "existing lock recorder unavailable; enable lock statistics before loading the candidate");
    for (const method of ["acquired", "failed", "phase", "fenceRefused"]) {
      const original = registry.stats[method]; if (typeof original !== "function") continue;
      patch(undo, registry.stats, method, function (storeRoot, ...args) {
        // Lock metrics for unrelated databases remain production-owned, not observer admission.
        const result = original.call(this, storeRoot, ...args);
        safe("lock-observation", () => {
          let actual; try { actual = canonical(storeRoot); } catch { return; }
          const stack = frames(normalize), row = { ...stamp(), storeRoot: actual,
            storeScope: actual === sharedRoot ? "shared-mesh" : "auxiliary-store", method, args: args.slice(0, 8), stack, callSite: location(stack[0]) };
          if (liveBatch()?.storeRoot !== actual) { row.storeId = null; row.storeID = null; }
          totals.locks++;
          if (method === "failed") {
            row.lockClass = args[0]; row.failedWaitMs = args[1]; row.tried = args[2];
            const key = JSON.stringify([row.batchId, actual]), attempt = failedAttempts.get(key);
            if (attempt) { attempt.failedLockSample = { waitMs: row.failedWaitMs, tried: row.tried }; attempt.lockClass ??= row.lockClass;
              row.connectionId = attempt.database.connectionId; row.correlation = "same-batch-store-last-failed-attempt"; failedAttempts.delete(key); }
          }
          if (method === "acquired") {
            failedAttempts.delete(JSON.stringify([row.batchId, actual]));
            totals.acquiredLocks++; row.lockClass = args[0]; row.waitMs = args[1]; row.holdMs = args[2];
            const key = JSON.stringify([row.batchId, actual]), queue = pendingHolds.get(key), begin = queue?.shift();
            if (queue?.length === 0) pendingHolds.delete(key);
            row.correlation = begin ? "same-batch-store-release-order" : "unpaired-lock-seam";
            if (begin) { row.sqlHoldId = begin.holdId; row.connectionId = begin.database.connectionId;
              begin.lockClass ??= row.lockClass;
              begin.lockSample = { waitMs: row.waitMs, holdMs: row.holdMs, lockClass: row.lockClass, method, correlation: row.correlation }; }
          }
          bounded("locks", locks, row);
        });
        return result;
      });
    }
  } catch (error) {
    try { restoreAll(undo); } finally { observerActive = false; }
    throw error;
  }
  const restore = () => {
    if (restored) return; restored = true;
    try { restoreAll(undo); } finally { observerActive = false; als.disable(); activityAls.disable(); activeActivities.clear(); pendingHolds.clear(); failedAttempts.clear(); }
  };
  return { stores, setPhase, runActivity, restore,
    getCapture() {
      // No store calls, SQL, filesystem scans, fresh-state reads or readiness checks here.
      const dropped = Object.values(drops).reduce((sum, count) => sum + count, 0);
      const snapshot = { schema: 3, role, label, pid: process.pid, phase, phases, calls, locks, transactions,
        stores: inventory, dropped, drops: { ...drops }, observerErrorCount, observerErrors,
        totals: { ...totals, inflightHolds: stopSql.live.size }, liveBatches: active.size,
        rankValid: dropped === 0 && observerErrorCount === 0,
        limitations: ["Raw records and nested detail have independent finite caps; any drop invalidates ranking.",
          "Close and automatic/unobserved rollback are release bounds, not exact holds.",
          "SQL custody and the acquired lock seam overlap and must not be added.",
          "Snapshots/readiness belong to the caller outside measurement phases."] };
      // Promises and live references are deliberately excluded; returned snapshots cannot be mutated by later hooks.
      return JSON.parse(JSON.stringify(snapshot, (key, value) => key === "promise" ? undefined : typeof value === "bigint" ? String(value) : value));
    },
    async drain(timeoutMs) {
      assert(Number.isFinite(timeoutMs) && timeoutMs >= 0 && timeoutMs <= 60000, "invalid drain bound");
      // Event-driven ONCE: new calls admitted after this snapshot are not part of this drain.
      const promises = [...active.values()].map(row => row.promise).filter(Boolean);
      let timer;
      const timedOut = await Promise.race([Promise.allSettled(promises).then(() => false),
        new Promise(resolve => { timer = setTimeout(() => resolve(true), timeoutMs); })]);
      clearTimeout(timer);
      return { timedOut, pendingAtStart: promises.length, liveBatches: active.size };
    } };
}
