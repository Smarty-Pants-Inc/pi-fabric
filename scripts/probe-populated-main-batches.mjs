#!/usr/bin/env node
// Attribution-only native cohort. Run under the caller's owned process-group supervisor.
// No detach, existing mesh, remote model, production credential or runtime accessor.
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import readline from "node:readline";
import { spawn, execFileSync } from "node:child_process";
import { fileURLToPath, pathToFileURL } from "node:url";
import {
  sha256, digest, canonicalDigest, parseArguments, populationPlan, temporaryParent, prepareOutput, privateDirectory,
  freshJson, pinFile, verifyPin, readDependencyGate, loadDependencies, candidatePins, sdkEntryFromCli,
  createNativeJournals, semanticPopulation, writeActorRegistries, fabricSettings, bootstrapCode,
  trafficSchedule, requireReady, assertCommonWindow, validateChildExit, databaseIdentity, sameDatabase, beneath, noLinks, eventTarget, selectCandidates,
} from "./populated-main-fixture.mjs";

const self = fileURLToPath(import.meta.url);
const fixtureFile = fileURLToPath(new URL("./populated-main-fixture.mjs", import.meta.url));
const providerFile = fileURLToPath(new URL("../tests/fixtures/populated-main-cli-provider.ts", import.meta.url));
const sleep = ms => new Promise(resolve => setTimeout(resolve, Math.max(0, ms)));
const errorText = error => error?.stack ?? String(error);
const openPrivate = file => fs.openSync(file, fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_WRONLY | (fs.constants.O_NOFOLLOW ?? 0), 0o600);

export class Deadline {
  constructor(milliseconds) {
    assert(milliseconds > 0 && milliseconds <= 1590000, "global deadline must be finite");
    this.endsAt = Date.now() + milliseconds;
    this.controller = new AbortController();
    this.failure = new Promise((_, reject) => { this.reject = reject; }); this.failure.catch(() => {});
    this.timer = setTimeout(() => this.fail(new Error("global cohort deadline exceeded")), milliseconds);
  }
  fail(error) { if (!this.controller.signal.aborted) { this.controller.abort(error); this.reject(error); } }
  async within(promise, ms, label) {
    const remaining = Math.min(ms, this.endsAt - Date.now()); assert(remaining > 0, `deadline before ${label}`);
    let timer;
    try { return await Promise.race([promise, this.failure, new Promise((_, reject) => {
      timer = setTimeout(() => { const error = new Error(`bounded operation expired: ${label}`); this.fail(error); reject(error); }, remaining);
    })]); } finally { clearTimeout(timer); }
  }
  async until(at) { await this.within(sleep(at - Date.now()), Math.max(1, at - Date.now() + 50), "scheduled boundary"); }
  close() { clearTimeout(this.timer); }
}

export class PhaseClock {
  constructor(observer, setEnvironment = () => {}, now = () => Date.now(), monotonic = () => performance.now()) {
    this.observer = observer; this.setEnvironment = setEnvironment; this.now = now; this.monotonic = monotonic;
    this.phase = "startup"; this.windows = {}; this.timers = []; this.next = "idle";
  }
  stamp() { return { at: this.now(), monotonicMs: this.monotonic() }; }
  set(phase) { this.phase = phase; this.setEnvironment(phase); return this.observer.setPhase(phase); }
  arm({ phase, startAt, endAt }) {
    assert.equal(phase, this.next, "phase order or duplicate arm");
    assert(this.phase === "startup" || this.phase === "post-idle", "phase already active");
    assert(startAt > this.now() + 50 && endAt > startAt && endAt - startAt <= 601000, "invalid common-clock window");
    const window = {}; this.windows[phase] = window; this.next = phase === "idle" ? "traffic" : "done";
    this.timers.push(setTimeout(() => { window.start = this.stamp(); this.set(phase); }, startAt - this.now()));
    this.timers.push(setTimeout(() => { window.end = this.stamp(); this.set(`post-${phase}`); }, endAt - this.now()));
    return { phase, startAt, endAt };
  }
  outsideWindow() { assert(!["idle", "traffic"].includes(this.phase), "observer snapshot inside measurement window refused"); }
  close() { for (const timer of this.timers) clearTimeout(timer); }
}

const serializedRows = rows => rows.map(row => ({ ...row, value: row.value }));
function storeInventory(observer, spec) {
  return [...observer.stores].map((store, index) => {
    assert(beneath(spec.root, fs.realpathSync(store.root)), "store escaped owned fixture");
    const diagnostics = store.stateDiagnostics();
    const identity = store.stateBackend === "sqlite" && typeof diagnostics.database === "string"
      ? databaseIdentity(diagnostics.database) : null;
    return { storeId: index, root: fs.realpathSync(store.root), backend: store.stateBackend,
      diagnostics, databaseIdentity: identity, scope: sameDatabase(identity, spec.sharedDatabase) ? "shared-main" : "auxiliary" };
  });
}
const sharedStore = (observer, spec) => {
  const store = [...observer.stores].find(store => fs.realpathSync(store.root) === fs.realpathSync(spec.mesh));
  assert(store, "actual shared MeshStore was not observed"); return store;
};

export const observedKey = key => typeof key === "string" && (key.startsWith("topology/participants/") || key.startsWith("actors/"));

// A small passive seam, composed AFTER the frozen transaction observer. Only
// strings/numbers returned by native #slot/kvGet and already-bound #applyPut
// arguments are retained under custody. No SELECT, hashing, JSON or file I/O is
// added there. The frozen observer still owns stacks, batch hooks and timing.
export function installHistoricalAudit(spec, phase, limits = {}) {
  const { DatabaseSync, StatementSync } = process.getBuiltinModule("node:sqlite");
  const originals = { prepare: DatabaseSync.prototype.prepare, exec: DatabaseSync.prototype.exec,
    close: DatabaseSync.prototype.close, run: StatementSync.prototype.run, get: StatementSync.prototype.get, all: StatementSync.prototype.all };
  const maxRecords = limits.maxRecords ?? 100000, maxBytes = limits.maxBytes ?? 64 * 1024 * 1024;
  const maxKeys = limits.maxKeys ?? 256, maxLive = 128;
  for (const limit of [maxRecords, maxBytes, maxKeys]) assert(Number.isSafeInteger(limit) && limit > 0);
  const statements = new WeakMap(), connections = new WeakMap(), live = new Set(), queue = [];
  const fd = openPrivate(path.join(spec.directory, "historical-sql.jsonl"));
  let sequence = 0, transactionSequence = 0, count = 0, bytes = 0, readBytes = 0, dropped = 0, errors = 0, closed = false;
  let unknownDatabase = 0, foreignStatements = 0, unsupported = 0, unpaired = 0, flushed = 0;
  const safe = fn => { try { return fn(); } catch { errors++; } };
  const connection = db => {
    let item = connections.get(db);
    if (item) return item;
    item = { receiverId: ++sequence, identity: null, scope: "unknown-sqlite", tx: null };
    connections.set(db, item);
    // No PRAGMA fallback. A receiver first encountered inside custody is UNKNOWN.
    if (!db.isTransaction && live.size === 0) safe(() => {
      const file = db.location?.("main");
      if (file && file !== ":memory:") {
        item.identity = databaseIdentity(file);
        item.scope = sameDatabase(item.identity, spec.sharedDatabase) ? "selected-primary" : "foreign-database";
      }
    });
    return item;
  };
  const finish = (item, status, before, after) => {
    if (!item.tx) return;
    Object.assign(item.tx, { status, releaseAt: Date.now(), releaseStartedMonotonicMs: before, releaseEndedMonotonicMs: after });
    readBytes -= item.tx.readBytes; item.tx.readBytes = 0;
    item.tx.reads.clear(); live.delete(item.tx); item.tx = null;
  };
  const classify = sql => {
    const text = String(sql).trim().replace(/;\s*$/, "").replace(/\s+/g, " ");
    if (/^BEGIN(?: (?:IMMEDIATE|EXCLUSIVE|DEFERRED))?(?: TRANSACTION)?$/i.test(text)) return /IMMEDIATE|EXCLUSIVE/i.test(text) ? "writer-begin" : "read-begin";
    if (/^(COMMIT|END)(?: TRANSACTION)?$/i.test(text)) return "commit";
    if (/^ROLLBACK(?: TRANSACTION)?$/i.test(text)) return "rollback";
    if (/^SELECT key, value, version, updated_at, updated_by, bytes FROM kv WHERE key = \?$/i.test(text)) return "existing-row";
    if (/^INSERT INTO kv\(key, value, version, updated_at, updated_by, bytes\) VALUES \(\?, \?, \?, \?, \?, \?\) ON CONFLICT\(key\) DO UPDATE SET value = excluded.value, version = excluded.version, updated_at = excluded.updated_at, updated_by = excluded.updated_by, bytes = excluded.bytes$/i.test(text)) return "upsert";
    if (/^DELETE FROM kv WHERE key = \?$/i.test(text)) return "delete";
    // Unrecognised KV mutation, savepoint or compound boundary invalidates proof,
    // rather than guessing what it did. Ordinary metadata/feed statements are ignored.
    if (/\b(SAVEPOINT|RELEASE|ROLLBACK|BEGIN|COMMIT|END)\b/i.test(text) || (/\bkv\b/i.test(text) && !/^SELECT\b/i.test(text))) return "unsupported";
    return "other";
  };
  const touch = (tx, key) => {
    if (!tx) return;
    const prior = tx.reads.get(key);
    if (prior) { readBytes -= prior.cost; tx.readBytes -= prior.cost; tx.reads.delete(key); }
    if (!tx.keys.has(key) && tx.keys.size >= maxKeys) { tx.tainted = true; dropped++; return; }
    tx.keys.set(key, (tx.keys.get(key) ?? 0) + 1);
  };
  const invoke = (db, kind, method, args, action) => {
    const item = connection(db), beforeTime = performance.now();
    let result, succeeded = false;
    try { result = action(); succeeded = true; return result; }
    finally { safe(() => {
      const afterTime = performance.now();
      if ((kind === "writer-begin" || kind === "read-begin") && succeeded) {
        if (item.tx) { unpaired++; finish(item, "unobserved-release", beforeTime, afterTime); }
        if (live.size >= maxLive) { dropped++; return; }
        item.tx = { id: ++transactionSequence, writer: kind === "writer-begin", status: "inflight", tainted: false,
          beginStartedMonotonicMs: beforeTime, beginEndedMonotonicMs: afterTime, reads: new Map(), readBytes: 0, keys: new Map() };
        live.add(item.tx);
      } else if ((kind === "commit" || kind === "rollback") && succeeded) {
        finish(item, kind === "commit" ? "complete-commit" : "complete-rollback", beforeTime, afterTime);
      } else if (kind === "unsupported" && item.scope === "selected-primary") {
        unsupported++;
        if (item.tx) { item.tx.tainted = true; readBytes -= item.tx.readBytes; item.tx.readBytes = 0; item.tx.reads.clear(); }
      } else if (succeeded && observedKey(args[0]) && ["existing-row", "upsert", "delete"].includes(kind)) {
        if (item.scope !== "selected-primary") {
          if (item.scope === "foreign-database") foreignStatements++; else unknownDatabase++;
        } else {
          const tx = item.tx, key = args[0];
          if (kind === "existing-row" && method === "get" && tx?.writer) {
            const cost = 512 + 2 * (key.length + (result?.value?.length ?? 0) + (result?.updated_by?.length ?? 0));
            const prior = tx.reads.get(key), oldCost = prior?.cost ?? 0;
            if ((tx.reads.size >= maxKeys && !prior) || bytes + readBytes - oldCost + cost > maxBytes) {
              dropped++; tx.tainted = true;
              if (prior) { readBytes -= oldCost; tx.readBytes -= oldCost; tx.reads.delete(key); }
            } else {
              readBytes += cost - oldCost; tx.readBytes += cost - oldCost;
              tx.reads.set(key, result === undefined ? { present: false, monotonicMs: afterTime, cost } : {
                present: true, key: result.key, value: result.value, version: result.version,
                updatedAt: result.updated_at, updatedBy: result.updated_by, monotonicMs: afterTime, cost });
            }
          } else if (kind === "upsert" && method === "run") {
            count++;
            const prior = tx?.reads.get(key), rawBytes = 1024 + 2 * (key.length + (args[1]?.length ?? 0) + (args[4]?.length ?? 0) +
              (prior?.value?.length ?? 0) + (prior?.updatedBy?.length ?? 0));
            touch(tx, key);
            if (count > maxRecords || bytes + readBytes + rawBytes > maxBytes) { dropped++; return; }
            bytes += rawBytes;
            queue.push({ key, value: args[1], version: args[2], metadataUpdatedAt: args[3], updatedBy: args[4],
              prior, tx, databaseIdentity: item.identity, receiverId: item.receiverId, at: Date.now(), monotonicMs: beforeTime,
              endedMonotonicMs: afterTime, phase: phase(), pid: process.pid, label: spec.owner.label, role: spec.owner.role,
              statementSucceeded: true });
          } else if (kind === "delete") touch(tx, key);
        }
      }
      // An implicit rollback/compound release cannot leave stale pre-state usable.
      if (item.tx && !db.isTransaction) { unpaired++; finish(item, "unobserved-release", beforeTime, afterTime); }
    }); }
  };
  DatabaseSync.prototype.prepare = function (sql, ...args) {
    connection(this); // identity resolution is outside BEGIN, never in an upsert wrapper
    const statement = originals.prepare.call(this, sql, ...args);
    statements.set(statement, { db: this, kind: classify(sql) }); return statement;
  };
  DatabaseSync.prototype.exec = function (sql, ...args) {
    return invoke(this, classify(sql), "exec", [], () => originals.exec.call(this, sql, ...args));
  };
  for (const method of ["get", "run", "all"]) StatementSync.prototype[method] = function (...args) {
    const statement = statements.get(this);
    return statement ? invoke(statement.db, statement.kind, method, args, () => originals[method].apply(this, args)) : originals[method].apply(this, args);
  };
  DatabaseSync.prototype.close = function (...args) {
    const result = originals.close.apply(this, args), item = connections.get(this);
    if (item?.tx) { unpaired++; finish(item, "closed-uncommitted", performance.now(), performance.now()); }
    return result;
  };
  const quality = () => ({ count, flushed, queued: queue.length, bufferedBytes: bytes, pendingReadBytes: readBytes, dropped, errors, unknownDatabase,
    foreignStatements, unsupported, unpaired, liveTransactions: live.size,
    complete: !dropped && !errors && !unknownDatabase && !unsupported && !unpaired && live.size === 0 && queue.length === 0 });
  const flush = () => {
    assert(!["idle", "traffic"].includes(phase()) && live.size === 0, "audit flush requires outside-window, released custody");
    // Deferred JSON/hash work only. Raw strings are retained in the evidence too.
    for (const raw of queue.splice(0)) {
      try {
        const { tx, prior, ...row } = raw;
        const describe = (value, author) => {
          const payload = JSON.parse(value), identity = JSON.parse(author);
          return { payloadSha256: digest(value), payloadCanonicalSha256: canonicalDigest(payload),
            updatedBySha256: digest(author), updatedByCanonicalSha256: canonicalDigest(identity), updatedById: identity.id,
            payloadUpdatedAt: payload?.updatedAt, payloadStartedAt: payload?.startedAt };
        };
        const repeatedKeyInTransaction = (tx?.keys.get(row.key) ?? 0) > 1;
        const known = !!tx?.writer && !tx.tainted && !!prior && !repeatedKeyInTransaction &&
          (!prior.present || (prior.key === row.key && typeof prior.value === "string" && typeof prior.updatedBy === "string" &&
            Number.isSafeInteger(prior.version) && row.version > prior.version));
        const event = { ...row, ...describe(row.value, row.updatedBy), prior: prior ?? null,
          priorDigests: prior?.present ? describe(prior.value, prior.updatedBy) : null,
          transaction: tx ? { id: tx.id, writer: tx.writer, status: tx.status, tainted: tx.tainted,
            beginStartedMonotonicMs: tx.beginStartedMonotonicMs, beginEndedMonotonicMs: tx.beginEndedMonotonicMs,
            releaseAt: tx.releaseAt, releaseStartedMonotonicMs: tx.releaseStartedMonotonicMs, releaseEndedMonotonicMs: tx.releaseEndedMonotonicMs } : null,
          repeatedKeyInTransaction, beforeEvidence: known ? "same-receiver-native-read-in-writer" : "UNKNOWN",
          rawValueEqual: known && prior.present ? prior.value === row.value : null,
          rawAuthorEqual: known && prior.present ? prior.updatedBy === row.updatedBy : null };
        fs.writeSync(fd, JSON.stringify(event) + "\n"); flushed++;
      } catch { errors++; }
    }
    bytes = 0; return quality();
  };
  return { quality, flush, close() {
    if (closed) return;
    try { flush(); }
    finally {
      closed = true;
      for (const method of ["prepare", "exec", "close"]) DatabaseSync.prototype[method] = originals[method];
      for (const method of ["get", "run", "all"]) StatementSync.prototype[method] = originals[method];
      fs.closeSync(fd);
      freshJson(path.join(spec.directory, "historical-sql-quality.json"), quality());
    }
  } };
}

async function nativeHostRun(pi, observer, input, milliseconds = 15000) {
  return observer.runActivity("fixture-driver", () => new Promise((resolve, reject) => {
    const controller = new AbortController();
    const timer = setTimeout(() => { controller.abort(); reject(new Error(`native host program timed out: ${input.correlationId}`)); }, milliseconds);
    pi.events.emit("pi-fabric:program:run:v1", { ref: "cohort-driver", input, signal: controller.signal,
      reply: reply => { clearTimeout(timer); reply.ok ? resolve(reply.value) : reject(new Error(`native host program failed: ${reply.error}`)); } });
  }));
}

function commandOutput(spec, packet, result) {
  assert(/^[a-z0-9-]{1,80}$/.test(packet.id), "invalid command receipt id");
  freshJson(path.join(spec.directory, `${packet.id}.json`), { id: packet.id, at: Date.now(), ...result });
  freshJson(path.join(spec.directory, `${packet.id}.done`), { complete: true });
}

export function ownRootBoundary(mesh, spec, native, rootParticipant, phase) {
  const key = `topology/participants/${digest(native.rootId)}`;
  const read = file => {
    try {
      noLinks(file); const raw = fs.readFileSync(file, "utf8"), stat = fs.statSync(file);
      return { file, bytes: stat.size, mtimeMs: stat.mtimeMs, rawSha256: digest(raw), value: JSON.parse(raw) };
    } catch (error) { if (error.code === "ENOENT") return { file, absent: true }; throw error; }
  };
  const kv = mesh.get(key, { fresh: true }) ?? null;
  const file = read(path.join(spec.mesh, "participants", `${digest(native.rootId)}.json`));
  const hostId = rootParticipant?.ownerHostId ?? native.hostId;
  const lease = hostId ? read(path.join(spec.mesh, "host-leases", `${digest(hostId).slice(0, 32)}.json`)) : null;
  return { phase, at: Date.now(), rootId: native.rootId, key, databaseIdentity: spec.sharedDatabase,
    kv, kvPayloadSha256: kv ? digest(JSON.stringify(kv.value)) : null,
    kvPayloadCanonicalSha256: kv ? canonicalDigest(kv.value) : null,
    kvAuthorCanonicalSha256: kv ? canonicalDigest(kv.updatedBy) : null, participantFile: file, hostLeaseFile: lease,
    publication: kv ? "kv-present" : file.absent ? "no-own-root-kv-or-file-observed" : "participant-file-only-no-kv",
    limitation: "Outside-window observations are non-atomic. Equal endpoints after a version advance are candidates, never intermediate-write proof." };
}

function localBoundary(observer, clock, spec, native, actors, publications, extra = {}) {
  clock.outsideWindow();
  const mesh = sharedStore(observer, spec);
  const allActors = actors.filter(actor => actor.rootId === spec.owner.rootId);
  const ownPublications = publications.filter(row => row.rootId === spec.owner.rootId &&
    (row.kind === "root" || allActors.some(actor => actor.id === row.id)));
  const rootParticipant = ownPublications.find(row => row.id === native.rootId);
  const actorRows = serializedRows(mesh.listAll(`actors/${spec.owner.sessionId}/`, { fresh: true }));
  for (const expected of spec.actorIds) assert(actorRows.some(row => row.value.id === expected.id && row.value.rootId === native.rootId), "native actor mesh publication missing");
  assert(allActors.every(actor => !actor.lastRunId && !actor.currentRun && !actor.inFlightRun && !actor.preparing && !actor.queued), "unexpected actor activation/queue");
  return { label: spec.owner.label, role: spec.owner.role, pid: process.pid, ready: true, native,
    actors: allActors, publications: ownPublications, rootParticipant,
    ownRootObservation: ownRootBoundary(mesh, spec, native, rootParticipant, clock.phase),
    loadedCandidate: spec.loadedCandidate,
    ownerHost: rootParticipant ? mesh.get(`topology/hosts/${digest(rootParticipant.ownerHostId)}`, { fresh: true }) : null,
    legacySession: mesh.get(`sessions/${spec.owner.sessionId}`, { fresh: true }),
    actorRows, stores: storeInventory(observer, spec), windows: clock.windows, phase: clock.phase,
    inbox: mesh.get(`topology/inbox/${digest(native.rootId).slice(0, 32)}`),
    subscriptions: serializedRows(mesh.listAll("topology/subscriptions/", { fresh: true })),
    historical: (spec.historicalTargets ?? []).map(target => {
      const entry = mesh.get(target.key, { fresh: true }), native = publications.find(row => row.id === target.id);
      return { ...target, entry, writerIdMatches: !!entry && entry.updatedBy.id === entry.value.ownerIdentityId,
        writerMatchesExpected: !!entry && !!target.seedWriter && canonicalDigest(entry.updatedBy) === canonicalDigest(target.seedWriter),
        writerFieldsMatchPayload: !!entry && ["id", "name", "sessionId"].every(field => entry.updatedBy[field] === entry.value[field]),
        envelopeTimesAreActual: true,
        parserAcceptance: native ? "observed-in-native-directory" : "not-observed-in-native-directory", nativeParticipant: native ?? null };
    }),
    usage: process.resourceUsage(), memory: process.memoryUsage(), ...extra };
}

// Ordinary Pi extension wrapper: install the frozen observer, then compose the
// entire production extension and its real session_start handlers unchanged.
export default async function populatedMainExtension(pi) {
  assert(process.env.POPULATED_MAIN_SPEC, "cohort extension needs its private spec");
  const spec = JSON.parse(fs.readFileSync(process.env.POPULATED_MAIN_SPEC, "utf8"));
  validateSpec(spec);
  const { createMainBatchObserver } = await import(pathToFileURL(verifyPin(spec.gate.observer)).href);
  const observer = await createMainBatchObserver({ candidate: spec.candidate, root: spec.root, mesh: spec.mesh,
    role: spec.owner.role, label: spec.owner.label, maxRecords: 250000 });
  const clock = new PhaseClock(observer, phase => { process.env.POPULATED_MAIN_PHASE = phase; });
  const audit = installHistoricalAudit(spec, () => clock.phase);
  const production = await import(pathToFileURL(verifyPin(spec.production)).href);
  await production.default(pi);
  let native;
  pi.on("session_start", (_event, context) => {
    native = { mode: context.mode, trusted: context.isProjectTrusted(), sessionId: context.sessionManager.getSessionId(),
      sessionFile: context.sessionManager.getSessionFile(), rootId: `session:${context.sessionManager.getSessionId()}`, hasUI: context.hasUI };
    assert.equal(native.sessionId, spec.owner.sessionId, "CLI failed to reopen pinned native journal");
  });
  pi.on("agent_start", () => { assert.notEqual(clock.phase, "idle", "agent turn during idle window"); });
  pi.on("session_shutdown", async () => {
    clock.close();
    try { await observer.drain(5000); }
    finally {
      if (!fs.existsSync(path.join(spec.directory, "shutdown-capture.json"))) freshJson(path.join(spec.directory, "shutdown-capture.json"), observer.getCapture());
      audit.close(); observer.restore();
    }
  });
  pi.registerCommand("cohort-probe", { description: "Private bounded attribution controller", handler: async (text, context) => {
    const packet = JSON.parse(text);
    try {
      assert(native?.trusted && context.mode === "rpc");
      let result;
      if (packet.action === "arm") result = { armed: clock.arm(packet.window) };
      else if (packet.action === "run") {
        assert(["startup", "traffic", "post-traffic", "drain"].includes(clock.phase), "host workload forbidden during idle");
        result = { value: await nativeHostRun(pi, observer, packet.input, packet.timeoutMs ?? 15000) };
      } else if (packet.action === "boundary") {
        clock.outsideWindow(); assert(context.isIdle(), "native Main not idle at boundary");
        const state = await nativeHostRun(pi, observer, { action: "inspect", correlationId: `COHORT_BOUNDARY_${packet.id.toUpperCase()}` });
        assert.equal(state.main.id, native.rootId);
        result = localBoundary(observer, clock, spec, native, state.actors, state.participants, { histories: state.histories });
        result.sqlAuditQuality = audit.flush();
      } else if (packet.action === "schema") {
        clock.outsideWindow(); result = { label: spec.owner.label, capture: observer.getCapture() };
      } else if (packet.action === "capture") {
        clock.outsideWindow(); clock.set("drain");
        const drain = await observer.drain(packet.timeoutMs ?? 15000);
        audit.flush();
        const capture = observer.getCapture();
        const file = path.join(spec.directory, `${packet.id}-capture.json`); freshJson(file, capture);
        result = { captureFile: file, drain, windows: clock.windows, label: spec.owner.label };
      } else throw new Error("unsupported private controller action");
      commandOutput(spec, packet, { ok: true, result });
    } catch (error) { commandOutput(spec, packet, { ok: false, error: errorText(error) }); }
  } });
}

function validateSpec(spec) {
  privateDirectory(spec.root); privateDirectory(spec.directory);
  assert(beneath(spec.root, spec.directory) && beneath(spec.root, spec.mesh));
  assert.equal(spec.schema, 1);
  assert.equal(spec.gate.approved, true); assert.equal(spec.gate.interface, "populated-main-fixed-v1");
  verifyPin(spec.gate.observer); verifyPin(spec.production);
  assert(sameDatabase(databaseIdentity(path.join(spec.mesh, "state.db")), spec.sharedDatabase));
}

// Real bare ResidentHost in a separate native process. No launcher, restart
// config, paused-consumer callback, fake actor manager or replacement broker.
async function residentMain(file) {
  noLinks(file);
  const spec = JSON.parse(fs.readFileSync(file, "utf8")); validateSpec(spec);
  const { createMainBatchObserver } = await import(pathToFileURL(verifyPin(spec.gate.observer)).href);
  const observer = await createMainBatchObserver({ candidate: spec.candidate, root: spec.root, mesh: spec.mesh,
    role: "residentHost", label: spec.owner.label, maxRecords: 250000 });
  const clock = new PhaseClock(observer);
  const audit = installHistoricalAudit(spec, () => clock.phase);
  const { ResidentHost } = await import(pathToFileURL(verifyPin(spec.residentEntry)).href);
  const config = JSON.parse(fs.readFileSync(spec.configFile, "utf8"));
  assert(!fs.existsSync(path.join(config.residencyRoot, "config.json")), "bare host must have no wake/restart config");
  const host = new ResidentHost(config);
  let closing, failure;
  const close = () => closing ??= (async () => {
    clock.close(); clock.set("termination");
    try { await host.close(); await observer.drain(5000); }
    finally { freshJson(path.join(spec.directory, "shutdown-capture.json"), observer.getCapture()); audit.close(); observer.restore(); }
  })();
  process.once("SIGTERM", () => { void close().then(() => process.exit(0), () => process.exit(1)); });
  process.once("SIGINT", () => { void close().then(() => process.exit(0), () => process.exit(1)); });
  try {
    await host.start();
    const native = { sessionId: config.sessionId, sessionFile: spec.owner.sessionFile, rootId: config.rootId, hostId: host.hostId };
    const reader = readline.createInterface({ input: process.stdin });
    const write = value => process.stdout.write(JSON.stringify(value) + "\n");
    write({ type: "resident_started", pid: process.pid });
    let chain = Promise.resolve();
    reader.on("line", line => {
      chain = chain.then(async () => {
        const packet = JSON.parse(line);
        try {
          let result;
          if (packet.action === "arm") result = { armed: clock.arm(packet.window) };
          else if (packet.action === "boundary") {
            clock.outsideWindow();
            const actors = host.actors.listOwned();
            result = localBoundary(observer, clock, spec, native, actors, host.participants.list({ scope: "project", includeStale: true, fresh: true }),
              { canConsumeMesh: host.participants.canConsumeMesh(), noWakeConfig: !fs.existsSync(path.join(config.residencyRoot, "config.json")),
                histories: actors.map(actor => ({ actorId: actor.id, messages: host.actors.messages(actor.id, 100) })),
                owner: JSON.parse(fs.readFileSync(path.join(config.residencyRoot, "owner.json"), "utf8")) });
            result.sqlAuditQuality = audit.flush();
          } else if (packet.action === "schema") {
            clock.outsideWindow(); result = { label: spec.owner.label, capture: observer.getCapture() };
          } else if (packet.action === "capture") {
            clock.outsideWindow(); clock.set("drain"); const drain = await observer.drain(packet.timeoutMs ?? 15000);
            audit.flush();
            const captureFile = path.join(spec.directory, `${packet.id}-capture.json`); freshJson(captureFile, observer.getCapture());
            result = { captureFile, drain, label: spec.owner.label, windows: clock.windows };
          } else throw new Error("unsupported resident controller action");
          commandOutput(spec, packet, { ok: true, result }); write({ type: "response", id: packet.id, success: true });
        } catch (error) {
          commandOutput(spec, packet, { ok: false, error: errorText(error) }); write({ type: "response", id: packet.id, success: true });
        }
      }).catch(error => { failure = error; reader.close(); process.stdin.destroy(); });
    });
    await new Promise(resolve => reader.once("close", resolve));
    await chain;
  } catch (error) { failure = error; }
  finally { await close().catch(error => { failure ??= error; }); }
  if (failure) { process.stderr.write(errorText(failure) + "\n"); process.exitCode = 1; }
}

export class NativeChild {
  constructor({ owner, args, env, directory, project, deadline, kind }) {
    this.owner = owner; this.directory = directory; this.deadline = deadline; this.kind = kind;
    this.pending = new Map(); this.events = []; this.sequence = 0; this.stopping = false; this.bytes = 0;
    this.stderr = openPrivate(path.join(directory, "stderr.log"));
    this.stdout = openPrivate(path.join(directory, "stdout.jsonl"));
    try { this.child = spawn(process.execPath, args, { cwd: project, env, stdio: ["pipe", "pipe", this.stderr], detached: false }); }
    catch (error) { fs.closeSync(this.stderr); fs.closeSync(this.stdout); throw error; }
    fs.closeSync(this.stderr);
    this.exited = new Promise(resolve => this.child.once("close", (code, signal) => {
      this.exit = { label: owner.label, pid: this.child.pid, code, signal, at: Date.now() };
      for (const pending of this.pending.values()) { clearTimeout(pending.timer); pending.reject(new Error(`native child exited: ${owner.label}`)); }
      this.pending.clear();
      fs.closeSync(this.stdout);
      if (!this.stopping) this.deadline.fail(new Error(`native child exited before termination: ${owner.label}`));
      resolve(this.exit);
    }));
    this.child.on("error", error => this.deadline.fail(error));
    this.child.stdin.on("error", error => { if (!this.stopping) this.deadline.fail(error); });
    // Record raw chunks first, including malformed framing. No silently dropped traces.
    this.child.stdout.on("data", chunk => {
      this.bytes += chunk.length;
      if (this.bytes > 64 * 1024 * 1024) this.deadline.fail(new Error(`native raw trace ceiling: ${owner.label}`));
      else fs.writeSync(this.stdout, chunk);
    });
    const reader = readline.createInterface({ input: this.child.stdout });
    reader.on("line", line => {
      try {
        assert(Buffer.byteLength(line) < 4 * 1024 * 1024, "native RPC record too large");
        const event = JSON.parse(line), at = Date.now();
        if (this.events.length >= 100000) throw new Error("native event record ceiling");
        this.events.push({ ...event, observedAt: at });
        if (event.type === "response" && this.pending.has(event.id)) {
          const pending = this.pending.get(event.id); this.pending.delete(event.id); clearTimeout(pending.timer);
          event.success ? pending.resolve(event.data) : pending.reject(new Error(event.error));
        }
      } catch (error) { this.deadline.fail(error); }
    });
  }
  request(packet, timeoutMs = 20000) {
    assert(!this.exit && !this.stopping, "request to exited/stopping native child");
    assert.equal(this.pending.size, 0, "overlapping native requests refused");
    const id = packet.id ?? `rpc-${++this.sequence}`;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id); const error = new Error(`ambiguous RPC timeout; never retried: ${this.owner.label}/${id}`);
        this.deadline.fail(error); reject(error);
      }, timeoutMs);
      this.pending.set(id, { resolve, reject, timer });
      this.child.stdin.write(JSON.stringify({ ...packet, id }) + "\n");
    });
  }
  async command(action, extra = {}, timeoutMs = 20000) {
    const id = `command-${++this.sequence}`, packet = { id, action, ...extra };
    await this.request(this.kind === "resident" ? packet : { type: "prompt", message: `/cohort-probe ${JSON.stringify(packet)}` }, timeoutMs);
    const file = path.join(this.directory, `${id}.json`);
    // RPC prompt acknowledgement is not a completion or delivery receipt.
    // A single fs watch waits for the command's own exclusive completion file.
    await this.deadline.within(waitForFile(path.join(this.directory, `${id}.done`)), timeoutMs, `native command completion ${id}`);
    const reply = JSON.parse(fs.readFileSync(file, "utf8")); assert.equal(reply.id, id);
    assert(reply.ok, reply.error); return reply.result;
  }
  async setup() {
    assert.equal(this.kind, "main");
    const settled = this.waitEvent(event => event.type === "agent_settled", 45000); settled.catch(() => {});
    await this.request({ type: "prompt", message: "COHORT_SETUP" });
    await settled;
    const state = await this.request({ type: "get_state" }); assert.equal(state.isStreaming, false);
    const trace = readJsonLines(path.join(this.directory, "transport.jsonl"));
    assert.equal(trace.filter(row => row.event === "transport" && row.stage === "synthetic-setup-tool").length, 1);
    assert(trace.some(row => row.event === "tool_execution_end" && row.tool === "fabric_exec" && row.isError === false), "native setup tool failed");
    assert(!trace.some(row => row.event === "transport-error"));
  }
  waitEvent(predicate, timeoutMs) {
    // stdout is the event source; no readiness polling loop.
    return new Promise((resolve, reject) => {
      const listener = chunk => {
        buffered += chunk.toString();
        let newline;
        while ((newline = buffered.indexOf("\n")) >= 0) {
          const line = buffered.slice(0, newline); buffered = buffered.slice(newline + 1);
          try { const event = JSON.parse(line); if (predicate(event)) { cleanup(); resolve(event); return; } }
          catch (error) { cleanup(); reject(error); return; }
        }
      };
      let buffered = "";
      const cleanup = () => { clearTimeout(timer); this.child.stdout.off("data", listener); this.child.off("close", died); };
      const died = () => { cleanup(); reject(new Error("child exited while waiting for native event")); };
      const timer = setTimeout(() => { cleanup(); reject(new Error("native event deadline")); }, timeoutMs);
      this.child.stdout.on("data", listener); this.child.once("close", died);
    });
  }
  async stop() {
    this.stopping = true;
    if (this.exit) return this.exit;
    this.child.stdin.end();
    const term = setTimeout(() => this.child.kill("SIGTERM"), 10000);
    const kill = setTimeout(() => this.child.kill("SIGKILL"), 20000);
    const fail = setTimeout(() => this.deadline.fail(new Error("owned native PID failed to join after KILL")), 28000);
    try { return await Promise.race([this.exited, new Promise((_, reject) => {
      this.joinTimer = setTimeout(() => reject(new Error(`owned native PID not reaped: ${this.child.pid}`)), 29000);
    })]); } finally { clearTimeout(term); clearTimeout(kill); clearTimeout(fail); clearTimeout(this.joinTimer); }
  }
}

function waitForFile(file) {
  if (fs.existsSync(file)) return Promise.resolve();
  return new Promise((resolve, reject) => {
    const watcher = fs.watch(path.dirname(file), (_event, name) => {
      if (String(name) === path.basename(file) && fs.existsSync(file)) { watcher.close(); resolve(); }
    });
    watcher.once("error", reject);
    const timer = setTimeout(() => { watcher.close(); reject(new Error("native command receipt not created")); }, 20000);
    watcher.on("close", () => clearTimeout(timer));
    if (fs.existsSync(file)) { watcher.close(); resolve(); }
  });
}
const readJsonLines = file => fs.readFileSync(file, "utf8").split("\n").filter(Boolean).map(line => JSON.parse(line));

function ownedEnvironment(root, profile, project, mesh) {
  // Allowlist rather than inheriting identity overrides, model credentials or host metadata.
  return { PATH: process.env.PATH, HOME: path.join(root, "home"), TMPDIR: root, TMP: root, TEMP: root,
    LANG: "C.UTF-8", PI_CODING_AGENT_DIR: profile, PI_OFFLINE: "1", PI_FABRIC_PROJECT_ROOT: project,
    PI_FABRIC_RUN_ROOT: path.join(root, "runs"), PI_FABRIC_AGENT_DIR: path.join(profile, "exports"),
    PI_FABRIC_MESH_ROOT: mesh, PI_FABRIC_LOCK_STATS: "1", PI_FABRIC_MESH_STATE_BACKEND: "sqlite",
    POPULATED_MAIN_PRIVATE_ROOT: root, POPULATED_MAIN_PHASE: "startup" };
}

export const counterTotal = value => {
  if (value === undefined || value === null || value === false) return 0;
  if (typeof value === "number") { assert(Number.isFinite(value) && value >= 0); return value; }
  if (Array.isArray(value)) return value.length;
  assert(typeof value === "object", "unknown observer quality counter schema");
  return Object.values(value).reduce((total, next) => total + counterTotal(next), 0);
};

function storeResolver(capture, sharedDatabase) {
  const stores = Array.isArray(capture.stores) ? capture.stores : [];
  const byId = new Map(stores.map(store => [store.id ?? store.storeId, store]));
  const byBatch = new Map(capture.calls.map(row => [row.id, row]));
  const classify = row => {
    if (!row) return null;
    const candidate = row.store ?? byId.get(row.storeId) ?? row.database ?? row.connection;
    const object = typeof candidate === "object" && candidate !== null ? candidate : row;
    const identity = object.databaseIdentity ?? object.identity ?? object;
    const root = object.root ?? object.storeRoot ?? object.canonicalRoot ?? row.root ?? row.storeRoot;
    const file = identity.file ?? object.database;
    if (sharedDatabase && file && identity.device !== undefined && identity.inode !== undefined) {
      return sameDatabase(identity, sharedDatabase) ? "shared-main" : `auxiliary:${file}`;
    }
    // Calls have native store-root metadata; SQL receivers must carry exact identity.
    if (sharedDatabase && !file && root === path.dirname(sharedDatabase.file)) return "shared-main";
    if (!sharedDatabase && ["mesh-state", "shared-main", "main-shared"].includes(object.scope)) return "shared-main";
    if (root) return `auxiliary:${root}`;
    if (file && typeof file === "string") return `auxiliary:${file}`;
    return null;
  };
  return row => classify(row) ?? classify(byBatch.get(row.batchId)) ?? "unattributed-store";
}

// Fail before the first window if the frozen observer's serialized field layout
// is not understood. This never probes a moving sibling by loading it early.
export function assertObserverContract(captures, sharedDatabase) {
  for (const { capture, label } of captures) {
    assert(Array.isArray(capture.calls) && Array.isArray(capture.transactions) && Array.isArray(capture.locks), "observer raw capture schema unsupported");
    const resolve = storeResolver(capture, sharedDatabase);
    assert(capture.calls.length > 0 && capture.calls.some(row => resolve(row) === "shared-main"), `observer shared-store field adapter unsupported: ${label}`);
    assert([...capture.calls, ...capture.transactions, ...capture.locks].every(row => resolve(row) !== "unattributed-store"), `observer cannot identify every native store: ${label}`);
    assert(Object.hasOwn(capture, "dropped") && (Object.hasOwn(capture, "observerErrors") || Object.hasOwn(capture, "errors")), "observer quality counter fields unsupported");
    assert(capture.incomplete !== true && capture.complete !== false, "observer reports incomplete capture");
    assert.equal(counterTotal(capture.dropped), 0, "observer has dropped records");
    assert.equal(capture.observerErrorCount ?? 0, 0, "observer instrumentation error count");
    assert(capture.rankValid !== false, "observer invalidated rank");
    assert.equal(counterTotal(capture.observerErrors ?? capture.errors), 0, "observer instrumentation errors");
  }
}

export function makeRanks(captures, aggregateSites, aggregateTransactions, sharedDatabase) {
  assert(captures.length > 0);
  const partitions = [], combined = { calls: [], locks: [], transactions: [] }, shared = { calls: [], locks: [], transactions: [] };
  let unattributed = 0;
  for (const { label, role, capture } of captures) {
    assert(Array.isArray(capture.calls) && Array.isArray(capture.transactions) && Array.isArray(capture.locks), "observer raw capture schema unsupported");
    const resolve = storeResolver(capture, sharedDatabase);
    const activityOf = row => row.activity ?? "background";
    for (const phase of ["idle", "traffic"]) {
      const keys = new Set([...capture.calls, ...capture.transactions].filter(row => row.phase === phase)
        .map(row => JSON.stringify([resolve(row), activityOf(row)])));
      for (const key of keys) {
        const [store, activity] = JSON.parse(key);
        const keep = row => row.phase === phase && resolve(row) === store && activityOf(row) === activity;
        const selected = { ...capture, calls: capture.calls.filter(keep), transactions: capture.transactions.filter(keep), locks: capture.locks.filter(keep) };
        partitions.push({ label, role, phase, store, activity, sites: aggregateSites(selected, phase), transactions: aggregateTransactions(selected, phase) });
      }
    }
    for (const field of Object.keys(combined)) for (const row of capture[field]) {
      const normalized = { ...row, label, role,
        ...(row.id !== undefined ? { id: `${label}:${row.id}` } : {}),
        ...(row.storeId !== undefined ? { storeId: `${label}:${row.storeId}` } : {}),
        ...(row.batchId !== null && row.batchId !== undefined ? { batchId: `${label}:${row.batchId}` } : {}) };
      combined[field].push(normalized);
      if (resolve(row) === "shared-main") shared[field].push(normalized);
      else if (resolve(row) === "unattributed-store") unattributed++;
    }
  }
  const aggregate = capture => Object.fromEntries(["idle", "traffic"].map(phase => [phase,
    { sites: aggregateSites(capture, phase), transactions: aggregateTransactions(capture, phase) }]));
  const incomplete = unattributed > 0 || captures.some(({ capture }) => capture.incomplete === true || capture.complete === false ||
    capture.rankValid === false || (capture.observerErrorCount ?? 0) > 0 ||
    !Object.hasOwn(capture, "dropped") || (!Object.hasOwn(capture, "observerErrors") && !Object.hasOwn(capture, "errors")) ||
    counterTotal(capture.dropped) + counterTotal(capture.observerErrors ?? capture.errors) > 0);
  return { schema: 1, incomplete, unattributed, partitions, combinedAllStores: aggregate(combined),
    measuredMain: partitions.filter(row => row.label === "M00"), sharedDatabaseRank: aggregate(shared),
    limits: ["SQL acquire-to-release pairs are primary; lockStats is corroboration, not additional held time.",
      "Quantiles are recomputed by the frozen helper over raw transactions, never added across process ranks.",
      "Unknown-store records and dropped/error counters make this an incomplete rank."] };
}

function processResources(children) {
  return children.map(child => {
    let status;
    if (process.platform === "linux" && !child.exit) {
      try { status = fs.readFileSync(path.join("/proc", String(child.child.pid), "status"), "utf8").split("\n")
        .filter(line => /^(VmRSS|VmHWM|Threads):/.test(line)); } catch (error) { status = { error: String(error) }; }
    }
    return { label: child.owner.label, pid: child.child.pid, exit: child.exit ?? null, status };
  });
}

export function verifyTraffic({ actions, boundaries, owners, root }) {
  const counts = { scheduled: actions.length, dispatched: actions.filter(row => row.dispatchedAt).length,
    accepted: actions.filter(row => row.result).length, filtered: 0, delivered: 0, committed: 0, actorActivations: 0,
    rootInboxConsumed: 0, followUpsConsumed: 0, lifecycleDelivered: 0 };
  for (const owner of owners) {
    const boundary = boundaries.find(row => row.label === owner.label);
    for (const actor of boundary.actors) {
      assert.equal(actor.status, "idle");
      assert(!actor.lastRunId && !actor.queued && !actor.preparing && !actor.inFlightRun && !actor.currentRun, "unexpected native actor activity");
    }
  }
  for (const action of actions) {
    assert(action.result, `action not accepted: ${action.correlationId}`);
    if (action.action === "history") {
      const boundary = boundaries.find(row => row.label === action.owner);
      const expectedId = action.result.event?.id; assert(expectedId, "history publication has no native event id");
      const rows = boundary.actorRows.map(row => row.value);
      const scheduled = actions.filter(row => row.action === "history" && row.owner === action.owner).length;
      assert(rows.every(row => row.filteredCount >= scheduled), "native skip counts do not cover scheduled history events");
      const histories = boundary.histories ?? [];
      assert.equal(histories.length, action.expectedFiltered, "missing native actor histories");
      assert(histories.every(history => history.messages.some(message => message.reason === "filtered: cohort-skip" &&
        message.data?.filteredEventId === expectedId && message.data?.sequence === action.result.event.sequence)), "matched event missing from native skip-only history");
      counts.filtered += action.expectedFiltered; counts.committed += action.expectedFiltered;
    } else if (["inbox", "followUp"].includes(action.action)) {
      const owner = owners.find(row => row.rootId === action.to), directory = path.join(root, owner.label);
      const entries = readJsonLines(owner.sessionFile), trace = readJsonLines(path.join(directory, "transport.jsonl"));
      const id = action.action === "inbox" ? action.result.event?.id : action.result.receipt?.messageId;
      assert(id, "native sender receipt has no correlation id");
      const customs = entries.filter(entry => entry.type === "custom_message");
      const matches = action.action === "inbox" ? customs.filter(entry => entry.customType === "pi-fabric-inbox" && entry.details?.ids?.includes(id))
        : customs.filter(entry => entry.customType === "pi-fabric-agent-message" && (entry.details?.items ?? [entry.details]).some(item => item?.id === id));
      assert.equal(matches.length, 1, "send is not native session consumption exactly once");
      assert(trace.some(row => row.event === "transport" && row.stage === "fixed-ack" && row.markers.includes(action.correlationId)), "local fixture did not ACK consumed work");
      const observed = trace.filter(row => row.event === "native_custom_message" && JSON.stringify(row.message.details).includes(id));
      assert.equal(observed.length, 1, "carrier not observed by actual native message_end");
      if (action.action === "inbox") {
        const state = boundaries.find(row => row.label === owner.label).inbox?.value;
        assert(state && !state.pending && Math.max(state.after, state.ackedThrough ?? 0) >= action.result.event.sequence,
          "inbox event not committed by native cursor/ACK"); counts.rootInboxConsumed++;
      } else counts.followUpsConsumed++;
      counts.delivered++; counts.committed++;
    }
  }
  assert(counts.filtered > 0 && counts.rootInboxConsumed > 0 && counts.followUpsConsumed > 0, "required native traffic path unexercised");
  return counts;
}

async function controllerMain(argv) {
  const options = parseArguments(argv);
  if (options.help) {
    console.log("--profile FILE --import-root PACKAGE --pi-cli CLI --out FRESH.json [--mains N --residents N --idle-seconds N --traffic-seconds N]");
    console.log("Caller-owned supervisor required. Set POPULATED_MAIN_DEPENDENCY_GATE to the private approved interface/byte-pin receipt."); return;
  }
  const profile = JSON.parse(fs.readFileSync(options.profile, "utf8")), plan = populationPlan(profile, options.overrides);
  const eventsTarget = plan.focused ? { bytes: 65536, lineBytes: 2048, lines: null } : eventTarget(profile);
  if (plan.focused) assert(plan.idleSeconds + plan.trafficSeconds <= 10, "focused measured smoke must be shorter than 60 seconds");
  // LEAD-ANSWER-1318Z supersedes CHOICE A, including when reading v2/v3.
  // Historical missing flags are static data, never a live compatibility workload.
  // Before this line there is no helper read, native import, DB access or process launch.
  const gate = readDependencyGate(process.env.POPULATED_MAIN_DEPENDENCY_GATE);
  const output = prepareOutput(options.out), root = output.files.root;
  const deadline = new Deadline((plan.startupSeconds + plan.idleSeconds + plan.trafficSeconds + plan.drainSeconds + plan.terminateSeconds) * 1000);
  const children = [], resources = [], captures = [], actions = [];
  let failure, fixture, population, owners, provenance, initial, before, afterIdle, afterTraffic, idleWindow, trafficWindow, ranks, trafficCounts;
  const saved = new Set();
  const finalize = (name, value) => { output.write(name, value); saved.add(name); };
  const interrupt = signal => deadline.fail(new Error(`controller interrupted: ${signal}`));
  const onTerm = () => interrupt("SIGTERM"), onInt = () => interrupt("SIGINT");
  process.once("SIGTERM", onTerm); process.once("SIGINT", onInt);
  try {
    process.umask(0o077);
    for (const directory of ["home", "project", "runs", "controller-profile"]) fs.mkdirSync(path.join(root, directory), { mode: 0o700 });
    Object.assign(process.env, ownedEnvironment(root, path.join(root, "controller-profile"), path.join(root, "project"), path.join(root, "unopened-mesh")));
    const dependencies = await loadDependencies(gate);
    const candidate = candidatePins(options.candidate, options.cli, [self, fixtureFile, providerFile, options.profile]);
    const sdk = sdkEntryFromCli(options.cli);
    provenance = { at: new Date().toISOString(), candidate, sdk, gate, profile: pinFile(options.profile), node: process.version,
      sqlite: process.getBuiltinModule("node:sqlite") ? process.versions.sqlite : null,
      sourceHead: gate.head ?? execFileSync("git", ["-C", path.dirname(self), "rev-parse", "HEAD"], { encoding: "utf8", timeout: 1000 }).trim(),
      sources: ["src/extension.ts", "src/fabric-runtime-state.ts", "src/config.ts", "src/residency/host.ts", "src/residency/protocol.ts",
        "src/actors/registry-store.ts", "src/actors/manager.ts", "src/actors/activation-filter.ts", "src/topology/root-inbox.ts", "src/programs/host.ts"]
        .map(file => pinFile(path.join(options.candidate, file))),
      availableParallelism: os.availableParallelism(), totalMemory: os.totalmem(), freeMemoryAtStartup: os.freemem(),
      remoteInference: 0, networkIntent: "offline; isolated allowlisted environment; deterministic transport only",
      limitations: ["Resource observations are not a diagnosis of the host.", "Instrumentation overhead remains part of this diagnostic run."] };
    finalize("provenance", provenance);
    const project = path.join(root, "project");
    owners = selectCandidates(await deadline.within(createNativeJournals(plan, root, project, sdk), 60000, "native journals"), options.candidate);
    const baseProfile = { version: profile.baseTableSchemaVersion ?? 1, tables: profile.tables };
    // Derive writer identity from the frozen helper's actual supported seed
    // output; do not invent updatedBy or patch the returned fixture metadata.
    const preview = dependencies.seeder.buildStateSeed(baseProfile);
    const seedWriter = Object.values(preview.state.entries)[0].updatedBy;
    population = semanticPopulation(profile, plan, owners, project, Date.now(), seedWriter);
    const meshParent = path.join(root, "mesh-import"); fs.mkdirSync(meshParent, { mode: 0o700 });
    fixture = await deadline.within(dependencies.seeder.createPopulatedMesh({ candidate: options.candidate, parent: meshParent,
      profile: baseProfile, overlays: population.overlays, eventBytes: eventsTarget.bytes,
      eventLineBytes: eventsTarget.lineBytes }), 180000, "supported populated SQLite import");
    assert(beneath(meshParent, fs.realpathSync(fixture.root)), "seeder returned foreign root");
    initial = await dependencies.seeder.inspectPopulatedMesh(fixture);
    const actualEvents = fixture.manifest.events;
    assert(actualEvents.actualBytes >= eventsTarget.bytes && actualEvents.actualBytes - eventsTarget.bytes <= eventsTarget.lineBytes,
      "EVENT_SHAPE_UNSUPPORTED: retained event bytes outside bounded-line tolerance");
    freshJson(path.join(root, "event-shape.json"), { target: eventsTarget, actual: actualEvents,
      lineDelta: eventsTarget.lines === null ? null : actualEvents.count - eventsTarget.lines,
      interpretation: "Actual line count reported, not claimed exact; small event log in focused proof." });
    writeActorRegistries(fixture.root, owners, population.actors);
    const sharedDatabase = databaseIdentity(path.join(fixture.root, "state.db"));
    freshJson(path.join(root, "base-profile-adapter.json"), { original: pinFile(options.profile), originalVersion: profile.version,
      derived: baseProfile, derivedSha256: digest(JSON.stringify(baseProfile)), seedWriter,
      originalJsonSha256: digest(JSON.stringify(profile)),
      adapter: "Only base table schema version/tables are passed to v1 seeder; full operational/semantic profile is unchanged." });
    finalize("population", { preseed: initial, seeder: { manifest: fixture.manifest, receipts: fixture.receipts, ownership: fixture.ownership, DBidentity: fixture.DBidentity },
      semantic: population.manifest, sharedDatabase, registryPins: owners.map(owner => pinFile(path.join(fixture.root, "actors", owner.sessionId, "actors.json"))) });
    const configModule = await import(pathToFileURL(path.join(options.candidate, "dist/config.js")).href);
    assert.equal(typeof configModule.normalizeFabricConfig, "function", "compiled native config normalizer missing");
    const normalized = configModule.normalizeFabricConfig(fabricSettings());
    const startupEndsAt = deadline.endsAt - (plan.idleSeconds + plan.trafficSeconds + plan.drainSeconds + plan.terminateSeconds) * 1000;
    for (let index = 0; index < owners.length; index += 4) {
      const group = owners.slice(index, index + 4);
      const launched = group.map(owner => {
        const loadedCandidate = candidate;
        for (const pin of loadedCandidate.files) verifyPin(pin);
        verifyPin(gate.observer); verifyPin(gate.seeder);
        const directory = path.join(root, owner.label), profileDirectory = path.join(directory, "profile");
        fs.mkdirSync(profileDirectory, { mode: 0o700 });
        freshJson(path.join(profileDirectory, "settings.json"), { compaction: { enabled: false }, retry: { enabled: false }, defaultProvider: "cohort-local", defaultModel: "offline" });
        freshJson(path.join(profileDirectory, "fabric.json"), fabricSettings());
        const spec = { schema: 1, candidate: owner.candidate, root, mesh: fixture.root, directory, owner, sharedDatabase, gate, loadedCandidate,
          production: pinFile(path.join(owner.candidate, "dist/index.js")), residentEntry: pinFile(path.join(owner.candidate, "dist/residency/host.js")),
          historicalTargets: population.manifest.historicalTargets,
          actorIds: population.actors.filter(actor => actor.rootId === owner.rootId).map(({ id }) => ({ id })) };
        const env = ownedEnvironment(root, profileDirectory, project, fixture.root), specFile = path.join(directory, "spec.json");
        let args, kind;
        if (owner.role === "residentHost") {
          kind = "resident";
          const config = { format: 1, rootId: owner.rootId, sessionId: owner.sessionId, cwd: project, projectRoot: project,
            meshRoot: fixture.root, actorRoot: path.join(fixture.root, "actors"), sessionActorRoot: path.join(fixture.root, "actors", owner.sessionId),
            residencyRoot: path.join(fixture.root, "residency", digest(owner.rootId)), fullCodeMode: normalized.fullCodeMode,
            agents: normalized.agents, mesh: normalized.mesh, retention: normalized.retention, actors: normalized.actors,
            workerPath: path.join(options.candidate, "dist/worker.js"), fabricExtensionPath: path.join(options.candidate, "dist/index.js"),
            piBinary: options.cli, claudeBinary: normalized.agents.claude.binary, vedaBinary: normalized.agents.veda.binary };
          spec.configFile = path.join(directory, "bare-host-input.json"); freshJson(spec.configFile, config);
          args = ["--enable-source-maps", self, "--resident", specFile];
        } else {
          kind = "main"; const transport = path.join(directory, "transport.jsonl"); fs.closeSync(openPrivate(transport));
          const setup = path.join(directory, "bootstrap.ts"); const setupFd = openPrivate(setup); fs.writeFileSync(setupFd, bootstrapCode()); fs.closeSync(setupFd);
          env.POPULATED_MAIN_SPEC = specFile; env.POPULATED_MAIN_TRANSPORT_TRACE = transport; env.POPULATED_MAIN_BOOTSTRAP_FILE = setup;
          args = ["--enable-source-maps", options.cli, "--mode", "rpc", "--offline", "--no-extensions", "--no-skills", "--no-prompt-templates", "--no-context-files",
            "--no-themes", "--no-builtin-tools", "--approve", "--provider", "cohort-local", "--model", "offline", "--session", owner.sessionFile,
            "--session-dir", owner.sessionDir, "-e", self, "-e", providerFile];
        }
        freshJson(specFile, spec);
        const child = new NativeChild({ owner, args, env, directory, project, deadline, kind }); children.push(child);
        return child;
      });
      await deadline.within(Promise.all(launched.map(async child => {
        if (child.kind === "main") {
          await child.request({ type: "get_state" }, 60000);
          await child.setup();
        } else if (!child.events.some(event => event.type === "resident_started")) await child.waitEvent(event => event.type === "resident_started", 60000);
      })), Math.max(1, startupEndsAt - Date.now()), "staggered native startup");
      resources.push({ phase: "startup-group", at: Date.now(), children: processResources(children) });
    }
    const mains = children.filter(child => child.kind === "main"), driver = mains.at(-1), target = mains[0];
    // The first native inbox and follow-up exercise setup/consumption before idle.
    const bootstrapActions = [
      { action: "inbox", to: target.owner.rootId, owner: target.owner.label, correlationId: "COHORT_BOOTSTRAP_INBOX" },
      { action: "followUp", to: target.owner.rootId, owner: target.owner.label, correlationId: "COHORT_BOOTSTRAP_FOLLOWUP" },
    ];
    for (const action of bootstrapActions) {
      const ack = waitForNativeAck(target, action.correlationId, deadline, 20000); ack.catch(() => {});
      action.result = (await driver.command("run", { input: action })).value;
      await ack;
    }
    // Subscribe only after setup turns: the once subscription must remain live
    // across idle and be consumed by a genuine traffic-phase native lifecycle.
    const setupSubscriptions = await target.command("run", { input: { action: "subscribe", from: target.owner.rootId, correlationId: "COHORT_BOOTSTRAP_SUBSCRIBE" } });
    assert.equal(setupSubscriptions.value.subscriptions.length, 2);
    before = await Promise.all(children.map(child => child.command("boundary")));
    requireReady(before, owners, sharedDatabase);
    const schemas = await Promise.all(children.map(child => child.command("schema")));
    assertObserverContract(schemas, sharedDatabase);
    assert(Date.now() < startupEndsAt, "all-process ready barrier exceeded startup budget");
    resources.push({ phase: "ready-barrier", at: Date.now(), children: processResources(children) });
    const idleStartAt = Date.now() + 1500, idleEndAt = idleStartAt + plan.idleSeconds * 1000 + 250;
    await Promise.all(children.map(child => child.command("arm", { window: { phase: "idle", startAt: idleStartAt, endAt: idleEndAt } })));
    await deadline.until(idleEndAt + 150);
    assert(children.every(child => !child.exit));
    afterIdle = await Promise.all(children.map(child => child.command("boundary")));
    idleWindow = assertCommonWindow(afterIdle, "idle", plan.idleSeconds, owners.map(owner => owner.label));
    requireReady(afterIdle, owners, sharedDatabase);
    freshJson(path.join(root, "own-root-idle.json"), ownRootProgress(before, afterIdle, "idle"));
    for (const child of mains) assert.equal(child.events.filter(event => event.type === "agent_start" && event.observedAt >= idleWindow.start && event.observedAt <= idleWindow.end).length, 0, "idle window agent turn");
    resources.push({ phase: "after-idle", at: Date.now(), children: processResources(children) });
    const trafficStartAt = Date.now() + 1500, trafficEndAt = trafficStartAt + plan.trafficSeconds * 1000 + 250;
    await Promise.all(children.map(child => child.command("arm", { window: { phase: "traffic", startAt: trafficStartAt, endAt: trafficEndAt } })));
    for (const slot of trafficSchedule(plan, owners)) {
      await deadline.until(trafficStartAt + slot.offsetMs + 50);
      for (const action of slot.actions) {
        const row = { ...action, slot: slot.slot, scheduledAt: trafficStartAt + slot.offsetMs, dispatchedAt: Date.now() }; actions.push(row);
        const remaining = trafficStartAt + slot.deadlineOffsetMs - Date.now(); assert(remaining > 0, "traffic backlog; refusing overlap/retry");
        const receiver = ["inbox", "followUp"].includes(action.action) ? children.find(child => child.owner.rootId === action.to) : null;
        const ack = receiver ? waitForNativeAck(receiver, action.correlationId, deadline, remaining) : null; ack?.catch(() => {});
        row.result = (await driver.command("run", { input: action, timeoutMs: Math.min(15000, remaining) }, Math.min(20000, remaining))).value;
        row.acceptedAt = Date.now();
        if (ack) { await ack; row.ackAt = Date.now(); }
      }
    }
    await deadline.until(trafficEndAt + 150);
    afterTraffic = await Promise.all(children.map(child => child.command("boundary")));
    trafficWindow = assertCommonWindow(afterTraffic, "traffic", plan.trafficSeconds, owners.map(owner => owner.label));
    requireReady(afterTraffic, owners, sharedDatabase);
    freshJson(path.join(root, "own-root-traffic.json"), ownRootProgress(afterIdle, afterTraffic, "traffic"));
    trafficCounts = verifyTraffic({ actions, boundaries: afterTraffic, owners, root });
    // Do not claim lifecycle completion from successful subscribe/send framing.
    trafficCounts.lifecycle = verifyLifecycle(setupSubscriptions.value.subscriptions, target, afterTraffic.find(row => row.label === target.owner.label));
    trafficCounts.lifecycleDelivered = trafficCounts.lifecycle.delivered;
    const captureReplies = await Promise.all(children.map(child => child.command("capture", { timeoutMs: 15000 })));
    for (let index = 0; index < children.length; index++) {
      const capture = JSON.parse(fs.readFileSync(captureReplies[index].captureFile, "utf8"));
      captures.push({ label: children[index].owner.label, role: children[index].owner.role, capture });
    }
    ranks = makeRanks(captures, dependencies.observer.aggregateSites, dependencies.observer.aggregateTransactions, sharedDatabase);
    assert(!ranks.incomplete, "incomplete observer capture; no complete rank claim");
    finalize("ranks", ranks);
  } catch (error) { failure = error; fixture ??= error.fixture; }
  finally {
    const exits = await Promise.all(children.map(child => child.stop().catch(error => { failure ??= error; return { label: child.owner.label, pid: child.child.pid, error: errorText(error), joined: false }; })));
    for (const exit of exits) try { validateChildExit(exit, true); } catch (error) { failure ??= error; }
    resources.push({ phase: "after-owned-termination", at: Date.now(), children: processResources(children) });
    const aliveOwnedPids = children.filter(child => !child.exit).map(child => child.child.pid);
    if (aliveOwnedPids.length) failure ??= new Error("owned native process custody incomplete");
    let finalShape;
    if (fixture && aliveOwnedPids.length === 0) {
      try { const { seeder } = await loadDependencies(gate); finalShape = await seeder.inspectPopulatedMesh(fixture); }
      catch (error) { failure ??= error; }
    }
    const historicalRecords = [], historicalCaptures = [], auditQualities = [];
    for (const child of children) {
      const auditFile = path.join(child.directory, "historical-sql.jsonl"), captureFile = path.join(child.directory, "shutdown-capture.json");
      if (fs.existsSync(auditFile)) historicalRecords.push(...readJsonLines(auditFile));
      if (fs.existsSync(captureFile)) historicalCaptures.push({ label: child.owner.label, capture: JSON.parse(fs.readFileSync(captureFile, "utf8")) });
      const qualityFile = path.join(child.directory, "historical-sql-quality.json");
      if (fs.existsSync(qualityFile)) {
        const quality = JSON.parse(fs.readFileSync(qualityFile, "utf8")); auditQualities.push({ label: child.owner.label, ...quality });
        if (!quality.complete) failure ??= new Error("participant/actor SQL observation incomplete");
      } else { auditQualities.push({ label: child.owner.label, complete: false, missingQuality: true }); }
    }
    const historicalReputs = attributeHistoricalReputs(historicalRecords, historicalCaptures,
      { idle: idleWindow, traffic: trafficWindow }, population?.manifest.historicalTargets ?? [], fixture ? databaseIdentity(path.join(fixture.root, "state.db")) : null, auditQualities);
    freshJson(path.join(root, "historical-reputs.json"), historicalReputs);
    freshJson(path.join(root, "own-root-sql-attribution.json"), {
      idle: ownRootProgress(before ?? [], afterIdle ?? [], "idle", historicalReputs),
      traffic: ownRootProgress(afterIdle ?? [], afterTraffic ?? [], "traffic", historicalReputs),
    });
    if (!historicalReputs.complete) failure ??= new Error("historical re-put attribution incomplete; retain raw statements, do not claim complete ranks");
    try { fixture?.close(); } catch (error) { failure ??= error; }
    if (!saved.has("provenance")) finalize("provenance", { incomplete: true, provenance, failure: errorText(failure) });
    if (!saved.has("population")) finalize("population", { incomplete: true, semantic: population?.manifest, failure: errorText(failure) });
    if (!saved.has("ranks")) finalize("ranks", { incomplete: true, ranks, captureFiles: children.map(child => path.join(child.directory, "shutdown-capture.json")), failure: errorText(failure) });
    finalize("custody", { schema: 1, detached: false, inheritedOwnedGroup: true, exits, aliveOwnedPids, resources,
      emptyOwnedChildSet: aliveOwnedPids.length === 0, groupEmptyAttestation: "Caller supervisor must verify group emptiness after this controller exits; controller cannot attest its own empty group." });
    deadline.close(); process.off("SIGTERM", onTerm); process.off("SIGINT", onInt);
    const report = { schema: 1, passed: !failure, performanceAssignmentFulfilled: false,
      resultKind: "source implementation / diagnostic run; independent performance judgment remains external", options, plan, gate,
      root, evidence: output.files, idleWindow, trafficWindow, initial, before, afterIdle, afterTraffic, finalShape, actions, trafficCounts, historicalReputs,
      setupTurns: children.filter(child => child.kind === "main").map(child => ({ label: child.owner.label,
        agentStarts: child.events.filter(event => event.type === "agent_start" && (!idleWindow || event.observedAt < idleWindow.start)).length })),
      realLlmInference: 0, failure: failure ? errorText(failure) : null,
      limitations: ["No result substitutes for the caller's full-duration run and independent acceptance.",
        "Shared-DB rank field integration and native functional verification require the approved frozen helper gate.",
        "Current Mains only. No old-Main adapter or historical native process is permitted.",
        "Historical missing flags never imply live ownership or proved dual-renew; no forced re-put obtains coverage.",
        "Historical semantic rows are synthetic, expired/nonexecutable, not historical authority or a dataset replay.",
        "No independent audit is performed by this controller."] };
    finalize("summary", report); output.close();
    console.log(JSON.stringify({ passed: report.passed, out: options.out, ownedPidsJoined: !aliveOwnedPids.length, failure: report.failure }));
  }
  if (failure) process.exitCode = 1;
}

async function waitForNativeAck(child, marker, deadline, timeoutMs) {
  await deadline.within(child.waitEvent(event => event.type === "agent_settled", timeoutMs), timeoutMs, `native ACK ${marker}`);
  const trace = readJsonLines(path.join(child.directory, "transport.jsonl"));
  assert(trace.some(row => row.event === "transport" && row.stage === "fixed-ack" && row.markers?.includes(marker)), `settle did not ACK ${marker}`);
}

const inCommonWindow = (event, phase, window) => event.phase === phase && (!Number.isFinite(window?.start) ||
  (event.at >= window.start && event.at <= window.end && event.transaction?.releaseAt >= window.start && event.transaction?.releaseAt <= window.end));

export function attributeHistoricalReputs(records, captures, windows, targets = [], selectedDatabase, qualities = []) {
  const indices = new Map(), expected = new Map(), targetByKey = new Map(targets.map(row => [row.key, row]));
  const resultKey = (label, pid, key, version) => JSON.stringify([label, pid, key, version]);
  for (const { label, capture } of captures) {
    const index = new Map(); indices.set(label, { capture, index });
    for (const call of capture.calls) for (const [ordinal, result] of (call.results ?? []).entries()) {
      if (!result.applied || !observedKey(result.key)) continue;
      const key = resultKey(label, call.pid, result.key, result.version), items = index.get(key) ?? [];
      items.push({ call, ordinal }); index.set(key, items);
      if (["idle", "traffic"].includes(call.phase) && call.storeRoot === path.dirname(selectedDatabase?.file ?? "")) expected.set(key, 0);
    }
  }
  const txCache = new Map();
  const events = records.map(record => {
    const { capture, index } = indices.get(record.label) ?? {}, key = resultKey(record.label, record.pid, record.key, record.version);
    const matches = (index?.get(key) ?? []).filter(({ call }) => call.monotonicMs <= record.monotonicMs && call.endedMonotonicMs >= record.endedMonotonicMs);
    const match = matches.length === 1 ? matches[0] : null, call = match?.call;
    const ownTx = record.transaction, primary = sameDatabase(record.databaseIdentity, selectedDatabase);
    const txKey = JSON.stringify([record.label, record.pid, record.receiverId, ownTx?.id]);
    let transactions = txCache.get(txKey);
    if (!transactions) {
      transactions = (capture?.transactions ?? []).filter(tx => tx.pid === record.pid && tx.succeeded && tx.writer &&
        tx.verb?.startsWith("BEGIN") && sameDatabase(tx.database, record.databaseIdentity) && ownTx &&
        tx.acquiredMonotonicMs >= ownTx.beginStartedMonotonicMs && tx.acquiredMonotonicMs <= ownTx.beginEndedMonotonicMs &&
        tx.releaseMonotonicMs >= ownTx.releaseStartedMonotonicMs && tx.releaseMonotonicMs <= ownTx.releaseEndedMonotonicMs);
      txCache.set(txKey, transactions);
    }
    const tx = transactions.length === 1 ? transactions[0] : null;
    const ops = call ? [...(call.ops ?? []), ...(call.prepared ?? []).flat()] : [];
    const operation = match ? ops[match.ordinal] : null;
    const operationPaired = operation?.key === record.key && operation?.kind === "put";
    const committed = primary && !!call && call.storeRoot === path.dirname(selectedDatabase.file) && operationPaired && tx?.batchId === call.id &&
      tx.holdStatus === "complete-commit" && ownTx?.status === "complete-commit";
    const rolledBack = primary && ownTx?.status === "complete-rollback";
    if (committed && expected.has(key)) expected.set(key, expected.get(key) + 1);
    const canonicalValueEqual = record.priorDigests && record.priorDigests.payloadCanonicalSha256 === record.payloadCanonicalSha256;
    const completeAuthorEqual = record.priorDigests && record.priorDigests.updatedByCanonicalSha256 === record.updatedByCanonicalSha256;
    let proof = "UNKNOWN";
    if (committed && record.beforeEvidence === "same-receiver-native-read-in-writer" && !record.repeatedKeyInTransaction && !ownTx.tainted) {
      proof = !record.prior?.present ? "creation" : !completeAuthorEqual ? "changed-author" : record.rawValueEqual ? "unchanged-value-and-author"
        : canonicalValueEqual ? "raw-order-only-value" : "changed-value";
    }
    const target = targetByKey.get(record.key);
    return { ...record, matchedBatchId: call?.id ?? null, immediateSite: call?.callSite ?? null,
      storeRoot: call?.storeRoot ?? null, storeId: call?.storeId ?? null, activity: call?.activity ?? null,
      namespace: record.key.startsWith("topology/participants/") ? "topology/participants" : record.key.split("/").slice(0, 2).join("/"),
      originalOperationCount: call?.inputOpsCount ?? null, preparedOperationCount: call?.preparedOpsCount ?? null,
      operationOrdinal: match?.ordinal ?? null, operationOrigin: match ? match.ordinal < (call.inputOpsCount ?? call.ops.length) ? "original" : "post-prepare" : null,
      operationPaired, operation, matchedSqlHoldId: tx?.holdId ?? null, frozenConnectionId: tx?.database.connectionId ?? null,
      classification: !primary ? "foreign-or-unknown-database" : committed ? "committed-upsert" : rolledBack ? "rolled-back-statement" : "uncommitted-or-uncorrelated-statement",
      proof, canonicalValueEqual: canonicalValueEqual ?? null, completeAuthorEqual: completeAuthorEqual ?? null,
      ambiguousMatches: matches.length, transactionMatches: transactions.length,
      payloadUnchangedFromSeed: target ? target.payloadSha256 === record.payloadSha256 : null,
      canonicalPayloadUnchangedFromSeed: target ? target.payloadCanonicalSha256 === record.payloadCanonicalSha256 : null,
      payloadAgeAloneProvesUnchangedValue: false };
  });
  const missingOrDuplicateApplied = [...expected].filter(([, count]) => count !== 1).map(([key, count]) => ({ key: JSON.parse(key), observedStatements: count }));
  const measured = events.filter(event => ["idle", "traffic"].includes(event.phase));
  const committedRankComplete = qualities.every(row => row.complete) && captures.every(({ capture }) => capture.rankValid !== false &&
    counterTotal(capture.dropped) === 0 && (capture.observerErrorCount ?? 0) === 0 && counterTotal(capture.observerErrors ?? capture.errors) === 0) &&
    missingOrDuplicateApplied.length === 0 && measured.every(event =>
    ["committed-upsert", "rolled-back-statement", "foreign-or-unknown-database"].includes(event.classification));
  const unchangedRankComplete = committedRankComplete && measured.every(event => event.classification !== "committed-upsert" || event.proof !== "UNKNOWN");
  const partitions = ["idle", "traffic"].map(phase => {
    const window = windows[phase], seconds = window?.overlapMs / 1000;
    const selected = events.filter(event => inCommonWindow(event, phase, window) && event.classification === "committed-upsert"), groups = new Map();
    for (const event of selected) {
      const key = JSON.stringify([event.key, event.role, event.label, event.immediateSite, event.storeRoot, event.storeId, event.activity]);
      const group = groups.get(key) ?? { key: event.key, namespace: event.namespace, role: event.role, label: event.label,
        immediateSite: event.immediateSite, storeRoot: event.storeRoot, storeId: event.storeId, activity: event.activity, count: 0, proofs: {} };
      group.count++; group.proofs[event.proof] = (group.proofs[event.proof] ?? 0) + 1; groups.set(key, group);
    }
    const unchanged = selected.filter(event => event.proof === "unchanged-value-and-author").length;
    return { phase, seconds: Number.isFinite(seconds) ? seconds : null, committedUpserts: selected.length,
      provenUnchangedValueAndAuthor: unchanged, unknownBefore: selected.filter(event => event.proof === "UNKNOWN").length,
      perSecond: seconds > 0 ? selected.length / seconds : null, unchangedPerSecond: seconds > 0 ? unchanged / seconds : null,
      groups: [...groups.values()].map(group => ({ ...group, perSecond: seconds > 0 ? group.count / seconds : null }))
        .sort((a, b) => (b.proofs["unchanged-value-and-author"] ?? 0) - (a.proofs["unchanged-value-and-author"] ?? 0) || b.count - a.count) };
  });
  return { schema: 2, scope: ["topology/participants/", "actors/"], selectedDatabase, windows, targets, events, partitions, qualities,
    complete: unchangedRankComplete, committedRankComplete, unchangedRankComplete, missingOrDuplicateApplied, reproductionClaim: false,
    limitation: "Per-commit proof requires a native same-receiver read after writer acquisition, unique applied operation and successful receiver/frozen SQL commit. Repeated keys, unobserved pre-state and endpoint equality stay UNKNOWN. Forwarded author is not physical-process/runtime attribution; zero is not reproduction." };
}

export function ownRootProgress(before, after, phase, attribution) {
  return after.filter(row => row.role !== "residentHost").map(row => {
    const first = before.find(item => item.label === row.label)?.ownRootObservation, last = row.ownRootObservation;
    const versionAdvanced = !!first?.kv && !!last?.kv && last.kv.version > first.kv.version;
    const writes = attribution?.events.filter(event => inCommonWindow(event, phase, attribution.windows?.[phase]) &&
      event.key === last?.key && event.classification === "committed-upsert");
    return { label: row.label, phase, before: first, after: last, versionAdvanced,
      sqlAttribution: attribution ? { complete: attribution.complete, committedWrites: writes.length,
        provenUnchangedValueAndAuthor: writes.filter(event => event.proof === "unchanged-value-and-author").length,
        writes: writes.map(event => ({ label: event.label, role: event.role, pid: event.pid, key: event.key, version: event.version,
          matchedBatchId: event.matchedBatchId, immediateSite: event.immediateSite, storeRoot: event.storeRoot, storeId: event.storeId,
          activity: event.activity, proof: event.proof, at: event.at })) } : null,
      boundaryEqualCandidate: versionAdvanced && first.kvPayloadSha256 === last.kvPayloadSha256 && first.kvAuthorCanonicalSha256 === last.kvAuthorCanonicalSha256,
      perCommitProof: false, publication: last?.publication ?? "UNKNOWN" };
  });
}

function verifyLifecycle(subscriptions, target, boundary) {
  const [ongoing, once] = subscriptions;
  const current = boundary.subscriptions.find(row => row.value.id === ongoing.id)?.value;
  assert(current?.lastDeliveredAt && current.lastEventId && current.afterSequence > ongoing.afterSequence, "native lifecycle cursor not delivered/committed");
  assert(!boundary.subscriptions.some(row => row.value.id === once.id), "native once subscription has not committed removal");
  const entries = readJsonLines(target.owner.sessionFile);
  const delivered = entries.filter(entry => entry.type === "custom_message" && JSON.stringify(entry).includes(current.lastEventId));
  assert(delivered.length > 0, "lifecycle receipt is absent from native destination journal");
  assert(!current.lastError, "native lifecycle delivery failed");
  return { ongoingCursor: current.afterSequence, eventId: current.lastEventId, onceRemoved: true, delivered: delivered.length,
    limitation: "Exact once tombstone/event correlation must also be checked in the final frozen observer capture." };
}

if (process.argv[1] && path.resolve(process.argv[1]) === self) {
  if (process.argv[2] === "--resident") await residentMain(process.argv[3]);
  else await controllerMain(process.argv.slice(2));
}
