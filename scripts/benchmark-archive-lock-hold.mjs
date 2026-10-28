#!/usr/bin/env node
/**
 * Retained, real-entrypoint HOLD/WAIT experiment; NOT a timing gate or a production incident attribution.
 *
 * prepare --ref SHA --out PRIVATE_DIR             immutable git-show bundle (no Git writes)
 * seed --fixture PRIVATE_DIR --entries 40000      two DIFFERENT runs directories, no cleanup
 * run --bundle PRIVATE_DIR/api.mjs --fixture DIR --out NEW_DIR [--samples 3 --slow-ms 1600]
 *
 * Worktree bundles require --expected-head SHA --quiescent-pins FILE instead of --ref.
 * Pins: {head: SHA, files: {"src/path.ts": "sha256", ...}}. The caller owns the writer barrier.
 * Run long commands under smarty-reap. Source/archive copies and all receipts remain on disk.
 * Only the product's intrinsic lock/ticket release removes metadata. Never invokes manager
 * close/cleanup/commitActorArchive, retention/pruning, or fixture deletion.
 */
import assert from "node:assert/strict";
import { fork, execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { syncBuiltinESMExports } from "node:module";
import { fileURLToPath, pathToFileURL } from "node:url";

const self = fileURLToPath(import.meta.url);
const repo = path.resolve(path.dirname(self), "..");
const now = () => Number(process.hrtime.bigint()) / 1e6; // host-monotonic, comparable across worker processes
const sha256 = bytes => createHash("sha256").update(bytes).digest("hex");
const id = n => n.toString(16).padStart(32, "0");
const writeJSON = (file, value) => fs.writeFileSync(file, `${JSON.stringify(value, null, 2)}\n`, { flag: "wx", mode: 0o600 });
const readJSON = file => JSON.parse(fs.readFileSync(file, "utf8"));
const sleepSync = ms => Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
const git = (...args) => execFileSync("git", args, { cwd: repo, encoding: "utf8", maxBuffer: 64 * 1024 * 1024 });
const cpuDelta = start => process.cpuUsage(start);
const metrics = async operation => {
  const cpu = process.cpuUsage(), start = now();
  const value = await operation();
  return { value, wallMs: now() - start, cpuMicros: cpuDelta(cpu) };
};
function newDirectory(directory) {
  assert.ok(path.isAbsolute(directory), "use explicit absolute private evidence/fixture paths");
  fs.mkdirSync(directory, { mode: 0o700 });
}

/** Bundle ORIGINAL production modules, not a copied lock or archive implementation. */
export async function prepareSnapshot({ ref, out, expectedHead, quiescentPins }) {
  newDirectory(out);
  const immutable = ref !== undefined;
  assert.ok(immutable || (expectedHead && quiescentPins), "worktree source needs explicit quiescent pins");
  const head = immutable ? git("rev-parse", `${ref}^{commit}`).trim() : git("rev-parse", "HEAD").trim();
  const pins = immutable ? undefined : readJSON(quiescentPins);
  if (!immutable) {
    assert.equal(head, expectedHead); assert.equal(pins.head, head);
    for (const [file, hash] of Object.entries(pins.files)) {
      assert.equal(sha256(fs.readFileSync(path.join(repo, file))), hash, `pin mismatch: ${file}`);
    }
    for (const required of ["src/agents/manager.ts", "src/actors/manager.ts", "src/mesh/mesh-lock.ts", "src/mesh/lock-queue.ts"])
      assert.ok(pins.files[required], `missing writer pin: ${required}`);
  }
  const files = new Set(git("ls-tree", "-r", "--name-only", head).trim().split("\n"));
  if (!immutable) {
    // New lazy helpers are allowed ONLY when the writer explicitly pinned them.
    for (const file of Object.keys(pins.files)) files.add(file);
  }
  const hashes = {}, externals = {}, loaded = new Map();
  const load = relative => {
    if (loaded.has(relative)) return loaded.get(relative);
    const bytes = immutable ? execFileSync("git", ["show", `${head}:${relative}`], { cwd: repo, maxBuffer: 32 * 1024 * 1024 })
      : fs.readFileSync(path.join(repo, relative));
    hashes[relative] = sha256(bytes); loaded.set(relative, bytes); return bytes;
  };
  const { build } = await import("esbuild");
  const bundle = path.join(out, "api.mjs");
  await build({ stdin: { contents: [
    'export { AgentManager } from "./src/agents/manager.ts";',
    'export { ActorLogStore } from "./src/actors/log-store.ts";',
    'export { MeshLock } from "./src/mesh/mesh-lock.ts";',
    'export { meshLockQueueDirectory } from "./src/mesh/lock-queue.ts";',
    'export { DEFAULT_FABRIC_CONFIG } from "./src/config.ts";',
  ].join("\n"), resolveDir: repo, sourcefile: "archive-lock-hold-entry.mjs" },
    outfile: bundle, bundle: true, platform: "node", format: "esm", packages: "external", target: "node24",
    plugins: [{ name: "pinned-production-source", setup(build) {
      build.onResolve({ filter: /^[^./]/ }, args => {
        // A private bundle may live outside the repository. Resolve installed packages
        // from this script, not from a temporary directory's nonexistent node_modules.
        let resolved;
        try { resolved = import.meta.resolve(args.path); }
        catch { resolved = args.path; } // optional runtime-specific packages remain lazy/external
        const external = resolved.startsWith("file:") ? fileURLToPath(resolved) : resolved;
        externals[args.path] = external;
        return { path: external, external: true };
      });
      build.onResolve({ filter: /^(\.|\/)/ }, args => {
        const relative = path.relative(repo, path.resolve(args.resolveDir || repo, args.path)).split(path.sep).join("/");
        const variants = [relative, relative.replace(/\.js$/, ".ts"), `${relative}.ts`, `${relative}/index.ts`];
        const found = variants.find(file => files.has(file));
        assert.ok(found, `missing pinned source ${relative}`);
        return { path: found, namespace: "pinned" };
      });
      build.onLoad({ filter: /.*/, namespace: "pinned" }, args => ({ contents: load(args.path),
        loader: args.path.endsWith(".json") ? "json" : "ts", resolveDir: path.dirname(path.join(repo, args.path)) }));
    } }],
  });
  // Retain every original byte used by the bundle, including lazy transitive modules.
  for (const [file, bytes] of loaded) {
    const dest = path.join(out, "source", file);
    fs.mkdirSync(path.dirname(dest), { recursive: true, mode: 0o700 });
    fs.writeFileSync(dest, bytes, { flag: "wx", mode: 0o600 });
  }
  if (!immutable) {
    assert.equal(git("rev-parse", "HEAD").trim(), head);
    for (const [file, hash] of Object.entries(hashes)) assert.equal(sha256(fs.readFileSync(path.join(repo, file))), hash, `source changed during bundle: ${file}`);
    for (const [file, hash] of Object.entries(pins.files)) assert.equal(sha256(fs.readFileSync(path.join(repo, file))), hash);
  }
  const manifest = { format: 1, head, sourceKind: immutable ? "immutable-git-show" : "writer-quiescent-worktree",
    bundle, bundleSha256: sha256(fs.readFileSync(bundle)), sourceHashes: hashes, externals,
    dependencyPins: { packageJSON: sha256(fs.readFileSync(path.join(repo, "package.json"))), bunLock: sha256(fs.readFileSync(path.join(repo, "bun.lock"))) },
    quiescentPins: pins ?? null, runtime: process.version, at: new Date().toISOString() };
  writeJSON(path.join(out, "manifest.json"), manifest);
  return manifest;
}

/** Host source discovery and retained actor inventory are deliberately NOT the same directory. */
export function seedFixture({ fixture, entries = 40000 }) {
  assert.ok(Number.isSafeInteger(entries) && entries >= 16);
  newDirectory(fixture);
  const actorId = "archive-lock-hold", sessionFile = path.join(fixture, "actors", actorId, "session.jsonl");
  const sourceRuns = path.join(fixture, "host", "runs"), retainedRuns = path.join(path.dirname(sessionFile), "runs");
  fs.mkdirSync(sourceRuns, { recursive: true, mode: 0o700 });
  fs.mkdirSync(retainedRuns, { recursive: true, mode: 0o700 });
  fs.writeFileSync(sessionFile, "{\"fixtureSession\":true}\n", { flag: "wx", mode: 0o600 });
  const sentinels = {};
  for (let n = 1; n <= entries; n++) {
    const runId = id(n), source = path.join(sourceRuns, runId), retained = path.join(retainedRuns, id(0x100000 + n));
    fs.mkdirSync(source, { mode: 0o700 }); fs.mkdirSync(retained, { mode: 0o700 });
    // Every source entry exercises a valid marker read; only one belongs to this actor.
    fs.writeFileSync(path.join(source, "actor-run-archive-pending.json"), JSON.stringify({ format: 1, runId,
      actorId: n === 1 ? actorId : "foreign-actor", sessionFile }), { flag: "wx", mode: 0o600 });
    const sentinel = `{"retainedRawSentinel":${n}}\n`;
    fs.writeFileSync(path.join(retained, "events.jsonl"), sentinel, { flag: "wx", mode: 0o600 });
    sentinels[id(0x100000 + n)] = sha256(sentinel);
  }
  const targetRunId = id(1), source = path.join(sourceRuns, targetRunId);
  const raw = { "events.jsonl": '{"rawSourceSentinel":"KEEP-ALL-TRACES"}\n',
    "status.json": JSON.stringify({ id: targetRunId, status: "completed" }), "task.txt": "retained fixture task\n" };
  for (const [name, bytes] of Object.entries(raw)) fs.writeFileSync(path.join(source, name), bytes, { flag: "wx", mode: 0o600 });
  const manifest = { format: 1, fixture, entries, actorId, sessionFile, sourceRuns, retainedRuns, targetRunId,
    sourceHashes: Object.fromEntries(Object.entries(raw).map(([name, bytes]) => [name, sha256(bytes)])), retainedHashes: sentinels };
  writeJSON(path.join(fixture, "fixture.json"), manifest);
  return manifest;
}

/** Buffered syscall-boundary evidence: successful canonical publication THROUGH confirmed rmdir/rename release.
 * Start/end syscall brackets are retained; upperBoundMs includes syscall uncertainty and instrumentation overhead.
 * The callback interval is separately buffered. No logging or receipt file write occurs in a critical callback.
 */
export function instrumentCanonical(root, role) {
  const canonical = path.join(root, ".lock"), events = [], holds = [], callbacks = [], requests = [];
  let active, currentRequest;
  const originals = {};
  for (const method of ["mkdirSync", "renameSync", "rmdirSync", "writeFileSync"]) originals[method] = fs[method];
  const publish = (method, lower, upper) => {
    assert.equal(active, undefined, "overlapping own canonical acquisitions");
    active = { role, pid: process.pid, request: currentRequest, method, publicationBeforeMs: lower, publicationAfterMs: upper };
    events.push({ kind: "publish", ...active });
  };
  const release = (method, lower, upper) => {
    assert.ok(active, "release without recorded publication");
    holds.push({ ...active, releaseMethod: method, releaseBeforeMs: lower, releaseAfterMs: upper,
      lowerBoundMs: Math.max(0, lower - active.publicationAfterMs), upperBoundMs: upper - active.publicationBeforeMs,
      releaseConfirmed: true });
    events.push({ kind: "release", role, pid: process.pid, atMs: upper }); active = undefined;
  };
  fs.mkdirSync = function(file, ...args) {
    const targeted = String(file) === canonical, before = targeted ? now() : 0;
    const result = originals.mkdirSync.call(this, file, ...args);
    if (targeted) publish("mkdir", before, now()); return result;
  };
  fs.renameSync = function(from, to, ...args) {
    const publication = String(to) === canonical, relinquish = String(from) === canonical;
    const before = publication || relinquish ? now() : 0;
    const result = originals.renameSync.call(this, from, to, ...args);
    if (publication) publish("rename", before, now());
    if (relinquish) release("rename", before, now()); return result;
  };
  fs.rmdirSync = function(file, ...args) {
    const targeted = String(file) === canonical, before = targeted ? now() : 0;
    const result = originals.rmdirSync.call(this, file, ...args);
    if (targeted) release("rmdir", before, now()); return result;
  };
  fs.writeFileSync = function(file, bytes, ...args) {
    const result = originals.writeFileSync.call(this, file, bytes, ...args);
    if (active && String(file) === path.join(canonical, "owner")) active.token = String(bytes).split("\n")[0];
    return result;
  };
  syncBuiltinESMExports();
  return { events, holds, callbacks, requests,
    async acquire(lock, label, body = () => undefined, acrossAwait = false) {
      const request = { label, pid: process.pid, requestedMs: now() }; requests.push(request);
      currentRequest = label;
      const callback = () => {
        const record = { label, pid: process.pid, enteredMs: now() }; callbacks.push(record);
        if (acrossAwait) return Promise.resolve(body()).finally(() => { record.leftMs = now(); });
        try { return body(); } finally { record.leftMs = now(); }
      };
      try {
        const value = await (acrossAwait ? lock.withLockAcrossAwait(callback) : lock.withLock(callback));
        request.settledMs = now(); request.ok = true; return value;
      } catch (error) {
        request.settledMs = now(); request.ok = false; request.error = { name: error.name, code: error.code, message: error.message }; throw error;
      } finally { currentRequest = undefined; }
    },
    finish() {
      for (const [method, original] of Object.entries(originals)) fs[method] = original;
      syncBuiltinESMExports(); assert.equal(active, undefined, "canonical release UNCONFIRMED");
      // Concurrent requests make a process-global currentRequest ambiguous; callback order resolves the label.
      assert.equal(holds.length, callbacks.length);
      for (let n = 0; n < holds.length; n++) {
        const hold = holds[n], callback = callbacks[n], request = requests.find(r => r.label === callback.label);
        hold.request = callback.label; hold.callback = callback;
        hold.waitLowerMs = Math.max(0, hold.publicationBeforeMs - request.requestedMs);
        hold.waitMs = hold.publicationAfterMs - request.requestedMs;
      }
      return { role, pid: process.pid, events, holds, callbacks, requests, releaseMeasurement: "successful canonical syscall, NOT withTryLock callback" };
    },
  };
}

/** Delay ONE actual target archive-file barrier, and always invoke the underlying fsync.
 * onReady is an off-lock control handshake. Delay and real syscall times remain separate.
 */
export function instrumentArchiveFsync({ targetDirectory, slowMs, onReady = () => {}, onStart = () => {}, fail = false }) {
  const original = fs.fsyncSync, barriers = [];
  let injected = false;
  fs.fsyncSync = function(fd) {
    const target = fs.readlinkSync(`/proc/self/fd/${fd}`);
    const targeted = target.startsWith(`${targetDirectory}${path.sep}`) &&
      (path.basename(target) === "events.jsonl" || path.basename(target).startsWith(".events.jsonl."));
    const record = { fd, target, targeted, callMs: now(), pid: process.pid };
    if (targeted && !injected) {
      injected = true; record.readyWitness = onReady();
      record.injectedStartMs = now(); onStart(); sleepSync(slowMs); record.injectedEndMs = now();
    }
    const before = now();
    try { const value = original.call(this, fd); record.underlyingSucceeded = true; return value; }
    finally {
      record.underlyingStartMs = before; record.underlyingEndMs = now(); barriers.push(record);
      if (targeted && fail) throw Object.assign(new Error("injected archive fsync failure AFTER real fsync"), { code: "EIO" });
    }
  };
  syncBuiltinESMExports();
  return { barriers, finish() { fs.fsyncSync = original; syncBuiltinESMExports(); assert.ok(injected, "actual archive events-file fsync was not reached"); return barriers; } };
}

export function verifySentinels(f, archiveRequired = true) {
  const source = path.join(f.sourceRuns, f.targetRunId), archive = path.join(f.retainedRuns, f.targetRunId);
  for (const [file, hash] of Object.entries(f.sourceHashes)) {
    assert.equal(sha256(fs.readFileSync(path.join(source, file))), hash, `changed source: ${file}`);
    if (archiveRequired) assert.equal(sha256(fs.readFileSync(path.join(archive, file))), hash, `wrong archive: ${file}`);
  }
  assert.ok(fs.existsSync(path.join(source, "actor-run-archive-pending.json")), "source custody marker must remain");
  for (const [runId, hash] of Object.entries(f.retainedHashes))
    assert.equal(sha256(fs.readFileSync(path.join(f.retainedRuns, runId, "events.jsonl"))), hash, `changed retained raw trace: ${runId}`);
  assert.equal(fs.readdirSync(f.sourceRuns).length, f.entries);
  return { sourceFiles: Object.keys(f.sourceHashes).length, retainedFiles: Object.keys(f.retainedHashes).length,
    sourceEntries: f.entries, retainedEntries: fs.readdirSync(f.retainedRuns).length, markerRetained: true, archiveRequired };
}

function channel(child) {
  const backlog = [], waiters = [];
  const deliver = message => {
    const index = waiters.findIndex(w => w.type === message.type);
    if (index < 0) backlog.push(message);
    else { const waiter = waiters.splice(index, 1)[0]; clearTimeout(waiter.timer); waiter.resolve(message); }
  };
  let exit;
  const terminal = new Promise(resolve => child.once("exit", (code, signal) => { exit = { code, signal }; resolve(exit);
    for (const w of waiters.splice(0)) { clearTimeout(w.timer); w.reject(new Error(`worker exited before ${w.type}: ${JSON.stringify(exit)}`)); }
  }));
  child.on("message", deliver);
  return { child, terminal, send: message => child.send(message),
    receive(type, limitMs = 30000) {
      const index = backlog.findIndex(m => m.type === type);
      if (index >= 0) return Promise.resolve(backlog.splice(index, 1)[0]);
      if (exit) return Promise.reject(new Error(`worker already exited: ${JSON.stringify(exit)}`));
      return new Promise((resolve, reject) => {
        const waiter = { type, resolve, reject, timer: setTimeout(() => { waiters.splice(waiters.indexOf(waiter), 1); reject(new Error(`worker control deadline: ${type}`)); }, limitMs) };
        waiters.push(waiter);
      });
    } };
}
async function loadAPI(bundle) {
  const manifest = readJSON(path.join(path.dirname(bundle), "manifest.json"));
  assert.equal(sha256(fs.readFileSync(bundle)), manifest.bundleSha256);
  return import(pathToFileURL(bundle).href);
}
const send = message => process.send(message);
const receive = type => new Promise(resolve => {
  const listener = message => { if (message.type === type) { process.off("message", listener); resolve(message); } };
  process.on("message", listener);
});

async function worker(options) {
  const { bundle, fixture, root, role, slowMs, out, budgetMs, harnessSha256 } = options;
  assert.equal(sha256(fs.readFileSync(self)), harnessSha256, "harness changed before worker start");
  const api = await loadAPI(bundle), f = readJSON(path.join(fixture, "fixture.json"));
  const lock = new api.MeshLock(root, { lockProtocol: 1, lockTimeoutMs: budgetMs, lockHeldEventMs: 0 }, () => undefined);
  const trace = instrumentCanonical(root, role);
  send({ type: "ready", pid: process.pid }); await receive("go");
  const operation = await metrics(async () => {
    if (role === "contender") {
      await trace.acquire(lock, "healthy-contender"); return {};
    }
    const agents = new api.AgentManager(fixture, { ...api.DEFAULT_FABRIC_CONFIG.agents, retainRuns: true, budgetUsd: 0 }, { runRoot: f.sourceRuns });
    const logs = new api.ActorLogStore({ maxEventBytes: 65536 }, api.DEFAULT_FABRIC_CONFIG.mesh, { actorRunArchiveMs: Infinity });
    const discovery = await metrics(async () => {
      const start = now(), pending = agents.actorArchiveSources(f.actorId, f.sessionFile), returnedMs = now() - start;
      const sources = await pending;
      assert.deepEqual([...sources.keys()], [f.targetRunId]); return { returnedMs, found: sources.size };
    });
    // A separate measurement of actor-retained inventory, never mislabelled host-source discovery.
    const inventory = await metrics(() => logs.retainedRunIds({ sessionFile: f.sessionFile }));
    const inventoryCount = inventory.value.length; delete inventory.value;
    assert.equal(trace.holds.length, 0, "discovery/inventory ran without canonical custody");
    send({ type: "prepared", pid: process.pid }); await receive("queue");
    const head = trace.acquire(lock, "blocked-head"), successor = trace.acquire(lock, "blocked-successor");
    // Handle refusals immediately so a blocked main thread cannot produce unhandled rejection noise.
    const pending = Promise.allSettled([head, successor]);
    const names = fs.readdirSync(api.meshLockQueueDirectory(root)).filter(name => /^\d{24}-\d+-[a-f0-9-]+$/.test(name)).sort();
    const queueObservedMs = now(), own = names.filter(name => Number(name.split("-")[1]) === process.pid);
    assert.equal(own.length, 2); assert.deepEqual(names.slice(-2), own);
    const targetDirectory = path.join(f.retainedRuns, f.targetRunId);
    const io = instrumentArchiveFsync({ targetDirectory, slowMs, onStart: () => send({ type: "injected" }), onReady: () => {
      send({ type: "barrier-ready", pid: process.pid, queue: names, ownTickets: own, queueObservedMs, barrierReadyMs: now() });
      // Event: coordinator confirms setup gate release. Bounded sync file observation is needed
      // precisely because this process's IPC callbacks and timers are intentionally frozen here.
      const deadline = now() + 30000;
      while (!fs.existsSync(path.join(out, "gate-released"))) {
        assert.ok(now() < deadline, "coordinator did not release setup gate"); sleepSync(2);
      }
      assert.equal(fs.existsSync(path.join(root, ".lock")), false, "archive injection starts only after canonical release");
      return { canonicalAbsent: true, observedMs: now() };
    } });
    const archive = await metrics(() => logs.retainRun({ sessionFile: f.sessionFile }, f.targetRunId, path.join(f.sourceRuns, f.targetRunId)));
    assert.equal(archive.value.archived, true);
    const barriers = io.finish(), tickets = await pending;
    assert.ok(tickets.every(result => result.status === "fulfilled"), JSON.stringify(tickets));
    return { discovery, inventory: { ...inventory, entries: inventoryCount }, archive, barriers, ownTickets: own, queueBeforeStall: names };
  });
  const result = { role, pid: process.pid, ...operation, trace: trace.finish(), totalProcessCpuMicros: process.cpuUsage() };
  assert.equal(sha256(fs.readFileSync(self)), harnessSha256, "harness changed while worker ran");
  writeJSON(path.join(out, `${role}.json`), result); send({ type: "done", role }); process.disconnect();
}

export function analyzeSample(sample) {
  const { archive, contender, gate } = sample;
  const injected = archive.value.barriers.filter(b => b.injectedStartMs !== undefined);
  assert.equal(injected.length, 1);
  const barrier = injected[0];
  assert.equal(barrier.underlyingSucceeded, true);
  const holds = archive.trace.holds;
  const overlapMs = holds.reduce((sum, h) => sum + Math.max(0,
    Math.min(h.releaseAfterMs, barrier.injectedEndMs) - Math.max(h.publicationBeforeMs, barrier.injectedStartMs)), 0);
  assert.equal(overlapMs, 0, "slow archive barrier must not contribute archive-process canonical HOLD");
  const allArchiveFsyncOverlapMs = archive.value.barriers.reduce((sum, b) => sum + holds.reduce((subtotal, h) => subtotal + Math.max(0,
    Math.min(h.releaseAfterMs, b.underlyingEndMs) - Math.max(h.publicationBeforeMs, b.callMs)), 0), 0);
  assert.equal(allArchiveFsyncOverlapMs, 0, "archive fsync syscall ranges are OFF-lock");
  for (const trace of [archive.trace, contender.trace, gate]) for (const hold of trace.holds) {
    assert.equal(hold.releaseConfirmed, true); assert.ok(hold.upperBoundMs >= hold.lowerBoundMs);
    assert.ok(hold.waitMs >= 0); assert.ok(hold.callback.enteredMs >= hold.publicationBeforeMs);
    assert.ok(hold.callback.leftMs <= hold.releaseAfterMs);
  }
  const contenderHold = contender.trace.holds[0]; assert.ok(contenderHold);
  return { injectedDelayMs: barrier.injectedEndMs - barrier.injectedStartMs, archiveCanonicalOverlapMs: overlapMs, allArchiveFsyncOverlapMs,
    underlyingTargetFsyncMs: barrier.underlyingEndMs - barrier.underlyingStartMs,
    contenderAcquiredDuringInjectedStall: contenderHold.publicationAfterMs >= barrier.injectedStartMs && contenderHold.publicationAfterMs < barrier.injectedEndMs,
    contenderAcquiredBeforeArchiveTickets: contenderHold.publicationAfterMs < Math.min(...holds.map(h => h.publicationAfterMs)),
    contenderWaitMs: contenderHold.waitMs,
    ticketAgeAtInjectionMs: archive.value.ownTickets.map(name => barrier.injectedStartMs - Number(BigInt(name.split("-")[0])) / 1e6),
    canonicalAbsentWitness: barrier.readyWitness,
    archiveHolds: holds.length, archiveHoldUpperMs: holds.map(h => h.upperBoundMs),
    archiveTicketWaitMs: holds.map(h => h.waitMs),
    setupGateHoldExcludedMs: gate.holds.map(h => h.upperBoundMs),
    totalArchiveWallMs: archive.wallMs, archiveCpuMicros: archive.cpuMicros,
    sampleWallMs: sample.coordinatorWallMs,
    summedProcessCpuMicros: Object.fromEntries(["user", "system"].map(kind => [kind,
      sample.coordinatorCpuMicros[kind] + archive.totalProcessCpuMicros[kind] + contender.totalProcessCpuMicros[kind]])),
    discovery: archive.value.discovery, inventory: archive.value.inventory, archive: archive.value.archive,
    fsyncCalls: archive.value.barriers.length, targetFsyncCalls: archive.value.barriers.filter(b => b.targeted).length };
}

export async function runExperiment({ bundle, fixture, out, samples = 3, slowMs = 1600, budgetMs = 5000 }) {
  assert.equal(process.platform, "linux", "descriptor attribution requires Linux /proc");
  assert.ok(Number.isSafeInteger(samples) && samples > 0 && samples <= 20);
  assert.ok(slowMs > 0 && slowMs < budgetMs && budgetMs >= 2000);
  newDirectory(out);
  const api = await loadAPI(bundle), f = readJSON(path.join(fixture, "fixture.json"));
  const bundleManifest = readJSON(path.join(path.dirname(bundle), "manifest.json"));
  const harnessSha256 = sha256(fs.readFileSync(self));
  const before = verifySentinels(f, false), results = [];
  for (let n = 0; n < samples; n++) {
    const sampleOut = path.join(out, `sample-${n}`); newDirectory(sampleOut);
    const root = path.join(sampleOut, "mesh"); newDirectory(root);
    const start = now(), cpu = process.cpuUsage();
    const gateTrace = instrumentCanonical(root, "setup-gate");
    const gateLock = new api.MeshLock(root, { lockProtocol: 1, lockTimeoutMs: budgetMs, lockHeldEventMs: 0 }, () => undefined);
    let release = () => {}, gate, traceFinished = false;
    const workers = ["archive", "contender"].map(role => {
      const log = fs.openSync(path.join(sampleOut, `${role}.stderr`), "wx", 0o600);
      const stdout = fs.openSync(path.join(sampleOut, `${role}.stdout`), "wx", 0o600);
      const child = fork(self, ["worker", JSON.stringify({ bundle, fixture, root, role, slowMs, out: sampleOut, budgetMs, harnessSha256 })],
        { stdio: ["ignore", stdout, log, "ipc"], execArgv: [], env: { ...process.env, PI_FABRIC_LOCK_STATS: "0", PI_FABRIC_LOCK_EVENTS: "0" } });
      fs.closeSync(log); fs.closeSync(stdout); return channel(child);
    });
    const [archiver, healthy] = workers;
    try {
      await Promise.all(workers.map(w => w.receive("ready"))); archiver.send({ type: "go" });
      // Neither discovery nor retained inventory runs under even the synthetic setup gate.
      await archiver.receive("prepared", 90000);
      let entered;
      const ready = new Promise(resolve => { entered = resolve; });
      gate = gateTrace.acquire(gateLock, "SETUP-ONLY-not-archive-hold", async () => {
        entered(); await new Promise(resolve => { release = resolve; });
      }, true);
      await ready; archiver.send({ type: "queue" });
      const observation = await archiver.receive("barrier-ready");
      assert.equal(observation.ownTickets.length, 2);
      release(); await gate;
      const gateResult = gateTrace.finish(); traceFinished = true;
      // Gate receipt is written off-lock. Start the healthy process only AFTER the archive
      // worker confirms injection has started; no same-loop timer is progress evidence.
      fs.writeFileSync(path.join(sampleOut, "gate-released"), "confirmed\n", { flag: "wx", mode: 0o600 });
      await archiver.receive("injected");
      healthy.send({ type: "go" });
      await Promise.all(workers.map(w => w.receive("done")));
      const terminals = await Promise.all(workers.map(w => w.terminal));
      assert.deepEqual(terminals, [{ code: 0, signal: null }, { code: 0, signal: null }]);
      const sample = { sample: n, archive: readJSON(path.join(sampleOut, "archive.json")),
        contender: readJSON(path.join(sampleOut, "contender.json")), gate: gateResult, observation, terminals,
        coordinatorWallMs: now() - start, coordinatorCpuMicros: cpuDelta(cpu) };
      sample.analysis = analyzeSample(sample); writeJSON(path.join(sampleOut, "sample.json"), sample); results.push(sample);
    } finally {
      release(); await gate;
      if (!traceFinished) gateTrace.finish();
      // No process signals/deletion: the outer owned-command deadline controls exceptional workers.
      for (const w of workers) if (w.child.connected) w.child.disconnect();
    }
  }
  const after = verifySentinels(f);
  assert.equal(sha256(fs.readFileSync(self)), harnessSha256, "harness changed during measurement");
  const result = { format: 1, bundleManifest, harnessSha256, fixture, out, samples, slowMs, budgetMs, backend: "canonical-file-lock-protocol-1",
    runtime: process.version, platform: process.platform, filesystem: fs.statfsSync(fixture), before, after,
    at: new Date().toISOString(), results, scope: "Real AgentManager discovery + ActorLogStore archive/inventory + MeshLock. Not ActorManager completion, adoption, event publication, SQLite, or production attribution. Setup gate excluded from workload HOLD. Source custody is deliberately NEVER discharged." };
  writeJSON(path.join(out, "result.json"), result); return result;
}

export async function failureProbe({ bundle, fixture, out }) {
  newDirectory(out);
  const api = await loadAPI(bundle), f = readJSON(path.join(fixture, "fixture.json"));
  const logs = new api.ActorLogStore({ maxEventBytes: 65536 }, api.DEFAULT_FABRIC_CONFIG.mesh, { actorRunArchiveMs: Infinity });
  const archiveDirectory = path.join(f.retainedRuns, f.targetRunId), existingArchiveHashes = {};
  for (const file of Object.keys(f.sourceHashes)) {
    const existing = path.join(archiveDirectory, file);
    if (fs.existsSync(existing)) existingArchiveHashes[file] = sha256(fs.readFileSync(existing));
  }
  const io = instrumentArchiveFsync({ targetDirectory: archiveDirectory, slowMs: 1, fail: true });
  let refused;
  try { await logs.retainRun({ sessionFile: f.sessionFile }, f.targetRunId, path.join(f.sourceRuns, f.targetRunId)); }
  catch (error) { refused = { name: error.name, code: error.code, message: error.message }; }
  const barriers = io.finish(); assert.equal(refused?.code, "FABRIC_ACTOR_RUN_ARCHIVE_REFUSED");
  const sentinels = verifySentinels(f, false);
  for (const [file, hash] of Object.entries(existingArchiveHashes))
    assert.equal(sha256(fs.readFileSync(path.join(archiveDirectory, file))), hash, `failed retry changed prior archive: ${file}`);
  const result = { refused, barriers, sentinels, existingArchiveHashes, rawSourcesRetained: true, collectionInvoked: false };
  writeJSON(path.join(out, "failure.json"), result); return result;
}

async function main() {
  const [command, ...args] = process.argv.slice(2);
  if (command === "worker") return worker(JSON.parse(args[0]));
  const options = {};
  for (let n = 0; n < args.length; n += 2) {
    assert.ok(args[n].startsWith("--") && args[n + 1], "expected --option value pairs");
    const key = args[n].slice(2).replace(/-([a-z])/g, (_, c) => c.toUpperCase());
    options[key] = ["entries", "samples", "slowMs", "budgetMs"].includes(key) ? Number(args[n + 1]) : args[n + 1];
  }
  let result;
  if (command === "prepare") result = await prepareSnapshot(options);
  else if (command === "seed") { const f = seedFixture(options); result = { ...f, retainedHashes: `${Object.keys(f.retainedHashes).length} hashes retained in fixture.json` }; }
  else if (command === "run") { const r = await runExperiment(options); result = { out: r.out, before: r.before, after: r.after, samples: r.results.map(s => s.analysis) }; }
  else if (command === "failure") result = await failureProbe(options);
  else throw new Error("usage: benchmark-archive-lock-hold.mjs prepare|seed|run|failure --option value");
  console.log(JSON.stringify(result, null, 2));
}
if (process.argv[1] && path.resolve(process.argv[1]) === self) main().catch(error => { console.error(error.stack); process.exitCode = 1; if (process.connected) process.disconnect(); });
