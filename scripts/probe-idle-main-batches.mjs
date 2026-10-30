#!/usr/bin/env node
// Real RPC Main, no prompt/provider/inference. Never supply an existing mesh.
// Build first; run under the caller's owned process-group wrapper:
// node scripts/probe-idle-main-batches.mjs --import-root PACKAGE --pi-cli CLI --out REPORT.json
// Repeat the identical command with a rebuilt candidate and a fresh --out path.
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import readline from "node:readline";
import { spawn, execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { AsyncLocalStorage } from "node:async_hooks";
import { DatabaseSync } from "node:sqlite";
import { fileURLToPath, pathToFileURL } from "node:url";

const self = fileURLToPath(import.meta.url);
const hash = file => createHash("sha256").update(fs.readFileSync(file)).digest("hex");
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const privateOpen = file => {
  const fd = fs.openSync(file, "wx", 0o600);
  if (process.platform !== "win32") fs.fchmodSync(fd, 0o600);
  return fd;
};
export const save = (file, value) => {
  const fd = privateOpen(file);
  try { fs.writeFileSync(fd, JSON.stringify(value, null, 2) + "\n"); }
  finally { fs.closeSync(fd); }
};

export function prepareOutput(out) {
  const prefix = out.replace(/\.json$/, "");
  const files = { summary: out, migration: prefix + ".migration.json", ready: prefix + ".ready.json",
    capture: prefix + ".capture.json", rpc: prefix + ".rpc.json", stderr: prefix + ".stderr.log" };
  for (const file of Object.values(files)) {
    assert(!fs.lstatSync(file, { throwIfNoEntry: false }), `use fresh evidence paths: ${file}`);
  }
  const parent = path.dirname(out);
  fs.mkdirSync(parent, { recursive: true, mode: 0o700 });
  const stat = fs.lstatSync(parent);
  assert(stat.isDirectory(), "evidence parent must be a directory, not a link");
  if (process.platform !== "win32") assert.equal(stat.mode & 0o777, 0o700, "use a private mode 0700 evidence parent");
  return files;
}

export function databaseIdentity(file) {
  const canonical = fs.realpathSync(file), stat = fs.statSync(canonical, { bigint: true });
  assert(stat.isFile(), "SQLite main database must be a regular file");
  return { file: canonical, device: String(stat.dev), inode: String(stat.ino) };
}
const sameDatabase = (a, b) => a.file === b.file && a.device === b.device && a.inode === b.inode;

// openNodeSqlite may receive a descriptor-pinned path for the native driver.
// Ask that actual connection for its main file; resolve the pin, then bind the
// canonical path AND inode to the mesh file also verified by store diagnostics.
export function captureTransactions(meshDatabase, stamp, record) {
  const connections = new WeakMap();
  let sequence = 0;
  const exec = DatabaseSync.prototype.exec;
  DatabaseSync.prototype.exec = function (sql, ...rest) {
    const verb = typeof sql === "string" && /^(BEGIN IMMEDIATE|COMMIT|ROLLBACK)$/.exec(sql)?.[1];
    if (!verb) return exec.call(this, sql, ...rest);
    let database = connections.get(this);
    if (!database) {
      database = { connectionId: ++sequence, openedFile: null, scope: "unknown-sqlite" };
      try {
        const main = this.prepare("PRAGMA database_list").all().find(row => row.name === "main");
        database.openedFile = main?.file ?? null;
        Object.assign(database, databaseIdentity(main.file));
        database.scope = sameDatabase(database, meshDatabase) ? "mesh-state" : "other-sqlite";
      } catch (error) { database.identityError = String(error); }
      connections.set(this, database);
    }
    const row = { ...stamp(), database, verb, succeeded: false }, began = performance.now();
    try { const result = exec.call(this, sql, ...rest); row.succeeded = true; return result; }
    finally { row.elapsedMs = performance.now() - began; record(row); }
  };
  return () => { DatabaseSync.prototype.exec = exec; };
}
const meshBegin = tx => tx.database.scope === "mesh-state" && tx.verb === "BEGIN IMMEDIATE" && tx.succeeded;

export function aggregateSites(capture) {
  const sites = new Map();
  for (const row of capture.calls.filter(row => row.phase === "idle")) {
    const frame = row.stack[0].replace(/^at\s+/, "");
    const callSite = frame.endsWith(")") ? frame.slice(frame.lastIndexOf("(") + 1, -1) : frame.replace(/^async\s+/, "");
    assert(/:\d+:\d+$/.test(callSite), "immediate caller must have a source location");
    if (!sites.has(callSite)) sites.set(callSite, { callSite, stacks: [], count: 0, inputOps: 0, preparedOps: 0, resultOps: 0,
      appliedOps: 0, failures: 0, hooks: {}, keys: {}, transactions: 0, lockAcquisitions: 0 });
    const site = sites.get(callSite);
    let variant = site.stacks.find(item => item.stack.join("\n") === row.stack.join("\n"));
    if (!variant) { variant = { stack: row.stack, count: 0 }; site.stacks.push(variant); }
    variant.count++; site.count++; site.inputOps += row.ops.length;
    site.preparedOps += row.prepared.flat().length; site.resultOps += row.results?.length ?? 0;
    site.appliedOps += row.results?.filter(result => result.applied).length ?? 0;
    if (!row.succeeded) site.failures++;
    for (const hook of row.hookRuns) site.hooks[hook.name] = (site.hooks[hook.name] ?? 0) + 1;
    for (const op of [...row.ops, ...row.prepared.flat()]) site.keys[`${op.kind} ${op.key}`] = (site.keys[`${op.kind} ${op.key}`] ?? 0) + 1;
    site.transactions += capture.transactions.filter(tx => tx.batchId === row.id && meshBegin(tx)).length;
    site.lockAcquisitions += capture.locks.filter(lock => lock.batchId === row.id && lock.method === "acquired").length;
  }
  return [...sites.values()].sort((a, b) => b.count - a.count);
}

// Loaded as an ordinary Pi extension in the owned RPC child. The real production
// extension registers all its own hooks; this wrapper does not compose substitutes.
export default async function probeExtension(pi) {
  const spec = JSON.parse(process.env.IDLE_MAIN_BATCH_SPEC);
  const als = new AsyncLocalStorage();
  const calls = [], locks = [], transactions = [], stores = new Set();
  let phase = "startup", sequence = 0, windowStart, windowEnd;
  let dropped = 0;
  const bounded = (list, row) => { if (list.length < 100000) list.push(row); else dropped++; };
  const stamp = () => ({ at: Date.now(), monotonicMs: performance.now(), phase, batchId: als.getStore()?.id ?? null });
  const normalize = text => text.replaceAll(spec.candidate, "<candidate>").replaceAll(spec.root, "<scratch>")
    .replaceAll(path.dirname(self), "<harness-dir>");
  const describe = ops => ops.map(op => ({ kind: op.kind, key: op.key, valueKind: typeof op.value,
    ...(op.ifVersion === undefined ? {} : { ifVersion: op.ifVersion }),
    ...(op.onConflict === undefined ? {} : { onConflict: typeof op.onConflict === "function" ? "function" : op.onConflict }),
    condition: typeof op.condition === "function" }));

  const meshDatabase = databaseIdentity(path.join(spec.mesh, "state.db"));
  captureTransactions(meshDatabase, stamp, row => bounded(transactions, row));
  const { MeshStore } = await import(pathToFileURL(path.join(spec.candidate, "dist/mesh.js")).href);
  const registry = globalThis[Symbol.for("pi-fabric.mesh.lock-stats")];
  assert(registry?.stats, "existing lock recorder unavailable");
  for (const method of ["acquired", "failed", "phase"]) {
    const original = registry.stats[method];
    registry.stats[method] = function (...args) {
      const result = original.apply(this, args);
      if (args[0] === spec.mesh) bounded(locks, { ...stamp(), method, args: args.slice(1) });
      return result;
    };
  }
  const batch = MeshStore.prototype.writeBatch;
  MeshStore.prototype.writeBatch = function (input) {
    assert.equal(this.root, spec.mesh, "probe must not write another mesh");
    stores.add(this);
    const stack = normalize(new Error().stack ?? "").split("\n").slice(2).map(line => line.trim());
    const row = { ...stamp(), id: ++sequence, stack, ops: describe(input.ops), hooks: {},
      lockClass: input.lockClass ?? "writeBatch", lockTiming: input.lockTiming ?? null,
      sqliteHoldTag: input.sqliteHoldTag ?? null, prepared: [], hookRuns: [] };
    bounded(calls, row);
    const wrapped = { ...input };
    for (const name of ["prepare", "beforeCommit", "afterCommit", "commitOutbox"]) {
      row.hooks[name] = typeof input[name] === "function";
      if (!row.hooks[name]) continue;
      wrapped[name] = function (...args) {
        const run = { name, ...stamp(), succeeded: false }, began = performance.now();
        row.hookRuns.push(run);
        try {
          const result = input[name].apply(this, args);
          if (name === "prepare") row.prepared.push(describe(result ?? []));
          run.succeeded = true;
          return result;
        } finally { run.elapsedMs = performance.now() - began; }
      };
    }
    return als.run(row, async () => {
      const began = performance.now();
      try {
        const results = await batch.call(this, wrapped);
        row.results = results.map(({ key, applied, version }) => ({ key, applied, version }));
        row.succeeded = true;
        return results;
      } catch (error) { row.succeeded = false; row.error = normalize(String(error)); throw error; }
      finally { row.elapsedMs = performance.now() - began; row.endedAt = Date.now(); }
    });
  };
  const production = await import(pathToFileURL(path.join(spec.candidate, "dist/index.js")).href);
  await production.default(pi);
  const snapshots = () => [...stores].map(store => {
    const diagnostics = store.stateDiagnostics();
    assert(sameDatabase(databaseIdentity(diagnostics.database), meshDatabase), "store diagnostics must name the captured mesh database");
    return { backend: store.stateBackend, diagnostics, databaseIdentity: meshDatabase,
      entries: store.listAll("", { fresh: true }).map(({ key, version }) => ({ key, version })) };
  });
  let native;
  pi.on("session_start", (_event, ctx) => {
    native = { mode: ctx.mode, trusted: ctx.isProjectTrusted(), sessionId: ctx.sessionManager.getSessionId(),
      sessionFile: ctx.sessionManager.getSessionFile(), hasUI: ctx.hasUI };
  });
  pi.registerCommand("idle-main-probe", { description: "Private idle transaction measurement", handler: async (args, ctx) => {
    assert.equal(ctx.mode, "rpc");
    assert(ctx.isIdle(), "Main must be idle");
    if (args.trim() === "start") {
      assert.equal(phase, "startup");
      assert(native?.trusted, "production activation needs native project trust");
      const state = snapshots();
      assert(state.length > 0 && state.every(item => item.backend === "sqlite"), "real Main SQLite store was not intercepted");
      const id = `session:${native.sessionId}`;
      assert([...stores].some(store => store.listAll("topology/participants/").some(entry => entry.value.id === id)),
        "normal runtime did not publish this native Main");
      assert(transactions.some(tx => meshBegin(tx) && calls.some(call => call.id === tx.batchId && call.succeeded)),
        "startup batch must reach the measured mesh SQLite connection");
      save(spec.ready, { native, stores: state });
      phase = "idle"; windowStart = { at: Date.now(), monotonicMs: performance.now() };
    } else if (args.trim() === "finish") {
      assert.equal(phase, "idle");
      windowEnd = { at: Date.now(), monotonicMs: performance.now() }; phase = "post-window";
      registry.flush();
      save(spec.capture, { native, windowStart, windowEnd, stores: snapshots(), calls, locks, transactions, dropped });
    } else throw new Error("expected start or finish");
  } });
}

async function main() {
  const options = { seconds: 120, warmupSeconds: 5 };
  for (let i = 2; i < process.argv.length; i++) {
    const flag = process.argv[i];
    if (flag === "--help") {
      console.log("--import-root PACKAGE --pi-cli CLI --out REPORT.json [--seconds 120] [--warmup-seconds 5]"); return;
    }
    const value = process.argv[++i]; assert(value, `missing value for ${flag}`);
    if (flag === "--import-root") options.candidate = path.resolve(value);
    else if (flag === "--pi-cli") options.cli = path.resolve(value);
    else if (flag === "--out") options.out = path.resolve(value);
    else if (flag === "--seconds") options.seconds = Number(value);
    else if (flag === "--warmup-seconds") options.warmupSeconds = Number(value);
    else throw new Error(`unknown argument ${flag}`);
  }
  assert(options.candidate && options.cli && options.out, "--import-root, --pi-cli and --out required");
  assert(Number.isFinite(options.seconds) && options.seconds > 0 && options.seconds <= 600);
  assert(Number.isFinite(options.warmupSeconds) && options.warmupSeconds >= 0 && options.warmupSeconds <= 60);
  const files = prepareOutput(options.out);
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "idle-main-batches-"));
  fs.chmodSync(root, 0o700);
  const profile = path.join(root, "profile"), project = path.join(root, "project");
  for (const dir of [profile, project, path.join(root, "home")]) fs.mkdirSync(dir, { mode: 0o700 });
  const env = { PATH: process.env.PATH, HOME: path.join(root, "home"), TMPDIR: root, PI_CODING_AGENT_DIR: profile,
    PI_OFFLINE: "1", PI_FABRIC_PROJECT_ROOT: project, PI_FABRIC_RUN_ROOT: path.join(root, "runs"),
    PI_FABRIC_AGENT_DIR: path.join(root, "exports"), PI_FABRIC_LOCK_STATS: "1", PI_FABRIC_MESH_STATE_BACKEND: "sqlite" };
  let child, exited, exitInfo, stopping = false, failure, beginState, endState, capture;
  const pending = new Map(), events = [];
  let watchdog;
  const migrationFile = files.migration;
  const request = data => new Promise((resolve, reject) => {
    const id = `probe-${pending.size}-${Date.now()}`;
    const timer = setTimeout(() => { pending.delete(id); reject(new Error(`RPC timeout ${data.type}`)); }, 30000);
    pending.set(id, { resolve, reject, timer });
    child.stdin.write(JSON.stringify({ id, ...data }) + "\n");
  });
  try {
    // Supported CLI creates a fresh scratch root and imports our own empty state.
    // No fixture-only initialization symbol or readiness override is used.
    const seed = path.join(root, "empty-state.json");
    save(seed, { format: 1, entries: {}, tombstones: {}, highWater: 0 });
    const migrationArgs = [path.join(options.candidate, "bin/fabric-mesh-backend"), "import", "--scratch-proof-from", seed,
      "--scratch-parent", root, "--json"];
    const migration = JSON.parse(execFileSync(process.execPath, migrationArgs, { env, encoding: "utf8", timeout: 30000 }));
    save(migrationFile, migration);
    assert.equal(migration.scratchProof, true);
    const mesh = migration.root;
    assert(mesh && path.relative(root, mesh) && !path.relative(root, mesh).startsWith(".."));
    env.PI_FABRIC_MESH_ROOT = mesh;
    const spec = { candidate: options.candidate, root, mesh, ready: files.ready, capture: files.capture };
    env.IDLE_MAIN_BATCH_SPEC = JSON.stringify(spec);
    save(path.join(profile, "settings.json"), { compaction: { enabled: false }, retry: { enabled: false } });
    save(path.join(profile, "fabric.json"), { fullCodeMode: false, mesh: { enabled: true, announce: true, persist: true, stateBackend: "sqlite" },
      mcp: { enabled: false }, memory: { enabled: false }, jev: { enabled: false }, entropy: { compile: false },
      speculation: { enabled: false }, prewalk: { enabled: false }, ui: { enabled: false }, components: [], compaction: { engine: "pi" } });
    const args = ["--enable-source-maps", options.cli, "--mode", "rpc", "--offline", "--no-extensions", "--no-skills",
      "--no-prompt-templates", "--no-context-files", "--no-themes", "--no-builtin-tools", "--approve",
      "--session-dir", path.join(root, "sessions"), "-e", self];
    const stderr = privateOpen(files.stderr);
    child = spawn(process.execPath, args, { cwd: project, env, stdio: ["pipe", "pipe", stderr] });
    fs.closeSync(stderr);
    exited = new Promise(resolve => child.once("close", (code, signal) => {
      exitInfo = { pid: child.pid, code, signal };
      for (const item of pending.values()) { clearTimeout(item.timer); item.reject(new Error("RPC child exited")); }
      pending.clear(); resolve();
    }));
    child.on("error", error => { failure ??= error; });
    child.stdin.on("error", error => { if (!stopping) failure ??= error; });
    const reader = readline.createInterface({ input: child.stdout });
    reader.on("line", line => {
      try {
        const event = JSON.parse(line); events.push({ at: Date.now(), ...event });
        if (event.type === "response" && pending.has(event.id)) {
          const item = pending.get(event.id); pending.delete(event.id); clearTimeout(item.timer);
          if (event.success) item.resolve(event.data); else item.reject(new Error(event.error));
        }
      } catch (error) { failure ??= error; }
    });
    watchdog = setTimeout(() => { failure ??= new Error("finite probe deadline exceeded"); child.kill("SIGTERM"); },
      (options.seconds + options.warmupSeconds + 90) * 1000);
    beginState = await request({ type: "get_state" });
    assert.equal(beginState.isStreaming, false);
    await sleep(options.warmupSeconds * 1000);
    await request({ type: "prompt", message: "/idle-main-probe start" });
    assert(fs.existsSync(spec.ready), "start command did not confirm native runtime");
    console.log(JSON.stringify({ phase: "measuring", seconds: options.seconds, pid: child.pid, root, evidence: options.out }));
    await sleep(options.seconds * 1000);
    assert(!exitInfo && !failure, "Main exited/failed during measurement");
    await request({ type: "prompt", message: "/idle-main-probe finish" });
    endState = await request({ type: "get_state" });
    capture = JSON.parse(fs.readFileSync(spec.capture, "utf8"));
    assert.equal(endState.isStreaming, false);
    assert.equal(events.filter(event => ["agent_start", "message_start", "tool_execution_start"].includes(event.type)).length, 0,
      "probe must not trigger an agent turn, tool or inference");
    assert.equal(capture.dropped, 0);
    assert(capture.windowEnd.monotonicMs - capture.windowStart.monotonicMs >= options.seconds * 1000);
  } catch (error) { failure = error; }
  finally {
    clearTimeout(watchdog); stopping = true;
    if (child && !exitInfo) {
      child.stdin.end();
      const term = setTimeout(() => child.kill("SIGTERM"), 10000);
      const kill = setTimeout(() => child.kill("SIGKILL"), 15000);
      await exited; clearTimeout(term); clearTimeout(kill);
    }
    if (exitInfo && (exitInfo.code !== 0 || exitInfo.signal !== null)) failure ??= new Error("unclean native shutdown");
    save(files.rpc, events);
    const idle = capture?.calls.filter(row => row.phase === "idle") ?? [];
    const report = { schema: 2, passed: !failure, recordedAt: new Date().toISOString(), options, root,
      head: execFileSync("git", ["rev-parse", "HEAD"], { encoding: "utf8" }).trim(),
      harnessSha256: hash(self), entrySha256: hash(path.join(options.candidate, "dist/index.js")), cliSha256: hash(options.cli),
      host: { hostname: os.hostname(), platform: process.platform, arch: process.arch, node: process.version },
      entrypoint: "native Pi RPC CLI -> production extension -> session_start mesh.announce activation",
      inferenceTurns: events.filter(event => event.type === "agent_start").length,
      windowMs: capture ? capture.windowEnd.monotonicMs - capture.windowStart.monotonicMs : 0,
      counts: { startupBatches: capture?.calls.filter(row => row.phase === "startup").length ?? 0, idleBatches: idle.length,
        idleSqliteBegins: capture?.transactions.filter(tx => tx.phase === "idle" && meshBegin(tx)).length ?? 0,
        idleOtherSqliteBegins: capture?.transactions.filter(tx => tx.phase === "idle" && tx.database.scope === "other-sqlite"
          && tx.verb === "BEGIN IMMEDIATE" && tx.succeeded).length ?? 0,
        idleUnknownSqliteBegins: capture?.transactions.filter(tx => tx.phase === "idle" && tx.database.scope === "unknown-sqlite"
          && tx.verb === "BEGIN IMMEDIATE" && tx.succeeded).length ?? 0,
        idleLocks: capture?.locks.filter(lock => lock.phase === "idle" && lock.method === "acquired").length ?? 0 },
      sites: capture ? aggregateSites(capture) : [], beginState, endState, exit: exitInfo,
      evidence: files,
      limitations: ["Fresh empty private project/mesh; no actors, peer sessions, traffic or user-installed extensions.",
        "RPC Main, not interactive TUI. Optional inference services disabled; no user prompt or provider invoked.",
        "Stacks/source maps, hook wrappers, AsyncLocalStorage and in-memory transaction/lock capture add diagnostic overhead.",
        "Only native driver exec transaction verbs are observed; other/unknown SQLite databases are labeled separately.",
        "Counts are observed native runtime activity, not a prediction for a populated shared mesh."],
      failure: failure?.stack ?? null };
    save(options.out, report);
    console.log(JSON.stringify({ passed: report.passed, windowMs: report.windowMs, counts: report.counts, exit: report.exit, out: options.out }));
  }
  if (failure) process.exitCode = 1;
}

if (process.argv[1] && path.resolve(process.argv[1]) === self) await main();
