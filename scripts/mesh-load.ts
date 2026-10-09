#!/usr/bin/env bun
import os from "node:os";
import path from "node:path";
import { createHash, randomBytes } from "node:crypto";
import { spawn, type ChildProcess } from "node:child_process";
import { createInterface } from "node:readline";
import fs from "node:fs";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { readLockStats, summarizeLockStats } from "../src/mesh/commit-stats.js";

const args = new Map<string, string | true>();
for (let i = 2; i < process.argv.length; i++) {
  const arg = process.argv[i]!;
  if (arg.startsWith("--")) args.set(arg, process.argv[i + 1] && !process.argv[i + 1]!.startsWith("--") ? process.argv[++i]! : true);
}
const number = (key: string, fallback: number): number => {
  const value = args.get(key);
  return value === undefined || value === true ? fallback : Number(value);
};
const hash = (value: string): string => createHash("sha256").update(value).digest("hex");
/** Each worker heartbeats every 5 s; one heartbeat is one mesh write, so a worker never writes fewer than 12/min. */
const HEARTBEAT_WRITES_PER_MIN = 12;
/** Worker ids are `<run>-<n>`, so every identity name and key carries the run namespace `mesh-load-<run>-`. */
const workerName = (id: string): string => `mesh-load-${id}`;
const workerKeys = (id: string): { presenceKey: string; actorKey: string } => ({
  presenceKey: `topology/participants/${hash(workerName(id))}`,
  actorKey: `actors/${workerName(id)}/actor`,
});
/**
 * Each process flushes L8 lock stats 0.5-2.5 s after a wall-clock minute; reading "the last complete minute"
 * right at the boundary saw almost nothing, so the busy shed never fired. Read as of 5 s ago instead.
 */
const STATS_SETTLE_MS = 5_000;
/** A worker's keyed puts rotate over this many keys, so state does not grow with the run. */
const PUT_KEYS = 8;
const putKey = (id: string, seq: number): string => `fleet/work/${workerName(id)}/k${seq % PUT_KEYS}`;
const pad = (value: number, width: number): string => String(value).padStart(width, "0");
/** Hub-shaped bulk (an actor registry), the same record shape as benchmark-mesh-lock's --state-mb seed. */
const SEED_PREFIX = "fleet/actors/load-seed-a";
const seedValue = (i: number) => ({ id: `a${pad(i, 6)}`, kind: "actor", status: i % 3 ? "idle" : "done", host: `h${pad(i % 32, 2)}`,
  model: "load-seed-model", summary: "s".repeat(820) });
/**
 * Grow the effective backend's state to at least `mb` decimal MB: state.json for file/shadow,
 * state.db plus its WAL for sqlite. Idempotent: a root already at that size is left as is, and
 * seed keys are not run keys, so they stay for later runs like a hub's standing state.
 */
const seedState = async (root: string, mb: number): Promise<{
  stateBytes: number; seeded: number; backend: MeshStore["stateBackend"]; databaseBytes?: number; walBytes?: number;
}> => {
  const store = new MeshStore(root, 64 * 1024, 100, { lockTimeoutMs: 120_000 });
  try {
    // The seeder identity must not carry the run namespace: seed entries are not synthetic run keys.
    const identity: MeshIdentity = { id: "load-seeder", name: "load-seeder", kind: "agent" };
    const statePath = path.join(root, "state.json");
    // listAll also initializes SQLite before its database file is required for measurement.
    let next = store.listAll("", { fresh: true }).filter(entry => entry.key.startsWith(SEED_PREFIX)).length;
    const backend = store.stateBackend;
    const database = store.stateDiagnostics().database;
    if (backend === "sqlite" && !database) throw new Error("mesh-load: SQLite seed measurement has no database path");
    const sqliteSize = (file: string, optional = false): number => {
      try {
        const stat = fs.statSync(file);
        fs.accessSync(file, fs.constants.R_OK);
        if (!stat.isFile()) throw new Error("not a regular file");
        return stat.size;
      } catch (error) {
        if (optional && (error as NodeJS.ErrnoException).code === "ENOENT") return 0;
        throw new Error(`mesh-load: failed to measure SQLite seed state at ${file}: ${error instanceof Error ? error.message : String(error)}`, { cause: error });
      }
    };
    const sizeOf = () => {
      if (backend === "sqlite") {
        const databaseBytes = sqliteSize(database!);
        const walBytes = sqliteSize(`${database!}-wal`, true);
        return { stateBytes: databaseBytes + walBytes, databaseBytes, walBytes };
      }
      try { return { stateBytes: fs.statSync(statePath).size }; } catch { return { stateBytes: 0 }; }
    };
    const target = mb * 1e6;
    let seeded = 0;
    let size = sizeOf();
    while (size.stateBytes < target) {
      const count = Math.max(1, Math.min(1000, Math.ceil((target - size.stateBytes) / 1000)));
      await store.writeBatch({ identity, ops: Array.from({ length: count }, () => {
        const i = next++;
        return { kind: "put" as const, key: `${SEED_PREFIX}${pad(i, 6)}`, value: seedValue(i) };
      }) });
      seeded += count;
      size = sizeOf();
    }
    return { ...size, seeded, backend };
  } finally {
    store.closeState();
  }
};
const USAGE = `Usage: bun scripts/mesh-load.ts --root <dir> --target-writes-per-min <n> --target-processes <n> [options]
  --root <dir>                 mesh root (required; never inferred)
  --target-writes-per-min <n>  mesh writes/min to sustain; at least ${HEARTBEAT_WRITES_PER_MIN} x --target-processes,
                               because each worker heartbeats every 5 s (${HEARTBEAT_WRITES_PER_MIN} writes/min)
  --target-processes <n>       minimum worker processes (positive integer)
  --max-workers <n>            worker cap (default 64)
  --seed-state-mb <n>          before the run, grow effective backend state to n decimal MB (0-30, default 0):
                               state.json for file/shadow, state.db + WAL for sqlite, with hub-shaped actor
                               records (kept after the run). File/shadow puts rewrite state.json under the lock;
                               sqlite puts commit database transactions instead
  --put-share <f>              fraction (0-1, default 0) of the paced writes that are keyed puts (writeBatch,
                               backend-dependent state hold) instead of publishes (event append, about 0.5 ms hold)
  --custody-share <f>          fraction (0-1, default 0; with --put-share at most 1) of the paced writes that are
                               custody ops: scan all keyed state fresh through the effective backend under the
                               mesh lock, as registry and inbox custody does, without writing. The hold scales
                               with state size; it does not invalidate other writers' prepared snapshots
  --max-in-flight <n>          writes in flight per worker (default 1); a paced write due while the worker is
                               at the cap is skipped and counted, never queued
  --control-interval <s>       controller period in seconds (default 30)
  --duration <seconds>         stop after this many seconds; 0 (default) is explicit unbounded mode,
                               running until "stop" on stdin, SIGINT or SIGTERM
  --profile <name>             publish profile tag (default fleet)
  --dry-run                    validate and print the plan without writing
  --allow-epyc1                allow running on epyc1
Each run takes a random 8-hex run id; worker identities and keys are mesh-load-<run>-<n>,
and a run deletes only its own keys. Stop a run on any platform by writing "stop" to its stdin.
The controller starts --target-processes workers, adds one only when the last full control window fell
below 90% of the target, sheds one above 110% or when lock busy exceeds 65%, and ignores the window
right after each resize (worker start-up).
Backend selection follows MeshStore (PI_FABRIC_MESH_STATE_BACKEND; default file, with runtime/filesystem fallback).
Hub profile (ryzen5 file-backend scratch mesh, about 60% lock busy): see docs/mesh-lock-plan.md, "mesh-load hub profile".
`;
const usageError = (message: string): never => {
  process.stderr.write(`mesh-load: ${message}\n${USAGE}`);
  process.exit(2);
};

if (args.has("--worker")) {
  const root = String(args.get("--root"));
  const id = String(args.get("--id"));
  const rate = Math.max(0, number("--rate", 1));
  const putShare = Math.min(1, Math.max(0, number("--put-share", 0)));
  const custodyShare = Math.min(1 - putShare, Math.max(0, number("--custody-share", 0)));
  const maxInFlight = Math.max(1, Math.floor(number("--max-in-flight", 1)));
  const profile = String(args.get("--profile") ?? "fleet");
  const identity: MeshIdentity = { id, name: workerName(id), kind: "agent" };
  const store = new MeshStore(root, 64 * 1024, 100);
  let seq = 0;
  let putCredit = 0;
  let custodyCredit = 0;
  let stopped = false;
  const tasks = new Set<Promise<unknown>>();
  const putKeys = new Set<string>();
  const start = (fn: () => Promise<unknown>) => {
    if (stopped) return;
    // In-flight cap: a write due at the cap is skipped, so overload shows as a shortfall, not a queue of waiters.
    if (tasks.size >= maxInFlight) { process.stdout.write('{"type":"skip"}\n'); return; }
    const inFlight = tasks.size + 1;
    const task = fn().then(() => process.stdout.write(`{"type":"write","inFlight":${inFlight}}\n`))
      .catch(error => process.stderr.write(`mesh-load worker ${id}: ${error instanceof Error ? error.message : String(error)}\n`));
    tasks.add(task);
    void task.finally(() => tasks.delete(task));
  };
  const pacedWrite = () => {
    const data = { worker: id, seq: ++seq, at: Date.now(), profile };
    putCredit += putShare;
    custodyCredit += custodyShare;
    if (custodyCredit >= 1) {
      custodyCredit -= 1;
      return store.exclusive(() => store.listAll("", { fresh: true })).then(entries => {
        process.stdout.write(JSON.stringify({ type: "custody", entries: entries.length }) + "\n");
      });
    }
    if (putCredit >= 1) {
      putCredit -= 1;
      const key = putKey(id, seq);
      putKeys.add(key);
      return store.writeBatch({ identity, ops: [{ kind: "put", key, value: data }] });
    }
    return store.publish({ topic: "fleet.work.mesh-load", from: identity, data });
  };
  let pubTimer: ReturnType<typeof setInterval> | undefined;
  const setRate = (writesPerMin: number) => {
    if (pubTimer) clearInterval(pubTimer);
    const interval = writesPerMin > 0 ? Math.max(1, Math.round(60_000 / writesPerMin)) : 0;
    pubTimer = interval ? setInterval(() => start(pacedWrite), interval) : undefined;
  };
  setRate(rate);
  const control = createInterface({ input: process.stdin });
  control.on("line", line => {
    if (line.trim() === "stop") { stopAndExit(); return; }
    try { const message = JSON.parse(line) as { rate?: unknown }; if (typeof message.rate === "number" && Number.isFinite(message.rate) && message.rate >= 0) setRate(message.rate); } catch { /* Ignore malformed control input. */ }
  });
  // The controller closes stdin when it stops; EOF also means stop, so an orphaned worker cleans up.
  control.on("close", () => stopAndExit());
  const { presenceKey, actorKey } = workerKeys(id);
  const name = workerName(id);
  const heartbeat = () => start(() => store.writeBatch({ identity, ops: [
    { kind: "put", key: presenceKey, value: { format: 1, id: name, kind: "root", rootId: name, ownerHostId: name, ownerIdentityId: id, name, status: "running", runner: "pi", transport: "host", capabilities: ["steer", "followUp", "fabric"], sessionId: id, startedAt: Date.now(), updatedAt: Date.now(), controlProtocol: "v1", synthetic: true } },
    { kind: "put", key: actorKey, value: { id: "actor", name: `${name}-actor`, runner: "pi", status: "idle", createdAt: Date.now(), updatedAt: Date.now(), synthetic: true } },
  ] }));
  const heartbeatTimer = setInterval(heartbeat, 5_000);
  heartbeat();
  let cleaning: Promise<void> | undefined;
  const cleanup = (): Promise<void> => cleaning ??= (async () => {
    stopped = true;
    if (pubTimer) clearInterval(pubTimer);
    clearInterval(heartbeatTimer);
    await Promise.allSettled([...tasks]);
    await store.writeBatch({ identity, ops: [presenceKey, actorKey, ...putKeys].map(key => ({ kind: "delete" as const, key })) });
  })();
  // "stop"/EOF on stdin works on every platform; signals remain for POSIX use.
  function stopAndExit(): void {
    void cleanup().then(() => process.exit(0), error => {
      process.stderr.write(`mesh-load worker ${id}: cleanup failed: ${error instanceof Error ? error.message : String(error)}\n`);
      process.exit(1);
    });
  }
  process.on("SIGINT", stopAndExit);
  process.on("SIGTERM", stopAndExit);
} else {
  if (args.has("--help")) { process.stdout.write(USAGE); process.exit(0); }
  const rootArg = args.get("--root");
  if (typeof rootArg !== "string" || !rootArg.trim()) usageError("--root is required; mesh-load never infers a mesh root");
  if (os.hostname().toLowerCase().startsWith("epyc1") && !args.has("--allow-epyc1")) throw new Error("refusing to run on epyc1 without --allow-epyc1");
  const root = path.resolve(rootArg as string);
  const target = number("--target-writes-per-min", NaN);
  const requested = number("--target-processes", NaN);
  const maxWorkers = number("--max-workers", 64);
  const durationArg = args.get("--duration");
  const duration = durationArg === undefined ? 0 : typeof durationArg === "string" && durationArg.trim() ? Number(durationArg) : NaN;
  if (!Number.isFinite(duration) || duration < 0) usageError(`--duration must be a number of seconds >= 0 (0 = unbounded), got ${JSON.stringify(durationArg)}`);
  if (!Number.isInteger(requested) || requested < 1 || !Number.isInteger(maxWorkers) || maxWorkers < 1 || requested > maxWorkers) {
    usageError("--target-processes and --max-workers must be positive integers, with --target-processes <= --max-workers");
  }
  if (!Number.isFinite(target) || target < HEARTBEAT_WRITES_PER_MIN * requested) {
    usageError(`--target-writes-per-min must be at least ${HEARTBEAT_WRITES_PER_MIN} x --target-processes (${HEARTBEAT_WRITES_PER_MIN * requested}): each worker heartbeats every 5 s, which alone is ${HEARTBEAT_WRITES_PER_MIN} writes/min`);
  }
  const seedMb = number("--seed-state-mb", 0);
  const putShare = number("--put-share", 0);
  const custodyShare = number("--custody-share", 0);
  const maxInFlight = number("--max-in-flight", 1);
  const controlIntervalS = number("--control-interval", 30);
  if (!Number.isFinite(seedMb) || seedMb < 0 || seedMb > 30) usageError("--seed-state-mb must be a number of MB from 0 to 30 (the state cap is 32 MiB)");
  if (!Number.isFinite(putShare) || putShare < 0 || putShare > 1) usageError("--put-share must be a fraction from 0 to 1");
  if (!Number.isFinite(custodyShare) || custodyShare < 0 || putShare + custodyShare > 1) usageError("--custody-share must be a fraction from 0 to 1, and --put-share + --custody-share at most 1");
  if (!Number.isInteger(maxInFlight) || maxInFlight < 1) usageError("--max-in-flight must be a positive integer");
  if (!Number.isFinite(controlIntervalS) || controlIntervalS < 1) usageError("--control-interval must be at least 1 second");
  // Heartbeat floor: more than floor(target / 12) workers overshoot the target on heartbeats alone.
  const workerCap = Math.min(maxWorkers, Math.floor(target / HEARTBEAT_WRITES_PER_MIN));
  const runId = randomBytes(4).toString("hex");
  const spawnedIds: string[] = [];
  const live = new Set<ChildProcess>();
  const children = new Map<number, ChildProcess>();
  let writes = 0;
  let custodyReads = 0;
  let custodyEntries = 0;
  let skipped = 0;
  let maxSeenInFlight = 0;
  let stopped = false;
  let nextId = 0;
  let peakWorkers = 0;
  const spawnWorker = () => {
    const id = `${runId}-${++nextId}`;
    spawnedIds.push(id);
    const rate = Math.max(0, target / requested - HEARTBEAT_WRITES_PER_MIN);
    const child = spawn(process.execPath, [process.argv[1]!, "--worker", "--root", root, "--id", id, "--rate", String(rate), "--profile", String(args.get("--profile") ?? "fleet"),
      "--put-share", String(putShare), "--custody-share", String(custodyShare), "--max-in-flight", String(maxInFlight)], { stdio: ["pipe", "pipe", "inherit"], env: process.env });
    child.stdin?.on("error", () => { /* The worker already exited. */ });
    const key = child.pid ?? nextId;
    children.set(key, child);
    live.add(child);
    peakWorkers = Math.max(peakWorkers, children.size);
    let buffer = "";
    child.stdout?.on("data", chunk => {
      buffer += String(chunk);
      const lines = buffer.split("\n"); buffer = lines.pop() ?? "";
      for (const line of lines) try {
        const message = JSON.parse(line) as { type?: unknown; inFlight?: unknown; entries?: unknown };
        if (message.type === "write") {
          writes++;
          if (typeof message.inFlight === "number") maxSeenInFlight = Math.max(maxSeenInFlight, message.inFlight);
        } else if (message.type === "skip") skipped++;
        else if (message.type === "custody" && typeof message.entries === "number") {
          custodyReads++;
          custodyEntries = Math.max(custodyEntries, message.entries);
        }
      } catch { /* Ignore diagnostics. */ }
    });
    child.on("exit", () => { children.delete(key); live.delete(child); });
    // "close" follows the last stdout data, so a stopping worker's final writes are counted.
    child.on("close", () => closed.add(child));
  };
  // Cross-platform stop: SIGTERM on Windows kills without running handlers, so ask on stdin instead.
  const stopWorker = (child: ChildProcess) => {
    if (child.exitCode !== null || child.signalCode !== null || !child.stdin || child.stdin.writableEnded) return;
    child.stdin.write("stop\n");
    child.stdin.end();
  };
  const closed = new Set<ChildProcess>();
  const waitExit = (child: ChildProcess) => closed.has(child) ? Promise.resolve() : new Promise<void>(resolve => {
    const timer = setTimeout(() => child.kill(), 30_000);
    child.once("close", () => { clearTimeout(timer); resolve(); });
  });
  const resize = (count: number) => {
    const desired = Math.max(1, Math.min(workerCap, count));
    while (children.size < desired) spawnWorker();
    while (children.size > desired) { const [key, child] = [...children.entries()].at(-1)!; children.delete(key); stopWorker(child); }
    const rate = Math.max(0, target / Math.max(1, children.size) - HEARTBEAT_WRITES_PER_MIN);
    for (const child of children.values()) child.stdin?.write(JSON.stringify({ rate }) + "\n");
  };
  if (args.has("--dry-run")) {
    process.stdout.write(JSON.stringify({ dryRun: true, root, runId, targetWritesPerMin: target, targetProcesses: requested, maxWorkers, workerCap, durationSec: duration,
      seedStateMb: seedMb, putShare, custodyShare, maxInFlight, controlIntervalSec: controlIntervalS }) + "\n");
    process.exit(0);
  }
  const store = new MeshStore(root, 64 * 1024, 100);
  const backend = store.stateBackend;
  store.closeState();
  let seededStateBytes = 0;
  if (seedMb > 0) {
    const seeded = await seedState(root, seedMb);
    seededStateBytes = seeded.stateBytes;
    process.stderr.write(seeded.backend === "sqlite"
      ? `mesh-load: seeded ${seeded.seeded} records; backend=sqlite state.db is ${seeded.databaseBytes} bytes + wal ${seeded.walBytes} bytes (${seeded.stateBytes} bytes total)\n`
      : `mesh-load: seeded ${seeded.seeded} records; state.json is ${seeded.stateBytes} bytes\n`);
  }
  process.stderr.write(`mesh-load: run ${runId} root ${root} backend=${backend}\n`);
  resize(requested);
  const started = Date.now();
  let lastMinute = started;
  let lastWrites = 0;
  const shutdown = async () => {
    if (stopped) return;
    stopped = true; clearInterval(tick); clearInterval(control);
    const stopping = [...live];
    for (const child of stopping) stopWorker(child);
    await Promise.all(stopping.map(waitExit));
    // Workers clean their own keys; delete anything left in this run's namespace (never another run's).
    let exitCode = 0;
    try {
      const store = new MeshStore(root, 64 * 1024, 100);
      const own = new Set(spawnedIds.flatMap(id => Object.values(workerKeys(id))));
      const leftover = store.listAll().map(entry => entry.key).filter(key => own.has(key) || key.includes(`mesh-load-${runId}-`));
      if (leftover.length) await store.writeBatch({ identity: { id: runId, name: `mesh-load-${runId}`, kind: "agent" }, ops: leftover.map(key => ({ kind: "delete" as const, key })) });
    } catch (error) {
      process.stderr.write(`mesh-load: run ${runId} cleanup failed: ${error instanceof Error ? error.message : String(error)}\n`);
      exitCode = 1;
    }
    const elapsedMin = Math.max(1 / 60, (Date.now() - started) / 60_000);
    const summary = summarizeLockStats(root, readLockStats(root), { minutes: 1, now: Date.now() - STATS_SETTLE_MS });
    // The final line covers the whole run, including successful keyed-state custody reads and initial seed size.
    process.stdout.write(JSON.stringify({ at: new Date().toISOString(), final: true, backend, workers: peakWorkers, writesPerMin: Math.round(writes / elapsedMin),
      custodyReads, custodyEntries, seededStateBytes, skipped, maxInFlight: maxSeenInFlight, busyPct: summary.busyPct, timeouts: summary.timeouts }) + "\n");
    process.exit(exitCode);
  };
  // The controller keeps its own window, independent of the minute report: reading the report's freshly reset
  // minute as a full window made it add a worker every 30 s even at target. The window after a resize
  // includes worker start-up, so it is discarded rather than judged.
  let controlWrites = 0;
  let controlAt = Date.now();
  let settling = true;
  const control = setInterval(() => {
    const now = Date.now();
    const achieved = Math.round((writes - controlWrites) / Math.max(1 / 120, (now - controlAt) / 60_000));
    controlWrites = writes; controlAt = now;
    if (settling) { settling = false; return; }
    const summary = summarizeLockStats(root, readLockStats(root), { minutes: 1, now: Date.now() - STATS_SETTLE_MS });
    const before = children.size;
    if (summary.busyPct > 65 && children.size > 1) resize(children.size - 1);
    else if (achieved > target * 1.1 && children.size > requested) resize(children.size - 1);
    else if (achieved < target * 0.9 && children.size < workerCap) resize(children.size + 1);
    if (children.size !== before) settling = true;
  }, controlIntervalS * 1000);
  const tick = setInterval(() => {
    const now = Date.now();
    const elapsedMin = (now - lastMinute) / 60_000;
    const achieved = elapsedMin > 0 ? Math.round((writes - lastWrites) / elapsedMin) : 0;
    lastWrites = writes; lastMinute = now;
    const summary = summarizeLockStats(root, readLockStats(root), { minutes: 1, now: Date.now() - STATS_SETTLE_MS });
    process.stdout.write(JSON.stringify({ at: new Date(now).toISOString(), backend, workers: children.size, writesPerMin: achieved, skipped,
      acqPerMin: summary.n, holdMeanMs: Math.round(summary.holdMeanMs * 100) / 100, waitP99Ms: summary.waitP99Ms,
      busyPct: summary.busyPct, timeouts: summary.timeouts }) + "\n");
    if (duration > 0 && now - started >= duration * 1000) void shutdown();
  }, 60_000);
  createInterface({ input: process.stdin }).on("line", line => { if (line.trim() === "stop") void shutdown(); });
  process.on("SIGINT", () => { void shutdown(); });
  process.on("SIGTERM", () => { void shutdown(); });
  if (duration > 0) setTimeout(() => { void shutdown(); }, duration * 1000 + 100);
}
