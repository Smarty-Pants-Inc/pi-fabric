#!/usr/bin/env bun
import os from "node:os";
import path from "node:path";
import { createHash, randomBytes } from "node:crypto";
import { spawn, type ChildProcess } from "node:child_process";
import { createInterface } from "node:readline";
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
const USAGE = `Usage: bun scripts/mesh-load.ts --root <dir> --target-writes-per-min <n> --target-processes <n> [options]
  --root <dir>                 mesh root (required; never inferred)
  --target-writes-per-min <n>  mesh writes/min to sustain; at least ${HEARTBEAT_WRITES_PER_MIN} x --target-processes,
                               because each worker heartbeats every 5 s (${HEARTBEAT_WRITES_PER_MIN} writes/min)
  --target-processes <n>       minimum worker processes (positive integer)
  --max-workers <n>            worker cap (default 64)
  --duration <seconds>         stop after this many seconds; 0 (default) is explicit unbounded mode,
                               running until "stop" on stdin, SIGINT or SIGTERM
  --profile <name>             publish profile tag (default fleet)
  --dry-run                    validate and print the plan without writing
  --allow-epyc1                allow running on epyc1
Each run takes a random 8-hex run id; worker identities and keys are mesh-load-<run>-<n>,
and a run deletes only its own keys. Stop a run on any platform by writing "stop" to its stdin.
`;
const usageError = (message: string): never => {
  process.stderr.write(`mesh-load: ${message}\n${USAGE}`);
  process.exit(2);
};

if (args.has("--worker")) {
  const root = String(args.get("--root"));
  const id = String(args.get("--id"));
  const rate = Math.max(0, number("--rate", 1));
  const identity: MeshIdentity = { id, name: workerName(id), kind: "agent" };
  const store = new MeshStore(root, 64 * 1024, 100);
  let seq = 0;
  let stopped = false;
  const tasks = new Set<Promise<unknown>>();
  const report = () => process.stdout.write('{"type":"write"}\n');
  const start = (fn: () => Promise<unknown>) => {
    if (stopped) return;
    const task = fn().then(report).catch(error => process.stderr.write(`mesh-load worker ${id}: ${error instanceof Error ? error.message : String(error)}\n`));
    tasks.add(task);
    void task.finally(() => tasks.delete(task));
  };
  let pubTimer: ReturnType<typeof setInterval> | undefined;
  const setRate = (writesPerMin: number) => {
    if (pubTimer) clearInterval(pubTimer);
    const interval = writesPerMin > 0 ? Math.max(1, Math.round(60_000 / writesPerMin)) : 0;
    pubTimer = interval ? setInterval(() => start(() => store.publish({
      topic: "fleet.work.mesh-load", from: identity,
      data: { worker: id, seq: ++seq, at: Date.now(), profile: String(args.get("--profile") ?? "fleet") },
    })), interval) : undefined;
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
    await store.writeBatch({ identity, ops: [{ kind: "delete", key: presenceKey }, { kind: "delete", key: actorKey }] });
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
  // Heartbeat floor: more than floor(target / 12) workers overshoot the target on heartbeats alone.
  const workerCap = Math.min(maxWorkers, Math.floor(target / HEARTBEAT_WRITES_PER_MIN));
  const runId = randomBytes(4).toString("hex");
  const spawnedIds: string[] = [];
  const live = new Set<ChildProcess>();
  const children = new Map<number, ChildProcess>();
  let writes = 0;
  let stopped = false;
  let nextId = 0;
  let peakWorkers = 0;
  const spawnWorker = () => {
    const id = `${runId}-${++nextId}`;
    spawnedIds.push(id);
    const rate = Math.max(0, target / requested - HEARTBEAT_WRITES_PER_MIN);
    const child = spawn(process.execPath, [process.argv[1]!, "--worker", "--root", root, "--id", id, "--rate", String(rate), "--profile", String(args.get("--profile") ?? "fleet")], { stdio: ["pipe", "pipe", "inherit"], env: process.env });
    child.stdin?.on("error", () => { /* The worker already exited. */ });
    const key = child.pid ?? nextId;
    children.set(key, child);
    live.add(child);
    peakWorkers = Math.max(peakWorkers, children.size);
    let buffer = "";
    child.stdout?.on("data", chunk => {
      buffer += String(chunk);
      const lines = buffer.split("\n"); buffer = lines.pop() ?? "";
      for (const line of lines) try { if (JSON.parse(line).type === "write") writes++; } catch { /* Ignore diagnostics. */ }
    });
    child.on("exit", () => { children.delete(key); live.delete(child); });
  };
  // Cross-platform stop: SIGTERM on Windows kills without running handlers, so ask on stdin instead.
  const stopWorker = (child: ChildProcess) => {
    if (child.exitCode !== null || child.signalCode !== null || !child.stdin || child.stdin.writableEnded) return;
    child.stdin.write("stop\n");
    child.stdin.end();
  };
  const waitExit = (child: ChildProcess) => child.exitCode !== null || child.signalCode !== null ? Promise.resolve() : new Promise<void>(resolve => {
    const timer = setTimeout(() => child.kill(), 30_000);
    child.once("exit", () => { clearTimeout(timer); resolve(); });
  });
  const resize = (count: number) => {
    const desired = Math.max(1, Math.min(workerCap, count));
    while (children.size < desired) spawnWorker();
    while (children.size > desired) { const [key, child] = [...children.entries()].at(-1)!; children.delete(key); stopWorker(child); }
    const rate = Math.max(0, target / Math.max(1, children.size) - HEARTBEAT_WRITES_PER_MIN);
    for (const child of children.values()) child.stdin?.write(JSON.stringify({ rate }) + "\n");
  };
  if (args.has("--dry-run")) {
    process.stdout.write(JSON.stringify({ dryRun: true, root, runId, targetWritesPerMin: target, targetProcesses: requested, maxWorkers, workerCap, durationSec: duration }) + "\n");
    process.exit(0);
  }
  process.stderr.write(`mesh-load: run ${runId} root ${root}\n`);
  resize(requested);
  const started = Date.now();
  let lastMinute = started;
  let lastWrites = 0;
  let hasReported = false;
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
    const summary = summarizeLockStats(root, readLockStats(root), { minutes: 1 });
    if (!hasReported) {
      process.stdout.write(JSON.stringify({ at: new Date().toISOString(), workers: peakWorkers, writesPerMin: Math.round(writes / elapsedMin), busyPct: summary.busyPct, timeouts: summary.timeouts }) + "\n");
    }
    process.exit(exitCode);
  };
  const control = setInterval(() => {
    const summary = summarizeLockStats(root, readLockStats(root), { minutes: 1 });
    const elapsedMin = Math.max(1 / 120, (Date.now() - lastMinute) / 60_000);
    const achieved = Math.round((writes - lastWrites) / elapsedMin);
    if (achieved < target * 0.9 && children.size < workerCap) resize(children.size + 1);
    else if (achieved > target * 1.1 && children.size > requested) resize(children.size - 1);
    else if (summary.busyPct > 65 && children.size > 1) resize(children.size - 1);
  }, 30_000);
  const tick = setInterval(() => {
    const now = Date.now();
    const elapsedMin = (now - lastMinute) / 60_000;
    const achieved = elapsedMin > 0 ? Math.round((writes - lastWrites) / elapsedMin) : 0;
    lastWrites = writes; lastMinute = now;
    const summary = summarizeLockStats(root, readLockStats(root), { minutes: 1 });
    process.stdout.write(JSON.stringify({ at: new Date(now).toISOString(), workers: children.size, writesPerMin: achieved, busyPct: summary.busyPct, timeouts: summary.timeouts }) + "\n");
    hasReported = true;
    if (duration > 0 && now - started >= duration * 1000) void shutdown();
  }, 60_000);
  createInterface({ input: process.stdin }).on("line", line => { if (line.trim() === "stop") void shutdown(); });
  process.on("SIGINT", () => { void shutdown(); });
  process.on("SIGTERM", () => { void shutdown(); });
  if (duration > 0) setTimeout(() => { void shutdown(); }, duration * 1000 + 100);
}
