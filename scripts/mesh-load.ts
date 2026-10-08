#!/usr/bin/env bun
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
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

if (args.has("--worker")) {
  const root = String(args.get("--root"));
  const id = String(args.get("--id"));
  const rate = Math.max(0, number("--rate", 1));
  const identity: MeshIdentity = { id, name: `mesh-load-${id}`, kind: "agent" };
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
  createInterface({ input: process.stdin }).on("line", line => {
    try { const message = JSON.parse(line) as { rate?: unknown }; if (typeof message.rate === "number" && Number.isFinite(message.rate) && message.rate >= 0) setRate(message.rate); } catch { /* Ignore malformed control input. */ }
  });
  const presenceKey = `topology/participants/${hash(`mesh-load-${id}`)}`;
  const actorKey = `actors/mesh-load-${id}/actor`;
  const heartbeat = () => start(() => store.writeBatch({ identity, ops: [
    { kind: "put", key: presenceKey, value: { format: 1, id: `mesh-load-${id}`, kind: "root", rootId: `mesh-load-${id}`, ownerHostId: `mesh-load-${id}`, ownerIdentityId: id, name: `mesh-load-${id}`, status: "running", runner: "pi", transport: "host", capabilities: ["steer", "followUp", "fabric"], sessionId: id, startedAt: Date.now(), updatedAt: Date.now(), controlProtocol: "v1", synthetic: true } },
    { kind: "put", key: actorKey, value: { id: "actor", name: `mesh-load-${id}-actor`, runner: "pi", status: "idle", createdAt: Date.now(), updatedAt: Date.now(), synthetic: true } },
  ] }));
  const heartbeatTimer = setInterval(heartbeat, 5_000);
  heartbeat();
  const cleanup = async () => {
    if (stopped) return;
    stopped = true;
    if (pubTimer) clearInterval(pubTimer);
    clearInterval(heartbeatTimer);
    await Promise.allSettled([...tasks]);
    await store.writeBatch({ identity, ops: [{ kind: "delete", key: presenceKey }, { kind: "delete", key: actorKey }] });
  };
  process.on("SIGINT", () => { void cleanup().finally(() => process.exit(0)); });
  process.on("SIGTERM", () => { void cleanup().finally(() => process.exit(0)); });
} else {
  const rootArg = args.get("--root");
  if (typeof rootArg !== "string" || !rootArg.trim()) throw new Error("--root is required; mesh-load never infers a mesh root");
  if (os.hostname().toLowerCase().startsWith("epyc1") && !args.has("--allow-epyc1")) throw new Error("refusing to run on epyc1 without --allow-epyc1");
  const root = path.resolve(rootArg);
  const target = number("--target-writes-per-min", NaN);
  const requested = number("--target-processes", NaN);
  const maxWorkers = number("--max-workers", 64);
  const duration = number("--duration", 0);
  if (!Number.isFinite(target) || target < 0 || !Number.isInteger(requested) || requested < 1 || !Number.isInteger(maxWorkers) || maxWorkers < 1 || requested > maxWorkers) {
    throw new Error("target writes/min must be nonnegative; processes and max-workers must be positive integers, with processes <= max-workers");
  }
  const children = new Map<number, ChildProcess>();
  let writes = 0;
  let stopped = false;
  let nextId = 0;
  let peakWorkers = 0;
  const spawnWorker = () => {
    const id = `worker-${++nextId}`;
    const rate = Math.max(0, target / requested - 12);
    const child = spawn(process.execPath, [process.argv[1]!, "--worker", "--root", root, "--id", id, "--rate", String(rate), "--profile", String(args.get("--profile") ?? "fleet")], { stdio: ["pipe", "pipe", "inherit"], env: process.env });
    const key = child.pid ?? nextId;
    children.set(key, child);
    peakWorkers = Math.max(peakWorkers, children.size);
    let buffer = "";
    child.stdout?.on("data", chunk => {
      buffer += String(chunk);
      const lines = buffer.split("\n"); buffer = lines.pop() ?? "";
      for (const line of lines) try { if (JSON.parse(line).type === "write") writes++; } catch { /* Ignore diagnostics. */ }
    });
    child.on("exit", () => children.delete(key));
  };
  const resize = (count: number) => {
    const desired = Math.max(1, Math.min(maxWorkers, count));
    while (children.size < desired) spawnWorker();
    while (children.size > desired) { const [key, child] = [...children.entries()].at(-1)!; children.delete(key); child.kill("SIGTERM"); }
    const rate = Math.max(0, target / Math.max(1, children.size) - 12);
    for (const child of children.values()) child.stdin?.write(JSON.stringify({ rate }) + "\n");
  };
  if (args.has("--dry-run")) {
    process.stdout.write(JSON.stringify({ dryRun: true, root, targetWritesPerMin: target, targetProcesses: requested, maxWorkers }) + "\n");
    process.exit(0);
  }
  resize(requested);
  const started = Date.now();
  let lastMinute = started;
  let lastWrites = 0;
  let hasReported = false;
  const shutdown = async () => {
    if (stopped) return;
    stopped = true; clearInterval(tick); clearInterval(control);
    for (const child of children.values()) child.kill("SIGTERM");
    await Promise.all([...children.values()].map(child => new Promise<void>(resolve => child.once("exit", () => resolve()))));
    const elapsedMin = Math.max(1 / 60, (Date.now() - started) / 60_000);
    const summary = summarizeLockStats(root, readLockStats(root), { minutes: 1 });
    if (!hasReported) {
      process.stdout.write(JSON.stringify({ at: new Date().toISOString(), workers: peakWorkers, writesPerMin: Math.round(writes / elapsedMin), busyPct: summary.busyPct, timeouts: summary.timeouts }) + "\n");
    }
    process.exit(0);
  };
  const control = setInterval(() => {
    const summary = summarizeLockStats(root, readLockStats(root), { minutes: 1 });
    const elapsedMin = Math.max(1 / 120, (Date.now() - lastMinute) / 60_000);
    const achieved = Math.round((writes - lastWrites) / elapsedMin);
    if (achieved < target * 0.9 && children.size < maxWorkers) resize(children.size + 1);
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
  process.on("SIGINT", () => { void shutdown(); });
  process.on("SIGTERM", () => { void shutdown(); });
  if (duration > 0) setTimeout(() => { void shutdown(); }, duration * 1000 + 100);
}
