#!/usr/bin/env bun
// smarty-dev#8526: several resident hosts of one project share ONE (empty) project actor
// registry lock; each owns its actors in its own session registry. Host 0 is the slow one
// (89 actors, 22 KB instructions, a mutation every --slowPulse ms); the others own 5 actors
// and mutate every --pulse ms. A second process holds BEGIN IMMEDIATE on the shared SQLite
// mesh in 100-300 ms bursts. Reports, per host, waits for and holds of the shared lock.
//   taskset -c 4,5 bun scripts/probe-shared-project-fence.ts --hosts=4 --duration=30000
import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { ActorRegistryStore } from "../src/actors/registry-store.js";

const args = Object.fromEntries(process.argv.slice(2).map(arg => arg.replace(/^--/, "").split("=")));
const pct = (values: number[], p: number) => values.length ? Math.round(values[Math.min(values.length - 1, Math.floor(values.length * p))]!) : 0;
const dist = (values: number[]) => { const ms = [...values].sort((a, b) => a - b);
  return { n: ms.length, totalMs: Math.round(ms.reduce((a, b) => a + b, 0)), p50: pct(ms, 0.5), p90: pct(ms, 0.9), max: pct(ms, 1) }; };

if (args.role === "sqlite-writer") {
  const { Database } = await import("bun:sqlite");
  const db = new Database(path.join(args.mesh!, "state.db"));
  db.exec("PRAGMA busy_timeout = 5000");
  while (Date.now() < Number(args.until)) {
    db.exec("BEGIN IMMEDIATE");
    await new Promise(resolve => setTimeout(resolve, 100 + Math.random() * 200));
    db.exec("COMMIT");
    await new Promise(resolve => setTimeout(resolve, 100));
  }
  process.exit(0);
}

if (args.role === "host") {
  const { DEFAULT_FABRIC_CONFIG } = await import("../src/config.js");
  const { ResidentHost } = await import("../src/residency/host.js");
  const { residentRoot } = await import("../src/residency/protocol.js");
  const base = args.base!, index = Number(args.index), actors = Number(args.actors), pulse = Number(args.pulse);
  const rootId = `session:shared-fence-${index}`, meshRoot = path.join(base, "mesh");
  const sessionActorRoot = path.join(base, "project-actors", `session-${index}`);
  const config = {
    format: 1, rootId, sessionId: `shared-fence-${index}`, cwd: base, projectRoot: base,
    meshRoot, actorRoot: path.join(base, "project-actors"), sessionActorRoot,
    residencyRoot: residentRoot(meshRoot, rootId), fullCodeMode: true,
    agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, nice: 19 }, mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, stateBackend: "sqlite" },
    retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: path.resolve("dist/worker.js"),
    fabricExtensionPath: path.resolve("dist/index.js"), piBinary: "unused", claudeBinary: "unused", vedaBinary: "unused",
  } as unknown as import("../src/residency/protocol.js").ResidentHostConfig;
  fs.mkdirSync(config.residencyRoot, { recursive: true });
  fs.writeFileSync(path.join(config.residencyRoot, "config.json"), JSON.stringify(config));
  fs.mkdirSync(sessionActorRoot, { recursive: true });
  const now = Date.now();
  fs.writeFileSync(path.join(sessionActorRoot, "actors.json"), JSON.stringify({ format: 1, actors: Array.from({ length: actors }, (_, i) => ({
    id: `${(index + 1).toString(16).padStart(4, "0")}${(i + 1).toString(16).padStart(28, "0")}`, name: `a-${index}-${i}`,
    instructions: "Review the change for correctness. ".repeat(630), createdAt: now, updatedAt: now, rootId, residency: "durable",
    runner: "pi", status: "idle", scope: "session", events: [], topics: [], messages: [] })) }));
  // Shared-lock waits and holds; the shared project registry is the one with no rows.
  const waits: number[] = [], holds: number[] = [], otherHolds: number[] = [];
  const proto = ActorRegistryStore.prototype as unknown as { withLock: (op: () => unknown, timeoutMs?: number) => Promise<unknown> };
  const withLock = proto.withLock;
  proto.withLock = async function (this: ActorRegistryStore, operation: () => unknown, timeoutMs?: number) {
    const shared = this.snapshot().actors.length === 0, asked = performance.now();
    let acquired = 0;
    try { return await withLock.call(this, () => { acquired = performance.now(); if (shared) waits.push(acquired - asked); return operation(); }, timeoutMs); }
    finally { if (acquired) (shared ? holds : otherHolds).push(performance.now() - acquired); }
  };
  const host = new ResidentHost(config);
  await host.start();
  await host.participants.refresh();
  const owned = host.actors.listOwned().map(actor => actor.id);
  process.stdout.write("ready\n");
  await new Promise<void>(resolve => process.stdin.once("data", () => resolve()));
  waits.length = 0; holds.length = 0; otherHolds.length = 0;
  const until = Date.now() + Number(args.duration);
  let mutations = 0, errors = 0;
  while (Date.now() < until) {
    const id = owned[mutations++ % owned.length]!;
    try { await host.actors.setActivationFilter(id, mutations % 2 ? [{ id: "probe", topic: ["probe.noise"], kind: ["skip"] }] : null); }
    catch { errors++; }
    await new Promise(resolve => setTimeout(resolve, pulse));
  }
  const result = { host: index, actors: owned.length, mutations, errors, sharedWaitMs: dist(waits), sharedHoldMs: dist(holds), sessionHoldMs: dist(otherHolds) };
  await host.close();
  process.stderr.write(`RESULT ${JSON.stringify(result)}\n`);
  process.exit(0);
}

const hosts = Number(args.hosts ?? 4), duration = Number(args.duration ?? 30_000);
const base = fs.mkdtempSync(path.join(args.tmp ?? os.tmpdir(), "shared-fence-"));
fs.mkdirSync(path.join(base, "project-actors"), { recursive: true });
fs.writeFileSync(path.join(base, "project-actors", "actors.json"), JSON.stringify({ format: 1, actors: [] }));
fs.mkdirSync(path.join(base, "mesh"), { recursive: true });
await (await import("../src/mesh/backend-migration.js")).importMeshState(path.join(base, "mesh"));
const { MeshStore } = await import("../src/mesh/store.js");
const { LIVENESS_POLICY_KEY } = await import("../src/topology/host-leases.js");
await new MeshStore(path.join(base, "mesh"), 256 * 1024, 500, { stateBackend: "sqlite" } as never).put({ key: LIVENESS_POLICY_KEY,
  value: { version: 1, hostLeases: "files", participants: "files" }, identity: { id: "session:probe", name: "probe", kind: "main", sessionId: "probe" } });
const results: string[] = [];
const children = Array.from({ length: hosts }, (_, index) => {
  const child = spawn(process.execPath, [import.meta.filename ?? process.argv[1]!, "--role=host", `--base=${base}`, `--index=${index}`,
    `--actors=${index === 0 ? 89 : 5}`, `--pulse=${index === 0 ? Number(args.slowPulse ?? 200) : Number(args.pulse ?? 1000)}`, `--duration=${duration}`],
  { stdio: ["pipe", "pipe", "pipe"] });
  child.stderr.on("data", chunk => { for (const line of String(chunk).split("\n")) if (line.startsWith("RESULT ")) results.push(line.slice(7)); });
  const ready = new Promise<void>((resolve, reject) => {
    child.stdout.on("data", chunk => { if (String(chunk).includes("ready")) resolve(); });
    child.once("exit", code => reject(new Error(`host ${index} exited ${code} before ready`)));
  });
  const done = new Promise<void>(resolve => child.once("close", () => resolve()));
  return { child, ready, done };
});
const processes = children.map(({ child }) => child);
try {
  await Promise.all(children.map(child => child.ready));
  processes.push(spawn(process.execPath, [import.meta.filename ?? process.argv[1]!, "--role=sqlite-writer", `--mesh=${path.join(base, "mesh")}`,
    `--until=${Date.now() + duration}`], { stdio: "ignore" }));
  for (const { child } of children) child.stdin!.write("go\n");
  await Promise.all(children.map(child => child.done));
} finally {
  // Stop every child and wait for it to exit before the fixture directory goes.
  await Promise.all(processes.map(child => new Promise<void>(resolve => {
    if (child.exitCode !== null || child.signalCode !== null) return resolve();
    child.once("exit", () => resolve());
    child.kill();
  })));
  fs.rmSync(base, { recursive: true, force: true });
}
console.log(JSON.stringify(results.map(line => JSON.parse(line)).sort((a, b) => a.host - b.host), null, 1));
