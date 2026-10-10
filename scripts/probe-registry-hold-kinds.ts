#!/usr/bin/env bun
// smarty-dev#8526: who holds an actor registry lock, and for how long, by call site.
// A real ResidentHost owns 19 durable actors in a root shaped like the live review-fleet-lead
// root (89 actors, ~22 KB instructions each) and changes one owned actor per --pulse ms through
// the real manager (registry save + presence change refresh). Separate saver processes write the
// other 70 actors' rows (foreign lineages) through ActorRegistryStore.update.
// Run (isolated, one CPU, CPU-saturated):
//   taskset -c 4 bun scripts/probe-registry-hold-kinds.ts --savers=6 --busy=3 --duration=30000
import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { ActorRegistryStore } from "../src/actors/registry-store.js";

const args = Object.fromEntries(process.argv.slice(2).map(arg => arg.replace(/^--/, "").split("=")));
const ACTORS = 89, OWNED = 19, INSTRUCTIONS = 22_000;
const idOf = (i: number) => (i + 1).toString(16).padStart(32, "0");
type Hold = { kind: string; ms: number; wrote: boolean };
const seen = new Set<string>();

/** Tag every lock hold of this process by its synchronous call site. */
const instrument = (holds: Hold[], kindOf: (stack: string) => string): void => {
  const proto = ActorRegistryStore.prototype as unknown as { withLock: (op: () => unknown, timeoutMs?: number) => Promise<unknown> };
  const withLock = proto.withLock;
  let writes = 0;
  const write = ActorRegistryStore.prototype.prepare;
  ActorRegistryStore.prototype.prepare = function (this: ActorRegistryStore, ...rest: Parameters<typeof write>) {
    const prepared = write.apply(this, rest);
    return { ...prepared, commit: () => { prepared.commit(); writes++; } };
  } as typeof write;
  const rawWrite = ActorRegistryStore.prototype.write;
  ActorRegistryStore.prototype.write = function (this: ActorRegistryStore, ...rest: Parameters<typeof rawWrite>) {
    rawWrite.apply(this, rest); writes++;
  } as typeof rawWrite;
  proto.withLock = async function (this: unknown, operation: () => unknown, timeoutMs?: number) {
    const stack = new Error().stack ?? "";
    const kind = kindOf(stack);
    if (process.env.PROBE_STACKS && !seen.has(kind)) { seen.add(kind); console.error(kind, stack.split("\n").slice(1, 14).join("\n")); }
    let acquired = 0, before = 0;
    try {
      return await withLock.call(this, () => { acquired = performance.now(); before = writes; return operation(); }, timeoutMs);
    } finally { if (acquired) holds.push({ kind, ms: performance.now() - acquired, wrote: writes > before }); }
  };
};

if (args.role === "sqlite-writer") {
  // Another busy hub writer: BEGIN IMMEDIATE held for a random 100-300 ms burst, then a gap.
  const { Database } = await import("bun:sqlite");
  const db = new Database(path.join(args.mesh!, "state.db"));
  db.exec("PRAGMA busy_timeout = 5000");
  const until = Number(args.until), low = Number(args.low ?? 100), high = Number(args.high ?? 300), gap = Number(args.gap ?? 100);
  let bursts = 0;
  while (Date.now() < until) {
    db.exec("BEGIN IMMEDIATE");
    await new Promise(resolve => setTimeout(resolve, low + Math.random() * (high - low)));
    db.exec("COMMIT");
    bursts++;
    await new Promise(resolve => setTimeout(resolve, gap));
  }
  process.stdout.write(JSON.stringify({ bursts }));
  process.exit(0);
}

if (args.role === "saver") {
  const root = args.root!, index = Number(args.index), savers = Number(args.savers), until = Number(args.until);
  const holds: Hold[] = [];
  instrument(holds, () => "saver-update");
  const store = new ActorRegistryStore(root);
  const mine = Array.from({ length: ACTORS - OWNED }, (_, i) => idOf(OWNED + i)).filter((_, i) => i % savers === index);
  const rows = new Map(store.snapshot().actors.filter(row => mine.includes(row.id)).map(row => [row.id, { ...row }]));
  let errors = 0;
  while (Date.now() < until) {
    const row = [...rows.values()][Math.floor(Math.random() * rows.size)]!;
    row.updatedAt = Date.now(); row.status = row.status === "running" ? "idle" : "running";
    try {
      await store.update(current => ({ actors: [...current.filter(r => !rows.has(r.id)), ...[...rows.values()].map(r => ({ ...r }))], value: true }));
    } catch { errors++; }
    await new Promise(resolve => setTimeout(resolve, 300 + Math.random() * 400));
  }
  process.stdout.write(JSON.stringify({ holds, errors }));
  process.exit(0);
}

const savers = Number(args.savers ?? 6), busy = Number(args.busy ?? 3), duration = Number(args.duration ?? 30_000);
const pulse = Number(args.pulse ?? 500), backend = args.backend ?? "sqlite";
const base = fs.mkdtempSync(path.join(args.tmp ?? os.tmpdir(), "registry-hold-kinds-"));
const { DEFAULT_FABRIC_CONFIG } = await import("../src/config.js");
const { ResidentHost } = await import("../src/residency/host.js");
const { residentRoot } = await import("../src/residency/protocol.js");
const rootId = "session:hold-kinds-probe";
const config = {
  format: 1, rootId, sessionId: "hold-kinds-probe", cwd: base, projectRoot: base,
  meshRoot: path.join(base, "mesh"), actorRoot: path.join(base, "actors"), sessionActorRoot: path.join(base, "session-actors"),
  residencyRoot: residentRoot(path.join(base, "mesh"), rootId), fullCodeMode: true,
  agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, nice: 19 }, mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, stateBackend: backend },
  retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: path.resolve("dist/worker.js"),
  fabricExtensionPath: path.resolve("dist/index.js"), piBinary: "unused", claudeBinary: "unused", vedaBinary: "unused",
} as unknown as import("../src/residency/protocol.js").ResidentHostConfig;
fs.mkdirSync(config.residencyRoot, { recursive: true });
if (backend === "sqlite") await (await import("../src/mesh/backend-migration.js")).importMeshState(config.meshRoot);
fs.writeFileSync(path.join(config.residencyRoot, "config.json"), JSON.stringify(config));
const now = Date.now();
const persona = (i: number) => `persona ${i % 40}\n` + "Review the change for correctness. ".repeat(Math.ceil(INSTRUCTIONS / 35)).slice(0, INSTRUCTIONS);
fs.mkdirSync(config.actorRoot, { recursive: true });
fs.mkdirSync(config.sessionActorRoot!, { recursive: true });
fs.writeFileSync(path.join(config.sessionActorRoot!, "actors.json"), JSON.stringify({ format: 1, actors: [] }));
fs.writeFileSync(path.join(config.actorRoot, "actors.json"), JSON.stringify({ format: 1, actors: Array.from({ length: ACTORS }, (_, i) => ({
  id: idOf(i), name: `review-${i}`, instructions: persona(i), createdAt: now, updatedAt: now,
  rootId: i < OWNED ? rootId : `session:foreign-${i % savers}`, residency: "durable", runner: "pi", status: "idle", scope: "project",
  events: [], topics: [], messages: [],
})) }));
const holds: Hold[] = [];
// Bun inlines withLocks/acquire; the fence's caller (participant-directory) still shows.
// The fence takes the project registry (sorted first) outermost; the nested hold is the session root.
let fenceDepth = 0;
const kindOf = (stack: string): string =>
  /participant-directory/.test(stack) ? (fenceDepth++ % 2 ? "presence-fence-session-root" : "presence-fence-project-root")
    : /\bupdate\b/.test(stack) ? "host-registry-update" : `host-other:${/\n\s+at \S+ \(([^)]*)\)/.exec(stack.split("\n").slice(2).join("\n"))?.[1] ?? "?"}`;
instrument(holds, kindOf);
// Every MeshStore.withTryLock in the host process (the presence fence's mesh step among them):
// wait before the operation, operation time, and operations that returned false (published nothing).
const { MeshStore } = await import("../src/mesh/store.js");
const fenceParts = { tryLockWaitMs: [] as number[], publishMs: [] as number[], tryLockFailures: 0, unchanged: 0 };
const withTryLock = MeshStore.prototype.withTryLock;
MeshStore.prototype.withTryLock = async function <T>(this: InstanceType<typeof MeshStore>, operation: () => Promise<T>, timeoutMs?: number) {
  const asked = performance.now();
  let started = 0;
  try {
    return await withTryLock.call(this, async () => {
      started = performance.now(); fenceParts.tryLockWaitMs.push(started - asked);
      try { const value = await operation(); if (value === false) fenceParts.unchanged++; return value; }
      finally { fenceParts.publishMs.push(performance.now() - started); }
    }, timeoutMs) as T;
  } catch (error) { if (!started) fenceParts.tryLockFailures++; throw error; }
} as typeof withTryLock;
const meshHold = Number(args.meshHold ?? 0), meshGap = Number(args.meshGap ?? 0);
const host = new ResidentHost(config);
// The fleet's liveness policy: participant records and host leases live in files (smarty-dev#2004).
if (args.files !== "false") {
  const { LIVENESS_POLICY_KEY } = await import("../src/topology/host-leases.js");
  await new MeshStore(config.meshRoot, 256 * 1024, 500, { stateBackend: backend } as never).put({ key: LIVENESS_POLICY_KEY,
    value: { version: 1, hostLeases: "files", participants: "files" }, identity: host.identity });
}
const burners = Array.from({ length: busy }, () => spawn(process.execPath, ["-e", "for(;;){}"], { stdio: "ignore" }));
let result: Record<string, unknown> | undefined;
try {
  await host.start();
  await new Promise(resolve => setTimeout(resolve, 200));
  await host.participants.refresh();
  const owned = host.actors.listOwned().map(actor => actor.id);
  if (owned.length !== OWNED) throw new Error(`host owns ${owned.length} actors, expected ${OWNED}`);
  holds.length = 0;
  fenceParts.tryLockWaitMs.length = 0; fenceParts.publishMs.length = 0; fenceParts.tryLockFailures = 0; fenceParts.unchanged = 0;
  const until = Date.now() + duration;
  if (args.sqliteBusy === "true") burners.push(spawn(process.execPath, [import.meta.filename ?? process.argv[1]!, "--role=sqlite-writer",
    `--mesh=${config.meshRoot}`, `--until=${until}`, `--low=${args.busyLow ?? 100}`, `--high=${args.busyHigh ?? 300}`, `--gap=${args.busyGap ?? 100}`],
  { stdio: ["ignore", "ignore", "inherit"] }));
  // Other hosts' mesh custody on a busy hub: a holder that takes the mesh lock for meshHold ms every meshGap ms.
  if (meshHold > 0) burners.push(spawn(process.execPath, [path.resolve("tests/fixtures/hold-mesh-lock.mjs"), config.meshRoot,
    String(meshHold), String(meshGap), String(duration)], { stdio: "ignore" }));
  const children = Array.from({ length: savers }, (_, index) => new Promise<{ holds: Hold[]; errors: number }>((resolve, reject) => {
    const child = spawn(process.execPath, [import.meta.filename ?? process.argv[1]!, "--role=saver", `--root=${config.actorRoot}`,
      `--index=${index}`, `--savers=${savers}`, `--until=${until}`], { stdio: ["ignore", "pipe", "inherit"] });
    let out = "";
    child.stdout.on("data", chunk => { out += chunk; });
    child.on("close", code => code === 0 ? resolve(JSON.parse(out)) : reject(new Error(`saver ${index} exited ${code}`)));
  }));
  // Real owned-actor mutations: a registry save plus a presence change refresh each.
  let mutations = 0, mutationErrors = 0;
  while (Date.now() < until) {
    const id = owned[mutations++ % owned.length]!;
    try { await host.actors.setActivationFilter(id, mutations % 2 ? [{ id: "probe", topic: ["probe.noise"], kind: ["skip"] }] : null); }
    catch { mutationErrors++; }
    await new Promise(resolve => setTimeout(resolve, pulse));
  }
  const others = await Promise.all(children);
  const all = [...holds, ...others.flatMap(other => other.holds)];
  const kinds = [...new Set(all.map(hold => hold.kind))].sort();
  const pct = (values: number[], p: number) => values.length ? Math.round(values[Math.min(values.length - 1, Math.floor(values.length * p))]!) : 0;
  const summary = (selected: Hold[]) => {
    const ms = selected.map(hold => hold.ms).sort((a, b) => a - b);
    return { holds: ms.length, wrote: selected.filter(hold => hold.wrote).length, totalMs: Math.round(ms.reduce((a, b) => a + b, 0)),
      p50: pct(ms, 0.5), p90: pct(ms, 0.9), p99: pct(ms, 0.99), max: pct(ms, 1) };
  };
  const part = (values: number[]) => { const ms = [...values].sort((a, b) => a - b); return { n: ms.length, p50: pct(ms, 0.5), p90: pct(ms, 0.9), p99: pct(ms, 0.99), max: pct(ms, 1) }; };
  result = { backend, files: args.files !== "false", savers, busy, durationMs: duration, pulseMs: pulse, meshHold, meshGap, mutations, mutationErrors,
    meshTryLockWaitMs: part(fenceParts.tryLockWaitMs), meshTryLockRunMs: part(fenceParts.publishMs), meshTryLockFailures: fenceParts.tryLockFailures, meshTryLockRunsPublishingNothing: fenceParts.unchanged,
    saverErrors: others.reduce((total, other) => total + other.errors, 0),
    all: summary(all), byKind: Object.fromEntries(kinds.map(kind => [kind, summary(all.filter(hold => hold.kind === kind))])) };
} finally {
  for (const burner of burners) burner.kill();
  await host.close();
  fs.rmSync(base, { recursive: true, force: true });
}
console.log(JSON.stringify(result, null, 1));
process.exit(0);
