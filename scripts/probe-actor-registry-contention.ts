#!/usr/bin/env bun
// smarty-dev#6829: one actor root shaped like the live review-fleet-lead root (89 actors,
// ~22 KB instructions each, 19 running), written by several processes at once.
// Each writer owns a slice of the actors, flips status/updatedAt of its running ones,
// and saves through ActorRegistryStore.update with the manager's validate() clauses.
// Run (isolated, two CPUs, CPU-saturated like a load-50 host):
//   taskset -c 0,1 bun scripts/probe-actor-registry-contention.ts --writers=6 --busy=4 --duration=20000
import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { ActorRegistryStore, ActorRegistryUpdateVetoedError } from "../src/actors/registry-store.js";

const args = Object.fromEntries(process.argv.slice(2).map(arg => arg.replace(/^--/, "").split("=")));
const ACTORS = Number(args.actors ?? 89), RUNNING = Number(args.running ?? 19);
const INSTRUCTIONS = Number(args.instructions ?? 22_000);
// Loaded actors keep their last-100 ring in memory, as the manager does after first use.
const MESSAGES = Number(args.messages ?? 0), MESSAGE_BYTES = Number(args.messageBytes ?? 2_000);
const ring = (id: string) => Array.from({ length: MESSAGES }, (_, j) => ({ id: `m-${id}-${j}`, actorId: id, direction: j % 2 ? "out" : "in",
  source: "direct", createdAt: 1 + j, text: "x".repeat(MESSAGE_BYTES) }));

if (args.role === "writer") {
  const root = args.root!, index = Number(args.index), writers = Number(args.writers), until = Number(args.until);
  const store = new ActorRegistryStore(root);
  const ids = Array.from({ length: ACTORS }, (_, i) => (i + 1).toString(16).padStart(32, "0"));
  const owned = ids.filter((_, i) => i % writers === index);
  const actors = new Map<string, Record<string, unknown>>();
  for (const row of store.snapshot().actors) if (owned.includes(row.id)) {
    const { messageHistory: _history, ...loaded } = row;
    actors.set(row.id, { ...loaded, messages: ring(row.id) });
  }
  const running = [...actors.values()].filter(row => row.status === "running" || row.status === "idle-running");
  // A running actor changes status or updatedAt every ~1 s (turn start/finish, tool activity).
  const tick = setInterval(() => {
    const row = running[Math.floor(Math.random() * running.length)];
    if (row) { row.updatedAt = Date.now(); row.status = row.status === "running" ? "idle-running" : "running"; }
  }, Math.max(20, 1000 / Math.max(1, running.length)));
  const stats = { saves: 0, committed: 0, vetoedErrors: 0, lockTimeouts: 0, otherErrors: 0,
    attempts: 0, failGeneration: 0, failStates: 0, holdMs: [] as number[], waitMs: [] as number[], windowMs: [] as number[], saveMs: [] as number[] };
  const withLock = store.withLock.bind(store);
  store.withLock = (async (operation: () => unknown, timeoutMs?: number) => {
    const asked = performance.now();
    let acquired = 0;
    try {
      return await withLock(() => { acquired = performance.now(); stats.waitMs.push(acquired - asked); return operation(); }, timeoutMs);
    } finally { if (acquired) stats.holdMs.push(performance.now() - acquired); }
  }) as typeof store.withLock;
  const prepare = store.prepare.bind(store);
  let selectedAt = 0;
  store.prepare = ((rows, options, snapshot) => {
    const prepared = prepare(rows, options, snapshot);
    const valid = prepared.valid, commit = prepared.commit;
    return { ...prepared, valid: () => { stats.attempts++; const ok = valid(); if (!ok) stats.failGeneration++; return ok; },
      commit: () => { commit(); stats.windowMs.push(performance.now() - selectedAt); } };
  }) as typeof store.prepare;
  while (Date.now() < until) {
    const started = performance.now();
    stats.saves++;
    try {
      await store.update(current => {
        selectedAt = performance.now();
        const rows = [...actors.values()].map(row => ({ ...row }));
        const states = rows.map(row => ({ row: actors.get(row.id as string)!, updatedAt: row.updatedAt, status: row.status }));
        return { actors: [...current.filter(row => !actors.has(row.id)), ...rows],
          validate: () => {
            const ok = states.every(({ row, updatedAt, status }) => row.updatedAt === updatedAt && row.status === status);
            if (!ok) stats.failStates++;
            return ok;
          }, value: true };
      });
      stats.committed++;
    } catch (error) {
      if (error instanceof ActorRegistryUpdateVetoedError) stats.vetoedErrors++;
      else if (/registry lock/.test(String(error))) stats.lockTimeouts++;
      else { stats.otherErrors++; console.error(error); }
    }
    stats.saveMs.push(performance.now() - started);
    await new Promise(resolve => setTimeout(resolve, 200 + Math.random() * 300));
  }
  clearInterval(tick);
  process.stdout.write(JSON.stringify(stats));
  process.exit(0);
}

const writers = Number(args.writers ?? 6), busy = Number(args.busy ?? 4), duration = Number(args.duration ?? 20_000);
const root = fs.mkdtempSync(path.join(args.tmp ?? os.tmpdir(), "registry-contention-"));
const now = Date.now();
// Distinct persona texts per actor (the live fleet had ~2.3 actors per distinct text).
const persona = (i: number) => `persona ${i % 40}\n` + "Review the change for correctness. ".repeat(Math.ceil(INSTRUCTIONS / 35)).slice(0, INSTRUCTIONS);
fs.writeFileSync(path.join(root, "actors.json"), JSON.stringify({ format: 1, actors: Array.from({ length: ACTORS }, (_, i) => ({
  id: (i + 1).toString(16).padStart(32, "0"), name: `review-${i}`, instructions: persona(i), createdAt: now, updatedAt: now,
  rootId: "session:fixture", residency: "durable", runner: "pi", status: i < RUNNING ? "running" : "idle", scope: "project",
  events: [], topics: [], messages: ring((i + 1).toString(16).padStart(32, "0")),
})) }));
// Live registries are already migrated: histories archived, `messages: []` stubs inline.
await new ActorRegistryStore(root).update(current => ({ actors: current.map(row => ({ ...row, updatedAt: now + 1 })), value: true }));
const bytes = fs.statSync(path.join(root, "actors.json")).size;
const burners = Array.from({ length: busy }, () => spawn(process.execPath, ["-e", "for(;;){}"], { stdio: "ignore" }));
const until = Date.now() + duration;
const results = await Promise.all(Array.from({ length: writers }, (_, index) => new Promise<Record<string, number | number[]>>((resolve, reject) => {
  const child = spawn(process.execPath, [import.meta.filename ?? process.argv[1]!, "--role=writer", `--root=${root}`, `--index=${index}`,
    `--writers=${writers}`, `--until=${until}`, `--actors=${ACTORS}`, `--running=${RUNNING}`, `--instructions=${INSTRUCTIONS}`,
    `--messages=${MESSAGES}`, `--messageBytes=${MESSAGE_BYTES}`],
  { stdio: ["ignore", "pipe", "inherit"] });
  let out = "";
  child.stdout.on("data", chunk => { out += chunk; });
  child.on("close", code => code === 0 ? resolve(JSON.parse(out)) : reject(new Error(`writer ${index} exited ${code}`)));
})));
for (const burner of burners) burner.kill();
const sum = (key: string) => results.reduce((total, row) => total + (row[key] as number), 0);
const all = (key: string) => results.flatMap(row => row[key] as number[]).sort((a, b) => a - b);
const pct = (values: number[], p: number) => values.length ? Math.round(values[Math.min(values.length - 1, Math.floor(values.length * p))]!) : 0;
const dist = (key: string) => { const values = all(key); return { n: values.length, p50: pct(values, 0.5), p90: pct(values, 0.9), p99: pct(values, 0.99), max: pct(values, 1) }; };
console.log(JSON.stringify({ actors: ACTORS, running: RUNNING, registryBytes: bytes, finalRegistryBytes: fs.statSync(path.join(root, "actors.json")).size,
  writers, busy, durationMs: duration,
  saves: sum("saves"), committed: sum("committed"), vetoedErrors: sum("vetoedErrors"), lockTimeouts: sum("lockTimeouts"), otherErrors: sum("otherErrors"),
  validations: sum("attempts"), failGeneration: sum("failGeneration"), failStates: sum("failStates"),
  lockWaitMs: dist("waitMs"), lockHoldMs: dist("holdMs"), selectToCommitMs: dist("windowMs"), saveMs: dist("saveMs") }, null, 1));
fs.rmSync(root, { recursive: true, force: true });
