#!/usr/bin/env node
// Real production writer entry point: N=10k awaited, one-row transactions per sample.
// Only the recorder threshold differs; both stores use the same WAL file and defaults.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createJiti } from "jiti";
const jiti = createJiti(import.meta.url);
const { SqliteStateStore } = await jiti.import("../src/mesh/state-sqlite.ts");
const { sqliteHoldsPath } = await jiti.import("../src/mesh/sqlite-holds.ts");
const n = 10_000, pairs = Number(process.env.PAIRS ?? 15);
if (!Number.isSafeInteger(pairs) || pairs < 1) throw new Error("PAIRS must be a positive integer");
const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-hold-store-bench-"));
const identity = { id: "benchmark", name: "benchmark", kind: "agent" };
const saved = process.env.PI_FABRIC_SQLITE_HOLD_MS;
let off, on;
try {
  process.env.PI_FABRIC_SQLITE_HOLD_MS = "0";
  off = await SqliteStateStore.open(root, 64 * 1024, 1000, { initialize: "detached" });
  process.env.PI_FABRIC_SQLITE_HOLD_MS = "2000";
  on = await SqliteStateStore.open(root, 64 * 1024, 1000, { initialize: "detached" });
  let value = 0;
  async function run(store, count) {
    const cpu = process.cpuUsage();
    const started = performance.now();
    for (let i = 0; i < count; i++) await store.put({ key: "bench", value: ++value, identity });
    const wallMs = performance.now() - started;
    const usage = process.cpuUsage(cpu);
    return { wallMs, cpuMs: (usage.user + usage.system) / 1000 };
  }
  for (let i = 0; i < 3; i++) { await run(off, n); await run(on, n); }
  const samples = [];
  for (let i = 0; i < pairs; i++) {
    const baseline = { wallMs: 0, cpuMs: 0 }, recorded = { wallMs: 0, cpuMs: 0 };
    // Interleave 100-transaction blocks to reduce drift; each sample still contains exactly
    // N=10k transactions per variant. Reverse AB/BA order each block and each sample.
    for (let block = 0; block < n / 100; block++) {
      let a, b;
      if ((i + block) % 2 === 0) { a = await run(off, 100); b = await run(on, 100); }
      else { b = await run(on, 100); a = await run(off, 100); }
      baseline.wallMs += a.wallMs; baseline.cpuMs += a.cpuMs;
      recorded.wallMs += b.wallMs; recorded.cpuMs += b.cpuMs;
    }
    samples.push({ offMs: baseline.wallMs, onMs: recorded.wallMs, offCpuMs: baseline.cpuMs, onCpuMs: recorded.cpuMs,
      differencePct: (recorded.wallMs / baseline.wallMs - 1) * 100 });
  }
  const median = values => values.toSorted((a, b) => a - b)[Math.floor(values.length / 2)];
  const offMs = median(samples.map(s => s.offMs)), onMs = median(samples.map(s => s.onMs));
  if (on.get("bench", { fresh: true }).value !== value) throw new Error("recorder changed the final value");
  if (fs.existsSync(sqliteHoldsPath(root))) throw new Error("short holds emitted metrics");
  console.log(JSON.stringify({ runtime: process.version, entry: "SqliteStateStore.put", n, pairs, synchronous: "NORMAL", journalMode: "WAL",
    interleavedBlockSize: 100, offMedianMs: offMs, onMedianMs: onMs, differencePct: (onMs / offMs - 1) * 100,
    medianPairedDifferencePct: median(samples.map(s => s.differencePct)),
    offMedianCpuMs: median(samples.map(s => s.offCpuMs)), onMedianCpuMs: median(samples.map(s => s.onCpuMs)),
    samples, finalValue: value, metricsLines: 0 }, null, 2));
} finally {
  off?.close(); on?.close();
  if (saved === undefined) delete process.env.PI_FABRIC_SQLITE_HOLD_MS; else process.env.PI_FABRIC_SQLITE_HOLD_MS = saved;
  fs.rmSync(root, { recursive: true, force: true });
}
