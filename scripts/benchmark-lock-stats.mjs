#!/usr/bin/env node
// Mesh-lock stats overhead (smarty-dev#6477 L8). Runs against dist/ (bun run build first).
//
// 1. A/B: uncontended mesh-lock cycles (exclusive, and put on a small state) in fresh,
//    interleaved processes with PI_FABRIC_LOCK_STATS=0 versus the default (on).
// 2. The hook's direct cost in an "on" process: one acquired() record plus the three
//    performance.now() stamps, against the cheapest uncontended lock cycle measured in (1).
// 3. A small multi-process demo on one root, printed through fabric-mesh-lock-stats.
//
//   node scripts/benchmark-lock-stats.mjs [--rounds=9] [--ops=3000] [--puts=400] [--demo=8]
import { fork } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const args = Object.fromEntries(process.argv.slice(2).map(arg => arg.replace(/^--/, "").split("=")));
const identity = { id: `bench-${process.pid}`, name: "bench", kind: "main" };
const loadStore = async () => (await import(pathToFileURL(path.join(repo, "dist/mesh.js")).href)).MeshStore;

if (args.worker === "ab") {
  const MeshStore = await loadStore();
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "lock-stats-bench-"));
  try {
    const store = new MeshStore(root, 65536, 100);
    const ops = Number(args.ops), puts = Number(args.puts);
    for (let i = 0; i < 300; i++) await store.exclusive(() => undefined);
    let started = performance.now();
    for (let i = 0; i < ops; i++) await store.exclusive(() => undefined);
    const exclusiveUs = (performance.now() - started) * 1000 / ops;
    for (let i = 0; i < 50; i++) await store.put({ key: `bench/${i % 20}`, value: { i }, identity });
    started = performance.now();
    for (let i = 0; i < puts; i++) await store.put({ key: `bench/${i % 20}`, value: { i }, identity });
    const putUs = (performance.now() - started) * 1000 / puts;
    const report = { exclusiveUs, putUs };
    const registry = globalThis[Symbol.for("pi-fabric.mesh.lock-stats")];
    if (registry?.stats) {
      // A root that does not exist: the exit flush fails closed (ENOENT) and writes nothing.
      const fake = path.join(root, "missing", "mesh");
      const n = 2_000_000;
      const classes = ["put/delete", "writeBatch", "publish", "custody"];
      for (let i = 0; i < 100_000; i++) registry.stats.acquired(fake, classes[i & 3], i % 7, (i % 13) * 0.37);
      started = performance.now();
      for (let i = 0; i < n; i++) registry.stats.acquired(fake, classes[i & 3], i % 7, (i % 13) * 0.37);
      report.recordNs = (performance.now() - started) * 1e6 / n;
      let sink = 0;
      started = performance.now();
      for (let i = 0; i < n; i++) sink += performance.now();
      report.nowNs = (performance.now() - started) * 1e6 / n;
      if (sink === 42) console.log("");
    }
    process.send(report);
  } finally { fs.rmSync(root, { recursive: true, force: true }); }
  process.disconnect();
} else if (args.worker === "demo") {
  const MeshStore = await loadStore();
  const store = new MeshStore(args.root, 65536, 100);
  const me = { id: `demo-${process.pid}`, name: "demo", kind: "main" };
  for (let i = 0; i < 25; i++) {
    await store.put({ key: `presence/${process.pid}`, value: { i }, identity: me });
    await store.writeBatch({ identity: me, ops: [{ kind: "put", key: `topology/participants/${process.pid}`, value: { i } }] });
    await store.publish({ topic: "demo.lock", from: me, text: String(i) });
    await store.confirmWritable();
    await store.exclusive(() => undefined);
  }
  process.send("done");
  process.disconnect();
} else {
  const rounds = Number(args.rounds ?? 9), ops = Number(args.ops ?? 3000), puts = Number(args.puts ?? 400);
  const run = (worker, env, extra = []) => new Promise((resolve, reject) => {
    const child = fork(fileURLToPath(import.meta.url), [`--worker=${worker}`, `--ops=${ops}`, `--puts=${puts}`, ...extra],
      { env: { ...process.env, ...env }, stdio: ["ignore", "inherit", "inherit", "ipc"] });
    let report;
    child.on("message", value => { report = value; });
    child.once("error", reject);
    child.once("exit", code => code === 0 ? resolve(report) : reject(new Error(`${worker} exited ${code}`)));
  });
  const median = values => { const sorted = [...values].sort((a, b) => a - b); return sorted[Math.floor(sorted.length / 2)]; };
  const off = [], on = [];
  for (let round = 0; round < rounds; round++) {
    // Alternate the order so drift on a loaded host hits both arms.
    const pair = round % 2 ? [["1", on], ["0", off]] : [["0", off], ["1", on]];
    for (const [setting, into] of pair) into.push(await run("ab", { PI_FABRIC_LOCK_STATS: setting }));
  }
  const offExclusive = median(off.map(r => r.exclusiveUs)), onExclusive = median(on.map(r => r.exclusiveUs));
  const offPut = median(off.map(r => r.putUs)), onPut = median(on.map(r => r.putUs));
  const recordNs = median(on.map(r => r.recordNs)), nowNs = median(on.map(r => r.nowNs));
  const hookNs = recordNs + 3 * nowNs;
  const round2 = value => Math.round(value * 100) / 100;
  // Adjacent pairs share the host's load at that moment; their ratio is the robust A/B signal.
  const paired = key => off.map((r, index) => (on[index][key] / r[key] - 1) * 100);
  const spread = values => ({ min: round2(Math.min(...values)), median: round2(median(values)), max: round2(Math.max(...values)) });
  const result = {
    host: os.hostname(), node: process.version, cpus: os.cpus().length, load1: round2(os.loadavg()[0]), rounds, ops, puts,
    exclusiveUs: { off: round2(offExclusive), on: round2(onExclusive), deltaPct: round2((onExclusive / offExclusive - 1) * 100),
      offSpread: spread(off.map(r => r.exclusiveUs)), onSpread: spread(on.map(r => r.exclusiveUs)), pairedDeltaPct: spread(paired("exclusiveUs")) },
    putUs: { off: round2(offPut), on: round2(onPut), deltaPct: round2((onPut / offPut - 1) * 100),
      offSpread: spread(off.map(r => r.putUs)), onSpread: spread(on.map(r => r.putUs)), pairedDeltaPct: spread(paired("putUs")) },
    hook: { recordNs: round2(recordNs), nowNs: round2(nowNs), totalNs: round2(hookNs),
      pctOfUncontendedExclusive: round2(hookNs / (offExclusive * 1000) * 100), pctOfUncontendedPut: round2(hookNs / (offPut * 1000) * 100) },
  };
  console.log(JSON.stringify(result, null, 2));
  const demo = Number(args.demo ?? 8);
  if (demo > 0) {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "lock-stats-demo-"));
    try {
      await Promise.all(Array.from({ length: demo }, () => run("demo", { PI_FABRIC_LOCK_STATS: "1" }, [`--root=${root}`])));
      const { main } = await import(pathToFileURL(path.join(repo, "dist/mesh-lock-stats-cli.js")).href);
      // Show the current minute as if it were complete.
      console.log(`\n$ fabric-mesh-lock-stats --mesh <demo root> --minutes 2   # ${demo} processes x 25 x (put, writeBatch, publish, confirm, exclusive)`);
      main(["--mesh", root, "--minutes", "2", "--top", "3"], { now: Date.now() + 60_000 });
    } finally { fs.rmSync(root, { recursive: true, force: true }); }
  }
}
