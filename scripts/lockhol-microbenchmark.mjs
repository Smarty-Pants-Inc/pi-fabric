#!/usr/bin/env node
// Bounded local MeshLock microbenchmark, not a production load/rollout claim.
// Run under smarty-reap. HEAD's lock-queue.ts is the baseline; every other source byte is shared.
// All bundles, raw samples and roots stay in private scratch (no cleanup/reaping of evidence).
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { build } from "esbuild";

process.env.PI_FABRIC_LOCK_STATS = "0";
const head = execFileSync("git", ["rev-parse", "HEAD"], { encoding: "utf8" }).trim();
const original = execFileSync("git", ["show", `${head}:src/mesh/lock-queue.ts`], { encoding: "utf8" });
const current = fs.readFileSync("src/mesh/lock-queue.ts", "utf8");
const directory = fs.mkdtempSync(path.join(os.tmpdir(), "lockhol-microbenchmark-"));
const modules = {};
for (const variant of ["baseline", "candidate"]) {
  const outfile = path.join(directory, `${variant}.mjs`);
  await build({ entryPoints: [path.resolve("src/mesh/mesh-lock.ts")], bundle: true, platform: "node", format: "esm", outfile,
    banner: { js: 'import { createRequire as __cr } from "node:module"; const require = __cr(import.meta.url);' },
    plugins: variant === "baseline" ? [{ name: "lockhol-head-queue", setup(builder) {
      builder.onLoad({ filter: /[/\\]mesh[/\\]lock-queue\.ts$/ }, () => ({ contents: original, loader: "ts", resolveDir: path.resolve("src/mesh") }));
    } }] : [],
  });
  modules[variant] = await import(pathToFileURL(outfile).href);
}
const quantiles = values => {
  const sorted = [...values].sort((a, b) => a - b);
  const at = q => sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * q))];
  return { count: values.length, p50: at(0.5), p95: at(0.95), max: sorted.at(-1) };
};
const run = async (variant, sample) => {
  const { MeshLock } = modules[variant];
  const parent = path.join(directory, `${sample}-${variant}`);
  const root = path.join(parent, "mesh");
  fs.mkdirSync(root, { recursive: true, mode: 0o700 });
  const lock = new MeshLock(root, { lockTimeoutMs: 5000 }, () => {});
  const marker = path.join(root, "inside");
  let acquisitions = 0;
  const section = () => {
    // These assertions execute INSIDE the actual MeshLock critical section, after canonical mkdir and pinning.
    assert.equal(Number(fs.readFileSync(path.join(root, ".lock", "owner"), "utf8").split("\n")[1]), process.pid);
    fs.writeFileSync(marker, "inside", { flag: "wx", mode: 0o600 });
    fs.unlinkSync(marker);
    acquisitions += 1;
  };
  for (let index = 0; index < 30; index += 1) await lock.withLock(section);
  const cpu = process.cpuUsage();
  const start = performance.now();
  const uncontended = [];
  for (let index = 0; index < 500; index += 1) {
    const before = performance.now();
    await lock.withLock(section);
    uncontended.push(performance.now() - before);
  }
  const contended = [];
  for (let wave = 0; wave < 15; wave += 1) {
    let release;
    let entered;
    const inside = new Promise(resolve => { entered = resolve; });
    const held = lock.withLockAcrossAwait(() => new Promise(resolve => { release = resolve; entered(); }));
    await inside;
    const order = [];
    const waiting = Array.from({ length: 12 }, (_, index) => {
      const before = performance.now();
      return lock.withLock(() => {
        section();
        order.push(index);
        contended.push(performance.now() - before);
      });
    });
    release();
    await held;
    await Promise.all(waiting);
    assert.deepEqual(order, Array.from({ length: 12 }, (_, index) => index));
  }
  const elapsedMs = performance.now() - start;
  const used = process.cpuUsage(cpu);
  assert.equal(acquisitions, 710); // 30 warmups + 500 uncontended + 15 x 12 contended
  assert.equal(fs.existsSync(path.join(root, ".lock")), false);
  assert.equal(fs.existsSync(marker), false);
  return { variant, sample, elapsedMs, cpuMs: (used.user + used.system) / 1000,
    uncontendedMs: quantiles(uncontended), healthyQueueWaitMs: quantiles(contended),
    acquisitions: acquisitions - 30, fifoWaves: 15, overlaps: 0 };
};
const samples = [];
for (const [index, variant] of ["baseline", "candidate", "candidate", "baseline"].entries()) samples.push(await run(variant, index));
const report = { head, directory, platform: process.platform, node: process.version,
  sourceSha256: { baseline: createHash("sha256").update(original).digest("hex"), candidate: createHash("sha256").update(current).digest("hex") },
  scope: "Same-process protocol-1 real canonical acquisition/release, healthy FIFO waves; no filesystem pressure, stalled-pair or production-latency estimate.", samples };
fs.writeFileSync(path.join(directory, "report.json"), JSON.stringify(report, null, 2) + "\n", { mode: 0o600 });
console.log(JSON.stringify(report, null, 2));
