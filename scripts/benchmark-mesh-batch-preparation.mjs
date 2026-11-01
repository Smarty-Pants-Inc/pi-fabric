#!/usr/bin/env node
// Source-only microbenchmark: no build or installed runtime, and no default mesh root.
// Run with TMPDIR on tmpfs; the explicit parent may be a small scratch ext/XFS directory.
import fs from "node:fs";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { createJiti } from "jiti";

const [parent, countText = "12"] = process.argv.slice(2);
const count = Number(countText);
if (!parent || !path.isAbsolute(parent) || !Number.isSafeInteger(count) || count < 1 || count > 100) {
  throw new Error("Usage: TMPDIR=<tmpfs> node scripts/benchmark-mesh-batch-preparation.mjs <absolute-scratch-parent> [1..100]");
}
// Loading source must not populate a shared dependency or TMPDIR cache.
const jiti = createJiti(import.meta.url, { fsCache: false });
const { isolateTestFleetEnvironment } = await jiti.import("./test-temp.ts");
isolateTestFleetEnvironment();
process.env.PI_FABRIC_LOCK_STATS = "1";
const { MeshStore } = await jiti.import("../src/mesh/store.ts");
const { MESH_ARCHIVE_CONFIG } = await jiti.import("../src/mesh/archive.ts");
const { lockStats } = await jiti.import("../src/mesh/mesh-lock.ts");
if (!lockStats) throw new Error("Source lock timing must be enabled");
const base = fs.mkdtempSync(path.join(parent, "mesh-batch-preparation-"));
const root = path.join(base, "common", "mesh");
const archive = path.join(base, "common", "archive");
const holds = [];
// Consume the source's actual custody-to-release measurement, not total publication time.
lockStats.acquired = (actualRoot, operation, _wait, hold) => {
  if (actualRoot === root && operation === "bridge") holds.push(hold);
};
const from = { id: "session:batch-benchmark", name: "batch-benchmark", kind: "main" };
const sync = fs.fsyncSync.bind(fs), datasync = fs.fdatasyncSync.bind(fs);
let store;
try {
  fs.mkdirSync(root, { recursive: true, mode: 0o700 });
  fs.mkdirSync(archive, { recursive: true, mode: 0o700 });
  fs.writeFileSync(path.join(root, MESH_ARCHIVE_CONFIG), JSON.stringify({ version: 1, dir: archive }));
  store = new MeshStore(root, 64 * 1024, 1000, { stateBackend: "file" });
  await store.publish({ topic: "benchmark.batch", from, text: "warm archive" });
  await store.publishBatch([{ topic: "benchmark.batch", from, dedupeKey: "warm", text: "warm receipts" }]);
  holds.length = 0;
  let heldDirectory = 0, heldFile = 0, heldData = 0, offLockDirectory = 0;
  fs.fsyncSync = fd => {
    const directory = fs.fstatSync(fd).isDirectory();
    const held = fs.existsSync(path.join(root, ".lock"));
    if (held) { if (directory) heldDirectory++; else heldFile++; }
    else if (directory) offLockDirectory++;
    sync(fd);
  };
  fs.fdatasyncSync = fd => { if (fs.existsSync(path.join(root, ".lock"))) heldData++; datasync(fd); };
  const wall = performance.now();
  for (let i = 0; i < count; i++) {
    // One keyed archived event per batch fixes equal work even on a disk exceeding the 50 ms budget.
    const events = await store.publishBatch([{ topic: "benchmark.batch", from, dedupeKey: `batch-${i}`, text: "x".repeat(1024) }]);
    if (events.length !== 1) throw new Error("Benchmark must commit exactly one event per batch");
  }
  const wallMs = performance.now() - wall;
  const sorted = [...holds].sort((a, b) => a - b);
  if (holds.length !== count) throw new Error(`Expected ${count} source hold samples, got ${holds.length}`);
  const quantile = q => sorted[Math.ceil(q * sorted.length) - 1];
  const fixtureBytes = directory => fs.readdirSync(directory, { withFileTypes: true }).reduce((bytes, entry) => {
    const file = path.join(directory, entry.name);
    return bytes + (entry.isDirectory() ? fixtureBytes(file) : fs.lstatSync(file).size);
  }, 0);
  console.log(JSON.stringify({ filesystem: fs.statfsSync(base).type.toString(16), batches: count, eventsPerBatch: 1,
    fixtureBytes: fixtureBytes(base),
    holdMs: { mean: holds.reduce((a, b) => a + b, 0) / count, p50: quantile(.5), p95: quantile(.95), max: quantile(1), samples: holds },
    wallMsPerBatch: wallMs / count, barriersPerBatch: { heldDirectory: heldDirectory / count, heldFile: heldFile / count,
      heldData: heldData / count, offLockDirectory: offLockDirectory / count } }));
} finally {
  fs.fsyncSync = sync; fs.fdatasyncSync = datasync;
  store?.closeState();
  // Only the fresh mkdtemp fixture owned by this invocation; its sibling queue is inside it too.
  fs.rmSync(base, { recursive: true, force: true });
}
