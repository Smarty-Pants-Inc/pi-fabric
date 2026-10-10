#!/usr/bin/env node
// smarty-dev#8305: actual MeshStore.publish wall time and the lock-stats hook's holdMs.
// Build first. Keep scratch in a caller-owned TMPDIR; leave it for inspection/owner cleanup.
// node scripts/benchmark-archive-group-commit.mjs [--module /abs/store.mjs] [--count 40]
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const args = process.argv.slice(2);
const option = (name, fallback) => {
  const at = args.indexOf(name);
  if (at < 0) return fallback;
  if (!args[at + 1] || args[at + 1].startsWith("--")) throw new Error(`Missing ${name}`);
  return args[at + 1];
};
const count = Number(option("--count", "40"));
if (!Number.isSafeInteger(count) || count < 1) throw new Error("--count must be a positive integer");
const modulePath = path.resolve(option("--module", fileURLToPath(new URL("../dist/mesh.js", import.meta.url))));
process.env.PI_FABRIC_LOCK_STATS = "1";
const { MeshStore } = await import(pathToFileURL(modulePath).href);
const registry = globalThis[Symbol.for("pi-fabric.mesh.lock-stats")];
if (!registry?.stats) throw new Error("Mesh lock-stats recorder did not load");
const base = fs.mkdtempSync(path.join(os.tmpdir(), "archive-group-proof-"));
const root = path.join(base, "mesh"), archive = path.join(base, "archive");
fs.mkdirSync(root, { mode: 0o700 });
fs.mkdirSync(archive, { mode: 0o700 });
fs.writeFileSync(path.join(root, "event-archive.json"), JSON.stringify({ version: 1, dir: archive }));
const store = new MeshStore(root, 65536, 1000);
const from = { id: `proof:${process.pid}`, name: "archive-group-proof", kind: "main" };
// One first-boot setup append, excluded identically in both arms.
await store.publish({ topic: "proof.archive", from, text: "warmup" });
const holds = [], waits = [], walls = [];
const syncs = { held: { fsync: 0, fdatasync: 0 }, released: { fsync: 0, fdatasync: 0 } };
const originalSyncs = new Map();
for (const [method, kind] of [["fsyncSync", "fsync"], ["fdatasyncSync", "fdatasync"]]) {
  const original = fs[method];
  originalSyncs.set(method, original);
  fs[method] = fd => {
    syncs[fs.existsSync(path.join(root, ".lock")) ? "held" : "released"][kind]++;
    return original(fd);
  };
}
const acquired = registry.stats.acquired;
registry.stats.acquired = function (recordedRoot, lockClass, waitMs, holdMs) {
  if (recordedRoot === root && lockClass === "publish") { holds.push(holdMs); waits.push(waitMs); }
  return acquired.call(this, recordedRoot, lockClass, waitMs, holdMs);
};
try {
  for (let i = 0; i < count; i++) {
    const started = performance.now();
    await store.publish({ topic: "proof.archive", from, text: `publish ${i}` });
    walls.push(performance.now() - started);
  }
} finally {
  registry.stats.acquired = acquired;
  for (const [method, original] of originalSyncs) fs[method] = original;
}
registry.flush();
const percentile = (values, fraction) => {
  const sorted = [...values].sort((a, b) => a - b);
  return Number(sorted[Math.ceil(sorted.length * fraction) - 1].toFixed(3));
};
const distribution = values => ({ p50: percentile(values, 0.5), p90: percentile(values, 0.9), max: percentile(values, 1) });
const liveLines = fs.readFileSync(path.join(root, "events.jsonl"), "utf8").trimEnd().split("\n");
const archiveLines = [];
const visit = directory => {
  for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
    const file = path.join(directory, entry.name);
    if (entry.isDirectory() && /^\d+$/.test(entry.name)) visit(file);
    else if (entry.isFile() && entry.name.endsWith(".jsonl")) archiveLines.push(...fs.readFileSync(file, "utf8").trimEnd().split("\n"));
  }
};
visit(archive);
archiveLines.sort((a, b) => JSON.parse(a).sequence - JSON.parse(b).sequence);
const head = JSON.parse(fs.readFileSync(path.join(archive, "HEAD.json"), "utf8"));
const last = JSON.parse(liveLines.at(-1));
const consistent = archiveLines.length === liveLines.length && archiveLines.every((line, i) => line === liveLines[i]) &&
  head.sequence === last.sequence && head.id === last.id;
console.log(JSON.stringify({ host: os.hostname(), node: process.version, module: modulePath, scratch: base,
  count, publishWallMs: distribution(walls), lockStatsHoldMs: distribution(holds), lockStatsWaitMs: distribution(waits),
  acquired: holds.length, holdsPerPublish: holds.length / count, syncs,
  heldSyncsPerPublish: (syncs.held.fsync + syncs.held.fdatasync) / count,
  archiveHead: head.sequence, liveSequence: last.sequence, events: liveLines.length, consistent,
  samples: { publishWallMs: walls, lockStatsHoldMs: holds } }, null, 2));
if (!consistent || holds.length < count) throw new Error("Archive/live evidence or lock-stats samples do not cover every publish");
