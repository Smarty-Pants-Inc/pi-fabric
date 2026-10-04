import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { writeJsonAtomic } from "../src/core/atomic-write.js";

const args = process.argv.slice(2);
const index = args.indexOf("--module");
const modulePath = index < 0 ? path.resolve("src/topology/host-leases.ts") : args[index + 1]!;
const { writeHostLease } = await import(pathToFileURL(modulePath).href);
const durable = args.includes("--durable-reference");
const root = fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "fabric-lease-bench-"));
const durationMs = 60_000;
const intervalMs = 15_000;
const count = 10;
const originalSync = fs.fsyncSync;
let fileSyncs = 0, directorySyncs = 0, writes = 0;
try {
  // Exclude initial acquisition/mkdir from the renewal measurement.
  const lease = (index: number, now: number) => ({ id: `bench:${index}`, rootId: "bench", identityId: "bench", updatedAt: now, expiresAt: now + 45_000 });
  for (let i = 0; i < count; i++) writeHostLease(root, lease(i, Date.now()));
  fs.fsyncSync = ((fd: number) => {
    if (fs.fstatSync(fd).isDirectory()) directorySyncs++; else fileSyncs++;
    originalSync(fd);
  }) as typeof fs.fsyncSync;
  const began = performance.now();
  for (let tick = 0; tick < durationMs / intervalMs; tick++) {
    const wait = began + tick * intervalMs - performance.now();
    if (wait > 0) await new Promise(resolve => setTimeout(resolve, wait));
    for (let i = 0; i < count; i++) {
      if (durable) writeJsonAtomic(path.join(root, "host-leases", `reference-${i}.json`), { format: 1, ...lease(i, Date.now()) }, { durable: true });
      else writeHostLease(root, lease(i, Date.now()));
      writes++;
    }
  }
  const remaining = began + durationMs - performance.now();
  if (remaining > 0) await new Promise(resolve => setTimeout(resolve, remaining));
  console.log(JSON.stringify({ mode: durable ? "durable-reference-not-baseline" : "actual-module", modulePath, leases: count, intervalMs, durationMs,
    elapsedMs: Math.round(performance.now() - began), writes, fileSyncs, directorySyncs, totalSyncs: fileSyncs + directorySyncs,
    fsyncsPerSecond: (fileSyncs + directorySyncs) / (durationMs / 1_000) }, null, 2));
} finally {
  fs.fsyncSync = originalSync;
  fs.rmSync(root, { recursive: true, force: true });
}
