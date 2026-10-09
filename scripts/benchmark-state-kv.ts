import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { openAsyncMeshStateStore, type AsyncMeshStateStore } from "../src/mesh/state-async.ts";

const output = process.env.FABRIC_NATS_EVIDENCE_DIR ?? process.env.TASK_OUT;
if (!output) throw new Error("Set TASK_OUT or FABRIC_NATS_EVIDENCE_DIR");
const iterations = Number(process.env.FABRIC_STATE_BENCH_ITERATIONS ?? 1_000);
const warmup = 100;
const seedKeys = 168;
const valueBytes = Number(process.env.FABRIC_STATE_BENCH_VALUE_BYTES ?? 1_024);
if (!Number.isSafeInteger(iterations) || iterations < 100) throw new Error("At least 100 iterations required for p99");
if (!Number.isSafeInteger(valueBytes) || valueBytes < 1 || valueBytes > 100 * 1024) throw new Error("value bytes must be 1..102400");
const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-state-latency-"));
const stores: AsyncMeshStateStore[] = [];
const identity = { id: "benchmark", name: "state latency", kind: "agent" as const };
const value = { text: "x".repeat(valueBytes) };
const key = "bench/target";
const servers = process.env.FABRIC_NATS_TEST_SERVERS;
const stats = (samples: number[]) => {
  const sorted = samples.slice().sort((a, b) => a - b);
  const percentile = (p: number) => sorted[Math.max(0, Math.ceil(p * sorted.length) - 1)]!;
  return { samples: sorted.length, unit: "ms", p50: percentile(0.5), p99: percentile(0.99), min: sorted[0], max: sorted.at(-1) };
};
const samples: Record<string, Record<string, number[]>> = {};
try {
  stores.push(await openAsyncMeshStateStore(path.join(root, "file"), { backend: "file" }));
  if (servers) stores.push(await openAsyncMeshStateStore(path.join(root, "nats"),
    { backend: "nats-kv", nats: { servers: servers.split(","), experimentalNatsKv: true } }));
  for (const store of stores) {
    samples[store.kind] = { get: [], put: [], cas: [] };
    for (let n = 0; n < seedKeys; n++) await store.put({ key: `topology/participants/${n.toString(16).padStart(64, "0")}`, value: { text: "x".repeat(1_024) }, identity });
    await store.put({ key: "actors/root/large", value: { text: "x".repeat(100 * 1024) }, identity });
    await store.put({ key, value, identity });
  }
  for (const operation of ["get", "put", "cas"] as const) {
    for (let n = -warmup; n < iterations; n++) {
      // Alternate backend order every iteration to avoid always measuring one under warmer load.
      for (const store of n % 2 ? stores.slice().reverse() : stores) {
        // CAS timing is the CAS publish only: its necessary pre-read is measured separately by GET.
        const version = operation === "cas" ? (await store.get(key))!.version : undefined;
        const begin = performance.now();
        if (operation === "get") await store.get(key);
        else await store.put({ key, value, identity, ...(version !== undefined ? { ifVersion: version } : {}) });
        const elapsed = performance.now() - begin;
        if (n >= 0) samples[store.kind]![operation]!.push(elapsed);
      }
    }
  }
  const numbers = Object.fromEntries(stores.map(store => [store.kind, Object.fromEntries(Object.entries(samples[store.kind]!).map(([operation, values]) => [operation, stats(values)]))]));
  const result = { status: servers ? "MEASURED" : "PARTIAL_NATS_BLOCKED", hostname: os.hostname(), runtime: process.version,
    iterations, warmup, seededKeys: seedKeys + 2, mutationPayloadTextBytes: valueBytes,
    method: "sequential client calls, alternating backends/order; GET uses each backend's normal authority read path; CAS pre-read excluded; default file atomic rename (no fsync claim)",
    nats: servers ? { replicas: 3, sync_interval: "always (runner config; operator must attest when run standalone)" }
      : { status: "BLOCKED", reason: "No authorized official nats-server 2.14.7+ binary plus SHA256SUMS; no NATS latency fabricated" },
    numbers, rawSamplesMs: samples };
  fs.writeFileSync(path.join(output, `latency-${valueBytes}.json`), JSON.stringify(result, null, 2) + "\n");
  console.log(JSON.stringify({ ...result, rawSamplesMs: undefined }, null, 2));
} finally {
  await Promise.allSettled(stores.map(store => store.close()));
  fs.rmSync(root, { recursive: true, force: true });
}
