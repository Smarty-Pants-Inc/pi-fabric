import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { openFileEventLog, openJetStreamEventLog, type MeshEventLogBackend } from "../src/mesh/event-backend.js";
import { startNatsCluster } from "../tests/helpers/nats-cluster.js";

const out = process.env.TASK_OUT;
if (!out) throw new Error("Set TASK_OUT for kept benchmark evidence");
const samples = 1000;
const backlog = 10_000;
const from = { id: "benchmark:event-log", name: "benchmark", kind: "main" as const };
const rows: unknown[] = [];
const quantile = (values: number[], q: number) => values[Math.min(values.length - 1, Math.ceil(values.length * q) - 1)]!;

async function measure(name: string, log: MeshEventLogBackend, durable: boolean) {
  try {
    for (let n = 0; n < 100; n++) await log.publish({ topic: "bench.warmup", from, data: n, durable });
    const times = [];
    for (let n = 0; n < samples; n++) {
      const start = performance.now();
      await log.publish({ topic: "bench.latency", from, data: { n, payload: "x".repeat(256) }, durable });
      times.push(performance.now() - start);
    }
    const after = await log.latestSequence();
    for (let offset = 0; offset < backlog;) {
      const size = Math.min(256, backlog - offset);
      const batch = await log.publishBatch(Array.from({ length: size }, (_, n) => ({
        topic: "bench.throughput", from, data: { n: offset + n, payload: "x".repeat(256) }, durable,
      })));
      assert(batch.length > 0);
      offset += batch.length;
    }
    const reads = [];
    for (let run = 0; run < 3; run++) {
      const start = performance.now();
      const events = await log.read({ after, limit: backlog });
      const elapsedMs = performance.now() - start;
      assert.equal(events.length, backlog);
      assert.equal((events[0]!.data as { n: number }).n, 0);
      assert.equal((events.at(-1)!.data as { n: number }).n, backlog - 1);
      assert.equal(new Set(events.map(event => event.id)).size, backlog);
      reads.push({ elapsedMs, eventsPerSecond: backlog / (elapsedMs / 1000) });
    }
    times.sort((a, b) => a - b);
    rows.push({ name, durable, samples, backlog, payloadBytes: 256, publishP50Ms: quantile(times, 0.50),
      publishP99Ms: quantile(times, 0.99), readRuns: reads, readEventsPerSecond: reads[1]!.eventsPerSecond });
    fs.writeFileSync(path.join(out!, "numbers.json"), JSON.stringify({ host: os.hostname(), node: process.version,
      server: "2.14.7", client: "2.29.3", syncInterval: "always", samples, backlog, rows }, null, 2) + "\n");
    console.log(JSON.stringify(rows.at(-1)));
  } finally { await log.close(); }
}

const root = fs.mkdtempSync(path.join(os.tmpdir(), "event-log-bench-"));
try {
  await measure("file-default-unkeyed", await openFileEventLog(path.join(root, "file-default"), 64 * 1024, backlog), false);
  await measure("file-durable-unkeyed", await openFileEventLog(path.join(root, "file-durable"), 64 * 1024, backlog), true);
  for (const size of [1, 3] as const) {
    const cluster = await startNatsCluster(size, `benchmark-r${size}`);
    try {
      await measure(`jetstream-r${size}`, await openJetStreamEventLog({ root: path.join(root, `r${size}`), servers: cluster.servers,
        replicas: size, maxReadEvents: backlog }), true);
    } finally { await cluster.stop(); }
  }
} finally { fs.rmSync(root, { recursive: true, force: true }); }
