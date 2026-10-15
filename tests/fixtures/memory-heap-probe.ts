import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createFileSystemMemorySource } from "../../src/memory/fs-source.js";
import { reconstructSessionLineage } from "../../src/memory/lineage.js";
import { normalizeSession } from "../../src/memory/normalize.js";

const targetBytes = 50 * 1024 * 1024;
const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-memory-heap-probe-"));
const file = path.join(root, "synthetic.jsonl");
const fd = fs.openSync(file, "w");
const header = JSON.stringify({ type: "session", id: "heap-probe", cwd: "/probe" }) + "\n";
fs.writeSync(fd, header);
let bytes = Buffer.byteLength(header);
let index = 0;
while (bytes < targetBytes) {
  const line = JSON.stringify({
    type: "message",
    id: `entry-${index}`,
    parentId: index === 0 ? null : `entry-${index - 1}`,
    message: { role: "user", content: "x".repeat(16_000) },
  }) + "\n";
  fs.writeSync(fd, line);
  bytes += Buffer.byteLength(line);
  index += 1;
}
fs.closeSync(fd);

const gc = (globalThis as typeof globalThis & { gc?: () => void }).gc;
gc?.();
const baseline = process.memoryUsage();
let peakHeap = baseline.heapUsed;
let peakRss = baseline.rss;
const sample = (): void => {
  const usage = process.memoryUsage();
  peakHeap = Math.max(peakHeap, usage.heapUsed);
  peakRss = Math.max(peakRss, usage.rss);
};
const mode = process.argv[2] ?? "streaming";
let records = 0;
let listed = 0;
let parseSamples = 0;
const nativeParse = JSON.parse;
const restoreParse = JSON.parse;
(JSON as { parse: typeof JSON.parse }).parse = ((...args: Parameters<typeof JSON.parse>) => {
  const value = nativeParse(...args);
  parseSamples += 1;
  sample();
  return value;
}) as typeof JSON.parse;
try {
  if (mode === "baseline") {
    // This is the retired whole-file source path: retain the decoded source,
    // split lines, and parsed records together so the regression stays RED.
    const content = fs.readFileSync(file, "utf8");
    sample();
    const lines = content.split("\n");
    sample();
    const parsed: Record<string, unknown>[] = [];
    for (const line of lines) {
      if (!line.trim()) continue;
      try {
        const value: unknown = JSON.parse(line);
        if (value !== null && typeof value === "object" && !Array.isArray(value)) {
          parsed.push(value as Record<string, unknown>);
        }
      } catch {
        // Match the source's tolerant JSONL behavior.
      }
    }
    sample();
    records = parsed.length;
    listed = 1;
  } else if (mode === "streaming") {
    const source = createFileSystemMemorySource({ id: "probe", root });
    const listedResult = await source.listSessions({ limit: 1 });
    sample();
    listed = "sessions" in listedResult ? listedResult.sessions.length : listedResult.length;
    const snapshot = await source.loadSession("synthetic.jsonl", {});
    sample();
    records = snapshot?.records.length ?? 0;
  } else if (mode === "normalize") {
    const normalized = normalizeSession(file, 1_000);
    sample();
    records = normalized.entries.length;
    listed = 1;
  } else if (mode === "lineage") {
    const lineage = reconstructSessionLineage(file, "active");
    sample();
    records = lineage.entryOrdinals?.size ?? 0;
    listed = 1;
  } else {
    throw new Error(`Unknown heap probe mode: ${mode}`);
  }
} finally {
  (JSON as { parse: typeof JSON.parse }).parse = restoreParse;
}
process.stdout.write(JSON.stringify({
  mode,
  bytes,
  records,
  listed,
  parseSamples,
  heapDelta: peakHeap - baseline.heapUsed,
  rssDelta: peakRss - baseline.rss,
}) + "\n");
