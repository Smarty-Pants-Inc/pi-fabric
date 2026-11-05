import { execFile } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { promisify } from "node:util";
import { afterEach, describe, expect, it } from "vitest";

// Keep reason: data safety and the recorded #18675 lock-hold incident. Run the actual
// publishBatch entry point in a fresh process. The probe forwards EVERY fsync/fdatasync
// to the OS; it neither mocks durability nor disables directory barriers or deletions.
const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
interface Sample { fsyncSync: number; fdatasyncSync: number; holdMs: number; directories: string[]; files: string[] }
interface Probe {
  mode: string; root: string; archive: string; receiptDirectory: string; samples: Sample[];
  listings: Array<[string, number]>; receiptsMatch: boolean; retriesMatch: boolean;
  liveCount: number; uniqueIds: number; archivedIdsMatch: boolean | null;
  lastIndex: { committed: boolean } | null;
}
const chainLength = (directory: string): number => {
  let count = 1;
  for (let current = directory; path.dirname(current) !== current; current = path.dirname(current)) count++;
  return count;
};
const median = (values: number[]) => [...values].sort((a, b) => a - b)[Math.floor(values.length / 2)]!;

describe.skipIf(process.platform !== "linux")("keyed batch real-barrier budget (#18675)", () => {
  it.each(["archive", "no-archive"])("%s: prepare receipts off-lock; preserve durable fences and exactly-once retry", async mode => {
    const base = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-keyed-batch-budget-"));
    fs.chmodSync(base, 0o700); roots.push(base);
    const { stdout } = await promisify(execFile)(process.execPath,
      [path.resolve("tests/fixtures/mesh-keyed-batch-probe.mjs"), path.join(base, "mesh"), mode],
      { timeout: 30_000, maxBuffer: 4 * 1024 * 1024 });
    const result = JSON.parse(stdout.trim()) as Probe;
    const counts = result.samples.map(sample => sample.fsyncSync + sample.fdatasyncSync);
    console.log("KEYED_BATCH_BARRIERS " + JSON.stringify({ mode, counts,
      medianHoldMs: median(result.samples.map(sample => sample.holdMs)),
      maxHoldMs: Math.max(...result.samples.map(sample => sample.holdMs)),
      listings: result.listings, receiptChain: chainLength(result.receiptDirectory),
      indexChain: chainLength(path.join(result.archive, "sequence-index", "0")), liveChain: chainLength(result.root) }));
    expect(result.samples).toHaveLength(12);
    expect(result.receiptsMatch).toBe(true);
    expect(result.retriesMatch).toBe(true);
    expect(result.liveCount).toBe(13);
    expect(result.uniqueIds).toBe(13);
    if (mode === "archive") {
      expect(result.archivedIdsMatch).toBe(true);
      // False is advisory, NOT non-publication. The hot path need not rewrite it true.
      expect(result.lastIndex?.committed).toBe(false);
    }
    for (const sample of result.samples) {
      expect(sample.holdMs).toBeGreaterThanOrEqual(0);
      // Intent, receipt-install and intent-unlink each still pay a fresh leaf barrier.
      expect(sample.directories.filter(directory => directory === result.receiptDirectory)).toHaveLength(3);
      if (mode === "no-archive") {
        expect(sample.directories).toEqual(Array(3).fill(result.receiptDirectory));
        expect(sample.fsyncSync).toBe(5); // two files, three fresh receipt leaves
        expect(sample.fdatasyncSync).toBe(0);
      } else {
        const indexLeaf = path.join(result.archive, "sequence-index", "0");
        // Durable absent reservation and durable exact archive address, never a third
        // advisory committed:true rewrite. The live inode and its FULL namespace stay paid.
        expect(sample.files.filter(file => file.startsWith(indexLeaf + path.sep))).toHaveLength(2);
        expect(sample.fdatasyncSync).toBe(1);
        expect(sample.fsyncSync + sample.fdatasyncSync).toBe(6 + 2 * chainLength(indexLeaf) + 3 + chainLength(result.root));
      }
    }
  }, 45_000);
});
