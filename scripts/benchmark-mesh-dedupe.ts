// Structural + timing probe, not a flaky CI timing threshold.
// Run foreground: nice -n 19 bun scripts/benchmark-mesh-dedupe.ts
// All roots are temporary and removed. The archive and live log both grow 20x.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { MESH_ARCHIVE_CONFIG, MeshArchive } from "../src/mesh/archive.js";

const temporary = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-dedupe-bench-"));
const from: MeshIdentity = { id: "session:bench", name: "bench", kind: "main" };
const cases: Array<{ count: number; store: MeshStore; historyBytes: number; holds: number[]; reads: number[] }> = [];
const original = { write: fs.writeFileSync, rename: fs.renameSync, read: fs.readSync, fullRead: fs.readFileSync,
  readdir: fs.readdirSync, readAfter: MeshArchive.prototype.readAfter, history: MeshStore.prototype.read };
let active: typeof cases[number] | undefined;
let entered = 0;
let readBytes = 0;
let forbiddenHistoryReads = 0;
let receiptDirectoryScans = 0;
try {
  for (const count of [10_000, 200_000]) {
    const root = path.join(temporary, String(count));
    const dir = path.join(root, "archive");
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(root, MESH_ARCHIVE_CONFIG), JSON.stringify({ version: 1, dir }));
    const store = new MeshStore(root, 64 * 1024, 100, { maxEventLogBytes: 256 * 1024 * 1024 });
    const seed = await store.publish({ topic: "mesh.bench", from, text: "seed" });
    const events = Array.from({ length: count - 1 }, (_, index) => ({ ...seed, id: `history-${index}`, sequence: index + 2 }));
    MeshArchive.fromRoot(root)!.catchUp(events.map(event => ({ event, line: JSON.stringify(event) })));
    fs.appendFileSync(path.join(root, "events.jsonl"), events.map(event => `${JSON.stringify(event)}\n`).join(""));
    fs.writeFileSync(path.join(root, "sequence"), `${count}\n`);
    cases.push({ count, store, historyBytes: fs.statSync(path.join(root, "events.jsonl")).size, holds: [], reads: [] });
  }
  // Observe the actual held interval: owner publication to release rename, not time waiting.
  fs.writeFileSync = ((...args: Parameters<typeof fs.writeFileSync>) => {
    const result = original.write(...args);
    if (active && args[0] === path.join(active.store.root, ".lock", "owner")) entered = performance.now();
    return result;
  }) as typeof fs.writeFileSync;
  fs.renameSync = ((source: fs.PathLike, destination: fs.PathLike) => {
    if (active && source === path.join(active.store.root, ".lock") && String(destination).includes(".lock.released.")) {
      active.holds.push(performance.now() - entered);
      active.reads.push(readBytes);
    }
    return original.rename(source, destination);
  }) as typeof fs.renameSync;
  fs.readSync = ((...args: unknown[]) => {
    const result = (original.read as (...args: unknown[]) => number)(...args);
    if (active) readBytes += result;
    return result;
  }) as typeof fs.readSync;
  fs.readFileSync = ((...args: unknown[]) => {
    if (active && String(args[0]).endsWith(".jsonl")) forbiddenHistoryReads++;
    return (original.fullRead as (...args: unknown[]) => unknown)(...args);
  }) as typeof fs.readFileSync;
  fs.readdirSync = ((...args: unknown[]) => {
    if (active && args[0] === path.join(active.store.root, "event-receipts")) receiptDirectoryScans++;
    return (original.readdir as (...args: unknown[]) => unknown)(...args);
  }) as typeof fs.readdirSync;
  MeshArchive.prototype.readAfter = () => { forbiddenHistoryReads++; throw new Error("Unexpected archive history read"); };
  MeshStore.prototype.read = () => { forbiddenHistoryReads++; throw new Error("Unexpected live history read"); };
  // Warm once, then interleave cases to avoid machine-load/order bias.
  for (let sample = 0; sample < 26; sample++) {
    const order = sample % 2 ? [...cases].reverse() : cases;
    for (const item of order) {
      active = item; readBytes = 0;
      await item.store.publish({ topic: "mesh.bench", from, dedupeKey: `new:${sample}`, text: "new-key" });
      active = undefined;
    }
  }
  if (forbiddenHistoryReads) throw new Error("New-key publication did history-size work");
  if (receiptDirectoryScans !== 26 * cases.length) throw new Error("Expected one capacity enumeration per new key");
  const reports = cases.map(item => {
    const times = item.holds.slice(1).sort((a, b) => a - b);
    const bytes = item.reads.slice(1);
    return { historyEvents: item.count, historyBytes: item.historyBytes, samples: times.length,
      medianLockHoldMs: +times[Math.floor(times.length / 2)]!.toFixed(3),
      p95LockHoldMs: +times[Math.floor(times.length * 0.95)]!.toFixed(3),
      minLockHoldMs: +times[0]!.toFixed(3), maxLockHoldMs: +times.at(-1)!.toFixed(3),
      minMetadataReadBytes: Math.min(...bytes), maxMetadataReadBytes: Math.max(...bytes) };
  });
  if (reports[0]!.maxMetadataReadBytes !== reports[1]!.maxMetadataReadBytes) throw new Error("Read work grew with history");
  console.log(JSON.stringify({ reports, forbiddenHistoryReads, receiptDirectoryScans,
    note: "Read bytes are fixed-size sequence/torn-tail/archive-append metadata, not dedupe history scans. One bounded receipt/intent capacity enumeration per new key. No compaction or reboot backfill is included." }, null, 2));
} finally {
  fs.writeFileSync = original.write; fs.renameSync = original.rename; fs.readSync = original.read;
  fs.readFileSync = original.fullRead; fs.readdirSync = original.readdir;
  MeshArchive.prototype.readAfter = original.readAfter; MeshStore.prototype.read = original.history;
  fs.rmSync(temporary, { recursive: true, force: true });
}
