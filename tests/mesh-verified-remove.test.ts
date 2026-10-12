// smarty-dev#8907: every mesh-internal deleter that judges a file expired and then deletes it removes only the judged
// inode (verifiedRemove, smarty-dev#7916). One table: each case makes a file its deleter judges expired; the test
// swaps a NEW file onto the same path between the judgment and the delete (verifiedRemove's beforeOpen seam), runs the
// real deleter, and asserts the new file survives. On main the deleter unlinks by name, so the new file is gone.
import { createHash, randomUUID } from "node:crypto";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { censusHostSlug, MeshStore, pruneDeadCensusRecords } from "../src/mesh/store.js";
import { censusSync } from "../src/mesh/writer-census.js";
import { sweepAbandonedStateTemporaries } from "../src/mesh/temp-janitor.js";
import { createLockStats, LOCK_STATS_DIR } from "../src/mesh/commit-stats.js";

const lockKey = Symbol.for("pi-fabric.mesh.lock-stats");
const registry = globalThis as typeof globalThis & { [lockKey]?: { flush(): void; dispose(): void } };
import { verifiedRemoveTestHooks } from "../src/storage/verified-remove.js";
import { importMeshState } from "../src/mesh/backend-migration.js";

const roots: string[] = [];
const tempRoot = (): string => { const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-verified-remove-")); roots.push(root); return root; };
afterEach(() => {
  vi.restoreAllMocks();
  delete verifiedRemoveTestHooks.beforeOpen;
  registry[lockKey]?.dispose();
  delete registry[lockKey];
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const deadPid = (): number => spawnSync(process.execPath, ["-e", ""]).pid!;
const old = (file: string, ageMs = 2 * 24 * 60 * 60 * 1000): void => { const at = (Date.now() - ageMs) / 1000; fs.utimesSync(file, at, at); };
const NEW = "NEW FILE swapped in after the judgment";
/** Swap a new inode onto `target` right before verifiedRemove opens it (after the deleter's judgment). */
const swapAtDelete = (target: string): { swapped: () => boolean } => {
  let swapped = false;
  verifiedRemoveTestHooks.beforeOpen = file => {
    if (file !== target || swapped) return;
    const staged = `${target}.${randomUUID()}.swap`;
    fs.writeFileSync(staged, NEW);
    fs.renameSync(staged, target);
    swapped = true;
  };
  return { swapped: () => swapped };
};
const deadCensusRecord = (root: string): string => {
  const directory = path.join(root, ".writer-census");
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  const pid = deadPid(), startedAt = 1_700_000_000_000;
  const file = path.join(directory, `${censusHostSlug(os.hostname())}-${pid}-${startedAt}.json`);
  fs.writeFileSync(file, JSON.stringify({ format: 1, pid, host: os.hostname(), startedAt }));
  return file;
};

interface Case { name: string; setup: () => { target: string; run: () => unknown | Promise<unknown>; arm?: () => { swapped: () => boolean } } }
/** commit-stats keeps its own rename-aside proof (it is loaded from one copied file): swap right before its move. */
const swapAtRename = (target: string): { swapped: () => boolean } => {
  let swapped = false;
  const rename = fs.renameSync.bind(fs);
  vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
    if (!swapped && String(from) === target && /\.prune-\d+$/.test(String(to))) {
      swapped = true;
      const staged = `${target}.${randomUUID()}.swap`;
      fs.writeFileSync(staged, NEW);
      rename(staged, target);
    }
    return rename(from, to);
  });
  return { swapped: () => swapped };
};
const cases: Case[] = [
  { name: "dead-writer census prune (store.ts pruneDeadCensusRecords)", setup: () => {
    const root = tempRoot(); const target = deadCensusRecord(root);
    return { target, run: () => pruneDeadCensusRecords(root) };
  } },
  { name: "dead-writer census record (writer-census.ts censusSync)", setup: () => {
    const root = tempRoot(); const target = deadCensusRecord(root);
    return { target, run: () => censusSync(root) };
  } },
  { name: "dead-writer census torn temporary (writer-census.ts censusSync)", setup: () => {
    const root = tempRoot();
    const directory = path.join(root, ".writer-census");
    fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
    const target = path.join(directory, `${censusHostSlug(os.hostname())}-${deadPid()}-1700000000000.json.abcdef.tmp`);
    fs.writeFileSync(target, "{");
    return { target, run: () => censusSync(root) };
  } },
  { name: "abandoned prepared state temporary (temp-janitor.ts)", setup: () => {
    const root = tempRoot();
    const target = path.join(root, `state.json.${deadPid()}.${randomUUID()}.prepared.tmp`);
    fs.writeFileSync(target, "{}"); old(target);
    return { target, run: () => sweepAbandonedStateTemporaries(root, { minAgeMs: 0, dead: () => true }) };
  } },
  { name: "stale prepared state of a dead pid (state-file.ts #sweepPreparedState)", setup: () => {
    const root = tempRoot();
    const target = path.join(root, `state.json.${deadPid()}.${randomUUID()}.prepared.tmp`);
    fs.writeFileSync(target, "{}");
    return { target, run: () => new MeshStore(root, 1024, 100) }; // the file backend sweeps at open
  } },
  { name: "blank state.json dropped by the import (backend-migration.ts dropBlankStateFile)", setup: () => {
    const root = tempRoot();
    const target = path.join(root, "state.json");
    fs.writeFileSync(target, "");
    // A swapped-in file is not blank state: the import then refuses it as damage, which is the point (it survives).
    return { target, run: () => importMeshState(root).catch(() => undefined) };
  } },
  { name: "commit-stats aged temporary (commit-stats.ts prune)", setup: () => {
    const root = tempRoot();
    const directory = path.join(root, LOCK_STATS_DIR);
    fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
    const target = path.join(directory, "gone-1.json.aged.tmp");
    fs.writeFileSync(target, "x"); old(target);
    return { target, arm: () => swapAtRename(target),
      run: () => { createLockStats("1")!.acquired(root, "custody", 1, 1); registry[lockKey]!.flush(); } };
  } },
];

describe("mesh judge-then-delete paths remove only the judged inode (smarty-dev#8907)", () => {
  const linux = process.platform === "linux" ? it.each(cases) : it.skip.each(cases);
  linux("$name: a file swapped in after the judgment survives", async ({ setup }) => {
    const { target, run, arm } = setup();
    const swap = arm ? arm() : swapAtDelete(target);
    await run();
    expect(swap.swapped()).toBe(true); // the deleter judged it expired and reached its delete
    expect(fs.existsSync(target)).toBe(true);
    expect(fs.readFileSync(target, "utf8")).toBe(NEW);
  });

  // The counterexample: with no swap, each deleter still removes its expired file.
  const plain = process.platform === "linux" ? it.each(cases) : it.skip.each(cases);
  plain("$name: with no swap, the expired file is removed", async ({ setup }) => {
    const { target, run } = setup();
    const judgedInode = fs.lstatSync(target).ino;
    await run();
    // Gone, or (the import) its name now holds a different file the import wrote: the judged inode is removed.
    expect(!fs.existsSync(target) || fs.lstatSync(target).ino !== judgedInode).toBe(true);
  });
});

// Event-log dedupe receipt compaction is driven through the real byte-triggered pass (as mesh-dedupe-retention does).
describe("dedupe receipt compaction removes only the judged receipt (smarty-dev#8907)", () => {
  const from = { id: "session:verified", name: "verified", kind: "main" as const };
  const receipt = (root: string, key: string) => path.join(root, "event-receipts", createHash("sha256").update(key).digest("hex") + ".json");
  const onLinux = process.platform === "linux" ? it : it.skip;
  onLinux("a receipt replaced after its expiry judgment survives the compaction", async () => {
    let now = 1_800_000_000_000;
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const root = tempRoot();
    const store = new MeshStore(root, 1024, 100, { maxEventLogBytes: 4096, retainedEventLogBytes: 1025, dedupeReceiptTtlMs: 1000 });
    const original = await store.publish({ topic: "mesh.verified", from, dedupeKey: "expires" });
    now = original.createdAt + 1000;
    const target = receipt(root, "expires");
    const swap = swapAtDelete(target);
    for (let index = 0; index < 20 && !swap.swapped(); index++) await store.publish({ topic: "mesh.verified", from, text: "x".repeat(600) });
    expect(swap.swapped()).toBe(true);
    expect(fs.readFileSync(target, "utf8")).toBe(NEW);
  });
});
