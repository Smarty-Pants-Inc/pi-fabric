import fs from "node:fs";
import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { execFileSync } from "node:child_process";
import { monitorEventLoopDelay } from "node:perf_hooks";
import { setImmediate as yieldTurn } from "node:timers/promises";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG, normalizeFabricConfig } from "../src/config.js";
import { RetentionReferenceScan } from "../src/storage/reference-scan.js";
import { ResidentLegacyRunArchive, legacyRunTreeProof, linuxRunFilesIdle } from "../src/residency/legacy-run-archive.js";

const roots: string[] = [];
const temporary = () => { const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-legacy-archive-")); roots.push(root); return root; };
const now = Date.now(), old = now - 49 * 60 * 60 * 1000;
const run = (root: string, id: string, record: Record<string, unknown> = {}) => {
  const dir = path.join(root, "runs", id);
  fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
  fs.writeFileSync(path.join(dir, "status.json"), JSON.stringify({ id, status: "completed", finishedAt: old, transport: "process", ...record }), { mode: 0o600 });
  fs.writeFileSync(path.join(dir, "events.jsonl"), Buffer.from([0, 255, 10, 65, 66]), { mode: 0o600 });
  return dir;
};
const emptyProc = (root: string) => { const dir = path.join(root, "proc"); fs.mkdirSync(dir, { mode: 0o700 }); return dir; };
const archiver = (root: string, procRoot: string, options = {}) => new ResidentLegacyRunArchive(root, {}, {
  isRetained: () => false, processFilesIdle: dirs => linuxRunFilesIdle(dirs, procRoot), ...options,
});
afterEach(() => { vi.restoreAllMocks(); for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });

describe("bounded retention reference preparation", () => {
  it("caps each turn, continues its cursor, caches unchanged generations and vetoes incomplete/overflow proofs", () => {
    const scan = new RetentionReferenceScan(); let visited = 0;
    const factory = function* (protect: (id: string) => void) { for (let i = 0; i < 20_000; i++) { visited++; protect(String(i)); yield; } };
    expect(scan.snapshot("one", factory, { now: 0, budgetMs: 100, maxEntries: 64 }).has("*")).toBe(true);
    expect(visited).toBe(64);
    for (let i = 0; i < 400; i++) scan.snapshot("one", factory, { now: 0, budgetMs: 100 });
    expect(visited).toBe(20_000); expect(scan.rebuilds).toBe(1);
    expect(scan.snapshot("one", factory, { now: 1 }).size).toBeLessThanOrEqual(1025);
    scan.snapshot("two", factory, { now: 1 }); expect(scan.rebuilds).toBe(2);
    scan.close();
  });

  it("20,000 disk runs never restart the prefix, unchanged polls read zero statuses and the loop remains below 50 ms p99", async () => {
    const root = temporary();
    for (let i = 0; i < 20_000; i++) run(root, `run-${String(i).padStart(5, "0")}`);
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 }, { runRoot: path.join(root, "runs") });
    let reads = 0;
    const original = fs.readFileSync;
    // Count without retaining 20,000 Vitest spy call/result objects: their GC
    // would contaminate the event-loop histogram during the later archive phase.
    fs.readFileSync = ((...args: Parameters<typeof original>) => {
      if (String(args[0]).startsWith(path.join(root, "runs") + "/") && String(args[0]).endsWith("/status.json")) reads++;
      return original(...args);
    }) as typeof original;
    const histogram = monitorEventLoopDelay({ resolution: 10 }); histogram.enable();
    let beat = Date.now(), maxGap = 0;
    const heartbeat = setInterval(() => { const current = Date.now(); maxGap = Math.max(maxGap, current - beat); beat = current; }, 25);
    try {
      for (let ticks = 0; reads < 20_000 && ticks < 5000; ticks++) { manager.retentionReferences({ now }); await yieldTurn(); }
      expect(reads).toBe(20_000);
      manager.retentionReferences({ now });
      const completeReads = reads;
      for (let i = 0; i < 400; i++) { manager.retentionReferences({ now }); await yieldTurn(); }
      expect(reads).toBe(completeReads);
      await new Promise(resolve => setTimeout(resolve, 30));
      expect(histogram.percentile(99) / 1e6).toBeLessThan(50);
      expect(maxGap).toBeLessThan(15_000); // actual participant TTL, not a mock deadline
      // Same 20k fixture, 32 small archive slices, each with a complete
      // isolated process view. No production permission-error bypass.
      if (process.platform !== "linux") return;
      const archive = archiver(root, emptyProc(root));
      const savedEvents = Buffer.from([0, 255, 10, 65, 66]);
      for (let i = 0; i < 32; i++) await archive.sweep(now, 8, 1000);
      expect(archive.health.error).toBe("");
      expect(archive.health.archived).toBe(256);
      expect(histogram.percentile(99) / 1e6).toBeLessThan(50);
      expect(maxGap).toBeLessThan(15_000);
      expect(fs.readdirSync(path.join(root, "runs"))).toHaveLength(19_744);
      await archive.close();
      const restored = path.join(root, "restored"); fs.mkdirSync(restored);
      const bundle = fs.readdirSync(path.join(root, "archive")).find(name => name.endsWith(".tar.gz"))!;
      execFileSync("tar", ["--ignore-zeros", "-xzf", path.join(root, "archive", bundle), "-C", restored]);
      expect(fs.readdirSync(restored)).toHaveLength(256);
      for (const id of fs.readdirSync(restored)) expect(fs.readFileSync(path.join(restored, id, "events.jsonl"))).toEqual(savedEvents);
      const manifest = fs.readFileSync(path.join(root, "archive", bundle.replace(".tar.gz", ".manifest.jsonl")), "utf8").trim().split("\n");
      expect(manifest).toHaveLength(256);
    } finally { fs.readFileSync = original; clearInterval(heartbeat); histogram.disable(); await manager.close(); }
  }, 90_000);
});

describe.skipIf(process.platform !== "linux")("legacy run archive proof", () => {
  it("rejects young/nonterminal/unknown/pending/nested/live-identity runs and restores every file byte-for-byte", async () => {
    const root = temporary(), proc = emptyProc(root);
    const good = run(root, "good"); run(root, "failed", { status: "failed" }); run(root, "stopped", { status: "stopped" });
    const rejected = ["running", "young", "no-finish", "pid", "pending", "unknown", "nested", "link", "unresolved"];
    run(root, "running", { status: "running" }); run(root, "young", { finishedAt: now - 1000 }); run(root, "no-finish", { finishedAt: null });
    run(root, "pid", { sessionId: String(process.pid) });
    const pending = run(root, "pending"); fs.mkdirSync(path.join(pending, "deliveries")); fs.writeFileSync(path.join(pending, "deliveries", "message.json"), "pending");
    fs.writeFileSync(path.join(run(root, "unknown"), "mystery"), "uncertain");
    const parent = run(root, "nested"); const child = path.join(parent, "nested", "child"); fs.mkdirSync(child, { recursive: true, mode: 0o700 });
    fs.writeFileSync(path.join(child, "status.json"), JSON.stringify({ status: "running", finishedAt: old }));
    fs.symlinkSync(path.join(good, "events.jsonl"), path.join(run(root, "link"), "link"));
    fs.writeFileSync(path.join(run(root, "unresolved"), "unresolved-worker.json"), "{}");
    for (const id of rejected) expect(await legacyRunTreeProof(path.join(root, "runs", id), now, 48 * 60 * 60 * 1000), id).toBeUndefined();
    const before = new Map(fs.readdirSync(good).map(name => [name, fs.readFileSync(path.join(good, name))]));
    const archive = archiver(root, proc);
    await archive.sweep(now, 32, 1000); expect(archive.health.error).toBe(""); expect(archive.health.archived).toBe(3); await archive.close();
    for (const id of rejected) expect(fs.existsSync(path.join(root, "runs", id)), id).toBe(true);
    const restored = path.join(root, "restore"); fs.mkdirSync(restored);
    execFileSync("tar", ["--ignore-zeros", "-xzf", path.join(root, "archive", new Date(now).toISOString().slice(0, 10) + ".tar.gz"), "-C", restored]);
    for (const [name, bytes] of before) expect(fs.readFileSync(path.join(restored, "good", name))).toEqual(bytes);
  });

  it("checks real open descriptors, cwd and mmap paths and fails closed for incomplete /proc", async () => {
    const root = temporary(), dir = run(root, "held"), proc = emptyProc(root);
    const pid = path.join(proc, String(process.pid)); fs.symlinkSync(`/proc/${process.pid}`, pid);
    const fd = fs.openSync(path.join(dir, "events.jsonl"), "r");
    try { expect(await linuxRunFilesIdle([dir], proc)).toBe(false); } finally { fs.closeSync(fd); }
    expect(await linuxRunFilesIdle([dir], proc)).toBe(true);
    fs.mkdirSync(path.join(proc, "2147483647"));
    expect(await linuxRunFilesIdle([dir], proc)).toBe(false);
    const archive = archiver(root, proc); await archive.sweep(now, 32, 1000); await archive.close();
    expect(fs.existsSync(dir)).toBe(true); expect(archive.health.processProofIncomplete).toBe(1);
  });

  it("vetoes restricted Linux visibility before reading any run status, with a 60-second retry", async () => {
    const root = temporary(); for (let i = 0; i < 4; i++) run(root, `old-${i}`);
    vi.spyOn(fsp, "readlink").mockRejectedValue(Object.assign(new Error("hidden proc"), { code: "EACCES" }));
    let statusReads = 0; const read = fsp.readFile;
    vi.spyOn(fsp, "readFile").mockImplementation((...args: Parameters<typeof read>) => {
      if (String(args[0]).endsWith("/status.json")) statusReads++;
      return read(...args);
    });
    const archive = new ResidentLegacyRunArchive(root, {}, { isRetained: () => false });
    await archive.sweep(now); await archive.sweep(now + 1);
    expect(statusReads).toBe(0); expect(archive.health.checked).toBe(0);
    expect(archive.health.processProofIncomplete).toBe(1);
    await archive.sweep(now + 60_001); await archive.close();
    expect(archive.health.processProofIncomplete).toBe(2);
    expect(fs.readdirSync(path.join(root, "runs"))).toHaveLength(4);
  });

  it("retains staged bytes and freezes after compression failure, including across restart", async () => {
    const root = temporary(), proc = emptyProc(root);
    run(root, "first"); run(root, "second");
    const archive = archiver(root, proc), originalPath = process.env.PATH;
    try {
      process.env.PATH = path.join(root, "no-tools");
      await archive.sweep(now, 1, 1000);
    } finally { process.env.PATH = originalPath; }
    expect(archive.health.archived).toBe(0); expect(archive.health.error).toMatch(/ENOENT/);
    const staging = fs.readdirSync(path.join(root, "archive")).find(name => name.startsWith(".staging-"))!;
    expect(fs.readdirSync(path.join(root, "archive", staging)).some(name => fs.statSync(path.join(root, "archive", staging, name)).isDirectory())).toBe(true);
    const remaining = fs.readdirSync(path.join(root, "runs"));
    await archive.sweep(now, 32, 1000); expect(fs.readdirSync(path.join(root, "runs"))).toEqual(remaining);
    await archive.close();
    const restarted = archiver(root, proc); await restarted.sweep(now, 32, 1000); await restarted.close();
    expect(restarted.health.error).toMatch(/operator recovery/); expect(fs.readdirSync(path.join(root, "runs"))).toEqual(remaining);
  });

  it("rolls back staging when process custody appears after rename", async () => {
    const root = temporary(), proc = emptyProc(root), dir = run(root, "race"); let probes = 0;
    const archive = archiver(root, proc, { processFilesIdle: async () => ++probes === 1 });
    await archive.sweep(now, 32, 1000); await archive.close();
    expect(fs.existsSync(dir)).toBe(true); expect(archive.health.archived).toBe(0);
    expect(fs.readdirSync(path.join(root, "archive"))).toHaveLength(0);
  });

  it("honors disable/age and actor, manager, admission and outbox custody", async () => {
    const root = temporary(), proc = emptyProc(root), dir = run(root, "held");
    const policy = { legacyRunArchiveEnabled: false, legacyRunArchiveAgeMs: 60 * 60 * 1000 };
    const archive = new ResidentLegacyRunArchive(root, policy, { isRetained: () => false, processFilesIdle: dirs => linuxRunFilesIdle(dirs, proc) });
    await archive.sweep(now); expect(fs.existsSync(dir)).toBe(true); policy.legacyRunArchiveEnabled = true;
    const outbox = path.join(root, "delivery-outbox"); fs.mkdirSync(outbox); fs.writeFileSync(path.join(outbox, "pending.json"), "{}");
    await archive.sweep(now, 32, 1000); expect(fs.existsSync(dir)).toBe(true); await archive.close();
    fs.rmSync(outbox, { recursive: true });
    const actorRoot = path.join(root, "actors"); fs.mkdirSync(actorRoot); fs.writeFileSync(path.join(actorRoot, "actors.json"), JSON.stringify({ actors: [{ id: "actor", lastRunId: "held" }] }));
    const referenced = archiver(root, proc, { actorRoots: [actorRoot] }); await referenced.sweep(now, 32, 1000); await referenced.close(); expect(fs.existsSync(dir)).toBe(true);
    const held = archiver(root, proc, { isRetained: () => true }); await held.sweep(now, 32, 1000); await held.close(); expect(fs.existsSync(dir)).toBe(true);
    const config = normalizeFabricConfig({ retention: { legacyRunArchiveEnabled: false, legacyRunArchiveAgeMs: 3_600_000 } });
    expect(config.retention.legacyRunArchiveEnabled).toBe(false); expect(config.retention.legacyRunArchiveAgeMs).toBe(3_600_000);
  });
});
