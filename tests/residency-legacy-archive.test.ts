import fs from "node:fs";
import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { execFileSync, spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { ResidentRequestRetention } from "../src/residency/retention.js";
import { acknowledgeResidentResponse, commitResidentRequest, type ResidentCommand } from "../src/residency/protocol.js";
import { newResidentRequestId } from "../src/residency/request-expiry.js";
import { monitorEventLoopDelay } from "node:perf_hooks";
import { setImmediate as yieldTurn } from "node:timers/promises";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG, normalizeFabricConfig } from "../src/config.js";
import { RetentionReferenceScan } from "../src/storage/reference-scan.js";
import { ResidentLegacyRunArchive, legacyRunTreeProof } from "../src/residency/legacy-run-archive.js";

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
const archiver = (root: string, options = {}) => new ResidentLegacyRunArchive(root, {}, { isRetained: () => false, ...options });
// Native Windows exercises the real platform branch; never impersonate POSIX.
// Assert one audit per invocation, zero candidate work and zero mutation calls.
const sweepArchive = async (archive: ResidentLegacyRunArchive, ...args: Parameters<ResidentLegacyRunArchive["sweep"]>) => {
  if (process.platform !== "win32") return archive.sweep(...args);
  const audit = vi.spyOn(console, "info").mockImplementation(() => undefined);
  const probes = [vi.spyOn(fsp, "opendir"), vi.spyOn(fsp, "mkdir"), vi.spyOn(fsp, "mkdtemp"),
    vi.spyOn(fsp, "rename"), vi.spyOn(fsp, "rm"), vi.spyOn(fsp, "unlink"),
    vi.spyOn(fs, "renameSync"), vi.spyOn(fs, "rmSync"), vi.spyOn(fs, "unlinkSync"), vi.spyOn(fs, "writeFileSync")];
  // An existing injected EXDEV/partial-failure spy may have prior calls.
  for (const probe of probes) probe.mockClear();
  try {
    await archive.sweep(...args);
    expect(audit).toHaveBeenCalledExactlyOnceWith("[pi-fabric] Legacy run retirement pass: no-op on win32 (POSIX-only; smarty-dev#5132)");
    for (const probe of probes) expect(probe).not.toHaveBeenCalled();
    expect(archive.health).toEqual({ checked: 0, archived: 0, skipped: 0, error: "" });
  } finally { audit.mockRestore(); for (const probe of probes) probe.mockRestore(); }
};
const retiredRuns = (root: string): string[] => {
  const day = path.join(root, "runs-retired", new Date(now).toISOString().slice(0, 10));
  if (!fs.existsSync(day)) return [];
  return fs.readdirSync(day).flatMap(slice => fs.readdirSync(path.join(day, slice)).map(id => path.join(day, slice, id)));
};
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

  it("reconciles a run added while the final cached actor-hint phase is suspended", async () => {
    const root = temporary();
    for (let i = 0; i < 4; i++) run(root, `history-${i}`, { sessionId: "2147483647" });
    const runRoot = path.join(root, "runs");
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 }, { runRoot });
    let clock = 0, suspended = false;
    const timing = vi.spyOn(performance, "now").mockImplementation(() => clock);
    const close = fs.Dir.prototype.closeSync;
    const cursor = vi.spyOn(fs.Dir.prototype, "closeSync").mockImplementation(function(this: fs.Dir) {
      close.call(this);
      if (this.path === runRoot && !suspended) { suspended = true; clock = 101; }
    });
    try {
      expect(manager.retentionReferences({ now, budgetMs: 100, maxEntries: 64 }).has("*")).toBe(true);
      expect(suspended).toBe(true);
      run(root, "late-run", { status: "running", sessionId: "2147483647", actorId: "late-actor" });
      const refs = manager.retentionReferences({ now, budgetMs: 100, maxEntries: 64 });
      expect(refs.has("*")).toBe(false);
      expect(refs.has("late-run")).toBe(true); expect(refs.has("late-actor")).toBe(true);
      expect(manager.retentionCustodyVeto("late-actor")).toBe(true);
    } finally { timing.mockRestore(); cursor.mockRestore(); await manager.close(); }
  });

  it.each(["native", "win32"])("historical tail progresses under new runs and UI/status activity, then safely expires an acknowledged exchange (%s)", async platform => {
    const nativePlatform = Object.getOwnPropertyDescriptor(process, "platform")!;
    if (platform === "win32") Object.defineProperty(process, "platform", { ...nativePlatform, value: "win32" });
    const root = temporary(), count = 4096;
    for (let i = 0; i < count; i++) run(root, `history-${i}`, { sessionId: "2147483647" });
    const cursor = fs.opendirSync(path.join(root, "runs")); let tail = "";
    try { let entry: fs.Dirent | null; while ((entry = cursor.readSync())) tail = entry.name; } finally { cursor.closeSync(); }
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, retainRuns: true }, {
      runRoot: path.join(root, "runs"), workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
    });
    const reads = new Map<string, number>(), original = fs.readFileSync;
    fs.readFileSync = ((...args: Parameters<typeof original>) => {
      const file = String(args[0]);
      if (file.startsWith(path.join(root, "runs", "history-")) && path.basename(file) === "status.json") reads.set(file, (reads.get(file) ?? 0) + 1);
      return original(...args);
    }) as typeof original;
    const completedAt = now - 25 * 60 * 60 * 1000, requestId = newResidentRequestId(completedAt);
    const command = { format: 3, requestId, rootId: "session:activity", operation: "cleanup", id: tail, deleteBranch: false, createdAt: completedAt } as ResidentCommand;
    commitResidentRequest(root, command, tail, "host");
    const response = { format: 1 as const, requestId, ok: true, completedAt };
    fs.mkdirSync(path.join(root, "responses"), { recursive: true });
    fs.writeFileSync(path.join(root, "responses", `${requestId}.json`), JSON.stringify(response));
    acknowledgeResidentResponse(root, response, completedAt, 3);
    const retention = new ResidentRequestRetention(root, [], {}, undefined, { run: id => manager.hasRunCustody(id), reference: id => manager.retentionCustodyVeto(id) });
    let complete = false, lastRefs = new Set<string>();
    try {
      const K = 1024;
      for (let slice = 0; slice < K; slice++) {
        if (slice % 64 === 0) run(root, `new-${slice}`, { sessionId: "2147483647" });
        if (slice % 32 === 0) {
          const outcome = await manager.run({ task: `ordinary UI/status activity ${slice}`, transport: "process" });
          manager.status(outcome.id); manager.listForUi();
        }
        // This exercises activity/cursor correctness, not filesystem latency.
        // A 2-ms turn cannot guarantee four 5-read disk proofs on Windows.
        // Keep the count bound and every ownership/read assertion; the 20k
        // test below independently exercises the production 2-ms budget.
        const live = manager.retentionReferences({ now, budgetMs: 100, maxEntries: 64 }); lastRefs = live;
        if (!live.has("*")) {
          complete = true;
          expect(manager.retentionCustodyVeto(tail)).toBe(false);
          retention.sweep(now, live, 100);
          break;
        }
      }
      expect(complete, JSON.stringify({ visited: reads.size, refs: [...lastRefs].slice(0, 20), reads: [...reads.values()].reduce((a, b) => a + b, 0) })).toBe(true);
      expect(reads.has(path.join(root, "runs", tail, "status.json"))).toBe(true);
      expect(fs.existsSync(path.join(root, "decisions", `${requestId}.json`))).toBe(false);
      // Initial record/tree/removability checks plus the fresh resident sweep
      // are bounded. Activity does not repeat historical ownership preparation.
      expect(reads.get(path.join(root, "runs", "history-0", "status.json"))).toBeLessThanOrEqual(8);
      const historicalReads = [...reads.values()].reduce((a, b) => a + b, 0);
      for (let slice = 0; slice < 8; slice++) {
        run(root, `after-${slice}`, { sessionId: "2147483647" });
        manager.listForUi();
        manager.retentionReferences({ now, budgetMs: 100, maxEntries: 64 });
      }
      expect([...reads.values()].reduce((a, b) => a + b, 0)).toBe(historicalReads);
      // New pending sources retain their fresh, target-specific veto regardless
      // of a completed historical watermark.
      const held = run(root, "new-pending", { sessionId: "2147483647" });
      fs.writeFileSync(path.join(held, "archive-pending.json"), "{}");
      expect(manager.retentionCustodyVeto("new-pending")).toBe(true);
    } finally {
      fs.readFileSync = original; retention.close();
      try { await manager.close(); } finally { Object.defineProperty(process, "platform", nativePlatform); }
    }
  }, 30_000);

  it("keeps the historical prefix read bound when the resident run cursor visits it", async () => {
    const root = temporary(); run(root, "history-0", { sessionId: "2147483647" });
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 }, { runRoot: path.join(root, "runs") });
    const retention = new ResidentRequestRetention(root);
    const read = fs.readFileSync; let reads = 0;
    vi.spyOn(fs, "readFileSync").mockImplementation((...args: Parameters<typeof read>) => {
      if (String(args[0]) === path.join(root, "runs", "history-0", "status.json")) reads++;
      return read(...args);
    });
    const rename = vi.spyOn(fs, "renameSync");
    try {
      const refs = manager.retentionReferences({ now, budgetMs: 100, maxEntries: 64 });
      expect(refs.has("*")).toBe(false); expect(reads).toBe(5);
      // A one-run fixture guarantees history-0 is visited on every native FS,
      // unlike relying on the platform's 4096-run directory enumeration order.
      retention.sweep(now, refs, 1000);
      expect(reads).toBe(6); expect(reads).toBeLessThanOrEqual(8);
      expect(rename.mock.calls.some(call => String(call[1]).endsWith("events.jsonl"))).toBe(false);
    } finally { retention.close(); await manager.close(); }
  });

  it("resumes at a timed-out predicate without rereading the prepared historical prefix", async () => {
    const root = temporary(); run(root, "boundary-run", { sessionId: "2147483647" });
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 }, { runRoot: path.join(root, "runs") });
    let clock = 0, reads = 0;
    vi.spyOn(performance, "now").mockImplementation(() => clock);
    const read = fs.readFileSync;
    vi.spyOn(fs, "readFileSync").mockImplementation((...args: Parameters<typeof read>) => {
      const result = read(...args);
      if (String(args[0]) === path.join(root, "runs", "boundary-run", "status.json") && ++reads === 5) clock += 3;
      return result;
    });
    try {
      let refs = manager.retentionReferences({ now, budgetMs: 2 });
      expect(refs.has("*")).toBe(true); expect(reads).toBe(5);
      for (let slice = 0; slice < 4 && refs.has("*"); slice++) refs = manager.retentionReferences({ now, budgetMs: 2 });
      expect(refs.has("*")).toBe(false); expect(reads).toBeLessThanOrEqual(8);
    } finally { vi.restoreAllMocks(); await manager.close(); }
  });

  it("retries transient custody on the next pass without caching a timeout for 60 seconds", async () => {
    const root = temporary(); run(root, "retry-run", { sessionId: "2147483647" });
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 }, { runRoot: path.join(root, "runs") });
    let clock = 0, slow = true, reads = 0;
    vi.spyOn(performance, "now").mockImplementation(() => clock);
    const read = fs.readFileSync;
    vi.spyOn(fs, "readFileSync").mockImplementation((...args: Parameters<typeof read>) => {
      const result = read(...args);
      if (String(args[0]) === path.join(root, "runs", "retry-run", "status.json")) { reads++; if (slow) clock += 3; }
      return result;
    });
    try {
      for (let slice = 0; slice < 8; slice++) expect(manager.retentionReferences({ now, budgetMs: 2 }).has("*")).toBe(true);
      const failedReads = reads; slow = false;
      let refs = new Set(["*"]);
      for (let slice = 0; slice < 8 && refs.has("*"); slice++) refs = manager.retentionReferences({ now, budgetMs: 2 });
      expect(reads).toBeGreaterThan(failedReads); expect(refs.has("*")).toBe(false);
      expect(manager.retentionCustodyVeto("retry-run")).toBe(false);
    } finally { vi.restoreAllMocks(); await manager.close(); }
  });

  it("crossing the refresh interval mid-walk preserves the prepared historical prefix", async () => {
    const root = temporary();
    for (let i = 0; i < 256; i++) run(root, `history-${i}`, { sessionId: "2147483647" });
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 }, { runRoot: path.join(root, "runs") });
    // Keep the slice budget independent of native filesystem speed. The
    // explicit refresh timestamp below crosses after exactly 64 prepared runs,
    // not after an arbitrary prefix cut short by the 100 ms wall-clock budget.
    const budgetClock = vi.spyOn(performance, "now").mockReturnValue(0);
    const reads = new Map<string, number>(), original = fs.readFileSync;
    fs.readFileSync = ((...args: Parameters<typeof original>) => {
      const file = String(args[0]);
      if (file.startsWith(path.join(root, "runs") + path.sep) && path.basename(file) === "status.json") reads.set(file, (reads.get(file) ?? 0) + 1);
      return original(...args);
    }) as typeof original;
    try {
      expect(manager.retentionReferences({ now, budgetMs: 100, maxEntries: 64 }).has("*")).toBe(true);
      const prefixReads = [...reads.values()].reduce((sum, count) => sum + count, 0);
      expect(reads.size).toBe(64);
      let complete = false;
      for (let slice = 0; slice < 16; slice++) {
        if (!manager.retentionReferences({ now: now + 60_001, budgetMs: 100, maxEntries: 64 }).has("*")) { complete = true; break; }
      }
      expect(complete).toBe(true); expect(reads.size).toBe(256);
      // Status + persisted exit + removability proof: exactly five reads per
      // run, including the prefix inspected before the interval boundary.
      expect(prefixReads).toBe(64 * 5);
      expect([...reads.values()].every(count => count === 5)).toBe(true);
    } finally { fs.readFileSync = original; budgetClock.mockRestore(); await manager.close(); }
  });

  it("20,000 disk runs cap ownership work per turn, never restart the prefix and read zero statuses on unchanged polls", async () => {
    const root = temporary();
    for (let i = 0; i < 20_000; i++) run(root, `run-${String(i).padStart(5, "0")}`);
    const runRoot = path.join(root, "runs");
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 }, { runRoot });
    let reads = 0, entries = 0;
    const original = fs.readFileSync, originalRead = fs.Dir.prototype.readSync;
    // Count without retaining 20,000 Vitest spy call/result objects: their GC
    // would contaminate the event-loop histogram during the later archive phase.
    fs.readFileSync = ((...args: Parameters<typeof original>) => {
      if (String(args[0]).startsWith(runRoot + path.sep) && path.basename(String(args[0])) === "status.json") reads++;
      return original(...args);
    }) as typeof original;
    fs.Dir.prototype.readSync = function(this: fs.Dir) {
      const entry = originalRead.call(this);
      if (this.path === runRoot && entry) entries++;
      return entry;
    };
    const histogram = monitorEventLoopDelay({ resolution: 10 }); histogram.enable();
    let beat = Date.now(), maxGap = 0;
    const heartbeat = setInterval(() => { const current = Date.now(); maxGap = Math.max(maxGap, current - beat); beat = current; }, 25);
    // Count actual disk entries, not just generator resumes. One missing yield
    // must fail even on a fast machine, rather than depend on a shared CI clock.
    const prepareTurn = () => {
      const previousReads = reads, previousEntries = entries;
      const refs = manager.retentionReferences({ now });
      expect(reads - previousReads).toBeLessThanOrEqual(64);
      expect(entries - previousEntries).toBeLessThanOrEqual(64);
      return refs;
    };
    try {
      // Prove the default count cap independently of the 2-ms time budget.
      const budgetClock = vi.spyOn(performance, "now").mockReturnValue(0);
      try { expect(prepareTurn().has("*")).toBe(true); expect(reads).toBe(64); expect(entries).toBe(64); }
      finally { budgetClock.mockRestore(); }
      let turns = 1;
      await yieldTurn();
      for (; reads < 20_000 && turns < 5000; turns++) { prepareTurn(); await yieldTurn(); }
      expect(turns).toBeGreaterThanOrEqual(Math.ceil(20_000 / 64));
      expect(reads).toBe(20_000); expect(entries).toBe(20_000);
      prepareTurn();
      const completeReads = reads, completeEntries = entries;
      for (let i = 0; i < 400; i++) { prepareTurn(); await yieldTurn(); }
      expect(reads).toBe(completeReads); expect(entries).toBe(completeEntries);
      await new Promise(resolve => setTimeout(resolve, 30));
      // Sanity guard only: scheduler contention/GC is not archive work. The
      // deterministic entry/read cap above is the non-blocking regression.
      expect(histogram.percentile(99) / 1e6).toBeLessThan(500);
      expect(maxGap).toBeLessThan(15_000); // actual participant TTL, not a mock deadline
      // Same 20k fixture: 32 bounded rename-only retention slices.
      const archive = archiver(root);
      const savedEvents = Buffer.from([0, 255, 10, 65, 66]);
      for (let i = 0; i < 32; i++) await sweepArchive(archive, now, 8, 1000);
      if (process.platform === "win32") {
        expect(histogram.percentile(99) / 1e6).toBeLessThan(500);
        expect(maxGap).toBeLessThan(15_000);
        expect(fs.readdirSync(runRoot)).toHaveLength(20_000);
        expect(retiredRuns(root)).toEqual([]);
        expect(fs.existsSync(path.join(root, "runs-retired"))).toBe(false);
        expect(fs.readFileSync(path.join(runRoot, "run-00000", "events.jsonl"))).toEqual(savedEvents);
        await archive.close();
        return;
      }
      expect(archive.health.error).toBe("");
      expect(archive.health.archived).toBe(256);
      expect(histogram.percentile(99) / 1e6).toBeLessThan(500);
      expect(maxGap).toBeLessThan(15_000);
      expect(fs.readdirSync(runRoot)).toHaveLength(19_744);
      await archive.close();
      const retired = retiredRuns(root);
      expect(retired).toHaveLength(256);
      for (const directory of retired) expect(fs.readFileSync(path.join(directory, "events.jsonl"))).toEqual(savedEvents);
      // Restore needs no snapshot/decompress operation: move the SAME inodes.
      const restored = path.join(root, "restored"); fs.mkdirSync(restored);
      for (const directory of retired) fs.renameSync(directory, path.join(restored, path.basename(directory)));
      expect(fs.readdirSync(restored)).toHaveLength(256);
      expect(fs.existsSync(path.join(root, "archive"))).toBe(false);
    } finally { fs.readFileSync = original; fs.Dir.prototype.readSync = originalRead; clearInterval(heartbeat); histogram.disable(); await manager.close(); }
  }, 90_000);
});

describe("rename-only legacy run retention", () => {
  it.skipIf(process.platform !== "win32")("logs one no-op per native Windows pass, including disabled policy and repeated calls", async () => {
    const root = temporary(), directory = run(root, "kept");
    const policy = { legacyRunArchiveEnabled: false };
    const archive = new ResidentLegacyRunArchive(root, policy, { isRetained: () => { throw new Error("must not inspect runs"); } });
    const before = fs.readFileSync(path.join(directory, "events.jsonl"));
    try {
      await sweepArchive(archive, now, 32, 1000);
      policy.legacyRunArchiveEnabled = true;
      await sweepArchive(archive, now, 32, 1000);
      await sweepArchive(archive, now, 32, 1000);
      expect(fs.readFileSync(path.join(directory, "events.jsonl"))).toEqual(before);
      expect(fs.existsSync(path.join(root, "runs-retired"))).toBe(false);
      expect(fs.existsSync(path.join(root, "archive-retention.json"))).toBe(false);
    } finally { await archive.close(); }
  });

  it("rejects young/nonterminal/unknown/pending/nested/live-identity runs and retains every file byte-for-byte", async () => {
    const root = temporary();
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
    const inode = fs.statSync(good).ino;
    const archive = archiver(root);
    try { await sweepArchive(archive, now, 32, 1000); expect(archive.health.error).toBe(""); expect(archive.health.archived).toBe(process.platform === "win32" ? 0 : 3); }
    finally { await archive.close(); }
    for (const id of rejected) expect(fs.existsSync(path.join(root, "runs", id)), id).toBe(true);
    if (process.platform === "win32") {
      for (const id of ["good", "failed", "stopped", ...rejected]) expect(fs.existsSync(path.join(root, "runs", id)), id).toBe(true);
      expect(retiredRuns(root)).toEqual([]);
      expect(fs.statSync(good).ino).toBe(inode);
      for (const [name, bytes] of before) expect(fs.readFileSync(path.join(good, name))).toEqual(bytes);
      return;
    }
    const moved = retiredRuns(root).find(directory => path.basename(directory) === "good")!;
    expect(fs.statSync(moved).ino).toBe(inode);
    for (const [name, bytes] of before) expect(fs.readFileSync(path.join(moved, name))).toEqual(bytes);
    expect(fs.existsSync(path.join(root, "archive"))).toBe(false);
  });

  it.skipIf(process.platform !== "linux")("keeps an unreceived SCM_RIGHTS descriptor queued through retention; later writes reach the moved file", async () => {
    const root = temporary(), directory = run(root, "queued"), file = path.join(directory, "events.jsonl");
    const before = fs.readFileSync(file), identity = fs.statSync(file);
    const child = spawn("python3", [fileURLToPath(new URL("./fixtures/queued-run-descriptor.py", import.meta.url)), file], { stdio: ["pipe", "pipe", "pipe"] });
    let stdout = "", stderr = "";
    child.stderr.on("data", data => { stderr += data; });
    const finished = new Promise<number | null>(resolve => { child.once("error", () => resolve(-1)); child.once("close", resolve); });
    const queued = new Promise<Record<string, unknown>>((resolve, reject) => {
      child.stdout.on("data", data => { stdout += data; if (stdout.includes("\n")) resolve(JSON.parse(stdout.split("\n")[0]!)); });
      child.once("error", reject);
      child.once("close", () => { if (!stdout.includes("\n")) reject(new Error(`descriptor fixture exited: ${stderr}`)); });
    });
    const archive = archiver(root);
    try {
      expect(await queued).toEqual({ queued: true, matching_fds: [], ino: identity.ino, dev: identity.dev });
      await sweepArchive(archive, now, 32, 1000);
      expect(archive.health.error).toBe(""); expect(archive.health.archived).toBe(1);
      const moved = retiredRuns(root);
      expect(moved).toHaveLength(1); expect(fs.existsSync(directory)).toBe(false);
      const retained = path.join(moved[0]!, "events.jsonl"), stat = fs.statSync(retained);
      expect([stat.dev, stat.ino, stat.nlink]).toEqual([identity.dev, identity.ino, 1]);
      expect(fs.readFileSync(retained)).toEqual(before);
      // The receiver has not been released through the ENTIRE pass, including fsync.
      expect(stdout.trim().split("\n")).toHaveLength(1);
      child.stdin.end("receive\n");
      expect(await finished, stderr).toBe(0);
      expect(JSON.parse(stdout.trim().split("\n")[1]!)).toEqual({ received_same_inode: true, nlink: 1 });
      expect(fs.readFileSync(retained)).toEqual(Buffer.concat([before, Buffer.from("late SCM_RIGHTS write\n")]));
      expect(fs.statSync(retained).nlink).toBe(1); expect(fs.existsSync(path.join(root, "archive"))).toBe(false);
    } finally { child.stdin.end(); if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL"); await finished; await archive.close(); }
  }, 15_000);

  it("does not overwrite a retired run ID and needs no tar executable or proc visibility", async () => {
    const root = temporary(); run(root, "same");
    const first = archiver(root), originalPath = process.env.PATH;
    try { process.env.PATH = path.join(root, "no-tools"); await sweepArchive(first, now, 32, 1000); }
    finally { process.env.PATH = originalPath; await first.close(); }
    expect(first.health.error).toBe(""); expect(first.health.archived).toBe(process.platform === "win32" ? 0 : 1);
    if (process.platform === "win32") expect(fs.readFileSync(path.join(root, "runs", "same", "events.jsonl"))).toEqual(Buffer.from([0, 255, 10, 65, 66]));
    const secondSource = run(root, "same"); fs.writeFileSync(path.join(secondSource, "events.jsonl"), "second");
    const second = archiver(root); try { await sweepArchive(second, now, 32, 1000); } finally { await second.close(); }
    if (process.platform === "win32") {
      expect(retiredRuns(root)).toEqual([]);
      expect(fs.readFileSync(path.join(secondSource, "events.jsonl"), "utf8")).toBe("second");
      return;
    }
    const retired = retiredRuns(root); expect(retired).toHaveLength(2);
    expect(retired.map(directory => fs.readFileSync(path.join(directory, "events.jsonl"), "utf8")).sort()).toEqual([Buffer.from([0, 255, 10, 65, 66]).toString(), "second"].sort());
  });

  it.each(["residency", "mesh", "runs"])("vetoes a %s symlink ancestor without moving bytes", async kind => {
    const sandbox = temporary(), physical = path.join(sandbox, "physical"); fs.mkdirSync(physical, { mode: 0o700 });
    const realRoot = kind === "mesh" ? path.join(physical, "residency", "host") : physical;
    const realDir = run(realRoot, "aliased"), view = path.join(sandbox, "view"); fs.symlinkSync(physical, view);
    let root = kind === "mesh" ? path.join(view, "residency", "host") : view;
    if (kind === "runs") { root = path.join(sandbox, "resident"); fs.mkdirSync(root, { mode: 0o700 }); fs.symlinkSync(path.join(physical, "runs"), path.join(root, "runs")); }
    const archive = archiver(root); try { await sweepArchive(archive, now, 32, 1000); expect(archive.health.archived).toBe(0); expect(fs.existsSync(realDir)).toBe(true); }
    finally { await archive.close(); }
  });

  it("does not follow an aliased retirement destination or fall back to copying on EXDEV", async () => {
    for (const failure of ["alias", "EXDEV"]) {
      const root = temporary(), directory = run(root, "kept");
      if (failure === "alias") { const foreign = path.join(root, "foreign"); fs.mkdirSync(foreign); fs.symlinkSync(foreign, path.join(root, "runs-retired")); }
      else vi.spyOn(fsp, "rename").mockRejectedValue(Object.assign(new Error("cross-device"), { code: "EXDEV" }));
      const archive = archiver(root);
      try {
        await sweepArchive(archive, now, 32, 1000); expect(archive.health.archived).toBe(0); expect(fs.existsSync(directory)).toBe(true);
        if (process.platform === "win32") expect(archive.health.error).toBe(""); else expect(archive.health.error).not.toBe("");
      }
      finally { await archive.close(); vi.restoreAllMocks(); }
      if (failure === "alias") expect(fs.readdirSync(path.join(root, "foreign"))).toEqual([]);
    }
  });

  it("rolls back the move when known path-based custody appears after rename", async () => {
    const root = temporary(), directory = run(root, "race"), rename = fsp.rename;
    let retained = false;
    vi.spyOn(fsp, "rename").mockImplementation(async (...args: Parameters<typeof rename>) => { await rename(...args); retained = true; });
    const archive = archiver(root, { isRetained: () => retained });
    try { await sweepArchive(archive, now, 32, 1000); expect(archive.health.archived).toBe(0); expect(fs.existsSync(directory)).toBe(true); expect(retiredRuns(root)).toEqual([]); }
    finally { await archive.close(); }
  });

  it("retains moved bytes on partial failure and resumes with a new collector after restart", async () => {
    const root = temporary(); run(root, "one"); run(root, "two");
    const rename = fsp.rename; let calls = 0;
    vi.spyOn(fsp, "rename").mockImplementation(async (...args: Parameters<typeof rename>) => { if (++calls === 2) throw new Error("interrupted move"); return rename(...args); });
    const archive = archiver(root);
    try {
      await sweepArchive(archive, now, 32, 1000);
      if (process.platform === "win32") { expect(archive.health.error).toBe(""); expect(calls).toBe(0); }
      else { expect(archive.health.error).toContain("interrupted move"); expect(archive.health.archived).toBe(1); }
    }
    finally { await archive.close(); vi.restoreAllMocks(); }
    if (process.platform === "win32") {
      const restarted = archiver(root);
      try { await sweepArchive(restarted, now, 32, 1000); } finally { await restarted.close(); }
      expect(retiredRuns(root)).toEqual([]);
      expect(fs.readdirSync(path.join(root, "runs")).sort()).toEqual(["one", "two"]);
      for (const id of ["one", "two"]) expect(fs.readFileSync(path.join(root, "runs", id, "events.jsonl"))).toEqual(Buffer.from([0, 255, 10, 65, 66]));
      return;
    }
    const first = retiredRuns(root)[0]!; const bytes = fs.readFileSync(path.join(first, "events.jsonl"));
    const restarted = archiver(root); try { await restarted.sweep(now, 32, 1000); expect(restarted.health.error).toBe(""); expect(restarted.health.archived).toBe(1); } finally { await restarted.close(); }
    expect(retiredRuns(root)).toHaveLength(2); expect(fs.readFileSync(path.join(first, "events.jsonl"))).toEqual(bytes);
  });

  it.each(["outbox", "actor-latest", "actor-in-flight", "manager", "admission", "removal", "live-descendant"])("keeps original paths for %s custody", async custody => {
    const root = temporary(), directory = run(root, "held"), actorRoot = path.join(root, "actors");
    fs.mkdirSync(actorRoot, { mode: 0o700 });
    if (custody === "outbox") { fs.mkdirSync(path.join(root, "delivery-outbox")); fs.writeFileSync(path.join(root, "delivery-outbox", "pending.json"), "{}"); }
    if (custody.startsWith("actor-")) fs.writeFileSync(path.join(actorRoot, "actors.json"), JSON.stringify({ actors: [{ id: "actor", ...(custody === "actor-latest" ? { lastRunId: "held" } : { inFlightRun: { id: "held" } }) }] }));
    if (custody === "admission") { fs.mkdirSync(path.join(root, "agents")); fs.writeFileSync(path.join(root, "agents", "held.json"), "{}"); }
    if (custody === "removal") fs.writeFileSync(path.join(actorRoot, "removal-actor.json"), "{}");
    if (custody === "live-descendant") { const child = path.join(directory, "nested", "child"); fs.mkdirSync(child, { recursive: true, mode: 0o700 }); fs.writeFileSync(path.join(child, "status.json"), JSON.stringify({ status: "completed", finishedAt: old, sessionId: String(process.pid) })); }
    const archive = archiver(root, { actorRoots: [actorRoot], isRetained: () => custody === "manager" });
    try { await sweepArchive(archive, now, 32, 1000); expect(fs.existsSync(directory)).toBe(true); expect(archive.health.archived).toBe(0); }
    finally { await archive.close(); }
  });

  it("honors live disable and age policies", async () => {
    const root = temporary(), directory = run(root, "held"), policy = { legacyRunArchiveEnabled: false, legacyRunArchiveAgeMs: 60 * 60 * 1000 };
    const archive = new ResidentLegacyRunArchive(root, policy, { isRetained: () => false });
    try {
      await sweepArchive(archive, now, 32, 1000); expect(fs.existsSync(directory)).toBe(true);
      policy.legacyRunArchiveEnabled = true; policy.legacyRunArchiveAgeMs = 72 * 60 * 60 * 1000;
      await sweepArchive(archive, now, 32, 1000); expect(fs.existsSync(directory)).toBe(true);
    } finally { await archive.close(); }
    const config = normalizeFabricConfig({ retention: { legacyRunArchiveEnabled: false, legacyRunArchiveAgeMs: 3_600_000 } });
    expect(config.retention.legacyRunArchiveEnabled).toBe(false); expect(config.retention.legacyRunArchiveAgeMs).toBe(3_600_000);
  });
});
