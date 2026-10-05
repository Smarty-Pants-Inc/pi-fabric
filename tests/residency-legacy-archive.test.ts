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
const emptyProc = (root: string) => {
  const dir = path.join(root, "proc"); fs.mkdirSync(dir, { mode: 0o700 });
  // An isolated complete view includes the same topology evidence as production.
  // Only the filesystem type syscall is mocked; mount/namespace checks run in full.
  fs.mkdirSync(path.join(dir, "self", "ns"), { recursive: true });
  fs.mkdirSync(path.join(dir, "1", "ns"), { recursive: true });
  for (const [name, value] of Object.entries({ pid: "pid:[4026531836]", user: "user:[4026531837]", cgroup: "cgroup:[4026531835]", mnt: "mnt:[4026531841]" })) {
    fs.symlinkSync(value, path.join(dir, "self", "ns", name));
  }
  fs.symlinkSync("mnt:[4026531841]", path.join(dir, "1", "ns", "mnt"));
  fs.writeFileSync(path.join(dir, "self", "mountinfo"), `26 31 0:24 / ${dir} rw - proc proc rw\n`);
  const stat = `1 (init) ${["Z", ...Array(18).fill("0"), "100"].join(" ")}`;
  fs.writeFileSync(path.join(dir, "1", "stat"), stat);
  fs.mkdirSync(path.join(dir, "1", "task", "1"), { recursive: true });
  fs.writeFileSync(path.join(dir, "1", "task", "1", "stat"), stat);
  const statfs = fsp.statfs;
  vi.spyOn(fsp, "statfs").mockImplementation((...args: Parameters<typeof statfs>) => {
    if (String(args[0]) === dir) return Promise.resolve({ type: 0x9fa0 } as Awaited<ReturnType<typeof statfs>>);
    return statfs(...args);
  });
  return dir;
};
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
    const reads = new Map<string, number>(), original = fs.readFileSync;
    fs.readFileSync = ((...args: Parameters<typeof original>) => {
      const file = String(args[0]);
      if (file.startsWith(path.join(root, "runs") + path.sep) && path.basename(file) === "status.json") reads.set(file, (reads.get(file) ?? 0) + 1);
      return original(...args);
    }) as typeof original;
    try {
      expect(manager.retentionReferences({ now, budgetMs: 100, maxEntries: 64 }).has("*")).toBe(true);
      const prefixReads = [...reads.values()].reduce((sum, count) => sum + count, 0);
      let complete = false;
      for (let slice = 0; slice < 16; slice++) {
        if (!manager.retentionReferences({ now: now + 60_001, budgetMs: 100, maxEntries: 64 }).has("*")) { complete = true; break; }
      }
      expect(complete).toBe(true); expect(reads.size).toBe(256);
      // Status + persisted exit + removability proof: exactly five reads per
      // run, including the prefix inspected before the interval boundary.
      expect(prefixReads).toBe(64 * 5);
      expect([...reads.values()].every(count => count === 5)).toBe(true);
    } finally { fs.readFileSync = original; await manager.close(); }
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
      if (String(args[0]).startsWith(path.join(root, "runs") + path.sep) && path.basename(String(args[0])) === "status.json") reads++;
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

it("Windows has no /proc archival proof and safely leaves every legacy byte in place", async () => {
  const nativePlatform = Object.getOwnPropertyDescriptor(process, "platform")!;
  const root = temporary(), directory = run(root, "windows-retained"), before = fs.readFileSync(path.join(directory, "events.jsonl"));
  const idle = vi.fn(async () => true);
  Object.defineProperty(process, "platform", { ...nativePlatform, value: "win32" });
  const archive = new ResidentLegacyRunArchive(root, {}, { isRetained: () => false, processFilesIdle: idle });
  try {
    expect(await linuxRunFilesIdle([directory], path.join(root, "absent-proc"))).toBe(false);
    await archive.sweep(now, 32, 1000);
    expect(idle).not.toHaveBeenCalled(); expect(archive.health.archived).toBe(0);
    expect(fs.readFileSync(path.join(directory, "events.jsonl"))).toEqual(before);
    expect(fs.existsSync(path.join(root, "archive"))).toBe(false);
  } finally { await archive.close(); Object.defineProperty(process, "platform", nativePlatform); }
});

describe.skipIf(process.platform !== "linux")("legacy run archive proof", () => {
  it.each(["hidepid=1", "hidepid=2", "hidepid=4", "hidepid=ptraceable", "subset=pid"])("vetoes invisible file custody in a filtered procfs view (%s)", async option => {
    const root = temporary(), dir = run(root, "invisible-holder"), proc = emptyProc(root);
    // The holder is omitted entirely, not a visible PID returning EACCES.
    const fd = fs.openSync(path.join(dir, "events.jsonl"), "a");
    const mountinfo = path.join(proc, "self", "mountinfo");
    fs.writeFileSync(mountinfo, `26 31 0:24 / ${proc} rw - proc proc rw,${option}\n`);
    const archive = archiver(root, proc);
    try {
      expect(fs.readdirSync(proc).filter(name => /^\d+$/.test(name))).toEqual(["1"]);
      expect(await linuxRunFilesIdle([], proc)).toBe(false);
      expect(await linuxRunFilesIdle([dir], proc)).toBe(false);
      await archive.sweep(now, 32, 1000);
      expect(archive.health.archived).toBe(0); expect(archive.health.processProofIncomplete).toBe(1);
      expect(fs.existsSync(dir)).toBe(true); expect(fs.existsSync(path.join(root, "archive"))).toBe(false);
      fs.writeSync(fd, "invisible writer retained\n");
      expect(fs.readFileSync(path.join(dir, "events.jsonl"), "utf8")).toContain("invisible writer retained");
    } finally { fs.closeSync(fd); await archive.close(); }
  });

  it.each(["pid", "user", "cgroup", "mnt"])("vetoes a container/restricted %s namespace despite a stable visible census", async namespace => {
    const root = temporary(), dir = run(root, "namespace-held"), proc = emptyProc(root);
    const link = path.join(proc, "self", "ns", namespace);
    fs.unlinkSync(link); fs.symlinkSync(`${namespace}:[999999]`, link);
    expect(await linuxRunFilesIdle([dir], proc)).toBe(false);
  });

  it("fences a procfs view that becomes filtered between task censuses", async () => {
    const root = temporary(), dir = run(root, "view-changed"), proc = emptyProc(root);
    const open = fsp.opendir;
    let censuses = 0;
    vi.spyOn(fsp, "opendir").mockImplementation((...args: Parameters<typeof open>) => {
      if (String(args[0]) === proc && ++censuses === 2) {
        fs.writeFileSync(path.join(proc, "self", "mountinfo"), `26 31 0:24 / ${proc} rw - proc proc rw,hidepid=2\n`);
      }
      return open(...args);
    });
    expect(await linuxRunFilesIdle([dir], proc)).toBe(false); expect(censuses).toBe(2);
  });

  it("inspects every live task and every fd, including a private table after an unrelated descriptor", async () => {
    const root = temporary(), dir = run(root, "task-private-fd"), proc = emptyProc(root);
    const group = path.join(proc, "123"), stat = (id: number) => `${id} (fixture) ${["S", ...Array(18).fill("0"), "100"].join(" ")}`;
    fs.mkdirSync(group); fs.writeFileSync(path.join(group, "stat"), stat(123));
    for (const tid of [123, 124]) {
      const task = path.join(group, "task", String(tid)); fs.mkdirSync(path.join(task, "fd"), { recursive: true });
      fs.writeFileSync(path.join(task, "stat"), stat(tid)); fs.writeFileSync(path.join(task, "maps"), "");
      fs.symlinkSync(root, path.join(task, "cwd")); fs.symlinkSync("/", path.join(task, "root"));
      fs.symlinkSync(path.join(root, "unrelated"), path.join(task, "fd", "0"));
    }
    expect(await linuxRunFilesIdle([dir], proc)).toBe(true);
    fs.symlinkSync(path.join(dir, "events.jsonl"), path.join(group, "task", "124", "fd", "9"));
    expect(await linuxRunFilesIdle([dir], proc)).toBe(false);
    const archive = archiver(root, proc);
    try { await archive.sweep(now, 32, 1000); expect(archive.health.archived).toBe(0); expect(fs.existsSync(dir)).toBe(true); }
    finally { await archive.close(); }
  });

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

  it("a zombie leader with a surviving task is held, even when the leader fd view is unavailable", async () => {
    const root = temporary(), dir = run(root, "thread-held"), proc = emptyProc(root);
    const pid = path.join(proc, "123"), task = path.join(pid, "task", "124");
    fs.mkdirSync(task, { recursive: true });
    const stat = (id: number, state: string) => `${id} (fixture) ${[state, ...Array(18).fill("0"), "100"].join(" ")}`;
    fs.writeFileSync(path.join(pid, "stat"), stat(123, "Z"));
    fs.writeFileSync(path.join(task, "stat"), stat(124, "S"));
    fs.mkdirSync(path.join(task, "fd")); fs.symlinkSync(path.join(dir, "events.jsonl"), path.join(task, "fd", "3"));
    expect(await linuxRunFilesIdle([dir], proc)).toBe(false);
    const archive = archiver(root, proc);
    try { await archive.sweep(now, 32, 1000); expect(archive.health.archived).toBe(0); expect(fs.existsSync(dir)).toBe(true); }
    finally { await archive.close(); }
  });

  it("keeps a real pthread_exit zombie-leader run until the surviving file-holder is joined", async () => {
    // The synthetic task regression above is unconditional on Linux. Exercise
    // the real kernel state too wherever a C compiler is installed.
    try { execFileSync("cc", ["--version"], { stdio: "ignore", timeout: 5000 }); } catch { return; }
    const root = temporary(), dir = run(root, "pthread-held"), proc = emptyProc(root), binary = path.join(root, "holder");
    execFileSync("cc", ["-pthread", fileURLToPath(new URL("./fixtures/zombie-leader.c", import.meta.url)), "-o", binary], { timeout: 10_000 });
    const child = spawn(binary, [path.join(dir, "events.jsonl")], { stdio: "ignore" });
    const closed = new Promise<void>((resolve, reject) => { child.once("error", reject); child.once("exit", () => resolve()); });
    try {
      expect(child.pid).toBeDefined();
      fs.symlinkSync(`/proc/${child.pid}`, path.join(proc, String(child.pid)));
      let states: string[] = [];
      for (let retry = 0; retry < 200; retry++) {
        states = fs.readdirSync(`/proc/${child.pid}/task`).map(tid => fs.readFileSync(`/proc/${child.pid}/task/${tid}/stat`, "utf8").split(") ")[1]!.split(" ")[0]!);
        if (states.includes("Z") && states.some(state => !["Z", "X"].includes(state))) break;
        await new Promise(resolve => setTimeout(resolve, 10));
      }
      expect(states).toContain("Z"); expect(states.some(state => !["Z", "X"].includes(state))).toBe(true);
      expect(await linuxRunFilesIdle([dir], proc)).toBe(false);
      const archive = archiver(root, proc);
      try { await archive.sweep(now, 32, 1000); expect(archive.health.archived).toBe(0); expect(fs.existsSync(dir)).toBe(true); }
      finally { await archive.close(); }
    } finally { child.kill("SIGKILL"); await closed; }
    expect(await linuxRunFilesIdle([dir], proc)).toBe(true);
  });

  it.each(["residency", "mesh", "runs"])("vetoes a %s symlink ancestor with a real held descriptor before staging", async kind => {
    const sandbox = temporary(), physical = path.join(sandbox, "physical");
    fs.mkdirSync(physical, { mode: 0o700 });
    const realRoot = kind === "mesh" ? path.join(physical, "residency", "host") : physical;
    const realDir = run(realRoot, "aliased-held"), proc = emptyProc(sandbox);
    fs.symlinkSync(`/proc/${process.pid}`, path.join(proc, String(process.pid)));
    const view = path.join(sandbox, "view"); fs.symlinkSync(physical, view);
    let root = kind === "mesh" ? path.join(view, "residency", "host") : view;
    if (kind === "runs") {
      root = path.join(sandbox, "resident"); fs.mkdirSync(root, { mode: 0o700 });
      fs.symlinkSync(path.join(physical, "runs"), path.join(root, "runs"));
    }
    const dir = path.join(root, "runs", "aliased-held"), file = path.join(dir, "events.jsonl");
    const fd = fs.openSync(file, "a"), archive = archiver(root, proc);
    try {
      expect(fs.readlinkSync(`/proc/self/fd/${fd}`)).toBe(path.join(realDir, "events.jsonl"));
      expect(fs.fstatSync(fd).ino).toBe(fs.statSync(file).ino);
      expect(await linuxRunFilesIdle([dir], proc)).toBe(false);
      // The veto is the namespace, not restricted /proc visibility or PID death.
      expect(await linuxRunFilesIdle([dir], emptyProc(realRoot))).toBe(false);
      expect(await legacyRunTreeProof(dir, now, 0)).toBeUndefined();
      await archive.sweep(now, 32, 1000);
      expect(archive.health.archived).toBe(0); expect(fs.existsSync(dir)).toBe(true);
      expect(fs.existsSync(path.join(root, "archive"))).toBe(false);
      fs.writeSync(fd, "still held\n"); expect(fs.readFileSync(file, "utf8")).toContain("still held");
    } finally { fs.closeSync(fd); await archive.close(); }
  });

  it("vetoes the staged alias while an original-path descriptor follows the renamed inode", async () => {
    const sandbox = temporary(), physical = path.join(sandbox, "physical"); fs.mkdirSync(physical, { mode: 0o700 });
    const view = path.join(sandbox, "view"); fs.symlinkSync(physical, view);
    const dir = run(view, "held"), proc = emptyProc(sandbox);
    fs.symlinkSync(`/proc/${process.pid}`, path.join(proc, String(process.pid)));
    const batch = path.join(view, "archive", ".staging-probe"); fs.mkdirSync(batch, { recursive: true, mode: 0o700 });
    const fd = fs.openSync(path.join(dir, "events.jsonl"), "a");
    try {
      const staged = path.join(batch, "held"); fs.renameSync(dir, staged);
      expect(fs.readlinkSync(`/proc/self/fd/${fd}`)).toBe(path.join(physical, "archive", ".staging-probe", "held", "events.jsonl"));
      expect(fs.fstatSync(fd).ino).toBe(fs.statSync(path.join(staged, "events.jsonl")).ino);
      expect(await linuxRunFilesIdle([staged], proc)).toBe(false);
      expect(await linuxRunFilesIdle([staged], emptyProc(physical))).toBe(false);
      const archive = archiver(view, proc);
      try { await archive.sweep(now, 32, 1000); expect(archive.health.archived).toBe(0); expect(fs.existsSync(staged)).toBe(true); }
      finally { await archive.close(); }
      fs.writeSync(fd, "after staging\n"); expect(fs.readFileSync(path.join(staged, "events.jsonl"), "utf8")).toContain("after staging");
    } finally { fs.closeSync(fd); }
  });

  it.each([2, 3, 4])("keeps a real file holder when archive becomes aliased at custody boundary %i", async boundary => {
    const root = temporary(), dir = run(root, "held"), proc = emptyProc(root);
    fs.symlinkSync(`/proc/${process.pid}`, path.join(proc, String(process.pid)));
    let calls = 0, fd: number | undefined, heldFile = "", observedIdle: boolean | undefined;
    const archive = archiver(root, proc, { processFilesIdle: async (dirs: readonly string[]) => {
      if (++calls === boundary) {
        const original = path.join(root, "archive"), physical = path.join(root, "physical-archive");
        fs.renameSync(original, physical); fs.symlinkSync(physical, original);
        heldFile = path.join(dirs[0]!, "events.jsonl"); fd = fs.openSync(heldFile, "a");
        expect(fs.readlinkSync(`/proc/self/fd/${fd}`)).toContain("/physical-archive/");
        observedIdle = await linuxRunFilesIdle(dirs, proc);
        // Even a custom observer claiming idleness cannot bypass the post-I/O veto.
        return true;
      }
      return linuxRunFilesIdle(dirs, proc);
    } });
    try {
      await archive.sweep(now, 32, 1000);
      expect(archive.health.archived).toBe(0); expect(observedIdle).toBe(false);
      expect(calls).toBe(boundary); expect(fd).toBeDefined();
      const retained = boundary === 2 ? path.join(dir, "events.jsonl") : heldFile;
      expect(fs.existsSync(retained)).toBe(true);
      fs.writeSync(fd!, "retained writer\n"); expect(fs.readFileSync(retained, "utf8")).toContain("retained writer");
      if (boundary > 2) expect(archive.health.error).toMatch(/proof changed/);
    } finally { if (fd !== undefined) fs.closeSync(fd); await archive.close(); }
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
