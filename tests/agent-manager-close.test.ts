import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { activeBudgetState, appendBudgetLedger, readBudgetLedger } from "../src/agents/budget-ledger.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { hasUnresolvedWorker, runTreeExitVeto } from "../src/storage/retention.js";
import { processAlive } from "../src/storage/scratch.js";

const managers: AgentManager[] = [];
const roots: string[] = [];
const setup = (retainRuns = true, extra: ConstructorParameters<typeof AgentManager>[2] = {}) => {
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "manager-close-test-"));
  roots.push(tempRoot);
  vi.spyOn(os, "tmpdir").mockReturnValue(tempRoot);
  vi.stubEnv("PI_FABRIC_RUN_ROOT", undefined);
  vi.stubEnv("PI_FABRIC_DEPTH", "0");
  for (const key of ["PI_FABRIC_BUDGET", "PI_FABRIC_BUDGET_FILE", "PI_FABRIC_BUDGET_ID"]) vi.stubEnv(key, undefined);
  const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, maxConcurrent: 1, retainRuns, transport: "process", sessionExport: false }, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
    sweepPath: path.resolve("dist/storage/sweep-main.js"), ...extra,
  });
  managers.push(manager);
  const name = fs.readdirSync(tempRoot).find((name) => name.startsWith("pi-fabric-runs-"));
  return { manager, tempRoot, root: name ? path.join(tempRoot, name) : extra.runRoot! };
};
afterEach(async () => {
  await Promise.allSettled(managers.splice(0).map((manager) => manager.close()));
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("AgentManager close storage", () => {
  it("removes empty managed roots immediately, without recreating them on repeated close", async () => {
    const { manager, root } = setup();
    const first = manager.close();
    expect(manager.close()).toBe(first);
    await first;
    expect(fs.existsSync(root)).toBe(false);
    await manager.close();
    expect(fs.existsSync(root)).toBe(false);
    await expect(manager.spawn({ task: "late" })).rejects.toThrow("closing");
  });

  it("honors retainRuns:false for a managed root after stopping a running worker", async () => {
    const { manager, root } = setup(false);
    await manager.spawn({ task: "HANG", runner: "pi", extensions: false });
    expect(fs.readdirSync(root).length).toBeGreaterThan(1);
    await manager.close();
    expect(fs.existsSync(root)).toBe(false);
  });

  it.each(["EBUSY", "EPERM"])("retries transient %s when removing an owned empty root", async (code) => {
    const { manager, root } = setup(false);
    const rmdir = fs.rmdirSync;
    let attempts = 0;
    vi.spyOn(fs, "rmdirSync").mockImplementation((directory) => {
      if (directory === root && attempts++ === 0) {
        expect(fs.readdirSync(root)).toEqual([]);
        throw Object.assign(new Error("Windows directory handle is still closing"), { code });
      }
      return rmdir(directory);
    });
    await manager.close();
    expect(attempts).toBe(2);
    expect(fs.existsSync(root)).toBe(false);
  });

  it("bounds retries for a persistently busy owned empty root", async () => {
    const { manager, root } = setup(false);
    const rmdir = fs.rmdirSync;
    let attempts = 0;
    vi.spyOn(fs, "rmdirSync").mockImplementation((directory) => {
      if (directory === root) {
        attempts++;
        throw Object.assign(new Error("directory remains busy"), { code: "EBUSY" });
      }
      return rmdir(directory);
    });
    const started = Date.now();
    await manager.close();
    expect(Date.now() - started).toBeGreaterThanOrEqual(2_000);
    expect(attempts).toBeGreaterThan(1);
    expect(attempts).toBeLessThanOrEqual(42);
    expect(fs.existsSync(root)).toBe(true);
    expect(fs.readdirSync(root)).toEqual([]);
  });

  it("preserves unknown contents appearing during an empty-root retry", async () => {
    const { manager, root } = setup(false);
    const rmdir = fs.rmdirSync;
    let attempts = 0;
    vi.spyOn(fs, "rmdirSync").mockImplementation((directory) => {
      if (directory === root && attempts++ === 0) {
        fs.writeFileSync(path.join(root, "unrelated"), "not an agent artifact");
        throw Object.assign(new Error("directory handle is still closing"), { code: "EBUSY" });
      }
      return rmdir(directory);
    });
    await manager.close();
    expect(attempts).toBe(2);
    expect(fs.readFileSync(path.join(root, "unrelated"), "utf8")).toBe("not an agent artifact");
  });

  it("retains closed managed run artifacts by default", async () => {
    const { manager, root } = setup(true);
    await manager.run({ task: "hello", runner: "pi", extensions: false });
    await manager.close();
    expect(fs.existsSync(root)).toBe(true);
    expect(JSON.parse(fs.readFileSync(path.join(root, ".fabric-owner.json"), "utf8"))).toMatchObject({ childrenStopped: true, closedAt: expect.any(Number) });
  });

  it("preserves unknown managed-root contents even when deletion was requested", async () => {
    const { manager, root } = setup(false);
    fs.writeFileSync(path.join(root, "unrelated"), "not an agent artifact");
    await manager.close();
    expect(fs.readFileSync(path.join(root, "unrelated"), "utf8")).toBe("not an agent artifact");
  });

  it("does not follow a replaced managed root or a malformed ownership marker", async () => {
    const { manager, root, tempRoot } = setup(false);
    fs.writeFileSync(path.join(root, ".fabric-owner.json"), "{}");
    await manager.close();
    expect(fs.existsSync(root)).toBe(true);
    expect(fs.existsSync(tempRoot)).toBe(true);
  });

  it("does not perform retention scans merely by constructing a manager", async () => {
    const { manager, tempRoot } = setup();
    const sentinel = path.join(tempRoot, "pi-fabric-runs-sentinel");
    fs.mkdirSync(sentinel);
    const marker = { pid: 2147483647, startedAt: 1, heartbeatAt: 1, orphanedAt: 1 };
    fs.writeFileSync(path.join(sentinel, ".fabric-owner.json"), JSON.stringify(marker));
    const second = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 });
    managers.push(second);
    await new Promise((resolve) => setImmediate(resolve));
    expect(fs.existsSync(sentinel)).toBe(true);
    const started = performance.now();
    await manager.close();
    // smarty-dev#2010: close only starts the detached sweep; it never waits for the walk.
    expect(performance.now() - started).toBeLessThan(5_000);
    await vi.waitFor(() => expect(fs.existsSync(sentinel)).toBe(false), { timeout: 20_000 });
  });

  it("starts the close sweep through a real JS runtime under a Bun-compiled Pi (smarty-dev#2010)", async () => {
    const originalExecPath = process.execPath;
    const originalOverride = process.env.PI_FABRIC_NODE_BINARY;
    const { manager, tempRoot } = setup();
    const sentinel = path.join(tempRoot, "pi-fabric-runs-bundled");
    fs.mkdirSync(sentinel);
    fs.writeFileSync(path.join(sentinel, ".fabric-owner.json"), JSON.stringify({ pid: 2147483647, startedAt: 1, heartbeatAt: 1, orphanedAt: 1 }));
    // process.execPath is the Pi executable itself, which cannot run sweep-main.js.
    process.execPath = "/usr/local/bin/pi";
    process.env.PI_FABRIC_NODE_BINARY = originalExecPath;
    try {
      await manager.close();
    } finally {
      process.execPath = originalExecPath;
      if (originalOverride === undefined) delete process.env.PI_FABRIC_NODE_BINARY;
      else process.env.PI_FABRIC_NODE_BINARY = originalOverride;
    }
    await vi.waitFor(() => expect(fs.existsSync(sentinel)).toBe(false), { timeout: 20_000 });
  });

  it("preserves explicit caller roots with retainRuns:true", async () => {
    const caller = fs.mkdtempSync(path.join(os.tmpdir(), "caller-root-"));
    roots.push(caller);
    fs.writeFileSync(path.join(caller, "mine"), "caller data");
    const { manager } = setup(true, { runRoot: caller });
    await manager.close();
    expect(fs.readFileSync(path.join(caller, "mine"), "utf8")).toBe("caller data");
    expect(fs.existsSync(path.join(caller, ".fabric-owner.json"))).toBe(false);
  });

  it.each(["live", "unknown", "exited"] as const)("keeps an evicted parent's owned budget until checked descendant exit (%s)", async (state) => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "manager-close-eviction-"));
    roots.push(tempRoot);
    vi.spyOn(os, "tmpdir").mockReturnValue(tempRoot);
    vi.stubEnv("PI_FABRIC_TMPDIR", undefined);
    vi.stubEnv("PI_FABRIC_RUN_ROOT", undefined);
    vi.stubEnv("PI_FABRIC_DEPTH", "0");
    for (const key of ["PI_FABRIC_BUDGET", "PI_FABRIC_BUDGET_FILE", "PI_FABRIC_BUDGET_ID"]) vi.stubEnv(key, undefined);
    const config = { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 100, maxConcurrent: 1, retainRuns: false, transport: "process" as const, sessionExport: false };
    const workerPath = path.resolve("tests/fixtures/fake-worker.mjs");
    const manager = new AgentManager(process.cwd(), config, { workerPath, runRoot: path.join(tempRoot, "runs") });
    managers.push(manager);
    const budget = activeBudgetState()!;
    expect(budget.budget).toBe(100);
    appendBudgetLedger(budget.file, { id: "existing-spend", depth: 0, cost: 0.25, tokens: 25, ts: Date.now() });
    const parent = await manager.run({ task: "recursive parent", recursive: true });
    const parentDirectory = manager.runDirectory(parent.id)!;
    // A real nested manager inherits the parent's ledger and launches a real worker.
    vi.stubEnv("PI_FABRIC_DEPTH", "1");
    const childManager = new AgentManager(process.cwd(), { ...config, retainRuns: true }, { workerPath, runRoot: path.join(parentDirectory, "nested") });
    managers.push(childManager);
    vi.stubEnv("PI_FABRIC_DEPTH", "0");
    const child = await childManager.spawn({ task: "HANG_WITH_PROGRESS", extensions: false });
    const childDirectory = childManager.runDirectory(child.id)!;
    const childStatus = path.join(childDirectory, "status.json");
    await vi.waitFor(() => expect(childManager.status(child.id)).toMatchObject({ status: "running", turns: 3 }), { timeout: 5_000 });
    const pid = Number(child.sessionId);
    expect(processAlive(pid)).toBe(true);
    // Persist the real transport identity, as production workers do; UNKNOWN is
    // an ordinary readable record with no identity and no unresolved marker.
    const record = JSON.parse(fs.readFileSync(childStatus, "utf8"));
    const { sessionId: _savedPid, ...withoutIdentity } = record;
    fs.writeFileSync(childStatus, JSON.stringify(state === "unknown" ? withoutIdentity : { ...record, sessionId: String(pid) }));

    // Exercise actual registration, settlement, and #pruneRetainedUiRecords:
    // only pressure-run execution is substituted, never #runs or its handle cap.
    // Both the handle and persisted record need an absent identity: settlement
    // alone cannot release the budget after these handles have been evicted.
    const absentPid = "2147483647";
    expect(processAlive(Number(absentPid))).toBe(false);
    const launch = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async (request) => {
      const arg = (flag: string) => request.workerArguments[request.workerArguments.indexOf(flag) + 1]!;
      fs.writeFileSync(arg("--status-file"), JSON.stringify({
        id: request.id, name: request.name, task: "eviction pressure", status: "completed", runner: "pi", transport: "process", sessionId: absentPid,
        cwd: request.cwd, startedAt: Date.now(), updatedAt: Date.now(), finishedAt: Date.now(), text: "done", turns: 1, toolCalls: 0,
        exitCode: 0, usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
      }));
      return { kind: "process", sessionId: absentPid, isAlive: async () => false, stop: async () => {} };
    });
    try {
      for (let index = 0; index < 1_000; index++) {
        expect((await manager.run({ task: `eviction pressure ${index}`, extensions: false })).status).toBe("completed");
      }
    } finally { launch.mockRestore(); }
    expect(manager.runDirectory(parent.id), "settled parent handle was really evicted").toBeUndefined();
    expect(processAlive(Number(parent.sessionId)), "evicted primary process has exited").toBe(false);
    expect(fs.existsSync(parentDirectory)).toBe(true);
    expect(hasUnresolvedWorker(parentDirectory)).toBe(false);
    if (state === "exited") {
      await childManager.stop(child.id);
      expect(processAlive(pid), "checked child process exit").toBe(false);
      expect(runTreeExitVeto(parentDirectory, 0, undefined, true)).toBeUndefined();
    } else {
      expect(runTreeExitVeto(parentDirectory, 0, undefined, true)).toMatch(state === "unknown" ? /unknown descendant identity/ : /descendant worker may still be running/);
    }
    const accounting = fs.readFileSync(budget.file, "utf8");
    expect(readBudgetLedger(budget.file).cost).toBeGreaterThanOrEqual(0.25);
    await manager.close();
    if (state === "exited") {
      expect(fs.existsSync(path.dirname(budget.file)), "checked descendant exit releases owned budget").toBe(false);
    } else {
      expect(fs.existsSync(budget.file), "evicted live/unknown descendant still owns the ledger").toBe(true);
      expect(fs.readFileSync(budget.file, "utf8")).toBe(accounting);
      const before = readBudgetLedger(budget.file);
      await childManager.stop(child.id);
      expect(processAlive(pid)).toBe(false);
      expect(readBudgetLedger(budget.file).cost, "descendant can still append its final accounting after parent close").toBeGreaterThan(before.cost);
    }
  }, 30_000);

  it("cancels queued admissions on close instead of launching after shutdown", async () => {
    const { manager, root } = setup(false);
    await manager.spawn({ task: "HANG", extensions: false });
    const queued = await manager.spawn({ task: "queued", extensions: false });
    expect(queued.status).toBe("queued");
    await manager.close();
    await expect(manager.wait(queued.id)).resolves.toMatchObject({ status: "stopped" });
    expect(fs.existsSync(root)).toBe(false);
  });

  it("waits for a pending spawn to observe close before deleting the root", async () => {
    let resume!: () => void;
    const gate = new Promise<void>((resolve) => { resume = resolve; });
    const { manager, root } = setup(false, { preparePiModel: async () => { await gate; } });
    const spawning = manager.spawn({ task: "hello" });
    const rejected = expect(spawning).rejects.toThrow("closing");
    await Promise.resolve();
    const closed = manager.close();
    resume();
    await Promise.all([rejected, closed]);
    expect(fs.existsSync(root)).toBe(false);
  });
});
