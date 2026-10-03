import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";

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
  it.each(["before-close", "during-drain"] as const)("joins a child settled %s without foreground consumption, but real wait still consumes", async (timing) => {
    const fence = vi.fn();
    const consumed = vi.fn();
    const settled = vi.fn();
    const stoppedAtClose = vi.fn();
    const { manager } = setup(true, {
      onBeforeResultReturned: fence, onResultConsumed: consumed,
      onSettled: settled, onStoppedAtClose: stoppedAtClose,
    });
    let release!: () => void;
    const drain = new Promise<void>((resolve) => { release = resolve; });
    let alive = true;
    let statusFile = "";
    const publish = () => {
      const record = JSON.parse(fs.readFileSync(statusFile, "utf8"));
      fs.writeFileSync(statusFile, JSON.stringify({ ...record, status: "completed", finishedAt: Date.now(),
        updatedAt: Date.now(), text: "unread full outcome", value: { private: "structured outcome" } }));
      alive = false;
    };
    const stop = vi.fn(async () => {
      if (timing === "during-drain") { publish(); await drain; }
      alive = false;
    });
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async (request) => {
      statusFile = request.workerArguments[request.workerArguments.indexOf("--status-file") + 1]!;
      fs.writeFileSync(statusFile, JSON.stringify({ id: request.id, name: request.name, task: "unread", status: "running",
        runner: "pi", transport: "process", cwd: request.cwd, startedAt: Date.now(), updatedAt: Date.now(),
        turns: 1, toolCalls: 0, text: "", usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } }));
      return { kind: "process", isAlive: async () => alive, stop };
    });
    const child = await manager.spawn({ task: "unread", transport: "process", extensions: false });
    if (timing === "before-close") { publish(); await manager.join(child.id); }
    const statusReads = vi.spyOn(fs, "readFileSync");
    const closing = manager.close();
    try {
      if (timing === "during-drain") {
        await vi.waitFor(() => {
          expect(stop).toHaveBeenCalledOnce();
          // Both stop's snapshot and the monitor must see the terminal record
          // before the exact transport receipt releases either continuation.
          expect(statusReads.mock.calls.filter(([file]) => file === statusFile).length).toBeGreaterThanOrEqual(3);
        });
        release();
      }
      await closing;
    } finally { release(); await closing; statusReads.mockRestore(); }
    expect(stop).toHaveBeenCalledOnce();
    expect(settled).toHaveBeenCalledOnce();
    expect(fence).not.toHaveBeenCalled();
    expect(consumed).not.toHaveBeenCalled();
    if (timing === "before-close") expect(stoppedAtClose).not.toHaveBeenCalled();
    await expect(manager.wait(child.id)).resolves.toMatchObject({
      status: "completed", text: "unread full outcome", value: { private: "structured outcome" },
    });
    expect(fence).toHaveBeenCalledExactlyOnceWith(child.id);
    expect(consumed).toHaveBeenCalledExactlyOnceWith(child.id);
  });

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
