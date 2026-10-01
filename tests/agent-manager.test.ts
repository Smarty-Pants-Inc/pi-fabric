import { execFileSync, spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { SessionManager } from "@earendil-works/pi-coding-agent";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import type { FabricLifecyclePublishRequest } from "../src/lifecycle/types.js";
import { snapshotHandoffSession } from "../src/agents/handoff.js";
import {
  effectiveAgentTimeoutMs,
  AgentManager,
} from "../src/agents/manager.js";
import { markUnresolvedWorker } from "../src/storage/retention.js";
import * as retentionStorage from "../src/storage/retention.js";
import { writeJsonAtomic } from "../src/core/atomic-write.js";
import {
  clearOwnedBudgetEnv,
  readBudgetLedgerDetailed,
} from "../src/agents/budget-ledger.js";
import type { AgentRunRecord, AgentRunResult } from "../src/agents/types.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";

const managers: AgentManager[] = [];
const roots: string[] = [];
type FabricSurfaceResult = AgentRunResult & {
  fullCodeMode?: string;
  tools?: string[];
  extensions?: string;
  mainAgentId?: string;
  fabricExtension?: string;
  grantedRisks?: string[];
};
const handoffSeed = (fact = "Rare handoff fact 43117") => {
  const source = SessionManager.inMemory();
  source.appendMessage({ role: "user", content: fact, timestamp: 1 });
  source.appendModelChange("anthropic", "frontier");
  source.appendThinkingLevelChange("high");
  source.appendMessage({
    role: "assistant",
    content: [
      { type: "text", text: "Ready to continue from the fork." },
      {
        type: "toolCall",
        id: "outer-manager-handoff",
        name: "fabric_exec",
        arguments: { code: "return agents.handoff(...)" },
      },
    ],
    api: "anthropic",
    provider: "anthropic",
    model: "frontier",
    usage: {
      input: 0,
      output: 0,
      cacheRead: 0,
      cacheWrite: 0,
      totalTokens: 0,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
    },
    stopReason: "toolUse",
    timestamp: 2,
  });
  return snapshotHandoffSession(
    source,
    { provider: "anthropic", id: "frontier" },
    {
      role: "toolResult",
      toolCallId: "outer-manager-handoff",
      toolName: "fabric_exec",
      content: [{ type: "text", text: "Manager boundary complete" }],
      details: { success: true },
      isError: false,
      timestamp: 3,
    },
    "outer-manager-handoff",
  );
};
const fabricEnvKeys = [
  "PI_FABRIC_DEPTH",
  "PI_FABRIC_BUDGET",
  "PI_FABRIC_BUDGET_FILE",
  "PI_FABRIC_BUDGET_ID",
] as const;
const inheritedFabricEnv = new Map(
  fabricEnvKeys.map((key) => [key, process.env[key]]),
);

beforeAll(() => {
  for (const key of fabricEnvKeys) delete process.env[key];
});

afterAll(() => {
  for (const [key, value] of inheritedFabricEnv) {
    if (value === undefined) delete process.env[key];
    else process.env[key] = value;
  }
});

describe("effectiveAgentTimeoutMs", () => {
  it("ignores per-call timeouts below the configured default", () => {
    expect(effectiveAgentTimeoutMs(3_600_000, 240_000)).toBe(3_600_000);
  });

  it("accepts per-call timeouts above the configured default", () => {
    expect(effectiveAgentTimeoutMs(3_600_000, 7_200_000)).toBe(7_200_000);
  });

  it("respects a configured default below 60 minutes", () => {
    expect(effectiveAgentTimeoutMs(1_800_000, 900_000)).toBe(1_800_000);
    expect(effectiveAgentTimeoutMs(1_800_000, 2_400_000)).toBe(2_400_000);
  });
});

afterEach(async () => {
  await Promise.all(managers.splice(0).map((manager) => manager.close()));
  // ponytail: on Windows a just-exited child can still hold its cwd (EBUSY on rmdir);
  // Node retries EBUSY/ENOTEMPTY/EPERM with backoff when maxRetries is set (smarty-dev#883).
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
});

describe("AgentManager fleet model admission (#2490)", () => {
  it("round 3 F3 rejects unknown Veda defaults before queue admission and preserves allowed configured default", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-veda-default-policy-")); roots.push(root);
    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, runner: "veda", deniedModels: ["cliproxyapi/gpt-6-astra"], budgetUsd: 0,
      veda: { binary: DEFAULT_FABRIC_CONFIG.agents.veda.binary, persona: DEFAULT_FABRIC_CONFIG.agents.veda.persona, backend: "pi" },
    }, { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: root }); managers.push(manager);
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    try {
      await expect(manager.spawn({ task: "backend default must not launch" })).rejects.toMatchObject({ name: "FabricModelDeniedError", code: "FABRIC_MODEL_DENIED" });
      expect(manager.list()).toEqual([]); expect(launch).not.toHaveBeenCalled();
      expect(fs.readdirSync(root).filter(entry => fs.statSync(path.join(root, entry)).isDirectory())).toEqual([]);
      manager.config.veda.model = "veda/cliproxyapi/gpt-6.1-sol";
      expect((await manager.run({ task: "allowed configured default", transport: "process" })).status).toBe("completed");
      expect(launch).toHaveBeenCalledOnce();
    } finally { launch.mockRestore(); }
  });

  it.each([
    ["veda", "veda/cliproxyapi/gpt-6-astra", "cliproxyapi/gpt-6-astra"],
    ["claude", "claude/denied-backend", "denied-backend"],
    ["claude", "anthropic/denied-backend", "denied-backend"],
  ] as const)("review round F1 denies normalized %s selector %s before queue admission", async (runner, selector, denied) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-policy-runner-"));
    roots.push(root);
    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, runner, deniedModels: [denied], budgetUsd: 0, maxConcurrent: 1,
      claude: { ...DEFAULT_FABRIC_CONFIG.agents.claude, ...(runner === "claude" ? { model: selector } : {}) },
      veda: { ...DEFAULT_FABRIC_CONFIG.agents.veda, backend: "pi", ...(runner === "veda" ? { model: selector } : {}) },
    }, { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: root });
    managers.push(manager);
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    try {
      for (const model of [selector, undefined]) {
        await expect(manager.spawn({ task: "HANG", ...(model ? { model } : {}) })).rejects.toMatchObject({ code: "FABRIC_MODEL_DENIED" });
      }
      expect(manager.list()).toEqual([]);
      expect(launch).not.toHaveBeenCalled();
      expect(fs.readdirSync(root).filter((entry) => fs.statSync(path.join(root, entry)).isDirectory())).toEqual([]);
      const allowed = await manager.run({ task: "allowed control", model: runner === "veda" ? "veda/cliproxyapi/gpt-6.1-sol" : "claude/allowed-backend", transport: "process" });
      expect(allowed.status).toBe("completed");
      expect(launch).toHaveBeenCalledTimes(1);
    } finally { launch.mockRestore(); }
  });
  it.each(["explicit", "default", "resolved"])("refuses a denied %s before creating a child", async (source) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-policy-"));
    roots.push(root);
    const denied = "cliproxyapi/gpt-6-astra";
    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, ...(source === "default" ? { model: denied } : {}), deniedModels: [denied], deniedModelReplacement: "cliproxyapi/gpt-6.1-sol" }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: root,
      preparePiModel: async (model) => source === "resolved" ? denied : model,
    });
    managers.push(manager);
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    try {
      await expect(manager.spawn({ task: "review", ...(source === "explicit" ? { model: " CLIPROXYAPI/GPT-6-ASTRA " } : {}) })).rejects.toMatchObject({ name: "FabricModelDeniedError", code: "FABRIC_MODEL_DENIED", model: denied, replacement: "cliproxyapi/gpt-6.1-sol" });
      expect(manager.list()).toEqual([]);
      expect(launch).not.toHaveBeenCalled();
      expect(fs.readdirSync(root).filter((entry) => fs.statSync(path.join(root, entry)).isDirectory())).toEqual([]);
    } finally { launch.mockRestore(); }
  });
});

describe("AgentManager", () => {
  it("F1 tracked retention retries the full failed save before collection, without pinning session or actor runs", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-save-fault-"));
    roots.push(root);
    let sweep: (() => void) | undefined;
    const interval = globalThis.setInterval;
    const timer = vi.spyOn(globalThis, "setInterval").mockImplementation(((callback: () => void, ms?: number, ...args: unknown[]) => {
      if (ms === 15 * 60 * 1_000) sweep = callback;
      return interval(callback, ms, ...args);
    }) as typeof setInterval);
    // This probe owns one manager, not the host-global detached temp-directory sweep.
    const detachedSweep = vi.spyOn(retentionStorage, "claimTempRunSweep").mockReturnValue(false);
    let unblocked = false;
    let clock: ReturnType<typeof vi.spyOn> | undefined;
    const save = vi.fn((result: AgentRunResult) => {
      if (result.task !== "LARGE_RESULT") return; // only the public durable task uses this result store
      const file = path.join(root, `${result.id}.json`);
      if (!unblocked) fs.mkdirSync(file, { recursive: true });
      writeJsonAtomic(file, result);
    });
    const inheritedRunRoot = process.env.PI_FABRIC_RUN_ROOT;
    delete process.env.PI_FABRIC_RUN_ROOT; // task-agent nesting must not disable the managed-root timer
    let manager: AgentManager;
    try {
      manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: false, budgetUsd: 0 }, {
        workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
        retention: { ...DEFAULT_FABRIC_CONFIG.retention, oneShotRunMs: 1_000 }, onSettled: save,
      });
    } finally {
      if (inheritedRunRoot !== undefined) process.env.PI_FABRIC_RUN_ROOT = inheritedRunRoot;
    }
    managers.push(manager);
    try {
      const result = await manager.run({ task: "LARGE_RESULT", residency: "durable", transport: "process" });
      const run = manager.runDirectory(result.id)!;
      roots.push(path.dirname(run));
      const worker = fs.readFileSync(path.join(run, "status.json"), "utf8");
      expect(result).toMatchObject({ status: "completed", text: "x".repeat(100_000), value: { output: "x".repeat(100_000) } });
      const ordinary = await manager.run({ task: "ordinary session", transport: "process" });
      const ordinaryRun = manager.runDirectory(ordinary.id)!;
      const actor = await manager.run({ task: "actor activation", residency: "durable", actorId: "actor:fixture", transport: "process" });
      const actorRun = manager.runDirectory(actor.id)!;
      expect(sweep).toBeTypeOf("function");
      // Advance only the manager's clock after real subprocess completion; do not fabricate status.
      clock = vi.spyOn(Date, "now").mockReturnValue(Date.now() + 2_000);
      const attempts = save.mock.calls.length;
      sweep!();
      await vi.waitFor(() => expect(fs.existsSync(ordinaryRun)).toBe(false), { timeout: 2_000 });
      expect(fs.existsSync(run), "unsaved completion stays tracked").toBe(true);
      expect(save.mock.calls.length).toBeGreaterThan(attempts);
      expect(fs.readFileSync(path.join(run, "status.json"), "utf8")).toBe(worker);
      await expect(manager.cleanup(result.id)).rejects.toThrow(/Cannot clean up agent.*Terminal result save failed/);
      expect(fs.readFileSync(path.join(run, "status.json"), "utf8")).toBe(worker);
      expect(result.warnings).toEqual(expect.arrayContaining([expect.stringMatching(/save failed.*retained/i)]));
      expect(result.warnings!.join(" ")).toContain(`${result.id}.json`);
      expect(fs.existsSync(actorRun), "retention still leaves activation cleanup to its actor").toBe(true);
      expect((await manager.wait(result.id)).text).toHaveLength(100_000);
      // Fix the native obstruction. The next sweep must retry the original, non-UI-truncated result.
      fs.rmSync(path.join(root, `${result.id}.json`), { recursive: true });
      unblocked = true;
      sweep!();
      await vi.waitFor(() => expect(fs.existsSync(run)).toBe(false), { timeout: 2_000 });
      const saved = JSON.parse(fs.readFileSync(path.join(root, `${result.id}.json`), "utf8"));
      expect(saved).toMatchObject({ id: result.id, status: "completed", text: "x".repeat(100_000), value: { output: "x".repeat(100_000) } });
      expect(saved.warnings).toBeUndefined(); // a failed save must not corrupt the original completion
      await manager.close();
      expect(fs.existsSync(actorRun), "ordinary close can still collect a finished actor activation").toBe(false);
    } finally {
      clock?.mockRestore();
      await manager.close(); // keep the host-global sweep disabled even on an assertion failure
      timer.mockRestore();
      detachedSweep.mockRestore();
    }
  }, 15_000);

  it.each([["cleanup", "durable"], ["close", "session"]] as const)("F1 %s retries a thrown save with the original full completion before collection (%s request)", async (collection, residency) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-save-throw-"));
    roots.push(root);
    let blocked = true;
    const savedPath = path.join(root, "result.json");
    const save = vi.fn((result: AgentRunResult) => {
      if (result.task !== "LARGE_RESULT") return;
      if (blocked) throw new Error(`EIO writing ${savedPath}`);
      writeJsonAtomic(savedPath, result);
    });
    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: false, budgetUsd: 0 }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"), onSettled: save,
    });
    managers.push(manager);
    const result = await manager.run({ task: "LARGE_RESULT", residency, transport: "process" });
    const run = manager.runDirectory(result.id)!;
    const worker = JSON.parse(fs.readFileSync(path.join(run, "status.json"), "utf8"));
    expect(result).toMatchObject({ status: worker.status, text: worker.text, value: worker.value });
    expect(result.text).toHaveLength(100_000);
    expect(result.error).toBe(worker.error);
    expect(result.warnings).toEqual(expect.arrayContaining([expect.stringContaining(savedPath)]));
    expect(fs.existsSync(savedPath)).toBe(false);
    if (collection === "cleanup") {
      await expect(manager.cleanup(result.id)).rejects.toThrow(/EIO writing/);
      expect(fs.existsSync(run), "throwing callback must veto explicit cleanup").toBe(true);
    }
    // Ordinary nonresident manager: no callback and no persistence obligation.
    const ordinary = new AgentManager(process.cwd(), manager.config, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "ordinary"),
    });
    managers.push(ordinary);
    const session = await ordinary.run({ task: "ordinary session", transport: "process" });
    const sessionRun = ordinary.runDirectory(session.id)!;
    await ordinary.cleanup(session.id);
    expect(fs.existsSync(sessionRun)).toBe(false);
    // Actor callback returns normally; it must not inherit the unrelated public task's pin.
    const actor = await manager.run({ task: "actor activation", actorId: "actor:fixture", residency: "durable", transport: "process" });
    const actorRun = manager.runDirectory(actor.id)!;
    await manager.cleanup(actor.id);
    expect(fs.existsSync(actorRun)).toBe(false);
    blocked = false;
    if (collection === "cleanup") await manager.cleanup(result.id);
    else await manager.close();
    expect(fs.existsSync(run), "successful preservation authorizes collection").toBe(false);
    const saved = JSON.parse(fs.readFileSync(savedPath, "utf8"));
    expect(saved).toMatchObject(worker); // manager may add transport metadata, never truncate the completion
    expect(saved.error).toBe(worker.error);
    expect(saved.warnings).toBeUndefined();
    expect(save.mock.calls.filter(([record]) => record.id === result.id).length).toBeGreaterThan(1);
  }, 15_000);

  it("notifies and releases UI subscribers", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
      runRoot: root,
    });
    managers.push(manager);
    const listener = vi.fn();
    const unsubscribe = manager.subscribeUi(listener);

    const result = await manager.run({ task: "Observe state", transport: "process" });
    expect(listener).toHaveBeenCalled();

    unsubscribe();
    const beforeCleanup = listener.mock.calls.length;
    await manager.cleanup(result.id);
    expect(listener).toHaveBeenCalledTimes(beforeCleanup);
  });

  it("owns a returned queued handle independently of the caller's abort signal", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 1 }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: root,
    });
    managers.push(manager);
    const first = await manager.spawn({ task: "HANG", transport: "process" });
    const caller = new AbortController();
    const queued = await manager.spawn({ task: "complete after caller abort", transport: "process" }, caller.signal);
    expect(queued.status).toBe("queued");
    caller.abort();
    expect(manager.listForUi().find((run) => run.id === queued.id)).toMatchObject({ status: "queued", queuePosition: 1 });
    await manager.stop(first.id);
    expect((await manager.wait(queued.id)).status).toBe("completed");
  });

  it.each([false, true])("keeps the resident commit fence separate from activation authority (queued: %s)", async (queued) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const prepared = vi.fn(async () => {});
    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 1 }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: root,
      preparePiModel: prepared,
    });
    managers.push(manager);
    const first = queued ? await manager.spawn({ task: "HANG", transport: "process" }) : undefined;
    const commit = vi.fn((id: string): void => {
      expect(prepared).toHaveBeenCalled();
      expect(fs.existsSync(path.join(root, id))).toBe(false);
    });
    const handle = await manager.spawn({ task: "independent commit", transport: "process" }, undefined, () => true, commit);
    if (first) {
      expect(handle.status).toBe("queued");
      expect(commit).not.toHaveBeenCalled();
      await manager.stop(first.id);
    }
    expect(await manager.wait(handle.id)).toMatchObject({ status: "completed" });
    expect(commit).toHaveBeenCalledExactlyOnceWith(handle.id);
  });

  it.each([false, true])("vetoes an abandoned resident commit before worker mutation and releases capacity (queued: %s)", async (queued) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 1 }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: root,
    });
    managers.push(manager);
    const first = queued ? await manager.spawn({ task: "HANG", transport: "process" }) : undefined;
    let abandonedId = "";
    const commit = (id: string): void => { abandonedId = id; throw new Error("resident request abandoned"); };
    const spawn = manager.spawn({ task: "no late mutation", transport: "process" }, undefined, () => true, commit);
    if (first) {
      const handle = await spawn;
      expect(handle.status).toBe("queued");
      await manager.stop(first.id);
      expect(await manager.wait(handle.id)).toMatchObject({ status: "failed", error: "resident request abandoned" });
    } else {
      await expect(spawn).rejects.toThrow("resident request abandoned");
    }
    expect(abandonedId).not.toBe("");
    expect(fs.existsSync(path.join(root, abandonedId))).toBe(false);
    expect(manager.runningCount()).toBe(0);
    expect(await manager.run({ task: "capacity released", transport: "process" })).toMatchObject({ status: "completed" });
  });

  it("revokes a queued activation when its owner generation changes without aborting the signal", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 1 }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: root,
    });
    managers.push(manager);
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    try {
      const first = await manager.spawn({ task: "HANG", transport: "process" });
      let generation = 1;
      const owner = new AbortController();
      const queued = await manager.spawn({ task: "stale generation", transport: "process" }, owner.signal, () => generation === 1);
      generation++;
      await manager.stop(first.id);
      expect(await manager.wait(queued.id)).toMatchObject({ status: "stopped", error: "Agent activation no longer authorized" });
      expect(owner.signal.aborted).toBe(false);
      expect(launch).toHaveBeenCalledTimes(1);
      expect(manager.runningCount()).toBe(0);
    } finally { launch.mockRestore(); }
  });

  it("checks owner revocation again after asynchronous transport preparation", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 1 }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: root,
    });
    managers.push(manager);
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    const first = await manager.spawn({ task: "HANG", transport: "process" });
    let release!: () => void;
    let ready!: () => void;
    const gate = new Promise<void>((resolve) => { release = resolve; });
    const preparing = new Promise<void>((resolve) => { ready = resolve; });
    const available = vi.spyOn(ProcessTransport.prototype, "available").mockImplementation(async () => { ready(); await gate; return true; });
    try {
      let authorized = true;
      const queued = await manager.spawn({ task: "revoked during preparation", transport: "process" }, undefined, () => authorized);
      await manager.stop(first.id);
      await preparing;
      authorized = false;
      release();
      expect(await manager.wait(queued.id)).toMatchObject({ status: "stopped", error: "Agent activation no longer authorized" });
      expect(launch).toHaveBeenCalledTimes(1);
    } finally { release(); available.mockRestore(); launch.mockRestore(); }
  });

  it("rechecks the owner generation inside delayed transport creation", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 1 }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: root,
    });
    managers.push(manager);
    const first = await manager.spawn({ task: "HANG", transport: "process" });
    const original = ProcessTransport.prototype.launch;
    let release!: () => void;
    let ready!: () => void;
    const gate = new Promise<void>((resolve) => { release = resolve; });
    const creating = new Promise<void>((resolve) => { ready = resolve; });
    const created: string[] = [];
    const launch = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, request) {
      ready(); await gate;
      const handle = await original.call(this, request);
      created.push(request.id);
      return handle;
    });
    try {
      let authorized = true;
      const owner = new AbortController();
      const queued = await manager.spawn({ task: "revoked generation during creation", transport: "process" }, owner.signal, () => authorized);
      await manager.stop(first.id);
      await creating;
      authorized = false;
      release();
      expect(await manager.wait(queued.id)).toMatchObject({ status: "stopped", error: "Agent activation no longer authorized" });
      expect(owner.signal.aborted).toBe(false);
      expect(created).toEqual([]);
    } finally { release(); launch.mockRestore(); }
  });

  it("cancels an admitted queued run during model preparation without launching its worker", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    let resume!: () => void;
    let preparing!: () => void;
    const gate = new Promise<void>((resolve) => { resume = resolve; });
    const ready = new Promise<void>((resolve) => { preparing = resolve; });
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 1 }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: root,
      preparePiModel: async (model) => { if (model === "test/queued") { preparing(); await gate; } },
    });
    managers.push(manager);
    const first = await manager.spawn({ task: "HANG", transport: "process" });
    const queued = await manager.spawn({ task: "queued preparation", model: "test/queued", transport: "process" });
    await manager.stop(first.id);
    await ready;
    const stopped = manager.stop(queued.id);
    resume();
    expect((await stopped).status).toBe("stopped");
    expect(launch).toHaveBeenCalledTimes(1);
    expect(manager.runningCount()).toBe(0);
  });

  it.each([new Error("queued model preparation failed"), null])("settles and notifies a detached queued launch failure exactly once (%s)", async (failure) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const complete = vi.fn();
    const settled = vi.fn();
    const lifecycle = vi.fn();
    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 1 }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: root,
      preparePiModel: async (model) => { if (model === "test/broken") throw failure; },
      onBackgroundComplete: complete, onSettled: settled, onLifecycle: lifecycle,
    });
    managers.push(manager);
    const first = await manager.spawn({ task: "HANG", transport: "process" });
    const queued = await manager.spawn({ task: "queued failure", model: "test/broken", transport: "process" });
    manager.detachSignal(queued.id);
    await manager.stop(first.id);
    await vi.waitFor(() => expect(manager.status(queued.id).status).toBe("failed"));
    expect(complete.mock.calls.filter(([result]) => result.id === queued.id)).toHaveLength(1);
    expect(settled.mock.calls.filter(([result]) => result.id === queued.id)).toHaveLength(1);
    expect(lifecycle).toHaveBeenCalledWith(expect.objectContaining({ event: "run.failed", runId: queued.id }));
    expect(await manager.wait(queued.id)).toMatchObject({ status: "failed", error: failure instanceof Error ? failure.message : String(failure) });
    expect(await manager.cleanup(queued.id)).toMatchObject({ cleaned: true });
    expect(manager.list().some((run) => run.id === queued.id)).toBe(false);
  });

  it("preserves queued launch-uncertainty artifacts and refuses cleanup", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 1, retainRuns: false }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: root,
    });
    managers.push(manager);
    const first = await manager.spawn({ task: "HANG", transport: "process" });
    const queued = await manager.spawn({ task: "uncertain queued launch", transport: "process" });
    vi.spyOn(ProcessTransport.prototype, "launch").mockRejectedValueOnce(Object.assign(new Error("launch outcome unknown"), { launchOutcome: "unknown" }));
    await manager.stop(first.id);
    expect(await manager.wait(queued.id)).toMatchObject({ status: "failed", error: "launch outcome unknown" });
    const runDirectory = path.join(root, queued.id);
    expect(fs.existsSync(runDirectory)).toBe(true);
    await expect(manager.cleanup(queued.id)).rejects.toThrow("lost track of its worker");
    await manager.close();
    expect(fs.existsSync(runDirectory)).toBe(true);
  });

  it.each(["alive", "failed-probe", "lost-contact", "hung-probe", "failed-stop"] as const)("retains cancelled queued worker files and reports cleanup pending (%s)", async (mode) => {
    const repository = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-cancelled-repo-"));
    roots.push(repository);
    const git = (...args: string[]) => execFileSync("git", args, { cwd: repository, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"], env: { ...process.env, GIT_AUTHOR_NAME: "Fabric tests", GIT_AUTHOR_EMAIL: "tests@example.invalid", GIT_COMMITTER_NAME: "Fabric tests", GIT_COMMITTER_EMAIL: "tests@example.invalid" } });
    git("init", "-q");
    fs.writeFileSync(path.join(repository, "README.md"), "test\n");
    git("add", "."); git("commit", "-q", "-m", "init");
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const manager = new AgentManager(repository, { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 1, retainRuns: false }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: root,
    });
    managers.push(manager);
    const first = await manager.spawn({ task: "HANG", transport: "process" });
    let release!: () => void;
    let ready!: () => void;
    const gate = new Promise<void>((resolve) => { release = resolve; });
    const creating = new Promise<void>((resolve) => { ready = resolve; });
    let workerExited = false;
    let worktree: string | undefined;
    const stop = vi.fn(async () => { if (mode === "failed-stop") throw new Error("close acknowledgment lost"); });
    const launch = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementationOnce(async (request) => {
      worktree = request.cwd;
      ready(); await gate;
      return { kind: "process", sessionId: "unconfirmed-worker", stop, isAlive: async () => {
        if (workerExited) return false;
        if (mode === "failed-probe") throw new Error("liveness acknowledgment lost");
        if (mode === "hung-probe") return new Promise<boolean>(() => {});
        return mode !== "lost-contact";
      }, lostContact: () => !workerExited && mode === "lost-contact" ? "server unreachable; worker may still run" : undefined };
    });
    try {
      const queued = await manager.spawn({ task: "cancel during worker creation", transport: "process", worktree: true });
      await manager.stop(first.id);
      await creating;
      const started = Date.now();
      const stopped = manager.stop(queued.id);
      release();
      // Exit is not confirmed: even a successful stop request is not deletion authority.
      const result = await stopped;
      expect({ status: result.status, error: result.error, worktreeRetained: fs.existsSync(worktree!) }).toMatchObject({
        status: "stopped", error: expect.stringContaining("cleanup pending"), worktreeRetained: true,
      });
      expect(Date.now() - started).toBeLessThan(9_000);
      if (mode === "alive") expect(Date.now() - started).toBeGreaterThanOrEqual(6_900);
      expect(stop).toHaveBeenCalledTimes(1);
      const runDirectory = path.join(root, queued.id);
      expect(JSON.parse(fs.readFileSync(path.join(runDirectory, "unresolved-worker.json"), "utf8"))).toMatchObject({ runId: queued.id, worktree, cleanupPending: true, sessionId: "unconfirmed-worker" });
      expect(fs.existsSync(path.join(runDirectory, "task.txt"))).toBe(true);
      expect(fs.existsSync(worktree!)).toBe(true);
      await expect(manager.cleanup(queued.id)).rejects.toThrow("lost track of its worker");
      expect(fs.existsSync(runDirectory)).toBe(true);
      expect(fs.existsSync(worktree!)).toBe(true);
      // The manager and retention guards preserve the obligation across shutdown too.
      workerExited = true;
      await manager.close();
      expect(fs.existsSync(runDirectory)).toBe(true);
      expect(fs.existsSync(worktree!)).toBe(true);
    } finally {
      workerExited = true; release(); launch.mockRestore();
      if (worktree && fs.existsSync(worktree)) git("worktree", "remove", "--force", worktree);
    }
  }, 20_000);

  it("waits for a cancelled queued worker's confirmed exit before allowing cleanup", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 1, retainRuns: false }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: root,
    });
    managers.push(manager);
    const first = await manager.spawn({ task: "HANG", transport: "process" });
    let release!: () => void;
    let ready!: () => void;
    const gate = new Promise<void>((resolve) => { release = resolve; });
    const creating = new Promise<void>((resolve) => { ready = resolve; });
    let stoppedAt: number | undefined;
    const launch = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementationOnce(async () => {
      ready(); await gate;
      return { kind: "process", stop: async () => { stoppedAt = Date.now(); }, isAlive: async () => stoppedAt === undefined || Date.now() - stoppedAt < 150 };
    });
    try {
      const queued = await manager.spawn({ task: "exit after termination", transport: "process" });
      await manager.stop(first.id);
      await creating;
      let settled = false;
      const stopped = manager.stop(queued.id).then((result) => { settled = true; return result; });
      release();
      await new Promise((resolve) => setTimeout(resolve, 50));
      expect(settled).toBe(false);
      expect(fs.existsSync(path.join(root, queued.id, "task.txt"))).toBe(true);
      expect(await stopped).toMatchObject({ status: "stopped", error: "Agent launch aborted" });
      expect(fs.existsSync(path.join(root, queued.id, "unresolved-worker.json"))).toBe(false);
      expect(await manager.cleanup(queued.id)).toEqual({ cleaned: true });
      expect(fs.existsSync(path.join(root, queued.id))).toBe(false);
    } finally { release(); launch.mockRestore(); }
  });

  it("reattaches completion notification when a queued wait reaches its bound after admission", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const complete = vi.fn();
    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 1 }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: root, onBackgroundComplete: complete,
    });
    managers.push(manager);
    const first = await manager.spawn({ task: "HANG", transport: "process" });
    const queued = await manager.spawn({ task: "LIVE_WITH_PROGRESS", transport: "process" });
    const waiting = expect(manager.wait(queued.id, { timeoutMs: 500 })).rejects.toThrow("is still running after");
    await manager.stop(first.id);
    await waiting;
    await vi.waitFor(() => expect(complete).toHaveBeenCalledWith(expect.objectContaining({ id: queued.id, status: "completed" })), { timeout: 10_000 });
    expect(await manager.wait(queued.id)).toMatchObject({ status: "completed" });
  });

  it.each([false, true])("detaches an aborted queued wait without consuming and delivers completion exactly once (deferred: %s)", async (deferred) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-queued-wait-"));
    roots.push(root);
    const complete = vi.fn();
    const consumed = vi.fn();
    const deferConsumption = vi.fn();
    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 1 }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: root,
      onBackgroundComplete: complete, onResultConsumed: consumed,
    });
    managers.push(manager);
    const first = await manager.spawn({ task: "HANG", transport: "process" });
    const queued = await manager.spawn({ task: "LIVE_WITH_PROGRESS", transport: "process" });
    expect(queued.status).toBe("queued");
    const controller = new AbortController();
    const wait = manager.wait(queued.id, {
      signal: controller.signal,
      ...(deferred ? { deferConsumption } : {}),
    });
    let timer: NodeJS.Timeout | undefined;
    const observation = Promise.race([wait, new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error("queued observation remained blocked")), 100);
    })]);
    controller.abort(new Error("Main stopped waiting for queued run"));
    try {
      await expect(observation).rejects.toThrow("Main stopped waiting for queued run");
    } finally {
      clearTimeout(timer);
    }
    expect(manager.status(queued.id).status).toBe("queued");
    expect(consumed.mock.calls.filter(([id]) => id === queued.id)).toHaveLength(0);
    expect(deferConsumption).not.toHaveBeenCalled();
    expect(complete).not.toHaveBeenCalled();
    await manager.stop(first.id);
    await vi.waitFor(() => expect(complete).toHaveBeenCalledWith(expect.objectContaining({
      id: queued.id, status: "completed", text: expect.stringMatching(/^live attempt \d+ complete$/),
    })), { timeout: 10_000 });
    expect(consumed.mock.calls.filter(([id]) => id === queued.id)).toHaveLength(0);
    expect(deferConsumption).not.toHaveBeenCalled();
    expect(await manager.wait(queued.id)).toMatchObject({ status: "completed" });
    expect(consumed.mock.calls.filter(([id]) => id === queued.id)).toHaveLength(1);
    expect(complete.mock.calls.filter(([result]) => result.id === queued.id)).toHaveLength(1);
  });

  it("runs a worker through the direct process transport", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
      runRoot: root,
      fullCodeMode: false,
    });
    managers.push(manager);
    const result = await manager.run({ task: "Inspect this repository", transport: "process" });
    expect(result.status).toBe("completed");
    expect((result as AgentRunResult & { fullCodeMode?: string }).fullCodeMode).toBe("false");
    expect(result.text).toBe("fake worker complete");
    expect(result.transport).toBe("process");
    expect(manager.list()).toHaveLength(1);
    fs.rmSync(path.join(manager.runDirectory(result.id)!, "status.json"));
    expect(manager.status(result.id).status).toBe("completed");
  });

  it("adds component guidance to direct participants without duplicating recursive guidance", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const resolveParticipantGuidance = vi.fn(({ model }: { model?: string }) =>
      model === "deepseek/deepseek-chat" ? "DeepSeek participant guidance" : undefined);
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
      runRoot: root,
      resolveParticipantGuidance,
    });
    managers.push(manager);

    const direct = await manager.run({
      task: "Direct guided participant",
      transport: "process",
      model: "deepseek/deepseek-chat",
      systemPrompt: "Actor role prompt",
    });
    expect((direct as AgentRunResult & { systemPrompt?: string }).systemPrompt).toBe(
      "Actor role prompt\n\nDeepSeek participant guidance",
    );
    expect(resolveParticipantGuidance).toHaveBeenCalledWith({
      model: "deepseek/deepseek-chat",
      runner: "pi",
    });

    const secondDirect = await manager.run({
      task: "Different task, same guided participant",
      transport: "process",
      model: "deepseek/deepseek-chat",
      systemPrompt: "Actor role prompt",
    });
    expect((secondDirect as AgentRunResult & { systemPrompt?: string }).systemPrompt).toBe(
      (direct as AgentRunResult & { systemPrompt?: string }).systemPrompt,
    );

    resolveParticipantGuidance.mockClear();
    const recursive = await manager.run({
      task: "Recursive participant",
      transport: "process",
      model: "deepseek/deepseek-chat",
      recursive: true,
      systemPrompt: "Recursive role prompt",
    });
    expect((recursive as AgentRunResult & { systemPrompt?: string }).systemPrompt).toBe(
      "Recursive role prompt",
    );
    expect(resolveParticipantGuidance).not.toHaveBeenCalled();
  });

  it("relays child Pi lifecycle records and a normalized terminal event", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const lifecycle: FabricLifecyclePublishRequest[] = [];
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
      runRoot: root,
      mainAgentId: "session:root",
      hostId: "host:root",
      identityId: "session:root",
      onLifecycle: (event) => lifecycle.push(event),
    });
    managers.push(manager);

    const result = await manager.run({ task: "Observe lifecycle", transport: "process" });

    expect(lifecycle.map((event) => event.event)).toEqual([
      "pi.agent_start",
      "pi.turn_end",
      "pi.agent_end",
      "pi.agent_settled",
      "run.completed",
    ]);
    expect(lifecycle.at(-1)).toMatchObject({
      source: {
        id: result.id,
        kind: "agent",
        rootId: "session:root",
        ownerHostId: "host:root",
        ownerIdentityId: "session:root",
      },
      runId: result.id,
      status: "completed",
    });
    expect(lifecycle.find((event) => event.event === "pi.turn_end")?.data).toEqual({
      turnIndex: 0,
    });
  });

  it("materializes a private Pi session for a trajectory handoff seed", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
      runRoot: root,
    });
    managers.push(manager);
    const handle = await manager.spawn({
      task: "HANG while the handoff session is inspected",
      runner: "pi",
      transport: "process",
      model: "anthropic/executor",
      sessionSeed: handoffSeed(),
    });
    const handoffDirectory = path.join(manager.runDirectory(handle.id)!, "handoff-session");
    const [sessionName] = fs.readdirSync(handoffDirectory);
    expect(sessionName).toBeDefined();
    const session = SessionManager.open(path.join(handoffDirectory, sessionName!));
    expect(session.buildSessionContext()).toMatchObject({
      messages: [
        { role: "user", content: "Rare handoff fact 43117" },
        {
          role: "assistant",
          content: [
            { type: "text", text: "Ready to continue from the fork." },
            { type: "toolCall", id: "outer-manager-handoff", name: "fabric_exec" },
          ],
        },
        {
          role: "toolResult",
          toolCallId: "outer-manager-handoff",
          toolName: "fabric_exec",
          content: [{ type: "text", text: "Manager boundary complete" }],
        },
      ],
      model: { provider: "anthropic", modelId: "frontier" },
      thinkingLevel: "high",
    });
    await manager.stop(handle.id);
  });

  it("rejects unsupported activation windows before model preparation or transport launch", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-window-manager-"));
    roots.push(root);
    const preparePiModel = vi.fn();
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: root, preparePiModel,
    });
    managers.push(manager);
    const request = { task: "must not infer", inferenceContext: "activation" as const, actorId: "actor", sessionFile: path.join(root, "actor.jsonl") };
    await expect(manager.spawn({ ...request, runner: "claude" })).rejects.toThrow(/persistent Pi actor/);
    await expect(manager.spawn({ ...request, runner: "veda" })).rejects.toThrow(/persistent Pi actor/);
    await expect(manager.spawn({ task: "missing actor", inferenceContext: "activation" })).rejects.toThrow(/persistent Pi actor/);
    await expect(manager.spawn({ ...request, inferenceContext: "invalid" as "activation" })).rejects.toThrow(/inference context/);
    expect(preparePiModel).not.toHaveBeenCalled();
    expect(manager.list()).toEqual([]);
  });

  it("passes prepared destination settings only to compacted handoffs", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-budget-"));
    roots.push(root);
    const resolveBudget = vi.fn(async () => ({ contextWindow: 50_000, targetContextRatio: 0.5, reserveTokens: 5000, keepRecentTokens: 1000 }));
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: root,
      preparePiModel: async () => "anthropic/resolved-target",
      resolveHandoffCompactionBudget: resolveBudget,
    });
    managers.push(manager);
    const handle = await manager.spawn({ task: "HANG", model: "alias", sessionSeed: handoffSeed(`Fact ${"x".repeat(60_000)}`), handoffCompact: {} });
    expect(resolveBudget).toHaveBeenCalledExactlyOnceWith("anthropic/resolved-target", process.cwd());
    const directory = path.join(manager.runDirectory(handle.id)!, "handoff-session");
    const child = SessionManager.open(path.join(directory, fs.readdirSync(directory)[0]!));
    expect(child.getBranch().find(e => e.type === "compaction")).toMatchObject({ details: { budget: { contextWindow: 50_000, keepRecentTokens: 1000 } } });
    await manager.stop(handle.id);
    const plain = await manager.spawn({ task: "HANG", model: "alias", sessionSeed: handoffSeed() });
    expect(resolveBudget).toHaveBeenCalledTimes(1);
    await manager.stop(plain.id);
  });

  it("rejects trajectory seeds for the Claude runner and conflicting session files", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
      runRoot: root,
    });
    managers.push(manager);
    const sessionSeed = handoffSeed("Invalid seed");
    await expect(
      manager.spawn({ task: "invalid", runner: "claude", sessionSeed }),
    ).rejects.toThrow(/only supported by the Pi runner/);
    await expect(
      manager.spawn({
        task: "invalid",
        runner: "pi",
        sessionSeed,
        sessionFile: path.join(root, "existing.jsonl"),
      }),
    ).rejects.toThrow(/cannot combine sessionSeed with sessionFile/);
  });

  it("does not let same-provider preparation authorize a different model", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const preparePiModel = vi.fn(async (model: string | undefined) => {
      await new Promise((resolve) => setTimeout(resolve, 50));
      if (model === "openai-codex/gpt-hidden") {
        throw new Error(`Model ${JSON.stringify(model)} is not available to this Pi session`);
      }
      return model;
    });
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
      runRoot: root,
      preparePiModel,
    });
    managers.push(manager);

    const [visible, hidden] = await Promise.allSettled([
      manager.run({
        task: "Visible prepared child",
        model: "openai-codex/gpt-visible",
        transport: "process",
      }),
      manager.run({
        task: "Hidden prepared child",
        model: "openai-codex/gpt-hidden",
        transport: "process",
      }),
    ]);

    expect(visible).toMatchObject({ status: "fulfilled", value: { status: "completed" } });
    expect(hidden).toMatchObject({
      status: "rejected",
      reason: expect.objectContaining({ message: expect.stringContaining("not available to this Pi session") }),
    });
    expect(preparePiModel).toHaveBeenCalledTimes(2);
    expect(preparePiModel.mock.calls.map(([model]) => model).sort()).toEqual([
      "openai-codex/gpt-hidden",
      "openai-codex/gpt-visible",
    ]);
  });

  it("validates the configured Pi model default before launching", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const manager = new AgentManager(
      process.cwd(),
      { ...DEFAULT_FABRIC_CONFIG.agents, model: "provider/hidden" },
      {
        workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
        runRoot: root,
        preparePiModel: async (model) => {
          throw new Error(`Model ${JSON.stringify(model)} is not available to this Pi session`);
        },
      },
    );
    managers.push(manager);

    await expect(manager.spawn({ task: "Do not launch", runner: "pi" })).rejects.toThrow(
      /not available to this Pi session/,
    );
    expect(fs.readdirSync(root)).toEqual([]);
  });

  it("retries a Pi child that fails before its first turn", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker-startup-retry.mjs"),
      runRoot: root,
    });
    managers.push(manager);

    const result = await manager.run({ task: "Recover startup", transport: "process" });

    expect(result.status).toBe("completed");
    expect(result.text).toBe("startup retry recovered");
    expect(
      fs.readFileSync(path.join(manager.runDirectory(result.id)!, "startup-attempts"), "utf8"),
    ).toBe("2");
  });

  it("does not retry deterministic failures before the first turn", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker-startup-retry.mjs"),
      runRoot: root,
    });
    managers.push(manager);

    const result = await manager.run({ task: "Reject startup", transport: "process" });

    expect(result.status).toBe("failed");
    expect(result.error).toBe("provider rejected the prompt");
    expect(
      fs.readFileSync(path.join(manager.runDirectory(result.id)!, "startup-attempts"), "utf8"),
    ).toBe("1");
  });

  it("retries a child whose transport exits before producing a result", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker-transport-death.mjs"),
      runRoot: root,
    });
    managers.push(manager);

    const result = await manager.run({ task: "Recoverable boot death", transport: "process" });

    expect(result.status).toBe("completed");
    expect(result.text).toBe("transport death retry recovered");
    expect(
      fs.readFileSync(path.join(manager.runDirectory(result.id)!, "startup-attempts"), "utf8"),
    ).toBe("2");
  },
  30_000);

  // smarty-dev#347: a dropped transport call made a live worker look dead; the relaunch
  // then ran the same task in a second worker while the first kept going.
  it("stops the previous worker before relaunching one whose liveness was misjudged", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const events: string[] = [];
    const launch = ProcessTransport.prototype.launch;
    let launches = 0;
    const spy = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, request) {
      const handle = await launch.call(this, request);
      const index = ++launches;
      events.push(`launch:${index}`);
      if (index > 1) return handle;
      let stopped = false;
      return {
        ...handle,
        // The first worker is alive, but its liveness check is "dropped" until it is stopped.
        isAlive: async () => (stopped ? handle.isAlive() : false),
        stop: async () => { events.push("stop:1"); stopped = true; await handle.stop(); },
      };
    });
    try {
      const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
        workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
        runRoot: root,
      });
      managers.push(manager);
      const handle = await manager.spawn({ task: "HANG until stopped", transport: "process" });
      await expect.poll(() => launches, { timeout: 15_000, interval: 50 }).toBe(2);

      expect(events.slice(0, 3)).toEqual(["launch:1", "stop:1", "launch:2"]);
      const relaunches = fs.readFileSync(path.join(manager.runDirectory(handle.id)!, "relaunches.jsonl"), "utf8")
        .trim().split("\n").map((line) => JSON.parse(line));
      expect(relaunches).toEqual([expect.objectContaining({ kind: "startup-retry", previousError: expect.stringContaining("exited without a result") })]);
      await manager.stop(handle.id);
    } finally {
      spy.mockRestore();
    }
  }, 30_000);

  // smarty-dev#266 (Herdr): a transport that cannot prove a lost worker is gone never relaunches it.
  it("fails a lost run instead of relaunching it when its transport is not relaunchable", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const launch = ProcessTransport.prototype.launch;
    let launches = 0;
    const handles: Array<Awaited<ReturnType<typeof launch>>> = [];
    const spy = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, request) {
      const handle = await launch.call(this, request);
      launches++;
      handles.push(handle);
      // The worker looks lost at once, as a Herdr pane whose server stayed unreachable.
      return { ...handle, relaunchable: false, isAlive: async () => false };
    });
    try {
      const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
        workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
        runRoot: root,
      });
      managers.push(manager);
      const result = await manager.run({ task: "HANG until stopped", transport: "process" });
      expect(launches).toBe(1);
      expect(result.status).toBe("failed");
      expect(result.error).toContain("Agent transport exited without a result");
      expect(fs.existsSync(path.join(manager.runDirectory(result.id)!, "relaunches.jsonl"))).toBe(false);
    } finally {
      spy.mockRestore();
      for (const handle of handles) await handle.stop();
    }
  }, 30_000);

  // dev-lead review D1 on #26: lost contact is not an exit. The run fails as lost, once, and
  // neither cleanup nor shutdown deletes files that the still-running worker may use.
  it("fails a run whose transport lost contact as lost, and keeps its worker's files", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const launch = ProcessTransport.prototype.launch;
    let launches = 0;
    const handles: Array<Awaited<ReturnType<typeof launch>>> = [];
    const spy = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, request) {
      const handle = await launch.call(this, request);
      launches++;
      handles.push(handle);
      // As a Herdr handle past its bound: the worker keeps running, contact is lost.
      return { ...handle, relaunchable: false, isAlive: async () => false, lostContact: () => "the Herdr server has been unreachable for 300 s" };
    });
    try {
      const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: false }, {
        workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
        runRoot: root,
      });
      managers.push(manager);
      const result = await manager.run({ task: "HANG until stopped", transport: "process" });
      expect(launches).toBe(1);
      expect(result.status).toBe("failed");
      expect(result.error).toMatch(/^Lost track of the worker: the Herdr server has been unreachable/);
      expect(result.error).not.toContain("exited without a result");
      const runDirectory = manager.runDirectory(result.id)!;
      await expect(manager.cleanup(result.id)).rejects.toThrow(/lost track of its worker/);
      expect(fs.existsSync(runDirectory)).toBe(true);
      expect(await handles[0]!.isAlive()).toBe(true);           // the real worker still runs
      await manager.close();
      expect(fs.existsSync(runDirectory)).toBe(true);           // shutdown kept its files
    } finally {
      spy.mockRestore();
      for (const handle of handles) await handle.stop();
    }
  }, 30_000);

  // review/astra on 3257dba, D1: every settlement path keeps a possibly live worker's evidence.
  const lostOnStop = (launched: Array<{ stop(): Promise<void> }>) => {
    const launch = ProcessTransport.prototype.launch;
    return vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, request) {
      const handle = await launch.call(this, request);
      launched.push(handle);
      // As a Herdr handle whose server is gone: stop cannot reach the worker, which keeps running.
      let stopped = false;
      return {
        ...handle,
        relaunchable: false,
        isAlive: async () => !stopped,
        stop: async () => { stopped = true; },
        lostContact: () => (stopped ? "the Herdr server has been unreachable for 300 s" : undefined),
      };
    });
  };

  it.each(["stop", "deadline"] as const)("marks a run whose worker was lost on the %s path, and refuses its cleanup", async (path_) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const launched: Array<{ stop(): Promise<void> }> = [];
    const spy = lostOnStop(launched);
    try {
      // A request can only extend the configured timeout, so the deadline case configures it.
      const manager = new AgentManager(process.cwd(), {
        ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: false, ...(path_ === "deadline" ? { timeoutMs: 1_500 } : {}),
      }, {
        workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
        runRoot: root,
      });
      managers.push(manager);
      const handle = await manager.spawn({ task: "HANG until stopped", transport: "process" });
      const result = path_ === "stop" ? await manager.stop(handle.id) : await manager.wait(handle.id);
      expect(result.status).toBe(path_ === "stop" ? "stopped" : "timed_out");
      const runDirectory = manager.runDirectory(handle.id)!;
      expect(JSON.parse(fs.readFileSync(path.join(runDirectory, "unresolved-worker.json"), "utf8")).reason)
        .toMatch(/Herdr server has been unreachable/);
      await expect(manager.cleanup(handle.id)).rejects.toThrow(/lost track of its worker/);
      await manager.close();
      expect(fs.existsSync(runDirectory)).toBe(true);
    } finally {
      spy.mockRestore();
      for (const handle of launched) await handle.stop();
    }
  }, 30_000);

  // review/astra on 3257dba, D3: an unconfirmed launch keeps its worktree and run files.
  it("keeps the worktree and run files of a launch whose outcome is unknown", async () => {
    const repository = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-unknown-launch-repo-"));
    roots.push(repository);
    const git = (...args: string[]) => execFileSync("git", args, { cwd: repository, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] });
    git("init", "-q");
    git("config", "user.email", "pi-fabric-tests@example.invalid");
    git("config", "user.name", "Pi Fabric tests");
    fs.writeFileSync(path.join(repository, "README.md"), "test\n");
    git("add", ".");
    git("commit", "-q", "-m", "init");
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const launch = ProcessTransport.prototype.launch;
    const launched: Array<{ stop(): Promise<void> }> = [];
    const spy = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, request) {
      launched.push(await launch.call(this, request));       // the worker starts ...
      // ... but the reply is lost, as a dropped Herdr layout.apply reply.
      throw Object.assign(new Error("Herdr did not confirm the launch"), { launchOutcome: "unknown" });
    });
    let worktree: string | undefined;
    try {
      const manager = new AgentManager(repository, DEFAULT_FABRIC_CONFIG.agents, {
        workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
        runRoot: root,
      });
      managers.push(manager);
      await expect(manager.spawn({ task: "HANG until stopped", transport: "process", worktree: true })).rejects.toThrow("did not confirm");
      const marked = fs.readdirSync(root).map((name) => path.join(root, name, "unresolved-worker.json")).filter((file) => fs.existsSync(file));
      expect(marked).toHaveLength(1);
      worktree = JSON.parse(fs.readFileSync(marked[0]!, "utf8")).worktree as string;
      expect(fs.existsSync(worktree)).toBe(true);
      expect(git("worktree", "list", "--porcelain")).toContain("branch refs/heads/");
      await manager.close();
      expect(fs.existsSync(path.dirname(marked[0]!))).toBe(true);
      expect(fs.existsSync(worktree)).toBe(true);
    } finally {
      spy.mockRestore();
      for (const handle of launched) await handle.stop();
      if (worktree) execFileSync("git", ["worktree", "remove", "--force", worktree], { cwd: repository, stdio: "ignore" });
    }
  }, 30_000);

  // dev-lead review F2: a stop that does not take effect must never lead to a second worker.
  it("fails the run instead of relaunching while the previous worker is still alive after its stop", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const launch = ProcessTransport.prototype.launch;
    let launches = 0;
    let first: Awaited<ReturnType<typeof launch>> | undefined;
    const spy = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, request) {
      const handle = await launch.call(this, request);
      if (++launches > 1) return handle;
      first = handle;
      let stopRequested = false;
      return {
        ...handle,
        // Misjudged as dead until a stop is requested; the stop is then lost, so it stays alive.
        isAlive: async () => (stopRequested ? handle.isAlive() : false),
        stop: async () => { stopRequested = true; },
      };
    });
    try {
      const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
        workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
        runRoot: root,
      });
      managers.push(manager);
      const handle = await manager.spawn({ task: "HANG until stopped", transport: "process" });
      const result = await manager.wait(handle.id);
      expect(launches).toBe(1);
      expect(result.status).toBe("failed");
      expect(result.error).toContain("did not stop, so it was not relaunched");
      const relaunches = fs.readFileSync(path.join(manager.runDirectory(handle.id)!, "relaunches.jsonl"), "utf8")
        .trim().split("\n").map((line) => JSON.parse(line));
      expect(relaunches).toEqual([expect.objectContaining({ kind: "relaunch-failed" })]);
      // review/astra on e170d9e: the worker that did not stop may still use its files.
      await expect(manager.cleanup(handle.id)).rejects.toThrow(/lost track of its worker/);
      expect(fs.existsSync(manager.runDirectory(handle.id)!)).toBe(true);
    } finally {
      spy.mockRestore();
      await first?.stop();
    }
  }, 45_000);

  it.skipIf(process.platform === "win32")("R3 close preserves an untracked surviving worker directory and removes tracked terminal runs", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-orphan-"));
    roots.push(root);
    const runRoot = path.join(root, "runs");
    const untracked = path.join(runRoot, "previous-host-worker");
    fs.mkdirSync(untracked, { recursive: true });
    fs.writeFileSync(path.join(untracked, "evidence"), "still in use");
    const child = spawn("sleep", ["60"], { stdio: "ignore" });
    const exited = new Promise<void>((resolve) => child.once("exit", () => resolve()));
    try {
      await new Promise<void>((resolve, reject) => { child.once("spawn", resolve); child.once("error", reject); });
      const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: false }, {
        workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot,
      });
      managers.push(manager);
      const result = await manager.run({ task: "complete quickly", transport: "process" });
      expect(result.status).toBe("completed");
      const tracked = manager.runDirectory(result.id)!;
      await manager.close();
      expect(child.exitCode).toBeNull();
      expect(fs.existsSync(tracked)).toBe(false);
      expect(fs.existsSync(untracked)).toBe(true);
      expect(fs.readFileSync(path.join(untracked, "evidence"), "utf8")).toBe("still in use");
    } finally { child.kill(); await exited; }
  }, 30_000);

  // review/astra on e170d9e: a marked nested child keeps its completed parent's files too.
  it("keeps a completed parent run whose nested child is marked unresolved", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const runRoot = path.join(root, "runs");                   // a custom (not managed temp) root
    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: false }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
      runRoot,
    });
    managers.push(manager);
    const result = await manager.run({ task: "complete quickly", transport: "process" });
    expect(result.status).toBe("completed");
    const runDirectory = manager.runDirectory(result.id)!;
    markUnresolvedWorker(path.join(runDirectory, "nested", "child"), "the Herdr server has been unreachable for 300 s");
    await expect(manager.cleanup(result.id)).rejects.toThrow(/lost track of its worker/);
    await manager.close();
    expect(fs.existsSync(path.join(runDirectory, "nested", "child"))).toBe(true);
  }, 30_000);

  it("gives up retrying a child whose transport always exits before producing a result", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker-transport-death.mjs"),
      runRoot: root,
    });
    managers.push(manager);

    const result = await manager.run({ task: "Terminal boot death", transport: "process" });

    expect(result.status).toBe("failed");
    expect(result.error).toContain("Agent transport exited without a result");
    // AGENT_STARTUP_MAX_ATTEMPTS counts the initial launch: exactly 3 total.
    expect(
      fs.readFileSync(path.join(manager.runDirectory(result.id)!, "startup-attempts"), "utf8"),
    ).toBe("3");
  },
  30_000);

  it("resumes a run whose worker was stopped mid-run and carries its progress forward", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
      runRoot: root,
    });
    managers.push(manager);

    const result = await manager.run({ task: "RESUME_AFTER_STOP", transport: "process" });

    expect(result.status).toBe("completed");
    expect(result.text).toBe("resumed attempt 2");
    // The resumed attempt kept the stopped attempt's counters and usage, so the
    // settled result reports the whole run instead of only its final slice.
    expect(result.turns).toBe(6);
    expect(result.toolCalls).toBe(4);
    expect(result.usage.input).toBe(110);
    expect(result.usage.cost).toBeCloseTo(0.011, 6);
    // The resumed child is told what it is continuing, not handed a bare task.
    expect(result.task).toContain("[Fabric continuation]");
    const runDirectory = manager.runDirectory(result.id)!;
    expect(fs.readFileSync(path.join(runDirectory, "resume-attempts"), "utf8")).toBe("2");
  },
  30_000);

  // review/astra on #26: a failed relaunch is terminal. No fallback launch runs after it, so
  // the saved failure can never mask a later result.
  it("makes a failed resume relaunch terminal, with no fallback launch", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const launch = ProcessTransport.prototype.launch;
    let launches = 0;
    const spy = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, request) {
      launches++;
      if (launches === 2) throw new Error("transient launch failure");
      return launch.call(this, request);
    });
    try {
      const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
        workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
        runRoot: root,
      });
      managers.push(manager);
      const result = await manager.run({ task: "RESUME_AFTER_CRASH", transport: "process" });
      expect(launches).toBe(2);
      expect(result.status).toBe("failed");
      expect(result.error).toContain("relaunch failed: transient launch failure");
    } finally {
      spy.mockRestore();
    }
  }, 45_000);

  it("resumes a run whose transport died after doing work", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
      runRoot: root,
    });
    managers.push(manager);

    const result = await manager.run({ task: "RESUME_AFTER_CRASH", transport: "process" });

    expect(result.status).toBe("completed");
    expect(result.text).toBe("resumed attempt 2");
    expect(result.turns).toBe(1);
  },
  30_000);

  // smarty-dev#2184 item 8b: a removed actor's run (its caller aborted after progress, so it was
  // detached) was relaunched when its worker died, so it ran on and its removal never finished.
  it("ends an abandoned run as failed when its worker dies, with no relaunch", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
      runRoot: root,
    });
    managers.push(manager);
    const abort = new AbortController();
    const handle = await manager.spawn({ task: "HANG_WITH_PROGRESS", transport: "process" }, abort.signal);
    const runDirectory = manager.runDirectory(handle.id)!;
    await vi.waitFor(
      () => expect((manager.status(handle.id) as AgentRunRecord).turns).toBe(3),
      { timeout: 10_000 },
    );
    abort.abort();                                             // the actor was stopped: detached
    manager.abandon(handle.id);                                // and its run given up
    const worker = Number(handle.sessionId);
    process.kill(worker, "SIGKILL");
    const result = await Promise.race([
      manager.wait(handle.id),
      new Promise<"hung">((resolve) => setTimeout(resolve, 15_000, "hung")),
    ]);
    expect(result).not.toBe("hung");
    expect((result as AgentRunResult).status).toBe("failed");
    expect((result as AgentRunResult).error).toContain("Agent transport exited without a result");
    expect(fs.existsSync(path.join(runDirectory, "relaunches.jsonl"))).toBe(false);
  },
  30_000);

  // Review round 2 (security S3) on pi-fabric#160: an abandonment that lands while recovery is
  // already under way (after the backoff, or while the new worker launches) still stops it.
  it.each(["after the backoff", "during the relaunch"] as const)(
    "never keeps a relaunched worker for a run abandoned %s",
    async (when) => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
      roots.push(root);
      const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
        workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
        runRoot: root,
      });
      managers.push(manager);
      let id = "";
      const launch = ProcessTransport.prototype.launch;
      const launched: Array<{ stop(): Promise<void>; isAlive(): Promise<boolean> }> = [];
      const spy = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, request) {
        const handle = await launch.call(this, request);
        launched.push(handle);
        if (launched.length === 1 && when === "after the backoff") {
          // #relaunch stops the dead worker's transport first, after the backoff.
          const stop = handle.stop;
          handle.stop = async () => { manager.abandon(id); return stop(); };
        }
        if (launched.length === 2 && when === "during the relaunch") manager.abandon(id);
        return handle;
      });
      try {
        const handle = await manager.spawn({ task: "HANG_WITH_PROGRESS", transport: "process" });
        id = handle.id;
        await vi.waitFor(() => expect((manager.status(id) as AgentRunRecord).turns).toBe(3), { timeout: 10_000 });
        process.kill(Number(handle.sessionId), "SIGKILL");
        const result = await Promise.race([
          manager.wait(id),
          new Promise<"hung">((resolve) => setTimeout(resolve, 20_000, "hung")),
        ]);
        expect(result).not.toBe("hung");
        expect((result as AgentRunResult).status).toBe("failed");
        // The relaunched worker was signalled; it is gone within moments, not left running.
        for (const transport of launched) {
          await vi.waitFor(async () => expect(await transport.isAlive()).toBe(false), { timeout: 5_000 });
        }
        expect(launched.length).toBe(when === "after the backoff" ? 1 : 2);
      } finally {
        spy.mockRestore();
      }
    },
    40_000,
  );

  // Review round 1 on pi-fabric#160: a caller that only stopped waiting still wants the run.
  it("still resumes a detached run whose caller only stopped waiting when its worker dies", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
      runRoot: root,
    });
    managers.push(manager);
    const abort = new AbortController();
    const handle = await manager.spawn({ task: "HANG_WITH_PROGRESS", transport: "process" }, abort.signal);
    const runDirectory = manager.runDirectory(handle.id)!;
    await vi.waitFor(
      () => expect((manager.status(handle.id) as AgentRunRecord).turns).toBe(3),
      { timeout: 10_000 },
    );
    abort.abort();                                             // a canceled wait: detached, still wanted
    process.kill(Number(handle.sessionId), "SIGKILL");
    await vi.waitFor(
      () => expect(fs.existsSync(path.join(runDirectory, "relaunches.jsonl"))).toBe(true),
      { timeout: 15_000 },
    );
    expect(manager.status(handle.id).status).toBe("running");
    await manager.stop(handle.id);
  },
  30_000);

  it("never resumes a run an operator stopped, and aborts only unused runs", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
      runRoot: root,
    });
    managers.push(manager);

    const handle = await manager.spawn({ task: "LIVE_WITH_PROGRESS", transport: "process" });
    const runDirectory = manager.runDirectory(handle.id)!;
    await vi.waitFor(
      () => expect((manager.status(handle.id) as AgentRunRecord).turns).toBe(4),
      { timeout: 10_000 },
    );
    const stopped = await manager.stop(handle.id);
    expect(stopped.status).toBe("stopped");
    // An explicit stop is terminal: no relaunch follows it.
    await new Promise((resolve) => setTimeout(resolve, 1_500));
    expect(fs.readFileSync(path.join(runDirectory, "resume-attempts"), "utf8")).toBe("1");
  },
  30_000);

  it("detaches a run with work in flight when its caller aborts", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
      runRoot: root,
    });
    managers.push(manager);

    const controller = new AbortController();
    const handle = await manager.spawn(
      { task: "LIVE_WITH_PROGRESS", transport: "process" },
      controller.signal,
    );
    await vi.waitFor(
      () => expect((manager.status(handle.id) as AgentRunRecord).turns).toBe(4),
      { timeout: 10_000 },
    );
    controller.abort();

    // The caller is gone, but the run it started is not: it keeps working and
    // reports its own terminal state instead of being killed mid-flight.
    const result = await manager.wait(handle.id);
    expect(result.status).toBe("completed");
    expect(result.text).toBe("live attempt 1 complete");
  },
  30_000);

  it("keeps full results in the API and compact projections for the dashboard", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
      runRoot: root,
      fullCodeMode: false,
    });
    managers.push(manager);
    const result = await manager.run({ task: "LARGE_RESULT", transport: "process" });
    expect(result.text).toHaveLength(100_000);
    expect((result.value as { output: string }).output).toHaveLength(100_000);

    const records = manager.listForUi();
    const compact = records[0] as AgentRunRecord;
    expect(compact.text.length).toBeLessThanOrEqual(16_001);
    expect(compact.value).toMatchObject({ fabricTruncated: true });
    expect(manager.listForUi()).toBe(records);
    expect((manager.status(result.id) as AgentRunRecord).text).toHaveLength(100_000);
    expect((await manager.wait(result.id)).text).toHaveLength(100_000);
  });

  it("readLog returns the run's event stream and status", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
      runRoot: root,
      fullCodeMode: false,
    });
    managers.push(manager);
    const result = await manager.run({ task: "Inspect this repository", transport: "process" });
    expect(manager.runDirectory(result.id)).toBeDefined();
    const log = manager.readLog(result.id);
    expect(log.id).toBe(result.id);
    expect(log.logFile).toContain("events.jsonl");
    expect(log.runDirectory).toContain(path.basename(root));
    expect(log.status?.status).toBe("completed");
    const types = log.events.map((line) => (line.parsed as { type?: string } | undefined)?.type);
    expect(types).toContain("agent_start");
    expect(types).toContain("message_end");
    expect(types).toContain("agent_settled");
  });

  it("derives trusted log paths and recursively discovers bounded nested runs", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
      runRoot: root,
    });
    managers.push(manager);
    const result = await manager.run({ task: "Inspect nesting", transport: "process" });
    const runDirectory = manager.runDirectory(result.id)!;
    const topStatus = JSON.parse(fs.readFileSync(path.join(runDirectory, "status.json"), "utf8"));
    fs.writeFileSync(
      path.join(runDirectory, "status.json"),
      JSON.stringify({ ...topStatus, logFile: "/tmp/untrusted-top.jsonl" }),
    );
    const childDirectory = path.join(runDirectory, "nested", "child");
    const grandchildDirectory = path.join(childDirectory, "nested", "grandchild");
    fs.mkdirSync(grandchildDirectory, { recursive: true });
    fs.writeFileSync(
      path.join(childDirectory, "status.json"),
      JSON.stringify({ ...result, id: "child", name: "child", logFile: "/tmp/untrusted-child.jsonl" }),
    );
    fs.writeFileSync(
      path.join(grandchildDirectory, "status.json"),
      JSON.stringify({ ...result, id: "grandchild", name: "grandchild", logFile: "/tmp/untrusted-grandchild.jsonl" }),
    );

    const status = manager.status(result.id) as AgentRunRecord;
    expect(status.logFile).toBe(path.join(runDirectory, "events.jsonl"));
    expect(status.nestedAgents?.[0]?.logFile).toBe(path.join(childDirectory, "events.jsonl"));
    expect(status.nestedAgents?.[0]?.nestedAgents?.[0]?.logFile).toBe(
      path.join(grandchildDirectory, "events.jsonl"),
    );

    status.nestedAgents![0]!.name = "caller mutation";
    fs.rmSync(path.join(runDirectory, "nested"), { recursive: true, force: true });
    const retained = manager.status(result.id) as AgentRunRecord;
    expect(retained.nestedAgents?.[0]?.name).toBe("child");
    expect(retained.nestedAgents?.[0]?.nestedAgents?.[0]?.name).toBe("grandchild");
  });

  it("captures recursive leaves before the child process removes their directories", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
      runRoot: root,
    });
    managers.push(manager);
    const handle = await manager.spawn({
      task: "HANG while nested agents finish",
      transport: "process",
      recursive: true,
    });
    const runDirectory = manager.runDirectory(handle.id)!;
    const statusFile = path.join(runDirectory, "status.json");
    const deadline = Date.now() + 2_000;
    while (!fs.existsSync(statusFile) && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
    const parentStatus = JSON.parse(fs.readFileSync(statusFile, "utf8"));
    const leafDirectory = path.join(runDirectory, "nested", "finished-leaf");
    fs.mkdirSync(leafDirectory, { recursive: true });
    fs.writeFileSync(
      path.join(leafDirectory, "status.json"),
      JSON.stringify({
        ...parentStatus,
        id: "finished-leaf",
        name: "finished leaf",
        status: "completed",
        finishedAt: Date.now(),
      }),
    );

    await new Promise((resolve) => setTimeout(resolve, 250));
    fs.rmSync(path.join(runDirectory, "nested"), { recursive: true, force: true });
    const retained = manager.status(handle.id) as AgentRunRecord;
    expect(retained.nestedAgents?.[0]).toMatchObject({
      id: "finished-leaf",
      name: "finished leaf",
      status: "completed",
    });
    await manager.stop(handle.id);
  });

  it("inherits full code mode for ordinary extension-enabled children", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
      runRoot: root,
      fullCodeMode: true,
      mainAgentId: "session:root-main",
    });
    managers.push(manager);
    const direct = (await manager.run({
      task: "Use Fabric tools",
      transport: "process",
      tools: ["read", "grep"],
    })) as FabricSurfaceResult;
    expect(direct.fullCodeMode).toBe("true");
    expect(direct.tools).toEqual(["read", "grep", "fabric_exec"]);
    expect(direct.extensions).toBe("true");
    expect(direct.mainAgentId).toBe("session:root-main");
    expect(direct.fabricExtension).toContain("index");
    expect(direct.grantedRisks).toEqual([]);

    const recursive = (await manager.run({
      task: "Delegate recursively",
      transport: "process",
      tools: ["read"],
      recursive: true,
    })) as FabricSurfaceResult;
    expect(recursive.fullCodeMode).toBe("true");
    expect(recursive.tools).toEqual(["read", "fabric_exec"]);
    expect(recursive.mainAgentId).toBe("session:root-main");
    expect(recursive.grantedRisks).toEqual(["agent"]);
  });

  it("keeps explicit extensions:false children native in a full-code parent", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
      runRoot: root,
      fullCodeMode: true,
    });
    managers.push(manager);
    const native = (await manager.run({
      task: "Native opt-out",
      transport: "process",
      tools: ["read"],
      extensions: false,
    })) as FabricSurfaceResult;
    expect(native.fullCodeMode).toBe("false");
    expect(native.tools).toEqual(["read"]);
    expect(native.fabricExtension).toBeUndefined();
    expect(native.grantedRisks).toEqual([]);
  });

  it("keeps ordinary children native when the parent is not full-code", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
      runRoot: root,
      fullCodeMode: false,
    });
    managers.push(manager);
    const direct = (await manager.run({
      task: "Native child of native parent",
      transport: "process",
      tools: ["read"],
    })) as FabricSurfaceResult;
    expect(direct.fullCodeMode).toBe("false");
    expect(direct.tools).toEqual(["read"]);
    expect(direct.fabricExtension).toBeUndefined();

    // Recursive children keep their recursive surface even from a native parent.
    const recursive = (await manager.run({
      task: "Delegate recursively from a native parent",
      transport: "process",
      tools: ["read"],
      recursive: true,
    })) as FabricSurfaceResult;
    expect(recursive.fullCodeMode).toBe("false");
    expect(recursive.tools).toEqual(["read", "fabric_exec"]);
    expect(recursive.fabricExtension).toContain("index");
    expect(recursive.grantedRisks).toEqual(["agent"]);
  });

  it("allows a cwd leaf agent to inherit the Fabric surface", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const leafCwd = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-leaf-cwd-"));
    roots.push(leafCwd);
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
      runRoot: root,
      fullCodeMode: true,
    });
    managers.push(manager);
    const result = (await manager.run({
      task: "Leaf agent with a custom cwd",
      transport: "process",
      tools: ["read"],
      cwd: leafCwd,
    })) as FabricSurfaceResult;
    expect(result.status).toBe("completed");
    expect(result.cwd).toBe(fs.realpathSync(leafCwd));
    expect(result.fullCodeMode).toBe("true");
    expect(result.tools).toEqual(["read", "fabric_exec"]);
    expect(result.grantedRisks).toEqual([]);
  });

  it.each(['["read","fabric_exec"]', 'invalid'])('does not widen recursive alternate-cwd authority (%s)', async (allowlist) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-recursive-security-"));
    roots.push(root);
    const target = path.join(root, "target");
    fs.mkdirSync(target);
    const saved = process.env.PI_FABRIC_TOOL_ALLOWLIST;
    process.env.PI_FABRIC_TOOL_ALLOWLIST = allowlist;
    let manager: AgentManager;
    try {
      manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
        workerPath: path.resolve("src/worker.ts"),
        piBinary: path.resolve("tests/fixtures/fake-pi-launch-probe.mjs"),
        runRoot: path.join(root, "runs"), fullCodeMode: true,
        projectRoot: process.cwd(), meshRoot: path.join(root, "mesh"),
        mainAgentId: "root:security", kernel: () => "python", pythonRuntime: () => "monty",
      });
      managers.push(manager);
    } finally {
      if (saved === undefined) delete process.env.PI_FABRIC_TOOL_ALLOWLIST;
      else process.env.PI_FABRIC_TOOL_ALLOWLIST = saved;
    }
    const result = await manager.run({
      task: "REPORT_LAUNCH_SURFACE", cwd: target, recursive: true,
      tools: ["read", "bash", "write"], transport: "process",
      capabilityRequirements: ["pi.read"],
    });
    expect(result.status).toBe("completed");
    const tools = allowlist === 'invalid' ? ["fabric_exec"] : ["read", "fabric_exec"];
    expect(JSON.parse(result.text)).toMatchObject({
      cwd: fs.realpathSync(target), trustFlags: [], tools, toolAllowlistEnv: tools,
      grantedRisksEnv: ["agent"], fullCodeModeEnv: "true", extensions: true,
      projectRoot: process.cwd(), meshRoot: path.join(root, "mesh"),
      kernel: "python", pythonRuntime: "monty", depth: "1", mainAgentId: "root:security",
      capabilityRequirements: ["pi.read"],
    });
  });

  it("launches inherited children with the full-code surface through the real worker", { timeout: 15_000 }, async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const fakePi = path.resolve("tests/fixtures/fake-pi-launch-probe.mjs");
    fs.chmodSync(fakePi, 0o755);
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("src/worker.ts"),
      piBinary: fakePi,
      runRoot: root,
      fullCodeMode: true,
    });
    managers.push(manager);

    const inherited = await manager.run({
      task: "REPORT_LAUNCH_SURFACE",
      transport: "process",
      tools: ["read"],
      timeoutMs: 5_000,
    });
    expect(inherited.status).toBe("completed");
    expect(JSON.parse(inherited.text)).toMatchObject({
      extensions: true,
      extensionPath: expect.stringContaining("index"),
      tools: ["read", "fabric_exec"],
      fullCodeModeEnv: "true",
      toolAllowlistEnv: ["read", "fabric_exec"],
      grantedRisksEnv: [],
    });

    const native = await manager.run({
      task: "REPORT_LAUNCH_SURFACE",
      transport: "process",
      tools: ["read"],
      extensions: false,
      timeoutMs: 5_000,
    });
    expect(native.status).toBe("completed");
    expect(JSON.parse(native.text).extensionPath).toBeUndefined();
    expect(JSON.parse(native.text)).toMatchObject({
      extensions: false,
      tools: ["read"],
      fullCodeModeEnv: "false",
      toolAllowlistEnv: ["read"],
      grantedRisksEnv: [],
    });
  });

  it("validates structured output through the real Fabric worker", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const fakePi = path.resolve("tests/fixtures/fake-pi-rpc.mjs");
    fs.chmodSync(fakePi, 0o755);
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("src/worker.ts"),
      piBinary: fakePi,
      runRoot: root,
      fullCodeMode: false,
    });
    managers.push(manager);
    const result = await manager.run({
      task: "Return a directive",
      transport: "process",
      systemPrompt: "You are a test actor.",
      sessionFile: path.join(root, "actor-session.jsonl"),
      actorId: "actor-test",
      actorName: "test-actor",
      meshRoot: path.join(root, "mesh"),
      schema: {
        type: "object",
        properties: {
          action: { type: "string", enum: ["message"] },
          message: { type: "string" },
        },
        required: ["action", "message"],
        additionalProperties: false,
      },
    });
    expect(result.status).toBe("completed");
    expect(result.value).toEqual({
      action: "message",
      message: "validated actor response:false",
    });
    expect(result.usage).toMatchObject({ input: 3, output: 4 });
  });

  it("propagates the exact root Main identity into recursive child Pi", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const fakePi = path.resolve("tests/fixtures/fake-pi-rpc.mjs");
    fs.chmodSync(fakePi, 0o755);
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("src/worker.ts"),
      piBinary: fakePi,
      runRoot: root,
      fullCodeMode: true,
      mainAgentId: "session:root-main",
    });
    managers.push(manager);

    const result = await manager.run({
      task: "REPORT_FABRIC_IDENTITY",
      name: "recursive implementor",
      transport: "process",
      recursive: true,
      timeoutMs: 5_000,
    });

    expect(result.status).toBe("completed");
    expect(JSON.parse(result.text)).toEqual({
      mainAgentId: "session:root-main",
      parentRun: result.id,
      agentName: "recursive implementor",
    });
  });

  it("marks ordinary process children as task agents without replacing actor identity (smarty-dev#2088)", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const fakePi = path.resolve("tests/fixtures/fake-pi-rpc.mjs");
    fs.chmodSync(fakePi, 0o755);
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("src/worker.ts"),
      piBinary: fakePi,
      runRoot: root,
    });
    managers.push(manager);
    const report = async (actorName?: string): Promise<unknown> => {
      const result = await manager.run({
        task: "REPORT_FLEET_ROLE", transport: "process", timeoutMs: 5_000,
        ...(actorName ? { actorId: "fleet-role-test", actorName } : {}),
      });
      expect(result.status).toBe("completed");
      return JSON.parse(result.text);
    };

    try {
      vi.stubEnv("SMARTY_ROLE", "worktree-agent@abc123");
      vi.stubEnv("PI_FABRIC_ACTOR_NAME", undefined);
      vi.stubEnv("PI_FABRIC_ROLE", undefined);
      expect(await report()).toEqual({ role: "task-agent", actorName: null, fabricRole: null });
      expect(await report("security-review")).toEqual({
        role: "worktree-agent@abc123", actorName: "security-review", fabricRole: null,
      });
      vi.stubEnv("SMARTY_ROLE", undefined);
      expect(await report()).toEqual({ role: "task-agent", actorName: null, fabricRole: null });
      expect(await report("security-review")).toEqual({
        role: null, actorName: "security-review", fabricRole: null,
      });
      // These inherited identities are deliberately unchanged: the governor prioritizes actors,
      // and participantRole prioritizes PI_FABRIC_ROLE over SMARTY_ROLE.
      vi.stubEnv("PI_FABRIC_ACTOR_NAME", "parent-actor");
      vi.stubEnv("PI_FABRIC_ROLE", "project-agent");
      expect(await report()).toEqual({
        role: "task-agent", actorName: "parent-actor", fabricRole: "project-agent",
      });
    } finally {
      vi.unstubAllEnvs();
    }
  });

  it("keeps the RPC worker alive when Pi announces a retry", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const fakePi = path.resolve("tests/fixtures/fake-pi-rpc.mjs");
    fs.chmodSync(fakePi, 0o755);
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("src/worker.ts"),
      piBinary: fakePi,
      runRoot: root,
      fullCodeMode: true,
    });
    managers.push(manager);

    const result = await manager.run({
      task: "RETRY_THEN_SUCCEED",
      transport: "process",
      timeoutMs: 5_000,
    });

    expect(result.status).toBe("completed");
    expect(result.text).toBe("retry recovered");
    expect(result.error).toBeUndefined();
    expect(result.exitCode).toBe(0);
  });

  it("preserves provider diagnostics when the final agent attempt fails", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const fakePi = path.resolve("tests/fixtures/fake-pi-rpc.mjs");
    fs.chmodSync(fakePi, 0o755);
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("src/worker.ts"),
      piBinary: fakePi,
      runRoot: root,
      fullCodeMode: true,
    });
    managers.push(manager);

    const result = await manager.run({
      task: "FAIL_PROVIDER",
      transport: "process",
      timeoutMs: 5_000,
    });

    expect(result.status).toBe("failed");
    expect(result.exitCode).toBe(0);
    expect(result.error).toContain("openai-codex/gpt-test: fetch failed · WebSocket error");
    expect(result.error).not.toContain("exited with code 0");
  });

  it("forwards the configured default model when a call omits one", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const config = { ...DEFAULT_FABRIC_CONFIG.agents, model: "claude-sonnet-4-5" };
    const manager = new AgentManager(process.cwd(), config, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
      runRoot: root,
      fullCodeMode: false,
    });
    managers.push(manager);
    const result = await manager.run({ task: "Use the default model", transport: "process" });
    expect(result.status).toBe("completed");
    expect(result.model).toBe("claude-sonnet-4-5");
  });

  it("lets a per-call model override the configured default", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const config = { ...DEFAULT_FABRIC_CONFIG.agents, model: "claude-sonnet-4-5" };
    const manager = new AgentManager(process.cwd(), config, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
      runRoot: root,
      fullCodeMode: false,
    });
    managers.push(manager);
    const result = await manager.run({
      task: "Override the model",
      transport: "process",
      model: "gpt-override",
    });
    expect(result.status).toBe("completed");
    expect(result.model).toBe("gpt-override");
  });

  it("forwards the configured default thinking when a call omits one", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const config = { ...DEFAULT_FABRIC_CONFIG.agents, thinking: "high" as const };
    const manager = new AgentManager(process.cwd(), config, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
      runRoot: root,
      fullCodeMode: false,
    });
    managers.push(manager);
    const result = await manager.run({ task: "Use the default thinking", transport: "process" });
    expect(result.status).toBe("completed");
    expect(result.thinking).toBe("high");
  });

  it("lets a per-call thinking override the configured default", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const config = { ...DEFAULT_FABRIC_CONFIG.agents, thinking: "high" as const };
    const manager = new AgentManager(process.cwd(), config, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
      runRoot: root,
      fullCodeMode: false,
    });
    managers.push(manager);
    const result = await manager.run({
      task: "Override the thinking",
      transport: "process",
      thinking: "max",
    });
    expect(result.status).toBe("completed");
    expect(result.thinking).toBe("max");
  });

  it("forwards the medium default when neither config nor call set a thinking level", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
      runRoot: root,
      fullCodeMode: false,
    });
    managers.push(manager);
    const result = await manager.run({ task: "Default medium thinking", transport: "process" });
    expect(result.status).toBe("completed");
    expect(result.thinking).toBe("medium");
  });

  it("inherits the host model when neither config nor call set one", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
      runRoot: root,
      fullCodeMode: false,
    });
    managers.push(manager);
    const result = await manager.run({ task: "Inherit the host model", transport: "process" });
    expect(result.status).toBe("completed");
    expect(result.model).toBeUndefined();
  });

  it("notifies when a detached background agent completes", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    let resolveCompletion: ((text: string) => void) | undefined;
    const completion = new Promise<string>((resolve) => {
      resolveCompletion = resolve;
    });
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
      runRoot: root,
      onBackgroundComplete: (result) => resolveCompletion?.(result.text),
    });
    managers.push(manager);
    const handle = await manager.spawn({ task: "Background task", transport: "process" });
    manager.detachSignal(handle.id);
    await expect(completion).resolves.toBe("fake worker complete");
  });

  // smarty-dev#854: agents.wait without a bound blocked its session for over an hour.
  it("bounds a wait: the run continues, nothing is consumed, and its completion is still delivered", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    let resolveCompletion: ((text: string) => void) | undefined;
    const completion = new Promise<string>((resolve) => {
      resolveCompletion = resolve;
    });
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
      runRoot: root,
      onBackgroundComplete: (result) => resolveCompletion?.(result.text),
    });
    managers.push(manager);
    const handle = await manager.spawn({ task: "LIVE_WITH_PROGRESS", transport: "process" });   // runs 1.5 s
    await expect(manager.wait(handle.id, { timeoutMs: 300 })).rejects.toThrow(/is still running after 0\.3 s\. It continues/);
    expect(manager.status(handle.id).status).toBe("running");
    await expect(completion).resolves.toMatch(/^live attempt \d+ complete$/);
  }, 15_000);

  it.each([false, true])("cancels only a Main wait observation and preserves completion (already aborted: %s)", async alreadyAborted => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-main-wait-"));
    roots.push(root);
    const consumed = vi.fn();
    let resolveCompletion!: (text: string) => void;
    const completion = new Promise<string>(resolve => { resolveCompletion = resolve; });
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: root,
      onBackgroundComplete: result => resolveCompletion(result.text),
      onResultConsumed: consumed,
    });
    managers.push(manager);
    const handle = await manager.spawn({ task: "LIVE_WITH_PROGRESS", transport: "process" });
    const controller = new AbortController();
    if (alreadyAborted) controller.abort(new Error("Main ceiling hit"));
    const wait = manager.wait(handle.id, { timeoutMs: 60_000, signal: controller.signal });
    if (!alreadyAborted) controller.abort(new Error("Main ceiling hit"));
    // A bounded assertion lets the regression fail promptly on the unfixed base.
    const observation = Promise.race([wait, new Promise((_, reject) => setTimeout(() => reject(new Error("observation remained blocked")), 100))]);
    await expect(observation).rejects.toThrow("Main ceiling hit");
    expect(manager.status(handle.id).status).toBe("running");
    expect(consumed).not.toHaveBeenCalled();
    await expect(completion).resolves.toMatch(/^live attempt \d+ complete$/);
    expect(consumed).not.toHaveBeenCalled();
  });

  it("surfaces the run-log tail when a worker exits without a terminal result", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker-crash.mjs"),
      runRoot: root,
    });
    managers.push(manager);
    const result = await manager.run({ task: "crash test", transport: "process" });
    expect(result.status).toBe("failed");
    expect(result.error).toContain("exited without a result");
    expect(result.error).toContain("model rate limit exceeded");
    expect(result.error).toContain("worker_stderr: provider authentication failed retry required");
  },
  30_000);

  it("rejects empty tasks", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-manager-"));
    roots.push(root);
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
      runRoot: root,
    });
    managers.push(manager);
    await expect(manager.spawn({ task: "" })).rejects.toThrow("must not be empty");
  });

  it("enforces a cross-process cost budget across spawned agents", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-budget-"));
    roots.push(root);
    const config = { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0.1 };
    const manager = new AgentManager(process.cwd(), config, {
      workerPath: path.resolve("tests/fixtures/fake-worker-budget.mjs"),
      runRoot: root,
    });
    managers.push(manager);

    const first = await manager.run({ task: "COST 0.06", transport: "process" });
    expect(first.status).toBe("completed");
    expect(first.usage.cost).toBeCloseTo(0.06);
    expect(first.budget).toBeDefined();
    expect(first.budget?.limit).toBe(0.1);
    expect(first.budget?.spent).toBeCloseTo(0.06);
    expect(first.budget?.remaining).toBeCloseTo(0.04);

    // The check runs before the child lands its cost, so a tree may slightly
    // overshoot (matching ypi's best-effort RLM_BUDGET semantics).
    const second = await manager.run({ task: "COST 0.06", transport: "process" });
    expect(second.status).toBe("completed");
    expect(second.budget?.spent).toBeCloseTo(0.12);
    expect(second.budget?.remaining).toBe(0);

    // A third call is rejected because the accumulated spend now meets the budget.
    await expect(manager.spawn({ task: "COST 0.06", transport: "process" })).rejects.toThrow(
      /budget exceeded/,
    );
  });

  it("inherits a budget ledger from the environment for recursive children", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-budget-"));
    roots.push(root);
    process.env.PI_FABRIC_BUDGET = "0.05";
    process.env.PI_FABRIC_BUDGET_FILE = path.join(root, "tree-cost.jsonl");
    process.env.PI_FABRIC_BUDGET_ID = "inherited-tree";
    fs.writeFileSync(process.env.PI_FABRIC_BUDGET_FILE, "", { mode: 0o600 });
    try {
      const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
        workerPath: path.resolve("tests/fixtures/fake-worker-budget.mjs"),
        runRoot: root,
      });
      managers.push(manager);

      const result = await manager.run({ task: "COST 0.02", transport: "process" });
      expect(result.budget?.limit).toBe(0.05);
      expect(result.budget?.spent).toBeCloseTo(0.02);
      expect(result.budget?.remaining).toBeCloseTo(0.03);

      const ledger = fs.readFileSync(process.env.PI_FABRIC_BUDGET_FILE, "utf8");
      expect(ledger).toContain("\"cost\":0.02");
    } finally {
      delete process.env.PI_FABRIC_BUDGET;
      delete process.env.PI_FABRIC_BUDGET_FILE;
      delete process.env.PI_FABRIC_BUDGET_ID;
    }
  });

  it("attributes token usage per tokens.usage events and closes the settle gap", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-budget-"));
    roots.push(root);
    process.env.PI_FABRIC_BUDGET = "0.05";
    process.env.PI_FABRIC_BUDGET_FILE = path.join(root, "tree-cost.jsonl");
    process.env.PI_FABRIC_BUDGET_ID = "attributed-tree";
    fs.writeFileSync(process.env.PI_FABRIC_BUDGET_FILE, "", { mode: 0o600 });
    try {
      const life: Array<{ event: string; data?: unknown }> = [];
      const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
        workerPath: path.resolve("tests/fixtures/fake-worker-usage.mjs"),
        runRoot: root,
        onLifecycle: (event) => life.push({ event: event.event, data: event.data }),
      });
      managers.push(manager);

      const result = await manager.run({
        task: "anything",
        transport: "process",
        actorId: "actor-test",
        actorName: "attr-test",
      });
      expect(result.status).toBe("completed");

      const usageEvents = life.filter((entry) => entry.event === "tokens.usage");
      expect(usageEvents).toHaveLength(1);
      const payload = usageEvents[0]!.data as {
        runId: string; runner: string; depth: number; actorId?: string; cumulativeTokens: number;
        input: number; output: number; cost: number;
      };
      expect(payload.runner).toBe("pi");
      expect(payload.depth).toBe(1);
      expect(payload.actorId).toBe("actor-test");
      expect(payload.cumulativeTokens).toBe(13);
      expect(payload.input).toBe(4);
      expect(payload.output).toBe(6);

      // Live-delta path recorded 13 tokens; settle closes the remaining 10
      // from the status file (input 8 + output 10 + cacheRead 3 + cacheWrite 2).
      const detail = readBudgetLedgerDetailed(process.env.PI_FABRIC_BUDGET_FILE);
      const totalCost = detail.entries.reduce((sum, entry) => sum + entry.cost, 0);
      const totalTokens = detail.entries.reduce((sum, entry) => sum + entry.tokens, 0);
      expect(totalTokens).toBe(23);
      expect(totalCost).toBeCloseTo(0.0015);
      expect(detail.byRunner.pi?.tokens).toBe(23);
      expect(detail.byRunner.pi?.cost).toBeCloseTo(0.0015);
      expect(detail.byActor["actor-test"]?.tokens).toBe(23);
    } finally {
      clearOwnedBudgetEnv();
    }
  });

  it("terminates a child that exceeds the per-child token limit", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-tokens-"));
    roots.push(root);
    const fakePi = path.resolve("tests/fixtures/fake-pi-rpc.mjs");
    fs.chmodSync(fakePi, 0o755);
    // The fake pi emits one assistant turn with 7 tokens (input 3 + output 4);
    // a 5-token ceiling trips the guard after the first message_end.
    const config = { ...DEFAULT_FABRIC_CONFIG.agents, maxTokensPerChild: 5 };
    const manager = new AgentManager(process.cwd(), config, {
      workerPath: path.resolve("src/worker.ts"),
      piBinary: fakePi,
      runRoot: root,
      fullCodeMode: false,
    });
    managers.push(manager);
    const result = await manager.run({
      task: "burn tokens",
      transport: "process",
      timeoutMs: 5_000,
    });
    expect(result.status).toBe("timed_out");
    expect(result.error ?? "").toMatch(/token limit/i);
    expect(result.error ?? "").toMatch(/7 tokens/);
    // The parent model reads this error verbatim: it must name the config key
    // and remedy so the failure is actionable without reading worker.ts.
    expect(result.error ?? "").toContain("agents.maxTokensPerChild");
    expect(result.error ?? "").toContain("/fabric settings");
  });
});

describe("AgentManager multimodal prompts", () => {
  it("forwards image blocks to the Pi worker RPC prompt", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-images-"));
    roots.push(root);
    const promptLog = path.join(root, "prompt.json");
    process.env.FAKE_PI_BEHAVIOR = "capture-prompt";
    process.env.FAKE_PI_PROMPT_LOG = promptLog;
    try {
      const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
        workerPath: path.resolve("src/worker.ts"),
        piBinary: path.resolve("tests/fixtures/fake-pi.mjs"),
        runRoot: path.join(root, "runs"),
      });
      managers.push(manager);
      const result = await manager.run({
        task: "Inspect the attached image",
        transport: "process",
        images: [{ type: "image", data: "aGVsbG8=", mimeType: "image/png" }],
      });

      expect(result.status).toBe("completed");
      const frame = JSON.parse(fs.readFileSync(promptLog, "utf8")) as Record<string, unknown>;
      expect(frame).toEqual({
        type: "prompt",
        message: "Inspect the attached image",
        images: [{ type: "image", data: "aGVsbG8=", mimeType: "image/png" }],
      });
      expect(fs.existsSync(path.join(manager.runDirectory(result.id)!, "images.json"))).toBe(false);
    } finally {
      delete process.env.FAKE_PI_BEHAVIOR;
      delete process.env.FAKE_PI_PROMPT_LOG;
    }
  });
});

describe("AgentManager Claude runner", () => {
  const fakeClaude = path.resolve("tests/fixtures/fake-claude.mjs");

  it("uses the independent configured Claude runner and model defaults", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-claude-"));
    roots.push(root);
    const config = {
      ...DEFAULT_FABRIC_CONFIG.agents,
      runner: "claude" as const,
      model: "openai/pi-only",
      claude: { ...DEFAULT_FABRIC_CONFIG.agents.claude, model: "claude/haiku" },
    };
    const manager = new AgentManager(process.cwd(), config, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
      runRoot: root,
    });
    managers.push(manager);

    const result = await manager.run({ task: "Use Claude defaults", transport: "process" });
    expect(result).toMatchObject({
      status: "completed",
      runner: "claude",
      model: "claude/haiku",
    });
  });

  it("runs Claude stream-json with mapped tools, native schema output, and usage", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-claude-"));
    roots.push(root);
    const invocationLog = path.join(root, "claude-args.jsonl");
    process.env.FAKE_CLAUDE_LOG = invocationLog;
    try {
      const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
        workerPath: path.resolve("src/worker.ts"),
        claudeBinary: fakeClaude,
        runRoot: root,
      });
      managers.push(manager);
      const result = await manager.run({
        task: "Return structured output",
        runner: "claude",
        transport: "process",
        model: "claude/haiku",
        thinking: "minimal",
        tools: ["read", "grep", "find", "ls"],
        schema: {
          type: "object",
          properties: { ok: { type: "boolean" } },
          required: ["ok"],
          additionalProperties: false,
        },
      });

      expect(result).toMatchObject({
        status: "completed",
        runner: "claude",
        model: "claude/haiku",
        thinking: "minimal",
        turns: 2,
        toolCalls: 1,
        value: { ok: true },
        runnerSessionId: "11111111-1111-4111-8111-111111111111",
        usage: { input: 10, output: 7, cacheRead: 2, cacheWrite: 3, cost: 0.001 },
      });
      const invocation = JSON.parse(fs.readFileSync(invocationLog, "utf8").trim()) as {
        argv: string[];
      };
      expect(invocation.argv).toEqual(
        expect.arrayContaining([
          "--model",
          "haiku",
          "--effort",
          "low",
          "--tools",
          "Read,Grep,Glob",
          "--allowedTools",
          "Read,Grep,Glob",
          "--no-session-persistence",
        ]),
      );
      expect(invocation.argv).not.toContain("fabric_exec");
    } finally {
      delete process.env.FAKE_CLAUDE_LOG;
    }
  });

  it("preserves Claude result diagnostics on a failed run", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-claude-"));
    roots.push(root);
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("src/worker.ts"),
      claudeBinary: fakeClaude,
      runRoot: root,
    });
    managers.push(manager);

    const result = await manager.run({
      task: "CLAUDE_FAIL",
      runner: "claude",
      transport: "process",
      tools: ["read"],
    });
    expect(result).toMatchObject({
      status: "failed",
      runner: "claude",
      error: "fake Claude failure",
      exitCode: 0,
    });
  });

  it("delivers Claude steering and follow-up messages on later turns", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-claude-"));
    roots.push(root);
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("src/worker.ts"),
      claudeBinary: fakeClaude,
      runRoot: root,
    });
    managers.push(manager);

    const handle = await manager.spawn({
      task: "Initial Claude task",
      runner: "claude",
      transport: "process",
      tools: ["read"],
    });
    manager.steer(handle.id, "Redirect the active analysis");
    manager.followUp(handle.id, "Check one final detail");
    const result = await manager.wait(handle.id);

    expect(result).toMatchObject({
      status: "completed",
      runner: "claude",
      turns: 6,
      toolCalls: 3,
      usage: { input: 30, output: 21, cacheRead: 6, cacheWrite: 9, cost: 0.003 },
      pendingMessages: { steering: [], followUp: [] },
    });
  });

  it("enumerates models from the Claude runtime control handshake", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-claude-"));
    roots.push(root);
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      claudeBinary: fakeClaude,
      runRoot: root,
    });
    managers.push(manager);

    const models = await manager.claudeModels();
    expect(models.map((model) => model.value)).toEqual(["default", "haiku"]);
    expect(models[1]).toMatchObject({
      value: "haiku",
      resolvedModel: "claude-haiku-test",
      displayName: "Haiku (test)",
    });
  });

  it("rejects recursive Fabric and unsupported tools before launching Claude", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-claude-"));
    roots.push(root);
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      claudeBinary: fakeClaude,
      runRoot: root,
    });
    managers.push(manager);

    await expect(
      manager.run({ task: "recurse", runner: "claude", recursive: true }),
    ).rejects.toThrow(/does not support recursive Fabric/);
    await expect(
      manager.run({ task: "unknown tool", runner: "claude", tools: ["custom"] }),
    ).rejects.toThrow(/does not support Fabric tool/);
    await expect(
      manager.run({ task: "prototype tool", runner: "claude", tools: ["__proto__"] }),
    ).rejects.toThrow(/does not support Fabric tool/);
    await expect(
      manager.run({ task: "blank model", runner: "claude", model: "claude/" }),
    ).rejects.toThrow(/must include a runtime model value/);
  });
});

describe("AgentManager steering", () => {
  const fakeWorker = path.resolve("tests/fixtures/fake-worker.mjs");
  const fakePiSteer = path.resolve("tests/fixtures/fake-pi-rpc-steer.mjs");

  const waitFor = async (predicate: () => boolean, timeoutMs = 2_000): Promise<void> => {
    const deadline = Date.now() + timeoutMs;
    while (!predicate()) {
      if (Date.now() >= deadline) throw new Error("Timed out waiting for steer state");
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
  };

  const readSteerFile = (runDir: string): Array<Record<string, unknown>> => {
    const file = path.join(runDir, "steer.jsonl");
    if (!fs.existsSync(file)) return [];
    return fs
      .readFileSync(file, "utf8")
      .split("\n")
      .filter((line) => line.trim())
      .map((line) => JSON.parse(line) as Record<string, unknown>);
  };

  const hangManager = (root: string, workerPath = fakeWorker, piBinary?: string) => {
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath,
      ...(piBinary ? { piBinary } : {}),
      runRoot: root,
      fullCodeMode: false,
    });
    managers.push(manager);
    return manager;
  };

  it("steer appends a queued steer command for a running agent", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-steer-"));
    roots.push(root);
    const manager = hangManager(root);
    const handle = await manager.spawn({ task: "HANG", transport: "process" });
    const result = manager.steer(handle.id, "drop the token branch");
    expect(result).toEqual({ queued: true, messageId: expect.any(String) });
    const entries = readSteerFile(manager.runDirectory(handle.id)!);
    expect(entries).toHaveLength(1);
    expect(entries[0]).toMatchObject({ type: "steer", message: "drop the token branch" });
    await manager.stop(handle.id);
  });

  it("steer throws for a finished agent", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-steer-"));
    roots.push(root);
    const manager = hangManager(root);
    const result = await manager.run({ task: "done", transport: "process" });
    expect(() => manager.steer(result.id, "too late")).toThrow(/already finished/);
  });

  it("followUp appends a follow_up command", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-steer-"));
    roots.push(root);
    const manager = hangManager(root);
    const handle = await manager.spawn({ task: "HANG", transport: "process" });
    manager.followUp(handle.id, "then summarize");
    const entries = readSteerFile(manager.runDirectory(handle.id)!);
    expect(entries[0]).toMatchObject({ type: "follow_up", message: "then summarize" });
    await manager.stop(handle.id);
  });

  it("setSteeringMode and setFollowUpMode append mode commands", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-steer-"));
    roots.push(root);
    const manager = hangManager(root);
    const handle = await manager.spawn({ task: "HANG", transport: "process" });
    manager.setSteeringMode(handle.id, "all");
    manager.setFollowUpMode(handle.id, "one-at-a-time");
    const entries = readSteerFile(manager.runDirectory(handle.id)!);
    expect(entries[0]).toMatchObject({ type: "set_steering_mode", mode: "all" });
    expect(entries[1]).toMatchObject({ type: "set_follow_up_mode", mode: "one-at-a-time" });
    await manager.stop(handle.id);
  });

  it("forwards a steer to the child pi over RPC and surfaces pendingMessages", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-steer-"));
    roots.push(root);
    fs.chmodSync(fakePiSteer, 0o755);
    const received = path.join(root, "received.jsonl");
    process.env.FAKE_PI_STEER_LOG = received;
    try {
      const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
        workerPath: path.resolve("src/worker.ts"),
        piBinary: fakePiSteer,
        runRoot: root,
        fullCodeMode: false,
      });
      managers.push(manager);
      const handle = await manager.spawn({ task: "STEER_ME", transport: "process" });
      await waitFor(() => manager.status(handle.id).status === "running");
      manager.steer(handle.id, "redirect to session expiry");
      await waitFor(
        () =>
          fs.existsSync(received) &&
          fs.readFileSync(received, "utf8").includes("redirect to session expiry"),
        3_000,
      );
      const forwarded = fs
        .readFileSync(received, "utf8")
        .split("\n")
        .filter((line) => line.trim())
        .map((line) => JSON.parse(line) as Record<string, unknown>);
      expect(
        forwarded.some((e) => e.type === "steer" && e.message === "redirect to session expiry"),
      ).toBe(true);
      await waitFor(() => {
        const status = manager.status(handle.id) as AgentRunRecord;
        return Boolean(status.pendingMessages?.steering.includes("redirect to session expiry"));
      }, 3_000);
      await manager.stop(handle.id);
    } finally {
      delete process.env.FAKE_PI_STEER_LOG;
    }
  });

  it("preserves a partial UTF-8 steering record across worker polls", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-steer-"));
    roots.push(root);
    fs.chmodSync(fakePiSteer, 0o755);
    const received = path.join(root, "received.jsonl");
    process.env.FAKE_PI_STEER_LOG = received;
    try {
      const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
        workerPath: path.resolve("src/worker.ts"),
        piBinary: fakePiSteer,
        runRoot: root,
      });
      managers.push(manager);
      const handle = await manager.spawn({ task: "STEER_ME", transport: "process" });
      await waitFor(() => manager.status(handle.id).status === "running");
      const steerFile = path.join(manager.runDirectory(handle.id)!, "steer.jsonl");
      const line = Buffer.from(`${JSON.stringify({ type: "steer", message: "转向界面 🚀" })}\n`);
      const split = line.indexOf(Buffer.from("界")) + 1;
      fs.appendFileSync(steerFile, line.subarray(0, split));
      await new Promise((resolve) => setTimeout(resolve, 300));
      fs.appendFileSync(steerFile, line.subarray(split));
      await waitFor(
        () => fs.existsSync(received) && fs.readFileSync(received, "utf8").includes("转向界面 🚀"),
        3_000,
      );
      await manager.stop(handle.id);
    } finally {
      delete process.env.FAKE_PI_STEER_LOG;
    }
  });

  it("forwards a follow_up and a queue mode to the child pi over RPC", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-steer-"));
    roots.push(root);
    fs.chmodSync(fakePiSteer, 0o755);
    const received = path.join(root, "received.jsonl");
    process.env.FAKE_PI_STEER_LOG = received;
    try {
      const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
        workerPath: path.resolve("src/worker.ts"),
        piBinary: fakePiSteer,
        runRoot: root,
        fullCodeMode: false,
      });
      managers.push(manager);
      const handle = await manager.spawn({ task: "STEER_ME", transport: "process" });
      await waitFor(() => manager.status(handle.id).status === "running");
      // The worker is running before its cold child process is RPC-ready.
      // Wait for that handshake separately; retain the forwarding deadline.
      await waitFor(
        () => fs.existsSync(received) && fs.readFileSync(received, "utf8").includes('"type":"prompt"'),
        10_000,
      );
      manager.setSteeringMode(handle.id, "all");
      manager.followUp(handle.id, "then run the tests");
      await waitFor(
        () => {
          if (!fs.existsSync(received)) return false;
          const text = fs.readFileSync(received, "utf8");
          return text.includes('"type":"set_steering_mode"') && text.includes("then run the tests");
        },
        3_000,
      );
      const forwarded = fs
        .readFileSync(received, "utf8")
        .split("\n")
        .filter((line) => line.trim())
        .map((line) => JSON.parse(line) as Record<string, unknown>);
      expect(forwarded.some((e) => e.type === "set_steering_mode" && e.mode === "all")).toBe(true);
      expect(
        forwarded.some((e) => e.type === "follow_up" && e.message === "then run the tests"),
      ).toBe(true);
      await manager.stop(handle.id);
    } finally {
      delete process.env.FAKE_PI_STEER_LOG;
    }
  }, 20_000);

  it("compact appends a compact entry to the steer channel for a running pi child", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-compact-"));
    roots.push(root);
    const manager = hangManager(root);
    const handle = await manager.spawn({ task: "HANG", transport: "process" });
    const result = manager.compact(handle.id, "Keep the file map");
    expect(result).toEqual({ queued: true, messageId: expect.any(String) });
    const entries = readSteerFile(manager.runDirectory(handle.id)!);
    expect(entries).toHaveLength(1);
    expect(entries[0]).toMatchObject({ type: "compact", instructions: "Keep the file map" });
    await manager.stop(handle.id);
  });

  it("compact appends a compact entry without instructions when omitted", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-compact-"));
    roots.push(root);
    const manager = hangManager(root);
    const handle = await manager.spawn({ task: "HANG", transport: "process" });
    manager.compact(handle.id);
    const entries = readSteerFile(manager.runDirectory(handle.id)!);
    expect(entries[0]).toMatchObject({ type: "compact" });
    expect(entries[0]).not.toHaveProperty("instructions");
    await manager.stop(handle.id);
  });

  it("compact throws for a finished agent", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-compact-"));
    roots.push(root);
    const manager = hangManager(root);
    const result = await manager.run({ task: "done", transport: "process" });
    expect(() => manager.compact(result.id)).toThrow(/already finished/);
  });

  it("compact rejects claude-runner children with a clear error", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-compact-"));
    roots.push(root);
    const fakeClaude = path.resolve("tests/fixtures/fake-claude.mjs");
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("src/worker.ts"),
      claudeBinary: fakeClaude,
      runRoot: root,
    });
    managers.push(manager);
    const handle = await manager.spawn({
      task: "Initial Claude task",
      runner: "claude",
      transport: "process",
      tools: ["read"],
    });
    expect(() => manager.compact(handle.id)).toThrow(/only supported for Pi-runner children/);
    await manager.stop(handle.id);
  });

  it("forwards a correlated compact frame only after child agent_settled", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-compact-"));
    roots.push(root);
    fs.chmodSync(fakePiSteer, 0o755);
    const received = path.join(root, "received.jsonl");
    process.env.FAKE_PI_STEER_LOG = received;
    try {
      const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
        workerPath: path.resolve("src/worker.ts"),
        piBinary: fakePiSteer,
        runRoot: root,
        fullCodeMode: false,
      });
      managers.push(manager);
      const handle = await manager.spawn({ task: "STEER_ME", transport: "process" });
      await waitFor(() => manager.status(handle.id).status === "running");
      manager.compact(handle.id, "Preserve the test plan");
      await waitFor(
        () => fs.existsSync(received) && fs.readFileSync(received, "utf8").includes("compact"),
        // Includes two cold Node processes and Pi settlement, not just RPC latency.
        10_000,
      );
      const forwarded = fs
        .readFileSync(received, "utf8")
        .split("\n")
        .filter((line) => line.trim())
        .map((line) => JSON.parse(line) as Record<string, unknown>);
      expect(
        forwarded.some(
          (e) =>
            e.type === "compact" &&
            typeof e.id === "string" &&
            e.customInstructions === "Preserve the test plan",
        ),
      ).toBe(true);
      const result = await manager.wait(handle.id);
      expect(result.status).toBe("completed");
      expect(result.compaction?.status).toBe("completed");
    } finally {
      delete process.env.FAKE_PI_STEER_LOG;
    }
  }, 20_000);
});
