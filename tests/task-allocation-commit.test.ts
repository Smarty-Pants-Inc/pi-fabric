import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { AgentManager, managerRunTestHooks } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import type { AgentRunRecord, AgentRunRequest, AgentRunResult } from "../src/agents/types.js";
import { createArchiveTaskControlDirectory, createHostTaskRun, readFinalAnswerReceipt, saveFinalAnswerReceipt, taskControlDirectory, taskSteerFile, withLegacyTaskControlDirectories } from "../src/worker/terminal-answer.js";
import { settleTerminalControl, trackTerminalControl } from "../src/worker/terminal-controls.js";

// Keep reason: admission/commit failure paths and archive data safety. These are
// recorded-call probes of the real manager and allocation producer. Central
// rename is copy-only emulated (the stage is retained), as in the Windows
// allocation probes. No process, real central rename, deletion or symlink is used.
const probe = vi.hoisted(() => ({
  calls: [] as string[],
  removals: [] as string[],
  allocations: new Map<string, { dev: bigint; ino: bigint }>(),
}));
vi.mock("../src/worker/terminal-answer.js", async importOriginal => {
  const actual = await importOriginal<typeof import("../src/worker/terminal-answer.js")>();
  return {
    ...actual,
    createHostTaskRun: (run: string) => {
      probe.calls.push(`allocate:${path.basename(run)}`);
      actual.createHostTaskRun(run);
      const stat = fs.lstatSync(run, { bigint: true });
      probe.allocations.set(run, { dev: stat.dev, ino: stat.ino });
    },
    initializeTaskControlDirectory: (run: string) => { actual.initializeTaskControlDirectory(run); probe.calls.push(`validate:${path.basename(run)}`); },
  };
});
vi.mock("../src/agents/archive-custody.js", async importOriginal => {
  const actual = await importOriginal<typeof import("../src/agents/archive-custody.js")>();
  return {
    ...actual,
    stageRunArchive: (run: string, archive: { kind: string; result: AgentRunResult }) => {
      expect(probe.allocations.has(run)).toBe(true);
      probe.calls.push(`stage-${archive.kind}:${archive.result.id}`);
      actual.stageRunArchive(run, archive as Parameters<typeof actual.stageRunArchive>[1]);
    },
    commitRunArchive: (run: string, kind: "settlement" | "shutdown") => { probe.calls.push(`archive-${kind}:${path.basename(run)}`); actual.commitRunArchive(run, kind); },
  };
});
vi.mock("../src/agents/model-route.js", () => ({
  prepareRouteDispatch: (_decision: unknown, _unused: unknown, run: string, id: string, options: { runPin: { use: (operation: () => void) => void } }) => {
    options.runPin.use(() => { probe.calls.push(`route:${id}`); });
    return {
      header: "probe-route",
      bindSession: () => undefined,
      outcome: () => options.runPin.use(() => { probe.calls.push(`route-outcome:${id}`); }),
    };
  },
}));

const managers: AgentManager[] = [];
const exits: (() => void)[] = [];
beforeEach(() => {
  probe.calls.length = 0;
  probe.removals.length = 0;
  probe.allocations.clear();
  const guardedRename = fs.renameSync;
  const state = path.resolve(process.env.XDG_STATE_HOME!);
  const temp = fs.realpathSync(os.tmpdir());
  expect(state === temp || state.startsWith(temp + path.sep)).toBe(false);
  vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
    const destination = path.join(fs.realpathSync(path.dirname(String(to))), path.basename(String(to)));
    if (!(destination === state || destination.startsWith(state + path.sep))) return guardedRename(from, to);
    // Never call the guarded central rename: record and copy bytes only; all
    // source directories/files remain. This is not a race/atomicity claim.
    if (fs.lstatSync(from).isDirectory()) {
      fs.mkdirSync(to, { mode: 0o700 });
      // A real rename preserves the stage's mode regardless of the caller's
      // umask; the copy-only stand-in must preserve it too.
      if (process.platform !== "win32") fs.chmodSync(to, fs.statSync(from).mode & 0o777);
      for (const name of fs.readdirSync(from)) {
        fs.writeFileSync(path.join(String(to), name), fs.readFileSync(path.join(String(from), name)), { mode: 0o600, flag: "wx" });
      }
    } else fs.writeFileSync(to, fs.readFileSync(from), { mode: 0o600 });
  });
  // Record destructive calls only. Never delegate them, even inside TMPDIR.
  vi.spyOn(fs, "rmSync").mockImplementation(file => { probe.removals.push(String(file)); });
  vi.spyOn(fs.promises, "rm").mockImplementation(async file => { probe.removals.push(String(file)); });
});
afterEach(async () => {
  delete managerRunTestHooks.afterRunAllocation;
  for (const exit of exits.splice(0)) exit();
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  vi.restoreAllMocks();
});

const fixture = (options: {
  prepare?: (model: string | undefined, requiredPin?: boolean) => Promise<string | void>;
  settled?: (result: AgentRunResult) => void;
  shutdown?: (results: AgentRunResult[]) => void;
  retainRuns?: boolean;
} = {}) => {
  const base = fs.mkdtempSync(path.join(os.tmpdir(), "allocation-commit-"));
  const cwd = path.join(base, "workspace"), root = path.join(base, "runs");
  fs.mkdirSync(cwd, { mode: 0o700 });
  fs.mkdirSync(root, { mode: 0o700 });
  vi.spyOn(ProcessTransport.prototype, "available").mockResolvedValue(true);
  const launch = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
    const run = path.join(root, request.id);
    expect(probe.allocations.has(run)).toBe(true);
    expect(fs.readFileSync(path.join(run, "task.txt"), "utf8")).not.toBe("");
    probe.calls.push(`launch:${request.id}`);
    const now = Date.now();
    const record: AgentRunRecord = { id: request.id, name: request.name, task: "probe", status: "running", runner: "pi", transport: "process",
      cwd, startedAt: now, updatedAt: now, turns: 0, toolCalls: 0, text: "", usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } };
    fs.writeFileSync(path.join(run, "status.json"), JSON.stringify(record), { mode: 0o600 });
    let alive = true;
    let exit!: () => void;
    const closed = new Promise<void>(resolve => { exit = () => { alive = false; resolve(); }; });
    exits.push(exit);
    return { kind: "process", closed, isAlive: async () => alive, stop: async () => exit(), waitForClose: () => closed };
  });
  const manager = new AgentManager(cwd, { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 1, budgetUsd: 0, retainRuns: options.retainRuns ?? true,
    deniedModels: ["test/denied"] }, {
    runRoot: root, fullCodeMode: false, preparePiModel: options.prepare ?? (async model => model),
    ...(options.settled ? { onSettled: options.settled } : {}), ...(options.shutdown ? { onStoppedAtClose: options.shutdown } : {}),
  });
  managers.push(manager);
  return { manager, root, launch };
};
const allocations = () => probe.calls.filter(call => call.startsWith("allocate:"));
const routed = (): NonNullable<AgentRunRequest["routeDecision"]> => {
  const pin = { model: "test/allowed", effort: "low" as const };
  return { ...pin, decisionId: "allocation-probe", mode: "shadow", routeClass: "bounded-lookup", parentSessionId: "parent", pin,
    candidates: [pin], shadowChoice: pin, confidence: null, probability: null, reasonCode: "shadow-choice", latencyMs: 0 };
};

// State must be supplied outside the private test temp. The manager cwd is an
// explicit temp workspace so a worktree-local XDG_STATE_HOME cannot overlap it.
describe("task allocation follows preparation and commit, except routed receipts", () => {
  it.skipIf(process.platform === "win32")("publishes a fresh central root and stage privately under umask 0177", async () => {
    const state = process.env.XDG_STATE_HOME!;
    const { manager, root } = fixture();
    const mask = process.umask(0o177);
    process.env.XDG_STATE_HOME = path.join(state, path.basename(path.dirname(root)));
    try {
      const handle = await manager.spawn({ task: "restrictive mask", model: "test/allowed" });
      const run = path.join(root, handle.id), control = taskControlDirectory(run);
      expect(fs.statSync(run).mode & 0o777).toBe(0o700);
      expect(fs.statSync(control).mode & 0o777).toBe(0o700);
      expect(fs.statSync(path.dirname(control)).mode & 0o777).toBe(0o700);
      expect(fs.readFileSync(path.join(run, "task.txt"), "utf8")).toBe("restrictive mask");
    } finally { process.umask(mask); process.env.XDG_STATE_HOME = state; }
  });

  it("uses central control paths, current transport metadata and archive-specific bindings", async () => {
    const { manager, root } = fixture();
    const handle = await manager.spawn({ task: "central contract", model: "test/allowed" });
    const run = path.join(root, handle.id), controls = taskControlDirectory(run);
    const id = "11111111-1111-4111-8111-111111111111";
    trackTerminalControl(run, { id, delivery: "steer" });
    expect(settleTerminalControl(run, id, "delivered")).toEqual({ id, delivery: "steer", state: "delivered", forwarded: false });
    // Recorded-only removal leaves the prior finality lock in place. Use a
    // separate genuine allocation rather than emulating lock deletion/reuse.
    const answerRun = path.join(root, "answer"); createHostTaskRun(answerRun);
    const answer = saveFinalAnswerReceipt(answerRun, "answer", "complete answer");
    expect(readFinalAnswerReceipt(answerRun, "answer")).toEqual(answer);
    expect(taskSteerFile(run)).toBe(path.join(controls, "steer.jsonl"));
    expect(fs.existsSync(path.join(answerRun, "final-answer.json"))).toBe(false);
    expect(fs.existsSync(path.join(run, "terminal-controls"))).toBe(false);
    expect(withLegacyTaskControlDirectories(run, sources => sources.length)).toBe(0);
    const archive = path.join(root, "archive"); fs.mkdirSync(archive, { mode: 0o700 });
    createArchiveTaskControlDirectory(archive);
    const stat = fs.lstatSync(archive, { bigint: true });
    const binding = JSON.parse(fs.readFileSync(path.join(taskControlDirectory(archive), ".legacy-read.json"), "utf8"));
    expect(binding).toEqual({ version: 2, runId: "archive", dev: String(stat.dev), ino: String(stat.ino), snapshot: null });
    expect(binding).not.toEqual(JSON.parse(fs.readFileSync(path.join(controls, ".legacy-read.json"), "utf8")));
  });

  it("does not allocate when the resolved model is denied", async () => {
    const { manager, root, launch } = fixture({ prepare: async () => "test/denied" });
    await expect(manager.spawn({ task: "denied", model: "test/allowed" })).rejects.toMatchObject({ code: "FABRIC_MODEL_DENIED" });
    expect(allocations()).toEqual([]);
    expect(fs.readdirSync(root)).toEqual([]);
    expect(launch).not.toHaveBeenCalled();
    expect(manager.runningCount()).toBe(0);
  });

  it("does not allocate on preparation timeout or a late resolution", async () => {
    let resume!: (model: string) => void;
    const gate = new Promise<string>(resolve => { resume = resolve; });
    const { manager, root, launch } = fixture({ prepare: () => gate });
    await expect(manager.spawn({ task: "timeout", model: "test/allowed" }, undefined, undefined, undefined, undefined, undefined,
      { timeoutMs: 10 })).rejects.toMatchObject({ code: "FABRIC_AGENT_LAUNCH_PREPARATION_TIMEOUT", launchOutcome: "unlaunched" });
    resume("test/allowed");
    await gate;
    await Promise.resolve();
    expect(allocations()).toEqual([]);
    expect(fs.readdirSync(root)).toEqual([]);
    expect(launch).not.toHaveBeenCalled();
    expect(manager.runningCount()).toBe(0);
  });

  it("does not allocate when activation authority is revoked after preparation", async () => {
    let authorized = true;
    const { manager, root, launch } = fixture({ prepare: async model => { authorized = false; return model; } });
    const commit = vi.fn();
    await expect(manager.spawn({ task: "revoked", model: "test/allowed" }, undefined, () => authorized, commit))
      .rejects.toThrow("Agent activation no longer authorized");
    expect(allocations()).toEqual([]);
    expect(fs.readdirSync(root)).toEqual([]);
    expect(commit).not.toHaveBeenCalled();
    expect(launch).not.toHaveBeenCalled();
    expect(manager.runningCount()).toBe(0);
  });

  it("does not allocate on a resident commit veto and releases capacity", async () => {
    const { manager, root, launch } = fixture();
    await expect(manager.spawn({ task: "veto", model: "test/allowed" }, undefined, undefined, () => {
      expect(allocations()).toEqual([]);
      throw new Error("resident commit veto");
    })).rejects.toThrow("resident commit veto");
    expect(fs.readdirSync(root)).toEqual([]);
    expect(launch).not.toHaveBeenCalled();
    const handle = await manager.spawn({ task: "next", model: "test/allowed" });
    expect(handle.status).toBe("running");
    expect(allocations()).toEqual([`allocate:${handle.id}`]);
  });

  it("allocates exactly once after commit and before task/control/transport use", async () => {
    const { manager, root } = fixture({ prepare: async model => { probe.calls.push("prepare"); expect(allocations()).toEqual([]); return model; } });
    managerRunTestHooks.afterRunAllocation = run => { expect(probe.allocations.has(run)).toBe(true); expect(fs.readdirSync(run)).toEqual([]); };
    const handle = await manager.spawn({ task: "normal", model: "test/allowed" }, undefined, undefined, () => { probe.calls.push("commit"); expect(allocations()).toEqual([]); });
    expect(probe.calls.slice(0, 5)).toEqual(["prepare", "commit", `allocate:${handle.id}`, `validate:${handle.id}`, `launch:${handle.id}`]);
    expect(fs.readdirSync(root)).toEqual([handle.id]);
    const binding = JSON.parse(fs.readFileSync(path.join(taskControlDirectory(path.join(root, handle.id)), ".legacy-read.json"), "utf8"));
    const stat = fs.lstatSync(path.join(root, handle.id), { bigint: true });
    expect(binding).toEqual({ version: 2, runId: handle.id, dev: String(stat.dev), ino: String(stat.ino), snapshot: null });
    expect(allocations()).toHaveLength(1);
  });

  it.each(["denied", "veto", "cancelled", "timeout"] as const)("keeps an unlaunched queued %s receipt in memory with no allocation or status write", async reason => {
    const { manager, root, launch } = fixture({ prepare: async model => {
      if (model === "test/queued" && reason === "timeout") return new Promise<string>(() => {});
      return model === "test/queued" && reason === "denied" ? "test/denied" : model;
    } });
    const first = await manager.spawn({ task: "occupy", model: "test/allowed" });
    const queued = await manager.spawn({ task: "queued", model: "test/queued" }, undefined, undefined,
      reason === "veto" ? () => { throw new Error("queued commit veto"); } : undefined,
      undefined, undefined, { timeoutMs: 10 });
    expect(queued.status).toBe("queued");
    expect(fs.existsSync(path.join(root, queued.id))).toBe(false);
    if (reason === "cancelled") await manager.stop(queued.id);
    else await manager.stop(first.id);
    const result = await manager.wait(queued.id, { timeoutMs: 1000 });
    expect(result.status).toBe(reason === "cancelled" ? "stopped" : "failed");
    if (reason === "timeout") expect(result.launchPreparationTimeoutMs).toBe(10);
    expect(result.warnings).toBeUndefined();
    expect(allocations()).toEqual([`allocate:${first.id}`]);
    expect(fs.existsSync(path.join(root, queued.id))).toBe(false);
    expect(probe.calls.some(call => call.includes(queued.id))).toBe(false);
    expect(launch).toHaveBeenCalledTimes(1);
  });

  it("cleans an unallocated cancelled receipt without asking missing controls for authority", async () => {
    const { manager, root } = fixture({ retainRuns: false });
    await manager.spawn({ task: "occupy", model: "test/allowed" });
    const queued = await manager.spawn({ task: "cancel before launch", model: "test/allowed" });
    await manager.stop(queued.id);
    expect(fs.existsSync(path.join(root, queued.id))).toBe(false);
    probe.removals.length = 0;
    await expect(manager.cleanup(queued.id)).resolves.toEqual({ cleaned: true });
    expect(probe.removals).toEqual([]);
  });

  it("refuses cleanup of planted unknown bytes after an unallocated cancellation", async () => {
    const { manager, root } = fixture({ retainRuns: false });
    await manager.spawn({ task: "occupy", model: "test/allowed" });
    const queued = await manager.spawn({ task: "cancel before launch", model: "test/allowed" });
    await manager.stop(queued.id);
    const run = path.join(root, queued.id);
    fs.mkdirSync(run, { mode: 0o700 });
    fs.writeFileSync(path.join(run, "preserve"), "unknown bytes");
    probe.removals.length = 0;
    await expect(manager.cleanup(queued.id)).rejects.toThrow();
    expect(probe.removals).toEqual([]);
    expect(fs.readFileSync(path.join(run, "preserve"), "utf8")).toBe("unknown bytes");
  });

  it("strictly allocates an unlaunched queued archive source and retains the full result when its sink fails", async () => {
    let queuedId = "";
    const settled = vi.fn((result: AgentRunResult) => { if (result.id === queuedId) throw new Error("archive sink unavailable"); });
    const { manager, root, launch } = fixture({ settled, prepare: async model => model === "test/queued" ? "test/denied" : model });
    const first = await manager.spawn({ task: "occupy", model: "test/allowed" });
    const queued = await manager.spawn({ task: "queued full source", model: "test/queued" });
    queuedId = queued.id;
    expect(fs.existsSync(path.join(root, queued.id))).toBe(false);
    await manager.stop(first.id);
    const result = await manager.wait(queued.id, { timeoutMs: 1000 });
    expect(result).toMatchObject({ status: "failed", task: "queued full source", turns: 0 });
    expect(result.warnings?.join(" ")).toContain("archive sink unavailable");
    const directory = path.join(root, queued.id);
    expect(JSON.parse(fs.readFileSync(path.join(directory, "queued-result.json"), "utf8"))).toMatchObject({ id: queued.id, task: result.task, status: "failed" });
    expect(fs.existsSync(path.join(directory, "status.json"))).toBe(false);
    const custody = JSON.parse(fs.readFileSync(path.join(directory, "archive-pending.json"), "utf8"));
    expect(custody.pending.settlement.result).toMatchObject({ id: queued.id, task: result.task, status: "failed" });
    expect(allocations().filter(call => call.endsWith(queued.id))).toHaveLength(1);
    expect(probe.calls.indexOf(`allocate:${queued.id}`)).toBeLessThan(probe.calls.indexOf(`stage-settlement:${queued.id}`));
    expect(launch).toHaveBeenCalledTimes(1);
  });

  it("keeps strict central authority before a successful queued archive and preserves recorded-only custody", async () => {
    let queuedId = "", root = "";
    const settled = vi.fn((result: AgentRunResult) => {
      if (result.id !== queuedId) return;
      const directory = path.join(root, queuedId);
      const binding = JSON.parse(fs.readFileSync(path.join(taskControlDirectory(directory), ".legacy-read.json"), "utf8"));
      expect(binding).toMatchObject({ version: 2, runId: queuedId, snapshot: null });
      expect(JSON.parse(fs.readFileSync(path.join(directory, "queued-result.json"), "utf8"))).toMatchObject({ id: queuedId, status: "failed" });
      expect(JSON.parse(fs.readFileSync(path.join(directory, "archive-pending.json"), "utf8")).pending.settlement.result.id).toBe(queuedId);
      expect(fs.existsSync(path.join(directory, "status.json"))).toBe(false);
    });
    const f = fixture({ settled, prepare: async model => model === "test/queued" ? "test/denied" : model });
    root = f.root;
    const first = await f.manager.spawn({ task: "occupy", model: "test/allowed" });
    const queued = await f.manager.spawn({ task: "archived queued failure", model: "test/queued" });
    queuedId = queued.id;
    await f.manager.stop(first.id);
    const result = await f.manager.wait(queued.id, { timeoutMs: 1000 });
    expect(result.status).toBe("failed");
    expect(result.warnings).toBeUndefined();
    expect(settled.mock.calls.filter(([value]) => value.id === queuedId)).toHaveLength(1);
    expect(probe.calls).toContain(`archive-settlement:${queuedId}`);
    expect(probe.removals).toContain(path.join(root, queuedId, "archive-pending.json"));
    // A recorded-only removal leaves the real custody marker intact. The exit
    // veto correctly withholds collectible status; do not emulate deletion.
    expect(fs.existsSync(path.join(root, queuedId, "status.json"))).toBe(false);
    expect(fs.existsSync(path.join(root, queuedId, "archive-pending.json"))).toBe(true);
    expect(allocations().filter(call => call.endsWith(queuedId))).toHaveLength(1);
    expect(f.launch).toHaveBeenCalledTimes(1);
  });

  it("allocates a successful queued request once after its commit", async () => {
    const { manager, root, launch } = fixture();
    const first = await manager.spawn({ task: "occupy", model: "test/allowed" });
    let launched!: (id: string) => void;
    const committed = new Promise<string>(resolve => { launched = resolve; });
    const queued = await manager.spawn({ task: "queued success", model: "test/queued" }, undefined, undefined, id => {
      expect(fs.existsSync(path.join(root, id))).toBe(false);
      probe.calls.push(`commit:${id}`);
    }, undefined, handle => launched(handle.id));
    expect(queued.status).toBe("queued");
    expect(fs.existsSync(path.join(root, queued.id))).toBe(false);
    await manager.stop(first.id);
    const id = await committed;
    expect(allocations().filter(call => call.endsWith(id))).toHaveLength(1);
    expect(probe.calls.indexOf(`commit:${id}`)).toBeLessThan(probe.calls.indexOf(`allocate:${id}`));
    expect(probe.calls.indexOf(`allocate:${id}`)).toBeLessThan(probe.calls.indexOf(`launch:${id}`));
    expect(launch).toHaveBeenCalledTimes(2);
  });

  it("does not adopt a pre-existing queued directory for archive custody", async () => {
    const { manager, root, launch } = fixture({ settled: () => {}, prepare: async model => model === "test/queued" ? "test/denied" : model });
    const first = await manager.spawn({ task: "occupy", model: "test/allowed" });
    const queued = await manager.spawn({ task: "queued failure", model: "test/queued" });
    const directory = path.join(root, queued.id);
    fs.mkdirSync(directory, { mode: 0o700 });
    fs.writeFileSync(path.join(directory, "marker"), "preserve", { mode: 0o600 });
    await manager.stop(first.id);
    const result = await manager.wait(queued.id, { timeoutMs: 1000 });
    expect(result.status).toBe("failed");
    expect(result.warnings?.join(" ")).toContain("already exists");
    expect(fs.readdirSync(directory)).toEqual(["marker"]);
    expect(fs.readFileSync(path.join(directory, "marker"), "utf8")).toBe("preserve");
    expect(probe.allocations.has(directory)).toBe(false);
    expect(probe.calls.some(call => call === `stage-settlement:${queued.id}`)).toBe(false);
    expect(launch).toHaveBeenCalledTimes(1);
  });

  it("strictly allocates a shutdown-only queued archive before staging it", async () => {
    const shutdown = vi.fn();
    const { manager, root } = fixture({ shutdown });
    await manager.spawn({ task: "occupy", model: "test/allowed" });
    const queued = await manager.spawn({ task: "shutdown queued", model: "test/queued" });
    expect(fs.existsSync(path.join(root, queued.id))).toBe(false);
    await manager.close();
    expect(shutdown).toHaveBeenCalledWith(expect.arrayContaining([expect.objectContaining({ id: queued.id, status: "stopped" })]));
    expect(allocations().filter(call => call.endsWith(queued.id))).toHaveLength(1);
    expect(probe.calls.indexOf(`allocate:${queued.id}`)).toBeLessThan(probe.calls.indexOf(`stage-shutdown:${queued.id}`));
  });

  // #1150 intentionally refuses routed Windows publication; #12444 owns its
  // replacement. These two fd-identity counterexamples exercise the POSIX pin.
  it.skipIf(process.platform === "win32")("keeps routed allocation and identity pinning before dispatch and preparation", async () => {
    const { manager } = fixture({ prepare: async model => { probe.calls.push("prepare"); throw new Error("routed preparation failed"); } });
    await expect(manager.spawn({ task: "routed", routeDecision: routed() })).rejects.toThrow("routed preparation failed");
    const allocation = allocations()[0]!;
    const id = allocation.slice("allocate:".length);
    expect(probe.calls.slice(0, 3)).toEqual([allocation, `route:${id}`, "prepare"]);
    expect(allocations()).toHaveLength(1);
    expect(probe.calls).toContain(`route-outcome:${id}`);
  });

  it.skipIf(process.platform === "win32")("still refuses a real routed replacement after preparation without launching or saving an outcome", async () => {
    let run = "";
    managerRunTestHooks.afterRunAllocation = directory => { run = directory; };
    const { manager, launch } = fixture({ prepare: async () => {
      fs.renameSync(run, `${run}.moved`);
      fs.mkdirSync(run, { mode: 0o700 });
      return "test/allowed";
    } });
    await expect(manager.spawn({ task: "routed replacement", routeDecision: routed() })).rejects.toMatchObject({ code: "FABRIC_RUN_IDENTITY_REFUSED" });
    expect(launch).not.toHaveBeenCalled();
    expect(fs.readdirSync(run)).toEqual([]);
    expect(probe.calls.some(call => call.startsWith("route-outcome:"))).toBe(false);
  });
});
