import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { pathToFileURL } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { DEFAULT_FABRIC_CONFIG, normalizeFabricConfig } from "../src/config.js";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { FabricRuntimeState } from "../src/fabric-runtime-state.js";
import { restoreStoppedRuns, STOPPED_AGENTS_ENTRY } from "../src/agents/stopped-runs.js";
import { canRemoveTerminalRun, canRemoveManagedRunRoot, FABRIC_RUN_ROOT_PREFIX, runTreeExitVeto, sweepTempRunRoots } from "../src/storage/retention.js";
import type { AgentRunRecord, AgentRunResult, AgentTransportLaunch } from "../src/agents/types.js";

const roots: string[] = [];
const managers: AgentManager[] = [];
afterEach(async () => {
  vi.restoreAllMocks();
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const fixture = (options: { timeoutMs?: number; lost?: boolean; dead?: boolean; retainRuns?: boolean; onStoppedAtClose?: (runs: AgentRunResult[]) => void } = {}) => {
  const temp = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-terminal-durability-")); roots.push(temp);
  const root = path.join(temp, FABRIC_RUN_ROOT_PREFIX + "fixture");
  let alive = !options.dead;
  const stop = vi.fn(async () => { alive = false; });
  const launch = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async (request: AgentTransportLaunch) => {
    const args = request.workerArguments;
    const statusFile = args[args.indexOf("--status-file") + 1]!;
    const record: AgentRunRecord = { id: request.id, name: request.name, task: "fixture", status: "running", runner: "pi", transport: options.lost ? "herdr" : "process", cwd: request.cwd, startedAt: Date.now(), updatedAt: Date.now(), turns: 0, toolCalls: 0, text: "", usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 }, logFile: path.join(path.dirname(statusFile), "events.jsonl") };
    fs.writeFileSync(statusFile, JSON.stringify(record));
    return { kind: options.lost ? "herdr" : "process", relaunchable: false, livenessPollIntervalMs: 1, isAlive: async () => options.lost ? false : alive, stop, ...(options.lost ? { lostContact: () => "Herdr contact lost; pane may still run" } : {}) };
  });
  const consumed = vi.fn();
  const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: options.timeoutMs ?? 60_000, retainRuns: options.retainRuns ?? true, maxConcurrent: 1 }, { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: root, onResultConsumed: consumed, ...(options.onStoppedAtClose ? { onStoppedAtClose: options.onStoppedAtClose } : {}) });
  managers.push(manager);
  return { temp, root, manager, launch, stop, consumed, exit: () => { alive = false; } };
};
const faults = (reject: (file: string, fd: number) => boolean) => {
  const descriptors = new Map<number, string>();
  const open = fs.openSync.bind(fs), sync = fs.fsyncSync.bind(fs);
  vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => { const fd = open(file, flags, mode); descriptors.set(fd, String(file)); return fd; });
  return vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
    if (reject(descriptors.get(fd) ?? "", fd)) throw new Error("injected publication barrier failure");
    sync(fd);
  });
};
const status = (manager: AgentManager, id: string) => path.join(manager.runDirectory(id)!, "status.json");

describe("Astra F15-F22 terminal publication obligations", () => {
  it.skipIf(process.platform === "win32")("F18 replacement runtime restores and confirms the original answer through session entries", async () => {
    const { temp, manager: unused, exit, launch } = fixture({ retainRuns: false });
    await unused.close();
    const entries: unknown[] = [];
    const createRuntime = () => {
      const sendMessage = vi.fn();
      const pi = { events: { emit: vi.fn() }, getThinkingLevel: () => "off", sendMessage, on: vi.fn(),
        appendEntry: (customType: string, data: unknown) => entries.push({ type: "custom", customType, data }),
      } as unknown as ExtensionAPI;
      const context = {
        cwd: temp, hasUI: true, isProjectTrusted: () => true, isIdle: () => true, hasPendingMessages: () => false,
        model: { provider: "dest", id: "old", contextWindow: 48_000 },
        modelRegistry: { getAvailable: () => [{ provider: "dest", id: "old", contextWindow: 48_000 }], find: () => ({ provider: "dest", id: "old", contextWindow: 48_000 }), getApiKeyAndHeaders: async () => ({ ok: true, apiKey: "fixture" }), refresh: async () => ({}) },
        sessionManager: { getSessionId: () => "terminal-replacement", getSessionFile: () => undefined, getBranch: () => [], getEntries: () => entries, getLeafId: () => null },
        ui: { setStatus: vi.fn(), notify: vi.fn() },
      } as unknown as ExtensionContext;
      const runtime = new FabricRuntimeState(pi, new CapturedToolCatalog(), { paths: {
        extension: path.resolve("dist/index.js"), worker: path.resolve("tests/fixtures/fake-worker.mjs"), residentHost: path.join(temp, "unused.mjs"), skills: temp,
      } });
      const config = normalizeFabricConfig({ fullCodeMode: false, agents: { enabled: true, budgetUsd: 0, retainRuns: false, notifyOnComplete: true, sessionExport: false }, mcp: { enabled: false }, memory: { enabled: false }, residency: { enabled: false }, mesh: { enabled: true, root: path.join(temp, "mesh") }, prewalk: { enabled: false, alwaysRearm: false } });
      return { runtime, sendMessage, init: () => runtime.initialize(context, config) };
    };
    vi.stubEnv("PI_FABRIC_PROJECT_ROOT", temp);
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(temp, "agent"));
    for (const name of ["PI_FABRIC_MESH_ROOT", "PI_FABRIC_ACTOR_ID", "PI_FABRIC_PARENT_RUN", "PI_FABRIC_SESSION_ID", "PI_FABRIC_MAIN_AGENT_ID"]) vi.stubEnv(name, undefined);
    let first: ReturnType<typeof createRuntime> | undefined = createRuntime();
    let second: ReturnType<typeof createRuntime> | undefined;
    let third: ReturnType<typeof createRuntime> | undefined;
    let fault: ReturnType<typeof faults> | undefined;
    try {
      await first.init();
      const handle = await first.runtime.agents.spawn({ task: "fixture", transport: "process", model: "dest/old" });
      const file = status(first.runtime.agents, handle.id), directory = path.dirname(file);
      const answer = { ...JSON.parse(fs.readFileSync(file, "utf8")), status: "completed", finishedAt: Date.now(), text: "original completed answer after replacement", turns: 7 };
      fs.writeFileSync(file, JSON.stringify(answer));
      exit();
      let unavailable = true, confirmations = 0;
      fault = faults((target, fd) => target === directory && fs.fstatSync(fd).isDirectory() && (++confirmations, unavailable));
      await first.runtime.shutdown();
      first = undefined; // The old manager/runtime is no longer accessible to recovery.
      expect(confirmations).toBeGreaterThan(0);
      expect(fs.existsSync(file)).toBe(true);
      second = createRuntime();
      await second.init();
      await expect(second.runtime.agents.wait(handle.id, { timeoutMs: 350 })).rejects.toThrow(/publication barrier/);
      expect(() => second!.runtime.agents.status(handle.id)).toThrow(/publication barrier/);
      expect(entries).toContainEqual(expect.objectContaining({ customType: STOPPED_AGENTS_ENTRY, data: expect.objectContaining({ stopped: expect.arrayContaining([expect.objectContaining({ id: handle.id, text: answer.text })]) }) }));
      expect(second.sendMessage).not.toHaveBeenCalled();
      unavailable = false;
      await vi.waitFor(() => expect(second!.sendMessage).toHaveBeenCalledOnce(), { timeout: 4000 });
      expect(second.runtime.agents.status(handle.id)).toMatchObject({ status: "completed", text: answer.text, turns: 7 });
      expect(await second.runtime.agents.wait(handle.id)).toMatchObject({ status: "completed", text: answer.text, turns: 7 });
      expect(launch).toHaveBeenCalledOnce();
      await second.runtime.shutdown();
      second = undefined;
      third = createRuntime();
      await third.init();
      expect(await third.runtime.agents.wait(handle.id)).toMatchObject({ status: "completed", text: answer.text, turns: 7 });
      expect(third.sendMessage).not.toHaveBeenCalled();
      expect(launch).toHaveBeenCalledOnce();
    } finally {
      fault?.mockRestore();
      await first?.runtime.shutdown();
      await second?.runtime.shutdown();
      await third?.runtime.shutdown();
      vi.unstubAllEnvs();
    }
  }, 30_000);

  it.skipIf(process.platform === "win32")("F18 close retains a completed answer with default retention until terminal confirmation recovers", async () => {
    const { manager, exit, consumed } = fixture({ retainRuns: false });
    const handle = await manager.spawn({ task: "fixture", transport: "process" });
    const file = status(manager, handle.id), directory = path.dirname(file);
    const answer = { ...JSON.parse(fs.readFileSync(file, "utf8")), status: "completed", finishedAt: Date.now(), text: "original completed answer", turns: 7 };
    fs.writeFileSync(file, JSON.stringify(answer));
    exit();
    let unavailable = true, confirmations = 0;
    const fault = faults((target, fd) => target === directory && fs.fstatSync(fd).isDirectory() && (++confirmations, unavailable));
    try {
      await manager.close();
      expect(confirmations).toBeGreaterThan(0);
      expect(consumed).not.toHaveBeenCalled();
      expect(fs.existsSync(file)).toBe(true);
      expect(fs.existsSync(path.join(directory, "task.txt"))).toBe(true);
      await expect(manager.cleanup(handle.id)).rejects.toThrow();
      await expect(manager.wait(handle.id, { timeoutMs: 350 })).rejects.toThrow();
      unavailable = false;
      expect(await manager.wait(handle.id, { timeoutMs: 3000 })).toMatchObject({ status: "completed", text: answer.text, turns: 7 });
      expect(consumed).toHaveBeenCalled();
    } finally { fault.mockRestore(); }
  });

  it("F18 replacement manager republishes a close-time pre-rename obligation with notices off", async () => {
    const entries: unknown[] = [];
    const entry = (data: unknown) => ({ type: "custom", customType: STOPPED_AGENTS_ENTRY, data });
    const { manager, launch } = fixture({ retainRuns: false, onStoppedAtClose: runs => { entries.push(entry({ stopped: runs })); } });
    const handle = await manager.spawn({ task: "fixture", transport: "process" });
    const file = status(manager, handle.id);
    let unavailable = true;
    const fault = faults(target => target.startsWith(file + ".") && target.endsWith(".tmp") && unavailable);
    try {
      await manager.close();
      expect(JSON.parse(fs.readFileSync(file, "utf8")).status).toBe("running");
      const { manager: next, consumed } = fixture();
      const enqueue = vi.fn();
      let sessionUnavailable = false;
      restoreStoppedRuns({ entries, notifyOnComplete: false,
        restore: (runs, confirmed) => next.restorePreviousRuns(runs, confirmed), enqueue,
        appendEntry: data => { if (sessionUnavailable) throw new Error("confirmation handoff unavailable"); entries.push(entry(data)); },
      });
      await expect(next.stop(handle.id)).rejects.toThrow("publication barrier");
      expect(consumed).not.toHaveBeenCalled();
      expect(enqueue).not.toHaveBeenCalled();
      unavailable = false;
      sessionUnavailable = true;
      await expect(next.wait(handle.id)).rejects.toThrow("confirmation handoff unavailable");
      expect(consumed).not.toHaveBeenCalled();
      sessionUnavailable = false;
      expect(await next.wait(handle.id)).toMatchObject({ status: "stopped" });
      expect(JSON.parse(fs.readFileSync(file, "utf8")).status).toBe("stopped");
      expect(enqueue).not.toHaveBeenCalled();
      expect(launch).toHaveBeenCalledOnce();
      expect(entries.at(-1)).toMatchObject({ data: { stopped: [expect.not.objectContaining({ terminalPending: expect.anything() })] } });
    } finally { fault.mockRestore(); }
  });

  it("F18 refuses close retirement if a pending terminal handoff cannot be saved", async () => {
    const { manager } = fixture({ onStoppedAtClose: () => { throw new Error("session handoff unavailable"); } });
    const handle = await manager.spawn({ task: "fixture", transport: "process" });
    const file = status(manager, handle.id);
    const fault = faults(target => target.startsWith(file + ".") && target.endsWith(".tmp"));
    try {
      await expect(manager.close()).rejects.toThrow("session handoff unavailable");
      expect(fs.existsSync(file)).toBe(true);
      expect(fs.existsSync(path.join(path.dirname(file), "task.txt"))).toBe(true);
    } finally {
      fault.mockRestore();
      await manager.wait(handle.id, { timeoutMs: 3000 });
      managers.splice(managers.indexOf(manager), 1); // Its intentionally rejected close is already joined.
    }
  });

  it("F19 caller abort before progress owns a one-shot terminal fsync rejection and retries settlement", async () => {
    const { manager, launch, stop } = fixture();
    const abort = new AbortController();
    const handle = await manager.spawn({ task: "fixture", transport: "process" }, abort.signal);
    const file = status(manager, handle.id);
    let attempts = 0;
    const fault = faults(target => target.startsWith(file + ".") && target.endsWith(".tmp") && attempts++ === 0);
    const unhandled: unknown[] = [];
    const rejection = (error: unknown) => { unhandled.push(error); };
    process.on("unhandledRejection", rejection);
    try {
      abort.abort();
      expect((await manager.wait(handle.id, { timeoutMs: 4000 })).status).toBe("stopped");
      expect(attempts).toBeGreaterThanOrEqual(2);
      expect(unhandled).toEqual([]);
      expect(stop).toHaveBeenCalledOnce();
      expect(launch).toHaveBeenCalledOnce();
      const next = await manager.spawn({ task: "next admitted", transport: "process" });
      expect(launch).toHaveBeenCalledTimes(2);
      await manager.stop(next.id);
    } finally { fault.mockRestore(); process.off("unhandledRejection", rejection); }
  });

  it.skipIf(process.platform === "win32")("F15 stop retries visible post-rename bytes without settling while directory confirmation still fails", async () => {
    const { manager, stop, consumed } = fixture();
    const handle = await manager.spawn({ task: "fixture", transport: "process" });
    const file = status(manager, handle.id);
    let unavailable = true, barriers = 0;
    const fault = faults((target, fd) => target === path.dirname(file) && fs.fstatSync(fd).isDirectory() && (++barriers, unavailable));
    try {
      await expect(manager.stop(handle.id)).rejects.toThrow("injected publication");
      expect(JSON.parse(fs.readFileSync(file, "utf8")).status).toBe("stopped");
      const beforeRetry = barriers;
      await expect(manager.stop(handle.id)).rejects.toThrow("injected publication");
      expect(barriers).toBeGreaterThan(beforeRetry);
      await expect(manager.wait(handle.id, { timeoutMs: 350 })).rejects.toThrow();
      expect(consumed).not.toHaveBeenCalled();
      unavailable = false;
      expect((await manager.wait(handle.id, { timeoutMs: 3000 })).status).toBe("stopped");
      expect(stop).toHaveBeenCalledOnce();
    } finally { fault.mockRestore(); }
  });

  it.skipIf(process.platform === "win32")("F15 monitor cannot consume a worker paused between rename and directory confirmation", async () => {
    const { manager, consumed } = fixture();
    const handle = await manager.spawn({ task: "fixture", transport: "process" });
    const file = status(manager, handle.id), gate = path.join(path.dirname(file), "writer-resume");
    const module = pathToFileURL(path.resolve("src/worker/run-record.ts")).href;
    const script = `import fs from 'node:fs'; import {writeRunRecord} from ${JSON.stringify(module)};
      const file = ${JSON.stringify(file)}, gate = ${JSON.stringify(gate)};
      const record = {...JSON.parse(fs.readFileSync(file,'utf8')),status:'completed',finishedAt:Date.now(),text:'durable answer'};
      const sync = fs.fsyncSync; let paused = false;
      fs.fsyncSync = fd => { if (!paused && fs.fstatSync(fd).isDirectory()) { paused = true; fs.writeSync(1,'renamed\\n'); while (!fs.existsSync(gate)) Atomics.wait(new Int32Array(new SharedArrayBuffer(4)),0,0,10); } sync(fd); };
      writeRunRecord(file,record);`;
    let unavailable = true;
    const fault = faults((target, fd) => target === path.dirname(file) && fs.fstatSync(fd).isDirectory() && unavailable);
    const writer = spawn(process.execPath, ["--input-type=module", "--eval", script], { stdio: ["ignore", "pipe", "pipe"] });
    const exited = once(writer, "exit");
    let stderr = ""; writer.stderr.on("data", chunk => { stderr += String(chunk); });
    try {
      await vi.waitFor(() => expect(JSON.parse(fs.readFileSync(file, "utf8")).status).toBe("completed"));
      await expect(manager.wait(handle.id, { timeoutMs: 350 })).rejects.toThrow();
      expect(consumed).not.toHaveBeenCalled();
      expect(writer.exitCode).toBeNull();
      unavailable = false;
      // Reader may discharge the owed barrier itself, even while the writer is paused.
      expect((await manager.wait(handle.id, { timeoutMs: 3000 })).text).toBe("durable answer");
    } finally {
      fault.mockRestore(); fs.writeFileSync(gate, "resume");
      expect(await exited, stderr).toEqual([0, null]);
    }
  });

  it.each(["deadline", "transport-death"] as const)("F16 %s monitor retries one-shot pre-rename fsync failure, settles and releases admission once", async mode => {
    const { manager, launch, stop } = fixture({ timeoutMs: mode === "deadline" ? 1 : 60_000, dead: mode === "transport-death" });
    const handle = await manager.spawn({ task: "fixture", transport: "process" });
    const file = status(manager, handle.id);
    let failures = 0;
    const fault = faults(target => target.startsWith(file + ".") && target.endsWith(".tmp") && failures++ === 0);
    const unhandled: unknown[] = [];
    const rejection = (error: unknown) => { unhandled.push(error); };
    process.on("unhandledRejection", rejection);
    try {
      const result = await manager.wait(handle.id, { timeoutMs: 4000 });
      expect(result.status).toBe(mode === "deadline" ? "timed_out" : "failed");
      expect(failures).toBeGreaterThanOrEqual(2);
      expect(unhandled).toEqual([]);
      expect(stop).toHaveBeenCalledTimes(mode === "deadline" ? 1 : 0);
      expect(launch).toHaveBeenCalledOnce();
      const next = await manager.spawn({ task: "next admitted", transport: "process" });
      expect(launch).toHaveBeenCalledTimes(2);
      await manager.stop(next.id);
    } finally { fault.mockRestore(); process.off("unhandledRejection", rejection); }
  });

  it("F17 retries a one-shot unresolved-marker publication failure before settlement", async () => {
    const { manager } = fixture({ lost: true });
    const handle = await manager.spawn({ task: "fixture", transport: "process" });
    const directory = manager.runDirectory(handle.id)!;
    let attempts = 0;
    const fault = faults(target => target.startsWith(path.join(directory, "unresolved-worker.json") + ".") && attempts++ === 0);
    try {
      expect((await manager.wait(handle.id, { timeoutMs: 4000 })).status).toBe("failed");
      expect(attempts).toBeGreaterThanOrEqual(2);
      expect(JSON.parse(fs.readFileSync(path.join(directory, "unresolved-worker.json"), "utf8")).reason).toContain("Herdr contact lost");
    } finally { fault.mockRestore(); }
  });

  it.each(["closed", "orphaned"] as const)("F17 failed veto then successful terminal publication survives manager exit and %s offline sweep", async ownerState => {
    const { temp, root, manager } = fixture({ lost: true });
    const handle = await manager.spawn({ task: "fixture", transport: "process" });
    const directory = manager.runDirectory(handle.id)!;
    let attempts = 0;
    const fault = faults(target => target.startsWith(path.join(directory, "unresolved-worker.json") + ".") && (++attempts, true));
    try {
      expect((await manager.wait(handle.id, { timeoutMs: 4000 })).status).toBe("failed");
      expect(attempts).toBeGreaterThan(0);
      expect(fs.existsSync(path.join(directory, "unresolved-worker.json"))).toBe(false);
      expect(JSON.parse(fs.readFileSync(status(manager, handle.id), "utf8"))).toMatchObject({ status: "failed", transport: "herdr" });
      await manager.close();
      fs.writeFileSync(path.join(root, ".fabric-owner.json"), JSON.stringify({ pid: 2147483647, startedAt: 1, heartbeatAt: 1, ...(ownerState === "closed" ? { closedAt: 1, childrenStopped: true } : { orphanedAt: 1 }) }));
      const removable = canRemoveTerminalRun(directory);
      const removableRoot = canRemoveManagedRunRoot(root);
      const veto = runTreeExitVeto(directory);
      const result = sweepTempRunRoots({ tempRoot: temp, now: Date.now() + 10_000, orphanedTempRunRetentionMs: 0, oneShotRunRetentionMs: 0 });
      expect(result).toEqual({ removedRoots: [], removedRuns: [] });
      expect(removable).toBe(false);
      expect(removableRoot).toBe(false);
      expect(veto).toMatch(/no checked worker exit receipt/);
      expect(fs.existsSync(path.join(directory, "task.txt"))).toBe(true);
      // A still-live pane can overwrite the synthesized failure. That cannot
      // remove the persistent transport-based obligation either.
      const record = JSON.parse(fs.readFileSync(status(manager, handle.id), "utf8"));
      fs.writeFileSync(status(manager, handle.id), JSON.stringify({ ...record, status: "completed" }));
      expect(canRemoveTerminalRun(directory)).toBe(false);
      expect(sweepTempRunRoots({ tempRoot: temp, now: Date.now() + 10_000, orphanedTempRunRetentionMs: 0, oneShotRunRetentionMs: 0 })).toEqual({ removedRoots: [], removedRuns: [] });
    } finally { fault.mockRestore(); }
  });
});
