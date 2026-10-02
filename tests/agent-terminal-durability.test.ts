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

const runtimeFixture = (temp: string, entries: unknown[]) => {
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
  const config = normalizeFabricConfig({ fullCodeMode: false, retention: { orphanedTempRunMs: 60 * 60 * 1000, oneShotRunMs: 60 * 60 * 1000 }, agents: { enabled: true, budgetUsd: 0, retainRuns: false, notifyOnComplete: true, sessionExport: false }, mcp: { enabled: false }, memory: { enabled: false }, residency: { enabled: false }, mesh: { enabled: true, root: path.join(temp, "mesh") }, prewalk: { enabled: false, alwaysRearm: false } });
  return { runtime, sendMessage, init: () => runtime.initialize(context, config) };
};

describe("Astra F15-F22 terminal publication obligations", () => {
  it.skipIf(process.platform === "win32")("F18 two-run runtime replacement preserves answers across slow stop and older recovery waits", async () => {
    const { temp, manager: unused, exit, launch } = fixture({ retainRuns: false });
    await unused.close();
    const entries: unknown[] = [];
    vi.stubEnv("PI_FABRIC_PROJECT_ROOT", temp);
    vi.stubEnv("PI_FABRIC_TMPDIR", temp);
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(temp, "agent"));
    for (const name of ["PI_FABRIC_RUN_ROOT", "PI_FABRIC_MESH_ROOT", "PI_FABRIC_ACTOR_ID", "PI_FABRIC_PARENT_RUN", "PI_FABRIC_SESSION_ID", "PI_FABRIC_MAIN_AGENT_ID"]) vi.stubEnv(name, undefined);
    let first: ReturnType<typeof runtimeFixture> | undefined = runtimeFixture(temp, entries);
    let second: ReturnType<typeof runtimeFixture> | undefined;
    let third: ReturnType<typeof runtimeFixture> | undefined;
    let fourth: ReturnType<typeof runtimeFixture> | undefined;
    let releaseSlowStop: (() => void) | undefined;
    let closing: Promise<void> | undefined;
    let shutdown: Promise<void> | undefined;
    let fault: ReturnType<typeof faults> | undefined;
    try {
      await first.init();
      const handle = await first.runtime.agents.spawn({ task: "A", transport: "process", model: "dest/old" });
      const originalLaunch = launch.getMockImplementation()!;
      let slowAlive = true;
      const exitDelay = new Promise<void>(resolve => { releaseSlowStop = resolve; });
      const slowStop = vi.fn(async () => { await exitDelay; slowAlive = false; });
      launch.mockImplementationOnce(async request => ({ ...await originalLaunch(request), isAlive: async () => slowAlive, stop: slowStop }));
      const slow = await first.runtime.agents.spawn({ task: "B", transport: "process", model: "dest/old" });
      const file = status(first.runtime.agents, handle.id), directory = path.dirname(file);
      const answer = { ...JSON.parse(fs.readFileSync(file, "utf8")), status: "completed", sessionId: "2147483647", finishedAt: Date.now(), text: "A's exact completed answer\n" + "full answer ".repeat(2000), value: { exact: [1, "retained", { result: true }] }, usage: { input: 123, output: 456, cacheRead: 789, cacheWrite: 12, cost: 0.345 }, turns: 7 };
      fs.writeFileSync(file, JSON.stringify(answer));
      exit();
      let confirmations = 0;
      fault = faults((target, fd) => target === directory && fs.fstatSync(fd).isDirectory() && ++confirmations === 1);
      const originalStop = first.runtime.agents.stop.bind(first.runtime.agents);
      let rejected = false, settled = false, closed = false;
      vi.spyOn(first.runtime.agents, "stop").mockImplementation(async id => {
        try { return await originalStop(id); }
        catch (error) { if (id === handle.id) rejected = true; throw error; }
      });
      // join observes real settlement without consuming A or saving a session answer.
      const joined = first.runtime.agents.join(handle.id).then(() => { settled = true; });
      // Enter close synchronously so its stop sees the one-shot failure before the monitor.
      closing = first.runtime.agents.close().then(() => { closed = true; });
      shutdown = first.runtime.shutdown();
      await vi.waitFor(() => {
        expect(rejected).toBe(true);
        expect(slowStop).toHaveBeenCalledOnce();
        expect(settled).toBe(true);
      }, { timeout: 4000 });
      await joined;
      expect(confirmations).toBeGreaterThan(1);
      expect(closed).toBe(false);
      expect(first.sendMessage).not.toHaveBeenCalled();
      expect(entries.some(entry => (entry as { data?: { delivered?: string[] } }).data?.delivered?.includes(handle.id))).toBe(false);
      expect(fs.existsSync(file)).toBe(true);
      await expect(first.runtime.agents.cleanup(handle.id)).rejects.toThrow(/not durably preserved/);
      expect(fs.existsSync(file)).toBe(true);
      releaseSlowStop!();
      await closing;
      await shutdown;
      first = undefined; // Recovery must use the saved session, not the settled old manager.
      expect(entries).toContainEqual(expect.objectContaining({ customType: STOPPED_AGENTS_ENTRY, data: expect.objectContaining({ stopped: expect.arrayContaining([expect.objectContaining({ ...answer, terminalPending: { statusFile: file, publication: false } })]) }) }));
      fault.mockRestore();
      vi.mocked(fs.openSync).mockRestore();
      let recoveryUnavailable = true, recoveryBarriers = 0;
      fault = faults(target => path.basename(target).startsWith("status.json.") && target.endsWith(".tmp") && (++recoveryBarriers, recoveryUnavailable));
      // Control only poll/inbox timeouts; runtime intervals and I/O remain real.
      vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
      second = runtimeFixture(temp, entries);
      await second.init();
      await expect(second.runtime.agents.wait(handle.id)).rejects.toThrow(/publication barrier/);
      expect(recoveryBarriers).toBeGreaterThan(0);
      expect(JSON.stringify(second.sendMessage.mock.calls)).not.toContain("A's exact completed answer");
      expect(entries.some(entry => (entry as { data?: { delivered?: string[] } }).data?.delivered?.includes(handle.id))).toBe(false);
      // Put fresh N's next monitor tick just after R=A's retry. R then sleeps
      // until 500 ms, leaving N's 251 ms tick inside close's initial recovery wait.
      await vi.advanceTimersByTimeAsync(1);
      let freshAlive = true;
      launch.mockImplementationOnce(async request => ({ ...await originalLaunch(request), isAlive: async () => freshAlive, stop: async () => { freshAlive = false; } }));
      const fresh = await second.runtime.agents.spawn({ task: "N", transport: "process", model: "dest/old" });
      await vi.advanceTimersByTimeAsync(249);
      const freshFile = status(second.runtime.agents, fresh.id);
      const freshAnswer = { ...JSON.parse(fs.readFileSync(freshFile, "utf8")), status: "completed", sessionId: "2147483647", finishedAt: Date.now(), text: "N's exact completed answer\n" + "fresh full answer ".repeat(2000), value: { fresh: [2, "retained", { result: true }] }, usage: { input: 234, output: 567, cacheRead: 890, cacheWrite: 23, cost: 0.456 }, turns: 8 };
      fs.writeFileSync(freshFile, JSON.stringify(freshAnswer));
      freshAlive = false;
      let freshSettled = false, recoveryCloseDone = false;
      const freshJoined = second.runtime.agents.join(fresh.id).then(() => { freshSettled = true; });
      closing = second.runtime.agents.close().then(() => { recoveryCloseDone = true; });
      await vi.advanceTimersByTimeAsync(1);
      expect(freshSettled).toBe(true);
      await freshJoined;
      expect(recoveryCloseDone).toBe(false);
      expect(JSON.stringify(second.sendMessage.mock.calls)).not.toContain("N's exact completed answer");
      expect(entries.some(entry => (entry as { data?: { delivered?: string[] } }).data?.delivered?.includes(fresh.id))).toBe(false);
      expect(fs.existsSync(freshFile)).toBe(true);
      await vi.advanceTimersByTimeAsync(249);
      await closing;
      await second.runtime.shutdown();
      second = undefined; // Neither answer may depend on the discarded manager.
      vi.useRealTimers();
      recoveryUnavailable = false;
      third = runtimeFixture(temp, entries);
      await third.init();
      // Session-only restoration must retain full N, not its compact UI record.
      expect(third.runtime.agents.status(fresh.id)).toMatchObject(freshAnswer);
      expect(await third.runtime.agents.wait(fresh.id)).toMatchObject(freshAnswer);
      // The completion inbox may batch A and B, or deliver B before A's storage recovers.
      await vi.waitFor(() => expect(JSON.stringify(third!.sendMessage.mock.calls)).toContain("A's exact completed answer"), { timeout: 4000 });
      expect(third.runtime.agents.status(handle.id)).toMatchObject(answer);
      expect(await third.runtime.agents.wait(handle.id)).toMatchObject(answer);
      expect(third.runtime.agents.status(slow.id)).toMatchObject({ status: "stopped" });
      expect(launch).toHaveBeenCalledTimes(3);
      await third.runtime.shutdown();
      third = undefined;
      fourth = runtimeFixture(temp, entries);
      await fourth.init();
      expect(await fourth.runtime.agents.wait(handle.id)).toMatchObject(answer);
      expect(await fourth.runtime.agents.wait(fresh.id)).toMatchObject(freshAnswer);
      expect(fourth.sendMessage).not.toHaveBeenCalled();
      expect(launch).toHaveBeenCalledTimes(3);
    } finally {
      fault?.mockRestore();
      releaseSlowStop?.();
      if (vi.isFakeTimers()) {
        await vi.advanceTimersByTimeAsync(1000);
        vi.useRealTimers();
      }
      await closing;
      await shutdown;
      await first?.runtime.shutdown();
      await second?.runtime.shutdown();
      await third?.runtime.shutdown();
      await fourth?.runtime.shutdown();
      vi.useRealTimers();
      vi.unstubAllEnvs();
    }
  }, 30_000);

  it.skipIf(process.platform === "win32").each(["original", "collected", "collected-pre-rename", "collected-post-rename"] as const)("F18 replacement runtime restores the exact session answer after %s storage handoff", async mode => {
    const { temp, manager: unused, exit, launch } = fixture({ retainRuns: false });
    await unused.close();
    const entries: unknown[] = [];
    const createRuntime = () => runtimeFixture(temp, entries);
    vi.stubEnv("PI_FABRIC_PROJECT_ROOT", temp);
    vi.stubEnv("PI_FABRIC_TMPDIR", temp);
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(temp, "agent"));
    for (const name of ["PI_FABRIC_RUN_ROOT", "PI_FABRIC_MESH_ROOT", "PI_FABRIC_ACTOR_ID", "PI_FABRIC_PARENT_RUN", "PI_FABRIC_SESSION_ID", "PI_FABRIC_MAIN_AGENT_ID"]) vi.stubEnv(name, undefined);
    let first: ReturnType<typeof createRuntime> | undefined = createRuntime();
    let second: ReturnType<typeof createRuntime> | undefined;
    let third: ReturnType<typeof createRuntime> | undefined;
    let fault: ReturnType<typeof faults> | undefined;
    try {
      await first.init();
      const handle = await first.runtime.agents.spawn({ task: "fixture", transport: "process", model: "dest/old" });
      const file = status(first.runtime.agents, handle.id), directory = path.dirname(file);
      const answer = { ...JSON.parse(fs.readFileSync(file, "utf8")), status: "completed", sessionId: "2147483647", finishedAt: Date.now(), text: "original completed answer after replacement\n" + "full answer ".repeat(2000), value: { exact: [1, "retained", { result: true }] }, usage: { input: 123, output: 456, cacheRead: 789, cacheWrite: 12, cost: 0.345 }, turns: 7 };
      fs.writeFileSync(file, JSON.stringify(answer));
      exit();
      let unavailable = true, confirmations = 0;
      fault = faults((target, fd) => target === directory && fs.fstatSync(fd).isDirectory() && (++confirmations, unavailable));
      await first.runtime.shutdown();
      first = undefined; // The old manager/runtime is no longer accessible to recovery.
      expect(confirmations).toBeGreaterThan(0);
      expect(fs.existsSync(file)).toBe(true);
      expect(entries).toContainEqual(expect.objectContaining({ customType: STOPPED_AGENTS_ENTRY, data: expect.objectContaining({ stopped: expect.arrayContaining([expect.objectContaining({ ...answer, terminalPending: { statusFile: file, publication: false } })]) }) }));
      let recoveryBarriers = 0;
      if (mode !== "original") {
        fault.mockRestore();
        vi.mocked(fs.openSync).mockRestore();
        const root = path.dirname(directory), now = Date.now(), grace = 60 * 60 * 1000;
        // The old host is gone; let the real collector detect the orphan, then
        // expire the shortest configured grace. Do not just unlink status.json.
        fs.writeFileSync(path.join(root, ".fabric-owner.json"), JSON.stringify({ pid: 2147483647, startedAt: 1, heartbeatAt: 1 }));
        const sweep = (at: number) => sweepTempRunRoots({ tempRoot: temp, now: at, orphanedTempRunRetentionMs: grace, oneShotRunRetentionMs: grace });
        expect(sweep(now)).toEqual({ removedRoots: [], removedRuns: [] });
        expect(JSON.parse(fs.readFileSync(path.join(root, ".fabric-owner.json"), "utf8")).orphanedAt).toBe(now);
        expect(sweep(now + grace - 1)).toEqual({ removedRoots: [], removedRuns: [] });
        expect(fs.existsSync(file)).toBe(true);
        expect(sweep(now + grace + 1).removedRoots).toContain(root);
        expect(fs.existsSync(root)).toBe(false);
        unavailable = mode !== "collected";
        fault = faults((target, fd) => {
          const recoveryPublication = mode === "collected-pre-rename"
            ? path.basename(target).startsWith("status.json.") && target.endsWith(".tmp")
            : mode === "collected-post-rename" && fs.fstatSync(fd).isDirectory() && fs.existsSync(path.join(target, "status.json"));
          return !!recoveryPublication && (++recoveryBarriers, unavailable);
        });
      }
      second = createRuntime();
      await second.init();
      if (unavailable) {
        await expect(second.runtime.agents.wait(handle.id, { timeoutMs: 350 })).rejects.toThrow(/publication barrier/);
        expect(() => second!.runtime.agents.status(handle.id)).toThrow(/publication barrier/);
        expect(second.sendMessage).not.toHaveBeenCalled();
        expect(entries.some(entry => (entry as { data?: { delivered?: string[] } }).data?.delivered?.includes(handle.id))).toBe(false);
        if (mode !== "original") {
          expect(recoveryBarriers).toBeGreaterThan(0);
          // Discard this failed recovery manager as well: the only retained
          // obligation for the next attempt is still the original session entry.
          await second.runtime.shutdown();
          second = undefined;
          unavailable = false;
          second = createRuntime();
          await second.init();
        }
      }
      unavailable = false;
      await vi.waitFor(() => expect(second!.sendMessage).toHaveBeenCalledOnce(), { timeout: 4000 });
      expect(second.runtime.agents.status(handle.id)).toMatchObject(answer);
      expect(await second.runtime.agents.wait(handle.id)).toMatchObject(answer);
      expect(entries).toContainEqual(expect.objectContaining({ data: { stopped: [expect.not.objectContaining({ terminalPending: expect.anything() })] } }));
      if (mode !== "original") expect(fs.existsSync(file)).toBe(false);
      expect(launch).toHaveBeenCalledOnce();
      await second.runtime.shutdown();
      second = undefined;
      third = createRuntime();
      await third.init();
      expect(await third.runtime.agents.wait(handle.id)).toMatchObject(answer);
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

  it.each(["lstat-EIO", "open-EACCES", "open-ENOENT", "mismatched", "mismatched-publication"] as const)("F18 never republishes an existing target on %s", async mode => {
    const { manager, launch, consumed, root } = fixture();
    const original = path.join(root, "previous", "status.json");
    fs.mkdirSync(path.dirname(original), { recursive: true });
    const answer: AgentRunResult = { id: "previous", name: "previous", task: "already done", status: "completed", runner: "pi", transport: "process", cwd: root, startedAt: 1, updatedAt: 2, finishedAt: 2, turns: 3, toolCalls: 4, text: "retained answer", value: { exact: true }, usage: { input: 1, output: 2, cacheRead: 3, cacheWrite: 4, cost: 0.5 } };
    const bytes = JSON.stringify({ ...answer, id: mode.startsWith("mismatched") ? "someone-else" : answer.id });
    fs.writeFileSync(original, bytes);
    const confirmed = vi.fn();
    let fault: { mockRestore: () => void } | undefined;
    if (mode === "lstat-EIO") {
      const stat = fs.lstatSync.bind(fs);
      fault = vi.spyOn(fs, "lstatSync").mockImplementation((...args: Parameters<typeof fs.lstatSync>) => {
        if (String(args[0]) === original) throw Object.assign(new Error("injected original I/O failure"), { code: "EIO" });
        return stat(...args);
      });
    } else if (mode.startsWith("open-")) {
      const open = fs.openSync.bind(fs);
      fault = vi.spyOn(fs, "openSync").mockImplementation((file, flags, permissions) => {
        if (String(file) === original) throw Object.assign(new Error("injected original I/O failure"), { code: mode.slice(5) });
        return open(file, flags, permissions);
      });
    }
    try {
      manager.restorePreviousRuns([{ ...answer, terminalPending: { statusFile: original, publication: mode === "mismatched-publication" } }], confirmed);
      const message = mode.startsWith("mismatched") ? /identity changed/ : /original I\/O failure/;
      await expect(manager.wait(answer.id)).rejects.toThrow(message);
      expect(() => manager.status(answer.id)).toThrow(message);
      expect(confirmed).not.toHaveBeenCalled();
      expect(consumed).not.toHaveBeenCalled();
      expect(launch).not.toHaveBeenCalled();
      expect(fs.readdirSync(root)).toEqual(["previous"]);
      expect(fs.readFileSync(original, "utf8")).toBe(bytes);
      await manager.close();
    } finally { fault?.mockRestore(); }
  });

  it.each([false, true])("F18 missing status republishes in successor storage but gates delivery on session saving (publication=%s)", async publication => {
    const { manager, root, launch, consumed } = fixture();
    const oldDirectory = path.join(root, "old-run");
    fs.mkdirSync(oldDirectory, { recursive: true });
    const original = path.join(oldDirectory, "status.json"); // Only the status path is absent.
    const answer: AgentRunResult = { id: "previous", name: "previous", task: "already done", status: "completed", runner: "pi", transport: "process", cwd: root, startedAt: 1, updatedAt: 2, finishedAt: 2, turns: 3, toolCalls: 4, text: "retained answer", value: { exact: true }, usage: { input: 1, output: 2, cacheRead: 3, cacheWrite: 4, cost: 0.5 } };
    const entries: unknown[] = [{ type: "custom", customType: STOPPED_AGENTS_ENTRY, data: { stopped: [{ ...answer, terminalPending: { statusFile: original, publication } }] } }];
    let sessionUnavailable = true;
    const enqueue = vi.fn();
    restoreStoppedRuns({ entries, notifyOnComplete: true,
      restore: (runs, confirmed) => manager.restorePreviousRuns(runs, confirmed), enqueue,
      appendEntry: data => { if (sessionUnavailable) throw new Error("recovery session save unavailable"); entries.push({ type: "custom", customType: STOPPED_AGENTS_ENTRY, data }); },
    });
    await expect(manager.wait(answer.id)).rejects.toThrow("recovery session save unavailable");
    expect(() => manager.status(answer.id)).toThrow("recovery session save unavailable");
    expect(consumed).not.toHaveBeenCalled();
    expect(enqueue).not.toHaveBeenCalled();
    expect(entries).toHaveLength(1);
    expect(fs.existsSync(original)).toBe(false);
    const recoveredDirectory = fs.readdirSync(root).find(name => name !== "old-run")!;
    expect(JSON.parse(fs.readFileSync(path.join(root, recoveredDirectory, "status.json"), "utf8"))).toEqual(answer);
    sessionUnavailable = false;
    expect(await manager.wait(answer.id)).toEqual(answer);
    expect(consumed).toHaveBeenCalledOnce();
    expect(enqueue).toHaveBeenCalledOnce();
    expect(entries.at(-1)).toMatchObject({ data: { stopped: [answer] } });
    expect(fs.readdirSync(root)).toHaveLength(2); // Retries reuse this runtime's publication target.
    expect(launch).not.toHaveBeenCalled();
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
      // Later monitor recovery cannot authorize collection after the session save failed.
      await expect(manager.cleanup(handle.id)).rejects.toThrow(/not durably preserved/);
      expect(fs.existsSync(file)).toBe(true);
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
