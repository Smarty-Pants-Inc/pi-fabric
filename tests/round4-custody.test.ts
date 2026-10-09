import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { EventEmitter } from "node:events";
import type { ChildProcess } from "node:child_process";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ActorManager } from "../src/actors/manager.js";
import { MeshStore } from "../src/mesh/store.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import * as processUtils from "../src/agents/transports/process-utils.js";
import { executionGroup } from "../src/worker/execution-group.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ResidentHost } from "../src/residency/host.js";
import * as fileLock from "../src/residency/file-lock.js";
import { RESIDENT_HOST_FORMAT, type ResidentHostConfig } from "../src/residency/protocol.js";

const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
afterEach(() => vi.restoreAllMocks());

describe("round 4 execution custody", () => {
  it.each(["immediate", "queued"] as const)("F6 retains every error-layer permit for an unknown %s launch receipt", async mode => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-r4-unknown-"));
    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 1 }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: root,
    });
    let spy: ReturnType<typeof vi.spyOn> | undefined;
    try {
      const first = mode === "queued" ? await manager.spawn({ task: "HANG", transport: "process" }) : undefined;
      spy = vi.spyOn(ProcessTransport.prototype, "launch").mockRejectedValueOnce(Object.assign(new Error("unknown receipt"), { launchOutcome: "unknown" }));
      const uncertain = await manager.spawn({ task: "unknown launch", transport: "process", actorId: "unknown-actor" });
      if (first) await manager.stop(first.id);
      await expect(manager.wait(uncertain.id, { timeoutMs: 50 })).rejects.toThrow(/still running/);
      await expect(manager.stop(uncertain.id)).rejects.toThrow(/execution exit unconfirmed/);
      const next = await manager.spawn({ task: "no replacement", transport: "process" });
      expect(next.status).toBe("queued");
      expect(spy).toHaveBeenCalledTimes(1);
      await manager.stop(next.id);
      await expect(manager.close()).rejects.toThrow(/execution exit unconfirmed/);
    } finally {
      spy?.mockRestore();
      await manager.close().catch(() => undefined);
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it.skipIf(process.platform !== "linux")("P3 retries registered close after a transient cleanup failure using the exact owned exit", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-r4-retry-"));
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: root,
    });
    const original = ProcessTransport.prototype.launch;
    let blocked = true;
    let exact: Awaited<ReturnType<typeof original>> | undefined;
    const spy = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, request) {
      exact = await original.call(this, request);
      return { ...exact, stop: async () => { if (blocked) throw new Error("transient stop failure"); await exact!.stop(); } };
    });
    try {
      const handle = await manager.spawn({ task: "HANG", transport: "process" });
      await expect(manager.stop(handle.id)).rejects.toThrow();
      await expect(manager.close()).rejects.toThrow(/execution exit unconfirmed/);
      const marker = path.join(root, handle.id, "unresolved-worker.json");
      expect(fs.existsSync(marker)).toBe(true);
      blocked = false;
      await exact!.stop();
      await manager.close();
      expect(fs.existsSync(marker)).toBe(false);
      expect((await manager.wait(handle.id)).status).toBe("stopped");
    } finally {
      blocked = false; await exact?.stop(); spy.mockRestore();
      await manager.close().catch(() => undefined);
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
  it.skipIf(process.platform !== "linux").each(["immediate", "queued"] as const)("F6 fences the actor's next activation after an unconfirmed %s launch", async mode => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-r4-actor-"));
    const agents = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: mode === "immediate" ? 2 : 1 }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
    });
    const actors = new ActorManager("r4", { id: "session:r4", name: "main", kind: "main", sessionId: "r4" },
      new MeshStore(path.join(root, "mesh"), 64 * 1024, 100), { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {},
      { actorRoot: path.join(root, "actors"), persistent: false });
    const original = ProcessTransport.prototype.launch;
    const handles: Awaited<ReturnType<typeof original>>[] = [];
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    let launched!: () => void;
    const atLaunch = new Promise<void>(resolve => { launched = resolve; });
    let blocked = true;
    let actorRunId = "";
    let actorLaunches = 0;
    const spy = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, request) {
      const handle = await original.call(this, request);
      handles.push(handle);
      if (request.name !== "r4 actor") return handle;
      if (++actorLaunches !== 1) return handle;
      actorRunId = request.id; launched(); await gate;
      return { ...handle,
        stop: async () => { if (blocked) throw new Error("injected actor cleanup unconfirmed"); await handle.stop(); },
        isAlive: async () => { if (blocked) throw new Error("injected actor exit receipt unavailable"); return handle.isAlive(); },
      };
    });
    try {
      const blocker = mode === "queued" ? await agents.spawn({ task: "HANG", transport: "process" }) : undefined;
      const actor = await actors.create({ name: "r4 actor", instructions: "Reply.", responseMode: "text", transport: "process" });
      actors.tell(actor.id, "HANG first activation");
      if (blocker) {
        await vi.waitFor(() => expect(agents.list().some(run => run.actorId === actor.id && run.status === "queued")).toBe(true));
        await agents.stop(blocker.id);
      }
      await atLaunch;
      expect(actors.haltAll().halted).toBe(1);
      release();
      await vi.waitFor(() => expect(fs.existsSync(path.join(root, "runs", actorRunId, "unresolved-worker.json"))).toBe(true));
      actors.tell(actor.id, "second activation");
      actors.dispatchHostEvent("input", { source: "user" });
      await delay(150);
      expect(actors.inFlightCount()).toBe(1);
      expect(actorLaunches).toBe(1);
      await expect(agents.stop(actorRunId)).rejects.toThrow(/execution exit unconfirmed/);
      await expect(agents.wait(actorRunId, { timeoutMs: 20 })).rejects.toThrow(/still running/);
      // Immediate mode deliberately has another free permit: only the actor's
      // pending result/controller prevents a same-session activation overlap.
      if (mode === "immediate") {
        const unrelated = await agents.spawn({ task: "HANG", transport: "process" });
        expect(unrelated.status).toBe("running");
        await agents.stop(unrelated.id);
      }
      blocked = false;
      await agents.stop(actorRunId);
      await vi.waitFor(() => expect(actorLaunches).toBe(2), { timeout: 10000 });
      await vi.waitFor(() => expect(actors.inFlightCount()).toBe(0), { timeout: 10000 });
    } finally {
      blocked = false; release();
      for (const handle of handles) await handle.stop();
      if (actorRunId) await agents.stop(actorRunId).catch(() => undefined);
      spy.mockRestore();
      await actors.close(); await agents.close().catch(() => undefined);
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 20000);
  it("F5 refuses a Windows execution-tree receipt even after native child close", () => {
    vi.spyOn(process, "platform", "get").mockReturnValue("win32");
    const child = Object.assign(new EventEmitter(), { pid: 123, exitCode: 0, signalCode: null }) as unknown as ChildProcess;
    expect(() => executionGroup(child)).toThrow(/Windows execution-tree custody is unsupported/);
  });

  it("F5 Windows process admission uses the legacy native-child path, not the tree-custody channel", async () => {
    vi.spyOn(process, "platform", "get").mockReturnValue("win32");
    const spawn = vi.spyOn(processUtils, "spawnDetached").mockResolvedValue({ pid: 123, closed: Promise.resolve(), stop: async () => {}, isAlive: async () => false, lostContact: () => undefined, waitForClose: async () => {} });
    await new ProcessTransport().launch({ id: "windows", name: "windows", cwd: process.cwd(), workerPath: "worker.js", workerArguments: [] });
    expect(spawn.mock.calls[0]?.[5]).toBeUndefined();
    expect(spawn.mock.calls[0]?.[6]).toBe(7_000);
    expect(spawn.mock.calls[0]?.[7]).toBe(false);
  });

  it.skipIf(process.platform !== "linux")("F7 protects a fresh inode's creator from a contender acquiring its flock first", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-r4-first-claim-"));
    const config: ResidentHostConfig = {
      format: RESIDENT_HOST_FORMAT, rootId: "session:r4", sessionId: "r4", cwd: process.cwd(), projectRoot: process.cwd(),
      meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"), residencyRoot: path.join(root, "resident"),
      fullCodeMode: true, agents: DEFAULT_FABRIC_CONFIG.agents, mesh: DEFAULT_FABRIC_CONFIG.mesh, retention: DEFAULT_FABRIC_CONFIG.retention,
      workerPath: path.resolve("dist/worker.js"), fabricExtensionPath: path.resolve("dist/index.js"), piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
    };
    const first = new ResidentHost(config);
    const second = new ResidentHost(config);
    const lock = path.join(config.residencyRoot, "host.lock");
    const realLock = fileLock.lockFile;
    let atCreator!: () => void;
    const creatorPaused = new Promise<void>(resolve => { atCreator = resolve; });
    let resumeCreator!: () => void;
    const creatorGate = new Promise<void>(resolve => { resumeCreator = resolve; });
    let resumeContender!: () => void;
    const contenderGate = new Promise<void>(resolve => { resumeContender = resolve; });
    let contenderHeld = false;
    let attempts = 0;
    const spy = vi.spyOn(fileLock, "lockFile").mockImplementation(async (...args) => {
      if (args[0] !== lock) return realLock(...args);
      if (++attempts === 1) { atCreator(); await creatorGate; }
      const fd = await realLock(...args);
      if (attempts > 1) { contenderHeld = true; await contenderGate; }
      return fd;
    });
    let a: Promise<PromiseSettledResult<void>> | undefined;
    let b: Promise<PromiseSettledResult<void>> | undefined;
    try {
      a = first.start().then(() => ({ status: "fulfilled", value: undefined } as const), reason => ({ status: "rejected", reason } as const));
      await creatorPaused;
      b = second.start().then(() => ({ status: "fulfilled", value: undefined } as const), reason => ({ status: "rejected", reason } as const));
      // Old code lets B acquire host.lock; fixed code refuses B on the first-claim guard.
      await vi.waitFor(() => expect(contenderHeld || attempts === 1 && fs.existsSync(path.join(config.residencyRoot, "host-fence-establish.lock"))).toBe(true));
      resumeCreator();
      await a;
      resumeContender();
      const results = await Promise.all([a, b]);
      expect(results.filter(result => result.status === "fulfilled")).toHaveLength(1);
      const inode = fs.statSync(lock).ino;
      await Promise.all([first.close(), second.close()]);
      spy.mockRestore();
      const next = new ResidentHost(config);
      await next.start();
      expect(fs.statSync(lock).ino).toBe(inode);
      await next.close();
    } finally {
      resumeCreator?.(); resumeContender?.();
      await Promise.all([a, b]);
      spy.mockRestore();
      await Promise.all([first.close(), second.close()]);
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 15000);

  it.skipIf(process.platform !== "linux")("F6 retains queued stop, wait, files and admission for a live pre-registration process, then retries exact exit", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-r4-admission-"));
    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 1, retainRuns: false }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: root,
    });
    const realLaunch = ProcessTransport.prototype.launch;
    let releaseLaunch!: () => void;
    const gate = new Promise<void>(resolve => { releaseLaunch = resolve; });
    let launched!: () => void;
    const atLaunch = new Promise<void>(resolve => { launched = resolve; });
    let exactHandle: Awaited<ReturnType<typeof realLaunch>> | undefined;
    let blocked = true;
    let calls = 0;
    const spy = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, request) {
      calls++;
      const handle = await realLaunch.call(this, request);
      if (request.name !== "cancel postlaunch") return handle;
      exactHandle = handle;
      launched(); await gate;
      return { ...handle, stop: async () => { if (blocked) throw new Error("injected cleanup unconfirmed"); await handle.stop(); },
        isAlive: async () => { if (blocked) throw new Error("injected receipt unavailable"); return handle.isAlive(); } };
    });
    let stopped: Promise<unknown> | undefined;
    try {
      const first = await manager.spawn({ task: "HANG", transport: "process" });
      const queued = await manager.spawn({ task: "HANG", name: "cancel postlaunch", transport: "process", actorId: "r4-actor" });
      await manager.stop(first.id);
      await atLaunch;
      stopped = manager.stop(queued.id);
      // Attach a rejection observer immediately: unconfirmed stop must be bounded, not terminal success.
      const outcome = stopped.then(value => ({ value }), error => ({ error }));
      releaseLaunch();
      const result = await outcome;
      expect(result).toHaveProperty("error");
      expect(String((result as { error?: unknown }).error)).toMatch(/unconfirmed|unavailable/);
      expect(await exactHandle!.isAlive()).toBe(true);
      let completed = false;
      void manager.wait(queued.id).then(() => { completed = true; });
      const next = await manager.spawn({ task: "HANG", transport: "process" });
      await delay(100);
      expect(completed).toBe(false);
      expect(next.status).toBe("queued");
      expect(calls).toBe(2);
      const runDirectory = path.join(root, queued.id);
      expect(fs.existsSync(path.join(runDirectory, "task.txt"))).toBe(true);
      expect(fs.existsSync(path.join(runDirectory, "unresolved-worker.json"))).toBe(true);
      await expect(manager.cleanup(queued.id)).rejects.toThrow(/running agent/);
      blocked = false;
      expect((await manager.stop(queued.id)).status).toBe("stopped");
      expect((await manager.wait(queued.id)).status).toBe("stopped");
      expect(fs.existsSync(path.join(runDirectory, "unresolved-worker.json"))).toBe(false);
      await vi.waitFor(() => expect(calls).toBe(3));
      await manager.stop(next.id);
      expect(await manager.cleanup(queued.id)).toEqual({ cleaned: true });
    } finally {
      blocked = false; releaseLaunch();
      await stopped?.catch(() => undefined);
      await exactHandle?.stop();
      spy.mockRestore();
      await manager.close().catch(() => undefined);
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 35000); // three refusing workers each retain the 7s cgroup grace
});
