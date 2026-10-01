import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import * as processIdentity from "../src/core/process-identity.js";
import { ActorDirectory } from "../src/actors/directory.js";
import type { FabricActorMessage } from "../src/actors/types.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import * as retention from "../src/storage/retention.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { LifecycleBroker } from "../src/lifecycle/broker.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import { ResidencyClient } from "../src/residency/client.js";
import { residentHostId, residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { writeHostLease } from "../src/topology/host-leases.js";
import { lockFile } from "../src/residency/file-lock.js";
import * as residencyFileLock from "../src/residency/file-lock.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import type { FabricParticipantInfo, FabricParticipantSource } from "../src/topology/types.js";
import { startTime, stopOwned, type Owned } from "./helpers/owned-processes.js";

const successorLoads = vi.hoisted(() => vi.fn());
vi.mock("../src/residency/successor-removal.js", async original => {
  successorLoads();
  return original<typeof import("../src/residency/successor-removal.js")>();
});
const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => { for (const cleanup of cleanups.splice(0).reverse()) await cleanup(); });
const context = { cwd: process.cwd(), update() {} } as unknown as FabricInvocationContext;
const kernelId = () => `${fs.readFileSync("/proc/sys/kernel/random/boot_id", "utf8").trim()}/${fs.readlinkSync("/proc/self/ns/pid")}`;

// Real disposable processes: every signal is fenced by the fixture's recorded start time.
const child = async (script = "setInterval(() => {}, 1000)") => {
  const proc = spawn(process.execPath, ["-e", script], { stdio: ["ignore", "ignore", "ignore", "ipc"], detached: true });
  await new Promise<void>((resolve, reject) => { proc.once("spawn", resolve); proc.once("error", reject); });
  const pid = proc.pid!;
  const owned: Owned = { pid, started: startTime(pid), at: Date.now(), argv: [] };
  const identity = { pid, startTime: owned.started, kernelId: kernelId(), commandLine: fs.readFileSync(`/proc/${pid}/cmdline`, "utf8") };
  const exited = new Promise<void>((resolve) => proc.once("exit", () => resolve()));
  cleanups.push(async () => { if (await stopOwned(owned, 1_000, 1_000)) throw new Error(`Fixture process ${pid} did not stop`); await exited; });
  return { proc, owned, identity, exited };
};
const dead = async (process: Pick<Awaited<ReturnType<typeof child>>, "owned" | "exited">) => {
  await stopOwned(process.owned, 1_000, 1_000);
  await process.exited;
};

const fixture = async (options: { liveMain?: boolean; project?: string; agentName?: string; role?: string; mismatchHost?: boolean; noIdentity?: boolean; realHost?: boolean; sessionRegistry?: boolean; sharedRuntime?: boolean; fakeProcesses?: boolean } = {}) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-successor-"));
  cleanups.push(async () => fs.rmSync(root, { recursive: true, force: true }));
  const main = options.fakeProcesses ? fakeChild() : await child();
  const host = options.fakeProcesses ? fakeChild() : await child();
  if (!options.liveMain && !options.fakeProcesses) await dead(main);
  const project = fs.realpathSync(process.cwd());
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const meshConfig = { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 };
  const agents = new AgentManager(project, DEFAULT_FABRIC_CONFIG.agents, { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs") });
  cleanups.push(() => agents.close());
  const oldId = "session:predecessor";
  const identity: MeshIdentity = { id: "session:successor", name: "main", kind: "main", sessionId: "successor" };
  const rootRecord = (id: string, processIdentity: typeof main.identity): FabricParticipantInfo => ({
    format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id,
    name: "main", agentName: "knowledge-lead", role: "project-agent", project,
    processIdentity, status: "idle", runner: "pi", transport: "host", capabilities: ["fabric"],
    sessionId: id.slice(8), startedAt: 1, updatedAt: 1, controlProtocol: "v1", local: id === identity.id, stale: id !== identity.id,
  } as FabricParticipantInfo);
  const caller = rootRecord(identity.id, options.fakeProcesses ? { ...fakeChild().identity, pid: process.pid } :
    { pid: process.pid, startTime: startTime(process.pid), kernelId: kernelId(), commandLine: fs.readFileSync(`/proc/${process.pid}/cmdline`, "utf8") });
  const predecessor = { ...rootRecord(oldId, main.identity), project: options.project ?? project,
    agentName: options.agentName ?? "knowledge-lead", role: options.role ?? "project-agent" };
  const participants: FabricParticipantSource = {
    self: () => caller, get: (id) => id === caller.id ? caller : options.liveMain && id === oldId ? { ...predecessor, stale: false } : undefined,
    list: () => [caller, ...(options.liveMain ? [{ ...predecessor, stale: false }] : [])], peers: () => [],
    refresh: async () => {}, scheduleRefresh() {},
  };
  const projectActorRoot = path.join(mesh.root, "actors");
  const actorRoot = options.sessionRegistry ? path.join(projectActorRoot, "predecessor") : projectActorRoot;
  const oldActors = new ActorManager("predecessor", { id: residentHostId(oldId), name: "Fabric resident host", kind: "agent" }, mesh, meshConfig, agents, () => {},
    { persistent: true, actorRoot, actorScope: options.sessionRegistry ? "session" : "project", claimResidency: "durable", rootId: oldId, project, role: "project-agent", canManageActor: () => undefined });
  cleanups.push(() => oldActors.close());
  const actor = await oldActors.create({ name: "durable-review", instructions: "Review changes.", residency: "durable" });
  await oldActors.close();
  const actors = new ActorDirectory(["successor", identity, mesh, meshConfig, agents, () => {},
    { persistent: true, claimResidency: "session", rootId: identity.id, project, role: "project-agent", canManageActor: () => undefined }],
    { project: projectActorRoot, session: path.join(projectActorRoot, "successor") }, "project");
  cleanups.push(() => actors.close());
  const config: ResidentHostConfig = { format: 1, rootId: identity.id, sessionId: "successor", cwd: project, projectRoot: project,
    project, role: "project-agent", rootOwner: caller, meshRoot: mesh.root, actorRoot: projectActorRoot, sessionActorRoot: path.join(projectActorRoot, "successor"),
    residencyRoot: residentRoot(mesh.root, identity.id), fullCodeMode: true, agents: DEFAULT_FABRIC_CONFIG.agents,
    mesh: meshConfig, retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: "", fabricExtensionPath: "", piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
  } as ResidentHostConfig;
  const oldDir = residentRoot(mesh.root, oldId);
  fs.mkdirSync(path.join(oldDir, "runs"), { recursive: true });
  fs.writeFileSync(path.join(oldDir, "config.json"), JSON.stringify({ ...config, rootId: oldId, sessionId: "predecessor", residencyRoot: oldDir, project: predecessor.project, role: predecessor.role, sessionActorRoot: options.sessionRegistry ? actorRoot : path.join(projectActorRoot, "predecessor"),
    rootOwner: options.noIdentity ? { ...predecessor, processIdentity: undefined } : predecessor }));
  const processIdentity = { ...host.identity, ...(options.mismatchHost ? { startTime: `${host.identity.startTime}-wrong` } : {}) };
  fs.writeFileSync(path.join(oldDir, "owner.json"), JSON.stringify({ format: 1, hostId: residentHostId(oldId), pid: host.proc.pid, token: "fixture", startedAt: 1, readyAt: 1, processIdentity }));
  fs.writeFileSync(path.join(oldDir, "host.lock"), JSON.stringify({ pid: host.proc.pid, token: "fixture", processIdentity }));
  let realOwner: { pid: number; processIdentity?: { startTime: string; commandLine: string; pid: number } } | undefined;
  if (options.realHost) {
    await dead(host);
    fs.rmSync(path.join(oldDir, "owner.json"));
    fs.rmSync(path.join(oldDir, "host.lock"));
    const oldConfig = JSON.parse(fs.readFileSync(path.join(oldDir, "config.json"), "utf8")) as ResidentHostConfig;
    oldConfig.workerPath = path.resolve("tests/fixtures/fake-worker.mjs");
    oldConfig.fabricExtensionPath = path.resolve("dist/index.js");
    const oldClient = new ResidencyClient({ config: oldConfig, mesh, participants,
      mainAgent: { id: oldId, local: true } as FabricMainAgentTarget, hostPath: path.resolve("dist/residency/launcher.js") });
    cleanups.push(() => oldClient.close());
    realOwner = await oldClient.ensureHost();
    const owned = { pid: realOwner.pid, started: realOwner.processIdentity!.startTime, at: Date.now(), argv: [] };
    cleanups.push(async () => { if (await stopOwned(owned, 20_000, 5_000)) throw new Error(`Resident fixture ${owned.pid} did not stop`); });
  }
  const mainAgent = { id: identity.id, local: true } as FabricMainAgentTarget;
  const residency = new ResidencyClient({ config, mesh, participants, mainAgent });
  cleanups.push(() => residency.close());
  vi.spyOn(residency, "removeActor").mockRejectedValue(new Error(`Resident host does not own ${actor.id}`));
  const lifecycle = new LifecycleBroker(mesh, identity, participants, { enabled: false, pollMs: 20, maxReadEvents: 100 }, async () => {});
  cleanups.push(() => lifecycle.close());
  const provider = new AgentsProvider(agents, actors, new GlobalActorRegistry(root, 64 * 1024), mainAgent, participants, undefined, lifecycle, () => false, residency, !options.sharedRuntime);
  const remove = () => provider.invoke("remove", { id: actor.id, successor: true }, context);
  return { root, actorRoot, actor, oldId, oldDir, main, host, actors, agents, meshConfig, provider, residency, mesh, remove, predecessor, identity, realOwner };
};

const runEvidence = (root: string, id: string, worker: Awaited<ReturnType<typeof child>>["identity"], runner = worker) => {
  const dir = path.join(root, id);
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, "status.json"), JSON.stringify({ id, status: "running", transport: "process" }));
  fs.writeFileSync(path.join(dir, "worker-processes.jsonl"), JSON.stringify({ worker }) + "\n" + JSON.stringify({ worker, runner }) + "\n");
  return dir;
};
// Portable authority fakes: only native-Main admission and predecessor host death are
// supplied by a test. Worker settlement still reads the actual injected platform semantics.
let fakePid = 2147483600;
const fakeChild = () => {
  const pid = fakePid++;
  const identity = { pid, startTime: "1", kernelId: "00000000-0000-0000-0000-000000000000/pid:[123]", commandLine: "fixture\0" };
  return { proc: { pid }, identity, owned: { pid, started: "1", at: 1, argv: [] }, exited: Promise.resolve() };
};
describe.skipIf(process.platform !== "linux")("dead predecessor durable removal (#2386)", () => {
  it("does not load successor safety machinery on provider import, construction or idle", async () => {
    await fixture();
    await new Promise((resolve) => setTimeout(resolve, 25));
    expect(successorLoads).not.toHaveBeenCalled();
  });
  it("records the named native Main PID/start time without publishing Main arguments", async () => {
    const f = await fixture();
    const directory = new ParticipantDirectory(f.mesh, { enabled: false, identity: f.identity, rootId: f.identity.id, hostId: f.identity.id });
    const record = directory.root({ id: f.identity.id, name: "Main", kind: "main", status: "idle", runner: "pi", transport: "host",
      sessionId: "successor", updatedAt: 1, pendingMessages: false, local: true }, "knowledge-lead");
    expect(record.agentName).toBe("knowledge-lead");
    expect(record.processIdentity).toEqual({ pid: process.pid, startTime: startTime(process.pid), kernelId: kernelId() });
    expect(record.processIdentity).not.toHaveProperty("commandLine");
    await directory.close();
  });
  it("keeps removal unaccepted and files intact while a dead host's detached worker lives, then removes once", async () => {
    const f = await fixture({ sessionRegistry: true });
    await dead(f.host);
    const worker = await child();
    runEvidence(path.join(f.oldDir, "runs"), "orphan-run", worker.identity, worker.identity);
    const write = vi.spyOn(ActorRegistryStore.prototype, "write");
    const signal = vi.spyOn(process, "kill");
    try {
      expect(await f.remove()).toMatchObject({ removed: false, pending: expect.stringContaining("settlement not proven") });
      expect(signal).not.toHaveBeenCalled();
      expect(new ActorRegistryStore(f.actorRoot).records()[0]?.rootId).toBe(f.oldId);
      expect(fs.existsSync(path.join(f.actorRoot, f.actor.id))).toBe(true);
      expect(write).not.toHaveBeenCalled();
      expect(f.mesh.get(`actor-removals/${f.actor.id}`)).toBeUndefined();
    } finally { signal.mockRestore(); }
    await dead(worker);
    await expect(Promise.all([f.remove(), f.remove()])).resolves.toEqual([{ removed: true }, { removed: true }]);
    expect(write.mock.calls.filter(([rows]) => !rows.some(row => row.id === f.actor.id))).toHaveLength(1);
    expect(fs.existsSync(path.join(f.actorRoot, f.actor.id))).toBe(false);
    expect(f.mesh.get(`actor-removals/${f.actor.id}`)?.updatedBy.id).toBe(f.identity.id);
    write.mockRestore();
  });
  it.each(["direct", "nested"])("scheduled retention preserves a live %s runner's evidence and rejects a false clean-close receipt until exit", async kind => {
    const f = await fixture();
    await dead(f.host);
    const runner = await child();
    const runs = path.join(f.oldDir, "runs");
    // Exercise the real registered retention callback against the predecessor's canonical
    // run tree. Disable only managed-temp housekeeping (resident trees have no owner marker)
    // and the unrelated detached global sweep; process settlement stays real.
    const mkdtemp = fs.mkdtempSync;
    const allocate = vi.spyOn(fs, "mkdtempSync").mockImplementation((prefix, options) =>
      String(prefix).endsWith("pi-fabric-runs-") ? runs : mkdtemp(prefix, options));
    const mark = vi.spyOn(retention, "markRunRootActive").mockImplementation(() => {});
    const heartbeat = vi.spyOn(retention, "heartbeatRunRoot").mockImplementation(() => {});
    const globalSweep = vi.spyOn(retention, "claimTempRunSweep").mockReturnValue(false);
    const interval = globalThis.setInterval;
    let scheduled: (() => void) | undefined;
    const timer = vi.spyOn(globalThis, "setInterval").mockImplementation((callback, ms, ...args) => {
      if (ms === 15 * 60 * 1_000 && !scheduled) scheduled = callback as () => void;
      return interval(callback, ms, ...args);
    });
    const inheritedRunRoot = process.env.PI_FABRIC_RUN_ROOT;
    delete process.env.PI_FABRIC_RUN_ROOT;
    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, retainRuns: true, sessionExport: false },
      { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), retention: { ...DEFAULT_FABRIC_CONFIG.retention, oneShotRunMs: 60_000 } });
    if (inheritedRunRoot !== undefined) process.env.PI_FABRIC_RUN_ROOT = inheritedRunRoot;
    cleanups.push(() => manager.close());
    allocate.mockRestore();
    const launch = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
      const dir = path.dirname(request.workerArguments[request.workerArguments.indexOf("--status-file") + 1]!);
      const attempt = request.workerArguments[request.workerArguments.indexOf("--launch-attempt") + 1]!;
      const parentRunner = kind === "direct" ? runner.identity : f.main.identity;
      runEvidence(runs, request.id, f.main.identity, parentRunner);
      fs.appendFileSync(path.join(dir, "worker-processes.jsonl"), JSON.stringify({ attempt, worker: f.main.identity, runner: parentRunner }) + "\n");
      if (kind === "nested") runEvidence(path.join(dir, "nested"), "nested-runner", f.main.identity, runner.identity);
      const status = { id: request.id, name: request.name,
        task: "worker exited with surviving runner", status: "completed", runner: "pi", transport: "process",
        cwd: request.cwd, startedAt: Date.now(), updatedAt: Date.now(), finishedAt: Date.now(), turns: 0, toolCalls: 0, text: "done", exitCode: 0,
        usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } };
      fs.writeFileSync(path.join(dir, "status.json"), JSON.stringify(status));
      if (kind === "nested") fs.writeFileSync(path.join(dir, "nested", "nested-runner", "status.json"), JSON.stringify({ ...status, id: "nested-runner" }));
      return { kind: "process", sessionId: String(f.main.identity.pid), isAlive: async () => false, stop: async () => {} };
    });
    const { runResidentHostFromConfigPath } = await import("../src/residency/host.js");
    const configPath = path.join(f.oldDir, "config.json");
    const config = JSON.parse(fs.readFileSync(configPath, "utf8")) as ResidentHostConfig;
    config.agents = { ...config.agents, budgetUsd: 0, retainRuns: true, sessionExport: false };
    fs.writeFileSync(configPath, JSON.stringify(config));
    const closeHost = () => runResidentHostFromConfigPath(configPath, AbortSignal.abort());
    // The scheduled callback queues its async sweep on setImmediate; drain it before observing.
    const sweep = async () => {
      expect(scheduled).toBeTypeOf("function");
      scheduled!();
      await new Promise(resolve => setTimeout(resolve, 100));
    };
    try {
      const result = await manager.run({ task: "worker exited with surviving runner", transport: "process", extensions: false });
      expect(result.status).toBe("completed");
      launch.mockRestore();
      const dir = path.join(runs, result.id);
      expect(manager.runDirectory(result.id)).toBe(dir);
      const evidenceDir = kind === "nested" ? path.join(dir, "nested", "nested-runner") : dir;
      const processJournal = fs.readFileSync(path.join(evidenceDir, "worker-processes.jsonl"), "utf8");
      const launchJournal = fs.readFileSync(path.join(dir, "worker-launches.jsonl"), "utf8");
      // Make this terminal run due only after its initial scheduled startup sweep drains.
      await new Promise(resolve => setTimeout(resolve, 100));
      const statusFile = path.join(dir, "status.json");
      const terminal = JSON.parse(fs.readFileSync(statusFile, "utf8"));
      fs.writeFileSync(statusFile, JSON.stringify({ ...terminal, finishedAt: 1, updatedAt: 1 }));
      await sweep();
      expect.soft(manager.runDirectory(result.id)).toBe(dir);
      expect.soft(fs.existsSync(path.join(evidenceDir, "worker-processes.jsonl"))).toBe(true);
      expect.soft(fs.existsSync(path.join(dir, "worker-launches.jsonl"))).toBe(true);
      if (fs.existsSync(dir)) {
        expect(fs.readFileSync(path.join(evidenceDir, "worker-processes.jsonl"), "utf8")).toBe(processJournal);
        expect(fs.readFileSync(path.join(dir, "worker-launches.jsonl"), "utf8")).toBe(launchJournal);
      }
      await closeHost(); // real host shutdown/receipt code, not a test-side settlement predicate
      expect.soft(fs.existsSync(path.join(f.oldDir, "workers-settled.json"))).toBe(false);
      const removal = await f.remove();
      expect.soft(removal).toMatchObject({ removed: false, pending: expect.stringContaining("settlement not proven") });
      expect.soft(f.mesh.get(`actor-removals/${f.actor.id}`)).toBeUndefined();
      expect(processIdentity.processStartIdentityState(runner.identity)).toBe("alive");
      if ((removal as { removed?: boolean }).removed) return; // Baseline failure already proves unsafe acceptance; cleanup still runs.
      await dead(runner);
      await sweep();
      await vi.waitFor(() => expect(fs.existsSync(dir)).toBe(false));
      await vi.waitFor(() => expect(manager.runDirectory(result.id)).toBeUndefined());
      // Successor preflight permanently retired this root; it must not restart to
      // produce a receipt. The safely emptied evidence tree now authorizes retry.
      expect(fs.existsSync(path.join(f.oldDir, "workers-settled.json"))).toBe(false);
      await expect(f.remove()).resolves.toEqual({ removed: true });
    } finally {
      launch.mockRestore();
      await dead(runner);
      await manager.close();
      timer.mockRestore();
      globalSweep.mockRestore();
      heartbeat.mockRestore();
      mark.mockRestore();
    }
  }, 30_000);
  it.each(["worker", "runner"])("keeps removal pending after a replacement %s spawn crashes before process registration", async kind => {
    const f = await fixture();
    await dead(f.host);
    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, retainRuns: true, sessionExport: false },
      { runRoot: path.join(f.oldDir, "runs"), workerPath: path.resolve("tests/fixtures/fake-worker.mjs") });
    cleanups.push(() => manager.close());
    let spawned: Awaited<ReturnType<typeof child>> | undefined;
    let dir = "";
    let attempt = "";
    const launch = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
      dir = path.dirname(request.workerArguments[request.workerArguments.indexOf("--status-file") + 1]!);
      attempt = request.workerArguments[request.workerArguments.indexOf("--launch-attempt") + 1]!;
      // This is the adapter's entry, BEFORE its actual spawn. The manager intent is durable.
      expect(JSON.parse(fs.readFileSync(path.join(dir, "worker-launches.jsonl"), "utf8").trim())).toEqual({ attempt });
      runEvidence(path.dirname(dir), path.basename(dir), f.main.identity); // complete earlier attempt
      if (kind === "runner") fs.appendFileSync(path.join(dir, "worker-processes.jsonl"),
        JSON.stringify({ attempt, worker: f.main.identity }) + "\n"); // new worker registered; runner not yet
      spawned = await child();
      // Crash point: runnable replacement exists, but its post-spawn identity was never appended.
      fs.writeFileSync(path.join(dir, "status.json"), JSON.stringify({ id: request.id, actorId: f.actor.id,
        name: request.name, task: "replacement crash injection", status: "running", runner: "pi", transport: "process",
        cwd: request.cwd, startedAt: Date.now(), updatedAt: Date.now(), turns: 0, toolCalls: 0, text: "", exitCode: null,
        usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } }));
      return { kind: "process", sessionId: String(spawned.identity.pid),
        isAlive: async () => processIdentity.processStartIdentityState(spawned!.identity) === "alive",
        stop: async () => dead(spawned!) };
    });
    try {
      await manager.spawn({ task: "replacement crash injection", transport: "process", extensions: false });
    } finally { launch.mockRestore(); }
    const write = vi.spyOn(ActorRegistryStore.prototype, "write");
    const signal = vi.spyOn(process, "kill");
    try {
      expect(await f.remove()).toMatchObject({ removed: false, pending: expect.stringContaining(
        kind === "worker" ? "Unregistered worker launch attempt" : "Missing runner launch evidence") });
      expect(write).not.toHaveBeenCalled();
      expect(signal).not.toHaveBeenCalled();
      expect(fs.existsSync(path.join(f.actorRoot, f.actor.id))).toBe(true);
      expect(fs.existsSync(path.join(dir, "worker-launches.jsonl"))).toBe(true);
      expect(new ActorRegistryStore(f.actorRoot).records()[0]?.rootId).toBe(f.oldId);
      expect(f.mesh.get(`actor-removals/${f.actor.id}`)).toBeUndefined();
      expect(processIdentity.processStartIdentityState(spawned!.identity)).toBe("alive");
    } finally { write.mockRestore(); signal.mockRestore(); }
    await manager.close(); // stop only the owned child; missing launch evidence still fails closed
    expect(await f.remove()).toMatchObject({ removed: false, pending: expect.stringContaining("Removal unaccepted") });
  });
  it("keeps removal pending when a live runner rewrites its process title until that runner exits", async () => {
    const f = await fixture();
    await dead(f.host);
    const runner = await child("process.on('message', () => { process.title = 'rewritten-runner'; process.send('rewritten'); }); setInterval(() => {}, 1000)");
    runEvidence(path.join(f.oldDir, "runs"), "title-run", f.main.identity, runner.identity);
    const rewritten = new Promise<void>(resolve => runner.proc.once("message", () => resolve()));
    runner.proc.send("rewrite");
    await rewritten;
    expect(startTime(runner.identity.pid)).toBe(runner.identity.startTime);
    expect(fs.readFileSync(`/proc/${runner.identity.pid}/cmdline`, "utf8")).not.toBe(runner.identity.commandLine);
    expect(processIdentity.processIdentityState(runner.identity)).toBe("mismatch"); // still unsafe to signal
    expect(processIdentity.processStartIdentityState(runner.identity)).toBe("alive");
    const write = vi.spyOn(ActorRegistryStore.prototype, "write");
    const signal = vi.spyOn(process, "kill");
    try {
      expect(await f.remove()).toMatchObject({ removed: false, pending: expect.stringContaining("is alive; settlement not proven") });
      expect(write).not.toHaveBeenCalled();
      expect(signal).not.toHaveBeenCalled();
      expect(new ActorRegistryStore(f.actorRoot).records()[0]?.rootId).toBe(f.oldId);
      expect(fs.existsSync(path.join(f.actorRoot, f.actor.id))).toBe(true);
      expect(fs.existsSync(path.join(f.oldDir, "runs", "title-run", "worker-processes.jsonl"))).toBe(true);
      expect(f.mesh.get(`actor-removals/${f.actor.id}`)).toBeUndefined();
    } finally { write.mockRestore(); signal.mockRestore(); }
    await dead(runner);
    await expect(f.remove()).resolves.toEqual({ removed: true });
    expect(fs.existsSync(path.join(f.actorRoot, f.actor.id))).toBe(false);
  });
  it("checks retained actor runs and recursively nested runners even when their parent process is dead", async () => {
    const f = await fixture();
    await dead(f.host);
    const worker = await child();
    const parent = runEvidence(path.join(f.actorRoot, f.actor.id, "runs"), "retained-parent", f.main.identity);
    runEvidence(path.join(parent, "nested"), "nested-worker", worker.identity);
    expect(await f.remove()).toMatchObject({ removed: false, pending: expect.stringContaining("nested-worker") });
    expect(fs.existsSync(path.join(f.actorRoot, f.actor.id))).toBe(true);
    await dead(worker);
    await expect(f.remove()).resolves.toEqual({ removed: true });
  });
  it.each(["absent runs", "missing journal", "unresolved worker", "missing referenced run", "unknown kernel", "incomplete attempt"])("keeps %s settlement evidence pending without accepting cleanup", async kind => {
    const f = await fixture();
    await dead(f.host);
    const runs = path.join(f.oldDir, "runs");
    const dir = runEvidence(runs, "unknown-run", f.main.identity);
    if (kind === "absent runs") fs.rmSync(runs, { recursive: true });
    if (kind === "missing journal") fs.rmSync(path.join(dir, "worker-processes.jsonl"));
    if (kind === "unresolved worker") fs.writeFileSync(path.join(dir, "unresolved-worker.json"), JSON.stringify({ runId: "unknown-run" }));
    if (kind === "missing referenced run") {
      const registry = new ActorRegistryStore(f.actorRoot);
      await registry.withLock(() => registry.write(registry.records().map(row => ({ ...row, lastRunId: "missing-run" }))));
    }
    if (kind === "unknown kernel") runEvidence(runs, "unknown-run", { ...f.main.identity, kernelId: "00000000-0000-0000-0000-000000000000/pid:[123]" });
    if (kind === "incomplete attempt") fs.appendFileSync(path.join(dir, "worker-processes.jsonl"), JSON.stringify({ worker: { ...f.main.identity, startTime: "1" } }) + "\n");
    expect(await f.remove()).toMatchObject({ removed: false, pending: expect.stringContaining("Removal unaccepted") });
    expect(new ActorRegistryStore(f.actorRoot).records()[0]?.rootId).toBe(f.oldId);
    expect(fs.existsSync(path.join(f.actorRoot, f.actor.id))).toBe(true);
    expect(f.mesh.get(`actor-removals/${f.actor.id}`)).toBeUndefined();
  });
  it("allows a recorded worker's PID reuse only after its kernel identity no longer matches, without signalling it", async () => {
    const f = await fixture();
    await dead(f.host);
    const unrelated = await child();
    runEvidence(path.join(f.oldDir, "runs"), "old-worker", { ...unrelated.identity, startTime: "1" });
    const signal = vi.spyOn(process, "kill");
    try { await expect(f.remove()).resolves.toEqual({ removed: true }); expect(signal).not.toHaveBeenCalled(); }
    finally { signal.mockRestore(); }
    expect(startTime(unrelated.identity.pid)).toBe(unrelated.identity.startTime);
  });
  it.each(["PID reuse", "command line", "lock token", "unknown process", "resumed Main"])("rechecks %s during durable marker I/O: refuses without signal and removes marker", async kind => {
    const f = await fixture();
    let markerWritten = false;
    const rename = fs.renameSync;
    const read = fs.readFileSync;
    const renameSpy = vi.spyOn(fs, "renameSync").mockImplementation((source, target) => {
      rename(source, target);
      if (String(target) !== path.join(f.oldDir, "retired.json")) return;
      markerWritten = true;
      if (kind === "lock token") for (const name of ["owner.json", "host.lock"]) {
        const file = path.join(f.oldDir, name);
        const record = JSON.parse(read(file, "utf8"));
        record.token = "resumed-owner-token";
        fs.writeFileSync(file, JSON.stringify(record));
      }
      if (kind === "resumed Main") {
        const file = path.join(f.oldDir, "config.json");
        const config = JSON.parse(read(file, "utf8"));
        config.rootOwner.processIdentity = f.residency.options.participants.self()!.processIdentity;
        fs.writeFileSync(file, JSON.stringify(config));
      }
    });
    const readSpy = vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...args: unknown[]) => {
      if (markerWritten && String(file) === `/proc/${f.host.identity.pid}/stat`) {
        if (kind === "unknown process") throw Object.assign(new Error("unreadable process"), { code: "EACCES" });
        if (kind === "PID reuse") {
          const stat = String(read(file, "utf8"));
          const end = stat.lastIndexOf(")") + 2;
          const fields = stat.slice(end).split(" ");
          fields[19] = String(Number(f.host.identity.startTime) + 1);
          return stat.slice(0, end) + fields.join(" ");
        }
      }
      if (markerWritten && kind === "command line" && String(file) === `/proc/${f.host.identity.pid}/cmdline`) return "reused-process\0";
      return (read as (...args: unknown[]) => unknown)(file, ...args);
    }) as typeof fs.readFileSync);
    const signal = vi.spyOn(process, "kill");
    try {
      await expect(f.remove()).rejects.toThrow(kind === "resumed Main" ? "owned by a live root" : "process identity mismatch");
      expect(markerWritten).toBe(true);
      expect(signal).not.toHaveBeenCalled();
      expect(fs.existsSync(path.join(f.oldDir, "retired.json"))).toBe(false);
      expect(new ActorRegistryStore(f.actorRoot).records()[0]?.rootId).toBe(f.oldId);
    } finally { signal.mockRestore(); readSpy.mockRestore(); renameSpy.mockRestore(); }
    expect(startTime(f.host.identity.pid)).toBe(f.host.identity.startTime);
  });
  it("a released diagnostic never authorizes successor cleanup while the kernel fence is held", async () => {
    const f = await fixture();
    await dead(f.host);
    fs.rmSync(path.join(f.oldDir, "owner.json"));
    fs.writeFileSync(path.join(f.oldDir, "host.lock"), JSON.stringify({ released: true }));
    const fd = await lockFile(path.join(f.oldDir, "host.lock"), 0, true);
    const signal = vi.spyOn(process, "kill");
    try {
      await expect(f.remove()).rejects.toThrow("timed out");
      expect(signal).not.toHaveBeenCalled();
      expect(new ActorRegistryStore(f.actorRoot).records()[0]?.rootId).toBe(f.oldId);
      expect(f.mesh.get(`actor-removals/${f.actor.id}`)).toBeUndefined();
    } finally { signal.mockRestore(); fs.closeSync(fd); }
    await expect(f.remove()).resolves.toEqual({ removed: true });
  });
  it("removes after Main rotation, stops the verified host, and records the successor", async () => {
    const f = await fixture();
    await expect(f.remove()).resolves.toEqual({ removed: true });
    await f.host.exited;
    expect(new ActorRegistryStore(f.actorRoot).records()).toEqual([]);
    expect(fs.existsSync(path.join(f.actorRoot, f.actor.id))).toBe(false);
    expect(f.residency.removeActor).not.toHaveBeenCalled();
    expect(successorLoads).toHaveBeenCalledTimes(1);
    const receipt = f.mesh.get(`actor-removals/${f.actor.id}`);
    expect(receipt?.updatedBy.id).toBe(f.identity.id);
    expect(receipt?.value).toMatchObject({ successor: { fromRootId: f.oldId, by: { id: f.identity.id } } });
  });
  it("allows native Main successor removal when FabricRuntimeState owns lifecycle cleanup", async () => {
    const f = await fixture({ sharedRuntime: true });
    expect(f.provider.ownsRuntime).toBe(false);
    await expect(f.remove()).resolves.toEqual({ removed: true });
    expect(new ActorRegistryStore(f.actorRoot).records()).toEqual([]);
  });
  it("still refuses a remote caller when FabricRuntimeState owns lifecycle cleanup", async () => {
    const f = await fixture({ sharedRuntime: true });
    Object.assign(f.provider.mainAgent, { local: false });
    await expect(f.remove()).rejects.toThrow("requires the native Main root");
    expect(startTime(f.host.proc.pid!)).toBe(f.host.identity.startTime);
    expect(fs.existsSync(path.join(f.oldDir, "retired.json"))).toBe(false);
  });
  it.each(["dead", "missing"])("refuses a stale launcher with %s Main identity after retiring a real compiled host", { timeout: 60_000 }, async (identity) => {
    const f = await fixture({ realHost: true });
    expect(f.realOwner?.processIdentity?.startTime).toBeTruthy();
    await expect(f.remove()).resolves.toEqual({ removed: true });
    // /proc retains a zombie's start time until the recorded launcher reaps it; it is already
    // provably not running, but wait for reaping too so this test leaves no process behind.
    const reapDeadline = Date.now() + 5_000;
    while (startTime(f.realOwner!.pid) === f.realOwner!.processIdentity!.startTime && Date.now() < reapDeadline) {
      await new Promise((resolve) => setTimeout(resolve, 25));
    }
    expect(startTime(f.realOwner!.pid)).not.toBe(f.realOwner!.processIdentity!.startTime);
    expect(fs.existsSync(path.join(f.oldDir, "retired.json"))).toBe(true);
    const lockInode = fs.statSync(path.join(f.oldDir, "host.lock")).ino;
    const oldConfig = JSON.parse(fs.readFileSync(path.join(f.oldDir, "config.json"), "utf8")) as ResidentHostConfig;
    const retry = new ResidencyClient({ config: oldConfig, mesh: f.mesh, participants: f.residency.options.participants,
      mainAgent: { id: f.oldId, local: true } as FabricMainAgentTarget, hostPath: path.resolve("dist/residency/launcher.js") });
    if (identity === "missing") delete oldConfig.rootOwner!.processIdentity;
    try {
      await expect(retry.ensureHost()).rejects.toThrow("retired by a successor Main");
      expect(fs.statSync(path.join(f.oldDir, "host.lock")).ino).toBe(lockInode);
      expect(JSON.parse(fs.readFileSync(path.join(f.oldDir, "host.lock"), "utf8"))).toMatchObject({ released: true });
    } finally { await retry.close(); }
    expect(new ActorRegistryStore(f.actorRoot).records()).toEqual([]);
  });
  it("allows ensureHost from a new live Main of the retired root and runs its durable actor", { timeout: 60_000 }, async () => {
    const f = await fixture({ realHost: true });
    await expect(f.remove()).resolves.toEqual({ removed: true });
    const oldConfig = JSON.parse(fs.readFileSync(path.join(f.oldDir, "config.json"), "utf8")) as ResidentHostConfig;
    const resumed = { ...f.predecessor, local: true, stale: false,
      processIdentity: f.residency.options.participants.self()!.processIdentity! };
    const identity: MeshIdentity = { id: f.oldId, name: "main", kind: "main", sessionId: "predecessor" };
    const participants = new ParticipantDirectory(f.mesh, { enabled: true, identity, rootId: f.oldId, hostId: f.oldId, heartbeatMs: 50, leaseMs: 300 });
    participants.registerSource(() => [resumed]);
    cleanups.push(() => participants.close());
    await participants.start();
    oldConfig.piModels = { available: [{ provider: "provider", id: "visible" }], aliases: {}, defaultModel: "provider/visible" };
    const client = new ResidencyClient({ config: oldConfig, mesh: f.mesh, participants,
      mainAgent: { id: f.oldId, local: true } as FabricMainAgentTarget, hostPath: path.resolve("dist/residency/launcher.js") });
    cleanups.push(() => client.close());
    const owner = await client.ensureHost();
    const owned: Owned = { pid: owner.pid, started: owner.processIdentity!.startTime, at: Date.now(), argv: [] };
    cleanups.push(async () => { if (await stopOwned(owned, 20_000, 5_000)) throw new Error(`Resumed fixture ${owned.pid} did not stop`); });
    expect(oldConfig.rootOwner?.processIdentity).toEqual(resumed.processIdentity);
    expect(JSON.parse(fs.readFileSync(path.join(f.oldDir, "config.json"), "utf8")).rootOwner.processIdentity).toEqual(resumed.processIdentity);
    expect(fs.existsSync(path.join(f.oldDir, "retired.json"))).toBe(true);
    const actor = await client.createActor({ name: "resumed durable", instructions: "Reply.", residency: "durable",
      responseMode: "text", topics: ["successor.resumed"] });
    await f.mesh.publish({ topic: "successor.resumed", from: identity, text: "run after resume" });
    const store = new ActorRegistryStore(f.actorRoot);
    const deadline = Date.now() + 15_000;
    const reply = () => (store.records().find((record) => record.id === actor.id)?.messages as FabricActorMessage[] | undefined)
      ?.find((message) => message.direction === "out" && !message.error);
    while (!reply() && Date.now() < deadline) await new Promise((resolve) => setTimeout(resolve, 25));
    expect(reply()?.text, JSON.stringify(store.records())).toBe("fake worker complete");
    await expect(client.removeActor(actor.id)).resolves.toEqual({ removed: true });
  });
  it("refuses removal without retiring when Main resumes between the two pre-signal liveness checks", async () => {
    const f = await fixture();
    const list = vi.spyOn(f.residency.options.participants, "list").mockImplementationOnce(() => {
      const file = path.join(f.oldDir, "config.json");
      const config = JSON.parse(fs.readFileSync(file, "utf8"));
      config.rootOwner.processIdentity = f.residency.options.participants.self()!.processIdentity;
      fs.writeFileSync(file, JSON.stringify(config));
      return [];
    });
    try { await expect(f.remove()).rejects.toThrow("owned by a live root"); }
    finally { list.mockRestore(); }
    expect(fs.existsSync(path.join(f.oldDir, "retired.json"))).toBe(false);
    expect(startTime(f.host.proc.pid!)).toBe(f.host.identity.startTime);
    expect(new ActorRegistryStore(f.actorRoot).records()[0]?.rootId).toBe(f.oldId);
  });
  it("removes a durable actor in the dead predecessor's session registry, not loaded by the new Main", { timeout: 60_000 }, async () => {
    const f = await fixture({ sessionRegistry: true, realHost: true });
    expect(f.actors.list().some((actor) => actor.id === f.actor.id)).toBe(false);
    await expect(f.remove()).resolves.toEqual({ removed: true });
    expect(new ActorRegistryStore(f.actorRoot).records()).toEqual([]);
    expect(f.mesh.get(`actor-removals/${f.actor.id}`)?.updatedBy.id).toBe(f.identity.id);
  });
  it("does not reap the caller's unrelated session actor presence while reading an old registry", async () => {
    const f = await fixture({ sessionRegistry: true });
    const other = await f.actors.create({ name: "unrelated session actor", instructions: "Stay idle.", scope: "session", residency: "session" });
    const key = `actors/successor/${other.id}`;
    expect(f.mesh.get(key)?.value).toMatchObject({ scope: "session", id: other.id });
    await expect(f.remove()).resolves.toEqual({ removed: true });
    expect(f.mesh.get(key)?.value).toMatchObject({ scope: "session", id: other.id });
  });
  it("retries a cleanup in a foreign session registry without accepting removal twice", async () => {
    const f = await fixture({ sessionRegistry: true });
    const deletion = vi.spyOn(f.mesh, "delete").mockRejectedValue(new Error("retry session cleanup"));
    expect(await f.remove()).toMatchObject({ removed: true, cleaned: false });
    deletion.mockRestore();
    await expect(f.remove()).resolves.toEqual({ removed: true });
    expect(fs.existsSync(path.join(f.actorRoot, `removal-${f.actor.id}.json`))).toBe(false);
  });
  it("retains cleanup obligations on failure and finishes them after a Main reload", async () => {
    const f = await fixture();
    const deletion = vi.spyOn(f.mesh, "delete").mockRejectedValue(new Error("injected cleanup failure"));
    const result = await f.remove();
    expect(result).toMatchObject({ removed: true, cleaned: false });
    const markerPath = path.join(f.actorRoot, `removal-${f.actor.id}.json`);
    const marker = JSON.parse(fs.readFileSync(markerPath, "utf8"));
    expect(marker.owner).toMatchObject({ rootId: f.identity.id, successor: { by: { id: f.identity.id } } });
    expect(new ActorRegistryStore(f.actorRoot).records()).toEqual([]);
    deletion.mockRestore();
    await f.actors.close();
    const reloaded = new ActorManager("successor", f.identity, f.mesh, f.meshConfig, f.agents, () => {},
      { persistent: true, actorRoot: f.actorRoot, rootId: f.identity.id, claimResidency: "session", project: f.predecessor.project, canManageActor: () => undefined });
    cleanups.push(() => reloaded.close());
    expect(reloaded.owns(f.actor.id)).toBe(true);
    await reloaded.finishPendingRemovals();
    expect(fs.existsSync(markerPath)).toBe(false);
    expect(fs.existsSync(path.join(f.actorRoot, f.actor.id))).toBe(false);
    expect(f.mesh.get(`actor-removals/${f.actor.id}`)?.updatedBy.id).toBe(f.identity.id);
  });
  it("keeps all presence cleanup debts when an accepted removal rotates again", async () => {
    const f = await fixture();
    const oldestPresence = `actors/predecessor/${f.actor.id}`;
    const firstSuccessorPresence = `actors/successor/${f.actor.id}`;
    await f.mesh.put({ key: oldestPresence, identity: f.identity, value: { id: f.actor.id } });
    const deletion = vi.spyOn(f.mesh, "delete").mockRejectedValue(new Error("defer cleanup until next rotation"));
    expect(await f.remove()).toMatchObject({ removed: true, cleaned: false });
    deletion.mockRestore();
    await f.actors.close();
    const nextIdentity: MeshIdentity = { id: "session:third-main", name: "main", kind: "main", sessionId: "third-main" };
    const next = new ActorManager("third-main", nextIdentity, f.mesh, f.meshConfig, f.agents, () => {},
      { persistent: true, actorRoot: f.actorRoot, rootId: nextIdentity.id, claimResidency: "session", project: f.predecessor.project, canManageActor: () => undefined });
    cleanups.push(() => next.close());
    // Unit-probe the host-only transaction after preflight; public liveness refusals are tested separately.
    await expect(next.removeSuccessor(f.actor.id, f.identity.id, { presenceKey: firstSuccessorPresence, assertSafe() {} })).resolves.toEqual({ removed: true });
    expect(f.mesh.get(oldestPresence)).toBeUndefined();
    expect(f.mesh.get(firstSuccessorPresence)).toBeUndefined();
    expect(f.mesh.get(`actor-removals/${f.actor.id}`)?.updatedBy.id).toBe(nextIdentity.id);
  });
  it("takes over an already revoked predecessor cleanup obligation", async () => {
    const f = await fixture();
    const markerPath = path.join(f.actorRoot, `removal-${f.actor.id}.json`);
    fs.writeFileSync(markerPath, JSON.stringify({ id: f.actor.id, sessionDir: path.join(f.actorRoot, f.actor.id),
      presenceKey: `actors/predecessor/${f.actor.id}`, owner: { name: f.actor.name, rootId: f.oldId,
        project: f.predecessor.project, residency: "durable", requestedAt: 42 } }));
    const registry = new ActorRegistryStore(f.actorRoot);
    await registry.withLock(() => registry.write([], { durable: true }));
    await expect(f.remove()).resolves.toEqual({ removed: true });
    expect(fs.existsSync(markerPath)).toBe(false);
    expect(f.mesh.get(`actor-removals/${f.actor.id}`)?.value).toMatchObject({ requestedAt: 42, successor: { by: { id: f.identity.id } } });
  });
  it("preserves an accepted predecessor removal decision rather than running its queued work", async () => {
    const f = await fixture();
    const registry = new ActorRegistryStore(f.actorRoot);
    await registry.withLock(() => registry.write(registry.records().map((row) => ({ ...row,
      removal: { requestedAt: 42, runId: "predecessor-run", runStartedAt: 30 } })), { durable: true }));
    runEvidence(path.join(f.oldDir, "runs"), "predecessor-run", f.main.identity, f.main.identity);
    const spawn = vi.spyOn(f.agents, "spawn");
    await expect(f.remove()).resolves.toEqual({ removed: true });
    expect(spawn).not.toHaveBeenCalled();
    expect(f.mesh.get(`actor-removals/${f.actor.id}`)?.value).toMatchObject({ requestedAt: 42 });
    spawn.mockRestore();
  });
  it("rechecks root death after waiting for the registry lock", async () => {
    const f = await fixture();
    const original = ActorRegistryStore.prototype.withLock;
    const lock = vi.spyOn(ActorRegistryStore.prototype, "withLock").mockImplementationOnce(function (this: ActorRegistryStore, operation) {
      f.residency.options.participants.get = (id) => id === f.oldId ? { ...f.predecessor, stale: false } : f.residency.options.participants.self();
      return original.call(this, operation);
    });
    await expect(f.remove()).rejects.toThrow("owned by a live root");
    lock.mockRestore();
    expect(new ActorRegistryStore(f.actorRoot).records()[0]?.rootId).toBe(f.oldId);
  });
  it("rechecks recorded Main identity before acceptance even before a restarted Main publishes its lease", async () => {
    const f = await fixture();
    const original = ActorRegistryStore.prototype.withLock;
    const lock = vi.spyOn(ActorRegistryStore.prototype, "withLock").mockImplementationOnce(function (this: ActorRegistryStore, operation) {
      const file = path.join(f.oldDir, "config.json");
      const record = JSON.parse(fs.readFileSync(file, "utf8"));
      record.rootOwner.processIdentity = { pid: process.pid, startTime: startTime(process.pid), kernelId: kernelId() };
      fs.writeFileSync(file, JSON.stringify(record));
      return original.call(this, operation);
    });
    await expect(f.remove()).rejects.toThrow("owned by a live root");
    lock.mockRestore();
    expect(new ActorRegistryStore(f.actorRoot).records()[0]?.rootId).toBe(f.oldId);
  });
  it("shares concurrent successor claims and revokes once", async () => {
    const f = await fixture();
    const write = vi.spyOn(ActorRegistryStore.prototype, "write");
    await expect(Promise.all([f.remove(), f.remove()])).resolves.toEqual([{ removed: true }, { removed: true }]);
    expect(write.mock.calls.filter(([rows]) => !rows.some((row) => row.id === f.actor.id))).toHaveLength(1);
    write.mockRestore();
  });
  it("refuses a command-line mismatch even with a matching PID and start time", async () => {
    const f = await fixture();
    for (const name of ["owner.json", "host.lock"]) {
      const file = path.join(f.oldDir, name);
      const record = JSON.parse(fs.readFileSync(file, "utf8"));
      record.processIdentity.commandLine = "different recorded command";
      fs.writeFileSync(file, JSON.stringify(record));
    }
    await expect(f.remove()).rejects.toThrow("process identity mismatch");
    expect(startTime(f.host.proc.pid!)).toBe(f.host.identity.startTime);
    expect(fs.existsSync(path.join(f.oldDir, "retired.json"))).toBe(false);
  });
  it("fails closed on a malformed lease instead of treating it as absent", async () => {
    const f = await fixture();
    fs.mkdirSync(path.join(f.mesh.root, "host-leases"), { recursive: true });
    fs.writeFileSync(path.join(f.mesh.root, "host-leases", "unknown.json"), "{broken");
    await expect(f.remove()).rejects.toThrow("Cannot verify recorded ownership");
    expect(startTime(f.host.proc.pid!)).toBe(f.host.identity.startTime);
  });
  it("refuses an old root with a live participant", async () => {
    const f = await fixture({ liveMain: true });
    await expect(f.remove()).rejects.toThrow("owned by a live root");
    expect(startTime(f.host.proc.pid!)).toBe(f.host.identity.startTime);
  });
  it("refuses an expired/absent lease when the old Main process still runs", async () => {
    const f = await fixture({ liveMain: true });
    f.residency.options.participants.get = (id) => id === f.identity.id ? f.residency.options.participants.self() : undefined;
    f.residency.options.participants.list = () => [f.residency.options.participants.self()];
    await expect(f.remove()).rejects.toThrow("owned by a live root");
    expect(startTime(f.host.proc.pid!)).toBe(f.host.identity.startTime);
  });
  it("refuses a live Main lease even when the old Main process is dead", async () => {
    const f = await fixture();
    writeHostLease(f.mesh.root, { id: f.oldId, rootId: f.oldId, identityId: f.oldId, updatedAt: Date.now(), expiresAt: Date.now() + 60_000 });
    await expect(f.remove()).rejects.toThrow("owned by a live root");
    expect(startTime(f.host.proc.pid!)).toBe(f.host.identity.startTime);
  });
  it("refuses a different project without signalling its host", async () => {
    const f = await fixture({ project: os.tmpdir() });
    await expect(f.remove()).rejects.toThrow("same project");
    expect(startTime(f.host.proc.pid!)).toBe(f.host.identity.startTime);
  });
  it.each([{ agentName: "other-lead" }, { role: "worktree-agent" }])("refuses a different agent/role: %j", async (options) => {
    const f = await fixture(options);
    await expect(f.remove()).rejects.toThrow("same agent and role");
    expect(startTime(f.host.proc.pid!)).toBe(f.host.identity.startTime);
  });
  it("refuses process evidence from another kernel or PID namespace", async () => {
    const f = await fixture();
    const file = path.join(f.oldDir, "config.json");
    const record = JSON.parse(fs.readFileSync(file, "utf8"));
    record.rootOwner.processIdentity.kernelId = "00000000-0000-0000-0000-000000000000/pid:[123]";
    fs.writeFileSync(file, JSON.stringify(record));
    await expect(f.remove()).rejects.toThrow("Cannot prove predecessor Main process is dead");
    expect(startTime(f.host.proc.pid!)).toBe(f.host.identity.startTime);
  });
  it("never signals a recycled/mismatched host PID", async () => {
    const f = await fixture({ mismatchHost: true });
    await expect(f.remove()).rejects.toThrow("process identity mismatch");
    expect(startTime(f.host.proc.pid!)).toBe(f.host.identity.startTime);
    expect(new ActorRegistryStore(f.actorRoot).records()[0]?.rootId).toBe(f.oldId);
  });
  it("refuses legacy roots without a recorded Main process identity", async () => {
    const f = await fixture({ noIdentity: true });
    await expect(f.remove()).rejects.toThrow("recorded Main process identity");
    expect(startTime(f.host.proc.pid!)).toBe(f.host.identity.startTime);
  });
});

describe.each(["win32", "darwin"] as const)("%s successor unknown worker evidence", platformName => {
  it.each(["null journal", "unknown kernel"])("keeps %s removal pending without accepting or deleting", async kind => {
    const nativePlatform = process.platform;
    // Model worker kernel support only; durable writes must see the native host.
    const kernelState = processIdentity.processStartIdentityState;
    const f = await fixture({ fakeProcesses: true });
    // Authority is already faked for this worker-evidence unit case. Supply only
    // a disposable fd fence: the real POSIX lock needs getuid/flock, unavailable
    // on Windows. Keep native durable writes and worker-settlement checks real.
    const fence = vi.spyOn(residencyFileLock, "lockFile").mockImplementation(async file =>
      fs.openSync(file, fs.constants.O_RDWR | fs.constants.O_CREAT, 0o600));
    const caller = f.residency.options.participants.self()!.processIdentity!;
    const current = vi.spyOn(processIdentity, "readProcessStartIdentity").mockReturnValue(caller);
    const mainState = vi.spyOn(processIdentity, "processStartIdentityState").mockImplementation(expected =>
      expected.pid === f.main.identity.pid ? "dead" : kernelState(expected, platformName));
    const hostState = vi.spyOn(processIdentity, "processIdentityState").mockImplementation(expected =>
      expected.pid === f.host.identity.pid ? "dead" : "unknown");
    const worker = fakeChild().identity;
    // On Linux, use the real kernel so absence would otherwise prove death:
    // only the injected unsupported platform may make this worker unknown.
    if (nativePlatform === "linux") worker.kernelId = kernelId();
    const dir = runEvidence(path.join(f.oldDir, "runs"), "unknown-run", worker);
    if (kind === "null journal") fs.writeFileSync(path.join(dir, "worker-processes.jsonl"), '{"worker":null}\n{"worker":null,"runner":null}\n');
    const write = vi.spyOn(ActorRegistryStore.prototype, "write");
    const signal = vi.spyOn(process, "kill");
    const fsync = fs.fsyncSync;
    let directorySyncs = 0;
    let fileSyncs = 0;
    // Reproduce Windows' directory-fsync EPERM on any host if a platform
    // override escapes the process-identity seam into the real filesystem.
    const sync = vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      if (fs.fstatSync(fd).isDirectory()) {
        directorySyncs++;
        if (process.platform !== nativePlatform) {
          throw Object.assign(new Error("EPERM: spoofed platform reached directory fsync"), { code: "EPERM" });
        }
      } else fileSyncs++;
      fsync(fd);
    });
    try {
      expect(await f.remove()).toMatchObject({ removed: false, pending: expect.stringContaining(
        kind === "null journal" ? "Missing worker/runner process identity" : "is unknown; settlement not proven") });
      expect(fence).toHaveBeenCalledExactlyOnceWith(path.join(f.oldDir, "host.lock"), 0, true);
      expect(process.platform).toBe(nativePlatform);
      expect(fileSyncs).toBeGreaterThan(0);
      if (nativePlatform === "win32") expect(directorySyncs).toBe(0);
      else expect(directorySyncs).toBeGreaterThan(0);
      expect(write).not.toHaveBeenCalled();
      expect(signal).not.toHaveBeenCalled();
      expect(new ActorRegistryStore(f.actorRoot).records()[0]?.rootId).toBe(f.oldId);
      expect(fs.existsSync(path.join(f.actorRoot, f.actor.id))).toBe(true);
      expect(fs.existsSync(dir)).toBe(true);
      expect(f.mesh.get(`actor-removals/${f.actor.id}`)).toBeUndefined();
    } finally {
      sync.mockRestore();
      signal.mockRestore(); write.mockRestore(); current.mockRestore(); mainState.mockRestore(); hostState.mockRestore(); fence.mockRestore();
    }
  });
});
