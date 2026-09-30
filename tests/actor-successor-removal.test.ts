import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { ActorDirectory } from "../src/actors/directory.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { LifecycleBroker } from "../src/lifecycle/broker.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import { ResidencyClient } from "../src/residency/client.js";
import { residentHostId, residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { writeHostLease } from "../src/topology/host-leases.js";
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
const child = async () => {
  const proc = spawn(process.execPath, ["-e", "setInterval(() => {}, 1000)"], { stdio: "ignore" });
  await new Promise<void>((resolve, reject) => { proc.once("spawn", resolve); proc.once("error", reject); });
  const pid = proc.pid!;
  const owned: Owned = { pid, started: startTime(pid), at: Date.now(), argv: [] };
  const identity = { pid, startTime: owned.started, kernelId: kernelId(), commandLine: fs.readFileSync(`/proc/${pid}/cmdline`, "utf8") };
  const exited = new Promise<void>((resolve) => proc.once("exit", () => resolve()));
  cleanups.push(async () => { if (await stopOwned(owned, 1_000, 1_000)) throw new Error(`Fixture process ${pid} did not stop`); await exited; });
  return { proc, owned, identity, exited };
};
const dead = async (process: Awaited<ReturnType<typeof child>>) => {
  await stopOwned(process.owned, 1_000, 1_000);
  await process.exited;
};

const fixture = async (options: { liveMain?: boolean; project?: string; agentName?: string; role?: string; mismatchHost?: boolean; noIdentity?: boolean; realHost?: boolean; sessionRegistry?: boolean } = {}) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-successor-"));
  cleanups.push(async () => fs.rmSync(root, { recursive: true, force: true }));
  const main = await child();
  const host = await child();
  if (!options.liveMain) await dead(main);
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
  const caller = rootRecord(identity.id, { pid: process.pid, startTime: startTime(process.pid), kernelId: kernelId(), commandLine: fs.readFileSync(`/proc/${process.pid}/cmdline`, "utf8") });
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
  fs.mkdirSync(oldDir, { recursive: true });
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
  const provider = new AgentsProvider(agents, actors, new GlobalActorRegistry(root, 64 * 1024), mainAgent, participants, undefined, lifecycle, () => false, residency);
  const remove = () => provider.invoke("remove", { id: actor.id, successor: true }, context);
  return { root, actorRoot, actor, oldId, oldDir, main, host, actors, agents, meshConfig, provider, residency, mesh, remove, predecessor, identity, realOwner };
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
  it("removes an actor loaded in a real compiled resident host and prevents its restart", { timeout: 60_000 }, async () => {
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
    const oldConfig = JSON.parse(fs.readFileSync(path.join(f.oldDir, "config.json"), "utf8")) as ResidentHostConfig;
    const retry = new ResidencyClient({ config: oldConfig, mesh: f.mesh, participants: f.residency.options.participants,
      mainAgent: { id: f.oldId, local: true } as FabricMainAgentTarget, hostPath: path.resolve("dist/residency/launcher.js") });
    try { await expect(retry.ensureHost()).rejects.toThrow("retired by a successor Main"); }
    finally { await retry.close(); }
    expect(new ActorRegistryStore(f.actorRoot).records()).toEqual([]);
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
