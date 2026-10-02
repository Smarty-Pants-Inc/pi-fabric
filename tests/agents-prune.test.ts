import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { spawn } from "node:child_process";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { FabricRuntimeState } from "../src/fabric-runtime-state.js";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { ActorDirectory } from "../src/actors/directory.js";
import { AgentManager } from "../src/agents/manager.js";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import { DEFAULT_FABRIC_CONFIG, normalizeFabricConfig } from "../src/config.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import { FabricExecutionService } from "../src/execution-service.js";
import { LifecycleBroker } from "../src/lifecycle/broker.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { actorParticipantRecord } from "../src/topology/records.js";
import { writeHostLease, readHostLeases } from "../src/topology/host-leases.js";
import { ResidentHost } from "../src/residency/host.js";
import { RESIDENT_HOST_FORMAT, type ResidentHostConfig } from "../src/residency/protocol.js";
import { residentRoot } from "../src/residency/protocol.js";
import { processStartTime } from "../src/residency/process-identity.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { ActorBindingStore } from "../src/actors/binding-store.js";
import { lockFile } from "../src/residency/file-lock.js";
import * as residentLocks from "../src/residency/file-lock.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";

const closers: Array<() => Promise<void>> = [];
const roots: string[] = [];
afterEach(async () => {
  vi.restoreAllMocks();
  for (const close of closers.splice(0).reverse()) await close();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  vi.unstubAllEnvs();
});
const hash = (text: string) => createHash("sha256").update(text).digest("hex");
const snapshot = (root: string): Record<string, string> => {
  const files: Record<string, string> = {};
  const visit = (at: string) => {
    if (!fs.existsSync(at)) return;
    for (const entry of fs.readdirSync(at, { withFileTypes: true })) {
      const file = path.join(at, entry.name);
      if (entry.isDirectory()) visit(file);
      else files[path.relative(root, file)] = fs.readFileSync(file).toString("base64");
    }
  };
  visit(root); return files;
};
const fixture = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-prune-")); roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const open = async (sessionId: string) => {
    const identity: MeshIdentity = { id: `session:${sessionId}`, name: "main", kind: "main", sessionId };
    const main = { id: identity.id, local: true, matches: (id: string) => id === identity.id || id === "main",
      info: () => ({ id: identity.id, name: "Main", kind: "main", status: "idle", runner: "pi", transport: "host", cwd: root,
        sessionId, startedAt: 1, updatedAt: 1, pendingMessages: false, local: true }),
      deliverAgent: () => ({ queued: true, messageId: "unused", routed: "main" }) } as FabricMainAgentTarget;
    const agents = new AgentManager(root, DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, `runs-${sessionId}`) });
    const participants = new ParticipantDirectory(mesh, { enabled: true, hostId: identity.id, rootId: identity.id, identity, reapDeadHosts: false });
    const actorRoots = { project: path.join(mesh.root, "actors"), session: path.join(mesh.root, "actors", sessionId) };
    const actors = new ActorDirectory([sessionId, identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 60_000 }, agents, () => {}, {
      persistent: true, rootId: identity.id, claimResidency: "session", project: "not-a-project-root", role: "worktree-agent",
      canManageActor: id => { const p = participants.get(id); return p ? p.ownerHostId === identity.id : undefined; },
      lineageAlive: id => participants.get(id) !== undefined,
    }], actorRoots, "project");
    participants.registerSource(() => [participants.root(main.info({} as ExtensionContext)),
      ...actors.listOwned().map(actor => actorParticipantRecord(actor, identity.id, identity.id, identity.id, identity.id))]);
    await participants.refresh();
    const lifecycle = new LifecycleBroker(mesh, identity, participants, { enabled: true, pollMs: 60_000, maxReadEvents: 100 }, async () => {});
    const provider = new AgentsProvider(agents, actors, new GlobalActorRegistry(root, 64 * 1024), main, participants, undefined, lifecycle);
    let closed = false;
    const close = async () => { if (closed) return; closed = true; await lifecycle.close(); await actors.close(); await agents.close(); await participants.close(); };
    closers.push(close);
    return { identity, actors, participants, provider, close, actorRoots };
  };
  const owner = await open("old-main");
  const project = await owner.actors.create({ name: "old-project-actor", instructions: "Wait." });
  const session = await owner.actors.create({ name: "old-session-actor", instructions: "Wait.", scope: "session" });
  await owner.participants.refresh();
  const caller = await open("caller");
  const config = structuredClone(DEFAULT_FABRIC_CONFIG); config.approvals.agent = "allow"; config.approvals.read = "allow";
  config.executor.timeoutMs = 10_000;
  const registry = new ActionRegistry(); registry.register(caller.provider);
  const service = new FabricExecutionService(registry, config);
  const run = (code: string) => service.execute({ code, signal: undefined, parentToolCallId: "prune-regression",
    context: { cwd: root, hasUI: false } as ExtensionContext, onPartial() {} });
  return { root, mesh, owner, caller, project, session, run };
};

const nativeResume = (h: Awaited<ReturnType<typeof fixture>>) => {
  vi.stubEnv("PI_CODING_AGENT_DIR", path.join(h.root, "agent"));
  vi.stubEnv("PI_FABRIC_PROJECT_ROOT", h.root);
  vi.stubEnv("PI_FABRIC_MESH_ROOT", h.mesh.root);
  const pi = { events: { emit: vi.fn() }, getThinkingLevel: () => "off", sendMessage: vi.fn() } as unknown as ExtensionAPI;
  const context = { cwd: h.root, hasUI: false, isProjectTrusted: () => true, isIdle: () => true,
    hasPendingMessages: () => false, modelRegistry: { find: vi.fn(), getApiKeyAndHeaders: vi.fn() },
    sessionManager: { getSessionId: () => "old-main", getSessionFile: () => undefined, getBranch: () => [], getLeafId: () => undefined },
    ui: { setStatus: vi.fn(), notify: vi.fn() } } as unknown as ExtensionContext;
  const runtime = new FabricRuntimeState(pi, new CapturedToolCatalog(), { paths: {
    extension: path.resolve("tests/fixtures/fake-worker.mjs"), worker: path.resolve("tests/fixtures/fake-worker.mjs"),
    residentHost: path.resolve("tests/fixtures/fake-worker.mjs"), skills: h.root } });
  closers.push(() => runtime.shutdown());
  const config = normalizeFabricConfig({ fullCodeMode: true, mesh: { enabled: true, actorPollMs: 60_000 },
    mcp: { enabled: false, cache: { enabled: false } }, memory: { enabled: false }, jev: { enabled: false },
    agents: { enabled: false }, residency: { enabled: false }, prewalk: { enabled: false, alwaysRearm: false } });
  return { runtime, start: () => runtime.initialize(context, config) };
};
const deferred = () => {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
};

describe("agents.prune real Fabric path (#2184 item 7)", () => {
  it.skipIf(process.platform !== "linux")("F7 exact-root native resume waits for prune, loads post-prune rows and cannot resurrect them", async () => {
    const h = await fixture();
    await h.owner.close();
    const store = new ActorRegistryStore(h.owner.actorRoots.project);
    const durable = { ...store.records().find(row => row.id === h.project.id)!, id: "d".repeat(32), name: "durable-control", residency: "durable" };
    const unrelated = { ...durable, id: "e".repeat(32), name: "unrelated-control", residency: "session", rootId: h.caller.identity.id };
    await store.withLock(() => store.write([...store.records(), durable, unrelated]));
    for (const [at, id] of [[h.owner.actorRoots.project, h.project.id], [h.owner.actorRoots.session, h.session.id],
      [h.owner.actorRoots.project, durable.id], [h.caller.actorRoots.project, unrelated.id]]) {
      fs.mkdirSync(path.join(at!, id!), { recursive: true });
      fs.writeFileSync(path.join(at!, id!, "session.jsonl"), `transcript-${id}\n`);
    }
    const resume = nativeResume(h);
    const leaseEntered = deferred(), releaseLease = deferred(), startupReached = deferred();
    const scheduleRefresh = ParticipantDirectory.prototype.scheduleRefresh;
    vi.spyOn(ParticipantDirectory.prototype, "scheduleRefresh").mockImplementation(function (this: ParticipantDirectory) {
      if (this.options.hostId !== h.owner.identity.id) scheduleRefresh.call(this);
    });
    const originalStart = ParticipantDirectory.prototype.start;
    vi.spyOn(ParticipantDirectory.prototype, "start").mockImplementation(async function (this: ParticipantDirectory) {
      if (this.options.hostId === h.owner.identity.id) { leaseEntered.resolve(); startupReached.resolve(); await releaseLease.promise; }
      return originalStart.call(this);
    });
    const originalLock = residentLocks.lockFile;
    vi.spyOn(residentLocks, "lockFile").mockImplementation(async (...args) => {
      if (args[0].endsWith("/main-start.lock") && args[1] !== 0) startupReached.resolve();
      return originalLock(...args);
    });
    let starting: Promise<void> | undefined;
    let loadedDuringPrune = false;
    const publish = h.mesh.publish.bind(h.mesh);
    vi.spyOn(h.mesh, "publish").mockImplementation(async (...args) => {
      if (args[0].kind === "actor.prune") {
        starting = resume.start();
        await startupReached.promise;
        try { loadedDuringPrune = resume.runtime.actors.list().some(a => a.id === h.project.id || a.id === h.session.id); } catch { /* correctly fenced before construction */ }
      }
      return publish(...args);
    });
    try {
      const result = await h.run('return await agents.prune({ root: "session:old-main" });');
      await leaseEntered.promise;
      releaseLease.resolve(); await starting;
      expect(loadedDuringPrune, "native Main must contend before loading its old ownership root").toBe(false);
      expect(result.success, result.error).toBe(true);
      expect((result.value as any).removed.actors).toBe(2);
      // A subsequent real registry write must not merge back objects loaded before deletion.
      await resume.runtime.actors.create({ name: "post-prune-control", instructions: "New work.", scope: "session" });
      if (resume.runtime.actors.list().some(a => a.id === h.project.id)) await resume.runtime.actors.setInstructions(h.project.id, "Must not resurrect.");
      const ids = [...store.records(), ...new ActorRegistryStore(h.owner.actorRoots.session).records()].map(row => row.id);
      expect(ids).not.toContain(h.project.id); expect(ids).not.toContain(h.session.id);
      expect(resume.runtime.actors.list().map(a => a.id)).not.toContain(h.project.id);
      expect(resume.runtime.actors.list().map(a => a.id)).not.toContain(h.session.id);
      for (const [id, text] of [[durable.id, `transcript-${durable.id}\n`], [unrelated.id, `transcript-${unrelated.id}\n`]]) {
        expect(fs.readFileSync(path.join(h.owner.actorRoots.project, id!, "session.jsonl"), "utf8")).toBe(text);
        expect(ids).toContain(id);
      }
      expect(fs.existsSync(path.join(h.owner.actorRoots.project, h.project.id))).toBe(false);
      expect(fs.existsSync(path.join(h.owner.actorRoots.session, h.session.id))).toBe(false);
    } finally { releaseLease.resolve(); await starting; }
  });

  it.skipIf(process.platform !== "linux")("F7 native resume with delayed first lease excludes prune before any actor file deletion", async () => {
    const h = await fixture(); await h.owner.close();
    const resume = nativeResume(h);
    const leaseEntered = deferred(), releaseLease = deferred();
    const scheduleRefresh = ParticipantDirectory.prototype.scheduleRefresh;
    vi.spyOn(ParticipantDirectory.prototype, "scheduleRefresh").mockImplementation(function (this: ParticipantDirectory) {
      if (this.options.hostId !== h.owner.identity.id) scheduleRefresh.call(this);
    });
    const originalStart = ParticipantDirectory.prototype.start;
    vi.spyOn(ParticipantDirectory.prototype, "start").mockImplementation(async function (this: ParticipantDirectory) {
      if (this.options.hostId === h.owner.identity.id) { leaseEntered.resolve(); await releaseLease.promise; }
      return originalStart.call(this);
    });
    const starting = resume.start();
    try {
      await leaseEntered.promise;
      const result = await h.run('return await agents.prune({ root: "session:old-main" });');
      expect(result.success).toBe(false);
      expect(result.error).toMatch(/live.*(?:startup|host lock)/i);
      expect(fs.existsSync(path.join(h.owner.actorRoots.project, h.project.id))).toBe(true);
      expect(fs.existsSync(path.join(h.owner.actorRoots.session, h.session.id))).toBe(true);
    } finally { releaseLease.resolve(); await starting; }
    expect(resume.runtime.actors.listOwned().map(a => a.id)).toEqual(expect.arrayContaining([h.project.id, h.session.id]));
  });
  it.skipIf(process.platform !== "linux")("F7 native Main can resume while its resident lifetime host.lock remains held", async () => {
    const h = await fixture(); await h.owner.close();
    const dir = residentRoot(h.mesh.root, h.owner.identity.id);
    fs.mkdirSync(dir, { recursive: true });
    const fd = await lockFile(path.join(dir, "host.lock"), 0, true);
    try {
      const resume = nativeResume(h); await resume.start();
      expect(resume.runtime.actors.listOwned().map(a => a.id)).toEqual(expect.arrayContaining([h.project.id, h.session.id]));
      await expect(lockFile(path.join(dir, "host.lock"), 0, true)).rejects.toBeInstanceOf(residentLocks.FileLockBusy);
      const result = await h.run('return await agents.prune({ root: "session:old-main" });');
      expect(result.success).toBe(false);
      expect(fs.existsSync(path.join(h.owner.actorRoots.project, h.project.id))).toBe(true);
      expect(fs.existsSync(path.join(h.owner.actorRoots.session, h.session.id))).toBe(true);
    } finally { fs.closeSync(fd); }
  });

  it.skipIf(process.platform !== "linux")("F7 failed first native lease publication loads no actors and releases its startup fence", async () => {
    const h = await fixture(); await h.owner.close();
    const before = [h.owner.actorRoots.project, h.owner.actorRoots.session].map(at => fs.readFileSync(path.join(at, "actors.json"), "utf8"));
    const resume = nativeResume(h);
    vi.spyOn(ParticipantDirectory.prototype, "start").mockRejectedValue(new Error("fixture initial lease EIO"));
    await expect(resume.start()).rejects.toThrow("fixture initial lease EIO");
    expect(() => resume.runtime.actors).toThrow(/not initialized/);
    expect([h.owner.actorRoots.project, h.owner.actorRoots.session].map(at => fs.readFileSync(path.join(at, "actors.json"), "utf8"))).toEqual(before);
    const fd = await lockFile(path.join(residentRoot(h.mesh.root, h.owner.identity.id), "main-start.lock"), 0, true, false);
    fs.closeSync(fd);
  });

  it.skipIf(process.platform !== "linux")("F7 native Linux startup never skips the fence on an unavailable per-process capability probe", async () => {
    const h = await fixture(); await h.owner.close();
    vi.spyOn(residentLocks, "kernelFenceAvailable").mockReturnValue(false);
    const originalLock = residentLocks.lockFile;
    const locked = vi.spyOn(residentLocks, "lockFile").mockImplementation(originalLock);
    const resume = nativeResume(h); await resume.start();
    expect(locked).toHaveBeenCalledWith(path.join(residentRoot(h.mesh.root, h.owner.identity.id), "main-start.lock"), 120, true);
    expect(resume.runtime.actors.listOwned().map(a => a.id)).toEqual(expect.arrayContaining([h.project.id, h.session.id]));
  });

  it.skipIf(process.platform !== "linux").each(["shutdown", "reload", "reinitialize"])(
    "S4 native ownership fence survives %s writer draining and preserves its inode", async mode => {
      const h = await fixture(); await h.owner.close();
      const resume = nativeResume(h); await resume.start();
      const file = path.join(residentRoot(h.mesh.root, h.owner.identity.id), "main-start.lock");
      const inode = fs.statSync(file);
      await expect(lockFile(file, 0, true, false)).rejects.toBeInstanceOf(residentLocks.FileLockBusy);
      const entered = deferred(), release = deferred();
      const actors = resume.runtime.actors, close = actors.close.bind(actors);
      vi.spyOn(actors, "close").mockImplementation(async () => {
        entered.resolve(); await release.promise; await close();
      });
      const draining = mode === "reinitialize" ? resume.start() : resume.runtime.shutdown(mode === "reload" ? "reload" : undefined);
      try {
        await entered.promise;
        await expect(lockFile(file, 0, true, false)).rejects.toBeInstanceOf(residentLocks.FileLockBusy);
      } finally { release.resolve(); await draining; }
      if (mode === "reinitialize") {
        await expect(lockFile(file, 0, true, false)).rejects.toBeInstanceOf(residentLocks.FileLockBusy);
        expect(resume.runtime.actors.listOwned().map(actor => actor.id)).toEqual(expect.arrayContaining([h.project.id, h.session.id]));
        await resume.runtime.shutdown();
      }
      const fd = await lockFile(file, 0, true, false);
      try { expect(fs.fstatSync(fd).ino).toBe(inode.ino); expect(fs.fstatSync(fd).dev).toBe(inode.dev); }
      finally { fs.closeSync(fd); }
    });

  it.skipIf(process.platform !== "linux")("S4 failed native writer drain retains ownership until successful shutdown", async () => {
    const h = await fixture(); await h.owner.close();
    const resume = nativeResume(h); await resume.start();
    const file = path.join(residentRoot(h.mesh.root, h.owner.identity.id), "main-start.lock");
    const fault = vi.spyOn(resume.runtime.actors, "close").mockRejectedValueOnce(new Error("fixture actor drain EIO"));
    await expect(resume.runtime.shutdown()).rejects.toThrow("fixture actor drain EIO");
    await expect(lockFile(file, 0, true, false)).rejects.toBeInstanceOf(residentLocks.FileLockBusy);
    fault.mockRestore(); await resume.runtime.shutdown();
    const fd = await lockFile(file, 0, true, false); fs.closeSync(fd);
  });

  const expiredOwner = async (h: Awaited<ReturnType<typeof fixture>>) => {
    const lease = { id: h.owner.identity.id, rootId: h.owner.identity.id, identityId: h.owner.identity.id,
      updatedAt: 1, expiresAt: 2 };
    await h.mesh.put({ key: `topology/hosts/${hash(lease.id)}`, identity: h.owner.identity,
      value: { format: 1, ...lease, identity: h.owner.identity, startedAt: 1 } });
    writeHostLease(h.mesh.root, lease);
    return path.join(h.mesh.root, "host-leases", `${hash(lease.id).slice(0, 32)}.json`);
  };
  it.skipIf(process.platform !== "linux").each([true, false, "exited"] as const)(
    "S4 initialized native Main with expired leases: dryRun=%s honors process lifetime", async mode => {
      const h = await fixture(); await h.owner.close();
      const store = new ActorRegistryStore(h.owner.actorRoots.project);
      const durable = { ...store.records().find(row => row.id === h.project.id)!, id: "d".repeat(32), name: "durable-control", residency: "durable" };
      const unrelated = { ...durable, id: "e".repeat(32), name: "unrelated-control", residency: "session", rootId: h.caller.identity.id };
      await store.withLock(() => store.write([...store.records(), durable, unrelated]));
      for (const [at, id] of [[h.owner.actorRoots.project, h.project.id], [h.owner.actorRoots.session, h.session.id],
        [h.owner.actorRoots.project, durable.id], [h.owner.actorRoots.project, unrelated.id]]) {
        fs.mkdirSync(path.join(at!, id!), { recursive: true });
        for (const file of ["session.jsonl", "mailbox.json", "queue.json"]) fs.writeFileSync(path.join(at!, id!, file), `${file}-${id}\n`);
      }
      const child = spawn("bun", [path.resolve("tests/fixtures/native-main-prune.ts"), h.root, h.mesh.root],
        { stdio: ["ignore", "pipe", "pipe"] });
      let stdout = "", stderr = "";
      child.stdout.on("data", chunk => { stdout += chunk; });
      child.stderr.on("data", chunk => { stderr += chunk; });
      const exited = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve, reject) => {
        child.once("error", reject); child.once("close", (code, signal) => resolve({ code, signal }));
      });
      // Observe spawn errors without abandoning the finally/checked-exit path.
      void exited.catch(() => undefined);
      const until = async (predicate: () => boolean) => {
        const deadline = Date.now() + 10_000;
        while (!predicate()) {
          if (child.exitCode !== null || child.signalCode !== null || Date.now() > deadline) {
            throw new Error(`Native Main fixture failed: ${stdout}\n${stderr}`);
          }
          await new Promise(resolve => setTimeout(resolve, 20));
        }
      };
      try {
        await until(() => stdout.includes('"ready":true'));
        const ready = JSON.parse(stdout.trim().split("\n").find(line => line.includes('"ready":true'))!);
        expect(ready.initialized).toBe(true); expect(ready.pid).toBe(child.pid);
        expect(ready.actors).toEqual(expect.arrayContaining([h.project.id, h.session.id]));
        expect(child.kill("SIGSTOP")).toBe(true);
        await until(() => /^State:\s+T/m.test(fs.readFileSync(`/proc/${child.pid}/status`, "utf8")));
        // Age the legacy session compatibility stamp too; real timers/deadlines
        // still advance, while the stopped native process cannot renew anything.
        const now = Date.now.bind(Date);
        vi.spyOn(Date, "now").mockImplementation(() => now() + 60_000);
        await expiredOwner(h);
        await h.caller.participants.refresh();
        expect(h.caller.participants.writeStalled()).toBeUndefined();
        expect(h.caller.participants.get(h.owner.identity.id, Date.now(), { fresh: true })).toBeUndefined();
        expect(h.caller.participants.get(h.project.id, Date.now(), { fresh: true })).toBeUndefined();
        expect(readHostLeases(h.mesh.root).get(h.owner.identity.id)?.expiresAt).toBe(2);
        const actorBytes = snapshot(path.join(h.mesh.root, "actors"));
        if (mode === "exited") {
          expect(child.kill("SIGKILL")).toBe(true);
          expect(await exited).toEqual({ code: null, signal: "SIGKILL" });
          expect(() => process.kill(ready.pid, 0)).toThrow();
          const plan = await h.run('return await agents.prune({ root: "session:old-main", dryRun: true });');
          expect(plan.success, plan.error).toBe(true); expect((plan.value as any).actors).toHaveLength(2);
          expect(snapshot(path.join(h.mesh.root, "actors"))).toEqual(actorBytes);
          const result = await h.run('return await agents.prune({ root: "session:old-main" });');
          expect(result.success, result.error).toBe(true); expect((result.value as any).removed.actors).toBe(2);
          expect(fs.existsSync(path.join(h.owner.actorRoots.project, h.project.id))).toBe(false);
          expect(fs.existsSync(path.join(h.owner.actorRoots.session, h.session.id))).toBe(false);
          expect(store.records().map(row => row.id)).toEqual(expect.arrayContaining([durable.id, unrelated.id]));
          for (const id of [durable.id, unrelated.id]) for (const file of ["session.jsonl", "mailbox.json", "queue.json"]) {
            expect(fs.readFileSync(path.join(h.owner.actorRoots.project, id, file), "utf8")).toBe(`${file}-${id}\n`);
          }
        } else {
          const before = snapshot(h.root);
          const result = await h.run(`return await agents.prune({ root: "session:old-main", dryRun: ${mode} });`);
          expect(result.success, result.error).toBe(false);
          expect(result.error).toMatch(/live lineage.*native Main/i);
          expect(snapshot(h.root)).toEqual(before);
          expect(snapshot(path.join(h.mesh.root, "actors"))).toEqual(actorBytes);
          expect(() => process.kill(ready.pid, 0)).not.toThrow();
          expect(child.exitCode).toBeNull(); expect(child.signalCode).toBeNull();
        }
      } finally {
        if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
        await exited;
      }
    }, 25_000);

  it.each(["invalid", "invalid-fields", "unreadable", "directory", "cached-renewal"])("F1 refuses unknown %s lease evidence byte-for-byte", async fault => {
    const h = await fixture(); await h.owner.close();
    const file = await expiredOwner(h);
    expect(readHostLeases(h.mesh.root).get(h.owner.identity.id)?.expiresAt).toBe(2);
    if (fault === "invalid") fs.writeFileSync(file, "{broken");
    if (fault === "invalid-fields") fs.writeFileSync(file, JSON.stringify({ ...JSON.parse(fs.readFileSync(file, "utf8")), expiresAt: "expired" }));
    if (fault === "cached-renewal") {
      writeHostLease(h.mesh.root, { id: h.owner.identity.id, rootId: h.owner.identity.id,
        identityId: h.owner.identity.id, updatedAt: Date.now(), expiresAt: Date.now() + 60_000 });
    }
    const before = snapshot(h.root);
    const readFile = fs.readFileSync;
    const readDir = fs.readdirSync;
    if (fault === "unreadable" || fault === "cached-renewal") vi.spyOn(fs, "readFileSync").mockImplementation(((at: unknown, ...args: unknown[]) => {
      if (String(at) === file) throw Object.assign(new Error("fixture EIO lease"), { code: "EIO" });
      return (readFile as Function)(at, ...args);
    }) as typeof fs.readFileSync);
    if (fault === "directory") vi.spyOn(fs, "readdirSync").mockImplementation(((at: unknown, ...args: unknown[]) => {
      if (String(at) === path.dirname(file)) throw Object.assign(new Error("fixture EACCES leases"), { code: "EACCES" });
      return (readDir as Function)(at, ...args);
    }) as typeof fs.readdirSync);
    for (const dryRun of [true, false]) {
      const result = await h.run(`return await agents.prune({ root: "session:old-main", dryRun: ${dryRun} });`);
      expect(result.success, `fault=${fault}, dryRun=${dryRun}`).toBe(false);
      expect(result.error).toMatch(/cannot prove.*(?:lease|ownership)/i);
    }
    vi.restoreAllMocks(); expect(snapshot(h.root)).toEqual(before);
  });
  it.skipIf(process.platform !== "linux")("F1 rechecks unknown leases at the registry commit boundary", async () => {
    const h = await fixture(); await h.owner.close(); const file = await expiredOwner(h);
    const original = ActorRegistryStore.prototype.withLock;
    let before: Record<string, string> | undefined;
    vi.spyOn(ActorRegistryStore.prototype, "withLock").mockImplementation(function (this: ActorRegistryStore, operation) {
      return original.call(this, () => {
        fs.writeFileSync(file, "{bad renewal"); before = snapshot(h.root);
        return operation();
      }) as ReturnType<typeof original>;
    });
    const result = await h.run('return await agents.prune({ root: "session:old-main" });');
    expect(result.success).toBe(false); expect(result.error).toMatch(/cannot prove.*lease/i);
    expect(before).toBeDefined();
    // The registry lock is transient; the already-appended audit is intentionally retained.
    const after = snapshot(h.root);
    for (const [file, bytes] of Object.entries(before!)) if (!file.includes("actors.json.lock/")) expect(after[file], file).toBe(bytes);
  });
  it.skipIf(process.platform !== "linux")("F2 fences a real resident starting after the initial probe, before lease publication", async () => {
    const h = await fixture(); await h.owner.close();
    const config: ResidentHostConfig = { format: RESIDENT_HOST_FORMAT, rootId: h.owner.identity.id, sessionId: "old-main",
      cwd: h.root, projectRoot: h.root, meshRoot: h.mesh.root, actorRoot: h.owner.actorRoots.project,
      residencyRoot: residentRoot(h.mesh.root, h.owner.identity.id), fullCodeMode: true,
      agents: DEFAULT_FABRIC_CONFIG.agents, mesh: DEFAULT_FABRIC_CONFIG.mesh, retention: DEFAULT_FABRIC_CONFIG.retention,
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fabricExtensionPath: path.resolve("dist/index.js"),
      piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda" };
    const host = new ResidentHost(config); closers.push(() => host.close());
    // A starter must contend on the real host lock before actors/participants are initialized.
    vi.spyOn(ParticipantDirectory.prototype, "start").mockResolvedValue(undefined);
    const publish = h.mesh.publish.bind(h.mesh); let blocked = false;
    vi.spyOn(h.mesh, "publish").mockImplementation(async (...args) => {
      if (args[0].kind === "actor.prune") {
        try { await host.start(); } catch (error) { expect(String(error)).toMatch(/already running/i); blocked = true; }
      }
      return publish(...args);
    });
    const result = await h.run('return await agents.prune({ root: "session:old-main" });');
    expect(blocked, "startup must lose the shared fence before loading actors").toBe(true);
    expect(host.actors).toBeUndefined(); expect(result.success, result.error).toBe(true);
    expect((result.value as any).removed.actors).toBe(2);
    // After the prune releases the same persistent inode, a new host loads the committed empty registry.
    const successor = new ResidentHost(config); closers.push(() => successor.close());
    await successor.start(); expect(successor.actors.list().some(a => [h.project.id, h.session.id].includes(a.id))).toBe(false);
    await successor.close();
  });
  it.skipIf(process.platform !== "linux")("F3 preserves durable records, files, bindings, removal markers and actor/participant state in a mixed root", async () => {
    const h = await fixture(); await h.owner.close();
    const durable = "d".repeat(32); const at = h.owner.actorRoots.project;
    const store = new ActorRegistryStore(at); const source = store.records()[0]!;
    const durableRow = { ...source, id: durable, name: "keep-durable", residency: "durable" };
    await store.withLock(() => store.write([...store.records(), durableRow]));
    fs.mkdirSync(path.join(at, durable), { recursive: true });
    fs.writeFileSync(path.join(at, durable, "session.jsonl"), "durable transcript\n");
    const marker = path.join(at, `removal-${durable}.json`);
    fs.writeFileSync(marker, JSON.stringify({ id: durable, owner: { rootId: h.owner.identity.id, residency: "durable" } }));
    const binding = new ActorBindingStore("old-main", at); await binding.setThinking(durable, "high");
    const actorKey = `actors/old-main/${durable}`;
    const participantKey = `topology/participants/${hash(durable)}`;
    await h.mesh.put({ key: actorKey, value: { id: durable, rootId: h.owner.identity.id, residency: "durable" }, identity: h.owner.identity });
    const actor = { ...h.project, id: durable, name: "keep-durable", residency: "durable" as const };
    const participant = actorParticipantRecord(actor, h.owner.identity.id, h.owner.identity.id, h.owner.identity.id, h.owner.identity.id);
    const participantEntry = await h.mesh.put({ key: participantKey, value: participant, identity: h.owner.identity });
    writeParticipantFile(h.mesh.root, participantEntry);
    const preservedState = [h.mesh.get(actorKey, { fresh: true }), h.mesh.get(participantKey, { fresh: true })];
    const bytes = [path.join(at, durable, "session.jsonl"), marker, binding.filePath!, path.join(h.mesh.root, "participants", `${hash(durable)}.json`)].map(file => [file, fs.readFileSync(file, "utf8")] as const);
    const plan = await h.run('return await agents.prune({ root: "session:old-main", dryRun: true });');
    expect(plan.success, plan.error).toBe(true); expect((plan.value as any).actors.map((a: any) => a.id)).not.toContain(durable);
    const result = await h.run('return await agents.prune({ root: "session:old-main" });');
    expect(result.success, result.error).toBe(true); expect((result.value as any).removed.actors).toBe(2);
    expect(store.records()).toEqual([durableRow]);
    for (const [file, text] of bytes) expect(fs.readFileSync(file, "utf8"), file).toBe(text);
    expect([h.mesh.get(actorKey, { fresh: true }), h.mesh.get(participantKey, { fresh: true })]).toEqual(preservedState);
    expect((await h.run('return await agents.prune({ root: "session:old-main" });')).success).toBe(true);
  });
  it.each(["shared-state", "participant-file", "participant-directory"])("F1 refuses unknown %s ownership evidence without changes", async fault => {
    const h = await fixture(); await h.owner.close();
    if (fault === "shared-state") fs.writeFileSync(path.join(h.mesh.root, "state.json"), "{damaged");
    if (fault === "participant-file") fs.writeFileSync(path.join(h.mesh.root, "participants", `${hash(h.owner.identity.id)}.json`), "{damaged");
    const before = snapshot(h.root); const readDir = fs.readdirSync;
    if (fault === "participant-directory") vi.spyOn(fs, "readdirSync").mockImplementation(((at: unknown, ...args: unknown[]) => {
      if (String(at) === path.join(h.mesh.root, "participants")) throw Object.assign(new Error("fixture EIO participants"), { code: "EIO" });
      return (readDir as Function)(at, ...args);
    }) as typeof fs.readdirSync);
    for (const dryRun of [true, false]) {
      const result = await h.run(`return await agents.prune({ root: "session:old-main", dryRun: ${dryRun} });`);
      expect(result.success).toBe(false); expect(result.error).toMatch(/cannot prove.*ownership/i);
    }
    vi.restoreAllMocks(); expect(snapshot(h.root)).toEqual(before);
  });
  it.skipIf(process.platform !== "linux").each(["registry", "actor-state", "removal"])("F1 refuses unknown %s actor ownership byte-for-byte", async fault => {
    const h = await fixture(); await h.owner.close();
    if (fault === "registry") {
      const store = new ActorRegistryStore(h.owner.actorRoots.project);
      await store.withLock(() => store.write(store.records().map(row => ({ ...row, residency: "unknown" }))));
    } else if (fault === "actor-state") {
      await h.mesh.put({ key: `actors/old-main/${h.project.id}`, value: { id: h.project.id, rootId: 42 }, identity: h.owner.identity });
    } else fs.writeFileSync(path.join(h.owner.actorRoots.project, `removal-${h.project.id}.json`), JSON.stringify({ id: h.project.id }));
    const dir = residentRoot(h.mesh.root, h.owner.identity.id); fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, "host.lock"), "");
    fs.writeFileSync(path.join(dir, "main-start.lock"), ""); // persistent vacant startup fence: refusal must not change its bytes either
    const before = snapshot(h.root);
    for (const dryRun of [true, false]) {
      const result = await h.run(`return await agents.prune({ root: "session:old-main", dryRun: ${dryRun} });`);
      expect(result.success).toBe(false); expect(result.error).toMatch(/cannot prove.*ownership/i);
      expect(snapshot(h.root)).toEqual(before);
    }
  });
  it.skipIf(process.platform !== "linux").each(["invalid-lock", "unknown-process"])("F1 refuses %s resident evidence byte-for-byte", async fault => {
    const h = await fixture(); await h.owner.close(); const dir = residentRoot(h.mesh.root, h.owner.identity.id);
    fs.mkdirSync(dir, { recursive: true }); const pid = 2_000_000_000;
    if (fault === "invalid-lock") fs.writeFileSync(path.join(dir, "host.lock"), "{invalid owner");
    else fs.writeFileSync(path.join(dir, "owner.json"), JSON.stringify({ pid }));
    const before = snapshot(h.root); const kill = process.kill;
    if (fault === "unknown-process") vi.spyOn(process, "kill").mockImplementation((id, signal) => {
      if (id === pid) throw Object.assign(new Error("fixture EIO process probe"), { code: "EIO" });
      return kill(id, signal);
    });
    for (const dryRun of [true, false]) {
      const result = await h.run(`return await agents.prune({ root: "session:old-main", dryRun: ${dryRun} });`);
      expect(result.success).toBe(false); expect(result.error).toMatch(/cannot prove.*resident/i);
    }
    vi.restoreAllMocks(); expect(snapshot(h.root)).toEqual(before);
  });
  it.each(["unsupported-platform", "missing-kernel-fence"])("F2 refuses destructive pruning with %s before creating files", async fault => {
    const h = await fixture(); await h.owner.close(); const before = snapshot(h.root);
    const descriptor = Object.getOwnPropertyDescriptor(process, "platform")!;
    try {
      if (fault === "unsupported-platform") Object.defineProperty(process, "platform", { ...descriptor, value: "win32" });
      else vi.spyOn(residentLocks, "kernelFenceAvailable").mockReturnValue(false);
      const result = await h.run('return await agents.prune({ root: "session:old-main" });');
      expect(result.success).toBe(false); expect(result.error).toMatch(/cannot prove.*fence unavailable/i);
      expect(snapshot(h.root)).toEqual(before);
    } finally { Object.defineProperty(process, "platform", descriptor); }
  });
  it.skipIf(process.platform !== "linux")("F2 rechecks resident process ownership after a registry-lock wait", async () => {
    const h = await fixture(); await h.owner.close();
    const original = ActorRegistryStore.prototype.withLock;
    vi.spyOn(ActorRegistryStore.prototype, "withLock").mockImplementation(function (this: ActorRegistryStore, operation) {
      return original.call(this, () => {
        const dir = residentRoot(h.mesh.root, h.owner.identity.id); fs.mkdirSync(dir, { recursive: true });
        fs.writeFileSync(path.join(dir, "owner.json"), JSON.stringify({ pid: process.pid, processStartTime: processStartTime(process.pid) }));
        return operation();
      }) as ReturnType<typeof original>;
    });
    const result = await h.run('return await agents.prune({ root: "session:old-main" });');
    expect(result.success).toBe(false); expect(result.error).toMatch(/resident owner/i);
    expect(new ActorRegistryStore(h.owner.actorRoots.project).records().map(a => a.id)).toContain(h.project.id);
    expect(fs.existsSync(path.join(h.owner.actorRoots.project, h.project.id))).toBe(true);
  });
  it.skipIf(process.platform !== "linux").each(["first-registry-removal", "shared-state-commit"])("F5 resumes after %s without losing dead-root ownership or durable controls", async fault => {
    const h = await fixture(); await h.owner.close(); const lease = await expiredOwner(h);
    const at = h.owner.actorRoots.project; const store = new ActorRegistryStore(at);
    const durable = "d".repeat(32); const durableRow = { ...store.records()[0]!, id: durable, name: "keep-durable", residency: "durable" };
    await store.withLock(() => store.write([...store.records(), durableRow]));
    fs.mkdirSync(path.join(at, durable), { recursive: true });
    const durableFile = path.join(at, durable, "session.jsonl"); fs.writeFileSync(durableFile, "durable transcript\n");
    for (const actor of [h.project, h.session, { ...h.project, id: durable, name: "keep-durable", residency: "durable" as const }]) {
      const value = actorParticipantRecord(actor, h.owner.identity.id, h.owner.identity.id, h.owner.identity.id, h.owner.identity.id);
      const entry = await h.mesh.put({ key: `topology/participants/${hash(actor.id)}`, value, identity: h.owner.identity });
      writeParticipantFile(h.mesh.root, entry);
    }
    const durableParticipantFile = path.join(h.mesh.root, "participants", `${hash(durable)}.json`);
    const durableParticipantBytes = fs.readFileSync(durableParticipantFile, "utf8");
    const durableKey = `actors/old-main/${durable}`;
    await h.mesh.put({ key: durableKey, value: { id: durable, rootId: h.owner.identity.id, residency: "durable" }, identity: h.owner.identity });
    const unrelated = await h.caller.actors.create({ name: "keep-unrelated", instructions: "Wait.", scope: "session" });
    const controls = h.mesh.listAll("actors/", { fresh: true }).filter(e => e.key.endsWith(durable) || e.key.endsWith(unrelated.id));
    const write = ActorRegistryStore.prototype.write; const writeBatch = h.mesh.writeBatch.bind(h.mesh); let injected = false;
    if (fault === "first-registry-removal") vi.spyOn(ActorRegistryStore.prototype, "write").mockImplementation(function (this: ActorRegistryStore, rows, options) {
      const removed = this.records().some(row => row.id === h.project.id) && !rows.some(row => row.id === h.project.id);
      write.call(this, rows, options);
      if (!injected && removed) { injected = true; throw new Error("fixture failure after first registry removal"); }
    });
    else vi.spyOn(h.mesh, "writeBatch").mockImplementation(async options => {
      if (options.ops.some(op => op.kind === "delete" && op.key.startsWith("actors/"))) { injected = true; fs.writeFileSync(lease, "{bad renewal"); }
      return writeBatch(options);
    });
    const first = await h.run('return await agents.prune({ root: "session:old-main" });');
    expect(first.success).toBe(false); expect(injected).toBe(true);
    expect(new ActorRegistryStore(at).records().some(row => row.id === h.project.id)).toBe(false);
    const receiptPath = path.join(residentRoot(h.mesh.root, h.owner.identity.id), "prune.json");
    const receipt = JSON.parse(fs.readFileSync(receiptPath, "utf8"));
    expect(receipt.rootId).toBe(h.owner.identity.id);
    expect(receipt.actors.map((actor: any) => actor.id).sort()).toEqual([h.project.id, h.session.id].sort());
    expect(receipt.removed.files).toContain(path.join(at, h.project.id));
    vi.restoreAllMocks(); await expiredOwner(h);
    const retry = await h.run('const plan = await agents.prune({ root: "session:old-main" }); return { plan, members: await agents.members({ includeStale: true }), actors: await agents.actors() };');
    expect(retry.success, retry.error).toBe(true);
    const value = retry.value as any;
    expect(value.plan.removed.actors).toBe(fault === "first-registry-removal" ? 1 : 0);
    for (const id of [h.project.id, h.session.id]) {
      expect(h.mesh.listAll("actors/", { fresh: true }).some(e => e.key.endsWith(id))).toBe(false);
      expect(fs.existsSync(path.join(h.mesh.root, "participants", `${hash(id)}.json`))).toBe(false);
      expect([...value.members, ...value.actors].some((actor: any) => actor.id === id)).toBe(false);
    }
    expect(fs.existsSync(path.join(at, h.project.id))).toBe(false);
    expect(fs.existsSync(path.join(h.owner.actorRoots.session, h.session.id))).toBe(false);
    expect(store.records()).toEqual([durableRow]); expect(fs.readFileSync(durableFile, "utf8")).toBe("durable transcript\n");
    expect(fs.existsSync(receiptPath)).toBe(false);
    expect(fs.readFileSync(durableParticipantFile, "utf8")).toBe(durableParticipantBytes);
    expect(h.mesh.listAll("actors/", { fresh: true }).filter(e => e.key.endsWith(durable) || e.key.endsWith(unrelated.id))).toEqual(controls);
  });
  it.skipIf(process.platform !== "linux")("S3 preserves a live root's same-name model/thinking bindings when storage and root IDs differ", async () => {
    const h = await fixture(); await h.owner.close(); const at = h.owner.actorRoots.project;
    const liveRoot = "session:live-main"; const liveId = "e".repeat(32); const durable = "d".repeat(32);
    writeHostLease(h.mesh.root, { id: liveRoot, rootId: liveRoot, identityId: liveRoot, updatedAt: Date.now(), expiresAt: Date.now() + 60_000 });
    const store = new ActorRegistryStore(at);
    await store.withLock(() => store.write([...store.records(), { ...store.records()[0]!, id: liveId, name: h.project.name, rootId: liveRoot },
      { ...store.records()[0]!, id: durable, name: "keep-live-durable", rootId: liveRoot, residency: "durable" }]));
    const liveBinding = new ActorBindingStore("old-main", at, liveRoot);
    await liveBinding.setModel(liveId, "provider/live-model"); await liveBinding.setThinking(liveId, "high");
    await liveBinding.setThinking(durable, "low");
    const before = fs.readFileSync(liveBinding.filePath!, "utf8");
    const deadBinding = new ActorBindingStore("dead-storage-overlay", at, h.owner.identity.id);
    await deadBinding.setModel(h.project.id, "provider/dead-model"); await deadBinding.setThinking(h.project.id, "low");
    const legacyBinding = new ActorBindingStore("legacy-storage-overlay", at);
    await legacyBinding.setThinking(h.project.id, "high"); const legacyBytes = fs.readFileSync(legacyBinding.filePath!, "utf8");
    const result = await h.run('return await agents.prune({ root: "session:old-main" });');
    expect(result.success, result.error).toBe(true);
    expect(fs.existsSync(liveBinding.filePath!)).toBe(true);
    expect(fs.readFileSync(liveBinding.filePath!, "utf8")).toBe(before);
    expect(liveBinding.get(liveId)).toMatchObject({ model: "provider/live-model", thinking: "high" });
    expect(liveBinding.get(durable)).toMatchObject({ thinking: "low" });
    expect(deadBinding.get(h.project.id)).toBeUndefined();
    expect(fs.readFileSync(legacyBinding.filePath!, "utf8")).toBe(legacyBytes);
    const repeat = await h.run('return await agents.prune({ root: "session:old-main" });');
    expect(repeat.success, repeat.error).toBe(true); expect(fs.readFileSync(liveBinding.filePath!, "utf8")).toBe(before);
  });
  it.skipIf(process.platform !== "linux")("F1 rechecks unknown leases inside the shared-state commit", async () => {
    const h = await fixture(); await h.owner.close(); const file = await expiredOwner(h);
    const inboxKey = `topology/inbox/${hash(h.owner.identity.id).slice(0, 32)}`;
    await h.mesh.put({ key: inboxKey, value: { after: 0 }, identity: h.owner.identity });
    const entry = h.mesh.listAll("actors/", { fresh: true }).find(e => e.key.endsWith(h.project.id))!;
    const writeBatch = h.mesh.writeBatch.bind(h.mesh);
    vi.spyOn(h.mesh, "writeBatch").mockImplementation(async options => {
      if (options.ops.some(op => op.kind === "delete" && op.key.startsWith("actors/"))) fs.writeFileSync(file, "{bad renewal");
      return writeBatch(options);
    });
    const result = await h.run('return await agents.prune({ root: "session:old-main" });');
    expect(result.success).toBe(false); expect(result.error).toMatch(/cannot prove.*lease/i);
    expect(h.mesh.get(entry.key, { fresh: true })).toEqual(entry);
    vi.restoreAllMocks(); await expiredOwner(h);
    const retry = await h.run('return await agents.prune({ root: "session:old-main" });');
    expect(retry.success, retry.error).toBe(true);
    expect(h.mesh.get(entry.key, { fresh: true })).toBeUndefined();
    expect(h.mesh.get(inboxKey, { fresh: true })).toBeUndefined();
    expect(h.mesh.listAll("topology/participants/", { fresh: true }).some(e => (e.value as any).rootId === h.owner.identity.id)).toBe(false);
    expect(fs.existsSync(path.join(residentRoot(h.mesh.root, h.owner.identity.id), "prune.json"))).toBe(false);
  });
  it.skipIf(process.platform !== "linux")("F5 establishes the durable receipt barrier before the first destructive operation", async () => {
    const h = await fixture(); await h.owner.close(); const dir = residentRoot(h.mesh.root, h.owner.identity.id);
    fs.mkdirSync(dir, { recursive: true }); fs.writeFileSync(path.join(dir, "host.lock"), "");
    fs.writeFileSync(path.join(dir, "main-start.lock"), "");
    const before = snapshot(h.root); const fsync = fs.fsyncSync; let injected = false;
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      if (fs.readlinkSync(`/proc/self/fd/${fd}`).includes("/prune.json.")) {
        injected = true; throw Object.assign(new Error("fixture prune receipt fsync EIO"), { code: "EIO" });
      }
      return fsync(fd);
    });
    const result = await h.run('return await agents.prune({ root: "session:old-main" });');
    expect(result.success).toBe(false); expect(injected).toBe(true); expect(result.error).toMatch(/fsync EIO/);
    vi.restoreAllMocks(); expect(snapshot(h.root)).toEqual(before);
  });
  it.skipIf(process.platform !== "linux").each(["root", "registry", "residency", "malformed"])("F5 refuses %s prune receipt ambiguity without deleting anything", async fault => {
    const h = await fixture(); await h.owner.close(); const dir = residentRoot(h.mesh.root, h.owner.identity.id);
    fs.mkdirSync(dir, { recursive: true }); fs.writeFileSync(path.join(dir, "host.lock"), "");
    fs.writeFileSync(path.join(dir, "main-start.lock"), "");
    const receipt = { format: 1, rootId: fault === "root" ? "session:foreign" : h.owner.identity.id, meshRoot: h.mesh.root,
      actors: [{ at: fault === "registry" ? path.join(h.root, "foreign-registry") : h.owner.actorRoots.project,
        id: h.project.id, residency: fault === "residency" ? "durable" : "session" }],
      removed: { actors: [], files: [], stateKeys: [] } };
    fs.writeFileSync(path.join(dir, "prune.json"), fault === "malformed" ? "{broken" : JSON.stringify(receipt));
    const before = snapshot(h.root);
    for (const dryRun of [true, false]) {
      const result = await h.run(`return await agents.prune({ root: "session:old-main", dryRun: ${dryRun} });`);
      expect(result.success).toBe(false);
      expect(snapshot(h.root)).toEqual(before);
    }
  });
  it.skipIf(process.platform !== "linux")("F5 never derives orphan cleanup authority from state when registry proof and receipt are absent", async () => {
    const h = await fixture(); await h.owner.close();
    for (const at of [h.owner.actorRoots.project, h.owner.actorRoots.session]) {
      const store = new ActorRegistryStore(at); await store.withLock(() => store.write([]));
    }
    const participant = actorParticipantRecord(h.project, h.owner.identity.id, h.owner.identity.id, h.owner.identity.id, h.owner.identity.id);
    const participantEntry = await h.mesh.put({ key: `topology/participants/${hash(h.project.id)}`, value: participant, identity: h.owner.identity });
    writeParticipantFile(h.mesh.root, participantEntry);
    const before = h.mesh.listAll("actors/", { fresh: true });
    const result = await h.run('return await agents.prune({ root: "session:old-main" });');
    expect(result.success, result.error).toBe(true); expect((result.value as any).removed.actors).toBe(0);
    expect(h.mesh.listAll("actors/", { fresh: true })).toEqual(before);
    expect(fs.existsSync(path.join(h.owner.actorRoots.project, h.project.id))).toBe(true);
    expect(fs.existsSync(path.join(h.mesh.root, "participants", `${hash(h.project.id)}.json`))).toBe(true);
  });
  it.skipIf(process.platform !== "linux")("F3 aborts a session-to-durable scope change under the registry lock", async () => {
    const h = await fixture(); await h.owner.close(); const original = ActorRegistryStore.prototype.withLock;
    vi.spyOn(ActorRegistryStore.prototype, "withLock").mockImplementation(function (this: ActorRegistryStore, operation) {
      return original.call(this, () => {
        this.write(this.records().map(row => row.id === h.project.id ? { ...row, residency: "durable" } : row));
        return operation();
      }) as ReturnType<typeof original>;
    });
    const result = await h.run('return await agents.prune({ root: "session:old-main" });');
    expect(result.success).toBe(false); expect(result.error).toMatch(/ownership changed/i);
    expect(fs.existsSync(path.join(h.owner.actorRoots.project, h.project.id))).toBe(true);
    expect(new ActorRegistryStore(h.owner.actorRoots.project).records().find(a => a.id === h.project.id)?.residency).toBe("durable");
  });
  it.skipIf(process.platform !== "linux")("prunes an exited non-project owner, files and discovery, preserving other roots and audit", async () => {
    const h = await fixture();
    const oldBinding = new ActorBindingStore("old-main", h.owner.actorRoots.project, h.owner.identity.id);
    await oldBinding.setThinking(h.project.id, "low");
    for (const [at, actor] of [[h.owner.actorRoots.project, h.project], [h.owner.actorRoots.session, h.session]] as const) {
      fs.mkdirSync(path.join(at, actor.id), { recursive: true });
      fs.writeFileSync(path.join(at, actor.id, "messages.jsonl"), '{"text":"old mail"}\n');
      fs.writeFileSync(path.join(at, actor.id, "queue-dead.json"), '{"items":[{"payload":"old work"}]}');
      fs.writeFileSync(path.join(at, actor.id, "session.jsonl"), '{"text":"old session"}\n');
    }
    await h.owner.close();
    const own = await h.caller.actors.create({ name: "keep-caller", instructions: "Wait.", scope: "session" });
    const inboxKey = `topology/inbox/${hash(h.owner.identity.id).slice(0, 32)}`;
    await h.mesh.put({ key: inboxKey, value: { after: 0 }, identity: h.owner.identity });
    const described = await h.run('return await tools.describe({ ref: "agents.prune" });');
    expect(described.success, described.error).toBe(true);
    expect(JSON.stringify(described.value)).toContain("dryRun");
    const result = await h.run(`const plan = await agents.prune({ root: "session:old-main" });
      const actors = await agents.actors(); const members = await agents.members({ includeStale: true });
      const listed = await agents.list({ scope: "project" }); return { plan, actors, members, listed };`);
    expect(result.success, result.error).toBe(true);
    const value = result.value as any;
    expect(value.plan.actors.map((a: any) => a.id).sort()).toEqual([h.project.id, h.session.id].sort());
    expect(value.plan.removed.actors).toBe(2);
    expect(value.actors.map((a: any) => a.id)).toEqual([own.id]);
    expect([...value.members, ...value.listed].some((p: any) => [h.project.id, h.session.id].includes(p.id))).toBe(false);
    expect(fs.existsSync(path.join(h.owner.actorRoots.project, h.project.id))).toBe(false);
    expect(fs.existsSync(path.join(h.owner.actorRoots.session, h.session.id))).toBe(false);
    expect(oldBinding.get(h.project.id)).toBeUndefined();
    expect(JSON.parse(fs.readFileSync(oldBinding.filePath!, "utf8")).bindings).toEqual({});
    expect(h.mesh.get(inboxKey, { fresh: true })).toBeUndefined();
    expect(h.mesh.listAll("actors/", { fresh: true }).some(e => e.key.endsWith(h.project.id) || e.key.endsWith(h.session.id))).toBe(false);
    expect(h.mesh.read({ topic: "ops.owner", limit: 100 }).some(e => e.kind === "actor.prune")).toBe(true);
    const again = await h.run('return await agents.prune({ root: "session:old-main" });');
    expect(again.success, again.error).toBe(true); expect((again.value as any).removed.actors).toBe(0);
  });
  it.skipIf(process.platform !== "linux")("keeps an actor claimed by a racing adopter under the registry lock and resumes its receipt", async () => {
    const h = await fixture();
    const revoked = await h.owner.actors.create({ name: "already-revoked", instructions: "Wait." });
    await h.owner.close();
    const at = h.owner.actorRoots.project; const store = new ActorRegistryStore(at);
    const durable = "d".repeat(32); const durableRow = { ...store.records()[0]!, id: durable, name: "keep-durable", residency: "durable" };
    await store.withLock(() => store.write([...store.records().filter(row => row.id !== revoked.id), durableRow], { durable: true }));
    fs.rmSync(path.join(at, revoked.id), { recursive: true, force: true });
    fs.mkdirSync(path.join(at, durable), { recursive: true });
    fs.writeFileSync(path.join(at, durable, "session.jsonl"), "durable transcript\n");
    const unrelated = await h.caller.actors.create({ name: "keep-unrelated", instructions: "Wait.", scope: "session" });
    const oldBinding = new ActorBindingStore("old-main", at, h.owner.identity.id);
    await oldBinding.setThinking(h.project.id, "high"); await oldBinding.setThinking(durable, "low");
    const adoptedBinding = new ActorBindingStore("winner-overlay", at, "session:winner");
    await adoptedBinding.setModel(h.project.id, "provider/adopted-model");
    const unrelatedBinding = new ActorBindingStore("unrelated-overlay", at, h.caller.identity.id);
    await unrelatedBinding.setThinking(unrelated.id, "high");
    fs.writeFileSync(path.join(at, `removal-${h.project.id}.json`), JSON.stringify({ id: h.project.id,
      owner: { rootId: h.owner.identity.id, residency: "session" } }));
    for (const actor of [h.project, h.session, revoked, { ...h.project, id: durable, residency: "durable" as const }]) {
      const value = actorParticipantRecord(actor, h.owner.identity.id, h.owner.identity.id, h.owner.identity.id, h.owner.identity.id);
      const entry = await h.mesh.put({ key: `topology/participants/${hash(actor.id)}`, value, identity: h.owner.identity });
      writeParticipantFile(h.mesh.root, entry);
    }
    await h.mesh.put({ key: `actors/old-main/${durable}`, value: { id: durable, rootId: h.owner.identity.id, residency: "durable" }, identity: h.owner.identity });
    const receiptPath = path.join(residentRoot(h.mesh.root, h.owner.identity.id), "prune.json");
    fs.mkdirSync(path.dirname(receiptPath), { recursive: true });
    const revokedProof = { actors: [`${at}/${revoked.id}`], files: [path.join(at, revoked.id)], stateKeys: [] };
    fs.writeFileSync(receiptPath, JSON.stringify({ format: 1, rootId: h.owner.identity.id, meshRoot: h.mesh.root,
      actors: [{ at, id: revoked.id, residency: "session" }], removed: revokedProof }));
    const original = ActorRegistryStore.prototype.withLock;
    let raced = false;
    vi.spyOn(ActorRegistryStore.prototype, "withLock").mockImplementation(function (this: ActorRegistryStore, operation) {
      return original.call(this, () => {
        if (!raced) {
          raced = true;
          const rows = this.records();
          this.write(rows.map(row => row.id === h.project.id ? { ...row, rootId: "session:winner", adoptedAt: Date.now() } : row));
          writeHostLease(h.mesh.root, { id: "session:winner", rootId: "session:winner", identityId: "session:winner",
            updatedAt: Date.now(), expiresAt: Date.now() + 60_000 });
        }
        return operation();
      }) as ReturnType<typeof original>;
    });
    const result = await h.run('return await agents.prune({ root: "session:old-main" });');
    expect(result.success).toBe(false); expect(result.error).toMatch(/ownership changed/i);
    expect(fs.existsSync(path.join(at, h.project.id))).toBe(true);
    expect(store.records().find(row => row.id === h.project.id)?.rootId).toBe("session:winner");
    expect(JSON.parse(fs.readFileSync(receiptPath, "utf8")).actors.map((actor: any) => actor.id).sort())
      .toEqual([h.project.id, h.session.id, revoked.id].sort());
    vi.restoreAllMocks();
    const adoptedRow = store.records().find(row => row.id === h.project.id)!;
    const controlState = h.mesh.listAll("actors/", { fresh: true }).filter(entry => [h.project.id, durable, unrelated.id].some(id => entry.key.endsWith(id)));
    const controls = snapshot(h.root);
    const plan = await h.run('return await agents.prune({ root: "session:old-main", dryRun: true });');
    expect(plan.success, plan.error).toBe(true);
    expect((plan.value as any).actors.map((actor: any) => actor.id)).toEqual([h.session.id]);
    expect((plan.value as any).stateKeys).toContain(`actors/old-main/${revoked.id}`);
    expect((plan.value as any).files).not.toContain(path.join(at, h.project.id));
    expect(snapshot(h.root)).toEqual(controls);
    let checkpointed = false;
    const publish = h.mesh.publish.bind(h.mesh);
    vi.spyOn(h.mesh, "publish").mockImplementation(async options => {
      if (options.kind === "actor.prune") {
        checkpointed = true;
        const receipt = JSON.parse(fs.readFileSync(receiptPath, "utf8"));
        expect(receipt.actors.map((actor: any) => actor.id).sort()).toEqual([h.session.id, revoked.id].sort());
        expect(receipt.removed).toEqual(revokedProof);
      }
      return publish(options);
    });
    const retry = await h.run('const plan = await agents.prune({ root: "session:old-main" }); return { plan, members: await agents.members({ includeStale: true }), actors: await agents.actors() };');
    expect(retry.success, retry.error).toBe(true); expect(checkpointed).toBe(true);
    const value = retry.value as any; expect(value.plan.removed.actors).toBe(1);
    for (const id of [h.session.id, revoked.id]) {
      expect(h.mesh.listAll("actors/", { fresh: true }).some(entry => entry.key.endsWith(id))).toBe(false);
      expect(fs.existsSync(path.join(h.mesh.root, "participants", `${hash(id)}.json`))).toBe(false);
      expect([...value.members, ...value.actors].some((actor: any) => actor.id === id)).toBe(false);
    }
    expect(new ActorRegistryStore(h.owner.actorRoots.session).records()).toEqual([]);
    expect(fs.existsSync(path.join(h.owner.actorRoots.session, h.session.id))).toBe(false);
    expect(fs.existsSync(receiptPath)).toBe(false);
    expect(store.records()).toEqual([adoptedRow, durableRow]);
    const after = snapshot(h.root);
    for (const [file, bytes] of Object.entries(controls)) {
      if (file.startsWith(path.relative(h.root, path.join(at, h.project.id))) ||
          file.startsWith(path.relative(h.root, path.join(at, durable))) ||
          file.startsWith(path.relative(h.root, path.join(h.caller.actorRoots.session, unrelated.id))) ||
          file.startsWith(path.relative(h.root, path.join(at, "bindings"))) ||
          file === path.relative(h.root, path.join(at, `removal-${h.project.id}.json`)) ||
          [h.project.id, durable, unrelated.id].some(id => file === `mesh/participants/${hash(id)}.json`)) {
        expect(after[file], file).toBe(bytes);
      }
    }
    expect(h.mesh.listAll("actors/", { fresh: true }).filter(entry => [h.project.id, durable, unrelated.id].some(id => entry.key.endsWith(id)))).toEqual(controlState);
    expect(store.records().find(row => row.id === h.project.id)?.rootId).toBe("session:winner");
  });
  it.skipIf(process.platform !== "linux").each(["absent", "expired", "malformed", "host-conflict", "participant-conflict", "scope", "revoked", "disappeared-under-lock", "expired-under-lock"])("F6 refuses ambiguous %s receipt adoption without cleanup", async fault => {
    const h = await fixture(); await h.owner.close();
    const at = h.owner.actorRoots.project; const store = new ActorRegistryStore(at);
    const winner = "session:winner"; const now = Date.now();
    await store.withLock(() => store.write(store.records().map(row => row.id === h.project.id ? {
      ...row, rootId: fault === "scope" ? h.owner.identity.id : winner, residency: fault === "scope" ? "durable" : "session",
    } : row)));
    if (fault !== "absent") writeHostLease(h.mesh.root, { id: winner, rootId: winner, identityId: winner,
      updatedAt: fault === "expired" ? 1 : now, expiresAt: fault === "expired" ? 2 : now + 60_000 });
    if (fault === "malformed") fs.writeFileSync(path.join(h.mesh.root, "host-leases", `${hash(winner).slice(0, 32)}.json`), "{broken");
    if (fault === "host-conflict") await h.mesh.put({ key: `topology/hosts/${hash(winner)}`, identity: h.caller.identity,
      value: { format: 1, id: winner, rootId: winner, identity: h.caller.identity, updatedAt: now, startedAt: 1, expiresAt: now + 60_000 } });
    if (fault === "participant-conflict") await h.mesh.put({ key: `topology/participants/${hash(h.project.id)}`, identity: h.caller.identity,
      value: actorParticipantRecord(h.project, winner, winner, h.caller.identity.id, h.caller.identity.id) });
    const dir = residentRoot(h.mesh.root, h.owner.identity.id); fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, "host.lock"), "");
    fs.writeFileSync(path.join(dir, "main-start.lock"), "");
    fs.writeFileSync(path.join(dir, "prune.json"), JSON.stringify({ format: 1, rootId: h.owner.identity.id, meshRoot: h.mesh.root,
      actors: [{ at, id: h.project.id, residency: "session" }],
      removed: { actors: fault === "revoked" ? [`${at}/${h.project.id}`] : [], files: [], stateKeys: [] } }));
    const original = ActorRegistryStore.prototype.withLock;
    let before = snapshot(h.root);
    if (fault.endsWith("under-lock")) vi.spyOn(ActorRegistryStore.prototype, "withLock").mockImplementation(function (this: ActorRegistryStore, operation) {
      return original.call(this, () => {
        if (fault === "disappeared-under-lock") this.write(this.records().filter(row => row.id !== h.project.id));
        else writeHostLease(h.mesh.root, { id: winner, rootId: winner, identityId: winner, updatedAt: 1, expiresAt: 2 });
        // The injected ownership change is allowed; prune itself must leave all evidence untouched.
        before = snapshot(h.root);
        delete before[path.relative(h.root, path.join(at, "actors.json.lock", "owner"))];
        return operation();
      }) as ReturnType<typeof original>;
    });
    for (const dryRun of [true, false]) {
      const result = await h.run(`return await agents.prune({ root: "session:old-main", dryRun: ${dryRun} });`);
      expect(result.success).toBe(false); expect(result.error).toMatch(/ownership|receipt/i);
      expect(snapshot(h.root)).toEqual(before);
      // This attempt cannot prove adoption once the row vanishes during its lock wait.
      if (fault === "disappeared-under-lock") break;
    }
  });
  it("refuses a recently adopted lineage even before its owner presence appears", async () => {
    const h = await fixture(); await h.owner.close();
    const store = new ActorRegistryStore(h.owner.actorRoots.project);
    await store.withLock(() => store.write(store.records().map(row => ({ ...row, adoptedAt: Date.now() }))));
    const before = snapshot(h.root);
    const result = await h.run('return await agents.prune({ root: "session:old-main" });');
    expect(result.success).toBe(false); expect(result.error).toMatch(/live lineage/i); expect(snapshot(h.root)).toEqual(before);
  });
  it.skipIf(process.platform !== "linux")("keeps adopted foreign presence and append-only work events under an old session key", async () => {
    const h = await fixture(); await h.owner.close();
    const foreign = `actors/old-main/${"f".repeat(32)}`;
    await h.mesh.put({ key: foreign, value: { id: "f".repeat(32), rootId: "session:winner" }, identity: h.caller.identity });
    const event = await h.mesh.publish({ topic: "fleet.work", to: h.owner.identity.id, from: h.caller.identity, text: "Keep audit." });
    const result = await h.run('return await agents.prune({ root: "session:old-main" });');
    expect(result.success, result.error).toBe(true);
    expect(h.mesh.get(foreign, { fresh: true })).toBeDefined();
    expect(h.mesh.read({ topic: "fleet.work", limit: 100 }).map(item => item.id)).toContain(event.id);
  });
  it.skipIf(process.platform !== "linux")("never creates an absent resident host lock when probing existing ownership", async () => {
    const h = await fixture(); await h.owner.close();
    const dir = residentRoot(h.mesh.root, h.owner.identity.id);
    fs.mkdirSync(dir, { recursive: true });
    const file = path.join(dir, "host.lock");
    await expect(lockFile(file, 0, true, false)).rejects.toMatchObject({ code: "ENOENT" });
    expect(fs.existsSync(file)).toBe(false);
  });
  it("refuses a live root and changes nothing", async () => {
    const h = await fixture(); const before = snapshot(h.root);
    const result = await h.run('return await agents.prune({ root: "session:old-main" });');
    expect(result.success).toBe(false); expect(result.error).toMatch(/live lineage/i);
    expect(snapshot(h.root)).toEqual(before);
  });
  it("dryRun returns the dead-root plan without changing files, state or audit", async () => {
    const h = await fixture(); await h.owner.close(); const before = snapshot(h.root);
    const result = await h.run('return await agents.prune({ root: "session:old-main", dryRun: true });');
    expect(result.success, result.error).toBe(true);
    expect((result.value as any).actors).toHaveLength(2); expect((result.value as any).removed.actors).toBe(0);
    expect(snapshot(h.root)).toEqual(before);
  });
  it("refuses the caller's own root even for dryRun", async () => {
    const h = await fixture(); const before = snapshot(h.root);
    for (const dryRun of [false, true]) {
      const result = await h.run(`return await agents.prune({ root: "session:caller", dryRun: ${dryRun} });`);
      expect(result.success).toBe(false); expect(result.error).toMatch(/own root/i);
    }
    expect(snapshot(h.root)).toEqual(before);
  });
  it.each(["main", "resident"])("refuses a live %s file lease without a visible Main", async role => {
    const h = await fixture(); await h.owner.close();
    writeHostLease(h.mesh.root, { id: role === "main" ? h.owner.identity.id : "resident:lease-only", rootId: h.owner.identity.id,
      identityId: "lease-only", updatedAt: Date.now(), expiresAt: Date.now() + 60_000 });
    const before = snapshot(h.root); const result = await h.run('return await agents.prune({ root: "session:old-main" });');
    expect(result.success).toBe(false); expect(result.error).toMatch(/live lineage/i); expect(snapshot(h.root)).toEqual(before);
  });
  it("refuses a live resident process owner even after its lease expired", async () => {
    const h = await fixture(); await h.owner.close(); const at = residentRoot(h.mesh.root, h.owner.identity.id);
    fs.mkdirSync(at, { recursive: true }); fs.writeFileSync(path.join(at, "owner.json"), JSON.stringify({ pid: process.pid, processStartTime: processStartTime(process.pid) }));
    const before = snapshot(h.root); const result = await h.run('return await agents.prune({ root: "session:old-main" });');
    expect(result.success).toBe(false); expect(result.error).toMatch(/live lineage/i); expect(snapshot(h.root)).toEqual(before);
  });
  it.skipIf(process.platform !== "linux")("refuses a held resident host lock with no owner record", async () => {
    const h = await fixture(); await h.owner.close(); const at = residentRoot(h.mesh.root, h.owner.identity.id);
    fs.mkdirSync(at, { recursive: true }); const fd = await lockFile(path.join(at, "host.lock"), 0, true);
    try {
      const before = snapshot(h.root); const result = await h.run('return await agents.prune({ root: "session:old-main" });');
      expect(result.success).toBe(false); expect(result.error).toMatch(/live lineage/i); expect(snapshot(h.root)).toEqual(before);
    } finally { fs.closeSync(fd); }
  });
});
