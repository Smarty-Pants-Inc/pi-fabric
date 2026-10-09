import { beforeEach } from "vitest";
import { installInProcessResidentFence } from "./helpers/in-process-resident-fence.js";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { ActorLogStore } from "../src/actors/log-store.js";
import { ACTOR_RETENTION_BATCH_SIZE } from "../src/actors/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { execFileSync, spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { projectOf, repositoryOf } from "../src/topology/project-identity.js";
import { residentDeliveryPrefix, residentRoot } from "../src/residency/protocol.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";
import { removeParticipantFileIf, writeParticipantFile } from "../src/topology/participant-files.js";
import { LIVENESS_POLICY_KEY } from "../src/topology/host-leases.js";
import { lockFile } from "../src/residency/file-lock.js";
import * as fileLock from "../src/residency/file-lock.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { ResidentHost, sweepResidentRuns } from "../src/residency/host.js";
import { ResidencyClient } from "../src/residency/client.js";
import { ResidentActorClient } from "../src/residency/actor-client.js";
import { ResidentRequestRetention } from "../src/residency/retention.js";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import * as processUtils from "../src/agents/transports/process-utils.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import { RESIDENT_HOST_FORMAT, residentResultPath, type ResidentHostConfig } from "../src/residency/protocol.js";
import { processStartTime, residentProcessAlive } from "../src/residency/process-identity.js";
import { launchLog, same } from "./helpers/owned-processes.js";

beforeEach(() => installInProcessResidentFence());

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const fixture = (retention: Partial<ResidentHostConfig["retention"]> = {}, canonicalRoot = false) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-host-review-"));
  const config: ResidentHostConfig = {
    format: RESIDENT_HOST_FORMAT, rootId: "session:review", sessionId: "review",
    cwd: process.cwd(), projectRoot: process.cwd(), meshRoot: path.join(root, "mesh"),
    actorRoot: path.join(root, "actors"), residencyRoot: canonicalRoot ? residentRoot(path.join(root, "mesh"), "session:review") : path.join(root, "resident"),
    fullCodeMode: true, agents: DEFAULT_FABRIC_CONFIG.agents, mesh: DEFAULT_FABRIC_CONFIG.mesh,
    retention: { ...DEFAULT_FABRIC_CONFIG.retention, ...retention }, workerPath: path.resolve("dist/agents/worker.js"),
    fabricExtensionPath: path.resolve("dist/index.js"), piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
  };
  fs.mkdirSync(config.residencyRoot, { recursive: true });
  const configPath = path.join(config.residencyRoot, "config.json");
  fs.writeFileSync(configPath, JSON.stringify(config));
  const idle = vi.fn();
  const host = new ResidentHost(config, idle);
  return { root, config, host, idle };
};

describe("watchdog custody at host admission", () => {
  it("refuses custody before initialization and releases the acquired host fence", async () => {
    const { root, config, host } = fixture();
    const marker = path.join(config.residencyRoot, "watchdog-custody.json");
    fs.writeFileSync(marker, "uncertain attempt, not a PID lease");
    const successor = new ResidentHost(config, () => {});
    try {
      await expect(host.start()).rejects.toThrow("watchdog custody");
      expect(host.agents).toBeUndefined();
      expect(host.actors).toBeUndefined();
      expect(fs.existsSync(path.join(config.residencyRoot, "owner.json"))).toBe(false);
      expect(fs.existsSync(path.join(config.residencyRoot, "maintenance-ready.json"))).toBe(false);
      // Refusal must not strand our fence. Explicit fixture drain, not PID expiry.
      fs.rmSync(marker);
      await successor.start();
      expect(successor.agents).toBeDefined();
    } finally {
      await host.close(); await successor.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it.skipIf(process.platform === "win32")("does not initialize while a custody publisher owns the transaction lock", async () => {
    const { root, config, host } = fixture();
    const publisher = await lockFile(path.join(config.residencyRoot, "handover.lock"), 0, process.platform === "linux");
    try {
      await expect(host.start()).rejects.toBeInstanceOf(fileLock.FileLockBusy);
      expect(host.agents).toBeUndefined();
      expect(host.actors).toBeUndefined();
      expect(fs.existsSync(path.join(config.residencyRoot, "owner.json"))).toBe(false);
      // The publisher may now commit custody; a subsequent host must observe it.
      fs.writeFileSync(path.join(config.residencyRoot, "watchdog-custody.json"), "retained");
    } finally { fs.closeSync(publisher); }
    const successor = new ResidentHost(config, () => {});
    try {
      await expect(successor.start()).rejects.toThrow("watchdog custody");
      expect(successor.agents).toBeUndefined();
    } finally {
      await host.close(); await successor.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});

describe("fresh startup ownership batches", () => {
  it.each([0, 5_000])("keeps shared state exact and the explicit list TTL un-floored (%i ms)", async ttl => {
    const { root, config, host } = fixture();
    config.mesh = { ...config.mesh, idleReadCoalesceMs: ttl };
    const identity = { id: "session:peer-writer", name: "peer", kind: "main" as const };
    try {
      await host.start();
      expect(host.mesh.readCacheMs).toBe(0);
      expect(host.mesh.backgroundReadCacheMs).toBe(Math.max(1_000, ttl));
      expect(host.participants.options.listReadCacheMs).toBe(ttl);
      const peer = new MeshStore(config.meshRoot, 65536, 100);
      await peer.put({ key: "test/exact-state", value: "before", identity });
      expect(host.mesh.get("test/exact-state")?.value).toBe("before");
      await peer.put({ key: "test/exact-state", value: "after", identity });
      expect(host.mesh.get("test/exact-state")?.value).toBe("after");
      const actor = await host.actors.create({ name: "before", instructions: "idle", residency: "durable" });
      await host.participants.refresh();
      host.participants.list({ scope: "project", fresh: true });
      const key = "topology/participants/" + createHash("sha256").update(actor.id).digest("hex");
      const file = path.join(config.meshRoot, "participants", key.split("/").at(-1)! + ".json");
      const entry = JSON.parse(fs.readFileSync(file, "utf8"));
      entry.updatedAt += 1_000; entry.value.name = "after";
      // A file-only external replacement does not invalidate the reader's process cache.
      fs.writeFileSync(file + ".new", JSON.stringify(entry)); fs.renameSync(file + ".new", file);
      expect(host.participants.list({ scope: "project" }).find(row => row.id === actor.id)?.name)
        .toBe(ttl === 0 ? "after" : "before");
      expect(host.participants.list({ scope: "project", fresh: true }).find(row => row.id === actor.id)?.name).toBe("after");
    } finally {
      await host.close(); fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("a file-only writer between cached batches vetoes prune/manage and the locked registry merge", async () => {
    const { root, config, host } = fixture();
    const peerIdentity = { id: "session:peer-owner", name: "peer", kind: "main" as const };
    const peer = new ParticipantDirectory(new MeshStore(config.meshRoot, 65536, 100), {
      enabled: true, hostId: peerIdentity.id, rootId: config.rootId, identity: peerIdentity,
    });
    const sweeps: Array<() => void> = [];
    const interval = globalThis.setInterval;
    vi.spyOn(globalThis, "setInterval").mockImplementation(((callback: () => void, ms: number, ...args: unknown[]) => {
      if (ms === 15 * 60 * 1000) sweeps.push(callback);
      return interval(callback, ms, ...args);
    }) as typeof setInterval);
    try {
      await host.start(); await peer.start();
      await new Promise<void>((resolve) => setImmediate(resolve));
      const actors: Awaited<ReturnType<typeof host.actors.create>>[] = [];
      for (let i = 0; i < 10; i++) actors.push(await host.actors.create({ name: `owner-${i}`, instructions: "local", residency: "durable" }));
      await host.participants.refresh(); // Publish every actor before priming the reader cache.
      const victim = actors[9]!;
      const expiredRun = path.join(path.dirname(victim.sessionFile!), "runs", "expired");
      fs.mkdirSync(expiredRun, { recursive: true });
      fs.writeFileSync(path.join(expiredRun, "status.json"), JSON.stringify({ status: "completed", transport: "process", sessionId: "2147483646", finishedAt: 1 }));
      const registry = new ActorRegistryStore(config.actorRoot);
      const publishOwner = (target: typeof victim) => {
        const prior = host.participants.list({ scope: "project", fresh: true }).find((record) => record.id === target.id)!;
        expect(prior.ownerHostId).toBe(host.hostId);
        const key = "topology/participants/" + createHash("sha256").update(target.id).digest("hex");
        const entry = { format: 1, key, version: 2, updatedAt: Date.now() + 1000, updatedBy: peerIdentity,
          value: { ...prior, ownerHostId: peerIdentity.id, ownerIdentityId: peerIdentity.id, updatedAt: Date.now() + 1000 } };
        const file = path.join(config.meshRoot, "participants", key.split("/").at(-1)! + ".json");
        // A different process must not invalidate this reader's production 2s cache.
        execFileSync(process.execPath, ["-e", "const fs=require('node:fs');fs.writeFileSync(process.argv[1]+'.new',process.argv[2]);fs.renameSync(process.argv[1]+'.new',process.argv[1]);", file, JSON.stringify(entry)]);
        expect(host.participants.list({ scope: "project" }).find((record) => record.id === target.id)?.ownerHostId).toBe(host.hostId);
        registry.write(registry.records().map((record) => record.id === target.id ? { ...record, instructions: "remote-owner-state" } : record));
      };
      let published = false, writerError: unknown;
      const prune = ActorLogStore.prototype.pruneRuns;
      vi.spyOn(ActorLogStore.prototype, "pruneRuns").mockImplementation(function(this: ActorLogStore, actor, now) {
        prune.call(this, actor, now);
        if (actor.sessionFile === actors[7]!.sessionFile && !published) {
          setImmediate(() => {
            try { publishOwner(victim); published = true; }
            catch (error) { writerError = error; }
          });
        }
      });
      expect(sweeps).toHaveLength(2); for (const sweep of sweeps) sweep();
      const batchSize = ACTOR_RETENTION_BATCH_SIZE[process.platform === "win32" ? "win32" : "other"];
      // Reach every maintenance batch and the writer queued at the batch boundary.
      const maintenanceTurns = Math.ceil(actors.length / batchSize) + 2;
      for (let i = 0; i < maintenanceTurns; i++) await new Promise<void>((resolve) => setImmediate(resolve));
      expect(writerError).toBeUndefined();
      expect(published).toBe(true);
      expect(fs.existsSync(expiredRun)).toBe(true);
      host.actors.resumeQueued();
      expect(host.actors.owns(victim.id)).toBe(false);
      const saveVictim = actors[0]!;
      await host.actors.create({ name: "save-trigger", instructions: "save", residency: "durable" }, {
        asRegistryOwner: true, beforeCommit: () => publishOwner(saveVictim),
      });
      for (const id of [victim.id, saveVictim.id]) {
        expect(registry.records().find((record) => record.id === id)?.instructions).toBe("remote-owner-state");
      }
    } finally {
      vi.restoreAllMocks(); await peer.close(); await host.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});

describe("resident activation filter telemetry", () => {
  it("returns owning-host skips and expiry through the Main's resident status path", async () => {
    const { root, config, host } = fixture({}, true);
    const client = new ResidentActorClient(config.meshRoot, config.rootId);
    const caller = { identity: { id: config.rootId, name: "Main", kind: "main" as const, sessionId: config.sessionId }, hostId: config.rootId };
    const filter = [{ id: "review-claim", topic: ["github.demo"] }];
    const participants = new ParticipantDirectory(new MeshStore(config.meshRoot, 65536, 1000), {
      enabled: true, hostId: caller.hostId, rootId: config.rootId, identity: caller.identity,
    });
    participants.registerSource(() => [{
      format: 1, id: config.rootId, kind: "root", rootId: config.rootId, ownerHostId: caller.hostId, ownerIdentityId: config.rootId,
      name: "Main", status: "idle", residency: "session", runner: "pi", transport: "host", capabilities: [],
      sessionId: config.sessionId, startedAt: Date.now(), updatedAt: Date.now(), controlProtocol: "v1",
    }]);
    try {
      await participants.start();
      await host.start();
      const actor = await host.actors.create({ name: "claimed-review", instructions: "x", residency: "durable", topics: ["github.demo"], coalesceKey: "payload.number" });
      const expiresAt = Date.now() + 30_000;
      await client.setActor({ operation: "setActivationFilter", id: actor.id, activationFilter: filter, expiresAt }, undefined, caller);
      const event = await new MeshStore(config.meshRoot, 65536, 1000).publish({ topic: "github.demo", from: caller.identity, data: { payload: { number: 7 } } });
      const deadline = Date.now() + 5000;
      while (host.actors.status(actor.id).filterSkipped.count !== 1) {
        if (Date.now() > deadline) throw new Error("Resident skip did not arrive");
        await delay(20);
      }
      expect(await client.actorStatus(actor.id)).toMatchObject({
        activationFilterExpiresAt: expiresAt,
        filterSkipped: { count: 1, lastKey: JSON.stringify(["mesh", event.topic, 7]), lastTopic: event.topic, lastAt: expect.any(Number) },
      });
      await client.setActor({ operation: "setActivationFilter", id: actor.id, activationFilter: filter, expiresAt: Date.now() - 1 }, undefined, caller);
      while (host.actors.status(actor.id).activationFilter) {
        if (Date.now() > deadline) throw new Error("Resident expiry did not clear");
        await delay(20);
      }
      expect(await client.actorStatus(actor.id)).toMatchObject({ filterSkipped: { count: 0, lastKey: null, lastTopic: null, lastAt: null } });
      expect(host.actors.messages(actor.id)).toContainEqual(expect.objectContaining({ reason: "activationFilter cleared: expired" }));
    } finally { await host.close(); await participants.close(); fs.rmSync(root, { recursive: true, force: true }); }
  });
});

describe("verified resident supervisor creation (#7452)", () => {
  it("mints a durable binding only for a freshly verified owning Main creator", async () => {
    const { root, config, host } = fixture({}, true);
    const identity = { id: config.rootId, name: "Main", kind: "main" as const, sessionId: config.sessionId };
    const mesh = new MeshStore(config.meshRoot, 65536, 1000);
    const participants = new ParticipantDirectory(mesh, {
      enabled: true, hostId: config.rootId, rootId: config.rootId, identity,
    });
    participants.registerSource(() => [{
      format: 1, id: config.rootId, kind: "root", rootId: config.rootId, ownerHostId: config.rootId, ownerIdentityId: config.rootId,
      name: "Main", status: "idle", residency: "session", runner: "pi", transport: "host", capabilities: [],
      sessionId: config.sessionId, startedAt: Date.now(), updatedAt: Date.now(), controlProtocol: "v1",
    }]);
    const client = new ResidencyClient({ config, mesh, participants, commandTimeoutMs: 5_000,
      mainAgent: { id: config.rootId, local: true } as FabricMainAgentTarget });
    const legacy = new ResidencyClient({ config, mesh, participants, commandTimeoutMs: 5_000,
      mainAgent: { id: config.rootId, local: false } as FabricMainAgentTarget });
    const request = { name: "supervisor", instructions: "Watch Main.", residency: "durable" as const,
      events: ["agent_settled" as const], responseMode: "directive" as const, delivery: "steer" as const, triggerTurn: true };
    try {
      await participants.start(); await host.start();
      const own = await client.createActor(request);
      expect(own.supervisorFor).toBe(config.rootId);
      expect(host.actors.status(own.id).supervisorFor).toBe(config.rootId);
      const unbound = await legacy.createActor({ ...request, name: "legacy-supervisor", supervisorFor: config.rootId } as any);
      expect(unbound.supervisorFor).toBeUndefined();
    } finally {
      await client.close(); await legacy.close(); await host.close(); await participants.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});

describe("resident maintenance readiness attachment", () => {
  it("waits for the published live generation before one accepted create, without another launcher", async () => {
    const { root, config, host } = fixture();
    const mesh = new MeshStore(config.meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents);
    const participants = new ParticipantDirectory(mesh, { enabled: true, hostId: "caller", rootId: config.rootId,
      identity: { id: "caller", name: "caller", kind: "main" } });
    const client = new ResidencyClient({ config, mesh, participants, startupTimeoutMs: 2_000, commandTimeoutMs: 5_000,
      mainAgent: { local: false } as FabricMainAgentTarget });
    let creation: ReturnType<typeof client.createActor> | undefined;
    try {
      await host.start();
      const ownerPath = path.join(config.residencyRoot, "owner.json");
      const owner = fs.readFileSync(ownerPath, "utf8");
      // Simulate temporarily missing visibility for an already-owned generation.
      // Independent attachment still waits without stopping or replacing it.
      const receiptPath = path.join(config.residencyRoot, "maintenance-ready.json");
      const receipt = fs.readFileSync(receiptPath, "utf8");
      fs.rmSync(receiptPath);
      const create = vi.spyOn(host.actors, "create");
      let settled = false;
      creation = client.createActor({ name: "attached-once", instructions: "Watch", residency: "durable" });
      void creation.then(() => { settled = true; }, () => { settled = true; });
      await delay(120);
      expect(settled).toBe(false);
      expect(create).not.toHaveBeenCalled();
      expect(fs.existsSync(path.join(config.residencyRoot, "launcher.log"))).toBe(false);
      expect(fs.readFileSync(ownerPath, "utf8")).toBe(owner);
      fs.writeFileSync(receiptPath, receipt);
      const actor = await creation;
      expect(host.actors.owns(actor.id)).toBe(true);
      expect(participants.get(actor.id)?.ownerHostId).toBe(host.hostId);
      expect(create).toHaveBeenCalledOnce();
      expect(JSON.parse(fs.readFileSync(path.join(config.residencyRoot, "maintenance-ready.json"), "utf8"))).toMatchObject({
        token: JSON.parse(owner).token,
      });
      expect(fs.readFileSync(ownerPath, "utf8")).toBe(owner);
      expect(fs.existsSync(path.join(config.residencyRoot, "launcher.log"))).toBe(false);
    } finally {
      await creation?.catch(() => undefined); await client.close(); await host.close();
      vi.restoreAllMocks(); fs.rmSync(root, { recursive: true, force: true });
    }
  });
});

describe("Astra resident lease delivery fence", () => {
  it("F2 binding resolution loses its lease without tell/sequence delivery, then resumes its owned claim once", async () => {
    const { root, config, host } = fixture();
    let sender: FabricControlPlane | undefined;
    let release!: () => void, entered!: () => void;
    const waiting = new Promise<void>(resolve => { release = resolve; });
    const preparing = new Promise<void>(resolve => { entered = resolve; });
    let healthy = true;
    let result: Promise<unknown> | undefined;
    try {
      await host.start();
      vi.spyOn(host.participants, "canConsumeMesh").mockImplementation(() => healthy);
      vi.spyOn(host.actors, "owns").mockReturnValue(true);
      vi.spyOn(host.actors, "status").mockReturnValue({ rootId: config.rootId } as ReturnType<typeof host.actors.status>);
      vi.spyOn(host.actors, "resolveActivationBinding").mockImplementation(async () => { entered(); await waiting; return {}; });
      const tell = vi.spyOn(host.actors, "tell").mockReturnValue({ messageId: "lease-delivery-once" } as ReturnType<typeof host.actors.tell>);
      const identity = { id: config.rootId, name: "Main", kind: "main" as const, sessionId: config.sessionId };
      sender = new FabricControlPlane(new MeshStore(config.meshRoot, 65536, 1000), identity, { enabled: true, hostId: identity.id, pollMs: 20, acknowledgementTimeoutMs: 5000 });
      sender.start(() => ({ accepted: false }));
      result = sender.request(host.hostId, "actor", "followUp", { message: "accepted work" }).catch(error => error);
      await preparing; healthy = false; release(); await delay(150);
      expect(tell).not.toHaveBeenCalled();
      const seenRoot = path.join(config.meshRoot, "control-seen", createHash("sha256").update(host.hostId).digest("hex").slice(0, 32));
      const seen = new MeshStore(seenRoot, 65536, 1000);
      expect(seen.listAll("topology/control-seen/").every(entry => !(entry.value as { sequence?: number }).sequence)).toBe(true);
      healthy = true;
      expect(await result).toMatchObject({ acknowledged: true, messageId: "lease-delivery-once" });
      expect(tell).toHaveBeenCalledOnce();
    } finally {
      healthy = true; release?.(); await result; await sender?.close();
      vi.restoreAllMocks(); await host.close(); fs.rmSync(root, { recursive: true, force: true });
    }
  }, 15000);
});

describe("#3662 resident actor delivery routing", () => {
  it.each(["read-denied", "stat-denied", "invalid-json", "invalid-envelope", "invalid-participant"] as const)(
    "S1 keeps file-only %s lineage at its mailbox until confirmed withdrawal", async (fault) => {
      const { root, config, host } = fixture();
      const integratorId = "session:11111111-1111-4111-8111-111111111111";
      config.cwd = root;
      config.projectRoot = root;
      config.project = projectOf(root);
      vi.stubEnv("SMARTY_LEAD_SESSION", "");
      fs.mkdirSync(path.join(root, ".local"));
      fs.writeFileSync(path.join(root, ".local", "lead"), integratorId);
      let readFault: ReturnType<typeof vi.spyOn> | undefined;
      let statFault: ReturnType<typeof vi.spyOn> | undefined;
      try {
        await host.start();
        await host.mesh.put({ key: LIVENESS_POLICY_KEY, identity: host.identity, value: { version: 1, participants: "files" } });
        const publishRoot = async (id: string) => {
          const identity = { id, name: "main", kind: "main" as const };
          const hash = createHash("sha256").update(id).digest("hex");
          const key = `topology/participants/${hash}`;
          const participant: FabricParticipantRecord = {
            format: 1, id, rootId: id, kind: "root", ownerHostId: id, ownerIdentityId: id,
            name: "main", status: "idle", runner: "pi", transport: "host", role: "project-agent",
            project: config.project!, cwd: root, capabilities: ["steer", "followUp", "fabric"],
            startedAt: 1, updatedAt: Date.now(), controlProtocol: "v1",
          };
          await host.mesh.put({ key: `topology/hosts/${hash}`, identity, value: {
            format: 1, id, rootId: id, identity, startedAt: 1, updatedAt: Date.now(), expiresAt: Date.now() + 120_000,
          } });
          const entry = { key, value: participant, version: 1, updatedAt: Date.now(), updatedBy: identity };
          writeParticipantFile(config.meshRoot, entry);
          return { key, participant, file: path.join(config.meshRoot, "participants", `${hash}.json`), text: JSON.stringify({ format: 1, ...entry }) };
        };
        await publishRoot(integratorId);
        const original = await publishRoot(config.rootId);
        // No lookup warmed the root slot before the fault. Denials persist through every retry.
        if (fault === "invalid-json") fs.writeFileSync(original.file, "{torn");
        if (fault === "invalid-envelope") fs.writeFileSync(original.file, JSON.stringify({ format: 99 }));
        if (fault === "invalid-participant") fs.writeFileSync(original.file, JSON.stringify({
          format: 1, key: original.key, value: { ...original.participant, kind: "invalid" },
          version: 1, updatedAt: Date.now(), updatedBy: host.identity,
        }));
        if (fault === "read-denied") {
          const read = fs.readFileSync;
          readFault = vi.spyOn(fs, "readFileSync").mockImplementation((...args: Parameters<typeof fs.readFileSync>) => {
            if (String(args[0]) === original.file) throw Object.assign(new Error("denied"), { code: "EACCES" });
            return read(...args);
          });
        }
        if (fault === "stat-denied") {
          const stat = fs.statSync;
          statFault = vi.spyOn(fs, "statSync").mockImplementation((...args: Parameters<typeof fs.statSync>) => {
            if (String(args[0]) === original.file) throw Object.assign(new Error("denied"), { code: "EACCES" });
            return stat(...args);
          });
        }
        expect(host.mesh.get(original.key, { fresh: true })).toBeUndefined();
        expect(host.participants.get(config.rootId, Date.now(), { fresh: true })).toBeUndefined();
        expect(host.participants.lastKnown(config.rootId)).toBeUndefined();
        expect(host.participants.get(integratorId, Date.now(), { fresh: true })?.id).toBe(integratorId);
        const send = async (text: string, total: number) => {
          host.actors.onDeliver({
            actor: { id: "actor:supervisor", name: "supervisor", project: config.project } as Parameters<typeof host.actors.onDeliver>[0]["actor"],
            message: { id: text, actorId: "actor:supervisor", actorName: "supervisor", direction: "out", source: "actor", createdAt: Date.now(), text },
            delivery: "steer", triggerTurn: true,
          });
          await vi.waitFor(() => expect(host.mesh.listAll("residency/deliveries/", { fresh: true })).toHaveLength(total));
        };
        await send("while unknown", 1);
        expect(host.mesh.listAll(residentDeliveryPrefix(config.rootId))).toHaveLength(1);
        expect(host.mesh.listAll(residentDeliveryPrefix(integratorId))).toHaveLength(0);
        readFault?.mockRestore();
        statFault?.mockRestore();
        fs.writeFileSync(original.file, original.text);
        await send("after repair", 2);
        expect(host.mesh.listAll(residentDeliveryPrefix(config.rootId))).toHaveLength(2);
        expect(host.mesh.listAll(residentDeliveryPrefix(integratorId))).toHaveLength(0);
        // Confirmed withdrawal still does not permit actor-output succession: the owner root
        // remains the only custody address, even when its recorded lineage is closed.
        expect(await removeParticipantFileIf(host.mesh, original.key, () => true)).toBe(true);
        expect(host.participants.lineageAlive(config.rootId)).toBe(true); // Removal is not positive proof.
        await host.mesh.put({
          key: `topology/lineage-closures/${createHash("sha256").update(config.rootId).digest("hex")}`,
          identity: { id: config.rootId, name: "main", kind: "main" },
          value: { format: 1, rootId: config.rootId, ownerHostId: config.rootId, ownerIdentityId: config.rootId, closedAt: Date.now() },
        });
        expect(host.participants.lineageAlive(config.rootId)).toBe(false);
        await send("after withdrawal", 3);
        expect(host.mesh.listAll(residentDeliveryPrefix(config.rootId))).toHaveLength(3);
        expect(host.mesh.listAll(residentDeliveryPrefix(integratorId))).toHaveLength(0);
      } finally {
        readFault?.mockRestore();
        statFault?.mockRestore();
        await host.close();
        vi.unstubAllEnvs();
        fs.rmSync(root, { recursive: true, force: true });
      }
    },
  );
  it.each([
    ["expired lease with live lineage", true, true, "root"],
    ["dead root without bound integrator", false, false, "root"],
    ["dead root with exact bound integrator", false, true, "root"],
    ["dead root with actor repository different from host cwd", false, true, "root"],
  ] as const)("keeps %s at the exact actor-owner mailbox", async (_case, rootPresent, bound, target) => {
    const { root, config, host } = fixture();
    const integratorId = "session:11111111-1111-4111-8111-111111111111";
    config.cwd = root;
    config.projectRoot = root;
    let actorProject = root;
    if (_case === "dead root with actor repository different from host cwd") {
      actorProject = path.join(root, "actor-project");
      fs.mkdirSync(actorProject);
      for (const [cwd, origin] of [[root, "https://forge.test/team/host.git"], [actorProject, "https://forge.test/team/actors.git"]]) {
        execFileSync("git", ["init", "-q"], { cwd, stdio: "ignore" });
        execFileSync("git", ["config", "remote.origin.url", origin!], { cwd, stdio: "ignore" });
      }
    }
    const project = projectOf(actorProject);
    const repository = repositoryOf(project);
    config.project = project;
    vi.stubEnv("SMARTY_LEAD_SESSION", "");
    if (bound) {
      fs.mkdirSync(path.join(root, ".local"));
      fs.writeFileSync(path.join(root, ".local", "lead"), integratorId);
    }
    try {
      await host.start();
      const publishRoot = async (id: string, startedAt: number, expiresAt: number) => {
        const identity = { id, name: "main", kind: "main" as const };
        const participant: FabricParticipantRecord = {
          format: 1, id, rootId: id, kind: "root", ownerHostId: id, ownerIdentityId: id,
          name: "main", status: "idle", runner: "pi", transport: "host", role: "project-agent",
          project, ...(repository ? { repository } : {}), cwd: root, capabilities: ["steer", "followUp", "fabric"],
          startedAt, updatedAt: Date.now(), controlProtocol: "v1",
        };
        const key = (prefix: string) => prefix + createHash("sha256").update(id).digest("hex");
        await host.mesh.put({ key: key("topology/hosts/"), identity, value: {
          format: 1, id, rootId: id, identity, startedAt, updatedAt: Date.now(), expiresAt,
        } });
        await host.mesh.put({ key: key("topology/participants/"), identity, value: participant });
      };
      if (rootPresent) await publishRoot(config.rootId, 1, Date.now() - 60_000);
      else await host.mesh.put({
        key: `topology/lineage-closures/${createHash("sha256").update(config.rootId).digest("hex")}`,
        identity: { id: config.rootId, name: "main", kind: "main" },
        value: { format: 1, rootId: config.rootId, ownerHostId: config.rootId, ownerIdentityId: config.rootId, closedAt: Date.now() },
      });
      if (bound) await publishRoot(integratorId, rootPresent ? 99 : 2, Date.now() + 120_000);
      if (!rootPresent) await publishRoot("session:newer-project-agent", 99, Date.now() + 120_000);
      const liveRoots = host.participants.list({ scope: "project", kinds: ["root"], fresh: true });
      expect(liveRoots.some(candidate => candidate.id === config.rootId)).toBe(false);
      if (rootPresent) expect(host.participants.lastKnown(config.rootId)?.participant.stale).toBe(true);
      host.actors.onDeliver({
        actor: { id: "actor:supervisor", name: "supervisor", project: config.project } as Parameters<typeof host.actors.onDeliver>[0]["actor"],
        message: { id: "message", actorId: "actor:supervisor", actorName: "supervisor", direction: "out", source: "actor", createdAt: Date.now(), text: "directive" },
        delivery: "steer", triggerTurn: true,
      });
      await vi.waitFor(() => expect(host.mesh.listAll("residency/deliveries/").length).toBe(1));
      const expected = config.rootId;
      const deliveries = host.mesh.listAll(residentDeliveryPrefix(expected));
      expect(deliveries).toHaveLength(1);
      expect(deliveries[0]?.value).toMatchObject({ rootId: expected, message: "directive", delivery: "steer" });
      expect(host.mesh.listAll(residentDeliveryPrefix("session:newer-project-agent"))).toHaveLength(0);
    } finally {
      await host.close();
      vi.unstubAllEnvs();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});
const RESIDENT_RUN_RETENTION_MS = 24 * 60 * 60 * 1_000;
describe("#2566 startup fence", () => {
  it.skipIf(process.platform !== "linux")("item 1 fails closed when an injected non-Linux start has no shared flock", async () => {
    const { root, host } = fixture();
    const platform = vi.spyOn(process, "platform", "get").mockReturnValue("darwin");
    const unavailable = vi.spyOn(fileLock, "lockFile").mockRejectedValue(Object.assign(new Error("flock unavailable"), { code: "ENOENT" }));
    try {
      await expect(host.start()).rejects.toThrow("flock unavailable");
      expect(host.actors).toBeUndefined();
    } finally { unavailable.mockRestore(); platform.mockRestore(); await host.close(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it.skipIf(process.platform !== "linux")("item 2 rejects an inode replaced by an already-committed legacy reclaimer during acquisition", async () => {
    const { root, config, host } = fixture();
    const lock = path.join(config.residencyRoot, "host.lock");
    const acquire = fileLock.lockFile;
    const replacement = vi.spyOn(fileLock, "lockFile").mockImplementation(async (...args) => {
      const fd = await acquire(...args);
      if (args[0] === lock) {
        fs.renameSync(lock, `${lock}.legacy-inode`);
        fs.writeFileSync(lock, "");
      }
      return fd;
    });
    try {
      await expect(host.start()).rejects.toThrow(/replaced|drain/);
      expect(host.actors).toBeUndefined();
      expect(fs.readFileSync(lock, "utf8")).toBe("");
      expect(fs.existsSync(path.join(config.residencyRoot, "owner.json"))).toBe(false);
    } finally { replacement.mockRestore(); await host.close(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it.skipIf(process.platform !== "linux")("item 1 fences an injected non-Linux delayed starter with stale diagnostics", async () => {
    const { root, config, host: first } = fixture();
    const second = new ResidentHost(config);
    const lock = path.join(config.residencyRoot, "host.lock");
    const read = fs.readFileSync.bind(fs);
    let delayed = false;
    const stale = JSON.stringify({ pid: -1, token: "stale" });
    const platform = vi.spyOn(process, "platform", "get").mockReturnValue("darwin");
    const reads = vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, options?: unknown) => {
      if (delayed && String(file) === lock) return stale;
      return read(file, options as never);
    }) as typeof fs.readFileSync);
    const control = vi.spyOn(FabricControlPlane.prototype, "start");
    try {
      const starting = first.start();
      delayed = true;
      const following = second.start();
      delayed = false;
      const results = await Promise.allSettled([starting, following]);
      expect(results.filter((result) => result.status === "fulfilled")).toHaveLength(1);
      expect(control).toHaveBeenCalledTimes(1);
      const loser = results[0]!.status === "rejected" ? first : second;
      expect(loser.actors).toBeUndefined();
      reads.mockRestore();
      const inode = fs.statSync(lock).ino;
      await Promise.all([first.close(), second.close()]);
      expect(fs.statSync(lock).ino).toBe(inode);
    } finally {
      reads.mockRestore(); platform.mockRestore(); control.mockRestore();
      await Promise.all([first.close(), second.close()]);
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it.skipIf(process.platform !== "linux").each(["", "{", "null", JSON.stringify({ pid: -1, token: "legacy-dead" })])(
    "item 2 refuses an unproven legacy startup inode (%j) without overwriting it", async (record) => {
      const { root, config, host } = fixture();
      const lock = path.join(config.residencyRoot, "host.lock");
      fs.writeFileSync(lock, record);
      const inode = fs.statSync(lock).ino;
      try {
        await expect(host.start()).rejects.toThrow(/legacy|uncertain|drain/i);
        expect(host.actors).toBeUndefined();
        expect(fs.statSync(lock).ino).toBe(inode);
        expect(fs.readFileSync(lock, "utf8")).toBe(record);
        // A legacy starter already committed to unlinking this dead record can
        // now replace it; the new host must not have initialized alongside it.
        fs.rmSync(lock);
        fs.writeFileSync(lock, JSON.stringify({ pid: process.pid, token: "legacy-reclaimer" }));
        expect(host.actors).toBeUndefined();
      } finally { await host.close(); fs.rmSync(root, { recursive: true, force: true }); }
    },
  );
});

describe.skipIf(process.platform !== "linux")("#2566 owned startup shutdown", () => {
  it("item 5 retains the exact launcher after failed verification and surfaces close cleanup failure", async () => {
    const { root, config, host } = fixture();
    const stop = vi.fn<() => Promise<void>>().mockRejectedValue(new Error("owned process exit unconfirmed"));
    const spawn = vi.spyOn(processUtils, "spawnDetached").mockResolvedValue({ pid: process.pid, closed: new Promise<void>(() => {}), stop, isAlive: async () => true, lostContact: () => undefined, waitForClose: async () => {} });
    const client = new ResidencyClient({ config, mesh: host.mesh, participants: host.participants,
      mainAgent: { local: false } as FabricMainAgentTarget, startupTimeoutMs: 20 });
    try {
      await expect(client.ensureHost()).rejects.toThrow("owned process exit unconfirmed");
      expect(stop).toHaveBeenCalledTimes(1);
      await expect(client.close()).rejects.toThrow("owned process exit unconfirmed");
      expect(stop).toHaveBeenCalledTimes(2);
      stop.mockResolvedValue(undefined);
      await client.close();
      expect(stop).toHaveBeenCalledTimes(3);
      expect(spawn).toHaveBeenCalledTimes(1);
    } finally {
      stop.mockResolvedValue(undefined); await client.close(); spawn.mockRestore();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it.each(["timeout", "close", "failure"] as const)("item 5 confirms launcher and refusing Pi group exit before reporting %s", async (mode) => {
    const { root, config, host } = fixture();
    const launches = launchLog(root);
    for (const [key, value] of Object.entries(launches.env)) vi.stubEnv(key, value);
    const client = new ResidencyClient({
      config, mesh: host.mesh, participants: host.participants,
      mainAgent: { local: false } as FabricMainAgentTarget,
      hostPath: path.resolve("tests/fixtures/stubborn-resident-launcher.mjs"),
      startupTimeoutMs: mode === "timeout" ? 400 : 10_000,
    });
    const executing = () => launches.owned().filter((owned) => {
      if (!same(owned)) return false;
      try {
        const stat = fs.readFileSync(`/proc/${owned.pid}/stat`, "utf8");
        return !["Z", "X"].includes(stat.slice(stat.lastIndexOf(")") + 2).split(" ")[0]!);
      } catch { return false; }
    });
    // Attach the rejection observer immediately, including when close races startup.
    const starting = client.ensureHost().then(() => undefined, (error: unknown) => error);
    try {
      await vi.waitFor(() => expect(fs.existsSync(path.join(config.residencyRoot, "stubborn-child-ready"))).toBe(true), { timeout: 5_000 });
      expect(executing()).toHaveLength(2);
      if (mode === "close") await client.close();
      if (mode === "failure") fs.writeFileSync(path.join(config.residencyRoot, "error.json"), JSON.stringify({ error: "injected failure" }));
      const error = await starting;
      expect(error).toBeInstanceOf(Error);
      expect(String(error)).toMatch(mode === "timeout" ? /Timed out/ : mode === "close" ? /client is closed/ : /injected failure/);
      // No post-rejection grace period: settlement itself is the execution boundary.
      expect(executing()).toEqual([]);
    } finally {
      // Baseline's fire-and-forget TERM leaves both alive. Kill ONLY launch-log
      // identities from this fixture, with a fresh birth check before each signal.
      for (const owned of executing()) if (same(owned)) {
        try { process.kill(owned.pid, "SIGKILL"); } catch { /* already gone */ }
      }
      await starting;
      await client.close();
      await vi.waitFor(() => expect(executing()).toEqual([]), { timeout: 5_000 });
      vi.unstubAllEnvs();
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 15_000);
});

describe("resident loaded-path census metadata", () => {
  it("publishes the startup generation rather than a later desired configuration", async () => {
    const { root, config, host } = fixture();
    const loaded = config.fabricExtensionPath;
    try {
      await host.start();
      const ownerPath = path.join(config.residencyRoot, "owner.json");
      const owner = fs.readFileSync(ownerPath, "utf8");
      expect(JSON.parse(owner)).toMatchObject({ pid: process.pid, fabricExtensionPath: loaded });
      fs.writeFileSync(path.join(config.residencyRoot, "config.json"), JSON.stringify({
        ...config, fabricExtensionPath: path.join(root, "replacement", "dist", "index.js"),
      }));
      expect(fs.readFileSync(ownerPath, "utf8")).toBe(owner);
      expect(host.config.fabricExtensionPath).toBe(loaded);
    } finally {
      await host.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});

describe("resident retention config reload", () => {
  it.each([
    { change: "age threshold", initial: { terminalRunEventsAgeMs: 12 * 60 * 60 * 1000, terminalRunEventsMaxBytes: 128 * 1024 },
      reloaded: { terminalRunEventsAgeMs: 6 * 60 * 60 * 1000 } },
    { change: "byte cap", initial: { terminalRunEventsMaxBytes: 512 * 1024 },
      reloaded: { terminalRunEventsMaxBytes: 128 * 1024 } },
  ].flatMap(policy => [0, 6].map(recoveryMs => ({ ...policy, recoveryMs }))))("applies a same-release client reload to an already-running host's next sweep without replacing its owner ($change, recovery=$recoveryMs ms)", async ({ initial, reloaded, recoveryMs }) => {
    // The streamed first sweep must genuinely retain this fixture: unlike the
    // former startup sweep, it runs after the log is created. Set the initial
    // policy before constructing the host, which takes its own shared copy.
    const { root, config, host } = fixture(initial);
    const initialRetention = { ...config.retention };
    let client: ResidencyClient | undefined;
    let elapsed = 0;
    if (process.platform === "win32") {
      // This is a policy-reload test, not a native filesystem throughput test.
      // A cold Windows status read can exhaust the 2-ms reference-proof slice
      // and keep a protective live hint for this safely terminal fixture. Freeze
      // the monotonic clock from the first scan; only injected recovery below
      // advances it. Date.now/finishedAt and the host poll stay real; the 2-ms
      // reference and 5-ms sweep budget values are unchanged.
      vi.spyOn(performance, "now").mockImplementation(() => elapsed);
    }
    try {
      await host.start();
      const ownerPath = path.join(config.residencyRoot, "owner.json");
      const owner = fs.readFileSync(ownerPath, "utf8");
      const run = path.join(config.residencyRoot, "runs", "reload-retention");
      fs.mkdirSync(run, { recursive: true });
      // Stay below the 200-line tail limit so the byte-cap case tests only bytes.
      const log = Buffer.from((JSON.stringify({ text: "x".repeat(3000) }) + "\n").repeat(100));
      const status = JSON.stringify({ status: "completed", transport: "process", sessionId: "2147483647",
        finishedAt: Date.now() - 8 * 60 * 60 * 1000 });
      fs.writeFileSync(path.join(run, "status.json"), status);
      fs.writeFileSync(path.join(run, "events.jsonl"), log);
      fs.writeFileSync(path.join(run, "reply.json"), '{"text":"keep"}');
      // Observe a completed production scan, not merely elapsed wall time.
      await vi.waitFor(() => expect(fs.existsSync(path.join(config.residencyRoot, "request-retention.json"))).toBe(true));
      expect(fs.readFileSync(path.join(run, "events.jsonl"))).toEqual(log);
      // The real reload creates a new client; ensureHost publishes desired config and reuses the owner.
      // Each case changes only one threshold, making stale policy observable.
      const next = { ...config, retention: { ...config.retention, ...reloaded } };
      client = new ResidencyClient({ config: next, mesh: host.mesh, participants: host.participants,
        mainAgent: { local: false } as FabricMainAgentTarget });
      expect((await client.ensureHost()).pid).toBe(process.pid);
      // Model consistently slow Windows filesystem recovery separately from age:
      // no wall-clock sleep, age/mtime adjustment, open handle, or rename failure.
      // Keep the native host poll and the unchanged production 5-ms slice.
      if (recoveryMs) {
        if (process.platform !== "win32") {
          const nativeNow = performance.now.bind(performance);
          vi.spyOn(performance, "now").mockImplementation(() => nativeNow() + elapsed);
        }
        const recover = host.agents.recoverPendingArchives.bind(host.agents);
        vi.spyOn(host.agents, "recoverPendingArchives").mockImplementation((...args) => {
          const result = recover(...args);
          elapsed += recoveryMs;
          return result;
        });
      }
      // Advance only the sample clock; keep the real host poll and its production 5-ms transaction.
      const due = vi.spyOn(ResidentRequestRetention.prototype, "due").mockReturnValue(true);
      const nativeSweep = ResidentRequestRetention.prototype.sweep;
      const policies: Array<ResidentRequestRetention["retention"]> = [];
      const sweep = vi.spyOn(ResidentRequestRetention.prototype, "sweep").mockImplementation(function (this: ResidentRequestRetention, now, ...args) {
        policies.push({ ...this.retention });
        return nativeSweep.call(this, now + 60_001, ...args);
      });
      const deadline = Date.now() + 2500;
      while (fs.statSync(path.join(run, "events.jsonl")).size > 128 * 1024 && Date.now() < deadline) await delay(10);
      expect(sweep).toHaveBeenCalled();
      expect(sweep.mock.calls.every(call => call[2] === 5)).toBe(true);
      expect(policies).toContainEqual({ ...next.retention, retainRuns: config.agents.retainRuns });
      due.mockRestore(); sweep.mockRestore();
      expect(fs.statSync(path.join(run, "events.jsonl")).size).toBeLessThanOrEqual(128 * 1024);
      expect(config.retention).toEqual(initialRetention);
      expect(fs.readFileSync(path.join(run, "status.json"), "utf8")).toBe(status);
      expect(fs.readFileSync(path.join(run, "reply.json"), "utf8")).toBe('{"text":"keep"}');
      expect(fs.readFileSync(ownerPath, "utf8")).toBe(owner);
      expect(DEFAULT_FABRIC_CONFIG.retention.terminalRunEventsAgeMs).toBe(6 * 60 * 60 * 1000);
      expect(DEFAULT_FABRIC_CONFIG.retention.terminalRunEventsMaxBytes).toBe(256 * 1024);
    } finally {
      vi.restoreAllMocks(); await client?.close(); await host.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});

describe("resident tracked result preservation", () => {
  it.each(["native", "win32-injected"] as const)("releases the host fence and closes delivery/participant work even if agent close fails (%s)", async (platformCase) => {
    const { root, config, host } = fixture();
    const successor = new ResidentHost(config);
    const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
    const getuid = Object.getOwnPropertyDescriptor(process, "getuid");
    let fault: ReturnType<typeof vi.spyOn> | undefined;
    try {
      if (platformCase === "win32-injected") {
        Object.defineProperty(process, "platform", { ...platform, value: "win32" });
        Object.defineProperty(process, "getuid", { configurable: true, writable: true, value: undefined });
        // Exercise the product Windows exclusive-create claim, not the POSIX adapter.
      }
      await host.start();
      await expect(successor.start()).rejects.toThrow(/already running/);
      expect(successor.actors).toBeUndefined();
      const closeAgents = host.agents.close.bind(host.agents);
      fault = vi.spyOn(host.agents, "close").mockImplementation(async () => {
        await closeAgents();
        throw new Error("fixture agent close failure");
      });
      const closeParticipants = vi.spyOn(host.participants, "close");
      await expect(host.close()).rejects.toThrow("fixture agent close failure");
      expect(closeParticipants).toHaveBeenCalledOnce();
      expect(fs.existsSync(path.join(config.residencyRoot, "owner.json"))).toBe(false);
      const lock = path.join(config.residencyRoot, "host.lock");
      const windows = process.platform === "win32";
      expect(fs.existsSync(lock)).toBe(!windows);
      if (!windows) {
        const fd = await lockFile(lock, 0);
        fs.closeSync(fd);
      }
      // Both kernel and exclusive-create claims admit a successor only after close.
      await successor.start();
      expect(JSON.parse(fs.readFileSync(path.join(config.residencyRoot, "owner.json"), "utf8")).pid).toBe(process.pid);
      await successor.close();
      expect(fs.existsSync(path.join(config.residencyRoot, "owner.json"))).toBe(false);
      expect(fs.existsSync(lock)).toBe(!windows);
      closeParticipants.mockRestore();
    } finally {
      fault?.mockRestore();
      try {
        await host.close();
        await successor.close();
      } finally {
        Object.defineProperty(process, "platform", platform);
        if (getuid) Object.defineProperty(process, "getuid", getuid);
        else Reflect.deleteProperty(process, "getuid");
        fs.rmSync(root, { recursive: true, force: true });
      }
    }
  });
  it.each(["LARGE_RESULT", "FAIL_DIRECTIVE"])("F1 save failure keeps the worker's %s completion through close and two host/client restarts", async (task) => {
    const { root, config, host: first } = fixture();
    config.workerPath = path.resolve("tests/fixtures/fake-worker.mjs");
    config.agents = { ...config.agents, retainRuns: false, budgetUsd: 0 };
    config.piModels = { available: [{ provider: "fixture", id: "visible" }], aliases: {}, defaultModel: "fixture/visible" };
    let host = first;
    let client: ResidencyClient | undefined;
    let obstructed: string | undefined;
    const launch = ProcessTransport.prototype.launch;
    const fault = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, request) {
      // Use the real id before the child can finish; atomic rename onto a directory must fail.
      obstructed ??= residentResultPath(config.residencyRoot, request.id);
      fs.mkdirSync(obstructed, { recursive: true });
      return launch.call(this, request);
    });
    // A public spawn is made by a real session caller, never by the hidden resident executor.
    const callerIdentity = { id: config.rootId, name: "Main", kind: "main" as const, sessionId: config.sessionId };
    const callerMesh = new MeshStore(config.meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents);
    const callerParticipants = new ParticipantDirectory(callerMesh, {
      enabled: true, hostId: config.rootId, rootId: config.rootId, identity: callerIdentity,
    });
    callerParticipants.registerSource(() => [{
      format: 1, id: config.rootId, rootId: config.rootId, kind: "root", name: "Main", status: "idle",
      ownerHostId: config.rootId, ownerIdentityId: config.rootId, sessionId: config.sessionId,
      runner: "pi", transport: "host", capabilities: ["fabric"], controlProtocol: "v1",
      startedAt: Date.now(), updatedAt: Date.now(),
    }]);
    const connect = () => new ResidencyClient({
      config, mesh: callerMesh, participants: callerParticipants,
      mainAgent: { local: false } as FabricMainAgentTarget,
    });
    try {
      await callerParticipants.start();
      await host.start();
      client = connect();
      const handle = await client.spawnAgent({ task, transport: "process", residency: "durable" }, AbortSignal.timeout(5_000));
      fault.mockRestore();
      const original = await host.agents.wait(handle.id, { timeoutMs: 5_000 });
      expect(original.status).toBe(task === "FAIL_DIRECTIVE" ? "failed" : "completed");
      expect(original.text).toBe(task === "LARGE_RESULT" ? "x".repeat(100_000) : "fake worker complete");
      const run = host.agents.runDirectory(handle.id)!;
      const worker = fs.readFileSync(path.join(run, "status.json"), "utf8");
      expect(JSON.parse(worker)).toMatchObject({ status: original.status, text: original.text });
      expect(original.error).toBe(JSON.parse(worker).error);
      expect(fs.statSync(obstructed!).isDirectory()).toBe(true); // no authoritative saved result
      // The worker file alone cannot certify settlement while its pinned supervisor lives.
      await expect(client.waitAgent(handle.id, AbortSignal.timeout(100))).rejects.toThrow("aborted");
      // Counterexample: an unobstructed public completion must not pin all tracked runs.
      const saved = await client.spawnAgent({ task: "saved normally", transport: "process", residency: "durable" }, AbortSignal.timeout(5_000));
      await host.agents.wait(saved.id, { timeoutMs: 5_000 });
      const savedRun = host.agents.runDirectory(saved.id)!;
      expect(JSON.parse(fs.readFileSync(residentResultPath(config.residencyRoot, saved.id), "utf8")).status).toBe("completed");
      await client.close();
      await host.close();
      expect(fs.existsSync(savedRun)).toBe(false);
      expect(fs.existsSync(run), "failed save must not delete the only completion").toBe(true);
      expect(fs.readFileSync(path.join(run, "status.json"), "utf8")).toBe(worker);
      // Production restarts exit the supervisor process. These embedded hosts share the
      // still-live test PID, so explicitly model the predecessor's exit in its launch pin.
      // A new host does not inherit the predecessor's authority to retry this run.
      const manifestPath = path.join(run, "completion-recipient.json");
      const manifest = JSON.parse(fs.readFileSync(manifestPath, "utf8"));
      expect(manifest.supervisor.pid).toBe(process.pid);
      manifest.supervisor = { pid: 2147483647 };
      fs.writeFileSync(manifestPath, JSON.stringify(manifest));
      const expected = { id: handle.id, status: original.status, text: original.text, residency: "durable",
        ...(original.error === undefined ? {} : { error: original.error }) };
      for (let restart = 1; restart <= 2; restart++) {
        host = new ResidentHost(config);
        await host.start();
        client = connect();
        expect(client.statusAgent(handle.id), `restart ${restart}`).toMatchObject(expected);
        expect(await client.waitAgent(handle.id, AbortSignal.timeout(2_000))).toMatchObject(expected);
        expect(fs.readFileSync(path.join(run, "status.json"), "utf8")).toBe(worker);
        expect(fs.statSync(obstructed!).isDirectory()).toBe(true);
        await client.close();
        await host.close();
      }
    } finally {
      fault.mockRestore();
      await client?.close();
      await host.close();
      await callerParticipants.close();
      fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
    }
  }, 20_000);
});

describe("resident orphan retention", () => {
  it("F1 retains a public task's only completion until a valid saved terminal result authorizes collection", async () => {
    const { root, config, host } = fixture();
    const id = "a".repeat(32);
    const runs = path.join(config.residencyRoot, "runs");
    const run = path.join(runs, id);
    const result = {
      id, name: "orphaned public task", task: "work", status: "completed", text: "original completion",
      // Already-exited orphan: persist a usable PID whose absence is checked below.
      runner: "pi", transport: "process", sessionId: "2147483647", cwd: config.cwd, startedAt: 1, updatedAt: 2, finishedAt: 2,
      turns: 1, toolCalls: 0, usage: { input: 1, output: 2, cacheRead: 0, cacheWrite: 0, cost: 0 },
    };
    expect(() => process.kill(Number(result.sessionId), 0)).toThrow(expect.objectContaining({ code: "ESRCH" }));
    fs.mkdirSync(run, { recursive: true });
    fs.writeFileSync(path.join(run, "status.json"), JSON.stringify(result));
    const metadataPath = path.join(config.residencyRoot, "agents", `${id}.json`);
    fs.mkdirSync(path.dirname(metadataPath), { recursive: true });
    fs.writeFileSync(metadataPath, JSON.stringify({
      format: RESIDENT_HOST_FORMAT, rootId: config.rootId, id, runDirectory: run,
      handle: { ...result, status: "running", text: "", residency: "durable" }, createdAt: 1, updatedAt: 1,
    }));
    const expiredAt = Date.now() - RESIDENT_RUN_RETENTION_MS - 60_000;
    fs.utimesSync(run, expiredAt / 1_000, expiredAt / 1_000);
    const resultPath = residentResultPath(config.residencyRoot, id);
    try {
      // Lost host: the detached worker wrote status.json, but onSettled never saved results/id.
      await host.start();
      expect(fs.existsSync(run)).toBe(true);
      expect(fs.existsSync(resultPath)).toBe(false);
      fs.mkdirSync(path.dirname(resultPath), { recursive: true });
      for (const malformed of [
        "{", "null", "[]", JSON.stringify({ ...result, id: "b".repeat(32) }),
        JSON.stringify({ ...result, status: "running" }),
        JSON.stringify({ id, status: "completed" }),
        JSON.stringify({ ...result, text: null }),
        JSON.stringify({ ...result, usage: { ...result.usage, output: "invalid" } }),
      ]) {
        fs.writeFileSync(resultPath, malformed);
        expect(sweepResidentRuns(runs), malformed).toEqual([]);
        expect(fs.existsSync(run), malformed).toBe(true);
        expect(JSON.parse(fs.readFileSync(path.join(run, "status.json"), "utf8"))).toEqual(result);
      }
      // A saved result cannot turn uncertain public metadata into permission to delete.
      fs.writeFileSync(resultPath, JSON.stringify(result));
      const metadata = fs.readFileSync(metadataPath, "utf8");
      const parsedMetadata = JSON.parse(metadata);
      for (const malformed of [
        "{", "null", "[]", JSON.stringify({ id }),
        JSON.stringify({ ...parsedMetadata, handle: null }),
        JSON.stringify({ ...parsedMetadata, runDirectory: path.join(runs, "unrelated") }),
      ]) {
        fs.writeFileSync(metadataPath, malformed);
        expect(sweepResidentRuns(runs), malformed).toEqual([]);
        expect(fs.existsSync(run), malformed).toBe(true);
        expect(JSON.parse(fs.readFileSync(path.join(run, "status.json"), "utf8"))).toEqual(result);
      }
      fs.writeFileSync(metadataPath, metadata);
      // Counterexample: this SAME public task becomes collectable once its authoritative copy exists.
      expect(sweepResidentRuns(runs)).toEqual([run]);
      expect(fs.existsSync(run)).toBe(false);
      expect(JSON.parse(fs.readFileSync(resultPath, "utf8"))).toEqual(result);
      expect(fs.existsSync(metadataPath)).toBe(true);
    } finally { await host.close(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it("F6 sweeps old terminal untracked runs after lease publication, preserving recent/live/unknown/unresolved runs", async () => {
    const { root, config, host } = fixture();
    const runs = path.join(config.residencyRoot, "runs");
    const now = Date.now();
    const make = (name: string, status: Record<string, unknown>, age = RESIDENT_RUN_RETENTION_MS + 60_000) => {
      const run = path.join(runs, name);
      fs.mkdirSync(run, { recursive: true });
      // Positive fixtures need saved root exit evidence, not terminal status alone.
      fs.writeFileSync(path.join(run, "status.json"), JSON.stringify({ transport: "process", sessionId: "2147483647", ...status }));
      fs.utimesSync(run, (now - age) / 1_000, (now - age) / 1_000);
      return run;
    };
    expect(() => process.kill(2147483647, 0)).toThrow(expect.objectContaining({ code: "ESRCH" }));
    const old = make("terminal-old", { status: "completed" });
    const actor = make("actor-old", { status: "completed", actorId: "actor-without-public-metadata",
      transport: "process", sessionId: "2147483647", processStartTime: "1" });
    const recent = make("terminal-recent", { status: "completed" }, 1_000);
    const live = make("live", { status: "completed", transport: "process", sessionId: String(process.pid) });
    const unknown = make("unknown", { status: "running" });
    const recordlessIdentity = make("missing-root-identity", { status: "completed", sessionId: undefined });
    const external = make("external", { status: "completed", transport: "herdr" });
    const malformed = make("malformed", { status: "completed", transport: "process", sessionId: "unknown" });
    const deadWithoutBirth = make("dead-without-birth", { status: "running", transport: "process", sessionId: "2147483647" });
    const unresolved = make("unresolved", { status: "completed" });
    fs.writeFileSync(path.join(unresolved, "unresolved-worker.json"), "{}");
    fs.utimesSync(unresolved, (now - RESIDENT_RUN_RETENTION_MS - 60_000) / 1_000, (now - RESIDENT_RUN_RETENTION_MS - 60_000) / 1_000);
    try {
      await host.start();
      // Startup does not inspect archives; the streaming collector runs later.
      expect(fs.existsSync(old)).toBe(true);
      // This case tests streaming collection, not the separately covered
      // reference-preparation budget. Finish that synchronous preparation before
      // the first collector tick so transient live hints cannot skip this sample.
      const references = host.agents.retentionReferences({ budgetMs: 100, maxEntries: 64 });
      // Malformed/live fixtures deliberately retain the wildcard; neither
      // collectable ID may be a transient per-run hint in this first sample.
      expect(references.has("terminal-old")).toBe(false);
      expect(references.has("actor-old")).toBe(false);
      await vi.waitFor(() => {
        expect(fs.existsSync(old)).toBe(false);
        expect(fs.existsSync(actor)).toBe(false);
      });
      for (const run of [recent, live, unknown, malformed, deadWithoutBirth, unresolved]) expect(fs.existsSync(run), run).toBe(true);
      expect(fs.existsSync(old)).toBe(false);
      expect(fs.existsSync(actor)).toBe(false);
      for (const run of [recent, live, unknown, recordlessIdentity, external, malformed, deadWithoutBirth, unresolved]) expect(fs.existsSync(run), run).toBe(true);
      // Inject the clock: a preserved terminal survivor becomes eligible on a later host start.
      expect(sweepResidentRuns(runs, now + RESIDENT_RUN_RETENTION_MS + 60_000)).toEqual([recent]);
      expect(sweepResidentRuns(runs, now + 10 * RESIDENT_RUN_RETENTION_MS, 0)).toEqual([]);
    } finally { await host.close(); fs.rmSync(root, { recursive: true, force: true }); }
  });
});

describe("resident host ownership", () => {
  it.each(["setModel", "setThinking"] as const)("refuses Main-only %s commands rather than delivering them to a resident actor", async operation => {
    const { root, host } = fixture();
    let handler!: Parameters<typeof host.control.start>[0];
    const control = vi.spyOn(FabricControlPlane.prototype, "start").mockImplementation(accept => { handler = accept; });
    try {
      await host.start(); control.mockRestore();
      vi.spyOn(host.actors, "owns").mockReturnValue(true);
      vi.spyOn(host.actors, "status").mockReturnValue({ rootId: host.config.rootId } as ReturnType<typeof host.actors.status>);
      vi.spyOn(host.actors, "resolveActivationBinding").mockResolvedValue({});
      const tell = vi.spyOn(host.actors, "tell").mockReturnValue({ messageId: "incorrectly-delivered" } as ReturnType<typeof host.actors.tell>);
      const command = { version: 1 as const, operation, targetId: "actor", commandId: "main-only", replyTo: "caller", requestedAt: Date.now(),
        message: "must not be treated as a followUp", binding: { model: "provider/model", thinking: "high" as const } };
      await expect(handler(command, host.identity, new AbortController().signal, "mesh"))
        .resolves.toMatchObject({ accepted: false, error: "remote Main model changes are not supported yet; see smarty-dev#4153" });
      expect(tell).not.toHaveBeenCalled();
    } finally { control.mockRestore(); await host.close(); fs.rmSync(root, { recursive: true, force: true }); }
  });
  it("followUp advisory A2 resident-owned running task ACK and persisted replay preserve the warning", async () => {
    const { root, config, host } = fixture();
    config.workerPath = path.resolve("tests/fixtures/fake-worker.mjs");
    config.piModels = { available: [{ provider: "fixture", id: "visible" }], aliases: {}, defaultModel: "fixture/visible" };
    let sender: FabricControlPlane | undefined;
    try {
      await host.start();
      sender = new FabricControlPlane(host.mesh, { id: "session:sender", name: "Sender", kind: "main" },
        { enabled: true, hostId: "session:sender", pollMs: 20, acknowledgementTimeoutMs: 2000 });
      sender.start(() => ({ accepted: false }));
      const child = await host.agents.spawn({ task: "HANG", transport: "process" });
      await vi.waitFor(() => expect(fs.existsSync(path.join(host.agents.runDirectory(child.id)!, "status.json"))).toBe(true));
      const receipt = await sender.request(host.hostId, child.id, "followUp", { message: "later" }, host.identity.id);
      const warning = { code: "FABRIC_FOLLOW_UP_RUNNING_TASK", targetId: child.id, kind: "agent", status: "running",
        message: "followUp to a running task waits until its current run finishes; use agents.steer for a correction needed before completion." };
      const command = host.mesh.read({ topic: "fabric.control.command", limit: 10 })[0]!;
      await host.mesh.publish({ topic: command.topic, kind: command.kind, from: command.from, to: command.to!, data: command.data });
      await vi.waitFor(() => expect(host.mesh.read({ topic: "fabric.control.ack", limit: 10 })).toHaveLength(2));
      const entries = fs.readFileSync(path.join(host.agents.runDirectory(child.id)!, "steer.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line));
      expect(entries).toHaveLength(1); expect(entries[0]).toMatchObject({ type: "follow_up", message: "later" });
      expect(entries[0]).not.toHaveProperty("warning");
      for (const ack of host.mesh.read({ topic: "fabric.control.ack", limit: 10 })) expect(ack.data).toMatchObject({ accepted: true, warning });
      expect(receipt).toEqual({ queued: true, messageId: expect.any(String), routed: "mesh", acknowledged: true, warning });
    } finally { await sender?.close(); await host.close(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it.each(["ask", "followUp"] as const)("preserves owner-default provenance through %s admission without pinning resolved defaults", async (operation) => {
    const { root, config, host } = fixture();
    let handler!: Parameters<typeof host.control.start>[0];
    const control = vi.spyOn(FabricControlPlane.prototype, "start").mockImplementation((accept) => { handler = accept; });
    try {
      await host.start();
      control.mockRestore();
      vi.spyOn(host.actors, "owns").mockReturnValue(true);
      vi.spyOn(host.actors, "status").mockReturnValue({ rootId: config.rootId } as ReturnType<typeof host.actors.status>);
      const binding = vi.spyOn(host.actors, "resolveActivationBinding").mockResolvedValue({ model: "provider/current", thinking: "high" });
      const tell = vi.spyOn(host.actors, "tell").mockReturnValue({ messageId: "accepted" } as ReturnType<typeof host.actors.tell>);
      const ask = vi.spyOn(host.actors, "ask").mockResolvedValue({ id: "accepted" } as Awaited<ReturnType<typeof host.actors.ask>>);
      const principal = { id: "paul", binding: "voice-call" as const };
      const command = { operation, principal, targetId: "actor", commandId: "owner-defaults", message: "keep working", binding: { model: "provider/pinned" }, bindingProvenance: { kind: "owner-defaults" as const, rootId: config.rootId } } as Parameters<typeof handler>[0];
      const from = { id: config.rootId, name: "Main", kind: "main" as const, sessionId: config.sessionId };
      const signal = new AbortController().signal;
      await expect(handler(command, from, signal, "mesh")).resolves.toMatchObject({ accepted: true, messageId: "accepted" });
      const options = { overrides: { model: "provider/pinned" } };
      const provenance = expect.objectContaining({ principal });
      if (operation === "ask") expect(ask).toHaveBeenCalledWith("actor", "keep working", undefined, signal, { ...options, provenance });
      else {
        expect(binding).toHaveBeenCalledWith("actor", options);
        expect(tell).toHaveBeenCalledWith("actor", "keep working", undefined, { ...options, provenance });
      }
      await expect(handler(command, { ...from, id: "session:foreign" }, signal)).resolves.toMatchObject({ accepted: false, error: "Invalid actor owner-default binding provenance" });
      expect(operation === "ask" ? ask : tell).toHaveBeenCalledOnce();
      const { bindingProvenance: _ignored, binding: _pinned, ...resolved } = command;
      for (const foreignBinding of [undefined, {}, { thinking: "medium" as const }]) {
        await expect(handler({ ...resolved, ...(foreignBinding ? { binding: foreignBinding } : {}) }, { ...from, id: "session:foreign" }, signal, "bridge")).resolves.toMatchObject({ accepted: true });
        const foreignOptions = { binding: foreignBinding ?? {} };
        if (operation === "ask") expect(ask).toHaveBeenLastCalledWith("actor", "keep working", undefined, signal, { ...foreignOptions, provenance });
        else {
          expect(binding).toHaveBeenLastCalledWith("actor", foreignOptions);
          expect(tell).toHaveBeenLastCalledWith("actor", "keep working", undefined, { ...foreignOptions, provenance });
        }
      }
    } finally {
      control.mockRestore();
      await host.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
  it("publishes the request fence and process birth identity together, then releases ownership", async () => {
    const { root, config, host } = fixture();
    const ownerPath = path.join(config.residencyRoot, "owner.json");
    try {
      await host.start();
      const owner = JSON.parse(fs.readFileSync(ownerPath, "utf8"));
      expect(owner).toMatchObject({ requestFence: 1, callerBoundSpawn: 1, requestExpiry: 1, creationIdempotency: 1, commands: expect.arrayContaining(["spawnBound", "setModel", "setTools"]), pid: process.pid, hostId: host.hostId });
      expect(owner.processStartTime).toBe(processStartTime(process.pid));
      expect(residentProcessAlive(owner.pid, owner.processStartTime)).toBe(true);
      const sharedHostKey = `topology/hosts/${createHash("sha256").update(host.hostId).digest("hex")}`;
      expect(host.mesh.get(sharedHostKey, { fresh: true })).toBeDefined();
      await host.close();
      expect(fs.existsSync(ownerPath)).toBe(false);
      expect(host.mesh.get(sharedHostKey, { fresh: true })).toBeUndefined();
    } finally { await host.close(); fs.rmSync(root, { recursive: true, force: true }); }
  });
  it("never follows mutable config alone without a Main intent and attested launcher custody", async () => {
    const { root, config, host, idle } = fixture();
    const next = path.join(root, "other-package");
    fs.mkdirSync(path.join(next, "dist/residency"), { recursive: true });
    fs.writeFileSync(path.join(next, "package.json"), JSON.stringify({ name: "pi-fabric" }));
    fs.writeFileSync(path.join(next, "dist/residency/launcher.js"), "// must not run");
    let handler!: Parameters<typeof host.control.start>[0];
    const control = vi.spyOn(FabricControlPlane.prototype, "start").mockImplementation((accept) => { handler = accept; });
    try {
      await host.start();
      control.mockRestore();
      const ownerPath = path.join(config.residencyRoot, "owner.json");
      const owner = fs.readFileSync(ownerPath, "utf8");
      vi.spyOn(host.actors, "listOwned").mockReturnValue([{ residency: "durable", status: "idle", queued: 0 }] as ReturnType<typeof host.actors.listOwned>);
      vi.spyOn(host.actors, "owns").mockReturnValue(true);
      vi.spyOn(host.actors, "status").mockReturnValue({ rootId: config.rootId } as ReturnType<typeof host.actors.status>);
      vi.spyOn(host.actors, "resolveActivationBinding").mockResolvedValue({});
      const tell = vi.spyOn(host.actors, "tell").mockReturnValue({ messageId: "accepted" } as ReturnType<typeof host.actors.tell>);
      fs.writeFileSync(path.join(config.residencyRoot, "config.json"), JSON.stringify({ ...config, fabricExtensionPath: path.join(next, "dist/index.js") }));
      await delay(150);
      expect(idle).not.toHaveBeenCalled();
      expect(fs.readFileSync(ownerPath, "utf8")).toBe(owner);
      await expect(handler({ operation: "followUp", targetId: "actor", commandId: "after-config", message: "keep working" } as Parameters<typeof handler>[0], host.identity, new AbortController().signal)).resolves.toMatchObject({ accepted: true, messageId: "accepted" });
      expect(tell).toHaveBeenCalledOnce();
    } finally {
      control.mockRestore();
      await host.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("rejects delayed and new admissions during ordinary close", async () => {
    const { root, config, host } = fixture();
    let handler!: Parameters<typeof host.control.start>[0];
    const control = vi.spyOn(FabricControlPlane.prototype, "start").mockImplementation((accept) => { handler = accept; });
    let resolve: ((binding: Awaited<ReturnType<typeof host.actors.resolveActivationBinding>>) => void) | undefined;
    let closing: Promise<void> | undefined;
    try {
      await host.start();
      control.mockRestore();
      vi.spyOn(host.actors, "owns").mockReturnValue(true);
      vi.spyOn(host.actors, "status").mockReturnValue({ rootId: config.rootId } as ReturnType<typeof host.actors.status>);
      vi.spyOn(host.actors, "resolveActivationBinding").mockImplementation(() => new Promise((done) => { resolve = done; }));
      const tell = vi.spyOn(host.actors, "tell");
      const admission = handler({ operation: "followUp", targetId: "actor", commandId: "delayed", message: "keep me" } as Parameters<typeof handler>[0], host.identity, new AbortController().signal);
      closing = host.close();
      await expect(handler({ operation: "followUp", targetId: "actor", message: "retry me" } as Parameters<typeof handler>[0], host.identity, new AbortController().signal)).resolves.toMatchObject({ accepted: false, error: "Fabric resident host is closing; retry" });
      await expect(host.lifecycle.deliver({ to: "actor" } as Parameters<typeof host.lifecycle.deliver>[0], {} as Parameters<typeof host.lifecycle.deliver>[1])).rejects.toThrow("Fabric resident host is closing; retry");
      resolve!({});
      await expect(admission).resolves.toMatchObject({ accepted: false, error: "Fabric resident host is closing; retry" });
      expect(tell).not.toHaveBeenCalled();
      await closing;
    } finally {
      resolve?.({});
      control.mockRestore();
      await closing;
      await host.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it.skipIf(process.platform !== "linux").each([
    ["host.lock", true], ["host.lock", false], ["owner.json", true],
  ] as const)("R3 refuses a live legacy owner in %s (birth identity %s)", async (file, birth) => {
    const { root, config, host } = fixture();
    const child = spawn("sleep", ["60"], { stdio: "ignore" });
    const exited = new Promise<void>((resolve) => child.once("exit", () => resolve()));
    try {
      await new Promise<void>((resolve, reject) => { child.once("spawn", resolve); child.once("error", reject); });
      const record = JSON.stringify({ pid: child.pid, ...(birth ? { processStartTime: processStartTime(child.pid!) } : {}) });
      const recordPath = path.join(config.residencyRoot, file);
      fs.writeFileSync(recordPath, record);
      await expect(host.start()).rejects.toThrow(/already running/);
      expect(host.actors).toBeUndefined();
      expect(fs.readFileSync(recordPath, "utf8")).toBe(record);
      const fd = await lockFile(path.join(config.residencyRoot, "host.lock"), 0);
      fs.closeSync(fd); // Refusal released the flock; it did not overwrite legacy bytes.
    } finally { child.kill(); await exited; await host.close(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it.skipIf(process.platform !== "linux")("R2 two concurrent starters fence all actor/control execution before one wins", async () => {
    const { root, config, host: first } = fixture();
    const second = new ResidentHost(config);
    const control = vi.spyOn(FabricControlPlane.prototype, "start");
    try {
      expect(first.actors).toBeUndefined(); expect(second.actors).toBeUndefined();
      const results = await Promise.allSettled([first.start(), second.start()]);
      expect(results.filter((r) => r.status === "fulfilled")).toHaveLength(1);
      const rejected = results.find((r) => r.status === "rejected") as PromiseRejectedResult;
      expect(rejected.reason.constructor.name).toBe("ResidentHostAlreadyRunning");
      const loser = results[0]!.status === "rejected" ? first : second;
      expect(loser.actors).toBeUndefined();
      expect(control).toHaveBeenCalledTimes(1);
    } finally { control.mockRestore(); await Promise.all([first.close(), second.close()]); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it.skipIf(process.platform !== "linux")("R2 stale diagnostic file naming a dead PID cannot block or replace the kernel inode", async () => {
    const { root, config, host } = fixture();
    const lock = path.join(config.residencyRoot, "host.lock");
    // Establish the shared-fence inode, then corrupt only its diagnostic bytes.
    await host.start(); await host.close();
    const recovering = new ResidentHost(config);
    fs.writeFileSync(lock, JSON.stringify({ pid: -1, token: "stale" }));
    const inode = fs.statSync(lock).ino;
    // Our own PID is not a different live legacy owner.
    fs.writeFileSync(path.join(config.residencyRoot, "owner.json"), JSON.stringify({ pid: process.pid }));
    try {
      await recovering.start();
      expect(fs.statSync(lock).ino).toBe(inode);
      await recovering.close();
      expect(fs.statSync(lock).ino).toBe(inode);
    } finally { await recovering.close(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it.skipIf(process.platform !== "linux")("R2 SIGKILL releases host ownership without replacing its lock file", async () => {
    const { root, config, host: successor } = fixture();
    const lock = path.join(config.residencyRoot, "host.lock");
    const child = spawn("bun", ["-e", `const {ResidentHost}=await import(${JSON.stringify(path.resolve("src/residency/host.ts"))}); const host=new ResidentHost(${JSON.stringify(config)}); await host.start(); console.log('ready');`], { stdio: ["ignore", "pipe", "pipe"] });
    const done = new Promise<void>((resolve) => child.once("exit", () => resolve()));
    try {
      await new Promise<void>((resolve, reject) => { child.stdout!.once("data", () => resolve()); child.once("error", reject); child.once("exit", () => reject(new Error("fixture died before ready"))); });
      const inode = fs.statSync(lock).ino;
      // Diagnostic content lies: only the OS fence must reject this contender.
      fs.rmSync(path.join(config.residencyRoot, "owner.json"));
      fs.writeFileSync(lock, JSON.stringify({ pid: -1, token: "corrupt-diagnostic" }));
      await expect(successor.start()).rejects.toThrow(/already running/);
      child.kill("SIGKILL"); await done;
      await successor.start();
      expect(fs.statSync(lock).ino).toBe(inode);
    } finally { child.kill("SIGKILL"); await done; await successor.close(); fs.rmSync(root, { recursive: true, force: true }); }
  }, 20_000);

  // Preserve round one's unknown-Linux-identity regression: unreadable is not dead.
  it.skipIf(process.platform !== "linux")("never reclaims a live host's lock on an unreadable start time", async () => {
    const { root, config, host } = fixture();
    const contender = new ResidentHost(config);
    let eio: ReturnType<typeof vi.spyOn> | undefined;
    try {
      await host.start();
      const lock = path.join(config.residencyRoot, "host.lock");
      const live = fs.readFileSync(lock, "utf8");
      const read = fs.readFileSync.bind(fs) as typeof fs.readFileSync;
      eio = vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, options?: unknown) => {
        if (String(file) === `/proc/${process.pid}/stat`) throw Object.assign(new Error("EIO"), { code: "EIO" });
        return read(file, options as never);
      }) as typeof fs.readFileSync);
      expect(residentProcessAlive(process.pid, "1")).toBe(true);
      await expect(contender.start()).rejects.toThrow(/already running|starting/);
      expect(fs.readFileSync(lock, "utf8")).toBe(live);
    } finally { eio?.mockRestore(); await Promise.all([host.close(), contender.close()]); fs.rmSync(root, { recursive: true, force: true }); }
    expect(residentProcessAlive(process.pid, "0")).toBe(processStartTime(process.pid) === undefined);
  });

  it("rejects a proven reused PID but conservatively accepts unknown or legacy owners", () => {
    expect(residentProcessAlive(process.pid)).toBe(true);
    expect(residentProcessAlive(-1)).toBe(false);
    const start = processStartTime(process.pid);
    if (start !== undefined) {
      expect(start).toMatch(/^\d+$/);
      expect(residentProcessAlive(process.pid, start)).toBe(true);
      expect(residentProcessAlive(process.pid, "0")).toBe(false);
    } else {
      expect(residentProcessAlive(process.pid, "0")).toBe(true);
    }
  });
});
