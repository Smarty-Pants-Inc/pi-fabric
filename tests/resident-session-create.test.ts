import fs from "node:fs";
import { createHash } from "node:crypto";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { ActorDirectory } from "../src/actors/directory.js";
import { ActorRegistryOwnershipError } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { removeHostLease } from "../src/topology/host-leases.js";
import { actorParticipantRecord } from "../src/topology/records.js";
import { isOwnResidentActor } from "../src/residency/actor-ownership.js";
import { ResidencyClient } from "../src/residency/client.js";
import { ResidentHost } from "../src/residency/host.js";
import { residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";

const fixture = async (residentRootId = "session:create", scope: "session" | "project" = "session") => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-session-create-"));
  const identity = { id: "session:create", name: "Main", kind: "main" as const, sessionId: "create" };
  const meshRoot = path.join(root, "mesh");
  const mesh = new MeshStore(meshRoot, 64 * 1024, 100);
  const meshConfig = { ...DEFAULT_FABRIC_CONFIG.mesh, actorScope: "session" as const, actorPollMs: 20 };
  const actorRoots = { project: path.join(root, "actors"), session: path.join(root, "actors", "create") };
  const participants = new ParticipantDirectory(mesh, { enabled: true, hostId: identity.id, rootId: identity.id, identity });
  const agents = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 }, {
    runRoot: path.join(root, "runs"), workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
  });
  const deliveries: string[] = [];
  const actors = new ActorDirectory([identity.sessionId, identity, mesh, meshConfig, agents,
    ({ message }) => { if (message.text) deliveries.push(message.text); }, {
      persistent: true, rootId: identity.id, claimResidency: "session",
      canManageActor: id => {
        const actor = participants.get(id);
        return actor ? actor.ownerHostId === identity.id : undefined;
      },
      isOwnResidentActor: id => isOwnResidentActor(participants, id, identity.id),
      resolvePiModel: model => model,
    }], actorRoots, "session");
  participants.registerSource(() => [{
    format: 1, id: identity.id, kind: "root", rootId: identity.id, ownerHostId: identity.id, ownerIdentityId: identity.id,
    name: "Main", status: "idle", residency: "session", runner: "pi", transport: "host", capabilities: ["fabric"],
    cwd: root, sessionId: identity.sessionId, startedAt: 1, updatedAt: Date.now(), controlProtocol: "v1",
  }, ...actors.listOwned().map(actor => actorParticipantRecord(actor, identity.id, identity.id, identity.id, identity.id))]);
  await participants.start();
  const config: ResidentHostConfig = {
    format: 1, rootId: residentRootId, sessionId: identity.sessionId, cwd: root, projectRoot: root, meshRoot,
    actorRoot: actorRoots.project, sessionActorRoot: actorRoots.session, residencyRoot: residentRoot(meshRoot, residentRootId),
    fullCodeMode: true, agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 }, mesh: meshConfig,
    retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
    fabricExtensionPath: path.resolve("dist/index.js"), piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
    piModels: { available: [{ provider: "fixture", id: "visible" }], aliases: {}, defaultModel: "fixture/visible" },
  };
  const host = new ResidentHost(config, () => {});
  await host.start();
  const client = new ResidencyClient({ config, mesh, participants, commandTimeoutMs: 5_000,
    mainAgent: { id: residentRootId, local: true } as FabricMainAgentTarget });
  const durable = await client.createActor({ name: "durable", instructions: "Watch", residency: "durable", scope, model: "fixture/visible" });
  const registry = () => JSON.parse(fs.readFileSync(path.join(actorRoots[scope], "actors.json"), "utf8")) as { actors: Array<{ id: string }> };
  return { root, identity, mesh, participants, agents, actors, config, host, client, durable, registry, deliveries,
    close: async () => {
      await actors.close(); await agents.close(); await client.close(); await host.close(); await participants.close();
      fs.rmSync(root, { recursive: true, force: true });
    },
  };
};

describe("Main session creation alongside its resident host (#2483)", () => {
  it.each(["session", "project"] as const)("admits %s storage while leaving the durable actor fenced to its live owner", async scope => {
    const state = await fixture(undefined, scope);
    try {
      expect(state.actors.owns(state.durable.id)).toBe(false);
      const original = state.registry().actors.find(actor => actor.id === state.durable.id);
      const session = await state.actors.create({ name: "session", instructions: "Reply", residency: "session", scope, model: "fixture/visible", delivery: "steer", triggerTurn: false });
      expect(state.actors.owns(session.id)).toBe(true);
      expect(state.host.actors.owns(state.durable.id)).toBe(true);
      expect(state.host.actors.owns(session.id)).toBe(false);
      expect(state.registry().actors.find(actor => actor.id === state.durable.id)).toEqual(original);
      expect(() => state.actors.tell(state.durable.id, "not our run")).toThrow("owned by another host");
      await expect(state.actors.setInstructions(state.durable.id, "not our write")).rejects.toThrow("owned by another host");
      const durableRun = vi.spyOn(state.host.agents, "run");
      const sessionRun = vi.spyOn(state.agents, "run");
      await state.host.actors.ask(state.durable.id, "run durable once");
      state.actors.tell(session.id, "run session once");
      const deadline = Date.now() + 5_000;
      while (state.deliveries.length === 0) {
        if (Date.now() >= deadline) throw new Error("Session actor did not deliver to Main");
        await new Promise(resolve => setTimeout(resolve, 10));
      }
      expect(state.host.actors.status(state.durable.id).messages).toBe(2);
      expect(state.actors.status(session.id).messages).toBe(2);
      expect(state.deliveries).toHaveLength(1);
      expect(sessionRun).toHaveBeenCalledTimes(1);
      expect(durableRun).toHaveBeenCalledTimes(1);
    } finally { await state.close(); }
  });

  it("also admits the default session residency", async () => {
    const state = await fixture();
    try {
      const actor = await state.actors.create({ name: "implicit", instructions: "Reply" });
      expect(actor.residency).toBe("session");
      expect(state.actors.owns(state.durable.id)).toBe(false);
    } finally { await state.close(); }
  });

  it("refuses a foreign root with a live resident owner on the same registry", async () => {
    const state = await fixture("session:foreign");
    try {
      expect(state.participants.get(state.durable.id)?.stale).toBe(false);
      await expect(state.actors.create({ name: "blocked", instructions: "Reply", residency: "session" })).rejects.toThrow(ActorRegistryOwnershipError);
    } finally { await state.close(); }
  });

  it.each(["foreign host", "foreign identity", "dead host"] as const)("refuses a same-root %s, even with the resident display name", async control => {
    const state = await fixture();
    let impostor: ParticipantDirectory | undefined;
    try {
      await state.host.close();
      if (control !== "dead host") {
        const hostId = control === "foreign host" ? "host:foreign" : state.host.hostId;
        const ownerIdentity = { id: "identity:foreign", name: state.host.identity.name, kind: "agent" as const };
        impostor = new ParticipantDirectory(state.mesh, { enabled: true, hostId, rootId: state.identity.id, identity: ownerIdentity });
        impostor.registerSource(() => [actorParticipantRecord(state.durable, state.identity.id, hostId, ownerIdentity.id, state.identity.id)]);
        await impostor.start();
        expect(state.participants.get(state.durable.id)?.ownerHostId).toBe(hostId);
      } else expect(state.participants.get(state.durable.id)).toBeUndefined();
      const original = state.registry();
      await expect(state.actors.create({ name: "blocked", instructions: "Reply", residency: "session" })).rejects.toThrow(ActorRegistryOwnershipError);
      expect(state.registry()).toEqual(original);
      expect(state.actors.owns(state.durable.id)).toBe(false);
    } finally { await impostor?.close(); await state.close(); }
  });

  it("refuses an expired resident lease even while its actor record and owner publication remain", async () => {
    const state = await fixture();
    try {
      const key = "topology/hosts/" + createHash("sha256").update(state.host.hostId).digest("hex");
      const entry = state.mesh.get(key, { fresh: true })!;
      await state.mesh.put({ key, value: { ...entry.value as Record<string, unknown>, expiresAt: Date.now() - 1_000 }, identity: state.host.identity });
      removeHostLease(state.mesh.root, state.host.hostId);
      expect(fs.existsSync(path.join(state.config.residencyRoot, "owner.json"))).toBe(true);
      expect(state.participants.get(state.durable.id, Date.now(), { fresh: true })).toBeUndefined();
      await expect(state.actors.create({ name: "blocked", instructions: "Reply", residency: "session" })).rejects.toThrow(ActorRegistryOwnershipError);
      expect(state.actors.owns(state.durable.id)).toBe(false);
    } finally { await state.close(); }
  });

  it("does not extend the exception to local durable creation or take over its own resident actor", async () => {
    const state = await fixture();
    try {
      await expect(state.actors.create({ name: "blocked", instructions: "Reply", residency: "durable" })).rejects.toThrow(ActorRegistryOwnershipError);
      expect(state.actors.listOwned()).toEqual([]);
      expect(state.host.actors.listOwned().map(actor => actor.id)).toEqual([state.durable.id]);
    } finally { await state.close(); }
  });
});
