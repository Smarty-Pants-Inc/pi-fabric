import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { LifecycleBroker } from "../src/lifecycle/broker.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { actorParticipantRecord } from "../src/topology/records.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";

// R6b: exercise the public provider with the production ownership/admission
// callbacks, not a mocked health result or an injected refresh counter.
it("reads lapsed owner admission without maintenance while ordinary consumption still refreshes", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-health-admission-"));
  const identity: MeshIdentity = { id: "session:health-admission", kind: "main", name: "Main", sessionId: "health-admission" };
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const participants = new ParticipantDirectory(mesh, {
    enabled: true, hostId: identity.id, rootId: identity.id, identity,
    heartbeatMs: 60_000, leaseMs: 600_000, reapDeadHosts: false,
  });
  const agents = new AgentManager(root, DEFAULT_FABRIC_CONFIG.agents, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"), hostId: identity.id,
  });
  const mainAgent: FabricMainAgentTarget = {
    id: identity.id, local: true, matches: id => id === identity.id,
    info: () => ({ id: identity.id, name: "Main", kind: "main", status: "idle", runner: "pi", transport: "host",
      startedAt: 1, updatedAt: Date.now(), pendingMessages: false, local: true, sessionId: identity.sessionId! }),
    deliverAgent: () => ({ queued: true, messageId: "fixture", routed: "main" }),
  };
  let setup = true;
  const actors = new ActorManager(identity.sessionId!, identity, mesh,
    { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 60_000 }, agents, () => {}, {
      actorRoot: path.join(root, "actors"), persistent: true, mainAgent, rootId: identity.id,
      canManageActor: id => {
        if (setup) return true;
        const owner = participants.get(id, undefined, { fresh: true });
        return owner ? owner.ownerHostId === identity.id : undefined;
      },
      canConsumeMesh: options => setup || participants.canConsumeMesh(undefined, options),
    });
  const lifecycle = new LifecycleBroker(mesh, identity, participants,
    { enabled: false, pollMs: 60_000, maxReadEvents: 100 }, () => {});
  const provider = new AgentsProvider(agents, actors, new GlobalActorRegistry(root, 64 * 1024), mainAgent,
    participants, undefined, lifecycle);
  try {
    const actor = await actors.create({ name: "health-admission", instructions: "Wait for work.", delivery: "mailbox" });
    const rootRecord: FabricParticipantRecord = {
      format: 1, id: identity.id, kind: "root", rootId: identity.id, ownerHostId: identity.id, ownerIdentityId: identity.id,
      name: "Main", status: "idle", runner: "pi", transport: "host", capabilities: ["steer", "followUp", "fabric"],
      startedAt: Date.now(), updatedAt: Date.now(), controlProtocol: "v1",
    };
    participants.registerSource(() => [rootRecord, actorParticipantRecord(actor, identity.id, identity.id, identity.id, identity.id)]);
    await participants.start();
    setup = false;
    const before = participants.confirmedAt();
    const refresh = vi.spyOn(participants, "refresh"); // Call through to real publication and locks.
    const admission = vi.spyOn(participants, "controlAdmission");
    // Confirmation lapses before the native owner lease expires, with no real
    // heartbeat timer firing. The owner is still reachable through fresh reads.
    vi.spyOn(Date, "now").mockReturnValue(Date.now() + 120_001);
    const eventsBefore = mesh.tail(0, 100).events;
    const context: FabricInvocationContext = {
      cwd: root, signal: undefined, parentToolCallId: "health-admission", nestedToolCallId: "health-admission",
      extensionContext: {} as ExtensionContext, update() {}, activity() {},
    };
    const result = await provider.invoke("actorHealth", { id: actor.id }, context);
    // Await any unexpected real refresh too: absence of a timestamp change must
    // not merely mean that the asynchronous mutation has not finished yet.
    for (const call of refresh.mock.results) if (call.value instanceof Promise) await call.value;
    expect(result).toEqual({ state: "unknown", lastProgressAt: null });
    expect(refresh, "health must not renew the real participant publication").not.toHaveBeenCalled();
    expect(admission, "health must not maintain admission incidents or diagnostics").not.toHaveBeenCalled();
    expect(participants.confirmedAt()).toBe(before);
    expect(mesh.tail(0, 100).events).toEqual(eventsBefore);

    // Publication carries execution evidence for its next fenced commit, not
    // the previous heartbeat's admission. It must not renew while preparing it.
    expect(actors.listOwned(true)[0]?.health).toEqual({ state: "healthy", lastProgressAt: null });
    expect(refresh).not.toHaveBeenCalled();
    expect(admission).not.toHaveBeenCalled();
    expect(await provider.invoke("actorHealth", { id: actor.id }, context)).toEqual(result);

    // The explicit read option is pure even outside the provider's observation
    // scope. Inside that scope even an older callback that ignores the option
    // must not start admission maintenance.
    expect(participants.canConsumeMesh(undefined, { refresh: false })).toBe(false);
    expect(participants.observe(() => participants.canConsumeMesh())).toBe(false);
    expect(refresh).not.toHaveBeenCalled();
    expect(admission).not.toHaveBeenCalled();
    expect(participants.confirmedAt()).toBe(before);

    // The normal consumer gate still fails closed and starts real recovery.
    expect(participants.canConsumeMesh()).toBe(false);
    expect(admission).toHaveBeenCalledOnce();
    expect(refresh).toHaveBeenCalledOnce();
    await refresh.mock.results[0]!.value;
    expect(participants.confirmedAt()).toBeGreaterThan(before);
    expect(participants.canConsumeMesh()).toBe(true);
  } finally {
    vi.restoreAllMocks();
    await provider.close().catch(() => undefined);
    await participants.close().catch(() => undefined);
    mesh.closeState();
    fs.rmSync(root, { recursive: true, force: true });
  }
}, 60_000);
