import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { ParticipantDirectory as OldParticipantDirectory } from "./fixtures/pre-main-bindings-participant-directory.js";
import { LIVENESS_POLICY_KEY } from "../src/topology/host-leases.js";
import { readParticipantFiles } from "../src/topology/participant-files.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { AgentMessageRouter } from "../src/providers/agents-message-router.js";
import type { FabricMainAgentInfo } from "../src/main-agent.js";

const roots: string[] = [];
const directories: Array<ParticipantDirectory | OldParticipantDirectory> = [];
const planes: FabricControlPlane[] = [];
afterEach(async () => {
  await Promise.all(planes.splice(0).map(plane => plane.close()));
  await Promise.all(directories.splice(0).map(directory => directory.close()));
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("mixed-generation Main discovery (#409)", () => {
  it("keeps upgraded Main discovery and ordinary messaging working for an existing reader in files-only mode", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "main-reader-compatibility-")); roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const identity = (name: string): MeshIdentity => ({ id: `session:${name}`, sessionId: name, kind: "main", name });
    const upgraded = identity("upgraded"), existing = identity("existing");
    const mesh = () => new MeshStore(meshRoot, 64 * 1024, 1_000);
    await mesh().put({ key: LIVENESS_POLICY_KEY, value: { version: 1, hostLeases: "files", participants: "files" }, identity: existing });
    const options = (id: MeshIdentity) => ({ enabled: true, hostId: id.id, rootId: id.id, identity: id, reapDeadHosts: false as const });
    const writer = new ParticipantDirectory(mesh(), options(upgraded));
    const reader = new OldParticipantDirectory(mesh(), options(existing));
    directories.push(writer, reader);
    vi.stubEnv("SMARTY_ROLE", "project-agent");
    const info = { id: upgraded.id, name: "Main", kind: "main", status: "idle", runner: "pi", transport: "host",
      cwd: root, sessionId: upgraded.sessionId, startedAt: Date.now(), updatedAt: Date.now(), pendingMessages: false, local: true } as FabricMainAgentInfo;
    writer.registerSource(() => [{ ...writer.root(info, true), repository: "github.com/smarty-pants-inc/pi-fabric" }]);
    await writer.refresh(); await reader.refresh();
    // No sessions/ or shared participant copy can mask a parser rejection or protocol downgrade.
    expect(mesh().listAll("sessions/", { fresh: true })).toEqual([]);
    expect(mesh().listAll("topology/participants/", { fresh: true })).toEqual([]);
    const published = readParticipantFiles(meshRoot, { maxAgeMs: 0 }).find(entry => (entry.value as { id: string }).id === upgraded.id)!;
    expect(published).toMatchObject({ value: { format: 1, capabilities: ["steer", "followUp", "fabric"], mainBindings: false } });
    const retained = { id: upgraded.id, role: "project-agent", repository: "github.com/smarty-pants-inc/pi-fabric", controlProtocol: "v1", stale: false };
    expect(reader.get(upgraded.id, undefined, { fresh: true })).toMatchObject(retained);
    expect(reader.sessions()).toEqual([expect.objectContaining(retained)]);
    expect(reader.peers()).toEqual([expect.objectContaining({ id: upgraded.id, role: retained.role, repository: retained.repository })]);

    type Ports = ConstructorParameters<typeof AgentMessageRouter>;
    const manager = { status: () => { throw new Error("Unknown Fabric agent"); } } as unknown as Ports[0];
    const actors = (id: MeshIdentity) => ({ identity: id }) as Ports[1];
    const received: unknown[] = [];
    const deliverAgent = vi.fn((message: unknown) => { received.push(message); return { queued: true, messageId: `received-${received.length}` }; });
    const main = (id: MeshIdentity, receive = vi.fn()) => ({ id: id.id, local: true, matches: (target: string) => target === id.id, deliverAgent: receive }) as Ports[2];
    const plane = (id: MeshIdentity) => {
      const value = new FabricControlPlane(mesh(), id, { enabled: true, hostId: id.id, pollMs: 20, acknowledgementTimeoutMs: 2_000 });
      planes.push(value); return value;
    };
    const ownerControl = plane(upgraded), senderControl = plane(existing);
    const owner = new AgentMessageRouter(manager, actors(upgraded), main(upgraded, deliverAgent), writer, ownerControl, binding => binding);
    const sender = new AgentMessageRouter(manager, actors(existing), main(existing), reader, senderControl, binding => binding);
    ownerControl.start((...args) => owner.acceptControl(...args));
    senderControl.start(() => ({ accepted: false }));
    for (const delivery of ["steer", "followUp"] as const) {
      await expect(sender.routeMessage(upgraded.id, `old-reader ${delivery}`, undefined, delivery))
        .resolves.toMatchObject({ queued: true, routed: "mesh", acknowledged: true });
    }
    expect(received).toEqual([
      expect.objectContaining({ message: "old-reader steer", delivery: "steer", from: expect.objectContaining({ id: existing.id }) }),
      expect.objectContaining({ message: "old-reader followUp", delivery: "followUp", from: expect.objectContaining({ id: existing.id }) }),
    ]);
    expect(deliverAgent).toHaveBeenCalledTimes(2);
  });
});
