import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { MainAgentController } from "../src/main-agent.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { AgentMessageRouter } from "../src/providers/agents-message-router.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";

// Keep: the A2 trust boundary is the late directory lookup immediately before control publication.
// This in-process mesh reaches that refusal without a live-host race, then proves explicit same-key
// retry and owner-side command deduplication. The post-install script covers the two-process path.
const roots: string[] = [];
const stores: MeshStore[] = [];
const directories: ParticipantDirectory[] = [];
const planes: FabricControlPlane[] = [];
const mains: MainAgentController[] = [];
const who = (id: string): MeshIdentity => ({ id, name: id, kind: "main" });
type RouterPorts = ConstructorParameters<typeof AgentMessageRouter>;
const unknownManager = { status: (id: string) => { throw new Error(`Unknown Fabric agent: ${id}`); } } as unknown as RouterPorts[0];
const actors = (identity: MeshIdentity) => ({
  identity,
  status: (id: string) => { throw new Error(`Unknown Fabric actor: ${id}`); },
  validateDirectMessage: () => undefined,
} as unknown as RouterPorts[1]);

const fixture = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-a2-probe-")); roots.push(root);
  const meshRoot = path.join(root, "mesh");
  const senderId = "session:a2-sender", receiverId = "session:a2-receiver";
  const mesh = () => { const store = new MeshStore(meshRoot, 65_536, 1_000); stores.push(store); return store; };
  const senderDirectory = new ParticipantDirectory(mesh(), {
    enabled: true, hostId: senderId, rootId: senderId, identity: who(senderId),
    heartbeatMs: 60_000, leaseMs: 120_000, reapDeadHosts: false,
  });
  const receiverDirectory = new ParticipantDirectory(mesh(), {
    enabled: true, hostId: receiverId, rootId: receiverId, identity: who(receiverId),
    heartbeatMs: 60_000, leaseMs: 120_000, reapDeadHosts: false,
  });
  directories.push(senderDirectory, receiverDirectory);
  const receiverIncarnation = receiverDirectory.ownerIncarnation;
  if (!receiverIncarnation) throw new Error("receiver directory did not allocate an incarnation");
  const presence: FabricParticipantRecord = {
    format: 1, id: receiverId, rootId: receiverId, kind: "root",
    ownerHostId: receiverId, ownerIdentityId: receiverId,
    ownerIncarnation: receiverIncarnation,
    name: "a2-probe-receiver", status: "running", runner: "pi", transport: "host",
    capabilities: ["followUp", "steer"], controlProtocol: "v1", startedAt: 1, updatedAt: Date.now(),
  };
  receiverDirectory.registerSource(() => [presence]);
  await receiverDirectory.refresh();
  await senderDirectory.refresh();
  expect(senderDirectory.get(receiverId, undefined, { fresh: true })).toMatchObject({ id: receiverId, stale: false });

  const main = new MainAgentController({
    on: () => () => {}, sendMessage: vi.fn(), getThinkingLevel: () => "off",
  } as unknown as ExtensionAPI, receiverId, true, root, "a2-probe-receiver");
  mains.push(main);
  main.attachFollowUpDrain({
    isIdle: () => false,
    hasPendingMessages: () => false,
    sessionManager: { getEntries: () => [] },
  } as never, 60_000, path.join(root, "receiver-followups.json"));
  const receiver = new FabricControlPlane(mesh(), who(receiverId), {
    enabled: true, hostId: receiverId, ownerIncarnation: receiverIncarnation,
    pollMs: 5, acknowledgementTimeoutMs: 1_000,
  });
  const sender = new FabricControlPlane(mesh(), who(senderId), {
    enabled: true, hostId: senderId, pollMs: 5, acknowledgementTimeoutMs: 1_000,
  });
  planes.push(receiver, sender);
  const receiverRouter = new AgentMessageRouter(unknownManager, actors(who(receiverId)), main, receiverDirectory, receiver, binding => binding);
  const accept = vi.fn((...args: Parameters<AgentMessageRouter["acceptControl"]>) => receiverRouter.acceptControl(...args));
  receiver.start(accept);
  sender.start(() => ({ accepted: false }));
  const senderMain = { id: senderId, local: true, matches: (id: string) => id === senderId } as unknown as RouterPorts[2];
  const route = new AgentMessageRouter(unknownManager, actors(who(senderId)), senderMain, senderDirectory, sender, binding => binding);
  const commands = () => sender.mesh.read({ topic: "fabric.control.command", limit: 100 })
    .filter(event => event.kind === "followUp" && (event.data as { message?: string } | undefined)?.message?.includes("a2probe-") === true);
  return { root, senderDirectory, route, sender, receiver, accept, commands, receiverId, receiverDirectory, presence };
};

afterEach(async () => {
  await Promise.all(planes.splice(0).map(plane => plane.close()));
  await Promise.all(directories.splice(0).map(directory => directory.close()));
  for (const main of mains.splice(0)) main.closeFollowUpDrain();
  for (const store of stores.splice(0)) store.closeState();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  vi.restoreAllMocks();
});

describe("A2 late directory refusal and same-key retry", () => {
  it("publishes no refusal attempt, then delivers one receiver message across two same-key retries", async () => {
    const f = await fixture();
    const key = "a2probe-test-20261017T000000Z";
    const message = `A2PROBE ${key}`;
    const failure = new Error("late canonical lookup unavailable");
    vi.spyOn(f.senderDirectory, "get").mockImplementationOnce(() => { throw failure; });

    await expect(f.route.routeMessage(f.receiverId, message, undefined, "followUp", undefined, { idempotencyKey: key }))
      .rejects.toMatchObject({
        name: "FabricDirectoryUnavailableError",
        code: "FABRIC_DIRECTORY_UNAVAILABLE",
        retryable: true,
        cause: failure,
      });
    expect(f.commands()).toHaveLength(0);

    await expect(f.route.routeMessage(f.receiverId, message, undefined, "followUp", undefined, { idempotencyKey: key }))
      .resolves.toMatchObject({ queued: true, acknowledged: true, routed: "mesh" });
    expect(f.commands()).toHaveLength(1);
    expect(f.accept).toHaveBeenCalledTimes(1);

    await expect(f.route.routeMessage(f.receiverId, message, undefined, "followUp", undefined, { idempotencyKey: key }))
      .resolves.toMatchObject({ queued: true, acknowledged: true, routed: "mesh" });
    expect(f.accept).toHaveBeenCalledTimes(1);
    expect(new Set(f.commands().map(event => (event.data as { commandId: string }).commandId)).size).toBe(1);
  });
});
