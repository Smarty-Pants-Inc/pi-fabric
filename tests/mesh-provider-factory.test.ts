import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import { MeshProvider } from "../src/providers/mesh-provider.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";

// smarty-dev#9352: the hub factory reads by topic under the literal role address `factory`; no
// participant record answers to it, so an immediate publish must not need a directory member.
const roots: string[] = [];
const context = {} as FabricInvocationContext;
const identity: MeshIdentity = { id: "session:factory-test", name: "main", kind: "main", sessionId: "factory-test" };

afterEach(() => {
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-mesh-factory-")); roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  // A real, empty directory: the factory is absent from it, exactly as on any host.
  const participants = new ParticipantDirectory(mesh, { enabled: true, hostId: identity.id, rootId: identity.id, identity });
  const provider = new MeshProvider(mesh, identity, participants);
  const outcomes = () => fs.existsSync(path.join(mesh.root, "delivery-outcomes"))
    ? fs.readdirSync(path.join(mesh.root, "delivery-outcomes")) : [];
  return { mesh, provider, outcomes };
};

describe("mesh.publish to the hub factory role address", () => {
  it.each([
    [{ topic: "fleet.work.pi-fabric.9349", kind: "wait", text: "WAIT: blocked on review of pi-fabric#9349" }],
    [{ topic: "fleet.work.pi-fabric.1", kind: "eta", text: "ETA: PR ready in ~20m" }],
    [{ topic: "fleet.work.pi-fabric.42", kind: "ask", text: "which branch?" }],
    [{ topic: "fleet.work.smarty.dev.7", kind: "wait", text: "dotted repo" }],
    [{ topic: "fleet.work..github.12", kind: "eta", text: ".github repo" }],
    [{ topic: "fleet.work.pi-fabric.9007199254740991", text: "max safe number" }],
    [{ topic: "ops.owner", kind: "pr.wake", data: { pr: 1 } }],
    [{ topic: "ops.owner", kind: "inbox.age.alarm", text: "inbox aged" }],
  ])("accepts an immediate %o with the host identity and kind kept", async (args) => {
    const { mesh, provider, outcomes } = fixture();
    const forged = { id: "session:forged", name: "forged", kind: "main" };
    const event = await provider.invoke("publish", { ...args, to: "factory", from: forged }, context) as Record<string, unknown>;
    expect(event).toMatchObject({ topic: args.topic, to: "factory", from: identity });
    expect(event.from).toEqual(identity);
    // MeshStore defaults an omitted kind to "message".
    expect(event.kind).toBe((args as { kind?: string }).kind ?? "message");
    expect(event).not.toHaveProperty("scheduled");
    expect(mesh.latestSequence()).toBeGreaterThan(0);
    expect(outcomes()).toEqual([]);
  });

  const refused: [string, Record<string, unknown>][] = [
    ["Factory", { topic: "fleet.work.pi-fabric.1", kind: "wait" }],
    ["factory-lead", { topic: "fleet.work.pi-fabric.1", kind: "wait" }],
    [" factory", { topic: "fleet.work.pi-fabric.1", kind: "wait" }],
    ["factory ", { topic: "ops.owner", kind: "pr.wake" }],
    ["factory", { topic: "fleet.work.pi-fabric", kind: "wait" }],
    ["factory", { topic: "fleet.work.", kind: "wait" }],
    ["factory", { topic: "fleet.work..1", kind: "wait" }],
    ["factory", { topic: "fleet.work.a:b.1", kind: "wait" }],
    ["factory", { topic: "fleet.work.inbox-receipts", kind: "wait" }],
    ["factory", { topic: "fleet.work.pi-fabric.01", kind: "wait" }],
    ["factory", { topic: "fleet.work.pi-fabric.+1", kind: "wait" }],
    ["factory", { topic: "fleet.work.pi-fabric.1.extra", kind: "wait" }],
    ["factory", { topic: "fleet.work.pi-fabric.9007199254740993", kind: "wait" }],
    ["factory", { topic: "fleet.workx.pi-fabric.1", kind: "wait" }],
    ["factory", { topic: "work", kind: "wait" }],
    ["factory", { topic: "ops.owner" }],
    ["factory", { topic: "ops.owner", kind: "root.presence.alarm" }],
    ["factory", { topic: "ops.owner", kind: "fabric.main.provider-dead" }],
    ["factory", { topic: "ops.owner.extra", kind: "pr.wake" }],
  ];
  it.each(refused)("keeps the unknown-target refusal for to=%j on %o", async (to, args) => {
    const { mesh, provider, outcomes } = fixture();
    await expect(provider.invoke("publish", { ...args, to, text: "x" }, context)).rejects.toThrow(`Unknown Fabric target: ${to}`);
    expect(mesh.latestSequence()).toBe(0);
    expect(outcomes()).not.toEqual([]);
  });

  it("still refuses reserved control topics addressed to the factory", async () => {
    const { mesh, provider } = fixture();
    for (const topic of ["fabric.control.ack", "fabric.control.command", "fabric.participant.lifecycle"]) {
      await expect(provider.invoke("publish", { topic, to: "factory", data: {} }, context)).rejects.toThrow("reserved for host coordination");
    }
    expect(mesh.latestSequence()).toBe(0);
  });

  it("leaves scheduled addressed publishes unchanged", async () => {
    const { provider, outcomes } = fixture();
    await expect(provider.invoke("publish", { topic: "work", to: "factory", text: "later", afterMs: 60_000 }, context))
      .resolves.toMatchObject({ scheduled: true });
    await expect(provider.invoke("publish", { topic: "fleet.work.pi-fabric.1", kind: "wait", to: "factory", afterMs: 60_000 }, context))
      .resolves.toMatchObject({ scheduled: true });
    expect(outcomes()).toEqual([]);
  });
});
