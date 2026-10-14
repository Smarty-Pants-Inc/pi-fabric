import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it } from "vitest";
import { ActionRegistry, type FabricRegistryInvocationContext } from "../src/core/action-registry.js";
import { type BridgePublish, isHubFactoryEvent, RemoteBridgeSide, serveBridgeAgent, StoreBridgeSide } from "../src/mesh/bridge.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";
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

// Decision B (smarty-dev#9349 review P2): the public registry path deliberately normalizes an
// optional `to: null` to omission (src/providers/arg-normalization.ts strips nonnullable optional
// nulls) before the provider; the raw `.invoke` cases above stay as compatibility cases.
const registryContext = (): FabricRegistryInvocationContext => ({
  cwd: process.cwd(),
  signal: undefined,
  parentToolCallId: "parent",
  nestedToolCallId: "nested",
  extensionContext: {} as ExtensionContext,
  update() {},
  approve: async () => {},
  audits: [],
  maxResultChars: 10_000,
}) as FabricRegistryInvocationContext;

const viaRegistry = () => {
  const fx = fixture();
  const registry = new ActionRegistry();
  registry.register(fx.provider);
  return { ...fx, registry };
};

describe("mesh.publish through the ActionRegistry (Decision B)", () => {
  it.each([
    ["omitted", {}, undefined],
    ["null", { to: null }, undefined],
    ["factory", { to: "factory" }, "factory"],
  ] as const)("persists to=%s on a parsed work topic for wait and eta", async (_label, toArgs, expectedTo) => {
    const { mesh, registry } = viaRegistry();
    for (const kind of ["wait", "eta"]) {
      const topic = `fleet.work.pi-fabric.${kind === "wait" ? 9349 : 1}`;
      const text = kind === "wait" ? "WAIT: review (owner fv2-9349) next-check ~00:40Z" : "ETA ~00:40Z";
      await registry.invoke("mesh.publish", { topic, kind, text, ...toArgs }, registryContext());
      const [event] = mesh.read({ after: 0, limit: 100 }).filter((item) => item.topic === topic);
      expect(event).toMatchObject({ topic, kind, from: identity });
      expect(event!.from).toEqual(identity); // sender stays host-owned
      if (expectedTo === undefined) expect(event).not.toHaveProperty("to");
      else expect(event!.to).toBe(expectedTo);
    }
    expect(mesh.latestSequence()).toBe(2);
  });

  it("refuses a caller-supplied sender at the public schema, appending nothing", async () => {
    const { mesh, registry } = viaRegistry();
    await expect(registry.invoke("mesh.publish", { topic: "fleet.work.pi-fabric.1", kind: "wait", to: "factory", from: { id: "session:forged" } }, registryContext()))
      .rejects.toThrow(/mesh\.publish[\s\S]*\/from/);
    expect(mesh.latestSequence()).toBe(0);
  });

  it.each([[["factory"]], [{ id: "factory" }], [42]])("refuses malformed non-null to=%j at the schema, appending nothing", async (to) => {
    const { mesh, registry } = viaRegistry();
    await expect(registry.invoke("mesh.publish", { topic: "fleet.work.pi-fabric.1", kind: "wait", text: "x", to }, registryContext()))
      .rejects.toThrow(/Invalid arguments for mesh\.publish: must be string/);
    expect(mesh.latestSequence()).toBe(0);
  });
});

// The bridge wire has no provider normalization: a raw `to: null` is malformed there.
describe("bridge wire publish without a recipient", () => {
  it("rejects to=null/array/object with 'no recipient' and stores nothing", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-mesh-wire-")); roots.push(root);
    const far = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const toAgent = new PassThrough();
    const fromAgent = new PassThrough();
    const served = serveBridgeAgent(new StoreBridgeSide(far, "dev1"), toAgent, fromAgent);
    const remote = new RemoteBridgeSide(fromAgent, toAgent);
    try {
      for (const to of [null, ["factory"], { id: "factory" }]) {
        // BridgePublish types `to` as a required string; this cast is the deliberate malformed input.
        const event = {
          topic: "fleet.work.pi-fabric.9349", kind: "wait", to, text: "WAIT: x",
          from: { id: "session:far", name: "far", kind: "main" },
          data: { bridge: { from: "forge", id: `wire-${JSON.stringify(to)}` } },
        } as unknown as BridgePublish;
        await expect(remote.publish(event)).rejects.toThrow(/no recipient/);
      }
      expect(far.latestSequence()).toBe(0);
    } finally {
      toAgent.end(); // only after every RPC above replied; ending the agent input resolves serveBridgeAgent
      await served;
    }
  });

  it("does not treat a raw to=null WAIT as an unaddressed hub factory event", () => {
    const raw = { topic: "fleet.work.pi-fabric.9349", kind: "wait", to: null } as unknown as Pick<MeshEvent, "topic" | "kind" | "to">;
    expect(isHubFactoryEvent(raw)).toBe(false);
    expect(isHubFactoryEvent({ topic: "fleet.work.pi-fabric.9349", kind: "wait" })).toBe(true);
  });
});
