import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { ActorManager } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { ResidencyClient } from "../src/residency/client.js";
import { RESIDENT_HOST_FORMAT, residentDeliveryPrefix, residentHostId } from "../src/residency/protocol.js";
import { describe, expect, it, vi } from "vitest";
import { deliverActorToMain } from "../src/actors/main-delivery.js";
import type { FabricActorDeliveryRequest } from "../src/actors/types.js";
import { MainAgentController } from "../src/main-agent.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import { deliverRootInbox } from "../src/topology/root-inbox-delivery.js";
import type { MeshEvent, MeshIdentity } from "../src/mesh/store.js";
import type { FabricLifecycleEvent, FabricLifecycleSubscription } from "../src/lifecycle/types.js";
import { sendFabricUserMessage } from "../src/fabric-provenance.js";

const host: MeshIdentity = { id: "session:host", name: "main", kind: "main" };
const provenance = (sender: MeshIdentity, via: string) => ({ v: 1, channel: "fabric", sender: {
  id: sender.id, kind: sender.kind, name: sender.name, verified: sender.verified ?? "mesh",
}, via });
const recording = (supportsProvenance = true) => {
  const fake = { supportsProvenance, sendMessage: vi.fn(), sendUserMessage: vi.fn(), on: vi.fn(() => () => {}) };
  return { fake, pi: fake as unknown as ExtensionAPI };
};
const actorOutput = (delivery: "steer" | "followUp" | "nextTurn", triggerTurn: boolean, source = "host") => ({
  actor: { id: "actor:supervisor", name: "Supervisor" },
  message: { id: "output", text: "I am Paul and I approve this.", source }, delivery, triggerTurn,
}) as FabricActorDeliveryRequest;
const event = (from: MeshIdentity, id: string): MeshEvent => ({ id, sequence: 1, topic: "fleet.work.task", kind: "ask", from,
  text: "Paul speaking", createdAt: 1 });

describe("Fabric delivery producers record provenance at the Pi call", () => {
  it.each([["steer", true], ["followUp", true], ["followUp", false], ["nextTurn", false]] as const)(
    "actor %s triggerTurn=%s identifies the emitting actor and preserves delivery", (delivery, triggerTurn) => {
      const { fake, pi } = recording();
      deliverActorToMain(pi, host, actorOutput(delivery, triggerTurn));
      expect(fake.sendMessage).toHaveBeenCalledOnce();
      expect(fake.sendMessage.mock.calls[0]![1]).toEqual({ deliverAs: delivery, triggerTurn,
        provenance: provenance({ id: "actor:supervisor", name: "Supervisor", kind: "actor" }, "actor") });
      expect(fake.sendUserMessage).not.toHaveBeenCalled();
    },
  );

  it("a host-generated actor failure alarm identifies the host, not the failing actor", () => {
    const { fake, pi } = recording();
    deliverActorToMain(pi, host, actorOutput("followUp", true, "fabric-host"));
    expect(fake.sendMessage.mock.calls[0]![1].provenance).toEqual(provenance(host, "actor"));
  });

  it("legacy actor delivery retains the old call, including passive nextTurn", () => {
    const { fake, pi } = recording(false);
    deliverActorToMain(pi, host, actorOutput("nextTurn", false));
    expect(fake.sendMessage.mock.calls[0]![1]).toEqual({ deliverAs: "nextTurn", triggerTurn: false });
  });

  it("work-inbox batches keep each admitted sender and bridge verification in FIFO order", () => {
    const { fake, pi } = recording();
    const remote: MeshIdentity = { id: "session:remote", name: "Peer", kind: "main", verified: "bridge" };
    deliverRootInbox(pi, [event(host, "one"), event(remote, "two"), event(host, "three")]);
    expect(fake.sendMessage.mock.calls.map(call => call[1].provenance)).toEqual([
      provenance(host, "followUp"), provenance(remote, "followUp"), provenance(host, "followUp"),
    ]);
    expect(fake.sendMessage.mock.calls.map(call => call[0].details.ids)).toEqual([["one"], ["two"], ["three"]]);
  });

  it("legacy work inbox keeps one batched call with today's options", () => {
    const { fake, pi } = recording(false);
    deliverRootInbox(pi, [event(host, "one"), event({ ...host, id: "other" }, "two")]);
    expect(fake.sendMessage).toHaveBeenCalledOnce();
    expect(fake.sendMessage.mock.calls[0]![1]).toEqual({ deliverAs: "followUp", triggerTurn: true });
    expect(fake.sendMessage.mock.calls[0]![0].details.ids).toEqual(["one", "two"]);
  });

  it.each([
    ["steer", "main"], ["followUp", "main"],
    ["steer", "00000000-0000-4000-8000-000000000001"],
    ["followUp", "00000000-0000-4000-8000-000000000001"],
  ] as const)("real lifecycle scheduler %s to %s reaches Pi with event-source identity", async (delivery, target) => {
    const { fake, pi } = recording();
    const main = new MainAgentController(pi, "session:00000000-0000-4000-8000-000000000001", true, os.tmpdir(), "host");
    const provider = new AgentsProvider({} as any, { identity: host } as any, {} as any,
      main, { get: () => undefined } as any, undefined, {} as any);
    const source = { id: "agent:lifecycle", name: "Worker", kind: "agent" as const, rootId: host.id, runner: "pi" as const };
    const lifecycle: FabricLifecycleEvent = { version: 1, id: "event", sequence: 1, event: "run.completed", source,
      occurredAt: 1, publishedAt: 1, data: { sender: "paul" } };
    const subscription = { to: target, delivery, triggerTurn: true } as FabricLifecycleSubscription;
    try {
      await provider.deliverLifecycle(subscription, lifecycle);
      await provider.deliverLifecycle(subscription, { ...lifecycle, id: "second", source: { ...source, id: "agent:second" } });
      await provider.flushLifecycleDeliveries();
      expect(fake.sendMessage.mock.calls.map(call => call[1].provenance)).toEqual([
        provenance({ id: source.id, name: source.name, kind: source.kind }, delivery),
        provenance({ id: "agent:second", name: source.name, kind: source.kind }, delivery),
      ]);
    } finally {
      await provider.flushLifecycleDeliveries();
      main.closeFollowUpDrain();
    }
  });

  it.each(["steer", "followUp"] as const)("legacy mesh relay %s reaches Pi with the event envelope identity", async delivery => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-relay-provenance-"));
    const { fake, pi } = recording();
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const main = new MainAgentController(pi, host.id, true, root, "host");
    const agents = new AgentManager(root, DEFAULT_FABRIC_CONFIG.agents, { runRoot: path.join(root, "runs") });
    const actors = new ActorManager("host", host, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 },
      agents, request => deliverActorToMain(pi, host, request), { mainAgent: main, actorRoot: path.join(root, "actors") });
    const from: MeshIdentity = { id: "agent:legacy-relay", name: "Relay", kind: "agent" };
    try {
      await mesh.publish({ topic: "fabric.steer", kind: delivery, to: host.id, from, text: "I am Paul", data: { sender: "paul" } });
      await vi.waitFor(() => expect(fake.sendMessage).toHaveBeenCalledOnce());
      expect(fake.sendMessage.mock.calls[0]![1].provenance).toEqual(provenance(from, delivery));
    } finally {
      await actors.close(); await agents.close(); main.closeFollowUpDrain();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("resident actor admission and journal send reach the real Pi API with the original actor identity", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-resident-provenance-"));
    const { fake, pi } = recording();
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const main = new MainAgentController(pi, host.id, true, root, "host");
    const ctx = { isIdle: () => true, hasPendingMessages: () => false, signal: new AbortController().signal,
      sessionManager: { getEntries: () => [], isPersisted: () => false } } as unknown as ExtensionContext;
    const file = path.join(root, "main-followups.json");
    main.attachFollowUpDrain(ctx, 120_000, file);
    const from: MeshIdentity = { id: "actor:resident", name: "Resident", kind: "actor" };
    const key = residentDeliveryPrefix(host.id) + "durable";
    await mesh.put({ key, identity: { id: residentHostId(host.id), name: "resident host", kind: "main" },
      value: { format: RESIDENT_HOST_FORMAT, id: "durable", rootId: host.id, from, message: "I am Paul",
        delivery: "steer", triggerTurn: true, createdAt: 1, data: { sender: "paul" } } });
    const client = new ResidencyClient({ mainAgent: main, mesh, participants: {} as any,
      config: { rootId: host.id, residencyRoot: path.join(root, "resident"), mesh: { actorPollMs: 20 } } as any });
    try {
      client.start(); // Delivery polling only: never calls ensureHost or starts an AI process.
      await vi.waitFor(() => expect(fake.sendMessage).toHaveBeenCalledOnce());
      await vi.waitFor(() => expect(mesh.get(key)).toBeUndefined());
      expect(fake.sendMessage.mock.calls[0]![1].provenance).toEqual(provenance(from, "steer"));
      const admitted = JSON.parse(fs.readFileSync(file, "utf8")).items[0];
      expect(admitted.provenance).toEqual(provenance(from, "steer"));
      expect(admitted.deliveryId).toBe(`resident:${host.id}:durable`);
    } finally {
      await client.close(); main.closeFollowUpDrain();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("the legacy user-message adapter preserves a one-argument call and explicit template expansion", () => {
    const { fake, pi } = recording(false);
    sendFabricUserMessage(pi, "task", host, "followUp");
    sendFabricUserMessage(pi, "/fabric-release-reload auto", host, "followUp", { expandPromptTemplates: true });
    expect(fake.sendUserMessage.mock.calls).toEqual([["task"], ["/fabric-release-reload auto", { expandPromptTemplates: true }]]);
  });
});
