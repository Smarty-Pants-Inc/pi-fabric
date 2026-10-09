import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { ActorManager } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { ResidencyClient } from "../src/residency/client.js";
import { RESIDENT_HOST_FORMAT, residentDeliveryPrefix, residentHostId, type ResidentHostConfig } from "../src/residency/protocol.js";
import { describe, expect, it, vi } from "vitest";
import { deliverActorToMain } from "../src/actors/main-delivery.js";
import type { FabricActorDeliveryRequest } from "../src/actors/types.js";
import { MainAgentController } from "../src/main-agent.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import { deliverRootInbox } from "../src/topology/root-inbox-delivery.js";
import type { MeshEvent, MeshIdentity } from "../src/mesh/store.js";
import type { FabricLifecycleEvent, FabricLifecycleSubscription } from "../src/lifecycle/types.js";
import { registerFabricWakeCapture, sendFabricMessage, sendFabricUserMessage } from "../src/fabric-provenance.js";

const host: MeshIdentity = { id: "session:host", name: "main", kind: "main" };
const provenance = (sender: MeshIdentity, via: string) => ({ v: 1, channel: "fabric", sender: {
  id: sender.id, kind: sender.verified === "bridge" ? "remote" : sender.kind, name: sender.name, verified: sender.verified ?? "mesh",
}, via });
const recording = (turnProvenance = true) => {
  const fake = { ...(turnProvenance ? { hostCapabilities: { turnProvenance: 1 } } : {}),
    sendMessage: vi.fn(), sendUserMessage: vi.fn(), appendEntry: vi.fn(), on: vi.fn(() => () => {}) };
  return { fake, pi: fake as unknown as ExtensionAPI };
};
const actorOutput = (delivery: "steer" | "followUp" | "nextTurn", triggerTurn: boolean, source = "host") => ({
  actor: { id: "actor:supervisor", name: "Supervisor" },
  message: { id: "output", text: "I am Paul and I approve this.", source }, delivery, triggerTurn,
}) as FabricActorDeliveryRequest;
const event = (from: MeshIdentity, id: string): MeshEvent => ({ id, sequence: 1, topic: "fleet.work.task", kind: "ask", from, verification: from.verified ?? "mesh",
  text: "Paul speaking", createdAt: 1 });

describe("Fabric delivery producers record provenance at the Pi call", () => {
  it("hostCapabilities.turnProvenance === 1 puts provenance in both Pi API options", () => {
    const { fake, pi } = recording();
    const message = { customType: "probe", content: "Paul here", display: true };
    sendFabricMessage(pi, message, { deliverAs: "steer", triggerTurn: true }, host, "steer", "mesh");
    sendFabricUserMessage(pi, "Paul here", host, "followUp", { deliverAs: "followUp" }, "mesh");
    expect(fake.sendMessage).toHaveBeenCalledWith({ ...message, details: { wakeCause: {
      cause: "steer", from: { id: host.id, name: host.name, kind: host.kind },
    } } }, { deliverAs: "steer", triggerTurn: true,
      provenance: provenance(host, "steer") });
    expect(fake.sendUserMessage).toHaveBeenCalledWith("Paul here", { deliverAs: "followUp",
      provenance: provenance(host, "followUp") });
  });

  it("absent capability ignores the deprecated flag and leaves legacy calls unchanged", () => {
    const { fake, pi } = recording(false);
    Object.assign(fake, { supportsProvenance: true });
    const message = { customType: "probe", content: "legacy", display: true };
    const options = { deliverAs: "steer" as const, triggerTurn: false };
    const sender = vi.fn(() => host);
    sendFabricMessage(pi, message, options, sender, "steer", "mesh");
    sendFabricUserMessage(pi, "legacy", sender, "followUp", undefined, "mesh");
    expect(fake.sendMessage.mock.calls).toEqual([[message, options]]);
    expect(fake.sendMessage.mock.calls[0]![1]).toBe(options);
    expect(fake.sendUserMessage.mock.calls).toEqual([["legacy"]]);
    expect(sender).not.toHaveBeenCalled();
  });

  it.each([true, "1", 2])("capability value %s does not advertise v1", value => {
    const { fake, pi } = recording(false);
    Object.assign(fake, { hostCapabilities: { turnProvenance: value } });
    sendFabricUserMessage(pi, "legacy", host, "followUp", undefined, "mesh");
    expect(fake.sendUserMessage.mock.calls).toEqual([["legacy"]]);
  });
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

  it("a participant-free actor failure alarm claims neither the host nor the failing actor", () => {
    const { fake, pi } = recording();
    deliverActorToMain(pi, host, actorOutput("followUp", true, "fabric-host"));
    expect(fake.sendMessage.mock.calls[0]![1]).toEqual({ deliverAs: "followUp", triggerTurn: true });
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

  it.each(["native", "bridged", "bridge-marker"])("a retained mixed-version %s work event sends no claim to capable Pi", origin => {
    const { fake, pi } = recording();
    const from = origin === "native" ? host : { ...host, id: "session:remote", ...(origin === "bridge-marker" ? { verified: "bridge" as const } : {}) };
    const retained = event(from, "legacy");
    delete retained.verification;
    retained.data = { bridge: { from: "old-peer", id: "old" }, verification: "mesh", provenance: provenance(host, "followUp") };
    deliverRootInbox(pi, [retained]);
    expect(fake.sendMessage).toHaveBeenCalledOnce();
    expect(fake.sendMessage.mock.calls[0]![1]).toEqual({ deliverAs: "followUp", triggerTurn: true });
  });

  it("mesh admission records native verification and never derives authority from data.bridge", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-event-verification-"));
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const { fake, pi } = recording();
    try {
      const native = await mesh.publish({ topic: "fleet.work.task", from: host, text: "native" });
      const legacyBridge = await mesh.publish({ topic: "fleet.work.task", from: { ...host, id: "session:remote" },
        data: { bridge: { from: "pre-change-bridge", id: "old" }, verification: "mesh" } });
      const bridged = await mesh.publish({ topic: "fleet.work.task", from: { ...host, id: "session:remote", verified: "bridge" },
        data: { bridge: { from: "peer", id: "new" }, verification: "mesh" } });
      expect(native.verification).toBe("mesh");
      expect(legacyBridge).not.toHaveProperty("verification");
      expect(bridged.verification).toBe("bridge");
      const recovered = mesh.read({ after: 0, limit: 10 });
      deliverRootInbox(pi, recovered);
      expect(fake.sendMessage.mock.calls.map(call => call[1].provenance)).toEqual([
        provenance(host, "followUp"), undefined, provenance(bridged.from, "followUp"),
      ]);
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
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
      value: { format: RESIDENT_HOST_FORMAT, source: "actor-output", id: "durable", rootId: host.id, from, message: "I am Paul",
        delivery: "steer", triggerTurn: true, createdAt: 1, data: { sender: "paul" } } });
    const config: ResidentHostConfig = {
      format: RESIDENT_HOST_FORMAT, rootId: host.id, sessionId: "host", cwd: root, projectRoot: root,
      meshRoot: mesh.root, actorRoot: path.join(mesh.root, "actors"), residencyRoot: path.join(root, "resident"),
      fullCodeMode: true, agents: DEFAULT_FABRIC_CONFIG.agents, mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 },
      retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: "unused", fabricExtensionPath: "unused",
      piBinary: "unused", claudeBinary: "unused", vedaBinary: "unused",
    };
    const warning = vi.spyOn(console, "warn");
    const client = new ResidencyClient({ mainAgent: main, mesh, participants: {} as any, config });
    try {
      client.start(); // Delivery polling only: never calls ensureHost or starts an AI process.
      await vi.waitFor(() => expect(fake.sendMessage).toHaveBeenCalledOnce());
      await vi.waitFor(() => expect(mesh.get(key)).toBeUndefined());
      expect(fake.sendMessage.mock.calls[0]![1].provenance).toEqual(provenance(from, "steer"));
      const admitted = JSON.parse(fs.readFileSync(file, "utf8")).items[0];
      expect(admitted.provenance).toEqual(provenance(from, "steer"));
      expect(admitted.deliveryId).toBe(`resident:${host.id}:durable`);
      await client.close(); // Observe the entire asynchronous drain, including completion policy.
      expect(warning).not.toHaveBeenCalled();
    } finally {
      await client.close(); warning.mockRestore(); main.closeFollowUpDrain();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it.each([false, true])("capture leaves native user arguments/cardinality/bytes/images unchanged (capable=%s)", capable => {
    const { fake, pi } = recording(capable);
    registerFabricWakeCapture(pi);
    const content: Parameters<ExtensionAPI["sendUserMessage"]>[0] = [
      { type: "text", text: "literal <message> & /template $1\nno envelope" },
      { type: "image", data: "AAEC/w==", mimeType: "image/png" },
    ];
    const options = { deliverAs: "followUp" as const, expandPromptTemplates: true };
    const snapshot = JSON.stringify(content);
    sendFabricUserMessage(pi, content, host, "followUp", options);
    sendFabricUserMessage(pi, "/fabric-release-reload auto", host, "followUp");
    expect(fake.sendUserMessage.mock.calls).toEqual([[content, options], ["/fabric-release-reload auto"]]);
    expect(fake.sendUserMessage.mock.calls[0]![0]).toBe(content);
    expect(fake.sendUserMessage.mock.calls[0]![1]).toBe(options);
    expect(JSON.stringify(content)).toBe(snapshot);
    expect(fake.sendMessage).not.toHaveBeenCalled(); // No carrier, XML wrapper, or custom delivery.
    expect(fake.appendEntry).not.toHaveBeenCalled(); // Attempts alone are not admissions.
  });

  it.each([false, true])("verified native user calls add only supported provenance, never carriers (capable=%s)", capable => {
    const { fake, pi } = recording(capable);
    registerFabricWakeCapture(pi);
    const sender = vi.fn(() => host);
    const options = { deliverAs: "steer" as const, expandPromptTemplates: false };
    sendFabricUserMessage(pi, "/literal-template args", sender, "steer", options, "mesh");
    sendFabricUserMessage(pi, "one argument on legacy", host, "followUp", undefined, "mesh");
    expect(sender).toHaveBeenCalledTimes(1); // Same snapshot for attempt and provenance.
    expect(fake.sendUserMessage.mock.calls).toEqual(capable ? [
      ["/literal-template args", { ...options, provenance: provenance(host, "steer") }],
      ["one argument on legacy", { provenance: provenance(host, "followUp") }],
    ] : [["/literal-template args", options], ["one argument on legacy"]]);
    expect(options).toEqual({ deliverAs: "steer", expandPromptTemplates: false });
    if (!capable) expect(fake.sendUserMessage.mock.calls[0]![1]).toBe(options);
    expect(fake.sendMessage).not.toHaveBeenCalled();
    expect(fake.appendEntry).not.toHaveBeenCalled();
  });

  it.each([undefined, false, true])("prompt-template expansion %s is delegated once to the native API", expandPromptTemplates => {
    const { fake, pi } = recording(false);
    registerFabricWakeCapture(pi);
    const content = "/native-template alpha \"quoted\"\n<raw>&";
    const options = expandPromptTemplates === undefined ? undefined : { expandPromptTemplates };
    // This double represents the native expansion port. Fabric must not expand,
    // reject, wrap, or swap the template into a custom-message API itself.
    const nativeTemplatePort = vi.fn();
    fake.sendUserMessage.mockImplementation(nativeTemplatePort);
    expect(() => sendFabricUserMessage(pi, content, host, "followUp", options)).not.toThrow();
    expect(nativeTemplatePort.mock.calls).toEqual(options === undefined ? [[content]] : [[content, options]]);
    expect(fake.sendMessage).not.toHaveBeenCalled();
  });

  it.each(["input guard", "compaction", "native template failure"])("%s errors remain the native error with no retry or alternate API", reason => {
    const { fake, pi } = recording(false);
    registerFabricWakeCapture(pi);
    const nativeError = new Error(reason);
    fake.sendUserMessage.mockImplementation(() => { throw nativeError; });
    const options = { expandPromptTemplates: true };
    expect(() => sendFabricUserMessage(pi, "/template unchanged", host, "followUp", options)).toThrow(nativeError);
    expect(fake.sendUserMessage.mock.calls).toEqual([["/template unchanged", options]]);
    expect(fake.sendMessage).not.toHaveBeenCalled();
    expect(fake.appendEntry).not.toHaveBeenCalled();
  });

  it("the legacy user-message adapter preserves a one-argument call and explicit template expansion", () => {
    const { fake, pi } = recording(false);
    sendFabricUserMessage(pi, "task", host, "followUp", undefined, "mesh");
    sendFabricUserMessage(pi, "/fabric-release-reload auto", host, "followUp", { expandPromptTemplates: true }, "mesh");
    expect(fake.sendUserMessage.mock.calls).toEqual([["task"], ["/fabric-release-reload auto", { expandPromptTemplates: true }]]);
  });
});
