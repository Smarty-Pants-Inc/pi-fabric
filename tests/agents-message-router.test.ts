import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { Agent, type AgentMessage, type QueueMode } from "@earendil-works/pi-agent-core";
import { createFauxCore, fauxAssistantMessage } from "@earendil-works/pi-ai";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import principalDelivery from "../src/worker/principal-delivery.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { AgentMessageRouter } from "../src/providers/agents-message-router.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import type { FabricParticipantInfo } from "../src/topology/types.js";

const message = "followUp to a running task waits until its current run finishes; use agents.steer for a correction needed before completion.";
const warning = (targetId: string) => ({ code: "FABRIC_FOLLOW_UP_RUNNING_TASK", targetId, kind: "agent", status: "running", message });
const identity: MeshIdentity = { id: "session:owner", name: "Owner", kind: "main" };
const roots: string[] = [];
const managers: AgentManager[] = [];
const planes: FabricControlPlane[] = [];
type Ports = ConstructorParameters<typeof AgentMessageRouter>;

const router = (manager: Ports[0], entries: FabricParticipantInfo[] = [], control?: Ports[4]) => {
  const actors = {
    identity, validateDirectMessage: vi.fn(),
    status: vi.fn((id: string) => {
      if (id !== "actor:running") throw new Error(`Unknown Fabric actor: ${id}`);
      return { id, status: "running", runner: "pi" };
    }),
    owns: () => true, tell: vi.fn(() => ({ messageId: "mailbox" })),
    ask: vi.fn(), stop: vi.fn(), steerRemote: vi.fn(), resolveBinding: vi.fn(),
  } as unknown as Ports[1];
  const main = { id: identity.id, local: true, matches: (id: string) => id === "main" || id === identity.id,
    deliverAgent: vi.fn(() => ({ queued: true, messageId: "main-queue", routed: "main" })) } as unknown as Ports[2];
  const participants = { get: (id: string) => entries.find(p => p.id === id), scheduleRefresh: vi.fn(), lastKnown: () => undefined };
  return { value: new AgentMessageRouter(manager, actors, main, participants, control, b => b), actors, main };
};
const running = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-followup-advisory-")); roots.push(root);
  const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
    runRoot: path.join(root, "runs"), workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fullCodeMode: false,
  });
  managers.push(manager);
  const handle = await manager.spawn({ task: "HANG", transport: "process" });
  const run = manager.runDirectory(handle.id)!;
  const statusFile = path.join(run, "status.json");
  await vi.waitFor(() => {
    expect(fs.existsSync(statusFile)).toBe(true);
    expect(JSON.parse(fs.readFileSync(statusFile, "utf8")).status).toBe("running");
  });
  const record = JSON.parse(fs.readFileSync(statusFile, "utf8"));
  const entries = () => fs.existsSync(path.join(run, "steer.jsonl"))
    ? fs.readFileSync(path.join(run, "steer.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line)) : [];
  return { root, manager, id: handle.id, statusFile, record, entries };
};
const remote = (id: string, status = "running", kind = "agent") => ({ id, kind, status, local: false,
  ownerHostId: "host:owner", ownerIdentityId: "host:owner", capabilities: ["steer", "followUp"] } as FabricParticipantInfo);
const unknown = { status: (id: string) => { throw new Error(`Unknown Fabric agent: ${id}`); } } as unknown as Ports[0];

afterEach(async () => {
  await Promise.all(planes.splice(0).map(p => p.close()));
  await Promise.all(managers.splice(0).map(m => m.close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("running-task followUp advisory (#3005)", () => {
  it("A1 local receipt warns after exactly one tracked follow_up append with unchanged payload", async () => {
    const f = await running();
    const data = { private: "unchanged" }, admittedAt = Date.now();
    const receipt = await router(f.manager).value.routeMessage(f.id, "later", data, "followUp");
    expect(f.entries()).toEqual([{ type: "follow_up", message: "later", data, provenance: expect.any(Object),
      followUpId: receipt.messageId, deadlineAt: receipt.deadlineAt, id: receipt.messageId, ts: expect.any(Number) }]);
    expect(f.entries()[0]).not.toHaveProperty("warning");
    expect(receipt).toEqual({ queued: true, messageId: expect.any(String), routed: "local", warning: warning(f.id), deadlineAt: expect.any(Number) });
    expect(receipt.deadlineAt).toBeGreaterThanOrEqual(admittedAt + 600_000);
    expect(receipt.deadlineAt).toBeLessThanOrEqual(Date.now() + 600_000);
    expect(f.manager.status(f.id).followUpDeliveries).toEqual([{ messageId: receipt.messageId, deadlineAt: receipt.deadlineAt, state: "queued" }]);
  });

  it.each([
    { followUpMode: "all", steeringMode: "one-at-a-time", firstBatch: ["FOLLOW_FIRST", "FOLLOW_SECOND"] },
    { followUpMode: "one-at-a-time", steeringMode: "all", firstBatch: ["FOLLOW_FIRST"] },
  ] as const)("public tracked follow-ups honour $followUpMode independently of steering $steeringMode", async ({ followUpMode, steeringMode, firstBatch }) => {
    const f = await running(), r = router(f.manager).value;
    f.manager.setSteeringMode(f.id, steeringMode);
    f.manager.setFollowUpMode(f.id, followUpMode);
    const run = f.manager.runDirectory(f.id)!, directory = path.join(run, "deliveries");
    fs.mkdirSync(directory, { recursive: true });
    const previous = process.env.PI_FABRIC_DELIVERY_DIR;
    const handlers = new Map<string, (...args: any[]) => any>();
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    const contexts: AgentMessage[][] = [];
    const faux = createFauxCore({ tokensPerSecond: 100_000 });
    faux.setResponses(Array.from({ length: 4 }, () => async (_context, _options, state) => {
      if (state.callCount === 1) await gate;
      return fauxAssistantMessage("done");
    }));
    const modes = f.entries();
    const receiver = new Agent({
      initialState: { model: faux.getModel() }, streamFn: faux.streamSimple,
      steeringMode: modes[0].mode as QueueMode, followUpMode: modes[1].mode as QueueMode,
      transformContext: async messages => {
        const result = await handlers.get("context")?.({ messages }, {});
        const consumed = result?.messages ?? messages;
        contexts.push(consumed);
        return consumed;
      },
    });
    const send = vi.fn((text: string, options: { deliverAs: "steer" | "followUp" }) => {
      const message: AgentMessage = { role: "user", content: [{ type: "text", text }], timestamp: Date.now() };
      if (options.deliverAs === "steer") receiver.steer(message);
      else receiver.followUp(message);
    });
    process.env.PI_FABRIC_DELIVERY_DIR = directory;
    try {
      principalDelivery({
        registerCommand: (_name: string, command: any) => handlers.set("command", command.handler),
        on: (name: string, handler: any) => handlers.set(name, handler), sendUserMessage: send,
      } as unknown as ExtensionAPI);
    } finally {
      if (previous === undefined) delete process.env.PI_FABRIC_DELIVERY_DIR;
      else process.env.PI_FABRIC_DELIVERY_DIR = previous;
    }
    receiver.subscribe(async event => {
      if (event.type === "turn_end") await handlers.get("turn_end")?.(event, { isIdle: () => false, signal: receiver.signal });
    });
    const processing = receiver.prompt("initial request");
    try {
      await vi.waitFor(() => expect(faux.state.callCount).toBe(1));
      const receipts = [];
      for (const marker of ["FOLLOW_FIRST", "FOLLOW_SECOND", "FOLLOW_CANCELLED"]) {
        const receipt = await r.routeMessage(f.id, marker, undefined, "followUp");
        receipts.push(receipt);
        const entry = f.entries().at(-1)!;
        expect(entry).toMatchObject({ followUpId: receipt.messageId, deadlineAt: receipt.deadlineAt });
        fs.writeFileSync(path.join(directory, receipt.messageId + ".json"), JSON.stringify({
          message: entry.message, delivery: "followUp", followUpId: entry.followUpId, provenance: entry.provenance,
        }));
        await handlers.get("command")!(receipt.messageId, { isIdle: () => false });
      }
      expect(send).not.toHaveBeenCalled();
      expect(f.manager.status(f.id).followUpDeliveries?.map(d => d.state)).toEqual(["queued", "queued", "queued"]);
      expect(f.manager.cancelFollowUp(f.id, receipts[2]!.messageId).state).toBe("cancelled");
      release(); await processing;
      const markers = (messages: AgentMessage[]) => messages.flatMap(m => m.role === "user" && Array.isArray(m.content)
        ? m.content.flatMap(c => c.type === "text" && c.text.startsWith("FOLLOW_") ? [c.text] : []) : []);
      expect(markers(contexts[1]!)).toEqual(firstBatch);
      expect(markers(contexts.at(-1)!)).toEqual(["FOLLOW_FIRST", "FOLLOW_SECOND"]);
      expect(send.mock.calls.map(call => call[1].deliverAs)).toEqual(["followUp", "followUp"]);
      expect(f.manager.status(f.id).followUpDeliveries?.map(d => d.state)).toEqual(["delivered", "delivered", "cancelled"]);
      expect(receipts.every(receipt => Number.isSafeInteger(receipt.deadlineAt))).toBe(true);
    } finally {
      release(); receiver.abort(); await processing;
    }
  });
  it("A2 ordinary remote owner ACK and replay retain the warning without re-enqueue", async () => {
    const f = await running();
    const meshRoot = path.join(f.root, "mesh");
    const make = (id: string) => {
      const p = new FabricControlPlane(new MeshStore(meshRoot, 64 * 1024, 1000), { ...identity, id },
        { enabled: true, hostId: id, pollMs: 20, acknowledgementTimeoutMs: 2000 });
      planes.push(p); return p;
    };
    const owner = make("host:owner"), sender = make("host:sender");
    const accept = vi.fn((...args: Parameters<AgentMessageRouter["acceptControl"]>) => router(f.manager).value.acceptControl(...args));
    owner.start(accept); sender.start(() => ({ accepted: false }));
    const receipt = await router(unknown, [remote(f.id, "idle")], sender).value.routeMessage(f.id, "later", undefined, "followUp");
    const command = sender.mesh.read({ topic: "fabric.control.command", limit: 10 })[0]!;
    await owner.close(); // Replay through a new owner: exercise the persisted seen outcome, not an in-memory map.
    const restarted = make("host:owner"); restarted.start(accept);
    await sender.mesh.publish({ topic: command.topic, kind: command.kind, from: command.from, to: command.to!, data: command.data });
    await vi.waitFor(() => expect(sender.mesh.read({ topic: "fabric.control.ack", limit: 10 }).length).toBeGreaterThanOrEqual(2));
    expect(accept).toHaveBeenCalledOnce(); expect(f.entries()).toHaveLength(1);
    for (const ack of sender.mesh.read({ topic: "fabric.control.ack", limit: 10 })) expect(ack.data).toMatchObject({ accepted: true, warning: warning(f.id) });
    expect(receipt).toEqual({ queued: true, messageId: expect.any(String), routed: "mesh", acknowledged: true, warning: warning(f.id) });
  });

  it("A3 steer, queue-mode and compact keep their entries and never warn", async () => {
    const f = await running();
    const receipt = await router(f.manager).value.routeMessage(f.id, "correct now", undefined, "steer");
    const receipts = [receipt, f.manager.setSteeringMode(f.id, "all"), f.manager.setFollowUpMode(f.id, "one-at-a-time"), f.manager.compact(f.id, "keep facts")];
    for (const result of receipts) expect(result).not.toHaveProperty("warning");
    expect(f.entries().map(e => e.type)).toEqual(["steer", "set_steering_mode", "set_follow_up_mode", "compact"]);
    expect(f.entries()[0].message).toBe("correct now"); expect(f.entries()[3].instructions).toBe("keep facts");
  });

  it("A4 running Main, peer root and actor keep their original routes without task admission", async () => {
    const admission = vi.fn(() => { throw new Error("Unexpected task admission"); });
    const manager = { ...unknown, followUp: admission };
    const request = vi.fn(async () => ({ queued: true as const, messageId: "peer", routed: "mesh" as const, acknowledged: true as const }));
    const r = router(manager, [remote("session:peer", "running", "root")], { request });
    expect(await r.value.routeMessage("main", "later", undefined, "followUp")).toEqual({ queued: true, messageId: "main-queue", routed: "main" });
    expect(await r.value.routeMessage("session:peer", "later", undefined, "followUp")).toEqual({ queued: true, messageId: "peer", routed: "mesh", acknowledged: true });
    expect(await r.value.routeMessage("actor:running", "later", undefined, "followUp")).toEqual({ queued: true, messageId: "mailbox", routed: "local" });
    expect(r.main.deliverAgent).toHaveBeenCalledOnce(); expect(request).toHaveBeenCalledOnce(); expect(r.actors.tell).toHaveBeenCalledOnce(); expect(admission).not.toHaveBeenCalled();
    expect(await r.value.acceptControl({ operation: "followUp", targetId: identity.id, message: "later", commandId: "root" } as never, identity)).not.toHaveProperty("warning");
    expect(await r.value.acceptControl({ operation: "followUp", targetId: "actor:running", message: "later", commandId: "actor" } as never, identity)).not.toHaveProperty("warning");
  });

  it("A5 owner status wins over stale mirrors in both directions", async () => {
    const f = await running();
    const owner = router(f.manager).value;
    const control = { request: vi.fn(async (_host: string, id: string, operation: "steer" | "followUp", input: { message?: string }) => {
      const acceptance = await owner.acceptControl({ targetId: id, operation, message: input.message, commandId: "stale" } as never, identity);
      if (!acceptance.accepted) throw new Error(acceptance.error);
      return { queued: true as const, routed: "mesh" as const, acknowledged: true as const, ...acceptance };
    }) } as unknown as Ports[4];
    fs.writeFileSync(f.statusFile, JSON.stringify({ ...f.record, status: "completed" }));
    await expect(router(unknown, [remote(f.id)], control).value.routeMessage(f.id, "too late", undefined, "followUp")).rejects.toThrow(/already finished/);
    expect(f.entries()).toEqual([]);
    fs.writeFileSync(f.statusFile, JSON.stringify(f.record));
    const receipt = await router(unknown, [remote(f.id, "idle")], control).value.routeMessage(f.id, "later", undefined, "followUp");
    expect(f.entries()).toHaveLength(1); expect(receipt).toHaveProperty("warning", warning(f.id));
  });

  it.each(["missing", "queued"])("A6 %s owner record does not invent running status", async status => {
    const f = await running();
    if (status === "missing") fs.rmSync(f.statusFile);
    else fs.writeFileSync(f.statusFile, JSON.stringify({ ...f.record, status }));
    const receipt = f.manager.followUp(f.id, "later");
    expect(receipt).toEqual({ queued: true, messageId: expect.any(String) });
    expect(f.entries()).toHaveLength(1);
  });
});
