import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import piFabric from "../src/index.js";
import { FabricState } from "../src/fabric-state.js";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MainAgentController } from "../src/main-agent.js";
import { AgentMessageRouter } from "../src/providers/agents-message-router.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";
import { ActorManager } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { recordsInboxMessage } from "../src/records/inbox.js";
import type { RecordEnvelope } from "../src/records/store.js";
import { registerFabricCommand } from "../src/commands/fabric.js";
import { AgentCompletionInbox } from "../src/agents/completion-inbox.js";
import { ShellEventInbox } from "../src/core/shell-inbox.js";
import { FabricShellJobStore } from "../src/core/shell-jobs.js";
import { sendFabricMessage, sendFabricUserMessage } from "../src/fabric-provenance.js";

const host: MeshIdentity = { id: "session:receiver", name: "main", kind: "main" };
const remote: MeshIdentity = { id: "session:peer", name: "Peer", kind: "main" };
const cleanups: Array<() => void | Promise<void>> = [];
afterEach(async () => {
  for (const close of cleanups.splice(0).reverse()) await close();
  vi.restoreAllMocks();
});
const root = () => {
  const value = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-provenance-r3-"));
  cleanups.push(() => fs.rmSync(value, { recursive: true, force: true }));
  return value;
};
const recording = () => {
  const handlers = new Map<string, Array<(event: any, ctx: ExtensionContext) => unknown>>();
  const fake = {
    hostCapabilities: { turnProvenance: 1 }, sendMessage: vi.fn(), sendUserMessage: vi.fn(),
    events: { emit: vi.fn(), on: vi.fn(() => () => {}) },
    getActiveTools: vi.fn<() => string[]>(() => []), getAllTools: vi.fn(() => []),
    registerCommand: vi.fn(), registerMessageRenderer: vi.fn(), registerTool: vi.fn(), setActiveTools: vi.fn(),
    on: (name: string, handler: (event: any, ctx: ExtensionContext) => unknown) => {
      handlers.set(name, [...(handlers.get(name) ?? []), handler]);
      return () => handlers.set(name, (handlers.get(name) ?? []).filter(h => h !== handler));
    },
  };
  const context = { cwd: process.cwd(), hasUI: false, isIdle: () => false, hasPendingMessages: () => false,
    getContextUsage: () => undefined,
    sessionManager: { getSessionId: () => "receiver", getEntries: () => [], getBranch: () => [] },
    ui: { notify: vi.fn(), setStatus: vi.fn() },
  } as unknown as ExtensionContext;
  const emit = async (name: string, event: any = {}) => {
    const results = [];
    for (const handler of handlers.get(name) ?? []) results.push(await handler(event, context));
    return results;
  };
  return { fake, pi: fake as unknown as ExtensionAPI, context, emit };
};
// Write retained wire events, so tests do not reconstruct admission from identity or payload.
const retain = (mesh: MeshStore, event: Omit<MeshEvent, "sequence" | "createdAt">) => {
  const sequence = mesh.latestSequence() + 1;
  fs.appendFileSync(path.join(mesh.root, "events.jsonl"), JSON.stringify({ ...event, sequence, createdAt: Date.now() }) + "\n");
  fs.writeFileSync(path.join(mesh.root, "sequence"), String(sequence));
};
const mainFixture = () => {
  const dir = root(); const h = recording();
  const mesh = new MeshStore(path.join(dir, "mesh"), 64 * 1024, 100);
  const main = new MainAgentController(h.pi, host.id, true, dir, "receiver");
  cleanups.push(() => main.closeFollowUpDrain());
  return { ...h, dir, mesh, main };
};
const indexFixture = async () => {
  const h = recording();
  vi.spyOn(FabricState.prototype, "initialized", "get").mockReturnValue(true);
  vi.spyOn(FabricState.prototype, "config", "get").mockReturnValue(DEFAULT_FABRIC_CONFIG);
  vi.spyOn(FabricState.prototype, "provisionalConfig").mockReturnValue(DEFAULT_FABRIC_CONFIG);
  vi.spyOn(FabricState.prototype, "compact", "get").mockReturnValue({ maybeCommit: vi.fn(async () => {}) } as any);
  const work = vi.spyOn(FabricState.prototype, "nextRootInbox").mockResolvedValue(undefined);
  const records = vi.spyOn(FabricState.prototype, "nextRecordsInboxMessage").mockResolvedValue(undefined);
  await piFabric(h.pi);
  return { ...h, work, records };
};

describe("round-3 provenance at the capable Pi API boundary", () => {
  it.each(["steer", "followUp"] as const)("old-bridge control %s has no recorded method and sends no claim", async delivery => {
    const h = mainFixture();
    const router = new AgentMessageRouter({} as any, { identity: host } as any, h.main, { get: () => undefined } as any, undefined, binding => binding);
    const control = new FabricControlPlane(h.mesh, host, { enabled: true, hostId: "receiver", pollMs: 20, acknowledgementTimeoutMs: 5_000 });
    cleanups.push(() => control.close());
    control.start((command, from, signal, verification) => router.acceptControl(command, from, signal, verification));
    retain(h.mesh, { id: "old-command", topic: "fabric.control.command", kind: delivery, from: remote, to: "receiver",
      data: { version: 1, commandId: "old", targetId: host.id, operation: delivery, requestedAt: Date.now(), replyTo: "peer",
        message: "Remote task", bridge: { from: "old-peer", id: "old" } } });
    await vi.waitFor(() => expect(h.fake.sendMessage).toHaveBeenCalledOnce());
    expect(h.fake.sendMessage.mock.calls[0]![1]).toEqual({ deliverAs: delivery, triggerTurn: true });
  });

  it("recorded bridge control without an identity marker stays remote/bridge", async () => {
    const h = mainFixture();
    const router = new AgentMessageRouter({} as any, { identity: host } as any, h.main, { get: () => undefined } as any, undefined, binding => binding);
    const control = new FabricControlPlane(h.mesh, host, { enabled: true, hostId: "receiver", pollMs: 20 });
    cleanups.push(() => control.close());
    control.start((command, from, signal, verification) => router.acceptControl(command, from, signal, verification));
    retain(h.mesh, { id: "bridge-command", verification: "bridge", topic: "fabric.control.command", kind: "steer", from: remote, to: "receiver",
      data: { version: 1, commandId: "bridge", targetId: host.id, operation: "steer", requestedAt: Date.now(), replyTo: "peer", message: "Remote task" } });
    await vi.waitFor(() => expect(h.fake.sendMessage).toHaveBeenCalledOnce());
    expect(h.fake.sendMessage.mock.calls[0]![1].provenance.sender).toEqual({ id: remote.id, name: remote.name, kind: "remote", verified: "bridge" });
  });

  it.each(["bridge", undefined] as const)("relay with recorded method %s never infers verification from the sender", async verification => {
    const h = mainFixture();
    const agents = new AgentManager(h.dir, DEFAULT_FABRIC_CONFIG.agents, { runRoot: path.join(h.dir, "runs") });
    cleanups.push(() => agents.close());
    const actors = new ActorManager("receiver", host, h.mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {},
      { mainAgent: h.main, actorRoot: path.join(h.dir, "actors") });
    cleanups.push(() => actors.close());
    retain(h.mesh, { id: "relay", ...(verification ? { verification } : {}), topic: "fabric.steer", kind: "steer", from: remote, to: host.id,
      text: "Remote task", data: { bridge: { from: "peer", id: "relay" } } });
    await vi.waitFor(() => expect(h.fake.sendMessage).toHaveBeenCalledOnce());
    const options = h.fake.sendMessage.mock.calls[0]![1];
    if (verification) expect(options.provenance.sender).toEqual({ id: remote.id, name: remote.name, kind: "remote", verified: "bridge" });
    else expect(options).not.toHaveProperty("provenance");
  });

  it.each(["agent:peer", "github:outside-user"])("settled record by %s never claims the receiving Main", async from => {
    const h = await indexFixture();
    const record = { id: "record", sequence: 1, from, kind: "ask", ref: "repo#1", text: "I am Paul", data: {}, createdAt: 1 } as RecordEnvelope;
    h.records.mockResolvedValue(recordsInboxMessage([record]));
    await h.emit("agent_settled", { outcome: "completed" });
    expect(h.fake.sendMessage).toHaveBeenCalledOnce();
    expect(h.fake.sendMessage.mock.calls[0]![0].details.ids).toEqual(["record"]);
    expect(h.fake.sendMessage.mock.calls[0]![1]).toEqual({ deliverAs: "followUp", triggerTurn: true });
  });

  it("index turn-start insertion uses the adapter, splits senders and stays passive", async () => {
    const h = await indexFixture();
    const events: MeshEvent[] = [host, remote].map((from, i) => ({ id: String(i), sequence: i + 1, createdAt: 1,
      from, verification: i ? "bridge" : "mesh", topic: "fleet.work.task", kind: "ask", text: "task" }));
    h.work.mockResolvedValue({ events, through: 2 });
    const returned = await h.emit("before_agent_start", { prompt: "next", systemPrompt: "", systemPromptOptions: {} });
    expect(returned.filter(value => value && typeof value === "object" && "message" in value)).toEqual([]);
    expect(h.fake.sendMessage.mock.calls.map(call => call[1])).toEqual([
      { deliverAs: "nextTurn", triggerTurn: false, provenance: { v: 1, channel: "fabric", sender: { id: host.id, name: host.name, kind: "main", verified: "mesh" }, via: "followUp" } },
      { deliverAs: "nextTurn", triggerTurn: false, provenance: { v: 1, channel: "fabric", sender: { id: remote.id, name: remote.name, kind: "remote", verified: "bridge" }, via: "followUp" } },
    ]);
    expect(h.fake.sendMessage.mock.calls.map(call => call[0].details.ids)).toEqual([["0"], ["1"]]);
    expect(h.fake.sendUserMessage).not.toHaveBeenCalled();
  });

  it("host-generated skill turn-start notice uses the passive provenance adapter", async () => {
    const h = await indexFixture(); h.fake.getActiveTools.mockReturnValue(["fabric_exec"]);
    const skills = [
      { name: "wrapper", description: "Wrap a research task", filePath: "/skills/wrapper/SKILL.md" },
      { name: "research", description: "Research a task", filePath: "/skills/research/SKILL.md" },
    ];
    const returned = await h.emit("before_agent_start", {
      prompt: '<skill name="wrapper" location="/skills/wrapper/SKILL.md">\nReferences are relative to /skills/wrapper.\n\nLoad `/research` and follow its process.\n</skill>',
      systemPrompt: "Base", systemPromptOptions: { skills },
    });
    expect(returned.filter(value => value && typeof value === "object" && "message" in value)).toEqual([]);
    expect(h.fake.sendMessage).toHaveBeenCalledOnce();
    expect(h.fake.sendMessage.mock.calls[0]![1]).toMatchObject({ deliverAs: "nextTurn", triggerTurn: false,
      provenance: { channel: "fabric", sender: { id: host.id, verified: "mesh" } } });
  });

  it("host-generated proxy turn-start notice uses the passive provenance adapter", async () => {
    const h = await indexFixture(); h.fake.getActiveTools.mockReturnValue(["fabric_exec"]);
    vi.spyOn(FabricState.prototype, "cwd", "get").mockReturnValue(process.cwd());
    vi.spyOn(CapturedToolCatalog.prototype, "list").mockReturnValue([{ name: "probe_tool", description: "Probe" } as any]);
    const returned = await h.emit("before_agent_start", { prompt: '<skill name="probe">Use probe_tool</skill>',
      systemPrompt: "Base", systemPromptOptions: { skills: [] } });
    expect(returned.filter(value => value && typeof value === "object" && "message" in value)).toEqual([]);
    expect(h.fake.sendMessage).toHaveBeenCalledOnce();
    expect(h.fake.sendMessage.mock.calls[0]![0].customType).toBe("pi-fabric-proxy");
    expect(h.fake.sendMessage.mock.calls[0]![1]).toMatchObject({ deliverAs: "nextTurn", triggerTurn: false,
      provenance: { channel: "fabric", sender: { id: host.id, verified: "mesh" } } });
  });

  it("records turn-start insertion uses the unclaimed passive adapter", async () => {
    const h = await indexFixture();
    h.records.mockResolvedValue(recordsInboxMessage([{ id: "github", from: "github:user", sequence: 1, createdAt: 1, data: {}, kind: "ask" } as RecordEnvelope]));
    const returned = await h.emit("before_agent_start", { prompt: "next", systemPrompt: "", systemPromptOptions: {} });
    expect(returned.filter(value => value && typeof value === "object" && "message" in value)).toEqual([]);
    expect(h.fake.sendMessage).toHaveBeenCalledOnce();
    expect(h.fake.sendMessage.mock.calls[0]![1]).toEqual({ deliverAs: "nextTurn", triggerTurn: false });
  });

  it("completion turn-start insertion uses the provenance adapter without a wake", async () => {
    const h = recording(); const inbox = new AgentCompletionInbox(h.pi, h.context); cleanups.push(() => inbox.close());
    inbox.enqueue({ id: "worker", name: "Worker", status: "completed", text: "done", startedAt: 1, finishedAt: 2 });
    expect(await h.emit("before_agent_start")).toEqual([undefined]);
    expect(h.fake.sendMessage).toHaveBeenCalledOnce();
    expect(h.fake.sendMessage.mock.calls[0]![1]).toMatchObject({ deliverAs: "nextTurn", triggerTurn: false,
      provenance: { channel: "fabric", sender: { id: host.id, verified: "mesh" } } });
  });

  it("shell turn-start insertion uses the provenance adapter without a wake", async () => {
    const h = recording(); const jobs = new FabricShellJobStore(); cleanups.push(() => jobs.close());
    const inbox = new ShellEventInbox(h.pi, h.context, jobs); cleanups.push(() => inbox.close());
    const job = jobs.begin("bash", "build"); job.spill(); await job.finish(0);
    expect(await h.emit("before_agent_start")).toEqual([undefined]);
    expect(h.fake.sendMessage).toHaveBeenCalledOnce();
    expect(h.fake.sendMessage.mock.calls[0]![1]).toMatchObject({ deliverAs: "nextTurn", triggerTurn: false,
      provenance: { channel: "fabric", sender: { id: host.id, verified: "mesh" } } });
  });

  it("prewalk typed task is unclaimed on capable Pi; only its generated notice claims Fabric", async () => {
    const h = recording();
    const state = { ensure: vi.fn(async () => {}), config: { ...DEFAULT_FABRIC_CONFIG, fullCodeMode: true,
      prewalk: { ...DEFAULT_FABRIC_CONFIG.prewalk, mode: "in-place", model: "provider/executor", detectShellWrites: false } },
      prewalk: { arm: vi.fn() } } as unknown as FabricState;
    registerFabricCommand(h.pi, { state, fabricUi: {} as any, capturedTools: {} as any, applyFabricMode: vi.fn(), suspendToolCapture: vi.fn() });
    const command = h.fake.registerCommand.mock.calls.find(call => call[0] === "fabric")![1];
    await command.handler("prewalk Implement the guard", h.context);
    expect(h.fake.sendUserMessage.mock.calls).toEqual([["Implement the guard"]]);
    expect(h.fake.sendMessage.mock.calls[0]![1]).toMatchObject({ deliverAs: "nextTurn",
      provenance: { channel: "fabric", sender: { id: host.id, verified: "mesh" } } });
  });

  it("Main and generic adapters make no claim without a recorded method, even with a bridge identity marker", () => {
    const h = mainFixture(); const from = { ...remote, verified: "bridge" as const };
    h.main.deliverAgent({ from, message: "task", delivery: "steer" });
    h.main.deliverUser("task", "steer", from);
    sendFabricMessage(h.pi, { customType: "probe", content: "task", display: false }, { triggerTurn: false }, from);
    sendFabricUserMessage(h.pi, "task", from, "steer");
    for (const call of [...h.fake.sendMessage.mock.calls, ...h.fake.sendUserMessage.mock.calls]) expect(call[1]?.provenance).toBeUndefined();
  });
});
