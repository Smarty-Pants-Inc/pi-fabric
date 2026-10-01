import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { AgentCompletionInbox } from "../src/agents/completion-inbox.js";
import { completionConsumed, completionSuccessor, pendingCompletions, saveCompletion, saveWorkerCompletion, type CompletionRecipient } from "../src/agents/completion-journal.js";
import type { AgentRunResult } from "../src/agents/types.js";
import { MeshStore } from "../src/mesh/store.js";
import { ResidencyClient } from "../src/residency/client.js";
import { residentDeliveryPrefix, residentHostId, residentResultPath, residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import type { FabricParticipantInfo, FabricParticipantSource } from "../src/topology/types.js";

const roots: string[] = [];
const clients: ResidencyClient[] = [];
const inboxes: AgentCompletionInbox[] = [];
afterEach(async () => {
  for (const client of clients.splice(0)) await client.close();
  for (const inbox of inboxes.splice(0)) inbox.close();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const waitFor = async (test: () => boolean) => {
  const end = Date.now() + 3000;
  while (!test()) { if (Date.now() > end) throw new Error("completion probe timed out"); await new Promise(resolve => setTimeout(resolve, 10)); }
};
const harness = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "completion-successor-")); roots.push(root);
  const meshRoot = path.join(root, "mesh");
  const mesh = new MeshStore(meshRoot, DEFAULT_FABRIC_CONFIG.mesh.maxEventBytes, 100);
  let live: FabricParticipantInfo[] = [];
  const participant = (session: string, startedAt: number, extra = {}): FabricParticipantInfo => ({
    format: 1, id: `session:${session}`, rootId: `session:${session}`, ownerHostId: `session:${session}`,
    ownerIdentityId: `session:${session}`, sessionId: session, name: "main", role: "lane-main", project: root,
    cwd: root, kind: "root", status: "idle", runner: "pi", transport: "host", capabilities: ["steer", "followUp", "fabric"],
    interactive: true, startedAt, updatedAt: startedAt, controlProtocol: "v1", local: false, stale: false, ...extra,
  });
  const participants = { list: () => live, lastKnown: () => undefined } as unknown as FabricParticipantSource;
  const config = (session: string, startedAt: number): ResidentHostConfig => ({
    format: 1, rootId: `session:${session}`, sessionId: session, cwd: root, projectRoot: root,
    mainName: "main", mainStartedAt: startedAt, role: "lane-main", project: root, meshRoot,
    residencyRoot: residentRoot(meshRoot, `session:${session}`), actorRoot: path.join(meshRoot, "actors"), fullCodeMode: true,
    agents: { ...DEFAULT_FABRIC_CONFIG.agents }, mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, retention: DEFAULT_FABRIC_CONFIG.retention,
    workerPath: "unused", fabricExtensionPath: "unused", piBinary: "unused", claudeBinary: "unused", vedaBinary: "unused",
  });
  const result: AgentRunResult = { id: "a".repeat(32), name: "finished child", task: "work", status: "completed",
    runner: "pi", transport: "process", cwd: root, startedAt: 1, updatedAt: 2, finishedAt: 2, turns: 1, toolCalls: 0,
    text: "authoritative task result", usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } };
  const recipient: CompletionRecipient = { rootId: "session:A", sessionId: "A", cwd: root, projectRoot: root,
    name: "main", role: "lane-main", startedAt: 100 };
  const client = (session: string, startedAt: number, extra = {}) => {
    const handlers = new Map<string, (event: unknown, ctx: ExtensionContext) => unknown>();
    const sendMessage = vi.fn();
    const context = { hasUI: false, isIdle: () => false, hasPendingMessages: () => false,
      sessionManager: { getSessionId: () => session } } as unknown as ExtensionContext;
    const inbox = new AgentCompletionInbox({ on: (name: string, handler: any) => handlers.set(name, handler), sendMessage } as any, context); inboxes.push(inbox);
    const completed = vi.fn((value: AgentRunResult, delivered: () => void) => inbox.enqueue(value, delivered));
    const cfg = { ...config(session, startedAt), ...extra };
    const client = new ResidencyClient({ config: cfg, mesh, participants,
      mainAgent: { local: true, deliverAgent: vi.fn() } as any, onBackgroundComplete: completed }); clients.push(client);
    return { client, inbox, completed, sendMessage, turn: () => handlers.get("turn_end")?.({ message: { role: "assistant", stopReason: "stop" } }, context) };
  };
  const seedResident = async () => {
    const cfg = config("A", 100);
    fs.mkdirSync(path.join(cfg.residencyRoot, "results"), { recursive: true });
    fs.writeFileSync(path.join(cfg.residencyRoot, "config.json"), JSON.stringify(cfg));
    fs.writeFileSync(residentResultPath(cfg.residencyRoot, result.id), JSON.stringify(result));
    const sourceKey = `${residentDeliveryPrefix(cfg.rootId)}legacy-result`;
    await mesh.put({ key: sourceKey, identity: { id: residentHostId(cfg.rootId), name: "resident", kind: "main" }, ifVersion: 0,
      value: { format: 1, id: "legacy-result", rootId: cfg.rootId, from: { id: result.id, name: result.name, kind: "agent" },
        agentCompletionId: result.id, delivery: "followUp", triggerTurn: true, message: "legacy clipped summary", data: { fabricTruncated: true }, createdAt: 2 } });
    return sourceKey;
  };
  return { root, meshRoot, mesh, participant, recipient, result, client, seedResident, setLive: (value: FabricParticipantInfo[]) => { live = value; } };
};

describe("dead Main completion succession", () => {
  it("re-delivers an authenticated legacy resident result once; a second successor cannot replay it", async () => {
    const h = harness(); const key = await h.seedResident(); h.setLive([h.participant("B", 200)]);
    const b = h.client("B", 200); b.client.start();
    await waitFor(() => b.completed.mock.calls.length > 0);
    expect(b.client.statusAgent(h.result.id)).toMatchObject({ text: h.result.text, completionDelivery: { status: "undelivered", addressedTo: "A" } });
    b.turn(); b.turn();
    expect(b.sendMessage).toHaveBeenCalledOnce();
    expect(b.sendMessage.mock.calls[0]![0].content).toContain("re-delivered from dead Main session A");
    expect(b.sendMessage.mock.calls[0]![0].content).toContain(h.result.text);
    await waitFor(() => !h.mesh.get(key));
    expect(completionConsumed(h.meshRoot, h.result.id)).toBe(true);
    expect(b.client.hasAgent(h.result.id)).toBe(true);
    expect(b.client.statusAgent(h.result.id)).toMatchObject({ text: h.result.text });
    expect((b.client.statusAgent(h.result.id) as AgentRunResult).completionDelivery).toBeUndefined();
    await b.client.close(); b.inbox.close();
    h.setLive([h.participant("C", 300)]); const c = h.client("C", 300); c.client.start();
    await new Promise(resolve => setTimeout(resolve, 100)); c.turn();
    expect(c.completed).not.toHaveBeenCalled(); expect(c.sendMessage).not.toHaveBeenCalled();
  });

  it("keeps no-successor results pending and visible in list/status, even with notifications disabled", async () => {
    const h = harness(); const key = await h.seedResident(); h.setLive([h.participant("observer", 200, { role: "other-lane" })]);
    const b = h.client("observer", 200, { role: "other-lane", agents: { ...DEFAULT_FABRIC_CONFIG.agents, notifyOnComplete: false } }); b.client.start();
    await waitFor(() => b.client.listAgents().length === 1);
    expect(b.client.statusAgent(h.result.id)).toMatchObject({ completionDelivery: { status: "undelivered", addressedTo: "A" } });
    b.client.acknowledgeCompletion(h.result.id); b.turn();
    expect(b.sendMessage).not.toHaveBeenCalled(); expect(completionConsumed(h.meshRoot, h.result.id)).toBe(false);
    expect(h.mesh.get(key)).toBeDefined(); expect(pendingCompletions(h.meshRoot, h.root)).toHaveLength(1);
  });

  it("retains a session inbox outcome after the original Main dies, and reclaims an unconsumed successor", async () => {
    const h = harness(); h.setLive([h.participant("A", 100)]); const a = h.client("A", 100);
    a.client.enqueueCompletion(h.result); a.client.start(); await waitFor(() => a.completed.mock.calls.length > 0);
    await a.client.close(); a.inbox.close(); h.setLive([h.participant("B", 200)]);
    const b = h.client("B", 200); b.client.start(); await waitFor(() => b.completed.mock.calls.length > 0);
    await b.client.close(); b.inbox.close(); h.setLive([h.participant("C", 300)]);
    const c = h.client("C", 300); c.client.start(); await waitFor(() => c.completed.mock.calls.length > 0); c.turn();
    expect(c.sendMessage).toHaveBeenCalledOnce(); expect(completionConsumed(h.meshRoot, h.result.id)).toBe(true);
  });

  it("a newer concurrently live successor cannot duplicate an already claimed inbox result", async () => {
    const h = harness(); saveCompletion(h.meshRoot, h.recipient, h.result);
    h.setLive([h.participant("B", 200)]); const b = h.client("B", 200); b.client.start();
    await waitFor(() => b.completed.mock.calls.length > 0);
    h.setLive([h.participant("B", 200), h.participant("C", 300)]);
    const c = h.client("C", 300); c.client.start(); await new Promise(resolve => setTimeout(resolve, 100));
    c.turn(); expect(c.completed).not.toHaveBeenCalled(); b.turn();
    expect(b.sendMessage).toHaveBeenCalledOnce();
    await new Promise(resolve => setTimeout(resolve, 60)); expect(c.sendMessage).not.toHaveBeenCalled();
  });

  it("never steals from a live original, another cwd/name/role/project, older or non-interactive Main", () => {
    const h = harness(); const b = h.participant("B", 200);
    expect(completionSuccessor(h.recipient, [h.participant("A", 100), b])).toBeUndefined();
    for (const extra of [{ cwd: path.join(h.root, "other") }, { name: "other" }, { role: "other" },
      { startedAt: 99 }, { stale: true }, { interactive: false }, { remoteHost: "other-host" }, { capabilities: ["fabric"] }]) {
      expect(completionSuccessor(h.recipient, [h.participant("B", 200, extra)])).toBeUndefined();
    }
    expect(completionSuccessor({ ...h.recipient, projectRoot: path.dirname(h.root) }, [b])).toBeUndefined();
    expect(completionSuccessor(h.recipient, [b, h.participant("C", 300)])?.sessionId).toBe("C");
  });

  it("journals an orphan worker's terminal outcome via its host-owned launch return address", async () => {
    const h = harness(); const run = path.join(h.root, "run"); fs.mkdirSync(run);
    fs.writeFileSync(path.join(run, "completion-recipient.json"), JSON.stringify({ meshRoot: h.meshRoot, recipient: h.recipient }));
    saveWorkerCompletion(path.join(run, "status.json"), h.result);
    h.setLive([h.participant("B", 200)]); const b = h.client("B", 200); b.client.start();
    await waitFor(() => b.completed.mock.calls.length > 0); b.turn();
    expect(b.sendMessage).toHaveBeenCalledOnce();
    expect(await b.client.waitAgent(h.result.id)).toMatchObject({ text: h.result.text });
  });

  it("honors a legacy metadata receipt even when its host journaled the outcome", async () => {
    const h = harness(); saveCompletion(h.meshRoot, h.recipient, h.result);
    const agents = path.join(residentRoot(h.meshRoot, h.recipient.rootId), "agents"); fs.mkdirSync(agents, { recursive: true });
    fs.writeFileSync(path.join(agents, `${h.result.id}.json`), JSON.stringify({ rootId: h.recipient.rootId, id: h.result.id, completionConsumedAt: 3 }));
    h.setLive([h.participant("B", 200)]); const b = h.client("B", 200); b.client.start();
    await new Promise(resolve => setTimeout(resolve, 60)); expect(b.completed).not.toHaveBeenCalled();
    expect((b.client.statusAgent(h.result.id) as AgentRunResult).completionDelivery).toBeUndefined();
  });

  it("persists late-wait receipt before notification publication, suppressing successor delivery", async () => {
    const h = harness(); const a = h.client("A", 100); a.client.acknowledgeCompletion(h.result.id);
    saveCompletion(h.meshRoot, h.recipient, h.result); h.setLive([h.participant("B", 200)]);
    const b = h.client("B", 200); b.client.start(); await new Promise(resolve => setTimeout(resolve, 60));
    expect(b.completed).not.toHaveBeenCalled(); expect(pendingCompletions(h.meshRoot, h.root)).toHaveLength(0);
  });
});
