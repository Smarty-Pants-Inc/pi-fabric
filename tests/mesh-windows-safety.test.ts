import { createHash } from "node:crypto";
import fs, { type FSWatcher } from "node:fs";
import os from "node:os";
import path from "node:path";
import { EventEmitter } from "node:events";
import { afterEach, describe, expect, it, vi } from "vitest";
import { meshObserverWatch, meshObserverWatchCurrent } from "../src/actors/mesh-monitor.js";
import { CompletionJournal, consumeCompletion, saveCompletion } from "../src/agents/completion-journal.js";
import type { AgentRunResult } from "../src/agents/types.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { LifecycleBroker } from "../src/lifecycle/broker.js";
import { FABRIC_PARTICIPANT_LIFECYCLE_TOPIC } from "../src/lifecycle/types.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import { MeshStore } from "../src/mesh/store.js";
import { ResidencyClient } from "../src/residency/client.js";
import { residentDeliveryPrefix, residentHostId, residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import type { FabricParticipantSource } from "../src/topology/types.js";

const roots: string[] = [], closers: Array<() => void | Promise<void>> = [];
afterEach(async () => {
  for (const close of closers.splice(0)) await close();
  vi.restoreAllMocks(); vi.useRealTimers();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const temp = () => { const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-windows-safety-")); roots.push(root); return root; };
const identity = { id: "owner", name: "owner", kind: "main" as const };
const source = { id: "source", name: "source", kind: "root" as const, rootId: "source", runner: "pi" as const,
  ownerHostId: "source", ownerIdentityId: "source" };
const participants: FabricParticipantSource = {
  get: id => ({ format: 1, ...source, id, ownerHostId: id, ownerIdentityId: id, rootId: id,
    status: "idle", transport: "host", capabilities: ["followUp"], startedAt: 1, updatedAt: 1,
    controlProtocol: "v1", local: true, stale: false }),
  publishes: () => true, list: () => [], peers: () => [], self: () => participants.get("owner")!,
  refresh: async () => {}, scheduleRefresh: () => {},
};
function fakeWatch() {
  const watches: Array<{ dir: string; recursive: boolean; notify: (event: string, filename: string | null) => void;
    watcher: EventEmitter & { close: ReturnType<typeof vi.fn>; unref: ReturnType<typeof vi.fn> } }> = [];
  vi.spyOn(fs, "watch").mockImplementation(((...args: unknown[]) => {
    const watcher = Object.assign(new EventEmitter(), { close: vi.fn(), unref: vi.fn() });
    watches.push({ dir: String(args[0]), recursive: Boolean((args[1] as { recursive?: boolean })?.recursive),
      notify: args.at(-1) as typeof watches[number]["notify"], watcher });
    return watcher as unknown as FSWatcher;
  }) as typeof fs.watch);
  return watches;
}
const immediate = setImmediate;
const settled = async () => { for (let i = 0; i < 40; i++) await new Promise<void>(resolve => immediate(resolve)); };
function plane(mesh: MeshStore, id = "owner", platform: NodeJS.Platform = "win32", lease?: () => boolean) {
  const control = new FabricControlPlane(mesh, { ...identity, id }, { platform, enabled: true, hostId: id,
    pollMs: 20, acknowledgementTimeoutMs: 5_000, ...(lease ? { canConsumeMesh: lease } : {}) });
  closers.push(() => control.close()); return control;
}
const command = (mesh: MeshStore, operation = "followUp") => mesh.publish({ topic: "fabric.control.command",
  from: { ...identity, id: "sender" }, to: "owner", data: { version: 1, commandId: "one", targetId: "actor",
    operation, replyTo: "sender", requestedAt: Date.now(), deadlineAt: Date.now() + 120_000 } });
function resident(platform: NodeJS.Platform = "win32") {
  const root = temp(), meshRoot = path.join(root, "mesh"), rootId = "session:windows";
  const config: ResidentHostConfig = { format: 1, rootId, sessionId: "windows", mainName: "main", mainStartedAt: 1,
    cwd: root, projectRoot: root, meshRoot, actorRoot: path.join(root, "actors"), residencyRoot: residentRoot(meshRoot, rootId),
    fullCodeMode: true, agents: { ...DEFAULT_FABRIC_CONFIG.agents }, mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 },
    retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: "unused", fabricExtensionPath: "unused", piBinary: "unused",
    claudeBinary: "unused", vedaBinary: "unused" };
  const mesh = new MeshStore(meshRoot, 65_536, 100), complete = vi.fn(), deliverAgent = vi.fn();
  const client = new ResidencyClient({ platform, config, mesh,
    participants: { list: () => [], get: () => undefined, lastKnown: () => undefined } as unknown as FabricParticipantSource,
    mainAgent: { id: rootId, local: true, deliverAgent } as unknown as FabricMainAgentTarget, onBackgroundComplete: complete });
  closers.push(() => client.close());
  const recipient = { rootId, sessionId: config.sessionId, projectRoot: root, cwd: root, name: "main", startedAt: 1 };
  const result: AgentRunResult = { id: "a".repeat(32), name: "task", task: "test", status: "completed", runner: "pi",
    transport: "process", cwd: root, text: "done", startedAt: 1, updatedAt: 2, turns: 1, toolCalls: 0,
    usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } };
  return { root, config, mesh, client, complete, deliverAgent, recipient, result };
}
const message = (h: ReturnType<typeof resident>, id: string) => h.mesh.put({ key: residentDeliveryPrefix(h.config.rootId) + id,
  identity: { id: residentHostId(h.config.rootId), name: "host", kind: "main" }, value: { format: 1, id,
    rootId: h.config.rootId, from: { id: "actor", name: "actor", kind: "actor" }, message: id,
    delivery: "followUp", triggerTurn: false, createdAt: Date.now() } });

describe("Windows observation recovery", () => {
  it("observes recursively from the stable parent, filters siblings and notices root replacement", () => {
    const watches = fakeWatch(), mesh = new MeshStore(path.join(temp(), "mesh"), 65_536, 100), notify = vi.fn();
    const watcher = meshObserverWatch(mesh.root, { persistent: false }, notify, "win32")!;
    closers.push(() => watcher.close());
    expect(watches[0]).toMatchObject({ dir: path.dirname(mesh.root), recursive: true });
    watches[0]!.notify("change", path.join("sibling", "events.jsonl")); expect(notify).not.toHaveBeenCalled();
    watches[0]!.notify("change", path.join("mesh", "events.jsonl"));
    expect(notify).toHaveBeenLastCalledWith("change", "events.jsonl");
    watches[0]!.notify("rename", "mesh"); expect(notify).toHaveBeenLastCalledWith("rename", null);
    expect(meshObserverWatchCurrent(watcher, mesh.root)).toBe(true);
    fs.renameSync(mesh.root, mesh.root + ".retired"); fs.mkdirSync(mesh.root);
    expect(meshObserverWatchCurrent(watcher, mesh.root)).toBe(false);
  });

  it("recovers a missed control command at 5 s and pause/close never rearms delivery", async () => {
    vi.useFakeTimers(); fakeWatch(); const mesh = new MeshStore(path.join(temp(), "mesh"), 65_536, 100);
    const control = plane(mesh), handled = vi.fn(() => ({ accepted: true })); control.start(handled);
    await vi.advanceTimersByTimeAsync(0); await command(mesh);
    await vi.advanceTimersByTimeAsync(4_999); expect(handled).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1); expect(handled).toHaveBeenCalledOnce();
    control.pause(); const tail = vi.spyOn(mesh, "tail");
    await vi.advanceTimersByTimeAsync(15_000); expect(tail).not.toHaveBeenCalled();
    control.resume(); await vi.advanceTimersByTimeAsync(0); expect(handled).toHaveBeenCalledOnce();
    await control.close(); tail.mockClear(); await vi.advanceTimersByTimeAsync(15_000);
    expect(tail).not.toHaveBeenCalled(); expect(vi.getTimerCount()).toBe(0);
  });

  it("does not make an empty lease-denied Linux control plane poll periodically", async () => {
    vi.useFakeTimers(); fakeWatch(); const mesh = new MeshStore(path.join(temp(), "mesh"), 65_536, 100);
    const lease = vi.fn(() => false), control = plane(mesh, "owner", "linux", lease); control.start(() => ({ accepted: false }));
    await vi.advanceTimersByTimeAsync(0); expect(lease).toHaveBeenCalledOnce();
    const tail = vi.spyOn(mesh, "tail"); await vi.advanceTimersByTimeAsync(180_000);
    expect(lease).toHaveBeenCalledOnce(); expect(tail).not.toHaveBeenCalled();
    await control.close(); expect(vi.getTimerCount()).toBe(0);
  });

  it("recovers a missed ACK while the request is pending", async () => {
    vi.useFakeTimers(); fakeWatch(); const mesh = new MeshStore(path.join(temp(), "mesh"), 65_536, 100);
    const sender = plane(mesh, "sender"); sender.start(() => ({ accepted: false })); await vi.advanceTimersByTimeAsync(0);
    const outcome = sender.request("owner", "actor", "followUp", { message: "work" });
    await settled(); const sent = mesh.read({ topic: "fabric.control.command" })[0]!.data as { commandId: string; targetId: string };
    await mesh.publish({ topic: "fabric.control.ack", from: identity, to: "sender", data: { version: 1,
      commandId: sent.commandId, targetId: sent.targetId, accepted: true, messageId: "once" } });
    await vi.advanceTimersByTimeAsync(5_000);
    await expect(outcome).resolves.toMatchObject({ acknowledged: true, messageId: "once" });
    await sender.close(); expect(vi.getTimerCount()).toBe(0);
  });

  it.each(["linux", "win32"] as const)("recovers lease-paused completed outcomes on %s without replay or idle reads", async platform => {
    vi.useFakeTimers(); fakeWatch(); const mesh = new MeshStore(path.join(temp(), "mesh"), 65_536, 100);
    let healthy = true, release!: () => void;
    const held = new Promise<void>(resolve => { release = resolve; });
    const handled = vi.fn(async () => { await held; return { accepted: true, messageId: "once" }; });
    await command(mesh, "ask"); const control = plane(mesh, "owner", platform, () => healthy); control.start(handled);
    await vi.advanceTimersByTimeAsync(0); expect(handled).toHaveBeenCalledOnce();
    healthy = false; release(); await vi.advanceTimersByTimeAsync(120);
    expect(mesh.read({ topic: "fabric.control.ack" })).toHaveLength(0);
    const seen = new MeshStore(path.join(mesh.root, "control-seen", createHash("sha256").update("owner").digest("hex").slice(0, 32)), 65_536, 100);
    expect(seen.listAll("topology/control-seen/").every(entry => !(entry.value as { sequence?: number }).sequence)).toBe(true);
    healthy = true; await vi.advanceTimersByTimeAsync(100);
    expect(mesh.read({ topic: "fabric.control.ack" })).toHaveLength(1); expect(handled).toHaveBeenCalledOnce();
    const tail = vi.spyOn(mesh, "tail"); await vi.advanceTimersByTimeAsync(platform === "linux" ? 180_000 : 4_000);
    expect(tail).not.toHaveBeenCalled(); await control.close(); expect(vi.getTimerCount()).toBe(0);
  });

  it("recovers missed lifecycle appends and uses only a stable-parent watch", async () => {
    vi.useFakeTimers(); const watches = fakeWatch(), mesh = new MeshStore(path.join(temp(), "mesh"), 65_536, 100);
    for (const dir of ["participants", "host-leases"]) fs.mkdirSync(path.join(mesh.root, dir));
    const delivered = vi.fn(), broker = new LifecycleBroker(mesh, identity, participants,
      { platform: "win32", enabled: true, pollMs: 20, maxReadEvents: 100 }, delivered);
    closers.push(() => broker.close());
    await broker.subscribe({ from: "source", to: "owner", events: ["pi.agent_settled"], delivery: "followUp", triggerTurn: false });
    broker.start(); await settled(); expect(watches).toHaveLength(1);
    expect(watches[0]).toMatchObject({ dir: path.dirname(mesh.root), recursive: true });
    await mesh.publish({ topic: FABRIC_PARTICIPANT_LIFECYCLE_TOPIC, kind: "pi.agent_settled", from: { ...identity, id: "source" },
      data: { version: 1, event: "pi.agent_settled", source, occurredAt: Date.now() } });
    await vi.advanceTimersByTimeAsync(4_999); expect(delivered).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1); await settled(); expect(delivered).toHaveBeenCalledOnce();
    broker.pause(); const list = vi.spyOn(mesh, "listAll"); await vi.advanceTimersByTimeAsync(10_000);
    expect(list).not.toHaveBeenCalled(); await broker.close(); expect(vi.getTimerCount()).toBe(0);
  });

  it("recovers a missed completion-directory replacement and a new delivery within 5 s", async () => {
    vi.useFakeTimers(); const watches = fakeWatch(), h = resident();
    const journal = path.join(h.config.meshRoot, "agent-completions"); fs.mkdirSync(journal);
    const drains = vi.spyOn(CompletionJournal.prototype, "drainChanged");
    h.client.start(); await vi.advanceTimersByTimeAsync(20); await drains.mock.results.at(-1)!.value;
    expect(watches).toHaveLength(1); expect(watches[0]).toMatchObject({ dir: h.root, recursive: true });
    fs.renameSync(journal, journal + ".retired"); saveCompletion(h.config.meshRoot, h.recipient, h.result);
    await message(h, "missed");
    await vi.advanceTimersByTimeAsync(4_979); expect(h.complete).not.toHaveBeenCalled(); expect(h.deliverAgent).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1); await settled(); await drains.mock.results.at(-1)!.value;
    expect(h.complete).toHaveBeenCalledOnce(); expect(h.deliverAgent).toHaveBeenCalledOnce();
    await h.client.close(); await settled(); expect(watches.every(w => w.watcher.close.mock.calls.length > 0)).toBe(true);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("recovers a missed consumed-completion receipt without replaying the admitted notice", async () => {
    vi.useFakeTimers(); fakeWatch(); const h = resident(), root = h.config.residencyRoot;
    const run = path.join(root, "runs", h.result.id), agents = path.join(root, "agents");
    fs.mkdirSync(run, { recursive: true }); fs.mkdirSync(agents, { recursive: true });
    fs.writeFileSync(path.join(root, "config.json"), JSON.stringify(h.config));
    fs.writeFileSync(path.join(agents, `${h.result.id}.json`), JSON.stringify({ format: 1, rootId: h.config.rootId,
      id: h.result.id, runDirectory: run, handle: h.result, createdAt: 1, updatedAt: 2 }));
    fs.writeFileSync(path.join(run, "status.json"), JSON.stringify(h.result));
    fs.writeFileSync(path.join(run, "completion-recipient.json"), JSON.stringify({ meshRoot: h.config.meshRoot, recipient: h.recipient }));
    const key = residentDeliveryPrefix(h.config.rootId) + h.result.id;
    await h.mesh.put({ key, identity: { id: residentHostId(h.config.rootId), name: "host", kind: "main" }, value: {
      format: 1, id: h.result.id, rootId: h.config.rootId, from: { id: h.result.id, name: "task", kind: "agent" },
      agentCompletionId: h.result.id, message: "done", delivery: "followUp", triggerTurn: true, createdAt: 2 } });
    const drains = vi.spyOn(CompletionJournal.prototype, "drainChanged");
    h.client.start(); await vi.advanceTimersByTimeAsync(20); await drains.mock.results.at(-1)!.value; await settled();
    expect(h.complete).toHaveBeenCalledOnce(); expect(h.mesh.get(key, { fresh: true })).toBeDefined();
    consumeCompletion(h.config.meshRoot, h.result.id, h.config.sessionId); // Intentionally no native callback.
    await vi.advanceTimersByTimeAsync(5_000); await settled(); await drains.mock.results.at(-1)!.value;
    expect(h.mesh.get(key, { fresh: true })).toBeUndefined(); expect(h.complete).toHaveBeenCalledOnce();
    await h.client.close(); await settled(); expect(vi.getTimerCount()).toBe(0);
  });

  it("does not turn a permanent refusal into periodic Windows retry work", async () => {
    vi.useFakeTimers(); fakeWatch(); const h = resident();
    h.deliverAgent.mockImplementation(() => { throw new Error("validation refused"); }); await message(h, "refused");
    h.client.start(); await vi.advanceTimersByTimeAsync(20); await settled(); expect(h.deliverAgent).toHaveBeenCalledOnce();
    await vi.advanceTimersByTimeAsync(60_000); await settled(); expect(h.deliverAgent).toHaveBeenCalledOnce();
    expect(h.mesh.listAllShared(residentDeliveryPrefix(h.config.rootId))).toHaveLength(1);
    await h.client.close(); expect(vi.getTimerCount()).toBe(0);
  });

  it("keeps the Linux client at zero idle timers and never scans five minutes of unchanged work", async () => {
    vi.useFakeTimers(); fakeWatch(); const h = resident("linux"), drains = vi.spyOn(CompletionJournal.prototype, "drainChanged");
    h.client.start(); await vi.advanceTimersByTimeAsync(20); await drains.mock.results.at(-1)!.value; await settled();
    expect(vi.getTimerCount()).toBe(0); const lists = vi.spyOn(h.mesh, "listAllShared"), reads = vi.spyOn(fs, "readFileSync");
    await vi.advanceTimersByTimeAsync(5 * 60_000); await settled();
    expect(lists).not.toHaveBeenCalled(); expect(reads).not.toHaveBeenCalled(); expect(drains).toHaveBeenCalledOnce();
    await h.client.close(); expect(vi.getTimerCount()).toBe(0);
  });

  it("allows native mesh and journal retirement while Windows-policy observers remain open", async () => {
    const h = resident(), journal = path.join(h.config.meshRoot, "agent-completions"); fs.mkdirSync(journal);
    // A nested actor root must not add a second watch handle on mesh.root.
    h.config.actorRoot = path.join(h.config.meshRoot, "actors");
    const control = plane(h.mesh), broker = new LifecycleBroker(h.mesh, identity, participants,
      { platform: "win32", enabled: true, pollMs: 20, maxReadEvents: 100 }, () => {});
    closers.push(() => broker.close()); control.start(() => ({ accepted: true })); broker.start(); h.client.start();
    await settled(); fs.renameSync(journal, journal + ".retired"); fs.mkdirSync(journal);
    fs.renameSync(h.mesh.root, h.mesh.root + ".retired"); fs.cpSync(h.mesh.root + ".retired", h.mesh.root, { recursive: true });
    saveCompletion(h.config.meshRoot, h.recipient, h.result);
    await vi.waitFor(() => expect(h.complete).toHaveBeenCalledOnce(), { timeout: 6_000 });
  });
});
