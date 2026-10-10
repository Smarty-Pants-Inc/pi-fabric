import fs from "node:fs";
import { createHash } from "node:crypto";
import os from "node:os";
import path from "node:path";
import { EventEmitter } from "node:events";
import { afterEach, describe, expect, it, vi } from "vitest";
import { CompletionJournal, consumeCompletion, saveCompletion, type CompletionRecipient } from "../src/agents/completion-journal.js";
import type { AgentRunResult } from "../src/agents/types.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import { MeshStore } from "../src/mesh/store.js";
import { RESIDENCY_NOTIFICATION_DIR, residencyNotificationName } from "../src/mesh/residency-notifications.js";
import { ResidencyClient } from "../src/residency/client.js";
import * as kernelFence from "../src/residency/file-lock.js";
import { MeshLockTimeoutError } from "../src/core/atomic-write.js";
import { residentHostId, residentDeliveryPrefix, residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import type { FabricParticipantSource } from "../src/topology/types.js";

const roots: string[] = [], clients: ResidencyClient[] = [];
afterEach(async () => { for (const client of clients.splice(0)) await client.close(); vi.restoreAllMocks(); vi.useRealTimers(); for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
// The mocked event-only fixtures assert the Linux idle contract, on every CI host.
const fixture = (stateBackend: "file" | "sqlite" = "file", platform: NodeJS.Platform = "linux") => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "residency-events-")); roots.push(root);
  const meshRoot = path.join(root, "mesh"), rootId = "session:event-idle";
  const config: ResidentHostConfig = { format: 1, rootId, sessionId: "event-idle", mainName: "main", mainStartedAt: 1, cwd: root, projectRoot: root, meshRoot,
    actorRoot: path.join(root, "actors"), residencyRoot: residentRoot(meshRoot, rootId), fullCodeMode: true,
    agents: { ...DEFAULT_FABRIC_CONFIG.agents }, mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, retention: DEFAULT_FABRIC_CONFIG.retention,
    workerPath: "unused", fabricExtensionPath: "unused", piBinary: "unused", claudeBinary: "unused", vedaBinary: "unused" };
  const mesh = new MeshStore(meshRoot, 64 * 1024, 100, { readCacheMs: 2_000, stateBackend });
  const participants = { list: () => [], get: () => undefined, lastKnown: () => undefined, self: () => { throw new Error("no owner"); } } as unknown as FabricParticipantSource;
  const deliverAgent = vi.fn(), complete = vi.fn();
  const client = new ResidencyClient({ platform, config, mesh, participants, mainAgent: { id: rootId, local: true, deliverAgent } as unknown as FabricMainAgentTarget, onBackgroundComplete: complete }); clients.push(client);
  const recipient: CompletionRecipient = { rootId, sessionId: config.sessionId, projectRoot: root, cwd: root, name: "main", startedAt: 1 };
  const result = (index: number): AgentRunResult => ({ id: index.toString(16).padStart(32, "0"), name: "task", task: "test", status: "completed", runner: "pi", transport: "process", cwd: root, text: "done", startedAt: 1, updatedAt: 2, turns: 1, toolCalls: 0, usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } });
  return { root, meshRoot, config, mesh, client, complete, deliverAgent, recipient, result };
};
const fakeWatches = () => {
  const watches: Array<{ dir: string; callback: (event: string, filename: string | null) => void; watcher: EventEmitter & { close: ReturnType<typeof vi.fn>; unref: ReturnType<typeof vi.fn> } }> = [];
  vi.spyOn(fs, "watch").mockImplementation(((dir: fs.PathLike, callback: (event: string, filename: string | null) => void) => {
    const watcher = Object.assign(new EventEmitter(), { close: vi.fn(), unref: vi.fn() });
    watches.push({ dir: String(dir), callback, watcher }); return watcher;
  }) as unknown as typeof fs.watch);
  const fire = (dir: string, filename: string | null, event = "change") => {
    const active = watches.filter(watch => watch.dir === dir && !watch.watcher.close.mock.calls.length).at(-1);
    expect(active).toBeDefined(); active!.callback(event, filename);
  };
  return { watches, fire };
};
const seedLegacy = async (h: ReturnType<typeof fixture>, index = 1, rootId = h.config.rootId, writer = residentHostId(rootId)) => {
  const result = h.result(index), root = residentRoot(h.meshRoot, rootId), runDirectory = path.join(root, "runs", result.id);
  const recipient = { ...h.recipient, rootId, sessionId: rootId === h.config.rootId ? h.config.sessionId : "other" };
  fs.mkdirSync(runDirectory, { recursive: true }); fs.mkdirSync(path.join(root, "agents"), { recursive: true });
  fs.writeFileSync(path.join(root, "config.json"), JSON.stringify({ ...h.config, rootId, sessionId: recipient.sessionId, residencyRoot: root }));
  const metadataPath = path.join(root, "agents", `${result.id}.json`);
  fs.writeFileSync(metadataPath, JSON.stringify({ format: 1, rootId, id: result.id, runDirectory, handle: result, createdAt: 1, updatedAt: 2 }));
  fs.writeFileSync(path.join(runDirectory, "status.json"), JSON.stringify(result));
  fs.writeFileSync(path.join(runDirectory, "completion-recipient.json"), JSON.stringify({ meshRoot: h.meshRoot, recipient }));
  const key = residentDeliveryPrefix(rootId) + result.id;
  await h.mesh.put({ key, identity: { id: writer, name: "resident", kind: "main" }, value: {
    format: 1, id: result.id, rootId, from: { id: result.id, name: result.name, kind: "agent" }, agentCompletionId: result.id,
    message: "legacy summary", delivery: "followUp", triggerTurn: true, createdAt: 2,
  } });
  return { key, result, runDirectory, metadataPath };
};
const immediate = setImmediate;
const settled = async () => { for (let i = 0; i < 30; i++) await new Promise<void>(resolve => immediate(resolve)); };

describe("ResidencyClient event-driven idle", () => {
  it("arms no recurring fast idle timer and never discovers a missed completion during five idle minutes", async () => {
    const h = fixture(), events = fakeWatches();
    vi.useFakeTimers();
    const interval = vi.spyOn(globalThis, "setInterval"), drain = vi.spyOn(CompletionJournal.prototype, "drainChanged");
    h.client.start(); await vi.advanceTimersByTimeAsync(20); await drain.mock.results[0]!.value; await settled();
    // No interval at all: a healthy idle client holds zero timers (no 60 s safety tick).
    expect(interval).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
    saveCompletion(h.meshRoot, h.recipient, h.result(1));
    await vi.advanceTimersByTimeAsync(5 * 60_000); await settled();
    expect(drain).toHaveBeenCalledOnce();
    expect(h.complete).not.toHaveBeenCalled(); expect(h.deliverAgent).not.toHaveBeenCalled();
    events.fire(h.meshRoot, "agent-completions", "rename"); await settled();
    await drain.mock.results.at(-1)!.value; await settled();
    expect(h.complete).toHaveBeenCalledOnce();
    await vi.advanceTimersByTimeAsync(5 * 60_000); await settled();
    expect(h.complete).toHaveBeenCalledOnce();
    await h.client.close(); expect(vi.getTimerCount()).toBe(0);
  });

  it("retains a failed admitted completion without timer retries, then retries its file event once", async () => {
    const h = fixture(), events = fakeWatches();
    saveCompletion(h.meshRoot, h.recipient, h.result(1));
    h.complete.mockImplementationOnce(() => { throw new Error("inbox full"); });
    vi.useFakeTimers();
    const drain = vi.spyOn(CompletionJournal.prototype, "drainChanged");
    h.client.start(); await vi.advanceTimersByTimeAsync(20); await drain.mock.results[0]!.value; await settled();
    // The journal retains its refused admission for its own file event; the pass did not
    // fault, so no fault timer is armed and no idle timer exists.
    expect(h.complete).toHaveBeenCalledOnce(); expect(vi.getTimerCount()).toBe(0);
    await vi.advanceTimersByTimeAsync(5 * 60_000); await settled();
    expect(h.complete).toHaveBeenCalledOnce();
    const filename = `${createHash("sha256").update(h.result(1).id).digest("hex")}.json`;
    events.fire(path.join(h.meshRoot, "agent-completions"), filename); await settled();
    await drain.mock.results.at(-1)!.value; await settled();
    expect(h.complete).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(5 * 60_000); await settled();
    expect(h.complete).toHaveBeenCalledTimes(2);
    await h.client.close(); expect(vi.getTimerCount()).toBe(0);
  });

  it("subscribes before discovery, stays quiet through heartbeats, and closes all observation resources", async () => {
    const h = fixture(), events = fakeWatches();
    for (let i = 1; i <= 40; i++) saveCompletion(h.meshRoot, { ...h.recipient, rootId: "session:other", sessionId: "other" }, h.result(i));
    const initial = CompletionJournal.prototype.drainChanged;
    const drain = vi.spyOn(CompletionJournal.prototype, "drainChanged");
    drain.mockImplementation(function (this: CompletionJournal, ...args) { expect(events.watches.length).toBeGreaterThan(0); return initial!.apply(this, args); });
    h.client.start(); await vi.waitFor(() => expect(drain).toHaveBeenCalledOnce()); await settled();
    const open = vi.spyOn(fs.promises, "open"), read = vi.spyOn(fs.promises, "readFile"), readdir = vi.spyOn(fs.promises, "readdir");
    for (let i = 0; i < 3; i++) {
      await h.mesh.put({ key: "topology/heartbeats/other", identity: { id: "other", name: "other", kind: "main" }, value: { at: i } });
      events.fire(h.meshRoot, "state.json", "rename"); await settled();
    }
    expect(drain).toHaveBeenCalledOnce(); expect(open).not.toHaveBeenCalled(); expect(read).not.toHaveBeenCalled(); expect(readdir).not.toHaveBeenCalled();
    await h.client.close(); expect(events.watches.every(watch => watch.watcher.close.mock.calls.length > 0)).toBe(true);
  });

  it("never retries pending deliveries or turns Main on lease renewals, then retries one real receipt", async () => {
    const h = fixture(), events = fakeWatches(), own = await seedLegacy(h), unread = await seedLegacy(h, 2);
    const receipts = path.join(h.meshRoot, "agent-completions", "receipts"); fs.mkdirSync(receipts, { recursive: true });
    h.client.start(); await vi.waitFor(() => expect(h.complete).toHaveBeenCalledTimes(2)); await settled();
    const original = h.mesh.get(own.key, { fresh: true })!;
    const deleting = h.mesh.delete.bind(h.mesh); let refused = false;
    const remove = vi.spyOn(h.mesh, "delete").mockImplementation(input => {
      if (input.key === own.key && !refused) { refused = true; return Promise.resolve({ deleted: false }); }
      return deleting(input);
    });
    // Journal claim retirement is independently async (slower on Windows).
    // This assertion is about delivery retries, not legitimate claim cleanup.
    const deliveryDeletes = () => remove.mock.calls.filter(([input]) => input.key.startsWith("residency/deliveries/"));
    h.complete.mock.calls[0]![1](); await settled();
    expect(deliveryDeletes()).toHaveLength(1); expect(h.mesh.get(own.key, { fresh: true })).toEqual(original);
    remove.mockClear(); h.complete.mockClear(); h.deliverAgent.mockClear();
    const get = vi.spyOn(h.mesh, "get"), select = vi.spyOn(h.mesh, "listAllShared");
    for (let n = 1; n <= 12; n++) {
      await h.mesh.writeBatch({ identity: { id: "lease-owner", name: "lease-owner", kind: "main" }, ops: [
        { kind: "put", key: "topology/hosts/lease-owner", value: { updatedAt: n * 60_000, expiresAt: (n + 2) * 60_000 } },
        { kind: "put", key: "topology/participants/lease-owner", value: { status: "idle", updatedAt: n * 60_000 } },
      ] });
      for (const file of ["state.json", "state.read-journal.jsonl", "state.read-signal.json"]) events.fire(h.meshRoot, file);
      await settled();
    }
    expect(get.mock.calls.filter(([key]) => key.startsWith("residency/deliveries/"))).toEqual([]);
    expect(select).not.toHaveBeenCalled(); expect(deliveryDeletes()).toHaveLength(0);
    expect(h.complete).not.toHaveBeenCalled(); expect(h.deliverAgent).not.toHaveBeenCalled();
    get.mockClear();
    const filename = `${createHash("sha256").update(own.result.id).digest("hex")}.json`;
    events.fire(receipts, filename); events.fire(receipts, filename); await settled();
    expect(get.mock.calls.filter(([key]) => key.startsWith("residency/deliveries/")).map(([key]) => key)).toEqual([own.key]);
    expect(deliveryDeletes()).toHaveLength(1); expect(h.mesh.get(own.key, { fresh: true })).toBeUndefined();
    expect(h.mesh.get(unread.key, { fresh: true })).toBeDefined();
    expect(h.complete).not.toHaveBeenCalled(); expect(h.deliverAgent).not.toHaveBeenCalled();
  });

  it.each(["file", "sqlite"] as const)("admits only a changed delivery key, not lease writes or another delivery, on %s", async backend => {
    const h = fixture(backend), events = fakeWatches();
    const key = residentDeliveryPrefix(h.config.rootId) + "failed";
    const writer = { id: residentHostId(h.config.rootId), name: "host", kind: "main" as const };
    const value = { format: 1, id: "failed", rootId: h.config.rootId, from: { id: "actor", name: "actor", kind: "actor" },
      message: "failed", delivery: "followUp", triggerTurn: true, createdAt: 1 };
    await h.mesh.put({ key, identity: writer, value });
    h.deliverAgent.mockImplementationOnce(() => { throw new Error("Main busy"); });
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    // Fake clock: the failed key's fault retry must not race the event assertions.
    vi.useFakeTimers();
    try {
      const drain = vi.spyOn(CompletionJournal.prototype, "drainChanged");
      h.client.start(); await vi.advanceTimersByTimeAsync(20); await drain.mock.results[0]!.value; await settled();
      expect(h.deliverAgent).toHaveBeenCalledOnce();
      expect(vi.getTimerCount()).toBe(1); // Only the failed owned key's bounded fault retry.
      h.deliverAgent.mockClear();
      const select = vi.spyOn(h.mesh, "listAllShared"), get = vi.spyOn(h.mesh, "get");
      const residencyReads = () => get.mock.calls.map(([read]) => read).filter(read => read.startsWith("residency/"));
      for (let n = 1; n <= 12; n++) {
        await h.mesh.put({ key: "topology/hosts/lease-owner", identity: writer, value: { updatedAt: n * 60_000, expiresAt: (n + 2) * 60_000 } });
        for (const file of ["state.json", "state.read-journal.jsonl", "state.read-signal.json"]) events.fire(h.meshRoot, file);
        await settled();
      }
      expect(h.deliverAgent).not.toHaveBeenCalled(); expect(h.complete).not.toHaveBeenCalled(); expect(select).not.toHaveBeenCalled();
      expect(residencyReads()).toEqual([]);
      const other = residentDeliveryPrefix(h.config.rootId) + "new";
      await h.mesh.put({ key: other, identity: writer, value: { ...value, id: "new", message: "new" } });
      const notifications = path.join(h.meshRoot, RESIDENCY_NOTIFICATION_DIR);
      events.fire(notifications, residencyNotificationName(other)); await settled();
      expect(h.deliverAgent).toHaveBeenCalledOnce(); expect(h.deliverAgent.mock.calls[0]![0].message).toBe("new");
      // A genuinely new key costs one shared lookup of its namespace to resolve the hash.
      // It admits only that key: the unchanged failed key is neither read nor retried.
      expect(select.mock.calls).toEqual([["residency/deliveries/", { fresh: true }]]);
      expect(residencyReads()).toEqual([other]);
      expect(h.mesh.get(key, { fresh: true })).toBeDefined();
      select.mockClear(); get.mockClear(); h.deliverAgent.mockClear();
      // A duplicate/adversarial notification of a known key is a per-key re-read, not a
      // state change, retry grant or scan.
      events.fire(notifications, residencyNotificationName(key)); events.fire(notifications, residencyNotificationName(key)); await settled();
      expect(h.deliverAgent).not.toHaveBeenCalled(); expect(select).not.toHaveBeenCalled(); expect(residencyReads()).toEqual([key]);
      get.mockClear();
      // A forged unknown filename admits nothing: one lookup per namespace finds no match,
      // the failed key is not retried, and repeating it at the same state costs nothing.
      const forged = `${"f".repeat(64)}.json`;
      events.fire(notifications, forged); await settled();
      expect(select.mock.calls.map(([prefix]) => prefix)).toEqual(["residency/deliveries/", "residency/completion-claims/"]);
      expect(residencyReads()).toEqual([]); expect(h.deliverAgent).not.toHaveBeenCalled();
      select.mockClear();
      events.fire(notifications, forged); events.fire(notifications, `.${residencyNotificationName(other)}.tmp`); await settled();
      expect(select).not.toHaveBeenCalled(); expect(residencyReads()).toEqual([]); expect(h.deliverAgent).not.toHaveBeenCalled();
      await h.mesh.put({ key, identity: writer, value: { ...value, message: "repaired" } });
      events.fire(notifications, residencyNotificationName(key)); events.fire(notifications, residencyNotificationName(key)); await settled();
      expect(h.deliverAgent).toHaveBeenCalledOnce(); expect(h.deliverAgent.mock.calls[0]![0].message).toBe("repaired");
      expect(select).not.toHaveBeenCalled(); expect(h.mesh.get(key, { fresh: true })).toBeUndefined();
      // The pending fault retry finds its key settled, then the healthy client holds no timer.
      await vi.advanceTimersByTimeAsync(60_000); await settled();
      expect(h.deliverAgent).toHaveBeenCalledOnce(); expect(select).not.toHaveBeenCalled(); expect(vi.getTimerCount()).toBe(0);
    } finally { warn.mockRestore(); await h.client.close(); h.mesh.closeState(); }
  });

  it.each([
    "Main busy",
    "Main's followUp queue is full (test); Main is busy and reads followUps only at its next tool boundary. Wait, or send a short steer.",
    "Main has no follow-up journal open; retry the durable delivery later",
    "Main follow-up journal barrier is closed",
  ])("redelivers one transient admission failure after backoff, without an event or safety tick: %s", async message => {
    const h = fixture(); fakeWatches();
    const key = residentDeliveryPrefix(h.config.rootId) + "busy-once";
    await h.mesh.put({ key, identity: { id: residentHostId(h.config.rootId), name: "host", kind: "main" }, value: {
      format: 1, id: "busy-once", rootId: h.config.rootId, from: { id: "actor", name: "actor", kind: "actor" },
      message: "busy-once", delivery: "followUp", triggerTurn: true, createdAt: 1 } });
    h.deliverAgent.mockImplementationOnce(() => { throw new Error(message); });
    vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.useFakeTimers();
    const drain = vi.spyOn(CompletionJournal.prototype, "drainChanged");
    h.client.start(); await vi.advanceTimersByTimeAsync(20); await drain.mock.results[0]!.value; await settled();
    expect(h.deliverAgent).toHaveBeenCalledOnce(); expect(vi.getTimerCount()).toBe(1);
    const select = vi.spyOn(h.mesh, "listAllShared"), get = vi.spyOn(h.mesh, "get");
    await vi.advanceTimersByTimeAsync(999); await settled(); expect(h.deliverAgent).toHaveBeenCalledOnce();
    await vi.advanceTimersByTimeAsync(1); await settled(); expect(h.deliverAgent).toHaveBeenCalledTimes(2);
    expect(get.mock.calls.map(([read]) => read).filter(read => read.startsWith("residency/deliveries/"))).toEqual([key]);
    expect(select).not.toHaveBeenCalled(); expect(vi.getTimerCount()).toBe(0);
    expect(h.mesh.get(key, { fresh: true })).toBeUndefined();
    select.mockClear(); get.mockClear();
    await vi.advanceTimersByTimeAsync(300_000); await settled();
    expect(h.deliverAgent).toHaveBeenCalledTimes(2); expect(select).not.toHaveBeenCalled(); expect(get).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each(["Main agent message must not be empty", "Unknown target: vanished", "Permanent refusal: forbidden"])(
    "does not sweep a permanent failure into another key's transient retry: %s", async permanent => {
      const h = fixture(); fakeWatches();
      const writer = { id: residentHostId(h.config.rootId), name: "host", kind: "main" as const };
      const keys = ["permanent", "transient"].map(id => residentDeliveryPrefix(h.config.rootId) + id);
      for (const [index, key] of keys.entries()) await h.mesh.put({ key, identity: writer, value: {
        format: 1, id: index ? "transient" : "permanent", rootId: h.config.rootId, from: { id: "actor", name: "actor", kind: "actor" },
        message: index ? "transient" : "permanent", delivery: "followUp", triggerTurn: true, createdAt: 1 } });
      let transientAttempts = 0;
      h.deliverAgent.mockImplementation(request => {
        if (request.message === "permanent") throw new Error(permanent);
        if (++transientAttempts === 1) throw new Error("Main busy");
      });
      vi.spyOn(console, "warn").mockImplementation(() => {}); vi.useFakeTimers();
      const drain = vi.spyOn(CompletionJournal.prototype, "drainChanged");
      h.client.start(); await vi.advanceTimersByTimeAsync(20); await drain.mock.results[0]!.value; await settled();
      expect(h.deliverAgent).toHaveBeenCalledTimes(2); expect(vi.getTimerCount()).toBe(1);
      await vi.advanceTimersByTimeAsync(1_000); await settled();
      expect(h.deliverAgent.mock.calls.filter(([request]) => request.message === "permanent")).toHaveLength(1);
      expect(transientAttempts).toBe(2); expect(h.mesh.get(keys[0]!, { fresh: true })).toBeDefined();
      expect(h.mesh.get(keys[1]!, { fresh: true })).toBeUndefined(); expect(vi.getTimerCount()).toBe(0);
      await vi.advanceTimersByTimeAsync(300_000); await settled();
      expect(h.deliverAgent).toHaveBeenCalledTimes(3); expect(vi.getTimerCount()).toBe(0);
    });

  it.each(["Main busy", "mesh lock"])("caps %s delivery attempts at eight, logs exhaustion once, and retains explicit recovery", async kind => {
    const h = fixture(); fakeWatches();
    const key = residentDeliveryPrefix(h.config.rootId) + "exhausted";
    await h.mesh.put({ key, identity: { id: residentHostId(h.config.rootId), name: "host", kind: "main" }, value: {
      format: 1, id: "exhausted", rootId: h.config.rootId, from: { id: "actor", name: "actor", kind: "actor" },
      message: "exhausted", delivery: "followUp", triggerTurn: true, createdAt: 1 } });
    h.deliverAgent.mockImplementation(() => { throw kind === "mesh lock" ? new MeshLockTimeoutError("test", 1, 1) : new Error(kind); });
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {}); vi.useFakeTimers();
    // This test owns delivery retries, not asynchronous journal I/O; keep the
    // startup clock anchored before asserting exact per-attempt backoff.
    vi.spyOn(CompletionJournal.prototype, "drainChanged").mockResolvedValue(undefined);
    h.client.start(); await vi.advanceTimersByTimeAsync(20); await settled();
    expect(h.deliverAgent).toHaveBeenCalledOnce();
    for (const [index, ms] of [1_000, 2_000, 4_000, 5_000, 5_000, 5_000, 5_000].entries()) {
      await vi.advanceTimersByTimeAsync(ms); await settled(); expect(h.deliverAgent).toHaveBeenCalledTimes(index + 2);
    }
    await vi.advanceTimersByTimeAsync(300_000); await settled();
    expect(h.deliverAgent).toHaveBeenCalledTimes(8); expect(vi.getTimerCount()).toBe(0);
    expect(warn.mock.calls.filter(([line]) => String(line).includes("retry exhausted after 8 attempts"))).toHaveLength(1);
    expect(h.mesh.get(key, { fresh: true })).toBeDefined();
    h.deliverAgent.mockImplementation(() => {}); h.client.retryDeliveries();
    await vi.waitFor(() => expect(h.mesh.get(key, { fresh: true })).toBeUndefined()); await settled();
    expect(h.deliverAgent).toHaveBeenCalledTimes(9);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("retries a faulted owned delivery without any event at 1 s then 2 s, then idles with zero timers", async () => {
    const h = fixture(), events = fakeWatches();
    const key = residentDeliveryPrefix(h.config.rootId) + "faulted";
    await h.mesh.put({ key, identity: { id: residentHostId(h.config.rootId), name: "host", kind: "main" }, value: {
      format: 1, id: "faulted", rootId: h.config.rootId, from: { id: "actor", name: "actor", kind: "actor" },
      message: "faulted", delivery: "followUp", triggerTurn: true, createdAt: 1 } });
    h.deliverAgent.mockImplementationOnce(() => { throw new Error("Main busy"); }).mockImplementationOnce(() => { throw new Error("Main busy"); });
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.useFakeTimers();
    const drain = vi.spyOn(CompletionJournal.prototype, "drainChanged");
    // The startup pass settles at t=20 (fake clock), so its fault retry is due at t=1020.
    h.client.start(); await vi.advanceTimersByTimeAsync(20); await drain.mock.results[0]!.value; await settled();
    expect(h.deliverAgent).toHaveBeenCalledOnce(); expect(vi.getTimerCount()).toBe(1);
    const select = vi.spyOn(h.mesh, "listAllShared");
    await vi.advanceTimersByTimeAsync(999); await settled(); expect(h.deliverAgent).toHaveBeenCalledOnce();
    await vi.advanceTimersByTimeAsync(1); await settled(); expect(h.deliverAgent).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(1_999); await settled(); expect(h.deliverAgent).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(1); await settled(); expect(h.deliverAgent).toHaveBeenCalledTimes(3);
    expect(h.mesh.get(key, { fresh: true })).toBeUndefined();
    // Owned-key retries re-read the key, never a namespace sweep; success disarms the backoff.
    expect(select).not.toHaveBeenCalled(); expect(vi.getTimerCount()).toBe(0);
    await vi.advanceTimersByTimeAsync(5 * 60_000); await settled();
    expect(h.deliverAgent).toHaveBeenCalledTimes(3); expect(warn).toHaveBeenCalledOnce(); // Deduplicated diagnostic.
    expect(events.watches.length).toBeGreaterThan(0);
    await h.client.close(); expect(vi.getTimerCount()).toBe(0);
  });

  it("reattaches a watcher error through the fault backoff, and close cancels the armed retry", async () => {
    const h = fixture(), events = fakeWatches();
    vi.useFakeTimers();
    const drain = vi.spyOn(CompletionJournal.prototype, "drainChanged");
    h.client.start(); await vi.advanceTimersByTimeAsync(20); await drain.mock.results[0]!.value; await settled();
    expect(vi.getTimerCount()).toBe(0);
    const live = () => events.watches.filter(watch => watch.dir === h.meshRoot && !watch.watcher.close.mock.calls.length);
    const lost = live()[0]!; lost.watcher.emit("error", new Error("lost watch")); await settled();
    expect(lost.watcher.close).toHaveBeenCalled(); expect(live()).toHaveLength(0);
    // Lost at t=20: reattachment is due at t=1020 and is not an idle poll.
    await vi.advanceTimersByTimeAsync(999); expect(live()).toHaveLength(0);
    await vi.advanceTimersByTimeAsync(1); await settled(); expect(live()).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(5_000); await settled(); expect(vi.getTimerCount()).toBe(0);
    live()[0]!.watcher.emit("error", new Error("lost again")); await settled();
    expect(vi.getTimerCount()).toBeGreaterThan(0);
    await h.client.close(); expect(vi.getTimerCount()).toBe(0);
    await vi.advanceTimersByTimeAsync(120_000); expect(live()).toHaveLength(0);
  });

  it("retains a trailing filename notification during an awaited initial drain and close joins it", async () => {
    const h = fixture(), events = fakeWatches();
    fs.mkdirSync(path.join(h.meshRoot, "agent-completions"), { recursive: true });
    let release!: () => void, entered!: () => void;
    const held = new Promise<void>(resolve => { release = resolve; }), reading = new Promise<void>(resolve => { entered = resolve; });
    const original = CompletionJournal.prototype.drainChanged;
    const drain = vi.spyOn(CompletionJournal.prototype, "drainChanged");
    let first = true;
    drain.mockImplementation(async function (this: CompletionJournal, ...args) { if (first) { first = false; entered(); await held; } await original.apply(this, args); });
    h.client.start(); await reading;
    const value = h.result(1); saveCompletion(h.meshRoot, h.recipient, value);
    const filename = fs.readdirSync(path.join(h.meshRoot, "agent-completions")).find(file => file.endsWith(".json"))!;
    events.fire(path.join(h.meshRoot, "agent-completions"), filename);
    release(); await vi.waitFor(() => expect(drain).toHaveBeenCalledTimes(2)); await vi.waitFor(() => expect(h.complete).toHaveBeenCalledOnce());
    await h.client.close();
  });

  it("wakes real delivery and completion arrivals after idle, including an initially missing subdirectory", async () => {
    const h = fixture("file", process.platform); h.client.start(); await settled();
    saveCompletion(h.meshRoot, h.recipient, h.result(1));
    await vi.waitFor(() => expect(h.complete).toHaveBeenCalledOnce(), { timeout: 6_000 });
    await h.mesh.put({ key: residentDeliveryPrefix(h.config.rootId) + "message", identity: { id: residentHostId(h.config.rootId), name: "host", kind: "main" },
      value: { format: 1, id: "message", rootId: h.config.rootId, from: { id: "actor", name: "actor", kind: "actor" }, message: "arrival", delivery: "followUp", triggerTurn: false, createdAt: Date.now() } });
    await vi.waitFor(() => expect(h.deliverAgent).toHaveBeenCalledOnce(), { timeout: 6_000 });
    expect(h.deliverAgent.mock.calls[0]![0]).toMatchObject({ message: "arrival", deliveryId: `resident:${h.config.rootId}:message` });
  });

  it.skipIf(process.platform === "win32")("reattaches a real watcher after the whole completion directory is replaced", async () => {
    const h = fixture(), journal = path.join(h.meshRoot, "agent-completions"); fs.mkdirSync(journal, { recursive: true });
    const drain = vi.spyOn(CompletionJournal.prototype, "drainChanged");
    h.client.start(); await vi.waitFor(() => expect(drain).toHaveBeenCalledOnce()); await drain.mock.results[0]!.value;
    fs.renameSync(journal, path.join(h.meshRoot, "old-completions"));
    saveCompletion(h.meshRoot, h.recipient, h.result(1));
    await vi.waitFor(() => expect(h.complete).toHaveBeenCalledOnce());
  });

  it("wakes a known quiet exact-owner envelope on syncPiModels policy reload without foreign discovery", async () => {
    const h = fixture(); fakeWatches(); h.config.agents.notifyOnComplete = false;
    saveCompletion(h.meshRoot, h.recipient, h.result(1));
    saveCompletion(h.meshRoot, { ...h.recipient, rootId: "session:other", sessionId: "other" }, h.result(2));
    const drain = vi.spyOn(CompletionJournal.prototype, "drainChanged");
    h.client.start(); await vi.waitFor(() => expect(h.mesh.listAll("residency/completion-claims/")).toHaveLength(1)); await settled();
    expect(h.complete).not.toHaveBeenCalled();
    const readdir = vi.spyOn(fs.promises, "readdir");
    h.config.agents.notifyOnComplete = true; h.client.syncPiModels();
    await vi.waitFor(() => expect(h.complete).toHaveBeenCalledOnce());
    expect(drain.mock.calls.at(-1)?.[0]).toBe(true); expect(readdir).not.toHaveBeenCalled();
  });

  it.each(["explicit", "inferred"])("revisits a quiet own legacy %s completion exactly once on policy enable without changing its durable record", async (kind) => {
    const h = fixture(); fakeWatches(); h.config.agents.notifyOnComplete = false;
    const seed = async (index: number, rootId: string, writer = residentHostId(rootId)) => {
      const result = h.result(index), root = residentRoot(h.meshRoot, rootId), runDirectory = path.join(root, "runs", result.id);
      const recipient = { ...h.recipient, rootId, sessionId: rootId === h.config.rootId ? h.config.sessionId : "other" };
      fs.mkdirSync(runDirectory, { recursive: true }); fs.mkdirSync(path.join(root, "agents"), { recursive: true });
      fs.writeFileSync(path.join(root, "config.json"), JSON.stringify({ ...h.config, rootId, sessionId: recipient.sessionId, residencyRoot: root }));
      fs.writeFileSync(path.join(root, "agents", `${result.id}.json`), JSON.stringify({ format: 1, rootId, id: result.id, runDirectory, handle: result, createdAt: 1, updatedAt: 2 }));
      fs.writeFileSync(path.join(runDirectory, "status.json"), JSON.stringify(result));
      fs.writeFileSync(path.join(runDirectory, "completion-recipient.json"), JSON.stringify({ meshRoot: h.meshRoot, recipient }));
      const key = residentDeliveryPrefix(rootId) + result.id;
      await h.mesh.put({ key, identity: { id: writer, name: "resident", kind: "main" }, value: {
        format: 1, id: result.id, rootId, from: { id: result.id, name: result.name, kind: "agent" },
        ...(kind === "explicit" ? { agentCompletionId: result.id } : { data: result }),
        message: "legacy summary", delivery: "followUp", triggerTurn: true, createdAt: 2,
      } });
      return { key, result, runDirectory };
    };
    const own = await seed(1, h.config.rootId), foreign = await seed(2, "session:other");
    const spoof = await seed(3, h.config.rootId, residentHostId("session:other"));
    const original = h.mesh.get(own.key, { fresh: true });
    const drain = vi.spyOn(CompletionJournal.prototype, "drainChanged");
    vi.useFakeTimers(); h.client.start(); await vi.advanceTimersByTimeAsync(0);
    await drain.mock.results[0]!.value; await settled();
    expect(h.complete).not.toHaveBeenCalled(); expect(h.deliverAgent).not.toHaveBeenCalled();
    const before = drain.mock.calls.length;
    const journal = path.join(h.meshRoot, "agent-completions");
    const foreignFiles = fs.readdirSync(journal).filter(file => /^[a-f0-9]{64}\.json$/.test(file)).map(file => path.join(journal, file));
    expect(foreignFiles).toHaveLength(1); // Only the foreign legacy import exists while own notifications are disabled.
    const read = vi.spyOn(fs, "readFileSync"), asyncRead = vi.spyOn(fs.promises, "readFile"), open = vi.spyOn(fs.promises, "open"), readdir = vi.spyOn(fs.promises, "readdir");
    h.client.syncPiModels(); await settled();
    await vi.advanceTimersByTimeAsync(59_999); await settled();
    expect(drain).toHaveBeenCalledTimes(before); expect(h.complete).not.toHaveBeenCalled();
    expect(read.mock.calls.some(([file]) => String(file).startsWith(own.runDirectory))).toBe(false);
    h.config.agents.notifyOnComplete = true; h.client.syncPiModels(); await settled();
    await vi.waitFor(() => expect(h.complete).toHaveBeenCalledOnce());
    expect(h.complete.mock.calls[0]![0]).toMatchObject({ id: own.result.id, text: "done" });
    expect(h.mesh.get(own.key, { fresh: true })).toEqual(original); // No durable-record mutation wakes it.
    h.client.syncPiModels(); await vi.advanceTimersByTimeAsync(60_001); await settled();
    expect(h.complete).toHaveBeenCalledOnce(); expect(h.deliverAgent).not.toHaveBeenCalled();
    expect(h.mesh.get(foreign.key, { fresh: true })).toBeDefined(); expect(h.mesh.get(spoof.key, { fresh: true })).toBeDefined();
    expect(read.mock.calls.some(([file]) => [foreign.runDirectory, spoof.runDirectory].some(dir => String(file).startsWith(dir)))).toBe(false);
    for (const calls of [read.mock.calls, asyncRead.mock.calls, open.mock.calls]) {
      expect(calls.some(([file]) => foreignFiles.includes(String(file)))).toBe(false);
    }
    // Saving the newly enabled own envelope changes the directory stamp; the
    // journal pass may list names, but never reopens foreign bodies.
    expect(readdir.mock.calls.length).toBeLessThanOrEqual(1);
    await h.client.close(); expect(vi.getTimerCount()).toBe(0);
  });

  it("retires the unchanged legacy source only after actual inbox consumption on reconnect", async () => {
    const h = fixture(), own = await seedLegacy(h);
    h.client.start(); await vi.waitFor(() => expect(h.complete).toHaveBeenCalledOnce()); await settled();
    const original = h.mesh.get(own.key, { fresh: true });
    await h.client.close();
    expect(JSON.parse(fs.readFileSync(own.metadataPath, "utf8")).completionConsumedAt).toBeUndefined();
    h.complete.mockClear();
    const reconnect = new ResidencyClient({ config: h.config, mesh: h.mesh, participants: h.client.options.participants,
      mainAgent: h.client.options.mainAgent, onBackgroundComplete: h.complete }); clients.push(reconnect);
    reconnect.start(); await vi.waitFor(() => expect(h.complete).toHaveBeenCalledOnce()); await settled();
    expect(h.mesh.get(own.key, { fresh: true })).toEqual(original);
    h.complete.mock.calls[0]![1]();
    await vi.waitFor(() => expect(h.mesh.get(own.key, { fresh: true })).toBeUndefined());
    expect(JSON.parse(fs.readFileSync(own.metadataPath, "utf8")).completionConsumedAt).toBeGreaterThan(0);
    expect(h.complete).toHaveBeenCalledOnce();
  });

  it("wakes only the acknowledged exact key on late wait; unread and foreign sources stay quiet", async () => {
    const h = fixture(); fakeWatches();
    const own = await seedLegacy(h), unread = await seedLegacy(h, 2), foreign = await seedLegacy(h, 3, "session:other"),
      spoof = await seedLegacy(h, 4, h.config.rootId, residentHostId("session:other"));
    vi.useFakeTimers(); h.client.start(); await vi.advanceTimersByTimeAsync(0);
    await vi.waitFor(() => expect(h.complete).toHaveBeenCalledTimes(2)); await settled();
    const get = vi.spyOn(h.mesh, "get"), select = vi.spyOn(h.mesh, "listAllShared"), read = vi.spyOn(fs, "readFileSync");
    await vi.advanceTimersByTimeAsync(59_000); await settled();
    expect(get).not.toHaveBeenCalled(); expect(select).not.toHaveBeenCalled();
    // A status that may still run/retry cannot acknowledge even a previously enqueued result.
    fs.writeFileSync(path.join(unread.runDirectory, "status.json"), JSON.stringify({ ...unread.result, status: "running" }));
    h.client.acknowledgeCompletion(unread.result.id); await settled();
    expect(get).not.toHaveBeenCalled();
    expect(JSON.parse(fs.readFileSync(unread.metadataPath, "utf8")).completionConsumedAt).toBeUndefined();
    expect((await h.client.waitAgent(own.result.id)).status).toBe("completed"); await settled();
    const deliveryReads = get.mock.calls.filter(([key]) => key.startsWith("residency/deliveries/"));
    expect(deliveryReads.map(([key]) => key)).toEqual([own.key]);
    expect(deliveryReads[0]![1]).toEqual({ fresh: true }); expect(select).not.toHaveBeenCalled();
    expect(read.mock.calls.some(([file]) => String(file).startsWith(foreign.runDirectory) || String(file).startsWith(spoof.runDirectory))).toBe(false);
    expect(h.mesh.get(own.key, { fresh: true })).toBeUndefined();
    for (const item of [unread, foreign, spoof]) expect(h.mesh.get(item.key, { fresh: true })).toBeDefined();
    expect(h.complete).toHaveBeenCalledTimes(2);
    await h.client.close(); expect(vi.getTimerCount()).toBe(0);
  });

  it("treats receipt filenames as hints, not acknowledgment authority, and wakes only known sources", async () => {
    const h = fixture(), events = fakeWatches(), own = await seedLegacy(h);
    const receipts = path.join(h.meshRoot, "agent-completions", "receipts"); fs.mkdirSync(receipts, { recursive: true });
    h.client.start(); await vi.waitFor(() => expect(h.complete).toHaveBeenCalledOnce()); await settled();
    const get = vi.spyOn(h.mesh, "get"), remove = vi.spyOn(h.mesh, "delete");
    events.fire(receipts, `${"f".repeat(64)}.json`); await settled();
    expect(get.mock.calls.some(([key]) => key.startsWith("residency/deliveries/"))).toBe(false);
    const filename = `${createHash("sha256").update(own.result.id).digest("hex")}.json`;
    events.fire(receipts, filename); await settled();
    expect(remove).not.toHaveBeenCalled(); expect(h.mesh.get(own.key, { fresh: true })).toBeDefined();
    consumeCompletion(h.meshRoot, own.result.id, h.config.sessionId);
    events.fire(receipts, filename); await vi.waitFor(() => expect(h.mesh.get(own.key, { fresh: true })).toBeUndefined());
    expect(remove).toHaveBeenCalledWith({ key: own.key, ifVersion: expect.any(Number) });
    expect(h.complete).toHaveBeenCalledOnce();
  });

  it.each(["foreign-writer", "CAS-refusal"])("revalidates an acknowledgment wake against %s before source retirement", async mode => {
    const h = fixture(), events = fakeWatches(); const own = await seedLegacy(h);
    h.client.start(); await vi.waitFor(() => expect(h.complete).toHaveBeenCalledOnce()); await settled();
    const original = h.mesh.get(own.key, { fresh: true })!;
    const remove = vi.spyOn(h.mesh, "delete");
    if (mode === "foreign-writer") {
      await h.mesh.put({ key: own.key, value: original.value, identity: { id: residentHostId("session:other"), name: "foreign", kind: "main" } });
    } else remove.mockResolvedValueOnce({ deleted: false });
    h.complete.mock.calls[0]![1](); await settled();
    expect(h.mesh.get(own.key, { fresh: true })).toBeDefined();
    if (mode === "foreign-writer") expect(remove).not.toHaveBeenCalled();
    else {
      expect(remove).toHaveBeenCalledWith({ key: own.key, ifVersion: original.version });
      vi.useFakeTimers();
      await vi.advanceTimersByTimeAsync(5 * 60_000); await settled();
      expect(h.mesh.get(own.key, { fresh: true })).toBeDefined(); // No retry tick.
      h.client.retryDeliveries(); await settled();
      expect(h.mesh.get(own.key, { fresh: true })).toBeUndefined();
    }
    expect(h.complete).toHaveBeenCalledOnce();
  });

  it("keeps file-only watchdog recovery independent of a validated mesh selector outage", async () => {
    const h = fixture(); fakeWatches();
    fs.mkdirSync(h.config.actorRoot, { recursive: true });
    fs.writeFileSync(path.join(h.config.actorRoot, "actors.json"), JSON.stringify({ actors: [
      { id: "actor", rootId: h.config.rootId, residency: "durable", status: "idle" },
    ] }));
    vi.spyOn(kernelFence, "kernelFenceAvailable").mockReturnValue(true);
    vi.spyOn(h.mesh, "listAllShared").mockImplementation(() => { throw new Error("selector unavailable"); });
    const start = vi.spyOn(h.client, "ensureHost").mockResolvedValue({} as Awaited<ReturnType<typeof h.client.ensureHost>>);
    vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.useFakeTimers(); h.client.start(); await vi.advanceTimersByTimeAsync(20);
    expect(start).toHaveBeenCalledOnce();
    await h.client.close(); expect(vi.getTimerCount()).toBe(0);
  });

  it("repairs watch attachment at the non-waking safety boundary, but never discovers missed work there", async () => {
    const h = fixture(), events = fakeWatches();
    await h.mesh.put({ key: "topology/heartbeats/other", identity: { id: "other", name: "other", kind: "main" }, value: { at: 1 } });
    vi.useFakeTimers();
    const drain = vi.spyOn(CompletionJournal.prototype, "drainChanged");
    h.client.start(); await vi.advanceTimersByTimeAsync(0);
    expect(drain).toHaveBeenCalledOnce(); await drain.mock.results[0]!.value;
    const select = vi.spyOn(h.mesh, "listAllShared"), read = vi.spyOn(fs.promises, "readFile"), readdir = vi.spyOn(fs.promises, "readdir");
    const syncRead = vi.spyOn(fs, "readFileSync");
    await vi.advanceTimersByTimeAsync(59_999); expect(select).not.toHaveBeenCalled(); expect(read).not.toHaveBeenCalled(); expect(readdir).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1); await drain.mock.results.at(-1)!.value;
    expect(read).not.toHaveBeenCalled(); expect(readdir).not.toHaveBeenCalled();
    expect(syncRead.mock.calls.some(([file]) => String(file) === path.join(h.meshRoot, "state.json"))).toBe(false);
    const active = events.watches.find(watch => watch.dir === h.meshRoot)!; active.watcher.emit("error", new Error("lost watch")); await settled();
    // Join the watcher-error pass (real async I/O) so the omitted-event write cannot race into it.
    expect(drain).toHaveBeenCalledTimes(2); await drain.mock.results.at(-1)!.value; await settled();
    saveCompletion(h.meshRoot, h.recipient, h.result(1)); // Deliberately omit its event.
    await vi.advanceTimersByTimeAsync(5 * 60_000); await settled();
    expect(h.complete).not.toHaveBeenCalled();
    expect(drain).toHaveBeenCalledTimes(2); // Startup and the real watcher error only.
    events.fire(path.join(h.meshRoot, "agent-completions"), null); await settled();
    await vi.waitFor(() => expect(drain).toHaveBeenCalledTimes(3));
    await drain.mock.results.at(-1)!.value; await settled();
    expect(h.complete).toHaveBeenCalledOnce();
    await h.client.close(); expect(vi.getTimerCount()).toBe(0);
  });
});
