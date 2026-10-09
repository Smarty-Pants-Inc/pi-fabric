import fs, { type FSWatcher } from "node:fs";
import os from "node:os";
import path from "node:path";
import { EventEmitter } from "node:events";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorMeshMonitor, meshObserverWatch } from "../src/actors/mesh-monitor.js";
import { LifecycleBroker } from "../src/lifecycle/broker.js";
import { FABRIC_PARTICIPANT_LIFECYCLE_TOPIC } from "../src/lifecycle/types.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import type { FabricParticipantSource } from "../src/topology/types.js";

const roots: string[] = [];
const closers: Array<() => void | Promise<void>> = [];
afterEach(async () => {
  for (const close of closers.splice(0)) await close();
  vi.restoreAllMocks(); vi.useRealTimers();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const identity: MeshIdentity = { id: "target", name: "target", kind: "main", sessionId: "target" };
const source = { id: "source", name: "source", kind: "root" as const, rootId: "source", runner: "pi" as const,
  ownerHostId: "source", ownerIdentityId: "source" };
const participants: FabricParticipantSource = {
  get: id => ({ format: 1, ...source, id, ownerHostId: id, ownerIdentityId: id, rootId: id,
    status: "idle", transport: "host", capabilities: ["followUp"], startedAt: 1, updatedAt: 1,
    controlProtocol: "v1", local: true, stale: false }),
  publishes: () => true, list: () => [], peers: () => [], self: () => participants.get("target")!,
  refresh: async () => {}, scheduleRefresh: () => {},
};
function store() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-observers-idle-")); roots.push(root);
  return new MeshStore(path.join(root, "mesh"), 65_536, 100);
}
function mockWatch() {
  const watchers: Array<{ directory: string; notify: (event: string, filename: string | null) => void; emitter: EventEmitter; close: ReturnType<typeof vi.fn> }> = [];
  vi.spyOn(fs, "watch").mockImplementation((...args: unknown[]) => {
    const emitter = new EventEmitter(); const close = vi.fn();
    watchers.push({ directory: String(args[0]), notify: args.at(-1) as typeof watchers[number]["notify"], emitter, close });
    return Object.assign(emitter, { close }) as unknown as FSWatcher;
  });
  return watchers;
}
const flush = async () => { for (let i = 0; i < 20; i++) await Promise.resolve(); };
const publish = (mesh: MeshStore) => mesh.publish({ topic: FABRIC_PARTICIPANT_LIFECYCLE_TOPIC,
  kind: "pi.agent_settled", from: { id: "source", name: "source", kind: "main" },
  data: { version: 1, event: "pi.agent_settled", source, occurredAt: Date.now() } });
async function lifecycle(mesh: MeshStore, deliver = vi.fn()) {
  const broker = new LifecycleBroker(mesh, identity, participants, { enabled: true, pollMs: 20, maxReadEvents: 100 }, deliver);
  closers.push(() => broker.close());
  await broker.subscribe({ from: "source", to: "target", events: ["pi.agent_settled"], delivery: "followUp", triggerTurn: false });
  return broker;
}

describe("event-driven mesh observers", () => {
  it("rejects a watch when its directory changes during attachment", () => {
    const mesh = store(); const close = vi.fn();
    vi.spyOn(fs, "watch").mockImplementation(() => {
      fs.renameSync(mesh.root, mesh.root + ".retired"); fs.mkdirSync(mesh.root);
      return Object.assign(new EventEmitter(), { close }) as unknown as FSWatcher;
    });
    expect(meshObserverWatch(mesh.root, { persistent: false }, () => {})).toBeUndefined();
    expect(close).toHaveBeenCalledOnce();
  });

  it("retires missing roots, ignores retired callbacks, and attaches a recreated root at safety cadence", async () => {
    vi.useFakeTimers(); const watches = mockWatch(); const mesh = store();
    const broker = await lifecycle(mesh); broker.start();
    const control = new FabricControlPlane(mesh, identity, { enabled: true, hostId: "target", pollMs: 20 });
    closers.push(() => control.close()); control.start(() => ({ accepted: true }));
    const actor = new ActorMeshMonitor(mesh, { enabled: true, actorPollMs: 20, maxReadEvents: 100 },
      { beforePoll: () => true, onEvent: () => {} });
    closers.push(() => actor.close()); actor.start(); await vi.advanceTimersByTimeAsync(0); await flush();
    const retired = [...watches]; fs.renameSync(mesh.root, mesh.root + ".retired");
    await vi.advanceTimersByTimeAsync(60_000);
    for (const watch of retired) expect(watch.close).toHaveBeenCalledOnce();
    fs.cpSync(mesh.root + ".retired", mesh.root, { recursive: true });
    await vi.advanceTimersByTimeAsync(60_000);
    expect(watches.length).toBe(retired.length * 2);
    const tails = vi.spyOn(mesh, "tail"); const lists = vi.spyOn(mesh, "listAll");
    for (const watch of retired) { watch.notify("change", null); watch.emitter.emit("error", new Error("late")); }
    await vi.advanceTimersByTimeAsync(0); await flush();
    expect(tails).not.toHaveBeenCalled(); expect(lists).not.toHaveBeenCalled();
  });

  it("reconciles silently replaced ancillary directories without churning the unchanged root", async () => {
    vi.useFakeTimers(); const watches = mockWatch(); const mesh = store();
    const directory = path.join(mesh.root, "participants"); fs.mkdirSync(directory);
    const broker = await lifecycle(mesh); broker.start(); const beforePoll = vi.fn(() => true);
    const actor = new ActorMeshMonitor(mesh, { enabled: true, actorPollMs: 20, maxReadEvents: 100 },
      { beforePoll, onEvent: () => {} });
    closers.push(() => actor.close()); actor.start(); await flush();
    const rootWatches = watches.filter(watch => watch.directory === mesh.root);
    const retired = watches.filter(watch => watch.directory === directory);
    const count = watches.length;
    fs.renameSync(directory, directory + ".retired"); fs.mkdirSync(directory);
    await vi.advanceTimersByTimeAsync(60_000); await flush();
    expect(watches).toHaveLength(count + 2);
    for (const watch of retired) expect(watch.close).toHaveBeenCalledOnce();
    for (const watch of rootWatches) expect(watch.close).not.toHaveBeenCalled();
    const lists = vi.spyOn(mesh, "listAll"); const tails = vi.spyOn(mesh, "tail"); beforePoll.mockClear();
    for (const watch of retired) watch.notify("change", "late.json");
    await flush(); expect(lists).not.toHaveBeenCalled(); expect(beforePoll).not.toHaveBeenCalled();
    for (const watch of watches.slice(count)) watch.notify("change", "new.json");
    await flush(); expect(lists).toHaveBeenCalled(); expect(beforePoll).toHaveBeenCalled(); expect(tails).not.toHaveBeenCalled();
  });

  it("reattaches native root and ancillary inode replacements and wakes on NEW writes before another safety tick", async () => {
    vi.useFakeTimers(); const mesh = store();
    for (const directory of ["actors", "participants", "host-leases"]) fs.mkdirSync(path.join(mesh.root, directory));
    const delivered = vi.fn(); const broker = await lifecycle(mesh, delivered); broker.start();
    const handled = vi.fn(() => ({ accepted: true }));
    const control = new FabricControlPlane(mesh, identity, { enabled: true, hostId: "target", pollMs: 20 });
    closers.push(() => control.close()); control.start(handled);
    const seen = vi.fn(); const beforePoll = vi.fn(() => true);
    const actor = new ActorMeshMonitor(mesh, { enabled: true, actorPollMs: 20, maxReadEvents: 100 },
      { beforePoll, onEvent: event => { if (event.topic === "fleet.work.replaced") seen(event); } });
    closers.push(() => actor.close()); actor.start(); await vi.advanceTimersByTimeAsync(0); await flush();
    fs.renameSync(mesh.root, mesh.root + ".retired"); fs.cpSync(mesh.root + ".retired", mesh.root, { recursive: true });
    await vi.advanceTimersByTimeAsync(60_000); await flush(); vi.useRealTimers();
    await publish(mesh); await mesh.publish({ topic: "fleet.work.replaced", from: identity });
    await mesh.publish({ topic: "fabric.control.command", from: { ...identity, id: "sender" }, to: "target",
      data: { version: 1, commandId: "replaced", targetId: "target", operation: "steer", replyTo: "sender",
        requestedAt: Date.now(), deadlineAt: Date.now() + 120_000 } });
    await vi.waitFor(() => { expect(delivered).toHaveBeenCalledOnce(); expect(seen).toHaveBeenCalledOnce(); expect(handled).toHaveBeenCalledOnce(); });
    await new Promise(resolve => setTimeout(resolve, 1_100));
    // Ancillary-only replacement, independent of the root identity.
    for (const directory of ["actors", "participants", "host-leases"]) {
      fs.renameSync(path.join(mesh.root, directory), path.join(mesh.root, directory + ".retired"));
      fs.mkdirSync(path.join(mesh.root, directory));
    }
    await new Promise(resolve => setTimeout(resolve, 1_100));
    const lists = vi.spyOn(mesh, "listAll"); const tails = vi.spyOn(mesh, "tail");
    beforePoll.mockClear();
    fs.writeFileSync(path.join(mesh.root, "participants", "new.json"), "{}");
    await vi.waitFor(() => { expect(beforePoll).toHaveBeenCalled(); expect(lists).toHaveBeenCalled(); });
    expect(tails).not.toHaveBeenCalled();
  });
  it("does not scan lifecycle subscriptions or reread the unchanged control/actor tail during idle safety checks", async () => {
    vi.useFakeTimers(); const watches = mockWatch(); const mesh = store();
    const broker = await lifecycle(mesh); broker.start();
    const control = new FabricControlPlane(mesh, identity, { enabled: true, hostId: "target", pollMs: 20 });
    closers.push(() => control.close()); control.start(() => ({ accepted: true }));
    const actor = new ActorMeshMonitor(mesh, { enabled: true, actorPollMs: 20, maxReadEvents: 100 },
      { beforePoll: () => true, onEvent: () => {} });
    closers.push(() => actor.close()); actor.start();
    await vi.advanceTimersByTimeAsync(0); await flush();
    const lists = vi.spyOn(mesh, "listAll"); const tails = vi.spyOn(mesh, "tail"); const reads = vi.spyOn(fs, "readFileSync");
    const attached = watches.length;
    await vi.advanceTimersByTimeAsync(180_000);
    expect(watches).toHaveLength(attached); for (const watch of watches) expect(watch.close).not.toHaveBeenCalled();
    expect(lists).not.toHaveBeenCalled(); expect(tails).not.toHaveBeenCalled(); expect(reads).not.toHaveBeenCalled();
  });

  it("reconciles actor ownership on state notifications without rereading an unchanged event tail", async () => {
    vi.useFakeTimers(); const watches = mockWatch(); const mesh = store();
    const tail = vi.spyOn(mesh, "tail"); const beforePoll = vi.fn(() => true);
    const actor = new ActorMeshMonitor(mesh, { enabled: true, actorPollMs: 20, maxReadEvents: 100 },
      { beforePoll, onEvent: () => {} });
    closers.push(() => actor.close()); actor.start(); await flush(); tail.mockClear(); beforePoll.mockClear();
    for (let i = 0; i < 3; i++) {
      watches[0]!.notify("rename", "state.json"); await flush(); await vi.advanceTimersByTimeAsync(5_000);
    }
    expect(beforePoll).toHaveBeenCalledTimes(3); expect(tail).not.toHaveBeenCalled();
  });

  it.each(["halted", "lease"])("does not fast-retry an actor with no known work when admission is denied by %s", async reason => {
    vi.useFakeTimers(); mockWatch(); const mesh = store();
    const beforePoll = vi.fn(() => reason !== "halted"); const canConsumeMesh = vi.fn(() => reason !== "lease");
    const tail = vi.spyOn(mesh, "tail");
    const actor = new ActorMeshMonitor(mesh, { enabled: true, actorPollMs: 20, maxReadEvents: 100 },
      { beforePoll, canConsumeMesh, onEvent: () => {} });
    closers.push(() => actor.close()); actor.start(); await flush();
    expect(beforePoll).toHaveBeenCalledOnce(); expect(tail).not.toHaveBeenCalled(); expect(vi.getTimerCount()).toBe(1);
    await vi.advanceTimersByTimeAsync(59_999);
    expect(beforePoll).toHaveBeenCalledOnce(); expect(tail).not.toHaveBeenCalled(); expect(vi.getTimerCount()).toBe(1);
    await vi.advanceTimersByTimeAsync(1); expect(beforePoll).toHaveBeenCalledTimes(2); expect(tail).not.toHaveBeenCalled();
    actor.close(); expect(vi.getTimerCount()).toBe(0);
  });

  it("retains an actor notification made during synchronous dispatch as a trailing drain", async () => {
    vi.useFakeTimers(); const watches = mockWatch(); const mesh = store();
    const first = await mesh.publish({ topic: "fleet.work.first", from: identity });
    const second = { ...first, id: "second", sequence: first.sequence + 1 };
    const tail = vi.spyOn(mesh, "tail").mockReturnValueOnce({ events: [first], nextOffset: 10 })
      .mockReturnValue({ events: [second], nextOffset: 20 });
    let actor!: ActorMeshMonitor;
    const onEvent = vi.fn(event => { if (event.id === first.id) { watches[0]!.notify("change", null); actor.schedule(); } });
    actor = new ActorMeshMonitor({ root: mesh.root, latestOffset: () => 0, tail },
      { enabled: true, actorPollMs: 20, maxReadEvents: 100 }, { beforePoll: () => true, onEvent });
    closers.push(() => actor.close()); actor.start(); await flush();
    expect(onEvent.mock.calls.map(([event]) => event.id)).toEqual([first.id, second.id]);
    actor.close(); expect(vi.getTimerCount()).toBe(0);
  });

  it("recovers a missed lifecycle append at the fixed-file safety witness, not a fast fallback", async () => {
    vi.useFakeTimers(); const watches = mockWatch(); const mesh = store(); const deliver = vi.fn();
    const broker = await lifecycle(mesh, deliver); broker.start(); await flush();
    await publish(mesh); // Intentionally omit notification.
    await vi.advanceTimersByTimeAsync(59_999); expect(deliver).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1); await flush(); expect(deliver).toHaveBeenCalledOnce();
    const lists = vi.spyOn(mesh, "listAll");
    watches[0]!.emitter.emit("error", new Error("lost watch")); await flush(); lists.mockClear();
    await vi.advanceTimersByTimeAsync(59_999); expect(lists).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1); expect(watches).toHaveLength(2); expect(lists).not.toHaveBeenCalled();
  });

  it("retains a null-filename notification arriving during awaited lifecycle delivery", async () => {
    vi.useFakeTimers(); const watches = mockWatch(); const mesh = store();
    let release!: () => void;
    const held = new Promise<void>(resolve => { release = resolve; });
    const deliver = vi.fn().mockImplementationOnce(() => held);
    const broker = await lifecycle(mesh, deliver); broker.start(); await flush();
    await publish(mesh); watches[0]!.notify("change", null); await flush();
    expect(deliver).toHaveBeenCalledOnce();
    await publish(mesh); watches[0]!.notify("change", null); release(); await flush();
    expect(deliver).toHaveBeenCalledTimes(2);
    broker.pause(); const lists = vi.spyOn(mesh, "listAll");
    watches[0]!.notify("rename", null); await vi.advanceTimersByTimeAsync(120_000); expect(lists).not.toHaveBeenCalled();
    broker.resume(); await flush(); expect(lists).toHaveBeenCalled();
    await broker.close(); lists.mockClear(); watches[0]!.notify("change", null);
    watches[0]!.emitter.emit("error", new Error("late")); broker.resume(); broker.start();
    await vi.advanceTimersByTimeAsync(120_000); expect(lists).not.toHaveBeenCalled(); expect(vi.getTimerCount()).toBe(0);
  });

  it("does not let its own visible-but-unconfirmed lifecycle writes bypass the receipt retry cadence", async () => {
    vi.useFakeTimers(); const watches = mockWatch(); const mesh = store(); const deliver = vi.fn();
    const broker = await lifecycle(mesh, deliver); broker.start(); await flush();
    const put = mesh.put.bind(mesh);
    const writes = vi.spyOn(mesh, "put").mockImplementation(async input => { await put(input); throw new Error("unconfirmed fsync"); });
    await publish(mesh); watches[0]!.notify("change", "events.jsonl"); await flush();
    expect(deliver).toHaveBeenCalledOnce(); expect(writes).toHaveBeenCalledOnce();
    watches[0]!.notify("rename", "state.json"); await flush();
    await vi.advanceTimersByTimeAsync(19); expect(writes).toHaveBeenCalledOnce();
    await vi.advanceTimersByTimeAsync(1); expect(writes).toHaveBeenCalledTimes(2); expect(deliver).toHaveBeenCalledOnce();
    broker.pause(); writes.mockRestore(); await broker.checkpointForRelease();
  });

  it("recovers a missed control append and prevents pause/close callbacks from rearming", async () => {
    vi.useFakeTimers(); const watches = mockWatch(); const mesh = store(); const handler = vi.fn(() => ({ accepted: true }));
    const control = new FabricControlPlane(mesh, identity, { enabled: true, hostId: "target", pollMs: 20 });
    closers.push(() => control.close()); control.start(handler); await vi.advanceTimersByTimeAsync(0);
    await mesh.publish({ topic: "fabric.control.command", from: { id: "sender", name: "sender", kind: "main" }, to: "target",
      data: { version: 1, commandId: "missed", targetId: "target", operation: "steer", replyTo: "sender", requestedAt: Date.now(), deadlineAt: Date.now() + 120_000 } });
    await vi.advanceTimersByTimeAsync(59_999); expect(handler).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1); expect(handler).toHaveBeenCalledOnce();
    control.pause(); const tail = vi.spyOn(mesh, "tail"); watches[0]!.notify("change", null);
    watches[0]!.emitter.emit("error", new Error("paused")); await vi.advanceTimersByTimeAsync(120_000);
    expect(tail).not.toHaveBeenCalled(); expect(vi.getTimerCount()).toBe(0);
    control.resume(); await vi.advanceTimersByTimeAsync(0); expect(watches).toHaveLength(2);
    await control.close(); tail.mockClear(); watches[1]!.notify("change", null); control.resume();
    await vi.advanceTimersByTimeAsync(120_000); expect(tail).not.toHaveBeenCalled(); expect(vi.getTimerCount()).toBe(0);
  });

  it("retries full actor work without a log change and closes every retry resource", async () => {
    vi.useFakeTimers(); mockWatch(); const mesh = store();
    const event = await mesh.publish({ topic: "fleet.work.test", from: identity });
    let full = true; const onEvent = vi.fn(() => !full);
    const tail = vi.spyOn(mesh, "tail").mockReturnValue({ events: [event], nextOffset: 10, cursors: [10] });
    const actor = new ActorMeshMonitor({ root: mesh.root, latestOffset: () => 0, tail },
      { enabled: true, actorPollMs: 20, maxReadEvents: 100 }, { beforePoll: () => true, onEvent });
    closers.push(() => actor.close()); actor.start(); await flush(); expect(onEvent).toHaveBeenCalledOnce();
    full = false; await vi.advanceTimersByTimeAsync(20); expect(onEvent).toHaveBeenCalledTimes(2);
    tail.mockReturnValue({ events: [], nextOffset: 10 }); actor.close();
    await vi.advanceTimersByTimeAsync(120_000); expect(onEvent).toHaveBeenCalledTimes(2); expect(vi.getTimerCount()).toBe(0);
  });

  it("wakes actor reconciliation for native registry, child-completion and ownership changes without a mesh append", async () => {
    const mesh = store(); const directory = path.join(mesh.root, "actors", "session", "child-completions");
    fs.mkdirSync(directory, { recursive: true });
    fs.mkdirSync(path.join(mesh.root, "participants")); fs.mkdirSync(path.join(mesh.root, "host-leases"));
    const beforePoll = vi.fn(() => true); const tail = vi.spyOn(mesh, "tail");
    const actor = new ActorMeshMonitor(mesh, { enabled: true, actorPollMs: 20, maxReadEvents: 100 },
      { beforePoll, onEvent: () => {} });
    closers.push(() => actor.close()); actor.start(); await new Promise(resolve => setTimeout(resolve, 40));
    beforePoll.mockClear(); tail.mockClear();
    fs.writeFileSync(path.join(path.dirname(directory), "actors.json"), "{}");
    await vi.waitFor(() => expect(beforePoll).toHaveBeenCalled());
    await new Promise(resolve => setTimeout(resolve, 25)); beforePoll.mockClear();
    fs.writeFileSync(path.join(directory, "child.result.json"), "{}");
    await vi.waitFor(() => expect(beforePoll).toHaveBeenCalled(), { timeout: 2_000 });
    for (const directory of ["participants", "host-leases"]) {
      await new Promise(resolve => setTimeout(resolve, 25)); beforePoll.mockClear();
      fs.writeFileSync(path.join(mesh.root, directory, "changed.json"), "{}");
      await vi.waitFor(() => expect(beforePoll).toHaveBeenCalled(), { timeout: 2_000 });
    }
    expect(tail).not.toHaveBeenCalled();
  });

  it("delivers native post-idle appends to lifecycle, control and actor observers", async () => {
    const mesh = store(); const delivered = vi.fn(); const broker = await lifecycle(mesh, delivered); broker.start();
    const owner = new FabricControlPlane(mesh, identity, { enabled: true, hostId: "target", pollMs: 20 });
    const senderId = { ...identity, id: "sender" };
    const sender = new FabricControlPlane(mesh, senderId, { enabled: true, hostId: "sender", pollMs: 20 });
    closers.push(() => owner.close(), () => sender.close());
    const handled = vi.fn(() => ({ accepted: true })); owner.start(handled); sender.start(() => ({ accepted: false }));
    const seen = vi.fn(); const actor = new ActorMeshMonitor(mesh, { enabled: true, actorPollMs: 20, maxReadEvents: 100 },
      { beforePoll: () => true, onEvent: event => { if (event.topic === "fleet.work.native") seen(event); } });
    closers.push(() => actor.close()); actor.start();
    await new Promise(resolve => setTimeout(resolve, 80));
    await publish(mesh); await mesh.publish({ topic: "fleet.work.native", from: identity });
    await expect(sender.request("target", "target", "steer", { message: "native" })).resolves.toMatchObject({ acknowledged: true });
    await vi.waitFor(() => { expect(delivered).toHaveBeenCalledOnce(); expect(seen).toHaveBeenCalledOnce(); });
    expect(handled).toHaveBeenCalledOnce();
  });
});
