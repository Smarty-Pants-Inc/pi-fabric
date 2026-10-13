import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshLockTimeoutError } from "../src/core/atomic-write.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { FabricControlPlane, type FabricControlPlaneOptions } from "../src/topology/control-plane.js";

// fv2-7978: the control plane drains on real mesh notifications and its own publishes. Idle has
// no timer; one-shot timers exist only for retained work, real deadlines and a watcher re-arm.

const roots: string[] = [];
const planes: FabricControlPlane[] = [];
const identity = (id: string): MeshIdentity => ({ id, name: id, kind: "main", sessionId: id });
const meshRoot = (): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-control-wakes-"));
  roots.push(root);
  return path.join(root, "mesh");
};
const plane = (root: string, id: string, options: Partial<FabricControlPlaneOptions> = {}): FabricControlPlane => {
  const value = new FabricControlPlane(new MeshStore(root, 64 * 1024, 1_000), identity(id), {
    enabled: true, hostId: id, pollMs: 20, acknowledgementTimeoutMs: 1_000, ...options,
  });
  planes.push(value);
  return value;
};
const watchLimit = () => Object.assign(
  new Error("ENOSPC: System limit for number of file watchers reached, watch '/mesh'"), { code: "ENOSPC", syscall: "watch" });
const commands = (value: FabricControlPlane) => value.mesh.read({ topic: "fabric.control.command", limit: 100 });

afterEach(async () => {
  vi.useRealTimers();
  vi.restoreAllMocks();
  await Promise.all(planes.splice(0).map((value) => value.close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("control plane idle wakes", () => {
  it("arms no timer and reads nothing while idle; the old 20 ms poll would have ticked 3,000 times", async () => {
    const owner = plane(meshRoot(), "host:owner");
    const tail = vi.spyOn(owner.mesh, "tail");
    vi.useFakeTimers();
    owner.start(() => ({ accepted: true }));
    await vi.advanceTimersByTimeAsync(0);
    expect(tail).toHaveBeenCalledTimes(1); // the start drain
    expect(vi.getTimerCount()).toBe(0);
    await vi.advanceTimersByTimeAsync(60_000);
    expect(tail).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(0);
    await owner.close();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("delivers a remote command through the owner's real mesh watcher, with no poll interval", async () => {
    const root = meshRoot();
    // A 10 s poll interval would miss the bound below: only the notification can wake the owner.
    const owner = plane(root, "host:owner", { pollMs: 10_000, acknowledgementTimeoutMs: 40_000 });
    const sender = plane(root, "host:sender", { pollMs: 10_000, acknowledgementTimeoutMs: 40_000 });
    const handler = vi.fn(() => ({ accepted: true, messageId: "m1" }));
    owner.start(handler);
    sender.start(() => ({ accepted: false }));
    await new Promise((resolve) => setTimeout(resolve, 50));
    const ownerTail = vi.spyOn(owner.mesh, "tail");
    const startedAt = Date.now();
    await expect(sender.request("host:owner", "agent:target", "steer", { message: "hi", ownerIncarnation: owner.incarnation }, "host:owner"))
      .resolves.toMatchObject({ acknowledged: true, messageId: "m1" });
    expect(Date.now() - startedAt).toBeLessThan(5_000);
    expect(handler).toHaveBeenCalledOnce();
    expect(ownerTail).toHaveBeenCalled(); // woken by the event-log notification, not a timer
  });

  it("drains its own publish without any watcher, and warns once truthfully on the inotify limit", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    vi.spyOn(fs, "watch").mockImplementation(() => { throw watchLimit(); });
    const self = plane(meshRoot(), "host:self", { pollMs: 10_000, acknowledgementTimeoutMs: 40_000 });
    const handler = vi.fn(() => ({ accepted: true, messageId: "own" }));
    self.start(handler);
    await expect(self.request("host:self", "agent:target", "steer", { message: "hi", ownerIncarnation: self.incarnation }, "host:self"))
      .resolves.toMatchObject({ acknowledged: true, messageId: "own" });
    expect(handler).toHaveBeenCalledOnce();
    const lines = warn.mock.calls.map((call) => String(call[0])).filter((line) => line.includes("control plane: mesh notifications failed"));
    expect(lines).toHaveLength(1);
    expect(lines[0]).toContain("inotify watch limit, not disk space");
    expect(lines[0]).toContain("There is no polling");
    expect(lines.join("\n")).not.toMatch(/falling back to polling/i);
  });

  it("recovers from a watcher error with one rescan and a bounded one-shot re-arm, never an interval", async () => {
    vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const root = meshRoot();
    const watch = vi.spyOn(fs, "watch").mockImplementation(() => { throw watchLimit(); });
    const owner = plane(root, "host:owner", { pollMs: 10_000, acknowledgementTimeoutMs: 40_000 });
    const sender = plane(root, "host:sender", { pollMs: 10_000, acknowledgementTimeoutMs: 40_000 });
    const handler = vi.fn(() => ({ accepted: true, messageId: "late" }));
    owner.start(handler);
    sender.start(() => ({ accepted: false }));
    await new Promise((resolve) => setTimeout(resolve, 50)); // the start rescan has drained an empty log
    const outcome = sender.request("host:owner", "agent:target", "steer", { message: "hi", ownerIncarnation: owner.incarnation }, "host:owner");
    await vi.waitFor(() => expect(commands(sender)).toHaveLength(1));
    await new Promise((resolve) => setTimeout(resolve, 200));
    expect(handler).not.toHaveBeenCalled(); // unarmed and no polling: nothing reads the command yet
    watch.mockRestore(); // the limit is raised; the 1 s one-shot re-arm restores notifications
    await expect(outcome).resolves.toMatchObject({ acknowledged: true, messageId: "late" });
    expect(handler).toHaveBeenCalledOnce();
  }, 15_000);

  it("retries a deferred drain with bounded backoff without a new event, and keeps the command", async () => {
    const root = meshRoot();
    const owner = plane(root, "host:owner", { pollMs: 10_000, acknowledgementTimeoutMs: 40_000 });
    const sender = plane(root, "host:sender", { pollMs: 10_000, acknowledgementTimeoutMs: 40_000 });
    const handler = vi.fn(() => ({ accepted: true, messageId: "retried" }));
    owner.start(handler);
    sender.start(() => ({ accepted: false }));
    await new Promise((resolve) => setTimeout(resolve, 50));
    const tail = owner.mesh.tail.bind(owner.mesh);
    let deferred = 0;
    vi.spyOn(owner.mesh, "tail").mockImplementation((offset, limit) => {
      const page = tail(offset, limit);
      if (deferred === 0 && page.events.some((event) => event.topic === "fabric.control.command")) {
        deferred++;
        throw new MeshLockTimeoutError("", 3, 1);
      }
      return page;
    });
    vi.spyOn(console, "warn").mockImplementation(() => undefined);
    await expect(sender.request("host:owner", "agent:target", "steer", { message: "hi", ownerIncarnation: owner.incarnation }, "host:owner"))
      .resolves.toMatchObject({ acknowledged: true, messageId: "retried" });
    expect(deferred).toBe(1);
    expect(handler).toHaveBeenCalledOnce();
    expect(commands(sender)).toHaveLength(1); // one publication: the retry was the owner's, not a new event
  });

  it("does not lose a command published while a drain is in flight", async () => {
    const root = meshRoot();
    const owner = plane(root, "host:owner", { pollMs: 10_000, acknowledgementTimeoutMs: 40_000 });
    const sender = plane(root, "host:sender", { pollMs: 10_000, acknowledgementTimeoutMs: 40_000 });
    let release!: () => void;
    const gate = new Promise<void>((resolve) => { release = resolve; });
    const seen: string[] = [];
    owner.start(async (command) => {
      seen.push(command.targetId);
      if (command.targetId === "agent:first") await gate;
      return { accepted: true, messageId: command.targetId };
    });
    sender.start(() => ({ accepted: false }));
    await new Promise((resolve) => setTimeout(resolve, 50));
    const first = sender.request("host:owner", "agent:first", "steer", { message: "1", ownerIncarnation: owner.incarnation }, "host:owner");
    await vi.waitFor(() => expect(seen).toEqual(["agent:first"]));
    const second = sender.request("host:owner", "agent:second", "steer", { message: "2", ownerIncarnation: owner.incarnation }, "host:owner");
    await vi.waitFor(() => expect(commands(sender)).toHaveLength(2));
    await new Promise((resolve) => setTimeout(resolve, 100)); // its notification lands mid-drain
    release();
    await expect(first).resolves.toMatchObject({ messageId: "agent:first" });
    await expect(second).resolves.toMatchObject({ messageId: "agent:second" });
    expect(seen).toEqual(["agent:first", "agent:second"]);
  });

  it("settles a request to a dead owner at its real deadline without any event, then keeps no timer", async () => {
    const sender = plane(meshRoot(), "host:sender", { pollMs: 20, acknowledgementTimeoutMs: 100 });
    sender.start(() => ({ accepted: false }));
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "setInterval", "clearInterval"] });
    const outcome = sender.request("host:dead", "agent:target", "steer", { message: "hi", ownerIncarnation: "fixture:dead" }, "host:dead")
      .then(() => undefined, (error: Error) => error);
    await vi.advanceTimersByTimeAsync(0);
    expect(commands(sender)).toHaveLength(1);
    expect(vi.getTimerCount()).toBe(1); // the per-command ACK deadline, nothing else
    await vi.advanceTimersByTimeAsync(400);
    expect((await outcome)?.message).toContain("now withdrawn");
    await vi.advanceTimersByTimeAsync(0);
    expect(vi.getTimerCount()).toBe(0);
  });

  // C1: a consumption gate retains dirty work with no timer; a lease/presence change or resume reopens it.
  const recordDeletes = () => {
    const deletes: string[] = [];
    const rmSync = fs.rmSync.bind(fs);
    // Recorded, and executed only inside this test's own temporary roots: no feature deletion elsewhere.
    vi.spyOn(fs, "rmSync").mockImplementation((target, options) => {
      deletes.push(String(target));
      if (roots.some((root) => String(target).startsWith(`${root}${path.sep}`))) rmSync(target, options);
    });
    return deletes;
  };
  const insideRoots = (deletes: string[]) =>
    expect(deletes.filter((target) => !roots.some((root) => target.startsWith(`${root}${path.sep}`)))).toEqual([]);

  it("keeps a gated empty-log startup dirty with zero timers and no reads, then a lease change drains it once", async () => {
    const deletes = recordDeletes();
    const root = meshRoot();
    let canConsume = false;
    const owner = plane(root, "host:owner", { pollMs: 250, canConsumeMesh: () => canConsume });
    const tail = vi.spyOn(owner.mesh, "tail");
    const read = vi.spyOn(owner.mesh, "read");
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "setInterval", "clearInterval"] });
    owner.start(() => ({ accepted: true }));
    await vi.advanceTimersByTimeAsync(0);
    expect(vi.getTimerCount()).toBe(0);
    await vi.advanceTimersByTimeAsync(600_000);
    expect(vi.getTimerCount()).toBe(0);
    expect(tail).not.toHaveBeenCalled();
    expect(read).not.toHaveBeenCalled();
    canConsume = true;
    const other = new MeshStore(root, 64 * 1024, 1_000);
    await other.put({ key: "topology/hosts/host:peer", value: { lease: 1 }, identity: identity("host:peer") });
    await vi.waitFor(() => expect(tail).toHaveBeenCalledTimes(1));
    await new Promise((resolve) => setImmediate(resolve));
    expect(tail).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(0);
    other.closeState();
    insideRoots(deletes);
  });

  it("keeps a gated plane timer-free across pause, and resume drains the retained wake once", async () => {
    const deletes = recordDeletes();
    let canConsume = false;
    const owner = plane(meshRoot(), "host:owner", { pollMs: 250, canConsumeMesh: () => canConsume });
    const tail = vi.spyOn(owner.mesh, "tail");
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "setInterval", "clearInterval"] });
    owner.start(() => ({ accepted: true }));
    await vi.advanceTimersByTimeAsync(600_000);
    expect(vi.getTimerCount()).toBe(0);
    owner.pause();
    canConsume = true;
    owner.resume();
    await vi.advanceTimersByTimeAsync(0);
    expect(tail).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(600_000);
    expect(tail).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(0);
    insideRoots(deletes);
  });

  it("delivers a command that arrived while gated once a lease change reopens the gate, within the sender deadline", async () => {
    const deletes = recordDeletes();
    const root = meshRoot();
    let canConsume = false;
    const owner = plane(root, "host:owner", { pollMs: 10_000, acknowledgementTimeoutMs: 40_000, canConsumeMesh: () => canConsume });
    const sender = plane(root, "host:sender", { pollMs: 10_000, acknowledgementTimeoutMs: 40_000 });
    const handler = vi.fn(() => ({ accepted: true, messageId: "gated" }));
    owner.start(handler);
    sender.start(() => ({ accepted: false }));
    const outcome = sender.request("host:owner", "agent:target", "steer", { message: "hi", ownerIncarnation: owner.incarnation }, "host:owner");
    await vi.waitFor(() => expect(commands(sender)).toHaveLength(1));
    await new Promise((resolve) => setTimeout(resolve, 200));
    expect(handler).not.toHaveBeenCalled();
    canConsume = true;
    await sender.mesh.put({ key: "topology/hosts/host:peer", value: { lease: 2 }, identity: identity("host:peer") });
    await expect(outcome).resolves.toMatchObject({ acknowledged: true, messageId: "gated" });
    expect(handler).toHaveBeenCalledOnce();
    insideRoots(deletes);
  });

  it("close releases the watcher, a pending retry and a re-arm timer", async () => {
    vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const owner = plane(meshRoot(), "host:owner");
    const watch = vi.spyOn(fs, "watch");
    vi.spyOn(owner.mesh, "tail").mockImplementation(() => { throw new MeshLockTimeoutError("", 3, 1); });
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "setInterval", "clearInterval"] });
    owner.start(() => ({ accepted: true }));
    await vi.advanceTimersByTimeAsync(0);
    expect(watch).toHaveBeenCalled();
    const watchers = watch.mock.results.map((result) => result.value as fs.FSWatcher);
    const closes = watchers.map((watcher) => vi.spyOn(watcher, "close"));
    expect(vi.getTimerCount()).toBe(1); // the deferred drain's one-shot backoff
    watchers[0]!.emit("error", watchLimit()); // runtime watcher error: rescan + one re-arm timer
    await vi.advanceTimersByTimeAsync(0);
    expect(vi.getTimerCount()).toBe(2);
    await owner.close();
    expect(vi.getTimerCount()).toBe(0);
    for (const close of closes) expect(close).toHaveBeenCalled();
  });
});
