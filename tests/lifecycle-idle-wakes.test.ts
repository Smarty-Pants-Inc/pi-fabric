import { EventEmitter } from "node:events";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { LifecycleBroker } from "../src/lifecycle/broker.js";
import { FABRIC_PARTICIPANT_LIFECYCLE_TOPIC, type FabricLifecycleEvent } from "../src/lifecycle/types.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import type { FabricParticipantInfo, FabricParticipantSource } from "../src/topology/types.js";

// pi-fabric#7978: the broker drained on a pollMs interval forever. It now drains at start, on
// mesh directory notifications, and on one-shot retries for failed work only.

const roots: string[] = [];
const brokers: LifecycleBroker[] = [];
const restores: Array<() => void> = [];

const target: MeshIdentity = { id: "session:target", name: "main", kind: "main", sessionId: "target" };
const sourceIdentity: MeshIdentity = { id: "session:source", name: "Peer source", kind: "main", sessionId: "source" };
const record = (identity: MeshIdentity, local: boolean): FabricParticipantInfo => ({
  format: 1, id: identity.id, kind: "root", rootId: identity.id, ownerHostId: identity.id,
  ownerIdentityId: identity.id, name: identity.name, status: "idle", runner: "pi", transport: "host",
  capabilities: ["steer", "followUp", "fabric"], startedAt: 1, updatedAt: 1, controlProtocol: "v1", local, stale: false,
});
const participants = (): FabricParticipantSource => {
  const records = [record(target, true), record(sourceIdentity, false)];
  return {
    list: () => records, get: (id) => records.find((entry) => entry.id === id), self: () => records[0]!,
    peers: () => [], async refresh() {}, scheduleRefresh() {},
  };
};
const source = {
  id: sourceIdentity.id, name: sourceIdentity.name, kind: "root" as const, rootId: sourceIdentity.id,
  runner: "pi" as const, ownerHostId: sourceIdentity.id, ownerIdentityId: sourceIdentity.id,
};

interface FakeWatcher { directory: string; callback: (event: string, name: string | null) => void; watcher: EventEmitter & { close: () => void } }
/** Replaces fs.watch so only the test (not real filesystem notifications) can wake the broker. */
const fakeWatch = () => {
  const watchers: FakeWatcher[] = [];
  const spy = vi.spyOn(fs, "watch").mockImplementation(((directory: string, callback: FakeWatcher["callback"]) => {
    const watcher = Object.assign(new EventEmitter(), { close: vi.fn() });
    watchers.push({ directory, callback, watcher });
    return watcher as unknown as fs.FSWatcher;
  }) as unknown as typeof fs.watch);
  restores.push(() => spy.mockRestore());
  return { watchers, spy, root: (meshRoot: string) => watchers.filter((entry) => entry.directory === meshRoot).at(-1)! };
};

const setup = async (deliver: (event: FabricLifecycleEvent) => unknown) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-lifecycle-wakes-")); roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const broker = new LifecycleBroker(mesh, target, participants(), { enabled: true, pollMs: 20, maxReadEvents: 100 },
    (_subscription, event) => deliver(event) as void);
  brokers.push(broker);
  await broker.subscribe({ from: source.id, events: ["pi.agent_settled"], to: target.id, delivery: "followUp", triggerTurn: false });
  const settle = (occurredAt: number) => mesh.publish({ topic: FABRIC_PARTICIPANT_LIFECYCLE_TOPIC, kind: "pi.agent_settled",
    from: sourceIdentity, data: { version: 1, event: "pi.agent_settled", source, occurredAt } });
  return { mesh, broker, settle };
};

const flush = async () => { for (let index = 0; index < 50; index++) await new Promise((resolve) => setImmediate(resolve)); };
const realDelay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

afterEach(async () => {
  vi.useRealTimers();
  await Promise.all(brokers.splice(0).map((broker) => broker.close()));
  for (const restore of restores.splice(0)) restore();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("LifecycleBroker wakes", () => {
  it("drains at start and then holds no recurring timer while idle", async () => {
    fakeWatch();
    const delivered: FabricLifecycleEvent[] = [];
    const { broker, settle } = await setup((event) => { delivered.push(event); });
    await settle(1);
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "setInterval", "clearInterval"] });
    broker.start();
    await flush();
    expect(delivered.map((event) => event.occurredAt)).toEqual([1]);   // the immediate drain
    expect(vi.getTimerCount()).toBe(0);
    vi.advanceTimersByTime(60_000);
    await flush();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("drains on a mesh directory notification", async () => {
    const fake = fakeWatch();
    const delivered: FabricLifecycleEvent[] = [];
    const { mesh, broker, settle } = await setup((event) => { delivered.push(event); });
    broker.start();
    await flush();
    await settle(2);
    await realDelay(60);
    expect(delivered).toEqual([]);                                     // nothing polls
    fake.root(mesh.root).callback("change", "events.jsonl");
    await vi.waitFor(() => expect(delivered.map((event) => event.occurredAt)).toEqual([2]), { timeout: 2_000, interval: 10 });
  });

  it("answers a watcher error with one rescan and a one-shot re-arm, not a poll", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    restores.push(() => warn.mockRestore());
    const fake = fakeWatch();
    const delivered: FabricLifecycleEvent[] = [];
    const { mesh, broker, settle } = await setup((event) => { delivered.push(event); });
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "setInterval", "clearInterval"] });
    broker.start();
    await flush();
    const armed = fake.spy.mock.calls.length;
    await settle(3);                                                   // its notification is lost
    fake.root(mesh.root).watcher.emit("error", Object.assign(new Error("watch broke"), { code: "EIO" }));
    await flush();
    expect(delivered.map((event) => event.occurredAt)).toEqual([3]);   // the one rescan
    expect(warn).toHaveBeenCalledOnce();
    expect(String(warn.mock.calls[0]?.[0])).toMatch(/rescanned once, re-arming in 1000 ms/);
    expect(String(warn.mock.calls[0]?.[0])).not.toMatch(/falling back to polling|no events or writes are lost/i);
    expect(vi.getTimerCount()).toBe(1);                                // the re-arm, only
    vi.advanceTimersByTime(1_000);
    await flush();
    expect(fake.spy.mock.calls.length).toBeGreaterThan(armed);         // re-armed
    expect(vi.getTimerCount()).toBe(0);
    await settle(4);
    fake.root(mesh.root).callback("change", "events.jsonl");           // the new watcher wakes it
    await flush();
    expect(delivered.map((event) => event.occurredAt)).toEqual([3, 4]);
  });

  it("retries a failed delivery on a one-shot backoff without another notification", async () => {
    fakeWatch();
    let failures = 1;
    const delivered: FabricLifecycleEvent[] = [];
    const { broker, settle } = await setup((event) => {
      if (failures-- > 0) throw new Error("target busy");
      delivered.push(event);
    });
    await settle(5);
    broker.start();
    await vi.waitFor(() => expect(delivered.map((event) => event.occurredAt)).toEqual([5]), { timeout: 2_000, interval: 10 });
    expect(broker.list()[0]?.lastError).toBeUndefined();
  });

  it("does not strand an event that arrives while a drain runs", async () => {
    const fake = fakeWatch();
    const delivered: FabricLifecycleEvent[] = [];
    let mesh!: MeshStore;
    let settleLater!: (occurredAt: number) => Promise<unknown>;
    const { broker, settle, mesh: store } = await setup(async (event) => {
      delivered.push(event);
      if (event.occurredAt === 6) {
        await settleLater(7);                                          // published mid-drain ...
        fake.root(mesh.root).callback("change", "events.jsonl");       // ... with its notification
      }
    });
    mesh = store; settleLater = settle;
    await settle(6);
    broker.start();
    await vi.waitFor(() => expect(delivered.map((event) => event.occurredAt)).toEqual([6, 7]), { timeout: 2_000, interval: 10 });
    await realDelay(60);
    expect(delivered).toHaveLength(2);
  });
});
