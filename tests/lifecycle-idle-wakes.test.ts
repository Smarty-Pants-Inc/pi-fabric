import { EventEmitter } from "node:events";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { LifecycleBroker } from "../src/lifecycle/broker.js";
import { FABRIC_LIFECYCLE_SUBSCRIPTION_PREFIX, FABRIC_PARTICIPANT_LIFECYCLE_TOPIC,
  type FabricLifecycleEvent, type FabricLifecycleSubscription } from "../src/lifecycle/types.js";
import { SqliteStateStore } from "../src/mesh/state-sqlite.js";
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

const setup = async (deliver: (event: FabricLifecycleEvent) => unknown, stateBackend?: "file" | "sqlite") => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-lifecycle-wakes-")); roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100, stateBackend ? { stateBackend } : {});
  restores.push(() => mesh.closeState());
  const directory = participants();
  const broker = new LifecycleBroker(mesh, target, directory, { enabled: true, pollMs: 20, maxReadEvents: 100 },
    (_subscription, event) => deliver(event) as void);
  brokers.push(broker);
  await broker.subscribe({ from: source.id, events: ["pi.agent_settled"], to: target.id, delivery: "followUp", triggerTurn: false });
  const settle = (occurredAt: number) => mesh.publish({ topic: FABRIC_PARTICIPANT_LIFECYCLE_TOPIC, kind: "pi.agent_settled",
    from: sourceIdentity, data: { version: 1, event: "pi.agent_settled", source, occurredAt } });
  return { mesh, broker, settle, directory };
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
  it("retains SQLite prefix selections across unrelated commits, with one full refresh for a feed gap", async () => {
    const fake = fakeWatch();
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-lifecycle-deltas-")); roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const foreign = await SqliteStateStore.open(meshRoot, 64 * 1024, 100, { changesRetained: 1 });
    restores.push(() => foreign.close());
    const subscription: FabricLifecycleSubscription = {
      format: 1, id: "sub", from: source.id, events: ["pi.agent_settled"], to: target.id,
      delivery: "followUp", triggerTurn: false, once: false, afterSequence: 0,
      createdAt: 1, updatedAt: 1, createdBy: target,
    };
    await foreign.writeBatch({ identity: target, ops: [
      { kind: "put", key: FABRIC_LIFECYCLE_SUBSCRIPTION_PREFIX + subscription.id, value: subscription },
      ...Array.from({ length: 512 }, (_, index) => ({ kind: "put" as const, key: `noise/${index}`, value: { index } })),
    ] });
    const mesh = new MeshStore(meshRoot, 64 * 1024, 100, { stateBackend: "sqlite" });
    restores.push(() => mesh.closeState());
    expect(mesh.stateBackend).toBe("sqlite");
    const deliver = vi.fn();
    const broker = new LifecycleBroker(mesh, target, participants(), { enabled: true, pollMs: 20, maxReadEvents: 100 }, deliver);
    brokers.push(broker);
    const scans = vi.spyOn(SqliteStateStore.prototype, "listAll");
    const exports = vi.spyOn(SqliteStateStore.prototype, "exportLive");
    const deltas = vi.spyOn(SqliteStateStore.prototype, "readDelta");
    const feeds = vi.spyOn(SqliteStateStore.prototype, "changesSince");
    const selections = vi.spyOn(mesh, "listAllShared");
    restores.push(() => { scans.mockRestore(); exports.mockRestore(); deltas.mockRestore(); feeds.mockRestore(); selections.mockRestore(); });
    broker.start();
    await flush();
    expect(selections).toHaveBeenCalledTimes(1);
    const writes = 16;
    const updates: Array<{ key: string; version: number }> = [];
    for (let index = 0; index < writes; index++) {
      updates.push(await foreign.put({ key: `noise/${index}`, value: { updated: index }, identity: target }));
      fake.root(mesh.root).callback("change", "state.db-wal");
      await flush();
    }
    // A delta-only snapshot still filters the whole sorted mesh here. Prefix proofs must
    // avoid that CPU work as well as avoiding SQL value-row reads on these unrelated wakes.
    expect(selections).toHaveBeenCalledTimes(1);
    expect(exports).toHaveBeenCalledTimes(1);
    expect(deltas).not.toHaveBeenCalled();
    expect(feeds).toHaveBeenCalledTimes(writes);
    expect(feeds.mock.results.map(result => result.value.changes)).toEqual(
      updates.map(({ key, version }) => [expect.objectContaining({ key, version, deleted: false })]),
    );
    // Miss enough commits for the real bounded feed to trim past the drain's last snapshot.
    for (let index = 0; index < 150; index++) {
      await foreign.put({ key: "noise/gap", value: index, identity: target });
    }
    fake.root(mesh.root).callback("change", "state.db-wal");
    await flush();
    // The first commit after gap recovery must be incremental again.
    await foreign.put({ key: "noise/after-gap", value: true, identity: target });
    fake.root(mesh.root).callback("change", "state.db-wal");
    await flush();
    expect(scans).not.toHaveBeenCalled();
    expect(exports).toHaveBeenCalledTimes(2); // startup and the one actual feed gap only
    expect(selections).toHaveBeenCalledTimes(2); // startup and gap; not the unrelated post-gap wake
    expect(deltas).toHaveBeenCalledTimes(1);
    expect(deltas.mock.results[0]!.value).toMatchObject({ complete: false, changed: [] });
    expect(feeds).toHaveBeenCalledTimes(writes + 2);
    const steps = feeds.mock.results.map(result => result.value as ReturnType<SqliteStateStore["changesSince"]>);
    expect(steps.filter(step => !step.complete)).toHaveLength(1);
    expect(steps.filter(step => step.complete).map(step => step.changes.length)).toEqual(Array(writes + 1).fill(1));
    expect(deliver).not.toHaveBeenCalled();
    expect(mesh.listAllShared(FABRIC_LIFECYCLE_SUBSCRIPTION_PREFIX, { snapshot: mesh.stateToken() }))
      .toMatchObject([{ value: subscription }]);
  });

  it("SQLite: delivers new event-only wakes even when the subscription snapshot is unchanged", async () => {
    const fake = fakeWatch();
    const delivered: FabricLifecycleEvent[] = [];
    const { mesh, broker, settle } = await setup(event => { delivered.push(event); }, "sqlite");
    const selections = vi.spyOn(mesh, "listAllShared");
    restores.push(() => selections.mockRestore());
    broker.start();
    await flush();
    const initialSelections = selections.mock.calls.length;
    for (const occurredAt of [11, 12]) {
      const snapshot = mesh.stateToken();
      await settle(occurredAt);
      expect(mesh.stateToken()).toBe(snapshot); // no state/subscription change can select this work
      expect(delivered).toHaveLength(occurredAt - 11);
      fake.root(mesh.root).callback("change", "events.jsonl");
      await flush();
      expect(delivered.map(event => event.occurredAt)).toEqual(occurredAt === 11 ? [11] : [11, 12]);
      expect(mesh.get(FABRIC_LIFECYCLE_SUBSCRIPTION_PREFIX + broker.list()[0]!.id)?.value)
        .toMatchObject({ afterSequence: mesh.latestSequence(), lastEventId: delivered.at(-1)!.id });
      // The first event replays the retained selection; the second refreshes only because
      // the preceding delivery changed its cursor, not because an event-only wake needs it.
      expect(selections).toHaveBeenCalledTimes(initialSelections + occurredAt - 11);
    }
  });

  it("SQLite: sees relevant-prefix filter updates, deletes and recreation after retaining a selection", async () => {
    const fake = fakeWatch();
    const delivered: FabricLifecycleEvent[] = [];
    const { mesh, broker, settle } = await setup(event => { delivered.push(event); }, "sqlite");
    const initial = broker.list()[0]!;
    const key = FABRIC_LIFECYCLE_SUBSCRIPTION_PREFIX + initial.id;
    const foreign = await SqliteStateStore.open(mesh.root, 64 * 1024, 100);
    restores.push(() => foreign.close());
    const selections = vi.spyOn(mesh, "listAllShared");
    restores.push(() => selections.mockRestore());
    broker.start();
    await flush();
    const initialSelections = selections.mock.calls.length;
    await foreign.put({ key, value: { ...initial, events: ["pi.agent_start"] }, identity: target });
    await settle(21);
    fake.root(mesh.root).callback("change", "state.db-wal");
    await flush();
    expect(delivered).toEqual([]);
    expect(selections).toHaveBeenCalledTimes(initialSelections + 1);
    await foreign.delete({ key });
    await settle(22);
    fake.root(mesh.root).callback("change", "state.db-wal");
    await flush();
    expect(delivered).toEqual([]);
    expect(selections).toHaveBeenCalledTimes(initialSelections + 2);
    await foreign.put({ key, value: { ...initial, afterSequence: mesh.latestSequence() }, identity: target });
    await settle(23);
    fake.root(mesh.root).callback("change", "state.db-wal");
    await flush();
    expect(delivered.map(event => event.occurredAt)).toEqual([23]);
    expect(selections).toHaveBeenCalledTimes(initialSelections + 3);
  });

  it("SQLite: never advances a retained witness past a concurrent write after the proof", async () => {
    const fake = fakeWatch();
    const delivered: FabricLifecycleEvent[] = [];
    const { mesh, broker, settle } = await setup(event => { delivered.push(event); }, "sqlite");
    const initial = broker.list()[0]!;
    const key = FABRIC_LIFECYCLE_SUBSCRIPTION_PREFIX + initial.id;
    const foreign = await SqliteStateStore.open(mesh.root, 64 * 1024, 100);
    restores.push(() => foreign.close());
    broker.start();
    await flush();
    const captured = mesh.stateRevision()!;
    const prove = mesh.stateUnchangedSince.bind(mesh);
    let concurrent: Promise<unknown> | undefined;
    const proofs = vi.spyOn(mesh, "stateUnchangedSince").mockImplementationOnce((revision, prefixes) => {
      const unchanged = prove(revision, prefixes);
      expect(unchanged).toBe(true);
      // The native uncontended writer commits synchronously before its promise resolves:
      // this is a real commit strictly after proof, before a buggy post-proof stamp read.
      concurrent = foreign.put({ key, value: { ...initial, events: ["pi.agent_start"] }, identity: target });
      expect(foreign.stateStamp()).not.toBe(captured);
      return unchanged;
    });
    restores.push(() => proofs.mockRestore());
    fake.root(mesh.root).callback("change", "state.db-wal");
    await flush();
    expect(concurrent).toBeDefined();
    await concurrent;
    await settle(24);
    fake.root(mesh.root).callback("change", "events.jsonl");
    await flush();
    expect(proofs.mock.calls[1]?.[0]).toBe(captured);
    expect(delivered).toEqual([]); // the concurrent filter update was not skipped
    await foreign.put({ key, value: initial, identity: target });
    await settle(25);
    fake.root(mesh.root).callback("change", "state.db-wal");
    await flush();
    expect(delivered.map(event => event.occurredAt)).toEqual([25]);
  });

  it.each(["proof", "selection"] as const)("SQLite: keeps the previous witness/entries pair when %s throws and retries", async failure => {
    const fake = fakeWatch();
    const delivered: FabricLifecycleEvent[] = [];
    const { mesh, broker, settle } = await setup(event => { delivered.push(event); }, "sqlite");
    const initial = broker.list()[0]!;
    const key = FABRIC_LIFECYCLE_SUBSCRIPTION_PREFIX + initial.id;
    const foreign = await SqliteStateStore.open(mesh.root, 64 * 1024, 100);
    restores.push(() => foreign.close());
    broker.start();
    await flush();
    const previous = mesh.stateRevision()!;
    const proofs = vi.spyOn(mesh, "stateUnchangedSince");
    const selections = vi.spyOn(mesh, "listAllShared");
    restores.push(() => { proofs.mockRestore(); selections.mockRestore(); });
    const fail = () => { throw new Error("transient selection read failed"); };
    if (failure === "proof") proofs.mockImplementationOnce(fail);
    else selections.mockImplementationOnce(fail);
    await foreign.put({ key, value: { ...initial, events: ["pi.agent_start"] }, identity: target });
    fake.root(mesh.root).callback("change", "state.db-wal");
    await flush();
    // No more notification: the existing bounded failed-drain retry must finish this read.
    await vi.waitFor(() => expect(selections).toHaveBeenCalledTimes(failure === "proof" ? 1 : 2), { timeout: 2_000, interval: 10 });
    expect(proofs.mock.calls.slice(0, 2).map(([revision]) => revision)).toEqual([previous, previous]);
    await settle(26);
    fake.root(mesh.root).callback("change", "events.jsonl");
    await flush();
    expect(delivered).toEqual([]); // no stale old-filter replay after the failed refresh
    const started = await mesh.publish({ topic: FABRIC_PARTICIPANT_LIFECYCLE_TOPIC, kind: "pi.agent_start",
      from: sourceIdentity, data: { version: 1, event: "pi.agent_start", source, occurredAt: 27 } });
    fake.root(mesh.root).callback("change", "events.jsonl");
    await flush();
    expect(delivered.map(event => event.id)).toEqual([started.id]);
  });

  it("SQLite: replays unchanged subscriptions after target movement and a gated wake", async () => {
    const fake = fakeWatch();
    const delivered: FabricLifecycleEvent[] = [];
    const { mesh, broker, settle, directory } = await setup(event => { delivered.push(event); }, "sqlite");
    const targetRecord = directory.get(target.id)!;
    targetRecord.local = false;
    directory.publishes = id => directory.get(id)?.local === true;
    broker.start();
    await flush();
    await settle(13);
    fake.root(mesh.root).callback("change", "events.jsonl");
    await flush();
    expect(delivered).toEqual([]);
    const snapshot = mesh.stateToken();
    targetRecord.local = true; // host/lease ownership can move without a subscription change
    fake.root(mesh.root).callback("change", "state.db-wal");
    await flush();
    expect(delivered.map(event => event.occurredAt)).toEqual([13]);
    expect(snapshot).not.toBe(mesh.stateToken()); // only the delivery's cursor changed state
    broker.pause();
    const pausedSnapshot = mesh.stateToken();
    await settle(14);
    fake.root(mesh.root).callback("change", "events.jsonl");
    await flush();
    expect(delivered.map(event => event.occurredAt)).toEqual([13]);
    expect(mesh.stateToken()).toBe(pausedSnapshot);
    broker.resume();
    await flush();
    expect(delivered.map(event => event.occurredAt)).toEqual([13, 14]);
  });

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

  it("exhausted lifecycle watches still deliver external events on the minute tick, then stop scans after recovery", async () => {
    const fake = fakeWatch();
    const healthyWatch = fake.spy.getMockImplementation()!;
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    restores.push(() => warn.mockRestore());
    fake.spy.mockImplementation(() => { throw Object.assign(new Error("ENOSPC: watcher limit"), { code: "ENOSPC" }); });
    const delivered: FabricLifecycleEvent[] = [];
    const { mesh, broker, settle } = await setup((event) => { delivered.push(event); });
    const reads = vi.spyOn(mesh, "listAllShared");
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "setInterval", "clearInterval"] });
    broker.start();
    await vi.advanceTimersByTimeAsync(63_000);
    await flush();
    expect(broker.observationHealth()).toEqual(["observation-degraded:lifecycle"]);
    expect(vi.getTimerCount()).toBe(1);
    await settle(8); // another writer's only durable event, no local publish wake
    await vi.advanceTimersByTimeAsync(59_999);
    await flush();
    expect(delivered).toEqual([]);
    await vi.advanceTimersByTimeAsync(1);
    await flush();
    expect(delivered.map((event) => event.occurredAt)).toEqual([8]);
    expect(warn).toHaveBeenCalledTimes(2);
    fake.spy.mockImplementation(healthyWatch);
    await settle(9);
    await vi.advanceTimersByTimeAsync(60_000);
    await flush();
    expect(delivered.map((event) => event.occurredAt)).toEqual([8, 9]);
    expect(broker.observationHealth()).toEqual([]);
    expect(vi.getTimerCount()).toBe(0);
    expect(warn).toHaveBeenCalledTimes(3);
    const recoveredReads = reads.mock.calls.length;
    await vi.advanceTimersByTimeAsync(300_000);
    await flush();
    expect(reads).toHaveBeenCalledTimes(recoveredReads);
    await settle(10);
    fake.root(mesh.root).callback("change", "events.jsonl");
    await flush();
    expect(delivered.map((event) => event.occurredAt)).toEqual([8, 9, 10]);
    await broker.close();
    expect(vi.getTimerCount()).toBe(0);
    // Supported same-instance start must reopen the carrier, not inherit a
    // closed helper that silently abandons a second exhaustion episode.
    fake.spy.mockImplementation(() => { throw Object.assign(new Error("ENOSPC: watcher limit"), { code: "ENOSPC" }); });
    broker.start();
    await vi.advanceTimersByTimeAsync(63_000);
    await flush();
    expect(broker.observationHealth()).toEqual(["observation-degraded:lifecycle"]);
    expect(vi.getTimerCount()).toBe(1);
    await broker.close(); // close while degraded disposes the carrier too
    const closedReads = reads.mock.calls.length;
    await settle(11);
    await vi.advanceTimersByTimeAsync(300_000);
    await flush();
    expect(reads).toHaveBeenCalledTimes(closedReads);
    expect(delivered.map((event) => event.occurredAt)).toEqual([8, 9, 10]);
    expect(broker.observationHealth()).toEqual([]);
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each(["file", "sqlite"] as const)("retries a failed delivery on a one-shot backoff without another notification (%s)", async stateBackend => {
    fakeWatch();
    let failures = 1;
    const delivered: FabricLifecycleEvent[] = [];
    const { broker, settle } = await setup((event) => {
      if (failures-- > 0) throw new Error("target busy");
      delivered.push(event);
    }, stateBackend);
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
