import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { LifecycleBroker } from "../src/lifecycle/broker.js";
import {
  FABRIC_PARTICIPANT_LIFECYCLE_TOPIC,
  type FabricLifecycleEvent,
  type FabricLifecycleSubscription,
} from "../src/lifecycle/types.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { removeHostLease } from "../src/topology/host-leases.js";
import type {
  FabricParticipantInfo,
  FabricParticipantSource,
} from "../src/topology/types.js";

const roots: string[] = [];
const brokers: LifecycleBroker[] = [];

const targetIdentity: MeshIdentity = {
  id: "session:target",
  name: "main",
  kind: "main",
  sessionId: "target",
};

const sourceIdentity: MeshIdentity = {
  id: "session:source",
  name: "Peer source",
  kind: "main",
  sessionId: "source",
};

const participant = (
  identity: MeshIdentity,
  local: boolean,
): FabricParticipantInfo => ({
  format: 1,
  id: identity.id,
  kind: identity.kind === "main" ? "root" : identity.kind,
  rootId: identity.id,
  ownerHostId: identity.id,
  ownerIdentityId: identity.id,
  name: identity.name,
  status: "idle",
  runner: "pi",
  transport: "host",
  capabilities: ["steer", "followUp", "fabric"],
  ...(identity.sessionId ? { sessionId: identity.sessionId } : {}),
  startedAt: 1,
  updatedAt: 1,
  controlProtocol: "v1",
  local,
  stale: false,
});

const participants = (localId: string): FabricParticipantSource => {
  const records = [
    participant(targetIdentity, targetIdentity.id === localId),
    participant(sourceIdentity, sourceIdentity.id === localId),
  ];
  return {
    list: () => records,
    get: (id) => records.find((record) => record.id === id),
    self: () => records[0]!,
    peers: () => [],
    async refresh() {},
    scheduleRefresh() {},
  };
};

const source = {
  id: sourceIdentity.id,
  name: sourceIdentity.name,
  kind: "root" as const,
  rootId: sourceIdentity.id,
  runner: "pi" as const,
  ownerHostId: sourceIdentity.id,
  ownerIdentityId: sourceIdentity.id,
};

const waitFor = async (predicate: () => boolean, timeoutMs = 2_000): Promise<void> => {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error("Timed out waiting for lifecycle delivery");
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
};

afterEach(async () => {
  await Promise.all(brokers.splice(0).map((broker) => broker.close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("LifecycleBroker", () => {
  // smarty-dev#557: every host read the whole directory for every subscription on every poll,
  // about a quarter of a core per idle Pi on the fleet mesh.
  it("reads the directory only for subscriptions behind the log whose target this host publishes", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-lifecycle-"));
    roots.push(root);
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const here = new ParticipantDirectory(mesh, {
      enabled: true, hostId: targetIdentity.id, rootId: targetIdentity.id, identity: targetIdentity,
      heartbeatMs: 60_000, leaseMs: 60_000,
    });
    here.registerSource(() => [here.root({
      id: targetIdentity.id, name: "Main", kind: "main", status: "idle", runner: "pi", transport: "host",
      updatedAt: 1, pendingMessages: false, local: true,
    })]);
    expect(here.publishes(targetIdentity.id)).toBe(false);          // nothing published yet
    await here.refresh();
    expect([here.publishes(targetIdentity.id), here.publishes("main"), here.publishes(sourceIdentity.id)])
      .toEqual([true, true, false]);
    const subscribe = (id: string, to: string, afterSequence: number) => mesh.put({
      key: `topology/subscriptions/${id}`, identity: targetIdentity, value: {
        format: 1, id, from: source.id, events: ["pi.agent_settled"], to, delivery: "followUp",
        triggerTurn: false, once: false, afterSequence, createdAt: 1, updatedAt: 1, createdBy: targetIdentity,
      } satisfies FabricLifecycleSubscription,
    });
    await mesh.publish({ topic: "unrelated", from: sourceIdentity, text: "one" });
    await subscribe("elsewhere", sourceIdentity.id, 0);            // behind, but another host's target
    await subscribe("here", targetIdentity.id, mesh.latestSequence()); // this host's, caught up
    const gets = vi.spyOn(here, "get");
    const lists = vi.spyOn(here, "list");
    const broker = new LifecycleBroker(mesh, targetIdentity, here,
      { enabled: true, pollMs: 20, maxReadEvents: 100 }, () => {});
    brokers.push(broker);
    broker.start();
    await new Promise((resolve) => setTimeout(resolve, 200));      // about ten polls
    expect(gets).not.toHaveBeenCalled();
    expect(lists).not.toHaveBeenCalled();
    // A new event puts this host's subscription behind: it is read and drained, the other is not.
    const reads = vi.spyOn(mesh, "read");
    await mesh.publish({ topic: "unrelated", from: sourceIdentity, text: "two" });
    await waitFor(() => reads.mock.calls.length > 0);
    expect(new Set(gets.mock.calls.map(([id]) => id))).toEqual(new Set([targetIdentity.id]));
    expect((mesh.get("topology/subscriptions/elsewhere")?.value as FabricLifecycleSubscription).afterSequence)
      .toBe(0);
  });

  it("delivers only new matching source events and removes one-shot subscriptions", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-lifecycle-"));
    roots.push(root);
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const directory = participants(targetIdentity.id);
    const deliveries: Array<{
      subscription: FabricLifecycleSubscription;
      event: FabricLifecycleEvent;
    }> = [];
    const target = new LifecycleBroker(
      mesh,
      targetIdentity,
      directory,
      { enabled: true, pollMs: 20, maxReadEvents: 100 },
      (subscription, event) => {
        deliveries.push({ subscription, event });
      },
    );
    const publisher = new LifecycleBroker(
      mesh,
      sourceIdentity,
      participants(sourceIdentity.id),
      { enabled: true, pollMs: 20, maxReadEvents: 100 },
      () => {},
    );
    brokers.push(target, publisher);

    await mesh.publish({
      topic: FABRIC_PARTICIPANT_LIFECYCLE_TOPIC,
      kind: "pi.agent_settled",
      from: sourceIdentity,
      data: {
        version: 1,
        event: "pi.agent_settled",
        source,
        occurredAt: 1,
      },
    });
    const subscription = await target.subscribe({
      from: source.id,
      events: ["pi.agent_settled"],
      to: targetIdentity.id,
      delivery: "followUp",
      triggerTurn: false,
      once: true,
    });
    target.start();
    await new Promise((resolve) => setTimeout(resolve, 60));
    expect(deliveries).toEqual([]);

    await publisher.publish({ source, event: "pi.turn_end", data: { turnIndex: 1 } });
    await publisher.publish({
      source: { ...source, ownerHostId: "host:forged" },
      event: "pi.agent_settled",
    });
    await new Promise((resolve) => setTimeout(resolve, 60));
    expect(deliveries).toEqual([]);

    await publisher.publish({
      source,
      event: "pi.agent_settled",
      occurredAt: 42,
      data: { privateTranscript: undefined, idle: true },
    });
    await waitFor(() => deliveries.length === 1);

    expect(deliveries[0]).toMatchObject({
      subscription: {
        id: subscription.id,
        from: source.id,
        to: targetIdentity.id,
        triggerTurn: false,
      },
      event: {
        event: "pi.agent_settled",
        source: { id: source.id, kind: "root" },
        occurredAt: 42,
        data: { idle: true },
      },
    });
    await waitFor(() => target.list().length === 0);
  });

  // smarty-dev#557: the target's host saved the cursor after every new mesh event, a whole
  // shared-state write each, although almost none of those events matched.
  it("saves a cursor for a delivery or a decided skip, and otherwise only once a page ahead", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-lifecycle-"));
    roots.push(root);
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const delivered: FabricLifecycleEvent[] = [];
    const directory = participants(targetIdentity.id);
    const target = new LifecycleBroker(mesh, targetIdentity, directory,
      { enabled: true, pollMs: 20, maxReadEvents: 5 }, (_subscription, event) => { delivered.push(event); });
    const publisher = new LifecycleBroker(mesh, sourceIdentity, participants(sourceIdentity.id),
      { enabled: true, pollMs: 60_000, maxReadEvents: 5 }, () => {});
    brokers.push(target, publisher);
    const { id } = await target.subscribe({
      from: source.id, events: ["pi.agent_settled"], to: targetIdentity.id,
      delivery: "followUp", triggerTurn: false, once: false,
    });
    const saved = () => mesh.get(`topology/subscriptions/${id}`)!;
    const cursor = () => (saved().value as FabricLifecycleSubscription).afterSequence;
    const unrelated = () => mesh.publish({ topic: "unrelated", from: sourceIdentity, text: "noise" });
    const version = saved().version;
    const reads = vi.spyOn(mesh, "read");
    target.start();
    for (let index = 0; index < 3; index++) await unrelated();
    await waitFor(() => reads.mock.calls.length > 0);
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(saved().version).toBe(version);                         // passed unmatched events: not saved
    const gets = vi.spyOn(directory, "get");
    const settled = reads.mock.calls.length;
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(reads.mock.calls.length).toBe(settled);                 // caught up in memory: no more reads,
    expect(gets).not.toHaveBeenCalled();                           // and no directory reads either
    await unrelated();
    await unrelated();                                             // five unsaved: a page ahead
    await waitFor(() => cursor() === mesh.latestSequence());
    await publisher.publish({ source, event: "pi.agent_settled" });
    await waitFor(() => delivered.length === 1);
    await waitFor(() => cursor() === mesh.latestSequence());       // a delivery: saved at once
    expect((saved().value as FabricLifecycleSubscription).lastEventId).toBe(delivered[0]!.id);
    await publisher.publish({ source: { ...source, ownerHostId: "host:forged" }, event: "pi.agent_settled" });
    await waitFor(() => cursor() === mesh.latestSequence());       // skipped for good: saved at once
    expect(delivered).toHaveLength(1);
  });

  it("scans unsaved events again after a restart without delivering anything twice", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-lifecycle-"));
    roots.push(root);
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const delivered: string[] = [];
    const broker = () => {
      const value = new LifecycleBroker(mesh, targetIdentity, participants(targetIdentity.id),
        { enabled: true, pollMs: 20, maxReadEvents: 100 }, (_subscription, event) => { delivered.push(event.id); });
      brokers.push(value);
      return value;
    };
    const publisher = new LifecycleBroker(mesh, sourceIdentity, participants(sourceIdentity.id),
      { enabled: true, pollMs: 60_000, maxReadEvents: 100 }, () => {});
    brokers.push(publisher);
    const first = broker();
    const { id } = await first.subscribe({
      from: source.id, events: ["pi.agent_settled"], to: targetIdentity.id,
      delivery: "followUp", triggerTurn: false, once: false,
    });
    first.start();
    await publisher.publish({ source, event: "pi.agent_settled" });
    await waitFor(() => delivered.length === 1);
    await publisher.publish({ source, event: "pi.turn_end" });   // not subscribed: stays unsaved
    await mesh.publish({ topic: "unrelated", from: sourceIdentity, text: "noise" });
    await new Promise((resolve) => setTimeout(resolve, 100));
    await first.close();
    const savedCursor = (mesh.get(`topology/subscriptions/${id}`)!.value as FabricLifecycleSubscription).afterSequence;
    expect(savedCursor).toBeLessThan(mesh.latestSequence());
    const second = broker();
    second.start();
    await new Promise((resolve) => setTimeout(resolve, 150));
    expect(delivered).toHaveLength(1);
    await publisher.publish({ source, event: "pi.agent_settled" });
    await waitFor(() => delivered.length === 2);
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(new Set(delivered).size).toBe(2);
  });

  // review/astra on #49: with the runtime read cache, a publisher's cached listing must not
  // hide a subscription another host created a moment ago; the event would be lost for good.
  it("delivers an event emitted right after another host subscribes, despite the read cache", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-lifecycle-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    // Two hosts: separate stores on one mesh root; the publisher's store caches reads for 60 s.
    const targetMesh = new MeshStore(meshRoot, 64 * 1024, 100);
    const publisherMesh = new MeshStore(meshRoot, 64 * 1024, 100, { readCacheMs: 60_000 });
    const deliveries: FabricLifecycleEvent[] = [];
    const target = new LifecycleBroker(targetMesh, targetIdentity, participants(targetIdentity.id),
      { enabled: true, pollMs: 20, maxReadEvents: 100 }, (_subscription, event) => { deliveries.push(event); });
    // A long poll interval: nothing but the fresh read can refresh the publisher's cached view.
    const publisher = new LifecycleBroker(publisherMesh, sourceIdentity, participants(sourceIdentity.id),
      { enabled: true, pollMs: 60_000, maxReadEvents: 100 }, () => {});
    brokers.push(target, publisher);
    target.start();
    await targetMesh.put({ key: "unrelated/key", value: 1, identity: targetIdentity });   // the state file exists
    // The publisher reads (and caches) the state while nobody subscribes.
    await publisher.publish({ source, event: "pi.agent_settled", occurredAt: 1 });
    expect(publisherMesh.listAll("topology/subscriptions/")).toEqual([]);
    await target.subscribe({
      from: source.id, events: ["pi.agent_settled"], to: targetIdentity.id,
      delivery: "followUp", triggerTurn: false, once: false,
    });
    expect(publisherMesh.listAll("topology/subscriptions/")).toEqual([]);   // its cached view is stale
    await publisher.publish({ source, event: "pi.agent_settled", occurredAt: 2 });
    await waitFor(() => deliveries.length === 1);
    expect(deliveries[0]).toMatchObject({ event: "pi.agent_settled", occurredAt: 2 });
  });

  // review/astra on #49: the receiving side skips an event whose source is not the current
  // owner, for good. A cached view where the source's lease had lapsed must not decide that.
  it("delivers an event from a source whose lease was renewed after the receiver's cached read", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-lifecycle-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const targetMesh = new MeshStore(meshRoot, 64 * 1024, 100, { readCacheMs: 60_000 });
    const sourceMesh = new MeshStore(meshRoot, 64 * 1024, 100);
    const directory = (mesh: MeshStore, identity: MeshIdentity, leaseMs: number) => {
      const value = new ParticipantDirectory(mesh, {
        enabled: true, hostId: identity.id, rootId: identity.id, identity: { ...identity, kind: "actor" },
        heartbeatMs: 100, leaseMs,
      });
      value.registerSource(() => [{
        format: 1, id: identity.id, kind: "root", rootId: identity.id, ownerHostId: identity.id,
        ownerIdentityId: identity.id, name: identity.name, status: "idle", runner: "pi", transport: "host",
        capabilities: ["steer", "followUp", "fabric"], startedAt: 1, updatedAt: 2, pendingMessages: false,
        controlProtocol: "v1",
      }]);
      return value;
    };
    const sourceDirectory = directory(sourceMesh, sourceIdentity, 1_000);
    const targetDirectory = directory(targetMesh, targetIdentity, 60_000);   // the receiver stays live
    await sourceDirectory.refresh();                           // the source's lease: 1 s
    await targetDirectory.refresh();
    const deliveries: FabricLifecycleEvent[] = [];
    const target = new LifecycleBroker(targetMesh, targetIdentity, targetDirectory,
      { enabled: true, pollMs: 20, maxReadEvents: 100 }, (_subscription, event) => { deliveries.push(event); });
    const publisher = new LifecycleBroker(sourceMesh, sourceIdentity, sourceDirectory,
      { enabled: true, pollMs: 60_000, maxReadEvents: 100 }, () => {});
    brokers.push(target, publisher);
    await target.subscribe({
      from: source.id, events: ["pi.agent_settled"], to: targetIdentity.id,
      delivery: "followUp", triggerTurn: false, once: false,
    });
    await new Promise((resolve) => setTimeout(resolve, 1_100));  // the cached lease has lapsed
    expect(targetDirectory.get(source.id)).toBeUndefined();   // the receiver's cached view
    await sourceDirectory.refresh();                           // the source renews ...
    // ... only in the shared state, as a runtime before host lease files does: a file lease is
    // read fresh, and would already show the renewal (smarty-dev#816).
    removeHostLease(meshRoot, source.id);
    await publisher.publish({ source, event: "pi.agent_settled", occurredAt: 7 });   // ... and publishes
    expect(targetDirectory.get(source.id)).toBeUndefined();   // still the stale cached view
    target.start();
    await waitFor(() => deliveries.length === 1);
    expect(deliveries[0]).toMatchObject({ event: "pi.agent_settled", occurredAt: 7 });
  });

  // review/astra on #49, F2: after a handoff inside the cache window, an event from the former
  // owner must not be delivered (it would end a once subscription before the new owner's event).
  it("skips an event from a former owner after a handoff, and delivers the new owner's", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-lifecycle-"));
    roots.push(root);
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const former = participant(sourceIdentity, false);
    const current = { ...former, ownerHostId: "host:new", ownerIdentityId: "host:new" };
    const target = participant(targetIdentity, true);
    // The cached view still names the former owner; the current mesh state names the new one.
    const handedOff: FabricParticipantSource = {
      list: () => [target, current],
      get: (id, _now, options) => id === target.id ? target : id === source.id ? (options?.fresh ? current : former) : undefined,
      self: () => target,
      peers: () => [],
      async refresh() {},
      scheduleRefresh() {},
    };
    const deliveries: FabricLifecycleEvent[] = [];
    const receiver = new LifecycleBroker(mesh, targetIdentity, handedOff,
      { enabled: true, pollMs: 20, maxReadEvents: 100 }, (_subscription, event) => { deliveries.push(event); });
    const publisher = new LifecycleBroker(mesh, sourceIdentity, participants(sourceIdentity.id),
      { enabled: true, pollMs: 60_000, maxReadEvents: 100 }, () => {});
    brokers.push(receiver, publisher);
    await receiver.subscribe({
      from: source.id, events: ["pi.agent_settled"], to: targetIdentity.id,
      delivery: "followUp", triggerTurn: false, once: true,
    });
    await publisher.publish({ source, event: "pi.agent_settled", occurredAt: 1 });           // former owner
    await publisher.publish({ source: { ...source, ownerHostId: "host:new", ownerIdentityId: "host:new" },
      event: "pi.agent_settled", occurredAt: 2 });                                            // new owner
    receiver.start();
    await waitFor(() => deliveries.length === 1);
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(deliveries.map((event) => event.occurredAt)).toEqual([2]);
    await waitFor(() => receiver.list().length === 0);       // the once subscription ended on it
  });

  it("delivers attributed component state transitions", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-lifecycle-"));
    roots.push(root);
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const deliveries: FabricLifecycleEvent[] = [];
    const target = new LifecycleBroker(
      mesh,
      targetIdentity,
      participants(targetIdentity.id),
      { enabled: true, pollMs: 20, maxReadEvents: 100 },
      (_subscription, event) => { deliveries.push(event); },
    );
    const publisher = new LifecycleBroker(
      mesh,
      sourceIdentity,
      participants(sourceIdentity.id),
      { enabled: true, pollMs: 20, maxReadEvents: 100 },
      () => {},
    );
    brokers.push(target, publisher);
    await target.subscribe({
      from: source.id,
      events: ["component.state"],
      to: targetIdentity.id,
      delivery: "followUp",
      triggerTurn: false,
    });
    target.start();
    await publisher.publish({
      source,
      event: "component.state",
      data: { id: "indexer", state: "active", revision: 2 },
    });
    await waitFor(() => deliveries.length === 1);
    expect(deliveries[0]).toMatchObject({
      event: "component.state",
      data: { id: "indexer", state: "active", revision: 2 },
    });
  });

  it("persists cursors across broker restarts without redelivering old events", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-lifecycle-"));
    roots.push(root);
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const directory = participants(targetIdentity.id);
    const delivered: string[] = [];
    const publisher = new LifecycleBroker(
      mesh,
      sourceIdentity,
      participants(sourceIdentity.id),
      { enabled: true, pollMs: 20, maxReadEvents: 100 },
      () => {},
    );
    const first = new LifecycleBroker(
      mesh,
      targetIdentity,
      directory,
      { enabled: true, pollMs: 20, maxReadEvents: 100 },
      (_subscription, event) => {
        delivered.push(event.id);
      },
    );
    brokers.push(publisher, first);
    const subscription = await first.subscribe({
      from: source.id,
      events: ["pi.agent_settled"],
      to: targetIdentity.id,
      delivery: "followUp",
      triggerTurn: true,
    });
    first.start();
    await publisher.publish({ source, event: "pi.agent_settled" });
    await waitFor(() => delivered.length === 1);
    await first.close();

    const replacement = new LifecycleBroker(
      mesh,
      targetIdentity,
      directory,
      { enabled: true, pollMs: 20, maxReadEvents: 100 },
      (_record, event) => {
        delivered.push(event.id);
      },
    );
    brokers.push(replacement);
    replacement.start();
    await new Promise((resolve) => setTimeout(resolve, 60));
    expect(delivered).toHaveLength(1);

    await publisher.publish({ source, event: "pi.agent_settled" });
    await waitFor(() => delivered.length === 2);
    await expect(replacement.unsubscribe(subscription.id)).resolves.toEqual({ removed: true });
  });
});
