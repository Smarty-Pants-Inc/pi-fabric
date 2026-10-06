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
  it("3864 retains an awaited delivery receipt without advancing a cursor after lease loss", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-lifecycle-lease-")); roots.push(root);
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    let leased = true;
    const deliver = vi.fn(async () => { await Promise.resolve(); leased = false; });
    const broker = new LifecycleBroker(mesh, targetIdentity, participants(targetIdentity.id),
      { enabled: true, pollMs: 20, maxReadEvents: 100, canConsumeMesh: () => leased }, deliver);
    brokers.push(broker);
    const sub = await broker.subscribe({ from: source.id, events: ["pi.agent_settled"], to: targetIdentity.id,
      delivery: "followUp", triggerTurn: false });
    broker.start();
    await mesh.publish({ topic: FABRIC_PARTICIPANT_LIFECYCLE_TOPIC, kind: "pi.agent_settled", from: sourceIdentity,
      data: { version: 1, event: "pi.agent_settled", source, occurredAt: 42 } });
    await waitFor(() => deliver.mock.calls.length === 1);
    await new Promise(resolve => setTimeout(resolve, 80));
    expect(broker.list()[0]?.afterSequence).toBe(sub.afterSequence);
    leased = true;
    await waitFor(() => broker.list()[0]?.afterSequence === mesh.latestSequence());
    expect(deliver).toHaveBeenCalledOnce();
  });
  it.each([false, true])("3864 fences the cursor/once receipt under the commit lock after lease loss (once=%s)", async once => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-lifecycle-commit-lease-")); roots.push(root);
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    let leased = true;
    const deliver = vi.fn();
    const broker = new LifecycleBroker(mesh, targetIdentity, participants(targetIdentity.id),
      { enabled: true, pollMs: 20, maxReadEvents: 100, canConsumeMesh: () => leased }, deliver);
    brokers.push(broker);
    const sub = await broker.subscribe({ from: source.id, events: ["pi.agent_settled"], to: targetIdentity.id,
      delivery: "followUp", triggerTurn: false, once });
    const write = mesh.writeBatch.bind(mesh);
    const waiting = vi.spyOn(mesh, "writeBatch").mockImplementation(async input => {
      // Renewal fails while cursor persistence is waiting for its mesh lock.
      leased = false;
      return write(input);
    });
    broker.start();
    await mesh.publish({ topic: FABRIC_PARTICIPANT_LIFECYCLE_TOPIC, kind: "pi.agent_settled", from: sourceIdentity,
      data: { version: 1, event: "pi.agent_settled", source, occurredAt: 42 } });
    await waitFor(() => deliver.mock.calls.length === 1);
    await new Promise(resolve => setTimeout(resolve, 80));
    expect(broker.list()).toHaveLength(1);
    expect(broker.list()[0]?.afterSequence).toBe(sub.afterSequence);
    waiting.mockRestore(); leased = true;
    await waitFor(() => once ? broker.list().length === 0 : broker.list()[0]?.afterSequence === mesh.latestSequence());
    expect(deliver).toHaveBeenCalledOnce();
  });

  it.each([false, true])("blocks release on an unconfirmed delivered cursor/once deletion without replay (once=%s)", async (once) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-lifecycle-")); roots.push(root);
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const deliveries = vi.fn();
    const broker = new LifecycleBroker(mesh, targetIdentity, participants(targetIdentity.id),
      { enabled: true, pollMs: 20, maxReadEvents: 100 }, deliveries);
    brokers.push(broker);
    const sub = await broker.subscribe({ from: source.id, events: ["pi.agent_settled"], to: targetIdentity.id,
      delivery: "followUp", triggerTurn: false, once });
    const storage = once ? vi.spyOn(mesh, "delete") : vi.spyOn(mesh, "put");
    storage.mockRejectedValue(new Error("delivered receipt fsync failed"));
    broker.start();
    await mesh.publish({ topic: FABRIC_PARTICIPANT_LIFECYCLE_TOPIC, kind: "pi.agent_settled", from: sourceIdentity,
      data: { version: 1, event: "pi.agent_settled", source, occurredAt: 42 } });
    await waitFor(() => deliveries.mock.calls.length > 0);
    broker.pause();
    await expect(broker.checkpointForRelease()).rejects.toThrow(/unconfirmed|receipt|fsync/i);
    expect(deliveries).toHaveBeenCalledTimes(1);
    broker.resume(); await new Promise(resolve => setTimeout(resolve, 80)); broker.pause();
    expect(deliveries).toHaveBeenCalledTimes(1);
    // Retrying persistence must not re-execute the accepted activation.
    storage.mockRestore();
    await broker.checkpointForRelease();
    expect(once ? broker.list().length : broker.list()[0]?.afterSequence).toBe(once ? 0 : mesh.latestSequence());
    await broker.close();
    const successor = new LifecycleBroker(mesh, targetIdentity, participants(targetIdentity.id),
      { enabled: true, pollMs: 20, maxReadEvents: 100 }, deliveries);
    brokers.push(successor); successor.start();
    await new Promise(resolve => setTimeout(resolve, 80));
    expect(deliveries).toHaveBeenCalledTimes(1);
    if (once) expect(successor.list().find(s => s.id === sub.id)).toBeUndefined();
    else expect(successor.list()[0]?.id).toBe(sub.id);
  });


  it("confirms a visible delivered cursor after its put threw without re-executing delivery", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-lifecycle-")); roots.push(root);
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const deliveries = vi.fn();
    const broker = new LifecycleBroker(mesh, targetIdentity, participants(targetIdentity.id),
      { enabled: true, pollMs: 20, maxReadEvents: 100 }, deliveries); brokers.push(broker);
    await broker.subscribe({ from: source.id, events: ["pi.agent_settled"], to: targetIdentity.id,
      delivery: "followUp", triggerTurn: false, once: false });
    const put = mesh.put.bind(mesh);
    const storage = vi.spyOn(mesh, "put").mockImplementation(async input => {
      await put(input); throw Error("visible cursor has unconfirmed write receipt");
    });
    broker.start();
    await mesh.publish({ topic: FABRIC_PARTICIPANT_LIFECYCLE_TOPIC, kind: "pi.agent_settled", from: sourceIdentity,
      data: { version: 1, event: "pi.agent_settled", source, occurredAt: 42 } });
    await waitFor(() => deliveries.mock.calls.length === 1); broker.pause();
    await expect(broker.checkpointForRelease()).rejects.toThrow(/unconfirmed write receipt/);
    expect(broker.list()[0]?.afterSequence).toBe(mesh.latestSequence());
    storage.mockRestore();
    await broker.checkpointForRelease(); // repairs only the same exact accepted receipt
    broker.resume(); await new Promise(resolve => setTimeout(resolve, 80)); broker.pause();
    expect(deliveries).toHaveBeenCalledTimes(1);
  });

  it("retains a once-delete obligation that became visible before its write failed", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-lifecycle-")); roots.push(root);
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const deliveries = vi.fn();
    const broker = new LifecycleBroker(mesh, targetIdentity, participants(targetIdentity.id),
      { enabled: true, pollMs: 20, maxReadEvents: 100 }, deliveries); brokers.push(broker);
    await broker.subscribe({ from: source.id, events: ["pi.agent_settled"], to: targetIdentity.id,
      delivery: "followUp", triggerTurn: false, once: true });
    const remove = mesh.delete.bind(mesh);
    const storage = vi.spyOn(mesh, "delete").mockImplementation(async input => {
      await remove(input); throw Error("indeterminate deletion receipt");
    });
    broker.start();
    await mesh.publish({ topic: FABRIC_PARTICIPANT_LIFECYCLE_TOPIC, kind: "pi.agent_settled", from: sourceIdentity,
      data: { version: 1, event: "pi.agent_settled", source, occurredAt: 42 } });
    await waitFor(() => deliveries.mock.calls.length === 1); broker.pause();
    await expect(broker.checkpointForRelease()).rejects.toThrow(/once deletion receipt.*unconfirmed/);
    expect(broker.list()).toEqual([]); // visible absence is not a confirmed durable write
    storage.mockRestore();
    await expect(broker.checkpointForRelease()).rejects.toThrow(/unconfirmed/);
    expect(deliveries).toHaveBeenCalledTimes(1);
  });

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
  it.each(["files", "legacy state"])("delivers an event from a source whose lease was renewed after the receiver's cached read (%s)", async mode => {
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
    if (mode === "legacy state") {
      // Explicitly model a live pre-capability receiver. This must force source state renewal.
      for (const entry of targetMesh.listAll("topology/hosts/")) {
        if (entry.updatedBy.id === targetIdentity.id) await targetMesh.put({ key: entry.key, identity: targetIdentity,
          value: { ...(entry.value as Record<string, unknown>), livenessLeaseFiles: undefined } });
      }
    }
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
    const stateBefore = fs.readFileSync(path.join(meshRoot, "state.json"), "utf8");
    await sourceDirectory.refresh();                           // the source renews ...
    if (mode === "legacy state") {
      // Older writers only renew state; retain coverage of the receiver's fresh-read guard.
      removeHostLease(meshRoot, source.id);
      expect(targetDirectory.get(source.id)).toBeUndefined(); // still the stale cached view
    } else {
      // New writers renew the file, with no state commit. Cached identity is safe; lease is fresh.
      expect(fs.readFileSync(path.join(meshRoot, "state.json"), "utf8")).toBe(stateBefore);
      expect(targetDirectory.get(source.id)?.stale).toBe(false);
    }
    await publisher.publish({ source, event: "pi.agent_settled", occurredAt: 7 });   // ... and publishes
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

  // Each get() rebuilds the whole project directory from mesh state. Historical
  // mesh state reaches megabytes, so a per-subscription lookup turns every poll
  // into O(subscriptions x entries) work on the main thread.
  it("rebuilds the project directory once per drain cycle, not per subscription", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-lifecycle-lookup-"));
    roots.push(root);
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const records = [participant(targetIdentity, true), participant(sourceIdentity, false)];
    const list = vi.fn(() => records);
    const get = vi.fn((id: string) => records.find((record) => record.id === id));
    const directory: FabricParticipantSource = {
      list,
      get,
      self: () => records[0]!,
      peers: () => [],
      async refresh() {},
      scheduleRefresh() {},
    };
    const target = new LifecycleBroker(
      mesh,
      targetIdentity,
      directory,
      { enabled: true, pollMs: 20, maxReadEvents: 100 },
      () => {},
    );
    brokers.push(target);

    const kinds = ["pi.agent_settled", "pi.agent_end", "pi.turn_end"] as const;
    for (const kind of kinds) {
      await target.subscribe({
        from: source.id,
        events: [kind],
        to: targetIdentity.id,
        delivery: "followUp",
        triggerTurn: false,
      });
    }

    // Subscription creation resolves both endpoints once. After that, a drain
    // cycle resolves every target from a single directory build; only an id the
    // projection omits (for example the "main" alias) pays an individual lookup.
    const beforeDrain = get.mock.calls.length;
    await waitFor(() => list.mock.calls.length > 0);
    const duringDrain = get.mock.calls.slice(beforeDrain).map((call) => call[0]);
    expect(duringDrain).not.toContain(targetIdentity.id);
  });
});
