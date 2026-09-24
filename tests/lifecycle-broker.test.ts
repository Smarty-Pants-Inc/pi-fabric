import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { LifecycleBroker } from "../src/lifecycle/broker.js";
import {
  FABRIC_PARTICIPANT_LIFECYCLE_TOPIC,
  type FabricLifecycleEvent,
  type FabricLifecycleSubscription,
} from "../src/lifecycle/types.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
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
    const sourceDirectory = directory(sourceMesh, sourceIdentity, 300);
    const targetDirectory = directory(targetMesh, targetIdentity, 60_000);   // the receiver stays live
    await sourceDirectory.refresh();                           // the source's lease: 300 ms
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
    await new Promise((resolve) => setTimeout(resolve, 400));  // the cached lease has lapsed
    expect(targetDirectory.get(source.id)).toBeUndefined();   // the receiver's cached view
    await sourceDirectory.refresh();                           // the source renews ...
    await publisher.publish({ source, event: "pi.agent_settled", occurredAt: 7 });   // ... and publishes
    expect(targetDirectory.get(source.id)).toBeUndefined();   // still the stale cached view
    target.start();
    await waitFor(() => deliveries.length === 1);
    expect(deliveries[0]).toMatchObject({ event: "pi.agent_settled", occurredAt: 7 });
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
