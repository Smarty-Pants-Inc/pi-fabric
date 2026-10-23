import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterEach, describe, expect, it, vi } from "vitest";
import { LifecycleBroker } from "../src/lifecycle/broker.js";
import {
  FABRIC_LIFECYCLE_SUBSCRIPTION_PREFIX as PREFIX,
  FABRIC_PARTICIPANT_LIFECYCLE_TOPIC,
  type FabricLifecycleEvent,
  type FabricLifecycleSubscription,
} from "../src/lifecycle/types.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { readHostLeaseCurrent, writeHostLease } from "../src/topology/host-leases.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";

// Keep: the real fleet-renewal performance regression (drivers-map lifecycle attribution),
// and the state-only/await race that can silently lose remote subscription admission.
// No fake filesystem watches, lease authority or delivery/cursor storage in these flows.
const roots: string[] = [];
const brokers: LifecycleBroker[] = [];
const directories: ParticipantDirectory[] = [];
const stores: MeshStore[] = [];
const target: MeshIdentity = { id: "session:target", name: "target", kind: "main" };
const sourceIdentity: MeshIdentity = { id: "session:source", name: "source", kind: "main" };
const source = { id: sourceIdentity.id, name: sourceIdentity.name, kind: "root" as const,
  rootId: sourceIdentity.id, runner: "pi" as const, ownerHostId: sourceIdentity.id, ownerIdentityId: sourceIdentity.id };
const request = { from: source.id, events: ["pi.agent_settled" as const], to: target.id,
  delivery: "followUp" as const, triggerTurn: false };
const flush = async () => { for (let i = 0; i < 20; i++) await new Promise(resolve => setImmediate(resolve)); };
const until = async (check: () => boolean) => {
  await vi.waitFor(() => expect(check()).toBe(true), { timeout: 2_000, interval: 5 });
  await flush();
};

afterEach(async () => {
  await Promise.all(brokers.splice(0).map(broker => broker.close()));
  await Promise.all(directories.splice(0).map(directory => directory.close()));
  for (const store of stores.splice(0)) store.closeState();
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const fixture = async (stateBackend: "file" | "sqlite", deliver?: (subscription: FabricLifecycleSubscription, event: FabricLifecycleEvent) => Promise<void> | void) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "idle-lifecycle-subscriptions-")); roots.push(root);
  const meshRoot = path.join(root, "mesh");
  const receiver = new MeshStore(meshRoot, 64 * 1024, 100, { stateBackend });
  const remote = new MeshStore(meshRoot, 64 * 1024, 100, { stateBackend });
  stores.push(receiver, remote);
  expect(receiver.stateBackend).toBe(stateBackend); // never silently substitute FILE for SQLite coverage
  const directory = (mesh: MeshStore, identity: MeshIdentity) => {
    const value = new ParticipantDirectory(mesh, { enabled: true, hostId: identity.id, rootId: identity.id,
      identity: { ...identity, kind: "actor" }, heartbeatMs: 60_000, leaseMs: 120_000 });
    value.registerSource(() => [{ format: 1, id: identity.id, kind: "root", rootId: identity.id,
      ownerHostId: identity.id, ownerIdentityId: identity.id, name: identity.name, status: "idle", runner: "pi",
      transport: "host", capabilities: ["steer", "followUp", "fabric"], startedAt: 1, updatedAt: 2,
      controlProtocol: "v1", pendingMessages: false }]);
    directories.push(value);
    return value;
  };
  const remoteDirectory = directory(remote, sourceIdentity);
  const localDirectory = directory(receiver, target);
  await remoteDirectory.refresh();
  await localDirectory.refresh();
  const deliveries: Array<{ subscription: string; event: FabricLifecycleEvent }> = [];
  let open = true;
  const broker = new LifecycleBroker(receiver, target, localDirectory,
    { enabled: true, pollMs: 20, maxReadEvents: 100, canConsumeMesh: () => open }, async (subscription, event) => {
      deliveries.push({ subscription: subscription.id, event });
      await deliver?.(subscription, event);
    });
  const publisher = new LifecycleBroker(remote, sourceIdentity, remoteDirectory,
    { enabled: true, pollMs: 20, maxReadEvents: 100 }, () => { throw new Error("foreign target delivered on publisher"); });
  brokers.push(broker, publisher);
  const scan = vi.spyOn(receiver, "listAll");
  const scans = () => scan.mock.calls.filter(([prefix]) => prefix === PREFIX).length;
  const ownership = vi.spyOn(localDirectory, "publishes");
  const renew = (id = "host:unrelated", expiresAt = Date.now() + 120_000) => {
    const previous = readHostLeaseCurrent(meshRoot, id);
    writeHostLease(meshRoot, { ...(previous ?? { id, rootId: id, identityId: id }), updatedAt: Date.now(), expiresAt });
  };
  const start = async () => { broker.start(); publisher.start(); await flush(); };
  const value = (id: string, afterSequence = 0): FabricLifecycleSubscription => ({ format: 1, id, ...request,
    once: false, afterSequence, createdAt: 1, updatedAt: 1, createdBy: sourceIdentity });
  const put = (subscription: FabricLifecycleSubscription) => remote.put({ key: PREFIX + subscription.id,
    value: subscription, identity: sourceIdentity });
  const read = (id: string) => receiver.get(PREFIX + id, { fresh: true })?.value as FabricLifecycleSubscription | undefined;
  const emit = (occurredAt: number) => publisher.publish({ source, event: "pi.agent_settled", occurredAt });
  return { root: meshRoot, receiver, remote, broker, publisher, localDirectory, deliveries, ownership, scans, renew,
    start, value, put, read, emit, gate: (next: boolean) => { open = next; } };
};

describe.each(["file", "sqlite"] as const)("idle lifecycle subscription entries (%s)", backend => {
  it("handles continuing native unrelated lease/presence renewals without SQLite full subscription scans", async () => {
    const f = await fixture(backend);
    await f.publisher.subscribe(request); // admission on the other broker, target ownership stays here
    // A realistic namespace, mostly foreign targets; unrelated state is not a subscription invalidation.
    await f.remote.writeBatch({ identity: sourceIdentity, ops: Array.from({ length: 128 }, (_, i) => ({
      kind: "put" as const, key: PREFIX + `foreign-${i}`,
      value: { ...f.value(`foreign-${i}`), to: "session:elsewhere" },
    })) });
    await f.start();
    await until(() => f.ownership.mock.calls.length > 0);
    const before = f.scans();
    for (let i = 0; i < 8; i++) {
      const checked = f.ownership.mock.calls.length;
      f.renew();
      await until(() => f.ownership.mock.calls.length > checked); // actual native presence wake was drained
      await f.remote.put({ key: "topology/hosts/unrelated", value: { renewal: i }, identity: sourceIdentity });
      await flush();
    }
    if (backend === "sqlite") expect(f.scans()).toBe(before);
    else expect(f.scans()).toBeGreaterThan(before); // no namespace proof on FILE: canonical fallback
    await f.emit(1);
    await until(() => f.deliveries.length === 1);
    expect(f.deliveries[0]?.event.occurredAt).toBe(1);
  });

  it("observes state-only remote join, cursor mutation and deletion with no new lifecycle event", async () => {
    const f = await fixture(backend);
    // The event's notification drains an empty subscription namespace first.
    const event = await f.remote.publish({ topic: FABRIC_PARTICIPANT_LIFECYCLE_TOPIC, kind: "pi.agent_settled",
      from: sourceIdentity, data: { version: 1, event: "pi.agent_settled", source, occurredAt: 2 } });
    await f.start();
    const log = fs.statSync(path.join(f.root, "events.jsonl"));
    const sub = f.value("state-only");
    const proofs = vi.spyOn(f.receiver, "stateUnchangedSince");
    // Observe real drains; do not manufacture a wake or extend the wait.
    const checkpoint = async (phase: string, mutate: () => Promise<unknown>, check: () => boolean) => {
      const before = { revision: f.receiver.stateRevision() ?? null, scans: f.scans(), proofs: proofs.mock.calls.length,
        ownershipChecks: f.ownership.mock.calls.length };
      try {
        await mutate();
        await until(check);
      } catch (cause) {
        throw new Error(`state-only ${backend} ${phase}: ${JSON.stringify({ before,
          after: { revision: f.receiver.stateRevision() ?? null, scans: f.scans(), proofs: proofs.mock.calls.length,
            lastProof: { call: proofs.mock.calls.at(-1), result: proofs.mock.results.at(-1) },
            ownershipChecks: f.ownership.mock.calls.length },
          subscription: f.read(sub.id) ?? null, deliveries: f.deliveries.map(row => row.event.id),
        })}`, { cause });
      }
    };
    await checkpoint("join", () => f.put(sub), () => f.read(sub.id)?.lastEventId === event.id);
    expect(f.deliveries.map(row => row.event.id)).toEqual([event.id]);
    // Another host rewinds the canonical cursor: unchanged log is not a reason to skip the new entry.
    await checkpoint("cursor rewind", () => f.put({ ...sub, updatedAt: 3 }),
      () => f.deliveries.length === 2 && f.read(sub.id)?.lastEventId === event.id);
    expect(f.deliveries.map(row => row.event.id)).toEqual([event.id, event.id]);
    const scanned = f.scans();
    await checkpoint("deletion", async () => {
      expect(await f.publisher.unsubscribe(sub.id)).toEqual({ removed: true });
    }, () => f.scans() > scanned);
    const after = fs.statSync(path.join(f.root, "events.jsonl"));
    expect([after.size, after.mtimeMs]).toEqual([log.size, log.mtimeMs]);
    // Even a later received event cannot activate the deleted cached subscription.
    await f.remote.publish({ topic: FABRIC_PARTICIPANT_LIFECYCLE_TOPIC, kind: "pi.agent_settled",
      from: sourceIdentity, data: { version: 1, event: "pi.agent_settled", source, occurredAt: 3 } });
    await flush();
    expect(f.deliveries).toHaveLength(2);
  });

  it("rechecks stale target liveness on a lease-only wake, then serves a reopened gate and pause", async () => {
    const f = await fixture(backend);
    const sub = await f.publisher.subscribe(request);
    await f.start();
    const live = readHostLeaseCurrent(f.root, target.id)!;
    // Native liveness is the later of state and file expiry; lapse both, then revive only the file.
    const host = f.remote.listAll("topology/hosts/").find(entry => (entry.value as { id?: string }).id === target.id)!;
    await f.remote.put({ key: host.key, identity: target, value: { ...(host.value as object),
      updatedAt: Date.now() - 2_000, expiresAt: Date.now() - 1_000 } });
    f.renew(target.id, Date.now() - 1_000);
    await flush();
    expect(f.localDirectory.get(target.id)).toBeUndefined();
    const checked = f.ownership.mock.calls.length;
    await f.emit(4);
    await until(() => f.ownership.mock.calls.length > checked);
    expect(f.deliveries).toHaveLength(0);
    const before = f.scans();
    writeHostLease(f.root, { ...live, updatedAt: Date.now(), expiresAt: Date.now() + 120_000 });
    await until(() => f.read(sub.id)?.lastEventId === f.deliveries[0]?.event.id && f.deliveries.length === 1);
    if (backend === "sqlite") expect(f.scans()).toBeLessThanOrEqual(before + 1); // cursor invalidates, lease does not
    f.gate(false);
    await f.emit(5);
    await flush();
    expect(f.deliveries).toHaveLength(1);
    f.gate(true);
    f.renew(); // no new event or timer: presence must reconsider retained work
    await until(() => f.deliveries.length === 2);
    f.broker.pause();
    await f.emit(6);
    await flush();
    expect(f.deliveries).toHaveLength(2);
    f.broker.resume();
    await until(() => f.deliveries.length === 3);
    expect(f.deliveries.map(row => row.event.occurredAt)).toEqual([4, 5, 6]);
  });

  it("reconsiders a formerly foreign target when native participant publication makes it local", async () => {
    const f = await fixture(backend);
    const actor = { ...f.localDirectory.get(target.id)!, id: "actor:later", name: "later",
      kind: "actor" as const, transport: "process" as const };
    const sub = { ...f.value("foreign-first"), to: actor.id };
    await f.put(sub);
    await f.start();
    await f.emit(7);
    await flush();
    expect(f.localDirectory.publishes(actor.id)).toBe(false);
    expect(f.deliveries).toHaveLength(0);
    // Same canonical subscription, same event log; the target becomes owned here.
    f.localDirectory.registerSource(() => [actor]);
    await f.localDirectory.refresh();
    await until(() => f.deliveries.length === 1);
    expect(f.deliveries[0]?.event.occurredAt).toBe(7);
  });

  it("does not advance the cached revision past a state-only join during awaited delivery", async () => {
    let entered!: () => void;
    let finish!: () => void;
    const pending = new Promise<void>(resolve => { entered = resolve; });
    const held = new Promise<void>(resolve => { finish = resolve; });
    const f = await fixture(backend, async subscription => {
      if (subscription.id === "first") { entered(); await held; }
    });
    await f.put({ ...f.value("first"), once: true });
    await f.start();
    try {
      await f.emit(8);
      await pending;
      // Native state notifications arrive while the original list is still being drained.
      await f.put({ ...f.value("joined-during-await"), once: true });
      await flush();
    } finally { finish(); }
    await until(() => f.deliveries.length === 2 && !f.read("first") && !f.read("joined-during-await"));
    expect(f.deliveries.map(row => row.subscription)).toEqual(["first", "joined-during-await"]);
    expect(f.deliveries[0]?.event.id).toBe(f.deliveries[1]?.event.id);
  });
});

describe("idle lifecycle subscription proof fallback", () => {
  it("uses canonical entries after a real SQLite change-feed gap and an unknown revision", async () => {
    const f = await fixture("sqlite");
    await f.start();
    f.broker.pause();
    await f.remote.publish({ topic: FABRIC_PARTICIPANT_LIFECYCLE_TOPIC, kind: "pi.agent_settled",
      from: sourceIdentity, data: { version: 1, event: "pi.agent_settled", source, occurredAt: 9 } });
    await f.put({ ...f.value("trimmed-feed"), once: true });
    // Retention can remove the proof between drains. This is a separate connection to this
    // test's private database, never a product/backend change or a live-store deletion.
    const db = new DatabaseSync(path.join(f.root, "state.db"));
    try { db.exec("DELETE FROM changes"); } finally { db.close(); }
    f.broker.resume();
    await until(() => f.deliveries.length === 1 && !f.read("trimmed-feed"));
    f.broker.pause();
    const revision = vi.spyOn(f.receiver, "stateRevision").mockReturnValue(undefined);
    await f.put({ ...f.value("unknown-revision"), once: true });
    f.broker.resume();
    await until(() => f.deliveries.length === 2 && !f.read("unknown-revision"));
    revision.mockRestore();
    expect(f.deliveries.map(row => row.subscription)).toEqual(["trimmed-feed", "unknown-revision"]);
  });
});
