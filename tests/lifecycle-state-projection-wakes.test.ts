import { spawn } from "node:child_process";
import { EventEmitter } from "node:events";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { LifecycleBroker } from "../src/lifecycle/broker.js";
import {
  FABRIC_LIFECYCLE_SUBSCRIPTION_PREFIX, FABRIC_PARTICIPANT_LIFECYCLE_TOPIC, type FabricLifecycleEvent, type FabricLifecycleSubscription,
} from "../src/lifecycle/types.js";
import { closeNatsStateBackend, NatsStateBackend, openNatsStateBackend } from "../src/mesh/state-backend-nats.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import type { FabricParticipantInfo, FabricParticipantSource } from "../src/topology/types.js";
import { fakeJetStream } from "./helpers/nats-jetstream-fake.js";

// pi-fabric#7978 L1: an event-file wake can reach the receiver before its state projection shows the subscription
// that makes the event deliverable. The projection applying that subscription must wake one fresh drain.

const roots: string[] = [];
const brokers: LifecycleBroker[] = [];
const restores: Array<() => void> = [];
const cleanups: Array<() => void> = [];

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
const subscription = (id: string): FabricLifecycleSubscription => ({
  format: 1, id, from: source.id, events: ["pi.agent_settled"], to: target.id, delivery: "followUp", triggerTurn: false,
  once: false, afterSequence: 0, createdAt: 1, updatedAt: 1, createdBy: sourceIdentity,
});
const settleEvent = (mesh: MeshStore) => mesh.publish({ topic: FABRIC_PARTICIPANT_LIFECYCLE_TOPIC, kind: "pi.agent_settled",
  from: sourceIdentity, data: { version: 1, event: "pi.agent_settled", source, occurredAt: 1 } });

interface FakeWatcher { directory: string; callback: (event: string, name: string | null) => void }
/** Replaces fs.watch: only the test fires file notifications. */
const fakeWatch = () => {
  const watchers: FakeWatcher[] = [];
  const spy = vi.spyOn(fs, "watch").mockImplementation(((directory: string, callback: FakeWatcher["callback"]) => {
    watchers.push({ directory, callback });
    return Object.assign(new EventEmitter(), { close: vi.fn() }) as unknown as fs.FSWatcher;
  }) as unknown as typeof fs.watch);
  restores.push(() => spy.mockRestore());
  return { fire: (root: string, name: string) => watchers.filter((entry) => entry.directory === root).at(-1)!.callback("change", name) };
};

const flush = async () => { for (let index = 0; index < 50; index++) await new Promise((resolve) => setImmediate(resolve)); };
const until = async (check: () => boolean, ms = 10_000) => {
  const deadline = Date.now() + ms;
  while (!check()) {
    if (Date.now() > deadline) throw new Error("condition not reached");
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
};

afterEach(async () => {
  await Promise.all(brokers.splice(0).map((broker) => broker.close()));
  for (const restore of restores.splice(0)) restore();
  for (const cleanup of cleanups.splice(0)) cleanup();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const tempRoot = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-projection-wakes-")); roots.push(root);
  return path.join(root, "mesh");
};

describe("LifecycleBroker state-projection wakes (pi-fabric#7978 L1)", () => {
  it("NATS: an event woken before the receiver's projector applies its subscription is delivered once when the projection lands", async () => {
    const files = fakeWatch();
    const server = fakeJetStream();
    const root = tempRoot();
    fs.mkdirSync(root, { recursive: true, mode: 0o700 });
    // The receiver's projector is the root's prepared backend, on a port whose deliveries the test can hold.
    const receiverPort = server.port();
    cleanups.push(() => closeNatsStateBackend(root));
    const receiver = await openNatsStateBackend({ root, port: receiverPort, lockTimeoutMs: 2_000 });
    const mesh = new MeshStore(root, 64 * 1024, 100, { stateBackend: "nats" });
    expect(mesh.stateBackend).toBe("nats");
    const remote = await NatsStateBackend.open({ port: server.port(), stream: "S", subject: "S.state", snapshotSubject: "S.snapshot",
      maxEventBytes: 1 << 20, lockTimeoutMs: 2_000 });
    cleanups.push(() => remote.close());

    const delivered: FabricLifecycleEvent[] = [];
    const broker = new LifecycleBroker(mesh, target, participants(), { enabled: true, pollMs: 20, maxReadEvents: 100 },
      (_subscription, event) => { delivered.push(event); });
    brokers.push(broker);
    const unsubscribe = vi.fn();
    const subscribe = vi.spyOn(mesh, "subscribeStateChanges").mockImplementation((listener) => {
      const stop = receiver.subscribeStateChanges(listener);
      return () => { unsubscribe(); stop(); };
    });
    const drains = vi.spyOn(mesh, "listAll");
    broker.start();
    expect(subscribe).toHaveBeenCalledTimes(1);
    await flush();

    // Another host commits the subscription; this receiver's projector lags (its deliveries are held).
    receiverPort.pause();
    await remote.put({ key: FABRIC_LIFECYCLE_SUBSCRIPTION_PREFIX + "sub1", value: subscription("sub1"), identity: sourceIdentity, ifVersion: 0 });
    await flush();
    expect(mesh.listAll(FABRIC_LIFECYCLE_SUBSCRIPTION_PREFIX)).toEqual([]);

    // The durable event and its only file wake arrive first: the drain sees the stale projection.
    const event = await settleEvent(mesh);
    const before = drains.mock.calls.length;
    files.fire(root, "events.jsonl");
    await flush();
    expect(drains.mock.calls.length).toBeGreaterThan(before);
    expect(delivered).toEqual([]);

    // The projector applies the subscription: no second event, exactly one delivery.
    receiverPort.resume();
    await until(() => delivered.length > 0);
    await flush();
    expect(delivered.map((entry) => entry.id)).toEqual([event.id]);
    expect(mesh.latestSequence()).toBe(event.sequence);

    // Fleet-shaped unrelated projections do not drain or reread the event log.
    const eventReads = vi.spyOn(mesh, "read");
    restores.push(() => eventReads.mockRestore());
    const afterDelivery = drains.mock.calls.length;
    await remote.writeBatch({ identity: sourceIdentity, ops: Array.from({ length: 455 }, (_, index) => ({
      kind: "put" as const, key: `unrelated/${index}`, value: index,
    })) });
    await until(() => mesh.get("unrelated/454") !== undefined);
    await flush();
    expect(drains.mock.calls.length).toBe(afterDelivery);
    expect(eventReads).not.toHaveBeenCalled();
    expect(delivered).toHaveLength(1);

    // Updating or deleting the relevant projected subscription still wakes a canonical drain.
    const key = FABRIC_LIFECYCLE_SUBSCRIPTION_PREFIX + "sub1";
    const current = remote.get(key)!;
    await remote.put({ key, value: { ...(current.value as FabricLifecycleSubscription), triggerTurn: true }, identity: sourceIdentity });
    await until(() => (mesh.get(key)?.value as FabricLifecycleSubscription)?.triggerTurn === true);
    await flush();
    expect(drains.mock.calls.length).toBeGreaterThan(afterDelivery);
    expect(delivered).toHaveLength(1);
    const afterUpdate = drains.mock.calls.length;
    await remote.delete({ key });
    await until(() => mesh.get(key) === undefined);
    await flush();
    expect(drains.mock.calls.length).toBeGreaterThan(afterUpdate);
    expect(delivered).toHaveLength(1);

    // close() disposes the projection callback: later projections wake nothing.
    await broker.close();
    expect(unsubscribe).toHaveBeenCalledTimes(1);
    const closedDrains = drains.mock.calls.length;
    await remote.put({ key: "unrelated/other", value: 2, identity: sourceIdentity });
    await until(() => mesh.get("unrelated/other") !== undefined);
    await flush();
    expect(drains.mock.calls.length).toBe(closedDrains);
    expect(delivered).toHaveLength(1);
  });

  it("control: without the projection hook the same NATS sequence strands the event (the L1 gap)", async () => {
    const files = fakeWatch();
    const server = fakeJetStream();
    const root = tempRoot();
    fs.mkdirSync(root, { recursive: true, mode: 0o700 });
    const receiverPort = server.port();
    cleanups.push(() => closeNatsStateBackend(root));
    await openNatsStateBackend({ root, port: receiverPort, lockTimeoutMs: 2_000 });
    const mesh = new MeshStore(root, 64 * 1024, 100, { stateBackend: "nats" });
    const remote = await NatsStateBackend.open({ port: server.port(), stream: "S", subject: "S.state", snapshotSubject: "S.snapshot",
      maxEventBytes: 1 << 20, lockTimeoutMs: 2_000 });
    cleanups.push(() => remote.close());
    const delivered: FabricLifecycleEvent[] = [];
    const broker = new LifecycleBroker(mesh, target, participants(), { enabled: true, pollMs: 20, maxReadEvents: 100 },
      (_subscription, event) => { delivered.push(event); });
    brokers.push(broker);
    vi.spyOn(mesh, "subscribeStateChanges").mockReturnValue(() => undefined);
    broker.start();
    await flush();
    receiverPort.pause();
    await remote.put({ key: FABRIC_LIFECYCLE_SUBSCRIPTION_PREFIX + "sub1", value: subscription("sub1"), identity: sourceIdentity, ifVersion: 0 });
    await settleEvent(mesh);
    files.fire(root, "events.jsonl");
    await flush();
    receiverPort.resume();
    await until(() => mesh.listAll(FABRIC_LIFECYCLE_SUBSCRIPTION_PREFIX).length === 1);
    await flush();
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(delivered).toEqual([]);
  });

  it("NATS backend: notifies after the whole message is projected, coalesces, survives a throwing listener, unsubscribes", async () => {
    const server = fakeJetStream();
    const backend = await NatsStateBackend.open({ port: server.port(), stream: "S", subject: "S.state", snapshotSubject: "S.snapshot",
      maxEventBytes: 1 << 20, lockTimeoutMs: 2_000 });
    const writer = await NatsStateBackend.open({ port: server.port(), stream: "S", subject: "S.state", snapshotSubject: "S.snapshot",
      maxEventBytes: 1 << 20, lockTimeoutMs: 2_000 });
    cleanups.push(() => { backend.close(); writer.close(); });
    const seen: unknown[][] = [];
    const stopThrowing = backend.subscribeStateChanges(() => { throw new Error("listener failure"); });
    const stop = backend.subscribeStateChanges(() => { seen.push(backend.listAll("w/").map((entry) => entry.value)); });
    await writer.writeBatch({ identity: sourceIdentity, ops: [{ kind: "put", key: "w/a", value: 1 }, { kind: "put", key: "w/b", value: 2 }] });
    await until(() => seen.length > 0);
    // The listener never sees half a batch: both keys are visible at its first call.
    expect(seen[0]).toEqual([1, 2]);
    await writer.put({ key: "w/c", value: 3, identity: sourceIdentity });
    await until(() => seen.length > 1);
    expect(backend.get("w/c")?.value).toBe(3);
    stop(); stopThrowing();
    const count = seen.length;
    await writer.put({ key: "w/d", value: 4, identity: sourceIdentity });
    await until(() => backend.get("w/d") !== undefined);
    await flush();
    expect(seen).toHaveLength(count);
  });

  it("MeshStore delegates to a backend without the hook as a no-op", () => {
    const mesh = new MeshStore(tempRoot(), 64 * 1024, 100, { stateBackend: "file" });
    const stop = mesh.subscribeStateChanges(() => { throw new Error("never called"); });
    expect(typeof stop).toBe("function");
    stop();
  });
});

const SQLITE_CHILD = `
import { createJiti } from "jiti";
import { pathToFileURL } from "node:url";
const [root, key, value] = process.argv.slice(1);
const jiti = createJiti(pathToFileURL(process.cwd() + "/index.js").href);
const { MeshStore } = await jiti.import("./src/mesh/store.ts");
const store = new MeshStore(root, 64 * 1024, 100, { stateBackend: "sqlite" });
if (store.stateBackend !== "sqlite") throw new Error("not sqlite");
await store.put({ key, value: JSON.parse(value), identity: { id: "child", name: "child", kind: "agent" }, ifVersion: 0 });
store.closeState();
process.stdout.write("committed\\n");
`;

const runChild = (root: string, key: string, value: unknown): Promise<{ code: number | null; stdout: string; stderr: string }> =>
  new Promise((resolve, reject) => {
    const child = spawn(process.execPath, ["--disable-warning=ExperimentalWarning", "--input-type=module", "-e", SQLITE_CHILD, root, key, JSON.stringify(value)],
      { cwd: process.cwd(), stdio: ["ignore", "pipe", "pipe"], env: { ...process.env, PI_FABRIC_MESH_STATE_BACKEND: "" } });
    let stdout = ""; let stderr = "";
    child.stdout.on("data", (chunk) => { stdout += chunk; });
    child.stderr.on("data", (chunk) => { stderr += chunk; });
    child.once("error", reject);
    // The local deletion guard announces itself before the child runs; retain every other stderr byte.
    child.once("close", (code) => resolve({ code, stdout,
      stderr: stderr.replace(/^hosted-only \(outside-temp deletion reporting\): push and let CI run it\r?\n/, "") }));
  });

describe("LifecycleBroker canonical state-file relevance", () => {
  it.each(["file", "sqlite"] as const)("%s: unrelated real state commits do not drain, but a subscription commit delivers the existing event", async stateBackend => {
    const root = tempRoot();
    // A long file read-cache window must not hide a commit carried by the only state-file hint.
    const mesh = new MeshStore(root, 64 * 1024, 100, { stateBackend, readCacheMs: 60_000 });
    const writer = new MeshStore(root, 64 * 1024, 100, { stateBackend });
    expect(mesh.stateBackend).toBe(stateBackend);
    cleanups.push(() => { writer.closeState(); mesh.closeState(); });
    const event = await settleEvent(mesh);
    await mesh.put({ key: FABRIC_LIFECYCLE_SUBSCRIPTION_PREFIX + "caught-up", identity: target, ifVersion: 0,
      value: { ...subscription("caught-up"), afterSequence: event.sequence } });
    const delivered: FabricLifecycleEvent[] = [];
    const broker = new LifecycleBroker(mesh, target, participants(), { enabled: true, pollMs: 20, maxReadEvents: 100 },
      (_subscription, event) => { delivered.push(event); });
    brokers.push(broker);
    broker.start();
    await flush();
    await new Promise(resolve => setTimeout(resolve, 100));
    expect(delivered).toEqual([]);
    const events = fs.statSync(path.join(root, "events.jsonl"));
    const drains = vi.spyOn(mesh, "listAll"), reads = vi.spyOn(mesh, "read"), projections = vi.spyOn(mesh, "listAllShared");
    restores.push(() => { drains.mockRestore(); reads.mockRestore(); projections.mockRestore(); });

    await writer.writeBatch({ identity: sourceIdentity, ops: Array.from({ length: 455 }, (_, index) => ({
      kind: "put" as const, key: `unrelated/${index}`, value: index,
    })) });
    // Prove a native state-file notification reached the receiver, not merely that nothing happened.
    await until(() => projections.mock.calls.length > 0 || drains.mock.calls.length > 0);
    await new Promise(resolve => setTimeout(resolve, 100));
    await flush();
    expect(mesh.get("unrelated/454", { fresh: true })?.value).toBe(454);
    expect(drains).not.toHaveBeenCalled();
    expect(reads).not.toHaveBeenCalled();
    expect(delivered).toEqual([]);
    expect(projections).toHaveBeenCalled();

    // Same native watcher, same event log, no event-file write: only the relevant subscription changes.
    await writer.put({ key: FABRIC_LIFECYCLE_SUBSCRIPTION_PREFIX + "sub1", value: subscription("sub1"),
      identity: sourceIdentity, ifVersion: 0 });
    await until(() => delivered.length > 0);
    await flush();
    expect(delivered.map(entry => entry.id)).toEqual([event.id]);
    expect(drains).toHaveBeenCalled();
    const after = fs.statSync(path.join(root, "events.jsonl"));
    expect([after.size, after.mtimeMs]).toEqual([events.size, events.mtimeMs]);
  }, 30_000);
});

describe("LifecycleBroker sqlite cross-process state wakes (pi-fabric#7978 L1)", () => {
  it.skipIf(process.platform !== "linux")("a subscription committed by another process wakes the receiver through state.db notifications, with no event-file write", async () => {
    const root = tempRoot();
    const mesh = new MeshStore(root, 64 * 1024, 100, { stateBackend: "sqlite" });
    if (mesh.stateBackend !== "sqlite") return; // no node:sqlite or non-local filesystem: nothing to prove here
    cleanups.push(() => mesh.closeState());
    const delivered: FabricLifecycleEvent[] = [];
    const broker = new LifecycleBroker(mesh, target, participants(), { enabled: true, pollMs: 20, maxReadEvents: 100 },
      (_subscription, event) => { delivered.push(event); });
    brokers.push(broker);
    // The durable event predates the subscription; the broker's startup drain finds nothing to deliver.
    const event = await settleEvent(mesh);
    const watched = vi.spyOn(fs, "watch");
    restores.push(() => watched.mockRestore());
    broker.start();
    await flush();
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(delivered).toEqual([]);
    const events = fs.statSync(path.join(root, "events.jsonl"));

    const child = await runChild(root, FABRIC_LIFECYCLE_SUBSCRIPTION_PREFIX + "sub1", subscription("sub1"));
    expect(child).toMatchObject({ code: 0, stdout: "committed\n", stderr: "" });
    await until(() => delivered.length > 0);
    await flush();
    expect(delivered.map((entry) => entry.id)).toEqual([event.id]);
    // No event-file write carried this wake: only the other process's state commit did.
    const after = fs.statSync(path.join(root, "events.jsonl"));
    expect([after.size, after.mtimeMs]).toEqual([events.size, events.mtimeMs]);
    expect(watched).toHaveBeenCalled();
  }, 30_000);

  it.skipIf(process.platform !== "linux")("control: with filesystem notifications suppressed the same commit is not delivered (the wake is the state.db watch)", async () => {
    fakeWatch();
    const root = tempRoot();
    const mesh = new MeshStore(root, 64 * 1024, 100, { stateBackend: "sqlite" });
    if (mesh.stateBackend !== "sqlite") return;
    cleanups.push(() => mesh.closeState());
    const delivered: FabricLifecycleEvent[] = [];
    const broker = new LifecycleBroker(mesh, target, participants(), { enabled: true, pollMs: 20, maxReadEvents: 100 },
      (_subscription, event) => { delivered.push(event); });
    brokers.push(broker);
    await settleEvent(mesh);
    broker.start();
    await flush();
    const child = await runChild(root, FABRIC_LIFECYCLE_SUBSCRIPTION_PREFIX + "sub1", subscription("sub1"));
    expect(child.code).toBe(0);
    await new Promise((resolve) => setTimeout(resolve, 500));
    expect(mesh.listAll(FABRIC_LIFECYCLE_SUBSCRIPTION_PREFIX, { fresh: true })).toHaveLength(1);
    expect(delivered).toEqual([]);
  }, 30_000);
});
