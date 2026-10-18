// Local data-safety/trust-boundary probes: manual singleton delivery through real stores.
// No live process teardown; lease mutation removals are recorded, not executed.
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshBridge, RemoteBridgeSide, serveBridgeAgent, StoreBridgeSide, type BridgePublish } from "../src/mesh/bridge.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { FabricControlPlane, FabricControlStaleIncarnationError } from "../src/topology/control-plane.js";
import { FABRIC_IDEMPOTENCY_KEY_REUSED, FabricIdempotencyKeyReusedError } from "../src/mesh/control-idempotency.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { readHostLease, writeHostLease } from "../src/topology/host-leases.js";
import { readParticipantFile, writeParticipantFile } from "../src/topology/participant-files.js";

const removals = vi.hoisted(() => [] as string[]);
vi.mock("../src/topology/host-leases.js", async importOriginal => {
  const actual = await importOriginal<typeof import("../src/topology/host-leases.js")>();
  return { ...actual, removeHostLeaseIf: (root: string, id: string, owned: Parameters<typeof actual.removeHostLeaseIf>[2]) => {
    const lease = actual.readHostLease(root, id);
    if (!lease || !owned(lease)) return false;
    removals.push(`${root}:${id}`);
    return true;
  } };
});
const roots: string[] = [];
const closers: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const close of closers.splice(0).reverse()) await close();
  vi.restoreAllMocks();
  expect(removals).toEqual([]);
  removals.length = 0;
  // Hygiene is confined to this test's own private mkdtemp children.
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const key = (prefix: string, id: string): string => prefix + createHash("sha256").update(id).digest("hex");
const events = (store: MeshStore, topic: string) => store.read({ topic, after: 0, limit: 100 });
const markerFiles = (root: string, marker: string): string[] => {
  const found: string[] = [];
  const walk = (dir: string): void => {
    for (const name of fs.readdirSync(dir)) {
      const file = path.join(dir, name);
      try {
        const stat = fs.lstatSync(file);
        expect(stat.isSymbolicLink()).toBe(false);
        if (stat.isDirectory()) walk(file);
        else if (stat.isFile() && fs.readFileSync(file).includes(Buffer.from(marker))) found.push(path.relative(root, file));
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
      }
    }
  };
  walk(root);
  return found;
};
const bodyFreeReceipts = (hub: MeshStore): void => {
  for (const receipt of events(hub, "fabric.control.command")) {
    for (const field of ["data", "text", "message"]) expect(receipt).not.toHaveProperty(field);
  }
};
const native = async (store: MeshStore, name: string, incarnationToken?: string) => {
  const now = Date.now();
  const identity: MeshIdentity = { id: `session:${name}-00000000`, name: "main", kind: "main", sessionId: name };
  const ownerIncarnation = `fixture:${identity.id}`;
  await store.put({ key: key("topology/hosts/", identity.id), identity, value: {
    format: 1, id: identity.id, rootId: identity.id, identity, startedAt: now, updatedAt: now, expiresAt: now + 120_000,
    ...(incarnationToken !== undefined ? { incarnationToken } : {}),
  } });
  writeHostLease(store.root, { id: identity.id, rootId: identity.id, identityId: identity.id,
    startedAt: now, updatedAt: now, expiresAt: now + 120_000,
    ...(incarnationToken !== undefined ? { incarnationToken } : {}) });
  writeParticipantFile(store.root, { key: key("topology/participants/", identity.id), version: 1, updatedAt: now, updatedBy: identity, value: {
    format: 1, id: identity.id, kind: "root", rootId: identity.id, ownerHostId: identity.id, ownerIdentityId: identity.id,
    ownerIncarnation, name, label: name.toUpperCase(), status: "idle", runner: "pi", transport: "host",
    capabilities: ["steer", "followUp"], sessionId: name, startedAt: now, updatedAt: now, controlProtocol: "v1",
  } });
  return { identity, ownerIncarnation, startedAt: now, incarnationToken };
};

type RelayHook = (deliver: () => Promise<void>, event: BridgePublish) => Promise<void>;
const fixture = async (hook?: RelayHook, targetToken?: string) => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "mbr-safe-"));
  fs.chmodSync(dir, 0o700);
  roots.push(dir);
  const stores = Object.fromEntries(["ryzen1", "ryzen2", "ryzen3"].map(name => [name,
    new MeshStore(path.join(dir, name), 64 * 1024, 100, { stateBackend: "file" })]));
  const mains = Object.fromEntries(await Promise.all(Object.entries(stores).map(async ([name, store]) =>
    [name, await native(store, name, name === "ryzen2" ? targetToken : undefined)] as const)));
  const links: Array<{ bridge: MeshBridge; remote: RemoteBridgeSide }> = ["ryzen2", "ryzen3"].map(name => {
    const input = new PassThrough(), output = new PassThrough();
    const serving = serveBridgeAgent(new StoreBridgeSide(stores[name]!, "ryzen1"), input, output);
    const remote = new RemoteBridgeSide(output, input);
    closers.push(async () => { remote.close(); input.end(); await serving; output.end(); });
    const bridge = new MeshBridge({ localName: "ryzen1", remoteName: name,
      local: new StoreBridgeSide(stores.ryzen1!, name), remote, cursorPath: path.join(dir, `${name}.json`), presenceMs: 0,
      relay: async (destination, event) => {
        const target = links.find(link => link.bridge.options.remoteName === destination);
        if (!target) throw new Error("Missing test destination");
        const deliver = () => target.bridge.deliverRelay(name, event);
        if (hook) await hook(deliver, event); else await deliver();
      } });
    return { bridge, remote };
  });
  for (const link of links) await link.bridge.start();
  for (const link of links) await link.bridge.syncPresence();
  const drain = async (): Promise<void> => { for (const link of links) await link.bridge.step(); };
  return { dir, stores, mains, links, drain };
};
const senderFor = (f: Awaited<ReturnType<typeof fixture>>, mirrorLeaseWaitMs?: number) => {
  const a = f.stores.ryzen3!, source = f.mains.ryzen3!;
  const directory = new ParticipantDirectory(a, { enabled: true, hostId: source.identity.id, rootId: source.identity.id, identity: source.identity });
  const sender = new FabricControlPlane(a, source.identity, { enabled: true, hostId: source.identity.id,
    ...(mirrorLeaseWaitMs !== undefined ? { mirrorLeaseWaitMs } : {}),
    readMirroredOwner: (...args) => directory.mirroredControlOwner(...args) });
  sender.start(() => ({ accepted: false }));
  closers.push(() => sender.close());
  return sender;
};
const unavailable = (): Error => Object.assign(new Error("Injected unavailable relay response"), {
  code: "FABRIC_RELAY_DESTINATION_UNAVAILABLE", retryable: true });

describe("local singleton relay data safety", () => {
  it("delivers fresh full text/message/data A -> B, keeping body markers out of all hub files", async () => {
    const f = await fixture();
    const a = f.stores.ryzen3!, b = f.stores.ryzen2!, hub = f.stores.ryzen1!;
    const source = f.mains.ryzen3!, target = f.mains.ryzen2!;
    const marker = "local-relay-fresh-private-body";
    const receiver = new FabricControlPlane(b, target.identity, { enabled: true, hostId: target.identity.id, ownerIncarnation: target.ownerIncarnation });
    const receive = vi.fn(() => ({ accepted: true, messageId: "fixed-delivery-receipt" }));
    receiver.start(receive);
    closers.push(() => receiver.close());
    await a.publish({ topic: "fabric.control.command", kind: "followUp", from: source.identity, to: target.identity.id, text: marker,
      data: { version: 1, commandId: "fresh-command", targetId: target.identity.id, operation: "followUp", replyTo: source.identity.id,
        requestedAt: Date.now(), deadlineAt: Date.now() + 60_000, ownerIncarnation: target.ownerIncarnation,
        destinationRemoteHost: "ryzen2", message: marker, data: { nested: { marker } } } });
    await f.drain();
    await vi.waitFor(() => expect(events(b, "fabric.control.ack")).toHaveLength(1));
    await f.drain(); await f.drain();
    expect(receive).toHaveBeenCalledOnce();
    expect(receive.mock.calls[0]).toEqual(expect.arrayContaining([expect.objectContaining({ message: marker, data: { nested: { marker } } }),
      expect.objectContaining({ id: source.identity.id, verified: "bridge" })]));
    expect(events(b, "fabric.control.command")).toHaveLength(1);
    expect(events(b, "fabric.control.command")[0]).toMatchObject({ text: marker, verification: "bridge", from: source.identity,
      data: { bridge: { from: "ryzen3", via: "ryzen1" } } });
    expect(events(a, "fabric.control.ack")[0]).toMatchObject({ data: { accepted: true, messageId: "fixed-delivery-receipt" } });
    expect(markerFiles(a.root, marker).length).toBeGreaterThan(0);
    expect(markerFiles(b.root, marker).length).toBeGreaterThan(0);
    expect(markerFiles(hub.root, marker)).toEqual([]);
    bodyFreeReceipts(hub);
    for (const link of f.links) expect(fs.readFileSync(link.bridge.options.cursorPath, "utf8")).not.toContain(marker);
  });

  it("strips singleton sensitive ACK text/result at both the hub and A", async () => {
    const f = await fixture();
    const a = f.stores.ryzen3!, b = f.stores.ryzen2!, hub = f.stores.ryzen1!;
    const source = f.mains.ryzen3!, target = f.mains.ryzen2!;
    const marker = "local-relay-private-ack-text-and-result";
    await b.publish({ topic: "fabric.control.ack", kind: "accepted", from: target.identity, to: source.identity.id, text: marker,
      data: { version: 1, commandId: "singleton-ack", targetId: target.identity.id, ownerIncarnation: target.ownerIncarnation,
        accepted: true, messageId: "unrelated-ack-id", result: { marker }, triggered: true } });
    expect(markerFiles(b.root, marker).length).toBeGreaterThan(0);
    await f.drain(); await f.drain();
    for (const store of [hub, a]) {
      expect(events(store, "fabric.control.ack")).toHaveLength(1);
      const ack = events(store, "fabric.control.ack")[0]!;
      expect(ack).not.toHaveProperty("text");
      expect(ack.data).not.toHaveProperty("result");
      expect(ack.data).not.toHaveProperty("triggered");
      expect(ack).toMatchObject({ from: target.identity, data: { accepted: true, messageId: "unrelated-ack-id" } });
      expect(markerFiles(store.root, marker)).toEqual([]);
    }
  });

  it("returns typed retryable destination-down refusal without any real destination teardown", async () => {
    const f = await fixture(async () => { throw unavailable(); });
    const a = f.stores.ryzen3!, b = f.stores.ryzen2!, hub = f.stores.ryzen1!;
    const source = f.mains.ryzen3!, target = f.mains.ryzen2!;
    const sender = senderFor(f), marker = "local-relay-down-private-body";
    const pending = sender.request(target.identity.id, target.identity.id, "followUp",
      { message: marker, data: { marker }, ownerIncarnation: target.ownerIncarnation }, target.identity.id,
      { routedRemoteHost: "ryzen2", idempotencyKey: "down-stable-key" });
    void pending.catch(() => undefined);
    await vi.waitFor(() => expect(events(a, "fabric.control.command")).toHaveLength(1));
    await f.drain();
    await expect(pending).rejects.toMatchObject({ code: "FABRIC_RELAY_DESTINATION_UNAVAILABLE", retryable: true, idempotencyKey: "down-stable-key" });
    const ack = events(a, "fabric.control.ack")[0]!;
    expect(ack).toMatchObject({ from: { id: "bridge:ryzen1", name: "mesh-bridge", kind: "main" }, verification: "bridge", to: source.identity.id,
      data: { accepted: false, retryable: true, errorCode: "FABRIC_RELAY_DESTINATION_UNAVAILABLE", bridge: { from: "ryzen1" } } });
    expect(ack).not.toHaveProperty("text");
    for (const field of ["ownerIncarnation", "notRun", "notDelivered", "result", "message"]) expect(ack.data).not.toHaveProperty(field);
    expect(events(b, "fabric.control.command")).toEqual([]);
    expect(events(hub, "fabric.control.command")).toEqual([]);
    expect(events(hub, "fabric.control.ack")).toEqual([]);
    expect(markerFiles(a.root, marker).length).toBeGreaterThan(0);
    expect(markerFiles(b.root, marker)).toEqual([]);
    expect(markerFiles(hub.root, marker)).toEqual([]);
  });

  it("retries one lost response with the same caller key: one B append and one real handler delivery", async () => {
    let calls = 0;
    const f = await fixture(async deliver => { await deliver(); if (++calls === 1) throw unavailable(); });
    const a = f.stores.ryzen3!, b = f.stores.ryzen2!, hub = f.stores.ryzen1!, target = f.mains.ryzen2!;
    const sender = senderFor(f), marker = "local-relay-lost-response-private-body";
    const input = { message: marker, data: { marker }, ownerIncarnation: target.ownerIncarnation };
    const options = { routedRemoteHost: "ryzen2", idempotencyKey: "lost-response-stable-key" };
    const receiver = new FabricControlPlane(b, target.identity, { enabled: true, hostId: target.identity.id, ownerIncarnation: target.ownerIncarnation });
    const receive = vi.fn(() => ({ accepted: true, messageId: "single-handler-receipt" }));
    closers.push(() => receiver.close());
    const first = sender.request(target.identity.id, target.identity.id, "followUp", input, target.identity.id, options);
    void first.catch(() => undefined);
    await vi.waitFor(() => expect(events(a, "fabric.control.command")).toHaveLength(1));
    await f.drain();
    expect(events(b, "fabric.control.command")).toHaveLength(1);
    await expect(first).rejects.toMatchObject({ code: "FABRIC_RELAY_DESTINATION_UNAVAILABLE", retryable: true });
    const retry = sender.request(target.identity.id, target.identity.id, "followUp", input, target.identity.id, options);
    void retry.catch(() => undefined);
    await vi.waitFor(() => expect(events(a, "fabric.control.command")).toHaveLength(2));
    const commands = events(a, "fabric.control.command");
    expect((commands[0]!.data as { commandId: string }).commandId).toBe((commands[1]!.data as { commandId: string }).commandId);
    await f.drain();
    expect(calls).toBe(2);
    expect(events(b, "fabric.control.command")).toHaveLength(1);
    // Delaying execution until the retry is pending avoids racing the owner's ACK
    // against a caller that has not yet registered the new observation.
    receiver.start(receive);
    await vi.waitFor(() => expect(events(b, "fabric.control.ack")).toHaveLength(1));
    await f.drain(); await f.drain();
    await expect(retry).resolves.toMatchObject({ acknowledged: true, messageId: "single-handler-receipt" });
    expect(receive).toHaveBeenCalledOnce();
    expect(events(b, "fabric.control.command")).toHaveLength(1);
    expect(markerFiles(b.root, marker).length).toBeGreaterThan(0);
    expect(markerFiles(hub.root, marker)).toEqual([]);
    bodyFreeReceipts(hub);
  });

  it("rejects changed nested data against authority-only recovery, but dedupes reordered identical JSON at B", async () => {
    let calls = 0;
    const f = await fixture(async deliver => { await deliver(); if (++calls === 1) throw unavailable(); });
    const a = f.stores.ryzen3!, b = f.stores.ryzen2!, hub = f.stores.ryzen1!, target = f.mains.ryzen2!;
    const sender = senderFor(f), marker = "local-authority-only-original-body";
    let now = Date.now();
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const input = { message: marker, data: { nested: { marker, count: 1 }, list: [{ a: 1, b: 2 }] }, ownerIncarnation: target.ownerIncarnation };
    const options = { routedRemoteHost: "ryzen2", idempotencyKey: "authority-only-body-key" };
    const first = sender.request(target.identity.id, target.identity.id, "followUp", input, target.identity.id, options);
    void first.catch(() => undefined);
    await vi.waitFor(() => expect(events(a, "fabric.control.command")).toHaveLength(1));
    await f.drain();
    await expect(first).rejects.toMatchObject({ code: "FABRIC_RELAY_DESTINATION_UNAVAILABLE" });
    expect(sender.relayAcknowledgementRecoverySnapshot()).toMatchObject({ retained: 1, acknowledged: 0 });
    const conflict = sender.request(target.identity.id, target.identity.id, "followUp",
      { ...input, data: { ...input.data, nested: { ...input.data.nested, count: 2 } } }, target.identity.id, options);
    await expect(conflict).rejects.toBeInstanceOf(FabricIdempotencyKeyReusedError);
    await expect(conflict).rejects.toMatchObject({ name: "FabricIdempotencyKeyReusedError", code: FABRIC_IDEMPOTENCY_KEY_REUSED,
      retryable: false, idempotencyKey: options.idempotencyKey, message: "Fabric idempotency key reused with different input" });
    expect(events(a, "fabric.control.command")).toHaveLength(1);
    expect(events(b, "fabric.control.command")).toHaveLength(1);
    expect(calls).toBe(1);
    expect(sender.relayAcknowledgementRecoverySnapshot()).toMatchObject({ retained: 1, acknowledged: 0 });
    now += 1_000; // fresh attempt times, still inside the original absolute recovery window
    const retry = sender.request(target.identity.id, target.identity.id, "followUp",
      { ownerIncarnation: target.ownerIncarnation, data: { list: [{ b: 2, a: 1 }], nested: { count: 1, marker } }, message: marker },
      target.identity.id, options);
    void retry.catch(() => undefined);
    await vi.waitFor(() => expect(events(a, "fabric.control.command")).toHaveLength(2));
    const commands = events(a, "fabric.control.command");
    expect(commands[0]!.id).not.toBe(commands[1]!.id);
    expect((commands[0]!.data as { commandId: string }).commandId).toBe((commands[1]!.data as { commandId: string }).commandId);
    const originalAttempt = commands[0]!.data as { requestCreatedAt: number; requestedAt: number; deadlineAt: number };
    const freshAttempt = commands[1]!.data as typeof originalAttempt;
    expect(freshAttempt.requestCreatedAt - originalAttempt.requestCreatedAt).toBe(1_000);
    expect(freshAttempt.requestedAt - originalAttempt.requestedAt).toBe(1_000);
    expect(freshAttempt.deadlineAt - originalAttempt.deadlineAt).toBe(1_000);
    await f.drain();
    expect(calls).toBe(2);
    expect(events(b, "fabric.control.command")).toHaveLength(1);
    const receiver = new FabricControlPlane(b, target.identity, { enabled: true, hostId: target.identity.id, ownerIncarnation: target.ownerIncarnation });
    const receive = vi.fn(() => ({ accepted: true, messageId: "authority-only-original-receipt" }));
    receiver.start(receive);
    closers.push(() => receiver.close());
    await vi.waitFor(() => expect(events(b, "fabric.control.ack")).toHaveLength(1));
    await f.drain(); await f.drain();
    await expect(retry).resolves.toMatchObject({ acknowledged: true, messageId: "authority-only-original-receipt" });
    expect(receive).toHaveBeenCalledOnce();
    expect(events(b, "fabric.control.command")[0]!.data).toMatchObject({ message: marker, data: input.data });
    expect(markerFiles(hub.root, marker)).toEqual([]);
    bodyFreeReceipts(hub);
  });

  it("snapshots nested caller input before a delayed real source publication", async () => {
    const f = await fixture();
    const a = f.stores.ryzen3!, b = f.stores.ryzen2!, target = f.mains.ryzen2!;
    const sender = senderFor(f), marker = "local-snapshot-original-body";
    const input = { message: marker, data: { nested: { marker, count: 1 } }, ownerIncarnation: target.ownerIncarnation };
    const publish = a.publish.bind(a);
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    vi.spyOn(a, "publish").mockImplementationOnce(async publication => { await gate; return publish(publication); });
    const pending = sender.request(target.identity.id, target.identity.id, "followUp", input, target.identity.id,
      { routedRemoteHost: "ryzen2", idempotencyKey: "immutable-source-body-key" });
    void pending.catch(() => undefined);
    input.message = "caller-mutated-message";
    input.data.nested.marker = "caller-mutated-nested-data";
    input.data.nested.count = 2;
    release();
    await vi.waitFor(() => expect(events(a, "fabric.control.command")).toHaveLength(1));
    expect(events(a, "fabric.control.command")[0]!.data).toMatchObject({ message: marker, data: { nested: { marker, count: 1 } } });
    const receiver = new FabricControlPlane(b, target.identity, { enabled: true, hostId: target.identity.id, ownerIncarnation: target.ownerIncarnation });
    const receive = vi.fn(() => ({ accepted: true, messageId: "immutable-source-receipt" }));
    receiver.start(receive);
    closers.push(() => receiver.close());
    await f.drain();
    await vi.waitFor(() => expect(events(b, "fabric.control.ack")).toHaveLength(1));
    await f.drain(); await f.drain();
    await expect(pending).resolves.toMatchObject({ acknowledged: true, messageId: "immutable-source-receipt" });
    expect(events(b, "fabric.control.command")[0]!.data).toMatchObject({ message: marker, data: { nested: { marker, count: 1 } } });
    expect(receive).toHaveBeenCalledOnce();
    expect(markerFiles(f.stores.ryzen1!.root, marker)).toEqual([]);
  });

  it.each(["absent", "expired"] as const)("propagates a real destination body conflict to A when source recovery is %s", async recovery => {
    let calls = 0;
    const f = await fixture(async deliver => { calls += 1; await deliver(); if (calls === 1) throw unavailable(); });
    const a = f.stores.ryzen3!, b = f.stores.ryzen2!, hub = f.stores.ryzen1!, source = f.mains.ryzen3!, target = f.mains.ryzen2!;
    const sender = senderFor(f), marker = `local-destination-conflict-original-${recovery}`, changedMarker = `local-destination-conflict-changed-${recovery}`;
    let now = Date.now();
    if (recovery === "expired") vi.spyOn(Date, "now").mockImplementation(() => now);
    const input = { message: marker, data: { nested: { marker, count: 1 } }, ownerIncarnation: target.ownerIncarnation };
    const options = { routedRemoteHost: "ryzen2", idempotencyKey: `destination-conflict-${recovery}-key` };
    const first = sender.request(target.identity.id, target.identity.id, "followUp", input, target.identity.id, options);
    void first.catch(() => undefined);
    await vi.waitFor(() => expect(events(a, "fabric.control.command")).toHaveLength(1));
    await f.drain();
    await expect(first).rejects.toMatchObject({ code: "FABRIC_RELAY_DESTINATION_UNAVAILABLE" });
    expect(events(b, "fabric.control.command")).toHaveLength(1);
    const retrySender = recovery === "absent" ? senderFor(f) : sender;
    if (recovery === "expired") now += 8_000;
    expect(retrySender.relayAcknowledgementRecoverySnapshot()).toMatchObject({ retained: 0, acknowledged: 0 });
    const conflict = retrySender.request(target.identity.id, target.identity.id, "followUp",
      { ...input, data: { nested: { marker: changedMarker, count: 2 } } }, target.identity.id, options);
    void conflict.catch(() => undefined);
    if (recovery === "expired") {
      await expect(conflict).rejects.toBeInstanceOf(FabricIdempotencyKeyReusedError);
      await expect(conflict).rejects.toMatchObject({ name: "FabricIdempotencyKeyReusedError", code: FABRIC_IDEMPOTENCY_KEY_REUSED,
        retryable: false, idempotencyKey: options.idempotencyKey, message: "Fabric idempotency key reused with different input" });
      expect(events(a, "fabric.control.command")).toHaveLength(1); // Origin refused before publication.
      expect(calls).toBe(1);
      expect(events(b, "fabric.control.command")).toHaveLength(1);
      expect(events(b, "fabric.control.command")[0]!.data).toMatchObject({ message: marker, data: input.data });
      expect(markerFiles(b.root, changedMarker)).toEqual([]);
      bodyFreeReceipts(hub);
      return;
    }
    await vi.waitFor(() => expect(events(a, "fabric.control.command")).toHaveLength(2));
    await f.drain();
    await expect(conflict).rejects.toBeInstanceOf(FabricIdempotencyKeyReusedError);
    await expect(conflict).rejects.toMatchObject({ name: "FabricIdempotencyKeyReusedError", code: FABRIC_IDEMPOTENCY_KEY_REUSED,
      retryable: false, idempotencyKey: options.idempotencyKey, message: "Fabric idempotency key reused with different input" });
    expect(calls).toBe(2);
    expect(events(a, "fabric.control.command")).toHaveLength(2);
    expect(events(b, "fabric.control.command")).toHaveLength(1);
    expect(events(b, "fabric.control.command")[0]!.data).toMatchObject({ message: marker, data: input.data });
    const refusal = events(a, "fabric.control.ack").find(event => (event.data as { errorCode?: string }).errorCode === FABRIC_IDEMPOTENCY_KEY_REUSED)!;
    expect(refusal).toMatchObject({ from: { id: "bridge:ryzen1", name: "mesh-bridge", kind: "main" }, verification: "bridge", to: source.identity.id,
      data: { version: 1, targetId: target.identity.id, accepted: false, errorCode: FABRIC_IDEMPOTENCY_KEY_REUSED, retryable: false,
        error: "Fabric idempotency key reused with different input", bridge: { from: "ryzen1", id: events(a, "fabric.control.command")[1]!.id } } });
    expect(refusal).not.toHaveProperty("text");
    for (const field of ["ownerIncarnation", "notRun", "notDelivered", "result", "message", "dedupeFingerprint", "bodyFingerprint"]) expect(refusal.data).not.toHaveProperty(field);
    expect(retrySender.relayAcknowledgementRecoverySnapshot()).toMatchObject({ retained: 0, acknowledged: 0 });
    expect(events(hub, "fabric.control.command")).toEqual([]);
    expect(events(hub, "fabric.control.ack")).toEqual([]);
    expect(markerFiles(b.root, changedMarker)).toEqual([]);
    for (const privateMarker of [marker, changedMarker]) expect(markerFiles(hub.root, privateMarker)).toEqual([]);
    const receiver = new FabricControlPlane(b, target.identity, { enabled: true, hostId: target.identity.id, ownerIncarnation: target.ownerIncarnation });
    const receive = vi.fn(() => ({ accepted: true, messageId: "destination-original-body-receipt" }));
    receiver.start(receive);
    closers.push(() => receiver.close());
    await vi.waitFor(() => expect(events(b, "fabric.control.ack")).toHaveLength(1));
    await f.drain(); await f.drain();
    expect(receive).toHaveBeenCalledOnce();
    expect(events(b, "fabric.control.command")).toHaveLength(1);
    bodyFreeReceipts(hub);
  });

  it("keeps hub-local followUp delivery and full command body unchanged", async () => {
    const f = await fixture();
    const a = f.stores.ryzen3!, b = f.stores.ryzen2!, hub = f.stores.ryzen1!;
    const source = f.mains.ryzen3!, local = f.mains.ryzen1!;
    const sender = senderFor(f), marker = "local-hub-followUp-private-body";
    const receiver = new FabricControlPlane(hub, local.identity, { enabled: true, hostId: local.identity.id, ownerIncarnation: local.ownerIncarnation });
    const receive = vi.fn(() => ({ accepted: true, messageId: "hub-local-fixed-receipt" }));
    receiver.start(receive);
    closers.push(() => receiver.close());
    const pending = sender.request(local.identity.id, local.identity.id, "followUp",
      { message: marker, data: { marker }, ownerIncarnation: local.ownerIncarnation }, local.identity.id,
      { routedRemoteHost: "ryzen1", idempotencyKey: "hub-local-fixed-key" });
    void pending.catch(() => undefined);
    await vi.waitFor(() => expect(events(a, "fabric.control.command")).toHaveLength(1));
    await f.drain();
    await vi.waitFor(() => expect(events(hub, "fabric.control.ack")).toHaveLength(1));
    await f.drain();
    await expect(pending).resolves.toMatchObject({ acknowledged: true, messageId: "hub-local-fixed-receipt" });
    expect(receive).toHaveBeenCalledOnce();
    expect(receive.mock.calls[0]).toEqual(expect.arrayContaining([expect.objectContaining({ operation: "followUp", message: marker, data: { marker } }),
      expect.objectContaining({ id: source.identity.id, verified: "bridge" })]));
    expect(events(hub, "fabric.control.command")).toHaveLength(1);
    expect(markerFiles(a.root, marker).length).toBeGreaterThan(0);
    expect(markerFiles(hub.root, marker).length).toBeGreaterThan(0);
    expect(markerFiles(b.root, marker)).toEqual([]);
    expect(events(b, "fabric.control.command")).toEqual([]);
  });

  it("keeps a hub-native ACK's full text/result as the routing counterexample", async () => {
    const f = await fixture();
    const a = f.stores.ryzen3!, b = f.stores.ryzen2!, hub = f.stores.ryzen1!;
    const target = f.mains.ryzen2!, local = f.mains.ryzen1!, marker = "local-hub-native-private-ack";
    await b.publish({ topic: "fabric.control.ack", kind: "accepted", from: target.identity, to: local.identity.id, text: marker,
      data: { version: 1, commandId: "hub-native-ack", targetId: target.identity.id, accepted: true, result: { marker }, triggered: true } });
    await f.drain(); await f.drain();
    expect(events(hub, "fabric.control.ack")).toHaveLength(1);
    expect(events(hub, "fabric.control.ack")[0]).toMatchObject({ text: marker, data: { result: { marker }, triggered: true } });
    expect(markerFiles(hub.root, marker).length).toBeGreaterThan(0);
    expect(markerFiles(b.root, marker).length).toBeGreaterThan(0);
    expect(events(a, "fabric.control.ack")).toEqual([]);
    expect(markerFiles(a.root, marker)).toEqual([]);
  });

  it.each(["result", "message", "extra", "text", "via"])("ignores forbidden %s on a hub refusal, leaving a genuine owner ACK to settle", async field => {
    const f = await fixture();
    const a = f.stores.ryzen3!, source = f.mains.ryzen3!, target = f.mains.ryzen2!;
    const sender = senderFor(f);
    const pending = sender.request(target.identity.id, target.identity.id, "followUp", { message: "refusal-admission", ownerIncarnation: target.ownerIncarnation },
      target.identity.id, { routedRemoteHost: "ryzen2", idempotencyKey: `forbidden-${field}` });
    void pending.catch(() => undefined);
    await vi.waitFor(() => expect(events(a, "fabric.control.command")).toHaveLength(1));
    const command = events(a, "fabric.control.command")[0]!, commandId = (command.data as { commandId: string }).commandId;
    const ingress = new StoreBridgeSide(a, "ryzen1");
    const bridge = { from: "ryzen1", id: command.id, ...(field === "via" ? { via: "ryzen1" } : {}) };
    await ingress.publish({ topic: "fabric.control.ack", kind: "rejected", from: { id: "bridge:ryzen1", name: "mesh-bridge", kind: "main" },
      to: source.identity.id, ...(field === "text" ? { text: "forbidden-refusal-text" } : {}),
      data: { version: 1, commandId, targetId: target.identity.id, accepted: false, error: "Relay destination unavailable",
        errorCode: "FABRIC_RELAY_DESTINATION_UNAVAILABLE", retryable: true, bridge,
        ...(["result", "message", "extra"].includes(field) ? { [field]: "forbidden-refusal-body" } : {}) } }, [], true);
    await ingress.publish({ topic: "fabric.control.ack", kind: "accepted", from: target.identity, to: source.identity.id,
      data: { version: 1, commandId, targetId: target.identity.id, ownerIncarnation: target.ownerIncarnation, accepted: true,
        messageId: "genuine-owner-ack", bridge: { from: "ryzen2", id: "genuine-owner-event", via: "ryzen1" } } }, [], true);
    await expect(pending).resolves.toMatchObject({ acknowledged: true, messageId: "genuine-owner-ack" });
  });

  it.each(["retryable", "error"] as const)("ignores a malformed idempotency-refusal %s combination and still accepts the owner ACK", async field => {
    const f = await fixture();
    const a = f.stores.ryzen3!, source = f.mains.ryzen3!, target = f.mains.ryzen2!, sender = senderFor(f);
    const pending = sender.request(target.identity.id, target.identity.id, "followUp", { message: "typed-refusal-admission", ownerIncarnation: target.ownerIncarnation },
      target.identity.id, { routedRemoteHost: "ryzen2", idempotencyKey: `invalid-idempotency-refusal-${field}` });
    void pending.catch(() => undefined);
    await vi.waitFor(() => expect(events(a, "fabric.control.command")).toHaveLength(1));
    const command = events(a, "fabric.control.command")[0]!, commandId = (command.data as { commandId: string }).commandId;
    const ingress = new StoreBridgeSide(a, "ryzen1");
    await ingress.publish({ topic: "fabric.control.ack", kind: "rejected", from: { id: "bridge:ryzen1", name: "mesh-bridge", kind: "main" }, to: source.identity.id,
      data: { version: 1, commandId, targetId: target.identity.id, accepted: false, errorCode: FABRIC_IDEMPOTENCY_KEY_REUSED,
        error: field === "error" ? "Untrusted replacement error" : "Fabric idempotency key reused with different input",
        retryable: field === "retryable", bridge: { from: "ryzen1", id: command.id } } }, [], true);
    await ingress.publish({ topic: "fabric.control.ack", kind: "accepted", from: target.identity, to: source.identity.id,
      data: { version: 1, commandId, targetId: target.identity.id, ownerIncarnation: target.ownerIncarnation, accepted: true,
        messageId: "owner-after-invalid-idempotency-refusal", bridge: { from: "ryzen2", id: "valid-owner-after-invalid-refusal", via: "ryzen1" } } }, [], true);
    await expect(pending).resolves.toMatchObject({ acknowledged: true, messageId: "owner-after-invalid-idempotency-refusal" });
    expect(sender.relayAcknowledgementRecoverySnapshot()).toMatchObject({ retained: 0, acknowledged: 0 });
  });

  it.each(["fresh", "bridge-stale"] as const)("recovers the real owner ACK consumed at A before same-key retry registration (%s mirror)", async mirrorState => {
    let calls = 0;
    const f = await fixture(async deliver => { await deliver(); if (++calls === 1) throw unavailable(); });
    const a = f.stores.ryzen3!, b = f.stores.ryzen2!, hub = f.stores.ryzen1!, target = f.mains.ryzen2!;
    const sender = senderFor(f, 25), marker = "local-relay-ack-before-retry-private-body";
    const input = { message: marker, data: { marker }, ownerIncarnation: target.ownerIncarnation };
    const options = { routedRemoteHost: "ryzen2", idempotencyKey: "ack-before-retry-stable-key" };
    const receiver = new FabricControlPlane(b, target.identity, { enabled: true, hostId: target.identity.id, ownerIncarnation: target.ownerIncarnation });
    const receive = vi.fn(() => ({ accepted: true, messageId: "already-executed-owner-receipt" }));
    closers.push(() => receiver.close());
    const first = sender.request(target.identity.id, target.identity.id, "followUp", input, target.identity.id, options);
    void first.catch(() => undefined);
    await vi.waitFor(() => expect(events(a, "fabric.control.command")).toHaveLength(1));
    await f.drain();
    await expect(first).rejects.toMatchObject({ code: "FABRIC_RELAY_DESTINATION_UNAVAILABLE", retryable: true });
    expect(sender.relayAcknowledgementRecoverySnapshot()).toEqual({ retained: 1, acknowledged: 0, ttlMs: 8_000, limit: 256 });
    // Actual B handler runs only AFTER failure cleared A's pending. Its actual ACK then
    // crosses both real bridges, and the diagnostic proves A consumed it before retry.
    receiver.start(receive);
    await vi.waitFor(() => expect(events(b, "fabric.control.ack")).toHaveLength(1));
    await f.drain(); await f.drain();
    await vi.waitFor(() => expect(sender.relayAcknowledgementRecoverySnapshot().acknowledged).toBe(1));
    expect(events(a, "fabric.control.ack")).toHaveLength(2); // refusal + ordinary owner ACK
    if (mirrorState === "bridge-stale") {
      const lease = readHostLease(a.root, target.identity.id)!;
      expect(lease.mirror).toBeDefined();
      const expiredAt = Date.now() - 1;
      writeHostLease(a.root, { ...lease, expiresAt: expiredAt,
        mirror: { ...lease.mirror!, observedAt: expiredAt - 15_000 } });
    }
    let result: Awaited<ReturnType<typeof sender.request>> | undefined;
    const retry = sender.request(target.identity.id, target.identity.id, "followUp", input, target.identity.id, options)
      .then(value => { result = value; return value; });
    void retry.catch(() => undefined);
    await vi.waitFor(() => expect(result).toMatchObject({ acknowledged: true, messageId: "already-executed-owner-receipt" }));
    await expect(retry).resolves.toBe(result);
    expect(events(a, "fabric.control.command")).toHaveLength(1); // cache hit does not republish
    expect(events(b, "fabric.control.command")).toHaveLength(1);
    expect(receive).toHaveBeenCalledOnce();
    expect(calls).toBe(1);
    if (mirrorState === "bridge-stale") {
      // A cache miss still needs #1070's fresh mirror admission; no second append or handler run.
      await expect(sender.request(target.identity.id, target.identity.id, "followUp", input, target.identity.id,
        { ...options, idempotencyKey: "uncached-stale-mirror-key" }))
        .rejects.toMatchObject({ name: "FabricPeerLeaseStaleError" });
      expect(events(a, "fabric.control.command")).toHaveLength(1);
      expect(events(b, "fabric.control.command")).toHaveLength(1);
      expect(receive).toHaveBeenCalledOnce();
    }
    expect(markerFiles(a.root, marker).length).toBeGreaterThan(0);
    expect(markerFiles(b.root, marker).length).toBeGreaterThan(0);
    expect(markerFiles(hub.root, marker)).toEqual([]);
    bodyFreeReceipts(hub);
  });

  it("rejects changed body against a real cached late ACK without publishing, then accepts reordered identical input", async () => {
    let calls = 0;
    const f = await fixture(async deliver => { await deliver(); if (++calls === 1) throw unavailable(); });
    const a = f.stores.ryzen3!, b = f.stores.ryzen2!, hub = f.stores.ryzen1!, target = f.mains.ryzen2!;
    const sender = senderFor(f), marker = "local-cached-late-ack-original-body";
    const input = { message: marker, data: { nested: { marker, count: 1 }, list: [{ a: 1, b: 2 }] }, ownerIncarnation: target.ownerIncarnation,
      principal: { id: "cache-body-principal", binding: "herdr-client" as const } };
    const options = { routedRemoteHost: "ryzen2", idempotencyKey: "cached-late-ack-body-key" };
    const receiver = new FabricControlPlane(b, target.identity, { enabled: true, hostId: target.identity.id, ownerIncarnation: target.ownerIncarnation });
    const receive = vi.fn(() => ({ accepted: true, messageId: "cached-original-body-receipt" }));
    closers.push(() => receiver.close());
    const first = sender.request(target.identity.id, target.identity.id, "followUp", input, target.identity.id, options);
    void first.catch(() => undefined);
    await vi.waitFor(() => expect(events(a, "fabric.control.command")).toHaveLength(1));
    await f.drain();
    await expect(first).rejects.toMatchObject({ code: "FABRIC_RELAY_DESTINATION_UNAVAILABLE" });
    receiver.start(receive);
    await vi.waitFor(() => expect(events(b, "fabric.control.ack")).toHaveLength(1));
    await f.drain(); await f.drain();
    await vi.waitFor(() => expect(sender.relayAcknowledgementRecoverySnapshot().acknowledged).toBe(1));
    const publish = vi.spyOn(a, "publish"), controller = new AbortController();
    const abortListener = vi.spyOn(controller.signal, "addEventListener");
    for (const changed of [
      { ...input, message: "local-cached-changed-message" },
      { ...input, data: { ...input.data, nested: { ...input.data.nested, count: 2 } } },
      { ...input, principal: { ...input.principal, id: "cache-changed-principal" } },
    ]) {
      const conflict = sender.request(target.identity.id, target.identity.id, "followUp", changed, target.identity.id,
        { ...options, signal: controller.signal });
      await expect(conflict).rejects.toBeInstanceOf(FabricIdempotencyKeyReusedError);
      await expect(conflict).rejects.toMatchObject({ name: "FabricIdempotencyKeyReusedError", code: FABRIC_IDEMPOTENCY_KEY_REUSED,
        retryable: false, idempotencyKey: options.idempotencyKey, message: "Fabric idempotency key reused with different input" });
    }
    expect(publish).not.toHaveBeenCalled();
    expect(abortListener).not.toHaveBeenCalled();
    expect(events(a, "fabric.control.command")).toHaveLength(1);
    expect(events(b, "fabric.control.command")).toHaveLength(1);
    expect(events(b, "fabric.control.command")[0]!.data).toMatchObject({ message: marker, data: input.data });
    expect(receive).toHaveBeenCalledOnce();
    expect(calls).toBe(1);
    expect(sender.relayAcknowledgementRecoverySnapshot()).toMatchObject({ retained: 1, acknowledged: 1 });
    const retry = sender.request(target.identity.id, target.identity.id, "followUp",
      { principal: { binding: "herdr-client", id: "cache-body-principal" }, ownerIncarnation: target.ownerIncarnation,
        data: { list: [{ b: 2, a: 1 }], nested: { count: 1, marker } }, message: marker }, target.identity.id, options);
    await expect(retry).resolves.toMatchObject({ acknowledged: true, messageId: "cached-original-body-receipt" });
    expect(publish).not.toHaveBeenCalled();
    expect(events(a, "fabric.control.command")).toHaveLength(1);
    expect(events(b, "fabric.control.command")).toHaveLength(1);
    expect(receive).toHaveBeenCalledOnce();
    expect(markerFiles(hub.root, marker)).toEqual([]);
    bodyFreeReceipts(hub);
  });

  it("cannot poison late-ACK recovery with wrong owner/epoch/origin, native, or infrastructure receipts", async () => {
    const f = await fixture(async () => { throw unavailable(); });
    const a = f.stores.ryzen3!, source = f.mains.ryzen3!, target = f.mains.ryzen2!;
    const sender = senderFor(f), ingress = new StoreBridgeSide(a, "ryzen1");
    const input = { message: "cache-auth-counterexamples", ownerIncarnation: target.ownerIncarnation };
    const options = { routedRemoteHost: "ryzen2", idempotencyKey: "cache-auth-key" };
    const first = sender.request(target.identity.id, target.identity.id, "followUp", input, target.identity.id, options);
    void first.catch(() => undefined);
    await vi.waitFor(() => expect(events(a, "fabric.control.command")).toHaveLength(1));
    await f.drain();
    await expect(first).rejects.toMatchObject({ code: "FABRIC_RELAY_DESTINATION_UNAVAILABLE" });
    const command = events(a, "fabric.control.command")[0]!, commandId = (command.data as { commandId: string }).commandId;
    const originalTail = a.tail.bind(a);
    const reads = vi.spyOn(a, "tail");
    const drained = async (sequence: number): Promise<void> => {
      await vi.waitFor(() => expect(reads.mock.results.some(result => result.type === "return" &&
        result.value.events.some(event => event.sequence === sequence))).toBe(true));
      sender.pause(); await sender.checkpointForRelease(); sender.resume();
    };
    const ack = { topic: "fabric.control.ack", kind: "accepted", from: target.identity, to: source.identity.id,
      data: { version: 1, commandId, targetId: target.identity.id, ownerIncarnation: target.ownerIncarnation,
        accepted: true, messageId: "must-not-poison", bridge: { from: "ryzen2", id: "owner-event-unrelated-to-source", via: "ryzen1" } } };
    for (const [index, invalid] of [
      { ...ack, from: source.identity },
      { ...ack, data: { ...ack.data, ownerIncarnation: "wrong-owner-epoch" } },
      { ...ack, data: { ...ack.data, bridge: { ...ack.data.bridge, from: "ryzen3" } } },
      { ...ack, data: { ...ack.data, targetId: source.identity.id } },
    ].entries()) {
      const published = await ingress.publish({ ...invalid,
        data: { ...invalid.data, bridge: { ...invalid.data.bridge, id: `invalid-cache-event-${index}` } } }, [], true);
      await drained(published.sequence);
      expect(sender.relayAcknowledgementRecoverySnapshot()).toMatchObject({ retained: 1, acknowledged: 0 });
    }
    // Accessors cannot survive JSON transport, so inject one only at the source's real
    // tail observation boundary; every surrounding publication/store/drain remains real.
    const getter = vi.fn(() => true);
    reads.mockImplementation((cursor, limit) => {
      const tail = originalTail(cursor, limit);
      return { ...tail, events: tail.events.map(event => {
        if ((event.data as Record<string, unknown> | undefined)?.messageId !== "getter-owner") return event;
        const data = { ...(event.data as Record<string, unknown>) };
        Object.defineProperty(data, "accepted", { enumerable: true, get: getter });
        return { ...event, data };
      }) };
    });
    const getterAck = await ingress.publish({ ...ack, data: { ...ack.data, messageId: "getter-owner",
      bridge: { ...ack.data.bridge, id: "getter-owner-event" } } }, [], true);
    await drained(getterAck.sequence);
    expect(getter).not.toHaveBeenCalled();
    expect(sender.relayAcknowledgementRecoverySnapshot().acknowledged).toBe(0);
    reads.mockImplementation(originalTail);
    const nativeAck = await a.publish({ ...ack, data: { ...ack.data, bridge: undefined } });
    await drained(nativeAck.sequence);
    expect(sender.relayAcknowledgementRecoverySnapshot().acknowledged).toBe(0);
    const refusal = await ingress.publish({ topic: "fabric.control.ack", kind: "rejected",
      from: { id: "bridge:ryzen1", name: "mesh-bridge", kind: "main" }, to: source.identity.id,
      data: { version: 1, commandId, targetId: target.identity.id, accepted: false, error: "Relay destination unavailable",
        errorCode: "FABRIC_RELAY_DESTINATION_UNAVAILABLE", retryable: true, bridge: { from: "ryzen1", id: "late-infrastructure-event" } } }, [], true);
    await drained(refusal.sequence);
    expect(sender.relayAcknowledgementRecoverySnapshot().acknowledged).toBe(0);
    // A valid ordinary owner receipt is still admissible, irrespective of SOURCE event UUID.
    await ingress.publish({ ...ack, data: { ...ack.data, messageId: "genuine-late-owner" } }, [], true);
    await vi.waitFor(() => expect(sender.relayAcknowledgementRecoverySnapshot().acknowledged).toBe(1));
    await expect(sender.request(target.identity.id, target.identity.id, "followUp", input, target.identity.id, options))
      .resolves.toMatchObject({ acknowledged: true, messageId: "genuine-late-owner" });
    expect(events(a, "fabric.control.command")).toHaveLength(1);
    const newEpoch = "fixture:newly-resolved-owner";
    // An input epoch alone is not authority: #1070 revalidates it against the mirrored directory.
    await expect(sender.request(target.identity.id, target.identity.id, "followUp", { ...input, ownerIncarnation: newEpoch }, target.identity.id, options))
      .rejects.toBeInstanceOf(FabricControlStaleIncarnationError);
    expect(events(a, "fabric.control.command")).toHaveLength(1);
    // Publish the real replacement activation at its origin, then mirror it through both links.
    const b = f.stores.ryzen2!, participantKey = key("topology/participants/", target.identity.id);
    const participant = readParticipantFile(b.root, participantKey)!;
    const updatedAt = Date.now();
    writeParticipantFile(b.root, { ...participant, version: participant.version + 1, updatedAt,
      value: { ...(participant.value as Record<string, unknown>), ownerIncarnation: newEpoch, updatedAt } });
    await f.drain();
    expect(a.get(participantKey, { fresh: true })?.value).toMatchObject({ ownerIncarnation: newEpoch });
    // Same key with this newly resolved activation must NOT consume the old cached epoch.
    const changed = sender.request(target.identity.id, target.identity.id, "followUp", { ...input, ownerIncarnation: newEpoch }, target.identity.id, options);
    void changed.catch(() => undefined);
    await vi.waitFor(() => expect(events(a, "fabric.control.command")).toHaveLength(2));
    expect(sender.relayAcknowledgementRecoverySnapshot().retained).toBe(0);
    await ingress.publish({ ...ack, data: { ...ack.data, ownerIncarnation: newEpoch, messageId: "new-epoch-live-owner",
      bridge: { ...ack.data.bridge, id: "new-epoch-live-event" } } }, [], true);
    await expect(changed).resolves.toMatchObject({ acknowledged: true, messageId: "new-epoch-live-owner" });
  });

  it("expires recovery at the exact 8 s bound without idle timers and still allows a fresh live ACK", async () => {
    const f = await fixture(async () => { throw unavailable(); });
    const a = f.stores.ryzen3!, source = f.mains.ryzen3!, target = f.mains.ryzen2!;
    const sender = senderFor(f), ingress = new StoreBridgeSide(a, "ryzen1");
    let now = Date.now();
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const input = { message: "finite-recovery-window", ownerIncarnation: target.ownerIncarnation };
    const options = { routedRemoteHost: "ryzen2", idempotencyKey: "finite-recovery-key" };
    const first = sender.request(target.identity.id, target.identity.id, "followUp", input, target.identity.id, options);
    void first.catch(() => undefined);
    await vi.waitFor(() => expect(events(a, "fabric.control.command")).toHaveLength(1));
    await f.drain(); await expect(first).rejects.toMatchObject({ code: "FABRIC_RELAY_DESTINATION_UNAVAILABLE" });
    const commandId = (events(a, "fabric.control.command")[0]!.data as { commandId: string }).commandId;
    const ack = { topic: "fabric.control.ack", kind: "accepted", from: target.identity, to: source.identity.id,
      data: { version: 1, commandId, targetId: target.identity.id, ownerIncarnation: target.ownerIncarnation,
        accepted: true, messageId: "finite-owner-receipt", bridge: { from: "ryzen2", id: "finite-owner-event", via: "ryzen1" } } };
    await ingress.publish(ack, [], true);
    await vi.waitFor(() => expect(sender.relayAcknowledgementRecoverySnapshot().acknowledged).toBe(1));
    now += 7_999;
    expect(sender.relayAcknowledgementRecoverySnapshot()).toMatchObject({ retained: 1, acknowledged: 1 });
    now += 1;
    expect(sender.relayAcknowledgementRecoverySnapshot()).toMatchObject({ retained: 0, acknowledged: 0 });
    // Receipt after expiry cannot revive an orphan; a NEW pending can still authenticate it.
    await ingress.publish({ ...ack, data: { ...ack.data, bridge: { ...ack.data.bridge, id: "expired-owner-event" } } }, [], true);
    const retry = sender.request(target.identity.id, target.identity.id, "followUp", input, target.identity.id, options);
    void retry.catch(() => undefined);
    await vi.waitFor(() => expect(events(a, "fabric.control.command")).toHaveLength(2));
    await ingress.publish({ ...ack, data: { ...ack.data, bridge: { ...ack.data.bridge, id: "fresh-live-owner-event" } } }, [], true);
    await expect(retry).resolves.toMatchObject({ acknowledged: true, messageId: "finite-owner-receipt" });
    expect(sender.relayAcknowledgementRecoverySnapshot()).toMatchObject({ retained: 0, acknowledged: 0 });
  });

  it("caps refused-request authority at 256 and evicts the oldest without a full log scan", async () => {
    const f = await fixture();
    const a = f.stores.ryzen3!, source = f.mains.ryzen3!, target = f.mains.ryzen2!;
    const sender = senderFor(f), ingress = new StoreBridgeSide(a, "ryzen1");
    const now = Date.now();
    vi.spyOn(Date, "now").mockReturnValue(now); // no real sleep/TTL race during bounded capacity exercise
    let after = 0, firstCommandId = "", lastCommandId = "";
    for (let index = 0; index < 257; index++) {
      const pending = sender.request(target.identity.id, target.identity.id, "followUp", { ownerIncarnation: target.ownerIncarnation },
        target.identity.id, { routedRemoteHost: "ryzen2", idempotencyKey: `bounded-recovery-${index}` });
      void pending.catch(() => undefined);
      await vi.waitFor(() => expect(a.read({ topic: "fabric.control.command", after, limit: 1 })).toHaveLength(1), { interval: 1 });
      const command = a.read({ topic: "fabric.control.command", after, limit: 1 })[0]!;
      after = command.sequence;
      const commandId = (command.data as { commandId: string }).commandId;
      if (index === 0) firstCommandId = commandId;
      lastCommandId = commandId;
      await ingress.publish({ topic: "fabric.control.ack", kind: "rejected",
        from: { id: "bridge:ryzen1", name: "mesh-bridge", kind: "main" }, to: source.identity.id,
        data: { version: 1, commandId, targetId: target.identity.id, accepted: false, error: "Relay destination unavailable",
          errorCode: "FABRIC_RELAY_DESTINATION_UNAVAILABLE", retryable: true, bridge: { from: "ryzen1", id: command.id } } }, [], true);
      await expect(pending).rejects.toMatchObject({ code: "FABRIC_RELAY_DESTINATION_UNAVAILABLE" });
      expect(sender.relayAcknowledgementRecoverySnapshot().retained).toBe(Math.min(index + 1, 256));
    }
    for (const commandId of [firstCommandId, lastCommandId]) {
      await ingress.publish({ topic: "fabric.control.ack", kind: "accepted", from: target.identity, to: source.identity.id,
        data: { version: 1, commandId, targetId: target.identity.id, ownerIncarnation: target.ownerIncarnation,
          accepted: true, messageId: "capacity-owner", bridge: { from: "ryzen2", id: `capacity-${commandId}`, via: "ryzen1" } } }, [], true);
    }
    await vi.waitFor(() => expect(sender.relayAcknowledgementRecoverySnapshot()).toEqual({ retained: 256, acknowledged: 1, limit: 256, ttlMs: 8_000 }));
    expect(events(f.stores.ryzen2!, "fabric.control.command")).toEqual([]);
    expect(events(f.stores.ryzen1!, "fabric.control.command")).toEqual([]);
  }, 30_000);

  it("scrubs a seeded historical FULL hub ACK on singleton egress while preserving native hub marker bytes", async () => {
    const f = await fixture();
    const a = f.stores.ryzen3!, hub = f.stores.ryzen1!, source = f.mains.ryzen3!, target = f.mains.ryzen2!;
    const marker = "local-seeded-legacy-full-hub-ack";
    await new StoreBridgeSide(hub, "ryzen2").publish({ topic: "fabric.control.ack", kind: "accepted", from: target.identity,
      to: source.identity.id, text: marker, data: { version: 1, commandId: "legacy-full-singleton", targetId: target.identity.id,
        ownerIncarnation: target.ownerIncarnation, accepted: true, messageId: "legacy-owner-receipt", result: { marker },
        bridge: { from: "ryzen2", id: "legacy-original-event" } } });
    expect(markerFiles(hub.root, marker).length).toBeGreaterThan(0);
    expect(events(hub, "fabric.control.ack")[0]).toMatchObject({ text: marker, data: { result: { marker } } });
    await f.drain(); await f.drain();
    expect(events(a, "fabric.control.ack")).toHaveLength(1);
    expect(events(a, "fabric.control.ack")[0]).toMatchObject({ from: target.identity, verification: "bridge",
      data: { accepted: true, messageId: "legacy-owner-receipt", bridge: { from: "ryzen2", via: "ryzen1" } } });
    expect(events(a, "fabric.control.ack")[0]).not.toHaveProperty("text");
    expect(events(a, "fabric.control.ack")[0]!.data).not.toHaveProperty("result");
    expect(markerFiles(a.root, marker)).toEqual([]);
    expect(markerFiles(hub.root, marker).length).toBeGreaterThan(0); // historical fixture remains native-positive
  });

  it("reexports a token-bearing origin root with identical host and lease tokens at hub and A", async () => {
    const token = "local-safety-origin-incarnation-token";
    const f = await fixture(undefined, token), target = f.mains.ryzen2!;
    expect(target.incarnationToken).toBe(token);
    for (const store of [f.stores.ryzen2!, f.stores.ryzen1!, f.stores.ryzen3!]) {
      expect(store.get(key("topology/hosts/", target.identity.id), { fresh: true })!.value).toMatchObject({ incarnationToken: token });
      expect(readHostLease(store.root, target.identity.id)).toMatchObject({ id: target.identity.id, rootId: target.identity.id,
        identityId: target.identity.id, startedAt: target.startedAt, incarnationToken: token });
    }
    expect(f.stores.ryzen3!.get(key("topology/hosts/", target.identity.id), { fresh: true })!.value)
      .toMatchObject({ remoteHost: "ryzen2", bridgeVia: "ryzen1", incarnationToken: token });
    await f.drain(); await f.drain();
    for (const store of [f.stores.ryzen1!, f.stores.ryzen3!]) expect(readHostLease(store.root, target.identity.id)).toMatchObject({ incarnationToken: token });
    expect(removals).toEqual([]);
  });

  it("refuses a fulfilled destination RPC with an invalid sequence instead of recording hub success", async () => {
    const f = await fixture();
    const a = f.stores.ryzen3!, b = f.stores.ryzen2!, hub = f.stores.ryzen1!, target = f.mains.ryzen2!;
    // Narrow wire-receipt fault injection: the actual source and destination bridge
    // admission paths still run, and behavioral evidence is the origin API/store state.
    vi.spyOn(f.links[0]!.remote, "publish").mockResolvedValue({ sequence: 0 });
    const sender = senderFor(f), marker = "local-invalid-receipt-private-body";
    const pending = sender.request(target.identity.id, target.identity.id, "followUp",
      { message: marker, ownerIncarnation: target.ownerIncarnation }, target.identity.id,
      { routedRemoteHost: "ryzen2", idempotencyKey: "invalid-receipt-key" });
    void pending.catch(() => undefined);
    await vi.waitFor(() => expect(events(a, "fabric.control.command")).toHaveLength(1));
    await f.drain();
    await expect(pending).rejects.toMatchObject({ code: "FABRIC_RELAY_DESTINATION_UNAVAILABLE", retryable: true });
    expect(events(b, "fabric.control.command")).toEqual([]);
    expect(events(hub, "fabric.control.command")).toEqual([]);
    expect(markerFiles(a.root, marker).length).toBeGreaterThan(0);
    expect(markerFiles(hub.root, marker)).toEqual([]);
  });
});
