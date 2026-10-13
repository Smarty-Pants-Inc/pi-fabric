import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import { afterEach, describe, expect, it, vi } from "vitest";
import { BRIDGE_LEASE_MS, MeshBridge, RemoteBridgeSide, serveBridgeAgent, StoreBridgeSide } from "../src/mesh/bridge.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import { writeHostLease } from "../src/topology/host-leases.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";

// #11199: exercise lease-removal admission with recorded calls, never delete a lease locally.
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
const closers: Array<() => Promise<void>> = [];
const scratchRoots: string[] = [];
afterEach(async () => {
  for (const close of closers.splice(0)) await close();
  vi.restoreAllMocks();
  removals.length = 0;
  // #11199 permits hygiene of this test's own mkdtemp fixtures, not product deletion.
  for (const root of scratchRoots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const key = (prefix: string, id: string) => prefix + createHash("sha256").update(id).digest("hex");
const hostKey = (id: string) => key("topology/hosts/", id);
const participantKey = (id: string) => key("topology/participants/", id);
const events = (store: MeshStore, topic: string) => store.read({ topic, after: 0, limit: 100 });

const native = async (store: MeshStore, name: string, ttl = 60_000) => {
  const now = Date.now();
  const identity: MeshIdentity = { id: `session:${name}-00000000`, name: "main", kind: "main", sessionId: name };
  const ownerIncarnation = `fixture:${identity.id}`;
  const host = { format: 1 as const, id: identity.id, rootId: identity.id, identity, startedAt: now, updatedAt: now, expiresAt: now + ttl };
  await store.put({ key: hostKey(identity.id), identity, value: host });
  writeHostLease(store.root, { id: identity.id, rootId: identity.id, identityId: identity.id, startedAt: now, updatedAt: now, expiresAt: now + ttl });
  writeParticipantFile(store.root, { key: participantKey(identity.id), version: 1, updatedAt: now, updatedBy: identity, value: {
    format: 1, id: identity.id, kind: "root", rootId: identity.id, ownerHostId: identity.id, ownerIdentityId: identity.id,
    ownerIncarnation, name, label: name.toUpperCase(), status: "idle", runner: "pi", transport: "host",
    capabilities: ["steer", "followUp"], sessionId: name, startedAt: now, updatedAt: now, controlProtocol: "v1",
  } });
  return { identity, ownerIncarnation, host };
};

const fixture = async (targetTtl = 60_000) => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-spokes-"));
  scratchRoots.push(dir);
  const stores = Object.fromEntries(["ryzen1", "ryzen2", "ryzen3"].map(name => [name, new MeshStore(path.join(dir, name), 64 * 1024, 100)]));
  const hub = stores.ryzen1!;
  const links = ["ryzen2", "ryzen3"].map(name => {
    const input = new PassThrough();
    const output = new PassThrough();
    const serving = serveBridgeAgent(new StoreBridgeSide(stores[name]!, "ryzen1", () => Date.now()), input, output);
    const remote = new RemoteBridgeSide(output, input);
    closers.push(async () => { remote.close(); input.end(); await serving; output.end(); });
    const bridge = new MeshBridge({ localName: "ryzen1", remoteName: name, local: new StoreBridgeSide(hub, name, () => Date.now()), remote,
      cursorPath: path.join(dir, `${name}.json`), presenceMs: 0 });
    return { bridge, remote };
  });
  const roots = await Promise.all(Object.entries(stores).map(async ([name, store]) => [name, await native(store, name, name === "ryzen2" ? targetTtl : 60_000)] as const));
  const mains = Object.fromEntries(roots);
  for (const { bridge } of links) await bridge.start();
  for (const { bridge } of links) await bridge.syncPresence();
  const drain = async () => { for (const { bridge } of links) await bridge.step(); };
  return { dir, stores, mains, links, drain };
};

describe("spoke-to-spoke roots and addressed control through the hub", () => {
  it("routes a by-id followUp A -> hub -> B once, returns an origin-authenticated ACK, and keeps hub-local delivery", async () => {
    const f = await fixture();
    const a = f.stores.ryzen3!, b = f.stores.ryzen2!, hub = f.stores.ryzen1!;
    const source = f.mains.ryzen3!, target = f.mains.ryzen2!, local = f.mains.ryzen1!;
    const directory = new ParticipantDirectory(a, { enabled: true, hostId: source.identity.id, rootId: source.identity.id, identity: source.identity });
    expect(directory.get(target.identity.id, undefined, { fresh: true })).toMatchObject({ remoteHost: "ryzen2", bridgeVia: "ryzen1", stale: false });
    const sender = new FabricControlPlane(a, source.identity, { enabled: true, hostId: source.identity.id, pollMs: 5,
      readMirroredOwner: (...args) => directory.mirroredControlOwner(...args) });
    const receiver = new FabricControlPlane(b, target.identity, { enabled: true, hostId: target.identity.id, ownerIncarnation: target.ownerIncarnation, pollMs: 5 });
    const hubReceiver = new FabricControlPlane(hub, local.identity, { enabled: true, hostId: local.identity.id, ownerIncarnation: local.ownerIncarnation, pollMs: 5 });
    const receive = vi.fn(() => ({ accepted: true, messageId: "delivered-on-B" }));
    const receiveHub = vi.fn(() => ({ accepted: true, messageId: "delivered-on-hub" }));
    sender.start(() => ({ accepted: false })); receiver.start(receive); hubReceiver.start(receiveHub);
    closers.push(async () => { await sender.close(); await receiver.close(); await hubReceiver.close(); });
    const provider = new AgentsProvider({ cwd: f.dir } as any, { identity: source.identity } as any, {} as any,
      { local: true, id: source.identity.id, matches: (id: string) => id === source.identity.id } as any,
      directory, sender, {} as any, () => false, undefined, false);
    const checkpoints = f.links.map(({ bridge }) => fs.readFileSync(bridge.options.cursorPath, "utf8"));
    const pending = provider.invoke("followUp", { id: target.identity.id, message: "net to architecture" }, { cwd: f.dir } as any);
    void pending.catch(() => undefined);
    await vi.waitFor(() => expect(events(a, "fabric.control.command")).toHaveLength(1));
    await f.drain(); await f.drain();
    await vi.waitFor(() => expect(events(b, "fabric.control.ack")).toHaveLength(1));
    await f.drain(); await f.drain();
    await expect(pending).resolves.toMatchObject({ acknowledged: true, routed: "mesh", messageId: "delivered-on-B" });
    expect(receive).toHaveBeenCalledOnce();
    expect(receive.mock.calls[0]).toEqual(expect.arrayContaining([expect.objectContaining({ operation: "followUp", message: "net to architecture" }), expect.objectContaining({ id: source.identity.id, verified: "bridge" })]));
    expect(events(b, "fabric.control.command")[0]).toMatchObject({ verification: "bridge", from: source.identity,
      data: { destinationRemoteHost: "ryzen2", bridge: { from: "ryzen3", via: "ryzen1" } } });
    expect(events(a, "fabric.control.ack")[0]).toMatchObject({ data: { bridge: { from: "ryzen2", via: "ryzen1" } } });
    // Crash after append but before cursor checkpoint: restore the pre-command disk cursors,
    // then reconnect fresh bridge instances. Recovery must use the immediate hop's IDs.
    for (const [index, link] of f.links.entries()) {
      fs.writeFileSync(link.bridge.options.cursorPath, checkpoints[index]!);
      link.bridge = new MeshBridge({ ...link.bridge.options,
        local: new StoreBridgeSide(new MeshStore(hub.root, 64 * 1024, 100), link.bridge.options.remoteName) });
      await link.bridge.start();
    }
    await f.drain(); await f.drain();
    expect(events(b, "fabric.control.command")).toHaveLength(1);
    expect(events(a, "fabric.control.ack")).toHaveLength(1);
    expect(receive).toHaveBeenCalledOnce();
    for (const [name, root] of Object.entries(f.mains)) {
      expect(f.stores[name]!.get(hostKey(root.identity.id))!.value).not.toHaveProperty("remoteHost");
      expect(f.stores[name]!.get(participantKey(root.identity.id))).toBeUndefined(); // native file only
    }
    expect((await new StoreBridgeSide(a, "ryzen1").presence()).hosts.map(host => host.record.id)).toEqual([source.identity.id]);
    const toHub = provider.invoke("followUp", { id: local.identity.id, message: "still hub-local" }, { cwd: f.dir } as any);
    void toHub.catch(() => undefined);
    await vi.waitFor(() => expect(events(a, "fabric.control.command")).toHaveLength(2));
    await f.drain();
    await vi.waitFor(() => expect(receiveHub).toHaveBeenCalledOnce());
    await f.drain();
    await expect(toHub).resolves.toMatchObject({ acknowledged: true, messageId: "delivered-on-hub" });
    expect(receive).toHaveBeenCalledOnce();
  });

  it("removes B's expired root from A, without reviving it through A or executing lease deletion", async () => {
    const f = await fixture(10_000);
    const target = f.mains.ryzen2!;
    const hub = f.stores.ryzen1!, a = f.stores.ryzen3!;
    const expiry = target.host.expiresAt;
    vi.spyOn(Date, "now").mockReturnValue(expiry + BRIDGE_LEASE_MS + 1);
    await f.drain(); await f.drain();
    expect(hub.get(hostKey(target.identity.id))).toBeUndefined();
    expect(a.get(hostKey(target.identity.id))).toBeUndefined();
    expect(a.get(participantKey(target.identity.id))).toBeUndefined();
    expect(removals).toContain(`${a.root}:${target.identity.id}`);
    expect(hub.get(hostKey(f.mains.ryzen3!.identity.id))!.value).toMatchObject({ remoteHost: "ryzen3" });
    expect(a.get(participantKey(f.mains.ryzen1!.identity.id))!.value).toMatchObject({ remoteHost: "ryzen1" });
  });

  it("withdraws a B root from A when the native participant file is removed, while its live host remains", async () => {
    const f = await fixture();
    const b = f.stores.ryzen2!, a = f.stores.ryzen3!, hub = f.stores.ryzen1!;
    const id = f.mains.ryzen2!.identity.id;
    // Fixture-only rename removes source presence without invoking a product filesystem delete.
    fs.renameSync(path.join(b.root, "participants", participantKey(id).split("/").at(-1)! + ".json"),
      path.join(f.dir, "withdrawn-root.json"));
    await f.drain(); await f.drain();
    expect(hub.get(participantKey(id))).toBeUndefined();
    expect(a.get(participantKey(id))).toBeUndefined();
    expect(a.get(hostKey(id))!.value).toMatchObject({ remoteHost: "ryzen2", bridgeVia: "ryzen1" });
    expect(b.get(hostKey(id))!.value).not.toHaveProperty("remoteHost");
    expect(b.get(participantKey(id))).toBeUndefined(); // no other spoke can restore its removed origin
  });

  it("re-exports source additions and removals from real notifications without manual presence sync", async () => {
    const f = await fixture();
    const running = f.links.map(link => link.bridge.run());
    let failure: unknown;
    for (const run of running) void run.catch(error => { failure = error; });
    closers.unshift(async () => {
      for (const link of f.links) await link.bridge.stop();
      await Promise.allSettled(running);
    });
    const b = f.stores.ryzen2!, a = f.stores.ryzen3!;
    const root = await native(b, "added-after-connect");
    const directory = new ParticipantDirectory(a, { enabled: true, hostId: f.mains.ryzen3!.identity.id,
      rootId: f.mains.ryzen3!.identity.id, identity: f.mains.ryzen3!.identity });
    await vi.waitFor(() => expect(directory.get(root.identity.id, undefined, { fresh: true }))
      .toMatchObject({ remoteHost: "ryzen2", bridgeVia: "ryzen1", stale: false }), { timeout: 5_000 });
    fs.renameSync(path.join(b.root, "participants", participantKey(root.identity.id).split("/").at(-1)! + ".json"),
      path.join(f.dir, "notification-withdrawn.json"));
    await vi.waitFor(() => expect(a.get(participantKey(root.identity.id), { fresh: true })).toBeUndefined(), { timeout: 5_000 });
    expect(directory.get(f.mains.ryzen2!.identity.id, undefined, { fresh: true })).toMatchObject({ stale: false });
    expect(failure).toBeUndefined();
  });

  it("forwards a control/ACK batch with origin stamps but never relays work topics between spokes", async () => {
    const f = await fixture();
    const a = f.stores.ryzen3!, b = f.stores.ryzen2!;
    const source = f.mains.ryzen3!, target = f.mains.ryzen2!;
    await Promise.all(f.links.map(link => link.remote.hello())); // actual publishBatch RPC capability
    const commands = ["one", "two", "three"];
    for (const commandId of commands) {
      await a.publish({ topic: "fabric.control.command", kind: "followUp", from: source.identity, to: target.identity.id,
        data: { version: 1, commandId, targetId: target.identity.id, operation: "followUp", replyTo: source.identity.id,
          message: commandId, destinationRemoteHost: "ryzen2" } });
    }
    await a.publish({ topic: "fleet.work.smarty-dev.10068", kind: "ask", from: source.identity, to: target.identity.id, text: "not a control" });
    await f.drain(); await f.drain();
    expect(events(b, "fabric.control.command").map(event => (event.data as { commandId: string }).commandId)).toEqual(commands);
    for (const commandId of commands) {
      await b.publish({ topic: "fabric.control.ack", kind: "accepted", from: target.identity, to: source.identity.id,
        data: { version: 1, commandId, targetId: target.identity.id, accepted: true, triggered: true, pendingFollowUps: 1 } });
    }
    await f.drain(); await f.drain();
    expect(events(a, "fabric.control.ack").map(event => (event.data as { commandId: string }).commandId)).toEqual(commands);
    expect(events(a, "fabric.control.ack").every(event => {
      const data = event.data as { triggered: boolean; pendingFollowUps: number; bridge: { from: string; via: string } };
      return data.triggered && data.pendingFollowUps === 1 && data.bridge.from === "ryzen2" && data.bridge.via === "ryzen1";
    })).toBe(true);
    expect(events(b, "fleet.work.smarty-dev.10068")).toHaveLength(0);
    await f.drain();
    expect(events(b, "fabric.control.command")).toHaveLength(3);
    expect(events(a, "fabric.control.ack")).toHaveLength(3);
  });

  it("does not retarget a captured native or other-host control, but still carries the correctly captured destination", async () => {
    const f = await fixture();
    const a = f.stores.ryzen3!, b = f.stores.ryzen2!, hub = f.stores.ryzen1!;
    const source = f.mains.ryzen3!, target = f.mains.ryzen2!;
    for (const destinationRemoteHost of [null, "ryzen1", "ryzen3", "ryzen2"]) {
      await a.publish({ topic: "fabric.control.command", kind: "followUp", from: source.identity, to: target.identity.id,
        data: { version: 1, commandId: String(destinationRemoteHost), targetId: target.identity.id, operation: "followUp",
          replyTo: source.identity.id, message: "pinned", destinationRemoteHost } });
    }
    await f.drain(); await f.drain();
    expect(events(hub, "fabric.control.command")).toHaveLength(1);
    expect(events(b, "fabric.control.command")).toHaveLength(1);
    expect(events(b, "fabric.control.command")[0]!.data).toMatchObject({ destinationRemoteHost: "ryzen2" });
  });

  it("requires a valid, matching via-hop on participant and owner, without changing origin ACK attribution", async () => {
    const f = await fixture();
    const a = f.stores.ryzen3!, source = f.mains.ryzen3!, target = f.mains.ryzen2!;
    const directory = new ParticipantDirectory(a, { enabled: true, hostId: source.identity.id, rootId: source.identity.id, identity: source.identity });
    const entry = a.get(participantKey(target.identity.id))!;
    const record = entry.value as Record<string, unknown>;
    for (const bridgeVia of ["ryzen2", 123, "bad route", "another-hub"]) {
      await a.put({ key: entry.key, identity: target.identity, value: { ...record, bridgeVia } });
      expect(directory.mirroredControlOwner(target.identity.id, target.identity.id, target.identity.id)).toBeUndefined();
    }
    await a.put({ key: entry.key, identity: target.identity, value: record });
    expect(directory.mirroredControlOwner(target.identity.id, target.identity.id, target.identity.id))
      .toMatchObject({ remoteHost: "ryzen2", expiresAt: expect.any(Number) });
  });

  it("refuses untrusted re-export claims and native forged/extra-hop bridge stamps", async () => {
    const f = await fixture();
    const a = f.stores.ryzen3!, b = f.stores.ryzen2!, hub = f.stores.ryzen1!;
    const target = f.mains.ryzen2!, source = f.mains.ryzen3!;
    const claimed = { ...target.host, remoteHost: "ryzen2", bridgeVia: "ryzen3" };
    await new StoreBridgeSide(hub, "ryzen3").mirror({ hosts: [{ record: claimed, expiresAt: claimed.expiresAt }], participants: [] });
    expect(hub.get(hostKey(target.identity.id))!.value).toMatchObject({ remoteHost: "ryzen2" });
    for (const bridge of [{ from: "ryzen3", id: "forged" }, { from: "ryzen3", id: "again", via: "ryzen1" }]) {
      await a.publish({ topic: "fabric.control.command", kind: "followUp", from: source.identity, to: target.identity.id,
        data: { version: 1, commandId: bridge.id, targetId: target.identity.id, operation: "followUp", replyTo: source.identity.id, bridge } });
    }
    await f.drain(); await f.drain();
    expect(events(b, "fabric.control.command")).toHaveLength(0);
    expect(events(hub, "fabric.control.command")).toHaveLength(0);
  });
});
