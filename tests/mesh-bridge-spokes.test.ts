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
import { readHostLease, writeHostLease } from "../src/topology/host-leases.js";
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
// PR#1076 trust boundary (pi-fabric#928 SEC P1): the hub's shared log may hold only these outcome fields of an
// ACK it merely relays between spokes, plus its own bridge stamp. Mirrors bridge.ts RELAYED_ACK_FIELDS.
// Lead decision (split-only, no private spool): the destination spoke receives exactly the same outcome-only copy.
const RELAYED_ACK_FIELDS = new Set(["version", "commandId", "targetId", "ownerIncarnation", "accepted", "messageId", "error", "errorCode", "notRun", "bridge"]);
const expectRelayedOnly = (store: MeshStore, secret: string, to: string) => {
  const acks = events(store, "fabric.control.ack").filter(event => event.to === to);
  expect(acks.length).toBeGreaterThan(0);
  for (const ack of acks) {
    expect(Object.keys(ack.data as object).filter(field => !RELAYED_ACK_FIELDS.has(field))).toEqual([]);
    expect(ack).not.toHaveProperty("text");
    expect(JSON.stringify(ack)).not.toContain(secret);
  }
};

// Data safety: inspect actual persisted bytes, including state, event archives and outbox files,
// rather than proving only that a filtered read or recorded publish omitted the marker.
const persistedMarkerFiles = (root: string, marker: string): string[] => {
  const found: string[] = [];
  const visit = (dir: string): void => {
    for (const name of fs.readdirSync(dir)) {
      const file = path.join(dir, name);
      try {
        const stat = fs.lstatSync(file);
        expect(stat.isSymbolicLink()).toBe(false);
        if (stat.isDirectory()) visit(file);
        else if (stat.isFile() && fs.readFileSync(file).includes(Buffer.from(marker))) found.push(path.relative(root, file));
      } catch (error) {
        // Atomic lease/state writers may rename a temporary entry during the scan.
        if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
      }
    }
  };
  visit(root);
  return found;
};

const native = async (store: MeshStore, name: string, ttl = 60_000, incarnationToken?: string) => {
  const now = Date.now();
  const identity: MeshIdentity = { id: `session:${name}-00000000`, name: "main", kind: "main", sessionId: name };
  const ownerIncarnation = `fixture:${identity.id}`;
  const host = { format: 1 as const, id: identity.id, rootId: identity.id, identity, startedAt: now, updatedAt: now, expiresAt: now + ttl,
    ...(incarnationToken !== undefined ? { incarnationToken } : {}) };
  await store.put({ key: hostKey(identity.id), identity, value: host });
  writeHostLease(store.root, { id: identity.id, rootId: identity.id, identityId: identity.id, startedAt: now, updatedAt: now, expiresAt: now + ttl,
    ...(incarnationToken !== undefined ? { incarnationToken } : {}) });
  writeParticipantFile(store.root, { key: participantKey(identity.id), version: 1, updatedAt: now, updatedBy: identity, value: {
    format: 1, id: identity.id, kind: "root", rootId: identity.id, ownerHostId: identity.id, ownerIdentityId: identity.id,
    ownerIncarnation, name, label: name.toUpperCase(), status: "idle", runner: "pi", transport: "host",
    capabilities: ["steer", "followUp"], sessionId: name, startedAt: now, updatedAt: now, controlProtocol: "v1",
  } });
  return { identity, ownerIncarnation, host };
};

const fixture = async (targetTtl = 60_000, targetToken?: string,
  afterRelayDelivery?: (event: import("../src/mesh/bridge.js").BridgePublish) => Promise<void>) => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-spokes-"));
  scratchRoots.push(dir);
  const stores = Object.fromEntries(["ryzen1", "ryzen2", "ryzen3"].map(name => [name, new MeshStore(path.join(dir, name), 64 * 1024, 100)]));
  const hub = stores.ryzen1!;
  const links: Array<{ bridge: MeshBridge; remote: RemoteBridgeSide }> = ["ryzen2", "ryzen3"].map(name => {
    const input = new PassThrough();
    const output = new PassThrough();
    const serving = serveBridgeAgent(new StoreBridgeSide(stores[name]!, "ryzen1", () => Date.now()), input, output);
    const remote = new RemoteBridgeSide(output, input);
    closers.push(async () => { remote.close(); input.end(); await serving; output.end(); });
    const bridge = new MeshBridge({ localName: "ryzen1", remoteName: name, local: new StoreBridgeSide(hub, name, () => Date.now()), remote,
      cursorPath: path.join(dir, `${name}.json`), presenceMs: 0,
      relay: async (destinationRemote, event) => {
        const destination = links.find(link => link.bridge.options.remoteName === destinationRemote);
        if (!destination) throw Object.assign(new Error("Fixture relay destination unavailable"), {
          code: "FABRIC_RELAY_DESTINATION_UNAVAILABLE", retryable: true });
        await destination.bridge.deliverRelay(name, event);
        await afterRelayDelivery?.(event);
      } });
    return { bridge, remote };
  });
  const roots = await Promise.all(Object.entries(stores).map(async ([name, store]) => [name,
    await native(store, name, name === "ryzen2" ? targetTtl : 60_000, name === "ryzen2" ? targetToken : undefined)] as const));
  const mains = Object.fromEntries(roots);
  for (const { bridge } of links) await bridge.start();
  for (const { bridge } of links) await bridge.syncPresence();
  const drain = async () => { for (const { bridge } of links) await bridge.step(); };
  return { dir, stores, mains, links, drain };
};

// RELAY-1 (Smarty-Pants-Inc/smarty-dev#10068): hold only B's hub-local mirror write.
// The two RPC transports remain independent, so A can refresh without servicing B.
const holdHubMirror = async (bridge: MeshBridge) => {
  let resume!: () => void, entered!: () => void;
  const gate = new Promise<void>(resolve => { resume = resolve; });
  const observed = new Promise<void>(resolve => { entered = resolve; });
  const mirror = bridge.options.local.mirror.bind(bridge.options.local);
  vi.spyOn(bridge.options.local, "mirror").mockImplementationOnce(async (...args) => {
    entered();
    await gate;
    await mirror(...args);
  });
  const syncing = bridge.syncPresence();
  void syncing.catch(() => undefined);
  await observed;
  return async () => { resume(); await syncing; };
};
const sourceCursor = (bridge: MeshBridge): number =>
  (JSON.parse(fs.readFileSync(bridge.options.cursorPath, "utf8")) as { toLocal: { after: number } }).toLocal.after;
const relaySender = (f: Awaited<ReturnType<typeof fixture>>) => {
  const a = f.stores.ryzen3!, source = f.mains.ryzen3!;
  const directory = new ParticipantDirectory(a, { enabled: true, hostId: source.identity.id, rootId: source.identity.id, identity: source.identity });
  const sender = new FabricControlPlane(a, source.identity, { enabled: true, hostId: source.identity.id,
    acknowledgementTimeoutMs: 120_000, bridgeTimeoutMs: 120_000,
    readMirroredOwner: (...args) => directory.mirroredControlOwner(...args) });
  sender.start(() => ({ accepted: false }));
  closers.push(() => sender.close());
  return sender;
};

describe("spoke-to-spoke roots and addressed control through the hub", () => {
  it("RELAY-1 retains A's cursor while live B's mirror is held and expired, then executes and ACKs once across retries/restart", async () => {
    const f = await fixture();
    const a = f.stores.ryzen3!, b = f.stores.ryzen2!, hub = f.stores.ryzen1!;
    const source = f.mains.ryzen3!, target = f.mains.ryzen2!;
    const sourceLink = f.links.find(link => link.bridge.options.remoteName === "ryzen3")!;
    const destinationLink = f.links.find(link => link.bridge.options.remoteName === "ryzen2")!;
    let now = Date.now();
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const sender = relaySender(f);
    const receiver = new FabricControlPlane(b, target.identity, { enabled: true, hostId: target.identity.id,
      ownerIncarnation: target.ownerIncarnation });
    const receive = vi.fn(() => ({ accepted: true, messageId: "relay-1-recovered-on-B" }));
    receiver.start(receive);
    closers.push(() => receiver.close());
    const marker = "relay-1-held-mirror-private-command";
    const pending = sender.request(target.identity.id, target.identity.id, "followUp",
      { message: marker, ownerIncarnation: target.ownerIncarnation }, target.identity.id,
      { routedRemoteHost: "ryzen2", idempotencyKey: "relay-1-held-mirror-key" });
    void pending.catch(() => undefined);
    await vi.waitFor(() => expect(events(a, "fabric.control.command")).toHaveLength(1));
    const command = events(a, "fabric.control.command")[0]!;
    expect(command.data).toMatchObject({ destinationRemoteHost: "ryzen2", ownerIncarnation: target.ownerIncarnation });
    const checkpoint = fs.readFileSync(sourceLink.bridge.options.cursorPath, "utf8");
    const before = sourceCursor(sourceLink.bridge);
    const release = await holdHubMirror(destinationLink.bridge);
    try {
      // Native leases have 40 s left; only translated bridge authority expired.
      now += BRIDGE_LEASE_MS + 5_000;
      expect(readHostLease(b.root, target.identity.id)!.expiresAt).toBeGreaterThan(now);
      expect(readHostLease(hub.root, target.identity.id)!.expiresAt).toBeLessThan(now);
      await sourceLink.bridge.syncPresence();
      expect(sourceLink.bridge.options.local.holds!(source.identity.id, source.identity)).toBe(true);
      await sourceLink.bridge.step();
      expect(sourceCursor(sourceLink.bridge)).toBe(before);
      expect(events(a, "fabric.control.ack")).toEqual([]);
      expect(events(b, "fabric.control.command")).toEqual([]);
      expect(events(b, "fabric.control.ack")).toEqual([]);
      expect(receive).not.toHaveBeenCalled();
      expect(events(hub, "fabric.control.command")).toEqual([]);
      expect(events(hub, "fabric.control.ack")).toEqual([]);
      expect(persistedMarkerFiles(hub.root, marker)).toEqual([]);
      now += 1_000;
      await sourceLink.bridge.syncPresence();
      await sourceLink.bridge.step();
      expect(sourceCursor(sourceLink.bridge)).toBe(before);
      expect(events(a, "fabric.control.ack")).toEqual([]);
      expect(receive).not.toHaveBeenCalled();
      await release();
      // The old held sample lands stale. A NEW B sample must authorize recovery.
      await destinationLink.bridge.syncPresence();
      await sourceLink.bridge.syncPresence();
      await sourceLink.bridge.step();
      await vi.waitFor(() => expect(events(b, "fabric.control.ack")).toHaveLength(1));
      await destinationLink.bridge.step();
      await sourceLink.bridge.step();
      await expect(pending).resolves.toMatchObject({ acknowledged: true, messageId: "relay-1-recovered-on-B" });
      expect(sourceCursor(sourceLink.bridge)).toBeGreaterThanOrEqual(command.sequence);
      expect(receive).toHaveBeenCalledOnce();
      expect(receive.mock.calls[0]).toEqual(expect.arrayContaining([expect.objectContaining({ message: marker }),
        expect.objectContaining({ id: source.identity.id, verified: "bridge" })]));
      expect(events(b, "fabric.control.command")).toHaveLength(1);
      expect(events(b, "fabric.control.command")[0]).toMatchObject({ verification: "bridge", from: source.identity,
        data: { bridge: { from: "ryzen3", via: "ryzen1" } } });
      expect(events(a, "fabric.control.ack")).toHaveLength(1);
      expect(events(a, "fabric.control.ack")[0]).toMatchObject({ from: target.identity,
        data: { accepted: true, bridge: { from: "ryzen2", via: "ryzen1" } } });
      await a.publish({ topic: command.topic, kind: command.kind, from: source.identity, to: target.identity.id, data: command.data });
      await sourceLink.bridge.step();
      // Rewind the checkpoint: real body-free receipts and durable dedupe must win.
      fs.writeFileSync(sourceLink.bridge.options.cursorPath, checkpoint);
      sourceLink.bridge = new MeshBridge({ ...sourceLink.bridge.options,
        local: new StoreBridgeSide(new MeshStore(hub.root, 64 * 1024, 100), "ryzen3", () => Date.now()) });
      await sourceLink.bridge.start();
      await sourceLink.bridge.step();
      await destinationLink.bridge.step();
      await sourceLink.bridge.step();
      expect(receive).toHaveBeenCalledOnce();
      expect(events(b, "fabric.control.command")).toHaveLength(1);
      expect(events(b, "fabric.control.ack")).toHaveLength(1);
      expect(events(a, "fabric.control.ack")).toHaveLength(1);
      expect(persistedMarkerFiles(hub.root, marker)).toEqual([]);
      for (const receipt of events(hub, "fabric.control.command")) {
        for (const field of ["data", "text", "message"]) expect(receipt).not.toHaveProperty(field);
      }
    } finally { await release(); }
  });

  it("RELAY-1 bounds unknown B authority to 15 s from the first pending event despite 1 s retries and fresh A mirrors", async () => {
    const f = await fixture();
    const a = f.stores.ryzen3!, b = f.stores.ryzen2!, hub = f.stores.ryzen1!;
    const source = f.mains.ryzen3!, target = f.mains.ryzen2!;
    const sourceLink = f.links.find(link => link.bridge.options.remoteName === "ryzen3")!;
    const destinationLink = f.links.find(link => link.bridge.options.remoteName === "ryzen2")!;
    let now = Date.now();
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const marker = "relay-1-bounded-unknown-private-command";
    // Isolate the bridge grace from the sender's independent directory-stale auto-cancel.
    // The recovery test above covers a real control-plane waiter and receiver.
    await a.publish({ topic: "fabric.control.command", kind: "followUp", from: source.identity, to: target.identity.id,
      data: { version: 1, commandId: "relay-1-bounded-unknown", targetId: target.identity.id, operation: "followUp",
        replyTo: source.identity.id, requestedAt: Date.now(), deadlineAt: Date.now() + 120_000,
        destinationRemoteHost: "ryzen2", ownerIncarnation: target.ownerIncarnation, message: marker } });
    await vi.waitFor(() => expect(events(a, "fabric.control.command")).toHaveLength(1));
    const command = events(a, "fabric.control.command")[0]!;
    const before = sourceCursor(sourceLink.bridge);
    const release = await holdHubMirror(destinationLink.bridge);
    try {
      now += BRIDGE_LEASE_MS + 5_000;
      const firstPendingAt = now;
      for (let elapsed = 0; elapsed < BRIDGE_LEASE_MS; elapsed += 1_000) {
        now = firstPendingAt + elapsed;
        await sourceLink.bridge.syncPresence();
        expect(sourceLink.bridge.options.local.holds!(source.identity.id, source.identity)).toBe(true);
        await sourceLink.bridge.step();
        expect(sourceCursor(sourceLink.bridge), `retained at ${elapsed} ms`).toBe(before);
        expect(events(a, "fabric.control.ack")).toEqual([]);
        expect(events(b, "fabric.control.command")).toEqual([]);
        expect(events(hub, "fabric.control.command")).toEqual([]);
      }
      now = firstPendingAt + BRIDGE_LEASE_MS + 1;
      expect(readHostLease(b.root, target.identity.id)!.expiresAt).toBeGreaterThan(now);
      await sourceLink.bridge.syncPresence();
      await sourceLink.bridge.step();
      expect(sourceCursor(sourceLink.bridge)).toBeGreaterThanOrEqual(command.sequence);
      expect(events(a, "fabric.control.ack")).toHaveLength(1);
      expect(events(a, "fabric.control.ack")[0]).toMatchObject({ verification: "bridge", to: source.identity.id,
        from: { id: "bridge:ryzen1" }, data: { accepted: false, retryable: true,
          errorCode: "FABRIC_RELAY_DESTINATION_UNAVAILABLE", bridge: { from: "ryzen1", id: command.id } } });
      now += 1_000;
      await sourceLink.bridge.syncPresence();
      await sourceLink.bridge.step();
      expect(events(a, "fabric.control.ack")).toHaveLength(1);
      expect(events(b, "fabric.control.command")).toEqual([]);
      expect(events(b, "fabric.control.ack")).toEqual([]);
      expect(events(hub, "fabric.control.command")).toEqual([]);
      expect(events(hub, "fabric.control.ack")).toEqual([]);
      expect(persistedMarkerFiles(hub.root, marker)).toEqual([]);
      await release();
      await destinationLink.bridge.syncPresence();
      await sourceLink.bridge.syncPresence();
      await sourceLink.bridge.step();
      await destinationLink.bridge.step();
      await sourceLink.bridge.step();
      expect(events(a, "fabric.control.ack")).toHaveLength(1);
      expect(events(b, "fabric.control.command")).toEqual([]);
      expect(events(b, "fabric.control.ack")).toEqual([]);
      expect(events(hub, "fabric.control.command")).toEqual([]);
    } finally { await release(); }
  });

  it("RELAY-1 immediately refuses a never-seen B destination once rather than granting uncertainty grace", async () => {
    const f = await fixture();
    const a = f.stores.ryzen3!, b = f.stores.ryzen2!, hub = f.stores.ryzen1!;
    const source = f.mains.ryzen3!;
    const sourceLink = f.links.find(link => link.bridge.options.remoteName === "ryzen3")!;
    relaySender(f);
    const targetId = "session:never-registered-00000000";
    const marker = "relay-1-never-seen-private-command";
    expect(hub.get(hostKey(targetId))).toBeUndefined();
    await a.publish({ topic: "fabric.control.command", kind: "followUp", from: source.identity, to: targetId,
      data: { version: 1, commandId: "relay-1-never-seen", targetId, operation: "followUp", replyTo: source.identity.id,
        requestedAt: Date.now(), deadlineAt: Date.now() + 60_000, destinationRemoteHost: "ryzen2", message: marker } });
    const command = events(a, "fabric.control.command")[0]!;
    await sourceLink.bridge.step();
    expect(sourceCursor(sourceLink.bridge)).toBeGreaterThanOrEqual(command.sequence);
    expect(events(a, "fabric.control.ack")).toHaveLength(1);
    expect(events(a, "fabric.control.ack")[0]).toMatchObject({ verification: "bridge", to: source.identity.id,
      from: { id: "bridge:ryzen1" }, data: { accepted: false, retryable: true,
        errorCode: "FABRIC_RELAY_DESTINATION_UNAVAILABLE", bridge: { from: "ryzen1", id: command.id } } });
    await sourceLink.bridge.step();
    expect(events(a, "fabric.control.ack")).toHaveLength(1);
    expect(events(b, "fabric.control.command")).toEqual([]);
    expect(events(b, "fabric.control.ack")).toEqual([]);
    expect(events(hub, "fabric.control.command")).toEqual([]);
    expect(events(hub, "fabric.control.ack")).toEqual([]);
    expect(persistedMarkerFiles(hub.root, marker)).toEqual([]);
  });
  it.each(["retained", "missing", "Infinity", "NaN", "string", "future", "null-record", "primitive-record"] as const)(
    "RELAY-1 persists restart-bounded destination grace and repairs %s cursor metadata from local time", async mode => {
      const f = await fixture();
      const a = f.stores.ryzen3!, b = f.stores.ryzen2!, hub = f.stores.ryzen1!;
      const source = f.mains.ryzen3!, target = f.mains.ryzen2!;
      const sourceLink = f.links.find(link => link.bridge.options.remoteName === "ryzen3")!;
      const destinationLink = f.links.find(link => link.bridge.options.remoteName === "ryzen2")!;
      let now = Date.now();
      vi.spyOn(Date, "now").mockImplementation(() => now);
      const marker = `relay-1-restart-${mode}-private-command`;
      // The sender's directory-stale auto-cancel is independent of bridge grace.
      await a.publish({ topic: "fabric.control.command", kind: "followUp", from: source.identity, to: target.identity.id,
        data: { version: 1, commandId: `relay-1-restart-${mode}`, targetId: target.identity.id, operation: "followUp",
          replyTo: source.identity.id, requestedAt: now, deadlineAt: now + 120_000,
          destinationRemoteHost: "ryzen2", ownerIncarnation: target.ownerIncarnation, message: marker } });
      const command = events(a, "fabric.control.command")[0]!;
      const before = sourceCursor(sourceLink.bridge);
      const readCursor = () => JSON.parse(fs.readFileSync(sourceLink.bridge.options.cursorPath, "utf8")) as {
        toLocal: { after: number; mark: number; offset?: number; [field: string]: unknown };
      };
      const graceFields = () => Object.fromEntries(Object.entries(readCursor().toLocal)
        .filter(([field]) => !["after", "mark", "offset"].includes(field)));
      const restart = async () => {
        sourceLink.bridge = new MeshBridge({ ...sourceLink.bridge.options,
          local: new StoreBridgeSide(new MeshStore(hub.root, 64 * 1024, 100), "ryzen3", () => Date.now()) });
        await sourceLink.bridge.start();
        await sourceLink.bridge.step();
      };
      const step = async () => {
        await sourceLink.bridge.syncPresence();
        expect(sourceLink.bridge.options.local.holds!(source.identity.id, source.identity)).toBe(true);
        expect(readHostLease(hub.root, target.identity.id)!.expiresAt).toBeLessThan(now);
        expect(hub.get(hostKey(target.identity.id), { fresh: true })!.value).toMatchObject({ remoteHost: "ryzen2" });
        expect(readHostLease(b.root, target.identity.id)!.expiresAt).toBeGreaterThan(now);
        await sourceLink.bridge.step();
      };
      const retained = () => {
        expect(sourceCursor(sourceLink.bridge)).toBe(before);
        expect(events(a, "fabric.control.ack")).toEqual([]);
        expect(events(b, "fabric.control.command")).toEqual([]);
      };
      const release = await holdHubMirror(destinationLink.bridge);
      try {
        now += BRIDGE_LEASE_MS + 5_000;
        const firstPendingAt = now;
        await step();
        retained();
        let boundedFrom = firstPendingAt;
        if (mode !== "retained") {
          now += 2_000;
          boundedFrom = now;
          const cursor = readCursor();
          const { after, mark, offset } = cursor.toLocal;
          if (mode === "missing") {
            cursor.toLocal = { after, mark, ...(offset !== undefined ? { offset } : {}) };
          } else if (mode === "null-record" || mode === "primitive-record") {
            const fields = Object.keys(graceFields());
            for (const field of fields.length ? fields : ["relayGrace"]) {
              cursor.toLocal[field] = mode === "null-record" ? null : "not-a-grace-record";
            }
          } else {
            const invalid = mode === "Infinity" ? Infinity : mode === "NaN" ? NaN
              : mode === "future" ? now + 120_000 : "not-a-local-timestamp";
            const corrupt = (value: unknown): unknown => {
              if (typeof value === "number" && value >= firstPendingAt) return invalid;
              if (Array.isArray(value)) return value.map(corrupt);
              if (value && typeof value === "object") return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, corrupt(v)]));
              return value;
            };
            const fields = Object.keys(graceFields());
            for (const field of fields) cursor.toLocal[field] = corrupt(cursor.toLocal[field]);
            if (!fields.length) cursor.toLocal.relayGrace = { sequence: command.sequence, startedAt: invalid };
          }
          // JSON persists Infinity and NaN as null, exercising actual disk data.
          fs.writeFileSync(sourceLink.bridge.options.cursorPath, JSON.stringify(cursor));
          await restart();
          retained();
        }
        const persistedGrace = graceFields();
        now = boundedFrom + 4_000;
        await restart();
        await step();
        retained();
        expect(graceFields()).toEqual(persistedGrace);
        now = boundedFrom + 9_000;
        await restart();
        await step();
        retained();
        expect(graceFields()).toEqual(persistedGrace);
        now = boundedFrom + BRIDGE_LEASE_MS - 1;
        await step();
        retained();
        now = boundedFrom + BRIDGE_LEASE_MS;
        await step();
        expect(sourceCursor(sourceLink.bridge)).toBeGreaterThanOrEqual(command.sequence);
        expect(events(a, "fabric.control.ack")).toHaveLength(1);
        expect(events(a, "fabric.control.ack")[0]).toMatchObject({ verification: "bridge", to: source.identity.id,
          from: { id: "bridge:ryzen1" }, data: { accepted: false, retryable: true,
            errorCode: "FABRIC_RELAY_DESTINATION_UNAVAILABLE", bridge: { from: "ryzen1", id: command.id } } });
        expect(Object.keys(persistedGrace).length).toBeGreaterThan(0);
        expect(graceFields()).toEqual({});
        now += 1_000;
        await restart();
        await step();
        expect(events(a, "fabric.control.ack")).toHaveLength(1);
        expect(events(b, "fabric.control.command")).toEqual([]);
        expect(events(b, "fabric.control.ack")).toEqual([]);
        expect(events(hub, "fabric.control.command")).toEqual([]);
        expect(events(hub, "fabric.control.ack")).toEqual([]);
        expect(persistedMarkerFiles(hub.root, marker)).toEqual([]);
      } finally { await release(); }
    });

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
      const data = event.data as { accepted: boolean; bridge: { from: string; via: string } };
      return data.accepted && data.bridge.from === "ryzen2" && data.bridge.via === "ryzen1";
    })).toBe(true);
    // A cross-spoke ACK arrives outcome-only: the relaying hub drops the advisory fields for A too.
    for (const event of events(a, "fabric.control.ack")) {
      expect(event.data).not.toHaveProperty("triggered");
      expect(event.data).not.toHaveProperty("pendingFollowUps");
    }
    expect(events(b, "fleet.work.smarty-dev.10068")).toHaveLength(0);
    await f.drain();
    expect(events(b, "fabric.control.command")).toHaveLength(3);
    expect(events(a, "fabric.control.ack")).toHaveLength(3);
  });

  it("PR#1076 counterprobe: a cross-spoke result ACK B -> A is outcome-only in the hub log and at A's root", async () => {
    const f = await fixture();
    const a = f.stores.ryzen3!, b = f.stores.ryzen2!, hub = f.stores.ryzen1!;
    const source = f.mains.ryzen3!, target = f.mains.ryzen2!, local = f.mains.ryzen1!;
    const secret = "pr1076-result-secret-singleton";
    const directory = new ParticipantDirectory(a, { enabled: true, hostId: source.identity.id, rootId: source.identity.id, identity: source.identity });
    const hubDirectory = new ParticipantDirectory(hub, { enabled: true, hostId: local.identity.id, rootId: local.identity.id, identity: local.identity });
    const sender = new FabricControlPlane(a, source.identity, { enabled: true, hostId: source.identity.id, pollMs: 5,
      readMirroredOwner: (...args) => directory.mirroredControlOwner(...args) });
    const hubSender = new FabricControlPlane(hub, local.identity, { enabled: true, hostId: local.identity.id, ownerIncarnation: local.ownerIncarnation, pollMs: 5,
      readMirroredOwner: (...args) => hubDirectory.mirroredControlOwner(...args) });
    const receiver = new FabricControlPlane(b, target.identity, { enabled: true, hostId: target.identity.id, ownerIncarnation: target.ownerIncarnation, pollMs: 5 });
    const receive = vi.fn((command: { message?: string }) => ({ accepted: true, messageId: `on-B-${command.message}`, triggered: true,
      pendingFollowUps: 1, oldestAgeS: 0, result: { secret, for: command.message } }));
    sender.start(() => ({ accepted: false })); hubSender.start(() => ({ accepted: false })); receiver.start(receive as any);
    closers.push(async () => { await sender.close(); await hubSender.close(); await receiver.close(); });
    const pending = sender.request(target.identity.id, target.identity.id, "followUp",
      { message: "to-A" }, target.identity.id, { routedRemoteHost: "ryzen2" });
    void pending.catch(() => undefined);
    await vi.waitFor(() => expect(events(a, "fabric.control.command")).toHaveLength(1));
    await f.drain(); await f.drain();
    await vi.waitFor(() => expect(events(b, "fabric.control.ack")).toHaveLength(1));
    await f.drain(); await f.drain();
    // The hub only relays the B -> A ACK: its shared log holds no result and no non-relayed field.
    expectRelayedOnly(hub, secret, source.identity.id);
    // Spoke A's root gets the real FabricControlPlane outcome: accepted with B's messageId, no advisory fields.
    await expect(pending).resolves.toEqual({ queued: true, messageId: "on-B-to-A", routed: "mesh", acknowledged: true });
    expect(receive).toHaveBeenCalledOnce();
    expect(receive.mock.calls[0]).toEqual(expect.arrayContaining([expect.objectContaining({ id: source.identity.id, verified: "bridge" })]));
    // Origin authentication still holds at A: B's presence identity, stamped from ryzen2 via ryzen1.
    expect(events(a, "fabric.control.ack")).toHaveLength(1);
    expect(events(a, "fabric.control.ack")[0]).toMatchObject({ from: target.identity, to: source.identity.id,
      data: { accepted: true, messageId: "on-B-to-A", bridge: { from: "ryzen2", via: "ryzen1" } } });
    // A receives RELAYED_ACK_FIELDS only: no result, triggered, pendingFollowUps or oldestAgeS.
    expectRelayedOnly(a, secret, source.identity.id);
    // Target validation: an ACK whose target is not bound to B's link reaches neither the hub nor A.
    await b.publish({ topic: "fabric.control.ack", kind: "accepted", from: target.identity, to: source.identity.id,
      data: { version: 1, commandId: "forged-target", targetId: local.identity.id, accepted: true, result: { secret } } });
    await f.drain(); await f.drain();
    expect(events(hub, "fabric.control.ack").map(event => (event.data as { commandId: string }).commandId)).not.toContain("forged-target");
    expect(events(a, "fabric.control.ack")).toHaveLength(1);
    // Counterexample: an ACK addressed to the hub's own native root is hub-local and stays full there.
    const hubLocal = hubSender.requestResult<{ secret: string; for: string }>(target.identity.id, target.identity.id, "followUp",
      { message: "to-hub" }, target.identity.id, { routedRemoteHost: "ryzen2" });
    void hubLocal.catch(() => undefined);
    await vi.waitFor(() => expect(events(hub, "fabric.control.command").length).toBeGreaterThanOrEqual(1));
    await f.drain();
    await vi.waitFor(() => expect(events(b, "fabric.control.ack")).toHaveLength(3));
    await f.drain();
    await expect(hubLocal).resolves.toEqual({ secret, for: "to-hub" });
    expect(events(hub, "fabric.control.ack").find(event => event.to === local.identity.id)).toMatchObject({
      data: { triggered: true, result: { secret, for: "to-hub" }, bridge: { from: "ryzen2" } } });
    expect(events(a, "fabric.control.ack")).toHaveLength(1); // spoke A never sees the hub-local result
    // After the forged and hub-local ACKs, the hub's B -> A copy and A's copy both stay outcome-only.
    expectRelayedOnly(hub, secret, source.identity.id);
    expectRelayedOnly(a, secret, source.identity.id);
  });

  it("PR#1076 counterprobe: an actual publishBatch of cross-spoke result ACKs B -> A is outcome-only in the hub log and at A", async () => {
    const f = await fixture();
    const a = f.stores.ryzen3!, b = f.stores.ryzen2!, hub = f.stores.ryzen1!;
    const source = f.mains.ryzen3!, target = f.mains.ryzen2!;
    const secret = "pr1076-result-secret-batch";
    await Promise.all(f.links.map(link => link.remote.hello())); // actual publishBatch RPC capability
    const batches = vi.spyOn(StoreBridgeSide.prototype, "publishBatch");
    const commands = ["one", "two", "three"];
    for (const commandId of commands) {
      await a.publish({ topic: "fabric.control.command", kind: "followUp", from: source.identity, to: target.identity.id,
        data: { version: 1, commandId, targetId: target.identity.id, operation: "followUp", replyTo: source.identity.id,
          message: commandId, destinationRemoteHost: "ryzen2" } });
    }
    await f.drain(); await f.drain();
    expect(events(b, "fabric.control.command")).toHaveLength(3);
    batches.mockClear();
    for (const commandId of commands) {
      await b.publish({ topic: "fabric.control.ack", kind: "accepted", from: target.identity, to: source.identity.id,
        data: { version: 1, commandId, targetId: target.identity.id, accepted: true, triggered: true, pendingFollowUps: 1,
          result: { secret, commandId } } });
    }
    await f.drain(); await f.drain();
    // Both hops really batched: B -> hub (local side) and hub -> A (served over the RPC).
    expect(batches.mock.calls.filter(([batch]) => batch.length === 3)).toHaveLength(2);
    // The hub only relays: its shared log holds no result and no non-relayed field.
    expectRelayedOnly(hub, secret, source.identity.id);
    expect(events(a, "fabric.control.ack").map(event => (event.data as { commandId: string }).commandId)).toEqual(commands);
    for (const event of events(a, "fabric.control.ack")) {
      expect(event).toMatchObject({ from: target.identity, data: { accepted: true, targetId: target.identity.id,
        bridge: { from: "ryzen2", via: "ryzen1" } } });
    }
    // A receives RELAYED_ACK_FIELDS only: no result, triggered or pendingFollowUps.
    expectRelayedOnly(a, secret, source.identity.id);
  });

  it.each([false, true])("P1-b suppresses sensitive relayed ACK text in persisted hub/spoke stores (negotiated batch=%s), not hub-local text", async negotiatedBatch => {
    const f = await fixture();
    const a = f.stores.ryzen3!, b = f.stores.ryzen2!, hub = f.stores.ryzen1!;
    const source = f.mains.ryzen3!, target = f.mains.ryzen2!, local = f.mains.ryzen1!;
    if (negotiatedBatch) {
      await Promise.all(f.links.map(link => link.remote.hello()));
      expect(f.links.every(link => typeof link.remote.publishBatch === "function")).toBe(true);
    } else {
      expect(f.links.every(link => link.remote.publishBatch === undefined)).toBe(true);
    }
    const batches = vi.spyOn(StoreBridgeSide.prototype, "publishBatch");
    const count = negotiatedBatch ? 3 : 1;
    const markers = Array.from({ length: count }, (_, index) => `pr1076-ack-text-${negotiatedBatch}-${index}-PRIVATE`);
    for (const [index, marker] of markers.entries()) {
      await b.publish({ topic: "fabric.control.ack", kind: "accepted", from: target.identity, to: source.identity.id,
        text: marker, data: { version: 1, commandId: `text-${index}`, targetId: target.identity.id,
          ownerIncarnation: target.ownerIncarnation, accepted: true, messageId: `delivered-${index}`,
          triggered: true, pendingFollowUps: 1, result: { marker } } });
    }
    // Positive control: these are real native records containing the sensitive text at B.
    expect(events(b, "fabric.control.ack").map(event => event.text)).toEqual(markers);
    for (const marker of markers) expect(persistedMarkerFiles(b.root, marker).length).toBeGreaterThan(0);
    await f.drain(); await f.drain();
    expect(events(hub, "fabric.control.ack")).toHaveLength(count);
    expect(events(a, "fabric.control.ack")).toHaveLength(count);
    for (const [index, ack] of events(a, "fabric.control.ack").entries()) {
      expect(ack).toMatchObject({ from: target.identity, to: source.identity.id, verification: "bridge",
        data: { commandId: `text-${index}`, targetId: target.identity.id, accepted: true,
          messageId: `delivered-${index}`, ownerIncarnation: target.ownerIncarnation,
          bridge: { from: "ryzen2", via: "ryzen1" } } });
    }
    if (negotiatedBatch) {
      // Both actual store append boundaries: B -> hub and negotiated RPC hub -> A.
      expect(batches.mock.calls.filter(([batch]) => batch.length === count)).toHaveLength(2);
    } else expect(batches).not.toHaveBeenCalled();
    for (const marker of markers) {
      expectRelayedOnly(hub, marker, source.identity.id);
      expectRelayedOnly(a, marker, source.identity.id);
      expect(persistedMarkerFiles(hub.root, marker)).toEqual([]);
      expect(persistedMarkerFiles(a.root, marker)).toEqual([]);
      for (const link of f.links) expect(fs.readFileSync(link.bridge.options.cursorPath, "utf8")).not.toContain(marker);
    }
    // Exact branch counterexample: a native hub recipient still receives text and full data.
    const hubMarker = `pr1076-hub-local-ack-text-${negotiatedBatch}`;
    await b.publish({ topic: "fabric.control.ack", kind: "accepted", from: target.identity, to: local.identity.id,
      text: hubMarker, data: { version: 1, commandId: "hub-local-text", targetId: target.identity.id,
        accepted: true, messageId: "hub-delivered", triggered: true, result: { marker: hubMarker } } });
    await f.drain(); await f.drain();
    expect(events(hub, "fabric.control.ack").find(event => event.to === local.identity.id)).toMatchObject({
      text: hubMarker, data: { triggered: true, result: { marker: hubMarker }, bridge: { from: "ryzen2" } } });
    expect(persistedMarkerFiles(hub.root, hubMarker).length).toBeGreaterThan(0);
    expect(events(a, "fabric.control.ack")).toHaveLength(count);
    expect(persistedMarkerFiles(a.root, hubMarker)).toEqual([]);
    // A subsequent drain must not restore the private text or duplicate either receipt.
    await f.drain();
    expect(events(hub, "fabric.control.ack")).toHaveLength(count + 1);
    expect(events(a, "fabric.control.ack")).toHaveLength(count);
    for (const marker of markers) expect(persistedMarkerFiles(hub.root, marker)).toEqual([]);
  });

  it.each([false, true])("P1 legacy replay strips an old FULL hub ACK before sending it to A (negotiated batch=%s)", async negotiatedBatch => {
    const f = await fixture();
    const a = f.stores.ryzen3!, hub = f.stores.ryzen1!;
    const source = f.mains.ryzen3!, target = f.mains.ryzen2!;
    if (negotiatedBatch) await Promise.all(f.links.map(link => link.remote.hello()));
    const markers = Array.from({ length: negotiatedBatch ? 3 : 1 }, (_, index) => `pr1076-old-full-ack-${negotiatedBatch}-${index}`);
    // Seed historical hub bytes through the real bridge store publication boundary. The
    // upstream ingress scrub is intentionally bypassed: a pre-fix hub already holds these.
    const oldHubIngress = new StoreBridgeSide(hub, "ryzen2");
    for (const [index, marker] of markers.entries()) {
      await oldHubIngress.publish({ topic: "fabric.control.ack", kind: "accepted", from: target.identity, to: source.identity.id,
        text: marker, data: { version: 1, commandId: `legacy-${index}`, targetId: target.identity.id,
          ownerIncarnation: target.ownerIncarnation, accepted: true, messageId: `old-delivered-${index}`,
          triggered: true, pendingFollowUps: 1, result: { marker },
          bridge: { from: "ryzen2", id: `old-bridge-id-${index}` } } });
    }
    expect(events(hub, "fabric.control.ack").map(ack => ack.text)).toEqual(markers);
    for (const marker of markers) expect(persistedMarkerFiles(hub.root, marker).length).toBeGreaterThan(0);
    const batches = vi.spyOn(StoreBridgeSide.prototype, "publishBatch");
    await f.drain(); await f.drain();
    expect(events(a, "fabric.control.ack")).toHaveLength(markers.length);
    for (const [index, ack] of events(a, "fabric.control.ack").entries()) {
      expect(ack).toMatchObject({ verification: "bridge", from: target.identity, to: source.identity.id,
        data: { commandId: `legacy-${index}`, targetId: target.identity.id, accepted: true,
          messageId: `old-delivered-${index}`, ownerIncarnation: target.ownerIncarnation,
          bridge: { from: "ryzen2", via: "ryzen1" } } });
    }
    if (negotiatedBatch) expect(batches.mock.calls.filter(([batch]) => batch.length === markers.length)).toHaveLength(1);
    else expect(batches).not.toHaveBeenCalled();
    for (const marker of markers) {
      expectRelayedOnly(a, marker, source.identity.id);
      expect(persistedMarkerFiles(a.root, marker)).toEqual([]);
    }
    // The legacy payload really remains in the old hub log: this is egress prevention,
    // not a source assertion or a test that accidentally seeded already-scrubbed input.
    expect(events(hub, "fabric.control.ack")).toHaveLength(markers.length);
    await f.drain();
    expect(events(a, "fabric.control.ack")).toHaveLength(markers.length);
  });

  it("admits only a correlated captured-hub refusal; forged/body-bearing refusals do not steal a normal owner ACK", async () => {
    const f = await fixture();
    const a = f.stores.ryzen3!;
    const source = f.mains.ryzen3!, target = f.mains.ryzen2!;
    const directory = new ParticipantDirectory(a, { enabled: true, hostId: source.identity.id, rootId: source.identity.id, identity: source.identity });
    const sender = new FabricControlPlane(a, source.identity, { enabled: true, hostId: source.identity.id,
      readMirroredOwner: (...args) => directory.mirroredControlOwner(...args) });
    sender.start(() => ({ accepted: false }));
    closers.push(() => sender.close());
    const hubIdentity: MeshIdentity = { id: "bridge:ryzen1", name: "mesh-bridge", kind: "main" };
    const sourceIngress = new StoreBridgeSide(a, "ryzen1");
    const cases: Array<{ label: string; native?: boolean; from?: MeshIdentity; data?: Record<string, unknown>; text?: string; wrongEvent?: boolean }> = [
      { label: "native unverified hub lookalike", native: true },
      { label: "forged destination owner", from: target.identity },
      { label: "wrong bridge identity", from: { ...hubIdentity, id: "bridge:ryzen2" } },
      { label: "wrong command", data: { commandId: "not-pending" } },
      { label: "wrong target", data: { targetId: source.identity.id } },
      { label: "wrong source event", wrongEvent: true },
      { label: "owner epoch claim", data: { ownerIncarnation: target.ownerIncarnation } },
      { label: "notRun claim", data: { notRun: true } },
      { label: "notDelivered claim", data: { notDelivered: true } },
      { label: "private result", data: { result: { secret: "refusal-private-result" } } },
      { label: "private message", data: { message: "refusal-private-message" } },
      { label: "private text", text: "refusal-private-text" },
    ];
    for (const [index, invalid] of cases.entries()) {
      const pending = sender.request(target.identity.id, target.identity.id, "followUp",
        { message: invalid.label, ownerIncarnation: target.ownerIncarnation }, target.identity.id,
        { routedRemoteHost: "ryzen2", idempotencyKey: `refusal-case-${index}` });
      void pending.catch(() => undefined);
      await vi.waitFor(() => expect(events(a, "fabric.control.command")).toHaveLength(index + 1));
      const command = events(a, "fabric.control.command")[index]!;
      const commandId = (command.data as { commandId: string }).commandId;
      const data = { version: 1, commandId, targetId: target.identity.id, accepted: false,
        error: "Relay destination unavailable", errorCode: "FABRIC_RELAY_DESTINATION_UNAVAILABLE", retryable: true,
        bridge: { from: "ryzen1", id: invalid.wrongEvent ? "not-the-source-event" : command.id }, ...invalid.data };
      if (invalid.native) await a.publish({ topic: "fabric.control.ack", kind: "rejected", from: hubIdentity, to: source.identity.id, data });
      else await sourceIngress.publish({ topic: "fabric.control.ack", kind: "rejected", from: invalid.from ?? hubIdentity,
        to: source.identity.id, data, ...(invalid.text !== undefined ? { text: invalid.text } : {}) });
      // A real, origin-authenticated owner ACK is the behavioral sentinel: an invalid
      // infrastructure refusal must not settle/reject this pending request first.
      await sourceIngress.publish({ topic: "fabric.control.ack", kind: "accepted", from: target.identity, to: source.identity.id,
        data: { version: 1, commandId, targetId: target.identity.id, ownerIncarnation: target.ownerIncarnation,
          accepted: true, messageId: `owner-ack-${index}`, bridge: { from: "ryzen2", id: `owner-event-${index}`, via: "ryzen1" } } }, [], true);
      await expect(pending, invalid.label).resolves.toMatchObject({ acknowledged: true, messageId: `owner-ack-${index}` });
    }
    const valid = sender.request(target.identity.id, target.identity.id, "followUp",
      { message: "valid-refusal", ownerIncarnation: target.ownerIncarnation }, target.identity.id,
      { routedRemoteHost: "ryzen2", idempotencyKey: "valid-refusal-stable-key" });
    void valid.catch(() => undefined);
    await vi.waitFor(() => expect(events(a, "fabric.control.command")).toHaveLength(cases.length + 1));
    const command = events(a, "fabric.control.command").at(-1)!;
    await sourceIngress.publish({ topic: "fabric.control.ack", kind: "rejected", from: hubIdentity, to: source.identity.id,
      data: { version: 1, commandId: (command.data as { commandId: string }).commandId, targetId: target.identity.id,
        accepted: false, error: "Relay destination unavailable", errorCode: "FABRIC_RELAY_DESTINATION_UNAVAILABLE", retryable: true,
        bridge: { from: "ryzen1", id: command.id } } });
    await expect(valid).rejects.toMatchObject({ code: "FABRIC_RELAY_DESTINATION_UNAVAILABLE", retryable: true,
      idempotencyKey: "valid-refusal-stable-key" });
  });

  it("lost relay receipt retries the same sender key with exactly one destination append and handler delivery", async () => {
    let attempts = 0;
    const f = await fixture(60_000, undefined, async () => {
      if (++attempts === 1) throw Object.assign(new Error("Fixture dropped response after durable destination append"), {
        code: "FABRIC_RELAY_DESTINATION_UNAVAILABLE", retryable: true });
    });
    const a = f.stores.ryzen3!, b = f.stores.ryzen2!, hub = f.stores.ryzen1!;
    const source = f.mains.ryzen3!, target = f.mains.ryzen2!;
    const directory = new ParticipantDirectory(a, { enabled: true, hostId: source.identity.id, rootId: source.identity.id, identity: source.identity });
    const sender = new FabricControlPlane(a, source.identity, { enabled: true, hostId: source.identity.id,
      readMirroredOwner: (...args) => directory.mirroredControlOwner(...args) });
    const receiver = new FabricControlPlane(b, target.identity, { enabled: true, hostId: target.identity.id, ownerIncarnation: target.ownerIncarnation });
    sender.start(() => ({ accepted: false }));
    // Hold owner execution until the same-key retry is pending. This loses the relay
    // response AFTER the real append, not a pre-dispatch error or a mock-call assertion.
    const receive = vi.fn(() => ({ accepted: true, messageId: "one-real-delivery" }));
    closers.push(async () => { await sender.close(); await receiver.close(); });
    const marker = "pr1076-lost-relay-response-private";
    const input = { message: marker, data: { marker }, ownerIncarnation: target.ownerIncarnation };
    const options = { routedRemoteHost: "ryzen2", idempotencyKey: "lost-response-stable-key" };
    const first = sender.request(target.identity.id, target.identity.id, "followUp", input, target.identity.id, options);
    void first.catch(() => undefined);
    await vi.waitFor(() => expect(events(a, "fabric.control.command")).toHaveLength(1));
    await f.drain().catch(error => expect(error).toMatchObject({ code: "FABRIC_RELAY_DESTINATION_UNAVAILABLE" }));
    expect(events(b, "fabric.control.command")).toHaveLength(1); // append preceded loss
    expect(receive).not.toHaveBeenCalled();
    await expect(first).rejects.toMatchObject({ code: "FABRIC_RELAY_DESTINATION_UNAVAILABLE", retryable: true });
    const retry = sender.request(target.identity.id, target.identity.id, "followUp", input, target.identity.id, options);
    void retry.catch(() => undefined);
    await vi.waitFor(() => expect(events(a, "fabric.control.command")).toHaveLength(2));
    const sourceCommands = events(a, "fabric.control.command");
    expect((sourceCommands[0]!.data as { commandId: string }).commandId)
      .toBe((sourceCommands[1]!.data as { commandId: string }).commandId);
    await f.drain(); await f.drain();
    expect(attempts).toBeGreaterThanOrEqual(2);
    expect(events(b, "fabric.control.command")).toHaveLength(1);
    receiver.start(receive);
    await vi.waitFor(() => expect(events(b, "fabric.control.ack")).toHaveLength(1));
    await f.drain(); await f.drain();
    await expect(retry).resolves.toMatchObject({ acknowledged: true, messageId: "one-real-delivery" });
    expect(receive).toHaveBeenCalledOnce();
    expect(receive.mock.calls[0]).toEqual(expect.arrayContaining([expect.objectContaining({ message: marker }),
      expect.objectContaining({ id: source.identity.id, verified: "bridge" })]));
    expect(events(b, "fabric.control.command")).toHaveLength(1);
    expect(persistedMarkerFiles(b.root, marker).length).toBeGreaterThan(0);
    expect(persistedMarkerFiles(hub.root, marker)).toEqual([]);
    for (const receipt of events(hub, "fabric.control.command")) {
      for (const field of ["data", "text", "message"]) expect(receipt).not.toHaveProperty(field);
    }
  });

  it("P2 preserves a token-bearing origin's incarnation in both direct and re-exported mirror leases", async () => {
    const token = "pr1076-origin-incarnation-token";
    const f = await fixture(60_000, token);
    const b = f.stores.ryzen2!, hub = f.stores.ryzen1!, a = f.stores.ryzen3!;
    const target = f.mains.ryzen2!;
    for (const store of [b, hub, a]) {
      expect(store.get(hostKey(target.identity.id), { fresh: true })!.value).toMatchObject({ incarnationToken: token });
      expect(readHostLease(store.root, target.identity.id)).toMatchObject({ id: target.identity.id,
        rootId: target.identity.id, identityId: target.identity.id, startedAt: target.host.startedAt, incarnationToken: token });
    }
    const reexport = a.get(hostKey(target.identity.id), { fresh: true })!.value;
    expect(reexport).toMatchObject({ remoteHost: "ryzen2", bridgeVia: "ryzen1" });
    expect((await new StoreBridgeSide(a, "ryzen1").owned()).hosts.map(host => host.record))
      .toEqual(expect.arrayContaining([expect.objectContaining({ id: target.identity.id, incarnationToken: token })]));
    await f.drain(); await f.drain();
    for (const store of [hub, a]) expect(readHostLease(store.root, target.identity.id)).toMatchObject({ incarnationToken: token });
    expect(removals).toEqual([]); // no product lease removal is required for this counterprobe
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
