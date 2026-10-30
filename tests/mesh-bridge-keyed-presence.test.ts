// Keep: real lease-renewal/listing incident, incarnation/ownership and orphan-authority boundaries.
// Only notification delivery and the full-reader comparison switch are controlled; stores,
// lease files, parsing, presence admission, snapshot classification and mirror custody are real.
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { BridgeChange } from "../src/mesh/change-notifier.js";
import type { BridgePresence } from "../src/mesh/bridge.js";
import type { FabricHostRecord } from "../src/topology/types.js";
import type { FabricHostLease } from "../src/topology/host-leases.js";

const observation = vi.hoisted(() => ({
  full: false,
  windowsStamps: false,
  measure: false,
  listingSites: [] as string[][],
  notify: new Map<string, Set<(kind: BridgeChange) => void>>(),
}));
vi.mock("../src/mesh/change-notifier.js", () => ({
  watchBridgeStore: (root: string, changed: (kind: BridgeChange) => void) => {
    const callbacks = observation.notify.get(root) ?? new Set();
    observation.notify.set(root, callbacks);
    callbacks.add(changed);
    return () => { callbacks.delete(changed); };
  },
}));
vi.mock("../src/topology/host-leases.js", async importOriginal => {
  const actual = await importOriginal<typeof import("../src/topology/host-leases.js")>();
  return { ...actual, readHostLeases: vi.fn(actual.readHostLeases),
    readHostLeasesFor: (root: string, ids: Iterable<string>) => observation.full
      ? actual.readHostLeases(root) : actual.readHostLeasesFor(root, ids) };
});

// Exercise the real Windows metadata-only directory-stamp implementation on Linux too.
// Scope the platform override to this synchronous function, never to stores or custody.
vi.mock("../src/topology/publication-generation.js", async importOriginal => {
  const actual = await importOriginal<typeof import("../src/topology/publication-generation.js")>();
  return { ...actual, meshDirectoryStamp: (root: string, name: string) => {
    if (!observation.windowsStamps) return actual.meshDirectoryStamp(root, name);
    const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
    Object.defineProperty(process, "platform", { value: "win32" });
    try { return actual.meshDirectoryStamp(root, name); }
    finally { Object.defineProperty(process, "platform", platform); }
  } };
});

const { MeshBridge, StoreBridgeSide } = await import("../src/mesh/bridge.js");
const { MeshStore } = await import("../src/mesh/store.js");
const { hostLeasePath, readHostLease, readHostLeases, writeHostLease } = await import("../src/topology/host-leases.js");
const NOW = 1_800_000_000_000;
const key = (kind: string, id: string) => `topology/${kind}/${createHash("sha256").update(id).digest("hex")}`;
const turn = () => new Promise<void>(resolve => setImmediate(resolve));
const closers: Array<() => void> = [];
afterEach(() => {
  for (const close of closers.splice(0).reverse()) close();
  vi.restoreAllMocks();
  vi.mocked(readHostLeases).mockClear();
  observation.notify.clear();
  observation.full = false;
  observation.windowsStamps = false;
  observation.measure = false;
  observation.listingSites.length = 0;
  // Preserve owned fixture files, including displaced versions, as test evidence.
});
const claimed = (name: string, remoteHost?: string): BridgePresence => {
  const id = `session:${name}-00000000`;
  const identity = { id, name, kind: "main" as const, sessionId: `${name}-00000000` };
  return { hosts: [{ record: { format: 1, id, rootId: id, identity, startedAt: NOW - 10_000,
    incarnationToken: `${name}-token`, updatedAt: NOW - 1_000, expiresAt: NOW - 1,
    ...(remoteHost ? { remoteHost } : {}) }, expiresAt: NOW - 1, observedAt: NOW }],
  participants: [{ format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id,
    name, label: name, status: "idle", runner: "pi", transport: "host", capabilities: ["steer"],
    sessionId: identity.sessionId, startedAt: NOW - 10_000, updatedAt: NOW, controlProtocol: "v1",
    ...(remoteHost ? { remoteHost } : {}) }], reserved: [] };
};
const fixture = async (backend: "file" | "sqlite" = "sqlite") => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-keyed-presence-"));
  const store = new MeshStore(root, 64 * 1024, 100, { stateBackend: backend });
  closers.push(() => store.closeState());
  let now = NOW;
  const side = new StoreBridgeSide(store, "far", () => now);
  const native = claimed("native"), second = claimed("second"), badOwner = claimed("bad-owner");
  badOwner.participants[0]!.ownerIdentityId = "session:wrong-owner-00000000";
  const relay = claimed("relay", "other"), owned = claimed("owned", "far"), badWriter = claimed("bad-writer");
  const child: FabricHostRecord = { ...native.hosts[0]!.record, id: "runtime:worker",
    identity: { id: "worker", name: "worker", kind: "agent" } };
  const records = [...[native, second, badOwner, relay, owned, badWriter].flatMap(p => p.hosts.map(h => h.record)), child];
  for (const record of records) {
    const identity = record.id === badWriter.hosts[0]!.record.id ? native.hosts[0]!.record.identity : record.identity;
    await store.put({ key: key("hosts", record.id), value: record, identity });
  }
  for (const p of [native, second, badOwner, relay, owned, badWriter]) {
    await store.put({ key: key("participants", p.participants[0]!.id), value: p.participants[0], identity: p.hosts[0]!.record.identity });
  }
  const renew = (host: FabricHostRecord, expiresAt: number, extra: Partial<FabricHostLease> = {}) => {
    writeHostLease(root, { id: host.id, rootId: host.rootId, identityId: host.identity.id,
      startedAt: host.startedAt, ...(host.incarnationToken !== undefined ? { incarnationToken: host.incarnationToken } : {}),
      updatedAt: now, expiresAt, ...extra });
  };
  for (let i = 0; i < 1_000; i++) {
    const id = `session:orphan-${i}-00000000`;
    writeHostLease(root, { id, rootId: id, identityId: id, updatedAt: now, expiresAt: now + (i % 5 ? -1 : 60_000) });
  }
  for (const host of records) renew(host, now + 30_000);
  const directory = path.join(root, "host-leases");
  let listings = 0;
  const readdir = fs.readdirSync;
  vi.spyOn(fs, "readdirSync").mockImplementation(((file: fs.PathLike, ...args: unknown[]) => {
    if (String(file) === directory) {
      listings++;
      if (observation.measure) observation.listingSites.push((new Error().stack ?? "").split("\n")
        .filter(line => line.includes("/src/")).slice(0, 5).map(line => line.trim()));
    }
    return Reflect.apply(readdir, fs, [file, ...args]);
  }) as typeof fs.readdirSync);
  const reads = async () => ({ presence: await side.presence(), relays: await side.relayPresence(), owned: await side.owned(),
    origin: side.relayOrigin(relay.hosts[0]!.record.id), stale: side.relayStaleOrigin(relay.hosts[0]!.record.id) });
  const compare = async (live: string[], relayLive = true, ownedLive = true) => {
    const before = listings;
    const actual = await reads(); // keyed first: the full oracle must not prime changed files.
    const keyedListings = listings - before;
    observation.full = true;
    const expected = await reads();
    observation.full = false;
    expect(actual).toEqual(expected);
    expect(actual.presence.hosts.map(h => h.record.id).sort()).toEqual(live.sort());
    expect(actual.presence.participants.some(p => p.id === badOwner.participants[0]!.id)).toBe(false);
    expect(actual.presence.hosts.some(h => h.record.id === badWriter.hosts[0]!.record.id)).toBe(false);
    expect(actual.relays.hosts.map(h => h.record.id)).toEqual(relayLive ? [relay.hosts[0]!.record.id] : []);
    expect(actual.owned.hosts.map(h => h.record.id)).toEqual(ownedLive ? [owned.hosts[0]!.record.id] : []);
    expect(actual.origin).toBe(relayLive ? "other" : undefined);
    expect(actual.stale).toBe(relayLive ? undefined : "other");
    expect(keyedListings, "all read-only presence/relay/owned/origin paths must be keyed").toBe(0);
    return actual;
  };
  return { root, store, side, native: native.hosts[0]!.record, second: second.hosts[0]!.record,
    badOwner: badOwner.hosts[0]!.record, relay: relay.hosts[0]!.record, owned: owned.hosts[0]!.record, child,
    renew, compare, listings: () => listings, now: () => now, advance: (ms: number) => { now += ms; } };
};

describe("real StoreBridgeSide keyed lease consumers", () => {
  it.each(["file", "sqlite"] as const)("matches full reads through renewal, in-place change, expiry, removal and incarnations (%s)", async backend => {
    const f = await fixture(backend);
    const all = [f.native.id, f.second.id, f.badOwner.id, f.child.id];
    await f.compare(all);
    f.advance(1_000);
    f.renew(f.native, f.now() + 45_000); // atomic replacement
    await f.compare(all);
    fs.writeFileSync(hostLeasePath(f.root, f.second.id), JSON.stringify({ format: 1,
      ...readHostLease(f.root, f.second.id)!, updatedAt: f.now(), expiresAt: f.now() - 1 }));
    await f.compare(all.filter(id => id !== f.second.id));
    // Same identity and start time, different incarnation: cannot revive the old host row.
    f.renew(f.second, f.now() + 60_000, { incarnationToken: "successor" });
    await f.compare(all.filter(id => id !== f.second.id));
    await f.store.put({ key: key("hosts", f.second.id), identity: f.second.identity,
      value: { ...f.second, incarnationToken: "successor" } });
    await f.compare(all);
    // Remove without deleting evidence; recreating the same pathname must be observed now.
    fs.renameSync(hostLeasePath(f.root, f.native.id), path.join(f.root, "displaced-native-lease.json"));
    await f.compare(all.filter(id => id !== f.native.id));
    f.renew(f.native, f.now() + 45_000);
    await f.compare(all);
    f.renew(f.native, f.now() + 45_000, { identityId: "session:other-owner-00000000" });
    await f.compare(all.filter(id => id !== f.native.id));
    f.renew(f.native, f.now() + 45_000);
    f.advance(60_001); // no file changes: expired remains expired, including relay and owned.
    await f.compare([], false, false);
  });

  it.each([false, true])("classifies real notification snapshots identically without listing after each renewal (Windows stamps=%s)", async windowsStamps => {
    const f = await fixture();
    observation.windowsStamps = windowsStamps;
    const oracle: BridgeChange[] = [], keyed: BridgeChange[] = [], failures: Error[] = [];
    const beforeBaseline = f.listings();
    closers.push(f.side.subscribeChanges(kind => keyed.push(kind), error => failures.push(error)));
    expect(f.listings() - beforeBaseline).toBe(0);
    observation.full = true;
    closers.push(f.side.subscribeChanges(kind => oracle.push(kind), error => failures.push(error)));
    observation.full = false;
    const callbacks = [...observation.notify.get(f.root)!];
    const check = async (expected: BridgeChange[]) => {
      const before = f.listings();
      callbacks[0]!("presence");
      await turn();
      expect(f.listings() - before, "one notification capture must not enumerate leases").toBe(0);
      observation.full = true;
      callbacks[1]!("presence");
      await turn();
      observation.full = false;
      expect(keyed.splice(0)).toEqual(expected);
      expect(oracle.splice(0)).toEqual(expected);
      expect(failures).toEqual([]);
    };
    await check([]);
    f.renew(f.native, f.now() + 45_000);
    await check(["lease"]);
    f.renew(f.native, f.now() + 35_000);
    await check(["presence"]);
    fs.renameSync(hostLeasePath(f.root, f.owned.id), path.join(f.root, "displaced-owned-lease.json"));
    await check(["presence"]); // own-mirror lease existence is part of the fingerprint.
    f.renew(f.owned, f.now() + 30_000);
    await check(["presence"]);
    f.renew(f.owned, f.now() + 30_000, { incarnationToken: "owned-successor" });
    await check(["presence"]);
    f.advance(60_001);
    await check(["presence"]);
  });

  it("retains orphan native aliases for handoffs and orphan expired aliases for mirror collision/history", async () => {
    const f = await fixture();
    const moved = claimed("moved");
    const id = moved.hosts[0]!.record.id;
    await f.store.put({ key: key("handoffs", id), identity: f.native.identity, value: {
      format: 1, id, sessionId: id.slice("session:".length), name: "moved", link: "far",
      handedOffAt: NOW - 1_000, operator: "operator", hub: "hub" } });
    const orphan = "session:unknown-file-owner-00000000";
    const writeAlias = (expiresAt: number) => writeHostLease(f.root, { id: orphan, rootId: id, identityId: id,
      startedAt: NOW - 10_000, incarnationToken: "new-incarnation", bridgeSupersededTokens: ["old-incarnation"],
      updatedAt: NOW, expiresAt,
      session: { id, startedAt: NOW - 10_000, updatedAt: NOW, expiresAt } });
    writeAlias(NOW + 30_000);
    const full = vi.mocked(readHostLeases);
    full.mockClear();
    expect((await f.side.presence()).reserved).toContain(id);
    expect(full).toHaveBeenCalled(); // absence of a host row is NOT absence of authority.
    writeAlias(NOW - 1);
    expect((await f.side.presence()).reserved).not.toContain(id);
    // The handoff is in force after expiry, but the mirror's complete lease scan must
    // still see the expired orphan's incarnation history and refuse a superseded token.
    moved.hosts[0]!.record.incarnationToken = "old-incarnation";
    moved.hosts[0]!.record.startedAt = NOW - 20_000;
    moved.hosts[0]!.record.updatedAt = NOW;
    moved.hosts[0]!.record.expiresAt = NOW + 30_000;
    moved.hosts[0]!.expiresAt = NOW + 30_000;
    await expect(f.side.mirror(moved, NOW)).rejects.toThrow(/incarnation|stale/i);
    expect(f.store.get(key("hosts", id), { fresh: true })).toBeUndefined();
  });

  it("preserves an expired orphan's cross-root collision even with no handoff rows", async () => {
    const f = await fixture();
    const incoming = claimed("collision");
    const host = incoming.hosts[0]!;
    host.record.updatedAt = NOW;
    host.record.expiresAt = host.expiresAt = NOW + 30_000;
    const foreignRoot = "session:foreign-root-00000000";
    writeHostLease(f.root, { id: "session:orphan-collision-00000000", rootId: foreignRoot,
      identityId: host.record.id, startedAt: NOW - 20_000, incarnationToken: "orphan-token",
      updatedAt: NOW - 10_000, expiresAt: NOW - 1 });
    // A partial frame keeps unrelated omissions untouched; this checks only collision admission.
    await f.side.mirror({ ...incoming, partial: true }, NOW);
    expect(f.store.get(key("hosts", host.record.id), { fresh: true })).toBeUndefined();
    expect(readHostLease(f.root, "session:orphan-collision-00000000")?.rootId).toBe(foreignRoot);
  });

  it("measures a real MeshBridge.syncPresence and leaves global mirror/custody scans explicit", async () => {
    const f = await fixture();
    const farRoot = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-keyed-far-"));
    const farStore = new MeshStore(farRoot, 64 * 1024, 100, { stateBackend: "sqlite" });
    closers.push(() => farStore.closeState());
    const remote = new StoreBridgeSide(farStore, "hub", f.now);
    const bridge = new MeshBridge({ localName: "hub", remoteName: "far", local: f.side, remote,
      cursorPath: path.join(f.root, "cursor.json"), clock: f.now });
    const full = vi.mocked(readHostLeases);
    full.mockClear();
    const before = f.listings();
    observation.measure = true;
    await bridge.syncPresence();
    observation.measure = false;
    const mirrors = await remote.owned();
    expect(mirrors.hosts.map(h => h.record.id)).toEqual(expect.arrayContaining([f.native.id, f.second.id, f.child.id]));
    const measured = { localLeaseListings: f.listings() - before,
      fullAuthorityReads: full.mock.calls.filter(([root]) => root === f.root).length,
      listingSites: observation.listingSites };
    console.log("real syncPresence remaining global authority/custody:", JSON.stringify(measured));
    // This bounded change does not pretend mirror planning and its commit-time authority
    // validation are keyed. A whole-sync <=1 budget remains an explicit integration limit.
    expect(measured.fullAuthorityReads).toBeGreaterThan(0);
  });
});
