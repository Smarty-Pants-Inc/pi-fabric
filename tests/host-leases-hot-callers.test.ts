// Keep reason: data-safety/trust-boundary regression for arbitrary-id lease-only authority.
// Real lease files + real comms/directory entrypoints; an immutable state adapter avoids
// unrelated state mutation/cleanup. All fixture directories are deliberately retained.
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { BridgeStaleIncarnationError, StoreBridgeSide } from "../src/mesh/bridge.js";
import type { MeshStateEntry, MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { hostLeasePath, type FabricHostLease } from "../src/topology/host-leases.js";

const key = (kind: string, id: string) => `topology/${kind}/${createHash("sha256").update(id).digest("hex")}`;
const fixture = (handoff = false) => {
  const now = Date.now(), root = fs.mkdtempSync(path.join(os.tmpdir(), "host-leases-comms-"));
  fs.mkdirSync(path.join(root, "host-leases"));
  const native = "session:native-00000000", held = "session:held-00000000", relay = "session:relay-00000000";
  const put = (lease: FabricHostLease) => fs.writeFileSync(hostLeasePath(root, lease.id), JSON.stringify({ format: 1, ...lease }));
  const identity = (id: string) => ({ id, name: id, kind: "main" as const, sessionId: id.slice(8) });
  const rows: MeshStateEntry[] = [native, held, relay].map((id, index) => ({
    key: key("hosts", id), version: 1, updatedAt: now, updatedBy: identity(id),
    value: { format: 1, id, rootId: id, identity: identity(id), startedAt: now - 1_000,
      incarnationToken: `token:${id}`, updatedAt: now, expiresAt: now + 10_000,
      ...(index === 0 ? {} : { remoteHost: index === 1 ? "far" : "other" }) },
  }));
  for (const id of [native, held, relay]) put({ id, rootId: id, identityId: id, startedAt: now - 1_000,
    incarnationToken: `token:${id}`, updatedAt: now, expiresAt: now + 60_000 });
  for (let i = 3; i < 1229; i++) put({ id: `old:${i}`, rootId: `old:${i}`, identityId: `old:${i}`, updatedAt: 1, expiresAt: 2 });
  if (handoff) rows.push({ key: key("handoffs", held), version: 1, updatedAt: now, updatedBy: identity(native),
    value: { format: 1, id: held, sessionId: held.slice(8), link: "far", operator: "test", hub: "test", handedOffAt: now } });
  const view = { get: (key: string) => rows.find(row => row.key === key),
    listAll: (prefix: string) => rows.filter(row => row.key.startsWith(prefix)), version: () => 1 };
  const store = { root, stateBackend: "file", get: view.get, listAll: view.listAll, listAllShared: view.listAll,
    stateToken: () => view, stateRevision: () => undefined, prepareLock: async () => {},
  } as unknown as MeshStore;
  return { root, now, native, held, relay, identity, rows, put, store, side: new StoreBridgeSide(store, "far", () => now) };
};
afterEach(() => vi.restoreAllMocks());

const denyLeaseDiscovery = (root: string) => {
  const original = fs.readdirSync.bind(fs);
  return vi.spyOn(fs, "readdirSync").mockImplementation(((file: fs.PathLike, ...rest: unknown[]) => {
    if (String(file) === path.join(root, "host-leases")) throw new Error("synchronous global lease discovery");
    return (original as (...args: unknown[]) => unknown)(file, ...rest);
  }) as typeof fs.readdirSync);
};

describe("lease discovery at comms entrypoints", () => {
  it("keeps relay/held reads and a no-handoff subscription off the historical lease directory", async () => {
    const f = fixture();
    denyLeaseDiscovery(f.root);
    expect(f.side.relayOrigin(f.relay)).toBe("other");
    expect(f.side.relayStaleOrigin(f.relay)).toBeUndefined();
    expect((await f.side.relayPresence()).hosts.map(host => host.record.id)).toEqual([f.relay]);
    expect(f.side.holds(f.held)).toBe(true);
    expect((await f.side.presence()).hosts.map(host => host.record.id)).toEqual([f.native]);
    expect((await f.side.owned()).hosts.map(host => host.record.id)).toEqual([f.held]);
    const failures: Error[] = [];
    const stop = f.side.subscribeChanges(() => {}, error => failures.push(error));
    stop();
    expect(failures).toEqual([]);
  });

  it("point-reads current known-host liveness through the public participant listing", () => {
    const f = fixture();
    const id = f.native;
    f.rows.push({ key: key("participants", id), version: 1, updatedAt: f.now, updatedBy: f.identity(id), value: {
      format: 1, id, rootId: id, ownerHostId: id, ownerIdentityId: id, ownerIncarnation: `token:${id}`,
      kind: "root", name: "native", label: "native", status: "idle", sessionId: id.slice(8),
      runner: "pi", transport: "host", capabilities: [], controlProtocol: "v1", startedAt: f.now - 1_000, updatedAt: f.now,
    } });
    const host = f.rows.find(row => row.key === key("hosts", id))!.value as { expiresAt: number };
    host.expiresAt = f.now - 1; // Only its file lease supplies live authority.
    const directory = new ParticipantDirectory(f.store, { enabled: true, hostId: "own-host", rootId: "own-root",
      identity: { id: "own-root", name: "own", kind: "main" } });
    denyLeaseDiscovery(f.root);
    expect(directory.list({ scope: "project" }, f.now).some(peer => peer.id === id)).toBe(true);
    // In-place expiry is still observed; no directory mtime/cache TTL is authority here.
    f.put({ id, rootId: id, identityId: id, incarnationToken: `token:${id}`, updatedAt: f.now, expiresAt: f.now - 1 });
    expect(directory.list({ scope: "project" }, f.now).some(peer => peer.id === id)).toBe(false);
  });

  it.each(["rootId", "identityId"] as const)("retains full handoff authority for an arbitrary file id answering by %s", async field => {
    const f = fixture(true);
    expect(f.side.holds(f.held)).toBe(true); // Grant is initially in force.
    const alias: FabricHostLease = { id: "arbitrary-file-with-no-state-row", rootId: "unrelated-root", identityId: "unrelated-identity",
      [field]: f.held, updatedAt: f.now, expiresAt: f.now + 60_000,
      writer: { pid: process.pid, host: os.hostname(), releaseSha: "test", lockProtocol: 3, stateBackend: "file", startedAt: f.now - 1_000 } };
    f.put(alias);
    expect(f.side.holds(f.held)).toBe(false); // Lease-only native revokes the handoff.
    expect((await f.side.presence()).reserved).toContain(f.held);
    f.put({ ...alias, expiresAt: f.now - 1 });
    expect(f.side.holds(f.held)).toBe(true); // Clock/expiry is not hidden behind a discovery cache.
  });

  it("still fences superseded incoming tokens using arbitrary-id lease-only root authority", async () => {
    const f = fixture();
    const id = "session:incoming-00000000";
    f.put({ id: "lease-only-root-alias", rootId: id, identityId: id, startedAt: f.now - 500,
      incarnationToken: "new-token", bridgeSupersededTokens: ["old-token"], updatedAt: f.now, expiresAt: f.now + 60_000 });
    await expect(f.side.mirror({ hosts: [{ record: { format: 1, id, rootId: id, identity: f.identity(id),
      startedAt: f.now - 1_000, incarnationToken: "old-token", updatedAt: f.now, expiresAt: f.now + 60_000 },
      expiresAt: f.now + 60_000, observedAt: f.now }], participants: [] })).rejects.toBeInstanceOf(BridgeStaleIncarnationError);
  });
});
