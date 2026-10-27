import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { BRIDGE_LEASE_MS, StoreBridgeSide, type BridgePresence } from "../src/mesh/bridge.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { hostEntryLiveness, hostLeasePath, readHostLeaseCurrent, readHostLeases } from "../src/topology/host-leases.js";
import type { FabricHostRecord, FabricParticipantRecord } from "../src/topology/types.js";

// Keep reason: measured mirror-renewal incident and data safety. Exercise real mirror commits,
// filesystem lease replacements and custody; also check persisted authority and expiry.
const NOW = Date.UTC(2100, 0, 1);
const roots: string[] = [];
const stores: MeshStore[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const store of stores.splice(0)) store.closeState();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const key = (prefix: string, id: string) => prefix + createHash("sha256").update(id).digest("hex");
const hostKey = (id: string) => key("topology/hosts/", id);
const participantKey = (id: string) => key("topology/participants/", id);

const fixture = (stateBackend: "file" | "sqlite", count = 114) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "mirror-renew-skip-"));
  roots.push(root);
  const store = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100, { stateBackend });
  stores.push(store);
  expect(store.stateBackend).toBe(stateBackend);
  let now = NOW;
  const side = new StoreBridgeSide(store, "peer", () => now);
  const hosts: FabricHostRecord[] = Array.from({ length: count }, (_, index) => {
    const id = `session:renew-${index.toString().padStart(8, "0")}`;
    const identity: MeshIdentity = { id, name: `main-${index}`, kind: "main", sessionId: `renew-${index}` };
    return { format: 1, id, rootId: id, identity, incarnationToken: `token-${index}`,
      startedAt: NOW - 1_000, updatedAt: NOW, expiresAt: NOW + 60_000 };
  });
  const participants: FabricParticipantRecord[] = hosts.map(host => ({
    format: 1, id: host.id, kind: "root", rootId: host.rootId, ownerHostId: host.id,
    ownerIdentityId: host.identity.id, name: host.identity.name, label: host.identity.name,
    status: "idle", runner: "pi", transport: "host", capabilities: ["steer", "followUp"],
    sessionId: host.identity.sessionId!, startedAt: host.startedAt, updatedAt: NOW, controlProtocol: "v1",
  }));
  const presence = (): BridgePresence => ({ hosts: hosts.map(record => ({ record: { ...record, updatedAt: now },
    observedAt: now, expiresAt: now + 60_000 })), participants, reserved: [] });
  const files = new Set(hosts.map(host => hostLeasePath(store.root, host.id)));
  const renames = vi.spyOn(fs, "renameSync");
  const custody = vi.spyOn(store, "leaseCustody");
  const counts = () => ({
    renames: renames.mock.calls.filter(([, target]) => files.has(String(target))).length,
    locks: custody.mock.calls.filter(([file]) => files.has(file)).length,
  });
  const reset = () => { renames.mockClear(); custody.mockClear(); };
  return { store, side, hosts, participants, presence, counts, reset, setTime: (at: number) => { now = at; } };
};

describe.each(["file", "sqlite"] as const)("mirror lease renewal budget (%s)", backend => {
  it("114 unchanged leases across three passes take at most one renewal wave per half-lease period", async () => {
    const f = fixture(backend);
    const waves: number[] = [];
    const locks: number[] = [];
    for (const offset of [0, 1_000, BRIDGE_LEASE_MS / 2 - 1]) {
      f.setTime(NOW + offset);
      f.reset();
      await f.side.mirror(f.presence());
      waves.push(f.counts().renames);
      locks.push(f.counts().locks);
    }
    expect(waves).toEqual([114, 0, 0]);
    // SQLite's existing lease-only probe takes no gate for skipped effects. The file
    // backend deliberately retains its existing directory custody in this change.
    if (backend === "sqlite") expect(locks.slice(1)).toEqual([0, 0]);
    for (const host of f.hosts) {
      expect(readHostLeaseCurrent(f.store.root, host.id)).toMatchObject({
        incarnationToken: host.incarnationToken, expiresAt: NOW + BRIDGE_LEASE_MS,
        mirror: { observedAt: NOW },
      });
    }
    f.setTime(NOW + BRIDGE_LEASE_MS / 2);
    f.reset();
    await f.side.mirror(f.presence());
    expect(f.counts().renames).toBe(114);
    expect(readHostLeaseCurrent(f.store.root, f.hosts[0]!.id)?.expiresAt).toBe(NOW + BRIDGE_LEASE_MS * 1.5);
    f.setTime(NOW + BRIDGE_LEASE_MS / 2 + 1);
    f.reset();
    await f.side.mirror(f.presence());
    expect(f.counts().renames).toBe(0);
    if (backend === "sqlite") expect(f.counts().locks).toBe(0);
  }, 60_000);

  it.each(["host", "participant"] as const)("renews a changed %s immediately without rewriting the 113 unchanged leases", async kind => {
    const f = fixture(backend);
    await f.side.mirror(f.presence());
    const changed = f.hosts[0]!;
    f.setTime(NOW + 1_000);
    if (kind === "host") f.hosts[0] = { ...changed, identity: { ...changed.identity, name: "renamed" } };
    else f.participants[0] = { ...f.participants[0]!, status: "running", updatedAt: NOW + 1_000 };
    f.reset();
    await f.side.mirror(f.presence());
    expect(f.counts().renames).toBe(1);
    expect(readHostLeaseCurrent(f.store.root, changed.id)).toMatchObject({
      expiresAt: NOW + 1_000 + BRIDGE_LEASE_MS, mirror: { observedAt: NOW + 1_000 },
    });
    expect(readHostLeaseCurrent(f.store.root, f.hosts[1]!.id)?.mirror?.observedAt).toBe(NOW);
    expect(f.store.get(kind === "host" ? hostKey(changed.id) : participantKey(changed.id), { fresh: true })?.value)
      .toMatchObject(kind === "host" ? { identity: { name: "renamed" } } : { status: "running" });
  }, 60_000);

  it("renews near expiry, repairs a missing lease and removes an omitted lease normally", async () => {
    const f = fixture(backend, 2);
    await f.side.mirror(f.presence());
    f.setTime(NOW + BRIDGE_LEASE_MS / 2 + 1);
    f.reset();
    await f.side.mirror(f.presence());
    expect(f.counts().renames).toBe(2);
    const renewedAt = NOW + BRIDGE_LEASE_MS / 2 + 1;
    expect(readHostLeaseCurrent(f.store.root, f.hosts[0]!.id)?.expiresAt).toBe(renewedAt + BRIDGE_LEASE_MS);
    fs.unlinkSync(hostLeasePath(f.store.root, f.hosts[0]!.id));
    f.reset();
    await f.side.mirror(f.presence());
    expect(f.counts().renames).toBe(1);
    expect(readHostLeaseCurrent(f.store.root, f.hosts[0]!.id)).toBeDefined();
    const gone = f.hosts.pop()!;
    f.participants.pop();
    await f.side.mirror(f.presence());
    expect(readHostLeaseCurrent(f.store.root, gone.id)).toBeUndefined();
    expect(f.store.get(hostKey(gone.id), { fresh: true })).toBeUndefined();
    expect(f.store.get(participantKey(gone.id), { fresh: true })).toBeUndefined();
  });

  it("does not extend a silent peer's persisted lease while skipping healthy renewals", async () => {
    const f = fixture(backend, 1);
    await f.side.mirror(f.presence());
    f.setTime(NOW + 1_000);
    await f.side.mirror(f.presence());
    const lease = readHostLeaseCurrent(f.store.root, f.hosts[0]!.id)!;
    expect(lease.expiresAt).toBe(NOW + BRIDGE_LEASE_MS);
    f.setTime(NOW + BRIDGE_LEASE_MS);
    const entry = f.store.get(hostKey(f.hosts[0]!.id), { fresh: true })!;
    expect(hostEntryLiveness(entry, readHostLeases(f.store.root)).expiresAt).toBeLessThanOrEqual(NOW + BRIDGE_LEASE_MS);
    expect((await f.side.owned()).hosts).toEqual([]);
    expect(readHostLeaseCurrent(f.store.root, f.hosts[0]!.id)).toEqual(lease);
  });
});
