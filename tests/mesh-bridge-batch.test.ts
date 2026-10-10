import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { StoreBridgeSide, type BridgePresence } from "../src/mesh/bridge.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { COMMIT_OUTBOX_PREFIX } from "../src/mesh/commit-outbox.js";
import { readHostLeases, removeHostLease, STATE_LEASE_RENEW_MS, writeHostLease } from "../src/topology/host-leases.js";

const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const scratch = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-batch-"));
  roots.push(root);
  return root;
};
const key = (prefix: string, id: string) => prefix + createHash("sha256").update(id).digest("hex");
const hostKey = (id: string) => key("topology/hosts/", id);
const participantKey = (id: string) => key("topology/participants/", id);
const fleet = (n: number, now: number): BridgePresence => {
  const hosts: BridgePresence["hosts"] = [];
  const participants: BridgePresence["participants"] = [];
  for (let i = 0; i < n; i++) {
    const id = `session:batch-${i}`;
    const identity: MeshIdentity = { id, name: "main", kind: "main", sessionId: `batch-${i}` };
    hosts.push({ record: { format: 1, id, rootId: id, identity, startedAt: now, updatedAt: now, expiresAt: now + 15_000 }, expiresAt: now + 15_000 });
    participants.push({ format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id,
      name: `batch-${i}`, label: `BATCH-${i}`, status: "idle", runner: "pi", transport: "host",
      capabilities: ["steer"], sessionId: `batch-${i}`, startedAt: now, updatedAt: now, controlProtocol: "v1" });
  }
  return { hosts, participants, reserved: [] };
};

// The former per-record path for this admitted fleet: hosts, participants, leases, withdrawals.
// Keeping the reference on put/delete detects changes in revisions and owner attribution too.
const perRecord = async (store: MeshStore, presence: BridgePresence, now: number) => {
  const wanted = new Map<string, { value: Record<string, unknown>; identity: MeshIdentity }>();
  for (const { record, expiresAt } of presence.hosts) {
    const until = Math.min(expiresAt, now + 15_000);
    wanted.set(hostKey(record.id), { value: { ...record, updatedAt: now, expiresAt: until, remoteHost: "forge" }, identity: record.identity });
  }
  for (const p of presence.participants) {
    const owner = presence.hosts.find(h => h.record.id === p.ownerHostId)!.record;
    wanted.set(participantKey(p.id), { value: { ...p, remoteHost: "forge" }, identity: owner.identity });
  }
  const old = [...store.listAll("topology/hosts/"), ...store.listAll("topology/participants/")];
  const settled = (v: Record<string, unknown>) => JSON.stringify({ ...v, updatedAt: undefined, expiresAt: undefined });
  for (const [k, { value, identity }] of wanted) {
    const existing = store.get(k);
    const before = existing?.value as Record<string, unknown> | undefined;
    if (before && settled(before) === settled(value) && (k.startsWith("topology/hosts/")
      ? now - Number(before.updatedAt) < STATE_LEASE_RENEW_MS : before.updatedAt === value.updatedAt)) continue;
    await store.put({ key: k, value, identity });
  }
  for (const { record, expiresAt } of presence.hosts) {
    // Mirror leases carry the origin's incarnation (L2b owner review: owner-matched unlease).
    writeHostLease(store.root, { id: record.id, rootId: record.rootId, identityId: record.identity.id, startedAt: record.startedAt,
      updatedAt: Math.min(record.updatedAt, now), expiresAt: Math.min(expiresAt, now + 15_000) });
  }
  for (const entry of old) {
    if (wanted.has(entry.key)) continue;
    if (entry.key.startsWith("topology/hosts/")) removeHostLease(store.root, (entry.value as { id: string }).id);
    await store.delete({ key: entry.key, ifVersion: entry.version });
  }
};

const io = (store: MeshStore) => {
  const canonical = path.join(store.root, "state.json");
  const reads = vi.spyOn(fs, "readFileSync");
  const writes = vi.spyOn(fs, "renameSync");
  const locks = vi.spyOn(fs, "mkdirSync");
  return {
    counts: () => ({ reads: reads.mock.calls.filter(([p]) => String(p) === canonical).length,
      commits: writes.mock.calls.filter(([, p]) => String(p) === canonical).length,
      locks: locks.mock.calls.filter(([p]) => String(p) === path.join(store.root, ".lock")).length }),
    reset: () => { reads.mockClear(); writes.mockClear(); locks.mockClear(); },
  };
};

describe("one presence transaction (smarty-dev#3752)", () => {
  it("N=250 matches per-record state and leases with one read/lock/commit, including mixed updates/removals", async () => {
    let now = 1_800_000_000_000;
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const hub = new MeshStore(scratch(), 64 * 1024, 100);
    const reference = new MeshStore(scratch(), 64 * 1024, 100);
    const side = new StoreBridgeSide(hub, "forge");
    const presence = fleet(250, now);
    // Ensure even the first pass performs a canonical read, not an ENOENT probe.
    for (const store of [hub, reference]) fs.writeFileSync(path.join(store.root, "state.json"), JSON.stringify({ format: 1, entries: {} }));
    const probe = io(hub);
    // The hub also holds this link's commit-outbox rows (smarty-dev#6477 L2b, R11): its lease effects
    // are recorded in the same commit and retired by a later one. Compare the presence records;
    // those rows also draw from the store's version counter, so versions are compared as an order.
    const presenceRecords = (store: MeshStore) => {
      const entries = store.listAll().filter(entry => !entry.key.startsWith(COMMIT_OUTBOX_PREFIX));
      const rank = new Map([...new Set(entries.map(entry => entry.version))].sort((a, b) => a - b).map((version, index) => [version, index]));
      return entries.map(entry => ({ ...entry, version: rank.get(entry.version) }));
    };
    const compare = () => {
      expect(presenceRecords(hub)).toEqual(presenceRecords(reference));
      expect([...readHostLeases(hub.root)]).toEqual([...readHostLeases(reference.root)]);
    };
    await side.mirror(presence);
    expect(probe.counts()).toEqual({ reads: 1, commits: 1, locks: 1 });
    await perRecord(reference, presence, now);
    compare();
    now += 1;
    presence.participants = presence.participants.map(p => ({ ...p, status: "running", updatedAt: now }));
    probe.reset();
    await side.mirror(presence);
    expect(probe.counts()).toEqual({ reads: 1, commits: 1, locks: 1 });
    await perRecord(reference, presence, now);
    compare();
    now += 1;
    const mixed = fleet(275, now);
    mixed.hosts = mixed.hosts.slice(25);
    mixed.participants = mixed.participants.slice(25);
    probe.reset();
    await side.mirror(mixed);
    expect(probe.counts()).toEqual({ reads: 1, commits: 1, locks: 1 });
    await perRecord(reference, mixed, now);
    compare();
    probe.reset();
    await side.withdraw();
    expect(probe.counts()).toEqual({ reads: 1, commits: 1, locks: 1 });
    expect(presenceRecords(hub)).toEqual([]);
    expect(readHostLeases(hub.root).size).toBe(0);
    // Tombstone recreation is one commit too, not a failed put + retry per key.
    const restarted = new StoreBridgeSide(hub, "forge");
    probe.reset();
    await restarted.mirror(mixed);
    expect(probe.counts()).toEqual({ reads: 1, commits: 1, locks: 1 });
    expect(hub.listAll("topology/participants/")).toHaveLength(250);
  }, 30_000);

  it("renews 250 unchanged leases without state writes, but repairs a changed identity", async () => {
    let now = 1_800_000_000_000;
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const store = new MeshStore(scratch(), 64 * 1024, 100);
    const side = new StoreBridgeSide(store, "forge");
    const presence = fleet(250, now);
    await side.mirror(presence);
    const before = store.listAll();
    now += 5_000;
    // The origins renewed; only their lease fields move.
    presence.hosts = presence.hosts.map(({ record }) => ({ record: { ...record, updatedAt: now, expiresAt: now + 15_000 }, expiresAt: now + 15_000 }));
    const probe = io(store);
    await side.mirror(presence);
    expect(probe.counts()).toEqual({ reads: 1, commits: 0, locks: 1 });
    expect(store.listAll()).toEqual(before);
    expect([...readHostLeases(store.root).values()].every(l => l.updatedAt === now && l.expiresAt === now + 15_000)).toBe(true);
    const p = presence.participants[0]!;
    const record = store.get(participantKey(p.id))!;
    await store.put({ key: record.key, value: record.value, identity: { id: "session:wrong", name: "main", kind: "main" } });
    probe.reset();
    await side.mirror(presence);
    expect(probe.counts()).toEqual({ reads: 1, commits: 1, locks: 1 });
    expect(store.get(record.key)!.updatedBy.id).toBe(p.ownerIdentityId);
  });

  it("rechecks reserved root ids under the commit lock after a native takeover", async () => {
    const now = Date.now();
    const hub = new MeshStore(scratch(), 64 * 1024, 100);
    const presence = fleet(1, now);
    let release!: () => void;
    let entered!: () => void;
    const waiting = new Promise<void>(r => { entered = r; });
    const held = new Promise<void>(r => { release = r; });
    const batch = hub.writeBatch.bind(hub);
    vi.spyOn(hub, "writeBatch").mockImplementationOnce(async input => { entered(); await held; return batch(input); });
    const side = new StoreBridgeSide(hub, "forge");
    const pending = side.mirror(presence);
    await waiting;
    const native: MeshIdentity = { id: "session:native", name: "main", kind: "main" };
    // Different host key, but its root reserves the peer's proposed address (F1/F2).
    const value = { ...presence.hosts[0]!.record, id: native.id, identity: native };
    await hub.put({ key: hostKey(native.id), identity: native, value });
    release();
    await pending;
    expect(hub.get(hostKey(presence.hosts[0]!.record.id))).toBeUndefined();
    expect(hub.get(participantKey(presence.participants[0]!.id))).toBeUndefined();
    expect(readHostLeases(hub.root).size).toBe(0);
  });
});
