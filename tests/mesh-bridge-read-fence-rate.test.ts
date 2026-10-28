import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { BridgeChange } from "../src/mesh/change-notifier.js";
import type { BridgePresence } from "../src/mesh/bridge.js";

// Only notification delivery is controlled. State, transactions, leases, admission and
// snapshot contents cross the real MeshStore boundary. No elapsed-time performance gate.
const notifications = vi.hoisted(() => new Map<string, (kind: BridgeChange) => void>());
vi.mock("../src/mesh/change-notifier.js", () => ({
  watchBridgeStore: (root: string, changed: (kind: BridgeChange) => void) => {
    notifications.set(root, changed);
    return () => { notifications.delete(root); };
  },
}));
const { MeshStore } = await import("../src/mesh/store.js");
const { StoreBridgeSide } = await import("../src/mesh/bridge.js");
const { writeHostLease } = await import("../src/topology/host-leases.js");
const { writeParticipantFile } = await import("../src/topology/participant-files.js");

const NOW = 1_800_000_000_000;
const key = (family: string, id: string) => `topology/${family}/${createHash("sha256").update(id).digest("hex")}`;
const turn = () => new Promise<void>(resolve => setImmediate(resolve));
const roots: string[] = [];
const cleanups: Array<() => void> = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const close of cleanups.splice(0).reverse()) close();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  notifications.clear();
});
const presence = (name: string): BridgePresence => {
  const id = `session:${name}-00000000`;
  const identity = { id, name, kind: "main" as const, sessionId: `${name}-00000000` };
  return { hosts: [{ record: { format: 1, id, rootId: id, identity, startedAt: NOW - 1_000,
    incarnationToken: `${name}-token`, updatedAt: NOW, expiresAt: NOW + 120_000 }, expiresAt: NOW + 120_000 }],
  participants: [{ format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id,
    name, label: name, status: "idle", runner: "pi", transport: "host", capabilities: [],
    sessionId: identity.sessionId, startedAt: NOW - 1_000, updatedAt: NOW, controlProtocol: "v1" }], reserved: [] };
};
const fixture = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-read-fence-rate-"));
  roots.push(root);
  const store = new MeshStore(root, 64 * 1024, 100, { stateBackend: "sqlite", maxStateBytes: 16 * 1024 * 1024 });
  cleanups.push(() => store.closeState());
  const native = presence("native"), remote = presence("remote");
  const host = native.hosts[0]!.record, participant = native.participants[0]!, identity = host.identity;
  await store.writeBatch({ identity, ops: [
    ...Array.from({ length: 1_000 }, (_, index) => ({ kind: "put" as const,
      key: `unrelated/agents/${index}`, value: { index, status: "idle", metadata: "x".repeat(128) } })),
    { kind: "put", key: key("hosts", host.id), value: host },
    { kind: "put", key: key("participants", participant.id), value: participant },
  ] });
  let now = NOW;
  const side = new StoreBridgeSide(store, "far", () => now);
  await side.mirror(remote);
  const writers = vi.spyOn(store, "withStateWriteFence");
  const reads = vi.spyOn(store, "withStateReadSnapshot");
  const changes: BridgeChange[] = [], failures: Error[] = [];
  cleanups.push(side.subscribeChanges(kind => changes.push(kind), error => failures.push(error)));
  const fire = async (count = 1) => {
    const before = [writers.mock.calls.length, reads.mock.calls.length];
    for (let i = 0; i < count; i += 1) {
      notifications.get(root)!("presence");
      await Promise.resolve(); // distinct native callbacks drain microtasks within a burst
    }
    await turn();
    expect(writers.mock.calls.length - before[0]!).toBeLessThanOrEqual(1);
    expect(reads.mock.calls.length - before[1]!).toBeLessThanOrEqual(1);
    expect(failures).toEqual([]);
  };
  const publish = (id: string) => side.publish({ topic: "fleet.work.fence-rate", kind: "ask",
    from: remote.hosts[0]!.record.identity, to: host.id, text: id,
    data: { bridge: { from: "far", id } } }, [remote.hosts[0]!.record.id]);
  return { root, store, side, native, remote, host, participant, identity, writers, reads, changes, failures, fire, publish,
    advance: (ms: number) => { now += ms; } };
};

// Keep: recorded bridge observation amplification plus native-takeover trust boundary.
describe("bridge observation fence rate on real SQLite state", () => {
  it("uses >5x fewer snapshot calls and writer fences over 60 steady file/duplicate notification ticks", async () => {
    const f = await fixture();
    expect(f.store.listAll("unrelated/agents/")).toHaveLength(1_000);
    for (let tick = 0; tick < 60; tick += 1) {
      f.advance(1_000);
      if (tick === 10) writeHostLease(f.root, { id: f.host.id, rootId: f.host.id, identityId: f.identity.id,
        startedAt: f.host.startedAt, incarnationToken: f.host.incarnationToken!, updatedAt: NOW + 1, expiresAt: NOW + 150_000 });
      if (tick === 30) writeParticipantFile(f.root, { key: key("participants", f.participant.id), version: 1,
        updatedAt: NOW + 1, updatedBy: f.identity, value: { ...f.participant, status: "running" } });
      await f.fire(20);
    }
    // Modelled one-minute workload: 60 drain ticks, 20 hints each; old code fenced
    // at least once per tick. These are actual API counts, NOT live fleet savings.
    expect(f.writers).toHaveBeenCalledTimes(1); // initial settlement only
    expect(f.reads).toHaveBeenCalledTimes(3); // initial + each real file change
    expect(60 / f.writers.mock.calls.length).toBeGreaterThan(5);
    expect(60 / f.reads.mock.calls.length).toBeGreaterThan(5);
    expect(f.changes).toEqual(["lease", "presence"]);
    expect((await f.side.presence()).participants[0]).toMatchObject({ status: "running" });
  });

  it("reuses snapshots across proven unrelated commits, but conservatively settles each changed WAL stamp", async () => {
    const f = await fixture();
    for (let tick = 0; tick < 12; tick += 1) {
      await f.store.put({ key: `unrelated/agents/${tick}`, identity: f.identity, value: { tick } });
      await f.fire(20);
    }
    expect(f.writers).toHaveBeenCalledTimes(13);
    expect(f.reads).toHaveBeenCalledTimes(1);
    expect(f.changes).toEqual([]);
  });

  it("takes a full consistent snapshot on unknown prefix evidence and sees a relevant change on the next observation", async () => {
    const f = await fixture();
    const proof = vi.spyOn(f.store, "stateUnchangedSince").mockReturnValueOnce(false);
    await f.fire();
    expect(f.reads).toHaveBeenCalledTimes(2);
    expect(f.writers).toHaveBeenCalledTimes(1);
    proof.mockRestore();
    await f.store.put({ key: key("participants", f.participant.id), identity: f.identity,
      value: { ...f.participant, status: "running" } });
    await f.fire();
    expect(f.reads).toHaveBeenCalledTimes(3);
    expect(f.changes).toEqual(["presence"]);
    expect((await f.side.presence()).participants[0]).toMatchObject({ status: "running" });
  });

  it("does not reuse when raw WAL stamp evidence is unreadable", async () => {
    const f = await fixture();
    const stat = fs.statSync;
    const unavailable = vi.spyOn(fs, "statSync").mockImplementation((...args) => {
      if (String(args[0]) === path.join(f.root, "state.db-wal")) {
        throw Object.assign(new Error("unreadable raw evidence"), { code: "EACCES" });
      }
      return Reflect.apply(stat, fs, args);
    });
    await f.fire();
    unavailable.mockRestore();
    expect(f.writers).toHaveBeenCalledTimes(2);
    expect(f.reads).toHaveBeenCalledTimes(2);
    expect(f.changes).toEqual([]);
  });

  it("rebuilds on an already committed raw mutation whose change-feed coverage is unknown", async () => {
    const f = await fixture();
    const { DatabaseSync } = await import("node:sqlite");
    const raw = new DatabaseSync(path.join(f.root, "state.db"));
    try {
      raw.exec("BEGIN IMMEDIATE");
      raw.prepare("UPDATE kv SET value = ? WHERE key = ?").run(
        JSON.stringify({ ...f.participant, status: "raw-change" }), key("participants", f.participant.id));
      raw.exec("UPDATE meta SET value = CAST(value AS INTEGER) + 1 WHERE name = 'commit_no'");
      raw.exec("COMMIT");
      await f.fire();
      expect(f.reads).toHaveBeenCalledTimes(2);
      expect(f.changes).toEqual(["presence"]);
      expect((await f.side.presence()).participants[0]).toMatchObject({ status: "raw-change" });
    } finally { raw.close(); }
  });

  it("expires cached observations at host and handoff native-lease clock boundaries", async () => {
    const f = await fixture();
    const moved = "session:moved-00000000";
    await f.store.put({ key: key("handoffs", moved), identity: f.identity, value: { format: 1,
      id: moved, sessionId: "moved-00000000", name: "moved", link: "far", handedOffAt: NOW,
      operator: "operator", hub: "hub" } });
    writeHostLease(f.root, { id: moved, rootId: moved, identityId: moved, startedAt: NOW - 1_000,
      updatedAt: NOW, expiresAt: NOW + 1_000, session: { id: "moved-00000000", startedAt: NOW - 1_000, updatedAt: NOW, expiresAt: NOW + 1_000 } });
    await f.fire();
    expect((await f.side.presence()).reserved).toContain(moved);
    const reads = f.reads.mock.calls.length, writers = f.writers.mock.calls.length;
    f.advance(1_001);
    await f.fire();
    expect(f.reads).toHaveBeenCalledTimes(reads + 1);
    expect(f.writers).toHaveBeenCalledTimes(writers);
    expect((await f.side.presence()).reserved).not.toContain(moved);
    f.advance(120_000);
    await f.fire();
    expect((await f.side.presence()).hosts).toEqual([]);
  });

  it("does not swallow a pre-COMMIT hint after cached good admission, even with no later notification", async () => {
    const f = await fixture();
    await f.publish("before-takeover"); // real R20 admission succeeds before the native takes over
    await f.fire(); // unchanged cached observation
    const { DatabaseSync } = await import("node:sqlite");
    const raw = new DatabaseSync(path.join(f.root, "state.db"));
    raw.exec("PRAGMA busy_timeout = 0");
    const remote = f.remote.hosts[0]!.record;
    try {
      raw.exec("BEGIN IMMEDIATE");
      raw.prepare("UPDATE kv SET value = ? WHERE key = ?").run(JSON.stringify(remote), key("hosts", remote.id));
      raw.exec("UPDATE meta SET value = CAST(value AS INTEGER) + 1 WHERE name = 'commit_no'");
      // Emulate WAL spill's early fs hint. This test's watcher emits NOTHING at COMMIT.
      const stamp = new Date();
      fs.utimesSync(path.join(f.root, "state.db"), stamp, stamp);
      const before = f.writers.mock.calls.length;
      await f.fire();
      expect(f.writers).toHaveBeenCalledTimes(before + 1);
      expect(f.changes).toEqual([]);
      raw.exec("COMMIT");
      await new Promise(resolve => setTimeout(resolve, 150));
      await turn();
      expect(f.failures).toEqual([]);
      expect(f.changes).toEqual(["presence"]);
      await expect(f.publish("after-takeover")).rejects.toThrow(/no longer bound/);
      expect(f.store.read({ limit: 10 }).map(event => event.text)).toEqual(["before-takeover"]);
    } finally {
      try { raw.exec("ROLLBACK"); } catch { /* committed */ }
      raw.close();
    }
  });
});
