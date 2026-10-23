import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { BridgeOwnershipError, StoreBridgeSide, type BridgePublish } from "../src/mesh/bridge.js";
import { CommitOutbox } from "../src/mesh/commit-outbox.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { hostLiveness, hostLeasePath, readHostLease, readHostLeases, writeHostLease } from "../src/topology/host-leases.js";
import { hostLeaseAdditionsGatePath, withHostLeaseFileCustody } from "../src/topology/host-lease-lock.js";
import { readParticipantFile, writeParticipantFileIf } from "../src/topology/participant-files.js";
import type { FabricHostRecord, FabricParticipantRecord } from "../src/topology/types.js";

// smarty-dev#6477 L2b (R11): mirror state batches durably record host-lease effects, apply them
// after the topology batches, and capture participant files before each transaction.
const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const key = (prefix: string, id: string) => prefix + createHash("sha256").update(id).digest("hex");
const hostKey = (id: string) => key("topology/hosts/", id);
const participantKey = (id: string) => key("topology/participants/", id);
const opened: MeshStore[] = [];
const store = (stateBackend?: "sqlite") => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-bridge-outbox-"));
  roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100, stateBackend ? { stateBackend } : {});
  opened.push(mesh);
  return mesh;
};
afterEach(() => { for (const mesh of opened.splice(0)) mesh.closeState(); });
// A rename that installs a lease file (not the owner-matched removal's rename aside).
const leaseWrite = (to: string) => to.includes(`${path.sep}host-leases${path.sep}`) && to.endsWith(".json");
const bridgeIdentity: MeshIdentity = { id: "bridge:ryzen2", name: "ryzen2", kind: "main" };

// A native of the peer, as the far side reports it.
const remote = (name: string, now: number) => {
  const identity: MeshIdentity = { id: `session:${name}-00000000`, name: "main", kind: "main", sessionId: name };
  const host = { format: 1, id: identity.id, rootId: identity.id, identity, startedAt: now, updatedAt: now, expiresAt: now + 60_000 } as unknown as FabricHostRecord;
  const participant = {
    format: 1, id: identity.id, kind: "root", rootId: identity.id, ownerHostId: identity.id, ownerIdentityId: identity.id,
    name, label: name.toUpperCase(), status: "idle", runner: "pi", transport: "host", capabilities: ["steer", "followUp"],
    sessionId: name, startedAt: now, updatedAt: now, controlProtocol: "v1",
  } as unknown as FabricParticipantRecord;
  return { identity, host, participant, presence: { hosts: [{ record: host, expiresAt: now + 60_000 }], participants: [participant] } };
};

describe("bridge mirror commit outbox", () => {
  it("records lease rows in a changing commit, writes the lease after it and never uses exclusive()", async () => {
    const hub = store();
    const side = new StoreBridgeSide(hub, "ryzen2");
    const exclusive = vi.spyOn(hub, "exclusive");
    const peer = remote("alpha", Date.now());
    const batches: Array<{ keys: string[] }> = [];
    const writeBatch = hub.writeBatch.bind(hub);
    vi.spyOn(hub, "writeBatch").mockImplementation(async (input) => {
      const prepare = input.prepare;
      if (!prepare) return writeBatch(input);
      return writeBatch({ ...input, prepare: (view) => {
        const ops = prepare(view);
        batches.push({ keys: ops.map((op) => op.key) });
        return ops;
      } });
    });
    await side.mirror(peer.presence);
    expect(exclusive).not.toHaveBeenCalled();
    expect(hub.get(hostKey(peer.identity.id), { fresh: true })).toBeDefined();
    expect(readHostLease(hub.root, peer.identity.id)?.identityId).toBe(peer.identity.id);
    const rows = new CommitOutbox(hub, "bridge/ryzen2", bridgeIdentity, {});
    expect(batches[0]!.keys).toContain(rows.rowKey(`lease:${peer.identity.id}`));
    // A lease-only effect uses a bounded durable outbox row, so a crash before its file write
    // leaves fail-closed replay work even though the state topology itself did not change.
    fs.rmSync(path.join(hub.root, "host-leases"), { recursive: true, force: true });
    await side.mirror(peer.presence);
    expect(batches[1]!.keys).toContain(rows.rowKey(`lease:${peer.identity.id}`));
    expect(readHostLease(hub.root, peer.identity.id)).toBeDefined();
  });

  it("crash after the mirror commit, before the lease write: the next pass writes it once and deletes the row", async () => {
    const hub = store();
    const now = Date.now();
    const peer = remote("beta", now);
    // What a bridge that died between COMMIT and its effect leaves: the mirror and its row, no lease.
    const dead = new CommitOutbox(hub, "bridge/ryzen2", bridgeIdentity, {});
    const lease = { id: peer.identity.id, rootId: peer.identity.id, identityId: peer.identity.id, startedAt: peer.host.startedAt,
      updatedAt: now, expiresAt: now + 30_000 };
    await hub.writeBatch({ identity: bridgeIdentity, ops: [], prepare: () => dead.plan().stage([{
      kind: "put", key: hostKey(peer.identity.id), identity: peer.identity,
      value: { ...peer.host, updatedAt: now, expiresAt: now + 30_000, remoteHost: "ryzen2" },
    }], [{ kind: "lease", key: `lease:${peer.identity.id}`, payload: lease }]) });
    expect(readHostLease(hub.root, peer.identity.id)).toBeUndefined();
    expect(hub.listAll(dead.prefix, { fresh: true })).toHaveLength(1);
    const writes = vi.spyOn(fs, "renameSync");
    const side = new StoreBridgeSide(hub, "ryzen2");
    // An empty presence: only the recovered row can write the lease; the pass then withdraws the
    // mirror (the peer no longer reports it) and removes the lease again.
    await side.mirror({ hosts: [], participants: [] });
    const leaseWrites = writes.mock.calls.filter(([, to]) => leaseWrite(String(to)));
    expect(leaseWrites).toHaveLength(1);
    expect(hub.get(hostKey(peer.identity.id), { fresh: true })).toBeUndefined();
    expect(readHostLease(hub.root, peer.identity.id)).toBeUndefined();
    // The replayed rows are gone; only this pass's own removal row waits for the next commit.
    expect(hub.listAll(dead.prefix, { fresh: true }).map((entry) => (entry.value as { kind: string; key: string })))
      .toEqual([expect.objectContaining({ kind: "unlease", key: `lease:${peer.identity.id}` })]);
  });

  it("a replayed row never shortens a lease that was renewed since, and a replayed removal checks the commit", async () => {
    const hub = store();
    const now = Date.now();
    const peer = remote("gamma", now);
    const dead = new CommitOutbox(hub, "bridge/ryzen2", bridgeIdentity, {});
    const old = { id: peer.identity.id, rootId: peer.identity.id, identityId: peer.identity.id, updatedAt: now, expiresAt: now + 10_000 };
    const other = remote("delta", now);
    await hub.put({ key: hostKey(other.identity.id), identity: other.identity, value: other.host });
    await hub.writeBatch({ identity: bridgeIdentity, ops: [], prepare: () => dead.plan().stage([{
      kind: "put", key: hostKey(peer.identity.id), identity: peer.identity,
      value: { ...peer.host, remoteHost: "ryzen2" },
    }], [
      { kind: "lease", key: `lease:${peer.identity.id}`, payload: old },
      // A removal whose host is back (a native here): the replay must keep its lease.
      { kind: "unlease", key: `lease:${other.identity.id}`, payload: { key: hostKey(other.identity.id), id: other.identity.id } },
    ]) });
    writeHostLease(hub.root, { ...old, updatedAt: now + 5_000, expiresAt: now + 40_000 });
    writeHostLease(hub.root, { id: other.identity.id, rootId: other.identity.id, identityId: other.identity.id, updatedAt: now, expiresAt: now + 60_000 });
    const writes = vi.spyOn(fs, "renameSync");
    await new StoreBridgeSide(hub, "ryzen2").mirror({ hosts: [], participants: [] });
    // Neither replay wrote a lease (the renewed one is newer); the pass then withdrew the mirror.
    expect(writes.mock.calls.filter(([, to]) => leaseWrite(String(to)))).toEqual([]);
    expect(readHostLease(hub.root, peer.identity.id)).toBeUndefined();
    expect(readHostLease(hub.root, other.identity.id)).toBeDefined();
    // The replayed rows are gone; only this pass's own removal row waits for the next commit.
    expect(hub.listAll(dead.prefix, { fresh: true }).map((entry) => (entry.value as { kind: string; key: string })))
      .toEqual([expect.objectContaining({ kind: "unlease", key: `lease:${peer.identity.id}` })]);
  });

  it("holds lease and participant namespace custody from the input snapshot through COMMIT", async () => {
    const hub = store("sqlite");
    const now = Date.now();
    const peer = remote("epsilon", now);
    const side = new StoreBridgeSide(hub, "ryzen2");
    let entered!: () => void;
    let release!: () => void;
    const atWriteBatch = new Promise<void>(resolve => { entered = resolve; });
    const holdWriteBatch = new Promise<void>(resolve => { release = resolve; });
    const writeBatch = hub.writeBatch.bind(hub);
    vi.spyOn(hub, "writeBatch").mockImplementationOnce(async input => {
      entered();
      await holdWriteBatch;
      return writeBatch(input);
    });
    const mirroring = side.mirror(peer.presence);
    await atWriteBatch;

    // Both are legal writers using the same gates as production. They must wait until the
    // mirror's file/state snapshot has committed; neither can sneak in after capture but before
    // the SQL transaction's prepare/COMMIT boundary.
    let leaseWriterEntered = false;
    let participantWriterEntered = false;
    const replacement = { ...peer.host, startedAt: now + 10_000, updatedAt: now + 10_000 };
    const leaseWriter = withHostLeaseFileCustody(hub, hostLeasePath(hub.root, peer.identity.id), async () => {
      leaseWriterEntered = true;
      expect(hub.get(hostKey(peer.identity.id), { fresh: true })?.value).toMatchObject({ remoteHost: "ryzen2" });
      writeHostLease(hub.root, { id: peer.identity.id, rootId: peer.identity.id, identityId: peer.identity.id,
        startedAt: replacement.startedAt, updatedAt: replacement.updatedAt, expiresAt: now + 60_000 });
      await hub.put({ key: hostKey(peer.identity.id), identity: peer.identity, value: replacement });
    }, 5_000);
    const participantWriter = writeParticipantFileIf(hub, participantKey(peer.identity.id), () => {
      participantWriterEntered = true;
      expect(hub.get(participantKey(peer.identity.id), { fresh: true })?.value).toMatchObject({ remoteHost: "ryzen2" });
      return { key: participantKey(peer.identity.id), version: 1, updatedAt: now + 1, updatedBy: peer.identity,
        value: { ...peer.participant, status: "running", updatedAt: now + 1 } };
    });
    await new Promise<void>(resolve => setImmediate(resolve));
    expect(leaseWriterEntered).toBe(false);
    expect(participantWriterEntered).toBe(false);
    release();
    await Promise.all([mirroring, leaseWriter, participantWriter]);
    expect(leaseWriterEntered).toBe(true);
    expect(participantWriterEntered).toBe(true);
    expect(hub.get(hostKey(peer.identity.id), { fresh: true })?.value).toMatchObject({ startedAt: replacement.startedAt });
    expect(readHostLease(hub.root, peer.identity.id)?.startedAt).toBe(replacement.startedAt);
    expect(readParticipantFile(hub.root, participantKey(peer.identity.id))?.value).toMatchObject({ status: "running" });
  });

  it("batches a large first mirror, yields between batches, and releases SQLite for heartbeats", async () => {
    const hub = store("sqlite");
    const now = Date.now();
    const peers = Array.from({ length: 80 }, (_, index) => remote(`batch-${index}`, now));
    const presence = { hosts: peers.flatMap(peer => peer.presence.hosts),
      participants: peers.flatMap(peer => peer.presence.participants) };
    const side = new StoreBridgeSide(hub, "ryzen2", () => now);
    const original = hub.writeBatch.bind(hub);
    const batchSizes: number[] = [];
    let bridgeBatches = 0;
    let heartbeatDone = false;
    let firstCommitted!: () => void;
    let releaseFirst!: () => void;
    const firstBridgeCommit = new Promise<void>(resolve => { firstCommitted = resolve; });
    const waitForHeartbeat = new Promise<void>(resolve => { releaseFirst = resolve; });
    let heartbeatBeforeSecond = false;
    vi.spyOn(hub, "writeBatch").mockImplementation(async input => {
      if (input.lockClass !== "bridge" || !input.prepare) return original(input);
      const ordinal = ++bridgeBatches;
      if (ordinal === 2) {
        heartbeatBeforeSecond = heartbeatDone;
        const first = peers[0]!;
        const rows = new CommitOutbox(hub, "bridge/ryzen2", bridgeIdentity, {});
        expect(hub.get(hostKey(first.identity.id), { fresh: true })).toBeDefined();
        expect(hub.get(rows.rowKey(`lease:${first.identity.id}`), { fresh: true })).toBeDefined();
        // The row commit is visible but the file effect is still pending, so liveness fails closed.
        expect(readHostLease(hub.root, first.identity.id)).toBeUndefined();
      }
      const prepare = input.prepare;
      const result = await original({ ...input, prepare: view => {
        const ops = prepare(view);
        batchSizes.push(ops.length);
        return ops;
      } });
      if (ordinal === 1) { firstCommitted(); await waitForHeartbeat; }
      return result;
    });
    const mirror = side.mirror(presence);
    await firstBridgeCommit;
    const heartbeat = hub.put({ key: "heartbeat/concurrent", identity: bridgeIdentity, value: { at: now } })
      .then(() => { heartbeatDone = true; releaseFirst(); });
    await Promise.all([mirror, heartbeat]);
    expect(heartbeatBeforeSecond).toBe(true);
    expect(bridgeBatches).toBeGreaterThan(1);
    expect(Math.max(...batchSizes)).toBeLessThanOrEqual(144);
    for (const peer of peers) {
      expect(hub.get(hostKey(peer.identity.id), { fresh: true })).toBeDefined();
      expect(hub.get(participantKey(peer.identity.id), { fresh: true })).toBeDefined();
      expect(readHostLease(hub.root, peer.identity.id)).toBeDefined();
    }
    expect(hub.get("heartbeat/concurrent", { fresh: true })?.value).toEqual({ at: now });
  });
});

describe("owner-matched unlease (smarty-dev#6477 L2b owner review, P2)", () => {
  // A removal whose effect did not run (a crash between COMMIT and the effect): its row stays.
  const removeWithoutEffect = async (hub: MeshStore, side: StoreBridgeSide) => {
    const writeBatch = hub.writeBatch.bind(hub);
    // Only the mirror's own batch (recovery of earlier rows runs as usual).
    const spy = vi.spyOn(hub, "writeBatch").mockImplementation(async (input) => {
      if (input.lockClass !== "bridge") return writeBatch(input);
      const { commitOutbox: _dropped, ...rest } = input;
      return writeBatch(rest);
    });
    await side.mirror({ hosts: [], participants: [] });
    spy.mockRestore();
  };

  it("records the removed mirror's lease identity and a delayed replay removes only that lease", async () => {
    const hub = store();
    const peer = remote("zeta", Date.now());
    await new StoreBridgeSide(hub, "ryzen2").mirror(peer.presence);
    // The mirror's lease carries the origin's incarnation.
    expect(readHostLease(hub.root, peer.identity.id)?.startedAt).toBe(peer.host.startedAt);
    await removeWithoutEffect(hub, new StoreBridgeSide(hub, "ryzen2"));
    expect(hub.get(hostKey(peer.identity.id), { fresh: true })).toBeUndefined();
    const rows = new CommitOutbox(hub, "bridge/ryzen2", bridgeIdentity, {});
    expect(hub.get(rows.rowKey(`lease:${peer.identity.id}`), { fresh: true })?.value).toMatchObject({ kind: "unlease", payload: {
      key: hostKey(peer.identity.id), id: peer.identity.id, rootId: peer.identity.id, identityId: peer.identity.id, startedAt: peer.host.startedAt,
    } });
    expect(readHostLease(hub.root, peer.identity.id)).toBeDefined();
    // The replay finds the mirror's own lease and removes it.
    await new StoreBridgeSide(hub, "ryzen2").mirror({ hosts: [], participants: [] });
    expect(readHostLease(hub.root, peer.identity.id)).toBeUndefined();
  });

  it("a delayed replay keeps the lease of a replacement owner that wrote its file lease first", async () => {
    const hub = store();
    const now = Date.now();
    const peer = remote("eta", now);
    await new StoreBridgeSide(hub, "ryzen2").mirror(peer.presence);
    await removeWithoutEffect(hub, new StoreBridgeSide(hub, "ryzen2"));
    // A replacement owner of the same id (a new incarnation) writes its file lease before its
    // state record: the shared-state key is absent when the removal replays.
    const replacement = { id: peer.identity.id, rootId: peer.identity.id, identityId: peer.identity.id,
      startedAt: now + 5_000, updatedAt: now + 5_000, expiresAt: now + 60_000 };
    writeHostLease(hub.root, replacement);
    expect(hub.get(hostKey(peer.identity.id), { fresh: true })).toBeUndefined();
    await new StoreBridgeSide(hub, "ryzen2").mirror({ hosts: [], participants: [] });
    expect(readHostLease(hub.root, peer.identity.id)).toEqual(replacement);
    // The replayed row is retired all the same.
    const rows = new CommitOutbox(hub, "bridge/ryzen2", bridgeIdentity, {});
    await new StoreBridgeSide(hub, "ryzen2").mirror({ hosts: [], participants: [] });
    expect(hub.get(rows.rowKey(`lease:${peer.identity.id}`), { fresh: true })).toBeUndefined();
    expect(readHostLease(hub.root, peer.identity.id)).toEqual(replacement);
  });
});

describe("stale lease effect after a replacement mirror (pi-fabric#640 review round 2)", () => {
  it("overlapping SQLite bridge writers: the stale outbox effect keeps the replacement's lease and liveness", async () => {
    const hub = store("sqlite");
    expect(hub.stateBackend).toBe("sqlite");
    // A second writer for the same peer, through another connection to the same database.
    const other = new MeshStore(hub.root, 64 * 1024, 100, { stateBackend: "sqlite" });
    opened.push(other);
    const now = Date.now();
    const peer = remote("lambda", now);
    // The same id, root and identity, restarted: a new incarnation (startedAt).
    const restarted = { ...peer.host, startedAt: now + 5_000, updatedAt: now + 5_000 } as FabricHostRecord;
    const stale = new StoreBridgeSide(hub, "ryzen2");
    const replacing = new StoreBridgeSide(other, "ryzen2");
    let replacementLease: ReturnType<typeof readHostLease>;
    let staleCommitted = false;
    let replacementStarted = false;
    const writeBatch = hub.writeBatch.bind(hub);
    vi.spyOn(hub, "writeBatch").mockImplementation(async input => {
      const result = await writeBatch(input);
      if (input.lockClass === "bridge") staleCommitted = true;
      return result;
    });
    const leaseCustody = hub.leaseCustody.bind(hub);
    vi.spyOn(hub, "leaseCustody").mockImplementation(async (file, operation, timeoutMs, options) => {
      // The stale commit's directory custody has been released by the time its async effect
      // reacquires the additions gate. Install a replacement row/stamp in that gap.
      if (staleCommitted && !replacementStarted &&
          (file === hostLeaseAdditionsGatePath(hub.root) || file === hostLeasePath(hub.root, peer.identity.id))) {
        replacementStarted = true;
        await replacing.mirror({ hosts: [{ record: restarted, expiresAt: now + 60_000 }], participants: [peer.participant] });
        replacementLease = readHostLease(hub.root, peer.identity.id);
        expect(replacementLease?.startedAt).toBe(restarted.startedAt);
      }
      return leaseCustody(file, operation, timeoutMs, options);
    });
    await stale.mirror(peer.presence);
    expect(replacementStarted).toBe(true);
    const entry = hub.get(hostKey(peer.identity.id), { fresh: true });
    expect(entry?.value).toMatchObject({ startedAt: restarted.startedAt, remoteHost: "ryzen2" });
    // The replacement's lease survives the stale effect...
    expect(readHostLease(hub.root, peer.identity.id)).toEqual(replacementLease);
    // ...and liveness matches it: the live mirror never looks expired.
    const held = entry!.value as FabricHostRecord;
    const liveness = hostLiveness(readHostLeases(hub.root), held);
    expect(liveness.expiresAt).toBe(Math.max(held.expiresAt, replacementLease!.expiresAt));
    expect(liveness.expiresAt).toBeGreaterThan(Date.now());
    expect(readHostLeases(hub.root).get(peer.identity.id)?.startedAt).toBe(held.startedAt);
  });
});

describe("same-incarnation live lease effects (smarty-dev#6939)", () => {
  it("serialized SQLite mirrors preserve the later lease deadline", async () => {
    const hub = store("sqlite");
    expect(hub.stateBackend).toBe("sqlite");
    const other = new MeshStore(hub.root, 64 * 1024, 100, { stateBackend: "sqlite" });
    opened.push(other);
    const now = Date.now();
    const peer = remote("xi", now);
    const older = new StoreBridgeSide(hub, "ryzen2", () => now);
    const newer = new StoreBridgeSide(other, "ryzen2", () => now);
    await newer.mirror({ hosts: [], participants: [] });
    const writes = vi.spyOn(fs, "renameSync");
    await older.mirror({ hosts: [{ record: peer.host, expiresAt: now + 8_000 }], participants: [peer.participant] });
    expect(readHostLease(hub.root, peer.identity.id)?.expiresAt).toBe(now + 8_000);
    await newer.mirror({ hosts: [{ record: peer.host, expiresAt: now + 14_000 }], participants: [peer.participant] });
    const newerLease = readHostLease(hub.root, peer.identity.id);
    expect(newerLease).toMatchObject({ startedAt: peer.host.startedAt, expiresAt: now + 14_000 });
    expect(hub.get(hostKey(peer.identity.id), { fresh: true })?.value)
      .toMatchObject({ startedAt: peer.host.startedAt, remoteHost: "ryzen2" });
    expect(readHostLease(hub.root, peer.identity.id)).toEqual(newerLease);
    expect(writes.mock.calls.filter(([, to]) => leaseWrite(String(to)))).toHaveLength(2);
  });

  it("a live effect still replaces a later-expiring lease of another incarnation", async () => {
    const hub = store("sqlite");
    const now = Date.now();
    const peer = remote("omicron", now);
    const restarted = { ...peer.host, startedAt: now + 1_000, updatedAt: now + 1_000 } as FabricHostRecord;
    // The previous incarnation's lease, expiring later than the restarted mirror's.
    writeHostLease(hub.root, { id: peer.identity.id, rootId: peer.identity.id, identityId: peer.identity.id,
      startedAt: peer.host.startedAt, updatedAt: now, expiresAt: now + 60_000 });
    await new StoreBridgeSide(hub, "ryzen2", () => now + 1_000)
      .mirror({ hosts: [{ record: restarted, expiresAt: now + 9_000 }], participants: [peer.participant] });
    expect(readHostLease(hub.root, peer.identity.id)).toMatchObject({ startedAt: restarted.startedAt, expiresAt: now + 9_000 });
  });
});

describe("legacy outbox rows without an incarnation (pi-fabric#640 review round 3)", () => {
  it("a legacy lease row replayed after a restarted mirror writes nothing; a legacy unlease row removes nothing", async () => {
    const hub = store();
    const now = Date.now();
    const peer = remote("mu", now);
    const gone = remote("nu", now);
    // The same id, root and identity, restarted: the held mirror is the new incarnation.
    const restarted = { ...peer.host, startedAt: now + 5_000, updatedAt: now + 5_000, remoteHost: "ryzen2" };
    // Rows of an earlier release: no startedAt. The lease row expires later than the replacement's.
    const dead = new CommitOutbox(hub, "bridge/ryzen2", bridgeIdentity, {});
    await hub.writeBatch({ identity: bridgeIdentity, ops: [], prepare: () => dead.plan().stage([{
      kind: "put", key: hostKey(peer.identity.id), identity: peer.identity, value: restarted,
    }], [
      { kind: "lease", key: `lease:${peer.identity.id}`, payload: {
        id: peer.identity.id, rootId: peer.identity.id, identityId: peer.identity.id, updatedAt: now, expiresAt: now + 50_000 } },
      { kind: "unlease", key: `lease:${gone.identity.id}`, payload: {
        key: hostKey(gone.identity.id), id: gone.identity.id, rootId: gone.identity.id, identityId: gone.identity.id } },
    ]) });
    const replacement = { id: peer.identity.id, rootId: peer.identity.id, identityId: peer.identity.id,
      startedAt: restarted.startedAt, updatedAt: now + 5_000, expiresAt: now + 20_000 };
    writeHostLease(hub.root, replacement);
    // A lease file without an incarnation, whose state record is absent.
    const unstamped = { id: gone.identity.id, rootId: gone.identity.id, identityId: gone.identity.id, updatedAt: now, expiresAt: now + 60_000 };
    writeHostLease(hub.root, unstamped);
    // Only recovery runs effects: this pass's own commitOutbox is dropped.
    const writeBatch = hub.writeBatch.bind(hub);
    vi.spyOn(hub, "writeBatch").mockImplementation(async (input) => {
      if (input.lockClass !== "bridge") return writeBatch(input);
      const { commitOutbox: _dropped, ...rest } = input;
      return writeBatch(rest);
    });
    const writes = vi.spyOn(fs, "renameSync");
    await new StoreBridgeSide(hub, "ryzen2").mirror({ hosts: [], participants: [] });
    expect(writes.mock.calls.filter(([, to]) => leaseWrite(String(to)))).toEqual([]);
    // The replacement's lease and expiry stay as they were; the unstamped lease is not removed.
    expect(readHostLease(hub.root, peer.identity.id)).toEqual(replacement);
    expect(readHostLease(hub.root, gone.identity.id)).toEqual(unstamped);
    // Both legacy rows are retired all the same.
    expect(hub.get(dead.rowKey(`lease:${gone.identity.id}`), { fresh: true })).toBeUndefined();
    expect(hub.listAll(dead.prefix, { fresh: true }).map((entry) => (entry.value as { payload: { startedAt?: number } }).payload))
      .toEqual([expect.objectContaining({ startedAt: restarted.startedAt })]);
  });
});

describe("R20 bridge write fence on SQLite (smarty-dev#6477 L2b owner review, P1)", () => {
  const event = (id: string, from: MeshIdentity): BridgePublish => ({
    topic: "fleet.work.task", kind: "ask", from, to: "session:hub-main", data: { bridge: { from: "ryzen2", id } },
  });
  const rawDatabase = async (root: string) => {
    const { DatabaseSync } = await import("node:sqlite");
    const db = new DatabaseSync(path.join(root, "state.db"));
    db.exec("PRAGMA busy_timeout = 0");
    return db;
  };

  it("holds the SQLite write lock from holds() through the append: a takeover commits after it, then refuses the stale owner", async () => {
    const hub = store("sqlite");
    expect(hub.stateBackend).toBe("sqlite");
    const peer = remote("theta", Date.now());
    const side = new StoreBridgeSide(hub, "ryzen2");
    await side.mirror(peer.presence);
    expect(side.holds(peer.identity.id)).toBe(true);
    // A native takeover of the same id, through another connection to the same database.
    const native = new MeshStore(hub.root, 64 * 1024, 100, { stateBackend: "sqlite" });
    opened.push(native);
    const raw = await rawDatabase(hub.root);
    const holds = side.holds.bind(side);
    let probe: string | undefined;
    let takeover: Promise<unknown> | undefined;
    let sequenceAtTakeover = -1;
    vi.spyOn(side, "holds").mockImplementation((id) => {
      const held = holds(id);
      if (takeover) return held;
      // Between holds() and the append: another writer cannot take the write lock...
      try { raw.exec("BEGIN IMMEDIATE"); raw.exec("ROLLBACK"); probe = "acquired"; }
      catch (error) { probe = (error as { errcode?: number }).errcode === undefined ? String(error) : `errcode:${(error as { errcode: number }).errcode & 0xff}`; }
      // ...and a takeover's commit, attempted now, runs only after the append.
      takeover = native.writeBatch({ identity: peer.identity, ops: [], prepare: () => {
        sequenceAtTakeover = native.latestSequence();
        return [{ kind: "put", key: hostKey(peer.identity.id), value: { ...peer.host, startedAt: peer.host.startedAt + 1 }, identity: peer.identity }];
      } });
      return held;
    });
    const published = await side.publish(event("e1", peer.identity), [peer.identity.id]);
    expect(probe).toBe("errcode:5"); // SQLITE_BUSY
    await takeover;
    expect(sequenceAtTakeover).toBeGreaterThanOrEqual(published.sequence);
    raw.close();
    // The takeover committed: the stale owner's next publish is refused, nothing is appended.
    vi.mocked(side.holds).mockRestore();
    expect(side.holds(peer.identity.id)).toBe(false);
    await expect(side.publish(event("e2", peer.identity), [peer.identity.id])).rejects.toBeInstanceOf(BridgeOwnershipError);
    expect(hub.latestSequence()).toBe(published.sequence);
    // The fence wrote nothing and is released: an ordinary write goes through.
    await hub.put({ key: "probe/after", value: 1, identity: bridgeIdentity });
  });

  it("a busy write lock refuses before the event stamp; the same event publishes after it is free", async () => {
    const hub = store("sqlite");
    const peer = remote("iota", Date.now());
    const side = new StoreBridgeSide(hub, "ryzen2");
    await side.mirror(peer.presence);
    const before = hub.latestSequence();
    const raw = await rawDatabase(hub.root);
    raw.exec("BEGIN IMMEDIATE");
    const checks = vi.spyOn(side, "holds");
    const retriedEvent = event("e3", peer.identity);
    await expect(side.publish(retriedEvent, [peer.identity.id])).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    // Refused before the ownership check and the append, never published unfenced or stamped.
    expect(checks).not.toHaveBeenCalled();
    expect(hub.latestSequence()).toBe(before);
    raw.exec("ROLLBACK");
    raw.close();
    const published = await side.publish(retriedEvent, [peer.identity.id]);
    expect(published.sequence).toBeGreaterThan(before);
    // The sender's id, with the identity the event carries (checked against its record, pi-fabric#876).
    expect(checks).toHaveBeenCalledWith(peer.identity.id, expect.objectContaining({ id: peer.identity.id, name: peer.identity.name, kind: peer.identity.kind }));
  });

  it("an event with no held ids takes no state fence", async () => {
    const hub = store("sqlite");
    const peer = remote("kappa", Date.now());
    const side = new StoreBridgeSide(hub, "ryzen2");
    const fence = vi.spyOn(hub, "withStateWriteFence");
    await side.publish(event("e4", peer.identity));
    expect(fence).not.toHaveBeenCalled();
    await side.mirror(peer.presence);
    await side.publish(event("e5", peer.identity), [peer.identity.id]);
    expect(fence).toHaveBeenCalledTimes(1);
  });
});

