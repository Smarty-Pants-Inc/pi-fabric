import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { BridgeOwnershipError, StoreBridgeSide, type BridgePublish } from "../src/mesh/bridge.js";
import { CommitOutbox } from "../src/mesh/commit-outbox.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { hostLiveness, readHostLease, readHostLeases, writeHostLease } from "../src/topology/host-leases.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";
import type { FabricHostRecord, FabricParticipantRecord } from "../src/topology/types.js";

// smarty-dev#6477 L2b (R11): the bridge mirror's host-lease effects run after its state commit from
// rows recorded in that commit, and its participant-file reads happen before the transaction.
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
    // A lease-only renewal commits nothing and records nothing; it still renews the lease file.
    fs.rmSync(path.join(hub.root, "host-leases"), { recursive: true, force: true });
    await side.mirror(peer.presence);
    expect(batches[1]!.keys).toEqual([]);
    expect(readHostLease(hub.root, peer.identity.id)).toBeDefined();
  });

  it("crash after the mirror commit, before the lease write: the next pass writes it once and deletes the row", async () => {
    const hub = store();
    const now = Date.now();
    const peer = remote("beta", now);
    // What a bridge that died between COMMIT and its effect leaves: the mirror and its row, no lease.
    const dead = new CommitOutbox(hub, "bridge/ryzen2", bridgeIdentity, {});
    const lease = { id: peer.identity.id, rootId: peer.identity.id, identityId: peer.identity.id, updatedAt: now, expiresAt: now + 30_000 };
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

  it("reads participant files before the transaction and again only when their directory moved", async () => {
    const hub = store();
    const now = Date.now();
    const peer = remote("epsilon", now);
    const side = new StoreBridgeSide(hub, "ryzen2");
    // A native file for the same id lands between the pre-read and the commit: the stamp moved,
    // so the transaction reads again and the mirror never covers the native.
    const writeBatch = hub.writeBatch.bind(hub);
    vi.spyOn(hub, "writeBatch").mockImplementationOnce(async (input) => {
      writeParticipantFile(hub.root, { key: participantKey(peer.identity.id), version: 1, updatedAt: now, updatedBy: peer.identity,
        value: { ...peer.participant } });
      return writeBatch(input);
    });
    await side.mirror(peer.presence);
    expect(hub.get(participantKey(peer.identity.id), { fresh: true })).toBeUndefined();
  });
});

describe("owner-matched unlease (smarty-dev#6477 L2b owner review, P2)", () => {
  // A removal whose effect did not run (a crash between COMMIT and the effect): its row stays.
  const removeWithoutEffect = async (hub: MeshStore, side: StoreBridgeSide) => {
    const writeBatch = hub.writeBatch.bind(hub);
    // Only the mirror's own batch (recovery of earlier rows runs as usual).
    const spy = vi.spyOn(hub, "writeBatch").mockImplementation(async (input) => {
      if (input.lockClass !== "bridge") return writeBatch(input);
      const { afterCommit: _dropped, ...rest } = input;
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
  it("overlapping SQLite bridge writers: the stale afterCommit keeps the replacement's lease and liveness", async () => {
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
    let staleEffects = 0;
    const writeBatch = hub.writeBatch.bind(hub);
    vi.spyOn(hub, "writeBatch").mockImplementation(async (input) => {
      if (input.lockClass !== "bridge" || !input.afterCommit) return writeBatch(input);
      const { afterCommit, ...rest } = input;
      // The first writer commits its mirror; before its effect reacquires the store, the second
      // writer installs the replacement mirror and writes its lease.
      const results = await writeBatch(rest);
      await replacing.mirror({ hosts: [{ record: restarted, expiresAt: now + 60_000 }], participants: [peer.participant] });
      replacementLease = readHostLease(hub.root, peer.identity.id);
      expect(replacementLease?.startedAt).toBe(restarted.startedAt);
      // The stale effect then runs on the committed state, which now holds the replacement.
      await writeBatch({ identity: bridgeIdentity, ops: [], afterCommit: (view) => { staleEffects += 1; afterCommit(view); } });
      return results;
    });
    await stale.mirror(peer.presence);
    expect(staleEffects).toBe(1);
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

  it("a busy write lock refuses the fenced event before its stamp and the publish retries after it is free", async () => {
    const hub = store("sqlite");
    const peer = remote("iota", Date.now());
    const side = new StoreBridgeSide(hub, "ryzen2");
    await side.mirror(peer.presence);
    const before = hub.latestSequence();
    const raw = await rawDatabase(hub.root);
    raw.exec("BEGIN IMMEDIATE");
    const checks = vi.spyOn(side, "holds");
    let settled = false;
    const publishing = side.publish(event("e3", peer.identity), [peer.identity.id]).finally(() => { settled = true; });
    await new Promise((resolve) => setTimeout(resolve, 60));
    expect(settled).toBe(false);
    // Refused before the ownership check and the append, never published unfenced.
    expect(checks).not.toHaveBeenCalled();
    expect(hub.latestSequence()).toBe(before);
    raw.exec("ROLLBACK");
    raw.close();
    const published = await publishing;
    expect(published.sequence).toBeGreaterThan(before);
    expect(checks).toHaveBeenCalledWith(peer.identity.id);
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

