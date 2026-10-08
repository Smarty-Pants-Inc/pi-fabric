import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { StoreBridgeSide } from "../src/mesh/bridge.js";
import { CommitOutbox } from "../src/mesh/commit-outbox.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { readHostLease, writeHostLease } from "../src/topology/host-leases.js";
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
const store = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-bridge-outbox-"));
  roots.push(root);
  return new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
};
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
      return writeBatch({ ...input, prepare: prepare && ((view) => {
        const ops = prepare(view);
        batches.push({ keys: ops.map((op) => op.key) });
        return ops;
      }) });
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
    await hub.writeBatch({ identity: bridgeIdentity, ops: [], prepare: () => dead.stage([{
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
    const leaseWrites = writes.mock.calls.filter(([, to]) => String(to).includes(`${path.sep}host-leases${path.sep}`));
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
    await hub.writeBatch({ identity: bridgeIdentity, ops: [], prepare: () => dead.stage([{
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
    expect(writes.mock.calls.filter(([, to]) => String(to).includes(`${path.sep}host-leases${path.sep}`))).toEqual([]);
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
