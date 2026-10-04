import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { StoreBridgeSide } from "../src/mesh/bridge.js";
import { deadHostRecords, reapDeadHostRecords } from "../src/topology/host-reaper.js";
import { LIVENESS_POLICY_KEY, readHostLeases, writeHostLease } from "../src/topology/host-leases.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { isLiveLegacyRootEntry, sessionLiveness } from "../src/topology/legacy-root-liveness.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";

const directories: ParticipantDirectory[] = [];
const roots: string[] = [];
afterEach(async () => {
  vi.restoreAllMocks();
  await Promise.all(directories.splice(0).map(directory => directory.close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const key = (prefix: string, id: string) => prefix + createHash("sha256").update(id).digest("hex");
const setup = async (files: boolean, leaseMs = 15_000, kind: MeshIdentity["kind"] = "main", legacyPeer = false) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-heartbeat-contention-"));
  roots.push(root);
  let now = Date.now();
  vi.spyOn(Date, "now").mockImplementation(() => now);
  const identity: MeshIdentity = { id: "session:live", name: "main", kind, sessionId: "live" };
  const mesh = new MeshStore(root, 64 * 1024, 100, { readCacheMs: 0 });
  if (files) await mesh.put({ key: LIVENESS_POLICY_KEY, value: { version: 1, hostLeases: "files" }, identity });
  const record: FabricParticipantRecord = {
    format: 1, id: identity.id, rootId: identity.id, ownerHostId: identity.id, ownerIdentityId: identity.id,
    kind: "root", name: "main", label: "TEST-1", status: "idle", runner: "pi", transport: "host",
    capabilities: ["steer", "followUp", "fabric"], cwd: root, sessionId: "live", startedAt: now,
    updatedAt: now, pendingMessages: false, controlProtocol: "v1",
  };
  const directory = new ParticipantDirectory(mesh, {
    enabled: true, identity, rootId: identity.id, hostId: identity.id,
    heartbeatMs: 5_000, leaseMs, reapDeadHosts: false,
  });
  directories.push(directory);
  directory.registerSource(() => [{ ...record, updatedAt: Date.now() }]);
  if (legacyPeer) {
    const peer: MeshIdentity = { id: "session:legacy", name: "main", kind: "main", sessionId: "legacy" };
    await mesh.put({ key: key("topology/hosts/", peer.id), identity: peer, value: {
      format: 1, id: peer.id, rootId: peer.id, identity: peer, startedAt: now,
      updatedAt: now, expiresAt: now + 1_000_000,
    } });
  }
  await directory.refresh();
  const writes = vi.spyOn(mesh, "writeBatch");
  const hostKey = key("topology/hosts/", identity.id);
  const participantKey = key("topology/participants/", identity.id);
  return { root, mesh, identity, record, directory, writes, hostKey, participantKey,
    advance: (ms: number) => { now += ms; }, now: () => now };
};

describe("#3752 participant heartbeat contention", () => {
  it.each([false, true])("all-new fleet renews both TTLs only in the small file (policy: %s)", async files => {
    const { root, directory, mesh, writes, advance, hostKey, participantKey } = await setup(files, 120_000);
    const before = fs.readFileSync(path.join(root, "state.json"), "utf8");
    const session = mesh.get("sessions/live")!;
    const host = mesh.get(hostKey)!;
    const participant = mesh.get(participantKey)!;
    expect(participant.value).toMatchObject({ livenessLeaseFiles: 1 });
    for (let tick = 0; tick < 24; tick++) {
      advance(5_000);
      await directory.refresh();
      expect(isLiveLegacyRootEntry(session, Date.now(), root)).toBe(true);
      expect(sessionLiveness(session, root)).toEqual({ updatedAt: Date.now(), expiresAt: Date.now() + 15_000 });
    }
    expect(writes).not.toHaveBeenCalled();
    expect(fs.readFileSync(path.join(root, "state.json"), "utf8")).toBe(before);
    expect(mesh.get(hostKey)).toEqual(host);
    expect(mesh.get(participantKey)).toEqual(participant);
    advance(15_001); // a crashed Main's session still lapses at the original fixed TTL
    expect(isLiveLegacyRootEntry(session, Date.now(), root)).toBe(false);
    await directory.refresh(); // lock-confirmed re-acquisition, without another identity rewrite
    expect(isLiveLegacyRootEntry(session, Date.now(), root)).toBe(true);
    expect(writes).not.toHaveBeenCalled();
  });

  it("cached sessions and peers follow fresh file liveness, lapse at 15 s and re-acquire without state writes", async () => {
    const { root, directory, identity, writes, advance } = await setup(false);
    const observerIdentity: MeshIdentity = { id: "session:observer", name: "main", kind: "main", sessionId: "observer" };
    const observer = new ParticipantDirectory(new MeshStore(root, 64 * 1024, 100, { readCacheMs: 60_000 }), {
      enabled: true, identity: observerIdentity, rootId: observerIdentity.id, hostId: observerIdentity.id, reapDeadHosts: false,
    });
    directories.push(observer);
    expect(observer.sessions().map(root => root.id)).toEqual([identity.id]); // cache the identity
    advance(20_000);
    await directory.refresh();
    expect(observer.sessions().map(root => root.id)).toEqual([identity.id]);
    expect(observer.peers().map(peer => peer.id)).toEqual([identity.id]);
    expect(writes).not.toHaveBeenCalled();
    advance(15_001);
    expect(observer.sessions()).toEqual([]);
    expect(observer.peers()).toEqual([]);
    await directory.refresh();
    expect(observer.sessions().map(root => root.id)).toEqual([identity.id]);
    expect(writes).not.toHaveBeenCalled();
  });

  it("re-enters legacy mode on an old peer joining, then stops state renewal when it lapses", async () => {
    const { directory, mesh, writes, advance } = await setup(false);
    advance(20_000);
    await directory.refresh();
    expect(writes).not.toHaveBeenCalled();
    const peer: MeshIdentity = { id: "session:legacy", name: "main", kind: "main", sessionId: "legacy" };
    await mesh.put({ key: "sessions/legacy", identity: peer, value: {
      id: peer.id, sessionId: "legacy", cwd: "/tmp/legacy", status: "idle", startedAt: Date.now(),
    } });
    await directory.refresh();
    expect(writes).toHaveBeenCalledOnce();
    expect(mesh.get("sessions/live")!.updatedAt).toBe(Date.now());
    advance(10_000);
    await directory.refresh();
    expect(writes).toHaveBeenCalledTimes(2);
    advance(10_000); // older session is now stale: compatibility writes stop automatically
    await directory.refresh();
    expect(writes).toHaveBeenCalledTimes(2);
  });

  it.each(["root", "identity", "session", "incarnation", "host incarnation"])("a mismatched %s lease cannot revive a stale session", async mismatch => {
    const { root, mesh, identity, advance } = await setup(false);
    const session = mesh.get("sessions/live")!;
    const lease = readHostLeases(root).get(identity.id)!;
    advance(20_000);
    writeHostLease(root, { ...lease, updatedAt: Date.now(), expiresAt: Date.now() + 15_000,
      ...(mismatch === "root" ? { rootId: "another" } : {}),
      ...(mismatch === "host incarnation" ? { startedAt: lease.startedAt! + 1 } : {}),
      ...(mismatch === "identity" ? { identityId: "another" } : {}),
      session: { ...lease.session!, updatedAt: Date.now(), expiresAt: Date.now() + 15_000,
        ...(mismatch === "session" ? { id: "another" } : {}),
        ...(mismatch === "incarnation" ? { startedAt: lease.session!.startedAt + 1 } : {}),
      },
    });
    expect(isLiveLegacyRootEntry(session, Date.now(), root)).toBe(false);
  });

  it("temporarily restores state advertisements if an old peer joins a participant-file fleet", async () => {
    const { directory, mesh, identity, advance, participantKey } = await setup(true);
    await mesh.put({ key: LIVENESS_POLICY_KEY, identity, value: { version: 1, hostLeases: "files", participants: "files" } });
    await directory.refresh();
    expect(mesh.get("sessions/live")).toBeUndefined();
    expect(mesh.get(participantKey)).toBeUndefined();
    const peer: MeshIdentity = { id: "session:legacy", name: "main", kind: "main", sessionId: "legacy" };
    await mesh.put({ key: "sessions/legacy", identity: peer, value: {
      id: peer.id, sessionId: "legacy", cwd: "/tmp/legacy", status: "idle", startedAt: Date.now(),
    } });
    await directory.refresh();
    expect(mesh.get("sessions/live")!.updatedAt).toBe(Date.now());
    expect(mesh.get(participantKey)?.value).toMatchObject({ livenessLeaseFiles: 1 });
    advance(10_000);
    await directory.refresh();
    expect(mesh.get("sessions/live")!.updatedAt).toBe(Date.now());
    advance(10_000);
    await directory.refresh();
    expect(mesh.get("sessions/live")).toBeUndefined();
    expect(mesh.get(participantKey)).toBeUndefined();
  });

  it("an unadvertised live participant triggers fallback even under an advertised host", async () => {
    const { directory, mesh, writes, advance, record } = await setup(false);
    const peer: MeshIdentity = { id: "session:peer", name: "main", kind: "main", sessionId: "peer" };
    const participantKey = key("topology/participants/", peer.id);
    const participant = { ...record, id: peer.id, rootId: peer.id, ownerHostId: peer.id,
      ownerIdentityId: peer.id, sessionId: "peer" };
    await mesh.writeBatch({ identity: peer, ops: [
      { kind: "put", key: key("topology/hosts/", peer.id), value: { format: 1, id: peer.id, rootId: peer.id,
        identity: peer, startedAt: Date.now(), updatedAt: Date.now(), expiresAt: Date.now() + 120_000, livenessLeaseFiles: 1 } },
      { kind: "put", key: participantKey, value: participant },
    ] });
    writes.mockClear();
    advance(10_000);
    await directory.refresh();
    expect(writes).toHaveBeenCalledOnce();
    await mesh.put({ key: participantKey, value: { ...participant, livenessLeaseFiles: 1 }, identity: peer });
    advance(10_000);
    await directory.refresh();
    expect(writes).toHaveBeenCalledOnce(); // all live peers now advertise the reader contract
  });

  it.each([false, true])("timestamp-only refresh does not write state (file policy: %s)", async files => {
    const { root, directory, writes, identity, advance } = await setup(files);
    const before = fs.readFileSync(path.join(root, "state.json"), "utf8");
    advance(5_000);
    await directory.refresh();
    expect(writes).not.toHaveBeenCalled();
    expect(fs.readFileSync(path.join(root, "state.json"), "utf8")).toBe(before);
    expect(readHostLeases(root).get(identity.id)?.updatedAt).toBe(Date.now());
    expect(directory.confirmedAt()).toBe(Date.now());
  });

  it.each([false, true])("status and capability changes publish immediately (file policy: %s)", async files => {
    const { directory, record, writes, advance } = await setup(files);
    advance(1_000);
    record.status = "running";
    await directory.refresh();
    expect(writes).toHaveBeenCalledTimes(1);
    expect(directory.get(record.id)).toMatchObject({ status: "running", stale: false });
    record.capabilities = ["fabric"];
    await directory.refresh();
    expect(writes).toHaveBeenCalledTimes(2);
    expect(directory.get(record.id)?.capabilities).toEqual(["fabric"]);
  });

  it.each([false, true])("a real change does not republish a timestamp-only sibling (file policy: %s)", async files => {
    const { directory, mesh, record, writes, advance } = await setup(files);
    const sibling: FabricParticipantRecord = {
      ...record, id: "agent:sibling", kind: "agent", parentId: record.id, name: "sibling", transport: "process",
    };
    directory.registerSource(() => [{ ...sibling, updatedAt: Date.now() }]);
    await directory.refresh();
    const siblingKey = key("topology/participants/", sibling.id);
    const before = mesh.get(siblingKey)!;
    writes.mockClear();
    advance(1_000);
    record.status = "running";
    await directory.refresh();
    expect(writes).toHaveBeenCalledOnce();
    expect(directory.get(record.id)?.status).toBe("running");
    expect(mesh.get(siblingKey)!.version).toBe(before.version);
    expect(directory.get(sibling.id)).toMatchObject({ stale: false });
  });

  it("renews state leases at half-life without rewriting timestamp-only participants", async () => {
    const { directory, mesh, hostKey, participantKey, writes, advance } = await setup(false, 15_000, "main", true);
    const participant = mesh.get(participantKey)!;
    const host = mesh.get(hostKey)!;
    advance(5_000);
    await directory.refresh();
    advance(5_000);
    await directory.refresh();
    expect(writes).toHaveBeenCalledTimes(1);
    expect(mesh.get(hostKey)!.version).toBeGreaterThan(host.version);
    expect(mesh.get(participantKey)!.version).toBe(participant.version);
    expect(mesh.get("sessions/live")!.updatedAt).toBe(Date.now());
  });

  it.each([false, true])("renews the legacy session and host together at the old half-life (policy: %s)", async files => {
    const { directory, mesh, writes, advance } = await setup(files, 120_000, "main", true);
    advance(7_499);
    await directory.refresh();
    expect(writes).not.toHaveBeenCalled();
    advance(1);
    await directory.refresh();
    expect(writes).toHaveBeenCalledOnce();
    expect(writes.mock.calls[0]?.[0].ops.map(operation => operation.key)).toEqual([
      "sessions/live",
      expect.stringContaining("topology/hosts/"),
    ]);
    expect(mesh.get("sessions/live")!.updatedAt).toBe(Date.now());
  });

  it.each([false, true])("mixed idle renewal is six paired commits per minute on the default heartbeat (policy: %s)", async files => {
    const { directory, mesh, writes, advance, hostKey, participantKey, now } = await setup(files, 15_000, "main", true);
    const participant = mesh.get(participantKey)!;
    const start = now();
    const committedAt: number[] = [];
    for (let tick = 1; tick <= 24; tick++) {
      advance(5_000);
      const before = writes.mock.calls.length;
      await directory.refresh();
      if (writes.mock.calls.length > before) committedAt.push(now() - start);
      // This is the OLD session-only reader's check: no meshRoot/file-lease fallback.
      expect(isLiveLegacyRootEntry(mesh.get("sessions/live")!, now())).toBe(true);
    }
    expect(committedAt).toEqual(Array.from({ length: 12 }, (_, index) => (index + 1) * 10_000));
    expect(writes.mock.calls).toHaveLength(12);
    for (const [batch] of writes.mock.calls) {
      expect(batch.ops.map(operation => operation.key)).toEqual(["sessions/live", hostKey]);
    }
    expect(mesh.get(participantKey)).toEqual(participant);
  });

  it("session-only readers retain the fixed 15 s TTL, ignoring a stored expiry", async () => {
    const { mesh, advance, now } = await setup(false);
    const session = mesh.get("sessions/live")!;
    const advertised = { ...session, value: { ...(session.value as Record<string, unknown>), expiresAt: now() + 120_000, ttl: 120_000 } };
    advance(15_000);
    expect(isLiveLegacyRootEntry(advertised, now())).toBe(true);
    advance(1);
    expect(isLiveLegacyRootEntry(advertised, now())).toBe(false);
  });

  it("uses a long host lease's own half-life when there is no legacy session lease", async () => {
    const { directory, mesh, hostKey, participantKey, writes, advance } = await setup(false, 120_000, "actor", true);
    const participant = mesh.get(participantKey)!;
    const host = mesh.get(hostKey)!;
    expect(mesh.get("sessions/live")).toBeUndefined();
    for (let tick = 1; tick <= 12; tick++) {
      advance(10_000);
      await directory.refresh();
      expect(writes).toHaveBeenCalledTimes(Math.floor(tick / 6));
    }
    expect(mesh.get(hostKey)!.version).toBeGreaterThan(host.version);
    expect(mesh.get(participantKey)).toEqual(participant);
  });

  it("keeps the legacy session TTL fresh even with a longer host lease", async () => {
    const { directory, mesh, writes, advance } = await setup(false, 120_000, "main", true);
    advance(10_000);
    await directory.refresh();
    expect(writes).toHaveBeenCalledTimes(1);
    expect(mesh.get("sessions/live")!.updatedAt).toBe(Date.now());
  });

  it("unchanged participants stay live to directory, lineage, reaper and bridge, then expire", async () => {
    const { directory, mesh, hostKey, participantKey, identity, writes, advance, now } = await setup(true);
    const participant = mesh.get(participantKey)!;
    advance(20_000); // state lease has lapsed, but the independent file lease renews
    await directory.refresh();
    expect(writes).not.toHaveBeenCalled();
    expect(mesh.get(participantKey)!.version).toBe(participant.version);
    expect(directory.get(identity.id)).toMatchObject({ stale: false });
    expect(directory.lineageAlive(identity.id)).toBe(true);
    expect(deadHostRecords(mesh, { ownHostId: "observer", now: now(), deadAfterMs: 0 })).toEqual([]);
    const bridge = new StoreBridgeSide(mesh, "other", now);
    const presence = await bridge.presence();
    expect(presence.participants.map(p => p.id)).toEqual([identity.id]);
    expect(presence.hosts[0]!.record.updatedAt).toBe(now());
    expect(presence.hosts[0]!.expiresAt - presence.hosts[0]!.record.updatedAt).toBe(15_000);
    advance(20_000); // no refresh: both liveness sources lapse
    expect(directory.get(identity.id)).toBeUndefined();
    expect((await bridge.presence()).participants).toEqual([]);
    // Expiry is not proof of lineage closure, even when the stale row is reaped.
    expect(directory.lineageAlive(identity.id)).toBe(true);
    expect(await reapDeadHostRecords(mesh, identity, { ownHostId: "observer", now: now(), deadAfterMs: 0 })).toBeGreaterThan(0);
    expect(mesh.get(hostKey)).toBeUndefined();
    expect(mesh.get(participantKey)).toBeUndefined();
    expect(directory.lineageAlive(identity.id)).toBe(true);
  });
});
