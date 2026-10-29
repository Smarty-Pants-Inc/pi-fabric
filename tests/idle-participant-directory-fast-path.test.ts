import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { hostLeasePath, LIVENESS_POLICY_KEY, readHostLeaseCurrent, writeHostLease } from "../src/topology/host-leases.js";
import { ParticipantDirectory, type FabricRootCollision } from "../src/topology/participant-directory.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";

// Keep reason: real-sized idle-path regression plus compatibility/owner-liveness boundaries.
// Real MeshStore snapshots, participant files and lease files; no mocked directory/lease answers.
const roots: string[] = [];
const directories: ParticipantDirectory[] = [];
const stores: MeshStore[] = [];
afterEach(async () => {
  vi.restoreAllMocks();
  vi.useRealTimers();
  await Promise.all(directories.splice(0).map(directory => directory.close()));
  for (const store of stores.splice(0)) store.closeState();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const identityOf = (name: string): MeshIdentity => ({ id: `session:${name}`, name, kind: "main", sessionId: name });
const keyFor = (prefix: string, id: string): string => prefix + createHash("sha256").update(id).digest("hex");
const participantKey = (id: string): string => keyFor("topology/participants/", id);
const recordOf = (identity: MeshIdentity): FabricParticipantRecord => ({
  format: 1, id: identity.id, kind: "root", rootId: identity.id, ownerHostId: identity.id, ownerIdentityId: identity.id,
  name: identity.name, ...(identity.sessionId !== undefined ? { sessionId: identity.sessionId } : {}),
  status: "idle", runner: "pi", transport: "host",
  capabilities: ["steer", "followUp", "fabric"], cwd: process.cwd(), startedAt: 1,
  updatedAt: Date.now(), controlProtocol: "v1", livenessLeaseFiles: 1,
});

// Only the existing heartbeat clock is driven; disk/lock operations and their receipts stay real.
const clock = () => vi.useFakeTimers({ toFake: ["Date", "setInterval", "clearInterval"] });

describe.each(["file", "sqlite"] as const)("idle participant structural fast paths (%s)", backend => {
  const fixture = async () => {
    clock();
    const base = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-idle-structural-"));
    roots.push(base);
    const meshRoot = path.join(base, "mesh");
    const mesh = new MeshStore(meshRoot, 64 * 1024, 1_000, backend === "sqlite" ? { stateBackend: "sqlite" } : {});
    stores.push(mesh);
    const identity = identityOf("alpha");
    let own = recordOf(identity);
    const collisions: FabricRootCollision[] = [];
    const alpha = new ParticipantDirectory(mesh, { enabled: true, hostId: identity.id, rootId: identity.id, identity,
      reapDeadHosts: false, onRootCollision: collision => collisions.push(collision) });
    alpha.registerSource(() => [{ ...own, updatedAt: Date.now() }]);
    directories.push(alpha);
    await mesh.put({ key: LIVENESS_POLICY_KEY, identity, value: { version: 1, participants: "files" } });
    const dual = () => mesh.get(participantKey(identity.id), { fresh: true }) !== undefined;
    const lease = (peer: MeshIdentity, until: number) => writeHostLease(meshRoot, { id: peer.id, rootId: peer.id,
      identityId: peer.id, startedAt: 1, updatedAt: Date.now(), expiresAt: until });
    const host = (peer: MeshIdentity, until: number, modern = true) => ({
      format: 1, id: peer.id, rootId: peer.id, identity: peer, startedAt: 1,
      updatedAt: Date.now(), expiresAt: until, ...(modern ? { livenessLeaseFiles: 1 } : {}),
    });
    const putHost = (peer: MeshIdentity, until: number, modern = true) =>
      mesh.put({ key: keyFor("topology/hosts/", peer.id), identity: peer, value: host(peer, until, modern) });
    const file = (peer: MeshIdentity, value = recordOf(peer)) => writeParticipantFile(meshRoot, {
      key: participantKey(peer.id), value, version: 1, updatedAt: Date.now(), updatedBy: peer,
    });
    const warm = async () => { await alpha.start(); await alpha.refresh(); await alpha.refresh(); };
    return { mesh, meshRoot, identity, alpha, collisions, dual, lease, host, putHost, file, warm,
      rename: (name: string) => { own = { ...own, name }; } };
  };

  it("does no peer-lease enumeration or collision listing through 5s/10s heartbeats with 96 renewing modern peers", async () => {
    const f = await fixture();
    const peers = Array.from({ length: 96 }, (_, index) => identityOf(`peer-${index}`));
    for (const peer of peers) {
      await f.mesh.writeBatch({ identity: peer, ops: [
        { kind: "put", key: keyFor("topology/hosts/", peer.id), value: f.host(peer, Date.now() + 15_000) },
        { kind: "put", key: participantKey(peer.id), value: recordOf(peer) },
      ] });
      f.file(peer);
      f.lease(peer, Date.now() + 15_000);
    }
    await f.warm();
    expect(f.alpha.list({ scope: "project", kinds: ["root"], fresh: true })).toHaveLength(97);
    const listing = vi.spyOn(f.alpha, "list");
    const stats = vi.spyOn(fs, "statSync");
    const ownPath = hostLeasePath(f.meshRoot, f.identity.id);
    const peerStats = () => stats.mock.calls.filter(([file]) => {
      const name = String(file);
      return name !== ownPath && path.dirname(name) === path.dirname(ownPath) && name.endsWith(".json");
    }).length;
    for (let tick = 1; tick <= 2; tick += 1) {
      const before = readHostLeaseCurrent(f.meshRoot, f.identity.id)!;
      // Every foreign file changes, but no compatibility/collision candidate can be revived.
      for (const peer of peers) f.lease(peer, Date.now() + 20_000);
      await vi.advanceTimersByTimeAsync(5_000);
      await f.alpha.refresh(); // joins the real timer pass, including its confirmation receipt
      const after = readHostLeaseCurrent(f.meshRoot, f.identity.id)!;
      expect(after.incarnationToken).toBe(before.incarnationToken);
      expect(after.updatedAt).toBe(Date.now());
      expect(after.updatedAt - before.updatedAt).toBe(5_000);
      expect(after.expiresAt - after.updatedAt).toBe(15_000);
      expect(f.alpha.confirmedAt()).toBeGreaterThanOrEqual(after.updatedAt);
      expect(f.dual()).toBe(false);
    }
    expect(peerStats()).toBe(0);
    expect(listing).not.toHaveBeenCalled();
    // Retain the existing bounded 15s safety pass (not a new watcher or timer).
    await vi.advanceTimersByTimeAsync(5_000);
    await f.alpha.refresh();
    expect(listing).toHaveBeenCalledWith({ scope: "project", kinds: ["root"] });
    expect(peerStats()).toBeGreaterThanOrEqual(96);
  });

  it.each(["host", "state participant", "file participant"] as const)(
    "sees an old %s join, lapse and lease-only revival on the immediately requested pass", async source => {
      const f = await fixture();
      await f.warm();
      expect(f.dual()).toBe(false);
      const peer = identityOf("old-reader");
      const until = Date.now() + 2_000;
      await f.putHost(peer, until, source !== "host");
      f.lease(peer, until);
      if (source !== "host") {
        const { livenessLeaseFiles: _modern, ...old } = recordOf(peer);
        if (source === "file participant") f.file(peer, old);
        else await f.mesh.put({ key: participantKey(peer.id), identity: peer, value: old });
      }
      await f.alpha.refresh();
      expect(f.dual()).toBe(true);
      vi.setSystemTime(until + 1); // no state/file change: the true answer must expire on time
      await f.alpha.refresh();
      expect(f.dual()).toBe(false);
      const token = f.mesh.stateToken({ fresh: true });
      f.lease(peer, Date.now() + 2_000); // stale candidate revives without a structural event
      expect(f.mesh.stateToken({ fresh: true })).toBe(token);
      await f.alpha.refresh();
      expect(f.dual()).toBe(true);
    });

  it("sees an old session join/lapse/rejoin, without accepting an unadvertised session file renewal", async () => {
    const f = await fixture();
    await f.warm();
    const peer = identityOf("old-session");
    const value = { id: peer.id, sessionId: peer.sessionId, name: peer.name,
      cwd: process.cwd(), startedAt: 1, status: "idle" };
    const join = () => f.mesh.put({ key: "sessions/old-session", identity: peer, value });
    await join();
    await f.alpha.refresh();
    expect(f.dual()).toBe(true);
    vi.setSystemTime(Date.now() + 15_001);
    await f.alpha.refresh();
    expect(f.dual()).toBe(false);
    writeHostLease(f.meshRoot, { id: peer.id, rootId: peer.id, identityId: peer.id, startedAt: 1,
      updatedAt: Date.now(), expiresAt: Date.now() + 15_000,
      session: { id: "old-session", startedAt: 1, updatedAt: Date.now(), expiresAt: Date.now() + 15_000 } });
    await f.alpha.refresh();
    expect(f.dual()).toBe(false); // a pre-capability session has a fixed shared-state TTL
    await join();
    await f.alpha.refresh();
    expect(f.dual()).toBe(true);
  });

  it.each(["duplicate-session", "duplicate-name"] as const)(
    "does not skip a new %s when a stale modern root revives by lease alone", async reason => {
      const f = await fixture();
      const peer = identityOf("stale-root");
      const value = { ...recordOf(peer), ...(reason === "duplicate-session" ? { sessionId: "alpha" } : { name: "alpha" }) };
      await f.putHost(peer, Date.now() - 1);
      f.file(peer, value);
      f.lease(peer, Date.now() - 1);
      await f.warm();
      expect(f.collisions).toEqual([]);
      expect(f.alpha.get(peer.id, Date.now(), { fresh: true })).toBeUndefined();
      const token = f.mesh.stateToken({ fresh: true });
      f.lease(peer, Date.now() + 15_000);
      expect(f.mesh.stateToken({ fresh: true })).toBe(token);
      // Explicit authority/routing reads do not wait for the heartbeat/safety window either.
      await f.alpha.refreshRoutingView();
      expect(f.alpha.get(peer.id, Date.now(), { fresh: true })).toMatchObject({ id: peer.id, stale: false });
      expect(f.alpha.captureControlOwnerLease(peer.id, peer.id, peer.id)?.()).toBe(Date.now() + 15_000);
      await f.alpha.refresh();
      expect(f.collisions).toEqual([expect.objectContaining({ reason, ids: [f.identity.id, peer.id].sort() })]);
      await f.alpha.refresh();
      expect(f.collisions).toHaveLength(1);
    });

  it("detects file registration, peer rename and local root rename immediately, with deduplicated reports", async () => {
    const f = await fixture();
    await f.warm();
    const peer = identityOf("new-root");
    await f.putHost(peer, Date.now() + 15_000);
    f.lease(peer, Date.now() + 15_000);
    f.file(peer, { ...recordOf(peer), sessionId: "alpha" });
    await f.alpha.refresh();
    expect(f.collisions).toEqual([expect.objectContaining({ reason: "duplicate-session" })]);
    f.file(peer, { ...recordOf(peer), name: "alpha" });
    await f.alpha.refresh();
    expect(f.collisions).toHaveLength(2);
    expect(f.collisions[1]).toMatchObject({ reason: "duplicate-name", name: "alpha" });
    f.rename("new-root");
    f.file(peer);
    await f.alpha.refresh();
    expect(f.collisions).toHaveLength(3);
    expect(f.collisions[2]).toMatchObject({ reason: "duplicate-name", name: "new-root" });
    await f.alpha.refresh();
    expect(f.collisions).toHaveLength(3);
  });

  it("retains lease checks for an incompatible stale file shadowed by a modern state record", async () => {
    const f = await fixture();
    const peer = identityOf("shadowed-old");
    await f.putHost(peer, Date.now() - 1);
    const { livenessLeaseFiles: _modern, ...old } = recordOf(peer);
    f.file(peer, old);
    vi.setSystemTime(Date.now() + 1);
    await f.mesh.put({ key: participantKey(peer.id), identity: peer, value: recordOf(peer) });
    await f.warm();
    expect(f.dual()).toBe(false);
    const stats = vi.spyOn(fs, "statSync");
    f.lease(peer, Date.now() + 15_000);
    await f.alpha.refresh();
    expect(stats.mock.calls.some(([file]) => String(file) === hostLeasePath(f.meshRoot, peer.id))).toBe(true);
    expect(f.dual()).toBe(false); // selected modern record still wins; the old file is not erased
  });
});
