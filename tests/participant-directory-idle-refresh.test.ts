import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { SqliteStateStore } from "../src/mesh/state-sqlite.js";
import { LIVENESS_POLICY_KEY, writeHostLease } from "../src/topology/host-leases.js";
import { ParticipantDirectory, type FabricRootCollision } from "../src/topology/participant-directory.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";

// smarty-dev#7978: an idle Main's heartbeat re-derived its legacy-renewal decision (three times a pass)
// and its root-collision report from a full read of every host, session and participant entry, even
// when nothing changed. These tests keep each guarantee those reads gave, with the reads gone.
const roots: string[] = [];
const directories: ParticipantDirectory[] = [];
const stores: MeshStore[] = [];
afterEach(async () => {
  vi.restoreAllMocks();
  await Promise.all(directories.splice(0).map((directory) => directory.close()));
  // Release each state.db handle after its directory stops and before its root goes: Windows refuses
  // to delete an open SQLite file (EPERM).
  for (const store of stores.splice(0)) try { store.closeState(); } catch { /* closed by the test */ }
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const sha = (text: string): string => createHash("sha256").update(text).digest("hex");
const identityOf = (name: string): MeshIdentity => ({ id: `session:${name}`, name: "main", kind: "main", sessionId: name });
const rootRecord = (identity: MeshIdentity, sessionId: string): FabricParticipantRecord => ({
  format: 1, id: identity.id, kind: "root", rootId: identity.id, ownerHostId: identity.id, ownerIdentityId: identity.id,
  name: "main", status: "idle", runner: "pi", transport: "host", capabilities: ["steer", "followUp", "fabric"],
  cwd: "/tmp/project", sessionId, startedAt: 1, updatedAt: Date.now(), pendingMessages: false, controlProtocol: "v1",
});
// A slow heartbeat and a long lease: every pass below is an explicit refresh(), nothing lapses by accident.
const LEASE = { heartbeatMs: 60_000, leaseMs: 120_000 };

describe.each(["file", "sqlite"] as const)("ParticipantDirectory idle heartbeat reads (%s state, smarty-dev#7978)", (backend) => {
  const setup = () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-idle-refresh-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const store = (): MeshStore => {
      const created = new MeshStore(meshRoot, 64 * 1024, 1_000, backend === "sqlite" ? { stateBackend: "sqlite" } : {});
      stores.push(created);
      return created;
    };
    const collisions: FabricRootCollision[] = [];
    const directory = (name: string, sessionId = name, onRootCollision?: (collision: FabricRootCollision) => void) => {
      const identity = identityOf(name);
      const created = new ParticipantDirectory(store(), { enabled: true, hostId: identity.id, rootId: identity.id, identity,
        reapDeadHosts: false, ...LEASE, ...(onRootCollision ? { onRootCollision } : {}) });
      created.registerSource(() => [rootRecord(identity, sessionId)]);
      directories.push(created);
      return created;
    };
    const alpha = directory("alpha", "alpha", (collision) => collisions.push(collision));
    return { meshRoot, alpha, directory, collisions, mesh: alpha.mesh };
  };
  // The reads an idle pass used to make, by entry point: copies (listAll), shared views (listAllShared)
  // of the host prefix (only the full derivations read it), and the raw SQLite scan.
  const spyReads = (mesh: MeshStore) => {
    const copies = vi.spyOn(mesh, "listAll");
    const shared = vi.spyOn(mesh, "listAllShared");
    const scans = vi.spyOn(SqliteStateStore.prototype, "listAll");
    const directoryPrefixes = ["topology/hosts/", "topology/participants/", "sessions/"];
    return {
      copies: () => copies.mock.calls.filter(([prefix]) => directoryPrefixes.includes(prefix!)).length,
      hostScans: () => shared.mock.calls.filter(([prefix]) => prefix === "topology/hosts/").length,
      sqliteScans: () => scans.mock.calls.length,
      reset: () => { copies.mockClear(); shared.mockClear(); scans.mockClear(); },
    };
  };

  it("does no full read on a heartbeat over an unchanged store, and one per lease period as the safety net", async () => {
    const { alpha, directory, mesh } = setup();
    await directory("beta").start();
    await directory("gamma").start();
    await alpha.start();
    await alpha.refresh();
    const reads = spyReads(mesh);
    const listings = vi.spyOn(alpha, "list");
    await alpha.refresh();
    await alpha.refresh();
    expect(listings).not.toHaveBeenCalled();        // no root-collision listing either
    expect(reads.copies()).toBe(0);                 // no JSON copy of any directory entry
    expect(reads.hostScans()).toBe(0);              // neither derivation re-read the fleet
    expect(reads.sqliteScans()).toBe(0);            // and nothing scanned state.db
    expect(alpha.list({ scope: "project", kinds: ["root"] }).map((peer) => peer.id).sort())
      .toEqual(["session:alpha", "session:beta", "session:gamma"]);
    // Without any change event, a lease period later both derivations read the fleet again.
    const real = Date.now;
    const clock = vi.spyOn(Date, "now").mockImplementation(() => real() + LEASE.leaseMs);
    reads.reset();
    await alpha.refresh();
    expect(reads.hostScans()).toBeGreaterThanOrEqual(1);   // the legacy-renewal derivation
    expect(listings).toHaveBeenCalledWith({ scope: "project", kinds: ["root"] });   // the collision pass
    expect(reads.copies()).toBe(0);
    clock.mockRestore();
  });

  // Astra P2 on fd31b1c1: the state token moves on ANY commit, and a fleet commits an unrelated key every second.
  it("keeps both derived answers across unrelated state commits, and re-derives on a participant change within one pass", async () => {
    const { alpha, directory, mesh, collisions } = setup();
    await directory("beta").start();
    await directory("gamma").start();
    await alpha.start();
    await alpha.refresh();
    const churn = identityOf("churn");
    const reads = spyReads(mesh);
    const listings = vi.spyOn(alpha, "list");
    const shared = vi.spyOn(mesh, "listAllShared");
    for (let tick = 1; tick <= 3; tick += 1) {
      const before = mesh.stateToken();
      await mesh.put({ key: "bench/churn", value: { tick }, identity: churn });
      expect(mesh.stateToken({ fresh: true })).not.toBe(before);   // the store's change stamp moved
      await alpha.refresh();
    }
    expect(listings).not.toHaveBeenCalled();        // no root-collision pass
    expect(reads.copies()).toBe(0);                 // no JSON copy of any directory entry
    expect(reads.sqliteScans()).toBe(0);            // nothing scanned state.db
    // Per pass and derivation, only the change check: ONE uncopied selection of the host namespace (only
    // the derivations read it); a full legacy-renewal scan, or list(), would select it again.
    expect(shared.mock.calls.filter(([prefix]) => prefix === "topology/hosts/").length).toBe(3 * 2);
    // A participant change amid the churn: the collision pass runs within one pass and reports it.
    await directory("delta", "alpha").start();
    await mesh.put({ key: "bench/churn", value: { tick: 4 }, identity: churn });
    await alpha.refresh();
    expect(listings).toHaveBeenCalledWith({ scope: "project", kinds: ["root"] });
    expect(collisions).toEqual([expect.objectContaining({ reason: "duplicate-session", ids: ["session:alpha", "session:delta"] })]);
  });

  it("reports a root collision, once, and a new colliding participant within one pass", async () => {
    const { alpha, directory, collisions } = setup();
    await directory("beta", "alpha").start();               // the same Pi session as alpha: a forked fixture
    await alpha.start();
    expect(collisions).toEqual([expect.objectContaining({ reason: "duplicate-session", ids: ["session:alpha", "session:beta"] })]);
    await alpha.refresh();
    await alpha.refresh();
    expect(collisions).toHaveLength(1);                      // reported once, not every heartbeat
    await directory("gamma", "alpha").start();               // a participant change
    await alpha.refresh();                                   // seen within one pass
    expect(collisions).toHaveLength(2);
    expect(collisions[1]).toMatchObject({ reason: "duplicate-session", ids: ["session:alpha", "session:gamma"] });
  });

  it("re-derives the legacy-renewal answer within one pass on a legacy-session, host or participant change amid churn", async () => {
    const { alpha, mesh } = setup();
    const policy = identityOf("policy");
    const churn = identityOf("churn");
    await mesh.put({ key: LIVENESS_POLICY_KEY, value: { version: 1, participants: "files" }, identity: policy });
    const alphaKey = "topology/participants/" + sha("session:alpha");
    const dual = () => mesh.get(alphaKey, { fresh: true }) !== undefined;
    let tick = 0;
    const pass = async () => {
      await mesh.put({ key: "bench/churn", value: { tick: tick += 1 }, identity: churn });
      await alpha.refresh();
      return dual();
    };
    await alpha.start();
    expect(await pass()).toBe(false);                      // files only: no live old reader
    expect(await pass()).toBe(false);                      // unrelated churn changes nothing
    // A legacy session (a pre-directory Main's advertisement) appears: dual publication at once.
    const old = identityOf("old-session");
    await mesh.put({ key: "sessions/old-session", identity: old,
      value: { id: old.id, sessionId: "old-session", cwd: "/tmp/project", startedAt: 1, status: "idle" } });
    expect(await pass()).toBe(true);
    await mesh.delete({ key: "sessions/old-session" });
    expect(await pass()).toBe(false);                      // and back to files only when it goes
    // A live pre-capability host record appears (no livenessLeaseFiles).
    const host = identityOf("old-host");
    const hostValue = (files?: 1) => ({ format: 1, id: host.id, rootId: host.id, identity: host, startedAt: 1,
      updatedAt: Date.now(), expiresAt: Date.now() + 60_000, ...(files ? { livenessLeaseFiles: files } : {}) });
    await mesh.put({ key: "topology/hosts/" + sha(host.id), identity: host, value: hostValue() });
    expect(await pass()).toBe(true);
    await mesh.put({ key: "topology/hosts/" + sha(host.id), identity: host, value: hostValue(1) });
    expect(await pass()).toBe(false);                      // the host upgraded: files only again
    // A participant of that live host without livenessLeaseFiles: an old reader again.
    await mesh.put({ key: "topology/participants/" + sha(host.id), identity: host, value: rootRecord(host, "old-host") });
    expect(await pass()).toBe(true);
  });

  it("selects newly referenced hosts from a changed snapshot, with identity and incarnation checked on every lease change", async () => {
    const { alpha, mesh, meshRoot, collisions } = setup();
    await mesh.put({ key: LIVENESS_POLICY_KEY, value: { version: 1, participants: "files" }, identity: identityOf("policy") });
    await alpha.start();
    await alpha.refresh(); // Both derivations already have a cached answer without this host.
    const peer = identityOf("new-file-owner");
    const now = Date.now();
    await mesh.put({ key: "topology/hosts/" + sha(peer.id), identity: peer, value: {
      format: 1, id: peer.id, rootId: peer.id, identity: peer, startedAt: 1, incarnationToken: "current",
      updatedAt: now - 60_000, expiresAt: now - 30_000, livenessLeaseFiles: 1,
    } });
    await mesh.put({ key: "topology/participants/" + sha(peer.id), identity: peer,
      value: { ...rootRecord(peer, "alpha"), ownerIncarnation: "current" } });
    const lease = { id: peer.id, rootId: peer.id, identityId: peer.id, startedAt: 1,
      incarnationToken: "current", updatedAt: now, expiresAt: now + 60_000 };
    const alphaKey = "topology/participants/" + sha("session:alpha");
    for (const mismatch of [{ rootId: "wrong-root" }, { identityId: "wrong-identity" }, { incarnationToken: "predecessor" }]) {
      writeHostLease(meshRoot, { ...lease, ...mismatch });
      await alpha.refresh();
      expect(mesh.get(alphaKey, { fresh: true })).toBeUndefined();
      expect(alpha.list({ scope: "project" }).some(row => row.id === peer.id)).toBe(false);
      expect(collisions).toEqual([]);
    }
    // No state change: only the exact known host's file becomes authoritative.
    writeHostLease(meshRoot, lease);
    await alpha.refresh();
    expect(mesh.get(alphaKey, { fresh: true })).toBeDefined(); // pre-capability participant needs dual publication
    expect(alpha.list({ scope: "project" }).some(row => row.id === peer.id)).toBe(true);
    expect(collisions).toEqual([expect.objectContaining({ reason: "duplicate-session", ids: ["session:alpha", peer.id] })]);
  });

  it("detects a session-only collision when an explicitly named lease-only host changes without a state commit", async () => {
    const { alpha, mesh, meshRoot, collisions } = setup();
    await mesh.put({ key: LIVENESS_POLICY_KEY, value: { version: 1, participants: "files" }, identity: identityOf("policy") });
    await alpha.start();
    const old = identityOf("session-only");
    const now = Date.now();
    await mesh.put({ key: "sessions/alpha", identity: old, value: {
      id: old.id, sessionId: "alpha", cwd: "/tmp/project", startedAt: 1, status: "idle",
      livenessLeaseFiles: 1, livenessHostId: "arbitrary-lease-only-host", livenessIncarnationToken: "session-token",
    } });
    // Age only this immutable advertisement; its lease has not been created yet.
    const real = Date.now;
    const clock = vi.spyOn(Date, "now").mockImplementation(() => real() + 20_000);
    try {
      await alpha.refresh();
      expect(collisions).toEqual([]);
      expect(mesh.listAll("topology/hosts/").some(entry => (entry.value as { id?: string }).id === "arbitrary-lease-only-host")).toBe(false);
      const before = mesh.stateToken({ fresh: true });
      writeHostLease(meshRoot, { id: "arbitrary-lease-only-host", rootId: old.id, identityId: old.id,
        incarnationToken: "session-token", updatedAt: now + 20_000, expiresAt: now + 60_000,
        session: { id: "alpha", startedAt: 1, updatedAt: now + 20_000, expiresAt: now + 60_000 } });
      expect(mesh.stateToken({ fresh: true })).toBe(before);
      expect(alpha.list({ scope: "project", kinds: ["root"] }).some(row => row.id === old.id)).toBe(true);
      await alpha.refresh();
      expect(collisions).toEqual([expect.objectContaining({ reason: "duplicate-session", ids: ["session:alpha", old.id] })]);
    } finally { clock.mockRestore(); }
  });

  it("sees a changed lease within one pass, and lets the old reader's expiry take effect on time without a change", async () => {
    const { alpha, mesh, meshRoot } = setup();
    const policy = identityOf("policy");
    await mesh.put({ key: LIVENESS_POLICY_KEY, value: { version: 1, participants: "files" }, identity: policy });
    // A pre-capability peer (no livenessLeaseFiles): its shared host record has lapsed, so it is not live.
    const old = identityOf("old-reader");
    const start = Date.now();
    await mesh.put({ key: "topology/hosts/" + sha(old.id), identity: old,
      value: { format: 1, id: old.id, rootId: old.id, identity: old, startedAt: 1, updatedAt: start - 60_000, expiresAt: start - 30_000 } });
    const alphaKey = "topology/participants/" + sha("session:alpha");
    await alpha.start();
    await alpha.refresh();
    expect(mesh.get(alphaKey, { fresh: true })).toBeUndefined();   // files only: no live old reader
    // A lease change alone (no state commit) makes the old reader live: dual publication resumes at once.
    const until = Date.now() + 20_000;
    writeHostLease(meshRoot, { id: old.id, rootId: old.id, identityId: old.id, startedAt: 1, updatedAt: Date.now(), expiresAt: until });
    await alpha.refresh();
    expect(mesh.get(alphaKey, { fresh: true })?.value).toMatchObject({ id: "session:alpha" });
    const real = Date.now;
    const clock = vi.spyOn(Date, "now");
    try {
      clock.mockImplementation(() => until - 1);          // still live: the shared copy stays
      await alpha.refresh();
      expect(mesh.get(alphaKey, { fresh: true })).toBeDefined();
      // The old reader lapses with no change event at all, well inside the safety interval:
      // the next pass returns to files-only and removes the shared copy.
      clock.mockImplementation(() => until + 1);
      await alpha.refresh();
      expect(mesh.get(alphaKey, { fresh: true })).toBeUndefined();
      expect(real() - start).toBeLessThan(LEASE.leaseMs);
    } finally { clock.mockRestore(); }
  });
});
