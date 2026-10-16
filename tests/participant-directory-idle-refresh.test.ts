import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { SqliteStateStore } from "../src/mesh/state-sqlite.js";
import { hostLeasePath, LIVENESS_POLICY_KEY, writeHostLease } from "../src/topology/host-leases.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";
import { ParticipantDirectory, type FabricRootCollision } from "../src/topology/participant-directory.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";

// Keep reason: smarty-dev#7978 idle heartbeat regression and legacy-reader migration safety.
// Shared selections remain uncopied; legacy lease authority is point-read on every decision,
// while root-collision observations still reuse unchanged peer signals.
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
  const setup = (kind: "root" | "agent" = "root") => {
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
      created.registerSource(() => [{ ...rootRecord(identity, sessionId), kind }]);
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

  it("keeps heartbeat selections uncopied and only repeats the collision listing once per lease period", async () => {
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
    expect(reads.hostScans()).toBeGreaterThan(0);   // strict legacy decisions select potential owners
    expect(reads.sqliteScans()).toBe(0);            // and nothing scanned state.db
    expect(alpha.list({ scope: "project", kinds: ["root"] }).map((peer) => peer.id).sort())
      .toEqual(["session:alpha", "session:beta", "session:gamma"]);
    // Without any change event, a lease period later the collision observation is recomputed.
    const real = Date.now;
    const clock = vi.spyOn(Date, "now").mockImplementation(() => real() + LEASE.leaseMs);
    reads.reset();
    await alpha.refresh();
    expect(reads.hostScans()).toBeGreaterThanOrEqual(1);
    expect(listings).toHaveBeenCalledWith({ scope: "project", kinds: ["root"] });   // the collision pass
    expect(reads.copies()).toBe(0);
    clock.mockRestore();
  });

  // Astra P2 on fd31b1c1: the state token moves on ANY commit, and a fleet commits an unrelated key every second.
  it("keeps the collision observation across unrelated commits, and sees a participant change within one pass", async () => {
    const { alpha, directory, mesh, collisions } = setup();
    await directory("beta").start();
    await directory("gamma").start();
    await alpha.start();
    await alpha.refresh();
    const churn = identityOf("churn");
    const reads = spyReads(mesh);
    const listings = vi.spyOn(alpha, "list");
    for (let tick = 1; tick <= 3; tick += 1) {
      const before = mesh.stateToken();
      await mesh.put({ key: "bench/churn", value: { tick }, identity: churn });
      expect(mesh.stateToken({ fresh: true })).not.toBe(before);   // the store's change stamp moved
      await alpha.refresh();
    }
    expect(listings).not.toHaveBeenCalled();        // no root-collision pass
    expect(reads.copies()).toBe(0);                 // no JSON copy of any directory entry
    expect(reads.sqliteScans()).toBe(0);            // nothing scanned state.db
    expect(reads.hostScans()).toBeGreaterThan(0);   // legacy owners are selected without a memo
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

  it("does not enumerate or bulk-stat a large stale lease directory for an idle legacy decision", async () => {
    // No root candidate: root-collision's intentionally unchanged full-map read is outside this slice.
    const { alpha, mesh, meshRoot } = setup("agent");
    await mesh.put({ key: LIVENESS_POLICY_KEY, value: { version: 1, participants: "files" }, identity: identityOf("policy") });
    const staleFiles = new Set<string>();
    for (let index = 0; index < 512; index += 1) {
      const id = `session:stale-${index}`;
      writeHostLease(meshRoot, { id, rootId: id, identityId: id, startedAt: 1, updatedAt: 1, expiresAt: 2 });
      staleFiles.add(hostLeasePath(meshRoot, id));
    }
    await alpha.refresh();
    const modern = identityOf("stale-0");
    await mesh.put({ key: "topology/hosts/" + sha(modern.id), identity: modern, value: {
      format: 1, id: modern.id, rootId: modern.id, identity: modern, startedAt: 1,
      updatedAt: Date.now(), expiresAt: Date.now() + LEASE.leaseMs, livenessLeaseFiles: 1,
    } });
    await mesh.put({ key: "topology/participants/" + sha(modern.id), identity: modern,
      value: { ...rootRecord(modern, "stale-0"), livenessLeaseFiles: 1 } });
    await alpha.refresh();
    const stats = vi.spyOn(fs, "statSync");
    const listings = vi.spyOn(fs, "readdirSync");
    await alpha.refresh();
    await alpha.refresh();
    expect(stats.mock.calls.filter(([file]) => staleFiles.has(String(file)))).toEqual([]);
    expect(listings.mock.calls.filter(([dir]) => String(dir) === path.join(meshRoot, "host-leases"))).toEqual([]);
    expect(mesh.get("topology/participants/" + sha("session:alpha"), { fresh: true })).toBeUndefined();
  });

  it.each(["host", "shared participant", "file participant"] as const)(
    "strictly renews/vetoes a potential legacy %s owner, including an expired shared host", async (source) => {
      const { alpha, mesh, meshRoot } = setup("agent");
      const old = identityOf("old-owner");
      const now = Date.now();
      await mesh.put({ key: LIVENESS_POLICY_KEY, value: { version: 1, participants: "files" }, identity: identityOf("policy") });
      await mesh.put({ key: "topology/hosts/" + sha(old.id), identity: old, value: {
        format: 1, id: old.id, rootId: old.id, identity: old, startedAt: 1, incarnationToken: "current-owner",
        updatedAt: now - 60_000, expiresAt: now - 30_000, ...(source === "host" ? {} : { livenessLeaseFiles: 1 }),
      } });
      if (source !== "host") {
        const entry = { key: "topology/participants/" + sha(old.id), value: rootRecord(old, "old-owner"),
          version: 1, updatedAt: now, updatedBy: old };
        if (source === "file participant") writeParticipantFile(meshRoot, entry);
        else await mesh.put({ key: entry.key, value: entry.value, identity: old });
      }
      const key = "topology/participants/" + sha("session:alpha");
      const pass = async () => { await alpha.refresh(); return mesh.get(key, { fresh: true }) !== undefined; };
      expect(await pass()).toBe(false);
      const lease = { id: old.id, rootId: old.id, identityId: old.id, startedAt: 1,
        incarnationToken: "current-owner", updatedAt: now, expiresAt: now + LEASE.leaseMs };
      writeHostLease(meshRoot, lease);
      expect(await pass()).toBe(true);
      const file = hostLeasePath(meshRoot, old.id);
      let rewrite = 0;
      const replaceInPlace = (patch: Record<string, unknown> | null) => {
        fs.writeFileSync(file, patch === null ? "invalid lease" : JSON.stringify({ format: 1, ...lease, ...patch }));
        const stamp = new Date(now + ++rewrite * 1_000);
        fs.utimesSync(file, stamp, stamp);
      };
      for (const patch of [
        { expiresAt: now - 1 }, { identityId: "wrong-identity" }, { identityId: undefined },
        { rootId: "wrong-root" }, { incarnationToken: "previous-owner" }, { incarnationToken: undefined }, null,
      ]) {
        replaceInPlace(patch);
        expect(await pass(), JSON.stringify(patch)).toBe(false);
        replaceInPlace({});
        expect(await pass(), "matching in-place renewal").toBe(true);
      }
      fs.renameSync(file, `${file}.test-retained-missing`); // absence without deleting fixture bytes
      expect(await pass()).toBe(false);
    },
  );

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
