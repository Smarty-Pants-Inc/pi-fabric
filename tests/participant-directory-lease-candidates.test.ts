import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshIdentity, type MeshStateEntry } from "../src/mesh/store.js";
import { hostLeasePath, LIVENESS_POLICY_KEY, writeHostLease, type FabricHostLease } from "../src/topology/host-leases.js";
import { MIRROR_COLLISION_TOPIC, ParticipantDirectory, type FabricRootCollision } from "../src/topology/participant-directory.js";
import { readParticipantFile, writeParticipantFile } from "../src/topology/participant-files.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";

// #17515: real refreshes over retained fleet state. Keep reason: the synchronous in-place
// renewal and post-confirmation races cannot be arranged cheaply through an interactive Main.
const roots: string[] = [];
const stores: MeshStore[] = [];
const directories: ParticipantDirectory[] = [];
afterEach(async () => {
  vi.restoreAllMocks();
  await Promise.all(directories.splice(0).map(directory => directory.close()));
  for (const mesh of stores.splice(0)) mesh.closeState();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const sha = (id: string): string => createHash("sha256").update(id).digest("hex");
const hostKey = (id: string): string => "topology/hosts/" + sha(id);
const participantKey = (id: string): string => "topology/participants/" + sha(id);
const identity = (id: string): MeshIdentity => ({ id, name: "main", kind: "main", sessionId: id });
const record = (owner: MeshIdentity, patch: Partial<FabricParticipantRecord> = {}): FabricParticipantRecord => ({
  format: 1, id: owner.id, kind: "root", rootId: owner.id, ownerHostId: owner.id, ownerIdentityId: owner.id,
  name: "main", sessionId: owner.id, cwd: "/fixture/project", status: "idle", runner: "pi", transport: "host",
  capabilities: ["steer", "followUp", "fabric"], controlProtocol: "v1", startedAt: 1, updatedAt: Date.now(),
  livenessLeaseFiles: 1, ...patch,
});

const setup = async (stateBackend: "file" | "sqlite", filesOnly = true) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "participant-lease-candidates-")); roots.push(root);
  const meshRoot = path.join(root, "mesh");
  const mesh = new MeshStore(meshRoot, 64 * 1024, 100, { stateBackend }); stores.push(mesh);
  const self = identity("session:self");
  const collisions: FabricRootCollision[] = [];
  const directory = new ParticipantDirectory(mesh, { enabled: true, hostId: self.id, rootId: self.id, identity: self,
    reapDeadHosts: false, heartbeatMs: 60_000, leaseMs: 120_000, onRootCollision: collision => collisions.push(collision) });
  directories.push(directory);
  directory.registerSource(() => [record(self, { name: "shared-name", sessionId: "shared-session" })]);
  if (filesOnly) await mesh.put({ key: LIVENESS_POLICY_KEY, value: { version: 1, participants: "files" }, identity: self });
  const putHost = (owner: MeshIdentity, patch: Record<string, unknown> = {}) => mesh.put({ key: hostKey(owner.id), identity: owner,
    value: { format: 1, id: owner.id, rootId: owner.id, identity: owner, startedAt: 1,
      updatedAt: Date.now() - 60_000, expiresAt: Date.now() - 30_000, livenessLeaseFiles: 1, ...patch } });
  const putParticipant = (owner: MeshIdentity, patch: Partial<FabricParticipantRecord> = {}) =>
    mesh.put({ key: participantKey(patch.id ?? owner.id), identity: owner, value: record(owner, patch) });
  const lease = (owner: MeshIdentity, patch: Partial<FabricHostLease> = {}): FabricHostLease => ({ id: owner.id, rootId: owner.id,
    identityId: owner.id, startedAt: 1, updatedAt: Date.now(), expiresAt: Date.now() + 60_000, ...patch });
  const dual = () => mesh.get(participantKey(self.id), { fresh: true }) !== undefined;
  // Both canonical modern peers and orphaned/dead retained files must be irrelevant. Session
  // metadata also advertises lease support: checking its liveness first would point-read every file.
  const retained = async (count = 160) => {
    const irrelevant = new Set<string>();
    const ops: Array<{ kind: "put"; key: string; value: unknown }> = [];
    for (let index = 0; index < count; index++) {
      const owner = identity(`session:irrelevant-${index}`);
      irrelevant.add(hostLeasePath(meshRoot, owner.id));
      writeHostLease(meshRoot, lease(owner));
      ops.push({ kind: "put" as const, key: hostKey(owner.id), value: { format: 1, id: owner.id, rootId: owner.id, identity: owner,
        startedAt: 1, updatedAt: Date.now(), expiresAt: Date.now() + 60_000, livenessLeaseFiles: 1 } });
      ops.push({ kind: "put" as const, key: participantKey(owner.id), value: record(owner) });
      ops.push({ kind: "put" as const, key: `sessions/${owner.id}`, value: { id: owner.id, sessionId: owner.id, cwd: "/fixture/project",
        startedAt: 1, status: "idle", livenessLeaseFiles: 1 } });
      // Each entry must keep its own writer attribution; this batch is one writer per peer.
      await mesh.writeBatch({ identity: owner, ops: ops.splice(0) });
      const orphan = identity(`orphan:${index}`);
      irrelevant.add(hostLeasePath(meshRoot, orphan.id));
      writeHostLease(meshRoot, lease(orphan, { expiresAt: Date.now() - 1 }));
    }
    return irrelevant;
  };
  return { meshRoot, mesh, self, directory, collisions, putHost, putParticipant, lease, dual, retained };
};

// Observe actual filesystem work, not just JSON parses: a warm parsed cache still stats each
// irrelevant file in readHostLeases(). All probes are reset for the unchanged heartbeat phase.
const probe = (meshRoot: string, irrelevant: ReadonlySet<string>) => {
  const stats = vi.spyOn(fs, "statSync");
  const lstats = vi.spyOn(fs, "lstatSync");
  const reads = vi.spyOn(fs, "readFileSync");
  const lists = vi.spyOn(fs, "readdirSync");
  const calls = () => [...stats.mock.calls, ...lstats.mock.calls, ...reads.mock.calls].map(([file]) => String(file));
  return {
    untouched: () => expect(calls().filter(file => irrelevant.has(file)).length).toBe(0),
    noListing: () => expect(lists.mock.calls.filter(([file]) => String(file) === path.join(meshRoot, "host-leases"))).toEqual([]),
    statsFor: (id: string) => stats.mock.calls.filter(([file]) => String(file) === hostLeasePath(meshRoot, id)).length,
    reset: () => { stats.mockClear(); lstats.mockClear(); reads.mockClear(); lists.mockClear(); },
  };
};

describe.each(["file", "sqlite"] as const)("participant candidate lease reads (%s, #17515)", backend => {
  it("does not touch irrelevant retained leases on cold publication or unchanged own atomic heartbeats", async () => {
    const f = await setup(backend);
    const irrelevant = await f.retained();
    const io = probe(f.meshRoot, irrelevant);
    const listings = vi.spyOn(f.directory, "list");
    await f.directory.start();
    expect(f.dual()).toBe(false);
    io.untouched(); io.noListing();
    expect(listings).not.toHaveBeenCalled();
    io.reset();
    await f.directory.refresh();
    await f.directory.refresh();
    io.untouched(); io.noListing();
    expect(listings).not.toHaveBeenCalled();
  });

  it("freshly sees an in-place old-owner revival before a watch callback, expiry, and capability upgrade", async () => {
    const f = await setup(backend);
    const irrelevant = await f.retained(12);
    const old = identity("old-reader");
    await f.putHost(old, { livenessLeaseFiles: undefined, incarnationToken: "old-incarnation" });
    writeHostLease(f.meshRoot, f.lease(old, { incarnationToken: "wrong-incarnation" }));
    await f.directory.start();
    expect(f.dual()).toBe(false); // a different incarnation cannot revive this old host
    const io = probe(f.meshRoot, irrelevant);
    const file = hostLeasePath(f.meshRoot, old.id);
    const stamp = fs.statSync(path.dirname(file)).mtimeMs;
    const revived = f.lease(old, { incarnationToken: "old-incarnation", expiresAt: Date.now() + 20_000 });
    fs.writeFileSync(file, JSON.stringify({ format: 1, ...revived }));
    expect(fs.statSync(path.dirname(file)).mtimeMs).toBe(stamp);
    // refresh() begins synchronously, with no watch delivery / tick between rewrite and call.
    await f.directory.refresh();
    expect(f.dual()).toBe(true);
    io.untouched(); io.noListing();
    expect(io.statsFor(old.id)).toBeGreaterThan(0);
    expect(io.statsFor(old.id)).toBeLessThanOrEqual(6); // distinct owner, not every retained file
    const real = Date.now;
    vi.spyOn(Date, "now").mockImplementation(() => revived.expiresAt + 1);
    await f.directory.refresh();
    expect(f.dual()).toBe(false);
    fs.writeFileSync(file, JSON.stringify({ format: 1, ...revived, updatedAt: revived.expiresAt + 2, expiresAt: revived.expiresAt + 50_000 }));
    await f.directory.refresh();
    expect(f.dual()).toBe(true); // a memoized negative cannot hide a second in-place revival
    vi.mocked(Date.now).mockRestore();
    await f.putHost(old, { livenessLeaseFiles: 1, incarnationToken: "old-incarnation", expiresAt: real() + 60_000 });
    io.reset();
    await f.directory.refresh();
    expect(f.dual()).toBe(false);
    expect(io.statsFor(old.id)).toBe(0); // upgraded, and no collision candidate
    io.untouched();
  });

  it("selects compatibility participants only after file/state precedence, while preserving native over mirror", async () => {
    const f = await setup(backend);
    const peer = identity("participant-reader");
    await f.putHost(peer);
    writeHostLease(f.meshRoot, f.lease(peer));
    const shared = await f.putParticipant(peer); // capable host and capable participant
    await f.directory.start();
    expect(f.dual()).toBe(false);
    const entry = (updatedAt: number, legacy = false): MeshStateEntry => {
      const value = record(peer);
      if (legacy) delete value.livenessLeaseFiles;
      return { key: participantKey(peer.id), version: 1, updatedBy: peer, updatedAt, value };
    };
    writeParticipantFile(f.meshRoot, entry(shared.updatedAt - 1, true));
    await f.directory.refresh();
    expect(f.dual()).toBe(false); // later state beats old legacy file
    writeParticipantFile(f.meshRoot, entry(shared.updatedAt, true));
    await f.directory.refresh();
    expect(f.dual()).toBe(true); // tie: file wins
    await f.putParticipant(peer, { remoteHost: "forge" });
    await f.directory.refresh();
    expect(f.dual()).toBe(true); // mirrored state never outranks native file
    writeParticipantFile(f.meshRoot, entry(Date.now() + 1));
    await f.directory.refresh();
    expect(f.dual()).toBe(false);
  });

  it("reports same-name and session roots without touching unrelated owner leases or materializing list()", async () => {
    const f = await setup(backend);
    const irrelevant = await f.retained(24);
    const named = identity("named-peer");
    const session = identity("session-peer");
    for (const peer of [named, session]) { await f.putHost(peer); writeHostLease(f.meshRoot, f.lease(peer)); }
    await f.putParticipant(named, { name: "shared-name" });
    await f.putParticipant(session, { sessionId: "shared-session" });
    const io = probe(f.meshRoot, irrelevant);
    const listings = vi.spyOn(f.directory, "list");
    await f.directory.start();
    expect(f.collisions).toEqual(expect.arrayContaining([
      expect.objectContaining({ reason: "duplicate-name", ids: [named.id, f.self.id].sort() }),
      expect.objectContaining({ reason: "duplicate-session", ids: [session.id, f.self.id].sort() }),
    ]));
    expect(f.collisions).toHaveLength(2);
    io.untouched(); io.noListing();
    expect(listings).not.toHaveBeenCalled();
    io.reset();
    await f.directory.refresh();
    expect(f.collisions).toHaveLength(2);
    io.untouched(); io.noListing();
    expect(io.statsFor(named.id)).toBe(1);
    expect(io.statsFor(session.id)).toBe(1);
  });

  it("freshly detects an in-place collision-owner revival without reading unrelated owners", async () => {
    const f = await setup(backend);
    const irrelevant = await f.retained(12);
    const peer = identity("reviving-collision");
    await f.putHost(peer, { incarnationToken: "current" });
    await f.putParticipant(peer, { name: "shared-name" });
    writeHostLease(f.meshRoot, f.lease(peer, { incarnationToken: "predecessor" }));
    await f.directory.start();
    await f.directory.refresh();
    expect(f.collisions).toEqual([]);
    const io = probe(f.meshRoot, irrelevant);
    const file = hostLeasePath(f.meshRoot, peer.id);
    const stamp = fs.statSync(path.dirname(file)).mtimeMs;
    fs.writeFileSync(file, JSON.stringify({ format: 1, ...f.lease(peer, { incarnationToken: "current" }) }));
    expect(fs.statSync(path.dirname(file)).mtimeMs).toBe(stamp);
    await f.directory.refresh();
    expect(f.collisions).toEqual([expect.objectContaining({ reason: "duplicate-name", ids: [peer.id, f.self.id].sort() })]);
    io.untouched(); io.noListing();
    expect(io.statsFor(peer.id)).toBe(1);
  });

  it("suppresses same-id legacy collisions with an accepted nonmatching native root, but not an expired native", async () => {
    const f = await setup(backend);
    const peer = identity("native-peer");
    await f.putHost(peer, { expiresAt: Date.now() + 60_000 });
    await f.putParticipant(peer); // native does not match either collision selector
    await f.mesh.put({ key: "sessions/shared-session", identity: peer, value: { id: peer.id, sessionId: "shared-session",
      name: "shared-name", cwd: "/fixture/project", startedAt: 1, status: "idle", livenessLeaseFiles: 1,
      livenessHostId: peer.id, livenessStartedAt: 1 } });
    writeHostLease(f.meshRoot, f.lease(peer, { session: { id: "shared-session", startedAt: 1,
      updatedAt: Date.now(), expiresAt: Date.now() + 60_000 } }));
    await f.directory.start();
    expect(f.collisions).toEqual([]);
    // Source session remains live by its file despite a lapsed stored envelope and native host.
    await f.putHost(peer);
    writeHostLease(f.meshRoot, f.lease(peer, { expiresAt: Date.now() - 1, session: { id: "shared-session", startedAt: 1,
      updatedAt: Date.now(), expiresAt: Date.now() + 60_000 } }));
    await f.directory.refresh();
    expect(f.collisions).toEqual([expect.objectContaining({ reason: "duplicate-session" })]);
  });

  it("rejects mismatched mirror/native owners and non-live root statuses without losing a valid mirror collision", async () => {
    const f = await setup(backend);
    for (const [id, hostPatch, participantPatch] of [
      ["wrong-pair", {}, { remoteHost: "forge" }],
      ["stopped", {}, { status: "stopped" }],
      ["reloaded", {}, { status: "reloading", reloadUntil: Date.now() - 1 }],
      ["mirrored", { remoteHost: "forge" }, { remoteHost: "forge" }],
    ] as const) {
      const peer = identity(id);
      await f.putHost(peer, { expiresAt: Date.now() + 60_000, ...hostPatch });
      await f.putParticipant(peer, { name: "shared-name", ...participantPatch });
    }
    await f.directory.start();
    expect(f.collisions).toEqual([expect.objectContaining({ ids: ["mirrored", f.self.id].sort() })]);
  });

  it.each([true, false])("keeps only an authentic native root fallback under a newer observer actor-terminal envelope (authentic=%s)", async authentic => {
    const f = await setup(backend);
    const peer = identity("native-under-terminal");
    await f.putHost(peer, { expiresAt: Date.now() + 60_000 });
    writeParticipantFile(f.meshRoot, { key: participantKey(peer.id), version: 1, updatedBy: authentic ? peer : identity("forger"),
      updatedAt: Date.now() - 10_000, value: record(peer, { name: "shared-name" }) });
    await f.mesh.put({ key: participantKey(peer.id), identity: identity("observer"), value: {
      ...record(peer, { kind: "actor", name: "observer-terminal", status: "stopped" }), stopReason: "owner-root-gone",
    } });
    await f.directory.start();
    expect(f.collisions).toEqual(authentic
      ? [expect.objectContaining({ reason: "duplicate-name", ids: [peer.id, f.self.id].sort() })] : []);
  });

  it("preserves unrelated and shadowed mirror refusals from expired native claims without reading their leases", async () => {
    const f = await setup(backend);
    const irrelevant = await f.retained(12);
    const native = identity("native-claim");
    await f.putHost(native, { rootId: "claimed-root" }); // an expired native claim still refuses a mirror
    const remote = identity("unrelated-mirror-owner");
    await f.putHost(remote, { remoteHost: "forge", rootId: "claimed-root" });
    writeHostLease(f.meshRoot, f.lease(remote, { rootId: "claimed-root" }));
    irrelevant.add(hostLeasePath(f.meshRoot, remote.id));
    await f.putParticipant(remote, { id: "unrelated-mirror", rootId: "claimed-root", remoteHost: "forge", status: "stopped" });
    writeParticipantFile(f.meshRoot, { key: participantKey(native.id), version: 1, updatedBy: native,
      updatedAt: Date.now() - 10_000, value: record(native) });
    await f.putParticipant(remote, { id: native.id, rootId: "claimed-root", remoteHost: "forge" });
    const listings = vi.spyOn(f.directory, "list");
    const io = probe(f.meshRoot, irrelevant);
    await f.directory.start();
    await f.directory.refresh();
    expect(f.collisions).toEqual([]); // neither root matches our name/session
    io.untouched(); io.noListing();
    expect(listings).not.toHaveBeenCalled();
    await f.directory.close(); // join any in-flight advisory publication before reading persisted events
    const refusals = f.mesh.tail(0, 1_000).events.filter(event => event.topic === MIRROR_COLLISION_TOPIC);
    expect(refusals.map(event => (event.data as { id: string }).id).sort()).toEqual([native.id, "unrelated-mirror"].sort());
  });

  it("checks only a desired legacy actor's session owner and keeps its live file-backed takeover veto", async () => {
    let now = Date.now();
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const f = await setup(backend);
    const irrelevant = await f.retained(16);
    const peer = identity("legacy-actor-owner");
    const actorId = "actor:legacy-contested";
    f.directory.registerSource(() => [record(f.self, { id: actorId, kind: "actor", name: actorId })]);
    await f.mesh.put({ key: "sessions/legacy-actor-session", identity: peer, value: {
      id: peer.id, sessionId: "legacy-actor-session", cwd: "/fixture/project", startedAt: 1, status: "idle",
      livenessLeaseFiles: 1, livenessHostId: peer.id, livenessStartedAt: 1,
    } });
    await f.mesh.put({ key: `actors/legacy-actor-session/${actorId}`, identity: peer,
      value: { id: actorId, name: actorId, runner: "pi", status: "running" } });
    now += 20_000; // the stored legacy session is expired; only its matching session file keeps it live
    const fileLease = f.lease(peer, { session: { id: "legacy-actor-session", startedAt: 1, updatedAt: now, expiresAt: now + 60_000 } });
    writeHostLease(f.meshRoot, fileLease);
    const io = probe(f.meshRoot, irrelevant);
    await f.directory.start();
    expect(readParticipantFile(f.meshRoot, participantKey(actorId))).toBeUndefined();
    io.untouched(); io.noListing();
    expect(io.statsFor(peer.id)).toBe(1);
    io.reset();
    fs.writeFileSync(hostLeasePath(f.meshRoot, peer.id), JSON.stringify({ format: 1, ...fileLease,
      session: { ...fileLease.session!, expiresAt: now - 1 } }));
    await f.directory.refresh();
    expect(readParticipantFile(f.meshRoot, participantKey(actorId))?.value).toMatchObject({ ownerHostId: f.self.id });
    io.untouched(); io.noListing();
    expect(io.statsFor(peer.id)).toBe(1);
  });

  it("rechecks a relevant lease after awaited confirmation rather than retaining the prepared negative", async () => {
    let now = Date.now();
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const f = await setup(backend, false);
    const old = identity("racing-old-reader");
    await f.putHost(old, { livenessLeaseFiles: undefined });
    writeHostLease(f.meshRoot, f.lease(old, { expiresAt: now - 1 }));
    await f.directory.start();
    const sessionBefore = f.mesh.get(`sessions/${f.self.id}`, { fresh: true })!;
    now += 8_000; // legacy session's half-life passed, but no live old reader yet
    const confirm = f.mesh.confirmWritable.bind(f.mesh);
    const confirmations = vi.spyOn(f.mesh, "confirmWritable").mockImplementation(async onAcquired => {
      // The lock/confirmation boundary is awaited. A peer revives without a state commit.
      await Promise.resolve();
      fs.writeFileSync(hostLeasePath(f.meshRoot, old.id), JSON.stringify({ format: 1, ...f.lease(old) }));
      return confirm(onAcquired);
    });
    await f.directory.refresh();
    expect(confirmations).toHaveBeenCalledTimes(1);
    const after = f.mesh.get(`sessions/${f.self.id}`, { fresh: true })!;
    expect(after.version).toBeGreaterThan(sessionBefore.version);
    expect(after.updatedAt).toBe(now);
  });
});
