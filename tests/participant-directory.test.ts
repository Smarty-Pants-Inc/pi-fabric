import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { EventEmitter } from "node:events";
import fs from "node:fs";
import { projectOf } from "../src/topology/project-identity.js";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { MeshBackgroundRetry } from "../src/core/atomic-write.js";
import { confirmWitnessPlatform, DEFAULT_PARTICIPANT_LEASE_MS, PARTICIPANT_HEARTBEAT_MS, ParticipantDirectory, ROUTING_REFRESH_LOCK_BUDGET_MS } from "../src/topology/participant-directory.js";
import { MeshLock } from "../src/mesh/mesh-lock.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";
import { actorParticipantRecord } from "../src/topology/records.js";
import type { FabricActorInfo } from "../src/actors/types.js";
import { hostLeasePath, LIVENESS_POLICY_KEY, MIRROR_FRESH_MS, mirroredLeaseStatus, readHostLeases, removeHostLease, STATE_LEASE_RENEW_MS, writeHostLease, type FabricMirrorLease } from "../src/topology/host-leases.js";
import { MainAgentController } from "../src/main-agent.js";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import type { FabricParticipantRecord } from "../src/topology/types.js";
import { sessionLiveness } from "../src/topology/legacy-root-liveness.js";
import { awaitPeerSettle, type PeerSettleResult } from "../src/topology/peer-settle.js";
import { DIRECTORY_RETENTION_MS } from "../src/topology/host-reaper.js";

const roots: string[] = [];
const directories: ParticipantDirectory[] = [];
// smarty-dev#6956: for tests that read a peer's record right after start(), not across expiry.
// With the default 300 ms test lease, a stalled event loop (a loaded CI runner) let the peer's
// host lease lapse before the read; peers() then fell back to its legacy sessions/ advertisement
// (fixed 15 s TTL), which carries no role, project or repository. No stall outlasts this lease.
const steadyLease = { heartbeatMs: 60_000, leaseMs: 120_000 };

const rootRecord = (
  id: string,
  hostId: string,
  sessionId: string,
): FabricParticipantRecord => ({
  format: 1,
  id,
  kind: "root",
  rootId: id,
  ownerHostId: hostId,
  ownerIdentityId: hostId,
  name: "main",
  status: "idle",
  runner: "pi",
  transport: "host",
  capabilities: ["steer", "followUp", "fabric"],
  cwd: "/tmp/project",
  sessionId,
  startedAt: 1,
  updatedAt: 2,
  pendingMessages: false,
  controlProtocol: "v1",
});

const agentRecord = (
  id: string,
  rootId: string,
  hostId: string,
  parentId: string,
): FabricParticipantRecord => ({
  format: 1,
  id,
  kind: "agent",
  rootId,
  ownerHostId: hostId,
  ownerIdentityId: hostId,
  parentId,
  name: id,
  status: "running",
  runner: "pi",
  transport: "process",
  capabilities: ["steer", "followUp", "stop"],
  cwd: "/tmp/project",
  startedAt: 3,
  updatedAt: 4,
  controlProtocol: "v1",
});

const createDirectory = (
  meshRoot: string,
  identity: MeshIdentity,
  rootId: string,
  source: () => FabricParticipantRecord[],
  // smarty-dev#883: a test that does not exercise expiry uses a lease a starved Windows runner, whose
  // timers stall, cannot outlast between a write and the read that follows it. The directory makes
  // the lease at least two heartbeats.
  lease: { heartbeatMs: number; leaseMs: number } = { heartbeatMs: 100, leaseMs: 300 },
): ParticipantDirectory => {
  const hostId = identity.kind === "main" ? identity.id : "runtime:" + identity.sessionId;
  const directory = new ParticipantDirectory(
    new MeshStore(meshRoot, 64 * 1024, 1_000),
    { enabled: true, hostId, rootId, identity, ...lease },
  );
  directory.registerSource(source);
  directories.push(directory);
  return directory;
};

afterEach(async () => {
  await Promise.all(directories.splice(0).map((directory) => directory.close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

// Keep reason: private-receipt trust boundary and PR #1180's legacy retention bypass.
// Read-only stub: no mesh writes, actor execution, or product deletion.
describe("legacy actor tombstone retention", () => {
  const now = 2 * DIRECTORY_RETENTION_MS;
  const identity: MeshIdentity = { id: "session:observer", name: "Observer", kind: "main" };
  const terminal = (settledAt: number | undefined): FabricParticipantRecord => ({
    ...agentRecord("legacy-actor", "session:gone", "session:gone", "session:gone"),
    kind: "actor", status: "stopped", stopReason: "owner-root-gone", capabilities: [],
    transport: "host", controlProtocol: "legacy", ...(settledAt !== undefined ? { settledAt } : {}),
  });
  const fixture = (validate: () => FabricParticipantRecord | undefined,
    copies: "legacy" | "participant" | "dual" = "legacy") => {
    const entry = { key: "actors/gone/legacy-actor", version: 7, updatedAt: now + DIRECTORY_RETENTION_MS,
      updatedBy: identity, value: { id: "legacy-actor", status: "stopped", stopReason: "owner-root-gone",
        settledAt: now + DIRECTORY_RETENTION_MS } };
    const participant = { ...entry, key: "topology/participants/legacy-actor" };
    const entries = copies === "legacy" ? [entry] : copies === "participant" ? [participant] : [entry, participant];
    const listAll = (prefix: string) => entries.filter(candidate => candidate.key.startsWith(prefix));
    const token = {};
    const mesh = {
      root: path.join(os.tmpdir(), `p2-read-only-legacy-directory-${process.pid}`),
      listAll, listAllShared: listAll, stateToken: () => token,
    } as unknown as MeshStore;
    return new ParticipantDirectory(mesh, { enabled: true, hostId: identity.id, rootId: identity.id,
      identity, reapDeadHosts: false, terminalActor: validate });
  };

  it.each([
    ["fresh", now - DIRECTORY_RETENTION_MS + 1, true],
    ["expired despite a future public envelope", now - DIRECTORY_RETENTION_MS - 1, false],
    ["exact retention boundary", now - DIRECTORY_RETENTION_MS, false],
    ["missing private time", undefined, false],
    ["nonfinite private time", NaN, false],
    ["future private time", now + 1, false],
  ] as const)("lists only retained authenticated tombstones: %s", (_name, settledAt, listed) => {
    const directory = fixture(() => terminal(settledAt));
    const actors = directory.list({ scope: "project", kinds: ["actor"], includeStale: true }, now);
    expect(actors.map(actor => actor.id)).toEqual(listed ? ["legacy-actor"] : []);
    if (listed) expect(actors[0]).toMatchObject({ settledAt, capabilities: [], stale: true });
  });

  describe.each(["participant", "dual"] as const)("%s copies", copies => {
    it.each([
      ["fresh", now - DIRECTORY_RETENTION_MS + 1, true],
      ["exact retention boundary", now - DIRECTORY_RETENTION_MS, false],
      ["older despite future public time", now - DIRECTORY_RETENTION_MS - 1, false],
      ["missing private time", undefined, false],
      ["future private time", now + 1, false],
    ] as const)("uses immutable private time: %s", (_name, settledAt, listed) => {
      const directory = fixture(() => terminal(settledAt), copies);
      const actors = directory.list({ kinds: ["actor"], includeStale: true }, now);
      expect(actors.map(actor => actor.id)).toEqual(listed ? ["legacy-actor"] : []);
      if (listed) expect(actors[0]).toMatchObject({ settledAt, capabilities: [], stale: true });
    });

    it("ages repeated cached reads and revalidates proof", () => {
      let authenticated = true;
      const directory = fixture(() => authenticated ? terminal(now) : undefined, copies);
      const options = { kinds: ["actor"] as Array<"actor">, includeStale: true };
      expect(directory.list(options, now)).toHaveLength(1);
      expect(directory.list(options, now + DIRECTORY_RETENTION_MS - 1)).toHaveLength(1);
      expect(directory.list(options, now + DIRECTORY_RETENTION_MS)).toEqual([]);
      expect(directory.list(options, now + DIRECTORY_RETENTION_MS + 1)).toEqual([]);
      authenticated = false;
      expect(directory.list(options, now)).toEqual([]);
    });
  });

  it("does not trust a public terminal marker without private authentication", () => {
    const directory = fixture(() => undefined);
    expect(directory.list({ kinds: ["actor"], includeStale: true }, now)).toEqual([]);
  });

  it("ages unchanged legacy envelopes and revalidates private proof on each read", () => {
    let authenticated = true;
    const directory = fixture(() => authenticated ? terminal(now) : undefined);
    const options = { kinds: ["actor"] as Array<"actor">, includeStale: true };
    expect(directory.list(options, now)).toHaveLength(1);
    expect(directory.list(options, now + DIRECTORY_RETENTION_MS)).toEqual([]);
    authenticated = false;
    expect(directory.list(options, now)).toEqual([]);
  });
});

describe("ParticipantDirectory routing freshness (#2386)", () => {
  const fixture = () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-routing-view-"));
    roots.push(root);
    const identity: MeshIdentity = { id: "session:observer", name: "Observer", kind: "main" };
    return createDirectory(path.join(root, "mesh"), identity, identity.id, () => [], { heartbeatMs: 60_000, leaseMs: 120_000 });
  };

  it("distinguishes missing, fresh and overdue views, and does not upgrade read evidence to lease admission", async () => {
    const directory = fixture();
    expect(directory.routingUnavailable()).toContain("no confirmed view");
    expect(directory.canConsumeMesh()).toBe(false);
    // A bounded state operation, not exclusive() on the mesh lock (smarty-dev#6477 L2b).
    // smarty-dev#7651: withStateFence passes the budget LEFT (deadline - Date.now()), which drops to
    // 1999 when a millisecond ticks between computing the deadline and the try. Bound it by the clock
    // time that elapsed around the call instead of demanding the exact constant.
    const tryLock = directory.mesh.withTryLock.bind(directory.mesh);
    let triedAt = 0;
    const read = vi.spyOn(directory.mesh, "withTryLock").mockImplementation((operation, timeout) => {
      triedAt ||= Date.now(); return tryLock(operation, timeout);
    });
    const exclusive = vi.spyOn(directory.mesh, "exclusive");
    const fence = vi.spyOn(directory.mesh, "writeBatch");
    const calledAt = Date.now();
    await directory.refreshRoutingView();
    expect(read).toHaveBeenCalledWith(expect.any(Function), expect.any(Number));
    expect(read.mock.calls[0]![1]).toBeLessThanOrEqual(ROUTING_REFRESH_LOCK_BUDGET_MS);
    expect(read.mock.calls[0]![1]).toBeGreaterThanOrEqual(ROUTING_REFRESH_LOCK_BUDGET_MS - (triedAt - calledAt));
    expect(fence).toHaveBeenCalledWith(expect.objectContaining({ ops: [], prepare: expect.any(Function) }));
    expect(exclusive).not.toHaveBeenCalled();
    expect(directory.routingUnavailable()).toBeUndefined();
    expect(directory.canConsumeMesh()).toBe(false);
    await directory.refresh();
    expect(directory.routingUnavailable()).toBeUndefined();
    expect(directory.routingUnavailable(directory.confirmedAt() + 120_000)).toContain("overdue");
    const confirmed = directory.confirmedAt();
    await directory.refreshRoutingView();
    expect(directory.confirmedAt()).toBe(confirmed);
  });

  it("invalidates prior routing confirmation after any failed refresh and recovers with one canonical read", async () => {
    const directory = fixture();
    await directory.refresh();
    await directory.refreshRoutingView();
    const failure = new Error("directory refresh read failed");
    const heartbeat = vi.spyOn(directory.mesh, "confirmWritable").mockRejectedValueOnce(failure);
    await expect(directory.refresh()).rejects.toBe(failure);
    heartbeat.mockRestore();
    expect(directory.routingUnavailable()).toBe(failure.message);
    const confirmed = directory.confirmedAt();
    await directory.refreshRoutingView();
    expect(directory.routingUnavailable()).toBeUndefined();
    expect(directory.confirmedAt()).toBe(confirmed);
    expect(directory.canConsumeMesh()).toBe(false);
    // A canonical read failure cannot leave behind successful routing evidence.
    const read = vi.spyOn(directory.mesh, "stateToken").mockImplementation(() => { throw new Error("unreadable canonical view"); });
    await expect(directory.refreshRoutingView()).rejects.toThrow("unreadable canonical view");
    expect(directory.routingUnavailable()).toBeDefined();
    read.mockRestore();
    await directory.refreshRoutingView();
    expect(directory.routingUnavailable()).toBeUndefined();
  });

  // smarty-dev#6477: one fleet publish holds the lock ~250 ms (smarty-dev#8305); a 250 ms budget
  // failed the send behind one hold. The budget covers a few holds and still bounds a stuck holder.
  const holdLock = (directory: ParticipantDirectory, ms: number): Promise<void> =>
    new MeshLock(directory.mesh.root, {}, () => undefined)
      .withLockAcrossAwait(() => new Promise<void>((resolve) => setTimeout(resolve, ms)));

  it("survives a ~400 ms publish hold and still fails past its lock budget", async () => {
    const directory = fixture();
    let hold = holdLock(directory, 400);
    await new Promise((resolve) => setTimeout(resolve, 20));
    await directory.refreshRoutingView();
    expect(directory.routingUnavailable()).toBeUndefined();
    await hold;

    hold = holdLock(directory, ROUTING_REFRESH_LOCK_BUDGET_MS + 300);
    await new Promise((resolve) => setTimeout(resolve, 20));
    await expect(directory.refreshRoutingView()).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    expect(directory.routingUnavailable()).toBeDefined();
    await hold;
  }, 10_000);

  it("does not refresh a closed directory", async () => {
    const directory = fixture();
    await directory.refresh();
    await directory.close();
    expect(directory.routingUnavailable()).toContain("closed");
    await expect(directory.refreshRoutingView()).rejects.toThrow("closed");
  });
});

describe("#3662 ParticipantDirectory lineage liveness", () => {
  it("shares preparation reads across cache expiry but freshens after the confirmation fence (#4383)", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-refresh-snapshot-"));
    roots.push(root);
    const identity: MeshIdentity = { id: "session:snapshot", name: "main", kind: "main" };
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 1_000, { readCacheMs: 5_000 });
    const directory = new ParticipantDirectory(mesh, {
      enabled: true, hostId: identity.id, rootId: identity.id, identity,
      heartbeatMs: 5_000, leaseMs: 15_000, reapDeadHosts: false,
    });
    directories.push(directory);
    directory.registerSource(() => [rootRecord(identity.id, identity.id, "snapshot")]);
    let now = Date.now();
    const clock = vi.spyOn(Date, "now").mockImplementation(() => now);
    const originalGet = mesh.get.bind(mesh);
    let crossed = false;
    try {
      await directory.refresh();
      now += 4_999;
      const reads = vi.spyOn(fs, "readFileSync");
      const get = vi.spyOn(mesh, "get").mockImplementation((key, options) => {
        const result = originalGet(key, options);
        if (key === LIVENESS_POLICY_KEY && !crossed) { crossed = true; now += 2; }
        return result;
      });
      try {
        await directory.refresh();
        expect(crossed).toBe(true);
        const stateReads = reads.mock.calls.filter(([file]) => String(file) === path.join(mesh.root, "state.json"));
        expect(stateReads).toHaveLength(0); // The post-lock recheck gates unchanged metadata, not another payload parse.
        const hostKey = "topology/hosts/" + createHash("sha256").update(identity.id).digest("hex");
        const hostReads = get.mock.calls.filter(([key]) => key === hostKey);
        expect(hostReads).toHaveLength(2);
        expect(hostReads[0]![1]?.snapshot).toBeDefined();
        expect(hostReads[1]![1]?.snapshot).toBeUndefined();
      } finally { get.mockRestore(); reads.mockRestore(); }
    } finally { clock.mockRestore(); }
  });
  it("freshens lineage only on root activation or explicit demand, never on idle heartbeats (#4383)", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-lineage-demand-"));
    roots.push(root);
    const identity: MeshIdentity = { id: "session:resumed", name: "main", kind: "main" };
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 1_000, { readCacheMs: 5_000 });
    const writer = new MeshStore(mesh.root, 64 * 1024, 1_000);
    const directory = new ParticipantDirectory(mesh, {
      enabled: true, hostId: identity.id, rootId: identity.id, identity,
      heartbeatMs: 5_000, leaseMs: 15_000, reapDeadHosts: false,
    });
    directories.push(directory);
    let active = true;
    directory.registerSource(() => active ? [rootRecord(identity.id, identity.id, "resumed")] : []);
    const key = "topology/lineage-closures/" + createHash("sha256").update(identity.id).digest("hex");
    const closure = () => writer.put({ key, identity, value: { format: 1, rootId: identity.id, closedAt: Date.now() } });
    mesh.stateToken(); // Warm before a closure is added by the earlier generation.
    await closure();
    const resume = vi.spyOn(directory, "resumeLineage");
    const get = vi.spyOn(mesh, "get");
    try {
      await directory.refresh();
      expect(resume).toHaveBeenCalledOnce();
      expect(get).toHaveBeenCalledWith(key, { fresh: true });
      expect(writer.get(key, { fresh: true })).toBeUndefined();
      get.mockClear();
      for (let heartbeat = 0; heartbeat < 3; heartbeat++) await directory.refresh();
      expect(resume).toHaveBeenCalledOnce();
      expect(get.mock.calls.some(([target, read]) => target === key && read?.fresh)).toBe(false);
      // Explicit runtime resumption must remain canonical against a warm negative cache.
      await closure();
      expect(mesh.get(key)).toBeUndefined();
      await directory.resumeLineage();
      expect(writer.get(key, { fresh: true })).toBeUndefined();
      expect(resume).toHaveBeenCalledTimes(2);
      // Removing and re-adding a source is a new activation, not an idle heartbeat.
      active = false;
      await directory.refresh();
      await closure();
      active = true;
      await directory.refresh();
      expect(resume).toHaveBeenCalledTimes(3);
      expect(writer.get(key, { fresh: true })).toBeUndefined();
      await directory.close();
      await closure();
      await directory.start();
      expect(resume).toHaveBeenCalledTimes(4);
      expect(writer.get(key, { fresh: true })).toBeUndefined();
    } finally { get.mockRestore(); resume.mockRestore(); }
  });
  it.each([null, {}, { format: 1, id: "session:lineage", kind: "invalid" }])(
    "S1 retains invalid raw shared-state lineage %j until withdrawal", async (value) => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-lineage-raw-"));
      roots.push(root);
      const identity: MeshIdentity = { id: "observer", name: "main", kind: "main" };
      const directory = createDirectory(path.join(root, "mesh"), identity, identity.id, () => []);
      const id = "session:lineage";
      const key = "topology/participants/" + createHash("sha256").update(id).digest("hex");
      await directory.mesh.put({ key, identity, value });
      expect(directory.get(id, Date.now(), { fresh: true })).toBeUndefined();
      expect(directory.lastKnown(id)).toBeUndefined();
      expect(directory.lineageAlive(id)).toBe(true);
      await directory.mesh.delete({ key });
      expect(directory.lineageAlive(id)).toBe(true); // Removal alone is not a close receipt.
    },
  );

  it("S1 treats a failed raw lineage read as unknown, never dead", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-lineage-read-"));
    roots.push(root);
    const identity: MeshIdentity = { id: "observer", name: "main", kind: "main" };
    const directory = createDirectory(path.join(root, "mesh"), identity, identity.id, () => []);
    const read = vi.spyOn(directory.mesh, "get").mockImplementation(() => { throw new Error("unreadable state"); });
    try {
      expect(directory.lineageAlive("session:lineage")).toBe(true);
    } finally {
      read.mockRestore();
    }
    expect(directory.lineageAlive("session:lineage")).toBe(true);
  });

  it("does not treat lease expiry as lineage death, but observes withdrawal", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-lineage-"));
    roots.push(root);
    const id = "session:lineage";
    const directory = createDirectory(path.join(root, "mesh"), { id, name: "main", kind: "main" }, id,
      () => [rootRecord(id, id, "lineage")]);
    await directory.refresh();
    const expiredAt = Date.now() + 120_000;
    expect(directory.get(id, expiredAt)).toBeUndefined();
    expect(directory.lastKnown(id, expiredAt)?.participant.stale).toBe(true);
    expect(directory.lineageAlive(id, expiredAt)).toBe(true);
    expect(directory.lineageAlive("session:unknown", expiredAt)).toBe(true);
    await directory.closeLineage();
    expect(directory.lineageAlive(id)).toBe(false);
  });
});
describe("ParticipantDirectory.mirroredControlOwner", () => {
  const keyFor = (prefix: string, id: string) => prefix + createHash("sha256").update(id).digest("hex");
  const setup = async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-mirror-owner-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const writer = new MeshStore(meshRoot, 64 * 1024, 1_000);
    const reader = new MeshStore(meshRoot, 64 * 1024, 1_000, { readCacheMs: 60_000 });
    const localIdentity: MeshIdentity = { id: "local-identity", name: "main", kind: "main" };
    const identity: MeshIdentity = { id: "remote-identity", name: "main", kind: "main" };
    const directory = new ParticipantDirectory(reader, {
      enabled: true, hostId: "local-host", rootId: "local-root", identity: localIdentity,
    });
    const participant = {
      ...rootRecord("remote-root", "remote-host", "remote-session"),
      ownerIdentityId: identity.id, remoteHost: "forge",
    };
    const host = {
      format: 1, id: "remote-host", rootId: participant.id, identity,
      startedAt: 1, updatedAt: 2, expiresAt: Date.now() + 15_000, remoteHost: "forge",
    };
    const participantKey = keyFor("topology/participants/", participant.id);
    const hostKey = keyFor("topology/hosts/", host.id);
    const putParticipant = (patch: Record<string, unknown> = {}, author = identity) => writer.put({
      key: participantKey, value: { ...participant, ...patch }, identity: author,
    });
    const putHost = (patch: Record<string, unknown> = {}, author = identity) => writer.put({
      key: hostKey, value: { ...host, ...patch }, identity: author,
    });
    await putHost();
    await putParticipant();
    const read = () => directory.mirroredControlOwner(host.id, identity.id, participant.id);
    return { meshRoot, writer, reader, directory, identity, participant, host, participantKey, hostKey, putParticipant, putHost, read };
  };

  it("exposes the exact public read port and retains fresh expiry knowledge from first send through lapse", async () => {
    const { directory, reader, host, putHost, read, meshRoot } = await setup();
    expect(typeof ParticipantDirectory.prototype.mirroredControlOwner).toBe("function");
    const port: (host: string, identity: string | undefined, target: string) =>
      { remoteHost: string; expiresAt: number } | undefined = directory.mirroredControlOwner.bind(directory);
    expect(port(host.id, undefined, host.rootId)).toBeUndefined();
    const publish = vi.spyOn(reader, "publish");
    const put = vi.spyOn(reader, "put");
    try {
      expect(read()).toEqual({ remoteHost: "forge", expiresAt: host.expiresAt });
      const renewed = host.expiresAt + 30_000;
      await putHost({ expiresAt: renewed });
      expect(read()).toEqual({ remoteHost: "forge", expiresAt: renewed });
      const expired = Date.now() - 1_000;
      await putHost({ expiresAt: expired });
      expect(read()).toEqual({ remoteHost: "forge", expiresAt: expired });
      // #10068: an unsampled (older-bridge) projection proves only its unexpired deadline, never
      // the origin's death. Past it, the root stays attributed to forge (smarty-dev#2004) as bridge-stale.
      expect(directory.get(host.rootId, Date.now(), { fresh: true }))
        .toMatchObject({ id: host.rootId, remoteHost: "forge", stale: false, bridgeStale: true });

      // A current bridge's projection requires a matching source-clock sample (mirrorLease: 1).
      const sample = (observedAt: number, sourceExpiresAt: number): FabricMirrorLease => ({ remoteHost: "forge",
        observedAt, sourceObservedAt: observedAt - 600_000, sourceUpdatedAt: observedAt - 605_000, sourceExpiresAt });
      const lease = (mirror?: FabricMirrorLease) => writeHostLease(meshRoot, { id: host.id, rootId: host.rootId,
        identityId: host.identity.id, startedAt: host.startedAt, updatedAt: Date.now(), expiresAt: Date.now() + 15_000,
        mirrorLease: 1, ...(mirror ? { mirror } : {}) });
      await putHost({ updatedAt: Date.now(), expiresAt: Date.now() + 15_000, mirrorLease: 1 });

      // Fresh bridge, live source lease (judged on the source clock, not this hub's).
      const fresh = sample(Date.now(), Date.now() - 600_000 + 15_000);
      lease(fresh);
      const live = read();
      expect(live).toEqual({ remoteHost: "forge", expiresAt: expect.any(Number), mirror: fresh });
      expect(live!.expiresAt).toBeLessThanOrEqual(fresh.observedAt + MIRROR_FRESH_MS);
      expect(mirroredLeaseStatus(live!.remoteHost, live!.mirror, Date.now())).toBe("live");
      expect(directory.get(host.rootId, Date.now(), { fresh: true }))
        .toMatchObject({ remoteHost: "forge", stale: false });
      expect(directory.get(host.rootId, Date.now(), { fresh: true })?.bridgeStale).toBeUndefined();

      // Fresh bridge, source lease lapsed on the source clock: lease-stale, the root is gone.
      const lapsed = sample(Date.now(), Date.now() - 600_000 - 1);
      lease(lapsed);
      expect(read()).toEqual({ remoteHost: "forge", expiresAt: lapsed.observedAt - 1, mirror: lapsed });
      expect(mirroredLeaseStatus("forge", read()!.mirror, Date.now())).toBe("lease-stale");
      expect(directory.get(host.rootId, Date.now(), { fresh: true })).toBeUndefined();

      // Stale transport, live source sample: bridge-stale, not owner death, still attributed.
      const old = sample(Date.now() - MIRROR_FRESH_MS, Date.now() + 60_000);
      lease(old);
      expect(read()).toEqual({ remoteHost: "forge", expiresAt: expect.any(Number), mirror: old });
      expect(mirroredLeaseStatus("forge", read()!.mirror, Date.now())).toBe("bridge-stale");
      expect(directory.get(host.rootId, Date.now(), { fresh: true }))
        .toMatchObject({ remoteHost: "forge", stale: false, bridgeStale: true });

      // Required sample missing: doubt (bridge-stale), never a fallback to the state expiry.
      lease();
      expect(read()).toEqual({ remoteHost: "forge", expiresAt: expect.any(Number), bridgeStale: true });
      expect(directory.get(host.rootId, Date.now(), { fresh: true }))
        .toMatchObject({ remoteHost: "forge", stale: false, bridgeStale: true });
      expect(publish).not.toHaveBeenCalled();
      expect(put).not.toHaveBeenCalled();
    } finally {
      publish.mockRestore();
      put.mockRestore();
    }
  });

  it("uses later matching file expiry, and observes file renewals and removal", async () => {
    const { meshRoot, host, read } = await setup();
    const lease = {
      id: host.id, rootId: host.rootId, identityId: host.identity.id,
      updatedAt: Date.now(), expiresAt: host.expiresAt + 30_000,
    };
    const leaseFile = path.join(meshRoot, "host-leases",
      createHash("sha256").update(host.id).digest("hex").slice(0, 32) + ".json");
    const replaceLease = (expiresAt: number) => {
      writeHostLease(meshRoot, { ...lease, updatedAt: Date.now(), expiresAt });
    };
    replaceLease(lease.expiresAt);
    expect(read()).toEqual({ remoteHost: "forge", expiresAt: lease.expiresAt });
    replaceLease(lease.expiresAt + 100_000);
    expect(read()?.expiresAt).toBe(lease.expiresAt + 100_000);
    replaceLease(host.expiresAt - 1);
    expect(read()?.expiresAt).toBe(host.expiresAt);
    // Removal must discard a cached lease that still outranks state, not merely a shorter one.
    replaceLease(lease.expiresAt + 100_000);
    expect(read()?.expiresAt).toBe(lease.expiresAt + 100_000);
    removeHostLease(meshRoot, host.id);
    expect(fs.existsSync(leaseFile)).toBe(false);
    expect(read()?.expiresAt).toBe(host.expiresAt);
  });

  it.each(["rootId", "identityId"])("ignores a later file lease with mismatched %s", async (field) => {
    const { meshRoot, host, read } = await setup();
    writeHostLease(meshRoot, {
      id: host.id, rootId: host.rootId, identityId: host.identity.id,
      updatedAt: Date.now(), expiresAt: host.expiresAt + 30_000, [field]: "wrong",
    });
    expect(read()).toEqual({ remoteHost: "forge", expiresAt: host.expiresAt });
  });

  it("requires exact caller target, owner host and identity bindings", async () => {
    const { directory, host } = await setup();
    expect(directory.mirroredControlOwner(host.id, "wrong", host.rootId)).toBeUndefined();
    expect(directory.mirroredControlOwner("wrong", host.identity.id, host.rootId)).toBeUndefined();
    expect(directory.mirroredControlOwner(host.id, host.identity.id, "wrong")).toBeUndefined();
    directory.options.enabled = false;
    expect(directory.mirroredControlOwner(host.id, host.identity.id, host.rootId)).toBeUndefined();
  });

  it.each([
    { rootId: "wrong" }, { ownerHostId: "wrong" }, { ownerIdentityId: "wrong" },
    { remoteHost: "other" }, { remoteHost: undefined }, { remoteHost: "bad/name" },
    { remoteHost: 42 }, { kind: "agent" }, { format: 2 }, { id: "wrong" },
    { capabilities: ["unknown"] }, { role: 42 },
  ])("rejects invalid or conflicting participant metadata %j", async (patch) => {
    const { putParticipant, read } = await setup();
    await putParticipant(patch);
    expect(read()).toBeUndefined();
  });

  it.each([
    { rootId: "wrong" }, { identity: { id: "wrong", name: "main", kind: "main" } },
    { remoteHost: "other" }, { remoteHost: undefined }, { remoteHost: "bad/name" },
    { remoteHost: 42 }, { expiresAt: "wrong" }, { expiresAt: null },
    { format: 2 }, { id: "wrong" },
  ])("rejects invalid, native or conflicting host metadata %j", async (patch) => {
    const { putHost, read } = await setup();
    await putHost(patch);
    expect(read()).toBeUndefined();
  });

  it("rejects records whose entry author does not match the claimed owner", async () => {
    const { identity, putParticipant, putHost, read } = await setup();
    const stranger = { ...identity, id: "stranger" };
    await putParticipant({}, stranger);
    expect(read()).toBeUndefined();
    await putParticipant();
    await putHost({}, stranger);
    expect(read()).toBeUndefined();
  });

  it("does not attribute a missing participant or host", async () => {
    const { writer, participantKey, hostKey, putParticipant, read } = await setup();
    await writer.delete({ key: participantKey });
    expect(read()).toBeUndefined();
    await putParticipant();
    await writer.delete({ key: hostKey });
    expect(read()).toBeUndefined();
  });

  it("keeps a native participant file ahead of a newer state mirror", async () => {
    const { meshRoot, writer, participantKey, participant, putParticipant, read } = await setup();
    const entry = writer.get(participantKey, { fresh: true })!;
    writeParticipantFile(meshRoot, {
      ...entry, updatedAt: 0, value: { ...participant, remoteHost: undefined },
    });
    await putParticipant();
    expect(read()).toBeUndefined();
  });

  it.each(["id", "rootId", "identity"])("refuses collisions with a native host's %s without publishing", async (field) => {
    const { writer, reader, host, read } = await setup();
    const identity: MeshIdentity = { id: field === "identity" ? host.rootId : "native-identity", name: "main", kind: "main" };
    const nativeId = field === "id" ? host.rootId : "native-host";
    await writer.put({
      key: keyFor("topology/hosts/", nativeId), identity,
      value: {
        ...host, id: nativeId, identity, remoteHost: undefined,
        rootId: field === "rootId" ? host.rootId : "native-root",
      },
    });
    const publish = vi.spyOn(reader, "publish");
    try {
      expect(read()).toBeUndefined();
      expect(publish).not.toHaveBeenCalled();
    } finally {
      publish.mockRestore();
    }
  });

  it.each(["root", "identity"])("refuses a mirror colliding with this directory's %s", async (field) => {
    const { directory, participant, read } = await setup();
    if (field === "root") directory.options.rootId = participant.id;
    else directory.options.identity = { ...directory.options.identity, id: participant.id };
    expect(read()).toBeUndefined();
  });

  it("refuses a mirrored owner claiming this directory's host id", async () => {
    const { writer, identity, participant, host, putParticipant, read } = await setup();
    await writer.put({
      key: keyFor("topology/hosts/", "local-host"), identity,
      value: { ...host, id: "local-host" },
    });
    await putParticipant({ ownerHostId: "local-host" });
    const directory = new ParticipantDirectory(writer, {
      enabled: true, hostId: "local-host", rootId: "local-root",
      identity: { id: "local-identity", name: "main", kind: "main" },
    });
    expect(directory.mirroredControlOwner("local-host", identity.id, participant.id)).toBeUndefined();
    expect(read()).toBeUndefined();
  });
});

// smarty-dev#816: heartbeats were 78% of all writes under the one mesh lock, each a rewrite of
// the whole shared state. Hosts also renew a file lease of their own, without the lock.
describe("ParticipantDirectory host leases", () => {
  const identityOf = (name: string): MeshIdentity => ({ id: `session:${name}`, name: "main", kind: "main", sessionId: name });
  const setup = async (policy: boolean) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
    if (policy) {
      await store.put({ key: LIVENESS_POLICY_KEY, value: { version: 1, hostLeases: "files" }, identity: identityOf("owner") });
    }
    const alpha = createDirectory(meshRoot, identityOf("alpha"), "session:alpha", () => [rootRecord("session:alpha", "session:alpha", "alpha")]);
    const beta = createDirectory(meshRoot, identityOf("beta"), "session:beta", () => [rootRecord("session:beta", "session:beta", "beta")]);
    await alpha.start();
    await beta.start();
    const hostEntry = () => store.listAll("topology/hosts/", { fresh: true })
      .find((entry) => (entry.value as { id?: string }).id === "session:alpha");
    return { meshRoot, store, alpha, beta, hostEntry };
  };
  const alphaBatches = (spy: { mock: { calls: unknown[][] } }) =>
    spy.mock.calls.filter((call) => (call[0] as { identity: MeshIdentity }).identity.id === "session:alpha").length;

  it("keeps half-TTL state renewals for an advertising state-only peer", async () => {
    const { meshRoot, store, hostEntry } = await setup(false);
    const identity = identityOf("legacy-reader");
    await store.put({ key: "sessions/legacy-reader", identity, value: { id: identity.id, sessionId: "legacy-reader",
      cwd: "/tmp/project", startedAt: Date.now(), status: "idle" } });
    const before = hostEntry()!.updatedAt;
    await new Promise((resolve) => setTimeout(resolve, 450));      // several 100 ms heartbeats
    expect(readHostLeases(meshRoot).get("session:alpha")?.expiresAt).toBeGreaterThan(Date.now());
    expect(hostEntry()!.updatedAt).toBeGreaterThan(before);
  });

  it("under the policy, renew only the file lease, and peers still see the host live", async () => {
    const { beta, hostEntry } = await setup(true);
    const batches = vi.spyOn(MeshStore.prototype, "writeBatch");
    const shared = hostEntry()!;
    await new Promise((resolve) => setTimeout(resolve, 600));      // twice the 300 ms lease
    expect(alphaBatches(batches)).toBe(0);                         // no locked write for a renewal
    expect(hostEntry()!.version).toBe(shared.version);
    expect((hostEntry()!.value as { expiresAt: number }).expiresAt).toBeLessThan(Date.now());
    // includeStale keeps the participant's own record, not the legacy session entry (15 s lease).
    expect(beta.list({ scope: "project", includeStale: true }).find((participant) => participant.id === "session:alpha"))
      .toMatchObject({ stale: false });
    batches.mockRestore();
  });

  // review/astra F1 on #68: a file-only renewal must not certify that the shared state is
  // writable; otherwise a live peer blocked behind a held lock reads as departed (#24).
  it("under the policy, never settle a peer that lapsed behind a held lock, and report the stall", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    await new MeshStore(meshRoot, 64 * 1024, 1_000).put({
      key: LIVENESS_POLICY_KEY, value: { version: 1, hostLeases: "files" }, identity: identityOf("owner"),
    });
    let peerStatus: "idle" | "running" = "idle";
    const make = (name: string, timing: { heartbeatMs: number; leaseMs: number }) => {
      // A non-main peer: a main also writes a legacy session entry with a fixed 15 s lease.
      const identity: MeshIdentity = { id: `session:${name}`, name: "main", kind: name === "peer" ? "actor" : "main", sessionId: name };
      const directory = new ParticipantDirectory(new MeshStore(meshRoot, 64 * 1024, 1_000, { lockTimeoutMs: 10_000 }), {
        enabled: true, hostId: identity.id, rootId: identity.id, identity, ...timing,
      });
      directory.registerSource(() => [{ ...rootRecord(identity.id, identity.id, name), ...(name === "peer" ? { status: peerStatus } : {}) }]);
      directories.push(directory);
      return directory;
    };
    const reader = make("reader", { heartbeatMs: 100, leaseMs: 2_000 });
    const peer = make("peer", { heartbeatMs: 100, leaseMs: 400 });
    await Promise.all([reader.start(), peer.start()]);
    const seesPeer = () => reader.peers().some((candidate) => candidate.id === "session:peer");
    await vi.waitFor(() => expect(seesPeer()).toBe(true), { timeout: 5_000, interval: 20 });
    const lockPath = path.join(meshRoot, ".lock");
    fs.mkdirSync(lockPath, { mode: 0o700 });
    fs.writeFileSync(path.join(lockPath, "owner"), `stuck\n${process.pid}\n${Date.now()}\n`);
    try {
      peerStatus = "running";
      void peer.refresh().catch(() => undefined);                 // renews its file, then blocks
      let result: PeerSettleResult | undefined;
      void awaitPeerSettle({
        poll: () => reader.peers(),
        stalled: () => reader.writeStalled(),
        confirmedAt: () => reader.confirmedAt(),
        selector: "session:peer",
        settledForMs: 60_000,
        pollMs: 20,
      }).then((settled) => { result = settled; });
      await vi.waitFor(() => expect(seesPeer()).toBe(false), { timeout: 3_000, interval: 10 });   // its lease lapsed
      await vi.waitFor(() => expect(result).toBeDefined(), { timeout: 5_000, interval: 20 });
      expect(result).toEqual({ ok: false, error: expect.stringMatching(/peer lease lapsed/) });
    } finally {
      fs.rmSync(lockPath, { recursive: true, force: true });
    }
  });

  // review/astra F2 on #68: production stores cache reads (2 s). A confirmation must not leave a
  // snapshot from before it, or peer-settle certifies a peer list that is already out of date.
  it("under the policy, read peers after a file-only confirmation, not from an earlier cache", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    await new MeshStore(meshRoot, 64 * 1024, 1_000).put({
      key: LIVENESS_POLICY_KEY, value: { version: 1, hostLeases: "files" }, identity: identityOf("owner"),
    });
    const make = (name: string, status: "idle" | "running") => {
      const identity: MeshIdentity = { id: `session:${name}`, name: "main", kind: name === "observer" ? "main" : "actor", sessionId: name };
      const directory = new ParticipantDirectory(new MeshStore(meshRoot, 64 * 1024, 1_000, { readCacheMs: 60_000 }), {
        enabled: true, hostId: identity.id, rootId: identity.id, identity, heartbeatMs: 100, leaseMs: 2_000,
      });
      directory.registerSource(() => [{ ...rootRecord(identity.id, identity.id, name), status }]);
      directories.push(directory);
      return directory;
    };
    const observer = make("observer", "idle");
    await observer.start();
    await new Promise((resolve) => setTimeout(resolve, 300));        // file-only heartbeats now
    expect(observer.peers()).toEqual([]);                           // primes the 60 s cached view
    const joined = make("joined", "running");
    await joined.start();                                           // a running peer joins
    let result: PeerSettleResult | undefined;
    void awaitPeerSettle({
      poll: () => observer.peers(),
      stalled: () => observer.writeStalled(),
      confirmedAt: () => observer.confirmedAt(),
      settledForMs: 60_000,
      pollMs: 20,
    }).then((settled) => { result = settled; });
    await vi.waitFor(() => expect(observer.peers().map((peer) => peer.id)).toEqual(["session:joined"]), { timeout: 2_000, interval: 20 });
    await new Promise((resolve) => setTimeout(resolve, 400));        // several confirmed heartbeats
    expect(result).toBeUndefined();                                 // still waiting for the running peer
  });

  // #411 R1: a lock acquisition can outlast the pre-confirmation renewal decision.
  it.each([
    { crossing: "host threshold", leaseMs: 4_000, delayMs: 2_001, policy: false, legacy: true },
    { crossing: "host expiry", leaseMs: 4_000, delayMs: 4_001, policy: false, legacy: true },
    { crossing: "legacy threshold", leaseMs: 60_000, delayMs: 7_501, policy: false, legacy: true },
    { crossing: "legacy expiry", leaseMs: 60_000, delayMs: 15_001, policy: false, legacy: true },
    { crossing: "all-new minute delay", leaseMs: 60_000, delayMs: 60_001, policy: false, legacy: false },
    { crossing: "all-new policy delay", leaseMs: 60_000, delayMs: STATE_LEASE_RENEW_MS, policy: true, legacy: false },
    { crossing: "mixed policy delay", leaseMs: 60_000, delayMs: STATE_LEASE_RENEW_MS, policy: true, legacy: true },
  ])("renews after delayed confirmation crosses $crossing", async ({ leaseMs, delayMs, policy, legacy }) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-delayed-confirm-"));
    roots.push(root);
    const identity = identityOf("delayed");
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 1_000, { lockTimeoutMs: 5_000 });
    const directory = new ParticipantDirectory(mesh, {
      enabled: true, hostId: identity.id, rootId: identity.id, identity,
      heartbeatMs: 1_000, leaseMs, reapDeadHosts: false,
    });
    directories.push(directory);
    directory.registerSource(() => [rootRecord(identity.id, identity.id, "delayed")]);
    let now = Date.now();
    const clock = vi.spyOn(Date, "now").mockImplementation(() => now);
    const lockPath = path.join(mesh.root, ".lock");
    try {
      if (policy) await mesh.put({ key: LIVENESS_POLICY_KEY, value: { version: 1, hostLeases: "files" }, identity });
      if (legacy) {
        const peer = identityOf("state-only");
        await mesh.put({ key: "sessions/state-only", identity: peer, value: { id: peer.id, sessionId: "state-only",
          cwd: "/tmp/project", startedAt: now, status: "idle" } });
        // Keep this compatibility reader alive across the deterministic lock delay.
        await mesh.put({ key: "topology/hosts/" + createHash("sha256").update(peer.id).digest("hex"), identity: peer,
          value: { format: 1, id: peer.id, rootId: peer.id, identity: peer, startedAt: now, updatedAt: now, expiresAt: now + 1_000_000 } });
      }
      await directory.refresh(); // no heartbeat timer: only this awaited refresh can renew
      const hostBefore = mesh.listAll("topology/hosts/").find(entry => (entry.value as { id: string }).id === identity.id)!;
      const legacyBefore = mesh.get("sessions/delayed")!;
      const participantBefore = mesh.listAll("topology/participants/")[0]!;
      const writes = vi.spyOn(mesh, "writeBatch");
      const confirm = mesh.confirmWritable.bind(mesh);
      let entered!: () => void;
      const confirming = new Promise<void>((resolve) => { entered = resolve; });
      const confirmations = vi.spyOn(mesh, "confirmWritable").mockImplementation(async onAcquired => {
        const pending = confirm(onAcquired); // the real acquisition waits behind a live lock
        entered();
        await pending;
      });
      fs.mkdirSync(lockPath, { mode: 0o700 });
      fs.writeFileSync(path.join(lockPath, "owner"), `delayed\n${process.pid}\n${now}\n`);
      let settled = false;
      const refresh = directory.refresh().then(() => { settled = true; });
      await confirming;
      await new Promise((resolve) => setTimeout(resolve, 30));
      expect(settled).toBe(false);
      now += delayMs; // deterministic elapsed time during the actual lock wait
      fs.rmSync(lockPath, { recursive: true, force: true });
      await refresh;
      expect(confirmations).toHaveBeenCalledOnce();
      const hostAfter = mesh.listAll("topology/hosts/", { fresh: true }).find(entry => (entry.value as { id: string }).id === identity.id)!;
      const legacyAfter = mesh.get("sessions/delayed", { fresh: true })!;
      if (legacy) {
        const hostDue = policy ? delayMs >= STATE_LEASE_RENEW_MS : delayMs >= leaseMs / 2;
        const sessionDue = !policy && delayMs >= 7_500;
        expect(writes).toHaveBeenCalledTimes(hostDue || sessionDue ? 1 : 0);
        if (hostDue) {
          expect(hostAfter.version).toBeGreaterThan(hostBefore.version);
          expect(hostAfter.value).toMatchObject({ updatedAt: now, expiresAt: now + leaseMs });
        } else expect(hostAfter).toEqual(hostBefore);
        if (sessionDue) {
          expect(legacyAfter.version).toBeGreaterThan(legacyBefore.version);
          expect(legacyAfter.updatedAt).toBe(now);
        } else expect(legacyAfter).toEqual(legacyBefore);
      } else {
        // Thresholds no longer require state commits when every live reader uses lease files.
        expect(writes).not.toHaveBeenCalled();
        expect(hostAfter).toEqual(hostBefore);
        expect(legacyAfter).toEqual(legacyBefore);
      }
      expect(readHostLeases(mesh.root).get(identity.id)).toMatchObject({ updatedAt: now, expiresAt: now + leaseMs });
      if (policy) expect(legacyAfter).toBeUndefined();
      // smarty-dev#13468: the explicit 1 s heartbeat retains the fixed 15 s legacy session TTL.
      else expect(sessionLiveness(legacyAfter, mesh.root)).toEqual({ updatedAt: now, expiresAt: now + 15_000 });
      expect(mesh.listAll("topology/participants/", { fresh: true })[0]).toEqual(participantBefore);
      expect(directory.confirmedAt()).toBe(now);
    } finally {
      fs.rmSync(lockPath, { recursive: true, force: true });
      clock.mockRestore();
      vi.restoreAllMocks();
    }
  });

  it("renews idle state leases at half-life without rewriting timestamp-only participants, and commits real changes immediately", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-idle-"));
    roots.push(root);
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 1_000);
    const identity = identityOf("idle");
    let status = "idle";
    const record = () => ({ ...rootRecord(identity.id, identity.id, "idle"), status, updatedAt: Date.now() });
    const directory = new ParticipantDirectory(mesh, { enabled: true, hostId: identity.id, rootId: identity.id,
      identity, heartbeatMs: 5_000, leaseMs: 15_000, reapDeadHosts: false });
    directory.registerSource(() => [record()]);
    directories.push(directory);
    const observer = createDirectory(mesh.root, identityOf("observer"), "session:observer", () => []);
    let now = Date.now();
    const clock = vi.spyOn(Date, "now").mockImplementation(() => now);
    const trace = path.join(root, "commits.jsonl");
    vi.stubEnv("PI_FABRIC_COMMIT_TRACE", trace);
    try {
      // Keep an actual old reader live: this test retains the legacy half-life contract.
      const peer = identityOf("state-only");
      await mesh.put({ key: "topology/hosts/" + createHash("sha256").update(peer.id).digest("hex"), identity: peer,
        value: { format: 1, id: peer.id, rootId: peer.id, identity: peer, startedAt: now, updatedAt: now, expiresAt: now + 600_000 } });
      await directory.refresh();
      fs.writeFileSync(trace, "");
      const participant = mesh.listAll("topology/participants/")[0]!;
      const start = now;
      for (let tick = 1; tick <= 24; tick++) {
        now = start + tick * 5_000;
        await directory.refresh();
        // A legacy reader has no host-file fallback: its fixed 15 s TTL must never lapse.
        expect(now - mesh.get("sessions/idle", { fresh: true })!.updatedAt).toBeLessThan(15_000);
        // Read the canonical file here; fake wall-clock jumps do not change filesystem clocks.
        const lease = JSON.parse(fs.readFileSync(hostLeasePath(mesh.root, identity.id), "utf8"));
        expect(lease.expiresAt).toBe(now + 15_000);
        expect(observer.get(identity.id, now, { fresh: true })?.stale).toBe(false);
      }
      const commits = fs.readFileSync(trace, "utf8").trim().split("\n").map(line => JSON.parse(line));
      expect(commits).toHaveLength(12);
      expect(commits.map(commit => commit.at - start)).toEqual(
        Array.from({ length: 12 }, (_, index) => (index + 1) * 10_000),
      );
      expect(commits.every(commit => commit.keys.length === 2 && commit.keys.includes("sessions/idle"))).toBe(true);
      expect(mesh.get(participant.key)).toEqual(participant); // timestamp-only participant unchanged
      now += 15_001;
      expect(observer.get(identity.id, now, { fresh: true })).toBeUndefined(); // original TTL lapses
      await directory.refresh();
      expect(observer.get(identity.id, now, { fresh: true })?.stale).toBe(false); // re-acquire via file
      expect(fs.readFileSync(trace, "utf8").trim().split("\n")).toHaveLength(13);
      status = "running";
      await directory.refresh();
      expect(mesh.get(participant.key)?.value).toMatchObject({ status: "running" });
      const changed = fs.readFileSync(trace, "utf8").trim().split("\n").map(line => JSON.parse(line));
      expect(changed).toHaveLength(14);
      expect(changed[13].at).toBe(now); // no lease delay for durable state/authority changes
      expect(changed[13].keys).toContain(participant.key);
    } finally { clock.mockRestore(); vi.unstubAllEnvs(); }
  });

  it("renews at the stored host half-life even with a matching file lease", async () => {
    const { store, alpha, hostEntry, meshRoot } = await setup(false);
    const peer = identityOf("state-only-host");
    const now = Date.now();
    await store.put({ key: "topology/hosts/" + createHash("sha256").update(peer.id).digest("hex"), identity: peer,
      value: { format: 1, id: peer.id, rootId: peer.id, identity: peer, startedAt: now, updatedAt: now, expiresAt: now + 120_000 } });
    writeHostLease(meshRoot, { id: peer.id, rootId: "wrong-root", identityId: peer.id, updatedAt: now, expiresAt: now + 120_000 });
    const before = hostEntry()!;
    const clock = vi.spyOn(Date, "now").mockReturnValue(now + 200);
    try {
      await alpha.refresh();
      expect(hostEntry()!.version).toBeGreaterThan(before.version);
      writeHostLease(meshRoot, { id: peer.id, rootId: peer.id, identityId: peer.id, updatedAt: now, expiresAt: now + 120_000 });
      const shared = hostEntry()!;
      clock.mockReturnValue(now + 400);
      await alpha.refresh();
      expect(hostEntry()!.version).toBeGreaterThan(shared.version);
    } finally { clock.mockRestore(); }
  });

  it("all-new peers need no periodic shared record renewal, even under the old policy", async () => {
    const { hostEntry, alpha, meshRoot } = await setup(true);
    const shared = hostEntry()!;
    const now = Date.now;
    const clock = vi.spyOn(Date, "now").mockImplementation(() => now() + STATE_LEASE_RENEW_MS);
    try {
      await alpha.refresh();
      expect(hostEntry()).toEqual(shared);
      expect(readHostLeases(meshRoot).get("session:alpha")?.updatedAt).toBeGreaterThan(shared.updatedAt);
      expect(alpha.get("session:alpha")?.stale).toBe(false);
    } finally { clock.mockRestore(); }
  });
});

// smarty-dev#816: more than half of the participant rewrites of the shared state carried only
// activity counters. Those ride along with another write, or at most once a minute.
describe("ParticipantDirectory activity counters", () => {
  const identityOf = (name: string): MeshIdentity => ({ id: `session:${name}`, name: "main", kind: "main", sessionId: name });
  const setup = async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
    await store.put({ key: LIVENESS_POLICY_KEY, value: { version: 1, hostLeases: "files" }, identity: identityOf("owner") });
    const worker = { status: "running", turns: 0, toolCalls: 0, calls: 0 };
    // Every read of the source is new model activity: the counters and the current tool move.
    // Actor queue/mailbox counts are operational state, covered separately below.
    const alpha = createDirectory(meshRoot, identityOf("alpha"), "session:alpha", () => {
      worker.calls += 1;
      worker.turns += 1;
      worker.toolCalls += 2;
      return [
        rootRecord("session:alpha", "session:alpha", "alpha"),
        {
          ...agentRecord("agent:worker", "session:alpha", "session:alpha", "session:alpha"),
          status: worker.status,
          currentTool: `tool-${worker.calls}`,
          turns: worker.turns,
          toolCalls: worker.toolCalls,
          usage: { input: worker.turns * 10, output: worker.turns, cacheRead: 0, cacheWrite: 0, cost: worker.turns / 100 },
          updatedAt: Date.now(),
        },
      ];
    });
    const beta = createDirectory(meshRoot, identityOf("beta"), "session:beta", () => [rootRecord("session:beta", "session:beta", "beta")]);
    await alpha.start();
    await beta.start();
    const shared = () => store.listAll("topology/participants/", { fresh: true })
      .map((entry) => entry.value as FabricParticipantRecord)
      .find((participant) => participant.id === "agent:worker")!;
    const batches = vi.spyOn(MeshStore.prototype, "writeBatch");
    const alphaBatches = () => batches.mock.calls
      .filter((call) => (call[0] as { identity: MeshIdentity }).identity.id === "session:alpha").length;
    return { alpha, beta, worker, shared, batches, alphaBatches };
  };

  it("writes nothing for counter-only changes over many heartbeats and change refreshes", async () => {
    const { alpha, worker, shared, batches, alphaBatches } = await setup();
    const before = shared();
    const calls = worker.calls;
    for (let index = 0; index < 8; index++) {
      alpha.scheduleRefresh();
      await new Promise((resolve) => setTimeout(resolve, 100));    // 100 ms heartbeats
    }
    expect(worker.calls - calls).toBeGreaterThanOrEqual(5);        // the heartbeats did read new counters
    expect(alphaBatches()).toBe(0);
    expect(shared()).toEqual(before);
    batches.mockRestore();
  });

  it("writes a status change at once, with the latest counters", async () => {
    const { alpha, worker, shared, batches, alphaBatches } = await setup();
    worker.status = "completed";
    await alpha.refresh();
    expect(alphaBatches()).toBe(1);
    expect(shared()).toMatchObject({ status: "completed", turns: worker.turns, toolCalls: worker.toolCalls });
    batches.mockRestore();
  });

  it("carries the counters to a peer within ACTIVITY_REFRESH_MS", async () => {
    const { beta, worker, shared, batches, alphaBatches } = await setup();
    const before = shared().turns!;
    const now = Date.now;
    vi.spyOn(Date, "now").mockImplementation(() => now() + 60_000);
    await vi.waitFor(() => expect(shared().turns).toBeGreaterThan(before), { timeout: 2_000, interval: 20 });
    const seen = beta.get("agent:worker")!;
    expect(seen).toMatchObject({ stale: false, status: "running" });
    expect(seen.turns).toBeGreaterThan(before);
    expect(seen.currentTool).toMatch(/^tool-\d+$/);
    expect(seen.usage?.input).toBe(seen.turns! * 10);
    // One carrying write, then quiet again for the next minute.
    const carried = alphaBatches();
    await new Promise((resolve) => setTimeout(resolve, 400));
    expect(alphaBatches()).toBe(carried);
    vi.restoreAllMocks();
    batches.mockRestore();
  });
});

// #2726 / Astra F1: queue and mailbox counts are operational state, not model activity.
// Both provider reads consume this fresh row; their overlay mapping is covered separately in
// agents-provider.test.ts. Exercise the real mapper, publisher and filesystem reader here.
describe("ParticipantDirectory actor operational counters", () => {
  describe.each([
    { version: 1, hostLeases: "files" },
    { version: 1, hostLeases: "files", participants: "files" },
  ])("policy %j", (policy) => {
    const setup = async () => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-actor-counters-"));
      roots.push(root);
      const meshRoot = path.join(root, "mesh");
      const identity: MeshIdentity = { id: "session:owner", name: "main", kind: "main", sessionId: "owner" };
      const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
      await store.put({ key: LIVENESS_POLICY_KEY, value: policy, identity });
      const actor: FabricActorInfo = {
        filterSkipped: { count: 0, lastKey: null, lastTopic: null, lastAt: null },
        id: "actor:running", scope: "project", name: "running", rootId: identity.id,
        status: "running", residency: "durable", runner: "pi", events: [], topics: [],
        delivery: "mailbox", responseMode: "text", triggerTurn: false, coalesce: false,
        queued: 0, messages: 1, createdAt: 1, updatedAt: 2,
        inFlightRun: { id: "stable-run", startedAt: 2, ageS: 0 },
      };
      // No clock jump, root transition, child or new run can incidentally flush counts.
      // The heartbeat is a minute away; only the awaited ordinary refreshes publish updates.
      const timing = { heartbeatMs: 60_000, leaseMs: 120_000 };
      const owner = createDirectory(meshRoot, identity, identity.id, () => [
        rootRecord(identity.id, identity.id, "owner"),
        actorParticipantRecord(actor, identity.id, identity.id, identity.id, identity.id),
      ], timing);
      const readerIdentity: MeshIdentity = { id: "session:reader", name: "main", kind: "main", sessionId: "reader" };
      // A separate store/directory, with no owner-memory shortcut or stubbed get().
      const reader = createDirectory(meshRoot, readerIdentity, readerIdentity.id, () => [], timing);
      await owner.start();
      const read = () => reader.get(actor.id, undefined, { fresh: true });
      const stable = {
        id: actor.id, kind: "actor", status: "running", ownerHostId: identity.id,
        local: false, stale: false, actorRun: { id: "stable-run", startedAt: 2 },
      };
      expect(read()).toMatchObject({ ...stable, actorQueued: 0, actorMessages: 1 });
      // Assert that the requested policy really selected state+file vs file-only publication.
      expect(store.listAll("topology/participants/", { fresh: true }).length)
        .toBe(policy.participants === "files" ? 0 : 2);
      return { actor, owner, read, stable };
    };

    it.each([
      { change: "queue/message", counts: [[1, 2], [2, 3]] },
      { change: "message-only", counts: [[0, 2], [0, 3]] },
      { change: "queue-only", counts: [[1, 1], [2, 1]] },
    ])("publishes two successive $change updates on the same running activation", async ({ counts }) => {
      const { actor, owner, read, stable } = await setup();
      for (const [queued, messages] of counts) {
        actor.queued = queued!;
        actor.messages = messages!;
        actor.updatedAt += 1;
        await owner.refresh();
        // Soft assertions retain evidence for BOTH refreshes even when the first is RED.
        expect.soft(read()).toMatchObject({ ...stable, actorQueued: queued, actorMessages: messages });
      }
    });
  });
});

describe("ParticipantDirectory ownership deadlines", () => {
  it.each(["lease", "reload"] as const)("bounds poll reuse by the observed %s deadline and observes renewal", async kind => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-ownership-deadline-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const identity: MeshIdentity = { id: "session:owner", name: "main", kind: "main", sessionId: "owner" };
    const actorId = "actor:deadline";
    let reloadUntil = Date.now() + 10_000;
    const owner = createDirectory(meshRoot, identity, identity.id, () => [{
      ...agentRecord(actorId, identity.id, identity.id, identity.id), kind: "actor", transport: "host",
      ...(kind === "reload" ? { status: "reloading", reloadUntil } : {}),
    }], steadyLease);
    const reader = createDirectory(meshRoot, { id: "session:reader", name: "main", kind: "main" }, "session:reader", () => [], steadyLease);
    await owner.refresh(); // No heartbeat: only explicit renewals may change the observed deadline.
    const options = { actorIds: [actorId] };
    const fileLease = readHostLeases(meshRoot).get(identity.id)!;
    const deadline = kind === "reload" ? reloadUntil : fileLease.expiresAt;
    let now = Date.now();
    const clock = vi.spyOn(Date, "now").mockImplementation(() => now);
    const first = reader.actorOwnership(identity.id, options);
    expect(first.get(actorId)).toBe(true);
    expect(first.observedDeadline()).toBe(deadline);
    expect(first.observedDeadline([actorId])).toBe(deadline);
    now = deadline - 1;
    try {
      expect(reader.actorOwnership(identity.id, options).get(actorId)).toBe(true);
      now = deadline + 1;
      expect(reader.actorOwnership(identity.id, options).has(actorId)).toBe(false);
      // A renewal must produce a new observation, never extend the old map's authority.
      reloadUntil = now + 30_000;
      await owner.refresh();
      const renewed = reader.actorOwnership(identity.id, options);
      expect(renewed.get(actorId)).toBe(true);
      expect(renewed.observedDeadline()).toBeGreaterThan(deadline);
      expect(first.observedDeadline()).toBe(deadline);
    } finally { clock.mockRestore(); }
  });
});

// smarty-dev#784: actor ownership checks call get() per actor; it listed and cloned the whole
// directory each time.
describe("ParticipantDirectory.get", () => {
  it("reads one live participant without listing the directory, with the same answer as the list", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const alphaIdentity: MeshIdentity = { id: "session:alpha", name: "main", kind: "main", sessionId: "alpha" };
    const betaIdentity: MeshIdentity = { id: "session:beta", name: "main", kind: "main", sessionId: "beta" };
    const alpha = createDirectory(meshRoot, alphaIdentity, alphaIdentity.id, () => [
      rootRecord(alphaIdentity.id, alphaIdentity.id, "alpha"),
      agentRecord("agent:alpha-child", alphaIdentity.id, alphaIdentity.id, alphaIdentity.id),
    ]);
    const beta = createDirectory(meshRoot, betaIdentity, betaIdentity.id, () => [rootRecord(betaIdentity.id, betaIdentity.id, "beta")]);
    await alpha.start();
    await beta.start();
    const expected = (id: string) => beta.list({ scope: "project" }).find((participant) => participant.id === id);
    const listed = vi.spyOn(beta, "list");
    for (const id of ["agent:alpha-child", "session:alpha", "session:beta"]) {
      listed.mockClear();
      const found = beta.get(id);
      expect(listed).not.toHaveBeenCalled();
      listed.mockRestore();
      expect(found).toEqual(expected(id));
      vi.spyOn(beta, "list");
    }
    const fallback = vi.spyOn(beta, "list");
    expect(beta.get("agent:unknown")).toBeUndefined();                // absent: the full list decides
    expect(fallback).toHaveBeenCalled();
    fallback.mockRestore();
  });

  it("finds a participant whose host is alive only by its file lease, without listing", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const owner: MeshIdentity = { id: "session:owner", name: "main", kind: "main", sessionId: "owner" };
    await new MeshStore(meshRoot, 64 * 1024, 1_000).put({ key: LIVENESS_POLICY_KEY, value: { version: 1, hostLeases: "files" }, identity: owner });
    const alphaIdentity: MeshIdentity = { id: "session:alpha", name: "main", kind: "main", sessionId: "alpha" };
    const betaIdentity: MeshIdentity = { id: "session:beta", name: "main", kind: "main", sessionId: "beta" };
    const alpha = createDirectory(meshRoot, alphaIdentity, alphaIdentity.id, () => [
      agentRecord("agent:alpha-child", alphaIdentity.id, alphaIdentity.id, alphaIdentity.id),
    ]);
    const beta = createDirectory(meshRoot, betaIdentity, betaIdentity.id, () => [rootRecord(betaIdentity.id, betaIdentity.id, "beta")]);
    await alpha.start();
    await beta.start();
    await new Promise((resolve) => setTimeout(resolve, 600));        // past alpha's 300 ms shared lease
    // A heartbeat just now renews only the file lease; a stalled runner timer must not let it lapse
    // before the read (pi-fabric#142 CI: failed once on ubuntu-latest).
    await alpha.refresh();
    const listed = vi.spyOn(beta, "list");
    expect(beta.get("agent:alpha-child")).toMatchObject({ id: "agent:alpha-child", stale: false });
    expect(listed).not.toHaveBeenCalled();
    listed.mockRestore();
  });
});

// smarty-dev#784: roots publish their role and project, and peers show them.
describe("ParticipantDirectory role and project", () => {
  it("publishes normalized origin and marks print/JSON roots as discovery-only observers", async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-origin-presence-"));
    roots.push(dir);
    const git = (...args: string[]) => execFileSync("git", args, { cwd: dir, stdio: "ignore" });
    git("init", "-q");
    git("remote", "add", "origin", "git@github.com:Smarty-Pants-Inc/pi-fabric.git");
    const identity: MeshIdentity = { id: "session:audit", name: "main", kind: "main", sessionId: "audit" };
    let alpha: ParticipantDirectory;
    const info = { id: identity.id, name: "Main", kind: "main", status: "idle", runner: "pi", transport: "host",
      cwd: dir, sessionId: "audit", startedAt: 1, updatedAt: 2, pendingMessages: false, local: true } as const;
    alpha = createDirectory(path.join(dir, "mesh"), identity, identity.id, () => [alpha.root(info, false)], steadyLease);
    await alpha.start();
    expect(alpha.get(identity.id)).toMatchObject({ repository: "github.com/smarty-pants-inc/pi-fabric", interactive: false, capabilities: ["fabric"] });
    expect(alpha.root(info, true)).toMatchObject({ interactive: true, capabilities: ["steer", "followUp", "fabric"], mainBindings: false });
    const reader = createDirectory(path.join(dir, "mesh"), { id: "session:reader", name: "main", kind: "main" }, "session:reader", () => [], steadyLease);
    expect(reader.peers()).toEqual([expect.objectContaining({ id: identity.id, repository: "github.com/smarty-pants-inc/pi-fabric", interactive: false })]);
  });


  it("publishes a root's role and project, and shows them on peers", async () => {
    const previous = process.env.SMARTY_ROLE;
    process.env.SMARTY_ROLE = "project-agent@5358e96a418f";
    try {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
      roots.push(root);
      const meshRoot = path.join(root, "mesh");
      const alphaIdentity: MeshIdentity = { id: "session:alpha", name: "main", kind: "main", sessionId: "alpha" };
      const betaIdentity: MeshIdentity = { id: "session:beta", name: "main", kind: "main", sessionId: "beta" };
      let alpha: ParticipantDirectory | undefined;
      alpha = createDirectory(meshRoot, alphaIdentity, alphaIdentity.id, () => [alpha!.root({
        id: alphaIdentity.id, name: "main", kind: "main", status: "idle", runner: "pi", transport: "host",
        cwd: process.cwd(), sessionId: "alpha", startedAt: 1, updatedAt: 2, pendingMessages: false, local: true,
      } as never)], steadyLease);
      const beta = createDirectory(meshRoot, betaIdentity, betaIdentity.id, () => [rootRecord(betaIdentity.id, betaIdentity.id, "beta")], steadyLease);
      await alpha.start();
      await beta.start();
      expect(beta.peers().find((peer) => peer.id === "session:alpha")).toMatchObject({
        role: "project-agent", project: projectOf(process.cwd()),
      });
    } finally {
      if (previous === undefined) delete process.env.SMARTY_ROLE;
      else process.env.SMARTY_ROLE = previous;
    }
  });
});

describe("ParticipantDirectory root project", () => {
  // smarty-dev#977: a lead whose cwd is a worktree of another repository publishes the project it
  // names, so it is not taken for a project agent of that repository.
  it("publishes PI_FABRIC_PROJECT as the root's project instead of its cwd's", async () => {
    const previous = { role: process.env.SMARTY_ROLE, project: process.env.PI_FABRIC_PROJECT };
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
    roots.push(root);
    const own = path.join(root, "own-project");
    fs.mkdirSync(own);
    // A lane-local TMPDIR may sit inside another checkout: make this fixture a distinct project.
    fs.mkdirSync(path.join(own, ".git"));
    process.env.SMARTY_ROLE = "project-agent@5358e96a418f";
    process.env.PI_FABRIC_PROJECT = own;
    try {
      const meshRoot = path.join(root, "mesh");
      const alphaIdentity: MeshIdentity = { id: "session:alpha", name: "main", kind: "main", sessionId: "alpha" };
      const betaIdentity: MeshIdentity = { id: "session:beta", name: "main", kind: "main", sessionId: "beta" };
      let alpha: ParticipantDirectory | undefined;
      alpha = createDirectory(meshRoot, alphaIdentity, alphaIdentity.id, () => [alpha!.root({
        id: alphaIdentity.id, name: "main", kind: "main", status: "idle", runner: "pi", transport: "host",
        cwd: process.cwd(), sessionId: "alpha", startedAt: 1, updatedAt: 2, pendingMessages: false, local: true,
      } as never)], steadyLease);
      const beta = createDirectory(meshRoot, betaIdentity, betaIdentity.id, () => [rootRecord(betaIdentity.id, betaIdentity.id, "beta")], steadyLease);
      await alpha.start();
      await beta.start();
      const peer = beta.peers().find((candidate) => candidate.id === "session:alpha");
      expect(peer).toMatchObject({ role: "project-agent", project: projectOf(own), cwd: process.cwd() });
      expect(peer?.project).not.toBe(projectOf(process.cwd()));
    } finally {
      for (const [key, value] of [["SMARTY_ROLE", previous.role], ["PI_FABRIC_PROJECT", previous.project]] as const) {
        if (value === undefined) delete process.env[key];
        else process.env[key] = value;
      }
    }
  });
});

describe("ParticipantDirectory", () => {
  it("builds one project topology while preserving local ownership and lineage", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const alphaIdentity: MeshIdentity = {
      id: "session:alpha",
      name: "main",
      kind: "main",
      sessionId: "alpha",
    };
    const betaIdentity: MeshIdentity = {
      id: "session:beta",
      name: "main",
      kind: "main",
      sessionId: "beta",
    };
    const recursiveIdentity: MeshIdentity = {
      id: "agent:recursive",
      name: "recursive",
      kind: "agent",
      sessionId: "recursive-session",
    };
    // This tests live ownership and lineage, not expiry: keep all owners live across slow CI.
    const lease = { heartbeatMs: 60_000, leaseMs: 120_000 };
    const alpha = createDirectory(meshRoot, alphaIdentity, alphaIdentity.id, () => [
      rootRecord(alphaIdentity.id, alphaIdentity.id, "alpha"),
      agentRecord("agent:alpha-child", alphaIdentity.id, alphaIdentity.id, alphaIdentity.id),
    ], lease);
    const beta = createDirectory(meshRoot, betaIdentity, betaIdentity.id, () => [
      rootRecord(betaIdentity.id, betaIdentity.id, "beta"),
    ], lease);
    const recursive = createDirectory(meshRoot, recursiveIdentity, alphaIdentity.id, () => [
      agentRecord(
        "agent:grandchild",
        alphaIdentity.id,
        "runtime:recursive-session",
        recursiveIdentity.id,
      ),
    ], lease);

    await alpha.start();
    await beta.start();
    await recursive.start();

    expect(alpha.list({ scope: "project" }).map(({ id }) => id)).toEqual([
      "session:alpha",
      "session:beta",
      "agent:alpha-child",
      "agent:grandchild",
    ]);
    expect(alpha.list({ scope: "local" }).map(({ id }) => id)).toEqual([
      "session:alpha",
      "agent:alpha-child",
    ]);
    expect(alpha.list({ scope: "lineage" }).map(({ id }) => id)).toEqual([
      "session:alpha",
      "agent:alpha-child",
      "agent:grandchild",
    ]);
    expect(alpha.sessions()).toMatchObject([
      { id: "session:alpha", kind: "root", local: true },
      { id: "session:beta", kind: "root", local: false },
    ]);
    expect(alpha.peers()).toMatchObject([
      { id: "session:beta", name: "PRO-2", label: "PRO-2", kind: "peer", local: false },
    ]);
    expect(recursive.self()).toMatchObject({
      id: "agent:recursive",
      kind: "agent",
      rootId: "session:alpha",
    });
  });

  it("withdraws control capabilities before releasing its live host lease", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
    roots.push(root);
    const identity: MeshIdentity = {
      id: "session:quiesce",
      name: "main",
      kind: "main",
      sessionId: "quiesce",
    };
    const directory = createDirectory(path.join(root, "mesh"), identity, identity.id, () => [
      rootRecord(identity.id, identity.id, "quiesce"),
      agentRecord("agent:quiesce", identity.id, identity.id, identity.id),
    ]);

    await directory.start();
    await directory.quiesce();

    expect(directory.get("agent:quiesce")).toMatchObject({
      capabilities: [],
      local: true,
      stale: false,
    });
    // A root that is shutting down says so, for a clear steer error (smarty-dev#1113).
    expect(directory.get(identity.id)).toMatchObject({ capabilities: [], status: "stopping" });
    // Other roots still list it as a peer while it shuts down.
    const observerIdentity: MeshIdentity = { id: "session:observer", name: "main", kind: "main", sessionId: "observer" };
    const observer = createDirectory(path.join(root, "mesh"), observerIdentity, observerIdentity.id, () => [
      rootRecord(observerIdentity.id, observerIdentity.id, "observer"),
    ]);
    await observer.start();
    expect(observer.peers().map((peer) => peer.id)).toContain(identity.id);
    await observer.close();
    expect(directory.mesh.get("sessions/quiesce")).toBeUndefined();
  });

  it("stamps the host lease after participant writes that outlast it", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
    roots.push(root);
    const identity: MeshIdentity = {
      id: "session:slow",
      name: "main",
      kind: "main",
      sessionId: "slow",
    };
    // A lease of 1 s, and a write that outlasts it: the read after the commit keeps a 1 s margin,
    // which a starved Windows runner used up with a 300 ms lease (smarty-dev#883). The lease is
    // at least two heartbeats, so the heartbeat is 500 ms.
    const directory = createDirectory(path.join(root, "mesh"), identity, identity.id, () => [
      rootRecord(identity.id, identity.id, "slow"),
      agentRecord("agent:slow", identity.id, identity.id, identity.id),
    ], { heartbeatMs: 500, leaseMs: 1_000 });
    const write = directory.mesh.writeBatch.bind(directory.mesh);
    // A contended Windows mesh makes the heartbeat write outlast the lease;
    // the host lease is stamped at commit, so it must still be fresh afterwards.
    vi.spyOn(directory.mesh, "writeBatch").mockImplementation(async (input) => {
      await new Promise((resolve) => setTimeout(resolve, 1_100));
      return write(input);
    });

    await directory.start();

    expect(directory.get("agent:slow")).toMatchObject({
      capabilities: ["steer", "followUp", "stop"],
      local: true,
      stale: false,
    });
  });

  it("does not claim an actor still owned by a live legacy root", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const mesh = new MeshStore(meshRoot, 64 * 1024, 1_000);
    const oldIdentity: MeshIdentity = {
      id: "session:old",
      name: "main",
      kind: "main",
      sessionId: "old-session",
    };
    const session = await mesh.put({
      key: "sessions/old-session",
      value: {
        id: oldIdentity.id,
        name: "Peer old-sess",
        kind: "peer",
        status: "idle",
        runner: "pi",
        transport: "host",
        cwd: "/tmp/project",
        sessionId: "old-session",
        startedAt: 1,
        updatedAt: Date.now(),
        pendingMessages: false,
        local: false,
      },
      identity: oldIdentity,
    });
    await mesh.put({
      key: "actors/old-session/actor:legacy",
      value: {
        id: "actor:legacy",
        name: "legacy actor",
        status: "idle",
        runner: "pi",
        createdAt: 1,
      },
      identity: oldIdentity,
    });
    const identity: MeshIdentity = {
      id: "session:new",
      name: "main",
      kind: "main",
      sessionId: "new-session",
    };
    const directory = createDirectory(meshRoot, identity, identity.id, () => [
      rootRecord(identity.id, identity.id, "new-session"),
      {
        ...agentRecord("actor:legacy", identity.id, identity.id, identity.id),
        kind: "actor",
        transport: "host",
      },
    ]);

    await directory.start();
    expect(directory.get("actor:legacy")).toMatchObject({
      ownerHostId: oldIdentity.id,
      controlProtocol: "legacy",
      local: false,
    });

    await mesh.delete({ key: session.key, ifVersion: session.version });
    await directory.refresh();
    expect(directory.get("actor:legacy")).toMatchObject({
      ownerHostId: identity.id,
      controlProtocol: "v1",
      local: true,
    });
  });

  it("never shares agent prompts, results, or errors in participant state", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
    roots.push(root);
    const identity: MeshIdentity = {
      id: "session:private",
      name: "main",
      kind: "main",
      sessionId: "private",
    };
    const directory = createDirectory(path.join(root, "mesh"), identity, identity.id, () => [
      rootRecord(identity.id, identity.id, "private"),
      {
        ...agentRecord("agent:private", identity.id, identity.id, identity.id),
        task: "secret prompt",
        text: "secret result",
        error: "secret failure",
      } as FabricParticipantRecord,
      agentRecord("agent:wrong-lineage", "session:foreign", identity.id, identity.id),
    ], { heartbeatMs: 100, leaseMs: 30_000 });                          // no expiry here (smarty-dev#883)

    await directory.start();
    expect(directory.mesh.get("sessions/private")?.value).toMatchObject({
      id: identity.id,
      kind: "peer",
    });
    const participant = directory.get("agent:private") as unknown as Record<string, unknown>;
    expect(participant).not.toHaveProperty("task");
    expect(participant).not.toHaveProperty("text");
    expect(participant).not.toHaveProperty("error");
    expect(directory.get("agent:wrong-lineage")).toBeUndefined();
    expect(
      JSON.stringify(directory.mesh.listAll("topology/participants/")),
    ).not.toContain("secret");

    await directory.mesh.put({
      key: "topology/participants/not-a-canonical-hash",
      value: {
        ...agentRecord("agent:forged", identity.id, identity.id, identity.id),
        ownerIdentityId: identity.id,
      },
      identity,
    });
    expect(directory.get("agent:forged")).toBeUndefined();
    await directory.mesh.put({
      key: "sessions/claimed",
      value: {
        id: "session:victim",
        sessionId: "claimed",
        cwd: "/tmp/project",
        status: "idle",
        startedAt: 1,
      },
      identity,
    });
    expect(directory.get("session:victim")).toBeUndefined();
  });

  it("keeps one live execution owner for a colliding participant id", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const alphaIdentity: MeshIdentity = {
      id: "session:alpha",
      name: "main",
      kind: "main",
      sessionId: "alpha",
    };
    const betaIdentity: MeshIdentity = {
      id: "session:beta",
      name: "main",
      kind: "main",
      sessionId: "beta",
    };
    const shared = (identity: MeshIdentity): FabricParticipantRecord => ({
      ...agentRecord("actor:shared", identity.id, identity.id, identity.id),
      kind: "actor",
      transport: "host",
    });
    // The takeover follows alpha's close, not its expiry, so a slow runner must not expire alpha's
    // lease first and hand beta the record early (smarty-dev#883).
    const lease = { heartbeatMs: 100, leaseMs: 30_000 };
    const alpha = createDirectory(meshRoot, alphaIdentity, alphaIdentity.id, () => [
      rootRecord(alphaIdentity.id, alphaIdentity.id, "alpha"),
      shared(alphaIdentity),
    ], lease);
    const beta = createDirectory(meshRoot, betaIdentity, betaIdentity.id, () => [
      rootRecord(betaIdentity.id, betaIdentity.id, "beta"),
      shared(betaIdentity),
    ], lease);

    await alpha.start();
    // Mesh writes can lag a fresh enumeration on CI-bound filesystems
    // (Windows readdir caching): beta's first refresh relies on the occupancy
    // guard seeing alpha's records, so wait until beta's on-disk mesh view
    // actually contains alpha before contention starts — otherwise a blind
    // first write can converge on the wrong owner and never self-correct.
    await vi.waitFor(
      () => {
        expect(beta.mesh.listAll("topology/participants/").length).toBeGreaterThanOrEqual(2);
      },
      { timeout: 5_000, interval: 50 },
    );
    await beta.start();
    expect(beta.get("actor:shared")).toMatchObject({ ownerHostId: "session:alpha" });

    await alpha.close();
    // The takeover itself is another write-then-readback hop: poll refresh +
    // read until the delete propagates and beta claims the record.
    await vi.waitFor(
      async () => {
        await beta.refresh();
        expect(beta.get("actor:shared")).toMatchObject({ ownerHostId: "session:beta" });
      },
      { timeout: 5_000, interval: 50 },
    );
  });

  // smarty-dev#367: change-driven refreshes wrote the whole shared state on every agent UI
  // update. They now write only real changes, at most once per second after the first.
  describe("change-driven refreshes", () => {
    const changing = async () => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
      roots.push(root);
      const identity: MeshIdentity = { id: "session:busy", name: "main", kind: "main", sessionId: "busy" };
      const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 1_000);
      const directory = new ParticipantDirectory(mesh, {
        enabled: true, hostId: identity.id, rootId: identity.id, identity, heartbeatMs: 60_000, leaseMs: 180_000,
      });
      // The production Main source: MainAgentController.info() stamps updatedAt on every read.
      let status: "idle" | "running" = "idle";
      const main = new MainAgentController(
        { getThinkingLevel: () => "high" } as unknown as ExtensionAPI, identity.id, true, "/tmp/project", "busy");
      const context = {
        model: { provider: "anthropic", id: "model" },
        isIdle: () => status === "idle",
        hasPendingMessages: () => false,
      } as unknown as ExtensionContext;
      directory.registerSource(() => [directory.root(main.info(context))]);
      directories.push(directory);
      await directory.start();
      const writes = vi.spyOn(mesh, "writeBatch");
      return { directory, mesh, writes, setStatus: (next: typeof status) => { status = next; } };
    };

    it("writes nothing when no published record changed", async () => {
      const { directory, writes } = await changing();
      for (let index = 0; index < 20; index++) {
        directory.scheduleRefresh();
        await new Promise((resolve) => setTimeout(resolve, 100));   // each read gets a new updatedAt
      }
      await new Promise((resolve) => setTimeout(resolve, 1_200));
      expect(writes).not.toHaveBeenCalled();
    });

    it("defers change refreshes during a lock outage and preserves backoff until real heartbeat recovery", async () => {
      vi.useFakeTimers();
      // Exercise near-ceiling full-jitter draws: the fixed 200 ms tick below
      // must not consume the second outage's doubled backoff before checking it.
      vi.spyOn(Math, "random").mockReturnValue(0.999999);
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-directory-outage-"));
      roots.push(root);
      const identity: MeshIdentity = { id: "session:busy", name: "main", kind: "main", sessionId: "busy" };
      const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 1_000, { lockTimeoutMs: 100 });
      const source = vi.fn(() => [rootRecord(identity.id, identity.id, "busy")]);
      const directory = new ParticipantDirectory(mesh, {
        enabled: true, hostId: identity.id, rootId: identity.id, identity,
        heartbeatMs: 60_000, leaseMs: 180_000, reapDeadHosts: false,
      });
      directory.registerSource(source);
      directories.push(directory);
      const intervals = vi.spyOn(globalThis, "setInterval");
      const lockPath = path.join(mesh.root, ".lock");
      try {
        await directory.start();
        // Drive the real heartbeat callback without incidental ticks during backoff.
        const heartbeat = intervals.mock.calls[0]![0] as () => void;
        const runs = vi.spyOn(MeshBackgroundRetry.prototype, "run");
        const confirmations = vi.spyOn(mesh, "confirmWritable");
        const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
        const tick = async () => {
          heartbeat();
          const result = runs.mock.results.at(-1)!.value;
          await vi.advanceTimersByTimeAsync(200); // acquisition times out after 100 ms
          return await result;
        };
        const hold = () => {
          fs.mkdirSync(lockPath, { mode: 0o700 });
          fs.writeFileSync(path.join(lockPath, "owner"), `stuck\n${process.pid}\n${Date.now()}\n`);
        };
        hold();
        const holder = fs.readFileSync(path.join(lockPath, "owner"), "utf8");
        expect(await tick()).toBe("retry");
        expect(warn).toHaveBeenCalledOnce();
        expect(warn.mock.calls[0]![0]).toContain(`pid ${process.pid}`);
        const retry = runs.mock.contexts.at(-1) as MeshBackgroundRetry;
        const recovered = vi.spyOn(retry, "success");
        await vi.advanceTimersByTimeAsync(retry.waitMs);

        const readsBefore = source.mock.calls.length, attemptsBefore = runs.mock.calls.length;
        directory.scheduleRefresh();
        await Promise.resolve();
        // #4383: even an unchanged change refresh must not re-read the fleet or
        // become an extra retry. Pending changes ride the next heartbeat.
        expect(runs.mock.calls.length).toBe(attemptsBefore);
        expect(source.mock.calls.length).toBe(readsBefore);
        expect(confirmations).toHaveBeenCalledOnce();
        expect(recovered).not.toHaveBeenCalled();
        expect(directory.writeStalled()).toBeDefined();
        expect(fs.readFileSync(path.join(lockPath, "owner"), "utf8")).toBe(holder);

        expect(await tick()).toBe("retry");
        expect(warn).toHaveBeenCalledOnce(); // same live holder, same continuous outage
        expect(retry.waitMs).toBeGreaterThan(0); // second timeout retained the doubled delay
        heartbeat();
        expect(await runs.mock.results.at(-1)!.value).toBe("skipped");
        expect(confirmations).toHaveBeenCalledTimes(2);

        fs.rmSync(lockPath, { recursive: true, force: true });
        await vi.advanceTimersByTimeAsync(retry.waitMs);
        expect(await tick()).toBe("done"); // a real lock acquisition confirms recovery
        expect(directory.writeStalled()).toBeUndefined();
        expect(recovered).toHaveBeenCalledOnce();
        expect(retry.waitMs).toBe(0);
        hold();
        expect(await tick()).toBe("retry");
        // smarty-dev#8305: a later outage inside the warning window is counted, not printed.
        // (This asserted the old once-per-outage warning, unbounded on a flapping lock.)
        expect(warn).toHaveBeenCalledOnce();

        // The first real acquisition after the window prints the final summary of both
        // suppressed retries (the continuous outage's second timeout and the later outage).
        fs.rmSync(lockPath, { recursive: true, force: true });
        vi.setSystemTime(Date.now() + 60_000);
        expect(retry.waitMs).toBe(0);
        expect(await tick()).toBe("done");
        expect(recovered).toHaveBeenCalledTimes(2);
        expect(warn).toHaveBeenCalledTimes(2);
        expect(warn.mock.calls[1]![0]).toMatch(
          /^\[pi-fabric\] participant heartbeat\/change refresh: mesh lock recovered \(2 similar retries suppressed in the last 6\ds\)$/);
      } finally {
        fs.rmSync(lockPath, { recursive: true, force: true });
        await directory.close();
        vi.restoreAllMocks();
        vi.useRealTimers();
      }
    });

    it("publishes a burst of changes with at most two writes, ending on the latest", async () => {
      const { directory, mesh, writes, setStatus } = await changing();
      for (let index = 0; index < 10; index++) {
        setStatus(index % 2 === 0 ? "running" : "idle");
        directory.scheduleRefresh();
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
      setStatus("running");
      directory.scheduleRefresh();
      await vi.waitFor(() => expect(
        mesh.listAll("topology/participants/").map((entry) => (entry.value as { status?: string }).status),
      ).toEqual(["running"]), { timeout: 3_000, interval: 20 });
      await new Promise((resolve) => setTimeout(resolve, 1_200));
      expect(writes.mock.calls.length).toBeLessThanOrEqual(2);
    });

    it("still renews the file lease on a heartbeat with unchanged records", async () => {
      const { directory, writes } = await changing();
      const confirmed = vi.spyOn(directory.mesh, "confirmWritable");
      await directory.refresh();
      expect(writes).not.toHaveBeenCalled();
      expect(confirmed).toHaveBeenCalledOnce();
      expect(readHostLeases(directory.mesh.root).get(directory.options.hostId)?.expiresAt).toBeGreaterThan(Date.now());
      confirmed.mockRestore();
    });
  });

  // smarty-dev#447: the record behind a lapsed lease stays readable for a reply and a reason.
  it("reports how long ago a participant's lease lapsed, and nothing for live or unknown ids", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
    roots.push(root);
    const identity: MeshIdentity = { id: "session:alpha", name: "main", kind: "main", sessionId: "alpha" };
    const directory = createDirectory(path.join(root, "mesh"), identity, identity.id, () => [rootRecord(identity.id, identity.id, "alpha")]);
    await directory.start();
    await vi.waitFor(() => expect(directory.list({ scope: "project" })).toHaveLength(1), { timeout: 5_000, interval: 50 });
    expect(directory.lastKnown(identity.id)).toBeUndefined();                     // live
    expect(directory.lastKnown("session:nobody")).toBeUndefined();                // no record
    const later = Date.now() + 60_000;
    const known = directory.lastKnown(identity.id, later);
    expect(known?.participant).toMatchObject({ id: identity.id, kind: "root", stale: true });
    expect(known!.lapsedMs).toBeGreaterThan(0);
    expect(known!.lapsedMs).toBeLessThanOrEqual(60_000);
  });

  it("hides every participant owned by an expired host lease", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const identity: MeshIdentity = {
      id: "session:alpha",
      name: "main",
      kind: "main",
      sessionId: "alpha",
    };
    const directory = createDirectory(meshRoot, identity, identity.id, () => [
      rootRecord(identity.id, identity.id, "alpha"),
      agentRecord("agent:child", identity.id, identity.id, identity.id),
    ]);
    await directory.start();

    // Same filesystem readback lag: poll until the fresh enumeration shows
    // both records instead of asserting one shot.
    await vi.waitFor(() => expect(directory.list({ scope: "project" })).toHaveLength(2), {
      timeout: 5_000,
      interval: 50,
    });
    expect(directory.list({ scope: "project" }, Date.now() + 1_000)).toEqual([]);
    const stale = directory.list(
      { scope: "project", includeStale: true },
      Date.now() + 1_000,
    );
    expect(stale).toHaveLength(2);
    expect(stale.every((participant) => participant.stale)).toBe(true);
  });

  it("keeps the same topology API in local-only mode", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
    roots.push(root);
    const identity: MeshIdentity = {
      id: "session:local",
      name: "main",
      kind: "main",
      sessionId: "local",
    };
    const directory = new ParticipantDirectory(
      new MeshStore(path.join(root, "mesh"), 64 * 1024, 100),
      { enabled: false, hostId: identity.id, rootId: identity.id, identity },
    );
    directories.push(directory);
    directory.registerSource(() => [
      rootRecord(identity.id, identity.id, "local"),
      agentRecord("agent:local", identity.id, identity.id, identity.id),
    ]);
    await directory.start();

    expect(directory.list({ scope: "project" }).map(({ id }) => id)).toEqual([
      "session:local",
      "agent:local",
    ]);
    expect(readHostLeases(directory.mesh.root).has(identity.id)).toBe(false);
  });

  it("recovers via heartbeat when the initial publish fails at startup", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const identity: MeshIdentity = {
      id: "session:alpha",
      name: "main",
      kind: "main",
      sessionId: "alpha",
    };
    const directory = createDirectory(meshRoot, identity, identity.id, () => [
      rootRecord(identity.id, identity.id, "alpha"),
    ]);
    let failures = 1;
    const publish = directory.mesh.put.bind(directory.mesh);
    vi.spyOn(directory.mesh, "put").mockImplementation(async (input) => {
      if (failures > 0) {
        failures -= 1;
        throw new Error("mesh offline");
      }
      return publish(input);
    });

    await expect(directory.start()).rejects.toThrow("mesh offline");

    const deadline = Date.now() + 2_000;
    while (Date.now() < deadline && directory.mesh.listAll("topology/hosts/").length === 0) {
      await new Promise((resolve) => setTimeout(resolve, 25));
    }
    expect(directory.mesh.listAll("topology/hosts/")).toHaveLength(1);
    expect(directory.mesh.listAll("topology/participants/")).toHaveLength(1);
  });

  // smarty-dev#367: each heartbeat put rewrote the whole shared state file under the lock.
  it("writes one heartbeat as a single state-file write", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
    roots.push(root);
    const identity: MeshIdentity = { id: "session:batch", name: "main", kind: "main", sessionId: "batch" };
    const directory = createDirectory(path.join(root, "mesh"), identity, identity.id, () => [
      rootRecord(identity.id, identity.id, "batch"),
      agentRecord("agent:one", identity.id, identity.id, identity.id),
      agentRecord("agent:two", identity.id, identity.id, identity.id),
    ]);
    const renames = vi.spyOn(fs, "renameSync");
    await directory.refresh();
    const stateWrites = renames.mock.calls.filter(([, target]) => String(target).endsWith("state.json")).length;
    renames.mockRestore();
    // Peer-label claiming may add one write on first start; the heartbeat itself is one.
    expect(stateWrites).toBeLessThanOrEqual(2);
    expect(directory.mesh.listAll("topology/participants/")).toHaveLength(3);
    expect(directory.mesh.listAll("topology/hosts/")).toHaveLength(1);

    const again = vi.spyOn(fs, "renameSync");
    await directory.refresh();
    const steady = again.mock.calls.filter(([, target]) => String(target).endsWith("state.json")).length;
    again.mockRestore();
    expect(steady).toBe(0); // the fresh host lease needs no shared-state rewrite
  });

  // smarty-dev#266: a stopped mesh lock holder expired every lease; sessions() said [].
  // smarty-dev#883: no caller exercises expiry, so the lease is long enough that a starved Windows
  // runner cannot outlast it between start() and the first read; the 150 ms lock timeout and 100 ms
  // heartbeat still detect the stall quickly.
  const stallDirectory = (name: string, source: () => FabricParticipantRecord[]) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
    roots.push(root);
    const identity: MeshIdentity = { id: `session:${name}`, name: "main", kind: "main", sessionId: name };
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 1_000, { lockTimeoutMs: 150 });
    const directory = new ParticipantDirectory(mesh, {
      enabled: true, hostId: identity.id, rootId: identity.id, identity, heartbeatMs: 100, leaseMs: 10_000,
    });
    directory.registerSource(source);
    directories.push(directory);
    return { directory, mesh, identity };
  };

  it("reports a write-stalled mesh without throwing from its own reads, then recovers", async () => {
    const { directory, mesh, identity } = stallDirectory("stall", () => [rootRecord("session:stall", "session:stall", "stall")]);
    await directory.start();
    expect(directory.sessions().map((session) => session.id)).toEqual([identity.id]);
    expect(directory.writeStalled()).toBeUndefined();

    // A live holder that never releases: the store must not take its lock over.
    const lockPath = path.join(mesh.root, ".lock");
    fs.mkdirSync(lockPath, { mode: 0o700 });
    fs.writeFileSync(path.join(lockPath, "owner"), `stuck\n${process.pid}\n${Date.now()}\n`);
    await expect.poll(() => directory.writeStalled()?.message ?? "", { timeout: 5_000, interval: 50 })
      .toMatch(/^Fabric mesh is write-stalled: FABRIC_MESH_LOCK_TIMEOUT: Timed out waiting for the Fabric mesh lock/);
    // Timers, the dashboard and ownership checks read these: they must never throw.
    expect(() => directory.sessions()).not.toThrow();
    expect(() => directory.peers()).not.toThrow();
    expect(directory.get("session:departed")).toBeUndefined();

    fs.rmSync(lockPath, { recursive: true, force: true });
    await expect.poll(() => directory.writeStalled(), { timeout: 5_000, interval: 50 }).toBeUndefined();
    expect(directory.sessions().map((session) => session.id)).toEqual([identity.id]);
  });

  // Review F1/F3 on #24: two hosts on one mesh. A peer's lease can lapse before this host's
  // lock wait times out, and this host's heartbeat can run late.
  const meshPair = (reader: { heartbeatMs: number; leaseMs: number }, peer: { heartbeatMs: number; leaseMs: number }) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    // The peer's identity is not "main": a main also writes a legacy session entry, whose fixed
    // 15 s lease (the production host lease) would outlive these scaled leases.
    const make = (name: string, timing: { heartbeatMs: number; leaseMs: number }) => {
      const identity: MeshIdentity = { id: `session:${name}`, name: "main", kind: name === "peer" ? "actor" : "main", sessionId: name };
      const mesh = new MeshStore(meshRoot, 64 * 1024, 1_000, { lockTimeoutMs: 10_000 });
      const directory = new ParticipantDirectory(mesh, {
        enabled: true, hostId: identity.id, rootId: identity.id, identity, ...timing,
      });
      directory.registerSource(() => [rootRecord(identity.id, identity.id, name)]);
      directories.push(directory);
      return directory;
    };
    const lockPath = path.join(meshRoot, ".lock");
    return {
      reader: make("reader", reader),
      peer: make("peer", peer),
      hold: () => {
        fs.mkdirSync(lockPath, { mode: 0o700 });
        fs.writeFileSync(path.join(lockPath, "owner"), `stuck\n${process.pid}\n${Date.now()}\n`);
      },
      release: () => fs.rmSync(lockPath, { recursive: true, force: true }),
    };
  };
  const seesPeer = (directory: ParticipantDirectory) => directory.peers().some((peer) => peer.id === "session:peer");

  it("never settles a peer that lapsed behind a stalled lock, and reports it before the lock timeout", async () => {
    const { reader, peer, hold, release } = meshPair({ heartbeatMs: 600, leaseMs: 20_000 }, { heartbeatMs: 100, leaseMs: 400 });
    await Promise.all([reader.start(), peer.start()]);
    await vi.waitFor(() => expect(seesPeer(reader)).toBe(true), { timeout: 5_000, interval: 20 });
    await vi.waitFor(() => expect(Date.now() - reader.confirmedAt()).toBeLessThan(40), { timeout: 5_000, interval: 5 });
    hold();                                                    // right after the reader's last commit
    const started = Date.now();
    let result: PeerSettleResult | undefined;
    void awaitPeerSettle({
      poll: () => reader.peers(),
      stalled: () => reader.writeStalled(),
      confirmedAt: () => reader.confirmedAt(),
      selector: "session:peer",
      settledForMs: 60_000,
      pollMs: 20,
    }).then((settled) => { result = settled; });
    // The peer lapses while the reader's own heartbeat is not yet overdue: the plain read omits
    // it, but peer-settle does not take the absence as a departure.
    await vi.waitFor(() => expect(seesPeer(reader)).toBe(false), { timeout: 3_000, interval: 10 });
    expect(reader.writeStalled()).toBeUndefined();
    // review/astra on a85b2c4: a settle armed now sees no peer at all; it must not succeed either.
    let late: PeerSettleResult | undefined;
    void awaitPeerSettle({
      poll: () => reader.peers(),
      stalled: () => reader.writeStalled(),
      confirmedAt: () => reader.confirmedAt(),
      settledForMs: 60_000,
      pollMs: 20,
    }).then((settled) => { late = settled; });
    await new Promise((resolve) => setTimeout(resolve, 200));
    expect(result).toBeUndefined();
    expect(late).toBeUndefined();
    // Two heartbeat intervals without a commit: the lapse now reads as unknown visibility.
    await vi.waitFor(() => expect(result).toBeDefined(), { timeout: 5_000, interval: 20 });
    expect(result).toEqual({ ok: false, error: expect.stringMatching(/1 peer lease lapsed while this host's heartbeat has not committed/) });
    expect(reader.writeStalled()?.message).toMatch(/^Fabric mesh is write-stalled: 1 peer lease lapsed/);
    await vi.waitFor(() => expect(late).toEqual({ ok: false, error: expect.stringMatching(/peer lease lapsed/) }), { timeout: 2_000, interval: 20 });
    expect(Date.now() - started).toBeLessThan(5_000);          // long before the 10 s lock timeout
    release();
    await vi.waitFor(() => expect(seesPeer(reader)).toBe(true), { timeout: 5_000, interval: 20 });
    await vi.waitFor(() => expect(reader.writeStalled()).toBeUndefined(), { timeout: 5_000, interval: 20 });
  }, 20_000);

  it("settles a peer that genuinely departed on a working mesh, without a stall report", async () => {
    const { reader, peer } = meshPair({ heartbeatMs: 100, leaseMs: 2_000 }, { heartbeatMs: 100, leaseMs: 400 });
    await Promise.all([reader.start(), peer.start()]);
    await vi.waitFor(() => expect(seesPeer(reader)).toBe(true), { timeout: 5_000, interval: 20 });
    vi.spyOn(peer, "refresh").mockResolvedValue(undefined);    // the peer crashes: no renewals, no cleanup
    let flagged = false;
    const settled = awaitPeerSettle({
      poll: () => { if (reader.writeStalled()) flagged = true; return reader.peers(); },
      stalled: () => reader.writeStalled(),
      confirmedAt: () => reader.confirmedAt(),
      selector: "session:peer",
      settledForMs: 60_000,
      pollMs: 10,
    });
    await expect(settled).resolves.toEqual({ ok: true });
    expect(flagged).toBe(false);
  }, 20_000);

  it("keeps fresh listings usable while a lock wait lasts longer than a heartbeat", async () => {
    const { reader, peer, hold, release } = meshPair({ heartbeatMs: 200, leaseMs: 5_000 }, { heartbeatMs: 200, leaseMs: 5_000 });
    await Promise.all([reader.start(), peer.start()]);
    await vi.waitFor(() => expect(seesPeer(reader)).toBe(true), { timeout: 5_000, interval: 20 });
    hold();
    let flagged = false;
    let missing = false;
    const until = Date.now() + 1_500;                          // more than seven heartbeats, less than the 10 s timeout
    while (Date.now() < until) {
      if (reader.writeStalled()) flagged = true;
      if (!seesPeer(reader)) missing = true;
      await new Promise((resolve) => setTimeout(resolve, 25));
    }
    release();
    expect(flagged).toBe(false);
    expect(missing).toBe(false);
  }, 20_000);

  it("does not report a stall for lock waits shorter than one heartbeat", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
    roots.push(root);
    const identity: MeshIdentity = { id: "session:brief", name: "main", kind: "main", sessionId: "brief" };
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 1_000, { lockTimeoutMs: 10_000 });
    const directory = new ParticipantDirectory(mesh, {
      enabled: true, hostId: identity.id, rootId: identity.id, identity, heartbeatMs: 400, leaseMs: 1_200,
    });
    directory.registerSource(() => [rootRecord(identity.id, identity.id, "brief")]);
    directories.push(directory);
    await directory.start();
    const lockPath = path.join(mesh.root, ".lock");
    let flagged = false;
    for (let round = 0; round < 6; round++) {
      // Ordinary contention: another writer holds the lock for 60 ms at a time.
      fs.mkdirSync(lockPath, { mode: 0o700 });
      fs.writeFileSync(path.join(lockPath, "owner"), `brief\n${process.pid}\n${Date.now()}\n`);
      const until = Date.now() + 60;
      while (Date.now() < until) {
        if (directory.writeStalled()) flagged = true;
        await new Promise((resolve) => setTimeout(resolve, 10));
      }
      fs.rmSync(lockPath, { recursive: true, force: true });
      await new Promise((resolve) => setTimeout(resolve, 150));
      if (directory.writeStalled()) flagged = true;
    }
    expect(flagged).toBe(false);
  });

  it("returns an empty directory without a stall report when the mesh is healthy", async () => {
    const { directory } = stallDirectory("empty", () => []);
    await directory.start();
    await new Promise((resolve) => setTimeout(resolve, 400));
    expect(directory.sessions()).toEqual([]);
    expect(directory.writeStalled()).toBeUndefined();
  });

  it("does not report a stall for a heartbeat failure that is not a lock timeout", async () => {
    const { directory, mesh } = stallDirectory("disk", () => [rootRecord("session:disk", "session:disk", "disk")]);
    await directory.start();
    vi.spyOn(mesh, "writeBatch").mockRejectedValue(new Error("ENOSPC: no space left on device"));
    await new Promise((resolve) => setTimeout(resolve, 500));
    expect(directory.writeStalled()).toBeUndefined();
    expect(() => directory.sessions()).not.toThrow();
  });
});

// smarty-dev#557: every dashboard rebuild, peer listing and ownership check cloned the whole
// fleet directory. The directory now parses the shared state once per parse of the file.
describe("ParticipantDirectory reads", () => {
  const identity: MeshIdentity = { id: "session:reader", name: "main", kind: "main", sessionId: "reader" };
  const writer: MeshIdentity = { id: "session:writer", name: "main", kind: "main", sessionId: "writer" };

  it("prepares idle heartbeats from shared pinned entries without cloning retained actor payloads (#2039)", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-heartbeat-shared-"));
    roots.push(root);
    const reader = createDirectory(path.join(root, "mesh"), identity, identity.id, () => [],
      { heartbeatMs: 60_000, leaseMs: 120_000 });
    const actorKey = "actors/foreign/actor";
    await reader.mesh.put({ key: actorKey, value: { id: "actor", instructions: "x".repeat(30_000) }, identity: writer });
    const cloned = vi.spyOn(reader.mesh, "listAll");
    const original = reader.mesh.listAllShared.bind(reader.mesh);
    const shared = vi.spyOn(reader.mesh, "listAllShared").mockImplementation((prefix, options) => {
      const entries = original(prefix, options);
      for (const entry of entries) {
        if (typeof entry.value === "object" && entry.value !== null) Object.freeze(entry.value);
        Object.freeze(entry);
      }
      return entries;
    });
    await reader.start();
    const prefixes = ["topology/participants/", "sessions/", "actors/"];
    const preparation = shared.mock.calls.filter(([prefix, options]) => prefixes.includes(prefix!) && options?.snapshot);
    // smarty-dev#7978: the compatibility liveness probe (#legacyRenewalsRequired) reads the same pinned
    // snapshot without copies too, so every prefix is read shared, from one snapshot.
    expect(new Set(preparation.map(([prefix]) => prefix))).toEqual(new Set(prefixes));
    expect(new Set(preparation.map(([, options]) => options!.snapshot)).size).toBe(1);
    // Neither the preparation nor the liveness probes copy entries: the large retained actor
    // payloads, and every host, session and participant entry, are never cloned by this heartbeat.
    expect(cloned.mock.calls.filter(([prefix]) => ["actors/", "sessions/", "topology/participants/", "topology/hosts/"].includes(prefix!))).toEqual([]);
    expect(reader.mesh.get(actorKey, { fresh: true })?.value).toEqual({ id: "actor", instructions: "x".repeat(30_000) });
    cloned.mockRestore(); shared.mockRestore();
  });

  it("reuse one parse while the state is unchanged, and see a write at once", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
    const reader = createDirectory(meshRoot, identity, identity.id, () => [rootRecord(identity.id, identity.id, "reader")],
      { heartbeatMs: 60_000, leaseMs: 120_000 });
    const peer = createDirectory(meshRoot, writer, writer.id, () => [rootRecord(writer.id, writer.id, "writer")],
      { heartbeatMs: 60_000, leaseMs: 120_000 });
    await reader.start();
    await peer.start();
    const shared = vi.spyOn(reader.mesh, "listAllShared");
    const first = reader.list({ scope: "project" });
    const calls = shared.mock.calls.length;
    expect(first.map((participant) => participant.id)).toContain(writer.id);
    for (let i = 0; i < 5; i++) reader.list({ scope: "project" });
    reader.peers();
    expect(shared.mock.calls.length).toBe(calls);                  // no read of the directory per call
    // Callers get their own top-level objects; the shared parse is frozen.
    first[0]!.status = "changed";
    expect(reader.list({ scope: "project" })[0]!.status).not.toBe("changed");
    const capabilities = reader.list({ scope: "project" }).find((participant) => participant.id === writer.id)!.capabilities;
    expect(Object.isFrozen(capabilities)).toBe(true);

    await store.put({ key: "topology/participants/" + createHash("sha256").update("session:writer:agent").digest("hex"),
      value: agentRecord("session:writer:agent", writer.id, writer.id, writer.id), identity: writer });
    const after = reader.list({ scope: "project", fresh: true });
    expect(after.map((participant) => participant.id)).toContain("session:writer:agent");
    // The write copied only the new entry: an unchanged record is the same parse.
    expect(after.find((participant) => participant.id === writer.id)!.capabilities).toBe(capabilities);
    // A changed record is read again.
    const key = "topology/participants/" + createHash("sha256").update(writer.id).digest("hex");
    const current = store.get(key)!;
    await store.put({ key, value: { ...(current.value as FabricParticipantRecord), status: "busy" }, identity: writer, ifVersion: current.version });
    expect(reader.list({ scope: "project", fresh: true }).find((participant) => participant.id === writer.id)!.status).toBe("busy");
    shared.mockRestore();
  });
});

// review/astra F1 on pi-fabric#140: a listing reuses the lease files it read for heartbeat/5.
// Under file-only renewals those files are the only liveness evidence, so a reused read must
// never turn a renewed peer into an absent one.
describe("ParticipantDirectory lease reads", () => {
  const reader: MeshIdentity = { id: "session:reader", name: "main", kind: "main", sessionId: "reader" };
  const peer: MeshIdentity = { id: "session:peer", name: "main", kind: "main", sessionId: "peer" };
  const timing = { heartbeatMs: 60_000, leaseMs: 120_000 };            // leases are reused for 12 s
  const setup = async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
    await store.put({ key: LIVENESS_POLICY_KEY, value: { version: 1, hostLeases: "files" }, identity: peer });
    const readerDirectory = createDirectory(meshRoot, reader, reader.id, () => [rootRecord(reader.id, reader.id, "reader")], timing);
    const peerDirectory = createDirectory(meshRoot, peer, peer.id, () => [rootRecord(peer.id, peer.id, "peer")], timing);
    await readerDirectory.start();
    await peerDirectory.start();
    const fileLease = readHostLeases(meshRoot).get(peer.id)!;
    const shared = store.listAll("topology/hosts/").find((entry) => (entry.value as { id?: string }).id === peer.id)!;
    // The peer's effective expiry: the later of its file lease and its shared-state record.
    const lease = { expiresAt: Math.max(fileLease.expiresAt, (shared.value as { expiresAt: number }).expiresAt) };
    const renew = (at: number) => writeHostLease(meshRoot, { ...fileLease, updatedAt: at, expiresAt: at + timing.leaseMs });
    const seesPeer = () => readerDirectory.peers().some((candidate) => candidate.id === peer.id);
    return { readerDirectory, lease, renew, seesPeer };
  };
  afterEach(() => vi.restoreAllMocks());

  it.each(["native", "win32"])("keep a peer whose file lease was renewed after the read and before its old expiry (%s)", async (target) => {
    const { readerDirectory, lease, renew, seesPeer } = await setup();
    const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
    let renewed = false;
    try {
      if (target === "win32") {
        Object.defineProperty(process, "platform", { ...platform, value: "win32" });
        // NTFS file IDs can exceed Number's exact integer range. Model two distinct
        // IDs that round to the same number, with equal size and timestamps.
        const file = path.join(readerDirectory.mesh.root, "host-leases",
          createHash("sha256").update(peer.id).digest("hex").slice(0, 32) + ".json");
        const stat = fs.statSync.bind(fs);
        const before = stat(file);
        const beforeBig = stat(file, { bigint: true });
        vi.spyOn(fs, "statSync").mockImplementation(((name: fs.PathLike, options?: fs.StatOptions) => {
          const current = stat(name, options);
          if (String(name) !== file) return current;
          const ino = 2n ** 54n + (renewed ? 1n : 0n);
          return Object.assign(current!, options?.bigint
            ? { ...beforeBig, ino }
            : { ...before, ino: Number(ino) });
        }) as typeof fs.statSync);
      }
      const clock = vi.spyOn(Date, "now");
      clock.mockReturnValue(lease.expiresAt - 5_000);
      expect(seesPeer()).toBe(true);                                   // fills the lease read
      renew(lease.expiresAt - 2_000);                                  // file-only renewal
      renewed = true;
      clock.mockReturnValue(lease.expiresAt + 1);                      // old expiry passed, read reused
      expect(seesPeer()).toBe(true);
      clock.mockReturnValue(lease.expiresAt + 5_000);
      expect(seesPeer()).toBe(true);
    } finally {
      Object.defineProperty(process, "platform", platform);
    }
  });

  it("drop a peer whose lease lapsed and was not renewed", async () => {
    const { lease, seesPeer } = await setup();
    const clock = vi.spyOn(Date, "now");
    clock.mockReturnValue(lease.expiresAt - 5_000);
    expect(seesPeer()).toBe(true);
    clock.mockReturnValue(lease.expiresAt + 1);
    expect(seesPeer()).toBe(false);
  });

  it("read the lease files again after this host's heartbeat commits", async () => {
    const { readerDirectory, lease, renew, seesPeer } = await setup();
    const clock = vi.spyOn(Date, "now");
    clock.mockReturnValue(lease.expiresAt + 1_000);
    expect(seesPeer()).toBe(false);                                  // lapsed when read: the read is reused
    renew(lease.expiresAt + 1_500);                                  // the peer comes back
    clock.mockReturnValue(lease.expiresAt + 2_000);
    const confirmed = readerDirectory.confirmedAt();
    await readerDirectory.refresh();                                  // a confirming heartbeat
    expect(readerDirectory.confirmedAt()).toBeGreaterThan(confirmed);
    expect(seesPeer()).toBe(true);                                   // no view older than the commit
  });
});

describe("MeshStore.list", () => {
  it("returns the first page, sorted, as copies", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-topology-"));
    roots.push(root);
    const store = new MeshStore(path.join(root, "mesh"), 64 * 1024, 1_000);
    const identity: MeshIdentity = { id: "session:a", name: "main", kind: "main" };
    for (const key of ["c", "a", "b"]) await store.put({ key: "x/" + key, value: { key }, identity });
    const page = store.list("x/", 2);
    expect(page.map((entry) => entry.key)).toEqual(["x/a", "x/b"]);
    (page[0]!.value as { key: string }).key = "mutated";
    expect((store.get("x/a")!.value as { key: string }).key).toBe("a");
  });
});

// smarty-dev#6477 L6: an idle heartbeat writes nothing, so it must not queue on the mesh lock
// when another writer's commit already shows the shared state writable.
describe("idle confirmations without the mesh lock (smarty-dev#6477 L6)", () => {
  const idleFixture = async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-idle-confirm-"));
    roots.push(root);
    const identity: MeshIdentity = { id: "session:idle", name: "main", kind: "main", sessionId: "idle" };
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 1_000, { lockTimeoutMs: 150 });
    const other: MeshIdentity = { id: "session:other", name: "other", kind: "main", sessionId: "other" };
    const writer = new MeshStore(mesh.root, 64 * 1024, 1_000);
    await writer.put({ key: LIVENESS_POLICY_KEY, value: { version: 1, hostLeases: "files" }, identity: other });
    const directory = new ParticipantDirectory(mesh, { enabled: true, hostId: identity.id, rootId: identity.id, identity,
      heartbeatMs: 60_000, leaseMs: 180_000, reapDeadHosts: false });
    directory.registerSource(() => [rootRecord(identity.id, identity.id, "idle")]);
    directories.push(directory);
    await directory.refresh();                                 // admission: a real lock receipt
    await directory.refresh();                                 // settle first-publication migrations
    const lockPath = path.join(mesh.root, ".lock");
    // Count real canonical acquisitions: protocol 1 mkdir, protocol 2 rename into place.
    const mkdir = vi.spyOn(fs, "mkdirSync"), rename = vi.spyOn(fs, "renameSync");
    const acquisitions = () => mkdir.mock.calls.filter(([target]) => target === lockPath).length +
      rename.mock.calls.filter(([, target]) => target === lockPath).length;
    const confirmations = vi.spyOn(mesh, "confirmWritable");
    const writes = vi.spyOn(mesh, "writeBatch");
    return { directory, mesh, writer, other, lockPath, acquisitions, confirmations, writes };
  };
  afterEach(() => { vi.restoreAllMocks(); });
  // Witness times are whole milliseconds of file time (latestCommitWitness floors mtimeMs), so a
  // commit in the same millisecond as a receipt is, by design, no evidence. Before a commit that
  // must count as evidence, wait until file time passes every receipt and witness so far.
  // The latest file time under the mesh root, floored like latestCommitWitness: a superset of its witnesses.
  const meshFileTime = (meshRoot: string) =>
    Math.max(0, ...fs.readdirSync(meshRoot).map(name => Math.floor(fs.statSync(path.join(meshRoot, name)).mtimeMs)));
  const fileClockPast = (meshRoot: string, ...receipts: number[]) => {
    const latest = Math.max(...receipts, meshFileTime(meshRoot));
    const probe = path.join(path.dirname(meshRoot), "clock.probe");
    for (let i = 0; i < 100_000; i++) {
      fs.writeFileSync(probe, "");
      if (Math.floor(fs.statSync(probe).mtimeMs) > latest && Date.now() > latest) return;
    }
    throw new Error("file clock did not advance");
  };

  it("takes no mesh lock for an idle confirmation after another writer's commit, and still advances confirmedAt", async () => {
    const f = await idleFixture();
    const before = f.directory.confirmedAt();
    fileClockPast(f.mesh.root, before);
    await f.writer.put({ key: "bench/other", value: { at: 1 }, identity: f.other });
    const baseline = f.acquisitions();
    await f.directory.refresh();
    expect(f.acquisitions() - baseline).toBe(0);
    expect(f.confirmations).not.toHaveBeenCalled();
    expect(f.writes).not.toHaveBeenCalled();
    expect(f.directory.confirmedAt()).toBeGreaterThan(before);
    // The receipt is the commit's file time, clamped to Date.now(). Compare each bound with its own
    // clock: on Windows file time and Date.now() are separate clocks, and file time can lead by a
    // millisecond or more, so a Date.now() taken right after the commit is no bound (CI on #634).
    expect(f.directory.confirmedAt()).toBeLessThanOrEqual(meshFileTime(f.mesh.root));
    expect(f.directory.confirmedAt()).toBeLessThanOrEqual(Date.now());
    expect(f.directory.canConsumeMesh()).toBe(true);
    expect(readHostLeases(f.mesh.root).get(f.directory.options.hostId)?.expiresAt).toBeGreaterThan(Date.now());
    // The next view is canonical, as after confirmWritable: the other writer's commit is visible.
    expect(f.mesh.get("bench/other")?.value).toEqual({ at: 1 });
  });

  // smarty-dev#6477 L6 (Windows CI on #634): a commit's file time may lead Date.now(). Such evidence
  // confirms no later than the real clock, so a confirmation window never starts in the future.
  it("clamps a lock-free receipt to Date.now() when the commit's file time leads the clock", async () => {
    const f = await idleFixture();
    const before = f.directory.confirmedAt();
    fileClockPast(f.mesh.root, before);
    await f.writer.put({ key: "bench/other", value: { at: 5 }, identity: f.other });
    const witness = path.join(f.mesh.root, "state.json");
    fs.utimesSync(witness, fs.statSync(witness).atime, new Date(Date.now() + 30_000));
    const baseline = f.acquisitions();
    await f.directory.refresh();
    const after = Date.now();
    expect(f.acquisitions() - baseline).toBe(0);
    expect(f.confirmations).not.toHaveBeenCalled();
    expect(f.directory.confirmedAt()).toBeGreaterThan(before);
    expect(f.directory.confirmedAt()).toBeLessThanOrEqual(after);
  });

  // Review round 2 on #592: the fresh read after the witness is synchronous file I/O and can be
  // slow during a mesh stall. Evidence that ages past one heartbeat during that read must not
  // confirm: the heartbeat falls back to the lock path.
  it("falls back to the lock when the fresh read outlasts one heartbeat of witness age", async () => {
    const f = await idleFixture();
    const before = f.directory.confirmedAt();
    fileClockPast(f.mesh.root, before);
    await f.writer.put({ key: "bench/other", value: { at: 4 }, identity: f.other });
    const committedAt = Date.now();
    const realNow = Date.now.bind(Date);
    const realToken = f.mesh.stateToken.bind(f.mesh);
    let stalled = false;
    const clock = vi.spyOn(Date, "now").mockImplementation(() => realNow() + (stalled ? 60_001 : 0));
    // Only the confirmation's own read is fresh on this idle path; it completes one heartbeat late.
    const token = vi.spyOn(f.mesh, "stateToken").mockImplementation(options => {
      const result = realToken(options);
      if (options?.fresh === true) stalled = true;
      return result;
    });
    try {
      const baseline = f.acquisitions();
      await f.directory.refresh();
      expect(token.mock.calls.some(([options]) => options?.fresh === true)).toBe(true);
      expect(f.confirmations).toHaveBeenCalledOnce();
      expect(f.acquisitions() - baseline).toBe(1);
      expect(f.writes).not.toHaveBeenCalled();
      // The receipt is the lock's, taken after the stall, not the aged commit's time.
      expect(f.directory.confirmedAt()).toBeGreaterThan(committedAt + 60_000);
    } finally { clock.mockRestore(); token.mockRestore(); }
  });

  it("takes the lock exactly once when no unseen commit is evidence, and never confirms behind a stuck holder", async () => {
    const f = await idleFixture();
    const baseline = f.acquisitions();
    await f.directory.refresh();                               // nothing committed since its receipt
    expect(f.acquisitions() - baseline).toBe(1);
    expect(f.confirmations).toHaveBeenCalledOnce();
    // Evidence already behind a receipt never confirms twice; a stuck holder commits nothing.
    const confirmed = f.directory.confirmedAt();
    fs.mkdirSync(f.lockPath, { mode: 0o700 });
    fs.writeFileSync(path.join(f.lockPath, "owner"), "stuck\n" + process.pid + "\n" + Date.now() + "\n");
    try {
      await expect(f.directory.refresh()).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
      expect(f.directory.confirmedAt()).toBe(confirmed);
      expect(f.directory.canConsumeMesh()).toBe(false);
    } finally { fs.rmSync(f.lockPath, { recursive: true, force: true }); }
  });

  it("recovers an overdue or failed chain only with a real lock receipt, even after another commit", async () => {
    const f = await idleFixture();
    const failure = new Error("shared confirmation blocked");
    f.confirmations.mockRejectedValueOnce(failure);
    await expect(f.directory.refresh()).rejects.toBe(failure);
    await f.writer.put({ key: "bench/other", value: { at: 2 }, identity: f.other });
    const baseline = f.acquisitions();
    await f.directory.refresh();                               // a failed refresh needs the lock again
    expect(f.acquisitions() - baseline).toBe(1);
    expect(f.confirmations).toHaveBeenCalledTimes(2);
    // Overdue: two heartbeats without a receipt. Fresh evidence does not restart the chain.
    const now = Date.now();
    const clock = vi.spyOn(Date, "now").mockImplementation(() => now + 120_000);
    try {
      await f.writer.put({ key: "bench/other", value: { at: 3 }, identity: f.other });
      // That commit is one second old on the advanced clock: only the overdue chain refuses it.
      const witness = path.join(f.mesh.root, "state.json");
      fs.utimesSync(witness, fs.statSync(witness).atime, new Date(now + 119_000));
      await f.directory.refresh();
      expect(f.confirmations).toHaveBeenCalledTimes(3);
    } finally { clock.mockRestore(); }
  });

  // Windows CI on #592: Node defines fs.constants.O_NOFOLLOW only on POSIX. Since review round 4 the
  // product never writes the confirmation witness without an atomic no-follow open, so on Windows a
  // real confirmation leaves no witness for another idle participant (by design, not a timing race).
  // There, the native test below asserts that contract instead; this one runs wherever the witness exists.
  const nativeNoFollow = typeof fs.constants.O_NOFOLLOW === "number" && fs.constants.O_NOFOLLOW !== 0;
  it.skipIf(!nativeNoFollow)("lets one real confirmation on an idle mesh serve another idle participant without the lock", async () => {
    const f = await idleFixture();
    const identity: MeshIdentity = { id: "session:second", name: "main", kind: "main", sessionId: "second" };
    const second = new ParticipantDirectory(new MeshStore(f.mesh.root, 64 * 1024, 1_000), { enabled: true, hostId: identity.id,
      rootId: identity.id, identity, heartbeatMs: 60_000, leaseMs: 180_000, reapDeadHosts: false });
    second.registerSource(() => [rootRecord(identity.id, identity.id, "second")]);
    directories.push(second);
    await second.refresh();
    await second.refresh();
    await f.directory.refresh();                               // absorbs the second's first commits
    const confirmationsBefore = f.confirmations.mock.calls.length;
    fileClockPast(f.mesh.root, second.confirmedAt(), f.directory.confirmedAt());
    await f.directory.refresh();                               // nothing unseen: a real, witnessed acquisition
    expect(f.confirmations.mock.calls.length - confirmationsBefore).toBe(1);
    const before = second.confirmedAt(), baseline = f.acquisitions();
    // That real confirmation left its witness, on the file clock, after the second's receipt:
    // the proof the second may use. Checked here, so a missing proof cannot pass for a lock race.
    const witnessAt = Math.floor(fs.lstatSync(plantedWitness(f.mesh.root)).mtimeMs);
    expect(witnessAt).toBeGreaterThan(before);
    // CI flake on #592: the second's heartbeat read Date.now() several times (evidence age, the
    // post-read recheck) on a loaded runner. Pin its clock one millisecond after the witness: the
    // proof is fresh by construction, so the product must confirm without the lock. Age limits
    // and the fallback to the lock keep their own tests above (real and advanced clocks).
    const clock = vi.spyOn(Date, "now").mockReturnValue(witnessAt + 1);
    const secondConfirms = vi.spyOn(second.mesh, "confirmWritable");
    try {
      await second.refresh();
    } finally { clock.mockRestore(); }
    expect(f.acquisitions() - baseline).toBe(0);
    expect(secondConfirms).not.toHaveBeenCalled();
    expect(second.confirmedAt()).toBeGreaterThan(before);
    expect(second.confirmedAt()).toBeLessThanOrEqual(witnessAt + 1);
  });

  // Security round on #592: only this user's own regular witness file under the mesh root is proof.
  // A planted symlink (to a recently modified path), a non-regular file or a foreign file is not:
  // the heartbeat takes the lock, and touching the witness never follows or truncates a link.
  const plantedWitness = (meshRoot: string) => path.join(meshRoot, "participant-confirm.witness");
  it.skipIf(process.platform === "win32")("takes the lock when the witness is a symlink to a fresh path, and never writes through it", async () => {
    const f = await idleFixture();
    fileClockPast(f.mesh.root, f.directory.confirmedAt());
    const target = path.join(path.dirname(f.mesh.root), "fresh-target.txt");
    fs.writeFileSync(target, "keep");                         // modified after every receipt
    const witness = plantedWitness(f.mesh.root);
    fs.rmSync(witness, { force: true });
    fs.symlinkSync(target, witness);
    const baseline = f.acquisitions();
    await f.directory.refresh();
    expect(f.confirmations).toHaveBeenCalledOnce();
    expect(f.acquisitions() - baseline).toBe(1);
    expect(fs.lstatSync(witness).isSymbolicLink()).toBe(true);
    expect(fs.readFileSync(target, "utf8")).toBe("keep");     // the touch did not follow the link
  });

  it("takes the lock when the witness is not a regular file", async () => {
    const f = await idleFixture();
    fileClockPast(f.mesh.root, f.directory.confirmedAt());
    const witness = plantedWitness(f.mesh.root);
    fs.rmSync(witness, { force: true });
    fs.mkdirSync(witness);                                     // a fresh directory under the name
    const baseline = f.acquisitions();
    await f.directory.refresh();
    expect(f.confirmations).toHaveBeenCalledOnce();
    expect(f.acquisitions() - baseline).toBe(1);
    expect(fs.lstatSync(witness).isDirectory()).toBe(true);
  });

  it.skipIf(typeof process.getuid !== "function")("takes the lock when the witness belongs to another uid", async () => {
    const f = await idleFixture();
    fileClockPast(f.mesh.root, f.directory.confirmedAt());
    const witness = plantedWitness(f.mesh.root);
    fs.writeFileSync(witness, "");
    const realLstat = fs.lstatSync.bind(fs) as (...args: unknown[]) => fs.Stats | undefined;
    vi.spyOn(fs, "lstatSync").mockImplementation(((...args: unknown[]) => {
      const stat = realLstat(...args);
      if (args[0] === witness && stat) Object.defineProperty(stat, "uid", { value: process.getuid!() + 1 });
      return stat;
    }) as typeof fs.lstatSync);
    const baseline = f.acquisitions();
    await f.directory.refresh();
    expect(f.confirmations).toHaveBeenCalledOnce();
    expect(f.acquisitions() - baseline).toBe(1);
  });

  it("still confirms without the lock on this user's own fresh regular witness", async () => {
    const f = await idleFixture();
    const before = f.directory.confirmedAt();
    fileClockPast(f.mesh.root, before);
    fs.writeFileSync(plantedWitness(f.mesh.root), "");
    const baseline = f.acquisitions();
    await f.directory.refresh();
    expect(f.acquisitions() - baseline).toBe(0);
    expect(f.confirmations).not.toHaveBeenCalled();
    expect(f.directory.confirmedAt()).toBeGreaterThan(before);
  });

  // Review round 4 on #592: without an atomic no-follow open, an lstat-then-open could be raced by a
  // swap to a symlink. Such a platform never opens or writes the witness: no lock-free proof from
  // it, so the next idle participant takes the lock too.
  const expectNoWitnessSharing = async () => {
    const f = await idleFixture();
    const identity: MeshIdentity = { id: "session:second", name: "main", kind: "main", sessionId: "second" };
    const second = new ParticipantDirectory(new MeshStore(f.mesh.root, 64 * 1024, 1_000), { enabled: true, hostId: identity.id,
      rootId: identity.id, identity, heartbeatMs: 60_000, leaseMs: 180_000, reapDeadHosts: false });
    second.registerSource(() => [rootRecord(identity.id, identity.id, "second")]);
    directories.push(second);
    await second.refresh();
    await second.refresh();
    await f.directory.refresh();                               // absorbs the second's first commits
    const witness = plantedWitness(f.mesh.root);
    fs.rmSync(witness, { force: true });
    const opens = vi.spyOn(fs, "openSync");
    const confirmationsBefore = f.confirmations.mock.calls.length;
    fileClockPast(f.mesh.root, second.confirmedAt(), f.directory.confirmedAt());
    await f.directory.refresh();                               // a real acquisition, but no witness
    expect(f.confirmations.mock.calls.length - confirmationsBefore).toBe(1);
    expect(opens.mock.calls.some(([file]) => file === witness)).toBe(false);
    expect(fs.existsSync(witness)).toBe(false);
    const baseline = f.acquisitions();
    const secondConfirms = vi.spyOn(second.mesh, "confirmWritable");
    await second.refresh();
    expect(secondConfirms).toHaveBeenCalledOnce();
    expect(f.acquisitions() - baseline).toBe(1);
    expect(fs.existsSync(witness)).toBe(false);
  };
  it("never opens or writes the witness without O_NOFOLLOW, so idle participants take the lock", async () => {
    const platform = confirmWitnessPlatform.constants;
    confirmWitnessPlatform.constants = { ...fs.constants, O_NOFOLLOW: undefined };
    try { await expectNoWitnessSharing(); } finally { confirmWitnessPlatform.constants = platform; }
  });
  // The same contract with the platform's real constants (Windows): no seam, no witness, the lock.
  it.runIf(!nativeNoFollow)("on this platform without O_NOFOLLOW, a real confirmation leaves no witness and the next idle participant takes the lock", async () => {
    expect(confirmWitnessPlatform.constants.O_NOFOLLOW).toBeUndefined();
    await expectNoWitnessSharing();
  });

  // Review round 4 on #592: the descriptor must be the file lstat saw. A regular file of this user
  // swapped in between lstat and open passes fstat alone; its dev/ino differ, so it is not truncated.
  it.skipIf(fs.constants.O_NOFOLLOW === undefined)("refuses a witness swapped between lstat and open (dev/ino mismatch)", async () => {
    const f = await idleFixture();
    const witness = plantedWitness(f.mesh.root);
    fs.writeFileSync(witness, "seen");
    const old = new Date(Date.now() - 3_600_000);
    fs.utimesSync(witness, old, old);                          // no evidence: the heartbeat takes the lock
    const swapped = path.join(f.mesh.root, "swapped-in.txt");
    fs.writeFileSync(swapped, "keep");
    const realOpen = fs.openSync.bind(fs) as (...args: unknown[]) => number;
    let raced = 0, witnessFd = -1;
    vi.spyOn(fs, "openSync").mockImplementation(((...args: unknown[]) => {
      if (args[0] !== witness) return realOpen(...args);
      fs.renameSync(swapped, witness); raced++;
      return (witnessFd = realOpen(...args));
    }) as typeof fs.openSync);
    const truncations = vi.spyOn(fs, "ftruncateSync");
    await f.directory.refresh();                               // nothing unseen: the lock path touches the witness
    expect(f.confirmations).toHaveBeenCalledOnce();
    expect(raced).toBe(1);
    expect(truncations.mock.calls.some(([fd]) => fd === witnessFd)).toBe(false);
    expect(fs.readFileSync(witness, "utf8")).toBe("keep");    // the swapped-in file was not truncated
  });
});

// smarty-dev#11172 — Decision C: common, unit-level (real MeshStore files, injected clock/timer spies), not E2E.
// Reason: real incident #11172 (R-no-polling): the idle participant lease heartbeat woke every Main and resident
// every 5 s; chief-of-staff refused an idle-wake allowlist entry. Failure boundaries: an old reader that joins between
// 60 s heartbeats (state-commit wake, file + SQLite), and a stale compatibility tick (cancel paths). The E2E proof is the
// idle-wake gate (docs/idle-wake-gate.md), main and resident modes, owner fv2-11172wake/event-lead: overall <= 1 wake/s
// and no non-allowed site > 1/min.
describe("ParticipantDirectory default lease cadence (smarty-dev#11172)", () => {
  const owned: Array<{ mesh: MeshStore; directory: ParticipantDirectory }> = [];
  afterEach(async () => {
    // Close the borrowed store after its publisher, before Windows fixture-root cleanup.
    for (const { mesh, directory } of owned.splice(0)) {
      try { await directory.close(); } finally { mesh.closeState(); }
    }
  });
  // Deliver the production root-watch callback after a real commit, not after a wall-clock wait.
  const stateCommitWatch = (mesh: MeshStore) => {
    let listener: ((event: string, name: string) => void) | undefined;
    const realWatch = fs.watch;
    const spy = vi.spyOn(fs, "watch").mockImplementation(((target: fs.PathLike, ...rest: unknown[]) => {
      if (String(target) !== mesh.root) return (realWatch as any)(target, ...rest);
      listener = rest.at(-1) as typeof listener;
      return fakeWatcher();
    }) as typeof fs.watch);
    return {
      notify: (name = mesh.stateBackend === "sqlite" ? "state.db-wal" : "state.json") => {
        expect(listener).toBeTypeOf("function");
        listener!("change", name);
      },
      restore: () => spy.mockRestore(),
    };
  };
  const settledRefresh = async (refresh: Promise<void>) => {
    await refresh;
    // Clear the background runner's in-flight guard before testing the no-loop counterexamples.
    await new Promise<void>(resolve => setImmediate(resolve));
  };
  const identityOf = (name: string): MeshIdentity => ({ id: `session:${name}`, name: "main", kind: "main", sessionId: name });
  const fixture = (name: string, stateBackend: "file" | "sqlite" = "file") => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-11172-"));
    roots.push(root);
    const identity = identityOf(name);
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 1_000, { stateBackend });
    // No heartbeatMs/leaseMs: the production defaults.
    const directory = new ParticipantDirectory(mesh, { enabled: true, hostId: identity.id, rootId: identity.id, identity, reapDeadHosts: false });
    directory.registerSource(() => [rootRecord(identity.id, identity.id, name)]);
    directories.push(directory);
    owned.push({ mesh, directory });
    return { mesh, identity, directory };
  };
  const addStateOnlyReader = async (mesh: MeshStore, now: number) => {
    const peer = identityOf("state-only");
    await mesh.put({ key: "sessions/state-only", identity: peer, value: { id: peer.id, sessionId: "state-only",
      cwd: "/tmp/project", startedAt: now, status: "idle" } });
  };

  it("heartbeats every 60 s with a 180 s host and Main session TTL, fencing consumption after two beats", async () => {
    const { mesh, identity, directory } = fixture("idle");
    const intervals = vi.spyOn(globalThis, "setInterval");
    const timeouts = vi.spyOn(globalThis, "setTimeout");
    try {
      await directory.start();
      expect(PARTICIPANT_HEARTBEAT_MS).toBe(60_000);
      expect(DEFAULT_PARTICIPANT_LEASE_MS).toBe(3 * PARTICIPANT_HEARTBEAT_MS);
      expect(intervals.mock.calls.map(([, ms]) => ms)).toEqual([PARTICIPANT_HEARTBEAT_MS]);
      const lease = readHostLeases(mesh.root).get(identity.id)!;
      expect(lease.expiresAt - lease.updatedAt).toBe(180_000);
      // A capable reader takes the Main session from this file: it must not lapse after 15 s.
      expect(lease.session).toMatchObject({ expiresAt: lease.expiresAt, hostTtl: 1 });
      expect(sessionLiveness(mesh.get("sessions/idle", { fresh: true })!, mesh.root).expiresAt).toBe(lease.session!.expiresAt);
      expect(directory.canConsumeMesh()).toBe(true);
      // Without an old reader no extra cadence timer exists: the idle host wakes only on its heartbeat.
      expect(timeouts.mock.calls.filter(([, ms]) => ms === 5_000)).toHaveLength(0);
      const confirmationDeadline = directory.confirmedAt() + 2 * PARTICIPANT_HEARTBEAT_MS;
      expect(confirmationDeadline).toBeLessThan(lease.expiresAt);
      expect(directory.canConsumeMesh(confirmationDeadline - 1)).toBe(true);
      // Record the recovery request without actually renewing: elapsed confirmation must fail closed
      // even while the host and capable Main session leases still have one heartbeat remaining.
      const refresh = vi.spyOn(directory, "refresh").mockResolvedValue();
      try {
        expect(directory.canConsumeMesh(confirmationDeadline)).toBe(false);
        expect(directory.canConsumeMesh(confirmationDeadline + 1)).toBe(false);
        await new Promise<void>(resolve => setImmediate(resolve));
        expect(refresh).toHaveBeenCalledOnce();
      } finally { refresh.mockRestore(); }
    } finally {
      intervals.mockRestore();
      timeouts.mockRestore();
    }
  });

  it("keeps the fixed 15 s sessions/ entry renewed at the old cadence only while a state-only reader is live", async () => {
    const { mesh, directory } = fixture("mixed");
    let now = Date.now();
    const clock = vi.spyOn(Date, "now").mockImplementation(() => now);
    const timeouts = vi.spyOn(globalThis, "setTimeout");
    const legacyTicks = () => timeouts.mock.calls.filter(([, ms]) => ms === 5_000).length;
    try {
      await addStateOnlyReader(mesh, now);
      await directory.start();
      expect(legacyTicks()).toBe(1);                          // one deadline-bounded tick, not a poll
      const startedAt = now;
      for (let tick = 1; tick <= 3; tick++) {
        now = startedAt + tick * 5_000;
        await addStateOnlyReader(mesh, now);                   // the old reader keeps renewing
        const armed = timeouts.mock.calls.filter(([, ms]) => ms === 5_000).at(-1)!;
        (armed[0] as () => void)();                            // fire the armed tick (no 5 s real wait)
        // Each legacy tick renews the fixed-TTL entry and re-arms exactly one next tick.
        await vi.waitFor(() => expect(legacyTicks()).toBe(tick + 1));
        expect(now - mesh.get("sessions/mixed", { fresh: true })!.updatedAt).toBeLessThan(15_000);
      }
      // The reader stops renewing: past its 15 s deadline nothing more is armed.
      now += 20_000;
      const before = legacyTicks();
      (timeouts.mock.calls.filter(([, ms]) => ms === 5_000).at(-1)![0] as () => void)();
      await directory.refresh();
      expect(legacyTicks()).toBe(before);
    } finally {
      clock.mockRestore();
      timeouts.mockRestore();
    }
  });

  // Failure boundary (#11172 review): an old reader joining AFTER a normal lease cached
  // legacy-needed=false saw our 15 s sessions/ entry lapse for up to one 60 s heartbeat.
  for (const backend of ["file", "sqlite"] as const) {
    it(`a state-only reader joining between heartbeats wakes one prompt refresh through the ${backend} state commit, without a loop`, async () => {
      const { mesh, directory } = fixture("join", backend);
      if (mesh.stateBackend !== backend) return; // no node:sqlite here: nothing to prove for this backend
      const commits = stateCommitWatch(mesh);
      // Another store instance on the same root: its own snapshot cache, its own commits (as another process).
      const other = new MeshStore(mesh.root, 64 * 1024, 1_000, { stateBackend: backend });
      let now = Date.now();
      const clock = vi.spyOn(Date, "now").mockImplementation(() => now);
      const timeouts = vi.spyOn(globalThis, "setTimeout");
      const legacyTicks = () => timeouts.mock.calls.filter(([, ms]) => ms === 5_000).length;
      try {
        await directory.start();
        expect(legacyTicks()).toBe(0);
        const scheduled = vi.spyOn(directory, "refresh");
        // 20 s into the 60 s heartbeat: our fixed-TTL sessions/ entry already looks dead to an old reader.
        now += 20_000;
        const joinedAt = now;
        await addStateOnlyReader(other, joinedAt);
        commits.notify("events.jsonl"); // a non-state notification must not carry the join
        expect(scheduled).not.toHaveBeenCalled();
        commits.notify();
        expect(scheduled).toHaveBeenCalledOnce();
        await settledRefresh(scheduled.mock.results[0]!.value);
        expect(mesh.get("sessions/join", { fresh: true })!.updatedAt).toBe(joinedAt);
        expect(legacyTicks()).toBe(1);
        commits.notify(); // our own publication and its duplicate must not loop
        commits.notify();
        await other.put({ key: "unrelated/join-control", identity: identityOf("writer"), value: true });
        commits.notify();
        expect(scheduled).toHaveBeenCalledTimes(1);
        expect(legacyTicks()).toBe(1);
      } finally {
        clock.mockRestore();
        timeouts.mockRestore();
        commits.restore();
        if (backend === "sqlite") other.closeState();
      }
    }, 15_000);
  }

  it.each(["file", "sqlite"] as const)("does not turn unrelated %s commits into idle refreshes or compatibility timers", async backend => {
    const { mesh, directory } = fixture("no-old-reader", backend);
    const commits = stateCommitWatch(mesh);
    const other = new MeshStore(mesh.root, 64 * 1024, 1_000, { stateBackend: backend });
    const timeouts = vi.spyOn(globalThis, "setTimeout");
    const intervals = vi.spyOn(globalThis, "setInterval");
    try {
      await directory.start();
      const before = mesh.get("sessions/no-old-reader", { fresh: true });
      const scheduled = vi.spyOn(directory, "refresh");
      const tokens = vi.spyOn(mesh, "stateToken");
      await other.put({ key: "unrelated/join-control", identity: identityOf("writer"), value: { changed: true } });
      commits.notify();
      expect(tokens).toHaveBeenCalled();
      expect(scheduled).not.toHaveBeenCalled();
      expect(timeouts.mock.calls.filter(([, ms]) => ms === 5_000)).toHaveLength(0);
      expect(intervals.mock.calls.map(([, ms]) => ms)).toEqual([60_000]);
      expect(mesh.get("sessions/no-old-reader", { fresh: true })).toEqual(before);
    } finally {
      timeouts.mockRestore();
      intervals.mockRestore();
      commits.restore();
      if (backend === "sqlite") other.closeState();
    }
  });

  // CODE-1: a failed final tick must not leave A's join edge latched after its cadence expires.
  // Keep reason: transient pre-read failure + reader expiry is not cheaply reachable in a user flow.
  it.each(["file", "sqlite"] as const)("wakes exactly once for reader B after A expires and its final %s compatibility refresh fails", async backend => {
    const { mesh, directory } = fixture("expiry-join", backend);
    const commits = stateCommitWatch(mesh);
    expect(mesh.stateBackend).toBe(backend);
    const other = new MeshStore(mesh.root, 64 * 1024, 1_000, { stateBackend: backend });
    let now = Date.now();
    const clock = vi.spyOn(Date, "now").mockImplementation(() => now);
    const timeouts = vi.spyOn(globalThis, "setTimeout");
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const legacyTicks = () => timeouts.mock.calls.filter(([, ms]) => ms === 5_000).length;
    try {
      await directory.start();
      const scheduled = vi.spyOn(directory, "refresh");
      now += 30_000;
      await addStateOnlyReader(other, now);
      commits.notify();
      expect(scheduled).toHaveBeenCalledOnce();
      await settledRefresh(scheduled.mock.results[0]!.value);
      expect(legacyTicks()).toBe(1);
      now += 15_001; // A's fixed lease has expired; no intervening commit clears its latch.
      const failure = new Error("final compatibility refresh failed before canonical read");
      directory.options.beforePublication = async () => { throw failure; };
      const tick = timeouts.mock.calls.findIndex(([, ms]) => ms === 5_000);
      clearTimeout(timeouts.mock.results[tick]!.value);
      (timeouts.mock.calls[tick]![0] as () => void)();
      await expect(scheduled.mock.results.at(-1)!.value).rejects.toBe(failure);
      await vi.waitFor(() => expect(warn).toHaveBeenCalledOnce()); // background retry recorded the failure
      expect(legacyTicks()).toBe(1); // the expired cadence was cancelled, not re-polled
      delete directory.options.beforePublication;
      scheduled.mockClear();
      now += 1_001; // the existing non-transient background backoff, not another heartbeat
      const peer = identityOf("reader-b");
      await other.put({ key: "sessions/reader-b", identity: peer, value: { id: peer.id, sessionId: "reader-b",
        cwd: "/tmp/project", startedAt: now, status: "idle" } });
      commits.notify();
      expect(scheduled).toHaveBeenCalledOnce();
      await settledRefresh(scheduled.mock.results[0]!.value);
      expect(mesh.get("sessions/expiry-join", { fresh: true })!.updatedAt).toBe(now);
      expect(legacyTicks()).toBe(2);
      commits.notify(); // own publication
      // B's refresh commits to the same watch; neither that own commit nor unrelated commits loop.
      const tokens = vi.spyOn(mesh, "stateToken");
      await other.put({ key: "unrelated/expiry-join-control", identity: identityOf("writer"), value: true });
      commits.notify();
      expect(tokens).toHaveBeenCalled();
      expect(scheduled).toHaveBeenCalledOnce();
      expect(legacyTicks()).toBe(2);
    } finally {
      delete directory.options.beforePublication;
      clock.mockRestore();
      timeouts.mockRestore();
      warn.mockRestore();
      commits.restore();
      if (backend === "sqlite") other.closeState();
    }
  });

  it.each(["file", "sqlite"] as const)("does not reset the join latch for successful %s refreshes without a legacy session to publish", async backend => {
    const { mesh, identity, directory } = fixture("no-legacy-put", backend);
    const commits = stateCommitWatch(mesh);
    expect(mesh.stateBackend).toBe(backend);
    const other = new MeshStore(mesh.root, 64 * 1024, 1_000, { stateBackend: backend });
    let status = "idle";
    directory.registerSource(() => {
      const record = { ...rootRecord(identity.id, identity.id, "no-legacy-put"), status };
      delete record.cwd;
      delete record.sessionId;
      return [record];
    });
    const timeouts = vi.spyOn(globalThis, "setTimeout");
    try {
      await directory.start();
      const scheduled = vi.spyOn(directory, "refresh");
      await addStateOnlyReader(other, Date.now());
      commits.notify();
      expect(scheduled).toHaveBeenCalledOnce();
      await settledRefresh(scheduled.mock.results[0]!.value);
      commits.notify(); // own publication
      status = "running";
      await directory.refresh(); // real own state commit, but still no legacyPut/cadence
      commits.notify();
      const tokens = vi.spyOn(mesh, "stateToken");
      await other.put({ key: "unrelated/no-legacy-put", identity: identityOf("writer"), value: true });
      commits.notify();
      expect(tokens).toHaveBeenCalled();
      expect(scheduled).toHaveBeenCalledTimes(2); // join + explicit refresh only
      expect(mesh.get("sessions/no-legacy-put", { fresh: true })).toBeUndefined();
      expect(timeouts.mock.calls.filter(([, ms]) => ms === 5_000)).toHaveLength(0);
    } finally {
      timeouts.mockRestore();
      commits.restore();
      if (backend === "sqlite") other.closeState();
    }
  });

  // The join wake can share an idle refresh whose preparation still saw no old reader.
  // Its post-confirmation read must carry BOTH the session renewal and compatibility deadline.
  it.each(["file", "sqlite"] as const)("keeps the join deadline when the %s commit wake joins an in-flight confirmation", async backend => {
    const { mesh, directory } = fixture("confirm-join", backend);
    const commits = stateCommitWatch(mesh);
    const other = new MeshStore(mesh.root, 64 * 1024, 1_000, { stateBackend: backend });
    let now = Date.now();
    const clock = vi.spyOn(Date, "now").mockImplementation(() => now);
    const timeouts = vi.spyOn(globalThis, "setTimeout");
    const legacyTicks = () => timeouts.mock.calls.filter(([, ms]) => ms === 5_000).length;
    let release!: () => void;
    const held = new Promise<void>(resolve => { release = resolve; });
    let entered!: () => void;
    const confirming = new Promise<void>(resolve => { entered = resolve; });
    const confirm = mesh.confirmWritable.bind(mesh);
    let refreshing: Promise<void> | undefined;
    try {
      await directory.start();
      now += 30_000;
      vi.spyOn(mesh, "confirmWritable").mockImplementationOnce(async callback => {
        entered();
        await held;
        await confirm(callback);
      });
      refreshing = directory.refresh();
      await confirming;
      const scheduled = vi.spyOn(directory, "refresh");
      await addStateOnlyReader(other, now);
      commits.notify();
      expect(scheduled).toHaveBeenCalledTimes(1);
      release();
      await refreshing;
      await settledRefresh(scheduled.mock.results[0]!.value);
      expect(mesh.get("sessions/confirm-join", { fresh: true })!.updatedAt).toBe(now);
      expect(legacyTicks()).toBe(1);
      commits.notify(); // own publication
      expect(scheduled).toHaveBeenCalledTimes(1); // own publication does not wake another refresh
      expect(legacyTicks()).toBe(1);
    } finally {
      release();
      await refreshing;
      clock.mockRestore();
      timeouts.mockRestore();
      commits.restore();
      if (backend === "sqlite") other.closeState();
    }
  });

  it("control: without the root watch the joining reader waits for the heartbeat (the watch is the carrier)", async () => {
    const { mesh, directory } = fixture("unwatched");
    const other = new MeshStore(mesh.root, 64 * 1024, 1_000);
    const realWatch = fs.watch;
    const watched = vi.spyOn(fs, "watch").mockImplementation(((target: fs.PathLike, ...rest: unknown[]) =>
      String(target) === mesh.root ? new (class extends (Object as any) { close() {} on() { return this; } })()
        : (realWatch as any)(target, ...rest)) as typeof fs.watch);
    try {
      await directory.start();
      const scheduled = vi.spyOn(directory, "refresh");
      await addStateOnlyReader(other, Date.now());
      await new Promise((resolve) => setTimeout(resolve, 1_000));
      expect(scheduled).not.toHaveBeenCalled();
    } finally {
      watched.mockRestore();
    }
  });

  // CODE-3/SEC-1 (#11172 audit): the root watch is the required join carrier for the 60 s default; its
  // failure must fail closed, never fall back to a timer or poll. Decision C: unit-level, fs.watch stubbed.
  const fakeWatcher = () => {
    const emitter = new EventEmitter() as EventEmitter & { close: () => void };
    emitter.close = vi.fn();
    return emitter;
  };
  const stubRootWatch = (root: string, plan: Array<"throw" | "fake">) => {
    const realWatch = fs.watch;
    const made: ReturnType<typeof fakeWatcher>[] = [];
    let calls = 0;
    const spy = vi.spyOn(fs, "watch").mockImplementation(((target: fs.PathLike, ...rest: unknown[]) => {
      if (String(target) !== root) return (realWatch as any)(target, ...rest);
      const step = plan[calls++] ?? "throw";
      if (step === "throw") throw Object.assign(new Error("ENOSPC: watch limit"), { code: "ENOSPC" });
      const watcher = fakeWatcher();
      made.push(watcher);
      return watcher;
    }) as typeof fs.watch);
    return { spy, made, calls: () => calls };
  };
  const noTimers = () => {
    const timeouts = vi.spyOn(globalThis, "setTimeout");
    const intervals = vi.spyOn(globalThis, "setInterval");
    return {
      created: () => [...timeouts.mock.calls, ...intervals.mock.calls].map(([, ms]) => ms),
      restore: () => { timeouts.mockRestore(); intervals.mockRestore(); },
    };
  };

  it("retries a failed join-watch registration once synchronously and then serves normally", async () => {
    const { mesh, directory } = fixture("retry-ok");
    const watch = stubRootWatch(mesh.root, ["throw", "fake"]);
    try {
      await directory.start();
      expect(watch.calls()).toBe(2);
      expect(directory.heartbeatArmed).toBe(true);
      expect(directory.canConsumeMesh()).toBe(true);
    } finally { watch.spy.mockRestore(); }
  });

  it("rejects start and cancels the heartbeat when the join watch cannot be registered twice, without publishing", async () => {
    const { mesh, directory } = fixture("retry-fail");
    const watch = stubRootWatch(mesh.root, ["throw", "throw"]);
    const timers = noTimers();
    const clears = vi.spyOn(globalThis, "clearInterval");
    try {
      await expect(directory.start()).rejects.toThrow(/join watch .* could not be registered/);
      expect(watch.calls()).toBe(2);                       // exactly one synchronous retry
      expect(directory.heartbeatArmed).toBe(false);
      expect(clears).toHaveBeenCalledTimes(1);              // the 60 s heartbeat was cancelled
      expect(timers.created().filter((ms) => ms !== 60_000)).toEqual([]); // no degraded retry timer/poll
      expect(mesh.get("sessions/retry-fail", { fresh: true })).toBeUndefined(); // never advertised
      expect(directory.canConsumeMesh()).toBe(false);
    } finally {
      watch.spy.mockRestore();
      timers.restore();
      clears.mockRestore();
    }
  });

  it("re-registers a watch lost to a native error once and checks the state once", async () => {
    const { mesh, directory } = fixture("rearm");
    const watch = stubRootWatch(mesh.root, ["fake", "fake"]);
    try {
      await directory.start();
      const timers = noTimers();
      const tokens = vi.spyOn(mesh, "stateToken");
      try {
        watch.made[0]!.emit("error", new Error("EPERM"));
        expect(watch.made[0]!.close).toHaveBeenCalled();
        expect(watch.calls()).toBe(2);
        expect(tokens).toHaveBeenCalledTimes(1);            // one bounded state check, no loop
        expect(timers.created()).toEqual([]);              // no recovery timer
        expect(directory.heartbeatArmed).toBe(true);
        expect(directory.degraded).toBe(false);
        expect(directory.canConsumeMesh()).toBe(true);
      } finally {
        tokens.mockRestore();
        timers.restore();
      }
    } finally { watch.spy.mockRestore(); }
  });

  it("fails closed after a second watch loss: degraded, heartbeat stopped, consumption fenced, owner told once", async () => {
    const identity = identityOf("lost");
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-11172-"));
    roots.push(root);
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 1_000);
    const lost = vi.fn();
    const directory = new ParticipantDirectory(mesh, { enabled: true, hostId: identity.id, rootId: identity.id, identity,
      reapDeadHosts: false, onLifecycleLost: lost });
    directory.registerSource(() => [rootRecord(identity.id, identity.id, "lost")]);
    directories.push(directory);
    const watch = stubRootWatch(mesh.root, ["fake", "fake"]);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    try {
      await directory.start();
      const timers = noTimers();
      try {
        watch.made[0]!.emit("close");                       // unexpected close: the one re-arm
        watch.made[1]!.emit("error", new Error("EIO"));    // repeated failure: fail closed
        watch.made[1]!.emit("close");                       // trailing close is not a third event
        expect(watch.calls()).toBe(2);
        expect(directory.degraded).toBe(true);
        expect(directory.heartbeatArmed).toBe(false);
        expect(directory.canConsumeMesh()).toBe(false);
        expect(lost).toHaveBeenCalledTimes(1);
        expect(lost).toHaveBeenCalledWith(expect.any(Number), "participant join watch failed");
        expect(timers.created()).toEqual([]);
      } finally { timers.restore(); }
    } finally {
      watch.spy.mockRestore();
      warn.mockRestore();
    }
  });

  it("cancels the compatibility tick on suspend, on close and when the old reader lapses", async () => {
    const { mesh, directory } = fixture("cancel");
    let now = Date.now();
    const clock = vi.spyOn(Date, "now").mockImplementation(() => now);
    const timeouts = vi.spyOn(globalThis, "setTimeout");
    const clears = vi.spyOn(globalThis, "clearTimeout");
    const armed = () => timeouts.mock.results.filter((_r, index) => timeouts.mock.calls[index]![1] === 5_000).at(-1)!.value;
    try {
      await addStateOnlyReader(mesh, now);
      await directory.start();
      let handle = armed();
      directory.suspend();
      expect(clears).toHaveBeenCalledWith(handle);
      directory.resume();
      await directory.refresh();
      handle = armed();
      // The reader lapses: the next refresh drops the pending tick instead of keeping it.
      now += 20_000;
      await directory.refresh();
      expect(clears).toHaveBeenCalledWith(handle);
      await addStateOnlyReader(mesh, now);
      await directory.refresh();
      handle = armed();
      await directory.close();
      expect(clears).toHaveBeenCalledWith(handle);
    } finally {
      clock.mockRestore();
      timeouts.mockRestore();
      clears.mockRestore();
    }
  });
});
