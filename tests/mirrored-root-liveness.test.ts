import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import { afterEach, describe, expect, it, vi } from "vitest";
import { BridgeMirrorDeferredError, MeshBridge, RemoteBridgeSide, serveBridgeAgent, StoreBridgeSide, type BridgePresence } from "../src/mesh/bridge.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { CommitOutbox } from "../src/mesh/commit-outbox.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { AgentMessageRouter } from "../src/providers/agents-message-router.js";
import { FabricBridgeStaleError, hostMirrorLease, mirroredLeaseStatus, mirroredOwnerRoutingStatus, MIRROR_FRESH_MS, hostLeasePath, readHostLease, writeHostLease, type FabricMirrorLease } from "../src/topology/host-leases.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { deadHostRecords, DEAD_HOST_RECORDS_MS } from "../src/topology/host-reaper.js";

// No deletion in this suite (#11199); isolated fixture bytes are retained for the owner.
const close: Array<() => void | Promise<void>> = [];
afterEach(async () => { for (const stop of close.splice(0)) await stop(); vi.useRealTimers(); vi.restoreAllMocks(); });
const key = (prefix: string, id: string) => prefix + createHash("sha256").update(id).digest("hex");
const rootId = "session:ryzen4-lane-00000000";
const senderId = "session:ryzen1-lead-00000000";
const identity: MeshIdentity = { id: rootId, name: "lane", kind: "main", sessionId: "ryzen4-lane-00000000" };
const mirror = (at: number, patch: Partial<FabricMirrorLease> = {}): FabricMirrorLease => ({ remoteHost: "ryzen4",
  observedAt: at, sourceObservedAt: at - 60_000, sourceUpdatedAt: at - 62_000, sourceExpiresAt: at - 47_000, ...patch });

const fixture = async (sourceOffset = -60_000, sourceTtl = 13_000, path_: "cold" | "warm" = "warm") => {
  let now = Date.now();
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "mirror-liveness-"));
  const store = new MeshStore(path.join(dir, "hub"), 64 * 1024, 100);
  const local = new StoreBridgeSide(store, "ryzen4", () => now);
  const record = { format: 1 as const, id: rootId, rootId, identity, incarnationToken: "incarnation-1",
    startedAt: now - 120_000, updatedAt: now + sourceOffset - 2_000, expiresAt: now + sourceOffset + sourceTtl };
  const participant = { format: 1 as const, id: rootId, rootId, kind: "root" as const, name: "lane", ownerHostId: rootId,
    ownerIdentityId: rootId, ownerIncarnation: "activation-1", status: "idle", runner: "pi" as const, transport: "host" as const,
    capabilities: ["steer" as const, "followUp" as const], controlProtocol: "v1" as const, sessionId: identity.sessionId!,
    startedAt: record.startedAt, updatedAt: record.updatedAt };
  const presence: BridgePresence = { hosts: [{ record, expiresAt: record.expiresAt, observedAt: now + sourceOffset }],
    participants: [participant], reserved: [] };
  if (path_ === "warm") await local.mirror(presence, now);
  const directory = new ParticipantDirectory(store, { enabled: true, hostId: senderId, rootId: senderId,
    identity: { id: senderId, name: "lead", kind: "main" } });
  const control = new FabricControlPlane(store, { id: senderId, name: "lead", kind: "main" }, {
    enabled: true, hostId: senderId, now: () => now, pollMs: 10,
    readMirroredOwner: (...args) => directory.mirroredControlOwner(...args),
  });
  control.start(() => ({ accepted: false }));
  close.push(() => control.close());
  const lease = () => readHostLease(store.root, rootId)!;
  const advertise = (observation: FabricMirrorLease) => writeHostLease(store.root, { ...lease(), mirror: observation });
  return { store, local, directory, control, presence, lease, advertise, now: () => now, advance: (ms: number) => { now += ms; } };
};

const settle = <T>(promise: Promise<T>) => promise.then(value => ({ value, error: undefined }), (error: Error) => ({ value: undefined, error }));
const request = (f: Awaited<ReturnType<typeof fixture>>) => f.control.request(rootId, rootId, "followUp", {
  ownerIncarnation: "activation-1", message: "recorded-path probe" }, rootId, { routedRemoteHost: "ryzen4" });
const commands = (f: Awaited<ReturnType<typeof fixture>>) => f.store.read({ topic: "fabric.control.command" });

describe("source-clock root liveness (#10068)", () => {
  it.each([-60_000, 60_000])("judges the native sample, not hub/source skew (%i ms), then accepts only the attributed ACK", async offset => {
    const f = await fixture(offset);
    expect(f.directory.get(rootId, f.now())).toMatchObject({ id: rootId, stale: false, remoteHost: "ryzen4" });
    expect(f.lease().mirror).toMatchObject({ remoteHost: "ryzen4", sourceObservedAt: f.now() + offset });
    const outcome = request(f);
    void outcome.catch(() => undefined);
    await vi.waitFor(() => expect(commands(f)).toHaveLength(1));
    const commandId = (commands(f)[0]!.data as { commandId: string }).commandId;
    await f.store.publish({ topic: "fabric.control.ack", kind: "accepted", from: identity, to: senderId,
      data: { version: 1, commandId, targetId: rootId, ownerIncarnation: "activation-1", accepted: true,
        messageId: "accepted", bridge: { from: "wrong-host" } } });
    let resolved = false;
    void outcome.then(() => { resolved = true; });
    await new Promise(resolve => setTimeout(resolve, 30));
    expect(resolved).toBe(false);
    await f.store.publish({ topic: "fabric.control.ack", kind: "accepted", from: identity, to: senderId,
      data: { version: 1, commandId, targetId: rootId, ownerIncarnation: "activation-1", accepted: true,
        messageId: "accepted", bridge: { from: "ryzen4" } } });
    await expect(outcome).resolves.toMatchObject({ acknowledged: true });
  });

  it.each(["v1", "legacy"] as const)("the agent message router rejects a stale %s bridge instead of returning unknown or queued", async protocol => {
    const f = await fixture();
    vi.spyOn(Date, "now").mockImplementation(f.now);
    const sender = { id: senderId, name: "lead", kind: "main" as const };
    const steerRemote = vi.fn(async () => ({ queued: true }));
    const router = new AgentMessageRouter({} as any, { identity: sender, steerRemote } as any,
      { local: true, id: senderId, matches: (id: string) => id === senderId } as any,
      f.directory, f.control, binding => binding);
    const healthy = router.routeMessage(rootId, "healthy recorded call", undefined, "followUp");
    void healthy.catch(() => undefined);
    await vi.waitFor(() => expect(commands(f)).toHaveLength(1));
    const commandId = (commands(f)[0]!.data as { commandId: string }).commandId;
    await f.store.publish({ topic: "fabric.control.ack", kind: "accepted", from: identity, to: senderId,
      data: { version: 1, commandId, targetId: rootId, ownerIncarnation: "activation-1", accepted: true,
        messageId: "router-accepted", bridge: { from: "ryzen4" } } });
    await expect(healthy).resolves.toMatchObject({ acknowledged: true, routed: "mesh" });
    await f.local.mirror({ ...f.presence, participants: f.presence.participants.map(p => ({ ...p, controlProtocol: protocol })) }, f.now());
    f.advance(MIRROR_FRESH_MS + 1);
    await expect(router.routeMessage(rootId, "no fresh bridge", undefined, "followUp"))
      .rejects.toMatchObject({ code: "FABRIC_BRIDGE_STALE" });
    expect(commands(f)).toHaveLength(1);
    expect(steerRemote).not.toHaveBeenCalled();
  });

  it.each(["steer", "followUp"] as const)("refuses %s at the translated short source TTL while its observation is fresh, then admits a renewed sample", async operation => {
    const f = await fixture(-60_000, 1_000);
    vi.spyOn(Date, "now").mockImplementation(f.now);
    expect(f.directory.get(rootId, f.now())?.bridgeStale).toBeUndefined();
    const expiresAt = f.lease().expiresAt;
    expect(expiresAt).toBe(f.now() + 1_000);
    f.advance(1_000); // Equality is expired authority, even inside the 15 s freshness window.
    expect(mirroredLeaseStatus("ryzen4", f.lease().mirror, f.now())).toBe("live");
    expect(f.local.holds(rootId)).toBe(false);
    const refusal = settle(f.control.request(rootId, rootId, operation, {
      ownerIncarnation: "activation-1", message: "expired translated authority" }, rootId, { routedRemoteHost: "ryzen4" }));
    let settled = false;
    void refusal.then(() => { settled = true; });
    await vi.waitFor(() => expect(settled || commands(f).length > 0).toBe(true));
    expect(commands(f)).toHaveLength(0); // The old head publishes here: this is the negative control.
    expect((await refusal).error).toMatchObject({ code: "FABRIC_BRIDGE_STALE", remoteHost: "ryzen4", targetId: rootId,
      message: expect.stringContaining("this attempt was not published") });
    expect(f.directory.get(rootId, f.now())).toMatchObject({ remoteHost: "ryzen4", bridgeStale: true });
    expect(f.directory.list({ scope: "project", fresh: true }, f.now()).find(p => p.id === rootId))
      .toMatchObject({ remoteHost: "ryzen4", bridgeStale: true });

    const sourceAt = f.now() - 60_000;
    await f.local.mirror({ ...f.presence, hosts: [{ ...f.presence.hosts[0]!, observedAt: sourceAt,
      record: { ...f.presence.hosts[0]!.record, updatedAt: sourceAt, expiresAt: sourceAt + 1_000 },
      expiresAt: sourceAt + 1_000 }] }, f.now());
    expect(f.directory.get(rootId, f.now())?.bridgeStale).toBeUndefined();
    const renewed = f.control.request(rootId, rootId, operation, { ownerIncarnation: "activation-1", message: "renewed" },
      rootId, { routedRemoteHost: "ryzen4" });
    void renewed.catch(() => undefined);
    await vi.waitFor(() => expect(commands(f)).toHaveLength(1));
    const commandId = (commands(f)[0]!.data as { commandId: string }).commandId;
    await f.store.publish({ topic: "fabric.control.ack", kind: "accepted", from: identity, to: senderId,
      data: { version: 1, commandId, targetId: rootId, ownerIncarnation: "activation-1", accepted: true,
        messageId: "renewed-accepted", bridge: { from: "ryzen4" } } });
    await expect(renewed).resolves.toMatchObject({ acknowledged: true, routed: "mesh" });
  });

  // Remote-authority trust boundary; retained for the recorded Astra catch, not a timing smoke.
  it.each([
    ["cold", 1_000], ["warm", 1_000], ["cold", 250], ["warm", 250],
  ] as const)("a blocked mirror preserves the original 500 ms authority deadline (%s, %i ms wait)", async (path_, wait) => {
    const f = await fixture(-60_000, 500, path_);
    vi.spyOn(Date, "now").mockImplementation(f.now);
    const observedAt = f.now();
    const originalDeadline = observedAt + 500;
    const write = f.store.writeBatch.bind(f.store);
    const blocked = vi.spyOn(f.store, "writeBatch").mockImplementation(async input => {
      f.advance(wait); // Recorded lock wait, not a real sleep or a renewed source observation.
      return write(input);
    });
    const stateViews = vi.spyOn(f.store, "stateView");
    await f.local.mirror(f.presence, observedAt);
    expect(blocked).toHaveBeenCalledTimes(1);
    expect(stateViews).toHaveBeenCalledTimes(path_ === "warm" ? 1 : 0);
    blocked.mockRestore();
    stateViews.mockRestore();
    expect(f.lease().expiresAt).toBe(originalDeadline);
    expect(f.lease().mirror).toMatchObject({ observedAt, remoteHost: "ryzen4",
      sourceObservedAt: observedAt - 60_000, sourceExpiresAt: observedAt - 60_000 + 500 });
    expect(mirroredLeaseStatus("ryzen4", f.lease().mirror, f.now())).toBe("live");

    const sender = { id: senderId, name: "lead", kind: "main" as const };
    const steerRemote = vi.fn(async () => ({ queued: true }));
    const router = new AgentMessageRouter({} as any, { identity: sender, steerRemote } as any,
      { local: true, id: senderId, matches: (id: string) => id === senderId } as any,
      f.directory, f.control, binding => binding);
    const admit = async (operation: "steer" | "followUp") => {
      const before = commands(f).length;
      const outcome = router.routeMessage(rootId, "within original source life", undefined, operation);
      void outcome.catch(() => undefined);
      await vi.waitFor(() => expect(commands(f)).toHaveLength(before + 1));
      const commandId = (commands(f)[before]!.data as { commandId: string }).commandId;
      await f.store.publish({ topic: "fabric.control.ack", kind: "accepted", from: identity, to: senderId,
        data: { version: 1, commandId, targetId: rootId, ownerIncarnation: "activation-1", accepted: true,
          messageId: `${operation}-accepted`, bridge: { from: "ryzen4" } } });
      await expect(outcome).resolves.toMatchObject({ acknowledged: true, routed: "mesh" });
    };
    if (wait < 500) {
      expect(f.local.holds(rootId)).toBe(true);
      expect(f.directory.get(rootId, f.now())?.bridgeStale).toBeUndefined();
      // Positive counterexample: even one millisecond before the ORIGINAL deadline is admissible.
      f.advance(originalDeadline - f.now() - 1);
      for (const operation of ["steer", "followUp"] as const) await admit(operation);
      f.advance(1);
    }
    expect(f.now()).toBeGreaterThanOrEqual(originalDeadline);
    expect(f.now()).toBeLessThan(observedAt + 1_500); // Never wait for the old extended deadline.
    expect(f.local.holds(rootId)).toBe(false);
    expect(f.directory.get(rootId, f.now())).toMatchObject({ remoteHost: "ryzen4", bridgeStale: true });
    expect(f.directory.list({ scope: "project", fresh: true }, f.now()).find(p => p.id === rootId))
      .toMatchObject({ remoteHost: "ryzen4", bridgeStale: true });
    const before = commands(f).length;
    for (const operation of ["steer", "followUp"] as const) {
      const refusal = settle(f.control.request(rootId, rootId, operation, {
        ownerIncarnation: "activation-1", message: "expired original source deadline" }, rootId, { routedRemoteHost: "ryzen4" }));
      let settled = false;
      void refusal.then(() => { settled = true; });
      await vi.waitFor(() => expect(settled || commands(f).length > before).toBe(true));
      expect(commands(f)).toHaveLength(before);
      expect((await refusal).error).toMatchObject({ code: "FABRIC_BRIDGE_STALE", remoteHost: "ryzen4", targetId: rootId,
        message: expect.stringContaining("this attempt was not published") });
      await expect(router.routeMessage(rootId, "expired original source deadline", undefined, operation))
        .rejects.toMatchObject({ code: "FABRIC_BRIDGE_STALE", remoteHost: "ryzen4",
          message: expect.stringContaining("this attempt was not published") });
    }
    expect(commands(f)).toHaveLength(before);
    expect(steerRemote).not.toHaveBeenCalled();

    const sourceAt = f.now() - 60_000;
    await f.local.mirror({ ...f.presence, hosts: [{ ...f.presence.hosts[0]!, observedAt: sourceAt,
      record: { ...f.presence.hosts[0]!.record, updatedAt: sourceAt, expiresAt: sourceAt + 500 },
      expiresAt: sourceAt + 500 }] }, f.now());
    expect(f.lease().expiresAt).toBe(f.now() + 500);
    expect(f.local.holds(rootId)).toBe(true);
    expect(f.directory.get(rootId, f.now())).toMatchObject({ remoteHost: "ryzen4", stale: false });
    expect(f.directory.get(rootId, f.now())?.bridgeStale).toBeUndefined();
    for (const operation of ["steer", "followUp"] as const) await admit(operation);
  });

  // LUNA-DELTA-1: a legacy outbox effect runs INSIDE cold prepare, before the new
  // correction/removal commit. Final-state-only assertions miss this authority window.
  it.each(["missing", "same-sample inflated"] as const)("cold legacy outbox replay cannot publish expired authority at the prepare checkpoint (%s lease)", async disk => {
    const f = await fixture(-60_000, 500, "cold");
    vi.spyOn(Date, "now").mockImplementation(f.now);
    const t0 = f.now();
    const bridgeIdentity: MeshIdentity = { id: "bridge:ryzen4", name: "ryzen4", kind: "main" };
    const dead = new CommitOutbox(f.store, "bridge/ryzen4", bridgeIdentity, {});
    const host = f.presence.hosts[0]!.record;
    const inflated = { id: rootId, rootId, identityId: rootId, startedAt: host.startedAt,
      ...(host.incarnationToken ? { incarnationToken: host.incarnationToken } : {}), updatedAt: t0 + 1_000, expiresAt: t0 + 1_500,
      mirrorLease: 1 as const, mirror: mirror(t0, { sourceUpdatedAt: host.updatedAt,
        sourceExpiresAt: t0 - 60_000 + 500 }) };
    // Reproduce a pre-fix COMMIT whose effects never ran, including retained attribution.
    await f.store.writeBatch({ identity: bridgeIdentity, ops: [], prepare: () => dead.plan().stage([
      { kind: "put", key: key("topology/hosts/", rootId), identity,
        value: { ...host, remoteHost: "ryzen4", mirrorLease: 1, updatedAt: inflated.updatedAt, expiresAt: inflated.expiresAt } },
      { kind: "put", key: key("topology/participants/", rootId), identity,
        value: { ...f.presence.participants[0]!, remoteHost: "ryzen4" } },
    ], [{ kind: "lease", key: `lease:${rootId}`, payload: inflated }]) });
    expect(f.store.listAll(dead.prefix, { fresh: true })).toHaveLength(1);
    expect(readHostLease(f.store.root, rootId)).toBeUndefined();
    if (disk === "same-sample inflated") writeHostLease(f.store.root, inflated);
    f.advance(1_000);
    const sender = { id: senderId, name: "lead", kind: "main" as const };
    const steerRemote = vi.fn(async () => ({ queued: true }));
    const router = new AgentMessageRouter({} as any, { identity: sender, steerRemote } as any,
      { local: true, id: senderId, matches: (id: string) => id === senderId } as any,
      f.directory, f.control, binding => binding);
    // Record attempted publication, never wait on a nested write lock or mask admission
    // with the subsequent corrected state. The sentinel terminates an admitted old-head call.
    const admitted: string[] = [];
    vi.spyOn(f.store, "publish").mockImplementation(async input => {
      admitted.push(`${input.topic}:${input.kind}`);
      throw new Error("recorded prepare-checkpoint publication attempt");
    });
    const refusals: Array<ReturnType<typeof settle>> = [];
    let checkpoint: { expiresAt: number; observedAt: number | undefined; rowExpiry: unknown;
      participant: unknown; routing: string; commands: number } | undefined;
    const write = f.store.writeBatch.bind(f.store);
    vi.spyOn(f.store, "writeBatch").mockImplementation(input => write({ ...input, prepare: view => {
      const ops = input.prepare!(view); // Original prepare replays the pending lease here.
      if (checkpoint) return ops; // prepare may rerun; capture the first precommit replay only.
      checkpoint = { expiresAt: f.lease().expiresAt, observedAt: f.lease().mirror?.observedAt,
        rowExpiry: (view.get(key("topology/hosts/", rootId))!.value as { expiresAt: number }).expiresAt,
        participant: f.directory.get(rootId, f.now()),
        routing: mirroredOwnerRoutingStatus({ ...f.lease(), remoteHost: "ryzen4" }, f.now()),
        commands: commands(f).length };
      for (const operation of ["steer", "followUp"] as const) {
        refusals.push(settle(f.control.request(rootId, rootId, operation, {
          ownerIncarnation: "activation-1", message: "legacy replay checkpoint" }, rootId, { routedRemoteHost: "ryzen4" })));
        refusals.push(settle(router.routeMessage(rootId, "legacy replay checkpoint", undefined, operation)));
      }
      return ops;
    } }));
    await new StoreBridgeSide(f.store, "ryzen4", f.now).mirror(f.presence, t0);
    const recorded = await Promise.all(refusals);
    expect(checkpoint).toMatchObject({ expiresAt: t0 + 500, observedAt: t0, rowExpiry: t0 + 1_500,
      participant: { id: rootId, remoteHost: "ryzen4", bridgeStale: true }, routing: "bridge-stale", commands: 0 });
    expect(recorded).toHaveLength(4);
    for (const result of recorded) expect(result.error).toMatchObject({ code: "FABRIC_BRIDGE_STALE", remoteHost: "ryzen4",
      message: expect.stringContaining("this attempt was not published") });
    expect(admitted).toEqual([]);
    expect(steerRemote).not.toHaveBeenCalled();
    expect(commands(f)).toHaveLength(0);
  });

  it("cold legacy replay is live only to its original deadline and preserves a newer same-incarnation sample", async () => {
    for (const newer of [false, true]) {
      const f = await fixture(-60_000, 500, "cold");
      vi.spyOn(Date, "now").mockImplementation(f.now);
      const t0 = f.now();
      const bridgeIdentity: MeshIdentity = { id: "bridge:ryzen4", name: "ryzen4", kind: "main" };
      const dead = new CommitOutbox(f.store, "bridge/ryzen4", bridgeIdentity, {});
      const host = f.presence.hosts[0]!.record;
      const inflated = { id: rootId, rootId, identityId: rootId, startedAt: host.startedAt,
        ...(host.incarnationToken ? { incarnationToken: host.incarnationToken } : {}), updatedAt: t0 + 1_000, expiresAt: t0 + 1_500,
        mirrorLease: 1 as const, mirror: mirror(t0, { sourceUpdatedAt: host.updatedAt,
          sourceExpiresAt: t0 - 60_000 + 500 }) };
      await f.store.writeBatch({ identity: bridgeIdentity, ops: [], prepare: () => dead.plan().stage([
        { kind: "put", key: key("topology/hosts/", rootId), identity,
          value: { ...host, remoteHost: "ryzen4", mirrorLease: 1, expiresAt: inflated.expiresAt } },
        { kind: "put", key: key("topology/participants/", rootId), identity,
          value: { ...f.presence.participants[0]!, remoteHost: "ryzen4" } },
      ], [{ kind: "lease", key: `lease:${rootId}`, payload: inflated }]) });
      if (newer) writeHostLease(f.store.root, { ...inflated, updatedAt: t0 + 100, expiresAt: t0 + 600,
        mirror: mirror(t0 + 100, { sourceObservedAt: t0 - 60_000 + 100, sourceUpdatedAt: host.updatedAt + 100,
          sourceExpiresAt: t0 - 60_000 + 600 }) });
      f.advance(250);
      const snapshots: Array<{ expiresAt: number; observedAt: number | undefined; at: number; routing: string; participant: unknown }> = [];
      const write = f.store.writeBatch.bind(f.store);
      vi.spyOn(f.store, "writeBatch").mockImplementation(input => write({ ...input, prepare: view => {
        const ops = input.prepare!(view);
        const capture = () => snapshots.push({ expiresAt: f.lease().expiresAt, observedAt: f.lease().mirror?.observedAt,
          at: f.now(), routing: mirroredOwnerRoutingStatus({ ...f.lease(), remoteHost: "ryzen4" }, f.now()),
          participant: f.directory.get(rootId, f.now()) });
        capture();
        f.advance(250); // Equality at the OLD deadline; a genuinely newer sample stays live.
        capture();
        if (newer) { f.advance(100); capture(); }
        return ops;
      } }));
      await new StoreBridgeSide(f.store, "ryzen4", f.now).mirror(f.presence, t0);
      expect(snapshots[0]).toMatchObject({ expiresAt: t0 + (newer ? 600 : 500), observedAt: t0 + (newer ? 100 : 0),
        at: t0 + 250, routing: "live", participant: { remoteHost: "ryzen4" } });
      expect((snapshots[0]!.participant as { bridgeStale?: true }).bridgeStale).toBeUndefined();
      expect(snapshots[1]).toMatchObject({ at: t0 + 500, routing: newer ? "live" : "bridge-stale" });
      expect(snapshots.at(-1)).toMatchObject({ at: t0 + (newer ? 600 : 500), routing: "bridge-stale",
        participant: { remoteHost: "ryzen4", bridgeStale: true } });
      vi.restoreAllMocks();
    }
  });

  it.each(["v1", "legacy"] as const)("the %s router refuses short-TTL authority before control or legacy delivery and names a freshly proven lapse", async protocol => {
    const f = await fixture(-60_000, 1_000);
    vi.spyOn(Date, "now").mockImplementation(f.now);
    await f.local.mirror({ ...f.presence, participants: f.presence.participants.map(({ ownerIncarnation, ...p }) => ({ ...p,
      controlProtocol: protocol, ...(protocol === "v1" && ownerIncarnation ? { ownerIncarnation } : {}) })) }, f.now());
    const sender = { id: senderId, name: "lead", kind: "main" as const };
    const steerRemote = vi.fn(async () => ({ queued: true }));
    const router = new AgentMessageRouter({} as any, { identity: sender, steerRemote } as any,
      { local: true, id: senderId, matches: (id: string) => id === senderId } as any,
      f.directory, f.control, binding => binding);
    f.advance(1_001);
    for (const operation of ["steer", "followUp"] as const) {
      await expect(router.routeMessage(rootId, "expired translated authority", undefined, operation))
        .rejects.toMatchObject({ code: "FABRIC_BRIDGE_STALE", remoteHost: "ryzen4",
          message: expect.stringContaining("this attempt was not published") });
    }
    expect(commands(f)).toHaveLength(0);
    expect(steerRemote).not.toHaveBeenCalled();
    // A new expired SOURCE sample is different: it proves lapse and keeps immediate host attribution (#2004).
    f.advertise(mirror(f.now(), { sourceExpiresAt: f.now() - 60_001 }));
    expect(f.directory.get(rootId, f.now())).toBeUndefined();
    for (const operation of ["steer", "followUp"] as const) {
      await expect(router.routeMessage(rootId, "freshly proven source lapse", undefined, operation))
        .rejects.toThrow("its lease mirrored from remote host ryzen4 lapsed");
    }
    expect(commands(f)).toHaveLength(0);
    expect(steerRemote).not.toHaveBeenCalled();
  });

  it("retains a root behind an old bridge, but refuses publication with bridge-stale, never lease lapsed", async () => {
    const f = await fixture();
    f.advance(MIRROR_FRESH_MS + 1);
    expect(f.directory.get(rootId, f.now())).toMatchObject({ id: rootId, stale: false });
    const result = await settle(request(f));
    expect(result.error).toBeInstanceOf(FabricBridgeStaleError);
    expect(result.error).toHaveProperty("code", "FABRIC_BRIDGE_STALE");
    expect(result.error?.message).not.toContain("lapsed");
    expect(result.error?.message).toContain("not published");
    expect(commands(f)).toHaveLength(0);
    expect(f.local.holds(rootId)).toBe(false); // Retained routing attribution is NOT bridge authority.
  });

  it.each(["bridge-stale", "lease-stale"] as const)("pending command distinguishes %s, cancels once and preserves unknown outcome", async status => {
    const f = await fixture();
    const outcome = settle(request(f));
    await vi.waitFor(() => expect(commands(f)).toHaveLength(1));
    if (status === "bridge-stale") f.advance(MIRROR_FRESH_MS + 1);
    else f.advertise(mirror(f.now(), { sourceExpiresAt: f.now() - 60_001 }));
    const result = await outcome;
    expect(result.error?.message).toContain("outcome is unknown");
    expect(result.error).not.toHaveProperty("notRun");
    if (status === "bridge-stale") {
      expect(result.error).toBeInstanceOf(FabricBridgeStaleError);
      expect(result.error?.message).not.toContain("lapsed");
    } else {
      expect(result.error?.message).toContain("remote host ryzen4 lapsed");
      expect(f.directory.get(rootId, f.now())).toBeUndefined();
    }
    await vi.waitFor(() => expect(commands(f).filter(event => event.kind === "cancel")).toHaveLength(1));
  });

  it("a late mirror commit does not renew freshness; a new sample restores it", async () => {
    const f = await fixture();
    const observedAt = f.now();
    f.advance(60_000);
    await f.local.mirror(f.presence, observedAt);
    expect(mirroredLeaseStatus("ryzen4", f.lease().mirror, f.now())).toBe("bridge-stale");
    const sourceAt = f.now() - 60_000;
    await f.local.mirror({ ...f.presence, hosts: [{ ...f.presence.hosts[0]!, observedAt: sourceAt,
      record: { ...f.presence.hosts[0]!.record, updatedAt: sourceAt - 2_000 }, expiresAt: sourceAt + 13_000 }] }, f.now());
    expect(mirroredLeaseStatus("ryzen4", f.lease().mirror, f.now())).toBe("live");
  });

  it("never uses another host's freshness or another incarnation's observation", async () => {
    const f = await fixture();
    f.advertise(mirror(f.now(), { remoteHost: "ryzen3" }));
    expect((await settle(request(f))).error).toHaveProperty("code", "FABRIC_BRIDGE_STALE");
    writeHostLease(f.store.root, { ...f.lease(), incarnationToken: "other-incarnation", mirror: mirror(f.now()) });
    const host = f.store.get(key("topology/hosts/", rootId))!.value as typeof f.presence.hosts[0]["record"];
    expect(hostMirrorLease(new Map([[rootId, f.lease()]]), host)).toBeUndefined();
  });

  it("a withdrawn mirror's removal cannot erase a same-start-time replacement incarnation", async () => {
    const f = await fixture();
    const replacement = { ...f.lease(), incarnationToken: "replacement-incarnation", updatedAt: f.now() + 1 };
    const writeBatch = f.store.writeBatch.bind(f.store);
    vi.spyOn(f.store, "writeBatch").mockImplementation(input => writeBatch({ ...input, afterCommit: view => {
      writeHostLease(f.store.root, replacement); // Successor claims its file before publishing its state row.
      input.afterCommit?.(view);
    } }));
    await f.local.mirror({ hosts: [], participants: [] }, f.now());
    expect(f.lease().incarnationToken).toBe("replacement-incarnation");
  });

  it.each(["missing", "malformed"] as const)("a %s required observation never falls back to a fresh state expiry", async kind => {
    const f = await fixture();
    if (kind === "missing") fs.renameSync(hostLeasePath(f.store.root, rootId), path.join(f.store.root, "retained-lease.json"));
    else fs.writeFileSync(hostLeasePath(f.store.root, rootId), JSON.stringify({ format: 1, ...f.lease(),
      mirror: { ...f.lease().mirror, sourceObservedAt: null } }));
    expect(f.local.holds(rootId)).toBe(false);
    expect((await settle(request(f))).error).toHaveProperty("code", "FABRIC_BRIDGE_STALE");
    expect(commands(f)).toHaveLength(0);
    // Read-only selection probe: unconfirmed authority does not mean an epoch-old owner.
    // The reaper ages the row by its store commit stamp (wall clock at commit), not the fixture
    // clock, so the boundary is pinned to that stamp; a slow commit (hosted Windows) cannot skew it.
    const checkpoint = f.store.get(key("topology/hosts/", rootId))!.updatedAt;
    expect(deadHostRecords(f.store, { ownHostId: senderId, now: checkpoint + DEAD_HOST_RECORDS_MS - 2 })).toHaveLength(0);
    expect(deadHostRecords(f.store, { ownHostId: senderId, now: checkpoint + DEAD_HOST_RECORDS_MS }).length).toBeGreaterThan(0);
  });

  it("does not mirror a source-clock expired root even if hub time would consider it live", async () => {
    const f = await fixture(60_000);
    await f.local.mirror({ ...f.presence, hosts: [{ ...f.presence.hosts[0]!, expiresAt: f.now() + 59_999 }] }, f.now());
    expect(f.store.get(key("topology/hosts/", rootId))).toBeUndefined();
    expect(f.directory.get(rootId, f.now())).toBeUndefined();
  });

  it("a stale bridge wins over an old remote lease; freshness requires finite matching observations", () => {
    const at = Date.now();
    expect(mirroredLeaseStatus("ryzen4", mirror(at, { sourceExpiresAt: at - 60_001 }), at)).toBe("lease-stale");
    expect(mirroredLeaseStatus("ryzen4", mirror(at - MIRROR_FRESH_MS, { sourceExpiresAt: at - 90_000 }), at)).toBe("bridge-stale");
    expect(mirroredLeaseStatus("ryzen4", mirror(at, { observedAt: NaN }), at)).toBe("bridge-stale");
    expect(mirroredLeaseStatus("ryzen4", mirror(at + 1), at)).toBe("bridge-stale");
  });
});

const transport = (timeout = 30) => {
  const input = new PassThrough();
  const output = new PassThrough();
  const remote = new RemoteBridgeSide(input, output, timeout);
  const sent: Array<{ id: number; op: string }> = [];
  output.on("data", buffer => { sent.push(JSON.parse(buffer.toString())); });
  const reply = (id: number, result: unknown = undefined, error?: string) => input.write(JSON.stringify({ id,
    ok: error === undefined, ...(error === undefined ? { result } : { error }) }) + "\n");
  close.push(() => remote.close());
  return { remote, sent, reply };
};

describe("a slow mirror degrades, not disconnects (#10068)", () => {
  it("bounds the missed answer, gates all queued calls, and accepts a late answer before retrying a fresh mirror", async () => {
    const f = transport();
    const result = settle(f.remote.mirror({ hosts: [], participants: [] }));
    expect((await result).error).toBeInstanceOf(BridgeMirrorDeferredError);
    expect(f.sent).toHaveLength(1);
    const next = await settle(f.remote.presence());
    expect(next.error).toHaveProperty("code", "FABRIC_BRIDGE_STALE");
    expect(f.sent).toHaveLength(1); // No unbounded RPC queue behind the remote serial dispatch.
    f.reply(f.sent[0]!.id);
    const renewed = f.remote.mirror({ hosts: [], participants: [] });
    expect(f.sent).toHaveLength(2);
    f.reply(f.sent[1]!.id);
    await expect(renewed).resolves.toBeUndefined();
    const presence = f.remote.presence();
    f.reply(f.sent[2]!.id, { hosts: [], participants: [], reserved: [] });
    await expect(presence).resolves.toMatchObject({ hosts: [] });
  });

  it("a late permanent mirror rejection still closes the transport", async () => {
    const f = transport();
    await settle(f.remote.mirror({ hosts: [], participants: [] }));
    f.reply(f.sent[0]!.id, undefined, "Bridge presence is invalid");
    await expect(f.remote.closed).resolves.toHaveProperty("message", "Bridge agent: Bridge presence is invalid");
  });

  it("a serially queued mirror keeps the receiver's arrival time, not its late dispatch time", async () => {
    const f = await fixture();
    vi.spyOn(Date, "now").mockImplementation(f.now);
    const input = new PassThrough(), output = new PassThrough();
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    const sequence = vi.spyOn(f.local, "latestSequence").mockImplementation(async () => { await gate; return 0; });
    const replies: number[] = [];
    output.on("data", data => { replies.push(JSON.parse(data.toString()).id); });
    const serving = serveBridgeAgent(f.local, input, output);
    input.write(JSON.stringify({ id: 1, op: "latestSequence" }) + "\n");
    input.write(JSON.stringify({ id: 2, op: "mirror", args: f.presence }) + "\n");
    f.advance(60_000);
    release();
    await vi.waitFor(() => expect(replies).toContain(2));
    expect(mirroredLeaseStatus("ryzen4", f.lease().mirror, f.now())).toBe("bridge-stale");
    sequence.mockRestore();
    input.end();
    await serving;
  });

  it("non-mirror silence still closes the transport; an explicit close settles an in-flight mirror", async () => {
    const f = transport();
    const result = await settle(f.remote.presence());
    expect(result.error?.message).toContain("did not answer presence");
    await expect(f.remote.closed).resolves.toBe(result.error);
    const g = transport(100);
    const pending = settle(g.remote.mirror({ hosts: [], participants: [] }));
    g.remote.close(new Error("owned stop"));
    expect((await pending).error?.message).toBe("owned stop");
  });

  it.each([-120_000, 120_000])("resamples a silent long-lease origin across source clock skew (%i ms)", async offset => {
    const f = await fixture();
    vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"] });
    const start = Date.now();
    const side = new StoreBridgeSide(f.store, "ryzen4");
    let samples = 0;
    const remote = {
      protocolVersion: 2, latestSequence: async () => 0, read: async () => ({ events: [], through: 0 }),
      presence: async () => { samples++; return { ...f.presence, hosts: [{
        record: { ...f.presence.hosts[0]!.record, updatedAt: start + offset, expiresAt: start + offset + 90_000 },
        expiresAt: start + offset + 90_000, observedAt: Date.now() + offset }] }; },
      bridgedIds: async () => ({ ids: [], through: 0, done: true }), publish: async () => ({ sequence: 1 }),
      mirror: async () => undefined, subscribeChanges: () => () => undefined,
    };
    const local = { ...remote, presence: () => side.presence(), mirror: (p: BridgePresence, at?: number) => side.mirror(p, at),
      owned: () => side.owned(), withdraw: () => side.withdraw() };
    const bridge = new MeshBridge({ localName: "ryzen1", remoteName: "ryzen4", local, remote,
      cursorPath: path.join(f.store.root, "skew-cursor.json") });
    const running = bridge.run();
    void running.catch(() => undefined);
    await vi.advanceTimersByTimeAsync(0);
    await vi.waitFor(() => expect(samples).toBeGreaterThan(0));
    await vi.advanceTimersByTimeAsync(20_000);
    expect(samples).toBeGreaterThan(1);
    expect(side.holds(rootId)).toBe(true);
    await bridge.stop();
    await running;
  });

  it("keeps the real stdio bridge alive through a missed mirror answer, marks the host stale, then reobserves it", async () => {
    const f = await fixture();
    vi.spyOn(Date, "now").mockImplementation(f.now);
    const far = new MeshStore(path.join(f.store.root, "far"), 64 * 1024, 100);
    const agent = new StoreBridgeSide(far, "ryzen1", f.now);
    vi.spyOn(agent, "presence").mockImplementation(async () => {
      const at = f.now() - 60_000;
      return { ...f.presence, hosts: [{ ...f.presence.hosts[0]!, observedAt: at, expiresAt: at + 13_000,
        record: { ...f.presence.hosts[0]!.record, updatedAt: at - 2_000, expiresAt: at + 13_000 } }] };
    });
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    let mirrors = 0;
    const apply = agent.mirror.bind(agent);
    vi.spyOn(agent, "mirror").mockImplementation(async (presence, at) => {
      if (++mirrors === 1) { f.advance(30_001); await gate; }
      return apply(presence, at);
    });
    const input = new PassThrough(), output = new PassThrough();
    const serving = serveBridgeAgent(agent, input, output);
    const remote = new RemoteBridgeSide(output, input, 40);
    const logs: string[] = [];
    const bridge = new MeshBridge({ localName: "ryzen1", remoteName: "ryzen4", local: f.local, remote,
      cursorPath: path.join(f.store.root, "stdio-cursor.json"), walCapRetryMinMs: 10, stopMs: 30, log: message => logs.push(message) });
    let failure: unknown;
    let ended = false;
    const running = bridge.run().catch(error => { failure = error; }).finally(() => { ended = true; });
    close.push(async () => { release(); await bridge.stop(); remote.close(); input.end(); await serving; await running; });
    await vi.waitFor(() => expect(logs.some(message => message.includes("bridge mirror degraded"))).toBe(true));
    expect(ended).toBe(false);
    expect(failure).toBeUndefined();
    expect(f.directory.get(rootId, f.now())).toMatchObject({ bridgeStale: true });
    expect(f.local.holds(rootId)).toBe(false);
    release();
    await vi.waitFor(() => expect(mirrors > 1 && f.local.holds(rootId)).toBe(true));
    expect(ended).toBe(false);
    expect(f.directory.get(rootId, f.now())?.bridgeStale).toBeUndefined();
    await bridge.stop();
    await running;
    expect(failure).toBeUndefined();
  });

  it("keeps run alive with bounded backoff and recovers, while stop remains bounded for a silent mirror", async () => {
    const f = await fixture();
    const empty = { hosts: [], participants: [], reserved: [] };
    let mirrors = 0;
    const remote = {
      protocolVersion: 2, latestSequence: async () => 0, read: async () => ({ events: [], through: 0 }),
      presence: async () => f.presence, bridgedIds: async () => ({ ids: [], through: 0, done: true }),
      publish: async () => ({ sequence: 1 }), subscribeChanges: () => () => undefined,
      mirror: async () => { if (++mirrors === 1) throw new BridgeMirrorDeferredError(true, 30); },
    };
    const local = { ...remote, presence: async () => empty, mirror: (p: BridgePresence, at?: number) => f.local.mirror(p, at),
      owned: () => f.local.owned(), withdraw: () => f.local.withdraw() };
    const bridge = new MeshBridge({ localName: "ryzen1", remoteName: "ryzen4", local, remote,
      cursorPath: path.join(f.store.root, "cursor.json"), walCapRetryMinMs: 10, stopMs: 20 });
    let ended = false;
    const running = bridge.run().finally(() => { ended = true; });
    close.push(async () => { await bridge.stop(); await running; });
    await vi.waitFor(() => expect(mirrors).toBeGreaterThan(1));
    expect(ended).toBe(false);
    await bridge.stop();
    await running;
    expect(ended).toBe(true);
  });
});
