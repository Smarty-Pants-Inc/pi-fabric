import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { BRIDGE_LEASE_MS, BridgeStaleIncarnationError, StoreBridgeSide, type BridgePresence } from "../src/mesh/bridge.js";
import { CommitOutbox, type CommitOutboxPlan } from "../src/mesh/commit-outbox.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { hostLeasePath, hostLiveness, mirroredOwnerRoutingStatus, readHostLease, writeHostLease } from "../src/topology/host-leases.js";
import type { FabricHostRecord, FabricParticipantRecord } from "../src/topology/types.js";

// smarty-dev#7397: trust-boundary probes for lease-only authority changes and crash recovery.
// Record accepted removal admissions, never execute product lease deletion (#11199).
const removals = vi.hoisted(() => [] as string[]);
vi.mock("../src/topology/host-leases.js", async importOriginal => {
  const actual = await importOriginal<typeof import("../src/topology/host-leases.js")>();
  return { ...actual, removeHostLeaseIf: (root: string, id: string, owned: Parameters<typeof actual.removeHostLeaseIf>[2]) => {
    const lease = actual.readHostLease(root, id);
    if (!lease || !owned(lease)) return false;
    removals.push(`${root}:${id}`);
    return true;
  } };
});
const roots: string[] = [];
const stores: MeshStore[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const store of stores.splice(0)) store.closeState();
  // Hygiene is limited to this test's own mkdtemp fixtures, after SQLite is closed.
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  removals.length = 0;
});
const store = (stateBackend: "file" | "sqlite") => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-incarnation-boundary-"));
  roots.push(root);
  const hub = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100, { stateBackend });
  stores.push(hub);
  return hub;
};
const key = (prefix: string, id: string) => prefix + createHash("sha256").update(id).digest("hex");
const hostKey = (id: string) => key("topology/hosts/", id);
const participantKey = (id: string) => key("topology/participants/", id);
const bridgeIdentity: MeshIdentity = { id: "bridge:ryzen2", name: "ryzen2", kind: "main" };
// Fixed side clock and far-future expiry: no sleeps or wall-clock liveness races.
const NOW = Date.UTC(2100, 0, 1);
const identity: MeshIdentity = { id: "session:incarnation-00000000", name: "main", kind: "main", sessionId: "incarnation" };
const host: FabricHostRecord = {
  format: 1, id: identity.id, rootId: identity.id, identity,
  incarnationToken: "opaque-z-predecessor", startedAt: NOW - 2_000, updatedAt: NOW, expiresAt: NOW + 86_400_000,
};
const participant: FabricParticipantRecord = {
  format: 1, id: identity.id, kind: "root", rootId: identity.id, ownerHostId: identity.id, ownerIdentityId: identity.id,
  ownerIncarnation: "fixture:old-owner", name: "incarnation", label: "INCARNATION", status: "idle", runner: "pi",
  transport: "host", capabilities: ["steer", "followUp"], sessionId: "incarnation",
  startedAt: host.startedAt, updatedAt: NOW, controlProtocol: "v1",
};
const presence = { hosts: [{ record: host, expiresAt: host.expiresAt }], participants: [participant] };
const oldLease = {
  id: identity.id, rootId: identity.id, identityId: identity.id, startedAt: host.startedAt,
  incarnationToken: host.incarnationToken!, updatedAt: NOW, expiresAt: host.expiresAt,
};
// Tokens identify owners, not order; the newer owner's token sorts before its predecessor's.
const newerLease = { ...oldLease, startedAt: host.startedAt + 1_000, incarnationToken: "opaque-a-successor",
  bridgeSupersededTokens: [oldLease.incarnationToken] };

describe("bridge incarnation admission at persisted trust boundaries (smarty-dev#7397)", () => {
  // Keep reason: tokenless downgrade must not erase locally trusted incarnation evidence.
  describe.each(["file", "sqlite"] as const)("tokenless downgrade boundary (%s)", backend => {
    it.each(["mirror-and-lease", "mirror-only", "lease-only"] as const)("refuses downgrade without mutation and retains replay fencing with %s authority", async authority => {
      const hub = store(backend);
      const side = new StoreBridgeSide(hub, "ryzen2", () => NOW);
      if (authority === "mirror-and-lease") await side.mirror(presence);
      else {
        // Model either surviving authority without executing any product deletion.
        if (authority === "lease-only") writeHostLease(hub.root, oldLease);
        else await hub.put({ key: hostKey(identity.id), identity, value: { ...host, remoteHost: "ryzen2" } });
        await hub.put({ key: participantKey(identity.id), identity,
          value: { ...participant, remoteHost: "ryzen2" } });
        if (authority === "lease-only") expect(hub.get(hostKey(identity.id), { fresh: true })).toBeUndefined();
        else expect(readHostLease(hub.root, identity.id)).toBeUndefined();
      }
      const { incarnationToken: _token, ...tokenlessHost } = host;
      const downgrade = { hosts: [{ record: { ...tokenlessHost, startedAt: NOW + 120_000 },
        expiresAt: host.expiresAt }], participants: [{ ...participant, name: "tokenless-takeover" }] } satisfies
        Pick<BridgePresence, "hosts" | "participants">;
      const snapshot = () => ({
        rows: hub.listAll("", { fresh: true }),
        mirror: hub.get(hostKey(identity.id), { fresh: true }),
        participant: hub.get(participantKey(identity.id), { fresh: true }),
        lease: fs.existsSync(hostLeasePath(hub.root, identity.id))
          ? fs.readFileSync(hostLeasePath(hub.root, identity.id)) : undefined,
        stateBytes: backend === "file" ? fs.readFileSync(path.join(hub.root, "state.json")) : undefined,
      });
      const before = snapshot();
      await expect(side.mirror(downgrade)).rejects.toBeInstanceOf(BridgeStaleIncarnationError);
      expect(snapshot()).toEqual(before);
      // Refusal fences this connection even against the formerly admissible current A.
      await expect(side.mirror(presence)).rejects.toMatchObject({ code: "FABRIC_BRIDGE_STALE_INCARNATION" });
      expect(snapshot()).toEqual(before);
      expect(removals).toEqual([]);

      // Reopen the actual persistence backend: B must inherit A from local authority,
      // not merely from the old StoreBridgeSide's in-memory admission state.
      hub.closeState();
      const restarted = new MeshStore(hub.root, 64 * 1024, 100, { stateBackend: backend });
      stores.push(restarted);
      const replacement = { hosts: [{ record: { ...host, incarnationToken: newerLease.incarnationToken,
        startedAt: host.startedAt - 120_000 }, expiresAt: host.expiresAt }],
        participants: [{ ...participant, name: "genuine-successor-B" }] } satisfies
        Pick<BridgePresence, "hosts" | "participants">;
      await expect(new StoreBridgeSide(restarted, "ryzen2", () => NOW).mirror(replacement)).resolves.toBeUndefined();
      expect(restarted.get(hostKey(identity.id), { fresh: true })?.value).toMatchObject({
        incarnationToken: newerLease.incarnationToken, bridgeSupersededTokens: [host.incarnationToken],
      });
      expect(readHostLease(restarted.root, identity.id)).toMatchObject({
        incarnationToken: newerLease.incarnationToken, bridgeSupersededTokens: [host.incarnationToken],
      });
      expect(restarted.get(participantKey(identity.id), { fresh: true })?.value).toMatchObject({ name: "genuine-successor-B" });
      const afterB = restarted.listAll("", { fresh: true });
      const leaseB = fs.readFileSync(hostLeasePath(restarted.root, identity.id));
      const replayA = { hosts: [{ record: { ...host, startedAt: NOW + 240_000 }, expiresAt: host.expiresAt }],
        participants: [{ ...participant, name: "replayed-predecessor-A" }] };
      await expect(new StoreBridgeSide(restarted, "ryzen2", () => NOW).mirror(replayA))
        .rejects.toBeInstanceOf(BridgeStaleIncarnationError);
      expect(restarted.listAll("", { fresh: true })).toEqual(afterB);
      expect(fs.readFileSync(hostLeasePath(restarted.root, identity.id))).toEqual(leaseB);
      // The original refused side stays fenced after a separate connection acquires B.
      await expect(side.mirror(presence)).rejects.toMatchObject({ code: "FABRIC_BRIDGE_STALE_INCARNATION" });
      expect(restarted.listAll("", { fresh: true })).toEqual(afterB);
      expect(fs.readFileSync(hostLeasePath(restarted.root, identity.id))).toEqual(leaseB);
      expect(removals).toEqual([]);
    });

    it("admits and renews a tokenless-only legacy root across store restart", async () => {
      const hub = store(backend);
      const { incarnationToken: _token, ...legacyHost } = host;
      const legacy = { hosts: [{ record: legacyHost, expiresAt: host.expiresAt }], participants: [participant] } satisfies
        Pick<BridgePresence, "hosts" | "participants">;
      await expect(new StoreBridgeSide(hub, "ryzen2", () => NOW).mirror(legacy)).resolves.toBeUndefined();
      expect(hub.get(hostKey(identity.id), { fresh: true })?.value).toMatchObject({ remoteHost: "ryzen2" });
      expect(hub.get(hostKey(identity.id), { fresh: true })?.value).not.toHaveProperty("incarnationToken");
      expect(readHostLease(hub.root, identity.id)).not.toHaveProperty("incarnationToken");
      hub.closeState();
      const restarted = new MeshStore(hub.root, 64 * 1024, 100, { stateBackend: backend });
      stores.push(restarted);
      const renewed = { hosts: [{ record: { ...legacyHost, updatedAt: NOW + 1_000 }, expiresAt: host.expiresAt }],
        participants: [{ ...participant, name: "legacy-renewed" }] };
      await expect(new StoreBridgeSide(restarted, "ryzen2", () => NOW + 1_000).mirror(renewed)).resolves.toBeUndefined();
      expect(restarted.get(hostKey(identity.id), { fresh: true })?.value).not.toHaveProperty("incarnationToken");
      expect(readHostLease(restarted.root, identity.id)).toMatchObject({ updatedAt: NOW + 1_000 });
      expect(readHostLease(restarted.root, identity.id)).not.toHaveProperty("incarnationToken");
      expect(restarted.get(participantKey(identity.id), { fresh: true })?.value).toMatchObject({ name: "legacy-renewed" });
      expect(removals).toEqual([]);
    });

    it("allows a genuine same-token A renewal after reopening local authority", async () => {
      const hub = store(backend);
      await new StoreBridgeSide(hub, "ryzen2", () => NOW).mirror(presence);
      hub.closeState();
      const restarted = new MeshStore(hub.root, 64 * 1024, 100, { stateBackend: backend });
      stores.push(restarted);
      const renewed = { hosts: [{ record: { ...host, updatedAt: NOW + 1_000 }, expiresAt: host.expiresAt }],
        participants: [{ ...participant, name: "same-A-renewed" }] };
      const side = new StoreBridgeSide(restarted, "ryzen2", () => NOW + 1_000);
      await expect(side.mirror(renewed)).resolves.toBeUndefined();
      expect(restarted.get(hostKey(identity.id), { fresh: true })?.value).toMatchObject({
        incarnationToken: host.incarnationToken, bridgeSupersededTokens: [],
      });
      expect(readHostLease(restarted.root, identity.id)).toMatchObject({ incarnationToken: host.incarnationToken,
        bridgeSupersededTokens: [], updatedAt: NOW + 1_000, expiresAt: NOW + 1_000 + BRIDGE_LEASE_MS });
      expect(restarted.get(participantKey(identity.id), { fresh: true })?.value).toMatchObject({ name: "same-A-renewed" });
      expect(side.holds(identity.id, identity)).toBe(true);
      expect(removals).toEqual([]);
    });
  });

  it.each([false, true])("SQLite no-handoff early plan, intervening newer lease: %s", async leaseChanges => {
    const hub = store("sqlite");
    const side = new StoreBridgeSide(hub, "ryzen2", () => NOW);
    // A successful nonempty pass ends recovery; the next pass must use the early-plan path.
    await side.mirror(presence);
    expect(side.holds(identity.id, identity)).toBe(true);
    expect(hub.stateView()).toBeDefined();
    expect(hub.listAll("topology/handoffs/", { fresh: true })).toEqual([]);
    expect(hub.get(hostKey(identity.id), { fresh: true })?.value).toMatchObject({ ...host, remoteHost: "ryzen2", expiresAt: NOW + BRIDGE_LEASE_MS });
    expect(hub.get(participantKey(identity.id), { fresh: true })?.value).toMatchObject({ ...participant, remoteHost: "ryzen2" });
    const before = hub.listAll("", { fresh: true });
    const leaseBefore = fs.readFileSync(hostLeasePath(hub.root, identity.id));
    const views = vi.spyOn(hub, "stateView");
    const writeBatch = hub.writeBatch.bind(hub);
    let batches = 0;
    let installedBytes: Buffer | undefined;
    vi.spyOn(hub, "writeBatch").mockImplementation(input => {
      batches += 1;
      if (batches === 1) {
        // The early compute has finished, but the real transaction has not started.
        expect(views).toHaveBeenCalledOnce();
        if (leaseChanges) {
          writeHostLease(hub.root, newerLease);
          installedBytes = fs.readFileSync(hostLeasePath(hub.root, identity.id));
        }
      }
      return writeBatch(input);
    });

    if (leaseChanges) {
      await expect(side.mirror(presence)).rejects.toMatchObject({ code: "FABRIC_BRIDGE_STALE_INCARNATION" });
      expect(installedBytes).toBeDefined();
      expect(fs.readFileSync(hostLeasePath(hub.root, identity.id))).toEqual(installedBytes);
      expect(readHostLease(hub.root, identity.id)).toMatchObject(newerLease);
      expect(hub.listAll("", { fresh: true })).toEqual(before);
    } else {
      // Counterexample: equal incarnation renewal without an intervening lease is still allowed.
      await expect(side.mirror(presence)).resolves.toBeUndefined();
      expect(fs.readFileSync(hostLeasePath(hub.root, identity.id))).toEqual(leaseBefore);
      expect(side.holds(identity.id, identity)).toBe(true);
    }
    expect(batches).toBe(1);
    expect(hub.listAll("topology/handoffs/", { fresh: true })).toEqual([]);
    expect(removals).toEqual([]);
  });

  it("preserves a higher same-token lease fence across pending recovery replay and reconnect", async () => {
    const hub = store("file");
    const dead = new CommitOutbox(hub, "bridge/ryzen2", bridgeIdentity, {});
    // Crash image X90: the old mirror and pending lease share the on-disk X100 token.
    await hub.writeBatch({ identity: bridgeIdentity, ops: [], prepare: () => dead.plan().stage([
      { kind: "put", key: hostKey(identity.id), identity, value: { ...host, remoteHost: "ryzen2" } },
      { kind: "put", key: participantKey(identity.id), identity, value: { ...participant, remoteHost: "ryzen2" } },
    ], [{ kind: "lease", key: `lease:${identity.id}`, payload: oldLease }]) });
    const highLease = { ...oldLease, startedAt: host.startedAt + 1_000 };
    writeHostLease(hub.root, highLease);
    expect(hub.listAll(dead.prefix, { fresh: true })).toMatchObject([{ value: { kind: "lease", payload: oldLease } }]);
    const replay = vi.fn();
    const originalPlan = CommitOutbox.prototype.plan;
    vi.spyOn(CommitOutbox.prototype, "plan").mockImplementation(function (this: CommitOutbox): CommitOutboxPlan {
      const plan = originalPlan.call(this);
      return { ...plan, replay: view => { replay(); return plan.replay(view); } };
    });
    const fresh = new StoreBridgeSide(hub, "ryzen2", () => NOW);
    const equalUpdate = { ...presence, participants: [{ ...participant, name: "equal-token-recovered" }] } satisfies
      Pick<BridgePresence, "hosts" | "participants">;
    await expect(fresh.mirror(equalUpdate)).resolves.toBeUndefined();
    expect(replay).toHaveBeenCalledOnce();
    expect(hub.get(hostKey(identity.id), { fresh: true })?.value).toMatchObject({
      startedAt: highLease.startedAt, incarnationToken: host.incarnationToken, remoteHost: "ryzen2",
    });
    expect(readHostLease(hub.root, identity.id)).toMatchObject({ startedAt: highLease.startedAt,
      incarnationToken: host.incarnationToken });
    expect(hub.get(participantKey(identity.id), { fresh: true })?.value).toMatchObject({ name: "equal-token-recovered" });
    // Y95 is a new identity: X100's retained floor must not be used to order another token.
    const between = { hosts: [{ record: { ...host, startedAt: host.startedAt + 500,
      incarnationToken: "opaque-between-recovery" }, expiresAt: host.expiresAt }], participants: [participant] } satisfies
      Pick<BridgePresence, "hosts" | "participants">;
    await expect(new StoreBridgeSide(hub, "ryzen2", () => NOW).mirror(between)).resolves.toBeUndefined();
    expect(hub.get(hostKey(identity.id), { fresh: true })?.value).toMatchObject({
      startedAt: host.startedAt + 500, incarnationToken: "opaque-between-recovery",
      bridgeSupersededTokens: [host.incarnationToken],
    });
    expect(readHostLease(hub.root, identity.id)).toMatchObject({ startedAt: host.startedAt + 500,
      incarnationToken: "opaque-between-recovery", bridgeSupersededTokens: [host.incarnationToken] });
    await expect(new StoreBridgeSide(hub, "ryzen2", () => NOW).mirror(presence))
      .rejects.toMatchObject({ code: "FABRIC_BRIDGE_STALE_INCARNATION" });
    expect(removals).toEqual([]);
  });

  // Keep reason: Luna's #7397 P2, an older unsampled effect erased sampled authority.
  it.each(([
    { path: "replay", inflated: false }, { path: "replay", inflated: true },
    { path: "afterCommit", inflated: false }, { path: "afterCommit", inflated: true },
  ] as const).flatMap(test => [{ ...test, sameDeadline: false }, { ...test, sameDeadline: true }]))(
    "retains sampled M1 and repairs history at the unsampled $path checkpoint (inflated=$inflated, equal-deadline=$sameDeadline)", async ({ path, inflated, sameDeadline }) => {
    const hub = store("file");
    const deadline = NOW + 5_000;
    const sample = { remoteHost: "ryzen2", observedAt: NOW, sourceObservedAt: NOW - 60_000,
      sourceUpdatedAt: NOW - 61_000, sourceExpiresAt: NOW - 55_000 };
    const sampled = { ...oldLease, updatedAt: NOW, expiresAt: deadline + (inflated ? 1_000 : 0),
      mirrorLease: 1 as const, mirror: sample, bridgeSupersededTokens: ["disk-predecessor"],
      bridgeSupersededTokenSeen: { "disk-predecessor": 2 } };
    const pending = { ...oldLease, updatedAt: NOW - 1_000, expiresAt: sameDeadline ? sampled.expiresAt : NOW + 4_000,
      mirrorLease: 1 as const, bridgeSupersededTokens: ["effect-predecessor"],
      bridgeSupersededTokenSeen: { "effect-predecessor": 1 } };
    const incoming = { hosts: [{ record: { ...host, updatedAt: sample.sourceUpdatedAt },
      observedAt: sample.sourceObservedAt, expiresAt: sample.sourceExpiresAt }], participants: [participant] };
    const capture = () => {
      const lease = readHostLease(hub.root, identity.id)!;
      return { lease, routing: mirroredOwnerRoutingStatus({ ...lease, remoteHost: "ryzen2" }, NOW),
        liveness: hostLiveness(new Map([[identity.id, lease]]), { ...host, remoteHost: "ryzen2", mirrorLease: 1 }) };
    };
    let checkpoint: ReturnType<typeof capture> | undefined;
    if (path === "replay") {
      const dead = new CommitOutbox(hub, "bridge/ryzen2", bridgeIdentity, {});
      await hub.writeBatch({ identity: bridgeIdentity, ops: [], prepare: () => dead.plan().stage([
        { kind: "put", key: hostKey(identity.id), identity, value: { ...host, remoteHost: "ryzen2", mirrorLease: 1 } },
      ], [{ kind: "lease", key: `lease:${identity.id}`, payload: pending }]) });
      writeHostLease(hub.root, sampled);
      const originalPlan = CommitOutbox.prototype.plan;
      vi.spyOn(CommitOutbox.prototype, "plan").mockImplementation(function (this: CommitOutbox): CommitOutboxPlan {
        const plan = originalPlan.call(this);
        return { ...plan, replay: view => {
          const count = plan.replay(view);
          // Capture BEFORE recompute/current afterCommit could conceal an erased mirror.
          checkpoint = capture();
          expect(count).toBe(1);
          return count;
        } };
      });
      await new StoreBridgeSide(hub, "ryzen2", () => NOW).mirror(incoming, NOW);
    } else {
      const write = hub.writeBatch.bind(hub);
      let delayed: NonNullable<Parameters<MeshStore["writeBatch"]>[0]["afterCommit"]> | undefined;
      const intercept = vi.spyOn(hub, "writeBatch").mockImplementation(input => {
        const { afterCommit, ...withoutAfterCommit } = input;
        delayed = afterCommit;
        return write(withoutAfterCommit);
      });
      // Commit an unsampled effect, withholding only its real afterCommit callback.
      await new StoreBridgeSide(hub, "ryzen2", () => NOW - 1_000).mirror({ hosts: [{
        record: { ...host, updatedAt: pending.updatedAt }, expiresAt: pending.expiresAt }], participants: [participant] });
      intercept.mockRestore();
      expect(delayed).toBeTypeOf("function");
      // An intervening pass publishes sampled M1 before the older callback regains custody.
      await new StoreBridgeSide(hub, "ryzen2", () => NOW).mirror(incoming, NOW);
      await hub.put({ key: hostKey(identity.id), identity, value: { ...host, remoteHost: "ryzen2", mirrorLease: 1,
        bridgeSupersededTokens: ["effect-predecessor"], bridgeSupersededTokenSeen: { "effect-predecessor": 1 } } });
      writeHostLease(hub.root, sampled);
      await hub.writeBatch({ identity: bridgeIdentity, ops: [], afterCommit: view => {
        delayed!(view);
        checkpoint = capture(); // No later mirror pass can repair this checkpoint.
      } });
    }
    expect(checkpoint).toBeDefined();
    expect(checkpoint!.lease).toMatchObject({ mirror: sample, updatedAt: NOW, expiresAt: deadline,
      bridgeSupersededTokens: ["effect-predecessor", "disk-predecessor"],
      bridgeSupersededTokenSeen: { "effect-predecessor": 1, "disk-predecessor": 2 } });
    expect(checkpoint!.lease.expiresAt).toBeLessThanOrEqual(sample.observedAt +
      Math.min(BRIDGE_LEASE_MS, sample.sourceExpiresAt - sample.sourceObservedAt));
    expect(checkpoint!.routing).toBe("live");
    expect(checkpoint!.liveness).toEqual({ updatedAt: NOW, expiresAt: deadline });
    expect(removals).toEqual([]);
  });

  it("still allows a strictly later unsampled same-token deadline over sampled M1", async () => {
    const hub = store("file");
    const side = new StoreBridgeSide(hub, "ryzen2", () => NOW);
    await side.mirror({ hosts: [{ record: host, observedAt: NOW, expiresAt: NOW + 5_000 }], participants: [participant] }, NOW);
    expect(readHostLease(hub.root, identity.id)?.mirror).toBeDefined();
    await side.mirror(presence);
    expect(readHostLease(hub.root, identity.id)).toMatchObject({ expiresAt: NOW + BRIDGE_LEASE_MS });
    expect(readHostLease(hub.root, identity.id)?.mirror).toBeUndefined();
    expect(side.holds(identity.id, identity)).toBe(true);
    expect(removals).toEqual([]);
  });

  it("repairs same-token history during pending outbox replay before retiring the effect", async () => {
    const hub = store("file");
    const history = { bridgeSupersededTokens: ["retired-before-crash"],
      bridgeSupersededTokenSeen: { "retired-before-crash": 1 } };
    const dead = new CommitOutbox(hub, "bridge/ryzen2", bridgeIdentity, {});
    await hub.writeBatch({ identity: bridgeIdentity, ops: [], prepare: () => dead.plan().stage([
      { kind: "put", key: hostKey(identity.id), identity, value: { ...host, ...history, remoteHost: "ryzen2" } },
    ], [{ kind: "lease", key: `lease:${identity.id}`, payload: { ...oldLease, ...history } }]) });
    writeHostLease(hub.root, { ...oldLease, updatedAt: oldLease.updatedAt + 1_000,
      expiresAt: oldLease.expiresAt + 1_000 });
    expect(hub.listAll(dead.prefix, { fresh: true })).toHaveLength(1);
    await new StoreBridgeSide(hub, "ryzen2", () => NOW).mirror(presence);
    const repaired = readHostLease(hub.root, identity.id)!;
    expect(repaired).toMatchObject({ ...history, updatedAt: oldLease.updatedAt + 1_000,
      expiresAt: oldLease.expiresAt + 1_000 });
    const leaseOnly = store("file");
    writeHostLease(leaseOnly.root, repaired);
    const replay = { ...presence, hosts: [{ record: { ...host, incarnationToken: "retired-before-crash",
      startedAt: host.startedAt + 120_000 }, expiresAt: host.expiresAt }] };
    await expect(new StoreBridgeSide(leaseOnly, "ryzen2", () => NOW).mirror(replay))
      .rejects.toMatchObject({ code: "FABRIC_BRIDGE_STALE_INCARNATION" });
  });

  it.each(["file", "sqlite"] as const)("bounds and persists the last 16 superseded tokens across bridge/store restart (%s)", async backend => {
    const hub = store(backend);
    const frame = (index: number) => ({ hosts: [{ record: { ...host, incarnationToken: `generation-${index}`,
      startedAt: host.startedAt - index * 120_000 }, expiresAt: host.expiresAt }], participants: [participant] });
    for (let index = 0; index <= 20; index++) {
      await new StoreBridgeSide(hub, "ryzen2", () => NOW).mirror(frame(index));
    }
    const expected = Array.from({ length: 16 }, (_, index) => `generation-${index + 4}`);
    expect(hub.get(hostKey(identity.id), { fresh: true })?.value).toMatchObject({
      incarnationToken: "generation-20", bridgeSupersededTokens: expected,
    });
    expect(readHostLease(hub.root, identity.id)).toMatchObject({
      incarnationToken: "generation-20", bridgeSupersededTokens: expected,
    });
    hub.closeState();
    const restarted = new MeshStore(hub.root, 64 * 1024, 100, { stateBackend: backend });
    stores.push(restarted);
    // All retained tokens remain refused, despite claiming a much later start, after restart.
    for (let index = 4; index < 20; index++) {
      const replay = frame(index);
      replay.hosts[0]!.record.startedAt = NOW + 120_000;
      await expect(new StoreBridgeSide(restarted, "ryzen2", () => NOW).mirror(replay))
        .rejects.toMatchObject({ code: "FABRIC_BRIDGE_STALE_INCARNATION" });
    }
    expect(readHostLease(restarted.root, identity.id)?.bridgeSupersededTokens).toEqual(expected);
    // Persisted lease history alone is sufficient when the shared mirror was lost.
    const leaseOnly = store(backend);
    writeHostLease(leaseOnly.root, readHostLease(restarted.root, identity.id)!);
    await expect(new StoreBridgeSide(leaseOnly, "ryzen2", () => NOW).mirror(frame(19)))
      .rejects.toMatchObject({ code: "FABRIC_BRIDGE_STALE_INCARNATION" });
    expect(leaseOnly.get(hostKey(identity.id), { fresh: true })).toBeUndefined();
    // Once a token ages out, the bounded history proves nothing about it (the explicit N=16 trade-off).
    await expect(new StoreBridgeSide(restarted, "ryzen2", () => NOW).mirror(frame(0))).resolves.toBeUndefined();
    expect(readHostLease(restarted.root, identity.id)?.bridgeSupersededTokens).toEqual([...expected.slice(1), "generation-20"]);
  });

  it.each(["state", "lease"] as const)("keeps newest-16 history when %s authority is ahead during an overlapping renewal", async ahead => {
    const hub = store("file");
    const frame = (index: number) => ({ hosts: [{ record: { ...host, incarnationToken: `crash-generation-${index}`,
      startedAt: host.startedAt - index * 120_000 }, expiresAt: host.expiresAt }], participants: [participant] });
    const side = new StoreBridgeSide(hub, "ryzen2", () => NOW);
    for (let index = 0; index < 20; index++) await side.mirror(frame(index));
    const priorLease = readHostLease(hub.root, identity.id)!;
    const priorHost = hub.get(hostKey(identity.id), { fresh: true })!.value;
    await side.mirror(frame(20));
    // The same state/file mismatch can be observed by an already-recovered early plan,
    // not just recovery: an overlapping lease effect may lag or lead the shared row.
    if (ahead === "state") writeHostLease(hub.root, priorLease);
    else await hub.put({ key: hostKey(identity.id), identity, value: priorHost });
    await side.mirror(frame(20));
    const expected = Array.from({ length: 16 }, (_, index) => `crash-generation-${index + 4}`);
    expect(hub.get(hostKey(identity.id), { fresh: true })?.value).toMatchObject({ bridgeSupersededTokens: expected });
    expect(readHostLease(hub.root, identity.id)?.bridgeSupersededTokens).toEqual(expected);
    const replay = frame(4);
    replay.hosts[0]!.record.startedAt = NOW + 120_000;
    await expect(new StoreBridgeSide(hub, "ryzen2", () => NOW).mirror(replay))
      .rejects.toMatchObject({ code: "FABRIC_BRIDGE_STALE_INCARNATION" });
  });

  it.each([
    { recovery: false, longer: false }, { recovery: false, longer: true },
    { recovery: true, longer: false }, { recovery: true, longer: true },
  ])("repairs same-token history before lease-only replay (recovery=$recovery, longer=$longer)", async ({ recovery, longer }) => {
    const hub = store("file");
    const side = new StoreBridgeSide(hub, "ryzen2", () => NOW);
    await side.mirror(presence);
    const replacement = { hosts: [{ record: { ...host, incarnationToken: "same-token-repair-current",
      startedAt: host.startedAt - 120_000 }, expiresAt: host.expiresAt }], participants: [participant] };
    await side.mirror(replacement);
    const lease = readHostLease(hub.root, identity.id)!;
    expect(lease.bridgeSupersededTokens).toContain(oldLease.incarnationToken);
    // A partial/older file publication retains the current token/liveness but loses history.
    const { bridgeSupersededTokens: _tokens, bridgeSupersededTokenSeen: _seen, ...withoutHistory } = lease;
    writeHostLease(hub.root, { ...withoutHistory,
      updatedAt: lease.updatedAt + (longer ? 1_000 : 0), expiresAt: lease.expiresAt + (longer ? 1_000 : 0) });
    const receiver = recovery ? new StoreBridgeSide(hub, "ryzen2", () => NOW) : side;
    await expect(receiver.mirror(replacement)).resolves.toBeUndefined();
    const repaired = readHostLease(hub.root, identity.id)!;
    expect(repaired).toMatchObject({ incarnationToken: "same-token-repair-current",
      updatedAt: lease.updatedAt + (longer ? 1_000 : 0), expiresAt: lease.expiresAt + (longer ? 1_000 : 0) });
    expect(repaired.bridgeSupersededTokens).toContain(oldLease.incarnationToken);
    // Simulate state loss using an empty mesh with the exact repaired file authority;
    // no product deletion is executed. Lease-only admission must retain the refusal.
    const leaseOnly = store("file");
    writeHostLease(leaseOnly.root, repaired);
    const replay = { hosts: [{ record: { ...host, startedAt: NOW + 120_000 }, expiresAt: host.expiresAt }],
      participants: [participant] };
    await expect(new StoreBridgeSide(leaseOnly, "ryzen2", () => NOW).mirror(replay))
      .rejects.toMatchObject({ code: "FABRIC_BRIDGE_STALE_INCARNATION" });
    expect(leaseOnly.get(hostKey(identity.id), { fresh: true })).toBeUndefined();
  });

  it.each(["state", "lease"] as const)("keeps the newest 16 seen tokens when %s has the newer disjoint history", async newer => {
    const hub = store("file");
    const recent = Array.from({ length: 16 }, (_, index) => `recent-predecessor-${index}`);
    const ancient = Array.from({ length: 16 }, (_, index) => `ancient-predecessor-${index}`);
    const seen = (tokens: string[], start: number) => Object.fromEntries(tokens.map((token, index) => [token, start + index]));
    const currentHistory = newer === "state" ? recent : ancient;
    const fileHistory = newer === "state" ? ancient : recent;
    await hub.put({ key: hostKey(identity.id), identity, value: { ...host, remoteHost: "ryzen2",
      bridgeSupersededTokens: currentHistory, bridgeSupersededTokenSeen: seen(currentHistory, newer === "state" ? 101 : 1) } });
    writeHostLease(hub.root, { ...oldLease, bridgeSupersededTokens: fileHistory,
      bridgeSupersededTokenSeen: seen(fileHistory, newer === "state" ? 1 : 101) });
    await expect(new StoreBridgeSide(hub, "ryzen2", () => NOW).mirror(presence)).resolves.toBeUndefined();
    expect(hub.get(hostKey(identity.id), { fresh: true })?.value).toMatchObject({ bridgeSupersededTokens: recent,
      bridgeSupersededTokenSeen: seen(recent, 101) });
    expect(readHostLease(hub.root, identity.id)).toMatchObject({ bridgeSupersededTokens: recent,
      bridgeSupersededTokenSeen: seen(recent, 101) });
    const leaseOnly = store("file");
    writeHostLease(leaseOnly.root, readHostLease(hub.root, identity.id)!);
    await expect(new StoreBridgeSide(leaseOnly, "ryzen2", () => NOW).mirror({ hosts: [{ record: { ...host,
      incarnationToken: recent[0]!, startedAt: NOW + 120_000 }, expiresAt: host.expiresAt }], participants: [participant] }))
      .rejects.toMatchObject({ code: "FABRIC_BRIDGE_STALE_INCARNATION" });
  });

  it("prefers the current authority chain when disjoint legacy histories have no ordering evidence", async () => {
    const hub = store("file");
    const current = Array.from({ length: 16 }, (_, index) => `current-legacy-${index}`);
    const secondary = Array.from({ length: 16 }, (_, index) => `secondary-legacy-${index}`);
    await hub.put({ key: hostKey(identity.id), identity, value: { ...host, remoteHost: "ryzen2",
      bridgeSupersededTokens: current } });
    writeHostLease(hub.root, { ...oldLease, bridgeSupersededTokens: secondary });
    await new StoreBridgeSide(hub, "ryzen2", () => NOW).mirror(presence);
    expect(hub.get(hostKey(identity.id), { fresh: true })?.value).toMatchObject({ bridgeSupersededTokens: current });
    expect(readHostLease(hub.root, identity.id)?.bridgeSupersededTokens).toEqual(current);
  });

  it("never refreshes a retained token's first-seen ordinal when another authority repeats it", async () => {
    const hub = store("file");
    const retained = Array.from({ length: 15 }, (_, index) => `keep-first-seen-${index}`);
    const stateHistory = ["oldest-first-seen", ...retained];
    await hub.put({ key: hostKey(identity.id), identity, value: { ...host, remoteHost: "ryzen2",
      bridgeSupersededTokens: stateHistory,
      bridgeSupersededTokenSeen: Object.fromEntries(stateHistory.map((token, index) => [token, index + 1])) } });
    writeHostLease(hub.root, { ...oldLease, bridgeSupersededTokens: ["oldest-first-seen", "newest-first-seen"],
      bridgeSupersededTokenSeen: { "oldest-first-seen": 500, "newest-first-seen": 17 } });
    await new StoreBridgeSide(hub, "ryzen2", () => NOW).mirror(presence);
    const expected = [...retained, "newest-first-seen"];
    expect(hub.get(hostKey(identity.id), { fresh: true })?.value).toMatchObject({ bridgeSupersededTokens: expected });
    expect(readHostLease(hub.root, identity.id)?.bridgeSupersededTokens).toEqual(expected);
  });

  it("admits unknown tokens against historical token-bearing lease rows without history, regardless of clocks", async () => {
    const hub = store("file");
    const { bridgeSupersededTokens: _history, ...historical } = newerLease;
    writeHostLease(hub.root, historical);
    // An earlier start and a different token are not evidence that this is a replay.
    await expect(new StoreBridgeSide(hub, "ryzen2", () => NOW).mirror(presence)).resolves.toBeUndefined();
    expect(readHostLease(hub.root, identity.id)).toMatchObject({ incarnationToken: oldLease.incarnationToken,
      startedAt: oldLease.startedAt, bridgeSupersededTokens: [historical.incarnationToken] });
  });

  it("does not import peer-supplied superseded-token claims as local authority", async () => {
    const hub = store("file");
    const poisoned = { hosts: [{ record: { ...host, bridgeSupersededTokens: ["genuine-next-token"],
      bridgeSupersededTokenSeen: { "genuine-next-token": Number.MAX_SAFE_INTEGER } },
      expiresAt: host.expiresAt }], participants: [participant] };
    await new StoreBridgeSide(hub, "ryzen2", () => NOW).mirror(poisoned);
    const genuine = { hosts: [{ record: { ...host, incarnationToken: "genuine-next-token", startedAt: host.startedAt - 120_000 },
      expiresAt: host.expiresAt }], participants: [participant] };
    await expect(new StoreBridgeSide(hub, "ryzen2", () => NOW).mirror(genuine)).resolves.toBeUndefined();
    expect(readHostLease(hub.root, identity.id)).toMatchObject({ incarnationToken: "genuine-next-token",
      bridgeSupersededTokens: [host.incarnationToken] });
  });

  it("admits a new rollback token through recovery without replaying a superseded lease over disk authority", async () => {
    const hub = store("file");
    const dead = new CommitOutbox(hub, "bridge/ryzen2", bridgeIdentity, {});
    await hub.writeBatch({ identity: bridgeIdentity, ops: [], prepare: () => dead.plan().stage([
      { kind: "put", key: hostKey(identity.id), identity, value: { ...host, remoteHost: "ryzen2" } },
      { kind: "put", key: participantKey(identity.id), identity, value: { ...participant, remoteHost: "ryzen2" } },
    ], [{ kind: "lease", key: `lease:${identity.id}`, payload: oldLease }]) });
    writeHostLease(hub.root, newerLease);
    const before = fs.readFileSync(hostLeasePath(hub.root, identity.id));
    const originalPlan = CommitOutbox.prototype.plan;
    const replayed = vi.fn();
    vi.spyOn(CommitOutbox.prototype, "plan").mockImplementation(function (this: CommitOutbox): CommitOutboxPlan {
      const plan = originalPlan.call(this);
      return { ...plan, replay: view => {
        const count = plan.replay(view);
        expect(fs.readFileSync(hostLeasePath(hub.root, identity.id))).toEqual(before);
        replayed();
        return count;
      } };
    });
    const rollback = { hosts: [{ record: { ...host, incarnationToken: "new-rollback-through-recovery",
      startedAt: host.startedAt - 120_000 }, expiresAt: host.expiresAt }], participants: [participant] };
    await expect(new StoreBridgeSide(hub, "ryzen2", () => NOW).mirror(rollback)).resolves.toBeUndefined();
    expect(replayed).toHaveBeenCalledOnce();
    expect(readHostLease(hub.root, identity.id)).toMatchObject({ incarnationToken: "new-rollback-through-recovery",
      startedAt: host.startedAt - 120_000, bridgeSupersededTokens: expect.arrayContaining([
        oldLease.incarnationToken, newerLease.incarnationToken,
      ]) });
  });

  it("rejects a stale recovery snapshot before replaying a pending predecessor lease", async () => {
    const hub = store("file");
    const dead = new CommitOutbox(hub, "bridge/ryzen2", bridgeIdentity, {});
    // Crash image: committed old host + participant + lease outbox row, with no effect run.
    await hub.writeBatch({ identity: bridgeIdentity, ops: [], prepare: () => dead.plan().stage([
      { kind: "put", key: hostKey(identity.id), identity, value: { ...host, remoteHost: "ryzen2" } },
      { kind: "put", key: participantKey(identity.id), identity, value: { ...participant, remoteHost: "ryzen2" } },
    ], [{ kind: "lease", key: `lease:${identity.id}`, payload: oldLease }]) });
    expect(readHostLease(hub.root, identity.id)).toBeUndefined();
    writeHostLease(hub.root, newerLease);
    const pending = hub.listAll(dead.prefix, { fresh: true });
    expect(pending).toHaveLength(1);
    expect(pending[0]).toMatchObject({ key: dead.rowKey(`lease:${identity.id}`), value: {
      scope: "bridge/ryzen2", kind: "lease", key: `lease:${identity.id}`, payload: oldLease,
    } });
    const stateBefore = hub.listAll("", { fresh: true });
    const stateFile = path.join(hub.root, "state.json");
    const stateBytes = fs.readFileSync(stateFile);
    const leaseBytes = fs.readFileSync(hostLeasePath(hub.root, identity.id));
    // Observe the actual recovery boundary, delegating unchanged if replay is reached.
    const replay = vi.fn();
    const originalPlan = CommitOutbox.prototype.plan;
    vi.spyOn(CommitOutbox.prototype, "plan").mockImplementation(function (this: CommitOutbox): CommitOutboxPlan {
      const plan = originalPlan.call(this);
      return { ...plan, replay: view => { replay(); return plan.replay(view); } };
    });
    const fresh = new StoreBridgeSide(hub, "ryzen2", () => NOW);

    // Never send an empty mirror or withdraw: that would exercise the real-delete path.
    await expect(fresh.mirror(presence)).rejects.toMatchObject({ code: "FABRIC_BRIDGE_STALE_INCARNATION" });
    expect(replay).not.toHaveBeenCalled();
    expect(fs.readFileSync(stateFile)).toEqual(stateBytes);
    expect(fs.readFileSync(hostLeasePath(hub.root, identity.id))).toEqual(leaseBytes);
    expect(readHostLease(hub.root, identity.id)).toMatchObject(newerLease);
    expect(hub.listAll("", { fresh: true })).toEqual(stateBefore);
    expect(hub.listAll(dead.prefix, { fresh: true })).toEqual(pending);
    expect(removals).toEqual([]);
  });
});
