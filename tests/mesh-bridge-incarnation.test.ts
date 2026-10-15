import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { BRIDGE_LEASE_MS, StoreBridgeSide, type BridgePresence } from "../src/mesh/bridge.js";
import { CommitOutbox, type CommitOutboxPlan } from "../src/mesh/commit-outbox.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { hostLeasePath, readHostLease, writeHostLease } from "../src/topology/host-leases.js";
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
    const poisoned = { hosts: [{ record: { ...host, bridgeSupersededTokens: ["genuine-next-token"] },
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
