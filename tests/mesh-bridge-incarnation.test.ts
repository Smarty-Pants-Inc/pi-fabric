import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { BRIDGE_LEASE_MS, StoreBridgeSide } from "../src/mesh/bridge.js";
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
  incarnationToken: host.incarnationToken, updatedAt: NOW, expiresAt: host.expiresAt,
};
// Tokens identify owners, not order; the newer owner's token sorts before its predecessor's.
const newerLease = { ...oldLease, startedAt: host.startedAt + 1_000, incarnationToken: "opaque-a-successor" };

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
