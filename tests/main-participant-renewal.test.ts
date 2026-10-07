import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, expect, it, vi } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { LIVENESS_POLICY_KEY, readHostLease, removeHostLease } from "../src/topology/host-leases.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { readParticipantFile, removeParticipantFileIf } from "../src/topology/participant-files.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";

const roots: string[] = [], directories: ParticipantDirectory[] = [], planes: FabricControlPlane[] = [];
afterEach(async () => {
  await Promise.all(planes.splice(0).map(plane => plane.close()));
  await Promise.all(directories.splice(0).map(directory => directory.close()));
  vi.useRealTimers(); vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const key = (prefix: string, id: string) => prefix + createHash("sha256").update(id).digest("hex");
const setup = async (filesOnly: boolean, leaseMs = 15_000) => {
  vi.useFakeTimers();
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "main-participant-renewal-")); roots.push(root);
  const identity: MeshIdentity = { id: "session:live", name: "main", kind: "main", sessionId: "live" };
  const mesh = new MeshStore(root, 65_536, 100, { readCacheMs: 60_000 });
  if (filesOnly) await mesh.put({ key: LIVENESS_POLICY_KEY, identity,
    value: { version: 1, hostLeases: "files", participants: "files" } });
  const record: FabricParticipantRecord = {
    format: 1, id: identity.id, rootId: identity.id, ownerHostId: identity.id, ownerIdentityId: identity.id,
    kind: "root", name: "main", label: "TEST-1", status: "idle", runner: "pi", transport: "host",
    capabilities: ["steer", "followUp", "fabric"], cwd: root, sessionId: "live", startedAt: Date.now(),
    updatedAt: Date.now(), pendingMessages: false, controlProtocol: "v1",
  };
  let inFence = false;
  const fence = vi.fn();
  const publishFenced = async <T>(publish: () => Promise<T>): Promise<T> => {
    fence(); inFence = true;
    try { return await publish(); } finally { inFence = false; }
  };
  const directory = new ParticipantDirectory(mesh, { enabled: true, identity, rootId: identity.id, hostId: identity.id,
    heartbeatMs: 5_000, leaseMs, reapDeadHosts: false, withPublicationFence: publishFenced });
  directories.push(directory);
  directory.registerSource(() => [{ ...record, updatedAt: Date.now() }]);
  await directory.start();
  const observerIdentity: MeshIdentity = { id: "session:observer", name: "main", kind: "main", sessionId: "observer" };
  const observer = new ParticipantDirectory(new MeshStore(root, 65_536, 100, { readCacheMs: 60_000 }), {
    enabled: true, identity: observerIdentity, rootId: observerIdentity.id, hostId: observerIdentity.id, reapDeadHosts: false,
  }); directories.push(observer);
  const participantKey = key("topology/participants/", identity.id), hostKey = key("topology/hosts/", identity.id);
  const warning = vi.spyOn(console, "warn").mockImplementation(() => {});
  const writes = vi.spyOn(mesh, "writeBatch").mockImplementation(async options => {
    expect(inFence).toBe(true);
    return MeshStore.prototype.writeBatch.call(mesh, options);
  });
  return { root, mesh, identity, directory, observer, observerIdentity, participantKey, hostKey, warning, writes, fence };
};

it.each([false, true])("next renewal tick restores a deleted live Main and peers can steer it (files-only=%s)", async filesOnly => {
  const s = await setup(filesOnly);
  expect(s.observer.get(s.identity.id, Date.now(), { fresh: true })).toMatchObject({ stale: false });
  await removeParticipantFileIf(s.mesh, s.participantKey, () => true);
  const external = new MeshStore(s.root, 65_536, 100);
  for (const entryKey of [s.participantKey, s.hostKey, "sessions/live"]) await external.delete({ key: entryKey });
  removeHostLease(s.root, s.identity.id);
  expect(s.observer.get(s.identity.id, Date.now(), { fresh: true })).toBeUndefined();
  await vi.advanceTimersByTimeAsync(5_000); // real directory interval, not a manual publish
  const restored = s.observer.get(s.identity.id, Date.now(), { fresh: true })!;
  expect(restored).toMatchObject({ ownerHostId: s.identity.id, capabilities: expect.arrayContaining(["steer"]), stale: false });
  expect(readHostLease(s.root, s.identity.id)?.expiresAt).toBeGreaterThan(Date.now());
  expect(readParticipantFile(s.root, s.participantKey)).toBeDefined();
  expect(s.warning).toHaveBeenCalledExactlyOnceWith(expect.stringMatching(/Main participant re-published.*session:live.*missing/));
  expect(s.fence.mock.calls.length).toBeGreaterThan(1);
  const count = s.writes.mock.calls.length;
  await vi.advanceTimersByTimeAsync(5_000);
  expect(s.writes).toHaveBeenCalledTimes(count); expect(s.warning).toHaveBeenCalledTimes(1);

  vi.useRealTimers(); // actual control command -> owner handler -> acknowledged response
  const owner = new FabricControlPlane(s.mesh, s.identity, { enabled: true, hostId: s.identity.id, pollMs: 20 });
  const sender = new FabricControlPlane(s.observer.mesh, s.observerIdentity, { enabled: true, hostId: s.observerIdentity.id, pollMs: 20 });
  planes.push(owner, sender);
  const handled = vi.fn(() => ({ accepted: true, messageId: "restored-steer" }));
  owner.start(handled); sender.start(() => ({ accepted: false }));
  await expect(sender.request(restored.ownerHostId, restored.id, "steer", { message: "still reachable" }, restored.ownerIdentityId))
    .resolves.toMatchObject({ queued: true, acknowledged: true, messageId: "restored-steer" });
  expect(handled).toHaveBeenCalledOnce();
});

it.each([false, true])("a lapsed Main lease re-publishes its unchanged root once (files-only=%s)", async filesOnly => {
  const s = await setup(filesOnly);
  const before = readParticipantFile(s.root, s.participantKey)!;
  vi.setSystemTime(Date.now() + 20_000); // starvation/suspension: no intervening timer ticks
  expect(s.observer.get(s.identity.id, Date.now(), { fresh: true })).toBeUndefined();
  await vi.advanceTimersByTimeAsync(5_000);
  expect(readParticipantFile(s.root, s.participantKey)!.version).toBeGreaterThan(before.version);
  expect(s.observer.get(s.identity.id, Date.now(), { fresh: true })).toMatchObject({ stale: false });
  expect(s.warning).toHaveBeenCalledExactlyOnceWith(expect.stringMatching(/Main participant re-published.*session:live.*lapsed/));
  const count = s.writes.mock.calls.length;
  await vi.advanceTimersByTimeAsync(5_000);
  expect(s.writes).toHaveBeenCalledTimes(count); expect(s.warning).toHaveBeenCalledTimes(1);
});

it("the fixed Main session lease lapses even with a longer host lease", async () => {
  const s = await setup(true, 120_000);
  const before = readParticipantFile(s.root, s.participantKey)!;
  vi.setSystemTime(Date.now() + 20_000);
  await vi.advanceTimersByTimeAsync(5_000);
  expect(readParticipantFile(s.root, s.participantKey)!.version).toBeGreaterThan(before.version);
  expect(s.warning).toHaveBeenCalledExactlyOnceWith(expect.stringMatching(/Main participant re-published.*lapsed/));
});
it.each([false, true])("a reaped host lease while Main is live re-publishes presence and the root re-registers (files-only=%s)", async filesOnly => {
  const s = await setup(filesOnly);
  const before = readParticipantFile(s.root, s.participantKey)!;
  expect(s.observer.sessions().map(root => root.id)).toContain(s.identity.id);
  removeHostLease(s.root, s.identity.id); // lease lost (reaped) while the Main process stays live
  expect(readHostLease(s.root, s.identity.id)).toBeUndefined();
  await vi.advanceTimersByTimeAsync(5_000); // next real renewal tick, not a manual publish
  expect(readHostLease(s.root, s.identity.id)?.expiresAt).toBeGreaterThan(Date.now());
  expect(readParticipantFile(s.root, s.participantKey)!.version).toBeGreaterThan(before.version);
  expect(s.observer.get(s.identity.id, Date.now(), { fresh: true })).toMatchObject({ kind: "root", stale: false });
  const fresh = new ParticipantDirectory(new MeshStore(s.root, 65_536, 100), {
    enabled: true, identity: s.observerIdentity, rootId: s.observerIdentity.id, hostId: s.observerIdentity.id, reapDeadHosts: false,
  }); directories.push(fresh);
  expect(fresh.sessions().map(root => root.id)).toContain(s.identity.id); // re-registered as a live root
  expect(fresh.peers().map(peer => peer.id)).toContain(s.identity.id);
  expect(s.warning).toHaveBeenCalledExactlyOnceWith(expect.stringMatching(/Main participant re-published.*session:live.*lapsed/));
  const count = s.writes.mock.calls.length;
  await vi.advanceTimersByTimeAsync(5_000); // recovered: back to ordinary quiet renewal
  expect(s.writes).toHaveBeenCalledTimes(count); expect(s.warning).toHaveBeenCalledTimes(1);
});
