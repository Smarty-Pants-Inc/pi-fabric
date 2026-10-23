import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, expect, it, vi } from "vitest";
import { BridgeOwnershipError, StoreBridgeSide } from "../src/mesh/bridge.js";
import { CommitOutbox } from "../src/mesh/commit-outbox.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { hostLeasePath, readHostLease, writeHostLease } from "../src/topology/host-leases.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";
import type { FabricHostRecord, FabricParticipantRecord } from "../src/topology/types.js";

// Keep: real SA-04 data-safety failure, plus the guards an exact tombstone fix must preserve.
const roots: string[] = [], stores: MeshStore[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const store of stores.splice(0)) store.closeState();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const key = (kind: string, id: string) => `topology/${kind}/${createHash("sha256").update(id).digest("hex")}`;
const bridgeIdentity: MeshIdentity = { id: "bridge:peer", name: "peer", kind: "main" };
const fixture = async (backend: "file" | "sqlite") => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-file-tombstone-"));
  roots.push(root);
  const hub = new MeshStore(root, 64 * 1024, 100, { stateBackend: backend });
  stores.push(hub);
  expect(hub.stateBackend).toBe(backend);
  const now = Date.now(), id = "session:tombstone-00000000";
  const identity: MeshIdentity = { id, name: "remote", kind: "main", sessionId: "tombstone-00000000" };
  const host: FabricHostRecord = { format: 1, id, rootId: id, identity, startedAt: now,
    incarnationToken: "mirror-A", updatedAt: now, expiresAt: now + 60_000 };
  const participant = { format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id,
    name: "remote", label: "REMOTE", status: "idle", runner: "pi", transport: "host", capabilities: ["steer"],
    sessionId: identity.sessionId, startedAt: now, updatedAt: now, controlProtocol: "v1" } as FabricParticipantRecord;
  const side = new StoreBridgeSide(hub, "peer", () => now, false, true);
  await side.mirror({ hosts: [{ record: host, expiresAt: host.expiresAt, observedAt: now }], participants: [participant] }, now);
  await side.flushLeaseEffects();
  const outbox = new CommitOutbox(hub, "bridge/peer", bridgeIdentity, {});
  const file = hostLeasePath(root, id), hostKey = key("hosts", id), participantKey = key("participants", id);
  const lease = readHostLease(root, id)!;
  expect(lease.incarnationToken).toBe("mirror-A");
  return { root, hub, now, id, identity, host, participant, side, outbox, file, hostKey, participantKey, lease };
};

// The hook is precisely the deferred effect's SECOND per-id acquisition, after the
// deletion commit and before custody is reacquired. It never fabricates an effect view.
const beforeDeferredEffect = (hub: MeshStore, change: () => void | Promise<void>) => {
  const acquire = hub.leaseCustody.bind(hub);
  let acquisitions = 0;
  const spy = vi.spyOn(hub, "leaseCustody").mockImplementation(async (file, operation, timeout, options) => {
    if (++acquisitions === 2) await change();
    return acquire(file, operation, timeout, options);
  });
  return { spy, count: () => acquisitions };
};

it.each(["file", "sqlite"] as const)("deferred %s withdrawal removes its own lease after the versioned deletion", async backend => {
  const f = await fixture(backend);
  const custody = beforeDeferredEffect(f.hub, () => {
    expect(f.hub.get(f.hostKey, { fresh: true })).toBeUndefined();
    expect(f.hub.get(f.participantKey, { fresh: true })).toBeUndefined();
    expect(f.hub.get(f.outbox.rowKey(`lease:${f.id}`), { fresh: true })?.value).toMatchObject({ kind: "unlease" });
    expect(fs.existsSync(f.file)).toBe(true);
  });
  await f.side.mirror({ hosts: [], participants: [] });
  await f.side.flushLeaseEffects();
  expect(custody.count()).toBe(2);
  expect(f.hub.get(f.hostKey, { fresh: true })).toBeUndefined();
  expect(f.hub.get(f.participantKey, { fresh: true })).toBeUndefined();
  expect(fs.existsSync(f.file)).toBe(false);
  expect(f.hub.listAll(f.outbox.prefix, { fresh: true })).toEqual([]);
});

it.each(["file", "sqlite"] as const)("deferred %s explicit withdraw removes its own lease", async backend => {
  const f = await fixture(backend);
  await f.side.withdraw();
  await f.side.flushLeaseEffects();
  expect(f.hub.get(f.hostKey, { fresh: true })).toBeUndefined();
  expect(f.hub.get(f.participantKey, { fresh: true })).toBeUndefined();
  expect(fs.existsSync(f.file)).toBe(false);
  expect(f.hub.listAll(f.outbox.prefix, { fresh: true })).toEqual([]);
});

it.each(["file", "sqlite"] as const)("%s does not equate a newer delete/recreate/delete tombstone with the expected deletion", async backend => {
  const f = await fixture(backend);
  const custody = beforeDeferredEffect(f.hub, async () => {
    const recreated = await f.hub.put({ key: f.hostKey, identity: f.identity, value: f.host });
    const deleted = await f.hub.delete({ key: f.hostKey, ifVersion: recreated.version });
    expect(deleted.version).toBeGreaterThan(recreated.version);
  });
  await f.side.mirror({ hosts: [], participants: [] });
  await f.side.flushLeaseEffects();
  expect(custody.count()).toBe(2);
  // Same owner and absent host are insufficient: the exact version must still match.
  expect(readHostLease(f.root, f.id)).toEqual(f.lease);
  expect(f.hub.get(f.hostKey, { fresh: true })).toBeUndefined();
  expect(f.hub.listAll(f.outbox.prefix, { fresh: true })).toEqual([]);
});

it.each(["native-lease", "participant-file", "replacement-incarnation", "live-host"] as const)(
  "FILE deferred removal preserves a %s arriving before custody reacquisition", async claim => {
    const f = await fixture("file");
    let expectedBytes = fs.readFileSync(f.file);
    const custody = beforeDeferredEffect(f.hub, async () => {
      expect(f.hub.get(f.hostKey, { fresh: true })).toBeUndefined();
      if (claim === "participant-file") writeParticipantFile(f.root, { key: f.participantKey, value: f.participant,
        version: 1, updatedAt: f.now, updatedBy: f.identity });
      else if (claim === "live-host") await f.hub.put({ key: f.hostKey, identity: f.identity, value: f.host });
      else writeHostLease(f.root, { ...f.lease,
        ...(claim === "native-lease" ? {} : { incarnationToken: "native-B", startedAt: f.now + 1 }),
        ...(claim === "native-lease" ? { writer: { pid: process.pid, host: "native", releaseSha: "test",
          lockProtocol: 1, stateBackend: "file", startedAt: f.now + 1 } } : {}) });
      expectedBytes = fs.readFileSync(f.file);
    });
    await f.side.mirror({ hosts: [], participants: [] });
    await f.side.flushLeaseEffects();
    expect(custody.count()).toBe(2);
    expect(fs.readFileSync(f.file)).toEqual(expectedBytes);
    expect(f.side.holds(f.id, f.identity)).toBe(false);
    await expect(f.side.publish({ topic: "fleet.work.task", kind: "ask", from: f.identity,
      to: "session:hub-main", data: { bridge: { from: "peer", id: "stale-owner" } } }, [f.id]))
      .rejects.toBeInstanceOf(BridgeOwnershipError);
    expect(f.hub.latestSequence()).toBe(0);
    expect(f.hub.listAll(f.outbox.prefix, { fresh: true })).toEqual([]);
  },
);

it.each(["EACCES", "invalid"] as const)("FILE strict %s removal retains the exact row and retries under per-id custody", async failure => {
  const f = await fixture("file");
  const bytes = fs.readFileSync(f.file), read = fs.readFileSync.bind(fs);
  let active = false, strictReads = 0;
  const acquire = f.hub.leaseCustody.bind(f.hub);
  vi.spyOn(f.hub, "leaseCustody").mockImplementation((file, operation, timeout, options) =>
    acquire(file, async () => {
      expect(file).toBe(f.file);
      active = true;
      try { return await operation(); } finally { active = false; }
    }, timeout, options));
  const fault = vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...args: unknown[]) => {
    if (String(file) === f.file && active) {
      strictReads++;
      if (failure === "EACCES") throw Object.assign(new Error("unreadable current lease"), { code: "EACCES" });
      return "invalid current lease";
    }
    return Reflect.apply(read, fs, [file, ...args]);
  }) as typeof fs.readFileSync);
  await f.side.mirror({ hosts: [], participants: [] });
  await f.side.flushLeaseEffects();
  expect(strictReads).toBeGreaterThan(0);
  const row = f.hub.get(f.outbox.rowKey(`lease:${f.id}`), { fresh: true });
  expect(row?.value).toMatchObject({ kind: "unlease" });
  expect(fs.readFileSync(f.file)).toEqual(bytes);
  await f.side.flushLeaseEffects();
  expect(f.hub.get(f.outbox.rowKey(`lease:${f.id}`), { fresh: true })).toEqual(row);
  fault.mockRestore();
  // A new side proves durable replay, not merely retry of the old closure.
  const restarted = new StoreBridgeSide(f.hub, "peer", () => f.now, false, true);
  await restarted.mirror({ hosts: [], participants: [] });
  await restarted.flushLeaseEffects();
  expect(fs.existsSync(f.file)).toBe(false);
  expect(f.hub.listAll(f.outbox.prefix, { fresh: true })).toEqual([]);
});
