import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { readPhysicalHostIdentity } from "../src/core/atomic-write.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { rootPresenceAlarms } from "../src/topology/stall-alarms.js";
import { hostLeaseAdditionsGatePath } from "../src/topology/host-lease-lock.js";
import { reapDeadHostRecords } from "../src/topology/host-reaper.js";
import { hostLeasePath, readHostLeaseCurrent, readHostLeases, renewHostLease, writeHostLease,
  type FabricHostLease } from "../src/topology/host-leases.js";

const HOUR = 60 * 60 * 1_000;
const STALE_LEASES = 2_000;
const LIVE_HOSTS = 10;
const MAX_REAP_PASSES = 64;
const roots: string[] = [];
const stores: MeshStore[] = [];
const directories: ParticipantDirectory[] = [];

afterEach(async () => {
  vi.restoreAllMocks();
  await Promise.all(directories.splice(0).map(directory => directory.close()));
  for (const mesh of stores.splice(0)) mesh.closeState();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const stateHostKey = (id: string): string =>
  `topology/hosts/${createHash("sha256").update(id).digest("hex")}`;
const newMainIdentity = (name: string): MeshIdentity => {
  const sessionId = randomUUID();
  return { id: `session:${sessionId}`, name, kind: "main", sessionId };
};

const seedLiveHost = async (mesh: MeshStore, identity: MeshIdentity, now: number): Promise<FabricHostLease> => {
  const lease: FabricHostLease = {
    id: identity.id,
    rootId: identity.id,
    identityId: identity.id,
    startedAt: now - HOUR,
    updatedAt: now,
    expiresAt: now + HOUR,
    incarnationToken: randomUUID(),
  };
  writeHostLease(mesh.root, lease);
  await mesh.put({ key: stateHostKey(identity.id), identity, value: {
    format: 1, ...lease, identity, livenessLeaseFiles: 1,
  } });
  return lease;
};

const seedExpiredOrphanLease = (mesh: MeshStore, index: number, now: number): FabricHostLease => {
  const identity = newMainIdentity(`orphan-${index}`);
  const lease: FabricHostLease = {
    id: `host:${randomUUID()}`,
    rootId: identity.id,
    identityId: identity.id,
    startedAt: now - 9 * HOUR,
    updatedAt: now - 8 * HOUR,
    expiresAt: now - 7 * HOUR,
    incarnationToken: randomUUID(),
  };
  writeHostLease(mesh.root, lease);
  return lease;
};

/** Count actual filesystem observations under host-leases, not JSON parse calls or a fake reader. */
const watchLeaseIo = (meshRoot: string, stalePaths: ReadonlySet<string>, livePaths: ReadonlySet<string>) => {
  const leaseDirectory = path.resolve(meshRoot, "host-leases");
  const stats = vi.spyOn(fs, "statSync");
  const lstats = vi.spyOn(fs, "lstatSync");
  const reads = vi.spyOn(fs, "readFileSync");
  const listings = vi.spyOn(fs, "readdirSync");
  const normalize = (file: unknown): string => path.resolve(String(file));
  const observedFiles = (): string[] => [...stats.mock.calls, ...lstats.mock.calls, ...reads.mock.calls]
    .map(([file]) => normalize(file))
    .filter(file => path.dirname(file) === leaseDirectory);
  const isLiveLeaseOrAtomicTemp = (file: string): boolean => [...livePaths].some(lease =>
    file === lease || file.startsWith(`${lease}.`));
  return {
    assertBounded: () => {
      expect(listings.mock.calls.filter(([directory]) => normalize(directory) === leaseDirectory)).toEqual([]);
      const observed = observedFiles();
      expect(observed.filter(file => stalePaths.has(file))).toEqual([]);
      expect(observed.length).toBeLessThanOrEqual(LIVE_HOSTS * 12);
      expect(observed.every(isLiveLeaseOrAtomicTemp)).toBe(true);
    },
    restore: () => {
      stats.mockRestore();
      lstats.mockRestore();
      reads.mockRestore();
      listings.mockRestore();
    },
  };
};

const meshFixture = (prefix: string): { root: string; mesh: MeshStore } => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
  roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), 256 * 1024, 100);
  stores.push(mesh);
  return { root, mesh };
};

describe("host lease GC and heartbeat scaling (#18579)", () => {
  it("keeps real directory and committed alarm reads proportional to live hosts before and after orphan GC", async () => {
    const { mesh } = meshFixture("host-lease-gc-heartbeat-");
    const now = Date.now();
    const self = newMainIdentity("observer");
    const directory = new ParticipantDirectory(mesh, {
      enabled: true, hostId: self.id, rootId: self.id, identity: self,
      heartbeatMs: 60_000, leaseMs: 60_000, reapDeadHosts: false,
    });
    directories.push(directory);
    directory.registerSource(() => [directory.root({
      id: self.id, name: "Main", kind: "main", status: "idle", runner: "pi", transport: "host",
      cwd: process.cwd(), sessionId: self.sessionId!, startedAt: now, updatedAt: now, pendingMessages: false, local: true,
    })]);
    await directory.refresh();

    const liveIds = [self.id];
    for (let index = 1; index < LIVE_HOSTS; index++) {
      const identity = newMainIdentity(`live-${index}`);
      await seedLiveHost(mesh, identity, now);
      liveIds.push(identity.id);
    }
    expect(mesh.listAll("topology/hosts/", { fresh: true })).toHaveLength(LIVE_HOSTS);

    const staleLeases = Array.from({ length: STALE_LEASES }, (_, index) => seedExpiredOrphanLease(mesh, index, now));
    const stalePaths = new Set(staleLeases.map(lease => path.resolve(hostLeasePath(mesh.root, lease.id))));
    expect(staleLeases.every(lease => !mesh.get(stateHostKey(lease.id), { fresh: true }))).toBe(true);
    const expectedCandidates = new Map(staleLeases.map(lease => {
      const bytes = fs.readFileSync(hostLeasePath(mesh.root, lease.id));
      const leaseSha256 = createHash("sha256").update(bytes).digest("hex");
      return [lease.id, { hostId: lease.id, bytes, leaseSha256 }] as const;
    }));
    const livePaths = new Set(liveIds.map(id => path.resolve(hostLeasePath(mesh.root, id))));

    const alarmIdentityAvailable = readPhysicalHostIdentity() !== undefined;
    const measureDirectoryAndAlarmReads = async (): Promise<void> => {
      const io = watchLeaseIo(mesh.root, stalePaths, livePaths);
      try {
        expect(directory.list({ scope: "project", fresh: true }).map(participant => participant.id)).toContain(self.id);
        await directory.refresh();
        if (alarmIdentityAvailable) {
          // This is the production committed-owner pass; its bridge alarm subpass must not
          // readdir/stat the 2,000 orphan files merely to discover there are no mirrors.
          await rootPresenceAlarms(mesh, self, self.id, [], 15 * 60_000, now);
        }
        io.assertBounded();
      } finally {
        io.restore();
      }
    };

    // Pre-compaction is important: GC alone cannot remove CPU from every heartbeat.
    await measureDirectoryAndAlarmReads();

    const liveBeforeReap = new Map(liveIds.map(id => [id, readHostLeaseCurrent(mesh.root, id)!]));
    const reap = () => reapDeadHostRecords(mesh, self, { ownHostId: self.id, now });
    let passes = 0;
    const passSizes: number[] = [];
    while (passes < MAX_REAP_PASSES && staleLeases.some(lease => fs.existsSync(hostLeasePath(mesh.root, lease.id)))) {
      const remaining = staleLeases.filter(lease => fs.existsSync(hostLeasePath(mesh.root, lease.id))).length;
      const reaped = await reap();
      expect(reaped).toBe(Math.min(100, remaining));
      passSizes.push(reaped);
      passes++;
    }
    expect(passes).toBe(20);
    expect(passSizes).toEqual(Array<number>(20).fill(100));
    expect(staleLeases.filter(lease => fs.existsSync(hostLeasePath(mesh.root, lease.id)))).toEqual([]);
    for (const lease of staleLeases) expect(readHostLeaseCurrent(mesh.root, lease.id)).toBeUndefined();

    const archiveDirectory = path.join(mesh.root, "host-lease-archive", "batches");
    expect(fs.readdirSync(path.dirname(archiveDirectory))).toEqual(["batches"]);
    const archiveFiles = (): string[] => fs.readdirSync(archiveDirectory).map(name => path.join(archiveDirectory, name));
    const snapshots = new Map(archiveFiles().map(file => [file, fs.readFileSync(file)]));
    expect(snapshots.size).toBe(20);
    expect(expectedCandidates.size).toBe(STALE_LEASES);
    const archivedIds = new Set<string>(), archivedBatchSizes: number[] = [];
    for (const bytes of snapshots.values()) {
      const receipt = JSON.parse(bytes.toString("utf8")) as {
        candidates: { hostId: string; leaseSha256: string; leaseBase64: string }[];
      };
      expect(receipt).toEqual({ format: 2, status: "prepared", removalConfirmed: false, preparedAt: now,
        candidates: expect.any(Array) });
      archivedBatchSizes.push(receipt.candidates.length);
      for (const candidate of receipt.candidates) {
        const expected = expectedCandidates.get(candidate.hostId);
        expect(expected).toBeDefined();
        expect(archivedIds.has(candidate.hostId)).toBe(false);
        expect(candidate).toEqual({ hostId: expected!.hostId, leaseSha256: expected!.leaseSha256,
          leaseBase64: expected!.bytes.toString("base64") });
        const archived = Buffer.from(candidate.leaseBase64, "base64");
        expect(archived).toEqual(expected!.bytes);
        expect(createHash("sha256").update(archived).digest("hex")).toBe(expected!.leaseSha256);
        archivedIds.add(candidate.hostId);
      }
    }
    expect(archivedBatchSizes.sort((left, right) => right - left)).toEqual(passSizes);
    expect([...archivedIds].sort()).toEqual([...expectedCandidates.keys()].sort());

    // An empty real pass cannot publish another snapshot or rewrite an existing one as removed.
    const receiptWrites = vi.spyOn(fs.promises, "rename");
    expect(await reap()).toBe(0);
    expect(receiptWrites.mock.calls.filter(([, target]) => path.dirname(String(target)) === archiveDirectory)).toEqual([]);
    receiptWrites.mockRestore();
    expect(archiveFiles().sort()).toEqual([...snapshots.keys()].sort());
    for (const [file, bytes] of snapshots) expect(fs.readFileSync(file)).toEqual(bytes);
    expect([...readHostLeases(mesh.root).keys()].sort()).toEqual([...liveIds].sort());
    for (const id of liveIds) {
      const before = liveBeforeReap.get(id)!;
      const after = readHostLeaseCurrent(mesh.root, id);
      expect(after).toMatchObject({ id, rootId: before.rootId, identityId: before.identityId,
        incarnationToken: before.incarnationToken });
      expect(after!.expiresAt).toBeGreaterThan(now);
      expect(fs.existsSync(hostLeasePath(mesh.root, id))).toBe(true);
    }

    // Maintenance must leave the ordinary user path cheap too, not only make the directory small.
    await measureDirectoryAndAlarmReads();
  });

  it("keeps a real renewal that wins before the reaper acquires its target lease gate", async () => {
    const { mesh } = meshFixture("host-lease-gc-renewal-");
    const now = Date.now();
    const owner = newMainIdentity("renewing-owner");
    const target = newMainIdentity("target");
    const old: FabricHostLease = {
      id: target.id, rootId: target.id, identityId: target.id,
      startedAt: now - 9 * HOUR, updatedAt: now - 8 * HOUR, expiresAt: now - 7 * HOUR,
      incarnationToken: randomUUID(),
    };
    writeHostLease(mesh.root, old);
    const oldBytes = fs.readFileSync(hostLeasePath(mesh.root, old.id));
    await mesh.put({ key: stateHostKey(target.id), identity: target, value: {
      format: 1, ...old, identity: target,
    } });
    const renewed: FabricHostLease = { ...old, updatedAt: now, expiresAt: now + HOUR };
    const additionsGate = hostLeaseAdditionsGatePath(mesh.root);
    const originalLeaseCustody = mesh.leaseCustody.bind(mesh);
    let renewalCommitted = false;
    vi.spyOn(mesh, "leaseCustody").mockImplementation(async <T>(file: string, operation: () => T | Promise<T>,
      lockTimeoutMs = 0, options: { ownIncarnation?: string | undefined } = {}): Promise<T> => {
      if (!renewalCommitted && file === additionsGate) {
        renewalCommitted = true;
        await renewHostLease(mesh, renewed);
      }
      return originalLeaseCustody(file, operation, lockTimeoutMs, options);
    });

    expect(await reapDeadHostRecords(mesh, owner, { ownHostId: owner.id, now })).toBe(0);
    expect(renewalCommitted).toBe(true);
    expect(readHostLeaseCurrent(mesh.root, target.id)).toEqual(renewed);
    expect(mesh.get(stateHostKey(target.id), { fresh: true })).toBeDefined();
    const archiveDirectory = path.join(mesh.root, "host-lease-archive", "batches");
    expect(fs.readdirSync(path.dirname(archiveDirectory))).toEqual(["batches"]);
    const files = fs.readdirSync(archiveDirectory);
    expect(files).toHaveLength(1);
    const receipt = JSON.parse(fs.readFileSync(path.join(archiveDirectory, files[0]!), "utf8"));
    expect(receipt).toEqual({ format: 2, status: "prepared", removalConfirmed: false, preparedAt: now,
      candidates: [{ hostId: old.id, leaseSha256: createHash("sha256").update(oldBytes).digest("hex"),
        leaseBase64: oldBytes.toString("base64") }] });
    expect(Buffer.from(receipt.candidates[0].leaseBase64, "base64")).toEqual(oldBytes);
  });
});
