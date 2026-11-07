import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { hostLeaseAdditionsGatePath } from "../src/topology/host-lease-lock.js";
import { DEAD_HOST_RECORDS_MS, DIRECTORY_RETENTION_MS, reapDeadHostRecords } from "../src/topology/host-reaper.js";
import { hostLeasePath, prepareHostLeaseArchiveBatch, readHostLeaseCurrent, renewHostLease,
  removeHostLeaseVersionUnderCustody, withOwnedHostLease, writeHostLease,
  type FabricHostLease } from "../src/topology/host-leases.js";
import { readParticipantFiles, writeParticipantFile } from "../src/topology/participant-files.js";

// Keep reason: data safety and the recorded archive/custody stall regression. Exercise the real
// reaper and both state backends, not a source-text check or an archive-helper mock.
const roots: string[] = [], stores: MeshStore[] = [];
const identity: MeshIdentity = { id: "session:batch-observer", name: "observer", kind: "main" };
const hostKey = (id: string): string => "topology/hosts/" + createHash("sha256").update(id).digest("hex");
const sha256 = (bytes: Buffer): string => createHash("sha256").update(bytes).digest("hex");
const closeMeshStore = (mesh: MeshStore): void => mesh.closeState();
afterEach(() => {
  vi.restoreAllMocks();
  for (const mesh of stores.splice(0).reverse()) closeMeshStore(mesh);
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

type Backend = "file" | "sqlite";
const fixture = (stateBackend: Backend = "file") => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "lease-batch-")); roots.push(root);
  const mesh = new MeshStore(root, 256 * 1024, 1000, { stateBackend }); stores.push(mesh);
  expect(mesh.stateBackend).toBe(stateBackend); // A fallback must not silently erase SQLite coverage.
  const now = Date.now();
  const lease = (id: string, expiresAt = now - DEAD_HOST_RECORDS_MS - 1000): FabricHostLease => ({
    id, rootId: `root:${id}`, identityId: `identity:${id}`, startedAt: 1,
    incarnationToken: `token:${id}`, updatedAt: expiresAt - 1000, expiresAt,
  });
  const own = lease(identity.id, now + 60_000);
  writeHostLease(root, own);
  const reap = () => reapDeadHostRecords(mesh, identity, { ownHostId: own.id, now,
    withCommitFence: operation => withOwnedHostLease(mesh, own, operation) });
  return { root, mesh, now, lease, own, reap };
};
type Fixture = ReturnType<typeof fixture>;

// These are deliberately NOT canonical JSON bytes. Both unknown nested metadata and invalid
// UTF-8 are valid historical input once decoded by the lease parser, but must be archived verbatim.
const rawLeaseBytes = (lease: FabricHostLease, metadata = "before", invalidByte = 0xff): Buffer => Buffer.concat([
  Buffer.from(` { "format" : 1, ${JSON.stringify(lease).slice(1, -1)}, "future" : { "metadata" : "${metadata}", "raw" : "`),
  Buffer.from([invalidByte]), Buffer.from('" }, "tail" : [true, null, {"keep":"all"}] }\n'),
]);
const seed = (s: Fixture, lease: FabricHostLease, bytes = rawLeaseBytes(lease)) => {
  const file = hostLeasePath(s.root, lease.id);
  fs.writeFileSync(file, bytes);
  const expected = readHostLeaseCurrent(s.root, lease.id);
  expect(expected).toBeDefined();
  return { file, bytes, expected: expected! };
};
interface BatchSnapshot {
  format: 2;
  status: "prepared";
  removalConfirmed: false;
  preparedAt: number;
  candidates: Array<{ hostId: string; leaseSha256: string; leaseBase64: string }>;
}
const batchFiles = (root: string): string[] => {
  const directory = path.join(root, "host-lease-archive", "batches");
  return fs.readdirSync(directory).sort().map(name => path.join(directory, name));
};
const onlyBatch = (root: string) => {
  const files = batchFiles(root);
  expect(files).toHaveLength(1);
  const file = files[0]!, bytes = fs.readFileSync(file);
  const snapshot = JSON.parse(bytes.toString("utf8")) as BatchSnapshot;
  return { file, bytes, snapshot };
};
const expectSnapshot = (snapshot: BatchSnapshot, preparedAt: number, candidates: ReadonlyMap<string, Buffer>): void => {
  expect(snapshot).toEqual({ format: 2, status: "prepared", removalConfirmed: false, preparedAt,
    candidates: expect.any(Array) });
  expect(snapshot.candidates).toHaveLength(candidates.size);
  expect(new Set(snapshot.candidates.map(candidate => candidate.hostId))).toEqual(new Set(candidates.keys()));
  for (const candidate of snapshot.candidates) {
    const bytes = candidates.get(candidate.hostId)!;
    expect(candidate).toEqual({ hostId: candidate.hostId, leaseSha256: sha256(bytes), leaseBase64: bytes.toString("base64") });
    expect(Buffer.from(candidate.leaseBase64, "base64")).toEqual(bytes);
    expect(sha256(Buffer.from(candidate.leaseBase64, "base64"))).toBe(candidate.leaseSha256);
  }
};

const observeGates = (mesh: MeshStore) => {
  const held = new Set<string>(), requested: string[] = [];
  const custody = mesh.leaseCustody.bind(mesh);
  vi.spyOn(mesh, "leaseCustody").mockImplementation((file, operation, timeout, options) => {
    requested.push(file);
    return custody(file, async () => {
      held.add(file);
      try { return await operation(); } finally { held.delete(file); }
    }, timeout, options);
  });
  return { held, requested };
};
interface SyncEvent { file: string; directory: boolean }
// Patch the REAL opened FileHandle.sync, not the exported namespace function. Successful
// small tests call the real syscall; fault tests fail the same syscall site with EIO.
const observeAsyncSync = (onSync: (event: SyncEvent, sync: () => Promise<void>) => Promise<void>) => {
  const events: SyncEvent[] = [], open = fs.promises.open;
  vi.spyOn(fs.promises, "open").mockImplementation(async (...args: Parameters<typeof fs.promises.open>) => {
    const handle = await open(...args);
    const event = { file: String(args[0]), directory: (await handle.stat()).isDirectory() };
    const sync = handle.sync.bind(handle);
    vi.spyOn(handle, "sync").mockImplementation(async () => {
      events.push(event);
      await onSync(event, sync);
    });
    return handle;
  });
  return events;
};
const rejectSynchronousSync = () => {
  const unexpected = (): never => { throw new Error("Archive performed a synchronous durability barrier"); };
  return { fsync: vi.spyOn(fs, "fsyncSync").mockImplementation(unexpected),
    fdatasync: vi.spyOn(fs, "fdatasyncSync").mockImplementation(unexpected) };
};
const deferred = () => {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
};

// Include state, participant files and unrelated eligible bookkeeping so a failed preparation
// or final CAS must defer the WHOLE pass, not merely skip one lease unlink.
const seedStateVictims = async (s: Fixture, target: FabricHostLease) => {
  const other = s.lease("other-victim");
  const first = seed(s, target), second = seed(s, other);
  for (const value of [target, other]) {
    await s.mesh.put({ key: hostKey(value.id), identity, value: { format: 1, ...value,
      identity: { ...identity, id: value.identityId } } });
  }
  const participantKey = "topology/participants/" + createHash("sha256").update("archived-target").digest("hex");
  const entry = await s.mesh.put({ key: participantKey, identity,
    value: { id: "archived-target", ownerHostId: target.id } });
  writeParticipantFile(s.root, entry);
  const clock = vi.spyOn(Date, "now").mockReturnValue(s.now - DIRECTORY_RETENTION_MS - 1000);
  try {
    await s.mesh.put({ key: "sessions/old-terminal", identity, value: { status: "completed" } });
  } finally { clock.mockRestore(); }
  const state = s.mesh.listAll("", { fresh: true }), participants = readParticipantFiles(s.root);
  expect(participants).toHaveLength(1);
  expect(state.find(entry => entry.key === "sessions/old-terminal")!.updatedAt).toBeLessThan(s.now - DIRECTORY_RETENTION_MS);
  return { first, second, state, participants, candidates: new Map([[target.id, first.bytes], [other.id, second.bytes]]) };
};

describe.each(["file", "sqlite"] as const)("%s prepared host lease archive batches", backend => {
  it("LOGIC ONLY: drains 500 file-only leases in five 100-bounded passes, one snapshot per pass, no synchronous fsync under gates/outbox", async () => {
    const s = fixture(backend);
    const stale = Array.from({ length: 500 }, (_, index) => s.lease(`backlog:${index}`));
    const expected = new Map(stale.map(value => [value.id, seed(s, value).bytes]));
    const live = s.lease("live-bystander", s.now + 60_000), liveBytes = seed(s, live).bytes;
    expect(s.mesh.listAll("topology/hosts/", { fresh: true })).toEqual([]);
    const gates = observeGates(s.mesh), syncInCustody: SyncEvent[] = [];
    // Only this bulk logic test stubs the async barrier syscalls. It proves batching/order,
    // NOT fsync wall time or physical durability; real-fsync timing is separate evidence.
    const syncs = observeAsyncSync(async event => { if (gates.held.size) syncInCustody.push(event); });
    const synchronous = rejectSynchronousSync();
    const writeBatch = s.mesh.writeBatch.bind(s.mesh);
    let inOutbox = false, outboxes = 0, unlinks = 0;
    vi.spyOn(s.mesh, "writeBatch").mockImplementation(input => writeBatch({ ...input,
      commitOutbox: effects => {
        outboxes++;
        inOutbox = true;
        try { input.commitOutbox?.(effects); } finally { inOutbox = false; }
      } }));
    const remove = fs.rmSync;
    vi.spyOn(fs, "rmSync").mockImplementation((file, options) => {
      if (path.dirname(String(file)) === path.join(s.root, "host-leases") && path.basename(String(file)).startsWith(".unlease-")) {
        const id = (JSON.parse(fs.readFileSync(file, "utf8")) as { id: string }).id;
        expect(inOutbox).toBe(true);
        expect(gates.held.has(hostLeaseAdditionsGatePath(s.root))).toBe(true);
        expect(gates.held.has(hostLeasePath(s.root, id))).toBe(true);
        expect(gates.held.has(hostLeasePath(s.root, s.own.id))).toBe(true);
        unlinks++;
      }
      return remove(file, options);
    });
    const snapshots = new Map<string, Buffer>(), archivedIds = new Set<string>(), passSizes: number[] = [];
    let removed = 0;
    for (const count of [100, 100, 100, 100, 100]) {
      const syncStart = syncs.length;
      const reaped = await s.reap();
      expect(reaped).toBe(count);
      passSizes.push(reaped);
      removed += reaped;
      expect(outboxes).toBe(passSizes.length);
      expect(unlinks).toBe(removed);
      const files = batchFiles(s.root);
      expect(files).toHaveLength(passSizes.length);
      const added = files.filter(file => !snapshots.has(file));
      expect(added).toHaveLength(1);
      const file = added[0]!, bytes = fs.readFileSync(file);
      const snapshot = JSON.parse(bytes.toString("utf8")) as BatchSnapshot;
      expect(snapshot.candidates).toHaveLength(count);
      const candidates = new Map<string, Buffer>();
      for (const candidate of snapshot.candidates) {
        expect(expected.has(candidate.hostId)).toBe(true);
        expect(archivedIds.has(candidate.hostId)).toBe(false);
        candidates.set(candidate.hostId, expected.get(candidate.hostId)!);
        archivedIds.add(candidate.hostId);
      }
      expectSnapshot(snapshot, s.now, candidates);
      snapshots.set(file, bytes);
      for (const [archivedFile, archivedBytes] of snapshots) {
        expect(fs.readFileSync(archivedFile)).toEqual(archivedBytes); // Never promoted to "removed".
      }
      const passSyncs = syncs.slice(syncStart);
      expect(passSyncs.filter(event => !event.directory).map(event => event.file)).toEqual([file]);
      const directories = passSyncs.filter(event => event.directory).map(event => event.file);
      if (process.platform === "win32") expect(directories).toEqual([]);
      else {
        expect(directories).toContain(path.join(s.root, "host-lease-archive", "batches"));
        expect(new Set(directories).size).toBe(directories.length); // One shared namespace chain per pass.
      }
      for (const value of stale) {
        expect(fs.existsSync(hostLeasePath(s.root, value.id))).toBe(!archivedIds.has(value.id));
      }
      expect(readHostLeaseCurrent(s.root, s.own.id)).toEqual(s.own);
      expect(readHostLeaseCurrent(s.root, live.id)).toEqual(live);
      expect(fs.readFileSync(hostLeasePath(s.root, live.id))).toEqual(liveBytes);
      expect(gates.held.size).toBe(0);
    }
    expect(passSizes).toEqual([100, 100, 100, 100, 100]);
    expect([...archivedIds].sort()).toEqual([...expected.keys()].sort());
    expect(unlinks).toBe(500);
    expect(syncInCustody).toEqual([]);
    expect(synchronous.fsync).not.toHaveBeenCalled();
    expect(synchronous.fdatasync).not.toHaveBeenCalled();
    expect(syncs.filter(event => !event.directory)).toHaveLength(5);
    expect(fs.readdirSync(path.join(s.root, "host-leases")).filter(name => name.startsWith(".unlease-"))).toEqual([]);
    expect(fs.readdirSync(path.join(s.root, "host-lease-archive"))).toEqual(["batches"]);
    const syncCount = syncs.length;
    expect(await s.reap()).toBe(0);
    expect(outboxes).toBe(5);
    expect(unlinks).toBe(500);
    expect(batchFiles(s.root)).toEqual([...snapshots.keys()].sort());
    for (const [file, bytes] of snapshots) expect(fs.readFileSync(file)).toEqual(bytes);
    expect(syncs).toHaveLength(syncCount);
  }, 30_000);

  it("uses real async file/namespace barriers and leaves the successful snapshot immutable and unconfirmed", async () => {
    const s = fixture(backend), target = s.lease("real-barrier");
    // Backend bootstrap may sync its migration marker; observe only the archive path below.
    expect(s.mesh.listAll("topology/hosts/", { fresh: true })).toEqual([]);
    const value = seed(s, target), gates = observeGates(s.mesh);
    const syncInCustody: SyncEvent[] = [];
    let fileSynced = false, directoriesSynced = 0;
    const events = observeAsyncSync(async (event, sync) => {
      if (gates.held.size) syncInCustody.push(event);
      await sync();
      if (event.directory) directoriesSynced++; else fileSynced = true;
    });
    const synchronous = rejectSynchronousSync();
    const rename = fs.renameSync;
    let atDetach: ReturnType<typeof onlyBatch> | undefined;
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (String(from) === value.file && path.basename(String(to)).startsWith(".unlease-")) {
        expect(fileSynced).toBe(true);
        expect(directoriesSynced > 0).toBe(process.platform !== "win32");
        atDetach = onlyBatch(s.root);
      }
      return rename(from, to);
    });
    expect(await s.reap()).toBe(1);
    expect(atDetach).toBeDefined();
    expectSnapshot(atDetach!.snapshot, s.now, new Map([[target.id, value.bytes]]));
    expect(fs.readFileSync(atDetach!.file)).toEqual(atDetach!.bytes);
    expect(readHostLeaseCurrent(s.root, target.id)).toBeUndefined();
    expect(events.filter(event => !event.directory)).toHaveLength(1);
    expect(syncInCustody).toEqual([]);
    expect(synchronous.fsync).not.toHaveBeenCalled();
    expect(synchronous.fdatasync).not.toHaveBeenCalled();
  });

  for (const barrier of ["file", "directory"] as const) {
    it.skipIf(barrier === "directory" && process.platform === "win32")(
      `holds NO additions/target/own gate while the async ${barrier} sync is awaited`, async () => {
        const s = fixture(backend), target = s.lease(`paused:${barrier}`), value = seed(s, target);
        const gates = observeGates(s.mesh), reached = deferred(), release = deferred();
        const fence = vi.fn();
        let paused = false;
        observeAsyncSync(async (event, sync) => {
          if (!paused && event.directory === (barrier === "directory")) {
            paused = true;
            reached.resolve();
            await release.promise;
          }
          await sync();
        });
        const pending = reapDeadHostRecords(s.mesh, identity, { ownHostId: s.own.id, now: s.now,
          withCommitFence: operation => { fence(); return withOwnedHostLease(s.mesh, s.own, operation); } });
        try {
          await Promise.race([reached.promise, pending.then(() => { throw new Error("Reaper finished before awaited preparation"); })]);
          expect(gates.requested).toEqual([]);
          expect(gates.held.size).toBe(0);
          expect(fence).not.toHaveBeenCalled();
          expect(fs.readFileSync(value.file)).toEqual(value.bytes);
          const snapshot = onlyBatch(s.root);
          const peer = new MeshStore(s.root, 256 * 1024, 1000, { stateBackend: backend }); stores.push(peer);
          const renewed = { ...target, updatedAt: s.now, expiresAt: s.now + 60_000 };
          // Real peer operations prove the gates are available, beyond just observing our spy.
          await renewHostLease(peer, renewed);
          const addition = s.lease("created-during-preparation", s.now + 60_000);
          await renewHostLease(peer, addition, { claim: true });
          expect(await withOwnedHostLease(peer, s.own, () => "own gate available")).toBe("own gate available");
          expect(readHostLeaseCurrent(s.root, addition.id)).toEqual(addition);
          release.resolve();
          expect(await pending).toBe(0); // The renewal wins the later target revalidation.
          expect(readHostLeaseCurrent(s.root, target.id)).toEqual(renewed);
          expect(fs.readFileSync(snapshot.file)).toEqual(snapshot.bytes);
          expectSnapshot(snapshot.snapshot, s.now, new Map([[target.id, value.bytes]]));
          expect(fence).not.toHaveBeenCalled();
        } finally { release.resolve(); await pending; }
      });

    it.skipIf(barrier === "directory" && process.platform === "win32")(
      `real opened-handle ${barrier} sync failure prevents ALL state/participant/lease/bookkeeping deletes`, async () => {
        const s = fixture(backend), target = s.lease(`failed:${barrier}`);
        const before = await seedStateVictims(s, target);
        const gates = observeGates(s.mesh), batch = vi.spyOn(s.mesh, "writeBatch");
        let failures = 0;
        const events = observeAsyncSync(async (event, sync) => {
          if (event.directory === (barrier === "directory")) {
            failures++;
            throw Object.assign(new Error("injected FileHandle.sync failure"), { code: "EIO" });
          }
          await sync();
        });
        const rename = vi.spyOn(fs, "renameSync"), remove = vi.spyOn(fs, "rmSync");
        expect(await s.reap()).toBe(0);
        expect(failures).toBe(1); // Fault reached the actual durability call, not a fake helper.
        expect(events.filter(event => !event.directory)).toHaveLength(1);
        expect(gates.requested).toEqual([]);
        expect(batch).not.toHaveBeenCalled();
        expect(s.mesh.listAll("", { fresh: true })).toEqual(before.state);
        expect(readParticipantFiles(s.root)).toEqual(before.participants);
        expect(fs.readFileSync(before.first.file)).toEqual(before.first.bytes);
        expect(fs.readFileSync(before.second.file)).toEqual(before.second.bytes);
        expect(rename.mock.calls.filter(([, to]) => path.basename(String(to)).startsWith(".unlease-"))).toEqual([]);
        expect(remove.mock.calls.filter(([file]) => path.basename(String(file)).startsWith(".unlease-"))).toEqual([]);
        expectSnapshot(onlyBatch(s.root).snapshot, s.now, before.candidates);
      });
  }

  for (const stage of ["gate-entry", "final-commit"] as const) {
    it.each(["replacement", "renewal", "unknown-metadata", "non-UTF8-only"] as const)(
      `preserves a %s change after preparation at ${stage}, with no removal claim`, async change => {
        const s = fixture(backend), target = s.lease("changed-after-preparation");
        const before = await seedStateVictims(s, target);
        const replacement = change === "replacement" ? { ...target, incarnationToken: "successor-token" }
          : change === "renewal" ? { ...target, updatedAt: s.now, expiresAt: s.now + 60_000 } : target;
        const newBytes = rawLeaseBytes(replacement, change === "unknown-metadata" ? "after!" : "before",
          change === "non-UTF8-only" ? 0xfe : 0xff);
        let snapshot: ReturnType<typeof onlyBatch> | undefined, changed = false;
        const mutate = () => {
          expect(changed).toBe(false);
          snapshot = onlyBatch(s.root); // A published prepared snapshot already exists.
          fs.writeFileSync(before.first.file, newBytes); // Independent historical writer bypassing custody.
          changed = true;
        };
        if (change === "non-UTF8-only") expect(newBytes.toString("utf8")).toBe(before.first.bytes.toString("utf8"));
        if (stage === "gate-entry") {
          const custody = s.mesh.leaseCustody.bind(s.mesh);
          vi.spyOn(s.mesh, "leaseCustody").mockImplementation((file, operation, timeout, options) => {
            if (file === hostLeaseAdditionsGatePath(s.root) && !changed) mutate();
            return custody(file, operation, timeout, options);
          });
        } else {
          const writeBatch = s.mesh.writeBatch.bind(s.mesh);
          vi.spyOn(s.mesh, "writeBatch").mockImplementationOnce(input => writeBatch({ ...input,
            beforeCommit: () => { mutate(); input.beforeCommit?.(); } }));
        }
        expect(await s.reap()).toBe(0);
        expect(changed).toBe(true);
        expect(fs.readFileSync(before.first.file)).toEqual(newBytes);
        expect(readHostLeaseCurrent(s.root, target.id)).toEqual(replacement);
        expect(fs.readFileSync(before.second.file)).toEqual(before.second.bytes);
        expect(s.mesh.listAll("", { fresh: true })).toEqual(before.state);
        expect(readParticipantFiles(s.root)).toEqual(before.participants);
        expectSnapshot(snapshot!.snapshot, s.now, before.candidates);
        expect(fs.readFileSync(snapshot!.file)).toEqual(snapshot!.bytes);
        expect(batchFiles(s.root)).toEqual([snapshot!.file]);
      });
  }
});

describe("prepared snapshot handle and detached-byte boundary", () => {
  it("preparation alone is not a removal receipt; repeated preparation never rewrites the first snapshot", async () => {
    const s = fixture(), target = s.lease("prepared-not-removed"), value = seed(s, target);
    const first = await prepareHostLeaseArchiveBatch(s.root, [value.expected], s.now);
    const bytes = fs.readFileSync(first.file);
    expect(Object.isFrozen(first)).toBe(true);
    expect(first.meshRoot).toBe(s.root);
    expect(first.matches(target.id, value.bytes)).toBe(true);
    expect(first.matches("different-host", value.bytes)).toBe(false);
    const changed = rawLeaseBytes(target, "before", 0xfe);
    expect(changed.toString("utf8")).toBe(value.bytes.toString("utf8"));
    expect(first.matches(target.id, changed)).toBe(false);
    // This is the state a crash before commit leaves, NOT a claim to have simulated a crash.
    expect(fs.readFileSync(value.file)).toEqual(value.bytes);
    expectSnapshot(JSON.parse(bytes.toString("utf8")), s.now, new Map([[target.id, value.bytes]]));
    const second = await prepareHostLeaseArchiveBatch(s.root, [value.expected], s.now + 1);
    expect(second.file).not.toBe(first.file);
    expect(fs.readFileSync(first.file)).toEqual(bytes);
    expect(batchFiles(s.root)).toHaveLength(2);
    expect(readHostLeaseCurrent(s.root, target.id)).toEqual(value.expected);
  });

  it("does not turn a prepared snapshot into a removal claim when the detached unlink fails", async () => {
    const s = fixture(), target = s.lease("unlink-failure"), value = seed(s, target);
    const prepared = await prepareHostLeaseArchiveBatch(s.root, [value.expected], s.now);
    const snapshotBytes = fs.readFileSync(prepared.file), remove = fs.rmSync;
    const failure = Object.assign(new Error("detached unlink denied"), { code: "EACCES" });
    let failed = false;
    vi.spyOn(fs, "rmSync").mockImplementation((file, options) => {
      if (path.dirname(String(file)) === path.dirname(value.file) && path.basename(String(file)).startsWith(".unlease-")) {
        failed = true;
        throw failure;
      }
      return remove(file, options);
    });
    await expect(s.mesh.leaseCustody(value.file,
      () => removeHostLeaseVersionUnderCustody(s.root, value.expected, s.now, prepared))).rejects.toBe(failure);
    expect(failed).toBe(true);
    const aside = fs.readdirSync(path.dirname(value.file)).filter(name => name.startsWith(".unlease-"));
    expect(aside).toHaveLength(1);
    expect(fs.readFileSync(path.join(path.dirname(value.file), aside[0]!))).toEqual(value.bytes);
    expect(fs.readFileSync(prepared.file)).toEqual(snapshotBytes);
    expectSnapshot(JSON.parse(snapshotBytes.toString("utf8")), s.now, new Map([[target.id, value.bytes]]));
  });

  it.each(["unknown-metadata", "non-UTF8-only"] as const)(
    "restores a %s change that races the detach, even when the parsed version still matches", async change => {
      const s = fixture(), target = s.lease("detached-byte-race"), value = seed(s, target);
      const prepared = await prepareHostLeaseArchiveBatch(s.root, [value.expected], s.now);
      const snapshotBytes = fs.readFileSync(prepared.file);
      const newBytes = rawLeaseBytes(target, change === "unknown-metadata" ? "after!" : "before",
        change === "non-UTF8-only" ? 0xfe : 0xff);
      const synchronous = rejectSynchronousSync();
      const rename = fs.renameSync;
      let raced = false;
      vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
        if (String(from) === value.file && path.basename(String(to)).startsWith(".unlease-")) {
          raced = true;
          fs.writeFileSync(value.file, newBytes);
        }
        return rename(from, to);
      });
      expect(await s.mesh.leaseCustody(value.file,
        () => removeHostLeaseVersionUnderCustody(s.root, value.expected, s.now, prepared))).toBe(false);
      expect(raced).toBe(true);
      expect(readHostLeaseCurrent(s.root, target.id)).toEqual(value.expected);
      expect(fs.readFileSync(value.file)).toEqual(newBytes);
      expect(fs.readdirSync(path.dirname(value.file)).filter(name => name.startsWith(".unlease-"))).toEqual([]);
      expect(fs.readFileSync(prepared.file)).toEqual(snapshotBytes);
      expectSnapshot(JSON.parse(snapshotBytes.toString("utf8")), s.now, new Map([[target.id, value.bytes]]));
      expect(synchronous.fsync).not.toHaveBeenCalled();
      expect(synchronous.fdatasync).not.toHaveBeenCalled();
    });
});
