import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshStateEntry } from "../src/mesh/store.js";
import { DEAD_HOST_RECORDS_MS, reapDeadHostRecords } from "../src/topology/host-reaper.js";
import { hostLeaseArchivePath, hostLeasePath, readHostLeaseCurrent, removeHostLeaseVersionUnderCustody,
  writeHostLease, type FabricHostLease } from "../src/topology/host-leases.js";

// Data safety: exact detached bytes and failed durability barriers cannot be proved by the
// large heartbeat flow alone. These exercise the existing reaper/custody path on real files.
const roots: string[] = [], stores: MeshStore[] = [];
const identity = { id: "session:observer", name: "observer", kind: "main" as const };
const hostKey = (id: string): string => "topology/hosts/" + createHash("sha256").update(id).digest("hex");
const setup = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "host-lease-archive-")); roots.push(root);
  const mesh = new MeshStore(root, 256 * 1024, 1000); stores.push(mesh);
  const now = Date.now();
  const lease = (id: string, expiresAt = now - DEAD_HOST_RECORDS_MS - 1): FabricHostLease => ({
    id, rootId: id, identityId: id, incarnationToken: `token:${id}`, startedAt: 1,
    updatedAt: expiresAt - 1000, expiresAt,
  });
  return { root, mesh, now, lease };
};
const archive = (root: string, id: string, bytes: Buffer) => {
  const digest = createHash("sha256").update(bytes).digest("hex");
  return { file: hostLeaseArchivePath(root, id, digest), digest };
};
const preparedArchive = (root: string, preparedAt: number, snapshots: readonly { hostId: string; bytes: Buffer }[]) => {
  const directory = path.join(root, "host-lease-archive", "batches");
  expect(fs.readdirSync(path.dirname(directory))).toEqual(["batches"]);
  const names = fs.readdirSync(directory);
  expect(names).toHaveLength(1);
  const file = path.join(directory, names[0]!);
  const bytes = fs.readFileSync(file);
  const receipt = JSON.parse(bytes.toString("utf8"));
  expect(receipt).toEqual({ format: 2, status: "prepared", removalConfirmed: false, preparedAt,
    candidates: expect.arrayContaining(snapshots.map(snapshot => ({ hostId: snapshot.hostId,
      leaseSha256: createHash("sha256").update(snapshot.bytes).digest("hex"),
      leaseBase64: snapshot.bytes.toString("base64") }))) });
  expect(receipt.candidates).toHaveLength(snapshots.length);
  for (const snapshot of snapshots) {
    const candidate = receipt.candidates.find((value: { hostId: string }) => value.hostId === snapshot.hostId);
    const archived = Buffer.from(candidate.leaseBase64, "base64");
    expect(archived).toEqual(snapshot.bytes);
    expect(createHash("sha256").update(archived).digest("hex")).toBe(candidate.leaseSha256);
  }
  return { file, bytes };
};
const ARCHIVE_FSYNC_BARRIERS = process.platform === "win32"
  ? ["file"] as const
  : ["file", "directory"] as const;
afterEach(() => {
  vi.restoreAllMocks();
  for (const mesh of stores.splice(0)) mesh.closeState();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("expired host lease archive", () => {
  it("reaps an already-compacted orphan once, preserving the exact detached bytes", async () => {
    const { root, mesh, now, lease } = setup();
    const old = lease("orphan"); writeHostLease(root, old);
    // Unknown fields and invalid UTF-8 in an ignored string survive as bytes, not reserialized JSON.
    const bytes = Buffer.concat([Buffer.from(` { "format": 1, ${JSON.stringify(old).slice(1, -1)}, "extra": "`),
      Buffer.from([0xff]), Buffer.from('" }\n')]);
    fs.writeFileSync(hostLeasePath(root, old.id), bytes);
    expect(await reapDeadHostRecords(mesh, identity, { ownHostId: identity.id, now })).toBe(1);
    expect(fs.existsSync(hostLeasePath(root, old.id))).toBe(false);
    const batch = preparedArchive(root, now, [{ hostId: old.id, bytes }]);
    const writes = vi.spyOn(fs.promises, "rename");
    expect(await reapDeadHostRecords(mesh, identity, { ownHostId: identity.id, now })).toBe(0);
    expect(writes.mock.calls.filter(([, to]) => path.dirname(String(to)) === path.dirname(batch.file))).toEqual([]);
    expect(fs.readdirSync(path.dirname(batch.file))).toEqual([path.basename(batch.file)]);
    expect(fs.readFileSync(batch.file)).toEqual(batch.bytes);
  });

  it("retains own/live/boundary leases and an expired file protected by a live state row", async () => {
    const { root, mesh, now, lease } = setup();
    for (const value of [lease("old"), lease(identity.id), lease("boundary", now - DEAD_HOST_RECORDS_MS),
      lease("live", now + 15_000), lease("state-live")]) writeHostLease(root, value);
    const value = lease("state-live");
    const row: MeshStateEntry = { key: hostKey(value.id), version: 1, updatedAt: now, updatedBy: identity,
      value: { format: 1, ...value, identity: { ...identity, id: value.identityId }, expiresAt: now + 15_000 } };
    fs.writeFileSync(path.join(root, "state.json"), JSON.stringify({ format: 1, entries: { [row.key]: row } }));
    const snapshots = ["old", "state-live"].map(hostId => ({ hostId, bytes: fs.readFileSync(hostLeasePath(root, hostId)) }));
    expect(await reapDeadHostRecords(mesh, identity, { ownHostId: identity.id, now })).toBe(1);
    expect(readHostLeaseCurrent(root, "old")).toBeUndefined();
    for (const id of [identity.id, "boundary", "live", "state-live"]) expect(readHostLeaseCurrent(root, id)).toBeDefined();
    expect(mesh.get(row.key, { fresh: true })).toBeDefined();
    // Preparation records candidates, not removal: even the state-protected lease can appear.
    preparedArchive(root, now, snapshots);
  });

  it("keeps a lease renewed after selection and leaves only its unconfirmed old snapshot", async () => {
    const { root, mesh, now, lease } = setup();
    const old = lease("returning"); writeHostLease(root, old);
    const bytes = fs.readFileSync(hostLeasePath(root, old.id));
    const renewed = { ...old, updatedAt: now, expiresAt: now + 15_000 };
    const original = mesh.writeBatch.bind(mesh);
    vi.spyOn(mesh, "writeBatch").mockImplementationOnce(input => {
      // Independent/unfenced historical writer: deliberately bypass the held target gate.
      const file = hostLeasePath(root, old.id), temporary = `${file}.independent.tmp`;
      fs.writeFileSync(temporary, JSON.stringify({ format: 1, ...renewed })); fs.renameSync(temporary, file);
      return original(input);
    });
    expect(await reapDeadHostRecords(mesh, identity, { ownHostId: identity.id, now })).toBe(0);
    expect(readHostLeaseCurrent(root, old.id)).toEqual(renewed);
    preparedArchive(root, now, [{ hostId: old.id, bytes }]);
  });

  it("rechecks the detached file, not a matching incarnation read before its rename", async () => {
    const { root, mesh, now, lease } = setup();
    const old = lease("detach-race"), renewed = { ...old, updatedAt: now, expiresAt: now + 15_000 };
    writeHostLease(root, old);
    // Exact-version APIs consume an observed parser snapshot, not a newly constructed object
    // (whose JSON property order is not the parser's canonical order).
    const expected = readHostLeaseCurrent(root, old.id)!;
    let raced = false;
    await mesh.leaseCustody(hostLeasePath(root, old.id), () => {
      const rename = fs.renameSync;
      vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
        if (String(from) === hostLeasePath(root, old.id) && path.basename(String(to)).startsWith(".unlease-")) {
          raced = true;
          fs.writeFileSync(from, JSON.stringify({ format: 1, ...renewed }));
        }
        return rename(from, to);
      });
      expect(removeHostLeaseVersionUnderCustody(root, expected, now)).toBe(false);
    });
    expect(raced).toBe(true);
    expect(readHostLeaseCurrent(root, old.id)).toEqual(renewed);
    expect(fs.existsSync(path.join(root, "host-lease-archive"))).toBe(false);
  });

  it("archives a small orphan backlog in one batch and one sweep", async () => {
    const { root, mesh, now, lease } = setup();
    const stale = Array.from({ length: 5 }, (_, index) => lease(`backlog:${index}`));
    for (const value of stale) writeHostLease(root, value);
    const snapshots = stale.map(value => ({ hostId: value.id, bytes: fs.readFileSync(hostLeasePath(root, value.id)) }));
    expect(await reapDeadHostRecords(mesh, identity, { ownHostId: identity.id, now })).toBe(stale.length);
    expect(stale.filter(value => fs.existsSync(hostLeasePath(root, value.id)))).toEqual([]);
    preparedArchive(root, now, snapshots);
  });

  it.each(ARCHIVE_FSYNC_BARRIERS)("restores the exact lease when the archive %s fsync fails", async barrier => {
    const { root, mesh, now, lease } = setup();
    const old = lease(`failure:${barrier}`); writeHostLease(root, old);
    const bytes = fs.readFileSync(hostLeasePath(root, old.id));
    const expected = readHostLeaseCurrent(root, old.id)!;
    const sync = fs.fsyncSync;
    await mesh.leaseCustody(hostLeasePath(root, old.id), () => {
      vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
        if (fs.fstatSync(fd).isDirectory() === (barrier === "directory")) throw new Error("injected archive barrier failure");
        sync(fd);
      });
      expect(removeHostLeaseVersionUnderCustody(root, expected, now)).toBe(false);
      expect(fs.fsyncSync).toHaveBeenCalled();
      vi.mocked(fs.fsyncSync).mockRestore();
    });
    expect(fs.readFileSync(hostLeasePath(root, old.id))).toEqual(bytes);
    expect(fs.readdirSync(path.dirname(hostLeasePath(root, old.id))).filter(name => name.startsWith(".unlease-"))).toEqual([]);
  });

  it("never unlinks an unarchived aside if its canonical name cannot be restored", async () => {
    const { root, mesh, now, lease } = setup();
    const old = lease("restore-failure"); writeHostLease(root, old);
    const bytes = fs.readFileSync(hostLeasePath(root, old.id));
    const expected = readHostLeaseCurrent(root, old.id)!;
    await mesh.leaseCustody(hostLeasePath(root, old.id), () => {
      vi.spyOn(fs, "fsyncSync").mockImplementation(() => { throw new Error("archive unavailable"); });
      vi.spyOn(fs, "linkSync").mockImplementation(() => { throw Object.assign(new Error("restore denied"), { code: "EACCES" }); });
      expect(removeHostLeaseVersionUnderCustody(root, expected, now)).toBe(false);
      expect(fs.fsyncSync).toHaveBeenCalled(); expect(fs.linkSync).toHaveBeenCalled();
      vi.mocked(fs.fsyncSync).mockRestore(); vi.mocked(fs.linkSync).mockRestore();
    });
    const directory = path.dirname(hostLeasePath(root, old.id));
    const aside = fs.readdirSync(directory).find(name => name.startsWith(".unlease-"));
    expect(aside).toBeDefined(); expect(fs.readFileSync(path.join(directory, aside!))).toEqual(bytes);
  });
});

it("standalone removal keeps format-1 receipts and platform namespace barriers", async () => {
  const { root, mesh, now, lease } = setup();
  const old = lease("windows-archive"); writeHostLease(root, old);
  const bytes = fs.readFileSync(hostLeasePath(root, old.id));
  const expected = readHostLeaseCurrent(root, old.id)!;
  const syncedDirectories: boolean[] = [];
  const sync = fs.fsyncSync;
  await mesh.leaseCustody(hostLeasePath(root, old.id), () => {
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      syncedDirectories.push(fs.fstatSync(fd).isDirectory());
      sync(fd);
    });
    expect(removeHostLeaseVersionUnderCustody(root, expected, now)).toBe(true);
    vi.mocked(fs.fsyncSync).mockRestore();
  });
  expect(syncedDirectories).toContain(false);
  if (process.platform === "win32") expect(syncedDirectories).not.toContain(true);
  else expect(syncedDirectories).toContain(true);
  expect(readHostLeaseCurrent(root, old.id)).toBeUndefined();
  const { file, digest } = archive(root, old.id, bytes);
  const receipt = JSON.parse(fs.readFileSync(file, "utf8"));
  expect(receipt).toEqual({ format: 1, hostId: old.id, archivedAt: now,
    leaseSha256: digest, leaseBase64: bytes.toString("base64") });
});
