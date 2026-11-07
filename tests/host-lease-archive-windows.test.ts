import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { MeshStore } from "../src/mesh/store.js";
import { DEAD_HOST_RECORDS_MS, reapDeadHostRecords } from "../src/topology/host-reaper.js";
import { hostLeaseArchivePath, hostLeasePath, readHostLeaseCurrent, removeHostLeaseVersionUnderCustody,
  writeHostLease, type FabricHostLease } from "../src/topology/host-leases.js";

// Data safety / Windows regression: use real files and the actual atomic writer's Windows
// branch, even on POSIX CI. This proves file-sync ordering, not NTFS power-loss durability.
const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
const roots: string[] = [], stores: MeshStore[] = [];
const identity = { id: "session:observer", name: "observer", kind: "main" as const };
beforeEach(() => { Object.defineProperty(process, "platform", { ...platform, value: "win32" }); });
afterEach(() => {
  vi.restoreAllMocks();
  Object.defineProperty(process, "platform", platform);
  for (const mesh of stores.splice(0)) mesh.closeState();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const setup = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "host-lease-archive-windows-")); roots.push(root);
  const mesh = new MeshStore(root, 256 * 1024, 1000); stores.push(mesh);
  const now = Date.now(), expiresAt = now - DEAD_HOST_RECORDS_MS - 1;
  const old: FabricHostLease = { id: "session:old", rootId: "session:old", identityId: "session:old",
    incarnationToken: "old-token", updatedAt: expiresAt - 1000, expiresAt, startedAt: 1 };
  writeHostLease(root, old);
  // Unknown metadata, whitespace and non-UTF-8 bytes must survive without JSON reserialization.
  const bytes = Buffer.concat([Buffer.from(` { "format": 1, ${JSON.stringify(old).slice(1, -1)}, "extra": "`),
    Buffer.from([0xff]), Buffer.from('" }\n')]);
  const file = hostLeasePath(root, old.id);
  fs.writeFileSync(file, bytes);
  const expected = readHostLeaseCurrent(root, old.id)!;
  const digest = createHash("sha256").update(bytes).digest("hex");
  const archive = hostLeaseArchivePath(root, old.id, digest);
  return { root, mesh, now, old, bytes, file, expected, digest, archive };
};

const preparedArchive = (s: ReturnType<typeof setup>) => {
  const directory = path.join(s.root, "host-lease-archive", "batches");
  expect(fs.readdirSync(path.dirname(directory))).toEqual(["batches"]);
  const names = fs.readdirSync(directory);
  expect(names).toHaveLength(1);
  const file = path.join(directory, names[0]!);
  const bytes = fs.readFileSync(file);
  const receipt = JSON.parse(bytes.toString("utf8"));
  expect(receipt).toEqual({ format: 2, status: "prepared", removalConfirmed: false, preparedAt: s.now,
    candidates: [{ hostId: s.old.id, leaseSha256: s.digest, leaseBase64: s.bytes.toString("base64") }] });
  const archived = Buffer.from(receipt.candidates[0].leaseBase64, "base64");
  expect(archived).toEqual(s.bytes);
  expect(createHash("sha256").update(archived).digest("hex")).toBe(s.digest);
  return { file, bytes };
};

describe("Windows exact-version host lease archival", () => {
  it("reaps a file-only orphan after asynchronously publishing and syncing its prepared batch", async () => {
    const s = setup();
    const events: string[] = [];
    const directory = path.join(s.root, "host-lease-archive", "batches");
    let published: { file: string; bytes: Buffer } | undefined;
    const sync = fs.fsyncSync, remove = fs.rmSync;
    const open = fs.promises.open.bind(fs.promises), rename = fs.promises.rename.bind(fs.promises);
    vi.spyOn(fs.promises, "open").mockImplementation(async (...args) => {
      const handle = await open(...args);
      const syncHandle = handle.sync.bind(handle);
      if (path.dirname(String(args[0])) === directory) expect(args[1]).toBe("r+");
      vi.spyOn(handle, "sync").mockImplementation(async () => {
        // Forward the real barrier; neither async nor sync directory fsync is supported on Windows.
        expect((await handle.stat()).isFile()).toBe(true);
        await syncHandle();
        if (String(args[0]) === published?.file) events.push("archive-file-sync");
      });
      return handle;
    });
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      expect(fs.fstatSync(fd).isFile()).toBe(true);
      sync(fd);
    });
    vi.spyOn(fs.promises, "rename").mockImplementation(async (from, to) => {
      await rename(from, to);
      if (path.dirname(String(to)) === directory) {
        published = { file: String(to), bytes: fs.readFileSync(to) };
        events.push("archive-published");
      }
    });
    vi.spyOn(fs, "rmSync").mockImplementation((file, options) => {
      if (path.basename(String(file)).startsWith(".unlease-")) {
        expect(preparedArchive(s)).toEqual(published);
        expect(events).toEqual(["archive-published", "archive-file-sync"]);
        events.push("lease-unlinked");
      }
      return remove(file, options);
    });
    expect(process.platform).toBe("win32");
    expect(await reapDeadHostRecords(s.mesh, identity, { ownHostId: identity.id, now: s.now })).toBe(1);
    expect(fs.existsSync(s.file)).toBe(false);
    expect(preparedArchive(s)).toEqual(published);
    expect(events).toEqual(["archive-published", "archive-file-sync", "lease-unlinked"]);
    expect(await reapDeadHostRecords(s.mesh, identity, { ownHostId: identity.id, now: s.now })).toBe(0);
    expect(preparedArchive(s)).toEqual(published);
    expect(events).toEqual(["archive-published", "archive-file-sync", "lease-unlinked"]);
  });

  it.each(["write", "file-sync", "rename"] as const)("restores the exact canonical lease on archive %s failure", async failure => {
    const s = setup();
    let failed = false;
    await s.mesh.leaseCustody(s.file, () => {
      const rename = fs.renameSync;
      const error = () => { failed = true; throw Object.assign(new Error("archive I/O failure"), { code: "EIO" }); };
      if (failure === "write") vi.spyOn(fs, "writeFileSync").mockImplementationOnce(error);
      if (failure === "file-sync") vi.spyOn(fs, "fsyncSync").mockImplementationOnce(error);
      if (failure === "rename") vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
        if (String(to) === s.archive) return error();
        return rename(from, to);
      });
      try { expect(removeHostLeaseVersionUnderCustody(s.root, s.expected, s.now)).toBe(false); }
      finally { vi.restoreAllMocks(); }
    });
    expect(failed).toBe(true); // The real archive path reached the injected failure.
    expect(fs.readFileSync(s.file)).toEqual(s.bytes);
    expect(readHostLeaseCurrent(s.root, s.old.id)).toEqual(s.expected);
    expect(fs.existsSync(s.archive)).toBe(false);
    expect(fs.readdirSync(path.dirname(s.file)).filter(name => name.startsWith(".unlease-"))).toEqual([]);
  });

  it.each(["renewal", "replacement"] as const)("the real reaper preserves a %s after candidate selection", async change => {
    const s = setup();
    const current = change === "renewal"
      ? { ...s.old, updatedAt: s.now, expiresAt: s.now + 15_000 }
      : { ...s.old, incarnationToken: "successor-token" };
    const writeBatch = s.mesh.writeBatch.bind(s.mesh);
    vi.spyOn(s.mesh, "writeBatch").mockImplementationOnce(input => {
      // Independent historical writer deliberately bypassing target custody.
      writeHostLease(s.root, current);
      return writeBatch(input);
    });
    expect(await reapDeadHostRecords(s.mesh, identity, { ownHostId: identity.id, now: s.now })).toBe(0);
    expect(readHostLeaseCurrent(s.root, s.old.id)).toEqual(current);
    // The old candidate was prepared before selection lost authority; it never confirms removal.
    preparedArchive(s);
  });

  it.each(["renewal", "replacement"] as const)("rechecks a %s that races the detach, not just the initial read", async change => {
    const s = setup();
    const current = change === "renewal"
      ? { ...s.old, updatedAt: s.now, expiresAt: s.now + 15_000 }
      : { ...s.old, incarnationToken: "successor-token" };
    const currentBytes = Buffer.from(JSON.stringify({ format: 1, ...current }));
    let raced = false;
    await s.mesh.leaseCustody(s.file, () => {
      const rename = fs.renameSync;
      const spy = vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
        if (String(from) === s.file && path.basename(String(to)).startsWith(".unlease-")) {
          raced = true;
          fs.writeFileSync(from, currentBytes);
        }
        return rename(from, to);
      });
      try { expect(removeHostLeaseVersionUnderCustody(s.root, s.expected, s.now)).toBe(false); }
      finally { spy.mockRestore(); }
    });
    expect(raced).toBe(true);
    expect(fs.readFileSync(s.file)).toEqual(currentBytes);
    expect(fs.existsSync(path.join(s.root, "host-lease-archive"))).toBe(false);
    expect(fs.readdirSync(path.dirname(s.file)).filter(name => name.startsWith(".unlease-"))).toEqual([]);
  });
});
