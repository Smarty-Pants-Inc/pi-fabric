import fs from "node:fs";
import { spawnSync } from "node:child_process";
import { MeshStore } from "../src/mesh/store.js";
import { ownProcessIncarnation } from "../src/core/atomic-write.js";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { hostLeasesStamp, readHostLease, readHostLeases, readHostLeaseCurrent, readHostLeaseSnapshot, writeHostLease, writeHostLeaseIfCurrent, hostLeasePath, hostLeasePublishLockPath, acquireHostLeasePublishLock, HostLeasePublishLockBusyError, prepareHostLeasePublishLock, waitForHostLeasePublication, removeHostLease, removeHostLeaseIf, type FabricHostLease } from "../src/topology/host-leases.js";

// Windows fails an open with EPERM while the owner's heartbeat renames a new lease file over the
// old one. A live host then looked leaseless, and the failed read stayed cached until its next
// renewal: participants vanished from listings (pi-fabric main Test on windows-latest).
describe("host lease files on a transient read failure", () => {
  const roots: string[] = [];
  afterEach(() => {
    vi.restoreAllMocks();
    for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  });

  const setup = () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-leases-"));
    roots.push(root);
    const lease = (updatedAt: number): FabricHostLease => ({
      id: "host:a", rootId: "session:a", identityId: "session:a", updatedAt, expiresAt: updatedAt + 60_000,
    });
    const failReads = (times: number) => {
      const read = fs.readFileSync.bind(fs);
      let left = times;
      return vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...rest: unknown[]) => {
        if (left > 0 && String(file).includes("host-leases")) {
          left--;
          throw Object.assign(new Error("EPERM: operation not permitted, open"), { code: "EPERM" });
        }
        return (read as (...args: unknown[]) => unknown)(file, ...rest);
      }) as typeof fs.readFileSync);
    };
    return { root, lease, failReads };
  };

  it("creates only before ownership, then renews the exact incarnation without temp leaks", () => {
    const { root, lease } = setup();
    const original = { ...lease(1_000), startedAt: 1 };
    expect(writeHostLeaseIfCurrent(root, original, true)).toBe(true);
    expect(writeHostLeaseIfCurrent(root, { ...original, updatedAt: 2_000 })).toBe(true);
    expect(readHostLeaseCurrent(root, original.id)?.updatedAt).toBe(2_000);
    fs.unlinkSync(hostLeasePath(root, original.id));
    expect(writeHostLeaseIfCurrent(root, original)).toBe(false);
    expect(fs.existsSync(hostLeasePath(root, original.id))).toBe(false);
    expect(fs.readdirSync(path.join(root, "host-leases"))).toEqual([]);
  });

  it("an owned lease unlinked after opening its current inode is not recreated", () => {
    const { root, lease } = setup();
    const original = { ...lease(1_000), startedAt: 1 };
    writeHostLease(root, original);
    const file = hostLeasePath(root, original.id), read = fs.readFileSync.bind(fs);
    let removed = false;
    vi.spyOn(fs, "readFileSync").mockImplementation(((target: fs.PathOrFileDescriptor, ...args: unknown[]) => {
      const text = (read as (...args: unknown[]) => unknown)(target, ...args);
      if (typeof target === "number" && !removed) { removed = true; fs.unlinkSync(file); }
      return text;
    }) as typeof fs.readFileSync);
    expect(writeHostLeaseIfCurrent(root, original)).toBe(false);
    expect(removed).toBe(true); expect(fs.existsSync(file)).toBe(false);
    expect(fs.readdirSync(path.dirname(file))).toEqual([]);
  });

  it("a successor at the final check/rename window waits for publication, then survives every stale renewal", async () => {
    const { root, lease } = setup();
    const original = { ...lease(1_000), startedAt: 1 }, successor = { ...original, startedAt: 2 };
    writeHostLease(root, original);
    const file = hostLeasePath(root, original.id), rename = fs.renameSync.bind(fs);
    const abort = new AbortController();
    let publication: Promise<void> | undefined;
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (String(to) === file && String(from).includes(".renew-") && !publication) {
        // Exactly the old P1-A syscall gap. The successor CANNOT publish until
        // the stale holder leaves its exclusive check+rename critical section.
        expect(() => writeHostLease(root, successor)).toThrow(HostLeasePublishLockBusyError);
        publication = waitForHostLeasePublication(root, original.id, abort.signal).then(() => writeHostLease(root, successor));
      }
      rename(from, to);
    });
    try {
      expect(writeHostLeaseIfCurrent(root, original)).toBe(true);
      expect(publication).toBeDefined();
      await publication;
      expect(writeHostLeaseIfCurrent(root, original)).toBe(false);
      expect(readHostLeaseCurrent(root, original.id)).toEqual(successor);
      expect(fs.readdirSync(path.dirname(file))).toEqual([path.basename(file)]);
    } finally { abort.abort(); }
  });

  it("all publishers/removers share wx exclusion and a held renewal is skipped, not superseded", () => {
    const { root, lease } = setup(), original = { ...lease(1_000), startedAt: 1 };
    writeHostLease(root, original);
    const release = acquireHostLeasePublishLock(root, original.id);
    try {
      const owner = JSON.parse(fs.readFileSync(hostLeasePublishLockPath(root, original.id), "utf8"));
      expect(owner).toMatchObject({ pid: process.pid, token: expect.any(String) });
      expect(owner).toHaveProperty("startTime");
      expect(writeHostLeaseIfCurrent(root, original)).toBe("skipped");
      expect(() => writeHostLease(root, { ...original, startedAt: 2 })).toThrow(HostLeasePublishLockBusyError);
      expect(() => removeHostLease(root, original.id)).toThrow(HostLeasePublishLockBusyError);
      expect(() => removeHostLeaseIf(root, original.id, () => true)).toThrow(HostLeasePublishLockBusyError);
      // It is per LEASE, not global/mesh exclusion.
      writeHostLease(root, { ...original, id: "host:b" });
      expect(readHostLeaseCurrent(root, original.id)).toEqual(original);
    } finally { release(); }
  });

  it("reclaims a just-crashed holder by pid death without any age threshold", async () => {
    const { root, lease } = setup(), original = { ...lease(1_000), startedAt: 1 };
    writeHostLease(root, original);
    const lock = hostLeasePublishLockPath(root, original.id);
    const crash = spawnSync(process.execPath, ["-e", `require('node:fs').writeFileSync(process.argv[1], JSON.stringify({pid:process.pid,startTime:null,token:'crashed-holder'}), {flag:'wx'});`, lock]);
    expect(crash.status).toBe(0);
    expect(Date.now() - fs.statSync(lock).mtimeMs).toBeLessThan(10_000);
    await prepareHostLeasePublishLock(new MeshStore(root, 65_536, 100), original.id);
    expect(fs.existsSync(lock)).toBe(false);
    expect(writeHostLeaseIfCurrent(root, original)).toBe(true);
  });

  it("never reclaims a live holder by age, including UNKNOWN native start identity", async () => {
    const { root, lease } = setup(), original = { ...lease(1_000), startedAt: 1 };
    writeHostLease(root, original);
    const lock = hostLeasePublishLockPath(root, original.id);
    fs.writeFileSync(lock, JSON.stringify({ pid: process.pid, startTime: null, token: "live-old-holder" }), { flag: "wx" });
    fs.utimesSync(lock, 1, 1);
    await prepareHostLeasePublishLock(new MeshStore(root, 65_536, 100), original.id);
    expect(fs.existsSync(lock)).toBe(true);
    expect(writeHostLeaseIfCurrent(root, original)).toBe("skipped");
    fs.unlinkSync(lock);
  });

  it.skipIf(process.platform !== "linux")("reclaims PID reuse only when native start identity differs", async () => {
    const { root, lease } = setup(), original = { ...lease(1_000), startedAt: 1 };
    writeHostLease(root, original);
    const lock = hostLeasePublishLockPath(root, original.id), start = await ownProcessIncarnation();
    fs.writeFileSync(lock, JSON.stringify({ pid: process.pid, startTime: start, token: "same-start" }), { flag: "wx" });
    fs.utimesSync(lock, 1, 1);
    await prepareHostLeasePublishLock(new MeshStore(root, 65_536, 100), original.id);
    expect(fs.existsSync(lock)).toBe(true);
    fs.writeFileSync(lock, JSON.stringify({ pid: process.pid, startTime: `${BigInt(start!) + 1n}`, token: "reused-pid" }));
    await prepareHostLeasePublishLock(new MeshStore(root, 65_536, 100), original.id);
    expect(fs.existsSync(lock)).toBe(false);
  });

  it("racing dead-holder reclaimers never unlink the next holder's token", async () => {
    const { root, lease } = setup(), original = { ...lease(1_000), startedAt: 1 };
    writeHostLease(root, original);
    const lock = hostLeasePublishLockPath(root, original.id);
    const crash = spawnSync(process.execPath, ["-e", `require('node:fs').writeFileSync(process.argv[1], JSON.stringify({pid:process.pid,startTime:null,token:'dead-before-both-reads'}), {flag:'wx'});`, lock]);
    expect(crash.status).toBe(0);
    const mesh = new MeshStore(root, 65_536, 100), custody = mesh.custody.bind(mesh);
    let release: (() => void) | undefined, successorToken = "";
    vi.spyOn(mesh, "custody").mockImplementation(<T>(operation: () => T) => custody(() => {
      const result = operation();
      if (!release) {
        release = acquireHostLeasePublishLock(root, original.id);
        successorToken = JSON.parse(fs.readFileSync(lock, "utf8")).token;
      }
      return result;
    }));
    try {
      await Promise.all([prepareHostLeasePublishLock(mesh, original.id), prepareHostLeasePublishLock(mesh, original.id)]);
      expect(JSON.parse(fs.readFileSync(lock, "utf8")).token).toBe(successorToken);
    } finally { release?.(); }
  });

  it("a delayed release leaves a replacement publish-lock token intact", () => {
    const { root, lease } = setup(), original = lease(1_000);
    writeHostLease(root, original);
    const release = acquireHostLeasePublishLock(root, original.id), lock = hostLeasePublishLockPath(root, original.id);
    fs.writeFileSync(lock, JSON.stringify({ pid: process.pid, startTime: null, token: "replacement-token" }));
    release();
    expect(JSON.parse(fs.readFileSync(lock, "utf8")).token).toBe("replacement-token");
    fs.unlinkSync(lock);
  });

  it("reads back after rename and never restores an out-of-protocol successor", () => {
    const { root, lease } = setup(), original = { ...lease(1_000), startedAt: 1 }, successor = { ...original, startedAt: 2 };
    writeHostLease(root, original);
    const file = hostLeasePath(root, original.id), rename = fs.renameSync.bind(fs);
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      rename(from, to);
      if (String(to) === file) fs.writeFileSync(file, JSON.stringify({ format: 1, ...successor }));
    });
    expect(writeHostLeaseIfCurrent(root, original)).toBe(false);
    expect(readHostLeaseCurrent(root, original.id)).toEqual(successor);
  });

  it("round-trips the optional reload marker without adding it to ordinary leases", () => {
    const { root, lease } = setup();
    const reload = { ...lease(1_000), reloadUntil: lease(1_000).expiresAt };
    writeHostLease(root, reload);
    expect(readHostLeaseCurrent(root, "host:a")).toEqual(reload);
    expect(readHostLease(root, "host:a")).toEqual(reload);
    expect(readHostLeases(root).get("host:a")).toEqual(reload);
    writeHostLease(root, lease(2_000));
    expect(readHostLeaseCurrent(root, "host:a")).toEqual(lease(2_000));
  });

  it.each(["invalid", null])("rejects a malformed reload marker (%s)", (reloadUntil) => {
    const { root, lease } = setup();
    writeHostLease(root, lease(1_000));
    const file = path.join(root, "host-leases", fs.readdirSync(path.join(root, "host-leases"))[0]!);
    fs.writeFileSync(file, JSON.stringify({ format: 1, ...lease(1_000), reloadUntil }));
    expect(readHostLeaseCurrent(root, "host:a")).toBeUndefined();
    expect(readHostLease(root, "host:a")).toBeUndefined();
    expect(readHostLeases(root).size).toBe(0);
  });

  it.each(["single", "all"])("reparses equal-size atomic replacements with preserved mtime through %s", (reader) => {
    const { root, lease } = setup();
    writeHostLease(root, lease(1_000));
    const dir = path.join(root, "host-leases");
    const file = path.join(dir, fs.readdirSync(dir)[0]!);
    // Use an exactly representable timestamp, independent of filesystem timestamp precision.
    fs.utimesSync(file, 1_000, 1_000);
    const before = fs.statSync(file);
    const read = () => reader === "single"
      ? readHostLease(root, "host:a")
      : readHostLeases(root).get("host:a");
    expect(read()).toEqual(lease(1_000));

    const replace = (text: string) => {
      expect(Buffer.byteLength(text)).toBe(before.size);
      const temporary = file + ".tmp";
      fs.writeFileSync(temporary, text);
      fs.utimesSync(temporary, before.atime, before.mtime);
      // A separate writer's on-disk operation: no writeHostLease or cache invalidation.
      fs.renameSync(temporary, file);
      expect(fs.statSync(file).mtimeMs).toBe(before.mtimeMs);
      expect(fs.statSync(file).size).toBe(before.size);
    };
    const renewed = JSON.stringify({ format: 1, ...lease(2_000) });
    replace(renewed);
    const reads = vi.spyOn(fs, "readFileSync");
    expect(read()).toEqual(lease(2_000));
    expect(readHostLease(root, "host:a")).toEqual(lease(2_000));
    expect(readHostLeases(root).get("host:a")).toEqual(lease(2_000));
    expect(reads.mock.calls.filter(([name]) => name === file)).toHaveLength(1);

    // Changed metadata never bypasses the parser or retains a formerly valid answer.
    replace(renewed.replace('"format":1', '"format":2'));
    expect(read()).toBeUndefined();
    expect(readHostLease(root, "host:a")).toBeUndefined();
    expect(readHostLeases(root).size).toBe(0);
    expect(reads.mock.calls.filter(([name]) => name === file)).toHaveLength(2);
  });

  it("uses ctime when filesystem device and inode values are unavailable", () => {
    const { root, lease } = setup();
    writeHostLease(root, lease(1_000));
    const dir = path.join(root, "host-leases");
    const file = path.join(dir, fs.readdirSync(dir)[0]!);
    const stat = fs.statSync.bind(fs);
    const before = stat(file, { bigint: true });
    let ctimeNs = 1_000_000_000n;
    vi.spyOn(fs, "statSync").mockImplementation(((name: fs.PathLike, ...rest: unknown[]) => {
      const result = (stat as (...args: unknown[]) => fs.BigIntStats)(name, ...rest);
      return name === file
        ? Object.assign(result, { dev: 0n, ino: 0n, mtimeNs: before.mtimeNs, ctimeNs })
        : result;
    }) as typeof fs.statSync);
    expect(readHostLease(root, "host:a")).toEqual(lease(1_000));
    writeHostLease(root, lease(2_000));
    ctimeNs = 2_000_000_000n;
    expect(readHostLeases(root).get("host:a")).toEqual(lease(2_000));
  });

  it.each(["single", "all"])("preserves 64-bit Windows file IDs through %s reads and stamps", (reader) => {
    const { root, lease } = setup();
    writeHostLease(root, lease(1_000));
    const file = path.join(root, "host-leases", fs.readdirSync(path.join(root, "host-leases"))[0]!);
    const stat = fs.statSync.bind(fs);
    const before = stat(file);
    const beforeBig = stat(file, { bigint: true });
    let ino = 2n ** 54n;
    expect(Number(ino + 1n)).toBe(Number(ino));
    vi.spyOn(fs, "statSync").mockImplementation(((name: fs.PathLike, options?: fs.StatOptions) => {
      const current = stat(name, options);
      if (String(name) !== file) return current;
      return Object.assign(current!, options?.bigint ? { ...beforeBig, ino } : { ...before, ino: Number(ino) });
    }) as typeof fs.statSync);
    const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
    try {
      Object.defineProperty(process, "platform", { ...platform, value: "win32" });
      const read = () => reader === "single" ? readHostLease(root, "host:a") : readHostLeases(root).get("host:a");
      const reads = vi.spyOn(fs, "readFileSync");
      expect(read()).toEqual(lease(1_000));
      const stamp = hostLeasesStamp(root);
      writeHostLease(root, lease(2_000));
      ino += 1n; // Same numeric ID, different exact file identity; size and timestamps stay fixed.
      expect(read()).toEqual(lease(2_000));
      expect(hostLeasesStamp(root)).not.toBe(stamp);
      expect(readHostLease(root, "host:a")).toEqual(lease(2_000));
      expect(readHostLeases(root).get("host:a")).toEqual(lease(2_000));
      expect(reads.mock.calls.filter(([name]) => name === file)).toHaveLength(2);
    } finally {
      Object.defineProperty(process, "platform", platform);
    }
  });

  it("preserves sub-millisecond change times when file IDs are unavailable", () => {
    const { root, lease } = setup();
    writeHostLease(root, lease(1_000));
    const file = path.join(root, "host-leases", fs.readdirSync(path.join(root, "host-leases"))[0]!);
    const stat = fs.statSync.bind(fs);
    const before = stat(file);
    const beforeBig = stat(file, { bigint: true });
    let ctimeNs = 1_800_000_000_000_000_000n;
    const ctimeMs = Number(ctimeNs) / 1_000_000;
    expect(Number(ctimeNs + 100n) / 1_000_000).toBe(ctimeMs);
    vi.spyOn(fs, "statSync").mockImplementation(((name: fs.PathLike, options?: fs.StatOptions) => {
      const current = stat(name, options);
      if (String(name) !== file) return current;
      return Object.assign(current!, options?.bigint
        ? { ...beforeBig, dev: 0n, ino: 0n, ctimeNs }
        : { ...before, dev: 0, ino: 0, ctimeMs });
    }) as typeof fs.statSync);
    expect(readHostLease(root, "host:a")).toEqual(lease(1_000));
    writeHostLease(root, lease(2_000));
    ctimeNs += 100n;
    expect(readHostLeases(root).get("host:a")).toEqual(lease(2_000));
  });

  it("exposes a numeric snapshot timestamp for recovery comparisons", () => {
    const { root, lease } = setup();
    writeHostLease(root, lease(1_000));
    const dir = path.join(root, "host-leases");
    const file = path.join(dir, fs.readdirSync(dir)[0]!);
    fs.utimesSync(file, 1_800_000_000, 1_800_000_000);
    const stat = fs.statSync(file, { bigint: true });
    expect(typeof stat.mtimeMs).toBe("bigint");

    const snapshot = readHostLeaseSnapshot(root, "host:a")!;
    expect(snapshot.lease).toEqual(lease(1_000));
    expect(typeof snapshot.mtimeMs).toBe("number");
    expect(snapshot.mtimeMs).toBe(Number(stat.mtimeMs));
    // A file only advances recovery when newer than the numeric owner.updatedAt.
    expect(snapshot.mtimeMs > 1_799_999_999_999).toBe(true);
    expect(snapshot.mtimeMs > 1_800_000_000_000).toBe(false);
    expect(snapshot.mtimeMs > 1_800_000_000_001).toBe(false);
    expect(snapshot.mtimeMs - 1_799_999_999_999).toBe(1);
  });

  it("shares a valid unchanged cached lease without rereading", () => {
    const { root, lease } = setup();
    writeHostLease(root, lease(1_000));
    const file = path.join(root, "host-leases", fs.readdirSync(path.join(root, "host-leases"))[0]!);
    const reads = vi.spyOn(fs, "readFileSync");
    expect(readHostLeases(root).get("host:a")).toEqual(lease(1_000));
    expect(readHostLease(root, "host:a")).toEqual(lease(1_000));
    expect(readHostLeases(root).get("host:a")).toEqual(lease(1_000));
    expect(reads.mock.calls.filter(([name]) => name === file)).toHaveLength(1);
  });

  it("returns no lease for missing or removed files and directories", () => {
    const { root, lease } = setup();
    expect(readHostLease(root, "host:a")).toBeUndefined();
    expect(readHostLeases(root).size).toBe(0);
    writeHostLease(root, lease(1_000));
    expect(readHostLease(root, "host:a")).toEqual(lease(1_000));
    const dir = path.join(root, "host-leases");
    const file = path.join(dir, fs.readdirSync(dir)[0]!);
    fs.rmSync(file);
    expect(readHostLease(root, "host:a")).toBeUndefined();
    expect(readHostLeases(root).size).toBe(0);
    writeHostLease(root, lease(2_000));
    expect(readHostLeases(root).get("host:a")).toEqual(lease(2_000));
    fs.rmSync(dir, { recursive: true });
    expect(readHostLeases(root).size).toBe(0);
    expect(readHostLease(root, "host:a")).toBeUndefined();
  });

  it("retries a transient EPERM and reads the lease", () => {
    const { root, lease, failReads } = setup();
    writeHostLease(root, lease(1_000));
    failReads(2);
    expect(readHostLease(root, "host:a")?.updatedAt).toBe(1_000);
  });

  it("never caches a read that failed", () => {
    const { root, lease, failReads } = setup();
    writeHostLease(root, lease(1_000));
    const reads = failReads(100);
    expect(readHostLease(root, "host:a")).toBeUndefined();        // no answer yet
    expect(readHostLeases(root).size).toBe(0);
    reads.mockRestore();
    expect(readHostLease(root, "host:a")?.updatedAt).toBe(1_000); // the same file, read now
    expect(readHostLeases(root).get("host:a")?.updatedAt).toBe(1_000);
  });

  it("strict current reads do not reuse a cached lease when bytes are unreadable", () => {
    const { root, lease } = setup();
    writeHostLease(root, lease(1_000));
    expect(readHostLease(root, "host:a")).toEqual(lease(1_000));
    const file = path.join(root, "host-leases", fs.readdirSync(path.join(root, "host-leases"))[0]!);
    const read = fs.readFileSync;
    const spy = vi.spyOn(fs, "readFileSync").mockImplementation((target, options) => {
      if (target === file) throw Object.assign(new Error("lease unavailable"), { code: "EACCES" });
      return read(target, options as never);
    });
    try {
      expect(readHostLeaseCurrent(root, "host:a")).toBeUndefined();
      // The compatibility reader deliberately retains its last answer; the
      // strict operator reader never does.
      expect(readHostLease(root, "host:a")).toEqual(lease(1_000));
    } finally { spy.mockRestore(); }
  });

  it("keeps the last lease while a renewed file cannot be read", () => {
    const { root, lease, failReads } = setup();
    writeHostLease(root, lease(1_000));
    expect(readHostLease(root, "host:a")?.updatedAt).toBe(1_000);
    writeHostLease(root, lease(2_000_000));                      // the renewal, being replaced
    const reads = failReads(100);
    expect(readHostLease(root, "host:a")?.updatedAt).toBe(1_000);
    expect(readHostLeases(root).get("host:a")?.updatedAt).toBe(1_000);
    reads.mockRestore();
    expect(readHostLease(root, "host:a")?.updatedAt).toBe(2_000_000);
  });
});
