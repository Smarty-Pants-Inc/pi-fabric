import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { acquireMeshCustodyLock, MESH_CUSTODY_LOCK_NAME } from "../src/mesh/custody-lock.js";
import { MeshLock } from "../src/mesh/mesh-lock.js";

// smarty-dev#8777: a holder that stalled past staleMs and was reclaimed reaches its release between the
// owner check and the removal. The release must leave the reclaimer's (successor's) lock in place.
const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const tempRoot = (): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-pinned-release-"));
  roots.push(root);
  return root;
};
const successor = "successor-token\n424242\n1\n";

/**
 * Arms the stall: the release's next read of an owner record of `lock` that returns `ours` is followed,
 * before the release can act on it, by a reclaim (the old directory renamed to a .dead. receipt, as
 * clearStaleLock does) and a successor's complete lock at the same name.
 */
const stallAfterOwnerCheck = (lock: string, ours: () => string | undefined) => {
  const read = fs.readFileSync;
  let fired = false;
  vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, options?: unknown) => {
    const result = (read as (...args: unknown[]) => unknown)(file, options);
    if (!fired && ours() !== undefined && String(file).endsWith(`${path.sep}owner`) && result === ours()) {
      fired = true;
      fs.renameSync(lock, `${lock}.dead.reclaimed`);
      fs.mkdirSync(lock, { mode: 0o700 });
      fs.writeFileSync(path.join(lock, "owner"), successor, { mode: 0o600 });
    }
    return result;
  }) as typeof fs.readFileSync);
  return () => fired;
};

// The pin needs /proc/self/fd (Linux). Non-Linux keeps main's release; its stalled-holder race is tracked in
// smarty-dev#9569.
describe.runIf(process.platform === "linux")("mesh locks release only the directory they hold (smarty-dev#8777)", () => {
  it.each([1, 2] as const)("a reclaimed mesh .lock holder (protocol %i) never removes its successor's lock", async protocol => {
    const root = tempRoot();
    const lock = path.join(root, ".lock");
    const mesh = new MeshLock(root, { lockProtocol: protocol, lockTimeoutMs: 1_000 }, () => undefined);
    let record: string | undefined;
    let fired = () => false;
    await mesh.withLockAcrossAwait(async () => {
      record = fs.readFileSync(path.join(lock, "owner"), "utf8");
      fired = stallAfterOwnerCheck(lock, () => record);
    });
    expect(fired()).toBe(true);
    expect(fs.readFileSync(path.join(lock, "owner"), "utf8")).toBe(successor); // the successor still holds it
  });

  it("a reclaimed custody.lock holder never removes its successor's lock", async () => {
    const root = tempRoot();
    const lock = path.join(root, MESH_CUSTODY_LOCK_NAME);
    const release = await acquireMeshCustodyLock(root, 1_000, { ownIncarnation: undefined });
    const record = fs.readFileSync(path.join(lock, "owner"), "utf8");
    const fired = stallAfterOwnerCheck(lock, () => record);
    release();
    expect(fired()).toBe(true);
    expect(fs.readFileSync(path.join(lock, "owner"), "utf8")).toBe(successor);
  });

  it.each([1, 2] as const)("a normal release (protocol %i) still removes its own locks", async protocol => {
    const root = tempRoot();
    const mesh = new MeshLock(root, { lockProtocol: protocol, lockTimeoutMs: 1_000 }, () => undefined);
    await mesh.withLockAcrossAwait(async () => {
      expect(fs.existsSync(path.join(root, ".lock", "owner"))).toBe(true);
    });
    const release = await acquireMeshCustodyLock(root, 1_000, { ownIncarnation: undefined });
    expect(fs.existsSync(path.join(root, MESH_CUSTODY_LOCK_NAME, "owner"))).toBe(true);
    release();
    release(); // a second call is a no-op
    expect(fs.readdirSync(root).filter(name => name.startsWith(".lock") || name.startsWith(MESH_CUSTODY_LOCK_NAME))).toEqual([]);
  });
});

// pi-fabric#889 review: without a pin (non-Linux; here /proc/self/fd made unreadable) the custody release is main's,
// and like main it can be called again after a transient failure, so a live owner never keeps the lock.
describe("the unpinned custody release keeps main's retry", () => {
  const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
  afterEach(() => { Object.defineProperty(process, "platform", platform); });
  it("a release whose rename failed transiently succeeds when it is called again (non-Linux)", async () => {
    const root = tempRoot();
    const lock = path.join(root, MESH_CUSTODY_LOCK_NAME);
    // Off Linux there is no pin (pinFenceLock: undefined); a Linux host cannot take this path any more.
    Object.defineProperty(process, "platform", { ...platform, value: "darwin" });
    const release = await acquireMeshCustodyLock(root, 1_000, { ownIncarnation: undefined });
    const rename = fs.renameSync;
    let failures = 1;
    vi.spyOn(fs, "renameSync").mockImplementation(((from: fs.PathLike, to: fs.PathLike) => {
      if (String(from) === lock && failures-- > 0) throw Object.assign(new Error("EBUSY: transient"), { code: "EBUSY" });
      return rename(from, to);
    }) as typeof fs.renameSync);
    release();
    expect(fs.existsSync(path.join(lock, "owner"))).toBe(true); // the transient failure kept it
    release();
    expect(fs.existsSync(lock)).toBe(false);
  });
});

// pi-fabric#889 review (Astra P1): on Linux a pin that fails operationally must not strand our lock (FOREIGN_LOCK
// from a failed open) nor let the section run unpinned (undefined from an unreadable /proc/self/fd/<fd>/owner).
describe.runIf(process.platform === "linux")("a failed pin releases the just-created lock and fails retryably", () => {
  afterEach(() => { vi.restoreAllMocks(); });
  const failOpen = (lock: string) => {
    const open = fs.openSync;
    vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, ...rest: unknown[]) => {
      if (String(file) === lock) throw Object.assign(new Error("EMFILE: too many open files"), { code: "EMFILE" });
      return (open as (...args: unknown[]) => number)(file, ...rest);
    }) as typeof fs.openSync);
  };
  const failPinnedRead = () => {
    const read = fs.readFileSync;
    vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, options?: unknown) => {
      if (/^\/proc\/self\/fd\/\d+\/owner$/.test(String(file))) throw Object.assign(new Error("EIO: i/o error"), { code: "EIO" });
      return (read as (...args: unknown[]) => unknown)(file, options);
    }) as typeof fs.readFileSync);
  };

  it.each(["open EMFILE", "read EIO"] as const)("mesh .lock (%s): no section runs, the lock is gone, the next acquire succeeds", async failure => {
    const root = tempRoot();
    const lock = path.join(root, ".lock");
    const mesh = new MeshLock(root, { lockProtocol: 2, lockTimeoutMs: 1_000 }, () => undefined);
    if (failure === "open EMFILE") failOpen(lock); else failPinnedRead();
    const operation = vi.fn(async () => "ran");
    await expect(mesh.withLockAcrossAwait(operation)).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    expect(operation).not.toHaveBeenCalled();
    expect(fs.existsSync(lock)).toBe(false);
    vi.restoreAllMocks();
    await expect(mesh.withLockAcrossAwait(async () => "ran")).resolves.toBe("ran");
  });

  it.each(["open EMFILE", "read EIO"] as const)("custody.lock (%s): the acquire fails, the lock is gone, the next acquire succeeds", async failure => {
    const root = tempRoot();
    const lock = path.join(root, MESH_CUSTODY_LOCK_NAME);
    if (failure === "open EMFILE") failOpen(lock); else failPinnedRead();
    await expect(acquireMeshCustodyLock(root, 1_000, { ownIncarnation: undefined })).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    expect(fs.existsSync(lock)).toBe(false);
    vi.restoreAllMocks();
    const release = await acquireMeshCustodyLock(root, 1_000, { ownIncarnation: undefined });
    expect(fs.existsSync(path.join(lock, "owner"))).toBe(true);
    release();
  });
});
