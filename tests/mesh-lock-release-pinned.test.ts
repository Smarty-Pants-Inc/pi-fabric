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
