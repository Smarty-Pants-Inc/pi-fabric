import { spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { processIncarnation } from "../src/core/atomic-write.js";
import * as currentCustody from "../src/mesh/custody-lock.js";
import { FOREIGN_LOCK, holdMeshFenceSync, MeshFenceBusyError, pinFenceLock, releaseFenceLock } from "../src/mesh/fence-lock.js";
import * as currentMeshLock from "../src/mesh/mesh-lock.js";

// pi-fabric#694 P1 (smarty-dev#6477), third design: the release pins its OWN lock directory by an fd and removes only
// `<pin>/owner` (through /proc/self/fd), then rmdirs the name only while it still holds that inode. No step moves
// a file out of `<lock>` or renames the shared name, so a successor's record never leaves `<lock>/owner` and no
// two parties ever both hold the lock. The precondition (no acquirer recovers a LIVE owner's fence locks) is proven
// below for this tree and for the 13d1bbef release that can run on the same root during the cutover.

const OLD_TREE = process.env.PI_FABRIC_OLD_RELEASE_TREE ?? "/home/paul/lanes/r13d1";
const oldTree = fs.existsSync(path.join(OLD_TREE, "src/mesh/mesh-lock.ts")) ? OLD_TREE : undefined;
const linux = process.platform === "linux";

const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const rootIn = (): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-fence-lock-"));
  roots.push(root);
  return root;
};
const lockIn = (): string => path.join(rootIn(), ".lock");
const hold = (lock: string, record: string): void => {
  fs.mkdirSync(lock, { mode: 0o700 });
  fs.writeFileSync(path.join(lock, "owner"), record, { mode: 0o600 });
};
const owner = (lock: string): string => fs.readFileSync(path.join(lock, "owner"), "utf8");
/** A protocol-1 acquirer (mkdir, then a `wx` owner): true only when it now holds the lock. */
const tryAcquire = (lock: string, record: string): boolean => {
  try { fs.mkdirSync(lock, { mode: 0o700 }); } catch { return false; }
  try { fs.writeFileSync(path.join(lock, "owner"), record, { flag: "wx" }); return true; } catch { return false; }
};
/** A protocol-1 acquirer paused since an earlier mkdir: it resumes with its `wx` owner write into whatever is at `<lock>`. */
const resumeWrite = (lock: string, record: string): boolean => {
  try { fs.writeFileSync(path.join(lock, "owner"), record, { flag: "wx" }); return true; } catch { return false; }
};
/** Every owner record under the root, by relative path: the no-overlap and no-detach ledger. */
const records = (root: string): Record<string, string> => {
  const found: Record<string, string> = {};
  const walk = (dir: string): void => {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      const full = path.join(dir, entry.name);
      if (entry.isDirectory()) walk(full);
      else found[path.relative(root, full)] = fs.readFileSync(full, "utf8");
    }
  };
  walk(root);
  return found;
};
const acquireA = (lock: string): number | undefined => {
  hold(lock, "A\n");
  const pin = pinFenceLock(lock, "A\n");
  expect(pin).not.toBe(FOREIGN_LOCK);
  return pin;
};

describe("releaseFenceLock never detaches a successor (pi-fabric#694 P1)", () => {
  it("pins the lock directory on Linux and removes the releaser's own lock", () => {
    const lock = lockIn();
    const pin = acquireA(lock);
    if (linux) expect(pin).toBeGreaterThanOrEqual(0);
    releaseFenceLock(lock, "A\n", {}, pin);
    expect(fs.readdirSync(path.dirname(lock))).toEqual([]);
  });

  it("a directory without our record is never pinned (we never held it)", () => {
    const lock = lockIn();
    hold(lock, "B\n");
    expect(pinFenceLock(lock, "A\n")).toBe(linux ? FOREIGN_LOCK : undefined);
    expect(owner(lock)).toBe("B\n");
  });

  // The reviewer's interleaving: B replaces the lock after A's check, C tries to publish before A finishes.
  for (const how of ["renamed to a .dead receipt", "removed"] as const) {
    it.skipIf(!linux)(`A release vs B acquire vs C acquire, A's directory ${how} after the check: B alone holds, its record stays in <lock>`, () => {
      const lock = lockIn();
      const root = path.dirname(lock);
      const pin = acquireA(lock);
      let bHolds = false;
      let cHolds = false;
      releaseFenceLock(lock, "A\n", {
        afterCheck: () => {
          // Only possible if the precondition breaks: a recoverer took A's live lock.
          if (how === "removed") fs.rmSync(lock, { recursive: true });
          else fs.renameSync(lock, `${lock}.dead.x`);
          bHolds = tryAcquire(lock, "B\n");
          cHolds = tryAcquire(lock, "C\n") || resumeWrite(lock, "C\n");
        },
      }, pin);
      cHolds ||= tryAcquire(lock, "C\n") || resumeWrite(lock, "C\n");
      expect([bHolds, cHolds]).toEqual([true, false]);
      expect(records(root)).toEqual(how === "removed" ? { ".lock/owner": "B\n" } : { ".lock/owner": "B\n", ".lock.dead.x/owner": "A\n" });
    });
  }

  it("a paused protocol-1 writer that publishes into the emptied directory alone holds it (rmdir fails closed)", () => {
    const lock = lockIn();
    const root = path.dirname(lock);
    const pin = acquireA(lock);
    let cHolds = false;
    let bHolds = false;
    releaseFenceLock(lock, "A\n", { afterTake: () => {
      cHolds = resumeWrite(lock, "C\n");
      bHolds = tryAcquire(lock, "B\n") || resumeWrite(lock, "B\n");
    } }, pin);
    bHolds ||= tryAcquire(lock, "B\n");
    expect([cHolds, bHolds]).toEqual([true, false]);
    expect(records(root)).toEqual({ ".lock/owner": "C\n" });
  });

  it("a successor that recreates the name after the unlink is never removed (inode check before rmdir)", () => {
    const lock = lockIn();
    const root = path.dirname(lock);
    const pin = acquireA(lock);
    let bHolds = false;
    let cHolds = false;
    releaseFenceLock(lock, "A\n", { afterTake: () => {
      fs.rmdirSync(lock); // an aged-grace recoverer removes the empty directory
      bHolds = tryAcquire(lock, "B\n");
      cHolds = tryAcquire(lock, "C\n") || resumeWrite(lock, "C\n");
    } }, pin);
    cHolds ||= tryAcquire(lock, "C\n") || resumeWrite(lock, "C\n");
    expect([bHolds, cHolds]).toEqual([true, false]);
    expect(records(root)).toEqual({ ".lock/owner": "B\n" });
  });

  it("unpinned fallback: a successor's record taken inside <lock> goes back and C never enters", () => {
    const lock = lockIn();
    const root = path.dirname(lock);
    hold(lock, "A\n");
    let bHolds = false;
    let cHolds = false;
    releaseFenceLock(lock, "A\n", {
      afterCheck: () => { fs.rmSync(lock, { recursive: true }); bHolds = tryAcquire(lock, "B\n"); },
      afterTake: () => {
        expect(Object.keys(records(root)).every((name) => name.startsWith(".lock/"))).toBe(true); // never out of <lock>
        cHolds = tryAcquire(lock, "C\n");
      },
    }, undefined);
    cHolds ||= tryAcquire(lock, "C\n") || resumeWrite(lock, "C\n");
    expect([bHolds, cHolds]).toEqual([true, false]);
    expect(records(root)).toEqual({ ".lock/owner": "B\n" });
  });

  it("holdMeshFenceSync excludes a second holder and leaves a lock that replaced its own during the operation", () => {
    const lock = lockIn();
    const root = path.dirname(lock);
    holdMeshFenceSync(root, 0, () => {
      expect(() => holdMeshFenceSync(root, 0, () => undefined)).toThrow(MeshFenceBusyError);
      fs.rmSync(lock, { recursive: true });
      hold(lock, "successor\n");
    });
    expect(owner(lock)).toBe("successor\n");
    expect(fs.readdirSync(root).sort()).toEqual([".lock"]);
  });
});

interface LockModules {
  MeshLock: typeof currentMeshLock.MeshLock;
  acquireMeshCustodyLock: typeof currentCustody.acquireMeshCustodyLock;
}

/** Live owners: this process and its parent, with the fence's three-line wire and with a matching incarnation. */
const liveRecords = async (): Promise<Array<{ name: string; record: string }>> => {
  const old = Date.now() - 3_600_000;
  const records: Array<{ name: string; record: string }> = [];
  for (const pid of [process.pid, process.ppid]) {
    records.push({ name: `pid ${pid === process.pid ? "self" : "parent"}, fence wire`, record: `${randomUUID()}\n${pid}\n${old}\n` });
    const start = await processIncarnation(pid, 5_000);
    if (start) records.push({ name: `pid ${pid === process.pid ? "self" : "parent"}, incarnation`, record: `${randomUUID()}\n${pid}\n${old}\n${start}\n` });
  }
  return records;
};

const expectLiveFenceKept = async (modules: LockModules): Promise<number> => {
  let cases = 0;
  for (const { name, record } of await liveRecords()) {
    for (const lockProtocol of [1, 2] as const) {
      const root = rootIn();
      const mesh = path.join(root, ".lock");
      const custody = path.join(root, "custody.lock");
      hold(mesh, record);
      hold(custody, record);
      const aged = new Date(Date.now() - 3_600_000);
      for (const lock of [mesh, custody]) fs.utimesSync(lock, aged, aged);
      const meshLock = new modules.MeshLock(root, { lockProtocol, lockTimeoutMs: 400, staleLockMs: 100 }, () => undefined);
      let entered = false;
      await expect(meshLock.withLockAcrossAwait(async () => { entered = true; }, 400), `${name}, protocol ${lockProtocol}`)
        .rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
      await expect(modules.acquireMeshCustodyLock(root, 400), `${name} (custody)`)
        .rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
      expect(entered).toBe(false);
      expect(owner(mesh)).toBe(record);
      expect(owner(custody)).toBe(record);
      expect(fs.readdirSync(root).filter(entry => entry.includes(".dead.") || entry.includes(".released."))).toEqual([]);
      cases += 1;
    }
  }
  // Control: the same acquirers DO recover a dead owner's locks, so the cases above are not vacuous.
  const dead = spawnSync(process.execPath, ["-e", ""]).pid;
  const root = rootIn();
  const record = `${randomUUID()}\n${dead}\n${Date.now()}\n`;
  hold(path.join(root, ".lock"), record);
  hold(path.join(root, "custody.lock"), record);
  const release = await modules.acquireMeshCustodyLock(root, 2_000);
  release();
  let entered = false;
  await new modules.MeshLock(root, { lockProtocol: 1, lockTimeoutMs: 2_000 }, () => undefined)
    .withLockAcrossAwait(async () => { entered = true; }, 2_000);
  expect(entered).toBe(true);
  return cases;
};

describe("a live owner's fence locks are never recovered (the release precondition)", () => {
  it("this release's MeshLock and custody acquirers, past staleLockMs and the grace", async () => {
    expect(await expectLiveFenceKept({ MeshLock: currentMeshLock.MeshLock, acquireMeshCustodyLock: currentCustody.acquireMeshCustodyLock }))
      .toBeGreaterThanOrEqual(4);
  }, 30_000);

  it.skipIf(!oldTree)("the 13d1bbef release's MeshLock and custody acquirers (mixed-release cutover)", async () => {
    const oldMesh = await import(path.join(oldTree!, "src/mesh/mesh-lock.ts")) as typeof currentMeshLock;
    const oldCustody = await import(path.join(oldTree!, "src/mesh/custody-lock.ts")) as typeof currentCustody;
    expect(oldMesh.MeshLock).not.toBe(currentMeshLock.MeshLock);
    expect(await expectLiveFenceKept({ MeshLock: oldMesh.MeshLock, acquireMeshCustodyLock: oldCustody.acquireMeshCustodyLock }))
      .toBeGreaterThanOrEqual(4);
  }, 30_000);
});
