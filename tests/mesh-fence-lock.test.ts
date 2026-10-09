import { spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { processIncarnation } from "../src/core/atomic-write.js";
import * as currentCustody from "../src/mesh/custody-lock.js";
import { holdMeshFenceSync, releaseFenceLock } from "../src/mesh/fence-lock.js";
import * as currentMeshLock from "../src/mesh/mesh-lock.js";

// pi-fabric#694 P1 (smarty-dev#6477): a release takes its own owner file and rmdirs the lock; it never renames
// the lock directory, so a successor that appears at any interleaving point keeps its lock and its owner file.
// The precondition, that no acquirer recovers a LIVE owner's fence locks, is proven below for this tree and
// for the 13d1bbef release that can run on the same root during the cutover.

const OLD_TREE = process.env.PI_FABRIC_OLD_RELEASE_TREE ?? "/home/paul/lanes/r13d1";
const oldTree = fs.existsSync(path.join(OLD_TREE, "src/mesh/mesh-lock.ts")) ? OLD_TREE : undefined;

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

describe("releaseFenceLock (pi-fabric#694 P1)", () => {
  it("removes the releaser's own lock", () => {
    const lock = lockIn();
    hold(lock, "mine\n");
    releaseFenceLock(lock, "t1", "mine\n");
    expect(fs.readdirSync(path.dirname(lock))).toEqual([]);
  });

  it("a successor that replaces the lock between the owner check and the take keeps its lock and owner", () => {
    const lock = lockIn();
    hold(lock, "mine\n");
    releaseFenceLock(lock, "t1", "mine\n", { afterCheck: () => {
      // Only possible if the precondition breaks: a recoverer removed ours and a successor holds the lock.
      fs.rmSync(lock, { recursive: true });
      hold(lock, "successor\n");
    } });
    expect(owner(lock)).toBe("successor\n");
    expect(fs.readdirSync(path.dirname(lock))).toEqual([".lock"]);
  });

  it("a successor that publishes into the directory after the take keeps its lock and owner (rmdir fails closed)", () => {
    const lock = lockIn();
    hold(lock, "mine\n");
    releaseFenceLock(lock, "t1", "mine\n", { afterTake: () => {
      // A resumed protocol-1 initializer writes its owner into the (now ownerless) canonical directory.
      fs.writeFileSync(path.join(lock, "owner"), "successor\n", { flag: "wx" });
    } });
    expect(owner(lock)).toBe("successor\n");
    expect(fs.readdirSync(path.dirname(lock))).toEqual([".lock"]);
  });

  it("a successor that recreates the lock after the take (ours already gone) keeps it", () => {
    const lock = lockIn();
    hold(lock, "mine\n");
    releaseFenceLock(lock, "t1", "mine\n", { afterTake: () => {
      fs.rmdirSync(lock);
      hold(lock, "successor\n");
    } });
    expect(owner(lock)).toBe("successor\n");
    expect(fs.readdirSync(path.dirname(lock))).toEqual([".lock"]);
  });

  it("a foreign record taken by mistake is never deleted nor written over a newer owner", () => {
    const lock = lockIn();
    hold(lock, "mine\n");
    releaseFenceLock(lock, "t1", "mine\n", {
      afterCheck: () => { fs.rmSync(lock, { recursive: true }); hold(lock, "successor\n"); },
      afterTake: () => fs.writeFileSync(path.join(lock, "owner"), "newer\n", { flag: "wx" }),
    });
    expect(owner(lock)).toBe("newer\n");
    expect(fs.readFileSync(`${lock}.released.t1`, "utf8")).toBe("successor\n");
  });

  it("holdMeshFenceSync leaves a lock that replaced its own during the operation", () => {
    const lock = lockIn();
    const root = path.dirname(lock);
    holdMeshFenceSync(root, 0, () => {
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
