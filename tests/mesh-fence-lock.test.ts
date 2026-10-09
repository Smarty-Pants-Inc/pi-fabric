import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { holdMeshFenceSync, releaseFenceLock } from "../src/mesh/fence-lock.js";

// pi-fabric#694 security review P1-A: a release detaches first and verifies the owner record inside the
// detached directory, so a successor that replaced the lock after the owner check is never deleted.

const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const lockIn = (): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-fence-lock-"));
  roots.push(root);
  return path.join(root, ".lock");
};
const hold = (lock: string, record: string): void => {
  fs.mkdirSync(lock, { mode: 0o700 });
  fs.writeFileSync(path.join(lock, "owner"), record, { mode: 0o600 });
};
const owner = (lock: string): string => fs.readFileSync(path.join(lock, "owner"), "utf8");

describe("releaseFenceLock (pi-fabric#694 P1-A)", () => {
  it("removes the releaser's own lock", () => {
    const lock = lockIn();
    hold(lock, "mine\n");
    releaseFenceLock(lock, "t1", "mine\n");
    expect(fs.readdirSync(path.dirname(lock))).toEqual([]);
  });

  it("a successor that replaces the lock between the owner check and the rename survives", () => {
    const lock = lockIn();
    hold(lock, "mine\n");
    releaseFenceLock(lock, "t1", "mine\n", { afterCheck: () => {
      // A recoverer removed ours and a successor holds the lock now.
      fs.rmSync(lock, { recursive: true });
      hold(lock, "successor\n");
    } });
    expect(owner(lock)).toBe("successor\n");
    expect(fs.readdirSync(path.dirname(lock))).toEqual([".lock"]);
  });

  it("a detached foreign lock stays detached, never deleted, when a newer holder took the name", () => {
    const lock = lockIn();
    hold(lock, "mine\n");
    releaseFenceLock(lock, "t1", "mine\n", {
      afterCheck: () => { fs.rmSync(lock, { recursive: true }); hold(lock, "successor\n"); },
      afterDetach: () => hold(lock, "newer\n"),
    });
    expect(owner(lock)).toBe("newer\n");
    expect(owner(`${lock}.released.t1`)).toBe("successor\n");
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
