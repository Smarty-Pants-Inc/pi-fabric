import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshLockTimeoutError } from "../src/core/atomic-write.js";
import {
  acquireMeshCustodyLock, CUSTODY_LOCK_STALE_MS, MeshCustodyUnrecoverableError,
} from "../src/mesh/custody-lock.js";
import { FOREIGN_LOCK, pinFenceLock, releaseFenceLock } from "../src/mesh/fence-lock.js";

// Data-safety regression: a host-qualified waiter must survive the real release's
// ownerless interval without acquiring early or treating a foreign PID as local.
const roots: string[] = [];
const setup = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-custody-ownerless-"));
  roots.push(root);
  const lock = path.join(root, "custody.lock");
  return { root, lock, owner: path.join(lock, "owner") };
};
const options = { hostQualified: true, ownIncarnation: undefined };
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("host-qualified custody during ownerless release", () => {
  for (const pinned of [true, false]) {
    it.skipIf(pinned && process.platform !== "linux")(`waits through the ${pinned ? "pinned unlink" : "unpinned owner rename"} before rmdir`, async () => {
      const s = setup();
      const originalRelease = await acquireMeshCustodyLock(s.root, 0, options);
      const record = fs.readFileSync(s.owner, "utf8"), inode = fs.lstatSync(s.lock);
      const pin = pinned ? pinFenceLock(s.lock, record) : undefined;
      if (pinned) expect(pin).toBeGreaterThanOrEqual(0);
      expect(pin).not.toBe(FOREIGN_LOCK);
      const alarm = vi.spyOn(console, "warn").mockImplementation(() => {});
      const read = fs.readFileSync;
      let missingReads = 0;
      vi.spyOn(fs, "readFileSync").mockImplementation((...args: Parameters<typeof fs.readFileSync>) => {
        try { return read(...args); }
        catch (error) {
          if (String(args[0]) === s.owner && (error as NodeJS.ErrnoException).code === "ENOENT") missingReads++;
          throw error;
        }
      });
      let contender: Promise<{ release: () => void } | { error: unknown }> | undefined;
      try {
        const result = releaseFenceLock(s.lock, record, { afterTake: () => {
          // Release is paused at its production seam, after owner unlink/rename,
          // but before canonical rmdir. The acquirer reaches readOwner synchronously
          // (no incarnation setup await), so this is not a timing-dependent sleep.
          expect(fs.lstatSync(s.lock)).toMatchObject({ dev: inode.dev, ino: inode.ino });
          expect(fs.existsSync(s.owner)).toBe(false);
          contender = acquireMeshCustodyLock(s.root, 1_000, options).then(release => ({ release }), error => ({ error }));
          expect(missingReads).toBeGreaterThan(0);
          expect(fs.lstatSync(s.lock)).toMatchObject({ dev: inode.dev, ino: inode.ino });
        } }, pin);
        expect(result).toEqual({ released: true });
        expect(contender).toBeDefined();
        const outcome = await contender!;
        if ("error" in outcome) throw outcome.error;
        try {
          expect(fs.readFileSync(s.owner, "utf8")).not.toBe(record);
          expect(fs.readdirSync(s.root)).toEqual(["custody.lock"]);
          expect(alarm).not.toHaveBeenCalled();
        } finally { outcome.release(); }
      } finally { originalRelease(); }
    });
  }

  it("times out normally while a recent missing owner keeps the gate busy", async () => {
    const s = setup(); fs.mkdirSync(s.lock);
    const inode = fs.lstatSync(s.lock), alarm = vi.spyOn(console, "warn").mockImplementation(() => {});
    const error = await acquireMeshCustodyLock(s.root, 40, options).catch((error: unknown) => error);
    expect(error).toBeInstanceOf(MeshLockTimeoutError);
    expect(error).toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    expect(error).not.toBeInstanceOf(MeshCustodyUnrecoverableError);
    expect(fs.lstatSync(s.lock)).toMatchObject({ dev: inode.dev, ino: inode.ino });
    expect(fs.readdirSync(s.lock)).toEqual([]);
    expect(fs.readdirSync(s.root)).toEqual(["custody.lock"]);
    expect(alarm).not.toHaveBeenCalled();
  });

  it("uses post-unlink directory age, not a stale stat captured before release", async () => {
    const s = setup(), release = await acquireMeshCustodyLock(s.root, 0, options);
    const old = new Date(Date.now() - CUSTODY_LOCK_STALE_MS - 5_000);
    fs.utimesSync(s.lock, old, old);
    const read = fs.readFileSync;
    let removed = false;
    vi.spyOn(fs, "readFileSync").mockImplementation((...args: Parameters<typeof fs.readFileSync>) => {
      if (!removed && String(args[0]) === s.owner) {
        removed = true;
        fs.unlinkSync(s.owner); // The release's unlink occurs after the initial directory stat.
      }
      return read(...args);
    });
    const alarm = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      await expect(acquireMeshCustodyLock(s.root, 0, options)).rejects.toBeInstanceOf(MeshLockTimeoutError);
      expect(removed).toBe(true);
      expect(alarm).not.toHaveBeenCalled();
      expect(fs.existsSync(s.lock)).toBe(true);
    } finally { release(); }
  });

  it("alarms and refuses an unchanged ownerless directory past the grace, without recovering it", async () => {
    const s = setup(); fs.mkdirSync(s.lock);
    const old = new Date(Date.now() - CUSTODY_LOCK_STALE_MS - 5_000);
    fs.utimesSync(s.lock, old, old);
    const inode = fs.lstatSync(s.lock), alarm = vi.spyOn(console, "warn").mockImplementation(() => {});
    const kill = vi.spyOn(process, "kill");
    await expect(acquireMeshCustodyLock(s.root, 0, options)).rejects.toBeInstanceOf(MeshCustodyUnrecoverableError);
    expect(alarm).toHaveBeenCalledExactlyOnceWith(expect.stringContaining("ALARM custody-unrecoverable"));
    expect(kill).not.toHaveBeenCalled();
    expect(fs.lstatSync(s.lock)).toMatchObject({ dev: inode.dev, ino: inode.ino });
    expect(fs.readdirSync(s.lock)).toEqual([]);
    expect(fs.readdirSync(s.root)).toEqual(["custody.lock"]);
  });

  it.each(["", "foreign-owner\nnot-a-pid\n", "foreign-owner\n\n"])("immediately alarms on PRESENT invalid owner %j", async owner => {
    const s = setup(); fs.mkdirSync(s.lock); fs.writeFileSync(s.owner, owner);
    const alarm = vi.spyOn(console, "warn").mockImplementation(() => {}), kill = vi.spyOn(process, "kill");
    await expect(acquireMeshCustodyLock(s.root, 0, options)).rejects.toBeInstanceOf(MeshCustodyUnrecoverableError);
    expect(alarm).toHaveBeenCalledExactlyOnceWith(expect.stringContaining("ALARM custody-unrecoverable"));
    expect(kill).not.toHaveBeenCalled();
    expect(fs.readFileSync(s.owner, "utf8")).toBe(owner);
    expect(fs.readdirSync(s.root)).toEqual(["custody.lock"]);
  });
});
