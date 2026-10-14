import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, expect, it, vi } from "vitest";

// smarty-dev#8907: the sqlite lock reclaim removes only the inode it judged dead. Nothing here deletes: unlink/rm are
// recorded no-op probes, and verifiedRemove runs the real verifier as a dry run (the off-Linux cases record main's
// fallback unlink through the same no-op probe).
const removals: Array<{ target: string; ino: number | undefined; result: { removed: boolean } }> = [];
let dryRun = true;
vi.mock("../src/storage/verified-remove.js", async (load) => {
  const actual = await load<typeof import("../src/storage/verified-remove.js")>();
  return { ...actual, verifiedRemove: (...[target, judged, options]: Parameters<typeof actual.verifiedRemove>) => {
    const result = actual.verifiedRemove(target, judged, dryRun ? { ...options, dryRun: true } : options);
    removals.push({ target, ino: judged?.ino, result });
    return result;
  } };
});
const { tryLockFile, checkpointFlagRaised } = await import("../src/mesh/state-sqlite.js");
const { verifiedRemoveTestHooks } = await import("../src/storage/verified-remove.js");
// The inode verifier (swap refusals, judged-inode assertions) exists only on real Linux; off Linux main's fallback
// deliberately compares no inode (smarty-dev#7858), covered below by the controlled darwin stub on every host.
const onLinux = process.platform === "linux";

const real = { readFileSync: fs.readFileSync, renameSync: fs.renameSync, writeFileSync: fs.writeFileSync, rmSync: fs.rmSync };
// A live holder (this pid, no start ticks): a leftover reclaim claim is never itself reclaimed.
const TOKEN = `${process.pid}.-.token`;
let dir: string, lock: string, unlinked: string[];
beforeEach(() => {
  dir = fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "sqlite-vremove-"));
  lock = path.join(dir, "wal-reset.lock");
  fs.writeFileSync(lock, "ownerless\n", { mode: 0o600 }); // unreadable owner: dead by age (staleMs 0)
  fs.utimesSync(lock, new Date(0), new Date(0));
  removals.length = 0; dryRun = true; unlinked = [];
  const record = (file: fs.PathLike): void => { unlinked.push(String(file)); };
  vi.spyOn(fs, "unlinkSync").mockImplementation(record);
  vi.spyOn(fs, "rmSync").mockImplementation(record);
});
afterEach(() => { vi.restoreAllMocks(); real.rmSync(dir, { recursive: true, force: true }); });

it.runIf(onLinux)("a lock replaced after the reclaim's judgment and re-check is kept and never unlinked", () => {
  let reads = 0;
  vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, options?: unknown) => {
    const text = real.readFileSync(file, options as BufferEncoding);
    if (file === lock && ++reads === 2) { // the re-check's read: swap in a successor with the same record
      real.writeFileSync(`${lock}.next`, "ownerless\n", { mode: 0o600 });
      real.renameSync(`${lock}.next`, lock);
    }
    return text;
  }) as typeof fs.readFileSync);
  const judged = fs.lstatSync(lock).ino;
  expect(tryLockFile(lock, 0, TOKEN)).toBe(false);
  expect(unlinked).not.toContain(lock);
  expect(fs.lstatSync(lock).ino).not.toBe(judged);
  expect(removals).toEqual([{ target: lock, ino: judged, result: expect.objectContaining({ removed: false }) }]);
});

it.runIf(onLinux)("an unchanged dead lock is authorized for removal with its judged stat (dry run)", () => {
  const judged = fs.lstatSync(lock).ino;
  tryLockFile(lock, 0, TOKEN);
  expect(removals[0]).toEqual({ target: lock, ino: judged, result: expect.objectContaining({ removed: true }) });
  expect(unlinked).not.toContain(lock);
});

it.runIf(onLinux)("a stale dead checkpoint flag swapped before the verifier opens it is kept; a graveyard is no flag", () => {
  const flags = path.join(dir, "state-checkpoint.flags");
  fs.mkdirSync(flags, { mode: 0o700 });
  // This pid with other start ticks: a dead holder by the tryLockFile rule (Linux start ticks).
  const flag = path.join(flags, `${process.pid}.1.dead-holder`);
  fs.writeFileSync(flag, "", { mode: 0o600 });
  fs.utimesSync(flag, new Date(0), new Date(0));
  fs.mkdirSync(path.join(flags, ".retention-graveyard-1"), { mode: 0o700 }); // fresh: would read as a raised flag
  const judged = fs.lstatSync(flag).ino;
  verifiedRemoveTestHooks.beforeOpen = (at) => {
    if (at !== flag) return;
    real.writeFileSync(`${flag}.swap`, "successor", { mode: 0o600 });
    real.renameSync(`${flag}.swap`, flag);
  };
  try { expect(checkpointFlagRaised(dir)).toBe(false); } finally { delete verifiedRemoveTestHooks.beforeOpen; }
  expect(real.readFileSync(flag, "utf8")).toBe("successor");
  expect(unlinked).not.toContain(flag);
  expect(removals).toEqual([{ target: flag, ino: judged, result: expect.objectContaining({ removed: false }) }]);
});

const offLinux = (body: () => void): void => {
  const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
  Object.defineProperty(process, "platform", { value: "darwin" });
  dryRun = false;
  try { body(); } finally { Object.defineProperty(process, "platform", platform); }
};

it("off Linux the reclaim uses main's unlink (recorded, not run)", () => offLinux(() => {
  tryLockFile(lock, 0, TOKEN);
  expect(removals[0]).toEqual({ target: lock, ino: expect.any(Number), result: expect.objectContaining({ removed: true }) });
  expect(unlinked[0]).toBe(lock);
}));

it("off Linux a failed unlink fails the reclaim instead of reporting it removed", () => offLinux(() => {
  vi.mocked(fs.unlinkSync).mockImplementation((file) => { unlinked.push(String(file)); throw Object.assign(new Error("busy"), { code: "EBUSY" }); });
  expect(tryLockFile(lock, 0, TOKEN)).toBe(false);
  expect(removals).toEqual([{ target: lock, ino: expect.any(Number), result: expect.objectContaining({ removed: false }) }]);
  expect(unlinked[0]).toBe(lock);
}));
