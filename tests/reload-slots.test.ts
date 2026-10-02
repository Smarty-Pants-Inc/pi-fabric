import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { RELOAD_SLOT_STALE_MS, reloadSlotsDirectory, tryAcquireReloadSlot } from "../src/lifecycle/reload-slots.js";

let root: string;
const releases: Array<() => void> = [];
const acquire = (count = 2) => {
  const release = tryAcquireReloadSlot(count, root);
  if (release) releases.push(release);
  return release;
};
const record = (index = 0) => {
  const slot = path.join(root, `slot-${index}`);
  const file = path.join(slot, fs.readdirSync(slot)[0]!);
  return { slot, file, owner: JSON.parse(fs.readFileSync(file, "utf8")) };
};
beforeEach(() => { root = fs.mkdtempSync(path.join(os.tmpdir(), "reload-slots-test-")); });
afterEach(() => {
  for (const release of releases.splice(0)) release();
  vi.restoreAllMocks(); vi.useRealTimers(); vi.unstubAllEnvs();
  fs.rmSync(root, { recursive: true, force: true });
});

describe("host-local reload leases", () => {
  it("uses a host/user directory independent of a POSIX session's TMPDIR", () => {
    if (process.platform === "win32") return;
    const first = reloadSlotsDirectory();
    vi.stubEnv("TMPDIR", root);
    expect(reloadSlotsDirectory()).toBe(first);
    expect(first).toBe(`/tmp/pi-fabric-reload-slots-${process.getuid!()}`);
  });

  it("admits N holders, records pid/birth/start, and releases idempotently", () => {
    const first = acquire(); const second = acquire();
    expect(first).toBeTypeOf("function"); expect(second).toBeTypeOf("function");
    expect(acquire()).toBeUndefined();
    const { owner } = record();
    expect(owner).toMatchObject({ pid: process.pid, startedAt: expect.any(Number) });
    if (process.platform === "linux") expect(owner.birth).toMatch(/^\d+$/);
    first!(); first!();
    const successor = acquire(); expect(successor).toBeTypeOf("function");
    first!(); expect(acquire()).toBeUndefined(); // late old release cannot erase successor
  });

  it("reclaims a dead holder immediately", () => {
    acquire(1);
    const { file, owner } = record();
    fs.writeFileSync(file, JSON.stringify({ ...owner, pid: 2_147_483_647 }));
    expect(acquire(1)).toBeTypeOf("function");
    expect(acquire(1)).toBeUndefined();
  });

  it.runIf(process.platform === "linux")("reclaims a reused pid with a different birth identity", () => {
    acquire(1);
    const { file, owner } = record();
    fs.writeFileSync(file, JSON.stringify({ ...owner, birth: "0" }));
    expect(acquire(1)).toBeTypeOf("function");
  });

  it("treats denied/unknown process identity as live until expiry", () => {
    acquire(1);
    vi.spyOn(process, "kill").mockImplementation(() => { throw Object.assign(new Error("denied"), { code: "EPERM" }); });
    expect(acquire(1)).toBeUndefined();
  });

  it("reclaims a live lease strictly after 120 seconds; late release preserves its successor", () => {
    const old = acquire(1)!;
    const { file, owner } = record();
    vi.useFakeTimers(); vi.setSystemTime(owner.startedAt + RELOAD_SLOT_STALE_MS);
    expect(acquire(1)).toBeUndefined();
    vi.setSystemTime(owner.startedAt + RELOAD_SLOT_STALE_MS + 1);
    expect(acquire(1)).toBeTypeOf("function");
    old(); expect(fs.existsSync(file)).toBe(false);
    expect(acquire(1)).toBeUndefined();
  });

  it("timeout frees a hung reload without waiting for a future reclaimer", async () => {
    vi.useFakeTimers();
    const old = acquire(1)!;
    await vi.advanceTimersByTimeAsync(RELOAD_SLOT_STALE_MS);
    expect(fs.readdirSync(root)).toEqual([]);
    expect(acquire(1)).toBeTypeOf("function");
    old(); expect(acquire(1)).toBeUndefined();
  });

  it("concurrent stale recovery cannot remove a newly published nonempty slot", () => {
    const old = acquire(1)!;
    const { file, owner } = record();
    fs.writeFileSync(file, JSON.stringify({ ...owner, startedAt: Date.now() - RELOAD_SLOT_STALE_MS - 1 }));
    const unlink = fs.unlinkSync;
    let successor: (() => void) | undefined;
    vi.spyOn(fs, "unlinkSync").mockImplementation(target => {
      unlink(target);
      if (target === file && !successor) successor = acquire(1);
    });
    // The inner contender publishes between this reclaimer's unlink and rmdir.
    expect(acquire(1)).toBeUndefined();
    expect(successor).toBeTypeOf("function");
    old(); expect(acquire(1)).toBeUndefined();
  });

  it("damaged records stay occupied until their mtime expires; abandoned empty slots are reusable", () => {
    acquire(1);
    const { file } = record(); fs.writeFileSync(file, "{");
    expect(acquire(1)).toBeUndefined();
    const expired = new Date(Date.now() - RELOAD_SLOT_STALE_MS - 1);
    fs.utimesSync(file, expired, expired);
    expect(acquire(1)).toBeTypeOf("function");
    releases.splice(0).forEach(release => release());
    fs.mkdirSync(path.join(root, "slot-0"));
    expect(acquire(1)).toBeTypeOf("function");
  });
});
