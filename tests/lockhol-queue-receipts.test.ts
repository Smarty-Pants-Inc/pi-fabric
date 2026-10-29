import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshLockTicket, meshLockQueueDirectory } from "../src/mesh/lock-queue.js";
import { MeshLock, MeshLockQueueExpiredError } from "../src/mesh/mesh-lock.js";
import { verifiedRemoveTestHooks } from "../src/storage/verified-remove.js";

// Recorded catch: smarty-dev#9805's head + immediate successor share one stalled event loop.
// Real receipts, verifier and canonical acquisition; only time and the exact judgment/open race are controlled.
// Advance performance with timers as well as Date: unchanged observations measure monotonic elapsed time.
const parents: string[] = [];
const tickets: MeshLockTicket[] = [];
const scratch = (): string => {
  const parent = fs.mkdtempSync(path.join(os.tmpdir(), "lockhol-receipts-"));
  parents.push(parent);
  const root = path.join(parent, "mesh");
  fs.mkdirSync(root, { mode: 0o700 });
  return root;
};
const ticket = (root: string): MeshLockTicket => {
  const created = new MeshLockTicket(root, randomUUID(), 2500, { stalledHeadBypassLock: path.join(root, ".lock") });
  tickets.push(created);
  return created;
};
const receipts = (root: string): string[] => fs.readdirSync(meshLockQueueDirectory(root)).filter(name => /^\d{24}-\d+-/.test(name)).sort();
const ownerPid = (root: string): number => Number(fs.readFileSync(path.join(root, ".lock", "owner"), "utf8").split("\n")[1]);

afterEach(() => {
  delete verifiedRemoveTestHooks.beforeOpen;
  for (const created of tickets.splice(0)) created.close();
  vi.restoreAllMocks();
  vi.useRealTimers();
  for (const parent of parents.splice(0)) fs.rmSync(parent, { recursive: true, force: true });
});

describe.skipIf(process.platform !== "linux")("lockhol: deeper waiter verifies stalled-head identity", () => {
  it.each(["refreshed", "replaced"] as const)("keeps a head %s between judgment and open, then acquires healthy followers in FIFO order", async change => {
    const root = scratch();
    vi.useFakeTimers({ toFake: ["Date", "performance", "setTimeout", "clearTimeout"] });
    const head = ticket(root);
    const successor = ticket(root); // the deeper waiter's predecessor is NOT the stalled head
    expect(head.mayContend()).toBe(true);
    const headFile = path.join(meshLockQueueDirectory(root), receipts(root)[0]!);
    const judged = fs.lstatSync(headFile);
    const entered: number[] = [];
    const first = new MeshLock(root, { lockTimeoutMs: 2500 }, () => {}).withLock(() => {
      expect(ownerPid(root)).toBe(process.pid);
      entered.push(1);
    });
    const second = new MeshLock(root, { lockTimeoutMs: 2500 }, () => {}).withLock(() => {
      expect(ownerPid(root)).toBe(process.pid);
      entered.push(2);
    });
    let raced = false;
    verifiedRemoveTestHooks.beforeOpen = target => {
      if (target !== headFile || raced) return;
      raced = true;
      if (change === "refreshed") {
        const now = Date.now() / 1000;
        fs.utimesSync(headFile, now, now);
      } else {
        fs.renameSync(headFile, path.join(path.dirname(root), "saved-head"));
        fs.writeFileSync(headFile, "replacement", { mode: 0o600 });
        // Same name AND mtime: only inode identity detects this replacement.
        fs.utimesSync(headFile, judged.atimeMs / 1000, judged.mtimeMs / 1000);
      }
    };
    await vi.advanceTimersByTimeAsync(280); // 250 ms observation bound plus the 20 ms safety-net poll
    expect(raced).toBe(true);
    expect(fs.existsSync(headFile)).toBe(true);
    if (change === "refreshed") expect(fs.lstatSync(headFile).mtimeMs).not.toBe(judged.mtimeMs);
    else {
      expect(fs.lstatSync(headFile).ino).not.toBe(judged.ino);
      expect(fs.readFileSync(headFile, "utf8")).toBe("replacement");
    }
    expect(entered).toEqual([]);
    // A recovered/healthy head keeps its full place, even while younger waiters are polling its old predecessor.
    for (let elapsed = 0; elapsed < 400; elapsed += 50) {
      expect(head.mayContend()).toBe(true);
      await vi.advanceTimersByTimeAsync(50);
      expect(fs.existsSync(headFile)).toBe(true);
      expect(entered).toEqual([]);
    }
    head.close();
    expect(successor.mayContend()).toBe(true);
    await vi.advanceTimersByTimeAsync(20);
    expect(entered).toEqual([]); // healthy immediate successor stays ahead of both rescuers
    successor.close();
    await vi.advanceTimersByTimeAsync(50);
    await Promise.all([first, second]);
    expect(entered).toEqual([1, 2]);
    expect(receipts(root)).toEqual([]);
    expect(fs.existsSync(path.join(root, ".lock"))).toBe(false);
  });

  it("a replacement with unchanged mtime gets its own full observation bound", () => {
    const root = scratch();
    vi.useFakeTimers({ toFake: ["Date", "performance"] });
    ticket(root);
    ticket(root);
    const rescuer = ticket(root);
    const file = path.join(meshLockQueueDirectory(root), receipts(root)[0]!);
    const old = fs.lstatSync(file);
    expect(rescuer.mayContend()).toBe(false);
    vi.advanceTimersByTime(251);
    fs.renameSync(file, path.join(path.dirname(root), "saved-head"));
    fs.writeFileSync(file, "replacement", { mode: 0o600 });
    fs.utimesSync(file, old.atimeMs / 1000, old.mtimeMs / 1000);
    expect(fs.lstatSync(file).ino).not.toBe(old.ino);
    expect(rescuer.mayContend()).toBe(false);
    expect(fs.existsSync(file)).toBe(true);
    vi.advanceTimersByTime(250);
    expect(rescuer.mayContend()).toBe(false);
    expect(fs.existsSync(file)).toBe(true);
    vi.advanceTimersByTime(1);
    expect(rescuer.mayContend()).toBe(false); // first head reaped; the successor gets its own observation bound
    expect(fs.existsSync(file)).toBe(false);
    expect(receipts(root)).toHaveLength(2);
    vi.advanceTimersByTime(251);
    expect(rescuer.mayContend()).toBe(true);
    expect(rescuer.queued).toBe(true);
  });

  it("a canonical holder appearing at the verifier boundary vetoes reaping", () => {
    const root = scratch();
    vi.useFakeTimers({ toFake: ["Date", "performance"] });
    ticket(root);
    ticket(root);
    const rescuer = ticket(root);
    const original = receipts(root);
    const file = path.join(meshLockQueueDirectory(root), original[0]!);
    const canonical = path.join(root, ".lock");
    const owner = `legacy\n${process.pid}\n${Date.now()}\n`;
    expect(rescuer.mayContend()).toBe(false);
    let raced = false;
    verifiedRemoveTestHooks.beforeOpen = target => {
      if (target !== file || raced) return;
      raced = true;
      fs.mkdirSync(canonical, { mode: 0o700 });
      fs.writeFileSync(path.join(canonical, "owner"), owner, { mode: 0o600 });
    };
    vi.advanceTimersByTime(251);
    expect(rescuer.mayContend()).toBe(false);
    expect(raced).toBe(true);
    expect(receipts(root)).toEqual(original); // verifier moved the receipt back on its final canonical check
    expect(rescuer.queued).toBe(true);
    expect(fs.readFileSync(path.join(canonical, "owner"), "utf8")).toBe(owner);
  });
});

describe("lockhol: retained FIFO budget", () => {
  it("retains late fallback to recover a dead canonical owner behind an alive stalled head AND successor", async () => {
    const root = scratch();
    vi.useFakeTimers({ toFake: ["Date", "performance", "setTimeout", "clearTimeout"] });
    ticket(root);
    ticket(root); // both tickets name this live PID, but neither is being polled
    const original = receipts(root);
    expect(original.map(name => Number(name.split("-")[1]))).toEqual([process.pid, process.pid]);
    const canonical = path.join(root, ".lock");
    const deadPid = 999999999;
    expect(() => process.kill(deadPid, 0)).toThrowError(expect.objectContaining({ code: "ESRCH" }));
    fs.mkdirSync(canonical, { mode: 0o700 });
    const owner = `legacy-dead\n${deadPid}\n${Date.now() - 31_000}\n`;
    fs.writeFileSync(path.join(canonical, "owner"), owner, { mode: 0o600 });
    let acquiredAt = -1;
    const start = Date.now();
    const pending = new MeshLock(root, { lockTimeoutMs: 2500 }, () => {}).withLock(() => {
      acquiredAt = Date.now();
      expect(ownerPid(root)).toBe(process.pid);
      expect(receipts(root)).toEqual(original); // fallback closes only OUR advisory receipt
    }).catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(1999);
    expect(acquiredAt).toBe(-1);
    expect(fs.readFileSync(path.join(canonical, "owner"), "utf8")).toBe(owner);
    await vi.advanceTimersByTimeAsync(601);
    expect(await pending).toBeUndefined();
    expect(acquiredAt - start).toBeGreaterThanOrEqual(2000);
    expect(acquiredAt - start).toBeLessThan(2500);
    const recovered = fs.readdirSync(root).filter(name => name.startsWith(".lock.dead."));
    expect(recovered).toHaveLength(1);
    expect(fs.readFileSync(path.join(root, recovered[0]!, "owner"), "utf8")).toBe(owner);
    expect(receipts(root)).toEqual(original);
    expect(fs.existsSync(canonical)).toBe(false);
  });

  it("a deeper waiter expires at its fixed deadline behind a healthy head without probing or reaping", async () => {
    const root = scratch();
    vi.useFakeTimers({ toFake: ["Date", "performance", "setTimeout", "clearTimeout"] });
    const head = ticket(root);
    ticket(root);
    const original = receipts(root);
    const canonical = path.join(root, ".lock");
    fs.mkdirSync(canonical, { mode: 0o700 });
    const owner = `legacy\n${process.pid}\n${Date.now()}\n`;
    fs.writeFileSync(path.join(canonical, "owner"), owner, { mode: 0o600 });
    const mkdir = vi.spyOn(fs, "mkdirSync");
    const operation = vi.fn();
    let endedAt = -1;
    const start = Date.now();
    const pending = new MeshLock(root, { lockTimeoutMs: 2500 }, () => {}).withLock(operation)
      .catch((error: unknown) => error).finally(() => { endedAt = Date.now(); });
    for (let elapsed = 0; elapsed < 2600; elapsed += 50) {
      expect(head.mayContend()).toBe(true);
      await vi.advanceTimersByTimeAsync(50);
    }
    expect(await pending).toBeInstanceOf(MeshLockQueueExpiredError);
    expect(await pending).toMatchObject({ attempts: 0, waitedMs: 2500, waitersAhead: 2 });
    expect(endedAt - start).toBe(2500);
    expect(operation).not.toHaveBeenCalled();
    expect(mkdir.mock.calls.filter(([file]) => String(file) === canonical)).toEqual([]);
    expect(receipts(root)).toEqual(original);
    expect(fs.readFileSync(path.join(canonical, "owner"), "utf8")).toBe(owner);
  });
});
