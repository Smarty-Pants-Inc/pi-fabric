import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshLockTimeoutError } from "../src/core/atomic-write.js";
import { MeshLockTicket, meshLockQueueDirectory } from "../src/mesh/lock-queue.js";
import { MeshLock, MeshLockQueueExpiredError } from "../src/mesh/mesh-lock.js";

// Incident #19677: a dead FIFO receipt must not hide a dead canonical owner from the existing recovery path.
// Keep reason: data-safety counterexamples exercise live canonical ownership and FIFO through real lock admission.
const DEAD_PID = 999999999;
const parents: string[] = [];
const tickets: MeshLockTicket[] = [];
const scratch = (): string => {
  const parent = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-dead-predecessor-"));
  parents.push(parent);
  const root = path.join(parent, "mesh");
  fs.mkdirSync(root, { mode: 0o700 });
  return root;
};
const receipts = (root: string): string[] => fs.readdirSync(meshLockQueueDirectory(root)).filter(name => /^\d{24}-\d+-/.test(name)).sort();
const deadReceipt = (root: string, first = true): string => {
  const queue = meshLockQueueDirectory(root);
  fs.mkdirSync(queue, { recursive: true, mode: 0o700 });
  const order = first ? "0".repeat(24) : process.hrtime.bigint().toString().padStart(24, "0");
  const file = path.join(queue, `${order}-${DEAD_PID}-${randomUUID()}`);
  fs.writeFileSync(file, "", { mode: 0o600 });
  return file;
};
const ticket = (root: string, budget: number): MeshLockTicket => {
  const created = new MeshLockTicket(root, randomUUID(), budget, { stalledHeadBypassLock: path.join(root, ".lock") });
  tickets.push(created);
  return created;
};
const hold = (root: string, pid: number): { canonical: string; owner: string; ino: number } => {
  const canonical = path.join(root, ".lock");
  const owner = `${randomUUID()}\n${pid}\n${Date.now()}\n`;
  fs.mkdirSync(canonical);
  fs.writeFileSync(path.join(canonical, "owner"), owner);
  return { canonical, owner, ino: fs.lstatSync(canonical).ino };
};
afterEach(() => {
  for (const created of tickets.splice(0)) created.close();
  vi.restoreAllMocks();
  vi.useRealTimers();
  for (const parent of parents.splice(0)) fs.rmSync(parent, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
});

describe("mesh FIFO recovery behind an ESRCH-proven-dead predecessor", () => {
  it("reaches unchanged dead-canonical recovery immediately instead of waiting for the 8-9 s fallback", async () => {
    vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"] });
    const root = scratch();
    const { canonical, owner } = hold(root, DEAD_PID);
    const receipt = deadReceipt(root);
    const abort = new AbortController();
    const lock = new MeshLock(root, { lockTimeoutMs: 10_000, writeSignal: abort.signal }, () => undefined);
    const started = Date.now();
    const operation = vi.fn(() => {
      expect(fs.existsSync(receipt)).toBe(false);
      expect(fs.readFileSync(path.join(canonical, "owner"), "utf8").split("\n")[1]).toBe(String(process.pid));
      return "recovered";
    });
    let settled = false;
    const pending = lock.withLock(operation).catch((error: unknown) => error).finally(() => { settled = true; });
    try {
      await vi.advanceTimersByTimeAsync(0);
      expect(settled).toBe(true); // RED on the old guard: no canonical probe can occur while the receipt remains.
      expect(await pending).toBe("recovered");
      expect(Date.now() - started).toBe(0);
      expect(operation).toHaveBeenCalledOnce();
      const recovered = fs.readdirSync(root).filter(name => name.startsWith(".lock.dead."));
      expect(recovered).toHaveLength(1);
      expect(fs.readFileSync(path.join(root, recovered[0]!, "owner"), "utf8")).toBe(owner);
      expect(fs.existsSync(canonical)).toBe(false);
      expect(receipts(root)).toEqual([]);
      expect(vi.getTimerCount()).toBe(0);
    } finally {
      abort.abort(new Error("test cleanup"));
      await vi.advanceTimersByTimeAsync(0);
      await pending;
    }
  });

  it("removing a dead receipt never authorizes taking a live canonical holder", async () => {
    vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"] });
    const root = scratch();
    const { canonical, owner, ino } = hold(root, process.pid);
    const receipt = deadReceipt(root);
    const operation = vi.fn();
    const lock = new MeshLock(root, { lockTimeoutMs: 2_000 }, () => undefined);
    const pending = lock.withLock(operation).catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(2_001);
    const result = await pending;
    expect(result).toBeInstanceOf(MeshLockTimeoutError);
    expect(result).not.toBeInstanceOf(MeshLockQueueExpiredError);
    expect((result as MeshLockTimeoutError).attempts).toBeGreaterThan(1);
    expect(operation).not.toHaveBeenCalled();
    expect(fs.existsSync(receipt)).toBe(false);
    expect(fs.lstatSync(canonical).ino).toBe(ino);
    expect(fs.readFileSync(path.join(canonical, "owner"), "utf8")).toBe(owner);
    expect(fs.readdirSync(root).filter(name => name.startsWith(".lock.dead."))).toEqual([]);
    expect(receipts(root)).toEqual([]);
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each([undefined, "EPERM", "EIO"])("retains a live-aged canonical holder's receipt (probe error: %s)", (probeError) => {
    vi.useFakeTimers({ toFake: ["Date"] });
    const root = scratch();
    const { canonical, owner, ino } = hold(root, process.pid);
    ticket(root, 10_000);
    const head = path.join(meshLockQueueDirectory(root), receipts(root)[0]!);
    const old = (Date.now() - 31_000) / 1000;
    fs.utimesSync(head, old, old);
    const headIno = fs.lstatSync(head).ino;
    const follower = ticket(root, 10_000);
    if (probeError) {
      const kill = process.kill;
      vi.spyOn(process, "kill").mockImplementation((pid, signal) => {
        if (pid === process.pid) throw Object.assign(new Error("liveness unknown"), { code: probeError });
        return kill(pid, signal);
      });
    }
    expect(follower.mayContend()).toBe(false);
    vi.advanceTimersByTime(2_001);
    expect(follower.mayContend()).toBe(false);
    expect(follower.queued).toBe(true);
    expect(fs.lstatSync(head).ino).toBe(headIno);
    expect(receipts(root)).toHaveLength(2);
    expect(fs.lstatSync(canonical).ino).toBe(ino);
    expect(fs.readFileSync(path.join(canonical, "owner"), "utf8")).toBe(owner);
  });

  it("removes a dead middle receipt without passing a live progressing head, even past the follower's fallback", () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    const root = scratch();
    const { canonical, owner, ino } = hold(root, process.pid);
    const head = ticket(root, 10_000);
    expect(head.mayContend()).toBe(true);
    const receipt = deadReceipt(root, false);
    const follower = ticket(root, 2_000);
    const initial = receipts(root);
    expect(initial).toHaveLength(3);
    expect(follower.mayContend()).toBe(false);
    expect(fs.existsSync(receipt)).toBe(false);
    for (let elapsed = 0; elapsed < 2_000; elapsed += 50) {
      vi.advanceTimersByTime(50);
      expect(head.mayContend()).toBe(true);
      expect(follower.mayContend()).toBe(false);
    }
    expect(follower.queued).toBe(true);
    expect(receipts(root)).toEqual(initial.filter(name => name !== path.basename(receipt)));
    expect(fs.lstatSync(canonical).ino).toBe(ino);
    expect(fs.readFileSync(path.join(canonical, "owner"), "utf8")).toBe(owner);
    head.close();
    expect(follower.mayContend()).toBe(true); // only its live predecessor's release promotes it
  });
});
