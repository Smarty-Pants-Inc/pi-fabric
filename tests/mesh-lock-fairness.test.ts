import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { promisify } from "node:util";
import { buildSync } from "esbuild";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshLockTicket } from "../src/mesh/lock-queue.js";
import { MeshStore } from "../src/mesh/store.js";

const roots: string[] = [];
const scratch = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-fifo-"));
  roots.push(root);
  return root;
};
const queue = (root: string) => path.join(root, ".lock.q");
const held = (root: string) => {
  const lock = path.join(root, ".lock");
  fs.mkdirSync(lock);
  fs.writeFileSync(path.join(lock, "owner"), `legacy\n${process.pid}\n${Date.now()}\n`);
  return lock;
};
const oldTicket = (root: string, pid: number, age = 0) => {
  fs.mkdirSync(queue(root), { recursive: true });
  const file = path.join(queue(root), `${"0".repeat(24)}-${pid}-${randomUUID()}`);
  fs.writeFileSync(file, "");
  if (age) fs.utimesSync(file, new Date(Date.now() - age), new Date(Date.now() - age));
  return file;
};
afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("bounded FIFO mesh admission", () => {
  it("admits 24 enqueued waiters in ticket order, not repeated-winner order", async () => {
    // Leave the host monotonic clock real: fake hrtime gives all tickets the same age.
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    const root = scratch();
    const lock = held(root);
    const store = new MeshStore(root, 65536, 100);
    const order: number[] = [];
    const waiting = Array.from({ length: 24 }, (_, index) => store.exclusive(() => order.push(index)));
    expect(fs.readdirSync(queue(root))).toHaveLength(24);
    fs.rmSync(lock, { recursive: true });
    await vi.advanceTimersByTimeAsync(500);
    await Promise.all(waiting);
    expect(order).toEqual(Array.from({ length: 24 }, (_, index) => index));
    expect(fs.readdirSync(queue(root))).toEqual([]);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("non-heads inspect only the queue, not the holder or acquisition directory", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    const root = scratch();
    const head = new MeshLockTicket(root, randomUUID(), 7000);
    const store = new MeshStore(root, 65536, 100);
    const read = vi.spyOn(fs, "readFileSync");
    const mkdir = vi.spyOn(fs, "mkdirSync");
    const operation = vi.fn();
    const pending = store.exclusive(operation);
    await vi.advanceTimersByTimeAsync(100);
    expect(operation).not.toHaveBeenCalled();
    expect(read.mock.calls.some(([file]) => String(file).endsWith(".lock/owner"))).toBe(false);
    expect(mkdir.mock.calls.some(([file]) => String(file) === path.join(root, ".lock"))).toBe(false);
    head.close();
    await vi.advanceTimersByTimeAsync(20);
    await pending;
    expect(operation).toHaveBeenCalledOnce();
  });

  it.each(["dead", "stale"])("removes an oldest %s ticket without removing an actual lock", async (kind) => {
    const root = scratch();
    const stale = oldTicket(root, kind === "dead" ? 999999999 : process.pid, kind === "stale" ? 31000 : 0);
    const store = new MeshStore(root, 65536, 100);
    await expect(store.exclusive(() => "done")).resolves.toBe("done");
    expect(fs.existsSync(stale)).toBe(false);
    expect(fs.readdirSync(queue(root))).toEqual([]);
  });

  it("falls back after 80% of the budget even behind a live stalled ticket", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    const root = scratch();
    const first = oldTicket(root, process.pid);
    const store = new MeshStore(root, 65536, 100, { lockTimeoutMs: 100 });
    const operation = vi.fn(() => "fallback");
    const pending = store.exclusive(operation);
    await vi.advanceTimersByTimeAsync(79);
    expect(operation).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1);
    await expect(pending).resolves.toBe("fallback");
    expect(operation).toHaveBeenCalledOnce();
    expect(fs.existsSync(first)).toBe(true); // admission fallback never reaps a live receipt
  });

  it("a missing own ticket cannot strand a waiter", async () => {
    const root = scratch();
    const ticket = new MeshLockTicket(root, randomUUID(), 7000);
    for (const name of fs.readdirSync(queue(root))) fs.unlinkSync(path.join(queue(root), name));
    expect(ticket.mayContend()).toBe(true);
    ticket.close();
  });

  it("cleans receipts on cancellation, operation exceptions and typed timeouts", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    const root = scratch();
    const lock = held(root);
    const abort = new AbortController();
    const store = new MeshStore(root, 65536, 100, { lockTimeoutMs: 100, writeSignal: abort.signal });
    const pending = store.exclusive(() => {}).catch(error => error);
    abort.abort(new Error("cancel"));
    expect(await pending).toMatchObject({ message: "cancel" });
    expect(fs.readdirSync(queue(root))).toEqual([]);
    const second = new MeshStore(root, 65536, 100, { lockTimeoutMs: 100 });
    const timed = second.exclusive(() => {}).catch(error => error);
    await vi.advanceTimersByTimeAsync(100);
    expect(await timed).toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    expect(fs.readdirSync(queue(root))).toEqual([]);
    expect(fs.existsSync(lock)).toBe(true);
    fs.rmSync(lock, { recursive: true });
    await expect(second.exclusive(() => { throw new Error("operation"); })).rejects.toThrow("operation");
    expect(fs.readdirSync(queue(root))).toEqual([]);
    expect(fs.existsSync(lock)).toBe(false);
  });

  it.each([0, 4])("stress harness: 24 processes under CPU load, including %s legacy contenders", async (legacyN) => {
    const output = path.join(scratch(), "store.mjs");
    buildSync({ entryPoints: ["src/mesh/store.ts"], bundle: true, platform: "node", format: "esm", outfile: output });
    const { stdout } = await promisify(execFile)(process.execPath, ["scripts/stress-mesh-lock.mjs", `--module=${output}`,
      "--n=24", "--rounds=2", "--load=2", "--cpuMs=1", `--legacyN=${legacyN}`,
      `--legacyModule=${path.resolve("tests/fixtures/mesh-legacy-contender.mjs")}`], { timeout: 85000 });
    const report = JSON.parse(stdout);
    expect(report).toMatchObject({ n: 24, acquisitions: 48, timeouts: 0, mutualExclusion: true, remainingTickets: 0, legacyN });
    expect(report.waitMs.p50).toBeGreaterThanOrEqual(0);
    expect(report.waitMs.max).toBeGreaterThanOrEqual(report.waitMs.p99);
  }, 90000);
});
