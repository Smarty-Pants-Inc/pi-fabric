import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { promisify } from "node:util";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshLockTicket, meshLockQueueDirectory } from "../src/mesh/lock-queue.js";

// Fair mesh lock admission (smarty-dev#6477 L0b): wake-on-handoff, predecessor-only waiting,
// and no fallback barging while the queue head is alive and making progress.
const roots: string[] = [];
const tickets: MeshLockTicket[] = [];
const scratch = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-fair-"));
  roots.push(root);
  return root;
};
const ticket = (root: string, budgetMs: number) => {
  const created = new MeshLockTicket(root, randomUUID(), budgetMs);
  tickets.push(created);
  return created;
};
const names = (root: string) => fs.readdirSync(meshLockQueueDirectory(root)).sort();
const mtime = (root: string, name: string) => fs.statSync(path.join(meshLockQueueDirectory(root), name)).mtimeMs;
afterEach(() => {
  for (const created of tickets.splice(0)) created.close();
  vi.useRealTimers();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("fair mesh lock admission", () => {
  it("wakes a waiter as soon as its predecessor's receipt goes, not at its next poll", async () => {
    const root = scratch();
    const head = ticket(root, 10_000);
    const waiter = ticket(root, 10_000);
    expect(head.mayContend()).toBe(true);
    expect(waiter.mayContend()).toBe(false);
    const started = performance.now();
    const woken = waiter.wait(5000);
    setTimeout(() => head.close(), 20);
    await woken;
    expect(performance.now() - started).toBeLessThan(2500);
    expect(waiter.mayContend()).toBe(true);
  });

  it("wakes the head when .lock is released", async () => {
    const root = scratch();
    const lock = path.join(root, ".lock");
    fs.mkdirSync(lock);
    const head = ticket(root, 10_000);
    expect(head.mayContend()).toBe(true);
    const started = performance.now();
    const woken = head.wait(5000);
    setTimeout(() => fs.renameSync(lock, `${lock}.released.x`), 20);
    await woken;
    expect(performance.now() - started).toBeLessThan(2500);
  });

  it("non-heads stat only their predecessor and list the queue only when it goes", () => {
    const root = scratch();
    const head = ticket(root, 10_000);
    const waiter = ticket(root, 10_000);
    expect(waiter.mayContend()).toBe(false);
    const readdir = vi.spyOn(fs, "readdirSync");
    for (let i = 0; i < 10; i++) expect(waiter.mayContend()).toBe(false);
    expect(readdir).not.toHaveBeenCalled();
    head.close();
    expect(waiter.mayContend()).toBe(true);
    expect(readdir).toHaveBeenCalledOnce();
    readdir.mockRestore();
  });

  it("admits strictly in receipt order and reaps a dead receipt in the middle", () => {
    const root = scratch();
    const first = ticket(root, 10_000);
    const dead = path.join(meshLockQueueDirectory(root), `${process.hrtime.bigint().toString().padStart(24, "0")}-999999999-${randomUUID()}`);
    fs.writeFileSync(dead, "");
    const second = ticket(root, 10_000);
    const third = ticket(root, 10_000);
    expect([first.mayContend(), second.mayContend(), third.mayContend()]).toEqual([true, false, false]);
    expect(fs.existsSync(dead)).toBe(false);
    first.close();
    expect([third.mayContend(), second.mayContend()]).toEqual([false, true]);
    second.close();
    expect(third.mayContend()).toBe(true);
  });

  it("does not fall back while the head is alive and making progress, even past 80% of the budget", () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    const root = scratch();
    const head = ticket(root, 10_000);
    const waiter = ticket(root, 2000); // fallback point 1600 ms, stall bound 200 ms
    expect(head.mayContend()).toBe(true);
    for (let elapsed = 0; elapsed < 2000; elapsed += 20) {
      vi.advanceTimersByTime(20);
      expect(head.mayContend()).toBe(true); // the head probes, and its receipt heartbeat stays fresh
      expect(waiter.mayContend()).toBe(false);
    }
    expect(waiter.queued).toBe(true);
  });

  it("falls back past 80% of the budget when the live head stops making progress", () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    const root = scratch();
    const head = ticket(root, 10_000);
    const waiter = ticket(root, 2000);
    expect(head.mayContend()).toBe(true);
    vi.advanceTimersByTime(1500);
    expect(waiter.mayContend()).toBe(false); // before the fallback point: never
    vi.advanceTimersByTime(200);
    // Past 1600 ms: the head's heartbeat is 1700 ms old, but this is our first sight of it.
    expect(waiter.mayContend()).toBe(false);
    vi.advanceTimersByTime(250);
    // Still the same head and no heartbeat beyond the 200 ms stall bound: plain contest.
    expect(waiter.mayContend()).toBe(true);
    expect(waiter.queued).toBe(false);
    expect(names(root)).toHaveLength(1); // the live stalled head's receipt is never reaped early
  });

  it("short try budgets keep the unconditional 80% fallback", () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    const root = scratch();
    const head = ticket(root, 10_000);
    const tryer = ticket(root, 50);
    expect(head.mayContend()).toBe(true);
    expect(tryer.mayContend()).toBe(false);
    vi.advanceTimersByTime(40);
    expect(head.mayContend()).toBe(true);
    expect(tryer.mayContend()).toBe(true);
  });

  it("judges a head that has just taken over by first sight, not by its old creation time", () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    const root = scratch();
    const head = ticket(root, 10_000);
    const next = ticket(root, 10_000);
    const waiter = ticket(root, 2000);
    const successor = names(root)[1]!;
    expect(head.mayContend()).toBe(true);
    for (let elapsed = 0; elapsed < 1700; elapsed += 20) {
      vi.advanceTimersByTime(20);
      expect(head.mayContend()).toBe(true);
    }
    head.close(); // handoff: the successor's receipt is 1700 ms old and it has not probed yet
    expect(Date.now() - mtime(root, successor)).toBeGreaterThan(1600);
    expect(waiter.mayContend()).toBe(false);
    vi.advanceTimersByTime(250); // the new head never makes progress: now it is stalled
    expect(waiter.mayContend()).toBe(true);
    expect(next.queued).toBe(true);
  });

  it("an aborted wait rejects and leaves no timer", async () => {
    const root = scratch();
    ticket(root, 10_000);
    const waiter = ticket(root, 10_000);
    expect(waiter.mayContend()).toBe(false);
    const abort = new AbortController();
    const pending = waiter.wait(60_000, abort.signal).catch(error => error);
    abort.abort(new Error("cancel"));
    expect(await pending).toMatchObject({ message: "cancel" });
  });

  it.skipIf(process.platform === "win32")("multi-process: no queued Main starves or times out against legacy bargers", async () => {
    const { stdout } = await promisify(execFile)(process.execPath, ["scripts/benchmark-mesh-lock-fairness.mjs",
      "--n=12", "--legacy=2", "--try=2", "--seconds=2", "--budgetMs=10000", "--pLong=0"], { timeout: 85_000 });
    const report = JSON.parse(stdout);
    expect(report.timeouts.queued).toBe(0);
    expect(report.source.fallback).toBe(0);
    expect(report.source.queueHead).toBeGreaterThan(report.source.legacyBarge);
    expect(report.queuedWaitMs.max).toBeLessThan(8000);
  }, 90_000);
});
