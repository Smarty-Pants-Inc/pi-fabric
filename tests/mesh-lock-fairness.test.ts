import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { promisify } from "node:util";
import { buildSync } from "esbuild";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshLockTicket, meshLockQueueDirectory } from "../src/mesh/lock-queue.js";
import { MeshStore } from "../src/mesh/store.js";

const roots: string[] = [];
const scratch = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-fifo-"));
  roots.push(root);
  return root;
};
const queue = meshLockQueueDirectory;
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
  it("keeps pending and completed tickets outside every mesh-root file scan", () => {
    const root = scratch();
    fs.writeFileSync(path.join(root, "state.json"), "{}");
    const before = fs.readdirSync(root).map(name => [name, fs.readFileSync(path.join(root, name), "utf8")]);
    const ticket = new MeshLockTicket(root, randomUUID(), 7000);
    expect(path.relative(root, queue(root)).split(path.sep)[0]).toBe("..");
    expect(fs.readdirSync(root).map(name => [name, fs.readFileSync(path.join(root, name), "utf8")])).toEqual(before);
    // A queue receipt is NOT a canonical holder: the unchanged old-release mkdir
    // succeeds even while a new-release head is enqueued.
    const legacyLock = held(root);
    expect(ticket.mayContend()).toBe(true);
    expect(fs.readFileSync(path.join(legacyLock, "owner"), "utf8")).toContain("legacy\n");
    fs.rmSync(legacyLock, { recursive: true });
    ticket.close();
    expect(fs.readdirSync(queue(root))).toEqual([]);
    expect(fs.readdirSync(root).map(name => [name, fs.readFileSync(path.join(root, name), "utf8")])).toEqual(before);
  });

  it("keeps the shared admission namespace independent of process-specific TMPDIR", () => {
    const root = scratch();
    const directory = queue(root);
    const before = process.env.TMPDIR;
    try {
      process.env.TMPDIR = path.join(scratch(), "other-process-temp");
      expect(queue(root)).toBe(directory);
    } finally {
      if (before === undefined) delete process.env.TMPDIR;
      else process.env.TMPDIR = before;
    }
  });

  it.skipIf(process.platform === "win32" || process.getuid?.() === 0)("still writes to a writable mesh whose parent forbids advisory sidecars", async () => {
    const parent = scratch();
    const root = path.join(parent, "mesh");
    fs.mkdirSync(root);
    fs.chmodSync(parent, 0o500);
    try {
      const store = new MeshStore(root, 65536, 100);
      await expect(store.exclusive(() => "plain contest")).resolves.toBe("plain contest");
      expect(fs.readdirSync(parent)).toEqual(["mesh"]);
      // Only the writer census (smarty-dev#6477 L4a) lives in the root; no admission sidecar does.
      expect(fs.readdirSync(root)).toEqual([".writer-census"]);
    } finally { fs.chmodSync(parent, 0o700); }
  });

  it.each(["symlink", "file"])("ignores an unsafe %s namespace instead of treating it as a holder", async kind => {
    const root = scratch();
    const namespace = path.dirname(queue(root));
    const nativeMkdir = fs.mkdirSync.bind(fs);
    const stat = fs.lstatSync.bind(fs);
    // The shared ticket class still handles unsafe advisory namespaces for SQLite/legacy callers.
    // Inject only inspection of this namespace; never alter a real shared namespace.
    if (kind === "file") {
      vi.spyOn(fs, "mkdirSync").mockImplementation((file, options) => {
        if (String(file) === namespace) throw Object.assign(new Error("not a directory"), { code: "EEXIST" });
        return nativeMkdir(file, options);
      });
    } else {
      vi.spyOn(fs, "lstatSync").mockImplementation(((file: fs.PathLike, options?: unknown) => {
        if (String(file) === namespace) return { isDirectory: () => false };
        return stat(file, options as undefined);
      }) as typeof fs.lstatSync);
    }
    const ticket = new MeshLockTicket(root, randomUUID(), 7000);
    try {
      expect(ticket.queued).toBe(false);
      expect(ticket.mayContend()).toBe(true);
      const store = new MeshStore(root, 65536, 100);
      await expect(store.exclusive(() => "plain contest")).resolves.toBe("plain contest");
      expect(fs.readdirSync(root)).toEqual([".writer-census"]);
    } finally { ticket.close(); }
  });

  it("shares one queue across canonical mesh-root aliases and isolates other roots", () => {
    const root = scratch();
    const alias = path.join(scratch(), "alias");
    fs.symlinkSync(root, alias, process.platform === "win32" ? "junction" : "dir");
    expect(queue(alias)).toBe(queue(root));
    expect(queue(scratch())).not.toBe(queue(root));
    const head = new MeshLockTicket(root, randomUUID(), 7000);
    const tail = new MeshLockTicket(alias, randomUUID(), 7000);
    expect(tail.mayContend()).toBe(false);
    head.close();
    expect(tail.mayContend()).toBe(true);
    tail.close();
    expect(fs.readdirSync(root)).toEqual([]);
  });

  it("admits all 24 plain contenders exactly once after the native holder releases", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    const root = scratch();
    const lock = held(root);
    const store = new MeshStore(root, 65536, 100);
    const order: number[] = [];
    const waiting = Array.from({ length: 24 }, (_, index) => store.exclusive(() => order.push(index)));
    expect(order).toEqual([]);
    fs.rmSync(lock, { recursive: true });
    await vi.advanceTimersByTimeAsync(500);
    await Promise.all(waiting);
    expect([...order].sort((a, b) => a - b)).toEqual(Array.from({ length: 24 }, (_, index) => index));
    expect(vi.getTimerCount()).toBe(0);
  });

  it("non-heads inspect only the queue, not the holder or acquisition directory", () => {
    const root = scratch();
    const canonical = held(root);
    const owner = fs.readFileSync(path.join(canonical, "owner"), "utf8");
    const head = new MeshLockTicket(root, randomUUID(), 7000);
    const waiter = new MeshLockTicket(root, randomUUID(), 7000);
    const read = vi.spyOn(fs, "readFileSync");
    const mkdir = vi.spyOn(fs, "mkdirSync");
    try {
      for (let index = 0; index < 10; index++) expect(waiter.mayContend()).toBe(false);
      expect(read.mock.calls.some(([file]) => String(file) === path.join(canonical, "owner"))).toBe(false);
      expect(mkdir.mock.calls.some(([file]) => String(file) === canonical)).toBe(false);
      head.close();
      expect(waiter.mayContend()).toBe(true);
      expect(fs.readFileSync(path.join(canonical, "owner"), "utf8")).toBe(owner);
    } finally { head.close(); waiter.close(); }
  });

  it.each(["dead", "stale"])("removes an oldest %s ticket without removing an actual lock", (kind) => {
    const root = scratch();
    const stale = oldTicket(root, kind === "dead" ? 999999999 : process.pid, kind === "stale" ? 31000 : 0);
    const canonical = held(root);
    const owner = fs.readFileSync(path.join(canonical, "owner"), "utf8");
    const ticket = new MeshLockTicket(root, randomUUID(), 7000);
    try {
      expect(ticket.mayContend()).toBe(true);
      expect(fs.existsSync(stale)).toBe(false);
      expect(fs.readFileSync(path.join(canonical, "owner"), "utf8")).toBe(owner);
    } finally { ticket.close(); }
    expect(fs.readdirSync(queue(root))).toEqual([]);
  });

  it("falls back after 80% of the budget even behind a live stalled ticket", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    const root = scratch();
    const first = oldTicket(root, process.pid);
    const ticket = new MeshLockTicket(root, randomUUID(), 100);
    try {
      expect(ticket.mayContend()).toBe(false);
      await vi.advanceTimersByTimeAsync(79);
      expect(ticket.mayContend()).toBe(false);
      await vi.advanceTimersByTimeAsync(1);
      expect(ticket.mayContend()).toBe(true);
      expect(ticket.queued).toBe(false);
      expect(fs.existsSync(first)).toBe(true); // admission fallback never reaps a live receipt
    } finally { ticket.close(); }
  });

  it("a missing own ticket cannot strand a waiter", async () => {
    const root = scratch();
    const ticket = new MeshLockTicket(root, randomUUID(), 7000);
    for (const name of fs.readdirSync(queue(root))) fs.unlinkSync(path.join(queue(root), name));
    expect(ticket.mayContend()).toBe(true);
    ticket.close();
  });

  it("preserves the native holder on cancellation and typed timeout, and releases after operation exceptions", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    const root = scratch();
    const lock = held(root);
    const abort = new AbortController();
    const store = new MeshStore(root, 65536, 100, { lockTimeoutMs: 100, writeSignal: abort.signal });
    const pending = store.exclusive(() => {}).catch(error => error);
    abort.abort(new Error("cancel"));
    expect(await pending).toMatchObject({ message: "cancel" });
    expect(fs.existsSync(lock)).toBe(true);
    const second = new MeshStore(root, 65536, 100, { lockTimeoutMs: 100 });
    const timed = second.exclusive(() => {}).catch(error => error);
    await vi.advanceTimersByTimeAsync(100);
    expect(await timed).toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    expect(fs.existsSync(lock)).toBe(true);
    fs.rmSync(lock, { recursive: true });
    await expect(second.exclusive(() => { throw new Error("operation"); })).rejects.toThrow("operation");
    expect(fs.existsSync(lock)).toBe(false);
  });

  it.each([0, 4])("stress harness: 24 processes under CPU load, including %s legacy contenders", async (legacyN) => {
    const output = path.join(scratch(), "store.mjs");
    buildSync({ stdin: { contents: 'export { MeshStore } from "./src/mesh/store.ts"; export { meshLockQueueDirectory } from "./src/mesh/lock-queue.ts";', resolveDir: process.cwd() }, bundle: true, platform: "node", format: "esm", outfile: output });
    const { stdout } = await promisify(execFile)(process.execPath, ["scripts/stress-mesh-lock.mjs", `--module=${output}`,
      "--n=24", "--rounds=2", "--load=2", "--cpuMs=1", `--legacyN=${legacyN}`,
      `--legacyModule=${path.resolve("tests/fixtures/mesh-legacy-contender.mjs")}`],
      { timeout: 85000, env: { ...process.env, PI_FABRIC_LOCK_STATS: "1" } });
    const report = JSON.parse(stdout);
    expect(report).toMatchObject({ n: 24, acquisitions: 48, timeouts: 0, mutualExclusion: true, legacyN });
    // Every current contender recorded exactly its own acquisitions (legacy contenders record none).
    expect(report.lockStats).toMatchObject({ files: 24 - legacyN, acquisitions: 48 - 2 * legacyN, timeouts: 0 });
    expect(report.waitMs.p50).toBeGreaterThanOrEqual(0);
    expect(report.waitMs.max).toBeGreaterThanOrEqual(report.waitMs.p99);
  }, 90000);
});
