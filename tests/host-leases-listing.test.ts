import { EventEmitter } from "node:events";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { hostLeaseCacheGeneration, hostLeaseGeneration, hostLeasePath, hostLeasePredecessor, readHostLease, readHostLeases, removeHostLease, writeHostLease, type FabricHostLease } from "../src/topology/host-leases.js";

describe("strict host lease authority reads", () => {
  const roots: string[] = [];
  afterEach(() => {
    vi.restoreAllMocks();
    for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  });
  it("distinguishes ENOENT from unreadable/invalid current bytes without falling back to a warmed peer cache", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-lease-strict-"));
    roots.push(root);
    const id = "host:strict", lease = { id, rootId: id, identityId: id, updatedAt: 1000, expiresAt: 61000 };
    expect(hostLeasePredecessor(root, id)).toBeUndefined(); // legitimate missing-file creation
    writeHostLease(root, lease);
    expect(readHostLease(root, id)).toEqual(lease); // warm the lenient peer cache
    const target = hostLeasePath(root, id), read = fs.readFileSync.bind(fs);
    const fault = vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...args: unknown[]) => {
      if (String(file) === target) throw Object.assign(new Error("unreadable current bytes"), { code: "EACCES" });
      return Reflect.apply(read, fs, [file, ...args]);
    }) as typeof fs.readFileSync);
    expect(() => hostLeasePredecessor(root, id)).toThrow("unreadable current bytes");
    fault.mockRestore();
    fs.writeFileSync(target, "not a lease");
    expect(() => hostLeasePredecessor(root, id)).toThrow("Invalid host lease");
    writeHostLease(root, { ...lease, incarnationToken: "native-current" });
    expect(hostLeasePredecessor(root, id)?.incarnationToken).toBe("native-current");
  });
});

// smarty-dev#7978 follow-on: an inotify cache must avoid directory enumeration and per-file stats on clean reads.
describe.skipIf(process.platform === "win32")("host lease watcher cache", () => {
  const roots: string[] = [];
  const watchers: EventEmitter[] = [];
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
    watchers.length = 0;
    for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  });
  const lease = (id: string, updatedAt: number): FabricHostLease => ({
    id, rootId: id, identityId: id, updatedAt, expiresAt: updatedAt + 60_000,
  });
  const setup = () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-lease-watch-"));
    roots.push(root);
    for (let index = 0; index < 3; index++) writeHostLease(root, lease(`host:${index}`, 1_000));
    const dir = path.join(root, "host-leases");
    const watcherStart = watchers.length;
    const readdir = vi.spyOn(fs, "readdirSync");
    const stat = vi.spyOn(fs, "statSync");
    const watch = vi.spyOn(fs, "watch").mockImplementation(((() => {
      const watcher = new EventEmitter();
      Object.assign(watcher, { close: vi.fn(), ref: () => watcher, unref: () => watcher });
      watchers.push(watcher);
      return watcher as unknown as fs.FSWatcher;
    }) as typeof fs.watch));
    const leaseListings = () => readdir.mock.calls.filter(([at]) => String(at) === dir).length;
    const directoryStats = () => stat.mock.calls.filter(([at]) => String(at) === dir).length;
    const leaseStats = () => stat.mock.calls.filter(([at]) => String(at).startsWith(`${dir}${path.sep}`) && String(at).endsWith(".json")).length;
    const watcher = (index = 0) => watchers[watcherStart + index]!;
    const notify = (name: string, index = 0) => watcher(index).emit("change", "rename", name);
    return { root, dir, readdir, stat, watch, leaseListings, directoryStats, leaseStats, watcher, notify };
  };

  it("does zero readdir and per-file stat work across repeated unchanged reads", () => {
    const { root, readdir, stat, leaseListings, directoryStats, leaseStats } = setup();
    expect([...readHostLeases(root).keys()].sort()).toEqual(["host:0", "host:1", "host:2"]);
    expect(leaseListings()).toBe(1);
    expect(leaseStats()).toBe(3);
    readdir.mockClear();
    stat.mockClear();

    for (let index = 0; index < 10; index++) expect(readHostLeases(root).size).toBe(3);
    expect(leaseListings()).toBe(0);
    expect(directoryStats()).toBe(10); // one cheap stamp check per read
    expect(leaseStats()).toBe(0);
  });

  it("exposes a no-I/O cache generation for locked snapshot validation", () => {
    const { root, stat, readdir } = setup();
    readHostLeases(root);
    stat.mockClear();
    readdir.mockClear();
    const before = hostLeaseCacheGeneration(root);
    expect(hostLeaseCacheGeneration(root)).toBe(before);
    expect(stat).not.toHaveBeenCalled();
    expect(readdir).not.toHaveBeenCalled();

    writeHostLease(root, lease("host:0", 3_000));
    expect(hostLeaseCacheGeneration(root)).toBeGreaterThan(before);
  });

  it("keeps repeated absent-directory observations stable but invalidates a removed directory", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-lease-watch-absent-"));
    roots.push(root);
    expect(readHostLeases(root).size).toBe(0);
    const absent = hostLeaseGeneration(root, "host:pending");
    readHostLeases(root);
    readHostLeases(root);
    expect(hostLeaseGeneration(root, "host:pending")).toBe(absent);

    writeHostLease(root, lease("host:pending", 1_000));
    expect(readHostLeases(root).get("host:pending")?.updatedAt).toBe(1_000);
    const present = hostLeaseGeneration(root, "host:pending");
    fs.rmSync(path.join(root, "host-leases"), { recursive: true, force: true });
    expect(readHostLeases(root).size).toBe(0);
    expect(hostLeaseGeneration(root, "host:pending")).not.toBe(present);
    const removed = hostLeaseGeneration(root, "host:pending");
    expect(readHostLeases(root).size).toBe(0);
    expect(hostLeaseGeneration(root, "host:pending")).toBe(removed);
  });

  it("refreshes only a new, renewed, or removed lease after its filename event", () => {
    const { root, notify } = setup();
    readHostLeases(root);
    const added = hostLeasePath(root, "host:new");
    writeHostLease(root, lease("host:new", 2_000));
    notify(path.basename(added));
    expect(readHostLeases(root).get("host:new")?.updatedAt).toBe(2_000);

    const renewed = hostLeasePath(root, "host:0");
    writeHostLease(root, lease("host:0", 3_000));
    notify(path.basename(renewed));
    expect(readHostLeases(root).get("host:0")?.updatedAt).toBe(3_000);

    const removed = hostLeasePath(root, "host:1");
    removeHostLease(root, "host:1");
    notify(path.basename(removed));
    expect(readHostLeases(root).has("host:1")).toBe(false);
    expect([...readHostLeases(root).keys()].sort()).toEqual(["host:0", "host:2", "host:new"]);
  });

  it("observes an actual atomic lease rename through fs.watch", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-lease-watch-real-"));
    roots.push(root);
    writeHostLease(root, lease("host:real", 1_000));
    expect(readHostLeases(root).get("host:real")?.updatedAt).toBe(1_000);
    const dir = path.join(root, "host-leases");
    const name = path.basename(hostLeasePath(root, "host:real"));
    const monitor = fs.watch(dir, { persistent: false });
    try {
      const finalRename = new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => reject(new Error("final lease rename watcher event timed out")), 5_000);
        monitor.on("change", (event, filename) => {
          if (event === "rename" && filename?.toString() === name) {
            clearTimeout(timeout);
            resolve();
          }
        });
      });
      writeHostLease(root, lease("host:real", 2_000));
      await finalRename;
      expect(readHostLeases(root).get("host:real")?.updatedAt).toBe(2_000);
    } finally {
      monitor.close();
    }
  });

  it("catches a dropped event through the cheap lease-directory stamp gate", () => {
    const { root, leaseListings } = setup();
    expect(readHostLeases(root).get("host:0")?.updatedAt).toBe(1_000);
    writeHostLease(root, lease("host:0", 4_000)); // deliberately omit the watcher event

    expect(readHostLeases(root).get("host:0")?.updatedAt).toBe(4_000);
    expect(leaseListings()).toBe(2);
  });

  it("ignores atomic temporary names while the directory stamp catches a missing final-name event", () => {
    const { root, notify, leaseListings } = setup();
    readHostLeases(root);
    writeHostLease(root, lease("host:new", 2_500));
    notify(".host-lease-write-7f3a.tmp");

    expect(readHostLeases(root).get("host:new")?.updatedAt).toBe(2_500);
    expect(leaseListings()).toBe(2); // the temp entry is not stat'ed; the stamp forces a final directory scan
  });

  it("treats a null-filename overflow as dirty, full-scans, then re-arms", () => {
    const { root, watch, watcher, readdir, stat } = setup();
    readHostLeases(root);
    readdir.mockClear();
    stat.mockClear();
    writeHostLease(root, lease("host:0", 5_000));
    watcher().emit("change", "rename", null); // IN_Q_OVERFLOW is surfaced this way by Node

    expect(readHostLeases(root).get("host:0")?.updatedAt).toBe(5_000);
    expect(readdir.mock.calls.filter(([at]) => String(at) === path.join(root, "host-leases"))).toHaveLength(1);
    expect(stat.mock.calls.filter(([at]) => String(at).startsWith(`${path.join(root, "host-leases")}${path.sep}`) && String(at).endsWith(".json"))).toHaveLength(3);
    expect(watch).toHaveBeenCalledTimes(2); // initial watcher plus re-arm after the full scan
  });

  it("full-scans after a watcher error and re-arms after the scan", () => {
    const { root, watch, watcher, readdir, stat } = setup();
    readHostLeases(root);
    readdir.mockClear();
    stat.mockClear();
    watch.mockImplementationOnce(() => { throw new Error("watch unavailable during re-arm"); });
    writeHostLease(root, lease("host:0", 5_000));
    watcher().emit("error", new Error("inotify failed"));

    expect(readHostLeases(root).get("host:0")?.updatedAt).toBe(5_000);
    expect(watch).toHaveBeenCalledTimes(2); // initial watcher, then one re-arm after the full scan
    expect(readdir.mock.calls.filter(([at]) => String(at) === path.join(root, "host-leases"))).toHaveLength(1);
    expect(stat.mock.calls.filter(([at]) => String(at).startsWith(`${path.join(root, "host-leases")}${path.sep}`) && String(at).endsWith(".json"))).toHaveLength(3);
  });

  it("keeps the census problems path on a full readdir and per-file-stat scan", () => {
    const { root, readdir, stat, leaseListings, leaseStats } = setup();
    readHostLeases(root);
    readdir.mockClear();
    stat.mockClear();
    const first: string[] = [];
    const second: string[] = [];
    expect(readHostLeases(root, first).size).toBe(3);
    expect(readHostLeases(root, second).size).toBe(3);
    expect(first).toEqual([]);
    expect(second).toEqual([]);
    expect(leaseListings()).toBe(2);
    expect(leaseStats()).toBe(6);
  });
});
