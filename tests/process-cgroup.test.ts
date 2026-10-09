import fs from "node:fs";
import { EventEmitter } from "node:events";
import type { ChildProcess } from "node:child_process";
import { afterEach, describe, expect, it, vi } from "vitest";
import { cgroupCustody, executionCgroups, scopePath } from "../src/process-cgroup.js";
import { executionGroup } from "../src/worker/execution-group.js";

afterEach(() => { vi.restoreAllMocks(); vi.useRealTimers(); });
describe.skipIf(process.platform !== "linux")("cgroup execution custody", () => {
  const directory = "/sys/fs/cgroup/user.slice/app.slice/fabric-execution-test.scope";
  const setup = () => {
    let pids = [100, 101], populated = true, missing = false, inode = 1, unreadable = false;
    const births = new Map([[100, "leader"], [101, "orphan"]]);
    const watcher = Object.assign(new EventEmitter(), { close: vi.fn() });
    let event!: () => void;
    const watch = vi.spyOn(fs, "watch").mockImplementation(((_file: fs.PathLike, _options: unknown, listener: unknown) => {
      event = () => (listener as (...args: unknown[]) => void)("change", "cgroup.events");
      return watcher as unknown as fs.FSWatcher;
    }) as typeof fs.watch);
    vi.spyOn(fs, "statSync").mockImplementation(() => {
      if (missing) throw Object.assign(new Error("gone"), { code: "ENOENT" });
      return { dev: 1, ino: inode } as fs.Stats;
    });
    const read = vi.spyOn(fs, "readFileSync").mockImplementation(file => {
      const name = String(file);
      if (name === `${directory}/cgroup.events`) return `populated ${populated ? 1 : 0}\nfrozen 0\n`;
      if (name === `${directory}/cgroup.procs`) {
        if (unreadable) throw Object.assign(new Error("unreadable membership"), { code: "EACCES" });
        return pids.join("\n");
      }
      const pid = Number(name.split("/")[2]);
      if (!births.has(pid)) throw Object.assign(new Error("gone pid"), { code: "ENOENT" });
      if (name.endsWith("/cgroup")) return `0::${directory.slice("/sys/fs/cgroup".length)}\n`;
      const fields = Array<string>(20).fill("0");
      fields[0] = "S"; fields[2] = String(pid); fields[19] = births.get(pid)!;
      return `${pid} (fixture) ${fields.join(" ")}`;
    });
    const write = vi.spyOn(fs, "writeFileSync").mockImplementation(() => {});
    const kill = vi.spyOn(process, "kill").mockImplementation(() => true);
    const scan = vi.spyOn(fs, "readdirSync").mockImplementation(() => { throw new Error("full /proc scan is forbidden"); });
    const receipt = cgroupCustody(directory);
    const child = Object.assign(new EventEmitter(), { pid: 100, exitCode: null, signalCode: null }) as unknown as ChildProcess;
    executionCgroups.set(child, receipt);
    const group = executionGroup(child);
    return { receipt, group, child, watch, watcher, read, write, kill, scan, births,
      pids: (value: number[]) => { pids = value; },
      gone: () => { missing = true; }, recycle: () => { inode++; },
      unknown: () => { unreadable = true; },
      emptyEvent: () => { pids = []; populated = false; event(); },
    };
  };

  it("lists a setsid member and KILLs the entire scope, without signalling numeric groups", () => {
    const f = setup();
    expect(f.receipt.members()).toEqual([100, 101]);
    f.group.signal("SIGKILL");
    expect(f.write).toHaveBeenCalledExactlyOnceWith(`${directory}/cgroup.kill`, "1");
    expect(f.kill).not.toHaveBeenCalled();
    expect(f.scan).not.toHaveBeenCalled();
    f.emptyEvent();
  });
  it("retains an unobserved double-fork orphan after native leader exit", () => {
    const f = setup();
    f.pids([101]); f.births.delete(100); f.child.emit("close", 0);
    expect(f.group.exited()).toBe(false);
    expect(f.receipt.members()).toEqual([101]);
    f.group.signal("SIGKILL");
    expect(f.write).toHaveBeenCalledWith(`${directory}/cgroup.kill`, "1");
    f.emptyEvent();
  });
  it("does no /proc enumeration or member birth reads on observe/exited hot paths", () => {
    const f = setup(); f.read.mockClear();
    for (let i = 0; i < 10; i++) { f.group.observe(); expect(f.group.exited()).toBe(false); }
    expect(f.scan).not.toHaveBeenCalled();
    expect(f.read.mock.calls.map(call => String(call[0]))).toEqual(Array(10).fill(`${directory}/cgroup.procs`));
    f.emptyEvent();
  });
  it("birth-checks each individual TERM target and verifies current scope membership", () => {
    const f = setup();
    f.group.signal("SIGTERM");
    expect(f.kill.mock.calls).toEqual([[100, "SIGTERM"], [101, "SIGTERM"]]);
    expect(f.read.mock.calls.filter(call => String(call[0]) === "/proc/101/stat")).toHaveLength(2);
    f.emptyEvent();
  });
  it("does not TERM a pid whose birth changed just before signalling", () => {
    const f = setup(); const read = f.read.getMockImplementation()!;
    let reads = 0;
    f.read.mockImplementation(((...args: Parameters<typeof fs.readFileSync>) => {
      if (String(args[0]) === "/proc/101/stat" && ++reads === 2) f.births.set(101, "recycled");
      return read(...args);
    }) as typeof fs.readFileSync);
    f.group.signal("SIGTERM");
    expect(f.kill.mock.calls).toEqual([[100, "SIGTERM"]]);
    f.emptyEvent();
  });
  it("does not TERM a same-birth pid that moved to another scope", () => {
    const f = setup(); const read = f.read.getMockImplementation()!;
    f.read.mockImplementation(((...args: Parameters<typeof fs.readFileSync>) =>
      String(args[0]) === "/proc/101/cgroup" ? "0::/user.slice/foreign.scope\n" : read(...args)
    ) as typeof fs.readFileSync);
    f.group.signal("SIGTERM");
    expect(f.kill.mock.calls).toEqual([[100, "SIGTERM"]]);
    f.emptyEvent();
  });
  it("logs and birth-checks individual KILL only when an older v2 kernel lacks cgroup.kill", () => {
    const f = setup();
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    f.write.mockImplementation(() => { throw Object.assign(new Error("no kill interface"), { code: "ENOENT" }); });
    f.group.signal("SIGKILL");
    expect(f.kill.mock.calls).toEqual([[100, "SIGKILL"], [101, "SIGKILL"]]);
    expect(warn).toHaveBeenCalledOnce();
    f.emptyEvent();
  });
  it("a gone cgroup is exited and its empty latch never adopts a reused name", () => {
    const f = setup(); f.gone();
    expect(f.group.exited()).toBe(true);
    f.recycle(); f.group.signal("SIGKILL");
    expect(f.write).not.toHaveBeenCalled();
    expect(f.watcher.close).toHaveBeenCalledOnce();
  });
  it("does not signal a replacement directory or call changed identity exited", () => {
    const f = setup(); f.recycle();
    expect(() => f.group.exited()).toThrow(/identity changed/);
    expect(() => f.group.signal("SIGKILL")).toThrow(/identity changed/);
    expect(f.write).not.toHaveBeenCalled();
    f.gone(); f.group.exited();
  });
  it("does not turn unreadable membership into an exit receipt", () => {
    const f = setup(); f.unknown();
    expect(() => f.group.exited()).toThrow(/unreadable membership/);
    expect(() => f.group.signal("SIGTERM")).toThrow(/unreadable membership/);
    expect(f.kill).not.toHaveBeenCalled();
    f.emptyEvent();
  });
  it("shares one watcher among concurrent drain waiters and wakes immediately on populated 0", async () => {
    const f = setup(); vi.useFakeTimers();
    const first = f.receipt.waitForExit(10_000), second = f.receipt.waitForExit(10_000);
    f.emptyEvent();
    expect(await first).toBe(true); expect(await second).toBe(true);
    expect(f.watch).toHaveBeenCalledOnce();
    expect(f.watcher.close).toHaveBeenCalledOnce();
    expect(vi.getTimerCount()).toBe(0);
  });
  it("falls back to one-second checks after watch failure, without a false empty receipt", async () => {
    const f = setup(); vi.useFakeTimers();
    f.watcher.emit("error", new Error("watch unsupported"));
    const waiting = f.receipt.waitForExit(5_000); f.pids([]);
    await vi.advanceTimersByTimeAsync(999);
    expect(f.read.mock.calls.filter(call => String(call[0]).endsWith("cgroup.procs"))).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(await waiting).toBe(true);
    expect(vi.getTimerCount()).toBe(0);
  });
  it("never adopts an inherited service cgroup or a different scope at admission", () => {
    expect(scopePath("0::/user.slice/fabric-host.service\n")).toBeUndefined();
    expect(scopePath("0::/user.slice/foreign.scope\n", "fabric-execution-owned.scope")).toBeUndefined();
    expect(scopePath("0::/user.slice/../foreign.scope\n")).toBeUndefined();
  });
});
