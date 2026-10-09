import fs from "node:fs";
import { EventEmitter } from "node:events";
import type { ChildProcess } from "node:child_process";
import { afterEach, describe, expect, it, vi } from "vitest";
import { cgroupCustody, executionCgroups, scopePath } from "../src/process-cgroup.js";
import { executionGroup } from "../src/worker/execution-group.js";

afterEach(() => { vi.restoreAllMocks(); vi.useRealTimers(); });
describe.skipIf(process.platform !== "linux")("cgroup execution custody", () => {
  const directory = "/sys/fs/cgroup/user.slice/app.slice/fabric-execution-test.scope";
  const pinned = "/proc/self/fd/42";
  const setup = (watchFails = false) => {
    let pids = [100, 101], populated = true, missing = false, inode = 1, unreadable = false;
    const births = new Map([[100, "1000"], [101, "1001"]]);
    const states = new Map<number, string>();
    const scopes = new Map<number, string>();
    const statErrors = new Map<number, string>();
    const watcher = Object.assign(new EventEmitter(), { close: vi.fn() });
    let event: () => void = () => {};
    const watch = vi.spyOn(fs, "watch").mockImplementation(((_file: fs.PathLike, _options: unknown, listener: unknown) => {
      if (watchFails) throw new Error("watch unavailable");
      event = () => (listener as (...args: unknown[]) => void)("change", "cgroup.events");
      return watcher as unknown as fs.FSWatcher;
    }) as typeof fs.watch);
    const stat = vi.spyOn(fs, "statSync").mockImplementation(() => ({ dev: 1, ino: inode } as fs.Stats));
    const open = vi.spyOn(fs, "openSync").mockReturnValue(42);
    const fstat = vi.spyOn(fs, "fstatSync").mockReturnValue({ dev: 1, ino: 1 } as fs.Stats);
    const close = vi.spyOn(fs, "closeSync").mockImplementation(() => {});
    const access = vi.spyOn(fs, "accessSync").mockImplementation(() => {});
    const read = vi.spyOn(fs, "readFileSync").mockImplementation(file => {
      const name = String(file);
      if (missing && name.startsWith(pinned)) throw Object.assign(new Error("gone"), { code: "ENOENT" });
      if (name === `${pinned}/cgroup.events`) return `populated ${populated ? 1 : 0}\nfrozen 1\n`;
      if (name === `${pinned}/cgroup.procs`) {
        if (unreadable) throw Object.assign(new Error("unreadable membership"), { code: "EACCES" });
        return pids.join("\n");
      }
      const pid = Number(name.split("/")[2]);
      if (!births.has(pid)) throw Object.assign(new Error("gone pid"), { code: "ENOENT" });
      if (name.endsWith("/cgroup")) return `0::${(scopes.get(pid) ?? directory).slice("/sys/fs/cgroup".length)}\n`;
      if (statErrors.has(pid)) throw Object.assign(new Error("unreadable stat"), { code: statErrors.get(pid) });
      const fields = Array<string>(20).fill("0");
      fields[0] = states.get(pid) ?? "S"; fields[2] = String(pid); fields[3] = "100"; fields[19] = births.get(pid)!;
      return `${pid} (fixture) ${fields.join(" ")}`;
    });
    const write = vi.spyOn(fs, "writeFileSync").mockImplementation(() => {});
    const kill = vi.spyOn(process, "kill").mockImplementation(() => true);
    const scan = vi.spyOn(fs, "readdirSync").mockImplementation(() => { throw new Error("full /proc scan is forbidden on hot path"); });
    const receipt = cgroupCustody(directory, { pid: 100, parent: 1, group: 100, session: 100, started: "1000" });
    const child = Object.assign(new EventEmitter(), { pid: 100, exitCode: null, signalCode: null }) as unknown as ChildProcess;
    executionCgroups.set(child, receipt);
    const group = executionGroup(child);
    return { receipt, group, child, watch, watcher, read, write, kill, scan, births, open, fstat, close, stat, access,
      states, statErrors, scopes,
      pids: (value: number[]) => { pids = value; },
      gone: () => { missing = true; }, recycle: () => { inode++; },
      unknown: () => { unreadable = true; },
      emptyWithoutEvent: () => { pids = []; populated = false; },
      changeEvent: () => event(),
      emptyEvent: () => { pids = []; populated = false; event(); },
    };
  };

  it("lists a setsid member and KILLs the entire pinned scope, never numeric groups", async () => {
    const f = setup();
    expect(f.receipt.members()).toEqual([100, 101]);
    await f.group.signal("SIGKILL");
    expect(f.write).toHaveBeenCalledExactlyOnceWith(`${pinned}/cgroup.kill`, "1");
    expect(f.kill).not.toHaveBeenCalled(); expect(f.scan).not.toHaveBeenCalled();
    expect(f.open).toHaveBeenCalledExactlyOnceWith(directory, fs.constants.O_RDONLY | fs.constants.O_DIRECTORY);
    f.emptyEvent(); expect(f.close).toHaveBeenCalledExactlyOnceWith(42);
  });
  it("retains an unobserved double-fork orphan after native leader exit", async () => {
    const f = setup(); f.pids([101]); f.births.delete(100); f.child.emit("close", 0);
    expect(f.group.exited()).toBe(false); expect(f.receipt.members()).toEqual([101]);
    await f.group.signal("SIGKILL");
    expect(f.write).toHaveBeenCalledWith(`${pinned}/cgroup.kill`, "1"); f.emptyEvent();
  });
  it("does no /proc enumeration or member birth reads on observe/exited hot paths", () => {
    const f = setup(); f.read.mockClear();
    for (let i = 0; i < 10; i++) { f.group.observe(); expect(f.group.exited()).toBe(false); }
    expect(f.scan).not.toHaveBeenCalled();
    expect(f.read).not.toHaveBeenCalled(); f.emptyEvent();
  });
  it.each(["SIGTERM", "SIGINT", "SIGHUP"] as const)("skips %s without numeric PID/PGID authority or freezer IO", async signal => {
    const f = setup(); f.read.mockClear();
    await f.group.signal(signal);
    expect(f.kill).not.toHaveBeenCalled(); expect(f.write).not.toHaveBeenCalled();
    expect(f.read).not.toHaveBeenCalled(); expect(f.scan).not.toHaveBeenCalled(); f.emptyEvent();
  });
  it.each([false, true])("migration after the exact-path check cannot signal the migrated PID (reuse=%s)", async reuse => {
    const f = setup(); const read = f.read.getMockImplementation()!;
    const migrate = () => {
      f.pids([100]); f.scopes.set(101, "/sys/fs/cgroup/sibling.scope");
      if (reuse) f.births.set(101, "9999");
    };
    let statReads = 0;
    f.read.mockImplementation(((...args: Parameters<typeof fs.readFileSync>) => {
      const value = read(...args);
      // Adversarial same-UID migration immediately AFTER the old path's last
      // stat check, even if frozen. Returned stat still matches the old birth.
      if (String(args[0]) === "/proc/101/stat" && ++statReads === 2) migrate();
      return value;
    }) as typeof fs.readFileSync);
    // Capture a checked member, then request TERM. The new path has no unsafe
    // check/signal boundary at all; the old numeric path fails this assertion.
    expect(f.receipt.members()).toContain(101);
    expect(fs.readFileSync("/proc/101/cgroup", "utf8")).toBe(`0::${directory.slice("/sys/fs/cgroup".length)}\n`);
    await f.receipt.signal("SIGTERM");
    expect(f.kill).not.toHaveBeenCalled();
    // Also migrate after that checked snapshot, immediately before the atomic
    // kernel KILL. Fake cgroupfs records ONLY the members at the write itself.
    const killed: number[] = [];
    f.write.mockImplementation((file, value) => {
      expect(String(file)).toBe(`${pinned}/cgroup.kill`); expect(value).toBe("1");
      migrate(); killed.push(...f.receipt.members());
    });
    await f.receipt.signal("SIGKILL");
    expect(killed).toEqual([100]); expect(f.kill).not.toHaveBeenCalled();
    expect(statReads).toBe(0); f.emptyEvent();
  });
  it("skipped TERM leaves unreadable membership as debt but does not block atomic KILL", async () => {
    const f = setup(); f.unknown();
    expect(() => f.receipt.members()).toThrow(/unreadable membership/);
    expect(f.group.exited()).toBe(false);
    await f.group.signal("SIGTERM"); await f.group.signal("SIGKILL");
    expect(f.kill).not.toHaveBeenCalled(); expect(f.write).toHaveBeenCalledExactlyOnceWith(`${pinned}/cgroup.kill`, "1"); f.emptyEvent();
  });
  it("an atomic KILL failure does not poison retry and never authorizes numeric fallback", async () => {
    const f = setup();
    f.write.mockImplementationOnce(() => { throw Object.assign(new Error("denied"), { code: "EPERM" }); });
    await expect(f.receipt.signal("SIGKILL")).rejects.toThrow("denied");
    await f.receipt.signal("SIGKILL"); expect(f.write).toHaveBeenCalledTimes(2);
    expect(f.kill).not.toHaveBeenCalled(); f.emptyEvent();
  });
  it("requires only cgroup.kill at admission, closing an unadmitted fd if unavailable", () => {
    const f = setup(); f.watch.mockClear(); f.close.mockClear(); f.access.mockClear();
    f.access.mockImplementation(file => {
      expect(String(file)).toBe(`${pinned}/cgroup.kill`);
      throw Object.assign(new Error("missing cgroup.kill"), { code: "ENOENT" });
    });
    expect(() => cgroupCustody(directory)).toThrow(/controls unavailable/);
    expect(f.close).toHaveBeenCalledExactlyOnceWith(42);
    expect(f.watch).not.toHaveBeenCalled(); expect(f.kill).not.toHaveBeenCalled(); f.emptyEvent();
  });
  it("does not replace a disappeared cgroup.kill with numeric KILL", async () => {
    const f = setup();
    f.write.mockImplementation(() => { throw Object.assign(new Error("no kill"), { code: "ENOENT" }); });
    await expect(f.group.signal("SIGKILL")).rejects.toThrow("no kill");
    expect(f.kill).not.toHaveBeenCalled(); f.emptyEvent();
  });
  it("losing cgroup.kill is not successful cleanup when direct procs is empty but populated stays 1", async () => {
    const f = setup(); f.pids([]);
    f.write.mockImplementation(() => { throw Object.assign(new Error("no kill"), { code: "ENOENT" }); });
    await expect(f.receipt.signal("SIGKILL")).rejects.toThrow("no kill");
    expect(f.receipt.exited()).toBe(false); expect(f.kill).not.toHaveBeenCalled(); f.emptyEvent();
  });
  it("a gone pinned cgroup is exited and never adopts a reused name", async () => {
    const f = setup(); f.gone(); expect(await f.receipt.waitForExit(0)).toBe(true); expect(f.group.exited()).toBe(true);
    f.recycle(); await f.group.signal("SIGKILL"); expect(f.write).not.toHaveBeenCalled();
    expect(f.watcher.close).toHaveBeenCalledOnce(); expect(f.close).toHaveBeenCalledOnce();
  });
  it("a renamed/replaced path cannot redirect reads or cgroup.kill away from the pinned fd", async () => {
    const f = setup(); f.recycle(); expect(f.group.exited()).toBe(false);
    await f.group.signal("SIGKILL");
    expect(f.stat).toHaveBeenCalledOnce(); expect(f.write).toHaveBeenCalledExactlyOnceWith(`${pinned}/cgroup.kill`, "1");
    expect(f.read.mock.calls.some(call => String(call[0]).startsWith(directory))).toBe(false); f.emptyEvent();
  });
  it("rejects inode replacement during open and closes the rejected fd", () => {
    const f = setup(); f.fstat.mockReturnValue({ dev: 1, ino: 2 } as fs.Stats);
    expect(() => cgroupCustody(directory)).toThrow(/identity changed/);
    expect(f.close).toHaveBeenCalledWith(42); f.emptyEvent();
  });
  it("shares one watcher among drain waiters and wakes immediately on populated 0", async () => {
    const f = setup(); vi.useFakeTimers();
    const first = f.receipt.waitForExit(10_000), second = f.receipt.waitForExit(10_000);
    f.emptyEvent(); expect(await first).toBe(true); expect(await second).toBe(true);
    expect(f.watch).toHaveBeenCalledOnce(); expect(f.watcher.close).toHaveBeenCalledOnce(); expect(vi.getTimerCount()).toBe(0);
  });
  it("does not poll membership or events before its single deadline", async () => {
    const f = setup(); vi.useFakeTimers(); f.read.mockClear();
    const waiting = f.receipt.waitForExit(5_000);
    await vi.advanceTimersByTimeAsync(4_999); expect(f.read).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1); expect(await waiting).toBe(false);
    expect(f.read.mock.calls.map(call => String(call[0]))).toEqual([`${pinned}/cgroup.events`]);
    expect(vi.getTimerCount()).toBe(0); f.emptyEvent();
  });
  it("long waits have no repeating poll or 60-second safety timer", async () => {
    const f = setup(); vi.useFakeTimers(); f.read.mockClear();
    const waiting = f.receipt.waitForExit(180_000);
    await vi.advanceTimersByTimeAsync(179_999); expect(f.read).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1); expect(await waiting).toBe(false);
    expect(f.read.mock.calls.map(call => String(call[0]))).toEqual([`${pinned}/cgroup.events`]);
    expect(vi.getTimerCount()).toBe(0); f.emptyEvent();
  });
  it("one deadline safety read recovers a missed populated-0 event", async () => {
    const f = setup(); vi.useFakeTimers(); f.read.mockClear();
    const waiting = f.receipt.waitForExit(90_000); f.emptyWithoutEvent();
    await vi.advanceTimersByTimeAsync(89_999); expect(f.read).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1); expect(await waiting).toBe(true);
    expect(f.read).toHaveBeenCalledExactlyOnceWith(`${pinned}/cgroup.events`, "utf8"); expect(vi.getTimerCount()).toBe(0);
  });
  it.each(["before", "during"] as const)("watch failure %s a wait retains custody without a polling fallback", async when => {
    const f = setup(); vi.useFakeTimers(); f.read.mockClear();
    if (when === "before") f.watcher.emit("error", new Error("unsupported"));
    const waiting = f.receipt.waitForExit(5_000);
    if (when === "during") f.watcher.emit("error", new Error("unsupported"));
    f.emptyWithoutEvent(); await vi.advanceTimersByTimeAsync(4_999);
    expect(f.read).not.toHaveBeenCalled(); expect(f.receipt.exited()).toBe(false); expect(f.receipt.watching).toBe(false);
    await vi.advanceTimersByTimeAsync(1); expect(await waiting).toBe(true);
    expect(f.read).toHaveBeenCalledExactlyOnceWith(`${pinned}/cgroup.events`, "utf8"); expect(vi.getTimerCount()).toBe(0);
  });
  it("failed watcher creation has one deadline read, then permits atomic KILL instead of repeating polls", async () => {
    const f = setup(true); vi.useFakeTimers(); f.read.mockClear();
    const waiting = f.receipt.waitForExit(5_000);
    await vi.advanceTimersByTimeAsync(4_999); expect(f.read).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1); expect(await waiting).toBe(false);
    expect(f.read).toHaveBeenCalledExactlyOnceWith(`${pinned}/cgroup.events`, "utf8");
    await f.receipt.signal("SIGKILL"); expect(f.write).toHaveBeenCalledExactlyOnceWith(`${pinned}/cgroup.kill`, "1");
    expect(f.kill).not.toHaveBeenCalled(); expect(vi.getTimerCount()).toBe(0); f.receipt.dispose();
  });
  it("an unreadable deadline safety read is not exit and leaves no repeating timer", async () => {
    const f = setup(); vi.useFakeTimers(); f.watcher.emit("error", new Error("unsupported"));
    f.read.mockClear(); f.read.mockImplementation(() => { throw Object.assign(new Error("denied"), { code: "EACCES" }); });
    const waiting = f.receipt.waitForExit(5_000);
    await vi.advanceTimersByTimeAsync(5_000); expect(await waiting).toBe(false);
    expect(f.receipt.exited()).toBe(false); expect(f.read).toHaveBeenCalledOnce(); expect(vi.getTimerCount()).toBe(0); f.receipt.dispose();
  });
  it("a populated-1 event does not cause membership reads or restart the deadline", async () => {
    const f = setup(); vi.useFakeTimers(); f.read.mockClear();
    const waiting = f.receipt.waitForExit(5_000);
    await vi.advanceTimersByTimeAsync(4_000); f.changeEvent();
    await vi.advanceTimersByTimeAsync(1_000); expect(await waiting).toBe(false);
    expect(f.read.mock.calls.map(call => String(call[0]))).toEqual(Array(2).fill(`${pinned}/cgroup.events`)); expect(vi.getTimerCount()).toBe(0); f.emptyEvent();
  });
  it("empty direct procs is not recursive scope exit while populated stays 1", async () => {
    const f = setup(); f.pids([]);
    expect(f.receipt.members()).toEqual([]); expect(f.receipt.exited()).toBe(false); f.emptyEvent(); expect(f.receipt.exited()).toBe(true);
  });
  it("never adopts an inherited service cgroup or different scope at admission", () => {
    expect(scopePath("0::/user.slice/fabric-host.service\n")).toBeUndefined();
    expect(scopePath("0::/user.slice/foreign.scope\n", "fabric-execution-owned.scope")).toBeUndefined();
    expect(scopePath("0::/user.slice/../foreign.scope\n")).toBeUndefined();
  });
});
