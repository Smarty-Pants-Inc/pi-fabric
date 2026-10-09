import fs from "node:fs";
import { EventEmitter } from "node:events";
import type { ChildProcess } from "node:child_process";
import { afterEach, describe, expect, it, vi } from "vitest";
import { cgroupCustody, executionCgroups, scopePath, FREEZE_TIMEOUT_MS } from "../src/process-cgroup.js";
import { executionGroup } from "../src/worker/execution-group.js";

afterEach(() => { vi.restoreAllMocks(); vi.useRealTimers(); });
describe.skipIf(process.platform !== "linux")("cgroup execution custody", () => {
  const directory = "/sys/fs/cgroup/user.slice/app.slice/fabric-execution-test.scope";
  const pinned = "/proc/self/fd/42";
  const setup = () => {
    let pids = [100, 101], populated = true, missing = false, inode = 1, unreadable = false;
    let frozen = false, freezes = true, confirmFreeze = true;
    const births = new Map([[100, "1000"], [101, "1001"]]);
    const watcher = Object.assign(new EventEmitter(), { close: vi.fn() });
    let event!: () => void;
    const watch = vi.spyOn(fs, "watch").mockImplementation(((_file: fs.PathLike, _options: unknown, listener: unknown) => {
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
      if (name === `${pinned}/cgroup.events`) return `populated ${populated ? 1 : 0}\nfrozen ${frozen && confirmFreeze ? 1 : 0}\n`;
      if (name === `${pinned}/cgroup.procs`) {
        if (unreadable) throw Object.assign(new Error("unreadable membership"), { code: "EACCES" });
        return pids.join("\n");
      }
      const pid = Number(name.split("/")[2]);
      if (!births.has(pid)) throw Object.assign(new Error("gone pid"), { code: "ENOENT" });
      if (name.endsWith("/cgroup")) return `0::${directory.slice("/sys/fs/cgroup".length)}\n`;
      const fields = Array<string>(20).fill("0");
      fields[0] = "S"; fields[2] = String(pid); fields[3] = "100"; fields[19] = births.get(pid)!;
      return `${pid} (fixture) ${fields.join(" ")}`;
    });
    const write = vi.spyOn(fs, "writeFileSync").mockImplementation((file, value) => {
      if (String(file).endsWith("cgroup.freeze")) {
        if (!freezes) throw Object.assign(new Error("no freezer"), { code: "ENOENT" });
        frozen = value === "1";
      }
    });
    const kill = vi.spyOn(process, "kill").mockImplementation(() => true);
    const scan = vi.spyOn(fs, "readdirSync").mockImplementation(() => { throw new Error("full /proc scan is forbidden on hot path"); });
    const receipt = cgroupCustody(directory, { pid: 100, parent: 1, group: 100, session: 100, started: "1000" });
    const child = Object.assign(new EventEmitter(), { pid: 100, exitCode: null, signalCode: null }) as unknown as ChildProcess;
    executionCgroups.set(child, receipt);
    const group = executionGroup(child);
    return { receipt, group, child, watch, watcher, read, write, kill, scan, births, open, fstat, close, stat, access,
      pids: (value: number[]) => { pids = value; },
      gone: () => { missing = true; }, recycle: () => { inode++; },
      unknown: () => { unreadable = true; },
      noFreezer: () => { freezes = false; }, noConfirmation: () => { confirmFreeze = false; },
      frozen: () => frozen,
      confirm: () => { confirmFreeze = true; event(); },
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
    expect(f.read.mock.calls.map(call => String(call[0]))).toEqual(Array(10).fill(`${pinned}/cgroup.procs`)); f.emptyEvent();
  });
  it("freezes before TERM and always thaws after signalling frozen members", async () => {
    const f = setup(); f.read.mockClear();
    f.kill.mockImplementation(() => { expect(f.frozen()).toBe(true); return true; });
    await f.group.signal("SIGTERM");
    expect(f.kill.mock.calls).toEqual([[100, "SIGTERM"], [101, "SIGTERM"]]);
    expect(f.write.mock.calls).toEqual([[`${pinned}/cgroup.freeze`, "1"], [`${pinned}/cgroup.freeze`, "0"]]);
    expect(f.read.mock.calls.some(call => /\/proc\/\d+\//.test(String(call[0])))).toBe(false);
    expect(f.frozen()).toBe(false); f.emptyEvent();
  });
  it("an attempted exit/reuse between membership check and syscall cannot hit a non-member", async () => {
    const f = setup(); const read = f.read.getMockImplementation()!;
    let attempted = false, recycled = false;
    f.read.mockImplementation(((...args: Parameters<typeof fs.readFileSync>) => {
      const value = read(...args);
      if (String(args[0]) === `${pinned}/cgroup.procs`) {
        attempted = true;
        if (!f.frozen()) { recycled = true; f.births.set(101, "9999"); f.pids([100]); }
      }
      return value;
    }) as typeof fs.readFileSync);
    f.kill.mockImplementation(pid => { expect(pid === 101 && recycled).toBe(false); return true; });
    await f.group.signal("SIGTERM");
    expect(attempted).toBe(true); expect(recycled).toBe(false); f.emptyEvent();
  });
  it("does not signal a PID that exits and is reused outside the scope before frozen confirmation", async () => {
    const f = setup(); f.noConfirmation();
    const pending = f.receipt.signal("SIGTERM"); await Promise.resolve();
    f.births.set(101, "9999"); f.pids([100]); f.confirm(); await pending;
    expect(f.kill.mock.calls).toEqual([[100, "SIGTERM"]]); f.emptyEvent();
  });
  it("waits for a frozen 1 watch event, not just successful freezer write", async () => {
    const f = setup(); f.noConfirmation();
    const pending = f.receipt.signal("SIGTERM"); await Promise.resolve();
    expect(f.kill).not.toHaveBeenCalled(); f.confirm(); await pending;
    expect(f.kill).toHaveBeenCalledTimes(2); expect(f.watch).toHaveBeenCalledOnce(); f.emptyEvent();
  });
  it("attempts thaw even when the freezer write reports an error after taking effect", async () => {
    const f = setup(); const write = f.write.getMockImplementation()!;
    f.write.mockImplementation(((...args: Parameters<typeof fs.writeFileSync>) => {
      write(...args); if (args[1] === "1") throw new Error("freeze write failed after effect");
    }) as typeof fs.writeFileSync);
    await expect(f.receipt.signal("SIGTERM")).rejects.toThrow("freeze write failed");
    expect(f.frozen()).toBe(false); expect(f.kill).not.toHaveBeenCalled(); f.emptyEvent();
  });
  it("thaws when kill throws, and the failed signal does not poison later cleanup", async () => {
    const f = setup(); f.kill.mockImplementationOnce(() => { throw Object.assign(new Error("denied"), { code: "EPERM" }); });
    await expect(f.receipt.signal("SIGTERM")).rejects.toThrow("denied");
    expect(f.write).toHaveBeenLastCalledWith(`${pinned}/cgroup.freeze`, "0");
    expect(f.frozen()).toBe(false); await f.receipt.signal("SIGKILL"); f.emptyEvent();
  });
  it("thaws on membership failure and never treats unreadable membership as exit", async () => {
    const f = setup(); f.unknown();
    expect(() => f.group.exited()).toThrow(/unreadable membership/);
    await expect(f.group.signal("SIGTERM")).rejects.toThrow(/unreadable membership/);
    expect(f.kill).not.toHaveBeenCalled(); expect(f.frozen()).toBe(false); f.emptyEvent();
  });
  it("bounds unconfirmed freeze latency, thaws on timeout, and leaves no timers", async () => {
    const f = setup(); vi.useFakeTimers(); f.noConfirmation();
    const pending = f.receipt.signal("SIGTERM");
    const rejected = expect(pending).rejects.toThrow(/freeze unconfirmed/);
    await vi.advanceTimersByTimeAsync(FREEZE_TIMEOUT_MS); await rejected;
    expect(f.kill).not.toHaveBeenCalled(); expect(f.frozen()).toBe(false);
    expect(vi.getTimerCount()).toBe(0); f.emptyEvent();
  });
  it("thaws after an events read error, without signalling before freeze confirmation", async () => {
    const f = setup(); const read = f.read.getMockImplementation()!;
    f.read.mockImplementation(((...args: Parameters<typeof fs.readFileSync>) => {
      if (String(args[0]).endsWith("cgroup.events")) throw new Error("events failed");
      return read(...args);
    }) as typeof fs.readFileSync);
    await expect(f.receipt.signal("SIGTERM")).rejects.toThrow("events failed");
    expect(f.frozen()).toBe(false); expect(f.kill).not.toHaveBeenCalled(); f.emptyEvent();
  });
  it("keeps the pinned fd until finally has thawed a scope emptied during freezing", async () => {
    const f = setup(); f.noConfirmation();
    const pending = f.receipt.signal("SIGTERM"); await Promise.resolve();
    f.emptyEvent(); expect(f.close).not.toHaveBeenCalled(); await pending;
    expect(f.write).toHaveBeenLastCalledWith(`${pinned}/cgroup.freeze`, "0");
    expect(f.close).toHaveBeenCalledOnce(); expect(f.kill).not.toHaveBeenCalled();
  });
  it.each(["cgroup.freeze", "cgroup.kill"])("rejects custody without %s at admission, closing the rejected fd", control => {
    const f = setup(); f.watch.mockClear(); f.close.mockClear();
    f.access.mockImplementation(file => {
      if (String(file).endsWith(control)) throw Object.assign(new Error(`missing ${control}`), { code: "ENOENT" });
    });
    expect(() => cgroupCustody(directory)).toThrow(/controls unavailable/);
    expect(f.close).toHaveBeenCalledExactlyOnceWith(42);
    expect(f.watch).not.toHaveBeenCalled(); expect(f.kill).not.toHaveBeenCalled();
    f.emptyEvent();
  });
  it("never enters an unfrozen per-PID path if the admitted freezer disappears", async () => {
    const f = setup(); f.noFreezer();
    await expect(f.group.signal("SIGTERM")).rejects.toThrow("no freezer");
    expect(f.kill).not.toHaveBeenCalled(); f.emptyEvent();
  });
  it("does not replace a disappeared cgroup.kill with numeric KILL", async () => {
    const f = setup();
    f.write.mockImplementation(() => { throw Object.assign(new Error("no kill"), { code: "ENOENT" }); });
    await expect(f.group.signal("SIGKILL")).rejects.toThrow("no kill");
    expect(f.kill).not.toHaveBeenCalled(); f.emptyEvent();
  });
  it("a gone pinned cgroup is exited and never adopts a reused name", async () => {
    const f = setup(); f.gone(); expect(f.group.exited()).toBe(true);
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
  it("does not poll membership on one-second or deadline timer wakes with an active watcher", async () => {
    const f = setup(); vi.useFakeTimers(); f.read.mockClear();
    const waiting = f.receipt.waitForExit(5_000);
    await vi.advanceTimersByTimeAsync(4_999);
    expect(f.read.mock.calls.map(call => String(call[0]))).toEqual([`${pinned}/cgroup.procs`]);
    await vi.advanceTimersByTimeAsync(1); expect(await waiting).toBe(false);
    expect(f.read.mock.calls.map(call => String(call[0]))).toEqual([`${pinned}/cgroup.procs`]);
    expect(vi.getTimerCount()).toBe(0); f.emptyEvent();
  });
  it("permits only one 60-second safety read per watched wait, never a repeating poll", async () => {
    const f = setup(); vi.useFakeTimers(); f.read.mockClear();
    const waiting = f.receipt.waitForExit(180_000);
    await vi.advanceTimersByTimeAsync(59_999); expect(f.read).toHaveBeenCalledOnce();
    await vi.advanceTimersByTimeAsync(1); expect(f.read).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(120_000); expect(await waiting).toBe(false);
    expect(f.read.mock.calls.map(call => String(call[0]))).toEqual(Array(2).fill(`${pinned}/cgroup.procs`));
    expect(vi.getTimerCount()).toBe(0); f.emptyEvent();
  });
  it("uses the 60-second safety read to recover a missed empty event", async () => {
    const f = setup(); vi.useFakeTimers();
    const waiting = f.receipt.waitForExit(90_000); f.pids([]);
    await vi.advanceTimersByTimeAsync(60_000); expect(await waiting).toBe(true);
    expect(vi.getTimerCount()).toBe(0);
  });
  it("switches an in-flight watched wait to bounded fallback when the watcher fails", async () => {
    const f = setup(); vi.useFakeTimers();
    const waiting = f.receipt.waitForExit(5_000);
    f.watcher.emit("error", new Error("unsupported")); await Promise.resolve();
    f.pids([]); await vi.advanceTimersByTimeAsync(1_000);
    expect(await waiting).toBe(true); expect(vi.getTimerCount()).toBe(0);
  });
  it("falls back to one-second exit checks after watch failure without false empty", async () => {
    const f = setup(); vi.useFakeTimers(); f.watcher.emit("error", new Error("unsupported"));
    const waiting = f.receipt.waitForExit(5_000); f.pids([]);
    await vi.advanceTimersByTimeAsync(999);
    expect(f.read.mock.calls.filter(call => String(call[0]).endsWith("cgroup.procs"))).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(1); expect(await waiting).toBe(true); expect(vi.getTimerCount()).toBe(0);
  });
  it("never adopts an inherited service cgroup or different scope at admission", () => {
    expect(scopePath("0::/user.slice/fabric-host.service\n")).toBeUndefined();
    expect(scopePath("0::/user.slice/foreign.scope\n", "fabric-execution-owned.scope")).toBeUndefined();
    expect(scopePath("0::/user.slice/../foreign.scope\n")).toBeUndefined();
  });
});
