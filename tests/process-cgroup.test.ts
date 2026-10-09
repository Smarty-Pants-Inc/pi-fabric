import fs from "node:fs";
import { EventEmitter } from "node:events";
import type { ChildProcess } from "node:child_process";
import { afterEach, describe, expect, it, vi } from "vitest";
import { cgroupCustody, executionCgroups, scopeDirectory } from "../src/process-cgroup.js";
import { executionGroup } from "../src/worker/execution-group.js";

afterEach(() => vi.restoreAllMocks());
describe.skipIf(process.platform !== "linux")("spawn-pinned cgroup custody", () => {
  const directory = "/sys/fs/cgroup/user.slice/app.slice/fabric-execution-a.scope", pinned = "/proc/self/fd/42";
  const setup = () => {
    let populated = true, pids = [100, 101];
    const locations = new Map<number, string>(), births = new Map([[100, "1000"], [101, "1001"]]);
    const uid = process.getuid!();
    const stat = vi.spyOn(fs, "lstatSync").mockReturnValue({ dev: 1, ino: 2, uid, isDirectory: () => true } as fs.Stats);
    const open = vi.spyOn(fs, "openSync").mockReturnValue(42);
    const fstat = vi.spyOn(fs, "fstatSync").mockReturnValue({ dev: 1, ino: 2, uid } as fs.Stats);
    const close = vi.spyOn(fs, "closeSync").mockImplementation(() => {});
    const access = vi.spyOn(fs, "accessSync").mockImplementation(() => {});
    const read = vi.spyOn(fs, "readFileSync").mockImplementation(file => {
      if (String(file) === `${pinned}/cgroup.events`) return `populated ${populated ? 1 : 0}\n`;
      if (String(file) === `${pinned}/cgroup.procs`) return pids.join("\n");
      const pid = Number(String(file).split("/")[2]);
      if (String(file).endsWith("/cgroup")) return `0::${(locations.get(pid) ?? directory).slice("/sys/fs/cgroup".length)}\n`;
      const fields = Array<string>(20).fill("0"); fields[0] = "S"; fields[2] = "100"; fields[3] = "100"; fields[19] = births.get(pid)!;
      return `100 (fixture) ${fields.join(" ")}`;
    });
    const write = vi.spyOn(fs, "writeFileSync").mockImplementation(() => {});
    const kill = vi.spyOn(process, "kill").mockImplementation(() => true);
    const scan = vi.spyOn(fs, "readdirSync").mockImplementation(() => { throw new Error("numeric ownership scan forbidden"); });
    const identity = { pid: 100, parent: 1, group: 100, session: 100, started: "1000" };
    const receipt = cgroupCustody(directory, identity);
    const child = Object.assign(new EventEmitter(), { pid: 100 }) as ChildProcess;
    executionCgroups.set(child, receipt);
    return { receipt, group: executionGroup(child), identity, stat, open, fstat, close, access, read, write, kill, scan,
      empty: () => { populated = false; pids = []; },
      orphan: () => { pids = [101]; },
      directEmpty: () => { pids = []; },
      migrate: (reuse = false) => { pids = [100]; locations.set(101, "/sys/fs/cgroup/sibling.scope"); if (reuse) births.set(101, "9999"); },
      migrateLauncher: (reuse = false) => { locations.set(100, "/sys/fs/cgroup/sibling.scope"); if (reuse) births.set(100, "9999"); },
    };
  };
  it("pins exact inode and current UID; verifies launcher membership and birth", () => {
    const f = setup(); f.receipt.verify(f.identity);
    expect(f.open).toHaveBeenCalledExactlyOnceWith(directory, fs.constants.O_RDONLY | fs.constants.O_DIRECTORY | fs.constants.O_NOFOLLOW);
    expect(f.access).toHaveBeenCalledExactlyOnceWith(`${pinned}/cgroup.kill`, fs.constants.W_OK);
    expect(f.receipt.pin).toEqual({ dev: 1, ino: 2, uid: process.getuid!() }); f.receipt.dispose();
  });
  it.each([false, true])("rejects migrated/reused launcher ownership (PID reuse=%s)", reuse => {
    const f = setup(); f.migrateLauncher(reuse);
    expect(() => f.receipt.verify(f.identity)).toThrow("left its spawn scope");
    expect(f.kill).not.toHaveBeenCalled(); f.receipt.dispose();
  });
  it.each(["SIGTERM", "SIGINT", "SIGHUP"] as const)("skips %s without numeric PID/PGID signals or freezer IO", async signal => {
    const f = setup(); f.read.mockClear(); await f.group.signal(signal);
    expect(f.kill).not.toHaveBeenCalled(); expect(f.write).not.toHaveBeenCalled(); expect(f.read).not.toHaveBeenCalled(); f.receipt.dispose();
  });
  it.each([false, true])("migration plus PID reuse cannot redirect KILL (reuse=%s)", async reuse => {
    const f = setup(); f.receipt.verify(f.identity);
    await f.group.signal("SIGTERM"); f.migrate(reuse);
    const killed: number[] = [];
    f.write.mockImplementation((file, value) => {
      expect(String(file)).toBe(`${pinned}/cgroup.kill`); expect(value).toBe("1"); killed.push(...f.receipt.members());
    });
    await f.group.signal("SIGKILL");
    expect(killed).toEqual([100]); expect(f.kill).not.toHaveBeenCalled(); expect(f.scan).not.toHaveBeenCalled(); f.receipt.dispose();
  });
  it("retains orphan membership and recursive populated debt, not only direct procs", async () => {
    const f = setup(); f.orphan(); expect(f.group.exited()).toBe(false);
    f.directEmpty(); expect(f.receipt.members()).toEqual([]); expect(f.group.exited()).toBe(false);
    await f.group.signal("SIGKILL"); expect(f.write).toHaveBeenCalledExactlyOnceWith(`${pinned}/cgroup.kill`, "1");
    f.empty(); expect(f.group.exited()).toBe(true); expect(f.close).toHaveBeenCalledExactlyOnceWith(42);
  });
  it.each(["inode", "owner", "transfer"])("rejects changed %s during pin and closes the fd", kind => {
    const f = setup(); f.close.mockClear();
    if (kind === "inode") f.fstat.mockReturnValue({ dev: 1, ino: 999, uid: process.getuid!() } as fs.Stats);
    if (kind === "owner") f.stat.mockReturnValue({ dev: 1, ino: 2, uid: process.getuid!() + 1, isDirectory: () => true } as fs.Stats);
    expect(() => cgroupCustody(directory, undefined, kind === "transfer" ? { dev: 1, ino: 999, uid: process.getuid!() } : undefined)).toThrow("ownership/identity changed");
    expect(f.close).toHaveBeenCalledExactlyOnceWith(42); f.receipt.dispose();
  });
  it("never changes failed atomic KILL into numeric fallback and allows a retry", async () => {
    const f = setup(); f.write.mockImplementationOnce(() => { throw Object.assign(new Error("denied"), { code: "EPERM" }); });
    await expect(f.receipt.signal("SIGKILL")).rejects.toThrow("denied"); await f.receipt.signal("SIGKILL");
    expect(f.write).toHaveBeenCalledTimes(2); expect(f.kill).not.toHaveBeenCalled(); f.receipt.dispose();
  });
  it("does not adopt a replaced pathname after pin", async () => {
    const f = setup(); f.stat.mockReturnValue({ dev: 1, ino: 999 } as fs.Stats);
    await f.receipt.signal("SIGKILL"); expect(f.stat).toHaveBeenCalledOnce(); expect(f.write).toHaveBeenCalledExactlyOnceWith(`${pinned}/cgroup.kill`, "1"); f.receipt.dispose();
  });
  it("requires writable cgroup.kill before admission", () => {
    const f = setup(); f.access.mockImplementation(() => { throw new Error("kill unavailable"); });
    expect(() => cgroupCustody(directory)).toThrow("kill unavailable"); expect(f.kill).not.toHaveBeenCalled(); f.receipt.dispose();
  });
  it("derives exact nested slice placement without accepting marker paths", () => {
    expect(scopeDirectory("/user.slice/user@1002.service", "fabric-batch.slice", "fabric-execution-a.scope")).toBe("/sys/fs/cgroup/user.slice/user@1002.service/fabric.slice/fabric-batch.slice/fabric-execution-a.scope");
    expect(() => scopeDirectory("/user.slice/../sibling", "app.slice", "fabric-execution-a.scope")).toThrow();
    expect(() => scopeDirectory("/user.slice/user@1002.service", "../sibling.slice", "fabric-execution-a.scope")).toThrow();
  });
});
