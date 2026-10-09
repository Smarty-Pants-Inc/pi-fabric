import fs from "node:fs";
import { EventEmitter } from "node:events";
import { afterEach, describe, expect, it, vi } from "vitest";
import { cgroupCustody, type ExecutionIdentity } from "../src/process-cgroup.js";

afterEach(() => vi.restoreAllMocks());
describe.skipIf(process.platform !== "linux")("stop-only app.slice escape detection", () => {
  const app = "/sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service/app.slice/";
  const own = `${app}fabric-execution-owned.scope`, sibling = `${app}run-escape.scope`;
  const execution: ExecutionIdentity = { pid: 100, parent: 1, group: 100, session: 100, started: "1000" };
  const setup = (kind: "session" | "group" | "parent" = "session", leaderGone = false, customSlice = false) => {
    const ownedScope = customSlice ? own.replace("/app.slice/", "/batch.slice/") : own;
    const identities = new Map<number, ExecutionIdentity>([
      [100, execution],
      [101, { pid: 101, parent: kind === "parent" ? 100 : 1, group: kind === "group" ? 100 : 101, session: kind === "session" ? 100 : 101, started: "1001" }],
      [102, { pid: 102, parent: 1, group: 102, session: 102, started: "1002" }],
      [103, { pid: 103, parent: 1, group: 100, session: 100, started: "999" }], // older, never ours
    ]);
    const procs = new Map([[ownedScope, leaderGone ? [] : [100]], [sibling, [101, 102, 103]]]);
    if (leaderGone) identities.delete(100);
    const fds = new Map<number, string>(), frozen = new Set<string>();
    let next = 40;
    vi.spyOn(fs, "statSync").mockReturnValue({ dev: 1, ino: 1 } as fs.Stats);
    vi.spyOn(fs, "fstatSync").mockReturnValue({ dev: 1, ino: 1 } as fs.Stats);
    vi.spyOn(fs, "openSync").mockImplementation(file => { fds.set(++next, String(file)); return next; });
    const close = vi.spyOn(fs, "closeSync").mockImplementation(() => {});
    vi.spyOn(fs, "watch").mockImplementation(() => Object.assign(new EventEmitter(), { close: vi.fn() }) as unknown as fs.FSWatcher);
    const scan = vi.spyOn(fs, "readdirSync").mockReturnValue(["100", "101", "102", "103", "200", "self"] as unknown as ReturnType<typeof fs.readdirSync>);
    const read = vi.spyOn(fs, "readFileSync").mockImplementation(file => {
      const name = String(file);
      if (name.startsWith("/proc/self/fd/")) {
        const directory = fds.get(Number(name.split("/")[4]))!;
        if (name.endsWith("cgroup.procs")) return procs.get(directory)!.join("\n");
        return `populated ${procs.get(directory)!.length ? 1 : 0}\nfrozen ${frozen.has(directory) ? 1 : 0}\n`;
      }
      const pid = Number(name.split("/")[2]);
      if (name.endsWith("cgroup")) return `0::${pid === 200 ? "/system.slice/unrelated.service" : (pid === 100 ? ownedScope : sibling).slice("/sys/fs/cgroup".length)}\n`;
      const identity = identities.get(pid);
      if (!identity) throw Object.assign(new Error("gone"), { code: "ENOENT" });
      const fields = Array<string>(20).fill("0"); fields[0] = "S";
      fields[1] = String(identity.parent); fields[2] = String(identity.group); fields[3] = String(identity.session); fields[19] = identity.started;
      return `${pid} (fixture) ${fields.join(" ")}`;
    });
    const write = vi.spyOn(fs, "writeFileSync").mockImplementation((file, value) => {
      const name = String(file), directory = fds.get(Number(name.split("/")[4]))!;
      if (name.endsWith("cgroup.kill")) { for (const pid of procs.get(directory)!) identities.delete(pid); procs.set(directory, []); }
      else if (value === "1") frozen.add(directory); else frozen.delete(directory);
    });
    const kill = vi.spyOn(process, "kill").mockImplementation(pid => {
      expect(frozen.has(sibling)).toBe(true); expect(pid).toBe(101);
      identities.delete(pid); procs.set(sibling, procs.get(sibling)!.filter(value => value !== pid)); return true;
    });
    const receipt = cgroupCustody(ownedScope, execution);
    return { receipt, kill, write, read, scan, procs, identities, close };
  };
  it.each(["session", "group", "parent"] as const)("catches %s matches, not older or unrelated sibling members", async kind => {
    const f = setup(kind); f.read.mockClear();
    for (let i = 0; i < 10; i++) expect(f.receipt.exited()).toBe(false);
    expect(f.scan).not.toHaveBeenCalled();
    await f.receipt.signal("SIGKILL");
    expect(f.kill).toHaveBeenCalledExactlyOnceWith(101, "SIGKILL");
    expect(f.procs.get(sibling)).toEqual([102, 103]);
    expect(await f.receipt.waitForExit(1)).toBe(true);
    expect(f.scan).toHaveBeenCalledOnce();
    expect(f.read.mock.calls.filter(call => String(call[0]) === "/proc/200/stat")).toHaveLength(0);
    expect(f.close).toHaveBeenCalledTimes(2);
  });
  it.each(["session", "parent"] as const)("detects app.slice %s escapers from a configured worker slice", async kind => {
    const f = setup(kind, false, true);
    await f.receipt.signal("SIGKILL"); expect(f.kill).toHaveBeenCalledExactlyOnceWith(101, "SIGKILL");
    expect(await f.receipt.waitForExit(1)).toBe(true); expect(f.scan).toHaveBeenCalledOnce();
  });
  it("can freeze selected escapers in sibling services without adopting the service as full custody", async () => {
    const f = setup(); const read = f.read.getMockImplementation()!;
    const service = `${app}escape.service`;
    f.procs.set(service, f.procs.get(sibling)!);
    f.read.mockImplementation(((...args: Parameters<typeof fs.readFileSync>) => {
      const value = read(...args);
      return String(args[0]).endsWith("/cgroup") && String(value).includes("run-escape.scope") ? String(value).replace("run-escape.scope", "escape.service") : value;
    }) as typeof fs.readFileSync);
    f.kill.mockImplementation(pid => { expect(pid).toBe(101); f.identities.delete(pid); f.procs.set(service, [102, 103]); return true; });
    await f.receipt.signal("SIGKILL");
    expect(f.kill).toHaveBeenCalledExactlyOnceWith(101, "SIGKILL");
    expect(f.procs.get(service)).toEqual([102, 103]); expect(await f.receipt.waitForExit(1)).toBe(true);
    expect(f.write.mock.calls.filter(call => String(call[0]).endsWith("cgroup.kill"))).toHaveLength(1); // original scope only
    expect(() => cgroupCustody(service)).toThrow(/Invalid execution cgroup/);
  });
  it("still detects and kills a same-session sibling after original scope populated 0", async () => {
    const f = setup("session", true); expect(f.receipt.exited()).toBe(true);
    await f.receipt.signal("SIGKILL"); expect(f.kill).toHaveBeenCalledExactlyOnceWith(101, "SIGKILL");
    expect(await f.receipt.waitForExit(1)).toBe(true); expect(f.scan).toHaveBeenCalledOnce();
  });
  it("does not adopt reused session/group numbers when the recorded leader PID has a different birth", async () => {
    const f = setup("session", true);
    f.identities.set(100, { ...execution, started: "9999" });
    await f.receipt.signal("SIGKILL");
    expect(f.kill).not.toHaveBeenCalled(); expect(await f.receipt.waitForExit(1)).toBe(true);
    expect(f.procs.get(sibling)).toEqual([101, 102, 103]);
  });
  it("retains escaped birth, does not adopt a recycled sibling PID, and scans only once", async () => {
    const f = setup(); f.receipt.detectEscapes();
    f.kill.mockImplementation(pid => { expect(pid).toBe(100); return true; });
    f.identities.set(101, { ...f.identities.get(101)!, started: "9999" });
    await f.receipt.signal("SIGTERM"); await f.receipt.signal("SIGKILL");
    expect(f.kill.mock.calls).toEqual([[100, "SIGTERM"]]); expect(f.scan).toHaveBeenCalledOnce();
    expect(await f.receipt.waitForExit(1)).toBe(true);
  });
});
