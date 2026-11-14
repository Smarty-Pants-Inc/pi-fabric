import { spawn, type ChildProcess } from "node:child_process";
import { EventEmitter } from "node:events";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { linuxGroupMember, linuxProcesses, type LinuxGroupMember } from "../src/agents/transports/linux-process-census.js";
import { spawnDetached } from "../src/agents/transports/process-utils.js";

// Keep reason: smarty-dev#20387 is a recorded host-wide census performance
// incident. Unknown/escaped descendants becoming a clean exit is a dangerous
// custody failure that a cheap live user flow cannot reliably induce.
// https://github.com/Smarty-Pants-Inc/smarty-dev/issues/20387
vi.mock("node:child_process", async importOriginal => {
  const actual = await importOriginal<typeof import("node:child_process")>();
  return { ...actual, spawn: vi.fn(actual.spawn) };
});

const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
  vi.mocked(spawn).mockReset();
  for (const directory of roots.splice(0)) fs.rmSync(directory, { recursive: true, force: true });
});

function fixture() {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-process-census-"));
  roots.push(directory);
  const procRoot = path.join(directory, "proc");
  const cgroupDirectory = path.join(directory, "scope");
  fs.mkdirSync(procRoot);
  fs.mkdirSync(cgroupDirectory);
  const put = (pid: number, parent = 1, group = pid, state = "S", started = `${pid}00`) => {
    const fields = Array<string>(20).fill("0");
    fields[0] = state; fields[1] = String(parent); fields[2] = String(group); fields[19] = started;
    fs.mkdirSync(path.join(procRoot, String(pid)), { recursive: true });
    // A comm containing ')' and spaces must not shift the kernel stat fields.
    fs.writeFileSync(path.join(procRoot, String(pid), "stat"), `${pid} (worker ) with spaces) ${fields.join(" ")}\n`);
    return { pid, parent, group, state, started } satisfies LinuxGroupMember;
  };
  const scope = (pids: number[], relative = "") => {
    const target = path.join(cgroupDirectory, relative);
    fs.mkdirSync(target, { recursive: true });
    fs.writeFileSync(path.join(target, "cgroup.procs"), pids.map(String).join("\n") + "\n");
  };
  const remove = (pid: number) => fs.rmSync(path.join(procRoot, String(pid)), { recursive: true, force: true });
  return { directory, procRoot, cgroupDirectory, put, scope, remove };
}
const ids = (members: LinuxGroupMember[]) => members.map(member => member.pid).sort((a, b) => a - b);
const errno = (code: string) => Object.assign(new Error(`fixture ${code}`), { code });

function observeReads() {
  const actual = fs.readFileSync.bind(fs);
  return vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, options?: unknown) =>
    actual(file, options as never)) as typeof fs.readFileSync);
}

describe.skipIf(process.platform !== "linux")("Linux process census on real temporary proc/cgroup files", () => {
  it("reads only the small scoped tree, not unrelated host process stats", () => {
    const f = fixture();
    const expected = [f.put(410), f.put(411, 410, 410), f.put(412, 411, 412)];
    for (let pid = 1_000; pid < 1_120; pid++) f.put(pid);
    f.scope([410, 411, 412]);
    const read = observeReads();
    expect(linuxProcesses({ ...f, ownedPids: new Set([410]) }).sort((a, b) => a.pid - b.pid)).toEqual(expected);
    const statReads = read.mock.calls.map(([file]) => String(file)).filter(file => file.endsWith("/stat"));
    expect(statReads.length).toBeGreaterThan(0);
    expect(statReads.every(file => [410, 411, 412].some(pid => file === path.join(f.procRoot, String(pid), "stat")))).toBe(true);
    expect(linuxGroupMember(411, f.procRoot)).toEqual(expected[1]);
  });

  it("walks nested cgroups and deduplicates PIDs repeated during migration", () => {
    const f = fixture();
    f.put(410); f.put(411, 410, 410); f.put(412, 411, 412); f.put(999);
    f.scope([410, 410]); f.scope([410, 411], "child"); f.scope([411, 412], "child/grandchild");
    expect(ids(linuxProcesses({ ...f, ownedPids: [410, 411] }))).toEqual([410, 411, 412]);
  });

  it("keeps the full census when there is no verified cgroup directory", () => {
    const f = fixture();
    const expected = [f.put(410), f.put(411), f.put(999)];
    fs.mkdirSync(path.join(f.procRoot, "self"));
    fs.writeFileSync(path.join(f.procRoot, "not-a-pid"), "ignored");
    expect(ids(linuxProcesses({ procRoot: f.procRoot }))).toEqual(ids(expected));
  });

  it.each(["missing", "EACCES", "EPERM"])("falls back to the full census for %s cgroup.procs", failure => {
    const f = fixture();
    f.put(410); f.put(999);
    if (failure !== "missing") {
      f.scope([410]);
      const actual = fs.readFileSync.bind(fs);
      vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, options?: unknown) => {
        if (String(file) === path.join(f.cgroupDirectory, "cgroup.procs")) throw errno(failure);
        return actual(file, options as never);
      }) as typeof fs.readFileSync);
    }
    expect(ids(linuxProcesses({ ...f, ownedPids: [410] }))).toEqual([410, 999]);
  });

  it("falls back if a nested cgroup cannot be read instead of accepting a partial tree", () => {
    const f = fixture();
    f.put(410); f.put(411, 410); f.put(999);
    f.scope([410]); f.scope([411], "child");
    const actual = fs.readFileSync.bind(fs);
    vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, options?: unknown) => {
      if (String(file) === path.join(f.cgroupDirectory, "child", "cgroup.procs")) throw errno("EACCES");
      return actual(file, options as never);
    }) as typeof fs.readFileSync);
    expect(ids(linuxProcesses({ ...f, ownedPids: [410] }))).toEqual([410, 411, 999]);
  });

  it.each([false, true])("omits zombie/X and vanishing PIDs (scoped=%s)", scoped => {
    const f = fixture();
    f.put(410); f.put(411, 410, 410, "Z"); f.put(412, 410, 410, "X");
    fs.mkdirSync(path.join(f.procRoot, "413")); // enumerated just before exit removed stat
    f.scope([410, 411, 412, 413, 410]);
    expect(linuxGroupMember(411, f.procRoot)?.state).toBe("Z");
    expect(linuxGroupMember(413, f.procRoot)).toBeUndefined();
    expect(ids(linuxProcesses({ procRoot: f.procRoot, ...(scoped ? { cgroupDirectory: f.cgroupDirectory, ownedPids: [410] } : {}) }))).toEqual([410]);
  });

  it("tolerates ESRCH while reading a scoped PID stat", () => {
    const f = fixture();
    f.put(410); f.put(411, 410); f.scope([410, 411]);
    const actual = fs.readFileSync.bind(fs);
    vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, options?: unknown) => {
      if (String(file) === path.join(f.procRoot, "411", "stat")) throw errno("ESRCH");
      return actual(file, options as never);
    }) as typeof fs.readFileSync);
    expect(ids(linuxProcesses({ ...f, ownedPids: [410] }))).toEqual([410]);
  });

  it("takes a full census when a tracked owned PID escapes, discovering its new descendants", () => {
    const f = fixture();
    f.put(410); f.put(411, 410, 411); f.put(412, 411, 412); f.put(999);
    f.scope([410]); // 411 escaped the scope, then spawned 412 outside it
    expect(ids(linuxProcesses({ ...f, ownedPids: new Set([410, 411]) }))).toEqual([410, 411, 412, 999]);
  });

  it("takes a full census for empty scope before exit, finding an unseen escaped group member", () => {
    const f = fixture();
    f.put(412, 1, 410); f.put(999); f.scope([]);
    expect(ids(linuxProcesses({ ...f, ownedPids: [410] }))).toEqual([412, 999]);
    // Even without retained PIDs, an empty fast-path cannot establish host exit.
    expect(ids(linuxProcesses({ ...f }))).toEqual([412, 999]);
  });

  it.each(["EACCES", "EPERM"])("propagates %s stat errors on full, scoped and escaped-owned paths", code => {
    const f = fixture();
    f.put(410); f.put(411, 410); f.scope([410, 411]);
    const actual = fs.readFileSync.bind(fs);
    const error = errno(code);
    vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, options?: unknown) => {
      if (String(file) === path.join(f.procRoot, "411", "stat")) throw error;
      return actual(file, options as never);
    }) as typeof fs.readFileSync);
    expect(() => linuxGroupMember(411, f.procRoot)).toThrow(error);
    expect(() => linuxProcesses({ procRoot: f.procRoot })).toThrow(error);
    expect(() => linuxProcesses({ ...f, ownedPids: [410] })).toThrow(error);
    f.scope([410]);
    expect(() => linuxProcesses({ ...f, ownedPids: [410, 411] })).toThrow(error);
  });
});

// Recorded spawn boundary only: actual census code consumes real temp files;
// no real child, signal, systemd unit, or host cgroup is created or destroyed.
async function recordedScope() {
  vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout", "setInterval", "clearInterval"] });
  const f = fixture();
  const pid = 700_000_041;
  const descendant = pid + 1;
  const unrelated = pid + 100;
  f.put(pid); f.put(descendant, pid, descendant); f.put(unrelated);
  f.scope([pid, descendant]);
  fs.writeFileSync(path.join(f.cgroupDirectory, "cgroup.events"), "populated 1\nfrozen 0\n");
  const workerPath = path.join(f.directory, "worker.mjs");
  fs.writeFileSync(workerPath, "");
  let kernelScope = "";
  const route = (file: unknown): unknown => {
    if (typeof file !== "string") return file;
    if (file === "/proc") return f.procRoot;
    if (/^\/proc\/\d+(?:\/|$)/.test(file)) return path.join(f.procRoot, file.slice(6));
    if (kernelScope && (file === kernelScope || file.startsWith(`${kernelScope}/`))) {
      return path.join(f.cgroupDirectory, file.slice(kernelScope.length));
    }
    return file;
  };
  const actualRead = fs.readFileSync.bind(fs);
  const read = vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, options?: unknown) =>
    actualRead(route(file) as fs.PathOrFileDescriptor, options as never)) as typeof fs.readFileSync);
  const actualReaddir = fs.readdirSync.bind(fs);
  const census = vi.spyOn(fs, "readdirSync").mockImplementation(((file: fs.PathLike, options?: unknown) =>
    actualReaddir(route(file) as fs.PathLike, options as never)) as typeof fs.readdirSync);
  const actualLstat = fs.lstatSync.bind(fs);
  vi.spyOn(fs, "lstatSync").mockImplementation(((file: fs.PathLike, options?: unknown) =>
    actualLstat(route(file) as fs.PathLike, options as never)) as typeof fs.lstatSync);
  const actualStatfs = fs.statfsSync.bind(fs);
  vi.spyOn(fs, "statfsSync").mockImplementation(((file: fs.PathLike, options?: unknown) =>
    String(file) === kernelScope ? { type: 0x63677270 } : actualStatfs(file, options as never)) as typeof fs.statfsSync);
  const actualOpen = fs.openSync.bind(fs);
  vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => actualOpen(route(file) as fs.PathLike, flags, mode));
  const watcher = Object.assign(new EventEmitter(), { close: vi.fn() }) as unknown as fs.FSWatcher;
  vi.spyOn(fs, "watch").mockImplementation((...args: Parameters<typeof fs.watch>) => {
    const target = String(args[0]) === `${kernelScope}/cgroup.events` ? watcher
      : Object.assign(new EventEmitter(), { close: vi.fn() }) as unknown as fs.FSWatcher;
    const listener = args.at(-1);
    if (typeof listener === "function") target.on("change", listener);
    return target;
  });
  vi.spyOn(process, "kill").mockReturnValue(true);
  const child = Object.assign(new EventEmitter(), { pid, unref: vi.fn(), channel: {} });
  vi.mocked(spawn).mockImplementationOnce((_command, args) => {
    const marker = args![args!.indexOf("fabric-scope") + 1]!;
    const unit = args!.find(arg => arg.startsWith("--unit="))!.slice(7);
    const relative = `/test/fixture.slice/${unit}`;
    kernelScope = `/sys/fs/cgroup${relative}`;
    const receipt = `0::${relative}\n`;
    fs.writeFileSync(path.join(f.procRoot, String(pid), "cgroup"), receipt);
    fs.writeFileSync(`${marker}.cgroup`, receipt);
    fs.writeFileSync(marker, `admitted ${unit}`);
    return child as unknown as ChildProcess;
  });
  const unconfirmed = vi.fn();
  const handle = await spawnDetached(workerPath, [], f.directory, { onUnconfirmedExit: unconfirmed }, undefined,
    { executable: "/fixture-systemd-run", slice: "fixture.slice", warn: vi.fn() }, 7_000, true);
  expect(handle.treeClosed).toBeInstanceOf(Promise);
  return {
    ...f, pid, descendant, unrelated, handle, read, census, watcher, unconfirmed,
    scans: () => census.mock.calls.filter(([file]) => String(file) === "/proc").length,
    close: async () => {
      f.remove(pid);
      child.emit("exit", 0, null); child.emit("close", 0, null);
      await new Promise<void>(resolve => setImmediate(resolve));
    },
    empty: () => {
      f.scope([]);
      fs.writeFileSync(path.join(f.cgroupDirectory, "cgroup.events"), "populated 0\nfrozen 0\n");
      watcher.emit("change", "change");
    },
  };
}

describe.skipIf(process.platform !== "linux")("recorded spawnDetached verified-scope custody", () => {
  it("uses scoped membership while live and retains exit until the verified tree is empty", async () => {
    const f = await recordedScope();
    f.read.mockClear(); f.census.mockClear();
    expect(await f.handle.isAlive()).toBe(true);
    expect(f.handle.remainingDescendantCount?.()).toBe(1);
    expect(f.scans()).toBe(0);
    expect(f.read.mock.calls.some(([file]) => String(file) === `/proc/${f.unrelated}/stat`)).toBe(false);
    await f.close();
    expect(await f.handle.isAlive()).toBe(true);
    expect(f.scans()).toBe(0);
    f.remove(f.descendant); f.empty();
    await expect(f.handle.treeClosed).resolves.toBeUndefined();
    expect(f.scans()).toBe(1); // empty scope must check for unseen escapes
    expect(await f.handle.isAlive()).toBe(false);
    expect(f.handle.lostContact()).toBeUndefined();
    expect(f.unconfirmed).not.toHaveBeenCalled();
    expect(f.watcher.close).toHaveBeenCalledOnce();
  });

  it("retains escaped owned descendants and discovers children born outside the scope", async () => {
    const f = await recordedScope();
    expect(await f.handle.isAlive()).toBe(true); // retain the descendant birth
    f.scope([f.pid]);
    const escapedChild = f.descendant + 1;
    f.put(escapedChild, f.descendant, escapedChild);
    expect(await f.handle.isAlive()).toBe(true);
    expect(f.handle.remainingDescendantCount?.()).toBe(2);
    expect(f.scans()).toBeGreaterThan(0);
    await f.close(); f.empty();
    let settled = false;
    void f.handle.treeClosed!.then(() => { settled = true; });
    await Promise.resolve();
    expect(settled).toBe(false);
    expect(await f.handle.isAlive()).toBe(true);
    f.remove(f.descendant); f.remove(escapedChild);
    await vi.advanceTimersByTimeAsync(60_000);
    await expect(f.handle.treeClosed).resolves.toBeUndefined();
    expect(await f.handle.isAlive()).toBe(false);
    expect(f.unconfirmed).not.toHaveBeenCalled();
  });

  it("does not declare exit when empty-scope fallback finds an unseen escaped group member", async () => {
    const f = await recordedScope();
    f.remove(f.descendant);
    const unseen = f.descendant + 1;
    f.put(unseen, 1, f.pid); // never sampled, reparented but still in our group
    await f.close(); f.empty();
    // Discovery cannot safely adopt an unseen birth after its leader exited.
    // This is retained uncertainty, never a clean tree-empty receipt.
    await expect(f.handle.treeClosed).rejects.toThrow("Cannot confirm ownership/exit");
    await expect(f.handle.isAlive()).rejects.toThrow("Cannot confirm ownership/exit");
    expect(f.handle.remainingDescendantCount?.()).toBeGreaterThanOrEqual(1);
    expect(f.handle.lostContact()).toContain("Owned scope observation failed");
    expect(f.unconfirmed).toHaveBeenCalledOnce();
    f.remove(unseen);
    await expect(f.handle.treeClosed).rejects.toThrow("Cannot confirm ownership/exit");
  });

  it("rejects the exit receipt instead of treating unreadable process identity as absence", async () => {
    const f = await recordedScope();
    expect(await f.handle.isAlive()).toBe(true);
    const previousRead = f.read.getMockImplementation()!;
    const error = errno("EACCES");
    f.read.mockImplementation(((file: fs.PathOrFileDescriptor, options?: unknown) => {
      if (String(file) === `/proc/${f.descendant}/stat`) throw error;
      return previousRead(file, options as never);
    }) as typeof fs.readFileSync);
    await f.close(); f.empty();
    await expect(f.handle.treeClosed).rejects.toBe(error);
    await expect(f.handle.isAlive()).rejects.toBe(error);
    expect(f.handle.lostContact()).toContain("Owned scope observation failed");
    expect(f.unconfirmed).toHaveBeenCalledOnce();
  });
});
