import fs from "node:fs";
import { EventEmitter } from "node:events";
import { spawn, type ChildProcess } from "node:child_process";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as executionGroups from "../src/worker/execution-group.js";
import { executionGroup } from "../src/worker/execution-group.js";

afterEach(() => vi.restoreAllMocks());
describe.skipIf(process.platform !== "linux")("worker execution group identity", () => {
  const setup = () => {
    const processes = new Map<number, { started: string; group: number; parent?: number }>([[100, { started: "leader-birth", group: 100 }]]);
    let unreadable = false;
    const missing = () => Object.assign(new Error("gone"), { code: "ENOENT" });
    const readdir = vi.spyOn(fs, "readdirSync").mockImplementation(dir => {
      if (String(dir) === "/proc") return [...processes.keys()].map(String) as never;
      const pid = Number(String(dir).split("/")[2]);
      if (!processes.has(pid)) throw missing();
      return [String(pid)] as never;
    });
    const read = vi.spyOn(fs, "readFileSync").mockImplementation(file => {
      if (unreadable) throw Object.assign(new Error("identity unreadable"), { code: "EIO" });
      const pid = Number(String(file).split("/")[2]);
      const value = processes.get(pid);
      if (!value) throw missing();
      if (String(file).endsWith("/children")) {
        return [...processes].filter(([, row]) => row.parent === pid).map(([child]) => child).join(" ");
      }
      const fields = Array<string>(20).fill("0");
      fields[0] = "S"; fields[2] = String(value.group); fields[19] = value.started;
      return `${pid} (fixture) ${fields.join(" ")}`;
    });
    const child = Object.assign(new EventEmitter(), { pid: 100, exitCode: null, signalCode: null }) as unknown as ChildProcess;
    const kill = vi.spyOn(process, "kill").mockImplementation(() => true);
    const group = executionGroup(child);
    return { processes, child, kill, group, readdir, read, unknown: () => { unreadable = true; } };
  };

  it("retains a descendant birth after native leader exit and pipe close", () => {
    const { processes, child, kill, group } = setup();
    processes.set(101, { started: "descendant-birth", group: 100, parent: 100 });
    group.observe();
    processes.delete(100);
    Object.assign(child, { exitCode: 0 });
    child.emit("close", 0);
    expect(group.exited()).toBe(false);
    group.signal("SIGKILL");
    expect(kill).toHaveBeenCalledWith(-100, "SIGKILL");
    processes.clear();
    expect(group.exited()).toBe(true);
  });

  it("never adopts or signals a recycled numeric group without an owned birth", () => {
    const { processes, group, kill } = setup();
    processes.set(100, { started: "foreign-birth", group: 100 });
    expect(() => group.signal("SIGKILL")).toThrow(/no surviving owned birth/);
    expect(() => group.exited()).toThrow(/exit unconfirmed/);
    expect(kill).not.toHaveBeenCalled();
  });

  it("does not treat unreadable birth identity as exit or signal authority", () => {
    const { group, kill, unknown } = setup();
    unknown();
    expect(() => group.exited()).toThrow(/identity unreadable/);
    expect(() => group.signal("SIGTERM")).toThrow(/identity unreadable/);
    expect(kill).not.toHaveBeenCalled();
  });

  it("latches an observed empty group so a later recycled number is never signaled", () => {
    const { processes, group, kill } = setup();
    processes.clear();
    expect(group.exited()).toBe(true);
    processes.set(100, { started: "recycled-birth", group: 100 });
    group.signal("SIGKILL");
    expect(group.exited()).toBe(true);
    expect(kill).not.toHaveBeenCalled();
  });

  it("walks only the child's descendants, never unrelated host processes", () => {
    const { processes, group, readdir, read } = setup();
    for (let pid = 2000; pid < 2050; pid++) processes.set(pid, { started: "foreign", group: pid, parent: 1 });
    processes.set(101, { started: "child-birth", group: 100, parent: 100 });
    processes.set(102, { started: "grandchild-birth", group: 100, parent: 101 });
    group.observe();
    group.observe();
    const stats = read.mock.calls.map(([file]) => String(file)).filter(file => file.endsWith("/stat"));
    expect(stats.filter(file => /\/proc\/20\d\d\//.test(file))).toEqual([]);
    expect(readdir.mock.calls.filter(([dir]) => String(dir) === "/proc")).toEqual([]);
    expect(group.members().sort()).toEqual([100, 101, 102]);
  });

  it("keeps a recorded orphan after its parent exits and adopts reparented unrecorded ones", () => {
    const { processes, group } = setup();
    processes.set(101, { started: "child-birth", group: 100, parent: 100 });
    processes.set(102, { started: "grandchild-birth", group: 100, parent: 101 });
    group.observe();
    processes.set(103, { started: "late-birth", group: 100, parent: 102 });
    processes.delete(102);
    processes.set(103, { started: "late-birth", group: 100, parent: 1 });
    // 103 was never walked and is now outside the tree; 102 vanishing triggers one scan.
    expect(group.members().sort()).toEqual([100, 101, 103]);
    processes.delete(100); processes.delete(101);
    expect(group.members()).toEqual([103]);
    expect(group.exited()).toBe(false);
  });

  it("adopts group members still alive when the leader is reaped, even if no tick saw them", () => {
    const { processes, child, group, kill } = setup();
    processes.set(101, { started: "unseen-birth", group: 100, parent: 1 });
    processes.delete(100);
    child.emit("exit", 0);
    expect(group.exited()).toBe(false);
    group.signal("SIGKILL");
    expect(kill).toHaveBeenCalledWith(-100, "SIGKILL");
  });

  it("falls back to the host-wide scan once when task children are unavailable", async () => {
    vi.resetModules();
    const real = fs.existsSync;
    vi.spyOn(fs, "existsSync").mockImplementation(file => String(file).endsWith("/children") ? false : real(file));
    const stderr = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    const { executionGroup: fresh } = await import("../src/worker/execution-group.js");
    const processes = new Map([[100, 100], [101, 100], [102, 999]]);
    vi.spyOn(fs, "readdirSync").mockImplementation(() => [...processes.keys()].map(String) as never);
    vi.spyOn(fs, "readFileSync").mockImplementation(file => {
      const pid = Number(String(file).split("/")[2]);
      const fields = Array<string>(20).fill("0");
      fields[0] = "S"; fields[2] = String(processes.get(pid)); fields[19] = "birth";
      return `${pid} (fixture) ${fields.join(" ")}`;
    });
    const group = fresh(Object.assign(new EventEmitter(), { pid: 100 }) as unknown as ChildProcess);
    expect(group.members().sort()).toEqual([100, 101]);
    expect(group.members().sort()).toEqual([100, 101]);
    expect(stderr.mock.calls.filter(([text]) => String(text).includes("full /proc scan"))).toHaveLength(1);
  });

  it("runs the full /proc scan when the descendant walk exceeds its limit", () => {
    const processes = new Map<number, { group: number; parent?: number }>([[100, { group: 100 }]]);
    const children = new Map<number, number[]>();
    const add = (pid: number, parent: number, group = 100) => {
      processes.set(pid, { group, parent });
      children.set(parent, [...(children.get(parent) ?? []), pid]);
    };
    const missing = () => Object.assign(new Error("gone"), { code: "ENOENT" });
    const readdir = vi.spyOn(fs, "readdirSync").mockImplementation(dir => {
      if (String(dir) === "/proc") return [...processes.keys()].map(String) as never;
      const pid = Number(String(dir).split("/")[2]);
      if (!processes.has(pid)) throw missing();
      return [String(pid)] as never;
    });
    vi.spyOn(fs, "readFileSync").mockImplementation(file => {
      const pid = Number(String(file).split("/")[2]);
      const value = processes.get(pid);
      if (!value) throw missing();
      if (String(file).endsWith("/children")) return (children.get(pid) ?? []).join(" ");
      const fields = Array<string>(20).fill("0");
      fields[0] = "S"; fields[2] = String(value.group); fields[19] = `birth-${pid}`;
      return `${pid} (fixture) ${fields.join(" ")}`;
    });
    const group = executionGroup(Object.assign(new EventEmitter(), { pid: 100 }) as unknown as ChildProcess);
    const scans = () => readdir.mock.calls.filter(([dir]) => String(dir) === "/proc").length;
    // A normal tree is fully covered by the walk: no host-wide scan.
    for (let pid = 101; pid <= 110; pid++) add(pid, 100);
    expect(group.members().sort((a, b) => a - b)).toEqual([100, ...Array.from({ length: 10 }, (_, i) => 101 + i)]);
    expect(scans()).toBe(0);
    // More than 4096 reachable descendants: the walk is partial, so members()
    // must fall back to the full scan and report every member.
    for (let pid = 1000; pid < 6000; pid++) add(pid, 100 + (pid % 10) + 1);
    for (let pid = 9000; pid < 9010; pid++) add(pid, 1, 9000);
    const all = group.members();
    expect(scans()).toBe(1);
    expect(all).toHaveLength(5011);
    expect(new Set(all)).toEqual(new Set([...processes].filter(([, row]) => row.group === 100).map(([pid]) => pid)));
  });

  it("refuses a group whose recorded member moved to a foreign group", () => {
    const { processes, group, kill } = setup();
    processes.set(100, { started: "leader-birth", group: 200 });
    processes.set(101, { started: "foreign-birth", group: 100 });
    expect(() => group.signal("SIGTERM")).toThrow(/no surviving owned birth/);
    expect(kill).not.toHaveBeenCalled();
  });
});

describe.skipIf(process.platform !== "linux")("worker execution group on real processes", () => {
  const live: ChildProcess[] = [];
  afterEach(() => {
    vi.useRealTimers();
    for (const child of live.splice(0)) try { process.kill(-child.pid!, "SIGKILL"); } catch { /* exited */ }
  });
  const start = async (script: string) => {
    const child = spawn("sh", ["-c", script], { detached: true, stdio: ["ignore", "pipe", "ignore"] });
    live.push(child);
    const line = await new Promise<string>(resolve => child.stdout!.once("data", data => resolve(String(data).trim())));
    return { child, printed: Number(line) };
  };

  it("finds a child and the grandchild it forked, and drops the grandchild once it exits", async () => {
    const { child, printed: grandchild } = await start("sleep 30 & echo $!; exec sleep 30");
    const group = executionGroup(child);
    expect(group.members().sort((a, b) => a - b)).toEqual([child.pid!, grandchild].sort((a, b) => a - b));
    process.kill(grandchild, "SIGKILL");
    const deadline = Date.now() + 5_000;
    while (group.members().includes(grandchild) && Date.now() < deadline) await new Promise(resolve => setTimeout(resolve, 20));
    expect(group.members()).toEqual([child.pid!]);
    expect(group.exited()).toBe(false);
  });

  it("an idle observed worker makes fewer than 50 /proc reads in 5 s", async () => {
    const { child } = await start("echo $$; exec sleep 30");
    const group = executionGroup(child);
    const proc = (file: unknown) => String(file).startsWith("/proc");
    const read = vi.spyOn(fs, "readFileSync");
    const readdir = vi.spyOn(fs, "readdirSync");
    const exists = vi.spyOn(fs, "existsSync");
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval"] });
    // Before #10009 the worker observed every 100 ms with a host-wide scan.
    const observe = executionGroups.observeExecutionGroup as typeof executionGroups.observeExecutionGroup | undefined
      ?? ((value: { observe(): void }) => setInterval(() => value.observe(), 100));
    const timer = observe(group);
    vi.advanceTimersByTime(5_000);
    clearInterval(timer);
    const reads = [read, readdir, exists].flatMap(spy => spy.mock.calls.filter(([file]) => proc(file))).length;
    expect(reads).toBeGreaterThan(0);
    expect(reads).toBeLessThan(50);
  });
});
