import fs from "node:fs";
import { EventEmitter } from "node:events";
import type { ChildProcess } from "node:child_process";
import { afterEach, describe, expect, it, vi } from "vitest";
import { executionGroup } from "../src/worker/execution-group.js";

afterEach(() => vi.restoreAllMocks());
describe.skipIf(process.platform !== "linux")("worker execution group identity", () => {
  const setup = () => {
    const processes = new Map<number, { started: string; group: number }>([[100, { started: "leader-birth", group: 100 }]]);
    let unreadable = false;
    vi.spyOn(fs, "readdirSync").mockImplementation(() => [...processes.keys()].map(String) as never);
    vi.spyOn(fs, "readFileSync").mockImplementation(file => {
      if (unreadable) throw Object.assign(new Error("identity unreadable"), { code: "EIO" });
      const pid = Number(String(file).split("/")[2]);
      const value = processes.get(pid);
      if (!value) throw Object.assign(new Error("gone"), { code: "ENOENT" });
      const fields = Array<string>(20).fill("0");
      fields[0] = "S"; fields[2] = String(value.group); fields[19] = value.started;
      return `${pid} (fixture) ${fields.join(" ")}`;
    });
    const child = Object.assign(new EventEmitter(), { pid: 100, exitCode: null, signalCode: null }) as unknown as ChildProcess;
    const kill = vi.spyOn(process, "kill").mockImplementation(() => true);
    const group = executionGroup(child);
    return { processes, child, kill, group, unknown: () => { unreadable = true; } };
  };

  it("retains a descendant birth after native leader exit and pipe close", () => {
    const { processes, child, kill, group } = setup();
    processes.set(101, { started: "descendant-birth", group: 100 });
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

  it("refuses a group whose recorded member moved to a foreign group", () => {
    const { processes, group, kill } = setup();
    processes.set(100, { started: "leader-birth", group: 200 });
    processes.set(101, { started: "foreign-birth", group: 100 });
    expect(() => group.signal("SIGTERM")).toThrow(/no surviving owned birth/);
    expect(kill).not.toHaveBeenCalled();
  });
});
