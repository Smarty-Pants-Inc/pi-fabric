import fs from "node:fs";
import path from "node:path";
import { EventEmitter } from "node:events";
import { spawn, type ChildProcess, type SpawnOptions } from "node:child_process";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as processUtils from "../src/agents/transports/process-utils.js";
import * as cgroup from "../src/process-cgroup.js";
import { releaseScopedChild, spawnScopedExecution } from "../src/worker/scope-spawn.js";

afterEach(() => { vi.restoreAllMocks(); vi.useRealTimers(); });
describe("scope spawn compatibility", () => {
  it.each(["win32", "darwin"] as const)("preserves exact arguments/options/environment on %s", async platform => {
    vi.spyOn(process, "platform", "get").mockReturnValue(platform);
    const find = vi.spyOn(processUtils, "findExecutable");
    const child = new EventEmitter() as ChildProcess, args = ["literal '$value'", "line\nline"];
    const options: SpawnOptions = { env: { KEEP: "literal", XDG_RUNTIME_DIR: "/fixture" }, stdio: "pipe" };
    const launch = vi.fn(() => child);
    expect(await spawnScopedExecution(launch, "target", args, options)).toBe(child);
    expect(launch).toHaveBeenCalledExactlyOnceWith("target", args, options); expect(find).not.toHaveBeenCalled();
  });
  it("preserves direct options when the parent cannot retain scopes", async () => {
    const child = new EventEmitter() as ChildProcess, options: SpawnOptions = { stdio: "ignore" };
    const launch = vi.fn(() => child);
    expect(await spawnScopedExecution(launch, "target", [], options, false)).toBe(child);
    expect(launch.mock.calls[0]).toEqual(["target", [], options]);
  });
});
describe.skipIf(process.platform !== "linux")("spawn-result scope authority", () => {
  const setup = (migrate = false) => {
    vi.spyOn(processUtils, "findExecutable").mockImplementation(name => `/fixture/${name}`);
    vi.spyOn(processUtils, "executeFile").mockResolvedValue({ stdout: "/user.slice/user@1002.service\n", stderr: "" });
    const identity = { pid: 123, parent: 1, group: 123, session: 123, started: "1000" };
    vi.spyOn(cgroup, "executionIdentity").mockReturnValue(identity);
    let expected = "", current = "", marker = "";
    const gate = { on: vi.fn(), end: vi.fn((release?: string) => { if (!release) child.emit("close", 125); }) };
    const child = Object.assign(new EventEmitter(), { pid: 123, stdio: [null, null, null, gate], kill: vi.fn() }) as unknown as ChildProcess;
    const receipt = { directory: "", execution: identity, pin: { dev: 1, ino: 2, uid: process.getuid!() },
      members: () => [123], exited: () => false, signal: vi.fn(), dispose: vi.fn(),
      verify: vi.fn(() => { if (current !== expected) throw new Error("Launcher left its spawn scope"); }) } as cgroup.CgroupCustody;
    const pin = vi.spyOn(cgroup, "cgroupCustody").mockImplementation(directory => { receipt.directory = directory; return receipt; });
    vi.spyOn(cgroup, "processScopePath").mockImplementation(() => {
      const snapshot = current;
      if (migrate) current = expected.replace("/app.slice/", "/foreign.slice/");
      return snapshot;
    });
    const launch = vi.fn((_command: string, args: readonly string[], _options: SpawnOptions) => {
      const unit = args.find(arg => arg.startsWith("--unit="))!.slice(7);
      expected = cgroup.scopeDirectory("/user.slice/user@1002.service", "app.slice", unit); current = expected;
      marker = args[args.indexOf("fabric-execution") + 1]!;
      // Same-UID writer advertises a different scope with the SAME basename.
      fs.writeFileSync(marker, `0::/foreign.slice/${unit}\n`);
      return child;
    });
    return { child, gate, receipt, pin, launch, expected: () => expected, marker: () => marker };
  };
  it("pins/verifies spawn placement before trusting marker; marker bytes never choose custody", async () => {
    const f = setup(); const read = vi.spyOn(fs, "readFileSync");
    const child = await spawnScopedExecution(f.launch, "target", [], { stdio: "ignore" });
    expect(f.pin).toHaveBeenCalledExactlyOnceWith(f.expected(), f.receipt.execution);
    expect(cgroup.executionCgroups.get(child)?.directory).toBe(f.expected()); expect(f.gate.end).not.toHaveBeenCalled();
    expect(read.mock.calls.some(call => String(call[0]) === f.marker())).toBe(false);
    releaseScopedChild(child); expect(f.gate.end).toHaveBeenCalledExactlyOnceWith("release\n"); child.emit("close", 0);
  });
  it("migration between marker write and pin cannot redirect custody or open the target gate", async () => {
    const f = setup(true);
    await expect(spawnScopedExecution(f.launch, "target", [], { stdio: "ignore" })).rejects.toThrow("left its spawn scope");
    expect(f.pin).toHaveBeenCalledExactlyOnceWith(f.expected(), f.receipt.execution);
    expect(f.launch).toHaveBeenCalledOnce(); expect(f.gate.end).toHaveBeenCalledExactlyOnceWith(); expect(f.child.kill).not.toHaveBeenCalled();
    expect(f.receipt.dispose).toHaveBeenCalledOnce();
  });
  it("a marker written before the first membership check cannot downgrade a migrated launcher", async () => {
    const f = setup(); vi.useFakeTimers(); vi.spyOn(cgroup, "processScopePath").mockReturnValue("/sys/fs/cgroup/foreign.scope");
    const pending = spawnScopedExecution(f.launch, "target", [], { stdio: "ignore" });
    const outcome = pending.then(() => undefined, error => error);
    await vi.advanceTimersByTimeAsync(5_000);
    expect(await outcome).toMatchObject({ message: expect.stringContaining("refusing execution replay") });
    expect(f.pin).not.toHaveBeenCalled(); expect(f.launch).toHaveBeenCalledOnce(); expect(f.child.kill).not.toHaveBeenCalled();
  });
  it("failed pinned admission retains grace then uses atomic scope KILL, never launcher.kill", async () => {
    const f = setup(true); vi.useFakeTimers(); f.gate.end.mockImplementation(() => {});
    vi.mocked(f.receipt.signal).mockImplementation(async () => { f.child.emit("close", 125); });
    const outcome = spawnScopedExecution(f.launch, "target", [], { stdio: "ignore" }).then(() => undefined, error => error);
    await vi.advanceTimersByTimeAsync(1_999); expect(f.receipt.signal).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1); expect((await outcome).message).toContain("left its spawn scope");
    expect(f.receipt.signal).toHaveBeenCalledExactlyOnceWith("SIGKILL"); expect(f.child.kill).not.toHaveBeenCalled();
  });
  it("an unconfirmed unpinned launcher vetoes replay and exposes existing manager cleanup debt", async () => {
    const f = setup(); vi.useFakeTimers(); f.gate.end.mockImplementation(() => {});
    vi.spyOn(cgroup, "processScopePath").mockReturnValue(undefined);
    const outcome = spawnScopedExecution(f.launch, "target", [], { stdio: "ignore" }).then(() => undefined, error => error);
    await vi.advanceTimersByTimeAsync(7_000);
    expect(await outcome).toMatchObject({ launchOutcome: "unknown", cleanupPending: true, sessionId: "123" });
    expect(f.launch).toHaveBeenCalledOnce(); expect(f.child.kill).not.toHaveBeenCalled(); f.child.emit("close", 125);
  });
  it("an unreadable initial launcher birth closes its gate and never leaks or replays target", async () => {
    const f = setup(); vi.spyOn(cgroup, "executionIdentity").mockImplementation(() => { throw new Error("birth unreadable"); });
    await expect(spawnScopedExecution(f.launch, "target", [], { stdio: "ignore" })).rejects.toThrow("birth unreadable");
    expect(f.gate.end).toHaveBeenCalledExactlyOnceWith(); expect(f.launch).toHaveBeenCalledOnce(); expect(f.child.kill).not.toHaveBeenCalled();
  });
  it("a late migration before gate release is rejected rather than replayed", async () => {
    const f = setup(); const child = await spawnScopedExecution(f.launch, "target", [], { stdio: "ignore" });
    f.receipt.verify = () => { throw new Error("late migration"); };
    expect(() => releaseScopedChild(child)).toThrow("late migration"); expect(f.gate.end).not.toHaveBeenCalled();
    f.receipt.dispose(); child.emit("close", 125);
  });
  it("preserves original options after a real launcher exits before admission", async () => {
    vi.spyOn(processUtils, "findExecutable").mockImplementation(name => name === "systemd-run" ? "/bin/false" : "/usr/bin/systemctl");
    const target = new EventEmitter() as ChildProcess, options: SpawnOptions = { env: process.env, stdio: "ignore" };
    const launch = vi.fn((command: string, args: readonly string[], opts: SpawnOptions) => command === "/bin/false" ? spawn(command, [...args], opts) : target);
    expect(await spawnScopedExecution(launch, "target", [], options)).toBe(target);
    expect(launch).toHaveBeenLastCalledWith("target", [], options);
  });
});
