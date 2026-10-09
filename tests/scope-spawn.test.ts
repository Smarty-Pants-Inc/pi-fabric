import fs from "node:fs";
import { spawn, type ChildProcess, type SpawnOptions } from "node:child_process";
import { EventEmitter } from "node:events";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as processUtils from "../src/agents/transports/process-utils.js";
import { spawnScopedExecution } from "../src/worker/scope-spawn.js";
import { scopeLauncherEnvironment } from "../src/process-cgroup.js";

afterEach(() => { vi.restoreAllMocks(); vi.unstubAllEnvs(); });
describe("non-Linux execution scope compatibility", () => {
  it.each(["win32", "darwin"] as const)("preserves exact launch arguments/options/environment on %s", async platform => {
    vi.spyOn(process, "platform", "get").mockReturnValue(platform);
    const find = vi.spyOn(processUtils, "findExecutable");
    const child = new EventEmitter() as ChildProcess;
    const args = ["literal '$value'", "second line\nthird line"];
    const env = { KEEP: "literal", DBUS_SESSION_BUS_ADDRESS: "unix:path=/fixture/bus", XDG_RUNTIME_DIR: "/fixture/runtime" };
    const options: SpawnOptions = { cwd: "/fixture", env, stdio: "pipe" };
    const launch = vi.fn((_command: string, _args: readonly string[], _options: SpawnOptions) => child);
    expect(scopeLauncherEnvironment(env)).toBe(env);
    expect(await spawnScopedExecution(launch, "target", args, options)).toBe(child);
    expect(launch).toHaveBeenCalledExactlyOnceWith("target", args, options);
    expect(launch.mock.calls[0]![1]).toBe(args);
    expect(launch.mock.calls[0]![2]).toBe(options);
    expect(launch.mock.calls[0]![2].env).toBe(env);
    expect(find).not.toHaveBeenCalled();
  });
});

describe.skipIf(process.platform !== "linux")("scope spawn environment downgrade", () => {
  const options: SpawnOptions = { env: { DBUS_SESSION_BUS_ADDRESS: "unix:path=/fixture/bus", XDG_RUNTIME_DIR: "/fixture/runtime", KEEP: "literal" }, stdio: "ignore" };
  it.each(["incapable-parent", "missing-launcher"] as const)("preserves the original environment/options for %s", async reason => {
    vi.spyOn(processUtils, "findExecutable").mockReturnValue(undefined);
    const child = new EventEmitter() as ChildProcess;
    const launch = vi.fn((_command: string, _args: readonly string[], _options: SpawnOptions) => child);
    expect(await spawnScopedExecution(launch, "target", ["literal"], options, reason !== "incapable-parent")).toBe(child);
    expect(launch).toHaveBeenCalledExactlyOnceWith("target", ["literal"], options);
    expect(launch.mock.calls[0]![2]).toBe(options);
  });
  it("preserves ambient environment on direct fallback without synthesizing env", async () => {
    vi.stubEnv("DBUS_SESSION_BUS_ADDRESS", "unix:path=/fixture/bus"); vi.stubEnv("XDG_RUNTIME_DIR", "/fixture/runtime");
    const launch = vi.fn((_command: string, _args: readonly string[], _options: SpawnOptions) => new EventEmitter() as ChildProcess), inherited = { stdio: "ignore" } as const;
    await spawnScopedExecution(launch, "target", [], inherited, false);
    expect(launch.mock.calls[0]![2]).toBe(inherited);
    expect(launch.mock.calls[0]![2].env).toBeUndefined();
    expect(process.env.DBUS_SESSION_BUS_ADDRESS).toBe("unix:path=/fixture/bus");
    expect(process.env.XDG_RUNTIME_DIR).toBe("/fixture/runtime");
  });
  it("watch creation failure joins the captured launcher without releasing its target gate", async () => {
    vi.spyOn(processUtils, "findExecutable").mockReturnValue("/fixture/systemd-run");
    vi.spyOn(fs, "watch").mockImplementation(() => { throw new Error("watch unavailable"); });
    const gate = { on: vi.fn(), end: vi.fn() };
    const scoped = Object.assign(new EventEmitter(), { pid: 123, stdio: [null, null, null, gate], kill: vi.fn() }) as unknown as ChildProcess;
    const kill = vi.spyOn(scoped, "kill").mockImplementation(() => { scoped.emit("close", null, "SIGKILL"); return true; });
    const target = new EventEmitter() as ChildProcess;
    const launch = vi.fn((command: string, _args: readonly string[], _options: SpawnOptions) => command === "target" ? target : scoped);
    expect(await spawnScopedExecution(launch, "target", ["literal"], options)).toBe(target);
    expect(kill).toHaveBeenCalledExactlyOnceWith("SIGKILL"); expect(gate.end).not.toHaveBeenCalled();
    expect(launch).toHaveBeenCalledTimes(2); expect(launch).toHaveBeenLastCalledWith("target", ["literal"], options);
  });
  it("preserves options after a real scoped launcher fails before admission", async () => {
    vi.spyOn(processUtils, "findExecutable").mockReturnValue("/bin/false");
    const target = new EventEmitter() as ChildProcess;
    const launch = vi.fn((command: string, args: readonly string[], opts: SpawnOptions) =>
      command === "/bin/false" ? spawn(command, [...args], opts) : target);
    expect(await spawnScopedExecution(launch, "target", [], options)).toBe(target);
    expect(launch).toHaveBeenLastCalledWith("target", [], options);
    expect(launch.mock.calls[1]![2]).toBe(options);
  });
});
