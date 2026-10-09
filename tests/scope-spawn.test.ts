import { spawn, type ChildProcess, type SpawnOptions } from "node:child_process";
import { EventEmitter } from "node:events";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as processUtils from "../src/agents/transports/process-utils.js";
import { spawnScopedExecution } from "../src/worker/scope-spawn.js";

afterEach(() => { vi.restoreAllMocks(); vi.unstubAllEnvs(); });
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
