import fs from "node:fs";
import { afterEach, describe, expect, it, vi } from "vitest";
import { SYSTEMD_NOTIFY_PATH, systemdWatchdogNotify } from "../src/mesh/systemd-watchdog.js";

afterEach(() => vi.restoreAllMocks());

describe("systemd bridge watchdog notifier", () => {
  it("does nothing when NOTIFY_SOCKET is absent", () => {
    const run = vi.fn();
    systemdWatchdogNotify({ env: {}, platform: "linux", execFile: run as never });
    expect(run).not.toHaveBeenCalled();
  });

  it("does nothing when systemd-notify is not executable", () => {
    vi.spyOn(fs, "accessSync").mockImplementation(() => { throw new Error("missing"); });
    const run = vi.fn();
    systemdWatchdogNotify({ env: { NOTIFY_SOCKET: "/run/systemd/notify" }, platform: "linux", execFile: run as never });
    expect(run).not.toHaveBeenCalled();
  });

  it("invokes the absolute systemd-notify path without a shell and sends WATCHDOG=1", () => {
    vi.spyOn(fs, "accessSync").mockImplementation(() => undefined);
    const run = vi.fn((...args: unknown[]) => {
      const callback = args[3] as (error: Error | null) => void;
      callback(null);
      return {};
    });
    const env = { NOTIFY_SOCKET: "/run/systemd/notify" };
    systemdWatchdogNotify({ env, platform: "linux", execFile: run as never });
    expect(run).toHaveBeenCalledWith(SYSTEMD_NOTIFY_PATH, ["WATCHDOG=1"],
      expect.objectContaining({ env, timeout: 5_000, windowsHide: true }), expect.any(Function));
  });

  it("logs a failed notification only once and never throws", () => {
    vi.spyOn(fs, "accessSync").mockImplementation(() => undefined);
    const log = vi.fn();
    const run = vi.fn((...args: unknown[]) => {
      const callback = args[3] as (error: Error | null) => void;
      callback(new Error("exec failed"));
      return {};
    });
    const options = { env: { NOTIFY_SOCKET: "/run/systemd/notify" }, platform: "linux" as const, execFile: run as never, log };
    expect(() => { systemdWatchdogNotify(options); systemdWatchdogNotify(options); }).not.toThrow();
    expect(log).toHaveBeenCalledTimes(1);
  });

  it("is a no-op outside Linux", () => {
    const run = vi.fn();
    for (const platform of ["win32", "darwin"] as const) {
      systemdWatchdogNotify({ env: { NOTIFY_SOCKET: "/run/systemd/notify" }, platform, execFile: run as never });
    }
    expect(run).not.toHaveBeenCalled();
  });
});
