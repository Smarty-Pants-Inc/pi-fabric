import childProcess from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createProcessIncarnationReader, processIncarnation, validProcessIncarnation, type IncarnationCommandRunner } from "../src/core/atomic-write.js";

const nativePlatform = process.platform;
const nativeIdentityAvailable = nativePlatform !== "linux" || fs.existsSync(`/proc/${process.pid}/stat`);
afterEach(() => { vi.restoreAllMocks(); vi.unstubAllEnvs(); vi.useRealTimers(); });

describe("native process incarnation", () => {
  it.skipIf(!nativeIdentityAvailable || !["linux", "darwin", "win32"].includes(nativePlatform))("does no native identity work on cold import or idle, then reads on first use", async () => {
    vi.resetModules();
    const command = vi.spyOn(childProcess, "execFile");
    const syncCommand = vi.spyOn(childProcess, "execFileSync");
    const read = vi.spyOn(fs, "readFileSync");
    const cold = await import("../src/core/atomic-write.js");
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(command).not.toHaveBeenCalled();
    expect(syncCommand).not.toHaveBeenCalled();
    expect(read.mock.calls.filter(([file]) => String(file).startsWith("/proc/"))).toHaveLength(0);
    expect(await cold.processIncarnation(process.pid)).toBeDefined();
    if (nativePlatform === "linux") {
      expect(read.mock.calls.some(([file]) => String(file) === `/proc/${process.pid}/stat`)).toBe(true);
    } else expect(command).toHaveBeenCalledOnce();
    expect(syncCommand).not.toHaveBeenCalled();
  });

  it.skipIf(!nativeIdentityAvailable || !["linux", "darwin", "win32"].includes(nativePlatform))("reads a stable native identity for this live process", async () => {
    const first = await processIncarnation(process.pid);
    expect(first).toBeDefined(); // actually executes the native reader
    expect(validProcessIncarnation(first)).toBe(true);
    expect(await processIncarnation(process.pid)).toBe(first);
  });

  it.each(["darwin", "win32"] as const)("bounds and normalizes the injected native %s command", async (platform) => {
    const value = platform === "darwin" ? "Thu Oct  1 12:00:00 2026" : "639264528000000000";
    const run = vi.fn<IncarnationCommandRunner>().mockResolvedValue(` ${value}\r\n`);
    const reader = createProcessIncarnationReader({ platform, systemRoot: "C:\\Windows", run });
    expect(await reader.read(123)).toBe(`${platform}:${value}`);
    const [executable, args, options] = run.mock.calls[0]!;
    expect(options).toMatchObject({ timeout: 2_000, maxBuffer: 4_096, windowsHide: true, signal: expect.any(AbortSignal) });
    if (platform === "darwin") {
      expect(executable).toBe("/bin/ps");
      expect(args).toEqual(["-p", "123", "-o", "lstart="]);
      expect(options).toMatchObject({ env: { LC_ALL: "C", TZ: "UTC" } });
    } else {
      expect(executable).toBe(path.win32.join("C:\\Windows", "System32", "WindowsPowerShell", "v1.0", "powershell.exe"));
      expect(args).toEqual(["-NoLogo", "-NoProfile", "-NonInteractive", "-Command",
        "(Get-Process -Id 123 -ErrorAction Stop).StartTime.ToUniversalTime().Ticks.ToString([System.Globalization.CultureInfo]::InvariantCulture)"]);
    }
    for (const invalid of ["", "garbage", "123\n456", "Thu Oct  1 12:00:00 2026\nnoise"]) {
      run.mockResolvedValue(invalid);
      expect(await reader.read(123)).toBeUndefined();
    }
    run.mockRejectedValue(Object.assign(new Error("unreadable"), { code: "EACCES" }));
    expect(await reader.read(123)).toBeUndefined();
  });

  it("shares one lazy own-PID probe, but refreshes other PID evidence on every read", async () => {
    const run = vi.fn<IncarnationCommandRunner>().mockResolvedValue("639264528000000000\r\n");
    const reader = createProcessIncarnationReader({ platform: "win32", systemRoot: "C:\\Windows", run });
    expect(run).not.toHaveBeenCalled();
    const own = reader.own();
    expect(reader.own()).toBe(own);
    expect(await own).toBe("win32:639264528000000000");
    await reader.own();
    expect(run).toHaveBeenCalledOnce();
    expect(await reader.read(123)).toBe("win32:639264528000000000");
    run.mockResolvedValue("639264528000000001");
    expect(await reader.read(123)).toBe("win32:639264528000000001");
    expect(run).toHaveBeenCalledTimes(3);
  });

  it("a slow Windows runner never blocks timers, times out, aborts, and discards late evidence", async () => {
    vi.useFakeTimers();
    let signal!: AbortSignal;
    let finish!: (value: string) => void;
    const run: IncarnationCommandRunner = (_exe, _args, options) => {
      signal = options.signal;
      return new Promise((resolve) => { finish = resolve; });
    };
    const reader = createProcessIncarnationReader({ platform: "win32", systemRoot: "C:\\Windows", run });
    const ticks = vi.fn();
    const renewal = setInterval(ticks, 100);
    try {
      const pending = reader.own();
      await vi.advanceTimersByTimeAsync(2_000);
      expect(await pending).toBeUndefined();
      expect(ticks).toHaveBeenCalledTimes(20);
      expect(signal.aborted).toBe(true);
      finish("639264528000000000");
      expect(await reader.own()).toBeUndefined();
    } finally { clearInterval(renewal); }
  });

  it.each(["darwin", "win32"] as const)("uses the remaining %s acquisition budget, aborts its child and discards late output", async platform => {
    vi.useFakeTimers();
    let finish!: (value: string) => void;
    const run = vi.fn<IncarnationCommandRunner>().mockImplementation(() => new Promise(resolve => { finish = resolve; }));
    const reader = createProcessIncarnationReader({ platform, systemRoot: "C:\\Windows", run });
    expect(await reader.read(123, 0)).toBeUndefined();
    expect(run).not.toHaveBeenCalled();
    const pending = reader.read(123, 50);
    expect(run.mock.calls[0]![2].timeout).toBe(50);
    await vi.advanceTimersByTimeAsync(50);
    expect(await pending).toBeUndefined();
    expect(run.mock.calls[0]![2].signal.aborted).toBe(true);
    finish(platform === "win32" ? "639264528000000000" : "Thu Oct  1 12:00:00 2026");
    await Promise.resolve();
    expect(await pending).toBeUndefined();
  });

  it("does not compare foreign/malformed identity or run commands for invalid PIDs, missing tools or unsupported platforms", async () => {
    const run = vi.fn<IncarnationCommandRunner>();
    for (const platform of ["linux", "darwin", "win32", "freebsd"] as const) {
      const reader = createProcessIncarnationReader({ platform, run });
      expect(validProcessIncarnation(undefined, platform)).toBe(false);
      expect(validProcessIncarnation("foreign:123", platform)).toBe(false);
      expect(validProcessIncarnation("123\n", platform)).toBe(false);
      for (const pid of [0, -1, 1.5, NaN, Infinity]) expect(await reader.read(pid)).toBeUndefined();
      if (platform === "win32" || platform === "freebsd") expect(await reader.read(123)).toBeUndefined();
    }
    expect(run).not.toHaveBeenCalled();
  });

  it("preserves Linux field 22 including parentheses/spaces, and missing/denied procfs stays unknown", async () => {
    const reader = createProcessIncarnationReader({ platform: "linux" });
    const fields = ["S", ...Array(18).fill("0"), "12345"];
    vi.spyOn(fs, "readFileSync").mockReturnValue(`123 (name ) with spaces) ${fields.join(" ")}\n`);
    expect(await reader.read(123)).toBe("12345");
    for (const code of ["ENOENT", "EACCES"]) {
      vi.mocked(fs.readFileSync).mockImplementation(() => { throw Object.assign(new Error("unreadable"), { code }); });
      expect(await reader.read(123)).toBeUndefined();
    }
  });
});
