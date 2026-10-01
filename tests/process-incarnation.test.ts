import childProcess from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { processIncarnation, validProcessIncarnation } from "../src/core/atomic-write.js";

const nativePlatform = process.platform;
afterEach(() => { vi.restoreAllMocks(); vi.unstubAllEnvs(); });

describe("native process incarnation", () => {
  it.skipIf(!["linux", "darwin", "win32"].includes(nativePlatform))("does no native identity work on cold import or idle, then reads on first use", async () => {
    vi.resetModules();
    const command = vi.spyOn(childProcess, "execFileSync");
    const read = vi.spyOn(fs, "readFileSync");
    const cold = await import("../src/core/atomic-write.js");
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(command).not.toHaveBeenCalled();
    expect(read.mock.calls.filter(([file]) => String(file).startsWith("/proc/"))).toHaveLength(0);
    expect(cold.processIncarnation(process.pid)).toBeDefined();
    if (nativePlatform === "linux") {
      expect(read.mock.calls.some(([file]) => String(file) === `/proc/${process.pid}/stat`)).toBe(true);
    } else expect(command).toHaveBeenCalledOnce();
  });
  it.skipIf(!["linux", "darwin", "win32"].includes(nativePlatform))("reads a stable native identity for this live process", () => {
    const first = processIncarnation(process.pid);
    expect(first).toBeDefined(); // not a simulated-platform claim: executes the native reader
    expect(validProcessIncarnation(first)).toBe(true);
    expect(processIncarnation(process.pid)).toBe(first);
  });

  it.each(["darwin", "win32"] as const)("bounds and normalizes the native %s command", (platform) => {
    vi.spyOn(process, "platform", "get").mockReturnValue(platform);
    vi.stubEnv("SystemRoot", "C:\\Windows");
    const value = platform === "darwin" ? "Thu Oct  1 12:00:00 2026" : "639264528000000000";
    const command = vi.spyOn(childProcess, "execFileSync").mockReturnValue(` ${value}\r\n`);
    expect(processIncarnation(123)).toBe(`${platform}:${value}`);
    const [executable, args, options] = command.mock.calls[0]!;
    expect(options).toMatchObject({ encoding: "utf8", timeout: 2_000, maxBuffer: 4_096, windowsHide: true });
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
      command.mockReturnValue(invalid);
      expect(processIncarnation(123)).toBeUndefined();
    }
    command.mockImplementation(() => { throw Object.assign(new Error("unreadable"), { code: "EACCES" }); });
    expect(processIncarnation(123)).toBeUndefined();
  });

  it("does not compare unsupported, foreign or malformed identity and does not run a command for invalid PIDs", () => {
    const command = vi.spyOn(childProcess, "execFileSync");
    for (const platform of ["linux", "darwin", "win32", "freebsd"] as const) {
      vi.spyOn(process, "platform", "get").mockReturnValue(platform);
      expect(validProcessIncarnation(undefined)).toBe(false);
      expect(validProcessIncarnation("foreign:123")).toBe(false);
      expect(validProcessIncarnation("123\n")).toBe(false);
      for (const pid of [0, -1, 1.5, NaN, Infinity]) expect(processIncarnation(pid)).toBeUndefined();
    }
    expect(processIncarnation(123)).toBeUndefined(); // unsupported native platform
    expect(command).not.toHaveBeenCalled();
  });

  it.skipIf(nativePlatform !== "linux")("preserves Linux field 22 including names containing parentheses/spaces, and unreadable identity stays unknown", () => {
    const fields = ["S", ...Array(18).fill("0"), "12345"];
    vi.spyOn(fs, "readFileSync").mockReturnValue(`123 (name ) with spaces) ${fields.join(" ")}\n`);
    expect(processIncarnation(123)).toBe("12345");
    vi.mocked(fs.readFileSync).mockImplementation(() => { throw new Error("unreadable"); });
    expect(processIncarnation(123)).toBeUndefined();
  });
});
