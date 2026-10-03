import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { assertResidentWatchdogAdmission, latchResidentWatchdogAlarm, residentWatchdogAlarmPath, RESIDENT_WATCHDOG_BLOCKED } from "../src/residency/watchdog-admission.js";

describe("resident watchdog admission debt", () => {
  it("permits ordinary cold start only when no alarm exists", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-watchdog-admission-"));
    try {
      expect(() => assertResidentWatchdogAdmission(root)).not.toThrow();
      latchResidentWatchdogAlarm(root, { pid: 123, reason: "stale-lease" });
      const alarm = fs.readFileSync(residentWatchdogAlarmPath(root), "utf8");
      expect(() => assertResidentWatchdogAdmission(root)).toThrow(RESIDENT_WATCHDOG_BLOCKED);
      expect(() => latchResidentWatchdogAlarm(root, { pid: 456, reason: "unreaped-child" })).toThrow();
      expect(fs.readFileSync(residentWatchdogAlarmPath(root), "utf8")).toBe(alarm);
    } finally { fs.rmSync(root, { recursive: true, force: true }); }
  });

  it.each(["diagnostic-write", "file-sync"] as const)("retains admission debt after %s fails", mode => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-watchdog-admission-"));
    let fault: ReturnType<typeof vi.spyOn> | undefined;
    try {
      if (mode === "diagnostic-write") {
        const write = fs.writeFileSync;
        fault = vi.spyOn(fs, "writeFileSync").mockImplementation((...args: Parameters<typeof fs.writeFileSync>) => {
          if (typeof args[0] === "number") throw Object.assign(new Error("diagnostic write failed"), { code: "EIO" });
          return write(...args);
        });
      } else fault = vi.spyOn(fs, "fsyncSync").mockImplementation(() => { throw Object.assign(new Error("sync failed"), { code: "EIO" }); });
      expect(() => latchResidentWatchdogAlarm(root, { reason: "stale-lease" })).toThrow();
      expect(fs.existsSync(residentWatchdogAlarmPath(root))).toBe(true);
      expect(() => assertResidentWatchdogAdmission(root)).toThrow(RESIDENT_WATCHDOG_BLOCKED);
    } finally { fault?.mockRestore(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it.each(["empty", "torn", "null", "directory", "unreadable"] as const)("does not treat a %s alarm as absence or exit proof", mode => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-watchdog-admission-"));
    const file = residentWatchdogAlarmPath(root);
    let fault: ReturnType<typeof vi.spyOn> | undefined;
    try {
      if (mode === "directory") fs.mkdirSync(file);
      else fs.writeFileSync(file, mode === "torn" ? "{" : mode === "null" ? "null" : "");
      if (mode === "unreadable") {
        const lstat = fs.lstatSync;
        fault = vi.spyOn(fs, "lstatSync").mockImplementation((...args: Parameters<typeof fs.lstatSync>) => {
          if (String(args[0]) === file) throw Object.assign(new Error("denied"), { code: "EACCES" });
          return lstat(...args);
        });
      }
      expect(() => assertResidentWatchdogAdmission(root)).toThrow(RESIDENT_WATCHDOG_BLOCKED);
    } finally { fault?.mockRestore(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it.skipIf(process.platform === "win32")("blocks even a dangling alarm symlink", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-watchdog-admission-"));
    try {
      fs.symlinkSync(path.join(root, "absent"), residentWatchdogAlarmPath(root));
      expect(() => assertResidentWatchdogAdmission(root)).toThrow(RESIDENT_WATCHDOG_BLOCKED);
    } finally { fs.rmSync(root, { recursive: true, force: true }); }
  });
});
