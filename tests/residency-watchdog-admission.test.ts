import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { assertResidentWatchdogAdmission, latchResidentWatchdogAlarm, residentWatchdogAlarmPath, residentWatchdogAttemptPath, reserveResidentWatchdogAttempt, releaseResidentWatchdogAttempt, RESIDENT_WATCHDOG_BLOCKED } from "../src/residency/watchdog-admission.js";

describe("resident watchdog admission debt", () => {
  it.each(["open", "write", "sync"] as const)("leaves no launch permission when prelaunch reservation %s fails", mode => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-watchdog-reserve-"));
    const fail = () => { throw Object.assign(new Error("reservation fault"), { code: "EIO" }); };
    const fault = mode === "open" ? vi.spyOn(fs, "openSync").mockImplementation(fail)
      : mode === "write" ? vi.spyOn(fs, "writeFileSync").mockImplementation(fail)
      : vi.spyOn(fs, "fsyncSync").mockImplementation(fail);
    try {
      expect(() => reserveResidentWatchdogAttempt(root)).toThrow("reservation fault");
      fault.mockRestore();
      // No token was returned, so caller could not spawn. A partially reserved
      // inode is debt, never permission, and cannot be replaced on retry.
      if (mode !== "open") {
        expect(() => assertResidentWatchdogAdmission(root)).toThrow(RESIDENT_WATCHDOG_BLOCKED);
        expect(() => reserveResidentWatchdogAttempt(root)).toThrow(RESIDENT_WATCHDOG_BLOCKED);
      }
    } finally { fault.mockRestore(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it("does not clear the pre-established debt after an initial alarm open failure", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-watchdog-reserve-alarm-"));
    const token = reserveResidentWatchdogAttempt(root);
    const marker = fs.readFileSync(residentWatchdogAttemptPath(root), "utf8");
    const open = fs.openSync;
    const fault = vi.spyOn(fs, "openSync").mockImplementation((...args: Parameters<typeof fs.openSync>) => {
      if (String(args[0]) === residentWatchdogAlarmPath(root)) throw new Error("initial alarm open failed");
      return open(...args);
    });
    try {
      expect(() => latchResidentWatchdogAlarm(root, {})).toThrow("initial alarm open failed");
      fault.mockRestore(); releaseResidentWatchdogAttempt(root, token);
      expect(fs.existsSync(residentWatchdogAlarmPath(root))).toBe(false);
      expect(fs.readFileSync(residentWatchdogAttemptPath(root), "utf8")).toBe(marker);
      expect(() => assertResidentWatchdogAdmission(root)).toThrow(RESIDENT_WATCHDOG_BLOCKED);
    } finally { fault.mockRestore(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it("ordinary unalarmed native exit releases only its own attempt, preserving cold-start policy", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-watchdog-ordinary-"));
    try {
      const token = reserveResidentWatchdogAttempt(root);
      expect(() => assertResidentWatchdogAdmission(root)).toThrow(RESIDENT_WATCHDOG_BLOCKED);
      expect(() => releaseResidentWatchdogAttempt(root, "wrong-token")).toThrow(RESIDENT_WATCHDOG_BLOCKED);
      releaseResidentWatchdogAttempt(root, token);
      expect(() => assertResidentWatchdogAdmission(root)).not.toThrow();
    } finally { fs.rmSync(root, { recursive: true, force: true }); }
  });


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

  it("vetoes in memory and on the next check when initial alarm creation fails", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-watchdog-open-"));
    const open = fs.openSync;
    const fault = vi.spyOn(fs, "openSync").mockImplementation((...args: Parameters<typeof fs.openSync>) => {
      if (String(args[0]) === residentWatchdogAlarmPath(root)) throw Object.assign(new Error("no inode"), { code: "ENOSPC" });
      return open(...args);
    });
    try {
      expect(() => latchResidentWatchdogAlarm(root, { reason: "stale-lease" })).toThrow("no inode");
      expect(fs.existsSync(residentWatchdogAlarmPath(root))).toBe(false);
      expect(() => assertResidentWatchdogAdmission(root)).toThrow(RESIDENT_WATCHDOG_BLOCKED);
      fault.mockRestore();
      expect(() => assertResidentWatchdogAdmission(root)).toThrow(RESIDENT_WATCHDOG_BLOCKED);
    } finally { fault.mockRestore(); fs.rmSync(root, { recursive: true, force: true }); }
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
