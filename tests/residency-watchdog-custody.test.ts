import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { assertNoWatchdogCustody, watchdogCustodyPath } from "../src/residency/watchdog-custody.js";

describe("persistent watchdog custody", () => {
  it("does not expire unknown attempt membership on dead launcher/host PIDs or malformed marker contents", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "watchdog-custody-"));
    try {
      expect(() => assertNoWatchdogCustody(root)).not.toThrow();
      fs.writeFileSync(watchdogCustodyPath(root), JSON.stringify({ launcher: { pid: 999999 }, owner: { pid: 999998 } }));
      expect(() => assertNoWatchdogCustody(root)).toThrow("explicit installer drain");
      fs.writeFileSync(watchdogCustodyPath(root), "unreadable diagnostic bytes");
      expect(() => assertNoWatchdogCustody(root)).toThrow("explicit installer drain");
      fs.rmSync(watchdogCustodyPath(root));
      expect(() => assertNoWatchdogCustody(root)).not.toThrow();
    } finally { fs.rmSync(root, { recursive: true, force: true }); }
  });
  it("does not treat inability to check custody as permission to start", () => {
    const stat = vi.spyOn(fs, "lstatSync").mockImplementation(() => { throw Object.assign(new Error("denied"), { code: "EACCES" }); });
    try { expect(() => assertNoWatchdogCustody("/fixture")).toThrow("cannot be checked"); }
    finally { stat.mockRestore(); }
  });
});
