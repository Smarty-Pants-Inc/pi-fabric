import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { afterEach, describe, expect, it } from "vitest";
import {
  assertWindowsRequestsDirectoryPrivate, parseWindowsAclReport, prepareResidentRequestsDirectory,
  readWindowsDirectoryAcl, windowsRequestsAclViolations, windowsSystemExecutable, type WindowsDirectoryAcl,
} from "../src/residency/windows-acl.js";

const user = "S-1-5-21-1111111111-2222222222-3333333333-1001";
const other = "S-1-5-21-1111111111-2222222222-3333333333-1002";
// FileSystemRights values Get-Acl reports for icacls (F), (M), (W), (RX) and GENERIC_ALL.
const FULL = 2032127, MODIFY = 197055, WRITE = 278, READ_EXECUTE = 1179817, GENERIC_ALL = 268435456;
const report = (...lines: string[]) => [`user ${user}`, `owner ${user}`, ...lines, "end", ""].join("\r\n");
const ownerOnly = report(`ace Allow ${user} ${FULL}`, `ace Allow S-1-5-18 ${FULL}`, `ace Allow S-1-5-32-544 ${FULL}`);
const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const tempRoot = () => { const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-requests-acl-")); roots.push(root); return root; };

describe("Windows resident requests DACL policy", () => {
  it("serves an owner + SYSTEM + Administrators DACL, also with read-only or denied extra entries", async () => {
    expect(windowsRequestsAclViolations(parseWindowsAclReport(ownerOnly))).toEqual([]);
    const extraReadOnly = report(`ace Allow ${user} ${FULL}`, `ace Allow S-1-5-32-545 ${READ_EXECUTE}`, `ace Deny ${other} ${FULL}`);
    expect(windowsRequestsAclViolations(parseWindowsAclReport(extraReadOnly))).toEqual([]);
    await expect(assertWindowsRequestsDirectoryPrivate("C:\\r\\requests", async () => parseWindowsAclReport(ownerOnly))).resolves.toBeUndefined();
  });

  it.each([["(W)", WRITE], ["(M)", MODIFY], ["(F)", FULL], ["GENERIC_ALL", GENERIC_ALL], ["GENERIC_WRITE", 0x40000000],
    ["FILE_ADD_FILE", 0x2], ["DELETE_CHILD", 0x40], ["WRITE_DAC", 0x40000], ["WRITE_OWNER", 0x80000]])(
    "refuses an extra account granted %s and names its SID", async (_label, rights) => {
      const acl = parseWindowsAclReport(report(`ace Allow ${user} ${FULL}`, `ace Allow ${other} ${rights}`));
      expect(windowsRequestsAclViolations(acl)).toEqual([other]);
      await expect(assertWindowsRequestsDirectoryPrivate("C:\\r\\requests", async () => acl))
        .rejects.toThrow(new RegExp(`^Refusing to serve resident requests: .*writable by other accounts \\(${other}\\)`));
    });

  it("refuses a directory owned by another account even with an owner-only DACL", () => {
    const acl = parseWindowsAclReport(ownerOnly.replace(`owner ${user}`, `owner ${other}`));
    expect(windowsRequestsAclViolations(acl)).toEqual([`${other} (owner)`]);
  });

  it.each([
    ["empty", ""],
    ["truncated (no end marker)", `user ${user}\nowner ${user}\nace Allow ${user} ${FULL}`],
    ["an account name instead of a SID", report(`ace Allow BUILTIN\\Users ${WRITE}`)],
    ["an unknown access type", report(`ace Audit ${other} ${WRITE}`)],
    ["non-numeric rights", report(`ace Allow ${other} Write`)],
    ["icacls text", `C:\\r\\requests BUILTIN\\Users:(OI)(CI)(W)\nend`],
    ["missing user", [`owner ${user}`, `ace Allow ${user} ${FULL}`, "end"].join("\n")],
  ])("refuses an unparseable report: %s", async (_label, output) => {
    expect(() => parseWindowsAclReport(output)).toThrow();
    await expect(assertWindowsRequestsDirectoryPrivate("C:\\r\\requests", async () => parseWindowsAclReport(output)))
      .rejects.toThrow(/^Refusing to serve resident requests: cannot verify the DACL/);
  });

  it("refuses when the DACL query fails", async () => {
    await expect(assertWindowsRequestsDirectoryPrivate("C:\\r\\requests", async () => { throw new Error("spawn powershell.exe ENOENT"); }))
      .rejects.toThrow(/cannot verify the DACL .*ENOENT/);
  });

  it("simulated win32 creation restricts a staging directory before it becomes the requests path", async () => {
    const directory = path.join(tempRoot(), "requests");
    const restricted: string[] = [];
    let acl: WindowsDirectoryAcl = parseWindowsAclReport(report(`ace Allow ${user} ${FULL}`, `ace Allow S-1-5-11 ${MODIFY}`));
    await prepareResidentRequestsDirectory(directory, "win32", {
      readAcl: async () => acl,
      restrict: async (target, sid) => {
        expect(fs.existsSync(directory)).toBe(false);
        expect(sid).toBe(user);
        restricted.push(path.basename(target));
        acl = parseWindowsAclReport(ownerOnly);
      },
    });
    expect(fs.statSync(directory).isDirectory()).toBe(true);
    expect(restricted).toHaveLength(1);
    expect(restricted[0]).toMatch(/^requests\.create-/);
    expect(fs.readdirSync(path.dirname(directory))).toEqual(["requests"]);
  });

  it("simulated win32 refuses an existing requests directory without touching its DACL", async () => {
    const directory = path.join(tempRoot(), "requests");
    fs.mkdirSync(directory);
    let restricted = false;
    await expect(prepareResidentRequestsDirectory(directory, "win32", {
      readAcl: async () => parseWindowsAclReport(report(`ace Allow ${user} ${FULL}`, `ace Allow S-1-5-32-545 ${WRITE}`)),
      restrict: async () => { restricted = true; },
    })).rejects.toThrow(/writable by other accounts \(S-1-5-32-545\)/);
    expect(restricted).toBe(false);
  });

  it.skipIf(process.platform === "win32")("non-Windows keeps the 0700 mkdir without a DACL query", async () => {
    const directory = path.join(tempRoot(), "requests");
    await prepareResidentRequestsDirectory(directory, process.platform, {
      readAcl: async () => { throw new Error("unexpected DACL query"); }, restrict: async () => { throw new Error("unexpected restrict"); },
    });
    expect(fs.statSync(directory).mode & 0o777).toBe(0o700);
  });
});

describe.skipIf(process.platform !== "win32")("Windows resident requests DACL (real icacls)", () => {
  it("creates a private requests directory, then refuses once BUILTIN\\Users is granted write", async () => {
    const directory = path.join(tempRoot(), "requests");
    await prepareResidentRequestsDirectory(directory);
    const acl = await readWindowsDirectoryAcl(directory);
    expect(windowsRequestsAclViolations(acl)).toEqual([]);
    expect(acl.aces.every(ace => [acl.user, "S-1-5-18", "S-1-5-32-544"].includes(ace.sid))).toBe(true);
    // A second ACE for a well-known group with write, as another local account would need.
    await promisify(execFile)(windowsSystemExecutable("icacls.exe"), [directory, "/grant", "*S-1-5-32-545:(OI)(CI)(W)"], { windowsHide: true });
    await expect(prepareResidentRequestsDirectory(directory))
      .rejects.toThrow(/Refusing to serve resident requests: .*writable by other accounts \(S-1-5-32-545\)/);
  }, 60_000);
});
