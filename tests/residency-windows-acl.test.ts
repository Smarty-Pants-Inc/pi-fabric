import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { afterEach, describe, expect, it } from "vitest";
import {
  assertWindowsRequestsDirectoryPrivate, parseWindowsAclReport, prepareResidentRequestsDirectory,
  readWindowsDirectoryAcl, windowsRequestsAclViolations, windowsSystemExecutable, WindowsRequestsGuard,
  WINDOWS_REQUESTS_ACL_TTL_MS, type WindowsDirectoryAcl,
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
    const reads: string[] = [];
    const isStaging = (target: string) => path.basename(target).startsWith("requests.create-");
    // The staging directory carries an extra ACE until restricted; the verified parents do not.
    const inherited = parseWindowsAclReport(report(`ace Allow ${user} ${FULL}`, `ace Allow S-1-5-11 ${MODIFY}`));
    await prepareResidentRequestsDirectory(directory, "win32", {
      readAcl: async target => {
        reads.push(target === directory ? "requests" : isStaging(target) ? "staging" : target);
        return isStaging(target) && restricted.length === 0 ? inherited : parseWindowsAclReport(ownerOnly);
      },
      restrict: async (target, sid) => {
        expect(fs.existsSync(directory)).toBe(false);
        expect(sid).toBe(user);
        restricted.push(path.basename(target));
      },
    });
    expect(fs.statSync(directory).isDirectory()).toBe(true);
    expect(restricted).toHaveLength(1);
    expect(restricted[0]).toMatch(/^requests\.create-/);
    expect(fs.readdirSync(path.dirname(directory))).toEqual(["requests"]);
    // Parent chain first, then staging (owner SID, verify after restrict), then the final path.
    expect(reads).toEqual([path.dirname(path.dirname(directory)), path.dirname(directory), "staging", "staging", "requests"]);
  });

  it.each([["residency directory", 1], ["residency parent", 2]])(
    "simulated win32 refuses a %s with an extra writer before creating anything", async (role, depth) => {
      const directory = path.join(tempRoot(), "requests");
      const wide = path.dirname(depth === 1 ? directory : path.dirname(directory));
      let restricted = false;
      await expect(prepareResidentRequestsDirectory(directory, "win32", {
        readAcl: async target => parseWindowsAclReport(target === wide
          ? report(`ace Allow ${user} ${FULL}`, `ace Allow ${other} ${MODIFY}`) : ownerOnly),
        restrict: async () => { restricted = true; },
      })).rejects.toThrow(new RegExp(`^Refusing to serve resident requests: the ${role} .*writable by other accounts \\(${other}\\)`));
      expect(restricted).toBe(false);
      expect(fs.readdirSync(path.dirname(directory))).toEqual([]);
    });

  it("simulated win32 refuses a staging directory that is still wide or not empty, and removes it", async () => {
    for (const plant of [false, true]) {
      const directory = path.join(tempRoot(), "requests");
      let restricted = false;
      await expect(prepareResidentRequestsDirectory(directory, "win32", {
        readAcl: async target => parseWindowsAclReport(path.basename(target).startsWith("requests.create-") && (!restricted || !plant)
          ? report(`ace Allow ${user} ${FULL}`, `ace Allow ${other} ${WRITE}`) : ownerOnly),
        restrict: async target => {
          restricted = true;
          if (plant) fs.writeFileSync(path.join(target, "forged.json"), "{}");
        },
      })).rejects.toThrow(plant ? /staging directory .* is not empty/ : /staging directory .*writable by other accounts/);
      expect(fs.readdirSync(path.dirname(directory))).toEqual([]);
    }
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

  describe("simulated win32 request batch guard", () => {
    const privateAcl = () => parseWindowsAclReport(ownerOnly);
    const setup = async (readAcl: (target: string) => Promise<WindowsDirectoryAcl> = async () => privateAcl()) => {
      const directory = path.join(tempRoot(), "requests");
      let now = 1_000_000;
      const reads: string[] = [];
      const guard = new WindowsRequestsGuard(directory, {
        platform: "win32", now: () => now,
        deps: { readAcl: async target => { reads.push(target); return readAcl(target); }, restrict: async () => undefined },
      });
      await guard.prepare();
      return { directory, guard, reads, advance: (ms: number) => { now += ms; } };
    };

    it("serves an unchanged directory with one requests ACL read per 60 s", async () => {
      const { directory, guard, reads, advance } = await setup();
      const requestReads = () => reads.filter(target => target === directory).length;
      const atStart = requestReads();
      for (let i = 0; i < 5; i++) { await guard.assertBeforeConsume(); advance(10_000); }
      expect(requestReads()).toBe(atStart);
      advance(WINDOWS_REQUESTS_ACL_TTL_MS);
      await guard.assertBeforeConsume();
      await guard.assertBeforeConsume();
      expect(requestReads()).toBe(atStart + 1);
      advance(WINDOWS_REQUESTS_ACL_TTL_MS - 1);
      await guard.assertBeforeConsume();
      expect(requestReads()).toBe(atStart + 1);
      expect(guard.refusal).toBeUndefined();
    });

    it("refuses the next batch, and every later one, once requests is replaced while running", async () => {
      const { directory, guard, reads } = await setup();
      await guard.assertBeforeConsume();
      fs.renameSync(directory, `${directory}.old`);
      fs.mkdirSync(directory);
      const before = reads.length;
      // Within the cached 60 s, the identity check alone refuses; no ACL read can vouch for the new directory.
      await expect(guard.assertBeforeConsume()).rejects.toThrow(/^Refusing to serve resident requests: .* was replaced after its DACL was verified/);
      expect(reads.length).toBe(before);
      fs.rmSync(directory, { recursive: true });
      fs.renameSync(`${directory}.old`, directory);
      await expect(guard.assertBeforeConsume()).rejects.toThrow(/was replaced/);
      expect(guard.refusal?.message).toMatch(/was replaced/);
    });

    it("refuses when requests is removed or swapped for a non-directory", async () => {
      const { directory, guard } = await setup();
      fs.rmSync(directory, { recursive: true });
      fs.writeFileSync(directory, "");
      await expect(guard.assertBeforeConsume()).rejects.toThrow(/was replaced/);
    });

    it("refuses once the cached DACL expires and the requests DACL has widened", async () => {
      let wide = false;
      const { guard, advance } = await setup(async () => wide
        ? parseWindowsAclReport(report(`ace Allow ${user} ${FULL}`, `ace Allow ${other} ${MODIFY}`)) : privateAcl());
      await guard.assertBeforeConsume();
      wide = true;
      advance(WINDOWS_REQUESTS_ACL_TTL_MS);
      await expect(guard.assertBeforeConsume()).rejects.toThrow(new RegExp(`writable by other accounts \\(${other}\\)`));
      wide = false;
      await expect(guard.assertBeforeConsume()).rejects.toThrow(/writable by other accounts/);
    });

    it("never checks on POSIX", async () => {
      const directory = path.join(tempRoot(), "requests");
      const guard = new WindowsRequestsGuard(directory, {
        platform: "linux", deps: { readAcl: async () => { throw new Error("unexpected DACL query"); }, restrict: async () => undefined },
        identity: () => { throw new Error("unexpected identity check"); },
      });
      await guard.prepare();
      fs.rmSync(directory, { recursive: true });
      await expect(guard.assertBeforeConsume()).resolves.toBeUndefined();
    });
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

  it("refuses the next batch once the verified requests directory is replaced while running", async () => {
    const directory = path.join(tempRoot(), "requests");
    const guard = new WindowsRequestsGuard(directory);
    await guard.prepare();
    await expect(guard.assertBeforeConsume()).resolves.toBeUndefined();
    fs.renameSync(directory, `${directory}.old`);
    fs.mkdirSync(directory);
    await expect(guard.assertBeforeConsume()).rejects.toThrow(/was replaced after its DACL was verified/);
  }, 60_000);
});
