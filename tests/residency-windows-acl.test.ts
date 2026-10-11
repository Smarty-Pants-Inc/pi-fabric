import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  assertWindowsRequestsDirectoryPrivate, parseWindowsAclReport, parseWindowsFileAclBatch, prepareResidentRequestsDirectory,
  readWindowsDirectoryAcl, windowsRequestFileViolations, windowsRequestsAclViolations, windowsSystemExecutable, WindowsRequestsGuard,
  WINDOWS_REQUESTS_ACL_TTL_MS, type WindowsDirectoryAcl, type WindowsRequestFileReport,
} from "../src/residency/windows-acl.js";

const user = "S-1-5-21-1111111111-2222222222-3333333333-1001";
const other = "S-1-5-21-1111111111-2222222222-3333333333-1002";
// FileSystemRights values Get-Acl reports for icacls (F), (M), (W), (RX) and GENERIC_ALL.
const FULL = 2032127, MODIFY = 197055, WRITE = 278, READ_EXECUTE = 1179817, GENERIC_ALL = 268435456;
// The SDDL access section Get-Acl reports for the private DACL icacls sets.
const PRIVATE_SDDL = `D:PAI(A;OICI;FA;;;${user})(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)`;
const reportWith = (sddl: string, ...lines: string[]) =>
  [`user ${user}`, `owner ${user}`, `dacl ${sddl}`, ...lines, "end", ""].join("\r\n");
const report = (...lines: string[]) => reportWith(PRIVATE_SDDL, ...lines);
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
    ["D:NO_ACCESS_CONTROL", "NULL DACL"], ["D:PAINO_ACCESS_CONTROL", "NULL DACL"], ["", "no DACL"],
  ])("refuses a NULL or missing DACL (SDDL %j), which grants everyone access but lists no rules", async (sddl, label) => {
    const acl = parseWindowsAclReport(reportWith(sddl));
    expect(acl.aces).toEqual([]);
    expect(windowsRequestsAclViolations(acl)).toEqual([`S-1-1-0 (${label})`]);
    await expect(assertWindowsRequestsDirectoryPrivate("C:\\r\\requests", async () => acl))
      .rejects.toThrow(new RegExp(`^Refusing to serve resident requests: .*writable by other accounts \\(S-1-1-0 \\(${label}\\)\\)`));
  });

  it("simulated win32 refuses a NULL-DACL residency directory before creating anything", async () => {
    const directory = path.join(tempRoot(), "requests");
    let restricted = false;
    await expect(prepareResidentRequestsDirectory(directory, "win32", {
      readAcl: async target => parseWindowsAclReport(target === path.dirname(directory) ? reportWith("D:NO_ACCESS_CONTROL") : ownerOnly),
      restrict: async () => { restricted = true; },
    })).rejects.toThrow(/^Refusing to serve resident requests: the residency directory .*\(S-1-1-0 \(NULL DACL\)\)/);
    expect(restricted).toBe(false);
    expect(fs.readdirSync(path.dirname(directory))).toEqual([]);
  });

  it.each([
    ["empty", ""],
    ["no DACL line", [`user ${user}`, `owner ${user}`, `ace Allow ${user} ${FULL}`, "end"].join("\n")],
    ["two DACL lines", report(`dacl ${PRIVATE_SDDL}`, `ace Allow ${user} ${FULL}`)],
    ["an empty DACL", reportWith("D:P")],
    ["a non-DACL SDDL section", reportWith("O:BAG:SY", `ace Allow ${user} ${FULL}`)],
    ["unknown DACL flags", reportWith("D:XY(A;;FA;;;SY)", `ace Allow S-1-5-18 ${FULL}`)],
    ["unbalanced ACEs", reportWith("D:(A;;FA;;;SY", `ace Allow S-1-5-18 ${FULL}`)],
    ["text after the ACEs", reportWith("D:(A;;FA;;;SY)S:(AU;;FA;;;WD)", `ace Allow S-1-5-18 ${FULL}`)],
    ["a non-empty DACL with no listed rules", reportWith(PRIVATE_SDDL)],
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

  describe("pending request files (planted before the directory DACL was tightened)", () => {
    const ARCHIVE = 0x20, REPARSE_POINT = 0x400;
    const fileReport = (...lines: string[]): WindowsRequestFileReport => ({ attributes: ARCHIVE, acl: parseWindowsAclReport(report(...lines)) });
    const privateFile = () => fileReport(`ace Allow ${user} ${FULL}`, `ace Allow S-1-5-18 ${FULL}`, `ace Allow S-1-5-32-544 ${FULL}`);
    const regular = { regularFile: true, symbolicLink: false };

    it("accepts a regular file owned by this user with only trusted writers", () => {
      expect(windowsRequestFileViolations({ ...regular, report: privateFile() })).toEqual([]);
      // Administrators-owned (an elevated creator) and read-only extra entries are still trusted.
      const adminOwned = { attributes: ARCHIVE, acl: parseWindowsAclReport(report(`ace Allow ${user} ${FULL}`, `ace Allow S-1-5-32-545 ${READ_EXECUTE}`)
        .replace(`owner ${user}`, "owner S-1-5-32-544")) };
      expect(windowsRequestFileViolations({ ...regular, report: adminOwned })).toEqual([]);
    });

    it("refuses a file owned by another account, even with a private DACL", () => {
      const planted = { attributes: ARCHIVE, acl: parseWindowsAclReport(report(`ace Allow ${user} ${FULL}`).replace(`owner ${user}`, `owner ${other}`)) };
      expect(windowsRequestFileViolations({ ...regular, report: planted }))
        .toEqual([`owned or writable by other accounts (${other} (owner))`]);
    });

    it.each([["(W)", WRITE], ["(M)", MODIFY], ["(F)", FULL], ["GENERIC_WRITE", 0x40000000], ["WRITE_DAC", 0x40000]])(
      "refuses a file that grants another account %s", (_label, rights) => {
        expect(windowsRequestFileViolations({ ...regular, report: fileReport(`ace Allow ${user} ${FULL}`, `ace Allow ${other} ${rights}`) }))
          .toEqual([`owned or writable by other accounts (${other})`]);
      });

    it("refuses a NULL-DACL file, a reparse point, a symlink or junction, and a non-regular entry", () => {
      expect(windowsRequestFileViolations({ ...regular, report: { attributes: ARCHIVE, acl: parseWindowsAclReport(reportWith("D:NO_ACCESS_CONTROL")) } }))
        .toEqual(["owned or writable by other accounts (S-1-1-0 (NULL DACL))"]);
      expect(windowsRequestFileViolations({ ...regular, report: { ...privateFile(), attributes: ARCHIVE | REPARSE_POINT } as WindowsRequestFileReport }))
        .toEqual(["a reparse point"]);
      expect(windowsRequestFileViolations({ regularFile: false, symbolicLink: true, report: { ...privateFile(), attributes: REPARSE_POINT } as WindowsRequestFileReport }))
        .toEqual(["a symlink or junction", "a reparse point"]);
      expect(windowsRequestFileViolations({ regularFile: false, symbolicLink: false, report: privateFile() })).toEqual(["not a regular file"]);
      expect(windowsRequestFileViolations({ ...regular, report: { error: "Get-Acl failed" } })).toEqual(["unverifiable DACL (Get-Acl failed)"]);
    });

    it("parses one batch report per file; a bad file is an error entry, a bad batch throws", () => {
      const one = (sddl: string, ...lines: string[]) => [`attributes ${ARCHIVE}`, reportWith(sddl, ...lines)].join("\r\n");
      const batch = ["file 0", one(PRIVATE_SDDL, `ace Allow ${user} ${FULL}`), "file 1", "error", "file 2",
        one(PRIVATE_SDDL), "file 3", reportWith(PRIVATE_SDDL, `ace Allow ${user} ${FULL}`), "batch-end", ""].join("\r\n");
      const reports = parseWindowsFileAclBatch(batch, 4);
      expect(reports[0]).toMatchObject({ attributes: ARCHIVE, acl: { owner: user, aces: [{ sid: user }] } });
      expect(reports[1]).toEqual({ error: "Get-Acl failed" });
      expect(reports[2]).toMatchObject({ error: expect.stringMatching(/no access rules/) });
      expect(reports[3]).toEqual({ error: "missing file attributes" });
      expect(() => parseWindowsFileAclBatch(batch, 3)).toThrow(/covers 4 of 3/);
      expect(() => parseWindowsFileAclBatch(batch.replace("batch-end", ""), 4)).toThrow(/truncated/);
      expect(() => parseWindowsFileAclBatch(batch.replace("file 1", "file 2"), 4)).toThrow(/out-of-order/);
      expect(() => parseWindowsFileAclBatch(`stray\nfile 0\nerror\nbatch-end`, 1)).toThrow(/unparseable/);
    });

    const screenSetup = async (readFileAcls: (files: string[]) => Promise<WindowsRequestFileReport[]>, platform: NodeJS.Platform = "win32") => {
      const directory = path.join(tempRoot(), "requests");
      const guard = new WindowsRequestsGuard(directory, {
        platform, deps: { readAcl: async () => parseWindowsAclReport(ownerOnly), restrict: async () => undefined, readFileAcls },
      });
      await guard.prepare();
      return { directory, guard };
    };

    it("simulated win32 quarantines planted files in one batch read and returns only valid requests", async () => {
      const calls: string[][] = [];
      const byName: Record<string, () => WindowsRequestFileReport> = {
        "valid.json": privateFile,
        "foreign-owner.json": () => ({ attributes: ARCHIVE, acl: parseWindowsAclReport(report(`ace Allow ${user} ${FULL}`).replace(`owner ${user}`, `owner ${other}`)) }),
        "foreign-ace.json": () => fileReport(`ace Allow ${user} ${FULL}`, `ace Allow ${other} ${MODIFY}`),
        "reparse.json": () => ({ ...privateFile(), attributes: REPARSE_POINT } as WindowsRequestFileReport),
        "link.json": privateFile,
        "unreadable.json": () => ({ error: "Get-Acl failed" }),
      };
      const { directory, guard } = await screenSetup(async files => { calls.push(files); return files.map(file => byName[path.basename(file)]!()); });
      for (const name of Object.keys(byName)) if (name !== "link.json") fs.writeFileSync(path.join(directory, name), "{}");
      fs.symlinkSync(path.join(directory, "valid.json"), path.join(directory, "link.json"));
      const alarms = vi.spyOn(console, "error").mockImplementation(() => undefined);
      const names = [...Object.keys(byName), "vanished.json"];
      await expect(guard.screenPending(names)).resolves.toEqual(["valid.json"]);
      expect(calls).toHaveLength(1); // one Get-Acl process for the whole batch; a vanished entry is not queried
      expect(calls[0]!.map(file => path.basename(file))).toEqual(Object.keys(byName));
      const left = fs.readdirSync(directory).sort();
      expect(left.filter(name => name.endsWith(".json"))).toEqual(["valid.json"]);
      for (const name of ["foreign-owner.json", "foreign-ace.json", "reparse.json", "link.json", "unreadable.json"]) {
        expect(left.some(entry => entry.startsWith(`${name}.quarantine-`))).toBe(true);
      }
      expect(alarms.mock.calls.map(([line]) => String(line)).filter(line => line.includes("ALARM: quarantined"))).toHaveLength(5);
      expect(alarms.mock.calls.map(([line]) => String(line)).join("\n")).toMatch(new RegExp(`foreign-owner\\.json.*${other} \\(owner\\)`));
      alarms.mockRestore();
    });

    it("simulated win32 consumes nothing while the batch read fails, then serves again", async () => {
      let fail = true;
      const { directory, guard } = await screenSetup(async files => {
        if (fail) throw new Error("spawn powershell.exe ENOENT");
        return files.map(() => privateFile());
      });
      fs.writeFileSync(path.join(directory, "a.json"), "{}");
      const alarms = vi.spyOn(console, "error").mockImplementation(() => undefined);
      await expect(guard.screenPending(["a.json"])).resolves.toEqual([]);
      await expect(guard.screenPending(["a.json"])).resolves.toEqual([]);
      expect(alarms).toHaveBeenCalledOnce(); // one alarm per distinct failure, not one per poll
      expect(fs.readdirSync(directory)).toEqual(["a.json"]); // untouched, not quarantined
      fail = false;
      await expect(guard.screenPending(["a.json"])).resolves.toEqual(["a.json"]);
      alarms.mockRestore();
    });

    it("POSIX screens nothing", async () => {
      const { guard } = await screenSetup(async () => { throw new Error("unexpected file DACL query"); }, "linux");
      await expect(guard.screenPending(["x.json"])).resolves.toEqual(["x.json"]);
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
    expect(acl.dacl).toBe("present");
    expect(acl.aces.every(ace => [acl.user, "S-1-5-18", "S-1-5-32-544"].includes(ace.sid))).toBe(true);
    // A second ACE for a well-known group with write, as another local account would need.
    await promisify(execFile)(windowsSystemExecutable("icacls.exe"), [directory, "/grant", "*S-1-5-32-545:(OI)(CI)(W)"], { windowsHide: true });
    await expect(prepareResidentRequestsDirectory(directory))
      .rejects.toThrow(/Refusing to serve resident requests: .*writable by other accounts \(S-1-5-32-545\)/);
  }, 60_000);

  it("quarantines a pending request file that grants BUILTIN\\Users write and keeps a private one", async () => {
    const directory = path.join(tempRoot(), "requests");
    const guard = new WindowsRequestsGuard(directory);
    await guard.prepare();
    fs.writeFileSync(path.join(directory, "valid.json"), "{}");
    fs.writeFileSync(path.join(directory, "planted.json"), "{}");
    await promisify(execFile)(windowsSystemExecutable("icacls.exe"), [path.join(directory, "planted.json"), "/grant", "*S-1-5-32-545:(W)"], { windowsHide: true });
    const alarms = vi.spyOn(console, "error").mockImplementation(() => undefined);
    await expect(guard.screenPending(["valid.json", "planted.json"])).resolves.toEqual(["valid.json"]);
    expect(fs.readdirSync(directory).filter(name => name.endsWith(".json"))).toEqual(["valid.json"]);
    expect(alarms.mock.calls.map(([line]) => String(line)).join("\n")).toMatch(/ALARM: quarantined .*planted\.json.*S-1-5-32-545/);
    alarms.mockRestore();
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
