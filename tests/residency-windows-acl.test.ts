import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  assertWindowsRequestsDirectoryPrivate, assertWindowsResidentMetadataPrivate, decodeWindowsAclPath, encodeWindowsAclPath, parseWindowsAclReport,
  parseWindowsFileAclBatch, prepareResidentRequestsDirectory, readResidentMetadataFile, readWindowsDirectoryAcl, readWindowsFileAcls, windowsRequestFileViolations, windowsRequestsAclViolations, windowsSystemExecutable, WindowsRequestsGuard,
  WINDOWS_ACL_BATCH_ENV_BUDGET, WINDOWS_REQUESTS_ACL_TTL_MS, type WindowsDirectoryAcl, type WindowsRequestFileReport,
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
const privateBody = ownerOnly;
/**
 * Stand-in for the constant batch script: decodes each base64 token of FABRIC_ACL_PATHS, then writes
 * `file <i>` and either `path <re-encoded>`, `attributes` and the report, or `error` (reportFor returns undefined).
 */
const fakeBatch = (reportFor: (file: string) => string | undefined) => async (encoded: string): Promise<string> => {
  const out: string[] = [];
  encoded.split("\n").forEach((token, index) => {
    const file = decodeWindowsAclPath(token);
    const body = file === undefined ? undefined : reportFor(file);
    out.push(`file ${index}`);
    if (body === undefined) out.push("error");
    else out.push(`path ${encodeWindowsAclPath(file!)}`, "attributes 32", body);
  });
  out.push("batch-end");
  return out.join("\r\n");
};
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
      const files = ["C:\\r\\a.json", "C:\\r\\b.json", "C:\\r\\c.json", "C:\\r\\d.json", "C:\\r\\e.json", "C:\\r\\f.json"];
      const one = (file: string, sddl: string, ...lines: string[]) =>
        [`path ${encodeWindowsAclPath(file)}`, `attributes ${ARCHIVE}`, reportWith(sddl, ...lines)].join("\r\n");
      const batch = ["file 0", one(files[0]!, PRIVATE_SDDL, `ace Allow ${user} ${FULL}`), "file 1", "error", "file 2",
        one(files[2]!, PRIVATE_SDDL), "file 3", `path ${encodeWindowsAclPath(files[3]!)}`, reportWith(PRIVATE_SDDL, `ace Allow ${user} ${FULL}`),
        "file 4", one(files[0]!, PRIVATE_SDDL, `ace Allow ${user} ${FULL}`), "file 5", `attributes ${ARCHIVE}`, reportWith(PRIVATE_SDDL, `ace Allow ${user} ${FULL}`),
        "batch-end", ""].join("\r\n");
      const reports = parseWindowsFileAclBatch(batch, files);
      expect(reports[0]).toMatchObject({ attributes: ARCHIVE, acl: { owner: user, aces: [{ sid: user }] } });
      expect(reports[1]).toEqual({ error: "Get-Acl failed" });
      expect(reports[2]).toMatchObject({ error: expect.stringMatching(/no access rules/) });
      expect(reports[3]).toEqual({ error: "missing file attributes" });
      expect(reports[4]).toEqual({ error: "the report names another path" });
      expect(reports[5]).toEqual({ error: "missing file path" });
      expect(() => parseWindowsFileAclBatch(batch, files.slice(0, 5))).toThrow(/covers 6 of 5/);
      expect(() => parseWindowsFileAclBatch(batch.replace("batch-end", ""), files)).toThrow(/truncated/);
      expect(() => parseWindowsFileAclBatch(batch.replace("file 1", "file 2"), files)).toThrow(/out-of-order/);
      expect(() => parseWindowsFileAclBatch(`stray\nfile 0\nerror\nbatch-end`, files.slice(0, 1))).toThrow(/unparseable/);
    });

    it("encodes every path unambiguously: CR, LF, spaces and unpaired surrogates round-trip, junk does not decode", async () => {
      for (const name of ["C:\\r\\b\nc.json", "C:\\r\\d\r.json", "C:\\r\\file 0\r\nbatch-end.json", "C:\\r\\\uD800x.json", "C:\\r\\\uFFFDx.json"]) {
        const token = encodeWindowsAclPath(name);
        expect(token).toMatch(/^[A-Za-z0-9+/]+={0,2}$/);
        expect(decodeWindowsAclPath(token)).toBe(name);
      }
      expect(encodeWindowsAclPath("C:\\r\\\uD800x.json")).not.toBe(encodeWindowsAclPath("C:\\r\\\uFFFDx.json"));
      for (const junk of ["a b", "QQ", "QQ==", "QUJD", "QUFB", "not base64!", "QUE"]) expect(decodeWindowsAclPath(junk)).toBeUndefined();
      // A name made to look like report lines is still one report, bound to that exact path.
      const planted = "C:\\r\\x.json\nfile 1\nerror\nbatch-end";
      const envs: string[] = [];
      const reports = await readWindowsFileAcls([planted, "C:\\r\\y.json"], async encoded => { envs.push(encoded); return fakeBatch(() => privateBody)(encoded); });
      expect(envs).toHaveLength(1);
      expect(envs[0]!.split("\n").map(decodeWindowsAclPath)).toEqual([planted, "C:\\r\\y.json"]);
      expect(reports).toHaveLength(2);
      for (const entry of reports) expect(entry).toMatchObject({ attributes: ARCHIVE, acl: { owner: user } });
    });

    it("splits a large batch to stay within the environment budget; an oversized path is an error entry", async () => {
      const long = (n: number) => `C:\\r\\${String(n).padStart(4, "0")}${"x".repeat(3000)}.json`;
      const files = [long(1), long(2), long(3), long(4), long(5), `C:\\r\\${"y".repeat(20_000)}.json`];
      const envs: string[] = [];
      const reports = await readWindowsFileAcls(files, async encoded => { envs.push(encoded); return fakeBatch(() => privateBody)(encoded); });
      expect(envs.length).toBeGreaterThan(1);
      for (const env of envs) expect(env.length).toBeLessThanOrEqual(WINDOWS_ACL_BATCH_ENV_BUDGET);
      expect(envs.flatMap(env => env.split("\n").map(decodeWindowsAclPath))).toEqual(files.slice(0, 5));
      expect(reports.slice(0, 5).every(entry => "acl" in entry)).toBe(true);
      expect(reports[5]).toEqual({ error: "path too long for one ACL report" });
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
      for (const name of ["foreign-owner.json", "foreign-ace.json", "reparse.json", "link.json"]) {
        expect(left.some(entry => entry.startsWith(`${name}.quarantine-`))).toBe(true);
      }
      expect(alarms.mock.calls.map(([line]) => String(line)).filter(line => line.includes("ALARM: quarantined"))).toHaveLength(4);
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
      // One abort alarm and one "cannot isolate" alarm (the control path fails too: systemic), not one per poll.
      expect(alarms).toHaveBeenCalledTimes(2);
      expect(String(alarms.mock.calls[1]![0])).toMatch(/cannot isolate .*the report fails for the requests directory itself.*nothing quarantined/);
      expect(fs.readdirSync(directory)).toEqual(["a.json"]); // untouched, not quarantined
      fail = false;
      await expect(guard.screenPending(["a.json"])).resolves.toEqual(["a.json"]);
      alarms.mockRestore();
    });

    it.each([["unreadable", { error: "Get-Acl failed" }], ["malformed", { error: "missing file attributes" }]] as const)(
      "simulated win32 aborts the whole batch when one file's report is %s, then isolates and quarantines exactly that file", async (_kind, bad) => {
        const byName: Record<string, () => WindowsRequestFileReport> = {
          requests: privateFile, // the isolation control: the requests directory itself
          "valid.json": privateFile,
          "foreign-ace.json": () => fileReport(`ace Allow ${user} ${FULL}`, `ace Allow ${other} ${MODIFY}`),
          "bad.json": () => ({ ...bad }),
        };
        const calls: string[][] = [];
        const { directory, guard } = await screenSetup(async files => {
          calls.push(files.map(file => path.basename(file)));
          return files.map(file => byName[path.basename(file)]!());
        });
        const names = ["valid.json", "foreign-ace.json", "bad.json"];
        for (const name of names) fs.writeFileSync(path.join(directory, name), "{}");
        const alarms = vi.spyOn(console, "error").mockImplementation(() => undefined);
        await expect(guard.screenPending(names)).resolves.toEqual([]); // aborted: nothing consumed this poll
        expect(calls).toEqual([names, ["requests"], ["valid.json"], ["foreign-ace.json"], ["bad.json"]]);
        const lines = alarms.mock.calls.map(([line]) => String(line));
        expect(lines[0]).toMatch(new RegExp(`ALARM: not consuming resident requests .*bad\\.json: ${bad.error}`));
        expect(lines.slice(1)).toHaveLength(1);
        expect(lines[1]).toMatch(new RegExp(`ALARM: quarantined resident request .*bad\\.json as bad\\.json\\.quarantine-.* unverifiable DACL \\(${bad.error}\\)`));
        // Exactly the bad entry is quarantined; the valid sibling and the policy-violating one wait for the next poll.
        const left = fs.readdirSync(directory);
        expect(left.filter(name => name.endsWith(".json")).sort()).toEqual(["foreign-ace.json", "valid.json"]);
        expect(left.filter(name => name.startsWith("bad.json.quarantine-"))).toHaveLength(1);
        // Next poll: the host lists the directory again; the siblings proceed under the usual policy.
        await expect(guard.screenPending(fs.readdirSync(directory).filter(name => name.endsWith(".json")).sort())).resolves.toEqual(["valid.json"]);
        expect(fs.readdirSync(directory).some(entry => entry.startsWith("foreign-ace.json.quarantine-"))).toBe(true);
        alarms.mockRestore();
      });

    it("simulated win32 isolates a whole-batch failure to exactly the entry that causes it", async () => {
      const calls: string[][] = [];
      const { directory, guard } = await screenSetup(async files => {
        calls.push(files.map(file => path.basename(file)));
        // The batch process fails whenever the planted entry is in it; alone it yields a malformed report.
        if (files.some(file => path.basename(file) === "planted.json")) {
          if (files.length > 1) throw new Error("truncated file DACL batch report");
          return [{ error: "the report names another path" }];
        }
        return files.map(() => privateFile());
      });
      const names = ["a.json", "planted.json", "z.json"];
      for (const name of names) fs.writeFileSync(path.join(directory, name), "{}");
      const alarms = vi.spyOn(console, "error").mockImplementation(() => undefined);
      await expect(guard.screenPending(names)).resolves.toEqual([]);
      expect(fs.readdirSync(directory).filter(name => name.endsWith(".json")).sort()).toEqual(["a.json", "z.json"]);
      expect(fs.readdirSync(directory).filter(name => name.startsWith("planted.json.quarantine-"))).toHaveLength(1);
      const quarantined = alarms.mock.calls.map(([line]) => String(line)).filter(line => line.includes("ALARM: quarantined"));
      expect(quarantined).toHaveLength(1);
      expect(quarantined[0]).toContain(path.join(directory, "planted.json"));
      await expect(guard.screenPending(["a.json", "z.json"])).resolves.toEqual(["a.json", "z.json"]);
      expect(calls.at(-1)).toEqual(["a.json", "z.json"]);
      alarms.mockRestore();
    });

    it("simulated win32 quarantines a pending file whose name contains a newline; its siblings proceed on the next poll", async () => {
      const seen: string[] = [];
      // The real batch reader and parser over a stand-in for the PowerShell process: paths are carried base64-encoded,
      // and Get-Acl is taken to fail for a name containing CR or LF.
      const runner = async (encoded: string) => {
        seen.push(...encoded.split("\n").map(token => decodeWindowsAclPath(token)!));
        return fakeBatch(file => /[\r\n]/.test(path.basename(file)) ? undefined : privateBody)(encoded);
      };
      const { directory, guard } = await screenSetup(files => readWindowsFileAcls(files, runner));
      const names = ["a.json", "b\nc.json", "d.json"];
      for (const name of names) fs.writeFileSync(path.join(directory, name), "{}");
      const alarms = vi.spyOn(console, "error").mockImplementation(() => undefined);
      await expect(guard.screenPending(names)).resolves.toEqual([]); // the batch is aborted, never thrown out of the poll
      expect(seen).toContain(path.join(directory, "b\nc.json")); // carried exactly
      const left = fs.readdirSync(directory);
      expect(left.filter(name => name.endsWith(".json")).sort()).toEqual(["a.json", "d.json"]);
      expect(left.filter(name => name.startsWith("b\nc.json.quarantine-"))).toHaveLength(1);
      expect(alarms.mock.calls.map(([line]) => String(line)).join("\n")).toContain(`ALARM: quarantined resident request ${path.join(directory, "b\nc.json")}`);
      await expect(guard.screenPending(fs.readdirSync(directory).filter(name => name.endsWith(".json")).sort())).resolves.toEqual(["a.json", "d.json"]);
      alarms.mockRestore();
    });

    it("POSIX screens nothing", async () => {
      const { guard } = await screenSetup(async () => { throw new Error("unexpected file DACL query"); }, "linux");
      await expect(guard.screenPending(["x.json"])).resolves.toEqual(["x.json"]);
    });
  });

  describe("resident metadata (config.json, owner.json), same report and policy as request files", () => {
    const ARCHIVE = 0x20, REPARSE_POINT = 0x400;
    const acl = (...lines: string[]) => parseWindowsAclReport(report(...lines));
    const privateFile = (): WindowsRequestFileReport => ({ attributes: ARCHIVE,
      acl: acl(`ace Allow ${user} ${FULL}`, `ace Allow S-1-5-18 ${FULL}`, `ace Allow S-1-5-32-544 ${FULL}`) });
    const privateDirectory = async () => parseWindowsAclReport(ownerOnly);
    const metadata = () => {
      const root = tempRoot();
      const files = [path.join(root, "config.json"), path.join(root, "owner.json")];
      for (const file of files) fs.writeFileSync(file, "{}");
      return files;
    };
    const check = (files: string[], refusing: string, readFileAcls: (files: string[]) => Promise<WindowsRequestFileReport[]>,
      readAcl: (directory: string) => Promise<WindowsDirectoryAcl> = privateDirectory) =>
      assertWindowsResidentMetadataPrivate(files, refusing, "win32", readFileAcls, readAcl);

    it("simulated win32 passes a private directory and private files in one batch read, and pins their identities", async () => {
      const files = metadata();
      const calls: string[][] = [];
      const directories: string[] = [];
      const pins = await check(files, "stop/remove", async batch => { calls.push(batch); return batch.map(() => privateFile()); },
        async directory => { directories.push(directory); return parseWindowsAclReport(ownerOnly); });
      expect(calls).toEqual([files]);
      expect(directories).toEqual([path.dirname(files[0]!)]);
      expect([...pins!.keys()]).toEqual(files);
      expect(readResidentMetadataFile(files[0]!, pins, "stop/remove")).toBe("{}");
    });

    it("simulated win32 refuses a foreign write ACE, a foreign owner, a NULL DACL and a reparse point", async () => {
      const files = metadata();
      const refuse = (bad: WindowsRequestFileReport, index: number) =>
        check(files, "stop/remove", async batch => batch.map((_, i) => i === index ? bad : privateFile()));
      await expect(refuse({ attributes: ARCHIVE, acl: acl(`ace Allow ${user} ${FULL}`, `ace Allow ${other} ${WRITE}`) }, 0)).rejects.toThrow(
        `Resident metadata is not private to this OS user: ${files[0]} (owned or writable by other accounts (${other})); refusing stop/remove`);
      await expect(refuse({ attributes: ARCHIVE, acl: parseWindowsAclReport(report(`ace Allow ${user} ${FULL}`).replace(`owner ${user}`, `owner ${other}`)) }, 1))
        .rejects.toThrow(`Resident metadata is not private to this OS user: ${files[1]} (owned or writable by other accounts (${other} (owner)))`);
      await expect(refuse({ attributes: ARCHIVE, acl: parseWindowsAclReport(reportWith("D:NO_ACCESS_CONTROL")) }, 1)).rejects.toThrow(/NULL DACL/);
      await expect(refuse({ ...privateFile(), attributes: ARCHIVE | REPARSE_POINT } as WindowsRequestFileReport, 0)).rejects.toThrow(/a reparse point\); refusing/);
    });

    it("simulated win32 refuses when the files stay owned by the caller but another SID may write the resident directory", async () => {
      const files = metadata();
      const directory = path.dirname(files[0]!);
      const fileQueries: string[][] = [];
      const wide = async () => parseWindowsAclReport(report(`ace Allow ${user} ${FULL}`, `ace Allow ${other} ${MODIFY}`));
      await expect(check(files, "stop/remove", async batch => { fileQueries.push(batch); return batch.map(() => privateFile()); }, wide)).rejects.toThrow(
        `Resident directory is not private to this OS user: ${directory} (owned or writable by other accounts (${other})); refusing stop/remove`);
      expect(fileQueries).toEqual([]); // the container is judged first
      await expect(check(files, "offline removal", async batch => batch.map(() => privateFile()), async () => { throw new Error("Get-Acl failed"); }))
        .rejects.toThrow(`Cannot verify that the resident metadata is private to this Windows user (${directory}: Get-Acl failed); refusing offline removal`);
    });

    it("simulated win32 refuses a file replaced while its ACL is verified, and a read of a file replaced after it", async () => {
      const files = metadata();
      await expect(check(files, "stop/remove", async batch => {
        // Same owner, private report, but a different file now sits at the name.
        fs.writeFileSync(`${files[0]}.new`, "{\"actorRoot\":\"elsewhere\"}"); fs.renameSync(`${files[0]}.new`, files[0]!);
        return batch.map(() => privateFile());
      })).rejects.toThrow(`Resident metadata was replaced while its ACL was verified: ${files[0]}; refusing stop/remove`);
      const pins = await check(files, "stop/remove", async batch => batch.map(() => privateFile()));
      fs.writeFileSync(`${files[1]}.new`, "{\"pid\":1}"); fs.renameSync(`${files[1]}.new`, files[1]!);
      expect(() => readResidentMetadataFile(files[1]!, pins, "stop/remove"))
        .toThrow(`Resident metadata was replaced after its ACL was verified: ${files[1]}; refusing stop/remove`);
      expect(readResidentMetadataFile(files[0]!, pins, "stop/remove")).toContain("elsewhere");
      expect(readResidentMetadataFile(files[1]!, undefined, "stop/remove")).toBe("{\"pid\":1}"); // POSIX: no pins
    });

    it("simulated win32 refuses an unreadable report, a failed or short batch, and a symlink", async () => {
      const files = metadata();
      await expect(check(files, "offline removal", async batch => batch.map((_, i) => i ? { error: "Get-Acl failed" } : privateFile())))
        .rejects.toThrow(`Cannot verify that the resident metadata is private to this Windows user (${files[1]}: Get-Acl failed); refusing offline removal`);
      await expect(check(files, "offline removal", async () => { throw new Error("spawn powershell.exe ENOENT"); }))
        .rejects.toThrow("(spawn powershell.exe ENOENT); refusing offline removal");
      await expect(check(files, "offline removal", async () => [privateFile()])).rejects.toThrow("expected 2 file reports, got 1");
      if (process.platform !== "win32") {
        const link = path.join(path.dirname(files[0]!), "link.json"); fs.symlinkSync(files[0]!, link);
        await expect(check([link], "stop/remove", async batch => batch.map(() => privateFile())))
          .rejects.toThrow(/a symlink or junction/);
      }
    });

    it("POSIX does not query", async () => {
      await expect(assertWindowsResidentMetadataPrivate(["/nonexistent/config.json"], "stop/remove", "linux",
        async () => { throw new Error("unexpected file DACL query"); }, async () => { throw new Error("unexpected DACL query"); })).resolves.toBeUndefined();
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
