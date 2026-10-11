import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

export const WINDOWS_SYSTEM_SID = "S-1-5-18";
export const WINDOWS_ADMINISTRATORS_SID = "S-1-5-32-544";
export const WINDOWS_EVERYONE_SID = "S-1-1-0";
const sidPattern = /^S-1-\d+(-\d+)+$/;

/**
 * Rights that let a principal plant, replace or re-permission request files:
 * FILE_ADD_FILE/FILE_WRITE_DATA (0x2), FILE_ADD_SUBDIRECTORY (0x4),
 * FILE_DELETE_CHILD (0x40), DELETE (0x10000), WRITE_DAC (0x40000),
 * WRITE_OWNER (0x80000), GENERIC_ALL (0x10000000), GENERIC_WRITE (0x40000000).
 * Write (W), Modify (M) and Full (F) all include FILE_ADD_FILE.
 */
export const WINDOWS_WRITE_CLASS_RIGHTS = 0x2 | 0x4 | 0x40 | 0x10000 | 0x40000 | 0x80000 | 0x10000000 | 0x40000000;

export interface WindowsDirectoryAce { type: "Allow" | "Deny"; sid: string; rights: number }
/**
 * "present": the descriptor carries a DACL with at least one ACE. "null": a NULL
 * DACL (SDDL `D:NO_ACCESS_CONTROL`), which grants everyone full access while
 * GetAccessRules returns no entries. "absent": no DACL at all, which Windows
 * treats like a NULL DACL. Only "present" can be private.
 */
export type WindowsDaclState = "present" | "null" | "absent";
export interface WindowsDirectoryAcl { user: string; owner: string; dacl: WindowsDaclState; aces: WindowsDirectoryAce[] }

export function windowsSystemExecutable(...segments: string[]): string {
  return path.win32.join(process.env.SystemRoot ?? "C:\\Windows", "System32", ...segments);
}

/** Fixed Windows PowerShell path; scripts receive paths only through the environment. */
export function windowsPowerShellPath(): string {
  return windowsSystemExecutable("WindowsPowerShell", "v1.0", "powershell.exe");
}

// No double quotes: Windows argv quoting of " is unreliable for powershell.exe -Command.
// SIDs come from GetAccessRules(..., SecurityIdentifier), so output is locale independent.
// One report of $acl; `emit` wraps each line expression (console write or buffered add).
const aclReportStatements = (emit: (expression: string) => string): string[] => [
  emit("'user ' + [System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value"),
  emit("'owner ' + $acl.GetOwner($sid).Value"),
  // The raw DACL in SDDL: empty when absent, D:NO_ACCESS_CONTROL when NULL. GetAccessRules hides both.
  emit("'dacl ' + $acl.GetSecurityDescriptorSddlForm('Access')"),
  `foreach ($r in $acl.GetAccessRules($true, $true, $sid)) { ${emit("'ace ' + $r.AccessControlType.ToString() + ' ' + $r.IdentityReference.Value + ' ' + [string][int64]$r.FileSystemRights.value__")} }`,
  emit("'end'"),
];
const writeLine = (expression: string) => `[Console]::Out.WriteLine(${expression})`;
const aclReportScript = [
  "$ErrorActionPreference = 'Stop'",
  "$sid = [System.Security.Principal.SecurityIdentifier]",
  "$acl = Get-Acl -LiteralPath $env:FABRIC_ACL_PATH",
  ...aclReportStatements(writeLine),
].join("; ");

/**
 * Batch report for pending request files, one constant script for all of them.
 * Paths arrive newline-separated in FABRIC_ACL_PATHS. Each file yields
 * `file <index>`, then `attributes <FileAttributes>` (from the entry itself, so a
 * reparse point reports ReparsePoint) and the same report as one directory, or
 * `error` alone when it cannot be read. A file's lines are buffered and written
 * only once complete, so a failure never leaves a partial report.
 */
const aclBatchReportScript = [
  "$ErrorActionPreference = 'Stop'",
  "$sid = [System.Security.Principal.SecurityIdentifier]",
  "$i = 0",
  "foreach ($p in $env:FABRIC_ACL_PATHS.Split([char]10)) { try { " + [
    "$o = [System.Collections.Generic.List[string]]::new()",
    "$o.Add('attributes ' + [string][int64]([System.IO.File]::GetAttributes($p)))",
    "$acl = Get-Acl -LiteralPath $p",
    ...aclReportStatements(expression => `$o.Add(${expression})`),
    writeLine("'file ' + $i"),
    "foreach ($l in $o) { [Console]::Out.WriteLine($l) }",
  ].join("; ") + " } catch { " + writeLine("'file ' + $i") + "; " + writeLine("'error'") + " }; $i++ }",
  writeLine("'batch-end'"),
].join("; ");

const sddlDaclFlags = /^(?:P|AI|AR|NO_ACCESS_CONTROL)*$/;

/**
 * Classify the access (DACL) section of an SDDL string. An empty DACL, a missing
 * `D:` prefix or anything else unexpected throws, so callers fail closed.
 */
export function windowsDaclStateFromSddl(sddl: string): WindowsDaclState {
  if (sddl === "") return "absent";
  if (!sddl.startsWith("D:")) throw new Error(`unparseable DACL SDDL: ${sddl.slice(0, 120)}`);
  const body = sddl.slice(2);
  const open = body.indexOf("(");
  const flags = open === -1 ? body : body.slice(0, open);
  if (!sddlDaclFlags.test(flags)) throw new Error(`unparseable DACL SDDL flags: ${flags.slice(0, 120)}`);
  if (flags.includes("NO_ACCESS_CONTROL")) return "null";
  let depth = 0;
  let aces = 0;
  for (const char of open === -1 ? "" : body.slice(open)) {
    if (char === "(") { if (depth++ === 0) aces++; }
    else if (char === ")") { if (--depth < 0) break; }
    else if (depth === 0) { depth = -1; break; }
  }
  if (depth !== 0) throw new Error(`unparseable DACL SDDL ACEs: ${sddl.slice(0, 120)}`);
  if (aces === 0) throw new Error("the DACL has no entries");
  return "present";
}

/** Parse the Get-Acl report. Anything unexpected throws, so callers fail closed. */
export function parseWindowsAclReport(output: string): WindowsDirectoryAcl {
  const lines = output.split(/\r?\n/).map(line => line.trim()).filter(Boolean);
  if (lines.at(-1) !== "end") throw new Error("truncated DACL report");
  let user: string | undefined;
  let owner: string | undefined;
  let dacl: WindowsDaclState | undefined;
  const aces: WindowsDirectoryAce[] = [];
  for (const line of lines.slice(0, -1)) {
    const fields = line.split(" ");
    if (fields[0] === "user" && fields.length === 2 && user === undefined && sidPattern.test(fields[1]!)) user = fields[1];
    else if (fields[0] === "owner" && fields.length === 2 && owner === undefined && sidPattern.test(fields[1]!)) owner = fields[1];
    else if (fields[0] === "dacl" && dacl === undefined && (line === "dacl" || line.startsWith("dacl "))) {
      dacl = windowsDaclStateFromSddl(line.slice(5));
    } else if (fields[0] === "ace" && fields.length === 4 && (fields[1] === "Allow" || fields[1] === "Deny") &&
      sidPattern.test(fields[2]!) && /^-?\d+$/.test(fields[3]!)) {
      aces.push({ type: fields[1], sid: fields[2]!, rights: Number(fields[3]) });
    } else throw new Error(`unparseable DACL report line: ${line.slice(0, 120)}`);
  }
  if (!user || !owner) throw new Error("DACL report lacks the user or owner SID");
  if (dacl === undefined) throw new Error("DACL report lacks the DACL SDDL");
  // A real DACL whose entries GetAccessRules did not list cannot be judged from the list.
  if (dacl === "present" && aces.length === 0) throw new Error("DACL report lists no access rules for a non-empty DACL");
  return { user, owner, dacl, aces };
}

/** SIDs other than this user, SYSTEM and Administrators that own or may write the directory. */
export function windowsRequestsAclViolations(acl: WindowsDirectoryAcl): string[] {
  const trusted = new Set([acl.user, WINDOWS_SYSTEM_SID, WINDOWS_ADMINISTRATORS_SID]);
  const extra = new Set<string>();
  // The owner can always rewrite the DACL, whatever it currently says.
  if (!trusted.has(acl.owner)) extra.add(`${acl.owner} (owner)`);
  // A NULL or missing DACL grants everyone (S-1-1-0) full access and lists no entries.
  if (acl.dacl === "null") extra.add(`${WINDOWS_EVERYONE_SID} (NULL DACL)`);
  else if (acl.dacl !== "present") extra.add(`${WINDOWS_EVERYONE_SID} (no DACL)`);
  for (const ace of acl.aces) {
    // Deny entries only narrow access; any allowed write-class bit is a grant.
    if (ace.type === "Allow" && !trusted.has(ace.sid) && (ace.rights & WINDOWS_WRITE_CLASS_RIGHTS) !== 0) extra.add(ace.sid);
  }
  return [...extra];
}

/** FILE_ATTRIBUTE_DIRECTORY and FILE_ATTRIBUTE_REPARSE_POINT (symlinks, junctions, other reparse tags). */
export const WINDOWS_FILE_ATTRIBUTE_DIRECTORY = 0x10;
export const WINDOWS_FILE_ATTRIBUTE_REPARSE_POINT = 0x400;

/** One pending request file: its attributes and ACL, or why they could not be read. */
export type WindowsRequestFileReport = { attributes: number; acl: WindowsDirectoryAcl } | { error: string };

/**
 * Parse the batch report for `count` files. A malformed batch throws (nothing is
 * judged); a malformed or unreadable single file becomes an `error` entry, which
 * the callers treat as a failed batch (nothing is consumed or trusted).
 */
export function parseWindowsFileAclBatch(output: string, count: number): WindowsRequestFileReport[] {
  const lines = output.split(/\r?\n/).map(line => line.trim()).filter(Boolean);
  if (lines.at(-1) !== "batch-end") throw new Error("truncated file DACL batch report");
  const sections: string[][] = [];
  for (const line of lines.slice(0, -1)) {
    if (line.startsWith("file ")) {
      if (line !== `file ${sections.length}`) throw new Error(`out-of-order file DACL batch report: ${line.slice(0, 120)}`);
      sections.push([]);
    } else if (sections.length === 0) throw new Error(`unparseable file DACL batch line: ${line.slice(0, 120)}`);
    else sections.at(-1)!.push(line);
  }
  if (sections.length !== count) throw new Error(`file DACL batch report covers ${sections.length} of ${count} files`);
  return sections.map((section): WindowsRequestFileReport => {
    if (section.length === 1 && section[0] === "error") return { error: "Get-Acl failed" };
    const match = /^attributes (\d+)$/.exec(section[0] ?? "");
    if (!match) return { error: "missing file attributes" };
    try {
      return { attributes: Number(match[1]), acl: parseWindowsAclReport(section.slice(1).join("\n")) };
    } catch (error) {
      return { error: error instanceof Error ? error.message : String(error) };
    }
  });
}

/** What the host knows about a pending request before it may consume it. */
export interface WindowsRequestFileEvidence {
  /** From lstat: a regular file, and not a symlink or junction (lstat does not follow either). */
  regularFile: boolean;
  symbolicLink: boolean;
  /** The batch report for this file. */
  report: WindowsRequestFileReport;
}

/**
 * Reasons a pending request file must not be consumed; empty means it may be.
 * It must be a regular file that is not a reparse point, owned by this user,
 * SYSTEM or Administrators (the set trusted for the directory), and no other SID
 * may hold an allowed write-class right on it. A directory DACL tightened after
 * the file was planted does not change the file's own owner or DACL.
 */
export function windowsRequestFileViolations(evidence: WindowsRequestFileEvidence): string[] {
  const violations: string[] = [];
  if (evidence.symbolicLink) violations.push("a symlink or junction");
  else if (!evidence.regularFile) violations.push("not a regular file");
  if ("error" in evidence.report) return [...violations, `unverifiable DACL (${evidence.report.error})`];
  const { attributes, acl } = evidence.report;
  if ((attributes & WINDOWS_FILE_ATTRIBUTE_REPARSE_POINT) !== 0) violations.push("a reparse point");
  if ((attributes & WINDOWS_FILE_ATTRIBUTE_DIRECTORY) !== 0 && evidence.regularFile) violations.push("a directory");
  const foreign = windowsRequestsAclViolations(acl);
  if (foreign.length > 0) violations.push(`owned or writable by other accounts (${foreign.join(", ")})`);
  return violations;
}

/** One Get-Acl process for all pending files: the constant batch script, fixed argv, paths in the environment. */
export async function readWindowsFileAcls(files: string[]): Promise<WindowsRequestFileReport[]> {
  if (files.length === 0) return [];
  if (files.some(file => /[\r\n]/.test(file))) throw new Error("path contains a line break");
  const { stdout } = await execFileAsync(windowsPowerShellPath(), ["-NoProfile", "-NonInteractive", "-Command", aclBatchReportScript], {
    encoding: "utf8", windowsHide: true, timeout: 30_000, env: { ...process.env, FABRIC_ACL_PATHS: files.join("\n") },
  });
  return parseWindowsFileAclBatch(stdout, files.length);
}

export async function readWindowsDirectoryAcl(directory: string): Promise<WindowsDirectoryAcl> {
  if (/[\r\n]/.test(directory)) throw new Error("path contains a line break");
  const { stdout } = await execFileAsync(windowsPowerShellPath(), ["-NoProfile", "-NonInteractive", "-Command", aclReportScript], {
    encoding: "utf8", windowsHide: true, timeout: 30_000, env: { ...process.env, FABRIC_ACL_PATH: directory },
  });
  return parseWindowsAclReport(stdout);
}

function privateGrants(userSid: string): string[] {
  return [userSid, WINDOWS_SYSTEM_SID, WINDOWS_ADMINISTRATORS_SID].map(sid => `*${sid}:(OI)(CI)F`);
}

/** Replace inherited ACEs with explicit owner, SYSTEM and Administrators grants. */
export async function restrictWindowsDirectory(directory: string, userSid: string): Promise<void> {
  if (!sidPattern.test(userSid)) throw new Error(`malformed user SID: ${userSid}`);
  await execFileAsync(windowsSystemExecutable("icacls.exe"), [directory, "/inheritance:r", "/grant:r", ...privateGrants(userSid)], {
    encoding: "utf8", windowsHide: true, timeout: 30_000,
  });
}

export interface WindowsRequestsAclDeps {
  readAcl: (directory: string) => Promise<WindowsDirectoryAcl>;
  restrict: (directory: string, userSid: string) => Promise<void>;
  /** Batch reader for pending request files; defaults to the product adapter below. */
  readFileAcls?: (files: string[]) => Promise<WindowsRequestFileReport[]>;
}
/** Platform adapters, read per call; tests that inject win32 on another OS replace these explicitly. */
export const windowsRequestsAclAdapters: Required<WindowsRequestsAclDeps> = {
  readAcl: readWindowsDirectoryAcl, restrict: restrictWindowsDirectory, readFileAcls: readWindowsFileAcls,
};

/**
 * Windows: fail closed unless each resident metadata file (config.json, owner.json) is as private as a pending
 * request file, judged by the same batch report and policy (windowsRequestFileViolations). Whoever can write these
 * files picks the root, mesh, process and paths an operator tool acts on, so an owner SID check alone is not
 * enough: a foreign owner, a foreign write-class ACE, a NULL or missing DACL, a reparse point, or an unreadable
 * report refuses. POSIX: no-op (callers check the uid).
 */
export async function assertWindowsResidentMetadataPrivate(files: string[], refusing: string,
  platform: NodeJS.Platform = process.platform,
  readFileAcls: (files: string[]) => Promise<WindowsRequestFileReport[]> = windowsRequestsAclAdapters.readFileAcls): Promise<void> {
  if (platform !== "win32") return;
  const cannot = (reason: string) =>
    new Error(`Cannot verify that the resident metadata is private to this Windows user (${reason}); refusing ${refusing}`);
  const entries = files.map(file => {
    const stat = fs.lstatSync(file);
    return { file, regularFile: stat.isFile(), symbolicLink: stat.isSymbolicLink() };
  });
  let reports: WindowsRequestFileReport[];
  try {
    reports = await readFileAcls(files);
  } catch (error) {
    throw cannot(error instanceof Error ? error.message : String(error));
  }
  if (reports.length !== files.length) throw cannot(`expected ${files.length} file reports, got ${reports.length}`);
  entries.forEach(({ file, regularFile, symbolicLink }, index) => {
    const report = reports[index]!;
    if ("error" in report) throw cannot(`${file}: ${report.error}`);
    const violations = windowsRequestFileViolations({ regularFile, symbolicLink, report });
    if (violations.length > 0) {
      throw new Error(`Resident metadata is not private to this OS user: ${file} (${violations.join("; ")}); refusing ${refusing}`);
    }
  });
}

/** Fail closed unless only this user, SYSTEM and Administrators own or may write the directory. */
export async function assertWindowsRequestsDirectoryPrivate(directory: string,
  readAcl: WindowsRequestsAclDeps["readAcl"] = windowsRequestsAclAdapters.readAcl): Promise<void> {
  let acl: WindowsDirectoryAcl;
  try {
    acl = await readAcl(directory);
  } catch (error) {
    throw new Error(`Refusing to serve resident requests: cannot verify the DACL of ${directory} ` +
      `(${error instanceof Error ? error.message : String(error)})`);
  }
  const extra = windowsRequestsAclViolations(acl);
  if (extra.length === 0) return;
  throw new Error(`Refusing to serve resident requests: ${directory} is writable by other accounts (${extra.join(", ")}); ` +
    `only ${acl.user}, SYSTEM and Administrators may own or write it. Remove the directory while no request ` +
    `is pending; the host recreates it with a private DACL.`);
}

/** Owner + SYSTEM + Administrators only; otherwise throw `Refusing to serve resident requests: ...`. */
async function assertPrivateDirectory(directory: string, readAcl: WindowsRequestsAclDeps["readAcl"], role: string): Promise<WindowsDirectoryAcl> {
  let acl: WindowsDirectoryAcl;
  try {
    acl = await readAcl(directory);
  } catch (error) {
    throw new Error(`Refusing to serve resident requests: cannot verify the DACL of ${role} ${directory} ` +
      `(${error instanceof Error ? error.message : String(error)})`);
  }
  const extra = windowsRequestsAclViolations(acl);
  if (extra.length === 0) return acl;
  throw new Error(`Refusing to serve resident requests: ${role} ${directory} is writable by other accounts (${extra.join(", ")}); ` +
    `only ${acl.user}, SYSTEM and Administrators may own or write it, because an account that can write it can plant ` +
    `or replace request files.`);
}

/**
 * Identity of a real directory (not a symlink or junction): volume, file id and
 * creation time. On Windows the bigint `ino` is the NTFS file index, so a
 * directory removed and recreated under the same name gets a new identity.
 */
export function windowsDirectoryIdentity(directory: string): string {
  const stat = fs.lstatSync(directory, { bigint: true });
  if (!stat.isDirectory()) throw new Error(`${directory} is not a directory`);
  return `${stat.dev}:${stat.ino}:${stat.birthtimeNs}`;
}

/**
 * Create the residency `requests` directory and, on Windows, prove that no other
 * account can plant a request in it before the host consumes any request file.
 * The residency directory and its parent are verified first: an account that can
 * write either could replace `requests` or plant files during creation. A new
 * `requests` directory is then created under a private staging name inside the
 * verified residency directory (so it inherits only private ACEs from its first
 * moment), restricted to explicit grants, verified private and empty, and only
 * then renamed into place. Returns the verified directory identity on Windows.
 */
export async function prepareResidentRequestsDirectory(directory: string, platform: NodeJS.Platform = process.platform,
  deps: WindowsRequestsAclDeps = windowsRequestsAclAdapters): Promise<string | undefined> {
  if (platform !== "win32") { fs.mkdirSync(directory, { recursive: true, mode: 0o700 }); return undefined; }
  const residency = path.dirname(directory);
  fs.mkdirSync(residency, { recursive: true, mode: 0o700 });
  await assertPrivateDirectory(path.dirname(residency), deps.readAcl, "the residency parent");
  await assertPrivateDirectory(residency, deps.readAcl, "the residency directory");
  if (!fs.existsSync(directory)) {
    const staging = `${directory}.create-${randomUUID()}`;
    fs.mkdirSync(staging, { mode: 0o700 });
    try {
      const { user } = await deps.readAcl(staging);
      await deps.restrict(staging, user);
      await assertPrivateDirectory(staging, deps.readAcl, "the requests staging directory");
      if (fs.readdirSync(staging).length !== 0) {
        throw new Error(`Refusing to serve resident requests: the requests staging directory ${staging} is not empty`);
      }
      fs.renameSync(staging, directory);
    } catch (error) {
      fs.rmSync(staging, { recursive: true, force: true });
      // A concurrent creator won the name; the check below decides whether it is private.
      if (!fs.existsSync(directory)) throw error;
    }
  }
  const identity = windowsDirectoryIdentity(directory);
  await assertWindowsRequestsDirectoryPrivate(directory, deps.readAcl);
  if (windowsDirectoryIdentity(directory) !== identity) {
    throw new Error(`Refusing to serve resident requests: ${directory} was replaced while its DACL was verified`);
  }
  return identity;
}

/** A verified requests DACL is trusted for at most this long, and only for the same directory identity. */
export const WINDOWS_REQUESTS_ACL_TTL_MS = 60_000;

export interface WindowsRequestsGuardOptions {
  platform?: NodeJS.Platform;
  deps?: WindowsRequestsAclDeps;
  now?: () => number;
  identity?: (directory: string) => string;
}

/**
 * Guards consumption from the unauthenticated `requests` directory on Windows.
 * `prepare()` runs once at startup; `assertBeforeConsume()` runs before each
 * non-empty request batch. It re-checks the directory identity every time and
 * re-reads the DACL at most once per TTL for that identity. A replaced
 * directory or a widened DACL latches a refusal: the host stops serving
 * requests until it restarts, which re-verifies from scratch. POSIX is unchanged.
 */
export class WindowsRequestsGuard {
  #platform: NodeJS.Platform | undefined;
  #verified: { identity: string; at: number } | undefined;
  #refusal: Error | undefined;
  #screenFailure: string | undefined;
  /** Refused files whose quarantine rename failed: never consumed, alarmed once. */
  readonly #unquarantined = new Set<string>();
  readonly #deps: () => WindowsRequestsAclDeps;
  readonly #now: () => number;
  readonly #identity: (directory: string) => string;
  readonly #options: WindowsRequestsGuardOptions;
  readonly directory: string;

  constructor(directory: string, options: WindowsRequestsGuardOptions = {}) {
    this.directory = directory;
    this.#options = options;
    this.#deps = () => options.deps ?? windowsRequestsAclAdapters;
    this.#now = options.now ?? Date.now;
    this.#identity = options.identity ?? windowsDirectoryIdentity;
  }

  /** The latched refusal, once requests are no longer served. */
  get refusal(): Error | undefined { return this.#refusal; }

  async prepare(): Promise<void> {
    this.#platform = this.#options.platform ?? process.platform;
    const identity = await prepareResidentRequestsDirectory(this.directory, this.#platform, this.#deps());
    if (identity !== undefined) this.#verified = { identity, at: this.#now() };
  }

  /**
   * Windows: the pending entries (names in this directory) that may be consumed.
   * The directory DACL says nothing about files planted before it was tightened,
   * so each file is checked (see windowsRequestFileViolations) with one batch ACL
   * read. A file with a clean report that violates the policy is renamed to a
   * quarantine name (no `.json` suffix, so it is never enumerated again) with a
   * logged alarm, and is never returned. If the batch read fails, or any single
   * file's report is unreadable or malformed, the whole batch is aborted: nothing
   * is returned or quarantined this time, and each distinct failure alarms once.
   * POSIX returns the entries unchanged.
   */
  async screenPending(entries: string[]): Promise<string[]> {
    if (this.#platform !== "win32" || entries.length === 0) return entries;
    const candidates: { entry: string; file: string; regularFile: boolean; symbolicLink: boolean }[] = [];
    for (const entry of entries) {
      const file = path.join(this.directory, entry);
      if (this.#unquarantined.has(file)) continue; // refused and logged once already
      let stat: fs.Stats;
      try { stat = fs.lstatSync(file); } catch { continue; } // gone: consumed or removed meanwhile
      candidates.push({ entry, file, regularFile: stat.isFile(), symbolicLink: stat.isSymbolicLink() });
    }
    if (candidates.length === 0) return [];
    let reports: WindowsRequestFileReport[];
    try {
      const readFileAcls = this.#deps().readFileAcls ?? windowsRequestsAclAdapters.readFileAcls;
      reports = await readFileAcls(candidates.map(candidate => candidate.file));
      if (reports.length !== candidates.length) throw new Error(`expected ${candidates.length} file reports, got ${reports.length}`);
      // One unreadable or malformed per-file report aborts the whole batch; valid siblings wait for the next poll.
      const unreadable = reports.findIndex(report => "error" in report);
      if (unreadable !== -1) {
        throw new Error(`${candidates[unreadable]!.file}: ${(reports[unreadable] as { error: string }).error}`);
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      if (message !== this.#screenFailure) {
        console.error(`[pi-fabric] ALARM: not consuming resident requests in ${this.directory}: cannot verify the request files (${message}); retrying`);
      }
      this.#screenFailure = message;
      return [];
    }
    this.#screenFailure = undefined;
    const accepted: string[] = [];
    candidates.forEach((candidate, index) => {
      const violations = windowsRequestFileViolations({ ...candidate, report: reports[index]! });
      if (violations.length === 0) { accepted.push(candidate.entry); return; }
      const quarantine = `${candidate.file}.quarantine-${randomUUID()}`;
      try {
        fs.renameSync(candidate.file, quarantine);
        console.error(`[pi-fabric] ALARM: quarantined resident request ${candidate.file} as ${path.basename(quarantine)} ` +
          `without consuming it: ${violations.join("; ")}`);
      } catch (error) {
        this.#unquarantined.add(candidate.file);
        console.error(`[pi-fabric] ALARM: refusing resident request ${candidate.file} (${violations.join("; ")}); ` +
          `quarantine failed (${error instanceof Error ? error.message : String(error)}), so it is skipped`);
      }
    });
    return accepted;
  }

  /** Throws (and latches) unless the verified requests directory is still in place and private. */
  async assertBeforeConsume(): Promise<void> {
    if (this.#platform !== "win32") return;
    if (this.#refusal) throw this.#refusal;
    try {
      const verified = this.#verified;
      if (!verified) throw new Error(`Refusing to serve resident requests: ${this.directory} was never verified`);
      const replaced = () => new Error(`Refusing to serve resident requests: ${this.directory} was replaced after ` +
        `its DACL was verified; restart the resident host to re-verify it`);
      let identity: string;
      try { identity = this.#identity(this.directory); } catch { throw replaced(); }
      if (identity !== verified.identity) throw replaced();
      if (this.#now() - verified.at < WINDOWS_REQUESTS_ACL_TTL_MS) return;
      await assertWindowsRequestsDirectoryPrivate(this.directory, this.#deps().readAcl);
      let after: string;
      try { after = this.#identity(this.directory); } catch { throw replaced(); }
      if (after !== verified.identity) throw replaced();
      this.#verified = { identity, at: this.#now() };
    } catch (error) {
      this.#refusal = error instanceof Error ? error : new Error(String(error));
      throw this.#refusal;
    }
  }
}
