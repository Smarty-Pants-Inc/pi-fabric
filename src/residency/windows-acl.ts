import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { promisify } from "node:util";
import { windowsSecurityPowerShell } from "../storage/windows-powershell.js";

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

// Constant scripts; paths travel only through environment variables.
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
 * Each path arrives in FABRIC_ACL_PATHS as one base64 token of its UTF-16 code
 * units, tokens newline-separated, so any name (CR, LF and unpaired surrogates
 * included) decodes exactly and can never split or forge a report line. Each file
 * yields `file <index>`, then `path <base64>` (the decoded path, re-encoded the
 * same way, so the parser binds the report to the path it asked about),
 * `attributes <FileAttributes>` (from the entry itself, so a reparse point reports
 * ReparsePoint) and the same report as one directory, or `error` alone when it
 * cannot be read. A file's lines are buffered and written only once complete, so
 * a failure never leaves a partial report.
 */
const aclBatchReportScript = [
  "$ErrorActionPreference = 'Stop'",
  "$sid = [System.Security.Principal.SecurityIdentifier]",
  "$i = 0",
  "foreach ($e in $env:FABRIC_ACL_PATHS.Split([char]10)) { try { " + [
    "$b = [System.Convert]::FromBase64String($e)",
    "$c = [char[]]::new([int][Math]::Floor($b.Length / 2))",
    "for ($k = 0; $k -lt $c.Length; $k++) { $c[$k] = [char]([int]$b[2 * $k] + 256 * [int]$b[2 * $k + 1]) }",
    "$p = -join $c",
    "$r = [byte[]]::new(2 * $p.Length)",
    "for ($k = 0; $k -lt $p.Length; $k++) { $v = [int]$p[$k]; $r[2 * $k] = [byte]($v -band 255); $r[2 * $k + 1] = [byte]($v -shr 8) }",
    "$o = [System.Collections.Generic.List[string]]::new()",
    "$o.Add('path ' + [System.Convert]::ToBase64String($r))",
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

/** One path as the batch script receives and echoes it: base64 of its UTF-16LE code units (exact for any name). */
export function encodeWindowsAclPath(file: string): string {
  return Buffer.from(file, "utf16le").toString("base64");
}

/** Inverse of encodeWindowsAclPath; undefined for anything that is not a canonical token. */
export function decodeWindowsAclPath(token: string): string | undefined {
  if (!/^[A-Za-z0-9+/]*={0,2}$/.test(token)) return undefined;
  const bytes = Buffer.from(token, "base64");
  if (bytes.length % 2 !== 0 || bytes.toString("base64") !== token) return undefined;
  return bytes.toString("utf16le");
}

/**
 * Parse the batch report for `files` (in request order). A malformed batch throws
 * (nothing is judged); a malformed or unreadable single file, or a report that
 * names another path, becomes an `error` entry, which the callers treat as a
 * failed batch (nothing is consumed or trusted).
 */
export function parseWindowsFileAclBatch(output: string, files: readonly string[]): WindowsRequestFileReport[] {
  const count = files.length;
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
  return sections.map((section, index): WindowsRequestFileReport => {
    if (section.length === 1 && section[0] === "error") return { error: "Get-Acl failed" };
    const named = /^path (\S*)$/.exec(section[0] ?? "");
    if (!named) return { error: "missing file path" };
    if (decodeWindowsAclPath(named[1]!) !== files[index]) return { error: "the report names another path" };
    const match = /^attributes (\d+)$/.exec(section[1] ?? "");
    if (!match) return { error: "missing file attributes" };
    try {
      return { attributes: Number(match[1]), acl: parseWindowsAclReport(section.slice(2).join("\n")) };
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

/** Runs the constant batch script once with fixed argv; `encodedPaths` is the FABRIC_ACL_PATHS value. */
export type WindowsFileAclBatchRunner = (encodedPaths: string) => Promise<string>;
const runWindowsFileAclBatch: WindowsFileAclBatchRunner = async encodedPaths => {
  const command = windowsSecurityPowerShell(aclBatchReportScript);
  const { stdout } = await execFileAsync(command.file, command.args, {
    encoding: "utf8", windowsHide: true, timeout: 30_000, env: { ...command.env, FABRIC_ACL_PATHS: encodedPaths },
  });
  return stdout;
};

/** Upper bound for one FABRIC_ACL_PATHS value, well below the 32767-character Windows variable limit. */
export const WINDOWS_ACL_BATCH_ENV_BUDGET = 24_000;

/**
 * One Get-Acl process per batch of pending files: the constant batch script, fixed argv, every path
 * base64-encoded in the environment (any name, CR/LF included, is carried exactly). Batches are split
 * to stay within the environment budget; a single path too long for it is an `error` entry.
 */
export async function readWindowsFileAcls(files: string[], run: WindowsFileAclBatchRunner = runWindowsFileAclBatch): Promise<WindowsRequestFileReport[]> {
  const reports: WindowsRequestFileReport[] = new Array(files.length);
  const tokens = files.map(encodeWindowsAclPath);
  let chunk: number[] = [];
  let size = 0;
  const flush = async () => {
    if (chunk.length === 0) return;
    const indexes = chunk;
    chunk = []; size = 0;
    const output = await run(indexes.map(index => tokens[index]!).join("\n"));
    parseWindowsFileAclBatch(output, indexes.map(index => files[index]!)).forEach((report, j) => { reports[indexes[j]!] = report; });
  };
  for (const [index, token] of tokens.entries()) {
    if (token.length > WINDOWS_ACL_BATCH_ENV_BUDGET) { reports[index] = { error: "path too long for one ACL report" }; continue; }
    if (size + token.length + 1 > WINDOWS_ACL_BATCH_ENV_BUDGET) await flush();
    chunk.push(index);
    size += token.length + 1;
  }
  await flush();
  return reports;
}

export async function readWindowsDirectoryAcl(directory: string): Promise<WindowsDirectoryAcl> {
  if (/[\r\n]/.test(directory)) throw new Error("path contains a line break");
  const command = windowsSecurityPowerShell(aclReportScript);
  const { stdout } = await execFileAsync(command.file, command.args, {
    encoding: "utf8", windowsHide: true, timeout: 30_000, env: { ...command.env, FABRIC_ACL_PATH: directory },
  });
  return parseWindowsAclReport(stdout);
}

const restrictDirectoryScript = [
  "$sid = [System.Security.Principal.SecurityIdentifier]",
  "$user = [System.Security.Principal.WindowsIdentity]::GetCurrent().User",
  "$owner = (Get-Acl -LiteralPath $env:FABRIC_ACL_PATH).GetOwner($sid)",
  "if ($user.Value -ne $env:FABRIC_ACL_USER_SID -or $owner.Value -ne $user.Value) { throw 'Refusing to secure a directory not owned by the current user' }",
  "$acl = [System.Security.AccessControl.DirectorySecurity]::new()",
  "$acl.SetAccessRuleProtection($true, $false)",
  `foreach ($principal in @($env:FABRIC_ACL_USER_SID, '${WINDOWS_SYSTEM_SID}', '${WINDOWS_ADMINISTRATORS_SID}')) { ` +
    "$rule = [System.Security.AccessControl.FileSystemAccessRule]::new([System.Security.Principal.SecurityIdentifier]::new($principal), 'FullControl', 'ContainerInherit, ObjectInherit', 'None', 'Allow'); " +
    "$acl.AddAccessRule($rule) }",
  "Set-Acl -LiteralPath $env:FABRIC_ACL_PATH -AclObject $acl",
].join("; ");

/** Replace the entire DACL, not just inherited ACEs, with three protected private grants. */
export async function restrictWindowsDirectory(directory: string, userSid: string): Promise<void> {
  if (!sidPattern.test(userSid)) throw new Error(`malformed user SID: ${userSid}`);
  const command = windowsSecurityPowerShell(restrictDirectoryScript);
  await execFileAsync(command.file, command.args, {
    encoding: "utf8", windowsHide: true, timeout: 30_000,
    env: { ...command.env, FABRIC_ACL_PATH: directory, FABRIC_ACL_USER_SID: userSid },
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

/** Volume, file id and creation time of an entry (not followed); the bigint `ino` is the NTFS file index on Windows. */
const entryIdentity = (stat: fs.BigIntStats): string => `${stat.dev}:${stat.ino}:${stat.birthtimeNs}`;

/** The identity of each resident metadata file, pinned while its ACL was verified (win32). */
export type ResidentMetadataPins = ReadonlyMap<string, string>;

/**
 * Windows: fail closed unless the resident directory and each resident metadata file (config.json, owner.json)
 * are private. Whoever can write these files, or replace them through the directory, picks the root, mesh, actor
 * registries, process and paths an operator tool acts on, so an owner SID check alone is not enough. Each parent
 * directory is judged by the requests-directory policy (windowsRequestsAclViolations) and each file by the
 * request-file batch report and policy (windowsRequestFileViolations): a foreign owner, a foreign write-class
 * ACE, a NULL or missing DACL, a reparse point, or an unreadable report refuses. Every entry's identity is pinned
 * before the ACL reads and must be unchanged after them; the pins are returned so the caller reads exactly the
 * verified files (readResidentMetadataFile). POSIX: no-op returning undefined (callers check the uid).
 */
export async function assertWindowsResidentMetadataPrivate(files: string[], refusing: string,
  platform: NodeJS.Platform = process.platform,
  readFileAcls: (files: string[]) => Promise<WindowsRequestFileReport[]> = windowsRequestsAclAdapters.readFileAcls,
  readAcl: (directory: string) => Promise<WindowsDirectoryAcl> = windowsRequestsAclAdapters.readAcl): Promise<ResidentMetadataPins | undefined> {
  if (platform !== "win32") return undefined;
  const cannot = (reason: string) =>
    new Error(`Cannot verify that the resident metadata is private to this Windows user (${reason}); refusing ${refusing}`);
  const directories = [...new Set(files.map(file => path.dirname(file)))];
  const identify = () => new Map([...directories, ...files].map(entry => {
    const stat = fs.lstatSync(entry, { bigint: true });
    return [entry, { identity: entryIdentity(stat), regularFile: stat.isFile(), directory: stat.isDirectory(), symbolicLink: stat.isSymbolicLink() }];
  }));
  const pinned = identify();
  for (const directory of directories) {
    const entry = pinned.get(directory)!;
    if (entry.symbolicLink || !entry.directory) throw new Error(`Resident directory is not a real directory: ${directory}; refusing ${refusing}`);
    let acl: WindowsDirectoryAcl;
    try {
      acl = await readAcl(directory);
    } catch (error) {
      throw cannot(`${directory}: ${error instanceof Error ? error.message : String(error)}`);
    }
    const foreign = windowsRequestsAclViolations(acl);
    if (foreign.length > 0) {
      throw new Error(`Resident directory is not private to this OS user: ${directory} (owned or writable by other accounts (${foreign.join(", ")})); refusing ${refusing}`);
    }
  }
  const entries = files.map(file => ({ file, regularFile: pinned.get(file)!.regularFile, symbolicLink: pinned.get(file)!.symbolicLink }));
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
  let after: ReturnType<typeof identify>;
  try { after = identify(); } catch { after = new Map(); }
  for (const [entry, { identity }] of pinned) {
    if (after.get(entry)?.identity !== identity) throw new Error(`Resident metadata was replaced while its ACL was verified: ${entry}; refusing ${refusing}`);
  }
  return new Map(files.map(file => [file, pinned.get(file)!.identity]));
}

/**
 * Read one resident metadata file through a single descriptor (links not followed where the OS allows). With
 * pins (win32, from assertWindowsResidentMetadataPrivate) the descriptor must be the verified file: a file
 * replaced after its ACL was verified refuses instead of being trusted.
 */
export function readResidentMetadataFile(file: string, pins: ResidentMetadataPins | undefined, refusing: string): string {
  const fd = fs.openSync(file, fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW ?? 0));
  try {
    if (pins && (pins.get(file) === undefined || entryIdentity(fs.fstatSync(fd, { bigint: true })) !== pins.get(file))) {
      throw new Error(`Resident metadata was replaced after its ACL was verified: ${file}; refusing ${refusing}`);
    }
    return fs.readFileSync(fd, "utf8");
  } finally { fs.closeSync(fd); }
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

/** Tighten only current-user-owned directories, then re-read and apply the unchanged writer policy. */
async function preparePrivateDirectory(directory: string, deps: WindowsRequestsAclDeps, role: string): Promise<void> {
  const identity = windowsDirectoryIdentity(directory);
  let acl: WindowsDirectoryAcl;
  try {
    acl = await deps.readAcl(directory);
  } catch (error) {
    throw new Error(`Refusing to serve resident requests: cannot verify the DACL of ${role} ${directory} ` +
      `(${error instanceof Error ? error.message : String(error)})`);
  }
  // Ownership permits changing the DACL; never take over a directory owned by any other account.
  if (acl.owner !== acl.user) {
    throw new Error(`Refusing to serve resident requests: ${role} ${directory} is owned by another account (${acl.owner} (owner)); ` +
      `only ${acl.user} may own it before its DACL is secured.`);
  }
  try {
    await deps.restrict(directory, acl.user);
    acl = await deps.readAcl(directory);
  } catch (error) {
    throw new Error(`Refusing to serve resident requests: cannot secure or verify the DACL of ${role} ${directory} ` +
      `(${error instanceof Error ? error.message : String(error)})`);
  }
  if (windowsDirectoryIdentity(directory) !== identity) {
    throw new Error(`Refusing to serve resident requests: ${role} ${directory} was replaced while its DACL was secured`);
  }
  const extra = windowsRequestsAclViolations(acl);
  if (extra.length === 0 && acl.owner === acl.user) return;
  if (acl.owner !== acl.user && !extra.includes(`${acl.owner} (owner)`)) extra.push(`${acl.owner} (owner)`);
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
 * Current-user-owned residency directories and their parent are secured and
 * verified first: normal Windows temp/profile parents otherwise supply inherited
 * writer ACEs. Foreign ownership is always refused without changing the DACL.
 * A new directory is created under a private staging name inside the secured
 * residency directory, secured again, verified private and empty, and only then
 * renamed into place. Existing directories are also secured before verification;
 * files planted before hardening still require the guard's per-file ACL screen.
 * Also used for `processing`. Returns the verified directory identity on Windows.
 */
export async function prepareResidentRequestsDirectory(directory: string, platform: NodeJS.Platform = process.platform,
  deps: WindowsRequestsAclDeps = windowsRequestsAclAdapters): Promise<string | undefined> {
  if (platform !== "win32") { fs.mkdirSync(directory, { recursive: true, mode: 0o700 }); return undefined; }
  const residency = path.dirname(directory);
  fs.mkdirSync(residency, { recursive: true, mode: 0o700 });
  await preparePrivateDirectory(path.dirname(residency), deps, "the residency parent");
  await preparePrivateDirectory(residency, deps, "the residency directory");
  if (!fs.existsSync(directory)) {
    const staging = `${directory}.create-${randomUUID()}`;
    fs.mkdirSync(staging, { mode: 0o700 });
    try {
      await preparePrivateDirectory(staging, deps, "the requests staging directory");
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
  await preparePrivateDirectory(directory, deps, "the requests directory");
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
  /** The failure and candidate set last isolated, so a repeating systemic failure is not re-isolated every poll. */
  #isolated: string | undefined;
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
   * is returned this time, and each distinct failure alarms once. The abort is
   * then isolated (see #isolate): an entry whose own report is unreadable or
   * malformed is quarantined with an alarm naming it, so one planted name cannot
   * stall the directory; its siblings are judged again on the next poll.
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
      await this.#isolate(candidates.map(candidate => candidate.file), message);
      return [];
    }
    this.#screenFailure = undefined;
    this.#isolated = undefined;
    const accepted: string[] = [];
    candidates.forEach((candidate, index) => {
      const violations = windowsRequestFileViolations({ ...candidate, report: reports[index]! });
      if (violations.length === 0) { accepted.push(candidate.entry); return; }
      this.#quarantine(candidate.file, violations.join("; "));
    });
    return accepted;
  }

  /** Rename a refused file out of the `.json` namespace with an alarm; a failed rename skips it from now on. */
  #quarantine(file: string, reason: string): void {
    const quarantine = `${file}.quarantine-${randomUUID()}`;
    try {
      fs.renameSync(file, quarantine);
      console.error(`[pi-fabric] ALARM: quarantined resident request ${file} as ${path.basename(quarantine)} ` +
        `without consuming it: ${reason}`);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return; // gone meanwhile: nothing to consume
      this.#unquarantined.add(file);
      console.error(`[pi-fabric] ALARM: refusing resident request ${file} (${reason}); ` +
        `quarantine failed (${error instanceof Error ? error.message : String(error)}), so it is skipped`);
    }
  }

  /**
   * After an aborted batch, find the entries that caused it: the same report runs once for the requests
   * directory itself (a control path that must always be readable) and then once per file. If the control
   * fails too, the failure is systemic (PowerShell, permissions, the parser), not an entry: nothing is
   * quarantined and the next poll retries. Otherwise each file whose own report fails, is unreadable or is
   * malformed is quarantined with an alarm naming it; files that read cleanly alone are left for the next poll,
   * which judges them by the usual policy. The same failure on the same entries is isolated only once.
   */
  async #isolate(files: string[], failure: string): Promise<void> {
    const key = JSON.stringify([failure, files]);
    if (key === this.#isolated) return;
    this.#isolated = key;
    const readFileAcls = this.#deps().readFileAcls ?? windowsRequestsAclAdapters.readFileAcls;
    const single = async (file: string): Promise<string | undefined> => {
      try {
        const reports = await readFileAcls([file]);
        if (reports.length !== 1) return `expected 1 file report, got ${reports.length}`;
        return "error" in reports[0]! ? reports[0].error : undefined;
      } catch (error) {
        return error instanceof Error ? error.message : String(error);
      }
    };
    const control = await single(this.directory);
    if (control !== undefined) {
      console.error(`[pi-fabric] ALARM: cannot isolate the failed resident request batch in ${this.directory}: ` +
        `the report fails for the requests directory itself (${control}); nothing quarantined, retrying`);
      return;
    }
    for (const file of files) {
      const reason = await single(file);
      if (reason !== undefined) this.#quarantine(file, `unverifiable DACL (${reason}); it aborted the request batch`);
    }
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
