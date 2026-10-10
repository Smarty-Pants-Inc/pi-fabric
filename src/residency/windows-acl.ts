import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

export const WINDOWS_SYSTEM_SID = "S-1-5-18";
export const WINDOWS_ADMINISTRATORS_SID = "S-1-5-32-544";
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
export interface WindowsDirectoryAcl { user: string; owner: string; aces: WindowsDirectoryAce[] }

export function windowsSystemExecutable(...segments: string[]): string {
  return path.win32.join(process.env.SystemRoot ?? "C:\\Windows", "System32", ...segments);
}

/** Fixed Windows PowerShell path; scripts receive paths only through the environment. */
export function windowsPowerShellPath(): string {
  return windowsSystemExecutable("WindowsPowerShell", "v1.0", "powershell.exe");
}

// No double quotes: Windows argv quoting of " is unreliable for powershell.exe -Command.
// SIDs come from GetAccessRules(..., SecurityIdentifier), so output is locale independent.
const aclReportScript = [
  "$ErrorActionPreference = 'Stop'",
  "$sid = [System.Security.Principal.SecurityIdentifier]",
  "$acl = Get-Acl -LiteralPath $env:FABRIC_ACL_PATH",
  "[Console]::Out.WriteLine('user ' + [System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value)",
  "[Console]::Out.WriteLine('owner ' + $acl.GetOwner($sid).Value)",
  "foreach ($r in $acl.GetAccessRules($true, $true, $sid)) { [Console]::Out.WriteLine('ace ' + $r.AccessControlType.ToString() + ' ' + $r.IdentityReference.Value + ' ' + [string][int64]$r.FileSystemRights.value__) }",
  "[Console]::Out.WriteLine('end')",
].join("; ");

/** Parse the Get-Acl report. Anything unexpected throws, so callers fail closed. */
export function parseWindowsAclReport(output: string): WindowsDirectoryAcl {
  const lines = output.split(/\r?\n/).map(line => line.trim()).filter(Boolean);
  if (lines.at(-1) !== "end") throw new Error("truncated DACL report");
  let user: string | undefined;
  let owner: string | undefined;
  const aces: WindowsDirectoryAce[] = [];
  for (const line of lines.slice(0, -1)) {
    const fields = line.split(" ");
    if (fields[0] === "user" && fields.length === 2 && user === undefined && sidPattern.test(fields[1]!)) user = fields[1];
    else if (fields[0] === "owner" && fields.length === 2 && owner === undefined && sidPattern.test(fields[1]!)) owner = fields[1];
    else if (fields[0] === "ace" && fields.length === 4 && (fields[1] === "Allow" || fields[1] === "Deny") &&
      sidPattern.test(fields[2]!) && /^-?\d+$/.test(fields[3]!)) {
      aces.push({ type: fields[1], sid: fields[2]!, rights: Number(fields[3]) });
    } else throw new Error(`unparseable DACL report line: ${line.slice(0, 120)}`);
  }
  if (!user || !owner) throw new Error("DACL report lacks the user or owner SID");
  return { user, owner, aces };
}

/** SIDs other than this user, SYSTEM and Administrators that own or may write the directory. */
export function windowsRequestsAclViolations(acl: WindowsDirectoryAcl): string[] {
  const trusted = new Set([acl.user, WINDOWS_SYSTEM_SID, WINDOWS_ADMINISTRATORS_SID]);
  const extra = new Set<string>();
  // The owner can always rewrite the DACL, whatever it currently says.
  if (!trusted.has(acl.owner)) extra.add(`${acl.owner} (owner)`);
  for (const ace of acl.aces) {
    // Deny entries only narrow access; any allowed write-class bit is a grant.
    if (ace.type === "Allow" && !trusted.has(ace.sid) && (ace.rights & WINDOWS_WRITE_CLASS_RIGHTS) !== 0) extra.add(ace.sid);
  }
  return [...extra];
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
}
/** Platform adapters, read per call; tests that inject win32 on another OS replace these explicitly. */
export const windowsRequestsAclAdapters: WindowsRequestsAclDeps = { readAcl: readWindowsDirectoryAcl, restrict: restrictWindowsDirectory };

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
