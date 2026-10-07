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

/**
 * Create the residency `requests` directory and, on Windows, prove that no other
 * account can plant a request in it before the host consumes any request file.
 * A new Windows directory is restricted under a private staging name and only
 * then renamed into place, so it is never reachable with inherited write ACEs.
 */
export async function prepareResidentRequestsDirectory(directory: string, platform: NodeJS.Platform = process.platform,
  deps: WindowsRequestsAclDeps = windowsRequestsAclAdapters): Promise<void> {
  if (platform !== "win32") { fs.mkdirSync(directory, { recursive: true, mode: 0o700 }); return; }
  if (!fs.existsSync(directory)) {
    const staging = `${directory}.create-${randomUUID()}`;
    fs.mkdirSync(staging, { mode: 0o700 });
    try {
      const { user } = await deps.readAcl(staging);
      await deps.restrict(staging, user);
      fs.renameSync(staging, directory);
    } catch (error) {
      fs.rmSync(staging, { recursive: true, force: true });
      // A concurrent creator won the name; the check below decides whether it is private.
      if (!fs.existsSync(directory)) throw error;
    }
  }
  await assertWindowsRequestsDirectoryPrivate(directory, deps.readAcl);
}
