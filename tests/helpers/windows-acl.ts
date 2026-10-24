import { spawnSync } from "node:child_process";
import { windowsSecurityPowerShell } from "../../src/storage/windows-powershell.js";

// Foreign-owner fixture (smarty-dev#7858 / #4664), extracted from tests/verified-remove-windows.test.ts for reuse by
// the mesh state ACL suite (smarty-dev#8920): bounded Windows PowerShell 5.1 children, inputs by environment only.
// windowsSecurityPowerShell (production) pins 5.1 and its own module path: node spawned from pwsh inherits pwsh 7's
// PSModulePath, and 5.1 then fails to load Microsoft.PowerShell.Security (Get-Acl).
export const powershell = (script: string, env: Record<string, string> = {}) => {
  const command = windowsSecurityPowerShell(script, { ...process.env, ...env });
  return spawnSync(command.file, command.args, { encoding: "utf8", timeout: 90_000, windowsHide: true, env: command.env });
};
export const brief = (result: { status: number | null; error?: Error | undefined; stdout: unknown; stderr: unknown }): string =>
  `exit ${String(result.status)}${result.error ? ` ${result.error.message}` : ""}: ${`${String(result.stdout)} ${String(result.stderr)}`.replace(/\s+/g, " ").trim().slice(0, 300)}`;
/** This process's token SIDs, as the helper reads them: TokenUser and TokenOwner (WindowsIdentity.User / .Owner). */
export const tokenSids = (): { user: string; owner: string } => {
  const result = powershell("$i = [Security.Principal.WindowsIdentity]::GetCurrent(); \"$($i.User.Value) $($i.Owner.Value)\"");
  const match = /^(S-1-[\d-]+) (S-1-[\d-]+)$/.exec(String(result.stdout).trim());
  if (result.status !== 0 || !match) throw new Error(`token SID query failed (${brief(result)})`);
  return { user: match[1]!, owner: match[2]! };
};
/** The owner SID Get-Acl reads now (sid undefined when it cannot be read), with that child's brief() as trace. */
export const aclOwner = (file: string): { sid: string | undefined; trace: string } => {
  const result = powershell("(Get-Acl -LiteralPath $env:FABRIC_TEST_FILE).GetOwner([Security.Principal.SecurityIdentifier]).Value", { FABRIC_TEST_FILE: file });
  const sid = String(result.stdout).trim();
  return { sid: result.status === 0 && /^S-1-[\d-]+$/.test(sid) ? sid : undefined, trace: `Get-Acl ${brief(result)}` };
};
/** Sets only the owner section, with SeRestorePrivilege enabled in THIS child's own token (process-local, gone at its
 * exit; it is only enabled if already held: nothing global changes). Exit 5 when the privilege is not held. */
const PRIVILEGED_SET_OWNER = `$ErrorActionPreference = 'Stop'
Add-Type -TypeDefinition @'
using System; using System.Runtime.InteropServices;
public static class FabricTestRestorePrivilege {
  [StructLayout(LayoutKind.Sequential, Pack = 4)] struct Luid { public uint Low; public int High; }
  [StructLayout(LayoutKind.Sequential, Pack = 4)] struct Privileges { public int Count; public Luid Luid; public int Attributes; }
  [DllImport("advapi32.dll", SetLastError = true)] static extern bool OpenProcessToken(IntPtr process, uint access, out IntPtr token);
  [DllImport("advapi32.dll", SetLastError = true, CharSet = CharSet.Unicode)] static extern bool LookupPrivilegeValue(string system, string name, out Luid luid);
  [DllImport("advapi32.dll", SetLastError = true)] static extern bool AdjustTokenPrivileges(IntPtr token, bool disableAll, ref Privileges state, int length, IntPtr previous, IntPtr returned);
  [DllImport("kernel32.dll")] static extern IntPtr GetCurrentProcess();
  [DllImport("kernel32.dll")] static extern bool CloseHandle(IntPtr handle);
  public static int Enable() {
    IntPtr token;
    if (!OpenProcessToken(GetCurrentProcess(), 0x28, out token)) return Marshal.GetLastWin32Error();
    try {
      Privileges state = new Privileges(); state.Count = 1; state.Attributes = 2;
      if (!LookupPrivilegeValue(null, "SeRestorePrivilege", out state.Luid)) return Marshal.GetLastWin32Error();
      if (!AdjustTokenPrivileges(token, false, ref state, 0, IntPtr.Zero, IntPtr.Zero)) return Marshal.GetLastWin32Error();
      return Marshal.GetLastWin32Error();
    } finally { CloseHandle(token); }
  }
}
'@
$enabled = [FabricTestRestorePrivilege]::Enable()
if ($enabled -ne 0) { "SeRestorePrivilege not enabled (Win32 error $enabled)"; exit 5 }
$security = New-Object System.Security.AccessControl.FileSecurity
$security.SetOwner((New-Object System.Security.Principal.SecurityIdentifier $env:FABRIC_TEST_SID))
[System.IO.File]::SetAccessControl($env:FABRIC_TEST_FILE, $security)
'owner set'`;
/** Make `sid` the owner: icacls first, then the privileged child; each attempt confirmed by Get-Acl. */
export const setOwner = (file: string, sid: string): { changed: boolean; trace: string[] } => {
  const trace: string[] = [];
  const icacls = spawnSync("icacls", [file, "/setowner", `*${sid}`], { encoding: "utf8", timeout: 30_000, windowsHide: true });
  let now = aclOwner(file);
  trace.push(`icacls /setowner *${sid}: ${brief(icacls)}; Get-Acl owner ${String(now.sid)} (${now.trace})`);
  if (now.sid === sid) return { changed: true, trace };
  const privileged = powershell(PRIVILEGED_SET_OWNER, { FABRIC_TEST_FILE: file, FABRIC_TEST_SID: sid });
  now = aclOwner(file);
  trace.push(`SetAccessControl (SeRestorePrivilege) ${sid}: ${brief(privileged)}; Get-Acl owner ${String(now.sid)} (${now.trace})`);
  return { changed: now.sid === sid, trace };
};

// smarty-dev#8920 read-back helpers (new): the DACL as Get-Acl reads it (SIDs, numeric rights) and as icacls prints it.
export interface AclRule { sid: string; rights: number; type: string; inherited: boolean; inheritance: string; propagation: string }
export interface AclReadBack { owner: string; protected: boolean; sddl: string; rules: AclRule[] }
const READ_ACL = `$acl = Get-Acl -LiteralPath $env:FABRIC_TEST_FILE
$rules = @($acl.GetAccessRules($true, $true, [Security.Principal.SecurityIdentifier]) | ForEach-Object {
  [pscustomobject]@{ sid = $_.IdentityReference.Value; rights = $_.FileSystemRights.value__; type = [string]$_.AccessControlType;
    inherited = $_.IsInherited; inheritance = [string]$_.InheritanceFlags; propagation = [string]$_.PropagationFlags } })
ConvertTo-Json -Compress -Depth 4 -InputObject ([pscustomobject]@{ owner = $acl.GetOwner([Security.Principal.SecurityIdentifier]).Value;
  protected = $acl.AreAccessRulesProtected; sddl = $acl.Sddl; rules = $rules })`;
export const readAcl = (file: string): AclReadBack => {
  const result = powershell(READ_ACL, { FABRIC_TEST_FILE: file });
  if (result.status !== 0) throw new Error(`Get-Acl ${file} failed (${brief(result)})`);
  const parsed = JSON.parse(String(result.stdout).trim()) as AclReadBack & { rules: AclRule[] | AclRule | null };
  const rules = parsed.rules === null ? [] : Array.isArray(parsed.rules) ? parsed.rules : [parsed.rules];
  return { ...parsed, rules: rules.map(rule => ({ ...rule, rights: rule.rights >>> 0 })) };
};
/** `icacls <file>` output, failing loudly when icacls itself fails. */
export const icaclsShow = (file: string): string => {
  const result = spawnSync("icacls", [file], { encoding: "utf8", timeout: 30_000, windowsHide: true });
  if (result.status !== 0) throw new Error(`icacls ${file} failed (${brief(result)})`);
  return String(result.stdout);
};
/** A genuine grant through icacls (e.g. `*S-1-1-0:(OI)(CI)(M)`), failing loudly when it is not applied. */
export const icaclsGrant = (file: string, ace: string): void => {
  const result = spawnSync("icacls", [file, "/grant", ace], { encoding: "utf8", timeout: 30_000, windowsHide: true });
  if (result.status !== 0) throw new Error(`icacls ${file} /grant ${ace} failed (${brief(result)})`);
};
