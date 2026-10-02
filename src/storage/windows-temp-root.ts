import childProcess from "node:child_process";
import path from "node:path";
import { windowsSecurityPowerShell } from "./windows-powershell.js";

// Use SIDs, not localized names or the caller's group membership. These principals
// can already administer the machine. Services and ordinary user groups are not trusted.
const SYSTEM_SIDS = new Set([
  "S-1-5-18", // LocalSystem
  "S-1-5-32-544", // Built-in Administrators
  "S-1-5-80-956008885-3418522649-1831038044-1853292631-2271478464", // TrustedInstaller
]);
// Only known read/traverse/synchronize rights are harmless for an untrusted SID.
// This also rejects generic write/all, DELETE_CHILD, DELETE, WRITE_DAC, WRITE_OWNER,
// MAXIMUM_ALLOWED and unknown rights. Deny ACEs never cancel an unsafe grant here.
const READ_ONLY_RIGHTS = 0xa01200a9;
const SID = /^S-1-\d+(?:-\d+)+$/;
const record = (value: unknown): value is Record<string, unknown> => typeof value === "object" && value !== null && !Array.isArray(value);
const uint32 = (value: unknown): value is number => typeof value === "number" && Number.isInteger(value) && value >= 0 && value <= 0xffffffff;

// Fixed script: the directory names travel as JSON in the child environment, never
// as PowerShell source. Raw descriptors preserve null DACLs and unsupported ACEs
// that GetAccessRules() or localized icacls text could hide. No profiles or writes.
const INSPECT_ACLS = String.raw`
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
try {
  $paths = ConvertFrom-Json -InputObject $env:PI_FABRIC_ACL_CHAIN
  # A mapped network drive or SUBST alias can hide the physical ancestor chain.
  # Query the DOS device before any path access; only direct local volumes qualify.
  Add-Type -TypeDefinition @'
using System;
using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Text;
public static class FabricTempRootDevice {
  [DllImport("kernel32.dll", CharSet=CharSet.Unicode, SetLastError=true)]
  static extern uint QueryDosDevice(string name, StringBuilder target, int capacity);
  public static string Resolve(string drive) {
    var target = new StringBuilder(4096);
    if (QueryDosDevice(drive, target, target.Capacity) == 0) throw new Win32Exception();
    return target.ToString();
  }
}
'@
  $device = [FabricTempRootDevice]::Resolve([System.IO.Path]::GetPathRoot($paths[0]).Substring(0, 2))
  if ($device -notmatch '^\\Device\\HarddiskVolume[0-9]+$') { throw 'Not a direct local volume' }
  $directories = @(foreach ($directory in $paths) {
    $item = Get-Item -Force -LiteralPath $directory
    $acl = Get-Acl -LiteralPath $directory
    $raw = [System.Security.AccessControl.RawSecurityDescriptor]::new($acl.GetSecurityDescriptorBinaryForm(), 0)
    $dacl = $null
    if ($null -ne $raw.DiscretionaryAcl) {
      $dacl = @(foreach ($ace in $raw.DiscretionaryAcl) {
        $sid = $null
        $mask = $null
        if ($ace -is [System.Security.AccessControl.KnownAce]) {
          $sid = $ace.SecurityIdentifier.Value
          $mask = [uint32]([long]$ace.AccessMask -band 4294967295)
        }
        @{ type = [int]$ace.AceType; flags = [int]$ace.AceFlags; sid = $sid; mask = $mask }
      })
    }
    @{ path = $directory; attributes = [int]$item.Attributes; owner = $raw.Owner.Value; dacl = $dacl }
  })
  @{ userSid = [System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value; device = $device; directories = $directories } | ConvertTo-Json -Compress -Depth 8
} catch {
  [Console]::Error.WriteLine($_.Exception.Message)
  exit 1
}
`;

/** Existing, local Windows directories only; no repair or writes on uncertain ACLs. */
export const windowsDataRoot = (root: string): string => {
  const fail = (directory: string, reason: string): never => {
    throw new Error(`PI_FABRIC_TMPDIR is unsafe: ${directory} ${reason}`);
  };
  // Drive-relative, UNC, device and Win32-normalized names need different namespace
  // proofs. Do not accept those aliases, alternate data streams or reserved devices.
  if (!/^[a-z]:[\\/]/i.test(root)) fail(root, "must be an absolute local drive path");
  const components = root.slice(3).split(/[\\/]/).filter(Boolean);
  if (components.some(component => /[<>:"|?*\x00-\x1f]|[. ]$/.test(component) || /^(?:con|prn|aux|nul|com[1-9]|lpt[1-9])(?:\.|$)/i.test(component))) {
    fail(root, "has an ambiguous Windows path component");
  }
  const directory = path.win32.resolve(root);
  const chain: string[] = [];
  for (let current = directory; ; current = path.win32.dirname(current)) {
    chain.unshift(current);
    if (path.win32.dirname(current) === current) break;
  }
  let snapshot: unknown;
  try {
    const command = windowsSecurityPowerShell(INSPECT_ACLS, { ...process.env, PI_FABRIC_ACL_CHAIN: JSON.stringify(chain) });
    const output = childProcess.execFileSync(command.file, command.args, {
      env: command.env,
      encoding: "utf8", windowsHide: true, timeout: 15_000, maxBuffer: 1024 * 1024,
      stdio: ["ignore", "pipe", "pipe"],
    });
    snapshot = JSON.parse(output);
  } catch {
    return fail(directory, "could not prove native Windows ACL safety (directories must already exist)");
  }
  if (!record(snapshot) || typeof snapshot.userSid !== "string" || !SID.test(snapshot.userSid) || !Array.isArray(snapshot.directories) || snapshot.directories.length !== chain.length) {
    return fail(directory, "has an invalid native Windows ACL snapshot");
  }
  if (typeof snapshot.device !== "string" || !/^\\Device\\HarddiskVolume[0-9]+$/i.test(snapshot.device)) return fail(directory, "is not a proven direct local volume");
  const trusted = (sid: string) => sid === snapshot.userSid || SYSTEM_SIDS.has(sid);
  for (const [index, current] of chain.entries()) {
    const entry: unknown = snapshot.directories[index];
    if (!record(entry) || entry.path !== current || !uint32(entry.attributes)) fail(current, "has an invalid native Windows directory snapshot");
    const data = entry as Record<string, unknown>;
    if (((data.attributes as number) & 0x10) === 0 || ((data.attributes as number) & 0x400) !== 0) fail(current, "is not a real directory (reparse points are forbidden)");
    if (typeof data.owner !== "string" || !SID.test(data.owner)) fail(current, "has an unproven owner");
    if (!trusted(data.owner as string)) fail(current, "is owned by another user");
    if (!Array.isArray(data.dacl)) fail(current, "has a null or unproven DACL");
    for (const ace of data.dacl as unknown[]) {
      if (!record(ace) || (ace.type !== 0 && ace.type !== 1) || !uint32(ace.flags) || (ace.flags & ~0x1f) !== 0 || typeof ace.sid !== "string" || !SID.test(ace.sid) || !uint32(ace.mask)) {
        fail(current, "has an unsupported or unproven ACE");
      }
      const rule = ace as { type: number; sid: string; mask: number };
      // Include inherited/inherit-only grants: later scratch children must be private
      // too. Rejecting a deny+allow pair is deliberate conservative fail-closed policy.
      if (rule.type === 0 && !trusted(rule.sid) && (rule.mask & ~READ_ONLY_RIGHTS) !== 0) fail(current, "is writable or replaceable by an untrusted principal");
    }
  }
  return directory;
};
