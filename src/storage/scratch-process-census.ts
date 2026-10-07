import childProcess from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { windowsSecurityPowerShell } from "./windows-powershell.js";

export const UNSCOPED_SCRATCH_RETENTION_MS = 24 * 60 * 60 * 1000;
export interface ScratchHostEpoch { platform: string; hostname: string; boot: string }
const queryOptions = (): childProcess.ExecFileSyncOptionsWithStringEncoding => ({ encoding: "utf8" as const, timeout: process.platform === "win32" ? 15_000 : 1000, maxBuffer: 1024 * 1024,
  windowsHide: true, env: { ...process.env, LC_ALL: "C", TZ: "UTC", NODE_OPTIONS: "", BUN_OPTIONS: "", LD_PRELOAD: "", LD_AUDIT: "", LD_LIBRARY_PATH: "", LD_ORIGIN_PATH: "", GCONV_PATH: "" } });

/** The fallback inventory is host-local. Shared/unknown filesystem namespaces
 * cannot use it: another host may still have ordinary writers with this path. */
export const localScratchVolume = (directory: string): boolean => {
  try {
    if (process.platform === "win32") return true; // caller already proves a direct local volume + private DACL
    if (process.platform === "linux") {
      // ext*, tmpfs, ramfs, xfs, btrfs, zfs and local overlayfs. FUSE/NFS/SMB
      // and unknown filesystems retain custody, rather than assuming locality.
      return [0xef53, 0x01021994, 0x858458f6, 0x58465342, 0x9123683e, 0x2fc12fc1, 0x794c7630].includes(fs.statfsSync(directory).type);
    }
    if (process.platform === "darwin") {
      // BSD stat's %T describes a file, NOT its filesystem. Qualify the
      // trusted root filesystem class from the native mount table, then match
      // the target's kernel filesystem type. APFS Data-volume firmlinks need
      // not share the root's device number. Other classes stay fenced.
      // Do not stat unrelated mountpoints: autofs/NFS inspection can block.
      if (fs.statfsSync(directory).type !== fs.statfsSync("/").type) return false;
      const output = childProcess.execFileSync("/sbin/mount", [], queryOptions());
      const rows = output.trim().split("\n").map(line => line.match(/^.+ on (\/.*) \(([^)]+)\)$/));
      if (rows.some(row => !row)) return false;
      const roots = rows.filter(row => row![1] === "/");
      if (roots.length !== 1) return false;
      const flags = roots[0]![2]!.split(", ");
      return ["apfs", "hfs", "ufs"].includes(flags[0]!) && flags.includes("local");
    }
    return false;
  } catch { return false; }
};

/** An actual boot change on the same host terminates all prior ordinary holders.
 * Never substitute PID absence, a missing cgroup, or an unavailable boot query. */
export const scratchHostEpoch = (): ScratchHostEpoch | undefined => {
  try {
    let boot: string;
    if (process.platform === "linux") boot = fs.readFileSync("/proc/sys/kernel/random/boot_id", "utf8").trim();
    else if (process.platform === "darwin") boot = childProcess.execFileSync("/usr/sbin/sysctl", ["-n", "kern.bootsessionuuid"], queryOptions()).trim();
    else if (process.platform === "win32") {
      const options = queryOptions();
      const command = windowsSecurityPowerShell(WINDOWS_BOOT_IDENTITY, options.env);
      boot = childProcess.execFileSync(command.file, command.args, { ...options, env: command.env }).trim();
    }
    else return;
    if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(boot)) return;
    return { platform: process.platform, hostname: os.hostname(), boot: boot.toLowerCase() };
  } catch { return; }
};

// A calendar boot time can change with clock correction and is NOT a reboot
// receipt. Use the kernel's boot-session GUID, just as Linux uses boot_id.
const WINDOWS_BOOT_IDENTITY = String.raw`
Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;
public static class FabricBootIdentity {
  [StructLayout(LayoutKind.Sequential)]
  struct BootEnvironment { public Guid BootIdentifier; public int FirmwareType; public ulong BootFlags; }
  [DllImport("ntdll.dll")]
  static extern int NtQuerySystemInformation(int infoClass, out BootEnvironment info, int length, out int returned);
  public static string Read() {
    BootEnvironment info; int returned;
    int status = NtQuerySystemInformation(90, out info, Marshal.SizeOf(typeof(BootEnvironment)), out returned);
    if (status != 0 || returned < 16 || info.BootIdentifier == Guid.Empty) throw new InvalidOperationException("Unproved boot identity");
    return info.BootIdentifier.ToString();
  }
}
'@
[FabricBootIdentity]::Read()
`;

interface ProcessBirth { pid: number; ppid: number; birth: number; zombie: boolean; query?: boolean }
const WINDOWS_POPULATION = String.raw`
$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
try {
  $caller = [int]$env:PI_FABRIC_CENSUS_PID
  $session = (Get-Process -Id $caller).SessionId
  $query = Get-CimInstance Win32_Process -Filter "ProcessId=$PID"
  if ($query.ParentProcessId -ne $caller) { throw 'Unknown query parent' }
  $rows = @(Get-CimInstance Win32_Process | Where-Object { $_.SessionId -eq $session -and $_.ProcessId -notin @(0,4) } | ForEach-Object {
    if ($null -eq $_.CreationDate) { throw 'Unknown process birth' }
    @{ pid = [int]$_.ProcessId; ppid = [int]$_.ParentProcessId;
       birth = [DateTimeOffset]::new($_.CreationDate.ToUniversalTime()).ToUnixTimeMilliseconds();
       zombie = $false; query = ($_.ProcessId -eq $PID) }
  })
  ConvertTo-Json -InputObject $rows -Compress
} catch { [Console]::Error.WriteLine($_.Exception.Message); exit 1 }
`;

/** Complete conservative potential-holder population. Every ordinary descendant
 * is born after allocation, even if it closes all files, redirects stdio, setsid,
 * or changes TMPDIR later. Count *all* new same-user POSIX / same-login-session
 * Windows processes, not merely the worker PID, open files, or current env.
 * Unrelated newer processes can postpone collection; uncertainty never deletes.
 * Same-uid intentional IPC/path sharing, privilege/session migration and host
 * administrator clock/boot tampering are outside ordinary launch custody. */
export const noPotentialScratchHolders = (
  allocatedAt: number, directory: string, expired: () => boolean, allocatedUptime?: number,
): boolean => {
  if (expired() || !Number.isFinite(allocatedAt) || allocatedAt < 0) return false;
  if (allocatedUptime !== undefined) {
    const elapsed = (os.uptime() - allocatedUptime) * 1000;
    // Uptime and wall time must agree before using calendar process births.
    // A clock jump is uncertainty, NOT proof that a later writer is older.
    // Date construction reads the real clock independently of retention's
    // injectable Date.now test clock. os.uptime may round to whole seconds.
    if (!Number.isFinite(allocatedUptime) || elapsed < 0 || Math.abs(new Date().getTime() - allocatedAt - elapsed) > 2000) return false;
  }
  // A collector launched *inside* this run is itself a holder, not an exception.
  for (const key of ["TMPDIR", "TMP", "TEMP"]) {
    const value = process.env[key];
    if (value && (path.resolve(value) === directory || path.resolve(value).startsWith(directory + path.sep))) return false;
  }
  const census = (): ProcessBirth[] | undefined => {
    try {
      if (process.platform === "win32") {
        const options = queryOptions(); options.env = { ...options.env, PI_FABRIC_CENSUS_PID: String(process.pid) };
        const command = windowsSecurityPowerShell(WINDOWS_POPULATION, options.env);
        const rows: unknown = JSON.parse(childProcess.execFileSync(command.file, command.args, { ...options, env: command.env }));
        if (!Array.isArray(rows) || rows.length > 16384 || rows.some(row => !row ||
          !Number.isSafeInteger(row.pid) || row.pid <= 0 || !Number.isSafeInteger(row.ppid) || row.ppid < 0 ||
          !Number.isFinite(row.birth) || row.birth < 0 || row.zombie !== false || typeof row.query !== "boolean" ||
          (row.query && row.ppid !== process.pid))) return;
        return rows;
      }
      if (process.platform !== "linux" && process.platform !== "darwin") return;
      const output = childProcess.execFileSync("/bin/ps", ["-axo", "pid=,ppid=,uid=,stat=,lstart=,comm="], queryOptions());
      const lines = output.trim().split("\n");
      if (!output.trim() || lines.length > 16384) return;
      const rows: ProcessBirth[] = [];
      for (const line of lines) {
        const match = line.trim().match(/^(\d+)\s+(\d+)\s+(\d+)\s+(\S+)\s+(\w{3}\s+\w{3}\s+\d+\s+\d{2}:\d{2}:\d{2}\s+\d{4})\s+(.+)$/);
        if (!match) return;
        if (Number(match[3]) !== process.getuid!()) continue;
        const birth = Date.parse(match[5]! + " UTC"), pid = Number(match[1]), ppid = Number(match[2]);
        if (!Number.isFinite(birth) || !Number.isSafeInteger(pid) || pid <= 0 || !Number.isSafeInteger(ppid)) return;
        rows.push({ pid, ppid, birth, zombie: match[4]!.startsWith("Z"),
          query: ppid === process.pid && /^(?:\/usr)?\/bin\/ps$|^ps$/.test(match[6]!) });
      }
      return rows;
    } catch { return; }
  };
  // Bound both observations. No post-allocation progenitor may remain at either
  // census; launch and disposal also serialize on the pinned custody lock.
  for (let pass = 0; pass < 2; pass++) {
    if (expired()) return false;
    const rows = census();
    if (!rows?.some(row => row.pid === process.pid)) return false;
    if (rows.some(row => row.pid !== process.pid && !row.query && !row.zombie && row.birth + 3000 >= allocatedAt)) return false;
    if (expired()) return false;
  }
  return true;
};
