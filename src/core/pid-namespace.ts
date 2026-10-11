// The PID namespace of this process, and the reclaim verdict that uses it (smarty-dev#9569). A lazy entry point: the
// mesh and custody locks load it only when they write a protocol-3 record or check one, so the startup graph is
// unchanged.
//
// A lock holder's pid means something only inside its own PID namespace: a container or VM sharing a mesh root (Docker
// Desktop, WSL2, another host on a shared folder) sees ESRCH, or an unrelated process, for a LIVE foreign holder. Lock
// records of protocol 3 carry the holder's namespace, and a reclaimer uses pid evidence only for its own namespace.
//
// The id is `<os>:<machine>:<boot>:<pid namespace>`. Only an EQUAL id lets a reclaimer use pid evidence. Another boot of
// the same machine id is NOT inferred dead (pi-fabric#923: two running VM clones share a machine id), and a Windows id
// never qualifies (it cannot tell silos apart): both are unproven, recovered only by the audited operator override.
//   linux   linux:<machine-id|->:<boot_id>:<pid-ns inode>
//   darwin  darwin:<IOPlatformUUID|->:<kern.bootsessionuuid>:host
//   win32   win32:<MachineGuid>:<boot ticks>:host   (written for diagnosis; never proves a holder dead)
import fs from "node:fs";
import path from "node:path";
import { runIncarnationCommand, type IncarnationCommandRunner } from "./atomic-write.js";

const UUID = "[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}";
const NAMESPACE_FORMS: Record<string, RegExp> = {
  linux: new RegExp(`^linux:(?:[0-9a-f]{32}|-):${UUID}:\\d+$`),
  darwin: new RegExp(`^darwin:(?:${UUID}|-):${UUID}:host$`),
  win32: new RegExp(`^win32:${UUID}:\\d+:host$`),
};
export const validPidNamespace = (value: string | undefined, platform: string = process.platform): boolean =>
  !!value && !!NAMESPACE_FORMS[platform]?.test(value);
/** Any platform's well-formed namespace (a record may come from another OS sharing the root). */
export const wellFormedPidNamespace = (value: string | undefined): boolean =>
  Object.keys(NAMESPACE_FORMS).some(platform => validPidNamespace(value, platform));
const namespaceParts = (value: string): { os: string; machine: string; boot: string } => {
  const [os = "", machine = "", boot = ""] = value.split(":");
  return { os, machine, boot };
};

/** Injectable reader of this process's namespace. Unreadable or malformed is undefined (UNKNOWN), never a guess. */
export const createPidNamespaceReader = (options: {
  platform: NodeJS.Platform; systemRoot?: string | undefined; run?: IncarnationCommandRunner; timeoutMs?: number;
  readFile?: (file: string) => string; readLink?: (file: string) => string;
}) => {
  const { platform, systemRoot, run = runIncarnationCommand, timeoutMs = 2_000 } = options;
  const readFile = options.readFile ?? ((file: string) => fs.readFileSync(file, "utf8"));
  const readLink = options.readLink ?? ((file: string) => fs.readlinkSync(file));
  const linux = (): string | undefined => {
    try {
      const boot = readFile("/proc/sys/kernel/random/boot_id").trim();
      const inode = /^pid:\[(\d+)\]$/.exec(readLink("/proc/self/ns/pid"))?.[1];
      let machine = "-";
      try { const id = readFile("/etc/machine-id").trim(); if (/^[0-9a-f]{32}$/.test(id)) machine = id; } catch { /* unknown machine */ }
      const value = `linux:${machine}:${boot}:${inode ?? ""}`;
      return validPidNamespace(value, "linux") ? value : undefined;
    } catch { return undefined; }
  };
  let own: Promise<string | undefined> | undefined;
  let cached: string | undefined;
  let settled = false;
  const read = async (): Promise<string | undefined> => {
    if (platform === "linux") return linux();
    const command = async (executable: string, args: string[]): Promise<string | undefined> => {
      const abort = new AbortController();
      let timer: ReturnType<typeof setTimeout> | undefined;
      const timeout = new Promise<undefined>((resolve) => { timer = setTimeout(() => { abort.abort(); resolve(undefined); }, timeoutMs); });
      try { return await Promise.race([run(executable, args, { timeout: timeoutMs, maxBuffer: 16_384, windowsHide: true, signal: abort.signal }), timeout]); }
      catch { return undefined; } finally { clearTimeout(timer); }
    };
    let value: string;
    if (platform === "darwin") {
      const boot = (await command("/usr/sbin/sysctl", ["-n", "kern.bootsessionuuid"]))?.trim();
      const machine = /"IOPlatformUUID" = "([0-9A-Fa-f-]{36})"/.exec(await command("/usr/sbin/ioreg", ["-rd1", "-c", "IOPlatformExpertDevice"]) ?? "")?.[1];
      value = `darwin:${machine ?? "-"}:${boot ?? ""}:host`;
    } else if (platform === "win32" && systemRoot) {
      const out = await command(path.win32.join(systemRoot, "System32", "WindowsPowerShell", "v1.0", "powershell.exe"), [
        "-NoLogo", "-NoProfile", "-NonInteractive", "-Command",
        "$g=(Get-ItemProperty -Path 'HKLM:\\SOFTWARE\\Microsoft\\Cryptography' -Name MachineGuid -ErrorAction Stop).MachineGuid;" +
        "$b=(Get-CimInstance -ClassName Win32_OperatingSystem -ErrorAction Stop).LastBootUpTime.ToUniversalTime().Ticks;" +
        "\"$g`:$($b.ToString([System.Globalization.CultureInfo]::InvariantCulture))\""]);
      value = `win32:${out?.trim() ?? ""}:host`;
    } else return undefined;
    return validPidNamespace(value, platform) ? value : undefined;
  };
  return {
    /** This process's namespace (memoized: it cannot change while the process lives). */
    own: (): Promise<string | undefined> => own ??= read().then(value => { cached = value; settled = true; return value; }),
    /** The namespace if already known (Linux: computed now); undefined while unknown or not yet read. */
    ownSync: (): string | undefined => {
      if (settled) return cached;
      if (platform === "linux") { cached = linux(); settled = true; own = Promise.resolve(cached); return cached; }
      return undefined;
    },
  };
};

let namespaceReader: ReturnType<typeof createPidNamespaceReader> | undefined;
let namespaceOverride: { value: string | undefined } | undefined;
const pidNamespaceReader = () => namespaceReader ??= createPidNamespaceReader({ platform: process.platform, systemRoot: process.env.SystemRoot });
/** This process's PID namespace (memoized, lazy); undefined: UNKNOWN. */
export const ownPidNamespace = (): Promise<string | undefined> =>
  namespaceOverride ? Promise.resolve(namespaceOverride.value) : pidNamespaceReader().own();
/** The namespace if it is known now, without waiting. */
export const ownPidNamespaceSync = (): string | undefined => namespaceOverride ? namespaceOverride.value : pidNamespaceReader().ownSync();

/**
 * This process's PID namespace for a reclaim decision: known now, or read within the deadline. A registry-fenced
 * try may not start a native read (as for the incarnation): unknown, so the verdict is "unproven" (fail closed).
 */
export const ownNamespaceForReclaim = async (deadline: number, tryScope: boolean): Promise<string | undefined> => {
  const known = ownPidNamespaceSync();
  if (known !== undefined || tryScope) return known;
  const remaining = deadline - Date.now();
  if (remaining <= 0) return undefined;
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([ownPidNamespace(), new Promise<undefined>(resolve => { timer = setTimeout(() => resolve(undefined), remaining); })]);
  } finally { clearTimeout(timer); }
};

/** Test seam: pretend this process's namespace is `value` (undefined: unknown); `reset` restores the native reader. */
export const pidNamespaceTestHooks = {
  set: (value: string | undefined): void => { namespaceOverride = { value }; },
  reset: (): void => { namespaceOverride = undefined; },
};

/**
 * May a reclaimer use pid evidence (ESRCH, another incarnation) against this holder (smarty-dev#9569)?
 * - `pid`: the holder is of OUR namespace (a protocol-1/2 caller also passes a legacy record, `null`, here);
 * - `foreign`: another namespace: its pid means nothing here; never reclaimed (operator recovery only);
 * - `unproven`: never reclaimed: the record names a malformed namespace, or ours is unknown; or the same machine id
 *   with another boot id (pi-fabric#923, fabric-store: no previous-boot inference, since two running VM clones share a
 *   machine id with different boot ids); or a Windows namespace at all (a machine and boot identity cannot tell
 *   Windows silos apart, so on Windows nothing is reclaimed on pid evidence under protocol 3).
 */
export type PidNamespaceVerdict = "pid" | "foreign" | "unproven";
export const pidNamespaceVerdict = (recordNs: string | null, ownNs: string | undefined): PidNamespaceVerdict => {
  if (recordNs === null) return "pid";
  if (!wellFormedPidNamespace(recordNs) || !ownNs || !wellFormedPidNamespace(ownNs)) return "unproven";
  const record = namespaceParts(recordNs), own = namespaceParts(ownNs);
  if (record.os === "win32" || own.os === "win32") return "unproven";
  if (recordNs === ownNs) return "pid";
  if (record.os === own.os && record.machine !== "-" && record.machine === own.machine && record.boot !== own.boot) return "unproven";
  return "foreign";
};

/**
 * Linux filesystems that live on ONE host (pi-fabric#923 round 3, fabric-store decision). A live VM snapshot clone can
 * reproduce the whole (machine, boot, namespace) identity, but two clones can share a mesh root only on SHARED storage.
 * So pid evidence is allowed only when the root is on one of these local types: ext2/3/4, xfs, btrfs, tmpfs, zfs,
 * f2fs. Anything else (overlay, nfs, cifs/smb, 9p, virtiofs, fuse, ceph, gfs2, ocfs2, unknown) is unproven.
 */
// Round 4: overlay (0x794C7630) is NOT here: its backing filesystem is not checked, so it is unproven.
const LOCAL_LINUX_FILESYSTEMS = new Set([0xEF53, 0x58465342, 0x9123683E, 0x01021994, 0x2FC12FC1, 0xF2F52010]);
let filesystemTypeOverride: { type: number | undefined } | undefined;
/** Test seam: pretend the root's `statfs` type is `type` (undefined: unreadable); `reset` restores the real one. */
export const localFilesystemTestHooks = {
  set: (type: number | undefined): void => { filesystemTypeOverride = { type }; },
  reset: (): void => { filesystemTypeOverride = undefined; },
};
/**
 * Is this mesh root on a known host-local filesystem? Linux: `statfs` type in the allowlist. macOS: false (fail closed):
 * Node exposes only `statfs`'s numeric `f_type`, which macOS assigns at filesystem registration and does not keep stable,
 * and not `f_fstypename`. Windows: false (its namespace never proves a holder dead anyway).
 */
export const onLocalFilesystem = (root: string): boolean => {
  if (filesystemTypeOverride) return filesystemTypeOverride.type !== undefined && LOCAL_LINUX_FILESYSTEMS.has(filesystemTypeOverride.type);
  if (process.platform !== "linux") return false;
  // Round 4: the type of the PINNED root. Node has no fstatfs, so statfs goes through the held fd's /proc entry: a
  // swap of the path after the open cannot change the answer. Anything unreadable is unproven.
  let fd = -1;
  let type: number | undefined;
  try {
    fd = fs.openSync(root, fs.constants.O_RDONLY | fs.constants.O_DIRECTORY | (fs.constants.O_NOFOLLOW ?? 0));
    type = Number(fs.statfsSync(`/proc/self/fd/${fd}`).type);
  } catch { type = undefined; } finally { if (fd >= 0) try { fs.closeSync(fd); } catch { /* closed */ } }
  return type !== undefined && Number.isFinite(type) && LOCAL_LINUX_FILESYSTEMS.has(type);
};

/**
 * The reclaim verdict for a lock under `root`: `pidNamespaceVerdict`, and a "pid" verdict only when the root is on a
 * host-local filesystem (a clone with the same identity can only share the root over shared storage).
 */
export const reclaimVerdict = (recordNs: string | null, ownNs: string | undefined, root: string): PidNamespaceVerdict => {
  const verdict = pidNamespaceVerdict(recordNs, ownNs);
  return verdict === "pid" && recordNs !== null && !onLocalFilesystem(root) ? "unproven" : verdict;
};

/** The lock code's lazy handle on this module (a stable lazy entry point, scripts/build.mjs). */
export type PidNamespaceModule = typeof import("./pid-namespace.js");
