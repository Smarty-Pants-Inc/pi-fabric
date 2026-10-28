import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { knownOwnProcessIncarnation, ownProcessIncarnation, processIncarnation, readPhysicalHostIdentity, validBootId, validProcessIncarnation } from "./atomic-write.js";
import { diagnostics } from "./diagnostics.js";
import { ownPidNamespaceSync } from "./pid-namespace.js";

/** Shared with JSON lock owners. Unknown identity is recorded, but never proves staleness. */
export interface LockIdentity {
  pid: number;
  start: string | undefined;
  bootId: string | undefined;
  namespace: string | undefined;
}
export type LockIdentityVerdict = "live" | "unknown" | "other-boot" | "other-namespace" | "dead-pid" | "reused-pid";
const namespacePlatform = (namespace: string | undefined): "linux" | "darwin" | "win32" | undefined => {
  const platform = namespace?.split(":")[0];
  return platform === "linux" || platform === "darwin" || platform === "win32" ? platform : undefined;
};

/** Actual acquisition only: share the SAME immutable namespace cache with async reclaimers. Re-reading machine-id
 * independently could make a live sync owner appear foreign after that file changes. The synchronous reader never
 * starts ps, sysctl, ioreg or PowerShell; non-Linux platforms only reuse already-prepared native identity. */
export const readLockIdentitySync = (): LockIdentity => {
  const namespace = ownPidNamespaceSync();
  let start = knownOwnProcessIncarnation();
  let bootId = namespace?.split(":")[2];
  if (process.platform === "linux") {
    start = undefined;
    try {
      const stat = fs.readFileSync(`/proc/${process.pid}/stat`, "utf8");
      const value = stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/)[19];
      if (validProcessIncarnation(value, "linux")) start = value;
    } catch { /* missing native evidence stays UNKNOWN */ }
    if (!bootId) try {
      const value = fs.readFileSync("/proc/sys/kernel/random/boot_id", "utf8").trim();
      if (validBootId(value)) bootId = value;
    } catch { /* retain independently known process evidence */ }
  }
  return { pid: process.pid, start, bootId, namespace };
};

/** Native reads happen at actual acquisition, never import/registration. Prepared callers must not start native commands. */
export const readLockIdentity = async (options: { start?: string | undefined; prepared?: boolean } = {}): Promise<LockIdentity> => {
  const namespaces = await import("./pid-namespace.js");
  const knownNamespace = namespaces.ownPidNamespaceSync();
  const namespace = knownNamespace ?? (options.prepared ? undefined : await namespaces.ownPidNamespace());
  // An explicitly supplied start is already prepared, including UNKNOWN: do not retry a failed read. A bounded
  // caller without a supplied value uses only the cache; an ordinary first use may start the native reader.
  const start = Object.hasOwn(options, "start") ? options.start : options.prepared
    ? knownOwnProcessIncarnation() : await ownProcessIncarnation().catch(() => undefined);
  return { pid: process.pid, start, namespace, bootId: namespace?.split(":")[2] ?? readPhysicalHostIdentity()?.bootId };
};

/** Validate the holder's platform, not ours: another OS can have left a record on the same volume. */
export const completeLockIdentity = async (identity: LockIdentity): Promise<boolean> => {
  const namespaces = await import("./pid-namespace.js");
  return Number.isSafeInteger(identity.pid) && identity.pid > 0 && namespaces.wellFormedPidNamespace(identity.namespace) &&
    !!identity.bootId && identity.bootId === identity.namespace?.split(":")[2] &&
    validProcessIncarnation(identity.start, namespacePlatform(identity.namespace));
};

/** Explicit restart policy for NEW complete owners, including on PV/overlay. Legacy namespace policy is separate. */
export const lockIdentityVerdict = async (identity: LockIdentity, deadline: number, bounded = false): Promise<LockIdentityVerdict> => {
  if (!await completeLockIdentity(identity)) return "unknown";
  const namespaces = await import("./pid-namespace.js");
  const ownNamespace = await namespaces.ownNamespaceForReclaim(deadline, bounded);
  if (!ownNamespace || !namespaces.wellFormedPidNamespace(ownNamespace)) return "unknown";
  if (identity.bootId !== ownNamespace.split(":")[2]) return "other-boot";
  if (identity.namespace !== ownNamespace) return "other-namespace";
  try { process.kill(identity.pid, 0); }
  catch (error) {
    if (typeof error === "object" && error !== null && "code" in error && error.code === "ESRCH") return "dead-pid";
    return "unknown";
  }
  // Our cached native start is immutable for this process and already prepared by the writer. Comparing it
  // needs no new command or wait, including a zero-budget host-commit acquisition after a PID was reused.
  if (identity.pid === process.pid) {
    const actual = knownOwnProcessIncarnation();
    if (actual) return actual === identity.start ? "live" : "reused-pid";
  }
  if (bounded) return "unknown";
  const remaining = deadline - Date.now();
  if (remaining <= 0) return "unknown";
  let timer: ReturnType<typeof setTimeout> | undefined;
  let actual: string | undefined;
  try {
    actual = await Promise.race([
      processIncarnation(identity.pid, remaining).catch(() => undefined),
      new Promise<undefined>(resolve => { timer = setTimeout(() => resolve(undefined), remaining); }),
    ]);
  } finally { clearTimeout(timer); }
  if (!actual || Date.now() >= deadline) return "unknown";
  return actual === identity.start ? "live" : "reused-pid";
};

// Nine split fields (including the final empty field): no old mesh (4/5/6) or custody (4/5/6/7/8) parser accepts it.
// Keep token, pid, creation time and native start in their original slots for diagnostics and import-fence receipts.
const OWNER_FORMAT = "fabric-lock-owner-v1";
export const formatLockOwner = (token: string, createdAt: number, identity: LockIdentity, machine = ""): string =>
  `${token}\n${identity.pid}\n${createdAt}\n${identity.start ?? ""}\n${machine}\n${identity.bootId ?? ""}\n${identity.namespace ?? ""}\n${OWNER_FORMAT}\n`;
export const isIdentityLockOwner = (owner: string): boolean => owner.split("\n")[7] === OWNER_FORMAT;
export const parseIdentityLockOwner = async (owner: string): Promise<{ token: string; createdAt: number; identity: LockIdentity } | undefined> => {
  const fields = owner.split("\n");
  if (fields.length !== 9 || fields[7] !== OWNER_FORMAT || fields[8] !== "" || !fields[0] || !fields[2]?.trim()) return undefined;
  const createdAt = Number(fields[2]);
  const identity: LockIdentity = { pid: Number(fields[1]), start: fields[3], bootId: fields[5], namespace: fields[6] };
  const namespaces = await import("./pid-namespace.js");
  if ((identity.namespace && !namespaces.wellFormedPidNamespace(identity.namespace)) ||
    (identity.start && !(identity.namespace ? validProcessIncarnation(identity.start, namespacePlatform(identity.namespace))
      : (["linux", "darwin", "win32"] as const).some(platform => validProcessIncarnation(identity.start, platform)))) ||
    (identity.bootId && !validBootId(identity.bootId) && !/^\d+$/.test(identity.bootId)) ||
    (identity.namespace && identity.bootId && identity.bootId !== identity.namespace.split(":")[2])) return undefined;
  if (!Number.isSafeInteger(createdAt) || createdAt < 0 || !Number.isFinite(new Date(createdAt).getTime()) ||
    !Number.isSafeInteger(identity.pid) || identity.pid <= 0 || fields[1] !== String(identity.pid)) return undefined;
  return { token: fields[0], createdAt, identity };
};

/** An incomplete new writer retains the selected protocol's existing degraded-identity behavior. The ORIGINAL
 * bytes remain the ownership/anti-ABA fence and the known metadata stays on disk; this is only a legacy policy view.
 * In particular protocol 3 never invents a missing namespace, and host-qualified gates keep machine/boot evidence.
 */
export const legacyLockOwnerFields = (owner: string, protocol: 1 | 2 | 3, hostQualified = false): string[] => {
  const fields = owner.split("\n");
  const legacy = fields.slice(0, hostQualified ? 6 : 4);
  if (protocol === 3) legacy.push(fields[6] ?? "");
  else if (!hostQualified && !fields[3]) legacy.pop();
  legacy.push("");
  return legacy;
};

const warnedLegacy = new Set<string>();
export const warnLegacyLockOwner = (lock: string, owner: string): void => {
  const key = `${lock}:${createHash("sha256").update(owner).digest("hex")}`;
  if (warnedLegacy.has(key)) return;
  if (warnedLegacy.size >= 256) warnedLegacy.clear();
  warnedLegacy.add(key);
  try { diagnostics.warn("src/core/lock-identity.ts", `[pi-fabric] legacy lock owner at ${lock}: boot/PID-namespace/process-start identity is incomplete; retaining legacy recovery policy`); }
  catch { /* diagnostics never authorize or prevent recovery */ }
};

/** Retain the original directory, with a deterministic nonempty anti-ABA destination and a JSON decision receipt.
 * The date comes from immutable owner bytes, NEVER the recovery clock: a contender paused until another day must
 * still target the very same nonempty fence. The sibling receipt records the stale observation before detachment;
 * its existence alone is not a claim that rename succeeded. No canonical file is written or removed here.
 */
export const identityStaleLockAside = (lock: string, stat: fs.Stats, owner: string, createdAt: number): string => {
  const digest = createHash("sha256").update(`${stat.dev}:${stat.ino}:${owner}`).digest("hex");
  const date = new Date(createdAt).toISOString().replace(/[:.]/g, "-");
  return `${lock}.dead.${date}.${digest}`;
};

export const retainIdentityStaleLock = (lock: string, stat: fs.Stats, owner: string, createdAt: number,
  reason: LockIdentityVerdict | "operator-stopped", identity: LockIdentity, beforeRename?: () => void): boolean => {
  const aside = identityStaleLockAside(lock, stat, owner, createdAt);
  const unchanged = (): boolean => {
    const current = fs.lstatSync(lock);
    return current.isDirectory() && current.dev === stat.dev && current.ino === stat.ino &&
      fs.readFileSync(path.join(lock, "owner"), "utf8") === owner;
  };
  if (!unchanged()) return false;
  try {
    fs.writeFileSync(`${aside}.recovery.json`, `${JSON.stringify({ format: 1, kind: "stale-lock-observation", observedAt: new Date().toISOString(),
      source: lock, aside, reason, identity, owner, dev: String(stat.dev), ino: String(stat.ino), recovererPid: process.pid })}\n`,
    { flag: "wx", mode: 0o600 });
  } catch (error) {
    if (!(typeof error === "object" && error !== null && "code" in error && error.code === "EEXIST")) throw error;
  }
  if (!unchanged()) return false;
  beforeRename?.();
  if (!unchanged()) return false;
  // Retain forever. A second cleaner's native rename cannot replace this nonempty directory with a successor.
  fs.renameSync(lock, aside);
  return true;
};
