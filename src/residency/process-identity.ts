import fs from "node:fs";
import path from "node:path";
import { processIsAlive } from "../agents/transports/process-utils.js";
import { processIncarnation, validProcessIncarnation } from "../core/atomic-write.js";
import { diagnostics } from "../core/diagnostics.js";
import { ownPidNamespace, ownPidNamespaceSync, wellFormedPidNamespace } from "../core/pid-namespace.js";
import type { ResidentHostOwner } from "./protocol.js";

/** Field 22, after comm (which can itself contain spaces and parentheses). */
export const processStartTime = (pid: number): string | undefined => {
  if (process.platform !== "linux") return undefined;
  try {
    const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
    const close = stat.lastIndexOf(")");
    if (close < 0) return undefined;
    // Counted from after the LAST ")": comm may hold spaces and parentheses. Anything but clock ticks is a
    // parse failure: undefined, which every caller treats as unverified.
    const field = stat.slice(close + 2).trim().split(/\s+/)[19];
    return field !== undefined && /^\d+$/.test(field) ? field : undefined;
  } catch { return undefined; }
};

/**
 * A live pid is the recorded process unless its start time is READ and differs (a reused pid).
 * An unreadable start time (EIO, a permission change) proves nothing, so the process counts as
 * alive: a host lock is never reclaimed from a live host on unknown identity (lane B, #2505).
 */
export const residentProcessAlive = (pid: number, started?: string): boolean => {
  if (!Number.isSafeInteger(pid) || pid <= 0 || !processIsAlive(pid)) return false;
  if (process.platform !== "linux" || started === undefined) return true;
  const current = processStartTime(pid);
  return current === undefined || current === started;
};

/** Native reads happen at actual owner observation/startup, not registration.
 * Linux's synchronous first-use reader also covers publication/route gates;
 * platforms with async native readers remain conservative until preparation. */
export const prepareResidentOwnerIdentity = async (): Promise<void> => { await ownPidNamespace(); };

const warnedLegacyOwners = new Set<string>();
const warnLegacyOwner = (owner: ResidentOwnerIdentity, source: string): void => {
  const key = `${source}:${JSON.stringify(owner)}`;
  if (warnedLegacyOwners.has(key)) return;
  if (warnedLegacyOwners.size >= 256) warnedLegacyOwners.clear();
  warnedLegacyOwners.add(key);
  try { diagnostics.warn("src/residency/process-identity.ts",
    `[pi-fabric] legacy resident owner at ${source}: boot/PID-namespace identity is incomplete; retaining legacy PID liveness`); }
  catch { /* Diagnostics never authorize recovery. */ }
};

type ResidentOwnerIdentity = Pick<ResidentHostOwner, "pid" | "processStartTime" | "bootId" | "pidNamespace"> & Partial<ResidentHostOwner>;
const completeOwnerIdentity = (owner: ResidentOwnerIdentity): boolean => {
  if (typeof owner.bootId !== "string" || !owner.bootId || typeof owner.pidNamespace !== "string" ||
      typeof owner.processStartTime !== "string") return false;
  const platform = owner.pidNamespace.split(":")[0];
  return Number.isSafeInteger(owner.pid) && owner.pid > 0 && wellFormedPidNamespace(owner.pidNamespace) &&
    owner.bootId === owner.pidNamespace.split(":")[2] &&
    (platform === "linux" || platform === "darwin" || platform === "win32") &&
    validProcessIncarnation(owner.processStartTime, platform);
};

// Pin the WHOLE record, including token, host, launch attempt and timestamps, not
// merely pid/birth. A verdict at one owner path never applies at another path.
const ownerGeneration = (owner: ResidentOwnerIdentity): string => {
  const sort = (value: unknown): unknown => Array.isArray(value) ? value.map(sort) :
    value !== null && typeof value === "object" ? Object.fromEntries(Object.entries(value)
      .sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0).map(([key, field]) => [key, sort(field)])) : value;
  return JSON.stringify(sort(owner));
};
const nativeOwnerObservations = new Map<string, { generation: string; birth: string | undefined }>();
const readObservationOwner = (source: string): ResidentHostOwner | undefined => {
  let fd: number | undefined;
  try {
    // O_NONBLOCK also refuses a FIFO without hanging before the regular-file check.
    fd = fs.openSync(source, fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW ?? 0) | (fs.constants.O_NONBLOCK ?? 0));
    const stat = fs.fstatSync(fd), named = fs.lstatSync(source);
    if (!stat.isFile() || !named.isFile() || stat.dev !== named.dev || stat.ino !== named.ino ||
        (process.getuid && stat.uid !== process.getuid())) return undefined;
    const owner: unknown = JSON.parse(fs.readFileSync(fd, "utf8"));
    return owner !== null && typeof owner === "object" && !Array.isArray(owner) ? owner as ResidentHostOwner : undefined;
  } catch { return undefined; }
  finally { if (fd !== undefined) { try { fs.closeSync(fd); } catch { /* Observation never authorizes on an I/O failure. */ } } }
};

/** Actual async use only, before any publication/wake custody or synchronous veto.
 * Linux keeps its cheap synchronous procfs path. Native UNKNOWN never authorizes
 * recovery. Every await is followed by a fresh owner read; a late result cannot
 * condemn a replacement, even one reusing the pid, birth or token independently. */
export const prepareResidentOwnerObservation = async (root: string): Promise<void> => {
  if (process.platform === "linux") return;
  const source = path.resolve(root, "owner.json");
  let owner = readObservationOwner(source);
  if (!owner || !completeOwnerIdentity(owner)) return;
  let own: string | undefined;
  try { own = await ownPidNamespace(); } catch { return; }
  owner = readObservationOwner(source);
  if (!owner || !completeOwnerIdentity(owner) || !own || !wellFormedPidNamespace(own) ||
      owner.pidNamespace !== own || own.split(":")[0] !== process.platform) return;
  const generation = ownerGeneration(owner);
  let birth: string | undefined;
  try { birth = await processIncarnation(owner.pid); } catch { /* Unavailable native evidence is UNKNOWN. */ }
  const current = readObservationOwner(source);
  if (!current || ownerGeneration(current) !== generation || ownPidNamespaceSync() !== own) return;
  if (birth !== undefined && !validProcessIncarnation(birth)) birth = undefined;
  if (nativeOwnerObservations.size >= 256) nativeOwnerObservations.delete(nativeOwnerObservations.keys().next().value!);
  nativeOwnerObservations.set(source, { generation, birth });
};

/** Owner records, NOT generic child/launcher identities. A foreign boot/namespace
 * cannot be made live by a colliding local PID and start tick. UNKNOWN stays live;
 * incomplete/legacy owners retain exactly the old process predicate, with warning.
 * NEVER starts native commands: async entry points prepare exact-generation evidence. */
export const residentHostOwnerAlive = (owner: ResidentOwnerIdentity, source = "owner.json"): boolean => {
  if (!completeOwnerIdentity(owner)) {
    warnLegacyOwner(owner, source);
    return residentProcessAlive(owner.pid, owner.processStartTime);
  }
  const own = ownPidNamespaceSync();
  if (!own || !wellFormedPidNamespace(own)) return true;
  if (owner.bootId !== own.split(":")[2] || owner.pidNamespace !== own) return false;
  if (!residentProcessAlive(owner.pid, owner.processStartTime)) return false;
  if (process.platform === "linux") return true;
  const observed = nativeOwnerObservations.get(path.resolve(source));
  return !observed || observed.generation !== ownerGeneration(owner) || observed.birth === undefined ||
    observed.birth === owner.processStartTime;
};
