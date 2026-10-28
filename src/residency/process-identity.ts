import fs from "node:fs";
import { processIsAlive } from "../agents/transports/process-utils.js";
import { validProcessIncarnation } from "../core/atomic-write.js";
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

type ResidentOwnerIdentity = Pick<ResidentHostOwner, "pid" | "processStartTime" | "bootId" | "pidNamespace">;
/** Owner records, NOT generic child/launcher identities. A foreign boot/namespace
 * cannot be made live by a colliding local PID and start tick. UNKNOWN stays live;
 * incomplete/legacy owners retain exactly the old process predicate, with warning.
 * Linux birth checks use the existing proc reader; other platforms remain conservative. */
export const residentHostOwnerAlive = (owner: ResidentOwnerIdentity, source = "owner.json"): boolean => {
  if (typeof owner.bootId !== "string" || !owner.bootId ||
      typeof owner.pidNamespace !== "string" || !owner.pidNamespace ||
      typeof owner.processStartTime !== "string" || !owner.processStartTime) {
    warnLegacyOwner(owner, source);
    return residentProcessAlive(owner.pid, owner.processStartTime);
  }
  const platform = owner.pidNamespace.split(":")[0];
  const complete = Number.isSafeInteger(owner.pid) && owner.pid > 0 &&
    wellFormedPidNamespace(owner.pidNamespace) &&
    owner.bootId === owner.pidNamespace.split(":")[2] &&
    (platform === "linux" || platform === "darwin" || platform === "win32") &&
    validProcessIncarnation(owner.processStartTime, platform);
  if (!complete) {
    warnLegacyOwner(owner, source);
    return residentProcessAlive(owner.pid, owner.processStartTime);
  }
  const own = ownPidNamespaceSync();
  if (!own || !wellFormedPidNamespace(own)) return true;
  if (owner.bootId !== own.split(":")[2] || owner.pidNamespace !== own) return false;
  return residentProcessAlive(owner.pid, owner.processStartTime);
};
