import fs from "node:fs";
import { processIsAlive } from "../agents/transports/process-utils.js";

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
