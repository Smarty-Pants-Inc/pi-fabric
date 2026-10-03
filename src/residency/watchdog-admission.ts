import fs from "node:fs";
import path from "node:path";
import { syncDirectoryChain } from "../core/atomic-write.js";

export const residentWatchdogAlarmPath = (root: string): string => path.join(root, "watchdog-alarm.json");
export const RESIDENT_WATCHDOG_BLOCKED = "Resident watchdog recovery is blocked: complete attempt-owned descendant exit is unproven; use the explicit installer drain";

/** Persistent admission debt, not a process tracker or a descendant-exit receipt.
 * Like worker stop's unconfirmed-tree debt, native child exit cannot clear it.
 * No runtime/config opt-in, fence absence, PID scan or lease expiry clears it.
 */
export function latchResidentWatchdogAlarm(root: string, details: Record<string, unknown>): void {
  fs.mkdirSync(root, { recursive: true, mode: 0o700 });
  // Unlike a positive custody receipt, this is a negative admission latch:
  // even empty/torn bytes must veto a successor. Reserve it exclusively and
  // durably BEFORE fallible diagnostic serialization/writes, so a failed write
  // cannot leave the root looking unalarmed. Never replace or unlink it.
  const fd = fs.openSync(residentWatchdogAlarmPath(root), "wx", 0o600);
  try {
    fs.fsyncSync(fd);
    syncDirectoryChain(root);
    fs.writeFileSync(fd, JSON.stringify({
      ...details, error: RESIDENT_WATCHDOG_BLOCKED, alarmedAt: Date.now(),
    }, null, 2));
    fs.fsyncSync(fd);
  } finally { fs.closeSync(fd); }
}

/** Only an absent alarm permits ordinary cold start. Existing, malformed,
 * unreadable and symlink markers all block admission; their contents are never
 * interpreted as proof. Explicit drain must establish complete exit before an
 * operator removes this marker. Nothing in automatic recovery removes it.
 */
export function assertResidentWatchdogAdmission(root: string): void {
  try { fs.lstatSync(residentWatchdogAlarmPath(root)); }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return;
    throw new Error(RESIDENT_WATCHDOG_BLOCKED, { cause: error });
  }
  throw new Error(RESIDENT_WATCHDOG_BLOCKED);
}
