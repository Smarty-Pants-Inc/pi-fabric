import fs from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { syncDirectoryChain } from "../core/atomic-write.js";
import { processStartTime, residentProcessAlive } from "./process-identity.js";

export const residentWatchdogAlarmPath = (root: string): string => path.join(root, "watchdog-alarm.json");
export const residentWatchdogAttemptPath = (root: string): string => path.join(root, "watchdog-attempt.json");
export const RESIDENT_WATCHDOG_BLOCKED = "Resident watchdog recovery is blocked: complete attempt-owned descendant exit is unproven; use the explicit installer drain";
const vetoed = new Set<string>();

function present(file: string): boolean {
  try { fs.lstatSync(file); return true; }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return false;
    throw new Error(RESIDENT_WATCHDOG_BLOCKED, { cause: error });
  }
}

/** Latch in memory BEFORE any fallible persistence. The pre-launch attempt debt
 * independently survives an initial alarm open failure and launcher exit. */
export function latchResidentWatchdogAlarm(root: string, details: Record<string, unknown>): void {
  vetoed.add(path.resolve(root));
  fs.mkdirSync(root, { recursive: true, mode: 0o700 });
  const fd = fs.openSync(residentWatchdogAlarmPath(root), "wx", 0o600);
  try {
    fs.fsyncSync(fd);
    syncDirectoryChain(root);
    fs.writeFileSync(fd, JSON.stringify({ ...details, error: RESIDENT_WATCHDOG_BLOCKED, alarmedAt: Date.now() }, null, 2));
    fs.fsyncSync(fd);
  } finally { fs.closeSync(fd); }
}

export function assertResidentWatchdogAlarmClear(root: string): void {
  if (vetoed.has(path.resolve(root)) || present(residentWatchdogAlarmPath(root))) throw new Error(RESIDENT_WATCHDOG_BLOCKED);
}

/** Existing ready-host attachment is not successor admission. Only the exact
 * child of the reserving launcher may enter using its private launch token.
 * A free lock, dead PID, malformed marker or alarm absence never clears debt. */
export function assertResidentWatchdogAdmission(root: string, launchToken?: string): void {
  assertResidentWatchdogAlarmClear(root);
  const file = residentWatchdogAttemptPath(root);
  if (!present(file)) return;
  if (launchToken) {
    try {
      if (!fs.lstatSync(file).isFile() || fs.lstatSync(file).isSymbolicLink()) throw new Error("Invalid attempt marker");
      const attempt = JSON.parse(fs.readFileSync(file, "utf8"));
      if (attempt.token === launchToken && attempt.pid === process.ppid &&
          (process.platform !== "linux" || typeof attempt.processStartTime === "string") &&
          residentProcessAlive(attempt.pid, attempt.processStartTime)) return;
    } catch { /* Unknown is blocked, never an admission receipt. */ }
  }
  throw new Error(RESIDENT_WATCHDOG_BLOCKED);
}

/** Fail before spawning if reservation/write/fsync fails. Never replace an old
 * attempt's marker, even if its launcher is dead. Explicit checked drain owns it. */
export function reserveResidentWatchdogAttempt(root: string): string {
  assertResidentWatchdogAdmission(root);
  fs.mkdirSync(root, { recursive: true, mode: 0o700 });
  const token = randomUUID();
  const fd = fs.openSync(residentWatchdogAttemptPath(root), "wx", 0o600);
  try {
    fs.writeFileSync(fd, JSON.stringify({ format: 1, token, pid: process.pid, processStartTime: processStartTime(process.pid) }));
    fs.fsyncSync(fd);
    syncDirectoryChain(root);
  } finally { fs.closeSync(fd); }
  return token;
}

/** Ordinary non-watchdog native exit preserves the existing cold-start policy.
 * Alarmed exits (including failed alarm persistence) retain debt. A killed
 * launcher cannot execute this release. This is NOT a descendant-exit receipt
 * and cannot authorize watchdog recovery. */
export function releaseResidentWatchdogAttempt(root: string, token: string): void {
  if (vetoed.has(path.resolve(root)) || present(residentWatchdogAlarmPath(root))) return;
  const file = residentWatchdogAttemptPath(root);
  if (!present(file)) return;
  const attempt = JSON.parse(fs.readFileSync(file, "utf8"));
  if (attempt.token !== token || attempt.pid !== process.pid || attempt.processStartTime !== processStartTime(process.pid)) throw new Error(RESIDENT_WATCHDOG_BLOCKED);
  fs.unlinkSync(file);
  syncDirectoryChain(root);
}
