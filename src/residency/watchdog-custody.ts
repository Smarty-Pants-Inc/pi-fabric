import fs from "node:fs";
import path from "node:path";

/** Persistent uncertainty, not a PID lease. Only an explicit complete drain can
 * release this boundary; a dead launcher/host leaves escaped workers unproven. */
export const watchdogCustodyPath = (root: string): string => path.join(root, "watchdog-custody.json");

export function assertNoWatchdogCustody(root: string): void {
  try { fs.lstatSync(watchdogCustodyPath(root)); }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return;
    throw new Error(`Resident watchdog custody cannot be checked; startup deferred: ${String(error).slice(0, 300)}`);
  }
  throw new Error("Resident watchdog custody retains an attempt with unproven complete exit; startup deferred until explicit installer drain");
}
