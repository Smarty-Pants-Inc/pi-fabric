import fs from "node:fs";
import { recordOwnerLiveness, type LivenessProbes } from "../core/atomic-write.js";
import { processStartTime } from "./process-identity.js";

export interface OwnedProcess { pid: number; processStartTime: string; ppid: number; state: string; }
export interface ObservedProcessTree { processes: Map<number, OwnedProcess>; }
const delay = (ms: number): Promise<void> => new Promise(resolve => setTimeout(resolve, ms));
function processRows(): OwnedProcess[] {
  const rows: OwnedProcess[] = [];
  for (const name of fs.readdirSync("/proc")) {
    if (!/^\d+$/.test(name)) continue;
    try {
      const stat = fs.readFileSync(`/proc/${name}/stat`, "utf8");
      const fields = stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/);
      rows.push({ pid: Number(name), ppid: Number(fields[1]), state: fields[0]!, processStartTime: fields[19]! });
    } catch { /* Exited during observation. */ }
  }
  return rows;
}
export function captureDescendants(attempt: ObservedProcessTree): void {
  if (process.platform !== "linux") return;
  const rows = processRows();
  const selected = new Set(rows.filter((row) => attempt.processes.get(row.pid)?.processStartTime === row.processStartTime).map((row) => row.pid));
  let changed = true;
  while (changed) {
    changed = false;
    for (const row of rows) if (selected.has(row.ppid) && !selected.has(row.pid)) { selected.add(row.pid); changed = true; }
  }
  for (const row of rows) if (selected.has(row.pid)) attempt.processes.set(row.pid, row);
}
function ownedAlive(attempt: ObservedProcessTree): OwnedProcess[] {
  return processRows().filter((row) => row.state !== "Z" && attempt.processes.get(row.pid)?.processStartTime === row.processStartTime);
}
export function observeProcessTree(pid: number): ObservedProcessTree {
  const birth = processStartTime(pid);
  const processes = new Map<number, OwnedProcess>();
  if (birth !== undefined) processes.set(pid, { pid, processStartTime: birth, ppid: 0, state: "" });
  return { processes };
}

/** Join the existing launcher's birth-validated observed descendant cleanup.
 * Sampling does not prove complete membership and must never authorize fallback.
 * The direct child independently owes its native exit/close receipt.
 */
export async function stopObservedDescendants(attempt: ObservedProcessTree, childPid: number | undefined): Promise<void> {
  if (process.platform !== "linux") return;
  captureDescendants(attempt);
  const descendantsAlive = () => ownedAlive(attempt).filter(row => row.pid !== childPid);
  for (const signal of ["SIGTERM", "SIGKILL"] as const) {
    for (const row of descendantsAlive().reverse()) {
      if (processStartTime(row.pid) !== row.processStartTime) throw new Error("Owned successor birth became uncertain");
      try { process.kill(row.pid, signal); } catch (error) { if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error; }
    }
    const deadline = Date.now() + 5_000;
    while (descendantsAlive().length && Date.now() < deadline) { captureDescendants(attempt); await delay(50); }
    if (!descendantsAlive().length) return;
  }
  throw new Error("Observed resident processes did not exit; fallback is blocked");
}

export interface ResidentOwnerObservation {
  claimed: boolean;
  observedOwner: boolean;
  closeInput: boolean;
}

export function observeResidentOwner(
  ownerPid: number | undefined,
  childPid: number | undefined,
  claimed: boolean,
): ResidentOwnerObservation {
  if (childPid === undefined) {
    return { claimed, observedOwner: ownerPid !== undefined, closeInput: false };
  }
  if (ownerPid === childPid) {
    return { claimed: true, observedOwner: true, closeInput: false };
  }
  if (ownerPid !== undefined) {
    return { claimed, observedOwner: true, closeInput: true };
  }
  return { claimed, observedOwner: false, closeInput: claimed };
}

const signalAlive = (pid: number): boolean => {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
};

// An owner in another PID namespace is judged by its heartbeat; "unknown"
// keeps it observed. Identity-less owners keep the plain signal probe.
export const liveOwnerPid = (ownerPath: string, probes?: LivenessProbes): number | undefined => {
  try {
    const owner = JSON.parse(fs.readFileSync(ownerPath, "utf8")) as { pid?: unknown };
    if (typeof owner.pid !== "number") return undefined;
    const liveness = recordOwnerLiveness(owner, {
      legacyAlive: signalAlive,
      ...(probes ? { probes } : {}),
    });
    return liveness === "dead" ? undefined : owner.pid;
  } catch {
    return undefined;
  }
};
