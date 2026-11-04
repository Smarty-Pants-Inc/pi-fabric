import fs from "node:fs";
import { processStartTime } from "./process-identity.js";

export interface OwnedProcess { pid: number; processStartTime: string; ppid: number; state: string; }
export interface ObservedProcessTree { processes: Map<number, OwnedProcess>; }
const delay = (ms: number): Promise<void> => new Promise(resolve => setTimeout(resolve, ms));
interface SessionProcess extends OwnedProcess { sessionId: number; }
const ownedSessions = new WeakMap<ObservedProcessTree, { childPid: number; birth: string; members: Map<number, string> }>();
function readProcessRow(pid: number, checked = true): SessionProcess {
  const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
  const close = stat.lastIndexOf(")");
  const fields = stat.slice(close + 2).trim().split(/\s+/);
  if (checked && (close < 0 || Number(stat.slice(0, stat.indexOf(" "))) !== pid || fields.length < 20 ||
      !/^[A-Za-z]$/.test(fields[0]!) || !/^\d+$/.test(fields[1]!) ||
      !/^\d+$/.test(fields[3]!) || !/^\d+$/.test(fields[19]!))) throw new Error("invalid proc stat");
  return { pid, ppid: Number(fields[1]), state: fields[0]!, processStartTime: fields[19]!, sessionId: Number(fields[3]) };
}
function processRows(checked = false): SessionProcess[] {
  if (checked) {
    const mounts = fs.readFileSync("/proc/mounts", "utf8");
    if (/\bhidepid=(?!0\b)\w+/.test(mounts)) throw new Error("restricted proc visibility");
    if (!mounts.split("\n").some(line => { const fields = line.split(/\s+/); return fields[1] === "/proc" && fields[2] === "proc"; })) {
      throw new Error("proc mount identity unproven");
    }
  }
  const rows: SessionProcess[] = [];
  for (const name of fs.readdirSync("/proc")) {
    if (!/^\d+$/.test(name)) continue;
    try { rows.push(readProcessRow(Number(name), checked)); }
    catch (error) {
      if (checked && !["ENOENT", "ESRCH"].includes((error as NodeJS.ErrnoException).code ?? "")) throw error;
      // Only the checked session observation requires complete readable evidence.
    }
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

/** Stop birth-validated members of the launch-recorded child's OLD session.
 * Start before native.stop(): the initial live leader must anchor the session.
 * The caller still stops/joins the direct child. Session cleanup is necessary
 * evidence only, never whole-attempt containment or replacement authority.
 */
export async function stopOwnedSessionDescendants(attempt: ObservedProcessTree, childPid: number | undefined): Promise<void> {
  const anchor = childPid === undefined ? undefined : attempt.processes.get(childPid);
  if (process.platform !== "linux" || !childPid || !anchor || !/^\d+$/.test(anchor.processStartTime)) {
    throw new Error("Owned session has no launch-recorded child birth");
  }
  const birth = anchor.processStartTime;
  const previous = ownedSessions.get(attempt);
  if (previous && (previous.childPid !== childPid || previous.birth !== birth)) throw new Error("Owned session launch anchor changed");
  let leader: SessionProcess | undefined;
  try { leader = readProcessRow(childPid); }
  catch (error) { if (!["ENOENT", "ESRCH"].includes((error as NodeJS.ErrnoException).code ?? "")) throw error; }
  if (leader && (leader.processStartTime !== birth || leader.sessionId !== childPid)) throw new Error("Owned session child birth or session changed");
  if (!leader && !previous) throw new Error("Owned session has no live initial child anchor");
  const sessionMembers = previous?.members ?? new Map<number, string>([[childPid, birth]]);
  if (!previous) ownedSessions.set(attempt, { childPid, birth, members: sessionMembers });
  const capture = (requireContinuity = false) => {
    const check = checkResidentSessionExit(childPid);
    if (!check.known) throw new Error(`Owned session membership unknown: ${check.reason}`);
    const rows = processRows(true);
    const currentLeader = rows.find(row => row.pid === childPid);
    if (currentLeader && (currentLeader.processStartTime !== birth || currentLeader.sessionId !== childPid)) {
      throw new Error("Owned session child identity was reused");
    }
    // Reentry after native child exit needs a previously validated OLD member,
    // not a newly discovered process with the same numeric SID or an empty SID.
    if (requireContinuity && !rows.some(row => row.pid !== childPid && row.sessionId === childPid && sessionMembers.get(row.pid) === row.processStartTime)) {
      throw new Error("Owned session continuity after child exit is unproven");
    }
    for (const row of rows) {
      const previous = sessionMembers.get(row.pid);
      if (previous !== undefined && (previous !== row.processStartTime || row.sessionId !== childPid)) {
        throw new Error("Owned session member identity or session changed");
      }
      if (row.sessionId !== childPid) continue;
      if (BigInt(row.processStartTime) < BigInt(birth)) throw new Error("Owned session member predates child birth");
      const tracked = attempt.processes.get(row.pid);
      if (tracked && tracked.processStartTime !== row.processStartTime) throw new Error("Owned session member PID was reused");
      sessionMembers.set(row.pid, row.processStartTime);
      attempt.processes.set(row.pid, row);
    }
    const members = rows.filter(row => row.sessionId === childPid);
    // A group exists but neither complete scan found a member: do not pretend
    // that restricted/racing membership is a proven empty boundary.
    if (!check.empty && !check.members.length && !members.length) throw new Error("Owned session group membership unproven");
    return { members, leader: currentLeader };
  };
  let initial: ReturnType<typeof capture> | undefined = capture(!leader);
  for (const signal of ["SIGTERM", "SIGKILL"] as const) {
    const signaled = new Set<string>();
    const deadline = Date.now() + 5_000;
    do {
      const snapshot = initial ?? capture();
      initial = undefined;
      for (const row of snapshot.members.filter(row => row.pid !== childPid).reverse()) {
        const identity = `${row.pid}:${row.processStartTime}`;
        if (row.state === "Z" || signaled.has(identity)) continue;
        let current: SessionProcess;
        try { current = readProcessRow(row.pid); }
        catch (error) { if (["ENOENT", "ESRCH"].includes((error as NodeJS.ErrnoException).code ?? "")) continue; throw error; }
        if (current.processStartTime !== row.processStartTime || current.sessionId !== childPid) throw new Error("Owned session signal identity changed");
        // Revalidate the leader too: a recycled PID must not anchor a new session.
        let currentChild: SessionProcess | undefined;
        try { currentChild = readProcessRow(childPid); }
        catch (error) { if (!["ENOENT", "ESRCH"].includes((error as NodeJS.ErrnoException).code ?? "")) throw error; }
        if (currentChild && (currentChild.processStartTime !== birth || currentChild.sessionId !== childPid)) throw new Error("Owned session child identity was reused");
        try { process.kill(row.pid, signal); }
        catch (error) { if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error; }
        signaled.add(identity);
      }
      if (!snapshot.members.some(row => row.pid !== childPid) && (!snapshot.leader || snapshot.leader.state === "Z")) return;
      await delay(50); // Recapture SID membership, not just surviving PPID edges.
    } while (Date.now() < deadline);
  }
  throw new Error("Owned session descendants did not exit; cleanup remains unproven");
}

/** Necessary exit check for the detached child's entire Linux session, including
 * reparented members and zombies. This is NOT whole-attempt containment: workers
 * can call setsid()/enter external scopes. Never use an empty session alone as
 * permission to replace a serving resident (see assertAutomaticReleaseRecovery).
 * Unlike ancestry sampling, an unreadable member makes the check fail closed.
 */
export function checkResidentSessionExit(sessionId: number | undefined): { empty: boolean; reason: string; members: number[]; known: boolean } {
  const unavailable = (reason: string) => ({ empty: false, reason, members: [] as number[], known: false });
  if (process.platform !== "linux" || !sessionId) return unavailable("owned session check unavailable");
  try {
    // hidepid makes absence unprovable. This check never treats restricted /proc
    // visibility or an enumeration/read failure as an empty session.
    let rows: SessionProcess[];
    try { rows = processRows(true); }
    catch (error) {
      if (error instanceof Error && ["restricted proc visibility", "invalid proc stat"].includes(error.message)) return unavailable(error.message);
      throw error;
    }
    const members = rows.filter(row => row.sessionId === sessionId).map(row => row.pid);
    // Kernel group existence also catches a member born after enumeration. No
    // destructive group signal: owned shutdown remains PID/birth validated.
    try { process.kill(-sessionId, 0); return { empty: false, reason: "owned process group still exists", members, known: true }; }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error; }
    return { empty: members.length === 0, reason: members.length ? "owned session still has members" : "owned session exited", members, known: true };
  } catch (error) { return unavailable(`session exit unproven: ${String(error).slice(0, 300)}`); }
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
