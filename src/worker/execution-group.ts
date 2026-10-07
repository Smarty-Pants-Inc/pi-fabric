import fs from "node:fs";
import type { ChildProcess } from "node:child_process";

type Member = { pid: number; group: number; started: string; state: string };
const member = (pid: number): Member | undefined => {
  try {
    const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
    const fields = stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/);
    return { pid, group: Number(fields[2]), started: fields[19]!, state: fields[0]! };
  } catch (error) {
    if (["ENOENT", "ESRCH"].includes((error as NodeJS.ErrnoException).code ?? "")) return undefined;
    throw error;
  }
};

/** A group number is never ownership. Keep same-birth anchors while the native
 * leader lives, then retain surviving anchors until the entire group is empty.
 * An unobserved/recycled group is an unresolved obligation, not signal authority. */
export const executionGroup = (child: ChildProcess) => {
  // A Windows native child handle is not an owned execution-tree boundary.
  // Windows keeps its legacy native-child behavior outside this receipt API.
  if (process.platform === "win32") throw new Error("Windows execution-tree custody is unsupported");
  const pid = child.pid;
  const owned = new Map<number, string>();
  let empty = false;
  let closed = false;
  child.once("close", () => { closed = true; });
  if (process.platform === "linux" && pid) {
    try {
      const leader = member(pid);
      if (leader) owned.set(pid, leader.started);
    } catch { /* Retain an unresolved obligation; never escape after spawn. */ }
  }
  const members = (): Member[] => {
    if (empty || !pid) return [];
    const current = fs.readdirSync("/proc").flatMap(entry => {
      if (!/^\d+$/.test(entry)) return [];
      const value = member(Number(entry));
      return value && value.group === pid && !["Z", "X"].includes(value.state) ? [value] : [];
    });
    if (!current.length) { empty = true; return []; }
    if (!current.some(value => {
      const now = member(value.pid);
      return owned.get(value.pid) === value.started && now?.started === value.started && now.group === pid;
    })) {
      throw new Error(`Execution group ${pid} has no surviving owned birth; exit unconfirmed`);
    }
    for (const value of current) owned.set(value.pid, value.started);
    return current;
  };
  return {
    /** Refresh birth anchors; returns the observed membership so callers can back off while it is stable. */
    observe(): string {
      return process.platform === "linux" ? members().map(value => `${value.pid}:${value.started}`).join(",") : "";
    },
    exited(): boolean {
      if (!pid) return closed;
      if (process.platform === "linux") return members().length === 0;
      // Portable POSIX: pipes closing is not proof the detached group emptied.
      // Never signal a leaderless group without birth-safe ownership.
      if (empty) return true;
      try { process.kill(-pid, 0); return false; }
      catch (error) { if ((error as NodeJS.ErrnoException).code === "ESRCH") { empty = true; return true; } throw error; }
    },
    signal(signal: NodeJS.Signals): void {
      if (!pid) return;
      if (process.platform === "linux") {
        if (!members().length) return;
        // Refresh immediately before signal, not only in the observation timer.
        if (!members().some(value => {
          const now = member(value.pid);
          return now !== undefined && now.started === owned.get(value.pid) && now.group === pid;
        })) {
          throw new Error(`Execution group ${pid} exit unconfirmed`);
        }
      } else if (child.exitCode !== null || child.signalCode !== null) {
        throw new Error(`Execution group ${pid} leader exited; birth-safe cleanup unavailable`);
      }
      try { process.kill(-pid, signal); }
      catch (error) { if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error; }
    },
  };
};

/** A full /proc scan costs ~15-20 ms of CPU on a busy host (~1,000 processes),
 * so a fixed 100 ms observer kept every idle worker at ~17% of a core
 * (smarty-dev#4250). Observe at the original 100 ms while the group may be
 * changing: at start, after any observed membership change, and whenever the
 * caller pokes on child activity (Pi announces a tool before spawning it).
 * While the membership is unchanged, double the interval up to the cap. Cleanup,
 * exit checks and signals still scan immediately, so they never rely on this
 * cadence. */
export const GROUP_OBSERVE_MIN_MS = 100;
export const GROUP_OBSERVE_MAX_MS = 5_000;
export const observeGroupAdaptively = (
  observe: () => string,
  { minMs = GROUP_OBSERVE_MIN_MS, maxMs = GROUP_OBSERVE_MAX_MS }: { minMs?: number; maxMs?: number } = {},
) => {
  let delay = minMs;
  let last: string | undefined;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let stopped = false;
  const schedule = (): void => {
    if (stopped) return;
    timer = setTimeout(tick, delay);
  };
  const tick = (): void => {
    timer = undefined;
    let snapshot: string;
    // An unconfirmed group fails closed at cleanup; repeating the same error is not a change.
    try { snapshot = observe(); } catch (error) { snapshot = `error:${String(error)}`; }
    delay = snapshot === last ? Math.min(delay * 2, maxMs) : minMs;
    last = snapshot;
    schedule();
  };
  schedule();
  return {
    /** Child activity: return to the fast cadence (a no-op while already fast). */
    poke(): void {
      if (stopped || delay === minMs) return;
      delay = minMs;
      if (timer) clearTimeout(timer);
      schedule();
    },
    stop(): void {
      stopped = true;
      if (timer) clearTimeout(timer);
      timer = undefined;
    },
    get delayMs(): number { return delay; },
  };
};
