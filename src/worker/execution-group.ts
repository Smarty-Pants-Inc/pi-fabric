import fs from "node:fs";
import type { ChildProcess } from "node:child_process";

/** Background observation cadence for a live execution group. Each tick walks
 * only the child's own descendants; signal() and exited() refresh on demand, so
 * cleanup never relies on this timer having fired recently (#10009). */
export const EXECUTION_GROUP_OBSERVE_MS = 2_000;
/** Upper bound on processes visited by one descendant walk. */
const WALK_LIMIT = 4_096;

type Member = { pid: number; group: number; started: string; state: string };
const gone = (error: unknown): boolean => ["ENOENT", "ESRCH"].includes((error as NodeJS.ErrnoException).code ?? "");
const member = (pid: number): Member | undefined => {
  try {
    const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
    const fields = stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/);
    return { pid, group: Number(fields[2]), started: fields[19]!, state: fields[0]! };
  } catch (error) {
    if (gone(error)) return undefined;
    throw error;
  }
};

let childrenAvailable: boolean | undefined;
/** /proc/<pid>/task/<tid>/children needs CONFIG_PROC_CHILDREN (Linux >= 3.5).
 * Probe once per process, on first use, and log the fallback once. */
const descendantWalkAvailable = (): boolean => {
  if (childrenAvailable !== undefined) return childrenAvailable;
  childrenAvailable = fs.existsSync(`/proc/${process.pid}/task/${process.pid}/children`);
  if (!childrenAvailable) {
    process.stderr.write("[pi-fabric] /proc/<pid>/task/<tid>/children unavailable; execution-group custody falls back to a full /proc scan\n");
  }
  return childrenAvailable;
};

/** Direct children of pid across all of its threads; [] once pid is gone. */
const childrenOf = (pid: number): number[] => {
  let threads: string[];
  try { threads = fs.readdirSync(`/proc/${pid}/task`); }
  catch (error) { if (gone(error)) return []; throw error; }
  return threads.flatMap(thread => {
    try {
      return fs.readFileSync(`/proc/${pid}/task/${thread}/children`, "utf8").split(" ").filter(Boolean).map(Number);
    } catch (error) {
      if (gone(error)) return [];
      throw error;
    }
  });
};

/** Every live process reachable from roots by parent links (roots included).
 * truncated is true when WALK_LIMIT stopped the walk with unvisited pids still
 * queued: members is then a partial view and must never be treated as complete. */
const descendants = (roots: Iterable<number>): { members: Member[]; truncated: boolean } => {
  const seen = new Set<number>();
  const found: Member[] = [];
  const queue = [...roots];
  while (queue.length && seen.size < WALK_LIMIT) {
    const pid = queue.shift()!;
    if (seen.has(pid)) continue;
    seen.add(pid);
    const value = member(pid);
    if (!value) continue;
    found.push(value);
    queue.push(...childrenOf(pid));
  }
  return { members: found, truncated: queue.some(pid => !seen.has(pid)) };
};

/** The legacy host-wide scan: every process on the host. Only used as the
 * fallback, and to confirm emptiness or adopt reparented orphans after churn. */
const everyProcess = (): Member[] => fs.readdirSync("/proc").flatMap(entry => {
  if (!/^\d+$/.test(entry)) return [];
  const value = member(Number(entry));
  return value ? [value] : [];
});

/** A group number is never ownership. Keep same-birth anchors while the native
 * leader lives, then retain surviving anchors until the entire group is empty.
 * An unobserved/recycled group is an unresolved obligation, not signal authority. */
export const executionGroup = (child: ChildProcess) => {
  // A Windows native child handle is not an owned execution-tree boundary.
  // Windows keeps its legacy native-child behavior outside this receipt API.
  if (process.platform === "win32") throw new Error("Windows execution-tree custody is unsupported");
  const pid = child.pid;
  const owned = new Map<number, string>();
  let previous = new Set<number>();
  let empty = false;
  let closed = false;
  child.once("close", () => { closed = true; });
  if (process.platform === "linux" && pid) {
    try {
      const leader = member(pid);
      if (leader) owned.set(pid, leader.started);
    } catch { /* Retain an unresolved obligation; never escape after spawn. */ }
  }
  const inGroup = (values: Member[]): Member[] =>
    values.filter(value => value.group === pid && !["Z", "X"].includes(value.state));
  // The leader is this process's own child, so "exit" fires only after we reap
  // it. Every process still in group <pid> then is ours: Linux keeps a pid number
  // allocated while any process uses it as a process-group id, so it cannot have
  // been recycled. Adopt them now, once; with a 2 s observer a short-lived leader
  // can exit before any tick saw descendants that are now reparented out of its
  // tree (the old 100 ms full scan only narrowed that window).
  if (process.platform === "linux" && pid) child.once("exit", () => {
    if (empty) return;
    try {
      const current = inGroup(everyProcess());
      for (const value of current) if (!owned.has(value.pid)) owned.set(value.pid, value.started);
      previous = new Set(current.map(value => value.pid));
    } catch { /* Unreadable identity stays an unresolved obligation. */ }
  });
  const members = (): Member[] => {
    if (empty || !pid) return [];
    let current: Member[];
    if (!descendantWalkAvailable()) current = inGroup(everyProcess());
    else {
      // Walk the leader's tree plus every member seen last time, so an orphan
      // already recorded stays tracked after reparenting out of that tree.
      const walk = descendants([pid, ...previous]);
      current = inGroup(walk.members);
      // A vanished member may have orphaned unrecorded children, an empty walk
      // is about to latch, and a walk cut off at WALK_LIMIT omitted members:
      // only then confirm with one host-wide scan. Never trust a partial walk.
      const now = new Set(current.map(value => value.pid));
      if (walk.truncated || !current.length || [...previous].some(value => !now.has(value))) current = inGroup(everyProcess());
    }
    previous = new Set(current.map(value => value.pid));
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
    observe(): void { if (process.platform === "linux") members(); },
    /** Current live member pids (Linux); [] elsewhere. */
    members(): number[] { return process.platform === "linux" ? members().map(value => value.pid) : []; },
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

/** Unref'd background observer: adopts births between on-demand refreshes. */
export const observeExecutionGroup = (group: { observe(): void }): ReturnType<typeof setInterval> => {
  const timer = setInterval(() => { try { group.observe(); } catch { /* cleanup fails closed */ } }, EXECUTION_GROUP_OBSERVE_MS);
  timer.unref();
  return timer;
};
