import fs from "node:fs";
import path from "node:path";
import type { ChildProcess } from "node:child_process";

/** Only an execution-specific cgroup can become empty while this worker lives.
 * A shared user/session scope (including this worker) is not a tree receipt. */
const executionCgroupEvents = (pid: number): string | undefined => {
  try {
    const scope = (file: string): string | undefined => fs.readFileSync(file, "utf8")
      .split("\n").find(line => line.startsWith("0::"))?.slice(3);
    const childScope = scope(`/proc/${pid}/cgroup`);
    if (!childScope || childScope === "/" || childScope === scope("/proc/self/cgroup")) return undefined;
    const root = "/sys/fs/cgroup";
    const directory = path.resolve(root, `.${childScope}`);
    if (!directory.startsWith(root + path.sep)) return undefined;
    const events = path.join(directory, "cgroup.events");
    return fs.existsSync(events) ? events : undefined;
  } catch { return undefined; }
};

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
  const forcedDescendants = new Set<string>();
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
    cgroupEventsFile: process.platform === "linux" && pid ? executionCgroupEvents(pid) : undefined,
    observe(): void { if (process.platform === "linux") members(); },
    /** Count every birth-checked descendant signalled, including closeChild escalation. */
    forcedCleanupCount: (): number => forcedDescendants.size,
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
      if (process.platform === "linux") {
        // The execution root itself is a descendant of the transport worker.
        for (const value of members()) forcedDescendants.add(`${value.pid}:${value.started}`);
      }
      try { process.kill(-pid, signal); }
      catch (error) { if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error; }
    },
  };
};
