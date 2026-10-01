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
    observe(): void { if (process.platform === "linux") members(); },
    exited(): boolean {
      if (!pid) return closed;
      if (process.platform === "linux") return members().length === 0;
      // Windows has no detached POSIX group; native close is its receipt.
      if (process.platform === "win32") return closed;
      // Portable POSIX: pipes closing is not proof the detached group emptied.
      // Never signal a leaderless group without birth-safe ownership.
      if (empty) return true;
      try { process.kill(-pid, 0); return false; }
      catch (error) { if ((error as NodeJS.ErrnoException).code === "ESRCH") { empty = true; return true; } throw error; }
    },
    signal(signal: NodeJS.Signals): void {
      if (!pid) return;
      if (process.platform === "win32") {
        if (child.exitCode === null && child.signalCode === null) child.kill(signal);
        return;
      }
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
