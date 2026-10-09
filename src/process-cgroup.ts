import fs from "node:fs";
import path from "node:path";

export const CUSTODY_POLL_MS = 1_000;
const ROOT = "/sys/fs/cgroup";
const gone = (error: unknown): boolean => ["ENOENT", "ESRCH"].includes((error as NodeJS.ErrnoException).code ?? "");

/** Only a dedicated v2 scope is an execution boundary, never our inherited cgroup. */
export const scopePath = (record: string, unit?: string): string | undefined => {
  const relative = record.split("\n").find(line => line.startsWith("0::/"))?.slice(3);
  if (!relative || relative.split("/").includes("..")) return undefined;
  const name = path.posix.basename(relative);
  if (unit ? name !== unit : !name.endsWith(".scope")) return undefined;
  return path.join(ROOT, relative);
};
export const processScopePath = (pid: number, unit?: string): string | undefined => {
  if (process.platform !== "linux") return undefined;
  try { return scopePath(fs.readFileSync(`/proc/${pid}/cgroup`, "utf8"), unit); }
  catch (error) { if (gone(error)) return undefined; throw error; }
};
const birth = (pid: number): string | undefined => {
  try {
    const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
    const fields = stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/);
    return ["Z", "X"].includes(fields[0]!) ? undefined : fields[19];
  } catch (error) { if (gone(error)) return undefined; throw error; }
};

/** Kernel membership owns orphaned/setsid children; pid birth owns individual signals.
 * One events watcher is shared by observation, drain and all waiters on this receipt.
 * Pin the directory inode so a reused unit name can never become signal authority. */
export const cgroupCustody = (directory: string) => {
  if (!directory.startsWith(`${ROOT}/`) || path.normalize(directory) !== directory || !directory.endsWith(".scope")) {
    throw new Error(`Invalid execution cgroup ${directory}`);
  }
  let identity: fs.Stats | undefined;
  let empty = false;
  let watcher: fs.FSWatcher | undefined;
  let resolveEmpty!: () => void;
  const closed = new Promise<void>(resolve => { resolveEmpty = resolve; });
  const markEmpty = (): void => { empty = true; watcher?.close(); watcher = undefined; resolveEmpty(); };
  try { identity = fs.statSync(directory); }
  catch (error) { if (gone(error)) markEmpty(); else throw error; }
  const same = (): boolean => {
    if (empty) return false;
    try {
      const now = fs.statSync(directory);
      if (now.dev !== identity?.dev || now.ino !== identity.ino) throw new Error(`Execution cgroup ${directory} identity changed; exit unconfirmed`);
      return true;
    } catch (error) { if (gone(error)) { markEmpty(); return false; } throw error; }
  };
  const members = (): number[] => {
    if (!same()) return [];
    try {
      const text = fs.readFileSync(path.join(directory, "cgroup.procs"), "utf8");
      const pids = text.trim() ? text.trim().split(/\s+/).map(Number) : [];
      if (pids.some(pid => !Number.isSafeInteger(pid) || pid <= 0)) throw new Error(`Invalid membership in ${directory}`);
      if (!pids.length) markEmpty();
      return pids;
    } catch (error) { if (gone(error)) { markEmpty(); return []; } throw error; }
  };
  const observeEvents = (): void => {
    if (!same()) return;
    try {
      if (/^populated 0$/m.test(fs.readFileSync(path.join(directory, "cgroup.events"), "utf8"))) markEmpty();
    } catch (error) { if (gone(error)) markEmpty(); else throw error; }
  };
  if (!empty) {
    try {
      watcher = fs.watch(path.join(directory, "cgroup.events"), { persistent: false }, () => {
        try { observeEvents(); } catch { watcher?.close(); watcher = undefined; /* unreadable is not exited */ }
      });
      watcher.on("error", () => { watcher?.close(); watcher = undefined; });
      observeEvents(); // close the watch/read race
    } catch { watcher?.close(); watcher = undefined; /* 1s bounded fallback */ }
  }
  const individualSignal = (signal: NodeJS.Signals): void => {
    const targets = members().map(pid => ({ pid, started: birth(pid) }));
    for (const target of targets) {
      if (!target.started || birth(target.pid) !== target.started || processScopePath(target.pid) !== directory) continue;
      try { process.kill(target.pid, signal); }
      catch (error) { if (!gone(error)) throw error; }
    }
  };
  return {
    directory, closed, members,
    get watching(): boolean { return watcher !== undefined; },
    exited: (): boolean => members().length === 0,
    signal(signal: NodeJS.Signals): void {
      if (!same()) return;
      if (signal !== "SIGKILL") { individualSignal(signal); return; }
      try { fs.writeFileSync(path.join(directory, "cgroup.kill"), "1"); }
      catch (error) {
        if (!gone(error)) throw error;
        if (!same()) return;
        // Older v2 kernels lack cgroup.kill; never fall back to an unowned pgid.
        console.warn(`[pi-fabric] ${directory}/cgroup.kill unavailable; using birth-checked individual KILL`);
        individualSignal(signal);
      }
    },
    async waitForExit(ms: number): Promise<boolean> {
      const deadline = Date.now() + ms;
      while (!empty) {
        if (!members().length) return true;
        const remaining = deadline - Date.now();
        if (remaining <= 0) return false;
        let timer: ReturnType<typeof setTimeout> | undefined;
        try {
          await Promise.race([closed, new Promise<void>(resolve => {
            timer = setTimeout(resolve, Math.min(remaining, CUSTODY_POLL_MS));
          })]);
        } finally { clearTimeout(timer); }
      }
      return true;
    },
  };
};
export type CgroupCustody = ReturnType<typeof cgroupCustody>;
export const executionCgroups = new WeakMap<object, CgroupCustody>();
