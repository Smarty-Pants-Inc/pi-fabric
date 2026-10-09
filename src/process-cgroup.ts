import fs from "node:fs";
import path from "node:path";

export const CUSTODY_POLL_MS = 1_000;
export const FREEZE_TIMEOUT_MS = 1_000;
const ROOT = "/sys/fs/cgroup";
const gone = (error: unknown): boolean => ["ENOENT", "ESRCH"].includes((error as NodeJS.ErrnoException).code ?? "");

/** Nested Fabric launches run inside a scrubbed execution too. Give only the
 * trusted custody launcher its standard local user-bus discovery, not targets.
 * This is convenience hardening, not a barrier to same-UID hostile code. */
export const scopeLauncherEnvironment = (environment: NodeJS.ProcessEnv = process.env): NodeJS.ProcessEnv => {
  if (process.platform !== "linux" || environment.DBUS_SESSION_BUS_ADDRESS || environment.XDG_RUNTIME_DIR) return environment;
  const runtime = `/run/user/${process.getuid!()}`;
  return fs.existsSync(`${runtime}/bus`) ? { ...environment, XDG_RUNTIME_DIR: runtime } : environment;
};
const cgroupPath = (record: string): string | undefined => {
  const relative = record.split("\n").find(line => line.startsWith("0::/"))?.slice(3);
  if (!relative || relative.split("/").includes("..")) return undefined;
  return path.join(ROOT, relative);
};
/** Only a dedicated v2 scope is an execution boundary, never our inherited cgroup. */
export const scopePath = (record: string, unit?: string): string | undefined => {
  const directory = cgroupPath(record);
  if (!directory) return undefined;
  const name = path.posix.basename(directory);
  return (unit ? name === unit : name.endsWith(".scope")) ? directory : undefined;
};
export const processScopePath = (pid: number, unit?: string): string | undefined => {
  if (process.platform !== "linux") return undefined;
  try { return scopePath(fs.readFileSync(`/proc/${pid}/cgroup`, "utf8"), unit); }
  catch (error) { if (gone(error)) return undefined; throw error; }
};
export type ExecutionIdentity = { pid: number; parent: number; group: number; session: number; started: string };
export type LinuxGroupMember = ExecutionIdentity & { state: string };
export const linuxGroupMember = (pid: number): LinuxGroupMember | undefined => {
  try {
    const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
    const fields = stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/);
    return { pid, parent: Number(fields[1]), group: Number(fields[2]), session: Number(fields[3]), started: fields[19]!, state: fields[0]! };
  } catch (error) { if (gone(error)) return undefined; throw error; }
};
export const executionIdentity = (pid: number): ExecutionIdentity | undefined => {
  const member = linuxGroupMember(pid);
  if (!member || ["Z", "X"].includes(member.state)) return undefined;
  const { state: _state, ...identity } = member;
  return identity;
};

/** Kernel membership owns orphans. All file IO uses a pinned directory fd, not
 * a name that another same-UID process could replace between check and open. */
export interface CgroupCustody {
  directory: string;
  execution: ExecutionIdentity | undefined;
  closed: Promise<void>;
  watching: boolean;
  members(): number[];
  exited(): boolean;
  signal(value: NodeJS.Signals): Promise<void>;
  dispose(): void;
  waitForExit(ms: number): Promise<boolean>;
}
const pinCgroup = (directory: string, execution?: ExecutionIdentity): CgroupCustody => {
  if (!directory.startsWith(`${ROOT}/`) || path.normalize(directory) !== directory || !directory.endsWith(".scope")) {
    throw new Error(`Invalid execution cgroup ${directory}`);
  }
  let fd: number | undefined;
  let empty = false, active = 0;
  let watcher: fs.FSWatcher | undefined;
  const notifications = new Set<() => void>();
  let resolveEmpty!: () => void;
  const closed = new Promise<void>(resolve => { resolveEmpty = resolve; });
  const closeFd = (): void => { if (fd !== undefined && active === 0) { fs.closeSync(fd); fd = undefined; } };
  const notify = (): void => { for (const wake of notifications) wake(); };
  const markEmpty = (): void => {
    empty = true; watcher?.close(); watcher = undefined; closeFd(); resolveEmpty();
  };
  const file = (name: string): string => `/proc/self/fd/${fd}/${name}`;
  try {
    const recorded = fs.statSync(directory);
    fd = fs.openSync(directory, fs.constants.O_RDONLY | fs.constants.O_DIRECTORY);
    const pinned = fs.fstatSync(fd);
    if (pinned.dev !== recorded.dev || pinned.ino !== recorded.ino) throw new Error(`Execution cgroup ${directory} identity changed; exit unconfirmed`);
  } catch (error) { closeFd(); if (gone(error)) markEmpty(); else throw error; }
  if (!empty) {
    try {
      // ponytail: require both controls up front; old kernels use the existing
      // legacy custodian, never an unfrozen per-PID fallback inside this receipt.
      for (const name of ["cgroup.freeze", "cgroup.kill"]) fs.accessSync(file(name), fs.constants.W_OK);
    } catch (error) { closeFd(); throw new Error(`Execution cgroup ${directory} controls unavailable: ${String(error)}`); }
  }
  const members = (): number[] => {
    if (empty) return [];
    try {
      const text = fs.readFileSync(file("cgroup.procs"), "utf8");
      const pids = text.trim() ? text.trim().split(/\s+/).map(Number) : [];
      if (pids.some(pid => !Number.isSafeInteger(pid) || pid <= 0)) throw new Error(`Invalid membership in ${directory}`);
      if (!pids.length) markEmpty();
      return pids;
    } catch (error) { if (gone(error)) { markEmpty(); return []; } throw error; }
  };
  const observeEvents = (): void => {
    if (empty) return;
    try { if (/^populated 0$/m.test(fs.readFileSync(file("cgroup.events"), "utf8"))) markEmpty(); }
    catch (error) { if (gone(error)) markEmpty(); else throw error; }
  };
  if (!empty) {
    try {
      watcher = fs.watch(file("cgroup.events"), { persistent: false }, () => {
        try { observeEvents(); } catch { watcher?.close(); watcher = undefined; /* unreadable is not exited */ }
        notify();
      });
      watcher.on("error", () => { watcher?.close(); watcher = undefined; notify(); });
      observeEvents(); // close the watch/read race
    } catch { watcher?.close(); watcher = undefined; /* 1s bounded exit fallback */ }
  }
  const waitFrozen = (): Promise<boolean> => new Promise((resolve, reject) => {
    const finish = (error?: unknown, frozen = false): void => {
      clearTimeout(timer); notifications.delete(inspect);
      if (error) reject(error); else resolve(frozen);
    };
    const inspect = (): void => {
      if (empty) { finish(); return; }
      try { if (/^frozen 1$/m.test(fs.readFileSync(file("cgroup.events"), "utf8"))) finish(undefined, true); }
      catch (error) { finish(error); }
    };
    const timer = setTimeout(() => finish(new Error(`Execution cgroup ${directory} freeze unconfirmed after ${FREEZE_TIMEOUT_MS}ms`)), FREEZE_TIMEOUT_MS);
    notifications.add(inspect); inspect();
  });
  const frozenSignal = async (signal: NodeJS.Signals): Promise<void> => {
    try {
      fs.writeFileSync(file("cgroup.freeze"), "1");
      if (!await waitFrozen()) return;
      // ponytail: cgroup.procs contains numeric PIDs, including zombies that an
      // outside parent can reap despite freezing. Bind live stat/start time ->
      // exact scope membership -> same live stat/start time before signalling.
      // A live process inside OUR FROZEN scope at the middle read cannot exit
      // until thaw, so its PID cannot be reused before kill; the two stat reads
      // bind that identity across membership validation. An already-reused
      // outside PID fails the exact path check even if its first stat is live.
      for (const pid of members()) {
        let member: LinuxGroupMember | undefined;
        try {
          member = linuxGroupMember(pid);
          if (!member?.started || ["Z", "X"].includes(member.state)) continue;
          if (processScopePath(pid) !== directory) continue;
          const current = linuxGroupMember(pid);
          if (!current || current.started !== member.started || ["Z", "X"].includes(current.state)) continue;
        } catch { continue; }
        try { process.kill(pid, signal); }
        catch (error) { if (!gone(error)) throw error; }
      }
    } finally {
      try { fs.writeFileSync(file("cgroup.freeze"), "0"); }
      catch (error) { if (!gone(error)) throw error; }
    }
  };
  let signalling = Promise.resolve();
  const signal = (value: NodeJS.Signals): Promise<void> => {
    const run = async (): Promise<void> => {
      if (empty) return;
      active++;
      try {
        if (value === "SIGKILL") {
          try { fs.writeFileSync(file("cgroup.kill"), "1"); return; }
          catch (error) { if (gone(error) && !members().length) return; throw error; }
        }
        await frozenSignal(value);
      } finally { active--; if (empty) closeFd(); }
    };
    const pending = signalling.then(run);
    signalling = pending.catch(() => {}); // serialize thaw before any next signal
    return pending;
  };
  return {
    directory, execution, closed, members, signal,
    dispose(): void { watcher?.close(); watcher = undefined; closeFd(); },
    get watching(): boolean { return watcher !== undefined; },
    exited: (): boolean => members().length === 0,
    async waitForExit(ms: number): Promise<boolean> {
      const deadline = Date.now() + ms;
      const safetyAt = Date.now() + 60_000;
      let safetyRead = false;
      if (!members().length) return true;
      while (true) {
        const remaining = deadline - Date.now();
        if (remaining <= 0) return false;
        // ponytail: R-no-polling exception: one >=60s safety membership read per
        // watched wait for a missed kernel event; deadline wakes do not re-read.
        const interval = watcher ? (safetyRead ? Infinity : Math.max(0, safetyAt - Date.now())) : CUSTODY_POLL_MS;
        let timer: ReturnType<typeof setTimeout> | undefined;
        let wake!: () => void;
        let timerWake = false;
        try {
          await new Promise<void>(resolve => {
            wake = resolve; notifications.add(wake);
            timer = setTimeout(() => { timerWake = true; resolve(); }, Math.min(remaining, interval));
          });
        } finally { clearTimeout(timer); notifications.delete(wake); }
        if (empty) return true;
        if (timerWake && watcher) {
          if (remaining < interval) return false;
          safetyRead = true;
        }
        if (!members().length) return true;
      }
    },
  };
};
export const cgroupCustody = (directory: string, execution?: ExecutionIdentity): CgroupCustody => pinCgroup(directory, execution);
export const executionCgroups = new WeakMap<object, CgroupCustody>();
