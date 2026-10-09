import fs from "node:fs";
import path from "node:path";

export const CUSTODY_POLL_MS = 1_000;
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
    empty = true; watcher?.close(); watcher = undefined; closeFd(); resolveEmpty(); notify();
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
      // Require atomic membership-only KILL; never substitute numeric PIDs.
      fs.accessSync(file("cgroup.kill"), fs.constants.W_OK);
    } catch (error) { closeFd(); throw new Error(`Execution cgroup ${directory} controls unavailable: ${String(error)}`); }
  }
  const members = (): number[] => {
    if (empty) return [];
    try {
      const text = fs.readFileSync(file("cgroup.procs"), "utf8");
      const pids = text.trim() ? text.trim().split(/\s+/).map(Number) : [];
      if (pids.some(pid => !Number.isSafeInteger(pid) || pid <= 0)) throw new Error(`Invalid membership in ${directory}`);
      // Empty direct membership is not empty recursive membership; only
      // cgroup.events populated 0 (or a removed pinned scope) confirms exit.
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
    } catch { watcher?.close(); watcher = undefined; /* fail closed; one safety read at the drain deadline, never polling */ }
  }
  let signalling = Promise.resolve();
  const signal = (value: NodeJS.Signals): Promise<void> => {
    const run = async (): Promise<void> => {
      if (empty || value !== "SIGKILL") return;
      // ponytail: Node/Bun expose no pidfd_send_signal. A frozen member can be
      // moved by same-UID code, exit, and have its PID reused after ANY /proc
      // check. Even our recorded PGID can include a migrated, out-of-scope
      // process. Neither is scope-only signal authority: skip TERM (and other
      // non-KILL signals). Callers retain the grace deadline, then cgroup.kill
      // atomically targets current members. Trade-off: no cooperative TERM.
      active++;
      try {
        try { fs.writeFileSync(file("cgroup.kill"), "1"); }
        catch (error) {
          if (gone(error)) { observeEvents(); if (empty) return; }
          throw error;
        }
      } finally { active--; if (empty) closeFd(); }
    };
    const pending = signalling.then(run);
    signalling = pending.catch(() => {}); // a failed write must not poison retry
    return pending;
  };
  return {
    directory, execution, closed, members, signal,
    dispose(): void { watcher?.close(); watcher = undefined; closeFd(); },
    get watching(): boolean { return watcher !== undefined; },
    exited: (): boolean => empty,
    waitForExit(ms: number): Promise<boolean> {
      if (empty) return Promise.resolve(true);
      return new Promise(resolve => {
        let timer: ReturnType<typeof setTimeout> | undefined;
        const finish = (exited: boolean): void => {
          clearTimeout(timer); notifications.delete(wake); resolve(exited);
        };
        const wake = (): void => { if (empty) finish(true); };
        notifications.add(wake);
        // One deadline, shared populated-0 events, no periodic membership read.
        // A failed watcher retains custody until this deadline. One safety read
        // may confirm a missed empty/removal event; unknown is false so callers
        // escalate to pinned cgroup.kill rather than polling or releasing debt.
        timer = setTimeout(() => {
          try { observeEvents(); } catch { /* unreadable is not exited */ }
          finish(empty);
        }, Math.max(0, ms));
        wake();
      });
    },
  };
};
export const cgroupCustody = (directory: string, execution?: ExecutionIdentity): CgroupCustody => pinCgroup(directory, execution);
export const executionCgroups = new WeakMap<object, CgroupCustody>();
