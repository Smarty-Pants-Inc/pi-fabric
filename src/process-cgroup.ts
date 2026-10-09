import fs from "node:fs";
import path from "node:path";

export const CUSTODY_POLL_MS = 1_000;
export const FREEZE_TIMEOUT_MS = 1_000;
const ROOT = "/sys/fs/cgroup";
const gone = (error: unknown): boolean => ["ENOENT", "ESRCH"].includes((error as NodeJS.ErrnoException).code ?? "");
let warnedLegacySignal = false;

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
const processCgroupPath = (pid: number): string | undefined => {
  try { return cgroupPath(fs.readFileSync(`/proc/${pid}/cgroup`, "utf8")); }
  catch (error) { if (gone(error)) return undefined; throw error; }
};
export const processScopePath = (pid: number, unit?: string): string | undefined => {
  if (process.platform !== "linux") return undefined;
  try { return scopePath(fs.readFileSync(`/proc/${pid}/cgroup`, "utf8"), unit); }
  catch (error) { if (gone(error)) return undefined; throw error; }
};
export type ExecutionIdentity = { pid: number; parent: number; group: number; session: number; started: string };
export const executionIdentity = (pid: number): ExecutionIdentity | undefined => {
  try {
    const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
    const fields = stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/);
    if (["Z", "X"].includes(fields[0]!)) return undefined;
    return { pid, parent: Number(fields[1]), group: Number(fields[2]), session: Number(fields[3]), started: fields[19]! };
  } catch (error) { if (gone(error)) return undefined; throw error; }
};
const birth = (pid: number): string | undefined => executionIdentity(pid)?.started;

/** Kernel membership owns orphans. All file IO uses a pinned directory fd, not
 * a name that another same-UID process could replace between check and open. */
export interface CgroupCustody {
  directory: string;
  execution: ExecutionIdentity | undefined;
  closed: Promise<void>;
  watching: boolean;
  members(): number[];
  exited(): boolean;
  signal(value: NodeJS.Signals, selected?: ReadonlyMap<number, string>): Promise<void>;
  detectEscapes(): void;
  dispose(): void;
  waitForExit(ms: number): Promise<boolean>;
}
const pinCgroup = (directory: string, execution?: ExecutionIdentity, selectedOnly = false): CgroupCustody => {
  if (!directory.startsWith(`${ROOT}/`) || path.normalize(directory) !== directory || (!selectedOnly && !directory.endsWith(".scope"))) {
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
  const ownMembers = (): number[] => {
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
  const escapes = new Map<string, { receipt: CgroupCustody; targets: Map<number, string> }>();
  let scanned = false;
  const detectEscapes = (): void => {
    if (scanned || !execution) return;
    const index = directory.indexOf("/app.slice/");
    const parts = directory.split("/");
    const manager = parts.findIndex(part => /^user@[0-9]+[.]service$/.test(part));
    const app = index >= 0 ? directory.slice(0, index + "/app.slice/".length)
      : manager >= 0 ? `${parts.slice(0, manager + 1).join("/")}/app.slice/` : undefined;
    if (!app) { scanned = true; return; }
    // ponytail: same UID can write sibling cgroups; permissions cannot contain
    // it. Stop-only sampling catches cooperative re-homing, not hostile code.
    // Named residual (#7248): changing BOTH session/group AND cgroup after
    // reparenting escapes these anchors. Landlock/separate UID is the boundary.
    const candidates = fs.readdirSync("/proc").flatMap(entry => {
      if (!/^\d+$/.test(entry)) return [];
      const pid = Number(entry);
      const scope = processCgroupPath(pid);
      if (!scope?.startsWith(app)) return [];
      const identity = executionIdentity(pid); // stat only app.slice candidates
      return identity && BigInt(identity.started) >= BigInt(execution.started) ? [{ ...identity, scope }] : [];
    });
    const leader = candidates.find(value => value.pid === execution.pid) ?? executionIdentity(execution.pid);
    const anchorsValid = !leader || leader.started === execution.started;
    const descendants = new Set<number>(leader?.started === execution.started ? [execution.pid] : []);
    let changed = true;
    while (changed) {
      changed = false;
      for (const value of candidates) if (descendants.has(value.parent) && !descendants.has(value.pid)) { descendants.add(value.pid); changed = true; }
    }
    for (const value of candidates) {
      if (value.scope === directory || !(descendants.has(value.pid) || (anchorsValid && (value.session === execution.session || value.group === execution.group)))) continue;
      let retained = escapes.get(value.scope);
      if (!retained) {
        retained = { receipt: pinCgroup(value.scope, undefined, true), targets: new Map() };
        escapes.set(value.scope, retained);
        void retained.receipt.closed.then(notify);
      }
      retained.targets.set(value.pid, value.started);
    }
    scanned = true;
  };
  const members = (): number[] => {
    const pids = ownMembers();
    for (const [scope, escaped] of escapes) {
      const current = escaped.receipt.members().filter(pid => escaped.targets.has(pid) && escaped.targets.get(pid) === birth(pid));
      pids.push(...current);
      if (!current.length) { escaped.receipt.dispose(); escapes.delete(scope); }
    }
    return pids;
  };
  const individualSignal = (signal: NodeJS.Signals, selected?: ReadonlyMap<number, string>): void => {
    const targets = ownMembers().map(pid => ({ pid, started: selected?.get(pid) ?? (selected ? undefined : birth(pid)) }));
    for (const target of targets) {
      if (!target.started || birth(target.pid) !== target.started || processCgroupPath(target.pid) !== directory) continue;
      try { process.kill(target.pid, signal); }
      catch (error) { if (!gone(error)) throw error; }
    }
  };
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
  const frozenSignal = async (signal: NodeJS.Signals, selected?: ReadonlyMap<number, string>): Promise<void> => {
    let thaw = false;
    try {
      try { thaw = true; fs.writeFileSync(file("cgroup.freeze"), "1"); }
      catch (error) {
        if (!gone(error)) throw error;
        thaw = false;
        if (!ownMembers().length) return;
        if (!warnedLegacySignal) {
          warnedLegacySignal = true;
          console.warn("[pi-fabric] cgroup.freeze unavailable (kernel < 5.2); birth-checked signals retain a non-atomic PID-reuse race");
        }
        individualSignal(signal, selected); return;
      }
      if (!await waitFrozen()) return;
      // Frozen members cannot exit/reuse their PID between this list and kill.
      // Node has no pidfd_send_signal; the freezer is our atomic-signal answer.
      for (const pid of ownMembers()) {
        if (selected && (!selected.has(pid) || selected.get(pid) !== birth(pid))) continue;
        try { process.kill(pid, signal); }
        catch (error) { if (!gone(error)) throw error; }
      }
    } finally {
      if (thaw) {
        try { fs.writeFileSync(file("cgroup.freeze"), "0"); }
        catch (error) { if (!gone(error)) throw error; }
      }
    }
  };
  let signalling = Promise.resolve();
  const signal = (value: NodeJS.Signals, selected?: ReadonlyMap<number, string>): Promise<void> => {
    const run = async (): Promise<void> => {
      if (selectedOnly && !selected) throw new Error("Escaped cgroup requires selected birth identities");
      detectEscapes();
      for (const escaped of escapes.values()) await escaped.receipt.signal(value, escaped.targets);
      if (empty) return;
      active++;
      try {
        if (value === "SIGKILL" && !selected) {
          try { fs.writeFileSync(file("cgroup.kill"), "1"); return; }
          catch (error) { if (!gone(error)) throw error; }
        }
        await frozenSignal(value, selected);
      } finally { active--; if (empty) closeFd(); }
    };
    const pending = signalling.then(run);
    signalling = pending.catch(() => {}); // serialize thaw before any next signal
    return pending;
  };
  return {
    directory, execution, closed, members, signal, detectEscapes,
    dispose(): void { watcher?.close(); watcher = undefined; closeFd(); },
    get watching(): boolean { return watcher !== undefined; },
    exited: (): boolean => members().length === 0,
    async waitForExit(ms: number): Promise<boolean> {
      const deadline = Date.now() + ms;
      while (true) {
        if (!members().length) return true;
        const remaining = deadline - Date.now();
        if (remaining <= 0) return false;
        let timer: ReturnType<typeof setTimeout> | undefined;
        let wake!: () => void;
        try {
          await new Promise<void>(resolve => {
            wake = resolve; notifications.add(wake);
            timer = setTimeout(resolve, Math.min(remaining, CUSTODY_POLL_MS));
          });
        } finally { clearTimeout(timer); notifications.delete(wake); }
      }
    },
  };
};
export const cgroupCustody = (directory: string, execution?: ExecutionIdentity): CgroupCustody => pinCgroup(directory, execution);
export const executionCgroups = new WeakMap<object, CgroupCustody>();
