import fs from "node:fs";
import path from "node:path";

const ROOT = "/sys/fs/cgroup";
const gone = (error: unknown): boolean => ["ENOENT", "ESRCH"].includes((error as NodeJS.ErrnoException).code ?? "");

/** Bus discovery is for the trusted launcher only, not its execution target. */
export const scopeLauncherEnvironment = (environment: NodeJS.ProcessEnv = process.env): NodeJS.ProcessEnv => {
  if (process.platform !== "linux" || environment.DBUS_SESSION_BUS_ADDRESS || environment.XDG_RUNTIME_DIR) return environment;
  const runtime = `/run/user/${process.getuid!()}`;
  return fs.existsSync(`${runtime}/bus`) ? { ...environment, XDG_RUNTIME_DIR: runtime } : environment;
};

/** Compute the exact placement from the user manager root, slice and unit chosen
 * BEFORE spawn. Neither a marker nor the launcher's later location chooses it. */
export const scopeDirectory = (root: string, slice: string, unit: string): string => {
  if (!root.startsWith("/") || root === "/" || path.posix.normalize(root) !== root || root.split("/").includes("..") ||
    !/^fabric-(?:execution|worker)-[0-9a-f-]+[.]scope$/.test(unit)) throw new Error("Invalid Fabric scope placement");
  if (slice === "-.slice") return path.join(ROOT, root, unit);
  if (!/^[A-Za-z0-9_.:]+(?:-[A-Za-z0-9_.:]+)*[.]slice$/.test(slice)) throw new Error(`Unsupported execution slice ${slice}`);
  const parts = slice.slice(0, -6).split("-");
  return path.join(ROOT, root, ...parts.map((_, index) => `${parts.slice(0, index + 1).join("-")}.slice`), unit);
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
export const processScopePath = (pid: number): string | undefined => {
  try {
    const relative = fs.readFileSync(`/proc/${pid}/cgroup`, "utf8").split("\n").find(line => line.startsWith("0::/"))?.slice(3);
    return relative && path.posix.normalize(relative) === relative && !relative.split("/").includes("..") ? path.join(ROOT, relative) : undefined;
  } catch (error) { if (gone(error)) return undefined; throw error; }
};
export type ScopePin = { dev: number; ino: number; uid: number };
export interface CgroupCustody {
  directory: string;
  execution: ExecutionIdentity | undefined;
  pin: ScopePin;
  members(): number[];
  verify(execution: ExecutionIdentity): void;
  exited(): boolean;
  signal(value: NodeJS.Signals): Promise<void>;
  dispose(): void;
}

/** Membership custody uses only a pinned cgroup fd. Poll cadence is deliberately
 * unchanged in callers; event-driven liveness is a separate change. */
export const cgroupCustody = (directory: string, execution?: ExecutionIdentity, expected?: ScopePin): CgroupCustody => {
  if (!directory.startsWith(`${ROOT}/`) || path.normalize(directory) !== directory || directory.split("/").includes("..") || !directory.endsWith(".scope")) {
    throw new Error(`Invalid execution cgroup ${directory}`);
  }
  let fd: number | undefined;
  let empty = false;
  const dispose = (): void => { if (fd !== undefined) { fs.closeSync(fd); fd = undefined; } };
  let pin: ScopePin;
  try {
    const recorded = fs.lstatSync(directory);
    fd = fs.openSync(directory, fs.constants.O_RDONLY | fs.constants.O_DIRECTORY | fs.constants.O_NOFOLLOW);
    const pinned = fs.fstatSync(fd);
    pin = { dev: pinned.dev, ino: pinned.ino, uid: pinned.uid };
    if (!recorded.isDirectory() || recorded.uid !== process.getuid!() || pinned.uid !== recorded.uid || pinned.dev !== recorded.dev || pinned.ino !== recorded.ino ||
      (expected && (pin.dev !== expected.dev || pin.ino !== expected.ino || pin.uid !== expected.uid))) {
      throw new Error(`Execution cgroup ${directory} ownership/identity changed; exit unconfirmed`);
    }
    fs.accessSync(`/proc/self/fd/${fd}/cgroup.kill`, fs.constants.W_OK);
  } catch (error) { dispose(); throw error; }
  const file = (name: string): string => `/proc/self/fd/${fd}/${name}`;
  const exited = (): boolean => {
    if (empty) return true;
    try { empty = /^populated 0$/m.test(fs.readFileSync(file("cgroup.events"), "utf8")); }
    catch (error) { if (gone(error)) empty = true; else throw error; }
    if (empty) dispose();
    return empty;
  };
  const members = (): number[] => {
    if (empty) return [];
    const text = fs.readFileSync(file("cgroup.procs"), "utf8").trim();
    const pids = text ? text.split(/\s+/).map(Number) : [];
    if (pids.some(pid => !Number.isSafeInteger(pid) || pid <= 0)) throw new Error(`Invalid membership in ${directory}`);
    return pids;
  };
  const verify = (identity: ExecutionIdentity): void => {
    // Read exact placement and birth around membership verification. These are
    // admission checks, NEVER numeric signal authority. Migration cannot select
    // another scope; subsequent cleanup still targets this fd only.
    if (processScopePath(identity.pid) !== directory || executionIdentity(identity.pid)?.started !== identity.started ||
      !members().includes(identity.pid) || processScopePath(identity.pid) !== directory || executionIdentity(identity.pid)?.started !== identity.started) {
      throw new Error(`Launcher ${identity.pid} left its spawn scope or changed identity`);
    }
  };
  return {
    directory, execution, pin, members, verify, exited, dispose,
    async signal(value): Promise<void> {
      if (empty || value !== "SIGKILL") return;
      // ponytail: Node/Bun expose no pidfd_send_signal. Same-UID code can move
      // even a frozen member, exit and reuse its PID after ANY /proc check; a
      // PGID can also retain migrated members. Skip cooperative TERM entirely.
      // Callers retain today's grace period, then cgroup.kill atomically targets
      // only current recursive members. Trade-off: no target OS TERM handlers.
      try { fs.writeFileSync(file("cgroup.kill"), "1"); }
      catch (error) { if (gone(error) && exited()) return; throw error; }
    },
  };
};
export const executionCgroups = new WeakMap<object, CgroupCustody>();
