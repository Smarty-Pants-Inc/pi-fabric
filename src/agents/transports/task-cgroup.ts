import fs from "node:fs";

/**
 * Capped Linux task scopes (smarty-dev#8257). A process task that requests
 * `memoryMaxBytes` runs in its own `fabric-task-<unique>.scope` with
 * MemoryMax/MemorySwapMax=0. Its verified cgroup-v2 leaf is the cap and kill
 * boundary: changing a process session or group does not leave it. The native
 * bash hook routes supported nested systemd-run scopes through an in-place shim.
 *
 * ponytail: this is a resource and cleanup boundary, NOT an OS sandbox against
 * arbitrary same-UID code. Supported agent tools reach nested `systemd-run`
 * only through the task-local PATH wrapper (task-scope-wrapper.ts), which keeps
 * the nested command inside the already applied cap. No systemd unit scans: only the owned captured leaf
 * (retained directory descriptor) is ever read, killed or signalled.
 */
export const TASK_SCOPE_PREFIX = "fabric-task-";
const MAX_NESTED_CGROUPS = 4096;
/** Kernel stores memory.max rounded down to its page size (64 KiB at most). */
const PAGE_TOLERANCE = 65_536;

/** Bounded verified-boundary stop ended with members still present. */
export class TaskCgroupStopUnconfirmedError extends Error {
  readonly code = "TASK_CGROUP_STOP_UNCONFIRMED" as const;
  constructor(readonly cgroup: string, readonly pids: readonly number[], reason: string) {
    super(`Task cgroup ${cgroup} stop unconfirmed (${reason}); live pids: ${pids.length ? pids.join(", ") : "unknown"}`);
    this.name = "TaskCgroupStopUnconfirmedError";
  }
}

/** A launched capped scope could not prove its boundary/cap; its worker never ran and is not replayed uncapped. */
export class TaskScopeAdmissionError extends Error {
  readonly code = "TASK_SCOPE_ADMISSION_FAILED" as const;
  constructor(readonly unit: string, reason: string) {
    super(`Capped task scope admission failed (${unit}): ${reason}`);
    this.name = "TaskScopeAdmissionError";
  }
}

export const assertMemoryMaxBytes = (value: unknown): number => {
  // Host/config contract: any positive safe integer. A cap the kernel rounds to 0 (or too
  // small to admit the shim) fails gated admission; the worker is never executed.
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`Task scope memoryMaxBytes must be a positive safe integer, got ${String(value)}`);
  }
  return value;
};

/** Environment only the released capped shim may set (stripped from the launch environment). */
export const TASK_SCOPE_ENVIRONMENT = ["FABRIC_TASK_MEMORY_MAX", "FABRIC_TASK_CGROUP", "FABRIC_TASK_SYSTEMD_RUN_WRAPPER"] as const;

export const taskScopeProperties = (bytes: number): string[] =>
  ["-p", `MemoryMax=${bytes}`, "-p", "MemorySwapMax=0", "-p", "Delegate=yes"];

/**
 * Admission shim for capped scopes. It records its own cgroup receipt and the
 * marker, then blocks on the inherited gate pipe (no sleep/poll). Only an
 * explicit parent release exports FABRIC_TASK_* and execs the worker; EOF,
 * parent death or any other line exits 125 before the worker is executed.
 */
export const taskScopeShim = (gateFd: number): string => [
  'while IFS= read -r line; do printf "%s\\n" "$line"; done < /proc/self/cgroup > "$1.cgroup" || exit 125',
  'printf "admitted %s" "$2" > "$1" || exit 125',
  // systemd-run expands ${NAME} in its command line: this script must never use braces.
  // Gate line: release <memory.max> <cgroup> <wrapper bin dir (last: may contain spaces)>.
  `IFS=" " read -r verb FABRIC_TASK_MEMORY_MAX FABRIC_TASK_CGROUP wrapperbin <&${gateFd} || exit 125`,
  `exec ${gateFd}<&-`,
  '[ "$verb" = release ] && [ -n "$FABRIC_TASK_MEMORY_MAX" ] && [ -n "$FABRIC_TASK_CGROUP" ] && [ -x "$wrapperbin/systemd-run" ] || exit 125',
  // Task-local systemd-run wrapper first on PATH: nested scopes run inside this capped boundary.
  'PATH="$wrapperbin:$PATH"',
  'FABRIC_TASK_SYSTEMD_RUN_WRAPPER="$wrapperbin/systemd-run"',
  'export FABRIC_TASK_MEMORY_MAX FABRIC_TASK_CGROUP FABRIC_TASK_SYSTEMD_RUN_WRAPPER PATH',
  'shift 2',
  'exec "$@"',
].join("\n");

const errno = (error: unknown): string | undefined => (error as NodeJS.ErrnoException | undefined)?.code;
const gone = (error: unknown): boolean => errno(error) === "ENOENT" || errno(error) === "ENODEV" || errno(error) === "ESRCH";

/** An owned cgroup leaf addressed only through its retained directory descriptor. */
export interface TaskCgroupBoundary {
  /** Unified (v2) cgroup path from the verified receipt, e.g. /user.slice/.../fabric-task-x.scope. */
  readonly relative: string;
  /** Retained O_DIRECTORY descriptor of the admitted leaf. */
  readonly fd: number;
  /** Effective kernel memory.max at admission. */
  readonly memoryMax: number;
}

const leaf = (boundary: TaskCgroupBoundary, ...parts: string[]): string =>
  [`/proc/self/fd/${boundary.fd}`, ...parts].join("/");

/** Verify configured limits through the retained descriptor; undefined when unproven. */
export const verifiedTaskMemoryMax = (fd: number, bytes: number): number | undefined => {
  const read = (file: string): string => fs.readFileSync(`/proc/self/fd/${fd}/${file}`, "utf8").trim();
  const max = read("memory.max");
  if (!/^\d+$/.test(max) || read("memory.swap.max") !== "0") return undefined;
  const actual = Number(max);
  return actual > 0 && actual <= bytes && bytes - actual < PAGE_TOLERANCE ? actual : undefined;
};

/** All pids in the leaf and every nested cgroup; undefined once the leaf was removed. */
export const taskCgroupPids = (boundary: TaskCgroupBoundary): number[] | undefined => {
  const pids: number[] = [];
  const pending: string[][] = [[]];
  let visited = 0;
  while (pending.length) {
    const parts = pending.pop()!;
    if (++visited > MAX_NESTED_CGROUPS) throw new Error(`Task cgroup ${boundary.relative} has too many nested cgroups`);
    let procs: string;
    let entries: fs.Dirent[];
    try {
      procs = fs.readFileSync(leaf(boundary, ...parts, "cgroup.procs"), "utf8");
      entries = fs.readdirSync(leaf(boundary, ...parts), { withFileTypes: true });
    } catch (error) {
      if (!gone(error)) throw error;
      if (parts.length === 0) return undefined; // removed leaf: v2 cannot rmdir a populated cgroup
      continue; // a nested cgroup emptied and was removed concurrently
    }
    for (const line of procs.split("\n")) if (/^\d+$/.test(line)) pids.push(Number(line));
    for (const entry of entries) if (entry.isDirectory()) pending.push([...parts, entry.name]);
  }
  return [...new Set(pids)];
};

const processStart = (pid: number): string | undefined => {
  try {
    const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
    return stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/)[19];
  } catch (error) { if (gone(error)) return undefined; throw error; }
};

const insideBoundary = (pid: number, relative: string): boolean => {
  try {
    const unified = fs.readFileSync(`/proc/${pid}/cgroup`, "utf8").split("\n").find(line => line.startsWith("0::"))?.slice(3);
    return unified === relative || Boolean(unified?.startsWith(`${relative}/`));
  } catch (error) { if (gone(error)) return false; throw error; }
};

/** Older-kernel fallback: signal each member only while its birth and live membership hold. */
const signalMembers = (boundary: TaskCgroupBoundary, pids: readonly number[], signal: NodeJS.Signals): void => {
  for (const pid of pids) {
    const started = processStart(pid);
    if (started === undefined || !insideBoundary(pid, boundary.relative)) continue;
    // Re-read birth right before signalling: a recycled pid has another start time.
    if (processStart(pid) !== started || !insideBoundary(pid, boundary.relative)) continue;
    try { process.kill(pid, signal); } catch {
      // An unsignalable member must not shield its killable siblings. Continue the
      // bounded sweep; final subtree membership, not signal success, proves stop.
    }
  }
};

/** Write cgroup.kill through the retained leaf. false: unsupported kernel; undefined: leaf removed. */
const writeKill = (boundary: TaskCgroupBoundary): boolean | undefined => {
  let fd: number;
  try { fd = fs.openSync(leaf(boundary, "cgroup.kill"), fs.constants.O_WRONLY | fs.constants.O_NOFOLLOW); }
  catch (error) {
    if (!gone(error)) throw error;
    return taskCgroupPids(boundary) === undefined ? undefined : false;
  }
  try { fs.writeSync(fd, "1"); return true; }
  catch (error) { if (gone(error)) return undefined; throw error; }
  finally { fs.closeSync(fd); }
};

/**
 * Kill the whole owned leaf and confirm every nested cgroup.procs is empty
 * within bounded deadlines. Throws TaskCgroupStopUnconfirmedError naming the
 * remaining pids and cgroup path.
 */
export const stopTaskCgroup = async (
  boundary: TaskCgroupBoundary,
  options: { termMs: number; killMs: number; delay: () => Promise<void> },
): Promise<"cgroup.kill" | "signals" | "removed"> => {
  try { return await stopTaskCgroupOnce(boundary, options); }
  catch (error) {
    if (error instanceof TaskCgroupStopUnconfirmedError) throw error;
    // EPERM signalling, unreadable membership, etc.: typed failure naming what remains.
    let pids: number[] = [];
    try { pids = taskCgroupPids(boundary) ?? []; } catch { /* name no pids rather than guess */ }
    throw new TaskCgroupStopUnconfirmedError(boundary.relative, pids, `observation/signal failed: ${errno(error) ?? String(error)}`);
  }
};

const stopTaskCgroupOnce = async (
  boundary: TaskCgroupBoundary,
  options: { termMs: number; killMs: number; delay: () => Promise<void> },
): Promise<"cgroup.kill" | "signals" | "removed"> => {
  const confirm = async (ms: number, again: () => void): Promise<number[]> => {
    const deadline = Date.now() + ms;
    for (;;) {
      const pids = taskCgroupPids(boundary);
      if (!pids || pids.length === 0) return [];
      if (Date.now() >= deadline) return pids;
      again();
      await options.delay();
    }
  };
  const fail = (pids: number[], reason: string): never => { throw new TaskCgroupStopUnconfirmedError(boundary.relative, pids, reason); };
  let killed: boolean | undefined;
  try { killed = writeKill(boundary); }
  catch (error) { fail(taskCgroupPids(boundary) ?? [], `cgroup.kill failed: ${errno(error) ?? String(error)}`); }
  if (killed === undefined) return "removed";
  if (killed) {
    const remaining = await confirm(options.killMs, () => { writeKill(boundary); });
    if (remaining.length) fail(remaining, "cgroup.kill did not empty the subtree");
    return "cgroup.kill";
  }
  const initial = taskCgroupPids(boundary);
  if (!initial) return "removed";
  signalMembers(boundary, initial, "SIGTERM");
  let remaining = await confirm(options.termMs, () => undefined);
  if (remaining.length === 0) return "signals";
  remaining = await confirm(options.killMs, () => signalMembers(boundary, taskCgroupPids(boundary) ?? [], "SIGKILL"));
  if (remaining.length) fail(remaining, "SIGTERM/SIGKILL did not empty the subtree");
  return "signals";
};
