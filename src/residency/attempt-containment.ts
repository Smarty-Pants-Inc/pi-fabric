import fs from "node:fs";
import path from "node:path";
import type { StdioOptions } from "node:child_process";
import type { TaskCgroupBoundary } from "../agents/transports/task-cgroup.js";
import { taskCgroupPids } from "../agents/transports/task-cgroup.js";
import { delegatedUserCgroupAvailable } from "../agents/transports/cgroup-eligibility.js";
import type { ResidentLaunchSpec } from "./handover.js";
import { launchSnapshotPath, writeLaunchSnapshot } from "./handover.js";

export type AttemptLaunchReceipt = { attemptId: string; scope: string; cgroupPath: string; launchedInScope: true; at: number };
export const assertResidentAttemptId = (attemptId: string): void => {
  if (!/^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$/.test(attemptId)) throw new ContainmentUnavailableError("invalid attempt identity", false);
};

export const assertFreshResidentAttemptId = (attemptId: string): void => {
  assertResidentAttemptId(attemptId);
  if (boundaries?.has(attemptId) || completedReceipts?.has(attemptId)) throw new ContainmentUnavailableError("attempt identity was already admitted", false);
};

export const residentAttestationPath = (residencyRoot: string, attemptId: string): string => {
  assertResidentAttemptId(attemptId);
  return path.join(residencyRoot, `attempt-launch-${attemptId}.json`);
};

/** Publish the exact lead record before releasing the gated resident shim. */
export const writeResidentAttestation = (residencyRoot: string, receipt: AttemptLaunchReceipt): string => {
  const file = residentAttestationPath(residencyRoot, receipt.attemptId);
  const bytes = Buffer.from(JSON.stringify(receipt));
  let fd: number | undefined;
  try {
    fd = fs.openSync(file, fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_NOFOLLOW, 0o600);
    let offset = 0;
    while (offset < bytes.length) offset += fs.writeSync(fd, bytes, offset, bytes.length - offset);
    fs.fsyncSync(fd);
    return file;
  } finally { if (fd !== undefined) fs.closeSync(fd); }
};
export type AttemptMembership = { attemptId: string; scope: string; pids: number[]; populated: boolean };
export type AttemptExitReceipt = { attemptId: string; scope: string; populated: false; at: number };

export class ContainmentUnavailableError extends Error {
  constructor(readonly reason: string, readonly attemptStarted = false) {
    super(`Resident attempt containment unavailable${attemptStarted ? " after scope launch" : " before launch"}: ${reason}`);
    this.name = "ContainmentUnavailableError";
  }
}
export class AttemptExitUnconfirmedError extends Error {
  readonly remainingPids: number[];
  constructor(input: { attemptId: string; remainingPids: number[] }) {
    super(`Resident attempt ${input.attemptId} exit unconfirmed; remaining pids: ${input.remainingPids.length ? input.remainingPids.join(", ") : "unknown"}`);
    this.name = "AttemptExitUnconfirmedError";
    this.attemptId = input.attemptId;
    this.remainingPids = input.remainingPids;
  }
  readonly attemptId: string;
}

export interface AttemptContainment {
  launchInScope(spec: ResidentLaunchSpec, attemptId: string): Promise<AttemptLaunchReceipt>;
  attemptMembership(attemptId: string): AttemptMembership;
  joinAttemptExit(attemptId: string, deadlineAtMs: number): Promise<AttemptExitReceipt>;
}
interface RetainedAttempt {
  boundary: TaskCgroupBoundary;
  eventsFd: number;
  eventsIdentity: { dev: number; ino: number };
  watcher: fs.FSWatcher;
  populated: boolean;
  emptyObservedAt?: number;
  failure?: unknown;
  waiters: Set<() => void>;
}
const boundaries = new Map<string, RetainedAttempt>();
const completedReceipts = new Map<string, { scope: string; receipt: AttemptExitReceipt }>();
const COMPLETED_RECEIPT_LIMIT = 128;
const rememberCompleted = (attemptId: string, scope: string, receipt: AttemptExitReceipt): void => {
  completedReceipts.delete(attemptId);
  completedReceipts.set(attemptId, { scope, receipt });
  while (completedReceipts.size > COMPLETED_RECEIPT_LIMIT) completedReceipts.delete(completedReceipts.keys().next().value!);
};
const remaining = (_id: string, boundary: TaskCgroupBoundary): number[] => {
  try { return taskCgroupPids(boundary) ?? []; } catch { return []; }
};

const observe = (attemptId: string, entry: RetainedAttempt): boolean => {
  try {
    const stat = fs.fstatSync(entry.eventsFd);
    if (stat.dev !== entry.eventsIdentity.dev || stat.ino !== entry.eventsIdentity.ino) throw new Error("retained cgroup.events identity changed");
    const buffer = Buffer.alloc(4096);
    const size = fs.readSync(entry.eventsFd, buffer, 0, buffer.length, 0);
    const value = buffer.toString("utf8", 0, size).match(/^populated ([01])$/m)?.[1];
    if (value === undefined) throw new Error("retained cgroup.events populated state is unreadable");
    entry.populated = value === "1";
    if (!entry.populated) entry.emptyObservedAt ??= Date.now();
    return entry.populated;
  } catch (error) {
    // systemd may collect an already-empty scope. ENODEV on this exact, opened
    // kernfs descriptor is a terminal empty receipt; ENOENT/path absence is not.
    if ((error as NodeJS.ErrnoException)?.code === "ENODEV") { entry.populated = false; entry.emptyObservedAt ??= Date.now(); return false; }
    entry.failure = error;
    throw new AttemptExitUnconfirmedError({ attemptId, remainingPids: remaining(attemptId, entry.boundary) });
  }
};

/** Launcher-only: duplicate and subscribe before the gated worker is released. */
export const retainAttemptBoundary = (attemptId: string, source: TaskCgroupBoundary): void => {
  assertFreshResidentAttemptId(attemptId);
  let leafFd: number | undefined, eventsFd: number | undefined, watcher: fs.FSWatcher | undefined;
  try {
    leafFd = fs.openSync(`/proc/self/fd/${source.fd}`, fs.constants.O_RDONLY | fs.constants.O_DIRECTORY);
    const held = fs.fstatSync(leafFd), original = fs.fstatSync(source.fd);
    if (!held.isDirectory() || held.dev !== original.dev || held.ino !== original.ino) throw new Error("attempt cgroup descriptor identity changed");
    const boundary = { ...source, fd: leafFd };
    eventsFd = fs.openSync(`/proc/self/fd/${leafFd}/cgroup.events`, fs.constants.O_RDONLY);
    const stat = fs.fstatSync(eventsFd), buffer = Buffer.alloc(4096);
    const size = fs.readSync(eventsFd, buffer, 0, buffer.length, 0);
    const populated = buffer.toString("utf8", 0, size).match(/^populated ([01])$/m)?.[1];
    if (populated === undefined) throw new Error("attempt cgroup.events has no populated receipt");
    const entry: RetainedAttempt = { boundary, eventsFd, eventsIdentity: { dev: stat.dev, ino: stat.ino },
      watcher: undefined as unknown as fs.FSWatcher, populated: populated === "1",
      ...(populated === "0" ? { emptyObservedAt: Date.now() } : {}), waiters: new Set() };
    watcher = fs.watch(`/proc/self/fd/${leafFd}/cgroup.events`, { persistent: false }, () => {
      try { observe(attemptId, entry); } catch { /* failure retained */ }
      for (const wake of entry.waiters) wake();
    });
    watcher.on("error", error => { entry.failure = error; for (const wake of entry.waiters) wake(); });
    entry.watcher = watcher;
    boundaries.set(attemptId, entry);
  } catch (error) {
    watcher?.close(); if (eventsFd !== undefined) fs.closeSync(eventsFd); if (leafFd !== undefined) fs.closeSync(leafFd);
    throw new ContainmentUnavailableError(String(error), true);
  }
};

export const releaseAttemptBoundary = (attemptId: string): void => {
  const entry = boundaries.get(attemptId);
  if (!entry) return;
  // A populated or uncertain attempt remains queryable; disposal is not authority
  // to discard the only complete-exit evidence.
  if (entry.failure !== undefined || (entry.emptyObservedAt === undefined && observe(attemptId, entry))) return;
  if (entry.waiters.size) return;
  const receipt: AttemptExitReceipt = { attemptId, scope: entry.boundary.relative, populated: false, at: entry.emptyObservedAt ?? Date.now() };
  rememberCompleted(attemptId, entry.boundary.relative, receipt);
  boundaries.delete(attemptId);
  entry.watcher.close();
  fs.closeSync(entry.eventsFd); fs.closeSync(entry.boundary.fd);
};

/** Number of retained native boundaries; exported for bounded-lifecycle tests. */
export const retainedAttemptBoundaryCount = (): number => boundaries.size;
const retainedFor = (attemptId: string): RetainedAttempt => {
  const entry = boundaries.get(attemptId);
  if (!entry) throw new ContainmentUnavailableError(`no retained admitted cgroup boundary for ${attemptId}`, true);
  if (entry.failure !== undefined) throw new AttemptExitUnconfirmedError({ attemptId, remainingPids: remaining(attemptId, entry.boundary) });
  return entry;
};

export const attemptMembership = (attemptId: string): AttemptMembership => {
  const completed = completedReceipts.get(attemptId);
  if (completed) return { attemptId, scope: completed.scope, pids: [], populated: false };
  const entry = retainedFor(attemptId), populated = observe(attemptId, entry);
  let pids: number[] | undefined;
  try { pids = taskCgroupPids(entry.boundary); }
  catch { throw new AttemptExitUnconfirmedError({ attemptId, remainingPids: [] }); }
  if (pids === undefined && populated) throw new AttemptExitUnconfirmedError({ attemptId, remainingPids: [] });
  return { attemptId, scope: entry.boundary.relative, pids: pids ?? [], populated };
};

const completeAttemptExit = (attemptId: string, entry: RetainedAttempt, receipt: AttemptExitReceipt): AttemptExitReceipt => {
  rememberCompleted(attemptId, entry.boundary.relative, receipt);
  releaseAttemptBoundary(attemptId); // closes native resources only after empty and all joined consumers settle
  return receipt;
};

export const joinAttemptExit = async (attemptId: string, deadlineAtMs: number): Promise<AttemptExitReceipt> => {
  if (!Number.isFinite(deadlineAtMs)) {
    const active = boundaries.get(attemptId);
    throw new AttemptExitUnconfirmedError({ attemptId, remainingPids: active ? remaining(attemptId, active.boundary) : [] });
  }
  const completed = completedReceipts.get(attemptId);
  if (completed) {
    if (completed.receipt.at <= deadlineAtMs) return completed.receipt;
    throw new AttemptExitUnconfirmedError({ attemptId, remainingPids: [] });
  }
  const entry = retainedFor(attemptId);
  for (;;) {
    if (entry.emptyObservedAt !== undefined && entry.emptyObservedAt <= deadlineAtMs) {
      const receipt = { attemptId, scope: entry.boundary.relative, populated: false as const, at: entry.emptyObservedAt };
      return completeAttemptExit(attemptId, entry, receipt);
    }
    if (Date.now() >= deadlineAtMs) throw new AttemptExitUnconfirmedError({ attemptId, remainingPids: remaining(attemptId, entry.boundary) });
    if (!observe(attemptId, entry)) {
      if (entry.emptyObservedAt !== undefined && entry.emptyObservedAt <= deadlineAtMs) {
        const receipt = { attemptId, scope: entry.boundary.relative, populated: false as const, at: entry.emptyObservedAt };
        return completeAttemptExit(attemptId, entry, receipt);
      }
      throw new AttemptExitUnconfirmedError({ attemptId, remainingPids: remaining(attemptId, entry.boundary) });
    }
    await new Promise<void>((resolve, reject) => {
      let settled = false;
      const wake = (): void => {
        if (settled) return;
        if (entry.failure !== undefined) {
          settled = true; clearTimeout(timer); entry.waiters.delete(wake);
          reject(new AttemptExitUnconfirmedError({ attemptId, remainingPids: remaining(attemptId, entry.boundary) })); return;
        }
        if (Date.now() >= deadlineAtMs) {
          settled = true; clearTimeout(timer); entry.waiters.delete(wake);
          reject(new AttemptExitUnconfirmedError({ attemptId, remainingPids: remaining(attemptId, entry.boundary) })); return;
        }
        settled = true; clearTimeout(timer); entry.waiters.delete(wake); resolve();
      };
      const timer = setTimeout(() => {
        if (settled) return;
        settled = true; entry.waiters.delete(wake);
        reject(new AttemptExitUnconfirmedError({ attemptId, remainingPids: remaining(attemptId, entry.boundary) }));
      }, Math.max(0, deadlineAtMs - Date.now()));
      entry.waiters.add(wake);
      try { if (!observe(attemptId, entry)) wake(); }
      catch (error) { settled = true; clearTimeout(timer); entry.waiters.delete(wake); reject(error); }
    });
  }
};

interface InternalLaunch {
  onSpawn?: (child: import("node:child_process").ChildProcess) => void;
  stdio?: StdioOptions;
  env?: NodeJS.ProcessEnv;
  configPath?: string;
  launcher?: unknown;
  attempt?: unknown;
  onAdmitted?: (receipt: AttemptLaunchReceipt) => void;
}
/** Public contract launch receipt; internal launcher may request its native handle separately. */
const launchSpec = (spec: ResidentLaunchSpec, attemptId: string, internal: InternalLaunch) => {
  const config = spec.config;
  const args = ["--mode", "rpc", "--no-session", "--no-tools", "--no-extensions", "--no-skills",
    "--no-prompt-templates", "--no-context-files", "--extension", spec.entry];
  const script = /\.[cm]?[jt]s$/i.test(config.piBinary);
  const command = script ? spec.runtime : config.piBinary;
  const commandArgs = script ? [config.piBinary, ...args] : args;
  const env = { ...process.env, ...(internal.env ?? {}), PI_FABRIC_RESIDENT_CONFIG: internal.configPath ?? launchSnapshotPath(config.residencyRoot, spec),
    PI_FABRIC_RESIDENT_LAUNCHER: internal.launcher ? JSON.stringify(internal.launcher) : process.env.PI_FABRIC_RESIDENT_LAUNCHER ?? "",
    PI_FABRIC_RESIDENT_SPEC_DIGEST: spec.digest,
    PI_FABRIC_RESIDENT_ATTEMPT: internal.attempt ? JSON.stringify(internal.attempt) : process.env.PI_FABRIC_RESIDENT_ATTEMPT ?? "" };
  return { attemptId, command, args: commandArgs, cwd: config.cwd, env, unitName: `fabric-resident-attempt-${attemptId}.scope`, memoryMaxBytes: 24 * 1024 ** 3 };
};

const launchAdmitted = async (input: ReturnType<typeof launchSpec>, internal: InternalLaunch) => {
  if (!delegatedUserCgroupAvailable(input.env)) throw new ContainmentUnavailableError("delegated cgroup-v2/user bus unavailable");
  const { findExecutable, spawnDetached } = await import("../agents/transports/process-utils.js");
  const executable = findExecutable("systemd-run", input.env);
  if (!executable) throw new ContainmentUnavailableError("systemd-run executable unavailable");
  let receipt: AttemptLaunchReceipt | undefined;
  try {
    const handle = await spawnDetached(input.command, input.args, input.cwd, undefined, input.env,
      { executable, slice: "app.slice", warn: () => {}, memoryMaxBytes: input.memoryMaxBytes, attemptId: input.attemptId, unitName: input.unitName },
      7_000, true, { directCommand: true, requireCappedAdmission: true, stdio: internal.stdio ?? ["pipe", "pipe", "pipe", "ipc"],
        ...(internal.onSpawn ? { onSpawn: internal.onSpawn } : {}),
        onAdmitted: admittedReceipt => {
          receipt = admittedReceipt;
          internal.onAdmitted?.(admittedReceipt);
        } });
    const boundary = handle.boundary;
    if (!boundary || !handle.child || handle.cgroup !== boundary.relative || !receipt || receipt.scope !== boundary.relative) {
      throw new ContainmentUnavailableError("admission returned no verified boundary/receipt", true);
    }
    return { receipt, handle };
  } catch (error) {
    if (error instanceof ContainmentUnavailableError) throw error;
    throw new ContainmentUnavailableError(String(error), true);
  }
};

/** Public stable contract takes the resident's immutable launch specification. */
export const launchInScope = async (spec: ResidentLaunchSpec, attemptId: string): Promise<AttemptLaunchReceipt> => {
  assertFreshResidentAttemptId(attemptId);
  const configPath = writeLaunchSnapshot(spec.config.residencyRoot, spec);
  const input = launchSpec(spec, attemptId, { configPath });
  const result = await launchAdmitted(input, { onAdmitted: receipt => { writeResidentAttestation(spec.config.residencyRoot, receipt); } });
  return result.receipt;
};

/** Launcher-only split carries native child custody while public API remains receipt-only. */
export const launchResidentInScope = async (spec: ResidentLaunchSpec, attemptId: string, internal: InternalLaunch) => {
  assertFreshResidentAttemptId(attemptId);
  return launchAdmitted(launchSpec(spec, attemptId, internal), internal);
};
export const attemptContainment: AttemptContainment = { launchInScope, attemptMembership, joinAttemptExit };

