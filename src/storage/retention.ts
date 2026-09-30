import fs from "node:fs";
import path from "node:path";
import { writeJsonAtomic } from "../core/atomic-write.js";
import { ownedStat, processAlive } from "./scratch.js";
import { hasUnsettledRecordedProcesses } from "./worker-settlement.js";

export const FABRIC_RUN_ROOT_PREFIX = "pi-fabric-runs-";
const RUN_ROOT_OWNER_FILE = ".fabric-owner.json";
const TERMINAL_STATUSES = new Set(["completed", "failed", "stopped", "timed_out"]);
interface RunRootOwner {
  pid: number;
  startedAt: number;
  heartbeatAt: number;
  orphanedAt?: number;
  closedAt?: number;
  childrenStopped?: boolean;
}
interface RunRecordSummary {
  status?: string;
  actorId?: string;
  finishedAt?: number;
  updatedAt?: number;
  transport?: string;
  sessionId?: string;
}
export interface RetentionSweepResult {
  removedRoots: string[];
  removedRuns: string[];
}
const ownerPath = (root: string): string => path.join(root, RUN_ROOT_OWNER_FILE);
const readJson = <T>(file: string): T | undefined => {
  try {
    const stat = ownedStat(file);
    if (!stat?.isFile() || stat.size > 1024 * 1024) return;
    return JSON.parse(fs.readFileSync(file, "utf8")) as T;
  } catch { return; }
};
const time = (value: unknown): value is number => typeof value === "number" && Number.isFinite(value) && value >= 0;
const validOwner = (owner: RunRootOwner | undefined): owner is RunRootOwner => !!owner &&
  Number.isSafeInteger(owner.pid) && owner.pid > 0 && time(owner.startedAt) && time(owner.heartbeatAt) &&
  (owner.closedAt === undefined || time(owner.closedAt)) &&
  (owner.orphanedAt === undefined || time(owner.orphanedAt)) &&
  (owner.childrenStopped === undefined || typeof owner.childrenStopped === "boolean");
const writeOwner = (root: string, owner: RunRootOwner): void => {
  if (fs.existsSync(root) && !ownedStat(root)?.isDirectory()) throw new Error("Unsafe Fabric run root");
  const file = ownerPath(root);
  if (fs.existsSync(file)) {
    const existing = readJson<RunRootOwner>(file);
    if (!validOwner(existing) || existing.pid !== owner.pid) throw new Error("Unsafe Fabric run owner marker");
  }
  writeJsonAtomic(file, owner);
};
export const markRunRootActive = (root: string, now = Date.now()): void => {
  const existing = readJson<RunRootOwner>(ownerPath(root));
  writeOwner(root, { pid: process.pid, startedAt: validOwner(existing) ? existing.startedAt : now, heartbeatAt: now });
};
export const heartbeatRunRoot = markRunRootActive;
export const markRunRootClosed = (root: string, now = Date.now(), childrenStopped = false): void => {
  const existing = readJson<RunRootOwner>(ownerPath(root));
  writeOwner(root, { pid: process.pid, startedAt: validOwner(existing) ? existing.startedAt : now, heartbeatAt: now, closedAt: now, childrenStopped });
};
/**
 * A run whose worker may still be running (lost contact, or an unconfirmed launch) keeps
 * this file in its run directory. Every cleanup path refuses such a run, across restarts.
 */
export const UNRESOLVED_WORKER_FILE = "unresolved-worker.json";
/** A sweep's wall-clock budget; walks stop when it returns true (smarty-dev#2010). */
type Deadline = () => boolean;
const noDeadline: Deadline = () => false;
/**
 * True when this run, or any nested child run below it, is marked. A walk cut short by the
 * deadline also answers true: an unfinished check never authorizes deletion.
 */
export const hasUnresolvedWorker = (runDirectory: string, depth = 0, expired: Deadline = noDeadline): boolean => {
  if (expired()) return true;
  if (fs.existsSync(path.join(runDirectory, UNRESOLVED_WORKER_FILE))) return true;
  if (depth >= 32) return false;
  const nested = path.join(runDirectory, "nested");
  try {
    return fs.readdirSync(nested, { withFileTypes: true })
      .some((entry) => entry.isDirectory() && hasUnresolvedWorker(path.join(nested, entry.name), depth + 1, expired));
  } catch {
    return false;
  }
};
export const markUnresolvedWorker = (runDirectory: string, reason: string, details: Record<string, unknown> = {}): void => {
  fs.mkdirSync(runDirectory, { recursive: true, mode: 0o700 });
  writeJsonAtomic(path.join(runDirectory, UNRESOLVED_WORKER_FILE), { reason, markedAt: Date.now(), ...details });
};
const recordAgeReference = (record: RunRecordSummary, fallback: number): number =>
  time(record.finishedAt) ? record.finishedAt : time(record.updatedAt) ? record.updatedAt : fallback;
// Every file the worker and manager write into a run directory. A missing name made the run
// unremovable forever: 54k expired actor runs with reply.json piled up in /tmp (smarty-dev#2010).
const runFiles = new Set([
  "task.txt", "status.json", "events.jsonl", "lifecycle.jsonl", "steer.jsonl", "schema.json", "images.json",
  "reply.json", "relaunches.jsonl", "worker-processes.jsonl", "worker-launches.jsonl",
]);
const runFile = (name: string): boolean => runFiles.has(name) || /^oversized-event-prefix(-\d+)?\.txt$/.test(name);
/** Unknown transports/contents and live descendants veto removal, even under a dead host. */
const safeRunTree = (root: string, childrenStopped: boolean, depth = 0, expired: Deadline = noDeadline): boolean => {
  if (expired() || depth > 32 || !ownedStat(root)?.isDirectory()) return false;
  if (hasUnresolvedWorker(root, 0, expired) || hasUnsettledRecordedProcesses(root)) return false;
  const record = readJson<RunRecordSummary>(path.join(root, "status.json"));
  const pid = record?.transport === "process" && typeof record.sessionId === "string" && /^\d+$/.test(record.sessionId)
    ? Number(record.sessionId) : undefined;
  if (pid !== undefined && processAlive(pid)) return false;
  if (!record?.status || !TERMINAL_STATUSES.has(record.status)) {
    if (!childrenStopped && pid === undefined) return false;
    if (!ownedStat(path.join(root, "task.txt"))?.isFile()) return false;
  }
  try {
    for (const name of fs.readdirSync(root)) {
      if (expired()) return false;
      const file = path.join(root, name);
      const stat = ownedStat(file);
      if (!stat) return false;
      if (stat.isFile() && runFile(name)) continue;
      if (stat.isDirectory() && name === "handoff-session") {
        // This directory is exclusively populated by Fabric's session fork writer.
        if (fs.readdirSync(file).some((child) => !child.endsWith(".jsonl") || !ownedStat(path.join(file, child))?.isFile())) return false;
        continue;
      }
      if (stat.isDirectory() && name === "nested") {
        for (const child of fs.readdirSync(file)) if (!safeRunTree(path.join(file, child), false, depth + 1, expired)) return false;
        continue;
      }
      return false;
    }
    return true;
  } catch { return false; }
};
const safeRootContents = (root: string, childrenStopped: boolean): boolean => {
  try { return fs.readdirSync(root).every((name) => name === RUN_ROOT_OWNER_FILE || safeRunTree(path.join(root, name), childrenStopped)); }
  catch { return false; }
};
export const canRemoveManagedRunRoot = (root: string): boolean => {
  if (!ownedStat(root)?.isDirectory()) return false;
  const owner = readJson<RunRootOwner>(ownerPath(root));
  return validOwner(owner) && owner.pid === process.pid && safeRootContents(root, true);
};
export const removeEmptyRunRoot = (root: string): boolean => {
  try {
    if (!ownedStat(root)?.isDirectory() || !validOwner(readJson<RunRootOwner>(ownerPath(root)))) return false;
    if (fs.readdirSync(root).some((name) => name !== RUN_ROOT_OWNER_FILE)) return false;
    fs.rmSync(root, { recursive: true, force: true });
    return true;
  } catch { return false; }
};
const pruneClosedRunRoot = (
  root: string, owner: RunRootOwner, orphanMs: number, oneShotMs: number, now: number, expired: () => boolean,
): string[] => {
  const removed: string[] = [];
  // Every run started after its root, so no run of a root younger than the shortest retention is due.
  if (now - owner.startedAt < Math.min(orphanMs, oneShotMs)) return removed;
  let entries: fs.Dirent[];
  try { entries = fs.readdirSync(root, { withFileTypes: true }); } catch { return removed; }
  for (const entry of entries) {
    if (expired()) break;
    if (!entry.isDirectory()) continue;
    const directory = path.join(root, entry.name);
    // Age first: a young run is kept either way, so it needs no full tree walk (smarty-dev#2010).
    const record = readJson<RunRecordSummary>(path.join(directory, "status.json"));
    const terminal = !!record?.status && TERMINAL_STATUSES.has(record.status);
    const reference = terminal ? recordAgeReference(record!, ownedStat(directory)?.mtimeMs ?? now) : owner.closedAt!;
    const retention = terminal && !record?.actorId ? oneShotMs : orphanMs;
    if (now - reference < retention) continue;
    if (!safeRunTree(directory, owner.childrenStopped === true, 0, expired)) continue;
    try { fs.rmSync(directory, { recursive: true, force: true }); removed.push(directory); } catch {}
  }
  return removed;
};
/** Host-wide marker of the last temp-root sweep; a dotfile, so the root pattern never matches it. */
export const RUN_ROOT_SWEEP_MARKER = ".pi-fabric-runs-sweep.json";
/**
 * Claim the host's next temp-root sweep: false when any process of this user swept within
 * `minIntervalMs`. The claim is written before the walk, so processes that close together do not
 * all sweep; a stale marker, or one dated in the future, does not suppress collection.
 */
export const claimTempRunSweep = (tempRoot: string, minIntervalMs: number, now = Date.now()): boolean => {
  const marker = path.join(tempRoot, RUN_ROOT_SWEEP_MARKER);
  const last = readJson<{ sweptAt?: unknown }>(marker)?.sweptAt;
  if (time(last) && last <= now && now - last < minIntervalMs) return false;
  try { writeJsonAtomic(marker, { sweptAt: now }); } catch {}
  return true;
};
export interface TempRunSweepRequest {
  tempRoot: string;
  currentRoot?: string;
  orphanedTempRunRetentionMs: number;
  oneShotRunRetentionMs: number;
}
export const sweepTempRunRoots = (options: {
  tempRoot: string;
  currentRoot?: string;
  orphanedTempRunRetentionMs: number;
  oneShotRunRetentionMs: number;
  now?: number;
  /**
   * Skip when any process of this user swept the temp root within this interval. The sweep walks
   * every retained run on the host synchronously: with tens of thousands of runs it blocked each
   * Pi exit for 20 s or more (smarty-dev#2010). One sweep per interval per host is enough.
   */
  minIntervalMs?: number;
  /**
   * Stop after this much wall time. Roots are visited in random order, so successive bounded
   * sweeps still cover every root; neither an exit nor a live event loop blocks for long.
   */
  budgetMs?: number;
}): RetentionSweepResult => {
  const startedAt = performance.now();
  const expired = (): boolean =>
    options.budgetMs !== undefined && performance.now() - startedAt >= options.budgetMs;
  const now = options.now ?? Date.now();
  const result: RetentionSweepResult = { removedRoots: [], removedRuns: [] };
  if (options.minIntervalMs !== undefined && !claimTempRunSweep(options.tempRoot, options.minIntervalMs, now)) return result;
  let entries: fs.Dirent[];
  try { entries = fs.readdirSync(options.tempRoot, { withFileTypes: true }); } catch { return result; }
  if (options.budgetMs !== undefined) {
    for (let index = entries.length - 1; index > 0; index--) {
      const other = Math.floor(Math.random() * (index + 1));
      [entries[index], entries[other]] = [entries[other]!, entries[index]!];
    }
  }
  for (const entry of entries) {
    if (expired()) break;
    if (!entry.isDirectory() || !/^pi-fabric-runs-[A-Za-z0-9_-]+$/.test(entry.name)) continue;
    const root = path.join(options.tempRoot, entry.name);
    if (options.currentRoot && path.resolve(root) === path.resolve(options.currentRoot)) continue;
    if (!ownedStat(root)?.isDirectory()) continue;
    const owner = readJson<RunRootOwner>(ownerPath(root));
    if (!validOwner(owner)) continue;
    if (owner.closedAt !== undefined) {
      result.removedRuns.push(...pruneClosedRunRoot(
        root, owner, options.orphanedTempRunRetentionMs, options.oneShotRunRetentionMs, now, expired,
      ));
      if (removeEmptyRunRoot(root)) result.removedRoots.push(root);
      continue;
    }
    if (processAlive(owner.pid)) continue;
    if (owner.orphanedAt === undefined) {
      try { writeOwner(root, { ...owner, orphanedAt: now }); } catch {}
      continue;
    }
    if (now - owner.orphanedAt < options.orphanedTempRunRetentionMs) continue;
    // Run by run, not one whole-root walk and removal: each run passes the same safety check
    // before it goes, the deadline is checked between runs, and the root goes once empty.
    let runs: fs.Dirent[];
    try { runs = fs.readdirSync(root, { withFileTypes: true }); } catch { continue; }
    for (const run of runs) {
      if (expired()) break;
      if (run.name === RUN_ROOT_OWNER_FILE || !run.isDirectory()) continue;
      const directory = path.join(root, run.name);
      if (!safeRunTree(directory, false, 0, expired)) continue;
      // Reported as the root's removal once it is empty, as before.
      try { fs.rmSync(directory, { recursive: true, force: true }); } catch {}
    }
    if (removeEmptyRunRoot(root)) result.removedRoots.push(root);
  }
  return result;
};

export const pruneActorRunArchives = (options: {
  runsDirectory: string;
  latestRunId?: string;
  retentionMs: number;
  now?: number;
}): string[] => {
  const now = options.now ?? Date.now();
  const removed: string[] = [];
  if (!ownedStat(options.runsDirectory)?.isDirectory()) return removed;
  let entries: fs.Dirent[];
  try { entries = fs.readdirSync(options.runsDirectory, { withFileTypes: true }); } catch { return removed; }
  for (const entry of entries) {
    if (!entry.isDirectory() || entry.name === options.latestRunId) continue;
    const directory = path.join(options.runsDirectory, entry.name);
    const record = readJson<RunRecordSummary>(path.join(directory, "status.json"));
    if (!record?.status || !TERMINAL_STATUSES.has(record.status) || !safeRunTree(directory, false)) continue;
    if (now - recordAgeReference(record, ownedStat(directory)?.mtimeMs ?? now) < options.retentionMs) continue;
    try { fs.rmSync(directory, { recursive: true, force: true }); removed.push(directory); } catch {}
  }
  return removed;
};
