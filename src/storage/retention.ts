import fs from "node:fs";
import path from "node:path";
import { writeFileAtomic, writeJsonAtomic } from "../core/atomic-write.js";
import { ownedStat, processAlive } from "./scratch.js";
import { hasNeverStartedReceipt, NEVER_STARTED_FILE, runScratchExitVeto } from "./run-scratch.js";
import { processStartTime } from "../residency/process-identity.js";

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
  processStartTime?: string;
}
export interface RetentionSweepResult {
  removedRoots: string[];
  removedRuns: string[];
}
const ownerPath = (root: string): string => path.join(root, RUN_ROOT_OWNER_FILE);
const readJson = <T>(file: string, maxBytes = 1024 * 1024): T | undefined => {
  try {
    const stat = ownedStat(file);
    if (!stat?.isFile() || stat.size > maxBytes) return;
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
/** A terminal record is not a descendant exit receipt. Share this persistent,
 * tree-wide veto across tracked, recovered and offline cleanup before removing
 * worktrees or files; absence of an unresolved marker never proves worker exit.
 * Recordless pre-launch rollback remains distinct from an admitted process run.
 * Ownership retention additionally requires checked process exit for every
 * descendant, without coupling that proof to cleanup's artifact allowlist. */
export const runTreeExitVeto = (
  directory: string, depth = 0, expired: Deadline = noDeadline, requireDescendantExit = false,
): string | undefined => {
  if (expired() || depth > 32) return "worker exit is unconfirmed: run-tree inspection was incomplete";
  // A previously removed tree has no worker files left to collect. Only this
  // initial absence is safe; errors or changes during inspection veto cleanup.
  try { fs.lstatSync(directory); }
  catch (error) {
    return (error as NodeJS.ErrnoException).code === "ENOENT" && !requireDescendantExit
      ? undefined : "worker exit is unconfirmed: run-tree inspection failed";
  }
  try {
    if (!ownedStat(directory)?.isDirectory()) return "worker exit is unconfirmed: unsafe run directory";
    // A parent's populated=0 scope may still contain empty nested cgroups.
    // Retire only checked nested run scopes bottom-up, before parent rmdir.
    // Unknown children/receipts/identities veto the entire parent collection.
    const nested = path.join(directory, "nested");
    let hasNested = false;
    try { fs.lstatSync(nested); hasNested = true; }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
    if (hasNested) {
      if (!ownedStat(nested)?.isDirectory()) return "worker exit is unconfirmed: unsafe nested run directory";
      for (const name of fs.readdirSync(nested)) {
        const reason = runTreeExitVeto(path.join(nested, name), depth + 1, expired, requireDescendantExit);
        if (reason) return reason;
      }
    }
    if (fs.existsSync(path.join(directory, UNRESOLVED_WORKER_FILE))) return "its worker may still be running (unresolved worker marker)";
    if (expired()) return "worker exit is unconfirmed: run-tree inspection was incomplete";
    const statusFile = path.join(directory, "status.json");
    const record = readJson<RunRecordSummary>(statusFile);
    if (expired()) return "worker exit is unconfirmed: run-tree inspection was incomplete";
    if (fs.existsSync(statusFile) && !record) return "worker exit is unconfirmed: unreadable run record";
    if (record?.transport === "tmux" || record?.transport === "screen") {
      return `${record.transport} transport has no checked worker exit receipt (${directory})`;
    }
    // Tracked ownership and collection compose conservatively: the descendant
    // ownership check must pass as well as the terminal PID/birth proof below.
    // A surviving tracked root has its own transport exit evidence; descendants
    // have no surviving handles and must retain their persisted identities.
    if (requireDescendantExit && depth > 0) {
      const pid = record?.transport === "process" && typeof record.sessionId === "string" && /^\d+$/.test(record.sessionId)
        ? Number(record.sessionId) : undefined;
      if (pid === undefined || !Number.isSafeInteger(pid) || pid <= 0) {
        if (!hasNeverStartedReceipt(directory)) {
          const reason = "worker exit is unconfirmed: unknown descendant identity";
          const scratchVeto = runScratchExitVeto(directory, expired);
          return scratchVeto ? `${reason}; ${scratchVeto}` : reason;
        }
      }
      if (pid !== undefined && processAlive(pid)) return `worker exit is unconfirmed: its descendant worker may still be running (${directory})`;
    }
    if (record?.transport === "process") {
      if (!record.status || !TERMINAL_STATUSES.has(record.status)) {
        return `worker exit is unconfirmed: nonterminal process record (${directory})`;
      }
      // A missing/invalid PID is unknown, not a never-launched record. ESRCH
      // proves absence; a live PID is safe only if its checked start identity
      // differs from the worker's saved identity (PID reuse). Query errors and
      // unreadable birth identity never authorize removal.
      const pid = typeof record.sessionId === "string" && /^\d+$/.test(record.sessionId)
        ? Number(record.sessionId) : NaN;
      const validPid = Number.isSafeInteger(pid) && pid > 0;
      let alive = false;
      if (validPid) {
        try { process.kill(pid, 0); alive = true; }
        catch (error) {
          if ((error as NodeJS.ErrnoException).code !== "ESRCH") {
            return `worker exit is unconfirmed: saved process identity is live or unknown (${directory})`;
          }
        }
      }
      const savedStart = typeof record.processStartTime === "string" && /^\d+$/.test(record.processStartTime)
        ? record.processStartTime : undefined;
      const currentStart = alive && savedStart ? processStartTime(pid) : undefined;
      if (!validPid || (alive && (currentStart === undefined || currentStart === savedStart))) {
        return `worker exit is unconfirmed: saved process identity is live or unknown (${directory})`;
      }
    }
    // Report a known worker/descendant obligation before the independent
    // scratch fence. Both still have to pass; native exit never bypasses it.
    const scratchVeto = runScratchExitVeto(directory, expired);
    if (scratchVeto) return scratchVeto;
  } catch { return "worker exit is unconfirmed: run-tree inspection failed"; }
};
const recordAgeReference = (record: RunRecordSummary, fallback: number): number =>
  time(record.finishedAt) ? record.finishedAt : time(record.updatedAt) ? record.updatedAt : fallback;
// Every file the worker and manager write into a run directory. A missing name made the run
// unremovable forever: 54k expired actor runs with reply.json piled up in /tmp (smarty-dev#2010).
const runFiles = new Set([
  "task.txt", "task.txt.provenance.json", "status.json", "events.jsonl", "lifecycle.jsonl", "steer.jsonl", "schema.json", "images.json",
  "reply.json", "relaunches.jsonl", "route-session.jsonl",
  // Native session of an unrouted process Pi task (worker.ts persistentPiTask); owned file only.
  "session.jsonl",
]);
const runFile = (name: string): boolean => runFiles.has(name) || /^oversized-event-prefix(-\d+)?\.txt$/.test(name);
/** Unknown transports/contents and live descendants veto removal, even under a dead host. */
const safeRunTree = (root: string, childrenStopped: boolean, depth = 0, expired: Deadline = noDeadline): boolean => {
  if (expired() || depth > 32 || !ownedStat(root)?.isDirectory()) return false;
  if (runTreeExitVeto(root, 0, expired, true)) return false;
  const neverStarted = hasNeverStartedReceipt(root);
  const record = readJson<RunRecordSummary>(path.join(root, "status.json"));
  // Automatic retention keeps its independent live-writer fence. A mismatched
  // birth identity can clear explicit cleanup's exit veto, but never authorizes
  // a sweep to remove a run with a live or unknown saved PID. Apply this at
  // every level, including descendants, alongside the recursive exit proof.
  const pid = record?.transport === "process" && typeof record.sessionId === "string" && /^\d+$/.test(record.sessionId)
    ? Number(record.sessionId) : undefined;
  if (pid !== undefined && processAlive(pid)) return false;
  if (!record?.status || !TERMINAL_STATUSES.has(record.status)) {
    if (!childrenStopped && !neverStarted) return false;
    if (!ownedStat(path.join(root, "task.txt"))?.isFile()) return false;
  }
  try {
    for (const name of fs.readdirSync(root)) {
      if (expired()) return false;
      const file = path.join(root, name);
      const stat = ownedStat(file);
      if (!stat) return false;
      if (stat.isFile() && (runFile(name) || (name === NEVER_STARTED_FILE && neverStarted))) continue;
      if (stat.isDirectory() && name === "handoff-session") {
        // This directory is exclusively populated by Fabric's session fork writer.
        if (fs.readdirSync(file).some((child) => !child.endsWith(".jsonl") || !ownedStat(path.join(file, child))?.isFile())) return false;
        continue;
      }
      if (stat.isDirectory() && name === "deliveries") {
        // The worker always creates this ingress directory; the native Pi hook
        // unlinks consumed items. Any remaining item is pending or unknown,
        // even when it has a known filename or valid JSON: keep the whole run.
        if (fs.readdirSync(file).length !== 0) return false;
        // Only the worker's UUID-addressed private delivery envelopes are ours.
        // Empty directories are normal; unknown content, links and non-files veto.
        for (const child of fs.readdirSync(file)) {
          if (expired() || !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.json$/.test(child) ||
              !ownedStat(path.join(file, child))?.isFile()) return false;
        }
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
/** Explicit resident roots have no managed-temp owner. Require terminal status
 * plus checked process absence, and veto nested survivors and unresolved markers. */
export const canRemoveTerminalRun = (directory: string, expired: Deadline = noDeadline): boolean => {
  const record = readJson<RunRecordSummary>(path.join(directory, "status.json"));
  return !!record?.status && TERMINAL_STATUSES.has(record.status) && safeRunTree(directory, false, 0, expired);
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

/** Full persisted latest-run references, shared by resident startup and streaming retention.
 * An unreadable registry is a wildcard veto, never proof that a lastRunId is absent. */
export const retainedActorRunIds = (actorRoots: readonly string[]): Set<string> => {
  const refs = new Set<string>();
  try {
    for (const root of actorRoots) {
      try { fs.lstatSync(root); }
      catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") continue; throw error; }
      if (!ownedStat(root)?.isDirectory()) throw new Error("Unsafe actor root");
      const file = path.join(root, "actors.json");
      try { fs.lstatSync(file); }
      catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") continue; throw error; }
      // ActorRegistryStore's writer/reader has no byte-size protocol limit: every
      // actor includes instructions and up to 100 message bodies, so even one
      // ordinary actor can exceed the 1-MiB summary-file guard. Match that existing
      // JSON contract rather than inventing a fleet-size limit that disables all
      // retention. Ownership, JSON/schema errors and unsafe references still veto.
      const registry = readJson<{ actors?: Array<{ id?: unknown; lastRunId?: unknown }> }>(file, Number.MAX_SAFE_INTEGER);
      if (!Array.isArray(registry?.actors)) throw new Error("Unreadable actor registry");
      for (const actor of registry.actors) {
        if (!actor || typeof actor.id !== "string" ||
            (actor.lastRunId !== undefined && typeof actor.lastRunId !== "string")) throw new Error("Unknown actor run reference");
        if (actor.lastRunId) refs.add(actor.lastRunId);
      }
    }
  } catch { refs.add("*"); }
  return refs;
};

export interface TerminalRunEventsRetention {
  terminalRunEventsAgeMs?: number;
  terminalRunEventsMaxBytes?: number;
}

const EVENT_TAIL_MARKER = Buffer.from('{"fabricTruncated":true,"reason":"terminal run event retention"}\n');

/** Compact only an owned, safely terminal run. Read a bounded suffix, keep complete JSONL
 * lines, and atomically replace only events.jsonl; status/reply/result remain byte-for-byte.
 * The marker counts against the byte cap. A single oversized final event may leave only
 * the marker rather than a corrupt JSON fragment. Already bounded logs are never rewritten.
 * Callers retain their ownership/latest-run vetoes before entering this shared predicate. */
export const compactTerminalRunEvents = (
  directory: string,
  options: TerminalRunEventsRetention & { now?: number; expired?: Deadline; isRetained?: () => boolean } = {},
): boolean => {
  const now = options.now ?? Date.now();
  const ageMs = options.terminalRunEventsAgeMs ?? 24 * 60 * 60 * 1_000;
  const maxBytes = options.terminalRunEventsMaxBytes ?? 256 * 1024;
  const expired = options.expired ?? noDeadline;
  if (!Number.isSafeInteger(ageMs) || ageMs < 0 || !Number.isSafeInteger(maxBytes) ||
      maxBytes < EVENT_TAIL_MARKER.length || expired() || options.isRetained?.() || !ownedStat(directory)?.isDirectory()) return false;
  const record = readJson<RunRecordSummary>(path.join(directory, "status.json"));
  if (!record?.status || !TERMINAL_STATUSES.has(record.status) ||
      now - recordAgeReference(record, ownedStat(directory)?.mtimeMs ?? now) < ageMs) return false;
  const file = path.join(directory, "events.jsonl");
  const stat = ownedStat(file);
  if (!stat?.isFile() || stat.size <= maxBytes || runTreeExitVeto(directory, 0, expired, true) ||
      !canRemoveTerminalRun(directory, expired)) return false;
  try {
    const fd = fs.openSync(file, fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW ?? 0));
    let tail: Buffer;
    try {
      const opened = fs.fstatSync(fd);
      if (opened.dev !== stat.dev || opened.ino !== stat.ino || opened.size !== stat.size ||
          opened.mtimeMs !== stat.mtimeMs) return false;
      // One look-behind byte lets an exactly aligned final line survive the boundary.
      const length = maxBytes - EVENT_TAIL_MARKER.length + 1;
      tail = Buffer.alloc(length);
      let read = 0;
      while (read < length) {
        if (expired()) return false;
        const count = fs.readSync(fd, tail, read, length - read, stat.size - length + read);
        if (count === 0) return false;
        read += count;
      }
    } finally { fs.closeSync(fd); }
    const newline = tail.indexOf(0x0a);
    const retained = newline < 0 ? Buffer.alloc(0) : tail.subarray(newline + 1);
    const checked = ownedStat(file);
    if (!checked || checked.dev !== stat.dev || checked.ino !== stat.ino ||
        checked.size !== stat.size || checked.mtimeMs !== stat.mtimeMs ||
        runTreeExitVeto(directory, 0, expired, true) || !canRemoveTerminalRun(directory, expired) ||
        expired() || options.isRetained?.()) return false;
    writeFileAtomic(file, Buffer.concat([EVENT_TAIL_MARKER, retained]));
    return true;
  } catch { return false; }
};

export const pruneActorRunArchives = (options: {
  runsDirectory: string;
  latestRunId?: string;
  retentionMs: number;
  terminalRunEventsAgeMs?: number;
  terminalRunEventsMaxBytes?: number;
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
    if (now - recordAgeReference(record, ownedStat(directory)?.mtimeMs ?? now) < options.retentionMs) {
      compactTerminalRunEvents(directory, { ...options, now });
      continue;
    }
    try { fs.rmSync(directory, { recursive: true, force: true }); removed.push(directory); } catch {}
  }
  return removed;
};
