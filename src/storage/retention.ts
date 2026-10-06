import fs from "node:fs";
import { retentionV2Enabled } from "./retention-platform.js";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { writeJsonAtomic } from "../core/atomic-write.js";
import { ownedStat, processAlive } from "./scratch.js";
import { logWindowsScratchScopeCut, runScratchExitVeto } from "./run-scratch.js";
import { processStartTime } from "../residency/process-identity.js";
import { recoverActorRunArchives } from "../actors/child-completions.js";
import { copyFabricProvenance } from "../fabric-provenance.js";
import { inspectTreeSync, inspectTreeAsync, treeStat, treeRead, treeList, treeExists, treeOpen, treeHandleStat, treeClose, treeTail, treeWrite, treeTimes, type TreeWalk } from "./retention-io.js";

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
  queuedArchiveCommitted?: boolean;
  cleanupPending?: boolean;
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
/** A terminal record is not a worker exit receipt. Share this persistent,
 * tree-wide veto across tracked, recovered and offline cleanup before removing
 * worktrees or files; absence of an unresolved marker never proves worker exit.
 * Recordless pre-launch rollback remains distinct from an admitted process run.
 * Ownership retention additionally requires persisted, checked process exit for
 * the admitted root and every descendant, independently of the artifact allowlist.
 * External transports currently have no durable native exit-receipt contract: skip
 * them even when a surviving host once observed a terminal result. */
export interface RunTreeExitOptions {
  /** Compaction/dry-run inspect custody without collecting scratch or creating locks. */
  disposeScratch?: boolean;
  /** Live admission knows this root never launched; never inherited by descendants.
   * Applies only to a recordless root, not to saved or ambiguous worker identities. */
  allowUnlaunchedRoot?: boolean;
}
export const runTreeExitVeto = (
  directory: string, depth = 0, expired: Deadline = noDeadline, requirePersistedExit = false,
  options: RunTreeExitOptions = {},
): string | undefined => runTreeVeto(directory, depth, expired, requirePersistedExit, true, requirePersistedExit, options);
/** Native resource safety is independent of full-result archival. Run-file
 * collection must use runTreeExitVeto with its archive/root fence. */
export const runTreeResourceVeto = (
  directory: string, depth = 0, expired: Deadline = noDeadline, requireDescendantExit = false, requireRootExit = false,
): string | undefined => runTreeVeto(directory, depth, expired, requireDescendantExit, false, requireRootExit, {});
interface RunInspection { root: fs.Stats; record: RunRecordSummary | undefined; statusStat: fs.Stats | undefined; names?: string[] }
const isOwnedStat = (stat: fs.Stats): boolean => !stat.isSymbolicLink() && !(stat.isFile() && stat.nlink !== 1) &&
  (!process.getuid || stat.uid === process.getuid());
function* treeOwnedStat(file: string): TreeWalk<fs.Stats | undefined> {
  try { const stat = yield* treeStat(file); return isOwnedStat(stat) ? stat : undefined; } catch { return; }
}
function* treeJson<T>(file: string, maxBytes = 1024 * 1024): TreeWalk<T | undefined> {
  try {
    const stat = yield* treeOwnedStat(file);
    if (!stat?.isFile() || stat.size > maxBytes) return;
    return JSON.parse(yield* treeRead(file)) as T;
  } catch { return; }
}
function* inspectRunTree(
  directory: string, depth: number, expired: Deadline, requirePersistedExit: boolean, preserveArchives: boolean, requireRootExit: boolean,
  options: RunTreeExitOptions,
  // Internal collection-only optimization. Every directory will subsequently
  // pass safeRunTree's recursive artifact allowlist, which rejects tmp and all
  // scratch receipts. Never used by the public exit/resource predicates.
  scratchCoveredByAllowlist = false,
  inspections?: Map<string, RunInspection>,
  // Only the synchronous archive collector may reuse this census. Async IO
  // and public exit/resource proofs must keep their independent observations.
  reuseCensus = false,
): TreeWalk<string | undefined> {
  if (expired() || depth > 32) return "worker exit is unconfirmed: run-tree inspection was incomplete";
  // A previously removed tree has no worker files left to collect. Only this
  // initial absence is safe; errors or changes during inspection veto cleanup.
  let rootStat: fs.Stats;
  try { rootStat = yield* treeStat(directory); }
  catch (error) {
    return (error as NodeJS.ErrnoException).code === "ENOENT" && !requirePersistedExit
      ? undefined : "worker exit is unconfirmed: run-tree inspection failed";
  }
  try {
    // The Windows scope cut preserves main's exact inspection crossings.
    const mainWindows = process.platform === "win32" && !scratchCoveredByAllowlist;
    const ownedRoot = mainWindows ? yield* treeOwnedStat(directory) : rootStat;
    if (!ownedRoot || !isOwnedStat(ownedRoot) || !ownedRoot.isDirectory()) return "worker exit is unconfirmed: unsafe run directory";
    if (expired()) return "worker exit is unconfirmed: run-tree inspection was incomplete";
    const names = reuseCensus ? yield* treeList(directory) : undefined;
    if (preserveArchives && (names ? names.includes("archive-pending.json") : yield* treeExists(path.join(directory, "archive-pending.json")))) return "terminal result archive is pending";
    if (expired()) return "worker exit is unconfirmed: run-tree inspection was incomplete";
    if (preserveArchives && (names ? names.includes("actor-run-archive-pending.json") : yield* treeExists(path.join(directory, "actor-run-archive-pending.json")))) return "actor run receipt archive is pending";
    if (expired()) return "worker exit is unconfirmed: run-tree inspection was incomplete";
    if (names ? names.includes(UNRESOLVED_WORKER_FILE) : yield* treeExists(path.join(directory, UNRESOLVED_WORKER_FILE))) return "its worker may still be running (unresolved worker marker)";
    if (expired()) return "worker exit is unconfirmed: run-tree inspection was incomplete";
    const statusFile = path.join(directory, "status.json");
    let statusStat: fs.Stats | undefined;
    try { statusStat = yield* treeStat(statusFile); }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
    if (expired()) return "worker exit is unconfirmed: run-tree inspection was incomplete";
    const statusExists = statusStat !== undefined;
    let record: RunRecordSummary | undefined;
    if (statusStat && isOwnedStat(statusStat) && statusStat.isFile() && statusStat.size <= 1024 * 1024) {
      try { record = JSON.parse(yield* treeRead(statusFile)) as RunRecordSummary; } catch { /* existing unreadable records veto below */ }
    }
    if (expired()) return "worker exit is unconfirmed: run-tree inspection was incomplete";
    if ((mainWindows ? yield* treeExists(statusFile) : statusExists) && !record) return "worker exit is unconfirmed: unreadable run record";
    if (record?.cleanupPending !== undefined && record.cleanupPending !== false) return "worker cleanup is not joined";
    if (record?.transport === "tmux" || record?.transport === "screen") {
      return `${record.transport} transport has no checked worker exit receipt (${directory})`;
    }
    // Offline compaction/collection has no surviving transport handle. Require
    // the saved root identity too; a free host flock, terminal status, exitCode,
    // or absent unresolved marker does not prove the root writer has exited.
    // Recordless pre-launch rollback uses the non-retention mode explicitly;
    // missing persisted status is never evidence for an admitted worker.
    // A committed queued archive proves never-launched admission for the root only.
    const committedPrelaunch = depth === 0 && record?.queuedArchiveCommitted === true &&
      record.transport === undefined && !!record.status && TERMINAL_STATUSES.has(record.status);
    const unlaunchedRoot = depth === 0 && options.allowUnlaunchedRoot && !statusExists;
    if (((requirePersistedExit && depth > 0) || (requireRootExit && depth === 0)) && !committedPrelaunch && !unlaunchedRoot) {
      const pid = record?.transport === "process" && typeof record.sessionId === "string" && /^\d+$/.test(record.sessionId)
        ? Number(record.sessionId) : undefined;
      const worker = depth > 0 ? "descendant" : "root";
      if (pid === undefined || !Number.isSafeInteger(pid) || pid <= 0) return `worker exit is unconfirmed: unknown ${worker} identity`;
      if (processAlive(pid)) return `worker exit is unconfirmed: its ${worker} worker may still be running (${directory})`;
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
    // A parent's populated=0 scope may still contain empty nested cgroups.
    // Retire only checked nested run scopes bottom-up, before parent rmdir.
    // Unknown children/receipts/identities veto the entire parent collection.
    const nested = path.join(directory, "nested");
    let hasNested = false;
    let nestedStat: fs.Stats | undefined;
    try {
      if (!names || names.includes("nested")) { nestedStat = yield* treeStat(nested); hasNested = true; }
    }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
    if (hasNested) {
      if (!nestedStat || !isOwnedStat(nestedStat) || !nestedStat.isDirectory()) return "worker exit is unconfirmed: unsafe nested run directory";
      for (const name of (yield* treeList(nested))) {
        const reason = yield* inspectRunTree(path.join(nested, name), depth + 1, expired, requirePersistedExit, preserveArchives, requireRootExit,
          { disposeScratch: options.disposeScratch !== false }, scratchCoveredByAllowlist, inspections, reuseCensus);
        if (reason) return reason;
      }
    }
    inspections?.set(directory, { root: rootStat, record, statusStat, ...(names ? { names } : {}) });
    // Report a known worker/descendant obligation before the independent
    // scratch fence. Both still have to pass; native exit never bypasses it.
    if (!scratchCoveredByAllowlist) {
      const scratchVeto = runScratchExitVeto(directory, expired, options.disposeScratch);
      if (scratchVeto) return scratchVeto;
    }
  } catch { return "worker exit is unconfirmed: run-tree inspection failed"; }
};
const runTreeVeto = (
  directory: string, depth: number, expired: Deadline, requirePersistedExit: boolean, preserveArchives: boolean,
  requireRootExit: boolean, options: RunTreeExitOptions,
): string | undefined => inspectTreeSync(inspectRunTree(directory, depth, expired, requirePersistedExit, preserveArchives, requireRootExit, options));
const recordAgeReference = (record: RunRecordSummary, fallback: number): number =>
  time(record.finishedAt) ? record.finishedAt : time(record.updatedAt) ? record.updatedAt : fallback;
// Every file the worker and manager write into a run directory. A missing name made the run
// unremovable forever: 54k expired actor runs with reply.json piled up in /tmp (smarty-dev#2010).
const runFiles = new Set([
  "task.txt", "task.txt.provenance.json", "status.json", "events.jsonl", "lifecycle.jsonl", "steer.jsonl", "schema.json", "images.json",
  "reply.json", "relaunches.jsonl", "completion-recipient.json", "route-session.jsonl", "route-dispatch-receipt.json",
  // Native session of an unrouted process Pi task (worker.ts persistentPiTask); owned file only.
  "session.jsonl",
]);
const runFile = (name: string): boolean => runFiles.has(name) || /^oversized-event-prefix(-\d+)?\.txt$/.test(name);
const followUpName = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.json$/;
/** Only paired, owned admission/final-receipt artifacts authorize collection.
 * A passed alarm deadline is not expiry of the payload: the sender may wait.
 * Queued, crashed claims, malformed state, links and unknown contents veto. */
function* inspectFollowUps(directory: string, expired: Deadline): TreeWalk<boolean> {
  const names = (yield* treeList(directory));
  for (const name of names) {
    if (expired()) return false;
    const admissionName = name.endsWith(".settled") ? name.slice(0, -8) : name;
    if (!followUpName.test(admissionName)) return false;
    const file = path.join(directory, admissionName);
    const admission = (yield* treeJson<{ messageId?: unknown; deadlineAt?: unknown }>(file, 4096));
    if (!admission || admission.messageId !== admissionName.slice(0, -5) ||
        !Number.isSafeInteger(admission.deadlineAt) || (admission.deadlineAt as number) < 0 ||
        Object.keys(admission).some(key => key !== "messageId" && key !== "deadlineAt")) return false;
    const settled = file + ".settled";
    if (!(yield* treeOwnedStat(settled))?.isDirectory()) return false;
    const contents = (yield* treeList(settled));
    if (contents.length !== 1 || contents[0] !== "state") return false;
    const stateFile = path.join(settled, "state"), stat = (yield* treeOwnedStat(stateFile));
    if (!stat?.isFile() || stat.size > 9) return false;
    const state = (yield* treeRead(stateFile));
    if (state !== "delivered" && state !== "cancelled") return false;
  }
  return true;
};
/** Unknown transports/contents and live descendants veto removal, even under a dead host. */
function* inspectSafeRunTree(root: string, childrenStopped: boolean, depth = 0, expired: Deadline = noDeadline,
  options: Pick<RunTreeExitOptions, "disposeScratch"> = {}, inspections?: Map<string, RunInspection>): TreeWalk<boolean> {
  if (expired() || depth > 32) return false;
  // Public synchronous proofs keep independent exit and allowlist observations.
  // A reference-preparation cursor can suspend between these predicates; its
  // previous exit/status evidence is not collection authority. Snapshot reuse
  // is reserved for internal collection callers that supply inspections.
  let record: RunRecordSummary | undefined;
  let inspection: RunInspection | undefined;
  if (!inspections) {
    if (!(yield* treeOwnedStat(root))?.isDirectory()) return false;
    if (yield* inspectRunTree(root, 0, expired, true, true, true, options)) return false;
    record = yield* treeJson<RunRecordSummary>(path.join(root, "status.json"));
  } else {
    // Only internal collection callers supply an inspection snapshot.
    inspection = inspections.get(root);
    if (!inspection) return false;
    record = inspection.record;
  }
  // Automatic retention keeps its independent live-writer fence. A mismatched
  // birth identity can clear explicit cleanup's exit veto, but never authorizes
  // a sweep to remove a run with a live or unknown saved PID. Apply this at
  // every level, including descendants, alongside the recursive exit proof.
  const pid = record?.transport === "process" && typeof record.sessionId === "string" && /^\d+$/.test(record.sessionId)
    ? Number(record.sessionId) : undefined;
  if (pid !== undefined && processAlive(pid)) return false;
  if (!record?.status || !TERMINAL_STATUSES.has(record.status)) {
    if (!childrenStopped) return false;
    if (!(yield* treeOwnedStat(path.join(root, "task.txt")))?.isFile()) return false;
  }
  try {
    const names = inspection?.names ?? (yield* treeList(root));
    if (inspection) inspection.names = names;
    for (const name of names) {
      if (expired()) return false;
      const file = path.join(root, name);
      const stat = name === "status.json" && inspection ? inspection.statusStat : yield* treeOwnedStat(file);
      if (!stat || expired()) return false;
      if (stat.isFile() && (runFile(name) || (name === "queued-result.json" && record?.queuedArchiveCommitted === true))) continue;
      if (stat.isDirectory() && name === "handoff-session") {
        // This directory is exclusively populated by Fabric's session fork writer.
        for (const child of yield* treeList(file)) {
          if (expired() || !child.endsWith(".jsonl") || !(yield* treeOwnedStat(path.join(file, child)))?.isFile()) return false;
        }
        continue;
      }
      if (stat.isDirectory() && name === "follow-ups") {
        if (!(yield* inspectFollowUps(file, expired))) return false;
        continue;
      }
      if (stat.isDirectory() && name === "deliveries") {
        // Empty ingress is normal. A final tracked receipt also authorizes
        // its owned envelope: a crash can fall between receipt write and unlink.
        // Anonymous, pending, uncertain, malformed or linked content still vetoes.
        const children = (yield* treeList(file));
        const followUps = path.join(root, "follow-ups");
        if (children.length && (!(yield* treeOwnedStat(followUps))?.isDirectory() || !(yield* inspectFollowUps(followUps, expired)))) return false;
        for (const child of children) {
          if (expired() || !followUpName.test(child) || !(yield* treeOwnedStat(path.join(followUps, child)))?.isFile()) return false;
          const item = (yield* treeJson<{ message?: unknown; delivery?: unknown; followUpId?: unknown; provenance?: unknown }>(path.join(file, child)));
          if (!item || item.delivery !== "followUp" || item.followUpId !== child.slice(0, -5) ||
              typeof item.message !== "string" || !copyFabricProvenance(item.provenance) ||
              Object.keys(item).some(key => !["message", "delivery", "followUpId", "provenance"].includes(key))) return false;
        }
        continue;
      }
      if (stat.isDirectory() && name === "nested") {
        for (const child of (yield* treeList(file))) if (!(yield* inspectSafeRunTree(path.join(file, child), false, depth + 1, expired, options, inspections))) return false;
        continue;
      }
      return false;
    }
    return true;
  } catch { return false; }
};
const safeRunTree = (root: string, childrenStopped: boolean, depth = 0, expired: Deadline = noDeadline,
  options: Pick<RunTreeExitOptions, "disposeScratch"> = {}): boolean =>
  inspectTreeSync(inspectSafeRunTree(root, childrenStopped, depth, expired, options));
/** Explicit resident roots require the same terminal/exit/allowlist proof. */
export const canRemoveTerminalRun = (directory: string, expired: Deadline = noDeadline,
  options: Pick<RunTreeExitOptions, "disposeScratch"> = {}): boolean => {
  // Keep the terminal precondition independent of the exit/allowlist proof on
  // every platform. Reference preparation resumes at predicate boundaries and
  // must still observe deadline/record changes at main's original crossings.
  const record = readJson<RunRecordSummary>(path.join(directory, "status.json"));
  return !!record?.status && TERMINAL_STATUSES.has(record.status) && safeRunTree(directory, false, 0, expired, options);
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
  eventsRetention: TerminalRunEventsRetention,
): string[] => {
  const removed: string[] = [];
  // Every run started after its root, so no run of a root younger than the shortest retention is due.
  if (now - owner.startedAt < Math.min(orphanMs, oneShotMs, eventsRetention.terminalRunEventsAgeMs ?? 6 * 60 * 60 * 1_000)) return removed;
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
    if (now - reference < retention) {
      compactTerminalRunEvents(directory, { ...eventsRetention, now, expired });
      continue;
    }
    if (!processAlive(owner.pid)) recoverActorRunArchives(directory, expired);
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
export interface TempRunSweepRequest extends TerminalRunEventsRetention {
  tempRoot: string;
  currentRoot?: string;
  orphanedTempRunRetentionMs: number;
  oneShotRunRetentionMs: number;
}
export const sweepTempRunRoots = (options: TempRunSweepRequest & {
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
        root, owner, options.orphanedTempRunRetentionMs, options.oneShotRunRetentionMs, now, expired, options,
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
      recoverActorRunArchives(directory, expired);
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

const TERMINAL_EVENT_TAIL_LINES = 200;
const EVENT_TAIL_MARKER = Buffer.from('{"fabricTruncated":true,"reason":"terminal run event retention"}\n');

/** Compact only an owned, safely terminal run. Read a bounded suffix, keep complete JSONL
 * lines (at most the last 200), and atomically replace only events.jsonl; status/reply/result remain byte-for-byte.
 * The marker counts against the byte cap. A single oversized final event may leave only
 * the marker rather than a corrupt JSON fragment. Already bounded logs are never rewritten.
 * Callers retain their ownership/latest-run vetoes before entering this shared predicate. */
type TerminalRunCompactionOptions = TerminalRunEventsRetention & {
  now?: number; expired?: Deadline; isRetained?: () => boolean; dryRun?: boolean;
  onCompact?: (change: { path: string; beforeBytes: number; afterBytes: number }) => void;
};
function* inspectCompactionTree(directory: string, expired: Deadline, scratchCoveredByAllowlist: boolean): TreeWalk<boolean> {
  const options = { disposeScratch: false };
  if (process.platform === "win32" && !scratchCoveredByAllowlist) {
    // Synchronous Windows compaction keeps main's independent exit, terminal
    // wrapper and allowlist reads. Only the explicitly async archive walk below
    // uses a within-slice inspection snapshot; neither path disposes scratch.
    if (yield* inspectRunTree(directory, 0, expired, true, true, true, options)) return false;
    const record = yield* treeJson<RunRecordSummary>(path.join(directory, "status.json"));
    return !!record?.status && TERMINAL_STATUSES.has(record.status) &&
      (yield* inspectSafeRunTree(directory, false, 0, expired, options));
  }
  const inspections = new Map<string, RunInspection>();
  // Async Windows inspection never enters the synchronous scratch API. The
  // recursive artifact allowlist immediately following this exit proof vetoes
  // every scratch directory/fence/receipt instead; no disposal is authorized.
  if (yield* inspectRunTree(directory, 0, expired, true, true, true, options, scratchCoveredByAllowlist, inspections)) return false;
  return yield* inspectSafeRunTree(directory, false, 0, expired, options, inspections);
}
function* inspectTerminalRunEvents(directory: string, options: TerminalRunCompactionOptions,
  scratchCoveredByAllowlist = false): TreeWalk<boolean> {
  const now = options.now ?? Date.now();
  const ageMs = options.terminalRunEventsAgeMs ?? 6 * 60 * 60 * 1_000;
  const maxBytes = options.terminalRunEventsMaxBytes ?? 256 * 1024;
  const expired = options.expired ?? noDeadline;
  // Compaction is observation until atomic replacement, including dry-run and
  // already-bounded no-ops. Scratch disposal belongs to collection, never to
  // these guards: even a failed disposal can alter the directory's TTL clock.
  const directoryStat = yield* treeOwnedStat(directory);
  if (!Number.isSafeInteger(ageMs) || ageMs < 0 || !Number.isSafeInteger(maxBytes) ||
      maxBytes < EVENT_TAIL_MARKER.length || expired() || options.isRetained?.() || !directoryStat?.isDirectory()) return false;
  const record = (yield* treeJson<RunRecordSummary>(path.join(directory, "status.json")));
  if (!record?.status || !TERMINAL_STATUSES.has(record.status) ||
      now - recordAgeReference(record, (yield* treeOwnedStat(directory))?.mtimeMs ?? now) < ageMs) return false;
  const file = path.join(directory, "events.jsonl");
  const stat = yield* treeOwnedStat(file);
  if (!stat?.isFile() || stat.size === 0 || (!retentionV2Enabled() &&
      !(yield* inspectCompactionTree(directory, expired, scratchCoveredByAllowlist)))) return false;
  try {
    const fd = yield* treeOpen(file);
    let tail: Buffer;
    try {
      const opened = yield* treeHandleStat(fd);
      if (opened.dev !== stat.dev || opened.ino !== stat.ino || opened.size !== stat.size ||
          opened.mtimeMs !== stat.mtimeMs) return false;
      // Establish whether the original satisfies both limits before reserving
      // marker bytes. Reading at most the cap preserves near-limit unmarked logs
      // and still includes the look-behind needed for the smaller marked tail.
      const length = Math.min(stat.size, maxBytes);
      tail = Buffer.alloc(length);
      let read = 0;
      while (read < length) {
        if (expired()) return false;
        const count = yield* treeTail(fd, tail, read, length - read, stat.size - length + read);
        if (count === 0) return false;
        read += count;
      }
    } finally { yield* treeClose(fd); }
    // Skip a partial first line only when we did not read from the start.
    let retained = tail;
    if (tail.length < stat.size) {
      const newline = tail.indexOf(0x0a);
      retained = newline < 0 ? Buffer.alloc(0) : tail.subarray(newline + 1);
    } else if (tail.subarray(0, EVENT_TAIL_MARKER.length).equals(EVENT_TAIL_MARKER)) {
      retained = tail.subarray(EVENT_TAIL_MARKER.length);
    }
    let lines = 0;
    for (let index = retained.length - 1; index >= 0; index--) {
      if (retained[index] === 0x0a && index !== retained.length - 1 && ++lines === TERMINAL_EVENT_TAIL_LINES) {
        retained = retained.subarray(index + 1);
        break;
      }
    }
    // A full unmarked log of <=200 events, or our already bounded tail, is a no-op.
    if (stat.size <= maxBytes && (retained.length === stat.size ||
        (tail.length === stat.size && stat.size === EVENT_TAIL_MARKER.length + retained.length &&
         tail.subarray(0, EVENT_TAIL_MARKER.length).equals(EVENT_TAIL_MARKER)))) return false;
    // Reading an unchanged bounded suffix grants no mutation authority. Avoid
    // four redundant status reads for that no-op (native directory order can
    // put the historical prefix inside the resident run phase).
    // A real replacement still requires both independent fresh safety walks
    // in inspectCompactionTree below, including this PR's scratch custody veto.
    // Truncation is now known to be necessary. Reserve the marker's space and
    // drop only complete prefix lines; look behind by one byte so an exactly
    // aligned final event is kept. An oversized final event leaves only a marker.
    const eventBytes = maxBytes - EVENT_TAIL_MARKER.length;
    if (retained.length > eventBytes) {
      const newline = retained.indexOf(0x0a, retained.length - eventBytes - 1);
      retained = newline < 0 ? Buffer.alloc(0) : retained.subarray(newline + 1);
    }
    const checked = yield* treeOwnedStat(file);
    if (!checked || checked.dev !== stat.dev || checked.ino !== stat.ino ||
        checked.size !== stat.size || checked.mtimeMs !== stat.mtimeMs ||
        !(yield* inspectCompactionTree(directory, expired, scratchCoveredByAllowlist)) ||
        expired() || options.isRetained?.()) return false;
    if (!options.dryRun) {
      try { yield* treeWrite(file, Buffer.concat([EVENT_TAIL_MARKER, retained])); }
      finally {
        // Residency expiry (and legacy timestamp fallback) uses directory mtime.
        // Creating/renaming the atomic tail must not restart that retention clock.
        const current = yield* treeOwnedStat(directory);
        if (current?.dev === directoryStat.dev && current.ino === directoryStat.ino) {
          try { yield* treeTimes(directory, directoryStat.atimeMs / 1000, directoryStat.mtimeMs / 1000); }
          catch { /* A failed timestamp restore only delays collection; it cannot authorize it. */ }
        }
      }
    }
    options.onCompact?.({ path: file, beforeBytes: stat.size, afterBytes: EVENT_TAIL_MARKER.length + retained.length });
    return true;
  } catch { return false; }
};

export const compactTerminalRunEvents = (directory: string, options: TerminalRunCompactionOptions = {}): boolean =>
  inspectTreeSync(inspectTerminalRunEvents(directory, options));

/** Immutable, ordinary rotation history only. Malformed-session orphan backups are
 * recovery evidence, not rotation history, and keep their existing exemption. */
export const pruneActorSessionBackups = (sessionFile: string, options: {
  dryRun?: boolean; onPrune?: (change: { path: string; bytes: number }) => void;
} = {}): string[] => {
  const directory = path.dirname(sessionFile);
  if (!ownedStat(directory)?.isDirectory()) return [];
  const prefix = `${path.basename(sessionFile)}.`;
  try {
    const backups = fs.readdirSync(directory).flatMap(name => {
      if (!name.startsWith(prefix)) return [];
      const match = /^(\d{8}T\d{9}Z)(?:-(\d+))?\.bak$/.exec(name.slice(prefix.length));
      const stat = ownedStat(path.join(directory, name));
      return match && stat?.isFile() ? [{ name, stamp: match[1]!, suffix: Number(match[2] ?? 0), stat }] : [];
    }).sort((a, b) => a.stamp.localeCompare(b.stamp) || a.suffix - b.suffix);
    const removed: string[] = [];
    for (const backup of backups.slice(0, -1)) {
      const file = path.join(directory, backup.name);
      const checked = ownedStat(file);
      if (!checked || checked.dev !== backup.stat.dev || checked.ino !== backup.stat.ino ||
          checked.size !== backup.stat.size || checked.mtimeMs !== backup.stat.mtimeMs) continue;
      if (!options.dryRun) fs.unlinkSync(file);
      options.onPrune?.({ path: file, bytes: backup.stat.size });
      removed.push(file);
    }
    return removed;
  } catch { return []; }
};

interface ActorRunArchivePruneOptions {
  runsDirectory: string;
  latestRunId?: string;
  retainRun?: (runId: string) => boolean;
  retentionMs: number;
  terminalRunEventsAgeMs?: number;
  terminalRunEventsMaxBytes?: number;
  now?: number;
}

// Cooperatively cap Windows metadata walks, including large descendant trees.
// A timed-out candidate is retained and retried by a later sweep, with fresh
// evidence. A single native synchronous IO call cannot be preempted here; the
// production Windows queue below awaits IO rather than blocking the RPC turn.
const WINDOWS_ARCHIVE_INSPECTION_BUDGET_MS = 25;

/** One run-tree inspection/deletion per resume. The caller must refresh its
 * ownership/publication fence before each next(), never carry it across a yield. */
export function* pruneActorRunArchiveSlices(options: ActorRunArchivePruneOptions): Generator<void, string[]> {
  const now = options.now ?? Date.now();
  const removed: string[] = [];
  if (!ownedStat(options.runsDirectory)?.isDirectory()) return removed;
  let entries: fs.Dirent[];
  try { entries = fs.readdirSync(options.runsDirectory, { withFileTypes: true }); } catch { return removed; }
  for (const entry of entries) {
    yield; // Also bound scans of latest, malformed and live entries.
    if (!entry.isDirectory() || entry.name === options.latestRunId || options.retainRun?.(entry.name)) continue;
    const directory = path.join(options.runsDirectory, entry.name);
    const windows = process.platform === "win32";
    if (windows) logWindowsScratchScopeCut();
    // Reuse metadata/status only within this uninterrupted collection slice,
    // never across the generator yield or an asynchronous filesystem crossing.
    // Windows' recursive allowlist rejects every scratch artifact, so it needs
    // no scratch inspection/disposal (including negative stats). Public exit
    // and resource predicates keep their independent fresh observations.
    const deadline = windows ? performance.now() + WINDOWS_ARCHIVE_INSPECTION_BUDGET_MS : Infinity;
    const expired = windows ? () => performance.now() >= deadline : noDeadline;
    const inspections = new Map<string, RunInspection>();
    if (inspectTreeSync(inspectRunTree(directory, 0, expired, true, true, true, {}, windows, inspections, windows)) ||
        !inspectTreeSync(inspectSafeRunTree(directory, false, 0, expired, {}, inspections)) || expired()) continue;
    const { record, root } = inspections.get(directory)!;
    if (!record?.status || !TERMINAL_STATUSES.has(record.status)) continue;
    if (now - recordAgeReference(record, root?.mtimeMs ?? now) < options.retentionMs) {
      compactTerminalRunEvents(directory, { ...options, now, expired, isRetained: () => options.retainRun?.(entry.name) ?? false });
      continue;
    }
    try { fs.rmSync(directory, { recursive: true, force: true }); removed.push(directory); } catch {}
  }
  return removed;
}

/** Windows' bounded queue: one in-flight run, every filesystem crossing awaited.
 * No scratch disposal or synchronous compaction on the RPC slice. Event-tail
 * compaction uses the same shared policy and awaited IO driver too.
 * Ownership/publication/latestRunId are refreshed after IO,
 * immediately before removal, never cached across the asynchronous inspection. */
export const pruneActorRunArchivesAsync = async (options: ActorRunArchivePruneOptions, canPrune: () => boolean): Promise<string[]> => {
  const removed: string[] = [], now = options.now ?? Date.now();
  if (!canPrune() || !(await inspectTreeAsync(treeOwnedStat(options.runsDirectory)))?.isDirectory()) return removed;
  let entries: fs.Dirent[];
  try { entries = await fs.promises.readdir(options.runsDirectory, { withFileTypes: true }); } catch { return removed; }
  for (const entry of entries) {
    await new Promise<void>(resolve => setImmediate(resolve));
    if (!canPrune()) return removed;
    if (!entry.isDirectory() || entry.name === options.latestRunId || options.retainRun?.(entry.name)) continue;
    const directory = path.join(options.runsDirectory, entry.name), inspections = new Map<string, RunInspection>();
    const expired = () => !!options.retainRun?.(entry.name);
    if (await inspectTreeAsync(inspectRunTree(directory, 0, expired, true, true, true, { disposeScratch: false }, true, inspections)) ||
        !await inspectTreeAsync(inspectSafeRunTree(directory, false, 0, expired, { disposeScratch: false }, inspections))) continue;
    const { record, root } = inspections.get(directory)!;
    if (!record?.status || !TERMINAL_STATUSES.has(record.status)) continue;
    if (now - recordAgeReference(record, root.mtimeMs) < options.retentionMs) {
      await inspectTreeAsync(inspectTerminalRunEvents(directory, { ...options, now,
        expired: () => !canPrune(), isRetained: () => !!options.retainRun?.(entry.name) }, true));
      continue;
    }
    // Async IO allows other work between crossings. Refuse changed namespaces or
    // status records, not only a changed host authority, before starting deletion.
    let unchanged = true;
    for (const [run, checked] of inspections) {
      for (const [file, before] of [[run, checked.root], [path.join(run, "status.json"), checked.statusStat]] as const) {
        const after = await inspectTreeAsync(treeOwnedStat(file));
        if (!before || !after || before.dev !== after.dev || before.ino !== after.ino ||
            before.mtimeMs !== after.mtimeMs || before.ctimeMs !== after.ctimeMs || before.size !== after.size) { unchanged = false; break; }
      }
      if (!unchanged) break;
      // Directory timestamps can be coarse/unchanged after a late insertion
      // (including tmp). Recheck the accepted names after the asynchronous
      // metadata crossings, not just mtime/ctime, before deleting this tree.
      try {
        const names = await fs.promises.readdir(run), accepted = new Set(checked.names);
        if (!checked.names || names.length !== checked.names.length || names.some(name => !accepted.has(name))) { unchanged = false; break; }
      } catch { unchanged = false; break; }
    }
    if (!unchanged || expired() || !canPrune()) continue;
    try { await fs.promises.rm(directory, { recursive: true, force: true }); removed.push(directory); } catch { /* retain on failed removal */ }
  }
  return removed;
};

/** Synchronous callers keep the original behavior and return value. */
export const pruneActorRunArchives = (options: ActorRunArchivePruneOptions): string[] => {
  const slices = pruneActorRunArchiveSlices(options);
  for (;;) {
    const step = slices.next();
    if (step.done) return step.value;
  }
};
