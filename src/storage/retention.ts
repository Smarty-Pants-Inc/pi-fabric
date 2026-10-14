import fs from "node:fs";
import { retentionV2Enabled } from "./retention-platform.js";
import path from "node:path";
import { writeFileAtomic, writeJsonAtomic } from "../core/atomic-write.js";
import { ownedStat, processAlive } from "./scratch.js";
import { boundedRunTree } from "./reference-scan.js";
import { countRefusal, removalTallyOn, verifiedRemove } from "./verified-remove.js";
import { processStartTime } from "../residency/process-identity.js";
import { FileLockBusy, lockFile } from "../residency/file-lock.js";
import { recoverActorRunArchives } from "../actors/child-completions.js";
import { recoverEnodevTaskRun } from "../agents/enodev-recovery.js";
import { copyFabricProvenance } from "../fabric-provenance.js";
import { readFinalAnswerReceipt } from "../worker/terminal-answer.js";
import { readTerminalAdmission, terminalAdmissionFromEntry, terminalControlMatchesAdmission, terminalSenderMatches, type TerminalAdmission } from "../agents/terminal-notices.js";

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
  id?: string;
  finalAnswerReceipt?: { id: string; recordedAt: number };
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
/**
 * Automatic retention's live-writer fence. By default any live PID vetoes, whatever its saved
 * birth identity: routed workers, temp run roots and every pre-existing caller keep the strict
 * live veto (pi-fabric#645 CI: a routed run naming a live process was swept). Only the mesh-wide
 * sweep opts into `acceptPidReuse` (smarty-dev#3252): there a live PID is the run's saved worker
 * unless its checked start identity proves reuse (the saved start time was read and differs from
 * the live process's). A missing saved identity or an unreadable current one still counts as live.
 */
const savedWriterAlive = (pid: number, record: RunRecordSummary | undefined, acceptPidReuse = false): boolean => {
  if (!processAlive(pid)) return false;
  if (!acceptPidReuse) return true;
  const saved = typeof record?.processStartTime === "string" && /^\d+$/.test(record.processStartTime)
    ? record.processStartTime : undefined;
  if (saved === undefined) return true;
  const current = processStartTime(pid);
  return current === undefined || current === saved;
};
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
const TASK_FINAL_ANSWER_LOCK = ".task-final-answer.lock";
const TASK_FINAL_ANSWER_LOCK_STALE_MS = 30_000;
/** The final-answer writer's exact mkdir-lock shape. Ownerless, malformed, young, linked, or live locks veto retention. */
const safeStaleFinalAnswerLock = (directory: string): boolean => {
  if (!ownedStat(directory)?.isDirectory()) return false;
  try {
    const entries = fs.readdirSync(directory);
    if (entries.length !== 1 || entries[0] !== "owner") return false;
    const ownerFile = path.join(directory, "owner");
    const stat = ownedStat(ownerFile);
    if (!stat?.isFile() || stat.size > 256) return false;
    const text = fs.readFileSync(ownerFile, "utf8");
    const match = /^([0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})\n([1-9][0-9]*)\n([0-9]+)\n$/i.exec(text);
    if (!match) return false;
    const pid = Number(match[2]), createdAt = Number(match[3]);
    return Number.isSafeInteger(pid) && pid > 0 && Number.isSafeInteger(createdAt) && createdAt >= 0 &&
      Date.now() - createdAt > TASK_FINAL_ANSWER_LOCK_STALE_MS && !processAlive(pid);
  } catch { return false; }
};
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
export const runTreeExitVeto = (
  directory: string, depth = 0, expired: Deadline = noDeadline, requirePersistedExit = false, acceptPidReuse = false,
): string | undefined => runTreeVeto(directory, depth, expired, requirePersistedExit, true, requirePersistedExit, acceptPidReuse);
/** Native resource safety is independent of full-result archival. This only
 * authorizes pre-launch worktree rollback or isolation of shutdown obligations;
 * its tracked root has native transport custody, but descendants need saved exit
 * proof. Run-file collection must use runTreeExitVeto with its archive/root fence. */
export const runTreeResourceVeto = (
  directory: string, depth = 0, expired: Deadline = noDeadline, requireDescendantExit = false, requireRootExit = false,
): string | undefined => runTreeVeto(directory, depth, expired, requireDescendantExit, false, requireRootExit);
const runTreeVeto = (
  directory: string, depth: number, expired: Deadline, requirePersistedExit: boolean, preserveArchives: boolean, requireRootExit: boolean,
  acceptPidReuse = false,
): string | undefined => {
  if (expired() || depth > 32) return "worker exit is unconfirmed: run-tree inspection was incomplete";
  // A previously removed tree has no worker files left to collect. Only this
  // initial absence is safe; errors or changes during inspection veto cleanup.
  try { fs.lstatSync(directory); }
  catch (error) {
    return (error as NodeJS.ErrnoException).code === "ENOENT" && !requirePersistedExit
      ? undefined : "worker exit is unconfirmed: run-tree inspection failed";
  }
  try {
    if (!ownedStat(directory)?.isDirectory()) return "worker exit is unconfirmed: unsafe run directory";
    if (expired()) return "worker exit is unconfirmed: run-tree inspection was incomplete";
    if (preserveArchives && fs.existsSync(path.join(directory, "archive-pending.json"))) return "terminal result archive is pending";
    if (expired()) return "worker exit is unconfirmed: run-tree inspection was incomplete";
    if (preserveArchives && fs.existsSync(path.join(directory, "actor-run-archive-pending.json"))) return "actor run receipt archive is pending";
    if (expired()) return "worker exit is unconfirmed: run-tree inspection was incomplete";
    if (fs.existsSync(path.join(directory, UNRESOLVED_WORKER_FILE))) return "its worker may still be running (unresolved worker marker)";
    if (expired()) return "worker exit is unconfirmed: run-tree inspection was incomplete";
    const statusFile = path.join(directory, "status.json");
    const record = readJson<RunRecordSummary>(statusFile);
    if (expired()) return "worker exit is unconfirmed: run-tree inspection was incomplete";
    if (fs.existsSync(statusFile) && !record) return "worker exit is unconfirmed: unreadable run record";
    if (record?.cleanupPending !== undefined && record.cleanupPending !== false) return "worker cleanup is not joined";
    if (record?.transport === "tmux" || record?.transport === "screen") {
      return `${record.transport} transport has no checked worker exit receipt (${directory})`;
    }
    // Offline compaction/collection has no surviving transport handle. Require
    // the saved root identity too; a free host flock, terminal status, exitCode,
    // or absent unresolved marker does not prove the root writer has exited.
    // Recordless pre-launch rollback uses the non-retention mode explicitly;
    // missing persisted status is never evidence for an admitted worker.
    // A committed queued archive is explicit never-launched admission evidence,
    // not an unknown legacy tree. Its durable marker permits later collection
    // after best-effort deletion failed, but never exempts any descendant.
    const committedPrelaunch = depth === 0 && record?.queuedArchiveCommitted === true &&
      record.transport === undefined && !!record.status && TERMINAL_STATUSES.has(record.status);
    if (((requirePersistedExit && depth > 0) || (requireRootExit && depth === 0)) && !committedPrelaunch) {
      const pid = record?.transport === "process" && typeof record.sessionId === "string" && /^\d+$/.test(record.sessionId)
        ? Number(record.sessionId) : undefined;
      const worker = depth > 0 ? "descendant" : "root";
      if (pid === undefined || !Number.isSafeInteger(pid) || pid <= 0) return `worker exit is unconfirmed: unknown ${worker} identity`;
      if (savedWriterAlive(pid, record, acceptPidReuse)) return `worker exit is unconfirmed: its ${worker} worker may still be running (${directory})`;
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
      // Explicit cleanup's own PID-reuse proof (pre-existing); the persisted-exit fence above stays strict.
      if (!validPid || (alive && savedWriterAlive(pid, record, true))) {
        return `worker exit is unconfirmed: saved process identity is live or unknown (${directory})`;
      }
    }
    const nested = path.join(directory, "nested");
    try { fs.lstatSync(nested); }
    catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return; throw error; }
    if (!ownedStat(nested)?.isDirectory()) return "worker exit is unconfirmed: unsafe nested run directory";
    for (const name of fs.readdirSync(nested)) {
      const reason = runTreeVeto(path.join(nested, name), depth + 1, expired, requirePersistedExit, preserveArchives, requireRootExit, acceptPidReuse);
      if (reason) return reason;
    }
  } catch { return "worker exit is unconfirmed: run-tree inspection failed"; }
};
// Native receipt completion precedes teardown. An exit-only status rewrite
// must not restart retention; deletion still requires receipt and exit checks.
const recordAgeReference = (record: RunRecordSummary, fallback: number): number =>
  time(record.finalAnswerReceipt?.recordedAt) ? record.finalAnswerReceipt.recordedAt :
    time(record.finishedAt) ? record.finishedAt : time(record.updatedAt) ? record.updatedAt : fallback;
// Every file the worker and manager write into a run directory. A missing name made the run
// unremovable forever: 54k expired actor runs with reply.json piled up in /tmp (smarty-dev#2010).
const runFiles = new Set([
  "task.txt", "task.txt.provenance.json", "status.json", "events.jsonl", "lifecycle.jsonl", "steer.jsonl", "schema.json", "images.json",
  // Manager-owned exact settlement and byte-evicted legacy nested history.
  // Neither artifact substitutes for the worker status/receipt/exit proof.
  "manager-result.json", "manager-nested.json",
  "reply.json", "relaunches.jsonl", "completion-recipient.json", "route-session.jsonl", "route-dispatch-receipt.json",
  // Native session of an unrouted process Pi task (worker.ts persistentPiTask); owned file only.
  "session.jsonl",
  // The Pi worker's settlement receipt (worker.ts savePiSettlementReceipt), written for every completed process run:
  // without it no such run was ever collected (smarty-dev#9855).
  "settlement.json",
]);
const runFile = (name: string): boolean => runFiles.has(name) || /^oversized-event-prefix(-\d+)?\.txt$/.test(name);
const followUpName = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.json$/;
/** Only paired, owned admission/final-receipt artifacts authorize collection.
 * A passed alarm deadline is not expiry of the payload: the sender may wait.
 * Queued, crashed claims, malformed state, links and unknown contents veto. */
const safeFollowUps = (directory: string, expired: Deadline): boolean => {
  const names = fs.readdirSync(directory);
  for (const name of names) {
    if (expired()) return false;
    const admissionName = name.endsWith(".settled") ? name.slice(0, -8) : name;
    if (!followUpName.test(admissionName)) return false;
    const file = path.join(directory, admissionName);
    const admission = readJson<{ messageId?: unknown; deadlineAt?: unknown }>(file, 4096);
    if (!admission || admission.messageId !== admissionName.slice(0, -5) ||
        !Number.isSafeInteger(admission.deadlineAt) || (admission.deadlineAt as number) < 0 ||
        Object.keys(admission).some(key => key !== "messageId" && key !== "deadlineAt")) return false;
    const settled = file + ".settled";
    if (!ownedStat(settled)?.isDirectory()) return false;
    const contents = fs.readdirSync(settled);
    if (contents.length !== 1 || contents[0] !== "state") return false;
    const stateFile = path.join(settled, "state"), stat = ownedStat(stateFile);
    if (!stat?.isFile() || stat.size > 9) return false;
    const state = fs.readFileSync(stateFile, "utf8");
    if (state !== "delivered" && state !== "cancelled") return false;
  }
  return true;
};
/** A refusal journal is not a sender route receipt. Every unconsumed admission
 * needs its retained delivered notice; pending, malformed and linked artifacts veto. */
const safeTerminalArtifacts = (root: string, expired: Deadline): boolean => {
  const answerFile = path.join(root, "final-answer.json");
  const controls = path.join(root, "terminal-controls"), notices = path.join(root, "terminal-notices");
  // Read original native admissions before trusting any worker consumption row.
  const admissions = new Map<string, TerminalAdmission>();
  const steering = path.join(root, "steer.jsonl");
  if (fs.existsSync(steering)) {
    if (!ownedStat(steering)?.isFile()) return false;
    for (const line of fs.readFileSync(steering, "utf8").split("\n")) {
      if (expired()) return false;
      if (!line.trim()) continue;
      const entry = JSON.parse(line);
      if (entry?.type !== "steer" && entry?.type !== "follow_up") continue;
      const admission = terminalAdmissionFromEntry(entry);
      if (!admission || admissions.has(admission.id)) return false;
      admissions.set(admission.id, admission);
    }
  }
  if (!fs.existsSync(answerFile)) {
    if (fs.existsSync(notices) && (!ownedStat(notices)?.isDirectory() || fs.readdirSync(notices).length > 0)) return false;
    if (!fs.existsSync(controls)) return true;
    if (!ownedStat(controls)?.isDirectory()) return false;
    // A failed/interrupted task need not have a final answer. Already consumed
    // controls remain known collectible artifacts; queued/unknown ones do not.
    return fs.readdirSync(controls).every(name => {
      if (expired() || !/^[a-zA-Z0-9_-]{1,200}\.json$/.test(name)) return false;
      const control = readJson<{ id?: string; state?: string; delivery?: string; provenance?: unknown; forwarded?: unknown }>(path.join(controls, name));
      return !!control && control.state === "delivered" &&
        (control.forwarded === undefined || typeof control.forwarded === "boolean") &&
        Object.keys(control).every(key => ["id", "state", "delivery", "provenance", "forwarded"].includes(key)) &&
        terminalControlMatchesAdmission(control, admissions.get(name.slice(0, -5)));
    });
  }
  if (!ownedStat(answerFile)?.isFile()) return false;
  const record = readJson<RunRecordSummary>(path.join(root, "status.json"));
  // Retained trees can be copied under archive aliases. Bind the receipt to
  // the original run identity, not its collection path; controls still require
  // their exact original admission below.
  const answer = typeof record?.id === "string" ? readFinalAnswerReceipt(root, record.id) : undefined;
  if (!answer || expired() || record?.id !== answer.runId || record.finalAnswerReceipt?.id !== answer.id ||
      record.finalAnswerReceipt.recordedAt !== answer.recordedAt || record.actorId) return false;
  const routed = new Set<string>();
  if (fs.existsSync(notices)) {
    if (!ownedStat(notices)?.isDirectory()) return false;
    for (const name of fs.readdirSync(notices)) {
      if (expired() || !/^[a-zA-Z0-9_-]{1,200}\.json(?:\.delivered)?$/.test(name)) return false;
      const deliveredName = name.endsWith(".delivered") ? name : `${name}.delivered`;
      const notice = readJson<{ code?: string; targetId?: string; messageId?: string; delivery?: string; finalAnswerReceiptId?: string; sender?: { id?: string; kind?: string; verified?: string } }>(path.join(notices, deliveredName));
      if (!notice || notice.code !== "FABRIC_TARGET_TERMINAL" || notice.targetId !== answer.runId ||
          notice.finalAnswerReceiptId !== answer.id || deliveredName !== `${notice.messageId}.json.delivered` ||
          !["steer", "followUp"].includes(notice.delivery ?? "") || !notice.sender?.id ||
          !["mesh", "bridge"].includes(notice.sender.verified ?? "") || !["main", "actor", "agent", "remote"].includes(notice.sender.kind ?? "")) return false;
      const admission = admissions.get(notice.messageId!);
      if (!admission || notice.delivery !== admission.delivery || !terminalSenderMatches(notice.sender, admission.sender)) return false;
      if (!name.endsWith(".delivered")) {
        const pending = readJson<unknown>(path.join(notices, name));
        if (JSON.stringify(pending) !== JSON.stringify(notice)) return false;
      }
      routed.add(notice.messageId!);
    }
  }
  const consumed = new Set<string>();
  if (fs.existsSync(controls)) {
    if (!ownedStat(controls)?.isDirectory()) return false;
    for (const name of fs.readdirSync(controls)) {
      if (expired() || !/^[a-zA-Z0-9_-]{1,200}\.json$/.test(name)) return false;
      const control = readJson<{ id?: string; state?: string; delivery?: string; provenance?: unknown; forwarded?: unknown }>(path.join(controls, name));
      const id = name.slice(0, -5), admission = admissions.get(id);
      if (!control || !admission || typeof control.id !== "string" || !/^[a-zA-Z0-9_-]{1,200}$/.test(control.id) ||
          !["steer", "followUp"].includes(control.delivery ?? "") || !["queued", "refused", "delivered"].includes(control.state ?? "") ||
          Object.keys(control).some(key => !["id", "state", "delivery", "provenance", "forwarded"].includes(key)) ||
          control.forwarded !== undefined && typeof control.forwarded !== "boolean" ||
          control.provenance !== undefined && !copyFabricProvenance(control.provenance)) return false;
      // A previously fabricated route receipt cannot discharge an ambiguous
      // forwarded frame, nor a still-queued legacy row without transport proof.
      if (control.state === "queued") return false;
      // Legacy shape (no `forwarded`): a refused row may hide a buffered, consumed
      // frame, so even an old route receipt cannot retire it. Delivered stays allowed.
      if (control.forwarded === undefined && control.state !== "delivered") return false;
      if (control.state === "delivered" && terminalControlMatchesAdmission(control, admission)) consumed.add(id);
      else if (!routed.has(id)) return false; // Only the original-attributed refusal discharges mismatched rows.
    }
  }
  for (const id of admissions.keys()) {
    if (expired() || !consumed.has(id) && !routed.has(id)) return false;
  }
  return true;
};
const DURABLE_KEY = /^(?:initial|[0-9a-f]{64})$/;
const DURABLE_LEASE_FILES = new Set(["lease.sqlite", "lease.sqlite-journal"]);
const DURABLE_STORE_FILE = /^(?:main|(?:doc|task)-(?:0|[1-9]\d*))\.jsonl(?:\.reclaim)?$/;
const onlyOwnedFiles = (directory: string, allowed: (name: string) => boolean, expired: Deadline): boolean =>
  !expired() && fs.readdirSync(directory).every((name) => !expired() && allowed(name) && ownedStat(path.join(directory, name))?.isFile() === true) && !expired();
/** Only the known pi-durable journal layout is an artifact; custody still decides removal. */
const safeDurableTree = (directory: string, expired: Deadline): boolean => !expired() && fs.readdirSync(directory).every((key) => {
  const conversation = path.join(directory, key);
  if (expired() || !DURABLE_KEY.test(key) || !ownedStat(conversation)?.isDirectory()) return false;
  return fs.readdirSync(conversation).every((name) => {
    if (expired()) return false;
    const file = path.join(conversation, name);
    if (name === "store") return ownedStat(file)?.isDirectory() === true && onlyOwnedFiles(file, (child) => DURABLE_STORE_FILE.test(child), expired);
    return DURABLE_LEASE_FILES.has(name) && ownedStat(file)?.isFile() === true;
  });
}) && !expired();
/** Unknown transports/contents and live descendants veto removal, even under a dead host. */
const safeRunTree = (root: string, childrenStopped: boolean, depth = 0, expired: Deadline = noDeadline, acceptPidReuse = false): boolean => {
  if (expired() || depth > 32 || !ownedStat(root)?.isDirectory()) return false;
  // Offline collection cannot establish never-launched custody from filenames
  // or a host-wide childrenStopped marker. Only the live admission caller can
  // authorize recordless pre-launch rollback through the non-retention mode.
  if (runTreeExitVeto(root, 0, expired, true, acceptPidReuse)) return false;
  const record = readJson<RunRecordSummary>(path.join(root, "status.json"));
  // Automatic retention keeps its independent live-writer fence at every level,
  // including descendants, alongside the recursive exit proof. A live PID vetoes;
  // only the mesh-wide sweep's opt-in accepts checked PID reuse (smarty-dev#3252).
  // Unknown identity always vetoes.
  const pid = record?.transport === "process" && typeof record.sessionId === "string" && /^\d+$/.test(record.sessionId)
    ? Number(record.sessionId) : undefined;
  if (pid !== undefined && savedWriterAlive(pid, record, acceptPidReuse)) return false;
  if (!record?.status || !TERMINAL_STATUSES.has(record.status)) {
    if (!childrenStopped) return false;
    if (!ownedStat(path.join(root, "task.txt"))?.isFile()) return false;
  }
  try {
    if (!safeTerminalArtifacts(root, expired)) return false;
    for (const name of fs.readdirSync(root)) {
      if (expired()) return false;
      const file = path.join(root, name);
      const stat = ownedStat(file);
      if (!stat) return false;
      if (stat.isFile() && (runFile(name) || name === "final-answer.json" || (name === "queued-result.json" && record?.queuedArchiveCommitted === true))) continue;
      if (stat.isDirectory() && name === TASK_FINAL_ANSWER_LOCK) {
        if (!record?.status || !TERMINAL_STATUSES.has(record.status) || !safeStaleFinalAnswerLock(file)) return false;
        continue;
      }
      if (stat.isDirectory() && (name === "terminal-controls" || name === "terminal-notices")) continue;
      if (stat.isDirectory() && name === "tmp") {
        // Manager-created task scratch belongs to this run. Reuse the owned,
        // bounded walk: links, hardlinks, special files or incomplete scans veto.
        if (!boundedRunTree(file, expired)) return false;
        continue;
      }
      if (stat.isDirectory() && name === "handoff-session") {
        // This directory is exclusively populated by Fabric's session fork writer.
        if (fs.readdirSync(file).some((child) => !child.endsWith(".jsonl") || !ownedStat(path.join(file, child))?.isFile())) return false;
        continue;
      }
      if (stat.isDirectory() && name === "follow-ups") {
        if (!safeFollowUps(file, expired)) return false;
        continue;
      }
      if (stat.isDirectory() && name === "deliveries") {
        // Empty ingress is normal. A final tracked receipt also authorizes
        // its owned envelope: a crash can fall between receipt write and unlink.
        // Anonymous, pending, uncertain, malformed or linked content still vetoes.
        const children = fs.readdirSync(file);
        const followUps = path.join(root, "follow-ups");
        for (const child of children) {
          if (expired() || !followUpName.test(child)) return false;
          const id = child.slice(0, -5);
          const item = readJson<{ message?: unknown; delivery?: unknown; followUpId?: unknown; controlId?: unknown; images?: unknown; provenance?: unknown }>(path.join(file, child));
          if (!item || typeof item.message !== "string" ||
              item.images !== undefined && !Array.isArray(item.images) ||
              Object.keys(item).some(key => !["message", "delivery", "followUpId", "controlId", "images", "provenance"].includes(key))) return false;
          if (item.controlId === id) {
            const admission = readTerminalAdmission(root, id);
            const control = readJson<{ state?: string }>(path.join(root, "terminal-controls", child));
            if (!control || !["delivered", "refused"].includes(control.state ?? "") ||
                !terminalControlMatchesAdmission(control, admission) ||
                !terminalControlMatchesAdmission({ id, delivery: item.delivery, provenance: item.provenance }, admission)) return false;
          } else if (item.controlId !== undefined || item.delivery !== "followUp" || !copyFabricProvenance(item.provenance)) return false;
          if (item.followUpId !== undefined || item.controlId === undefined) {
            if (item.delivery !== "followUp" || item.followUpId !== id || !ownedStat(path.join(followUps, child))?.isFile() ||
                !ownedStat(followUps)?.isDirectory() || !safeFollowUps(followUps, expired)) return false;
          }
        }
        continue;
      }
      if (stat.isDirectory() && name === "durable") {
        if (!safeDurableTree(file, expired)) return false;
        continue;
      }
      if (stat.isDirectory() && name === "nested") {
        for (const child of fs.readdirSync(file)) if (!safeRunTree(path.join(file, child), false, depth + 1, expired, acceptPidReuse)) return false;
        continue;
      }
      return false;
    }
    return true;
  } catch { return false; }
};
/** Why safeRunTree refused a run, as a fixed category for the retention report (ids and paths removed). Report only:
 * it repeats the checks in safeRunTree's order and is not a deletion authority. */
const runTreeRefusal = (root: string, acceptPidReuse: boolean): string => {
  const exit = runTreeExitVeto(root, 0, noDeadline, true, acceptPidReuse);
  if (exit) return exit.replace(/ \([^)]*\)$/, "");
  const record = readJson<RunRecordSummary>(path.join(root, "status.json"));
  const pid = record?.transport === "process" && typeof record.sessionId === "string" && /^\d+$/.test(record.sessionId) ? Number(record.sessionId) : undefined;
  if (pid !== undefined && savedWriterAlive(pid, record, acceptPidReuse)) return "saved worker PID is live";
  try {
    if (!safeTerminalArtifacts(root, noDeadline)) return "terminal answer, notices or controls not proven consumed";
    for (const name of fs.readdirSync(root)) {
      const file = path.join(root, name);
      const stat = ownedStat(file);
      if (!stat) return "an entry is a link, hard-linked or not ours";
      if (stat.isDirectory() && name === TASK_FINAL_ANSWER_LOCK &&
          (!record?.status || !TERMINAL_STATUSES.has(record.status) || !safeStaleFinalAnswerLock(file))) {
        return "terminal-answer lock owner is live, malformed, or not proven stale";
      }
      if (name === "tmp" && stat.isDirectory() && !boundedRunTree(file, noDeadline)) {
        return "task scratch subtree is unsafe or exceeds scan bounds";
      }
      const known = stat.isFile() ? runFile(name) || name === "final-answer.json" || name === "queued-result.json"
        : [TASK_FINAL_ANSWER_LOCK, "terminal-controls", "terminal-notices", "handoff-session", "follow-ups", "deliveries", "nested", "tmp"].includes(name);
      if (!known) return `unknown entry ${name.replace(/[0-9a-f-]{8,}/gi, "<id>")}`;
    }
  } catch { return "run tree unreadable"; }
  return "follow-ups, deliveries, handoff or nested run not proven safe";
};
/** Explicit resident roots have no managed-temp owner. Require terminal status
 * plus checked process absence, and veto nested survivors and unresolved markers. */
export const canRemoveTerminalRun = (directory: string, expired: Deadline = noDeadline, acceptPidReuse = false): boolean => {
  const record = readJson<RunRecordSummary>(path.join(directory, "status.json"));
  return !!record?.status && TERMINAL_STATUSES.has(record.status) && safeRunTree(directory, false, 0, expired, acceptPidReuse);
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
/** smarty-dev#9855: offline recovery of a dead root's ENODEV-unconfirmed task-agent run (delivers or settles it
 * once and clears its marker); a failure keeps the veto, as for recoverActorRunArchives. */
const recoverEnodevTask = (directory: string): void => {
  try { recoverEnodevTaskRun(directory); } catch { /* the marker stays: the run stays vetoed */ }
};
/** Main's path-based recursive removal: the non-Linux fallback of verifiedRemove only (smarty-dev#7858). */
const rmTree = (target: string) => (): boolean => { fs.rmSync(target, { recursive: true, force: true }); return true; };
export const removeEmptyRunRoot = (root: string): boolean => {
  try {
    const judged = ownedStat(root);
    if (!judged?.isDirectory() || !validOwner(readJson<RunRootOwner>(ownerPath(root)))) return false;
    if (fs.readdirSync(root).some((name) => name !== RUN_ROOT_OWNER_FILE)) return false;
    return verifiedRemove(root, judged, { deleter: "removeEmptyRunRoot", fallback: rmTree(root) }).removed;
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
    if (!processAlive(owner.pid)) { recoverActorRunArchives(directory, expired); recoverEnodevTask(directory); }
    // The judged stat: taken after recovery, before the safety walk; the removal requires it unchanged.
    const judged = ownedStat(directory);
    if (!safeRunTree(directory, owner.childrenStopped === true, 0, expired)) { countRefusal("pruneClosedRunRoot", "run tree is not proven safe", directory); continue; }
    if (verifiedRemove(directory, judged, { deleter: "pruneClosedRunRoot", fallback: rmTree(directory) }).removed) removed.push(directory);
  }
  return removed;
};
/** Per-interval claim files of the mesh-wide retention sweep (smarty-dev#3252). */
export const MESH_RETENTION_SWEEP_PREFIX = ".mesh-retention-sweep-";
const meshSweepSlot = (name: string): number | undefined => {
  const match = name.startsWith(MESH_RETENTION_SWEEP_PREFIX) ? /^(\d+)\.json$/.exec(name.slice(MESH_RETENTION_SWEEP_PREFIX.length)) : null;
  return match ? Number(match[1]) : undefined;
};
/** Diagnostics only (pi-fabric#883 SEC P1): whether a slot file names a sweeper whose pid and /proc start time still
 * match. It only keeps that slot file from being dropped; single flight is the sweep lock (lockMeshRetentionSweep). */
const liveMeshSweeper = (file: string): boolean => {
  const claim = readJson<{ sweeper?: { pid?: unknown; startTime?: unknown } }>(file, 4096);
  const pid = claim?.sweeper?.pid, startTime = claim?.sweeper?.startTime;
  if (typeof pid !== "number" || !Number.isSafeInteger(pid) || pid <= 0 || typeof startTime !== "string") return false;
  return processAlive(pid) && processStartTime(pid) === startTime;
};
/**
 * Claim this interval's mesh-wide sweep. Exactly one process per mesh wins a slot: the claim
 * is an exclusive create of the slot's file, so two owners never both start a sweep in the
 * same interval. Single flight across slots (a sweep can outlast its interval, smarty-dev#7916) is the sweep lock,
 * taken before this claim (lockMeshRetentionSweep). Other slot files, but the previous one, are dropped once a newer
 * slot is claimed, unless their recorded sweeper still runs.
 */
export const claimMeshRetentionSweep = (meshRoot: string, intervalMs: number, now = Date.now()): boolean => {
  if (!Number.isSafeInteger(intervalMs) || intervalMs <= 0 || !ownedStat(meshRoot)?.isDirectory()) return false;
  const slot = Math.floor(now / intervalMs);
  const name = `${MESH_RETENTION_SWEEP_PREFIX}${slot}.json`;
  try {
    const fd = fs.openSync(path.join(meshRoot, name), fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | (fs.constants.O_NOFOLLOW ?? 0), 0o600);
    try { fs.writeSync(fd, JSON.stringify({ pid: process.pid, claimedAt: now })); } finally { fs.closeSync(fd); }
  } catch { return false; }
  try {
    for (const other of fs.readdirSync(meshRoot)) {
      // Also slots of another interval (the former hourly numbering is larger than today's daily slot).
      const otherSlot = meshSweepSlot(other);
      if (otherSlot !== undefined && otherSlot !== slot && otherSlot !== slot - 1 && !liveMeshSweeper(path.join(meshRoot, other))) {
        const file = path.join(meshRoot, other);
        verifiedRemove(file, ownedStat(file), { deleter: "claimMeshRetentionSweep", fallback: () => { fs.rmSync(file, { force: true }); return true; } });
      }
    }
  } catch { /* stale slot files are tiny; the next claim retries */ }
  return true;
};
/** The mesh retention sweep's lock file (pi-fabric#883 SEC P1). */
export const MESH_RETENTION_SWEEP_LOCK = ".mesh-retention-sweep.lock";
/**
 * Single flight of the mesh retention sweep (pi-fabric#883 SEC P1): an exclusive flock(2) on the lock file, taken
 * before the slot claim. Returns the locked descriptor, which the claimer passes to the sweep process (inherited as
 * fd 3) and then closes, so the SWEEP holds the lock from its first instruction until it exits; a crash releases it.
 * "busy": a sweep holds it, skip. Throws when the lock state is uncertain (flock missing, an open or ownership
 * error, an unexpected exit): the caller does not sweep and alarms.
 */
// ponytail: reuses residency/file-lock.ts lockFile (util-linux flock(1) on an inherited open file description, the
// resident host's lock) instead of a native addon or wrapping the sweep in `flock`: the descriptor is handed to the
// sweep, so the kernel ties the lock to the sweep's lifetime without any pid record.
export const lockMeshRetentionSweep = async (meshRoot: string): Promise<number | "busy"> => {
  try { return await lockFile(path.join(meshRoot, MESH_RETENTION_SWEEP_LOCK), 0); }
  catch (error) { if (error instanceof FileLockBusy) return "busy"; throw error; }
};
/**
 * The only way a mesh retention sweep starts (pi-fabric#883 SEC/CODE P1): a successful non-blocking lock, then this
 * interval's slot claim, then `start(lockFd)`, which must hand the descriptor to the sweep process. The descriptor is
 * closed here afterwards, so only the sweep keeps the lock. No record, an unreadable or failed record, or a scan
 * error never leads to a sweep: "busy" (a sweep holds the lock), "claimed" (this slot already ran), or "uncertain"
 * (the lock state is unknown; the caller alarms). Skipping a daily sweep is safe.
 */
export const startLockedMeshRetentionSweep = async (meshRoot: string, intervalMs: number, start: (lockFd: number) => void,
  now = Date.now(), wanted: () => boolean = () => true): Promise<"started" | "busy" | "claimed" | "cancelled" | { uncertain: string }> => {
  let lock: number | "busy";
  try { lock = await lockMeshRetentionSweep(meshRoot); }
  catch (error) { return { uncertain: error instanceof Error ? error.message : String(error) }; }
  if (lock === "busy") return "busy";
  try {
    // smarty-dev#9508: the owner may have closed during the async acquire: no claim, no spawn, and the lock is released.
    if (!wanted()) return "cancelled";
    if (!claimMeshRetentionSweep(meshRoot, intervalMs, now)) return "claimed";
    start(lock);
    return "started";
  } finally { fs.closeSync(lock); }
};
/** Diagnostics only: record the sweep process this slot's claim started (its pid and /proc start time). */
export const recordMeshRetentionSweeper = (meshRoot: string, intervalMs: number, pid: number, now: number): void => {
  const startTime = processStartTime(pid);
  if (startTime === undefined) return;
  try {
    writeJsonAtomic(path.join(meshRoot, `${MESH_RETENTION_SWEEP_PREFIX}${Math.floor(now / intervalMs)}.json`),
      { pid: process.pid, claimedAt: now, sweeper: { pid, startTime } });
  } catch { /* unrecorded: the next slot is not blocked (today's behaviour) */ }
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
      recoverEnodevTask(directory);
      const judged = ownedStat(directory);
      if (!safeRunTree(directory, false, 0, expired)) { countRefusal("sweepTempRunRoots", "run tree is not proven safe", directory); continue; }
      // Reported as the root's removal once it is empty, as before.
      verifiedRemove(directory, judged, { deleter: "sweepTempRunRoots", fallback: rmTree(directory) });
    }
    if (removeEmptyRunRoot(root)) result.removedRoots.push(root);
  }
  return result;
};

type RegistryRunRow = { id?: unknown; lastRunId?: unknown; inFlightRun?: unknown; preparing?: unknown; removal?: unknown };
/** Every run a registry row references: latest, in-flight, preparing and pending-removal runs.
 * A malformed reference throws (the caller turns that into a wildcard veto). */
const rowRunIds = (actor: RegistryRunRow): string[] => {
  const ids: string[] = [];
  if (actor.lastRunId !== undefined) {
    if (typeof actor.lastRunId !== "string") throw new Error("Unknown actor run reference");
    if (actor.lastRunId) ids.push(actor.lastRunId);
  }
  for (const [field, key] of [["inFlightRun", "id"], ["preparing", "runId"], ["removal", "runId"]] as const) {
    const value = actor[field];
    if (value === undefined) continue;
    if (!value || typeof value !== "object") throw new Error("Unknown actor run reference");
    const id = (value as Record<string, unknown>)[key];
    if (field === "inFlightRun" ? typeof id !== "string" : id !== undefined && typeof id !== "string") {
      throw new Error("Unknown actor run reference");
    }
    if (typeof id === "string" && id) ids.push(id);
  }
  return ids;
};
/** Full persisted run references (latest, in-flight, preparing, pending-removal) of every
 * registry, shared by resident startup, streaming retention and the mesh sweep.
 * An unreadable registry is a wildcard veto, never proof that a reference is absent.
 * `requireRegistries`: a registry root without actors.json is uncertain, not empty
 * (the mesh sweep's final pre-delete check over registries it discovered). */
export const retainedActorRunIds = (actorRoots: readonly string[], options: { requireRegistries?: boolean } = {}): Set<string> =>
  registryReferences(actorRoots, options).snapshot();

export interface RegistryReferences {
  /** The references as last read; no filesystem access. For early skips only, never for a delete. */
  snapshot(): Set<string>;
  /** Re-checks every registry's identity (lstat) and re-parses only a registry whose actors.json changed. The final
   * pre-delete check. An unreadable, unsafe or vanished registry is a wildcard veto. */
  fresh(): Set<string>;
  /** The rows of a registry as last parsed, or undefined when it is unreadable. */
  actors(root: string): Record<string, any>[] | undefined;
  /** Parses per registry root (smarty-dev#7916: one per sweep unless the registry itself changed). */
  readonly parses: ReadonlyMap<string, number>;
}
/** Registry references read once per sweep (smarty-dev#7916, measured on Ryzen 1: the sweep re-parsed every registry
 * for every candidate run and did not finish in 30 min over 606 registries). Each registry is parsed once, then only
 * when its actors.json identity (dev, ino, size, mtime, ctime) changes; the per-delete check costs one lstat per
 * registry, not a parse. */
export const registryReferences = (actorRoots: readonly string[], options: { requireRegistries?: boolean } = {}): RegistryReferences => {
  const cache = new Map<string, { stamp: string; ids: string[]; actors: Record<string, any>[] }>();
  const parses = new Map<string, number>();
  let last = new Set<string>();
  // `actorRoots` is read on every call: the mesh sweep replaces its contents when registry roots come or go.
  const uid = typeof process.getuid === "function" ? process.getuid() : undefined;
  const fresh = (): Set<string> => {
    const refs = new Set<string>();
    try {
      for (const root of actorRoots) {
        const file = path.join(root, "actors.json");
        // The per-delete cost: one lstat of actors.json while it is unchanged. The full checks run on a change.
        const cached = cache.get(root);
        const quick = cached && fs.lstatSync(file, { throwIfNoEntry: false });
        if (cached && quick && quick.isFile() && quick.nlink === 1 && (uid === undefined || quick.uid === uid) &&
            `${quick.dev}:${quick.ino}:${quick.size}:${quick.mtimeMs}:${quick.ctimeMs}` === cached.stamp) {
          for (const id of cached.ids) refs.add(id);
          continue;
        }
        try { fs.lstatSync(root); }
        catch (error) { if (!options.requireRegistries && (error as NodeJS.ErrnoException).code === "ENOENT") { cache.delete(root); continue; } throw error; }
        if (!ownedStat(root)?.isDirectory()) throw new Error("Unsafe actor root");
        try { fs.lstatSync(file); }
        catch (error) { if (!options.requireRegistries && (error as NodeJS.ErrnoException).code === "ENOENT") { cache.delete(root); continue; } throw error; }
        const stat = ownedStat(file);
        if (!stat?.isFile()) throw new Error("Unsafe actor registry");
        // Taken before the read: a change during the read gives a new stamp, so the next check re-parses.
        const stamp = `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeMs}:${stat.ctimeMs}`;
        let entry = cache.get(root);
        if (entry?.stamp !== stamp) {
          cache.delete(root);
          parses.set(root, (parses.get(root) ?? 0) + 1);
          // ActorRegistryStore's writer/reader has no byte-size protocol limit: every
          // actor includes instructions and up to 100 message bodies, so even one
          // ordinary actor can exceed the 1-MiB summary-file guard. Match that existing
          // JSON contract rather than inventing a fleet-size limit that disables all
          // retention. Ownership, JSON/schema errors and unsafe references still veto.
          const registry = readJson<{ actors?: RegistryRunRow[] }>(file, Number.MAX_SAFE_INTEGER);
          if (!Array.isArray(registry?.actors)) throw new Error("Unreadable actor registry");
          const ids: string[] = [];
          for (const actor of registry.actors) {
            if (!actor || typeof actor.id !== "string") throw new Error("Unknown actor run reference");
            ids.push(...rowRunIds(actor));
          }
          entry = { stamp, ids, actors: registry.actors as Record<string, any>[] };
          cache.set(root, entry);
        }
        for (const id of entry.ids) refs.add(id);
      }
    } catch { refs.add("*"); }
    return (last = refs);
  };
  fresh();
  return { snapshot: () => last, fresh, actors: root => cache.get(root)?.actors, parses };
};
/** The final pre-delete check of every mesh retention removal (pi-fabric#645 review rounds 1-2):
 * re-reads every registry uncached immediately before a run is deleted. An unreadable registry
 * (and, with `requireRegistries`, a vanished root or actors.json) is a wildcard veto. */
export const actorRunReferencedNow = (actorRoots: readonly string[], runId: string, options: { requireRegistries?: boolean } = {}): boolean => {
  const ids = retainedActorRunIds(actorRoots, options);
  return ids.has("*") || ids.has(runId);
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
export const compactTerminalRunEvents = (
  directory: string,
  options: TerminalRunEventsRetention & { now?: number; expired?: Deadline; isRetained?: () => boolean; dryRun?: boolean;
    onCompact?: (change: { path: string; beforeBytes: number; afterBytes: number }) => void } = {},
): boolean => {
  const now = options.now ?? Date.now();
  const ageMs = options.terminalRunEventsAgeMs ?? 6 * 60 * 60 * 1_000;
  const maxBytes = options.terminalRunEventsMaxBytes ?? 256 * 1024;
  const expired = options.expired ?? noDeadline;
  const directoryStat = ownedStat(directory);
  if (!Number.isSafeInteger(ageMs) || ageMs < 0 || !Number.isSafeInteger(maxBytes) ||
      maxBytes < EVENT_TAIL_MARKER.length || expired() || options.isRetained?.() || !directoryStat?.isDirectory()) return false;
  const record = readJson<RunRecordSummary>(path.join(directory, "status.json"));
  if (!record?.status || !TERMINAL_STATUSES.has(record.status) ||
      now - recordAgeReference(record, ownedStat(directory)?.mtimeMs ?? now) < ageMs) return false;
  const file = path.join(directory, "events.jsonl");
  const stat = ownedStat(file);
  if (!stat?.isFile() || stat.size === 0 || (!retentionV2Enabled() &&
      (runTreeExitVeto(directory, 0, expired, true) || !canRemoveTerminalRun(directory, expired)))) return false;
  try {
    const fd = fs.openSync(file, fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW ?? 0));
    let tail: Buffer;
    try {
      const opened = fs.fstatSync(fd);
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
        const count = fs.readSync(fd, tail, read, length - read, stat.size - length + read);
        if (count === 0) return false;
        read += count;
      }
    } finally { fs.closeSync(fd); }
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
    // A real replacement still requires both independent fresh safety walks.
    if (retentionV2Enabled() && (runTreeExitVeto(directory, 0, expired, true) || !canRemoveTerminalRun(directory, expired))) return false;
    // Truncation is now known to be necessary. Reserve the marker's space and
    // drop only complete prefix lines; look behind by one byte so an exactly
    // aligned final event is kept. An oversized final event leaves only a marker.
    const eventBytes = maxBytes - EVENT_TAIL_MARKER.length;
    if (retained.length > eventBytes) {
      const newline = retained.indexOf(0x0a, retained.length - eventBytes - 1);
      retained = newline < 0 ? Buffer.alloc(0) : retained.subarray(newline + 1);
    }
    const checked = ownedStat(file);
    if (!checked || checked.dev !== stat.dev || checked.ino !== stat.ino ||
        checked.size !== stat.size || checked.mtimeMs !== stat.mtimeMs ||
        runTreeExitVeto(directory, 0, expired, true) || !canRemoveTerminalRun(directory, expired) ||
        expired() || options.isRetained?.()) return false;
    const next = Buffer.concat([EVENT_TAIL_MARKER, retained]);
    let replaced = false;
    try {
      // The judged events.jsonl goes through the one verified removal (smarty-dev#7916): verified, moved into the
      // graveyard and verified again; the new tail then takes the free name (link fails on EEXIST), and only then
      // is the judged inode removed. A failed publish moves it back.
      replaced = verifiedRemove(file, stat, { deleter: "compactTerminalRunEvents", freed: stat.size - next.length,
        ...(options.dryRun ? { dryRun: true } : {}),
        proceed: () => publishExclusive(file, next),
        fallback: () => { writeFileAtomic(file, next); return true; } }).removed;
    } finally {
      if (!options.dryRun) {
        // Residency expiry (and legacy timestamp fallback) uses directory mtime.
        // Creating/renaming the atomic tail must not restart that retention clock.
        const current = ownedStat(directory);
        if (current?.dev === directoryStat.dev && current.ino === directoryStat.ino) {
          try { fs.utimesSync(directory, directoryStat.atimeMs / 1000, directoryStat.mtimeMs / 1000); }
          catch { /* A failed timestamp restore only delays collection; it cannot authorize it. */ }
        }
      }
    }
    if (!replaced) return false;
    options.onCompact?.({ path: file, beforeBytes: stat.size, afterBytes: EVENT_TAIL_MARKER.length + retained.length });
    return true;
  } catch { return false; }
};

/** Publish `data` at `file` only while the name is free: an O_EXCL|O_NOFOLLOW private temp beside it, then link
 * (EEXIST never replaces anything), then the temp goes. */
const publishExclusive = (file: string, data: Buffer): boolean => {
  const temp = `${file}.${process.pid}.${Date.now()}.tmp`;
  try {
    const fd = fs.openSync(temp, fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | (fs.constants.O_NOFOLLOW ?? 0), 0o600);
    try { fs.writeSync(fd, data); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
    fs.linkSync(temp, file);
    return true;
  } catch { return false; }
  finally { try { fs.unlinkSync(temp); } catch { /* never created */ } }
};

/** Immutable, ordinary rotation history only. Malformed-session orphan backups are
 * recovery evidence, not rotation history, and keep their existing exemption. */
export const pruneActorSessionBackups = (sessionFile: string, options: {
  dryRun?: boolean; onPrune?: (change: { path: string; bytes: number }) => void;
  /** Asked right before each unlink; true stops the prune there (smarty-dev#7766: a hold or epoch change mid-prune). */
  stop?: () => boolean;
} = {}): string[] => {
  const directory = path.dirname(sessionFile);
  if (!ownedStat(directory)?.isDirectory()) return [];
  const prefix = `${path.basename(sessionFile)}.`;
  try {
    const backups = fs.readdirSync(directory).flatMap(name => {
      if (!name.startsWith(prefix)) return [];
      const match = /^(\d{8}T\d{9}Z)(?:-(\d+))?\.bak$/.exec(name.slice(prefix.length));
      const stat = ownedStat(path.join(directory, name));
      const stamp = match?.[1];
      const at = stamp === undefined ? NaN : Date.parse(`${stamp.slice(0, 4)}-${stamp.slice(4, 6)}-${stamp.slice(6, 8)}T${stamp.slice(9, 11)}:${stamp.slice(11, 13)}:${stamp.slice(13, 15)}.${stamp.slice(15, 18)}Z`);
      return match && stat?.isFile() ? [{ name, stamp: match[1]!, suffix: Number(match[2] ?? 0), stat, at }] : [];
    });
    // smarty-dev#7916 design 3: the order comes from trusted data (mtime, then the rotation's ctime, then the inode),
    // never the name or the wall clock. On doubt the newest is kept: the newest by that order AND the newest by name,
    // and every backup whose name is dated in the future (or unparsable) stays.
    const byName = [...backups].sort((a, b) => a.stamp.localeCompare(b.stamp) || a.suffix - b.suffix);
    const byAge = [...backups].sort((a, b) => a.stat.mtimeMs - b.stat.mtimeMs || a.stat.ctimeMs - b.stat.ctimeMs || a.stat.ino - b.stat.ino);
    const now = Date.now();
    const keep = new Set([byName.at(-1)?.name, byAge.at(-1)?.name]);
    for (const backup of backups) {
      if (!(backup.at <= now)) {
        keep.add(backup.name);
        countRefusal("pruneActorSessionBackups", "future-dated or unparsable backup name", path.join(directory, backup.name));
      }
    }
    const removed: string[] = [];
    for (const backup of byAge) {
      if (keep.has(backup.name)) continue;
      const file = path.join(directory, backup.name);
      if (options.stop?.()) break;
      if (!verifiedRemove(file, backup.stat, { deleter: "pruneActorSessionBackups", ...(options.dryRun ? { dryRun: true } : {}),
        fallback: () => { fs.unlinkSync(file); return true; } }).removed) continue;
      options.onPrune?.({ path: file, bytes: backup.stat.size });
      removed.push(file);
    }
    return removed;
  } catch { return []; }
};

export const pruneActorRunArchives = (options: {
  runsDirectory: string;
  latestRunId?: string;
  retentionMs: number;
  terminalRunEventsAgeMs?: number;
  terminalRunEventsMaxBytes?: number;
  now?: number;
  /** Further live references (registry latest/in-flight runs), checked again just before removal;
   * `final` marks that last pre-delete check (callers re-read their references uncached). */
  isRetained?: (runId: string, final?: boolean) => boolean;
  dryRun?: boolean;
  /** Size accounting of a removal candidate, done before the final check so nothing slow runs
   * between that check and the delete (pi-fabric#645 review round 4). */
  measure?: (directory: string) => number;
  /** Called after a run directory is removed (or, in a dry run, would be), with its `measure` bytes. */
  onRemove?: (directory: string, bytes: number) => void;
  onCompact?: (change: { path: string; beforeBytes: number; afterBytes: number }) => void;
  /** Mesh-wide sweep only (smarty-dev#3252): a live PID whose checked birth identity differs is reuse,
   * not the run's writer. Every other caller keeps the strict live-PID veto. */
  acceptPidReuse?: boolean;
}): string[] => {
  const now = options.now ?? Date.now();
  const removed: string[] = [];
  if (!ownedStat(options.runsDirectory)?.isDirectory()) return removed;
  let entries: fs.Dirent[];
  try { entries = fs.readdirSync(options.runsDirectory, { withFileTypes: true }); } catch { return removed; }
  for (const entry of entries) {
    if (!entry.isDirectory()) continue;
    const directory = path.join(options.runsDirectory, entry.name);
    // Reported refusals carry their size (the CLI's report only; in-process sweeps measure nothing).
    const refuse = (reason: string) => countRefusal("pruneActorRunArchives", reason, directory, removalTallyOn() ? options.measure?.(directory) ?? 0 : 0);
    if (entry.name === options.latestRunId || options.isRetained?.(entry.name)) { refuse("latest or referenced run"); continue; }
    // The judged stat, before the judgment reads anything; the removal requires it unchanged.
    const judged = ownedStat(directory);
    const record = readJson<RunRecordSummary>(path.join(directory, "status.json"));
    if (!record?.status || !TERMINAL_STATUSES.has(record.status)) { refuse(`status is ${record ? String(record.status) : "unreadable"}`); continue; }
    if (!safeRunTree(directory, false, 0, noDeadline, options.acceptPidReuse === true)) {
      if (removalTallyOn()) refuse(runTreeRefusal(directory, options.acceptPidReuse === true));
      continue;
    }
    if (now - recordAgeReference(record, judged?.mtimeMs ?? now) < options.retentionMs) {
      compactTerminalRunEvents(directory, {
        ...(options.terminalRunEventsAgeMs !== undefined ? { terminalRunEventsAgeMs: options.terminalRunEventsAgeMs } : {}),
        ...(options.terminalRunEventsMaxBytes !== undefined ? { terminalRunEventsMaxBytes: options.terminalRunEventsMaxBytes } : {}),
        now, isRetained: () => !!options.isRetained?.(entry.name),
        ...(options.dryRun !== undefined ? { dryRun: options.dryRun } : {}),
        ...(options.onCompact ? { onCompact: options.onCompact } : {}),
      });
      continue;
    }
    const bytes = options.measure?.(directory) ?? 0;
    const isRetained = options.isRetained;
    if (!removeUnreferencedRun(directory, judged, isRetained && (() => isRetained(entry.name, true)), options.dryRun, "pruneActorRunArchives", bytes)) continue;
    removed.push(directory);
    options.onRemove?.(directory, bytes);
  }
  return removed;
};

/** The final pre-delete check and the delete, back to back (pi-fabric#645 review round 4), through the one verified
 * removal (smarty-dev#7916): check; verify the judged inode and its tree; move it into the graveyard and verify it
 * again; check again (`proceed`); then delete it, or move it back when the re-check finds a reference (a throwing
 * check counts as a reference). A dry run takes the first check and the full verification, with no move or delete.
 * Returns whether the run was (or would be) removed. Off Linux main's tombstone path below runs (smarty-dev#7858).
 * ponytail: no fence excludes registry writers. ActorRegistryStore's lock is an exclusive, async mkdir
 * lock with no shared mode; holding every registry's lock across each delete would stall every actor's
 * saves, and these sweeps are synchronous. The tombstone narrows the remaining window to a reference
 * written after the re-check, which names a run already renamed away: its writer finds the run missing,
 * exactly as after any completed deletion. A tombstone left by a crash is a run proven unreferenced; the
 * next sweep treats it as an ordinary run entry. */
export const removeUnreferencedRun = (
  directory: string, judged: fs.Stats | undefined, referenced?: () => boolean, dryRun = false, deleter = "removeUnreferencedRun",
  bytes = 0,
): boolean => {
  const check = (): boolean => { try { return !!referenced?.(); } catch { return true; } };
  if (check()) { countRefusal(deleter, "referenced at the final check", directory, bytes); return false; }
  return verifiedRemove(directory, judged, { deleter, ...(dryRun ? { dryRun: true } : {}), proceed: () => !check(),
    fallback: () => mainRemoveUnreferencedRun(directory, referenced, check) }).removed;
};
/** Main's path-based tombstone removal, unchanged: the non-Linux fallback only (smarty-dev#7858). */
const mainRemoveUnreferencedRun = (directory: string, referenced: (() => boolean) | undefined, check: () => boolean): boolean => {
  if (!referenced) {
    try { fs.rmSync(directory, { recursive: true, force: true }); return true; } catch { return false; }
  }
  const tombstone = path.join(path.dirname(directory), `.retention-tombstone-${path.basename(directory)}`);
  try { fs.renameSync(directory, tombstone); } catch { return false; }
  if (check()) {
    try { fs.renameSync(tombstone, directory); } catch { /* Kept under the tombstone name; never deleted while referenced. */ }
    return false;
  }
  try { fs.rmSync(tombstone, { recursive: true, force: true }); return true; } catch { return false; }
};
