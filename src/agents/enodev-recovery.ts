import fs from "node:fs";
import { diagnostics } from "../core/diagnostics.js";
import path from "node:path";
import { syncDirectoryChain, writeJsonAtomic } from "../core/atomic-write.js";
import { processStartTime } from "../residency/process-identity.js";
import { ownedStat, processAlive } from "../storage/scratch.js";
import { UNRESOLVED_WORKER_FILE } from "../storage/retention.js";
import { clearRunStopIntent, commitRunArchive, readRunStopIntent } from "./archive-custody.js";
import type { AgentRunResult } from "./types.js";

/**
 * smarty-dev#8935 (R-stall #6829 follow-up). Before pi-fabric#849 a scoped worker whose empty scope systemd removed
 * before Node reported its close was marked "execution exit unconfirmed" (ENODEV on cgroup.events): its run dir
 * keeps unresolved-worker.json forever, and after a restart its actor reruns the activation, losing the completed
 * answer the worker saved in settlement.json. This settles such a run ONCE, for the restored activation it served,
 * only when every fact holds:
 * - the unresolved reason is that ENODEV observation failure. ENODEV on the retained cgroup.events fd is the
 *   kernel's answer for a REMOVED cgroup, and cgroup v2 removes only an empty one: the reason is itself the proof
 *   that the scope cgroup is gone (the run dir records no cgroup path to re-check);
 * - the recorded worker is gone: its pid (status.json sessionId) is not alive, or a live pid's /proc start time
 *   differs from the recorded birth (status.json processStartTime; without a recorded birth a live pid vetoes);
 * - settlement.json is the worker's completed receipt for exactly this run (version 1, runId = the run's id), and
 *   the run's task carries this activation's id and actor.
 * Then the status record becomes the completed result, the custody files are discharged, and normal retention
 * collects the directory. Anything else keeps the veto.
 *
 * The close-timeout path below does not treat stderr/event logs as scope proof. New runs record a host-verified scope
 * unit and cgroup path; older runs require an operator receipt after an explicit unit's cgroup absence is checked.
 * Both use the same actor delivery/queue-ack boundary before removing unresolved custody.
 */
export const ENODEV_UNRESOLVED_REASON = "Owned scope observation failed: Error: ENODEV";
const RUN_ID = /^[a-f0-9]{32}$/;
const MAX_FILE = 4 * 1024 * 1024;
export const PROCESS_SCOPE_FILE = "scope.json";
export const CLOSE_TIMEOUT_OPERATOR_FILE = "close-timeout-operator.json";
const CLOSE_TIMEOUT_DETAIL = "Owned process worker did not confirm tree/native close within 7000ms";
const CLOSE_TIMEOUT_SUFFIX = `execution exit unconfirmed; custody retained: ${CLOSE_TIMEOUT_DETAIL}`;
const CLOSE_TIMEOUT_OPERATOR_REASON = (id: string): string => `Error: Agent ${id} ${CLOSE_TIMEOUT_SUFFIX}`;
const isCloseTimeoutReason = (reason: unknown, id: string): boolean =>
  reason === CLOSE_TIMEOUT_OPERATOR_REASON(id) || reason === CLOSE_TIMEOUT_DETAIL;
const SCOPE_UNIT = /^(?:fabric-scope-[A-Za-z0-9]{6}|fabric-task-[1-9][0-9]*-[a-f0-9]{16})\.scope$/;

// Host-only provenance: neither worker fields nor serialized checkpoints can mark a result.
const recoveredDurableStops = new WeakSet<AgentRunResult>();
/** Only stopped/timed_out objects returned by recoverDurableActorStop carry this identity. */
export const isRecoveredDurableStop = (result: AgentRunResult): boolean => recoveredDurableStops.has(result);

const readJson = (file: string): Record<string, unknown> | undefined => {
  try {
    const stat = ownedStat(file);
    if (!stat?.isFile() || stat.size > MAX_FILE) return undefined;
    const parsed: unknown = JSON.parse(fs.readFileSync(file, "utf8"));
    return typeof parsed === "object" && parsed !== null && !Array.isArray(parsed) ? parsed as Record<string, unknown> : undefined;
  } catch { return undefined; }
};

/** The run's worker is provably gone (pid-reuse safe). */
export const enodevWorkerGone = (record: Record<string, unknown>): boolean => {
  const pid = Number(record.sessionId);
  if (!Number.isSafeInteger(pid) || pid <= 1 || pid === process.pid) return false;
  if (!processAlive(pid)) return true;
  const birth = typeof record.processStartTime === "string" && /^\d+$/.test(record.processStartTime) ? record.processStartTime : undefined;
  const current = processStartTime(pid);
  return birth !== undefined && current !== undefined && current !== birth;
};

/** Run dirs under `runRoot` marked with the ENODEV observation failure (a one-time scan per host). */
export const enodevUnresolvedRuns = (runRoot: string): string[] => {
  let names: string[];
  try { names = fs.readdirSync(runRoot); } catch { return []; }
  return names.filter(name => RUN_ID.test(name)).map(name => path.join(runRoot, name)).filter(directory => {
    const marker = readJson(path.join(directory, UNRESOLVED_WORKER_FILE));
    return (typeof marker?.reason === "string" && marker.reason.startsWith(ENODEV_UNRESOLVED_REASON)) ||
      readRunStopIntent(directory, readJson(path.join(directory, "status.json"))) !== undefined;
  });
};

/**
 * pi-fabric#912: the actor's queue state was unknown (`unknown`) or known at this start. Counts consecutive unknown
 * starts in each of the actor's ENODEV markers (`unknownStarts`; a known start resets it) and returns the highest.
 */
/** From this many consecutive unknown starts the actor's owner is alarmed (ActorManager, ops.owner). */
export const UNKNOWN_STARTS_ALARM = 3;
export const countUnknownEnodevStarts = (directories: readonly string[], actorId: string, unknown: boolean):
  { starts: number; alarm: boolean } => {
  let highest = 0;
  let alarm = false;
  for (const directory of directories) {
    const file = path.join(directory, UNRESOLVED_WORKER_FILE);
    const marker = readJson(file);
    if (typeof marker?.reason !== "string" || !marker.reason.startsWith(ENODEV_UNRESOLVED_REASON) ||
      readJson(path.join(directory, "status.json"))?.actorId !== actorId) continue;
    const prior = typeof marker.unknownStarts === "number" ? marker.unknownStarts : 0;
    // Saturates at the alarm threshold (round 5 P2): the one alarm goes out when a marker reaches it.
    const next = unknown ? Math.min(prior + 1, UNKNOWN_STARTS_ALARM) : 0;
    if (unknown && prior < UNKNOWN_STARTS_ALARM && next === UNKNOWN_STARTS_ALARM) alarm = true;
    // The observed count escalates even when it cannot be saved (round 3 P2): a count that cannot persist would
    // never reach the third start, so an unknown start whose count is not saved escalates at once.
    highest = Math.max(highest, next);
    if (next !== prior) {
      try { writeJsonAtomic(file, { ...marker, unknownStarts: next }, { durable: true }); } catch (error) {
        diagnostics.warn("src/agents/enodev-recovery.ts", `[pi-fabric] actor ${actorId}: could not save unknownStarts ${next} in ${file}: ${String(error)} (smarty-dev#8935)`);
        if (unknown) { highest = Math.max(highest, UNKNOWN_STARTS_ALARM); alarm = true; }
      }
    }
  }
  return { starts: highest, alarm };
};

/** Host-authored first line of task.txt; input data cannot impersonate this manifest. */
export const ACTOR_ACTIVATION_HEADER = "Fabric activation: ";

interface ActorActivationManifest { activationId: string; actorId?: string }
/** The immutable activation and (for new tasks) actor identity, independent of payload shape. */
const activationManifestOf = (directory: string): ActorActivationManifest | undefined => {
  try {
    const task = fs.readFileSync(path.join(directory, "task.txt"), "utf8");
    if (task.startsWith(ACTOR_ACTIVATION_HEADER)) {
      const manifest: unknown = JSON.parse(task.split("\n", 1)[0]!.slice(ACTOR_ACTIVATION_HEADER.length));
      if (typeof manifest !== "object" || manifest === null || Array.isArray(manifest)) return undefined;
      const { activationId, actorId, batchIds } = manifest as Record<string, unknown>;
      if (typeof activationId !== "string" || activationId.length === 0 || !Array.isArray(batchIds) ||
          batchIds.length === 0 || batchIds[0] !== activationId || !batchIds.every(id => typeof id === "string" && id.length > 0) ||
          new Set(batchIds).size !== batchIds.length || (actorId !== undefined && (typeof actorId !== "string" || !actorId))) return undefined;
      return { activationId, ...(typeof actorId === "string" ? { actorId } : {}) };
    }
    // Legacy single-event tasks remain recoverable. Never fall back from a malformed manifest.
    const quoted = /^  "id": ("(?:[^"\\]|\\.)*")$/m.exec(task)?.[1];
    if (quoted === undefined) return undefined;
    const activationId: unknown = JSON.parse(quoted);
    return typeof activationId === "string" && activationId ? { activationId } : undefined;
  } catch { return undefined; }
};
const activationOf = (directory: string): string | undefined => activationManifestOf(directory)?.activationId;
export const actorActivationFromRun = (directory: string): ActorActivationManifest | undefined => activationManifestOf(directory);

export interface ProcessScopeReceipt { version: 1; runId: string; scopeUnit: string; cgroupPath: string }
export interface CloseTimeoutOperatorIdentity { uid: number | null; user: string; pid: number; agent?: string; session?: string }
export interface DeadRootRecoveryAuthorization {
  authorizedBy: string;
  authorityRef: string;
  rootProof: {
    version: 1; rootId: string; checkedAt: number; staleBoundMs: number;
    participantResolution: "not-found"; participantLastSeenAt: number; ownerLeaseExpiresAt?: number;
    mainLiveness: string; evidence: object;
  };
}
interface CloseTimeoutOperatorReceipt {
  version: 1; runId: string; actorId: string; activationId: string; scopeUnit: string; checkedAt: number;
  operator: CloseTimeoutOperatorIdentity; authorization?: DeadRootRecoveryAuthorization;
}

const validCgroupPath = (value: unknown, scopeUnit: string): value is string => typeof value === "string" && value.startsWith("/") &&
  !value.startsWith("//") && !/[\0\r\n]/.test(value) && path.posix.normalize(value) === value &&
  !value.split("/").some(part => part === "." || part === "..") && path.posix.basename(value) === scopeUnit;
const validScopeReceipt = (value: Record<string, unknown> | undefined, runId: string): value is Record<string, unknown> & ProcessScopeReceipt =>
  value?.version === 1 && value.runId === runId && typeof value.scopeUnit === "string" && SCOPE_UNIT.test(value.scopeUnit) &&
  validCgroupPath(value.cgroupPath, value.scopeUnit);

/** The host persists the verified systemd scope identity separately from worker-writable status.json. */
export const recordProcessScope = (directory: string, runId: string, scopeUnit: string, cgroupPath: string): void => {
  if (!RUN_ID.test(runId) || path.basename(path.resolve(directory)) !== runId || !ownedStat(directory)?.isDirectory() ||
      !SCOPE_UNIT.test(scopeUnit) || !validCgroupPath(cgroupPath, scopeUnit)) throw new Error("Invalid process scope receipt");
  writeJsonAtomic(path.join(directory, PROCESS_SCOPE_FILE), { version: 1, runId, scopeUnit, cgroupPath } satisfies ProcessScopeReceipt, { durable: true });
};

/** Positive absence check for a recorded cgroup-v2 path; any unreadable/invalid state vetoes recovery. */
export const scopeCgroupPathAbsent = (cgroupPath: string, cgroupRoot = "/sys/fs/cgroup"): boolean => {
  if (!path.isAbsolute(cgroupRoot) || !cgroupPath.startsWith("/") || cgroupPath.startsWith("//") || /[\0\r\n]/.test(cgroupPath) ||
      path.posix.normalize(cgroupPath) !== cgroupPath || cgroupPath.split("/").some(part => part === "." || part === "..")) return false;
  const root = path.resolve(cgroupRoot), parts = cgroupPath.split("/").filter(Boolean);
  if (parts.length === 0) return false;
  try { const stat = fs.lstatSync(root); if (!stat.isDirectory() || stat.isSymbolicLink()) return false; } catch { return false; }
  let current = root;
  for (let index = 0; index < parts.length; index++) {
    current = path.join(current, parts[index]!);
    let stat: fs.Stats;
    try { stat = fs.lstatSync(current); }
    catch (error) { return (error as NodeJS.ErrnoException).code === "ENOENT"; }
    if (stat.isSymbolicLink()) return false;
    if (index === parts.length - 1) return false;
    if (!stat.isDirectory()) return false;
  }
  return false;
};
const pathEntryExists = (file: string): boolean => {
  try { fs.lstatSync(file); return true; }
  catch (error) { return (error as NodeJS.ErrnoException).code !== "ENOENT"; }
};

/** For old runs with only an operator-supplied unit name, search the whole cgroup tree; incomplete walks veto. */
export const scopeUnitAbsent = (scopeUnit: string, cgroupRoot = "/sys/fs/cgroup"): boolean => {
  if (!SCOPE_UNIT.test(scopeUnit) || !path.isAbsolute(cgroupRoot)) return false;
  const root = path.resolve(cgroupRoot);
  try { if (!fs.lstatSync(root).isDirectory()) return false; } catch { return false; }
  const pending = [root];
  let visited = 0;
  while (pending.length) {
    const directory = pending.pop()!;
    if (++visited > 100_000) return false;
    let entries: fs.Dirent[];
    try { entries = fs.readdirSync(directory, { withFileTypes: true }); } catch { return false; }
    for (const entry of entries) {
      if (entry.name === scopeUnit) return false;
      if (entry.isSymbolicLink()) return false;
      if (!entry.isDirectory()) continue;
      pending.push(path.join(directory, entry.name));
    }
  }
  return true;
};

interface CloseTimeoutFacts { id: string; statusFile: string; record: Record<string, unknown>; actorId: string; activationId: string; text: string }
const closeTimeoutFacts = (directory: string, runId: string): CloseTimeoutFacts | undefined => {
  if (!RUN_ID.test(runId) || path.basename(path.resolve(directory)) !== runId || !ownedStat(directory)?.isDirectory()) return undefined;
  const marker = readJson(path.join(directory, UNRESOLVED_WORKER_FILE));
  if (marker?.runId !== runId || !isCloseTimeoutReason(marker.reason, runId)) return undefined;
  const statusFile = path.join(directory, "status.json"), record = readJson(statusFile);
  if (!record || record.id !== runId || record.transport !== "process" || record.status !== "completed" ||
      typeof record.actorId !== "string" || !record.actorId || !enodevWorkerGone(record)) return undefined;
  const task = activationManifestOf(directory);
  if (!task || (task.actorId !== undefined && task.actorId !== record.actorId)) return undefined;
  const settlement = readJson(path.join(directory, "settlement.json"));
  if (settlement?.version !== 1 || settlement.runId !== runId || settlement.outcome !== "completed" || typeof settlement.text !== "string") return undefined;
  return { id: runId, statusFile, record, actorId: record.actorId, activationId: task.activationId, text: settlement.text };
};

const validDeadRootAuthorization = (value: unknown): value is DeadRootRecoveryAuthorization => {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  const authorization = value as Record<string, unknown>;
  const proof = authorization.rootProof;
  if (typeof authorization.authorizedBy !== "string" || !/^(?:org|[A-Za-z][A-Za-z0-9._-]{0,63})$/.test(authorization.authorizedBy) ||
      typeof authorization.authorityRef !== "string" || !/^https:\/\/github\.com\/[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+\/issues\/[1-9][0-9]*(?:#issuecomment-[1-9][0-9]*)?$/.test(authorization.authorityRef) ||
      typeof proof !== "object" || proof === null || Array.isArray(proof)) return false;
  const record = proof as Record<string, unknown>;
  const evidence = record.evidence;
  if (record.version !== 1 || typeof record.rootId !== "string" || !record.rootId || typeof record.checkedAt !== "number" || !Number.isSafeInteger(record.checkedAt) ||
      typeof record.staleBoundMs !== "number" || !Number.isSafeInteger(record.staleBoundMs) || record.staleBoundMs <= 0 || record.participantResolution !== "not-found" ||
      typeof record.participantLastSeenAt !== "number" || !Number.isSafeInteger(record.participantLastSeenAt) || typeof record.mainLiveness !== "string" ||
      !record.mainLiveness.startsWith("stale:") || typeof evidence !== "object" || evidence === null || Array.isArray(evidence)) return false;
  const snapshot = evidence as Record<string, unknown>;
  const participants = snapshot.participants;
  return Array.isArray(participants) && participants.length > 0 && participants.every(value => {
    if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
    const participant = value as Record<string, unknown>;
    return (participant.source === "file" || participant.source === "state") && typeof participant.identity === "string" &&
      participant.identity.startsWith("gone:") && typeof participant.lastSeen === "number" && Number.isFinite(participant.lastSeen) &&
      (participant.ownerLeaseExpiresAt === null || typeof participant.ownerLeaseExpiresAt === "number");
  });
};

const validOperatorReceipt = (value: Record<string, unknown> | undefined, facts: CloseTimeoutFacts): value is Record<string, unknown> & CloseTimeoutOperatorReceipt => {
  const operator = value?.operator;
  return value?.version === 1 && value.runId === facts.id && value.actorId === facts.actorId && value.activationId === facts.activationId &&
    typeof value.scopeUnit === "string" && SCOPE_UNIT.test(value.scopeUnit) && typeof value.checkedAt === "number" && Number.isSafeInteger(value.checkedAt) &&
    typeof operator === "object" && operator !== null && !Array.isArray(operator) &&
    ((operator as Record<string, unknown>).uid === null || (typeof (operator as Record<string, unknown>).uid === "number" &&
      Number.isSafeInteger((operator as Record<string, unknown>).uid))) && typeof (operator as Record<string, unknown>).user === "string" &&
    typeof (operator as Record<string, unknown>).pid === "number" && Number.isSafeInteger((operator as Record<string, unknown>).pid) &&
    ((operator as Record<string, unknown>).agent === undefined || typeof (operator as Record<string, unknown>).agent === "string") &&
    ((operator as Record<string, unknown>).session === undefined || typeof (operator as Record<string, unknown>).session === "string") &&
    (value.authorization === undefined || validDeadRootAuthorization(value.authorization));
};

/** One-shot operator proof for a legacy run lacking the structured launch scope record. */
export const writeCloseTimeoutOperatorReceipt = (directory: string, runId: string, scopeUnit: string,
  operator: CloseTimeoutOperatorIdentity, cgroupRoot = "/sys/fs/cgroup",
  authorization?: DeadRootRecoveryAuthorization): CloseTimeoutOperatorReceipt | undefined => {
  if (!SCOPE_UNIT.test(scopeUnit) || !Number.isSafeInteger(operator.pid) || operator.pid !== process.pid || !operator.user ||
      operator.uid !== (process.getuid?.() ?? null) || (authorization !== undefined && !validDeadRootAuthorization(authorization))) return undefined;
  const facts = closeTimeoutFacts(directory, runId);
  if (!facts) return undefined;
  const scopeFile = path.join(directory, PROCESS_SCOPE_FILE), scope = readJson(scopeFile);
  if (pathEntryExists(scopeFile) && (!validScopeReceipt(scope, runId) || scope.scopeUnit !== scopeUnit)) return undefined;
  if (scope && validScopeReceipt(scope, runId) ? !scopeCgroupPathAbsent(scope.cgroupPath, cgroupRoot) : !scopeUnitAbsent(scopeUnit, cgroupRoot)) return undefined;
  const file = path.join(directory, CLOSE_TIMEOUT_OPERATOR_FILE);
  const existing = readJson(file);
  const sameAuthorization = (receipt: Record<string, unknown>): boolean => {
    const current = receipt.authorization as DeadRootRecoveryAuthorization | undefined;
    return authorization === undefined ? current === undefined : current?.authorizedBy === authorization.authorizedBy &&
      current.authorityRef === authorization.authorityRef && current.rootProof.rootId === authorization.rootProof.rootId;
  };
  if (existing) return validOperatorReceipt(existing, facts) && existing.scopeUnit === scopeUnit && sameAuthorization(existing) ? existing : undefined;
  const receipt: CloseTimeoutOperatorReceipt = { version: 1, runId, actorId: facts.actorId, activationId: facts.activationId,
    scopeUnit, checkedAt: Date.now(), operator, ...(authorization ? { authorization } : {}) };
  let fd: number | undefined;
  try {
    fd = fs.openSync(file, "wx", 0o600);
    fs.writeFileSync(fd, JSON.stringify(receipt));
    fs.fsyncSync(fd);
    fs.closeSync(fd); fd = undefined;
    syncDirectoryChain(directory);
    return receipt;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "EEXIST") {
      const raced = readJson(file);
      return validOperatorReceipt(raced, facts) && raced.scopeUnit === scopeUnit && sameAuthorization(raced) ? raced : undefined;
    }
    return undefined;
  } finally { if (fd !== undefined) fs.closeSync(fd); }
};

/** Actor recovery gate: structured launch scope for new runs, or a validated operator receipt for legacy runs. */
export const peekCloseTimeoutRun = (directory: string, actorId: string, activationId: string,
  cgroupRoot = "/sys/fs/cgroup"): AgentRunResult | undefined => {
  const facts = closeTimeoutFacts(directory, path.basename(directory));
  if (!facts || facts.actorId !== actorId || facts.activationId !== activationId) return undefined;
  const task = activationManifestOf(directory);
  const scopeFile = path.join(directory, PROCESS_SCOPE_FILE), scope = readJson(scopeFile);
  let operatorReceipt: Record<string, unknown> | undefined;
  if (pathEntryExists(scopeFile) && !validScopeReceipt(scope, facts.id)) return undefined;
  if (validScopeReceipt(scope, facts.id)) {
    if (task?.actorId !== actorId || !scopeCgroupPathAbsent(scope.cgroupPath, cgroupRoot)) return undefined;
  } else {
    operatorReceipt = readJson(path.join(directory, CLOSE_TIMEOUT_OPERATOR_FILE));
    if (!validOperatorReceipt(operatorReceipt, facts) || !scopeUnitAbsent(operatorReceipt.scopeUnit, cgroupRoot)) return undefined;
  }
  return { ...facts.record, id: facts.id, status: "completed", text: facts.text, finishedAt:
    typeof facts.record.finishedAt === "number" ? facts.record.finishedAt : Date.now() } as unknown as AgentRunResult;
};

/** Delivery receipt and commit are deliberately separate: marker removal follows the actor's queue acknowledgement. */
export const noteCloseTimeoutDelivered = (directory: string, runId: string, activationId: string): void =>
  writeJsonAtomic(path.join(directory, "close-timeout-delivered.json"), { version: 1, runId, activationId, deliveredAt: Date.now() }, { durable: true });
export const closeTimeoutDelivered = (directory: string, runId: string, activationId: string): boolean => {
  const receipt = readJson(path.join(directory, "close-timeout-delivered.json"));
  return receipt?.version === 1 && receipt.runId === runId && receipt.activationId === activationId;
};
export const settleCloseTimeoutRun = (directory: string, actorId: string, activationId: string,
  cgroupRoot = "/sys/fs/cgroup"): AgentRunResult | undefined => {
  const run = peekCloseTimeoutRun(directory, actorId, activationId, cgroupRoot);
  if (!run || !closeTimeoutDelivered(directory, run.id, activationId)) return undefined;
  const facts = closeTimeoutFacts(directory, run.id);
  if (!facts) return undefined;
  return settle(directory, { ...facts, ...(run.text !== undefined ? { text: run.text } : {}) },
    { status: "completed", text: facts.text, closeTimeoutRecovered: true });
};

/** Close-timeout debts of actor runs; old runs remain held until an operator receipt exists. */
export const closeTimeoutUnresolvedRuns = (runRoot: string): string[] => {
  let names: string[];
  try { names = fs.readdirSync(runRoot); } catch { return []; }
  return names.filter(name => RUN_ID.test(name)).map(name => path.join(runRoot, name)).filter(directory => {
    const marker = readJson(path.join(directory, UNRESOLVED_WORKER_FILE));
    return marker?.runId === path.basename(directory) && isCloseTimeoutReason(marker.reason, path.basename(directory));
  });
};

/** Keep the restored activation in flight until the old process identity is proven gone.
 * Run-file changes wake the join; one absolute 60s observation deadline also checks process identity.
 * A still-live or unproven worker at that deadline keeps custody and its running checkpoint.
 * Host close abandons observation, NOT custody: the promise stays pending for the next host to recover.
 */
const waitForStoppedWorker = (directory: string, currentRecord: () => Record<string, unknown>, closeSignal: AbortSignal): Promise<Record<string, unknown>> =>
  new Promise(resolve => {
    const initial = currentRecord();
    if (enodevWorkerGone(initial)) { resolve(initial); return; }
    let watcher: fs.FSWatcher | undefined;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const cleanup = (): void => { watcher?.close(); if (timer) clearTimeout(timer); closeSignal.removeEventListener("abort", cleanup); };
    const check = (): void => {
      const record = currentRecord();
      if (enodevWorkerGone(record)) { cleanup(); resolve(record); }
    };
    if (closeSignal.aborted) return;
    try { watcher = fs.watch(directory, check); watcher.unref(); watcher.on("error", () => { watcher?.close(); }); } catch { /* identity fallback below */ }
    // ponytail: R-no-polling exception pi-fabric#1193: run-file events are primary; no process-exit
    // event survives the dead host's lost transport/cgroup descriptors, and Node has no pidfd binding.
    // One unref'd absolute 60s identity check per recovered stop, never rearmed: bounded run-file reads,
    // kill(pid, 0) and /proc birth checks. It cannot launch Main or wake a resident on its own.
    // Recovered terminal stops settle this activation quietly; saved completed answers retain delivery.
    timer = setTimeout(check, 60_000);
    timer.unref();
    closeSignal.addEventListener("abort", cleanup, { once: true });
    check(); // Covers an exit between the first identity check and registering the watcher.
  });

/** Recover explicit stops before requiring ENODEV evidence: stop intent is written before transport.stop(). */
export const recoverDurableActorStop = async (runRoot: string, actorId: string, activationId: string,
  closeSignal: AbortSignal): Promise<AgentRunResult | undefined> => {
  let directories: string[];
  try { directories = fs.readdirSync(runRoot).map(name => path.join(runRoot, name)); } catch { return undefined; }
  for (const directory of directories) {
    const id = path.basename(directory);
    if (!RUN_ID.test(id) || activationOf(directory) !== activationId) continue;
    const unresolved = readJson(path.join(directory, UNRESOLVED_WORKER_FILE));
    if (isCloseTimeoutReason(unresolved?.reason, id)) continue; // no stop-intent fallback may discharge this custody
    const statusFile = path.join(directory, "status.json");
    const status = readJson(statusFile);
    const intent = readRunStopIntent(directory, status);
    if (!intent) continue;
    // Legacy receipts have no identity snapshot. They still fence admission when
    // retry removed status, but cannot prove an unknown replacement gone.
    let taskActor: unknown;
    try { taskActor = JSON.parse(fs.readFileSync(path.join(directory, "task.txt"), "utf8")).actorId; } catch { /* not an actor task */ }
    const matches = (record: Record<string, unknown> | undefined): record is Record<string, unknown> =>
      record?.id === id && record.transport === "process" && record.actorId === actorId;
    const fallback = { id, actorId, transport: "process", status: "running" };
    const snapshot = intent.recoveryRecord;
    if (!matches(snapshot) && !matches(status) && taskActor !== actorId) continue;
    const currentRecord = (): Record<string, unknown> => {
      const latestStatus = readJson(statusFile);
      const latestSnapshot = readRunStopIntent(directory, latestStatus)?.recoveryRecord;
      // An explicit identity-less snapshot means launch is pending: never fall
      // back to a superseded attempt's status/pid to discharge that custody.
      return matches(latestSnapshot) ? latestSnapshot : matches(latestStatus) ? latestStatus : fallback;
    };
    let record = await waitForStoppedWorker(directory, currentRecord, closeSignal);
    const latest = readJson(statusFile);
    if (matches(latest)) record = latest;
    // Restore the process record needed by the existing completion delivery/ack
    // path, without consuming or changing its immutable settlement receipt.
    if (!latest) writeJsonAtomic(statusFile, record, { durable: true });
    // Completion wins even without ENODEV. Reuse the existing delivery/queue-ack
    // protocol below rather than overwriting the receipt or settling before delivery.
    if (savedEnodevAnswer(directory) !== undefined) return undefined;
    const recoveredIntent = readRunStopIntent(directory, record) ?? intent;
    if (record.status !== "running" && record.status !== "queued") {
      if (!["completed", "failed", "stopped", "timed_out"].includes(String(record.status))) continue;
      const outcome = { ...record, stopIntent: recoveredIntent } as unknown as AgentRunResult;
      writeJsonAtomic(path.join(directory, "status.json"), outcome, { durable: true });
      if (outcome.status === "stopped" || outcome.status === "timed_out") recoveredDurableStops.add(outcome);
      return outcome;
    }
    const outcome = { ...record, ...uncleanReceipt(record), status: recoveredIntent.reason === "explicit" ? "stopped" : "timed_out",
      error: recoveredIntent.reason === "explicit" ? "Agent stopped" : "Agent timed out",
      stopIntent: recoveredIntent, finishedAt: Date.now() } as unknown as AgentRunResult;
    writeJsonAtomic(path.join(directory, "status.json"), outcome, { durable: true });
    recoveredDurableStops.add(outcome);
    return outcome;
  }
  return undefined;
};

interface EnodevRun { id: string; statusFile: string; record: Record<string, unknown>; activationId: string; text?: string }
/** Every fact except the settlement receipt: the ENODEV marker, the actor's process run, its activation, a gone worker. */
const inspect = (directory: string, actorId: string): EnodevRun | undefined => {
  const id = path.basename(directory);
  const marker = readJson(path.join(directory, UNRESOLVED_WORKER_FILE));
  const statusFile = path.join(directory, "status.json");
  const record = readJson(statusFile);
  if (!record || record.id !== id || record.actorId !== actorId || record.transport !== "process") return undefined;
  if (isCloseTimeoutReason(marker?.reason, id)) return undefined; // close-timeout custody requires the scope proof below
  if (!(typeof marker?.reason === "string" && marker.reason.startsWith(ENODEV_UNRESOLVED_REASON)) &&
      !readRunStopIntent(directory, record)) return undefined;
  const activationId = activationOf(directory);
  if (activationId === undefined || !enodevWorkerGone(record)) return undefined;
  const settlement = readJson(path.join(directory, "settlement.json"));
  const text = settlement?.version === 1 && settlement.runId === id && settlement.outcome === "completed" &&
    typeof settlement.text === "string" ? settlement.text : undefined;
  return { id, statusFile, record, activationId, ...(text === undefined ? {} : { text }) };
};

/** Writes the terminal record, discharges the archive custody, and removes the marker LAST (so it runs once). */
const settle = (directory: string, run: EnodevRun, terminal: Record<string, unknown>): AgentRunResult => {
  const stopIntent = readRunStopIntent(directory, run.record);
  const result = { ...run.record, id: run.id, ...terminal, ...(stopIntent ? { stopIntent } : {}), ...uncleanReceipt(run.record),
    finishedAt: typeof run.record.finishedAt === "number" ? run.record.finishedAt : Date.now() } as unknown as AgentRunResult;
  writeJsonAtomic(run.statusFile, result, { durable: true });
  try { commitRunArchive(directory, "settlement"); } catch { /* no awaiting archive: nothing to discharge */ }
  clearRunStopIntent(directory);
  fs.rmSync(path.join(directory, UNRESOLVED_WORKER_FILE), { force: true });
  return result;
};

/**
 * smarty-dev#9855 part A: a launch of THIS manager whose exit is ENODEV-unconfirmed, proven gone (the ENODEV marker,
 * its own process record, its worker gone, pid-reuse safe). No actor or activation fact: the caller owns the run
 * (it holds its admission permit). Settles it: a terminal record that has one keeps it; otherwise completed with the
 * saved answer, else failed lost. Marker removed last. Undefined keeps the veto (and the caller keeps the permit).
 */
/** What the MANAGER recorded about a launched worker (never the worker-writable status file): pid and /proc birth. */
export interface OwnedProcess { pid: number; started?: string }

/** The manager's own record of `pid` now: its start time when it can be read (Linux /proc), for pid-reuse checks. */
export const ownedProcess = (sessionId: unknown): OwnedProcess | undefined => {
  const pid = Number(sessionId);
  if (!Number.isSafeInteger(pid) || pid <= 1 || pid === process.pid) return undefined;
  const started = processStartTime(pid);
  return { pid, ...(started !== undefined ? { started } : {}) };
};

/**
 * pi-fabric#939 round 1: proven gone from the manager's OWN record only. The recorded pid is not alive, or it is
 * alive with a different start time (reused). A live pid with no recorded birth is not proven gone.
 */
export const ownedProcessGone = (owned: OwnedProcess): boolean => {
  if (!processAlive(owned.pid)) return true;
  const current = processStartTime(owned.pid);
  return owned.started !== undefined && current !== undefined && current !== owned.started;
};

/** The worker's saved answer for run `directory` (its settlement.json receipt for exactly this run), else undefined. */
export const savedEnodevAnswer = (directory: string): string | undefined => {
  const id = path.basename(directory);
  const settlement = readJson(path.join(directory, "settlement.json"));
  return settlement?.version === 1 && settlement.runId === id && settlement.outcome === "completed" &&
    typeof settlement.text === "string" ? settlement.text : undefined;
};

/**
 * pi-fabric#939 round 3 (SEC 1): the exact gate, checked BEFORE anything is delivered or written. The manager's own
 * pid record proves the worker gone, the run carries the exact ENODEV marker, and its status record is this process
 * run. A missing, malformed or other-reason marker, or a foreign status record, keeps the veto. Returns the record.
 */
export const provenGoneEnodevLaunch = (directory: string, owned: OwnedProcess | undefined): Record<string, unknown> | undefined => {
  if (!owned || !ownedProcessGone(owned)) return undefined;
  const id = path.basename(directory);
  const marker = readJson(path.join(directory, UNRESOLVED_WORKER_FILE));
  if (typeof marker?.reason !== "string" || !marker.reason.startsWith(ENODEV_UNRESOLVED_REASON)) return undefined;
  const record = readJson(path.join(directory, "status.json"));
  return record && record.id === id && record.transport === "process" ? record : undefined;
};

/**
 * pi-fabric#1034 round 7: a Pi run promoted here from a running checkpoint did not end cleanly; what ran after the
 * checkpoint is unknown, so its receipt (or an empty one) is marked truncated. A terminal record keeps its own.
 */
export const uncleanReceipt = (record: Record<string, unknown>): { writeReceipt?: Record<string, unknown> } => {
  if (record.runner !== "pi" && record.runner !== "pi-durable") return {};
  if (record.status !== "running" && record.status !== "queued") return {};
  const receipt = typeof record.writeReceipt === "object" && record.writeReceipt !== null
    ? record.writeReceipt as Record<string, unknown> : { paths: [], commands: [] };
  return { writeReceipt: { ...receipt, truncated: true } };
};

/** The terminal record settleGoneEnodevLaunch writes for a proven-gone run, WITHOUT writing it (round 5: save first). */
export const goneEnodevOutcome = (directory: string, record: Record<string, unknown>): AgentRunResult => {
  const id = path.basename(directory);
  const text = savedEnodevAnswer(directory);
  const stopIntent = readRunStopIntent(directory, record);
  const terminal = record.status !== "running" && record.status !== "queued" ? {}
    : text !== undefined ? { status: "completed", text }
    : stopIntent?.reason === "explicit" ? { status: "stopped", error: "Agent stopped" }
    : stopIntent?.reason === "deadline" ? { status: "timed_out", error: "Agent timed out" }
    : { status: "failed", settledLost: true, error: "lost: the worker exit was unconfirmed (ENODEV) and no answer was saved" };
  return { ...record, id, ...terminal, ...uncleanReceipt(record), finishedAt: typeof record.finishedAt === "number" ? record.finishedAt : Date.now() } as unknown as AgentRunResult;
};

/**
 * Settles a launch proven gone (provenGoneEnodevLaunch). `settledHere` names a run THIS manager already settled through
 * #912's recovery (its marker is gone by that settle): only then does a missing marker count, and nothing is written.
 */
export const settleGoneEnodevLaunch = (directory: string, owned: OwnedProcess | undefined, settledHere = false): AgentRunResult | undefined => {
  const id = path.basename(directory);
  const statusFile = path.join(directory, "status.json");
  if (settledHere) {
    if (!owned || !ownedProcessGone(owned)) return undefined;
    return readJson(statusFile) as unknown as AgentRunResult | undefined ?? ({ id } as unknown as AgentRunResult);
  }
  const record = provenGoneEnodevLaunch(directory, owned);
  if (!record) return undefined;
  const settlement = readJson(path.join(directory, "settlement.json"));
  const text = settlement?.version === 1 && settlement.runId === id && settlement.outcome === "completed" &&
    typeof settlement.text === "string" ? settlement.text : undefined;
  const run: EnodevRun = { id, statusFile, record, activationId: "", ...(text === undefined ? {} : { text }) };
  const stopIntent = readRunStopIntent(directory, record);
  const terminal = record.status !== "running" && record.status !== "queued" ? {}
    : text !== undefined ? { status: "completed", text }
    : stopIntent?.reason === "explicit" ? { status: "stopped", error: "Agent stopped" }
    : stopIntent?.reason === "deadline" ? { status: "timed_out", error: "Agent timed out" }
    : { status: "failed", settledLost: true, error: "lost: the worker exit was unconfirmed (ENODEV) and no answer was saved" };
  return settle(directory, run, terminal);
};

/**
 * The saved answer for this restored actor activation when every fact above holds, WITHOUT settling: the marker and
 * the record stay until the actor delivered it and commits (settleEnodevRun), so a delivery its ownership check
 * refuses keeps both for the next owner (pi-fabric#912 round 3).
 */
export const peekEnodevRun = (directory: string, actorId: string, activationId: string): AgentRunResult | undefined => {
  const run = inspect(directory, actorId);
  if (!run || run.activationId !== activationId) return undefined;
  const stopIntent = readRunStopIntent(directory, run.record);
  if (run.text === undefined && !stopIntent) return undefined;
  const stopped = run.text === undefined && stopIntent?.reason === "explicit";
  const timedOut = run.text === undefined && stopIntent?.reason === "deadline";
  return { ...run.record, id: run.id, status: stopped ? "stopped" : timedOut ? "timed_out" : "completed",
    ...(run.text !== undefined ? { text: run.text } : {}),
    ...(stopped ? { error: "Agent stopped" } : timedOut ? { error: "Agent timed out" } : {}),
    ...uncleanReceipt(run.record), finishedAt: typeof run.record.finishedAt === "number" ? run.record.finishedAt : Date.now() } as unknown as AgentRunResult;
};

/** Settles `directory` for this restored actor activation when every fact above holds; undefined keeps the veto. */
export const settleEnodevRun = (directory: string, actorId: string, activationId: string): AgentRunResult | undefined => {
  const run = inspect(directory, actorId);
  if (!run || run.activationId !== activationId) return undefined;
  if (run.text !== undefined) return settle(directory, run, { status: "completed", enodevDelivered: true, text: run.text });
  const stopIntent = readRunStopIntent(directory, run.record);
  if (stopIntent?.reason === "explicit") return settle(directory, run, { status: "stopped", error: "Agent stopped" });
  if (stopIntent?.reason === "deadline") return settle(directory, run, { status: "timed_out", error: "Agent timed out" });
  return undefined;
};

/**
 * smarty-dev#9855: an ENODEV-unconfirmed TASK-AGENT run (no actorId; #912 recovers actor runs) found by offline
 * retention, whose manager is gone. The same facts as #912: the exact ENODEV marker, this run's own process status
 * record, its worker proven gone (pid-reuse safe). Then it settles the run LOST, once, and delivers nothing: a
 * record its manager already made terminal keeps its outcome and gains settledLost; a running one becomes completed
 * when its worker saved an answer (a settlement.json receipt for exactly this run), or failed with none. Recovery never
 * writes answer text into status.json: a saved answer stays only in settlement.json. The marker goes last, so retention can collect the run. Nothing is deleted here. Undefined keeps the
 * veto. Delivery was removed in pi-fabric#1027 review round 1 (principal-data P1): a same-uid worker can rewrite the
 * run's recipient manifest and its exit proof, so no answer recovered here may cross to another session.
 */
export const recoverEnodevTaskRun = (directory: string): "lost" | undefined => {
  const id = path.basename(directory);
  if (!RUN_ID.test(id)) return undefined;
  const marker = readJson(path.join(directory, UNRESOLVED_WORKER_FILE));
  if (typeof marker?.reason !== "string" || !marker.reason.startsWith(ENODEV_UNRESOLVED_REASON)) return undefined;
  const statusFile = path.join(directory, "status.json");
  const record = readJson(statusFile);
  if (!record || record.id !== id || record.transport !== "process" || record.actorId !== undefined) return undefined;
  if (!enodevWorkerGone(record)) return undefined;
  const saved = savedEnodevAnswer(directory) !== undefined;
  const stopIntent = readRunStopIntent(directory, record);
  const terminal = record.status !== "running" && record.status !== "queued";
  // settle() writes the record plus these fields only: no answer text is added (or removed).
  settle(directory, { id, statusFile, record, activationId: "" }, terminal ? { settledLost: true } : saved
    ? { status: "completed", settledLost: true }
    : stopIntent?.reason === "explicit" ? { status: "stopped", error: "Agent stopped" }
    : stopIntent?.reason === "deadline" ? { status: "timed_out", error: "Agent timed out" }
    : { status: "failed", settledLost: true, error: "lost: the worker exit was unconfirmed (ENODEV), no answer was saved and its manager is gone" });
  return "lost";
};

/**
 * pi-fabric#912 round 4, crash safety. The actor writes this durable receipt when it delivered a recovered answer,
 * BEFORE it acknowledges the activation in its queue and long before the marker goes. Each crash point then has one
 * outcome on restart:
 * - before the receipt, or after it but before the queue ack: the activation is still pending, the marker is there,
 *   and the answer is recovered again (at least once; no rerun);
 * - after the queue ack, before the commit: the activation is no longer pending; the lost sweep finds the receipt
 *   and settles the run as delivered (enodevDelivered), not lost.
 * The marker is removed only by the commit, after the queue ack.
 */
export const ENODEV_DELIVERED_FILE = "enodev-delivered.json";
export const noteEnodevDelivered = (directory: string, runId: string, activationId: string): void =>
  writeJsonAtomic(path.join(directory, ENODEV_DELIVERED_FILE), { version: 1, runId, activationId, deliveredAt: Date.now() }, { durable: true });
export const enodevDelivered = (directory: string, runId: string, activationId: string): boolean => {
  const receipt = readJson(path.join(directory, ENODEV_DELIVERED_FILE));
  return receipt?.version === 1 && receipt.runId === runId && receipt.activationId === activationId;
};

/**
 * The activation this run served is no longer pending for its actor (dropped, e.g. "restored after three restarts"),
 * so no run request will ever claim it: settle it as lost, without delivery, so retention can collect it. The
 * worker must still be proven gone. Returns the activation id when it settled.
 */
export const settleLostEnodevRun = (directory: string, actorId: string, pending: ReadonlySet<string>): string | undefined => {
  const run = inspect(directory, actorId);
  if (!run || pending.has(run.activationId)) return undefined;
  // Crash after delivery and the queue ack, before the commit (#912 round 4): it WAS delivered; settle it so.
  if (run.text !== undefined && enodevDelivered(directory, run.id, run.activationId)) {
    settle(directory, run, { status: "completed", enodevDelivered: true, text: run.text });
    return run.activationId;
  }
  // settledLost marks it for the install check (pi-fabric#912): a lost run's answer reached no actor.
  const stopIntent = readRunStopIntent(directory, run.record);
  settle(directory, run, run.text !== undefined
    ? { status: "completed", settledLost: true, text: run.text }
    : stopIntent?.reason === "explicit" ? { status: "stopped", error: "Agent stopped" }
    : stopIntent?.reason === "deadline" ? { status: "timed_out", error: "Agent timed out" }
    : { status: "failed", settledLost: true, error: "lost: the worker exit was unconfirmed (ENODEV) and its activation is no longer pending" });
  return run.activationId;
};
