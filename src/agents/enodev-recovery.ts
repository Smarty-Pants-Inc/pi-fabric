import fs from "node:fs";
import path from "node:path";
import { writeJsonAtomic } from "../core/atomic-write.js";
import { processStartTime } from "../residency/process-identity.js";
import { ownedStat, processAlive } from "../storage/scratch.js";
import { UNRESOLVED_WORKER_FILE } from "../storage/retention.js";
import { commitRunArchive } from "./archive-custody.js";
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
 */
export const ENODEV_UNRESOLVED_REASON = "Owned scope observation failed: Error: ENODEV";
const RUN_ID = /^[a-f0-9]{32}$/;
const MAX_FILE = 4 * 1024 * 1024;

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
    return typeof marker?.reason === "string" && marker.reason.startsWith(ENODEV_UNRESOLVED_REASON);
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
        console.warn(`[pi-fabric] actor ${actorId}: could not save unknownStarts ${next} in ${file}: ${String(error)} (smarty-dev#8935)`);
        if (unknown) { highest = Math.max(highest, UNKNOWN_STARTS_ALARM); alarm = true; }
      }
    }
  }
  return { starts: highest, alarm };
};

/** The actor activation a run served: the top-level `id` of the item JSON its task carries (ActorManager#runRequest). */
const activationOf = (directory: string): string | undefined => {
  try {
    const quoted = /^  "id": ("(?:[^"\\]|\\.)*")$/m.exec(fs.readFileSync(path.join(directory, "task.txt"), "utf8"))?.[1];
    return quoted === undefined ? undefined : JSON.parse(quoted) as string;
  } catch { return undefined; }
};

interface EnodevRun { id: string; statusFile: string; record: Record<string, unknown>; activationId: string; text?: string }
/** Every fact except the settlement receipt: the ENODEV marker, the actor's process run, its activation, a gone worker. */
const inspect = (directory: string, actorId: string): EnodevRun | undefined => {
  const id = path.basename(directory);
  const marker = readJson(path.join(directory, UNRESOLVED_WORKER_FILE));
  if (typeof marker?.reason !== "string" || !marker.reason.startsWith(ENODEV_UNRESOLVED_REASON)) return undefined;
  const statusFile = path.join(directory, "status.json");
  const record = readJson(statusFile);
  if (!record || record.id !== id || record.actorId !== actorId || record.transport !== "process") return undefined;
  const activationId = activationOf(directory);
  if (activationId === undefined || !enodevWorkerGone(record)) return undefined;
  const settlement = readJson(path.join(directory, "settlement.json"));
  const text = settlement?.version === 1 && settlement.runId === id && settlement.outcome === "completed" &&
    typeof settlement.text === "string" ? settlement.text : undefined;
  return { id, statusFile, record, activationId, ...(text === undefined ? {} : { text }) };
};

/** Writes the terminal record, discharges the archive custody, and removes the marker LAST (so it runs once). */
const settle = (directory: string, run: EnodevRun, terminal: Record<string, unknown>): AgentRunResult => {
  const result = { ...run.record, id: run.id, ...terminal,
    finishedAt: typeof run.record.finishedAt === "number" ? run.record.finishedAt : Date.now() } as unknown as AgentRunResult;
  writeJsonAtomic(run.statusFile, result, { durable: true });
  try { commitRunArchive(directory, "settlement"); } catch { /* no awaiting archive: nothing to discharge */ }
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

/** The terminal record settleGoneEnodevLaunch writes for a proven-gone run, WITHOUT writing it (round 5: save first). */
export const goneEnodevOutcome = (directory: string, record: Record<string, unknown>): AgentRunResult => {
  const id = path.basename(directory);
  const text = savedEnodevAnswer(directory);
  const terminal = record.status !== "running" && record.status !== "queued" ? {}
    : text !== undefined ? { status: "completed", text }
    : { status: "failed", settledLost: true, error: "lost: the worker exit was unconfirmed (ENODEV) and no answer was saved" };
  return { ...record, id, ...terminal, finishedAt: typeof record.finishedAt === "number" ? record.finishedAt : Date.now() } as unknown as AgentRunResult;
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
  const terminal = record.status !== "running" && record.status !== "queued" ? {}
    : text !== undefined ? { status: "completed", text }
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
  if (!run || run.activationId !== activationId || run.text === undefined) return undefined;
  return { ...run.record, id: run.id, status: "completed", text: run.text,
    finishedAt: typeof run.record.finishedAt === "number" ? run.record.finishedAt : Date.now() } as unknown as AgentRunResult;
};

/** Settles `directory` for this restored actor activation when every fact above holds; undefined keeps the veto. */
export const settleEnodevRun = (directory: string, actorId: string, activationId: string): AgentRunResult | undefined => {
  const run = inspect(directory, actorId);
  if (!run || run.activationId !== activationId || run.text === undefined) return undefined;
  return settle(directory, run, { status: "completed", enodevDelivered: true, text: run.text });
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
  settle(directory, run, run.text === undefined
    ? { status: "failed", settledLost: true, error: "lost: the worker exit was unconfirmed (ENODEV) and its activation is no longer pending" }
    : { status: "completed", settledLost: true, text: run.text });
  return run.activationId;
};
