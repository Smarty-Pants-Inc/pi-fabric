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

/** Settles `directory` for this restored actor activation when every fact above holds; undefined keeps the veto. */
export const settleEnodevRun = (directory: string, actorId: string, activationId: string): AgentRunResult | undefined => {
  const run = inspect(directory, actorId);
  if (!run || run.activationId !== activationId || run.text === undefined) return undefined;
  return settle(directory, run, { status: "completed", text: run.text });
};

/**
 * The activation this run served is no longer pending for its actor (dropped, e.g. "restored after three restarts"),
 * so no run request will ever claim it: settle it as lost, without delivery, so retention can collect it. The
 * worker must still be proven gone. Returns the activation id when it settled.
 */
export const settleLostEnodevRun = (directory: string, actorId: string, pending: ReadonlySet<string>): string | undefined => {
  const run = inspect(directory, actorId);
  if (!run || pending.has(run.activationId)) return undefined;
  settle(directory, run, run.text === undefined
    ? { status: "failed", error: "lost: the worker exit was unconfirmed (ENODEV) and its activation is no longer pending" }
    : { status: "completed", text: run.text });
  return run.activationId;
};
