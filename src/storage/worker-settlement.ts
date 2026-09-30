import fs from "node:fs";
import path from "node:path";
import { processStartIdentityState, validProcessIdentity, type ProcessIdentity } from "../core/process-identity.js";

export interface RunProcessEvidence { id: string; actorId?: string; processes: ProcessIdentity[] }
export interface WorkerSettlementReceipt { format: 1; rootId: string; runs: RunProcessEvidence[] }

const record = (file: string): Record<string, unknown> => {
  if (!fs.lstatSync(file).isFile() || fs.realpathSync.native(file) !== file) throw new Error(`Unknown worker evidence at ${file}`);
  const value: unknown = JSON.parse(fs.readFileSync(file, "utf8"));
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error(`Unknown worker evidence at ${file}`);
  return value as Record<string, unknown>;
};
const directory = (dir: string): string[] => {
  if (!fs.lstatSync(dir).isDirectory() || fs.realpathSync.native(dir) !== path.resolve(dir)) {
    throw new Error(`Cannot verify worker evidence directory ${dir}`);
  }
  return fs.readdirSync(dir);
};

/** Must complete before invoking a transport: an unregistered spawn stays pending forever,
 * rather than allowing a prior attempt's dead worker/runner to authorize deletion. */
export const recordWorkerLaunchAttempt = (dir: string, attempt: string): void => {
  const fd = fs.openSync(path.join(dir, "worker-launches.jsonl"), "a", 0o600);
  try {
    fs.writeFileSync(fd, JSON.stringify({ attempt }) + "\n", "utf8");
    fs.fsyncSync(fd);
  } finally { fs.closeSync(fd); }
};
const journalEntries = (file: string): Array<Record<string, unknown>> => {
  if (!fs.lstatSync(file).isFile() || fs.realpathSync.native(file) !== file) throw new Error(`Unknown worker evidence at ${file}`);
  return fs.readFileSync(file, "utf8").trim().split("\n").map(line => {
    const entry: unknown = JSON.parse(line);
    if (!entry || typeof entry !== "object" || Array.isArray(entry)) throw new Error(`Malformed worker evidence at ${file}`);
    return entry as Record<string, unknown>;
  });
};
const readRun = (dir: string, depth = 0): RunProcessEvidence[] => {
  if (depth > 32) throw new Error("Cannot verify deeply nested workers");
  directory(dir);
  if (fs.existsSync(path.join(dir, "unresolved-worker.json"))) throw new Error(`Unresolved worker at ${dir}`);
  const id = path.basename(dir);
  const status = record(path.join(dir, "status.json"));
  if (status.id !== id) throw new Error(`Run identity mismatch at ${dir}`);
  const file = path.join(dir, "worker-processes.jsonl");
  const launchFile = path.join(dir, "worker-launches.jsonl");
  const attempts = new Set<string>();
  if (fs.existsSync(launchFile)) {
    for (const entry of journalEntries(launchFile)) {
      if (typeof entry.attempt !== "string" || !entry.attempt || attempts.has(entry.attempt)) {
        throw new Error(`Malformed worker launch attempt at ${launchFile}`);
      }
      attempts.add(entry.attempt);
    }
  }
  const registered = new Set<string>();
  const processes: ProcessIdentity[] = [];
  const launched = new Map<string, boolean>();
  for (const entry of journalEntries(file)) {
    if (!validProcessIdentity(entry.worker) || (entry.runner !== undefined && !validProcessIdentity(entry.runner))) {
      throw new Error(`Missing worker/runner process identity at ${file}`);
    }
    if (entry.attempt !== undefined && (typeof entry.attempt !== "string" || !entry.attempt)) {
      throw new Error(`Malformed worker launch attempt at ${file}`);
    }
    const key = JSON.stringify([entry.attempt ?? null, entry.worker]);
    if (typeof entry.attempt === "string" && entry.runner !== undefined) registered.add(entry.attempt);
    launched.set(key, launched.get(key) === true || entry.runner !== undefined);
    processes.push(entry.worker);
    if (validProcessIdentity(entry.runner)) processes.push(entry.runner);
  }
  if (!launched.size || [...launched.values()].some(value => !value)) throw new Error(`Missing runner launch evidence at ${file}`);
  for (const attempt of attempts) {
    if (!registered.has(attempt)) throw new Error(`Unregistered worker launch attempt ${attempt} at ${file}`);
  }
  const runs = [{ id, ...(typeof status.actorId === "string" ? { actorId: status.actorId } : {}), processes }];
  const nested = path.join(dir, "nested");
  if (fs.existsSync(nested)) runs.push(...readRunProcessEvidence(nested, depth + 1));
  return runs;
};

/** Capture every attempt, including recursive workers. Missing evidence is never settlement. */
export const readRunProcessEvidence = (root: string, depth = 0): RunProcessEvidence[] =>
  directory(root).flatMap(id => readRun(path.join(root, id), depth));

/** No signals: kernel absence or a changed kernel start time is the only stop proof.
 * Command-line changes (title rewrites/exec) do not settle a still-live process. */
export const assertRunProcessesSettled = (runs: RunProcessEvidence[]): void => {
  if (!Array.isArray(runs)) throw new Error("Missing predecessor run settlement evidence");
  for (const run of runs) {
    if (!run || typeof run.id !== "string" || !run.id || !Array.isArray(run.processes) || !run.processes.length) {
      throw new Error("Malformed predecessor run settlement evidence");
    }
    for (const identity of run.processes) {
      if (!validProcessIdentity(identity)) throw new Error(`Missing worker process identity for ${run.id}`);
      const state = processStartIdentityState(identity);
      if (state !== "dead" && state !== "mismatch") throw new Error(`Predecessor run ${run.id} worker/runner ${identity.pid} is ${state}; settlement not proven`);
    }
  }
};

/** Preserve Linux journals until all runners/nested workers settle, even if their worker died.
 * Unsupported platforms and legacy owned cleanup still use the established transport proof
 * and unresolved-worker checks; foreign deletion always requires readRun + kernel settlement. */
export const hasUnsettledRecordedProcesses = (runDirectory: string): boolean => {
  if (process.platform !== "linux") return false;
  try {
    fs.lstatSync(path.join(runDirectory, "worker-processes.jsonl"));
    assertRunProcessesSettled(readRun(runDirectory));
    return false;
  } catch (error) {
    return (error as NodeJS.ErrnoException).code !== "ENOENT" || fs.existsSync(path.join(runDirectory, "worker-processes.jsonl"));
  }
};
