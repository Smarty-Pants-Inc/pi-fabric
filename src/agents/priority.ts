import { childProcessEnvironment } from "../core/atomic-write.js";
import os from "node:os";
import { spawnSync } from "node:child_process";

/** Highest Unix niceness; agents.nice and per-spawn nice are clamped to 0..19. */
export const MAX_AGENT_NICE = 19;

/** Parse a per-spawn nice value: a finite number, clamped to an integer 0..19. Non-numbers throw. */
export function parseAgentNice(value: number): number;
export function parseAgentNice(value: unknown): number | undefined;
export function parseAgentNice(value: unknown): number | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new Error(`Invalid nice: ${String(value)}; expected an integer 0-${MAX_AGENT_NICE}`);
  }
  return Math.min(MAX_AGENT_NICE, Math.max(0, Math.floor(value)));
}

/** A per-spawn value can only raise niceness above the configured floor, never lower it. */
export const effectiveAgentNice = (configured: number, requested?: number): number =>
  Math.min(MAX_AGENT_NICE, Math.max(configured, requested ?? 0));

export interface ChildPriorityDeps {
  getPriority: (pid: number) => number;
  setPriority: (pid: number, priority: number) => void;
  ionice: (pid: number) => { error?: Error; status: number | null; stderr?: string | Buffer | null };
  platform: NodeJS.Platform;
  log: (message: string) => void;
}

// Failures are logged once per process per kind, never thrown.
const loggedFailures = new Set<string>();

const logOnce = (log: (message: string) => void, kind: string, message: string): void => {
  if (loggedFailures.has(kind)) return;
  loggedFailures.add(kind);
  log(message);
};

/** Test hook: forget which failures were already logged. */
export const resetChildPriorityLog = (): void => loggedFailures.clear();

const defaultDeps = (log: (message: string) => void): ChildPriorityDeps => ({
  getPriority: (pid) => os.getPriority(pid),
  setPriority: (pid, priority) => os.setPriority(pid, priority),
  ionice: (pid) => spawnSync("ionice", ["-c2", "-n7", "-p", String(pid)], { env: childProcessEnvironment(), stdio: ["ignore", "ignore", "pipe"], timeout: 5_000 }),
  platform: process.platform,
  log,
});

/**
 * Lower a process's CPU priority (and IO priority on Linux) to `nice`. The worker
 * calls it on itself before it spawns the CLI: on Linux both are per thread and
 * inherited at fork, so the child, all its threads and its tools start lowered.
 * Already lower (a niced host) is kept: an unprivileged process cannot raise it.
 * Best effort: on Windows os.setPriority maps to a priority class.
 */
export const applyChildPriority = (
  pid: number | undefined,
  nice: number,
  log: (message: string) => void,
  deps: ChildPriorityDeps = defaultDeps(log),
): void => {
  if (!pid || nice <= 0) return;
  try {
    let current = 0;
    try { current = deps.getPriority(pid); } catch { /* unknown: try to set it */ }
    if (current < nice) deps.setPriority(pid, Math.min(MAX_AGENT_NICE, nice));
  } catch (error) {
    logOnce(deps.log, "setPriority", `agents.nice: setPriority(${pid}, ${nice}) failed: ${error instanceof Error ? error.message : String(error)}`);
  }
  if (deps.platform !== "linux") return;
  try {
    const result = deps.ionice(pid);
    // ENOENT: ionice is not installed; skip it silently.
    if ((result.error as NodeJS.ErrnoException | undefined)?.code === "ENOENT") return;
    if (result.error || result.status !== 0) {
      const detail = result.error?.message ?? String(result.stderr ?? "").trim() ?? `exit ${result.status}`;
      logOnce(deps.log, "ionice", `agents.nice: ionice -c2 -n7 -p ${pid} failed: ${detail || `exit ${result.status}`}`);
    }
  } catch (error) {
    logOnce(deps.log, "ionice", `agents.nice: ionice failed: ${error instanceof Error ? error.message : String(error)}`);
  }
};
