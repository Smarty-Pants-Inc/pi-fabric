import fs from "node:fs";

interface CommitStats {
  record(bytes: number, keys: readonly string[]): void;
}
interface ProcessCommitStats {
  /** Registry generation, not the JSONL schema version. The counter API stays stable. */
  version: number;
  file: string | undefined;
  counter: CommitStats | undefined;
  dispose(): void;
}
const processKey = Symbol.for("pi-fabric.mesh.commit-stats");
const processGlobals = globalThis as typeof globalThis & { [processKey]?: ProcessCommitStats };

/**
 * Capture the opt-in once, including disabled, across module/release reloads.
 * Older and newer generations reuse the stable record API, original sink and minute
 * boundary rather than replacing the owner. Keep this registry contract compatible:
 * a future implementation must hand over explicitly before changing that contract.
 */
export const createCommitStats = (file = process.env.PI_FABRIC_COMMIT_STATS): CommitStats | undefined => {
  const existing = processGlobals[processKey];
  if (existing) return existing.counter;
  if (!file) {
    processGlobals[processKey] = { version: 1, file, counter: undefined, dispose() {} };
    return undefined;
  }
  let since = Date.now();
  let commits = 0;
  let bytesWritten = 0;
  let byReason: Record<string, { commits: number; bytesWritten: number }> = Object.create(null);
  const reasonOf = (key: string): string => key.startsWith("sessions/") ? "legacy-session"
    : key.startsWith("topology/hosts/") ? "host-lease"
    : key.startsWith("topology/participants/") ? "participant"
    : key.startsWith("topology/") ? "topology" : key.split("/")[0] || "state";
  const timer = setInterval(() => {
    const at = Date.now();
    try {
      fs.appendFileSync(file, JSON.stringify({ version: 1, pid: process.pid, since, at,
        commits, bytesWritten, byReason }) + "\n", { mode: 0o600 });
      since = at;
      commits = 0;
      bytesWritten = 0;
      byReason = Object.create(null);
    } catch { /* Diagnostics must not fail or change a committed operation; retry next minute. */ }
  }, 60_000);
  timer.unref();
  const dispose = () => {
    clearInterval(timer);
    process.removeListener("exit", dispose);
  };
  const counter: CommitStats = { record(bytes, keys) {
    const reason = [...new Set(keys.map(reasonOf))].sort().join("+") || "state";
    const bucket = byReason[reason] ??= { commits: 0, bytesWritten: 0 };
    bucket.commits++;
    bucket.bytesWritten += bytes;
    commits++;
    bytesWritten += bytes;
  } };
  processGlobals[processKey] = { version: 1, file, counter, dispose };
  process.once("exit", dispose);
  return counter;
};
