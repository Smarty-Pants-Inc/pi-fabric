import fs from "node:fs";

/** One process-wide diagnostic, only constructed when opted in before module load. */
export const createCommitStats = (file: string): { record(bytes: number, keys: readonly string[]): void } => {
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
  return { record(bytes, keys) {
    const reason = [...new Set(keys.map(reasonOf))].sort().join("+") || "state";
    const bucket = byReason[reason] ??= { commits: 0, bytesWritten: 0 };
    bucket.commits++;
    bucket.bytesWritten += bytes;
    commits++;
    bytesWritten += bytes;
  } };
};
