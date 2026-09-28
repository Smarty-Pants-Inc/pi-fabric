import { mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

// Git Bash (MSYS2) keeps its mount table, with /tmp resolved from the first shell's TEMP, in
// per-user shared memory for as long as any MSYS process lives. A shell a test could not reap
// (a `sleep` orphaned by taskkill /T) pins it, so after one vitest run deletes its temp, every
// Git Bash in the next run starts with "bash.exe: warning: could not find /tmp, please create!"
// on stderr. On a Windows CI runner, reuse one path per job so a pinned /tmp always exists again.
// ponytail: a runner runs one job at a time and RUNNER_TEMP is per runner, so the path is private.
const stableRoot = (prefix: string): string | undefined => {
  const runnerTemp = process.env.RUNNER_TEMP;
  if (process.platform !== "win32" || !runnerTemp) return;
  const directory = join(runnerTemp, `${prefix}job`);
  mkdirSync(directory, { recursive: true });
  return directory;
};

/** Tests must never prune a real session's caches or leave their own behind. */
export function isolatedTestTemp(prefix: string): Record<"TMPDIR" | "TMP" | "TEMP", string> {
  const directory = stableRoot(prefix) ?? mkdtempSync(join(tmpdir(), prefix));
  process.once("exit", () => {
    try {
      rmSync(directory, { recursive: true, force: true, maxRetries: 5, retryDelay: 50 });
    } catch {
      // A killed child may still hold a Windows handle; never mask test results.
    }
  });
  return { TMPDIR: directory, TMP: directory, TEMP: directory };
}
