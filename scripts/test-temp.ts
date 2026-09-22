import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { captureAllocation, captureCleanup } from "./test-temp-capture.mjs";

/** Tests must never prune a real session's caches or leave their own behind. */
export function isolatedTestTemp(prefix: string, capture = false): Record<"TMPDIR" | "TMP" | "TEMP", string> {
  const directory = mkdtempSync(join(tmpdir(), prefix));
  if (capture) captureAllocation(directory);
  process.once("exit", () => {
    if (capture) captureCleanup("cleanup-before", directory);
    try {
      rmSync(directory, { recursive: true, force: true, maxRetries: 5, retryDelay: 50 });
      if (capture) captureCleanup("cleanup-after", directory);
    } catch (error) {
      if (capture) captureCleanup("cleanup-error", directory, error);
      // A killed child may still hold a Windows handle; never mask test results.
    }
  });
  return { TMPDIR: directory, TMP: directory, TEMP: directory };
}
