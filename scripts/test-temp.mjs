import { mkdtempSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import { captureAllocation, captureCleanup } from "./test-temp-capture.mjs";

/** Tests must never prune a real session's caches or leave their own behind. */
export function isolatedTestTemp(prefix, capture = false) {
  const shared = process.env.PI_TEST_TEMP_ROOT;
  if (prefix === "pi-fabric-vitest-" && shared) {
    if (!basename(shared).startsWith(prefix) || !statSync(shared).isDirectory() ||
        ["TMPDIR", "TMP", "TEMP"].some(key => process.env[key] !== shared))
      throw new Error("Invalid test-group temporary directory inheritance");
    if (capture) captureAllocation(shared, true);
    return { TMPDIR: shared, TMP: shared, TEMP: shared };
  }
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

export function testTempEnvironment() {
  // ponytail: MSYS can retain its first /tmp usertemp mapping across runner exits.
  // One batch owner keeps that root alive through every child, then cleans it once.
  const env = isolatedTestTemp("pi-fabric-vitest-", true);
  return { ...env, PI_TEST_TEMP_ROOT: env.TMPDIR };
}
