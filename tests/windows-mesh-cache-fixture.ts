import fs from "node:fs";
import os from "node:os";
import path from "node:path";

// Capture the hosted user's real profile before a negative temp-policy test overrides its environment.
// Never fall back to RUNNER_TEMP/os.tmpdir(): its NETWORK SERVICE-owned ancestry is legitimately untrusted.
const localAppData = process.env.LOCALAPPDATA;

export const windowsMeshLocalAppData = (): string => {
  if (process.platform !== "win32" || os.type() !== "Windows_NT") throw new Error("real Windows profile required");
  if (!localAppData || !path.isAbsolute(localAppData)) throw new Error("actual absolute LOCALAPPDATA required for cache fixtures");
  return fs.realpathSync.native(localAppData);
};

/** A uniquely test-owned allocation beneath the actual user profile, NOT the default pi-fabric/native cache.
 * Callers register this exact directory for teardown, after all cache users/children have joined.
 * No environment override or ACL repair: the production bootstrap still judges the real ancestry.
 */
export const windowsMeshCacheFixture = (prefix: string): string =>
  fs.realpathSync.native(fs.mkdtempSync(path.join(windowsMeshLocalAppData(), prefix)));
