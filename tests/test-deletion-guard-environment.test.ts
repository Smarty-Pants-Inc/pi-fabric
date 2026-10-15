import { tmpdir } from "node:os";
import { expect, it } from "vitest";

// Data-safety boundary: malformed BUN_OPTIONS must not drop the child's guard.
// Existing test-deletion-guard-bun/child-process suites exercise real children;
// these cases cover the Windows paths that Linux cannot produce natively.
const { pinnedBunEnvironment } = await import(new URL("./test-deletion-guard.mjs", import.meta.url).href);

it.each([
  [String.raw`C:\a\pi-fabric\tests\test-deletion-guard.mjs`, "--preload=C:/a/pi-fabric/tests/test-deletion-guard.mjs"],
  [String.raw`\\server\share\tests\test-deletion-guard.mjs`, "--preload=//server/share/tests/test-deletion-guard.mjs"],
])("serializes a simulated Windows preload path %s without JSON path escapes", (path, token) => {
  const original = { BUN_OPTIONS: "--smol", ONLY: "keep" };
  const env = pinnedBunEnvironment(original, tmpdir(), path, "win32");
  expect(env).toEqual({ ...original, BUN_OPTIONS: `--smol ${token}`, TEST_DELETION_GUARD_ROOT: tmpdir() });
  expect(pinnedBunEnvironment(env, tmpdir(), path, "win32")).toEqual(env);
  expect(original).toEqual({ BUN_OPTIONS: "--smol", ONLY: "keep" });
});
