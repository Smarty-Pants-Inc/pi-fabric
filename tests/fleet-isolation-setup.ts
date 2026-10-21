import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { isolateTestFleetEnvironment, testTempParentEnvironment } from "../scripts/test-temp.js";

const testTempParent = process.env[testTempParentEnvironment];
if (!testTempParent) throw new Error("Vitest test temp parent is missing");
const ownedTestTempParent = resolve(testTempParent);
// smarty-dev#11145: the guard remains pinned to process TMPDIR. The one unique, private
// harness-owned sibling parent is registered explicitly so its fixtures can be cleaned safely.
// Install before test imports. Node children inherit the same module through NODE_OPTIONS --import.
const deletionGuard = await import(new URL("./test-deletion-guard.mjs", import.meta.url).href);
deletionGuard.allowTestDeletionRoot(ownedTestTempParent);

// Vitest runs setup before each test file's static imports. Config already scrubbed
// the parent; give this file its own private fleet root beside (not under) its TMPDIR.
// Tests can still opt into specific behavior with vi.stubEnv after this boundary.
isolateTestFleetEnvironment(ownedTestTempParent);

// smarty-dev#6477: production sqlite mode opens only an imported root. In-process test fixtures that
// build fresh sqlite roots keep doing so (state.db plus the moved marker, as an import leaves a fresh
// root); tests of the production rule delete this symbol. No import: setup stays free of the mesh graph.
(globalThis as Record<symbol, unknown>)[Symbol.for("pi-fabric.mesh.sqlite-initialize.test-fixtures")] = "create";

// smarty-dev#7936: tests run src, where the build-time define of the helpers' digests (scripts/build.mjs) is
// absent; the bare identifier then resolves to this global. In dist the identifier is a literal.
{
  let digests = "{}";
  try { digests = JSON.stringify((JSON.parse(readFileSync("dist/native/manifest.json", "utf8")) as { helpers?: unknown }).helpers ?? {}); }
  catch { /* no build: the probe fails closed */ }
  (globalThis as { __FABRIC_NATIVE_DIGESTS__?: string }).__FABRIC_NATIVE_DIGESTS__ = digests;
}
