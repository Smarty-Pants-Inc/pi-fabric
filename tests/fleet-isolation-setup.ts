import { readFileSync } from "node:fs";
import { isolateTestFleetEnvironment } from "../scripts/test-temp.js";

// Vitest runs setup before each test file's static imports. Config already scrubbed
// the parent; give this file (and children spawned by it) its own private state.
// Tests can still opt into specific behavior with vi.stubEnv after this boundary.
isolateTestFleetEnvironment();

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
