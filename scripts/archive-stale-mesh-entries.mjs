#!/usr/bin/env node
// Operator-only entry: never infer or open a default/live mesh.
import { archiveStaleMeshEntriesCli } from "../dist/topology/stale-mesh-entries.js";

try {
  console.log(JSON.stringify(await archiveStaleMeshEntriesCli(process.argv.slice(2)), null, 2));
} catch (error) {
  console.error(`archive-stale-mesh-entries: ${error instanceof Error ? error.message : String(error)}`);
  process.exitCode = 1;
}
