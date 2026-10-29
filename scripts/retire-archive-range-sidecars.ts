#!/usr/bin/env bun
import path from "node:path";
import { retireArchiveRangeSidecars } from "../src/mesh/archive-range-migration.js";

// Explicit administrative action: range reads themselves never run migrations.
const args = process.argv.slice(2);
if (args.length !== 1 || !path.isAbsolute(args[0]!)) {
  console.error("Usage: bun scripts/retire-archive-range-sidecars.ts ABSOLUTE_ARCHIVE_ROOT");
  process.exitCode = 2;
} else {
  console.log(JSON.stringify({ version: 1, moved: retireArchiveRangeSidecars(args[0]!) }, null, 2));
}
