import fs from "node:fs";
import path from "node:path";
import { testTempParentEnvironment } from "../../scripts/test-temp.js";

/**
 * Create a fixture root inside the unique private data parent registered with
 * the test deletion guard. Archive tests use these roots so their mesh archive
 * destination is not itself inside process TMPDIR.
 */
export function testTempRoot(prefix: string): string {
  const parent = process.env[testTempParentEnvironment];
  const processTemp = process.env.TMPDIR;
  if (!parent || !processTemp) throw new Error("Vitest private temp roots are not initialized");
  const relativeParent = path.relative(path.resolve(processTemp), path.resolve(parent));
  if (relativeParent === "" || (!path.isAbsolute(relativeParent) && relativeParent !== ".." && !relativeParent.startsWith(`..${path.sep}`))) {
    throw new Error("Test fixture parent must be outside process TMPDIR");
  }
  return fs.mkdtempSync(path.join(parent, prefix));
}
