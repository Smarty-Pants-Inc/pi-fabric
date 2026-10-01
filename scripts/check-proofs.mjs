#!/usr/bin/env node
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../", import.meta.url));
// The fork owns its workflow and still provisions Bend 2.0.26. Never silently
// downgrade proof reproduction to that compiler. CI checks the pinned artifact;
// contributors and publishing must reproduce with 2.0.34 explicitly.
const artifactOnly = process.env.GITHUB_ACTIONS === "true" && process.env.CI === "true";
if (artifactOnly) console.log("GitHub Actions: artifact hash verification only; Bend proof reproduction is not claimed.");
const checks = artifactOnly
  ? [["scripts/verified-kernels.mjs", "--artifact"]]
  : [["scripts/verified-kernels.mjs", "--check"], ["scripts/test-verified-kernels.mjs"]];
for (const args of checks) {
  const result = spawnSync(process.execPath, args, { cwd: root, env: process.env, stdio: "inherit" });
  if (result.error) throw result.error;
  if (result.status !== 0) process.exit(result.status ?? 1);
}
