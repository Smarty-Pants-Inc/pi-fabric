#!/usr/bin/env node
// Installs the packed package the way `pi install` does (no peer and no dev
// dependencies) and starts every entry point that runs outside Pi's module
// aliases. Such a process resolves bare imports from the package's own
// node_modules, so a runtime import declared only as a peer or dev dependency
// fails here with ERR_MODULE_NOT_FOUND.
import { execFileSync, spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

// Spawned by Fabric as their own Node processes (see src/agents and src/memory).
const OUT_OF_PROCESS_ENTRIES = ["dist/worker.js", "dist/memory/file-worker.js"];
const MISSING = /ERR_MODULE_NOT_FOUND|Cannot find (package|module)/;
const npm = process.platform === "win32" ? "npm.cmd" : "npm";

const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-install-"));
try {
  const packed = execFileSync(npm, ["pack", "--silent", "--ignore-scripts", "--pack-destination", root], {
    encoding: "utf8", shell: process.platform === "win32",
  }).trim().split(/\r?\n/).pop();
  const project = path.join(root, "project");
  fs.mkdirSync(project);
  fs.writeFileSync(path.join(project, "package.json"), JSON.stringify({ name: "install-smoke", private: true }));
  // Pi's package manager: npm --legacy-peer-deps (peers are not installed), production only.
  execFileSync(npm, ["install", "--legacy-peer-deps", "--omit=dev", "--ignore-scripts", "--no-audit", "--no-fund",
    path.join(root, packed)], { cwd: project, stdio: "inherit", shell: process.platform === "win32" });
  const installed = path.join(project, "node_modules", "pi-fabric");
  let failed = false;
  for (const entry of OUT_OF_PROCESS_ENTRIES) {
    const result = spawnSync(process.execPath, [path.join(installed, entry)], {
      cwd: project, encoding: "utf8", timeout: 20_000, input: "",
    });
    const output = `${result.stdout ?? ""}${result.stderr ?? ""}`;
    if (MISSING.test(output)) {
      failed = true;
      console.error(`${entry}: missing module in a peer-less install\n${output.split("\n").filter((line) => MISSING.test(line)).slice(0, 3).join("\n")}`);
    } else {
      console.log(`${entry}: all imports resolve (exit ${result.status ?? result.signal})`);
    }
  }
  if (failed) process.exit(1);
} finally {
  fs.rmSync(root, { recursive: true, force: true });
}
