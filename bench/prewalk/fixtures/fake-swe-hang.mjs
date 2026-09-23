#!/usr/bin/env node
// Hostile worker for the SWE coordinator's timeout contract: it ignores
// SIGTERM and leaves a descendant that also ignores SIGTERM, so only a
// process-group termination can end the attempt. It records both pids so the
// test can confirm neither survived.
import { spawn } from "node:child_process";
import fs from "node:fs";

process.on("SIGTERM", () => {});

const pidsFile = process.argv[2];
if (!pidsFile) {
  console.error("usage: fake-swe-hang.mjs <pidsFile>");
  process.exit(2);
}

const descendant = spawn(process.execPath, ["-e", "process.on('SIGTERM',()=>{});setInterval(()=>{},1000);"], {
  stdio: "ignore",
});
fs.writeFileSync(pidsFile, `${process.pid} ${descendant.pid}\n`);
setInterval(() => {}, 1000);
