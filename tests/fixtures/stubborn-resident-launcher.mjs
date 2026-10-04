// Startup owns both this launcher and its non-detached Pi-shaped child.
// Both refuse TERM, so client cleanup must escalate and confirm their exit.
import { spawn } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
const configPath = process.argv[process.argv.indexOf("--config") + 1];
const dir = path.dirname(configPath);
process.on("SIGTERM", () => {});
const child = spawn(process.execPath, ["-e", `
  const fs = require("node:fs");
  process.on("SIGTERM", () => {});
  fs.writeFileSync(${JSON.stringify(path.join(dir, "stubborn-child-ready"))}, "ready");
  setInterval(() => {}, 1000);
`], { stdio: "ignore" });
fs.appendFileSync(path.join(dir, "launcher.log"), `${JSON.stringify({ event: "launcher-started", at: Date.now(), pid: process.pid })}\n`);
fs.writeFileSync(path.join(dir, "stubborn-child.pid"), String(child.pid));
setInterval(() => {}, 1000);
