// A resident launcher that starts, spawns a child into its own process group,
// and then never produces a host: a start that outlives any budget.
import { spawn } from "node:child_process";
import fs from "node:fs";
import path from "node:path";

const config = process.argv[process.argv.indexOf("--config") + 1];
const dir = path.dirname(config);
const child = spawn(process.execPath, ["-e", "setInterval(() => {}, 1000)"], { stdio: "ignore" });
fs.appendFileSync(path.join(dir, "launcher.log"), `${JSON.stringify({ event: "launcher-started", at: Date.now(), pid: process.pid })}\n`);
fs.writeFileSync(path.join(dir, "stalled-child.pid"), String(child.pid));
setInterval(() => {}, 1000);
