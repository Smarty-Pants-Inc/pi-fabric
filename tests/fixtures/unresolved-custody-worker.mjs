// A real failed custodian, not an injected unresolved-worker.json marker.
// It acquires native execution custody, starts a detached scratch holder, then
// exits without joining it or returning the execution-settled receipt.
import fs from "node:fs";
import path from "node:path";
import { spawn } from "node:child_process";

const args = new Map();
for (let i = 2; i < process.argv.length; i += 2) args.set(process.argv[i], process.argv[i + 1]);
const run = path.dirname(args.get("--status-file"));
const ready = path.join(run, "descendant.json");
if (!process.send) throw new Error("Unresolved custody fixture needs the real POSIX IPC transport");
await new Promise(resolve => {
  process.on("message", message => { if (message?.type === "fabric-execution-custody-ack") resolve(); });
  process.send({ type: "fabric-execution-custody" });
});
const sleeper = spawn(process.execPath, ["--input-type=module", "-e", `
import fs from "node:fs";
import path from "node:path";
const tmpdir = process.env.TMPDIR;
const scratch = path.join(tmpdir, "live-descendant");
fs.writeFileSync(scratch, "still owned by the detached descendant");
const stat = fs.readFileSync("/proc/self/stat", "utf8");
const fields = stat.slice(stat.lastIndexOf(")") + 2).split(" ");
fs.writeFileSync(${JSON.stringify(ready)}, JSON.stringify({pid: process.pid, started: fields[19], group: Number(fields[2]), tmpdir, scratch}));
setInterval(() => {}, 1000);
`], { detached: true, stdio: "ignore", env: process.env });
sleeper.unref();
while (!fs.existsSync(ready)) await new Promise(resolve => setTimeout(resolve, 10));
const descendant = JSON.parse(fs.readFileSync(ready, "utf8"));
const now = Date.now();
fs.writeFileSync(args.get("--status-file"), JSON.stringify({
  id: args.get("--id"), name: args.get("--name"), task: "unresolved descendant custody", status: "completed",
  runner: "pi", transport: "process", sessionId: String(process.pid), cwd: args.get("--cwd"),
  startedAt: now, updatedAt: now, finishedAt: now, turns: 1, toolCalls: 0,
  text: JSON.stringify(descendant), usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
}));
// Custodian crash/early exit: deliberately no fabric-execution-settled message.
process.exit(0);
