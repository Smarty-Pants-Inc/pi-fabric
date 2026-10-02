import fs from "node:fs";
import { execFileSync } from "node:child_process";
import path from "node:path";

const args = new Map();
for (let index = 2; index < process.argv.length; index += 2) {
  args.set(process.argv[index].slice(2), process.argv[index + 1]);
}
const statusFile = args.get("status-file");
const taskFile = args.get("task-file");
if (!statusFile || !taskFile) process.exit(2);
const task = fs.readFileSync(taskFile, "utf8");
const marker = path.join(path.dirname(statusFile), "startup-attempts");
const attempts = fs.existsSync(marker) ? Number(fs.readFileSync(marker, "utf8")) + 1 : 1;
fs.writeFileSync(marker, String(attempts));
const now = Date.now();
const retryable = task !== "Reject startup";
const windowRefusal = task === "window before first turn" || task === "window after work";
let capabilityMissing = false;
if (task === "Recover pinned Pi startup") {
  const binary = args.get("pi-binary");
  const probe = JSON.parse(execFileSync(process.execPath, [binary], { encoding: "utf8" }));
  const turnProvenance = probe.hostCapabilities.turnProvenance;
  capabilityMissing = turnProvenance !== 1;
  fs.appendFileSync(path.join(path.dirname(statusFile), "pi-launches.jsonl"), JSON.stringify({ binary, turnProvenance }) + "\n");
}
const failed = capabilityMissing || windowRefusal || (retryable ? attempts === 1 : true);
fs.writeFileSync(
  statusFile,
  JSON.stringify({
    id: args.get("id"),
    name: args.get("name"),
    task,
    status: failed ? "failed" : "completed",
    runner: args.get("runner") ?? "pi",
    transport: args.get("transport"),
    cwd: args.get("cwd"),
    startedAt: now,
    updatedAt: now,
    finishedAt: now,
    turns: task === "window after work" ? 1 : failed ? 0 : 1,
    toolCalls: 0,
    text: failed ? "" : "startup retry recovered",
    ...(failed
      ? { error: capabilityMissing
          ? "Child Pi exited before requested model admission completed; task was not sent: [pi-fabric] Pi does not advertise hostCapabilities.turnProvenance === 1"
          : windowRefusal
            ? (task === "window after work" ? "Agent transport exited without a result · " : "") + "Context exceeds window: estimated 272511 input tokens, window 272000 · No API key found"
            : retryable ? "No API key found for openai-codex" : "provider rejected the prompt" }
      : {}),
    exitCode: 0,
    usage: failed
      ? { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 }
      : { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, cost: 0 },
  }),
);
