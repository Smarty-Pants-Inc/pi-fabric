// Offline worker-protocol fixture: session state must survive distinct activations.
import fs from "node:fs";
const context = JSON.parse(fs.readFileSync(process.argv[2], "utf8"));
if (!context.actorId || !context.sessionFile) throw new Error("Missing persistent actor launch context");
const previous = fs.existsSync(context.sessionFile) ? Number(fs.readFileSync(context.sessionFile, "utf8")) : 0;
fs.writeFileSync(context.sessionFile, String(previous + 1));
const now = Date.now();
const text = `ACK ${previous + 1}: ${context.actorName}`;
fs.appendFileSync(context.files.logFile, JSON.stringify({ type: "message_end", message: { role: "assistant", content: text } }) + "\n");
fs.writeFileSync(`${context.files.statusFile}.tmp`, JSON.stringify({
  id: context.id, name: context.name, task: context.task, runner: "custom-actor", transport: "process",
  cwd: context.cwd, status: "completed", startedAt: now, updatedAt: now, finishedAt: now,
  turns: 1, toolCalls: 0, text,
  usage: { input: 0, output: 1, cacheRead: 0, cacheWrite: 0, cost: 0 },
}));
fs.renameSync(`${context.files.statusFile}.tmp`, context.files.statusFile);
