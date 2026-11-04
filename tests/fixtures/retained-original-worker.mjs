import fs from "node:fs";
import { performance } from "node:perf_hooks";
import { compactTerminalRunLog } from "../../src/worker/run-log.ts";

// Synthetic native worker: real exit, terminal record and real log compaction,
// without inference, native git deletion or any external service.
const args = new Map();
for (let i = 2; i < process.argv.length; i += 2) args.set(process.argv[i].slice(2), process.argv[i + 1]);
const content = [{ type: "text", text: "synthetic-result-".repeat(700) }];
const events = [
  { type: "tool_execution_end", toolCallId: "fixture", toolName: "synthetic", result: { content }, isError: false },
  { type: "message_end", message: { role: "toolResult", toolCallId: "fixture", toolName: "synthetic", content, isError: false } },
  { type: "agent_end", willRetry: false },
];
const log = args.get("log-file");
fs.writeFileSync(log, events.map(event => JSON.stringify(event)).join("\r\n") + "\n", { mode: 0o600 });
// This lifecycle fixture requires a rewrite, not a scheduler-budget skip.
// Separate compactor suites exercise its real work deadline and failure paths.
const clock = performance.now;
let outcome;
try {
  performance.now = () => 0;
  outcome = compactTerminalRunLog(log, "completed");
} finally { performance.now = clock; }
if (outcome.compacted !== 1 || outcome.error) throw new Error(JSON.stringify(outcome));
const now = Date.now();
const failed = fs.readFileSync(args.get("task-file"), "utf8").includes("SYNTHETIC_FAILURE");
if (args.get("lifecycle-file")) fs.writeFileSync(args.get("lifecycle-file"), [
  "pi.agent_start", "pi.turn_end", "pi.agent_end", "pi.agent_settled",
].map(event => JSON.stringify({ version: 1, event, occurredAt: now })).join("\n") + "\n");
const statusFile = args.get("status-file");
const temporary = `${statusFile}.${process.pid}.tmp`;
fs.writeFileSync(temporary, JSON.stringify({
  id: args.get("id"), name: args.get("name"), task: fs.readFileSync(args.get("task-file"), "utf8"),
  runner: args.get("runner") ?? "pi", cwd: args.get("cwd"), status: failed ? "failed" : "completed",
  transport: "process", sessionId: String(process.pid), startedAt: now, updatedAt: now, finishedAt: now,
  turns: 1, toolCalls: 1, text: "synthetic completion", exitCode: 0,
  usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, cost: 0 },
}));
fs.renameSync(temporary, statusFile);
