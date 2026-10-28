import fs from "node:fs";
import path from "node:path";

// Same flag/value and atomic status conventions as fake-worker.mjs. This is a
// native worker, not a fake manager: only inference is replaced with fixed text.
const args = new Map();
for (let index = 2; index < process.argv.length; index += 2) {
  args.set(process.argv[index].slice(2), process.argv[index + 1]);
}
const statusFile = args.get("status-file");
const directory = path.dirname(statusFile);
const task = fs.readFileSync(args.get("task-file"), "utf8");
const field = name => {
  const match = task.match(new RegExp(`"${name}":\\s*("(?:\\\\.|[^"\\\\])*")`));
  if (!match) throw new Error(`Missing gone-saved-result fixture field: ${name}`);
  return JSON.parse(match[1]);
};
const reply = field("fixtureReply");
const saveGate = field("fixtureSaveGate");
const exitGate = field("fixtureExitGate");
const writeJson = (file, value) => {
  const temporary = `${file}.${process.pid}.tmp`;
  fs.writeFileSync(temporary, JSON.stringify(value));
  fs.renameSync(temporary, file);
};

// File creation is the normal event source; one deadline bounds a broken test.
const waitForGate = gate => new Promise((resolve, reject) => {
  let finished = false;
  const finish = error => {
    if (finished) return;
    finished = true;
    clearTimeout(timer);
    watcher.close();
    if (error) reject(error); else resolve();
  };
  const watcher = fs.watch(path.dirname(gate), () => {
    if (fs.existsSync(gate)) finish();
  });
  const timer = setTimeout(() => finish(new Error(`Worker gate deadline: ${gate}`)), 30_000);
  watcher.on("error", finish);
  if (fs.existsSync(gate)) finish();
});
let processStartTime;
if (process.platform === "linux") {
  const stat = fs.readFileSync(`/proc/${process.pid}/stat`, "utf8");
  processStartTime = stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/)[19];
}
const now = Date.now();
const record = {
  id: args.get("id"), name: args.get("name"), task, status: "running",
  runner: args.get("runner") ?? "pi", transport: args.get("transport"),
  sessionId: String(process.pid), processStartTime, cwd: args.get("cwd"),
  startedAt: now, updatedAt: now, turns: 1, toolCalls: 0, text: "", exitCode: null,
  usage: { input: 7, output: 11, cacheRead: 0, cacheWrite: 0, cost: 0 },
};
writeJson(statusFile, record);
writeJson(path.join(directory, "fixture-ready.json"), { pid: process.pid, processStartTime, reply });
await waitForGate(saveGate);
const finishedAt = Date.now();
const directive = { action: "message", message: reply, data: { savedByNativeWorker: true } };
const text = JSON.stringify(directive);
// Match the production worker's saved completion and structured reply seams.
writeJson(path.join(directory, "settlement.json"), { version: 1, runId: record.id, outcome: "completed", text });
writeJson(path.join(directory, "reply.json"), directive);
const lifecycle = ["pi.agent_start", "pi.turn_end", "pi.agent_end", "pi.agent_settled"]
  .map(event => ({ version: 1, event, occurredAt: finishedAt }));
fs.writeFileSync(args.get("lifecycle-file"), lifecycle.map(event => JSON.stringify(event)).join("\n") + "\n");
fs.writeFileSync(args.get("log-file"), [
  { type: "agent_start" },
  { type: "message_end", message: { role: "assistant", content: text } },
  { type: "agent_settled" },
].map(event => JSON.stringify(event)).join("\n") + "\n");
// Saving a logical outcome is not an OS-exit observation: preserve null even
// though this fixture will voluntarily exit later, and retain its saved warning.
writeJson(statusFile, { ...record, status: "completed", updatedAt: finishedAt, finishedAt,
  text, value: directive, exitCode: null,
  warnings: ["fixture: terminal result saved before native exit; OS exit unobserved"] });
writeJson(path.join(directory, "fixture-saved.json"), { pid: process.pid, reply, finishedAt });
// Keep the actual process alive after saving a terminal result, until the test
// requests voluntary exit. No fixture signal, detached descendant, or fake PID.
await waitForGate(exitGate);
writeJson(path.join(directory, "fixture-exiting.json"), { pid: process.pid, at: Date.now() });
