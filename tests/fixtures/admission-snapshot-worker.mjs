import fs from "node:fs";
import path from "node:path";

// No provider, Pi runtime, system bus or inherited coordination channel. The
// caller owns this private directory and explicitly commands progress/exit.
const args = new Map();
for (let index = 2; index < process.argv.length; index += 2) {
  args.set(process.argv[index].slice(2), process.argv[index + 1]);
}
const task = fs.readFileSync(args.get("task-file"), "utf8");
// Also accepts the payload embedded in ActorManager's real activation envelope.
const match = task.match(/"admissionControlRoot"\s*:\s*("(?:\\.|[^"\\])*")/);
if (!match) throw new Error("Admission fixture requires admissionControlRoot");
const controlRoot = JSON.parse(match[1]);
const statusFile = args.get("status-file");
const lifecycleFile = args.get("lifecycle-file");
const startedAt = Date.now();
const running = {
  id: args.get("id"), name: args.get("name"), task, status: "running",
  runner: args.get("runner") ?? "pi", transport: args.get("transport"),
  sessionId: String(process.pid), cwd: args.get("cwd"), model: args.get("model"),
  startedAt, updatedAt: startedAt, turns: 0, toolCalls: 0, text: "", exitCode: null,
  usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
};
let writes = 0;
function writeStatus(record) {
  const temporary = `${statusFile}.${process.pid}.${++writes}.tmp`;
  fs.writeFileSync(temporary, JSON.stringify(record), { mode: 0o600 });
  for (let attempt = 0; ; attempt++) {
    try { fs.renameSync(temporary, statusFile); return; } catch (error) {
      if (process.platform !== "win32" || attempt >= 50 || !["EPERM", "EACCES", "EBUSY"].includes(error?.code)) throw error;
      Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 10);
    }
  }
}

writeStatus(running);
fs.writeFileSync(path.join(controlRoot, "ready"), "ready", { mode: 0o600 });
let events = 0;
const deadline = startedAt + 20_000;
while (!fs.existsSync(path.join(controlRoot, "release")) && !fs.existsSync(path.join(controlRoot, "fail"))) {
  if (Date.now() >= deadline) throw new Error("Admission fixture control deadline exceeded");
  while (fs.existsSync(path.join(controlRoot, `event-${events + 1}`))) {
    events++;
    // Deliberately do NOT rewrite status: this proves the existing lifecycle
    // ingestion path, not a changing updatedAt or a synthetic heartbeat.
    fs.appendFileSync(lifecycleFile, JSON.stringify({
      version: 1, event: "pi.turn_end", occurredAt: Date.now(), data: { turnIndex: events },
    }) + "\n", { mode: 0o600 });
    fs.writeFileSync(path.join(controlRoot, `event-${events}.done`), "emitted", { mode: 0o600 });
  }
  // Explicit signal files are the event source; this fixture-only 20 ms bounded
  // check observes them. No production monitor/timer is installed or changed.
  await new Promise((resolve) => setTimeout(resolve, 20));
}
const failed = fs.existsSync(path.join(controlRoot, "fail"));
const finishedAt = Date.now();
const value = { action: "silent" };
writeStatus({
  ...running, status: failed ? "failed" : "completed", updatedAt: finishedAt,
  finishedAt, turns: events, text: JSON.stringify(value), value, replyVia: "tool",
  exitCode: failed ? 1 : 0,
  ...(failed ? { error: "Admission fixture requested failure" } : {}),
});
process.exitCode = failed ? 1 : 0;
