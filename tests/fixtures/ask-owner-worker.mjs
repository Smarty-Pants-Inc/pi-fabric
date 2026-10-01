// Ownership tests control readiness and completion independently of process/bridge speed.
import fs from "node:fs";
import path from "node:path";
const args = new Map();
for (let index = 2; index < process.argv.length; index += 2) args.set(process.argv[index].slice(2), process.argv[index + 1]);
const statusFile = args.get("status-file");
const task = fs.readFileSync(args.get("task-file"), "utf8");
if (task.includes("HANG")) {
  // Keep the existing explicit-stop fixture semantics.
  await import("./fake-worker.mjs");
} else {
  process.on("SIGTERM", () => process.exit(0));
  process.on("SIGINT", () => process.exit(0));
  const runRoot = path.dirname(path.dirname(statusFile));
  const waitFor = async (name) => {
    while (!fs.existsSync(path.join(runRoot, name))) await new Promise(resolve => setTimeout(resolve, 10));
  };
  await waitFor("owner-worker-ready");
  const startedAt = Date.now();
  const running = {
    id: args.get("id"), name: args.get("name"), task, status: "running", runner: args.get("runner") ?? "pi",
    transport: args.get("transport"), cwd: args.get("cwd"), startedAt, updatedAt: startedAt,
    turns: 0, toolCalls: 0, text: "", exitCode: null,
    usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
  };
  fs.mkdirSync(path.dirname(statusFile), { recursive: true });
  fs.writeFileSync(statusFile, JSON.stringify(running));
  await waitFor("owner-worker-complete");
  const finishedAt = Date.now();
  fs.writeFileSync(statusFile, JSON.stringify({
    ...running, status: "completed", updatedAt: finishedAt, finishedAt,
    turns: 5, toolCalls: 3, text: "accepted owner work complete",
  }));
}
