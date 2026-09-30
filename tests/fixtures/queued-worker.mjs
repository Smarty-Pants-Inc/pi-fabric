import fs from "node:fs";

const args = new Map();
for (let i = 2; i < process.argv.length; i += 2) args.set(process.argv[i].slice(2), process.argv[i + 1]);
const task = fs.readFileSync(args.get("task-file"), "utf8");
const { gate } = JSON.parse(task);
const startedAt = Date.now();
const record = {
  id: args.get("id"), name: args.get("name"), task, runner: args.get("runner"),
  transport: args.get("transport"), cwd: args.get("cwd"), status: "running",
  startedAt, updatedAt: startedAt, turns: 0, toolCalls: 0, text: "",
  usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
};
fs.writeFileSync(args.get("status-file"), JSON.stringify(record));
while (!fs.existsSync(gate)) await new Promise((resolve) => setTimeout(resolve, 10));
const finishedAt = Date.now();
fs.writeFileSync(args.get("status-file"), JSON.stringify({
  ...record, status: "completed", updatedAt: finishedAt, finishedAt, turns: 1, text: "queue worker complete",
}));
