// Deterministic native worker for the two-mesh remote-task proof. No model or secrets.
import fs from "node:fs";
const args = new Map();
for (let i = 2; i < process.argv.length; i += 2) args.set(process.argv[i].slice(2), process.argv[i + 1]);
const task = fs.readFileSync(args.get("task-file"), "utf8");
const delay = JSON.parse(task).delayMs ?? 0;
const startedAt = Date.now();
const record = { id: args.get("id"), name: args.get("name"), task, runner: "pi", transport: "process", cwd: process.cwd(),
  startedAt, updatedAt: startedAt, status: "running", turns: 1, toolCalls: 1, inferenceStarted: true, text: "",
  sessionId: String(process.pid), usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } };
const write = value => { const tmp = `${args.get("status-file")}.${process.pid}.tmp`; fs.writeFileSync(tmp, JSON.stringify(value)); fs.renameSync(tmp, args.get("status-file")); };
write(record);
if (delay) await new Promise(resolve => setTimeout(resolve, Math.min(5000, delay)));
const value = { cwd: process.cwd(), corpus: fs.existsSync("corpus.txt") ? fs.readFileSync("corpus.txt", "utf8") : null,
  spawner: process.env.PI_FABRIC_SPAWNER_ID, spawnerSession: process.env.PI_FABRIC_SPAWNER_SESSION_ID,
  returnAddress: JSON.parse(args.get("task-return-address")), spawnerArgument: args.get("spawner-id"),
  writePolicy: args.get("write-policy") ? JSON.parse(args.get("write-policy")) : null, pid: process.pid };
write({ ...record, status: "completed", finishedAt: Date.now(), updatedAt: Date.now(), text: JSON.stringify(value), value });
