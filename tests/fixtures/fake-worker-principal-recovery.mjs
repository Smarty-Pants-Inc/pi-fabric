import fs from "node:fs";
import path from "node:path";

// A native-session/steering transport stub. No host receipts or authority are synthesized.
const args = new Map();
for (let index = 2; index < process.argv.length; index += 2) args.set(process.argv[index].slice(2), process.argv[index + 1]);
const statusFile = args.get("status-file");
const sessionFile = args.get("session-file");
const steerFile = args.get("steer-file");
const taskFile = args.get("task-file");
const task = fs.readFileSync(taskFile, "utf8");
const admission = fs.existsSync(taskFile + ".provenance.json") ? JSON.parse(fs.readFileSync(taskFile + ".provenance.json", "utf8")) : undefined;
const running = { id: args.get("id"), name: args.get("name"), task, status: "running", runner: "pi", transport: args.get("transport"), cwd: args.get("cwd"), startedAt: Date.now(), updatedAt: Date.now(), turns: 0, toolCalls: 0, text: "", exitCode: null, usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } };
fs.mkdirSync(path.dirname(statusFile), { recursive: true });
fs.writeFileSync(statusFile, JSON.stringify(running));
const append = (content, provenance) => { if (sessionFile) fs.appendFileSync(sessionFile, JSON.stringify({ type: "message", message: { role: "user", content, provenance } }) + "\n"); };
append(task, admission);
let attempt = 1;
if (task.includes("RECOVERY_LINEAGE")) {
  const marker = sessionFile + ".security-attempts";
  attempt = fs.existsSync(marker) ? Number(fs.readFileSync(marker, "utf8")) + 1 : 1;
  fs.writeFileSync(marker, String(attempt));
}
const complete = () => {
  const history = task.includes("RECOVERY_LINEAGE") ? fs.readFileSync(sessionFile, "utf8") : "";
  fs.writeFileSync(statusFile, JSON.stringify({ ...running, status: "completed", updatedAt: Date.now(), finishedAt: Date.now(), text: task + "\n" + history, turns: 1 }));
};
process.on("SIGTERM", () => process.exit(0));
process.on("SIGINT", () => process.exit(0));
if ((task.includes("RECOVERY_LINEAGE") && attempt === 1) || task.includes("HANG")) {
  const seen = new Set();
  const timer = setInterval(() => {
    if (steerFile && fs.existsSync(steerFile)) {
      for (const line of fs.readFileSync(steerFile, "utf8").trim().split("\n").filter(Boolean)) {
        const entry = JSON.parse(line);
        if (seen.has(entry.id)) continue;
        seen.add(entry.id);
        if (entry.type === "steer" || entry.type === "follow_up") append(entry.message, entry.provenance);
      }
    }
    if (sessionFile && fs.existsSync(sessionFile + ".release")) { clearInterval(timer); complete(); }
  }, 10);
} else complete();
