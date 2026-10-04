// Actual worker execution child: accepts model admission, refuses TERM/EOF.
import fs from "node:fs";
process.on("SIGTERM", () => {});
let buffer = "";
process.stdin.on("data", chunk => {
  buffer += chunk;
  while (buffer.includes("\n")) {
    const split = buffer.indexOf("\n");
    const frame = JSON.parse(buffer.slice(0, split)); buffer = buffer.slice(split + 1);
    const result = frame.type === "get_state" ? { model: { provider: "fixture", id: "visible" }, thinkingLevel: "off" } : {};
    process.stdout.write(JSON.stringify({ type: "response", command: frame.type, id: frame.id, success: true, data: result }) + "\n");
  }
});
const stat = fs.readFileSync(`/proc/${process.pid}/stat`, "utf8");
fs.writeFileSync(process.env.REFUSING_EXECUTION_READY, JSON.stringify({ pid: process.pid, started: stat.slice(stat.lastIndexOf(")") + 2).split(" ")[19] }));
setInterval(() => {}, 1000);
