// A real native session writer, with deterministic fake model output.
// Unlike fake-worker, this validates the header and appends Pi's actual tree entries.
import fs from "node:fs";
import path from "node:path";
// Test-only narrow entry to the *same* native SessionManager exported by Pi.
// Every activation starts a fresh process: the public barrel also loads CLI,
// UI and provider catalogs (~1.5 s/worker on Windows), unrelated to session I/O.
// Resolve from the installed package, not a checkout or a hard-coded node_modules.
const piEntry = import.meta.resolve("@earendil-works/pi-coding-agent");
const { SessionManager } = await import(new URL("./core/session-manager.js", piEntry).href);
const args = new Map();
for (let index = 2; index < process.argv.length; index += 2) args.set(process.argv[index].slice(2), process.argv[index + 1]);
const task = fs.readFileSync(args.get("task-file"), "utf8");
const file = args.get("session-file");
const session = file ? SessionManager.open(file, path.dirname(file), args.get("cwd")) : undefined;
// Let the fake produce run output, never its lightweight/headerless transcript.
const index = process.argv.indexOf("--session-file");
if (index >= 0) process.argv.splice(index, 2);
await import("./fake-worker.mjs");
// Model a slow final native-session flush after terminal status publication.
// Windows SIGTERM kills the process rather than allowing a cooperative drain.
if (task.includes("DELAY_SESSION_TAIL")) {
  await new Promise(resolve => setTimeout(resolve, 300));
}
if (session) {
  session.appendMessage({ role: "user", content: task, timestamp: Date.now() });
  session.appendMessage({ role: "assistant", content: [{ type: "text", text: "fake actor advice" }], api: "anthropic-messages", provider: "test", model: "test", usage: { input: 1, output: 2, cacheRead: 0, cacheWrite: 0, totalTokens: 3, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } }, stopReason: "stop", timestamp: Date.now() });
}
