// Real AgentManager -> ProcessTransport -> worker -> native execution, without a model provider.
import fs from "node:fs";
import path from "node:path";
import { spawn } from "node:child_process";
import { fileURLToPath, pathToFileURL } from "node:url";
import { createInterface } from "node:readline";

const self = fileURLToPath(import.meta.url);
const emit = event => fs.writeSync(1, JSON.stringify(event) + "\n");
const publish = (file, value) => {
  fs.writeFileSync(file + ".tmp", JSON.stringify(value));
  fs.renameSync(file + ".tmp", file); // readiness must never expose a partial JSON body
};
const waitForFile = file => new Promise(resolve => {
  const check = () => { if (fs.existsSync(file)) { watcher.close(); resolve(); } };
  const watcher = fs.watch(path.dirname(file), check);
  check();
});

if (process.argv.includes("--mode")) {
  // A native Pi stand-in that keeps partial output and an in-group tool process
  // alive, ignoring both EOF and TERM so worker-owned KILL is exercised.
  const root = process.cwd();
  process.on("SIGTERM", () => {});
  process.stdin.resume();
  setInterval(() => {}, 1000);
  const descendant = spawn(process.execPath, [self, "descendant", root], { stdio: "ignore" });
  await waitForFile(path.join(root, "descendant.json"));
  fs.writeFileSync(path.join(root, "native-trace.jsonl"), '{"synthetic":"raw trace retained"}\n');
  emit({ type: "agent_start" });
  emit({ type: "message_end", message: { role: "assistant", content: "partial work retained", stopReason: "toolUse" } });
  publish(path.join(root, "native.json"), { pid: process.pid, descendant: descendant.pid });
} else if (process.argv[2] === "descendant") {
  process.on("SIGTERM", () => {});
  setInterval(() => {}, 1000);
  publish(path.join(process.argv[3], "descendant.json"), { pid: process.pid });
} else {
  const [root, workerPath, mode] = process.argv.slice(2);
  const { createJiti } = await import("jiti");
  const jiti = createJiti(pathToFileURL(path.join(process.cwd(), "index.js")).href);
  const { AgentManager } = await jiti.import("./src/agents/manager.ts");
  const { DEFAULT_FABRIC_CONFIG } = await jiti.import("./src/config.ts");
  const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents,
    timeoutMs: 60_000, retainRuns: true, maxConcurrent: 1 }, {
    workerPath, piBinary: self, runRoot: path.join(root, "runs"), meshRoot: path.join(root, "mesh"),
    completionRecipient: { rootId: "session:orphan-owner", sessionId: "orphan-owner", projectRoot: root,
      cwd: root, name: "orphan-owner", startedAt: Date.now() },
  });
  const handle = await manager.spawn({ task: "keep partial work", transport: "process", needs: ["local"] });
  if (mode === "before-ack") await waitForFile(path.join(root, "custody-ready.json"));
  else await waitForFile(path.join(root, "native.json"));
  emit({ event: "ready", id: handle.id, worker: Number(handle.sessionId) });
  const lines = createInterface({ input: process.stdin });
  lines.on("line", async command => {
    if (command === "deadline") {
      // Match Main's one-second terminal hook bound: an earlier shutdown step
      // never resolves, so AgentManager.close is never reached before exit.
      const shutdown = new Promise(() => {});
      const expired = new Promise(resolve => setTimeout(() => resolve("deadline"), 1000));
      if (await Promise.race([shutdown.then(() => manager.close()), expired]) === "deadline") {
        emit({ event: "shutdown-deadline", managerCloseReached: false });
        process.exit(1);
      }
    } else if (command === "status") emit({ event: "status", status: manager.status(handle.id).status });
    else if (command === "close") { await manager.close(); process.exit(0); }
  });
  // Keep the Main-like owner alive even though the process transport is unref'd.
  setInterval(() => {}, 1000);
}
