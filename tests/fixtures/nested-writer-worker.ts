// Controlled primary crash after a real recursive AgentManager process launch.
// No fabricated descendant PID/status or unresolved marker: the nested manager
// and process transport write the normal production run tree.
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { AgentManager } from "../../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../../src/config.js";

const args = new Map<string, string>();
for (let i = 2; i < process.argv.length; i += 2) args.set(process.argv[i]!.slice(2), process.argv[i + 1]!);
const task = fs.readFileSync(args.get("task-file")!, "utf8");
if (task.startsWith("NESTED_WRITER ")) {
  const { release } = JSON.parse(task.slice("NESTED_WRITER ".length)) as { release: string };
  const now = Date.now();
  const record = {
    id: args.get("id"), name: args.get("name"), task, status: "running", runner: "pi",
    transport: "process", sessionId: String(process.pid), cwd: args.get("cwd"),
    startedAt: now, updatedAt: now, turns: 4, toolCalls: 2, text: "", exitCode: null,
    usage: { input: 40, output: 20, cacheRead: 0, cacheWrite: 0, cost: 0.002 },
  };
  fs.writeFileSync(args.get("status-file")!, JSON.stringify(record));
  const deadline = Date.now() + 30_000;
  while (!fs.existsSync(release)) {
    if (Date.now() >= deadline) throw new Error("Nested writer release gate timed out");
    await new Promise(resolve => setTimeout(resolve, 10));
  }
  fs.writeFileSync(args.get("status-file")!, JSON.stringify({ ...record, status: "completed", turns: 5, updatedAt: Date.now(), finishedAt: Date.now() }));
  process.exit(0);
}
const field = (name: string): string => JSON.parse(task.match(new RegExp(`"${name}":\\s*("(?:\\\\.|[^"\\\\])*")`))![1]!);
const crash = field("primaryCrashPath");
const release = field("nestedReleasePath");
const observation = field("nestedObservationPath");
const manager = new AgentManager(args.get("cwd")!, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 }, {
  runRoot: args.get("run-root")!, workerPath: fileURLToPath(import.meta.url),
  fabricExtensionPath: args.get("fabric-extension")!,
});
const child = await manager.spawn({ task: `NESTED_WRITER ${JSON.stringify({ release })}`, transport: "process", extensions: false });
const childStatus = path.join(manager.runDirectory(child.id)!, "status.json");
const deadline = Date.now() + 30_000;
while (!fs.existsSync(childStatus)) {
  if (Date.now() >= deadline) throw new Error("Nested process did not publish status");
  await new Promise(resolve => setTimeout(resolve, 10));
}
fs.writeFileSync(observation, JSON.stringify({ id: child.id, pid: Number(child.sessionId), statusFile: childStatus }));
const now = Date.now();
fs.writeFileSync(args.get("status-file")!, JSON.stringify({
  id: args.get("id"), name: args.get("name"), actorId: args.get("actor-id"), task,
  status: "running", runner: "pi", transport: "process", sessionId: String(process.pid),
  cwd: args.get("cwd"), startedAt: now, updatedAt: now, turns: 3, toolCalls: 1,
  text: "", exitCode: null, usage: { input: 30, output: 10, cacheRead: 0, cacheWrite: 0, cost: 0.001 },
}));
// Simulate a primary-only crash: intentionally skip manager.close(), leaving
// its detached nested writer alive with a readable ordinary running record.
while (!fs.existsSync(crash)) {
  if (Date.now() >= deadline) throw new Error("Primary crash gate was not released");
  await new Promise(resolve => setTimeout(resolve, 10));
}
// Public durable tasks need a non-recoverable primary failure, rather than the
// actor stop fence used by the activation tests. Keep the same live descendant.
if (task.includes('"primaryTerminalFailure":true')) {
  const statusFile = args.get("status-file")!;
  const record = JSON.parse(fs.readFileSync(statusFile, "utf8"));
  fs.writeFileSync(statusFile, JSON.stringify({ ...record, status: "failed", error: "controlled primary failure", finishedAt: Date.now(), updatedAt: Date.now() }));
}
process.exit(3);
