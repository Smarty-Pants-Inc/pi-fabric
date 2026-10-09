// Fake Pi boundary for native launcher/host lifecycle tests. No model or agent is spawned.
import fs from "node:fs";
import path from "node:path";
import { AgentManager } from "../../src/agents/manager.js";
import { ResidentHost } from "../../src/residency/host.js";
import type { ResidentHostConfig } from "../../src/residency/protocol.js";

const config = JSON.parse(fs.readFileSync(process.env.PI_FABRIC_RESIDENT_CONFIG!, "utf8")) as ResidentHostConfig;
AgentManager.prototype.run = async function (request, _signal, onSpawned) {
  const id = "0".repeat(32);
  onSpawned?.({ id } as never);
  fs.appendFileSync(path.join(config.cwd, "processed.jsonl"), JSON.stringify({ task: request.task }) + "\n");
  return { id, status: "completed", text: "processed", toolCalls: 0, startedAt: Date.now(), finishedAt: Date.now() } as never;
};
let idle!: () => void;
const done = new Promise<void>(resolve => { idle = resolve; });
const host = new ResidentHost(config, idle);
for (const signal of ["SIGTERM", "SIGINT"] as const) process.once(signal, idle);
await host.start();
if (!host.actors.listOwned().length) await host.actors.create({ name: "native-listener", instructions: "process",
  topics: ["native.wake"], coalesce: false, residency: "durable" });
fs.writeFileSync(path.join(config.cwd, "ready.json"), JSON.stringify({ pid: process.pid }));
await done;
await host.close();
process.exit(0);
