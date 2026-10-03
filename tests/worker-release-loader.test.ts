import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import http from "node:http";
import { pathToFileURL } from "node:url";
import { afterEach, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { WORKER_PROTOCOL_VERSION } from "../src/agents/worker-protocol.js";

const roots: string[] = [];
const managers: AgentManager[] = [];
const servers: http.Server[] = [];
afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  await Promise.all(servers.splice(0).map(server => new Promise<void>(resolve => server.close(() => resolve()))));
  vi.restoreAllMocks(); vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

it.each([
  { entry: "cli", change: false }, { entry: "cli", change: true },
  { entry: "sdk", change: false }, { entry: "sdk", change: true },
])("native Pi excludes incompatible Fabric before factories run ($entry; selector changes: $change)", async ({ entry, change }) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-native-release-loader-")); roots.push(root);
  const profile = path.join(root, "profile"); fs.mkdirSync(profile);
  const marks = path.join(root, "loaded.jsonl");
  const settings = path.join(profile, "settings.json");
  const release = (name: string, protocol = WORKER_PROTOCOL_VERSION) => {
    const dir = path.join(root, name); fs.mkdirSync(dir);
    fs.cpSync(path.resolve("dist"), path.join(dir, "dist"), { recursive: true });
    fs.symlinkSync(path.resolve("node_modules"), path.join(dir, "node_modules"), "junction");
    fs.writeFileSync(path.join(dir, "package.json"), JSON.stringify({ name: "pi-fabric", type: "module", pi: { extensions: ["dist/index.js"] } }));
    fs.writeFileSync(path.join(dir, "dist/worker-protocol.json"), JSON.stringify({ version: protocol }));
    fs.writeFileSync(path.join(dir, "dist/index.js"), `import fs from 'node:fs'; export default function() { fs.appendFileSync(${JSON.stringify(marks)}, JSON.stringify({ name: ${JSON.stringify(name)}, path: import.meta.url, argv: process.argv }) + '\\n'); }`);
    return dir;
  };
  const parent = release("parent"); const incompatible = release("incompatible", WORKER_PROTOCOL_VERSION + 1); const changed = release("changed");
  const other = path.join(root, "other.mjs");
  fs.writeFileSync(other, `import fs from 'node:fs'; export default function() { fs.appendFileSync(${JSON.stringify(marks)}, JSON.stringify({ name: 'other' }) + '\\n'); }`);
  const select = (dir: string) => fs.writeFileSync(settings, JSON.stringify({ packages: [dir], extensions: [other], enableInstallTelemetry: false, compaction: { enabled: false }, ...(entry === "cli" ? { retry: { enabled: false } } : {}) }));
  select(incompatible);
  if (change) fs.appendFileSync(path.join(parent, "dist/index.js"), `\nfs.writeFileSync(${JSON.stringify(settings)}, ${JSON.stringify(JSON.stringify({ packages: [changed], extensions: [other], enableInstallTelemetry: false, compaction: { enabled: false }, ...(entry === "cli" ? { retry: { enabled: false } } : {}) }))});\n`);
  const server = http.createServer((request, response) => {
    request.resume(); request.on("end", () => {
      response.writeHead(200, { "Content-Type": "text/event-stream" });
      for (const [delta, finish_reason] of [[{ role: "assistant", content: "native loader proof" }, null], [{}, "stop"]]) response.write(`data: ${JSON.stringify({ id: "offline", object: "chat.completion.chunk", choices: [{ index: 0, delta, finish_reason }] })}\n\n`);
      response.end("data: [DONE]\n\n");
    });
  }); servers.push(server); await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
  fs.writeFileSync(path.join(profile, "models.json"), JSON.stringify({ providers: { proof: { baseUrl: `http://127.0.0.1:${(server.address() as { port: number }).port}/v1`, apiKey: "offline-only", api: "openai-completions", models: [{ id: "offline", name: "offline", reasoning: false, input: ["text"], contextWindow: 32000, maxTokens: 1024, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } }] } } }));
  vi.stubEnv("PI_CODING_AGENT_DIR", profile); vi.stubEnv("PI_OFFLINE", "1"); vi.stubEnv("PI_FABRIC_RUN_ROOT", undefined);
  const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
  const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, nice: 19, timeoutMs: 30000, retainRuns: true }, { workerPath: path.join(parent, "dist/worker.js"), piBinary: path.resolve("node_modules/@earendil-works/pi-coding-agent/dist/cli.js"), runRoot: path.join(root, "runs") }); managers.push(manager);
  const result = await manager.run({ task: "offline native resource selection", model: "proof/offline", extensions: true, tools: [], transport: "process" });
  expect(result.status, result.error).toBe("completed");
  expect(result.fabricRelease).toBe(parent);
  expect(warn).toHaveBeenCalledTimes(1);
  const loaded = fs.readFileSync(marks, "utf8").trim().split("\n").map(line => JSON.parse(line));
  expect(new Set(loaded.map(row => row.name))).toEqual(new Set(["parent", "other"]));
  const parentLoads = loaded.filter(row => row.name === "parent");
  expect(parentLoads.every(row => row.path === pathToFileURL(path.join(parent, "dist/index.js")).href)).toBe(true);
  expect(parentLoads.every(row => row.argv[1].endsWith(entry === "sdk" ? path.join("worker", "task-entry.js") : "cli.js"))).toBe(true);
}, 45000);
