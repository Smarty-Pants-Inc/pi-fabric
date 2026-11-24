import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { RpcClient } from "@earendil-works/pi-coding-agent";
import { describe, expect, it } from "vitest";

// smarty-dev#6207 / PR #574: the provider path on the wire. A REAL Pi 1.2.0 process in RPC mode loads
// the BUILT Fabric (dist/index.js) and talks to an offline openai-completions stub on 127.0.0.1 that
// records every request's raw HTTP headers. Turn 1 is a plain Main prompt (the Main's hook); turn 2 has
// the Main call fabric_exec agents.run with transport process, so a real child Pi worker sends its own
// request to the same stub (the worker's -e hook). No mock of Pi, Fabric or the hook.
// Gated: run `bun run build`, then FABRIC_6207_REAL_PI=1 bunx vitest run <this file>. CI's Linux job
// runs it after its build step (.github/workflows/test.yml, "Real Pi sender headers").
// FABRIC_6207_ENTRY points the session at another built Fabric entry (used to show the test fails
// when the hook is removed in a scratch copy of dist/).
const repo = fileURLToPath(new URL("../", import.meta.url));
const entry = process.env.FABRIC_6207_ENTRY ?? path.join(repo, "dist/index.js");
const cli = process.env.FABRIC_6207_PI_CLI ?? path.join(repo, "node_modules/@earendil-works/pi-coding-agent/dist/cli.js");

const MAIN_NAME = "herdr-main-6207";
const CHILD_NAME = "hdr-child-6207";
const CHILD_MARKER = "CHILD-6207-TASK";
const REV = "0123456789abcdef0123456789abcdef01234567";
const RESERVED = ["x-smarty-role", "x-smarty-agent", "x-smarty-spawner"];
// Caller-set values: the provider's models.json headers, in three letter cases. None may reach the wire.
const FORGED = { "X-Smarty-Role": "forged-role", "x-smarty-agent": "forged-agent", "X-SMARTY-SPAWNER": "forged-spawner", "X-Stub-Control": "kept" };

const SPAWN_CODE = `const run = await agents.run({ task: "${CHILD_MARKER}: reply with the word done.", name: "${CHILD_NAME}",
  transport: "process", model: "stub/stub-model", extensions: false });
return JSON.stringify({ status: run.status, name: run.name });`;

type Recorded = { who: "main" | "child"; turn: string; rawHeaders: Array<[string, string]> };

const run = async () => {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-6207-")));
  const agentDir = path.join(root, "agent"), project = path.join(root, "project");
  for (const dir of [agentDir, project]) fs.mkdirSync(dir, { recursive: true });
  const requests: Recorded[] = [];
  const text = (content: unknown): string => typeof content === "string" ? content
    : Array.isArray(content) ? content.map(part => typeof part === "string" ? part : String(part?.text ?? "")).join("") : "";
  const server = http.createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on("data", chunk => chunks.push(chunk));
    req.on("end", () => {
      const body = JSON.parse(Buffer.concat(chunks).toString("utf8"));
      const messages: Array<{ role: string; content: unknown }> = body.messages ?? [];
      const users = messages.filter(m => m.role === "user").map(m => text(m.content));
      const child = users.some(u => u.includes(CHILD_MARKER));
      const lastUser = users.at(-1) ?? "";
      const toolResults = messages.filter(m => m.role === "tool").length;
      const pairs: Array<[string, string]> = [];
      for (let i = 0; i < req.rawHeaders.length; i += 2) pairs.push([req.rawHeaders[i]!, req.rawHeaders[i + 1]!]);
      const turn = child ? "child" : lastUser.includes("TURN2") ? (toolResults ? "turn2-result" : "turn2") : "turn1";
      requests.push({ who: child ? "child" : "main", turn, rawHeaders: pairs });
      const delta = turn === "turn2"
        ? { role: "assistant", tool_calls: [{ index: 0, id: "call-6207", type: "function", function: { name: "fabric_exec", arguments: JSON.stringify({ code: SPAWN_CODE, resultFormat: "text" }) } }] }
        : { role: "assistant", content: child ? "done" : `ack ${turn}` };
      res.writeHead(200, { "content-type": "text/event-stream" });
      for (const [part, finish] of [[delta, null], [{}, turn === "turn2" ? "tool_calls" : "stop"]] as const) {
        res.write(`data: ${JSON.stringify({ id: "offline-6207", object: "chat.completion.chunk", choices: [{ index: 0, delta: part, finish_reason: finish }] })}\n\n`);
      }
      res.end("data: [DONE]\n\n");
    });
  });
  await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
  const port = (server.address() as { port: number }).port;
  fs.writeFileSync(path.join(agentDir, "models.json"), JSON.stringify({ providers: { stub: {
    baseUrl: `http://127.0.0.1:${port}/v1`, api: "openai-completions", apiKey: "offline-only", headers: FORGED,
    models: [{ id: "stub-model", name: "Stub", reasoning: false, input: ["text"], contextWindow: 200000, maxTokens: 4096,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } }] } } }));
  fs.writeFileSync(path.join(agentDir, "settings.json"), JSON.stringify({ retry: { enabled: false }, compaction: { enabled: false } }));
  fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({
    executor: { kernel: "typescript" }, fullCodeMode: false, mesh: { enabled: false },
    mcp: { enabled: false }, jev: { enabled: false }, memory: { enabled: false },
    compaction: { engine: "pi" }, entropy: { compile: false }, speculation: { enabled: false },
  }));
  // The test host may itself run inside Pi/Fabric or a task lane: never let its sender env leak in.
  const saved = Object.entries(process.env).filter(([key]) => /^(PI_(SESSION|MODEL|PROVIDER|REASONING|FABRIC_)|SMARTY_|TASK_OUT$)/.test(key));
  for (const [key] of saved) delete process.env[key];
  const client = new RpcClient({ cliPath: cli, cwd: project, provider: "stub", model: "stub-model",
    env: { PI_CODING_AGENT_DIR: agentDir, PI_FABRIC_AGENT_DIR: path.join(root, "exports"), PI_FABRIC_MESH_ROOT: path.join(root, "mesh"),
      PI_FABRIC_PROJECT_ROOT: project, PI_FABRIC_RUN_ROOT: path.join(root, "runs"), PI_FABRIC_PI_BINARY: cli,
      SMARTY_ROLE: `project-agent@${REV}`, HERDR_PANE_ID: "", HERDR_SOCKET_PATH: "", PI_OFFLINE: "1" },
    args: ["--no-extensions", "--no-skills", "--no-prompt-templates", "--no-themes", "--no-context-files", "--approve",
      "--no-builtin-tools", "--thinking", "off", "--session-dir", path.join(root, "sessions"), "-e", entry] });
  const events: Array<Record<string, any>> = [];
  client.onEvent(event => { events.push(event as Record<string, any>); });
  const settles = () => events.filter(e => e.type === "agent_settled").length;
  const prompt = async (message: string, done: () => boolean) => {
    const before = settles();
    await client.prompt(message);
    const deadline = Date.now() + 120_000;
    while (!(settles() > before && done())) {
      if (Date.now() > deadline) throw new Error(`timed out after "${message}"; requests ${JSON.stringify(requests.map(r => r.turn))}; stderr ${client.getStderr()}`);
      await new Promise(resolve => setTimeout(resolve, 25));
    }
  };
  try {
    await client.start();
    await client.setSessionName(MAIN_NAME);
    await prompt("TURN1 hello", () => requests.some(r => r.turn === "turn1"));
    await prompt("TURN2 spawn the child", () => requests.some(r => r.turn === "turn2-result"));
    const state = await client.getState() as { sessionId?: string; sessionFile?: string };
    return { requests, sessionId: state.sessionId, stderr: client.getStderr() };
  } finally {
    for (const [key, value] of saved) process.env[key] = value;
    await client.abort().catch(() => undefined);
    await client.stop().catch(() => undefined);
    await new Promise(resolve => server.close(resolve));
    fs.rmSync(root, { recursive: true, force: true });
  }
};

/** Every reserved X-Smarty-* header line on the wire, as sent (name case kept, duplicates kept). */
const reserved = (request: Recorded): Array<[string, string]> => request.rawHeaders.filter(([name]) => RESERVED.includes(name.toLowerCase()));
const one = (request: Recorded, name: string): string | undefined => {
  const lines = reserved(request).filter(([key]) => key.toLowerCase() === name.toLowerCase());
  expect(lines.length, `${request.turn}: ${name} appears ${lines.length} times: ${JSON.stringify(lines)}`).toBeLessThanOrEqual(1);
  return lines[0]?.[1];
};

// The flag is not PI_FABRIC_-prefixed: vitest.config.ts strips that fleet namespace from workers.
describe.runIf(process.env.FABRIC_6207_REAL_PI === "1")("real Pi + Fabric: sender headers on the wire (smarty-dev#6207, PR #574)", () => {
  it("the Main's and its process child's real provider requests carry their own X-Smarty-* headers, once, never caller-set", async () => {
    const result = await run();
    const evidence = JSON.stringify(result.requests.map(r => ({ who: r.who, turn: r.turn, reserved: reserved(r),
      control: r.rawHeaders.filter(([name]) => name.toLowerCase() === "x-stub-control") })), null, 2);
    console.log(`[sender-headers-real-pi] ${evidence}`);
    if (process.env.FABRIC_6207_EVIDENCE) fs.writeFileSync(process.env.FABRIC_6207_EVIDENCE, `${evidence}\n`);
    const main = result.requests.filter(r => r.who === "main");
    const child = result.requests.filter(r => r.who === "child");
    expect(main.map(r => r.turn), result.stderr).toEqual(["turn1", "turn2", "turn2-result"]);
    expect(child.length, result.stderr).toBeGreaterThanOrEqual(1);
    expect(result.sessionId).toBeTruthy();
    for (const request of result.requests) {
      // Precondition: the provider's configured headers do reach the wire, so the forged ones were offered.
      expect(request.rawHeaders.filter(([name]) => name.toLowerCase() === "x-stub-control").map(([, value]) => value)).toEqual(["kept"]);
      // No caller-set value survives; no reserved header twice or in a second letter case.
      expect(JSON.stringify(reserved(request))).not.toMatch(/forged/);
      for (const name of RESERVED) {
        const spellings = reserved(request).filter(([key]) => key.toLowerCase() === name);
        expect(spellings.length, `${request.turn} ${name}: ${JSON.stringify(spellings)}`).toBeLessThanOrEqual(1);
      }
      // A session id never reaches an X-Smarty-* header.
      expect(JSON.stringify(reserved(request))).not.toContain(result.sessionId!);
    }
    for (const request of main) {
      expect(one(request, "X-Smarty-Role"), request.turn).toBe("project-agent");
      expect(one(request, "X-Smarty-Agent"), request.turn).toBe(MAIN_NAME);
      expect(one(request, "X-Smarty-Spawner"), request.turn).toBeUndefined();
    }
    for (const request of child) {
      expect(one(request, "X-Smarty-Role")).toBe("task");
      expect(one(request, "X-Smarty-Agent")).toBe(CHILD_NAME);
      expect(one(request, "X-Smarty-Spawner")).toBe(MAIN_NAME);
    }
  }, 240_000);
});
