import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
import { RpcClient } from "@earendil-works/pi-coding-agent";
import { describe, expect, it } from "vitest";
import { MeshStore } from "../src/mesh/store.js";

// smarty-dev#8742 / PR #841: the real path. A real Pi process in RPC mode loads the BUILT Fabric
// (dist/index.js) and talks to a local stub anthropic-messages provider; no mocks of Pi or Fabric.
// Gated: run `bun run build`, then FABRIC_8742_REAL_PI=1 bunx vitest run <this file>. CI's Linux job
// runs it after its build step (.github/workflows/test.yml, "Real Pi provider-dead path").
type RpcEvent = Record<string, any>;
const repo = fileURLToPath(new URL("../", import.meta.url));
const entry = path.join(repo, "dist/index.js");
const cli = process.env.FABRIC_8742_PI_CLI ?? path.join(repo, "node_modules/@earendil-works/pi-coding-agent/dist/cli.js");
// A provider that echoes prompt or secret content in its error body (security review P1).
const SECRET = "SECRET-sk-ant-8742 echoed prompt hunter2";
const POISON = "SIGNED-BLOCK";

type Mode = "non-retryable" | "unclassified" | "retryable";

const run = async (mode: Mode) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), `fabric-8742-${mode}-`));
  const agentDir = path.join(root, "agent"), project = path.join(root, "project"), meshRoot = path.join(root, "mesh");
  for (const dir of [agentDir, project]) fs.mkdirSync(dir, { recursive: true });
  const requests: Array<{ who: "main" | "summarizer"; poisoned: boolean; status: number }> = [];
  const server = http.createServer((req, res) => {
    let body = "";
    req.on("data", chunk => { body += chunk; });
    req.on("end", () => {
      const summarizer = body.includes("context summarization assistant");
      const poisoned = !summarizer && body.includes(POISON);
      requests.push({ who: summarizer ? "summarizer" : "main", poisoned, status: poisoned ? (mode === "unclassified" ? 0 : mode === "retryable" ? 503 : 409) : 200 });
      if (poisoned && mode === "unclassified") { req.socket.destroy(); return; }       // no status, no body
      if (poisoned && mode === "retryable") {
        // A retryable 5xx whose body echoes the secret: #4012's one retry, then its BLOCKED report.
        res.writeHead(503, { "content-type": "application/json", "x-should-retry": "false" });
        res.end(JSON.stringify({ type: "error", error: { type: "api_error", message: `upstream echoed: ${SECRET}` } }));
        return;
      }
      if (poisoned) {
        res.writeHead(409, { "content-type": "application/json", "x-should-retry": "false" });
        res.end(JSON.stringify({ type: "error", error: { type: "invalid_request_error", message: `compaction_affinity_missing: ${SECRET}` } }));
        return;
      }
      res.writeHead(200, { "content-type": "text/event-stream" });
      const ev = (type: string, data: object) => res.write(`event: ${type}\ndata: ${JSON.stringify({ type, ...data })}\n\n`);
      ev("message_start", { message: { id: `msg_${randomUUID()}`, type: "message", role: "assistant", model: "stub-model", content: [], stop_reason: null, usage: { input_tokens: 10, output_tokens: 1 } } });
      ev("content_block_start", { index: 0, content_block: { type: "text", text: "" } });
      ev("content_block_delta", { index: 0, delta: { type: "text_delta", text: summarizer ? "## Goal\nSeeded turns." : `ack ${"lorem ipsum ".repeat(120)}` } });
      ev("content_block_stop", { index: 0 });
      ev("message_delta", { delta: { stop_reason: "end_turn" }, usage: { output_tokens: 50 } });
      ev("message_stop", {});
      res.end();
    });
  });
  await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
  const port = (server.address() as { port: number }).port;
  fs.writeFileSync(path.join(agentDir, "models.json"), JSON.stringify({ providers: { stub: {
    baseUrl: `http://127.0.0.1:${port}`, api: "anthropic-messages", apiKey: "stub-key",
    models: [{ id: "stub-model", name: "Stub", contextWindow: 200000, maxTokens: 4096, reasoning: false, input: ["text"] }] } } }));
  fs.writeFileSync(path.join(agentDir, "settings.json"), JSON.stringify({ retry: { enabled: false }, compaction: { enabled: true, keepRecentTokens: 200 } }));
  fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({
    executor: { kernel: "typescript" }, fullCodeMode: false, mesh: { enabled: true, announce: true },
    mcp: { enabled: false }, jev: { enabled: false }, memory: { enabled: false },
    compaction: { engine: "pi" }, entropy: { compile: false }, speculation: { enabled: false },
  }));
  // The test host may itself run inside Pi/Fabric: never let its session or Fabric env leak in.
  const saved = Object.entries(process.env).filter(([key]) => /^PI_(SESSION|MODEL|PROVIDER|REASONING|FABRIC_)/.test(key));
  for (const [key] of saved) delete process.env[key];
  const client = new RpcClient({ cliPath: cli, cwd: project, provider: "stub", model: "stub-model",
    env: { PI_CODING_AGENT_DIR: agentDir, PI_FABRIC_AGENT_DIR: path.join(root, "exports"), PI_FABRIC_MESH_ROOT: meshRoot,
      PI_FABRIC_PROJECT_ROOT: project, PI_FABRIC_RUN_ROOT: path.join(root, "runs"), HERDR_PANE_ID: "", HERDR_SOCKET_PATH: "", PI_OFFLINE: "1" },
    args: ["--no-extensions", "--no-skills", "--no-prompt-templates", "--no-themes", "--no-context-files", "--approve",
      "--no-builtin-tools", "--thinking", "off", "--session-dir", path.join(root, "sessions"), "-e", entry] });
  const events: RpcEvent[] = [];
  client.onEvent(event => { events.push(event as RpcEvent); });
  const notices = () => events.filter(e => e.type === "extension_ui_request" && e.method === "notify").map(e => String(e.message));
  const settles = () => events.filter(e => e.type === "agent_settled").length;
  const wait = async (check: () => boolean, label: string, timeout = 60_000) => {
    const deadline = Date.now() + timeout;
    while (!check()) {
      if (Date.now() > deadline) throw new Error(`${mode}: timed out waiting for ${label}; notices ${JSON.stringify(notices())}`);
      await new Promise(resolve => setTimeout(resolve, 25));
    }
  };
  const prompt = async (message: string) => {
    const before = settles();
    await client.prompt(message);
    await wait(() => settles() > before, `settle after "${message}"`);
  };
  try {
    await client.start();
    for (const seed of ["seed one", "seed two"]) await prompt(seed);
    // The retryable path never alarms provider-dead; it ends in #4012's one BLOCKED report.
    const dead = mode === "retryable" ? /^BLOCKED: provider error/ : /^provider-dead/;
    for (let attempt = 0; attempt < 4 && !notices().some(n => dead.test(n)); attempt++) {
      await prompt(`${POISON}: continue the task (attempt ${attempt + 1})`);
      // Let Fabric's idle-boundary recovery (compaction, one attempt, #4012's one retry) finish.
      await wait(() => notices().some(n => dead.test(n)) || events.at(-1)?.type === "agent_settled", "recovery to settle", 30_000)
        .then(() => new Promise(resolve => setTimeout(resolve, 1_500)));
    }
    await wait(() => notices().some(n => dead.test(n)), "the provider-dead or BLOCKED notice");
    const mainRequests = requests.filter(r => r.who === "main").length;
    await new Promise(resolve => setTimeout(resolve, 2_000));        // no silent retry loop follows the alarm
    const state = await client.getState();
    const mesh = new MeshStore(meshRoot, 256 * 1024, 1000);
    const read = (topic: string) => mesh.read({ topic, after: 0, limit: 200 });
    // Mesh delivery is best-effort background work; give it a moment.
    if (mode !== "retryable") await wait(() => read("ops.owner").some(e => e.kind === "fabric.main.provider-dead"), "the ops.owner alarm", 15_000);
    const meshFiles = (dir: string): string[] => fs.readdirSync(dir, { withFileTypes: true })
      .flatMap(d => d.isDirectory() ? meshFiles(path.join(dir, d.name)) : [path.join(dir, d.name)]);
    const sessionFile = (state as { sessionFile?: string }).sessionFile;
    return {
      requests, notices: notices(), mainRequests, mainRequestsAfter: requests.filter(r => r.who === "main").length,
      opsOwner: read("ops.owner"), mainWake: read("fabric.main.wake"),
      meshLeaks: meshFiles(meshRoot).filter(file => fs.readFileSync(file).includes(SECRET)),
      sessionHasSecret: sessionFile !== undefined && fs.existsSync(sessionFile) && fs.readFileSync(sessionFile, "utf8").includes(SECRET),
      stderr: client.getStderr(),
    };
  } finally {
    for (const [key, value] of saved) process.env[key] = value;
    await client.abort().catch(() => undefined);
    await client.stop().catch(() => undefined);
    await new Promise(resolve => server.close(resolve));
    fs.rmSync(root, { recursive: true, force: true });
  }
};

// The flag is not PI_FABRIC_-prefixed: vitest.config.ts strips that fleet namespace from workers.
describe.runIf(process.env.FABRIC_8742_REAL_PI === "1")("real Pi + stub provider: provider-dead Main (smarty-dev#8742, PR #841)", () => {
  it("non-retryable 409: one compaction, one retry, then one alarm with no provider text on the mesh", async () => {
    const result = await run("non-retryable");
    expect(result.notices).toContain("Provider rejected Main's request as non-retryable (409 invalid_request_error: compaction_affinity_missing); compacting once to recover");
    expect(result.notices.filter(n => n.startsWith("provider-dead"))).toEqual([
      expect.stringMatching(/^provider-dead \(non-retryable: 409 invalid_request_error: compaction_affinity_missing\): (the one retry after compaction failed again|the recovery compaction failed)\. The provider's error text is in the session log\./),
    ]);
    expect(result.requests.filter(r => r.who === "summarizer").length).toBeGreaterThan(0);   // Pi's real compaction ran
    expect(result.mainRequestsAfter).toBe(result.mainRequests);
    const alarm = result.opsOwner.find(e => e.kind === "fabric.main.provider-dead")!;
    expect(alarm.data).toMatchObject({ kind: "non-retryable", status: 409, type: "invalid_request_error", code: "compaction_affinity_missing",
      error: "409 invalid_request_error: compaction_affinity_missing" });
    expect(result.mainWake.map(e => e.kind)).toContain("fabric.main.provider-dead");
    // Untrusted provider text: in the local session log only, never in notices, alarms or any mesh file.
    expect(result.sessionHasSecret).toBe(true);
    expect(JSON.stringify([result.notices, result.opsOwner, result.mainWake])).not.toContain(SECRET);
    expect(result.meshLeaks).toEqual([]);
  }, 150_000);

  it("unclassified (connection reset, no status or body): alarms provider-dead (unclassified) after three failures", async () => {
    const result = await run("unclassified");
    expect(result.notices.filter(n => n.startsWith("provider-dead"))).toEqual([
      expect.stringMatching(/^provider-dead \(unclassified\): 3 provider failures in a row without an HTTP status or error body\./),
    ]);
    expect(result.notices.some(n => n.startsWith("Provider rejected"))).toBe(false);          // never a compaction recovery
    expect(result.requests.filter(r => r.who === "main" && r.poisoned).length).toBeGreaterThanOrEqual(3);
    expect(result.mainRequestsAfter).toBe(result.mainRequests);
    expect(result.opsOwner.find(e => e.kind === "fabric.main.provider-dead")!.data)
      .toMatchObject({ kind: "unclassified", cause: "unclassified-repeated", error: "unclassified", failures: 3 });
    expect(result.meshLeaks).toEqual([]);
  }, 150_000);

  it("retryable 503 echoing a secret: one retry, then a BLOCKED report with only the status and error type", async () => {
    const result = await run("retryable");
    expect(result.notices.filter(n => n.startsWith("BLOCKED: provider error"))).toEqual(["BLOCKED: provider error: 503 api_error"]);
    expect(result.notices.some(n => n.startsWith("provider-dead") || n.startsWith("Provider rejected"))).toBe(false);
    expect(result.requests.filter(r => r.who === "main" && r.poisoned).length).toBeGreaterThanOrEqual(2);   // the failure and #4012's retry
    expect(result.mainRequestsAfter).toBe(result.mainRequests);
    expect(result.opsOwner.some(e => e.kind === "fabric.main.provider-dead")).toBe(false);
    // Untrusted provider text: in the local session log only, never in a notice or any mesh file.
    expect(result.sessionHasSecret).toBe(true);
    expect(JSON.stringify(result.notices)).not.toContain(SECRET);
    expect(result.meshLeaks).toEqual([]);
  }, 150_000);
});
