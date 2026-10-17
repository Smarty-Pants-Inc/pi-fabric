import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import { RpcClient } from "@earendil-works/pi-coding-agent";
import { expect, it } from "vitest";
import { normalizeFabricConfig } from "../src/config.js";
import type { FabricState } from "../src/fabric-state.js";
import { createFabricExecTool } from "../src/fabric-exec-tool.js";
import { defaultCodePreviewSettings } from "../src/ui/code-preview.js";

// Real CLI and process-worker model requests; only inference is replaced.
// Keep reason: the request boundary catches prompt growth that unit helpers miss.
it.each(["additive", "native"] as const)("keeps fresh Main, lane, and process-task Fabric guidance below 250 tokens (%s)", { timeout: 120_000 }, async profile => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-guidance-budget-"));
  const agentDir = path.join(root, "agent");
  fs.mkdirSync(agentDir);
  const first = new Map<string, any>();
  const requests: any[] = [];
  const mainPrograms = [
    { code: 'const child = await agents.spawn({task:"Report ready.", model:"guidance/task", transport:"process", extensions:true, tools:["fabric_exec"], writePolicy:"unrestricted"}); return await agents.wait({id:child.id});' },
    { code: "return π.body;" },
    { code: "return π.body;", payloads: { body: "literal-payload-recovered" } },
    { code: "return process.env;" },
    { code: 'return JSON.stringify({recovered:"guest-builtins"});' },
    { code: 'const process = {env:"guest-local-name"}; return process.env;' },
  ];
  const systemText = (payload: any): string => payload.messages
    .filter((message: any) => message.role === "system" || message.role === "developer")
    .map((message: any) => typeof message.content === "string" ? message.content : JSON.stringify(message.content)).join("\n");
  const server = http.createServer((request, response) => {
    let body = "";
    request.on("data", chunk => { body += chunk; });
    request.on("end", () => {
      let payload: any;
      try { payload = JSON.parse(body); } catch { response.writeHead(400); response.end("Bad Request"); return; }
      if (!payload || typeof payload.model !== "string" || !Array.isArray(payload.messages)) {
        response.writeHead(400); response.end("Bad Request"); return;
      }
      if (!first.has(payload.model)) first.set(payload.model, payload);
      requests.push(payload);
      const program = payload.model === "main" ? mainPrograms.shift() : undefined;
      const delta = program
        ? { role: "assistant", tool_calls: [{ index: 0, id: `guidance-${requests.length}`, type: "function", function: {
          name: "fabric_exec", arguments: JSON.stringify(program),
        } }] }
        : { role: "assistant", content: "ready" };
      response.writeHead(200, { "Content-Type": "text/event-stream" });
      for (const [part, finish] of [[delta, null], [{}, program ? "tool_calls" : "stop"]]) {
        response.write("data: " + JSON.stringify({ id: "guidance", object: "chat.completion.chunk", created: 1,
          model: payload.model, choices: [{ index: 0, delta: part, finish_reason: finish }] }) + "\n\n");
      }
      response.end("data: [DONE]\n\n");
    });
  });
  await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("loopback provider did not bind");
  fs.writeFileSync(path.join(agentDir, "models.json"), JSON.stringify({ providers: { guidance: {
    baseUrl: `http://127.0.0.1:${address.port}/v1`, apiKey: "offline-test", api: "openai-completions",
    models: ["main", "lane", "task"].map(id => ({ id, name: id, reasoning: false, input: ["text"], contextWindow: 128000,
      maxTokens: 1024, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } })),
  } } }));
  fs.writeFileSync(path.join(agentDir, "settings.json"), JSON.stringify({ retry: { enabled: false }, compaction: { enabled: false } }));
  fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({ fullCodeMode: false, executor: { codemodeProfile: profile },
    agents: { retainRuns: true, sessionExport: false, timeoutMs: 60_000 }, mesh: { enabled: false }, mcp: { enabled: false },
    memory: { enabled: false }, jev: { enabled: true }, residency: { enabled: false }, entropy: { compile: false }, ui: { enabled: false },
  }));
  const piBinary = path.resolve("node_modules/@earendil-works/pi-coding-agent/dist/cli.js");
  const cleanEnv: NodeJS.ProcessEnv = { ...process.env };
  for (const key of Object.keys(cleanEnv)) if (key.startsWith("PI_FABRIC_")) delete cleanEnv[key];
  delete cleanEnv.HERDR_PANE_ID;
  delete cleanEnv.HERDR_SOCKET_PATH;
  try {
    for (const kind of ["main", "lane"] as const) {
      const cwd = path.join(root, kind);
      fs.mkdirSync(cwd);
      const client = new RpcClient({ cliPath: piBinary, cwd, provider: "guidance", model: kind, env: {
        ...cleanEnv, PI_CODING_AGENT_DIR: agentDir, PI_FABRIC_AGENT_DIR: path.join(root, "exports"),
        PI_FABRIC_RUN_ROOT: path.join(root, "runs"), PI_FABRIC_PI_BINARY: piBinary,
        PI_FABRIC_PROJECT_ROOT: cwd, PI_OFFLINE: "1", SMARTY_ROLE: kind === "main" ? "main" : "worktree-agent",
      }, args: ["--no-skills", "--no-prompt-templates", "--no-themes", "--no-context-files", "--approve",
        "--thinking", "off", "--session-dir", path.join(root, `${kind}-sessions`), "-e", path.resolve("dist/index.js")] });
      try {
        await client.start();
        await client.prompt(kind === "main" ? "Spawn one process task and wait for it." : "Report ready.");
        await client.waitForIdle(90_000);
        expect(await client.getLastAssistantText()).toBe("ready");
      } finally { await client.abort().catch(() => undefined); await client.stop(); }
    }
    expect([...first.keys()].sort()).toEqual(["lane", "main", "task"]);
    const results = requests.filter(request => request.model === "main").at(-1)?.messages
      .filter((message: any) => message.role === "tool").map((message: any) => JSON.stringify(message.content));
    expect(results).toHaveLength(6);
    expect(results[1]).toContain("Add payloads: { body: '...' }");
    expect(results[2]).toContain("literal-payload-recovered");
    expect(results[3]).toContain("Recovery hint:");
    expect(results[3]).toContain("native `bash` tool");
    expect(results[4]).toContain("guest-builtins");
    expect(results[5]).toContain("guest-local-name");
    expect(results[5]).not.toContain("Recovery hint:");
    const tool = createFabricExecTool({ bootstrapped: true, config: normalizeFabricConfig({ fullCodeMode: false }) } as FabricState,
      defaultCodePreviewSettings(), new Map(), value => value);
    const rows = ["main", "lane", "task"].map(kind => {
      const payload = first.get(kind);
      const system = systemText(payload);
      const execution = system.match(/<fabric_execution>\s*([\s\S]*?)\s*<\/fabric_execution>/)?.[1] ?? "";
      const guidelines = (tool.promptGuidelines ?? []).filter(line => system.includes(line));
      const declaration = payload.tools?.find((entry: any) => entry.function?.name === "fabric_exec")?.function;
      expect(declaration, `${kind} declares fabric_exec`).toBeDefined();
      const contract = [declaration.description, declaration.parameters.properties.code.description,
        declaration.parameters.properties.payloads.description].join("\n");
      const injected = [execution, ...guidelines].filter(Boolean).join("\n");
      const text = [injected, contract].filter(Boolean).join("\n");
      return { kind, injected, contract, text, injectedChars: injected.length, injectedTokens: Math.ceil(injected.length / 4),
        contractChars: contract.length, totalChars: text.length, totalTokens: Math.ceil(text.length / 4), payload };
    });
    if (process.env.FABRIC_GUIDANCE_EVIDENCE) {
      const evidencePath = process.env.FABRIC_GUIDANCE_EVIDENCE + (profile === "native" ? ".native" : "");
      fs.writeFileSync(evidencePath, JSON.stringify({ profile, rows }, null, 2));
    }
    for (const row of rows) {
      expect(row.injected, `${row.kind}: no unsolicited rules/digests`).toBe("");
      expect(systemText(row.payload), `${row.kind}: compiled prompt has no workflow advice`).not.toMatch(
        /acceptance ledger|Batch independent Fabric operations|Amortize round trips|Jev supplies typed Choice|Use `display\.name` and objective/,
      );
      expect(row.totalTokens, `${row.kind}: ${row.totalChars} chars/4`).toBeLessThanOrEqual(250);
    }
  } finally {
    await new Promise<void>(resolve => { server.closeAllConnections(); server.close(() => resolve()); });
    fs.rmSync(root, { recursive: true, force: true });
  }
});
