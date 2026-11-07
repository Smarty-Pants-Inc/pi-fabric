import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { RpcClient } from "@earendil-works/pi-coding-agent";
import { describe, expect, it } from "vitest";

// smarty-dev#5492 / PR #567 acceptance: a REAL Pi 1.2.0 process in RPC mode loads the BUILT Fabric
// (dist/index.js) and a fixture extension standing in for pi-better-openai with its three tools
// disabled. An offline deterministic openai-completions stub drives fabric_exec. No mock of Pi or
// Fabric: the catalog, the system prompt and the refusal are read from the live session.
// Gated: run `bun run build`, then FABRIC_5492_REAL_PI=1 bunx vitest run <this file>. CI's Linux job
// runs it after its build step (.github/workflows/test.yml, "Real Pi extension-hidden tools").
// FABRIC_5492_ENTRY points the session at another built Fabric entry (used to show the test fails
// when the catalog filter is reverted in a scratch copy of dist/).
const repo = fileURLToPath(new URL("../", import.meta.url));
const entry = process.env.FABRIC_5492_ENTRY ?? path.join(repo, "dist/index.js");
const cli = process.env.FABRIC_5492_PI_CLI ?? path.join(repo, "node_modules/@earendil-works/pi-coding-agent/dist/cli.js");

const HIDDEN = ["openai_decide", "openai_image", "openai_websearch"];
const CONTROL = "openai_usage";
const PACKAGE = "pi-better-openai-fixture";

// pi-better-openai's source is not vendored here or installed on the test hosts, so this fixture
// reproduces the mechanism Fabric's fix targets (src/core/tool-ownership.ts hostToolVisibility): the
// extension keeps its tools REGISTERED and hides the disabled ones by dropping them from Pi's active
// tool set with the public ExtensionAPI, pi.setActiveTools(pi.getActiveTools().filter(...)), when
// the session starts. One tool of the same extension stays offered as the visible control.
const fixtureExtension = (executedFile: string, activeFile: string) => `import fs from "node:fs";
const HIDDEN = ${JSON.stringify(HIDDEN)};
export default function (pi) {
  for (const name of [...HIDDEN, ${JSON.stringify(CONTROL)}]) {
    pi.registerTool({
      name, label: name, description: name + " (pi-better-openai fixture)",
      parameters: { type: "object", properties: {}, additionalProperties: false },
      async execute() {
        fs.appendFileSync(${JSON.stringify(executedFile)}, name + "\\n");
        return { content: [{ type: "text", text: name + " ran" }], details: {} };
      },
    });
  }
  pi.on("session_start", () => pi.setActiveTools(pi.getActiveTools().filter((name) => !HIDDEN.includes(name))));
  pi.on("before_agent_start", () => {
    fs.writeFileSync(${JSON.stringify(activeFile)}, JSON.stringify({ active: pi.getActiveTools(), all: pi.getAllTools().map((tool) => tool.name) }));
  });
}
`;

// Model turn 1 lists the catalog; turn 2 invokes each hidden tool and the control through Fabric.
// Hidden names are assembled at run time so the code itself never names them.
// Turn 1 returns every openai_* name found anywhere in each listing (the full catalog is too large
// to return verbatim: Fabric would truncate it before the model sees it).
const LIST_CODE = `const named = (value: unknown) => [...new Set(JSON.stringify(value).match(/openai_[a-z]+/g) ?? [])].sort();
const catalog = named(await tools.catalog());
const list = named(await tools.list({ provider: "extensions" }));
const search = named(await tools.search({ query: "openai" }));
return JSON.stringify({ catalog, list, search });`;
const INVOKE_CODE = `const names = ["decide", "image", "websearch"].map((suffix) => "openai_" + suffix);
const outcomes: Record<string, string> = {};
for (const name of names) {
  try { await tools.call({ ref: ["extensions", name].join("."), args: {} }); outcomes[name] = "INVOKED"; }
  catch (error) { outcomes[name] = "REFUSED: " + String((error as Error)?.message ?? error); }
}
const control = await tools.call({ ref: ["extensions", "${CONTROL}"].join("."), args: {} });
return JSON.stringify({ outcomes, control });`;

type Request = { system: string; tools: string[]; toolResults: string[] };

const run = async () => {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-5492-")));
  const agentDir = path.join(root, "agent"), project = path.join(root, "project"), ext = path.join(root, PACKAGE);
  for (const dir of [agentDir, project, ext]) fs.mkdirSync(dir, { recursive: true });
  const executedFile = path.join(root, "executed.log"), activeFile = path.join(root, "active.json");
  fs.writeFileSync(path.join(ext, "package.json"), JSON.stringify({ name: PACKAGE, type: "module" }));
  fs.writeFileSync(path.join(ext, "index.ts"), fixtureExtension(executedFile, activeFile));
  const requests: Request[] = [];
  const text = (content: unknown): string => typeof content === "string" ? content
    : Array.isArray(content) ? content.map(part => typeof part === "string" ? part : String(part?.text ?? "")).join("") : "";
  const server = http.createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on("data", chunk => chunks.push(chunk));
    req.on("end", () => {
      const body = JSON.parse(Buffer.concat(chunks).toString("utf8"));
      const messages: Array<{ role: string; content: unknown }> = body.messages ?? [];
      const request: Request = {
        system: messages.filter(m => m.role === "system" || m.role === "developer").map(m => text(m.content)).join("\n"),
        tools: (body.tools ?? []).map((tool: { function?: { name?: string } }) => String(tool.function?.name)),
        toolResults: messages.filter(m => m.role === "tool").map(m => text(m.content)),
      };
      requests.push(request);
      const turn = request.toolResults.length;
      const code = turn === 0 ? LIST_CODE : turn === 1 ? INVOKE_CODE : undefined;
      const delta = code
        ? { role: "assistant", tool_calls: [{ index: 0, id: `call-5492-${turn}`, type: "function", function: { name: "fabric_exec", arguments: JSON.stringify({ code, resultFormat: "text" }) } }] }
        : { role: "assistant", content: "hidden-tools probe done" };
      res.writeHead(200, { "content-type": "text/event-stream" });
      for (const [part, finish] of [[delta, null], [{}, code ? "tool_calls" : "stop"]] as const) {
        res.write(`data: ${JSON.stringify({ id: "offline-5492", object: "chat.completion.chunk", choices: [{ index: 0, delta: part, finish_reason: finish }] })}\n\n`);
      }
      res.end("data: [DONE]\n\n");
    });
  });
  await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
  const port = (server.address() as { port: number }).port;
  fs.writeFileSync(path.join(agentDir, "models.json"), JSON.stringify({ providers: { stub: {
    baseUrl: `http://127.0.0.1:${port}/v1`, api: "openai-completions", apiKey: "offline-only",
    models: [{ id: "stub-model", name: "Stub", reasoning: false, input: ["text"], contextWindow: 200000, maxTokens: 4096,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } }] } } }));
  fs.writeFileSync(path.join(agentDir, "settings.json"), JSON.stringify({ retry: { enabled: false }, compaction: { enabled: false } }));
  // Full code mode: Fabric owns the model's tools, names extension tools in its system-prompt roster
  // and routes them through fabric_exec. Read risk keeps approval prompts out of the refusal path.
  fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({
    executor: { kernel: "typescript" }, fullCodeMode: true, mesh: { enabled: false },
    mcp: { enabled: false }, jev: { enabled: false }, memory: { enabled: false },
    compaction: { engine: "pi" }, entropy: { compile: false }, speculation: { enabled: false },
    capture: { risks: Object.fromEntries([...HIDDEN, CONTROL].map(name => [name, "read"])) },
  }));
  // The test host may itself run inside Pi/Fabric: never let its session or Fabric env leak in.
  const saved = Object.entries(process.env).filter(([key]) => /^PI_(SESSION|MODEL|PROVIDER|REASONING|FABRIC_)/.test(key));
  for (const [key] of saved) delete process.env[key];
  const client = new RpcClient({ cliPath: cli, cwd: project, provider: "stub", model: "stub-model",
    env: { PI_CODING_AGENT_DIR: agentDir, PI_FABRIC_AGENT_DIR: path.join(root, "exports"), PI_FABRIC_MESH_ROOT: path.join(root, "mesh"),
      PI_FABRIC_PROJECT_ROOT: project, PI_FABRIC_RUN_ROOT: path.join(root, "runs"), HERDR_PANE_ID: "", HERDR_SOCKET_PATH: "", PI_OFFLINE: "1" },
    args: ["--no-extensions", "--no-skills", "--no-prompt-templates", "--no-themes", "--no-context-files", "--approve",
      "--no-builtin-tools", "--thinking", "off", "--session-dir", path.join(root, "sessions"), "-e", entry, "-e", path.join(ext, "index.ts")] });
  const events: Array<Record<string, any>> = [];
  client.onEvent(event => { events.push(event as Record<string, any>); });
  try {
    await client.start();
    await client.prompt("List the extension tools, then try each OpenAI tool.");
    const deadline = Date.now() + 90_000;
    while (!events.some(e => e.type === "agent_settled") && requests.length < 3) {
      if (Date.now() > deadline) throw new Error(`timed out; requests ${requests.length}; stderr ${client.getStderr()}`);
      await new Promise(resolve => setTimeout(resolve, 25));
    }
    while (!events.some(e => e.type === "agent_settled")) {
      if (Date.now() > deadline) throw new Error(`timed out waiting to settle; stderr ${client.getStderr()}`);
      await new Promise(resolve => setTimeout(resolve, 25));
    }
    return {
      requests,
      active: fs.existsSync(activeFile) ? JSON.parse(fs.readFileSync(activeFile, "utf8")) as { active: string[]; all: string[] } : undefined,
      executed: fs.existsSync(executedFile) ? fs.readFileSync(executedFile, "utf8").trim().split("\n") : [],
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
describe.runIf(process.env.FABRIC_5492_REAL_PI === "1")("real Pi + Fabric: extension-hidden tools (smarty-dev#5492, PR #567)", () => {
  it("keeps three tools pi-better-openai disabled out of fabric_exec's catalog and the prompt roster, and refuses them", async () => {
    const result = await run();
    const evidence = JSON.stringify({ requests: result.requests, active: result.active, executed: result.executed }, null, 2);
    if (process.env.TASK_OUT) fs.writeFileSync(path.join(process.env.TASK_OUT, "extension-hidden-tools-real-pi.json"), evidence + "\n");
    expect(result.requests.length, result.stderr).toBe(3);
    const [first, listed, invoked] = result.requests as [Request, Request, Request];

    // Precondition: Pi itself hid the three tools; they stay registered (the fixture, not Fabric, hid them).
    expect(result.active?.all).toEqual(expect.arrayContaining([...HIDDEN, CONTROL]));
    expect(result.active?.active).toContain(CONTROL);
    for (const name of HIDDEN) expect(result.active?.active).not.toContain(name);

    // (a) fabric_exec's catalog, extensions listing and search never name a hidden tool; the control is there.
    const listing = JSON.parse(listed.toolResults[0]!.slice(listed.toolResults[0]!.indexOf("{"))) as Record<string, string[]>;
    expect(listing).toEqual({ catalog: [CONTROL], list: [CONTROL], search: [CONTROL] });
    for (const name of HIDDEN) expect(listed.toolResults[0], `catalog names ${name}`).not.toContain(name);

    // (b) the system prompt (with Fabric's extension roster) and the declared tools never name them.
    for (const request of result.requests) {
      for (const name of HIDDEN) {
        expect(request.system, `system prompt names ${name}`).not.toContain(name);
        expect(request.tools).not.toContain(name);
      }
    }
    expect(first.tools).toContain("fabric_exec");

    // (c) the visible control tool of the same extension is in the roster and callable.
    expect(first.system).toContain(`- ${PACKAGE}: ${CONTROL}`);

    // (d) invoking a hidden tool through Fabric is refused; nothing hidden ever executed.
    const outcome = JSON.parse(invoked.toolResults[1]!.slice(invoked.toolResults[1]!.indexOf("{"))) as { outcomes: Record<string, string>; control: unknown };
    for (const name of HIDDEN) expect(outcome.outcomes[name], name).toMatch(new RegExp(`^REFUSED: (Unknown Fabric action: extensions\\.${name}\\b|.*hidden by its extension)`));
    expect(JSON.stringify(outcome.control)).toContain(`${CONTROL} ran`);
    expect(result.executed).toEqual([CONTROL]);
  }, 150_000);
});
