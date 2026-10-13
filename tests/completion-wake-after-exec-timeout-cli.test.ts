import { createHash } from "node:crypto";
import fs from "node:fs";
import http from "node:http";
import type { AddressInfo } from "node:net";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { RpcClient } from "@earendil-works/pi-coding-agent";
import { describe, expect, it, vi } from "vitest";

type RpcEvent = Parameters<Parameters<RpcClient["onEvent"]>[0]>[0];
const repo = fileURLToPath(new URL("../", import.meta.url));
const entry = path.join(repo, "dist/index.js");
const cli = process.env.FABRIC_6134_PI_CLI ?? path.join(repo, "node_modules/@earendil-works/pi-coding-agent/dist/cli.js");
const CHILD_MARKER = "CHILD_DONE_9271";
const SEEN_MARKER = "SAW_COMPLETION_9271";
const CHILD_NAME = "slow-child-9271";
// The executor deadline of the Main's fabric_exec (the incident ran under the 120 s default).
const OUTER_TIMEOUT_MS = 8_000;

// smarty-dev#9271 (round 2 of #6134): a lane Main's fabric_exec timed out around agents.spawn
// after the child had started. The Main recovered the id with agents.list, agents.wait returned
// RUNNING/waitTimedOut, and the Main ended its turn. The child's later completion must wake the
// idle Main into a new turn. Real native Pi RPC Main -> fabric_exec agents.spawn -> built worker
// -> real native Pi child; only inference is a loopback Anthropic Messages server.
// Explicit artifact proof: bun run build, then FABRIC_6134_REAL_CLI=1 bunx vitest run <this file>.
describe.runIf(process.platform === "linux" && process.env.FABRIC_6134_REAL_CLI === "1")("child completion after a timed-out fabric_exec (#9271)", () => {
  it("wakes the idle Main into a new turn that sees the completion within 10 s", async () => {
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-timeout-wake-9271-")));
    const cwd = path.join(root, "main");
    const agentDir = path.join(root, "agent");
    fs.mkdirSync(cwd, { recursive: true });
    fs.mkdirSync(agentDir, { recursive: true });
    const requests: Array<{ at: number; model: string; reply: string; marker: boolean }> = [];
    const events: Array<{ at: number; event: RpcEvent }> = [];
    const timing: Record<string, number | undefined> = {};
    let releaseChild!: () => void;
    const childGate = new Promise<void>(resolve => { releaseChild = resolve; });
    let count = 0;
    const server = http.createServer((request, response) => {
      let body = "";
      request.on("data", chunk => { body += chunk; });
      request.on("end", async () => {
        const data = JSON.parse(body) as { model: string; messages: unknown[] };
        const transcript = JSON.stringify(data.messages);
        const send = (blocks: Array<{ type: "text"; text: string } | { type: "tool_use"; id: string; name: string; input: unknown }>, stopReason: string) => {
          response.writeHead(200, { "content-type": "text/event-stream" });
          const event = (name: string, value: unknown) => response.write(`event: ${name}\ndata: ${JSON.stringify({ type: name, ...value as object })}\n\n`);
          event("message_start", { message: { id: `msg_${++count}`, type: "message", role: "assistant", model: data.model, content: [],
            stop_reason: null, usage: { input_tokens: 1, output_tokens: 0 } } });
          blocks.forEach((block, index) => {
            if (block.type === "text") {
              event("content_block_start", { index, content_block: { type: "text", text: "" } });
              event("content_block_delta", { index, delta: { type: "text_delta", text: block.text } });
            } else {
              event("content_block_start", { index, content_block: { type: "tool_use", id: block.id, name: block.name, input: {} } });
              event("content_block_delta", { index, delta: { type: "input_json_delta", partial_json: JSON.stringify(block.input) } });
            }
            event("content_block_stop", { index });
          });
          event("message_delta", { delta: { stop_reason: stopReason }, usage: { output_tokens: 1 } });
          event("message_stop", {});
          response.end();
        };
        const log = (reply: string) => requests.push({ at: Date.now(), model: data.model, reply, marker: transcript.includes(CHILD_MARKER) });
        const tool = (id: string, code: string, extra: Record<string, unknown> = {}) =>
          send([{ type: "tool_use", id, name: "fabric_exec", input: { code, ...extra } }], "tool_use");
        if (data.model === "child") {
          timing.childRequested = Date.now();
          await childGate;
          log("child-result");
          send([{ type: "text", text: `${CHILD_MARKER}: slow child finished` }], "end_turn");
          timing.childResponded = Date.now();
        } else if (transcript.includes(CHILD_MARKER)) {
          log("saw-completion");
          send([{ type: "text", text: SEEN_MARKER }], "end_turn");
        } else if (!transcript.includes("toolu_spawn_9271")) {
          // 1. The spawn returns, then the program outlives its executor deadline.
          log("spawn");
          tool("toolu_spawn_9271", [
            `const handle = await agents.spawn({ name: '${CHILD_NAME}', task: 'Answer briefly.', model: 'timeout-9271/child', transport: 'process', extensions: false, tools: [] });`,
            "print(`spawned ${handle.id}`);",
            "await new Promise(resolve => setTimeout(resolve, 600000));",
            "return handle.id;",
          ].join("\n"), { timeoutMs: OUTER_TIMEOUT_MS });
        } else if (!transcript.includes("toolu_list_9271")) {
          // 2. The Main recovers the child's id from agents.list.
          log("list");
          tool("toolu_list_9271", "const list = await agents.list(); return list.map(agent => ({ id: agent.id, name: agent.name, status: agent.status }));");
        } else if (!transcript.includes("toolu_wait_9271")) {
          // 3. A bounded wait sees the child still running.
          log("wait");
          tool("toolu_wait_9271", `const child = (await agents.list()).find(agent => agent.name === '${CHILD_NAME}'); return await agents.wait({ id: child.id, timeoutMs: 2000 });`);
        } else {
          // 4. The Main ends its turn normally and goes idle.
          log("done");
          send([{ type: "text", text: "done" }], "end_turn");
        }
      });
    });
    await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
    const model = (id: string) => ({ id, name: id, reasoning: false, input: ["text"], contextWindow: 200_000, maxTokens: 4096,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } });
    fs.writeFileSync(path.join(agentDir, "models.json"), JSON.stringify({ providers: { "timeout-9271": {
      baseUrl: `http://127.0.0.1:${(server.address() as AddressInfo).port}`, apiKey: "offline-only", api: "anthropic-messages",
      models: [model("main"), model("child")] } } }));
    fs.writeFileSync(path.join(agentDir, "settings.json"), JSON.stringify({ retry: { enabled: false }, compaction: { enabled: false } }));
    fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({
      executor: { kernel: "typescript", timeoutMs: OUTER_TIMEOUT_MS }, fullCodeMode: false, autoReload: false,
      mesh: { enabled: true, announce: true }, agents: { transport: "process", nice: 19, sessionExport: false, timeoutMs: 120_000 },
      mcp: { enabled: false }, jev: { enabled: false }, memory: { enabled: false }, records: { enabled: false },
      prewalk: { enabled: false, alwaysRearm: false }, compaction: { engine: "pi" }, entropy: { compile: false }, speculation: { enabled: false },
    }));
    // The Main must not join this agent's own fleet session or mailbox.
    for (const key of Object.keys(process.env)) if (/^(PI_|HERDR_)/.test(key)) vi.stubEnv(key, undefined);
    const client = new RpcClient({ cliPath: cli, cwd, provider: "timeout-9271", model: "main",
      env: {
        PI_CODING_AGENT_DIR: agentDir, PI_FABRIC_AGENT_DIR: path.join(root, "exports"), PI_FABRIC_MESH_ROOT: path.join(root, "mesh"),
        PI_FABRIC_PROJECT_ROOT: cwd, PI_FABRIC_RUN_ROOT: path.join(root, "runs"), PI_FABRIC_PI_BINARY: cli, PI_OFFLINE: "1",
      },
      args: ["--no-extensions", "--no-skills", "--no-prompt-templates", "--no-themes", "--no-context-files", "--approve",
        "--thinking", "off", "--session-dir", path.join(root, "sessions"), "-e", entry],
    });
    client.onEvent(event => events.push({ at: Date.now(), event }));
    const of = (type: string) => events.filter(({ event }) => event.type === type);
    const toolEnd = (id: string) => of("tool_execution_end").map(({ event }) => event as unknown as
      { toolCallId: string; isError?: boolean; result?: unknown }).find(event => event.toolCallId === id);
    const wait = async (check: () => boolean, timeout: number, what: string) => {
      const deadline = Date.now() + timeout;
      while (!check()) {
        if (Date.now() > deadline) throw new Error(`timed out: ${what}`);
        await new Promise(resolve => setTimeout(resolve, 25));
      }
    };
    let state: unknown;
    try {
      await client.start();
      await client.prompt("Spawn the slow child, then carry on.");
      // 1-4. The spawn call times out, list and wait run, and the turn ends normally.
      await wait(() => requests.some(row => row.reply === "done"), OUTER_TIMEOUT_MS + 40_000, "Main ends its turn");
      await wait(() => {
        const settled = of("agent_settled");
        return settled.length > 0 && settled.length === of("agent_start").length && Date.now() - settled.at(-1)!.at > 3_000;
      }, 30_000, "Main idle after its turn");
      state = await client.getState();
      expect(state).toMatchObject({ isStreaming: false });
      expect(requests.filter(row => row.model === "main").map(row => row.reply)).toEqual(["spawn", "list", "wait", "done"]);
      const spawnCall = toolEnd("toolu_spawn_9271");
      expect(spawnCall?.isError).toBe(true);                       // the executor deadline is a tool error
      expect(JSON.stringify(spawnCall?.result)).toMatch(/timed out|deadline|timeout/i);
      expect(JSON.stringify(toolEnd("toolu_list_9271")?.result)).toContain(CHILD_NAME);
      expect(JSON.stringify(toolEnd("toolu_wait_9271")?.result)).toContain("waitTimedOut");
      // The timeout neither aborted the Main's run nor failed its turn.
      const stopReasons = events.flatMap(({ event }) => event.type === "message_end" && event.message.role === "assistant"
        ? [(event.message as { stopReason?: string }).stopReason] : []);
      expect(stopReasons).toEqual(["toolUse", "toolUse", "toolUse", "stop"]);
      expect(timing.childRequested).toBeTypeOf("number");     // the child is live, held at inference
      const startsBefore = of("agent_start").length;
      // 5. The child completes only after the Main's turn has ended.
      timing.released = Date.now();
      releaseChild();
      await wait(() => timing.childResponded !== undefined, 10_000, "child response");
      // 6. The completion wakes the Main into a new turn that sees it, within 10 s.
      try {
        await wait(() => requests.some(row => row.model === "main" && row.marker), 10_000, "Main turn with the child completion");
      } catch (error) {
        throw new Error(`${String(error)}; agent_start after release: ${of("agent_start").length - startsBefore}`);
      }
      timing.mainSawCompletion = requests.find(row => row.model === "main" && row.marker)!.at;
      expect(timing.mainSawCompletion - timing.childResponded!).toBeLessThan(10_000);
      expect(of("agent_start").length).toBeGreaterThan(startsBefore);
      await wait(() => of("agent_settled").length === of("agent_start").length && !!of("agent_settled").length &&
        of("agent_settled").at(-1)!.at > timing.mainSawCompletion!, 20_000, "woken turn settled");
      expect(await client.getLastAssistantText()).toBe(SEEN_MARKER);
      const messages = await client.getMessages();
      const completions = messages.filter(message => message.role === "custom" &&
        (message as { customType?: string }).customType === "pi-fabric-agent-complete");
      expect(completions).toHaveLength(1);
      expect(JSON.stringify(completions[0])).toContain(CHILD_MARKER);
    } finally {
      const entries = await client.getEntries().catch(() => undefined);
      await client.abort().catch(() => undefined);
      await client.stop();
      server.close();
      const dest = process.env.FABRIC_9271_EVIDENCE_DIR;
      if (dest) {
        fs.mkdirSync(dest, { recursive: true });
        fs.writeFileSync(path.join(dest, "rpc-events.json"), JSON.stringify(events, null, 2));
        fs.writeFileSync(path.join(dest, "provider-requests.json"), JSON.stringify(requests, null, 2));
        fs.writeFileSync(path.join(dest, "timing.json"), JSON.stringify({ ...timing,
          completionToMainTurnMs: timing.mainSawCompletion !== undefined && timing.childResponded !== undefined
            ? timing.mainSawCompletion - timing.childResponded : null, state }, null, 2));
        fs.writeFileSync(path.join(dest, "session-entries.json"), JSON.stringify(entries ?? null, null, 2));
        fs.writeFileSync(path.join(dest, "main-stderr.log"), client.getStderr());
        fs.writeFileSync(path.join(dest, "artifact.json"), JSON.stringify({ cli: fs.realpathSync(cli), entry,
          sha256: createHash("sha256").update(fs.readFileSync(entry)).digest("hex") }, null, 2));
      }
      fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
      vi.unstubAllEnvs();
    }
  }, 150_000);
});
