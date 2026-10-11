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
const CHILD_MARKER = "CHILD_DONE_9271_COMPACT";
const SEEN_MARKER = "SAW_COMPLETION_9271_COMPACT";
const CHILD_NAME = "slow-child-9271-compact";
const PROVIDER = "compact-9271";
// Pi's own summarization system prompt marks a compaction request.
const SUMMARIZER = "context summarization assistant";

// smarty-dev#9271 (round 3 of #6134): Pi counts a compacting Main as busy (isIdle() is false), and
// the completion inbox's idle timer skipped delivery when it found Main busy, without retrying.
// A compaction on an idle Main that no agent_settled follows (a manual /compact, Fabric's retry of a
// failed compaction) then left a child completion that landed during it undelivered until some
// other turn. Real native Pi RPC Main -> fabric_exec agents.spawn -> built worker -> real native Pi
// child; only inference (including the compaction summary) is a loopback Anthropic Messages server.
// Explicit artifact proof: bun run build, then FABRIC_6134_REAL_CLI=1 bunx vitest run <this file>.
type Mode = "manual" | "turn-end";
const scenario = async (mode: Mode): Promise<void> => {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), `fabric-compact-wake-9271-${mode}-`)));
  const cwd = path.join(root, "main");
  const agentDir = path.join(root, "agent");
  fs.mkdirSync(cwd, { recursive: true });
  fs.mkdirSync(agentDir, { recursive: true });
  const requests: Array<{ at: number; model: string; reply: string; marker: boolean }> = [];
  const events: Array<{ at: number; event: RpcEvent }> = [];
  const timing: Record<string, number | undefined> = {};
  let releaseChild!: () => void;
  const childGate = new Promise<void>(resolve => { releaseChild = resolve; });
  let releaseCompaction!: () => void;
  const compactionGate = new Promise<void>(resolve => { releaseCompaction = resolve; });
  let count = 0;
  const server = http.createServer((request, response) => {
    let body = "";
    request.on("data", chunk => { body += chunk; });
    request.on("end", async () => {
      const data = JSON.parse(body) as { model: string; messages: unknown[]; system?: unknown };
      const transcript = JSON.stringify(data.messages);
      const send = (blocks: Array<{ type: "text"; text: string } | { type: "tool_use"; id: string; name: string; input: unknown }>, stopReason: string) => {
        response.writeHead(200, { "content-type": "text/event-stream" });
        const event = (name: string, value: unknown) => response.write(`event: ${name}\ndata: ${JSON.stringify({ type: name, ...value as object })}\n\n`);
        // A large Main context makes Fabric's configured token threshold compact at turn end.
        event("message_start", { message: { id: `msg_${++count}`, type: "message", role: "assistant", model: data.model, content: [],
          stop_reason: null, usage: { input_tokens: data.model === "main" ? 5_000 : 1, output_tokens: 0 } } });
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
      const tool = (id: string, code: string) => send([{ type: "tool_use", id, name: "fabric_exec", input: { code } }], "tool_use");
      if (JSON.stringify(data.system ?? "").includes(SUMMARIZER)) {
        // Only the first compaction is held: it is the one the child completes during.
        if (timing.compactionRequested === undefined) {
          timing.compactionRequested = Date.now();
          await compactionGate;
        }
        log("summary");
        send([{ type: "text", text: "## Goal\nSummary of the earlier conversation." }], "end_turn");
        timing.compactionResponded ??= Date.now();
      } else if (data.model === "child") {
        timing.childRequested = Date.now();
        await childGate;
        log("child-result");
        send([{ type: "text", text: `${CHILD_MARKER}: slow child finished` }], "end_turn");
        timing.childResponded = Date.now();
      } else if (transcript.includes(CHILD_MARKER)) {
        log("saw-completion");
        send([{ type: "text", text: SEEN_MARKER }], "end_turn");
      } else if (!transcript.includes("toolu_spawn_9271c")) {
        log("spawn");
        tool("toolu_spawn_9271c", `const handle = await agents.spawn({ name: '${CHILD_NAME}', task: 'Answer briefly.', model: '${PROVIDER}/child', transport: 'process', extensions: false, tools: [] }); return handle.id;`);
      } else {
        log("done");
        send([{ type: "text", text: "done" }], "end_turn");
      }
    });
  });
  await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
  const model = (id: string) => ({ id, name: id, reasoning: false, input: ["text"], contextWindow: 200_000, maxTokens: 4096,
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } });
  fs.writeFileSync(path.join(agentDir, "models.json"), JSON.stringify({ providers: { [PROVIDER]: {
    baseUrl: `http://127.0.0.1:${(server.address() as AddressInfo).port}`, apiKey: "offline-only", api: "anthropic-messages",
    models: [model("main"), model("child")] } } }));
  // Pi's own automatic compaction stays off; keepRecentTokens 1 leaves a small session something to summarize.
  fs.writeFileSync(path.join(agentDir, "settings.json"), JSON.stringify({ retry: { enabled: false }, compaction: { enabled: false, keepRecentTokens: 1 } }));
  fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({
    executor: { kernel: "typescript" }, fullCodeMode: false, autoReload: false,
    mesh: { enabled: true, announce: true }, agents: { transport: "process", nice: 19, sessionExport: false, timeoutMs: 120_000 },
    mcp: { enabled: false }, jev: { enabled: false }, memory: { enabled: false }, records: { enabled: false },
    prewalk: { enabled: false, alwaysRearm: false }, entropy: { compile: false }, speculation: { enabled: false },
    // turn-end: Fabric's configured threshold compacts the idle Main inside its agent_settled hook.
    compaction: { engine: "pi", ...(mode === "turn-end" ? { tokenThresholds: { [`${PROVIDER}/main`]: 1_000 } } : {}) },
  }));
  // The Main must not join this agent's own fleet session or mailbox.
  for (const key of Object.keys(process.env)) if (/^(PI_|HERDR_)/.test(key)) vi.stubEnv(key, undefined);
  const client = new RpcClient({ cliPath: cli, cwd, provider: PROVIDER, model: "main",
    env: {
      PI_CODING_AGENT_DIR: agentDir, PI_FABRIC_AGENT_DIR: path.join(root, "exports"), PI_FABRIC_MESH_ROOT: path.join(root, "mesh"),
      PI_FABRIC_PROJECT_ROOT: cwd, PI_FABRIC_RUN_ROOT: path.join(root, "runs"), PI_FABRIC_PI_BINARY: cli, PI_OFFLINE: "1",
    },
    args: ["--no-extensions", "--no-skills", "--no-prompt-templates", "--no-themes", "--no-context-files", "--approve",
      "--thinking", "off", "--session-dir", path.join(root, "sessions"), "-e", entry],
  });
  client.onEvent(event => events.push({ at: Date.now(), event }));
  const of = (type: string) => events.filter(({ event }) => event.type === type);
  // RPC forwards the inbox's ui.notify as an extension_ui_request: the completion reached the inbox.
  const notice = (event: RpcEvent) => (event as { type: string }).type === "extension_ui_request" &&
    String((event as { message?: unknown }).message ?? "").includes(`Agent ${CHILD_NAME} completed`);
  const notified = () => events.some(({ event }) => notice(event));
  const wait = async (check: () => boolean, timeout: number, what: string) => {
    const deadline = Date.now() + timeout;
    while (!check()) {
      if (Date.now() > deadline) throw new Error(`timed out: ${what}`);
      await new Promise(resolve => setTimeout(resolve, 25));
    }
  };
  let state: unknown;
  let manualCompaction: Promise<unknown> | undefined;
  try {
    await client.start();
    await client.prompt("Spawn the slow child, then carry on.");
    await wait(() => requests.some(row => row.reply === "done"), 40_000, "Main ends its turn");
    await wait(() => timing.childRequested !== undefined, 30_000, "child live, held at inference");
    if (mode === "manual") {
      // The Main's run has fully settled and it is idle; then /compact starts with no run.
      await wait(() => of("agent_settled").length > 0 && of("agent_settled").length === of("agent_start").length, 20_000, "Main settled");
      state = await client.getState();
      expect(state).toMatchObject({ isStreaming: false });
      manualCompaction = client.compact().catch(error => error);
    }
    // turn-end: Fabric compacts inside the settling Main's agent_settled hook.
    await wait(() => timing.compactionRequested !== undefined, 20_000, "compaction requested");
    const startsBefore = of("agent_start").length;
    // The child completes while the Main is compacting; the inbox's idle timer finds Main busy.
    timing.released = Date.now();
    releaseChild();
    await wait(() => timing.childResponded !== undefined, 10_000, "child response");
    await wait(notified, 10_000, "completion reached the Main's inbox");
    timing.enqueued = events.find(({ event }) => notice(event))!.at;
    await new Promise(resolve => setTimeout(resolve, 1_500));
    expect(requests.some(row => row.model === "main" && row.marker)).toBe(false); // nothing delivered mid-compaction
    expect(of("agent_start").length).toBe(startsBefore);
    timing.compactionReleased = Date.now();
    releaseCompaction();
    await wait(() => of("compaction_end").length > 0, 10_000, "compaction end");
    timing.compactionEnded = of("compaction_end")[0]!.at;
    expect(of("compaction_end")[0]!.event).toMatchObject({ aborted: false });
    // The completion wakes the Main into a new turn that sees it, within 10 s of compaction ending.
    try {
      await wait(() => requests.some(row => row.model === "main" && row.marker), 10_000, "Main turn with the child completion");
    } catch (error) {
      throw new Error(`${String(error)}; agent_start after release: ${of("agent_start").length - startsBefore}`);
    }
    timing.mainSawCompletion = requests.find(row => row.model === "main" && row.marker)!.at;
    expect(timing.mainSawCompletion - timing.compactionEnded).toBeLessThan(10_000);
    expect(of("agent_start").length).toBeGreaterThan(startsBefore);
    await wait(() => of("agent_settled").length === of("agent_start").length && !!of("agent_settled").length &&
      of("agent_settled").at(-1)!.at > timing.mainSawCompletion!, 20_000, "woken turn settled");
    expect(await client.getLastAssistantText()).toBe(SEEN_MARKER);
    // Exactly one delivery: the retry never sends a completion twice. Count session entries, since a
    // later compaction drops the delivered message from the model context.
    const { entries } = await client.getEntries();
    const completions = entries.filter(entry => entry.type === "custom_message" &&
      (entry as { customType?: string }).customType === "pi-fabric-agent-complete");
    expect(completions).toHaveLength(1);
    expect(JSON.stringify(completions[0])).toContain(CHILD_MARKER);
    expect(requests.filter(row => row.model === "main" && row.reply === "saw-completion")).toHaveLength(1);
  } finally {
    releaseChild();
    releaseCompaction();
    await manualCompaction;
    const entries = await client.getEntries().catch(() => undefined);
    await client.abort().catch(() => undefined);
    await client.stop();
    server.close();
    const dest = process.env.FABRIC_9271_EVIDENCE_DIR;
    if (dest) {
      const dir = path.join(dest, mode);
      fs.mkdirSync(dir, { recursive: true });
      fs.writeFileSync(path.join(dir, "rpc-events.json"), JSON.stringify(events, null, 2));
      fs.writeFileSync(path.join(dir, "provider-requests.json"), JSON.stringify(requests, null, 2));
      fs.writeFileSync(path.join(dir, "timing.json"), JSON.stringify({ ...timing,
        compactionEndToMainTurnMs: timing.mainSawCompletion !== undefined && timing.compactionEnded !== undefined
          ? timing.mainSawCompletion - timing.compactionEnded : null, state }, null, 2));
      fs.writeFileSync(path.join(dir, "session-entries.json"), JSON.stringify(entries ?? null, null, 2));
      fs.writeFileSync(path.join(dir, "main-stderr.log"), client.getStderr());
      const sha = (file: string) => createHash("sha256").update(fs.readFileSync(file)).digest("hex");
      // The inbox is bundled into the runtime-state chunk, not the entry file.
      const inbox = path.join(repo, "dist/fabric-runtime-state.js");
      fs.writeFileSync(path.join(dir, "artifact.json"), JSON.stringify({ cli: fs.realpathSync(cli), entry,
        sha256: sha(entry), inboxChunk: inbox, inboxChunkSha256: sha(inbox) }, null, 2));
    }
    fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
    vi.unstubAllEnvs();
  }
};

describe.runIf(process.platform === "linux" && process.env.FABRIC_6134_REAL_CLI === "1")("child completion during an idle Main's compaction (#9271)", () => {
  it("manual /compact: wakes the Main into a new turn with the completion within 10 s of compaction ending", () => scenario("manual"), 120_000);
  it("turn-end threshold compaction: wakes the Main into a new turn with the completion within 10 s of compaction ending", () => scenario("turn-end"), 120_000);
});
