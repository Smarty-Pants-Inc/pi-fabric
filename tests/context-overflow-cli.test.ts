import { createHash } from "node:crypto";
import fs from "node:fs";
import http from "node:http";
import type { AddressInfo, Socket } from "node:net";
import os from "node:os";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import type { RpcClient as PiRpcClient } from "@earendil-works/pi-coding-agent";
import { describe, expect, it } from "vitest";
import {
  OVERFLOW_GIANT_BEGIN, OVERFLOW_GIANT_BYTES, OVERFLOW_GIANT_END, OVERFLOW_SMALL_TEXT, overflowGiantText,
} from "./fixtures/overflow-giant-reply.js";

// smarty-dev#19998 FixB: trust-boundary/data-safety proof through real native CLI + built Fabric.
// Expected skip: ordinary SDK runs use npm Pi 1.1, which predates native repair-only continuation.
// The parent MUST run both explicit repair modes against frozen built artifacts:
// - summary-cut: UNPATCHED installed 741 CLI + built Fabric (2,700,000 bytes, 3 MiB, small-only).
// - context-edit: compiled native candidate + built Fabric (3 MiB, small-only).
// FABRIC_OVERFLOW_REAL_CLI=1 FABRIC_OVERFLOW_REPAIR_MODE=summary-cut|context-edit \
//   FABRIC_OVERFLOW_PI_CLI=<installed-or-candidate>/dist/cli.js FABRIC_OVERFLOW_EVIDENCE_DIR=<private-dir> \
//   bunx vitest run tests/context-overflow-cli.test.ts
// Optional FABRIC_OVERFLOW_FABRIC_ENTRY=<pristine-clone>/dist/index.js selects an immutable RED baseline.
// No paid inference, mocks of recovery, prewritten session, capability injection, or manual retry.
const repo = fileURLToPath(new URL("../", import.meta.url));
// A frozen baseline artifact can prove RED without rewinding the live implementation lane.
const fabricEntry = path.resolve(process.env.FABRIC_OVERFLOW_FABRIC_ENTRY ?? path.join(repo, "dist/index.js"));
const fixture = path.join(repo, "tests/fixtures/overflow-giant-reply.ts");
const cli = process.env.FABRIC_OVERFLOW_PI_CLI;
const evidenceDir = process.env.FABRIC_OVERFLOW_EVIDENCE_DIR;
const repairMode = process.env.FABRIC_OVERFLOW_REPAIR_MODE ?? "context-edit";
const scenarios = [
  ...(repairMode === "summary-cut" ? [{ name: "2.7 MB org-sized reply", bytes: 2_700_000 }] : []),
  { name: "exact 3 MiB reply", bytes: OVERFLOW_GIANT_BYTES },
  { name: "small-only counterexample", bytes: 0 },
];
const PROVIDER = "overflow-private-loopback";
const KEY = "offline-overflow-test-not-a-real-key";
const PROMPT = "OVERFLOW_ONE_USER_PROMPT: call giant_reply and small_reply, then answer after recovery.";
const REOPEN_PROMPT = "OVERFLOW_REOPEN_CHECK: confirm the retained tool results without calling tools.";
const ANSWER = "OVERFLOW_RECOVERED_IN_THE_ORIGINAL_PROMPT";
const REOPEN_ANSWER = "OVERFLOW_REOPEN_RETAINED_THE_STUB";
const GIANT_CALL = "toolu_overflow_giant";
const SMALL_CALL = "toolu_overflow_small";
const ENV_ALLOW = new Set(["PATH", "LANG", "LC_ALL", "LC_CTYPE", "TZ"]);
const sha256 = (value: string | Buffer): string => createHash("sha256").update(value).digest("hex");
const textOf = (content: unknown): string => typeof content === "string" ? content : Array.isArray(content)
  ? content.flatMap(part => part?.type === "text" && typeof part.text === "string" ? [part.text] : []).join("\n") : "";

type RawEntry = {
  type: string; id: string; targetId?: string; replacement?: { content: unknown } | null;
  summary?: string; firstKeptEntryId?: string;
  message?: { role: string; toolName?: string; toolCallId?: string; content?: unknown; stopReason?: string };
};
type RequestBody = { model: string; system?: unknown; messages: Array<{ role: string; content: unknown }>; tools?: Array<{ name: string }> };
type ToolReceipt = { id: string; bytes: number; sha256: string; text: string | null };
type RequestReceipt = {
  step: number; model: string; bytes: number; sha256: string; kind: string; status: number;
  hasGiant: boolean; hasOriginalPrompt: boolean; hasReopenPrompt: boolean; hasSourceStub: boolean;
  isSummary: boolean; systemSha256: string; tools: string[]; toolUses: string[]; toolResults: ToolReceipt[];
};
type EventReceipt = { step: number; type: string; role?: string; stopReason?: string; textBytes?: number; textSha256?: string };
const parseEntries = (bytes: Buffer): RawEntry[] => bytes.toString("utf8").trimEnd().split("\n").map(line => JSON.parse(line) as RawEntry);
const toolEntry = (entries: RawEntry[], call: string): RawEntry => {
  const matches = entries.filter(entry => entry.type === "message" && entry.message?.role === "toolResult" && entry.message.toolCallId === call);
  expect(matches, `one intact raw tool result for ${call}`).toHaveLength(1);
  return matches[0]!;
};
const assertStub = (text: string, sourceId: string, bytes: number): void => {
  expect(Buffer.byteLength(text), "bounded model-only stub").toBeLessThan(8 * 1024);
  expect(text, "stub identifies retained source entry").toContain(sourceId);
  expect(text, "stub labels its original byte count").toMatch(/bytes?\b/i);
  expect(text.replace(/(\d)[,_ ](?=\d)/g, "$1"), "exact original UTF-8 bytes in stub").toContain(String(bytes));
  expect(text).not.toContain(OVERFLOW_GIANT_BEGIN);
  expect(text).not.toContain(OVERFLOW_GIANT_END);
};

// Import the candidate's own public RPC client, never this repo's protocol-incompatible npm client.
describe.runIf(process.platform === "linux" && process.env.FABRIC_OVERFLOW_REAL_CLI === "1")("real CLI context-overflow recovery", () => {
  it.each(scenarios)("$name: SAME-prompt recovery, append-only raw history and durable reopened projection", async ({ bytes: giantBytes }) => {
    expect(["context-edit", "summary-cut"], "select one explicit repair contract, never silently accept either").toContain(repairMode);
    if (!cli) throw new Error("FABRIC_OVERFLOW_PI_CLI must name the compiled native candidate cli.js");
    for (const artifact of [cli, fabricEntry, fixture]) expect(fs.existsSync(artifact), `missing built proof input: ${artifact}`).toBe(true);
    const candidateIndex = path.join(path.dirname(cli), "index.js");
    const { RpcClient } = await import(pathToFileURL(candidateIndex).href) as { RpcClient: typeof PiRpcClient };
    // All session data is newly created under the test-owned temp root, not the caller's profile.
    // An evidence directory retains only bounded JSON receipts/hashes, never JSONL, RPC traces or keys.
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-overflow-cli-")));
    const home = path.join(root, "home");
    const tmp = path.join(root, "tmp");
    const cwd = path.join(root, "workspace");
    const agentDir = path.join(root, "agent");
    for (const dir of [home, tmp, cwd, agentDir]) fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
    const prompt = giantBytes ? PROMPT : "OVERFLOW_SMALL_ONLY: call small_reply and answer without altering its result.";
    const giant = giantBytes ? overflowGiantText(giantBytes) : "";
    const giantHash = sha256(giant);
    expect(Buffer.byteLength(giant)).toBe(giantBytes);
    const requests: RequestReceipt[] = [];
    const events: EventReceipt[] = [];
    const failures: string[] = [];
    const sockets = new Set<Socket>();
    const clients: PiRpcClient[] = [];
    const checks: Record<string, boolean> = {};
    let step = 0;
    let phase: "initial" | "reopen" = "initial";
    let sessionFile = "";
    let prefix: Buffer | undefined;
    let originalGiant: RawEntry | undefined;
    let originalSmall: RawEntry | undefined;
    let afterRecovery: Buffer | undefined;
    let overflowIndex = -1;
    let passed = false;
    let testFailure: string | undefined;
    let manualPromptsBeforeReopen = 0;
    let stub = "";
    let repairEntry: RawEntry | undefined;
    // Both repair contracts count UTF-8 JSON-serialized MODEL-FACING CONTENT, not
    // whole-message JSON or plain text. The 50 KiB guard threshold is 51,200 such bytes.
    // Record all three measures so the original 2.7 MB text payload remains unambiguous.
    const serializedToolBytes = () => Buffer.byteLength(JSON.stringify(originalGiant!.message), "utf8");
    const serializedContentBytes = () => Buffer.byteLength(JSON.stringify(originalGiant!.message!.content), "utf8");
    const summaryStub = () => `[tool reply of ${serializedContentBytes()} bytes from giant_reply omitted; raw entry ${originalGiant!.id} kept in the session file]`;
    let reopenedBytes = 0;
    let reopenedPrefixHash: string | undefined;

    // Provider requests and RPC events drive the waits. A single deadline is only a safety net.
    const waiters = new Set<() => void>();
    const notify = () => { for (const waiter of [...waiters]) waiter(); };
    const until = (check: () => boolean, what: string): Promise<void> => new Promise((resolve, reject) => {
      const done = () => { clearTimeout(timer); waiters.delete(probe); };
      const probe = () => {
        if (failures.length) { done(); reject(new Error(failures.join("; "))); }
        else if (check()) { done(); resolve(); }
      };
      const timer = setTimeout(() => { done(); reject(new Error(`60 s deadline: ${what}`)); }, 60_000);
      waiters.add(probe);
      probe();
    });
    const assertPrefix = (bytes: Buffer): void => {
      expect(prefix, "raw prefix captured BEFORE returning HTTP400").toBeDefined();
      expect(bytes.length).toBeGreaterThanOrEqual(prefix!.length);
      expect(bytes.subarray(0, prefix!.length).equals(prefix!), "raw session starts with the exact pre-400 bytes").toBe(true);
      expect(toolEntry(parseEntries(bytes), GIANT_CALL)).toEqual(originalGiant);
      expect(toolEntry(parseEntries(bytes), SMALL_CALL)).toEqual(originalSmall);
    };
    const server = http.createServer((request, response) => {
      const chunks: Buffer[] = [];
      let size = 0;
      request.on("data", (chunk: Buffer) => {
        size += chunk.length;
        if (size > 16 * 1024 * 1024) request.destroy(new Error("offline request exceeds 16 MiB test budget"));
        else chunks.push(chunk);
      });
      request.on("error", () => { failures.push("loopback request stream failed"); notify(); });
      request.on("end", () => {
        let current: RequestReceipt | undefined;
        try {
          expect(request.socket.remoteAddress).toBe("127.0.0.1");
          expect(request.headers["x-api-key"]).toBe(KEY);
          const body = Buffer.concat(chunks);
          const data = JSON.parse(body.toString("utf8")) as RequestBody;
          const transcript = JSON.stringify(data.messages);
          const system = textOf(data.system);
          const isSummary = system.includes("You are a context summarization assistant.")
            && system.includes("Do NOT continue the conversation.");
          const toolResults = data.messages.flatMap(message => Array.isArray(message.content)
            ? message.content.filter(part => part?.type === "tool_result").map(part => {
              const text = textOf(part.content);
              return { id: String(part.tool_use_id), bytes: Buffer.byteLength(text), sha256: sha256(text),
                text: Buffer.byteLength(text) <= 8 * 1024 ? text : null };
            }) : []);
          const row: RequestReceipt = {
            step: ++step, model: data.model, bytes: body.length, sha256: sha256(body), kind: "unclassified", status: 200,
            // Native summarization may quote a bounded tool excerpt (normally 2k chars).
            // A complete giant, or even an unbounded 4k run, is never a repaired request.
            hasGiant: transcript.includes("G".repeat(4096)),
            hasOriginalPrompt: transcript.includes(prompt), hasReopenPrompt: transcript.includes(REOPEN_PROMPT),
            hasSourceStub: originalGiant !== undefined && transcript.includes(summaryStub()),
            isSummary, systemSha256: sha256(system), tools: (data.tools ?? []).map(tool => tool.name),
            toolUses: data.messages.flatMap(message => Array.isArray(message.content)
              ? message.content.filter(part => part?.type === "tool_use").map(part => String(part.id)) : []), toolResults,
          };
          requests.push(row);
          current = row;
          // Classify by the known native summarization SYSTEM prompt, not by ordinary
          // conversation text containing 'summary', or by the absence of a tool alone.
          const summary = isSummary;
          if (summary) expect(row.tools).toHaveLength(0);
          if (requests.length === 1) {
            expect(row.tools).toEqual(expect.arrayContaining(["fabric_exec", "giant_reply", "small_reply"]));
            expect(row.hasOriginalPrompt).toBe(true);
            row.kind = "tool_use";
          } else if (phase === "initial" && giantBytes === 0) {
            expect(toolResults.find(tool => tool.id === SMALL_CALL)?.text).toBe(OVERFLOW_SMALL_TEXT);
            expect(toolResults.some(tool => tool.id === GIANT_CALL)).toBe(false);
            row.kind = "small_complete";
          } else if (phase === "initial" && overflowIndex < 0) {
            expect(toolResults.find(tool => tool.id === GIANT_CALL)).toMatchObject({ bytes: giantBytes, sha256: giantHash });
            expect(toolResults.find(tool => tool.id === SMALL_CALL)?.text).toBe(OVERFLOW_SMALL_TEXT);
            expect(sessionFile.startsWith(root + path.sep), "never inspect a real user session").toBe(true);
            prefix = fs.readFileSync(sessionFile);
            expect(prefix.at(-1)).toBe(10);
            const entries = parseEntries(prefix);
            originalGiant = toolEntry(entries, GIANT_CALL);
            originalSmall = toolEntry(entries, SMALL_CALL);
            expect(originalGiant.message?.toolName).toBe("giant_reply");
            expect(textOf(originalGiant.message?.content)).toBe(giant);
            expect(textOf(originalSmall.message?.content)).toBe(OVERFLOW_SMALL_TEXT);
            expect(entries.some(entry => entry.type === "context_edit" && entry.targetId === originalGiant!.id)).toBe(false);
            checks.fullSizedRawBefore400 = true;
            row.kind = "overflow_400";
            row.status = 400;
            overflowIndex = requests.length - 1;
            response.writeHead(400, { "content-type": "application/json", "request-id": "offline-context-overflow" });
            response.end(JSON.stringify({ type: "error", error: { type: "invalid_request_error",
              message: "prompt is too long: 1100001 tokens > 1000000 maximum" } }));
            notify();
            return;
          } else {
            expect(row.hasGiant, "no post-400 request, including summaries, may resend the giant reply").toBe(false);
            row.kind = summary ? "summary" : phase === "reopen" ? "reopened" : "recovered";
          }
          response.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-cache" });
          const event = (name: string, value: object) => response.write(`event: ${name}\ndata: ${JSON.stringify({ type: name, ...value })}\n\n`);
          event("message_start", { message: { id: `msg_offline_${requests.length}`, type: "message", role: "assistant", model: data.model,
            content: [], stop_reason: null, usage: { input_tokens: 1, output_tokens: 0 } } });
          if (row.kind === "tool_use") {
            const calls = giantBytes ? [{ id: GIANT_CALL, name: "giant_reply", args: { bytes: giantBytes } },
              { id: SMALL_CALL, name: "small_reply", args: {} }] : [{ id: SMALL_CALL, name: "small_reply", args: {} }];
            for (const [index, call] of calls.entries()) {
              const { id, name, args } = call;
              event("content_block_start", { index, content_block: { type: "tool_use", id, name, input: {} } });
              event("content_block_delta", { index, delta: { type: "input_json_delta", partial_json: JSON.stringify(args) } });
              event("content_block_stop", { index });
            }
          } else {
            // Never manufacture the byte/source omission stub here: the real guard must add it.
            const reply = summary ? `## Goal\n${PROMPT}\n## Progress\nThe tools completed. Raw results are retained in the session file.\n## Next Steps\nAnswer the original user without asking for another prompt.`
              : phase === "reopen" ? REOPEN_ANSWER : ANSWER;
            event("content_block_start", { index: 0, content_block: { type: "text", text: "" } });
            event("content_block_delta", { index: 0, delta: { type: "text_delta", text: reply } });
            event("content_block_stop", { index: 0 });
          }
          event("message_delta", { delta: { stop_reason: row.kind === "tool_use" ? "tool_use" : "end_turn" }, usage: { output_tokens: 1 } });
          event("message_stop", {});
          response.end();
          notify();
        } catch (error) {
          if (current) { current.status = 500; current.kind = "proof_contract_failure"; }
          failures.push(String(error).slice(0, 2048));
          response.writeHead(500, { "content-type": "application/json" });
          response.end(JSON.stringify({ type: "error", error: { type: "api_error", message: "offline proof contract failed" } }));
          notify();
        }
      });
    });
    server.on("connection", socket => { sockets.add(socket); socket.on("close", () => sockets.delete(socket)); });
    const makeClient = (reopen?: string): PiRpcClient => {
      const omitted: Record<string, string | undefined> = {};
      // RpcClient merges process.env: explicitly unset every non-allowlisted inherited variable,
      // including proxy settings, provider credentials, NODE_OPTIONS, PI_*, HERDR_* and session IDs.
      for (const key of Object.keys(process.env)) if (!ENV_ALLOW.has(key)) omitted[key] = undefined;
      const env = { ...omitted, HOME: home, TMPDIR: tmp, XDG_CONFIG_HOME: path.join(home, ".config"),
        XDG_CACHE_HOME: path.join(home, ".cache"), XDG_DATA_HOME: path.join(home, ".local/share"), XDG_STATE_HOME: path.join(home, ".local/state"),
        PI_CODING_AGENT_DIR: agentDir, PI_FABRIC_AGENT_DIR: path.join(root, "exports"), PI_FABRIC_MESH_ROOT: path.join(root, "mesh"),
        PI_FABRIC_PROJECT_ROOT: cwd, PI_FABRIC_RUN_ROOT: path.join(root, "runs"), PI_FABRIC_PI_BINARY: cli, PI_OFFLINE: "1" };
      const client = new RpcClient({ cliPath: cli, cwd, provider: PROVIDER, model: "offline-million-token-model", env: env as Record<string, string>,
        args: ["--no-extensions", "--no-skills", "--no-prompt-templates", "--no-themes", "--no-context-files", "--approve", "--thinking", "off",
          "--session-dir", path.join(root, "sessions"), ...(reopen ? ["--session", reopen] : []), "-e", fabricEntry, "-e", fixture] });
      client.onEvent(event => {
        const row: EventReceipt = { step: ++step, type: event.type };
        if (event.type === "message_end") {
          row.role = event.message.role;
          const text = textOf("content" in event.message ? event.message.content : undefined);
          row.textBytes = Buffer.byteLength(text);
          row.textSha256 = sha256(text);
          if (event.message.role === "assistant") row.stopReason = event.message.stopReason;
        }
        events.push(row);
        notify();
      });
      clients.push(client);
      return client;
    };

    try {
      await new Promise<void>((resolve, reject) => { server.once("error", reject); server.listen(0, "127.0.0.1", resolve); });
      fs.writeFileSync(path.join(agentDir, "models.json"), JSON.stringify({ providers: { [PROVIDER]: {
        baseUrl: `http://127.0.0.1:${(server.address() as AddressInfo).port}`, apiKey: KEY, api: "anthropic-messages",
        models: [{ id: "offline-million-token-model", name: "Offline million-token model", reasoning: false, input: ["text"],
          contextWindow: 1_000_000, maxTokens: 4096, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } }],
      } } }));
      // 3 MiB / 4 ~= 786k estimated tokens, below the 1M threshold. We must reach the
      // provider with full tool text, not win by proactive compaction before the injected 400.
      fs.writeFileSync(path.join(agentDir, "settings.json"), JSON.stringify({ retry: { enabled: false }, compaction: { enabled: true } }));
      // Use Pi's native kept-tail selector, not Fabric's existing cut planner: the latter can drop the giant
      // before retry and hide the retained-oversize RED this fixture is meant to exercise in both modes.
      fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({ fullCodeMode: false, autoReload: false,
        compaction: { engine: "pi" }, mesh: { enabled: false }, mcp: { enabled: false }, jev: { enabled: false }, memory: { enabled: false }, records: { enabled: false },
        prewalk: { enabled: false, alwaysRearm: false }, entropy: { compile: false }, speculation: { enabled: false } }));
      const client = makeClient();
      await client.start();
      const initialState = await client.getState();
      expect(initialState.sessionFile).toEqual(expect.any(String));
      sessionFile = initialState.sessionFile!;
      manualPromptsBeforeReopen++;
      await client.prompt(prompt);
      await until(() => events.some(event => event.type === "agent_settled"), "original prompt settles after automatic recovery");
      expect(await client.getLastAssistantText(), "the original prompt must answer without a user nudge").toBe(ANSWER);
      expect(manualPromptsBeforeReopen).toBe(1);
      expect(await client.getState()).toMatchObject({ isStreaming: false, pendingMessageCount: 0 });
      if (giantBytes === 0) {
        const raw = parseEntries(fs.readFileSync(sessionFile));
        originalSmall = toolEntry(raw, SMALL_CALL);
        expect(textOf(originalSmall.message?.content)).toBe(OVERFLOW_SMALL_TEXT);
        expect(raw.some(entry => entry.type === "compaction" || (entry.type === "context_edit" && entry.targetId === originalSmall!.id)),
          "without an oversized tool reply the guard is a true no-op").toBe(false);
        expect(raw.filter(entry => entry.type === "message" && entry.message?.role === "user")).toHaveLength(1);
        expect(requests.map(row => row.kind)).toEqual(["tool_use", "small_complete"]);
        checks.smallResultNoRepairOrCompaction = true;
        passed = true;
        return;
      }
      expect(overflowIndex).toBe(1);
      const recovered = requests.filter(row => row.kind === "recovered");
      expect(recovered, "exactly one repaired automatic retry").toHaveLength(1);
      expect(requests.filter(row => row.kind === "overflow_400")).toHaveLength(1);
      expect(recovered[0]!.step).toBeLessThan(events.find(event => event.type === "agent_settled")!.step);
      checks.samePromptAutomaticAnswer = true;

      afterRecovery = fs.readFileSync(sessionFile);
      assertPrefix(afterRecovery);
      const raw = parseEntries(afterRecovery);
      expect(raw.filter(entry => entry.type === "message" && entry.message?.role === "user")).toHaveLength(1);
      if (repairMode === "context-edit") {
        const edits = raw.filter(entry => entry.type === "context_edit" && entry.targetId === originalGiant!.id);
        expect(edits.length, "append-only giant result replacement").toBeGreaterThan(0);
        repairEntry = edits.at(-1)!;
        expect(repairEntry.replacement).not.toBeNull();
        stub = textOf(repairEntry.replacement?.content);
        assertStub(stub, originalGiant!.id, serializedContentBytes());
        expect(raw.some(entry => entry.type === "context_edit" && entry.targetId === originalSmall!.id), "small result must not be stubbed or omitted").toBe(false);
        expect(recovered[0]!.toolResults.find(tool => tool.id === GIANT_CALL)?.text,
          "same correlated tool result with the exact durable replacement content").toBe(stub);
        expect(recovered[0]!.toolUses).toContain(GIANT_CALL);
        expect(recovered[0]!.toolResults.find(tool => tool.id === SMALL_CALL)?.text).toBe(OVERFLOW_SMALL_TEXT);
      } else {
        // On the UNPATCHED installed runtime, Fabric advances the cut past the entire
        // assistant call/results group and appends the source/byte stub to native compact()'s summary.
        expect(raw.some(entry => entry.type === "context_edit" && entry.targetId === originalGiant!.id),
          "summary-cut mode must not pass by accidentally testing native context-edit repair").toBe(false);
        repairEntry = raw.filter(entry => entry.type === "compaction").at(-1);
        expect(repairEntry, "a genuine durable compaction, not merely a transient context hook").toBeDefined();
        stub = summaryStub();
        expect(repairEntry!.summary).toContain(stub);
        const cut = raw.findIndex(entry => entry.id === repairEntry!.firstKeptEntryId);
        expect(cut, "cut follows the giant result and every sibling result of its assistant call").toBeGreaterThan(
          Math.max(raw.findIndex(entry => entry.id === originalGiant!.id), raw.findIndex(entry => entry.id === originalSmall!.id)));
        expect(recovered[0]!.hasSourceStub).toBe(true);
        expect(recovered[0]!.toolUses).not.toContain(GIANT_CALL);
        expect(recovered[0]!.toolResults.some(tool => tool.id === GIANT_CALL)).toBe(false);
      }
      expect(raw.findIndex(entry => entry.id === repairEntry!.id)).toBeGreaterThan(parseEntries(prefix!).length - 1);
      checks.appendOnlyRepair = true;
      checks.exactPre400PrefixPreserved = true;
      const liveEntries = (await client.getEntries()).entries as unknown as RawEntry[];
      expect(toolEntry(liveEntries, GIANT_CALL)).toEqual(originalGiant);
      expect(liveEntries.find(entry => entry.id === repairEntry!.id)).toEqual(repairEntry);
      await client.stop(); // No second runtime writer of this session exists when reopening it.

      phase = "reopen";
      const reopened = makeClient(sessionFile);
      await reopened.start();
      expect((await reopened.getState()).sessionId).toBe(initialState.sessionId);
      const reopenedEntries = (await reopened.getEntries()).entries as unknown as RawEntry[];
      expect(toolEntry(reopenedEntries, GIANT_CALL)).toEqual(originalGiant);
      expect(toolEntry(reopenedEntries, SMALL_CALL)).toEqual(originalSmall);
      expect(reopenedEntries.find(entry => entry.id === repairEntry!.id)).toEqual(repairEntry);
      const projected = await reopened.getMessages();
      const projectedGiant = projected.find(message => message.role === "toolResult" && message.toolCallId === GIANT_CALL);
      if (repairMode === "context-edit") {
        expect(projectedGiant?.role).toBe("toolResult");
        expect(textOf(projectedGiant && "content" in projectedGiant ? projectedGiant.content : undefined)).toBe(stub);
      } else {
        expect(projectedGiant).toBeUndefined();
        expect(projected.some(message => "content" in message && textOf(message.content).includes(stub))).toBe(true);
        expect(projected.some(message => message.role === "assistant" && message.content.some(part => part.type === "toolCall" && part.id === GIANT_CALL))).toBe(false);
      }
      expect(JSON.stringify(projected)).not.toContain("G".repeat(4096));
      const beforeReopenPrompt = fs.readFileSync(sessionFile);
      expect(beforeReopenPrompt.subarray(0, afterRecovery.length).equals(afterRecovery), "reopen does not rewrite prior history").toBe(true);
      // A separate explicit prompt is allowed ONLY AFTER the original prompt already proved recovery.
      // It verifies the reopened model projection across a fresh native process, not a manual retry.
      const settledBefore = events.filter(event => event.type === "agent_settled").length;
      await reopened.prompt(REOPEN_PROMPT);
      await until(() => events.filter(event => event.type === "agent_settled").length > settledBefore, "reopened projection reaches the provider");
      expect(await reopened.getLastAssistantText()).toBe(REOPEN_ANSWER);
      const replay = requests.filter(row => row.kind === "reopened");
      expect(replay).toHaveLength(1);
      expect(replay[0]!.hasReopenPrompt).toBe(true);
      if (repairMode === "context-edit") {
        expect(replay[0]!.toolResults.find(tool => tool.id === GIANT_CALL)?.text).toBe(stub);
        expect(replay[0]!.toolResults.find(tool => tool.id === SMALL_CALL)?.text).toBe(OVERFLOW_SMALL_TEXT);
      } else {
        expect(replay[0]!.hasSourceStub).toBe(true);
        expect(replay[0]!.toolUses).not.toContain(GIANT_CALL);
        expect(replay[0]!.toolResults.some(tool => tool.id === GIANT_CALL)).toBe(false);
      }
      const afterReopen = fs.readFileSync(sessionFile);
      assertPrefix(afterReopen);
      expect(afterReopen.subarray(0, afterRecovery.length).equals(afterRecovery)).toBe(true);
      reopenedBytes = afterReopen.length;
      reopenedPrefixHash = sha256(afterReopen.subarray(0, prefix!.length));
      checks.reopenedRawAndModelProjection = true;
      expect(failures).toEqual([]);
      passed = true;
    } catch (error) {
      testFailure = String(error).slice(0, 2048);
      throw error;
    } finally {
      for (const client of clients.reverse()) {
        await client.abort().catch(() => undefined);
        await client.stop().catch(() => undefined);
      }
      for (const socket of sockets) socket.destroy();
      if (server.listening) await new Promise<void>(resolve => server.close(() => resolve()));
      try {
        if (evidenceDir) {
          fs.mkdirSync(evidenceDir, { recursive: true, mode: 0o700 });
          const artifact = (file: string) => ({ file, sha256: sha256(fs.readFileSync(file)) });
          fs.writeFileSync(path.join(evidenceDir, `context-overflow-${repairMode}-${giantBytes}.receipt.json`), JSON.stringify({
            version: 1, repairMode, passed, checks, testFailure, manualPromptsBeforeReopen, overflowIndex,
            inference: "private 127.0.0.1 Anthropic Messages fixture; zero external inference", requests, events,
            raw: { textBytes: giantBytes, textSha256: giantHash,
              serializedContentBytes: originalGiant ? serializedContentBytes() : null,
              serializedToolBytes: originalGiant ? serializedToolBytes() : null, sourceEntryId: originalGiant?.id,
              smallEntryId: originalSmall?.id, pre400Bytes: prefix?.length, pre400Sha256: prefix ? sha256(prefix) : null,
              afterRecoveryBytes: afterRecovery?.length, afterRecoveryPrefixSha256: prefix && afterRecovery
                ? sha256(afterRecovery.subarray(0, prefix.length)) : null, reopenedBytes, reopenedPrefixSha256: reopenedPrefixHash,
              stub, retainedScratch: false },
            artifacts: [fabricEntry, cli, candidateIndex, fixture].map(artifact),
            // No stdout/stderr, request bodies, raw sessions, environment, API-key headers or auth files are exported.
            loopbackFailures: failures,
          }, null, 2), { mode: 0o600 });
        }
      } finally {
        fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
      }
    }
  }, 180_000);
});
