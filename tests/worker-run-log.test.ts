import "./fixtures/conversation-host.js";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { NativeConversationReader } from "../src/ui/conversation-native-reader.js";
import { AgentTranscriptReader } from "../src/ui/transcript-reader.js";
import { createRunLogWriter } from "../src/worker/run-log.js";

const directories: string[] = [];
afterEach(() => {
  vi.useRealTimers();
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true });
});

const usage = (output: number) => ({ input: 1, output, cacheRead: 0, cacheWrite: 0, totalTokens: 1 + output, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } });
const assistant = (content: unknown[], stopReason: string, timestamp: number) => ({
  role: "assistant", content, api: "anthropic-messages", provider: "anthropic", model: "m", usage: usage(9), stopReason, timestamp,
});
const firstAssistant = assistant([{ type: "thinking", thinking: "hmm ok" }, { type: "text", text: "well" }, { type: "toolCall", id: "call_1", name: "bash", arguments: { command: "ls" } }], "toolUse", 12);
const toolResult = { role: "toolResult", toolCallId: "call_1", toolName: "bash", content: [{ type: "text", text: "a\nb\nc" }], isError: false, timestamp: 13 };

// A Pi RPC run: user prompt, a streamed thinking + text + tool call, a
// streaming bash tool, the tool result message, then the next assistant turn.
const run: Array<Record<string, unknown>> = [
  { type: "agent_start" },
  { type: "message_start", message: { role: "user", content: "go", timestamp: 10 } },
  { type: "message_end", message: { role: "user", content: "go", timestamp: 10 } },
  { type: "message_start", message: assistant([], "pending", 11) },
  { type: "message_update", assistantMessageEvent: { type: "thinking_start", contentIndex: 0 } },
  ...["hm", "m ", "ok"].map((delta, index) => ({ type: "message_update", usage: usage(index), assistantMessageEvent: { type: "thinking_delta", contentIndex: 0, delta } })),
  { type: "message_update", assistantMessageEvent: { type: "thinking_end", contentIndex: 0, content: "hmm ok" } },
  { type: "message_update", assistantMessageEvent: { type: "text_start", contentIndex: 1 } },
  ...["we", "ll"].map((delta) => ({ type: "message_update", assistantMessageEvent: { type: "text_delta", contentIndex: 1, delta } })),
  { type: "message_update", assistantMessageEvent: { type: "toolcall_start", contentIndex: 2, id: "call_1", toolName: "bash" } },
  ...['{"com', 'mand":', '"ls"}'].map((delta) => ({ type: "message_update", assistantMessageEvent: { type: "toolcall_delta", contentIndex: 2, delta } })),
  { type: "message_update", assistantMessageEvent: { type: "toolcall_end", contentIndex: 2, toolCall: { type: "toolCall", id: "call_1", name: "bash", arguments: { command: "ls" } } } },
  { type: "message_end", message: firstAssistant },
  { type: "turn_end", turnIndex: 0, message: firstAssistant },
  { type: "tool_execution_start", toolCallId: "call_1", toolName: "bash", args: { command: "ls" } },
  ...["a", "a\nb", "a\nb\nc"].map((text) => ({ type: "tool_execution_update", toolCallId: "call_1", toolName: "bash", partialResult: { content: [{ type: "text", text }] } })),
  { type: "tool_execution_end", toolCallId: "call_1", toolName: "bash", result: { content: toolResult.content }, isError: false },
  { type: "message_start", message: toolResult },
  { type: "message_end", message: toolResult },
  { type: "message_start", message: assistant([], "pending", 14) },
  ...["do", "ne"].map((delta) => ({ type: "message_update", assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta } })),
  { type: "message_end", message: assistant([{ type: "text", text: "done" }], "stop", 15) },
  // An aborted turn, then the same prompt twice with one timestamp.
  { type: "message_start", message: assistant([], "pending", 16) },
  { type: "message_update", assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "cut" } },
  { type: "message_end", message: { ...assistant([{ type: "text", text: "cut" }], "aborted", 17), errorMessage: "Request was aborted" } },
  { type: "turn_end", turnIndex: 1, message: { ...assistant([{ type: "text", text: "cut" }], "aborted", 17), errorMessage: "Request was aborted" } },
  ...[0, 1].flatMap(() => [
    { type: "message_start", message: { role: "user", content: "again", timestamp: 18 } },
    { type: "message_end", message: { role: "user", content: "again", timestamp: 18 } },
  ]),
  { type: "agent_end" },
];

const write = (events: Array<Record<string, unknown>>, flushAfter = false): { lines: Array<Record<string, unknown>>; text: string } => {
  let text = "";
  const writer = createRunLogWriter((chunk) => { text += chunk; });
  for (const event of events) writer.event(JSON.stringify(event), event);
  if (flushAfter) writer.flush();
  return { text, lines: text.trim().split("\n").filter(Boolean).map((line) => JSON.parse(line) as Record<string, unknown>) };
};

const logFile = (text: string): string => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "run-log-"));
  directories.push(directory);
  const file = path.join(directory, "events.jsonl");
  fs.writeFileSync(file, text, "utf8");
  return file;
};
const readTranscript = (text: string) => new NativeConversationReader().read({ id: "run", status: "running", logFile: logFile(text) });
// Entry ids are derived from line positions, which compaction changes.
const readEntries = (text: string) => new AgentTranscriptReader()
  .read({ id: "run", status: "completed", logFile: logFile(text) })
  .entries.map(({ id: _id, ...entry }) => entry);

describe("worker run log", () => {
  it("merges deltas per content block, keeps the latest tool update, and drops repeated messages", () => {
    const { lines } = write(run, true);
    const deltas = lines.filter((line) => line.type === "message_update" && String((line.assistantMessageEvent as { type: string }).type).endsWith("_delta"));
    expect(deltas.map((line) => line.assistantMessageEvent)).toEqual([
      { type: "thinking_delta", contentIndex: 0, delta: "hmm ok" },
      { type: "text_delta", contentIndex: 1, delta: "well" },
      { type: "toolcall_delta", contentIndex: 2, delta: '{"command":"ls"}' },
      { type: "text_delta", contentIndex: 0, delta: "done" },
      { type: "text_delta", contentIndex: 0, delta: "cut" },
    ]);
    expect(deltas[0]?.usage).toEqual(usage(2));
    const updates = lines.filter((line) => line.type === "tool_execution_update");
    expect(updates).toHaveLength(1);
    expect(JSON.stringify(updates[0])).toContain("a\\nb\\nc");
    expect(lines.filter((line) => line.type === "message_start").map((line) => (line.message as { role: string }).role)).toEqual(["user", "assistant", "assistant", "assistant", "user", "user"]);
    // Every other event is kept, in order.
    expect(lines.filter((line) => line.type === "turn_end")).toEqual([{ type: "turn_end", turnIndex: 0 }, { type: "turn_end", turnIndex: 1 }]);
    const kept = (events: Array<Record<string, unknown>>) => events
      .filter((event) => !["message_update", "tool_execution_update", "turn_end"].includes(String(event.type)))
      .filter((event) => !(event.type === "message_start" && (event.message as { role: string }).role === "toolResult"))
      .map((event) => JSON.stringify(event));
    expect(kept(lines)).toEqual(kept(run));
  });

  it("gives the conversation reader and the transcript parser the same finished transcript", () => {
    const rawText = run.map((event) => `${JSON.stringify(event)}\n`).join("");
    const raw = readTranscript(rawText);
    const written = readTranscript(write(run, true).text);
    expect(written.messages).toEqual(raw.messages);
    expect(written.streaming.tools).toEqual(raw.streaming.tools);
    expect(written.messages.map((message) => message.role)).toEqual(["user", "assistant", "toolResult", "assistant", "assistant", "user"]);
    const entries = readEntries(write(run, true).text);
    expect(entries).toEqual(readEntries(rawText));
    expect(entries.filter((entry) => entry.kind === "user").map((entry) => entry.text)).toEqual(["go", "again", "again"]);
  });

  it("streams a live partial within the flush interval", () => {
    vi.useFakeTimers();
    let text = "";
    const writer = createRunLogWriter((chunk) => { text += chunk; }, 500);
    const live = run.slice(0, run.findIndex((event) => event.type === "message_update" && (event.assistantMessageEvent as { type: string }).type === "toolcall_start"));
    for (const event of live) writer.event(JSON.stringify(event), event);
    expect(readTranscript(text).streaming.partialAssistant?.content[1]).toMatchObject({ type: "text", text: "" });
    vi.advanceTimersByTime(500);
    const partial = readTranscript(text).streaming.partialAssistant;
    expect(partial?.content[0]).toMatchObject({ type: "thinking", thinking: "hmm ok" });
    expect(partial?.content[1]).toMatchObject({ type: "text", text: "well" });
  });

  it("writes worker records and unparsed lines after anything held", () => {
    let text = "";
    const writer = createRunLogWriter((chunk) => { text += chunk; });
    const delta = { type: "message_update", assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "x" } };
    writer.event(JSON.stringify(delta), delta);
    writer.raw('{"type":"worker_stderr","text":"e"}\n');
    writer.event("not json", undefined);
    expect(text).toBe(`${JSON.stringify(delta)}\n{"type":"worker_stderr","text":"e"}\nnot json\n`);
  });
});
