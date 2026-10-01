import "./fixtures/conversation-host.js";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { performance } from "node:perf_hooks";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { readJsonlPage } from "../src/log-tail.js";
import { NativeConversationReader } from "../src/ui/conversation-native-reader.js";
import { AgentTranscriptReader } from "../src/ui/transcript-reader.js";
import { compactTerminalRunLog, createRunLogWriter, MIN_ELIDED_TOOL_RESULT_BYTES, MAX_EVENT_LINE_CHARS, MAX_TERMINAL_LOG_BYTES, MAX_TERMINAL_LOG_RECORDS, MAX_TERMINAL_LOG_WORK_MS } from "../src/worker/run-log.js";
import { PiEventProjection } from "../src/worker/event-projection.js";
import { TranscriptAccumulator } from "../src/ui/transcript-parser.js";
import { fileIdentity, retainCompactionGeneration } from "./helpers/native-reader-file-generation.js";

// Legacy elision/safety fixtures must still exercise canonical eligibility,
// replacement and rehydration above the small-result retention threshold.
const largeResultText = (label: string): string => `${label}:${"x".repeat(9 * 1024)}`;

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

const write = (events: Array<Record<string, unknown>>, flushAfter = false, terminal = true): { lines: Array<Record<string, unknown>>; text: string } => {
  let text = "";
  const writer = createRunLogWriter((chunk) => { text += chunk; });
  for (const event of events) writer.event(JSON.stringify(event), event);
  if (flushAfter) writer.flush();
  if (terminal) text = compactText(text);
  return { text, lines: text.trim().split("\n").filter(Boolean).map((line) => JSON.parse(line) as Record<string, unknown>) };
};

const logFile = (text: string): string => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "run-log-"));
  directories.push(directory);
  const file = path.join(directory, "events.jsonl");
  fs.writeFileSync(file, text, "utf8");
  return file;
};
const compactText = (text: string): string => {
  const file = logFile(text);
  const outcome = compactTerminalRunLog(file, "completed");
  expect(outcome.error).toBeUndefined();
  return fs.readFileSync(file, "utf8");
};
// Model Windows sharing denial with a REAL external descriptor, then let the
// caller close it and require a native production replacement. No fake identity.
const denyHeldReplacement = (file: string, held: number, code?: "EPERM" | "EBUSY"): void => {
  const original = fs.readFileSync(file);
  const identity = fs.fstatSync(held, { bigint: true });
  const rename = code === undefined ? undefined : vi.spyOn(fs, "renameSync").mockImplementation((_from, to) => {
    expect(to).toBe(file);
    const current = fs.fstatSync(held, { bigint: true });
    expect(current.dev).toBe(identity.dev);
    expect(current.ino).toBe(identity.ino);
    throw Object.assign(new Error(`injected Windows ${code}: open destination`), { code });
  });
  // Count-bounded denial, without a 700ms real sleep or a raised work budget.
  const sleep = code === undefined ? undefined : vi.spyOn(Atomics, "wait").mockReturnValue("timed-out");
  try {
    const outcome = compactTerminalRunLog(file, "completed");
    expect(outcome).toMatchObject({ compacted: 0, beforeBytes: original.length, afterBytes: original.length });
    if (code !== undefined) {
      expect(outcome.error).toContain(code);
      expect(rename).toHaveBeenCalledTimes(8);
    } else {
      // Native Windows sharing denial may exhaust the real 500ms deadline.
      expect(outcome.error ?? outcome.compactionSkipped).toMatch(/EPERM|EBUSY|EACCES|MAX_TERMINAL_LOG_WORK_MS=.*full log retained/);
    }
  } finally { rename?.mockRestore(); sleep?.mockRestore(); }
  expect(fs.readFileSync(file).equals(original)).toBe(true);
  expect(fs.statSync(file, { bigint: true }).ino).toBe(identity.ino);
  expect(fs.readdirSync(path.dirname(file))).toEqual(["events.jsonl"]);
};

const readTranscript = (text: string) => new NativeConversationReader().read({ id: "run", status: "running", logFile: logFile(text) });
// Entry ids are derived from line positions, which compaction changes.
const readEntries = (text: string) => new AgentTranscriptReader()
  .read({ id: "run", status: "completed", logFile: logFile(text) })
  .entries.map(({ id: _id, ...entry }) => entry);

// Actual lexical projection and writer, with the worker's shared character cap
// applied before JSON.parse/runLog.event (newline > MAX_EVENT_LINE_CHARS).
const projectedWrite = (events: Array<Record<string, unknown>>) => {
  const projection = new PiEventProjection();
  const projected = projection.write(events.map((event) => `${JSON.stringify(event)}\n`).join("")) + projection.end();
  let text = "";
  const dropped: number[] = [];
  const writer = createRunLogWriter((chunk) => { text += chunk; });
  for (const line of projected.trimEnd().split("\n")) {
    if (line.length > MAX_EVENT_LINE_CHARS) { dropped.push(line.length); continue; }
    writer.event(line, JSON.parse(line) as Record<string, unknown>);
  }
  writer.flush();
  text = compactText(text);
  return { text, dropped, lines: text.trimEnd().split("\n").map((line) => JSON.parse(line) as Record<string, unknown>) };
};

const capEvents = (text: string, isError = false, extras = {}) => {
  const result = { content: [{ type: "text", text }], details: {}, ...extras };
  const end = { type: "tool_execution_end", toolCallId: "capcall", toolName: "cap", result, isError };
  const message = { role: "toolResult", toolCallId: "capcall", toolName: "cap", content: result.content,
    details: result.details, isError, timestamp: 1700000000000 };
  const events = [
    { type: "tool_execution_start", toolCallId: "capcall", toolName: "cap", args: {} }, end,
    { type: "message_start", message }, { type: "message_end", message },
    { type: "turn_end", toolResults: [message] }, { type: "agent_end", messages: [message] },
  ];
  return { events, end, message };
};

// Parent runs these after a fresh build. This is the actual worker and manager
// with an offline RPC emitter, not an elapsed-time source probe or a Pi/model run.
describe.skipIf(!fs.existsSync(path.resolve("dist/worker.js")))("actual worker terminal log bounds", () => {
  it.each(["ordinary", "near-deadline", "pathological"])("keeps original terminal result and the right log generation (%s)", async (scenario) => {
    const [{ AgentManager }, { DEFAULT_FABRIC_CONFIG }, { ProcessTransport }] = await Promise.all([
      import("../src/agents/manager.js"), import("../src/config.js"), import("../src/agents/transports/process-transport.js"),
    ]);
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), "terminal-worker-"));
    directories.push(directory);
    const workerPath = path.resolve("dist/worker.js");
    const binary = path.join(directory, "offline-rpc.mjs");
    const clockFile = path.join(directory, "clock.json");
    const receiptFile = path.join(directory, "before-terminal.json");
    const runRoot = path.join(directory, "runs");
    const timeoutMs = 8_000;
    fs.writeFileSync(binary, `const fixture = ${JSON.stringify({ clockFile, receiptFile, runRoot, scenario, byteBound: MAX_TERMINAL_LOG_BYTES })};\n` + String.raw`
import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
const emit = (event) => process.stdout.write(JSON.stringify(event) + "\n");
const model = { provider: "offline", id: "terminal-log" };
let thinkingLevel = "off";
let buffer = "";
let logFile;
process.stdin.setEncoding("utf8");
process.stdin.on("data", (text) => {
  buffer += text;
  while (buffer.includes("\n")) {
    const newline = buffer.indexOf("\n");
    const frame = JSON.parse(buffer.slice(0, newline));
    buffer = buffer.slice(newline + 1);
    const reply = (data) => emit({ type: "response", id: frame.id, command: frame.type, success: true, data });
    if (frame.type === "get_state") reply({ model, thinkingLevel, isStreaming: false, isCompacting: false });
    else if (frame.type === "set_model") reply(model);
    else if (frame.type === "set_thinking_level") { thinkingLevel = frame.level; reply(); }
    else if (frame.type === "prompt") {
      logFile = path.join(fixture.runRoot, process.env.PI_FABRIC_PARENT_RUN, "events.jsonl");
      emit({ type: "agent_start" });
      const pairs = fixture.scenario === "near-deadline" ? 400 : 1;
      for (let index = 0; index < pairs; index++) {
        const toolCallId = "real-" + index;
        const content = [{ type: "text", text: "x".repeat(16 * 1024) }];
        emit({ type: "tool_execution_start", toolCallId, toolName: "bash", args: {} });
        emit({ type: "tool_execution_end", toolCallId, toolName: "bash", result: { content, terminate: true }, isError: false });
        emit({ type: "message_end", message: { role: "toolResult", toolCallId, toolName: "bash", content, isError: false, timestamp: index } });
      }
      const finish = () => {
        emit({ type: "message_end", message: { role: "assistant", provider: model.provider, model: model.id,
          content: [{ type: "text", text: "original terminal result" }], stopReason: "stop", usage: { input: 1, output: 1 } } });
        emit({ type: "agent_end" });
        emit({ type: "agent_settled" });
      };
      const { finishAt } = JSON.parse(fs.readFileSync(fixture.clockFile, "utf8"));
      if (fixture.scenario === "ordinary") finish();
      else setTimeout(finish, Math.max(0, finishAt - Date.now()));
    }
  }
});
process.stdin.on("end", () => {
  // All worker-consumed events are drained before stdin closes. Seed an
  // exceptional raw log beyond the byte-work limit only in this scenario.
  if (fixture.scenario === "pathological") fs.truncateSync(logFile, fixture.byteBound + 1);
  const bytes = fs.readFileSync(logFile);
  fs.writeFileSync(fixture.receiptFile, JSON.stringify({ ino: fs.statSync(logFile).ino, bytes: bytes.length,
    sha256: createHash("sha256").update(bytes).digest("hex") }));
  process.exit(0);
});
`);
    const launch = ProcessTransport.prototype.launch;
    const spy = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async (request) => {
      // #monitor starts immediately AFTER launch returns, with timeoutMs + 1s
      // grace. Finish 0.5s before the earlier observed launch-based bound; do
      // not substitute worker.startedAt or a fresh compaction-start timeout.
      fs.writeFileSync(clockFile, JSON.stringify({ finishAt: Date.now() + timeoutMs - 500 }));
      return launch.call(new ProcessTransport(), request);
    });
    const manager = new AgentManager(directory, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs, retainRuns: true }, {
      workerPath, piBinary: binary, runRoot,
    });
    try {
      const result = await manager.run({ task: "offline terminal log regression", model: "offline/terminal-log", thinking: "off", transport: "process" });
      expect(result).toMatchObject({ status: "completed", exitCode: 0, text: "original terminal result" });
      const file = path.join(runRoot, result.id, "events.jsonl");
      const receipt = JSON.parse(fs.readFileSync(receiptFile, "utf8")) as { ino: number; bytes: number; sha256: string };
      const bytes = fs.readFileSync(file);
      if (scenario === "pathological" || result.compactionSkipped !== undefined) {
        // Real worker compaction is best-effort: scheduler/fsync delays can
        // exhaust its independent work clock even for an ordinary-size log.
        const bound = scenario === "pathological"
          ? `MAX_TERMINAL_LOG_BYTES=${MAX_TERMINAL_LOG_BYTES}`
          : `MAX_TERMINAL_LOG_WORK_MS=${MAX_TERMINAL_LOG_WORK_MS}`;
        expect(result.compactionSkipped).toBe(`Terminal run-log compaction skipped: ${bound} work bound exceeded; full log retained`);
        expect(manager.listForUi()[0]?.compactionSkipped).toBe(result.compactionSkipped);
        expect(fs.statSync(file).ino).toBe(receipt.ino);
        expect(bytes.length).toBe(receipt.bytes);
        expect(createHash("sha256").update(bytes).digest("hex")).toBe(receipt.sha256);
      } else {
        expect(result.compactionSkipped).toBeUndefined();
        expect(fs.statSync(file).ino).not.toBe(receipt.ino);
        expect(bytes.length).toBeLessThan(receipt.bytes);
        const ends = bytes.toString("utf8").trimEnd().split("\n").map((line) => JSON.parse(line)).filter((event) => event.type === "tool_execution_end");
        expect(ends).toHaveLength(scenario === "near-deadline" ? 400 : 1);
        expect(ends.every((event) => event.result.elided === true && event.resultMetadata.terminate === true)).toBe(true);
      }
    } finally { spy.mockRestore(); await manager.close(); }
  }, 25_000);
});

describe("worker run log", () => {
  beforeEach(() => {
    // These tests assert compaction semantics and native filesystem behavior,
    // not CI scheduling/fsync speed. Keep real I/O; deadline tests below
    // explicitly advance this clock. Real-worker tests above stay unmocked.
    vi.spyOn(performance, "now").mockReturnValue(0);
  });
  it("retains a small fabric_exec probe-1234 return verbatim after real terminal compaction", () => {
    const small = capEvents("probe-1234", false, { receipt: { exact: true } });
    (small.events[0]! as Record<string, unknown>).args = { code: 'return "probe-1234";' };
    for (const event of small.events) {
      if (event.toolName) event.toolName = "fabric_exec";
      const message = ("message" in event ? event.message : undefined) as Record<string, unknown> | undefined;
      if (message) message.toolName = "fabric_exec";
    }
    const large = capEvents("x".repeat(16 * 1024));
    const largeEvents = large.events.map((event) => JSON.parse(JSON.stringify(event).replaceAll("capcall", "largecall")) as Record<string, unknown>);
    const live = write([...small.events, ...largeEvents], true, false).text;
    const originalLine = live.split("\n").find((line) => line.includes('"tool_execution_end"') && line.includes("probe-1234"))!;
    const file = logFile(live);
    retainCompactionGeneration(file);
    const inode = fileIdentity(file);
    const outcome = compactTerminalRunLog(file, "completed");
    expect(outcome.error).toBeUndefined();
    expect(outcome.compactionSkipped).toBeUndefined();
    const compacted = fs.readFileSync(file, "utf8");
    expect(fileIdentity(file)).not.toBe(inode);
    expect(compacted.split("\n")).toContain(originalLine);
    expect(outcome.compacted).toBe(1);
    expect(readEntries(compacted)).toEqual(readEntries(live));
    expect(readTranscript(compacted).streaming).toEqual(readTranscript(live).streaming);
  });

  it("exports the small-result retention threshold as exactly 8KiB", () => {
    expect(MIN_ELIDED_TOOL_RESULT_BYTES).toBe(8 * 1024);
  });

  it.each([8191, 8192, 8193])("uses serialized UTF-8 result bytes at the 8KiB boundary (%s bytes)", (size) => {
    const fixture = capEvents("", true, { terminate: true, opaque: { values: [null, false, 7] } });
    const overhead = Buffer.byteLength(JSON.stringify(fixture.end.result));
    // Multibyte content ensures the boundary is bytes, not UTF-16 characters.
    const bodyBytes = size - overhead;
    const body = "界".repeat(Math.floor(bodyBytes / 3)) + "x".repeat(bodyBytes % 3);
    const { events, end } = capEvents(body, true, { terminate: true, opaque: { values: [null, false, 7] } });
    expect(Buffer.byteLength(JSON.stringify(end.result))).toBe(size);
    const live = write(events, true, false).text;
    const file = logFile(live);
    retainCompactionGeneration(file);
    const inode = fileIdentity(file);
    const outcome = compactTerminalRunLog(file, "completed");
    expect(outcome.error).toBeUndefined();
    expect(outcome.compactionSkipped).toBeUndefined();
    const compacted = fs.readFileSync(file, "utf8");
    const records = compacted.trimEnd().split("\n").map((line) => JSON.parse(line));
    if (size <= 8192) {
      expect(outcome.compacted).toBe(0);
      expect(compacted).toBe(live);
      expect(fileIdentity(file)).toBe(inode);
      expect(records[1]).toEqual(end);
    } else {
      expect(outcome.compacted).toBe(1);
      expect(records[1]).toEqual({ ...end, result: { elided: true, bytes: size }, resultMetadata: { terminate: true, opaque: { values: [null, false, 7] } } });
      expect(fileIdentity(file)).not.toBe(inode);
    }
    expect(readEntries(compacted)).toEqual(readEntries(live));
    expect(readEntries(compacted)[0]).toMatchObject({ status: "failed", result: end.result });
    expect(readTranscript(compacted).streaming).toEqual(readTranscript(live).streaming);
    expect(readTranscript(compacted).streaming.tools[0]?.result).toEqual({ content: end.result.content, details: end.result.details });
  });

  it("retries a single Windows EPERM replacement failure and commits the compacted log", () => {
    const text = write(capEvents(largeResultText("retry replacement payload")).events, true, false).text;
    const file = logFile(text);
    const rename = fs.renameSync;
    const spy = vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (spy.mock.calls.length === 1) throw Object.assign(new Error("injected EPERM replacement"), { code: "EPERM" });
      rename(from, to);
    });
    try {
      const outcome = compactTerminalRunLog(file, "completed");
      expect(outcome.error).toBeUndefined();
      expect(outcome.compactionSkipped).toBeUndefined();
      expect(outcome.compacted).toBe(1);
      expect(spy).toHaveBeenCalledTimes(2);
      expect(outcome.afterBytes).toBeLessThan(outcome.beforeBytes);
    } finally { spy.mockRestore(); }
    expect(fs.readFileSync(file, "utf8")).not.toBe(text);
    expect(fs.readdirSync(path.dirname(file))).toEqual(["events.jsonl"]);
  });

  it("closes every owned source/temp descriptor after fsync and before replacement", () => {
    const text = write(capEvents(largeResultText("closed source at rename")).events, true, false).text;
    const file = logFile(text);
    const open = fs.openSync;
    const close = fs.closeSync;
    const rename = fs.renameSync;
    const owned = new Set<number>();
    const synced: number[] = [];
    const fsync = fs.fsyncSync;
    const openSpy = vi.spyOn(fs, "openSync").mockImplementation((name, flags, mode) => {
      const fd = open(name, flags, mode);
      owned.add(fd);
      return fd;
    });
    const closeSpy = vi.spyOn(fs, "closeSync").mockImplementation((fd) => {
      close(fd);
      owned.delete(fd);
    });
    const syncSpy = vi.spyOn(fs, "fsyncSync").mockImplementation((fd) => { fsync(fd); synced.push(fd); });
    const renameSpy = vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      expect(synced).toHaveLength(2);
      expect(owned.size).toBe(0);
      rename(from, to);
    });
    try {
      expect(compactTerminalRunLog(file, "completed")).toMatchObject({ compacted: 1 });
      expect(renameSpy).toHaveBeenCalledTimes(1);
      expect(owned.size).toBe(0);
    } finally { openSpy.mockRestore(); closeSpy.mockRestore(); syncSpy.mockRestore(); renameSpy.mockRestore(); }
  });

  it.each(["EPERM", "EBUSY", "EACCES", "EIO"])("retains byte-identical full log with a warning after persistent %s replacement failure", (code) => {
    const text = write(capEvents(largeResultText("persistent replacement payload")).events, true, false).text;
    const file = logFile(text);
    const original = fs.readFileSync(file);
    const inode = fs.statSync(file).ino;
    const clock = vi.spyOn(performance, "now").mockReturnValue(0);
    const sleep = vi.spyOn(Atomics, "wait").mockReturnValue("timed-out");
    const rename = vi.spyOn(fs, "renameSync").mockImplementation(() => {
      throw Object.assign(new Error(`injected ${code} replacement`), { code });
    });
    const unlink = vi.spyOn(fs, "unlinkSync");
    try {
      const outcome = compactTerminalRunLog(file, "completed");
      expect(outcome).toMatchObject({ compacted: 0, beforeBytes: original.length, afterBytes: original.length,
        error: expect.stringContaining(code) });
      expect(rename).toHaveBeenCalledTimes(code === "EIO" ? 1 : 8);
      expect(sleep.mock.calls.map((call) => call[3])).toEqual(code === "EIO" ? [] : [25, 50, 75, 100, 125, 150, 175]);
      expect(unlink.mock.calls.every(([name]) => name !== file)).toBe(true);
    } finally { rename.mockRestore(); sleep.mockRestore(); clock.mockRestore(); unlink.mockRestore(); }
    expect(fs.readFileSync(file).equals(original)).toBe(true);
    expect(fs.statSync(file).ino).toBe(inode);
    expect(fs.readdirSync(path.dirname(file))).toEqual(["events.jsonl"]);
  });

  it.each(["identity", "size", "mtime"])("rechecks source %s before retrying replacement", (change) => {
    const text = write(capEvents(largeResultText("changed during retry")).events, true, false).text;
    const file = logFile(text);
    const replace = fs.renameSync;
    const original = fs.statSync(file);
    const rename = vi.spyOn(fs, "renameSync").mockImplementation(() => {
      if (change === "size") fs.appendFileSync(file, "external append\n");
      if (change === "mtime") fs.utimesSync(file, original.atime, new Date(original.mtimeMs + 1000));
      if (change === "identity") {
        const replacement = `${file}.external`;
        fs.writeFileSync(replacement, text);
        replace(replacement, file);
      }
      throw Object.assign(new Error("injected EPERM replacement"), { code: "EPERM" });
    });
    try {
      expect(compactTerminalRunLog(file, "completed")).toMatchObject({ compacted: 0,
        error: expect.stringContaining("changed during terminal compaction") });
      expect(rename).toHaveBeenCalledTimes(1);
    } finally { rename.mockRestore(); }
    expect(fs.readFileSync(file, "utf8")).toBe(change === "size" ? `${text}external append\n` : text);
    expect(fs.readdirSync(path.dirname(file))).toEqual(["events.jsonl"]);
  });

  it("keeps the terminal work deadline while backing off persistent replacement contention", () => {
    const text = write(capEvents(largeResultText("bounded replacement wait")).events, true, false).text;
    const file = logFile(text);
    let elapsed = 0;
    const clock = vi.spyOn(performance, "now").mockImplementation(() => elapsed);
    const sleep = vi.spyOn(Atomics, "wait").mockImplementation((_array, _index, _value, ms) => {
      elapsed += ms ?? 0;
      return "timed-out";
    });
    const rename = vi.spyOn(fs, "renameSync").mockImplementation(() => {
      throw Object.assign(new Error("injected EPERM replacement"), { code: "EPERM" });
    });
    try {
      const outcome = compactTerminalRunLog(file, "completed");
      expect(outcome).toMatchObject({ compacted: 0, beforeBytes: Buffer.byteLength(text), afterBytes: Buffer.byteLength(text),
        compactionSkipped: expect.stringContaining("MAX_TERMINAL_LOG_WORK_MS") });
      expect(rename).toHaveBeenCalledTimes(6);
      expect(elapsed).toBe(MAX_TERMINAL_LOG_WORK_MS);
    } finally { rename.mockRestore(); sleep.mockRestore(); clock.mockRestore(); }
    expect(fs.readFileSync(file, "utf8")).toBe(text);
    expect(fs.readdirSync(path.dirname(file))).toEqual(["events.jsonl"]);
  });

  it.each(["source", "temp"])("retains full log when real %s fsync exhausts the work clock", (phase) => {
    const text = write(capEvents(largeResultText("slow fsync fallback"), false, { terminate: true }).events, true, false).text;
    const file = logFile(text);
    const inode = fileIdentity(file);
    let elapsed = 0;
    let syncs = 0;
    const clock = vi.spyOn(performance, "now").mockImplementation(() => elapsed);
    const fsync = fs.fsyncSync;
    const sync = vi.spyOn(fs, "fsyncSync").mockImplementation((fd) => {
      fsync(fd);
      if (++syncs === (phase === "source" ? 1 : 2)) elapsed = MAX_TERMINAL_LOG_WORK_MS;
    });
    try {
      const outcome = compactTerminalRunLog(file, "completed");
      expect(outcome).toEqual({ compacted: 0, beforeBytes: Buffer.byteLength(text), afterBytes: Buffer.byteLength(text),
        compactionSkipped: `Terminal run-log compaction skipped: MAX_TERMINAL_LOG_WORK_MS=${MAX_TERMINAL_LOG_WORK_MS} work bound exceeded; full log retained` });
      expect(sync).toHaveBeenCalledTimes(phase === "source" ? 1 : 2);
    } finally { sync.mockRestore(); clock.mockRestore(); }
    expect(fileIdentity(file)).toBe(inode);
    expect(fs.readFileSync(file, "utf8")).toBe(text);
    expect(fs.readdirSync(path.dirname(file))).toEqual(["events.jsonl"]);
  });

  it("uses a write-capable source for Windows-style FlushFileBuffers and real fsync", () => {
    const { events, end, message } = capEvents(largeResultText("durable paired result"), true, { terminate: true });
    const text = write(events, true, false).text;
    const file = logFile(text);
    const original = fs.statSync(file);
    const open = fs.openSync;
    const fsync = fs.fsyncSync;
    let source: number | undefined;
    let sourceFlags = -1;
    const synced: number[] = [];
    const openSpy = vi.spyOn(fs, "openSync").mockImplementation((name, flags, mode) => {
      const fd = open(name, flags, mode);
      if (name === file) { source = fd; sourceFlags = Number(flags); }
      return fd;
    });
    const syncSpy = vi.spyOn(fs, "fsyncSync").mockImplementation((fd) => {
      // Capability repro only, not a native Windows claim. All permitted
      // flushes execute the platform's actual fsync; no failure is swallowed.
      if (fd === source && (sourceFlags & (fs.constants.O_RDWR | fs.constants.O_WRONLY)) === 0) {
        throw Object.assign(new Error("FlushFileBuffers requires GENERIC_WRITE"), { code: "EBADF" });
      }
      fsync(fd);
      synced.push(fd);
    });
    const writes = vi.spyOn(fs, "writeSync");
    try {
      const outcome = compactTerminalRunLog(file, "completed");
      expect(outcome.error).toBeUndefined();
      expect(outcome.compactionSkipped).toBeUndefined();
      expect(outcome.compacted).toBe(1);
      expect(sourceFlags & fs.constants.O_RDWR).toBe(fs.constants.O_RDWR);
      expect(sourceFlags & (fs.constants.O_CREAT | fs.constants.O_TRUNC | fs.constants.O_APPEND)).toBe(0);
      const noFollow = fs.constants.O_NOFOLLOW ?? 0;
      expect(sourceFlags & noFollow).toBe(noFollow);
      expect(synced).toHaveLength(2);
      expect(synced[0]).toBe(source);
      expect(writes.mock.calls.every(([fd]) => fd !== source)).toBe(true);
    } finally { openSpy.mockRestore(); syncSpy.mockRestore(); writes.mockRestore(); }
    const compacted = fs.readFileSync(file, "utf8");
    const records = compacted.trimEnd().split("\n").map((line) => JSON.parse(line));
    expect(records.filter((event) => event.type === "tool_execution_end")).toEqual([
      { ...end, result: { elided: true, bytes: Buffer.byteLength(JSON.stringify(end.result)) }, resultMetadata: { terminate: true } },
    ]);
    expect(records.filter((event) => event.type === "message_end")).toEqual([{ type: "message_end", message }]);
    expect(fs.statSync(file).ino).not.toBe(original.ino);
    expect(fs.statSync(file).mode & 0o777).toBe(original.mode & 0o777);
    expect(readEntries(compacted)).toEqual(readEntries(text));
    expect(readTranscript(compacted).streaming).toEqual(readTranscript(text).streaming);
  });

  it("retains full bytes and inode when source write access is denied", () => {
    const text = write(capEvents(largeResultText("read-only source")).events, true, false).text;
    const file = logFile(text);
    const inode = fs.statSync(file).ino;
    const open = fs.openSync;
    const spy = vi.spyOn(fs, "openSync").mockImplementation((name, flags, mode) => {
      if (name === file && (Number(flags) & fs.constants.O_RDWR) !== 0) {
        throw Object.assign(new Error("EACCES: source write access denied"), { code: "EACCES" });
      }
      return open(name, flags, mode);
    });
    try {
      expect(compactTerminalRunLog(file, "completed")).toMatchObject({ compacted: 0, error: expect.stringContaining("EACCES") });
    } finally { spy.mockRestore(); }
    expect(fs.readFileSync(file, "utf8")).toBe(text);
    expect(fs.statSync(file).ino).toBe(inode);
    expect(fs.readdirSync(path.dirname(file))).toEqual(["events.jsonl"]);
  });
  it.each(["execution", "session", "claude", "after-canonical"])("retains missing-first-start ends across a later reused-ID start (%s)", (kind) => {
    const content = [{ type: "text", text: largeResultText("same output") }];
    const details = { exitCode: 0 };
    const end = { type: "tool_execution_end", toolCallId: "reused", toolName: "bash", result: { content, details, terminate: true }, isError: false };
    const start = kind === "session"
      ? { type: "message", message: { role: "assistant", content: [{ type: "toolCall", id: "reused", name: "bash", arguments: {} }] } }
      : kind === "claude"
        ? { type: "assistant", message: { role: "assistant", content: [{ type: "tool_use", id: "reused", name: "bash", input: {} }] } }
        : { type: "tool_execution_start", toolCallId: "reused", toolName: "bash", args: { command: "second invocation" } };
    const canonical = { type: "message_end", message: { role: "toolResult", toolCallId: "reused", toolName: "bash", content, details, isError: false, timestamp: 2 } };
    const events = kind === "after-canonical" ? [end, canonical, start] : [end, start, canonical];
    const text = events.map((event) => `${JSON.stringify(event)}\n`).join("");
    const before = new TranscriptAccumulator();
    before.append(events);
    if (kind === "execution") {
      expect(before.entries.filter((entry) => entry.kind === "tool")).toHaveLength(2);
      expect(before.entries[0]).toMatchObject({ result: { terminate: true } });
    }
    const file = logFile(text);
    const inode = fs.statSync(file).ino;
    expect(compactTerminalRunLog(file, "completed")).toMatchObject({ compacted: 0, beforeBytes: Buffer.byteLength(text), afterBytes: Buffer.byteLength(text) });
    expect(fs.statSync(file).ino).toBe(inode);
    expect(fs.readFileSync(file, "utf8")).toBe(text);
    const after = new TranscriptAccumulator();
    after.append(fs.readFileSync(file, "utf8").trimEnd().split("\n").map((line) => JSON.parse(line)));
    expect(after.entries).toEqual(before.entries);
  });

  it("still compacts a unique same-lifecycle pair with no start", () => {
    const { end, message } = capEvents(largeResultText("allowed missing start"), false, { terminate: true });
    const text = [end, { type: "message_end", message }].map((event) => `${JSON.stringify(event)}\n`).join("");
    const file = logFile(text);
    expect(compactTerminalRunLog(file, "completed").compacted).toBe(1);
    expect(readEntries(fs.readFileSync(file, "utf8"))).toEqual(readEntries(text));
  });

  it("bounds 400 large paired results to two scans plus exact-offset canonical reads", () => {
    const text = Array.from({ length: 400 }, (_, index) => {
      const toolCallId = `linear-${index}`;
      const content = [{ type: "text", text: "x".repeat(16 * 1024) }];
      return [
        { type: "tool_execution_start", toolCallId, toolName: "bash", args: {} },
        { type: "tool_execution_end", toolCallId, toolName: "bash", result: { content }, isError: false },
        { type: "message_end", message: { role: "toolResult", toolCallId, toolName: "bash", content, isError: false, timestamp: index } },
      ].map((event) => `${JSON.stringify(event)}\n`).join("");
    }).join("");
    const file = logFile(text);
    const spy = vi.spyOn(fs, "readSync");
    const parse = vi.spyOn(JSON, "parse");
    // Work complexity is structural; wall time is tested separately below.
    const clock = vi.spyOn(performance, "now").mockReturnValue(0);
    try {
      const outcome = compactTerminalRunLog(file, "completed");
      expect(outcome.compacted).toBe(400);
      expect(outcome.compactionSkipped).toBeUndefined();
      expect(outcome.error).toBeUndefined();
      const bytesRead = spy.mock.results.reduce((sum, result) => sum + Number(result.value), 0);
      const positions = spy.mock.calls.map((args) => Number((args as unknown[])[4]));
      expect(bytesRead).toBeLessThanOrEqual(3 * Buffer.byteLength(text));
      expect(parse.mock.calls.length).toBeLessThanOrEqual(3 * 1200);
      expect(positions.filter((position) => position === 0)).toHaveLength(2);
      expect(positions.length).toBeLessThanOrEqual(2 * Math.ceil(Buffer.byteLength(text) / (64 * 1024)) + 400);
    } finally { spy.mockRestore(); parse.mockRestore(); clock.mockRestore(); }
  });

  it.each(["bytes", "records", "elapsed"])("retains full bytes/inode and removes partial temp at the named %s work bound", (bound) => {
    const text = write(capEvents(largeResultText("bounded fallback"), false, { terminate: true }).events, true, false).text;
    const file = logFile(text);
    if (bound === "bytes") fs.truncateSync(file, MAX_TERMINAL_LOG_BYTES + 1);
    if (bound === "records") fs.appendFileSync(file, "{}\n".repeat(MAX_TERMINAL_LOG_RECORDS));
    const original = fs.readFileSync(file);
    const inode = fs.statSync(file).ino;
    const writeSpy = vi.spyOn(fs, "writeSync");
    const clock = vi.spyOn(performance, "now").mockImplementation(() => bound === "elapsed" && writeSpy.mock.calls.length > 0 ? MAX_TERMINAL_LOG_WORK_MS : 0);
    try {
      const outcome = compactTerminalRunLog(file, "completed");
      expect(outcome).toMatchObject({ compacted: 0, beforeBytes: original.length, afterBytes: original.length,
        compactionSkipped: expect.stringContaining(bound === "bytes" ? "MAX_TERMINAL_LOG_BYTES" : bound === "records" ? "MAX_TERMINAL_LOG_RECORDS" : "MAX_TERMINAL_LOG_WORK_MS") });
      expect(outcome.error).toBeUndefined();
      if (bound === "elapsed") expect(writeSpy).toHaveBeenCalled();
    } finally { writeSpy.mockRestore(); clock.mockRestore(); }
    expect(fs.statSync(file).ino).toBe(inode);
    // Avoid deep-equality enumeration of every byte in the 64 MiB bound fixture.
    expect(fs.readFileSync(file).equals(original)).toBe(true);
    expect(fs.readdirSync(path.dirname(file))).toEqual(["events.jsonl"]);
  });
  it("keeps the full live end through crash/abort before canonical and refuses nonterminal compaction", () => {
    const { events, end } = capEvents(largeResultText("durable crash result"), false, { terminate: true });
    const live = write(events.slice(0, 2), true, false);
    expect(live.lines[1]).toEqual(end);
    const file = logFile(live.text);
    const inode = fs.statSync(file).ino;
    expect(compactTerminalRunLog(file, "running").compacted).toBe(0);
    expect(compactTerminalRunLog(file, "completed").compacted).toBe(0);
    expect(fs.statSync(file).ino).toBe(inode);
    expect(fs.readFileSync(file, "utf8")).toBe(live.text);
    expect(readEntries(live.text)[0]).toMatchObject({ status: "completed", result: end.result });
    expect(readTranscript(live.text).streaming.tools[0]?.result).toEqual({ content: end.result.content, details: end.result.details });
    const paired = write(events, true, false);
    expect(paired.lines[1]).toEqual(end); // Even after canonical, active log stays full.
    expect(compactTerminalRunLog(logFile(paired.text), "running").compacted).toBe(0);
  });

  it.each(["content", "details", "isError", "toolName", "partial", "duplicate", "reused"])("preserves full ends with unavailable/different/ambiguous canonical (%s)", (difference) => {
    const { events, end } = capEvents(largeResultText("main-kept result"), false, { opaque: { elided: true }, terminate: true });
    const message = { ...((events[3]! as Record<string, unknown>).message as Record<string, unknown>) };
    if (difference === "content") message.content = [{ type: "text", text: "different" }];
    if (difference === "details") message.details = { changed: true };
    if (difference === "isError") message.isError = true;
    if (difference === "toolName") message.toolName = "different";
    const records = [events[0]!, end, { type: "message_end", message }];
    if (difference === "duplicate") records.push({ type: "message_end", message });
    if (difference === "reused") records.push(events[0]!);
    let text = write(records, true, false).text;
    if (difference === "partial") text = text.trimEnd();
    const file = logFile(text);
    const inode = fs.statSync(file).ino;
    expect(compactTerminalRunLog(file, "failed")).toMatchObject({ compacted: 0, beforeBytes: Buffer.byteLength(text), afterBytes: Buffer.byteLength(text) });
    expect(fs.readFileSync(file, "utf8")).toBe(text);
    expect(fs.statSync(file).ino).toBe(inode);
  });

  it("preserves opaque legacy marker fields during paired compaction", () => {
    const content = [{ type: "text", text: largeResultText("legacy full result") }];
    const result = { content, details: { a: 1, b: 2 }, elided: true, bytes: 17, terminate: true };
    const events = [
      { type: "tool_execution_start", toolCallId: "legacy", toolName: "tool", args: {} },
      { type: "tool_execution_end", toolCallId: "legacy", toolName: "tool", result, isError: false },
      { type: "message_end", message: { role: "toolResult", toolCallId: "legacy", toolName: "tool", content, details: { a: 1, b: 2 }, isError: false, timestamp: 1 } },
    ];
    const full = write(events, true, false).text;
    const compacted = compactText(full);
    expect(JSON.parse(compacted.split("\n")[1]!).resultMetadata).toEqual({ elided: true, bytes: 17, terminate: true });
    expect(readEntries(compacted)).toEqual(readEntries(full));
    expect(readTranscript(compacted).streaming).toEqual(readTranscript(full).streaming);
  });

  it("keeps differently ordered equal details that would change bounded dashboard output", () => {
    const details = Object.fromEntries(Array.from({ length: 450 }, (_, index) => [`k${index}`, index]));
    const content = [{ type: "text", text: "budgeted details" }];
    const events = [
      { type: "tool_execution_start", toolCallId: "order", toolName: "tool", args: {} },
      // Add size after details so the original ordered-details dashboard
      // budget counterexample is not hidden by a newly enlarged content body.
      { type: "tool_execution_end", toolCallId: "order", toolName: "tool", result: { content, details, padding: "x".repeat(9 * 1024) }, isError: false },
      { type: "message_end", message: { role: "toolResult", toolCallId: "order", toolName: "tool", content, details: Object.fromEntries(Object.entries(details).reverse()), isError: false, timestamp: 1 } },
    ];
    const full = write(events, true, false).text;
    expect(compactText(full)).toBe(full);
    expect(readEntries(compactText(full))).toEqual(readEntries(full));
  });

  it.each(["source-fsync", "temp-fsync", "rename"])("preserves original and cleans only owned temp when %s fails", (failure) => {
    const text = write(capEvents(largeResultText("durability payload")).events, true, false).text;
    const file = logFile(text);
    const directory = path.dirname(file);
    const foreign = path.join(directory, "foreign.compact.tmp");
    fs.writeFileSync(foreign, "not ours");
    const inode = fs.statSync(file).ino;
    let syncs = 0;
    const fsync = fs.fsyncSync;
    const spy = failure === "rename"
      ? vi.spyOn(fs, "renameSync").mockImplementation(() => { throw new Error("injected rename failure"); })
      : vi.spyOn(fs, "fsyncSync").mockImplementation((fd) => {
        if (++syncs === (failure === "source-fsync" ? 1 : 2)) throw Object.assign(new Error("injected EIO fsync failure"), { code: "EIO" });
        fsync(fd);
      });
    try {
      expect(compactTerminalRunLog(file, "completed")).toMatchObject({ compacted: 0, error: expect.stringContaining("injected") });
    } finally { spy.mockRestore(); }
    expect(fs.readFileSync(file, "utf8")).toBe(text);
    expect(fs.statSync(file).ino).toBe(inode);
    expect(fs.readdirSync(directory).sort()).toEqual(["events.jsonl", "foreign.compact.tmp"]);
  });

  it("invalidates growing replacements when Windows file IDs round to the same number", () => {
    const live = write(capEvents("").events, true, false).text;
    const file = logFile(live);
    retainCompactionGeneration(file);
    const originalInode = fs.statSync(file, { bigint: true }).ino;
    const windowsId = 2n ** 54n;
    expect(Number(windowsId + 1n)).toBe(Number(windowsId));
    const fstat = fs.fstatSync;
    // Keep real descriptors, sizes and replacement I/O; model only Windows' wide IDs.
    const stat = vi.spyOn(fs, "fstatSync").mockImplementation((descriptor, options) => {
      const actual = fstat(descriptor, options);
      const id = fstat(descriptor, { bigint: true }).ino === originalInode ? windowsId : windowsId + 1n;
      Object.defineProperty(actual, "ino", { value: options?.bigint ? id : Number(id) });
      return actual;
    });
    try {
      const source = { id: "wide-id", status: "completed", logFile: file };
      const reader = new AgentTranscriptReader();
      const before = reader.read(source, false);
      const replacement = `${file}.replacement`;
      fs.writeFileSync(replacement, `${live}${JSON.stringify({ type: "agent_end", padding: "x".repeat(256) })}\n`);
      fs.renameSync(replacement, file);
      expect(reader.read(source, false).entries).toEqual(before.entries);
      fs.appendFileSync(file, `${JSON.stringify({ type: "message_end", message: { role: "user", content: "new-path-offset", timestamp: 99 } })}\n`);
      expect(reader.read(source).entries.at(-1)).toMatchObject({ kind: "user", text: "new-path-offset" });
    } finally { stat.mockRestore(); }
  });

  describe.each(["quiescent", "Windows EPERM denial", "Windows EBUSY denial", "POSIX held-FD replacement", "Windows native denial"])("reader replacement: %s", (capability) => {
    describe.skipIf((capability.startsWith("POSIX") && process.platform === "win32") || (capability === "Windows native denial" && process.platform !== "win32"))("native replacement capability", () => {
      it.each(["x".repeat(9 * 1024), ""])("invalidates held reader offsets on atomic replacement (body length=%s)", (body) => {
        const { events: fullEvents } = capEvents(body);
        const events: Array<Record<string, unknown>> = body ? fullEvents : [
          fullEvents[0]!,
          // Empty canonical content still exercises the old growing-replacement
          // case: opaque result metadata alone puts this result above 8KiB.
          { type: "tool_execution_end", toolCallId: "capcall", toolName: "cap", result: { content: [], padding: "x".repeat(9 * 1024) }, isError: false },
          { type: "message_end", message: { role: "toolResult", toolCallId: "capcall", toolName: "cap", content: [], isError: false, timestamp: 1 } },
        ];
        const raw = events.map((event) => `${JSON.stringify(event)}\n`).join("");
        const live = write(events, true, false).text;
        const file = logFile(live);
        const source = { id: "held", status: "completed", logFile: file };
        const dashboard = new AgentTranscriptReader();
        const native = new NativeConversationReader();
        const beforeDashboard = dashboard.read(source, false);
        const beforeNative = native.read(source, false);
        const cursor = readJsonlPage(file, 2);
        let fd = capability === "quiescent" ? undefined : fs.openSync(file, "r");
        // Windows file IDs exceed 2^53; a number `ino` can round two different files to one value.
        const inode = fs.statSync(file, { bigint: true }).ino;
        try {
          if (capability.startsWith("Windows")) {
            denyHeldReplacement(file, fd!, capability === "Windows native denial" ? undefined : capability.includes("EPERM") ? "EPERM" : "EBUSY");
            expect(native.read(source, false).streaming).toEqual(beforeNative.streaming);
            expect(dashboard.read(source, false).entries).toEqual(beforeDashboard.entries);
            expect(readJsonlPage(file, 2, cursor.before, undefined, cursor.generation).generation).toBe(cursor.generation);
            fs.closeSync(fd!);
            fd = undefined;
          }
          const outcome = compactTerminalRunLog(file, "completed");
          expect(outcome.error).toBeUndefined();
          expect(outcome.compactionSkipped).toBeUndefined();
          expect(outcome.compacted).toBe(1);
          expect(fs.statSync(file, { bigint: true }).ino).not.toBe(inode);
          expect(() => readJsonlPage(file, 2, cursor.before, undefined, cursor.generation)).toThrowError(expect.objectContaining({ name: "cursor-stale" }));
          if (fd !== undefined) expect(fs.readFileSync(fd, "utf8")).toBe(live);
          const compacted = fs.readFileSync(file, "utf8");
          expect(compacted.trimEnd().split("\n")).toHaveLength(live.trimEnd().split("\n").length);
          expect(readEntries(compacted)).toEqual(readEntries(raw));
          const fresh = readTranscript(compacted);
          expect(fresh.messages).toEqual(readTranscript(raw).messages);
          expect(fresh.streaming).toEqual(readTranscript(raw).streaming);
          const afterNative = native.read(source, false);
          expect(afterNative.revision).toBeGreaterThan(beforeNative.revision);
          expect(afterNative.hasNewer).toBe(false);
          expect(afterNative.messages).toEqual(beforeNative.messages);
          expect(afterNative.streaming).toEqual(beforeNative.streaming);
          expect(dashboard.read(source, false).entries).toEqual(beforeDashboard.entries);
          if (body) expect(outcome.afterBytes).toBeLessThan(outcome.beforeBytes);
          else expect(outcome.afterBytes).toBeGreaterThan(outcome.beforeBytes);
          fs.appendFileSync(file, `${JSON.stringify({ type: "message_end", message: { role: "user", content: "new-path-offset", timestamp: 99 } })}\n`);
          expect(native.read(source).messages.at(-1)).toMatchObject({ role: "user", content: "new-path-offset" });
          expect(dashboard.read(source).entries.at(-1)).toMatchObject({ kind: "user", text: "new-path-offset" });
          expect(beforeNative.messages).not.toEqual(native.last!.messages);
        } finally { if (fd !== undefined) fs.closeSync(fd); }
      });

      it.each([false, true])("retains loaded older native pages after large terminal replacement (follow=%s)", (follow) => {
        const directory = fs.mkdtempSync(path.resolve(".native-replacement-"));
        directories.push(directory);
        const file = path.join(directory, "events.jsonl");
        const events = Array.from({ length: 160 }, (_, index) => {
          const content = [{ type: "text", text: `body-${index}:${"x".repeat(9 * 1024)}` }];
          const toolCallId = `call-${index}`;
          return [
            { type: "tool_execution_start", toolCallId, toolName: "bash", args: {} },
            { type: "tool_execution_end", toolCallId, toolName: "bash", result: { content }, isError: false },
            { type: "message_end", message: { role: "toolResult", toolCallId, toolName: "bash", content, isError: false, timestamp: index + 1 } },
          ];
        }).flat();
        const text = events.map((event) => `${JSON.stringify(event)}\n`).join("");
        fs.writeFileSync(file, text);
        const source = { id: "loaded", status: "completed", eventsFile: file };
        const reader = new NativeConversationReader();
        const initial = reader.read(source, follow);
        const before = reader.loadOlder(2)!;
        expect(before.messages.length).toBeGreaterThan(initial.messages.length);
        expect(before.hasMore).toBe(true);
        const inode = fs.statSync(file, { bigint: true }).ino;
        const cursor = readJsonlPage(file, 2);
        let descriptor = capability === "quiescent" ? undefined : fs.openSync(file, "r");
        try {
          if (capability.startsWith("Windows")) {
            denyHeldReplacement(file, descriptor!, capability === "Windows native denial" ? undefined : capability.includes("EPERM") ? "EPERM" : "EBUSY");
            expect(reader.read(source, follow).messages).toEqual(before.messages);
            expect(readJsonlPage(file, 2, cursor.before, undefined, cursor.generation).generation).toBe(cursor.generation);
            fs.closeSync(descriptor!);
            descriptor = undefined;
          }
          const outcome = compactTerminalRunLog(file, "completed");
          expect(outcome.error).toBeUndefined();
          expect(outcome.compactionSkipped).toBeUndefined();
          expect(outcome.compacted).toBe(160);
          expect(outcome.afterBytes).toBeGreaterThan(256 * 1024);
          expect(fs.statSync(file, { bigint: true }).ino).not.toBe(inode);
          expect(() => readJsonlPage(file, 2, cursor.before, undefined, cursor.generation)).toThrowError(expect.objectContaining({ name: "cursor-stale" }));
          if (descriptor !== undefined) expect(fs.readFileSync(descriptor, "utf8")).toBe(text);
          const compacted = fs.readFileSync(file, "utf8");
          const types = (value: string) => value.trimEnd().split("\n").map((line) => JSON.parse(line).type);
          expect(types(compacted)).toEqual(types(text));
          const after = reader.read(source, follow);
          expect(after.messages.length).toBeGreaterThanOrEqual(before.messages.length);
          expect(after.messages.slice(-before.messages.length)).toEqual(before.messages);
          expect(after.hasMore).toBe(true);
          expect(after.hasNewer).toBe(false);
          expect(after.revision).toBeGreaterThan(before.revision);
        } finally {
          if (descriptor !== undefined) fs.closeSync(descriptor);
          reader.clear();
        }
      });
    });
  });

  it.each([false, true])("retains the only accepted result when canonical envelopes exceed the unchanged cap (isError=%s)", (isError) => {
    // Success is the exact review sequence: end4194304 / canonical4194344.
    const { events, end } = capEvents("x".repeat(4194157 + Number(isError)), isError);
    const original = JSON.stringify(events);
    expect(JSON.stringify(end).length).toBe(4194304);
    expect(JSON.stringify(events[3]).length).toBe(4194344);
    const { text, lines, dropped } = projectedWrite(events);
    expect(dropped).toEqual([4194346, 4194344]);
    expect(lines[1]).toEqual(end);
    expect(lines.at(-2)?.toolResults).toEqual([]);
    expect(lines.at(-1)?.messages).toEqual([]);
    expect(lines.filter((line) => line.type === "message_end")).toHaveLength(0);
    expect(JSON.stringify(events)).toBe(original);
    // There is no canonical/history/prefix fallback: the full accepted end is
    // sufficient for the production dashboard accumulator and whole-record reader.
    const accumulator = new TranscriptAccumulator();
    accumulator.append(lines);
    expect(accumulator.entries).toEqual([expect.objectContaining({
      kind: "tool", status: isError ? "failed" : "completed", result: expect.any(Object),
    })]);
    const reader = new NativeConversationReader();
    reader.read({ id: "cap", status: "completed", logFile: logFile(text) });
    // The initial tail can contain only the small lifecycle records after a
    // near-cap result; walk older whole-record pages, as the UI does.
    const retained = reader.loadOlder(3)!;
    expect(retained.hasMore).toBe(false);
    expect(retained.streaming.tools[0]?.result).toEqual(end.result);
    expect(retained.streaming.tools[0]?.status).toBe(isError ? "failed" : "completed");
    const legacyReader = new NativeConversationReader();
    legacyReader.read({ id: "legacy", status: "completed", logFile: logFile(events.filter((event) => event.type.startsWith("tool_execution_")).map((event) => `${JSON.stringify(event)}\n`).join("")) });
    const legacy = legacyReader.loadOlder(3)!;
    expect(retained.streaming).toEqual(legacy.streaming);
  });

  it("keeps a conservative envelope reserve without changing the worker cap", () => {
    expect(MAX_EVENT_LINE_CHARS).toBe(4194304);
    for (const reserve of [128, 127]) {
      const { events, end } = capEvents("x".repeat(4194157 - reserve));
      expect(JSON.stringify(end).length).toBe(MAX_EVENT_LINE_CHARS - reserve);
      const { lines, dropped } = projectedWrite(events);
      expect(dropped).toEqual([]);
      if (reserve === 128) expect(lines[1]?.result).toEqual({ elided: true, bytes: Buffer.byteLength(JSON.stringify(end.result)) });
      else expect(lines[1]).toEqual(end); // Deliberately conservative even if canonical fits.
    }
    const { events } = capEvents("x".repeat(4194158));
    expect(projectedWrite(events).dropped).toEqual([4194305, 4194347, 4194345]);
  });

  it("uses character admission rather than UTF-8 bytes and keeps normal canonical bodies single", () => {
    const extras = { terminate: true, arbitrary: { values: [null, false, 7] } };
    const { events, end } = capEvents("界".repeat(1_400_000), false, extras);
    const original = JSON.stringify(events);
    const { text, lines, dropped } = projectedWrite(events);
    expect(JSON.stringify(end).length).toBeLessThan(MAX_EVENT_LINE_CHARS - 128);
    expect(Buffer.byteLength(JSON.stringify(end.result))).toBeGreaterThan(MAX_EVENT_LINE_CHARS);
    expect(dropped).toEqual([]);
    expect(lines[1]?.result).toEqual({ elided: true, bytes: Buffer.byteLength(JSON.stringify(end.result)) });
    expect(lines[1]?.resultMetadata).toEqual(extras);
    expect(lines.filter((line) => (line.message as { role?: string } | undefined)?.role === "toolResult")).toHaveLength(1);
    const body = end.result.content[0]!.text;
    expect(text.indexOf(body)).toBeGreaterThan(0);
    expect(text.indexOf(body)).toBe(text.lastIndexOf(body));
    expect(JSON.stringify(events)).toBe(original);
    const reader = new NativeConversationReader();
    reader.read({ id: "unicode", status: "completed", logFile: logFile(text) });
    expect(reader.loadOlder(3)!.streaming.tools[0]?.result).toEqual({ content: end.result.content, details: end.result.details });
  });

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
      .filter((event) => !["message_update", "tool_execution_update", "tool_execution_end", "turn_end"].includes(String(event.type)))
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

  it.each([false, true])("writes each final result once and replays both readers (isError=%s)", (isError) => {
    const content = [
      { type: "text", text: largeResultText("unique final result 🦄") },
      { type: "image", data: "aW1hZ2U=", mimeType: "image/png" },
    ];
    const details = { exitCode: isError ? 1 : 0, audits: [{ toolCallId: "nested", output: "unclipped" }] };
    const result = { content, details };
    const end = { type: "tool_execution_end", toolCallId: "call_1", toolName: "bash", result, isError, extra: "kept" };
    const message = { ...toolResult, content, details, isError };
    const events = [
      { type: "tool_execution_start", toolCallId: "call_1", toolName: "bash", args: { command: "ls" } },
      end,
      { type: "message_start", message },
      { type: "message_end", message },
    ];
    const { text, lines } = write(events, true);
    expect(lines[1]).toEqual({ ...end, result: { elided: true, bytes: Buffer.byteLength(JSON.stringify(result), "utf8") } });
    expect(Buffer.byteLength(JSON.stringify(result), "utf8")).toBeGreaterThan(JSON.stringify(result).length);
    expect(text.match(/unique final result/g)).toHaveLength(1);
    expect(lines[2]).toEqual({ type: "message_end", message });
    expect(end.result).toBe(result);
    const rawText = events.map((event) => `${JSON.stringify(event)}\n`).join("");
    expect(readEntries(text)).toEqual(readEntries(rawText));
    expect(readEntries(text)).toEqual([expect.objectContaining({
      kind: "tool", toolName: "bash", status: isError ? "failed" : "completed", result,
    })]);
    const raw = readTranscript(rawText);
    const written = readTranscript(text);
    expect(written.messages).toEqual(raw.messages);
    expect(written.streaming).toEqual(raw.streaming);
    expect(written.streaming.tools[0]?.result).toEqual(result);
  });

  it.each([
    { terminate: true }, // Real fabric_reply result shape: Pi's message omits terminate.
    { opaque: { version: 2, values: [false, null, "kept"] }, customFlag: 0 },
  ])("retains noncanonical result metadata without duplicating the body (%j)", (metadata) => {
    const content = [{ type: "text", text: largeResultText("Reply delivered.") }];
    const details = {};
    const result = { content, details, ...metadata };
    const end = { type: "tool_execution_end", toolCallId: "reply", toolName: "fabric_reply", result, isError: false };
    const message = { role: "toolResult", toolCallId: "reply", toolName: "fabric_reply", content, details, isError: false, timestamp: 13 };
    const events = [
      { type: "tool_execution_start", toolCallId: "reply", toolName: "fabric_reply", args: { reply: "done" } },
      end,
      { type: "message_start", message },
      { type: "message_end", message },
    ];
    const { text, lines } = write(events, true);
    expect(lines[1]).toEqual({ ...end, result: { elided: true, bytes: Buffer.byteLength(JSON.stringify(result)) }, resultMetadata: metadata });
    expect(lines[1]?.resultMetadata).not.toHaveProperty("content");
    expect(lines[1]?.resultMetadata).not.toHaveProperty("details");
    expect(text.match(/Reply delivered\./g)).toHaveLength(1);
    expect(text.match(/"details"/g)).toHaveLength(1);
    expect(end.result).toBe(result);
    const rawText = events.map((event) => `${JSON.stringify(event)}\n`).join("");
    expect(readEntries(text)).toEqual(readEntries(rawText));
    expect(readEntries(text)).toEqual([expect.objectContaining({ kind: "tool", toolName: "fabric_reply", status: "completed", result })]);
    const raw = readTranscript(rawText);
    const written = readTranscript(text);
    expect(written.messages).toEqual(raw.messages);
    expect(written.streaming).toEqual(raw.streaming);
  });

  it("does not elide other payloads or add a missing execution result", () => {
    const events = [
      { type: "custom_event", result: { content: toolResult.content } },
      { type: "tool_execution_end", toolCallId: "missing", isError: true },
      { type: "message_end", message: toolResult },
    ];
    expect(write(events, true).lines).toEqual(events);
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
