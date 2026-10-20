import "./fixtures/conversation-host.js";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { constants, zstdCompressSync } from "node:zlib";
import { afterEach, describe, expect, it, vi } from "vitest";
import { readRunLogPage } from "../src/log-tail.js";
import { compressTerminalRunLog } from "../src/storage/run-log-compression.js";
import { restoreRunLog } from "../src/storage/run-log-restore.js";
import { AgentTranscriptReader } from "../src/ui/transcript-reader.js";
import { NativeConversationReader } from "../src/ui/conversation-native-reader.js";

// Data-safety boundary: only test-created private directories are compressed,
// restored or removed. No worker/session storage or reader checkpoints are used.
const directories: string[] = [];
const workspace = () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "run-readers-"));
  directories.push(directory);
  return directory;
};
afterEach(() => {
  vi.restoreAllMocks();
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true });
});
const frame = (bytes: Buffer) => zstdCompressSync(bytes, { params: { [constants.ZSTD_c_checksumFlag]: 1 } });
async function prepareCompressedReaderFixture(file: string): Promise<void> {
  const bytes = fs.readFileSync(file);
  const result = await compressTerminalRunLog(file);
  if (process.platform === "win32") {
    // Production must skip: no fake success or platform override. Model an artifact
    // imported from POSIX using REAL checksummed Node Zstd bytes in this private fixture.
    expect(result).toEqual({ kind: "skipped", reason: "platform" });
    expect(fs.readFileSync(file)).toEqual(bytes);
    expect(fs.existsSync(`${file}.zst`)).toBe(false);
    fs.writeFileSync(`${file}.zst`, frame(bytes), { flag: "wx", mode: 0o600 });
    fs.unlinkSync(file); // Test fixture installation only, never production compressor retirement.
  } else {
    expect(result).toMatchObject({ kind: "compressed", originalBytes: bytes.length });
  }
  expect(fs.existsSync(file)).toBe(false);
  const encoded = fs.readFileSync(`${file}.zst`);
  expect(encoded.readUInt32LE(0)).toBe(0xfd2fb528);
  expect(encoded[4]! & 4).toBe(4);
}
const fixture = () => {
  const directory = workspace();
  const file = path.join(directory, "events.jsonl");
  const events = [
    ...Array.from({ length: 320 }, (_, index) => ({ type: "message_end", message: {
      role: "user", content: `line-${index} α😀\r\n${"x".repeat(3000)}`, timestamp: index,
    } })),
    { type: "tool_execution_start", toolCallId: "call-α", toolName: "read", args: { path: "source.ts" } },
    { type: "tool_execution_update", toolCallId: "call-α", partialResult: { content: [{ type: "text", text: "partial😀" }], details: { bytes: 17 } } },
    { type: "message_start", message: { role: "assistant", content: [], timestamp: 1000 } },
    { type: "message_update", assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "stream α😀" } },
  ];
  const bytes = Buffer.from(events.map((event) => JSON.stringify(event)).join("\r\n") + '\r\n{"type":"unfinished');
  fs.writeFileSync(file, bytes, { mode: 0o600 });
  const referenceDirectory = path.join(directory, "reference");
  fs.mkdirSync(referenceDirectory, { mode: 0o700 });
  const reference = path.join(referenceDirectory, "events.jsonl");
  fs.writeFileSync(reference, bytes);
  return { file, reference, bytes };
};
const source = (logFile: string) => ({ id: "same-participant", status: "completed", logFile });
const dashboardShape = (value: ReturnType<AgentTranscriptReader["read"]>) => ({
  entries: value.entries, hasMore: value.hasMore, hasNewer: value.hasNewer, truncated: value.truncated,
  unavailable: value.unavailable, error: value.error,
});
const nativeShape = (value: ReturnType<NativeConversationReader["read"]>) => ({
  entries: value.entries, messages: value.messages, streaming: value.streaming, leafId: value.leafId,
  hasMore: value.hasMore, hasNewer: value.hasNewer, historyComplete: value.historyComplete,
  unavailable: value.unavailable, error: value.error,
});

describe("compressed run log readers — fixture-only user paths", () => {
  it("keeps dashboard page IDs/order/content and bookmarks through compressed-only storage and additive restore", async () => {
    const { file, reference } = fixture();
    const reader = new AgentTranscriptReader(), plain = new AgentTranscriptReader();
    const input = source(file), control = source(reference);
    expect(dashboardShape(reader.read(input))).toEqual(dashboardShape(plain.read(control)));
    expect(reader.read(input).entries).toContainEqual(expect.objectContaining({ kind: "tool", status: "running", args: { path: "source.ts" } }));
    const older = () => {
      expect(reader.loadOlder(input)).toBe(plain.loadOlder(control));
      expect(dashboardShape(reader.read(input, false))).toEqual(dashboardShape(plain.read(control, false)));
    };
    older();
    const pinned = dashboardShape(reader.read(input, false));
    await prepareCompressedReaderFixture(file);
    expect(dashboardShape(reader.read(input, false))).toEqual(pinned);
    older();
    const compressedPage = dashboardShape(reader.read(input, false));
    expect((await restoreRunLog(`${file}.zst`, { apply: true })).kind).toBe("restored");
    expect(fs.readFileSync(file)).toEqual(fs.readFileSync(reference));
    expect(fs.existsSync(`${file}.zst`)).toBe(true);
    expect(dashboardShape(reader.read(input, false))).toEqual(compressedPage);
    older();
    for (let page = 0; page < 3; page++) {
      expect(reader.loadNewer(input)).toBe(plain.loadNewer(control));
      expect(dashboardShape(reader.read(input, false))).toEqual(dashboardShape(plain.read(control, false)));
    }
    expect(reader.loadLatest(input)).toBe(plain.loadLatest(control));
    expect(dashboardShape(reader.read(input))).toEqual(dashboardShape(plain.read(control)));
  });

  it("keeps native full records, pinned history and streamed partials through compressed-only storage, restore and paging", async () => {
    const { file, reference } = fixture();
    const reader = new NativeConversationReader(), plain = new NativeConversationReader();
    const input = source(file), control = source(reference);
    expect(nativeShape(reader.read(input, false))).toEqual(nativeShape(plain.read(control, false)));
    expect(reader.last?.streaming.tools[0]?.partial).toEqual({ content: [{ type: "text", text: "partial😀" }], details: { bytes: 17 } });
    expect(reader.last?.streaming.partialAssistant?.content).toEqual([{ type: "text", text: "stream α😀" }]);
    expect(nativeShape(reader.loadOlder()!)).toEqual(nativeShape(plain.loadOlder()!));
    expect(reader.last?.hasMore).toBe(true);
    const pinned = nativeShape(reader.read(input, false));
    await prepareCompressedReaderFixture(file);
    expect(nativeShape(reader.read(input, false))).toEqual(pinned);
    expect(nativeShape(reader.loadOlder()!)).toEqual(nativeShape(plain.loadOlder()!));
    const compressedHistory = nativeShape(reader.read(input, false));
    expect((await restoreRunLog(`${file}.zst`, { apply: true })).kind).toBe("restored");
    expect(nativeShape(reader.read(input, false))).toEqual(compressedHistory);
    expect(nativeShape(reader.loadOlder(3)!)).toEqual(nativeShape(plain.loadOlder(3)!));
    expect(reader.last?.messages).toHaveLength(320);
    expect(nativeShape(reader.loadNewer()!)).toEqual(nativeShape(plain.loadNewer()!));
    expect(nativeShape(reader.loadLatest()!)).toEqual(nativeShape(plain.loadLatest()!));
    expect(reader.last?.unavailable).toBeUndefined();
    reader.clear(); plain.clear();
  });

  it("uses decoded page cursors but rejects stale physical generations after compression and restore", async () => {
    const { file, reference } = fixture();
    const initial = readRunLogPage(file, 40);
    expect(initial.before).toBeGreaterThan(0);
    expect(initial.generation).toBeTypeOf("string");
    await prepareCompressedReaderFixture(file);
    expect(() => readRunLogPage(file, 40, initial.before, undefined, initial.generation)).toThrow(/cursor-stale/);
    const compressed = readRunLogPage(file, 40);
    expect(compressed.generation).not.toBe(initial.generation);
    expect(compressed.lines).toEqual(initial.lines);
    const older = readRunLogPage(file, 40, compressed.before, undefined, compressed.generation);
    expect(older.lines).toEqual(readRunLogPage(reference, 40, initial.before).lines);
    expect((await restoreRunLog(`${file}.zst`, { apply: true })).kind).toBe("restored");
    expect(() => readRunLogPage(file, 40, compressed.before, undefined, compressed.generation)).toThrow(/cursor-stale/);
    const restored = readRunLogPage(file, 40);
    expect(restored.lines).toEqual(initial.lines);
    expect(restored.generation).not.toBe(compressed.generation);
  });

  it("does not retain a pinned dashboard page when replacement bytes change at the same offsets", () => {
    const { file, bytes } = fixture();
    const reader = new AgentTranscriptReader();
    const input = source(file);
    reader.read(input);
    expect(reader.loadOlder(input)).toBe(true);
    const pinned = dashboardShape(reader.read(input, false));
    const rewritten = Buffer.from(bytes.toString("utf8").replaceAll("line-", "edit-"));
    expect(rewritten.length).toBe(bytes.length);
    fs.writeFileSync(`${file}.zst`, frame(rewritten));
    fs.unlinkSync(file);
    const replaced = reader.read(input, false);
    expect(replaced.unavailable).toBeUndefined();
    expect(dashboardShape(replaced)).not.toEqual(pinned);
    expect(replaced.entries.filter(entry => entry.kind === "user").every(entry => entry.text?.startsWith("edit-"))).toBe(true);
    expect(replaced.hasNewer).toBe(false);
  });

  it.each([
    { corruption: "header", reason: "expected a standard zstd frame" },
    { corruption: "checksum", reason: "zstd frame decoding failed" },
  ])("shows $corruption-corrupt frames as unavailable rather than empty success and labels retained history cached", ({ corruption, reason }) => {
    const { file, bytes } = fixture();
    const dashboard = new AgentTranscriptReader(), native = new NativeConversationReader();
    const input = source(file);
    const cachedDashboard = dashboard.read(input), cachedNative = native.read(input);
    fs.unlinkSync(file);
    const corrupt = corruption === "header" ? Buffer.from("corrupt") : frame(bytes);
    if (corruption === "checksum") corrupt[corrupt.length - 1] = corrupt[corrupt.length - 1]! ^ 1;
    fs.writeFileSync(`${file}.zst`, corrupt);
    const failed = dashboard.read(input);
    expect(failed.unavailable).toBe(true);
    expect(failed.entries[0]).toMatchObject({ kind: "error", label: "Transcript unavailable", status: "failed", text: expect.stringContaining("run-log-codec") });
    expect(failed.entries[0]?.text).toMatch(/cached history/i);
    expect(failed.error).toContain(reason);
    expect(failed.entries.slice(1)).toEqual(cachedDashboard.entries);
    const failedNative = native.read(input);
    expect(failedNative).toMatchObject({ unavailable: { eventsFile: true }, error: expect.stringContaining("run-log-codec") });
    expect(failedNative.error).toMatch(/cached history/i);
    expect(failedNative.error).toContain(reason);
    expect(failedNative.messages).toEqual(cachedNative.messages);
    expect(failedNative.error!.length).toBeLessThanOrEqual(200);
    expect(new AgentTranscriptReader().read(input)).toMatchObject({ unavailable: true, entries: [expect.objectContaining({ kind: "error" })] });
    expect(new NativeConversationReader().read(input)).toMatchObject({ unavailable: { eventsFile: true }, error: expect.stringContaining("run-log-codec") });
    native.clear();
  });

  it("labels tool-only cached native history and reports a plain read failure without throwing", () => {
    const file = path.join(workspace(), "events.jsonl");
    fs.writeFileSync(file, [
      { type: "tool_execution_start", toolCallId: "tool-only", toolName: "read" },
      { type: "tool_execution_end", toolCallId: "tool-only", result: "cached result" },
    ].map(event => JSON.stringify(event)).join("\n") + "\n");
    const reader = new NativeConversationReader();
    const input = { ...source(file), eventsFile: file };
    const before = reader.read(input);
    expect(before.streaming.active).toBe(false);
    expect(before.streaming.tools).toHaveLength(1);
    fs.appendFileSync(file, '{"type":"agent_end"}\n');
    vi.spyOn(fs, "readSync").mockImplementation((() => {
      throw Object.assign(new Error("fixture read EIO"), { code: "EIO" });
    }) as typeof fs.readSync);
    const failed = reader.read(input);
    expect(failed).toMatchObject({ unavailable: { eventsFile: true }, error: expect.stringContaining("run-log-codec") });
    expect(failed.error).toMatch(/cached history/i);
    expect(failed.streaming.tools).toEqual(before.streaming.tools);
    reader.clear();
  });

  it.each([
    { boundary: "open", code: "EACCES" },
    { boundary: "read", code: "EIO" },
    { boundary: "read", code: "EPRIVATE_SECRET" },
    { boundary: "decode", code: undefined },
  ])("never exposes arbitrary $boundary exception messages in either reader or cached warnings (code=$code)", ({ boundary, code }) => {
    const file = path.join(workspace(), "events.jsonl");
    const bytes = Buffer.from('{"type":"message_end","message":{"role":"user","content":"cached user message","timestamp":1}}\n');
    fs.writeFileSync(file, bytes);
    const input = source(file);
    const dashboard = new AgentTranscriptReader(), native = new NativeConversationReader();
    const beforeDashboard = dashboard.read(input), beforeNative = native.read(input);
    const cause = Object.assign(new Error("private payload secret must not appear"), { code });
    if (boundary === "open") {
      const original = fs.openSync;
      vi.spyOn(fs, "openSync").mockImplementation(((name: fs.PathLike, ...args: unknown[]) => {
        if (name === file) throw cause;
        return Reflect.apply(original, fs, [name, ...args]);
      }) as typeof fs.openSync);
    } else if (boundary === "read") {
      fs.appendFileSync(file, '{"type":"agent_end"}\n');
      vi.spyOn(fs, "readSync").mockImplementation((() => { throw cause; }) as typeof fs.readSync);
    } else {
      fs.renameSync(file, `${file}.fixture-plain`);
      fs.writeFileSync(`${file}.zst`, frame(bytes));
      const original = process.getBuiltinModule;
      vi.spyOn(process, "getBuiltinModule").mockImplementation(((name: string) => name === "node:zlib"
        ? { zstdDecompressSync() { throw cause; } } : original(name)) as typeof process.getBuiltinModule);
    }
    const failedDashboard = dashboard.read(input), failedNative = native.read(input);
    expect(failedDashboard).toMatchObject({ unavailable: true, entries: [expect.objectContaining({ kind: "error", label: "Transcript unavailable" }), ...beforeDashboard.entries] });
    expect(failedNative).toMatchObject({ unavailable: { eventsFile: true }, messages: beforeNative.messages });
    expect(failedDashboard.entries[0]?.text).toMatch(/cached history/i);
    expect(failedNative.error).toMatch(/cached history/i);
    for (const failed of [failedDashboard, failedNative, new AgentTranscriptReader().read(input), new NativeConversationReader().read(input)]) {
      expect(failed.unavailable).toBeTruthy();
      expect(failed.error).toContain("run-log-codec");
      expect(failed.error!.length).toBeLessThanOrEqual(300);
      expect(JSON.stringify(failed)).not.toMatch(/private payload|secret|EPRIVATE_SECRET/);
      if (code === "EIO" || code === "EACCES") expect(failed.error).toContain(code);
    }
    expect(failedNative.error!.length).toBeLessThanOrEqual(200);
    vi.restoreAllMocks();
    expect(dashboard.read(input).unavailable).toBeUndefined();
    expect(native.read(input).unavailable).toBeUndefined();
    native.clear();
  });

  it("falls back only on plain ENOENT, gives plain priority, and never falls back for native session logs", () => {
    const directory = workspace();
    const file = path.join(directory, "events.jsonl");
    const bytes = Buffer.from('{"type":"message_end","message":{"role":"user","content":"plain wins","timestamp":1}}\n');
    fs.writeFileSync(file, bytes);
    fs.writeFileSync(`${file}.zst`, "corrupt");
    expect(new AgentTranscriptReader().read(source(file)).entries[0]?.text).toBe("plain wins");
    expect(new NativeConversationReader().read(source(file)).messages[0]).toMatchObject({ content: "plain wins" });
    fs.writeFileSync(`${file}.zst`, frame(bytes));
    const original = fs.openSync;
    vi.spyOn(fs, "openSync").mockImplementation(((name: fs.PathLike, ...args: unknown[]) => {
      if (name === file) throw Object.assign(new Error("fixture denied"), { code: "EACCES" });
      return Reflect.apply(original, fs, [name, ...args]);
    }) as typeof fs.openSync);
    expect(new AgentTranscriptReader().read(source(file))).toMatchObject({ unavailable: true, error: expect.stringContaining("EACCES") });
    expect(new NativeConversationReader().read(source(file))).toMatchObject({ unavailable: { eventsFile: true }, error: expect.stringContaining("EACCES") });
    vi.restoreAllMocks();
    const session = path.join(directory, "session.jsonl");
    fs.writeFileSync(session, '{"type":"session","id":"plain-session","version":3}\n');
    expect(new NativeConversationReader().read(source(session))).toMatchObject({ sessionFile: session, sessionId: "plain-session" });
    fs.unlinkSync(session);
    fs.writeFileSync(`${session}.zst`, frame(Buffer.from('{"type":"session","id":"must-not-load"}\n')));
    const absent = new NativeConversationReader().read({ ...source(session), sessionFile: session });
    expect(absent).toMatchObject({ unavailable: { sessionFile: true }, messages: [] });
    expect(absent.sessionId).toBeUndefined();
    expect(new AgentTranscriptReader().read(source(session)).entries).toEqual([]);
    const missing = path.join(directory, "missing", "events.jsonl");
    expect(new AgentTranscriptReader().read(source(missing)).entries).toEqual([]);
  });
});
