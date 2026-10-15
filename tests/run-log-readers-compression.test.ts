import "./fixtures/conversation-host.js";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { constants, zstdCompressSync } from "node:zlib";
import { describe, expect, it, vi } from "vitest";
import { AgentTranscriptReader } from "../src/ui/transcript-reader.js";
import { NativeConversationReader } from "../src/ui/conversation-native-reader.js";

const frame = (bytes: Buffer) => zstdCompressSync(bytes, { params: { [constants.ZSTD_c_checksumFlag]: 1 } });
const fixture = () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "run-readers-"));
  const file = path.join(directory, "events.jsonl");
  const events = [
    ...Array.from({ length: 140 }, (_, index) => ({ type: "message_end", message: {
      role: "user", content: `line-${index} α😀\r\n${"x".repeat(3000)}`, timestamp: index,
    } })),
    { type: "tool_execution_start", toolCallId: "call-α", toolName: "read", args: { path: "source.ts" } },
    { type: "tool_execution_update", toolCallId: "call-α", partialResult: { content: [{ type: "text", text: "partial😀" }], details: { bytes: 17 } } },
    { type: "message_start", message: { role: "assistant", content: [], timestamp: 1000 } },
    { type: "message_update", assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "stream α😀" } },
  ];
  const bytes = Buffer.from(events.map((event) => JSON.stringify(event)).join("\r\n") + '\r\n{"type":"unfinished');
  fs.writeFileSync(file, bytes);
  const zipped = path.join(directory, "zipped");
  fs.mkdirSync(zipped);
  const compressed = path.join(zipped, "events.jsonl");
  fs.writeFileSync(`${compressed}.zst`, frame(bytes));
  return { file, compressed, bytes };
};
const source = (logFile: string) => ({ id: "same-participant", status: "completed", logFile });
const dashboardShape = (value: ReturnType<AgentTranscriptReader["read"]>) => ({
  entries: value.entries, hasMore: value.hasMore, hasNewer: value.hasNewer, truncated: value.truncated,
});
const nativeShape = (value: ReturnType<NativeConversationReader["read"]>) => ({
  entries: value.entries, messages: value.messages, streaming: value.streaming, leafId: value.leafId,
  hasMore: value.hasMore, hasNewer: value.hasNewer, historyComplete: value.historyComplete,
  unavailable: value.unavailable, error: value.error,
});

describe("compressed run log readers — fixture-only user paths", () => {
  // __CUT1241__: Compressed dashboard paging is not a Windows rollout path.
  it.skipIf(process.platform === "win32")("preserves dashboard entry IDs/order/content and older/newer/latest partial-tool pages", () => {
    const { file, compressed } = fixture();
    const plain = new AgentTranscriptReader();
    const zipped = new AgentTranscriptReader();
    const a = source(file), b = source(compressed);
    expect(dashboardShape(zipped.read(b))).toEqual(dashboardShape(plain.read(a)));
    expect(zipped.read(b).entries).toContainEqual(expect.objectContaining({ kind: "tool", status: "running", args: { path: "source.ts" } }));
    for (let page = 0; page < 5; page++) {
      expect(zipped.loadOlder(b)).toBe(plain.loadOlder(a));
      expect(dashboardShape(zipped.read(b, false))).toEqual(dashboardShape(plain.read(a, false)));
    }
    for (let page = 0; page < 5; page++) {
      expect(zipped.loadNewer(b)).toBe(plain.loadNewer(a));
      expect(dashboardShape(zipped.read(b, false))).toEqual(dashboardShape(plain.read(a, false)));
    }
    expect(zipped.loadLatest(b)).toBe(plain.loadLatest(a));
    expect(dashboardShape(zipped.read(b))).toEqual(dashboardShape(plain.read(a)));
  });

  // __CUT1241__: Compressed native replay is not a Windows rollout path.
  it.skipIf(process.platform === "win32")("preserves native replay, full Unicode records and streamed tool partials across classification and paging", () => {
    const { file, compressed } = fixture();
    const plain = new NativeConversationReader();
    const zipped = new NativeConversationReader();
    const a = source(file), b = source(compressed);
    expect(nativeShape(zipped.read(b))).toEqual(nativeShape(plain.read(a)));
    expect(zipped.last?.streaming.tools[0]?.partial).toEqual({ content: [{ type: "text", text: "partial😀" }], details: { bytes: 17 } });
    expect(zipped.last?.streaming.partialAssistant?.content).toEqual([{ type: "text", text: "stream α😀" }]);
    for (let page = 0; page < 3; page++) {
      expect(nativeShape(zipped.loadOlder()!)).toEqual(nativeShape(plain.loadOlder()!));
    }
    expect(zipped.last?.messages).toHaveLength(140);
    expect(nativeShape(zipped.loadNewer()!)).toEqual(nativeShape(plain.loadNewer()!));
    expect(nativeShape(zipped.loadLatest()!)).toEqual(nativeShape(plain.loadLatest()!));
    expect(zipped.last?.unavailable).toBeUndefined();
  });

  // __CUT1241__: Windows does not adopt compressed archive generations.
  it.skipIf(process.platform === "win32")("adopts the new physical compressed generation without losing pinned native history or inventing unread arrivals", () => {
    const { file, bytes } = fixture();
    const reader = new NativeConversationReader();
    const input = source(file);
    reader.read(input, false);
    reader.loadOlder();
    const before = nativeShape(reader.read(input, false));
    fs.writeFileSync(`${file}.zst`, frame(bytes));
    fs.renameSync(file, `${file}.fixture-plain`);
    expect(nativeShape(reader.read(input, false))).toEqual(before);
    expect(reader.last?.messages).toHaveLength(140);
    expect(reader.last?.unavailable).toBeUndefined();
  });

  describe.each(["plain", "compressed"] as const)("precision-colliding physical generations: %s", (format) => {
    // __CUT1241__: Only the compressed cache variant is off on Windows; plain wide IDs remain asserted.
    it.skipIf(process.platform === "win32" && format === "compressed")("distinguishes precision-colliding physical generations in native replacement and compressed caches", () => {
      const directory = fs.mkdtempSync(path.join(os.tmpdir(), "reader-bigint-"));
      const file = path.join(directory, "events.jsonl");
      const bytes = (text: string) => Buffer.from(`${JSON.stringify({ type: "message_end", message: { role: "user", content: text, timestamp: 1 } })}\n`);
      const physical = format === "compressed" ? `${file}.zst` : file;
      const encode = (text: string) => format === "compressed" ? frame(bytes(text)) : bytes(text);
      fs.writeFileSync(physical, encode("first"));
      let inode = 9007199254740992n;
      const original = fs.fstatSync;
      vi.spyOn(fs, "fstatSync").mockImplementation(((...args: unknown[]) => {
        const stat = Reflect.apply(original, fs, args);
        return Object.assign(stat, { dev: 9007199254740994n, ino: inode, birthtimeNs: 1n, mtimeNs: 2n });
      }) as typeof fs.fstatSync);
      // Preserve the POSIX no-O_NOFOLLOW fence; plain Windows needs no path-stat protocol.
      if (process.platform !== "win32") {
        const originalPathStat = fs.lstatSync;
        vi.spyOn(fs, "lstatSync").mockImplementation(((...args: unknown[]) => {
          const stat = Reflect.apply(originalPathStat, fs, args);
          return Object.assign(stat, { dev: 9007199254740994n, ino: inode, birthtimeNs: 1n, mtimeNs: 2n });
        }) as typeof fs.lstatSync);
      }
      const reader = new NativeConversationReader();
      expect(reader.read(source(file)).messages[0]).toMatchObject({ content: "first" });
      fs.writeFileSync(physical, encode("other"));
      inode += 1n;
      expect(Number(inode)).toBe(Number(inode - 1n));
      expect(reader.read(source(file)).messages[0]).toMatchObject({ content: "other" });
      expect(reader.last?.unavailable).toBeUndefined();
    });
  });

  // __CUT1241__: Windows never decodes compressed run history or reports archive codec failures.
  it.skipIf(process.platform === "win32")("shows codec failure as an explicit dashboard error card and native unavailable state, including cached history", () => {
    const { file, compressed } = fixture();
    const dashboard = new AgentTranscriptReader();
    const native = new NativeConversationReader();
    dashboard.read(source(file));
    const cachedNative = native.read(source(file));
    fs.renameSync(file, `${file}.fixture-plain`);
    fs.writeFileSync(`${file}.zst`, "corrupt");
    const failed = dashboard.read(source(file));
    expect(failed.entries[0]).toMatchObject({ kind: "error", label: "Transcript unavailable", status: "failed" });
    expect(failed.entries[0]?.text).toMatch(/run-log-codec/);
    expect(failed.entries[0]?.text).toMatch(/cached/i);
    const failedNative = native.read(source(file));
    expect(failedNative).toMatchObject({ unavailable: { eventsFile: true }, error: expect.stringContaining("run-log-codec") });
    expect(failedNative.error).toMatch(/cached/i);
    expect(failedNative.messages).toEqual(cachedNative.messages);
    expect(failedNative.error!.length).toBeLessThanOrEqual(200);
    fs.writeFileSync(`${compressed}.zst`, "broken");
    expect(new AgentTranscriptReader().read(source(compressed)).entries[0]).toMatchObject({ kind: "error" });
    expect(new NativeConversationReader().read(source(compressed))).toMatchObject({ unavailable: { eventsFile: true }, error: expect.stringContaining("run-log-codec") });
    const toolsFile = path.join(path.dirname(file), "tools", "events.jsonl");
    fs.mkdirSync(path.dirname(toolsFile));
    fs.writeFileSync(toolsFile, [
      { type: "tool_execution_start", toolCallId: "tool-only", toolName: "read" },
      { type: "tool_execution_end", toolCallId: "tool-only", result: "cached result" },
    ].map((event) => JSON.stringify(event)).join("\n") + "\n");
    const toolsReader = new NativeConversationReader();
    const toolsBefore = toolsReader.read(source(toolsFile));
    expect(toolsBefore.streaming.tools).toHaveLength(1);
    expect(toolsBefore.streaming.active).toBe(false);
    fs.renameSync(toolsFile, `${toolsFile}.fixture-plain`);
    fs.writeFileSync(`${toolsFile}.zst`, "corrupt");
    const toolsAfter = toolsReader.read(source(toolsFile));
    expect(toolsAfter.streaming.tools).toEqual(toolsBefore.streaming.tools);
    expect(toolsAfter.error).toMatch(/cached/i);
    expect(toolsAfter.error!.length).toBeLessThanOrEqual(200);
  });

  it("reports plain descriptor I/O failure without throwing or silently verifying native cached history", () => {
    const { file } = fixture();
    const reader = new NativeConversationReader();
    const input = { ...source(file), eventsFile: file };
    const cached = reader.read(input);
    fs.appendFileSync(file, '\n{"type":"agent_end"}\n');
    vi.spyOn(fs, "readSync").mockImplementation((() => {
      throw Object.assign(new Error("fixture read EIO"), { code: "EIO" });
    }) as typeof fs.readSync);
    const failed = reader.read(input);
    expect(failed).toMatchObject({ unavailable: { eventsFile: true }, error: expect.stringContaining("run-log-codec") });
    expect(failed.error).toMatch(/cached/i);
    expect(failed.messages).toEqual(cached.messages);
  });

  it("keeps absent history distinct from denied/capped history and leaves native session files plain", () => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), "reader-failures-"));
    const missing = path.join(directory, "events.jsonl");
    expect(new AgentTranscriptReader().read(source(missing)).entries).toEqual([]);
    const original = fs.openSync;
    vi.spyOn(fs, "openSync").mockImplementation(((name: fs.PathLike, ...args: unknown[]) => {
      if (name === missing) throw Object.assign(new Error("fixture denied"), { code: "EACCES" });
      return Reflect.apply(original, fs, [name, ...args]);
    }) as typeof fs.openSync);
    expect(new AgentTranscriptReader().read(source(missing)).entries[0]).toMatchObject({ kind: "error", text: expect.stringContaining("EACCES") });
    expect(new NativeConversationReader().read({ ...source(missing), eventsFile: missing })).toMatchObject({ unavailable: { eventsFile: true }, error: expect.stringContaining("EACCES") });
    vi.restoreAllMocks();
    const session = path.join(directory, "session.jsonl");
    fs.writeFileSync(session, '{"type":"session","id":"session-id","version":3}\n');
    expect(new NativeConversationReader().read(source(session))).toMatchObject({ sessionFile: session, sessionId: "session-id" });
    fs.renameSync(session, `${session}.fixture-plain`);
    fs.writeFileSync(`${session}.zst`, frame(Buffer.from('{"type":"session","id":"not-allowed"}\n')));
    expect(new NativeConversationReader().read(source(session)).sessionId).toBeUndefined();
  });

  // __CUT1241__: Compressed input caps apply only to the POSIX read-through path.
  it.skipIf(process.platform === "win32")("reports capped compressed history distinctly from absent history", () => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), "reader-caps-"));
    const missing = path.join(directory, "events.jsonl");
    fs.writeFileSync(`${missing}.zst`, frame(Buffer.from("{}\n")));
    const originalStat = fs.fstatSync;
    vi.spyOn(fs, "fstatSync").mockImplementation(((...args: unknown[]) => {
      const stat = Reflect.apply(originalStat, fs, args);
      return Object.assign(stat, { size: 128n * 1024n * 1024n + 1n });
    }) as typeof fs.fstatSync);
    expect(new AgentTranscriptReader().read(source(missing)).entries[0]).toMatchObject({ kind: "error", text: expect.stringContaining("cap") });
    expect(new NativeConversationReader().read({ ...source(missing), eventsFile: missing })).toMatchObject({ unavailable: { eventsFile: true }, error: expect.stringContaining("cap") });
  });
});
